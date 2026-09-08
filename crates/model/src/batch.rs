//! 攒批批次状态：由 WAL 事件流重建（详细设计 §4.6 / 架构 §5.3.4）。
//!
//! BatchState 不单独存储，恢复时顺序重放 WAL 事件重建——
//! Data 与 BatchState 在同一 append-only 流中，顺序即因果。

use crate::wal_record::Record;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// 批次状态机：Pending → S3Written → Committed（终态）
/// Abort 为终态（C4：Abort 也进入终态，segment 才能释放，§5.3.6.1）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchStatus {
    Pending,
    S3Written,
    Committed,
    Abort,
}

/// C4 / §5.3.6.1：终态包含 Committed 与 Abort。
pub fn is_terminal(s: BatchStatus) -> bool {
    matches!(s, BatchStatus::Committed | BatchStatus::Abort)
}

/// 批次状态（详细设计 §4.6 / 架构 §5.3.4）
#[derive(Debug, Clone)]
pub struct BatchState {
    pub batch_id: String,
    pub client_request_id: Option<String>,
    pub shard: String,
    pub time_window: String,
    pub status: BatchStatus,
    pub s3_paths: Vec<String>,
    pub s3_upload_id: Option<String>,
    pub row_count: u64,
    pub schema_version: u64,
    pub created_at_ms: u64,
    /// 该批次覆盖的 WAL Data 记录区间
    pub wal_seq_range: (u64, u64),
}

impl BatchState {
    pub fn is_terminal(&self) -> bool {
        is_terminal(self.status)
    }

    /// 批次级超时判断（§5.3.6.1：默认 30 分钟）
    pub fn is_timed_out(&self, now_ms: u64, timeout: Duration) -> bool {
        !self.is_terminal() && now_ms.saturating_sub(self.created_at_ms) > timeout.as_millis() as u64
    }
}

/// BatchState 全集 + Data 攒批缓冲（崩溃恢复时重建）。
#[derive(Debug, Default)]
pub struct BatchStateMap {
    pub states: HashMap<String, BatchState>,
    /// (shard, window) -> 累积的 Data 记录（IPC bytes），BatchAbort 时按 batch 丢弃
    pub data_buffer: HashMap<(String, String), Vec<crate::wal_record::DataPayload>>,
}

/// 重放单条记录，应用状态机（详细设计 §4.6 `apply_record`）。
pub fn apply_record(state: &mut BatchStateMap, rec: &Record, _seq: u64) {
    match rec {
        Record::Data(p) => {
            // 累积到 (shard, window) 的攒批缓冲
            state
                .data_buffer
                .entry((p.shard.clone(), p.time_window.clone()))
                .or_default()
                .push(p.clone());
        }
        Record::BatchPending(p) => {
            state.states.insert(
                p.batch_id.clone(),
                BatchState {
                    batch_id: p.batch_id.clone(),
                    client_request_id: if p.client_request_id.is_empty() {
                        None
                    } else {
                        Some(p.client_request_id.clone())
                    },
                    shard: p.shard.clone(),
                    time_window: p.window.clone(),
                    status: BatchStatus::Pending,
                    s3_paths: Vec::new(),
                    s3_upload_id: None,
                    row_count: p.row_count,
                    schema_version: p.schema_version,
                    created_at_ms: p.created_at_ms,
                    wal_seq_range: (p.wal_seq_start, p.wal_seq_end.max(p.wal_seq_start)),
                },
            );
        }
        Record::BatchS3Written(p) => {
            if let Some(s) = state.states.get_mut(&p.batch_id) {
                s.status = BatchStatus::S3Written;
                s.s3_paths = p.s3_paths.clone();
                s.s3_upload_id = if p.s3_upload_id.is_empty() {
                    None
                } else {
                    Some(p.s3_upload_id.clone())
                };
            }
        }
        Record::BatchCommitted(p) => {
            if let Some(s) = state.states.get_mut(&p.batch_id) {
                s.status = BatchStatus::Committed;
            }
        }
        Record::BatchAbort(p) => {
            // 进入终态：移除 BatchState，丢弃其 Data
            state.states.remove(&p.batch_id);
        }
    }
}

/// 当前时间（Unix 毫秒）
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal_record::{
        BatchAbortPayload, BatchCommittedPayload, BatchPendingPayload, BatchS3WrittenPayload,
        DataPayload,
    };

    fn pending(id: &str) -> Record {
        Record::BatchPending(BatchPendingPayload {
            batch_id: id.into(),
            shard: "s0".into(),
            window: "w1".into(),
            wal_seq_start: 0,
            wal_seq_end: 1,
            schema_version: 1,
            client_request_id: "k".into(),
            created_at_ms: 1,
            row_count: 10,
        })
    }

    #[test]
    fn full_lifecycle() {
        let mut m = BatchStateMap::default();
        apply_record(&mut m, &pending("b1"), 1);
        assert_eq!(m.states["b1"].status, BatchStatus::Pending);

        apply_record(
            &mut m,
            &Record::BatchS3Written(BatchS3WrittenPayload {
                batch_id: "b1".into(),
                s3_paths: vec!["p1".into()],
                s3_upload_id: "u".into(),
                file_size: 1,
            }),
            2,
        );
        assert_eq!(m.states["b1"].status, BatchStatus::S3Written);
        assert_eq!(m.states["b1"].s3_upload_id.as_deref(), Some("u"));

        apply_record(
            &mut m,
            &Record::BatchCommitted(BatchCommittedPayload {
                batch_id: "b1".into(),
            }),
            3,
        );
        assert!(is_terminal(m.states["b1"].status));
    }

    #[test]
    fn abort_is_terminal_and_removes_state() {
        // C4: BatchAbort 属于终态
        let mut m = BatchStateMap::default();
        apply_record(&mut m, &pending("b1"), 1);
        apply_record(
            &mut m,
            &Record::BatchAbort(BatchAbortPayload {
                batch_id: "b1".into(),
            }),
            2,
        );
        assert!(m.states.get("b1").is_none());
    }

    #[test]
    fn data_buffer_accumulates() {
        let mut m = BatchStateMap::default();
        apply_record(
            &mut m,
            &Record::Data(DataPayload {
                table: "t".into(),
                shard: "s0".into(),
                schema_version: 1,
                batch_ipc: vec![1],
                client_request_id: String::new(),
                time_window: "w1".into(),
            }),
            1,
        );
        assert_eq!(m.data_buffer[&("s0".into(), "w1".into())].len(), 1);
    }

    #[test]
    fn timeout_only_for_non_terminal() {
        let mut m = BatchStateMap::default();
        apply_record(&mut m, &pending("b1"), 1);
        assert!(m.states["b1"].is_timed_out(now_ms(), Duration::from_secs(0)));
        // 终态不超时
        apply_record(
            &mut m,
            &Record::BatchCommitted(BatchCommittedPayload {
                batch_id: "b1".into(),
            }),
            2,
        );
        assert!(!m.states["b1"].is_timed_out(now_ms(), Duration::from_secs(0)));
    }
}
