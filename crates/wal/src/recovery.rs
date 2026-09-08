//! 崩溃恢复（详细设计 §4.5 / §4.6）。
//!
//! ```text
//! recover(shard):
//!   ① 读取 CURRENT（若损坏/不存在 → 取目录内最大 seq 的 segment）
//!   ② 从头开始顺序扫描所有 segment
//!   ③ for record in replay:
//!        读 length → 读 crc32+type+payload → 校验 CRC
//!          失败 → 【停止】，当前位置即 synced_seq（由 CRC 自然确定，§4.5）
//!          成功 → 应用记录（§4.6 状态机）
//!   ④ 重建 BatchState 表与攒批缓冲
//!   ⑤ 对每个非终态 BatchState，由调用方按状态分流（§5.6）
//! ```
//!
//! ★ `synced_seq` 无需持久化：CRC 校验边界 = fsync 边界（架构 §5.3.5.1 / 附录 H.3）。

use crate::config::WalConfig;
use crate::segment::{list_segments, load_segment};
use std::collections::HashMap;
use yuntun_model::batch::{apply_record, BatchStateMap};
use yuntun_model::error::{LakeError, WalError};

/// 单个 segment 的元信息（清理线程用）。
#[derive(Debug, Clone)]
pub struct SegmentInfo {
    /// segment 文件名
    pub name: String,
    /// 本 segment 记录的 seq 区间 [min, max]（空 segment 为 None）
    pub seq_range: Option<(u64, u64)>,
}

/// 恢复结果。
#[derive(Debug)]
pub struct Recovery {
    /// 下一条可用 seq（= CRC 边界 + 1）。
    /// fast 模式下即 `synced_seq + 1`；完整模式下等价。
    pub last_seq: u64,
    /// 已 fsync 的最高 seq（= 最后一条通过 CRC 校验的记录 seq；空 WAL 为 0）
    pub synced_seq: u64,
    /// 重建的批次状态 + Data 攒批缓冲
    pub states: BatchStateMap,
    /// 各 segment 元信息（按文件序号升序）
    pub segments: Vec<SegmentInfo>,
    /// 非 0 时表示扫描在 CRC 失败处停止（撕裂写入）
    pub torn_write_detected: bool,
}

/// 执行 recovery。
///
/// - `fast=true`：只为确定 seq 边界，不构建状态（`WalWriter::open` 用）
/// - `fast=false`：完整重建 `BatchStateMap` 与 segment 信息（启动流程用）
pub fn recover(cfg: &WalConfig, shard_id: u64, fast: bool) -> Result<Recovery, LakeError> {
    let shard_dir = cfg.shard_dir(shard_id);
    if !shard_dir.exists() {
        return Ok(Recovery {
            last_seq: 0,
            synced_seq: 0,
            states: BatchStateMap::default(),
            segments: Vec::new(),
            torn_write_detected: false,
        });
    }

    let segments = list_segments(&shard_dir)?;
    let mut first_first_seq: Option<u64> = None; // 首个 segment header.first_seq
    let mut synced_seq = 0u64;
    let mut found_any = false;
    let mut torn = false;
    let mut states = BatchStateMap::default();
    let mut seg_infos: Vec<SegmentInfo> = Vec::new();

    for (_, path) in &segments {
        let (header, records, complete) = match load_segment(path) {
            Ok(x) => x,
            Err(WalError::BadMagic(p)) => {
                // 该 segment 损坏 → 告警 + 跳过（人工介入，§4.9）
                tracing::error!(segment = %p, "segment header corrupted, skipping (manual intervention required)");
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        if first_first_seq.is_none() {
            first_first_seq = Some(header.first_seq);
        }
        let mut min_seq = Option::<u64>::None;
        let mut max_seq = Option::<u64>::None;
        for (seq, rec) in records {
            min_seq = Some(min_seq.map_or(seq, |m: u64| m.min(seq)));
            max_seq = Some(max_seq.map_or(seq, |m: u64| m.max(seq)));
            synced_seq = synced_seq.max(seq);
            found_any = true;
            if !fast {
                apply_record(&mut states, &rec, seq);
            }
        }
        seg_infos.push(SegmentInfo {
            name: path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default(),
            seq_range: match (min_seq, max_seq) {
                (Some(a), Some(b)) => Some((a, b)),
                _ => None,
            },
        });
        if !complete {
            // 撕裂写入边界 = fsync 边界（C3 的额外收益，§4.5）
            torn = true;
            tracing::warn!(
                shard = shard_id,
                segment = %path.display(),
                "torn write detected, replay stopped here (synced boundary)"
            );
            break;
        }
    }

    // 下一条可用 seq：
    // - 已解出过记录 → synced_seq + 1（CRC 边界即 fsync 边界，H.3）
    // - WAL 存在但无记录 → 首个 segment header.first_seq（通常为 0）
    // - 全新 WAL → 0
    let next_available = if found_any {
        synced_seq + 1
    } else {
        first_first_seq.unwrap_or(0)
    };

    Ok(Recovery {
        last_seq: next_available,
        synced_seq,
        states,
        segments: seg_infos,
        torn_write_detected: torn,
    })
}

/// 恢复后的状态分流表（§5.6）：按 BatchStatus 给出恢复动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryAction {
    /// `Pending`：未写 S3 → 重新编码 → 写 S3 → Commit（batch_id 复用）
    RedoS3AndCommit,
    /// `S3Written`：已写 S3 未 Commit → **只 Commit**（Meta 按 batch_id 幂等）
    CommitOnly,
    /// `Committed`：已完成 → 仅清理
    CleanupOnly,
    /// `Abort`：已放弃 → 丢弃；已写 S3 的文件由孤儿清理回收
    Discard,
}

pub fn recovery_action(status: yuntun_model::batch::BatchStatus) -> RecoveryAction {
    use yuntun_model::batch::BatchStatus::*;
    match status {
        Pending => RecoveryAction::RedoS3AndCommit,
        S3Written => RecoveryAction::CommitOnly,
        Committed => RecoveryAction::CleanupOnly,
        Abort => RecoveryAction::Discard,
    }
}

/// 构建 segment → batch_ids 映射（供清理线程，架构 §5.3.6）。
/// 基于 batch 的 `wal_seq_range` 与 segment 的 seq 区间求交。
pub fn segment_batches(
    segments: &[SegmentInfo],
    states: &BatchStateMap,
) -> HashMap<String, Vec<String>> {
    let mut m: HashMap<String, Vec<String>> = HashMap::new();
    for seg in segments {
        let Some((lo, hi)) = seg.seq_range else {
            continue;
        };
        for (id, st) in &states.states {
            let (s, e) = st.wal_seq_range;
            // Pending 的 Data 区间与 segment 相交即关联
            if s <= hi && e >= lo {
                m.entry(seg.name.clone()).or_default().push(id.clone());
            }
        }
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WalConfig;
    use crate::writer::WalWriter;
    use yuntun_model::batch::BatchStatus;
    use yuntun_model::wal_record::{
        BatchAbortPayload, BatchCommittedPayload, BatchPendingPayload, BatchS3WrittenPayload,
        DataPayload, Record,
    };

    fn tmpdir(name: &str) -> std::path::PathBuf {
        let d =
            std::env::temp_dir().join(format!("yuntun-wal-recovery-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[tokio::test]
    async fn seq_continues_across_restart() {
        let dir = tmpdir("restart");
        let cfg = WalConfig::for_dir(&dir);
        {
            let wal = WalWriter::open(cfg.clone(), 0).await.unwrap();
            for i in 0..5 {
                wal.append(pending(i)).await.unwrap();
            }
            assert_eq!(wal.synced_seq(), 4);
        } // drop → committer 退出

        let wal = WalWriter::open(cfg, 0).await.unwrap();
        let ack = wal.append(pending(100)).await.unwrap();
        assert!(
            ack.seq >= 5,
            "seq must continue after restart, got {}",
            ack.seq
        );
        assert_eq!(wal.synced_seq(), ack.seq);
    }

    #[tokio::test]
    async fn full_recovery_rebuilds_state_machine() {
        let dir = tmpdir("state");
        let cfg = WalConfig::for_dir(&dir);
        {
            let wal = WalWriter::open(cfg.clone(), 0).await.unwrap();
            // Data × 2
            for i in 0..2 {
                wal.append(Record::Data(DataPayload {
                    table: "t".into(),
                    shard: "s0".into(),
                    schema_version: 1,
                    batch_ipc: vec![i as u8],
                    client_request_id: String::new(),
                    time_window: "w1".into(),
                }))
                .await
                .unwrap();
            }
            wal.append(pending_with_range("b1", 0, 1)).await.unwrap();
            wal.append(Record::BatchS3Written(BatchS3WrittenPayload {
                batch_id: "b1".into(),
                s3_paths: vec!["s3://yuntun/f1".into()],
                s3_upload_id: "u1".into(),
                file_size: 10,
            }))
            .await
            .unwrap();
            wal.append(Record::BatchCommitted(BatchCommittedPayload {
                batch_id: "b1".into(),
            }))
            .await
            .unwrap();
            wal.append(pending_with_range("b2", 0, 1)).await.unwrap(); // 停在 Pending
        }

        let rec = recover(&cfg, 0, false).unwrap();
        assert!(!rec.torn_write_detected);
        assert_eq!(rec.states.states["b1"].status, BatchStatus::Committed);
        assert_eq!(rec.states.states["b2"].status, BatchStatus::Pending);
        assert_eq!(rec.states.data_buffer[&("s0".into(), "w1".into())].len(), 2);

        // §5.6 分流
        assert_eq!(
            recovery_action(BatchStatus::S3Written),
            RecoveryAction::CommitOnly
        );
        assert_eq!(
            recovery_action(BatchStatus::Pending),
            RecoveryAction::RedoS3AndCommit
        );
    }

    #[tokio::test]
    async fn torn_write_stops_replay_at_crc_boundary() {
        // Chaos T6.7 / H.3：崩溃后 synced 边界由 CRC 自然确定
        let dir = tmpdir("torn-recovery");
        let cfg = WalConfig::for_dir(&dir);
        {
            let wal = WalWriter::open(cfg.clone(), 0).await.unwrap();
            for i in 0..8 {
                wal.append(pending(i)).await.unwrap();
            }
            assert_eq!(wal.synced_seq(), 7);
        }
        // 模拟掉电撕裂：截断最后一条记录的一部分
        let seg = list_segments(&cfg.shard_dir(0)).unwrap().remove(0).1;
        let mut data = std::fs::read(&seg).unwrap();
        data.truncate(data.len() - 6);
        std::fs::write(&seg, &data).unwrap();

        let rec = recover(&cfg, 0, false).unwrap();
        assert!(rec.torn_write_detected);
        assert_eq!(rec.synced_seq, 6, "CRC 边界即 fsync 边界");
        assert_eq!(rec.states.states.len(), 7);
    }

    #[tokio::test]
    async fn abort_discards_batch_state() {
        let dir = tmpdir("abort");
        let cfg = WalConfig::for_dir(&dir);
        {
            let wal = WalWriter::open(cfg.clone(), 0).await.unwrap();
            wal.append(pending_with_range("b1", 0, 1)).await.unwrap();
            wal.append(Record::BatchAbort(BatchAbortPayload {
                batch_id: "b1".into(),
            }))
            .await
            .unwrap();
        }
        let rec = recover(&cfg, 0, false).unwrap();
        assert!(
            !rec.states.states.contains_key("b1"),
            "C4: Abort 后状态移除"
        );
    }

    fn pending(i: u64) -> Record {
        pending_with_range(&format!("b{i}"), i, i + 1)
    }

    fn pending_with_range(id: &str, s: u64, e: u64) -> Record {
        Record::BatchPending(BatchPendingPayload {
            batch_id: id.into(),
            shard: "s0".into(),
            window: "w".into(),
            wal_seq_start: s,
            wal_seq_end: e,
            schema_version: 1,
            client_request_id: String::new(),
            created_at_ms: 0,
            row_count: 1,
        })
    }
}
