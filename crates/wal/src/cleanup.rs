//! Segment 清理与 Batch 超时监控（详细设计 §4.8 / §5.3.6.1）。
//!
//! 【v11 分级超时】修复 batch 卡死导致 segment 永不释放、磁盘写满的 P0 风险：
//! | 级别 | 触发条件 | 动作 | 默认值 |
//! |---|---|---|---|
//! | 批次级超时 | 非终态超过 `batch_timeout` | 记录 `BatchAbort` → 终态 | 30 分钟 |
//! | 磁盘保护 | WAL 磁盘使用率 > `disk_high_watermark` | 强制 abort 最老的未完成 batch | 80% |
//!
//! 终态定义（C4）：`Committed | Abort` —— **Abort 也是终态**。

use crate::writer::WalWriter;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use yuntun_model::batch::{now_ms, BatchState};
use yuntun_model::error::LakeError;
use yuntun_model::wal_record::Record;

/// 运行期批次状态视图（由 ingest 侧的 live 状态表实现）。
pub trait BatchStateView: Send + Sync {
    /// 全部非终态批次（Pending / S3Written）
    fn non_terminal(&self) -> Vec<BatchState>;
}

/// 磁盘使用率提供者（0.0 ~ 1.0）。
pub trait DiskUsage: Send + Sync {
    fn usage(&self) -> f64;
}

/// 默认实现：WAL 目录字节数 / 配置的 max_wal_bytes（近似水位，
/// 生产环境建议接入 statvfs 的真实实现）。
pub struct DirSizeUsage {
    pub dir: std::path::PathBuf,
    pub max_bytes: u64,
}

impl DiskUsage for DirSizeUsage {
    fn usage(&self) -> f64 {
        if self.max_bytes == 0 {
            return 0.0;
        }
        let used = crate::config::dir_size_bytes(&self.dir) as f64;
        (used / self.max_bytes as f64).clamp(0.0, 1.0)
    }
}

/// 判断某 segment 是否可删除（架构 §5.3.6 安全清理）。
///
/// 安全性论证：`Committed` 意味着 Meta 已记录且 S3 文件存在；`Abort` 意味着
/// 该批次已明确放弃。二者都表示其 Data 记录不再需要 → 可安全删除。
///
/// `current_segment`：活跃 segment 永不删除。
/// `overlaps`: segment seq 区间与非终态 batch 的 wal_seq_range 是否相交。
pub fn segment_is_removable(
    seg_seq_range: Option<(u64, u64)>,
    is_current: bool,
    non_terminal_ranges: &[(u64, u64)],
) -> bool {
    if is_current {
        return false;
    }
    let Some((lo, hi)) = seg_seq_range else {
        // 空 segment（无记录）：只要不是活跃 segment 即可删
        return true;
    };
    !non_terminal_ranges
        .iter()
        .any(|(s, e)| *s <= hi && *e >= lo)
}

/// 后台监控线程（详细设计 §4.8 `cleanup_loop`，每 `monitor_interval` 执行一次）：
/// ① 批次级超时：非终态且超时 → 写 `BatchAbort`
/// ② 磁盘保护：> 水位 → 强制 abort 最老的未完成 batch
/// ③ segment 清理：全部批次终态的 segment 删除
pub fn spawn_timeout_monitor(
    wal: WalWriter,
    view: Arc<dyn BatchStateView>,
    disk: Option<Arc<dyn DiskUsage>>,
    segments_info: Vec<crate::recovery::SegmentInfo>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let cfg = wal.config().clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(cfg.monitor_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }
            let now = now_ms();

            // ① 批次级超时（§5.3.6.1）
            for st in view.non_terminal() {
                if st.is_timed_out(now, cfg.batch_timeout) {
                    tracing::warn!(batch_id = %st.batch_id, "batch timed out, writing BatchAbort");
                    if let Err(e) = wal
                        .append(Record::BatchAbort(
                            yuntun_model::wal_record::BatchAbortPayload {
                                batch_id: st.batch_id.clone(),
                            },
                        ))
                        .await
                    {
                        tracing::error!(error = %e, "failed to append BatchAbort");
                    }
                    // 该 batch 若已写 S3 → 文件成为孤儿，由孤儿清理回收
                }
            }

            // ② 磁盘保护（更激进）：> 水位 → 强制 abort 最老的未完成 batch
            if let Some(disk) = &disk {
                if disk.usage() > cfg.disk_high_watermark {
                    let mut cands = view.non_terminal();
                    cands.sort_by_key(|s| s.created_at_ms);
                    if let Some(oldest) = cands.first() {
                        tracing::warn!(
                            batch_id = %oldest.batch_id,
                            usage = %disk.usage(),
                            "disk watermark exceeded, force aborting oldest batch"
                        );
                        let _ = wal
                            .append(Record::BatchAbort(
                                yuntun_model::wal_record::BatchAbortPayload {
                                    batch_id: oldest.batch_id.clone(),
                                },
                            ))
                            .await;
                    }
                }
            }

            // ③ segment 清理：非活跃且所有关联 batch 均终态 → 删除
            let non_terminal: Vec<BatchState> = view.non_terminal();
            let ranges: Vec<(u64, u64)> = non_terminal.iter().map(|s| s.wal_seq_range).collect();
            let current = wal.current_segment();
            let shard_dir = wal.shard_dir();
            for seg in &segments_info {
                let seg_seq = crate::segment::parse_segment_file_name(&seg.name);
                if seg_seq == Some(current) {
                    continue;
                }
                if segment_is_removable(seg.seq_range, false, &ranges) {
                    let path = shard_dir.join(&seg.name);
                    match std::fs::remove_file(&path) {
                        Ok(()) => tracing::info!(segment = %seg.name, "removed wal segment"),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => {
                            tracing::warn!(segment = %seg.name, error = %e, "failed to remove segment")
                        }
                    }
                }
            }
        }
    })
}

/// 同步等待辅助（供测试）：等待 `view` 中无批次或超时。
#[allow(dead_code)]
pub async fn wait_until_quiet(
    view: Arc<dyn BatchStateView>,
    timeout: Duration,
) -> Result<(), LakeError> {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if view.non_terminal().is_empty() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(LakeError::Other("wait_until_quiet timeout".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use yuntun_model::batch::{is_terminal, BatchStatus};

    struct MapView(pub Mutex<HashMap<String, BatchState>>);

    impl MapView {
        fn new() -> Self {
            Self(Mutex::new(HashMap::new()))
        }
        fn insert(&self, id: &str, st: BatchStatus, created_ms: u64, range: (u64, u64)) {
            self.0.lock().unwrap().insert(
                id.to_string(),
                BatchState {
                    batch_id: id.into(),
                    client_request_id: None,
                    shard: "s0".into(),
                    time_window: "w".into(),
                    status: st,
                    s3_paths: vec![],
                    s3_upload_id: None,
                    file_size: 0,
                    row_count: 1,
                    schema_version: 1,
                    created_at_ms: created_ms,
                    wal_seq_range: range,
                },
            );
        }
    }

    impl BatchStateView for MapView {
        fn non_terminal(&self) -> Vec<BatchState> {
            self.0
                .lock()
                .unwrap()
                .values()
                .filter(|s| !s.is_terminal())
                .cloned()
                .collect()
        }
    }

    struct FixedUsage(pub f64);
    impl DiskUsage for FixedUsage {
        fn usage(&self) -> f64 {
            self.0
        }
    }

    #[test]
    fn removable_requires_all_terminal() {
        // 无非终态 → 可删
        assert!(segment_is_removable(Some((0, 100)), false, &[]));
        // 活跃 segment → 永不可删
        assert!(!segment_is_removable(Some((0, 100)), true, &[]));
        // 区间相交的非终态 batch → 不可删
        assert!(!segment_is_removable(Some((0, 100)), false, &[(50, 150)]));
        // 不相交 → 可删
        assert!(segment_is_removable(Some((0, 100)), false, &[(101, 200)]));
        // 空 segment 非活跃 → 可删
        assert!(segment_is_removable(None, false, &[(0, 10)]));
    }

    #[test]
    fn non_terminal_excludes_abort_and_committed() {
        let v = MapView::new();
        v.insert("a", BatchStatus::Committed, 0, (0, 1));
        v.insert("b", BatchStatus::Abort, 0, (2, 3));
        v.insert("c", BatchStatus::Pending, 0, (4, 5));
        v.insert("d", BatchStatus::S3Written, 0, (6, 7));
        let nt = v.non_terminal();
        assert_eq!(nt.len(), 2);
        assert!(nt.iter().all(|s| !is_terminal(s.status)));
    }

    #[tokio::test]
    async fn monitor_aborts_timed_out_batches() {
        let dir = std::env::temp_dir().join(format!("yuntun-wal-monitor-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = crate::config::WalConfig {
            batch_timeout: Duration::from_millis(500),
            monitor_interval: Duration::from_millis(30),
            ..crate::config::WalConfig::for_dir(&dir)
        };
        let wal = WalWriter::open(cfg, 0).await.unwrap();
        // 先把两个 batch 的 BatchPending 写入 WAL（模拟真实流程）
        for (id, s, e) in [("old", 0u64, 1u64), ("fresh", 2, 3)] {
            wal.append(Record::BatchPending(
                yuntun_model::wal_record::BatchPendingPayload {
                    batch_id: id.into(),
                    shard: "s0".into(),
                    window: "w".into(),
                    wal_seq_start: s,
                    wal_seq_end: e,
                    schema_version: 1,
                    client_request_id: String::new(),
                    created_at_ms: 0,
                    row_count: 1,
                },
            ))
            .await
            .unwrap();
        }
        let view = Arc::new(MapView::new());
        view.insert("old", BatchStatus::Pending, now_ms() - 10_000, (0, 1));
        view.insert("fresh", BatchStatus::Pending, now_ms(), (2, 3));

        let shutdown = CancellationToken::new();
        let handle = spawn_timeout_monitor(
            wal.clone(),
            view.clone(),
            None,
            Vec::new(),
            shutdown.clone(),
        );
        // 等 monitor 触发（interval=30ms）；fresh 年龄 ~150ms < 500ms 不会超时
        tokio::time::sleep(Duration::from_millis(150)).await;
        shutdown.cancel();
        let _ = handle.await;

        // 恢复 WAL 验证 BatchAbort 已写入
        let rec = crate::recovery::recover(wal.config(), 0, false).unwrap();
        assert!(
            !rec.states.states.contains_key("old"),
            "old batch must be aborted"
        );
        assert!(
            rec.states.states.contains_key("fresh"),
            "fresh batch must survive"
        );
    }

    #[tokio::test]
    async fn monitor_force_aborts_on_disk_watermark() {
        let dir = std::env::temp_dir().join(format!("yuntun-wal-watermark-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = crate::config::WalConfig {
            batch_timeout: Duration::from_secs(3600), // 不会触发批次超时
            monitor_interval: Duration::from_millis(30),
            disk_high_watermark: 0.80,
            ..crate::config::WalConfig::for_dir(&dir)
        };
        let wal = WalWriter::open(cfg, 0).await.unwrap();
        // 先把两个 batch 的 BatchPending 写入 WAL（模拟真实流程）
        for (id, s, e) in [("oldest", 0u64, 1u64), ("newest", 2, 3)] {
            wal.append(Record::BatchPending(
                yuntun_model::wal_record::BatchPendingPayload {
                    batch_id: id.into(),
                    shard: "s0".into(),
                    window: "w".into(),
                    wal_seq_start: s,
                    wal_seq_end: e,
                    schema_version: 1,
                    client_request_id: String::new(),
                    created_at_ms: 0,
                    row_count: 1,
                },
            ))
            .await
            .unwrap();
        }
        let view = Arc::new(MapView::new());
        view.insert("oldest", BatchStatus::Pending, now_ms() - 10_000, (0, 1));
        view.insert("newest", BatchStatus::Pending, now_ms(), (2, 3));

        let shutdown = CancellationToken::new();
        let handle = spawn_timeout_monitor(
            wal.clone(),
            view.clone(),
            Some(Arc::new(FixedUsage(0.95))), // > 80%
            Vec::new(),
            shutdown.clone(),
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
        shutdown.cancel();
        let _ = handle.await;

        let rec = crate::recovery::recover(wal.config(), 0, false).unwrap();
        assert!(
            !rec.states.states.contains_key("oldest"),
            "watermark aborts oldest"
        );
        assert!(
            rec.states.states.contains_key("newest"),
            "newest must survive"
        );
    }
}
