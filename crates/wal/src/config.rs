//! WAL 配置（详细设计 §11 配置项清单 [wal] 节）。

use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct WalConfig {
    /// WAL 根目录：`{dir}/shard={shard_id}/...`
    pub dir: PathBuf,
    /// segment 最大字节数（默认 64MB）
    pub segment_max_size: u64,
    /// segment 最大存在时间（默认 1h）
    pub segment_max_age: Duration,
    /// 组提交窗口（默认 1ms，§5.3.5）
    pub group_commit_window: Duration,
    /// 组提交最大批条数（默认 1024）
    pub group_commit_max_batch: usize,
    /// 【v11】批次超时 → BatchAbort（默认 30min，§5.3.6.1）
    pub batch_timeout: Duration,
    /// 【v11】磁盘保护水位（默认 0.80，强制 abort 最老未完成 batch）
    pub disk_high_watermark: f64,
    /// 批次超时监控间隔（默认 60s；测试可调小）
    pub monitor_interval: Duration,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("/var/lib/ingestor/wal"),
            segment_max_size: 64 * 1024 * 1024,
            segment_max_age: Duration::from_secs(3600),
            group_commit_window: Duration::from_millis(1),
            group_commit_max_batch: 1024,
            batch_timeout: Duration::from_secs(30 * 60),
            disk_high_watermark: 0.80,
            monitor_interval: Duration::from_secs(60),
        }
    }
}

impl WalConfig {
    pub fn for_dir(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            ..Default::default()
        }
    }

    pub fn shard_dir(&self, shard_id: u64) -> PathBuf {
        self.dir.join(format!("shard={shard_id}"))
    }
}

/// 解析 WAL 目录总字节数（用于磁盘水位判断的默认实现）。
pub fn dir_size_bytes(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(rd) = std::fs::read_dir(path) {
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                total += dir_size_bytes(&p);
            } else if let Ok(md) = entry.metadata() {
                total += md.len();
            }
        }
    }
    total
}
