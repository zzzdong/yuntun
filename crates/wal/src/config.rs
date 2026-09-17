//! WAL 配置（详细设计 §11 配置项清单 [wal] 节）。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// `fsync()` 的**故障注入点位**（`design.md` §12.3 #8）。
///
/// 断电只会落在两个瞬时状态之一，二者对"数据是否还在"的答案**相反** ——
/// 这正是需要在两个点位分别注入的原因：
///
/// | 点位 | 盘上有什么 | 客户端拿到什么 |
/// |---|---|---|
/// | [`FsyncPoint::BeforeSync`] | 字节可能**从未落盘** | 没有 ack（应该失败） |
/// | [`FsyncPoint::AfterSync`] | 字节**确定落盘** | 仍然没有 ack |
///
/// 第二行是"至少一次"的来源：客户端没拿到 ack 会重试，而数据其实已经在盘上 ——
/// 所以写入方必须有**幂等键**（§7.3），否则重试就是重复计数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPoint {
    /// 记录已 append 进文件，**尚未** fsync
    BeforeSync,
    /// 已 fsync 成功，**尚未** ack 客户端
    AfterSync,
}

/// 注入事件。
#[derive(Debug, Clone)]
pub struct FsyncEvent {
    pub point: FsyncPoint,
    /// 当前 segment 文件
    pub path: PathBuf,
    /// 本次批量写入**之前**的文件长度 = 此刻"确定已持久化"的字节边界。
    ///
    /// `BeforeSync` 时用它模拟掉电（`set_len(synced_len)` = 未 fsync 的字节从未落盘）；
    /// `AfterSync` 时它就是本次写入后的边界（因为刚 fsync 完）。
    pub synced_len: u64,
    /// 当前文件长度（含本次尚未 fsync 的写入）
    pub file_len: u64,
    /// 本次批量条数
    pub batch_len: usize,
}

/// fsync 注入钩子（**仅供测试**；生产为 `None`，路径上只有一次 `Option` 判断）。
///
/// 包一层而不是直接用 `Arc<dyn Fn>`：让 `WalConfig` 保持 `Debug`/`Clone`
/// （闭包没有 `Debug`，而配置项到处被打印）。
#[derive(Clone)]
pub struct FsyncHook(Arc<dyn Fn(FsyncEvent) + Send + Sync>);

impl FsyncHook {
    pub fn new(f: impl Fn(FsyncEvent) + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }
    pub fn call(&self, ev: FsyncEvent) {
        (self.0)(ev)
    }
}

impl std::fmt::Debug for FsyncHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<fsync-hook>")
    }
}

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
    /// fsync 故障注入钩子（默认 `None` = 生产路径）
    pub fsync_hook: Option<FsyncHook>,
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
            fsync_hook: None,
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
