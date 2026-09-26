//! **WAL 归档到共享存储**（ADR-9 的 `durable` 档，`§125`）。
//!
//! # 要保护的窗口（先说清"为什么需要它"）
//!
//! 客户端拿到成功的**唯一保证**是"这批数据已经 fsync 进**本节点私有**的 WAL" ——
//! `DoPut` 在 `Ingest` 返回（内部只做 WAL 组提交 fsync）之后立刻 ack，而
//! seal → 上传对象存储 → `commit_files`（raft）**全是后台异步**发生的：实测窗口
//! `time_threshold(5s) + 相位铺开(30s) + PUT + raft 往返`（`architecture.md` 给的上界 ~31.4s）。
//!
//! ⇒ 这段窗口里"整盘丢失"（磁盘坏 / 机器没了 / 误删目录）**ack 过的数据就没了** ——
//! 这正是 `best_effort` 档明确承认的那句话："未提交数据丢失"。
//!
//! `durable` 档要做的就是补上这段窗口：**把 WAL 段持续归档到共享存储**，
//! 于是"盘没了"之后还能把数据捞回来（走**既有**恢复通路，见 [`restore`]）。
//!
//! # RPO 的界（写清楚，不吹）
//!
//! **RPO ≤ 归档间隔 + 一次上传时延**（默认间隔 1s）。要"真正的 0"就得**同步归档**
//! （ack 之前先等 S3 PUT）—— 那是另一个取舍（拿写入时延换 RPO），本实现**不选它**。
//!
//! # 为什么连"正在写的段"也归档
//!
//! 段要 64MB / 1h 才轮转，只归档**已轮转**的段 ⇒ RPO 变成**小时级**，等于没做。
//! 所以这里对**所有**段周期性上传其**当前字节**。这不会把归档搞坏：撕裂的尾巴由 WAL 自己的
//! **CRC + `repair_torn_tails`** 兜住 —— 崩溃时"正在写的段"本来就要走这套处理
//! （见 `crates/wal/src/recovery.rs`）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use tokio_util::sync::CancellationToken;
use yuntun_model::error::LakeError;

/// WAL 归档配置（`§125`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveConfig {
    /// 归档前缀（共享存储上的一级目录）。
    pub prefix: String,
    /// 本节点实例 id。
    ///
    /// **为什么必须带上它**：WAL 的路径里**没有**实例维度（`{dir}/shard={shard}/{seq}.wal`，
    /// 而实际只用 `shard=0`）⇒ 若两个节点归档到同一前缀，`seg=…001.wal` 会**互相覆盖**。
    pub instance_id: String,
    /// 归档间隔。
    pub interval: Duration,
}

impl ArchiveConfig {
    /// 归档对象的键：`{prefix}/instance={id}/shard={shard}/seg={seq:020}.wal`。
    pub fn object_path(&self, shard: u64, seq: u64) -> String {
        format!(
            "{}/instance={}/shard={}/seg={:020}.wal",
            self.prefix.trim_end_matches('/'),
            self.instance_id,
            shard,
            seq
        )
    }

    /// 归档前缀（拉回时用来列举）。
    pub fn list_prefix(&self, shard: u64) -> String {
        format!(
            "{}/instance={}/shard={}/",
            self.prefix.trim_end_matches('/'),
            self.instance_id,
            shard
        )
    }
}

/// 段文件名 → `seq`。**两种形态都要认**：
/// * 本地段：`{seq:020}.wal`（`WalWriter` 的命名）；
/// * 归档键：`seg={seq:020}.wal`（[`ArchiveConfig::object_path`] 的命名）。
///
/// ⚠️ 只认前者会让 `restore` **一个段都拉不回来**（静默：`list_all` 有对象、但每个都解析失败
/// ⇒ 跳过）—— 本模块的验收用例正是先红在这里的。
fn seq_of(file_name: &str) -> Option<u64> {
    let stem = file_name.strip_suffix(".wal")?;
    stem.strip_prefix("seg=").unwrap_or(stem).parse().ok()
}

/// **归档一轮**：把 `{wal_dir}/shard={shard}` 下**所有**段上传（只传变了的那几个）。
///
/// `uploaded` 是"seq → 上次上传的字节数"的记忆：长度没变就不重传；当前段每轮都在长，
/// 所以它会**周期性重传**（这是有意的 —— 见模块文档"为什么连正在写的段也归档"）。
///
/// 返回本轮实际上传的段数（0 = 什么都没变）。WAL 目录不存在时同样返回 0（还没写过）。
pub async fn archive_once(
    cfg: &ArchiveConfig,
    wal_dir: &Path,
    shard: u64,
    store: &dyn ObjectStore,
    uploaded: &mut HashMap<u64, u64>,
) -> Result<usize, LakeError> {
    let dir = wal_dir.join(format!("shard={shard}"));
    let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
        return Ok(0); // 目录还没有 = 没东西可归档
    };
    let mut round = 0usize;
    while let Some(e) = entries
        .next_entry()
        .await
        .map_err(|e| LakeError::Io(e.to_string()))?
    {
        let Some(seq) = e.file_name().to_str().and_then(seq_of) else {
            continue; // CURRENT / CURRENT.tmp 等非段文件
        };
        let bytes = match tokio::fs::read(e.path()).await {
            Ok(b) => b,
            // 段刚被清理/轮转掉了：跳过（下一轮再看）
            Err(_) => continue,
        };
        let len = bytes.len() as u64;
        if len == 0 || uploaded.get(&seq) == Some(&len) {
            continue;
        }
        yuntun_store::put_bytes(store, &cfg.object_path(shard, seq), bytes).await?;
        uploaded.insert(seq, len);
        round += 1;
    }
    Ok(round)
}

/// 归档循环（后台任务）。`shutdown` 触发后退出 —— 与其它后台任务同款。
pub fn spawn_archiver(
    cfg: ArchiveConfig,
    wal_dir: PathBuf,
    shard: u64,
    store: Arc<dyn ObjectStore>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut uploaded: HashMap<u64, u64> = HashMap::new();
        let mut interval = tokio::time::interval(cfg.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }
            match archive_once(&cfg, &wal_dir, shard, store.as_ref(), &mut uploaded).await {
                Ok(0) => {}
                Ok(n) => tracing::info!(segments = n, "WAL 归档：本轮上传 {n} 个段"),
                // 归档失败**不致命**（数据还在本地 WAL，下一轮会重试），但必须响亮：
                // 它意味着"这段时间里丢盘就救不回"
                Err(e) => tracing::warn!(error = %e, "WAL 归档失败（下一轮重试）"),
            }
        }
    })
}

/// **从归档把段拉回本地 WAL 目录** —— 必须在 `WalWriter::open` **之前**调用。
///
/// # 为什么是"拉回来再走既有通路"，而不是"从 S3 流式重放"
///
/// WAL 的读取面（`WalReader::new(shard_dir)` / `recovery::recover` / `segment.rs`）**全部绑本地
/// FS 路径**，流式重放等于新写一套 reader + CRC 路径；而拉回来之后，DDL 重放
/// （`replay_wal_ddl`）、`resume_recovered` 的 `Pending` 分支（重做上传 + 提交）、
/// accumulator 重吸收**一行都不用改** —— `durable` 的重建**恰好就是**今天 `best_effort`
/// 的那条恢复通路，只是 WAL 的来源从"本地残留"变成"从归档拉回"。
///
/// # 不覆盖更新的本地数据
///
/// 本地已有同名段且**不小于**归档版本 ⇒ 跳过（归档是周期性的，本地可能已经往后写了；
/// 拿旧归档盖新数据是**真丢数据**）。
///
/// 返回拉回的段数（0 = 没有归档）。
pub async fn restore(
    cfg: &ArchiveConfig,
    wal_dir: &Path,
    shard: u64,
    store: &dyn ObjectStore,
) -> Result<usize, LakeError> {
    let dir = wal_dir.join(format!("shard={shard}"));
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|e| LakeError::Io(e.to_string()))?;

    let mut n = 0usize;
    for o in yuntun_store::list_all(store, &cfg.list_prefix(shard)).await? {
        let Some(seq) = o.path.rsplit('/').next().and_then(seq_of) else {
            continue;
        };
        let local = dir.join(format!("{seq:020}.wal"));
        if let Ok(m) = tokio::fs::metadata(&local).await
            && m.len() >= o.size
        {
            continue;
        }
        let bytes = yuntun_store::get_bytes(store, &o.path).await?;
        tokio::fs::write(&local, bytes)
            .await
            .map_err(|e| LakeError::Io(e.to_string()))?;
        n += 1;
    }
    Ok(n)
}
