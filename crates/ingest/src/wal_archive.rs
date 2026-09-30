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
//! # 表级 `durability`（ADR-9 的表级模型，`§162`）
//!
//! `ArchiveConfig::all_tables = true`（默认）= **老行为**：段全归档，RPO 保护对**所有**表生效。
//! `false` = 按表：**只有"段里出现过 `durable` 表的数据"的段才归档**
//! （判定看 `Data` / `BatchPending` 记录的 `table` —— 带数据的那两类）。
//!
//! ⚠️ **粒度是"段"，不是"记录"** —— 这是 WAL 格式的硬约束，不是实现偷懒：
//! 记录帧是 `length | crc | type | payload`，**seq 由 `header.first_seq` + 位置推导**
//! （`segment.rs::decode_with_stop`）⇒ 把某几条记录**过滤掉再编码**会让它之后**所有**记录的
//! seq 位移，而 `BatchPending.wal_seq_start/end` 记的是**原 seq 区间** ⇒ 恢复时会吸收错记录
//! （静默产出错数据，比"少归档"糟得多）。要真正按记录过滤，得先改 WAL 格式让**记录自带 seq**，
//! 或按表分段 —— 两者都不是这一刀的范围（台账 `D-18`）。
//!
//! 于是今天能得到的确切语义是：
//!
//! * **一张 `durable` 表的数据一定会被归档**（哪怕节点默认不归档）✓；
//! * 但它和别的表**共段**时，同段里别的表的数据**也**会被归档（成本按段摊，是"放宽"不是"收紧"）。
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
use yuntun_model::wal_record::Record;
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
    /// **归档所有表**（默认 `true` = 老行为，见模块文档）。
    ///
    /// * `true`：段一律归档（RPO 保护对全部表生效，成本也是全部）；
    /// * `false`：按表 —— 只有"段里出现过 `durable` 表的数据"的段才归档。
    ///
    /// 默认取 `true` 是刻意的：既有部署只要配了 `archive_prefix` 就是全归档，
    /// 把它改成默认 `false` 会**静默**把那些表的 RPO 从"秒级可救"变成"丢盘即丢"
    /// —— 那是部署方没同意过的语义变化。
    pub all_tables: bool,
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
/// **这个段要不要归档**（按表口径，`§162`）。
///
/// 规则只有一条：**段里出现过 `durable` 表的数据**（`Data` / `BatchPending` 记录的 `table`
/// 满足 `is_durable`）⇒ 归档。其余情况：
///
/// * 解不开 / 读不了 ⇒ **归档**（保守方向：宁可多传，也不要"以为没数据、结果丢了"）；
/// * 只有别的表的记录 ⇒ 不归档（这正是省钱的那一半）。
///
/// 为什么只看这两类记录：它们是**带数据的**那两类（`Data` 是批次本体、`BatchPending` 是它的
/// 账目），而且都带 `table`。`BatchS3Written/Committed/Abort` 只带 `batch_id`（判不出表），
/// 它们很小、且丢了也能被 `Pending` 的恢复通路重做 ⇒ 不进判定。
pub fn segment_is_archived(
    path: &Path,
    durable: &std::collections::HashSet<String>,
) -> bool {
    let Ok((_header, records, _torn)) = yuntun_wal::segment::load_segment(path) else {
        return true; // 读不了 ⇒ 保守归档
    };
    records.iter().any(|(_, rec)| match rec {
        Record::Data(p) => durable.contains(&p.table),
        Record::BatchPending(p) => durable.contains(&p.table),
        _ => false,
    })
}

/// **归档一轮**（`filter = None` ⇒ 老行为：所有段都归档；见 [`segment_is_archived`]）。
pub async fn archive_once(
    cfg: &ArchiveConfig,
    wal_dir: &Path,
    shard: u64,
    store: &dyn ObjectStore,
    uploaded: &mut HashMap<u64, u64>,
    durable: Option<&std::collections::HashSet<String>>,
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
        // 按表口径：段里没有 durable 表的数据就不传（**这是省钱的那一半**）
        if let Some(set) = durable
            && !segment_is_archived(&e.path(), set)
        {
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
    catalog: Option<Arc<dyn yuntun_catalog::CatalogOps>>,
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
            // 按表口径：每轮解析一次"哪些表是 durable"（表集合变化很慢，不必每段都查）
            let mut durable: Option<std::collections::HashSet<String>> = None;
            if !cfg.all_tables {
                match &catalog {
                    None => tracing::warn!(
                        "按表归档已开启但没有目录句柄：本轮按**全归档**处理（宁可多传）"
                    ),
                    Some(c) => match durable_tables(c.as_ref()).await {
                        Ok(set) => durable = Some(set),
                        // 拿不到目录 ⇒ **按全归档**：`durable` 判不出来时宁可多传，
                        // 也不要"以为没数据、结果丢了"（成本 > 数据这条取舍写在这里）
                        Err(e) => tracing::warn!(
                            error = %e,
                            "解析 durable 表失败：本轮按全归档处理（宁可多传，不要丢数据）"
                        ),
                    },
                }
            }
            // `durable == None` 有两种含义，都是"全归档"：`all_tables = true`，或解析失败
            match archive_once(
                &cfg,
                &wal_dir,
                shard,
                store.as_ref(),
                &mut uploaded,
                durable.as_ref(),
            )
            .await
            {
                Ok(0) => {}
                Ok(n) => tracing::info!(segments = n, "WAL 归档：本轮上传 {n} 个段"),
                // 归档失败**不致命**（数据还在本地 WAL，下一轮会重试），但必须响亮：
                // 它意味着"这段时间里丢盘就救不回"
                Err(e) => tracing::warn!(error = %e, "WAL 归档失败（下一轮重试）"),
            }
        }
    })
}

/// 目录里 `durability = 1` 的表（全限定名，与 WAL 记录里的 `table` 同形）。
pub async fn durable_tables(
    catalog: &dyn yuntun_catalog::CatalogOps,
) -> Result<std::collections::HashSet<String>, LakeError> {
    let tables = catalog.list_tables().await?;
    Ok(tables
        .into_iter()
        .filter(|t| {
            t.ingest_config
                .as_ref()
                .is_some_and(|c| c.durability == 1)
        })
        .map(|t| t.name)
        .collect())
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

#[cfg(test)]
mod tests {
    use super::*;
    use yuntun_model::wal_record::{BatchPendingPayload, DataPayload};
    use yuntun_wal::WalWriter;

    fn data(table: &str) -> Record {
        Record::Data(DataPayload {
            table: table.into(),
            shard: "s".into(),
            schema_version: 1,
            batch_ipc: vec![1, 2, 3],
            client_request_id: String::new(),
            time_window: "2026-09-30T00".into(),
        })
    }

    fn pending(table: &str, batch: &str) -> Record {
        Record::BatchPending(BatchPendingPayload {
            batch_id: batch.into(),
            shard: "s".into(),
            window: "2026-09-30T00".into(),
            wal_seq_start: 0,
            wal_seq_end: 1,
            schema_version: 1,
            client_request_id: String::new(),
            created_at_ms: 0,
            row_count: 1,
            table: table.into(),
        })
    }

    async fn write_segment(tag: &str, records: Vec<Record>) -> (std::path::PathBuf, TempWalDir) {
        let dir = TempWalDir::new(tag);
        let wal = WalWriter::open(
            yuntun_wal::WalConfig {
                dir: dir.path().to_path_buf(),
                ..Default::default()
            },
            0,
        )
        .await
        .unwrap();
        for r in records {
            wal.append(r).await.unwrap();
        }
        let seg = yuntun_wal::segment::list_segments(&dir.path().join("shard=0"))
            .unwrap()
            .into_iter()
            .map(|(_, p)| p)
            .next()
            .expect("应有一个段文件");
        (seg, dir)
    }

    struct TempWalDir(std::path::PathBuf);
    impl TempWalDir {
        fn new(tag: &str) -> Self {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let p = std::env::temp_dir().join(format!("yuntun-arch-{tag}-{nanos}"));
            std::fs::create_dir_all(&p).unwrap();
            Self(p)
        }
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for TempWalDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn set(v: &[&str]) -> std::collections::HashSet<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    /// **按表判定的真值表**（`§162`）：决定权在"段里有没有 durable 表的 `Data`/`BatchPending`"。
    #[tokio::test]
    async fn segment_is_archived_follows_the_durable_tables() {
        let (seg_durable, _d1) = write_segment("dur", vec![data("durable_t")]).await;
        let (seg_effort, _d2) = write_segment("eff", vec![data("best_effort_t")]).await;
        let (seg_mixed, _d3) = write_segment(
            "mix",
            vec![data("best_effort_t"), data("durable_t")],
        )
        .await;
        let (seg_pending, _d4) = write_segment("pend", vec![pending("durable_t", "b1")]).await;
        let durable = set(&["durable_t"]);

        assert!(
            segment_is_archived(&seg_durable, &durable),
            "段里有 durable 表的数据 ⇒ 必须归档（这是「durable 一定被保护」的那一半）"
        );
        assert!(
            segment_is_archived(&seg_mixed, &durable),
            "混段 ⇒ 归档（粒度是段：见模块文档的共租代价）"
        );
        assert!(
            segment_is_archived(&seg_pending, &durable),
            "`BatchPending` 也带 table 且是数据账目 ⇒ 它算 Durable 表在场"
        );
        assert!(
            !segment_is_archived(&seg_effort, &durable),
            "只有 best_effort 表的数据 ⇒ 不归档（**这是省钱的那一半**）"
        );

        // 读不了的文件（不存在 / 不是段）⇒ **保守归档**：宁可多传，不要"以为没数据"
        assert!(
            segment_is_archived(std::path::Path::new("/nonexistent/x.wal"), &durable),
            "读不了的段必须按「归档」处理（保守方向）"
        );
    }
}
