//! 后台 Compaction（详细设计 §9 / §6.5 / ADR-5）。
//!
//! 两类作业：
//! 1. **合并**：同一 shard 内文件数 > `min_files`（默认 5）→ 读入内存 → concat
//!    → 重写单个大文件（Batch 编码）→ 新文件 CommitFiles + 旧文件 deleted_at
//!    → 提交一个新的可见快照（L2）
//! 2. **孤儿清理**：S3 有但 Meta 无 batch_id 的文件 + S3 无但 Meta 有的文件，
//!    静置 1h 后删除（§9.1）
//!
//! 冲突安全性（§9.2）：Compaction 不处理 drop_shard 竞争 —— Catalog 层
//! `commit_compaction` 原子地"旧文件 deleted_at + 新文件 valid_from = snapshot+1"。

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use yuntun_catalog::CatalogOps;
use yuntun_model::error::LakeError;
use yuntun_model::meta::FileManifest;

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// 同 shard 触发合并的最少文件数（默认 5）
    pub min_files: usize,
    /// 合并产物最大行数（超过则拆分；MVP 单文件全量）
    pub max_rows_per_output: u64,
    /// 作业循环间隔（默认 60s）
    pub interval: Duration,
    /// 孤儿文件静置期（默认 1h）
    pub orphan_grace: Duration,
    /// 租约的**用途键**（`§81`）：全局作业的单持有者仲裁按它分组。
    ///
    /// 今天是固定的 `compaction`；**分片粒度**以后只需把键改成 `compaction:{table}:{shard}` ——
    /// 协议不用动（`purpose` 本来就是字符串）。
    pub lease_purpose: String,
    /// 租约期限（默认 30s）：到期即失效（接管方不必等它点头）。
    ///
    /// 与心跳口径无关：心跳是**发现**（秒级、不进 raft），租约是**授权**（进 raft、低频）。
    /// 期限越短接管越快，代价是续租（一次 raft 写）越频繁。
    pub lease_ttl: Duration,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            min_files: 5,
            max_rows_per_output: 5_000_000,
            interval: Duration::from_secs(60),
            orphan_grace: Duration::from_secs(3600),
            lease_purpose: "compaction".to_string(),
            lease_ttl: Duration::from_secs(30),
        }
    }
}

/// 合并依赖。
///
/// 【接缝】`catalog` 是 `Arc<dyn CatalogOps>` 而**不是** `Arc<MemoryCatalog>`：
/// "Compaction 与 Catalog 同进程"是部署事实，但版本演进（R2/R3：Catalog 转 gRPC）
/// 不能因此返工。L2 提交走 [`CatalogOps::commit_compaction`]、孤儿对账走
/// [`CatalogOps::known_batch_ids`]，两者都已在 trait 上（`plan.md §5.1-B`）。
pub struct Compactor {
    pub cfg: CompactionConfig,
    pub catalog: Arc<dyn CatalogOps>,
    pub store: Arc<dyn object_store::ObjectStore>,
    pub format: yuntun_format::DataFormat,
    /// **租约持有者身份**（= 本进程的 `instance_id`）。
    ///
    /// 必须**显式**且**唯一**：状态机把"同一持有者重复取租约"当作**幂等**（代次不变、期限顺延），
    /// 于是两个进程若共用一个身份，就会**双双拿到租约**。默认值在这里帮不上忙 ——
    /// 默认 `"compactor"` 恰好是最危险的那个值。
    pub lease_holder: String,
}

/// 墙钟毫秒（**发起方打点**：状态机不读钟，时刻随 op 过线 —— `§81`）。
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// **租约门**（T14.1）：压缩作业的"先拿租约再干活"。
///
/// 三条纪律（都来自 `§81`）：
///
/// 1. **拿不到就空转**：别人正持有 ⇒ 本轮跳过（不是错误，重复合并只是浪费）；
/// 2. **续租被拒 ⇒ 立刻停手**：`renew` 返回 false 意味着代次已被别人推进
///    （"我的租约已经不属于我了"）—— 这是时钟偏斜下唯一的防线；
/// 3. **元数据面不可达 ⇒ 也不敢干**：拿不到权威的续租回执时，宁可停一轮，
///    也不能凭"我觉得我还没到期"继续合并。
struct LeaseGate {
    purpose: String,
    holder: String,
    ttl_ms: u64,
    /// 0 = 尚未持有
    epoch: u64,
    expires_at_ms: u64,
}

impl LeaseGate {
    fn new(compactor: &Compactor) -> Self {
        Self {
            purpose: compactor.cfg.lease_purpose.clone(),
            holder: compactor.lease_holder.clone(),
            ttl_ms: compactor.cfg.lease_ttl.as_millis() as u64,
            epoch: 0,
            expires_at_ms: 0,
        }
    }

    /// 这一轮是否由本节点干活。
    async fn acquire_or_renew(&mut self, catalog: &Arc<dyn CatalogOps>) -> bool {
        let now = now_ms();
        if self.epoch == 0 {
            return match catalog
                .acquire_lease(&self.purpose, &self.holder, now, self.ttl_ms)
                .await
            {
                Ok(g) if g.granted => {
                    self.epoch = g.epoch;
                    self.expires_at_ms = g.expires_at_ms;
                    true
                }
                Ok(_) => false, // 别人持有：空转
                Err(e) => {
                    tracing::warn!(error = %e, "取租约失败（元数据面不可达？），本轮不合并");
                    false
                }
            };
        }

        // 还剩超过 1/3 期限就不续：别让"每轮都写一次 raft"成为常态
        if now + self.ttl_ms / 3 < self.expires_at_ms {
            return true;
        }
        match catalog
            .renew_lease(&self.purpose, &self.holder, self.epoch, now, self.ttl_ms)
            .await
        {
            Ok(true) => {
                self.expires_at_ms = now + self.ttl_ms;
                true
            }
            Ok(false) => {
                tracing::warn!(
                    purpose = %self.purpose,
                    holder = %self.holder,
                    epoch = self.epoch,
                    "续租被拒：本节点已不是租约持有者，停手（下一轮重新申请）"
                );
                self.epoch = 0;
                self.expires_at_ms = 0;
                false
            }
            Err(e) => {
                tracing::warn!(error = %e, "续租失败（元数据面不可达？），本轮不合并");
                self.epoch = 0;
                self.expires_at_ms = 0;
                false
            }
        }
    }
}

/// 合并单个 shard 的文件（详细设计 §9.1）。
/// 返回新快照号；无可合并文件返回 None。
pub async fn compact_shard(
    compactor: &Compactor,
    table: &str,
    shard: &str,
    snapshot: u64,
) -> Result<Option<u64>, LakeError> {
    let files = compactor
        .catalog
        .list_visible_files(table, snapshot, Some(shard))
        .await?;
    if files.len() < compactor.cfg.min_files {
        return Ok(None);
    }

    // 读入全部文件 → concat（MVP：单文件输出；超限拆分留给 Phase 0.5）
    let mut batches = Vec::new();
    for f in &files {
        let fb =
            yuntun_format::read_batch(&compactor.store, &f.file_path, compactor.format).await?;
        for b in fb {
            batches.push(b);
        }
    }
    if batches.is_empty() {
        return Ok(None);
    }
    // schema 对齐（多版本文件共存 → 取最大版本 schema，flush::align_batch 同规则）
    batches.sort_by_key(|b| b.schema().fields().len());
    let target = batches.last().unwrap().schema();
    // 简化：concat_batches 要求同 schema；版本差异由 flush 层已对齐，
    // 这里直接 concat（失败则跳过本轮，不阻塞写入路径）
    let merged = match arrow::compute::concat_batches(&target, &batches) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(table, shard, error = %e, "compaction concat failed, skip this round");
            return Ok(None);
        }
    };

    // 写新文件（batch_id = 随机 UUIDv7，ADR-4；幂等由 Meta 层保证）
    let new_batch_id = uuid::Uuid::now_v7().to_string();
    let (new_path, new_size, _rows) = yuntun_format::write_batch(
        &compactor.store,
        table,
        shard,
        files[0].time_window.as_str(),
        &new_batch_id,
        &merged,
        compactor.format,
    )
    .await?;

    // Meta：新文件 commit + 旧文件 deleted_at（L2，原子）
    let new_manifest = FileManifest {
        file_path: new_path.clone(),
        batch_id: new_batch_id.clone(),
        row_count: merged.num_rows() as u64,
        file_size: new_size,
        table: table.to_string(),
        shard: shard.to_string(),
        time_window: files[0].time_window.clone(),
        ..Default::default()
    };
    let old_ids: Vec<String> = files.iter().map(|f| f.batch_id.clone()).collect();
    let new_snapshot =
        commit_compaction_files(compactor, table, shard, old_ids, new_manifest).await?;

    tracing::info!(
        table,
        shard,
        merged_files = files.len(),
        new_path = %new_path,
        new_snapshot,
        "shard compacted"
    );
    Ok(Some(new_snapshot))
}

/// 通过 [`CatalogOps::commit_compaction`] 提交（L2：旧文件 `deleted_at` + 新文件
/// `valid_from = snapshot+1`，一次原子完成）。实现无关。
async fn commit_compaction_files(
    compactor: &Compactor,
    _table: &str,
    _shard: &str,
    old_ids: Vec<String>,
    new_manifest: FileManifest,
) -> Result<u64, LakeError> {
    compactor
        .catalog
        .commit_compaction(&old_ids, vec![new_manifest])
        .await
}

/// 孤儿文件判定（§9.1 / §12.2.1）：
/// - S3 有、Meta 无 batch_id → 孤儿（崩溃于写 S3 后、CommitFiles 前）
/// - S3 无、Meta 有 → 数据丢失，告警（人工介入）
pub fn classify_orphans(
    s3_paths: HashSet<String>,
    known_batch_ids: &HashSet<String>,
    last_modified_ms: u64,
    grace: Duration,
) -> Vec<String> {
    // 静置期检查用 last_modified（MVP：所有未匹配文件统一返回，
    // grace 过滤由调用方依据 last_modified 执行）
    let _ = (last_modified_ms, grace);
    s3_paths
        .into_iter()
        .filter(|p| match yuntun_format::extract_batch_id(p) {
            Some(id) => !known_batch_ids.contains(&id),
            None => true,
        })
        .collect()
}

/// 后台孤儿清理循环（§9.1 / §12.2.1）：
/// 每 interval 列举 `prefix` 下全部 S3 对象 → 与 Meta 已知 batch_id 对账
/// → 未匹配的文件记录"首次发现时间"，**静置超过 grace 才删除**。
///
/// 【正确性关键】grace 期内绝不删除 —— 防止误删"刚写完 S3、CommitFiles 还没落地"
/// 的进行中批次文件；恢复路径（resume_recovered）在启动阶段先行重建 Meta，
/// 因此重启场景下 known_batch_ids 在清理循环启动前已就绪。
pub fn spawn_orphan_cleanup(
    store: Arc<dyn object_store::ObjectStore>,
    catalog: Arc<dyn CatalogOps>,
    prefix: String,
    grace: Duration,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    spawn_orphan_cleanup_with_interval(
        store,
        catalog,
        prefix,
        grace,
        Duration::from_secs(60),
        shutdown,
    )
}

/// 同 [`spawn_orphan_cleanup`]，但可指定轮询间隔。
///
/// 生产用 60s（见上，`grace` 才是安全边界，间隔只影响回收及时性）；
/// **测试需要能把它压到毫秒级** —— 否则一条"不误删已知文件"的用例要跑一分钟以上，
/// 没人会去跑它，等于没有防线。
pub fn spawn_orphan_cleanup_with_interval(
    store: Arc<dyn object_store::ObjectStore>,
    catalog: Arc<dyn CatalogOps>,
    prefix: String,
    grace: Duration,
    interval: Duration,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // batch_id → 首次发现为孤儿的时刻（Unix 毫秒）
        let mut first_seen: std::collections::HashMap<String, u64> =
            std::collections::HashMap::new();
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }
            let objects = match yuntun_format::list_objects(&store, &prefix).await {
                Ok(o) => o,
                Err(e) => {
                    tracing::error!(error = %e, "orphan cleanup: list failed");
                    continue;
                }
            };
            let known: HashSet<String> = match catalog.known_batch_ids().await {
                Ok(ids) => ids.into_iter().collect(),
                Err(e) => {
                    // 【安全】对账基准拿不到时**绝不删除任何对象**：宁可留垃圾文件
                    tracing::error!(error = %e, "orphan cleanup: known_batch_ids failed, skip this round");
                    continue;
                }
            };
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let mut removed = 0usize;
            let mut current: HashSet<String> = HashSet::new();
            for (path, _size) in &objects {
                let Some(bid) = yuntun_format::extract_batch_id(path) else {
                    continue;
                };
                current.insert(bid.clone());
                if known.contains(&bid) {
                    first_seen.remove(&bid);
                    continue;
                }
                let seen_at = *first_seen.entry(bid.clone()).or_insert(now_ms);
                if now_ms.saturating_sub(seen_at) < grace.as_millis() as u64 {
                    continue; // 静置期内，不动
                }
                match yuntun_store::delete(store.as_ref(), path).await {
                    Ok(()) => {
                        removed += 1;
                        first_seen.remove(&bid);
                        tracing::info!(path = %path, batch_id = %bid, "orphan file removed");
                    }
                    Err(e) => {
                        tracing::warn!(path = %path, error = %e, "orphan delete failed")
                    }
                }
            }
            // 清理已消失对象的观察记录
            first_seen.retain(|bid, _| current.contains(bid));
            if removed > 0 {
                tracing::info!(removed, "orphan cleanup round done");
            }
        }
    })
}

/// 启动后台 Compaction 循环（每 interval：逐表逐 shard 检查合并）。
pub fn spawn_compaction_loop(
    compactor: Arc<Compactor>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(compactor.cfg.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut gate = LeaseGate::new(&compactor);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    // 优雅退出：把租约**还回去** —— 接手方不必干等 TTL（`§81`）。
                    // 代次为 0（还没持有）时这次调用空转，无副作用。
                    let _ = compactor
                        .catalog
                        .release_lease(
                            &compactor.cfg.lease_purpose,
                            &compactor.lease_holder,
                            gate.epoch,
                        )
                        .await;
                    break;
                }
                _ = interval.tick() => {}
            }
            // **先拿租约再干活**（T14.1）：拿不到就空转 —— 别的节点在干，重复合并只是浪费。
            if !gate.acquire_or_renew(&compactor.catalog).await {
                continue;
            }
            let Ok(tables) = compactor.catalog.list_tables().await else {
                continue;
            };
            let snapshot = compactor.catalog.current_snapshot().await;
            for t in &tables {
                // MVP：按 shard 聚合文件清单（list_visible_files 无 group-by，
                // 一次拉全量后内存分组）
                let files = match compactor
                    .catalog
                    .list_visible_files(&t.name, snapshot, None)
                    .await
                {
                    Ok(f) => f,
                    Err(e) => {
                        tracing::error!(error = %e, table = %t.name, "list files failed");
                        continue;
                    }
                };
                let mut by_shard: std::collections::HashMap<&str, Vec<&FileManifest>> =
                    std::collections::HashMap::new();
                for f in &files {
                    by_shard.entry(f.shard.as_str()).or_default().push(f);
                }
                for (shard, group) in by_shard {
                    if group.len() < compactor.cfg.min_files {
                        continue;
                    }
                    if let Err(e) = compact_shard(&compactor, &t.name, shard, snapshot).await {
                        tracing::error!(error = %e, table = %t.name, shard, "compaction failed");
                    }
                }
            }
        }
    })
}

/// 列举 S3 文件（孤儿清理辅助）。
pub async fn list_s3_files(
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> Result<Vec<(String, u64)>, LakeError> {
    yuntun_format::list_objects(store, prefix).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as SArc;
    use yuntun_catalog::MemoryCatalog;
    use yuntun_model::ops::CommitFilesRequest;
    use yuntun_model::ops::CreateTableRequest;

    fn batch() -> arrow::record_batch::RecordBatch {
        arrow::record_batch::RecordBatch::try_new(
            SArc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
            vec![SArc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap()
    }

    fn compactor(catalog: Arc<dyn CatalogOps>) -> Compactor {
        Compactor {
            lease_holder: "test-compactor".to_string(),
            cfg: CompactionConfig {
                min_files: 3,
                ..Default::default()
            },
            catalog,
            store: Arc::new(object_store::memory::InMemory::new()),
            format: yuntun_format::DataFormat::Parquet,
        }
    }

    async fn setup(catalog: &MemoryCatalog) {
        catalog
            .create_table(CreateTableRequest {
                name: "t".into(),
                namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
                schema: SArc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
                partition_cols: vec![],
                default_format: "parquet".into(),
                ingest_config: yuntun_model::meta::IngestConfig {
                    require_idempotency_key: false,
                    ..Default::default()
                },
            })
            .await
            .unwrap();
    }

    // T3.x 扩展：合并提交 L2 语义（旧文件 deleted_at + 新文件可见）
    #[tokio::test]
    async fn compact_merges_files_atomically() {
        let catalog: Arc<MemoryCatalog> = Arc::new(MemoryCatalog::new());
        setup(&catalog).await;
        let c = compactor(catalog.clone());

        // 写 3 个文件并提交
        let mut batch_ids = Vec::new();
        for _i in 0..3 {
            let bid = uuid::Uuid::now_v7().to_string();
            let (path, size, _rows) = yuntun_format::write_batch(
                &c.store,
                "t",
                "s0",
                "w1",
                &bid,
                &batch(),
                yuntun_format::DataFormat::Parquet,
            )
            .await
            .unwrap();
            let r = catalog
                .commit_files(CommitFilesRequest {
                    table: "t".into(),
                    batch_id: bid.clone(),
                    client_request_id: None,
                    client_request_ids: vec![],
                    shard: "s0".into(),
                    time_window: "w1".into(),
                    files: vec![FileManifest {
                        file_path: path,
                        batch_id: bid.clone(),
                        file_size: size,
                        row_count: 3,
                        ..Default::default()
                    }],
                    schema_version: 1,
                    row_count: 3,
                })
                .await
                .unwrap();
            assert!(r.accepted);
            batch_ids.push(bid);
        }

        let snap = catalog.current_snapshot().await;
        // 合并前 3 个可见文件
        assert_eq!(
            catalog
                .list_visible_files("t", snap, Some("s0"))
                .await
                .unwrap()
                .len(),
            3
        );

        // 执行合并
        let new_snap = compact_shard(&c, "t", "s0", snap).await.unwrap().unwrap();

        // 旧快照仍见 3 个（快照隔离）；新快照只见 1 个合并文件
        assert_eq!(
            catalog
                .list_visible_files("t", snap, Some("s0"))
                .await
                .unwrap()
                .len(),
            3
        );
        let after = catalog
            .list_visible_files("t", new_snap, Some("s0"))
            .await
            .unwrap();
        assert_eq!(after.len(), 1);
        // 合并文件行数 = 9
        assert_eq!(after[0].row_count, 9);
        let _ = batch_ids;
    }

    #[tokio::test]
    async fn compact_skips_when_below_threshold() {
        let catalog: Arc<MemoryCatalog> = Arc::new(MemoryCatalog::new());
        setup(&catalog).await;
        let c = compactor(catalog.clone());
        let snap = catalog.current_snapshot().await;
        // 无文件 → None
        assert!(compact_shard(&c, "t", "s0", snap).await.unwrap().is_none());
    }

    #[test]
    fn orphan_classification() {
        let mut s3 = HashSet::new();
        s3.insert(
            "yuntun/t/dt=w/shard=s0/018f0000-0000-7000-8000-000000000001.parquet".to_string(),
        );
        s3.insert(
            "yuntun/t/dt=w/shard=s0/018f0000-0000-7000-8000-000000000002.parquet".to_string(),
        );
        let mut known = HashSet::new();
        known.insert("018f0000-0000-7000-8000-000000000002".to_string());
        let orphans = classify_orphans(s3, &known, 0, Duration::from_secs(3600));
        assert_eq!(orphans.len(), 1);
        assert!(orphans[0].contains("000000000001"));
    }
}
