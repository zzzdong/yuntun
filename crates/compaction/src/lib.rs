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
            lease_purpose: yuntun_model::meta::COMPACTION_LEASE.to_string(),
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
///
/// 末位 `lease_epoch` 是**栅栏**：干活时持有的租约代次（无租约传 0）。提交时随 op 过线，
/// 状态机发现它落后于当前水位就**拒绝**（`§82`）。
pub async fn compact_shard(
    compactor: &Compactor,
    table: &str,
    shard: &str,
    snapshot: u64,
    lease_epoch: u64,
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

    // 【T14.3 的纪律同样约束压缩**自己**】产物也是一个"**先有对象、后有目录**"的文件：
    // 必须**先登记、再上传**。否则孤儿 GC（尤其 `orphan_grace` 取小时）会在
    // `write_batch` 与 `commit_compaction` 之间把它当孤儿删掉 —— 而它一旦提交，
    // 输入就转了墓碑 ⇒ **目录指向空气 = 真丢数据**（`R-9` 同类，只是主体从写者换成压缩器）。
    //
    // 提交成功时撤销（`commit_compaction` 内）；被栅栏拒绝/中途失败时留在集合里，
    // 由在途 TTL 清扫兜底 —— 与写者的处置完全一致。
    compactor
        .catalog
        .record_in_flight(&new_batch_id, now_ms())
        .await?;

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
        // **合并产物不属于任何实例**（T14.4）—— 这里刻意**显式留空**，而不是靠
        // `..Default::default()` 恰好是空串：
        //
        // 这个字段是 `architecture §4.4` 冷热切分的依据（"冷读该实例 ≤watermark 的文件"）。
        // 产物把**多个实例**的行融在一起，归给其中任何一个，都会让那一方的热数据范围
        // 被误当成"已经覆盖了这些行" ⇒ **重复计数**（`plan §5.3-1`：不报错，只是结果变多）。
        // 留空 = "谁也不属于" ⇒ 它永远只从**冷数据**读，谁也不认领。
        source_instance: String::new(),
        ..Default::default()
    };
    let old_ids: Vec<String> = files.iter().map(|f| f.batch_id.clone()).collect();
    let new_snapshot =
        commit_compaction_files(compactor, table, shard, old_ids, new_manifest, lease_epoch)
            .await?;

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
    lease_epoch: u64,
) -> Result<u64, LakeError> {
    compactor
        .catalog
        .commit_compaction(&old_ids, vec![new_manifest], lease_epoch)
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
/// 【正确性关键】判据是 `known_batch_ids` = **保护期内的文件 ∪ 在途批次**。两项各守一个方向：
///
/// - **在途**（T14.3）：写者上传**之前**的登记 ⇒ 判据从 grace（时间假设）变成**结构可见**；
/// - **保护期**（T14.5）：墓碑（`deleted_at != 0`）在当前快照越过它之后**退出**保护集合
///   ⇒ 被合并替换掉的旧对象**能被真正回收**（在此之前它永远"已知" ⇒ 对象只增不减，
///   与 `architecture §4.6` 的"等墓碑期 + 无在途引用才真正删除"正相反）。
///
/// `grace` 在这里的角色也随之变清楚：T14.3 之后写者安全**不再依赖它**，它只剩"给还在读
/// 旧快照的读者一个窗口"这一件事 —— 也就是**墓碑期的时长**。
///
/// 在 T14.3 之前这里靠**时间假设**兜底："刚写完 S3、CommitFiles 还没落地"的文件
/// 靠 grace 期内不删来保护 —— 而一个**上传慢于 grace** 的写者（大文件 / S3 抖动 /
/// 长 GC）就会被**误删**（`R-9`：删错文件是**真丢数据**，比重复合并严重得多）。
///
/// 现在写者在上传**之前**就把 `batch_id` 登记进目录（`CatalogOps::record_in_flight`），
/// 提交时撤销 —— 于是"在途"对 GC **显式可见**，grace 退回它本来的角色：
/// 只兜住"登记了但写者已死"的残局（配合在途 TTL 清扫）。
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
/// `grace` **同时就是墓碑期的时长**（T14.5）：一个墓碑退出保护集合后，还要再"静置"
/// `grace` 才被删。设计推荐的运行值是 10–60s（`architecture §4.6`），默认给大是保守取值 —
/// 因为**调小它是安全的**：写者安全由在途登记（结构保证）承担，不再由 grace 承担。
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
                    if let Err(e) =
                        compact_shard(&compactor, &t.name, shard, snapshot, gate.epoch).await
                    {
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
    use yuntun_format::{write_batch, DataFormat};
    use yuntun_model::ops::CommitFilesRequest;
    use yuntun_model::ops::CreateTableRequest;

    fn batch() -> arrow::record_batch::RecordBatch {
        arrow::record_batch::RecordBatch::try_new(
            SArc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
            vec![SArc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap()
    }

    /// **R6 准出专项**：多写者持续写 + 孤儿 GC 开启 ⇒ **零误删**。
    ///
    /// 与上面那条回归的分工：那条钉**单次**语义（grace = 0 也不许删在途），这条跑
    /// **并发压力** —— 两个写者真写文件、真登记、真提交，同时一个 GC 以 `grace = 0`
    /// 反复扫。判据不是"GC 什么都没删"，而是三条**一起**：
    ///
    /// 1. 目录里每个**可见文件**的对象都**真的存在**（删错 = 目录指向空气 ⇒ 真丢数据）；
    /// 2. 可见文件行数之和 == 写入行数之和（批次一个不少）；
    /// 3. GC **确实干过活**（预埋的真孤儿必须消失）—— 否则这条用例可能只是"GC 没跑"。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multi_writer_plus_gc_never_deletes_a_committed_file() {
        const TABLE: &str = "public.gcstress";
        const SHARD: &str = "s0";
        const WINDOW: &str = "w";
        const WRITERS: usize = 2;
        const ROUNDS: usize = 8;
        const ROWS: u64 = 3;

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let catalog: Arc<dyn CatalogOps> = Arc::new(yuntun_catalog::MemoryCatalog::new());
        catalog
            .create_table(CreateTableRequest {
                name: "gcstress".into(),
                namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
                schema: SArc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
                partition_cols: vec![],
                default_format: "parquet".into(),
                ingest_config: Default::default(),
            })
            .await
            .unwrap();

        // 预埋一个**真孤儿**：没人登记、目录里也没有 —— 它必须被回收（防空转）
        write_batch(
            &store,
            TABLE,
            SHARD,
            WINDOW,
            "planted-orphan",
            &batch(),
            DataFormat::Parquet,
        )
        .await
        .unwrap();

        let shutdown = CancellationToken::new();
        let _gc = spawn_orphan_cleanup_with_interval(
            store.clone(),
            catalog.clone(),
            "yuntun/".to_string(),
            Duration::ZERO, // **不靠时间假设**：全靠在途登记
            Duration::from_millis(20),
            shutdown.clone(),
        );

        // 两个写者并发：**先登记 → 再写文件 → 再提交**（真实写入顺序）
        let mut tasks = Vec::new();
        for w in 0..WRITERS {
            let store = store.clone();
            let catalog = catalog.clone();
            tasks.push(tokio::spawn(async move {
                for i in 0..ROUNDS {
                    let id = format!("w{w}-b{i}");
                    catalog.record_in_flight(&id, now_ms()).await.unwrap();
                    let (path, size, rows) = write_batch(
                        &store,
                        TABLE,
                        SHARD,
                        WINDOW,
                        &id,
                        &batch(),
                        DataFormat::Parquet,
                    )
                    .await
                    .unwrap();

                    // ⚠️ 这个 sleep **不是**为了抖时序，而是**让在途窗口真实存在**：
                    // 内存存储的 PUT 几乎是瞬时的，"已上传未提交"的窗口只有微秒级 ⇒
                    // GC（20ms 一轮）未必有机会扫到它 ⇒ 用例会**假通过**（只证明 GC 跑过，
                    // 没证明它有过可乘之机）。50ms > GC 间隔 ⇒ **每个文件都必然在
                    // "文件已存在、目录还不知道"的状态下被扫到过** —— 这正是 T14.3 要挡的那一下。
                    tokio::time::sleep(Duration::from_millis(50)).await;

                    catalog
                        .commit_files(CommitFilesRequest {
                            table: TABLE.into(),
                            batch_id: id.clone(),
                            client_request_id: None,
                            client_request_ids: vec![],
                            shard: SHARD.into(),
                            time_window: WINDOW.into(),
                            files: vec![FileManifest {
                                file_path: path,
                                batch_id: id.clone(),
                                file_size: size,
                                row_count: rows,
                                table: TABLE.into(),
                                shard: SHARD.into(),
                                time_window: WINDOW.into(),
                                ..Default::default()
                            }],
                            schema_version: 1,
                            row_count: rows,
                        })
                        .await
                        .unwrap();
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        // 再让 GC 扫几轮：把"已提交"的文件也暴露在扫描之下（它们靠 files 保护）
        tokio::time::sleep(Duration::from_millis(120)).await;
        shutdown.cancel();

        // ---- 判据 ①：每个可见文件的对象**真的存在** ----
        let snapshot = catalog.current_snapshot().await;
        let visible = catalog
            .list_visible_files(TABLE, snapshot, None)
            .await
            .unwrap();
        assert_eq!(visible.len(), WRITERS * ROUNDS, "批次数不该少");
        // 目录里此刻还躺着哪些对象（一次列举，判据 ① 与 ③ 共用）
        let left: Vec<String> = list_s3_files(&store, "yuntun/")
            .await
            .unwrap()
            .into_iter()
            .map(|(p, _)| p)
            .collect();

        // ---- 判据 ①：每个可见文件的对象**真的存在**（删错 = 目录指向空气）----
        for f in &visible {
            assert!(
                left.iter().any(|p| p == &f.file_path),
                "可见文件的对象不见了（误删 = 真丢数据）：{}",
                f.file_path
            );
        }

        // ---- 判据 ②：行数一个不少 ----
        assert_eq!(
            visible.iter().map(|f| f.row_count).sum::<u64>(),
            (WRITERS * ROUNDS) as u64 * ROWS,
            "可见行数必须等于写入行数"
        );

        // ---- 判据 ③：GC 真的干过活 ----
        assert!(
            !left.iter().any(|p| p.contains("planted-orphan")),
            "埋下的真孤儿必须被回收 —— 否则这条用例是空转：{left:?}"
        );
    }

    /// **T14.5 的回收用例**：墓碑过期后，被合并替换掉的旧对象**必须**真的被回收。
    ///
    /// 这是 `§84` 那条的**镜像** —— 两个方向都要钉住，缺一个都不算对：
    ///
    /// - `§84` 证明"**不误删**"：在途的、可见的东西一个都不许动；
    /// - 这条证明"**真会删**"：保护期一过，空间真的回来。
    ///
    /// 后者特别容易悄悄退化：本刀之前 `known_batch_ids` 取的是**全部** `files`（含墓碑）
    /// ⇒ 墓碑永远"已知" ⇒ 这条用例**必然失败**（旧对象一个都不会少）。所以它不是一条
    /// "应该通过"的用例，而是一条**守着机制**的用例（反证见 `§85.3`）。
    #[tokio::test]
    async fn expired_tombstones_are_reclaimed_by_gc() {
        let catalog: Arc<MemoryCatalog> = Arc::new(MemoryCatalog::new());
        setup(&catalog).await;
        let c = compactor(catalog.clone());

        // 3 个真文件 → 合并成 1 个（旧文件转墓碑）
        let mut old_paths = Vec::new();
        for _ in 0..3 {
            let bid = uuid::Uuid::now_v7().to_string();
            let (path, size, _rows) = write_batch(
                &c.store,
                "t",
                "s0",
                "w1",
                &bid,
                &batch(),
                DataFormat::Parquet,
            )
            .await
            .unwrap();
            catalog
                .commit_files(CommitFilesRequest {
                    table: "t".into(),
                    batch_id: bid.clone(),
                    client_request_id: None,
                    client_request_ids: vec![],
                    shard: "s0".into(),
                    time_window: "w1".into(),
                    files: vec![FileManifest {
                        file_path: path.clone(),
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
            old_paths.push(path);
        }
        let snap = catalog.current_snapshot().await;
        let new_snap = compact_shard(&c, "t", "s0", snap, 0)
            .await
            .unwrap()
            .unwrap();

        let visible = catalog
            .list_visible_files("t", new_snap, Some("s0"))
            .await
            .unwrap();
        assert_eq!(visible.len(), 1, "合并后只该有 1 个可见文件");
        let merged_path = visible[0].file_path.clone();

        // 【判据】旧文件已过保护期 ⇒ 退出保护集合；合并产物仍在其中
        let known: HashSet<String> = catalog
            .known_batch_ids()
            .await
            .unwrap()
            .into_iter()
            .collect();
        for p in &old_paths {
            let id = yuntun_format::extract_batch_id(p).unwrap();
            assert!(
                !known.contains(&id),
                "过期的墓碑不该还在保护集合里（否则永远回收不了）：{id}"
            );
        }
        assert!(
            known.contains(&visible[0].batch_id),
            "合并产物是活着的，必须仍在保护集合里"
        );

        // GC 跑起来：grace = ZERO（不靠时间假设）
        let shutdown = CancellationToken::new();
        let _gc = spawn_orphan_cleanup_with_interval(
            c.store.clone(),
            catalog.clone(),
            "yuntun/".to_string(),
            Duration::ZERO,
            Duration::from_millis(20),
            shutdown.clone(),
        );

        // 旧对象**真的消失**（空间回来了），同时合并产物**还在**（别删过头）
        let store = c.store.clone();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let left: Vec<String> = list_s3_files(&store, "yuntun/")
                .await
                .unwrap()
                .into_iter()
                .map(|(p, _)| p)
                .collect();
            let olds_gone = old_paths.iter().all(|p| !left.contains(p));
            if olds_gone && left.contains(&merged_path) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "回收超时。旧文件应消失、产物应保留：left={left:?}"
            );
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        shutdown.cancel();

        // 元数据不受影响：可见文件仍是那一个产物，行数仍是输入之和
        let after = catalog
            .list_visible_files("t", catalog.current_snapshot().await, Some("s0"))
            .await
            .unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].row_count, 9, "3 个文件 × 3 行");
    }

    /// **R6 准出专项**：多节点持续写入 + 压缩 + GC 三者**同时**跑 ⇒ **文件数收敛到稳定区间**，
    /// 且**行数一个不少**。
    ///
    /// 为什么单点用例凑不出这条结论：`§84`（不误删）、`§85`（真会删）、`§86`（合并不改行集）
    /// 各自钉住一个性质，但都不回答"**持续跑下去会不会失控**" —— 而"文件数收敛"正是 R6 存在的
    /// 理由（`plan §7.5` 准出）。
    ///
    /// 形态：两个写者（两个 `source_instance`，**同一 shard** —— 多节点写同一 partition 的真实
    /// 形态）持续写；**真的**压缩循环（带租约）与**真的**孤儿 GC 同时跑。收尾后要同时满足：
    ///
    /// 1. **行数一个不少**（可见文件 `row_count` 之和 == 写入总行数）；
    /// 2. **文件数收敛**（可见文件数 ≤ `min_files + 1`，而写入批次数是它的十几倍）；
    /// 3. **每个可见文件的对象都真的存在**（这是本刀补的那个洞的直接检验：压缩产物若没登记
    ///    在途，GC 可能在 PUT→commit 之间删掉它 —— 一旦提交，输入转墓碑 ⇒ 目录指向空气）；
    /// 4. **空间真的回收 + 在途集合归零**（对象数远小于写过的批次数；保护集合 == 可见文件）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writers_compaction_and_gc_converge() {
        const TABLE: &str = "public.converge";
        const SHARD: &str = "s0";
        const WINDOW: &str = "w";
        const WRITERS: usize = 2;
        const ROUNDS: usize = 16;
        const MIN_FILES: usize = 3;
        const ROWS: u64 = 3;

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let catalog: Arc<dyn CatalogOps> = Arc::new(yuntun_catalog::MemoryCatalog::new());
        catalog
            .create_table(CreateTableRequest {
                name: "converge".into(),
                namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
                schema: SArc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
                partition_cols: vec![],
                default_format: "parquet".into(),
                ingest_config: Default::default(),
            })
            .await
            .unwrap();

        let compactor = Arc::new(Compactor {
            lease_holder: "comp-1".into(),
            cfg: CompactionConfig {
                min_files: MIN_FILES,
                interval: Duration::from_millis(40),
                // **grace = 0 是有意的**：这条用例不靠任何时间假设兜底 ——
                // 所有保护都必须来自"结构可见"（在途登记 / 保护集合）。
                // 注意：它**不是**"产物必须先登记"的确定性反证 —— 那个窗口（PUT 完成→提交）
                // 只有毫秒级，测试里几乎撞不上；那条要求由 `a_fenced_merge_leaves_its_product_protected`
                // 用"提交必被栅栏拒绝"的路径**确定性**地观察。
                orphan_grace: Duration::ZERO,
                lease_ttl: Duration::from_secs(30),
                ..Default::default()
            },
            catalog: catalog.clone(),
            store: store.clone(),
            format: DataFormat::Parquet,
        });

        let shutdown = CancellationToken::new();
        let loop_handle = spawn_compaction_loop(compactor.clone(), shutdown.clone());
        let gc_handle = spawn_orphan_cleanup_with_interval(
            store.clone(),
            catalog.clone(),
            "yuntun/".to_string(),
            Duration::ZERO,
            Duration::from_millis(20),
            shutdown.clone(),
        );

        // 两个写者持续写（**先登记 → 写文件 → 提交**），压缩循环与 GC 同时在跑
        let mut tasks = Vec::new();
        for w in 0..WRITERS {
            let store = store.clone();
            let catalog = catalog.clone();
            tasks.push(tokio::spawn(async move {
                for i in 0..ROUNDS {
                    let id = format!("w{w}-b{i}");
                    catalog.record_in_flight(&id, now_ms()).await.unwrap();
                    let (path, size, rows) = write_batch(
                        &store,
                        TABLE,
                        SHARD,
                        WINDOW,
                        &id,
                        &batch(),
                        DataFormat::Parquet,
                    )
                    .await
                    .unwrap();
                    catalog
                        .commit_files(CommitFilesRequest {
                            table: TABLE.into(),
                            batch_id: id.clone(),
                            client_request_id: None,
                            client_request_ids: vec![],
                            shard: SHARD.into(),
                            time_window: WINDOW.into(),
                            files: vec![FileManifest {
                                file_path: path,
                                batch_id: id.clone(),
                                file_size: size,
                                row_count: rows,
                                table: TABLE.into(),
                                shard: SHARD.into(),
                                time_window: WINDOW.into(),
                                // 两个写者 = 两个实例（多节点形态）
                                source_instance: format!("inst-{w}"),
                                ..Default::default()
                            }],
                            schema_version: 1,
                            row_count: rows,
                        })
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }

        // ---- 等收敛：可见文件数落到 `min_files + 1` 以内 ----
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let visible = loop {
            let files = catalog
                .list_visible_files(TABLE, catalog.current_snapshot().await, None)
                .await
                .unwrap();
            if files.len() <= MIN_FILES + 1 {
                break files;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "文件数没有收敛：可见 {} 个（写入批次数 {}）",
                files.len(),
                WRITERS * ROUNDS
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        // ---- 判据 ①：行数一个不少 ----
        let total: u64 = visible.iter().map(|f| f.row_count).sum();
        assert_eq!(
            total,
            (WRITERS * ROUNDS) as u64 * ROWS,
            "压缩持续跑了一路，行数必须一个不少"
        );

        // ---- 判据 ②：文件数收敛（写入 {} 次 → 只剩这么几个）----
        assert!(
            visible.len() <= MIN_FILES + 1,
            "文件数应收敛到 {} 以内，实际 {}",
            MIN_FILES + 1,
            visible.len()
        );
        assert!(
            visible.len() * 4 < WRITERS * ROUNDS,
            "收敛必须是真的：可见 {} vs 写入 {}",
            visible.len(),
            WRITERS * ROUNDS
        );

        // ---- 判据 ③ + ④：对象真的在；空间回收；保护集合 == 可见文件 ----
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let objects: Vec<String> = list_s3_files(&store, "yuntun/")
                .await
                .unwrap()
                .into_iter()
                .map(|(p, _)| p)
                .collect();
            let known: Vec<String> = catalog.known_batch_ids().await.unwrap();
            let all_present = visible.iter().all(|f| objects.contains(&f.file_path));
            // 空间确实回收：对象数远小于写过的批次数
            let reclaimed = objects.len() * 2 < WRITERS * ROUNDS;
            // 在途集合归零：保护集合 == 可见文件（没有残留登记，也没有过期墓碑赖着）
            let settled = known.len() == visible.len();
            if all_present && reclaimed && settled {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "收尾未达成：
  可见文件的对象都在? {all_present}
  空间已回收? {reclaimed}（对象 {} vs 写入 {}）
  保护集合==可见({})? {} （known {}）",
                objects.len(),
                WRITERS * ROUNDS,
                visible.len(),
                settled,
                known.len()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown.cancel();
        let _ = loop_handle.await;
        let _ = gc_handle.await;
    }

    /// **压缩产物也必须"先登记、后上传"**（`R-9` 在压缩侧的形态）—— 确定性地观察这一步。
    ///
    /// 为什么不能靠压测观察：那个窗口是"对象已上传 → 目录已提交"之间的毫秒级缝隙
    /// （真正决定它有多长的是提交那一次元数据往返），压测试了三次都撞不上 ⇒ 会**假通过**。
    ///
    /// 换个角度就能确定性观察：**让提交必然失败**（用一个落后于水位、必被栅栏拒绝的
    /// `lease_epoch`）。此时产物的登记**不会**被撤销，于是：
    ///
    /// - 产物对象**真的在存储里**（说明窗口确实形成过）；
    /// - 它的 `batch_id` **仍在保护集合里**（⇒ 孤儿 GC 不会在这个窗口里把它删掉）。
    ///
    /// 这后一条正是本刀的全部内容：**没有它，产物会在窗口里被当孤儿删掉，而它一旦提交，
    /// 输入就转了墓碑 ⇒ 目录指向空气 = 真丢数据**（`R-9`）。去掉登记这一步，此用例必然失败。
    #[tokio::test]
    async fn a_fenced_merge_leaves_its_product_protected() {
        let catalog: Arc<MemoryCatalog> = Arc::new(MemoryCatalog::new());
        setup(&catalog).await;
        let c = compactor(catalog.clone());

        // 3 个真文件
        let mut input_ids = Vec::new();
        for _ in 0..3 {
            let bid = uuid::Uuid::now_v7().to_string();
            let (path, size, _rows) = write_batch(
                &c.store,
                "t",
                "s0",
                "w1",
                &bid,
                &batch(),
                DataFormat::Parquet,
            )
            .await
            .unwrap();
            catalog
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
            input_ids.push(bid);
        }

        // 先立一条租约**水位**（代次 1）：随后用代次 0 提交 ⇒ 必被栅栏拒绝（`§82`）。
        catalog
            .acquire_lease("compaction", "someone-else", 60_000, now_ms())
            .await
            .unwrap();

        let snap = catalog.current_snapshot().await;
        let fenced = compact_shard(&c, "t", "s0", snap, 0).await;
        assert!(fenced.is_err(), "代次落后必须被栅栏拒绝：{fenced:?}");

        // 产物对象确实写出去了（窗口真的形成过）
        let objects: Vec<String> = list_s3_files(&c.store, "yuntun/")
            .await
            .unwrap()
            .into_iter()
            .map(|(p, _)| p)
            .collect();
        let product_ids: Vec<String> = objects
            .iter()
            .filter_map(|p| yuntun_format::extract_batch_id(p))
            .filter(|id| !input_ids.contains(id))
            .collect();
        assert_eq!(
            product_ids.len(),
            1,
            "应当有且只有一个产物对象（窗口确实形成过）：{objects:?}"
        );

        // **关键断言**：产物还在保护集合里 ⇒ GC 不会在窗口里删它
        let known: HashSet<String> = catalog
            .known_batch_ids()
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert!(
            known.contains(&product_ids[0]),
            "上传后、提交前的产物必须在保护集合里（否则孤儿 GC 会删掉它，\
             而它一旦提交、输入就转墓碑 ⇒ 目录指向空气）"
        );

        // 输入没被动过：合并没有发生（撤销在途是提交成功才做的事，这里提交失败了）
        assert_eq!(
            catalog
                .list_visible_files("t", catalog.current_snapshot().await, Some("s0"))
                .await
                .unwrap()
                .len(),
            3,
            "被栅栏拒绝的合并不得改动输入"
        );
    }

    /// **T14.3 的回归用例**（`R-9`）：在途文件**连静置期都不用等**，GC 也不许删它。
    ///
    /// 这条用例在 T14.3 之前**必然失败**：那时判据只有"已提交文件"，一个刚上传、还没
    /// `commit_files` 的文件在静置期一过就被当孤儿删掉（这里 grace 取 `ZERO` ⇒ 立刻删）。
    /// 删错文件是**真丢数据**，比重复合并严重得多 —— 所以这条要有一条**对照**（真孤儿必须被回收），
    /// 否则用例可能只是"GC 根本没跑"。
    #[tokio::test]
    async fn gc_never_deletes_an_in_flight_file_even_with_zero_grace() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let catalog: Arc<dyn CatalogOps> = Arc::new(yuntun_catalog::MemoryCatalog::new());

        // 两个真文件：一个**登记为在途**，一个纯孤儿
        let (inflight_path, _, _) = yuntun_format::write_batch(
            &store,
            "public.gc",
            "s0",
            "w",
            "inflight-batch",
            &batch(),
            yuntun_format::DataFormat::Parquet,
        )
        .await
        .unwrap();
        let (orphan_path, _, _) = yuntun_format::write_batch(
            &store,
            "public.gc",
            "s0",
            "w",
            "orphan-batch",
            &batch(),
            yuntun_format::DataFormat::Parquet,
        )
        .await
        .unwrap();
        catalog.record_in_flight("inflight-batch", now_ms()).await.unwrap();

        let shutdown = CancellationToken::new();
        let _gc = spawn_orphan_cleanup_with_interval(
            store.clone(),
            catalog.clone(),
            "yuntun/".to_string(),
            Duration::ZERO, // 静置期取 0：**不靠时间假设**，只靠"在途可见"
            Duration::from_millis(20),
            shutdown.clone(),
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        shutdown.cancel();

        let left: Vec<String> = list_s3_files(&store, "yuntun/")
            .await
            .unwrap()
            .into_iter()
            .map(|(p, _)| p)
            .collect();
        assert!(
            left.iter().any(|p| p == &inflight_path),
            "在途文件绝不删（这正是 T14.3）：{left:?}"
        );
        assert!(
            !left.iter().any(|p| p == &orphan_path),
            "真孤儿应当被回收 —— 否则这条用例是空转：{left:?}"
        );
    }

    /// **栅栏**：被接管后的"在途提交"必须被拒 —— 否则被罢黜的持有者会产出第二份合并文件。
    ///
    /// 这条用例把两处接起来：状态机的栅栏判定（`§82`）与"压缩侧真的把代次带下去"。
    #[tokio::test]
    async fn a_deposed_holder_cannot_commit_its_in_flight_merge() {
        let cat: Arc<dyn CatalogOps> = Arc::new(yuntun_catalog::MemoryCatalog::new());
        let g1 = cat
            .acquire_lease("compaction", "a", 1_000, 1_000)
            .await
            .unwrap();
        assert_eq!(g1.epoch, 1);
        let mf = |id: &str| FileManifest {
            batch_id: id.into(),
            ..Default::default()
        };

        // 持有代次 1 ⇒ 提交通过
        assert!(cat.commit_compaction(&[], vec![mf("m1")], 1).await.is_ok());

        // 租约过期、被别人接管 ⇒ 代次 2
        let g2 = cat
            .acquire_lease("compaction", "b", 5_000, 1_000)
            .await
            .unwrap();
        assert_eq!(g2.epoch, 2);

        // **被罢黜者的在途提交（还带着 1）⇒ 必须被拒**（这就是"零重复产出"的保证）
        let e = cat
            .commit_compaction(&[], vec![mf("m2")], 1)
            .await
            .unwrap_err();
        assert!(e.to_string().contains("fenced"), "{e}");

        // 新持有者的提交照常
        assert!(cat.commit_compaction(&[], vec![mf("m3")], 2).await.is_ok());
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
        let new_snap = compact_shard(&c, "t", "s0", snap, 0).await.unwrap().unwrap();

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
        assert!(compact_shard(&c, "t", "s0", snap, 0).await.unwrap().is_none());
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
