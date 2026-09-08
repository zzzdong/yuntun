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
use yuntun_catalog::{CatalogOps, MemoryCatalog};
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
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            min_files: 5,
            max_rows_per_output: 5_000_000,
            interval: Duration::from_secs(60),
            orphan_grace: Duration::from_secs(3600),
        }
    }
}

/// 合并依赖。
/// 【阶段 0 约束】Compaction 是内部组件，与 MemoryCatalog 同进程；
/// 阶段 1 Catalog 引入 gRPC 后，L2 提交改走 `CommitCompaction` RPC。
pub struct Compactor {
    pub cfg: CompactionConfig,
    pub catalog: Arc<MemoryCatalog>,
    pub store: Arc<dyn object_store::ObjectStore>,
    pub format: yuntun_format::DataFormat,
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
        let fb = yuntun_format::read_batch(&compactor.store, &f.file_path, compactor.format)
            .await?;
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
    let new_snapshot = commit_compaction_files(compactor, table, shard, old_ids, new_manifest).await?;

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

/// 通过 [`MemoryCatalog::commit_compaction`] 提交（若 catalog 是其它实现，
/// MVP 直接报错 —— Compaction 是内部组件，与 MemoryCatalog 同进程）。
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
    catalog: Arc<MemoryCatalog>,
    prefix: String,
    grace: Duration,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // batch_id → 首次发现为孤儿的时刻（Unix 毫秒）
        let mut first_seen: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
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
            let known: HashSet<String> = catalog.known_batch_ids().into_iter().collect();
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
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {}
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
    use yuntun_model::ops::CommitFilesRequest;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as SArc;
    use yuntun_model::ops::CreateTableRequest;

    fn batch() -> arrow::record_batch::RecordBatch {
        arrow::record_batch::RecordBatch::try_new(
            SArc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
            vec![SArc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap()
    }

    fn compactor(catalog: Arc<MemoryCatalog>) -> Compactor {
        Compactor {
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
        for i in 0..3 {
            let bid = uuid::Uuid::now_v7().to_string();
            let (path, size, _rows) = yuntun_format::write_batch(
                &c.store, "t", "s0", "w1", &bid, &batch(), yuntun_format::DataFormat::Parquet,
            )
            .await
            .unwrap();
            let r = catalog
                .commit_files(CommitFilesRequest {
                    table: "t".into(),
                    batch_id: bid.clone(),
                    client_request_id: None,
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
            catalog.list_visible_files("t", snap, Some("s0")).await.unwrap().len(),
            3
        );

        // 执行合并
        let new_snap = compact_shard(&c, "t", "s0", snap).await.unwrap().unwrap();

        // 旧快照仍见 3 个（快照隔离）；新快照只见 1 个合并文件
        assert_eq!(
            catalog.list_visible_files("t", snap, Some("s0")).await.unwrap().len(),
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
        s3.insert("yuntun/t/dt=w/shard=s0/018f0000-0000-7000-8000-000000000001.parquet".to_string());
        s3.insert("yuntun/t/dt=w/shard=s0/018f0000-0000-7000-8000-000000000002.parquet".to_string());
        let mut known = HashSet::new();
        known.insert("018f0000-0000-7000-8000-000000000002".to_string());
        let orphans = classify_orphans(s3, &known, 0, Duration::from_secs(3600));
        assert_eq!(orphans.len(), 1);
        assert!(orphans[0].contains("000000000001"));
    }
}
