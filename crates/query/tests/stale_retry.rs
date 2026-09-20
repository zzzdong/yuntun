//! **STALE 的消费**：热读报 STALE ⇒ 刷新 manifest ⇒ 用新版本**重试**（T12.2 第二刀）。
//!
//! 契约出处 `operation-log §61.4` 第 2 条；它修补的是 `§63.3` 钉住的那个静默错误：
//! 实例已放弃本地副本、而查询的 manifest 里还没有那些文件时（`architecture-with-chunk §4.5`
//! 的"两头都没有"），**旧行为只回一个空**，查询会静默少一批数据。
//!
//! 本文件验两条**方向相反**的性质：
//! 1. 能追上的 STALE ⇒ 刷新后重试成功，数据拿到（不是"静默少数据"）；
//! 2. **追不上的** STALE ⇒ **明确失败**，绝不返回不完整结果。

use std::sync::{Arc, Mutex};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_model::error::LakeError;
use yuntun_model::ops::CreateTableRequest;
use yuntun_query::{LocalCatalog, QueryEngine};
use yuntun_store::{create_store, ShardId, ShardRead, ShardReader, ShardTier, StoreConfig};

/// 假热读器：按调用次数决定要不要报 STALE，并记录**每次热读时**的
/// `(known_manifest_ver, 缓存刷新计数)` —— 后者用来证明"重试发生在刷新**之后**"。
#[derive(Debug)]
struct FlakyHot {
    cache: Arc<LocalCatalog>,
    seen: Mutex<Vec<(u64, u64)>>,
    /// 前 N 次热读报 STALE（0 = 从不报）。`usize::MAX` = 永远报。
    stale_first: usize,
}

impl FlakyHot {
    fn new(cache: Arc<LocalCatalog>, stale_first: usize) -> Self {
        Self {
            cache,
            seen: Mutex::new(Vec::new()),
            stale_first,
        }
    }

    fn calls(&self) -> Vec<(u64, u64)> {
        self.seen.lock().unwrap().clone()
    }
}

#[async_trait]
impl ShardReader for FlakyHot {
    fn tier(&self) -> ShardTier {
        ShardTier::Memory
    }

    fn version(&self) -> u64 {
        0
    }

    async fn shards_of(&self, table: &str) -> Result<Vec<ShardId>, LakeError> {
        Ok(vec![ShardId::new(table, "default", "2026-09-12T10:00")])
    }

    async fn read_shard(
        &self,
        _id: &ShardId,
        known_manifest_ver: u64,
    ) -> Result<ShardRead, LakeError> {
        let refreshes = self.cache.stats().refreshes;
        let mut seen = self.seen.lock().unwrap();
        seen.push((known_manifest_ver, refreshes));
        let n = seen.len();
        drop(seen);

        let stale = n <= self.stale_first;
        Ok(ShardRead {
            // 一行数据：查出来的 count 必须是 1，否则就是"静默少了数据"
            batches: vec![batch(1)],
            // 报 STALE 时水位取一个"高于任何 manifest 版本"的值，语义与真实实现一致
            flushed_watermark: if stale { u64::MAX } else { 0 },
            stale,
        })
    }
}

fn batch(v: i64) -> arrow::record_batch::RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    arrow::record_batch::RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))])
        .unwrap()
}

async fn setup_table(catalog: &Arc<dyn CatalogOps>) {
    catalog
        .create_table(CreateTableRequest {
            name: "hot".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)])),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: Default::default(),
        })
        .await
        .unwrap();
}

async fn count(engine: &QueryEngine) -> Result<i64, datafusion::error::DataFusionError> {
    let batches = engine.sql("SELECT count(*) FROM yuntun.public.hot").await?;
    Ok(batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0))
}

/// ① **能追上的 STALE**：第一次热读报 STALE ⇒ 刷新 manifest ⇒ 重试 ⇒ 数据拿到。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_hot_read_is_retried_after_refreshing_manifest() {
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    setup_table(&catalog).await;

    let cache = Arc::new(LocalCatalog::new());
    // 本刀的接线：查询路径要能在 STALE 时**自己刷新 manifest**
    cache.set_catalog_ops(catalog.clone());
    let hot = Arc::new(FlakyHot::new(cache.clone(), 1)); // 仅第一次报 STALE
    cache.set_hot_shards(hot.clone());
    cache.refresh(&catalog).await.unwrap();
    let refreshes_before = cache.stats().refreshes;

    let engine = QueryEngine::new(create_store(&StoreConfig::Memory).unwrap(), cache.clone());
    let got = count(&engine).await.expect("刷新后重试应当成功");

    assert_eq!(got, 1, "数据必须拿到（旧行为会静默少这一行）");

    let calls = hot.calls();
    assert!(
        calls.len() >= 2,
        "必须重试过（热读至少两次），实际 {} 次：{calls:?}",
        calls.len()
    );
    assert!(
        cache.stats().refreshes > refreshes_before,
        "必须**刷新过 manifest**（否则重试没有意义）"
    );
    assert!(
        calls[1].1 > calls[0].1,
        "第二次热读必须发生在刷新**之后**：{calls:?}（第二列是当时的刷新计数）"
    );
}

/// ② **追不上的 STALE**：永远报 STALE ⇒ 有界重试耗尽后**明确失败**。
///
/// 这条防的是"用不完整结果顶替"——那是比失败更坏的结果：用户会当成正确答案。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn permanent_stale_fails_loudly_instead_of_returning_partial_result() {
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    setup_table(&catalog).await;

    let cache = Arc::new(LocalCatalog::new());
    cache.set_catalog_ops(catalog.clone());
    let hot = Arc::new(FlakyHot::new(cache.clone(), usize::MAX)); // 永远 STALE
    cache.set_hot_shards(hot.clone());
    cache.refresh(&catalog).await.unwrap();

    let engine = QueryEngine::new(create_store(&StoreConfig::Memory).unwrap(), cache.clone());
    let err = count(&engine).await.expect_err("永远 STALE 必须报错，不能回不完整结果");
    let msg = err.to_string();
    assert!(msg.contains("STALE"), "报错要说清是 STALE：{msg}");
    assert!(
        msg.contains('3'),
        "报错应含尝试次数（上限 3），便于定位：{msg}"
    );
    assert_eq!(
        hot.calls().len(),
        3,
        "重试必须**有界**（恰好撞上限），实际 {:?} 次",
        hot.calls().len()
    );
}

/// ③ 没接线 `CatalogOps` 时：STALE 只能**明确失败**（配置问题要看得见，不能默默降级）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_without_wired_ops_fails_with_a_config_error() {
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    setup_table(&catalog).await;

    let cache = Arc::new(LocalCatalog::new());
    // **故意不调** `set_catalog_ops`
    let hot = Arc::new(FlakyHot::new(cache.clone(), 1));
    cache.set_hot_shards(hot.clone());
    cache.refresh(&catalog).await.unwrap();

    let engine = QueryEngine::new(create_store(&StoreConfig::Memory).unwrap(), cache.clone());
    let err = count(&engine).await.expect_err("没接线时 STALE 必须报错");
    assert!(
        err.to_string().contains("CatalogOps"),
        "报错应指向装配缺失（set_catalog_ops）：{err}"
    );
}

/// ④ 水位报上来但 `known` 已到位 ⇒ **不**该触发重试（防误报：每次查询都重试会变成新的坑）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caught_up_reader_does_not_trigger_retry() {
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    setup_table(&catalog).await;

    let cache = Arc::new(LocalCatalog::new());
    cache.set_catalog_ops(catalog.clone());
    let hot = Arc::new(FlakyHot::new(cache.clone(), 0)); // 从不报 STALE
    cache.set_hot_shards(hot.clone());
    cache.refresh(&catalog).await.unwrap();
    let refreshes_before = cache.stats().refreshes;

    let engine = QueryEngine::new(create_store(&StoreConfig::Memory).unwrap(), cache.clone());
    assert_eq!(count(&engine).await.unwrap(), 1);
    assert_eq!(hot.calls().len(), 1, "不该重试：{:?}", hot.calls());
    assert_eq!(
        cache.stats().refreshes,
        refreshes_before,
        "不该多刷一次 manifest"
    );
}
