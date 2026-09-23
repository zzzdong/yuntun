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
    /// 缓存刷新计数达到这个值之前一直报 STALE（`u64::MAX` = 永远报）。
    ///
    /// **为什么按"刷新计数"而不是按"调用次数"**：契约把水位独立成了 `watermark()`
    /// （实例级属性，`§67`），于是**一轮查询会调用多次**（逐分片 + 一次水位）——
    /// 按次数决定会让同一轮内的几次调用给出互相矛盾的答案。按刷新计数则天然对齐
    /// "**一轮查询**"：引擎刷新 manifest 之后判定随之改变，这也正是真实实现的语义。
    stale_until_refreshes: u64,
}

impl FlakyHot {
    fn new(cache: Arc<LocalCatalog>, stale_until_refreshes: u64) -> Self {
        Self {
            cache,
            seen: Mutex::new(Vec::new()),
            stale_until_refreshes,
        }
    }

    fn calls(&self) -> Vec<(u64, u64)> {
        self.seen.lock().unwrap().clone()
    }

    /// 与真实实现同构：**水位与 stale 由同一处判定**（`read_shard` 与 `watermark` 都问它）
    fn is_stale(&self) -> bool {
        self.cache.stats().refreshes < self.stale_until_refreshes
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
        self.seen
            .lock()
            .unwrap()
            .push((known_manifest_ver, refreshes));

        let stale = self.is_stale();
        Ok(ShardRead {
            // 一行数据：查出来的 count 必须是 1，否则就是"静默少了数据"
            batches: vec![batch(1)],
            // 报 STALE 时水位取一个"高于任何 manifest 版本"的值，语义与真实实现一致
            flushed_watermark: if stale { u64::MAX } else { 0 },
            stale,
        })
    }

    /// 实例级水位：**与 `read_shard` 用同一判据**（真实数据节点也是同一处判定）
    async fn watermark(&self, _known_manifest_ver: u64) -> Result<ShardRead, LakeError> {
        let stale = self.is_stale();
        Ok(ShardRead {
            batches: Vec::new(),
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
    let hot = Arc::new(FlakyHot::new(cache.clone(), 2)); // setup 已刷过 1 次 ⇒ 首次仍 STALE，引擎刷新后追上
    // 名录是唯一真相（T12.3）：实例必须**登记进名录**，否则一次刷新
    // 就会用名录整体替换成员表、把这个实例（连同它的热读器）摘掉。
    catalog
        .register_datanode(yuntun_model::meta::DatanodeMember {
            instance_id: "inst-a".to_string(),
            address: String::new(),
            registered_at_ms: 0,
        })
        .await
        .unwrap();
        cache.set_hot_shards("inst-a", hot.clone());
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
    let hot = Arc::new(FlakyHot::new(cache.clone(), u64::MAX)); // 永远 STALE
    // 名录是唯一真相（T12.3）：实例必须**登记进名录**，否则一次刷新
    // 就会用名录整体替换成员表、把这个实例（连同它的热读器）摘掉。
    catalog
        .register_datanode(yuntun_model::meta::DatanodeMember {
            instance_id: "inst-a".to_string(),
            address: String::new(),
            registered_at_ms: 0,
        })
        .await
        .unwrap();
        cache.set_hot_shards("inst-a", hot.clone());
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
    let hot = Arc::new(FlakyHot::new(cache.clone(), 2));
    // 名录是唯一真相（T12.3）：实例必须**登记进名录**，否则一次刷新
    // 就会用名录整体替换成员表、把这个实例（连同它的热读器）摘掉。
    catalog
        .register_datanode(yuntun_model::meta::DatanodeMember {
            instance_id: "inst-a".to_string(),
            address: String::new(),
            registered_at_ms: 0,
        })
        .await
        .unwrap();
        cache.set_hot_shards("inst-a", hot.clone());
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
    let hot = Arc::new(FlakyHot::new(cache.clone(), 1)); // setup 已刷过 1 次 ⇒ 从不报 STALE
    // 名录是唯一真相（T12.3）：实例必须**登记进名录**，否则一次刷新
    // 就会用名录整体替换成员表、把这个实例（连同它的热读器）摘掉。
    catalog
        .register_datanode(yuntun_model::meta::DatanodeMember {
            instance_id: "inst-a".to_string(),
            address: String::new(),
            registered_at_ms: 0,
        })
        .await
        .unwrap();
        cache.set_hot_shards("inst-a", hot.clone());
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
