//! 读侧接缝（[`ShardReader`]）验证：查询路径对具体实现无感。
//!
//! 用 `RemoteShard`（**远端分片服务**形态，传输由 [`ShardFetch`] 注入）替换进程内
//! `MemoryShard`，SQL 仍能读到尚未落盘的热数据（读己之写）—— 分离部署时调用方零改动。

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_model::error::LakeError;
use yuntun_model::ops::CreateTableRequest;
use yuntun_query::{LocalCatalog, QueryEngine};
use yuntun_store::{RemoteShard, ShardFetch, ShardId, ShardRead, ShardTier};

/// 假传输：一份静态的"远端分片服务"数据（不依赖 MemoryShard，真正独立）。
struct CannedFetch {
    entries: HashMap<ShardId, Vec<arrow::record_batch::RecordBatch>>,
}

impl ShardFetch for CannedFetch {
    fn fetch_shards<'a>(
        &'a self,
        table: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<Vec<ShardId>, LakeError>> {
        Box::pin(async move {
            Ok(self
                .entries
                .keys()
                .filter(|id| id.table == table)
                .cloned()
                .collect())
        })
    }

    fn fetch_watermark<'a>(
        &'a self,
        known_manifest_ver: u64,
    ) -> futures::future::BoxFuture<'a, Result<ShardRead, LakeError>> {
        // 假服务端没有"已放弃的副本" ⇒ 水位 0（与它的 `fetch_shard` 一致）
        Box::pin(async move { Ok(ShardRead::empty(0, known_manifest_ver)) })
    }

    fn fetch_shard<'a>(
        &'a self,
        id: &'a ShardId,
        _known_manifest_ver: u64,
        _known_batch_ids: Vec<String>,
    ) -> futures::future::BoxFuture<'a, Result<ShardRead, LakeError>> {
        Box::pin(async move {
            // 本用例只验"查询能经远端接缝读到热数据"：水位取 0（不高于 known）⇒ 不报 STALE。
            // STALE 契约本身由 `store/src/shard.rs` 与 `chunk/src/store.rs` 两处单测钉住。
            Ok(ShardRead {
                batches: self.entries.get(id).cloned().unwrap_or_default(),
                flushed_watermark: 0,
                stale: false,
            })
        })
    }
}

fn batch(v: i64) -> arrow::record_batch::RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    arrow::record_batch::RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))])
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_reads_hot_data_through_remote_shard_reader() {
    // 表存在，但**没有任何已提交文件**（Manifest 为空）→ 只能靠内存分片
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
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

    // "远端分片服务"：两个分片各 1 行
    let mut entries = HashMap::new();
    entries.insert(
        ShardId::new("public.hot", "default", "2026-09-12T10:00"),
        vec![batch(1)],
    );
    entries.insert(
        ShardId::new("public.hot", "s1", "2026-09-12T10:01"),
        vec![batch(2)],
    );
    let reader: Arc<dyn yuntun_store::ShardReader> =
        Arc::new(RemoteShard::new(Arc::new(CannedFetch { entries })));
    assert_eq!(reader.tier(), ShardTier::Memory);

    let cache = Arc::new(LocalCatalog::new());
    // 名录是唯一真相（T12.3）：实例必须**登记进名录**，否则一次刷新
    // 就会用名录整体替换成员表、把这个实例（连同它的热读器）摘掉。
    catalog
        .register_datanode(yuntun_model::meta::DatanodeMember {
            instance_id: "standalone".to_string(),
            address: String::new(),
            registered_at_ms: 0,
        })
        .await
        .unwrap();
        cache.set_hot_shards("standalone", reader);
    cache.refresh(&catalog).await.unwrap();

    let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap();
    let engine = QueryEngine::new(store, cache);
    let batches = engine
        .sql("SELECT count(*) FROM yuntun.public.hot")
        .await
        .unwrap();
    let got = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(got, 2, "查询经 ShardReader 读到远端内存分片的热数据");
}

/// **按实例**持有热读器：多个实例的热数据都要被读到（`source_instance` 的第一个消费者）。
///
/// 这条钉住的是"按实例切分"这个**结构**本身：注册在两个 `instance_id` 下的热数据必须**都**
/// 参与查询 —— 只读"某一个"实例会让另一台的未落盘数据**静默消失**（比报错更难查）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hot_data_from_every_registered_instance_is_read() {
    // 表存在，但**没有任何已提交文件** → 只能靠热数据
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    catalog
        .create_table(CreateTableRequest {
            name: "hot2".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)])),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: Default::default(),
        })
        .await
        .unwrap();

    // 两个"实例"：各持有一个分片、各一行（模拟两个 datanode 各自的 chunk）
    let mut a = HashMap::new();
    a.insert(
        ShardId::new("public.hot2", "default", "2026-09-12T10:00"),
        vec![batch(1)],
    );
    let mut b = HashMap::new();
    b.insert(
        ShardId::new("public.hot2", "default", "2026-09-12T10:01"),
        vec![batch(2)],
    );

    let cache = Arc::new(LocalCatalog::new());
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
        cache.set_hot_shards(
        "inst-a",
        Arc::new(RemoteShard::new(Arc::new(CannedFetch { entries: a }))),
    );
    // 名录是唯一真相（T12.3）：实例必须**登记进名录**，否则一次刷新
    // 就会用名录整体替换成员表、把这个实例（连同它的热读器）摘掉。
    catalog
        .register_datanode(yuntun_model::meta::DatanodeMember {
            instance_id: "inst-b".to_string(),
            address: String::new(),
            registered_at_ms: 0,
        })
        .await
        .unwrap();
        cache.set_hot_shards(
        "inst-b",
        Arc::new(RemoteShard::new(Arc::new(CannedFetch { entries: b }))),
    );
    cache.refresh(&catalog).await.unwrap();

    let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap();
    let engine = QueryEngine::new(store, cache);
    let batches = engine
        .sql("SELECT count(*) FROM yuntun.public.hot2")
        .await
        .unwrap();
    let got = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        got, 2,
        "两个实例的热数据都必须被读到（只读一个 = 另一台的数据静默消失）"
    );
}
