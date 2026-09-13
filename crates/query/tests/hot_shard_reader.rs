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
use yuntun_query::{LocalCatalogCache, QueryEngine};
use yuntun_store::{RemoteShard, ShardFetch, ShardId, ShardTier};

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

    fn fetch_shard<'a>(
        &'a self,
        id: &'a ShardId,
        _cached_snapshot: u64,
    ) -> futures::future::BoxFuture<'a, Result<Vec<arrow::record_batch::RecordBatch>, LakeError>> {
        Box::pin(async move { Ok(self.entries.get(id).cloned().unwrap_or_default()) })
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

    let cache = Arc::new(LocalCatalogCache::new());
    cache.set_hot_shards(reader);
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
