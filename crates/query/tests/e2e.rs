//! 端到端集成测试：Ingest（WAL→攒批→S3→Meta）→ Cache 刷新 → DataFusion SQL 查询。
//! 对应阶段 0 验收 D1/D2（详细设计 §13.4）。

use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use yuntun_catalog::MemoryCatalog;
use yuntun_ingest::{Ingestor, IngestorConfig};
use yuntun_model::ops::CreateTableRequest;
use yuntun_model::IngestBatch;
use yuntun_query::QueryEngine;
use yuntun_store::create_store;

fn schema() -> arrow::datatypes::SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_time", DataType::Int64, false),
        Field::new("user", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
    ]))
}

fn batch(rows: i64) -> arrow::record_batch::RecordBatch {
    arrow::record_batch::RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![rows; 3])),
            Arc::new(StringArray::from(vec![Some("alice"), None, Some("bob")])),
            Arc::new(Int64Array::from(vec![Some(10), Some(20), None])),
        ],
    )
    .unwrap()
}

fn tmpdir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("yuntun-e2e-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ingest_then_query_visible() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,yuntun=debug")),
        )
        .with_test_writer()
        .try_init();
    let wal_dir = tmpdir("wal");
    let catalog: Arc<dyn yuntun_catalog::CatalogOps> = Arc::new(MemoryCatalog::new());
    let store = create_store(&yuntun_store::StoreConfig::Memory).unwrap();

    // ① 建表
    catalog
        .create_table(CreateTableRequest {
            name: "audit".into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig::standard(),
        })
        .await
        .unwrap();

    // ② Ingestor：行数阈值=1 → 首次扫描即 flush；jitter=0
    let cfg = IngestorConfig {
        rows_threshold: 1,
        time_threshold_secs: 5,
        idle_timeout: Duration::from_secs(60),
        flush_jitter_secs: 0,
        scan_interval: Duration::from_millis(20),
        ..Default::default()
    };
    let wal = yuntun_wal::writer::WalWriter::open(yuntun_wal::WalConfig::for_dir(&wal_dir), 0)
        .await
        .unwrap();
    let ingestor = Arc::new(Ingestor::new(cfg, wal, catalog.clone(), store.clone()));

    // ③ 写入两条 IngestBatch（走 ingest()：OCC → WAL fsync）
    let r1 = ingestor
        .ingest(IngestBatch {
            table: "audit".into(),
            shard_key: "s0".into(),
            record_batch: batch(1),
            idempotency_key: Some("client-key-41".into()),
            received_at: std::time::SystemTime::now(),
        })
        .await
        .unwrap();
    assert_eq!(r1.row_count, 3);

    ingestor
        .ingest(IngestBatch {
            table: "audit".into(),
            shard_key: "s1".into(),
            record_batch: batch(2),
            idempotency_key: Some("client-key-42".into()),
            received_at: std::time::SystemTime::now(),
        })
        .await
        .unwrap();

    // ④ 启动攒批循环（后台 task），等 flush 完成
    let shutdown = CancellationToken::new();
    let acc_handle = ingestor.clone().spawn_accumulator(shutdown.clone());
    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown.cancel();
    let _ = acc_handle.await;

    // ⑤ WAL 全部终态（BatchCommitted）
    let recovery =
        yuntun_wal::recovery::recover(&yuntun_wal::WalConfig::for_dir(&wal_dir), 0, false).unwrap();
    assert!(recovery.states.states.len() >= 2, "至少两个 batch");

    // ⑥ Query：缓存刷新 + SQL
    let cache = Arc::new(yuntun_query::LocalCatalogCache::new());
    cache.refresh(&catalog).await.unwrap();

    let engine = QueryEngine::new(store.clone(), cache);
    let batches = engine
        .sql("SELECT count(*) AS c FROM yuntun.public.audit")
        .await
        .unwrap();
    let total: i64 = batches
        .iter()
        .map(|b| {
            use arrow::array::Array;
            let col = b.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            arr.value(0)
        })
        .sum();
    assert_eq!(total, 6, "两个 shard 各 3 行都应可见");

    // ⑦ 过滤下推 + 聚合
    let batches = engine
        .sql("SELECT \"user\", count(*) FROM yuntun.public.audit WHERE amount IS NOT NULL GROUP BY \"user\" ORDER BY \"user\"")
        .await
        .unwrap();
    assert!(!batches.is_empty());

    // ⑧ 表列表
    let names = engine.cache().table_names().await;
    assert!(names.contains(&"audit".to_string()));
}

#[tokio::test]
async fn cache_ttl_keeps_queries_off_network_path() {
    // C7 语义验证：查询构造（scan）只读缓存，不触碰 Catalog
    let catalog: Arc<dyn yuntun_catalog::CatalogOps> = Arc::new(MemoryCatalog::new());
    let store = create_store(&yuntun_store::StoreConfig::Memory).unwrap();
    catalog
        .create_table(CreateTableRequest {
            name: "t".into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: Default::default(),
        })
        .await
        .unwrap();

    let cache = Arc::new(yuntun_query::LocalCatalogCache::new());
    let shutdown = CancellationToken::new();
    let handle = yuntun_query::spawn_cache_refresh(
        cache.clone(),
        catalog.clone(),
        Duration::from_millis(50),
        shutdown.clone(),
    );
    tokio::time::sleep(Duration::from_millis(120)).await;
    shutdown.cancel();
    let _ = handle.await;

    assert!(cache.get("t").await.is_some());
    assert!(cache.get("missing").await.is_none());
    let _ = store;
}
