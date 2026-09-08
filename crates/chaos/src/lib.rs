//! chaos 工具与验收测试（详细设计 §12 / 阶段 0.5 验收 E2/E3）。
//!
//! ## 模拟崩溃的方式
//! 阶段 0 单节点"硬崩溃" = **直接丢弃 Lakehouse 全部组件（不等待 flush、不 cancel）**
//! 然后全新重建（空 Catalog + 同一 WAL 目录 + 同一 local store 目录）。
//! 这与 `kill -9` 的可观测后果一致：
//! - 已 fsync 的 WAL 记录存活 → 恢复依据
//! - 已写对象存储的文件存活（local store）
//! - MemoryCatalog 清零（C5）→ 由 `resume_recovered` 从 WAL 重建
//!
//! ## 验收映射
//! - **E3（恢复后数据无丢失）**：[`crash_recovery_no_data_loss`] —— 每轮
//!   "写入若干批（拿到 ack）→ 硬崩溃 → 重建 → resume → 查询计数 == acked 行数"，
//!   共 5 轮，全部在**同一份 WAL/store 目录**上滚动，验证跨崩溃累积无丢失。
//! - **E2（chaos 轮次无失败）**：由 E3 的轮次循环覆盖（每轮即一次 kill/restart）。
//! - **T6.4（schema 并发冲突）**：[`concurrent_schema_evolution`] —— N 个并发
//!   writer 各带独立新列，OCC 冲突由 §5.2 重试循环消化，最终 schema 包含全部列。
//! - **T6.8（查询侧多版本对齐）**：[`query_multi_version_alignment`] ——
//!   v1/v2 文件共存，查询统一到最新 schema（缺失列 null 填充）。

// 本 crate 当前只含验收测试（E2/E3/T6.4/T6.8）与压测示例；以下导入均为测试专用。
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;
#[cfg(test)]
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use arrow::array::{Int64Array, StringArray};
#[cfg(test)]
use arrow::datatypes::{DataType, Field, Schema};
#[cfg(test)]
use yuntun_catalog::{CatalogOps, MemoryCatalog};
#[cfg(test)]
use yuntun_ingest::{Ingestor, IngestorConfig};
#[cfg(test)]
use yuntun_model::ops::CreateTableRequest;
#[cfg(test)]
use yuntun_model::IngestBatch;
#[cfg(test)]
use yuntun_query::QueryEngine;

/// 一套可重建的 Lakehouse 组件（chaos 轮次间共享目录）。
#[cfg(test)]
struct Setup {
    catalog: Arc<MemoryCatalog>,
    ingestor: Arc<Ingestor>,
    engine: Arc<QueryEngine>,
}

#[cfg(test)]
fn schema() -> arrow::datatypes::SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_time", DataType::Int64, false),
        Field::new("user", DataType::Utf8, true),
    ]))
}

#[cfg(test)]
fn batch(rows: i64) -> arrow::record_batch::RecordBatch {
    arrow::record_batch::RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![rows; 3])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])),
        ],
    )
    .unwrap()
}

#[cfg(test)]
fn tmpdir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("yuntun-chaos-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// 构建一套组件（每轮重建 = 模拟进程重启）。
///
/// `tables`：重启后需要恢复的表定义。阶段 1 表定义由 raft snapshot 恢复（C5/§5.4.2）；
/// 阶段 0 chaos harness 中由调用方重放（模拟 snapshot 已含表定义）。
#[cfg(test)]
async fn build(
    wal_dir: &std::path::Path,
    store_root: &std::path::Path,
    tables: &[(&str, arrow::datatypes::SchemaRef)],
) -> Setup {
    let catalog = Arc::new(MemoryCatalog::new());
    // 恢复表定义（模拟 raft snapshot；已存在则跳过）
    for (name, schema) in tables {
        if catalog.get_table(name).await.unwrap().is_none() {
            catalog
                .create_table(CreateTableRequest {
                    name: name.to_string(),
                    schema: schema.clone(),
                    partition_cols: vec![],
                    default_format: "parquet".into(),
                    ingest_config: yuntun_model::meta::IngestConfig {
                        require_idempotency_key: false,
                        ..yuntun_model::meta::IngestConfig::standard()
                    },
                })
                .await
                .unwrap();
        }
    }
    let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Local {
        root: store_root.to_string_lossy().to_string(),
    })
    .unwrap();
    let wal = yuntun_wal::writer::WalWriter::open(yuntun_wal::WalConfig::for_dir(wal_dir), 0)
        .await
        .unwrap();
    let ingestor = Arc::new(Ingestor::new(
        IngestorConfig {
            rows_threshold: 1,
            flush_jitter_secs: 0,
            scan_interval: Duration::from_millis(20),
            ..Default::default()
        },
        wal,
        catalog.clone(),
        store,
    ));
    // 崩溃恢复分流（build 阶段，模拟 Lakehouse::build_with_shutdown 行为）
    ingestor.resume_recovered().await.unwrap();
    let cache = Arc::new(yuntun_query::LocalCatalogCache::new());
    let engine = Arc::new(QueryEngine::new(
        yuntun_store::create_store(&yuntun_store::StoreConfig::Local {
            root: store_root.to_string_lossy().to_string(),
        })
        .unwrap(),
        cache,
    ));
    Setup {
        catalog,
        ingestor,
        engine,
    }
}

/// E3 / E2：crash 恢复无数据丢失（5 轮硬崩溃，同一目录滚动）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_recovery_no_data_loss() {
    let wal_dir = tmpdir("wal");
    let store_root = tmpdir("store");

    // 表定义随每轮 build 恢复（模拟 raft snapshot）
    let tables = [("audit", schema())];
    let mut setup: Option<Setup> = Some(build(&wal_dir, &store_root, &tables).await);

    let mut acked_rows = 0u64;
    let rounds = 5;
    for round in 0..rounds {
        // ① 本轮写入 3 批（拿到 ack = 已 fsync），每批 3 行
        let batch_rows = [
            (100 + round as i64 * 10, format!("r{round}-k0")),
            (200 + round as i64 * 10, format!("r{round}-k1")),
            (300 + round as i64 * 10, format!("r{round}-k2")),
        ];
        for (ts, key) in &batch_rows {
            let receipt = setup
                .as_ref()
                .unwrap()
                .ingestor
                .ingest(IngestBatch {
                    table: "audit".into(),
                    shard_key: format!("s{}", ts % 2),
                    record_batch: batch(*ts),
                    idempotency_key: Some(key.clone()),
                    received_at: std::time::SystemTime::now(),
                })
                .await
                .unwrap();
            acked_rows += receipt.row_count;
        }

        // ② 硬崩溃：不等 flush，直接丢弃全部组件（模拟 kill -9）
        drop(setup.take());

        // ③ 重启：空 Catalog + 同目录 WAL/store → resume → 攒批循环处理 Pending
        setup = Some(build(&wal_dir, &store_root, &tables).await);
        let cur = setup.as_ref().unwrap();
        let shutdown = CancellationToken::new();
        let _acc = cur.ingestor.clone().spawn_accumulator(shutdown.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;
        shutdown.cancel();

        // ④ 查询计数 == 累计 acked 行数（无丢失、无重复）
        cur.engine
            .cache()
            .refresh(&(cur.catalog.clone() as Arc<dyn yuntun_catalog::CatalogOps>))
            .await
            .unwrap();
        let batches = cur
            .engine
            .sql("SELECT count(*) FROM yuntun.public.audit")
            .await
            .unwrap();
        let got = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(
            got as u64, acked_rows,
            "round {round}: 恢复后行数必须等于累计 acked 行数（无丢失无重复）"
        );
        tracing::info!(round, acked_rows, "crash round OK");
    }
}

/// T6.4：schema 并发演进 —— N 个并发 writer 各带独立新列，OCC + 重试消化冲突。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_schema_evolution() {
    let wal_dir = tmpdir("wal-evo");
    let store_root = tmpdir("store-evo");
    let tables = [(
        "evo",
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
            as arrow::datatypes::SchemaRef,
    )];
    let setup = build(&wal_dir, &store_root, &tables).await;

    const N: usize = 8;
    let mut handles = Vec::new();
    for i in 0..N {
        let ingestor = setup.ingestor.clone();
        let col = format!("extra_{i}");
        handles.push(tokio::spawn(async move {
            let s = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new(&col, DataType::Utf8, true),
            ]));
            let b = arrow::record_batch::RecordBatch::try_new(
                s,
                vec![
                    Arc::new(Int64Array::from(vec![i as i64; 2])),
                    Arc::new(StringArray::from(vec![Some("x"), Some("y")])),
                ],
            )
            .unwrap();
            ingestor
                .ingest(IngestBatch {
                    table: "evo".into(),
                    shard_key: "s0".into(),
                    record_batch: b,
                    idempotency_key: None,
                    received_at: std::time::SystemTime::now(),
                })
                .await
        }));
    }
    for h in handles {
        let r = h.await.unwrap();
        assert!(r.is_ok(), "并发演进 + 重试后所有写入应成功: {r:?}");
    }

    // 最终 schema 包含全部 N 个新列
    let (_, version) = setup.catalog.table_schema("evo").await.unwrap().unwrap();
    let final_schema = setup.catalog.table_schema("evo").await.unwrap().unwrap().0;
    for i in 0..N {
        assert!(
            final_schema.field_with_name(&format!("extra_{i}")).is_ok(),
            "列 extra_{i} 必须在最终 schema 中"
        );
    }
    assert!(version > N as u64);
    let _ = store_root;
}

/// T6.8：查询侧多版本对齐 —— v1/v2 文件共存，查询统一到最新 schema。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_multi_version_alignment() {
    let wal_dir = tmpdir("wal-mv");
    let store_root = tmpdir("store-mv");
    let tables = [(
        "mv",
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
            as arrow::datatypes::SchemaRef,
    )];
    let setup = build(&wal_dir, &store_root, &tables).await;

    // v1 批次（只有 a 列）
    setup
        .ingestor
        .ingest(IngestBatch {
            table: "mv".into(),
            shard_key: "s0".into(),
            record_batch: arrow::record_batch::RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)])),
                vec![Arc::new(Int64Array::from(vec![1, 2]))],
            )
            .unwrap(),
            idempotency_key: None,
            received_at: std::time::SystemTime::now(),
        })
        .await
        .unwrap();

    // v2 批次（a + b 列 → 自动演进）
    setup
        .ingestor
        .ingest(IngestBatch {
            table: "mv".into(),
            shard_key: "s0".into(),
            record_batch: arrow::record_batch::RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("a", DataType::Int64, true),
                    Field::new("b", DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![3, 4])),
                    Arc::new(StringArray::from(vec![Some("x"), None])),
                ],
            )
            .unwrap(),
            idempotency_key: None,
            received_at: std::time::SystemTime::now(),
        })
        .await
        .unwrap();

    // flush 两个批次
    let shutdown = CancellationToken::new();
    let _acc = setup.ingestor.clone().spawn_accumulator(shutdown.clone());
    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown.cancel();

    // 查询：统一到 v2 schema；v1 文件缺失 b 列 → null 填充
    setup
        .engine
        .cache()
        .refresh(&(setup.catalog.clone() as Arc<dyn yuntun_catalog::CatalogOps>))
        .await
        .unwrap();

    let batches = setup
        .engine
        .sql("SELECT count(*) FROM yuntun.public.mv")
        .await
        .unwrap();
    let got = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(got, 4, "两个 schema_version 的文件行数合并");

    // b 列：v2 行有值，v1 行 null
    let batches = setup
        .engine
        .sql("SELECT count(*) FROM yuntun.public.mv WHERE b IS NOT NULL")
        .await
        .unwrap();
    let got = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(got, 1, "v1 文件缺失列应填 null（b IS NOT NULL 仅 1 行）");
}
