//! flush 链路 e2e：ingest → WAL → **chunk 吸收** → seal → flush → 对象存储 + Manifest。
//!
//! 与旧版的差别（架构 §5）：不再手工 `BatchAccumulator::drain_ready` ——
//! 分组 / seal / flush 到期全部由 `ChunkStore` 决策，测试只驱动真实攒批循环。

use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_ingest::{Ingestor, IngestorConfig};
use yuntun_model::ops::CreateTableRequest;
use yuntun_model::IngestBatch;

fn table_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]))
}

/// 立即 seal + 立即 flush 的配置：行数阈值 1 + 持久化上界 0。
fn eager_config(wal_dir: &std::path::Path) -> IngestorConfig {
    IngestorConfig {
        rows_threshold: 1,
        bytes_threshold: 1,
        time_threshold_secs: 0,
        max_flush_delay_secs: 0,
        flush_phase_spread_secs: 0,
        scan_interval: Duration::from_millis(20),
        spill_dir: wal_dir.join("spill"),
        chunk_mem_budget: 64 * 1024 * 1024,
        ..Default::default()
    }
}

#[tokio::test]
async fn ingest_accumulate_flush_reaches_manifest_and_wal_terminal() {
    let wal_guard = yuntun_testkit::TestDir::tmpfs("flush-e2e-wal");
    let wal_dir = wal_guard.path().to_path_buf();

    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap();
    catalog
        .create_table(CreateTableRequest {
            name: "t".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: table_schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig {
                require_idempotency_key: false,
                ..Default::default()
            },
        })
        .await
        .unwrap();

    let wal = yuntun_wal::writer::WalWriter::open(yuntun_wal::WalConfig::for_dir(&wal_dir), 0)
        .await
        .unwrap();
    let ingestor = Arc::new(Ingestor::new(
        eager_config(&wal_dir),
        wal.clone(),
        catalog.clone(),
        store.clone(),
    ));

    let receipt = ingestor
        .ingest(IngestBatch {
            table: "t".into(),
            shard_key: "s0".into(),
            record_batch: arrow::record_batch::RecordBatch::try_new(
                table_schema(),
                vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
            )
            .unwrap(),
            idempotency_key: None,
            received_at: std::time::SystemTime::now(),
        })
        .await
        .unwrap();
    assert_eq!(receipt.wal_seq, 0);
    assert_eq!(wal.synced_seq(), 0);
    // 可见性上界绑 WAL：回执承诺一个扫描周期内可查
    assert!(receipt.expected_visible_in_secs <= 1);

    // 驱动真实攒批循环直到数据落盘
    let shutdown = tokio_util::sync::CancellationToken::new();
    let handle = ingestor.clone().spawn_accumulator(shutdown.clone());
    let mut files = 0usize;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let snap = catalog.current_snapshot().await;
        files = catalog
            .list_visible_files("public.t", snap, None)
            .await
            .unwrap()
            .len();
        if files > 0 {
            break;
        }
    }
    shutdown.cancel();
    let _ = handle.await;
    assert_eq!(files, 1, "flush 后 Manifest 应有 1 个文件");

    // chunk 已被标记 Flushed（未释放：等查询缓存追上，I4）
    let stats = ingestor.chunks().stats();
    assert_eq!(stats.chunks, 1, "chunk 在缓存追上之前不得提前释放");
    assert_eq!(stats.flushed, 1);

    // WAL 恢复应看到 1 个 Committed 批次
    let rec =
        yuntun_wal::recovery::recover(&yuntun_wal::WalConfig::for_dir(&wal_dir), 0, false).unwrap();
    assert_eq!(rec.states.states.len(), 1);
    assert!(
        rec.states.states.values().next().unwrap().is_terminal(),
        "flush 终态必须是 Committed（Abort 视作失败）"
    );

    // 读己之写：未落盘数据经 ShardReader 可读（此例已提交，缓存未追上仍可读）
    let reader: Arc<dyn yuntun_store::ShardReader> = ingestor.chunks();
    let rows: usize = reader
        .read_table("public.t", 0)
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(rows, 3);
}

/// 指标口径回归（`plan.md` T6.12）：`wal_backlog` 是**半开区间**差值。
///
/// `synced_seq` 是含端点的最高已 fsync seq，`absorbed_seq` 是"下一条待吸收 seq"，
/// 因此积压 = `synced_seq + 1 - absorbed_seq`。**差一**就会在空闲时谎报积压
/// （或更糟：在真积压时谎报 0），而这类错误只会体现在运维判断上，不会让任何功能测试变红。
#[tokio::test]
async fn wal_backlog_counts_fsynced_but_unabsorbed_records() {
    let wal_guard = yuntun_testkit::TestDir::tmpfs("backlog-wal");
    let wal_dir = wal_guard.path().to_path_buf();

    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap();
    catalog
        .create_table(CreateTableRequest {
            name: "t".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: table_schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig {
                require_idempotency_key: false,
                ..Default::default()
            },
        })
        .await
        .unwrap();

    let wal = yuntun_wal::writer::WalWriter::open(yuntun_wal::WalConfig::for_dir(&wal_dir), 0)
        .await
        .unwrap();
    // 阈值拉满：只观察"吸收"，不触发 flush
    let mut cfg = eager_config(&wal_dir);
    cfg.rows_threshold = usize::MAX;
    cfg.bytes_threshold = usize::MAX;
    cfg.max_flush_delay_secs = 3600;
    let ingestor = Arc::new(Ingestor::new(cfg, wal.clone(), catalog.clone(), store));

    for v in [1i64, 2] {
        ingestor
            .ingest(IngestBatch {
                table: "t".into(),
                shard_key: "s0".into(),
                record_batch: arrow::record_batch::RecordBatch::try_new(
                    table_schema(),
                    vec![Arc::new(Int64Array::from(vec![v]))],
                )
                .unwrap(),
                idempotency_key: None,
                received_at: std::time::SystemTime::now(),
            })
            .await
            .unwrap();
    }

    // 攒批线程尚未启动：两条 Data 记录全部处于"已 fsync 未吸收" → 积压 = 2
    assert_eq!(wal.synced_seq(), 1);
    assert_eq!(ingestor.absorbed_seq(), 0);
    assert_eq!(ingestor.wal_backlog(), 2, "积压 = synced + 1 - absorbed");

    // 启动攒批：吸收完两条后积压归零
    let shutdown = tokio_util::sync::CancellationToken::new();
    let handle = ingestor.clone().spawn_accumulator(shutdown.clone());
    let mut backlog = ingestor.wal_backlog();
    for _ in 0..100 {
        backlog = ingestor.wal_backlog();
        if backlog == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    shutdown.cancel();
    let _ = handle.await;

    assert_eq!(backlog, 0, "吸收完成后积压必须归零（空闲时谎报积压会误导运维）");
    assert!(ingestor.absorbed_seq() > wal.synced_seq());
    // 数据确实进了 chunk（可见性承诺），只是还没落盘
    let rows: usize = ingestor
        .chunks()
        .read_table_sync("public.t", 0)
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(rows, 2);
    assert_eq!(ingestor.chunk_stats().flushed, 0, "阈值拉满时不应 flush");
}

#[tokio::test]
async fn sealed_chunk_survives_satisfies_wal_recovery_contract() {
    // 未 flush 的 chunk 保持可见（可见性上界绑 WAL，不绑 flush）
    let wal_guard = yuntun_testkit::TestDir::tmpfs("flush-e2e-pending");
    let wal_dir = wal_guard.path().to_path_buf();

    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap();
    catalog
        .create_table(CreateTableRequest {
            name: "t".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: table_schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig {
                require_idempotency_key: false,
                ..Default::default()
            },
        })
        .await
        .unwrap();

    let wal = yuntun_wal::writer::WalWriter::open(yuntun_wal::WalConfig::for_dir(&wal_dir), 0)
        .await
        .unwrap();
    // 故意让 flush 上界极长（1 小时）：数据只可能在 chunk 里，不可能在 Manifest 里
    let mut cfg = eager_config(&wal_dir);
    cfg.max_flush_delay_secs = 3600;
    let ingestor = Arc::new(Ingestor::new(cfg, wal.clone(), catalog.clone(), store));

    ingestor
        .ingest(IngestBatch {
            table: "t".into(),
            shard_key: "s0".into(),
            record_batch: arrow::record_batch::RecordBatch::try_new(
                table_schema(),
                vec![Arc::new(Int64Array::from(vec![7]))],
            )
            .unwrap(),
            idempotency_key: None,
            received_at: std::time::SystemTime::now(),
        })
        .await
        .unwrap();

    let shutdown = tokio_util::sync::CancellationToken::new();
    let handle = ingestor.clone().spawn_accumulator(shutdown.clone());
    let mut rows = 0usize;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        rows = ingestor
            .chunks()
            .read_table_sync("public.t", 0)
            .iter()
            .map(|b| b.num_rows())
            .sum();
        if rows > 0 {
            break;
        }
    }
    shutdown.cancel();
    let _ = handle.await;

    assert_eq!(rows, 1, "fsync 后一个扫描周期内必须可查（可见性上界绑 WAL）");
    let snap = catalog.current_snapshot().await;
    assert!(
        catalog
            .list_visible_files("public.t", snap, None)
            .await
            .unwrap()
            .is_empty(),
        "持久化上界未到，不应写盘"
    );
    let stats = ingestor.chunks().stats();
    assert_eq!(stats.sealed, 1);
    assert_eq!(stats.spilled + stats.flushed, 0);
}
