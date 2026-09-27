//! **`F.3` 前置：强制 flush**（`delta-dml-design §4.3` / `operation-log §148`）。
//!
//! 为什么 DELETE 需要它：删除只能作用于**已提交的文件**（数据文件不可变，删除是"标记行位"）。
//! 而刚写进来的行还在 chunk 里（未提交）—— 不去 flush 就扫不到它们，等它们落盘之后
//! 会以"没被删掉"的样子出现（**复活**）。所以 DELETE 的第一步必然是
//! "把这张表的在途数据落盘并提交"，且**必须同步等到它真的落盘**。
//!
//! 四条断言：
//! 1. `force_flush` **真的**把在途 chunk 落盘（落盘前清单为空，落盘后有文件、行数对得上）；
//! 2. 之后该表**没有非终态 chunk**（否则删除会漏掉那部分行）；
//! 3. 已经落盘的表再调一次是 **no-op**（幂等，0 个 chunk）；
//! 4. 攒批循环不在时**明确报错**（而不是静默返回"flush 完了"）。

use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_ingest::{Ingestor, IngestorConfig};
use yuntun_model::IngestBatch;
use yuntun_model::ops::CreateTableRequest;

const TABLE: &str = "t";

fn table_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]))
}

/// **惰性**配置：窗口不关、行数阈值极大、持久化上界极大 ⇒ 攒批循环自己不会 flush。
/// 这样"数据确实还在途"是可控的，`force_flush` 的效果才可归因。
fn lazy_config(wal_dir: &std::path::Path) -> IngestorConfig {
    IngestorConfig {
        rows_threshold: usize::MAX,
        bytes_threshold: usize::MAX,
        time_threshold_secs: 3600,
        max_flush_delay_secs: 3600,
        flush_phase_spread_secs: 0,
        scan_interval: Duration::from_millis(10),
        spill_dir: wal_dir.join("spill"),
        chunk_mem_budget: 64 * 1024 * 1024,
        ..Default::default()
    }
}

async fn fixture() -> (yuntun_testkit::TestDir, Arc<Ingestor>, Arc<dyn CatalogOps>) {
    let wal_guard = yuntun_testkit::TestDir::tmpfs("force-flush-wal");
    let wal_dir = wal_guard.path().to_path_buf();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap();
    catalog
        .create_table(CreateTableRequest {
            name: TABLE.into(),
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
        lazy_config(&wal_dir),
        wal,
        catalog.clone(),
        store,
    ));
    (wal_guard, ingestor, catalog)
}

async fn write_rows(ingestor: &Arc<Ingestor>, values: Vec<i64>) {
    let rows = values.len();
    ingestor
        .ingest(IngestBatch {
            table: TABLE.into(),
            shard_key: "s0".into(),
            record_batch: arrow::record_batch::RecordBatch::try_new(
                table_schema(),
                vec![Arc::new(Int64Array::from(values))],
            )
            .unwrap(),
            idempotency_key: None,
            received_at: std::time::SystemTime::now(),
        })
        .await
        .unwrap();
    assert!(rows > 0);
}

/// 表在清单里的行数（没落盘 ⇒ 0）。
async fn committed_rows(catalog: &Arc<dyn CatalogOps>) -> u64 {
    let snap = catalog.current_snapshot().await;
    catalog
        .list_visible_files(&format!("public.{TABLE}"), snap, None)
        .await
        .unwrap()
        .iter()
        .map(|f| f.row_count)
        .sum()
}

#[tokio::test]
async fn force_flush_lands_in_flight_rows_and_leaves_no_pending_chunk() {
    let (_guard, ingestor, catalog) = fixture().await;
    let shutdown = tokio_util::sync::CancellationToken::new();
    let handle = ingestor.clone().spawn_accumulator(shutdown.clone());

    write_rows(&ingestor, vec![1, 2, 3]).await;
    // 给攒批循环几轮机会去"吸收但不 flush"（配置是惰性的）
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        if !ingestor.chunks().is_empty() {
            break;
        }
    }
    assert!(
        !ingestor.chunks().is_empty(),
        "前提：数据应当还在途（本用例要证的正是「强制落盘」）"
    );
    assert_eq!(committed_rows(&catalog).await, 0, "前提：还没落盘就查不到");

    let done = ingestor.force_flush(&format!("public.{TABLE}")).await.unwrap();
    assert_eq!(done.rows, 3, "落盘的行数就是刚才写进去的行数");
    assert!(done.chunks >= 1, "至少落盘一个 chunk");

    assert_eq!(committed_rows(&catalog).await, 3, "强制 flush 之后必须查得到");
    let pending: Vec<_> = ingestor
        .chunks()
        .chunk_ids_of_table(&format!("public.{TABLE}"))
        .into_iter()
        .filter(|id| {
            !matches!(
                ingestor.chunks().chunk_state(*id),
                None | Some(yuntun_chunk::ChunkState::Flushed)
                    | Some(yuntun_chunk::ChunkState::Released)
            )
        })
        .collect();
    assert!(
        pending.is_empty(),
        "flush 之后不得再有非终态 chunk（否则删除会漏掉那部分行）：{pending:?}"
    );

    // 幂等：再调一次没有东西可 flush
    let again = ingestor.force_flush(&format!("public.{TABLE}")).await.unwrap();
    assert_eq!(again, Default::default(), "已落盘的表再强制 flush 是 no-op");

    shutdown.cancel();
    let _ = handle.await;
}

/// **攒批循环不在时不许假装成功**：报错，而不是返回"0 个 chunk"。
#[tokio::test]
async fn force_flush_fails_loudly_without_a_running_accumulator() {
    let (_guard, ingestor, _catalog) = fixture().await;
    let shutdown = tokio_util::sync::CancellationToken::new();
    let handle = ingestor.clone().spawn_accumulator(shutdown.clone());
    // 先把循环停掉（通道随之关闭）
    shutdown.cancel();
    let _ = handle.await;

    let e = ingestor
        .force_flush(&format!("public.{TABLE}"))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("攒批循环") || e.contains("超时"),
        "必须点名「没有执行体」这件事：{e}"
    );
}
