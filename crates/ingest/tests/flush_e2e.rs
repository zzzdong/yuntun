//! flush 链路同步调试测试：ingest → scan → 攒批 → flush_batch。

use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use yuntun_catalog::MemoryCatalog;
use yuntun_ingest::{BatchAccumulator, Ingestor, IngestorConfig};
use yuntun_model::ops::CreateTableRequest;
use yuntun_model::IngestBatch;

#[tokio::test]
async fn scan_accumulate_flush() {
    let wal_dir = std::env::temp_dir().join(format!("yuntun-flush-dbg-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&wal_dir);
    std::fs::create_dir_all(&wal_dir).unwrap();

    let catalog: Arc<dyn yuntun_catalog::CatalogOps> = Arc::new(MemoryCatalog::new());
    let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap();
    catalog
        .create_table(CreateTableRequest {
            name: "t".into(),
            schema: Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
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
        IngestorConfig {
            rows_threshold: 1,
            ..Default::default()
        },
        wal.clone(),
        catalog.clone(),
        store.clone(),
    ));

    let meta = catalog.get_table("t").await.unwrap().unwrap();
    eprintln!(
        "DEBUG ingest_config: require_key={:?} ttl={:?}",
        meta.ingest_config.as_ref().map(|c| c.require_idempotency_key),
        meta.ingest_config.as_ref().map(|c| c.idempotency_ttl_secs)
    );
    let b = arrow::record_batch::RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let receipt = ingestor
        .ingest(IngestBatch {
            table: "t".into(),
            shard_key: "s0".into(),
            record_batch: b,
            idempotency_key: None,
            received_at: std::time::SystemTime::now(),
        })
        .await
        .unwrap();
    eprintln!("receipt: {:?}", receipt);
    assert_eq!(receipt.wal_seq, 0);
    assert_eq!(wal.synced_seq(), 0);

    // 同步复刻 run_accumulator 的扫描逻辑
    let reader = yuntun_wal::reader::WalReader::new(wal.shard_dir());
    let records = reader.scan_range(0, wal.synced_seq() + 1).unwrap();
    eprintln!("scanned records: {}", records.len());
    assert_eq!(records.len(), 1);

    let mut acc = BatchAccumulator::new();
    for (seq, rec) in records {
        if let yuntun_model::wal_record::Record::Data(p) = rec {
            acc.push(p, seq, 3, yuntun_ingest::accumulator::now_ms());
        }
    }
    let cfg = IngestorConfig {
        rows_threshold: 1,
        ..Default::default()
    };
    let mut ready = acc.drain_ready(yuntun_ingest::accumulator::now_ms(), &cfg);
    eprintln!("ready groups: {}", ready.len());
    assert_eq!(ready.len(), 1);

    let out = ingestor.flush_now(ready.remove(0)).await;
    eprintln!("flush result: {:?}", out.as_ref().map(|o| o.file_path.clone()));
    out.unwrap();

    // WAL 恢复应看到 1 个 Committed 批次
    let rec = yuntun_wal::recovery::recover(&yuntun_wal::WalConfig::for_dir(&wal_dir), 0, false)
        .unwrap();
    eprintln!(
        "recovered states: {:?}",
        rec.states
            .states
            .iter()
            .map(|(k, v)| (k.clone(), format!("{:?}", v.status)))
            .collect::<Vec<_>>()
    );
    assert_eq!(rec.states.states.len(), 1);
    let _ = Duration::from_millis(0);
}
