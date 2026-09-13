//! 恢复路径的"表世代"守卫（回归）：
//! Pending 批次若属于已 DROP（或 DROP 后重建）的表世代，重做时必须 abort 而不是提交 ——
//! 否则会留下悬挂 Manifest，之后重建同名表即复活旧数据。

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_ingest::{Ingestor, IngestorConfig};
use yuntun_model::meta::serialize_schema;
use yuntun_model::wal_record::{
    ddl_op, BatchPendingPayload, DataPayload, DdlPayload, Record,
};

#[tokio::test]
async fn pending_batch_of_dropped_table_is_aborted_not_committed() {
    let dir = yuntun_testkit::TestDir::tmpfs("recovery-guard");
    let wal_dir = dir.path().to_path_buf();
    let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    let wal = yuntun_wal::writer::WalWriter::open(yuntun_wal::WalConfig::for_dir(&wal_dir), 0)
        .await
        .unwrap();
    let ingestor = Ingestor::new(
        IngestorConfig {
            rows_threshold: 1_000_000,
            ..Default::default()
        },
        wal.clone(),
        catalog.clone(),
        store,
    );

    // 手工构造 WAL：Ddl(CREATE t) → Data(t) → BatchPending(覆盖该 Data) → Ddl(DROP t)
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let batch =
        arrow::record_batch::RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![7]))])
            .unwrap();
    let mut ipc = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut ipc, &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    wal.append(Record::Ddl(DdlPayload {
        op: ddl_op::CREATE_TABLE,
        table: "public.t".into(),
        arrow_schema: serialize_schema(&schema),
        default_format: "parquet".into(),
    }))
    .await
    .unwrap();
    let data_seq = wal
        .append(Record::Data(DataPayload {
            table: "public.t".into(),
            shard: "default".into(),
            schema_version: 1,
            batch_ipc: ipc,
            client_request_id: String::new(),
            time_window: "2026-09-12T00:00".into(),
        }))
        .await
        .unwrap()
        .seq;
    wal.append(Record::BatchPending(BatchPendingPayload {
        batch_id: "b-pending".into(),
        table: "public.t".into(),
        shard: "default".into(),
        window: "2026-09-12T00:00".into(),
        wal_seq_start: data_seq,
        wal_seq_end: data_seq + 1,
        schema_version: 1,
        client_request_id: String::new(),
        created_at_ms: 1,
        row_count: 1,
    }))
    .await
    .unwrap();
    wal.append(Record::Ddl(DdlPayload {
        op: ddl_op::DROP_TABLE,
        table: "public.t".into(),
        arrow_schema: vec![],
        default_format: String::new(),
    }))
    .await
    .unwrap();

    let (redone, committed) = ingestor.resume_recovered().await.unwrap();
    assert_eq!(
        (redone, committed),
        (0, 0),
        "陈旧世代的 Pending 批次不得被重做/重提交"
    );

    // 不得留下悬挂 Manifest（否则重建同名表会复活该数据）
    let snap = catalog.current_snapshot().await;
    assert!(
        catalog
            .list_visible_files("public.t", snap, None)
            .await
            .unwrap()
            .is_empty(),
        "已 DROP 表不得留下悬挂 Manifest"
    );

    // 批次应进入终态（BatchAbort 会移除状态）
    let rec =
        yuntun_wal::recovery::recover(&yuntun_wal::WalConfig::for_dir(&wal_dir), 0, false).unwrap();
    assert!(
        !rec.states.states.contains_key("b-pending"),
        "陈旧批次应被 abort（终态）"
    );
}
