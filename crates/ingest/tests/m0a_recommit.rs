//! M0a 恢复改造回归（delta-dml-design §1.1）：
//! 终态批次按**原 batch_id 与既有对象**重提交内存 Catalog（不追加 WAL、不重写文件），
//! 替代旧语义"攒批全量重放重写全部历史"。
//!
//! 覆盖：
//! - A. 重提交：committed>0 / redone==0 / **对象存储零新增** / 文件可见 / 认领集就位；
//! - B. 世代闸门（R9）：DROP→同名重建→重启，旧世代数据不挂新表；
//! - C. 交错写入区间精确化（R8）：Pending 重做**按表过滤**组内空洞，
//!   他表 Data 不得混入本批文件（旧行为会跨表串数据）。

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use futures::stream::StreamExt;
use object_store::{ObjectStore as _, ObjectStoreExt as _};
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_ingest::{Ingestor, IngestorConfig};
use yuntun_model::meta::serialize_schema;
use yuntun_model::wal_record::{ddl_op, BatchPendingPayload, DataPayload, DdlPayload, Record};

const T1: &str = "public.t1";
const T2: &str = "public.t2";

struct Env {
    wal_dir: std::path::PathBuf,
    store: Arc<dyn object_store::ObjectStore>,
}

async fn wal_writer(env: &Env) -> yuntun_wal::writer::WalWriter {
    yuntun_wal::writer::WalWriter::open(yuntun_wal::WalConfig::for_dir(&env.wal_dir), 0)
        .await
        .unwrap()
}

fn new_ingestor(
    env: &Env,
    catalog: Arc<dyn CatalogOps>,
) -> Ingestor {
    let wal = futures::executor::block_on(wal_writer(env));
    Ingestor::new(
        IngestorConfig {
            rows_threshold: 1_000_000,
            ..Default::default()
        },
        wal,
        catalog.clone(),
        env.store.clone(),
    )
}

fn ipc_bytes(schema: &Schema, values: &[i64]) -> Vec<u8> {
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap();
    let mut out = Vec::new();
    let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut out, schema).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
    out
}

async fn append_data(wal: &yuntun_wal::writer::WalWriter, table: &str, values: &[i64]) -> u64 {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    wal.append(Record::Data(DataPayload {
        table: table.into(),
        shard: "default".into(),
        schema_version: 1,
        batch_ipc: ipc_bytes(&schema, values),
        client_request_id: String::new(),
        time_window: "2026-09-12T00:00".into(),
    }))
    .await
    .unwrap()
    .seq
}

async fn append_committed_batch(
    wal: &yuntun_wal::writer::WalWriter,
    env: &Env,
    batch_id: &str,
    table: &str,
    values: &[i64],
) -> u64 {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let first = append_data(wal, table, values).await;
    wal.append(Record::BatchPending(BatchPendingPayload {
        batch_id: batch_id.into(),
        table: table.into(),
        shard: "default".into(),
        window: "2026-09-12T00:00".into(),
        wal_seq_start: first,
        wal_seq_end: first + values.len() as u64,
        schema_version: 1,
        client_request_id: String::new(),
        created_at_ms: 1,
        row_count: values.len() as u64,
    }))
    .await
    .unwrap();
    let obj_path = format!("yuntun/{table}/dt=2026-09-12T00:00/shard=default/{batch_id}.parquet");
    env.store
        .put(
            &object_store::path::Path::from(obj_path.clone()),
            object_store::PutPayload::from(ipc_bytes(&schema, values)),
        )
        .await
        .unwrap();
    wal.append(Record::BatchS3Written(
        yuntun_model::wal_record::BatchS3WrittenPayload {
            batch_id: batch_id.into(),
            s3_paths: vec![obj_path.clone()],
            s3_upload_id: String::new(),
            file_size: ipc_bytes(&schema, values).len() as u64,
        },
    ))
    .await
    .unwrap();
    wal.append(Record::BatchCommitted(
        yuntun_model::wal_record::BatchCommittedPayload {
            batch_id: batch_id.into(),
        },
    ))
    .await
    .unwrap();
    first
}

async fn append_ddl(wal: &yuntun_wal::writer::WalWriter, op: u32, table: &str) {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    wal.append(Record::Ddl(DdlPayload {
        op,
        table: table.into(),
        arrow_schema: if op == ddl_op::CREATE_TABLE {
            serialize_schema(&schema)
        } else {
            vec![]
        },
        default_format: "parquet".into(),
    }))
    .await
    .unwrap();
}

async fn object_paths(store: &Arc<dyn object_store::ObjectStore>) -> Vec<String> {
    let mut out = Vec::new();
    let mut stream = store.list(None);
    while let Some(m) = stream.next().await {
        out.push(m.unwrap().location.to_string());
    }
    out.sort();
    out
}

#[tokio::test]
async fn committed_batches_are_recommitted_without_new_objects() {
    let dir = yuntun_testkit::TestDir::tmpfs("m0a-recommit");
    let env = Env {
        wal_dir: dir.path().to_path_buf(),
        store: yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap(),
    };
    let wal = wal_writer(&env).await;
    append_ddl(&wal, ddl_op::CREATE_TABLE, T1).await;
    append_committed_batch(&wal, &env, "b1", T1, &[1, 2]).await;
    drop(wal);

    let before = object_paths(&env.store).await;
    assert_eq!(before.len(), 1, "前置：一个已提交数据文件");

    // "重启"：全新 Catalog + 同一 WAL/store
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    let ingestor = new_ingestor(&env, catalog.clone());
    let (redone, committed) = ingestor.resume_recovered().await.unwrap();
    assert_eq!(
        (redone, committed),
        (0, 1),
        "终态批次应重提交（复用原 batch_id），而非重做"
    );

    // 对象存储零新增（旧语义会全量重写历史）
    let after = object_paths(&env.store).await;
    assert_eq!(before, after, "重提交不得产生新对象");

    // 文件对查询可见（内存 Catalog 已重建）
    let snap = catalog.current_snapshot().await;
    let visible = catalog.list_visible_files(T1, snap, None).await.unwrap();
    assert_eq!(visible.len(), 1, "重提交后文件应可见");

    // 认领集就位：重放时该批次 Data 不再入账
    let skip = ingestor.replay_skip.lock().unwrap();
    assert_eq!(skip.len(), 1);
    assert_eq!(skip[0].table, T1);
    assert_eq!(skip[0].shard, "default");
    assert_eq!(skip[0].epoch, 1, "CREATE 后表世代为 1");
    assert!(skip[0].start < skip[0].end, "半开区间 [start, end)");
}

#[tokio::test]
async fn recommit_respects_table_generation_gate() {
    // R9：DROP → 同名重建 → 重启，旧世代数据不得挂到新表上
    let dir = yuntun_testkit::TestDir::tmpfs("m0a-gate");
    let env = Env {
        wal_dir: dir.path().to_path_buf(),
        store: yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap(),
    };
    let wal = wal_writer(&env).await;
    append_ddl(&wal, ddl_op::CREATE_TABLE, T1).await;
    append_committed_batch(&wal, &env, "b-old", T1, &[1]).await;
    append_ddl(&wal, ddl_op::DROP_TABLE, T1).await;
    append_ddl(&wal, ddl_op::CREATE_TABLE, T1).await; // epoch +1
    drop(wal);

    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    let ingestor = new_ingestor(&env, catalog.clone());
    let (redone, committed) = ingestor.resume_recovered().await.unwrap();
    assert_eq!(
        (redone, committed),
        (0, 0),
        "旧世代批次不得重提交到重建的同名表上"
    );

    let snap = catalog.current_snapshot().await;
    assert!(
        catalog
            .list_visible_files(T1, snap, None)
            .await
            .unwrap()
            .is_empty(),
        "重建后的新表不得看到旧世代数据"
    );
}

#[tokio::test]
async fn pending_redo_filters_interleaved_foreign_rows() {
    // R8：交错写入下组内 seq 有空洞（属于别的表），Pending 重做必须按表过滤——
    // 旧行为会把 t2 的行混进 t1 的批文件（跨表串数据）
    let dir = yuntun_testkit::TestDir::tmpfs("m0a-interleaved");
    let env = Env {
        wal_dir: dir.path().to_path_buf(),
        store: yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap(),
    };
    let wal = wal_writer(&env).await;
    append_ddl(&wal, ddl_op::CREATE_TABLE, T1).await;
    append_ddl(&wal, ddl_op::CREATE_TABLE, T2).await;
    let s1 = append_data(&wal, T1, &[1]).await;
    let _s2 = append_data(&wal, T2, &[99]).await; // 空洞：属于 t2，未成批
    let s3 = append_data(&wal, T1, &[3]).await;
    wal.append(Record::BatchPending(BatchPendingPayload {
        batch_id: "b-t1".into(),
        table: T1.into(),
        shard: "default".into(),
        window: "2026-09-12T00:00".into(),
        wal_seq_start: s1,
        wal_seq_end: s3 + 1, // 精确 exclusive 右界（M0 修正后 flush 的写法）
        schema_version: 1,
        client_request_id: String::new(),
        created_at_ms: 1,
        row_count: 2,
    }))
    .await
    .unwrap();
    drop(wal);

    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    let ingestor = new_ingestor(&env, catalog.clone());
    let (redone, _committed) = ingestor.resume_recovered().await.unwrap();
    assert_eq!(redone, 1, "Pending 批次应被重做");

    // 重做后的批次只含 t1 的两行（t2 的交错行被过滤，不串表）
    let rec = yuntun_wal::recovery::recover(&yuntun_wal::WalConfig::for_dir(&env.wal_dir), 0, false)
        .unwrap();
    let st = &rec.states.states["b-t1"];
    assert_eq!(st.status, yuntun_model::batch::BatchStatus::Committed);
    assert_eq!(st.row_count, 2, "重做批次只应包含 t1 的 2 行");

    // t2 的交错 Data 未被认领（留给攒批重放），t2 无可见文件
    let snap = catalog.current_snapshot().await;
    assert!(
        catalog
            .list_visible_files(T2, snap, None)
            .await
            .unwrap()
            .is_empty()
    );
    let skip = ingestor.replay_skip.lock().unwrap();
    assert_eq!(skip.len(), 1);
    assert_eq!(skip[0].table, T1);
}
