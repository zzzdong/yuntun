//! **`durable` 的端到端验收**（`D-6`，`§126`）：归档 → **整盘丢失** → 拉回 → **重新提交成可见文件**。
//!
//! # 与 `wal_archive_durable.rs` 的分工
//!
//! | 用例 | 证到哪一步 |
//! |---|---|
//! | `wal_archive_durable.rs` | **机制级**：段进了共享存储、丢盘后能拉回、拉回的字节能通过 CRC 被读回 |
//! | **本文件** | **端到端**：拉回之后走**既有恢复通路**（`replay_wal_ddl` + `resume_recovered` 的 `Pending` 分支），那批"ack 过但还没提交"的数据**重新变成 Catalog 里的可见文件** |
//!
//! # 场景为什么是这里这一种
//!
//! 要保护的窗口是"**客户端已 ack、但还没 flush+commit**"（`§125.0` 量过：几十秒）。
//! 所以本用例刻意写一个 **`Pending`** 批次（只写 `Data` + `BatchPending`，**不写** `BatchS3Written`
//! /`BatchCommitted`），然后在"整盘丢失"之后要求它被**重做**（`resume_recovered` 返回 `redone=1`）
//! 并出现在 `list_visible_files` 里。
//!
//! # 两个忠实的细节
//!
//! * **Catalog 活着**：生产里它是**另一个服务**（meta raft），不随 datanode 的盘一起没 ⇒ 本用例
//!   用的是**同一个** `MemoryCatalog`（只丢私有目录）；DDL 仍按生产顺序重放（幂等，已存在即忽略）。
//! * **对照组**：一模一样，只是**不归档** ⇒ 丢盘之后一条也回不来（`redone=0`、无可见文件）。

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_ingest::wal_archive::{archive_once, restore, ArchiveConfig};
use yuntun_ingest::{Ingestor, IngestorConfig};
use yuntun_model::wal_record::{ddl_op, BatchPendingPayload, DataPayload, DdlPayload, Record};
use yuntun_model::meta::serialize_schema;

const T: &str = "public.durable";

fn archive_cfg() -> ArchiveConfig {
    ArchiveConfig {
        prefix: "wal-archive".into(),
        instance_id: "inst-1".into(),
        interval: std::time::Duration::from_secs(1),
    }
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

/// 写一个 **`Pending`** 批次（= "客户端已 ack、还没提交"那段窗口里的数据）。
async fn write_pending_batch(wal_dir: &std::path::Path, batch_id: &str, values: &[i64]) {
    let wal = yuntun_wal::writer::WalWriter::open(yuntun_wal::WalConfig::for_dir(wal_dir), 0)
        .await
        .expect("开 WAL");
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    wal.append(Record::Ddl(DdlPayload {
        op: ddl_op::CREATE_TABLE,
        table: T.into(),
        arrow_schema: serialize_schema(&schema),
        default_format: "parquet".into(),
    }))
    .await
    .expect("写 DDL");
    let first = wal
        .append(Record::Data(DataPayload {
            table: T.into(),
            shard: "default".into(),
            schema_version: 1,
            batch_ipc: ipc_bytes(&schema, values),
            client_request_id: "rid-1".into(),
            time_window: "2026-09-26T00:00".into(),
        }))
        .await
        .expect("append（这可视为客户端已经 ack）")
        .seq;
    wal.append(Record::BatchPending(BatchPendingPayload {
        batch_id: batch_id.into(),
        table: T.into(),
        shard: "default".into(),
        window: "2026-09-26T00:00".into(),
        wal_seq_start: first,
        wal_seq_end: first + values.len() as u64,
        schema_version: 1,
        client_request_id: "rid-1".into(),
        created_at_ms: 1,
        row_count: values.len() as u64,
    }))
    .await
    .expect("写 BatchPending");
    // ⚠️ 刻意**不写** BatchS3Written / BatchCommitted —— 这就是"还没提交"。
}

/// **整盘丢失之后**：拉回归档 → 起 Ingestor → 走既有恢复通路，返回 `(redone, committed)`。
async fn recover_after_total_disk_loss(
    wal_dir: &std::path::Path,
    store: &Arc<dyn object_store::ObjectStore>,
    catalog: Arc<dyn CatalogOps>,
    with_archive: bool,
) -> (usize, usize) {
    if with_archive {
        let n = restore(&archive_cfg(), wal_dir, 0, store.as_ref())
            .await
            .expect("从归档拉回");
        assert!(n >= 1, "应当至少拉回一个段（实测 {n}）");
    }
    let wal = yuntun_wal::writer::WalWriter::open(yuntun_wal::WalConfig::for_dir(wal_dir), 0)
        .await
        .expect("开 WAL（归档段此刻已经在本地目录里）");
    // 生产顺序：先重放 DDL，再恢复批次（`server/src/lib.rs`）。
    yuntun_ingest::replay_wal_ddl(&catalog, &wal)
        .await
        .expect("重放 DDL");
    let ingestor = Ingestor::new(
        IngestorConfig {
            rows_threshold: 1_000_000,
            ..Default::default()
        },
        wal,
        catalog,
        store.clone(),
    );
    ingestor.resume_recovered().await.expect("恢复")
}

#[tokio::test]
async fn durable_batch_becomes_visible_after_total_disk_loss() {
    let dir = yuntun_testkit::TestDir::tmpfs("wal-archive-e2e");
    let wal_dir = dir.path().to_path_buf();
    // 共享存储（生产 = S3）：冷数据与归档同在（不同前缀），**都不随私有盘一起没**
    let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());

    write_pending_batch(&wal_dir, "b-durable", &[1, 2, 3]).await;

    // 归档一轮（机制：可注入间隔；生产里是后台循环）
    let mut uploaded = std::collections::HashMap::new();
    let up = archive_once(&archive_cfg(), &wal_dir, 0, store.as_ref(), &mut uploaded)
        .await
        .expect("归档");
    assert!(up >= 1, "归档应当传了 {up} 个段");

    // **整盘丢失**：私有目录整个删掉
    std::fs::remove_dir_all(&wal_dir).expect("删掉私有目录：模拟整盘丢失");

    let (redone, committed) =
        recover_after_total_disk_loss(&wal_dir, &store, catalog.clone(), true).await;
    assert_eq!(
        redone, 1,
        "**有归档** ⇒ 那批 ack 过、还没提交的数据必须被**重做**（redone={redone} committed={committed}）"
    );

    let snap = catalog.current_snapshot().await;
    let visible = catalog.list_visible_files(T, snap, None).await.unwrap();
    assert_eq!(
        visible.len(),
        1,
        "重做之后它必须**对查询可见**（这才是 `durable` 的全部意义）：{visible:?}"
    );
}

#[tokio::test]
async fn without_archive_the_acked_batch_is_gone() {
    let dir = yuntun_testkit::TestDir::tmpfs("wal-archive-e2e-control");
    let wal_dir = dir.path().to_path_buf();
    let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());

    write_pending_batch(&wal_dir, "b-control", &[1, 2, 3]).await;
    // 对照组：**不归档**，直接丢盘
    std::fs::remove_dir_all(&wal_dir).expect("删掉私有目录：模拟整盘丢失");

    let (redone, _) =
        recover_after_total_disk_loss(&wal_dir, &store, catalog.clone(), false).await;
    assert_eq!(
        redone, 0,
        "**没归档** ⇒ 恢复通路找不到任何东西（{redone}）—— 这就是 `best_effort` 承认的那句话；\
         两次的差别只有归档，所以上面那次恢复**不可能是别的原因**"
    );
    let snap = catalog.current_snapshot().await;
    assert!(
        catalog
            .list_visible_files(T, snap, None)
            .await
            .unwrap()
            .is_empty(),
        "对照组不该有可见文件"
    );
}
