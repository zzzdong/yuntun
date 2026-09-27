//! **F.3d 验收：compaction 消费删除向量**（`plan.md` F.3 / `delta-dml-design §6.2` / `§150`）。
//!
//! 这条守的是 F.3 里**最危险的那个窗口**：数据文件不可变，删除是"行位标记"；
//! 合并会把基文件重读一遍再写一个新文件 —— 若不把 DV 应用上去，那些**已删的行会被原样写进新文件**
//! （复活），而且此后没有任何 DV 能解释它们（新文件的行号与旧文件不成对应）。
//!
//! 所以断言只有一条但足够狠：**合并前后逐行一致**（`F.4` 的对拍纪律用在删除上）。
//!
//! 外加三条内部事实：
//! * 合并产物**不含**被删的行（行数 = 输入行数 − 删除数）；
//! * 消费掉的 DV 被 `revoke`（当前快照不再生效）—— 但**合并之前的快照仍看得到它**（快照隔离）；
//! * 合并前查得到"删掉的行不见了"，合并后**仍然是**（不是"合并把它救回来了"）。

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_model::dv::{DeletionEntry, DvBitmap, dv_object_path};
use yuntun_model::meta::FileManifest;
use yuntun_model::ops::{CommitFilesRequest, CreateTableRequest};
use yuntun_query::{LocalCatalog, QueryEngine};

const TABLE: &str = "audit";

fn schema() -> arrow::datatypes::SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "event_time",
        DataType::Int64,
        false,
    )]))
}

fn parquet_of(values: Vec<i64>) -> (Vec<u8>, usize) {
    use parquet::arrow::ArrowWriter;
    let b = arrow::record_batch::RecordBatch::try_new(
        schema(),
        vec![Arc::new(Int64Array::from(values))],
    )
    .unwrap();
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema(), None).unwrap();
    w.write(&b).unwrap();
    w.close().unwrap();
    (buf, b.num_rows())
}

struct Fx {
    store: Arc<dyn object_store::ObjectStore>,
    catalog: Arc<dyn CatalogOps>,
    cache: Arc<LocalCatalog>,
    engine: QueryEngine,
    a_path: String,
}

impl Fx {
    async fn rows(&self) -> Vec<i64> {
        let batches = self
            .engine
            .sql(&format!("SELECT event_time FROM {TABLE} ORDER BY event_time"))
            .await
            .unwrap();
        let mut out = Vec::new();
        for b in &batches {
            let c = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            out.extend((0..c.len()).map(|i| c.value(i)));
        }
        out
    }

    async fn delete(&self, dv_id: &str, file: &str, positions: &[u32]) {
        let dv = DvBitmap::from_positions(positions.iter().copied());
        let entry = DeletionEntry {
            dv_id: dv_id.into(),
            table: format!("public.{TABLE}"),
            file_path: file.into(),
            batch_id: "b-a".into(),
            applied_at: 0,
            revoked_at: 0,
            card: dv.card() as u32,
            store_path: dv_object_path(file, dv_id),
        };
        yuntun_store::put_bytes(&*self.store, &entry.store_path, dv.to_bytes())
            .await
            .unwrap();
        self.catalog.apply_deletions(vec![entry]).await.unwrap();
        self.cache.refresh(&self.catalog).await.unwrap();
    }
}

/// 两个文件（A：0,2,4,6,8；B：100..105）+ 一个真实的 DV（删 A 的第 1、3 行 = 值 2、6）。
async fn fixture() -> Fx {
    let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    catalog
        .create_table(CreateTableRequest {
            name: TABLE.into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig::standard(),
        })
        .await
        .unwrap();

    let (a_bytes, a_rows) = parquet_of(vec![0, 2, 4, 6, 8]);
    let (b_bytes, b_rows) = parquet_of(vec![100, 101, 102, 103, 104]);
    let a_path = format!("yuntun/public/{TABLE}/dt=w/shard=s0/a.parquet");
    let b_path = format!("yuntun/public/{TABLE}/dt=w/shard=s0/b.parquet");
    yuntun_store::put_bytes(&*store, &a_path, a_bytes.clone())
        .await
        .unwrap();
    yuntun_store::put_bytes(&*store, &b_path, b_bytes.clone())
        .await
        .unwrap();
    for (batch, path, bytes, rows) in [
        ("b-a", &a_path, &a_bytes, a_rows as u64),
        ("b-b", &b_path, &b_bytes, b_rows as u64),
    ] {
        catalog
            .commit_files(CommitFilesRequest {
                table: TABLE.into(),
                batch_id: batch.into(),
                client_request_id: None,
                client_request_ids: vec![],
                shard: "s0".into(),
                time_window: "w".into(),
                files: vec![FileManifest {
                    file_path: path.clone(),
                    file_size: bytes.len() as u64,
                    row_count: rows,
                    ..Default::default()
                }],
                schema_version: 1,
                row_count: rows,
            })
            .await
            .unwrap();
    }

    let cache = Arc::new(LocalCatalog::new());
    cache.refresh(&catalog).await.unwrap();
    let engine = QueryEngine::new(store.clone(), cache.clone());
    Fx {
        store,
        catalog,
        cache,
        engine,
        a_path,
    }
}

#[tokio::test]
async fn compaction_consumes_the_deletion_vector_and_keeps_the_row_set_exact() {
    let f = fixture().await;
    f.delete("dv-a", &f.a_path, &[1, 3]).await;

    // 合并前：被删的 2、6 不见，其余都在
    let before = f.rows().await;
    assert_eq!(before, vec![0, 4, 8, 100, 101, 102, 103, 104]);

    // 真合并（`min_files: 1` ⇒ 这个 shard 的两个文件合成一个）
    let compactor = Arc::new(yuntun_compaction::Compactor {
        cfg: yuntun_compaction::CompactionConfig {
            min_files: 1,
            ..Default::default()
        },
        catalog: f.catalog.clone(),
        store: f.store.clone(),
        format: yuntun_format::DataFormat::Parquet,
        lease_holder: "comp-test".into(),
    });
    let snap = f.catalog.current_snapshot().await;
    let new_snap = yuntun_compaction::compact_shard(&compactor, &format!("public.{TABLE}"), "s0", snap, 0)
        .await
        .unwrap()
        .expect("两个文件 + min_files=1 ⇒ 必须合并");

    // 产物：行数 = 5 + 5 − 2（被删的两行**没有**被写进新文件）
    f.cache.refresh(&f.catalog).await.unwrap();
    let files = f
        .catalog
        .list_visible_files(&format!("public.{TABLE}"), new_snap, None)
        .await
        .unwrap();
    assert_eq!(files.len(), 1, "合并后应当只剩一个文件");
    assert_eq!(files[0].row_count, 8, "被删的两行不许出现在产物里");

    // **对拍**：合并前后逐行一致
    assert_eq!(
        f.rows().await,
        before,
        "合并不得改变行集（把已删的行写回新文件就是**复活**）"
    );

    // 消费掉的 DV 被撤销：当前快照不再生效……
    assert!(
        f.catalog
            .list_deletions(&format!("public.{TABLE}"), new_snap)
            .await
            .unwrap()
            .is_empty(),
        "合并消费掉的 DV 必须 revoke（否则它会一直挂在已转墓碑的文件名上）"
    );
    // ……但**合并之前**的快照仍看得到它（快照隔离：历史可以说清"当时为什么少两行"）
    assert_eq!(
        f.catalog
            .list_deletions(&format!("public.{TABLE}"), snap)
            .await
            .unwrap()
            .len(),
        1
    );
}

/// **整文件被删光**：合并的产物里不该留下任何"幽灵行"，也不该因为空批次而炸掉。
#[tokio::test]
async fn compaction_handles_a_fully_deleted_file() {
    let f = fixture().await;
    f.delete("dv-a", &f.a_path, &[0, 1, 2, 3, 4]).await; // A 全删
    assert_eq!(f.rows().await, vec![100, 101, 102, 103, 104]);

    let compactor = Arc::new(yuntun_compaction::Compactor {
        cfg: yuntun_compaction::CompactionConfig {
            min_files: 1,
            ..Default::default()
        },
        catalog: f.catalog.clone(),
        store: f.store.clone(),
        format: yuntun_format::DataFormat::Parquet,
        lease_holder: "comp-test".into(),
    });
    let snap = f.catalog.current_snapshot().await;
    let new_snap = yuntun_compaction::compact_shard(
        &compactor,
        &format!("public.{TABLE}"),
        "s0",
        snap,
        0,
    )
    .await
    .unwrap()
    .expect("必须合并");

    f.cache.refresh(&f.catalog).await.unwrap();
    let files = f
        .catalog
        .list_visible_files(&format!("public.{TABLE}"), new_snap, None)
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].row_count, 5, "只剩 B 的 5 行");
    assert_eq!(
        f.rows().await,
        vec![100, 101, 102, 103, 104],
        "A 的 5 行删光之后，合并也不许把它们带回来"
    );
}
