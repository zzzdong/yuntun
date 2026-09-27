//! **F.3d-2 验收：删除向量自己触发合并**（`delta-dml-design §6.2` / `operation-log §152`，
//! 台账 `D-10`）。
//!
//! 为什么需要这条：删除是"标记"（读侧 merge-on-read），**不消费就永远不收敛**。
//! 而原来的触发条件只有"同 shard 文件数 ≥ `min_files`" —— 一张写入稀疏的表可能永远等不到
//! 那一刻，DV 就一直挂着（读放大 O(活跃 DV 数)）。
//!
//! 三条断言（两条正面 + 两条反面）：
//!
//! 1. **单文件 + 大删除 ⇒ 自动合并**（`min_files = 5`，只有 1 个文件 —— 只可能是 DV 触发）；
//!    合并后行集**逐行不变**、DV 被消费（撤销）、产物行数 = 原行数 − 删除数；
//! 2. **小删除 ⇒ 不重写**（`card < dv_min_card`）：防"删几行就重写一个大文件"的抖动；
//! 3. **占比不足 ⇒ 不重写**（大文件删掉 < 10%）：同上是防抖动的另一半。
//!
//! 反面两条同样重要 —— **每轮都重写**本身就是一种新的抖动源（写放大）。

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

struct Fx {
    /// 与查询/合并**共用**的存储（memory store 每次 `create_store` 都是新实例 ——
    /// 夹具里必须显式共享，否则"合并读不到 DV"这类错误会伪装成产品缺陷）。
    store: Arc<dyn object_store::ObjectStore>,
    catalog: Arc<dyn CatalogOps>,
    cache: Arc<LocalCatalog>,
    engine: QueryEngine,
}

impl Fx {
    async fn rows(&self) -> Vec<i64> {
        let batches = self
            .engine
            .sql(&format!("SELECT count(*) FROM {TABLE}"))
            .await
            .unwrap();
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        vec![c.value(0)]
    }
}

/// 建表 + 一个 `rows` 行的文件 + 一份删掉 `deleted` 行的 DV。
async fn fixture(rows: usize, deleted: usize) -> Fx {
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

    let values: Vec<i64> = (0..rows as i64).collect();
    let b = arrow::record_batch::RecordBatch::try_new(
        schema(),
        vec![Arc::new(Int64Array::from(values))],
    )
    .unwrap();
    let (path, bytes, rows_written) = yuntun_format::write_batch(
        &store,
        &format!("public.{TABLE}"),
        "s0",
        "w",
        "b-single",
        &b,
        yuntun_format::DataFormat::Parquet,
    )
    .await
    .unwrap();
    catalog
        .commit_files(CommitFilesRequest {
            table: TABLE.into(),
            batch_id: "b-single".into(),
            client_request_id: None,
            client_request_ids: vec![],
            shard: "s0".into(),
            time_window: "w".into(),
            files: vec![FileManifest {
                file_path: path.clone(),
                file_size: bytes,
                row_count: rows_written,
                ..Default::default()
            }],
            schema_version: 1,
            row_count: rows as u64,
        })
        .await
        .unwrap();

    let dv = DvBitmap::from_positions((0..deleted as u32).collect::<Vec<u32>>());
    let entry = DeletionEntry {
        dv_id: "dv-big".into(),
        table: format!("public.{TABLE}"),
        file_path: path.clone(),
        batch_id: "b-single".into(),
        applied_at: 0,
        revoked_at: 0,
        card: dv.card() as u32,
        store_path: dv_object_path(&path, "dv-big"),
    };
    yuntun_store::put_bytes(&*store, &entry.store_path, dv.to_bytes())
        .await
        .unwrap();
    catalog.apply_deletions(vec![entry]).await.unwrap();

    let cache = Arc::new(LocalCatalog::new());
    cache.refresh(&catalog).await.unwrap();
    let engine = QueryEngine::new(store.clone(), cache.clone());
    Fx {
        store,
        catalog,
        cache,
        engine,
    }
}

/// `min_files = 5`（默认）而只有 1 个文件 ⇒ 唯一能触发的原因就是 DV。
fn compactor(fx: &Fx) -> Arc<yuntun_compaction::Compactor> {
    Arc::new(yuntun_compaction::Compactor {
        cfg: yuntun_compaction::CompactionConfig::default(),
        catalog: fx.catalog.clone(),
        store: fx.store.clone(),
        format: yuntun_format::DataFormat::Parquet,
        lease_holder: "comp-dv".into(),
    })
}

async fn try_compact(fx: &Fx) -> Option<u64> {
    let c = compactor(fx);
    let snap = fx.catalog.current_snapshot().await;
    yuntun_compaction::compact_shard(&c, &format!("public.{TABLE}"), "s0", snap, 0)
        .await
        .unwrap()
}

/// ① 单文件 + 大删除（60%）⇒ **DV 自己触发**合并，且行集逐行不变。
#[tokio::test]
async fn a_single_file_with_a_big_deletion_is_compacted_by_the_dv() {
    let fx = fixture(2_000, 1_200).await;
    assert_eq!(fx.rows().await, vec![800], "前提：读侧应用 DV 后剩 800 行");

    let new_snap = try_compact(&fx)
        .await
        .expect("单文件 + 1200 行删除（60%）必须被 DV 触发合并");

    fx.cache.refresh(&fx.catalog).await.unwrap();
    let files = fx
        .catalog
        .list_visible_files(&format!("public.{TABLE}"), new_snap, None)
        .await
        .unwrap();
    assert_eq!(files.len(), 1, "重写成一个文件（不是新增一个）");
    assert_eq!(files[0].row_count, 800, "产物里只剩没被删的行");
    assert_eq!(fx.rows().await, vec![800], "**行数不变**（重写不得改变行集）");
    assert!(
        fx.catalog
            .list_deletions(&format!("public.{TABLE}"), new_snap)
            .await
            .unwrap()
            .is_empty(),
        "消费掉的 DV 必须被撤销（否则它会一直挂在已转墓碑的文件名上）"
    );
}

/// ② 小删除（`card < dv_min_card`）⇒ **不重写**（防抖动）。
#[tokio::test]
async fn a_tiny_deletion_does_not_trigger_a_rewrite() {
    let fx = fixture(5_000, 5).await;
    assert!(try_compact(&fx).await.is_none(), "删 5 行不值得重写 5000 行的文件");
    // 而且**读侧照样是对的**（不重写 ≠ 删不掉：DV 还在，读侧一直在应用它）
    assert_eq!(fx.rows().await, vec![4_995]);
    assert_eq!(
        fx.catalog
            .list_deletions(
                &format!("public.{TABLE}"),
                fx.catalog.current_snapshot().await
            )
            .await
            .unwrap()
            .len(),
        1,
        "DV 仍生效（重写只是把删除物理化，不是删除的唯一载体）"
    );
}

/// ③ 占比不足（大文件删掉 7.5%）⇒ **不重写**（防抖动的另一半）。
#[tokio::test]
async fn a_low_ratio_deletion_does_not_trigger_a_rewrite() {
    let fx = fixture(20_000, 1_500).await;
    assert!(
        try_compact(&fx).await.is_none(),
        "1500/20000 = 7.5% < 10% ⇒ 单文件不合并（等文件数够了再顺带消费）"
    );
    assert_eq!(fx.rows().await, vec![18_500]);
}

/// 触发判定的边界（纯函数，与上面三条互为对照 —— 免得"用例恰好都没踩到阈值"）。
#[test]
fn the_trigger_needs_both_guards() {
    let cfg = yuntun_compaction::CompactionConfig::default();
    assert!(yuntun_compaction::dv_worth_compacting(&cfg, 1_200, 2_000), "60% ⇒ 是");
    assert!(yuntun_compaction::dv_worth_compacting(&cfg, 1_000, 10_000), "10% 且 1000 行 ⇒ 是（正好踩线）");
    assert!(!yuntun_compaction::dv_worth_compacting(&cfg, 999, 2_000), "下界差一行 ⇒ 否");
    assert!(!yuntun_compaction::dv_worth_compacting(&cfg, 1_500, 20_000), "7.5% ⇒ 否");
    assert!(!yuntun_compaction::dv_worth_compacting(&cfg, 0, 2_000), "没删 ⇒ 否");
    assert!(!yuntun_compaction::dv_worth_compacting(&cfg, 10, 0), "空文件 ⇒ 否");
    assert!(!yuntun_compaction::dv_worth_compacting(&cfg, 99, 10), "位图比行数还多（损坏）⇒ 否");
}
