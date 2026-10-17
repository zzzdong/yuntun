//! **冷读解码预算**（`§193` / D-20）的判据。
//!
//! # 为什么这条预算按"解码后的字节"算，而不是文件大小
//!
//! `§193.1` 的实测：可压缩形状（5×10^5 行低基数串）vortex 文件 **< 0.1 MB**、
//! 解码后 **12 MB**（≈ 2000×）—— 按文件大小设上界，对最危险的那类数据等于没有上界。
//! 所以 `table.rs` 的 scan 里：预检用 `file_size`（下界，能提前省掉一次 IO），
//! 记真账用 `get_array_memory_size()`（与实测内存一致）。
//!
//! 判据三条：
//! 1. 预算为 `0` ⇒ 不限（保留旧行为；对拍用）；
//! 2. 预算够大 ⇒ 查询正常，**结果一条不少**（预算不能变成静默少行）；
//! 3. 预算不够 ⇒ **整次查询失败**并点名文件与预算（不是 partial、不是空结果）。

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_model::meta::FileManifest;
use yuntun_model::ops::{CommitFilesRequest, CreateTableRequest};
use yuntun_query::{LocalCatalog, QueryEngine};
use yuntun_store::{StoreConfig, create_store};

const TABLE: &str = "public.budget";
const SHARD: &str = "s0";
const WINDOW: &str = "2026-10-17T10:00";
const ROWS: usize = 20_000;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ]))
}

fn batch() -> RecordBatch {
    let v: ArrayRef = Arc::new(Int64Array::from(
        (0..ROWS as i64).collect::<Vec<i64>>(),
    ));
    // 高基数十六进制串（近似 UUID）：**故意不好压**，让 `file_size` 接近解码大小
    let mut st = 0x2545F4914F6CDD1Du64;
    let s: ArrayRef = Arc::new(StringArray::from(
        (0..ROWS)
            .map(|_| {
                st ^= st << 13;
                st ^= st >> 7;
                st ^= st << 17;
                format!("{st:016x}{:016x}", st.rotate_left(17))
            })
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema(), vec![v, s]).unwrap()
}

async fn create_table(catalog: &Arc<dyn CatalogOps>) {
    catalog
        .create_table(CreateTableRequest {
            name: "budget".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "vortex".into(),
            ingest_config: Default::default(),
        })
        .await
        .unwrap();
}

async fn commit_file(
    store: &Arc<dyn object_store::ObjectStore>,
    catalog: &Arc<dyn CatalogOps>,
    id: &str,
) -> u64 {
    let (path, size, rows) = yuntun_format::write_batch(
        store,
        TABLE,
        SHARD,
        WINDOW,
        id,
        &batch(),
        yuntun_format::DataFormat::Vortex,
    )
    .await
    .unwrap();
    catalog
        .commit_files(CommitFilesRequest {
            table: TABLE.into(),
            batch_id: id.into(),
            client_request_id: None,
            client_request_ids: vec![],
            shard: SHARD.into(),
            time_window: WINDOW.into(),
            files: vec![FileManifest {
                file_path: path,
                batch_id: id.into(),
                file_size: size,
                row_count: rows,
                table: TABLE.into(),
                shard: SHARD.into(),
                time_window: WINDOW.into(),
                source_instance: "budget".into(),
                ..Default::default()
            }],
            schema_version: 1,
            row_count: rows,
        })
        .await
        .unwrap();
    size
}

async fn engine(
    store: Arc<dyn object_store::ObjectStore>,
    catalog: &Arc<dyn CatalogOps>,
    budget_bytes: u64,
) -> QueryEngine {
    let cache = Arc::new(LocalCatalog::new());
    cache.set_catalog_ops(catalog.clone());
    cache.refresh(catalog).await.unwrap();
    QueryEngine::new(store, cache).with_cold_read_budget_bytes(budget_bytes)
}

const SUM: &str = "SELECT sum(v) AS s FROM budget";

async fn sum_of(q: &QueryEngine) -> i64 {
    let out = q.sql(SUM).await.expect("预算够时必须成功");
    let b = &out[0];
    b.column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

/// 三个文件 ≈ 每个 0.9 MB（解码后略大于文件）；总行数 6 万。
fn expected_sum() -> i64 {
    3 * (ROWS as i64 * (ROWS as i64 - 1) / 2)
}

#[tokio::test]
async fn budget_zero_means_unlimited() {
    let store: Arc<dyn object_store::ObjectStore> = create_store(&StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;
    for id in ["b-1", "b-2", "b-3"] {
        commit_file(&store, &catalog, id).await;
    }
    let q = engine(store.clone(), &catalog, 0).await;
    assert_eq!(sum_of(&q).await, expected_sum(), "0 = 不限，结果必须完整");
}

#[tokio::test]
async fn a_big_budget_keeps_the_result_complete() {
    let store: Arc<dyn object_store::ObjectStore> = create_store(&StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;
    for id in ["b-1", "b-2", "b-3"] {
        commit_file(&store, &catalog, id).await;
    }
    let q = engine(store.clone(), &catalog, 64 * 1024 * 1024).await;
    assert_eq!(sum_of(&q).await, expected_sum());
}

#[tokio::test]
async fn a_small_budget_fails_loudly_and_names_the_limit() {
    let store: Arc<dyn object_store::ObjectStore> = create_store(&StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;
    for id in ["b-1", "b-2", "b-3"] {
        commit_file(&store, &catalog, id).await;
    }
    // 半个文件都装不下的预算
    let q = engine(store.clone(), &catalog, 512 * 1024).await;
    let err = q
        .sql(SUM)
        .await
        .expect_err("预算不足必须失败（不能静默少行）")
        .to_string();
    assert!(err.contains("冷读解码预算"), "{err}");
    assert!(err.contains("cold_read_budget_mb"), "错误里要给出改哪个键：{err}");
}

/// **累计**语义：预算够第一个文件、不够第二个 ⇒ 在第二个文件处失败（而不是把两个都读完）。
#[tokio::test]
async fn the_budget_is_cumulative_across_files() {
    let store: Arc<dyn object_store::ObjectStore> = create_store(&StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;
    let s1 = commit_file(&store, &catalog, "b-1").await;
    let s2 = commit_file(&store, &catalog, "b-2").await;

    // 预算 = 第一个文件 + 一半第二个文件 ⇒ 第一个读得下、第二个读不下
    let budget = s1 + s2 / 2;
    let q = engine(store.clone(), &catalog, budget).await;
    let err = q.sql(SUM).await.expect_err("第二个文件必须被挡住").to_string();
    assert!(err.contains("冷读解码预算"), "{err}");
}
