//! `§178`：**冷文件分配**的接缝与"完整"判据（N4b 的第一刀）。
//!
//! 冷文件在**共享**存储上 ⇒ 任何节点都能读，"谁来读"是**调度**问题。这一刀先把分配
//! 变成一个纯函数（`yuntun_query::placement::assign`），并把"这一支只读分配给它的文件"
//! 做成接缝（`QueryEngine::with_only_files`）。**执行**仍在协调者本地 —— 把读取搬走是下一刀。
//!
//! 判据有三条，**最要紧的是第三条**（它把"完整"从纯函数的层面推到了**数据**层面）：
//! 1. 接缝生效：只给一个文件 ⇒ 只看到那几行；
//! 2. 一个都不给 ⇒ `count(*)` 是 0、`sum` 是 NULL（不是"报错"，也不是"全表"）；
//! 3. **按分配切成两半分别查，加起来 == 全量查** —— 漏一个文件 = 少数据、重一个 = 双计，
//!    这两种错都**不报错**，只有这条判据抓得住。

use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_model::meta::FileManifest;
use yuntun_model::ops::{CommitFilesRequest, CreateTableRequest};
use yuntun_query::{LocalCatalog, QueryEngine, placement};
use yuntun_store::{StoreConfig, create_store};

const TABLE: &str = "public.place";
const SHARD: &str = "s0";
const WINDOW: &str = "2026-10-12T10:00";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]))
}

fn batch(vals: &[Option<i64>]) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
}

async fn create_table(catalog: &Arc<dyn CatalogOps>) {
    catalog
        .create_table(CreateTableRequest {
            name: "place".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: Default::default(),
        })
        .await
        .unwrap();
}

/// 提交一个**真 parquet 文件**；返回它的 `file_path`（分配的键就是它）。
async fn commit_file(
    store: &Arc<dyn object_store::ObjectStore>,
    catalog: &Arc<dyn CatalogOps>,
    id: &str,
    vals: &[Option<i64>],
) -> String {
    let (path, size, rows) = yuntun_format::write_batch(
        store,
        TABLE,
        SHARD,
        WINDOW,
        id,
        &batch(vals),
        yuntun_format::DataFormat::Parquet,
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
                file_path: path.clone(),
                batch_id: id.into(),
                file_size: size,
                row_count: rows,
                table: TABLE.into(),
                shard: SHARD.into(),
                time_window: WINDOW.into(),
                source_instance: "cold".into(),
                ..Default::default()
            }],
            schema_version: 1,
            row_count: rows,
        })
        .await
        .unwrap();
    path
}

/// 一个只带这张表的引擎；`only` = 只读这几个文件（`None` = 全读）。
///
/// ⚠️ **必须传写入时那个 store**：`StoreConfig::Memory` 每建一次就是**另一份**存储 ——
/// 第一次写这里就踩过（"Object at location … not found"），而那种失败看起来像
/// "文件不存在"，实际是"读到了另一个空 store"。
async fn engine(
    store: Arc<dyn object_store::ObjectStore>,
    catalog: &Arc<dyn CatalogOps>,
    only: Option<Vec<String>>,
) -> QueryEngine {
    let cache = Arc::new(LocalCatalog::new());
    cache.set_catalog_ops(catalog.clone());
    cache.refresh(catalog).await.unwrap();
    QueryEngine::new(store, cache).with_only_files(only)
}

/// 一条 `(count, sum)`。
async fn counts(q: &QueryEngine, sql: &str) -> (i64, Option<i64>) {
    let out = q.sql(sql).await.unwrap();
    let b = &out[0];
    let c = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let s = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    (
        c.value(0),
        if s.is_null(0) { None } else { Some(s.value(0)) },
    )
}

const Q: &str = "SELECT count(*) AS c, sum(v) AS s FROM place";

/// ① 接缝生效：只给一个文件 ⇒ 只看到那几行。
#[tokio::test]
async fn only_files_restricts_the_cold_read() {
    let store: Arc<dyn object_store::ObjectStore> = create_store(&StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;
    let a = commit_file(&store, &catalog, "b-a", &[Some(1), Some(2)]).await;
    let b = commit_file(&store, &catalog, "b-b", &[Some(10), Some(20)]).await;

    let all = engine(store.clone(), &catalog, None).await;
    assert_eq!(counts(&all, Q).await, (4, Some(33)));

    let only_a = engine(store.clone(), &catalog, Some(vec![a])).await;
    assert_eq!(counts(&only_a, Q).await, (2, Some(3)), "只看到文件 a");

    let only_b = engine(store.clone(), &catalog, Some(vec![b])).await;
    assert_eq!(counts(&only_b, Q).await, (2, Some(30)), "只看到文件 b");
}

/// ② 一个文件都不给 ⇒ `count(*)` 是 0、`sum` 是 NULL（**不是**报错，也**不是**退化成全表）。
#[tokio::test]
async fn an_empty_allowlist_reads_nothing_but_still_answers() {
    let store: Arc<dyn object_store::ObjectStore> = create_store(&StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;
    commit_file(&store, &catalog, "b-a", &[Some(1), Some(2)]).await;

    let none = engine(store.clone(), &catalog, Some(vec!["p/does-not-exist".into()])).await;
    assert_eq!(
        counts(&none, Q).await,
        (0, None),
        "没有文件 ⇒ count 0 / sum NULL（与空表同语义）"
    );
}

/// ③ **判据的核心**：按分配切成两半分别查，加起来必须 == 全量查。
///
/// 漏一个文件 = 少数据、重一个文件 = 双计 —— 两种都不报错，只有这条抓得住。
/// （顺便：`assign` 的"完整"因此不再只是纯函数层面的性质，而是**数据**层面的。）
#[tokio::test]
async fn assignments_split_the_data_evenly_and_add_up_to_the_whole() {
    let store: Arc<dyn object_store::ObjectStore> = create_store(&StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;
    let mut files = Vec::new();
    for (i, vals) in [
        vec![Some(1), Some(2), Some(3)],
        vec![Some(10)],
        vec![Some(20), Some(30)],
        vec![Some(40), Some(50), Some(60), Some(70)],
    ]
    .into_iter()
    .enumerate()
    {
        let p = commit_file(&store, &catalog, &format!("b-{i}"), &vals).await;
        files.push(FileManifest {
            file_path: p,
            ..Default::default()
        });
    }

    let whole = engine(store.clone(), &catalog, None).await;
    let (wc, ws) = counts(&whole, Q).await;
    assert_eq!((wc, ws), (10, Some(286)), "前提：全量是 10 行（6+10+50+220）");

    // 分给两个"节点"（其中一个就是本地 —— 空实例 id）
    // 本地（空实例 id）+ dn1 —— 调用方要把**自己**也放进去
    let got = placement::assign(&files, &[String::new(), "dn1".into()]);
    assert_eq!(got.len(), 2, "本地 + dn1");
    assert_eq!(got[0].instance, "", "空实例 id = 本地（排序在前）");

    let mut sum_c = 0i64;
    let mut sum_s = 0i64;
    for a in &got {
        let paths: Vec<String> = a.files.iter().map(|f| f.file_path.clone()).collect();
        assert!(!paths.is_empty(), "每个节点都该分到活干：{got:?}");
        let q = engine(store.clone(), &catalog, Some(paths)).await;
        let (c, s) = counts(&q, Q).await;
        sum_c += c;
        sum_s += s.unwrap_or(0);
    }
    assert_eq!(sum_c, wc, "各份的**行数**之和必须等于全量（漏/重都在这里现形）");
    assert_eq!(sum_s, ws.unwrap(), "各份的**求和**之和也必须等于全量");
}
