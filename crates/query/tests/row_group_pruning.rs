//! **块级（row-group）剪枝在工作** —— 但这条链**不是我们接的**（`§132`，含一次实测更正）。
//!
//! # 它证什么
//!
//! 一个 parquet 文件、**两个 row group**（每组 100 行，`event_time` 递增 ⇒ 两组统计区间不相交），
//! 于是 `WHERE event_time < 100` 能证明"第二组**不可能**命中"：
//! `EXPLAIN ANALYZE` 里 `row_groups_pruned_statistics > 0`，且结果照旧正确。
//!
//! ⚠️ 这条断言**必须**靠"块级"：文件级的 `[min, max]` 是 `[0, 199]`、与谓词**有交集**
//! ⇒ 文件级剪枝器（`§129`）在这里一条都剪不掉。两半各管一段，互不冒充。
//!
//! # ⚠️ 它**不**证什么（`§132` 的实验，别误读）
//!
//! 我原以为"块级剪枝"是我们该补的另一半，于是往 `scan` 里加了一次谓词下推
//!（`ParquetSource::with_predicate`）。**实测把那次下推关掉，本条用例照样绿** ——
//! 谓词是 **DataFusion 的优化器自己**推进 `DataSourceExec` 的，`ParquetSource` 早就在用
//! row-group 统计跳读了。⇒ 那根线**冗余**，已回退；`§129.3` 里"那句注释是假的"这个判读
//! 也据此更正（注释说的是**优化器**的行为，它没写错）。
//!
//! 所以本条守护的是**系统行为**（"这条链别退化"），不指向某一行代码；
//! 真正属于**我们**的那一半（**文件级**剪枝）由 `query/src/prune.rs` 的 8 条单测守着。
//!
//! # 为什么不用 `e2e.rs` 那套真 ingest
//!
//! 第一版就是那么写的（真 WAL + 7 万行 ⇒ 才够 `yuntun-format` 的 65,536 行组大小）。
//! 它**能过**，但全量并行跑时把 `chaos` 的 `disk_watermark_aborts_oldest_batch_…` 挤红了
//! （那条断言"WAL 批次全部终态"，单独跑 3/3 绿）—— 造测试数据的代价**转嫁给了邻居**。
//! 现在：内存存储 + 手写小 row group ⇒ **毫秒级、不碰盘、不碰 WAL**。

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_model::meta::FileManifest;
use yuntun_model::ops::{CommitFilesRequest, CreateTableRequest};
use yuntun_query::{LocalCatalog, QueryEngine};

const ROWS: usize = 200;
const GROUP: usize = 100; // ⇒ 两个 row group

fn schema() -> arrow::datatypes::SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_time", DataType::Int64, false),
        Field::new("user", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
    ]))
}

fn batch() -> arrow::record_batch::RecordBatch {
    arrow::record_batch::RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<i64>>())),
            Arc::new(StringArray::from(vec![Some("alice"); ROWS])),
            Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<i64>>())),
        ],
    )
    .unwrap()
}

/// 手写一个**小 row group** 的 parquet（`yuntun-format` 的组大小是常量，做不到这么小）。
fn parquet_bytes() -> Vec<u8> {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(GROUP))
        .build();
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema(), Some(props)).unwrap();
    w.write(&batch()).unwrap();
    w.close().unwrap();
    buf
}

fn parse_metric(plan: &str, name: &str) -> Option<u64> {
    let i = plan.find(name)? + name.len();
    let rest = plan[i..].trim_start_matches(['=', ':', ' ', '\t']);
    rest.chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse()
        .ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_groups_are_pruned_by_statistics() {
    let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    catalog
        .create_table(CreateTableRequest {
            name: "audit".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig::standard(),
        })
        .await
        .unwrap();

    // 文件真的放进存储（路径形状与 ingest 一致：`yuntun/public/{table}/…`）
    let path = "yuntun/public/audit/dt=w/shard=s0/rowgroups.parquet".to_string();
    let bytes = parquet_bytes();
    // 用 store 自己的便捷写入（生产路径也走它）—— 直接调 trait 的 `put` 会踩
    // `PutPayload` 那层转换，没必要在这里重复一遍。
    yuntun_store::put_bytes(&*store, &path, bytes.clone())
        .await
        .unwrap();
    catalog
        .commit_files(CommitFilesRequest {
            table: "audit".into(),
            batch_id: "b-rowgroup".into(),
            client_request_id: None,
            client_request_ids: vec![],
            shard: "s0".into(),
            time_window: "w".into(),
            files: vec![FileManifest {
                file_path: path,
                file_size: bytes.len() as u64,
                row_count: ROWS as u64,
                ..Default::default()
            }],
            schema_version: 1,
            row_count: ROWS as u64,
        })
        .await
        .unwrap();

    let cache = Arc::new(LocalCatalog::new());
    cache.refresh(&catalog).await.unwrap();
    let engine = QueryEngine::new(store.clone(), cache);

    // 先确认数据真的读得到（否则下面的"剪枝"可能是"压根没读"）
    let before = engine
        .sql("SELECT count(*) AS c FROM yuntun.public.audit")
        .await
        .unwrap();
    let total = arrow::util::display::array_value_to_string(before[0].column(0), 0).unwrap();
    assert_eq!(total, ROWS.to_string(), "先证明文件读得到、行数对");

    let out = engine
        .sql("EXPLAIN ANALYZE SELECT count(*) AS c FROM yuntun.public.audit WHERE event_time < 100")
        .await
        .expect("EXPLAIN ANALYZE 应当能跑");
    // ⚠️ `EXPLAIN` 的输出是**两列**（`plan_type` + `plan`）：只取第 0 列会拿到
    // "Plan with Metrics" 那一格、正文全丢（第一版就是这么踩的）⇒ 把所有列都拼进来。
    let plan: String = out
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).flat_map(move |r| {
                (0..b.num_columns()).map(move |c| {
                    arrow::util::display::array_value_to_string(b.column(c), r).unwrap_or_default()
                })
            })
        })
        .collect::<Vec<_>>()
        .join("\n");

    let n = parse_metric(&plan, "row_groups_pruned_statistics")
        .unwrap_or_else(|| panic!("计划里没找到 row_groups_pruned_statistics 指标：\n{plan}"));
    assert!(
        n > 0,
        "`WHERE event_time < 100` 对第二个 row group（100..=199）无解 ⇒ 应当剪掉它\n\
         实际剪掉 {n} 组\n完整计划：\n{plan}"
    );

    // 结果照旧正确（下推只省 IO，不接管过滤 —— 表级声明的是 `Inexact`）
    let rows = engine
        .sql("SELECT count(*) AS c FROM yuntun.public.audit WHERE event_time < 100")
        .await
        .unwrap();
    let got = arrow::util::display::array_value_to_string(rows[0].column(0), 0).unwrap();
    assert_eq!(got, "100", "过滤结果必须是 100（下推不改变语义）");
}
