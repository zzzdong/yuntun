//! **F.4 验收：行组级索引剪枝**（`plan.md` F.4 / `operation-log §145`）。
//!
//! 三条断言，按重要性排序：
//!
//! 1. **对拍（最关键）**：同一批查询，**开索引**与**关索引**的结果必须**逐格一致**——
//!    剪错 = 静默少数据，这是本仓最不能接受的失败形态（`plan.md` F.4 验收③）；
//! 2. **只有索引能剪掉的组，真的被跳过了**：构造一个"值落在该组 zone map **区间内**、
//!    但该组里根本没有"的谓词 —— DataFusion 的 footer 统计对此**无能为力**，
//!    只有我们的 XOR filter 能证明 ⇒ 用 `EXPLAIN ANALYZE` 的 `bytes_scanned`
//!    对照"开/关"两次真实读取（这是**IO 级**证据，不是"挂了个计划"）：
//!    `plan.md` F.4 验收②；
//! 3. **索引随写入生成并被目录登记**（验收①，见本文件末尾的真 ingest 用例）。
//!
//! # 为什么造 65537 行而不是 200 行
//!
//! 索引的"组"**必须与 parquet 的行组逐行对齐**（读侧要按行组序号跳过）⇒ 想让一个文件有
//! **两个**组，就只能真的写 65537 行。单列 `Int64`（不写字符串列）把代价压到毫秒级、
//! 且全部在内存存储上 —— 不碰盘、不碰 WAL（`§132` 的教训：别把造数据的代价转嫁给邻居）。

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_model::index::{IndexFile, INDEX_GROUP_ROWS};
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

/// 两个组：
///
/// * **组 0**（0..N）：全是**偶数**（`0, 2, 4, …, 2N-2`）⇒ zone map `[0, 2N-2]`；
/// * **组 1**（N..2N）：`1_000_000 + i`（与组 0 不相交）。
///
/// ⇒ 谓词 `event_time = 1` 落在组 0 的区间**内**（footer 统计剪不掉），但组 0 里**没有**奇数
/// —— 这正是"只有 XOR filter 能剪"的形态。
fn batch() -> (arrow::record_batch::RecordBatch, usize) {
    let n = INDEX_GROUP_ROWS;
    let mut ts: Vec<i64> = Vec::with_capacity(2 * n + 1);
    for i in 0..n {
        ts.push(2 * i as i64);
    }
    for i in 0..n {
        ts.push(1_000_000 + i as i64);
    }
    ts.push(5_000_000); // 第 3 组（只有 1 行）：验证"尾巴组"也算得对
    let rows = ts.len();
    (
        arrow::record_batch::RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from(ts))],
        )
        .unwrap(),
        rows,
    )
}

/// 手写 parquet（行组大小 = `INDEX_GROUP_ROWS`，与索引的"组"逐行对齐）+ 同批次的 `.idx`。
fn parquet_and_index() -> (Vec<u8>, Vec<u8>, usize) {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    let (b, rows) = batch();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(INDEX_GROUP_ROWS))
        .build();
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema(), Some(props)).unwrap();
    w.write(&b).unwrap();
    w.close().unwrap();
    let index = IndexFile::build(&b, &["event_time".to_string()]).unwrap();
    assert!(
        index.matches_shape(rows as u64),
        "索引形状必须与文件一致（组数 = ceil(行数/组大小)）"
    );
    (buf, index.to_bytes(), rows)
}

/// 建表 + 写入"一个真文件 + 真索引 + 真 manifest"，返回引擎。
async fn engine_with_data() -> (Arc<dyn object_store::ObjectStore>, QueryEngine, Arc<dyn CatalogOps>) {
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

    let (data, idx, rows) = parquet_and_index();
    let data_path = format!("yuntun/public/{TABLE}/dt=w/shard=s0/groups.parquet");
    let idx_path = yuntun_format::index_path(&data_path);
    yuntun_store::put_bytes(&*store, &data_path, data.clone())
        .await
        .unwrap();
    yuntun_store::put_bytes(&*store, &idx_path, idx.clone())
        .await
        .unwrap();

    catalog
        .commit_files(CommitFilesRequest {
            table: TABLE.into(),
            batch_id: "b-groups".into(),
            client_request_id: None,
            client_request_ids: vec![],
            shard: "s0".into(),
            time_window: "w".into(),
            files: vec![FileManifest {
                file_path: data_path,
                file_size: data.len() as u64,
                row_count: rows as u64,
                index_path: idx_path,
                index_size: idx.len() as u64,
                ..Default::default()
            }],
            schema_version: 1,
            row_count: rows as u64,
        })
        .await
        .unwrap();

    let cache = Arc::new(LocalCatalog::new());
    cache.refresh(&catalog).await.unwrap();
    let engine = QueryEngine::new(store.clone(), cache);
    (store, engine, catalog)
}

/// 结果 → 逐格的字符串（**顺序也参与比较**：查询都带 ORDER BY）
async fn rows_of(engine: &QueryEngine, sql: &str) -> Vec<String> {
    let batches = engine
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` 执行失败：{e}"));
    batches
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).flat_map(move |r| {
                (0..b.num_columns()).map(move |c| {
                    arrow::util::display::array_value_to_string(b.column(c), r).unwrap_or_default()
                })
            })
        })
        .collect()
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

/// `EXPLAIN ANALYZE` 的正文（所有列都拼进来 —— `§132` 踩过"只取第 0 列丢正文"）。
async fn explain(engine: &QueryEngine, sql: &str) -> String {
    let out = engine
        .sql(&format!("EXPLAIN ANALYZE {sql}"))
        .await
        .expect("EXPLAIN ANALYZE 应当能跑");
    out.iter()
        .flat_map(|b| {
            (0..b.num_rows()).flat_map(move |r| {
                (0..b.num_columns()).map(move |c| {
                    arrow::util::display::array_value_to_string(b.column(c), r).unwrap_or_default()
                })
            })
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// **验收②：只有索引能剪掉的组，真的被跳过了**（IO 级证据）。
///
/// 做法：同一个文件、同一条查询，跑两次 —— 一次开着索引剪枝、一次关掉，
/// 比较 `EXPLAIN ANALYZE` 里的 `bytes_scanned`。
///
/// * **关掉**时：DataFusion 只能靠 footer 统计 ⇒ 组 1、组 2 被剪，
///   但**组 0 剪不掉**（`event_time = 1` 落在它的 `[0, 2N-2]` 区间内）⇒ 必须读它的数据页；
/// * **开着**时：我们的 XOR filter 证明"1 不在组 0 里" ⇒ 连它一起跳过。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_pruning_skips_a_group_that_footer_stats_cannot() {
    let (_store, engine, _catalog) = engine_with_data().await;
    let sql = "SELECT count(*) AS c FROM yuntun.public.audit WHERE event_time = 1";

    // 先证明数据真读得到（否则"剪枝"可能是"压根没读"）
    let got = rows_of(&engine, sql).await;
    assert_eq!(got, vec!["0".to_string()], "事件时间 1 不存在 ⇒ 计数 0");

    let with = explain(&engine, sql).await;
    let without = explain(&engine.clone().with_index_pruning(false), sql).await;

    let on = parse_metric(&with, "bytes_scanned")
        .unwrap_or_else(|| panic!("计划里没有 bytes_scanned：\n{with}"));
    let off = parse_metric(&without, "bytes_scanned")
        .unwrap_or_else(|| panic!("计划里没有 bytes_scanned：\n{without}"));

    // 这条是**本用例的核心**：差异只能来自"我们跳过了 footer 统计剪不掉的那个组"
    //
    // 数字打出来（`--nocapture` 可见）：它同时是 `operation-log §145` 里的实测证据 ——
    // "剪枝真的省了 IO" 这件事，只有这种数字能证明，看代码看不出来。
    println!("[F.4] 同一条查询的 bytes_scanned：开索引 {on} / 关索引 {off}");
    assert!(
        on < off,
        "开索引后读的字节必须**更少**（说明真的跳过了组 0）：开={on} 关={off}\n\
         ---- 开 ----\n{with}\n---- 关 ----\n{without}"
    );
}

/// **验收③：开/关索引，结果逐格一致**（对拍；剪错的唯一防线）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_and_closed_pruning_agree_cell_by_cell() {
    let (_store, engine, _catalog) = engine_with_data().await;
    let closed = engine.clone().with_index_pruning(false);

    for sql in [
        // 等值：区间内的"洞"（只有 filter 能剪）
        "SELECT event_time FROM yuntun.public.audit WHERE event_time = 1 ORDER BY event_time",
        // 等值：真实存在的值（必须**不剪**、照常读出来）
        "SELECT event_time FROM yuntun.public.audit WHERE event_time = 4 ORDER BY event_time",
        // 区间：两级 zone map 都能剪（组 1/2 被剪，组 0 命中）
        "SELECT count(*) AS c FROM yuntun.public.audit WHERE event_time < 10",
        // 区间跨界（三组都可能命中）
        "SELECT count(*) AS c FROM yuntun.public.audit WHERE event_time >= 1000000",
        // `!=`：最容易被剪错的那条（只在区间退化成一点时才能剪）
        "SELECT count(*) AS c FROM yuntun.public.audit WHERE event_time != 0",
        // 尾巴组（只有 1 行）
        "SELECT event_time FROM yuntun.public.audit WHERE event_time = 5000000",
        // 没有谓词：一条都不该剪（`§129` 的纪律）
        "SELECT count(*) AS c FROM yuntun.public.audit",
        // 字符串谓词配整型列：约束必须被丢掉（类型族对不上）
        "SELECT count(*) AS c FROM yuntun.public.audit WHERE CAST(event_time AS VARCHAR) = '4'",
    ] {
        let a = rows_of(&engine, sql).await;
        let b = rows_of(&closed, sql).await;
        assert_eq!(a, b, "开/关索引剪枝的结果必须逐格一致：`{sql}`");
    }
}

/// **验收①（真写入路径）**：索引**随 flusd 生成**、被目录登记、能被解回来且形状对得上。
///
/// 前面几条用例是"手写文件 + 手写索引"（为了控制行组大小），这条走**真的 ingest 路径**
/// —— 否则"索引会不会被生成"根本没被测到（那是两条不同的接线）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn index_is_written_and_registered_by_flush() {
    use tokio_util::sync::CancellationToken;
    use yuntun_model::IngestBatch;

    let wal_dir = yuntun_testkit::TestDir::tmpfs("index-flush").into_path();
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

    let cfg = yuntun_ingest::IngestorConfig {
        rows_threshold: 1,
        time_threshold_secs: 0,
        max_flush_delay_secs: 0,
        flush_phase_spread_secs: 0,
        scan_interval: std::time::Duration::from_millis(20),
        spill_dir: wal_dir.join("spill"),
        ..Default::default()
    };
    let wal = yuntun_wal::writer::WalWriter::open(yuntun_wal::WalConfig::for_dir(&wal_dir), 0)
        .await
        .unwrap();
    let ingestor = Arc::new(yuntun_ingest::Ingestor::new(
        cfg,
        wal,
        catalog.clone(),
        store.clone(),
    ));
    let (b, _rows) = batch();
    ingestor
        .ingest(IngestBatch {
            table: TABLE.into(),
            shard_key: "s0".into(),
            record_batch: b.slice(0, 4),
            idempotency_key: Some("k-index-1".into()),
            received_at: std::time::SystemTime::now(),
        })
        .await
        .unwrap();

    let shutdown = CancellationToken::new();
    let acc = ingestor.clone().spawn_accumulator(shutdown.clone());
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    shutdown.cancel();
    let _ = acc.await;

    // 目录里那个文件必须带 index_path / index_size
    let files = catalog
        .list_visible_files(TABLE, catalog.current_snapshot().await, None)
        .await
        .unwrap();
    assert_eq!(files.len(), 1, "应当恰好落一个文件：{files:?}");
    let f = &files[0];
    assert!(
        !f.index_path.is_empty(),
        "有排序列（event_time）却没有索引路径 —— 索引没随写入生成"
    );
    assert_eq!(
        yuntun_format::index_path(&f.file_path),
        f.index_path,
        "索引与数据文件必须同 stem（GC 靠它把两者当同一批）"
    );

    // 对象真的在，而且解得回来、形状对得上
    let bytes = yuntun_store::get_bytes(&*store, &f.index_path).await.unwrap();
    assert_eq!(bytes.len() as u64, f.index_size, "登记的字节数与对象一致");
    let idx = IndexFile::from_bytes(&bytes).expect("索引必须能被解回");
    assert!(idx.matches_shape(f.row_count), "索引形状必须与 manifest 一致");
    assert_eq!(idx.groups.len(), 1, "4 行 ⇒ 一个组");
}
