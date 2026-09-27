//! **F.3b 验收：删除向量在读侧生效**（`plan.md` F.3 验收①②④的读侧部分 / `operation-log §147`）。
//!
//! 这一刀把 `§146` 立起来的 DV（表达层 + 目录能力）接到**读路径**上：带 DV 的文件在
//! `scan` 时拿到一个**整体行选择**（"保留哪些行"），由 parquet 读取器拆到行组粒度。
//!
//! 六条断言，按重要性排序：
//!
//! 1. **删后查不到、且没被删的行一行不少**（验收①）—— 少了是错、多了也是错（后者更难发现）；
//! 2. **同一文件多份 DV 叠加**（删除是追加式的设计取舍 `§2`）；
//! 3. **只影响被锚定的文件**（混有无 DV 文件，`delta-dml-design §8` 的 M1 验收矩阵）；
//! 4. **快照隔离**：**没刷新**的旧读者仍看到完整的表（验收②）—— 快照号是这条的分界线；
//! 5. **整文件删光**（保留集为空）⇒ 该文件贡献 0 行，不是"报错"也不是"全留"；
//! 6. **坏 DV / 基数不符 / 行号越界 ⇒ 查询失败**（`§146.1` 决定③）：
//!    DV 与索引**刻意相反** —— 索引坏了退回全读（只影响读多少），DV 坏了必须报错，
//!    因为"当没有删除"= **复活已删的行**。
//!
//! 另外顺带证一件事：带 DV 的文件**同时**有索引（`.idx`）时不会撞车
//! （DF 只接受一个 access extension：`ParquetAccessPlan` 与 `ParquetRowSelection` 互斥）。

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_model::dv::{DeletionEntry, DvBitmap, dv_object_path};
use yuntun_model::meta::FileManifest;
use yuntun_model::ops::{CommitFilesRequest, CreateTableRequest};
use yuntun_query::{LocalCatalog, QueryEngine};

const TABLE: &str = "audit";
/// 文件 A：偶数 0..20 里的 10 行（值 0,2,4,…,18）
const A_ROWS: i64 = 10;
/// 文件 B：值 100..110
const B_ROWS: i64 = 10;

fn schema() -> arrow::datatypes::SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "event_time",
        DataType::Int64,
        false,
    )]))
}

fn batch(offset: i64, n: i64, step: i64) -> arrow::record_batch::RecordBatch {
    let ts: Vec<i64> = (0..n).map(|i| offset + i * step).collect();
    arrow::record_batch::RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(ts))]).unwrap()
}

/// 手写一个小 parquet（行序 == 我们写入的顺序 ⇒ 行号可预测）。
fn parquet_of(offset: i64, n: i64, step: i64) -> (Vec<u8>, usize) {
    use parquet::arrow::ArrowWriter;
    let b = batch(offset, n, step);
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema(), None).unwrap();
    w.write(&b).unwrap();
    w.close().unwrap();
    (buf, n as usize)
}

struct Fixture {
    store: Arc<dyn object_store::ObjectStore>,
    catalog: Arc<dyn CatalogOps>,
    cache: Arc<LocalCatalog>,
    engine: QueryEngine,
    a_path: String,
    b_path: String,
}

/// 建表 + 两个真文件（A/B）+ 一个真索引挂在 A 上 + 真 manifest，返回装配好的引擎。
///
/// 为什么给 A 也挂索引：要证"带 DV 的文件不会被索引计划撞车"（两个扩展互斥）。
async fn fixture() -> Fixture {
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

    let (a_bytes, a_rows) = parquet_of(0, A_ROWS, 2);
    let (b_bytes, b_rows) = parquet_of(100, B_ROWS, 1);
    let a_path = format!("yuntun/public/{TABLE}/dt=w/shard=s0/a.parquet");
    let b_path = format!("yuntun/public/{TABLE}/dt=w/shard=s0/b.parquet");
    yuntun_store::put_bytes(&*store, &a_path, a_bytes.clone())
        .await
        .unwrap();
    yuntun_store::put_bytes(&*store, &b_path, b_bytes.clone())
        .await
        .unwrap();

    // A 的索引（F.4）：证明"带 DV 的文件不再取索引"这条互斥纪律
    let idx = yuntun_model::index::IndexFile::build(
        &batch(0, A_ROWS, 2),
        &["event_time".to_string()],
    )
    .unwrap();
    let idx_path = yuntun_format::index_path(&a_path);
    yuntun_store::put_bytes(&*store, &idx_path, idx.to_bytes())
        .await
        .unwrap();

    // ⚠️ **一个批次一个文件**（`files` 以 batch_id 为键；一次传两个会被状态机拒绝，
    //    见 `§147` 顺带补的护栏）。真实世界里这也是常态：一次 flush 一个文件。
    for (batch, path, bytes, rows, idx) in [
        ("b-a", &a_path, &a_bytes, a_rows as u64, Some((idx_path.clone(), idx.to_bytes()))),
        ("b-b", &b_path, &b_bytes, b_rows as u64, None),
    ] {
        let (index_path, index_bytes) = match idx {
            Some((p, b)) => (p, b),
            None => (String::new(), Vec::new()),
        };
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
                    index_path,
                    index_size: index_bytes.len() as u64,
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
    Fixture {
        store,
        catalog,
        cache,
        engine,
        a_path,
        b_path,
    }
}

impl Fixture {
    /// 登记一批删除：**先落位图对象、再进目录**（与设计 §4.1 的执行顺序一致：
    /// 对象先于 WAL/目录，"DV 已写、WAL 未提交"只会留下孤儿对象，不会留下悬空登记）。
    async fn delete(&self, dv_id: &str, file: &str, positions: &[u32]) {
        let dv = DvBitmap::from_positions(positions.iter().copied());
        let entry = DeletionEntry {
            dv_id: dv_id.into(),
            table: format!("public.{TABLE}"),
            file_path: file.into(),
            // 对账键：`dv → file_path → batch_id`（设计 §6.2）
            batch_id: if file == self.a_path { "b-a" } else { "b-b" }.into(),
            applied_at: 0, // 由目录分配（单快照原子）
            revoked_at: 0,
            card: dv.card() as u32,
            store_path: dv_object_path(file, dv_id),
        };
        yuntun_store::put_bytes(&*self.store, &entry.store_path, dv.to_bytes())
            .await
            .unwrap();
        self.catalog.apply_deletions(vec![entry]).await.unwrap();
    }

    /// 查询 → 升序的 `event_time` 列表（`num_rows()` 也一并返回，供"行数对得上"用）。
    async fn query(&self, sql: &str) -> Result<Vec<i64>, String> {
        let batches = self
            .engine
            .sql(sql)
            .await
            .map_err(|e| e.to_string())?;
        let mut out: Vec<i64> = Vec::new();
        for b in &batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "列不是 Int64".to_string())?;
            out.extend((0..col.len()).map(|i| col.value(i)));
        }
        Ok(out)
    }
}

/// A 的 10 行 + B 的 10 行，升序。
fn all_rows() -> Vec<i64> {
    let mut v: Vec<i64> = (0..A_ROWS).map(|i| i * 2).collect();
    v.extend((0..B_ROWS).map(|i| 100 + i));
    v
}

#[tokio::test]
async fn deleted_rows_disappear_and_the_rest_stay() {
    let f = fixture().await;
    assert_eq!(
        f.query(&format!("SELECT event_time FROM {TABLE}")).await.unwrap().len(),
        20,
        "前提：没有删除时两个文件都读得到"
    );

    // A 里删掉第 0、2、4 行（值 0、4、8）
    f.delete("dv-a", &f.a_path, &[0, 2, 4]).await;
    f.cache.refresh(&f.catalog).await.unwrap();

    let got = f.query(&format!("SELECT event_time FROM {TABLE}")).await.unwrap();
    assert_eq!(got.len(), 17, "删 3 行 ⇒ 少 3 行（少了是错，多了也是错）");
    assert!(!got.contains(&0) && !got.contains(&4) && !got.contains(&8), "被删的行必须查不到");
    // 没被删的一行不少 —— 这一条才是"剪错"那类失败的拦网
    let mut expect = all_rows();
    expect.retain(|v| ![0, 4, 8].contains(v));
    assert_eq!(got, expect, "除被删的 3 行外，其余行**逐行**一致");

    // 谓词查询也不受影响（DV 在文件级剪枝之后、扫描之前生效）
    let got = f
        .query(&format!("SELECT event_time FROM {TABLE} WHERE event_time < 10"))
        .await
        .unwrap();
    assert_eq!(got, vec![2, 6], "A 里 < 10 的 0/4/8 已删；B 全是 100+");
}

/// 删除是**追加式**的（设计 §2 的取舍）：同文件多份 DV 读侧必须叠加，而不是"最后一份生效"。
#[tokio::test]
async fn multiple_deletions_on_one_file_stack() {
    let f = fixture().await;
    f.delete("dv-a1", &f.a_path, &[0]).await;
    f.cache.refresh(&f.catalog).await.unwrap();
    assert!(!f.query(&format!("SELECT event_time FROM {TABLE}")).await.unwrap().contains(&0));

    f.delete("dv-a2", &f.a_path, &[2, 3]).await;
    f.cache.refresh(&f.catalog).await.unwrap();
    let got = f.query(&format!("SELECT event_time FROM {TABLE}")).await.unwrap();
    let mut expect = all_rows();
    expect.retain(|v| ![0, 4, 6].contains(v));
    assert_eq!(got, expect, "先后两次删除必须**叠加**（第二份不许把第一份盖掉）");
}

/// 只影响被锚定的文件：B 没有 DV ⇒ 一行不少（混有无 DV 文件的正确性）。
#[tokio::test]
async fn only_the_anchored_file_is_affected() {
    let f = fixture().await;
    f.delete("dv-a", &f.a_path, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).await; // A 全删
    f.cache.refresh(&f.catalog).await.unwrap();
    let got = f.query(&format!("SELECT event_time FROM {TABLE}")).await.unwrap();
    assert_eq!(
        got,
        (0..B_ROWS).map(|i| 100 + i).collect::<Vec<_>>(),
        "A 删光 ⇒ 只剩 B 的 10 行（B 不能被牵连）"
    );

    // 再把 B 也删光：两个文件都空 ⇒ 0 行（保留集为空的边界）
    f.delete("dv-b", &f.b_path, &(0..B_ROWS as u32).collect::<Vec<_>>())
        .await;
    f.cache.refresh(&f.catalog).await.unwrap();
    assert!(
        f.query(&format!("SELECT event_time FROM {TABLE}")).await.unwrap().is_empty(),
        "整文件删光 ⇒ 该文件贡献 0 行（既不是报错，也不是全留）"
    );
}

/// **快照隔离**：删除提交**之前**的读者（没刷新过的缓存）仍看到完整的表。
#[tokio::test]
async fn stale_reader_keeps_the_old_view() {
    let f = fixture().await;
    // 这一份快照是"删除之前的世界"
    let stale = f.cache.snapshot();
    assert!(f.cache.snapshot().get(&format!("public.{TABLE}")).unwrap().deletions.is_empty());

    f.delete("dv-a", &f.a_path, &[0, 1]).await;
    // ⚠️ **故意不刷新**：正是"删除之后的查询"与"删除之前就规划好的查询"的分界
    let old = f.query(&format!("SELECT event_time FROM {TABLE}")).await.unwrap();
    assert_eq!(old, all_rows(), "删除提交前的读者必须仍看到被删的行（快照隔离）");

    // 刷新之后（新快照）才看得到删除
    f.cache.refresh(&f.catalog).await.unwrap();
    let mut expect = all_rows();
    expect.retain(|v| ![0, 2].contains(v));
    assert_eq!(
        f.query(&format!("SELECT event_time FROM {TABLE}")).await.unwrap(),
        expect
    );
    assert!(
        stale.snapshot < f.cache.snapshot().snapshot,
        "快照号必须前进（DV 的 applied_at 就是这条分界线）"
    );
}

/// **坏 DV / 基数不符 / 行号越界 ⇒ 查询失败**（不许"当没有删除"）。
#[tokio::test]
async fn broken_deletion_vector_fails_the_query_loudly() {
    // ① 位图字节损坏（CRC 对不上）
    let f = fixture().await;
    f.delete("dv-a", &f.a_path, &[0]).await;
    let dp = dv_object_path(&f.a_path, "dv-a");
    yuntun_store::put_bytes(&*f.store, &dp, b"YTDV\x01\x02\x00\x00\x00\xff\xff\xff\xffXX".to_vec())
        .await
        .unwrap();
    f.cache.refresh(&f.catalog).await.unwrap();
    let e = f
        .query(&format!("SELECT event_time FROM {TABLE}"))
        .await
        .unwrap_err();
    assert!(
        e.contains("删除向量") && e.contains("损坏"),
        "坏 DV 必须让查询失败并点名：{e}"
    );

    // ② 目录记的基数与位图不符（对象与登记不是同一次写入）
    let f = fixture().await;
    f.delete("dv-a", &f.a_path, &[0]).await;
    f.cache.refresh(&f.catalog).await.unwrap();
    // 换掉对象：位图只剩 1 行、目录记着 5 行
    let dp = dv_object_path(&f.a_path, "dv-a");
    yuntun_store::put_bytes(
        &*f.store,
        &dp,
        DvBitmap::from_positions([0, 1, 2, 3, 4]).to_bytes(),
    )
    .await
    .unwrap();
    let e = f
        .query(&format!("SELECT event_time FROM {TABLE}"))
        .await
        .unwrap_err();
    assert!(
        e.contains("基数与目录不符"),
        "基数不符是「对象与登记不是同一次写入」的证据，必须报错：{e}"
    );

    // ③ 行号越界（DV 张冠李戴 / 文件被换过）
    let f = fixture().await;
    f.delete("dv-b", &f.b_path, &[0]).await;
    let dp = dv_object_path(&f.b_path, "dv-b");
    yuntun_store::put_bytes(&*f.store, &dp, DvBitmap::from_positions([999]).to_bytes())
        .await
        .unwrap();
    f.cache.refresh(&f.catalog).await.unwrap();
    let e = f
        .query(&format!("SELECT event_time FROM {TABLE}"))
        .await
        .unwrap_err();
    assert!(
        e.contains("对不上") && e.contains("放行"),
        "越界必须报错并说清后果（继续就会放行已删的行）：{e}"
    );
}

/// 带 DV 的文件**同时**有索引时不许撞车：DF 只接受一个 access extension
/// （`ParquetAccessPlan` 与 `ParquetRowSelection` 互斥），而我们的纪律是 **DV 优先**。
///
/// 这条用例同时是那个纪律的"证据"：若两个扩展都挂上，DF 会直接报
/// `Invalid parquet access extensions … not both` —— 查询跑通 + 行集正确即为反证。
#[tokio::test]
async fn deletion_vector_wins_over_index_on_the_same_file() {
    let f = fixture().await;
    // A 上真的挂了索引（见夹具）；这里再加 DV 并带**谓词**查询（谓词是索引计划的触发条件）
    f.delete("dv-a", &f.a_path, &[3]).await; // 值 6
    f.cache.refresh(&f.catalog).await.unwrap();
    let got = f
        .query(&format!(
            "SELECT event_time FROM {TABLE} WHERE event_time < 20 ORDER BY event_time"
        ))
        .await
        .unwrap();
    assert_eq!(
        got,
        vec![0, 2, 4, 8, 10, 12, 14, 16, 18],
        "有谓词（会触发索引计划）+ 有 DV 的文件：结果必须对，且不许两个扩展撞车"
    );
}

/// **登记的行数与文件不符** ⇒ 查询**报错**（而不是"自己挑一个数"）。
///
/// 这条是"整体行选择"的固有代价（`delta-dml-design §5.1`）：DF 要求选择器**恰好覆盖文件全部行**
/// （`try_new_from_overall_row_selection` 的硬校验），所以我们喂进去的 `row_count`
/// 必须与文件真实行数一致 —— 不一致时**宁可报错**，因为"猜一个数"就是选错行
/// （选少 = 少数据、选多 = 越界读），两种都比"查询失败"糟。
#[tokio::test]
async fn manifest_row_count_mismatch_fails_the_query() {
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
    let (bytes, rows) = parquet_of(0, A_ROWS, 2);
    let path = format!("yuntun/public/{TABLE}/dt=w/shard=s0/a.parquet");
    yuntun_store::put_bytes(&*store, &path, bytes.clone())
        .await
        .unwrap();
    // 清单里故意写**错的**行数（999 而不是 10）
    catalog
        .commit_files(CommitFilesRequest {
            table: TABLE.into(),
            batch_id: "b-a".into(),
            client_request_id: None,
            client_request_ids: vec![],
            shard: "s0".into(),
            time_window: "w".into(),
            files: vec![FileManifest {
                file_path: path.clone(),
                file_size: bytes.len() as u64,
                row_count: 999,
                ..Default::default()
            }],
            schema_version: 1,
            row_count: rows as u64,
        })
        .await
        .unwrap();
    let dv = DvBitmap::from_positions([0]);
    let entry = DeletionEntry {
        dv_id: "dv-a".into(),
        table: format!("public.{TABLE}"),
        file_path: path.clone(),
        batch_id: "b-a".into(),
        applied_at: 0,
        revoked_at: 0,
        card: 1,
        store_path: dv_object_path(&path, "dv-a"),
    };
    yuntun_store::put_bytes(&*store, &entry.store_path, dv.to_bytes())
        .await
        .unwrap();
    catalog.apply_deletions(vec![entry]).await.unwrap();

    let cache = Arc::new(LocalCatalog::new());
    cache.refresh(&catalog).await.unwrap();
    let engine = QueryEngine::new(store.clone(), cache);
    let e = engine
        .sql(&format!("SELECT event_time FROM {TABLE}"))
        .await
        .map(|_| String::new())
        .unwrap_or_else(|e| e.to_string());
    assert!(
        e.contains("RowSelection") || e.contains("rows"),
        "行数不符必须让查询失败并点名（不许猜一个数去选行）：{e}"
    );
}
