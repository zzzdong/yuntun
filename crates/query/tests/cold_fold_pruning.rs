//! `§183`：**远端冷折叠也要剪枝** —— 被剪掉的文件**根本不该被打开**。
//!
//! # 为什么需要这一刀
//!
//! `§182` 把一部分冷读分给了别的节点，而那些节点当时是**整文件读**：既不按
//! `FileManifest.stats` 做**文件级**剪枝，也不把条件交给 `ParquetSource` 做**行组级**跳读。
//! 那不是"少了个优化"，而是**分配引入的回退** —— 同一条查询，分出去的那半边比留在本地的
//! 那半边多读了数据。
//!
//! # 判据的设计：让"被剪掉的文件不存在"
//!
//! 不包装 store 数 `get` 调用，而是**干脆不写那个文件**：
//! - 剪了 ⇒ 它永远不会被打开 ⇒ 折叠成功；
//! - 没剪 ⇒ 一定去读它 ⇒ `object not found` ⇒ 用例失败。
//!
//! 再加一条**反证**：同一个调用去掉谓词 ⇒ **必须失败**（它真的会去读那个文件）——
//! 否则"成功"可能只是碰巧没碰到它。

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use yuntun_format::DataFormat;
use yuntun_model::agg::{AggKind, AggQuery, AggSpec, AggStates, PartialAgg};
use yuntun_model::meta::{FileManifest, StatisticsLite, compute_stats_lite};
use yuntun_model::predicate::{CmpOp, Literal, Predicate};
use yuntun_query::cold;
use yuntun_store::{StoreConfig, create_store};

const TABLE: &str = "public.prune";
const SHARD: &str = "s0";
const WINDOW: &str = "w";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]))
}

fn batch(vals: &[Option<i64>]) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
}

fn stats_of(vals: &[Option<i64>]) -> StatisticsLite {
    compute_stats_lite(&batch(vals), &["v".to_string()]).unwrap()
}

/// 写一个真 parquet，返回 `(路径, 大小)`。**注意**：调用方可以选择**不写**（见下面的用例）。
async fn write(
    store: &Arc<dyn object_store::ObjectStore>,
    id: &str,
    vals: &[Option<i64>],
) -> (String, u64) {
    let (path, size, _rows) = yuntun_format::write_batch(
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
    (path, size)
}

fn manifest(path: &str, size: u64, stats: StatisticsLite) -> FileManifest {
    FileManifest {
        file_path: path.to_string(),
        batch_id: "b".into(),
        file_size: size,
        row_count: 2,
        table: TABLE.into(),
        shard: SHARD.into(),
        time_window: WINDOW.into(),
        stats: Some(stats),
        ..Default::default()
    }
}

/// 折叠 `files`，返回 `(count(*), sum(v))`。
async fn fold(
    store: Arc<dyn object_store::ObjectStore>,
    files: Vec<FileManifest>,
    predicate: Option<Predicate>,
) -> Result<(u64, Option<i64>), yuntun_model::error::LakeError> {
    let query = AggQuery {
        specs: vec![AggSpec::count_star(), AggSpec::new(AggKind::SumInt, "v")],
        group_by: vec![],
        predicate,
    };
    let states = cold::fold_cold_files(
        store,
        TABLE,
        schema(),
        files,
        Vec::new(), // 没有 DV
        &query,
    )
    .await?;
    let AggStates::Plain(p) = states else {
        panic!("不分组 ⇒ 应当是 Plain");
    };
    let PartialAgg::Count(c) = p[0] else {
        panic!("第一个是 count(*)");
    };
    let PartialAgg::SumInt(s) = p[1] else {
        panic!("第二个是 sum");
    };
    Ok((c, s))
}

/// **剪枝生效**：被排除的文件**根本不存在**，折叠仍然成功。
#[tokio::test]
async fn a_file_excluded_by_the_filter_is_never_opened() {
    let store = create_store(&StoreConfig::Memory).unwrap();

    // A：值域 [1, 2] —— **故意不写**（它一旦被打开就会报 object not found）
    // 只算路径（**不写**：它一旦被打开就会 object not found）
    let path_a = yuntun_format::file_path(TABLE, SHARD, WINDOW, "b-a", DataFormat::Parquet);
    let a = manifest(&path_a, 1, stats_of(&[Some(1), Some(2)]));
    // B：值域 [100, 200] —— 真的写了
    let (path_b, size_b) = write(&store, "b-b", &[Some(100), Some(200)]).await;
    let b = manifest(&path_b, size_b, stats_of(&[Some(100), Some(200)]));

    let p = Predicate::Cmp {
        column: "v".into(),
        op: CmpOp::Gt,
        value: Literal::Int(50),
    };
    let got = fold(store.clone(), vec![a, b], Some(p))
        .await
        .expect("A 被剪掉了 ⇒ 不该去打开它（打开就是 object not found）");
    assert_eq!(got, (2, Some(300)), "只剩 B 的两行");
}

/// **反证**：同一个调用**去掉谓词** ⇒ 一定去读那个不存在的文件 ⇒ 必须失败。
///
/// 没有这条，"上面那条成功"可能只是碰巧没碰到 A。
#[tokio::test]
async fn without_the_filter_the_missing_file_is_actually_read_and_fails() {
    let store = create_store(&StoreConfig::Memory).unwrap();

    // 只算路径（**不写**：它一旦被打开就会 object not found）
    let path_a = yuntun_format::file_path(TABLE, SHARD, WINDOW, "b-a", DataFormat::Parquet);
    let a = manifest(&path_a, 1, stats_of(&[Some(1), Some(2)]));
    let (path_b, size_b) = write(&store, "b-b", &[Some(100), Some(200)]).await;
    let b = manifest(&path_b, size_b, stats_of(&[Some(100), Some(200)]));

    let e = fold(store.clone(), vec![a, b], None)
        .await
        .expect_err("没有谓词 ⇒ 不会剪 ⇒ 一定会去读那个不存在的文件");
    // 关键不是错误的措辞，而是**它点到了 A**：那说明没有谓词时 A 真的会被打开
    assert!(
        e.to_string().contains("b-a"),
        "失败原因必须点到那个不存在的文件（否则反证不成立）：{e}"
    );
}

/// 剪枝**不许剪错**：谓词能放行 A 时，A 必须被算进来（这里 A 是存在的，值域 [1,2]）。
#[tokio::test]
async fn a_file_allowed_by_the_filter_is_still_counted() {
    let store = create_store(&StoreConfig::Memory).unwrap();
    let (path_a, size_a) = write(&store, "b-a", &[Some(1), Some(2)]).await;
    let a = manifest(&path_a, size_a, stats_of(&[Some(1), Some(2)]));
    let (path_b, size_b) = write(&store, "b-b", &[Some(100), Some(200)]).await;
    let b = manifest(&path_b, size_b, stats_of(&[Some(100), Some(200)]));

    let p = Predicate::Cmp {
        column: "v".into(),
        op: CmpOp::Gt,
        value: Literal::Int(0),
    };
    let got = fold(store.clone(), vec![a, b], Some(p))
        .await
        .expect("两边都放行");
    assert_eq!(got, (4, Some(303)), "1+2+100+200");
}
