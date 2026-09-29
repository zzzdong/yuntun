//! **定位扫描**：在一个数据文件上求谓词，取回"**命中哪些行**"（`plan.md` F.3 / `delta-dml-design §5.3`）。
//!
//! # 这是给谁用的
//!
//! `DELETE FROM t WHERE …` 要先把命中行**定位**成 `(文件, 行号)`，才谈得上写删除向量（DV）。
//! 定位必须落在读侧这一层，因为两件东西只有这里有：
//!
//! * **行序即文件序**：位置只有相对"文件原始行序"才有意义（DV 的口径就是这个）；
//! * **SQL 语义**：谓词要用**查询那条**语义去求（同一个 DataFusion），不能自己写一个"差不多的"
//!   求值器 —— 差一点就是"删多了/删少了"，而两种都是静默错。
//!
//! # 形态（与设计 §5.3 的偏差，写在明处）
//!
//! 设计写的 M1 PoC 是"逐文件 child `DataSourceExec` + `ProvenanceExec` 附加 `__file_id`/`__row_idx`"
//! （**保持在 DF 的计划里**）。这里落的是**更朴素的一步**：
//!
//! 1. 用 `yuntun-format::read_batch` 把**一个文件**读进内存（行序 = 文件行序）；
//! 2. 给每行补一列 `__yuntun_row_idx`（文件内行号，0 起）；
//! 3. 装成 `MemTable`，让 DataFusion 跑 `SELECT __yuntun_row_idx FROM … WHERE <谓词>`。
//!
//! 为什么可以这样：**M1 只服务 DML 定位**（不是查询路径），一次只处理一个文件 ⇒ 内存可控；
//! 而"逐文件 child + 计划内附加列"的流式版本是**收益**（大文件不全读）不是**正确性** ——
//! 那条路留到 `F.3d` 与 RowSelection 一起收（设计 §9 的 M2）。
//!
//! # 纪律
//!
//! * **读不了就报错**：文件读不出来 / 谓词求值失败 / 行数与清单不符 ⇒ 全部**报错**。
//!   跳过任何一个文件 = 那个文件里该删的行**没删**（用户以为删了）—— 比"查询失败"糟得多；
//! * **行号是文件内的**（不是全局的），且**升序去重**后返回。

use std::sync::Arc;

use arrow::array::{Array, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use object_store::ObjectStore;

/// 行号列的列名（刻意取得不像用户列）。
pub const ROW_IDX_COLUMN: &str = "__yuntun_row_idx";

/// 定位结果。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LocatedRows {
    /// 命中行的**文件内行号**（升序、去重）—— 直接就是 DV 位图的内容
    pub positions: Vec<u32>,
    /// 这个文件实际有多少行（与清单里的 `row_count` 对不上 ⇒ 调用方必须报错）
    pub rows: u64,
}

/// 定位 + **投影**的结果（`F.3e-2`）：命中行号 **+** 这些行的新值。
#[derive(Debug, Clone, Default)]
pub struct ProjectedRows {
    /// 命中行的**文件内行号**（升序、去重）—— 与 [`LocatedRows`] 同一个口径
    pub positions: Vec<u32>,
    /// 这个文件实际有多少行（与清单里的 `row_count` 对不上 ⇒ 调用方必须报错）
    pub rows: u64,
    /// 命中行的**新值**（列 = 调用方给的投影列表；`UPDATE` 用它写产物文件）。
    ///
    /// **契约**：`batch` 的第 `i` 行 ↔ `positions[i]`。这条对齐不是"碰巧"：
    /// 查询结果先按行号**排序**再去掉行号列（见 [`locate_and_project`]）——
    /// 少了这一步，DataFusion 的输出顺序就没有保证，而"把 A 行的新值写到 B 行的位置"
    /// 是本仓最不能接受的那类静默错。
    pub batch: Option<RecordBatch>,
}

/// 在 `path` 指的文件上求 `predicate_sql`，返回命中行号。
///
/// `schema` 是**表**的 schema：列名解析按它，且文件批会先**对齐**到它
/// （缺列补 NULL、多列丢掉、类型宽化 —— 与查询路径同一套语义，见 `read_with_row_idx`）；
/// 文件里的列顺序/多寡因此不影响谓词语义。
pub async fn locate_matching_rows(
    store: &Arc<dyn ObjectStore>,
    path: &str,
    fmt: yuntun_format::DataFormat,
    schema: &SchemaRef,
    predicate_sql: &str,
) -> Result<LocatedRows, DataFusionError> {
    let (with_idx, rows) = read_with_row_idx(store, path, fmt, schema).await?;
    if with_idx.is_empty() {
        return Ok(LocatedRows::default());
    }
    let out = run_locate_sql(
        path,
        schema,
        with_idx,
        &format!("\"{ROW_IDX_COLUMN}\""),
        predicate_sql,
    )
    .await?;
    Ok(LocatedRows {
        positions: positions_of(&out, path)?,
        rows,
    })
}

/// **定位 + 投影**（`F.3e-2`）：一次扫描同时拿到"命中哪些行"与"这些行的新值"。
///
/// `projection_sql` 是 SELECT 列表（例如 `"v" + 100 AS "v", "c" AS "c"`），
/// 由调用方按**表 schema** 生成（`UPDATE` 里 = SET 过的列用表达式、其余列原样）。
///
/// 为什么一次做完而不是两次（先定位再取值）：数据文件不可变，两次读**不会**错位，
/// 但一次读少扫一遍文件（大文件上就是少一次全列 IO），而且"删哪些行"与"这些行的新值"
/// 在代码上出自**同一个读取点** —— 这种"两半同源"的形状本身就是个护栏。
pub async fn locate_and_project(
    store: &Arc<dyn ObjectStore>,
    path: &str,
    fmt: yuntun_format::DataFormat,
    schema: &SchemaRef,
    predicate_sql: &str,
    projection_sql: &str,
) -> Result<ProjectedRows, DataFusionError> {
    let (with_idx, rows) = read_with_row_idx(store, path, fmt, schema).await?;
    if with_idx.is_empty() {
        return Ok(ProjectedRows {
            rows,
            ..Default::default()
        });
    }
    let out = run_locate_sql(
        path,
        schema,
        with_idx,
        &format!("\"{ROW_IDX_COLUMN}\", {projection_sql}"),
        predicate_sql,
    )
    .await?;
    // ⚠️ 一行都没命中时 DataFusion 会返回**零个批次**（不是"一个空批次"）——
    // 这里必须先挡住，否则下面的 `out[0]` 直接索引越界（`§154` 的用例撞到过：
    // `UPDATE … WHERE v = 999`）。同时它也是"没命中就什么都不做"的正常路径。
    if out.is_empty() {
        return Ok(ProjectedRows {
            rows,
            ..Default::default()
        });
    }
    // 把多批次并成一个，再**按行号排序**：`positions[i]` 与结果第 `i` 行从此一一对应
    // （DataFusion 不保证输出顺序，不显式排序就等于把"哪一行的新值"交给实现细节）
    let schema_with_idx = out[0].schema();
    let all = arrow::compute::concat_batches(&schema_with_idx, &out).map_err(|e| {
        DataFusionError::Execution(format!("投影结果合并失败（文件 {path}）：{e}"))
    })?;
    let idx = all
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "投影结果的行号列不是 UInt32（文件 {path}）：{:?}",
                all.column(0).data_type()
            ))
        })?
        .clone();
    let order = arrow::compute::sort_to_indices(&idx, None, None).map_err(|e| {
        DataFusionError::Execution(format!("投影结果排序失败（文件 {path}）：{e}"))
    })?;
    let sorted = arrow::compute::take_record_batch(&all, &order).map_err(|e| {
        DataFusionError::Execution(format!("投影结果重排失败（文件 {path}）：{e}"))
    })?;
    let positions: Vec<u32> = sorted
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap()
        .iter()
        .flatten()
        .collect();
    // 去掉行号列（列序与 schema 都保留调用方给的那套）
    let keep: Vec<usize> = (1..sorted.num_columns()).collect();
    let batch = sorted.project(&keep).map_err(|e| {
        DataFusionError::Execution(format!("投影结果取列失败（文件 {path}）：{e}"))
    })?;
    Ok(ProjectedRows {
        positions,
        rows,
        batch: Some(batch),
    })
}

/// 读文件 + **按表 schema 对齐** + 补行号列（文件内、0 起、跨批次连续 —— 这就是 DV 的口径）。
///
/// # 为什么要对齐（`§155`，台账 `D-13`）
///
/// schema 演进（`ALTER TABLE ADD COLUMN` / 类型宽化）之后，**老文件里没有新列**。
/// 读路径早就处理了这件事（`ParquetSource` 的 `TableSchemaBuilder`：缺列 → NULL），
/// 写入路径也是（`flush::align_batch`）；但**定位路径曾经漏了** ⇒
/// `DELETE FROM t WHERE 新列 IS NULL` 会以一个 schema 错误**整条失败**，
/// 而查询那边明明把这些行显示成 NULL。
///
/// 语义必须**和查询看到的一致** —— "谓词/表达式看到的就是用户看到的"，否则 DML 与 SELECT
/// 会对同一批行给出不同答案（那是本仓最不能接受的失败形态：静默不一致）。
/// 所以这里复用**同一个** `arrow_util::align_batch`：缺列补 NULL、多列丢掉、类型按提升格 cast。
///
/// 对齐放在补行号列**之前**：行号只与"第几个"有关，与列怎么对齐无关；
/// 而这样得到的批次 schema 恰好是表 schema ⇒ `UPDATE` 的产物文件天然与表同 schema。
async fn read_with_row_idx(
    store: &Arc<dyn ObjectStore>,
    path: &str,
    fmt: yuntun_format::DataFormat,
    schema: &SchemaRef,
) -> Result<(Vec<RecordBatch>, u64), DataFusionError> {
    let batches = yuntun_format::read_batch(store, path, fmt)
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "定位扫描读不了文件 {path}：{e} —— 拒绝「跳过这个文件」\
                （那样它里面该删的行会被漏掉）"
            ))
        })?;
    let mut rows: u64 = 0;
    let mut with_idx: Vec<RecordBatch> = Vec::with_capacity(batches.len());
    for b in &batches {
        let n = b.num_rows();
        let aligned = yuntun_model::arrow_util::align_batch(b, schema).map_err(|e| {
            DataFusionError::Execution(format!(
                "文件 {path} 与表 schema 对齐失败：{e} —— \
                 拒绝「按没对齐的列去定位」（谓词看到的列与用户看到的必须一致）"
            ))
        })?;
        let mut fields: Vec<Field> = aligned
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let mut cols: Vec<Arc<dyn Array>> = aligned.columns().to_vec();
        fields.push(Field::new(ROW_IDX_COLUMN, DataType::UInt32, false));
        cols.push(Arc::new(UInt32Array::from(
            (rows..rows + n as u64)
                .map(|i| i as u32)
                .collect::<Vec<u32>>(),
        )));
        with_idx.push(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).map_err(|e| {
                DataFusionError::Execution(format!("附加行号列失败（{path}）：{e}"))
            })?,
        );
        rows += n as u64;
    }
    Ok((with_idx, rows))
}

/// 内存表 + 一次 SQL（**用查询那条语义**求谓词与投影，不另写求值器）。
async fn run_locate_sql(
    path: &str,
    schema: &SchemaRef,
    with_idx: Vec<RecordBatch>,
    select_list: &str,
    predicate_sql: &str,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let table_schema = with_idx[0].schema();
    let ctx = SessionContext::new();
    let mem = datafusion::datasource::memory::MemTable::try_new(table_schema, vec![with_idx])
        .map_err(|e| DataFusionError::Execution(format!("定位扫描建内存表失败：{e}")))?;
    ctx.register_table("__yuntun_locate", Arc::new(mem))
        .map_err(|e| DataFusionError::Execution(format!("定位扫描注册内存表失败：{e}")))?;
    let sql = format!("SELECT {select_list} FROM __yuntun_locate WHERE {predicate_sql}");
    // 规划与执行**都要**带上"这是谓词的问题"的上下文：列名写错在规划期就报，
    // 类型/运行期错误在执行期报 —— 两处都不许让调用方看到一个裸的 DF 错误
    let nth = |e: DataFusionError| {
        DataFusionError::Execution(format!(
            "定位扫描求谓词失败（文件 {path}）：{e}；表 schema = {:?}",
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
        ))
    };
    let df = ctx.sql(&sql).await.map_err(nth)?;
    df.collect().await.map_err(|e| {
        // 谓词本身出错（列名写错 / 类型不匹配）**必须报错**：猜一个"空结果"就是"什么都没删"
        DataFusionError::Execution(format!(
            "定位扫描求谓词失败（文件 {path}）：{e}；表 schema = {:?}",
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
        ))
    })
}

/// 从结果集第 0 列取行号（升序去重）。
fn positions_of(out: &[RecordBatch], path: &str) -> Result<Vec<u32>, DataFusionError> {
    let mut positions: Vec<u32> = Vec::new();
    for b in out {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "定位扫描的结果列不是 UInt32（文件 {path}）：{:?}",
                    b.column(0).data_type()
                ))
            })?;
        positions.extend(col.iter().flatten());
    }
    positions.sort_unstable();
    positions.dedup();
    Ok(positions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]))
    }

    async fn write_file(values: Vec<i64>) -> (Arc<dyn ObjectStore>, String) {
        let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap();
        let batch =
            RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values))]).unwrap();
        let (path, _bytes, _rows) = yuntun_format::write_batch(
            &store,
            "public.t",
            "s0",
            "w",
            "b-locate",
            &batch,
            yuntun_format::DataFormat::Parquet,
        )
        .await
        .unwrap();
        (store, path)
    }

    /// 写一个**自定义 schema** 的文件（测 schema 演进：老文件缺列 / 多列 / 类型窄）。
    async fn write_file_with(
        schema: SchemaRef,
        cols: Vec<Arc<dyn Array>>,
    ) -> (Arc<dyn ObjectStore>, String) {
        let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap();
        let batch = RecordBatch::try_new(schema, cols).unwrap();
        let (path, _bytes, _rows) = yuntun_format::write_batch(
            &store,
            "public.t",
            "s0",
            "w",
            "b-locate",
            &batch,
            yuntun_format::DataFormat::Parquet,
        )
        .await
        .unwrap();
        (store, path)
    }

    /// **老文件缺列**（`ALTER TABLE ADD COLUMN` 之后）⇒ 缺的那列按 **NULL** 参与谓词与投影。
    ///
    /// 这是台账 `D-13` 的单测面：语义必须与查询一致（查询那边老文件的缺列也是 NULL）——
    /// 否则 `DELETE FROM t WHERE 新列 IS NULL` 会**整条失败**，而用户看到的那些行明明是 NULL。
    #[tokio::test]
    async fn a_file_missing_a_column_sees_it_as_null() {
        let (store, path) = write_file(vec![1, 2, 3]).await; // 只有 v
        let table = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, true),
            Field::new("note", DataType::Utf8, true),
        ])) as SchemaRef;

        // `IS NULL` ⇒ 老文件的行**全部**命中（以前这里是"列不存在"的报错）
        let got = locate_matching_rows(
            &store,
            &path,
            yuntun_format::DataFormat::Parquet,
            &table,
            "note IS NULL",
        )
        .await
        .unwrap();
        assert_eq!(got.positions, vec![0, 1, 2], "缺列 = NULL ⇒ IS NULL 全都命中");

        // `= 'x'` ⇒ 一行都不命中（NULL 的三值逻辑：比较结果是 NULL，不是真）
        let got = locate_matching_rows(
            &store,
            &path,
            yuntun_format::DataFormat::Parquet,
            &table,
            "note = 'x'",
        )
        .await
        .unwrap();
        assert!(got.positions.is_empty(), "NULL 不等于任何值");

        // 投影：老值原样、新列全 NULL，且**结果 schema 就是表 schema**（写出去的产物要能被读回来）
        let got = locate_and_project(
            &store,
            &path,
            yuntun_format::DataFormat::Parquet,
            &table,
            "note IS NULL",
            "\"v\", \"note\"",
        )
        .await
        .unwrap();
        let b = got.batch.expect("有命中就有批次");
        assert_eq!(b.schema().fields().len(), 2);
        assert_eq!(b.schema().field(1).name(), "note");
        assert_eq!(b.column(1).null_count(), 3, "缺列补的 NULL");
    }

    /// **老文件有多余的列**（`DROP COLUMN` 之后）⇒ 对齐时丢掉：谓词/投影只见表 schema 的列。
    #[tokio::test]
    async fn extra_columns_are_dropped_by_alignment() {
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, true),
            Field::new("gone", DataType::Int64, true),
        ])) as SchemaRef;
        let (store, path) = write_file_with(
            file_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![9, 9, 9])),
            ],
        )
        .await;
        let table = schema(); // 只有 v
        let got = locate_and_project(
            &store,
            &path,
            yuntun_format::DataFormat::Parquet,
            &table,
            "v >= 2",
            "\"v\"",
        )
        .await
        .unwrap();
        assert_eq!(got.positions, vec![1, 2]);
        let b = got.batch.unwrap();
        assert_eq!(
            b.schema().fields().len(),
            1,
            "对齐到表 schema：多余的列必须被丢掉（否则产物文件会带着表没有的列）"
        );
    }

    /// **老文件类型更窄**（`Int32` 文件 vs `Int64` 表）⇒ 按提升格 cast 后正常求值。
    #[tokio::test]
    async fn a_narrower_column_is_widened_before_evaluation() {
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Int32,
            true,
        )])) as SchemaRef;
        let (store, path) = write_file_with(
            file_schema,
            vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]))],
        )
        .await;
        let got = locate_and_project(
            &store,
            &path,
            yuntun_format::DataFormat::Parquet,
            &schema(), // Int64 表
            "v = 3",
            "\"v\" + 10 AS \"v\"",
        )
        .await
        .unwrap();
        assert_eq!(got.positions, vec![2], "宽化之后等值比较才有意义");
        let b = got.batch.unwrap();
        assert_eq!(b.column(0).data_type(), &DataType::Int64, "产物的类型按表 schema");
    }

    #[tokio::test]
    async fn locates_exactly_the_matching_rows() {
        let (store, path) = write_file(vec![10, 20, 30, 40, 50]).await;
        let got = locate_matching_rows(
            &store,
            &path,
            yuntun_format::DataFormat::Parquet,
            &schema(),
            "v > 25",
        )
        .await
        .unwrap();
        assert_eq!(got.rows, 5, "文件行数要报出来（与清单核对用）");
        assert_eq!(got.positions, vec![2, 3, 4], "命中的是第 2/3/4 行（文件内、0 起）");
    }

    /// 没命中 ⇒ 空位置（而不是报错）；这不是"跳过"，是"确实没有"。
    #[tokio::test]
    async fn no_match_yields_empty_positions() {
        let (store, path) = write_file(vec![1, 2, 3]).await;
        let got = locate_matching_rows(
            &store,
            &path,
            yuntun_format::DataFormat::Parquet,
            &schema(),
            "v = 999",
        )
        .await
        .unwrap();
        assert!(got.positions.is_empty());
        assert_eq!(got.rows, 3);
    }

    /// **定位 + 投影**：命中行号与这些行的新值必须来自同一次扫描，且逐行对得上。
    #[tokio::test]
    async fn locate_and_project_returns_positions_and_new_values() {
        let (store, path) = write_file(vec![10, 20, 30, 40, 50]).await;
        let got = locate_and_project(
            &store,
            &path,
            yuntun_format::DataFormat::Parquet,
            &schema(),
            "v >= 30",
            "\"v\" + 1000 AS \"v\"",
        )
        .await
        .unwrap();
        assert_eq!(got.rows, 5);
        assert_eq!(
            got.positions,
            vec![2, 3, 4],
            "命中的是第 2/3/4 行（与 locate 同一口径）"
        );
        let b = got.batch.expect("有命中就必然有结果批次");
        assert_eq!(b.schema().fields().len(), 1, "投影只留新值列");
        assert_eq!(
            b.num_rows(),
            got.positions.len(),
            "**契约**：结果行数必须等于命中行数（否则对齐无从谈起）"
        );
        let c = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let vals: Vec<i64> = (0..c.len()).map(|i| c.value(i)).collect();
        assert_eq!(vals, vec![1030, 1040, 1050], "新值 = 原值 + 1000（SET 的效果）");
    }

    /// **一行都没命中** ⇒ 空结果（而不是 panic）。
    ///
    /// 这条盯的是一个真实撞到过的形态：DataFusion 在"零行"时返回**零个批次**，
    /// 而"取第一个批次拿 schema"的写法会直接索引越界（`§154` 修）。
    #[tokio::test]
    async fn no_match_yields_no_batch_instead_of_panicking() {
        let (store, path) = write_file(vec![1, 2, 3]).await;
        let got = locate_and_project(
            &store,
            &path,
            yuntun_format::DataFormat::Parquet,
            &schema(),
            "v > 100",
            "\"v\" + 1 AS \"v\"",
        )
        .await
        .unwrap();
        assert!(got.positions.is_empty());
        assert!(got.batch.is_none(), "没命中就没有结果批次（调用方据此什么都不做）");
        assert_eq!(got.rows, 3, "文件行数照样要报出来");
    }

    /// 投影写错（列不存在）⇒ **报错**（与谓词同一条纪律：不许猜一个"空结果"）。
    #[tokio::test]
    async fn a_broken_projection_is_an_error() {
        let (store, path) = write_file(vec![1, 2, 3]).await;
        let e = locate_and_project(
            &store,
            &path,
            yuntun_format::DataFormat::Parquet,
            &schema(),
            "v > 0",
            "no_such_column AS \"v\"",
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(
            e.contains("谓词"),
            "投影出错也要点名（同一个 SQL 里出的问题）：{e}"
        );
    }

    /// 谓词写错（列不存在）⇒ **报错**，不许悄悄返回"空结果"（那等于"什么都没删"还报成功）。
    #[tokio::test]
    async fn a_broken_predicate_is_an_error_not_an_empty_result() {
        let (store, path) = write_file(vec![1, 2, 3]).await;
        let e = locate_matching_rows(
            &store,
            &path,
            yuntun_format::DataFormat::Parquet,
            &schema(),
            "no_such_column = 1",
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(e.contains("谓词"), "必须点名是谓词的问题：{e}");
    }

    /// 文件读不出来（路径不存在）⇒ **报错**（跳过 = 漏删）。
    #[tokio::test]
    async fn a_missing_file_is_an_error() {
        let (store, _path) = write_file(vec![1, 2, 3]).await;
        let e = locate_matching_rows(
            &store,
            "yuntun/public/t/dt=w/shard=s0/nope.parquet",
            yuntun_format::DataFormat::Parquet,
            &schema(),
            "v = 1",
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(e.contains("读不了文件") && e.contains("漏掉"), "要说清后果：{e}");
    }
}
