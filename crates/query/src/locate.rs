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

/// 在 `path` 指的文件上求 `predicate_sql`，返回命中行号。
///
/// `schema` 是**表**的 schema（列名解析按它；文件里的列顺序/多寡不影响谓词语义）。
pub async fn locate_matching_rows(
    store: &Arc<dyn ObjectStore>,
    path: &str,
    fmt: yuntun_format::DataFormat,
    schema: &SchemaRef,
    predicate_sql: &str,
) -> Result<LocatedRows, DataFusionError> {
    let batches = yuntun_format::read_batch(store, path, fmt)
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "定位扫描读不了文件 {path}：{e} —— 拒绝「跳过这个文件」\
                 （那样它里面该删的行会被漏掉）"
            ))
        })?;

    // ① 补行号列（文件内、0 起、跨批次连续 —— 这就是 DV 的口径）
    let mut rows: u64 = 0;
    let mut with_idx: Vec<RecordBatch> = Vec::with_capacity(batches.len());
    for b in &batches {
        let n = b.num_rows();
        let mut fields: Vec<Field> = b.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut cols: Vec<Arc<dyn Array>> = b.columns().to_vec();
        fields.push(Field::new(ROW_IDX_COLUMN, DataType::UInt32, false));
        cols.push(Arc::new(UInt32Array::from(
            (rows..rows + n as u64).map(|i| i as u32).collect::<Vec<u32>>(),
        )));
        with_idx.push(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).map_err(|e| {
                DataFusionError::Execution(format!("附加行号列失败（{path}）：{e}"))
            })?,
        );
        rows += n as u64;
    }
    if with_idx.is_empty() {
        return Ok(LocatedRows::default());
    }

    // ② 交给 DataFusion 求谓词（**用查询那条语义**，不另写求值器）
    let table_schema = with_idx[0].schema();
    let ctx = SessionContext::new();
    let mem = datafusion::datasource::memory::MemTable::try_new(table_schema, vec![with_idx])
        .map_err(|e| DataFusionError::Execution(format!("定位扫描建内存表失败：{e}")))?;
    ctx.register_table("__yuntun_locate", Arc::new(mem))
        .map_err(|e| DataFusionError::Execution(format!("定位扫描注册内存表失败：{e}")))?;
    let sql = format!("SELECT \"{ROW_IDX_COLUMN}\" FROM __yuntun_locate WHERE {predicate_sql}");
    // 规划与执行**都要**带上"这是谓词的问题"的上下文：列名写错在规划期就报，
    // 类型/运行期错误在执行期报 —— 两处都不许让调用方看到一个裸的 DF 错误
    let nth = |e: DataFusionError| {
        DataFusionError::Execution(format!(
            "定位扫描求谓词失败（文件 {path}）：{e}；表 schema = {:?}",
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
        ))
    };
    let df = ctx.sql(&sql).await.map_err(nth)?;
    let out = df.collect().await.map_err(|e| {
        // 谓词本身出错（列名写错 / 类型不匹配）**必须报错**：猜一个"空结果"就是"什么都没删"
        DataFusionError::Execution(format!(
            "定位扫描求谓词失败（文件 {path}）：{e}；表 schema = {:?}",
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
        ))
    })?;

    let mut positions: Vec<u32> = Vec::new();
    for b in &out {
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
    Ok(LocatedRows { positions, rows })
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
        let (path, _rows, _size) = yuntun_format::write_batch(
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
