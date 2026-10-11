//! **可分解聚合的下推**（`§173`，`plan.md` G.2 / 归档 `S5-3`）。
//!
//! # 这一层做什么
//!
//! 把 `SELECT count(*), sum(x), min(y), max(z) FROM t`（**无 `WHERE` / 无 `GROUP BY`**）
//! 拆成两半：
//!
//! 1. **冷的一半**：照旧交给 DataFusion（它读已提交文件）—— 这样 **schema、列名、类型**都由
//!    DataFusion 给，我们不必手写一个"看起来一样"的结果集（那正是对拍会红的地方）；
//! 2. **热的一半**：逐来源下发 aggs（[`CatalogOps`][yuntun_catalog::CatalogOps] 之上的
//!    `read_table_aggs`）⇒ 网络里走的是**标量**，不是行；
//! 3. **合并**：把热的部分状态合并进冷结果的那一行（[`merge_into_cold`]）。
//!
//! # 判定原则：**拿不准就不下推**
//!
//! 与状态机那边的纪律方向相反、但道理相同：下推是一种**优化**，
//! "不下推"永远是正确且稍慢的；"推错了"是**结果悄悄错**。所以形状判定只认**最窄**的一类，
//! 任何不确定（有 `WHERE` / 有 `GROUP BY` / 列类型不支持 / 多表 / 子查询 / 有 `LIMIT`…）
//! 一律返回 `None`，让原路跑。
//!
//! # 边界（哪几种聚合能下推、为什么浮点不行）
//!
//! 写在 [`yuntun_model::agg`] 的模块文档里（那一层是"可分解 + 精确"的判定处）。

use arrow::array::{Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, ObjectName, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use yuntun_model::agg::{AggKind, AggScalar, AggSpec, PartialAgg};
use yuntun_model::error::LakeError;

use crate::cache::CatalogSnapshot;

/// 一次可以下推的聚合查询（判定通过后的形状）。
#[derive(Debug, Clone, PartialEq)]
pub struct Pushdown {
    /// 全限定表标识 `schema.table`（`scan` 与分片接口都用全限定名，`§167` 的教训）
    pub table: String,
    /// 与 `SELECT` 列表**同序**的聚合（合并时按这个下标对齐结果列）
    pub specs: Vec<AggSpec>,
}

/// 判定一条 SQL 能否走聚合下推；不能就返回 `None`（调用方走原路）。
pub fn detect(sql: &str, default_schema: &str, snapshot: &CatalogSnapshot) -> Option<Pushdown> {
    let stmts = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    let [Statement::Query(q)] = stmts.as_slice() else {
        return None; // 多语句 / 非查询：不碰
    };
    let select = single_simple_select(q)?;
    let (schema, table) = single_table(select)?;
    let ident = if table.contains('.') {
        table.clone()
    } else {
        yuntun_model::ops::qualified_name(
            if schema.is_empty() {
                default_schema
            } else {
                schema.as_str()
            },
            &table,
        )
    };
    let cached = snapshot.get(&ident)?;
    let specs = select
        .projection
        .iter()
        .map(|item| spec_of_item(item, &cached.schema))
        .collect::<Option<Vec<_>>>()?;
    if specs.is_empty() {
        return None;
    }
    Some(Pushdown { table: ident, specs })
}

/// `SELECT` 里唯一的来源，且**没有别名**（有别名就得处理 `t.c` 与 `a.c` 两种列引用 —— 不必要）。
fn single_table(select: &Select) -> Option<(String, String)> {
    if select.from.len() != 1 {
        return None;
    }
    let factor = &select.from[0];
    if factor.joins.iter().any(|_| true) {
        return None; // 有 JOIN：不碰
    }
    match &factor.relation {
        TableFactor::Table {
            name,
            alias,
            args,
            ..
        } => {
            if alias.is_some() || args.is_some() {
                return None; // 别名 / 表函数：不碰
            }
            let parts = ident_parts(name);
            match parts.len() {
                1 => Some((String::new(), parts[0].clone())),
                2 => Some((parts[0].clone(), parts[1].clone())),
                _ => None, // 三段以上（catalog.schema.table）不在本刀范围
            }
        }
        _ => None, // 子查询 / 表函数 / UNNEST…：不碰
    }
}

/// 只有"单个 `SELECT`、无一切修饰"的查询才可下推。
///
/// 逐条列出来的都是"漏一条就会算错"的：`WHERE`/`HAVING` 需要把谓词也下推（本刀不做 ⇒
/// 热的一半会算进不该算的行），`GROUP BY` 需要每个分组各自的部分状态（本刀不做），
/// `LIMIT`/`ORDER BY` 是"聚合之后"的事（下推到来源上语义不同）。
fn single_simple_select(q: &Query) -> Option<&Select> {
    if q.with.is_some() {
        return None;
    }
    if q.order_by.is_some() {
        return None;
    }
    if q.limit_clause.is_some() || q.fetch.is_some() {
        return None;
    }
    if !q.locks.is_empty() || q.for_clause.is_some() {
        return None;
    }
    let SetExpr::Select(select) = q.body.as_ref() else {
        return None; // UNION / VALUES / 嵌套查询：不碰
    };
    if select.distinct.is_some() {
        return None;
    }
    if select.selection.is_some() || select.prewhere.is_some() || select.having.is_some() {
        return None;
    }
    // `GROUP BY` 有三种写法（`Expressions` / `All` / …）——**只有"空 Expressions"** 才算没有
    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty() => {}
        _ => return None,
    }
    // 下面这些每一个都会改变"聚合在什么之上做"：一律不碰
    if select.top.is_some()
        || select.qualify.is_some()
        || select.into.is_some()
        || select.exclude.is_some()
        || !select.lateral_views.is_empty()
        || !select.named_window.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.connect_by.is_empty()
        || select.value_table_mode.is_some()
    {
        return None;
    }
    Some(select)
}

/// 一个 `SELECT` 项 → 一个可下推的 `AggSpec`；不是可下推的聚合就返回 `None`。
fn spec_of_item(item: &SelectItem, schema: &arrow::datatypes::Schema) -> Option<AggSpec> {
    let expr = match item {
        SelectItem::UnnamedExpr(e) => e,
        // 别名只改**输出列名**（DataFusion 那边照样会给对），合并不关心名字
        SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return None, // `*` / 限定通配：不碰
    };
    let Expr::Function(f) = expr else {
        return None;
    };
    let name = ident_parts(&f.name).join(".").to_ascii_lowercase();
    let empty: Vec<FunctionArg> = Vec::new();
    let args = match &f.args {
        FunctionArguments::List(list) => {
            // `count(DISTINCT v)` 需要"去重后的计数"—— 它**不可分解**（各来源的 distinct 数相加
            // 会把同一个值算多次）。这一类漏判会算错，所以这里跟 `ORDER BY` 一起拒绝。
            if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
                return None;
            }
            &list.args
        }
        // `count(*)` 在部分版本里落到 `FunctionArguments::None`；两种都当"无参数"处理
        FunctionArguments::None => &empty,
        _ => return None,
    };
    match name.as_str() {
        "count" => {
            // `count(*)`：`FunctionArguments::None`，或"单个 WILDCARD 参数"
            if args.is_empty()
                || matches!(
                    args.as_slice(),
                    [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
                )
            {
                return Some(AggSpec::count_star());
            }
            let col = single_column_arg(args)?;
            check_column(schema, &col, AggKind::Count)?;
            Some(AggSpec::new(AggKind::Count, col))
        }
        "sum" => {
            let col = single_column_arg(args)?;
            check_column(schema, &col, AggKind::SumInt)?;
            Some(AggSpec::new(AggKind::SumInt, col))
        }
        "min" | "max" => {
            let col = single_column_arg(args)?;
            let kind = if name == "min" { AggKind::Min } else { AggKind::Max };
            check_column(schema, &col, kind)?;
            Some(AggSpec::new(kind, col))
        }
        _ => None, // avg / stddev / count(distinct) / 自定函数…：不碰
    }
}

/// `count(*)` 之外只接受"单个普通列"参数。
fn single_column_arg(args: &[FunctionArg]) -> Option<String> {
    if args.len() != 1 {
        return None;
    }
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = &args[0] else {
        return None; // 通配 `*` / 命名参数 / 别的形状
    };
    match e {
        Expr::Identifier(i) => Some(i.value.clone()),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => Some(parts[1].value.clone()),
        _ => None, // 表达式参数（`sum(x+y)`）不做
    }
}

/// 列必须存在，且类型在**这一刀的下推边界**内（见 `yuntun_model::agg` 的模块文档）。
fn check_column(
    schema: &arrow::datatypes::Schema,
    column: &str,
    kind: AggKind,
) -> Option<()> {
    let dt = schema.field_with_name(column).ok()?.data_type();
    let ok = match kind {
        AggKind::CountStar | AggKind::Count => true,
        // 浮点求和不满足结合律 ⇒ 不下推
        AggKind::SumInt => matches!(dt, DataType::Int64),
        // 浮点的 NaN 口径要先复刻 ⇒ 不下推
        AggKind::Min | AggKind::Max => matches!(dt, DataType::Int64 | DataType::Utf8),
    };
    ok.then_some(())
}

fn ident_parts(name: &ObjectName) -> Vec<String> {
    name.0
        .iter()
        .map(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(i) => i.value.clone(),
            other => other.to_string(),
        })
        .collect()
}

/// 把热来源的部分状态合并进**冷结果的唯一一行**（`§173`）。
///
/// `hot[source][spec]` 的形状由调用方保证（与 `specs` 同序同长）。
///
/// 为什么"原地改 DataFusion 的结果"而不是自己拼一个结果集：列名、类型、可空性、以及
/// `count`/`sum` 的**返回类型**都是 DataFusion 定的（`Int64` vs `UInt64` 差一点就会让
/// 客户端拿到的 schema 变样）。这里只改**值**，schema 原样不动。
pub fn merge_into_cold(
    cold: &RecordBatch,
    specs: &[AggSpec],
    hot: &[Vec<PartialAgg>],
) -> Result<RecordBatch, LakeError> {
    if cold.num_rows() != 1 || cold.num_columns() != specs.len() {
        return Err(LakeError::Other(format!(
            "聚合下推的前提不成立：冷结果应当是 1 行 × {} 列，实际 {} 行 × {} 列",
            specs.len(),
            cold.num_rows(),
            cold.num_columns()
        )));
    }
    let mut columns: Vec<arrow::array::ArrayRef> = Vec::with_capacity(specs.len());
    for (i, spec) in specs.iter().enumerate() {
        let mut acc = cold_value_of(cold.column(i), spec)?;
        for per_source in hot {
            acc = yuntun_model::agg::merge(&acc, &per_source[i])?;
        }
        columns.push(array_of(acc, cold.column(i).data_type())?);
    }
    RecordBatch::try_new(cold.schema(), columns)
        .map_err(|e| LakeError::Other(format!("聚合下推结果拼装失败：{e}")))
}

/// 从冷结果的**第一个值**读出部分状态（NULL 要如实保留：`count` 是 0，`sum/min/max` 是 NULL）。
fn cold_value_of(col: &arrow::array::ArrayRef, spec: &AggSpec) -> Result<PartialAgg, LakeError> {
    let null = col.is_null(0);
    match spec.kind {
        AggKind::CountStar | AggKind::Count => {
            let v = if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                a.value(0) as u64
            } else if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
                a.value(0)
            } else {
                return Err(LakeError::Other(format!(
                    "count 的返回类型不认识：{:?}",
                    col.data_type()
                )));
            };
            Ok(PartialAgg::Count(v))
        }
        AggKind::SumInt => {
            let a = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                LakeError::Other(format!("sum 的返回类型不是 Int64：{:?}", col.data_type()))
            })?;
            Ok(PartialAgg::SumInt((!null).then(|| a.value(0))))
        }
        AggKind::Min | AggKind::Max => {
            let s = if null {
                None
            } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                Some(AggScalar::Int(a.value(0)))
            } else if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                Some(AggScalar::Text(a.value(0).to_string()))
            } else {
                return Err(LakeError::Other(format!(
                    "min/max 的返回类型不认识：{:?}",
                    col.data_type()
                )));
            };
            Ok(if matches!(spec.kind, AggKind::Min) {
                PartialAgg::Min(s)
            } else {
                PartialAgg::Max(s)
            })
        }
    }
}

/// 合并后的部分状态 → 与冷结果**同类型**的单元素数组（schema 不动）。
fn array_of(
    merged: PartialAgg,
    want: &DataType,
) -> Result<arrow::array::ArrayRef, LakeError> {
    match (merged, want) {
        (PartialAgg::Count(v), DataType::Int64) => {
            Ok(std::sync::Arc::new(Int64Array::from(vec![v as i64])))
        }
        (PartialAgg::Count(v), DataType::UInt64) => {
            Ok(std::sync::Arc::new(UInt64Array::from(vec![v])))
        }
        (PartialAgg::SumInt(v), DataType::Int64) => {
            Ok(std::sync::Arc::new(Int64Array::from(vec![v])))
        }
        (PartialAgg::Min(s) | PartialAgg::Max(s), DataType::Int64) => {
            let v = match s {
                Some(AggScalar::Int(i)) => Some(i),
                None => None,
                Some(AggScalar::Text(_)) => {
                    return Err(LakeError::Other("min/max 合并出了字符串，但列是 Int64".into()));
                }
            };
            Ok(std::sync::Arc::new(Int64Array::from(vec![v])))
        }
        (PartialAgg::Min(s) | PartialAgg::Max(s), DataType::Utf8) => {
            let v = match s {
                Some(AggScalar::Text(t)) => Some(t),
                None => None,
                Some(AggScalar::Int(_)) => {
                    return Err(LakeError::Other("min/max 合并出了整数，但列是 Utf8".into()));
                }
            };
            Ok(std::sync::Arc::new(StringArray::from(vec![v])))
        }
        (got, want) => Err(LakeError::Other(format!(
            "聚合下推结果类型与冷结果不一致：合并出 {:?}，冷结果列是 {want:?}",
            got.kind()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn snapshot_with(v: DataType) -> CatalogSnapshot {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", v, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        CatalogSnapshot::from_schema_for_test("public.cpu", schema)
    }

    #[test]
    fn the_narrow_shape_is_recognized() {
        let snap = snapshot_with(DataType::Int64);
        for (sql, want) in [
            (
                "SELECT count(*) FROM cpu",
                vec![AggSpec::count_star()],
            ),
            (
                "SELECT count(v), sum(v), min(v), max(v) FROM cpu",
                vec![
                    AggSpec::new(AggKind::Count, "v"),
                    AggSpec::new(AggKind::SumInt, "v"),
                    AggSpec::new(AggKind::Min, "v"),
                    AggSpec::new(AggKind::Max, "v"),
                ],
            ),
            (
                "SELECT max(s) FROM public.cpu",
                vec![AggSpec::new(AggKind::Max, "s")],
            ),
            (
                "SELECT count(*) AS c FROM cpu",
                vec![AggSpec::count_star()],
            ),
        ] {
            let got = detect(sql, "public", &snap).unwrap_or_else(|| panic!("{sql} 应当可下推"));
            assert_eq!(got.table, "public.cpu", "{sql}");
            assert_eq!(got.specs, want, "{sql}");
        }
    }

    /// **拿不准就不下推**：每一条都是一个"推了会算错"的形状（`§173` 的判定原则）。
    #[test]
    fn everything_else_falls_back() {
        let snap = snapshot_with(DataType::Int64);
        for sql in [
            // 谓词：热的一半没法只算该算的行（本刀不下推谓词）
            "SELECT count(*) FROM cpu WHERE v > 1",
            // 分组：每个分组要各自的部分状态
            "SELECT count(*) FROM cpu GROUP BY v",
            "SELECT count(*) FROM cpu HAVING count(*) > 1",
            // 聚合之后的事
            "SELECT count(*) FROM cpu LIMIT 1",
            "SELECT count(*) FROM cpu ORDER BY 1",
            // 不满足"可分解"或"精确"
            "SELECT avg(v) FROM cpu",
            "SELECT count(DISTINCT v) FROM cpu",
            "SELECT stddev(v) FROM cpu",
            // 表达式的参数
            "SELECT sum(v + 1) FROM cpu",
            // 多表 / 子查询 / CTE / 别名
            "SELECT count(*) FROM cpu a",
            "SELECT count(*) FROM cpu, other",
            "SELECT count(*) FROM (SELECT 1) t",
            "WITH x AS (SELECT 1) SELECT count(*) FROM x",
            // UNION
            "SELECT count(*) FROM cpu UNION ALL SELECT 1",
            // 不存在的列 / 不存在的表
            "SELECT sum(nope) FROM cpu",
            "SELECT count(*) FROM nope",
            // 非聚合列：合并不了
            "SELECT v FROM cpu",
            // 普通 SQL 之外的东西
            "CREATE TABLE t (a INT)",
        ] {
            assert!(
                detect(sql, "public", &snap).is_none(),
                "{sql} 不该被下推"
            );
        }
    }

    /// 浮点列：`sum` / `min` / `max` 都不下推（结合律 / NaN 口径），`count` 仍然可以。
    #[test]
    fn float_columns_are_only_countable() {
        let snap = snapshot_with(DataType::Float64);
        assert!(detect("SELECT sum(v) FROM cpu", "public", &snap).is_none());
        assert!(detect("SELECT min(v) FROM cpu", "public", &snap).is_none());
        assert_eq!(
            detect("SELECT count(v) FROM cpu", "public", &snap)
                .expect("count 与类型无关")
                .specs,
            vec![AggSpec::new(AggKind::Count, "v")]
        );
    }

    fn cold(values: Vec<PartialAgg>) -> RecordBatch {
        let mut cols: Vec<arrow::array::ArrayRef> = Vec::new();
        let mut fields: Vec<Field> = Vec::new();
        for (i, v) in values.iter().enumerate() {
            match v {
                PartialAgg::Count(c) => {
                    fields.push(Field::new(format!("c{i}"), DataType::Int64, false));
                    cols.push(Arc::new(Int64Array::from(vec![*c as i64])));
                }
                PartialAgg::SumInt(s) => {
                    fields.push(Field::new(format!("c{i}"), DataType::Int64, true));
                    cols.push(Arc::new(Int64Array::from(vec![*s])));
                }
                _ => unimplemented!("用例只用到 count / sum"),
            }
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    /// 合并：冷 + 各热来源，**逐值精确**；NULL 语义不被破坏。
    #[test]
    fn merge_adds_hot_contributions_exactly() {
        let specs = vec![AggSpec::count_star(), AggSpec::new(AggKind::SumInt, "v")];
        let cold_batch = cold(vec![PartialAgg::Count(10), PartialAgg::SumInt(Some(100))]);
        let hot = vec![
            vec![PartialAgg::Count(3), PartialAgg::SumInt(Some(7))],
            vec![PartialAgg::Count(0), PartialAgg::SumInt(None)],
        ];
        let got = merge_into_cold(&cold_batch, &specs, &hot).unwrap();
        assert_eq!(got.num_rows(), 1);
        let c = got.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let s = got.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(c.value(0), 13, "10 + 3 + 0");
        assert_eq!(s.value(0), 107, "100 + 7 + NULL");
    }

    /// 冷的一半是 NULL（空表）而热的有值 ⇒ 结果**不是 NULL**；两边都 NULL ⇒ 仍是 NULL。
    #[test]
    fn merge_keeps_null_only_when_everything_is_null() {
        let specs = vec![AggSpec::new(AggKind::SumInt, "v")];
        let cold_null = cold(vec![PartialAgg::SumInt(None)]);
        let with_hot = merge_into_cold(
            &cold_null,
            &specs,
            &[vec![PartialAgg::SumInt(Some(5))]],
        )
        .unwrap();
        assert_eq!(
            with_hot
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            5
        );
        let all_null =
            merge_into_cold(&cold_null, &specs, &[vec![PartialAgg::SumInt(None)]]).unwrap();
        assert!(all_null.column(0).is_null(0), "全 NULL ⇒ 仍然是 NULL（不是 0）");
    }

    /// 形状不成立（列数不符）时**响亮失败**，不许悄悄返回一个少了一列的批次。
    #[test]
    fn merge_refuses_a_mismatched_cold_batch() {
        let specs = vec![AggSpec::count_star(), AggSpec::new(AggKind::SumInt, "v")];
        let only_one = cold(vec![PartialAgg::Count(1)]);
        let e = merge_into_cold(&only_one, &specs, &[]).unwrap_err();
        assert!(e.to_string().contains("前提不成立"), "{e}");
    }
}
