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

use std::collections::BTreeMap;

use arrow::array::{Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, ObjectName, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use yuntun_model::agg::{AggKind, AggScalar, AggSpec, KeyPart, PartialAgg};
use yuntun_model::predicate::{CmpOp, Literal, Predicate};
use yuntun_model::error::LakeError;

use crate::cache::CatalogSnapshot;

/// 一次可以下推的聚合查询（判定通过后的形状）。
#[derive(Debug, Clone, PartialEq)]
pub struct Pushdown {
    /// 全限定表标识 `schema.table`（`scan` 与分片接口都用全限定名，`§167` 的教训）
    pub table: String,
    /// 与 `SELECT` 列表**同序**的聚合（合并时按这个下标对齐结果列）
    pub specs: Vec<AggSpec>,
    /// **分组键列**（`§177`；顺序是契约的一部分）。空 = 不分组（结果是"一行"）。
    ///
    /// 非空时 `specs` 是"**每组的**聚合"，且投影必须是"分组键（同序）在前、聚合在后"
    /// —— 否则结果列对不上，一律不下推。
    pub group_by: Vec<String>,
    /// 可选的行过滤（`§176`）：**`None` = 没有 `WHERE`**。
    ///
    /// 有 `WHERE` 但**翻译不出来**（算术 / 函数 / `LIKE` / 浮点 / 跨类型…）时，
    /// [`detect`] 会整体返回 `None`（拿不准不下推）—— 半个谓词比没有谓词危险得多。
    pub predicate: Option<Predicate>,
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

    // `§177`：**分组键**。只收"普通列"（`GROUP BY v+1` 这类表达式键不下推），
    // 且类型必须在支持面内（整数 / 字符串）—— 别的类型（浮点、布尔…）一律不下推。
    let group_by = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => {
            let mut keys = Vec::with_capacity(exprs.len());
            for e in exprs {
                let col = column_of(e)?;
                if !matches!(
                    cached.schema.field_with_name(&col).ok().map(|f| f.data_type()),
                    Some(DataType::Int64 | DataType::Utf8)
                ) {
                    return None;
                }
                keys.push(col);
            }
            keys
        }
        _ => return None,
    };

    // 投影必须是"**分组键（同序）在前、聚合在后**"：这样结果的列才对得上，
    // 也才能用 DataFusion 冷结果给的 schema/列名直接拼装（不必自己编一套列名）。
    let (key_items, agg_items) = select.projection.split_at(group_by.len());
    for (item, want) in key_items.iter().zip(group_by.iter()) {
        let got = match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => column_of(e),
            _ => None,
        };
        if got.as_deref() != Some(want.as_str()) {
            return None;
        }
    }
    let specs = agg_items
        .iter()
        .map(|item| spec_of_item(item, &cached.schema))
        .collect::<Option<Vec<_>>>()?;
    if specs.is_empty() {
        return None;
    }
    // `§176`：可选的行过滤。翻译不了 ⇒ **整体不下推**（见 `Pushdown::predicate` 的注释）。
    let predicate = match &select.selection {
        None => None,
        Some(e) => Some(predicate_of(e, &cached.schema)?),
    };
    Some(Pushdown {
        table: ident,
        specs,
        group_by,
        predicate,
    })
}

/// 把 `WHERE` 表达式翻译成可下推的谓词（`§176`）；**翻译不了就返回 `None`**。
///
/// 支持面窄是刻意的（见 `yuntun_model::predicate` 的模块文档）：比较、`IS [NOT] NULL`、`AND`/`OR`、
/// 括号。其余一律 `None` —— 包括 `NOT`（它需要对三值逻辑取反，而 `NOT UNKNOWN = UNKNOWN`，
/// 写错一处就会多算行；等有明确需求再收）、算术、`LIKE`/`IN`/`BETWEEN`、函数、浮点字面量、跨类型比较。
fn predicate_of(expr: &Expr, schema: &arrow::datatypes::Schema) -> Option<Predicate> {
    use sqlparser::ast::BinaryOperator as BinOp;
    match expr {
        Expr::Nested(inner) => predicate_of(inner, schema),
        Expr::BinaryOp { left, op, right } => match op {
            BinOp::And => Some(Predicate::And(vec![
                predicate_of(left, schema)?,
                predicate_of(right, schema)?,
            ])),
            BinOp::Or => Some(Predicate::Or(vec![
                predicate_of(left, schema)?,
                predicate_of(right, schema)?,
            ])),
            BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq => {
                let (column, op, value) = comparison(left, op, right, schema)?;
                Some(Predicate::Cmp { column, op, value })
            }
            // 算术 / LIKE / 别的二元运算：不碰
            _ => None,
        },
        Expr::IsNull(inner) => Some(Predicate::IsNull {
            column: column_of(inner)?,
            negated: false,
        }),
        Expr::IsNotNull(inner) => Some(Predicate::IsNull {
            column: column_of(inner)?,
            negated: true,
        }),
        _ => None,
    }
}

/// `列 <op> 字面量`（**字面量在左边时把比较符翻过来**，如 `5 < v` ⇒ `v > 5`）。
///
/// 类型必须对得上且**在支持面内**：整数对 `Int64`、字符串对 `Utf8`。
/// 对不上（如 `Int64 > 1.5`）⇒ `None`：那是"跨类型比较"，语义细节不值得赌。
fn comparison(
    left: &Expr,
    op: &sqlparser::ast::BinaryOperator,
    right: &Expr,
    schema: &arrow::datatypes::Schema,
) -> Option<(String, CmpOp, Literal)> {
    use sqlparser::ast::BinaryOperator as BinOp;
    let flip = |op: &BinOp| -> BinOp {
        match op {
            BinOp::Lt => BinOp::Gt,
            BinOp::LtEq => BinOp::GtEq,
            BinOp::Gt => BinOp::Lt,
            BinOp::GtEq => BinOp::LtEq,
            other => other.clone(),
        }
    };
    let op_of = |op: &BinOp| -> Option<CmpOp> {
        Some(match op {
            BinOp::Eq => CmpOp::Eq,
            BinOp::NotEq => CmpOp::Ne,
            BinOp::Lt => CmpOp::Lt,
            BinOp::LtEq => CmpOp::Le,
            BinOp::Gt => CmpOp::Gt,
            BinOp::GtEq => CmpOp::Ge,
            _ => return None,
        })
    };
    // 列在左、字面量在右
    if let (Some(col), Some(lit)) = (column_of(left), literal_of(right)) {
        let op = op_of(op)?;
        return type_ok(schema, &col, &lit).then_some((col, op, lit));
    }
    // 字面量在左、列在右 ⇒ 比较符翻过来（`5 < v` 与 `v > 5` 同义）
    if let (Some(lit), Some(col)) = (literal_of(left), column_of(right)) {
        let op = op_of(&flip(op))?;
        return type_ok(schema, &col, &lit).then_some((col, op, lit));
    }
    None
}

/// 一个**普通列引用**（限定名取最后一段：`t.v` ⇒ `v`）。
fn column_of(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(i) => Some(i.value.clone()),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => Some(parts[1].value.clone()),
        _ => None,
    }
}

/// 一个**受支持的字面量**（整数 / 单引号字符串）。浮点、布尔、NULL、表达式一律不算。
fn literal_of(expr: &Expr) -> Option<Literal> {
    use sqlparser::ast::UnaryOperator as UnOp;
    match expr {
        // `-1` 在 AST 里是**一元负号 + 字面量**（不是纯字面量）。负数字面量在谓词里太常见了
        // （`WHERE v > -1`），所以这里把它**当字面量**收下 —— 它确实只是一个常量，不是算术。
        // 注意与"`v + 1` 这类真算术"的区别：那个的**内层是列**，这里内层必须是字面量。
        Expr::UnaryOp {
            op: UnOp::Minus,
            expr: inner,
        } => match literal_of(inner)? {
            Literal::Int(i) => i.checked_neg().map(Literal::Int),
            Literal::Text(_) => None,
        },
        Expr::UnaryOp {
            op: UnOp::Plus,
            expr: inner,
        } => literal_of(inner),
        // 注意 sqlparser 0.62：`Expr::Value` 装的是 `ValueWithSpan`，真正的值在 `.value` 上
        Expr::Value(v) => match &v.value {
            sqlparser::ast::Value::Number(n, _) => n.parse::<i64>().ok().map(Literal::Int),
            sqlparser::ast::Value::SingleQuotedString(s) => Some(Literal::Text(s.clone())),
            _ => None,
        },
        _ => None,
    }
}

/// 列必须存在，且类型与字面量**同族**（否则不下推）。
fn type_ok(
    schema: &arrow::datatypes::Schema,
    column: &str,
    lit: &Literal,
) -> bool {
    let Ok(field) = schema.field_with_name(column) else {
        return false;
    };
    matches!(
        (field.data_type(), lit),
        (DataType::Int64, Literal::Int(_)) | (DataType::Utf8, Literal::Text(_))
    )
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
    // `WHERE`（`selection`）**不再直接拒绝**：`§176` 起由 `predicate_of` 判定能不能翻译，
    // 翻译不了就整体不下推。`PREWHERE`（ClickHouse 方言）与 `HAVING` 仍然不碰。
    if select.prewhere.is_some() || select.having.is_some() {
        return None;
    }
    // `GROUP BY` 有三种写法（`Expressions` / `All` / …）—— 只有"**列表达式列表**"这一族能下推，
    // 而且**不许带修饰**（`ROLLUP` / `CUBE` / `GROUPING SETS` 会多出"小计行"，那是另一种结果形状）
    match &select.group_by {
        GroupByExpr::Expressions(_, modifiers) if modifiers.is_empty() => {}
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
    hot: &[yuntun_model::agg::AggStates],
) -> Result<RecordBatch, LakeError> {
    if cold.num_rows() != 1 || cold.num_columns() != specs.len() {
        return Err(LakeError::Other(format!(
            "聚合下推的前提不成立：冷结果应当是 1 行 × {} 列，实际 {} 行 × {} 列",
            specs.len(),
            cold.num_rows(),
            cold.num_columns()
        )));
    }
    let mut states = states_of_cold_result(cold, specs)?;
    for per_source in hot {
        states = yuntun_model::agg::merge_states(&states, per_source)?;
    }
    states_to_batch(&cold.schema(), specs, &states)
}

/// **冷结果 → 部分状态**（`merge_*_into_cold` 的逆：DataFusion 算出的那一行/每一行读回状态）。
///
/// 为什么要这个方向：冷的一半可能被**分配**给多个节点分别算（`§180` 之后），
/// 而"哪一半在本地算、哪一半送出去"是**每次查询**的决定 —— 但最终输出必须仍是
/// **一个**带 DataFusion schema/列名的批次。于是流程统一成：
/// `本地那一份 → 状态 → 与各远端状态合并 → 拼回批次`。
pub fn states_of_cold_result(
    cold: &RecordBatch,
    specs: &[AggSpec],
) -> Result<yuntun_model::agg::AggStates, LakeError> {
    let grouped = cold.num_columns() > specs.len();
    if !grouped {
        if cold.num_rows() != 1 || cold.num_columns() != specs.len() {
            return Err(LakeError::Other(format!(
                "聚合下推的前提不成立：不分组时冷结果应当是 1 行 × {} 列，实际 {} 行 × {} 列",
                specs.len(),
                cold.num_rows(),
                cold.num_columns()
            )));
        }
        let mut states = Vec::with_capacity(specs.len());
        for (i, spec) in specs.iter().enumerate() {
            states.push(cold_value_at(cold.column(i), spec, 0)?);
        }
        return Ok(yuntun_model::agg::AggStates::Plain(states));
    }
    let nk = cold.num_columns() - specs.len();
    let mut out = Vec::with_capacity(cold.num_rows());
    for row in 0..cold.num_rows() {
        let mut key = Vec::with_capacity(nk);
        for i in 0..nk {
            key.push(cold_key_at(cold.column(i), row)?);
        }
        let states = (0..specs.len())
            .map(|j| cold_value_at(cold.column(nk + j), &specs[j], row))
            .collect::<Result<Vec<_>, LakeError>>()?;
        out.push(yuntun_model::agg::GroupStates { key, states });
    }
    // 与热数据同一口径：按键升序（两路合到一起时确定）
    out.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(yuntun_model::agg::AggStates::Grouped(out))
}

/// **部分状态 → 批次**（`states_of_cold_result` 的逆；schema/列名由调用方给 —— 它来自 DataFusion）。
pub fn states_to_batch(
    schema: &arrow::datatypes::SchemaRef,
    specs: &[AggSpec],
    states: &yuntun_model::agg::AggStates,
) -> Result<RecordBatch, LakeError> {
    let mut columns: Vec<arrow::array::ArrayRef> = Vec::with_capacity(schema.fields().len());
    match states {
        yuntun_model::agg::AggStates::Plain(p) => {
            for (state, field) in p.iter().zip(schema.fields()) {
                columns.push(array_of(state.clone(), field.data_type())?);
            }
        }
        yuntun_model::agg::AggStates::Grouped(g) => {
            let nk = schema.fields().len() - specs.len();
            for i in 0..nk {
                let vals: Vec<KeyPart> = g.iter().map(|x| x.key[i].clone()).collect();
                columns.push(key_array_of(schema.field(i).data_type(), &vals)?);
            }
            for j in 0..specs.len() {
                let vals: Vec<PartialAgg> = g.iter().map(|x| x.states[j].clone()).collect();
                columns.push(array_of_many(&vals, schema.field(nk + j).data_type())?);
            }
        }
    }
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| LakeError::Other(format!("聚合下推结果拼装失败：{e}")))
}

/// **分组**结果的合并（`§177`）：冷结果已经是"每组一行"，热来源的部分状态按**键**并入。
///
/// 与 [`merge_into_cold`] 的差别是这里要做**键的并集**（只存在于热数据的组要**新增**一行），
/// 而不是"在原行上改值"。顺序按**键升序** ⇒ 确定（可复现），而 `GROUP BY` 的行序本来
/// 就不在契约里（原路也没保证）。
pub fn merge_grouped_into_cold(
    cold: &RecordBatch,
    query: &yuntun_model::agg::AggQuery,
    hot: &[yuntun_model::agg::AggStates],
) -> Result<RecordBatch, LakeError> {
    let nk = query.group_by.len();
    if cold.num_columns() != nk + query.specs.len() {
        return Err(LakeError::Other(format!(
            "聚合下推的前提不成立：分组查询的冷结果应当是 {} 个键列 + {} 个聚合列，实际 {} 列",
            nk,
            query.specs.len(),
            cold.num_columns()
        )));
    }
    let cold_states = states_of_cold_result(cold, &query.specs)?;
    let mut acc: BTreeMap<Vec<KeyPart>, Vec<PartialAgg>> = BTreeMap::new();
    if let yuntun_model::agg::AggStates::Grouped(g) = cold_states {
        for grp in g {
            acc.insert(grp.key, grp.states);
        }
    }
    for source in hot {
        let yuntun_model::agg::AggStates::Grouped(groups) = source else {
            return Err(LakeError::Other(
                "聚合下推的前提不成立：热来源返回的不是分组结果".into(),
            ));
        };
        for g in groups {
            let states = acc.entry(g.key.clone()).or_insert_with(|| {
                g.states
                    .iter()
                    .map(|p| yuntun_model::agg::PartialAgg::empty(p.kind()))
                    .collect()
            });
            for (i, p) in g.states.iter().enumerate() {
                let cur = states.get(i).ok_or_else(|| {
                    LakeError::Other("热来源的状态个数与请求不符".into())
                })?;
                states[i] = yuntun_model::agg::merge(cur, p)?;
            }
        }
    }

    let states = yuntun_model::agg::AggStates::Grouped(
        acc.into_iter()
            .map(|(key, states)| yuntun_model::agg::GroupStates { key, states })
            .collect(),
    );
    states_to_batch(&cold.schema(), &query.specs, &states)
}

/// 冷结果里某个**分组键分量**的取值（NULL 要如实保留 —— 它是一组）。
fn cold_key_at(col: &arrow::array::ArrayRef, row: usize) -> Result<KeyPart, LakeError> {
    if col.is_null(row) {
        return Ok(None);
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Ok(Some(AggScalar::Int(a.value(row))));
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(AggScalar::Text(a.value(row).to_string())));
    }
    Err(LakeError::Other(format!(
        "分组键的类型不认识：{:?}",
        col.data_type()
    )))
}

fn key_array_of(
    want: &DataType,
    vals: &[KeyPart],
) -> Result<arrow::array::ArrayRef, LakeError> {
    match want {
        DataType::Int64 => {
            let mut out = Vec::with_capacity(vals.len());
            for v in vals {
                out.push(match v {
                    None => None,
                    Some(AggScalar::Int(i)) => Some(*i),
                    Some(AggScalar::Text(_)) => {
                        return Err(LakeError::Other("分组键是字符串，但列是 Int64".into()))
                    }
                });
            }
            Ok(std::sync::Arc::new(Int64Array::from(out)))
        }
        DataType::Utf8 => {
            let mut out: Vec<Option<String>> = Vec::with_capacity(vals.len());
            for v in vals {
                out.push(match v {
                    None => None,
                    Some(AggScalar::Text(t)) => Some(t.clone()),
                    Some(AggScalar::Int(_)) => {
                        return Err(LakeError::Other("分组键是整数，但列是 Utf8".into()))
                    }
                });
            }
            Ok(std::sync::Arc::new(StringArray::from(out)))
        }
        other => Err(LakeError::Other(format!(
            "分组键列的类型不支持：{other:?}"
        ))),
    }
}

/// 从冷结果的**第 `row` 行**读出部分状态（分组时每行一组）。
fn cold_value_at(
    col: &arrow::array::ArrayRef,
    spec: &AggSpec,
    row: usize,
) -> Result<PartialAgg, LakeError> {
    let null = col.is_null(row);
    match spec.kind {
        AggKind::CountStar | AggKind::Count => {
            let v = if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                a.value(row) as u64
            } else if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
                a.value(row)
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
            Ok(PartialAgg::SumInt((!null).then(|| a.value(row))))
        }
        AggKind::Min | AggKind::Max => {
            let s = if null {
                None
            } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                Some(AggScalar::Int(a.value(row)))
            } else if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                Some(AggScalar::Text(a.value(row).to_string()))
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
/// 一个聚合列的**一整列**值（分组时是多行）。
///
/// 类型由**冷结果那一列**决定（DataFusion 给的），我们只负责把合并后的状态写进去 ——
/// 状态与列类型不符 ⇒ 响亮失败（那说明判定层放行了一个它不该放行的形状）。
fn array_of_many(
    values: &[PartialAgg],
    want: &DataType,
) -> Result<arrow::array::ArrayRef, LakeError> {
    let mut ints: Vec<Option<i64>> = Vec::with_capacity(values.len());
    let mut texts: Vec<Option<String>> = Vec::with_capacity(values.len());
    let mut saw_text = false;
    for merged in values {
        let (iv, tv): (Option<i64>, Option<String>) = match (merged, want) {
            (PartialAgg::Count(v), DataType::Int64) => (Some(*v as i64), None),
            (PartialAgg::Count(v), DataType::UInt64) => (Some(*v as i64), None),
            (PartialAgg::SumInt(v), DataType::Int64) => (*v, None),
            (PartialAgg::Min(s) | PartialAgg::Max(s), DataType::Int64) => (
                match s {
                    Some(AggScalar::Int(i)) => Some(*i),
                    None => None,
                    Some(AggScalar::Text(_)) => {
                        return Err(LakeError::Other("min/max 合并出了字符串，但列是 Int64".into()))
                    }
                },
                None,
            ),
            (PartialAgg::Min(s) | PartialAgg::Max(s), DataType::Utf8) => (
                None,
                match s {
                    Some(AggScalar::Text(t)) => Some(t.clone()),
                    None => None,
                    Some(AggScalar::Int(_)) => {
                        return Err(LakeError::Other("min/max 合并出了整数，但列是 Utf8".into()))
                    }
                },
            ),
            (got, want) => {
                return Err(LakeError::Other(format!(
                    "聚合下推结果类型与冷结果不一致：合并出 {:?}，冷结果列是 {want:?}",
                    got.kind()
                )))
            }
        };
        saw_text |= tv.is_some();
        ints.push(iv);
        texts.push(tv);
    }
    Ok(if saw_text {
        std::sync::Arc::new(StringArray::from(texts))
    } else if matches!(want, DataType::UInt64) {
        // `count` 在 DataFusion 里是 UInt64 还是 Int64 由它决定，我们照着写
        std::sync::Arc::new(UInt64Array::from(
            ints.into_iter().map(|v| v.unwrap_or(0) as u64).collect::<Vec<_>>(),
        ))
    } else {
        std::sync::Arc::new(Int64Array::from(ints))
    })
}

/// 不分组时用：一个聚合列只有**一个**值。
fn array_of(
    merged: PartialAgg,
    want: &DataType,
) -> Result<arrow::array::ArrayRef, LakeError> {
    array_of_many(std::slice::from_ref(&merged), want)
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
            assert!(got.predicate.is_none(), "{sql} 没有 WHERE");
        }
    }

    /// **谓词（`WHERE`）也要能被翻译**（`§176`），包括两个容易写错的地方：
    /// 字面量在左边时比较符要**翻转**、`AND`/`OR` 按 SQL 的结合与顺序翻译。
    #[test]
    fn where_clauses_are_translated_into_pushable_predicates() {
        let snap = snapshot_with(DataType::Int64);
        let cases = [
            (
                "SELECT count(*) FROM cpu WHERE v > 0",
                Predicate::Cmp {
                    column: "v".into(),
                    op: CmpOp::Gt,
                    value: Literal::Int(0),
                },
            ),
            (
                // 字面量在左 ⇒ 必须翻成 `v > 5`（不翻就是**静默错**）
                "SELECT count(*) FROM cpu WHERE 5 < v",
                Predicate::Cmp {
                    column: "v".into(),
                    op: CmpOp::Gt,
                    value: Literal::Int(5),
                },
            ),
            (
                "SELECT sum(v) FROM cpu WHERE v IS NULL",
                Predicate::IsNull {
                    column: "v".into(),
                    negated: false,
                },
            ),
            (
                "SELECT count(*) FROM cpu WHERE v = 1 AND v <= 3 OR v > 10",
                Predicate::Or(vec![
                    Predicate::And(vec![
                        Predicate::Cmp {
                            column: "v".into(),
                            op: CmpOp::Eq,
                            value: Literal::Int(1),
                        },
                        Predicate::Cmp {
                            column: "v".into(),
                            op: CmpOp::Le,
                            value: Literal::Int(3),
                        },
                    ]),
                    Predicate::Cmp {
                        column: "v".into(),
                        op: CmpOp::Gt,
                        value: Literal::Int(10),
                    },
                ]),
            ),
            (
                // 负数字面量：AST 里是一元负号，但它是常量 ⇒ 收下
                "SELECT count(*) FROM cpu WHERE v < -1",
                Predicate::Cmp {
                    column: "v".into(),
                    op: CmpOp::Lt,
                    value: Literal::Int(-1),
                },
            ),
            (
                "SELECT max(s) FROM cpu WHERE s = 'x'",
                Predicate::Cmp {
                    column: "s".into(),
                    op: CmpOp::Eq,
                    value: Literal::Text("x".into()),
                },
            ),
        ];
        for (sql, want) in cases {
            let got = detect(sql, "public", &snap).unwrap_or_else(|| panic!("{sql} 应当可下推"));
            assert_eq!(got.predicate.as_ref(), Some(&want), "{sql}");
        }
    }

    /// **拿不准就不下推**：每一条都是一个"推了会算错"的形状（`§173` 的判定原则）。
    #[test]
    fn everything_else_falls_back() {
        let snap = snapshot_with(DataType::Int64);
        for sql in [
            // 谓词：**翻译不出来就整体不下推**（`§176`）—— 半个谓词比没有谓词危险得多
            "SELECT count(*) FROM cpu WHERE v LIKE 'a%'",
            "SELECT count(*) FROM cpu WHERE v > 1.5", // 浮点字面量
            "SELECT count(*) FROM cpu WHERE v = 'x'", // 跨类型比较
            "SELECT count(*) FROM cpu WHERE s > 1",   // 同上（列是 Utf8）
            "SELECT count(*) FROM cpu WHERE nope > 1", // 表里没这列
            "SELECT count(*) FROM cpu WHERE abs(v) > 1", // 函数
            "SELECT count(*) FROM cpu WHERE v + 1 > 2", // 算术
            "SELECT count(*) FROM cpu WHERE v BETWEEN 1 AND 2",
            "SELECT count(*) FROM cpu WHERE v IN (1, 2)",
            "SELECT count(*) FROM cpu WHERE NOT (v > 1)", // NOT：三值逻辑取反，本刀不碰
            "SELECT count(*) FROM cpu WHERE v IS TRUE",
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
            yuntun_model::agg::AggStates::Plain(vec![PartialAgg::Count(3), PartialAgg::SumInt(Some(7))]),
            yuntun_model::agg::AggStates::Plain(vec![PartialAgg::Count(0), PartialAgg::SumInt(None)]),
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
            &[yuntun_model::agg::AggStates::Plain(vec![PartialAgg::SumInt(Some(5))])],
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
            merge_into_cold(
                &cold_null,
                &specs,
                &[yuntun_model::agg::AggStates::Plain(vec![PartialAgg::SumInt(None)])],
            )
                .unwrap();
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

    // ------------------------------------------------ 分组合并（`§177`）

    /// 分组结果：`(键, 聚合值)` 表；键是 `Option<Int64>`（`None` = NULL 组）。
    fn cold_grouped(rows: Vec<(Option<i64>, i64)>) -> RecordBatch {
        let keys: Vec<Option<i64>> = rows.iter().map(|(k, _)| *k).collect();
        let vals: Vec<i64> = rows.iter().map(|(_, v)| *v).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, true),
            Field::new("c0", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .unwrap()
    }

    /// 热来源按**键**并入；只存在于热数据的组要**新增一行**（不是"在冷行上改值"）。
    #[test]
    fn grouped_merge_adds_groups_that_only_hot_data_has() {
        let q = yuntun_model::agg::AggQuery {
            specs: vec![AggSpec::count_star()],
            group_by: vec!["v".into()],
            predicate: None,
        };
        // 冷：v=1 有 10 行、NULL 组有 2 行
        let cold_batch = cold_grouped(vec![(Some(1), 10), (None, 2)]);
        let hot = vec![yuntun_model::agg::AggStates::Grouped(vec![
            yuntun_model::agg::GroupStates {
                key: vec![Some(AggScalar::Int(1))],
                states: vec![PartialAgg::Count(3)],
            },
            // 只存在于热数据的组
            yuntun_model::agg::GroupStates {
                key: vec![Some(AggScalar::Int(9))],
                states: vec![PartialAgg::Count(4)],
            },
        ])];
        let got = merge_grouped_into_cold(&cold_batch, &q, &hot).unwrap();
        let k = got.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let c = got.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let mut rows: Vec<(Option<i64>, i64)> = (0..got.num_rows())
            .map(|i| {
                (
                    if k.is_null(i) { None } else { Some(k.value(i)) },
                    c.value(i),
                )
            })
            .collect();
        rows.sort(); // 键升序（NULL 在前）
        assert_eq!(
            rows,
            vec![(None, 2), (Some(1), 13), (Some(9), 4)],
            "NULL 组不受影响、v=1 累加、v=9 是新组"
        );
        assert_eq!(got.schema(), cold_batch.schema(), "schema 必须沿用 DataFusion 的");
    }

    /// 分组形状不符（一边分组一边不分组）⇒ **响亮失败**（少一个组看起来也像正常答案）。
    #[test]
    fn grouped_merge_refuses_a_mismatched_shape() {
        let q = yuntun_model::agg::AggQuery {
            specs: vec![AggSpec::count_star()],
            group_by: vec!["v".into()],
            predicate: None,
        };
        let cold_batch = cold_grouped(vec![(Some(1), 10)]);
        let e = merge_grouped_into_cold(
            &cold_batch,
            &q,
            &[yuntun_model::agg::AggStates::Plain(vec![PartialAgg::Count(1)])],
        )
        .unwrap_err();
        assert!(e.to_string().contains("不是分组结果"), "{e}");
    }
}
