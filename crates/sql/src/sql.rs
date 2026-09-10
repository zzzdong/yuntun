//! SQL 前置解析纯函数模块（plan v2.0 §4.3 / operation-log §8.2，S1.6 / S1.7；
//! v13 自 server 迁入 yuntun-sql 能力 crate——SQL 语义唯一实现，协议适配层共用）。
//!
//! **原则**：把 SQL 解析为 AST 后按变体分流——只有 SELECT 让 DataFusion 处理；
//! DML / DDL 直接落到 ingest / catalog 能力（DataFusion 不触碰，避免会话级
//! 副作用与持久化 Catalog 语义冲突）。
//!
//! 技术选型：`sqlparser`（与 DataFusion 同源解析器，版本对齐其依赖 0.62）。
//! 本模块只承载**纯函数**（AST → 数据 / 元数据），分流编排（catalog / ingest /
//! query 调用）在 [`crate::SqlEngine`]。

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_cast::cast;
use sqlparser::ast::{
    ColumnDef, CreateTable, DataType as SqlDataType, Expr, ObjectName, Statement,
};
use sqlparser::ast::{ExactNumberInfo, TimezoneInfo, Value};
use sqlparser::dialect::{Dialect as SqlDialect, GenericDialect};
use sqlparser::parser::Parser;
use yuntun_model::error::LakeError;

// ------------------------------------------------------------ 解析入口

/// 单语句解析（plan §4.3：多语句 `;` 分隔明确拒绝，避免部分执行的语义复杂度）。
pub fn parse_single(sql: &str) -> Result<Statement, LakeError> {
    parse_single_with(&GenericDialect {}, sql)
}

/// 按方言的单语句解析（wire 协议会话传入 MySQL/PG 方言）。
pub fn parse_single_with(dialect: &dyn SqlDialect, sql: &str) -> Result<Statement, LakeError> {
    let stmts = Parser::parse_sql(dialect, sql)
        .map_err(|e| LakeError::Other(format!("SQL parse error: {e}")))?;
    if stmts.len() != 1 {
        return Err(LakeError::Other(format!(
            "exactly one SQL statement expected, got {} (multi-statement input is rejected)",
            stmts.len()
        )));
    }
    Ok(stmts.into_iter().next().expect("len checked"))
}

/// INSERT 目标表名（AST 优先；解析失败回落简易字符串匹配——兼容 prepared
/// statement 的不完整形态 `INSERT INTO t (a, b)`）。
pub fn insert_target(sql: &str) -> Option<String> {
    match parse_single(sql) {
        Ok(Statement::Insert(ins)) => match &ins.table {
            sqlparser::ast::TableObject::TableName(name) => Some(table_name_of(name)),
            _ => None,
        },
        _ => insert_target_table_str(sql),
    }
}

/// INSERT 目标表名解析（双引号 / 反引号 / 裸名）。解析失败 → None。
///
/// 仅作 [`insert_target`]（AST 优先）的**回落**：prepared statement 允许不完整
/// 形态 `INSERT INTO t (a, b)`（无 VALUES/SELECT 源），sqlparser 解析失败时由此
/// 字符串匹配兜底（S1.6 后保留该回落路径）。
pub fn insert_target_table_str(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    let pos = lower.find("insert into")?;
    let rest = sql[pos + "insert into".len()..].trim_start();
    let bytes = rest.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    let (name, _) = match bytes[0] {
        b'"' => {
            let end = rest[1..].find('"')? + 1;
            (&rest[1..end], end + 1)
        }
        b'`' => {
            let end = rest[1..].find('`')? + 1;
            (&rest[1..end], end + 1)
        }
        _ => {
            let end = rest
                .find(|c: char| c.is_whitespace() || c == '(' || c == ';')
                .unwrap_or(rest.len());
            (rest[..end].trim_end(), end)
        }
    };
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// ObjectName → 表名（取最后一个 Identifier 部分；`yuntun.public.t` → `t`）。
pub fn table_name_of(name: &ObjectName) -> String {
    name.0
        .iter()
        .rev()
        .find_map(|p| p.as_ident().map(|i| i.value.clone()))
        .unwrap_or_default()
}

// ------------------------------------------------------------ SHOW TABLES

/// SHOW TABLES → Catalog list_tables 的结果批次
/// （列结构与 DataFusion `SHOW TABLES` 一致，客户端输出可互换）。
pub fn show_tables_batch(tables: &[String]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("table_catalog", DataType::Utf8, false),
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
    ]));
    let mut names = tables.to_vec();
    names.sort();
    let n = names.len();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![yuntun_query::CATALOG_NAME; n])) as ArrayRef,
            Arc::new(StringArray::from(vec![yuntun_query::SCHEMA_NAME; n])) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
        ],
    )
    .expect("show tables batch")
}

// ------------------------------------------------------------ CREATE TABLE

/// CREATE TABLE 解析结果。
pub struct ParsedCreateTable {
    pub name: String,
    pub schema: SchemaRef,
    pub if_not_exists: bool,
}

/// `CREATE TABLE` 列定义 → Arrow Schema（plan §4.3：ColumnDef 的 DataType → Arrow）。
pub fn parse_create_table(ct: &CreateTable) -> Result<ParsedCreateTable, LakeError> {
    if ct.or_replace {
        return Err(LakeError::Other(
            "CREATE OR REPLACE TABLE is not supported (use DROP TABLE + CREATE TABLE)".into(),
        ));
    }
    if ct.query.is_some() {
        return Err(LakeError::Other(
            "CREATE TABLE AS SELECT is not supported (MVP DDL is column-definition only)".into(),
        ));
    }
    if ct.columns.is_empty() {
        return Err(LakeError::Other(
            "CREATE TABLE requires column definitions".into(),
        ));
    }
    let mut fields = Vec::with_capacity(ct.columns.len());
    for col in &ct.columns {
        fields.push(column_def_to_field(col)?);
    }
    let name = table_name_of(&ct.name);
    if name.is_empty() {
        return Err(LakeError::Other(
            "invalid table name in CREATE TABLE".into(),
        ));
    }
    Ok(ParsedCreateTable {
        name,
        schema: Arc::new(Schema::new(fields)),
        if_not_exists: ct.if_not_exists,
    })
}

/// 单列定义 → Arrow Field（NOT NULL → nullable=false；其余约束 MVP 忽略）。
fn column_def_to_field(col: &ColumnDef) -> Result<Field, LakeError> {
    let dt = map_column_type(&col.data_type)?;
    let not_null = col
        .options
        .iter()
        .any(|o| matches!(o.option, sqlparser::ast::ColumnOption::NotNull));
    Ok(Field::new(col.name.value.clone(), dt, !not_null))
}

/// sqlparser DataType → Arrow DataType（MVP 映射表；不支持类型明确拒绝）。
pub fn map_column_type(dt: &SqlDataType) -> Result<DataType, LakeError> {
    let t = match dt {
        SqlDataType::Boolean => DataType::Boolean,
        SqlDataType::TinyInt(_) => DataType::Int8,
        SqlDataType::SmallInt(_) => DataType::Int16,
        SqlDataType::Int(_) | SqlDataType::Integer(_) => DataType::Int32,
        SqlDataType::BigInt(_) => DataType::Int64,
        SqlDataType::Int8(_) => DataType::Int8,
        SqlDataType::Int16 => DataType::Int16,
        SqlDataType::Int32 => DataType::Int32,
        SqlDataType::Int64 => DataType::Int64,
        SqlDataType::UInt8 => DataType::UInt8,
        SqlDataType::UInt16 => DataType::UInt16,
        SqlDataType::UInt32 => DataType::UInt32,
        SqlDataType::UInt64 => DataType::UInt64,
        SqlDataType::Real => DataType::Float32,
        SqlDataType::Float(_)
        | SqlDataType::Double(_)
        | SqlDataType::DoublePrecision
        | SqlDataType::DoubleUnsigned(_) => DataType::Float64,
        SqlDataType::Char(_)
        | SqlDataType::Varchar(_)
        | SqlDataType::Text
        | SqlDataType::String(_)
        | SqlDataType::JSON
        | SqlDataType::JSONB => DataType::Utf8,
        SqlDataType::Binary(_)
        | SqlDataType::Blob(_)
        | SqlDataType::Varbinary(_)
        | SqlDataType::Bytea => DataType::Binary,
        SqlDataType::Date | SqlDataType::Date32 => DataType::Date32,
        SqlDataType::Timestamp(_, tz) => match tz {
            TimezoneInfo::None => DataType::Timestamp(TimeUnit::Nanosecond, None),
            other => {
                return Err(LakeError::Other(format!(
                    "timestamp with timezone ({other}) is not supported in CREATE TABLE"
                )))
            }
        },
        SqlDataType::Datetime(_) => DataType::Timestamp(TimeUnit::Nanosecond, None),
        SqlDataType::Decimal(info) => decimal_type(info)?,
        other => {
            return Err(LakeError::Other(format!(
                "unsupported column type in CREATE TABLE: {other}"
            )))
        }
    };
    Ok(t)
}

/// DECIMAL → Decimal128(p, s)；未指定精度 → (38, 10)。
fn decimal_type(info: &ExactNumberInfo) -> Result<DataType, LakeError> {
    let (p, s) = match info {
        ExactNumberInfo::None => (38u32, 10i8),
        ExactNumberInfo::Precision(p) => (*p as u32, 0),
        ExactNumberInfo::PrecisionAndScale(p, sc) => (*p as u32, *sc as i8),
    };
    if p == 0 || p > 38 || s < 0 || s as u32 > p {
        return Err(LakeError::Other(format!(
            "unsupported DECIMAL precision/scale: ({p}, {s})"
        )));
    }
    Ok(DataType::Decimal128(p as u8, s))
}

// ------------------------------------------------------------ INSERT VALUES

/// AST 字面量归一化中间值（按目标列类型再做最终转换）。
#[derive(Debug, Clone)]
enum Literal {
    Null,
    Bool(bool),
    /// 数值字面量原文（整数 / 浮点）
    Num(String),
    Str(String),
    /// TIMESTAMP '...' 的 ISO8601 解析结果（epoch 纳秒）
    TimestampNs(i64),
    /// DATE '...' 的 ISO 原文
    Date(String),
}

fn literal_of(expr: &Expr) -> Result<Literal, LakeError> {
    match expr {
        Expr::Value(v) => match &v.value {
            Value::Null => Ok(Literal::Null),
            Value::Boolean(b) => Ok(Literal::Bool(*b)),
            Value::Number(n, _) => Ok(Literal::Num(n.clone())),
            v => v
                .clone()
                .into_string()
                .map(Literal::Str)
                .ok_or_else(|| LakeError::Other(format!("unsupported literal: {expr}"))),
        },
        Expr::TypedString(ts) => {
            let s = ts
                .value
                .clone()
                .into_string()
                .ok_or_else(|| LakeError::Other("invalid typed string literal".into()))?;
            match ts.data_type {
                SqlDataType::Timestamp(_, _) | SqlDataType::Datetime(_) => parse_iso8601_ns(&s)
                    .map(Literal::TimestampNs)
                    .ok_or_else(|| LakeError::Other(format!("invalid TIMESTAMP literal: {s:?}"))),
                SqlDataType::Date | SqlDataType::Date32 => Ok(Literal::Date(s)),
                ref t => Err(LakeError::Other(format!(
                    "unsupported typed literal: {t:?} {s:?}"
                ))),
            }
        }
        other => Err(LakeError::Other(format!(
            "only literals are supported in INSERT VALUES, got expression: {}",
            expr_snippet(other)
        ))),
    }
}

/// 表达式片段（错误信息用，截断防刷屏）。
fn expr_snippet(e: &Expr) -> String {
    let s = e.to_string();
    if s.chars().count() > 60 {
        format!("{}...", s.chars().take(60).collect::<String>())
    } else {
        s
    }
}

/// 按表 schema 构造 `INSERT INTO t (cols...) VALUES (...)` 的 RecordBatch。
///
/// 规则（plan §4.3）：
/// - 列清单 `INSERT INTO t (a, b)` 支持指定列，缺省列填 NULL（NOT NULL 列缺失则报错）；
/// - 无列清单按表 schema 全列按序；
/// - 类型按表 schema 逐列转换（含范围检查）。
pub fn build_values_batch(
    table: &SchemaRef,
    columns: Option<&[ObjectName]>,
    rows: &[Vec<Expr>],
) -> Result<(RecordBatch, usize), LakeError> {
    if rows.is_empty() {
        return Err(LakeError::Other(
            "INSERT VALUES requires at least one row".into(),
        ));
    }

    // ① 目标列对齐（缺失列填 NULL）
    let field_indices: Vec<usize> = match columns {
        None => (0..table.fields().len()).collect(),
        Some(cols) => {
            if cols.is_empty() {
                return Err(LakeError::Other("empty column list in INSERT".into()));
            }
            let mut idx = Vec::with_capacity(cols.len());
            for c in cols {
                let name = table_name_of(c);
                if name.is_empty() {
                    return Err(LakeError::Other("invalid column name in INSERT".into()));
                }
                match table.index_of(&name) {
                    Ok(i) => {
                        if idx.contains(&i) {
                            return Err(LakeError::Other(format!(
                                "duplicate column in INSERT: {name}"
                            )));
                        }
                        idx.push(i);
                    }
                    Err(_) => {
                        return Err(LakeError::Other(format!(
                            "unknown column in INSERT: {name}"
                        )))
                    }
                }
            }
            idx
        }
    };

    let width = field_indices.len();
    let n_rows = rows.len();
    let mut selected: Vec<Vec<Literal>> = Vec::with_capacity(n_rows);
    for (ri, row) in rows.iter().enumerate() {
        if row.len() != width {
            return Err(LakeError::Other(format!(
                "INSERT row {ri}: expected {width} values, got {}",
                row.len()
            )));
        }
        selected.push(
            row.iter()
                .map(literal_of)
                .collect::<Result<Vec<_>, LakeError>>()?,
        );
    }

    // ② 逐列转换（含 NOT NULL 缺失检查）
    let all_fields = table.fields();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(all_fields.len());
    for (fi, field) in all_fields.iter().enumerate() {
        match field_indices.iter().position(|&i| i == fi) {
            Some(pos) => cols.push(build_column(field, n_rows, |r| selected[r][pos].clone())?),
            None => {
                if !field.is_nullable() {
                    return Err(LakeError::Other(format!(
                        "column {} is NOT NULL but missing in INSERT",
                        field.name()
                    )));
                }
                cols.push(arrow::array::new_null_array(field.data_type(), n_rows));
            }
        }
    }
    let batch = RecordBatch::try_new(table.clone(), cols)
        .map_err(|e| LakeError::Other(format!("build insert batch: {e}")))?;
    Ok((batch, n_rows))
}

/// 按列类型把字面量流转换为 Arrow 数组（范围检查内联完成）。
fn build_column(
    field: &Field,
    n: usize,
    get: impl Fn(usize) -> Literal,
) -> Result<ArrayRef, LakeError> {
    use Literal as L;

    macro_rules! int_col {
        ($arr:ty, $ty:ty) => {{
            let mut v: Vec<Option<$ty>> = Vec::with_capacity(n);
            for i in 0..n {
                match get(i) {
                    L::Null => v.push(None),
                    L::Num(s) => {
                        let x: i64 = s.parse().map_err(|_| bad_num(&s, field))?;
                        let c = <$ty>::try_from(x).map_err(|_| bad_num(&s, field))?;
                        v.push(Some(c));
                    }
                    other => return Err(bad_lit(&other, field)),
                }
            }
            Arc::new(<$arr>::from(v)) as ArrayRef
        }};
    }
    macro_rules! uint_col {
        ($arr:ty, $ty:ty) => {{
            let mut v: Vec<Option<$ty>> = Vec::with_capacity(n);
            for i in 0..n {
                match get(i) {
                    L::Null => v.push(None),
                    L::Num(s) => {
                        let x: u64 = s.parse().map_err(|_| bad_num(&s, field))?;
                        let c = <$ty>::try_from(x).map_err(|_| bad_num(&s, field))?;
                        v.push(Some(c));
                    }
                    other => return Err(bad_lit(&other, field)),
                }
            }
            Arc::new(<$arr>::from(v)) as ArrayRef
        }};
    }

    let arr: ArrayRef = match field.data_type() {
        DataType::Boolean => {
            let mut v: Vec<Option<bool>> = Vec::with_capacity(n);
            for i in 0..n {
                match get(i) {
                    L::Null => v.push(None),
                    L::Bool(b) => v.push(Some(b)),
                    other => return Err(bad_lit(&other, field)),
                }
            }
            Arc::new(BooleanArray::from(v))
        }
        DataType::Int8 => int_col!(Int8Array, i8),
        DataType::Int16 => int_col!(Int16Array, i16),
        DataType::Int32 => int_col!(Int32Array, i32),
        DataType::Int64 => int_col!(Int64Array, i64),
        DataType::UInt8 => uint_col!(UInt8Array, u8),
        DataType::UInt16 => uint_col!(UInt16Array, u16),
        DataType::UInt32 => uint_col!(UInt32Array, u32),
        DataType::UInt64 => uint_col!(UInt64Array, u64),
        DataType::Float32 => {
            let mut v: Vec<Option<f32>> = Vec::with_capacity(n);
            for i in 0..n {
                match get(i) {
                    L::Null => v.push(None),
                    L::Num(s) => v.push(Some(s.parse().map_err(|_| bad_num(&s, field))?)),
                    other => return Err(bad_lit(&other, field)),
                }
            }
            Arc::new(Float32Array::from(v))
        }
        DataType::Float64 => {
            let mut v: Vec<Option<f64>> = Vec::with_capacity(n);
            for i in 0..n {
                match get(i) {
                    L::Null => v.push(None),
                    L::Num(s) => v.push(Some(s.parse().map_err(|_| bad_num(&s, field))?)),
                    other => return Err(bad_lit(&other, field)),
                }
            }
            Arc::new(Float64Array::from(v))
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let mut v: Vec<Option<String>> = Vec::with_capacity(n);
            for i in 0..n {
                match get(i) {
                    L::Null => v.push(None),
                    L::Str(s) => v.push(Some(s)),
                    other => return Err(bad_lit(&other, field)),
                }
            }
            Arc::new(StringArray::from(v))
        }
        DataType::Binary | DataType::LargeBinary => {
            let mut v: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
            for i in 0..n {
                match get(i) {
                    L::Null => v.push(None),
                    L::Str(s) => v.push(Some(s.into_bytes())),
                    other => return Err(bad_lit(&other, field)),
                }
            }
            Arc::new(BinaryArray::from_iter(v.iter().map(|o| o.as_deref())))
        }
        DataType::Date32 => {
            let mut v: Vec<Option<i32>> = Vec::with_capacity(n);
            for i in 0..n {
                match get(i) {
                    L::Null => v.push(None),
                    L::Date(s) | L::Str(s) => {
                        let days = parse_date_days(&s).ok_or_else(|| {
                            LakeError::Other(format!("invalid DATE literal: {s:?}"))
                        })?;
                        v.push(Some(days));
                    }
                    other => return Err(bad_lit(&other, field)),
                }
            }
            Arc::new(Date32Array::from(v))
        }
        DataType::Timestamp(unit, None) => {
            // scale：目标 TimeUnit 相对纳秒的除数
            let scale: i64 = match unit {
                TimeUnit::Second => 1_000_000_000,
                TimeUnit::Millisecond => 1_000_000,
                TimeUnit::Microsecond => 1_000,
                TimeUnit::Nanosecond => 1,
            };
            let mut v: Vec<Option<i64>> = Vec::with_capacity(n);
            for i in 0..n {
                match get(i) {
                    L::Null => v.push(None),
                    L::TimestampNs(ns) => v.push(Some(ns / scale)),
                    // ISO8601 文本（wire 二进制日期参数解码后即此形态；
                    // 也覆盖 `VALUES ('2026-01-02 03:04:05')` 的常规写法）
                    L::Str(s) => {
                        let ns = parse_iso8601_ns(&s).ok_or_else(|| {
                            LakeError::Other(format!("invalid TIMESTAMP literal: {s:?}"))
                        })?;
                        v.push(Some(ns / scale));
                    }
                    // 整型字面量 = 毫秒（plan §4.3：ISO8601（UTC）与整型毫秒）
                    L::Num(s) => {
                        let ms: i64 = s.parse().map_err(|_| bad_num(&s, field))?;
                        v.push(Some(ms.saturating_mul(1_000_000) / scale));
                    }
                    other => return Err(bad_lit(&other, field)),
                }
            }
            match unit {
                TimeUnit::Second => Arc::new(TimestampSecondArray::from(v)) as ArrayRef,
                TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(v)) as ArrayRef,
                TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(v)) as ArrayRef,
                TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(v)) as ArrayRef,
            }
        }
        other => {
            return Err(LakeError::Other(format!(
                "column {}: unsupported type for INSERT VALUES: {other}",
                field.name()
            )))
        }
    };
    Ok(arr)
}

fn bad_num(s: &str, field: &Field) -> LakeError {
    LakeError::Other(format!(
        "column {}: value {s:?} is not a valid {}",
        field.name(),
        field.data_type()
    ))
}

fn bad_lit(l: &Literal, field: &Field) -> LakeError {
    LakeError::Other(format!(
        "column {}: literal {l:?} is not convertible to {}",
        field.name(),
        field.data_type()
    ))
}

// ------------------------------------------------------------ INSERT SELECT

/// SELECT 源结果列按位置对齐 + cast 到表 schema（plan §4.3：不支持指定列清单）。
/// 返回 cast 后的批次与总行数。
pub fn cast_batches_to_table(
    table: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<(Vec<RecordBatch>, usize), LakeError> {
    let want = table.fields().len();
    let mut out = Vec::with_capacity(batches.len());
    let mut rows = 0usize;
    for b in batches {
        if b.num_columns() != want {
            return Err(LakeError::Other(format!(
                "INSERT ... SELECT: SELECT produces {} columns, table has {want} \
                 (column lists are not supported for INSERT ... SELECT)",
                b.num_columns()
            )));
        }
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(want);
        for (i, f) in table.fields().iter().enumerate() {
            let c = b.column(i);
            let c = if c.data_type() == f.data_type() {
                c.clone()
            } else {
                cast(c, f.data_type())
                    .map_err(|e| LakeError::Other(format!("cast column {}: {e}", f.name())))?
            };
            cols.push(c);
        }
        rows += b.num_rows();
        if b.num_rows() > 0 {
            out.push(
                RecordBatch::try_new(table.clone(), cols)
                    .map_err(|e| LakeError::Other(format!("rebuild select batch: {e}")))?,
            );
        }
    }
    Ok((out, rows))
}

// ------------------------------------------------------------ 时间解析（civil，无 chrono）

/// ISO8601 → epoch 纳秒（UTC）。
///
/// 支持格式：`YYYY-MM-DD[('T'|' ')HH:MM[:SS[.frac]]][Z|±HH[:MM]]`；
/// 无时区按 UTC（plan §4.3：ISO8601（UTC）与整型毫秒）。
pub fn parse_iso8601_ns(s: &str) -> Option<i64> {
    let s = s.trim();
    let b = s.as_bytes();
    if b.len() < 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let y: i64 = s.get(0..4)?.parse().ok()?;
    let mo: u32 = s.get(5..7)?.parse().ok()?;
    let d: u32 = s.get(8..10)?.parse().ok()?;
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
        return None;
    }
    let days = days_from_civil_y(y, mo, d);
    if s.len() == 10 {
        return Some(days * 86_400 * 1_000_000_000);
    }
    // 时间分隔符
    if b[10] != b'T' && b[10] != b't' && b[10] != b' ' {
        return None;
    }
    let rest = &s[11..];

    // 尾部时区（Z / ±HH[:MM]）
    let (body, tz_secs) = match rest.find(['Z', 'z', '+', '-']) {
        Some(pos) => {
            let (body, tz) = rest.split_at(pos);
            match tz.as_bytes()[0] {
                b'Z' | b'z' => {
                    if !tz[1..].is_empty() {
                        return None;
                    }
                    (body, 0i64)
                }
                sign @ (b'+' | b'-') => {
                    let mut it = tz[1..].split(':');
                    let th: i64 = it.next()?.parse().ok()?;
                    let tm: i64 = match it.next() {
                        Some(x) => x.parse().ok()?,
                        None => 0,
                    };
                    let signed = if sign == b'+' {
                        th * 3600 + tm * 60
                    } else {
                        -(th * 3600 + tm * 60)
                    };
                    (body, signed)
                }
                _ => return None,
            }
        }
        None => (rest, 0i64),
    };
    if body.is_empty() {
        return None;
    }

    // 小数秒分离
    let (hms, frac) = match body.find('.') {
        Some(pos) => (&body[..pos], Some(&body[pos + 1..])),
        None => (body, None),
    };
    let parts: Vec<&str> = hms.split(':').collect();
    let (h, mi, sec) = match parts.len() {
        2 => {
            let h: i64 = parts[0].parse().ok()?;
            let mi: i64 = parts[1].parse().ok()?;
            (h, mi, 0i64)
        }
        3 => {
            let h: i64 = parts[0].parse().ok()?;
            let mi: i64 = parts[1].parse().ok()?;
            let sec: i64 = parts[2].parse().ok()?;
            (h, mi, sec)
        }
        _ => return None,
    };
    if !(0i64..24).contains(&h) || !(0i64..60).contains(&mi) || sec > 60 {
        return None;
    }
    let nanos: i64 = match frac {
        None => 0,
        Some(f) => {
            if f.is_empty() || f.len() > 9 || !f.bytes().all(|c| c.is_ascii_digit()) {
                return None;
            }
            let mut padded = f.to_string();
            while padded.len() < 9 {
                padded.push('0');
            }
            padded.parse().ok()?
        }
    };
    let secs = days * 86_400 + h * 3600 + mi * 60 + sec - tz_secs;
    Some(secs * 1_000_000_000 + nanos)
}

/// `YYYY-MM-DD` → Date32（自 1970-01-01 的天数）。
pub fn parse_date_days(s: &str) -> Option<i32> {
    let s = s.trim();
    let mut it = s.split(['-', '/']);
    let y: i64 = it.next()?.parse().ok()?;
    let mo: u32 = it.next()?.parse().ok()?;
    let d: u32 = it.next()?.parse().ok()?;
    if it.next().is_some() || !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
        return None;
    }
    i32::try_from(days_from_civil_y(y, mo, d)).ok()
}

/// Howard Hinnant `days_from_civil`（proleptic Gregorian）。
fn days_from_civil_y(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 } as i64;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// 错误信息用 SQL 片段（截断防刷屏）。
pub fn sql_snippet(s: &str) -> String {
    let t = s.trim();
    if t.chars().count() > 80 {
        format!("{}...", t.chars().take(80).collect::<String>())
    } else {
        t.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::TimestampNanosecondArray;
    use sqlparser::ast::{SetExpr, TableObject};

    fn parse(sql: &str) -> Statement {
        parse_single(sql).unwrap()
    }

    fn sch() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("user", DataType::Utf8, true),
            Field::new("cost", DataType::Int32, true),
            Field::new("score", DataType::Float64, true),
            Field::new("ok", DataType::Boolean, true),
        ]))
    }

    /// 从 Insert AST 取 VALUES 行（不 unwrap，错误断言用）。
    #[allow(clippy::type_complexity)]
    fn try_values_rows(
        s: Statement,
    ) -> Result<(Option<Vec<ObjectName>>, Vec<Vec<Expr>>), LakeError> {
        let Statement::Insert(ins) = s else {
            panic!("not insert")
        };
        let cols = if ins.columns.is_empty() {
            None
        } else {
            Some(ins.columns.clone())
        };
        let src = ins
            .source
            .ok_or_else(|| LakeError::Other("no source".into()))?;
        let SetExpr::Values(v) = src.body.as_ref() else {
            return Err(LakeError::Other("not values".into()));
        };
        let rows: Vec<Vec<Expr>> = v.rows.iter().map(|r| r.content.clone()).collect();
        Ok((cols, rows))
    }

    /// 从 Insert AST 取 VALUES 行并构造批次。
    fn extract_values(s: Statement, t: &SchemaRef) -> (RecordBatch, usize) {
        let (cols, rows) = try_values_rows(s).unwrap();
        build_values_batch(t, cols.as_deref(), &rows).unwrap()
    }

    // ---- 解析分流 ----

    #[test]
    fn parse_select_and_insert() {
        assert!(matches!(parse("SELECT 1"), Statement::Query(_)));
        let s = parse("INSERT INTO t (a, b) VALUES (1, 'x'), (2, NULL)");
        let Statement::Insert(ins) = &s else { panic!() };
        assert!(matches!(ins.table, TableObject::TableName(_)));
        assert_eq!(ins.columns.len(), 2);
        let Some(src) = &ins.source else { panic!() };
        let SetExpr::Values(v) = src.body.as_ref() else {
            panic!()
        };
        assert_eq!(v.rows.len(), 2);
    }

    #[test]
    fn parse_show_tables_and_ddl() {
        assert!(matches!(parse("SHOW TABLES"), Statement::ShowTables { .. }));
        assert!(matches!(
            parse("CREATE TABLE t (a BIGINT NOT NULL)"),
            Statement::CreateTable(_)
        ));
        assert!(matches!(
            parse("DROP TABLE IF EXISTS t"),
            Statement::Drop { .. }
        ));
        assert!(parse_single("SELECT 1; SELECT 2").is_err(), "多语句拒绝");
        assert!(parse_single("SELEC 1").is_err(), "语法错误拒绝");
    }

    #[test]
    fn insert_target_ast_and_fallback() {
        assert_eq!(
            insert_target("INSERT INTO audit VALUES (1)"),
            Some("audit".into())
        );
        assert_eq!(
            insert_target("INSERT INTO yuntun.public.audit (a) VALUES (1)"),
            Some("audit".into())
        );
        // 不完整形态（prepared 常见）：AST 失败 → 字符串回落
        assert_eq!(
            insert_target("INSERT INTO audit (event_time, user)"),
            Some("audit".into())
        );
        assert_eq!(insert_target("SELECT 1"), None);
    }

    // ---- CREATE TABLE 映射 ----

    #[test]
    fn create_table_type_mapping() {
        let s = parse(
            "CREATE TABLE IF NOT EXISTS m (a INT NOT NULL, b BIGINT, c VARCHAR(32), \
             d DOUBLE, e BOOLEAN, f TIMESTAMP, g TINYINT, h DATE, i DECIMAL(10,2), j BLOB)",
        );
        let Statement::CreateTable(ct) = &s else {
            panic!()
        };
        let p = parse_create_table(ct).unwrap();
        assert!(p.if_not_exists);
        assert_eq!(p.name, "m");
        let f = p.schema.fields();
        assert_eq!(f[0].data_type(), &DataType::Int32);
        assert!(!f[0].is_nullable(), "NOT NULL");
        assert_eq!(f[1].data_type(), &DataType::Int64);
        assert_eq!(f[2].data_type(), &DataType::Utf8);
        assert_eq!(f[3].data_type(), &DataType::Float64);
        assert_eq!(f[4].data_type(), &DataType::Boolean);
        assert_eq!(
            f[5].data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(f[6].data_type(), &DataType::Int8);
        assert_eq!(f[7].data_type(), &DataType::Date32);
        assert_eq!(f[8].data_type(), &DataType::Decimal128(10, 2));
        assert_eq!(f[9].data_type(), &DataType::Binary);
        assert!(f[1].is_nullable());
    }

    #[test]
    fn create_table_rejects_ctas_and_bad_types() {
        let s = parse("CREATE TABLE t AS SELECT 1");
        let Statement::CreateTable(ct) = &s else {
            panic!()
        };
        assert!(parse_create_table(ct).is_err(), "CTAS 拒绝");

        let s2 = parse("CREATE TABLE t (a ARRAY<INT>)");
        let Statement::CreateTable(ct2) = &s2 else {
            panic!()
        };
        assert!(parse_create_table(ct2).is_err(), "不支持类型拒绝");
    }

    // ---- INSERT VALUES 构造 ----

    #[test]
    fn values_full_column_order() {
        let t = sch();
        let s = parse(
            "INSERT INTO t VALUES (TIMESTAMP '2026-09-09T01:02:03.5Z', 'alice', 10, 1.5, TRUE)",
        );
        let (batch, rows) = extract_values(s, &t);
        assert_eq!(rows, 1);
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        // 2026-09-09T01:02:03.5Z
        let expect_secs = (parse_date_days("2026-09-09").unwrap() as i64) * 86_400 + 3_723;
        assert_eq!(ts.value(0), expect_secs * 1_000_000_000 + 500_000_000);
        assert_eq!(batch.num_columns(), 5);
    }

    #[test]
    fn values_timestamp_accepts_iso_string() {
        // wire 二进制日期参数解码后即 ISO 文本字面量（MySQL prepared datetime 路径）
        let t = sch();
        let s = parse("INSERT INTO t (ts, user) VALUES ('2026-01-02 03:04:05', 'bob')");
        let (batch, rows) = extract_values(s, &t);
        assert_eq!(rows, 1);
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let expect_secs = (parse_date_days("2026-01-02").unwrap() as i64) * 86_400 + 11_045;
        assert_eq!(ts.value(0), expect_secs * 1_000_000_000);
    }

    #[test]
    fn values_column_list_null_fill() {
        let t = sch();
        // 整型字面量 = 毫秒
        let s = parse("INSERT INTO t (ts, user) VALUES (1725840000000, 'bob')");
        let (batch, rows) = extract_values(s, &t);
        assert_eq!(rows, 1);
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_725_840_000_000i64 * 1_000_000);
        // 缺省列（cost/score/ok）填 NULL
        for col in [2usize, 3, 4] {
            assert!(batch.column(col).is_null(0), "缺省列 {col} 应为 NULL");
        }
    }

    #[test]
    fn values_not_null_missing_rejected() {
        let t = sch(); // ts NOT NULL
        let s = parse("INSERT INTO t (user) VALUES ('bob')");
        let (cols, rows) = try_values_rows(s).unwrap();
        assert!(build_values_batch(&t, cols.as_deref(), &rows).is_err());
    }

    #[test]
    fn values_int8_range_check_rejected() {
        let t = Arc::new(Schema::new(vec![Field::new("a", DataType::Int8, true)]));
        let s = parse("INSERT INTO t VALUES (1000)");
        let (cols, rows) = try_values_rows(s).unwrap();
        assert!(
            build_values_batch(&t, cols.as_deref(), &rows).is_err(),
            "Int8 范围检查：1000 超界必须拒绝"
        );
    }

    #[test]
    fn values_expression_rejected() {
        let t = Arc::new(Schema::new(vec![Field::new("a", DataType::Int8, true)]));
        let s = parse("INSERT INTO t VALUES (1 + 2)");
        let (cols, rows) = try_values_rows(s).unwrap();
        assert!(
            build_values_batch(&t, cols.as_deref(), &rows).is_err(),
            "表达式拒绝"
        );
    }

    // ---- INSERT SELECT cast ----

    #[test]
    fn select_cast_positional() {
        let t = sch();
        let src = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Int64, true), // Int64 → Int32
            Field::new("d", DataType::Float32, true),
            Field::new("e", DataType::Boolean, true),
        ]));
        let b = RecordBatch::try_new(
            src,
            vec![
                Arc::new(arrow::array::TimestampMillisecondArray::from(vec![Some(
                    1_000i64,
                )])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef,
                Arc::new(arrow::array::Int64Array::from(vec![7i64])) as ArrayRef,
                Arc::new(Float32Array::from(vec![2.5f32])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true)])) as ArrayRef,
            ],
        )
        .unwrap();
        let (out, rows) = cast_batches_to_table(&t, &[b]).unwrap();
        assert_eq!(rows, 1);
        assert_eq!(out[0].schema(), t);
    }

    #[test]
    fn select_column_mismatch_rejected() {
        let t = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let src = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let b = RecordBatch::try_new(
            src,
            vec![Arc::new(arrow::array::Int64Array::from(vec![1i64])) as ArrayRef],
        )
        .unwrap();
        assert!(cast_batches_to_table(&t, &[b]).is_err());
    }

    // ---- 时间解析 ----

    #[test]
    fn iso8601_parsing() {
        // 2026-01-01T00:00:00Z = 1767225600
        let base = 1_767_225_600i64;
        assert_eq!(
            parse_iso8601_ns("2026-01-01T00:00:00Z"),
            Some(base * 1_000_000_000)
        );
        assert_eq!(
            parse_iso8601_ns("2026-01-01 00:00:00"),
            Some(base * 1_000_000_000)
        );
        assert_eq!(parse_iso8601_ns("2026-01-01"), Some(base * 1_000_000_000));
        assert_eq!(
            parse_iso8601_ns("2026-01-01T00:00:01.5Z"),
            Some(base * 1_000_000_000 + 1_500_000_000)
        );
        assert_eq!(
            parse_iso8601_ns("2026-01-01T08:00:00+08:00"),
            Some(base * 1_000_000_000)
        );
        assert_eq!(parse_iso8601_ns("1970-01-01T00:00:00.000000001Z"), Some(1));
        assert_eq!(parse_iso8601_ns("2026-13-01"), None);
        assert_eq!(parse_iso8601_ns("not-a-time"), None);
    }

    #[test]
    fn date_days_parsing() {
        assert_eq!(parse_date_days("1970-01-01"), Some(0));
        assert_eq!(parse_date_days("1970-01-02"), Some(1));
        assert_eq!(parse_date_days("2026-09-09"), Some(20705));
        assert_eq!(parse_date_days("x"), None);
    }

    #[test]
    fn show_tables_batch_shape() {
        let b = show_tables_batch(&["b".into(), "a".into()]);
        assert_eq!(b.num_rows(), 2);
        let names = b.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(names.value(0), "a", "排序输出");
        assert_eq!(names.value(1), "b");
    }
}
