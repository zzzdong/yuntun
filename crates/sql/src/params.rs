//! 参数绑定（sql-access-design.md §4.3，G3）：
//! wire 协议参数 → SqlValue → **AST 级占位符替换**（非字符串拼接，无注入面），
//! 类型转换复用 INSERT VALUES 的 Literal 中间表示与范围检查。

use sqlparser::ast::Expr;
use sqlparser::ast::Value;
use sqlparser::ast::visit_expressions_mut;
use sqlparser::ast::Statement;
use sqlparser::ast::{DataType as SqlDataType, TimezoneInfo, TypedString, ValueWithSpan};
use std::ops::ControlFlow;
use yuntun_model::error::LakeError;

/// 协议无关的绑定参数值（各 wire 协议的参数解码目标）。
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
    /// 时间戳（epoch 纳秒，UTC）
    TsNs(i64),
    /// 日期（自 1970-01-01 天数）
    Date(i32),
}

/// 统计语句中的占位符个数（`?` / `$n`）。
pub fn count_placeholders(stmt: &mut Statement) -> usize {
    let mut n = 0usize;
    let _ = visit_expressions_mut(stmt, |expr: &mut Expr| {
        if is_placeholder(expr) {
            n += 1;
        }
        ControlFlow::<()>::Continue(())
    });
    n
}

/// AST 级参数替换：按出现顺序把 `?`/`$n` 占位符替换为参数字面量 AST。
/// 参数个数不匹配 → Err（ControlFlow 提前终止遍历）。
pub fn substitute(stmt: &mut Statement, params: &[SqlValue]) -> Result<(), LakeError> {
    let mut idx = 0usize;
    let flow = visit_expressions_mut(stmt, |expr: &mut Expr| {
        if is_placeholder(expr) {
            let Some(p) = params.get(idx) else {
                return ControlFlow::Break(LakeError::Other(format!(
                    "prepared statement expects at least {} parameter(s), got {}",
                    idx + 1,
                    params.len()
                )));
            };
            *expr = param_to_expr(p);
            idx += 1;
        }
        ControlFlow::<LakeError>::Continue(())
    });
    if let ControlFlow::Break(e) = flow {
        return Err(e);
    }
    if idx < params.len() {
        return Err(LakeError::Other(format!(
            "prepared statement has {idx} placeholder(s), got {} parameter(s)",
            params.len()
        )));
    }
    Ok(())
}

fn is_placeholder(expr: &Expr) -> bool {
    matches!(expr, Expr::Value(v) if matches!(v.value, Value::Placeholder(_)))
}

/// SqlValue → SQL 字面量 AST（渲染由 sqlparser Display 完成，转义安全）：
/// - 时间戳 → `TIMESTAMP '...'`（TypedString），走 INSERT VALUES 的 TimestampNs 路径；
/// - 日期 → `DATE '...'`（TypedString）；
/// - 字符串 → 单引号字面量（Display 自动转义内嵌单引号）。
fn param_to_expr(p: &SqlValue) -> Expr {
    let typed = |ty: SqlDataType, s: String| {
        Expr::TypedString(TypedString {
            data_type: ty,
            value: ValueWithSpan {
                value: Value::SingleQuotedString(s),
                span: sqlparser::tokenizer::Span::empty(),
            },
            uses_odbc_syntax: false,
        })
    };
    match p {
        SqlValue::Null => Expr::Value(ValueWithSpan {
            value: Value::Null,
            span: sqlparser::tokenizer::Span::empty(),
        }),
        SqlValue::Bool(b) => Expr::Value(ValueWithSpan {
            value: Value::Boolean(*b),
            span: sqlparser::tokenizer::Span::empty(),
        }),
        SqlValue::Int(i) => Expr::Value(ValueWithSpan {
            value: Value::Number(i.to_string(), false),
            span: sqlparser::tokenizer::Span::empty(),
        }),
        SqlValue::UInt(u) => Expr::Value(ValueWithSpan {
            value: Value::Number(u.to_string(), false),
            span: sqlparser::tokenizer::Span::empty(),
        }),
        SqlValue::Float(f) => Expr::Value(ValueWithSpan {
            value: Value::Number(format_f64(*f), false),
            span: sqlparser::tokenizer::Span::empty(),
        }),
        SqlValue::Str(s) => Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s.clone()),
            span: sqlparser::tokenizer::Span::empty(),
        }),
        SqlValue::Bytes(b) => Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(b.iter().map(|x| format!("{x:02X}")).collect()),
            span: sqlparser::tokenizer::Span::empty(),
        }),
        SqlValue::TsNs(ns) => typed(
            SqlDataType::Timestamp(None, TimezoneInfo::None),
            format_ts_ns(*ns),
        ),
        SqlValue::Date(days) => typed(
            SqlDataType::Date,
            format_date_days(*days),
        ),
    }
}

/// f64 最短往返表示（字面量渲染 / 文本编码共用语义）。
pub fn format_f64(f: f64) -> String {
    if f == f.trunc() && f.abs() < 1e15 {
        format!("{f:.1}")
    } else {
        format!("{f}")
    }
}

/// epoch 纳秒 → `YYYY-MM-DD HH:MM:SS[.fff]`（UTC，civil 算法，无 chrono）。
pub fn format_ts_ns(ns: i64) -> String {
    let secs = ns.div_euclid(1_000_000_000);
    let nanos = ns.rem_euclid(1_000_000_000);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (y, mo, d) = civil_from_days(days);
    let base = format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}");
    if nanos == 0 {
        base
    } else {
        let mut frac = format!("{nanos:09}");
        while frac.ends_with('0') {
            frac.pop();
        }
        format!("{base}.{frac}")
    }
}

/// 自 1970-01-01 天数 → `YYYY-MM-DD`。
pub fn format_date_days(days: i32) -> String {
    let (y, mo, d) = civil_from_days(days as i64);
    format!("{y:04}-{mo:02}-{d:02}")
}

/// Howard Hinnant `civil_from_days`（proleptic Gregorian）。
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;

    fn parse_mysql(sql: &str) -> Statement {
        Parser::parse_sql(&MySqlDialect {}, sql)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    }

    #[test]
    fn count_and_substitute() {
        let mut stmt = parse_mysql("INSERT INTO t VALUES (?, ?, ?)");
        assert_eq!(count_placeholders(&mut stmt), 3);
        substitute(
            &mut stmt,
            &[
                SqlValue::Int(7),
                SqlValue::Str("a'b".into()),
                SqlValue::Null,
            ],
        )
        .unwrap();
        assert_eq!(count_placeholders(&mut stmt), 0);
        assert_eq!(
            stmt.to_string(),
            "INSERT INTO t VALUES (7, 'a''b', NULL)"
        );
    }

    #[test]
    fn substitute_timestamp_and_date() {
        let mut stmt = parse_mysql("INSERT INTO t VALUES (?, ?)");
        substitute(
            &mut stmt,
            &[
                SqlValue::TsNs(1_767_225_600_000_000_000),
                SqlValue::Date(19_000),
            ],
        )
        .unwrap();
        assert_eq!(
            stmt.to_string(),
            "INSERT INTO t VALUES (TIMESTAMP '2026-01-01 00:00:00', DATE '2022-01-08')"
        );
    }

    #[test]
    fn param_count_mismatch_rejected() {
        let mut stmt = parse_mysql("INSERT INTO t VALUES (?, ?)");
        assert!(substitute(&mut stmt, &[SqlValue::Int(1)]).is_err());
        let mut stmt2 = parse_mysql("INSERT INTO t VALUES (1)");
        assert!(substitute(&mut stmt2, &[SqlValue::Int(1)]).is_err());
    }

    #[test]
    fn float_rendering() {
        assert_eq!(format_f64(3.0), "3.0");
        assert_eq!(format_f64(123.456), "123.456");
    }

    #[test]
    fn ts_formatting() {
        assert_eq!(format_ts_ns(0), "1970-01-01 00:00:00");
        assert_eq!(
            format_ts_ns(1_767_225_600_000_000_000),
            "2026-01-01 00:00:00"
        );
        assert_eq!(format_date_days(0), "1970-01-01");
    }
}
