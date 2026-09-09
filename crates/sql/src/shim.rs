//! 方言 shim（sql-access-design.md §4.4，G5）：
//! wire 客户端握手 / 元数据探测期的"非数据语句"在分流前拦截——
//! canned 响应或转调 SqlEngine 元数据 API。
//!
//! 仅对 `SqlDialect::MySql` 会话生效（Flight 简易轨 Generic 方言保持既有行为）。
//! shim 不直接查 catalog —— 表/列数据一律走 [`crate::SqlEngine`] 元数据 API（S-2 边界）。

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use sqlparser::ast::{Expr, Statement, Value};

use crate::session::{SessionCtx, SqlDialect};
use crate::sql::table_name_of;
use crate::{SqlEngine, SqlError, SqlResult};

/// canned 系统变量（MySQL 客户端握手/元数据探测高频项）。
fn canned_variable(name: &str) -> &'static str {
    match name {
        "version" => "8.0.32-yuntun",
        "version_comment" => "yuntun",
        "autocommit" => "1",
        "max_allowed_packet" => "67108864",
        "sql_mode" => "",
        "character_set_client" | "character_set_results" | "character_set_connection" => {
            "utf8mb4"
        }
        "collation_connection" | "collation_server" => "utf8mb4_general_ci",
        "lower_case_table_names" => "0",
        "tx_isolation" | "transaction_isolation" => "READ-COMMITTED",
        "sql_select_limit" => "18446744073709551615",
        "init_connect" => "",
        "license" => "Apache-2.0",
        _ => "",
    }
}

pub(crate) async fn intercept(
    stmt: &Statement,
    session: &mut SessionCtx,
    engine: &SqlEngine,
) -> Result<Option<SqlResult>, SqlError> {
    if session.dialect != SqlDialect::MySql {
        return Ok(None);
    }
    Ok(match stmt {
        // USE db：记录会话（R-3：MVP 单 schema，校验后记录）
        Statement::Use(u) => {
            let db = match u {
                sqlparser::ast::Use::Catalog(n)
                | sqlparser::ast::Use::Schema(n)
                | sqlparser::ast::Use::Database(n)
                | sqlparser::ast::Use::Object(n) => table_name_of(n),
                _ => String::new(),
            };
            if !db.is_empty() && db != "public" && db != "yuntun" {
                return Err(SqlError::Internal(format!(
                    "unknown database {db:?} (yuntun exposes a single schema: yuntun.public)"
                )));
            }
            session.default_db = Some("public".into());
            Some(SqlResult::Affected(0))
        }
        // 事务 no-op（S-6：单语句自动提交语义；含 ROLLBACK——CLI 友好，偏差留档 operation-log）
        Statement::StartTransaction { .. } | Statement::Commit { .. } | Statement::Rollback { .. } => {
            Some(SqlResult::Affected(0))
        }
        // SET ...：no-op
        Statement::Set(_) => Some(SqlResult::Affected(0)),
        Statement::ShowVariable { variable } => {
            let name = variable
                .last()
                .map(|i| i.value.clone())
                .unwrap_or_default();
            strings_result(
                &["Variable_name", "Value"],
                &[vec![name.clone(), canned_variable(&name).to_string()]],
            )
        }
        Statement::ShowVariables { .. } => {
            let vars: &[&str] = &[
                "version",
                "version_comment",
                "autocommit",
                "max_allowed_packet",
                "sql_mode",
                "character_set_client",
                "character_set_results",
                "character_set_connection",
                "collation_connection",
                "lower_case_table_names",
                "transaction_isolation",
                "sql_select_limit",
            ];
            let rows: Vec<Vec<String>> = vars
                .iter()
                .map(|v| vec![v.to_string(), canned_variable(v).to_string()])
                .collect();
            strings_result(&["Variable_name", "Value"], &rows)
        }
        Statement::ShowDatabases { .. } => strings_result(&["Database"], &[vec!["public".into()]]),
        // SHOW TABLES：MySQL 语义单列（Tables_in_<db>）
        Statement::ShowTables { .. } => {
            let names = engine.list_tables().await?;
            let rows: Vec<Vec<String>> = names.into_iter().map(|n| vec![n]).collect();
            let col =
                format!("Tables_in_{}", session.default_db.as_deref().unwrap_or("public"));
            strings_result(&[col.as_str()], &rows)
        }
        Statement::ShowColumns { show_options, .. } => {
            let table = table_from_options(show_options)
                .ok_or_else(|| SqlError::Internal("SHOW COLUMNS requires a table".into()))?;
            match engine.describe_table(&table).await? {
                Some(desc) => {
                    let rows: Vec<Vec<String>> = desc
                        .columns
                        .iter()
                        .map(|c| {
                            vec![
                                c.name.clone(),
                                c.mysql_type.clone(),
                                if c.nullable { "YES".into() } else { "NO".into() },
                                String::new(),
                                String::new(),
                                String::new(),
                            ]
                        })
                        .collect();
                    strings_result(
                        &["Field", "Type", "Null", "Key", "Default", "Extra"],
                        &rows,
                    )
                }
                None => return Err(SqlError::NotFound(table)),
            }
        }
        Statement::ShowCreate { obj_name, .. } => {
            let table = table_name_of(obj_name);
            match engine.describe_table(&table).await? {
                Some(desc) => {
                    let cols: Vec<String> = desc
                        .columns
                        .iter()
                        .map(|c| {
                            format!(
                                "  `{}` {}{}",
                                c.name,
                                c.mysql_type,
                                if c.nullable { "" } else { " NOT NULL" }
                            )
                        })
                        .collect();
                    let ddl = format!(
                        "CREATE TABLE `{}` (\n{}\n) ENGINE=YUNTUN",
                        desc.name,
                        cols.join(",\n")
                    );
                    strings_result(&["Table", "Create Table"], &[vec![desc.name.clone(), ddl]])
                }
                None => return Err(SqlError::NotFound(table)),
            }
        }
        // SELECT @@var / SELECT DATABASE()：无 FROM 的探测查询 → canned
        Statement::Query(q) => {
            let has_from = match q.body.as_ref() {
                sqlparser::ast::SetExpr::Select(s) => !s.from.is_empty(),
                _ => false,
            };
            if has_from {
                None
            } else {
                canned_probe(q).await?
            }
        }
        _ => None,
    })
}

fn table_from_options(opts: &sqlparser::ast::ShowStatementOptions) -> Option<String> {
    opts.show_in
        .as_ref()
        .and_then(|i| i.parent_name.as_ref())
        .map(table_name_of)
}

/// 无 FROM 的探测查询：全部投影为 @@var 或 DATABASE()/SCHEMA() 时返回 canned 行。
async fn canned_probe(q: &sqlparser::ast::Query) -> Result<Option<SqlResult>, SqlError> {
    let sel = match q.body.as_ref() {
        sqlparser::ast::SetExpr::Select(sel) => sel,
        _ => return Ok(None),
    };
    let mut names: Vec<String> = Vec::new();
    let mut values: Vec<String> = Vec::new();
    for item in sel.projection.iter() {
        let sqlparser::ast::SelectItem::UnnamedExpr(expr) = item else {
            return Ok(None);
        };
        match classify_probe_expr(expr) {
            ProbeKind::Variable(name) => {
                names.push(name.clone());
                values.push(canned_variable(&name).to_string());
            }
            ProbeKind::Database(name) => {
                names.push(name);
                values.push("public".into());
            }
            ProbeKind::Other => return Ok(None), // 混合真实列 → 交分流/DataFusion
        }
    }
    let cols: Vec<&str> = names.iter().map(String::as_str).collect();
    Ok(strings_result(&cols, &[values]))
}

enum ProbeKind {
    /// `@@var`（name 含 @@ 前缀）
    Variable(String),
    /// DATABASE() / SCHEMA()
    Database(String),
    Other,
}

fn classify_probe_expr(expr: &Expr) -> ProbeKind {
    match expr {
        Expr::Value(v) => match &v.value {
            Value::Placeholder(name) if name.starts_with("@@") => ProbeKind::Variable(name.clone()),
            _ => ProbeKind::Other,
        },
        Expr::Function(f) => {
            let name = f.name.to_string().to_ascii_lowercase();
            if name == "database" || name == "schema" {
                ProbeKind::Database(name)
            } else {
                ProbeKind::Other
            }
        }
        _ => ProbeKind::Other,
    }
}

/// 单一字符串列批次。
fn strings_result(col_names: &[&str], rows: &[Vec<String>]) -> Option<SqlResult> {
    let schema: SchemaRef = Arc::new(Schema::new(
        col_names
            .iter()
            .map(|n| Field::new((*n).to_string(), DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ));
    let n_cols = col_names.len();
    let cols: Vec<ArrayRef> = (0..n_cols)
        .map(|c| {
            Arc::new(StringArray::from(
                rows.iter().map(|r| r[c].clone()).collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect();
    let batch = RecordBatch::try_new(schema, cols).expect("canned batch");
    Some(SqlResult::Rows {
        schema: batch.schema(),
        batches: vec![batch],
    })
}

/// 最小 LIKE 支持：仅 `%` 通配（前缀/后缀/包含）。
#[allow(dead_code)]
fn simple_like(pattern: &str, value: &str) -> bool {
    let parts: Vec<&str> = pattern.split('%').collect();
    let (Some(first), Some(last)) = (parts.first(), parts.last()) else {
        return false;
    };
    let mut rest = value;
    if !first.is_empty() {
        match rest.strip_prefix(first) {
            Some(r) => rest = r,
            None => return false,
        }
    }
    if !last.is_empty() {
        match rest.strip_suffix(last) {
            Some(r) => rest = r,
            None => return false,
        }
    }
    for mid in &parts[1..parts.len() - 1] {
        if mid.is_empty() {
            continue;
        }
        match rest.find(mid) {
            Some(pos) => rest = &rest[pos + mid.len()..],
            None => return false,
        }
    }
    true
}
