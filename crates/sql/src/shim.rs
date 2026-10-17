//! 方言 shim（sql-access-design.md §4.4，G5）：
//! wire 客户端握手 / 元数据探测期的"非数据语句"在分流前拦截——
//! canned 响应或转调 SqlEngine 元数据 API。
//!
//! 仅对 `SqlDialect::MySql` 会话生效（Flight 简易轨 Generic 方言保持既有行为）。
//! shim 不直接查 catalog —— 表/列数据一律走 [`crate::SqlEngine`] 元数据 API（S-2 边界）。

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use sqlparser::ast::{Expr, Statement, Value};

use crate::session::{SessionCtx, SqlDialect};
use crate::sql::table_name_of;
use crate::{SqlEngine, SqlError, SqlResult};

/// 对客户端自报的 MySQL 版本串（**单一事实源**）。
///
/// 两处消费方必须一致，否则客户端会出现"握手一个版本、查询另一个版本"的怪象：
/// - MySQL wire 握手（[`yuntun_sqlwire::MysqlBackend::version`]）；
/// - `SELECT @@version` / `SHOW VARIABLES LIKE 'version'`（本模块 canned 响应）。
///
/// 声明 8.0.x 而非 5.x 还会影响驱动的能力分支：部分 ORM（如 ActiveRecord）按
/// 版本降级功能；5.1.10 会让现代驱动走老路径。
pub const MYSQL_VERSION: &str = "8.0.32-yuntun";

/// 系统变量的**当前值**：动态项（`max_connections`）走注册表，其余走 canned 表。
///
/// 为什么单独一层：`max_connections` 是唯一"由配置决定、且必须与实际准入一致"的变量。
/// 让它和别的变量共用一个 `&'static str` 表，就等于允许"变量说 151、闸门按 20 拦"。
fn variable_value(name: &str, engine: &SqlEngine) -> String {
    match name {
        "max_connections" => engine.registry().max_connections().to_string(),
        _ => canned_variable(name).to_string(),
    }
}

/// MySQL `LIKE` 的核心子集：`%`（任意串）、`_`（单字符），其余按字面比较，大小写不敏感。
///
/// 只做到"够客户端按变量名筛"的程度：不实现转义符 —— 变量名里没有 `%`/`_`，
/// 而写错模式时"多返回"比"漏返回"更难造成误判（这里刻意不做正则/全语法）。
fn like_match(value: &str, pattern: &str) -> bool {
    fn go(v: &[char], p: &[char]) -> bool {
        match p.first() {
            None => v.is_empty(),
            Some('%') => go(v, &p[1..]) || (!v.is_empty() && go(&v[1..], p)),
            Some('_') => !v.is_empty() && go(&v[1..], &p[1..]),
            Some(c) => v.first() == Some(c) && go(&v[1..], &p[1..]),
        }
    }
    let v: Vec<char> = value.to_lowercase().chars().collect();
    let p: Vec<char> = pattern.to_lowercase().chars().collect();
    go(&v, &p)
}

/// `SHOW ... LIKE` 的过滤。`WHERE` 表达式**不解释** ⇒ 返回全部：
/// "少返回一行"会让客户端以为那个变量不存在（静默），而多返回它自己会筛掉。
fn filter_rows(
    rows: Vec<Vec<String>>,
    filter: Option<&sqlparser::ast::ShowStatementFilter>,
) -> Vec<Vec<String>> {
    use sqlparser::ast::ShowStatementFilter as F;
    match filter {
        Some(F::Like(pat)) | Some(F::ILike(pat)) => rows
            .into_iter()
            .filter(|r| r.first().is_some_and(|n| like_match(n, pat)))
            .collect(),
        _ => rows,
    }
}

/// canned 系统变量（MySQL 客户端握手/元数据探测高频项）。
///
/// 未收录项返回 `""`（与 MySQL 未知变量行为不同，但对驱动更友好：
/// 驱动多只做字符串读取）。JDBC/DBeaver 握手必需的项已在此列全。
///
/// `max_connections` **不在这里**：它由 [`variable_value`] 从注册表读（唯一事实源）。
fn canned_variable(name: &str) -> &'static str {
    match name {
        "version" => MYSQL_VERSION,
        "version_comment" => "yuntun",
        "autocommit" => "1",
        "max_allowed_packet" => "67108864",
        "sql_mode" => "",
        "character_set_client" | "character_set_results" | "character_set_connection" => "utf8mb4",
        "collation_connection" | "collation_server" => "utf8mb4_general_ci",
        "lower_case_table_names" => "0",
        "tx_isolation" | "transaction_isolation" => "READ-COMMITTED",
        "sql_select_limit" => "18446744073709551615",
        "init_connect" => "",
        "license" => "Apache-2.0",
        // ---- JDBC（Connector/J）/ DBeaver 握手探测项 ----
        "auto_increment_increment" | "auto_increment_offset" => "1",
        "character_set_server" => "utf8mb4",
        "system_time_zone" => "UTC",
        "time_zone" => "SYSTEM",
        "wait_timeout" | "interactive_timeout" => "28800",
        "net_read_timeout" | "net_write_timeout" => "60",
        "query_cache_size" | "query_cache_type" => "0",
        "transaction_read_only" => "0",
        "sql_auto_is_null" => "0",
        "have_ssl" | "have_openssl" => "DISABLED",
        "version_compile_os" => "linux",
        "version_compile_machine" => "x86_64",
        "port" => "3306",
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
        // USE db：**多 schema 真实切换**（校验存在；未知库 → ER_BAD_DB_ERROR 语义）
        Statement::Use(u) => {
            let db = match u {
                sqlparser::ast::Use::Catalog(n)
                | sqlparser::ast::Use::Schema(n)
                | sqlparser::ast::Use::Database(n)
                | sqlparser::ast::Use::Object(n) => table_name_of(n),
                _ => String::new(),
            };
            if !db.is_empty() && db != yuntun_query::CATALOG_NAME {
                if engine.schema_exists(&db).await? {
                    session.set_schema(&db);
                } else {
                    return Err(SqlError::SchemaNotFound(db));
                }
            }
            Some(SqlResult::Affected(0))
        }
        // 事务 no-op（S-6：单语句自动提交语义；含 ROLLBACK——CLI 友好，偏差留档 operation-log）
        Statement::StartTransaction { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. } => Some(SqlResult::Affected(0)),
        // SET ...：no-op
        Statement::Set(_) => Some(SqlResult::Affected(0)),
        Statement::ShowVariable { variable } => {
            let name = variable.last().map(|i| i.value.clone()).unwrap_or_default();
            // sqlparser 0.62 没有 SHOW KEYS / INDEX / ENGINES 的独立变体——
            // 它们落到 ShowVariable，且 `SHOW KEYS FROM t` 的表名也被收进
            // variable（["KEYS", "FROM", "t"]）→ 以**首段**判定。
            // 列名必须真实存在：JDBC 的 getPrimaryKeys / getIndexInfo 按列名取值，
            // 缺失即抛 "Column 'Key_name' not found"。
            let head = variable
                .first()
                .map(|i| i.value.to_ascii_uppercase())
                .unwrap_or_default();
            match head.as_str() {
                // 无索引/主键概念 → 空结果集（列名齐全即语义正确："该表无主键/索引"）
                "KEYS" | "INDEX" | "INDEXES" => strings_result(SHOW_KEYS_COLUMNS, &[]),
                "ENGINES" => strings_result(
                    SHOW_ENGINES_COLUMNS,
                    &[vec![
                        "YUNTUN".into(),
                        "DEFAULT".into(),
                        "yuntun lakehouse storage engine".into(),
                        "NO".into(),
                        "NO".into(),
                        "NO".into(),
                    ]],
                ),
                _ => strings_result(
                    &["Variable_name", "Value"],
                    &[vec![name.clone(), canned_variable(&name).to_string()]],
                ),
            }
        }
        Statement::ShowVariables { filter, .. } => {
            let vars: &[&str] = &[
                "version",
                "version_comment",
                "autocommit",
                "max_allowed_packet",
                "max_connections",
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
                .map(|v| vec![(*v).to_string(), variable_value(v, engine)])
                .collect();
            // `SHOW VARIABLES LIKE 'max_connections'`：**过滤在服务端做** —— 客户端
            // （Connector/J）拿到 13 行再自己筛也行，但 `LIKE` 一个不存在的变量时要
            // 返回空集，而不是"全部变量"（后者会让"读不到"伪装成"读到了"）。
            strings_result(
                &["Variable_name", "Value"],
                &filter_rows(rows, filter.as_ref()),
            )
        }
        // `SHOW [GLOBAL|SESSION] STATUS`：**真实计数器**（`§192`）。
        //
        // GLOBAL 与 SESSION 共用一份：本实现没有"会话级计数器"，与其编造两张表，
        // 不如让两者答同一个数（并在文档里写明这个偏差）。
        Statement::ShowStatus { filter, .. } => {
            let s = engine.registry().status();
            let rows = vec![
                vec!["Uptime".to_string(), s.uptime_secs.to_string()],
                vec![
                    "Threads_connected".to_string(),
                    s.threads_connected.to_string(),
                ],
                vec!["Threads_running".to_string(), s.threads_running.to_string()],
                vec!["Connections".to_string(), s.connections_total.to_string()],
                vec![
                    "Max_used_connections".to_string(),
                    s.max_used_connections.to_string(),
                ],
                vec![
                    "Connection_errors_max_connections".to_string(),
                    s.connection_errors_max_connections.to_string(),
                ],
                vec!["Questions".to_string(), s.questions_total.to_string()],
                vec!["Max_connections".to_string(), s.max_connections.to_string()],
            ];
            strings_result(
                &["Variable_name", "Value"],
                &filter_rows(rows, filter.as_ref()),
            )
        }
        // SHOW DATABASES：catalog 中的 schema 清单（多 schema）
        Statement::ShowDatabases { .. } => {
            let rows: Vec<Vec<String>> = engine
                .list_schemas()
                .await?
                .into_iter()
                .map(|s| vec![s])
                .collect();
            strings_result(&["Database"], &rows)
        }
        // SHOW TABLES：MySQL 语义（Tables_in_<db>）——**限定库**的表
        // （`D-32`：`SHOW TABLES FROM/IN <db>` 曾忽略限定符，答成当前库的表）
        Statement::ShowTables {
            full, show_options, ..
        } => {
            let schema = schema_from_options(show_options, session);
            let names = engine.list_tables_in(&schema).await?;
            if *full {
                // SHOW FULL TABLES = 两列（MySQL 同款）：第二列 Table_type。
                // 无视图 ⇒ 恒 "BASE TABLE"；只给一列会让按列名取值的客户端少一列。
                let rows: Vec<Vec<String>> = names
                    .into_iter()
                    .map(|n| vec![n, "BASE TABLE".to_string()])
                    .collect();
                strings_result(&[&format!("Tables_in_{schema}"), "Table_type"], &rows)
            } else {
                let rows: Vec<Vec<String>> = names.into_iter().map(|n| vec![n]).collect();
                strings_result(&[&format!("Tables_in_{schema}")], &rows)
            }
        }
        Statement::ShowColumns {
            show_options,
            full,
            extended,
        } => {
            let table = table_from_options(show_options, session)
                .ok_or_else(|| SqlError::Internal("SHOW COLUMNS requires a table".into()))?;
            let desc = engine
                .describe_table(&table)
                .await?
                .ok_or(SqlError::NotFound(table))?;
            // SHOW FULL COLUMNS = 9 列（多 Collation / Privileges / Comment）
            // ——Connector/J 的 `DatabaseMetaData::getColumns` 按名取这三列，缺失即抛异常
            if *full || *extended {
                strings_result(SHOW_FULL_COLUMNS, &show_columns_rows(&desc, true))
            } else {
                strings_result(SHOW_COLUMNS, &show_columns_rows(&desc, false))
            }
        }
        // DESCRIBE / DESC t（MySQL CLI 与 DBeaver 高频）：语义 = SHOW COLUMNS
        // （EXPLAIN 同理落此变体，MVP 不做执行计划，留档）
        Statement::ExplainTable { table_name, .. } => {
            let table = crate::sql::resolve_table_ref(table_name, session);
            let desc = engine
                .describe_table(&table)
                .await?
                .ok_or(SqlError::NotFound(table))?;
            strings_result(SHOW_COLUMNS, &show_columns_rows(&desc, false))
        }
        Statement::ShowCollation { .. } => strings_result(
            SHOW_COLLATION_COLUMNS,
            &[vec![
                "utf8mb4_general_ci".into(),
                "utf8mb4".into(),
                "45".into(),
                "Yes".into(),
                "Yes".into(),
                "1".into(),
            ]],
        ),
        Statement::ShowCharset(_) => strings_result(
            SHOW_CHARSET_COLUMNS,
            &[vec![
                "utf8mb4".into(),
                "UTF-8 Unicode".into(),
                "utf8mb4_general_ci".into(),
                "4".into(),
            ]],
        ),
        Statement::ShowCreate { obj_name, .. } => {
            let table = crate::sql::resolve_table_ref(obj_name, session);
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
        // **查询管理面**（U4 / `§191`）：看到谁在跑什么。
        //
        // `SHOW FULL PROCESSLIST` 与不带 FULL 在这里是同一份事实（MySQL 的 FULL 只是
        // 不截断 `Info`；本实现从不截断），所以两个变体共用一个分支。
        Statement::ShowProcessList { .. } => Some(processlist_result(engine.registry().snapshot())),
        // `KILL [CONNECTION|QUERY] <id>`：点取消位；协议层负责"取消之后断不断连接"。
        // 未知 Id ⇒ 1094（`UnknownThread`），**不静默成功** —— 脚本靠这个报错发现打错了 Id。
        Statement::Kill { modifier, id } => {
            // MySQL 语义：**裸 `KILL <id>` = KILL CONNECTION**（不是 KILL QUERY）。
            // 写成 `matches!(modifier, Some(Connection))` 会让 `KILL 7` 退化成"只取消
            // 当前查询"，客户端以为连接已经断开 —— 静默的语义偏差。
            let connection = !matches!(
                modifier,
                Some(sqlparser::ast::KillType::Query) | Some(sqlparser::ast::KillType::Mutation)
            );
            match engine.registry().kill(*id, connection) {
                Ok(()) => Some(SqlResult::Affected(0)),
                Err(id) => return Err(SqlError::UnknownThread(id)),
            }
        }
        // SELECT @@var / SELECT DATABASE()：无 FROM 的探测查询 → canned
        Statement::Query(q) => {
            let sel = match q.body.as_ref() {
                sqlparser::ast::SetExpr::Select(s) => s,
                _ => return Ok(None),
            };
            if sel.from.is_empty() {
                return canned_probe(q, session, engine).await;
            }
            // information_schema 补洞：DataFusion 只提供 tables / views / columns /
            // schemata / routines / df_settings 等少数几张，而 MySQL 客户端（DBeaver）
            // 还会查 key_column_usage / statistics / triggers ... —— 这些表不存在会以
            // 1146 弹错。按 MySQL 定义返回**列名齐全的空结果集**（= 无约束/无索引/无触发器）。
            match missing_information_schema(&sel.from) {
                Some(cols) => strings_result(cols, &[]),
                None => None,
            }
        }
        _ => None,
    })
}

// ---- MySQL 元数据语句的列定义（列名必须与 MySQL 一致：驱动按**列名**取值）----

/// `SHOW COLUMNS` / `DESCRIBE` / `DESC`
const SHOW_COLUMNS: &[&str] = &["Field", "Type", "Null", "Key", "Default", "Extra"];
/// `SHOW FULL COLUMNS`（多 Collation / Privileges / Comment）
const SHOW_FULL_COLUMNS: &[&str] = &[
    "Field",
    "Type",
    "Collation",
    "Null",
    "Key",
    "Default",
    "Extra",
    "Privileges",
    "Comment",
];
/// `SHOW KEYS` / `SHOW INDEX`（空结果集 = 该表无索引/主键）
const SHOW_KEYS_COLUMNS: &[&str] = &[
    "Table",
    "Non_unique",
    "Key_name",
    "Seq_in_index",
    "Column_name",
    "Collation",
    "Cardinality",
    "Sub_part",
    "Packed",
    "Null",
    "Index_type",
    "Comment",
    "Index_comment",
    "Visible",
    "Expression",
];
/// `SHOW ENGINES`
const SHOW_ENGINES_COLUMNS: &[&str] = &[
    "Engine",
    "Support",
    "Comment",
    "Transactions",
    "XA",
    "Savepoints",
];
/// `SHOW COLLATION`
const SHOW_COLLATION_COLUMNS: &[&str] = &[
    "Collation",
    "Charset",
    "Id",
    "Default",
    "Compiled",
    "Sortlen",
];
/// `SHOW CHARSET` / `SHOW CHARACTER SET`
const SHOW_CHARSET_COLUMNS: &[&str] = &["Charset", "Description", "Default collation", "Maxlen"];

/// DataFusion `information_schema` **缺失**而 MySQL 客户端会查的表 → 列名（MySQL 8 定义）。
///
/// 全部返回 0 行：yuntun 无主键/外键/索引/触发器/分区概念，空集即正确语义；
/// 关键是**列名要齐**——DBeaver / JDBC 按列名取值，缺列会抛 "Column not found"。
const IS_EMPTY_TABLES: &[(&str, &[&str])] = &[
    (
        "key_column_usage",
        &[
            "CONSTRAINT_CATALOG",
            "CONSTRAINT_SCHEMA",
            "CONSTRAINT_NAME",
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "ORDINAL_POSITION",
            "POSITION_IN_UNIQUE_CONSTRAINT",
            "REFERENCED_TABLE_SCHEMA",
            "REFERENCED_TABLE_NAME",
            "REFERENCED_COLUMN_NAME",
        ],
    ),
    (
        "referential_constraints",
        &[
            "CONSTRAINT_CATALOG",
            "CONSTRAINT_SCHEMA",
            "CONSTRAINT_NAME",
            "UNIQUE_CONSTRAINT_CATALOG",
            "UNIQUE_CONSTRAINT_SCHEMA",
            "UNIQUE_CONSTRAINT_NAME",
            "MATCH_OPTION",
            "UPDATE_RULE",
            "DELETE_RULE",
            "TABLE_NAME",
            "REFERENCED_TABLE_NAME",
        ],
    ),
    (
        "table_constraints",
        &[
            "CONSTRAINT_CATALOG",
            "CONSTRAINT_SCHEMA",
            "CONSTRAINT_NAME",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "CONSTRAINT_TYPE",
            "ENFORCED",
        ],
    ),
    (
        "check_constraints",
        &[
            "CONSTRAINT_CATALOG",
            "CONSTRAINT_SCHEMA",
            "CONSTRAINT_NAME",
            "CHECK_CLAUSE",
        ],
    ),
    (
        "statistics",
        &[
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "NON_UNIQUE",
            "INDEX_SCHEMA",
            "INDEX_NAME",
            "SEQ_IN_INDEX",
            "COLUMN_NAME",
            "COLLATION",
            "CARDINALITY",
            "SUB_PART",
            "PACKED",
            "NULLABLE",
            "INDEX_TYPE",
            "COMMENT",
            "INDEX_COMMENT",
            "IS_VISIBLE",
            "EXPRESSION",
        ],
    ),
    (
        "triggers",
        &[
            "TRIGGER_CATALOG",
            "TRIGGER_SCHEMA",
            "TRIGGER_NAME",
            "EVENT_MANIPULATION",
            "EVENT_OBJECT_CATALOG",
            "EVENT_OBJECT_SCHEMA",
            "EVENT_OBJECT_TABLE",
            "ACTION_ORDER",
            "ACTION_CONDITION",
            "ACTION_STATEMENT",
            "ACTION_ORIENTATION",
            "ACTION_TIMING",
            "CREATED",
            "SQL_MODE",
            "DEFINER",
            "CHARACTER_SET_CLIENT",
            "COLLATION_CONNECTION",
            "DATABASE_COLLATION",
        ],
    ),
    (
        "partitions",
        &[
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "PARTITION_NAME",
            "SUBPARTITION_NAME",
            "PARTITION_ORDINAL_POSITION",
            "SUBPARTITION_ORDINAL_POSITION",
            "PARTITION_METHOD",
            "SUBPARTITION_METHOD",
            "PARTITION_EXPRESSION",
            "SUBPARTITION_EXPRESSION",
            "PARTITION_DESCRIPTION",
            "TABLE_ROWS",
            "AVG_ROW_LENGTH",
            "DATA_LENGTH",
            "MAX_DATA_LENGTH",
            "INDEX_LENGTH",
            "DATA_FREE",
            "CREATE_TIME",
            "UPDATE_TIME",
            "CHECK_TIME",
            "CHECKSUM",
            "PARTITION_COMMENT",
            "NODEGROUP",
            "TABLESPACE_NAME",
        ],
    ),
    (
        "events",
        &[
            "EVENT_CATALOG",
            "EVENT_SCHEMA",
            "EVENT_NAME",
            "DEFINER",
            "TIME_ZONE",
            "EVENT_BODY",
            "EVENT_DEFINITION",
            "EVENT_TYPE",
            "EXECUTE_AT",
            "INTERVAL_VALUE",
            "INTERVAL_FIELD",
            "SQL_MODE",
            "STARTS",
            "ENDS",
            "STATUS",
            "ON_COMPLETION",
            "CREATED",
            "LAST_ALTERED",
            "LAST_EXECUTED",
            "EVENT_COMMENT",
            "ORIGINATOR",
            "CHARACTER_SET_CLIENT",
            "COLLATION_CONNECTION",
            "DATABASE_COLLATION",
        ],
    ),
    (
        "processlist",
        &[
            "ID", "USER", "HOST", "DB", "COMMAND", "TIME", "STATE", "INFO",
        ],
    ),
    (
        "engines",
        &[
            "ENGINE",
            "SUPPORT",
            "COMMENT",
            "TRANSACTIONS",
            "XA",
            "SAVEPOINTS",
        ],
    ),
];

/// FROM 子句命中 `information_schema.<缺失表>` → 该表的列名。
fn missing_information_schema(
    from: &[sqlparser::ast::TableWithJoins],
) -> Option<&'static [&'static str]> {
    let sqlparser::ast::TableFactor::Table { name, .. } = &from.first()?.relation else {
        return None;
    };
    let parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|p| p.as_ident())
        .map(|i| i.value.to_ascii_lowercase())
        .collect();
    let (schema, table) = match parts.len() {
        0 => return None,
        1 => ("public".to_string(), parts[0].clone()),
        n => (parts[n - 2].clone(), parts[n - 1].clone()),
    };
    if schema != "information_schema" {
        return None;
    }
    IS_EMPTY_TABLES
        .iter()
        .find(|(n, _)| *n == table)
        .map(|(_, cols)| *cols)
}

/// 表列描述 → SHOW COLUMNS 行（无索引/主键概念 → Key/Default/Extra 恒空）。
fn show_columns_rows(desc: &crate::TableDesc, full: bool) -> Vec<Vec<String>> {
    desc.columns
        .iter()
        .map(|c| {
            let null = if c.nullable { "YES" } else { "NO" };
            if full {
                vec![
                    c.name.clone(),
                    c.mysql_type.clone(),
                    "utf8mb4_general_ci".into(),
                    null.into(),
                    String::new(),
                    String::new(),
                    String::new(),
                    "select,insert,update,references".into(),
                    String::new(),
                ]
            } else {
                vec![
                    c.name.clone(),
                    c.mysql_type.clone(),
                    null.into(),
                    String::new(),
                    String::new(),
                    String::new(),
                ]
            }
        })
        .collect()
}

fn table_from_options(
    opts: &sqlparser::ast::ShowStatementOptions,
    session: &SessionCtx,
) -> Option<String> {
    opts.show_in
        .as_ref()
        .and_then(|i| i.parent_name.as_ref())
        // 表引用 → 全限定标识（`SHOW COLUMNS FROM sales.orders` 支持跨 schema）
        .map(|n| crate::sql::resolve_table_ref(n, session))
}

/// `SHOW TABLES FROM/IN <db>` 的限定库名（**裸 schema 名**；无限定 → 会话 schema）。
///
/// 与 [`table_from_options`] 的差别：那边要的是**表**（`resolve_table_ref` 会把
/// 单段名补成 `schema.table` 全限定），这边要的是**库**本身 —— `SHOW TABLES FROM sales`
/// 的 `sales` 是 schema，不是"当前库下一张叫 sales 的表"（`D-32` 的根因正是这个混淆）。
pub(crate) fn schema_from_options(
    opts: &sqlparser::ast::ShowStatementOptions,
    session: &SessionCtx,
) -> String {
    opts.show_in
        .as_ref()
        .and_then(|i| i.parent_name.as_ref())
        // 取**最后一段**标识符：`yuntun.sales` / `sales` 都 → `sales`
        // （catalog 名可带可不带，与 `CREATE DATABASE` 的取名口径一致）
        .and_then(|n| {
            n.0.iter()
                .rev()
                .find_map(|p| p.as_ident().map(|i| i.value.clone()))
        })
        .unwrap_or_else(|| session.schema().to_string())
}

/// 无 FROM 的探测查询：全部投影为 @@var 或 DATABASE()/SCHEMA() 时返回 canned 行。
async fn canned_probe(
    q: &sqlparser::ast::Query,
    session: &SessionCtx,
    engine: &SqlEngine,
) -> Result<Option<SqlResult>, SqlError> {
    let sel = match q.body.as_ref() {
        sqlparser::ast::SetExpr::Select(sel) => sel,
        _ => return Ok(None),
    };
    let mut names: Vec<String> = Vec::new();
    let mut values: Vec<String> = Vec::new();
    for item in sel.projection.iter() {
        // 驱动普遍带别名（`SELECT @@version AS version`）：列名取别名，值仍按变量名
        let (expr, alias) = match item {
            sqlparser::ast::SelectItem::UnnamedExpr(e) => (e, None),
            sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                (expr, Some(alias.value.clone()))
            }
            _ => return Ok(None),
        };
        match classify_probe_expr(expr) {
            ProbeKind::Variable(name) => {
                names.push(alias.unwrap_or_else(|| name.clone()));
                // 与 `SHOW VARIABLES` 同一个取值入口（`@@max_connections` 也必须是真值）
                values.push(variable_value(&name, engine));
            }
            ProbeKind::Database(name) => {
                names.push(alias.unwrap_or(name));
                // 当前会话 schema（USE 后可反映）
                values.push(session.schema().to_string());
            }
            ProbeKind::Other => return Ok(None), // 混合真实列 → 交分流/DataFusion
        }
    }
    let cols: Vec<&str> = names.iter().map(String::as_str).collect();
    Ok(strings_result(&cols, &[values]))
}

enum ProbeKind {
    /// `@@var`（name 已去掉 `@@` 前缀，与 canned_variable 的键一致）
    Variable(String),
    /// DATABASE() / SCHEMA()
    Database(String),
    Other,
}

/// 变量表达式 → 规范化变量名（`@@session.auto_increment_increment`
/// → `auto_increment_increment`）。
fn var_name(raw: &str) -> String {
    raw.trim_start_matches('@').to_string()
}

fn classify_probe_expr(expr: &Expr) -> ProbeKind {
    match expr {
        Expr::Value(v) => match &v.value {
            Value::Placeholder(name) if name.starts_with("@@") => {
                ProbeKind::Variable(var_name(name))
            }
            _ => ProbeKind::Other,
        },
        // sqlparser 的形态差异：`@@version` → Identifier("@@version")；
        // `@@session.auto_increment_increment` → CompoundIdentifier(["@@session", "auto_increment_increment"])
        Expr::Identifier(id) if id.value.starts_with("@@") => {
            ProbeKind::Variable(var_name(&id.value))
        }
        Expr::CompoundIdentifier(parts) => {
            let first = parts.first().map(|i| i.value.as_str()).unwrap_or("");
            if !first.starts_with("@@") {
                return ProbeKind::Other;
            }
            parts
                .last()
                .map(|i| ProbeKind::Variable(var_name(&i.value)))
                .unwrap_or(ProbeKind::Other)
        }
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
/// `SHOW [FULL] PROCESSLIST`：列名/类型与 MySQL 8 一致。
///
/// `Id` / `Time` 用 **Int64**（不是 UInt64）：JDBC 的 `getLong` 对无符号列也能读，
/// 但部分客户端把 `BIGINT UNSIGNED` 显示成超过 i64 的值；进程 Id 不可能那么大，
/// 用有符号类型最省事。
fn processlist_result(rows: Vec<crate::registry::ProcessRow>) -> SqlResult {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("Id", DataType::Int64, false),
        Field::new("User", DataType::Utf8, false),
        Field::new("Host", DataType::Utf8, false),
        Field::new("db", DataType::Utf8, true),
        Field::new("Command", DataType::Utf8, false),
        Field::new("Time", DataType::Int64, false),
        Field::new("State", DataType::Utf8, true),
        Field::new("Info", DataType::Utf8, true),
    ]));
    let db = |r: &crate::registry::ProcessRow| {
        if r.db.is_empty() {
            None
        } else {
            Some(r.db.clone())
        }
    };
    let info = |r: &crate::registry::ProcessRow| {
        if r.info.is_empty() {
            None
        } else {
            Some(r.info.clone())
        }
    };
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.id as i64).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from_iter(
            rows.iter().map(|r| Some(r.user.as_str())),
        )),
        Arc::new(StringArray::from_iter(
            rows.iter().map(|r| Some(r.host.as_str())),
        )),
        Arc::new(StringArray::from_iter(rows.iter().map(db))),
        Arc::new(StringArray::from_iter(rows.iter().map(|r| Some(r.command)))),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.time_secs).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from_iter(
            rows.iter().map(|r| Some(r.state.as_str())),
        )),
        Arc::new(StringArray::from_iter(rows.iter().map(info))),
    ];
    let batch = RecordBatch::try_new(schema, cols).expect("processlist batch");
    SqlResult::Rows {
        schema: batch.schema(),
        batches: vec![batch],
        partial: yuntun_query::PartialRead::default(),
    }
}

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
        // shim 的 canned 结果不读热数据 ⇒ 恒完整（`§89`）
        partial: yuntun_query::PartialRead::default(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::SqlDialect;
    use crate::{ColumnDesc, TableDesc};
    use sqlparser::ast::{SelectItem, SetExpr, Statement};
    use std::sync::Arc;

    /// 解析 SELECT 并按 shim 的探测分类规则产出 `(列名, canned 值)`。
    fn probe(sql: &str) -> Vec<(String, String)> {
        let stmt = crate::sql::parse_single_with(SqlDialect::MySql.parser_dialect(), sql).unwrap();
        let Statement::Query(q) = stmt else {
            panic!("not a query: {sql}");
        };
        let SetExpr::Select(sel) = q.body.as_ref() else {
            panic!("not a select: {sql}");
        };
        sel.projection
            .iter()
            .map(|item| {
                let (expr, alias) = match item {
                    SelectItem::UnnamedExpr(e) => (e, None),
                    SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
                    _ => panic!("unexpected projection item in {sql}"),
                };
                match classify_probe_expr(expr) {
                    ProbeKind::Variable(n) => (
                        alias.unwrap_or_else(|| n.clone()),
                        canned_variable(&n).to_string(),
                    ),
                    ProbeKind::Database(n) => (alias.unwrap_or(n), "public".to_string()),
                    ProbeKind::Other => (String::new(), String::new()),
                }
            })
            .collect()
    }

    #[test]
    fn probe_variables_across_sqlparser_shapes() {
        // Value::Placeholder 形态
        assert_eq!(
            probe("SELECT @@version"),
            [("version".to_string(), "8.0.32-yuntun".to_string())]
        );
        // Identifier / CompoundIdentifier 形态（JDBC 握手路径）
        assert_eq!(
            probe("SELECT @@session.auto_increment_increment"),
            [("auto_increment_increment".to_string(), "1".to_string())]
        );
        // 带别名（驱动普遍写法）
        assert_eq!(
            probe("SELECT @@version_comment AS version_comment"),
            [("version_comment".to_string(), "yuntun".to_string())]
        );
        assert_eq!(
            probe("SELECT @@session.auto_increment_increment AS ai, @@character_set_client AS cs"),
            [
                ("ai".to_string(), "1".to_string()),
                ("cs".to_string(), "utf8mb4".to_string())
            ]
        );
        // DATABASE() / SCHEMA()
        assert_eq!(
            probe("SELECT DATABASE()"),
            [("database".to_string(), "public".to_string())]
        );
        // 真实列 → 不拦截（交分流/DataFusion）
        assert_eq!(probe("SELECT user"), [(String::new(), String::new())]);
        // 未知变量：空串（驱动只读取，不报错）
        assert_eq!(
            probe("SELECT @@some_unknown_var"),
            [("some_unknown_var".to_string(), String::new())]
        );
    }

    /// 解析 SELECT → FROM 是否命中 information_schema 补洞表（返回列名）。
    fn is_gap(sql: &str) -> Option<&'static [&'static str]> {
        let stmt = crate::sql::parse_single_with(SqlDialect::MySql.parser_dialect(), sql).unwrap();
        let Statement::Query(q) = stmt else {
            panic!("not a query: {sql}");
        };
        let SetExpr::Select(sel) = q.body.as_ref() else {
            panic!("not a select: {sql}");
        };
        missing_information_schema(&sel.from)
    }

    #[test]
    fn information_schema_gaps_are_filled() {
        // DBeaver 浏览时查询、DataFusion 未提供 → 列名齐全的空结果集（不再 1146）
        let kcu = is_gap("SELECT * FROM information_schema.key_column_usage").unwrap();
        assert!(kcu.contains(&"COLUMN_NAME"));
        assert!(kcu.contains(&"REFERENCED_TABLE_NAME"));
        assert!(is_gap("SELECT * FROM information_schema.referential_constraints").is_some());
        assert!(is_gap("SELECT * FROM information_schema.triggers").is_some());
        assert!(is_gap("SELECT * FROM information_schema.statistics").is_some());
        assert!(is_gap("SELECT * FROM information_schema.partitions").is_some());
        // 限定名（`yuntun.information_schema.x`）同样命中
        assert!(is_gap("SELECT * FROM yuntun.information_schema.key_column_usage").is_some());
        // DataFusion 已提供的表 → 不拦截（保留原生实现）
        assert!(is_gap("SELECT * FROM information_schema.tables").is_none());
        assert!(is_gap("SELECT * FROM information_schema.columns").is_none());
        // 普通表 → 不拦截
        assert!(is_gap("SELECT * FROM api_audit").is_none());
    }

    #[test]
    fn show_columns_row_shapes() {
        let desc = TableDesc {
            name: "t".into(),
            columns: vec![ColumnDesc {
                name: "a".into(),
                mysql_type: "int".into(),
                nullable: true,
            }],
            schema: Arc::new(arrow::datatypes::Schema::empty()),
        };
        // SHOW FULL COLUMNS = 9 列（JDBC getColumns 需要 Collation/Privileges/Comment）
        let full = show_columns_rows(&desc, true);
        assert_eq!(full[0].len(), SHOW_FULL_COLUMNS.len());
        assert_eq!(full[0][0], "a");
        assert_eq!(full[0][2], "utf8mb4_general_ci");
        // 普通 SHOW COLUMNS / DESCRIBE = 6 列
        let short = show_columns_rows(&desc, false);
        assert_eq!(short[0].len(), SHOW_COLUMNS.len());
        assert_eq!(short[0][2], "YES");
        // 无索引概念 → SHOW KEYS 列名齐全、行数为 0
        assert!(SHOW_KEYS_COLUMNS.contains(&"Key_name"));
        assert!(SHOW_KEYS_COLUMNS.contains(&"Table"));
    }
}
