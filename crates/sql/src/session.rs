//! 会话上下文（协议适配层持有并传入，裁决 S-4：引擎无状态）。

use sqlparser::ast::Statement;
use sqlparser::dialect::{Dialect as SqlDialectTrait, GenericDialect, MySqlDialect};
use sqlparser::dialect::PostgreSqlDialect;

/// SQL 方言（决定 sqlparser 解析行为；MySQL wire → MySql，Flight → Generic）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Generic,
    MySql,
    PostgreSql,
}

impl SqlDialect {
    pub fn parser_dialect(&self) -> &'static dyn SqlDialectTrait {
        match self {
            SqlDialect::Generic => &GenericDialect {},
            SqlDialect::MySql => &MySqlDialect {},
            SqlDialect::PostgreSql => &PostgreSqlDialect {},
        }
    }
}

/// 连接会话：由协议适配层按连接创建并持有。
#[derive(Debug, Clone)]
pub struct SessionCtx {
    pub dialect: SqlDialect,
    /// 当前 schema（MySQL 的 database 概念）：`USE db` / handshake database /
    /// PG `\c db` 切换；`None` = [`yuntun_model::ops::DEFAULT_SCHEMA`]。
    pub default_db: Option<String>,
}

impl Default for SessionCtx {
    fn default() -> Self {
        Self {
            dialect: SqlDialect::Generic,
            default_db: None,
        }
    }
}

impl SessionCtx {
    pub fn mysql() -> Self {
        Self {
            dialect: SqlDialect::MySql,
            default_db: None,
        }
    }

    /// 当前 schema（未切换 → `public`）：非限定表名解析的默认归属。
    pub fn schema(&self) -> &str {
        self.default_db
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or(yuntun_model::ops::DEFAULT_SCHEMA)
    }

    /// 切换 schema（`USE db` / handshake）；调用方负责先校验其存在。
    pub fn set_schema(&mut self, schema: &str) {
        self.default_db = Some(schema.to_string());
    }
}

/// 多语句占位（未来多语句支持时使用；MVP 恒单语句）。
pub type SingleStatement = Statement;
