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
    /// 当前库（USE db / PG \c）——MVP 校验后仅记录（R-3：单 schema yuntun.public）
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
}

/// 多语句占位（未来多语句支持时使用；MVP 恒单语句）。
pub type SingleStatement = Statement;
