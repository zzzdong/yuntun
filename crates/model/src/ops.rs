//! Catalog 操作的请求/响应类型（详细设计 §3.3 / §6.4）。
//!
//! 阶段 0 用 `MemoryCatalog`，阶段 1 用 `GrpcCatalogClient`，业务代码零修改。

use crate::meta::{FileManifest, TableMeta};
use crate::schema::SchemaChange;
use arrow::datatypes::SchemaRef;
use std::sync::Arc;

// ---------------- 表 / Schema ----------------

/// 默认 schema（MySQL 的 database 概念）：单 schema 部署与旧数据的归属地。
pub const DEFAULT_SCHEMA: &str = "public";

/// 全限定表标识 `schema.table`。
///
/// **跨层唯一表标识**：Catalog 内部键、Ingest/WAL 的 `table` 字段、对象存储路径
/// 派生、Query 缓存键全部使用它；裸表名只在 SQL 表面与 `TableMeta.name` 出现。
pub fn qualified_name(schema: &str, table: &str) -> String {
    format!("{schema}.{table}")
}

/// 拆分全限定表标识：`sales.orders` → `("sales", "orders")`；
/// 无 `.` 时视作默认 schema（兼容旧数据）。
pub fn split_qualified(qualified: &str) -> (&str, &str) {
    match qualified.split_once('.') {
        Some((schema, table)) if !schema.is_empty() && !table.is_empty() => (schema, table),
        _ => (DEFAULT_SCHEMA, qualified),
    }
}

/// schema 名合法性（与表名同规则；禁止 `.`，因为限定名用 `.` 分隔）。
pub fn validate_schema_name(name: &str) -> Result<(), crate::error::LakeError> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(crate::error::LakeError::Other(format!(
            "invalid schema name: {name:?}"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct CreateTableRequest {
    /// 裸表名（不含 schema）
    pub name: String,
    /// 所属 schema（MySQL 的 database）；空 → [`DEFAULT_SCHEMA`]
    pub namespace: String,
    pub schema: SchemaRef,
    pub partition_cols: Vec<String>,
    pub default_format: String, // "vortex" | "parquet"
    pub ingest_config: crate::meta::IngestConfig,
}

impl CreateTableRequest {
    /// 目标 schema（空 → 默认 `public`）。
    pub fn schema_name(&self) -> &str {
        if self.namespace.is_empty() {
            DEFAULT_SCHEMA
        } else {
            &self.namespace
        }
    }

    /// 全限定表标识。
    pub fn qualified_name(&self) -> String {
        qualified_name(self.schema_name(), &self.name)
    }
}

#[derive(Debug, Clone)]
pub struct EvolveSchemaRequest {
    pub table: String,
    pub change: SchemaChange,
    /// 【乐观锁】Ingestor 本地缓存的版本（C8：OCC 仅作用于 EvolveSchema）
    pub expected_version: u64,
}

#[derive(Debug, Clone)]
pub struct EvolveSchemaResponse {
    pub new_schema: SchemaRef,
    pub version: u64,
}

// ---------------- 文件 ----------------

#[derive(Debug, Clone, Default)]
pub struct CommitFilesRequest {
    pub table: String,
    /// 幂等主键（ADR-4：随机 UUIDv7，不参与幂等判断）
    pub batch_id: String,
    /// 客户端幂等键（唯一索引，可空）
    pub client_request_id: Option<String>,
    pub shard: String,
    pub time_window: String,
    pub files: Vec<FileManifest>,
    /// CommitFiles 不校验 schema version（C8）——仅作为记录写入 manifest
    pub schema_version: u64,
    pub row_count: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommitFilesResponse {
    /// false = 重复提交（幂等成功，不报错）
    pub accepted: bool,
    /// 返回的可见快照号
    pub snapshot: u64,
    /// 阶段 1：Raft log index
    pub commit_index: u64,
}

#[derive(Debug, Clone)]
pub struct ListVisibleFilesRequest {
    pub table: String,
    pub snapshot: u64,
    pub shard_filter: Option<String>,
}

// ---------------- 表名规整 ----------------

/// 表名合法性校验（仅允许 [a-zA-Z0-9_-]），防路径穿越。
pub fn validate_table_name(name: &str) -> Result<(), crate::error::LakeError> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(crate::error::LakeError::Other(format!(
            "invalid table name: {name:?}"
        )));
    }
    Ok(())
}

pub type TableMetaRef = Arc<TableMeta>;
