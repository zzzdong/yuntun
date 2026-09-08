//! Catalog 操作的请求/响应类型（详细设计 §3.3 / §6.4）。
//!
//! 阶段 0 用 `MemoryCatalog`，阶段 1 用 `GrpcCatalogClient`，业务代码零修改。

use crate::meta::{FileManifest, TableMeta};
use crate::schema::SchemaChange;
use arrow::datatypes::SchemaRef;
use std::sync::Arc;

// ---------------- 表 / Schema ----------------

#[derive(Debug, Clone)]
pub struct CreateTableRequest {
    pub name: String,
    pub schema: SchemaRef,
    pub partition_cols: Vec<String>,
    pub default_format: String, // "vortex" | "parquet"
    pub ingest_config: crate::meta::IngestConfig,
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
