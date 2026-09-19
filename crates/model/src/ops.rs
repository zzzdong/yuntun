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
    /// **本次提交携带的幂等键集合**（R3 S3-5，关闭 `operation-log §27.5` 遗留 #1）。
    ///
    /// 为什么需要它：一个 chunk 会聚合**多条带不同键的 Data 记录**，
    /// 而提交层此前只能接受**单个**键 → 无法按键集合去重，只能靠 ingest 入口预筛兜底
    /// （`flush.rs` 里当时刻意传 `None` 就是为此）。权威去重在 Catalog（R3 起在 raft 状态机）。
    ///
    /// 语义：集合中**任一**键已登记 → 整次提交判为重复（`accepted=false`），不重复落 manifest。
    /// 空 = 该批次无键（不参与去重）。
    pub client_request_ids: Vec<String>,
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

// ---------------- 版本与增量（S2-5 / S2-7）----------------

/// Catalog **版本号，分两组**（`refactor.md` S2-5）。
///
/// 为什么要分：`commit_files` 是最高频的写（每次 flush 一次），若与 schema 变更共用
/// 一个版本号，则**每次 flush 都会让全表 schema 缓存失效**，缓存退化为全量重建。
///
/// | 组 | 谁在推 | 变化频率 | 缓存该做什么 |
/// |---|---|---|---|
/// | `schema_ver` | `create/drop table`、`create/drop schema`、`evolve_schema` | 低（DDL） | 全量重建（表清单 + 每个表的结构） |
/// | `manifest_ver` | `commit_files`、`commit_compaction`、`drop_shard`、`drop_table` | 高（写入持续推） | **只按增量拉变化的表** |
///
/// 与既有两个号的区别（**不要混用**）：
/// - `snapshot`：**快照隔离**语义（文件 `valid_from <= snapshot` 才可见），查询一致性用；
/// - `read_index`：Raft 线性化位置（阶段 3 由 raft 提供）；
/// - 本结构：**缓存失效**语义，只回答"要不要重拉、拉哪些"。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct CatalogVersion {
    pub schema_ver: u64,
    pub manifest_ver: u64,
}

/// 自 `since_manifest_ver` 以来，文件清单发生过变化的表（S2-7 增量接口）。
///
/// 消费方语义：对 `changed_tables` 里的每个表重拉一次可见文件即可，
/// **不必**遍历全部表；其余表的缓存条目原样有效。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManifestDelta {
    /// 变更表的全限定标识（去重、升序）
    pub changed_tables: Vec<String>,
    /// `true` = 增量无法表达（如发生了删表/重建），调用方必须**全量重建**。
    /// 增量接口宁可保守：无法表达就要求全量，绝不允许"漏掉变更"。
    pub full_reload_required: bool,
}

impl ManifestDelta {
    pub fn full() -> Self {
        Self {
            changed_tables: Vec::new(),
            full_reload_required: true,
        }
    }

    pub fn is_empty(&self) -> bool {
        !self.full_reload_required && self.changed_tables.is_empty()
    }
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
