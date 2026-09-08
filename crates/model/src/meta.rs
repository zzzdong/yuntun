//! Catalog 元数据 prost 消息（详细设计 §6.2）。
//!
//! 即使阶段 0 单节点，接口也按 Raft 线性一致性语义设计（§6.1），
//! 消息结构直接对齐架构 §5.1，阶段 1 切换 gRPC 零改动。

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::convert::{fb_to_schema, IpcSchemaEncoder};
use arrow::ipc::root_as_schema;
use std::sync::Arc;

/// Arrow Schema ↔ IPC 字节互转（SchemaVersion.arrow_schema 的载体）。
pub fn serialize_schema(schema: &SchemaRef) -> Vec<u8> {
    IpcSchemaEncoder::new()
        .schema_to_fb(schema)
        .finished_data()
        .to_vec()
}

pub fn deserialize_schema(bytes: &[u8]) -> Result<SchemaRef, crate::error::LakeError> {
    let fb = root_as_schema(bytes)
        .map_err(|e| crate::error::LakeError::Other(format!("decode schema: {e}")))?;
    Ok(Arc::new(fb_to_schema(fb)))
}

/// 表元数据（架构 §5.1 TableMeta）
#[derive(Clone, PartialEq, prost::Message)]
pub struct TableMeta {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(uint64, tag = "2")]
    pub current_schema_version: u64,
    #[prost(string, repeated, tag = "3")]
    pub partition_cols: Vec<String>,
    /// "vortex" | "parquet"（ADR-1 FormatSwitch 回退开关）
    #[prost(string, tag = "4")]
    pub default_format: String,
    #[prost(message, optional, tag = "5")]
    pub ingest_config: Option<IngestConfig>,
    #[prost(uint64, tag = "6")]
    pub created_at: u64,
    /// v1 扩展：完整 Arrow Schema（IPC 序列化），随 SchemaVersion 链演进
    #[prost(bytes = "vec", tag = "7")]
    pub arrow_schema: Vec<u8>,
    /// v1 扩展：表模板（0=Audit 1=General 2=Metrics 3=Traces）
    #[prost(uint32, tag = "8")]
    pub table_template: u32,
}

impl TableMeta {
    pub fn schema(&self) -> Result<SchemaRef, crate::error::LakeError> {
        deserialize_schema(&self.arrow_schema)
    }

    pub fn with_schema(mut self, schema: &SchemaRef) -> Self {
        self.arrow_schema = serialize_schema(schema);
        self
    }
}

/// 表级摄入配置（架构 §4-ADR-9 / §7.2 / §7.3.2）
#[derive(Clone, PartialEq, prost::Message)]
pub struct IngestConfig {
    /// 【v9】幂等键默认开启；Metrics/Traces 模板可关闭（§7.3.2）
    #[prost(bool, tag = "1", default = true)]
    pub require_idempotency_key: bool,
    /// 幂等键 TTL，默认 24h（秒）
    #[prost(uint64, tag = "2")]
    pub idempotency_ttl_secs: u64,
    /// 攒批行数阈值（默认 10000，§7.2）
    #[prost(uint64, tag = "3")]
    pub rows_threshold: u64,
    /// 攒批时间阈值（默认 5s，秒）
    #[prost(uint64, tag = "4")]
    pub time_threshold_secs: u64,
    /// 持久性 SLA（ADR-9）：0=best_effort 1=durable
    #[prost(uint32, tag = "5")]
    pub durability: u32,
}

impl IngestConfig {
    /// 默认值（中吞吐通用表，§7.2 表格）。
    pub fn standard() -> Self {
        Self {
            require_idempotency_key: true,
            idempotency_ttl_secs: 24 * 3600,
            rows_threshold: 10_000,
            time_threshold_secs: 5,
            durability: 0,
        }
    }

    pub fn from_template(t: crate::TableTemplate) -> Self {
        let mut c = Self::standard();
        c.require_idempotency_key = t.require_idempotency_key();
        c
    }
}

/// Schema 版本链（架构 §5.1 SchemaVersion）
#[derive(Clone, PartialEq, prost::Message)]
pub struct SchemaVersion {
    /// 单调递增
    #[prost(uint64, tag = "1")]
    pub version: u64,
    /// 该版本的完整 Arrow Schema（IPC 序列化）
    #[prost(bytes = "vec", tag = "2")]
    pub arrow_schema: Vec<u8>,
    /// 0=ADD_COLUMN 1=WIDEN_TYPE 2=DROP_COLUMN
    #[prost(uint32, tag = "3")]
    pub change_kind: u32,
    #[prost(uint64, tag = "4")]
    pub created_at: u64,
    /// 人类可读，如 "add column user_agent: Utf8"
    #[prost(string, tag = "5")]
    pub change_desc: String,
}

impl SchemaVersion {
    pub fn schema(&self) -> Result<SchemaRef, crate::error::LakeError> {
        deserialize_schema(&self.arrow_schema)
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct FileManifest {
    #[prost(string, tag = "1")]
    pub file_path: String,
    /// 幂等主键（ADR-4：随机 UUIDv7）
    #[prost(string, tag = "2")]
    pub batch_id: String,
    /// 客户端幂等键，唯一索引（可空，§7.3）
    #[prost(string, tag = "3")]
    pub client_request_id: String,
    /// 该文件写入时的 Schema 版本（§5.1；不同文件可有不同版本，C8）
    #[prost(uint64, tag = "4")]
    pub schema_version: u64,
    /// 0=ACTIVE 1=STAGED 2=DELETED
    #[prost(uint32, tag = "5")]
    pub status: u32,
    /// 快照号：valid_from <= query_snapshot 才可见
    #[prost(uint64, tag = "6")]
    pub valid_from: u64,
    /// 0 = 未删除；query_snapshot < deleted_at 才可见
    #[prost(uint64, tag = "7")]
    pub deleted_at: u64,
    #[prost(message, optional, tag = "8")]
    pub stats: Option<StatisticsLite>,
    #[prost(uint64, tag = "9")]
    pub row_count: u64,
    #[prost(uint64, tag = "10")]
    pub file_size: u64,
    /// v1 扩展：所属表
    #[prost(string, tag = "11")]
    pub table: String,
    /// v1 扩展：所属 shard
    #[prost(string, tag = "12")]
    pub shard: String,
    /// v1 扩展：时间窗口（整分钟对齐，ADR-10）
    #[prost(string, tag = "13")]
    pub time_window: String,
}

impl FileManifest {
    pub fn is_active(&self) -> bool {
        self.status == FileStatus::Active as u32
    }

    /// 快照可见性规则（详细设计 §6.3）：
    /// 文件可见 ⟺ valid_from <= query_snapshot
    ///           AND (deleted_at == 0 OR query_snapshot < deleted_at)
    pub fn visible_at(&self, snapshot: u64) -> bool {
        self.valid_from <= snapshot && (self.deleted_at == 0 || snapshot < self.deleted_at)
    }
}

pub enum FileStatus {
    Active = 0,
    Staged = 1,
    Deleted = 2,
}

/// 幂等键记录（【v8 修正 1】独立表，不随 FileManifest 删除而删除，§7.3.1）
#[derive(Clone, PartialEq, prost::Message)]
pub struct IdempotencyRecord {
    /// 主键
    #[prost(string, tag = "1")]
    pub client_request_id: String,
    #[prost(string, tag = "2")]
    pub batch_id: String,
    /// TTL 24h 起算点（Unix 秒）
    #[prost(uint64, tag = "3")]
    pub committed_at: u64,
}

/// 精简统计：仅排序列 + 分区列的 min/max/null_count（架构 §5.2）
#[derive(Clone, PartialEq, prost::Message)]
pub struct StatisticsLite {
    #[prost(message, repeated, tag = "1")]
    pub columns: Vec<ColumnStatLite>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ColumnStatLite {
    #[prost(string, tag = "1")]
    pub name: String,
    /// Arrow 标量 IPC 序列化
    #[prost(bytes = "vec", tag = "2")]
    pub min: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub max: Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub null_count: u64,
}

/// 从 RecordBatch 计算精简统计（仅排序列 + 分区列）。
pub fn compute_stats_lite(
    batch: &arrow::record_batch::RecordBatch,
    columns: &[String],
) -> Result<StatisticsLite, crate::error::LakeError> {
    use arrow::array::Array;
    let mut cols = Vec::new();
    for name in columns {
        let Some((idx, _)) = batch.schema().column_with_name(name) else {
            continue;
        };
        let arr = batch.column(idx);
        // min/max 计算依赖 arrow-compute；此处仅收集 null_count（精简统计的必要成分），
        // min/max 由文件 footer（Vortex/Parquet）提供完整统计，Meta 只做文件级剪枝。
        cols.push(ColumnStatLite {
            name: name.clone(),
            min: Vec::new(),
            max: Vec::new(),
            null_count: arr.null_count() as u64,
        });
    }
    Ok(StatisticsLite { columns: cols })
}

/// 默认排序列：event_time（业务约定，缺失时无排序列）。
pub fn default_sort_columns(schema: &SchemaRef) -> Vec<String> {
    if schema.field_with_name("event_time").is_ok() {
        vec!["event_time".to_string()]
    } else {
        Vec::new()
    }
}

/// 数值类型提升格中的秩（详细设计 §8.1）：
/// Int8 < Int16 < Int32 < Int64 < Float64
/// Utf8 与数值不可互转 —— 拒绝。
pub fn promotion_rank(dt: &DataType) -> Option<u8> {
    Some(match dt {
        DataType::Int8 => 0,
        DataType::Int16 => 1,
        DataType::Int32 => 2,
        DataType::Int64 => 3,
        DataType::Float64 => 4,
        _ => return None,
    })
}

/// 单元测试辅助：构造一个最小 schema
pub fn test_schema(fields: &[(&str, DataType)]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|(n, t)| Field::new(*n, t.clone(), true))
            .collect::<Vec<_>>(),
    ))
}
