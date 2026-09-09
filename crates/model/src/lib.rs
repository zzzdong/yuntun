//! 核心数据模型 crate —— 最底层，不依赖任何 workspace 内 crate（详细设计 §2.2）。
//!
//! 包含：
//! - [`error`]: 全局错误类型（详细设计 §10.1）
//! - [`meta`]: Catalog 元数据 prost 消息（详细设计 §6.2）
//! - [`wal_record`]: WAL Record 类型与编解码（详细设计 §4.3）
//! - [`schema`]: Schema 演进：类型提升格 + 变更分类（详细设计 §8）
//! - [`ops`]: Catalog 操作的请求/响应类型（详细设计 §3.3）
//! - [`batch`]: 攒批批次状态（BatchState，由 WAL 事件重建，详细设计 §4.6）

pub mod batch;
pub mod error;
pub mod meta;
pub mod ops;
pub mod schema;
pub mod wal_record;

pub use batch::{BatchState, BatchStatus};
pub use error::LakeError;
pub use meta::*;
pub use ops::*;
pub use schema::{apply_change, classify, SchemaChange, SchemaChangeKind, SchemaCompatibility};
use std::time::SystemTime;
pub use wal_record::{DdlPayload, Record, RecordType};
/// 归一化写入单元 —— 下游完全不感知协议差异（详细设计 §3.1 / ADR-13）。
#[derive(Debug, Clone)]
pub struct IngestBatch {
    pub table: String,
    /// 由 Source 负责提取（各协议逻辑不同，详细设计 §3.1）
    pub shard_key: String,
    /// 自带 schema
    pub record_batch: arrow::record_batch::RecordBatch,
    /// 客户端幂等键（§7.3）
    pub idempotency_key: Option<String>,
    pub received_at: SystemTime,
}

/// 建表时的表模板（详细设计 §7.3.2）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableTemplate {
    /// 审计/交易流水：require_idempotency_key = true
    Audit,
    /// 默认模板：require_idempotency_key = true
    General,
    /// 高吞吐：require_idempotency_key = false（接受极低概率重复）
    Metrics,
    Traces,
}

impl TableTemplate {
    /// 处理矩阵（详细设计 §7.3.2）：
    /// - `Audit | General` 默认强制幂等键
    /// - `Metrics | Traces` 可选幂等（客户端主动传仍去重）
    pub fn require_idempotency_key(&self) -> bool {
        matches!(self, TableTemplate::Audit | TableTemplate::General)
    }
}

/// 持久性 SLA 分级（架构 §4-ADR-9）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    /// 默认：本地 WAL，节点磁盘故障 = 未提交数据丢失
    #[default]
    BestEffort,
    /// WAL 异步归档 S3，节点可从 S3 重建（阶段 1+ 实现）
    Durable,
}

/// 幂等键校验（详细设计 §7.3.2）。
/// 字符集不限制（UUID 或 `source_${ts}_${seq}` 均可），长度上限 256 字节。
pub fn validate_idempotency_key(key: &str) -> Result<(), LakeError> {
    if key.len() > 256 {
        return Err(LakeError::IdempotencyKeyTooLong);
    }
    Ok(())
}
