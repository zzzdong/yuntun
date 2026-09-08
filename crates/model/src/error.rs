//! 全局错误类型（详细设计 §10.1）。
//!
//! 错误分类决定重试策略（§10.2）：
//! - 客户端错误（SchemaIncompatible / IdempotencyKeyRequired / TableNotFound）不重试
//! - `SchemaChanged` 立即重试（拉新 schema 重新判定，最多 3 次）
//! - S3 / 网络：指数退避
//! - `Wal`：不重试，WAL 故障是致命错误

use std::sync::Arc;

#[derive(thiserror::Error, Debug, Clone)]
pub enum LakeError {
    // ---- 客户端错误（4xx，不重试）----
    #[error("schema incompatible: {0}")]
    SchemaIncompatible(String),
    #[error("idempotency key required")]
    IdempotencyKeyRequired,
    #[error("idempotency key too long (max 256)")]
    IdempotencyKeyTooLong,
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),
    #[error("invalid schema change: {0}")]
    InvalidSchemaChange(String),

    // ---- 可重试（5xx / 暂态）----
    // C8: OCC 仅作用于 EvolveSchema，不作用于 CommitFiles（详细设计 §8.2）。
    // 调用方收到此错误后应拉取新 schema 重新判定（详细设计 §5.2 时序）。
    #[error("schema changed, retry (actual version {actual_version})")]
    SchemaChanged {
        actual_version: u64,
        new_schema: Arc<arrow::datatypes::Schema>,
    },
    #[error("s3 error: {0}")]
    S3(String),
    #[error("wal error: {0}")]
    Wal(#[from] WalError),
    #[error("io error: {0}")]
    Io(String),
    #[error("{0}")]
    Other(String),
}

impl From<std::io::Error> for LakeError {
    fn from(e: std::io::Error) -> Self {
        LakeError::Io(e.to_string())
    }
}

/// WAL 专用错误（详细设计 §4.9）。
#[derive(thiserror::Error, Debug, Clone)]
pub enum WalError {
    #[error("torn write detected at offset {offset} (CRC mismatch)")]
    TornWrite { offset: u64 },
    #[error("record length {length} exceeds remaining file at offset {offset}")]
    Truncated { offset: u64, length: u64 },
    #[error("bad file header magic in {0}")]
    BadMagic(String),
    #[error("unsupported wal version {0}")]
    UnsupportedVersion(u16),
    #[error("CURRENT file corrupted: {0}")]
    CorruptedCurrent(String),
    #[error("{0}")]
    Other(String),
}
