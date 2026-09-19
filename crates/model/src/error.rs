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
    /// 多 schema：目标 schema 不存在（MySQL 语义 → ER_BAD_DB_ERROR / 1049）
    #[error("schema not found: {0}")]
    SchemaNotFound(String),
    #[error("schema already exists: {0}")]
    SchemaAlreadyExists(String),
    /// DROP SCHEMA 时 schema 下仍有表（MySQL 语义 → ER_DB_DROP_EXISTS / 1008）
    #[error("schema is not empty: {0}")]
    SchemaNotEmpty(String),
    #[error("invalid schema change: {0}")]
    InvalidSchemaChange(String),
    /// 背压阶梯第三级（架构 §2.7）：chunk 内存 / 磁盘达水位，**明确拒绝**而非静默降级。
    /// 客户端应退避后重试（DoPut 映射为 `RESOURCE_EXHAUSTED` + `retry-after`）。
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),

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

/// 状态机快照的帧/载荷错误（`metanode-design.md` §4.4）。
///
/// 单列一个类型而不是塞进 [`LakeError`]：快照编解码只发生在 metanode 的存储/传输层，
/// 且这些错误**都是致命一致性错误**（不能像 `S3`/`SchemaChanged` 那样重试），
/// 混进 `LakeError` 会诱导调用方按"可重试"处理。
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum SnapshotError {
    #[error("snapshot frame too short: {len} bytes (header needs 36)")]
    TooShort { len: usize },
    #[error("bad snapshot magic (not a yuntun snapshot)")]
    BadMagic,
    #[error("unsupported snapshot format version {0} (this build writes 1)")]
    UnsupportedVersion(u32),
    #[error("snapshot header CRC mismatch: stored {expected:#010x} computed {actual:#010x}")]
    HeaderCrcMismatch { expected: u32, actual: u32 },
    #[error("invalid snapshot chunk size {chunk_size} (must be 1..=64MiB)")]
    InvalidChunkSize { chunk_size: u32 },
    #[error("snapshot truncated at offset {offset}: need {needed} bytes, {remaining} left")]
    Truncated {
        offset: usize,
        needed: usize,
        remaining: usize,
    },
    #[error("snapshot chunk at offset {offset} CRC mismatch: stored {expected:#010x} computed {actual:#010x}")]
    ChunkCrcMismatch {
        offset: usize,
        expected: u32,
        actual: u32,
    },
    #[error("snapshot payload length mismatch: header says {declared}, chunks carried {actual}")]
    LengthMismatch { declared: u64, actual: usize },
    #[error("snapshot has {count} trailing bytes after the last chunk")]
    TrailingBytes { count: usize },
    #[error("snapshot payload does not decode: {0}")]
    Decode(String),
    /// 载荷结构合法（protobuf 解得开）但**语义非法** —— 如键重复、缺 `public`、条目为空值。
    /// 这类错误**不能容忍**：静默取"最后一个"会让副本间状态分歧。
    #[error("snapshot payload is semantically invalid: {0}")]
    InvalidState(String),
    /// 帧头 `revision` 与载荷里的快照号不一致 —— 说明帧与载荷不是同一次快照产生的。
    #[error("snapshot revision mismatch: frame says {framed}, payload says {payload}")]
    RevisionMismatch { framed: u64, payload: u64 },
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
