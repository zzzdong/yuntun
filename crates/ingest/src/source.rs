//! Source 可插拔抽象（ADR-13 / 详细设计 §3.1 / §7.1.1）。
//!
//! MVP 仅 Arrow Flight，但 trait 从第一天就定义好，
//! 所有协议输出统一的 [`IngestBatch`]，下游（攒批 / WAL / S3 / Meta）
//! 完全不感知协议差异。阶段 2+ 加 InfluxDB / Kafka 零重构。

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use yuntun_model::error::LakeError;

pub use yuntun_model::IngestBatch;

/// 摄入 Source（ADR-13）。
#[async_trait]
pub trait IngestSource: Send + Sync + 'static {
    /// 协议标识，用于日志与指标
    fn name(&self) -> &'static str;

    /// 启动服务，持续产出归一化的 IngestBatch。
    ///
    /// # 契约
    /// - `tx` 背压由下游控制，Source 不得无限缓冲
    /// - `shutdown` 触发后应在 grace period 内退出
    /// - **shard_key 由 Source 负责提取**（各协议逻辑不同）
    async fn run(
        &self,
        tx: mpsc::Sender<IngestBatch>,
        shutdown: CancellationToken,
    ) -> Result<(), LakeError>;
}

/// 写入回执（架构 §11.3）。
/// `expected_visible_at` 用于前端提示"数据将在 X 秒后可见"。
#[derive(Debug, Clone, serde::Serialize)]
pub struct Receipt {
    pub table: String,
    pub shard: String,
    /// 本批数据在 WAL 中的确认 seq
    pub wal_seq: u64,
    pub row_count: u64,
    pub schema_version: u64,
    /// 数据预计可见时间（Unix 毫秒）
    pub expected_visible_at: u64,
    /// 攒批时间窗（秒）
    pub expected_visible_in_secs: u64,
}

/// 从 Flight descriptor path 提取 (table, shard)。
/// Flight（MVP）的 shard_key 提取规则：客户端在 descriptor 中指定（§3.1 表格）。
pub fn extract_table_shard(raw_path: &[String]) -> Result<(String, String), LakeError> {
    match raw_path {
        [table, shard] if !table.is_empty() && !shard.is_empty() => {
            Ok((table.clone(), shard.clone()))
        }
        _ => Err(LakeError::Other(
            "flight descriptor path must be [table, shard]".into(),
        )),
    }
}
