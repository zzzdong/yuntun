//! Ingestor：写入路径（Source → WAL → chunk → 对象存储 → Meta，架构 §5 + 详细设计 §5）。
//!
//! 严格时序（C8 / §5.2）：
//! ① Schema 解析/演进（OCC，**必须在写对象存储之前**）
//! ② 写 WAL（Data，组提交 fsync）→ 数据已持久化，**可查**
//! ③ **chunk 吸收**（seal / spill / flush 计划统一由 `yuntun_chunk::ChunkStore` 决策）
//! ④ flush：chunk → 对象存储 → `CommitFiles` → `batch Committed`
//!
//! ADR-11：BatchState 不单独存储，由 WAL 事件流重建（顺序即因果）。
//!
//! ## 双阈值（架构 §5.2，不得合并）
//! - **可见性上界** = WAL fsync + 一个扫描周期（`scan_interval`）；
//! - **持久化上界** = `seal_time + max_flush_delay`（确定，无随机 jitter）。

pub mod accumulator;
pub mod flush;
pub mod pipeline;
pub mod schema_cache;
pub mod source;
pub mod timeutil;

pub use accumulator::{now_ms, window_of};
pub use flush::{
    abort_batch, flush_chunk, flush_chunk_with_id, payloads_to_batches, recommit_into_catalog,
    FlushDeps, FlushOutcome, LiveBatchTracker,
};
pub use pipeline::{Ingestor, IngestorConfig};
pub use schema_cache::SchemaCache;
pub use source::{IngestSource, Receipt};

/// chunk 层重导出：写入侧与查询侧共享同一套类型（避免上层重复依赖）。
pub use yuntun_chunk::{
    Chunk, ChunkData, ChunkId, ChunkKey, ChunkState, ChunkStore, ChunkStoreConfig, MemoryLedger,
    MemoryPartition, Pressure, SealPolicy, TableLiveness,
};
/// 分片读取接缝（store 层）：查询侧只依赖 `ShardReader`，换实现（进程内 / 远端）零改动。
pub use yuntun_store::{ShardId, ShardReader, ShardTier};

use yuntun_model::error::LakeError;

/// 幂等键处理矩阵（详细设计 §7.3.2，【关键】强制表未传键必须拒绝）。
/// 返回 `None` = 不去重直接写；`Some(key)` = 需去重写入。
pub fn resolve_idempotency(
    require_key: bool,
    key: Option<&str>,
) -> Result<Option<String>, LakeError> {
    match (require_key, key) {
        (true, Some(k)) => {
            yuntun_model::validate_idempotency_key(k)?;
            Ok(Some(k.to_string()))
        }
        // ⚠️ true 表未传幂等键必须【拒绝】而非静默降级，否则"强制"形同虚设（架构 §7.3.2）
        (true, None) => Err(LakeError::IdempotencyKeyRequired),
        (false, Some(k)) => {
            yuntun_model::validate_idempotency_key(k)?;
            Ok(Some(k.to_string()))
        }
        (false, None) => Ok(None),
    }
}

/// 把"一次请求的幂等键"派生为**该请求内第 `idx` 个批次**的键（§7.3 粒度）。
///
/// # 为什么必须有这一步
/// 幂等键标识的是**一次客户端请求**（一个 DoPut 流 / 一条语句），而一次请求
/// 可能产出**多个批次**（多 `FlightData` 消息、`INSERT ... SELECT` 的多结果批）。
/// 若这些批次共用同一个键，批次级预筛会把同流的第 2..N 批当成"第一批的重试"
/// —— **静默丢数据**。
///
/// 派生是**确定**的：同一请求重试得到同样的键序列，因此幂等是**逐批**成立的
/// —— 比"整流跳过"更正确：上次只成功了一部分时，重试只补缺失的批次，
/// 而不会因为第一批已存在就整条请求跳过。
///
/// 长度上限 256（[`yuntun_model::validate_idempotency_key`]）：超长时从**键主体**
/// 截断，后缀保留（后缀才是区分批次的部分）。
pub fn derive_batch_key(request_key: &str, idx: u64) -> String {
    const MAX_LEN: usize = 256;
    let suffix = format!("#{idx}");
    let room = MAX_LEN.saturating_sub(suffix.len());
    let base: String = request_key.chars().take(room).collect();
    format!("{base}{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotency_matrix() {
        // (true, true) => 正常去重
        assert_eq!(
            resolve_idempotency(true, Some("k1")).unwrap(),
            Some("k1".to_string())
        );
        // (true, false) => 拒绝
        assert!(matches!(
            resolve_idempotency(true, None),
            Err(LakeError::IdempotencyKeyRequired)
        ));
        // (false, true) => 客户端主动要求幂等，仍去重
        assert_eq!(
            resolve_idempotency(false, Some("k2")).unwrap(),
            Some("k2".to_string())
        );
        // (false, false) => 不去重
        assert_eq!(resolve_idempotency(false, None).unwrap(), None);
    }

    #[test]
    fn batch_key_derivation_is_deterministic_and_distinct() {
        // 同一请求内的多批次必须得到**不同**的键 —— 否则批次级预筛会把第 2..N 批
        // 当成"第一批的重试"而静默丢掉（多 FlightData / INSERT ... SELECT 都是这种形态）
        let a = derive_batch_key("req-1", 0);
        let b = derive_batch_key("req-1", 1);
        assert_eq!(a, "req-1#0");
        assert_ne!(a, b);

        // 重试：同一请求 + 同一下标 → 同一键（逐批幂等的前提）
        assert_eq!(derive_batch_key("req-1", 1), b);

        // 长度约束：派生后仍须通过校验，否则"强制幂等键"的表会被直接拒绝
        let long = "k".repeat(256);
        let derived = derive_batch_key(&long, 12345);
        assert!(derived.len() <= 256, "len={}", derived.len());
        assert!(yuntun_model::validate_idempotency_key(&derived).is_ok());
        assert!(derived.ends_with("#12345"), "批次区分后缀必须保留");
    }

    #[test]
    fn idempotency_key_too_long() {
        let long = "k".repeat(257);
        assert!(matches!(
            resolve_idempotency(true, Some(&long)),
            Err(LakeError::IdempotencyKeyTooLong)
        ));
    }
}
