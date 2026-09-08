//! 自实现 WAL（详细设计 §4 / 架构 §5.3 / ADR-11）。
//!
//! 仅需 append-only + 顺序 replay，fjall 等通用 KV 的能力全部用不上
//! （MemTable / SSTable / Compaction / 随机读）—— 多余能力即负担（ADR-11）。
//!
//! 关键设计（不可违背的编译期契约）：
//! - **C2**：攒批线程只读 `< synced_seq`（已 fsync）的 WAL 数据（§5.3.5.1）
//! - **C3**：CRC 覆盖 `type + payload`，不覆盖 `length`；CRC 校验边界 = fsync 边界，
//!   因此 `synced_seq` 无需持久化，由 replay 到 CRC 失败处自然确定（§4.5 / 附录 H.3）
//! - **C4**：`BatchAbort` 属于终态，包含 Abort 批次的 segment 可安全删除（§5.3.6.1）
//!
//! ## offset 语义说明
//! 本实现中 `synced_seq` / `scan_range(from, to)` 的"偏移"使用 **记录序号 seq**
//! （每 shard 单调递增的记录计数），而非字节偏移。这与 `BatchPending.wal_seq_range`
//! 的语义一致，且跨 segment 连续，避免字节偏移在 segment 轮转处的歧义。

pub mod cleanup;
pub mod config;
pub mod reader;
pub mod recovery;
pub mod segment;
pub mod writer;

pub use config::WalConfig;
pub use reader::WalReader;
pub use recovery::{recover, Recovery, SegmentInfo};
pub use writer::{WalAck, WalWriter};
