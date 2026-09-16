//! Chunk 层：数据平面核心（架构 `docs/architecture-with-chunk.md` §2）。
//!
//! **chunk ≡ 尚未成型的 RowGroup**：内存中是一组 `RecordBatch`，落对象存储后成为
//! Parquet 中的一个 RowGroup。本 crate 负责回答"数据在哪活着"这条边界，不负责
//! 编码写对象存储（那是 `yuntun-format` + flush 的职责）。
//!
//! ## 为什么内存不用 RowGroup 布局（架构 §2.1）
//! DoPut 入口本就是 Arrow 批次，转成 RowGroup 是纯亏的复制；RowGroup 512MB 量级也不适合
//! 做内存管理单元。因此内存侧直接持有 `RecordBatch`，落盘（spill / flush）时才编码。
//!
//! ## 实现时必须守住的不变量
//!
//! | # | 不变量 | 出处 | 违反后果 |
//! |---|---|---|---|
//! | I1 | **WAL 是唯一真相源**，spill 只是可丢弃的加速副本 | §2.6 | 长出对账类 bug |
//! | I2 | **spill 只落本地磁盘**（mmap / page cache 只属于本地层） | §2.5 | 走网络卸载内存压力，更慢 |
//! | I3 | **内存硬分区**：chunk 区与 query 执行区互不抢占 | §2.8 | 大基数 GROUP BY 挤光 chunk 内存压垮写入 |
//! | I4 | **commit 成功后才允许释放 chunk** | §4.5 | 数据"两头都没有"（可见性空洞） |
//! | I5 | **文件内同一 schema**：`schema_version` 变化强制 seal | §2.4 | 文件内混 schema，查询侧无法兜底 |
//!
//! ## 状态机（架构 §2.2）
//!
//! ```text
//! Open --seal--> Sealed --spill--> Spilled --+
//!   |              |                          | flush → commit_files
//!   +--------------+--------------------------+        → Flushed → Released
//! ```
//!
//! - **seal**：行数 / 字节数 / 时间阈值 / `schema_version` 变化；
//! - **spill**：内存压力下的卸载动作，spill 后仍可查（可见性承诺不破）；
//! - **flush**：编码写对象存储 + `commit_files`；flush 到期时间**确定**（`seal_time +
//!   max_flush_delay`），相位偏移由 `hash(instance) % spread` 分散（架构 §5.3）；
//! - **release**：`commit_files` 成功且查询缓存追上该快照后才释放（I4）。

pub mod budget;
pub mod chunk;
pub mod spill;
pub mod stats;
pub mod store;

pub use budget::{MemoryLedger, MemoryPartition, Pressure, PressureThresholds};
pub use chunk::{
    batch_memory_bytes, Chunk, ChunkData, ChunkId, ChunkKey, ChunkState, PartitionKey, SpillHandle,
    TableLiveness,
};
pub use spill::SpillMeta;
pub use stats::{ColumnStat, ColumnStats, StatValue};
pub use store::{
    AppendOutcome, ChunkStore, ChunkStoreConfig, FlushPlan, PressureAction, SealPolicy, SealReason,
};
