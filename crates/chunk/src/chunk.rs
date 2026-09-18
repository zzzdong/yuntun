//! Chunk 定义与生命周期状态机（架构 §2.1 / §2.2 / §2.4）。

use std::ops::Range;
use std::path::Path;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use yuntun_model::error::LakeError;
use yuntun_store::ShardId;

use crate::budget::Pressure;
use crate::spill::{self, SpillMeta};
// seal 原因定义在 store（与 `should_seal` 同一处，避免两处枚举漂移）
use crate::store::SealReason;
use crate::stats::ColumnStats;

/// 分钟毫秒（ADR-10：攒批窗口按整分钟对齐）。
pub const MINUTE_MS: i64 = 60_000;

/// 时间窗口起点（Unix 毫秒向下对齐到整分钟）。
pub fn window_start_ms(t: i64) -> i64 {
    t.div_euclid(MINUTE_MS) * MINUTE_MS
}

/// chunk 的进程内单调编号（重启后重新计数；跨进程唯一性由 [`ChunkKey`] 保证）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChunkId(pub u64);

impl std::fmt::Display for ChunkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "c{}", self.0)
    }
}

/// 表存活 + 世代（由 WAL DDL 记录派生）。
///
/// `epoch` = "该表被 CREATE 的累计次数"（按 WAL 顺序计数，重启重放后一致）。
/// 默认值 = 存活、世代 0 —— 兼容不经 WAL DDL 直接建表的调用方（测试 / 内部工具）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableLiveness {
    pub exists: bool,
    pub epoch: u64,
}

impl Default for TableLiveness {
    fn default() -> Self {
        Self {
            exists: true,
            epoch: 0,
        }
    }
}

/// chunk 归属键：`(表, shard, 时间窗口, 表世代)`。
///
/// 与 `IngestBatch.shard_key` / 对象路径 `dt=<window>/shard=<shard>` /
/// 攒批分组键一一对应；`epoch` 让"DROP 后重建同名表"的老世代数据不与新世代同组。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChunkKey {
    pub shard: ShardId,
    pub epoch: u64,
}

impl ChunkKey {
    pub fn new(shard: ShardId, epoch: u64) -> Self {
        Self { shard, epoch }
    }

    pub fn table(&self) -> &str {
        &self.shard.table
    }

    /// 逻辑分区键（架构 §2.3：**partition 是逻辑身份，file 是物理身份**）。
    pub fn partition_key(&self) -> PartitionKey {
        PartitionKey {
            dt: self.shard.window.clone(),
            extra: None,
        }
    }
}

/// 逻辑分区键：`dt` + 预留维度（架构 §2.1 `partition_key: (Dt, /* 预留 */)`）。
///
/// compaction 会合并文件；若把 partition 与 file 等同，每次合并后 partition 集合都会变化，
/// 因此分区身份独立于物理文件，由 `FileManifest.partition_key` 记录（S4-6）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionKey {
    /// 时间分区 `dt`（整分钟窗口，如 `2026-09-15T10:03`）
    pub dt: String,
    /// 预留维度（当前恒为 `None`；需要时启用，不改上层语义）
    pub extra: Option<String>,
}

/// 生命周期状态（架构 §2.2）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChunkState {
    /// 可继续追加
    Open,
    /// 已封口（schema / RowGroup 边界固定），可继续查
    Sealed,
    /// 已卸载到本地磁盘（数据仍在，读回时校验 CRC）
    Spilled,
    /// 已写对象存储且 `commit_files` 成功（**数据仍保留**，直到查询缓存追上，I4）
    Flushed,
    /// 已释放（终态：内存 / 磁盘副本已丢弃，查询改由 Manifest 覆盖）
    Released,
}

impl ChunkState {
    /// 状态转移合法性（架构 §2.2 状态机）。
    pub fn can_transition_to(self, next: ChunkState) -> bool {
        use ChunkState::*;
        matches!(
            (self, next),
            // 强制路径：内存压力下"一步到位"seal + spill
            (Open, Spilled)
                | (Open, Sealed)
                | (Sealed, Spilled)
                | (Sealed, Flushed)
                | (Spilled, Flushed)
                | (Flushed, Released)
        )
    }

    /// 是否仍持有数据副本（Flushed 也持有：查询缓存还没追上）。
    pub fn holds_data(self) -> bool {
        matches!(
            self,
            ChunkState::Open | ChunkState::Sealed | ChunkState::Spilled | ChunkState::Flushed
        )
    }
}

impl std::fmt::Display for ChunkState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ChunkState::Open => "open",
            ChunkState::Sealed => "sealed",
            ChunkState::Spilled => "spilled",
            ChunkState::Flushed => "flushed",
            ChunkState::Released => "released",
        };
        f.write_str(s)
    }
}

/// spill 落盘句柄（架构 §2.5 / §2.6）。
///
/// `wal_segment` + `wal_seq_range` 构成 spill 头记录的 **WAL 引用**：恢复重放时校验一致
/// 才能复用，否则丢弃重来。架构文档写作 `offset_range`，本仓库 WAL 对外暴露的单调坐标是
/// 记录序列号（`seq`），没有公开的字节偏移 API，故以 `seq` 区间作为等价坐标。
#[derive(Debug, Clone)]
pub struct SpillHandle {
    pub path: std::path::PathBuf,
    /// 落盘字节数（磁盘账本）
    pub file_bytes: u64,
    /// 原始内存占用估算（内存账本口径：spill 后仍按原值记账，水位判定才有意义）
    pub mem_bytes: usize,
    pub rows: usize,
    pub wal_segment: u64,
    pub wal_seq_range: Range<u64>,
    /// 载荷 CRC32（校验失败 = 副本不可信 → 丢弃重来）
    pub crc: u32,
}

/// chunk 的数据驻留形态（架构 §2.1）。
#[derive(Debug, Clone)]
pub enum ChunkData {
    /// Open / Sealed：直接持有 Arrow 批次（**不用 RowGroup 布局**，见 crate 文档）
    Mem(Vec<RecordBatch>),
    /// Spilled：本地磁盘上的 Arrow IPC(LZ4) 副本
    Spill(SpillHandle),
}

/// 内存中尚未成型的 RowGroup（架构 §2.1）。
///
/// `Clone` 是**廉价**的（`RecordBatch` / `SchemaRef` 内部均为 `Arc`），
/// 因此 [`crate::ChunkStore`] 可以"锁内克隆、锁外做磁盘 IO"，不阻塞查询侧。
#[derive(Debug, Clone)]
pub struct Chunk {
    pub id: ChunkId,
    pub key: ChunkKey,
    /// 该 chunk 的 schema 版本（文件内同一 schema，架构 §2.4）
    pub schema_version: u64,
    /// 建 chunk 时的表 schema（spill 头 / flush 对齐用）
    pub schema: SchemaRef,
    pub state: ChunkState,
    pub data: ChunkData,
    pub rows: usize,
    /// 内存账本：批次驻留内存时的字节数（spill 后保留原值，供水位判定）
    pub bytes: usize,
    /// chunk 级 min/max/null_count（块级跳过）
    pub stats: ColumnStats,
    pub created_at_ms: u64,
    /// 所属**到达分钟窗口**的结束时刻（Unix 毫秒）。
    ///
    /// ADR-10 的落点：时间维度的 seal 触发是**窗口关闭**，不是"创建后 N 秒"。
    /// 锚点取**到达时间**所在分钟（与旧实现的 `window_start + jitter` 同源）——
    /// 若锚事件时间，客户端回补历史数据时窗口早已关闭，会退化为"每条一批"。
    pub window_end_ms: u64,
    pub sealed_at_ms: Option<u64>,
    /// 封口的**原因**（`operation-log §34.3`：不落它就只能靠排除法推断"为什么文件这么小"）。
    /// 由 `ChunkStore` 在 seal 时写入，随 `ChunkFlushInput` 进 `FileManifest`。
    pub seal_reason: Option<SealReason>,
    /// 封口**瞬间的内存水位档位**：区分"阈值/窗口触发"与"内存压力触发"的唯一硬证据。
    pub pressure_at_seal: Option<Pressure>,
    /// 覆盖的 WAL seq（升序；组内直写时无空洞，交错写入下可能跳号）
    pub seqs: Vec<u64>,
    /// 覆盖的 WAL seq 半开区间 `[start, end)`（WAL 回收点语义用，S1-8）
    pub wal_seq_range: Range<u64>,
    /// `commit_files` 成功后的 manifest 快照号；`None` = 未提交
    pub committed_snapshot: Option<u64>,
    /// flush 连续失败次数（指数退避用；失败后批次保持非终态，由 WAL 超时监控兜底）
    pub flush_attempts: u32,
    /// 下次允许再次尝试 flush 的时刻（退避窗口内不重复打 S3，避免失败风暴）
    pub retry_after_ms: u64,
}

impl Chunk {
    /// 新建一个 Open chunk（schema 版本 / schema 在建 chunk 时确定，架构 §2.4）。
    pub fn new(
        id: ChunkId,
        key: ChunkKey,
        schema_version: u64,
        schema: SchemaRef,
        now_ms: u64,
    ) -> Self {
        Self {
            id,
            key,
            schema_version,
            schema,
            state: ChunkState::Open,
            data: ChunkData::Mem(Vec::new()),
            rows: 0,
            bytes: 0,
            stats: ColumnStats::default(),
            created_at_ms: now_ms,
            window_end_ms: (window_start_ms(now_ms as i64) + MINUTE_MS) as u64,
            sealed_at_ms: None,
            seal_reason: None,
            pressure_at_seal: None,
            seqs: Vec::new(),
            wal_seq_range: 0..0,
            committed_snapshot: None,
            flush_attempts: 0,
            retry_after_ms: 0,
        }
    }

    /// 追加一批数据（仅 `Open` 可追加）。
    ///
    /// 调用方（[`crate::ChunkStore`]）负责在 `schema_version` 变化时先 seal 并开新 chunk。
    pub fn append(&mut self, seq: u64, batch: RecordBatch) -> Result<(), LakeError> {
        if self.state != ChunkState::Open {
            return Err(LakeError::Other(format!(
                "chunk {} is {}, cannot append",
                self.id, self.state
            )));
        }
        if batch.schema() != self.schema {
            return Err(LakeError::Other(format!(
                "chunk {} schema mismatch: batch={:?} chunk={:?}",
                self.id,
                batch.schema().fields(),
                self.schema.fields()
            )));
        }
        self.rows += batch.num_rows();
        self.bytes += batch_memory_bytes(&batch);
        self.stats.merge(&ColumnStats::from_batch(&batch));
        if self.seqs.is_empty() {
            self.wal_seq_range = seq..seq + 1;
        } else {
            let start = self.wal_seq_range.start.min(seq);
            let end = self.wal_seq_range.end.max(seq + 1);
            self.wal_seq_range = start..end;
        }
        self.seqs.push(seq);
        match &mut self.data {
            ChunkData::Mem(batches) => batches.push(batch),
            ChunkData::Spill(_) => {
                return Err(LakeError::Other(format!(
                    "chunk {} is spilled, cannot append",
                    self.id
                )))
            }
        }
        Ok(())
    }

    /// seal：封口（行数 / 字节 / 时间 / schema 变化触发）。
    pub fn seal(&mut self, now_ms: u64) -> Result<(), LakeError> {
        self.seal_tagged(SealReason::Manual, None, now_ms)
    }

    /// 带**原因**与**封口时水位档位**的 seal（可观测性的落点，`operation-log §34.3`）。
    ///
    /// `reason` 必须由调用方给出：只有调用方知道是 `should_seal` 的哪个分支、
    /// 还是 `enforce_pressure`/`max_resident` 兜底 —— 这三者对"文件为什么这么小"
    /// 的含义完全不同（阈值=设计内，压力=水位顶掉削峰与窗口承诺）。
    pub fn seal_tagged(
        &mut self,
        reason: SealReason,
        pressure: Option<Pressure>,
        now_ms: u64,
    ) -> Result<(), LakeError> {
        self.transition(ChunkState::Sealed)?;
        self.sealed_at_ms = Some(now_ms);
        self.seal_reason = Some(reason);
        self.pressure_at_seal = pressure;
        Ok(())
    }

    /// spill：把内存批次卸载到本地磁盘（Arrow IPC + LZ4）。
    ///
    /// **只落本地磁盘**（I2）：spill 是节点私有状态，与 WAL 同级；卸载内存压力走网络更慢。
    /// spill 后数据仍可读（[`Chunk::read`] 自动读回），可见性承诺不破。
    ///
    /// ⚠️ 本方法含本地磁盘 IO。`ChunkStore` 走的是**两阶段**（先在锁外写盘、再装回句柄），
    /// 避免持锁做 IO 阻塞查询侧；本方法供单测 / 无需并发的场合使用。
    pub fn spill(&mut self, dir: &Path, wal_segment: u64) -> Result<(), LakeError> {
        if self.state == ChunkState::Spilled {
            return Ok(());
        }
        let batches = match std::mem::replace(&mut self.data, ChunkData::Mem(Vec::new())) {
            ChunkData::Mem(b) => b,
            ChunkData::Spill(h) => {
                self.data = ChunkData::Spill(h);
                return Ok(());
            }
        };
        match self.build_spill(dir, wal_segment, &batches) {
            Ok(handle) => {
                self.attach_spill(handle)?;
                Ok(())
            }
            Err(e) => {
                // 写盘失败：批次仍在内存里，退回原形态（不丢数据，下轮重试）
                self.data = ChunkData::Mem(batches);
                Err(e)
            }
        }
    }

    /// 生成 spill 文件（**不含状态转移**）：供 [`crate::ChunkStore`] 在锁外执行 IO。
    pub fn build_spill(
        &self,
        dir: &Path,
        wal_segment: u64,
        batches: &[RecordBatch],
    ) -> Result<SpillHandle, LakeError> {
        spill::write_spill(
            dir,
            &format!("{}-{}", self.id, uuid::Uuid::now_v7()),
            batches,
            SpillMeta {
                wal_segment,
                wal_seq_range: self.wal_seq_range.clone(),
                rows: self.rows,
                mem_bytes: self.bytes,
            },
        )
    }

    /// 取出内存中的批次（`Arc` 克隆，廉价），供锁外写盘。
    pub fn mem_batches(&self) -> Result<Vec<RecordBatch>, LakeError> {
        match &self.data {
            ChunkData::Mem(b) => Ok(b.clone()),
            ChunkData::Spill(_) => {
                Err(LakeError::Other(format!("chunk {} already spilled", self.id)))
            }
        }
    }

    /// 装回 spill 句柄：状态转 `Spilled`，内存批次丢弃。
    pub fn attach_spill(&mut self, handle: SpillHandle) -> Result<(), LakeError> {
        self.transition(ChunkState::Spilled)?;
        self.data = ChunkData::Spill(handle);
        Ok(())
    }

    /// `commit_files` 成功后标记（进入 `Flushed`）。
    ///
    /// ⚠️ **不得**在此释放数据：查询缓存可能还没刷新到该快照，
    /// 立即释放会出现可见性空洞（架构 §4.5 / I4）。
    pub fn mark_committed(&mut self, snapshot: u64) -> Result<(), LakeError> {
        if self.state != ChunkState::Flushed {
            self.transition(ChunkState::Flushed)?;
        }
        self.committed_snapshot = Some(snapshot);
        self.flush_attempts = 0;
        self.retry_after_ms = 0;
        Ok(())
    }

    /// flush 失败：记录退避窗口（批次保持非终态，由 WAL 超时监控兜底）。
    pub fn note_flush_failure(&mut self, now_ms: u64) {
        self.flush_attempts = self.flush_attempts.saturating_add(1);
        let backoff_ms = (200u64 << self.flush_attempts.min(7)).min(30_000);
        self.retry_after_ms = now_ms + backoff_ms;
    }

    /// 是否仍在 flush 失败退避窗口内。
    pub fn in_backoff(&self, now_ms: u64) -> bool {
        now_ms < self.retry_after_ms
    }

    /// 释放：丢弃本地副本（查询改由 Manifest 覆盖）。**仅在缓存追上后才可调用**（I4）。
    pub fn release(&mut self) -> Result<(), LakeError> {
        self.transition(ChunkState::Released)?;
        if let ChunkData::Spill(h) = &self.data {
            // 释放即删除落盘副本（失败不影响正确性，只留垃圾文件）
            if let Err(e) = spill::remove_spill(h) {
                tracing::warn!(chunk = %self.id, path = %h.path.display(), error = %e,
                    "remove spill file failed");
            }
        }
        self.data = ChunkData::Mem(Vec::new());
        Ok(())
    }

    /// 状态转移（非法转移直接报错，避免静默错状态）。
    pub fn transition(&mut self, next: ChunkState) -> Result<(), LakeError> {
        if !self.state.can_transition_to(next) {
            return Err(LakeError::Other(format!(
                "illegal chunk state transition {} -> {} (chunk {})",
                self.state, next, self.id
            )));
        }
        self.state = next;
        Ok(())
    }

    /// 读取数据（`Mem` 直接克隆 `RecordBatch` —— 内部是 `Arc`，克隆廉价；`Spill` 校验 CRC 后解码）。
    pub fn read(&self) -> Result<Vec<RecordBatch>, LakeError> {
        match &self.data {
            ChunkData::Mem(b) => Ok(b.clone()),
            ChunkData::Spill(h) => spill::read_spill(h),
        }
    }

    /// 查询可见性（架构 §4.5 交接语义）：
    /// - 未提交（`committed_snapshot == None`）→ 恒可见；
    /// - 已提交 → 查询缓存追上该快照前仍可见，之后交给 Manifest（避免重复计数）。
    pub fn visible(&self, cached_snapshot: u64) -> bool {
        if !self.state.holds_data() {
            return false;
        }
        match self.committed_snapshot {
            None => true,
            Some(s) => cached_snapshot < s,
        }
    }

    /// 是否需要 spill：**只有已 seal 的 chunk 才可卸载**。
    ///
    /// `Open` 的 chunk 还在增长，卸载它会让后续 append 无处可去（架构 §2.7 也只说
    /// "后台 spill 最老的 **sealed** chunk"）。内存压力下的 `Open` 由背压阶梯先强制 seal。
    pub fn needs_spill(&self) -> bool {
        self.state == ChunkState::Sealed && matches!(self.data, ChunkData::Mem(_))
    }

    /// 当前驻留形态是否为内存。
    pub fn is_in_memory(&self) -> bool {
        matches!(self.data, ChunkData::Mem(_))
    }

    /// 是否覆盖某个 WAL seq。
    pub fn covers_seq(&self, seq: u64) -> bool {
        self.seqs.contains(&seq)
    }
}

/// `RecordBatch` 驻留内存的字节数估算（内存账本口径）。
///
/// 用 Arrow 自带的 `get_array_memory_size`：含 buffer 容量与 null bitmap，
/// 正是"账本"该关心的量。**不用 `num_rows * width` 估算**——可变长列会严重低估。
pub fn batch_memory_bytes(batch: &RecordBatch) -> usize {
    batch.get_array_memory_size()
}

/// `RecordBatch` 序列的内存占用合计。
pub fn batches_memory_bytes(batches: &[RecordBatch]) -> usize {
    batches.iter().map(batch_memory_bytes).sum()
}
