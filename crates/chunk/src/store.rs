//! `ChunkStore`：chunk 注册表 + 内存账本 + seal/spill/flush 计划 + 读侧接缝。
//!
//! ## 三个职责，一处收敛
//!
//! | 职责 | 方法 | 关键约束 |
//! |---|---|---|
//! | **写入侧热缓冲** | [`ChunkStore::append`] | seal 策略统一在此（避免出现两套分组逻辑） |
//! | **读侧热数据视图** | [`ShardReader`] 实现 | 查询侧只依赖 `ShardReader`，换实现零改动 |
//! | **内存压力执行者** | [`ChunkStore::enforce_pressure`] | 背压阶梯三级（60/80/95%） |
//!
//! ## 并发与锁的约定
//! 所有磁盘 IO（spill 写、spill 读回）都在**锁外**完成：
//! 锁内只做"克隆 `Chunk` / 摘出候选"（`Chunk: Clone` 且克隆廉价），
//! 否则一次 spill 写盘会把查询侧的读路径整段阻塞。
//!
//! ## 相位偏移（架构 §5.3，替代随机 jitter）
//!
//! ```text
//! flush_deadline = seal_time + max_flush_delay              // 确定
//! actual_flush   = flush_deadline + hash(instance) % spread  // 相位分散，防惊群
//! ```
//!
//! 到期时间**确定可预测**（旧实现 `time_threshold(5s) + jitter(60s)` ≈ 65s 不可预测，
//! 会污染持久化上界）。

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use yuntun_model::error::LakeError;
use yuntun_model::wal_record::ddl_op;
use yuntun_store::{ShardId, ShardReader, ShardTier};

use crate::budget::{MemoryLedger, Pressure};
use crate::chunk::{
    batches_memory_bytes, Chunk, ChunkId, ChunkKey, ChunkState, TableLiveness,
};

/// seal / spill / flush 策略（架构 §2.2 / §5.2 / §5.3 / ADR-10）。
///
/// ## 时间维度：**窗口对齐**，不是"创建后 N 秒"（ADR-10 明文否决后者）
///
/// ADR-10：*"攒批窗口按整分钟对齐（**而非**'达到阈值后 5 秒'）……同时保持
/// **每窗口每 shard 最多 1 个文件**的小文件控制目标"*。
/// 因此时间维度的 seal 触发是 [`Chunk::window_end_ms`]（到达分钟窗口关闭），
/// `min_resident` 只作**最短驻留地板**，绝不单独作为 seal 时刻 ——
/// 否则低吞吐表会在一个窗口内产出十余个小文件，把 ADR-10 的小文件控制目标打回原形。
#[derive(Debug, Clone)]
pub struct SealPolicy {
    /// 行数阈值：让 RowGroup 一次成型（S1-10：50–100 万行量级）
    pub rows_threshold: usize,
    /// 字节阈值（内存口径）
    pub bytes_threshold: usize,
    /// **最短驻留地板**（秒）：创建后至少这么久才允许因窗口关闭而 seal。
    /// **不是** seal 时刻本身 —— seal 时刻由窗口关闭决定（ADR-10）。
    pub min_resident: Duration,
    /// seal → flush 的宽限期（架构 §5.2）：`flush_at = sealed_at + max_flush_delay (+ 相位)`
    pub max_flush_delay: Duration,
    /// **强制 seal + flush 的最大驻留时间**（S1-9：防慢写入流把 WAL 撑爆）
    pub max_resident: Duration,
    /// 确定性相位偏移上限（S2-9：`hash(instance) % phase_spread`，替代随机 jitter）
    pub phase_spread: Duration,
}

impl Default for SealPolicy {
    fn default() -> Self {
        Self {
            rows_threshold: 500_000,
            bytes_threshold: 128 * 1024 * 1024,
            min_resident: Duration::from_secs(5),
            max_flush_delay: Duration::from_secs(30),
            max_resident: Duration::from_secs(60),
            phase_spread: Duration::from_secs(5),
        }
    }
}

/// seal 触发原因（诊断用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealReason {
    /// 行数达阈值
    Rows,
    /// 字节数达阈值
    Bytes,
    /// **到达分钟窗口关闭**（ADR-10：整分钟对齐，保证每窗口每 shard 最多 1 个小文件）
    WindowClosed,
    /// `schema_version` 变化（架构 §2.4：文件内同一 schema）
    SchemaChanged,
}

/// [`ChunkStore::append`] 的结果。
#[derive(Debug, Clone)]
pub struct AppendOutcome {
    pub chunk_id: ChunkId,
    /// 本次 append 顺带 seal 掉的 chunk（含原因）
    pub sealed: Option<(ChunkId, SealReason)>,
    /// 当前背压水位
    pub pressure: Pressure,
}

/// 本轮需要执行的动作集合（由攒批循环按扫描周期调用，**不含 IO**）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlushPlan {
    /// 需要 seal 的 open chunk
    pub seal: Vec<ChunkId>,
    /// 需要 spill 的 chunk（内存压力）
    pub spill: Vec<ChunkId>,
    /// 已到期、需要 flush 的 chunk（sealed / spilled）
    pub flush: Vec<ChunkId>,
}

/// 背压执行动作（诊断 / 打点用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PressureAction {
    /// 水位 >= 80%：强制 seal open chunk
    ForcedSeal(ChunkId),
    /// 水位 >= 60%：卸载最老的 sealed chunk
    Spilled(ChunkId),
    /// 水位 >= 95%：后续 append 一律拒绝
    Rejecting,
}

/// `ChunkStore` 配置。
#[derive(Debug, Clone)]
pub struct ChunkStoreConfig {
    pub policy: SealPolicy,
    /// spill 目录（**必须本地磁盘**，I2）
    pub spill_dir: PathBuf,
    /// 实例标识：确定性 flush 相位的 hash 输入（standalone = `"standalone"`）
    pub instance_id: String,
    /// 初始 WAL segment 号（spill 头记录，运行期由 [`ChunkStore::set_wal_segment`] 更新）
    pub wal_segment: u64,
}

impl ChunkStoreConfig {
    pub fn new(spill_dir: impl Into<PathBuf>, instance_id: impl Into<String>) -> Self {
        Self {
            policy: SealPolicy::default(),
            spill_dir: spill_dir.into(),
            instance_id: instance_id.into(),
            wal_segment: 0,
        }
    }
}

/// 交给 flush 的 chunk 快照（锁外构造，含解码后的批次）。
///
/// `id` 为 `None` 表示**恢复路径**重新 flush 的批次（数据来自 WAL 重放，
/// 不对应任何在世的 chunk）。
#[derive(Debug, Clone)]
pub struct ChunkFlushInput {
    pub id: Option<ChunkId>,
    pub shard: ShardId,
    pub epoch: u64,
    pub schema_version: u64,
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub seqs: Vec<u64>,
    pub wal_seq_range: Range<u64>,
    pub rows: u64,
}

/// 观测/打点快照。
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkStoreStats {
    pub chunks: usize,
    pub open: usize,
    pub sealed: usize,
    pub spilled: usize,
    pub flushed: usize,
    /// 账本口径的内存占用（字节）
    pub resident_bytes: usize,
    pub ledger_limit: usize,
    pub pressure: Pressure,
}

#[derive(Debug, Default)]
struct Inner {
    liveness: HashMap<String, TableLiveness>,
    /// 每个 key 当前处于 `Open` 的 chunk
    open: HashMap<ChunkKey, ChunkId>,
    chunks: BTreeMap<ChunkId, Chunk>,
}

/// chunk 注册表与内存账本的唯一持有者（进程内共享）。
#[derive(Debug)]
pub struct ChunkStore {
    cfg: ChunkStoreConfig,
    ledger: Arc<MemoryLedger>,
    /// 变更计数：写入 / 提交 / DDL / 回收都会 +1，供查询侧做"提交驱动刷新"
    version: AtomicU64,
    next_id: AtomicU64,
    wal_segment: AtomicU64,
    inner: Mutex<Inner>,
}

impl ChunkStore {
    /// 构造：**启动时清理遗留 spill 文件**（见 [`Self::purge_leftover_spills`]）。
    pub fn new(cfg: ChunkStoreConfig, ledger: Arc<MemoryLedger>) -> Arc<Self> {
        let store = Arc::new(Self {
            cfg,
            ledger,
            version: AtomicU64::new(0),
            next_id: AtomicU64::new(0),
            wal_segment: AtomicU64::new(0),
            inner: Mutex::new(Inner::default()),
        });
        let purged = store.purge_leftover_spills();
        if purged > 0 {
            tracing::warn!(
                files = purged,
                dir = %store.cfg.spill_dir.display(),
                "purged leftover spill files from a previous run"
            );
        }
        store
    }

    /// 启动清理：删除上次运行遗留的 spill 文件，返回删除数量。
    ///
    /// 为什么必须清：spill 是**进程内**热数据的落盘副本，其存在意义只到本次进程退出为止
    /// （I1：权威始终是 WAL）。重启后 registry 为空，残留文件永远不会被任何 chunk 引用，
    /// 而崩溃重启循环会让它们无限堆积。
    ///
    /// 架构 §2.6 的"校验 WAL 引用一致则**复用**副本"是**后续优化**（需要把 chunk 与
    /// WAL 区间重新配对）：当前选择"丢弃重来"，正确性不受影响，代价是重启后重新编码。
    pub fn purge_leftover_spills(&self) -> usize {
        let Ok(entries) = std::fs::read_dir(&self.cfg.spill_dir) else {
            return 0;
        };
        let mut removed = 0;
        for e in entries.flatten() {
            let path = e.path();
            let is_spill = path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(".ipc.lz4") || n.ends_with(".ipc.lz4.tmp"));
            if is_spill && std::fs::remove_file(&path).is_ok() {
                removed += 1;
            }
        }
        removed
    }

    pub fn config(&self) -> &ChunkStoreConfig {
        &self.cfg
    }

    pub fn ledger(&self) -> &Arc<MemoryLedger> {
        &self.ledger
    }

    /// 当前背压水位（架构 §2.7）。
    pub fn pressure(&self) -> Pressure {
        self.ledger.pressure()
    }

    /// 变更计数（写入 / 提交 / DDL / 回收）。
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    /// 更新当前 WAL segment 号（spill 头记录 WAL 引用用）。
    pub fn set_wal_segment(&self, seg: u64) {
        self.wal_segment.store(seg, Ordering::SeqCst);
    }

    // ------------------------------------------------------------ 写入侧

    /// 追加已 fsync 的数据到 chunk（架构 §5 写入路径）。
    ///
    /// 语义要点：
    /// - **先入 chunk 再谈持久化**：数据此刻已对本节点可查（读己之写，可见性上界绑 WAL fsync）；
    /// - `schema_version` 变化 → 先 seal 当前 chunk 再开新 chunk（I5：文件内同一 schema）；
    /// - 内存越限**不拒写**：先卸载最老的 sealed chunk，再无条件记账（可见性优先，I1）；
    ///   只有水位达 95% 才返回 [`LakeError::ResourceExhausted`]（背压阶梯第三级）。
    pub fn append(
        &self,
        key: ChunkKey,
        schema_version: u64,
        schema: SchemaRef,
        seq: u64,
        batches: Vec<RecordBatch>,
        now_ms: u64,
    ) -> Result<AppendOutcome, LakeError> {
        if batches.is_empty() {
            return Err(LakeError::Other("chunk append: empty batch set".into()));
        }
        let pressure = self.pressure();
        if pressure.rejects_writes() {
            return Err(LakeError::ResourceExhausted(format!(
                "chunk memory {}/{} bytes ({:.0}%) — backpressure ladder rejected write",
                self.ledger.used(),
                self.ledger.limit(),
                self.ledger.ratio() * 100.0
            )));
        }

        let added = batches_memory_bytes(&batches);
        let mut sealed: Option<(ChunkId, SealReason)> = None;

        let id = {
            let mut inner = self.inner.lock().unwrap();

            // I5：schema 演进粒度 = 文件 —— 版本变化强制 seal 当前 chunk 并开新文件
            if let Some(prev) = inner.open.get(&key).copied() {
                let changed = inner
                    .chunks
                    .get(&prev)
                    .map(|c| c.schema_version != schema_version)
                    .unwrap_or(false);
                if changed {
                    if let Some(c) = inner.chunks.get_mut(&prev) {
                        c.seal(now_ms)?;
                    }
                    inner.open.remove(&key);
                    sealed = Some((prev, SealReason::SchemaChanged));
                }
            }

            let id = match inner.open.get(&key).copied() {
                Some(id) => id,
                None => {
                    let id = ChunkId(self.next_id.fetch_add(1, Ordering::SeqCst) + 1);
                    inner.chunks.insert(
                        id,
                        Chunk::new(id, key.clone(), schema_version, schema.clone(), now_ms),
                    );
                    inner.open.insert(key.clone(), id);
                    id
                }
            };

            let reason = {
                let chunk = inner
                    .chunks
                    .get_mut(&id)
                    .ok_or_else(|| LakeError::Other("chunk registry inconsistency".into()))?;
                for b in batches {
                    chunk.append(seq, b)?;
                }
                self.should_seal(chunk, now_ms)
            };

            if let Some(reason) = reason {
                inner.chunks.get_mut(&id).expect("checked above").seal(now_ms)?;
                inner.open.remove(&key);
                sealed = Some((id, reason));
            }
            id
        };

        // 内存账本：越限先卸载，再无条件记账（可见性承诺优先于预算，I1）
        if !self.ledger.try_reserve(added) {
            self.relieve_once();
            self.ledger.reserve(added);
        }
        self.version.fetch_add(1, Ordering::SeqCst);

        Ok(AppendOutcome {
            chunk_id: id,
            sealed,
            pressure: self.pressure(),
        })
    }

    fn should_seal(&self, c: &Chunk, now_ms: u64) -> Option<SealReason> {
        let p = &self.cfg.policy;
        if c.rows >= p.rows_threshold {
            return Some(SealReason::Rows);
        }
        if c.bytes >= p.bytes_threshold {
            return Some(SealReason::Bytes);
        }
        // ADR-10：时间维度看**窗口是否关闭**，而不是"创建后 N 秒"。
        // min_resident 只是地板（防单条记录即刻成文件）。
        if now_ms >= c.window_end_ms
            && now_ms.saturating_sub(c.created_at_ms) >= p.min_resident.as_millis() as u64
        {
            return Some(SealReason::WindowClosed);
        }
        None
    }

    /// 观察一条 DDL：维护表存活与世代（DROP 后重建同名表 → 新世代）。
    pub fn observe_ddl(&self, op: u32, table: &str) {
        let mut inner = self.inner.lock().unwrap();
        let e = inner.liveness.entry(table.to_string()).or_default();
        match op {
            ddl_op::CREATE_TABLE => {
                e.exists = true;
                e.epoch += 1;
            }
            ddl_op::DROP_TABLE => e.exists = false,
            // schema 事件不参与表世代（DROP SCHEMA 要求空库，无表）
            _ => return,
        }
        drop(inner);
        self.version.fetch_add(1, Ordering::SeqCst);
    }

    pub fn liveness(&self, table: &str) -> TableLiveness {
        self.inner
            .lock()
            .unwrap()
            .liveness
            .get(table)
            .copied()
            .unwrap_or_default()
    }

    /// 该世代是否已陈旧（表不存在，或世代已变 = 被 DROP 后重建）。
    pub fn is_stale(&self, table: &str, epoch: u64) -> bool {
        let l = self.liveness(table);
        !l.exists || l.epoch != epoch
    }

    fn open_id(&self, key: &ChunkKey) -> Option<ChunkId> {
        self.inner.lock().unwrap().open.get(key).copied()
    }

    // ------------------------------------------------------------ seal / spill

    /// seal 指定 chunk（幂等：已 seal 直接成功）。
    pub fn seal(&self, id: ChunkId, now_ms: u64) -> Result<(), LakeError> {
        let mut inner = self.inner.lock().unwrap();
        let key = match inner.chunks.get(&id) {
            Some(c) => c.key.clone(),
            None => return Ok(()),
        };
        let chunk = inner.chunks.get_mut(&id).expect("checked above");
        if chunk.state == ChunkState::Open {
            chunk.seal(now_ms)?;
        }
        if inner.open.get(&key).copied() == Some(id) {
            inner.open.remove(&key);
        }
        drop(inner);
        self.version.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// spill 指定 chunk（两阶段：锁内取副本 → 锁外写盘 → 锁内装回）。
    pub fn spill(&self, id: ChunkId) -> Result<(), LakeError> {
        // ① 锁内：确认可卸载 + 取出内存批次（Arc 克隆）
        let (chunk, batches, wal_segment) = {
            let inner = self.inner.lock().unwrap();
            let Some(c) = inner.chunks.get(&id) else {
                return Ok(());
            };
            if !c.needs_spill() {
                return Ok(());
            }
            (
                c.clone(),
                c.mem_batches()?,
                self.wal_segment.load(Ordering::SeqCst),
            )
        };

        // ② 锁外：写本地磁盘（不阻塞查询读路径）
        let handle = chunk.build_spill(&self.cfg.spill_dir, wal_segment, &batches)?;

        // ③ 锁内：装回句柄并释放账本
        {
            let mut inner = self.inner.lock().unwrap();
            match inner.chunks.get_mut(&id) {
                Some(c) => c.attach_spill(handle)?,
                None => {
                    // 期间被回收：删掉刚落盘的副本，避免垃圾文件
                    let _ = crate::spill::remove_spill(&handle);
                    return Ok(());
                }
            }
        }
        self.ledger.release(chunk.bytes);
        self.version.fetch_add(1, Ordering::SeqCst);
        tracing::debug!(chunk = %id, bytes = chunk.bytes, "chunk spilled to local disk");
        Ok(())
    }

    /// 卸载一个最老的可行 chunk（供越限时的即时缓解）。
    fn relieve_once(&self) -> bool {
        match self.oldest_spillable() {
            Some(id) => match self.spill(id) {
                Ok(()) => true,
                Err(e) => {
                    tracing::warn!(chunk = %id, error = %e, "chunk spill failed");
                    false
                }
            },
            None => false,
        }
    }

    fn oldest_spillable(&self) -> Option<ChunkId> {
        let inner = self.inner.lock().unwrap();
        inner
            .chunks
            .values()
            .find(|c| c.needs_spill())
            .map(|c| c.id)
    }

    /// 施加背压阶梯（架构 §2.7）。
    ///
    /// | 水位 | 动作 |
    /// |---|---|
    /// | >= 60% | 后台 spill 最老的 sealed chunk |
    /// | >= 80% | 强制 seal open chunk 并 spill |
    /// | >= 95% | 停止卸载，标记拒写（后续 append 返回 `RESOURCE_EXHAUSTED`） |
    pub fn enforce_pressure(&self, now_ms: u64) -> Vec<PressureAction> {
        let mut actions = Vec::new();
        let pressure = self.pressure();
        if !pressure.needs_spill() {
            return actions;
        }

        if pressure.needs_force_seal() {
            let open: Vec<ChunkId> = {
                let inner = self.inner.lock().unwrap();
                inner.open.values().copied().collect()
            };
            for id in open {
                // seal 已 sealed 的 chunk 幂等；失败只记日志（下一轮重试）
                if self.seal(id, now_ms).is_ok() {
                    actions.push(PressureAction::ForcedSeal(id));
                }
            }
        }

        // 反复卸载直到回到 Normal，或没有可卸载的对象
        for _ in 0..64 {
            if !self.pressure().needs_spill() {
                break;
            }
            let Some(id) = self.oldest_spillable() else {
                break;
            };
            match self.spill(id) {
                Ok(()) => actions.push(PressureAction::Spilled(id)),
                Err(e) => {
                    tracing::warn!(chunk = %id, error = %e, "pressure spill failed");
                    break;
                }
            }
        }

        if self.pressure().rejects_writes() {
            actions.push(PressureAction::Rejecting);
        }
        actions
    }

    // ------------------------------------------------------------ flush 计划

    /// 本轮需要 seal / spill / flush 的 chunk（纯决策，不含 IO）。
    ///
    /// 三条件并存，语义分明：
    /// - **正常 flush 到期**：`sealed_at + max_flush_delay + phase(instance)`（确定，架构 §5.3）；
    /// - **驻留硬兜底**（S1-9，防慢写入流把 WAL 撑爆）：open chunk 超 `max_resident` → 强制
    ///   seal **并同一轮 flush**（否则"强制"只剩一半，WAL 仍被拖住）；
    /// - 已 seal 超 `max_resident` 仍未 flush（如 S3 长时间失败重试）→ 强制 flush。
    ///
    /// ⚠️ **不变量**：`max_resident > max_flush_delay + phase_spread`。
    /// 否则硬兜底会早于正常到期时刻触发，**绕过相位分散** → 所有实例重新在同一秒 flush
    /// （ADR-10 的惊群问题复活）。配置侧应保证该关系（见 `Config` 校验）。
    pub fn plan_flush(&self, now_ms: u64) -> FlushPlan {
        let policy = &self.cfg.policy;
        let max_resident_ms = policy.max_resident.as_millis() as u64;
        let mut plan = FlushPlan::default();
        let inner = self.inner.lock().unwrap();
        for c in inner.chunks.values() {
            if matches!(c.state, ChunkState::Flushed | ChunkState::Released) {
                continue;
            }
            let open_too_long = now_ms.saturating_sub(c.created_at_ms) >= max_resident_ms;
            let sealed_too_long = c
                .sealed_at_ms
                .is_some_and(|s| now_ms.saturating_sub(s) >= max_resident_ms);

            let seal_due =
                c.state == ChunkState::Open && (open_too_long || self.should_seal(c, now_ms).is_some());
            if seal_due {
                plan.seal.push(c.id);
            }

            // 本轮会被 seal 的 chunk 也可同轮 flush（调用方顺序：seal → spill → flush）
            let flushable = matches!(c.state, ChunkState::Sealed | ChunkState::Spilled);
            let due = open_too_long || sealed_too_long || now_ms >= self.flush_due_at(c);
            if (flushable || seal_due) && !c.in_backoff(now_ms) && due {
                plan.flush.push(c.id);
            }
        }
        plan
    }

    /// flush 到期时刻（确定 + 相位分散，架构 §5.3）：`sealed_at + max_flush_delay + phase`。
    ///
    /// **锚点必须是 `sealed_at` 而不是 `created_at`**：窗口对齐 seal 会让 chunk 在窗口关闭时
    /// 才封口，若按创建时刻算到期，则"封口即到期" → 所有实例仍在同一秒 flush，
    /// 相位分散形同虚设。未 seal 的 chunk 不可 flush，返回 `u64::MAX`。
    pub fn flush_due_at(&self, c: &Chunk) -> u64 {
        match c.sealed_at_ms {
            Some(sealed) => {
                sealed
                    + self.cfg.policy.max_flush_delay.as_millis() as u64
                    + self.phase_offset_ms(&c.key)
            }
            None => u64::MAX,
        }
    }

    /// `hash(instance, key) % spread`：到期时刻确定，各实例仍分散（防惊群）。
    fn phase_offset_ms(&self, key: &ChunkKey) -> u64 {
        let spread = self.cfg.policy.phase_spread.as_millis() as u64;
        if spread == 0 {
            return 0;
        }
        let h = fnv1a_join(&[
            self.cfg.instance_id.as_bytes(),
            key.table().as_bytes(),
            key.shard.shard.as_bytes(),
            key.shard.window.as_bytes(),
        ]);
        h % spread
    }

    /// 取出待 flush 的 chunk 快照（锁内克隆、锁外读盘）。
    pub fn flush_input(&self, id: ChunkId) -> Result<Option<ChunkFlushInput>, LakeError> {
        let chunk = {
            let inner = self.inner.lock().unwrap();
            match inner.chunks.get(&id) {
                Some(c) if matches!(c.state, ChunkState::Sealed | ChunkState::Spilled) => c.clone(),
                _ => return Ok(None),
            }
        };
        let batches = chunk.read()?;
        Ok(Some(ChunkFlushInput {
            id: Some(chunk.id),
            shard: chunk.key.shard.clone(),
            epoch: chunk.key.epoch,
            schema_version: chunk.schema_version,
            schema: chunk.schema.clone(),
            batches,
            seqs: chunk.seqs.clone(),
            wal_seq_range: chunk.wal_seq_range.clone(),
            rows: chunk.rows as u64,
        }))
    }

    /// flush 成功 + `commit_files` 成功：标记 `Flushed`（**不释放数据**，I4）。
    pub fn mark_committed(&self, id: ChunkId, snapshot: u64) -> Result<(), LakeError> {
        {
            let mut inner = self.inner.lock().unwrap();
            match inner.chunks.get_mut(&id) {
                Some(c) => c.mark_committed(snapshot)?,
                None => return Ok(()),
            }
        }
        self.version.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// flush 失败：进入退避窗口（批次保持非终态，由 WAL 超时监控兜底）。
    pub fn note_flush_failure(&self, id: ChunkId, now_ms: u64) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(c) = inner.chunks.get_mut(&id) {
            c.note_flush_failure(now_ms);
        }
    }

    /// 丢弃 chunk（陈旧世代 / 回滚）：删本地副本并释放账本。
    pub fn discard_chunk(&self, id: ChunkId) -> bool {
        let (freed, spill_path) = {
            let mut inner = self.inner.lock().unwrap();
            let Some(c) = inner.chunks.remove(&id) else {
                return false;
            };
            let freed = if c.is_in_memory() { c.bytes } else { 0 };
            if inner.open.get(&c.key).copied() == Some(id) {
                inner.open.remove(&c.key);
            }
            let spill_path = match &c.data {
                crate::chunk::ChunkData::Spill(h) => Some(h.path.clone()),
                crate::chunk::ChunkData::Mem(_) => None,
            };
            (freed, spill_path)
        };
        if let Some(p) = spill_path {
            // 本地副本是节点私有加速品，删失败只留垃圾文件（I1：权威在 WAL）
            if let Err(e) = std::fs::remove_file(&p) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(path = %p.display(), error = %e, "remove discarded spill failed");
                }
            }
        }
        self.ledger.release(freed);
        self.version.fetch_add(1, Ordering::SeqCst);
        true
    }

    /// 回收：① 陈旧世代的 chunk（DROP / 重建同名表）；② 已被查询缓存追上的 `Flushed` chunk（I4）。
    ///
    /// 返回值 = 释放的字节数（账本口径）。
    pub fn reclaim(&self, cached_snapshot: u64) -> usize {
        let mut freed = 0usize;
        let mut touched = false;
        {
            let mut inner = self.inner.lock().unwrap();
            let live = inner.liveness.clone();
            let mut drop_ids: Vec<ChunkId> = Vec::new();
            for c in inner.chunks.values_mut() {
                let l = live.get(c.key.table()).copied().unwrap_or_default();
                if !l.exists || l.epoch != c.key.epoch {
                    // 陈旧世代：本地副本直接丢弃（其数据本就不该可见）
                    if c.is_in_memory() {
                        freed += c.bytes;
                    }
                    drop_ids.push(c.id);
                    continue;
                }
                let caught_up = c
                    .committed_snapshot
                    .is_some_and(|s| cached_snapshot >= s);
                if caught_up && c.state == ChunkState::Flushed {
                    if c.is_in_memory() {
                        freed += c.bytes;
                    }
                    drop_ids.push(c.id);
                }
            }
            for id in &drop_ids {
                if let Some(c) = inner.chunks.remove(id) {
                    if inner.open.get(&c.key).copied() == Some(*id) {
                        inner.open.remove(&c.key);
                    }
                    touched = true;
                }
            }
        }
        if freed > 0 {
            self.ledger.release(freed);
        }
        if touched {
            self.version.fetch_add(1, Ordering::SeqCst);
        }
        freed
    }

    // ------------------------------------------------------------ 读侧

    /// 可见的 chunk 快照（供查询路径；锁内克隆、锁外读盘）。
    fn visible_chunks(&self, table: Option<&str>, cached_snapshot: u64) -> Vec<Chunk> {
        let inner = self.inner.lock().unwrap();
        inner
            .chunks
            .values()
            .filter(|c| table.is_none_or(|t| c.key.table() == t))
            .filter(|c| {
                let l = inner
                    .liveness
                    .get(c.key.table())
                    .copied()
                    .unwrap_or_default();
                l.exists && l.epoch == c.key.epoch
            })
            .filter(|c| c.visible(cached_snapshot))
            .cloned()
            .collect()
    }

    /// 同步读入口（内部 / 单测）：返回该表对 `cached_snapshot` 尚不可见的热数据。
    pub fn read_table_sync(&self, table: &str, cached_snapshot: u64) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        for c in self.visible_chunks(Some(table), cached_snapshot) {
            match c.read() {
                Ok(b) => out.extend(b),
                Err(e) => tracing::warn!(chunk = %c.id, error = %e,
                    "chunk read failed; data remains protected by WAL replay"),
            }
        }
        out
    }

    /// 诊断快照。
    pub fn stats(&self) -> ChunkStoreStats {
        let inner = self.inner.lock().unwrap();
        let mut s = ChunkStoreStats {
            chunks: inner.chunks.len(),
            open: 0,
            sealed: 0,
            spilled: 0,
            flushed: 0,
            resident_bytes: self.ledger.used(),
            ledger_limit: self.ledger.limit(),
            pressure: self.pressure(),
        };
        for c in inner.chunks.values() {
            match c.state {
                ChunkState::Open => s.open += 1,
                ChunkState::Sealed => s.sealed += 1,
                ChunkState::Spilled => s.spilled += 1,
                ChunkState::Flushed => s.flushed += 1,
                ChunkState::Released => {}
            }
        }
        s
    }

    /// 当前存在的 chunk 数。
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().chunks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 某 key 的当前 open chunk（诊断 / 单测）。
    pub fn open_chunk_of(&self, key: &ChunkKey) -> Option<ChunkId> {
        self.open_id(key)
    }

    /// 某 chunk 的状态（诊断 / 单测）。
    pub fn chunk_state(&self, id: ChunkId) -> Option<ChunkState> {
        self.inner.lock().unwrap().chunks.get(&id).map(|c| c.state)
    }

    /// 某 key 下全部 chunk（诊断 / 单测）。
    pub fn chunk_ids_of(&self, key: &ChunkKey) -> Vec<ChunkId> {
        self.inner
            .lock()
            .unwrap()
            .chunks
            .values()
            .filter(|c| &c.key == key)
            .map(|c| c.id)
            .collect()
    }
}

#[async_trait]
impl ShardReader for ChunkStore {
    fn tier(&self) -> ShardTier {
        ShardTier::Memory
    }

    fn version(&self) -> u64 {
        ChunkStore::version(self)
    }

    async fn shards_of(&self, table: &str) -> Result<Vec<ShardId>, LakeError> {
        let inner = self.inner.lock().unwrap();
        let mut out: Vec<ShardId> = inner
            .chunks
            .values()
            .filter(|c| c.key.table() == table)
            .filter(|c| {
                let l = inner
                    .liveness
                    .get(c.key.table())
                    .copied()
                    .unwrap_or_default();
                l.exists && l.epoch == c.key.epoch
            })
            .map(|c| c.key.shard.clone())
            .collect();
        out.sort();
        out.dedup();
        Ok(out)
    }

    async fn read_shard(
        &self,
        id: &ShardId,
        cached_snapshot: u64,
    ) -> Result<Vec<RecordBatch>, LakeError> {
        let mut out = Vec::new();
        for c in self.visible_chunks(Some(&id.table), cached_snapshot) {
            if &c.key.shard != id {
                continue;
            }
            match c.read() {
                Ok(b) => out.extend(b),
                Err(e) => tracing::warn!(chunk = %c.id, error = %e, "chunk read failed"),
            }
        }
        Ok(out)
    }

    async fn read_table(
        &self,
        table: &str,
        cached_snapshot: u64,
    ) -> Result<Vec<RecordBatch>, LakeError> {
        Ok(self.read_table_sync(table, cached_snapshot))
    }

    fn reclaim(&self, cached_snapshot: u64) {
        ChunkStore::reclaim(self, cached_snapshot);
    }
}

/// FNV-1a 64 位哈希（拼接多段字节；确定性、无依赖）。
fn fnv1a_join(parts: &[&[u8]]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for p in parts {
        for b in p.iter() {
            h ^= *b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        // 分隔符：避免 ("ab","c") 与 ("a","bc") 撞哈希
        h ^= 0xff;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as SArc;

    fn schema() -> SchemaRef {
        SArc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]))
    }

    fn batch(vals: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![SArc::new(Int64Array::from(vals))]).unwrap()
    }

    fn key() -> ChunkKey {
        ChunkKey::new(
            ShardId::new("public.t", "default", "2026-09-15T10:00"),
            0,
        )
    }

    struct Fixture {
        store: Arc<ChunkStore>,
        dir: yuntun_testkit::TestDir,
    }

    fn fixture(name: &str, policy: SealPolicy, budget: usize) -> Fixture {
        let dir = yuntun_testkit::TestDir::tmpfs(name);
        let cfg = ChunkStoreConfig {
            policy,
            spill_dir: dir.join("spill"),
            instance_id: "node-a".into(),
            wal_segment: 0,
        };
        Fixture {
            store: ChunkStore::new(cfg, MemoryLedger::new("chunk", budget)),
            dir,
        }
    }

    fn tight_policy(rows: usize) -> SealPolicy {
        SealPolicy {
            rows_threshold: rows,
            bytes_threshold: usize::MAX,
            // 地板设得极长：默认只让行数阈值触发 seal（时间维度另有用例）
            min_resident: Duration::from_secs(3600),
            max_flush_delay: Duration::from_secs(30),
            max_resident: Duration::from_secs(60),
            phase_spread: Duration::ZERO,
        }
    }

    #[test]
    fn append_opens_chunk_and_seals_on_rows_threshold() {
        let f = fixture("chunk-rows", tight_policy(3), 1 << 20);
        let now = 1_000_000u64;

        let out = f
            .store
            .append(key(), 1, schema(), 0, vec![batch(vec![1, 2])], now)
            .unwrap();
        assert!(out.sealed.is_none(), "未达阈值不应 seal");
        assert_eq!(f.store.chunk_state(out.chunk_id), Some(ChunkState::Open));

        let out = f
            .store
            .append(key(), 1, schema(), 1, vec![batch(vec![3])], now)
            .unwrap();
        assert_eq!(
            out.sealed,
            Some((out.chunk_id, SealReason::Rows)),
            "达到行数阈值应 seal"
        );
        assert_eq!(f.store.chunk_state(out.chunk_id), Some(ChunkState::Sealed));
        // seal 后不再接受追加：同一 key 的下一次 append 会开新 chunk
        let out2 = f
            .store
            .append(key(), 1, schema(), 2, vec![batch(vec![4])], now)
            .unwrap();
        assert_ne!(out2.chunk_id, out.chunk_id);
    }

    #[test]
    fn schema_version_change_forces_seal_and_new_chunk() {
        // I5：文件内同一 schema —— 版本变化强 seal 当前文件
        let f = fixture("chunk-schema", tight_policy(usize::MAX), 1 << 20);
        let now = 1_000_000u64;
        let a = f
            .store
            .append(key(), 1, schema(), 0, vec![batch(vec![1])], now)
            .unwrap();
        let b = f
            .store
            .append(key(), 2, schema(), 1, vec![batch(vec![2])], now)
            .unwrap();
        assert_eq!(b.sealed, Some((a.chunk_id, SealReason::SchemaChanged)));
        assert_ne!(a.chunk_id, b.chunk_id);
        assert_eq!(f.store.chunk_state(a.chunk_id), Some(ChunkState::Sealed));
        assert_eq!(f.store.chunk_state(b.chunk_id), Some(ChunkState::Open));
    }

    #[test]
    fn time_seal_is_window_aligned_not_creation_offset() {
        // ADR-10：时间维度的 seal 触发是**整分钟窗口关闭**，而不是"创建后 N 秒"。
        // 若按后者，低吞吐表一个窗口会产出十余个小文件，ADR-10 的
        // "每窗口每 shard 最多 1 个文件" 直接失效。
        let mut p = tight_policy(usize::MAX);
        p.min_resident = Duration::from_secs(5);
        let f = fixture("chunk-window-seal", p, 1 << 20);

        let ws = crate::chunk::window_start_ms(1_752_000_000_000);
        let created = ws as u64 + 10_000; // 窗口内第 10 秒到达
        let window_end = ws as u64 + 60_000;

        // 窗口内持续写入：同窗口应始终只有 1 个 chunk（后续 append 落进同一个）
        for i in 0..5u64 {
            f.store
                .append(
                    key(),
                    1,
                    schema(),
                    i,
                    vec![batch(vec![1])],
                    created + i * 5_000,
                )
                .unwrap();
        }
        assert_eq!(f.store.chunk_ids_of(&key()).len(), 1, "同窗口不得裂成多个 chunk");

        // "创建后 5s"已过，但窗口未关闭 → 不得 seal
        assert!(
            f.store.plan_flush(created + 5_001).seal.is_empty(),
            "窗口未关闭不得 seal（ADR-10 否决'达到阈值后 N 秒'）"
        );
        // 窗口关闭 → seal；且此时"每窗口每 shard ≤1 文件"成立
        let plan = f.store.plan_flush(window_end);
        assert_eq!(plan.seal.len(), 1, "窗口关闭应 seal");
        assert!(plan.flush.is_empty(), "刚 seal 的 chunk 未到 flush 上界");
    }

    #[test]
    fn flush_deadline_is_deterministic_and_bounded() {
        let mut p = tight_policy(3);
        p.max_flush_delay = Duration::from_secs(30);
        let f = fixture("chunk-deadline", p, 1 << 20);
        let now = 1_000_000u64;
        let out = f
            .store
            .append(key(), 1, schema(), 0, vec![batch(vec![1, 2, 3])], now)
            .unwrap();
        assert!(out.sealed.is_some());

        let due = {
            let inner = f.store.inner.lock().unwrap();
            f.store.flush_due_at(inner.chunks.get(&out.chunk_id).unwrap())
        };
        assert_eq!(due, now + 30_000, "到期时间 = seal_time + max_flush_delay");

        // 到期前一轮不 flush，到期即 flush
        assert!(!f
            .store
            .plan_flush(due - 1)
            .flush
            .contains(&out.chunk_id));
        assert!(f.store.plan_flush(due).flush.contains(&out.chunk_id));
    }

    #[test]
    fn max_resident_forces_seal_and_flush_even_for_slow_stream() {
        // S1-9：慢写入流不得让 WAL 无限膨胀 —— max_resident 是硬兜底
        let mut p = tight_policy(usize::MAX);
        p.min_resident = Duration::from_secs(3600);
        p.max_flush_delay = Duration::from_secs(3600);
        p.max_resident = Duration::from_secs(60);
        let f = fixture("chunk-resident", p, 1 << 20);
        let out = f
            .store
            .append(key(), 1, schema(), 0, vec![batch(vec![1])], 0)
            .unwrap();

        assert!(f.store.plan_flush(59_999).seal.is_empty());
        let plan = f.store.plan_flush(60_000);
        assert!(plan.seal.contains(&out.chunk_id), "超驻留时间应强制 seal");
        assert!(
            plan.flush.contains(&out.chunk_id),
            "强制 seal 必须**同轮** flush，否则 WAL 仍被拖住（S1-9 只做了一半）"
        );
        // 已 sealed 且超驻留 → 持续强制 flush（如 S3 长时间失败重试）
        f.store.seal(out.chunk_id, 60_000).unwrap();
        assert!(f.store.plan_flush(60_001).flush.contains(&out.chunk_id));
    }

    #[test]
    fn spill_unloads_memory_but_data_stays_readable() {
        let f = fixture("chunk-spill", tight_policy(3), 1 << 20);
        let now = 1_000_000u64;
        let out = f
            .store
            .append(key(), 1, schema(), 0, vec![batch(vec![1, 2, 3])], now)
            .unwrap();
        let used_before = f.store.ledger().used();
        assert!(used_before > 0);

        f.store.spill(out.chunk_id).unwrap();
        assert_eq!(f.store.chunk_state(out.chunk_id), Some(ChunkState::Spilled));
        assert_eq!(f.store.ledger().used(), 0, "spill 后必须释放内存账本");

        // 可见性承诺不破：spill 后仍可查
        let rows: usize = f
            .store
            .read_table_sync("public.t", 0)
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 3);
    }

    #[test]
    fn committed_chunk_visible_until_cache_catches_up_then_reclaimed() {
        // I4：commit 成功后才允许释放；缓存追上之前必须仍可读（无可见性空洞）
        let f = fixture("chunk-release", tight_policy(3), 1 << 20);
        let now = 1_000_000u64;
        let out = f
            .store
            .append(key(), 1, schema(), 0, vec![batch(vec![1, 2, 3])], now)
            .unwrap();
        f.store.mark_committed(out.chunk_id, 7).unwrap();

        let rows = |snap| -> usize {
            f.store
                .read_table_sync("public.t", snap)
                .iter()
                .map(|b| b.num_rows())
                .sum()
        };
        assert_eq!(rows(6), 3, "缓存未追上（6 < 7）仍由 chunk 提供，避免空洞");
        assert_eq!(rows(7), 0, "缓存已到 7 → 交给 Manifest，避免重复计数");

        assert!(f.store.reclaim(7) > 0, "回收应释放驻留字节");
        assert!(f.store.is_empty(), "缓存追上后 chunk 应被回收");
        assert_eq!(f.store.ledger().used(), 0);
        assert!(f.store.plan_flush(1_000_000).flush.is_empty(), "已回收不得再 flush");
    }

    #[test]
    fn stale_epoch_chunks_are_hidden_and_reclaimed() {
        let f = fixture("chunk-epoch", tight_policy(3), 1 << 20);
        let now = 1_000_000u64;
        f.store.observe_ddl(ddl_op::CREATE_TABLE, "public.t");
        let epoch = f.store.liveness("public.t").epoch;
        let k = ChunkKey::new(key().shard, epoch);
        f.store
            .append(k.clone(), 1, schema(), 0, vec![batch(vec![1, 2, 3])], now)
            .unwrap();
        assert_eq!(f.store.read_table_sync("public.t", 0).len(), 1);

        // DROP → CREATE：世代 +1，老世代数据立刻不可见并被回收
        f.store.observe_ddl(ddl_op::DROP_TABLE, "public.t");
        f.store.observe_ddl(ddl_op::CREATE_TABLE, "public.t");
        assert!(f.store.is_stale("public.t", epoch));
        assert!(f.store.read_table_sync("public.t", 0).is_empty());
        f.store.reclaim(0);
        assert!(f.store.is_empty());
    }

    #[test]
    fn backpressure_ladder_spills_then_rejects() {
        // 小预算：一条 1 行批次就足以越限
        let p = tight_policy(usize::MAX);
        let f = fixture("chunk-ladder", p, 0);
        // 预算 0：任何 append 都会先尝试卸载（无可卸载）再记账 → 立刻到 Reject
        let out = f
            .store
            .append(key(), 1, schema(), 0, vec![batch(vec![1])], 0)
            .unwrap();
        assert_eq!(out.pressure, Pressure::Reject);
        assert_eq!(
            f.store.plan_flush(0).seal.len() + f.store.plan_flush(0).flush.len(),
            0,
            "未达阈值不回强行 seal"
        );

        let err = f
            .store
            .append(key(), 1, schema(), 1, vec![batch(vec![2])], 0)
            .unwrap_err();
        assert!(
            matches!(err, LakeError::ResourceExhausted(_)),
            "水位 >= 95% 必须明确拒绝而非静默降级: {err}"
        );
    }

    #[test]
    fn pressure_ladder_force_seals_then_spills_and_returns_to_normal() {
        // 预算 = 4 个批次的账面大小；写 3 次 → 75% → Hard（>= 80%? 0.75 < 0.80）
        // 用 3.5 个批次预算把水位推进 Hard 区间
        let unit = batches_memory_bytes(&[batch(vec![1])]);
        assert!(unit > 0, "账本口径必须有非零占用");
        let f = fixture("chunk-pressure", tight_policy(usize::MAX), unit * 3 + unit / 2);

        for i in 0..3u64 {
            f.store
                .append(key(), 1, schema(), i, vec![batch(vec![1])], 0)
                .unwrap();
        }
        assert_eq!(f.store.pressure(), Pressure::Hard);
        let chunk_id = f.store.open_chunk_of(&key()).unwrap();

        let actions = f.store.enforce_pressure(0);
        assert!(
            actions.contains(&PressureAction::ForcedSeal(chunk_id)),
            ">= 80% 应先强制 seal open chunk: {actions:?}"
        );
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, PressureAction::Spilled(_))),
            ">= 60% 应卸载 sealed chunk: {actions:?}"
        );
        assert_eq!(f.store.ledger().used(), 0);
        assert_eq!(f.store.pressure(), Pressure::Normal);
        // 卸载后数据仍可查（可见性承诺不破）
        let rows: usize = f
            .store
            .read_table_sync("public.t", 0)
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 3);
    }

    #[test]
    fn phase_offset_is_stable_and_spreads_across_instances() {
        let f = fixture("chunk-phase", SealPolicy::default(), 1 << 20);
        let k = key();
        let a = f.store.phase_offset_ms(&k);
        assert_eq!(a, f.store.phase_offset_ms(&k), "相位偏移必须确定可预测");
        assert!(a < 5_000, "相位偏移不得超出 max_flush_delay 之外的 spread");

        // 不同实例（节点）应分散
        let other = {
            let cfg = ChunkStoreConfig {
                instance_id: "node-b".into(),
                ..f.store.config().clone()
            };
            ChunkStore::new(cfg, MemoryLedger::new("chunk", 1 << 20))
        };
        let offsets: std::collections::HashSet<u64> = (0..16)
            .map(|i| {
                let k = ChunkKey::new(ShardId::new("public.t", format!("s{i}"), "w"), 0);
                f.store.phase_offset_ms(&k) ^ other.phase_offset_ms(&k)
            })
            .collect();
        assert!(offsets.len() > 1, "相位偏移应把不同实例分散开");
    }

    #[test]
    fn flush_input_reads_back_spilled_chunk() {
        let f = fixture("chunk-flush-input", tight_policy(3), 1 << 20);
        let out = f
            .store
            .append(key(), 7, schema(), 5, vec![batch(vec![1, 2, 3])], 0)
            .unwrap();
        f.store.spill(out.chunk_id).unwrap();

        let input = f.store.flush_input(out.chunk_id).unwrap().unwrap();
        assert_eq!(input.schema_version, 7);
        assert_eq!(input.rows, 3);
        assert_eq!(input.seqs, vec![5]);
        assert_eq!(input.wal_seq_range, 5..6);
        assert_eq!(input.shard.window, "2026-09-15T10:00");
        let rows: usize = input.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3, "spill 读回必须与内存一致");
    }

    #[test]
    fn store_exposes_shard_reader_seam() {
        // 查询侧只依赖 ShardReader：换实现零改动（本 crate 即实现之一）
        let f = fixture("chunk-seam", tight_policy(3), 1 << 20);
        f.store
            .append(key(), 1, schema(), 0, vec![batch(vec![1, 2, 3])], 0)
            .unwrap();
        let reader: Arc<dyn ShardReader> = f.store.clone();
        assert_eq!(reader.tier(), ShardTier::Memory);
        assert_ne!(reader.version(), 0, "变更计数用于提交驱动刷新");
        let ids = futures::executor::block_on(reader.shards_of("public.t")).unwrap();
        assert_eq!(ids.len(), 1);
        let total = futures::executor::block_on(reader.read_table("public.t", 0)).unwrap();
        assert_eq!(total.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    #[test]
    fn stats_report_states_and_pressure() {
        let f = fixture("chunk-stats", tight_policy(3), 1 << 20);
        f.store
            .append(key(), 1, schema(), 0, vec![batch(vec![1, 2, 3])], 0)
            .unwrap();
        let s = f.store.stats();
        assert_eq!(s.chunks, 1);
        assert_eq!(s.sealed, 1);
        assert_eq!(s.ledger_limit, 1 << 20);
        assert!(s.resident_bytes > 0);
    }

    #[test]
    fn dir_keeps_fixture_alive() {
        // 目录守卫必须在 store 存活期间有效（fixture 持有它）
        let f = fixture("chunk-dir", tight_policy(3), 1 << 20);
        assert!(f.dir.path().is_dir());
    }

    #[test]
    fn leftover_spill_files_are_purged_on_startup() {
        // 崩溃重启循环不得让 spill 文件无限堆积：残留副本永远不会被新进程引用（I1）
        let dir = yuntun_testkit::TestDir::tmpfs("chunk-purge");
        let spill_dir = dir.join("spill");
        std::fs::create_dir_all(&spill_dir).unwrap();
        for n in ["c1-abc.ipc.lz4", "c2-def.ipc.lz4", "c9-ghi.ipc.lz4.tmp"] {
            std::fs::write(spill_dir.join(n), b"junk").unwrap();
        }
        std::fs::write(spill_dir.join("keep.txt"), b"not a spill").unwrap();

        let cfg = ChunkStoreConfig {
            policy: SealPolicy::default(),
            spill_dir: spill_dir.clone(),
            instance_id: "node-a".into(),
            wal_segment: 0,
        };
        let store = ChunkStore::new(cfg, MemoryLedger::new("chunk", 1 << 20));
        assert_eq!(store.stats().chunks, 0);
        let left: Vec<String> = std::fs::read_dir(&spill_dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(left, vec!["keep.txt".to_string()], "只清 spill 副本，不动别的文件");
    }
}
