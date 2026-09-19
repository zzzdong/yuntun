//! metanode 的 raft `Storage` 实现（S3-1b；S3-3 会把它换成 fjall 落盘版）。
//!
//! # 为什么不能用 raft-rs 自带的 `MemStorage`
//!
//! 两个致命原因（都**不会**报错，只会静默不一致）：
//!
//! 1. **`MemStorageCore::snapshot()` 的 data 是空的**，元数据取自 `hard_state.commit`。
//!    它只适合"日志不截断"的玩具场景：一旦 follower 需要靠快照追赶，它会把
//!    **空 data** 的快照发出去 —— 装了就等于把 follower 的状态机清空。
//! 2. 它的 `compact()` 只丢日志、**不管状态机**。快照内容必须来自状态机（`CatalogState`），
//!    这是 `Storage` 实现方（我们）的责任，`MemStorage` 无从知道。
//!
//! # raft 索引的归属：**副本层所有**，不是状态机的 op 计数
//!
//! 快照/压缩的坐标必须与 raft 日志**同坐标**。而 [`CatalogState::last_applied`] 现在
//! 记的是"已 apply 的 **op 数**"—— 两者不等价：raft 日志里 **no-op**（新 leader 就位会写一条
//! 空条目）与 **ConfChange** 都占索引，于是"op 数"每遇到一条非 op 条目就与日志位置**错开一格**。
//!
//! 所以本轮把已应用位置放在**副本层**（本结构的 `applied_index`，由驱动方按 `entry.index` 设置），
//! 而不是读状态机的计数。用 op 计数当坐标的后果都是**静默**的：压缩位置偏小时快照元数据与内容
//! 不一致（follower 重复应用或跳过条目）、`advance_apply_to` 拿到错索引而重放。
//!
//! > 遗留（S3-3）：`CatalogState::last_applied` 作为**读栅栏**（`read_index()`）必须可比于
//! > raft 索引，届时要么把它也改成按 `entry.index` 设置、要么让读栅栏走副本层。
//! > 现在的状态机自增语义属 standalone 路径，不能直接搬到 metanode 上。
//!
//! # 本实现的核心不变量
//!
//! > **`artifact` 必须与 `compacted_index` 严格对应**：它必须是"应用到 `compacted_index`
//! > 那一刻"的状态机产物。
//!
//! 违反它的两种写法都会静默出错：
//!
//! | 错误写法 | 后果 |
//! |---|---|
//! | `snapshot()` 里**现场**从当前状态机取产物 | 元数据说"状态停在 index I"，内容却是 index J>I 的状态 → follower 装上后**把 I..J 的 op 当成没做过**（丢了）或**再 apply 一次**（重复） |
//! | 先 `compact` 再取产物 | 取到的产物已经含 `compacted_index` 之后的 op（同上） |
//!
//! 正确做法见 [`MetaStorage::compact_applied`]：**在应用线程里、先取产物、再截日志**。
//! 单测 `snapshot_data_matches_compacted_index_not_current_state` 专门钉住这一条。
//!
//! # 与 S3-3 的关系
//!
//! 内部状态换成 fjall 表后，本文件的结构（硬状态 / 日志 / 压缩位置 / 产物 / 严格对应关系）
//! **一行都不用改** —— 换的只是 `MetaInner` 的落盘方式。这也说明 S3-1b 与 S3-3 共用同一份设计。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::storage::{GetEntriesContext, RaftState, Storage};
// `StorageError` 在 raft 根模块重导出（`raft::storage` 里那个是私有 import）
use raft::StorageError;
use raft::{Error as RaftError, Result as RaftResult};

use yuntun_catalog::CatalogState;

/// raft `Storage` 的内存实现（日志 + 硬状态 + 快照元数据 + **状态机产物缓存**）。
#[derive(Clone)]
pub struct MetaStorage {
    inner: Arc<RwLock<MetaInner>>,
    /// 状态机：**快照内容的唯一来源**
    sm: Arc<Mutex<CatalogState>>,
    /// 安装过的快照数（观测用；测试断言"确实走了快照路径"而不是靠日志追上）
    installs: Arc<AtomicUsize>,
    /// 本节点 id（诊断与指标标签用；S3-6 的 metrics 也按它打标签）
    id: u64,
}

/// 存储内部状态（S3-3 落盘时这些字段变成 fjall 表/键值）。
#[derive(Default)]
struct MetaInner {
    hard_state: HardState,
    conf_state: ConfState,
    /// 已压缩前缀之外的日志（`entries[i].index = compacted_index + 1 + i`）
    entries: Vec<Entry>,
    /// 已压缩到的 index（= 最后一次快照覆盖到的位置）
    compacted_index: u64,
    /// `compacted_index` 处的任期（`term()` 在 `first_index - 1` 处仍要能回答）
    compacted_term: u64,
    /// 与 `compacted_index` **严格对应**的状态机产物
    artifact: Option<Vec<u8>>,
    /// **驱动方**报告的"已应用到哪条 raft 条目"（= `entry.index`）。
    ///
    /// 刻意不读状态机的 op 计数：no-op / ConfChange 也占 raft 索引（见文件头说明）。
    applied_index: u64,
}

impl MetaInner {
    fn first_index(&self) -> u64 {
        match self.entries.first() {
            Some(e) => e.index,
            None => self.compacted_index + 1,
        }
    }

    fn last_index(&self) -> u64 {
        match self.entries.last() {
            Some(e) => e.index,
            None => self.compacted_index,
        }
    }

    /// `index` 处的任期；不在日志里时回退到压缩位置（`index <= compacted_index` 只有这一种合法调用）
    fn term_at(&self, index: u64) -> u64 {
        if index == self.compacted_index {
            return self.compacted_term;
        }
        let base = self.first_index();
        if index >= base {
            let i = (index - base) as usize;
            if let Some(e) = self.entries.get(i) {
                return e.term;
            }
        }
        self.compacted_term
    }
}

impl MetaStorage {
    /// 建一个空存储（`voters` = 初始成员；生产由 `--init` 决定，扩容走 learner → voter）。
    pub fn new(sm: Arc<Mutex<CatalogState>>, voters: Vec<u64>) -> Self {
        Self::new_for(0, sm, voters)
    }

    /// 带节点 id 的构造（诊断/指标标签用）。
    pub fn new_for(id: u64, sm: Arc<Mutex<CatalogState>>, voters: Vec<u64>) -> Self {
        let inner = MetaInner {
            conf_state: ConfState {
                voters,
                ..Default::default()
            },
            ..Default::default()
        };
        Self {
            inner: Arc::new(RwLock::new(inner)),
            sm,
            installs: Arc::new(AtomicUsize::new(0)),
            id,
        }
    }

    /// 诊断轨迹（仅当 `YUNTUN_META_TRACE` 有值时输出）。
    fn trace(&self, what: &str, detail: String) {
        if std::env::var("YUNTUN_META_TRACE").is_ok() {
            eprintln!("[meta:{}] {what} {detail}", self.id);
        }
    }

    /// 克隆一个句柄（`RawNode` 会拿走存储的所有权，应用侧靠句柄继续操作）。
    pub fn handle(&self) -> Self {
        self.clone()
    }

    /// 安装过的快照数。
    pub fn installs(&self) -> usize {
        self.installs.load(Ordering::SeqCst)
    }

    /// 当前已压缩到的 index。
    pub fn compacted_index(&self) -> u64 {
        self.inner.read().unwrap().compacted_index
    }

    /// **驱动方在每个已提交条目之后调用**：报告"已应用到 raft 索引 `index`"。
    ///
    /// 必须对**所有**条目调用（含 no-op 与 ConfChange）—— 它们也占索引，漏报会让
    /// 压缩位置与日志错位。回退会被 `assert!` 抓住：那是驱动顺序错了，不能静默容忍。
    pub fn set_applied(&self, index: u64) {
        let mut inner = self.inner.write().unwrap();
        assert!(
            index >= inner.applied_index,
            "已应用索引不得回退：{} -> {index}",
            inner.applied_index
        );
        inner.applied_index = index;
    }

    /// 当前已应用到的 raft 索引（压缩的坐标）。
    pub fn applied_index(&self) -> u64 {
        self.inner.read().unwrap().applied_index
    }

    // ---------------------------------------------------------------- 应用侧写路径

    /// 追加日志条目（Ready 循环里 `rd.entries()` 的落点）。
    pub fn append(&self, ents: &[Entry]) {
        if ents.is_empty() {
            return;
        }
        self.trace(
            "append",
            format!(
                "{}..{}",
                ents.first().map(|e| e.index).unwrap_or(0),
                ents.last().map(|e| e.index).unwrap_or(0)
            ),
        );
        let mut inner = self.inner.write().unwrap();
        for e in ents {
            if e.index <= inner.compacted_index {
                // 已被快照覆盖（重复投递）；丢弃而不是报错：raft 认为它已持久化
                continue;
            }
            if let Some(last) = inner.entries.last() {
                if e.index <= last.index {
                    // 覆盖已有条目（raft 允许截断重写）
                    inner.entries.retain(|x| x.index < e.index);
                }
            }
            inner.entries.push(e.clone());
        }
    }

    /// 持久化硬状态（term / vote / commit）。
    pub fn set_hard_state(&self, hs: HardState) {
        self.inner.write().unwrap().hard_state = hs;
    }

    /// 只更新 commit（`advance` 后的 `commit_index`）。
    pub fn set_commit(&self, commit: u64) {
        self.inner.write().unwrap().hard_state.commit = commit;
    }

    /// 更新成员（conf change 应用后）。
    pub fn set_conf_state(&self, cs: ConfState) {
        self.inner.write().unwrap().conf_state = cs;
    }

    /// 安装收到的快照（follower 路径）。
    ///
    /// 返回 `false` 表示该快照比本地更旧（重复投递），**忽略而不是报错** ——
    /// 网络重传下这是正常现象，报错会让 raft 误判为存储故障。
    pub fn apply_snapshot(&self, mut snap: Snapshot) -> bool {
        let meta = snap.take_metadata();
        self.trace("recv_snapshot", format!("index={} term={}", meta.index, meta.term));
        let mut inner = self.inner.write().unwrap();
        if inner.first_index() > meta.index {
            self.trace("recv_snapshot", format!("忽略（旧于本地 first={}）", inner.first_index()));
            return false;
        }
        inner.entries.clear();
        inner.compacted_index = meta.index;
        inner.compacted_term = meta.term;
        // protobuf 生成的是 SingularPtrField：取出来落到具体值
        inner.conf_state = meta.conf_state.clone().into_option().unwrap_or_default();
        // 把收到的产物缓存下来：本节点日后成了 leader 也要能把它发出去
        inner.artifact = Some(snap.get_data().to_vec());
        // 快照覆盖到哪，就已应用到哪（单调推进）——
        // 否则本节点后续 `compact_applied` 会拿旧坐标当压缩位置。
        if meta.index > inner.applied_index {
            inner.applied_index = meta.index;
        }
        // 计数**只在接受时**加：被忽略的旧快照不算"靠快照追上了"
        self.installs.fetch_add(1, Ordering::SeqCst);
        true
    }

    /// **压缩到当前已应用位置**：先取状态机产物，再截日志（顺序不可交换）。
    ///
    /// ⚠️ 只允许在**应用线程**里调用（同一线程负责 apply + [`Self::set_applied`]，
    /// `applied_index` 不会被别处推进）：换上别的线程就可能出现
    /// "取的产物 = index J，记的 compacted_index = I≠J"。
    ///
    /// 返回压缩到的 index（未推进时返回原值 = no-op）。
    pub fn compact_applied(&self) -> u64 {
        // ① 先取"这一刻"的状态机产物（**在 sm 锁内取到，保证产物与 applied 同一瞬间**）
        let artifact = self.sm.lock().unwrap().snapshot_artifact();
        let mut inner = self.inner.write().unwrap();
        let applied = inner.applied_index;
        self.trace("compact", format!("applied={applied} 本已压缩到 {}", inner.compacted_index));
        if applied <= inner.compacted_index {
            return inner.compacted_index;
        }
        // ② 记位置与任期
        let term = inner.term_at(applied);
        inner.compacted_index = applied;
        inner.compacted_term = term;
        inner.artifact = Some(artifact);
        // ③ 最后才截日志：此刻产物已与 compacted_index 绑定
        inner.entries.retain(|e| e.index > applied);
        applied
    }
}

impl Storage for MetaStorage {
    fn initial_state(&self) -> RaftResult<RaftState> {
        let inner = self.inner.read().unwrap();
        Ok(RaftState::new(
            inner.hard_state.clone(),
            inner.conf_state.clone(),
        ))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> RaftResult<Vec<Entry>> {
        let inner = self.inner.read().unwrap();
        let (first, last) = (inner.first_index(), inner.last_index());
        if low < first {
            return Err(RaftError::Store(StorageError::Compacted));
        }
        if high > last + 1 {
            // 与 MemStorage 同款：越界访问是调用方 bug，不能静默返回短切片
            return Err(RaftError::Store(StorageError::Unavailable));
        }
        let mut out: Vec<Entry> = Vec::new();
        let max = max_size.into();
        let mut total: u64 = 0;
        for e in inner.entries.iter().skip((low - first) as usize) {
            if e.index >= high {
                break;
            }
            let sz = e.data.len() as u64 + 16;
            if let Some(m) = max {
                // 至少返回一条（raft 要求 `entries` 非空才能推进），之后按大小截断
                if !out.is_empty() && total + sz > m {
                    break;
                }
            }
            total += sz;
            out.push(e.clone());
        }
        Ok(out)
    }

    fn term(&self, idx: u64) -> RaftResult<u64> {
        let inner = self.inner.read().unwrap();
        let (first, last) = (inner.first_index(), inner.last_index());
        if idx == first - 1 {
            return Ok(inner.compacted_term);
        }
        if idx < first - 1 {
            return Err(RaftError::Store(StorageError::Compacted));
        }
        if idx > last {
            return Err(RaftError::Store(StorageError::Unavailable));
        }
        Ok(inner.term_at(idx))
    }

    fn first_index(&self) -> RaftResult<u64> {
        Ok(self.inner.read().unwrap().first_index())
    }

    fn last_index(&self) -> RaftResult<u64> {
        Ok(self.inner.read().unwrap().last_index())
    }

    /// 返回**本存储自己造的那份快照**（内容 = 状态机产物，元数据 = 产物对应的位置）。
    ///
    /// 判据（都必须满足，否则返回 `SnapshotTemporarilyUnavailable` 让 raft 稍后重试）：
    ///
    /// | 条件 | 理由 |
    /// |---|---|
    /// | 已压缩过（`artifact.is_some()`） | 没压缩就没有快照可言；此时 raft 也不需要（日志完整） |
    /// | `compacted_index > 0` | index = 0 的快照被 raft 视为非法（`need non-empty snapshot`） |
    /// | `compacted_index >= request_index` | 太旧的快照发出去等于把 follower 拉回旧状态 |
    fn snapshot(&self, request_index: u64, to: u64) -> RaftResult<Snapshot> {
        self.trace("send_snapshot?", format!("request_index={request_index} to={to}"));
        let inner = self.inner.read().unwrap();
        let unavailable = || RaftError::Store(StorageError::SnapshotTemporarilyUnavailable);
        let Some(bytes) = inner.artifact.as_ref() else {
            return Err(unavailable());
        };
        if inner.compacted_index == 0 || inner.compacted_index < request_index {
            return Err(unavailable());
        }
        let mut snap = Snapshot::default();
        {
            let meta = snap.mut_metadata();
            meta.index = inner.compacted_index;
            meta.term = inner.compacted_term;
            meta.set_conf_state(inner.conf_state.clone());
        }
        snap.set_data(bytes::Bytes::from(bytes.clone()));
        Ok(snap)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use yuntun_model::ops::{CommitFilesRequest, CreateTableRequest, DEFAULT_SCHEMA};
    use yuntun_model::meta::{FileManifest, IngestConfig};

    fn new_sm() -> Arc<Mutex<CatalogState>> {
        let mut st = CatalogState::new();
        st.create_table(
            CreateTableRequest {
                name: "cpu".into(),
                namespace: DEFAULT_SCHEMA.into(),
                schema: Arc::new(Schema::new(vec![Field::new(
                    "ts",
                    DataType::Int64,
                    false,
                )])),
                partition_cols: vec![],
                default_format: "parquet".into(),
                ingest_config: IngestConfig::standard(),
            },
            1_000,
        )
        .unwrap();
        Arc::new(Mutex::new(st))
    }

    /// 往状态机里塞一条提交（推进 `last_applied` = 快照位置）
    fn commit(sm: &Arc<Mutex<CatalogState>>, batch: &str, now: u64) {
        sm.lock()
            .unwrap()
            .commit_files(
                CommitFilesRequest {
                    table: "public.cpu".into(),
                    batch_id: batch.into(),
                    client_request_id: None,
                    client_request_ids: vec![],
                    shard: "s0".into(),
                    time_window: "w1".into(),
                    files: vec![FileManifest {
                        file_path: format!("p/{batch}.parquet"),
                        batch_id: batch.into(),
                        row_count: 1,
                        ..Default::default()
                    }],
                    schema_version: 1,
                    row_count: 1,
                },
                now,
            )
            .unwrap();
    }

    /// 没压缩过 → 不提供快照（返回可重试错误，而不是"空快照"）。
    ///
    /// 这一条正是 `MemStorage` 的失败点：它在没有内容时也会返回一个 data 为空的快照。
    #[test]
    fn snapshot_is_unavailable_before_any_compaction() {
        let sm = new_sm();
        let st = MetaStorage::new(sm, vec![1, 2, 3]);
        let err = st.snapshot(0, 2).unwrap_err();
        assert_eq!(
            err,
            RaftError::Store(StorageError::SnapshotTemporarilyUnavailable),
            "未压缩时必须报可重试的不可用，而不是给出空快照"
        );
    }

    /// **核心不变量**：压缩之后状态机继续前进，`snapshot()` 仍必须返回"压缩那一刻"的产物。
    ///
    /// 反例（本用例能抓到）：在 `snapshot()` 里现场从当前状态机取产物 →
    /// 元数据 index=1 而内容 read_index=2 → follower 装上后 index 1..2 的 op 会**重复/丢失**。
    #[test]
    fn snapshot_data_matches_compacted_index_not_current_state() {
        let sm = new_sm();
        let st = MetaStorage::new(sm.clone(), vec![1, 2, 3]);

        commit(&sm, "b1", 2_000);
        // ⚠️ 刻意用**与状态机 op 计数无关**的索引：压缩的坐标是 raft 索引（副本层），
        // 不是 `CatalogState::read_index()`（op 计数）。写成 `applied(&sm)` 会让
        // 本用例在"用错坐标"的实现下也通过 —— 那正是要防的坑。
        st.set_applied(10);
        let i1 = 10;
        assert_eq!(st.compact_applied(), i1, "压缩位置应等于已应用的 raft 索引");
        // 记下"压缩那一刻"的状态机 op 计数（= 载荷里应记录的值）
        let sm_ops_at_compact = sm.lock().unwrap().read_index();

        // 压缩之后继续前进（这正是"现场取产物"会出错的地方）
        commit(&sm, "b2", 2_001);
        st.set_applied(11);
        let i2 = 11;
        assert!(i2 > i1);

        let snap = st.snapshot(0, 2).unwrap();
        assert_eq!(
            snap.get_metadata().index, i1,
            "快照元数据 index 必须仍是压缩位置 {i1}（而不是当前的 {i2}）"
        );
        let restored = CatalogState::restore_snapshot(snap.get_data()).unwrap();
        // ⚠️ **两个时钟并存**（S3-3 必须统一，见文件头遗留）：
        //   ① 快照**载荷**里的 `last_applied` = 状态机的 **op 计数**（此处 2）；
        //   ② 快照**元数据**的 index = **raft 索引**（此处 10）。
        // 这里把两者都显式断言：写清楚现状，而不是含糊过去。
        assert_eq!(
            restored.read_index(),
            sm_ops_at_compact,
            "载荷里的 applied 是**压缩那一刻**的状态机 op 计数（时钟①）；\
             若这里等于当前 op 计数，说明产物是现场生成的"
        );
        assert!(
            sm.lock().unwrap().read_index() > sm_ops_at_compact,
            "用例前提：压缩后状态机必须又前进过"
        );
        // 真正的断言在这一条：产物必须是"压缩那一刻"的，**不能**含压缩点之后的 b2。
        // 若实现改成"现场从当前状态机取产物"，b2 会出现 → 立刻红。
        let text = String::from_utf8_lossy(&restored.encode_canonical()).to_string();
        assert!(
            text.contains("file b1") && !text.contains("file b2"),
            "产物含压缩点之后的数据（b2 出现了）→ 说明它是现场生成的最新状态，而不是 {i1} 时刻的：{text}"
        );
    }

    /// 压缩位置不前进时 `compact_applied` 是 no-op（可被周期性调用而不必自己判断）。
    #[test]
    fn compact_is_idempotent_when_nothing_new_applied() {
        let sm = new_sm();
        let st = MetaStorage::new(sm.clone(), vec![1, 2, 3]);
        commit(&sm, "b1", 3_000);
        st.set_applied(7);
        assert_eq!(st.compact_applied(), 7);
        assert_eq!(st.compact_applied(), 7, "没有新应用时不应推进");
        assert_eq!(st.snapshot(0, 2).unwrap().get_metadata().index, 7);
    }

    /// 压缩后老日志不可再读（`Compacted`），新日志正常可读。
    #[test]
    fn entries_below_first_index_report_compacted() {
        let sm = new_sm();
        let st = MetaStorage::new(sm.clone(), vec![1, 2, 3]);
        // 造 3 条日志（index 1..=3）
        let ents: Vec<Entry> = (1..=5u64)
            .map(|i| Entry {
                index: i,
                term: 1,
                ..Default::default()
            })
            .collect();
        st.append(&ents);
        commit(&sm, "b1", 4_000);
        st.set_applied(3); // raft 索引 3（日志恰好覆盖 1..=5）
        let c = st.compact_applied();
        assert!((1..5).contains(&c), "压缩位置应落在这批日志中间（实际 {c}）");

        assert_eq!(st.first_index().unwrap(), c + 1);
        assert_eq!(st.last_index().unwrap(), 5);
        // 压缩点之前的日志不可再读
        assert!(matches!(
            st.entries(1, c + 1, None, GetEntriesContext::empty(false)),
            Err(RaftError::Store(StorageError::Compacted))
        ));
        // 压缩点之后的日志正常可读
        let got = st
            .entries(c + 1, 6, None, GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(
            got.iter().map(|e| e.index).collect::<Vec<_>>(),
            ((c + 1)..=5).collect::<Vec<_>>()
        );
        // `term(compact_index)` 仍要能回答（raft 用它做日志匹配）
        assert_eq!(st.term(c).unwrap(), 1);
    }

    /// 重复投递的旧快照要被**忽略**而不是报错。
    #[test]
    fn stale_snapshot_is_ignored() {
        let sm = new_sm();
        let st = MetaStorage::new(sm.clone(), vec![1, 2, 3]);
        commit(&sm, "b1", 5_000);
        st.set_applied(4);
        st.compact_applied();
        let fresh = {
            commit(&sm, "b2", 5_001);
            st.set_applied(5);
            st.compact_applied();
            st.snapshot(0, 2).unwrap()
        };
        // 再造一个更旧的快照（index=1）
        assert!(!st.apply_snapshot(Snapshot::default()), "index=0 的默认快照应被忽略");
        let mut older = Snapshot::default();
        older.mut_metadata().index = 1;
        older.mut_metadata().term = 1;
        let _ = &older;
        assert!(
            !st.apply_snapshot(older),
            "比本地更旧的快照必须被忽略（网络重传是正常现象）"
        );
        // 本地状态不受影响
        assert_eq!(st.compacted_index(), fresh.get_metadata().index);
    }
}
