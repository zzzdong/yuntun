//! metanode 的 **fjall 落盘版** raft `Storage`（S3-3）。
//!
//! # 与内存版（[`crate::MetaStorage`]）的关系
//!
//! **不变量与写顺序完全一致**，只是把"真相"从内存搬到 fjall：
//!
//! | | 内存版 `MetaStorage` | 本实现 |
//! |---|---|---|
//! | 真相 | 进程内存 | **fjall**（每节点一个目录） |
//! | 缓存 | —— | 同构字段，**从 fjall 加载**、写路径上跟随更新 |
//! | 崩溃后 | 全丢（PoC 用它换测试速度与确定性） | 从 fjall 恢复（含 `applied_index` → 喂 `Config.applied`） |
//!
//! 内存版是"语义的定义处"（单测把它钉死），本实现必须逐条对齐它 —— 所以两个文件的
//! 方法名与不变量注释都刻意保持一致，便于对读。
//!
//! # 写顺序纪律：**先盘、后缓存**
//!
//! 每一步都必须**先写 fjall 成功、再更新缓存**。反过来（先更缓存）会让缓存领先于盘，
//! 崩溃后就出现"内存说有一份快照、盘上没有"—— 最难查的一类不一致。
//!
//! # 为什么压缩必须用 `batch(...).durability(SyncAll)`
//!
//! 压缩要同时做四件事：① 写新产物 ② 更新压缩位置与任期 ③ 删被覆盖的日志 ④ 删旧产物。
//! 逐条写的话，**中间任意一刻掉电**都会留下"索引指向不存在/对不上的产物"。
//! fjall 的 batch 是**原子提交 + 一次 fsync** ✓ 四件事要么全成、要么全不成 ——
//! 这正是把上一轮定下的不变量（`operation-log §42.2`）
//!
//! > `artifact` 必须与 `compacted_index` 严格对应
//!
//! 从"应用层小心维护"变成"**存储层保证**"的关键一步。
//!
//! # 哪些写要 fsync，哪些不必
//!
//! | 写 | 模式 | 理由 |
//! |---|---|---|
//! | `set_hard_state`（term/vote） | **SyncAll** | 投过票却没落盘 → 重启可能重复投票 → 破坏"一任期一票" |
//! | `append` | **SyncAll** | 日志是恢复的权威来源 |
//! | `apply_snapshot` / `compact_applied` | **SyncAll** | 同 + 上不变量 |
//! | `set_commit` | `Buffer` | commit 是**派生**值，重启后由 term/vote + 日志重新推出 |
//! | `set_applied` | `Buffer` | 只影响"从哪继续 apply"，保守重放是安全的（幂等） |
//!
//! ⚠️ 生产要**合并 fsync**（组提交思想）：本实现每个 Ready 一次 fsync。metanode 的提交速率是
//! "文件数/秒"量级（不是行数），够用；合并留给 S3-6。
//!
//! # 键布局（单 keyspace `raft`，日志/产物索引用**大端**以便范围扫描有序）
//!
//! ```text
//! "cs"                ConfState
//! "hs"                HardState
//! "ap"                applied_index (u64 LE)
//! "ci" / "ct"         compacted_index / compacted_term (u64 LE)
//! "li"                last_index (u64 LE)   ← 首个日志索引可由 ci+1 推出，故只存最后一个
//! "L" + idx(u64 BE)   日志条目
//! "S" + idx(u64 BE)   快照产物（**只保留最新一份**）
//! ```

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use protobuf::Message as PbMessage;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::storage::{GetEntriesContext, RaftState, Storage};
use raft::{Error as RaftError, Result as RaftResult, StorageError};

use yuntun_catalog::CatalogState;

const K_CONF_STATE: &[u8] = b"cs";
const K_HARD_STATE: &[u8] = b"hs";
const K_APPLIED: &[u8] = b"ap";
const K_COMPACT_IDX: &[u8] = b"ci";
const K_COMPACT_TERM: &[u8] = b"ct";
const K_LAST_IDX: &[u8] = b"li";
const K_ENTRY: u8 = b'L';
const K_ARTIFACT: u8 = b'S';

/// fjall 落盘的 raft 存储。
#[derive(Clone)]
pub struct FjallStorage {
    db: Database,
    ks: Keyspace,
    cache: Arc<Mutex<Cache>>,
    sm: Arc<Mutex<CatalogState>>,
    installs: Arc<AtomicUsize>,
    /// 本节点 id（诊断/指标标签）
    pub id: u64,
    /// 存储目录（每节点一个）
    pub dir: PathBuf,
}

#[derive(Debug, Clone, Default)]
struct Cache {
    hard_state: HardState,
    conf_state: ConfState,
    applied_index: u64,
    compacted_index: u64,
    compacted_term: u64,
    last_index: u64,
    /// 是否已有与 `compacted_index` 对应的产物（内容不进缓存：可能很大，按需从 fjall 取）
    has_artifact: bool,
}

impl FjallStorage {
    /// 打开（或首次新建）某节点的存储。
    ///
    /// `voters` 只在**首次**（`cs` 记录缺失）时写入；已存在的成员表不会被覆盖 ——
    /// 否则重启会悄悄把扩容结果回退。
    pub fn open(
        dir: impl AsRef<Path>,
        id: u64,
        sm: Arc<Mutex<CatalogState>>,
        voters: Vec<u64>,
    ) -> fjall::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let db = Database::builder(&dir).open()?;
        let ks = db.keyspace("raft", KeyspaceCreateOptions::default)?;

        let mut cache = Cache {
            hard_state: read_msg(&ks, K_HARD_STATE)?.unwrap_or_default(),
            applied_index: read_u64(&ks, K_APPLIED)?.unwrap_or(0),
            compacted_index: read_u64(&ks, K_COMPACT_IDX)?.unwrap_or(0),
            compacted_term: read_u64(&ks, K_COMPACT_TERM)?.unwrap_or(0),
            last_index: read_u64(&ks, K_LAST_IDX)?.unwrap_or(0),
            ..Default::default()
        };
        cache.conf_state = match read_msg(&ks, K_CONF_STATE)? {
            Some(cs) => cs,
            None => {
                let cs = ConfState {
                    voters,
                    ..Default::default()
                };
                write_msg(&ks, K_CONF_STATE, &cs)?;
                db.persist(PersistMode::SyncAll)?;
                cs
            }
        };
        if cache.last_index < cache.compacted_index {
            cache.last_index = cache.compacted_index;
        }
        cache.has_artifact =
            cache.compacted_index > 0 && ks.contains_key(artifact_key(cache.compacted_index))?;

        Ok(Self {
            db,
            ks,
            cache: Arc::new(Mutex::new(cache)),
            sm,
            installs: Arc::new(AtomicUsize::new(0)),
            id,
            dir,
        })
    }

    /// 句柄克隆（`RawNode` 拿走所有权，应用侧用同一份真相）。
    pub fn handle(&self) -> Self {
        self.clone()
    }

    /// 状态机句柄（快照安装/压缩都要它）。
    pub fn sm(&self) -> Arc<Mutex<CatalogState>> {
        self.sm.clone()
    }

    /// 盘上记录的成员表。
    ///
    /// 启动时必须与实际配置**比对**：不一致（典型：曾按 3 节点跑过，现在按单节点起）
    /// 会让本节点在一个"永远凑不齐成员"的组里静默空转 —— 宁可拒绝启动。
    /// 当前成员表（`ConfState`：voters / learners）。`§118` 起 `Join` 要用它回答成员查询。
    pub fn conf_state(&self) -> ConfState {
        self.cache.lock().unwrap().conf_state.clone()
    }

    pub fn voters(&self) -> Vec<u64> {
        let mut v = self.cache.lock().unwrap().conf_state.voters.clone();
        v.sort_unstable();
        v
    }

    /// 盘上那份快照覆盖到的 index（= 压缩位置）。**进程启动时 `Config.applied` 应取它**：
    /// 状态机正是从这个快照恢复的，raft 只需重放它之后的条目。
    pub fn snapshot_index(&self) -> u64 {
        self.cache.lock().unwrap().compacted_index
    }

    /// **进程启动路径**：打开存储并把状态机从盘上重建出来。
    ///
    /// 重建规则（只有两条，必须都做对，否则重启后要么状态凭空少一半、要么基座错位）：
    ///
    /// | 盘上有快照 | 状态机 | `Config.applied` |
    /// |---|---|---|
    /// | 有（`compacted_index > 0`） | `restore_snapshot(产物)` | `compacted_index`（raft 重放其后条目） |
    /// | 无 | 空状态机 | `0`（raft 从第 1 条开始重放全量日志） |
    ///
    /// ⚠️ 产物损坏时**报错**，绝不"当成没有快照、从空开始" —— 那会静默丢一半状态。
    pub fn open_with_state(
        dir: impl AsRef<Path>,
        id: u64,
        voters: Vec<u64>,
    ) -> fjall::Result<(Self, Arc<Mutex<CatalogState>>)> {
        let dir = dir.as_ref().to_path_buf();
        // 先探测产物（`open` 需要一个状态机句柄，这里先给个占位，随后替换）
        let probe = Arc::new(Mutex::new(CatalogState::new()));
        let storage = Self::open(&dir, id, probe, voters)?;
        let idx = storage.snapshot_index();
        let sm = if idx == 0 {
            Arc::new(Mutex::new(CatalogState::new()))
        } else {
            let bytes = storage
                .ks
                .get(artifact_key(idx))?
                .ok_or_else(|| fjall::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("盘上记录了快照 index={idx} 但产物缺失：拒绝以空状态启动（会静默丢一半状态）"),
                )))?;
            let st = CatalogState::restore_snapshot(&bytes).map_err(|e| {
                fjall::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("快照损坏：{e}"),
                ))
            })?;
            Arc::new(Mutex::new(st))
        };
        let storage = Self { sm: sm.clone(), ..storage };
        // 状态机在哪，`applied` 就该在哪（派生量按权威链重置，见 `reset_applied`）。
        // 快照 index = 0 时重置为 0 → raft 会从日志第 1 条重放 ✓ 正是重建所需。
        storage.reset_applied(idx)?;
        Ok((storage, sm))
    }

    pub fn installs(&self) -> usize {
        self.installs.load(Ordering::SeqCst)
    }

    pub fn applied_index(&self) -> u64 {
        self.cache.lock().unwrap().applied_index
    }

    pub fn compacted_index(&self) -> u64 {
        self.cache.lock().unwrap().compacted_index
    }

    fn trace(&self, what: &str, detail: String) {
        if crate::trace_on() {
            eprintln!("[meta:{}] {what} {detail}", self.id);
        }
    }

    // ---------------------------------------------------------------- 应用侧写路径

    /// 追加日志条目（必要时覆盖尾部）。
    ///
    /// 覆盖语义与内存版一致：先把 `index >= 新首条` 的旧条目删掉，再写新条目 ——
    /// **同一个 batch** 里完成，避免出现"旧的删了、新的没写"的空档。
    pub fn append(&self, ents: &[Entry]) -> fjall::Result<()> {
        if ents.is_empty() {
            return Ok(());
        }
        let start = ents[0].index;
        let new_last = ents[ents.len() - 1].index;
        self.trace("append", format!("{start}..{new_last}"));

        let mut batch = self.db.batch().durability(Some(PersistMode::SyncAll));
        let mut c = self.cache.lock().unwrap();
        if c.last_index >= start {
            for idx in start..=c.last_index {
                batch.remove(&self.ks, entry_key(idx));
            }
        }
        for e in ents {
            // protobuf 编码对我们的类型不会失败（无 map/无循环引用）
            batch.insert(&self.ks, entry_key(e.index), e.write_to_bytes().unwrap());
        }
        batch.insert(&self.ks, K_LAST_IDX, new_last.to_le_bytes().to_vec());
        batch.commit()?; // 原子 + fsync；失败则缓存不动（先盘后缓存）
        c.last_index = new_last;
        Ok(())
    }

    /// 持久化硬状态（term / vote / commit）。**必须 fsync**（见文件头表）。
    pub fn set_hard_state(&self, hs: HardState) -> fjall::Result<()> {
        write_msg(&self.ks, K_HARD_STATE, &hs)?;
        self.db.persist(PersistMode::SyncAll)?;
        self.cache.lock().unwrap().hard_state = hs;
        Ok(())
    }

    /// 只更新 commit（派生值，不必 fsync）。
    pub fn set_commit(&self, commit: u64) -> fjall::Result<()> {
        let mut c = self.cache.lock().unwrap();
        let mut hs = c.hard_state.clone();
        hs.commit = commit;
        write_msg(&self.ks, K_HARD_STATE, &hs)?;
        c.hard_state = hs;
        Ok(())
    }

    /// 更新成员表。
    pub fn set_conf_state(&self, cs: ConfState) -> fjall::Result<()> {
        write_msg(&self.ks, K_CONF_STATE, &cs)?;
        self.db.persist(PersistMode::SyncAll)?;
        self.cache.lock().unwrap().conf_state = cs;
        Ok(())
    }

    /// **启动路径专用**：把已应用位置**重置**为 `index`（不做单调断言）。
    ///
    /// 为什么需要它：`applied_index` 是**随状态机走的派生量**，不是权威持久化状态 ——
    /// 盘上留着的是"上一个进程那份内存状态已应用到哪"。而重启后状态机是**重建**出来的
    /// （从快照，或从空 + 日志重放），它的位置由**快照 index** 决定，与上一个进程的
    /// `applied` 无关。若沿用旧值，重建后的重放会撞上 `set_applied` 的单调断言
    /// （实测：`已应用索引不得回退：5 -> 1`）。
    ///
    /// > 权威链只有一条：**快照（index + 产物）→ 其后日志重放**。
    /// > `applied_index` 可以从它推出来，因此**崩溃后必须按它重置**。
    fn reset_applied(&self, index: u64) -> fjall::Result<()> {
        write_u64(&self.ks, K_APPLIED, index)?;
        self.cache.lock().unwrap().applied_index = index;
        Ok(())
    }

    /// **报告已应用到的 raft 索引**（驱动方对含 no-op/ConfChange 的每个条目都要报）。
    pub fn set_applied(&self, index: u64) -> fjall::Result<()> {
        let mut c = self.cache.lock().unwrap();
        assert!(
            index >= c.applied_index,
            "已应用索引不得回退：{} -> {index}",
            c.applied_index
        );
        if index == c.applied_index {
            return Ok(());
        }
        write_u64(&self.ks, K_APPLIED, index)?; // Buffer：不必 fsync
        c.applied_index = index;
        Ok(())
    }

    /// 安装收到的快照（follower 路径；返回 `false` = 旧快照被忽略）。
    pub fn apply_snapshot(&self, mut snap: Snapshot) -> fjall::Result<bool> {
        let meta = snap.take_metadata();
        self.trace(
            "recv_snapshot",
            format!("index={} term={}", meta.index, meta.term),
        );
        let data = snap.get_data().to_vec();
        let mut c = self.cache.lock().unwrap();
        if c.compacted_index + 1 > meta.index {
            return Ok(false); // 比本地更旧：忽略（网络重传是正常现象）
        }
        let mut batch = self.db.batch().durability(Some(PersistMode::SyncAll));
        // ① 新产物 ② 压缩位置/任期 ③ 删被覆盖的日志 ④ 删旧产物 —— 同一原子批
        batch.insert(&self.ks, artifact_key(meta.index), data);
        batch.insert(&self.ks, K_COMPACT_IDX, meta.index.to_le_bytes().to_vec());
        batch.insert(&self.ks, K_COMPACT_TERM, meta.term.to_le_bytes().to_vec());
        for idx in (c.compacted_index + 1)..=c.last_index.min(meta.index) {
            batch.remove(&self.ks, entry_key(idx));
        }
        if c.has_artifact && c.compacted_index != meta.index {
            batch.remove(&self.ks, artifact_key(c.compacted_index));
        }
        if c.applied_index < meta.index {
            batch.insert(&self.ks, K_APPLIED, meta.index.to_le_bytes().to_vec());
        }
        if c.last_index < meta.index {
            batch.insert(&self.ks, K_LAST_IDX, meta.index.to_le_bytes().to_vec());
        }
        batch.commit()?;
        c.compacted_index = meta.index;
        c.compacted_term = meta.term;
        c.conf_state = meta.conf_state.clone().into_option().unwrap_or_default();
        c.has_artifact = true;
        c.applied_index = c.applied_index.max(meta.index);
        c.last_index = c.last_index.max(meta.index);
        self.installs.fetch_add(1, Ordering::SeqCst);
        Ok(true)
    }

    /// **压缩到当前已应用位置**：先取状态机产物、再原子落盘（顺序与内存版一致）。
    ///
    /// 返回压缩到的 index（未推进时原值返回 = no-op）。
    pub fn compact_applied(&self) -> fjall::Result<u64> {
        // ① 先取"这一刻"的产物（与下面的坐标同一瞬间）
        let artifact = self.sm.lock().unwrap().snapshot_artifact();
        let mut c = self.cache.lock().unwrap();
        // 坐标是**副本层的 raft 索引**（`set_applied` 报进来的），**不是**状态机的 op 计数
        // （no-op/ConfChange 也占索引，用 op 计数会错开一格 —— `operation-log §42.3`）。
        let applied = c.applied_index;
        self.trace(
            "compact",
            format!("applied={applied} 本已压缩到 {}", c.compacted_index),
        );
        if applied <= c.compacted_index {
            return Ok(c.compacted_index);
        }
        let term = self.term_of(applied, &c)?;
        let mut batch = self.db.batch().durability(Some(PersistMode::SyncAll));
        batch.insert(&self.ks, artifact_key(applied), artifact);
        batch.insert(&self.ks, K_COMPACT_IDX, applied.to_le_bytes().to_vec());
        batch.insert(&self.ks, K_COMPACT_TERM, term.to_le_bytes().to_vec());
        for idx in (c.compacted_index + 1)..=c.last_index.min(applied) {
            batch.remove(&self.ks, entry_key(idx));
        }
        if c.has_artifact && c.compacted_index != applied {
            batch.remove(&self.ks, artifact_key(c.compacted_index));
        }
        batch.commit()?;
        c.compacted_index = applied;
        c.compacted_term = term;
        c.has_artifact = true;
        Ok(applied)
    }

    /// `index` 处的任期（缓存 + 必要的一次点查）。
    fn term_of(&self, index: u64, c: &Cache) -> fjall::Result<u64> {
        if index == c.compacted_index {
            return Ok(c.compacted_term);
        }
        if index > c.last_index {
            return Ok(0);
        }
        let term = match self.ks.get(entry_key(index))? {
            Some(v) => Entry::parse_from_bytes(&v).map(|e| e.term).unwrap_or(0),
            None => 0,
        };
        Ok(term)
    }
}

impl Storage for FjallStorage {
    fn initial_state(&self) -> RaftResult<RaftState> {
        let c = self.cache.lock().unwrap();
        Ok(RaftState::new(c.hard_state.clone(), c.conf_state.clone()))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> RaftResult<Vec<Entry>> {
        let (first, last) = {
            let c = self.cache.lock().unwrap();
            (c.compacted_index + 1, c.last_index)
        };
        if low < first {
            return Err(RaftError::Store(StorageError::Compacted));
        }
        if high > last + 1 {
            return Err(RaftError::Store(StorageError::Unavailable));
        }
        let max = max_size.into();
        let mut out: Vec<Entry> = Vec::new();
        let mut total: u64 = 0;
        for guard in self.ks.range(entry_key(low)..entry_key(high)) {
            let (_, v) = guard
                .into_inner()
                .map_err(|_| RaftError::Store(StorageError::Unavailable))?;
            let sz = v.len() as u64 + 16;
            if let Some(m) = max {
                // 至少返回一条（raft 要靠它推进），之后按大小截断
                if !out.is_empty() && total + sz > m {
                    break;
                }
            }
            total += sz;
            out.push(
                Entry::parse_from_bytes(&v)
                    .map_err(|_| RaftError::Store(StorageError::Unavailable))?,
            );
        }
        // 轨迹：`entries` 是"leader 该发哪些条目"的唯一来源，返回空 = 它会发**空** append，
        // 而空 append 会让 follower 误以为"没事"（`§106`/`§107` 的停摆就卡在这里）。
        self.trace(
            "entries",
            format!("[{low},{high}) first={first} last={last} → {} 条", out.len()),
        );
        Ok(out)
    }

    fn term(&self, idx: u64) -> RaftResult<u64> {
        let c = self.cache.lock().unwrap();
        let first = c.compacted_index + 1;
        if idx == first - 1 {
            return Ok(c.compacted_term);
        }
        if idx < first - 1 {
            return Err(RaftError::Store(StorageError::Compacted));
        }
        if idx > c.last_index {
            return Err(RaftError::Store(StorageError::Unavailable));
        }
        self.term_of(idx, &c)
            .map_err(|_| RaftError::Store(StorageError::Unavailable))
    }

    fn first_index(&self) -> RaftResult<u64> {
        Ok(self.cache.lock().unwrap().compacted_index + 1)
    }

    fn last_index(&self) -> RaftResult<u64> {
        Ok(self.cache.lock().unwrap().last_index)
    }

    /// 返回**盘上那一份**产物（索引与内容都来自同一次原子提交）。
    ///
    /// 三种情况一律返回"暂不可用"（可重试），**绝不伪造**：
    /// 没压缩过（`index == 0`，raft 视为非法）、本次请求要更新的、**索引在但产物不在**
    /// （说明盘上不一致 —— 宁可让 raft 重试，也不能把对不上的快照发出去）。
    fn snapshot(&self, request_index: u64, _to: u64) -> RaftResult<Snapshot> {
        let (index, term, conf_state) = {
            let c = self.cache.lock().unwrap();
            (c.compacted_index, c.compacted_term, c.conf_state.clone())
        };
        let unavailable = || RaftError::Store(StorageError::SnapshotTemporarilyUnavailable);
        if index == 0 || index < request_index {
            return Err(unavailable());
        }
        let bytes = self
            .ks
            .get(artifact_key(index))
            .map_err(|_| unavailable())?;
        let Some(bytes) = bytes else {
            // 索引有、产物没有 → 盘上不一致：报可重试，**不**返回对不上的快照
            self.trace("send_snapshot?", format!("index={index} 产物缺失→拒绝"));
            return Err(unavailable());
        };
        let mut snap = Snapshot::default();
        {
            let meta = snap.mut_metadata();
            meta.index = index;
            meta.term = term;
            meta.set_conf_state(conf_state);
        }
        snap.set_data(bytes::Bytes::from(bytes.to_vec()));
        Ok(snap)
    }
}

// ---------------------------------------------------------------- 键与编解码助手

fn entry_key(idx: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(9);
    k.push(K_ENTRY);
    k.extend_from_slice(&idx.to_be_bytes());
    k
}

fn artifact_key(idx: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(9);
    k.push(K_ARTIFACT);
    k.extend_from_slice(&idx.to_be_bytes());
    k
}

fn read_u64(ks: &Keyspace, key: &[u8]) -> fjall::Result<Option<u64>> {
    let Some(v) = ks.get(key)? else { return Ok(None) };
    if v.len() != 8 {
        return Ok(None);
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&v[..8]);
    Ok(Some(u64::from_le_bytes(buf)))
}

fn write_u64(ks: &Keyspace, key: &[u8], v: u64) -> fjall::Result<()> {
    ks.insert(key, v.to_le_bytes().to_vec())
}

fn read_msg<T: PbMessage>(ks: &Keyspace, key: &[u8]) -> fjall::Result<Option<T>> {
    Ok(ks
        .get(key)?
        .and_then(|v| T::parse_from_bytes(&v).ok()))
}

fn write_msg<T: PbMessage>(ks: &Keyspace, key: &[u8], msg: &T) -> fjall::Result<()> {
    ks.insert(key, msg.write_to_bytes().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use yuntun_model::meta::{FileManifest, IngestConfig};
    use yuntun_model::ops::{CommitFilesRequest, CreateTableRequest, DEFAULT_SCHEMA};

    /// 临时目录（Drop 时清理）。
    struct TempDir(PathBuf);
    impl TempDir {
        fn new(tag: &str) -> Self {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let p = std::env::temp_dir().join(format!("yuntun-meta-{tag}-{nanos}"));
            std::fs::create_dir_all(&p).unwrap();
            Self(p)
        }
        fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn new_sm() -> Arc<Mutex<CatalogState>> {
        let mut st = CatalogState::new();
        st.create_table(
            CreateTableRequest {
                name: "cpu".into(),
                namespace: DEFAULT_SCHEMA.into(),
                schema: Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)])),
                partition_cols: vec![],
                default_format: "parquet".into(),
                ingest_config: IngestConfig::standard(),
            },
            1_000,
        )
        .unwrap();
        Arc::new(Mutex::new(st))
    }

    fn commit(sm: &Arc<Mutex<CatalogState>>, batch_id: &str, now: u64) {
        sm.lock()
            .unwrap()
            .commit_files(
                CommitFilesRequest {
                    table: "public.cpu".into(),
                    batch_id: batch_id.into(),
                    client_request_id: None,
                    client_request_ids: vec![],
                    shard: "s0".into(),
                    time_window: "w1".into(),
                    files: vec![FileManifest {
                        file_path: format!("p/{batch_id}.parquet"),
                        batch_id: batch_id.into(),
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

    fn ent(index: u64, term: u64) -> Entry {
        Entry {
            index,
            term,
            ..Default::default()
        }
    }

    /// 日志 / 硬状态 / 已应用索引都要**跨 reopen 存活**（崩溃恢复的基础）。
    #[test]
    fn log_hardstate_and_applied_survive_reopen() {
        let dir = TempDir::new("reopen");
        {
            let sm = new_sm();
            let st = FjallStorage::open(dir.path(), 1, sm.clone(), vec![1, 2, 3]).unwrap();
            st.append(&[ent(1, 1), ent(2, 1), ent(3, 2)]).unwrap();
            st.set_hard_state(HardState {
                term: 2,
                vote: 3,
                commit: 3,
                ..Default::default()
            })
            .unwrap();
            st.set_applied(3).unwrap();
            commit(&sm, "b1", 2_000);
            st.set_applied(4).unwrap();
        }
        // 重新打开（模拟进程重启）
        let sm = new_sm();
        let st = FjallStorage::open(dir.path(), 1, sm, vec![1, 2, 3]).unwrap();
        assert_eq!(st.first_index().unwrap(), 1);
        assert_eq!(st.last_index().unwrap(), 3, "日志末索引必须恢复");
        assert_eq!(st.term(3).unwrap(), 2, "日志任期必须恢复");
        let rs = st.initial_state().unwrap();
        assert_eq!(rs.hard_state.term, 2);
        assert_eq!(rs.hard_state.vote, 3, "投票必须落盘（否则可能重复投票）");
        assert_eq!(rs.hard_state.commit, 3);
        assert_eq!(rs.conf_state.voters, vec![1, 2, 3]);
        assert_eq!(
            st.applied_index(),
            4,
            "applied 必须恢复 —— 它要喂给 `Config.applied`（不喂就会重放已应用条目）"
        );
    }

    /// **核心不变量跨崩溃**：产物必须与压缩位置严格对应。
    #[test]
    fn artifact_and_compacted_index_stay_consistent_across_reopen() {
        let dir = TempDir::new("artifact");
        let sm = new_sm();
        {
            let st = FjallStorage::open(dir.path(), 1, sm.clone(), vec![1, 2, 3]).unwrap();
            st.append(&[ent(1, 1), ent(2, 1), ent(3, 1), ent(4, 1)])
                .unwrap();
            commit(&sm, "b1", 2_000);
            st.set_applied(3).unwrap();
            let c = st.compact_applied().unwrap();
            assert_eq!(c, 3);
            // 压缩后再前进（内存版用例里"现场取产物"会在这里出错）
            commit(&sm, "b2", 2_001);
            st.set_applied(4).unwrap();
        }
        let st = FjallStorage::open(dir.path(), 1, sm.clone(), vec![1, 2, 3]).unwrap();
        assert_eq!(st.compacted_index(), 3, "压缩位置必须恢复");
        assert_eq!(st.first_index().unwrap(), 4, "压缩掉的日志必须真的没了");
        assert_eq!(st.last_index().unwrap(), 4);
        let snap = st.snapshot(0, 2).unwrap();
        assert_eq!(snap.get_metadata().index, 3);
        let restored = CatalogState::restore_snapshot(snap.get_data()).unwrap();
        let text = String::from_utf8_lossy(&restored.encode_canonical()).to_string();
        assert!(
            text.contains("file b1") && !text.contains("file b2"),
            "产物必须是压缩那一刻的（含 b1、不含 b2）：{text}"
        );
        // 压缩点之前的日志不可读，之后的可以
        assert!(matches!(
            st.entries(1, 2, None, GetEntriesContext::empty(false)),
            Err(RaftError::Store(StorageError::Compacted))
        ));
        assert_eq!(
            st.entries(4, 5, None, GetEntriesContext::empty(false))
                .unwrap()
                .len(),
            1
        );
    }

    /// 盘上不一致（索引有、产物没）时**宁可报可重试，也不发对不上的快照**。
    #[test]
    fn snapshot_is_not_faked_when_artifact_missing() {
        let dir = TempDir::new("missing-artifact");
        let sm = new_sm();
        let st = FjallStorage::open(dir.path(), 1, sm.clone(), vec![1, 2, 3]).unwrap();
        st.append(&[ent(1, 1), ent(2, 1)]).unwrap();
        commit(&sm, "b1", 2_000);
        st.set_applied(2).unwrap();
        assert_eq!(st.compact_applied().unwrap(), 2);
        assert!(st.snapshot(0, 2).is_ok(), "正常情况下应能给出快照");
        // 白盒破坏：把产物删掉（模拟"索引写了、产物没落"的极端情况）
        st.ks.remove(artifact_key(2)).unwrap();
        assert_eq!(
            st.snapshot(0, 2).unwrap_err(),
            RaftError::Store(StorageError::SnapshotTemporarilyUnavailable),
            "产物缺失时必须报可重试，绝不能返回对不上的快照"
        );
    }

    /// 旧快照（重复投递）忽略；更旧的产物不被保留。
    #[test]
    fn stale_snapshot_is_ignored_and_old_artifact_dropped() {
        let dir = TempDir::new("stale");
        let sm = new_sm();
        let st = FjallStorage::open(dir.path(), 1, sm.clone(), vec![1, 2, 3]).unwrap();
        st.append(&[ent(1, 1), ent(2, 1), ent(3, 1)]).unwrap();
        commit(&sm, "b1", 2_000);
        st.set_applied(2).unwrap();
        assert_eq!(st.compact_applied().unwrap(), 2);
        commit(&sm, "b2", 2_001);
        st.set_applied(3).unwrap();
        assert_eq!(st.compact_applied().unwrap(), 3);
        assert!(
            !st.ks.contains_key(artifact_key(2)).unwrap(),
            "只保留最新产物（旧的不留 → 避免无界增长）"
        );
        // 更旧的快照被忽略
        let mut older = Snapshot::default();
        older.mut_metadata().index = 1;
        older.mut_metadata().term = 1;
        assert!(!st.apply_snapshot(older).unwrap());
        assert_eq!(st.compacted_index(), 3, "忽略旧快照不应影响本地");
        assert_eq!(st.installs(), 0, "被忽略的不算安装");
    }

    /// 覆盖写（raft 允许截断重写尾部）不能留下"旧条目残留"。
    #[test]
    fn overwrite_truncates_old_tail() {
        let dir = TempDir::new("overwrite");
        let sm = new_sm();
        let st = FjallStorage::open(dir.path(), 1, sm, vec![1, 2, 3]).unwrap();
        st.append(&[ent(1, 1), ent(2, 1), ent(3, 1), ent(4, 1)])
            .unwrap();
        // 从 index=3 起重写（任期变 2）
        st.append(&[ent(3, 2), ent(4, 2), ent(5, 2)]).unwrap();
        assert_eq!(st.last_index().unwrap(), 5);
        assert_eq!(st.term(3).unwrap(), 2, "被覆盖条目的任期必须更新");
        assert_eq!(st.term(5).unwrap(), 2);
        let got = st
            .entries(1, 6, None, GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(
            got.iter().map(|e| e.index).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5],
            "覆盖后不应出现重复或空洞"
        );
    }
}
