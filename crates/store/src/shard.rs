//! 分片存储层（ShardStore）：一个 **shard**（表 + shard_key + 时间窗口）的数据
//! 可能以两种形态（tier）存在。
//!
//! | tier | 名称 | 内容 | 生命周期 |
//! |---|---|---|---|
//! | [`ShardTier::Memory`] | **内存分片** | 已 fsync WAL、尚未 flush 的"热"数据（Arrow 批次） | 进程内；提交后转 `Committed(snapshot)`，查询缓存追上即回收 |
//! | [`ShardTier::Disk`] | **磁盘分片** | 已编码落对象存储（本地磁盘 / S3）、由 Catalog Manifest 索引的"冷"数据 | 持久；受快照隔离 / Compaction / L1 分片移除管理 |
//!
//! ## 为什么放在 store 层（而不是 Ingestor）
//! "读己之写"要回答的问题是**"这个分片的数据现在在哪一形态"** —— 这是存储层语义，
//! 不是写入组件（Ingestor）的内部实现细节。收敛到本层后：
//! - 查询只依赖 store 层 API，**不依赖 Ingestor 进程**；
//! - 分离部署只是换一个 [`ShardReader`] 实现，写入 / 查询调用方零改动。
//!
//! ## 读侧接缝（[`ShardReader`]）
//! 查询侧**只面向** [`ShardReader`]（按分片读取的内存分片视图），实现有两种形态：
//!
//! | 实现 | 场景 | 传输 |
//! |---|---|---|
//! | [`MemoryShard`] | 阶段 0 单机 / 与写入同进程 | 进程内 `RwLock` |
//! | [`RemoteShard`] | 分离部署（查询节点 ↔ 分片服务） | 由 [`ShardFetch`] 注入（阶段 1：gRPC；单测：假实现） |
//!
//! ## 交接语义（无空洞 / 无重复）
//! 内存分片条目带状态：
//! - [`MemoryState::Live`]：未提交 → 查询可见；
//! - [`MemoryState::Committed(snapshot)`]：已提交、磁盘分片（Manifest）可能还没被查询缓存追上
//!   → `cached_snapshot < snapshot` 期间仍由内存分片提供，之后交给磁盘分片。
//!
//! 查询缓存刷到 `snapshot` 后调用 [`ShardReader::reclaim`] 回收本地副本。
//!
//! ## 表世代（DROP / 同名 CREATE）
//! [`MemoryShard::observe_ddl`] 由写入侧按 WAL DDL 顺序维护表存活与世代（`epoch`）；
//! 陈旧世代（已 DROP / DROP 后重建）的数据既不参与查询、也不参与提交。

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use object_store::ObjectStore;

use yuntun_model::error::LakeError;
use yuntun_model::wal_record::ddl_op;

/// 分片的数据形态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardTier {
    /// 内存分片：已 fsync、未落对象的"热"数据。
    Memory,
    /// 磁盘分片：对象存储（本地磁盘 / S3）上、由 Manifest 索引的"冷"数据。
    Disk,
}

/// 分片标识：`(表全限定名, shard_key, time_window)`。
///
/// 与 `IngestBatch.shard_key` / 对象路径 `dt=<window>/shard=<shard>` /
/// 攒批分组键 `(table, shard, window)` 一一对应。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardId {
    /// 全限定表标识 `schema.table`（裸名按 `public` 归一）
    pub table: String,
    pub shard: String,
    /// 时间窗口（整分钟对齐，如 `2026-09-12T10:09`）
    pub window: String,
}

impl ShardId {
    pub fn new(
        table: impl Into<String>,
        shard: impl Into<String>,
        window: impl Into<String>,
    ) -> Self {
        Self {
            table: table.into(),
            shard: shard.into(),
            window: window.into(),
        }
    }
}

// ---------------------------------------------------------------- 读侧接缝

/// **分片读接口**：查询侧唯一依赖的热数据入口。
///
/// - [`MemoryShard`]：进程内内存分片（阶段 0 单机 / 与写入同进程）；
/// - [`RemoteShard`]：远端分片服务（分离部署，传输由 [`ShardFetch`] 注入）。
///
/// 语义：返回该分片**对 `cached_snapshot` 尚不可见**的数据
/// （即尚未进入查询缓存所依据的 Manifest 快照的那部分），用于与磁盘分片求并集。
#[async_trait]
pub trait ShardReader: Send + Sync + std::fmt::Debug {
    fn tier(&self) -> ShardTier;

    /// 变更计数：查询侧据此做"近实时刷新"触发（远端实现取服务端水位，取不到则 0 → 退化为 TTL）。
    fn version(&self) -> u64;

    /// 枚举某表当前存在的分片。
    async fn shards_of(&self, table: &str) -> Result<Vec<ShardId>, LakeError>;

    /// 读**单个分片**对 `cached_snapshot` 尚不可见的数据。
    async fn read_shard(
        &self,
        id: &ShardId,
        cached_snapshot: u64,
    ) -> Result<Vec<RecordBatch>, LakeError>;

    /// 便捷：读整表（默认 = 枚举 + 逐分片；远端实现可覆写为单次 RPC）。
    async fn read_table(
        &self,
        table: &str,
        cached_snapshot: u64,
    ) -> Result<Vec<RecordBatch>, LakeError> {
        let mut out = Vec::new();
        for id in self.shards_of(table).await? {
            out.extend(self.read_shard(&id, cached_snapshot).await?);
        }
        Ok(out)
    }

    /// 回收**本地副本**中已被 `cached_snapshot` 覆盖（或已陈旧世代）的条目。
    ///
    /// 默认空实现：远端分片服务的 GC 由其自身负责，不由查询节点驱动。
    fn reclaim(&self, _cached_snapshot: u64) {}
}

/// 远端分片服务的**传输接缝**。
///
/// 阶段 1 用 gRPC / HTTP 实现；单测用假实现。本 trait 只关心"取哪些分片 / 取某分片的数据"，
/// 不关心序列化与连接管理，避免把传输细节泄漏到查询侧。
pub trait ShardFetch: Send + Sync {
    /// 枚举某表当前存在的分片（远端服务上的）。
    fn fetch_shards<'a>(
        &'a self,
        table: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<Vec<ShardId>, LakeError>>;

    /// 读单个分片对 `cached_snapshot` 尚不可见的数据。
    fn fetch_shard<'a>(
        &'a self,
        id: &'a ShardId,
        cached_snapshot: u64,
    ) -> futures::future::BoxFuture<'a, Result<Vec<RecordBatch>, LakeError>>;

    /// 服务端变更水位（拿不到返回 0：查询侧退化为 TTL 刷新）。
    fn fetch_version(&self) -> u64 {
        0
    }
}

/// 远端分片读取器：把 [`ShardFetch`] 适配成 [`ShardReader`]。
///
/// 查询节点在分离部署下用本结构替换 `MemoryShard`（调用方零改动）。
pub struct RemoteShard {
    fetch: Arc<dyn ShardFetch>,
}

impl std::fmt::Debug for RemoteShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteShard").finish()
    }
}

impl RemoteShard {
    pub fn new(fetch: Arc<dyn ShardFetch>) -> Self {
        Self { fetch }
    }
}

#[async_trait]
impl ShardReader for RemoteShard {
    fn tier(&self) -> ShardTier {
        ShardTier::Memory
    }

    fn version(&self) -> u64 {
        self.fetch.fetch_version()
    }

    async fn shards_of(&self, table: &str) -> Result<Vec<ShardId>, LakeError> {
        self.fetch.fetch_shards(table).await
    }

    async fn read_shard(
        &self,
        id: &ShardId,
        cached_snapshot: u64,
    ) -> Result<Vec<RecordBatch>, LakeError> {
        self.fetch.fetch_shard(id, cached_snapshot).await
    }
    // reclaim 用默认空实现：远端服务的 GC 由服务端负责
}

// ---------------------------------------------------------------- 内存分片

/// 内存分片条目状态（见模块注释"交接语义"）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryState {
    /// 尚未提交（热）：查询可见。
    Live,
    /// 已提交到 Catalog（携带 commit 快照号）：查询缓存未追上时仍可见。
    Committed(u64),
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

/// 内存分片里的一条数据（一个 WAL Data 记录可能解码出多个 RecordBatch）。
#[derive(Debug, Clone)]
pub struct HotBatch {
    pub seq: u64,
    pub epoch: u64,
    pub batches: Vec<RecordBatch>,
    pub state: MemoryState,
}

/// **内存分片集合**：进程内共享的"热数据"视图（读己之写）。
///
/// 同时是**写入侧**的热缓冲（`push` / `mark_committed` / `observe_ddl` / `sweep`）
/// 与**读侧**的 [`ShardReader`] 实现（阶段 0 单机）。
#[derive(Debug, Default)]
pub struct MemoryShard {
    liveness: RwLock<HashMap<String, TableLiveness>>,
    /// ShardId -> (wal_seq -> 条目)；BTreeMap 保证按写入顺序返回
    shards: RwLock<HashMap<ShardId, BTreeMap<u64, HotBatch>>>,
    /// 变更计数：供查询缓存做"提交驱动刷新"。
    version: AtomicU64,
}

impl MemoryShard {
    /// 共享句柄。
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// 变更计数：写入 / 提交 / DDL 都会 +1。
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    // ---- 写入侧（生命周期 / 世代）----

    /// 观察一条 DDL（写入侧按 WAL 顺序调用）：维护表存活与世代。
    pub fn observe_ddl(&self, op: u32, table: &str) {
        let mut live = self.liveness.write().unwrap();
        let e = live.entry(table.to_string()).or_default();
        match op {
            ddl_op::CREATE_TABLE => {
                e.exists = true;
                e.epoch += 1;
            }
            ddl_op::DROP_TABLE => e.exists = false,
            // schema 事件不参与表世代（DROP SCHEMA 要求空库，无表）
            _ => return,
        }
        drop(live);
        self.version.fetch_add(1, Ordering::SeqCst);
    }

    /// 当前存活状态与世代。
    pub fn liveness(&self, table: &str) -> TableLiveness {
        self.liveness
            .read()
            .unwrap()
            .get(table)
            .copied()
            .unwrap_or_default()
    }

    /// 该世代是否已陈旧（表不存在，或世代已变 = 被 DROP 后重建）。
    pub fn is_stale(&self, table: &str, epoch: u64) -> bool {
        let l = self.liveness(table);
        !l.exists || l.epoch != epoch
    }

    /// 发布一条"已 fsync、未提交"的热数据。
    pub fn push(&self, id: &ShardId, seq: u64, epoch: u64, batches: Vec<RecordBatch>) {
        if batches.is_empty() {
            return;
        }
        self.shards
            .write()
            .unwrap()
            .entry(id.clone())
            .or_default()
            .insert(
                seq,
                HotBatch {
                    seq,
                    epoch,
                    batches,
                    state: MemoryState::Live,
                },
            );
        self.version.fetch_add(1, Ordering::SeqCst);
    }

    /// 提交成功：条目标记为 `Committed(snapshot)`（不删除 —— 查询缓存可能还没追上）。
    pub fn mark_committed(&self, id: &ShardId, seqs: &[u64], snapshot: u64) {
        let mut shards = self.shards.write().unwrap();
        if let Some(m) = shards.get_mut(id) {
            for s in seqs {
                if let Some(e) = m.get_mut(s) {
                    e.state = MemoryState::Committed(snapshot);
                }
            }
        }
        drop(shards);
        self.version.fetch_add(1, Ordering::SeqCst);
    }

    /// 丢弃条目（世代陈旧 / 提交失败回滚）。
    pub fn remove(&self, id: &ShardId, seqs: &[u64]) {
        let mut shards = self.shards.write().unwrap();
        if let Some(m) = shards.get_mut(id) {
            for s in seqs {
                m.remove(s);
            }
            if m.is_empty() {
                shards.remove(id);
            }
        }
        drop(shards);
        self.version.fetch_add(1, Ordering::SeqCst);
    }

    // ---- 读侧（同步原语；[`ShardReader`] 实现与 `readable` 都基于它们）----

    fn shards_of_ids(&self, table: &str) -> Vec<ShardId> {
        self.shards
            .read()
            .unwrap()
            .keys()
            .filter(|id| id.table == table)
            .cloned()
            .collect()
    }

    fn read_shard_sync(&self, id: &ShardId, cached_snapshot: u64) -> Vec<RecordBatch> {
        let live = self.liveness(&id.table);
        if !live.exists {
            return Vec::new();
        }
        let shards = self.shards.read().unwrap();
        let Some(m) = shards.get(id) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for e in m.values() {
            if e.epoch != live.epoch {
                continue;
            }
            let visible = match e.state {
                MemoryState::Live => true,
                MemoryState::Committed(s) => cached_snapshot < s,
            };
            if visible {
                out.extend(e.batches.iter().cloned());
            }
        }
        out
    }

    /// 同步便捷入口（内部 / 单测）：等价于 enumerate + 逐分片读。
    pub fn readable(&self, table: &str, cached_snapshot: u64) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        for id in self.shards_of_ids(table) {
            out.extend(self.read_shard_sync(&id, cached_snapshot));
        }
        out
    }

    /// 回收：已提交且被查询缓存追上（`snapshot >= commit_snapshot`）的条目，
    /// 以及陈旧世代 / 表已删除的条目。
    pub fn sweep(&self, cached_snapshot: u64) {
        let mut shards = self.shards.write().unwrap();
        shards.retain(|id, m| {
            let live = self
                .liveness
                .read()
                .unwrap()
                .get(&id.table)
                .copied()
                .unwrap_or_default();
            m.retain(|_, e| {
                if !live.exists || e.epoch != live.epoch {
                    return false;
                }
                match e.state {
                    MemoryState::Live => true,
                    MemoryState::Committed(s) => cached_snapshot < s,
                }
            });
            !m.is_empty()
        });
    }

    /// 诊断：当前热数据条目数。
    pub fn len(&self) -> usize {
        self.shards.read().unwrap().values().map(|m| m.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl ShardReader for MemoryShard {
    fn tier(&self) -> ShardTier {
        ShardTier::Memory
    }

    fn version(&self) -> u64 {
        MemoryShard::version(self)
    }

    async fn shards_of(&self, table: &str) -> Result<Vec<ShardId>, LakeError> {
        Ok(self.shards_of_ids(table))
    }

    async fn read_shard(
        &self,
        id: &ShardId,
        cached_snapshot: u64,
    ) -> Result<Vec<RecordBatch>, LakeError> {
        Ok(self.read_shard_sync(id, cached_snapshot))
    }

    async fn read_table(
        &self,
        table: &str,
        cached_snapshot: u64,
    ) -> Result<Vec<RecordBatch>, LakeError> {
        Ok(self.readable(table, cached_snapshot))
    }

    fn reclaim(&self, cached_snapshot: u64) {
        self.sweep(cached_snapshot);
    }
}

// ---------------------------------------------------------------- 磁盘分片

/// **磁盘分片**：对象存储上的分片（冷数据），由 Catalog Manifest 索引文件清单。
///
/// 本结构只承载"分片在磁盘/对象存储上的位置与形态"这一层信息；文件清单的权威
/// 仍然是 Catalog（Manifest + 快照隔离）。
#[derive(Debug)]
pub struct DiskShard {
    store: Arc<dyn ObjectStore>,
}

impl DiskShard {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub fn tier(&self) -> ShardTier {
        ShardTier::Disk
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// 分片在对象存储上的目录前缀：`yuntun/<schema>/<table>/dt=<window>/shard=<shard>/`
    /// （与 `yuntun_format::file_path` 的目录部分一致；表标识为全限定 `schema.table`）。
    pub fn prefix(&self, id: &ShardId) -> String {
        let (ns, table) = id
            .table
            .split_once('.')
            .unwrap_or((yuntun_model::ops::DEFAULT_SCHEMA, id.table.as_str()));
        format!("yuntun/{ns}/{table}/dt={}/shard={}/", id.window, id.shard)
    }
}

// ---------------------------------------------------------------- 门面

/// **分片存储门面**：把同一分片的两种形态收在一个句柄里，供写入侧与查询侧共享。
///
/// 装配（server）创建一次：`ShardStore::local(store)`，
/// 写入侧写内存分片、提交后交棒给磁盘分片；查询侧通过 [`ShardStore::hot`] 拿
/// [`ShardReader`]（分离部署可换成 `RemoteShard`）。
#[derive(Debug)]
pub struct ShardStore {
    memory: Arc<MemoryShard>,
    disk: Arc<DiskShard>,
}

impl ShardStore {
    /// 本地部署：进程内内存分片 + 给定 object store 上的磁盘分片。
    pub fn local(store: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            memory: MemoryShard::shared(),
            disk: Arc::new(DiskShard::new(store)),
        })
    }

    /// 写入侧：本地内存分片（热缓冲 + 世代维护）。
    pub fn memory(&self) -> &Arc<MemoryShard> {
        &self.memory
    }

    /// 只读侧：热数据读取器（阶段 0 = 进程内内存分片；分离部署替换为 [`RemoteShard`]）。
    pub fn hot(&self) -> Arc<dyn ShardReader> {
        self.memory.clone()
    }

    pub fn disk(&self) -> &Arc<DiskShard> {
        &self.disk
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as SArc;

    fn batch(v: i64) -> RecordBatch {
        let schema = SArc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![SArc::new(Int64Array::from(vec![v]))]).unwrap()
    }

    fn rows(b: &[RecordBatch]) -> usize {
        b.iter().map(|x| x.num_rows()).sum()
    }

    fn id() -> ShardId {
        ShardId::new("public.t", "default", "2026-09-12T10:00")
    }

    #[test]
    fn live_data_visible_immediately() {
        let p = MemoryShard::default();
        p.push(&id(), 1, 0, vec![batch(1)]);
        assert_eq!(rows(&p.readable("public.t", 0)), 1);
    }

    #[test]
    fn committed_visible_until_cache_catches_up() {
        let p = MemoryShard::default();
        p.push(&id(), 1, 0, vec![batch(1)]);
        p.mark_committed(&id(), &[1], 7);
        // 缓存还没到 7 → 仍可见（不能出现可见性空洞）
        assert_eq!(rows(&p.readable("public.t", 6)), 1);
        // 缓存已到 7 → Manifest 已覆盖，内存副本不再返回（避免重复计数）
        assert_eq!(rows(&p.readable("public.t", 7)), 0);
        p.sweep(7);
        assert!(p.is_empty());
    }

    #[test]
    fn drop_then_create_same_name_hides_old_epoch() {
        let p = MemoryShard::default();
        p.observe_ddl(ddl_op::CREATE_TABLE, "public.t");
        let e1 = p.liveness("public.t").epoch;
        p.push(&id(), 1, e1, vec![batch(42)]);
        assert_eq!(rows(&p.readable("public.t", 0)), 1);

        // DROP → CREATE：世代 +1，老数据立刻不可见，并在 sweep 中回收
        p.observe_ddl(ddl_op::DROP_TABLE, "public.t");
        p.observe_ddl(ddl_op::CREATE_TABLE, "public.t");
        assert!(p.is_stale("public.t", e1));
        assert_eq!(rows(&p.readable("public.t", 0)), 0);
        p.sweep(0);
        assert!(p.is_empty());
    }

    #[test]
    fn dropped_table_data_hidden() {
        let p = MemoryShard::default();
        p.observe_ddl(ddl_op::CREATE_TABLE, "public.t");
        let e = p.liveness("public.t").epoch;
        p.push(&id(), 1, e, vec![batch(1)]);
        p.observe_ddl(ddl_op::DROP_TABLE, "public.t");
        assert!(p.is_stale("public.t", e));
        assert_eq!(rows(&p.readable("public.t", 0)), 0);
        p.sweep(0);
        assert!(p.is_empty());
    }

    #[test]
    fn shards_isolated_by_window_and_shard_key() {
        let p = MemoryShard::default();
        p.push(&id(), 1, 0, vec![batch(1)]);
        p.push(
            &ShardId::new("public.t", "s1", "2026-09-12T10:00"),
            2,
            0,
            vec![batch(2)],
        );
        p.push(
            &ShardId::new("public.t", "default", "2026-09-12T10:01"),
            3,
            0,
            vec![batch(3)],
        );
        // 同表跨分片/窗口都算该表的可见热数据
        assert_eq!(rows(&p.readable("public.t", 0)), 3);
        assert_eq!(rows(&p.readable("public.other", 0)), 0);
    }

    #[test]
    fn disk_shard_prefix_matches_layout() {
        let store = crate::create_store(&crate::StoreConfig::Memory).unwrap();
        let disk = DiskShard::new(store);
        assert_eq!(disk.tier(), ShardTier::Disk);
        assert_eq!(
            disk.prefix(&ShardId::new("sales.orders", "s0", "2026-09-12T10:00")),
            "yuntun/sales/orders/dt=2026-09-12T10:00/shard=s0/"
        );
    }

    // ---- 读侧接缝：ShardReader（内存分片 / 远端分片两个 impl）----

    #[tokio::test]
    async fn memory_shard_as_shard_reader() {
        let m = MemoryShard::shared();
        m.push(&id(), 1, 0, vec![batch(1)]);
        m.push(
            &ShardId::new("public.t", "s1", "2026-09-12T10:00"),
            2,
            0,
            vec![batch(2)],
        );
        let reader: Arc<dyn ShardReader> = m.clone();
        assert_eq!(reader.tier(), ShardTier::Memory);
        let mut ids = reader.shards_of("public.t").await.unwrap();
        ids.sort();
        assert_eq!(ids.len(), 2, "按表枚举到两个分片");
        assert_eq!(rows(&reader.read_shard(&id(), 0).await.unwrap()), 1);
        assert_eq!(rows(&reader.read_table("public.t", 0).await.unwrap()), 2);
        // reclaim 落到 sweep：提交并让快照追上后本地副本清空
        m.mark_committed(&id(), &[1], 9);
        reader.reclaim(9);
        assert_eq!(rows(&reader.read_table("public.t", 9).await.unwrap()), 1);
    }

    /// 假传输：直接代理到本地内存分片（模拟"远端分片服务"）。
    #[derive(Debug)]
    struct FakeFetch {
        local: Arc<MemoryShard>,
    }

    impl ShardFetch for FakeFetch {
        fn fetch_shards<'a>(
            &'a self,
            table: &'a str,
        ) -> futures::future::BoxFuture<'a, Result<Vec<ShardId>, LakeError>> {
            Box::pin(async move {
                let mut ids: Vec<ShardId> = self
                    .local
                    .shards_of_ids(table);
                ids.sort();
                Ok(ids)
            })
        }

        fn fetch_shard<'a>(
            &'a self,
            id: &'a ShardId,
            cached_snapshot: u64,
        ) -> futures::future::BoxFuture<'a, Result<Vec<RecordBatch>, LakeError>> {
            Box::pin(async move { Ok(self.local.read_shard_sync(id, cached_snapshot)) })
        }
    }

    #[tokio::test]
    async fn remote_shard_reader_delegates_to_fetch() {
        let local = MemoryShard::shared();
        local.push(&id(), 1, 0, vec![batch(1)]);
        local.push(
            &ShardId::new("public.t", "s1", "2026-09-12T10:00"),
            2,
            0,
            vec![batch(2)],
        );
        let reader: Arc<dyn ShardReader> = Arc::new(RemoteShard::new(Arc::new(FakeFetch {
            local: local.clone(),
        })));
        assert_eq!(reader.tier(), ShardTier::Memory);
        // 默认 read_table = 枚举 + 逐分片（远端实现可覆写为单次 RPC）
        assert_eq!(rows(&reader.read_table("public.t", 0).await.unwrap()), 2);
        assert_eq!(reader.shards_of("public.t").await.unwrap().len(), 2);
        // 远端 reader 不驱动本地回收（GC 归服务端）
        local.mark_committed(&id(), &[1], 9);
        reader.reclaim(9);
        assert_eq!(rows(&reader.read_table("public.t", 9).await.unwrap()), 1);
    }
}
