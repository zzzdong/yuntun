//! 分片存储层的**读侧接缝**：磁盘分片 + 热数据读取接口（架构 §2.5 / §2.3）。
//!
//! 本模块只保留"存储形态"与"读取接缝"，**不再持有热数据实现**：
//!
//! | 层 | 归属 |
//! |---|---|
//! | 热数据（内存 chunk / spill） | `yuntun-chunk`（[`ChunkStore`] 实现 [`ShardReader`]） |
//! | 冷数据（对象存储上的 Parquet 文件） | 本模块 [`DiskShard`] + Catalog Manifest |
//! | 远端热数据（分离部署） | 本模块 [`RemoteShard`]（传输由 [`ShardFetch`] 注入） |
//!
//! ## 为什么把热数据实现挪出 store 层
//! 热数据要处理内存账本、背压阶梯、spill 与 seal 状态机（架构 §2.7 / §2.8 / §2.2），
//! 这些是"chunk 层"的职责。store 层只保留**分片的物理位置**与**读取接口**，
//! 于是：
//! - 查询侧只依赖 [`ShardReader`]，换实现（进程内 / 远端）**调用方零改动**；
//! - 写入侧不再有两套分组逻辑（旧 `MemoryShard` 与 chunk 分组曾各算一套）。

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use object_store::ObjectStore;

use yuntun_model::error::LakeError;

/// 分片的数据形态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardTier {
    /// 分片在**内存 / 本地磁盘**上的"热"数据（尚未落对象存储）。
    Memory,
    /// 分片在对象存储（本地磁盘 / S3）上、由 Manifest 索引的"冷"数据。
    Disk,
}

/// 分片标识：`(表全限定名, shard_key, time_window)`。
///
/// 与 `IngestBatch.shard_key` / 对象路径 `dt=<window>/shard=<shard>` /
/// chunk 归属键 `ChunkKey.shard` 一一对应。
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

/// 一次热数据拉取的结果**与边界信息**（`operation-log §61.4` 定死的契约）。
#[derive(Debug, Clone)]
pub struct ShardRead {
    /// 对 `known_manifest_ver` 而言尚不可冷读的那部分数据
    pub batches: Vec<RecordBatch>,
    /// **本实例已 flush 且已放弃本地副本的最高 manifest 版本**（单调不回退）。
    ///
    /// 为什么是"**已放弃副本**"而不是"已 commit"：本仓的回收是**惰性**的（由查询缓存 swap
    /// 驱动），"已 commit 但还没回收"窗口里数据仍在热副本中**读得到**，用 commit 记会让
    /// `stale` 每次查询都误报；用 release 记则**精确** —— 没回收 ⇒ 不可能丢数据。
    pub flushed_watermark: u64,
    /// `known_manifest_ver < flushed_watermark` ⇒ 本次结果**可能不完整**，
    /// 调用方**必须**刷新 manifest 后用新版本重试，**不得**把 `batches` 当完整答案
    /// （也别把空 `batches` 当"没有热数据" —— 那正是这个窗口会骗人的地方）。
    pub stale: bool,
}

impl ShardRead {
    /// 空数据 + 给定水位的构造（`stale` 按契约由水位与 `known` 比较得出）。
    pub fn empty(flushed_watermark: u64, known_manifest_ver: u64) -> Self {
        Self {
            batches: Vec::new(),
            flushed_watermark,
            stale: flushed_watermark > known_manifest_ver,
        }
    }

    /// 行数（调用方最常用的聚合；避免到处写 `iter().map(num_rows).sum()`）。
    pub fn rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
}

// ---------------------------------------------------------------- 读侧接缝

/// **热数据读接口**：查询侧唯一依赖的热数据入口。
///
/// - `yuntun_chunk::ChunkStore`：进程内 chunk（单机 / 与写入同进程）；
/// - [`RemoteShard`]：远端分片服务（分离部署，传输由 [`ShardFetch`] 注入）。
///
/// 语义：返回该分片**对 `known_manifest_ver` 尚不可冷读**的数据（+ 边界信息 [`ShardRead`]），
/// 用于与磁盘分片求并集。
///
/// **边界信息是契约的一部分**（`operation-log §61.4`）：调用方拿旧 manifest 版本去拉热数据时，
/// 对方可能**已经放弃**了那批数据的本地副本（已 commit 并 reclaim）——此时热数据拉不到、
/// 调用方的 manifest 里也还没有那些文件，就是 `architecture-with-chunk §4.5` 的
/// "两头都没有"。`ShardRead::stale` 就是给这个窗口的信号：**刷新 manifest 后用新版本重试**。
#[async_trait]
pub trait ShardReader: Send + Sync + std::fmt::Debug {
    fn tier(&self) -> ShardTier;

    /// 变更计数：查询侧据此做"近实时刷新"触发（远端实现取服务端水位，取不到则 0 → 退化为 TTL）。
    fn version(&self) -> u64;

    /// 枚举某表当前存在的分片。
    async fn shards_of(&self, table: &str) -> Result<Vec<ShardId>, LakeError>;

    /// 读**单个分片**对 `known_manifest_ver` 尚不可冷读的数据 + 边界信息。
    ///
    /// `known_manifest_ver` = 调用方所依据的 manifest 版本（通常就是它的 Catalog 快照号）。
    async fn read_shard(
        &self,
        id: &ShardId,
        known_manifest_ver: u64,
    ) -> Result<ShardRead, LakeError>;

    /// **带栅栏**地读单个分片（`operation-log §28.1` 的读侧栅栏）。
    ///
    /// `exclude` = 调用方**已经能在自己那份 manifest 快照里读到**的批次
    /// （可见文件的 `batch_id`）：属于它们的本地热副本**不得**再回一次，否则在
    /// 「`commit_files` 成功 → 调用方 `mark_committed`」的窗口里，同一批数据会**同时**
    /// 从"已提交文件"与"热数据"被读到（重复计数）。
    ///
    /// 默认实现**忽略** `exclude`：对"本地热数据即权威"的实现（`ChunkStore`）必须覆写；
    /// 远端实现在**服务端**落这道栅栏（见 [`ShardFetch::fetch_shard`] 的 `known_batch_ids`）。
    async fn read_shard_excluding(
        &self,
        id: &ShardId,
        known_manifest_ver: u64,
        exclude: &[String],
    ) -> Result<ShardRead, LakeError> {
        let _ = exclude;
        self.read_shard(id, known_manifest_ver).await
    }

    /// **本实例的水位**（`operation-log §63.2` 的语义：已 flush 且**已放弃本地副本**的最高版本）。
    ///
    /// **必须单独实现，不能从分片推**：水位是**实例级**属性，与"当前有没有分片"无关 ——
    /// 实例刚放弃某批副本时（`reclaim` 之后），分片枚举恰好是**空**的，若把水位当成分片的
    /// 派生量，就会把"没有分片"误报成"没有已放弃的数据"（`stale = false`），
    /// 调用方于是**静默丢掉那批数据**。这不是假设：`shardrpc` 的远端往返用例当场抓到过
    /// （原始记录见 `operation-log §67`）。
    async fn watermark(&self, known_manifest_ver: u64) -> Result<ShardRead, LakeError>;

    /// 便捷：读整表（默认 = 枚举 + 逐分片 + **实例水位**；远端实现可覆写为更少的往返）。
    ///
    /// 合并口径：数据拼接；**水位与 `stale` 一律取 [`Self::watermark`] 的**（实例级属性，
    /// 分片级的值只是它的投影，不能拿来替代）。顺序也是刻意的：**先读分片、后取水位** ——
    /// 若在两者之间有数据被放弃，后取的水位能覆盖它（反过来会漏）。
    async fn read_table(
        &self,
        table: &str,
        known_manifest_ver: u64,
    ) -> Result<ShardRead, LakeError> {
        self.read_table_excluding(table, known_manifest_ver, &[]).await
    }

    /// 同 [`Self::read_table`]，但带**读侧栅栏**（`exclude` 见 [`Self::read_shard_excluding`]）。
    ///
    /// 查询侧必须走这条（把快照里"该实例已提交的 batch 集合"带上）；
    /// [`Self::read_table`] 只留给"没有 manifest 视图"的调用方（单测/诊断）。
    async fn read_table_excluding(
        &self,
        table: &str,
        known_manifest_ver: u64,
        exclude: &[String],
    ) -> Result<ShardRead, LakeError> {
        let mut out = Vec::new();
        for id in self.shards_of(table).await? {
            out.extend(
                self.read_shard_excluding(&id, known_manifest_ver, exclude)
                    .await?
                    .batches,
            );
        }
        let wm = self.watermark(known_manifest_ver).await?;
        Ok(ShardRead {
            batches: out,
            flushed_watermark: wm.flushed_watermark,
            stale: wm.stale,
        })
    }

    /// 回收**本地副本**中已被 `cached_snapshot` 覆盖（或已陈旧世代）的条目。
    ///
    /// 默认空实现：远端分片服务的 GC 由其自身负责，不由查询节点驱动。
    fn reclaim(&self, _cached_snapshot: u64) {}
}

/// 远端分片服务的**传输接缝**。
///
/// 分离部署用 gRPC / HTTP 实现；单测用假实现。本 trait 只关心"取哪些分片 / 取某分片的数据"，
/// 不关心序列化与连接管理，避免把传输细节泄漏到查询侧。
pub trait ShardFetch: Send + Sync {
    /// 枚举某表当前存在的分片（远端服务上的）。
    fn fetch_shards<'a>(
        &'a self,
        table: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<Vec<ShardId>, LakeError>>;

    /// 读单个分片对 `known_manifest_ver` 尚不可冷读的数据。
    ///
    /// **响应必须带边界信息**（`architecture-with-chunk §4.4`：水位随 pull 响应回来，
    /// 而不是靠推送）—— 远端实现若拿不到真实水位，**不能**用 0 装作"没有已放弃的数据"，
    /// 那会把 STALE 静默关掉；正确做法是让服务端把它的 watermark 一起返回（S5-6）。
    ///
    /// `known_batch_ids` = 调用方**已经能读到的批次**（读侧栅栏，`operation-log §28.1`）：
    /// 服务端**按它过滤**，不得自行推断 —— "哪些数据已进调用方的 manifest"只有调用方知道。
    /// 用 owned `Vec` 而不是借用，是为了不被 `&'a self` 的生命周期绑住。
    fn fetch_shard<'a>(
        &'a self,
        id: &'a ShardId,
        known_manifest_ver: u64,
        known_batch_ids: Vec<String>,
    ) -> futures::future::BoxFuture<'a, Result<ShardRead, LakeError>>;

    /// **实例级水位**（`FetchShard` 的边界信息走这里，而不是从分片推）。
    ///
    /// **没有默认实现是刻意的**：任何"默认值"（包括 0）都等于**静默关掉 STALE**，
    /// 而那正是 `§67` 记录的那个静默丢数据。远端实现拿不到真实水位时应当报错，
    /// 不该假装"没有已放弃的数据"。
    fn fetch_watermark<'a>(
        &'a self,
        known_manifest_ver: u64,
    ) -> futures::future::BoxFuture<'a, Result<ShardRead, LakeError>>;

    /// 服务端变更水位（拿不到返回 0：查询侧退化为 TTL 刷新）。
    fn fetch_version(&self) -> u64 {
        0
    }
}

/// 远端分片读取器：把 [`ShardFetch`] 适配成 [`ShardReader`]。
///
/// 查询节点在分离部署下用本结构替换进程内 chunk store（**调用方零改动**）。
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
        known_manifest_ver: u64,
    ) -> Result<ShardRead, LakeError> {
        self.fetch
            .fetch_shard(id, known_manifest_ver, Vec::new())
            .await
    }

    /// 栅栏在**服务端**落（`§28.1`）：把调用方已知的批集合原样传过去，本地不做判断
    /// —— 本地只有"怎么连"，没有"哪些数据已提交"。
    async fn read_shard_excluding(
        &self,
        id: &ShardId,
        known_manifest_ver: u64,
        exclude: &[String],
    ) -> Result<ShardRead, LakeError> {
        self.fetch
            .fetch_shard(id, known_manifest_ver, exclude.to_vec())
            .await
    }

    async fn watermark(&self, known_manifest_ver: u64) -> Result<ShardRead, LakeError> {
        self.fetch.fetch_watermark(known_manifest_ver).await
    }
    // `read_table` 用默认实现（枚举 + 逐片 + 水位）；reclaim 用默认空实现（GC 归服务端）
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc as SArc;

    fn batch(v: i64) -> RecordBatch {
        let schema = SArc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![SArc::new(Int64Array::from(vec![v]))]).unwrap()
    }

    fn id() -> ShardId {
        ShardId::new("public.t", "default", "2026-09-12T10:00")
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
        // 裸表名（旧数据）按 public 归一
        assert_eq!(
            disk.prefix(&ShardId::new("orders", "s0", "w")),
            "yuntun/public/orders/dt=w/shard=s0/"
        );
    }

    /// 假传输：一份静态的"远端分片服务"（不依赖本地实现，真正独立）。
    #[derive(Debug)]
    struct CannedFetch {
        entries: HashMap<ShardId, Vec<RecordBatch>>,
    }

    impl ShardFetch for CannedFetch {
        fn fetch_shards<'a>(
            &'a self,
            table: &'a str,
        ) -> futures::future::BoxFuture<'a, Result<Vec<ShardId>, LakeError>> {
            Box::pin(async move {
                let mut ids: Vec<ShardId> = self
                    .entries
                    .keys()
                    .filter(|i| i.table == table)
                    .cloned()
                    .collect();
                ids.sort();
                Ok(ids)
            })
        }

        fn fetch_watermark<'a>(
            &'a self,
            known_manifest_ver: u64,
        ) -> futures::future::BoxFuture<'a, Result<ShardRead, LakeError>> {
            // 与 `fetch_shard` 同源：水位固定 42（假服务端）
            Box::pin(async move { Ok(ShardRead::empty(42, known_manifest_ver)) })
        }

        fn fetch_shard<'a>(
            &'a self,
            id: &'a ShardId,
            known_manifest_ver: u64,
            _known_batch_ids: Vec<String>,
        ) -> futures::future::BoxFuture<'a, Result<ShardRead, LakeError>> {
            Box::pin(async move {
                let batches = self.entries.get(id).cloned().unwrap_or_default();
                // 假服务端：水位固定 42（与 `fetch_version` 同源），于是 `known < 42` 必须报 STALE
                Ok(ShardRead {
                    batches,
                    flushed_watermark: 42,
                    stale: 42 > known_manifest_ver,
                })
            })
        }

        fn fetch_version(&self) -> u64 {
            42
        }
    }

    #[tokio::test]
    async fn remote_shard_reader_delegates_to_fetch() {
        let mut entries = HashMap::new();
        entries.insert(id(), vec![batch(1)]);
        entries.insert(
            ShardId::new("public.t", "s1", "2026-09-12T10:00"),
            vec![batch(2)],
        );
        let reader: Arc<dyn ShardReader> =
            Arc::new(RemoteShard::new(Arc::new(CannedFetch { entries })));
        assert_eq!(reader.tier(), ShardTier::Memory);
        assert_eq!(reader.version(), 42, "远端水位供查询侧刷新触发");
        // 契约：响应带回水位；`known=42` 到位 ⇒ 不 STALE
        let all = reader.read_table("public.t", 42).await.unwrap();
        assert_eq!(all.rows(), 2);
        assert_eq!(all.flushed_watermark, 42);
        assert!(!all.stale, "known 已到水位，不该报 STALE");
        assert_eq!(reader.shards_of("public.t").await.unwrap().len(), 2);
        assert_eq!(reader.read_shard(&id(), 42).await.unwrap().rows(), 1);

        // 拿旧 manifest 版本读 ⇒ **必须**报警：对方可能已放弃那批数据的本地副本
        let behind = reader.read_table("public.t", 41).await.unwrap();
        assert!(behind.stale, "known 落后于水位必须报 STALE");
        assert_eq!(behind.flushed_watermark, 42);
        // 远端 reader 不驱动本地回收（GC 归服务端）
        reader.reclaim(9);
    }
}
