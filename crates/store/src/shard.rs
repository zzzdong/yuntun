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

// ---------------------------------------------------------------- 读侧接缝

/// **热数据读接口**：查询侧唯一依赖的热数据入口。
///
/// - `yuntun_chunk::ChunkStore`：进程内 chunk（单机 / 与写入同进程）；
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
/// 分离部署用 gRPC / HTTP 实现；单测用假实现。本 trait 只关心"取哪些分片 / 取某分片的数据"，
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
        cached_snapshot: u64,
    ) -> Result<Vec<RecordBatch>, LakeError> {
        self.fetch.fetch_shard(id, cached_snapshot).await
    }
    // reclaim 用默认空实现：远端服务的 GC 由服务端负责
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

    fn rows(b: &[RecordBatch]) -> usize {
        b.iter().map(|x| x.num_rows()).sum()
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

        fn fetch_shard<'a>(
            &'a self,
            id: &'a ShardId,
            _cached_snapshot: u64,
        ) -> futures::future::BoxFuture<'a, Result<Vec<RecordBatch>, LakeError>> {
            Box::pin(async move { Ok(self.entries.get(id).cloned().unwrap_or_default()) })
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
        assert_eq!(rows(&reader.read_table("public.t", 0).await.unwrap()), 2);
        assert_eq!(reader.shards_of("public.t").await.unwrap().len(), 2);
        assert_eq!(rows(&reader.read_shard(&id(), 0).await.unwrap()), 1);
        // 远端 reader 不驱动本地回收（GC 归服务端）
        reader.reclaim(9);
    }
}
