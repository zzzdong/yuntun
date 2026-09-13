//! 本地 Catalog 缓存（C7：Query 无网络调用的唯一允许路径，详细设计 §6.7 / ADR-6）。
//!
//! 后台任务周期刷新（默认 30s TTL，§11 [query].cache_ttl）：
//! tables + schemas + 当前快照可见文件。
//! 查询线程只读内存 —— 任何同步读路径都不得触碰 Catalog 网络。

use datafusion::error::DataFusionError;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use yuntun_catalog::CatalogOps;
use yuntun_model::meta::{FileManifest, TableMeta};
use yuntun_store::ShardReader;

/// 缓存的表条目。
#[derive(Debug, Clone)]
pub struct CachedTable {
    pub meta: TableMeta,
    pub schema: arrow::datatypes::SchemaRef,
    pub schema_version: u64,
    /// 刷新时刻的可见文件（快照隔离：以刷新时的 snapshot 为界）
    pub files: Vec<FileManifest>,
    pub snapshot: u64,
}

/// 本地缓存（进程内共享）。
///
/// **多 schema**：`tables` 以全限定表标识 `schema.table` 为键（跨 schema 同名表可区分）；
/// `schemas` 为 schema 清单（DataFusion `CatalogProvider::schema_names` 用）。
#[derive(Debug, Default)]
pub struct LocalCatalogCache {
    pub(crate) tables: RwLock<HashMap<String, CachedTable>>,
    pub(crate) schemas: RwLock<Vec<String>>,
    /// 热数据读侧（store 层的 [`ShardReader`]）：查询据此读**内存分片**（尚未落盘的热数据）。
    /// 阶段 0 = 进程内内存分片；分离部署 = `RemoteShard`（远端分片服务）。
    /// `None` = 未接线（单测/纯磁盘分片节点），退化为纯 Manifest 可见性。
    pub(crate) hot: std::sync::RwLock<Option<Arc<dyn ShardReader>>>,
}

impl LocalCatalogCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// 接线热数据读侧（server 装配时调用；与写入侧共享同一内存分片或注入远端实现）。
    pub fn set_hot_shards(&self, hot: Arc<dyn ShardReader>) {
        *self.hot.write().unwrap() = Some(hot);
    }

    /// 热数据读侧句柄（TableProvider 在 scan 时读内存分片）。
    pub fn hot_shards(&self) -> Option<Arc<dyn ShardReader>> {
        self.hot.read().unwrap().clone()
    }

    /// 内存分片变更计数（写入/提交/DDL）：供刷新任务做"近实时刷新"触发。
    pub fn shard_version(&self) -> u64 {
        self.hot
            .read()
            .unwrap()
            .as_ref()
            .map(|s| s.version())
            .unwrap_or(0)
    }

    /// 从 Catalog 全量刷新（表 + schema 清单）。
    pub async fn refresh(&self, catalog: &Arc<dyn CatalogOps>) -> Result<usize, DataFusionError> {
        let snapshot = catalog.current_snapshot().await;
        let schemas = catalog
            .list_schemas()
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let tables = catalog
            .list_tables()
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let mut map = HashMap::with_capacity(tables.len());
        for meta in tables {
            let ident = meta.qualified_name();
            let (schema, version) = match catalog.table_schema(&ident).await {
                Ok(Some(x)) => x,
                Ok(None) => continue,
                Err(e) => return Err(DataFusionError::Execution(e.to_string())),
            };
            let files = catalog
                .list_visible_files(&ident, snapshot, None)
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            tracing::debug!(ident = %ident, files = files.len(), snapshot, "cache refresh: table");
            map.insert(
                ident,
                CachedTable {
                    meta,
                    schema,
                    schema_version: version,
                    files,
                    snapshot,
                },
            );
        }
        let n = map.len();
        *self.tables.write().await = map;
        *self.schemas.write().await = schemas;
        // 回收本地副本里已被本次快照覆盖（或已陈旧世代）的内存分片条目
        // （远端分片服务的 GC 由其自身负责：`reclaim` 默认空实现）
        if let Some(s) = self.hot_shards() {
            s.reclaim(snapshot);
        }
        Ok(n)
    }

    /// 同步读（C7：调用方为 DataFusion 同步/异步 trait，无网络）：按全限定标识。
    pub async fn get(&self, qualified_table: &str) -> Option<CachedTable> {
        self.tables.read().await.get(qualified_table).cloned()
    }

    /// 按 (schema, 裸表名) 读——DataFusion `SchemaProvider::table(name)` 走这里。
    pub async fn get_in(&self, namespace: &str, table: &str) -> Option<CachedTable> {
        self.get(&yuntun_model::ops::qualified_name(namespace, table))
            .await
    }

    /// schema 清单（缓存刷新时更新）。
    pub async fn schema_names(&self) -> Vec<String> {
        self.schemas.read().await.clone()
    }

    pub async fn table_names(&self) -> Vec<String> {
        self.tables.read().await.keys().cloned().collect()
    }

    pub async fn table_count(&self) -> usize {
        self.tables.read().await.len()
    }
}

/// 缓存刷新后台任务（§6.7 + 提交驱动）。
///
/// 【修复】此前只在 TTL（默认 30s）到点刷新：即使 flush 已 commit，查询最长还要
/// 盲 30s（写后"查不到"的主因之一）。现在改为**变更驱动**：内存视图（写入/提交/DDL）
/// 计数一变就立刻刷新，TTL 仅作兜底（元数据/外部变更）。
pub fn spawn_cache_refresh(
    cache: Arc<LocalCatalogCache>,
    catalog: Arc<dyn CatalogOps>,
    ttl: Duration,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let poll = Duration::from_millis(200)
            .min(ttl)
            .max(Duration::from_millis(10));
        let mut interval = tokio::time::interval(poll);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // 启动立即刷一次（保证首个查询就有数据）
        if let Err(e) = cache.refresh(&catalog).await {
            tracing::error!(error = %e, "catalog cache refresh failed on startup");
        }
        let mut last_version = cache.shard_version();
        let mut last_refresh = std::time::Instant::now();
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }
            let version = cache.shard_version();
            if version == last_version && last_refresh.elapsed() < ttl {
                continue;
            }
            match cache.refresh(&catalog).await {
                Ok(n) => tracing::debug!(tables = n, "catalog cache refreshed"),
                Err(e) => tracing::error!(error = %e, "catalog cache refresh failed"),
            }
            // refresh 内部 sweep 也会推进计数，因此刷新后再取一次作为基线
            last_version = cache.shard_version();
            last_refresh = std::time::Instant::now();
        }
    })
}
