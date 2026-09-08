//! 本地 Catalog 缓存（C7：Query 无网络调用的唯一允许路径，详细设计 §6.7 / ADR-6）。
//!
//! 后台任务周期刷新（默认 30s TTL，§11 [query].cache_ttl）：
//! tables + schemas + 当前快照可见文件。
//! 查询线程只读内存 —— 任何同步读路径都不得触碰 Catalog 网络。

use datafusion::error::DataFusionError;
use yuntun_catalog::CatalogOps;
use yuntun_model::meta::{FileManifest, TableMeta};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

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
#[derive(Debug, Default)]
pub struct LocalCatalogCache {
    pub(crate) tables: RwLock<HashMap<String, CachedTable>>,
}

impl LocalCatalogCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// 从 Catalog 全量刷新。
    pub async fn refresh(&self, catalog: &Arc<dyn CatalogOps>) -> Result<usize, DataFusionError> {
        let snapshot = catalog.current_snapshot().await;
        let tables = catalog
            .list_tables()
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let mut map = HashMap::with_capacity(tables.len());
        for meta in tables {
            let (schema, version) = match catalog.table_schema(&meta.name).await {
                Ok(Some(x)) => x,
                Ok(None) => continue,
                Err(e) => return Err(DataFusionError::Execution(e.to_string())),
            };
            let files = catalog
                .list_visible_files(&meta.name, snapshot, None)
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            map.insert(
                meta.name.clone(),
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
        Ok(n)
    }

    /// 同步读（C7：调用方为 DataFusion 同步/异步 trait，无网络）。
    pub async fn get(&self, table: &str) -> Option<CachedTable> {
        self.tables.read().await.get(table).cloned()
    }

    pub async fn table_names(&self) -> Vec<String> {
        self.tables.read().await.keys().cloned().collect()
    }

    pub async fn table_count(&self) -> usize {
        self.tables.read().await.len()
    }
}

/// 缓存刷新后台任务（§6.7：TTL 30s，catch-up 可选）。
pub fn spawn_cache_refresh(
    cache: Arc<LocalCatalogCache>,
    catalog: Arc<dyn CatalogOps>,
    ttl: Duration,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(ttl);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // 启动立即刷一次（保证首个查询就有数据）
        if let Err(e) = cache.refresh(&catalog).await {
            tracing::error!(error = %e, "catalog cache refresh failed on startup");
        }
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }
            match cache.refresh(&catalog).await {
                Ok(n) => tracing::debug!(tables = n, "catalog cache refreshed"),
                Err(e) => tracing::error!(error = %e, "catalog cache refresh failed"),
            }
        }
    })
}
