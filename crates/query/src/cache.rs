//! 本地 Catalog 物化视图（C7：Query 无网络调用的唯一允许路径）。
//!
//! ## 形态（`refactor.md` S2-1 ~ S2-8）
//!
//! | 概念 | 说明 |
//! |---|---|
//! | [`CatalogSnapshot`] | **不可变快照**：一次查询取一次，查询期间不再读可变结构 |
//! | [`LocalCatalog`] | 物化视图宿主：由任意 [`CatalogOps`] 刷新，查询侧只读它 |
//! | 刷新 | **版本驱动 + 增量**：`schema_ver` 变 → 全量重建；仅 `manifest_ver` 变 → 只重拉变化的表 |
//!
//! ## 为什么必须"不可变快照"（S2-4）
//! DataFusion 的 `CatalogProvider` / `SchemaProvider` / `TableProvider` 是**同步** trait，
//! 规划期会被反复调用。若每次都去读一个会变的结构：
//! - 同一次查询的 **plan 与 scan 可能看到两个不同版本**（漏读/幻读）；
//! - 每次 `table()` 都要深拷贝文件清单（大表下是显著开销）。
//!
//! 因此刷新是"**构建新快照 → 原子替换**"（写时复制），读侧只拿 `Arc`。
//!
//! ## 为什么版本要分两组（S2-5）
//! flush 是最高频的写。若 schema 与 manifest 共用一个版本号，
//! **每次 flush 都会让全表 schema 缓存失效** → 缓存退化为全量重建。
//! 分组后：DDL 才全量，写入走增量（S2-7）。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use datafusion::error::DataFusionError;
use tokio_util::sync::CancellationToken;
use yuntun_catalog::CatalogOps;
use yuntun_model::meta::{FileManifest, TableMeta};
use yuntun_model::ops::CatalogVersion;
use yuntun_store::ShardReader;

/// 物化的表条目（**不可变**：只存在于 [`CatalogSnapshot`] 里，不单独对外可变借用）。
#[derive(Debug, Clone)]
pub struct CachedTable {
    pub meta: TableMeta,
    pub schema: arrow::datatypes::SchemaRef,
    pub schema_version: u64,
    /// 该表在 `snapshot` 下可见的文件（快照隔离：以刷新时的 snapshot 为界）
    pub files: Vec<FileManifest>,
}

/// **不可变 Catalog 快照**（S2-4）：一次查询共用一份。
///
/// - `Arc` 共享 → `table()` 不再深拷贝文件清单；
/// - 构建后不再修改 → 同一查询的 plan 与 scan 看到**同一个** Catalog 版本。
#[derive(Debug, Default, Clone)]
pub struct CatalogSnapshot {
    /// 全限定表标识 `schema.table` → 物化条目（`Arc`：快照写时复制的代价与文件数无关）
    pub tables: HashMap<String, Arc<CachedTable>>,
    /// schema 清单（DataFusion `CatalogProvider::schema_names` 用）
    pub schemas: Vec<String>,
    /// 构建该快照时的 Catalog 版本（下次刷新据此判断走全量还是增量）
    pub version: CatalogVersion,
    /// 构建该快照时的可见快照号（文件可见性边界）
    pub snapshot: u64,
    /// 数据节点列表。standalone = 本节点；R4/R5 起为真实成员表
    /// （**提前放在快照里**：查询规划需要的"分片归属"必须与 schema/manifest 同版本）
    pub nodes: Vec<String>,
}

impl CatalogSnapshot {
    /// 按全限定标识取表（同步、无锁、无网络 —— C7）。
    pub fn get(&self, qualified_table: &str) -> Option<&CachedTable> {
        self.tables.get(qualified_table).map(|t| t.as_ref())
    }

    /// 按 `(schema, 裸表名)` 取表（DataFusion `SchemaProvider::table(name)` 走这里）。
    pub fn get_in(&self, namespace: &str, table: &str) -> Option<&CachedTable> {
        self.get(&yuntun_model::ops::qualified_name(namespace, table))
    }

    /// 某 schema 下的裸表名（升序）。
    pub fn table_names_in(&self, namespace: &str) -> Vec<String> {
        let mut v: Vec<String> = self
            .tables
            .values()
            .filter(|t| t.meta.schema_name() == namespace)
            .map(|t| t.meta.name.clone())
            .collect();
        v.sort();
        v
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}

/// 一次刷新的结果（观测/日志用；也让调用方知道"这次贵不贵"）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshOutcome {
    /// true = 全量重建（DDL 或增量无法表达）
    pub full: bool,
    /// 本次实际重拉的表数
    pub tables_refreshed: usize,
    /// 本次看到的版本
    pub version: CatalogVersion,
    pub snapshot: u64,
}

/// 本地物化视图的刷新统计（观测用，`plan.md` T6.12）。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LocalCatalogStats {
    pub refreshes: u64,
    pub full_reloads: u64,
    /// 增量路径累计重拉的表数（用于对比"全量重建"的节省）
    pub delta_tables: u64,
    pub snapshot: u64,
    pub version: CatalogVersion,
    pub tables: usize,
}

/// 本地 Catalog（S2-2）：**由任意 `CatalogOps` 物化而来**，查询侧只读它。
///
/// 阶段 0/1 的 `CatalogOps` 是 `MemoryCatalog`（同进程），R3 之后换成 gRPC 客户端 ——
/// **本结构一行不改**：它只依赖 trait。
#[derive(Debug)]
pub struct LocalCatalog {
    current: RwLock<Arc<CatalogSnapshot>>,
    /// 热数据读侧（store 层 [`ShardReader`]）：查询据此读**chunk**（尚未落盘的热数据）。
    /// 阶段 0 = 进程内 chunk store；分离部署 = `RemoteShard`。
    hot: RwLock<Option<Arc<dyn ShardReader>>>,
    refreshes: AtomicU64,
    full_reloads: AtomicU64,
    delta_tables: AtomicU64,
    /// 最近一次刷新失败的原因（观测用；刷新失败不清空旧快照 —— 宁可读旧数据也别读不到）
    last_error: RwLock<Option<String>>,
    /// 数据节点列表（standalone = 单节点）。**提前放进快照**：查询规划中的
    /// "分片归属"必须与 schema/manifest 同一版本，否则会跨版本拼计划。
    nodes: RwLock<Vec<String>>,
}

impl Default for LocalCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalCatalog {
    pub fn new() -> Self {
        Self {
            current: RwLock::new(Arc::new(CatalogSnapshot::default())),
            hot: RwLock::new(None),
            refreshes: AtomicU64::new(0),
            full_reloads: AtomicU64::new(0),
            delta_tables: AtomicU64::new(0),
            last_error: RwLock::new(None),
            nodes: RwLock::new(vec!["standalone".to_string()]),
        }
    }

    /// 接线热数据读侧（装配时调用；与写入侧共享同一 chunk store 或注入远端实现）。
    pub fn set_hot_shards(&self, hot: Arc<dyn ShardReader>) {
        *self.hot.write().unwrap() = Some(hot);
    }

    /// 热数据读侧句柄（TableProvider 在 scan 时读热数据）。
    pub fn hot_shards(&self) -> Option<Arc<dyn ShardReader>> {
        self.hot.read().unwrap().clone()
    }

    /// 热分片变更计数（写入/提交/DDL）：供刷新任务做**提交驱动刷新**。
    pub fn shard_version(&self) -> u64 {
        self.hot
            .read()
            .unwrap()
            .as_ref()
            .map(|s| s.version())
            .unwrap_or(0)
    }

    /// **当前不可变快照**：一次查询取一次，之后整条链路共用（S2-4）。
    pub fn snapshot(&self) -> Arc<CatalogSnapshot> {
        self.current.read().unwrap().clone()
    }

    /// 按全限定标识取表（便捷；等价于 `snapshot().get(..)` 的克隆）。
    pub async fn get(&self, qualified_table: &str) -> Option<CachedTable> {
        self.snapshot().get(qualified_table).cloned()
    }

    /// 按 `(schema, 裸表名)` 取表。
    pub async fn get_in(&self, namespace: &str, table: &str) -> Option<CachedTable> {
        self.snapshot().get_in(namespace, table).cloned()
    }

    pub async fn schema_names(&self) -> Vec<String> {
        self.snapshot().schemas.clone()
    }

    pub async fn table_names(&self) -> Vec<String> {
        self.snapshot().tables.keys().cloned().collect()
    }

    pub async fn table_count(&self) -> usize {
        self.snapshot().table_count()
    }

    /// 刷新统计（观测用）。
    pub fn stats(&self) -> LocalCatalogStats {
        let snap = self.snapshot();
        LocalCatalogStats {
            refreshes: self.refreshes.load(Ordering::SeqCst),
            full_reloads: self.full_reloads.load(Ordering::SeqCst),
            delta_tables: self.delta_tables.load(Ordering::SeqCst),
            snapshot: snap.snapshot,
            version: snap.version,
            tables: snap.table_count(),
        }
    }

    /// 最近一次刷新失败原因（`None` = 健康）。
    pub fn last_error(&self) -> Option<String> {
        self.last_error.read().unwrap().clone()
    }

    /// **版本驱动刷新**（S2-4 / S2-5 / S2-6 / S2-7）。
    ///
    /// ```text
    /// version() 一次调用（无变化时零开销返回）
    ///   ├─ schema_ver 变 → 全量重建（DDL 是低频事件，重建代价可接受）
    ///   ├─ 仅 manifest_ver 变 → 拉增量，只重拉"变过的表"
    ///   └─ 都没变 → 什么都不做（连快照都不重建）
    /// ```
    ///
    /// **失败语义**：拉取失败时**保留旧快照**（宁可读稍旧的数据，也不要查询不可用），
    /// 并记录 `last_error`；下一次刷新会重试。
    pub async fn refresh(
        &self,
        catalog: &Arc<dyn CatalogOps>,
    ) -> Result<RefreshOutcome, DataFusionError> {
        let version = catalog.version().await;
        let current = self.snapshot();

        let outcome = if version.schema_ver != current.version.schema_ver {
            self.full_reload(catalog, version).await?
        } else if version.manifest_ver != current.version.manifest_ver {
            self.incremental_reload(catalog, &current, version).await?
        } else {
            // 无版本变化：连快照也不重建（S2-6：无变化零开销返回）
            RefreshOutcome {
                full: false,
                tables_refreshed: 0,
                version,
                snapshot: current.snapshot,
            }
        };

        self.refreshes.fetch_add(1, Ordering::SeqCst);
        *self.last_error.write().unwrap() = None;
        Ok(outcome)
    }

    /// 全量重建：表清单 + 每个表的 schema 与可见文件（一次一个新快照，写时复制）。
    async fn full_reload(
        &self,
        catalog: &Arc<dyn CatalogOps>,
        version: CatalogVersion,
    ) -> Result<RefreshOutcome, DataFusionError> {
        let snapshot_no = catalog.current_snapshot().await;
        let schemas = catalog
            .list_schemas()
            .await
            .map_err(|e| self.record_error(e.to_string()))?;
        let metas = catalog
            .list_tables()
            .await
            .map_err(|e| self.record_error(e.to_string()))?;

        let mut tables: HashMap<String, Arc<CachedTable>> = HashMap::with_capacity(metas.len());
        for meta in metas {
            let ident = meta.qualified_name();
            let (schema, schema_version) = match catalog.table_schema(&ident).await {
                Ok(Some(x)) => x,
                Ok(None) => continue,
                Err(e) => return Err(self.record_error(e.to_string())),
            };
            let files = catalog
                .list_visible_files(&ident, snapshot_no, None)
                .await
                .map_err(|e| self.record_error(e.to_string()))?;
            tables.insert(
                ident,
                Arc::new(CachedTable {
                    meta,
                    schema,
                    schema_version,
                    files,
                }),
            );
        }

        let n = tables.len();
        self.swap(CatalogSnapshot {
            tables,
            schemas,
            version,
            snapshot: snapshot_no,
            nodes: self.nodes(),
        });
        self.full_reloads.fetch_add(1, Ordering::SeqCst);
        tracing::debug!(tables = n, snapshot = snapshot_no, "local catalog: full reload");
        Ok(RefreshOutcome {
            full: true,
            tables_refreshed: n,
            version,
            snapshot: snapshot_no,
        })
    }

    /// 增量刷新：只重拉"自上次版本以来文件清单变过的表"（S2-7）。
    async fn incremental_reload(
        &self,
        catalog: &Arc<dyn CatalogOps>,
        current: &Arc<CatalogSnapshot>,
        version: CatalogVersion,
    ) -> Result<RefreshOutcome, DataFusionError> {
        let delta = catalog
            .manifest_delta(current.version.manifest_ver)
            .await
            .map_err(|e| self.record_error(e.to_string()))?;
        if delta.full_reload_required {
            return self.full_reload(catalog, version).await;
        }
        if delta.changed_tables.is_empty() {
            // 版本前进了但没有表清单变化（例如幂等重复提交）：只推进版本，数据不动
            let mut next = (**current).clone();
            next.version = version;
            next.snapshot = catalog.current_snapshot().await;
            self.swap(next);
            return Ok(RefreshOutcome {
                full: false,
                tables_refreshed: 0,
                version,
                snapshot: catalog.current_snapshot().await,
            });
        }

        let snapshot_no = catalog.current_snapshot().await;
        let mut next = (**current).clone();
        for ident in &delta.changed_tables {
            match catalog.get_table(ident).await {
                Ok(Some(meta)) => {
                    let (schema, schema_version) = match catalog.table_schema(ident).await {
                        Ok(Some(x)) => x,
                        Ok(None) => continue,
                        Err(e) => return Err(self.record_error(e.to_string())),
                    };
                    let files = catalog
                        .list_visible_files(ident, snapshot_no, None)
                        .await
                        .map_err(|e| self.record_error(e.to_string()))?;
                    next.tables.insert(
                        ident.clone(),
                        Arc::new(CachedTable {
                            meta,
                            schema,
                            schema_version,
                            files,
                        }),
                    );
                }
                // 表已不在（被删）→ 从快照移除
                Ok(None) => {
                    next.tables.remove(ident);
                }
                Err(e) => return Err(self.record_error(e.to_string())),
            }
        }
        next.version = version;
        next.snapshot = snapshot_no;
        next.nodes = self.nodes();
        let refreshed = delta.changed_tables.len();
        self.swap(next);
        self.delta_tables
            .fetch_add(refreshed as u64, Ordering::SeqCst);
        tracing::debug!(
            tables = refreshed,
            total = self.snapshot().table_count(),
            snapshot = snapshot_no,
            "local catalog: incremental refresh"
        );
        Ok(RefreshOutcome {
            full: false,
            tables_refreshed: refreshed,
            version,
            snapshot: snapshot_no,
        })
    }

    /// 原子替换快照，并回收已被新快照覆盖（或已陈旧世代）的热数据本地副本。
    fn swap(&self, next: CatalogSnapshot) {
        let snapshot_no = next.snapshot;
        *self.current.write().unwrap() = Arc::new(next);
        if let Some(s) = self.hot_shards() {
            s.reclaim(snapshot_no);
        }
    }

    /// 数据节点列表。standalone = 本节点；R4 起由成员发现提供。
    fn nodes(&self) -> Vec<String> {
        self.nodes.read().unwrap().clone()
    }

    /// 注入节点列表（装配层；standalone 传 `[ingest.instance_id]`）。
    pub fn set_nodes(&self, nodes: Vec<String>) {
        *self.nodes.write().unwrap() = nodes;
    }

    fn record_error(&self, msg: String) -> DataFusionError {
        tracing::error!(error = %msg, "local catalog refresh failed; keeping previous snapshot");
        *self.last_error.write().unwrap() = Some(msg.clone());
        DataFusionError::Execution(msg)
    }
}

/// 缓存刷新后台任务（S2-6：**带版本号请求 + 提交驱动**）。
///
/// 两个触发源，任一命中即刷新：
/// - **Catalog 版本变化**（schema/manifest 任一组前进）：这是主通道；
/// - **热分片变更计数变化**（写入把数据放进 chunk）：保证"读己之写"的近实时；
///
/// `ttl` 只作**兜底**（防止上面两条通道因 bug 静默失效）。
pub fn spawn_cache_refresh(
    cache: Arc<LocalCatalog>,
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
            tracing::error!(error = %e, "catalog refresh failed on startup");
        }
        let mut last_version = catalog.version().await;
        let mut last_shard_version = cache.shard_version();
        let mut last_refresh = std::time::Instant::now();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }
            let version = catalog.version().await;
            let shard_version = cache.shard_version();
            let changed = version != last_version || shard_version != last_shard_version;
            if !changed && last_refresh.elapsed() < ttl {
                continue;
            }
            match cache.refresh(&catalog).await {
                Ok(out) => tracing::debug!(
                    full = out.full,
                    tables = out.tables_refreshed,
                    snapshot = out.snapshot,
                    "catalog refreshed"
                ),
                Err(e) => tracing::error!(error = %e, "catalog refresh failed"),
            }
            // refresh 内部 reclaim 也会推进热分片计数，因此刷新后再取一次作为基线
            last_version = catalog.version().await;
            last_shard_version = cache.shard_version();
            last_refresh = std::time::Instant::now();
        }
    })
}
