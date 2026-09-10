//! Query 桥接：DataFusion 查询引擎（详细设计 §6.7 / §8 / ADR-6）。
//!
//! **架构铁律（C7）**：Query 严禁网络调用 —— Catalog 查询必须走本地缓存。
//! 本 crate 通过 [`cache::LocalCatalogCache`]（TTL 刷新，默认 30s，§11 [query].cache_ttl）
//! 满足此约束；缓存穿透错误即 bug。
//!
//! 表发现路径：`yuntun.public.<table>`；Manifest 驱动（C7）：TableProvider 的
//! scan 由 `list_visible_files(snapshot)` 结果构造 Parquet 文件组。

pub mod cache;
pub mod provider;
pub mod table;

pub use cache::{spawn_cache_refresh, CachedTable, LocalCatalogCache};
pub use provider::{YuntunCatalogProvider, YuntunSchemaProvider};
pub use table::YuntunTableProvider;

use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use yuntun_catalog::CatalogOps;

/// 对象存储注册用的固定 URL（scan 路径均相对此 URL）。
pub const STORE_URL: &str = "yuntun-store:///";
/// 固定 catalog / schema 名。
pub const CATALOG_NAME: &str = "yuntun";
pub const SCHEMA_NAME: &str = "public";

/// 查询引擎（详细设计 §6.7）。
pub struct QueryEngine {
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    cache: std::sync::Arc<LocalCatalogCache>,
}

impl QueryEngine {
    pub fn new(
        store: std::sync::Arc<dyn object_store::ObjectStore>,
        cache: std::sync::Arc<LocalCatalogCache>,
    ) -> Self {
        Self { store, cache }
    }

    pub fn cache(&self) -> std::sync::Arc<LocalCatalogCache> {
        self.cache.clone()
    }

    /// 构造会话：注册对象存储 + yuntun catalog（每次查询新会话，会话级状态隔离）。
    ///
    /// information_schema 显式开启：Flight SQL GetTables / SHOW TABLES / S1.7 DDL 依赖。
    pub async fn session(&self) -> Result<SessionContext, DataFusionError> {
        let ctx = SessionContext::new_with_config(
            datafusion::prelude::SessionConfig::new()
                .with_information_schema(true)
                // G2（sql-access-design §四）：非限定表名 `FROM t` 解析到默认
                // catalog/schema（yuntun.public），wire 客户端（MySQL/PG）直接可用
                .with_default_catalog_and_schema(CATALOG_NAME, SCHEMA_NAME),
        );
        let url: url::Url = STORE_URL
            .parse()
            .map_err(|e| DataFusionError::Configuration(format!("invalid store url: {e}")))?;
        ctx.register_object_store(&url, self.store.clone());
        ctx.register_catalog(
            CATALOG_NAME,
            std::sync::Arc::new(YuntunCatalogProvider::new(self.cache.clone())),
        );
        Ok(ctx)
    }

    /// 执行 SQL，返回全部批次。
    pub async fn sql(
        &self,
        query: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, DataFusionError> {
        let ctx = self.session().await?;
        let df = ctx.sql(query).await?;
        df.collect().await
    }

    /// 只取结果集 schema（逻辑计划，不执行物理计划）。
    /// Flight SQL GetFlightInfo/GetSchema 用：避免 LIMIT 0 收集零批次的歧义。
    pub async fn schema_of(
        &self,
        query: &str,
    ) -> Result<arrow::datatypes::SchemaRef, DataFusionError> {
        let ctx = self.session().await?;
        let df = ctx.sql(query).await?;
        Ok(std::sync::Arc::new(df.schema().as_arrow().clone()))
    }

    /// 启动缓存刷新任务（TTL 30s，§11）。
    pub fn spawn_refresh(
        &self,
        catalog: std::sync::Arc<dyn CatalogOps>,
        ttl: std::time::Duration,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        spawn_cache_refresh(self.cache.clone(), catalog, ttl, shutdown)
    }
}
