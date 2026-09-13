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

/// 流式结果集类型（S1.10：`do_get` 边算边发；协议层无需直接依赖 datafusion）。
pub use datafusion::execution::SendableRecordBatchStream;

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
        self.session_with_schema(SCHEMA_NAME).await
    }

    /// 按 **schema**（MySQL 的 database 概念）构造会话：
    /// 非限定表名 `FROM t` 解析到 `${CATALOG_NAME}.${schema}`（多 schema 支持）。
    pub async fn session_with_schema(
        &self,
        schema: &str,
    ) -> Result<SessionContext, DataFusionError> {
        let mut ctx = SessionContext::new_with_config(
            datafusion::prelude::SessionConfig::new()
                .with_information_schema(true)
                // G2（sql-access-design §四）：非限定表名 `FROM t` 解析到默认
                // catalog/schema，wire 客户端（MySQL/PG）直接可用
                .with_default_catalog_and_schema(CATALOG_NAME, schema),
        );
        // JSON SQL 函数（json_extract / json_get / json_is_valid ...）：
        // JSON 列以 Utf8 文本存储，查询能力由本函数集提供（0.1 支持 ARRAY/MAP 的同时补齐）
        datafusion_functions_json::register_all(&mut ctx)?;
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

    /// 执行 SQL，返回全部批次（默认 schema = `public`）。
    pub async fn sql(
        &self,
        query: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, DataFusionError> {
        self.sql_with_schema(query, SCHEMA_NAME).await
    }

    /// 按 schema 执行 SQL（多 schema：非限定表名解析到该 schema）。
    pub async fn sql_with_schema(
        &self,
        query: &str,
        schema: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, DataFusionError> {
        let ctx = self.session_with_schema(schema).await?;
        let df = ctx.sql(query).await?;
        df.collect().await
    }

    /// 流式执行 SQL（S1.10）：返回 `RecordBatch` 流，**不 collect 全量**——
    /// Flight `do_get` 用此路径边算边发，大结果集内存平稳。
    pub async fn sql_stream(
        &self,
        query: &str,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        self.sql_stream_with_schema(query, SCHEMA_NAME).await
    }

    /// 按 schema 流式执行（多 schema：非限定表名解析到该 schema）。
    pub async fn sql_stream_with_schema(
        &self,
        query: &str,
        schema: &str,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let ctx = self.session_with_schema(schema).await?;
        let df = ctx.sql(query).await?;
        df.execute_stream().await
    }

    /// 已收集批次 → 流：非 DataFusion 产出（方言 shim 的 canned 结果、SHOW TABLES
    /// 等）在协议层统一走流式编码出口。
    pub fn stream_from_batches(
        schema: arrow::datatypes::SchemaRef,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> SendableRecordBatchStream {
        Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches.into_iter().map(Ok)),
        ))
    }

    /// 只取结果集 schema（逻辑计划，不执行物理计划）。
    /// Flight SQL GetFlightInfo/GetSchema 用：避免 LIMIT 0 收集零批次的歧义。
    pub async fn schema_of(
        &self,
        query: &str,
    ) -> Result<arrow::datatypes::SchemaRef, DataFusionError> {
        self.schema_of_with_schema(query, SCHEMA_NAME).await
    }

    /// 按 schema 取结果集 schema。
    pub async fn schema_of_with_schema(
        &self,
        query: &str,
        schema: &str,
    ) -> Result<arrow::datatypes::SchemaRef, DataFusionError> {
        let ctx = self.session_with_schema(schema).await?;
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
