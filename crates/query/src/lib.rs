//! Query 桥接：DataFusion 查询引擎（详细设计 §6.7 / §8 / ADR-6）。
//!
//! **架构铁律（C7）**：Query 严禁网络调用 —— Catalog 查询必须走本地缓存。
//! 本 crate 通过 [`cache::LocalCatalog`]（**版本驱动 + 增量刷新**，S2-5/S2-7）满足此约束；
//! 缓存穿透错误即 bug。
//!
//! **每查询一次快照（S2-4）**：构造会话时取一次 [`cache::CatalogSnapshot`]（不可变、`Arc` 共享），
//! 规划期与 scan 全程复用 —— 同一次查询看到的 schema / 文件清单 / 可见性边界必然同一版本。
//!
//! 表发现路径：`yuntun.public.<table>`；Manifest 驱动（C7）：TableProvider 的
//! scan 由 `list_visible_files(snapshot)` 结果构造 Parquet 文件组。

pub mod cache;
pub mod partial;
pub mod provider;
pub mod table;

pub use cache::{
    spawn_cache_refresh, CachedTable, CatalogSnapshot, HotShards, LocalCatalog, LocalCatalogStats,
    RefreshOutcome,
    Member,
};
pub use partial::{MissingSource, PartialPolicy, PartialRead, PartialRejected, PartialSink};
pub use provider::{YuntunCatalogProvider, YuntunSchemaProvider};
pub use table::{HotReadStale, YuntunTableProvider};

/// 流式结果集类型（S1.10：`do_get` 边算边发；协议层无需直接依赖 datafusion）。
pub use datafusion::execution::SendableRecordBatchStream;

/// 从（可能被包了几层的）DataFusion 错误里认出 [`HotReadStale`]。
///
/// DataFusion 会用 `Context` 把计划期错误包一层（"failed to create physical plan" 之类），
/// 所以必须**沿链下钻**：只认最外层会漏掉它，STALE 就会被当成普通失败上报。
fn hot_read_stale(e: &DataFusionError) -> Option<&HotReadStale> {
    match e {
        DataFusionError::External(b) => b.downcast_ref::<HotReadStale>(),
        DataFusionError::Context(_, inner) => hot_read_stale(inner),
        _ => None,
    }
}

use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::SessionContext;
use yuntun_catalog::CatalogOps;

/// 对象存储注册用的固定 URL（scan 路径均相对此 URL）。
pub const STORE_URL: &str = "yuntun-store:///";
/// 固定 catalog / schema 名。
pub const CATALOG_NAME: &str = "yuntun";
pub const SCHEMA_NAME: &str = "public";

/// 查询引擎（详细设计 §6.7 + 架构 §2.8 内存硬分区之一）。
///
/// `runtime` 持有 **query 执行区**的内存池：与 chunk 区（读写热缓冲）是两块**独立账本**，
/// 互不借用。本块超限时 DataFusion 返回 `ResourcesExhausted`，
/// **绝不抢占 chunk 区**（否则一个大基数 `GROUP BY` 就能把写入压垮）。
/// **热读总预算的默认值**（`§88`）：一次查询在热数据上最多等多久。
///
/// 为什么默认是 10s：它要**大于**单个来源的传输超时（`§78` 默认 5s）—— 否则一个"慢但活着"
/// 的来源会被预算直接砍掉，等于把传输层的判断抢过来；又要**小于**"一个来源把它的每个 RPC
/// 都等满"的乘积（一个来源最坏数个 RPC），才能真的收住最坏情况。
pub const DEFAULT_HOT_READ_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

pub struct QueryEngine {
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    catalog: std::sync::Arc<LocalCatalog>,
    runtime: Option<std::sync::Arc<RuntimeEnv>>,
    /// partial 策略（`crate::partial`）：**默认 `Allow`** —— `architecture §4.2` 第三条
    /// "节点失败时返回可用结果 + 明确标记"；装配层可设为 `Reject`。
    partial_policy: PartialPolicy,
    /// 整段热读的总预算（`§88`）：默认 [`DEFAULT_HOT_READ_BUDGET`]，装配层可覆盖。
    hot_read_budget: std::time::Duration,
}

/// 一次查询的结果：批次 + **完整性标记**（`crate::partial`）。
///
/// 为什么把 partial 随结果交出去、而不是只写日志：设计要求"返回可用结果 +
/// `partial: true` + **缺失来源列表**"（`architecture §4.2`）—— 日志是给运维的，
/// 而"这份结果可不可信"是给**调用方**的，两者不能互相替代。
#[derive(Debug, Clone)]
pub struct QueryOutcome {
    pub batches: Vec<arrow::record_batch::RecordBatch>,
    /// 缺失来源；`is_partial()` 为 false 时是空的
    pub partial: PartialRead,
}

impl QueryOutcome {
    /// 结果是否**部分**（有来源没读到）。
    pub fn is_partial(&self) -> bool {
        self.partial.is_partial()
    }
}

impl QueryEngine {
    pub fn new(
        store: std::sync::Arc<dyn object_store::ObjectStore>,
        catalog: std::sync::Arc<LocalCatalog>,
    ) -> Self {
        Self {
            store,
            catalog,
            runtime: None,
            partial_policy: PartialPolicy::default(),
            hot_read_budget: DEFAULT_HOT_READ_BUDGET,
        }
    }

    /// 带 query 执行区内存上限的构造（架构 §2.8）。
    ///
    /// `query_mem_bytes = 0` 视为"不设上限"（单测 / 无写入同进程的场景）。
    pub fn with_query_memory_limit(
        store: std::sync::Arc<dyn object_store::ObjectStore>,
        catalog: std::sync::Arc<LocalCatalog>,
        query_mem_bytes: usize,
    ) -> Result<Self, DataFusionError> {
        if query_mem_bytes == 0 {
            return Ok(Self::new(store, catalog));
        }
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(std::sync::Arc::new(GreedyMemoryPool::new(query_mem_bytes)))
            .build()?;
        Ok(Self {
            store,
            catalog,
            runtime: Some(std::sync::Arc::new(runtime)),
            partial_policy: PartialPolicy::default(),
            hot_read_budget: DEFAULT_HOT_READ_BUDGET,
        })
    }

    /// query 执行区当前已预留字节（`None` = 未设上限）。
    pub fn query_memory_reserved(&self) -> Option<usize> {
        self.runtime
            .as_ref()
            .map(|rt| rt.memory_pool.reserved())
    }

    /// query 执行区内存上限（`None` = 未设上限）。
    pub fn query_memory_limit(&self) -> Option<usize> {
        self.runtime.as_ref().and_then(|rt| {
            match rt.memory_pool.memory_limit() {
                datafusion::execution::memory_pool::MemoryLimit::Finite(n) => Some(n),
                _ => None,
            }
        })
    }

    /// 设 partial 策略（装配层从配置来；默认 [`PartialPolicy::Allow`]）。
    pub fn with_partial_policy(mut self, policy: PartialPolicy) -> Self {
        self.partial_policy = policy;
        self
    }

    /// 当前 partial 策略。
    pub fn partial_policy(&self) -> PartialPolicy {
        self.partial_policy
    }

    /// 设**整段热读的总预算**（`§88`；装配层从配置来，默认 [`DEFAULT_HOT_READ_BUDGET`]）。
    ///
    /// 它管的是"**这次查询**愿意为热数据等多久"，与数据面 RPC 的传输超时（"一个 RPC
    /// 最多等多久"，`§78`）是两层：并发把 N 个来源的等待从相加变成取最大，预算是这个最大
    /// 之上的**硬上界**。超过它还没答的来源按"拿不到"处理 ⇒ 降级 + 点名（`Allow`）
    /// 或当场失败（`Reject`）—— 与 `§77` 同一条路。
    pub fn with_hot_read_budget(mut self, budget: std::time::Duration) -> Self {
        self.hot_read_budget = budget;
        self
    }

    /// 当前热读总预算。
    pub fn hot_read_budget(&self) -> std::time::Duration {
        self.hot_read_budget
    }

    /// 本地 Catalog（物化视图：刷新由后台任务驱动）。
    pub fn catalog(&self) -> std::sync::Arc<LocalCatalog> {
        self.catalog.clone()
    }

    /// 当前不可变 Catalog 快照（诊断 / 测试）。
    pub fn snapshot(&self) -> std::sync::Arc<CatalogSnapshot> {
        self.catalog.snapshot()
    }

    /// 构造会话：注册对象存储 + yuntun catalog（每次查询新会话，会话级状态隔离）。
    ///
    /// information_schema 显式开启：Flight SQL GetTables / SHOW TABLES / S1.7 DDL 依赖。
    pub async fn session(&self) -> Result<SessionContext, DataFusionError> {
        self.session_with_schema(SCHEMA_NAME).await
    }

    /// 按 **schema**（MySQL 的 database 概念）构造会话：
    /// 非限定表名 `FROM t` 解析到 `${CATALOG_NAME}.${schema}`（多 schema 支持）。
    ///
    /// 不带 partial 记录的版本（sink 当场丢弃）：给只关心"能不能执行"的调用方用；
    /// 想知道**结果完不完整**请用 [`Self::sql_with_partial`] / [`Self::session_with_partial`]。
    pub async fn session_with_schema(
        &self,
        schema: &str,
    ) -> Result<SessionContext, DataFusionError> {
        self.session_with_partial(
            schema,
            std::sync::Arc::new(PartialSink::new(self.partial_policy)),
        )
        .await
    }

    /// 按 schema 构造会话，并把"读不到的来源"记进 `partial`（**每查询一个** sink）。
    pub async fn session_with_partial(
        &self,
        schema: &str,
        partial: std::sync::Arc<PartialSink>,
    ) -> Result<SessionContext, DataFusionError> {
        let config = datafusion::prelude::SessionConfig::new()
            .with_information_schema(true)
            // G2（sql-access-design §四）：非限定表名 `FROM t` 解析到默认
            // catalog/schema，wire 客户端（MySQL/PG）直接可用
            .with_default_catalog_and_schema(CATALOG_NAME, schema);
        // 硬分区：会话级内存池 = query 执行区（与 chunk 区互不抢占，架构 §2.8）
        let mut ctx = match &self.runtime {
            Some(rt) => SessionContext::new_with_config_rt(config, rt.clone()),
            None => SessionContext::new_with_config(config),
        };
        // JSON SQL 函数（json_extract / json_get / json_is_valid ...）：
        // JSON 列以 Utf8 文本存储，查询能力由本函数集提供（0.1 支持 ARRAY/MAP 的同时补齐）
        datafusion_functions_json::register_all(&mut ctx)?;
        let url: url::Url = STORE_URL
            .parse()
            .map_err(|e| DataFusionError::Configuration(format!("invalid store url: {e}")))?;
        ctx.register_object_store(&url, self.store.clone());
        // 【S2-4】每查询取一次不可变快照：规划与 scan 全程同版本
        let snapshot = self.catalog.snapshot();
        let hot = self.catalog.hot_shards();
        ctx.register_catalog(
            CATALOG_NAME,
            std::sync::Arc::new(YuntunCatalogProvider::new(
                snapshot,
                hot,
                partial,
                self.hot_read_budget,
            )),
        );
        Ok(ctx)
    }

    /// 热读 STALE 的**重试上限**（`operation-log §61.4` 第 2 条）。
    ///
    /// 为什么有界：STALE 意味着"我们的 manifest 落后"，刷新一次就该追上；连着追不上说明
    /// 水位在持续推进（写入比刷新快）或刷新没生效 —— 两种情况都该**显式失败**，
    /// 而不是无限重试或返回不完整结果。
    const HOT_STALE_MAX_ATTEMPTS: usize = 3;

    /// STALE 重试：热读报 [`HotReadStale`] ⇒ 刷新 manifest ⇒ 用新版本**重试**。
    ///
    /// 三条约束（都来自 `§61.4` 第 2 条）：
    /// - **必须重试**（不得把不完整结果当答案）；
    /// - **有界**（耗尽即报错，绝不静默降级）；
    /// - 刷新失败**直接冒泡**（刷不动就别硬撑 —— 宁可失败也别给错数据）。
    async fn with_stale_retry<T, F, Fut>(&self, what: &str, attempt: F) -> Result<T, DataFusionError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, DataFusionError>>,
    {
        let mut n = 0usize;
        loop {
            n += 1;
            match attempt().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    let Some(stale) = hot_read_stale(&e) else {
                        return Err(e);
                    };
                    if n >= Self::HOT_STALE_MAX_ATTEMPTS {
                        return Err(DataFusionError::Execution(format!(
                            "{what}: 热读连续 {n} 次报 STALE（实例 {} 水位 {}）—— 刷新 manifest 也没追上；不返回不完整结果",
                            stale.instance, stale.flushed_watermark
                        )));
                    }
                    tracing::warn!(
                        attempt = n,
                        table = %stale.table,
                        known_manifest_ver = stale.known_manifest_ver,
                        flushed_watermark = stale.flushed_watermark,
                        "hot read reported STALE; refreshing manifest, then retrying"
                    );
                    self.catalog.refresh_now().await?;
                }
            }
        }
    }

    /// 执行 SQL，返回全部批次（默认 schema = `public`）。
    pub async fn sql(
        &self,
        query: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, DataFusionError> {
        self.sql_with_schema(query, SCHEMA_NAME).await
    }

    /// 按 schema 执行 SQL（多 schema：非限定表名解析到该 schema）；只返回批次。
    ///
    /// 降级（partial）发生在**内部**：策略 `Allow` 时返回可用部分并打一条 warn 日志；
    /// 策略 `Reject` 时直接报错。**想要"结果完不完整"这个事实，请用
    /// [`Self::sql_with_partial`]** —— 完整性不能只留在日志里。
    pub async fn sql_with_schema(
        &self,
        query: &str,
        schema: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, DataFusionError> {
        Ok(self.sql_with_partial(query, schema).await?.batches)
    }

    /// 执行 SQL 并**带回完整性标记**（默认 schema = `public`）。
    pub async fn sql_partial(&self, query: &str) -> Result<QueryOutcome, DataFusionError> {
        self.sql_with_partial(query, SCHEMA_NAME).await
    }

    /// 执行 SQL 并**带回完整性标记**：批次 + 缺失来源（`crate::partial`）。
    ///
    /// 三个刻意的做法：
    ///
    /// 1. **每次尝试都重建会话 *与* sink**：provider 持有当次快照（刷新后必须换新的）；
    ///    而 sink 是"**这一次尝试**"的事实 —— 上一次尝试缺的来源，若这次读到了就不该继续算缺，
    ///    否则会把**完整结果误标为部分**（假警报比漏报好，但仍然是错的）；
    /// 2. **降级不重试**：`§4.3` 说失败/超时 ⇒ 退化为只读冷数据 + 标记 partial，没有"再试一次"；
    ///    `Reject` 策略下的拒绝同样不重试（对一个不可达的节点重试没有意义）；
    /// 3. **STALE 仍然重试、仍然响亮失败**：那条路走 [`HotReadStale`]，与 partial 无关。
    pub async fn sql_with_partial(
        &self,
        query: &str,
        schema: &str,
    ) -> Result<QueryOutcome, DataFusionError> {
        let out = self
            .with_stale_retry("sql", || async {
                let partial = std::sync::Arc::new(PartialSink::new(self.partial_policy));
                let ctx = self.session_with_partial(schema, partial.clone()).await?;
                let df = ctx.sql(query).await?;
                let batches = df.collect().await?;
                Ok(QueryOutcome {
                    batches,
                    partial: partial.read(),
                })
            })
            .await?;
        if out.is_partial() {
            tracing::warn!(detail = %out.partial.describe(), "query returned a PARTIAL result");
        }
        Ok(out)
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
        // 同上：`scan` 在物理计划期被调用（热读就在那里），所以 STALE 会在 `execute_stream`
        // 这一步冒出来 —— 此时流还没被消费，重试是干净的。
        let (stream, partial) = self
            .with_stale_retry("sql_stream", || async {
                // 与 `sql_with_partial` 同理：每尝试一个 sink，免得把完整结果误标为部分
                let partial = std::sync::Arc::new(PartialSink::new(self.partial_policy));
                let ctx = self.session_with_partial(schema, partial.clone()).await?;
                let df = ctx.sql(query).await?;
                Ok((df.execute_stream().await?, partial.read()))
            })
            .await?;
        if partial.is_partial() {
            tracing::warn!(detail = %partial.describe(), "sql_stream returned a PARTIAL result");
        }
        Ok(stream)
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
        spawn_cache_refresh(self.catalog.clone(), catalog, ttl, shutdown)
    }
}
