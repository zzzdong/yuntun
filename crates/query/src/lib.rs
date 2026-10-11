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
/// 组级剪枝（`F.4`）：把索引文件变成"跳过哪些行组"。
pub mod index;
/// `F.3`：DML 的定位扫描（在一个文件上求谓词，取回命中行号）。
pub mod locate;
pub mod partial;
pub mod agg_pushdown;
mod stmt;
pub mod prune;
pub mod provider;
pub mod table;

pub use cache::{
    spawn_cache_refresh, CachedTable, CatalogSnapshot, HotShards, LocalCatalog, LocalCatalogStats,
    RefreshOutcome,
    Member,
};
pub use partial::{MissingSource, PartialPolicy, PartialRead, PartialRejected, PartialSink};
pub use provider::{YuntunCatalogProvider, YuntunSchemaProvider};
pub use table::{HotReadStale, ReadPath, YuntunTableProvider};

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

#[derive(Clone)]
pub struct QueryEngine {
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    catalog: std::sync::Arc<LocalCatalog>,
    runtime: Option<std::sync::Arc<RuntimeEnv>>,
    /// partial 策略（`crate::partial`）：**默认 `Allow`** —— `architecture §4.2` 第三条
    /// "节点失败时返回可用结果 + 明确标记"；装配层可设为 `Reject`。
    partial_policy: PartialPolicy,
    /// 整段热读的总预算（`§88`）：默认 [`DEFAULT_HOT_READ_BUDGET`]，装配层可覆盖。
    hot_read_budget: std::time::Duration,
    /// **行组级索引剪枝**（`plan.md` F.4）：默认开。
    ///
    /// 关掉只为一件事 —— **对拍**（同一查询开/关必须逐行一致）。
    /// 不放进 TOML：它不是一个"运维旋钮"，而是那条断言的载体（`table::ReadOptions`）。
    index_pruning: bool,
    /// **聚合下推开关**（`§173`）：默认开；关掉只为**对拍**（见 [`Self::with_agg_pushdown`]）。
    agg_pushdown: bool,
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

/// 一个热来源的**聚合**拉取结果 —— 与 `scan` 的三态**逐字对应**（`§173`）。
///
/// 刻意复用同一套三态语义：`Read` = 拿到了；`Missing` = 拿不到（降级 + 点名）；
/// `Stale` = 还没拿到（刷新重试，**不得当答案**）。两套语义只会在某个边界上漂。
enum AggOutcome {
    /// `§177`：可能是**分组**结果（每组的标量），不只是"一行"
    Read(yuntun_model::agg::AggStates),
    Missing(String),
    Stale(HotReadStale),
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
            index_pruning: true,
            // `§173`：聚合下推默认开（关掉只为对拍）
            agg_pushdown: true,
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
            index_pruning: true,
            agg_pushdown: true,
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

    /// 开关**行组级索引剪枝**（`F.4`；默认开）。
    ///
    /// 唯一用途是**对拍**：同一批查询在"开"与"关"下必须**逐行一致** ——
    /// 剪错 = 静默少数据，而这条开关就是那个断言的载体（`query/tests/index_parity.rs`）。
    /// **聚合下推开关**（`§173`）：默认开。
    ///
    /// 关掉只为一件事：**对拍**（同一查询在"下推"与"原路"两条路径上必须**逐位相同**）——
    /// 聚合算错不会报错，只会给出一个偏大/偏小的数字，所以这条开关是那个断言的载体，
    /// 不是给运维调的旋钮。
    pub fn with_agg_pushdown(mut self, on: bool) -> Self {
        self.agg_pushdown = on;
        self
    }

    /// 当前聚合下推开关。
    pub fn agg_pushdown_enabled(&self) -> bool {
        self.agg_pushdown
    }

    pub fn with_index_pruning(mut self, on: bool) -> Self {
        self.index_pruning = on;
        self
    }

    /// 当前索引剪枝开关。
    pub fn index_pruning(&self) -> bool {
        self.index_pruning
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
        self.session_inner(schema, partial, true).await
    }

    /// **只读冷数据**的会话（`§173`）：聚合下推先用它把"冷的一半"算出来
    /// （schema / 列名 / 类型由 DataFusion 给，不必手写一个"看起来一样"的结果集）。
    ///
    /// 热的那一半由聚合 RPC 单独拉 —— 两边都读会把同一批热数据算两遍。
    async fn session_cold_only(
        &self,
        schema: &str,
        partial: std::sync::Arc<PartialSink>,
    ) -> Result<SessionContext, DataFusionError> {
        self.session_inner(schema, partial, false).await
    }

    async fn session_inner(
        &self,
        schema: &str,
        partial: std::sync::Arc<PartialSink>,
        hot_enabled: bool,
    ) -> Result<SessionContext, DataFusionError> {
        // **读路径的按需保鲜**（`§163`）：会话是所有查询的必经点，缓存不落后于版本才建会话。
        // 冷缓存必刷（否则"进程刚起来 / 装配层没起后台任务"时查询会报 table not found），
        // 其余情况按节流问一次版本、没变则零开销返回。失败不阻断读（见 `ensure_fresh` 的文档）。
        self.catalog.ensure_fresh().await;
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
                ReadPath {
                    store: self.store.clone(),
                    hot_read_budget: self.hot_read_budget,
                    index_pruning: self.index_pruning,
                    hot_enabled,
                },
            )),
        );
        Ok(ctx)
    }

    /// **聚合下推**（`§173`，G.2 / 归档 `S5-3`）：形状可下推时，把这次聚合查询算成
    /// "**冷的一半交给 DataFusion** + **热的一半走聚合 RPC**"，然后合并。
    ///
    /// 不适用（形状不合 / 结果形状与判定不符）⇒ 返回 `None`，调用方走原路。
    ///
    /// 三条与 `scan` **逐字一致**的规则（有两套就会漂）：STALE 必须失败并交给上层刷新重试；
    /// 拿不到的来源按 partial 策略处置；装配**严格按键序**（并发只改时间，不改结果）。
    async fn aggregate_pushdown(
        &self,
        query: &str,
        schema: &str,
    ) -> Result<Option<QueryOutcome>, DataFusionError> {
        let snapshot = self.catalog.snapshot();
        let Some(plan) = agg_pushdown::detect(query, schema, &snapshot) else {
            return Ok(None);
        };

        // ---- ① 冷的一半：DataFusion，热读关闭 ----
        //
        // 为什么不让 DataFusion 顺手把热的一半也算了：那样热数据就只有"行"这一条路能回来
        // （网络白花），而且**同一批热数据会被算两遍**（一次在冷扫描里、一次在聚合 RPC 里）。
        let partial = std::sync::Arc::new(PartialSink::new(self.partial_policy));
        let ctx = self.session_cold_only(schema, partial.clone()).await?;
        let cold_batches = ctx.sql(query).await?.collect().await?;
        // ⚠️ **先拼成一张**：DataFusion 按分区产出批次，分组时几乎必然是**多批**
        // （不分组时也常是 1 批，但不能依赖这个 —— 依赖它就会"判定通过却退回原路"，
        //  而那种失败**没有任何报错**，只是悄悄不优化）。
        let cold = match cold_batches.first() {
            Some(first) => arrow::compute::concat_batches(&first.schema(), &cold_batches)
                .map_err(|e| DataFusionError::Execution(format!("聚合下推：冷结果拼接失败：{e}")))?,
            None => {
                tracing::debug!(
                    table = %plan.table,
                    "聚合下推：冷结果为空批次，退回原路"
                );
                return Ok(None);
            }
        };
        // 形状判定是**保守**的（拿不准不下推），所以"判定通过但产物形状不对"不该发生；
        // 真发生了就**原路重跑**，而不是猜一个（正确性优先于省下的那次扫描）。
        // `§177`：分组时冷结果是"**每组一行**"（行数由数据决定），不分组时恒为 1 行
        let want_cols = plan.group_by.len() + plan.specs.len();
        if cold.num_columns() != want_cols
            || (cold.num_rows() != 1 && plan.group_by.is_empty())
        {
            tracing::debug!(
                table = %plan.table,
                grouped = !plan.group_by.is_empty(),
                rows = cold.num_rows(),
                cols = cold.num_columns(),
                "聚合下推：冷结果的形状与判定不符，退回原路"
            );
            return Ok(None);
        }

        // ---- ② 热的一半：逐来源下发（只回标量）----
        let hot = self.catalog.hot_shards();
        let budget = self.hot_read_budget;
        let deadline = tokio::time::Instant::now() + budget;
        // 读侧栅栏（`§28.1`）与 `scan` **同一份集合**：本次快照里可见文件的批次
        let exclude: Vec<String> = snapshot
            .get(&plan.table)
            .map(|t| t.files.iter().map(|f| f.batch_id.clone()).collect())
            .unwrap_or_default();
        let snap_ver = snapshot.snapshot;

        let outcomes = futures::future::join_all(hot.iter().map(|(instance, reader)| {
            let ident = plan.table.clone();
            let aq = yuntun_model::agg::AggQuery {
                specs: plan.specs.clone(),
                group_by: plan.group_by.clone(),
                predicate: plan.predicate.clone(),
            };
            let exclude = exclude.clone();
            let instance = instance.clone();
            async move {
                let outcome = match tokio::time::timeout_at(
                    deadline,
                    // `§176`/`§177`：谓词与分组键一起送过去（都在**数据所在处**生效，
                    // 网络里仍只走标量 —— 分组只是"每组的标量"，不是行）
                    reader.read_table_aggs(&ident, snap_ver, &exclude, &aq),
                )
                .await
                {
                    Ok(Ok(a)) if a.stale => AggOutcome::Stale(HotReadStale {
                        instance: instance.clone(),
                        table: ident,
                        known_manifest_ver: snap_ver,
                        flushed_watermark: a.flushed_watermark,
                    }),
                    Ok(Ok(a)) => AggOutcome::Read(a.states),
                    Ok(Err(e)) => AggOutcome::Missing(e.to_string()),
                    Err(_elapsed) => AggOutcome::Missing(format!(
                        "超出热读预算（{} ms 内未答复）",
                        budget.as_millis()
                    )),
                };
                (instance, outcome)
            }
        }))
        .await;

        let mut per_source: Vec<yuntun_model::agg::AggStates> = Vec::with_capacity(hot.len());
        for (instance, outcome) in outcomes {
            match outcome {
                AggOutcome::Read(states) => per_source.push(states),
                AggOutcome::Missing(reason) => {
                    let source = MissingSource {
                        table: plan.table.clone(),
                        instance,
                        reason,
                    };
                    tracing::warn!(
                        table = %source.table,
                        instance = %source.instance,
                        error = %source.reason,
                        policy = self.partial_policy.as_str(),
                        "hot shard aggregate unavailable; this source degrades to a PARTIAL result"
                    );
                    partial.record(source)?;
                }
                AggOutcome::Stale(e) => {
                    return Err(DataFusionError::External(Box::new(e)));
                }
            }
        }

        // ---- ③ 合并：热的部分状态并进冷结果（不分组 = 并进那一行；分组 = 按**键**并入每一组）----
        let aq = yuntun_model::agg::AggQuery {
            specs: plan.specs.clone(),
            group_by: plan.group_by.clone(),
            predicate: None, // 冷结果那一半由 DataFusion 自己算了，合并时不需要谓词
        };
        let merged = if aq.is_grouped() {
            agg_pushdown::merge_grouped_into_cold(&cold, &aq, &per_source)
        } else {
            agg_pushdown::merge_into_cold(&cold, &plan.specs, &per_source)
        }
        .map_err(|e| DataFusionError::Execution(format!("聚合下推合并失败：{e}")))?;
        Ok(Some(QueryOutcome {
            batches: vec![merged],
            partial: partial.read(),
        }))
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
        // **F.1 语句路由**（`plan.md` F 组 / `§142`）：先用 sqlparser 判定语句类型 ——
        // DDL / DML 分流到我们的处理（`F.2` 的 DDL 落在 **SQL 层** `yuntun-sql::SqlEngine`；
        // `F.3` 的 DELETE / UPDATE 尚未支持）⇒ 这里给出**可读**拒绝；查询类**原样**交给
        // DataFusion（`§132` 实测：块级剪枝是它的优化器给的，别抢）。
        match crate::stmt::classify(query) {
            Ok(kind) if kind.routed() => {
                return Err(DataFusionError::NotImplemented(kind.rejection()));
            }
            Ok(_) => {} // 查询类：走下面原路径
            // 解析失败：给**可读**的原因（不是 sqlparser 的原始错误）
            Err(e) => return Err(DataFusionError::Plan(e)),
        }
        let out = self
            .with_stale_retry("sql", || async {
                // `§173`：先试**可分解聚合的下推**（形状不合适 ⇒ `None` ⇒ 原路，一个字不变）
                if self.agg_pushdown
                    && let Some(out) = self.aggregate_pushdown(query, schema).await?
                {
                    return Ok(out);
                }
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
    ///
    /// ⚠️ **流式路径在返回时还不知道结果完不完整**：热读发生在 `scan` 里，而 `scan` 要等流被
    /// **真正消费**才跑。所以这里不声称完整与否 —— 想要这个事实，用
    /// [`Self::sql_stream_with_partial`]，在流消费完之后读那个 sink。
    ///
    /// （`§89` 之前这里有一句 `if partial.is_partial()` 的告警：它在流还没被消费时读 sink，
    /// 因而**永远不成立** —— 一句恒假的日志比没有日志更糟：它让人以为"流式路径从没缺过来源"。）
    pub async fn sql_stream_with_schema(
        &self,
        query: &str,
        schema: &str,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        Ok(self.sql_stream_with_partial(query, schema).await?.0)
    }

    /// 按 schema 流式执行，并交出记录"读不到哪些来源"的 sink（`§89`）。
    ///
    /// 与 [`Self::sql_with_partial`] 的差别只有一个：那边结果已经收集完，**此刻**就能给你
    /// 一个结论；这边的结论要等**流读完**才成形 —— 所以给的是 sink 而不是 `PartialRead`。
    pub async fn sql_stream_with_partial(
        &self,
        query: &str,
        schema: &str,
    ) -> Result<
        (SendableRecordBatchStream, std::sync::Arc<PartialSink>),
        DataFusionError,
    > {
        // 同上：`scan` 在物理计划期被调用（热读就在那里），所以 STALE 会在 `execute_stream`
        // 这一步冒出来 —— 此时流还没被消费，重试是干净的。
        self.with_stale_retry("sql_stream", || async {
            // 与 `sql_with_partial` 同理：每尝试一个 sink，免得把完整结果误标为部分
            let partial = std::sync::Arc::new(PartialSink::new(self.partial_policy));
            let ctx = self.session_with_partial(schema, partial.clone()).await?;
            let df = ctx.sql(query).await?;
            Ok((df.execute_stream().await?, partial))
        })
        .await
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
