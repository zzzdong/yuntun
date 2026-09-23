//! Manifest 驱动的 TableProvider（C7 / 详细设计 §8）+ **热数据**（读己之写）。
//!
//! scan 把同一 shard 的两种形态合并成一个执行计划（见 `yuntun_store::shard`）：
//! ① **热数据**：已 fsync、尚未落盘的 chunk，经 store 层的 [`yuntun_store::ShardReader`] 读取
//!    （阶段 0 = 进程内 chunk store；分离部署 = `RemoteShard`）—— 写后立即可查；
//! ② **磁盘分片**：已落对象存储的 Parquet 文件组（Manifest 驱动，快照隔离）。
//!
//! 交接无空洞/无重复：chunk 在 `commit_files` 成功且**查询缓存追上该快照**之前一直可见
//! （架构 §4.5 / I4）。
//!
//! **快照一致性（S2-4）**：provider 持有的是 `Arc<CatalogSnapshot>`（规划期取一次），
//! 因此"可见文件清单"与"schema"必然同版本 —— 不会出现 schema 是新的、文件是旧的。
//!
//! - 列裁剪 / 谓词下推交给 DataFusion（Parquet 统计 + ParquetSource）；
//! - 【硬编码】file_path 全部以 `yuntun-store:///` 为根；
//! - 多 schema_version 共存：批次与文件都对齐到表当前 schema
//!   （同名列宽化 + 缺失列 null 填充，§6.8 / `arrow_util::align_batch`）。

/// **热读报 STALE**：本实例已放弃 (调用方的 manifest 版本, 它的水位] 之间数据的本地副本，
/// 而调用方的 manifest 里还没有那些文件（`architecture-with-chunk §4.5` 的"两头都没有"）。
///
/// **这是"可重试"信号，不是失败**：`QueryEngine` 认出它 → 刷新 manifest → 用新版本重试
/// （`operation-log §61.4` 第 2 条）。单独定义类型而不是靠字符串：字符串匹配认不出来，
/// 而这种错误一旦被当作普通失败上报，用户看到的就是莫名失败。
#[derive(Debug)]
pub struct HotReadStale {
    /// **哪个实例**的水位超前了（诊断的关键：多实例时"谁没追上"决定先查谁）
    pub instance: String,
    /// 触发 STALE 的表（全限定名）
    pub table: String,
    /// 调用方（查询）所依据的 manifest 版本
    pub known_manifest_ver: u64,
    /// 热数据所属实例**已放弃本地副本**的最高版本
    pub flushed_watermark: u64,
}

impl std::fmt::Display for HotReadStale {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "hot shard read is stale for {} at instance {}: 查询依据的 manifest 版本 {} 落后于该实例水位 {} \
             （该实例已放弃这部分数据的本地副本；需刷新 manifest 后用新版本重试）",
            self.table, self.instance, self.known_manifest_ver, self.flushed_watermark
        )
    }
}

impl std::error::Error for HotReadStale {}

use crate::cache::CatalogSnapshot;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource_parquet::source::ParquetSource;
use std::sync::Arc;
use crate::cache::HotShards;
use crate::partial::{MissingSource, PartialSink};

/// 一张 yuntun 表的 DataFusion 视图（**绑定在某个不可变 Catalog 快照上**）。
#[derive(Debug)]
pub struct YuntunTableProvider {
    /// 规划期取一次的 Catalog 快照：schema / 文件清单 / 可见性边界都取自它
    snapshot: Arc<CatalogSnapshot>,
    /// 全限定表标识 `schema.table`
    ident: String,
    schema: arrow::datatypes::SchemaRef,
    store_url: ObjectStoreUrl,
    /// 热数据读侧（store 层 [`yuntun_store::ShardReader`]），**按实例**：scan 时逐实例读 chunk。
    /// 空 map = 未接线（单测），退化为纯磁盘分片（Manifest）可见性。
    hot: HotShards,
    /// 读不到的来源往这里记（**每查询一个**）：`Allow` ⇒ 降级为部分结果并标记，
    /// `Reject` ⇒ 当场失败点名（`crate::partial`）。
    partial: Arc<PartialSink>,
}

impl YuntunTableProvider {
    pub fn new(
        snapshot: Arc<CatalogSnapshot>,
        ident: impl Into<String>,
        schema: arrow::datatypes::SchemaRef,
        hot: HotShards,
        partial: Arc<PartialSink>,
    ) -> Self {
        Self {
            snapshot,
            ident: ident.into(),
            schema,
            store_url: ObjectStoreUrl::parse(crate::STORE_URL)
                .unwrap_or_else(|_| ObjectStoreUrl::local_filesystem()),
            hot,
            partial,
        }
    }
}

#[async_trait]
impl datafusion::catalog::TableProvider for YuntunTableProvider {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    /// scan：热数据 ∪ 已提交文件组（见模块注释）。
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let schema = self.schema.clone();
        let mut inputs: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

        // 快照里的表条目 —— 不可变，规划期读多次结果一致
        let table = self.snapshot.get(&self.ident);

        // ① 热数据（读己之写）：写后即可查，不等 flush / 缓存 TTL。
        //
        // **按实例逐个拉**（`architecture §4.4`：冷热边界按实例二维切分）：每个实例有**自己的**
        // "已放弃本地副本的水位"，都用查询的快照版本去问，各自回答自己是否 STALE。
        // 键有序（`BTreeMap`）⇒ 批次拼接顺序确定 ⇒ 结果可复现。
        for (instance, reader) in &self.hot {
            // 【T13.4】热读**失败**（连接拒绝 / 超时 / 内部错误）= "**拿不到**"：
            // 按 `architecture §4.2/§4.3` 降级为**部分结果**并把来源记下来，
            // `Reject` 策略下 `record` 会当场返回错误（点名缺了谁）。
            //
            // ⚠️ 这**不是** STALE 那条路，两者绝不可混同：STALE 是成员**答上来了**、
            // 且答的是"我的水位超前于你的 manifest"（走下面 `read.stale`）—— 那是
            // "**还没拿到**"（刷新 manifest 就拿到了），必须重试，**降级它等于静默少数据**。
            let read = match reader.read_table(&self.ident, self.snapshot.snapshot).await {
                Ok(r) => r,
                Err(e) => {
                    let source = MissingSource {
                        table: self.ident.to_string(),
                        instance: instance.clone(),
                        reason: e.to_string(),
                    };
                    tracing::warn!(
                        table = %source.table,
                        instance = %source.instance,
                        error = %source.reason,
                        policy = self.partial.policy().as_str(),
                        "hot shard read failed; this source degrades to a PARTIAL result"
                    );
                    self.partial.record(source)?;
                    continue;
                }
            };
            // `stale` ⇒ 本实例已放弃 (本次快照, 水位] 之间数据的本地副本，而本次快照的 manifest
            // 里还没有那些文件（`architecture-with-chunk §4.5` 的"两头都没有"窗口）。
            //
            // **必须重试，绝不能当答案**（`operation-log §61.4` 第 2 条）：这里报一个**可识别**
            // 的错误，由 `QueryEngine` 刷新 manifest 后用新版本重试（`§64`）。
            // 用**类型**而不是字符串传达"该重试"：字符串匹配认不出来，而这种错误一旦被当成
            // 普通失败上报，用户就会看到莫名失败。
            if read.stale {
                return Err(datafusion::error::DataFusionError::External(Box::new(
                    HotReadStale {
                        instance: instance.clone(),
                        table: self.ident.to_string(),
                        known_manifest_ver: self.snapshot.snapshot,
                        flushed_watermark: read.flushed_watermark,
                    },
                )));
            }
            let raw = read.batches;
            if raw.is_empty() {
                continue;
            }
            let mut batches = Vec::with_capacity(raw.len());
            for b in raw {
                batches.push(
                    yuntun_model::arrow_util::align_batch(&b, &schema).map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!("pending align: {e}"))
                    })?,
                );
            }
            let exec =
                MemorySourceConfig::try_new_exec(&[batches], schema.clone(), projection.cloned())
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!("pending plan: {e}"))
                    })?;
            inputs.push(exec);
        }

        // ② 已提交文件（Manifest 驱动，C7）：文件清单来自**本 provider 的快照**
        if let Some(t) = table
            && !t.files.is_empty()
        {
            let files: Vec<PartitionedFile> = t
                .files
                .iter()
                .map(|f| PartitionedFile::new(f.file_path.clone(), f.file_size))
                .collect();

            let source: Arc<dyn FileSource> = Arc::new(ParquetSource::new(
                datafusion_datasource::table_schema::TableSchemaBuilder::new(schema.clone())
                    .build(),
            ));

            let mut builder = FileScanConfigBuilder::new(self.store_url.clone(), source)
                .with_file_group(FileGroup::new(files));
            if let Some(p) = projection {
                builder = builder
                    .with_projection_indices(Some(p.clone()))
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!("projection: {e}"))
                    })?;
            }
            inputs.push(DataSourceExec::from_data_source(builder.build()));
        }

        // ③ 空表：给一个空的内存执行（保持 schema / 投影语义）
        if inputs.is_empty() {
            let exec =
                MemorySourceConfig::try_new_exec(&[Vec::new()], schema.clone(), projection.cloned())
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!("empty plan: {e}"))
                    })?;
            inputs.push(exec);
        }

        let plan: Arc<dyn ExecutionPlan> = if inputs.len() == 1 {
            inputs.pop().unwrap()
        } else {
            UnionExec::try_new(inputs)?
        };
        // limit 只在合并后生效（对每个输入单独 limit 会放大总数）
        if let Some(l) = limit {
            return Ok(Arc::new(GlobalLimitExec::new(plan, 0, Some(l))));
        }
        Ok(plan)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        // MVP：谓词交给 Parquet row-group 统计在 scan 内部处理（ParquetSource 默认行为），
        // 表级先声明 Inexact —— 正确且保守。
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    /// 缓存中的文件行数（无真实统计时不承诺精确值）。
    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        None
    }
}
