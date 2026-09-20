//! Manifest 驱动的 TableProvider（C7 / 详细设计 §8）+ **热数据**（读己之写）。
//!
//! scan 把同一 shard 的两种形态合并成一个执行计划（见 `yuntun_store::shard`）：
//! ① **热数据**：已 fsync、尚未落盘的 chunk，经 store 层的 [`ShardReader`] 读取
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
use yuntun_store::ShardReader;

/// 一张 yuntun 表的 DataFusion 视图（**绑定在某个不可变 Catalog 快照上**）。
#[derive(Debug)]
pub struct YuntunTableProvider {
    /// 规划期取一次的 Catalog 快照：schema / 文件清单 / 可见性边界都取自它
    snapshot: Arc<CatalogSnapshot>,
    /// 全限定表标识 `schema.table`
    ident: String,
    schema: arrow::datatypes::SchemaRef,
    store_url: ObjectStoreUrl,
    /// 热数据读侧（store 层 [`ShardReader`]）：scan 时读 chunk。
    /// `None` = 未接线（单测），退化为纯磁盘分片（Manifest）可见性。
    hot: Option<Arc<dyn ShardReader>>,
}

impl YuntunTableProvider {
    pub fn new(
        snapshot: Arc<CatalogSnapshot>,
        ident: impl Into<String>,
        schema: arrow::datatypes::SchemaRef,
        hot: Option<Arc<dyn ShardReader>>,
    ) -> Self {
        Self {
            snapshot,
            ident: ident.into(),
            schema,
            store_url: ObjectStoreUrl::parse(crate::STORE_URL)
                .unwrap_or_else(|_| ObjectStoreUrl::local_filesystem()),
            hot,
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

        // ① 热数据（读己之写）：写后即可查，不等 flush / 缓存 TTL
        if let Some(hot) = &self.hot {
            let raw = hot
                .read_table(&self.ident, self.snapshot.snapshot)
                .await
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!("hot shard read: {e}"))
                })?;
            if !raw.is_empty() {
                let mut batches = Vec::with_capacity(raw.len());
                for b in raw {
                    batches.push(
                        yuntun_model::arrow_util::align_batch(&b, &schema).map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "pending align: {e}"
                            ))
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
