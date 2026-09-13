//! Manifest 驱动的 TableProvider（C7 / 详细设计 §8）+ **内存分片**（读己之写）。
//!
//! scan 把同一 shard 的两种形态合并成一个执行计划（见 `yuntun_store::shard`）：
//! ① **内存分片**：已 fsync、尚未落盘的"热"数据，经 store 层的 [`ShardReader`] 读取
//!    （阶段 0 = 进程内；分离部署 = `RemoteShard`）—— 写后立即可查，
//!    不再受 flush jitter（≤60s）与查询缓存 TTL（30s）影响；
//! ② **磁盘分片**：已落对象存储的 Parquet 文件组（Manifest 驱动，快照隔离）。
//!
//! 交接无空洞/无重复：内存分片条目在提交成功后转为 `Committed(snapshot)`，查询侧在
//! `cached_snapshot < snapshot` 时仍读内存分片、之后交给磁盘分片。
//!
//! - 列裁剪 / 谓词下推交给 DataFusion（Parquet 统计 + ParquetSource）；
//! - 【硬编码】file_path 全部以 `yuntun-store:///` 为根；
//! - 多 schema_version 共存：内存批次与文件都对齐到表当前 schema
//!   （同名列宽化 + 缺失列 null 填充，§6.8 / `arrow_util::align_batch`）。

use crate::cache::CachedTable;
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

/// 一张 yuntun 表的 DataFusion 视图（快照固定：缓存刷新间隔内一致）。
#[derive(Debug)]
pub struct YuntunTableProvider {
    table: CachedTable,
    store_url: ObjectStoreUrl,
    /// 热数据读侧（store 层 [`ShardReader`]）：scan 时读内存分片。
    /// `None` = 未接线（单测），退化为纯磁盘分片（Manifest）可见性。
    hot: Option<Arc<dyn ShardReader>>,
}

impl YuntunTableProvider {
    pub fn new(table: CachedTable, hot: Option<Arc<dyn ShardReader>>) -> Self {
        Self {
            table,
            store_url: ObjectStoreUrl::parse(crate::STORE_URL)
                .unwrap_or_else(|_| ObjectStoreUrl::local_filesystem()),
            hot,
        }
    }
}

#[async_trait]
impl datafusion::catalog::TableProvider for YuntunTableProvider {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.table.schema.clone()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    /// scan：内存未落盘数据 ∪ 已提交 Parquet 文件组（见模块注释）。
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let schema = self.table.schema.clone();
        let mut inputs: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

        // ① 内存分片（读己之写）：写后即可查，不等 flush jitter / 缓存 TTL
        if let Some(hot) = &self.hot {
            let ident = self.table.meta.qualified_name();
            let raw = hot
                .read_table(&ident, self.table.snapshot)
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

        // ② 已提交文件（Manifest 驱动，C7）
        if !self.table.files.is_empty() {
            let files: Vec<PartitionedFile> = self
                .table
                .files
                .iter()
                .map(|f| PartitionedFile::new(f.file_path.clone(), f.file_size))
                .collect();

            let source: Arc<dyn FileSource> = Arc::new(ParquetSource::new(
                datafusion_datasource::table_schema::TableSchemaBuilder::new(schema.clone()).build(),
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
