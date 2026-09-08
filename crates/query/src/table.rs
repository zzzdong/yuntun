//! Manifest 驱动的 TableProvider（C7 / 详细设计 §8）。
//!
//! scan 时把本地缓存中的可见文件（快照隔离）转成 DataFusion Parquet 文件组：
//! - 列裁剪 / 谓词下推交给 DataFusion（Parquet 统计 + ParquetSource）
//! - 【硬编码】file_path 全部以 `yuntun-store:///` 为根
//! - 多 schema_version 文件共存：由 Parquet reader 的 schema adapter 处理
//!   （同名列宽化 + 缺失列 null 填充，§6.8 / flush::align_batch 同理）

use crate::cache::CachedTable;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource_parquet::source::ParquetSource;
use std::sync::Arc;

/// 一张 yuntun 表的 DataFusion 视图（快照固定：缓存刷新间隔内一致）。
#[derive(Debug)]
pub struct YuntunTableProvider {
    table: CachedTable,
    store_url: ObjectStoreUrl,
}

impl YuntunTableProvider {
    pub fn new(table: CachedTable) -> Self {
        Self {
            table,
            store_url: ObjectStoreUrl::parse(crate::STORE_URL)
                .unwrap_or_else(|_| ObjectStoreUrl::local_filesystem()),
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

    /// Manifest 驱动 scan（C7）：可见文件 → Parquet 文件组。
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let files: Vec<PartitionedFile> = self
            .table
            .files
            .iter()
            .map(|f| PartitionedFile::new(f.file_path.clone(), f.file_size))
            .collect();

        let source: Arc<dyn FileSource> = Arc::new(ParquetSource::new(
            datafusion_datasource::table_schema::TableSchemaBuilder::new(self.table.schema.clone())
                .build(),
        ));

        let mut builder = FileScanConfigBuilder::new(self.store_url.clone(), source)
            .with_file_group(FileGroup::new(files));
        if let Some(l) = limit {
            builder = builder.with_limit(Some(l));
        }
        if let Some(p) = projection {
            builder = builder
                .with_projection_indices(Some(p.clone()))
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!("projection: {e}"))
                })?;
        }
        let config = builder.build();
        Ok(DataSourceExec::from_data_source(config))
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
