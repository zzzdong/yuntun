use datafusion::catalog::TableProvider;
use datafusion::logical_expr::{Expr, TableType, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use arrow::datatypes::SchemaRef;
use std::sync::Arc;
use crate::catalog::types::TableMetadata;
use crate::store::store_manager::StoreManager;

#[derive(Debug)]
pub struct YuntunTableProvider {
    table_metadata: Arc<TableMetadata>,
    store_manager: Arc<StoreManager>,
}

impl YuntunTableProvider {
    pub fn new(table_metadata: Arc<TableMetadata>, store_manager: Arc<StoreManager>) -> Self {
        Self {
            table_metadata,
            store_manager,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for YuntunTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_metadata.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // 收集所有chunk的表提供者
        let mut table_providers = vec![];
        
        for chunk_meta in &self.table_metadata.chunks {
            if let Some(chunk) = self.store_manager.get_chunk(&chunk_meta.chunk_id) {
                table_providers.push(chunk.as_table_provider());
            }
        }
        
        if table_providers.is_empty() {
            return Err(datafusion::common::DataFusionError::Execution(format!("No chunks found for table {}", self.table_metadata.name)));
        }
        
        // 如果只有一个chunk，直接扫描
        if table_providers.len() == 1 {
            return table_providers[0].scan(state, projection, filters, limit).await;
        }
        
        // 否则，需要合并多个chunk的扫描结果
        // 这里简化处理，后续可以实现更复杂的合并逻辑
        let first_provider = table_providers[0].clone();
        first_provider.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters.iter().map(|_| TableProviderFilterPushDown::Unsupported).collect())
    }
}
