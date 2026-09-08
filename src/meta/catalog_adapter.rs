use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::{Result, DataFusionError};
use std::sync::Arc;

use crate::meta::service::MetaService;
use crate::meta::types::TableMeta;

/// SchemaProvider adapter using MetaService
#[derive(Debug, Clone)]
pub struct MetaSchemaProvider {
    meta_service: Arc<MetaService>,
}

impl MetaSchemaProvider {
    /// Create a new MetaSchemaProvider
    pub fn new(meta_service: Arc<MetaService>) -> Self {
        Self {
            meta_service,
        }
    }

    /// Get table metadata (sync wrapper)
    pub fn get_table_metadata(&self, name: &str) -> Option<TableMeta> {
        // This is a sync method, but we need to call async method
        // We'll use a blocking approach for compatibility
        let self_clone = self.clone();
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                self_clone.meta_service.get_table(name).await.unwrap()
            })
    }
}

#[async_trait]
impl SchemaProvider for MetaSchemaProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // This is a sync method, but we need to call async method
        // We'll use a blocking approach for compatibility
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                self.meta_service.get_all_table_names().await.unwrap_or_default()
            })
    }

    async fn table(&self, _name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        // MetaSchemaProvider should only return table metadata
        // The actual TableProvider will be created by QueryService
        Err(DataFusionError::Execution(
            "MetaSchemaProvider should not directly create TableProvider. Use QueryService instead.".to_string()
        ))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        // This is a sync method, but we need to call async method
        // We'll use a blocking approach for compatibility
        let self_clone = self.clone();
        let schema = table.schema();
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let table_meta = TableMeta {
                    name: name.clone(),
                    schema,
                    chunks: vec![],
                    created_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    updated_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };
                self_clone.meta_service.add_table(table_meta).await.unwrap();
                Ok(Some(table))
            })
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        // This is a sync method, but we need to call async method
        // We'll use a blocking approach for compatibility
        let self_clone = self.clone();
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                if self_clone.meta_service.get_table(name).await.unwrap().is_some() {
                    self_clone.meta_service.delete_table(name).await.unwrap();
                    Ok(None)
                } else {
                    Ok(None)
                }
            })
    }

    fn table_exist(&self, name: &str) -> bool {
        // This is a sync method, but we need to call async method
        // We'll use a blocking approach for compatibility
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                self.meta_service.get_table(name).await.unwrap().is_some()
            })
    }
}
