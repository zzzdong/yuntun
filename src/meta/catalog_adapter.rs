use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::{Result, DataFusionError};
use std::sync::Arc;

use crate::meta::service::MetaService;
use crate::meta::types::TableMeta;
use crate::query::datafusion_integration::YuntunTableProvider;
use crate::store::store_manager::StoreManager;

/// SchemaProvider adapter using MetaService
#[derive(Debug, Clone)]
pub struct MetaSchemaProvider {
    meta_service: Arc<MetaService>,
    store_manager: Option<Arc<StoreManager>>,
}

impl MetaSchemaProvider {
    /// Create a new MetaSchemaProvider
    pub fn new(meta_service: Arc<MetaService>) -> Self {
        Self {
            meta_service,
            store_manager: None,
        }
    }

    /// Set store manager
    pub fn set_store_manager(&mut self, store_manager: Arc<StoreManager>) {
        self.store_manager = Some(store_manager);
    }
    
    /// Convert TableMeta to TableMetadata for compatibility
    fn convert_to_table_metadata(&self, table_meta: TableMeta) -> crate::catalog::types::TableMetadata {
        let chunks = table_meta.chunks.into_iter().map(|chunk| {
            crate::catalog::types::ChunkMetadata {
                chunk_id: chunk.chunk_id,
                chunk_type: crate::catalog::types::ChunkType::Memory,
                row_count: chunk.row_count,
                size_in_bytes: chunk.size_in_bytes,
                created_at: chunk.created_at,
                partition_info: None,
                index_info: None,
            }
        }).collect();
        
        crate::catalog::types::TableMetadata {
            name: table_meta.name,
            schema: table_meta.schema,
            chunks,
            created_at: table_meta.created_at,
            updated_at: table_meta.updated_at,
        }
    }
    
    /// Get table metadata
    pub fn get_table_metadata(&self, name: &str) -> Option<crate::catalog::types::TableMetadata> {
        // This is a sync method, but we need to call async method
        // We'll use a blocking approach for compatibility
        let self_clone = self.clone();
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                if let Some(table_meta) = self_clone.meta_service.get_table(name).await.unwrap() {
                    Some(self_clone.convert_to_table_metadata(table_meta))
                } else {
                    None
                }
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

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if let Some(table_meta) = self.meta_service.get_table(name).await.unwrap() {
            if let Some(store_manager) = &self.store_manager {
                let table_metadata = self.convert_to_table_metadata(table_meta);
                let table_provider = Arc::new(YuntunTableProvider::new(Arc::new(table_metadata), store_manager.clone()));
                Ok(Some(table_provider as Arc<dyn TableProvider>))
            } else {
                Err(DataFusionError::Execution("Store manager not set for schema provider".to_string()))
            }
        } else {
            Ok(None)
        }
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
                if let Some(table_meta) = self_clone.meta_service.get_table(name).await.unwrap() {
                    self_clone.meta_service.delete_table(name).await.unwrap();
                    if let Some(store_manager) = &self_clone.store_manager {
                        let table_metadata = self_clone.convert_to_table_metadata(table_meta);
                        let table_provider = Arc::new(YuntunTableProvider::new(Arc::new(table_metadata), store_manager.clone()));
                        Ok(Some(table_provider as Arc<dyn TableProvider>))
                    } else {
                        Err(DataFusionError::Execution("Store manager not set for schema provider".to_string()))
                    }
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
