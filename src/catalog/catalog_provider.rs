use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::common::Result;
use dashmap::DashMap;
use std::sync::Arc;
use std::any::Any;
use super::schema_provider::MemorySchemaProvider;
use super::types::DatabaseMetadata;
use fjall::{Config, Database};
use serde_json;
use tokio::sync::RwLock;

pub struct MemoryCatalogProvider {
    schemas: DashMap<String, Arc<dyn SchemaProvider>>,
    metadata: DatabaseMetadata,
    db: Arc<RwLock<Option<Database>>>,
}

impl std::fmt::Debug for MemoryCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryCatalogProvider")
            .field("schemas", &self.schemas)
            .field("metadata", &self.metadata)
            .field("db", &"Arc<RwLock<Option<Database>>>").finish()
    }
}

impl Clone for MemoryCatalogProvider {
    fn clone(&self) -> Self {
        Self {
            schemas: self.schemas.clone(),
            metadata: self.metadata.clone(),
            db: self.db.clone(),
        }
    }
}

impl CatalogProvider for MemoryCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).map(|entry| entry.value().clone())
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas
            .iter()
            .map(|schema| schema.key().clone())
            .collect()
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.insert(name.to_string(), schema))
    }

    fn deregister_schema(&self, name: &str, cascade: bool) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.remove(name).map(|(_, schema)| schema))
    }
}

impl MemoryCatalogProvider {
    pub async fn new(name: String) -> Self {
        let db = Arc::new(RwLock::new(None));
        let catalog = Self {
            schemas: DashMap::new(),
            metadata: DatabaseMetadata {
                name,
                tables: vec![],
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                updated_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            },
            db,
        };
        
        // 初始化fjall数据库
        catalog.init_db().await;
        
        // 加载数据库元数据
        catalog.load_metadata().await;
        
        catalog
    }
    
    async fn init_db(&self) {
        let config = Config::new(std::path::Path::new("./fjall_data"));
        match Database::open(config) {
            Ok(db) => {
                *self.db.write().await = Some(db);
                println!("Fjall database opened successfully");
            }
            Err(e) => {
                println!("Error opening fjall database: {}", e);
            }
        }
    }
    
    async fn load_metadata(&self) {
        if let Some(db) = &*self.db.read().await {
            // 获取或创建数据库元数据keyspace
            match db.keyspace("database_metadata", fjall::KeyspaceCreateOptions::default) {
                Ok(keyspace) => {
                    let key = format!("db:metadata:{}", self.metadata.name);
                    match keyspace.get(key.as_bytes()) {
                        Ok(Some(value)) => {
                            let value_vec = value.to_vec();
                            if let Ok(metadata) = serde_json::from_slice::<DatabaseMetadata>(&value_vec) {
                                // 加载数据库元数据
                                println!("Loaded database metadata: {}", self.metadata.name);
                            }
                        }
                        Ok(None) => {
                            // 首次初始化，保存默认元数据
                            self.save_metadata().await;
                        }
                        Err(e) => {
                            println!("Error loading database metadata: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("Error getting database_metadata keyspace: {}", e);
                }
            }
        }
    }
    
    async fn save_metadata(&self) {
        if let Some(db) = &*self.db.read().await {
            // 获取或创建数据库元数据keyspace
            match db.keyspace("database_metadata", fjall::KeyspaceCreateOptions::default) {
                Ok(keyspace) => {
                    let key = format!("db:metadata:{}", self.metadata.name);
                    let metadata_json = serde_json::to_vec(&self.metadata).unwrap();
                    match keyspace.insert(key.as_bytes(), &metadata_json) {
                        Ok(_) => {
                            println!("Saved database metadata: {}", self.metadata.name);
                        }
                        Err(e) => {
                            println!("Error saving database metadata: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("Error getting database_metadata keyspace: {}", e);
                }
            }
        }
    }

    pub fn get_metadata(&self) -> &DatabaseMetadata {
        &self.metadata
    }

    pub async fn add_default_schema(&self) {
        let default_schema = Arc::new(MemorySchemaProvider::new().await);
        
        // 将打开的数据库传递给schema_provider
        if let Some(db) = &*self.db.read().await {
            default_schema.init_with_db(db.clone()).await;
        }
        
        self.schemas.insert("public".to_string(), default_schema);
        
        // 在后台异步保存数据库元数据
        let self_clone = self.clone();
        tokio::spawn(async move {
            self_clone.save_metadata().await;
        });
    }
}
