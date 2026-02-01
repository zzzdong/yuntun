use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::{Result, exec_err};
use dashmap::DashMap;
use std::sync::Arc;
use std::any::Any;
use super::types::TableMetadata;
use crate::query::datafusion_integration::YuntunTableProvider;
use crate::store::store_manager::StoreManager;
use fjall::{Config, Database};
use serde_json;
use tokio::sync::RwLock;

pub struct MemorySchemaProvider {
    tables: DashMap<String, Arc<TableMetadata>>,
    store_manager: Option<Arc<StoreManager>>,
    db: Arc<RwLock<Option<Database>>>,
}

impl std::fmt::Debug for MemorySchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemorySchemaProvider")
            .field("tables", &self.tables)
            .field("store_manager", &self.store_manager)
            .field("db", &"Arc<RwLock<Option<Database>>>").finish()
    }
}

impl Clone for MemorySchemaProvider {
    fn clone(&self) -> Self {
        Self {
            tables: self.tables.clone(),
            store_manager: self.store_manager.clone(),
            db: self.db.clone(),
        }
    }
}

#[async_trait]
impl SchemaProvider for MemorySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|table| table.key().clone())
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if let Some(table_metadata) = self.get_table_metadata(name) {
            if let Some(store_manager) = &self.store_manager {
                let table_provider = Arc::new(YuntunTableProvider::new(table_metadata, store_manager.clone()));
                Ok(Some(table_provider))
            } else {
                Err(datafusion::common::DataFusionError::Execution("Store manager not set for schema provider".to_string()))
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
        // 创建TableMetadata并存储
        let schema = table.schema();
        let table_metadata = Arc::new(TableMetadata {
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
        });
        
        // 存储表元数据（同步版本）
        self.tables.insert(table_metadata.name.clone(), table_metadata.clone());
        
        // 保存到fjall数据库
        let self_clone = self.clone();
        let table_metadata_clone = table_metadata;
        tokio::spawn(async move {
            self_clone.save_table(&table_metadata_clone).await;
        });
        
        Ok(Some(table))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        // 从存储中移除表
        if let Some((_, table_metadata)) = self.tables.remove(name) {
            // 从fjall数据库中删除表元数据
            let self_clone = self.clone();
            let name_clone = name.to_string();
            tokio::spawn(async move {
                self_clone.delete_table(&name_clone).await;
            });
            
            // 创建一个临时的TableProvider用于返回
            if let Some(store_manager) = &self.store_manager {
                let table_provider = Arc::new(YuntunTableProvider::new(table_metadata, store_manager.clone()));
                Ok(Some(table_provider))
            } else {
                Err(datafusion::common::DataFusionError::Execution("Store manager not set for schema provider".to_string()))
            }
        } else {
            Ok(None)
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

impl MemorySchemaProvider {
    pub async fn new() -> Self {
        let db = Arc::new(RwLock::new(None));
        let schema_provider = Self {
            tables: DashMap::new(),
            store_manager: None,
            db,
        };
        
        // 初始化fjall数据库
        schema_provider.init_db().await;
        
        // 加载表元数据
        schema_provider.load_tables();
        
        schema_provider
    }

    pub async fn new_with_store_manager(store_manager: Arc<StoreManager>) -> Self {
        let db = Arc::new(RwLock::new(None));
        let schema_provider = Self {
            tables: DashMap::new(),
            store_manager: Some(store_manager),
            db,
        };
        
        // 初始化fjall数据库
        schema_provider.init_db().await;
        
        // 加载表元数据
        schema_provider.load_tables();
        
        schema_provider
    }
    
    pub async fn init_with_db(&self, db: Database) {
        *self.db.write().await = Some(db);
        println!("Schema provider initialized with fjall database");
        // 加载表元数据
        self.load_tables().await;
    }
    
    async fn init_db(&self) {
        // 暂时不初始化fjall数据库，避免锁定冲突
        // 我们将使用catalog_provider中已经打开的数据库
        println!("Schema provider initialized without fjall database");
    }
    
    async fn load_tables(&self) {
        if let Some(db) = &*self.db.read().await {
            // 获取或创建表元数据keyspace
            match db.keyspace("table_metadata", fjall::KeyspaceCreateOptions::default) {
                Ok(keyspace) => {
                    // 使用prefix方法扫描所有表元数据键
                    let iter = keyspace.prefix("table:metadata:");
                    for guard in iter {
                        if let Ok(value) = guard.value() {
                            if let Ok(table_serde) = serde_json::from_slice(&value) {
                                if let Ok(table_metadata) = TableMetadata::from_serde(table_serde) {
                                    let table_name = table_metadata.name.clone();
                                    self.tables.insert(table_name.clone(), Arc::new(table_metadata));
                                    println!("Loaded table: {}", table_name);
                                }
                            }
                        }
                    }
                    println!("Loaded {} tables from fjall database", self.tables.len());
                }
                Err(e) => {
                    println!("Error getting table_metadata keyspace: {}", e);
                }
            }
        }
    }
    
    async fn save_table(&self, table_metadata: &TableMetadata) {
        if let Some(db) = &*self.db.read().await {
            // 获取或创建表元数据keyspace
            match db.keyspace("table_metadata", fjall::KeyspaceCreateOptions::default) {
                Ok(keyspace) => {
                    let key = format!("table:metadata:{}", table_metadata.name);
                    let table_serde = table_metadata.to_serde();
                    let table_json = serde_json::to_vec(&table_serde).unwrap();
                    match keyspace.insert(key.as_bytes(), table_json) {
                        Ok(_) => {
                            println!("Saved table metadata: {}", table_metadata.name);
                        }
                        Err(e) => {
                            println!("Error saving table metadata: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("Error getting table_metadata keyspace: {}", e);
                }
            }
        }
    }
    
    async fn delete_table(&self, table_name: &str) {
        if let Some(db) = &*self.db.read().await {
            // 获取或创建表元数据keyspace
            match db.keyspace("table_metadata", fjall::KeyspaceCreateOptions::default) {
                Ok(keyspace) => {
                    let key = format!("table:metadata:{}", table_name);
                    match keyspace.remove(key.as_bytes()) {
                        Ok(_) => {
                            println!("Deleted table metadata: {}", table_name);
                        }
                        Err(e) => {
                            println!("Error deleting table metadata: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("Error getting table_metadata keyspace: {}", e);
                }
            }
        }
    }

    pub fn set_store_manager(&mut self, store_manager: Arc<StoreManager>) {
        self.store_manager = Some(store_manager);
    }

    pub async fn add_table(&self, table_metadata: Arc<TableMetadata>) {
        self.tables.insert(table_metadata.name.clone(), table_metadata.clone());
        
        // 保存表元数据到fjall数据库
        self.save_table(&table_metadata).await;
    }

    pub fn get_table_metadata(&self, name: &str) -> Option<Arc<TableMetadata>> {
        self.tables.get(name).map(|entry| entry.value().clone())
    }

    pub async fn update_table_metadata(&self, table_metadata: Arc<TableMetadata>) {
        self.tables.insert(table_metadata.name.clone(), table_metadata.clone());
        
        // 保存表元数据到fjall数据库
        self.save_table(&table_metadata).await;
    }
}
