use std::sync::Arc;
use tokio::sync::RwLock;
use anyhow::Result;

use crate::meta::types::{DatabaseMeta, TableMeta, StorageMeta, IndexMeta};
use crate::meta::storage::MetaStorage;
use fjall::{Database, Config};

/// Meta service for centralized metadata management
#[derive(Debug, Clone)]
pub struct MetaService {
    storage: MetaStorage,
    databases: Arc<RwLock<Vec<DatabaseMeta>>>,
    tables: Arc<RwLock<Vec<TableMeta>>>,
    storages: Arc<RwLock<Vec<StorageMeta>>>,
    indexes: Arc<RwLock<Vec<IndexMeta>>>,
}

impl MetaService {
    /// Create a new meta service
    pub fn new() -> Self {
        Self {
            storage: MetaStorage::new(),
            databases: Arc::new(RwLock::new(Vec::new())),
            tables: Arc::new(RwLock::new(Vec::new())),
            storages: Arc::new(RwLock::new(Vec::new())),
            indexes: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Initialize the meta service
    pub async fn init(&self) -> Result<()> {
        // 确保存储目录存在
        let meta_dir = "./storage/meta";
        std::fs::create_dir_all(meta_dir).unwrap();
        
        // Open or create fjall database
        let config = Config::new(std::path::Path::new(meta_dir));
        let db = Database::open(config)?;
        
        // Initialize storage
        self.storage.init(db).await;
        
        // Load all metadata
        self.load_all_metadata().await?;
        
        println!("Meta service initialized successfully");
        Ok(())
    }

    /// Load all metadata from storage
    pub async fn load_all_metadata(&self) -> Result<()> {
        // Load all table metadata
        let tables = self.storage.load_all_table_meta().await?;
        *self.tables.write().await = tables;
        
        println!("Loaded {} tables from storage", self.tables.read().await.len());
        Ok(())
    }

    /// Get database metadata
    pub async fn get_database(&self, name: &str) -> Result<Option<DatabaseMeta>> {
        let databases = self.databases.read().await;
        Ok(databases.iter().find(|db| db.name == name).cloned())
    }

    /// Add database metadata
    pub async fn add_database(&self, meta: DatabaseMeta) -> Result<()> {
        let mut databases = self.databases.write().await;
        if !databases.iter().any(|db| db.name == meta.name) {
            databases.push(meta.clone());
            self.storage.save_database_meta(&meta).await?;
        }
        Ok(())
    }

    /// Get table metadata
    pub async fn get_table(&self, name: &str) -> Result<Option<TableMeta>> {
        let tables = self.tables.read().await;
        Ok(tables.iter().find(|table| table.name == name).cloned())
    }

    /// Add table metadata
    pub async fn add_table(&self, meta: TableMeta) -> Result<()> {
        let mut tables = self.tables.write().await;
        if let Some(index) = tables.iter().position(|table| table.name == meta.name) {
            tables[index] = meta.clone();
        } else {
            tables.push(meta.clone());
        }
        self.storage.save_table_meta(&meta).await?;
        Ok(())
    }

    /// Delete table metadata
    pub async fn delete_table(&self, name: &str) -> Result<()> {
        let mut tables = self.tables.write().await;
        if tables.iter().any(|table| table.name == name) {
            tables.retain(|table| table.name != name);
            self.storage.delete_table_meta(name).await?;
        }
        Ok(())
    }

    /// Get all table names
    pub async fn get_all_table_names(&self) -> Result<Vec<String>> {
        let tables = self.tables.read().await;
        Ok(tables.iter().map(|table| table.name.clone()).collect())
    }

    /// Add a chunk to a table
    pub async fn add_chunk_to_table(&self, table_name: &str, chunk_meta: crate::meta::types::ChunkMeta) -> Result<()> {
        let mut tables = self.tables.write().await;
        if let Some(index) = tables.iter().position(|table| table.name == table_name) {
            tables[index].chunks.push(chunk_meta.clone());
            tables[index].updated_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            self.storage.save_table_meta(&tables[index]).await?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Table '{}' not found", table_name))
        }
    }

    /// Get storage metadata
    pub async fn get_storage(&self, storage_id: &str) -> Result<Option<StorageMeta>> {
        let storages = self.storages.read().await;
        Ok(storages.iter().find(|s| s.storage_id == storage_id).cloned())
    }

    /// Add storage metadata
    pub async fn add_storage(&self, meta: StorageMeta) -> Result<()> {
        let mut storages = self.storages.write().await;
        if let Some(index) = storages.iter().position(|s| s.storage_id == meta.storage_id) {
            storages[index] = meta.clone();
        } else {
            storages.push(meta.clone());
        }
        self.storage.save_storage_meta(&meta).await?;
        Ok(())
    }

    /// Get index metadata
    pub async fn get_index(&self, index_id: &str) -> Result<Option<IndexMeta>> {
        let indexes = self.indexes.read().await;
        Ok(indexes.iter().find(|i| i.index_id == index_id).cloned())
    }

    /// Add index metadata
    pub async fn add_index(&self, meta: IndexMeta) -> Result<()> {
        let mut indexes = self.indexes.write().await;
        if let Some(index) = indexes.iter().position(|i| i.index_id == meta.index_id) {
            indexes[index] = meta.clone();
        } else {
            indexes.push(meta.clone());
        }
        self.storage.save_index_meta(&meta).await?;
        Ok(())
    }
}
