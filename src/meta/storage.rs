use fjall::{Database, KeyspaceCreateOptions};
use serde_json;
use tokio::sync::RwLock;
use std::sync::Arc;

use crate::meta::types::{DatabaseMeta, TableMeta, TableMetaSerde, StorageMeta, IndexMeta};

/// Metadata storage implementation using fjall
#[derive(Clone)]
pub struct MetaStorage {
    db: Arc<RwLock<Option<Database>>>,
}

impl std::fmt::Debug for MetaStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaStorage")
            .field("db", &"Arc<RwLock<Option<Database>>>")
            .finish()
    }
}

impl MetaStorage {
    /// Create a new metadata storage
    pub fn new() -> Self {
        Self {
            db: Arc::new(RwLock::new(None)),
        }
    }

    /// Initialize the storage with a database
    pub async fn init(&self, db: Database) {
        *self.db.write().await = Some(db);
        println!("Meta storage initialized with fjall database");
    }

    /// Save database metadata
    pub async fn save_database_meta(&self, meta: &DatabaseMeta) -> Result<(), anyhow::Error> {
        if let Some(db) = &*self.db.read().await {
            let keyspace = db.keyspace("metadata", KeyspaceCreateOptions::default)?;
            let key = format!("db:{}", meta.name);
            let value = serde_json::to_vec(meta)?;
            keyspace.insert(key.as_bytes(), value)?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Meta storage not initialized"))
        }
    }

    /// Load database metadata
    pub async fn load_database_meta(&self, db_name: &str) -> Result<Option<DatabaseMeta>, anyhow::Error> {
        if let Some(db) = &*self.db.read().await {
            let keyspace = db.keyspace("metadata", KeyspaceCreateOptions::default)?;
            let key = format!("db:{}", db_name);
            match keyspace.get(key.as_bytes()) {
                Ok(Some(value)) => {
                    let meta = serde_json::from_slice(&value)?;
                    Ok(Some(meta))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(anyhow::anyhow!("Failed to load database metadata: {:?}", e)),
            }
        } else {
            Err(anyhow::anyhow!("Meta storage not initialized"))
        }
    }

    /// Save table metadata
    pub async fn save_table_meta(&self, meta: &TableMeta) -> Result<(), anyhow::Error> {
        if let Some(db) = &*self.db.read().await {
            let keyspace = db.keyspace("metadata", KeyspaceCreateOptions::default)?;
            let key = format!("table:{}", meta.name);
            let serde = meta.to_serde();
            let value = serde_json::to_vec(&serde)?;
            keyspace.insert(key.as_bytes(), value)?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Meta storage not initialized"))
        }
    }

    /// Load table metadata
    pub async fn load_table_meta(&self, table_name: &str) -> Result<Option<TableMeta>, anyhow::Error> {
        if let Some(db) = &*self.db.read().await {
            let keyspace = db.keyspace("metadata", KeyspaceCreateOptions::default)?;
            let key = format!("table:{}", table_name);
            match keyspace.get(key.as_bytes()) {
                Ok(Some(value)) => {
                    let serde: TableMetaSerde = serde_json::from_slice(&value)?;
                    let meta = TableMeta::from_serde(serde)?;
                    Ok(Some(meta))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(anyhow::anyhow!("Failed to load table metadata: {:?}", e)),
            }
        } else {
            Err(anyhow::anyhow!("Meta storage not initialized"))
        }
    }

    /// Load all table metadata
    pub async fn load_all_table_meta(&self) -> Result<Vec<TableMeta>, anyhow::Error> {
        let mut result = Vec::new();
        if let Some(db) = &*self.db.read().await {
            let keyspace = db.keyspace("metadata", KeyspaceCreateOptions::default)?;
            let iter = keyspace.prefix("table:");
            for guard in iter {
                if let Ok(value) = guard.value() {
                    if let Ok(serde) = serde_json::from_slice::<TableMetaSerde>(&value) {
                        if let Ok(meta) = TableMeta::from_serde(serde) {
                            result.push(meta);
                        }
                    }
                }
            }
        }
        Ok(result)
    }

    /// Delete table metadata
    pub async fn delete_table_meta(&self, table_name: &str) -> Result<(), anyhow::Error> {
        if let Some(db) = &*self.db.read().await {
            let keyspace = db.keyspace("metadata", KeyspaceCreateOptions::default)?;
            let key = format!("table:{}", table_name);
            keyspace.remove(key.as_bytes())?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Meta storage not initialized"))
        }
    }

    /// Save storage metadata
    pub async fn save_storage_meta(&self, meta: &StorageMeta) -> Result<(), anyhow::Error> {
        if let Some(db) = &*self.db.read().await {
            let keyspace = db.keyspace("metadata", KeyspaceCreateOptions::default)?;
            let key = format!("storage:{}", meta.storage_id);
            let value = serde_json::to_vec(meta)?;
            keyspace.insert(key.as_bytes(), value)?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Meta storage not initialized"))
        }
    }

    /// Save index metadata
    pub async fn save_index_meta(&self, meta: &IndexMeta) -> Result<(), anyhow::Error> {
        if let Some(db) = &*self.db.read().await {
            let keyspace = db.keyspace("metadata", KeyspaceCreateOptions::default)?;
            let key = format!("index:{}", meta.index_id);
            let value = serde_json::to_vec(meta)?;
            keyspace.insert(key.as_bytes(), value)?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Meta storage not initialized"))
        }
    }
}
