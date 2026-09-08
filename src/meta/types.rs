use serde::{Deserialize, Serialize};
use std::sync::Arc;
use arrow::datatypes::SchemaRef;

/// Database metadata
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatabaseMeta {
    pub name: String,
    pub tables: Vec<String>,
    pub created_at: u64,
    pub updated_at: u64,
}

/// Table metadata
#[derive(Debug, Clone)]
pub struct TableMeta {
    pub name: String,
    pub schema: SchemaRef,
    pub chunks: Vec<ChunkMeta>,
    pub created_at: u64,
    pub updated_at: u64,
}

/// Table metadata for query execution (alias for compatibility)
pub type TableMetadata = TableMeta;

/// Chunk metadata for query execution (alias for compatibility)
pub type ChunkMetadata = ChunkMeta;

/// Table metadata for serialization
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableMetaSerde {
    pub name: String,
    pub schema: serde_json::Value,
    pub chunks: Vec<ChunkMeta>,
    pub created_at: u64,
    pub updated_at: u64,
}

impl TableMeta {
    pub fn to_serde(&self) -> TableMetaSerde {
        // 简化处理，暂时不序列化schema
        let schema_json = serde_json::Value::Null;
        TableMetaSerde {
            name: self.name.clone(),
            schema: schema_json,
            chunks: self.chunks.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
    
    pub fn from_serde(serde: TableMetaSerde) -> Result<Self, serde_json::Error> {
        // 简化处理，暂时使用空schema
        let schema = arrow::datatypes::Schema::empty();
        Ok(Self {
            name: serde.name,
            schema: Arc::new(schema),
            chunks: serde.chunks,
            created_at: serde.created_at,
            updated_at: serde.updated_at,
        })
    }
}

/// Chunk metadata
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChunkMeta {
    pub chunk_id: String,
    pub chunk_type: ChunkType,
    pub row_count: usize,
    pub size_in_bytes: usize,
    pub created_at: u64,
    pub partition_info: Option<std::collections::HashMap<String, String>>,
    pub index_info: Option<std::collections::HashMap<String, String>>,
}

/// Chunk type
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum ChunkType {
    Memory,
    Parquet,
}

/// Storage metadata
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageMeta {
    pub storage_id: String,
    pub storage_type: StorageType,
    pub path: String,
    pub config: serde_json::Value,
    pub created_at: u64,
    pub updated_at: u64,
}

/// Storage type
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum StorageType {
    Local,
    S3,
    GCS,
    Azure,
}

/// Index metadata
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndexMeta {
    pub index_id: String,
    pub table_name: String,
    pub column_name: String,
    pub index_type: IndexType,
    pub created_at: u64,
    pub updated_at: u64,
}

/// Index type
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum IndexType {
    Btree,
    Hash,
    Bitmap,
}
