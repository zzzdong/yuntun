use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use arrow::datatypes::SchemaRef;

#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub name: String,
    pub schema: SchemaRef,
    pub chunks: Vec<ChunkMetadata>,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableMetadataSerde {
    pub name: String,
    pub schema: serde_json::Value,
    pub chunks: Vec<ChunkMetadata>,
    pub created_at: u64,
    pub updated_at: u64,
}

impl TableMetadata {
    pub fn to_serde(&self) -> TableMetadataSerde {
        // 简化处理，暂时不序列化schema
        let schema_json = serde_json::Value::Null;
        TableMetadataSerde {
            name: self.name.clone(),
            schema: schema_json,
            chunks: self.chunks.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
    
    pub fn from_serde(serde: TableMetadataSerde) -> Result<Self, serde_json::Error> {
        // 简化处理，暂时使用空schema
        let schema = arrow::datatypes::Schema::empty();
        Ok(Self {
            name: serde.name,
            schema: std::sync::Arc::new(schema),
            chunks: serde.chunks,
            created_at: serde.created_at,
            updated_at: serde.updated_at,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChunkMetadata {
    pub chunk_id: String,
    pub chunk_type: ChunkType,
    pub row_count: usize,
    pub size_in_bytes: usize,
    pub created_at: u64,
    pub partition_info: Option<HashMap<String, String>>,
    pub index_info: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum ChunkType {
    Memory,
    Parquet,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatabaseMetadata {
    pub name: String,
    pub tables: Vec<String>,
    pub created_at: u64,
    pub updated_at: u64,
}
