use super::chunk::Chunk;
use super::memory_chunk::MemoryChunk;
use super::parquet_chunk::ParquetChunk;
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::Arc;

pub struct StoreManager {
    chunks: DashMap<String, Arc<dyn Chunk>>,
    memory_chunks: DashMap<String, Arc<MemoryChunk>>,
    base_path: String,
    store: Arc<LocalFileSystem>,
}

impl std::fmt::Debug for StoreManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreManager")
            .field("base_path", &self.base_path)
            .field("memory_chunks", &self.memory_chunks)
            .finish()
    }
}

impl StoreManager {
    pub fn new(base_path: String) -> Self {
        // 确保base_path目录存在
        std::fs::create_dir_all(&base_path).unwrap();
        
        Self {
            chunks: DashMap::new(),
            memory_chunks: DashMap::new(),
            base_path,
            store: Arc::new(LocalFileSystem::new()),
        }
    }
    
    /// 创建新的内存chunk
    pub fn create_memory_chunk(&self, chunk_id: String, batches: Vec<RecordBatch>) -> Arc<MemoryChunk> {
        let chunk = Arc::new(MemoryChunk::new(chunk_id.clone(), batches));
        self.chunks.insert(chunk_id.clone(), chunk.clone() as Arc<dyn Chunk>);
        self.memory_chunks.insert(chunk_id, chunk.clone());
        chunk
    }
    
    /// 将内存chunk持久化为parquet文件
    pub async fn persist_chunk(&self, chunk_id: &str) -> Result<Arc<ParquetChunk>, anyhow::Error> {
        if let Some(memory_chunk) = self.memory_chunks.get(chunk_id) {
            let chunk = memory_chunk.value();
            
            // 生成parquet文件路径
            let data_dir = format!("{}/data", self.base_path);
            std::fs::create_dir_all(&data_dir).unwrap();
            let file_path = format!("{}/{}.parquet", data_dir, chunk_id);
            
            // 写入parquet文件
            let file = File::create(&file_path)?;
            let writer_props = WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(file, chunk.to_record_batches()[0].schema(), Some(writer_props))?;
            
            for batch in chunk.to_record_batches() {
                writer.write(&batch)?;
            }
            writer.close()?;
            
            // 创建ParquetChunk
            let parquet_chunk = ParquetChunk::new(
                chunk_id.to_string(),
                file_path,
                chunk.to_record_batches()[0].schema()
            ).await;
            let parquet_chunk_arc = Arc::new(parquet_chunk);
            
            // 更新chunks映射
            self.chunks.insert(chunk_id.to_string(), parquet_chunk_arc.clone() as Arc<dyn Chunk>);
            // 从memory_chunks中移除
            self.memory_chunks.remove(chunk_id);
            
            Ok(parquet_chunk_arc)
        } else {
            Err(anyhow::anyhow!("Memory chunk not found: {}", chunk_id))
        }
    }
    
    /// 获取chunk
    pub fn get_chunk(&self, chunk_id: &str) -> Option<Arc<dyn Chunk>> {
        self.chunks.get(chunk_id).map(|entry| entry.value().clone())
    }
    
    /// 列出所有chunk
    pub fn list_chunks(&self) -> Vec<String> {
        self.chunks
            .iter()
            .map(|chunk| chunk.key().clone())
            .collect()
    }
    
    /// 检查内存中的chunk是否需要落盘
    pub async fn check_and_persist_chunks(&self, max_rows: usize, max_age_seconds: u64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        for entry in self.memory_chunks.iter() {
            let chunk_id = entry.key();
            let chunk = entry.value();
            
            // 检查行数是否超过阈值
            if chunk.row_count() >= max_rows {
                // 落盘
                self.persist_chunk(chunk_id).await.unwrap();
            }
            
            // 这里可以添加基于时间的检查逻辑
            // 暂时省略
        }
    }
}
