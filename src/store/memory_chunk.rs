use super::chunk::Chunk;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use std::sync::Arc;

#[derive(Debug)]
pub struct MemoryChunk {
    chunk_id: String,
    batches: Vec<RecordBatch>,
    table_provider: Arc<MemTable>,
    row_count: usize,
    size_in_bytes: usize,
}

impl MemoryChunk {
    pub fn new(chunk_id: String, batches: Vec<RecordBatch>) -> Self {
        let schema = batches[0].schema();
        let partitions = vec![batches.clone()];
        let table_provider = Arc::new(MemTable::try_new(schema, partitions).unwrap());
        
        let row_count = batches.iter().map(|batch| batch.num_rows()).sum();
        let size_in_bytes = batches.iter().map(|batch| {
            batch.columns().iter().map(|col| col.get_array_memory_size()).sum::<usize>()
        }).sum();
        
        Self {
            chunk_id,
            batches,
            table_provider,
            row_count,
            size_in_bytes,
        }
    }
}

impl Chunk for MemoryChunk {
    fn chunk_id(&self) -> &str {
        &self.chunk_id
    }
    
    fn row_count(&self) -> usize {
        self.row_count
    }
    
    fn size_in_bytes(&self) -> usize {
        self.size_in_bytes
    }
    
    fn as_table_provider(&self) -> Arc<dyn TableProvider> {
        self.table_provider.clone()
    }
    
    fn to_record_batches(&self) -> Vec<RecordBatch> {
        self.batches.clone()
    }
}
