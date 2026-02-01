use arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use std::sync::Arc;

pub trait Chunk: Send + Sync {
    fn chunk_id(&self) -> &str;
    fn row_count(&self) -> usize;
    fn size_in_bytes(&self) -> usize;
    fn as_table_provider(&self) -> Arc<dyn TableProvider>;
    fn to_record_batches(&self) -> Vec<RecordBatch>;
}
