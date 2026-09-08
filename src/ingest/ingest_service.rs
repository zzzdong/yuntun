use crate::meta::catalog_adapter::MetaSchemaProvider;
use crate::meta::service::MetaService;
use crate::meta::types::{TableMeta, ChunkMeta, ChunkType};
use datafusion::catalog::SchemaProvider;
use crate::store::store_manager::StoreManager;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug)]
pub struct IngestService {
    schema_provider: Arc<MetaSchemaProvider>,
    meta_service: Arc<MetaService>,
    store_manager: Arc<StoreManager>,
}

impl IngestService {
    pub fn new(schema_provider: Arc<MetaSchemaProvider>, meta_service: Arc<MetaService>, store_manager: Arc<StoreManager>) -> Self {
        Self {
            schema_provider,
            meta_service,
            store_manager,
        }
    }



    /// 直接摄入 RecordBatch 列表
    pub async fn ingest_batches(&self, db: &str, batches: HashMap<String, RecordBatch>) -> Result<(), anyhow::Error> {
        // 处理每个measurement的数据
        for (measurement, batch) in batches {
            let table_name = format!("{}.{}", db, measurement);
            self.process_measurement(&table_name, vec![batch]).await?;
        }

        Ok(())
    }


    
    /// 处理单个表的数据
    async fn process_measurement(&self, table_name: &str, batches: Vec<RecordBatch>) -> Result<(), anyhow::Error> {
        // 检查表是否存在
        if !self.schema_provider.table_exist(table_name) {
            // 创建新表
            self.create_table(table_name, &batches[0]).await?;
        }

        // 创建新的内存chunk
        let chunk_id = Uuid::new_v4().to_string();

        // 计算row_count和size_in_bytes
        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        let size_in_bytes: usize = batches.iter().map(|batch| {
            batch.columns().iter().map(|col| col.get_array_memory_size()).sum::<usize>()
        }).sum();

        // 克隆batches后再移动
        self.store_manager.create_memory_chunk(chunk_id.clone(), batches.clone());

        // 更新表的chunk信息
        let chunk_meta = ChunkMeta {
            chunk_id: chunk_id.clone(),
            chunk_type: ChunkType::Memory,
            row_count,
            size_in_bytes,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            partition_info: None,
            index_info: None,
        };

        self.meta_service.add_chunk_to_table(table_name, chunk_meta).await?;

        Ok(())
    }
    
    /// 创建新表
    async fn create_table(&self, table_name: &str, batch: &RecordBatch) -> Result<(), anyhow::Error> {
        let table_meta = TableMeta {
            name: table_name.to_string(),
            schema: batch.schema(),
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

        self.meta_service.add_table(table_meta).await?;

        Ok(())
    }
}
