use super::line_protocol::{from_lines, parse_line_protocol, to_record_batch};
use crate::meta::catalog_adapter::MetaSchemaProvider;
use crate::meta::types::TableMeta;
use datafusion::catalog::SchemaProvider;
use crate::store::store_manager::StoreManager;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug)]
pub struct IngestService {
    schema_provider: Arc<MetaSchemaProvider>,
    store_manager: Arc<StoreManager>,
}

impl IngestService {
    pub fn new(schema_provider: Arc<MetaSchemaProvider>, store_manager: Arc<StoreManager>) -> Self {
        Self {
            schema_provider,
            store_manager,
        }
    }
    
    /// 摄入influx Line protocol格式的数据
    pub async fn ingest_line_protocol(&self, data: &str) -> Result<(), anyhow::Error> {
        // 解析数据为RecordBatch
        let batches = from_lines(data);
        
        // 按measurement分组处理
        let mut measurement_batches: std::collections::HashMap<String, Vec<RecordBatch>> = std::collections::HashMap::new();
        
        for batch in batches {
            // 从batch中提取measurement
            let measurement_array = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            let measurement = measurement_array.value(0).to_string();
            
            // 添加到对应分组
            measurement_batches.entry(measurement).or_default().push(batch);
        }
        
        // 处理每个measurement的数据
        for (measurement, batches) in measurement_batches {
            self.process_measurement(&measurement, batches).await?;
        }
        
        Ok(())
    }
    
    /// 处理单个measurement的数据
    async fn process_measurement(&self, measurement: &str, batches: Vec<RecordBatch>) -> Result<(), anyhow::Error> {
        // 检查表是否存在
        if !self.schema_provider.table_exist(measurement) {
            // 创建新表
            self.create_table(measurement, &batches[0]).await?;
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
        // 暂时注释掉这部分，因为MetaSchemaProvider还没有实现这些方法
        // if let Some(table_metadata) = self.schema_provider.get_table_metadata(measurement) {
        //     let mut new_metadata = (*table_metadata).clone();
        //     new_metadata.chunks.push(crate::meta::types::ChunkMeta {
        //         chunk_id,
        //         chunk_type: crate::meta::types::ChunkType::Memory,
        //         row_count,
        //         size_in_bytes,
        //         created_at: std::time::SystemTime::now()
        //             .duration_since(std::time::UNIX_EPOCH)
        //             .unwrap()
        //             .as_secs(),
        //         partition_info: None,
        //         index_info: None,
        //     });
        //     new_metadata.updated_at = std::time::SystemTime::now()
        //         .duration_since(std::time::UNIX_EPOCH)
        //         .unwrap()
        //         .as_secs();
        //     
        //     self.schema_provider.update_table_metadata(Arc::new(new_metadata));
        // }
        
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
        
        // 暂时注释掉这部分，因为MetaSchemaProvider还没有实现add_table方法
        // self.schema_provider.add_table(Arc::new(table_meta));
        
        Ok(())
    }
}
