pub mod catalog;
pub mod core;
pub mod ingest;
pub mod query;
pub mod store;
pub mod meta;
pub mod flight_sql;
pub mod pgwire;

#[cfg(test)]
mod tests {
    use super::*;
    use catalog::{catalog_provider::MemoryCatalogProvider, schema_provider::MemorySchemaProvider};
    use store::{store_manager::StoreManager, chunk::Chunk};
    use ingest::ingest_service::IngestService;
    use query::query_service::QueryService;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_ingest_and_query() {
        // 初始化存储管理器
        let store_manager = Arc::new(StoreManager::new("./test_data".to_string()));
        
        // 初始化schema提供者
        let schema_provider = Arc::new(MemorySchemaProvider::new_with_store_manager(store_manager.clone()));
        
        // 初始化catalog提供者
        let catalog_provider = Arc::new(MemoryCatalogProvider::new("yuntun".to_string()));
        catalog_provider.add_default_schema();
        
        // 初始化摄入服务
        let ingest_service = Arc::new(IngestService::new(schema_provider.clone(), store_manager.clone()));
        
        // 初始化查询服务
        let query_service = Arc::new(QueryService::new(
            catalog_provider.clone(),
            schema_provider.clone(),
            store_manager.clone()
        ).await);
        
        // 测试数据摄入
        let test_data = "cpu,host=server01,region=us-west value=0.64 1434055562000000000";
        ingest_service.ingest_line_protocol(test_data).await.unwrap();
        
        // 刷新表信息
        query_service.refresh_table("cpu").await.unwrap();
        
        // 测试SQL查询
        let results = query_service.execute_sql("SELECT * FROM cpu").await.unwrap();
        assert!(!results.is_empty());
        println!("Query returned {} record batches", results.len());
        
        // 清理测试数据
        std::fs::remove_dir_all("./test_data").unwrap();
    }

    #[tokio::test]
    async fn test_store_manager() {
        // 初始化存储管理器
        let store_manager = Arc::new(StoreManager::new("./test_store".to_string()));
        
        // 创建测试数据
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));
        
        let id_array = Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]));
        let name_array = Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"]));
        
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![id_array as Arc<dyn arrow::array::Array>, name_array as Arc<dyn arrow::array::Array>]
        ).unwrap();
        
        // 创建内存chunk
        let chunk = store_manager.create_memory_chunk("test_chunk".to_string(), vec![batch]);
        assert_eq!(chunk.row_count(), 3);
        
        // 持久化chunk
        let parquet_chunk = store_manager.persist_chunk("test_chunk").await.unwrap();
        assert_eq!(parquet_chunk.chunk_id(), "test_chunk");
        
        // 清理测试数据
        std::fs::remove_dir_all("./test_store").unwrap();
    }
    
    #[tokio::test]
    async fn test_ddl_commands() {
        // 初始化存储管理器
        let store_manager = Arc::new(StoreManager::new("./test_ddl".to_string()));
        
        // 初始化schema提供者
        let schema_provider = Arc::new(MemorySchemaProvider::new_with_store_manager(store_manager.clone()));
        
        // 初始化catalog提供者
        let catalog_provider = Arc::new(MemoryCatalogProvider::new("yuntun".to_string()));
        catalog_provider.add_default_schema();
        
        // 初始化查询服务
        let query_service = Arc::new(QueryService::new(
            catalog_provider.clone(),
            schema_provider.clone(),
            store_manager.clone()
        ).await);
        
        // 测试SHOW DATABASES
        let results = query_service.execute_sql("SHOW DATABASES").await.unwrap();
        assert!(!results.is_empty());
        println!("SHOW DATABASES returned {} batches", results.len());
        
        // 测试CREATE TABLE
        let create_sql = "CREATE TABLE test_table (id INT NOT NULL, name VARCHAR NOT NULL, value DOUBLE)";
        query_service.execute_sql(create_sql).await.unwrap();
        println!("CREATE TABLE executed successfully");
        
        // 刷新表信息
        query_service.refresh_table("test_table").await.unwrap();
        println!("Table refreshed successfully");
        
        // 测试SHOW TABLES
        let results = query_service.execute_sql("SHOW TABLES").await.unwrap();
        assert!(!results.is_empty());
        println!("SHOW TABLES returned {} batches", results.len());
        
        // 测试SHOW CREATE TABLE
        let results = query_service.execute_sql("SHOW CREATE TABLE test_table").await.unwrap();
        assert!(!results.is_empty());
        println!("SHOW CREATE TABLE returned {} batches", results.len());
        
        // 清理测试数据
        std::fs::remove_dir_all("./test_ddl").unwrap();
    }
}
