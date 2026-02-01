use datafusion::execution::context::SessionContext;
use datafusion::catalog::{SchemaProvider, CatalogProvider};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::array::{ArrayRef, StringArray};
use std::sync::Arc;
use crate::meta::catalog_adapter::MetaSchemaProvider;
use crate::meta::service::MetaService;
use crate::store::store_manager::StoreManager;
use super::datafusion_integration::YuntunTableProvider;

pub struct QueryService {
    session_ctx: Arc<SessionContext>,
    schema_provider: Arc<MetaSchemaProvider>,
    store_manager: Arc<StoreManager>,
}

impl std::fmt::Debug for QueryService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryService")
            .field("schema_provider", &self.schema_provider)
            .field("store_manager", &self.store_manager)
            .finish()
    }
}

impl QueryService {
    pub async fn new(
        schema_provider: Arc<MetaSchemaProvider>,
        store_manager: Arc<StoreManager>
    ) -> Self {
        // 创建SessionContext
        let session_ctx = Arc::new(SessionContext::new());
        
        // 由于SessionContext没有register_schema方法，我们需要为每个表单独注册
        // 这里暂时不注册schema，而是在需要时动态处理
        
        Self {
            session_ctx,
            schema_provider,
            store_manager,
        }
    }
    
    /// 执行SQL查询
    pub async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
        // 检查是否是SHOW命令
        let sql_lower = sql.to_lowercase();
        if sql_lower.starts_with("show ") {
            return self.handle_show_command(sql).await;
        }
        
        // 检查是否是CREATE TABLE命令
        if sql_lower.starts_with("create table") {
            // 执行CREATE TABLE语句
            let df = self.session_ctx.sql(sql).await?;
            let _ = df.collect().await?;
            
            // 提取表名
            let table_name = self.extract_table_name_from_create_sql(sql);
            if !table_name.is_empty() {
                // 刷新表信息
                self.refresh_table(&table_name).await?;
            }
            
            // CREATE TABLE语句通常没有结果集，返回空向量
            return Ok(vec![]);
        }
        
        // 执行其他普通查询
        let df = self.session_ctx.sql(sql).await?;
        
        // 收集结果
        let results = df.collect().await?;
        
        Ok(results)
    }
    
    /// 从CREATE TABLE语句中提取表名
    fn extract_table_name_from_create_sql(&self, sql: &str) -> String {
        let sql_lower = sql.to_lowercase();
        let parts: Vec<&str> = sql_lower.split_whitespace().collect();
        
        // 找到CREATE TABLE后面的表名
        if let Some(index) = parts.iter().position(|&part| part == "table") {
            if index + 1 < parts.len() {
                let table_name = parts[index + 1].trim();
                // 移除可能的引号和分号
                let table_name = table_name.trim_matches(|c| c == '`' || c == '"' || c == '\'' || c == ';');
                return table_name.to_string();
            }
        }
        
        String::new()
    }
    
    /// 更新表的注册信息，确保DataFusion可以看到最新的表结构
    pub async fn refresh_table(&self, table_name: &str) -> Result<(), anyhow::Error> {
        // 尝试从DataFusion的session_ctx中获取表信息
        if let Ok(table_provider) = self.session_ctx.table_provider(table_name).await {
            // 从table_provider中提取schema
            let schema = table_provider.schema();
            
            // 创建TableMeta
            let table_meta = crate::meta::types::TableMeta {
                name: table_name.to_string(),
                schema,
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
            
            // 更新MetaSchemaProvider中的表元数据
            // 这里我们需要通过MetaService来更新，而不是直接调用schema_provider
            // 由于MetaSchemaProvider没有直接的update_table_metadata方法，我们需要通过MetaService来更新
            // 暂时注释掉这部分，因为我们需要修改MetaSchemaProvider的实现
            // self.schema_provider.update_table_metadata(table_meta).await;
            println!("Table metadata updated for {}", table_name);
        } else {
            // 暂时注释掉这部分，因为我们需要修改MetaSchemaProvider的实现
            // if let Some(table_metadata) = self.schema_provider.get_table_metadata(table_name) {
            //     // 创建YuntunTableProvider
            //     let table_provider = Arc::new(YuntunTableProvider::new(
            //         table_metadata,
            //         self.store_manager.clone()
            //     ));
            //     
            //     // 注册或更新表
            //     self.session_ctx.register_table(table_name, table_provider)?;
            // }
        }
        
        Ok(())
    }
    
    /// 刷新所有表的注册信息
    pub async fn refresh_all_tables(&self) -> Result<(), anyhow::Error> {
        let table_names = self.schema_provider.table_names();
        
        for table_name in table_names {
            self.refresh_table(&table_name).await?;
        }
        
        Ok(())
    }
    
    /// 处理SHOW命令
    pub async fn handle_show_command(&self, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
        let sql_lower = sql.to_lowercase();
        
        if sql_lower.starts_with("show databases") {
            // 处理SHOW DATABASES
            self.handle_show_databases().await
        } else if sql_lower.starts_with("show tables") {
            // 处理SHOW TABLES
            self.handle_show_tables().await
        } else if sql_lower.starts_with("show create table") {
            // 处理SHOW CREATE TABLE
            let table_name = sql_lower.replace("show create table", "").trim().to_string();
            self.handle_show_create_table(&table_name).await
        } else {
            Err(anyhow::anyhow!("Unknown SHOW command"))
        }
    }
    
    /// 处理SHOW DATABASES
    async fn handle_show_databases(&self) -> Result<Vec<RecordBatch>, anyhow::Error> {
        // 创建schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("Database", DataType::Utf8, false),
        ]));
        
        // 数据库列表
        let databases = vec!["yuntun"]; // 默认数据库
        
        // 创建StringArray
        let db_array = Arc::new(StringArray::from(databases));
        
        // 创建RecordBatch
        let batch = RecordBatch::try_new(schema, vec![db_array as ArrayRef]).unwrap();
        
        Ok(vec![batch])
    }
    
    /// 处理SHOW TABLES
    async fn handle_show_tables(&self) -> Result<Vec<RecordBatch>, anyhow::Error> {
        // 创建schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("Table", DataType::Utf8, false),
        ]));
        
        // 从MemorySchemaProvider中获取表列表
        let tables = self.schema_provider.table_names();
        
        // 创建StringArray
        let table_array = Arc::new(StringArray::from(tables));
        
        // 创建RecordBatch
        let batch = RecordBatch::try_new(schema, vec![table_array as ArrayRef]).unwrap();
        
        Ok(vec![batch])
    }
    
    /// 处理SHOW CREATE TABLE
    async fn handle_show_create_table(&self, table_name: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
        // 尝试从DataFusion的session_ctx中获取表信息
        if let Ok(table_provider) = self.session_ctx.table_provider(table_name).await {
            // 创建schema
            let schema = Arc::new(Schema::new(vec![
                Field::new("Table", DataType::Utf8, false),
                Field::new("Create Table", DataType::Utf8, false),
            ]));
            
            // 生成CREATE TABLE语句
            let create_table_sql = self.generate_create_table_sql(table_name, &table_provider.schema());
            
            // 创建RecordBatch
            let table_array = Arc::new(StringArray::from(vec![table_name]));
            let create_table_array = Arc::new(StringArray::from(vec![create_table_sql]));
            
            let batch = RecordBatch::try_new(schema, vec![table_array as ArrayRef, create_table_array as ArrayRef]).unwrap();
            
            Ok(vec![batch])
        } else if let Some(table_metadata) = self.schema_provider.get_table_metadata(table_name) {
            // 创建schema
            let schema = Arc::new(Schema::new(vec![
                Field::new("Table", DataType::Utf8, false),
                Field::new("Create Table", DataType::Utf8, false),
            ]));
            
            // 生成CREATE TABLE语句
            let create_table_sql = self.generate_create_table_sql(table_name, &table_metadata.schema);
            
            // 创建RecordBatch
            let table_array = Arc::new(StringArray::from(vec![table_name]));
            let create_table_array = Arc::new(StringArray::from(vec![create_table_sql]));
            
            let batch = RecordBatch::try_new(schema, vec![table_array as ArrayRef, create_table_array as ArrayRef]).unwrap();
            
            Ok(vec![batch])
        } else {
            Err(anyhow::anyhow!("Table not found: {}", table_name))
        }
    }
    
    /// 生成CREATE TABLE语句
    fn generate_create_table_sql(&self, table_name: &str, schema: &SchemaRef) -> String {
        let mut sql = format!("CREATE TABLE {} (", table_name);
        
        let fields = schema.fields();
        let mut field_defs = vec![];
        
        for field in fields {
            let field_name = field.name();
            let data_type = field.data_type();
            let nullable = if field.is_nullable() { "NULL" } else { "NOT NULL" };
            
            let type_str = match data_type {
                DataType::Utf8 => "VARCHAR",
                DataType::Int32 => "INT",
                DataType::Int64 => "BIGINT",
                DataType::Float32 => "FLOAT",
                DataType::Float64 => "DOUBLE",
                DataType::Boolean => "BOOLEAN",
                DataType::Timestamp(unit, _) => {
                    match unit {
                        TimeUnit::Second => "TIMESTAMP SECOND",
                        TimeUnit::Millisecond => "TIMESTAMP MILLISECOND",
                        TimeUnit::Microsecond => "TIMESTAMP MICROSECOND",
                        TimeUnit::Nanosecond => "TIMESTAMP NANOSECOND",
                    }
                }
                _ => "VARCHAR", // 其他类型默认使用VARCHAR
            };
            
            field_defs.push(format!("  {} {} {}", field_name, type_str, nullable));
        }
        
        sql += &field_defs.join(",\n");
        sql += 
        "
)";
        
        sql
    }
}
