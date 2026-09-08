use super::datafusion_integration::YuntunTableProvider;
use crate::meta::catalog_adapter::MetaSchemaProvider;
use crate::store::store_manager::StoreManager;
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion_catalog::SchemaProvider;
use std::sync::Arc;

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
        store_manager: Arc<StoreManager>,
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

    /// 执行SQL查询 - 使用 DataFusion SQL 解析器而非字符串匹配
    pub async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
        // // 检查是否是特殊命令
        // let sql_upper = sql.trim().to_uppercase();
        // if sql_upper.starts_with("SHOW TABLES") {
        //     return self.handle_show_tables_command().await;
        // }
        // if sql_upper.starts_with("SHOW VARIABLE") || sql_upper.starts_with("SHOW DATABASES") {
        //     return self.handle_show_variable_command(&[]).await;
        // }
        // if sql_upper.starts_with("SHOW CREATE") {
        //     return Err(anyhow::anyhow!("SHOW CREATE TABLE is not yet supported"));
        // }

        // 其他 SQL 语句,直接通过 DataFusion 执行
        let df = self.session_ctx.sql(sql).await?;
        let results = df.collect().await?;
        Ok(results)
    }

    /// 处理 CREATE TABLE 命令
    async fn handle_create_table_command(
        &self,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, anyhow::Error> {
        // 执行 CREATE TABLE 语句
        let df = self.session_ctx.sql(sql).await?;
        let _ = df.collect().await?;

        // 刷新表信息
        self.refresh_all_tables().await?;

        Ok(vec![])
    }

    /// 更新表的注册信息,确保DataFusion可以看到最新的表结构
    pub async fn refresh_table(&self, table_name: &str) -> Result<(), anyhow::Error> {
        // 从 MetaSchemaProvider 获取表元数据
        if let Some(table_meta) = self.schema_provider.get_table_metadata(table_name) {
            // 创建 YuntunTableProvider
            let table_provider = Arc::new(YuntunTableProvider::new(
                Arc::new(table_meta),
                self.store_manager.clone(),
            ));

            // 注册或更新表到 SessionContext
            self.session_ctx
                .register_table(table_name, table_provider)?;
            println!("Table refreshed and registered: {}", table_name);
        } else {
            println!("Table not found in metadata: {}", table_name);
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

    /// 处理 SHOW TABLES 命令
    async fn handle_show_tables_command(
        &self,
    ) -> Result<Vec<RecordBatch>, anyhow::Error> {
        let table_names = self.schema_provider.table_names();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "Table",
            DataType::Utf8,
            false,
        )]));

        let table_array = Arc::new(StringArray::from(table_names));
        let batch = RecordBatch::try_new(schema, vec![table_array as ArrayRef]).unwrap();

        Ok(vec![batch])
    }

    /// 处理 SHOW VARIABLE 命令
    async fn handle_show_variable_command(
        &self,
        _variable: &[String],
    ) -> Result<Vec<RecordBatch>, anyhow::Error> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "Database",
            DataType::Utf8,
            false,
        )]));

        let databases = vec!["yuntun"];
        let db_array = Arc::new(StringArray::from(databases));
        let batch = RecordBatch::try_new(schema, vec![db_array as ArrayRef]).unwrap();

        Ok(vec![batch])
    }
}
