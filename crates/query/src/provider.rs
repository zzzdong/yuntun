//! DataFusion CatalogProvider / SchemaProvider（详细设计 §6.7）。
//!
//! 读路径全部走本地缓存（C7），同步 + 异步混合 trait 下零网络调用。

use crate::cache::LocalCatalogCache;
use crate::table::YuntunTableProvider;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::TableType;
use std::sync::Arc;

/// 表类型常量（SchemaProvider::table_type 用）。
const BASE_TABLE: TableType = TableType::Base;

/// schema 层：表发现。
#[derive(Debug)]
pub struct YuntunSchemaProvider {
    cache: Arc<LocalCatalogCache>,
}

impl YuntunSchemaProvider {
    pub fn new(cache: Arc<LocalCatalogCache>) -> Self {
        Self { cache }
    }
}

#[async_trait]
impl SchemaProvider for YuntunSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        // trait 是同步的：内部用 try_read（缓存从不长锁，不可能失败；
        // 万一被阻塞返回空列表 —— 绝不 panic 绝不网络调用）
        self.cache
            .tables
            .try_read()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        match self.cache.get(name).await {
            Some(t) => Ok(Some(Arc::new(YuntunTableProvider::new(t)))),
            None => Ok(None),
        }
    }

    async fn table_type(&self, _name: &str) -> Result<Option<TableType>, DataFusionError> {
        Ok(Some(BASE_TABLE))
    }

    fn register_table(
        &self,
        _name: String,
        _table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        // Lakehouse 表由 Catalog 管理，不允许通过 DataFusion 注册
        Err(DataFusionError::NotImplemented(
            "register_table is managed by the yuntun catalog".into(),
        ))
    }

    fn deregister_table(
        &self,
        _name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Err(DataFusionError::NotImplemented(
            "deregister_table is managed by the yuntun catalog".into(),
        ))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.cache
            .tables
            .try_read()
            .map(|m| m.contains_key(name))
            .unwrap_or(false)
    }
}

/// catalog 层：单 schema（public）。
#[derive(Debug)]
pub struct YuntunCatalogProvider {
    schema: Arc<YuntunSchemaProvider>,
}

impl YuntunCatalogProvider {
    pub fn new(cache: Arc<LocalCatalogCache>) -> Self {
        Self {
            schema: Arc::new(YuntunSchemaProvider::new(cache)),
        }
    }
}

impl CatalogProvider for YuntunCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        vec![crate::SCHEMA_NAME.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == crate::SCHEMA_NAME {
            Some(self.schema.clone())
        } else {
            None
        }
    }

    fn register_schema(
        &self,
        _name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>, DataFusionError> {
        Err(DataFusionError::NotImplemented(
            "register_schema is fixed to 'public'".into(),
        ))
    }

    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>, DataFusionError> {
        Err(DataFusionError::NotImplemented(
            "deregister_schema is fixed to 'public'".into(),
        ))
    }
}
