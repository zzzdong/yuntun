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

/// schema 层：表发现（**单个 schema** 的视图，多 schema 时按 namespace 实例化）。
#[derive(Debug)]
pub struct YuntunSchemaProvider {
    cache: Arc<LocalCatalogCache>,
    /// 本 provider 对应的 schema（MySQL 的 database 概念）
    namespace: String,
}

impl YuntunSchemaProvider {
    pub fn new(cache: Arc<LocalCatalogCache>, namespace: impl Into<String>) -> Self {
        Self {
            cache,
            namespace: namespace.into(),
        }
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
            .map(|m| {
                m.values()
                    .filter(|t| t.meta.schema_name() == self.namespace)
                    .map(|t| t.meta.name.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        match self.cache.get_in(&self.namespace, name).await {
            // 传入热数据读侧：scan 时把内存分片（尚未落盘的热数据）并进文件组（读己之写）
            Some(t) => Ok(Some(Arc::new(YuntunTableProvider::new(
                t,
                self.cache.hot_shards(),
            )))),
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
        let ident = yuntun_model::ops::qualified_name(&self.namespace, name);
        self.cache
            .tables
            .try_read()
            .map(|m| m.contains_key(&ident))
            .unwrap_or(false)
    }
}

/// catalog 层：**多 schema**（MySQL 的 database 概念）。
///
/// schema 清单来自本地缓存（刷新时同步 Catalog `list_schemas`），
/// 每个 schema 实例化一个 [`YuntunSchemaProvider`] 视图。
#[derive(Debug)]
pub struct YuntunCatalogProvider {
    cache: Arc<LocalCatalogCache>,
}

impl YuntunCatalogProvider {
    pub fn new(cache: Arc<LocalCatalogCache>) -> Self {
        Self { cache }
    }
}

impl CatalogProvider for YuntunCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.cache
            .schemas
            .try_read()
            .map(|v| v.clone())
            .unwrap_or_default()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if self.schema_names().iter().any(|s| s == name) {
            Some(Arc::new(YuntunSchemaProvider::new(
                self.cache.clone(),
                name.to_string(),
            )))
        } else {
            None
        }
    }

    fn register_schema(
        &self,
        _name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>, DataFusionError> {
        // schema 生命周期由 yuntun catalog 管理（CREATE/DROP DATABASE 走 SQL 层）
        Err(DataFusionError::NotImplemented(
            "register_schema is managed by the yuntun catalog".into(),
        ))
    }

    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>, DataFusionError> {
        Err(DataFusionError::NotImplemented(
            "deregister_schema is managed by the yuntun catalog".into(),
        ))
    }
}
