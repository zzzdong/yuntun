//! DataFusion CatalogProvider / SchemaProvider（详细设计 §6.7；S2-4 不可变快照）。
//!
//! 读路径全部走本地快照（C7），同步 + 异步混合 trait 下**零网络调用**。
//!
//! **关键点（S2-4）**：provider 持有的是 `Arc<CatalogSnapshot>` —— 一次查询（= 一个
//! `SessionContext`）在规划期无论调用多少次 `schema()`/`table()`，看到的都是**同一个
//! Catalog 版本**；`Arc` 共享也省掉了每次 `table()` 深拷贝文件清单的开销。

use crate::cache::CatalogSnapshot;
use crate::table::YuntunTableProvider;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::TableType;
use std::sync::Arc;
use yuntun_store::ShardReader;

/// 表类型常量（SchemaProvider::table_type 用）。
const BASE_TABLE: TableType = TableType::Base;

/// schema 层：表发现（**单个 schema** 的视图，多 schema 时按 namespace 实例化）。
#[derive(Debug)]
pub struct YuntunSchemaProvider {
    snapshot: Arc<CatalogSnapshot>,
    /// 本 provider 对应的 schema（MySQL 的 database 概念）
    namespace: String,
    /// 热数据读侧（与快照同源注入；`None` = 未接线，退化为纯磁盘分片）
    hot: Option<Arc<dyn ShardReader>>,
}

impl YuntunSchemaProvider {
    pub fn new(
        snapshot: Arc<CatalogSnapshot>,
        namespace: impl Into<String>,
        hot: Option<Arc<dyn ShardReader>>,
    ) -> Self {
        Self {
            snapshot,
            namespace: namespace.into(),
            hot,
        }
    }
}

#[async_trait]
impl SchemaProvider for YuntunSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        // 同步 + 不可变快照：无锁、无网络、不 panic
        self.snapshot.table_names_in(&self.namespace)
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        match self.snapshot.get_in(&self.namespace, name) {
            Some(t) => Ok(Some(Arc::new(YuntunTableProvider::new(
                self.snapshot.clone(),
                t.meta.qualified_name(),
                t.schema.clone(),
                self.hot.clone(),
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
        self.snapshot.get_in(&self.namespace, name).is_some()
    }
}

/// catalog 层：**多 schema**（MySQL 的 database 概念）。
///
/// schema 清单来自快照（S2-4：与表清单同版本），每个 schema 实例化一个
/// [`YuntunSchemaProvider`] 视图，共享同一份不可变快照。
#[derive(Debug)]
pub struct YuntunCatalogProvider {
    snapshot: Arc<CatalogSnapshot>,
    hot: Option<Arc<dyn ShardReader>>,
}

impl YuntunCatalogProvider {
    pub fn new(snapshot: Arc<CatalogSnapshot>, hot: Option<Arc<dyn ShardReader>>) -> Self {
        Self { snapshot, hot }
    }
}

impl CatalogProvider for YuntunCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.snapshot.schemas.clone()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if self.snapshot.schemas.iter().any(|s| s == name) {
            Some(Arc::new(YuntunSchemaProvider::new(
                self.snapshot.clone(),
                name.to_string(),
                self.hot.clone(),
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
