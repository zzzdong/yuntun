//! Flight 钩子实现：DoPut → Ingestor，DoGet → QueryEngine。

use async_trait::async_trait;
use std::sync::Arc;
use yuntun_ingest::source::{IngestBatch, Receipt};
use yuntun_ingest::Ingestor;
use yuntun_model::error::LakeError;

/// 把 Flight 服务与 Ingestor 连接起来。
pub struct IngestorHook {
    ingestor: Arc<Ingestor>,
}

impl IngestorHook {
    pub fn new(ingestor: Arc<Ingestor>) -> Self {
        Self { ingestor }
    }
}

#[async_trait]
impl yuntun_ingest::flight::FlightIngestHook for IngestorHook {
    async fn ingest(&self, batch: IngestBatch) -> Result<Receipt, LakeError> {
        self.ingestor.ingest(batch).await
    }
}

/// 把 Flight do_get 与 QueryEngine 连接起来（只读 SQL）。
pub struct QueryHook {
    engine: Arc<yuntun_query::QueryEngine>,
}

impl QueryHook {
    pub fn new(engine: Arc<yuntun_query::QueryEngine>) -> Self {
        Self { engine }
    }
}

#[async_trait]
impl yuntun_ingest::flight::FlightQueryHook for QueryHook {
    async fn query(
        &self,
        sql: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, LakeError> {
        self.engine
            .sql(sql)
            .await
            .map_err(|e| LakeError::Other(format!("query: {e}")))
    }
}
