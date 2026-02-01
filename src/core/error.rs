use thiserror::Error;

#[derive(Error, Debug)]
pub enum YuntunError {
    #[error("Catalog error: {0}")]
    Catalog(anyhow::Error),
    
    #[error("Store error: {0}")]
    Store(anyhow::Error),
    
    #[error("Ingest error: {0}")]
    Ingest(anyhow::Error),
    
    #[error("Query error: {0}")]
    Query(anyhow::Error),
    
    #[error("HTTP error: {0}")]
    Http(anyhow::Error),
    
    #[error("Arrow error: {0}")]
    Arrow(arrow::error::ArrowError),
    
    #[error("DataFusion error: {0}")]
    DataFusion(datafusion::common::DataFusionError),
}

// 为各种错误类型实现From trait
impl From<anyhow::Error> for YuntunError {
    fn from(err: anyhow::Error) -> Self {
        YuntunError::Catalog(err)
    }
}

impl From<arrow::error::ArrowError> for YuntunError {
    fn from(err: arrow::error::ArrowError) -> Self {
        YuntunError::Arrow(err)
    }
}

impl From<datafusion::common::DataFusionError> for YuntunError {
    fn from(err: datafusion::common::DataFusionError) -> Self {
        YuntunError::DataFusion(err)
    }
}
