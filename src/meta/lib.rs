//! Meta service module
//! 
//! This module provides centralized metadata management for the system,
//! including database, table, storage, and other metadata types.

pub mod service;
pub mod types;
pub mod storage;
pub mod catalog_adapter;

pub use service::MetaService;
pub use types::{DatabaseMeta, TableMeta, StorageMeta};
pub use catalog_adapter::MetaSchemaProvider;
