//! 对象存储抽象（详细设计 §2：S3 / 本地 / Mock）。
//!
//! 统一封装 `object_store`，测试用 `memory`（阶段 0.5 Mock S3），
//! 开发用 `local`（本地文件系统），生产用 `s3`（MinIO 兼容）。

use yuntun_model::error::LakeError;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use std::sync::Arc;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StoreConfig {
    /// 本地文件系统（开发）
    Local { root: String },
    /// 内存存储（测试 / Mock S3，阶段 0.5 压测）
    Memory,
    /// S3 / MinIO
    S3 {
        bucket: String,
        endpoint: String,
        access_key_id: String,
        secret_access_key: String,
        /// 兼容 MinIO 等 S3 兼容存储
        allow_http: bool,
    },
}

/// 按配置构造 object_store 实例。
pub fn create_store(cfg: &StoreConfig) -> Result<Arc<dyn ObjectStore>, LakeError> {
    Ok(match cfg {
        StoreConfig::Local { root } => {
            std::fs::create_dir_all(root)?;
            let fs = object_store::local::LocalFileSystem::new_with_prefix(root)
                .map_err(|e| LakeError::S3(e.to_string()))?;
            Arc::new(fs)
        }
        StoreConfig::Memory => Arc::new(object_store::memory::InMemory::new()),
        StoreConfig::S3 {
            bucket,
            endpoint,
            access_key_id,
            secret_access_key,
            allow_http,
        } => {
            let s3 = object_store::aws::AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_endpoint(endpoint)
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key)
                .with_allow_http(*allow_http)
                .with_virtual_hosted_style_request(false)
                .build()
                .map_err(|e| LakeError::S3(e.to_string()))?;
            Arc::new(s3)
        }
    })
}

/// 便捷写入：put 字节到路径，返回 ETag。
pub async fn put_bytes(
    store: &dyn ObjectStore,
    path: &str,
    bytes: Vec<u8>,
) -> Result<(), LakeError> {
    use object_store::path::Path as OsPath;
    let p = OsPath::from(path);
    store
        .put(&p, PutPayload::from_bytes(bytes.into()))
        .await
        .map_err(|e| LakeError::S3(e.to_string()))?;
    Ok(())
}

/// 便捷读取：get 全量字节。
pub async fn get_bytes(store: &dyn ObjectStore, path: &str) -> Result<bytes::Bytes, LakeError> {
    use object_store::path::Path as OsPath;
    let p = OsPath::from(path);
    let res = store
        .get(&p)
        .await
        .map_err(|e| LakeError::S3(e.to_string()))?;
    let buf = res
        .bytes()
        .await
        .map_err(|e| LakeError::S3(e.to_string()))?;
    Ok(buf)
}

/// 便捷删除。
pub async fn delete(store: &dyn ObjectStore, path: &str) -> Result<(), LakeError> {
    use object_store::path::Path as OsPath;
    store
        .delete(&OsPath::from(path))
        .await
        .map_err(|e| LakeError::S3(e.to_string()))?;
    Ok(())
}

/// 列举前缀下全部对象（孤儿清理用）。
pub async fn list_all(store: &dyn ObjectStore, prefix: &str) -> Result<Vec<ObjectSummary>, LakeError> {
    use futures::TryStreamExt;
    use object_store::path::Path as OsPath;
    let p = OsPath::from(prefix);
    let mut out = Vec::new();
    let mut stream = store.list(Some(&p));
    while let Some(meta) = stream
        .try_next()
        .await
        .map_err(|e| LakeError::S3(e.to_string()))?
    {
        out.push(ObjectSummary {
            path: meta.location.to_string(),
            size: meta.size as u64,
            last_modified: meta.last_modified.timestamp_millis().max(0) as u64,
        });
    }
    Ok(out)
}

/// 对象摘要（孤儿清理用）。
#[derive(Debug, Clone)]
pub struct ObjectSummary {
    pub path: String,
    pub size: u64,
    /// Unix 毫秒
    pub last_modified: u64,
}
