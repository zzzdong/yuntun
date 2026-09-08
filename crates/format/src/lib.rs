//! 文件格式封装：Vortex 主 / Parquet 回退（ADR-1 FormatSwitch）。
//!
//! **v11 工程决策**：Vortex 依赖必须锁定 Git Commit（ADR-1），但其 API 仍在演进。
//! 本 crate 把 Vortex 放在 `vortex` feature flag 后面（启用时由 workspace 锁定 commit），
//! 默认使用 **Parquet 回退路径**，保证阶段 0 全链路可用；
//! Phase 0.5 压测通过后再锁定 Vortex commit 并打开 feature。
//!
//! 文件路径布局（对齐架构 §11.3 回执示例）：
//! ```text
//! yuntun/{table}/dt={time_window}/shard={shard}/{batch_id}.{ext}
//! ```

use futures::TryStreamExt;
use object_store::path::Path as OsPath;
use object_store::{ObjectStoreExt, PutPayload};
use std::sync::Arc;
use yuntun_model::error::LakeError;

/// 存储格式（ADR-1 / §5.5 FormatSwitch）
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataFormat {
    /// 主格式（feature flag 后面，未启用时写入报错）
    Vortex,
    /// 回退格式（默认可用）
    Parquet,
}

impl DataFormat {
    pub fn ext(&self) -> &'static str {
        match self {
            DataFormat::Vortex => "vortex",
            DataFormat::Parquet => "parquet",
        }
    }

    /// 从配置字符串解析（未识别值回退 Parquet，ADR-1 回退语义）。
    pub fn parse(s: &str) -> Self {
        match s {
            "vortex" => DataFormat::Vortex,
            _ => DataFormat::Parquet,
        }
    }
}

/// 构造数据文件逻辑路径（不含 store 前缀）。
pub fn file_path(
    table: &str,
    shard: &str,
    time_window: &str,
    batch_id: &str,
    fmt: DataFormat,
) -> String {
    format!(
        "yuntun/{table}/dt={time_window}/shard={shard}/{batch_id}.{ext}",
        ext = fmt.ext()
    )
}

/// 从路径提取 batch_id（孤儿清理用，§12.2.1）。
pub fn extract_batch_id(path: &str) -> Option<String> {
    let name = path.rsplit('/').next()?;
    let stem = name.rsplit_once('.')?.0.to_string();
    // MVP：batch_id 即文件名 stem（UUID 或任意命名），统一返回
    Some(stem)
}

/// 将 RecordBatch 编码并写入对象存储，返回 (路径, 字节数, 行数)。
pub async fn write_batch(
    store: &Arc<dyn object_store::ObjectStore>,
    table: &str,
    shard: &str,
    time_window: &str,
    batch_id: &str,
    batch: &arrow::record_batch::RecordBatch,
    fmt: DataFormat,
) -> Result<(String, u64, u64), LakeError> {
    let path = file_path(table, shard, time_window, batch_id, fmt);
    let bytes = encode_batch(batch, fmt)?;
    let size = bytes.len() as u64;
    store
        .put(
            &OsPath::from(path.as_str()),
            PutPayload::from_bytes(bytes.into()),
        )
        .await
        .map_err(|e| LakeError::S3(e.to_string()))?;
    Ok((path, size, batch.num_rows() as u64))
}

/// 读取数据文件为 RecordBatch 列表。
pub async fn read_batch(
    store: &Arc<dyn object_store::ObjectStore>,
    path: &str,
    fmt: DataFormat,
) -> Result<Vec<arrow::record_batch::RecordBatch>, LakeError> {
    let res = store
        .get(&OsPath::from(path))
        .await
        .map_err(|e| LakeError::S3(e.to_string()))?;
    let bytes = res
        .bytes()
        .await
        .map_err(|e| LakeError::S3(e.to_string()))?;
    decode_batch(&bytes, fmt)
}

/// 编码 RecordBatch。
pub fn encode_batch(
    batch: &arrow::record_batch::RecordBatch,
    fmt: DataFormat,
) -> Result<Vec<u8>, LakeError> {
    match fmt {
        DataFormat::Parquet => encode_parquet(batch),
        DataFormat::Vortex => encode_vortex(batch),
    }
}

/// 解码数据文件。
pub fn decode_batch(
    bytes: &[u8],
    fmt: DataFormat,
) -> Result<Vec<arrow::record_batch::RecordBatch>, LakeError> {
    match fmt {
        DataFormat::Parquet => decode_parquet(bytes),
        DataFormat::Vortex => decode_vortex(bytes),
    }
}

// ---------------- Parquet（回退路径，默认可用）----------------

fn encode_parquet(batch: &arrow::record_batch::RecordBatch) -> Result<Vec<u8>, LakeError> {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::default(),
        ))
        .build();
    let mut buf = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
        .map_err(|e| LakeError::Other(format!("parquet writer: {e}")))?;
    writer
        .write(batch)
        .map_err(|e| LakeError::Other(format!("parquet write: {e}")))?;
    writer
        .close()
        .map_err(|e| LakeError::Other(format!("parquet close: {e}")))?;
    Ok(buf)
}

fn decode_parquet(bytes: &[u8]) -> Result<Vec<arrow::record_batch::RecordBatch>, LakeError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes.to_vec()))
        .map_err(|e| LakeError::Other(format!("parquet reader: {e}")))?
        .build()
        .map_err(|e| LakeError::Other(format!("parquet reader build: {e}")))?;
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| LakeError::Other(format!("parquet read: {e}")))?;
    Ok(batches)
}

// ---------------- Vortex（feature flag 后面，ADR-1）----------------

fn encode_vortex(_batch: &arrow::record_batch::RecordBatch) -> Result<Vec<u8>, LakeError> {
    // ADR-1: Vortex 依赖锁定 Git Commit 后启用。
    // 启用步骤：workspace Cargo.toml 添加
    //   vortex-datafusion = { git = "https://github.com/vortex-data/vortex.git", rev = "<锁定 commit>" }
    // 并在此调用 vortex 的 ArrayData encode API。
    Err(LakeError::Other(
        "vortex format not enabled: build with --features vortex and pin a git commit (ADR-1)"
            .into(),
    ))
}

fn decode_vortex(_bytes: &[u8]) -> Result<Vec<arrow::record_batch::RecordBatch>, LakeError> {
    Err(LakeError::Other(
        "vortex format not enabled: build with --features vortex and pin a git commit (ADR-1)"
            .into(),
    ))
}

/// 列举 prefix 下所有对象（孤儿清理用，转发 store）。
pub async fn list_objects(
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> Result<Vec<(String, u64)>, LakeError> {
    let mut out = Vec::new();
    let mut stream = store.list(Some(&OsPath::from(prefix)));
    while let Some(meta) = stream
        .try_next()
        .await
        .map_err(|e| LakeError::S3(e.to_string()))?
    {
        out.push((meta.location.to_string(), meta.size));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as SArc;

    fn sample_batch() -> arrow::record_batch::RecordBatch {
        let schema = Schema::new(vec![
            Field::new("event_time", DataType::Int64, false),
            Field::new("user", DataType::Utf8, true),
        ]);
        arrow::record_batch::RecordBatch::try_new(
            SArc::new(schema),
            vec![
                SArc::new(Int64Array::from(vec![1, 2, 3])),
                SArc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn parquet_roundtrip_via_memory_store() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let batch = sample_batch();
        let (path, size, rows) = write_batch(
            &store,
            "tbl",
            "s0",
            "2026-08-31T14:00",
            "018f0000-0000-7000-8000-000000000000",
            &batch,
            DataFormat::Parquet,
        )
        .await
        .unwrap();
        assert_eq!(rows, 3);
        assert!(size > 0);
        assert_eq!(
            extract_batch_id(&path).unwrap(),
            "018f0000-0000-7000-8000-000000000000"
        );
        assert!(path.starts_with("yuntun/tbl/dt=2026-08-31T14:00/shard=s0/"));
        assert!(path.ends_with(".parquet"));

        let batches = read_batch(&store, &path, DataFormat::Parquet)
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
        // 列名保留
        assert_eq!(batches[0].schema().field(1).name(), "user");
    }

    #[tokio::test]
    async fn vortex_disabled_falls_back_to_error() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let batch = sample_batch();
        let res = write_batch(
            &store,
            "tbl",
            "s0",
            "w",
            "018f0000-0000-7000-8000-000000000000",
            &batch,
            DataFormat::Vortex,
        )
        .await;
        assert!(res.is_err(), "vortex 未启用时应显式报错（ADR-1）");
    }
}
