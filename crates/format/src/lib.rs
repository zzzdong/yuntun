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
///
/// 表标识为**全限定 `schema.table`**（[`yuntun_model::ops::qualified_name`]），
/// 路径按 schema 分层：`yuntun/{schema}/{table}/dt=.../shard=.../{batch_id}.{ext}`。
/// 裸表名（旧数据）等价于 `public.<table>` 的旧布局 `yuntun/{table}/...`，读取不受影响
/// （读取以 Manifest 中的 `file_path` 为准）。
pub fn file_path(
    table: &str,
    shard: &str,
    time_window: &str,
    batch_id: &str,
    fmt: DataFormat,
) -> String {
    let table = table.replace('.', "/");
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
    // 编码同样是纯 CPU（`§124`）。`RecordBatch::clone` 是**浅拷贝**（列缓冲是 `Arc`）⇒ 不复制数据。
    let owned = batch.clone();
    let bytes = cpu_off_thread("encode", move || encode_batch(&owned, fmt)).await?;
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
    // **解码是纯 CPU**（解压 + 列式→Arrow）⇒ 走阻塞池，别占住 async worker（见 [`cpu_off_thread`]）。
    cpu_off_thread("decode", move || decode_batch(&bytes, fmt)).await
}

/// 把**纯 CPU** 的活交给 tokio 的**阻塞池**（与 async worker 池分开的那一个），`await` 结果。
///
/// # 为什么需要它（`§124`，落地设计 §9.3「Compaction 资源隔离」）
///
/// 解码 / 编码 / 合并这类活是**同步 CPU**：直接在 async 上下文里跑会占住 worker 线程，
/// 把**同一个节点上**的 ingest 攒批与 query 响应一起拖住 —— 现象是"查询莫名变慢"，
/// 而且**没有任何报错**（`architecture.md §12.1` 正是为这件事要求"独立 tokio blocking pool"）。
///
/// # 为什么这里用 `spawn_blocking` 是安全的
///
/// 会用到它的入口都是 **`async fn`**（`read_batch` / `write_batch` / `compact_shard`）⇒
/// 调用方必然已经把 future 交给某个运行时在驱动 ⇒ 不会触发"没有运行时"的 panic。
///
/// # 观测
///
/// `YUNTUN_FORMAT_TRACE=1` 时逐次打印"交给谁 + 当前线程名" —— 排查"谁在占 worker"用的。
pub async fn cpu_off_thread<T, F>(what: &'static str, f: F) -> Result<T, LakeError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, LakeError> + Send + 'static,
{
    // 两处都打：**提交方**（async worker）与**执行方**（阻塞池）各是谁 —— 一眼看出"活搬走了没有"。
    if trace_on() {
        eprintln!(
            "[format] {what} → 阻塞池（提交方 {:?}）",
            std::thread::current().name()
        );
    }
    let f = move || {
        if trace_on() {
            eprintln!(
                "[format] {what}：实际执行于 {:?}",
                std::thread::current().name()
            );
        }
        f()
    };
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| LakeError::Other(format!("阻塞任务失败（{what}）：{e}")))?
}

/// 逐次 CPU 轨迹开关（`YUNTUN_FORMAT_TRACE=1`）。
fn trace_on() -> bool {
    std::env::var("YUNTUN_FORMAT_TRACE").is_ok()
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

/// **单个 RowGroup 的最大行数**（显式设定，`operation-log §36`）。
///
/// 之前不设 → 用 crate 默认（约 100 万行）→ 实测**整文件恰好 1 个 RowGroup**
/// （`operation-log §33.4`：5.5k~27k 行的文件都是 1 组）。后果有两个方向：
///
/// - **查询**：剪枝粒度退化为"文件"。文件小（当前 1KB 行约 2.7 万行 / 16MB）时无害，
///   但一旦 `bytes_threshold` 上调或换窄 schema 使文件变大，就变成"文件内无法跳读"；
/// - **写入**：一个 RowGroup 的所有列缓冲要同时驻留内存才能落盘 —— 组越大峰值越高
///   （实测 VmHWM 峰值中编码缓冲占相当一部分，`operation-log §33.2`）。
///
/// 取 **65,536 行**：1KB 行时约 64MB/组（在 chunk 预算可承受范围），
/// 且对当前文件规模**不改变行为**（27k 行 < 64k → 仍是 1 组），只作为
/// "文件变大时不要退化成单组" 的**显式上界**。改这个值必须同时给
/// （文件大小、扫描剪枝、写入峰值内存）三组数据。
const MAX_ROWS_PER_ROW_GROUP: usize = 65_536;

fn encode_parquet(batch: &arrow::record_batch::RecordBatch) -> Result<Vec<u8>, LakeError> {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::default(),
        ))
        .set_max_row_group_row_count(Some(MAX_ROWS_PER_ROW_GROUP))
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

    /// RowGroup 边界**显式钉住**：`max_row_group_size` 不设会退回 crate 默认（约 100 万行）
    /// → 大文件退化为"一个 RowGroup"，剪枝粒度=文件、写入峰值内存无上界。
    ///
    /// 用 Int64 单列（无字符串分配）写 20 万行，断言分组数 = ceil(200000 / 65536) = 4。
    #[test]
    fn parquet_row_groups_are_bounded_by_explicit_setting() {
        let schema = SArc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let rows = MAX_ROWS_PER_ROW_GROUP * 3 + 1234; // 跨 3 个完整组 + 一个尾巴
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![SArc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>()))],
        )
        .unwrap();

        let bytes = encode_parquet(&batch).unwrap();
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let reader =
            SerializedFileReader::new(bytes::Bytes::copy_from_slice(&bytes)).expect("parquet footer");
        let groups = reader.metadata().num_row_groups();
        let expected = rows.div_ceil(MAX_ROWS_PER_ROW_GROUP);
        assert_eq!(
            groups, expected,
            "RowGroup 数必须由 MAX_ROWS_PER_ROW_GROUP 决定（rows={rows}，期望 {expected} 组，实际 {groups}）"
        );
        // 且每个组不超过上限（防止实现被换成"整批一组"后测试仍绿）
        for i in 0..groups {
            let rg = reader.metadata().row_group(i);
            assert!(
                (rg.num_rows() as usize) <= MAX_ROWS_PER_ROW_GROUP,
                "第 {i} 组 {} 行超过上限 {MAX_ROWS_PER_ROW_GROUP}",
                rg.num_rows()
            );
        }
        // 解码回来行数不变（分组不影响语义）
        let back: usize = decode_parquet(&bytes)
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(back, rows);
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
        // 多 schema：全限定表标识按 schema 分层（旧裸名布局保持兼容）
        let layered = file_path("sales.orders", "s0", "w", "b1", DataFormat::Parquet);
        assert_eq!(layered, "yuntun/sales/orders/dt=w/shard=s0/b1.parquet");
        assert!(path.ends_with(".parquet"));
        // 与 store 层的"磁盘分片"目录前缀约定保持一致（两处定义不得漂移）
        let disk = yuntun_store::DiskShard::new(store.clone());
        assert_eq!(
            disk.prefix(&yuntun_store::ShardId::new("sales.orders", "s0", "w")),
            layered.rsplit_once('/').map(|(d, _)| format!("{d}/")).unwrap()
        );

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
