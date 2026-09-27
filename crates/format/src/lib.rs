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

/// **删除向量（DV）对象的 GC 口径**：它锚定哪个数据文件（⇒ 用哪个 batch_id 保护它）。
///
/// DV 的路径是 `…/shard=…/dv/<数据文件名含扩展名>/<dv_id>.bin`（`delta-dml-design §3.1`）。
///
/// ⚠️ **最后一段是 `dv_id`，不是 `batch_id`** —— 若照 [`extract_batch_id`] 的口径对账，
/// 每一份 DV 都会被判成孤儿（`dv_id` 永远不在 `known_batch_ids` 里），静置期一过就被删掉，
/// 而它标记的那些行会**复活**（`F.3d` 差点踩上：`§150`）。
/// 所以 DV 的保护判据只能取自它**锚定**的那一段（数据文件名）。
pub fn dv_anchor_batch_id(path: &str) -> Option<String> {
    let (_, after) = path.split_once("/dv/")?;
    let file_name = after.split('/').next()?;
    if file_name.is_empty() || file_name == after {
        // `dv/` 后面必须紧跟一层"数据文件名"目录；否则这不是 DV 路径（保守返回 None）
        return None;
    }
    Some(
        file_name
            .rsplit_once('.')
            .map(|(stem, _)| stem.to_string())
            .unwrap_or_else(|| file_name.to_string()),
    )
}

/// 数据文件路径 → **索引文件路径**（`plan.md` F.4）：`…/x.parquet` → `…/x.idx`。
///
/// 命名刻意与数据文件**同 stem**（只换扩展名），于是：
///
/// * `extract_batch_id` 对两者给出**同一个** batch_id ⇒ 孤儿清理/GC 把它们当同一批
///   一起保护、一起回收（索引是**额外对象**，口径不同就会留垃圾或删掉活索引，`plan.md` F.6）；
/// * 不需要在目录里再加一个"索引在哪"的映射（`FileManifest.index_path` 仍然记着，
///   那是给读侧用的，不承担 GC 口径）。
pub fn index_path(data_path: &str) -> String {
    match data_path.rsplit_once('.') {
        Some((stem, _ext)) => format!("{stem}.idx"),
        None => format!("{data_path}.idx"),
    }
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
///
/// ⚠️ **这个值不在这里定义**：行组级索引（`plan.md` F.4）按**行组序号**说话，
/// 必须与它逐行对齐 ⇒ 唯一真相在 [`yuntun_model::index::INDEX_GROUP_ROWS`]
/// （那儿同时记着"为什么必须对齐"）。这里只保留别名，免得两个常量各自漂移。
pub use yuntun_model::index::INDEX_GROUP_ROWS as MAX_ROWS_PER_ROW_GROUP;

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

// ---------------- Vortex（feature flag 后面，ADR-1 / `§133`–`§139`）----------------
//
// # 为什么现在才写得出来
//
// `§130` 记过"Vortex 接不进来"：那时 `vortex 0.84` 用 **arrow 58**，与 datafusion 55 钉死的
// **arrow 59** 不同代。`§133` 查明真相：**MSRV 1.94 把 cargo 逼退到 0.84**（0.86 要 rustc 1.95），
// 而 **`vortex 0.86` 用 arrow-array ^59.2 —— 同代**。抬到 1.95 之后这条路才第一次真的通。
//
// # 四个"形状"上的坑（`§134`–`§138` 各踩过一遍，别再踩）
//
// 1. **session 必须有 runtime**：`array_session().with::<RuntimeSession>().with_handle(handle)`，
//    `handle` 由 **`single::block_on` 直接递给闭包**（`FnOnce(Handle) -> Fut`）——
//    别用 `Handle::find()`（裸线程里拿不到）；
// 2. **edition 要先注册、再启用**：`register_default_editions(&session)` →
//    `enable_default_editions(&session)`；前者填"编码白名单"（`§137` 的
//    `… not permitted by ctx` 就是白名单为空），后者选版本（`§138` 的
//    `cannot enable unregistered edition` 就是没先注册）；
// 3. **数组与写入必须同源**：一律走 `session.arrow()`（`ArrowSessionExt`），
//    别用 `ArrowSession::default()`；
// 4. **`async move` 会搬走 `buf`** ⇒ 缓冲区在异步块内造、作为结果返回。

#[cfg(feature = "vortex")]
fn encode_vortex(batch: &arrow::record_batch::RecordBatch) -> Result<Vec<u8>, LakeError> {
    use vortex::array::array_session;
    use vortex::array::iter::{ArrayIteratorAdapter, ArrayIteratorExt};
    use vortex::arrow::ArrowSessionExt;
    use vortex::editions::{enable_default_editions, register_default_editions};
    use vortex::file::VortexWriteOptions;
    use vortex::io::runtime::single::block_on;
    use vortex::io::session::{RuntimeSession, RuntimeSessionExt};

    let schema = batch.schema();
    let b = batch.clone();
    block_on(|handle| async move {
        let session = array_session().with::<RuntimeSession>().with_handle(handle);
        register_default_editions(&session);
        enable_default_editions(&session);
        // 数组由**这个** session 的 arrow 入口造（坑 3）
        let array = session
            .arrow()
            .from_arrow_record_batch(b, schema.as_ref())
            .map_err(|e| LakeError::Other(format!("vortex 编码（RecordBatch → Array）：{e}")))?;
        let dtype = array.dtype().clone();
        let stream =
            ArrayIteratorAdapter::new(dtype, std::iter::once(Ok(array))).into_array_stream();
        // 缓冲区在块内造（坑 4）、作为结果返回
        let mut buf: Vec<u8> = Vec::new();
        VortexWriteOptions::new(session)
            .write(&mut buf, stream)
            .await
            .map_err(|e| LakeError::Other(format!("vortex 写入：{e}")))?;
        Ok::<Vec<u8>, LakeError>(buf)
    })
}

#[cfg(feature = "vortex")]
fn decode_vortex(bytes: &[u8]) -> Result<Vec<arrow::record_batch::RecordBatch>, LakeError> {
    use arrow::array::StructArray;
    use futures::TryStreamExt;
    use vortex::array::{array_session, VortexSessionExecute};
    use vortex::arrow::ArrowSessionExt;
    use vortex::buffer::ByteBuffer;
    use vortex::editions::{enable_default_editions, register_default_editions};
    use vortex::file::OpenOptionsSessionExt;
    use vortex::io::runtime::single::block_on;
    use vortex::io::session::{RuntimeSession, RuntimeSessionExt};

    let bytes = bytes.to_vec();
    block_on(|handle| async move {
        let session = array_session().with::<RuntimeSession>().with_handle(handle);
        register_default_editions(&session);
        enable_default_editions(&session);
        // 内存字节 → `ByteBuffer` ⇒ 不必落盘
        let file = OpenOptionsSessionExt::open_options(&session)
            .open_buffer(ByteBuffer::from(bytes))
            .map_err(|e| LakeError::Other(format!("vortex 打开：{e}")))?;

        let arrays: Vec<_> = file
            .scan()
            .map_err(|e| LakeError::Other(format!("vortex 扫描：{e}")))?
            .into_array_stream()
            .map_err(|e| LakeError::Other(format!("vortex 数据流：{e}")))?
            .try_collect()
            .await
            .map_err(|e| LakeError::Other(format!("vortex 读取：{e}")))?;

        let mut out = Vec::with_capacity(arrays.len());
        for a in arrays {
            // 同样走**这个** session 的 arrow 入口（坑 3）；`target = None` ⇒ 由数据决定类型
            let mut ctx = session.create_execution_ctx();
            let arrow_array = session
                .arrow()
                .execute_arrow(a, None, &mut ctx)
                .map_err(|e| LakeError::Other(format!("vortex → arrow：{e}")))?;
            let st = arrow_array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| LakeError::Other("vortex 读出的顶层不是 StructArray".into()))?;
            out.push(arrow::record_batch::RecordBatch::from(st.clone()));
        }
        Ok::<_, LakeError>(out)
    })
}

#[cfg(not(feature = "vortex"))]
fn encode_vortex(_batch: &arrow::record_batch::RecordBatch) -> Result<Vec<u8>, LakeError> {
    // `§133` 起依赖**不再挡路**（`vortex 0.86` 与 arrow 59 同代），只是默认不编进去：
    // `cargo build -p yuntun-format --features vortex` 即拿到上面的真实现。
    Err(LakeError::Other(
        "vortex format not enabled: build with --features vortex (§133)".into(),
    ))
}

#[cfg(not(feature = "vortex"))]
fn decode_vortex(_bytes: &[u8]) -> Result<Vec<arrow::record_batch::RecordBatch>, LakeError> {
    Err(LakeError::Other(
        "vortex format not enabled: build with --features vortex (§133)".into(),
    ))
}

#[cfg(all(test, feature = "vortex"))]
mod vortex_tests {
    use super::*;
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(rows: usize) -> arrow::record_batch::RecordBatch {
        let users: Vec<String> = (0..rows).map(|i| format!("u{}", i % 37)).collect();
        arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("ts", DataType::Int64, true),
                Field::new("amount", DataType::Float64, true),
                Field::new("user", DataType::Utf8, true),
                Field::new("ok", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())),
                Arc::new(Float64Array::from(
                    (0..rows).map(|i| i as f64 * 1.5).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(users)),
                Arc::new(BooleanArray::from(
                    (0..rows).map(|i| i % 3 == 0).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    /// **Vortex 真的能往返**（写进去 → 读出来 → 逐列相同）。
    ///
    /// 编译器说"能过"不等于"读得回来"：`§135` 第一版编译过了却在 vortex 内部 panic，
    /// `§136`/`§138` 又各差一格。这条用例守的正是那一层。
    #[test]
    fn vortex_round_trips_a_batch_column_by_column() {
        let b = batch(1_000);
        let bytes = encode_batch(&b, DataFormat::Vortex).expect("vortex 编码应当成功");
        assert!(!bytes.is_empty(), "编码结果不该是空的");
        let out = decode_batch(&bytes, DataFormat::Vortex).expect("vortex 解码应当成功");
        assert!(!out.is_empty(), "解码结果不该是空的");
        // ⚠️ `RecordBatch::schema()` 返回的是一个**临时** `Arc<Schema>` ⇒ 先接住，
        // 否则下面 `field(i).data_type()` 借的是已释放的临时值。
        let want_schema = b.schema();
        // ⚠️ **vortex 挑的是它自己最便宜的物理类型**，不是我们写进去时的那套：
        // 这里字符串列读回来是 **`Utf8View`**（原 schema 是 `Utf8`）⇒ 直接拿原 schema 去
        // `concat_batches` 会报 `expected Utf8 but found Utf8View`。
        // 所以：**按各块自己的 schema 拼**，再**按原类型 cast 回去**逐列比
        //（值的判定不受物理类型影响）。
        let got = concat_batches(&out[0].schema(), &out).expect("把读到的块拼回来");
        assert_eq!(got.num_rows(), 1_000, "行数必须回来");
        assert_eq!(got.num_columns(), b.num_columns(), "列数必须回来");
        for i in 0..b.num_columns() {
            let want_ty = want_schema.field(i).data_type();
            let got_col = arrow::compute::cast(got.column(i), want_ty)
                .unwrap_or_else(|e| panic!("第 {i} 列 cast 到 {want_ty} 失败：{e}"));
            assert_eq!(
                b.column(i).as_ref(),
                got_col.as_ref(),
                "第 {i} 列（`{}`）必须**逐值**相同 —— 往返丢数据是最坏的失败形态",
                want_schema.field(i).name()
            );
        }
    }

    /// **体积对照**（`T9.x` 的第三件）：**两种数据形状**，各把两个数字摆出来。
    ///
    /// ⚠️ **不断言"谁更小"**：压缩率取决于数据形状 —— 下面两份数据就是反例：
    /// `ts` 递增时 Parquet 的 delta 编码占尽便宜；换成**高基数随机串**，结论会往回走。
    /// 断言一个方向只会造出"换数据就红"的用例 ⇒ 这里只断言**两条路都可用**。
    ///
    /// 另一件要记的：vortex **默认就开着压缩**（`vortex-file/src/writer.rs` 里
    /// `BtrBlocksCompressorBuilder::default()` 是缺省策略）⇒ 下面这两个数字**都是压缩后**的。
    #[test]
    fn sizes_are_reported_for_two_data_shapes() {
        for (label, b) in [
            ("递增整数（偏袒 Parquet 的 delta）", batch(1_000)),
            ("高基数随机串（偏袒字典/FSST）", random_strings_batch(1_000)),
        ] {
            let v = encode_batch(&b, DataFormat::Vortex).expect("vortex 编码").len();
            let p = encode_batch(&b, DataFormat::Parquet).expect("parquet 编码").len();
            println!("{label}: vortex={v}B parquet={p}B ratio={:.2}", v as f64 / p as f64);
            assert!(v > 0 && p > 0, "两种格式都得真写出东西：vortex={v} parquet={p}");
            // 两种形状下都要**读得回来**（体积之外，可用性同样要看）
            let back = decode_batch(
                &encode_batch(&b, DataFormat::Vortex).expect("vortex 编码"),
                DataFormat::Vortex,
            )
            .expect("vortex 解码");
            assert!(!back.is_empty(), "{label}: vortex 解码结果不该为空");
        }
    }

    /// 高基数随机串（模拟 UUID / 事件 id）—— 与递增整数正好是两种极端形状。
    fn random_strings_batch(rows: usize) -> arrow::record_batch::RecordBatch {
        // 用**确定性**的伪随机（LCG）：用例必须可复现，不能靠 `rand`
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            state >> 33
        };
        let ids: Vec<String> = (0..rows)
            .map(|_| format!("id-{:016x}-{:016x}", next(), next()))
            .collect();
        arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("ts", DataType::Int64, true),
                Field::new("id", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())),
                Arc::new(StringArray::from(ids)),
            ],
        )
        .unwrap()
    }
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

    /// **DV 的 GC 对账键是它锚定的数据文件**，不是 `dv_id`。
    ///
    /// 这条如果说错了，孤儿 GC 会把**活着的**删除向量当垃圾删掉（`dv_id` 永远不在
    /// `known_batch_ids` 里）—— 那些行随之复活。
    #[test]
    fn dv_paths_are_anchored_to_their_data_file() {
        let dv = "yuntun/public/t/dt=w/shard=s0/dv/b7.parquet/dv-1.bin";
        assert_eq!(dv_anchor_batch_id(dv).as_deref(), Some("b7"), "锚定数据文件的 batch_id");
        assert_eq!(
            extract_batch_id(dv).as_deref(),
            Some("dv-1"),
            "`extract_batch_id` 拿到的是 dv_id —— 所以它**不能**用来给 DV 对账"
        );
        // 非 DV 路径：不冒充锚定
        assert_eq!(dv_anchor_batch_id("yuntun/public/t/dt=w/shard=s0/b7.parquet"), None);
        assert_eq!(dv_anchor_batch_id("yuntun/public/t/dt=w/shard=s0/b7.idx"), None);
        // 形状不对（`dv/` 后面没有数据文件名那一层）⇒ 保守返回 None
        assert_eq!(dv_anchor_batch_id("yuntun/public/t/dv/"), None);
        assert_eq!(dv_anchor_batch_id("yuntun/public/t/dv/x"), None);
    }
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
