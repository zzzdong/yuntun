//! spill 通道（架构 §2.1 / §2.5 / §2.6）：**本地磁盘** + Arrow IPC(LZ4)。
//!
//! ## 为什么落盘用 Arrow IPC 而不是 Parquet（架构 §2.1）
//! spill 若用 Parquet：落盘 encode → flush 读回 decode → 写 S3 再 encode，编码付三次。
//! IPC 落盘 / 读回近乎零成本（不加字典、不做统计、不重排），flush 只付一次必需的编码。
//!
//! ## 文件布局（自描述，便于重启后复用）
//!
//! ```text
//! ┌──────────────── 64B 定长头 ────────────────┬──────────────┬─────────────┐
//! │ magic(4) ver(4) wal_seg(8) seq[2](16)      │ schema_ipc   │ payload     │
//! │ rows(8) mem_bytes(8) crc32(4) len(4)(8)    │ (IPC fb)     │ (IPC+LZ4)   │
//! └────────────────────────────────────────────┴──────────────┴─────────────┘
//! ```
//!
//! `crc32` 覆盖 payload：**校验失败 = 副本不可信 → 丢弃重来**（架构 §2.6）。
//! 权威始终是 WAL，因此丢弃副本不会丢数据，只会退化为重放重建。

use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::CompressionType;
use arrow::record_batch::RecordBatch;

use yuntun_model::error::LakeError;
use yuntun_model::meta::{deserialize_schema, serialize_schema};

use crate::chunk::SpillHandle;

/// spill 文件魔数。
pub const SPILL_MAGIC: [u8; 4] = *b"YSPL";
/// spill 文件格式版本。
pub const SPILL_VERSION: u32 = 1;
/// 定长头长度。
const HEADER_LEN: usize = 64;

/// spill 载体元信息（写入头）。
#[derive(Debug, Clone)]
pub struct SpillMeta {
    /// 写入时的 WAL segment 号
    pub wal_segment: u64,
    /// 覆盖的 WAL seq 半开区间（架构写作 `offset_range`，本仓库以 seq 为等价坐标）
    pub wal_seq_range: Range<u64>,
    pub rows: usize,
    /// 原始内存占用（账本口径）
    pub mem_bytes: usize,
}

/// 写 spill 文件（先写 `.tmp` 再 rename，避免半截文件被当成有效副本）。
pub fn write_spill(
    dir: &Path,
    name: &str,
    batches: &[RecordBatch],
    meta: SpillMeta,
) -> Result<SpillHandle, LakeError> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(format!("{name}.ipc.lz4"));
    let tmp = dir.join(format!("{name}.ipc.lz4.tmp"));

    let encoded = encode_batches(batches)?;
    let crc = crc32fast::hash(&encoded);

    let schema = batches
        .first()
        .map(|b| b.schema())
        .ok_or_else(|| LakeError::Other("spill: empty batch set has no schema".into()))?;
    let schema_ipc = serialize_schema(&schema);

    let mut buf = Vec::with_capacity(HEADER_LEN + schema_ipc.len() + encoded.len());
    buf.extend_from_slice(&SPILL_MAGIC);
    buf.extend_from_slice(&SPILL_VERSION.to_le_bytes());
    buf.extend_from_slice(&meta.wal_segment.to_le_bytes());
    buf.extend_from_slice(&meta.wal_seq_range.start.to_le_bytes());
    buf.extend_from_slice(&meta.wal_seq_range.end.to_le_bytes());
    buf.extend_from_slice(&(meta.rows as u64).to_le_bytes());
    buf.extend_from_slice(&(meta.mem_bytes as u64).to_le_bytes());
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(&(schema_ipc.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(encoded.len() as u64).to_le_bytes());
    debug_assert_eq!(buf.len(), HEADER_LEN);
    buf.extend_from_slice(&schema_ipc);
    buf.extend_from_slice(&encoded);

    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &path)?;

    Ok(SpillHandle {
        path,
        file_bytes: buf.len() as u64,
        mem_bytes: meta.mem_bytes,
        rows: meta.rows,
        wal_segment: meta.wal_segment,
        wal_seq_range: meta.wal_seq_range,
        crc,
    })
}

/// 读 spill 文件（校验 CRC 后解码）。校验失败 = 副本不可信，返回错误由调用方决定降级策略。
pub fn read_spill(handle: &SpillHandle) -> Result<Vec<RecordBatch>, LakeError> {
    let raw = std::fs::read(&handle.path)?;
    let (header, schema_bytes, payload) = parse_header(&raw, &handle.path)?;
    if header.crc != crc32fast::hash(payload) {
        return Err(LakeError::Io(format!(
            "spill crc mismatch at {}: header 0x{:08x} != payload 0x{:08x}",
            handle.path.display(),
            header.crc,
            crc32fast::hash(payload)
        )));
    }
    let schema = deserialize_schema(schema_bytes)?;
    decode_batches(payload, &schema)
}

/// 只校验不读回（恢复阶段判断"spill 副本是否可复用"，架构 §2.6）。
pub fn verify_spill(path: &Path) -> Result<SpillHeader, LakeError> {
    let raw = std::fs::read(path)?;
    let (header, _schema, payload) = parse_header(&raw, path)?;
    if header.crc != crc32fast::hash(payload) {
        return Err(LakeError::Io(format!(
            "spill crc mismatch at {}",
            path.display()
        )));
    }
    Ok(header)
}

/// 删除 spill 文件（释放 / 丢弃副本）。
pub fn remove_spill(handle: &SpillHandle) -> Result<(), LakeError> {
    match std::fs::remove_file(&handle.path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// 解析出的定长头。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpillHeader {
    pub version: u32,
    pub wal_segment: u64,
    pub wal_seq_range_start: u64,
    pub wal_seq_range_end: u64,
    pub rows: u64,
    pub mem_bytes: u64,
    pub crc: u32,
}

/// 从头解析：返回（头, schema IPC 字节, payload）。
fn parse_header<'a>(
    raw: &'a [u8],
    path: &Path,
) -> Result<(SpillHeader, &'a [u8], &'a [u8]), LakeError> {
    if raw.len() < HEADER_LEN {
        return Err(LakeError::Io(format!(
            "spill file too short at {}: {} bytes",
            path.display(),
            raw.len()
        )));
    }
    if raw[0..4] != SPILL_MAGIC {
        return Err(LakeError::Io(format!(
            "bad spill magic at {}",
            path.display()
        )));
    }
    let u32_at = |o: usize| u32::from_le_bytes(raw[o..o + 4].try_into().unwrap());
    let u64_at = |o: usize| u64::from_le_bytes(raw[o..o + 8].try_into().unwrap());

    let header = SpillHeader {
        version: u32_at(4),
        wal_segment: u64_at(8),
        wal_seq_range_start: u64_at(16),
        wal_seq_range_end: u64_at(24),
        rows: u64_at(32),
        mem_bytes: u64_at(40),
        crc: u32_at(48),
    };
    if header.version != SPILL_VERSION {
        return Err(LakeError::Io(format!(
            "unsupported spill version {} at {}",
            header.version,
            path.display()
        )));
    }
    let schema_len = u32_at(52) as usize;
    let payload_len = u64_at(56) as usize;

    let schema_start = HEADER_LEN;
    let payload_start = schema_start + schema_len;
    let payload_end = payload_start + payload_len;
    if payload_end > raw.len() {
        return Err(LakeError::Io(format!(
            "truncated spill file at {}: need {} bytes, have {}",
            path.display(),
            payload_end,
            raw.len()
        )));
    }
    Ok((
        header,
        &raw[schema_start..payload_start],
        &raw[payload_start..payload_end],
    ))
}

/// Arrow IPC 流（LZ4 frame 压缩）编码。
fn encode_batches(batches: &[RecordBatch]) -> Result<Vec<u8>, LakeError> {
    let schema = batches
        .first()
        .map(|b| b.schema())
        .ok_or_else(|| LakeError::Other("spill: empty batch set".into()))?;
    let opts = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))
        .map_err(|e| LakeError::Other(format!("spill ipc options: {e}")))?;
    let mut buf = Vec::with_capacity(1024);
    {
        let mut w = StreamWriter::try_new_with_options(&mut buf, &schema, opts)
            .map_err(|e| LakeError::Other(format!("spill ipc writer: {e}")))?;
        for b in batches {
            w.write(b)
                .map_err(|e| LakeError::Other(format!("spill ipc write: {e}")))?;
        }
        w.finish()
            .map_err(|e| LakeError::Other(format!("spill ipc finish: {e}")))?;
    }
    Ok(buf)
}

/// Arrow IPC 流解码（压缩标记在流内，无需外部参数）。
fn decode_batches(payload: &[u8], _schema: &arrow::datatypes::SchemaRef) -> Result<Vec<RecordBatch>, LakeError> {
    let reader = StreamReader::try_new(std::io::Cursor::new(payload), None)
        .map_err(|e| LakeError::Other(format!("spill ipc reader: {e}")))?;
    let mut out = Vec::new();
    for b in reader {
        out.push(b.map_err(|e| LakeError::Other(format!("spill ipc read: {e}")))?);
    }
    Ok(out)
}

/// spill 目录（节点私有：`<wal_root>/spill` 同级亦可，由装配层决定）。
pub fn default_spill_dir(base: &Path) -> PathBuf {
    base.join("spill")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_time", DataType::Int64, false),
            Field::new("user", DataType::Utf8, true),
        ]));
        vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![4, 5])),
                    Arc::new(StringArray::from(vec![Some("d"), Some("e")])),
                ],
            )
            .unwrap(),
        ]
    }

    fn meta() -> SpillMeta {
        SpillMeta {
            wal_segment: 7,
            wal_seq_range: 100..108,
            rows: 5,
            mem_bytes: 4096,
        }
    }

    #[test]
    fn spill_roundtrip_preserves_rows_schema_and_meta() {
        let dir = yuntun_testkit::TestDir::tmpfs("spill-roundtrip");
        let h = write_spill(dir.path(), "c1", &batches(), meta()).unwrap();
        assert_eq!(h.rows, 5);
        assert_eq!(h.wal_segment, 7);
        assert_eq!(h.wal_seq_range, 100..108);
        assert_eq!(h.mem_bytes, 4096);
        assert!(h.file_bytes >= HEADER_LEN as u64);

        let out = read_spill(&h).unwrap();
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 5);
        assert_eq!(out[0].schema().field(1).name(), "user");
        // 头自描述：读回后能核对 WAL 引用（架构 §2.6 复用判定）
        let header = verify_spill(&h.path).unwrap();
        assert_eq!(header.wal_segment, 7);
        assert_eq!(header.wal_seq_range_start, 100);
        assert_eq!(header.wal_seq_range_end, 108);
        assert_eq!(header.rows, 5);
        assert_eq!(header.mem_bytes, 4096);
    }

    #[test]
    fn corrupted_payload_is_detected_by_crc() {
        let dir = yuntun_testkit::TestDir::tmpfs("spill-corrupt");
        let h = write_spill(dir.path(), "c2", &batches(), meta()).unwrap();
        // 翻转 payload 的一个字节（头之后）
        let mut raw = std::fs::read(&h.path).unwrap();
        let last = raw.len() - 1;
        raw[last] ^= 0xff;
        std::fs::write(&h.path, &raw).unwrap();

        assert!(read_spill(&h).is_err(), "CRC 校验必须拦住被篡改的副本");
        assert!(verify_spill(&h.path).is_err());
    }

    #[test]
    fn truncated_file_is_rejected() {
        let dir = yuntun_testkit::TestDir::tmpfs("spill-truncated");
        let h = write_spill(dir.path(), "c3", &batches(), meta()).unwrap();
        let raw = std::fs::read(&h.path).unwrap();
        std::fs::write(&h.path, &raw[..raw.len() - 4]).unwrap();
        assert!(read_spill(&h).is_err());
    }

    #[test]
    fn bad_magic_is_rejected() {
        let dir = yuntun_testkit::TestDir::tmpfs("spill-magic");
        let p = dir.path().join("junk.ipc.lz4");
        std::fs::write(&p, vec![0u8; HEADER_LEN + 8]).unwrap();
        assert!(verify_spill(&p).is_err());
    }

    #[test]
    fn spill_uses_local_disk_only() {
        // I2：spill 是节点私有状态，路径必须在传入的本地目录下
        let dir = yuntun_testkit::TestDir::tmpfs("spill-local");
        let h = write_spill(dir.path(), "c4", &batches(), meta()).unwrap();
        assert_eq!(h.path.parent().unwrap(), dir.path());
        assert!(h.path.to_string_lossy().ends_with(".ipc.lz4"));
    }
}
