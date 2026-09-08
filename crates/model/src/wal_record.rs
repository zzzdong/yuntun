//! WAL Record 类型与编解码（详细设计 §4.3）。
//!
//! Record 是状态机事件，Data 与 BatchState 在同一条 append-only 流中，
//! 顺序即因果，原子性天然保证（架构 §5.3.4 / ADR-11）。
//!
//! 二进制格式（详细设计 §4.2）：
//! ```text
//! Record:
//!   length(u32) | crc32(u32) | type(u8) | payload(variable)
//!    └ length = payload 字节数
//!    └ crc32  覆盖 type + payload（不含 length）  ← C3
//! ```
//!
//! 序列化选 prost（与 proto 共用），便于阶段 2 跨语言（§4.3）。

use crate::error::WalError;

/// Record 类型（详细设计 §4.3 表格）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    Data = 0,
    BatchPending = 1,
    BatchS3Written = 2,
    BatchCommitted = 3,
    BatchAbort = 4,
}

impl RecordType {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => Self::Data,
            1 => Self::BatchPending,
            2 => Self::BatchS3Written,
            3 => Self::BatchCommitted,
            4 => Self::BatchAbort,
            _ => return None,
        })
    }
}

// ---------------- Payload 消息（prost）----------------

/// type=0 Data：Arrow IPC 序列化的 RecordBatch + 归属信息
#[derive(Clone, PartialEq, prost::Message)]
pub struct DataPayload {
    #[prost(string, tag = "1")]
    pub table: String,
    #[prost(string, tag = "2")]
    pub shard: String,
    #[prost(uint64, tag = "3")]
    pub schema_version: u64,
    /// Arrow IPC 序列化的 RecordBatch
    #[prost(bytes = "vec", tag = "4")]
    pub batch_ipc: Vec<u8>,
    /// 客户端幂等键（可空，§7.3）
    #[prost(string, tag = "5")]
    pub client_request_id: String,
    /// 窗口归属（整分钟对齐，ADR-10）：如 "2026-08-31T14:00"
    #[prost(string, tag = "6")]
    pub time_window: String,
}

/// type=1 BatchPending
#[derive(Clone, PartialEq, prost::Message)]
pub struct BatchPendingPayload {
    #[prost(string, tag = "1")]
    pub batch_id: String,
    #[prost(string, tag = "2")]
    pub shard: String,
    #[prost(string, tag = "3")]
    pub window: String,
    /// WAL 内该批次覆盖的 Data 记录区间（seq 语义）
    #[prost(uint64, tag = "4")]
    pub wal_seq_start: u64,
    #[prost(uint64, tag = "5")]
    pub wal_seq_end: u64,
    #[prost(uint64, tag = "6")]
    pub schema_version: u64,
    #[prost(string, tag = "7")]
    pub client_request_id: String,
    /// 创建时间（Unix 毫秒），批次级超时判断基准（§5.3.6.1）
    #[prost(uint64, tag = "8")]
    pub created_at_ms: u64,
    #[prost(uint64, tag = "9")]
    pub row_count: u64,
}

/// type=2 BatchS3Written
#[derive(Clone, PartialEq, prost::Message)]
pub struct BatchS3WrittenPayload {
    #[prost(string, tag = "1")]
    pub batch_id: String,
    #[prost(string, repeated, tag = "2")]
    pub s3_paths: Vec<String>,
    /// Multipart upload_id（持久化用于续传，§7.4）
    #[prost(string, tag = "3")]
    pub s3_upload_id: String,
    #[prost(uint64, tag = "4")]
    pub file_size: u64,
}

/// type=3 BatchCommitted
#[derive(Clone, PartialEq, prost::Message)]
pub struct BatchCommittedPayload {
    #[prost(string, tag = "1")]
    pub batch_id: String,
}

/// type=4 BatchAbort（终态，C4：Abort 也是终态，segment 可释放）
#[derive(Clone, PartialEq, prost::Message)]
pub struct BatchAbortPayload {
    #[prost(string, tag = "1")]
    pub batch_id: String,
}

/// WAL Record 枚举。
#[derive(Debug, Clone, PartialEq)]
pub enum Record {
    Data(DataPayload),
    BatchPending(BatchPendingPayload),
    BatchS3Written(BatchS3WrittenPayload),
    BatchCommitted(BatchCommittedPayload),
    BatchAbort(BatchAbortPayload),
}

impl Record {
    pub fn record_type(&self) -> RecordType {
        match self {
            Record::Data(_) => RecordType::Data,
            Record::BatchPending(_) => RecordType::BatchPending,
            Record::BatchS3Written(_) => RecordType::BatchS3Written,
            Record::BatchCommitted(_) => RecordType::BatchCommitted,
            Record::BatchAbort(_) => RecordType::BatchAbort,
        }
    }

    pub fn batch_id(&self) -> Option<&str> {
        match self {
            Record::BatchPending(p) => Some(&p.batch_id),
            Record::BatchS3Written(p) => Some(&p.batch_id),
            Record::BatchCommitted(p) => Some(&p.batch_id),
            Record::BatchAbort(p) => Some(&p.batch_id),
            Record::Data(_) => None,
        }
    }

    /// 编码 payload 为字节（prost）。
    pub fn encode_payload(&self) -> Vec<u8> {
        use prost::Message;
        match self {
            Record::Data(p) => p.encode_to_vec(),
            Record::BatchPending(p) => p.encode_to_vec(),
            Record::BatchS3Written(p) => p.encode_to_vec(),
            Record::BatchCommitted(p) => p.encode_to_vec(),
            Record::BatchAbort(p) => p.encode_to_vec(),
        }
    }

    /// 从 type + payload 解码。
    pub fn decode(ty: u8, payload: &[u8]) -> Result<Self, WalError> {
        use prost::Message;
        let rt = RecordType::from_u8(ty)
            .ok_or_else(|| WalError::Other(format!("unknown record type {ty}")))?;
        Ok(match rt {
            RecordType::Data => Record::Data(
                DataPayload::decode(payload).map_err(|e| WalError::Other(e.to_string()))?,
            ),
            RecordType::BatchPending => Record::BatchPending(
                BatchPendingPayload::decode(payload).map_err(|e| WalError::Other(e.to_string()))?,
            ),
            RecordType::BatchS3Written => Record::BatchS3Written(
                BatchS3WrittenPayload::decode(payload)
                    .map_err(|e| WalError::Other(e.to_string()))?,
            ),
            RecordType::BatchCommitted => Record::BatchCommitted(
                BatchCommittedPayload::decode(payload)
                    .map_err(|e| WalError::Other(e.to_string()))?,
            ),
            RecordType::BatchAbort => Record::BatchAbort(
                BatchAbortPayload::decode(payload).map_err(|e| WalError::Other(e.to_string()))?,
            ),
        })
    }
}

/// FileHeader（详细设计 §4.2）：magic(4B) | version(2B) | shard_id(8B) | first_seq(8B)，共 22 字节。
pub const WAL_MAGIC: [u8; 4] = [0x59, 0x55, 0x4E, 0x54]; // "YUNT"
pub const WAL_VERSION: u16 = 1;
pub const FILE_HEADER_SIZE: usize = 22;
/// Record 固定头：length(4B) + crc32(4B) + type(1B)
pub const RECORD_HEADER_SIZE: usize = 9;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileHeader {
    pub shard_id: u64,
    pub first_seq: u64,
}

impl FileHeader {
    pub fn encode(&self) -> [u8; FILE_HEADER_SIZE] {
        let mut buf = [0u8; FILE_HEADER_SIZE];
        buf[0..4].copy_from_slice(&WAL_MAGIC);
        buf[4..6].copy_from_slice(&WAL_VERSION.to_le_bytes());
        buf[6..14].copy_from_slice(&self.shard_id.to_le_bytes());
        buf[14..22].copy_from_slice(&self.first_seq.to_le_bytes());
        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Self, WalError> {
        if buf.len() < FILE_HEADER_SIZE {
            return Err(WalError::Other("header too short".into()));
        }
        if buf[0..4] != WAL_MAGIC {
            return Err(WalError::BadMagic("segment".into()));
        }
        let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        if version != WAL_VERSION {
            return Err(WalError::UnsupportedVersion(version));
        }
        Ok(Self {
            shard_id: u64::from_le_bytes(buf[6..14].try_into().unwrap()),
            first_seq: u64::from_le_bytes(buf[14..22].try_into().unwrap()),
        })
    }
}

/// CRC 计算（C3）：覆盖 `type || payload`，不覆盖 length。
pub fn record_crc(record_type: u8, payload: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(std::slice::from_ref(&record_type));
    h.update(payload);
    h.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_header_roundtrip() {
        let h = FileHeader {
            shard_id: 42,
            first_seq: 7,
        };
        let buf = h.encode();
        assert_eq!(buf.len(), FILE_HEADER_SIZE);
        assert_eq!(FileHeader::decode(&buf).unwrap(), h);
    }

    #[test]
    fn file_header_bad_magic() {
        let mut buf = FileHeader {
            shard_id: 1,
            first_seq: 1,
        }
        .encode();
        buf[0] = b'X';
        assert!(matches!(
            FileHeader::decode(&buf),
            Err(WalError::BadMagic(_))
        ));
    }

    #[test]
    fn record_roundtrip_all_types() {
        let records = vec![
            Record::Data(DataPayload {
                table: "t".into(),
                shard: "s0".into(),
                schema_version: 1,
                batch_ipc: vec![1, 2, 3],
                client_request_id: "k1".into(),
                time_window: "w".into(),
            }),
            Record::BatchPending(BatchPendingPayload {
                batch_id: "b1".into(),
                shard: "s0".into(),
                window: "w".into(),
                wal_seq_start: 1,
                wal_seq_end: 2,
                schema_version: 1,
                client_request_id: "k1".into(),
                created_at_ms: 123,
                row_count: 100,
            }),
            Record::BatchS3Written(BatchS3WrittenPayload {
                batch_id: "b1".into(),
                s3_paths: vec!["a".into(), "b".into()],
                s3_upload_id: "u1".into(),
                file_size: 999,
            }),
            Record::BatchCommitted(BatchCommittedPayload {
                batch_id: "b1".into(),
            }),
            Record::BatchAbort(BatchAbortPayload {
                batch_id: "b1".into(),
            }),
        ];
        for r in records {
            let payload = r.encode_payload();
            let ty = r.record_type() as u8;
            let back = Record::decode(ty, &payload).unwrap();
            assert_eq!(back.record_type(), r.record_type());
            assert_eq!(back.encode_payload(), payload);
        }
    }

    #[test]
    fn crc_excludes_length() {
        // C3: crc 覆盖 type+payload，同一 payload 无论 length 如何表述 crc 相同
        let payload = b"hello world";
        let c1 = record_crc(0, payload);
        let c2 = record_crc(0, payload);
        assert_eq!(c1, c2);
        assert_ne!(record_crc(1, payload), c1, "type participates in crc");
    }
}
