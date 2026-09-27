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
    /// S1.7：DDL 事件（CREATE/DROP TABLE），启动重放重建 Catalog 表清单
    Ddl = 5,
    /// `F.3`：**DELETE**（行位删除向量）—— 启动重放重建 `DeletionEntry`（`delta-dml-design §1.1` ③）
    Delete = 6,
}

impl RecordType {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => Self::Data,
            1 => Self::BatchPending,
            2 => Self::BatchS3Written,
            3 => Self::BatchCommitted,
            4 => Self::BatchAbort,
            5 => Self::Ddl,
            6 => Self::Delete,
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
    /// v1 扩展：所属表（全限定名，如 "public.t"）。老 segment 缺省为空 ——
    /// 恢复重提交时回退为从 s3_paths[0] 反解（delta-dml-design §1.1）。
    #[prost(string, tag = "10")]
    pub table: String,
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

/// type=5 Ddl（S1.7）：DDL 事件，Catalog 变更的 WAL 权威记录。
///
/// 语义：server 在 Catalog apply 成功后 append（顺序即因果）；启动时**先重放 DDL
/// 再分流 batch 恢复**，保证 SQL 写入的数据崩溃重启后表存在、可恢复（S1.6 验收）。
#[derive(Clone, PartialEq, prost::Message)]
pub struct DdlPayload {
    /// 0 = CreateTable, 1 = DropTable, 2 = CreateSchema, 3 = DropSchema, 4 = AlterTable
    #[prost(uint32, tag = "1")]
    pub op: u32,
    /// 表标识（**全限定 `schema.table`**）或 schema 名（CreateSchema/DropSchema）
    #[prost(string, tag = "2")]
    pub table: String,
    /// CreateTable / AlterTable：Arrow Schema（IPC 序列化，model::meta::serialize_schema）。
    ///
    /// AlterTable 存的是**变更后的目标 schema**（不是"变更本身"）：重放时按它把表
    /// **收敛到目标态**，于是"重放时表已经演进过"（幂等）不需要特殊分支。
    #[prost(bytes = "vec", tag = "3")]
    pub arrow_schema: Vec<u8>,
    /// CreateTable：default_format（"parquet" | "vortex"）
    #[prost(string, tag = "4")]
    pub default_format: String,
}

/// DdlPayload.op 取值。
pub mod ddl_op {
    pub const CREATE_TABLE: u32 = 0;
    pub const DROP_TABLE: u32 = 1;
    /// 多 schema：CREATE DATABASE / CREATE SCHEMA（`DdlPayload.table` = schema 名）
    pub const CREATE_SCHEMA: u32 = 2;
    pub const DROP_SCHEMA: u32 = 3;
    /// `ALTER TABLE`（F.2）：`arrow_schema` = **变更后的目标 schema**（重放即收敛到它）
    pub const ALTER_TABLE: u32 = 4;
}

/// type=6 Delete（`F.3`；`delta-dml-design §4.1`）。
///
/// **一条 DELETE 只有一条记录**：MVP 是单物理 WAL（`shard_key` 只是逻辑分片），
/// 跨逻辑 shard 的删除**合并**在同一条 payload 里 ⇒ 天然原子（要么全生效、要么全不生效）。
///
/// 位图**内联**在记录里（而不是只记对象路径）的理由：重放要能**只靠 WAL**重建目录
/// （`§1.1` 的 ③）—— 对象存储是副产品，WAL 才是权威（ADR-3）。
#[derive(Clone, PartialEq, prost::Message)]
pub struct DeletePayload {
    #[prost(string, tag = "1")]
    pub table: String,
    /// 幂等 id（= 触发它的请求 id；重放按它去重）
    #[prost(string, tag = "2")]
    pub dv_id: String,
    #[prost(message, repeated, tag = "3")]
    pub deletions: Vec<FileDeletion>,
    /// 表世代（与 `ChunkStore::liveness` 同源）：重放时**校验失败就跳过** ——
    /// `DROP` → 同名重建之后，旧世代的删除不得挂到新表上（`plan.md` M0 ⑥）
    #[prost(uint64, tag = "4")]
    pub schema_epoch: u64,
}

/// 一个数据文件上的删除（`FileDeletion`）。
#[derive(Clone, PartialEq, prost::Message)]
pub struct FileDeletion {
    #[prost(string, tag = "1")]
    pub file_path: String,
    /// 归属批次（清理对账键 `dv → file_path → batch_id`，`delta-dml-design §6.2`）。
    ///
    /// 设计 §4.1 的 `FileDeletion` 没写这一条 —— 但**重放时清单还是空的**
    /// （内存目录只重放了 DDL），`batch_id` 无处可推，只能从记录里带（同 `§146.2` 偏差 1 的道理）。
    #[prost(string, tag = "2")]
    pub batch_id: String,
    /// 该文件内被删行号的位图：`DvBitmap::to_bytes()`（roaring + 自证帧：魔数/版本/CRC）
    #[prost(bytes = "vec", tag = "3")]
    pub bitmap: Vec<u8>,
}

/// WAL Record 枚举。
#[derive(Debug, Clone, PartialEq)]
pub enum Record {
    Data(DataPayload),
    BatchPending(BatchPendingPayload),
    BatchS3Written(BatchS3WrittenPayload),
    BatchCommitted(BatchCommittedPayload),
    BatchAbort(BatchAbortPayload),
    Ddl(DdlPayload),
    Delete(DeletePayload),
}

impl Record {
    pub fn record_type(&self) -> RecordType {
        match self {
            Record::Data(_) => RecordType::Data,
            Record::BatchPending(_) => RecordType::BatchPending,
            Record::BatchS3Written(_) => RecordType::BatchS3Written,
            Record::BatchCommitted(_) => RecordType::BatchCommitted,
            Record::BatchAbort(_) => RecordType::BatchAbort,
            Record::Ddl(_) => RecordType::Ddl,
            Record::Delete(_) => RecordType::Delete,
        }
    }

    pub fn batch_id(&self) -> Option<&str> {
        match self {
            Record::BatchPending(p) => Some(&p.batch_id),
            Record::BatchS3Written(p) => Some(&p.batch_id),
            Record::BatchCommitted(p) => Some(&p.batch_id),
            Record::BatchAbort(p) => Some(&p.batch_id),
            Record::Data(_) => None,
            Record::Ddl(_) => None,
            Record::Delete(_) => None,
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
            Record::Ddl(p) => p.encode_to_vec(),
            Record::Delete(p) => p.encode_to_vec(),
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
            RecordType::Ddl => Record::Ddl(
                DdlPayload::decode(payload).map_err(|e| WalError::Other(e.to_string()))?,
            ),
            RecordType::Delete => Record::Delete(
                DeletePayload::decode(payload).map_err(|e| WalError::Other(e.to_string()))?,
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

    /// `F.3`：DELETE 记录里的位图**过线之后仍解得出**（帧带 CRC，坏一字节就该拒绝）。
    #[test]
    fn delete_payload_keeps_the_bitmaps_decodable() {
        let payload = DeletePayload {
            table: "public.t".into(),
            dv_id: "dv-1".into(),
            deletions: vec![FileDeletion {
                file_path: "p/a.parquet".into(),
                batch_id: "b1".into(),
                bitmap: crate::dv::DvBitmap::from_positions([1, 2, 3]).to_bytes(),
            }],
            schema_epoch: 7,
        };
        let bytes = Record::Delete(payload.clone()).encode_payload();
        let back = Record::decode(RecordType::Delete as u8, &bytes).unwrap();
        let Record::Delete(back) = back else {
            panic!("类型串了：{back:?}");
        };
        assert_eq!(back, payload);
        let dv = crate::dv::DvBitmap::from_bytes(&back.deletions[0].bitmap).unwrap();
        assert_eq!(dv.card(), 3, "位图必须原样过线（重放靠它重建 DeletionEntry）");
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
                table: "public.t".into(),
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
            Record::Ddl(DdlPayload {
                op: ddl_op::CREATE_TABLE,
                table: "t".into(),
                arrow_schema: vec![9, 9],
                default_format: "parquet".into(),
            }),
            // `F.3`：DELETE（位图内联；跨逻辑 shard 合并成一条）
            Record::Delete(DeletePayload {
                table: "public.t".into(),
                dv_id: "dv-1".into(),
                deletions: vec![
                    FileDeletion {
                        file_path: "p/a.parquet".into(),
                        batch_id: "b1".into(),
                        bitmap: crate::dv::DvBitmap::from_positions([0, 5]).to_bytes(),
                    },
                    FileDeletion {
                        file_path: "p/b.parquet".into(),
                        batch_id: "b2".into(),
                        bitmap: crate::dv::DvBitmap::from_positions([7]).to_bytes(),
                    },
                ],
                schema_epoch: 3,
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
