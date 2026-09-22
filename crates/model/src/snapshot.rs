//! Catalog 状态机快照的**帧格式**（`docs/metanode-design.md` §4.4）。
//!
//! # 为什么不是一个裸 blob
//!
//! 这是 WAL tearing 教训（`operation-log §29.1`）的推广：**部分接收必须能被识别**。
//!
//! 快照会在网络传输、对象存储、本地文件里被截断，而**截断后的字节前缀仍然是合法的
//! protobuf**（protobuf 本身不校验完整性、字段可选、未知数据被忽略）。于是"半个快照"
//! 会被静默解码成一个**看起来正常但缺了一半数据的状态**并安装到副本上 ——
//! 副本与 leader 状态不一致，且没有任何一处报错。
//!
//! # 三层保护
//!
//! | 层 | 保护 | 抓什么 |
//! |---|---|---|
//! | 帧头 | `magic` + `format_version` + **帧头 CRC** | 截断到 36 字节以内、`revision`/`payload_len`/`chunk_size` 被改 |
//! | 每块 | **块 CRC** + 显式块长 | 块内任意字节损坏、块被截半 |
//! | 整体 | `payload_len` 与实际收集长度一致 + 末尾无残留 | 丢块、多块 |
//!
//! **帧头也必须 CRC**：`revision` 只出现在帧头；而单字节翻转 `chunk_size` 会让块长上限
//! 检查变**松**（本来就是 `<=` 判定），块 CRC 依旧通过 —— 只有帧头 CRC 能抓到。
//!
//! # 帧布局
//!
//! 帧头（36 字节，全小端）：
//!
//! ```text
//! 0..8    magic "YTSNAP01"
//! 8..12   format_version u32
//! 12..20  revision u64          （= CatalogState.snapshot_version）
//! 20..28  payload_len u64
//! 28..32  chunk_size u32
//! 32..36  header_crc u32        （覆盖 0..32）
//! ```
//!
//! 之后是块序列，直到 `payload_len` 字节收齐：
//!
//! ```text
//! [+0..+4)  data_len u32（1..=chunk_size）
//! [+4..+8)  crc32 u32（覆盖 data）
//! [+8..+8+data_len)  data
//! ```
//!
//! 空载荷（`payload_len = 0`）合法且**没有块** —— 空状态机也有快照。

use crate::error::SnapshotError;

/// 帧头 magic（含版本字母，便于十六进制 dump 时肉眼识别）。
pub const SNAPSHOT_MAGIC: [u8; 8] = *b"YTSNAP01";
/// 本实现写出的帧格式版本。
/// 快照载荷布局版本。**改布局必须 +1**（例如 2：新增 `datanodes`）。
///
/// 为什么必须 bump：`decode_payload` 拿它做**严格相等**校验 —— 旧构建读到新载荷会
/// **明确拒绝**，而不是默默忽略未知字段。名录这类字段一旦被静默丢掉，查询侧就会按
/// 空成员表算归属（`§69` 那类"静默少数据"）。
pub const SNAPSHOT_FORMAT_VERSION: u32 = 2;
/// 帧头长度。
pub const SNAPSHOT_HEADER_LEN: usize = 36;
/// 块头长度（`data_len` + `crc`）。
pub const SNAPSHOT_CHUNK_OVERHEAD: usize = 8;
/// 默认块大小（1 MiB）：足够小以便"坏一块"的代价可控，足够大以免元数据开销占比过高。
pub const DEFAULT_CHUNK_SIZE: u32 = 1 << 20;
/// 块大小上限（防某个畸形帧声称块巨大从而造成大额分配）。
pub const MAX_CHUNK_SIZE: u32 = 64 << 20;

/// 解帧结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unframed {
    pub format_version: u32,
    /// 帧头里的快照号（与载荷内的 `revision` 必须一致，见 [`crate::snapshot::unframe`]）
    pub revision: u64,
    /// 载荷字节（未解码 —— 解码成状态机的责任在 `yuntun-catalog`）
    pub payload: Vec<u8>,
}

/// 把载荷打成帧。
///
/// `chunk_size = 0` 时用 [`DEFAULT_CHUNK_SIZE`]；超过 [`MAX_CHUNK_SIZE`] 时**截到上限**
/// （宁可多切几块，也不能让一次写入分配几十 MB 的块缓冲）。
pub fn frame(payload: &[u8], revision: u64, chunk_size: u32) -> Vec<u8> {
    let chunk_size = match chunk_size {
        0 => DEFAULT_CHUNK_SIZE,
        n if n > MAX_CHUNK_SIZE => MAX_CHUNK_SIZE,
        n => n,
    } as usize;

    let mut out = Vec::with_capacity(SNAPSHOT_HEADER_LEN + payload.len() + 64);
    out.extend_from_slice(&SNAPSHOT_MAGIC);
    out.extend_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&revision.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    out.extend_from_slice(&(chunk_size as u32).to_le_bytes());
    // 帧头 CRC 覆盖它前面的 32 字节
    let hcrc = crc32fast::hash(&out[..SNAPSHOT_HEADER_LEN - 4]);
    out.extend_from_slice(&hcrc.to_le_bytes());

    for chunk in payload.chunks(chunk_size) {
        out.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc32fast::hash(chunk).to_le_bytes());
        out.extend_from_slice(chunk);
    }
    out
}

/// 解帧并**逐层校验**。
///
/// 调用方拿到 `payload` 后**仍需**做语义校验（见 `CatalogState::restore_snapshot`）：
/// 帧只保证"字节没被截断/篡改"，不保证"内容是一个合法状态"。
pub fn unframe(bytes: &[u8]) -> Result<Unframed, SnapshotError> {
    if bytes.len() < SNAPSHOT_HEADER_LEN {
        return Err(SnapshotError::TooShort { len: bytes.len() });
    }
    if bytes[..8] != SNAPSHOT_MAGIC {
        return Err(SnapshotError::BadMagic);
    }
    let be32 = |o: usize| u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
    let be64 = |o: usize| u64::from_le_bytes(bytes[o..o + 8].try_into().unwrap());

    let format_version = be32(8);
    let revision = be64(12);
    let payload_len = be64(20);
    let chunk_size = be32(28);
    let stored_hcrc = be32(32);
    let actual_hcrc = crc32fast::hash(&bytes[..SNAPSHOT_HEADER_LEN - 4]);
    // ⚠️ 校验顺序**刻意**如此：magic → 帧头 CRC → 版本。
    //
    // 版本检查必须在帧头 CRC **之后**：若帧头被改坏（版本字段恰好变成别的数），
    // 先报 `UnsupportedVersion` 会把运维引向"去升级 build"，而真相是**文件损坏**
    // —— 错误信息的指向性直接决定恢复动作，这类顺序错会造成误诊。
    // magic 反过来必须在 CRC 之前：它不是快照时，报 `BadMagic` 比"CRC 不符"有用。
    if stored_hcrc != actual_hcrc {
        return Err(SnapshotError::HeaderCrcMismatch {
            expected: stored_hcrc,
            actual: actual_hcrc,
        });
    }
    if format_version != SNAPSHOT_FORMAT_VERSION {
        // 帧头完好（CRC 已过）但版本不同 = 确实是**别的版本写的合法快照**
        return Err(SnapshotError::UnsupportedVersion(format_version));
    }
    if chunk_size == 0 || chunk_size > MAX_CHUNK_SIZE {
        return Err(SnapshotError::InvalidChunkSize { chunk_size });
    }

    let mut payload = Vec::with_capacity(payload_len.min(1 << 20) as usize);
    let mut pos = SNAPSHOT_HEADER_LEN;
    while pos < bytes.len() {
        let remaining = bytes.len() - pos;
        if remaining < SNAPSHOT_CHUNK_OVERHEAD {
            // 块头都没齐 —— 这就是"截断"的形态，必须报错而不是当收尾
            return Err(SnapshotError::Truncated {
                offset: pos,
                needed: SNAPSHOT_CHUNK_OVERHEAD,
                remaining,
            });
        }
        let dlen = be32(pos) as usize;
        let dcrc = be32(pos + 4);
        if dlen == 0 || dlen > chunk_size as usize {
            return Err(SnapshotError::InvalidChunkSize {
                chunk_size: dlen as u32,
            });
        }
        let need = SNAPSHOT_CHUNK_OVERHEAD + dlen;
        if remaining < need {
            return Err(SnapshotError::Truncated {
                offset: pos,
                needed: need,
                remaining,
            });
        }
        let data = &bytes[pos + SNAPSHOT_CHUNK_OVERHEAD..pos + need];
        let actual = crc32fast::hash(data);
        if actual != dcrc {
            return Err(SnapshotError::ChunkCrcMismatch {
                offset: pos,
                expected: dcrc,
                actual,
            });
        }
        payload.extend_from_slice(data);
        pos += need;
    }

    if payload.len() as u64 != payload_len {
        return Err(SnapshotError::LengthMismatch {
            declared: payload_len,
            actual: payload.len(),
        });
    }
    Ok(Unframed {
        format_version,
        revision,
        payload,
    })
}

// ---------------------------------------------------------------- 快照载荷（prost 消息）
//
// 放在本模块而不是 `meta.rs`：**载荷与帧是同一个格式**的两层（§4.4），
// 分开会让"改字段忘了改帧/忘了加测试"更容易发生。

/// 快照里的一条表元数据。
#[derive(Clone, PartialEq, prost::Message)]
pub struct SnapshotTableEntry {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(message, optional, tag = "2")]
    pub meta: Option<crate::meta::TableMeta>,
}

/// 快照里的一条 schema 版本（键 = `(表, 版本)`）。
#[derive(Clone, PartialEq, prost::Message)]
pub struct SnapshotSchemaEntry {
    #[prost(string, tag = "1")]
    pub table: String,
    #[prost(uint64, tag = "2")]
    pub version: u64,
    #[prost(message, optional, tag = "3")]
    pub value: Option<crate::meta::SchemaVersion>,
}

/// 快照里的一条文件清单（键 = `batch_id`）。
#[derive(Clone, PartialEq, prost::Message)]
pub struct SnapshotFileEntry {
    #[prost(string, tag = "1")]
    pub batch_id: String,
    #[prost(message, optional, tag = "2")]
    pub manifest: Option<crate::meta::FileManifest>,
}

/// 快照里的一条幂等记录（键 = `client_request_id`）。
#[derive(Clone, PartialEq, prost::Message)]
pub struct SnapshotIdempotencyEntry {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(message, optional, tag = "2")]
    pub record: Option<crate::meta::IdempotencyRecord>,
}

/// 快照里的一条"每表最后清单变更版本"。
#[derive(Clone, PartialEq, prost::Message)]
pub struct SnapshotTableVerEntry {
    #[prost(string, tag = "1")]
    pub table: String,
    #[prost(uint64, tag = "2")]
    pub manifest_ver: u64,
}

/// **Catalog 状态机的完整快照**（`metanode-design.md` §4.4）。
///
/// # 为什么是"完整字段"而不是 `encode_canonical` 的文本
///
/// `encode_canonical` 是**对拍工具**（只编码能区分状态的少数字段、人可读）；
/// 快照必须**无损** —— 少一个字段就意味着换主/重启后状态静默回退。
/// 因此这里逐字段承载，并配一条"每个维度都必须体现在快照里"的测试
/// （`snapshot_covers_every_state_dimension`）专防"加字段忘加进快照"。
///
/// # 顺序无关
///
/// 各 `Vec` 的键序由 `BTreeMap`/`BTreeSet` 决定，**与插入历史无关**：
/// 同一最终状态 → 同一字节。这正是它可用于"副本间逐字节比对"的前提。
#[derive(Clone, PartialEq, prost::Message)]
pub struct CatalogStateSnapshot {
    /// 快照格式版本（**不是** schema 版本）：升级载荷布局时用它拒绝不兼容的旧/新载荷
    #[prost(uint64, tag = "1")]
    pub format_version: u64,
    /// 快照号 = `CatalogState.snapshot_version`（单调递增，§6.3）。
    /// 帧头里的同名值必须与它一致（防"帧与载荷来自不同快照"）。
    #[prost(uint64, tag = "2")]
    pub revision: u64,
    /// 已 apply 的变更数（= raft applied index）：安装快照后 `advance_apply_to` 要用
    #[prost(uint64, tag = "3")]
    pub last_applied: u64,
    /// 结构变更计数（缓存**全量重建**触发器）
    #[prost(uint64, tag = "4")]
    pub schema_ver: u64,
    /// 清单变更计数（缓存**增量刷新**触发器）
    #[prost(uint64, tag = "5")]
    pub manifest_ver: u64,
    /// schema 注册表（含 `public`）
    #[prost(string, repeated, tag = "6")]
    pub namespaces: Vec<String>,
    #[prost(message, repeated, tag = "7")]
    pub tables: Vec<SnapshotTableEntry>,
    #[prost(message, repeated, tag = "8")]
    pub schemas: Vec<SnapshotSchemaEntry>,
    #[prost(message, repeated, tag = "9")]
    pub files: Vec<SnapshotFileEntry>,
    #[prost(message, repeated, tag = "10")]
    pub idempotency: Vec<SnapshotIdempotencyEntry>,
    #[prost(message, repeated, tag = "11")]
    pub table_manifest_ver: Vec<SnapshotTableVerEntry>,
    /// **数据节点名录**（T12.3）：装快照后必须原样恢复 —— 它是"分片归属"的输入，
    /// 丢一份就等于全体查询按空成员表算归属。
    #[prost(message, repeated, tag = "12")]
    pub datanodes: Vec<crate::meta::DatanodeMember>,
}

// ---------------------------------------------------------------- 载荷编解码

/// 载荷编码（**不含**帧头 —— 打帧用 [`frame`]）。
///
/// 编码结果由各 `Vec` 的键序决定，而键序来自状态机的 `BTreeMap`：
/// 同一最终状态 → 同一字节（可用于副本间逐字节比对）。
pub fn encode_payload(msg: &CatalogStateSnapshot) -> Vec<u8> {
    use prost::Message as _;
    msg.encode_to_vec()
}

/// 载荷解码。
///
/// # ⚠️ 必须先 [`unframe`]
///
/// protobuf 本身**不校验完整性**：截断的前缀照样解得开（只丢掉尾部字段），
/// 于是"解开了"**不等于**"完整"。直接对本函数喂未校验的字节 = 静默半状态。
/// 因此正常路径是 [`unframe`] → `decode_payload`，本函数只负责后者。
pub fn decode_payload(bytes: &[u8]) -> Result<CatalogStateSnapshot, SnapshotError> {
    use prost::Message as _;
    CatalogStateSnapshot::decode(bytes).map_err(|e| SnapshotError::Decode(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_empty_payload() {
        let p = b"hello snapshot payload".to_vec();
        let framed = frame(&p, 7, 0);
        let un = unframe(&framed).unwrap();
        assert_eq!(un.format_version, SNAPSHOT_FORMAT_VERSION);
        assert_eq!(un.revision, 7);
        assert_eq!(un.payload, p);

        // 空载荷：合法，且**没有块**
        let framed = frame(&[], 1, 0);
        assert_eq!(framed.len(), SNAPSHOT_HEADER_LEN);
        assert_eq!(unframe(&framed).unwrap().payload, Vec::<u8>::new());
    }

    #[test]
    fn chunking_is_transparent_at_every_boundary() {
        // 块大小附近的多档载荷：解帧结果必须与载荷**逐字节相同**（切块不应可见）
        for len in [1usize, 7, 8, 9, 1023, 1024, 1025] {
            let p: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            for cs in [1u32, 2, 64, 1024] {
                let un = unframe(&frame(&p, 3, cs)).unwrap();
                assert_eq!(un.payload, p, "len={len} chunk_size={cs}");
            }
        }
    }

    #[test]
    fn truncation_at_any_offset_is_detected() {
        let p: Vec<u8> = (0..300).map(|i| (i % 251) as u8).collect();
        let framed = frame(&p, 9, 64);
        for cut in 0..framed.len() {
            let r = unframe(&framed[..cut]);
            assert!(
                r.is_err(),
                "截断到 {cut}/{} 字节竟被当成完整快照：{:?}",
                framed.len(),
                r.map(|u| u.payload.len())
            );
        }
    }

    #[test]
    fn single_byte_corruption_anywhere_is_detected() {
        // 载荷故意不重复（每个字节唯一）→ 翻转任一字节都必然改变内容/CRC
        let p: Vec<u8> = (0..300u32).map(|i| (i % 251) as u8).collect();
        let framed = frame(&p, 42, 64);
        for i in 0..framed.len() {
            let mut bad = framed.clone();
            bad[i] ^= 0x01;
            assert!(
                unframe(&bad).is_err(),
                "第 {i} 字节翻转未被检出（帧={} 字节；帧头 CRC 覆盖 0..32，块 CRC 覆盖各块数据）",
                framed.len()
            );
        }
    }

    #[test]
    fn trailing_garbage_is_rejected() {
        let p = b"abc".to_vec();
        let mut framed = frame(&p, 1, 0);
        framed.extend_from_slice(b"extra");
        // 残留字节会被当成"下一个块头"→ 块头不足 → Truncated（而不是静默忽略）
        assert!(matches!(
            unframe(&framed),
            Err(SnapshotError::Truncated { .. })
        ));
        // 若残留恰好构成一个合法块头 + 数据，则会被 LengthMismatch 抓到（载荷变多）
        let mut framed = frame(&p, 1, 0);
        framed.extend_from_slice(&3u32.to_le_bytes());
        framed.extend_from_slice(&crc32fast::hash(b"xyz").to_le_bytes());
        framed.extend_from_slice(b"xyz");
        assert!(matches!(
            unframe(&framed),
            Err(SnapshotError::LengthMismatch { .. })
        ));
    }

    #[test]
    fn bad_magic_and_version_are_distinguished() {
        let mut framed = frame(b"x", 1, 0);
        framed[0] = b'X';
        assert!(matches!(unframe(&framed), Err(SnapshotError::BadMagic)));

        // **相对当前版本**取"未来版本"，不要写死数字：
        // 写死会让每次 bump `SNAPSHOT_FORMAT_VERSION` 都把一个正确的实现判成失败
        // （本仓真踩过：`2` 从"未来版本"变成了"当前版本"）。
        let future_ver = SNAPSHOT_FORMAT_VERSION + 1;
        let mut framed = frame(b"x", 1, 0);
        framed[8..12].copy_from_slice(&future_ver.to_le_bytes());
        // 版本变了 → 先撞帧头 CRC（因为我们同时改了被 CRC 覆盖的字节）
        let err = unframe(&framed).unwrap_err();
        assert!(
            matches!(err, SnapshotError::HeaderCrcMismatch { .. }),
            "改版本号应先被帧头 CRC 拦下，实际 {err:?}"
        );
        // 构造一个"版本不符但帧头 CRC 正确"的帧：模拟来自未来版本的合法帧
        let mut future = frame(b"x", 1, 0);
        future[8..12].copy_from_slice(&future_ver.to_le_bytes());
        let hcrc = crc32fast::hash(&future[..SNAPSHOT_HEADER_LEN - 4]);
        future[32..36].copy_from_slice(&hcrc.to_le_bytes());
        assert!(matches!(
            unframe(&future),
            Err(SnapshotError::UnsupportedVersion(v)) if v == future_ver
        ));
    }

    #[test]
    fn chunk_size_zero_is_treated_as_default_and_oversize_is_clamped() {
        // 0 → 默认块大小
        let p = vec![7u8; 10];
        let framed = frame(&p, 1, 0);
        let declared = u32::from_le_bytes(framed[28..32].try_into().unwrap());
        assert_eq!(declared, DEFAULT_CHUNK_SIZE);
        // 超上限 → 截到上限（避免一次分配巨大块缓冲）
        let framed = frame(&p, 1, u32::MAX);
        let declared = u32::from_le_bytes(framed[28..32].try_into().unwrap());
        assert_eq!(declared, MAX_CHUNK_SIZE);
    }
}
