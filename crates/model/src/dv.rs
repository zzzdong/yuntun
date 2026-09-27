//! **删除向量（deletion vector，DV）**（`plan.md` F.3 / `delta-dml-design.md` §2–§3）。
//!
//! # 为什么是"行位删除"而不是别的
//!
//! 数据文件**不可变**（`ADR-1`）⇒ 行级删除只能"标记"。形态决策（`delta-dml-design §2`）：
//! 用 `(file_path, row_idx)` 的位图 —— 写廉价（不重写基文件）、读侧 merge-on-read。
//! equality-delete 需要主键、copy-on-write 写放大不可控，都已被设计否掉。
//!
//! # 这一层管什么
//!
//! 只管**两件事**，都不碰目录与执行：
//!
//! 1. [`DeletionEntry`]：一次删除在**快照维度**上的登记（`applied_at` / `revoked_at`）——
//!    它让"已提交但尚未 compaction 的删除"**对旧快照不可见**（快照隔离，`F.3` 验收②）；
//! 2. [`DvBitmap`]：一个数据文件里**被删掉的行号集合**（roaring 位图 + 自证帧）。
//!
//! 读侧要的那一步"从 DV 得到**保留**哪些行"也在这一层（[`DvBitmap::keep_ranges`]）——
//! 因为"哪些行留下"与"哪些行被删"是**同一个事实的两种说法**，分开放两个 crate 一定会漂移
//! （而漂移的后果是**静默少数据 / 静默复活已删行**，本仓最不能接受的两类失败）。
//!
//! # 纪律
//!
//! * **帧要能自证**：魔数 / 版本 / 长度 / CRC —— DV 解不开时，读侧**必须报错**，
//!   不许"当没有删除"（那就是**复活已删数据**，比少数据更糟：用户以为删了）；
//! * **越界即错**：位图里出现 `>= row_count` 的行号 ⇒ 这份 DV 与这个文件**对不上**
//!   （文件被换过 / 位图损坏）⇒ 报错，不许截断后继续（截断 = 悄悄放行一部分已删行）。

use crate::error::LakeError;

/// DV 文件的格式版本（帧头里带；不认识的版本**明确拒绝**）。
pub const DV_FORMAT_VERSION: u16 = 1;
/// 帧头魔数（`YTDV`）。
const MAGIC: [u8; 4] = *b"YTDV";
/// 帧头长度：magic(4) + version(2) + payload_len(4) + crc32(4)。
const HEADER_LEN: usize = 14;

/// DV 文件的对象路径（`delta-dml-design.md §3.1`）：
///
/// ```text
/// yuntun/<schema>/<table>/dt=…/shard=…/dv/<数据文件名含扩展名>/<dv_id>.bin
/// ```
///
/// 两条刻意的细节：
///
/// * **数据文件名含扩展名**（`x.parquet`）—— 与孤儿清理 `extract_batch_id` 取文件名 stem
///   的口径**不同**：那里要的是 batch_id（对账键），这里要的是"这份 DV 锚定哪个文件"，
///   两者不能混（`delta-dml-design §6.2` 明确警告过"不得把 dv_id 误当 batch_id"）；
/// * `dv/` 是一层**显式的分流前缀**：孤儿清理/对账要能一眼把人造对象与数据对象分开。
pub fn dv_object_path(data_path: &str, dv_id: &str) -> String {
    let (dir, name) = match data_path.rsplit_once('/') {
        Some((d, n)) => (d, n),
        None => ("", data_path),
    };
    format!("{dir}/dv/{name}/{dv_id}.bin")
}

/// 一次删除在**快照维度**上的登记（`delta-dml-design §3.2`）。
///
/// 行可见性 = 文件可见 **AND** 行号 ∉ {`applied_at <= snapshot < revoked_at` 的 DV 并集}。
/// 于是：
///
/// * 删除**提交之前**开始的查询（快照更早）自动看到未删除状态 ⇒ 快照隔离成立；
/// * 删除提交**之后**开始的查询看不到那些行；
/// * `revoked_at != 0` 表示 compaction 已经把这份 DV 物理消费掉（行已被重写掉）——
///   **保留行不删**：它让"某个快照当时看到什么"可回答（`§3.2` 的 revoke 语义）。
#[derive(Clone, PartialEq, prost::Message)]
pub struct DeletionEntry {
    /// 幂等 id（= 触发它的 WAL 记录 id）；重放按它去重
    #[prost(string, tag = "1")]
    pub dv_id: String,
    /// **全限定**表名（`schema.table`）—— 设计里没写这一条，但目录里每一类记录都按表分区，
    /// 而 `list_deletions(table, snapshot)` 是读侧唯一的入口（不给表名就得全表扫描）
    #[prost(string, tag = "2")]
    pub table: String,
    /// 锚定的数据文件（对象路径，含扩展名）
    #[prost(string, tag = "3")]
    pub file_path: String,
    /// 归属批次（孤儿清理对账用：`dv → file_path → batch_id`）
    #[prost(string, tag = "4")]
    pub batch_id: String,
    /// 快照号：`query_snapshot >= applied_at` 才应用本 DV
    #[prost(uint64, tag = "5")]
    pub applied_at: u64,
    /// 0 = 生效中；compaction 消费后置为快照号
    #[prost(uint64, tag = "6")]
    pub revoked_at: u64,
    /// 被删行数（= 位图基数；统计口径 `§5.4`：`row_count - card`）
    #[prost(uint32, tag = "7")]
    pub card: u32,
    /// 位图对象路径（`dv_object_path(file_path, dv_id)`）
    #[prost(string, tag = "8")]
    pub store_path: String,
}

impl DeletionEntry {
    /// 这份 DV 在 `snapshot` 这个快照下**生效**吗。
    pub fn active_at(&self, snapshot: u64) -> bool {
        self.applied_at <= snapshot && (self.revoked_at == 0 || snapshot < self.revoked_at)
    }
}

/// 一个数据文件的行号位图（roaring）+ 自证帧。
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DvBitmap {
    bits: roaring::RoaringBitmap,
}

impl DvBitmap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_positions(positions: impl IntoIterator<Item = u32>) -> Self {
        let mut bits = roaring::RoaringBitmap::new();
        for p in positions {
            bits.insert(p);
        }
        Self { bits }
    }

    pub fn is_empty(&self) -> bool {
        self.bits.is_empty()
    }

    pub fn card(&self) -> u64 {
        self.bits.len()
    }

    pub fn contains(&self, position: u32) -> bool {
        self.bits.contains(position)
    }

    /// 最大被删行号（空位图 ⇒ `None`）。
    pub fn max(&self) -> Option<u32> {
        self.bits.iter().next_back()
    }

    /// **保留哪些行**（补集，升序、互不重叠、恰好覆盖 `0..row_count`）。
    ///
    /// 这是读侧唯一需要的形状：[`parquet` 的整体 `RowSelection`] 就是"保留区间 + 自动补 skip"，
    /// 而 DF 要求选择器覆盖**文件的全部行**（行数不符直接报错，见
    /// `ParquetAccessPlan::try_new_from_overall_row_selection`）。
    ///
    /// # Errors
    ///
    /// 位图里有 `>= row_count` 的行号 ⇒ 这份 DV 与这个文件**对不上** ⇒ 报错。
    /// 不许"截断后继续"：那会**悄悄放行一部分已删行**（用户以为删了、其实还在）。
    pub fn keep_ranges(&self, row_count: u64) -> Result<Vec<std::ops::Range<usize>>, LakeError> {
        if let Some(max) = self.max()
            && u64::from(max) >= row_count
        {
            return Err(LakeError::Other(format!(
                "删除向量与文件对不上：位图里有第 {max} 行，而文件只有 {row_count} 行\
                 （DV 被张冠李戴或位图损坏）—— 拒绝继续（继续就会放行已删的行）"
            )));
        }
        let total = row_count as usize;
        let mut out = Vec::new();
        let mut start = 0usize;
        for p in self.bits.iter() {
            let p = p as usize;
            if p > start {
                out.push(start..p);
            }
            start = p + 1;
        }
        if start < total {
            out.push(start..total);
        }
        Ok(out)
    }

    /// 合并（同文件多份 DV 先 OR 再算 —— `§5.4` 的统计口径要求）。
    pub fn union_with(&mut self, other: &DvBitmap) {
        self.bits |= &other.bits;
    }

    /// 序列化（含帧头 + CRC）。
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        self.bits
            .serialize_into(&mut payload)
            .expect("位图序列化到 Vec 不会失败");
        let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&DV_FORMAT_VERSION.to_le_bytes());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        out.extend_from_slice(&payload);
        out
    }

    /// 反序列化：**任何一处不对就报错**（调用方据此**让查询失败**，不许当"没有删除"）。
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, LakeError> {
        fn bad(msg: impl std::fmt::Display) -> LakeError {
            LakeError::Other(format!("删除向量损坏：{msg}"))
        }
        if bytes.len() < HEADER_LEN {
            return Err(bad(format!("长度 {} 不足帧头", bytes.len())));
        }
        if bytes[..4] != MAGIC {
            return Err(bad("魔数不匹配（不是 yuntun 删除向量）"));
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != DV_FORMAT_VERSION {
            return Err(bad(format!(
                "不认识的格式版本 {version}（本二进制写/读 {DV_FORMAT_VERSION}）"
            )));
        }
        let len = u32::from_le_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]) as usize;
        let crc = u32::from_le_bytes([bytes[10], bytes[11], bytes[12], bytes[13]]);
        let payload = &bytes[HEADER_LEN..];
        if payload.len() != len {
            return Err(bad(format!("载荷长度不符：帧头说 {len}，实际 {}", payload.len())));
        }
        let actual = crc32fast::hash(payload);
        if actual != crc {
            return Err(bad(format!("CRC 不符：存 {crc:#010x}，算 {actual:#010x}")));
        }
        let bits = roaring::RoaringBitmap::deserialize_from(payload)
            .map_err(|e| bad(format!("位图解不开：{e}")))?;
        Ok(Self { bits })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Range;

    fn ranges(v: &DvBitmap, rows: u64) -> Vec<Range<usize>> {
        v.keep_ranges(rows).unwrap()
    }

    #[test]
    fn empty_dv_keeps_everything() {
        let dv = DvBitmap::new();
        assert!(dv.is_empty());
        assert_eq!(ranges(&dv, 5), vec![0..5], "没有删除 ⇒ 保留全部（一个连续区间）");
        assert!(ranges(&dv, 0).is_empty(), "零行文件");
    }

    /// **核心**：删掉的行必须**恰好**从保留集里消失，其它一行不多不少。
    #[test]
    fn keep_ranges_is_the_exact_complement() {
        let dv = DvBitmap::from_positions([0, 2, 2, 4]); // 重复位置 = 幂等
        assert_eq!(dv.card(), 3);
        assert_eq!(ranges(&dv, 5), vec![1..2, 3..4], "删 0/2/4 ⇒ 保留 1、3");
        // 尾部删除不影响前面
        let dv = DvBitmap::from_positions([4]);
        assert_eq!(ranges(&dv, 5), vec![0..4]);
        // 头部连续删除
        let dv = DvBitmap::from_positions([0, 1, 2]);
        assert_eq!(ranges(&dv, 5), vec![3..5]);
        // 全删
        let dv = DvBitmap::from_positions(0..4);
        assert!(ranges(&dv, 4).is_empty(), "整文件删光 ⇒ 保留集为空");
    }

    /// 保留区间的**总行数**必须恰好等于 `row_count - card`（DF 的硬要求：选择器覆盖全部行）。
    #[test]
    fn keep_ranges_covers_exactly_the_survivors() {
        for rows in [1u64, 7, 100, 1000] {
            let deleted: Vec<u32> = (0..rows as u32).filter(|p| p % 3 == 0).collect();
            let dv = DvBitmap::from_positions(deleted.clone());
            let rs = ranges(&dv, rows);
            let covered: usize = rs.iter().map(|r| r.len()).sum();
            assert_eq!(
                covered,
                rows as usize - deleted.len(),
                "保留区间的行数必须 = 总行数 - 删除数（rows={rows}）"
            );
            // 且与"逐行判定"逐行一致（两次不同实现互相对拍）
            let flat: Vec<usize> = rs.iter().flat_map(|r| r.clone()).collect();
            let expect: Vec<usize> = (0..rows as usize)
                .filter(|p| !deleted.contains(&(*p as u32)))
                .collect();
            assert_eq!(flat, expect, "保留区间展开后必须与逐行判定完全一致");
        }
    }

    #[test]
    fn out_of_range_position_is_rejected_loudly() {
        let dv = DvBitmap::from_positions([0, 9]);
        let e = dv.keep_ranges(5).unwrap_err().to_string();
        assert!(e.contains("对不上"), "越界必须点名原因：{e}");
        assert!(e.contains("放行"), "说清后果（放行已删的行）：{e}");
        // 恰好等于 row_count 也是越界（行号从 0 起）
        assert!(DvBitmap::from_positions([5]).keep_ranges(5).is_err());
    }

    #[test]
    fn union_merges_two_deletions_on_the_same_file() {
        let mut a = DvBitmap::from_positions([1]);
        let b = DvBitmap::from_positions([3, 1]);
        a.union_with(&b);
        assert_eq!(a.card(), 2);
        assert_eq!(ranges(&a, 4), vec![0..1, 2..3]);
    }

    #[test]
    fn frame_round_trip_and_tamper_rejection() {
        let dv = DvBitmap::from_positions([1, 5, 900_000]);
        let bytes = dv.to_bytes();
        assert_eq!(DvBitmap::from_bytes(&bytes).unwrap(), dv);

        // 截断
        assert!(DvBitmap::from_bytes(&bytes[..HEADER_LEN - 1]).is_err());
        // 魔数
        let mut m = bytes.clone();
        m[0] = b'X';
        assert!(DvBitmap::from_bytes(&m).is_err());
        // 版本
        let mut v = bytes.clone();
        v[4] = 99;
        assert!(DvBitmap::from_bytes(&v).is_err());
        // CRC（翻载荷最后一个字节）
        let mut c = bytes.clone();
        let last = c.len() - 1;
        c[last] ^= 0xff;
        let e = DvBitmap::from_bytes(&c).unwrap_err().to_string();
        assert!(e.contains("CRC"), "CRC 不符要点名：{e}");
    }

    /// 对象路径：`dv/<数据文件名含扩展名>/<dv_id>.bin`，且**不含** batch_id 的口径。
    #[test]
    fn object_path_shape() {
        let p = dv_object_path("yuntun/public/t/dt=w/shard=s0/b7.parquet", "dv-1");
        assert_eq!(p, "yuntun/public/t/dt=w/shard=s0/dv/b7.parquet/dv-1.bin");
        assert!(p.contains("/dv/"), "`dv/` 是孤儿清理的显式分流前缀");
        assert!(p.ends_with("b7.parquet/dv-1.bin"), "锚定完整文件名（含扩展名）");
    }

    #[test]
    fn active_at_follows_snapshot_window() {
        let e = DeletionEntry {
            dv_id: "dv-1".into(),
            table: "public.t".into(),
            file_path: "f.parquet".into(),
            batch_id: "b1".into(),
            applied_at: 10,
            revoked_at: 0,
            card: 3,
            store_path: "p".into(),
        };
        assert!(!e.active_at(9), "提交前开始的查询看不到这次删除（快照隔离）");
        assert!(e.active_at(10));
        assert!(e.active_at(u64::MAX), "revoked_at = 0 ⇒ 一直生效");
        let revoked = DeletionEntry {
            revoked_at: 12,
            ..e
        };
        assert!(revoked.active_at(11));
        assert!(!revoked.active_at(12), "compaction 消费后不再生效（行已被重写掉）");
    }
}
