//! **行组级索引文件**（`plan.md` F.4 / `operation-log §145`）。
//!
//! # 它解决什么
//!
//! `§129` 只做到**文件级**剪枝（用 `FileManifest.stats` 的 min/max 决定"这个文件要不要读"）。
//! 文件内部还有一层：数据文件按**组**（`INDEX_GROUP_ROWS` 行一组）分段，同一列在不同组里的
//! 取值范围差别很大时，能证明"这个组不可能命中"就可以**连它的字节都不读**。
//!
//! # 为什么"组"要跟 parquet 的行组对齐
//!
//! 读侧要把"跳过哪些组"交给 DataFusion 的 parquet 读取器（`ParquetAccessPlan`），
//! 而它按**行组序号**说话 ⇒ 我们的组必须与文件里的行组**一一对应**。
//! 所以组大小**不另设常量**：就取 `yuntun-format` 写 parquet 时用的那个行组行数
//! （[`INDEX_GROUP_ROWS`]，写入侧引用它，见 `format::MAX_ROWS_PER_ROW_GROUP`）。
//! 对不上就**不剪**（见读侧的 `groups_len_matches`）—— 硬凑会让 DF 直接报错。
//!
//! # 两组证据
//!
//! | 证据 | 能回答的谓词 | 为什么需要它 |
//! |---|---|---|
//! | **zone map**（每列每组的 min/max） | `<` `<=` `>` `>=` `=` `!=` | 区间谓词；与文件级剪枝**共用同一套判定**（`query::prune::can_prune`） |
//! | **XOR filter**（每列每组一个） | `=` / `IN` | **这一层真正的新增能力**：zone map 只有"组的取值范围"，而值在组内**很稀**时（本项目写入侧并不排序，`event_time` 只是业务约定）区间会很宽 ⇒ 只有 filter 能证明"这个值不在这个组里" |
//!
//! # 纪律
//!
//! 1. **零假阴性**：filter 说"不在"就必须真的不在 —— 这是 XOR filter 的定义性质，
//!    也是我们敢据此**不读字节**的唯一依据（假阳性只是少省一点，无害）；
//! 2. **拿不准不剪**：类型不支持、没有 filter、组数对不上、文件读不出来 ⇒ 一律当"要读"
//!    （剪错的后果是**静默少数据**，本仓最不能接受的失败形态）；
//! 3. **编解码要能自证**：帧头带魔数/版本/长度 + CRC —— 索引是**派生对象**，
//!    坏了就退回"不剪"，绝不能拿半个索引去删组。

use crate::error::LakeError;
use crate::meta::ColumnStatLite;
use arrow::array::Array;
// `encode_to_vec` / `decode`（prost 生成的 trait 方法）
use prost::Message as _;
// `Filter<u64>::contains`（XOR filter 的查询口）
use xorf::Filter as _;

/// 索引文件格式版本（帧头里带；不认识的版本**明确拒绝**而不是猜）。
pub const INDEX_FORMAT_VERSION: u16 = 1;

/// 一组多少行（**必须等于 parquet 的行组行数**）。
///
/// `yuntun-format` 用它设置 `max_row_group_row_count`，我们用同样的切法建索引 ——
/// 一处定义，两处引用（避免"两个常量各自漂移，然后剪错组"）。
pub const INDEX_GROUP_ROWS: usize = 65_536;

/// 索引文件帧头魔数（`YTIX` = Yuntun IndeX）。
const MAGIC: [u8; 4] = *b"YTIX";
/// 帧头长度：magic(4) + version(2) + payload_len(4) + crc32(4)。
const HEADER_LEN: usize = 14;

// ---------------------------------------------------------------- prost 结构

/// 一个索引文件（`{data}.idx` 的对象内容，不含帧头）。
#[derive(Clone, PartialEq, prost::Message)]
pub struct IndexFile {
    /// 建索引时用的组大小（**读侧必须核对**：与当前 [`INDEX_GROUP_ROWS`] 不一致就不剪）
    #[prost(uint32, tag = "1")]
    pub group_rows: u32,
    /// 文件总行数（读侧据此核对"组数 = ceil(行数/组大小)"）
    #[prost(uint64, tag = "2")]
    pub row_count: u64,
    /// 逐组：与文件里的行组**同序**（第 i 项 ↔ 第 i 个行组）
    #[prost(message, repeated, tag = "3")]
    pub groups: Vec<IndexGroup>,
}

/// 一个组（= 一个 parquet 行组）的索引。
#[derive(Clone, PartialEq, prost::Message)]
pub struct IndexGroup {
    /// 每列一份 zone map（min/max/null_count）—— 字段复用 `meta::ColumnStatLite`
    /// （与文件级 `StatisticsLite` **同一套编码**，于是同一套判定函数能用在两级）
    #[prost(message, repeated, tag = "1")]
    pub columns: Vec<ColumnStatLite>,
    /// 等值索引（不是每列都有：类型不支持 / 该组全是 null 就不生成）
    #[prost(message, repeated, tag = "2")]
    pub filters: Vec<ColumnFilter>,
}

/// 一列的 XOR filter（在某个组内）。
#[derive(Clone, PartialEq, prost::Message)]
pub struct ColumnFilter {
    #[prost(string, tag = "1")]
    pub column: String,
    #[prost(uint64, tag = "2")]
    pub seed: u64,
    #[prost(uint64, tag = "3")]
    pub block_length: u64,
    #[prost(bytes = "vec", tag = "4")]
    pub fingerprints: Vec<u8>,
}

// ---------------------------------------------------------------- 键编码

/// 一个可索引的值（**等值谓词**的归一化形态）。
///
/// 为什么不让 `model` 直接吃 DataFusion 的 `ScalarValue`：`model` 是最底层 crate
/// （不依赖 datafusion）⇒ 调用方把**标量**翻译成这个类型，两边共用同一套 `key_hash`
/// —— 建索引与查索引必须算出**同一个 u64**，这件事只能有一处实现。
#[derive(Debug, Clone, PartialEq)]
pub enum IndexKey {
    /// 布尔：**值侧能编码，但字面量侧（`prune::literal_bound`）还不认布尔标量**
    /// ⇒ 目前没有任何一条等值谓词会用到它（留着是为了编码表的完整性，不占索引体积）
    Bool(bool),
    Int(i64),
    UInt(u64),
    Bytes(Vec<u8>),
}

/// 值 → 索引键（`u64`）。
///
/// ⚠️ **这个函数是"索引有效期"的一部分**：索引文件会用**将来的**二进制读回来，
/// 所以这里不许用 `std::hash::DefaultHasher`（其算法**不保证跨 Rust 版本稳定**）——
/// 数值直接取位模式，字节串用 FNV-1a（写死、有测试钉住）。
pub fn key_hash(key: &IndexKey) -> u64 {
    match key {
        IndexKey::Bool(b) => u64::from(*b),
        // 有符号数按位模式（双射，且负数不冲突）
        IndexKey::Int(i) => *i as u64,
        IndexKey::UInt(u) => *u,
        // FNV-1a 64（偏移基与素数取标准值，写死）
        IndexKey::Bytes(b) => {
            let mut h: u64 = 0xcbf2_9ce4_8422_2325;
            for byte in b {
                h ^= u64::from(*byte);
                h = h.wrapping_mul(0x0000_0100_0000_01b3);
            }
            h
        }
    }
}

/// Arrow 数组的第 `i` 个值 → 索引键（`None` = **这个类型我们不索引**，或该值为 null）。
///
/// # 支持哪些类型（这条边界是"会不会剪错"的护栏）
///
/// 只索引"**等值字面量能被 `StatBound` 原样表达、且两边位模式必然一致**"的类型：
///
/// | 支持 | 为什么可以 |
/// |---|---|
/// | `Int8..Int64` / `UInt8..UInt64` | 原始位模式即键（`Int`/`UInt` 字面量与它对得上） |
/// | `Utf8` / `LargeUtf8` / `Binary` / `LargeBinary` | 按字节串哈希（FNV-1a） |
/// | `Date32`（天）/ `Timestamp(4 档刻度)` | 原始计数即键；**读侧必须逐档核对刻度**（`query::prune::scalar_matches_column`） |
///
/// **不支持**：浮点（`=` 有 `NaN` / `-0.0` 边界，删组风险大于收益）、布尔（字面量侧还不认布尔标量
/// ⇒ 建了也没人用，纯占体积）、嵌套/十进制等（无法无损映射到 u64）。
pub fn arrow_key(array: &dyn Array, i: usize) -> Option<IndexKey> {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if array.is_null(i) {
        return None; // null 不满足任何等值谓词 ⇒ 不进过滤集
    }
    Some(match array.data_type() {
        DataType::Int8 => IndexKey::Int(i64::from(
            array.as_any().downcast_ref::<Int8Array>()?.value(i),
        )),
        DataType::Int16 => IndexKey::Int(i64::from(
            array.as_any().downcast_ref::<Int16Array>()?.value(i),
        )),
        DataType::Int32 => IndexKey::Int(i64::from(
            array.as_any().downcast_ref::<Int32Array>()?.value(i),
        )),
        DataType::Int64 => IndexKey::Int(array.as_any().downcast_ref::<Int64Array>()?.value(i)),
        DataType::UInt8 => IndexKey::UInt(u64::from(
            array.as_any().downcast_ref::<UInt8Array>()?.value(i),
        )),
        DataType::UInt16 => IndexKey::UInt(u64::from(
            array.as_any().downcast_ref::<UInt16Array>()?.value(i),
        )),
        DataType::UInt32 => IndexKey::UInt(u64::from(
            array.as_any().downcast_ref::<UInt32Array>()?.value(i),
        )),
        DataType::UInt64 => IndexKey::UInt(array.as_any().downcast_ref::<UInt64Array>()?.value(i)),
        DataType::Date32 => IndexKey::Int(i64::from(
            array.as_any().downcast_ref::<Date32Array>()?.value(i),
        )),
        // 时间戳：取**原始计数**（单位随列类型；同一列单位一致 ⇒ 跨组可比）
        DataType::Timestamp(unit, _) => {
            use arrow::datatypes::TimeUnit;
            let raw = match unit {
                TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()?
                    .value(i),
                TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()?
                    .value(i),
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()?
                    .value(i),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()?
                    .value(i),
            };
            IndexKey::Int(raw)
        }
        DataType::Utf8 => IndexKey::Bytes(
            array
                .as_any()
                .downcast_ref::<StringArray>()?
                .value(i)
                .as_bytes()
                .to_vec(),
        ),
        DataType::LargeUtf8 => IndexKey::Bytes(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()?
                .value(i)
                .as_bytes()
                .to_vec(),
        ),
        DataType::Binary => {
            IndexKey::Bytes(array.as_any().downcast_ref::<BinaryArray>()?.value(i).to_vec())
        }
        DataType::LargeBinary => IndexKey::Bytes(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()?
                .value(i)
                .to_vec(),
        ),
        _ => return None,
    })
}

// ---------------------------------------------------------------- 构建

impl IndexFile {
    /// 按 `columns` 为 `batch` 建索引（**一列没有就返回空 columns**，不是错误）。
    ///
    /// 切组方式与 parquet 写行组的方式逐字相同：第 `i` 组 = 行 `[i*N, min((i+1)*N, rows))`。
    pub fn build(
        batch: &arrow::record_batch::RecordBatch,
        columns: &[String],
    ) -> Result<Self, LakeError> {
        let rows = batch.num_rows();
        let n_groups = rows.div_ceil(INDEX_GROUP_ROWS);
        let mut groups = Vec::with_capacity(n_groups);
        for g in 0..n_groups {
            let start = g * INDEX_GROUP_ROWS;
            let len = INDEX_GROUP_ROWS.min(rows - start);
            let slice = batch.slice(start, len);
            groups.push(build_group(&slice, columns)?);
        }
        Ok(Self {
            group_rows: INDEX_GROUP_ROWS as u32,
            row_count: rows as u64,
            groups,
        })
    }

    /// 组数是否**可信**（读侧挂 `ParquetAccessPlan` 之前的硬前提）。
    ///
    /// 三条一起核对：建索引时的组大小 == 现在这个常量、行数 == manifest 记的行数、
    /// 组数 == 行数按同样切法算出来的组数。任一条不符 ⇒ **不剪**
    /// （DF 会在"计划长度 ≠ 行组数"时直接报错，硬凑等于把查询弄挂）。
    pub fn matches_shape(&self, row_count: u64) -> bool {
        self.group_rows as usize == INDEX_GROUP_ROWS
            && self.row_count == row_count
            && row_count > 0
            && self.groups.len() == (row_count as usize).div_ceil(INDEX_GROUP_ROWS)
    }

    /// 某组某列的等值索引结论：`None` = **判不了**（没有该列的 filter），
    /// `Some(false)` = **可以证明"这个值不在这一组里"**（⇒ 可跳过该组）。
    ///
    /// ⚠️ `Some(false)` 是唯一的"可剪"信号：XOR filter 没有假阴性，所以它说"不在"
    /// 就是真的不在；反过来 `Some(true)` 只代表"可能在"（假阳性），**不可据此做任何事**。
    pub fn definitely_absent(&self, group: usize, column: &str, key: &IndexKey) -> Option<bool> {
        let g = self.groups.get(group)?;
        let f = g.filters.iter().find(|f| f.column == column)?;
        let filter = f.to_xor8()?;
        Some(!filter.contains(&key_hash(key)))
    }

    /// 某组某列的 zone map（读侧用 `query::prune::can_prune` 判区间谓词）。
    pub fn column_stats(&self, group: usize, column: &str) -> Option<&ColumnStatLite> {
        self.groups
            .get(group)?
            .columns
            .iter()
            .find(|c| c.name == column)
    }

    /// 序列化（含帧头 + CRC）。
    ///
    /// 名字刻意不叫 `encode` / `decode`：`prost::Message` 上已经有那两个名字，
    /// 而**固有方法优先于 trait 方法** ⇒ 同名会让 `Self::decode(..)` 递归到自己（栈溢出）。
    pub fn to_bytes(&self) -> Vec<u8> {
        let payload = self.encode_to_vec();
        let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&INDEX_FORMAT_VERSION.to_le_bytes());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        out.extend_from_slice(&payload);
        out
    }

    /// 反序列化：**任何一处不对就报错**（调用方据此退回"不剪"，绝不拿半个索引做决定）。
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, LakeError> {
        fn bad(msg: impl std::fmt::Display) -> LakeError {
            LakeError::Other(format!("索引文件损坏：{msg}"))
        }
        if bytes.len() < HEADER_LEN {
            return Err(bad(format!("长度 {} 不足帧头", bytes.len())));
        }
        if bytes[..4] != MAGIC {
            return Err(bad("魔数不匹配（不是 yuntun 索引文件）"));
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != INDEX_FORMAT_VERSION {
            return Err(bad(format!(
                "不认识的格式版本 {version}（本二进制写/读 {INDEX_FORMAT_VERSION}）"
            )));
        }
        let len = u32::from_le_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]) as usize;
        let crc = u32::from_le_bytes([bytes[10], bytes[11], bytes[12], bytes[13]]);
        let payload = &bytes[HEADER_LEN..];
        if payload.len() != len {
            return Err(bad(format!(
                "载荷长度不符：帧头说 {len}，实际 {}",
                payload.len()
            )));
        }
        let actual = crc32fast::hash(payload);
        if actual != crc {
            return Err(bad(format!("CRC 不符：存 {crc:#010x}，算 {actual:#010x}")));
        }
        Self::decode(payload).map_err(|e| bad(format!("载荷解不开：{e}")))
    }
}

impl ColumnFilter {
    /// 重建 XOR filter（`xorf::Xor8` 的字段是公开的 ⇒ 不需要 serde/bincode 依赖）。
    ///
    /// 结构不合法（`fingerprints` 长度与 `block_length` 对不上）⇒ `None`：
    /// 这种"半个 filter"绝不能参与判定 —— `contains` 会越界 panic，而那是**读取路径**。
    fn to_xor8(&self) -> Option<xorf::Xor8> {
        let block_length = usize::try_from(self.block_length).ok()?;
        if block_length == 0 || self.fingerprints.len() != block_length * 3 {
            return None;
        }
        Some(xorf::Xor8 {
            seed: self.seed,
            block_length,
            fingerprints: self.fingerprints.clone().into_boxed_slice(),
        })
    }
}

/// 建一个组的索引：逐列算 zone map（复用文件级那套）+ 尽力建等值 filter。
fn build_group(
    group: &arrow::record_batch::RecordBatch,
    columns: &[String],
) -> Result<IndexGroup, LakeError> {
    let stats = crate::meta::compute_stats_lite(group, columns)?;
    let mut filters = Vec::new();
    for col in columns {
        let Ok(idx) = group.schema().index_of(col) else {
            continue;
        };
        let array = group.column(idx).as_ref();
        // 去重：XOR filter 要求键**互不相同**（重复键会让构造失败/退化），
        // 而且去重本身就让 filter 更小。`BTreeSet` 而不是 `HashSet`：构建确定（纪律 2）
        let mut keys: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
        for i in 0..array.len() {
            if let Some(k) = arrow_key(array, i) {
                keys.insert(key_hash(&k));
            }
        }
        if keys.is_empty() {
            // 全 null / 类型不支持 ⇒ 不生成 filter（zone map 里已经记了 null_count，
            // "整列全 null ⇒ 比较谓词必不成立"由文件级同一套规则处理）
            continue;
        }
        let key_vec: Vec<u64> = keys.into_iter().collect();
        let filter = xorf::Xor8::from_iterator(key_vec.iter().copied());
        filters.push(ColumnFilter {
            column: col.clone(),
            seed: filter.seed,
            block_length: filter.block_length as u64,
            fingerprints: filter.fingerprints.into_vec(),
        });
    }
    Ok(IndexGroup {
        columns: stats.columns,
        filters,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn batch(rows: usize) -> RecordBatch {
        // event_time：**故意不排序**（真实写入形态）：偶数在 0..1000，奇数在 1_000_000..
        let ts: Vec<i64> = (0..rows)
            .map(|i| if i % 2 == 0 { i as i64 } else { 1_000_000 + i as i64 })
            .collect();
        let user: Vec<String> = (0..rows).map(|i| format!("u{}", i % 50)).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_time", DataType::Int64, true),
            Field::new("user", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ts)),
                Arc::new(StringArray::from(user)),
            ],
        )
        .unwrap()
    }

    fn cols() -> Vec<String> {
        vec!["event_time".to_string(), "user".to_string()]
    }

    #[test]
    fn build_splits_groups_like_parquet_row_groups() {
        let b = batch(INDEX_GROUP_ROWS + 10);
        let idx = IndexFile::build(&b, &cols()).unwrap();
        assert_eq!(idx.group_rows, INDEX_GROUP_ROWS as u32);
        assert_eq!(idx.row_count as usize, INDEX_GROUP_ROWS + 10);
        assert_eq!(idx.groups.len(), 2, "两个行组：满组 + 尾巴");
        assert!(idx.matches_shape(INDEX_GROUP_ROWS as u64 + 10));
        // 尾巴组只该有 10 行（zone map 的 null_count 为 0，min/max 出自那 10 行）
        assert_eq!(idx.groups[1].columns[0].name, "event_time");
    }

    /// **零假阴性**：组里有的值，filter 必须说"可能有"。
    ///
    /// 这条是整个索引的立身之本 —— 反过来说"不在"才敢不读字节。
    ///
    /// ⚠️ 探针只能喂**确实在组里**的值（第一版顺手写成 `1_000_000 + i`，而偶数位把
    /// `i` 留在低位 ⇒ 那些值根本不在数据里，用例因此红了**一次**：是**测试错**，
    /// 不是索引错 —— 但这条被写在这里，因为"零假阴性"的用例本身挂一次很值得记）。
    #[test]
    fn xor_filter_has_no_false_negatives() {
        let b = batch(5000);
        let idx = IndexFile::build(&b, &cols()).unwrap();
        for i in 0..5000usize {
            // 与 `batch()` 的构造**逐字对应**：偶数是 i，奇数是 1_000_000 + i
            let ts = if i % 2 == 0 {
                i as i64
            } else {
                1_000_000 + i as i64
            };
            for (col, key) in [
                ("event_time", IndexKey::Int(ts)),
                (
                    "user",
                    IndexKey::Bytes(format!("u{}", i % 50).into_bytes()),
                ),
            ] {
                assert_ne!(
                    idx.definitely_absent(0, col, &key),
                    Some(true),
                    "组里明明有 {key:?}，filter 却说不在（假阴性）= 会静默少数据"
                );
            }
        }
    }

    /// 真的能证明"不在"：组里没有的值 ⇒ `Some(true)`（否则索引没有任何用处）。
    #[test]
    fn xor_filter_proves_absence() {
        // 只有偶数（0..1000）⇒ 奇数 999 必然不在
        let ts: Vec<i64> = (0..1000).map(|i| i * 2).collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "event_time",
            DataType::Int64,
            true,
        )]));
        let b = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ts))]).unwrap();
        let idx = IndexFile::build(&b, &["event_time".to_string()]).unwrap();
        assert_eq!(
            idx.definitely_absent(0, "event_time", &IndexKey::Int(999)),
            Some(true),
            "999 不在组里（全是偶数）⇒ 必须能证明"
        );
        assert_eq!(
            idx.definitely_absent(0, "event_time", &IndexKey::Int(998)),
            Some(false),
            "998 在组里 ⇒ 不许说不在"
        );
        // 没有 filter 的列 ⇒ 判不了（`None`，调用方必须当"要读"）
        assert_eq!(idx.definitely_absent(0, "nope", &IndexKey::Int(1)), None);
        // 越界组号 ⇒ 一样判不了
        assert_eq!(idx.definitely_absent(9, "event_time", &IndexKey::Int(1)), None);
    }

    /// 全 null / 不支持类型 ⇒ 不生成 filter（拿不准不剪）。
    #[test]
    fn no_filter_for_all_null_or_unsupported_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
        ]));
        let b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![None, None])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0])),
            ],
        )
        .unwrap();
        let idx = IndexFile::build(&b, &["a".to_string(), "f".to_string()]).unwrap();
        assert!(idx.groups[0].filters.is_empty(), "全 null 与 float 都不该有 filter");
        assert_eq!(idx.definitely_absent(0, "a", &IndexKey::Int(1)), None);
        assert_eq!(idx.definitely_absent(0, "f", &IndexKey::Int(1)), None);
    }

    /// 时间戳列可取（原始计数）—— 这是本项目最常见的排序列类型。
    #[test]
    fn timestamp_columns_are_indexable() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "event_time",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        )]));
        let b = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampNanosecondArray::from(vec![10i64, 20]))],
        )
        .unwrap();
        let idx = IndexFile::build(&b, &["event_time".to_string()]).unwrap();
        assert_eq!(
            idx.definitely_absent(0, "event_time", &IndexKey::Int(20)),
            Some(false)
        );
        assert_eq!(
            idx.definitely_absent(0, "event_time", &IndexKey::Int(30)),
            Some(true)
        );
    }

    /// 编解码往返逐字节一致（同样的数据两次构建也一样 ⇒ 确定性的派生对象）。
    #[test]
    fn encode_decode_round_trip_is_deterministic() {
        let b = batch(300);
        let a = IndexFile::build(&b, &cols()).unwrap();
        let c = IndexFile::build(&b, &cols()).unwrap();
        let (ba, bc) = (a.to_bytes(), c.to_bytes());
        assert_eq!(ba, bc, "同样的数据必须产出同样的索引字节");
        let back = IndexFile::from_bytes(&ba).unwrap();
        assert_eq!(back, a);
    }

    /// 帧保护：魔数 / 版本 / 长度 / CRC 任一不对都要**明确拒绝**（不是猜）。
    #[test]
    fn tampered_or_truncated_index_is_rejected() {
        let b = batch(10);
        let bytes = IndexFile::build(&b, &cols()).unwrap().to_bytes();

        // 截断
        assert!(IndexFile::from_bytes(&bytes[..HEADER_LEN - 1]).is_err());
        assert!(IndexFile::from_bytes(&bytes[..bytes.len() - 1]).is_err(), "载荷长度不符");
        // 魔数
        let mut m = bytes.clone();
        m[0] = b'X';
        assert!(IndexFile::from_bytes(&m).is_err());
        // 版本
        let mut v = bytes.clone();
        v[4] = 99;
        assert!(IndexFile::from_bytes(&v).is_err());
        // CRC（翻转载荷的任意一个字节）
        let mut c = bytes.clone();
        let last = c.len() - 1;
        c[last] ^= 0xff;
        let e = IndexFile::from_bytes(&c).unwrap_err().to_string();
        assert!(e.contains("CRC"), "CRC 不符要被点名：{e}");
    }

    /// `key_hash` 的**稳定性**：数值取位模式、字节串走 FNV-1a —— 写死并钉住。
    ///
    /// 为什么单独钉：索引文件会被**将来的**二进制读回来，哈希算法一变，
    /// 老索引就会"静默剪错组"（说不在、其实在）。所以这里断言的是**具体数字**。
    #[test]
    fn key_hash_is_stable_and_documented() {
        assert_eq!(key_hash(&IndexKey::Int(42)), 42);
        assert_eq!(key_hash(&IndexKey::Int(-1)), u64::MAX);
        assert_eq!(key_hash(&IndexKey::UInt(7)), 7);
        assert_eq!(key_hash(&IndexKey::Bool(true)), 1);
        // FNV-1a 64("") = 偏移基；"a" = 0xaf63dc4c8601ec8c
        assert_eq!(key_hash(&IndexKey::Bytes(vec![])), 0xcbf2_9ce4_8422_2325);
        assert_eq!(key_hash(&IndexKey::Bytes(b"a".to_vec())), 0xaf63_dc4c_8601_ec8c);
    }

    /// 键编码：null ⇒ `None`（null 不满足任何等值谓词），类型不支持 ⇒ `None`。
    #[test]
    fn arrow_key_skips_nulls_and_unsupported_types() {
        let a = Int64Array::from(vec![Some(1), None]);
        assert_eq!(arrow_key(&a, 0), Some(IndexKey::Int(1)));
        assert_eq!(arrow_key(&a, 1), None);
        let f = arrow::array::Float64Array::from(vec![1.0]);
        assert_eq!(arrow_key(&f, 0), None, "浮点不索引");
    }

    /// 形状核对：组大小/行数/组数任一不符 ⇒ `matches_shape` 为 false（读侧据此不剪）。
    #[test]
    fn matches_shape_rejects_mismatches() {
        let b = batch(INDEX_GROUP_ROWS + 1);
        let mut idx = IndexFile::build(&b, &cols()).unwrap();
        assert!(idx.matches_shape(INDEX_GROUP_ROWS as u64 + 1));
        assert!(!idx.matches_shape(INDEX_GROUP_ROWS as u64), "行数不符");
        assert!(!idx.matches_shape(0), "零行（空文件）不剪");
        idx.group_rows = 1;
        assert!(!idx.matches_shape(INDEX_GROUP_ROWS as u64 + 1), "组大小不符");
    }

    /// 结构不完整的 filter 不参与判定（宁可判不了，也不许 panic 在读取路径上）。
    #[test]
    fn malformed_filter_is_not_usable() {
        let f = ColumnFilter {
            column: "c".into(),
            seed: 1,
            block_length: 10,
            fingerprints: vec![0; 5], // 应为 3*block_length = 30
        };
        assert!(f.to_xor8().is_none());
    }
}
