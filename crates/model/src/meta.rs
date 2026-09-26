//! Catalog 元数据 prost 消息（详细设计 §6.2）。
//!
//! 即使阶段 0 单节点，接口也按 Raft 线性一致性语义设计（§6.1），
//! 消息结构直接对齐架构 §5.1，阶段 1 切换 gRPC 零改动。

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::convert::{fb_to_schema, IpcSchemaEncoder};
use arrow::ipc::root_as_schema;
use std::sync::Arc;

/// Arrow Schema ↔ IPC 字节互转（SchemaVersion.arrow_schema 的载体）。
pub fn serialize_schema(schema: &SchemaRef) -> Vec<u8> {
    IpcSchemaEncoder::new()
        .schema_to_fb(schema)
        .finished_data()
        .to_vec()
}

pub fn deserialize_schema(bytes: &[u8]) -> Result<SchemaRef, crate::error::LakeError> {
    let fb = root_as_schema(bytes)
        .map_err(|e| crate::error::LakeError::Other(format!("decode schema: {e}")))?;
    Ok(Arc::new(fb_to_schema(fb)))
}

/// 表元数据（架构 §5.1 TableMeta）
#[derive(Clone, PartialEq, prost::Message)]
pub struct TableMeta {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(uint64, tag = "2")]
    pub current_schema_version: u64,
    #[prost(string, repeated, tag = "3")]
    pub partition_cols: Vec<String>,
    /// "vortex" | "parquet"（ADR-1 FormatSwitch 回退开关）
    #[prost(string, tag = "4")]
    pub default_format: String,
    #[prost(message, optional, tag = "5")]
    pub ingest_config: Option<IngestConfig>,
    #[prost(uint64, tag = "6")]
    pub created_at: u64,
    /// v1 扩展：完整 Arrow Schema（IPC 序列化），随 SchemaVersion 链演进
    #[prost(bytes = "vec", tag = "7")]
    pub arrow_schema: Vec<u8>,
    /// v1 扩展：表模板（0=Audit 1=General 2=Metrics 3=Traces）
    #[prost(uint32, tag = "8")]
    pub table_template: u32,
    /// **多 schema 扩展**：所属 schema（MySQL 的 database 概念）；空字符串视作
    /// [`crate::ops::DEFAULT_SCHEMA`]（向后兼容 v1 单 schema 数据）。
    #[prost(string, tag = "9")]
    pub namespace: String,
}

impl TableMeta {
    /// 表所属 schema（空值 → 默认 `public`，兼容旧数据）。
    pub fn schema_name(&self) -> &str {
        if self.namespace.is_empty() {
            crate::ops::DEFAULT_SCHEMA
        } else {
            &self.namespace
        }
    }

    /// 全限定表标识 `schema.table`（跨层唯一表标识：Catalog / Ingest / WAL / 对象路径）。
    pub fn qualified_name(&self) -> String {
        crate::ops::qualified_name(self.schema_name(), &self.name)
    }

    pub fn schema(&self) -> Result<SchemaRef, crate::error::LakeError> {
        deserialize_schema(&self.arrow_schema)
    }

    pub fn with_schema(mut self, schema: &SchemaRef) -> Self {
        self.arrow_schema = serialize_schema(schema);
        self
    }
}

/// 表级摄入配置（架构 §4-ADR-9 / §7.2 / §7.3.2）
#[derive(Clone, PartialEq, prost::Message)]
pub struct IngestConfig {
    /// 【v9】幂等键默认开启；Metrics/Traces 模板可关闭（§7.3.2）
    #[prost(bool, tag = "1", default = true)]
    pub require_idempotency_key: bool,
    /// 幂等键 TTL，默认 24h（秒）
    #[prost(uint64, tag = "2")]
    pub idempotency_ttl_secs: u64,
    /// 攒批行数阈值（默认 10000，§7.2）
    #[prost(uint64, tag = "3")]
    pub rows_threshold: u64,
    /// 攒批时间阈值（默认 5s，秒）
    #[prost(uint64, tag = "4")]
    pub time_threshold_secs: u64,
    /// 持久性 SLA（ADR-9）：0=best_effort 1=durable
    #[prost(uint32, tag = "5")]
    pub durability: u32,
}

impl IngestConfig {
    /// 默认值（中吞吐通用表，§7.2 表格）。
    pub fn standard() -> Self {
        Self {
            require_idempotency_key: true,
            idempotency_ttl_secs: 24 * 3600,
            rows_threshold: 10_000,
            time_threshold_secs: 5,
            durability: 0,
        }
    }

    pub fn from_template(t: crate::TableTemplate) -> Self {
        let mut c = Self::standard();
        c.require_idempotency_key = t.require_idempotency_key();
        c
    }
}

/// Schema 版本链（架构 §5.1 SchemaVersion）
#[derive(Clone, PartialEq, prost::Message)]
pub struct SchemaVersion {
    /// 单调递增
    #[prost(uint64, tag = "1")]
    pub version: u64,
    /// 该版本的完整 Arrow Schema（IPC 序列化）
    #[prost(bytes = "vec", tag = "2")]
    pub arrow_schema: Vec<u8>,
    /// 0=ADD_COLUMN 1=WIDEN_TYPE 2=DROP_COLUMN
    #[prost(uint32, tag = "3")]
    pub change_kind: u32,
    #[prost(uint64, tag = "4")]
    pub created_at: u64,
    /// 人类可读，如 "add column user_agent: Utf8"
    #[prost(string, tag = "5")]
    pub change_desc: String,
}

impl SchemaVersion {
    pub fn schema(&self) -> Result<SchemaRef, crate::error::LakeError> {
        deserialize_schema(&self.arrow_schema)
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct FileManifest {
    #[prost(string, tag = "1")]
    pub file_path: String,
    /// 幂等主键（ADR-4：随机 UUIDv7）
    #[prost(string, tag = "2")]
    pub batch_id: String,
    /// 客户端幂等键，唯一索引（可空，§7.3）
    #[prost(string, tag = "3")]
    pub client_request_id: String,
    /// 该文件写入时的 Schema 版本（§5.1；不同文件可有不同版本，C8）
    #[prost(uint64, tag = "4")]
    pub schema_version: u64,
    /// 0=ACTIVE 1=STAGED 2=DELETED
    #[prost(uint32, tag = "5")]
    pub status: u32,
    /// 快照号：valid_from <= query_snapshot 才可见
    #[prost(uint64, tag = "6")]
    pub valid_from: u64,
    /// 0 = 未删除；query_snapshot < deleted_at 才可见
    #[prost(uint64, tag = "7")]
    pub deleted_at: u64,
    #[prost(message, optional, tag = "8")]
    pub stats: Option<StatisticsLite>,
    #[prost(uint64, tag = "9")]
    pub row_count: u64,
    #[prost(uint64, tag = "10")]
    pub file_size: u64,
    /// v1 扩展：所属表
    #[prost(string, tag = "11")]
    pub table: String,
    /// v1 扩展：所属 shard
    #[prost(string, tag = "12")]
    pub shard: String,
    /// v1 扩展：时间窗口（整分钟对齐，ADR-10）
    #[prost(string, tag = "13")]
    pub time_window: String,
    /// **逻辑分区键 `dt`**（架构 §2.3）：partition 是逻辑身份，file 是物理身份，**不得等同** ——
    /// compaction 会合并文件，若两者等同则每次合并后 partition 集合都变化。
    #[prost(string, tag = "14")]
    pub partition_key: String,
    /// **写入该文件的 datanode 实例**（架构 §4.4）：多个 datanode 各自 flush，
    /// "已 flush 到哪"是**每实例各自的版本**；冷热边界必须按实例二维切分，
    /// 否则会出现"同一批数据被读两次"的重复计数（极难排查）。**此字段必须提前加**，
    /// 事后再加需要回填历史 manifest。
    #[prost(string, tag = "15")]
    pub source_instance: String,
    /// **本次 flush 启动的时刻**（chunk 封口 → 开始落盘，Unix 毫秒）。
    ///
    /// 与 `committed_at_ms` 之差 = 该文件从"写侧结束"到"持久化完成"的实际耗时
    /// （= `max_flush_delay + phase + 对象存储 PUT + CommitFiles`）。
    /// 这是对外承诺"数据 X 秒内持久"的**可核验口径** —— 无此字段只能靠推算。
    #[prost(uint64, tag = "16")]
    pub sealed_at_ms: u64,
    /// **提交 Meta 成功**的时刻（Unix 毫秒）。
    ///
    /// 用途：① 运维回答"这个文件什么时候提交的"；② T8 基线（提交时刻分布 →
    /// 惊群峰值 / 文件数·天 / 持久化 P99）。**不得**用 `deleted_at`/`valid_from`
    /// 推：那两个是快照语义，与墙上时钟无关。
    #[prost(uint64, tag = "17")]
    pub committed_at_ms: u64,
    /// **封口原因**（`SealReason::as_str()`，见 chunk 层）。
    ///
    /// 为什么必须落盘：高吞吐下"文件为什么只有 24MB"曾只能靠排除法推断（`operation-log §34.3`）——
    /// 阈值 / 窗口 / 驻留兜底 / 内存压力四种原因的含义**完全不同**：
    /// `pressure` 意味着**削峰与窗口承诺已被内存水位顶掉**，而 `rows_threshold` 是设计内行为。
    #[prost(string, tag = "18")]
    pub seal_reason: String,
    /// **封口瞬间的内存水位档位**（Normal/Soft/Hard/Reject）。
    /// 与 `seal_reason` 配对：区分"阈值触发"与"水位触发"的硬证据。
    #[prost(string, tag = "19")]
    pub seal_pressure: String,
}

impl FileManifest {
    pub fn is_active(&self) -> bool {
        self.status == FileStatus::Active as u32
    }

    /// 快照可见性规则（详细设计 §6.3）：
    /// 文件可见 ⟺ valid_from <= query_snapshot
    ///           AND (deleted_at == 0 OR query_snapshot < deleted_at)
    pub fn visible_at(&self, snapshot: u64) -> bool {
        self.valid_from <= snapshot && self.protects_at(snapshot)
    }

    /// 这个文件在快照 `snapshot` 下**还需要被保护吗**（T14.5）。
    ///
    /// 与 [`Self::visible_at`] 只差一个 `valid_from`，但这个差别是**故意的**：
    ///
    /// - `visible_at` 回答"读者**看得见**吗"；
    /// - `protects_at` 回答"**物理回收**会不会伤到谁" —— 一个 `valid_from` 还没到的
    ///   合并产物（`valid_from = snapshot + 1`）此刻不可见，但它**必须被保护**：
    ///   它是已经提交的真数据，只是还没到生效的那一刻。
    ///
    /// 于是回收判据是：**活着**（`deleted_at == 0`）或**墓碑期未过**
    /// （`snapshot < deleted_at`，旧快照的读者还看得见它）。过了这一线，任何快照都
    /// 看不见它了 —— 那就是"等墓碑期 + 无在途引用才真正删除"里的**墓碑期**（`architecture §4.6`）。
    pub fn protects_at(&self, snapshot: u64) -> bool {
        self.deleted_at == 0 || snapshot < self.deleted_at
    }
}

pub enum FileStatus {
    Active = 0,
    Staged = 1,
    Deleted = 2,
}

/// 数据节点名录里的一条（T12.3）。
///
/// **为什么进状态机而不是放配置**：成员名录必须与 schema/manifest **同版本**读出去
/// （`architecture-with-chunk §3.1`）—— 否则查询侧会拿"新的文件清单 + 旧的节点集合"
/// 拼计划。也因此它走 raft 的 op（`MetaService::Propose`），而不是某个旁路注册接口。
///
/// **心跳不在这里**：存活状态是秒级的，按设计走 metanode 内存 + 独立 RPC（`§3.2`）。
#[derive(Clone, PartialEq, prost::Message)]
pub struct DatanodeMember {
    #[prost(string, tag = "1")]
    pub instance_id: String,
    /// 数据面地址（`host:port`）
    #[prost(string, tag = "2")]
    pub address: String,
    /// 注册时刻（由发起方打点并随 op 传播；状态机不读钟）
    #[prost(uint64, tag = "3")]
    pub registered_at_ms: u64,
}

/// **租约条目**（T14.1）：全局作业（今天 = 压缩）的**单持有者**仲裁。
///
/// 为什么它**必须进 raft**：租约是**授权**（"谁有权合并"）—— 两个节点各信各的内存视图，
/// 就会**同时合并**（重复产物）。这与存活心跳**恰好相反**：心跳是**发现**（晚一点无所谓），
/// 所以秒级心跳绝不进 raft（`architecture-with-chunk §3.2`）。
///
/// `granted_at_ms` / `expires_at_ms` 都是**由 op 携带的时刻**（状态机不读钟，纪律 1）：
/// 到期判定是 `now_ms >= expires_at_ms`，而 `now_ms` 来自 `Op.now_ms` ——
/// 于是同一串 op 在所有副本上得到**同一状态**。
#[derive(Clone, PartialEq, prost::Message)]
pub struct LeaseEntry {
    /// 用途键：本轮是 `compaction`（分片粒度以后可扩成 `compaction:{table}:{shard}`）
    #[prost(string, tag = "1")]
    pub purpose: String,
    /// 持有者 `instance_id`
    #[prost(string, tag = "2")]
    pub holder: String,
    /// **代次**：每次授予/接管 +1。持有者用它判断"我的租约是不是已经被别人拿走了"
    /// —— **续租被拒即停手**，这是时钟偏斜下唯一的防线（`operation-log §81`）。
    #[prost(uint64, tag = "3")]
    pub epoch: u64,
    #[prost(uint64, tag = "4")]
    pub granted_at_ms: u64,
    #[prost(uint64, tag = "5")]
    pub expires_at_ms: u64,
}

/// 压缩作业的租约用途键（`§81`）。
///
/// **唯一来源**：状态机的栅栏判定与压缩侧的申请必须用同一个字符串 ——
/// 两处各写一遍，改一处就静默失配（栅栏失效 = 白写）。
pub const COMPACTION_LEASE: &str = "compaction";

/// 取租约的结果（`AcquireLease` 的 op 结果；trait 返回值同形状）。
// `prost::Message` 自带 `Debug`/`Default`，重复 derive 会冲突
#[derive(Clone, PartialEq, prost::Message)]
pub struct LeaseGrant {
    /// 是否授予（`false` = 别人正持有**且未过期**）
    #[prost(bool, tag = "1")]
    pub granted: bool,
    /// 授予时是**新**代次；拒绝时是**对方**的代次（便于诊断"谁挡着我"）
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
    /// 授予时是新的到期时刻；拒绝时是对方的
    #[prost(uint64, tag = "3")]
    pub expires_at_ms: u64,
}

/// 幂等键记录（【v8 修正 1】独立表，不随 FileManifest 删除而删除，§7.3.1）
#[derive(Clone, PartialEq, prost::Message)]

pub struct IdempotencyRecord {
    /// 主键
    #[prost(string, tag = "1")]
    pub client_request_id: String,
    #[prost(string, tag = "2")]
    pub batch_id: String,
    /// TTL 24h 起算点（Unix 秒）
    #[prost(uint64, tag = "3")]
    pub committed_at: u64,
}

/// 精简统计：仅排序列 + 分区列的 min/max/null_count（架构 §5.2）
#[derive(Clone, PartialEq, prost::Message)]
pub struct StatisticsLite {
    #[prost(message, repeated, tag = "1")]
    pub columns: Vec<ColumnStatLite>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ColumnStatLite {
    #[prost(string, tag = "1")]
    pub name: String,
    /// Arrow 标量 IPC 序列化
    #[prost(bytes = "vec", tag = "2")]
    pub min: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub max: Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub null_count: u64,
}

/// 一个**统计边界**（`§129`）：`compute_stats_lite` 把每列的 min/max 编成字节，
/// 这里给它一个**同一套语义**的值类型，让"写"与"读"共用一份编码规则（不许各写一半）。
///
/// 比较规则（`PartialOrd`）：
/// * 同族直接比；`I` 与 `U` 比时按数值（负数必小于任何 `U`）；`I`/`U` 与 `F` 比时都转 `f64`；
/// * **`B` 只与 `B` 比**；
/// * 任一侧是 `NaN` ⇒ `None`（"无法确定"）—— 调用方必须把 `None` 当**不可剪**处理。
#[derive(Debug, Clone, PartialEq)]
pub enum StatBound {
    I(i64),
    U(u64),
    F(f64),
    B(Vec<u8>),
}

impl StatBound {
    /// 能不能和对方比（不可比 ⇒ 调用方不得据此剪枝）。
    pub fn comparable(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::B(_), Self::B(_))
                | (Self::I(_) | Self::U(_) | Self::F(_), Self::I(_) | Self::U(_) | Self::F(_))
        )
    }
}

impl PartialOrd for StatBound {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;
        match (self, other) {
            (Self::I(a), Self::I(b)) => a.partial_cmp(b),
            (Self::U(a), Self::U(b)) => a.partial_cmp(b),
            (Self::F(a), Self::F(b)) => a.partial_cmp(b),
            (Self::B(a), Self::B(b)) => a.partial_cmp(b),
            (Self::I(a), Self::U(b)) => {
                if *a < 0 {
                    Some(Ordering::Less)
                } else {
                    (*a as u64).partial_cmp(b)
                }
            }
            (Self::U(a), Self::I(b)) => {
                if *b < 0 {
                    Some(Ordering::Greater)
                } else {
                    a.partial_cmp(&(*b as u64))
                }
            }
            // 与浮点混比：转 `f64`（大整数会损失精度 ⇒ 见 `comparable` 的注释与调用方的保守处理）
            (Self::F(a), _) => a.partial_cmp(&other.as_f64()?),
            (_, Self::F(b)) => self.as_f64()?.partial_cmp(b),
            _ => None,
        }
    }
}

impl StatBound {
    /// 只能用于"与浮点比较"这条路径；`B` 给 `None`。
    fn as_f64(&self) -> Option<f64> {
        Some(match self {
            Self::I(v) => *v as f64,
            Self::U(v) => *v as f64,
            Self::F(v) => *v,
            Self::B(_) => return None,
        })
    }
}

/// 把 [`StatBound`] 编成字节（**写侧**；读侧用 [`decode_bound`]，两者必须成对改）。
///
/// | 类型族 | 编码 |
/// |---|---|
/// | `Int8/16/32/64`、`Date32/64`、`Timestamp`、`Time32/64` | **i64 小端 8 字节** |
/// | `UInt8/16/32/64` | **u64 小端 8 字节** |
/// | `Float32/64` | **f64 小端 8 字节**（按位） |
/// | `Utf8` / `LargeUtf8` / `Binary` / `LargeBinary` | **原始字节** |
/// | 其余 | 空（= 没有边界，**永不剪**） |
pub fn encode_bound(dt: &arrow::datatypes::DataType, b: &StatBound) -> Vec<u8> {
    use arrow::datatypes::DataType as D;
    match (dt, b) {
        (D::UInt8 | D::UInt16 | D::UInt32 | D::UInt64, StatBound::U(v)) => {
            v.to_le_bytes().to_vec()
        }
        (D::Float32 | D::Float64, StatBound::F(v)) => v.to_le_bytes().to_vec(),
        (D::Utf8 | D::LargeUtf8 | D::Binary | D::LargeBinary, StatBound::B(v)) => v.clone(),
        (_, StatBound::I(v)) => v.to_le_bytes().to_vec(),
        // 类型与值的族对不上（不该发生）⇒ 空 = 没有边界，宁可不剪
        _ => Vec::new(),
    }
}

/// 字节 → [`StatBound`]（**读侧**）。类型不支持 / 字节数不对 ⇒ `None`。
pub fn decode_bound(dt: &arrow::datatypes::DataType, bytes: &[u8]) -> Option<StatBound> {
    use arrow::datatypes::DataType as D;
    macro_rules! i64b {
        () => {{
            let a: [u8; 8] = bytes.try_into().ok()?;
            StatBound::I(i64::from_le_bytes(a))
        }};
    }
    macro_rules! u64b {
        () => {{
            let a: [u8; 8] = bytes.try_into().ok()?;
            StatBound::U(u64::from_le_bytes(a))
        }};
    }
    macro_rules! f64b {
        () => {{
            let a: [u8; 8] = bytes.try_into().ok()?;
            StatBound::F(f64::from_le_bytes(a))
        }};
    }
    let b = match dt {
        D::Int8 | D::Int16 | D::Int32 | D::Int64 => i64b!(),
        D::Date32 | D::Date64 => i64b!(),
        D::Time32(_) | D::Time64(_) | D::Timestamp(_, _) => i64b!(),
        D::UInt8 | D::UInt16 | D::UInt32 | D::UInt64 => u64b!(),
        D::Float32 | D::Float64 => f64b!(),
        D::Utf8 | D::LargeUtf8 | D::Binary | D::LargeBinary => StatBound::B(bytes.to_vec()),
        _ => return None,
    };
    Some(b)
}

/// 从 RecordBatch 计算精简统计（仅排序列 + 分区列）。
///
/// ⚠️ **这里以前是空实现**：`min`/`max` 恒为 `Vec::new()`，注释写着"由文件 footer 提供完整统计"
/// —— 但**没人读 footer** ⇒ "文件级剪枝"这件事一行都没落地（查询侧连谓词都丢掉了）。
/// `§129` 把这条链补上：**算出来 → 存下来（下面这套编码）→ 查询侧用起来**。
pub fn compute_stats_lite(
    batch: &arrow::record_batch::RecordBatch,
    columns: &[String],
) -> Result<StatisticsLite, crate::error::LakeError> {
    use arrow::array::{Array, ArrayRef};
    use arrow::datatypes::DataType;

    /// 一列的 `(min, max)`；不支持的类型 / 全 null ⇒ `None`。
    fn bounds(arr: &ArrayRef) -> Option<(StatBound, StatBound)> {
        use arrow::array::*;
        macro_rules! prim {
            ($t:ty, $to:expr) => {{
                let a = arr.as_any().downcast_ref::<$t>()?;
                let mut it = a.iter().flatten();
                let first = it.next()?;
                let (mut lo, mut hi) = ($to(first), $to(first));
                for v in it {
                    let v = $to(v);
                    // 先把两个序都算出来，再动边界（`v` 会被移进 lo/hi，不能先用后移）
                    let lt = matches!(v.partial_cmp(&lo), Some(std::cmp::Ordering::Less));
                    let gt = matches!(v.partial_cmp(&hi), Some(std::cmp::Ordering::Greater));
                    // `partial_cmp` 给 `None`（NaN）⇒ 不动边界（保守：宁可不剪）
                    if lt {
                        lo = v.clone();
                    }
                    if gt {
                        hi = v;
                    }
                }
                Some((lo, hi))
            }};
        }
        // 字符数组与二进制数组的迭代项类型不同（`&str` vs `&[u8]`）⇒ 用转换器拉齐
        macro_rules! bytes_ {
            ($t:ty, $conv:expr) => {{
                let a = arr.as_any().downcast_ref::<$t>()?;
                let mut it = a.iter().flatten();
                let first = it.next()?;
                let (mut lo, mut hi) = ($conv(first), $conv(first));
                for v in it {
                    let v = $conv(v);
                    // 同 `prim!`：先算再移
                    let lt = v < lo;
                    let gt = v > hi;
                    if lt {
                        lo = v.clone();
                    }
                    if gt {
                        hi = v;
                    }
                }
                Some((StatBound::B(lo), StatBound::B(hi)))
            }};
        }
        match arr.data_type() {
            DataType::Int8 => prim!(Int8Array, |v: i8| StatBound::I(v as i64)),
            DataType::Int16 => prim!(Int16Array, |v: i16| StatBound::I(v as i64)),
            DataType::Int32 => prim!(Int32Array, |v: i32| StatBound::I(v as i64)),
            DataType::Int64 => prim!(Int64Array, |v: i64| StatBound::I(v)),
            DataType::UInt8 => prim!(UInt8Array, |v: u8| StatBound::U(v as u64)),
            DataType::UInt16 => prim!(UInt16Array, |v: u16| StatBound::U(v as u64)),
            DataType::UInt32 => prim!(UInt32Array, |v: u32| StatBound::U(v as u64)),
            DataType::UInt64 => prim!(UInt64Array, |v: u64| StatBound::U(v)),
            DataType::Float32 => prim!(Float32Array, |v: f32| StatBound::F(v as f64)),
            DataType::Float64 => prim!(Float64Array, |v: f64| StatBound::F(v)),
            DataType::Date32 => prim!(Date32Array, |v: i32| StatBound::I(v as i64)),
            DataType::Date64 => prim!(Date64Array, |v: i64| StatBound::I(v)),
            DataType::Time32(_) => prim!(Time32SecondArray, |v: i32| StatBound::I(v as i64)),
            DataType::Time64(_) => prim!(Time64MicrosecondArray, |v: i64| StatBound::I(v)),
            DataType::Timestamp(_, _) => {
                prim!(TimestampMicrosecondArray, |v: i64| StatBound::I(v))
            }
            DataType::Utf8 => bytes_!(StringArray, |v: &str| v.as_bytes().to_vec()),
            DataType::LargeUtf8 => bytes_!(LargeStringArray, |v: &str| v.as_bytes().to_vec()),
            DataType::Binary => bytes_!(BinaryArray, |v: &[u8]| v.to_vec()),
            DataType::LargeBinary => bytes_!(LargeBinaryArray, |v: &[u8]| v.to_vec()),
            // 其余类型（嵌套、字典…）：留空 ⇒ 查询侧不剪（**宁可少剪，不可错剪**）
            _ => None,
        }
    }

    let mut cols = Vec::new();
    for name in columns {
        let Some((idx, _)) = batch.schema().column_with_name(name) else {
            continue;
        };
        let arr = batch.column(idx);
        let (min, max) = match bounds(arr) {
            Some((lo, hi)) => (
                encode_bound(arr.data_type(), &lo),
                encode_bound(arr.data_type(), &hi),
            ),
            None => (Vec::new(), Vec::new()),
        };
        cols.push(ColumnStatLite {
            name: name.clone(),
            min,
            max,
            null_count: arr.null_count() as u64,
        });
    }
    Ok(StatisticsLite { columns: cols })
}

/// 默认排序列：event_time（业务约定，缺失时无排序列）。
pub fn default_sort_columns(schema: &SchemaRef) -> Vec<String> {
    if schema.field_with_name("event_time").is_ok() {
        vec!["event_time".to_string()]
    } else {
        Vec::new()
    }
}

/// 数值类型提升格中的秩（详细设计 §8.1）：
/// Int8 < Int16 < Int32 < Int64 < Float64
/// Utf8 与数值不可互转 —— 拒绝。
pub fn promotion_rank(dt: &DataType) -> Option<u8> {
    Some(match dt {
        DataType::Int8 => 0,
        DataType::Int16 => 1,
        DataType::Int32 => 2,
        DataType::Int64 => 3,
        DataType::Float64 => 4,
        _ => return None,
    })
}

/// 单元测试辅助：构造一个最小 schema
pub fn test_schema(fields: &[(&str, DataType)]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|(n, t)| Field::new(*n, t.clone(), true))
            .collect::<Vec<_>>(),
    ))
}
