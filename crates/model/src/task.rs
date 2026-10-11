//! **后台作业的任务目录**（`§169`，`plan.md` G.1）。
//!
//! # 为什么要有它
//!
//! `§166` 把压缩改成按 `(table, shard)` 分租约之后，多节点确实能**同时**干活了，
//! 但"**要合并哪些分片**"这件事仍然是**每个节点各自从清单算出来的** —— 于是：
//!
//! * 每个节点每轮都要把全量文件清单拉一遍（元数据读放大，且随分片数增长）；
//! * 彼此**看不见**对方的待办 ⇒ **优先级 / 公平 / 限流**无从谈起（谁手快谁做）；
//! * 运维问"现在有哪些活、谁在做" ⇒ 只能从日志里猜。
//!
//! 任务目录把这份"待办"变成**状态机里的一个事实**：随 raft 复制、随快照下发、
//! 所有节点读到的是**同一份**。
//!
//! # 两条设计纪律
//!
//! 1. **任务由状态机派生，不由某个节点扫描产生**。
//!    派生的输入只有"文件清单 + 删除向量"，两者都是确定性的 ⇒ **每个副本算出同一张任务表**
//!    （本仓的确定性纪律，`§38`）。反过来若是"某节点扫出来写进去"，就要先解决"谁来扫"
//!    —— 那是又一把租约和一个单点。
//! 2. **认领复用既有租约**（`§166` 的分领原语：purpose + epoch + TTL + 可接管），
//!    **不新造一套认领协议**。任务目录只回答"有哪些活"，"谁在做"由租约回答
//!    （两者拼起来就是运维要看的那张表）。
//!
//! # 任务的语义是"候选"，不是"命令"
//!
//! 派生用的阈值是**保守的超集**（容易登记、不漏活）：执行者仍按自己的 `[compaction]`
//! 配置判定（更严就跳过）。漏登记 = 没人做这件事；多登记 = 有人白看一眼 —— 前者才是事故。

/// 任务的键：`(全限定表, 分片)`。
///
/// 用**全限定名**（`§167` 的教训：`TableMeta.name` 是裸名，跨层标识一律是 `schema.table`）。
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskKey {
    pub table: String,
    pub shard: String,
}

impl TaskKey {
    pub fn new(table: impl Into<String>, shard: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            shard: shard.into(),
        }
    }

    /// 稳定字符串形式（`Status` / 日志 / 用途键都用它）。
    pub fn id(&self) -> String {
        format!("{}:{}", self.table, self.shard)
    }

    /// **认领这任务要用的租约用途键**（`§169`：认领 = 租约，不另造协议）。
    pub fn lease_purpose(&self) -> String {
        format!("task:{}", self.id())
    }
}

/// 为什么这个分片进了任务表（观测用：运维最先问的就是"为什么有这条"）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskReason {
    /// 同分片可见文件数达到阈值
    Files = 0,
    /// 某个文件的删除占比与绝对行数都够大（DV 需要被消费，`F.3d-2`）
    Deletions = 1,
}

/// 一条待办（状态机派生；随快照下发）。
#[derive(Clone, PartialEq, prost::Message)]
pub struct CompactionTask {
    /// 全限定表名（`schema.table`）
    #[prost(string, tag = "1")]
    pub table: String,
    #[prost(string, tag = "2")]
    pub shard: String,
    /// 这个分片当前的可见文件数
    #[prost(uint64, tag = "3")]
    pub files: u64,
    /// 这个分片当前生效的删除行数合计（DV 触发的那条路）
    #[prost(uint64, tag = "4")]
    pub deleted_rows: u64,
    /// 触发原因（0 = 文件数，1 = 删除占比）
    #[prost(uint32, tag = "5")]
    pub reason: u32,
    /// 首次登记的时刻（Unix 毫秒；观测"挂了多久"用）。
    ///
    /// **`0 = 未知`**：任务目录**不进快照**（它是"文件 + DV"的派生量），所以从快照还原出来的
    /// 任务在**下一次被携带时钟的 op 触碰之前**是不知道自己"挂了多久"的 —— 那时如实报 0，
    /// 由 `meta_probe tasks` / `/metrics` 渲染成 `unknown`，而不是 `now - 0`（那会是 54 年）。
    #[prost(uint64, tag = "6")]
    pub since_ms: u64,
}

impl CompactionTask {
    pub fn key(&self) -> TaskKey {
        TaskKey::new(self.table.clone(), self.shard.clone())
    }

    pub fn reason(&self) -> TaskReason {
        match self.reason {
            1 => TaskReason::Deletions,
            _ => TaskReason::Files,
        }
    }
}

/// 派生的阈值（`§169`）。
///
/// ⚠️ 它们是**超集**阈值：任务表回答"这个分片**值得看一看**"，真正的合并判定仍在执行者的
/// `[compaction]` 配置里（`min_files` / `dv_ratio_threshold` / `dv_min_card`）。
/// 单一来源的说明见 `docs/operation-log.md §169.2`。
pub mod threshold {
    /// 同分片可见文件数到这么多就登记。
    ///
    /// 取 **2** 的理由：它是"**能合并**"的必要条件（1 个文件没法合并），
    /// 也就是**最小超集** —— 再低没有意义，再高就会漏活（"值不值得"是**执行者**的事：
    /// 它的 `[compaction] min_files` 才是成本判定，默认 5）。
    pub const FILES: u64 = 2;
    /// 删除行数 / 文件行数 的占比：**执行者默认值的一半** ⇒ 保证是超集
    /// （`CompactionConfig::dv_ratio_threshold` 默认 0.1）。
    pub const DV_RATIO: f64 = 0.05;
    /// 删除行数的绝对下界，同样取执行者默认值的一半（默认 1000）⇒ 超集。
    pub const DV_MIN_CARD: u64 = 500;
}
