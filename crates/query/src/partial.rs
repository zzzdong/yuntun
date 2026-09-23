//! **部分结果**（R5 / T13.4 第一刀）：把"**拿不到**"与"**还没拿到**"分开。
//!
//! ## 两个形状几乎一样、语义完全相反的失败
//!
//! | | 判据 | 该怎么办 |
//! |---|---|---|
//! | **STALE**（还没拿到） | 成员**能答**，且答的是"我的水位超前于你的 manifest" | **刷新 + 重试**；连着追不上 ⇒ **响亮失败**（`operation-log §61.4`） |
//! | **不可达 / 读失败**（拿不到） | 成员**没能答**（连接拒绝、超时、内部错误） | **降级**：返回可用部分 + **明确标记缺了谁**（`architecture §4.3`） |
//!
//! 两者在代码里**本来就分处两个通道** —— STALE 走 [`yuntun_store::ShardRead::stale`]（读成功、
//! 携带水位），失败走 `Err`。但此前扇出把 `Err` 直接 `?` 冒泡 ⇒ **整条查询失败**，
//! 于是"一个监控节点挂了"就变成"整个看板打不开"✗。
//!
//! ## 为什么默认是降级而不是报错
//!
//! 设计原文（`architecture-with-chunk §4.2` 第三条）：
//!
//! > **partial response 默认允许**：节点失败时返回可用结果 + `partial: true` + 缺失来源列表。
//! > 监控场景下"90% 数据 + 明确标记"远好过整体报错（**可配置拒绝**）。
//!
//! 关键词是"**明确标记**"：降级本身不危险，**静默**降级才危险。所以缺了谁、为什么缺，
//! 必须随结果一起交出去（[`PartialRead::describe`]），而不能只留在日志里。
//!
//! ## 与 STALE 的边界（最容易写错的地方）
//!
//! **不可达不得被当成 STALE 的替代品**：把"成员读失败"降级成 partial，是承认这部分数据
//! **真的拿不到**；而 STALE 是"**拿得到**，只是我们的目录落后了" —— 对它降级 =
//! 把**可修复的落后**当成永久缺失，等于**静默少数据** ✗。所以本模块**只**处理失败通道，
//! STALE 的重试/响亮失败逻辑一行不动（`§64` / `stale_retry` 用例守着）。

use std::sync::Mutex;

use datafusion::error::DataFusionError;

/// 缺失的一个来源（"哪张表、哪个实例、为什么"）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingSource {
    /// 全限定表名（`schema.table`）
    pub table: String,
    /// 该数据的持有实例（数据节点 `instance_id`）
    pub instance: String,
    /// 原始失败原因（给排障用；不参与判定）
    pub reason: String,
}

/// 一次查询里"没能读到"的来源汇总。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartialRead {
    missing: Vec<MissingSource>,
}

impl PartialRead {
    /// 有没有缺来源（= 结果是否部分）。
    pub fn is_partial(&self) -> bool {
        !self.missing.is_empty()
    }

    /// 缺失来源列表（顺序 = 发现的顺序，**去重**按 (table, instance)）。
    pub fn missing(&self) -> &[MissingSource] {
        &self.missing
    }

    /// 缺失来源的可读摘要 —— **日志与错误都用它**，避免两处措辞漂移
    /// （同一条事实在日志里和在报错里不一样的措辞，会让排障时对不上）。
    pub fn describe(&self) -> String {
        if self.missing.is_empty() {
            return "（无缺失来源）".to_string();
        }
        let items: Vec<String> = self
            .missing
            .iter()
            .map(|m| format!("{}@{}（{}）", m.table, m.instance, m.reason))
            .collect();
        format!(
            "缺失 {} 个来源：{} —— 结果是**部分**的（该来源的热数据未参与本次查询）",
            items.len(),
            items.join("; ")
        )
    }
}

/// partial 策略（`architecture §4.2`：默认允许，可配置拒绝）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PartialPolicy {
    /// **默认**：降级为部分结果，并把缺失来源标记出来。
    #[default]
    Allow,
    /// 拒绝部分结果：读不到任何来源就**当场失败**（点名缺了谁）。
    Reject,
}

impl PartialPolicy {
    /// 配置字符串解析（`"allow"` / `"reject"`）；未知值返回 `None` —— 由调用方决定
    /// 是"报错退出"还是"取默认"（配置错误不该被默默吞掉）。
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "allow" => Some(Self::Allow),
            "reject" => Some(Self::Reject),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Reject => "reject",
        }
    }
}

/// 策略 `Reject` 下拒绝部分结果时报的错。
///
/// 用**类型**而不是字符串（与本仓 `HotReadStale` 同一考虑）：字符串匹配认不出来，
/// 而"因为缺来源而失败"与"因为别的原因失败"在排障上要能一眼分开。
#[derive(Debug)]
pub struct PartialRejected {
    pub source: MissingSource,
    /// 到拒绝为止**已经**记下的全部缺失来源
    pub missing: Vec<MissingSource>,
}

impl std::fmt::Display for PartialRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "拒绝部分结果（partial = reject）：来源 {}@{} 读不到（{}）；本次查询共缺 {} 个来源。\
             要么修好该节点，要么把 partial 设为 allow 以接受带标记的部分结果",
            self.source.table,
            self.source.instance,
            self.source.reason,
            self.missing.len()
        )
    }
}

impl std::error::Error for PartialRejected {}

/// **每查询一个** sink：记录读不到的来源，并按策略决定"降级"还是"拒绝"。
///
/// 为什么是每查询而不是全局：`partial` 是**这一次查询**的属性 —— 全局累计会让上一次查询
/// 缺的来源污染下一次的判定（"这次到底完整没有"就答不出来了）。
#[derive(Debug)]
pub struct PartialSink {
    policy: PartialPolicy,
    missing: Mutex<Vec<MissingSource>>,
}

impl PartialSink {
    pub fn new(policy: PartialPolicy) -> Self {
        Self {
            policy,
            missing: Mutex::new(Vec::new()),
        }
    }

    pub fn policy(&self) -> PartialPolicy {
        self.policy
    }

    /// 记下一个"读不到"的来源，并**按策略**决定后续：
    ///
    /// - [`PartialPolicy::Allow`] ⇒ `Ok(())`：降级，查询继续（调用方最后能从
    ///   [`Self::read`] 看到缺了谁）；
    /// - [`PartialPolicy::Reject`] ⇒ `Err`：当场失败，错误里点名缺了谁。
    ///
    /// 去重按 `(table, instance)`：同一张表在一条 SQL 里被扫两次（自连接）时，
    /// "谁缺了"是同一个事实，列两遍只会让摘要变长而不多信息。
    pub fn record(&self, source: MissingSource) -> Result<(), DataFusionError> {
        let mut missing = self.missing.lock().unwrap();
        let dup = missing
            .iter()
            .any(|m| m.table == source.table && m.instance == source.instance);
        if !dup {
            missing.push(source.clone());
        }
        let all = missing.clone();
        drop(missing);

        match self.policy {
            PartialPolicy::Allow => Ok(()),
            PartialPolicy::Reject => Err(DataFusionError::External(Box::new(PartialRejected {
                source,
                missing: all,
            }))),
        }
    }

    /// 本次查询到目前为止的缺失来源。
    pub fn read(&self) -> PartialRead {
        PartialRead {
            missing: self.missing.lock().unwrap().clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn src(table: &str, instance: &str) -> MissingSource {
        MissingSource {
            table: table.to_string(),
            instance: instance.to_string(),
            reason: "connection refused".to_string(),
        }
    }

    #[test]
    fn allow_degrades_and_lists_missing_sources() {
        let sink = PartialSink::new(PartialPolicy::Allow);
        assert!(!sink.read().is_partial());

        sink.record(src("public.t", "inst-a")).expect("allow 不报错");
        sink.record(src("public.t", "inst-b")).expect("allow 不报错");

        let r = sink.read();
        assert!(r.is_partial());
        assert_eq!(r.missing().len(), 2);
        // 摘要要点出"谁"与"为什么"——否则标记等于没标
        let d = r.describe();
        assert!(d.contains("inst-a") && d.contains("connection refused"), "{d}");
        assert!(d.contains("部分"), "{d}");
    }

    #[test]
    fn reject_fails_immediately_and_names_the_source() {
        let sink = PartialSink::new(PartialPolicy::Reject);
        let e = sink.record(src("public.t", "inst-a")).unwrap_err();
        let msg = e.to_string();
        assert!(msg.contains("inst-a") && msg.contains("public.t"), "{msg}");
        assert!(msg.contains("reject"), "{msg}");
    }

    #[test]
    fn duplicate_sources_are_recorded_once() {
        let sink = PartialSink::new(PartialPolicy::Allow);
        sink.record(src("public.t", "inst-a")).unwrap();
        sink.record(src("public.t", "inst-a")).unwrap();
        sink.record(src("public.t", "inst-b")).unwrap();
        assert_eq!(sink.read().missing().len(), 2);
    }

    #[test]
    fn policy_parse_is_explicit() {
        assert_eq!(PartialPolicy::parse("allow"), Some(PartialPolicy::Allow));
        assert_eq!(PartialPolicy::parse(" Reject "), Some(PartialPolicy::Reject));
        // 未知值**不静默取默认**：免得配置写错却以为生效了
        assert_eq!(PartialPolicy::parse("alloww"), None);
        assert_eq!(PartialPolicy::default(), PartialPolicy::Allow);
    }
}
