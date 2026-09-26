//! 内存账本、背压阶梯与内存硬分区（架构 §2.7 / §2.8）。
//!
//! ## 为什么必须硬分区（§2.8）
//! datanode 内同时跑写入与查询。不做硬分区，**一个大基数 `GROUP BY` 就能把 chunk 内存挤光、
//! 把写入压垮**。因此把内存预算切成两块互不抢占的区域：
//!
//! | 区域 | 超限行为 |
//! |---|---|
//! | [`MemoryPartition::chunk`] | spill → flush → 背压拒写，**必须保底** |
//! | [`MemoryPartition::query`] | 超限直接返回错误（agg/sort 临时内存），**绝不抢占 chunk 区** |
//!
//! 分区在本 crate 只体现为**两个独立账本**；query 侧由 `yuntun-query` 把 query 账本接成
//! DataFusion 的内存池，从而在物理计划层真正生效（两边不共享任何计数器）。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// 背压水位（架构 §2.7 背压阶梯）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pressure {
    /// < 60%：正常
    Normal,
    /// >= 60%：后台 spill 最老的 sealed chunk
    Soft,
    /// >= 80%：强制 seal open chunk 并 spill
    Hard,
    /// >= 95%（或磁盘达水位）：拒绝写入，DoPut 返回 `RESOURCE_EXHAUSTED` + `retry-after`
    Reject,
}

impl Pressure {
    pub fn needs_spill(&self) -> bool {
        matches!(self, Pressure::Soft | Pressure::Hard | Pressure::Reject)
    }

    pub fn needs_force_seal(&self) -> bool {
        matches!(self, Pressure::Hard | Pressure::Reject)
    }

    pub fn rejects_writes(&self) -> bool {
        matches!(self, Pressure::Reject)
    }
}

/// 阶梯阈值（可配置）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PressureThresholds {
    pub soft: f64,
    pub hard: f64,
    pub reject: f64,
}

impl Default for PressureThresholds {
    fn default() -> Self {
        Self {
            soft: 0.60,
            hard: 0.80,
            reject: 0.95,
        }
    }
}

impl PressureThresholds {
    pub fn classify(&self, ratio: f64) -> Pressure {
        if ratio >= self.reject {
            Pressure::Reject
        } else if ratio >= self.hard {
            Pressure::Hard
        } else if ratio >= self.soft {
            Pressure::Soft
        } else {
            Pressure::Normal
        }
    }
}

/// 单一区域的内存账本（**硬上限**：超过上限的预留直接失败）。
#[derive(Debug)]
pub struct MemoryLedger {
    name: &'static str,
    limit: usize,
    used: AtomicUsize,
    thresholds: PressureThresholds,
}

impl MemoryLedger {
    pub fn new(name: &'static str, limit: usize) -> Arc<Self> {
        Arc::new(Self {
            name,
            limit,
            used: AtomicUsize::new(0),
            thresholds: PressureThresholds::default(),
        })
    }

    pub fn with_thresholds(name: &'static str, limit: usize, thresholds: PressureThresholds) -> Arc<Self> {
        Arc::new(Self {
            name,
            limit,
            used: AtomicUsize::new(0),
            thresholds,
        })
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::SeqCst)
    }

    pub fn available(&self) -> usize {
        self.limit.saturating_sub(self.used())
    }

    /// 水位比例 `[0, +inf)`；`limit == 0` 视为已满（0 字节预算 = 禁止驻留）。
    pub fn ratio(&self) -> f64 {
        if self.limit == 0 {
            if self.used() == 0 {
                0.0
            } else {
                f64::INFINITY
            }
        } else {
            self.used() as f64 / self.limit as f64
        }
    }

    /// 当前背压水位（架构 §2.7）。
    pub fn pressure(&self) -> Pressure {
        self.thresholds.classify(self.ratio())
    }

    /// 尝试预留：超限返回 `false`（调用方据此 spill 或拒写）。
    pub fn try_reserve(&self, bytes: usize) -> bool {
        let mut cur = self.used.load(Ordering::SeqCst);
        loop {
            let next = cur + bytes;
            if next > self.limit {
                return false;
            }
            match self
                .used
                .compare_exchange_weak(cur, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }

    /// 无条件记账（已确定要驻留的数据，如"WAL 已 fsync 的可见数据"）。
    ///
    /// 允许越过上限：可见性承诺优先于内存预算，越过之后由背压阶梯（[`Self::pressure`]）
    /// 推进 spill / 拒写把水位收回来。**绝不因此丢弃已 fsync 的数据**（I1）。
    pub fn reserve(&self, bytes: usize) {
        self.used.fetch_add(bytes, Ordering::SeqCst);
    }

    /// 释放账面占用（spill / release 后调用）。
    pub fn release(&self, bytes: usize) {
        self.used.fetch_sub(bytes, Ordering::SeqCst);
    }
}

/// 内存硬分区（架构 §2.8）。
#[derive(Debug, Clone)]
pub struct MemoryPartition {
    chunk: Arc<MemoryLedger>,
    query: Arc<MemoryLedger>,
}

impl MemoryPartition {
    /// `chunk_bytes` / `query_bytes` 相互独立，互不借用。
    pub fn new(chunk_bytes: usize, query_bytes: usize) -> Self {
        Self::with_thresholds(
            chunk_bytes,
            query_bytes,
            PressureThresholds::default(),
        )
    }

    pub fn with_thresholds(
        chunk_bytes: usize,
        query_bytes: usize,
        thresholds: PressureThresholds,
    ) -> Self {
        Self {
            chunk: MemoryLedger::with_thresholds("chunk", chunk_bytes, thresholds),
            query: MemoryLedger::with_thresholds("query", query_bytes, thresholds),
        }
    }

    /// chunk 区账本（必须保底）。
    pub fn chunk(&self) -> &Arc<MemoryLedger> {
        &self.chunk
    }

    /// query 执行区账本（超限直接失败，绝不抢占 chunk 区）。
    pub fn query(&self) -> &Arc<MemoryLedger> {
        &self.query
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ladder_thresholds_classify() {
        let t = PressureThresholds::default();
        assert_eq!(t.classify(0.0), Pressure::Normal);
        assert_eq!(t.classify(0.59), Pressure::Normal);
        assert_eq!(t.classify(0.60), Pressure::Soft);
        assert_eq!(t.classify(0.79), Pressure::Soft);
        assert_eq!(t.classify(0.80), Pressure::Hard);
        assert_eq!(t.classify(0.94), Pressure::Hard);
        assert_eq!(t.classify(0.95), Pressure::Reject);
        assert_eq!(t.classify(2.0), Pressure::Reject);
    }

    #[test]
    fn ledger_enforces_hard_limit_on_try_reserve() {
        let l = MemoryLedger::new("chunk", 100);
        assert!(l.try_reserve(60));
        assert!(!l.try_reserve(50), "超过硬上限必须失败");
        assert!(l.try_reserve(40));
        assert_eq!(l.used(), 100);
        assert_eq!(l.ratio(), 1.0);
        assert_eq!(l.pressure(), Pressure::Reject);
        l.release(100);
        assert_eq!(l.used(), 0);
        assert_eq!(l.pressure(), Pressure::Normal);
    }

    #[test]
    fn reserve_is_unconditional_and_release_restores() {
        // 可见性承诺优先于预算：已 fsync 的数据必须能记账（然后靠背压收回水位）
        let l = MemoryLedger::new("chunk", 10);
        l.reserve(50);
        assert_eq!(l.used(), 50);
        assert_eq!(l.pressure(), Pressure::Reject);
        l.release(50);
        assert_eq!(l.pressure(), Pressure::Normal);
    }

    #[test]
    fn zero_budget_ledger_reports_full_when_used() {
        let l = MemoryLedger::new("chunk", 0);
        assert_eq!(l.ratio(), 0.0);
        assert_eq!(l.pressure(), Pressure::Normal);
        l.reserve(1);
        assert_eq!(l.pressure(), Pressure::Reject);
    }

    #[test]
    fn partition_regions_are_independent() {
        // 硬分区：query 区用满不得影响 chunk 区（反之亦然）
        let p = MemoryPartition::new(100, 100);
        assert!(p.query().try_reserve(100));
        assert_eq!(p.query().pressure(), Pressure::Reject);
        assert_eq!(p.chunk().used(), 0);
        assert_eq!(p.chunk().pressure(), Pressure::Normal);
        assert!(p.chunk().try_reserve(100), "chunk 区必须保底，不被 query 抢占");
    }

    /// **硬分区**（架构 §2.8，plan `T6.14`）：chunk 区触顶**与** query 区无关，反之亦然。
    ///
    /// 判据用"**真拒绝**"（`try_reserve` 返回 `false`）而不是"水位比例"——
    /// 比例只是算术，**拒绝**才是分区在起作用。
    #[test]
    fn chunk_and_query_regions_are_hard_partitioned() {
        let p = MemoryPartition::new(100, 100);

        // ① chunk 区塞到限额：再要一个字节必须**被拒**，且被拒的那次**不计入**用量
        assert!(p.chunk().try_reserve(100), "限额内的申请应当成功");
        assert!(!p.chunk().try_reserve(1), "超出限额必须**被拒**（不是静默超限）");
        assert_eq!(p.chunk().used(), 100, "被拒的申请不得计入用量");
        assert!(
            p.chunk().pressure().rejects_writes(),
            "满额 ⇒ 第三级背压（明确拒绝写入，而不是静默超限）"
        );

        // ② **query 区一点都没被碰** —— 这就是"硬分区"那句话的可执行形式
        assert_eq!(
            p.query().used(),
            0,
            "chunk 区触顶**不得**动用 query 区（硬分区）"
        );
        assert!(p.query().try_reserve(100), "query 区仍可正常申请");

        // ③ 反向：query 区触顶时，chunk 区照样能收（只要它还有余额）
        assert!(!p.query().try_reserve(1), "query 区满额后同样**被拒**");
        p.chunk().release(100);
        assert!(
            p.chunk().try_reserve(50),
            "chunk 区释放后应能再收 —— 它不受 query 区满额的影响"
        );

        // ④ 释放是**各自**的：动 chunk 不影响 query 的用量
        let query_before = p.query().used();
        p.chunk().release(50);
        assert_eq!(p.chunk().used(), 0);
        assert_eq!(
            p.query().used(),
            query_before,
            "释放必须只动自己那一区（否则两个区会通过释放互相串味）"
        );
    }

    /// 两个区的**限额互不相干**（同一个 `MemoryPartition` 里各配各的）。
    #[test]
    fn region_limits_are_independent() {
        let p = MemoryPartition::new(10, 1000);
        assert_eq!(p.chunk().limit(), 10);
        assert_eq!(p.query().limit(), 1000);
        assert!(p.chunk().try_reserve(10));
        assert!(!p.chunk().try_reserve(1), "chunk 区到 10 就满");
        assert!(
            p.query().try_reserve(1000),
            "query 区的限额与 chunk 区无关（1000 照样给）"
        );
    }
}
