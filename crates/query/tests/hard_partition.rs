//! **硬分区**（架构 §2.8，plan `T6.14`）：查询区有**自己的、有限的**内存池。
//!
//! # 这条用例证什么、不证什么
//!
//! **证**：查询引擎的内存上限**真的被强制执行** —— 一个大基数聚合把它撑爆时，得到的是
//! **可诊断的错误**（`Resources exhausted`），而不是静默降级、也不是把进程拖垮。
//! 没有它，"查询区有上限"只是一句配置说明。
//!
//! **不证**：写入侧（chunk 区）在同一次压力下"一点没被碰" —— 那由**零件级**用例守
//! （`chunk::budget::tests::chunk_and_query_regions_are_hard_partitioned`：两个账本各自触顶、
//! 各自拒绝、各自释放）。两层的分工写在这里，免得下次有人把其中一层当成另一层。

use std::sync::Arc;

use yuntun_query::QueryEngine;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_query_that_blows_its_own_pool_fails_loudly() {
    let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap();
    let cache = Arc::new(yuntun_query::LocalCatalog::new());
    // 查询区上限 **1 MiB**（故意极小）：50 万组的聚合状态装不下
    let engine = QueryEngine::with_query_memory_limit(store, cache, 1 << 20)
        .expect("建引擎（限制 1 MiB）");

    let r = engine
        .sql("SELECT value, count(*) AS c FROM generate_series(1, 500000) GROUP BY value ORDER BY value")
        .await;
    let err = r.expect_err("1 MiB 的查询池装不下 50 万组聚合状态 ⇒ 必须**失败**，不能静默给答案");
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("resources exhausted") || msg.to_lowercase().contains("memory"),
        "失败必须**可诊断**（内存类错误），实际：{msg}"
    );
    // 引擎本身仍然可用（不是"炸了一次就废了"）：换个小查询照样跑
    let ok = engine.sql("SELECT count(*) AS c FROM generate_series(1, 10)").await;
    assert!(
        ok.is_ok(),
        "一次查询撞到上限不该让引擎失效（独立的内存池应当各自归还）：{:?}",
        ok.map(|b| b.len())
    );
}
