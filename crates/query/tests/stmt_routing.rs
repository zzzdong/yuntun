//! **F.1 语句路由**的端到端用例（`§142`）。
//!
//! 守三件事：① 要接管的语句**被认出来**并给出**可读**的"尚未支持"（不是 DataFusion
//! 那句不知所云的解析错误）；② 查询类**原样**交给 DataFusion（行为不变）；③ 垃圾 SQL
//! 的报错可读。

use yuntun_query::QueryEngine;

fn engine() -> QueryEngine {
    let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap();
    let cache = std::sync::Arc::new(yuntun_query::LocalCatalog::new());
    QueryEngine::new(store, cache)
}

/// 被分流的语句：**认得出 + 说人话**。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routed_statements_are_recognized_and_rejected_readably() {
    let e = engine();
    for (sql, want) in [
        ("ALTER TABLE t ADD COLUMN c INT", "ALTER TABLE"),
        ("DELETE FROM t WHERE a = 1", "DELETE"),
        ("UPDATE t SET a = 2 WHERE a = 1", "UPDATE"),
    ] {
        let err = e
            .sql(sql)
            .await
            .expect_err("{sql} 应当被我们接管（而不是当成查询执行）");
        let msg = format!("{err}");
        assert!(
            msg.contains(want) && msg.contains("尚未支持"),
            "报错要点名语句类型并说明状态（{want}），实际：{msg}"
        );
        assert!(
            !msg.contains("sql parser error"),
            "不该再掉回 DataFusion 的解析错误：{msg}"
        );
    }
}

/// **查询类行为不变**（路由层不许把 SELECT 弄脏）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn select_is_untouched_by_the_router() {
    let e = engine();
    let out = e
        .sql("SELECT count(*) AS c FROM generate_series(1, 10)")
        .await
        .expect("SELECT 必须照常能跑");
    let got = arrow::util::display::array_value_to_string(out[0].column(0), 0).unwrap();
    assert_eq!(got, "10", "路由层不得改变查询结果：{got}");
}

/// 垃圾 SQL ⇒ **可读**的报错（不是内部 panic / 不知所云的堆栈）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn garbage_sql_fails_readably() {
    let e = engine();
    let err = e.sql("NOT SQL AT ALL ;;;").await.expect_err("垃圾 SQL 必须报错");
    let msg = format!("{err}");
    assert!(
        msg.contains("SQL 解析失败"),
        "报错要能让人看懂是解析问题，实际：{msg}"
    );
}
