//! **`EXPLAIN` 的判据**（`§184` 普查后补）：查询计划原样转发 DataFusion，
//! 非查询形态明确拒绝。
//!
//! 反证形态：修好之前 `EXPLAIN SELECT …` 落在 dispatch 的 `other =>` 分支，
//! 报 1149 "only SELECT / … are supported" —— 一个连执行计划都看不到的数据库
//! 没法回答"这条查询为什么慢"。

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_model::ops::CreateTableRequest;
use yuntun_query::{LocalCatalog, QueryEngine};
use yuntun_sql::session::SessionCtx;
use yuntun_sql::{SqlEngine, SqlError};
use yuntun_store::{StoreConfig, create_store};

fn engine() -> SqlEngine {
    let catalog = Arc::new(MemoryCatalog::new());
    // 建一张真表：聚合计划断言要对着它（空 catalog 上 `FROM t` 是 NotFound）
    {
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("usage", DataType::Float64, true),
        ]));
        let c = catalog.clone();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(c.create_table(CreateTableRequest {
                name: "cpu".to_string(),
                namespace: "public".to_string(),
                schema,
                partition_cols: vec![],
                default_format: "vortex".to_string(),
                ingest_config: yuntun_model::meta::IngestConfig::standard(),
            }))
        })
        .expect("建表");
    }
    // 查询路径读的是 LocalCatalog 快照（不是 catalog 本身）⇒ 先刷一次，
    // 否则"表已建但快照里没有"= NotFound（与生产里 spawn_refresh 的职责同源）
    let cache = Arc::new(LocalCatalog::new());
    {
        let (cache, catalog) = (cache.clone(), catalog.clone() as Arc<dyn CatalogOps>);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(cache.refresh(&catalog))
                .expect("刷新本地目录快照");
        });
    }
    SqlEngine::new_readonly(
        Arc::new(QueryEngine::new(
            create_store(&StoreConfig::Memory).expect("memory store"),
            cache,
        )),
        catalog,
    )
}

async fn run(sql: &str) -> Result<Vec<arrow::record_batch::RecordBatch>, SqlError> {
    let mut session = SessionCtx::default();
    match engine().execute(sql, &mut session).await? {
        yuntun_sql::SqlResult::Rows { batches, .. } => Ok(batches),
        yuntun_sql::SqlResult::Affected(_) => panic!("EXPLAIN 应返回结果集"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explain_renders_the_plan_for_queries() {
    // ① 常量查询：逻辑计划 + 物理计划两行
    let batches = run("EXPLAIN SELECT 1 + 1").await.expect("EXPLAIN SELECT");
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2, "EXPLAIN 返回 logical_plan + physical_plan 两行");
    let plan_types: Vec<&str> = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .iter()
        .map(|v| v.unwrap_or_default())
        .collect();
    assert_eq!(plan_types, vec!["logical_plan", "physical_plan"]);

    // ② 聚合查询：计划里看得到聚合与扫描（证明转发了**这条**查询而不是 canned 答案）
    let batches = run("EXPLAIN SELECT host, avg(usage) FROM cpu GROUP BY host")
        .await
        .expect("EXPLAIN agg");
    let plan = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .value(0)
        .to_string();
    assert!(plan.contains("Aggregate"), "逻辑计划应含聚合：{plan}");
    assert!(plan.contains("TableScan"), "逻辑计划应含扫描：{plan}");

    // ③ `EXPLAIN ANALYZE` 真的跑（DataFusion 原生语义）
    let batches = run("EXPLAIN ANALYZE SELECT 1")
        .await
        .expect("EXPLAIN ANALYZE");
    assert!(batches.iter().map(|b| b.num_rows()).sum::<usize>() >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explain_rejects_non_queries_loudly() {
    // DML / DDL 没有 DataFusion 计划可渲染 ⇒ 明确拒绝（不转一句看不懂的 SQL）
    for sql in [
        "EXPLAIN INSERT INTO t VALUES (1)",
        "EXPLAIN CREATE TABLE x (a INT)",
        "EXPLAIN DELETE FROM t",
    ] {
        let err = run(sql).await.expect_err(sql);
        assert!(
            matches!(err, SqlError::Unsupported(_)),
            "{sql} 应被明确拒绝，实际：{err:?}"
        );
        assert!(
            err.to_string().contains("EXPLAIN"),
            "拒绝理由要点名 EXPLAIN：{err}"
        );
    }
}
