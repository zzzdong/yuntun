//! **读路径按需保鲜**的端到端验收（`operation-log §163`；`refactor.md` S2-10 的收口）。
//!
//! 守的是一个实证撞到过的形态：**缓存冷、目录已经有**。后台刷新循环是**另一个任务** ——
//! 进程刚起来时它还没被调度到，或者装配层（测试、嵌入式用法）根本没起它，
//! 那段时间里查询拿空缓存去计划，报 `table not found`（`§157`/`§160` 的用例都撞过，
//! 当时都是自己显式 `refresh` 才过的）。
//!
//! 本用例**刻意不起后台刷新任务**（`spawn_background` 一行都不调）⇒ 缓存只可能靠
//! 读路径自己保鲜 ✓ 全程无 `sleep`（不靠"等后台拍子"来蒙对）。
//!
//! 两半：
//!
//! * **冷缓存**：`SELECT` 直接认识"绕过缓存建的表"（以前是 `table not found`）；
//! * **版本变更**：绕过缓存做 DDL 之后，下一次 `SELECT` 立刻看得见新列。

use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use yuntun_model::ops::{CreateTableRequest, EvolveSchemaRequest};
use yuntun_model::schema::SchemaChange;
use yuntun_server::Lakehouse;
use yuntun_sql::SqlResult;
use yuntun_sql::session::SessionCtx;

fn config(wal_dir: &str, store_root: &str) -> yuntun_server::Config {
    yuntun_server::Config::from_toml(&format!(
        r#"
[store]
type = "local"
root = "{store_root}"

[wal]
dir = "{wal_dir}"

[meta]
mode = "memory"

[chunk]
spill_dir = "{wal_dir}/spill"
"#
    ))
    .unwrap()
}

/// 执行一条语句；返回结果行数（`SELECT`）或受影响行数（写语句）。
async fn run(lakehouse: &Lakehouse, session: &mut SessionCtx, sql: &str) -> i64 {
    match lakehouse.sql.execute(sql, session).await {
        Ok(SqlResult::Rows { batches, .. }) => batches.iter().map(|b| b.num_rows() as i64).sum(),
        Ok(SqlResult::Affected(n)) => n,
        Err(e) => panic!("`{sql}` 失败：{e}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_path_refreshes_its_own_cache_before_planning() {
    let guard = yuntun_testkit::TestDir::tmpfs("lazy-refresh");
    let base = guard.string();
    let cfg = config(&format!("{base}/wal"), &format!("{base}/store"));
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    // ⚠️ 刻意**不**调 `spawn_background`：没有任何后台刷新任务 ⇒ 缓存只能靠读路径自己保鲜
    let mut session = SessionCtx::default();

    // ① 绕过缓存建表（走目录句柄，不经过 SQL 的写路径刷新）
    lakehouse
        .catalog
        .create_table(CreateTableRequest {
            name: "t".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, true),
            ])),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig::standard(),
        })
        .await
        .unwrap();
    assert!(
        lakehouse.query.snapshot().get("public.t").is_none(),
        "前提：缓存是冷的（没有任何后台任务刷过它）"
    );

    // ② 查询必须自己把它刷上 —— 以前这里报 `table not found: yuntun.public.t`
    assert_eq!(
        run(&lakehouse, &mut session, "SELECT count(*) FROM t").await,
        1,
        "冷缓存上的第一个查询也要能计划（按需保鲜）"
    );
    assert!(
        lakehouse.query.snapshot().get("public.t").is_some(),
        "按需保鲜之后缓存里应当有这张表"
    );
    let after_first = lakehouse.query.catalog().stats();
    assert_eq!(after_first.lazy_refreshes, 1, "这一次是读路径刷的");
    assert_eq!(
        after_first.freshness_checks, 1,
        "冷缓存不看节流，只问一次版本"
    );

    // ③ 节流：紧接着的第二个查询**连版本都不问**（远端 `version()` 是一次 RPC，
    //    读路径不能把查询 QPS 放大到元数据面）
    assert_eq!(
        run(&lakehouse, &mut session, "SELECT count(*) FROM t").await,
        1
    );
    assert_eq!(
        lakehouse.query.catalog().stats().freshness_checks,
        1,
        "节流窗口内不许再问版本（零开销的那一半）"
    );

    // ④ 版本变更：绕过缓存加一列 ⇒ 下一次查询立刻看得见新列
    lakehouse
        .catalog
        .evolve_schema(EvolveSchemaRequest {
            table: "public.t".into(),
            expected_version: 1,
            change: SchemaChange::AddColumn {
                field: arrow::datatypes::Field::new("c", arrow::datatypes::DataType::Int64, true),
            },
        })
        .await
        .unwrap();
    // 把节流窗口调小，让这条用例确定地走到"问版本 ⇒ 变了 ⇒ 刷"（不靠 sleep）
    lakehouse
        .query
        .catalog()
        .set_freshness_check_interval(std::time::Duration::ZERO);
    // 表里没有数据 ⇒ `SELECT c` 返回 0 行是正常的；**关键是不报错**
    // （缓存没跟上时这里会以 `column c not found` 整条失败）
    assert_eq!(
        run(&lakehouse, &mut session, "SELECT c FROM t").await,
        0,
        "空表上的 SELECT 应当成功返回 0 行"
    );
    // 直接核缓存内容：新列必须在里面（这才是"版本变更被看见了"的硬判据）
    let cached = lakehouse
        .query
        .snapshot()
        .get("public.t")
        .expect("缓存里应当有 t")
        .clone();
    assert!(
        cached.schema.field_with_name("c").is_ok(),
        "缓存里的 schema 必须已经有新列 c：{:?}",
        cached.schema
    );
    assert!(
        lakehouse.query.catalog().stats().lazy_refreshes >= 2,
        "这一次也是读路径刷的：{:?}",
        lakehouse.query.catalog().stats().lazy_refreshes
    );

    shutdown.cancel();
}
