//! **查询管理面**的判据（U4 / `§191`）：`SHOW PROCESSLIST` 与 `KILL`。
//!
//! # 为什么判据要长这样
//!
//! 这两个语句最危险的失败形态都是**静默**：
//!
//! * `SHOW PROCESSLIST` 返回空集 / 漏掉自己 ⇒ 运维以为"没有查询在跑"；
//! * `KILL` 打中一个不存在的 Id 却返回 OK ⇒ 脚本以为杀掉了（实际还在烧 CPU）。
//!
//! 所以除了"正例"，每条都配一个**反证**：未知 Id 必须报 1094 语义；取消位必须真的
//! 被点，且**只对当前这条**生效（下一条查询要重置）。
//!
//! 这里用 `SqlEngine` + 手工登记的会话（与 `sqlwire` 的用法一致）；真正的
//! "慢查询被 KILL 打断"由进程级冒烟 `scripts/standalone_smoke.py` 覆盖。

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use futures::StreamExt as _;
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_query::{LocalCatalog, QueryEngine};
use yuntun_sql::registry::QueryRegistry;
use yuntun_sql::session::SessionCtx;
use yuntun_sql::{SqlEngine, SqlError, SqlResult};
use yuntun_store::{StoreConfig, create_store};

fn engine() -> SqlEngine {
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    SqlEngine::new_readonly(
        Arc::new(QueryEngine::new(
            create_store(&StoreConfig::Memory).expect("memory store"),
            Arc::new(LocalCatalog::new()),
        )),
        catalog,
    )
}

/// 从结果集里按列名取一行（`SHOW PROCESSLIST` 的列顺序是 MySQL 契约，不能靠猜）。
fn rows_of(out: SqlResult) -> (Vec<String>, Vec<arrow::record_batch::RecordBatch>) {
    let SqlResult::Rows {
        schema, batches, ..
    } = out
    else {
        panic!("应当是结果集");
    };
    let names = schema.fields().iter().map(|f| f.name().clone()).collect();
    (names, batches)
}

fn col_str(batch: &arrow::record_batch::RecordBatch, name: &str, row: usize) -> String {
    let idx = batch.schema().index_of(name).expect("列名必须存在");
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("{name} 应是 Utf8"));
    arr.value(row).to_string()
}

fn col_i64(batch: &arrow::record_batch::RecordBatch, name: &str, row: usize) -> i64 {
    let idx = batch.schema().index_of(name).expect("列名必须存在");
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("{name} 应是 Int64"))
        .value(row)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn processlist_has_mysql_columns_and_shows_queries_and_sleepers() {
    let engine = engine();
    let registry: Arc<QueryRegistry> = Arc::clone(engine.registry());
    let id = registry.alloc_id();
    let conn = registry.attach(id, "10.0.0.7:51234");
    conn.set_user("alice");
    // 模拟"alice 正在跑一条查询"
    let running = conn.begin("SELECT * FROM sales.orders", "sales");

    let mut session = SessionCtx::mysql();
    let out = engine
        .execute("SHOW PROCESSLIST", &mut session)
        .await
        .expect("SHOW PROCESSLIST 必须被支持");
    let (names, batches) = rows_of(out);
    assert_eq!(
        names,
        vec![
            "Id", "User", "Host", "db", "Command", "Time", "State", "Info"
        ],
        "列名是 MySQL 驱动按名取值的契约"
    );
    let batch = &batches[0];

    // ① alice 那条在，且是 Query + 原始 SQL
    let mut found = false;
    for row in 0..batch.num_rows() {
        if col_i64(batch, "Id", row) == id as i64 {
            found = true;
            assert_eq!(col_str(batch, "User", row), "alice");
            assert_eq!(col_str(batch, "Host", row), "10.0.0.7:51234");
            assert_eq!(col_str(batch, "db", row), "sales");
            assert_eq!(col_str(batch, "Command", row), "Query");
            assert_eq!(col_str(batch, "Info", row), "SELECT * FROM sales.orders");
        }
    }
    assert!(found, "正在跑的查询必须出现在进程列表里");

    // ② **自己**这条 `SHOW PROCESSLIST` 也在（反证"表里只有手工登记的那条"）
    let mut own = false;
    for row in 0..batch.num_rows() {
        if col_str(batch, "Info", row) == "SHOW PROCESSLIST" {
            own = true;
            assert_eq!(col_str(batch, "Command", row), "Query");
        }
    }
    assert!(own, "执行中的 SHOW PROCESSLIST 本身也应当可见");

    // ③ 查询结束后同一条连接变成 Sleep（Id 不变 —— 客户端拿它继续 KILL）
    drop(running);
    let out = engine
        .execute("SHOW PROCESSLIST", &mut session)
        .await
        .unwrap();
    let (_, batches) = rows_of(out);
    let batch = &batches[0];
    let mut slept = false;
    for row in 0..batch.num_rows() {
        if col_i64(batch, "Id", row) == id as i64 {
            slept = true;
            assert_eq!(col_str(batch, "Command", row), "Sleep");
            assert!(col_str(batch, "Info", row).is_empty());
        }
    }
    assert!(slept, "空闲连接仍应在表里（Command=Sleep）");
}

/// 长连接的 Id **跨查询稳定**：`sqlwire` 把连接 Id 放进会话，引擎据此复用登记。
///
/// 反证形态：如果引擎每次都新建临时会话（`lookup` 失败 / 借用句柄误注销），
/// 进程列表里会出现 host=flight 的幽灵行，客户端拿到的 Id 第二条查询就失效。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_registered_connection_keeps_its_id_across_queries() {
    let engine = engine();
    let registry: Arc<QueryRegistry> = Arc::clone(engine.registry());
    let id = registry.alloc_id();
    let conn = registry.attach(id, "10.0.0.9:3306");
    conn.set_user("bob");

    let mut session = SessionCtx::mysql();
    session.conn_id = Some(id);
    for _ in 0..2 {
        engine
            .execute("SHOW DATABASES", &mut session)
            .await
            .unwrap();
        let rows = registry.snapshot();
        assert_eq!(
            rows.len(),
            1,
            "只应有一条连接（不能每次查询都新建临时会话）"
        );
        assert_eq!(rows[0].id, id, "Id 必须跨查询稳定");
        assert_eq!(rows[0].host, "10.0.0.9:3306");
        assert_eq!(rows[0].user, "bob");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_query_cancels_only_the_target_and_rejects_unknown_ids() {
    let engine = engine();
    let registry: Arc<QueryRegistry> = Arc::clone(engine.registry());
    let victim_id = registry.alloc_id();
    let victim = registry.attach(victim_id, "h1");
    let other_id = registry.alloc_id();
    let other = registry.attach(other_id, "h2");
    let vq = victim.begin("SELECT slow", "public");
    let oq = other.begin("SELECT unrelated", "public");

    let mut session = SessionCtx::mysql();
    let out = engine
        .execute(&format!("KILL QUERY {victim_id}"), &mut session)
        .await
        .expect("KILL QUERY 已知 Id 必须成功");
    assert!(matches!(out, SqlResult::Affected(0)));

    // ① 目标被点、旁观者不受影响
    assert!(vq.is_cancelled(), "目标查询必须被取消");
    assert!(!oq.is_cancelled(), "别的查询**不能**被误杀");

    // ② 未知 Id ⇒ 1094 语义（不静默成功）
    match engine.execute("KILL QUERY 424242", &mut session).await {
        Err(SqlError::UnknownThread(id)) => assert_eq!(id, 424242),
        Err(e) => panic!("期望 UnknownThread，实际错误 {e}"),
        Ok(_) => panic!("期望 UnknownThread，实际执行成功"),
    }

    // ③ 取消位是**每查询**的：下一条查询不能被上一条的 KILL 波及
    drop(vq);
    let next = victim.begin("SELECT next", "public");
    assert!(!next.is_cancelled(), "取消位必须在新查询开始时重置");

    // ④ KILL CONNECTION 立断连旗标；KILL QUERY 不立
    engine
        .execute(&format!("KILL {other_id}"), &mut session)
        .await
        .expect("KILL <id> = KILL CONNECTION");
    assert!(other.kill_requested());
    assert!(!victim.kill_requested());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_interrupts_a_stream_that_has_not_started_yet() {
    // 流式路径的取消挂在流上（`CancellableStream`）：KILL 之后**第一次 poll** 必须
    // 得到错误，而不是"流悄悄结束"（截断的流会被客户端当成完整结果）。
    let engine = engine();
    let registry: Arc<QueryRegistry> = Arc::clone(engine.registry());
    let id = registry.alloc_id();
    let conn = registry.attach(id, "h");
    let mut session = SessionCtx::mysql();
    session.conn_id = Some(id);

    let out = engine
        .execute_stream("SELECT 1 AS one", &mut session)
        .await
        .expect("流式查询");
    let yuntun_sql::SqlStreamResult::Rows { mut stream, .. } = out else {
        panic!("SELECT 应当是流式结果集");
    };

    registry.kill(id, false).expect("KILL QUERY");
    match stream.next().await {
        Some(Err(e)) => {
            let msg = e.to_string();
            assert!(msg.contains("interrupted"), "错误应说明被打断：{msg}");
        }
        Some(Ok(b)) => panic!("KILL 之后不能照常返回数据（{} 行）", b.num_rows()),
        None => panic!("KILL 之后不能伪装成'流正常结束'"),
    }
    let _ = conn;
}

/// `SHOW STATUS` 是**真实计数器**（`§192`），不是 canned 常量；`max_connections`
/// 在 `SHOW VARIABLES` / `SELECT @@` 与准入闸门之间必须同一个数。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_and_variable_come_from_the_registry() {
    let engine = engine().with_max_connections(7);
    assert_eq!(engine.registry().max_connections(), 7);

    let registry: Arc<QueryRegistry> = Arc::clone(engine.registry());
    let id = registry.alloc_id();
    let conn = registry.attach(id, "h");
    let mut session = SessionCtx::mysql();

    let out = engine.execute("SHOW STATUS", &mut session).await.unwrap();
    let (names, batches) = rows_of(out);
    assert_eq!(names, vec!["Variable_name", "Value"]);
    let batch = &batches[0];
    let get = |var: &str| -> String {
        for row in 0..batch.num_rows() {
            if col_str(batch, "Variable_name", row) == var {
                return col_str(batch, "Value", row);
            }
        }
        panic!("SHOW STATUS 缺 `{var}` 行");
    };
    // 这条 SHOW STATUS 自己挂在一条**临时会话**上（裸引擎：没有 conn_id）：
    // `Threads_connected` 只数客户端连接（=1），`Threads_running` 含它自己（=1）。
    assert_eq!(get("Threads_connected"), "1", "一条客户端连接");
    assert_eq!(get("Threads_running"), "1", "正在跑的就是 SHOW STATUS 自己");
    assert_eq!(get("Max_connections"), "7");
    assert!(
        get("Questions").parse::<u64>().unwrap() >= 1,
        "SHOW STATUS 自己也算一条语句"
    );

    // 再加一条在跑的查询 ⇒ Threads_running 必须变成 2（真有别的活）
    let running = conn.begin("SELECT 1", "public");
    let out = engine.execute("SHOW STATUS", &mut session).await.unwrap();
    let (_, batches) = rows_of(out);
    let batch = &batches[0];
    let get = |var: &str| -> String {
        for row in 0..batch.num_rows() {
            if col_str(batch, "Variable_name", row) == var {
                return col_str(batch, "Value", row);
            }
        }
        panic!("SHOW STATUS 缺 `{var}` 行");
    };
    assert_eq!(get("Threads_running"), "2", "两条查询同时在跑");

    // `LIKE` 在服务端过滤；不存在的名字必须是**空集**（返回全部 = 让"读不到"伪装成"读到了"）
    let out = engine
        .execute("SHOW STATUS LIKE 'Threads%'", &mut session)
        .await
        .unwrap();
    let (_, batches) = rows_of(out);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2, "Threads_connected / Threads_running");
    assert!(batch.num_rows() < 8, "LIKE 必须真的过滤");

    let out = engine
        .execute("SHOW VARIABLES LIKE 'max_connections'", &mut session)
        .await
        .unwrap();
    let (_, batches) = rows_of(out);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(col_str(batch, "Value", 0), "7");

    let out = engine
        .execute("SHOW VARIABLES LIKE 'no_such_variable'", &mut session)
        .await
        .unwrap();
    let (_, batches) = rows_of(out);
    assert_eq!(batches[0].num_rows(), 0, "不存在的变量必须是空集");

    // 驱动的探测表达式与 SHOW VARIABLES 同一事实源（列名 = 变量名，值是字符串）
    let out = engine
        .execute("SELECT @@max_connections", &mut session)
        .await
        .unwrap();
    let (_, batches) = rows_of(out);
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(col_str(&batches[0], "max_connections", 0), "7");

    drop(running);
    drop(conn);
    assert_eq!(registry.status().threads_connected, 0);
}

/// 一道准入闸门的**语义**（`§192`）：超限拒绝、断开后归还。
///
/// 这里只测注册表这一层的计数与配额事实；"socket 那侧真的被关"由
/// `server/tests/sql_limits_e2e.rs` 进程内验证。
#[test]
fn admission_counters_are_observable() {
    let r = Arc::new(QueryRegistry::new());
    r.set_max_connections(2);
    assert_eq!(r.max_connections(), 2);
    r.set_max_connections(0);
    assert_eq!(r.max_connections(), 1, "0 不是无限，clamp 到 1");

    let a = r.attach(r.alloc_id(), "a");
    r.note_connection();
    let b = r.attach(r.alloc_id(), "b");
    r.note_connection();
    r.note_rejected();
    let s = r.status();
    assert_eq!(s.threads_connected, 2);
    assert_eq!(s.connections_total, 2);
    assert_eq!(s.max_used_connections, 2);
    assert_eq!(s.connection_errors_max_connections, 1);

    drop(a);
    assert_eq!(r.status().threads_connected, 1, "断开必须归还");
    drop(b);
    assert_eq!(r.status().threads_connected, 0);
}
