//! **F.2 SQL 形式的 schema 变更**的端到端验收（`plan.md` F.2 / `operation-log §144`）。
//!
//! 守 `plan.md` F.2 的五条验收（一条一个断言组）：
//!
//! ① DDL 后**新写入带新列**；
//! ② **旧文件仍可读**（缺列按 null 对齐）—— 用例先断言"旧数据真的落成了文件"，
//!    否则"旧文件仍可读"这句话根本无从谈起（热数据那条路本来就会 align）；
//! ③ 版本 +1 且**其它表不受影响**（对照表）；
//! ④ OCC：并发两个 DDL **只有一个成功**；
//! ⑤ 破坏性变更（改类型 / 收紧可空性）与"没有载体"的形状（重命名）**被明确拒绝**。
//!
//! 外加两条这个仓库最看重的反面：
//!
//! - **只读形态**（没有写入侧）下 DDL 给**可读拒绝**，绝不静默成功；
//! - `ALTER TABLE` 的 WAL DDL 记录能让**重启后**的表回到新 schema（`[meta] mode="memory"`
//!   下元数据不落盘，此时**只有** WAL 重放这一条路）。

use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use arrow::array::Int64Array;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementUpdate, DoPutUpdateResult};
use arrow_flight::{FlightData, FlightDescriptor, PutResult};
use futures::StreamExt;
use prost::Message;
use yuntun_catalog::CatalogOps;
use yuntun_server::Lakehouse;
use yuntun_sql::session::SessionCtx;
use yuntun_sql::{SqlEngine, SqlError};

// ---------------------------------------------------------------- 夹具

async fn collect_do_get(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    sql: &str,
) -> Vec<arrow::record_batch::RecordBatch> {
    let mut stream = client
        .do_get(arrow_flight::Ticket {
            ticket: sql.as_bytes().to_vec().into(),
        })
        .await
        .unwrap_or_else(|e| panic!("do_get `{sql}` 失败：{e}"))
        .into_inner();
    let mut datas = Vec::new();
    while let Some(fd) = stream.next().await {
        datas.push(fd.unwrap());
    }
    arrow_flight::utils::flight_data_to_batches(&datas).unwrap()
}

/// do_get 执行 SQL 并断言**报错**（错误路径用例）。
async fn expect_do_get_error(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    sql: &str,
) -> String {
    let res = client
        .do_get(arrow_flight::Ticket {
            ticket: sql.as_bytes().to_vec().into(),
        })
        .await;
    match res {
        Ok(_) => panic!("expected error for: {sql}"),
        Err(e) => e.message().to_string(),
    }
}

async fn execute_update(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    sql: &str,
) -> i64 {
    let first = FlightData {
        flight_descriptor: Some(FlightDescriptor {
            r#type: 2, // CMD
            cmd: yuntun_server::flight::command_bytes(&CommandStatementUpdate {
                query: sql.to_string(),
                transaction_id: None,
            })
            .into(),
            path: vec![],
        }),
        ..Default::default()
    };
    let mut acks: tonic::Streaming<PutResult> = client
        .do_put(tokio_stream::iter(vec![first]))
        .await
        .unwrap_or_else(|e| panic!("do_put `{sql}` 失败：{e}"))
        .into_inner();
    let ack = acks.next().await.unwrap().unwrap();
    DoPutUpdateResult::decode(&*ack.app_metadata)
        .unwrap()
        .record_count
}

async fn refresh_cache(lakehouse: &Lakehouse) {
    lakehouse
        .query
        .catalog()
        .refresh(&(lakehouse.catalog.clone()))
        .await
        .unwrap();
}

/// 等"数据**真的落成了文件**"（验收②的前提）。
///
/// 为什么必须等：刚写完的数据在 chunk（热数据）里也读得到 —— 而"旧文件仍可读"这条验收
/// 说的是**文件**那条路（读侧按当前 schema 对齐、缺列填 null）。不等它，用例会变成
/// "热数据能读"的断言，**测不到要测的东西**（"读己之写"本来就是设计保证）。
async fn wait_for_files(lakehouse: &Lakehouse, table: &str) {
    for _ in 0..50 {
        refresh_cache(lakehouse).await;
        let files = lakehouse
            .catalog
            .list_visible_files(table, lakehouse.catalog.current_snapshot().await, None)
            .await
            .unwrap();
        if !files.is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("{table} 始终没有可见文件：等不到 flush，验收②就无从谈起");
}

/// 表当前 schema 版本（**权威目录**，不是查询缓存）。
async fn version_of(lakehouse: &Lakehouse, table: &str) -> u64 {
    lakehouse
        .catalog
        .get_table(table)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("表 {table} 不存在"))
        .current_schema_version
}

/// 取一个整数结果（`Int64` 或 `Int32` —— `ADD COLUMN c INT` 落的是 `Int32`）。
fn int_of(batches: &[arrow::record_batch::RecordBatch], col: usize) -> i64 {
    let a = batches[0].column(col);
    if let Some(x) = a.as_any().downcast_ref::<Int64Array>() {
        return x.value(0);
    }
    if let Some(x) = a.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return x.value(0) as i64;
    }
    panic!("期望整数结果列，实际 {:?}", a.data_type());
}

fn config(wal_dir: &str, store_root: &str) -> yuntun_server::Config {
    yuntun_server::Config::from_toml(&format!(
        r#"
[store]
type = "local"
root = "{store_root}"

[wal]
dir = "{wal_dir}"

[chunk]
# 每个测试 = 一个节点：私有目录（spill）必须各用各的，否则闸门会（正确地）拒绝第二个消费者
spill_dir = "{wal_dir}/spill"

[ingest]
rows_threshold = 1
time_threshold_secs = 5
max_flush_delay_secs = 1
flush_phase_spread_secs = 0
scan_interval_ms = 20
"#
    ))
    .unwrap()
}

async fn serve(
    lakehouse: &Arc<Lakehouse>,
    shutdown: CancellationToken,
) -> FlightServiceClient<tonic::transport::Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let catalog: Arc<dyn CatalogOps> = lakehouse.catalog.clone();
    let svc = arrow_flight::flight_service_server::FlightServiceServer::new(
        yuntun_server::FlightServer::new(
            lakehouse.ingestor.clone(),
            lakehouse.query.clone(),
            catalog,
        ),
    );
    let sd = shutdown.clone();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move { sd.cancelled().await },
            )
            .await
            .unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    FlightServiceClient::new(channel)
}

// ---------------------------------------------------------------- 验收 ①–③⑤

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_and_drop_column_end_to_end() {
    let base_guard = yuntun_testkit::TestDir::tmpfs("sql-alter-e2e");
    let base = base_guard.string();
    let cfg = config(&format!("{base}/wal"), &format!("{base}/store"));
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let _bg = lakehouse.spawn_background(&cfg);
    let mut client = serve(&lakehouse, shutdown.clone()).await;

    // 建表 + 一张**对照表**（验收③：其它表不受影响）
    execute_update(&mut client, "CREATE TABLE t (a BIGINT NOT NULL, b VARCHAR)").await;
    execute_update(&mut client, "CREATE TABLE other (x BIGINT)").await;
    let other_before = version_of(&lakehouse, "public.other").await;
    assert_eq!(version_of(&lakehouse, "public.t").await, 1);

    // 旧数据：两行，**没有 c 列** —— 这就是"旧 schema_version 的文件"
    let n = execute_update(&mut client, "INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')").await;
    assert_eq!(n, 2);
    // 这一步是验收②的**前提**：旧数据必须先真的落成文件（否则测的还是热数据那条路）
    wait_for_files(&lakehouse, "public.t").await;

    // ---- ①/③ ALTER TABLE t ADD COLUMN c INT ----
    let n = execute_update(&mut client, "ALTER TABLE t ADD COLUMN c INT").await;
    assert_eq!(n, 0, "DDL 的受影响行数是 0（不是「没执行」）");
    assert_eq!(
        version_of(&lakehouse, "public.t").await,
        2,
        "ADD COLUMN 必须让表 schema 版本 +1"
    );
    assert_eq!(
        version_of(&lakehouse, "public.other").await,
        other_before,
        "其它表的版本**不该**被这次 DDL 推动"
    );

    // 新写入带新列
    let n = execute_update(&mut client, "INSERT INTO t (a, b, c) VALUES (3, 'z', 30)").await;
    assert_eq!(n, 1);
    tokio::time::sleep(Duration::from_millis(700)).await;
    refresh_cache(&lakehouse).await;

    // ---- ② 旧文件仍可读：行数不变，缺列按 null 对齐 ----
    let b = collect_do_get(
        &mut client,
        "SELECT count(*) AS c, sum(a) AS s FROM yuntun.public.t",
    )
    .await;
    assert_eq!(int_of(&b, 0), 3, "旧文件的行不能因为加列而消失");
    assert_eq!(int_of(&b, 1), 6);
    let b = collect_do_get(
        &mut client,
        "SELECT count(*) FROM yuntun.public.t WHERE c IS NULL",
    )
    .await;
    assert_eq!(int_of(&b, 0), 2, "旧文件缺 c 列 ⇒ 读出来是 NULL");
    let b = collect_do_get(&mut client, "SELECT c FROM yuntun.public.t WHERE a = 3").await;
    assert_eq!(int_of(&b, 0), 30, "新写入的行带新列的值");

    // ---- DROP COLUMN：逻辑删除（旧文件不动，读侧按当前 schema 对齐）----
    execute_update(&mut client, "ALTER TABLE t DROP COLUMN b").await;
    assert_eq!(version_of(&lakehouse, "public.t").await, 3);
    refresh_cache(&lakehouse).await;
    let b = collect_do_get(&mut client, "SELECT count(*) FROM yuntun.public.t").await;
    assert_eq!(int_of(&b, 0), 3, "删列是逻辑删除：行数一个不少");
    let b = collect_do_get(&mut client, "SELECT a FROM yuntun.public.t ORDER BY a").await;
    assert_eq!(int_of(&b, 0), 1);
    // 删掉的列**不可再查**：明确报错，而不是静默给一列 NULL
    let msg = expect_do_get_error(&mut client, "SELECT b FROM yuntun.public.t").await;
    assert!(
        msg.contains("b") || msg.contains("not found") || msg.contains("No field"),
        "查询已删除的列必须明确失败：{msg}"
    );

    shutdown.cancel();
    for h in _bg {
        let _ = h.await;
    }
}

/// 验收⑤ + 幂等形态：**破坏性变更被明确拒绝**；`IF (NOT) EXISTS` 语义正确。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn destructive_and_unrepresentable_ddl_are_rejected() {
    let base_guard = yuntun_testkit::TestDir::tmpfs("sql-alter-reject");
    let base = base_guard.string();
    let cfg = config(&format!("{base}/wal"), &format!("{base}/store"));
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let _bg = lakehouse.spawn_background(&cfg);
    let mut client = serve(&lakehouse, shutdown.clone()).await;

    execute_update(&mut client, "CREATE TABLE t (a BIGINT, b VARCHAR)").await;
    let v1 = version_of(&lakehouse, "public.t").await;

    // 破坏性 / 无载体 ⇒ **明确拒绝**（报错要能看出"为什么"，不是一句"不支持"）
    for (sql, want) in [
        ("ALTER TABLE t ALTER COLUMN a TYPE VARCHAR", "破坏性"),
        ("ALTER TABLE t MODIFY COLUMN a VARCHAR", "破坏性"),
        ("ALTER TABLE t ALTER COLUMN a SET NOT NULL", "载体"),
        ("ALTER TABLE t RENAME COLUMN b TO bb", "载体"),
        ("ALTER TABLE t RENAME TO t2", "载体"),
    ] {
        let msg = expect_do_get_error(&mut client, sql).await;
        assert!(
            msg.contains(want),
            "`{sql}` 的拒绝理由要点名（期望含 `{want}`），实际：{msg}"
        );
    }
    // **拒绝 = 没改**：版本不许动
    assert_eq!(
        version_of(&lakehouse, "public.t").await,
        v1,
        "被拒绝的 DDL 不得改动 schema 版本"
    );

    // 目标态已满足 + 没写 IF ⇒ 明确报错（绝不静默成功）
    let msg = expect_do_get_error(&mut client, "ALTER TABLE t ADD COLUMN a INT").await;
    assert!(msg.contains("已满足"), "重复加列必须报错并说清原因：{msg}");
    let msg = expect_do_get_error(&mut client, "ALTER TABLE t DROP COLUMN nope").await;
    assert!(msg.contains("已满足"), "删不存在的列必须报错：{msg}");
    assert_eq!(version_of(&lakehouse, "public.t").await, v1);

    // 写了 IF ⇒ 幂等：不报错、**也不推版本**（目标态本来就满足）
    execute_update(&mut client, "ALTER TABLE t ADD COLUMN IF NOT EXISTS a INT").await;
    execute_update(&mut client, "ALTER TABLE t DROP COLUMN IF EXISTS nope").await;
    assert_eq!(
        version_of(&lakehouse, "public.t").await,
        v1,
        "IF NOT EXISTS / IF EXISTS 命中时不该推版本（没有真实变更）"
    );

    // 表不存在 ⇒ NotFound（不是"静默成功"）
    let msg = expect_do_get_error(&mut client, "ALTER TABLE nope ADD COLUMN c INT").await;
    assert!(
        msg.contains("nope"),
        "目标表不存在时要点名表名，实际：{msg}"
    );

    // 删到零列 ⇒ 拒绝（要清空整表请 DROP TABLE）。单列表才可能触发，故单列建一张
    execute_update(&mut client, "CREATE TABLE single_col (only_col BIGINT)").await;
    let msg = expect_do_get_error(&mut client, "ALTER TABLE single_col DROP COLUMN only_col").await;
    assert!(
        msg.contains("最后一列"),
        "删最后一列要被单独拒绝，实际：{msg}"
    );

    shutdown.cancel();
    for h in _bg {
        let _ = h.await;
    }
}

/// **只读形态**（没有写入侧）：DDL 给**可读拒绝**，绝不静默成功（`§74` 的同一件事）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ddl_on_readonly_engine_is_readably_rejected() {
    let base_guard = yuntun_testkit::TestDir::tmpfs("sql-alter-readonly");
    let base = base_guard.string();
    let cfg = config(&format!("{base}/wal"), &format!("{base}/store"));
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let mut client = serve(&lakehouse, shutdown.clone()).await;
    execute_update(&mut client, "CREATE TABLE t (a BIGINT)").await;

    // 只读装配：`new_readonly`（查询节点形态 —— 没有 Ingestor，策略也声明只读）
    let ro = SqlEngine::new_readonly(lakehouse.query.clone(), lakehouse.catalog.clone());
    let mut session = SessionCtx::default();
    let err = match ro.execute("ALTER TABLE t ADD COLUMN c INT", &mut session).await {
        Ok(_) => panic!("只读形态必须拒绝 DDL（绝不能静默成功）"),
        Err(e) => e,
    };
    assert!(
        matches!(err, SqlError::ReadOnly),
        "必须是**可读的** ReadOnly 拒绝（不能静默成功、也不能是内部错误）：{err:?}"
    );

    // 对照：同一份 SQL 在**有写入侧**的引擎上能跑（证明拒绝来自策略/能力，而不是这条语句本身）
    let mut session = SessionCtx::default();
    lakehouse
        .sql
        .execute("ALTER TABLE t ADD COLUMN c INT", &mut session)
        .await
        .expect("有写入侧的引擎上 ADD COLUMN 应当成功");
    assert_eq!(version_of(&lakehouse, "public.t").await, 2);

    shutdown.cancel();
}

/// 验收④：**并发两个 DDL 只有一个成功**（OCC 的语义在 SQL 层真的被用上）。
///
/// 为什么用"同一条 `ADD COLUMN c` 并发两遍"来构造：它的两种交错都只有**一个**赢家 ——
/// 要么撞在 OCC 上（`SchemaChanged`），要么撞在"目标态已满足"上。于是这条断言
/// **不依赖调度时序**，可以常绿地钉住"恰好一个成功"。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_ddl_has_exactly_one_winner() {
    let base_guard = yuntun_testkit::TestDir::tmpfs("sql-alter-occ");
    let base = base_guard.string();
    let cfg = config(&format!("{base}/wal"), &format!("{base}/store"));
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );

    for i in 0..3 {
        let table = format!("occ{i}");
        let mut s = SessionCtx::default();
        lakehouse
            .sql
            .execute(&format!("CREATE TABLE {table} (a BIGINT)"), &mut s)
            .await
            .unwrap();
        let v0 = version_of(&lakehouse, &format!("public.{table}")).await;

        let sql = format!("ALTER TABLE {table} ADD COLUMN c INT");
        let mut s1 = SessionCtx::default();
        let mut s2 = SessionCtx::default();
        let fut_a = lakehouse.sql.execute(&sql, &mut s1);
        let fut_b = lakehouse.sql.execute(&sql, &mut s2);
        let (ra, rb) = tokio::join!(fut_a, fut_b);
        // "恰好一个成功"由这个 match 自己守：两个都成功 / 两个都失败都会当场炸掉
        let msg = match (ra, rb) {
            (Ok(_), Err(e)) | (Err(e), Ok(_)) => e.to_string(),
            (Ok(_), Ok(_)) => panic!("第 {i} 轮：并发两个 DDL 都成功了 —— OCC 没起作用"),
            (Err(a), Err(b)) => panic!("第 {i} 轮：并发两个 DDL 都失败了：{a} / {b}"),
        };
        assert!(
            msg.contains("版本冲突") || msg.contains("已满足"),
            "输家拿到的必须是**可读**的冲突（OCC 冲突 / 目标态已满足），实际：{msg}"
        );

        // 只能 +1 一次，且列只有一个
        assert_eq!(
            version_of(&lakehouse, &format!("public.{table}")).await,
            v0 + 1,
            "第 {i} 轮：版本只该前进一次"
        );
        let meta = lakehouse
            .catalog
            .get_table(&format!("public.{table}"))
            .await
            .unwrap()
            .unwrap();
        let schema = meta.schema().unwrap();
        assert_eq!(
            schema
                .fields()
                .iter()
                .filter(|f| f.name() == "c")
                .count(),
            1,
            "第 {i} 轮：列 c 只能出现一次"
        );
    }

    shutdown.cancel();
}

/// `ALTER TABLE` 的 WAL DDL 记录：**重启后表回到新 schema**。
///
/// 用 `[meta] mode="memory"`（元数据不落盘）⇒ 重启后目录是空的，
/// **只有 WAL 重放**这一条路能让表回到新 schema —— 否则这条断言测不出东西。
///
/// 两条 ALTER（加列 + 删列）都要重放：它们分别走 `converge_schema` 的两个分支
/// （`classify` 报 `NeedsEvolve` / `classify` 说 `Compatible` 但当前 schema 有**多余**列）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_survives_restart_via_wal_ddl_replay() {
    let base_guard = yuntun_testkit::TestDir::tmpfs("sql-alter-replay");
    let base = base_guard.string();
    let cfg = yuntun_server::Config::from_toml(&format!(
        r#"
[store]
type = "local"
root = "{base}/store"

[wal]
dir = "{base}/wal"

[chunk]
spill_dir = "{base}/wal/spill"

[meta]
# **关键**：内存形态 ⇒ 重启后目录为空，恢复只能靠 WAL 里的 DDL 记录
mode = "memory"

[ingest]
rows_threshold = 1
time_threshold_secs = 5
max_flush_delay_secs = 1
flush_phase_spread_secs = 0
scan_interval_ms = 20
"#
    ))
    .unwrap();

    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let _bg = lakehouse.spawn_background(&cfg);
    let mut client = serve(&lakehouse, shutdown.clone()).await;
    execute_update(&mut client, "CREATE TABLE t (a BIGINT, b VARCHAR)").await;
    execute_update(&mut client, "ALTER TABLE t ADD COLUMN c VARCHAR").await;
    execute_update(&mut client, "ALTER TABLE t DROP COLUMN b").await;
    assert_eq!(version_of(&lakehouse, "public.t").await, 3);

    // ---- 真重启（等旧节点真的停下 —— 否则私有目录会被新节点正确地拒绝）----
    shutdown.cancel();
    for h in _bg {
        let _ = h.await;
    }
    drop(client);
    drop(lakehouse);
    let lakehouse = Arc::new(Lakehouse::build(&cfg).await.unwrap());

    // 表在（CREATE 重放）**且**回到 v3 = `[a, c]`（两条 ALTER 重放：收敛到各自的目标态）
    let meta = lakehouse
        .catalog
        .get_table("public.t")
        .await
        .unwrap()
        .expect("表应经 WAL DDL 重放恢复");
    assert_eq!(
        meta.current_schema_version, 3,
        "两条 ALTER 的 DDL 记录都必须被重放"
    );
    let schema = meta.schema().unwrap();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["a", "c"], "加列与删列都要收敛到目标态");
    assert_eq!(
        schema.field_with_name("c").unwrap().data_type(),
        &arrow::datatypes::DataType::Utf8,
        "重放后的列类型要与 DDL 时一致"
    );
}
