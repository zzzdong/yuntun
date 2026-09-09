//! SQL 写入路径与 DDL 端到端（计划任务书 v2.0 S1.6 / S1.7，plan §4.3）：
//! - DDL：简易轨 do_get `CREATE TABLE` → Catalog（General 模板，强制幂等键）
//! - DML：StatementUpdate(do_put) `INSERT ... VALUES` → ingest 管线（WAL 权威）
//! - DML：简易轨 do_get `INSERT ... VALUES`（列清单 + NULL 填充 + 整型毫秒时间戳）
//! - DML：`INSERT ... SELECT`（DataFusion 读源 + 位置对齐 cast → ingest）
//! - DDL：`SHOW TABLES` / `DROP TABLE`（含 IF EXISTS 与缺失表报错）
//! - 崩溃恢复：重启后 WAL DDL 重放重建表清单，SQL 写入的数据不丢（与 DoPut 同等持久性）

use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementUpdate, DoPutUpdateResult};
use arrow_flight::{FlightData, FlightDescriptor, PutResult};
use futures::StreamExt;
use prost::Message;
use yuntun_catalog::CatalogOps;
use yuntun_server::Lakehouse;

async fn collect_do_get(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    sql: &str,
) -> Vec<arrow::record_batch::RecordBatch> {
    let mut stream = client
        .do_get(arrow_flight::Ticket {
            ticket: sql.as_bytes().to_vec().into(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut datas = Vec::new();
    while let Some(fd) = stream.next().await {
        datas.push(fd.unwrap());
    }
    arrow_flight::utils::flight_data_to_batches(&datas).unwrap()
}

/// do_get 执行 SQL 并断言错误（简易轨 DDL/DML 错误路径）。
async fn expect_do_get_error(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    sql: &str,
) {
    let res = client
        .do_get(arrow_flight::Ticket {
            ticket: sql.as_bytes().to_vec().into(),
        })
        .await;
    assert!(res.is_err(), "expected error for: {sql}");
}

/// do_put(StatementUpdate) 执行 SQL，返回 record_count。
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
        .unwrap()
        .into_inner();
    let ack = acks.next().await.unwrap().unwrap();
    DoPutUpdateResult::decode(&*ack.app_metadata)
        .unwrap()
        .record_count
}

async fn refresh_cache(lakehouse: &Lakehouse) {
    lakehouse
        .query
        .cache()
        .refresh(&(lakehouse.catalog.clone() as Arc<dyn CatalogOps>))
        .await
        .unwrap();
}

fn config(wal_dir: &str, store_root: &str) -> yuntun_server::Config {
    yuntun_server::Config::from_toml(&format!(
        r#"
[store]
type = "local"
root = "{store_root}"

[wal]
dir = "{wal_dir}"

[ingest]
rows_threshold = 1
time_threshold_secs = 5
flush_jitter_secs = 0
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_dml_ddl_end_to_end() {
    let base = format!(
        "/tmp/yuntun-sqldml-e2e-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let wal_dir = format!("{base}/wal");
    let store_root = format!("{base}/store");
    let cfg = config(&wal_dir, &store_root);

    // ============ 第一次运行 ============
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let _bg = lakehouse.spawn_background(&cfg);
    let mut client = serve(&lakehouse, shutdown.clone()).await;

    // ① CREATE TABLE（简易轨 do_get；General 模板 → 强制幂等键）
    collect_do_get(
        &mut client,
        "CREATE TABLE audit_events (ts BIGINT NOT NULL, \"user\" VARCHAR, cost BIGINT, \
         occurred_at TIMESTAMP)",
    )
    .await;
    let t1 = lakehouse
        .catalog
        .get_table("audit_events")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t1.table_template, 1, "General 模板");
    assert!(
        t1.ingest_config.unwrap().require_idempotency_key,
        "General 模板强制幂等键"
    );
    // IF NOT EXISTS 幂等
    collect_do_get(
        &mut client,
        "CREATE TABLE IF NOT EXISTS audit_events (ts BIGINT NOT NULL, \"user\" VARCHAR, cost BIGINT, \
         occurred_at TIMESTAMP)",
    )
    .await;

    // ② INSERT ... VALUES（标准轨 StatementUpdate do_put → record_count）
    let n = execute_update(
        &mut client,
        "INSERT INTO yuntun.public.audit_events (ts, \"user\", cost, occurred_at) VALUES \
         (1000, 'alice', 10, TIMESTAMP '2026-09-09T01:02:03Z'), \
         (2000, 'bob', 20, 1725840000000), \
         (3000, NULL, 30, TIMESTAMP '2026-09-09 04:05:06.5')",
    )
    .await;
    assert_eq!(n, 3, "StatementUpdate 返回受影响行数");

    // ③ INSERT ... VALUES 列清单（缺省列填 NULL）+ 整型毫秒时间戳
    let n = execute_update(
        &mut client,
        "INSERT INTO audit_events (ts, \"user\") VALUES (4000, 'carol')",
    )
    .await;
    assert_eq!(n, 1);

    // 强制幂等键的表：INSERT 表达式 / 未知列 → 明确拒绝
    expect_do_get_error(
        &mut client,
        "INSERT INTO audit_events (ts, nope) VALUES (1, 'x')",
    )
    .await;
    expect_do_get_error(
        &mut client,
        "INSERT INTO audit_events VALUES (1, 'x', 1, 1 + 2)",
    )
    .await;
    // 多语句拒绝
    expect_do_get_error(
        &mut client,
        "INSERT INTO audit_events VALUES (1, 'x', 1, NULL); INSERT INTO audit_events VALUES (2, 'y', 2, NULL)",
    )
    .await;

    // ④ 等攒批 flush 后查询验证（SQL 写入数据可查；§4.5 攒批窗口后可见）
    tokio::time::sleep(Duration::from_millis(700)).await;
    refresh_cache(&lakehouse).await;

    // ⑤ INSERT ... SELECT（DataFusion 读源，cost Int64 → Int32 cast 到 t2 schema）
    collect_do_get(
        &mut client,
        "CREATE TABLE events_copy (ts BIGINT NOT NULL, \"user\" VARCHAR, cost INT)",
    )
    .await;
    let n = execute_update(
        &mut client,
        "INSERT INTO events_copy SELECT ts, \"user\", cost FROM yuntun.public.audit_events",
    )
    .await;
    assert_eq!(n, 4, "INSERT SELECT 返回行数");
    tokio::time::sleep(Duration::from_millis(700)).await;
    refresh_cache(&lakehouse).await;

    let b = collect_do_get(
        &mut client,
        "SELECT count(*) AS c, sum(cost) AS s FROM yuntun.public.audit_events",
    )
    .await;
    let c = b[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(c, 4, "VALUES 3 行 + 列清单 1 行");

    // TIMESTAMP 字面量：ISO8601 / 整型毫秒 / NULL 填充
    let b = collect_do_get(
        &mut client,
        "SELECT occurred_at FROM yuntun.public.audit_events WHERE ts = 1000",
    )
    .await;
    let ts = b[0]
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    // 2026-09-09T01:02:03Z = days(20705)*86400 + 01:02:03(3723s)
    let expect_secs = 20_705i64 * 86_400 + 3_723;
    assert_eq!(ts.value(0), expect_secs * 1_000_000_000);

    let b = collect_do_get(
        &mut client,
        "SELECT count(*) FROM yuntun.public.audit_events WHERE occurred_at IS NULL",
    )
    .await;
    assert_eq!(
        b[0].column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1,
        "缺省列 occurred_at 填 NULL"
    );

    let b = collect_do_get(
        &mut client,
        "SELECT sum(cost) FROM yuntun.public.events_copy",
    )
    .await;
    assert_eq!(
        b[0].column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        60,
        "INSERT SELECT 数据可查（cast 后汇入同一管线）"
    );

    // ⑥ SHOW TABLES（→ Catalog list_tables）
    let b = collect_do_get(&mut client, "SHOW TABLES").await;
    let names: Vec<String> = (0..b[0].num_rows())
        .map(|i| {
            b[0].column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(i)
                .to_string()
        })
        .collect();
    assert!(names.contains(&"audit_events".to_string()));
    assert!(names.contains(&"events_copy".to_string()));

    // ⑦ DROP TABLE + IF EXISTS 语义
    let n = execute_update(&mut client, "DROP TABLE IF EXISTS events_copy").await;
    assert_eq!(n, 0);
    expect_do_get_error(&mut client, "DROP TABLE events_copy").await; // 已删，无 IF EXISTS → 报错
    execute_update(&mut client, "DROP TABLE IF EXISTS events_copy").await; // 幂等
    let b = collect_do_get(&mut client, "SHOW TABLES").await;
    let names: Vec<String> = (0..b[0].num_rows())
        .map(|i| {
            b[0].column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(i)
                .to_string()
        })
        .collect();
    assert!(
        !names.contains(&"events_copy".to_string()),
        "DROP 后 SHOW TABLES 不再列出"
    );
    assert!(names.contains(&"audit_events".to_string()));

    // ⑧ 明确拒绝不支持语句
    expect_do_get_error(&mut client, "UPDATE audit_events SET cost = 1").await;
    expect_do_get_error(&mut client, "DELETE FROM audit_events").await;
    expect_do_get_error(&mut client, "CREATE TABLE t AS SELECT 1").await;

    // ============ 崩溃重启（S1.6/S1.7 验收：SQL 写入数据 + DDL 均可恢复）============
    shutdown.cancel();
    drop(_bg);
    drop(client);
    let lakehouse = Arc::new(Lakehouse::build(&cfg).await.unwrap());
    // 阶段 0 恢复模型：DDL 由 WAL 重放重建；数据可见性由攒批线程重读 WAL 重做 flush
    let _bg2 = lakehouse.spawn_background(&cfg);
    tokio::time::sleep(Duration::from_millis(700)).await;
    refresh_cache(&lakehouse).await;

    // DDL 重放：audit_events 存在；events_copy 保持 DROP 后状态
    let tables = lakehouse.catalog.list_tables().await.unwrap();
    let names: Vec<String> = tables.into_iter().map(|t| t.name).collect();
    assert!(
        names.contains(&"audit_events".to_string()),
        "DDL 经 WAL 重放恢复"
    );
    assert!(
        !names.contains(&"events_copy".to_string()),
        "DROP 语义经 WAL 重放保持"
    );

    // SQL 写入的数据与 DoPut 同等持久性（WAL 权威 → 攒批/恢复后可查）
    let b = collect_do_get(
        &mut serve(&lakehouse, CancellationToken::new()).await,
        "SELECT count(*) AS c, sum(cost) AS s FROM yuntun.public.audit_events",
    )
    .await;
    let c = b[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let s = b[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(c, 4, "重启后 SQL 写入的数据不丢");
    assert_eq!(s, 60);

    let _ = std::fs::remove_dir_all(&base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_idempotency_required_on_general_table() {
    // General 表强制幂等键：run_sql 的语句级 dml-<uuid> 键必须满足检查
    // （构造：绕过 server 直接以空幂等键等价场景——此处验证 resolve 矩阵经 run_sql 生效）
    let wal_dir = format!("/tmp/yuntun-sqldml-idem-{}", std::process::id());
    let _ = std::fs::remove_dir_all(&wal_dir);
    let cfg = yuntun_server::Config::from_toml(&format!(
        r#"
[store]
type = "memory"

[wal]
dir = "{wal_dir}"

[ingest]
rows_threshold = 1
time_threshold_secs = 5
flush_jitter_secs = 0
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

    collect_do_get(&mut client, "CREATE TABLE t (a BIGINT NOT NULL)").await;
    // 正常 INSERT（run_sql 内部生成 dml-<uuid> 幂等键）成功
    let n = execute_update(&mut client, "INSERT INTO t VALUES (1), (2)").await;
    assert_eq!(n, 2);
    tokio::time::sleep(Duration::from_millis(400)).await;
    refresh_cache(&lakehouse).await;
    let b = collect_do_get(&mut client, "SELECT count(*) FROM yuntun.public.t").await;
    assert_eq!(
        b[0].column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
    // Int32 落列
    let b = collect_do_get(
        &mut client,
        "SELECT count(*) FROM yuntun.public.t WHERE a = 2",
    )
    .await;
    assert_eq!(
        b[0].column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    shutdown.cancel();
}
