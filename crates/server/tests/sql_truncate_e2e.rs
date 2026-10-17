//! **`D-35`：`TRUNCATE TABLE` 的端到端验收**。
//!
//! 口径：`TRUNCATE [TABLE] t` = **无 `WHERE` 的全表删** —— 同一条数据路径
//! （`dml_prologue` → `execute_purge`，文件级下线），差别只在语句长相。
//! 本用例守四件事：
//!
//! ① **语义**：清空后一行查不到、没有可见文件，但**表定义还在**（可再写）；
//! ② **在途数据不复活**：清空**之前**刚写入、还没落盘的数据必须被先 flush 再清除
//!    （否则"删完又回来"—— 这正是 `dml_prologue` 把 force_flush 放在最前面的原因）；
//! ③ **形态拒绝**：多表 / 分区 / CASCADE 选项明确报错（不静默忽略）；
//! ④ **不存在的表** → `NotFound`（与 DELETE 同口径，不是"清了个空"）。
//!
//! 反证形态（用例存在的理由）：TRUNCATE 曾被 1149 拒绝之列外；而"清空"这条路
//! 一旦新造（不复用 purge），最容易漏的就是 ② —— 在途数据不属于任何文件，
//! purge 扫不到它，之后攒批落盘 = 刚删的数据又回来了。

use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementUpdate, DoPutUpdateResult};
use arrow_flight::{FlightData, FlightDescriptor, PutResult};
use futures::StreamExt;
use prost::Message;
use yuntun_catalog::CatalogOps;
use yuntun_server::Lakehouse;

// ---------------------------------------------------------------- 夹具

async fn execute_update_sql(
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

async fn col_v(client: &mut FlightServiceClient<tonic::transport::Channel>, sql: &str) -> Vec<i64> {
    let batches = collect_do_get(client, sql).await;
    let mut out = Vec::new();
    for b in &batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap_or_else(|| panic!("第一列应是 Int64，实际 {:?}", b.schema().field(0)));
        for i in 0..col.len() {
            out.push(col.value(i));
        }
    }
    out
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
    });
    let mut last = None;
    for _ in 0..100 {
        match tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
        {
            Ok(c) => {
                last = Some(FlightServiceClient::new(c));
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
        }
    }
    last.expect("连不上 Flight")
}

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

// ---------------------------------------------------------------- 用例

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_empties_the_table_but_keeps_its_definition() {
    let guard = yuntun_testkit::TestDir::tmpfs("sql-truncate-e2e");
    let base = guard.string();
    let cfg = config(&format!("{base}/wal"), &format!("{base}/store"));
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let _bg = lakehouse.spawn_background(&cfg);
    let mut client = serve(&lakehouse, shutdown.clone()).await;

    execute_update_sql(&mut client, "CREATE TABLE t (v BIGINT NOT NULL)").await;
    assert_eq!(
        execute_update_sql(&mut client, "INSERT INTO t VALUES (1),(2),(3)").await,
        3
    );

    // ---- TRUNCATE ----
    execute_update_sql(&mut client, "TRUNCATE TABLE t").await;

    // ① 一行不剩、没有可见文件（文件级下线）
    assert!(
        col_v(&mut client, "SELECT v FROM t ORDER BY v")
            .await
            .is_empty(),
        "TRUNCATE 之后一行都查不到"
    );
    let snap = lakehouse.catalog.current_snapshot().await;
    assert!(
        lakehouse
            .catalog
            .list_visible_files("public.t", snap, None)
            .await
            .unwrap()
            .is_empty(),
        "TRUNCATE 走文件级下线：没有可见文件"
    );
    // ② 表定义还在：可以再写再查（"清空"不是"把表变成黑洞"）
    assert_eq!(
        execute_update_sql(&mut client, "INSERT INTO t VALUES (9)").await,
        1
    );
    let mut last = Vec::new();
    for _ in 0..40 {
        last = col_v(&mut client, "SELECT v FROM t").await;
        if last == vec![9] {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(last, vec![9], "TRUNCATE 之后必须还能写入并读出");

    // ③ 形态拒绝：多表 / 选项一律明确报错（不静默忽略）
    for bad in [
        "TRUNCATE TABLE t, t",
        "TRUNCATE TABLE t CASCADE",
        "TRUNCATE TABLE t RESTART IDENTITY",
    ] {
        let err = {
            let first = FlightData {
                flight_descriptor: Some(FlightDescriptor {
                    r#type: 2,
                    cmd: yuntun_server::flight::command_bytes(&CommandStatementUpdate {
                        query: bad.to_string(),
                        transaction_id: None,
                    })
                    .into(),
                    path: vec![],
                }),
                ..Default::default()
            };
            client
                .do_put(tokio_stream::iter(vec![first]))
                .await
                .expect_err(bad)
        };
        let _ = err; // 到得了这里就是"被拒绝"（状态码由 tonic 给出）
    }

    // ④ 不存在的表 → NotFound（与 DELETE 同口径）
    let e = {
        let first = FlightData {
            flight_descriptor: Some(FlightDescriptor {
                r#type: 2,
                cmd: yuntun_server::flight::command_bytes(&CommandStatementUpdate {
                    query: "TRUNCATE TABLE nope".to_string(),
                    transaction_id: None,
                })
                .into(),
                path: vec![],
            }),
            ..Default::default()
        };
        client
            .do_put(tokio_stream::iter(vec![first]))
            .await
            .expect_err("TRUNCATE 不存在的表必须报错")
    };
    assert!(
        e.message().contains("not found") || e.message().contains("NotFound"),
        "错误信息应点名表不存在：{}",
        e.message()
    );

    // 收尾
    shutdown.cancel();
}

/// **在途数据不复活**：TRUNCATE 之前刚写入、还没落盘的数据，必须被先 flush 再清除。
///
/// 反证：若清空路径漏了 force_flush，在途数据不属于任何文件 ⇒ purge 扫不到它 ⇒
/// 攒批落盘后"刚删的数据又回来了"（本用例等一个攒批周期就能看到）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_also_clears_in_flight_data() {
    let guard = yuntun_testkit::TestDir::tmpfs("sql-truncate-inflight");
    let base = guard.string();
    // 攒批窗口拉长：保证 TRUNCATE 时数据还在途（没落盘）
    let mut cfg = config(&format!("{base}/wal"), &format!("{base}/store"));
    cfg.ingest.time_threshold_secs = 600;
    cfg.ingest.max_flush_delay_secs = 600;
    cfg.ingest.flush_phase_spread_secs = 600;
    cfg.ingest.chunk_max_resident_secs = 600;
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let _bg = lakehouse.spawn_background(&cfg);
    let mut client = serve(&lakehouse, shutdown.clone()).await;

    execute_update_sql(&mut client, "CREATE TABLE t (v BIGINT NOT NULL)").await;
    assert_eq!(
        execute_update_sql(&mut client, "INSERT INTO t VALUES (1),(2)").await,
        2
    );
    // 读己之写：写入即可查（内存视图）
    let mut last = Vec::new();
    for _ in 0..40 {
        last = col_v(&mut client, "SELECT v FROM t ORDER BY v").await;
        if last == vec![1, 2] {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(last, vec![1, 2], "前提：在途数据读己之写可见");

    execute_update_sql(&mut client, "TRUNCATE TABLE t").await;
    assert!(
        col_v(&mut client, "SELECT v FROM t ORDER BY v")
            .await
            .is_empty(),
        "TRUNCATE 之后立刻查不到"
    );

    // 等一个攒批周期（50ms × 80 = 4s，远超 scan_interval）—— 复活的话这里现形
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    assert!(
        col_v(&mut client, "SELECT v FROM t ORDER BY v")
            .await
            .is_empty(),
        "在途数据不许复活：TRUNCATE 必须先把它们落成文件再清除"
    );

    shutdown.cancel();
}
