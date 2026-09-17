//! 多 schema（MySQL 的 database）端到端：建库 / 跨库同名表隔离 / DROP 规则 /
//! WAL 重放恢复（schema 事件 + 全限定表标识）。
//!
//! 走 Flight 简易轨（ticket = SQL）：`CREATE DATABASE`、`sales.orders` 这类
//! **限定名**是 SQL 层语义，与协议无关；MySQL 协议的 `USE db` 切换由
//! `scripts/pymysql_smoke.py`（真实客户端）覆盖。

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use yuntun_catalog::CatalogOps;
use yuntun_server::Lakehouse;

/// 简易轨执行 SQL（返回结果批次；DDL/INSERT 为空）。
async fn run_sql(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    sql: &str,
) -> Vec<arrow::record_batch::RecordBatch> {
    let mut stream = client
        .do_get(Ticket {
            ticket: sql.as_bytes().to_vec().into(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut datas = Vec::new();
    while let Some(fd) = stream.next().await {
        datas.push(fd.unwrap());
    }
    if datas.is_empty() {
        return Vec::new();
    }
    arrow_flight::utils::flight_data_to_batches(&datas).unwrap_or_default()
}

async fn run_sql_err(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    sql: &str,
) -> tonic::Status {
    client
        .do_get(Ticket {
            ticket: sql.as_bytes().to_vec().into(),
        })
        .await
        .expect_err("expected error")
}

/// 单值查询（count/sum）。
async fn scalar(client: &mut FlightServiceClient<tonic::transport::Channel>, sql: &str) -> i64 {
    let batches = run_sql(client, sql).await;
    batches
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .map(|a| a.value(0))
        .unwrap_or(-1)
}

/// 等写入可见（攒批 flush + 缓存刷新）。
async fn wait_count(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    sql: &str,
    expect: i64,
) {
    for _ in 0..40 {
        if scalar(client, sql).await == expect {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("等待 {expect} 超时：{sql}");
}

async fn serve(
    lakehouse: &Arc<Lakehouse>,
    shutdown: CancellationToken,
) -> FlightServiceClient<tonic::transport::Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = arrow_flight::flight_service_server::FlightServiceServer::new(
        yuntun_server::FlightServer::new(
            lakehouse.ingestor.clone(),
            lakehouse.query.clone(),
            lakehouse.catalog.clone(),
        )
        .with_sql(lakehouse.sql.clone()),
    );
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move { shutdown.cancelled().await },
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

fn config(wal_dir: &str) -> String {
    format!(
        r#"
[server]
listen = "127.0.0.1:0"

[store]
type = "memory"

[wal]
dir = "{wal_dir}"

[ingest]
rows_threshold = 1
time_threshold_secs = 1
max_flush_delay_secs = 1
flush_phase_spread_secs = 0
scan_interval_ms = 20

[query]
cache_ttl_secs = 1
"#
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_schema_create_isolate_and_recover() {
    // 写盘测试默认走 tmpfs（内存盘）；真实落盘语义的用例用 TestDir::disk
    let wal_dir = yuntun_testkit::TestDir::tmpfs("multi-schema-wal")
        .into_path()
        .to_string_lossy()
        .to_string();
    let cfg = yuntun_server::Config::from_toml(&config(&wal_dir)).unwrap();

    // ============ 第一次运行 ============
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let _bg = lakehouse.spawn_background(&cfg);
    let mut client = serve(&lakehouse, shutdown.clone()).await;

    // ① 建库 + SHOW DATABASES
    run_sql(&mut client, "CREATE DATABASE sales").await;
    run_sql(&mut client, "CREATE DATABASE recover").await;
    let b = run_sql(&mut client, "SHOW DATABASES").await;
    let names: Vec<String> = b
        .first()
        .map(|b| {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            (0..col.len()).map(|i| col.value(i).to_string()).collect()
        })
        .unwrap_or_default();
    assert!(names.contains(&"sales".to_string()), "{names:?}");
    assert!(names.contains(&"recover".to_string()), "{names:?}");

    // ② 重复建库 → already_exists；未知库建表 → not_found
    let err = run_sql_err(&mut client, "CREATE DATABASE sales").await;
    assert_eq!(err.code(), tonic::Code::AlreadyExists, "{err:?}");
    let err = run_sql_err(&mut client, "CREATE TABLE nope.t (v BIGINT)").await;
    assert_eq!(err.code(), tonic::Code::NotFound, "{err:?}");

    // ③ 跨 schema **同名表**：sales.orders 与 public.orders 互不影响
    run_sql(
        &mut client,
        "CREATE TABLE sales.orders (ts BIGINT, amt BIGINT)",
    )
    .await;
    run_sql(
        &mut client,
        "CREATE TABLE public.orders (ts BIGINT, amt BIGINT)",
    )
    .await;
    run_sql(&mut client, "INSERT INTO sales.orders VALUES (1, 100)").await;
    run_sql(&mut client, "INSERT INTO public.orders VALUES (9, 900)").await;
    run_sql(&mut client, "CREATE TABLE recover.evt (ts BIGINT)").await;

    wait_count(&mut client, "SELECT count(*) AS c FROM sales.orders", 1).await;
    assert_eq!(
        scalar(&mut client, "SELECT sum(amt) AS s FROM sales.orders").await,
        100,
        "sales.orders 只有自己的行"
    );
    assert_eq!(
        scalar(&mut client, "SELECT sum(amt) AS s FROM public.orders").await,
        900,
        "public.orders 与 sales.orders 隔离"
    );
    // 未创建的第三个 schema 查询 → not_found
    let err = run_sql_err(&mut client, "SELECT count(*) FROM other.orders").await;
    assert_eq!(err.code(), tonic::Code::NotFound, "{err:?}");

    // ④ DROP 规则：非空库拒绝；删表后可删；public 不可删
    let err = run_sql_err(&mut client, "DROP DATABASE sales").await;
    assert_eq!(err.code(), tonic::Code::FailedPrecondition, "{err:?}");
    let err = run_sql_err(&mut client, "DROP DATABASE public").await;
    assert!(err.code() == tonic::Code::Internal || err.code() == tonic::Code::InvalidArgument);
    run_sql(&mut client, "DROP TABLE sales.orders").await;
    run_sql(&mut client, "DROP DATABASE sales").await;
    let b = run_sql(&mut client, "SHOW DATABASES").await;
    let cols = b.first().unwrap().column(0);
    let names: Vec<String> = (0..cols.len())
        .map(|i| {
            cols.as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(i)
                .to_string()
        })
        .collect();
    assert!(!names.contains(&"sales".to_string()), "{names:?}");

    // ============ 崩溃重启（schema 事件 + 表定义均由 WAL 重放）============
    shutdown.cancel();
    drop(_bg);
    drop(client);
    let lakehouse = Arc::new(Lakehouse::build(&cfg).await.unwrap());
    let _bg2 = lakehouse.spawn_background(&cfg);
    tokio::time::sleep(Duration::from_millis(700)).await;
    lakehouse
        .query
        .catalog()
        .refresh(&(lakehouse.catalog.clone() as Arc<dyn CatalogOps>))
        .await
        .unwrap();
    let shutdown2 = CancellationToken::new();
    let mut client = serve(&lakehouse, shutdown2.clone()).await;

    // ⑤ recover schema 与其中的表经 WAL 重放恢复；已 DROP 的 sales 不复活
    let b = run_sql(&mut client, "SHOW DATABASES").await;
    let cols = b.first().unwrap().column(0);
    let names: Vec<String> = (0..cols.len())
        .map(|i| {
            cols.as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(i)
                .to_string()
        })
        .collect();
    assert!(names.contains(&"recover".to_string()), "{names:?}");
    assert!(!names.contains(&"sales".to_string()), "DROP 语义保持: {names:?}");
    let err = run_sql_err(&mut client, "SELECT count(*) FROM recover.nope").await;
    assert_eq!(err.code(), tonic::Code::NotFound, "{err:?}");
    // 重放后的表可查（schema 存在 → 解析成功 → 0 行）
    assert_eq!(
        scalar(&mut client, "SELECT count(*) AS c FROM recover.evt").await,
        0,
        "recover.evt 经 WAL 重放恢复"
    );

    shutdown2.cancel();
}
