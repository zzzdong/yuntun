//! S1.8 幂等键透传端到端（`sql-access-design.md` §七 Q1/Q2 及 plan S1.8）。
//!
//! 覆盖三条通道：
//! 1. **SQL 文本注释**（简易轨 `do_get` 执行 INSERT，注释 `/* idempotency_key=... */`）；
//! 2. **FlightSQL prepared 上的键**（`ActionCreatePreparedStatementRequest` 的 SQL
//!    携带注释 → 装载 `CommandPreparedStatementUpdate` 时作为默认键）；
//! 3. **服务端语句级生成兜底**（prepared 未携带键时，require 表仍可装载成功）。
//!
//! 表为 `IngestConfig::standard()`（require 幂等键）——缺键必须拒绝而不是静默降级
//! （架构 §7.3.2），因此本测试同时证明「键被正确送到 ingest 管线」。

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{ActionCreatePreparedStatementRequest, CommandPreparedStatementUpdate};
use arrow_flight::{Action, FlightData, FlightDescriptor, PutResult, Ticket};
use futures::StreamExt;
use prost::Message;
use tokio_util::sync::CancellationToken;
use yuntun_catalog::CatalogOps;
use yuntun_model::ops::CreateTableRequest;
use yuntun_server::flight::command_bytes;
use yuntun_server::Lakehouse;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("event_time", DataType::Int64, false),
        Field::new("user", DataType::Utf8, true),
    ]))
}

fn batch(ts: i64) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![ts, ts + 1])),
            Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])),
        ],
    )
    .unwrap()
}

/// 简易轨执行 SQL（ticket = SQL 文本），排空结果流。
async fn run_sql_do_get(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    sql: &str,
) {
    let mut stream = client
        .do_get(Ticket {
            ticket: sql.as_bytes().to_vec().into(),
        })
        .await
        .unwrap()
        .into_inner();
    while let Some(fd) = stream.next().await {
        fd.unwrap();
    }
}

/// 创建 prepared 语句 → 返回 handle。
async fn create_prepared(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    query: &str,
) -> Vec<u8> {
    let result = client
        .do_action(Action {
            r#type: "CreatePreparedStatement".to_string(),
            body: command_bytes(&ActionCreatePreparedStatementRequest {
                query: query.to_string(),
                transaction_id: None,
            })
            .into(),
        })
        .await
        .unwrap()
        .into_inner()
        .next()
        .await
        .unwrap()
        .unwrap();
    let any = arrow_flight::sql::Any::decode(&*result.body).unwrap();
    arrow_flight::sql::ActionCreatePreparedStatementResult::decode(any.value.as_ref())
        .unwrap()
        .prepared_statement_handle
        .to_vec()
}

/// 绑定数据装载（prepared update；app_metadata 不带键）→ 返回 record_count。
async fn put_prepared(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    handle: Vec<u8>,
    ts: i64,
) -> i64 {
    let mut msgs = arrow_flight::utils::batches_to_flight_data(schema().as_ref(), vec![batch(ts)])
        .unwrap();
    let data = msgs.remove(1); // 不带 app_metadata：键只能来自 prepared 上的键或服务端生成
    let first = FlightData {
        flight_descriptor: Some(FlightDescriptor {
            r#type: 2, // CMD
            cmd: command_bytes(&CommandPreparedStatementUpdate {
                prepared_statement_handle: handle.into(),
            })
            .into(),
            path: vec![],
        }),
        ..msgs.remove(0)
    };
    let mut acks: tonic::Streaming<PutResult> = client
        .do_put(tokio_stream::iter(vec![first, data]))
        .await
        .unwrap()
        .into_inner();
    let ack = acks.next().await.unwrap().unwrap();
    arrow_flight::sql::DoPutUpdateResult::decode(&*ack.app_metadata)
        .unwrap()
        .record_count
}

async fn count_rows(lakehouse: &Lakehouse) -> i64 {
    // 等 flush + 手动刷新缓存（等价 background refresh，测试内确定性）
    tokio::time::sleep(Duration::from_millis(700)).await;
    lakehouse
        .query
        .cache()
        .refresh(&(lakehouse.catalog.clone() as Arc<dyn CatalogOps>))
        .await
        .unwrap();
    let batches = lakehouse
        .query
        .sql("SELECT count(*) AS c FROM yuntun.public.audit")
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_idempotency_key_passthrough() {
    // ① 装配
    // 写盘测试默认走 tmpfs（内存盘）；真实落盘语义的用例用 TestDir::disk
    let wal_guard = yuntun_testkit::TestDir::tmpfs("idem-e2e-wal");
    let wal_dir = wal_guard.string();
    let cfg = yuntun_server::Config::from_toml(&format!(
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
"#
    ))
    .unwrap();
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    // require 幂等键（General 模板）
    lakehouse
        .catalog
        .create_table(CreateTableRequest {
            name: "audit".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig::standard(),
        })
        .await
        .unwrap();
    let _bg = lakehouse.spawn_background(&cfg);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = arrow_flight::flight_service_server::FlightServiceServer::new(
        yuntun_server::FlightServer::new(
            lakehouse.ingestor.clone(),
            lakehouse.query.clone(),
            lakehouse.catalog.clone(),
        ),
    );
    let server_shutdown = shutdown.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move { server_shutdown.cancelled().await },
            )
            .await
            .unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightServiceClient::new(channel);

    // ② SQL 文本注释通道：INSERT 带 /* idempotency_key=... */ 应正常执行
    run_sql_do_get(
        &mut client,
        "INSERT /* idempotency_key=sql-anno-1 */ INTO audit (event_time, user) VALUES (1, 'a')",
    )
    .await;
    assert_eq!(count_rows(&lakehouse).await, 1, "SQL 注释通道 INSERT 生效");

    // ③ prepared 上的键：装载时 app_metadata 无键 → 用 prepared 的注释键
    let handle = create_prepared(
        &mut client,
        "INSERT /* idempotency_key=prep-anno-1 */ INTO audit (event_time, user) VALUES (?, ?)",
    )
    .await;
    let n = put_prepared(&mut client, handle.clone(), 200).await;
    assert_eq!(n, 2, "prepared 装载回执行数");
    assert_eq!(
        count_rows(&lakehouse).await,
        3,
        "prepared 携带注释键的装载应被 accept（require 表）"
    );

    // ④ 无键 prepared：服务端语句级生成兜底（require 表仍可写入）
    let handle = create_prepared(
        &mut client,
        "INSERT INTO audit (event_time, user) VALUES (?, ?)",
    )
    .await;
    let n = put_prepared(&mut client, handle, 400).await;
    assert_eq!(n, 2);
    assert_eq!(count_rows(&lakehouse).await, 5, "服务端生成兜底键");

    shutdown.cancel();
    let _ = server.await;
}
