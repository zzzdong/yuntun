//! Flight gRPC 端到端：真实 tonic 客户端 DoPut → WAL fsync → 攒批 flush → SQL 查询。
//! 验收对应 T1.3（Flight 源）+ D1（写入路径贯通）。

use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, FlightDescriptor, PutResult};
use futures::StreamExt;
use yuntun_catalog::CatalogOps;
use yuntun_model::ops::CreateTableRequest;
use yuntun_server::Lakehouse;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("event_time", DataType::Int64, false),
        Field::new("user", DataType::Utf8, true),
    ]))
}

fn batch() -> arrow::record_batch::RecordBatch {
    arrow::record_batch::RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])),
        ],
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flight_doput_end_to_end() {
    // ① 装配（内存 store；WAL 目录按进程隔离，避免重跑残留数据）
    let wal_dir = format!("/tmp/yuntun-flight-e2e-wal-{}", std::process::id());
    let _ = std::fs::remove_dir_all(&wal_dir);
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
    lakehouse
        .catalog
        .create_table(CreateTableRequest {
            name: "audit".into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig::standard(),
        })
        .await
        .unwrap();
    let _bg = lakehouse.spawn_background(&cfg);

    // ② 启动 Flight 服务（随机端口）
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

    // ③ 客户端连接 + DoPut（tonic 0.14：Endpoint::connect → Channel）
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightServiceClient::new(channel);

    // batches_to_flight_data → [schema 消息, batch 消息]
    let mut flight_msgs =
        arrow_flight::utils::batches_to_flight_data(schema().as_ref(), vec![batch()]).unwrap();
    let mut data_flight = flight_msgs.remove(1);
    data_flight.app_metadata = br#"{"idempotency_key":"flight-key-1"}"#.to_vec().into();

    let schema_flight_with_desc = FlightData {
        flight_descriptor: Some(FlightDescriptor {
            r#type: 1, // PATH
            path: vec!["audit".into(), "s0".into()],
            cmd: Default::default(),
        }),
        ..flight_msgs.remove(0)
    };

    let requests = vec![schema_flight_with_desc, data_flight];
    let mut responses: tonic::Streaming<PutResult> = client
        .do_put(tokio_stream::iter(requests))
        .await
        .unwrap()
        .into_inner();
    let ack = responses.next().await.unwrap().unwrap();
    let receipt: serde_json::Value = serde_json::from_slice(&ack.app_metadata).unwrap();
    assert_eq!(receipt["row_count"], 2, "回执行数");
    assert_eq!(receipt["shard"], "s0");

    // ④ 等 flush → 缓存刷新 → SQL 查询
    tokio::time::sleep(Duration::from_millis(600)).await;
    lakehouse
        .query
        .cache()
        .refresh(&(lakehouse.catalog.clone() as Arc<dyn CatalogOps>))
        .await
        .unwrap();

    let batches = lakehouse
        .query
        .sql("SELECT count(*) FROM yuntun.public.audit")
        .await
        .unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), 2, "Flight 写入的 2 行应可查询");

    // ④' 外部查询通道：do_get（Ticket = SQL）
    let mut do_get_stream = client
        .do_get(arrow_flight::Ticket {
            ticket: b"SELECT count(*) AS c FROM yuntun.public.audit"
                .to_vec()
                .into(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut datas = Vec::new();
    while let Some(fd) = do_get_stream.next().await {
        datas.push(fd.unwrap());
    }
    assert!(!datas.is_empty(), "do_get 必须返回 IPC 流（含 schema）");
    let q_batches = arrow_flight::utils::flight_data_to_batches(&datas).unwrap();
    let q_arr = q_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(q_arr.value(0), 2, "do_get 外部 SQL 查询应查到 2 行");

    // get_flight_info（cmd = SQL）→ endpoint ticket 可直接 do_get
    let info = client
        .get_flight_info(arrow_flight::FlightDescriptor {
            r#type: 2,
            cmd: b"SELECT count(*) AS c FROM yuntun.public.audit"
                .to_vec()
                .into(),
            path: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.endpoint.len(), 1);
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let mut s2 = client.do_get(ticket).await.unwrap().into_inner();
    let mut d2 = Vec::new();
    while let Some(fd) = s2.next().await {
        d2.push(fd.unwrap());
    }
    let b2 = arrow_flight::utils::flight_data_to_batches(&d2).unwrap();
    let a2 = b2[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(a2.value(0), 2, "get_flight_info → do_get 路径同样可用");

    shutdown.cancel();
    let _ = server.await;
}
