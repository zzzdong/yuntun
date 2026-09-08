//! Flight SQL 标准协议端到端（计划任务书 v2.0 S1.3–S1.5）：
//! - 语句查询：GetFlightInfo(CommandStatementQuery) → DoGet(TicketStatementQuery)
//! - 元数据：GetCatalogs / GetTables（含 schema）
//! - Prepared 批量写入：CreatePreparedStatement(INSERT) → DoPut(CommandPreparedStatementUpdate)
//!   绑定数据 → DoPutUpdateResult.record_count → 可查询
//! - 双轨并存：简易 ticket 路径不受影响

use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{
    ActionCreatePreparedStatementRequest, CommandGetCatalogs, CommandGetTables,
    CommandPreparedStatementUpdate, CommandStatementQuery, DoPutUpdateResult, TicketStatementQuery,
};
use arrow_flight::{Action, FlightData, FlightDescriptor, PutResult};
use futures::StreamExt;
use prost::Message;
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

fn batch(ts: i64) -> arrow::record_batch::RecordBatch {
    arrow::record_batch::RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![ts, ts + 1])),
            Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])),
        ],
    )
    .unwrap()
}

async fn collect_do_get(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    ticket: arrow_flight::Ticket,
) -> Vec<arrow::record_batch::RecordBatch> {
    let mut stream = client.do_get(ticket).await.unwrap().into_inner();
    let mut datas = Vec::new();
    while let Some(fd) = stream.next().await {
        datas.push(fd.unwrap());
    }
    arrow_flight::utils::flight_data_to_batches(&datas).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flight_sql_end_to_end() {
    // ① 装配（与 flight_e2e 相同的基础设施）
    let wal_dir = format!("/tmp/yuntun-flightsql-e2e-wal-{}", std::process::id());
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

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = arrow_flight::flight_service_server::FlightServiceServer::new(
        yuntun_server::FlightServer::new(lakehouse.ingestor.clone(), lakehouse.query.clone()),
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

    // ② 简易轨写入 2 行（既有链路，双轨并存验证）
    let mut flight_msgs =
        arrow_flight::utils::batches_to_flight_data(schema().as_ref(), vec![batch(100)]).unwrap();
    let mut data_flight = flight_msgs.remove(1);
    data_flight.app_metadata = br#"{"idempotency_key":"sql-e2e-1"}"#.to_vec().into();
    let schema_flight_with_desc = FlightData {
        flight_descriptor: Some(FlightDescriptor {
            r#type: 1,
            path: vec!["audit".into(), "s0".into()],
            cmd: Default::default(),
        }),
        ..flight_msgs.remove(0)
    };
    let mut acks: tonic::Streaming<PutResult> = client
        .do_put(tokio_stream::iter(vec![
            schema_flight_with_desc,
            data_flight,
        ]))
        .await
        .unwrap()
        .into_inner();
    assert!(
        !acks.next().await.unwrap().unwrap().app_metadata.is_empty(),
        "简易轨回执"
    );

    tokio::time::sleep(Duration::from_millis(600)).await;
    lakehouse
        .query
        .cache()
        .refresh(&(lakehouse.catalog.clone() as Arc<dyn CatalogOps>))
        .await
        .unwrap();

    // ③ 标准轨查询：GetFlightInfo(CommandStatementQuery) → DoGet
    let query = "SELECT count(*) AS c FROM yuntun.public.audit";
    let qcmd = command_bytes(&CommandStatementQuery {
        query: query.to_string(),
        transaction_id: None,
    });
    let info = client
        .get_flight_info(FlightDescriptor {
            r#type: 2,
            cmd: qcmd.into(),
            path: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.endpoint.len(), 1, "语句查询返回 1 个 endpoint");
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    // ticket 必须是标准 TicketStatementQuery（Any 编码）
    let any = arrow_flight::sql::Any::decode(&*ticket.ticket).unwrap();
    let tsq = TicketStatementQuery::decode(any.value.as_ref()).unwrap();
    assert!(!tsq.statement_handle.is_empty());

    let batches = collect_do_get(&mut client, ticket).await;
    let got = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(got, 2, "FlightSQL 查询应查到简易轨写入的 2 行");

    // ④ 元数据：GetCatalogs
    let ccmd = command_bytes(&CommandGetCatalogs {});
    let cinfo = client
        .get_flight_info(FlightDescriptor {
            r#type: 2,
            cmd: ccmd.into(),
            path: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    let cb = collect_do_get(&mut client, cinfo.endpoint[0].ticket.clone().unwrap()).await;
    let catalogs = cb[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(catalogs.value(0), "yuntun");

    // ⑤ 元数据：GetTables（include_schema = true）
    let tcmd = command_bytes(&CommandGetTables {
        catalog: None,
        db_schema_filter_pattern: None,
        table_name_filter_pattern: None,
        table_types: vec![],
        include_schema: true,
    });
    let tinfo = client
        .get_flight_info(FlightDescriptor {
            r#type: 2,
            cmd: tcmd.into(),
            path: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    let tb = collect_do_get(&mut client, tinfo.endpoint[0].ticket.clone().unwrap()).await;
    let names = tb[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut has_audit = false;
    for i in 0..tb[0].num_rows() {
        if names.value(i) == "audit" {
            has_audit = true;
        }
    }
    assert!(has_audit, "GetTables 必须列出 audit 表");
    // table_schema 列（IPC 封装消息格式：continuation + length + flatbuffer）可解析回
    // Arrow schema（含 event_time 字段）
    let schema_col = tb[0]
        .column(4)
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .unwrap();
    let raw = schema_col.value(0);
    assert_eq!(&raw[0..4], &0xFFFF_FFFFu32.to_le_bytes(), "封装前缀");
    let flat = &raw[8..];
    let ipc = arrow::ipc::root_as_message(flat).unwrap();
    let fb = ipc.header_as_schema().unwrap();
    let s = arrow::ipc::convert::fb_to_schema(fb);
    assert!(s.field_with_name("event_time").is_ok());

    // ⑥ 标准轨写入：CreatePreparedStatement(INSERT) → DoPut(CommandPreparedStatementUpdate) 绑定数据
    let create_req = ActionCreatePreparedStatementRequest {
        query: "INSERT INTO audit (event_time, user)".to_string(),
        transaction_id: None,
    };
    let result = client
        .do_action(Action {
            r#type: "CreatePreparedStatement".to_string(),
            body: command_bytes(&create_req).into(),
        })
        .await
        .unwrap()
        .into_inner()
        .next()
        .await
        .unwrap()
        .unwrap();
    let any = arrow_flight::sql::Any::decode(&*result.body).unwrap();
    let created =
        arrow_flight::sql::ActionCreatePreparedStatementResult::decode(any.value.as_ref()).unwrap();
    let handle = created.prepared_statement_handle.to_vec();

    // 绑定数据：descriptor.cmd = Any(CommandPreparedStatementUpdate)，schema + 批次
    let mut bound =
        arrow_flight::utils::batches_to_flight_data(schema().as_ref(), vec![batch(200)]).unwrap();
    let data = bound.remove(1);
    let put_first = FlightData {
        flight_descriptor: Some(FlightDescriptor {
            r#type: 2, // CMD
            cmd: command_bytes(&CommandPreparedStatementUpdate {
                prepared_statement_handle: handle.clone().into(),
            })
            .into(),
            path: vec![],
        }),
        ..bound.remove(0)
    };
    let mut put_acks: tonic::Streaming<PutResult> = client
        .do_put(tokio_stream::iter(vec![put_first, data]))
        .await
        .unwrap()
        .into_inner();
    let put_ack = put_acks.next().await.unwrap().unwrap();
    let upd = DoPutUpdateResult::decode(&*put_ack.app_metadata).unwrap();
    assert_eq!(upd.record_count, 2, "Prepared 装载返回受影响行数");

    tokio::time::sleep(Duration::from_millis(600)).await;
    lakehouse
        .query
        .cache()
        .refresh(&(lakehouse.catalog.clone() as Arc<dyn CatalogOps>))
        .await
        .unwrap();

    // ⑦ 两轨数据合流可查：2（简易轨）+ 2（标准轨）= 4
    let qcmd2 = command_bytes(&CommandStatementQuery {
        query: "SELECT count(*) AS c FROM yuntun.public.audit".to_string(),
        transaction_id: None,
    });
    let info2 = client
        .get_flight_info(FlightDescriptor {
            r#type: 2,
            cmd: qcmd2.into(),
            path: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    let b2 = collect_do_get(&mut client, info2.endpoint[0].ticket.clone().unwrap()).await;
    let got2 = b2[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(got2, 4, "双轨写入的数据都汇入同一 ingest 管线");

    shutdown.cancel();
    let _ = server.await;
}
