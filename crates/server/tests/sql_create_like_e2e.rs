//! **`CREATE TABLE new LIKE old` 的端到端验收**（`§184` 普查后补）。
//!
//! MySQL 语义：**拷 schema、不拷数据**（列名 / 类型 / `NOT NULL` 全部带过去）。
//! 本用例守三件事：
//!
//! ① 新表列定义与源表逐字段相同（含可空性 —— 漏了它，写入侧的 NOT NULL 校验
//!    会在新表上静默失效）；
//! ② 新表是**空的**（拷了数据就是另一个语义了）；
//! ③ 源表一个字节没动（拷 schema 不该碰源表的数据）。
//!
//! 反证形态：修好之前 `CREATE TABLE … LIKE` 落在 `parse_create_table` 的
//! "requires column definitions" 拒绝上（1149）—— 而它是 MySQL 客户端/脚本
//! 里最常见的建表形态之一。

use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementQuery, CommandStatementUpdate, DoPutUpdateResult};
use arrow_flight::{FlightData, FlightDescriptor, PutResult};
use futures::StreamExt;
use prost::Message;
use yuntun_catalog::CatalogOps;
use yuntun_server::Lakehouse;

// ---------------------------------------------------------------- 夹具

async fn exec(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    sql: &str,
) -> i64 {
    let first = FlightData {
        flight_descriptor: Some(FlightDescriptor {
            r#type: 2,
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

async fn query(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    sql: &str,
) -> Vec<arrow::record_batch::RecordBatch> {
    let info = client
        .get_flight_info(arrow_flight::FlightDescriptor {
            r#type: 2,
            cmd: yuntun_server::flight::command_bytes(&CommandStatementQuery {
                query: sql.to_string(),
                transaction_id: None,
            })
            .into(),
            path: vec![],
        })
        .await
        .unwrap_or_else(|e| panic!("get_flight_info `{sql}` 失败：{e}"))
        .into_inner();
    let ticket = info.endpoint[0].ticket.clone().expect("ticket");
    let mut stream = client.do_get(ticket).await.unwrap().into_inner();
    let mut datas = Vec::new();
    while let Some(fd) = stream.next().await {
        datas.push(fd.unwrap());
    }
    arrow_flight::utils::flight_data_to_batches(&datas).unwrap()
}

/// 取某表当前 Arrow schema（字段名 + 类型 + 可空性）。
async fn table_schema(
    lakehouse: &Arc<Lakehouse>,
    table: &str,
) -> Vec<(String, arrow::datatypes::DataType, bool)> {
    let meta = lakehouse
        .catalog
        .get_table(table)
        .await
        .unwrap_or_else(|e| panic!("get_table {table}: {e}"))
        .unwrap_or_else(|| panic!("表 {table} 应存在"));
    meta.schema()
        .unwrap()
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
        .collect()
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
async fn create_table_like_copies_schema_without_data() {
    let guard = yuntun_testkit::TestDir::tmpfs("sql-create-like");
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

    // 源表：三种类型 + 一个 NOT NULL（可空性必须带过去）
    exec(&mut client, "CREATE TABLE src (a INT NOT NULL, b TEXT, c DOUBLE)").await;
    assert_eq!(
        exec(&mut client, "INSERT INTO src VALUES (1, 'x', 2.5)").await,
        1
    );

    // ---- CREATE TABLE ... LIKE ----
    exec(&mut client, "CREATE TABLE dst LIKE src").await;

    // ① schema 逐字段相同（含 NOT NULL）
    let src_schema = table_schema(&lakehouse, "public.src").await;
    let dst_schema = table_schema(&lakehouse, "public.dst").await;
    assert_eq!(dst_schema, src_schema, "LIKE 必须逐字段拷贝 schema（含可空性）");
    assert!(!dst_schema[0].2, "NOT NULL 必须带过去（否则写入侧校验在新表上失效）");

    // ② 新表是空的（拷 schema 不拷数据）
    let rows = query(&mut client, "SELECT count(*) AS c FROM dst").await;
    let c = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(c, 0, "LIKE 只拷 schema，新表必须是空的");

    // ③ 源表没被碰（还在）
    let rows = query(&mut client, "SELECT count(*) AS c FROM src").await;
    let c = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(c, 1, "源表数据不该被动");

    // ④ 新表可写（schema 完整拷过来的直接证据）
    assert_eq!(
        exec(&mut client, "INSERT INTO dst VALUES (7, 'y', 8.5)").await,
        1
    );

    // ⑤ 源表不存在 → NotFound（不静默建空表）
    let e = {
        let first = FlightData {
            flight_descriptor: Some(FlightDescriptor {
                r#type: 2,
                cmd: yuntun_server::flight::command_bytes(&CommandStatementUpdate {
                    query: "CREATE TABLE dst2 LIKE nope".to_string(),
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
            .expect_err("LIKE 不存在的表必须报错")
    };
    assert!(
        e.message().contains("not found") || e.message().contains("NotFound"),
        "错误应点名源表不存在：{}",
        e.message()
    );

    shutdown.cancel();
}
