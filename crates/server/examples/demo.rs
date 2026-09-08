//! 端到端演示：Flight 写入 → WAL → 攒批 → Parquet(S3) → Meta → DataFusion SQL 查询。
//! 运行：cargo run -p yuntun-server --example demo

use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use arrow::array::{Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, FlightDescriptor};
use tokio_stream::StreamExt;
use yuntun_catalog::CatalogOps;
use yuntun_model::ops::CreateTableRequest;
use yuntun_server::Lakehouse;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "event_time",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("user", DataType::Utf8, true),
        Field::new("endpoint", DataType::Utf8, true),
        Field::new("cost_ms", DataType::Int64, true),
    ]))
}

fn make_batch(
    base_ms: i64,
    users: &[&str],
    endpoint: &str,
    cost: i64,
) -> arrow::record_batch::RecordBatch {
    let n = users.len();
    arrow::record_batch::RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampMillisecondArray::from(
                (0..n)
                    .map(|i| base_ms + i as i64 * 1000)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(users.to_vec())),
            Arc::new(StringArray::from(vec![endpoint; n])),
            Arc::new(Int64Array::from(
                (0..n as i64).map(|i| cost + 10 * i).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn print_batches(title: &str, batches: &[arrow::record_batch::RecordBatch]) {
    println!("\n=== {title} ===");
    if batches.is_empty() {
        println!("(empty)");
        return;
    }
    arrow::util::pretty::print_batches(batches).unwrap();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!("yuntun demo starting");

    // ---- ① 装配 Lakehouse（内存 S3 模拟 + 临时 WAL）----
    let wal_dir = format!("/tmp/yuntun-demo-wal-{}", std::process::id());
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
scan_interval_ms = 50
"#
    ))
    .unwrap();

    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(Lakehouse::build_with_shutdown(&cfg, shutdown.clone()).await?);

    // 建表（幂等键：可选。Metrics/Traces 模板可关闭；Audit/General 默认强制）
    lakehouse
        .catalog
        .create_table(CreateTableRequest {
            name: "api_audit".into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig {
                require_idempotency_key: false,
                ..yuntun_model::meta::IngestConfig::standard()
            },
        })
        .await
        .unwrap();
    println!("[1] 建表 api_audit 完成");

    // ---- ② 启动后台任务（攒批/监控/Compaction/缓存刷新）----
    let _bg = lakehouse.spawn_background(&cfg);
    println!("[2] 后台任务已启动（攒批循环 / WAL 超时监控 / Compaction / 缓存刷新）");

    // ---- ③ 启动 Flight gRPC 服务 ----
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let svc = arrow_flight::flight_service_server::FlightServiceServer::new(
        yuntun_server::FlightServer::new(lakehouse.ingestor.clone(), lakehouse.query.clone()),
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
    println!("[3] Flight gRPC 服务已监听 http://{addr}");

    // ---- ④ 客户端经 Flight 写入两个 shard 的数据 ----
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))?
        .connect()
        .await?;
    let mut client = FlightServiceClient::new(channel);

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as i64;

    for (shard, users, ep, cost, key) in [
        (
            "s0",
            vec!["alice", "bob", "carol"],
            "/api/login",
            120,
            "req-001",
        ),
        ("s1", vec!["dave", "erin"], "/api/orders", 300, "req-002"),
    ] {
        let mut msgs = arrow_flight::utils::batches_to_flight_data(
            schema().as_ref(),
            vec![make_batch(now_ms, &users, ep, cost)],
        )?;
        let mut data = msgs.remove(1);
        data.app_metadata = format!(r#"{{"idempotency_key":"{key}"}}"#)
            .into_bytes()
            .into();
        let schema_msg = FlightData {
            flight_descriptor: Some(FlightDescriptor {
                r#type: 1,
                path: vec!["api_audit".into(), shard.into()],
                cmd: Default::default(),
            }),
            ..msgs.remove(0)
        };

        let mut responses = client
            .do_put(tokio_stream::iter(vec![schema_msg, data]))
            .await?
            .into_inner();
        let ack = responses.next().await.unwrap()?;
        let receipt: serde_json::Value = serde_json::from_slice(&ack.app_metadata)?;
        println!(
            "[4] Flight DoPut shard={shard} rows={} → WAL ack seq={}（已 fsync，回执可见性 ~{}s）",
            receipt["row_count"], receipt["wal_seq"], receipt["expected_visible_in_secs"]
        );
    }

    // ---- ⑤ 等 flush（WAL → Parquet → Meta 提交）----
    println!("[5] 等待攒批 flush（WAL → 编码 Parquet → 写对象存储 → Meta CommitFiles）...");
    tokio::time::sleep(Duration::from_millis(700)).await;
    let snap = lakehouse.catalog.current_snapshot().await;
    let files = lakehouse
        .catalog
        .list_visible_files("api_audit", snap, None)
        .await?;
    for f in &files {
        println!(
            "    committed: {} ({} rows, {} bytes, schema v{})",
            f.file_path, f.row_count, f.file_size, f.schema_version
        );
    }

    // ---- ⑥ SQL 查询（走 Flight do_get 外部通道：Ticket = SQL）----
    lakehouse
        .query
        .cache()
        .refresh(&(lakehouse.catalog.clone() as Arc<dyn CatalogOps>))
        .await?;

    // FlightServiceClient 可克隆（tonic Channel 多路复用），闭包按值捕获
    async fn run(
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

    print_batches(
        "SQL(do_get): SELECT * FROM yuntun.public.api_audit ORDER BY cost_ms",
        &run(
            &mut client,
            "SELECT * FROM yuntun.public.api_audit ORDER BY cost_ms",
        )
        .await,
    );
    print_batches("SQL(do_get): SELECT \"user\", count(*) cnt, avg(cost_ms) avg_ms FROM yuntun.public.api_audit GROUP BY \"user\" ORDER BY cnt DESC", &run(&mut client, "SELECT \"user\", count(*) cnt, avg(cost_ms) avg_ms FROM yuntun.public.api_audit GROUP BY \"user\" ORDER BY cnt DESC").await);
    print_batches(
        "SQL(do_get): SELECT * FROM yuntun.public.api_audit WHERE cost_ms > 130",
        &run(
            &mut client,
            "SELECT * FROM yuntun.public.api_audit WHERE cost_ms > 130",
        )
        .await,
    );

    println!("\n[6] 全链路贯通（全部经由 gRPC）：DoPut 写入 → WAL(组提交 fsync) → 攒批(Jitter) → Parquet → Meta(快照隔离) → DoGet SQL 查询 ✓");

    // ---- ⑦（可选）保持服务运行，供外部客户端（pyarrow / ADBC）冒烟 ----
    if std::env::var("YUNTUN_DEMO_SERVE").as_deref() == Ok("1") {
        println!("[7] YUNTUN_DEMO_SERVE=1 → 保持服务运行，等待 Ctrl-C（外部客户端可连接）");
        tokio::signal::ctrl_c().await?;
    }

    shutdown.cancel();
    Ok(())
}
