//! yuntun-client 端到端（S1.9）：in-process FlightServer + SDK 全流程。
//!
//! 覆盖：DDL/INSERT（简易轨 do_get 承载写语句）、`table_schema`、
//! `do_put` 批量写入与回执、`list_tables`、流式查询（query_stream）。

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio_util::sync::CancellationToken;
use yuntun_client::Client;
use yuntun_server::Lakehouse;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("host", DataType::Utf8, true),
        Field::new("usage", DataType::Float64, true),
    ]))
}

fn batch(ts: i64, host: &str, usage: f64) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![ts])) as arrow::array::ArrayRef,
            Arc::new(StringArray::from(vec![host])) as arrow::array::ArrayRef,
            Arc::new(Float64Array::from(vec![usage])) as arrow::array::ArrayRef,
        ],
    )
    .unwrap()
}

/// 起一个 in-process Flight 服务，返回已连接客户端与关停句柄。
async fn start_server() -> (
    Client,
    Arc<Lakehouse>,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    // 写盘测试默认走 tmpfs（内存盘）；真实落盘语义的用例用 TestDir::disk。
    // 注意：本函数返回后服务仍在使用该目录 → `into_path()` 放弃 Drop 清理
    //（目录留在 tmpfs，进程退出由系统回收）。
    let wal_dir = yuntun_testkit::TestDir::tmpfs("client-e2e-wal")
        .into_path()
        .to_string_lossy()
        .to_string();
    let cfg = yuntun_server::Config::from_toml(&format!(
        r#"
[server]
listen = "127.0.0.1:0"

[store]
type = "memory"

[wal]
dir = "{wal_dir}"

[chunk]
# 每个测试 = 一个节点：私有目录各用各的（私有目录闸门会（正确地）拒绝两个消费者）
spill_dir = "{wal_dir}/spill"

[ingest]
rows_threshold = 1
time_threshold_secs = 1
max_flush_delay_secs = 1
flush_phase_spread_secs = 0
scan_interval_ms = 20

[query]
cache_ttl_secs = 1
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
    let server_shutdown = shutdown.clone();
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move { server_shutdown.cancelled().await },
            )
            .await
            .unwrap();
    });

    let client = Client::connect(&addr.to_string()).await.unwrap();
    (client, lakehouse, shutdown, handle)
}

/// 轮询等待「单值查询」返回期望值（攒批 flush + 缓存刷新窗口，抗机器负载）。
async fn wait_count(client: &Client, sql: &str, expect: i64) {
    let mut last: Option<String> = None;
    for _ in 0..40 {
        match client.query(sql).await {
            Ok(batches) => {
                let got = batches
                    .first()
                    .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
                    .map(|a| a.value(0));
                if got == Some(expect) {
                    return;
                }
                last = Some(format!("got={got:?}"));
            }
            Err(e) => last = Some(format!("err={e}")),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("等待 {expect} 行超时（10s）：{sql}；最后状态: {last:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_sdk_end_to_end() {
    let (client, _lakehouse, shutdown, server) = start_server().await;

    // ① DDL（简易轨 do_get 承载写语句）
    client
        .execute("CREATE TABLE cpu (ts BIGINT, host TEXT, usage DOUBLE)")
        .await
        .unwrap();

    // ② schema 探测（get_schema，逻辑计划）
    let schema = client.table_schema("cpu").await.unwrap();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "ts");
    assert_eq!(schema.field(2).data_type(), &DataType::Float64);

    // ③ do_put 批量写入 + 回执
    let receipts = client
        .insert("cpu", vec![batch(1, "a", 0.5), batch(2, "b", 0.75)])
        .await
        .unwrap();
    assert_eq!(receipts.len(), 2, "每批一个回执（逐批 WAL fsync）");
    assert_eq!(receipts.iter().map(|r| r.row_count).sum::<u64>(), 2);
    assert_eq!(receipts[0].table, "cpu");
    assert_eq!(receipts[0].shard, yuntun_client::DEFAULT_SHARD);
    assert!(receipts[0].wal_seq > 0);

    // ④ 表清单
    let tables = client.list_tables().await.unwrap();
    assert_eq!(tables, vec!["cpu".to_string()]);

    // ⑤ 可见性 → 一次性查询
    wait_count(&client, "SELECT count(*) AS c FROM yuntun.public.cpu", 2).await;
    let batches = client
        .query("SELECT count(*) AS c, sum(usage) AS s FROM yuntun.public.cpu")
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    let c = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(c.value(0), 2);
    let s = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((s.value(0) - 1.25).abs() < f64::EPSILON);

    // ⑥ 流式查询（边到边用，不落全量）
    let mut stream = client
        .query_stream("SELECT ts, host FROM yuntun.public.cpu ORDER BY ts")
        .await
        .unwrap();
    let mut rows = 0usize;
    while let Some(b) = futures::StreamExt::next(&mut stream).await {
        let b = b.unwrap();
        assert_ne!(b.schema().field(0).name(), ""); // 数据批次带真实 schema
        rows += b.num_rows();
    }
    assert_eq!(rows, 2);

    // ⑦ INSERT ... VALUES（SQL 写入路径）与空批次写入
    client
        .execute("INSERT INTO cpu VALUES (3, 'c', 1.0)")
        .await
        .unwrap();
    let empty = client.insert("cpu", vec![]).await.unwrap();
    assert!(empty.is_empty(), "空批次不应发起 do_put");

    wait_count(&client, "SELECT count(*) AS c FROM cpu", 3).await;

    shutdown.cancel();
    let _ = server.await;
}

/// 查询当前行数（不等值，用于断言"重试之后**没有**增加"）。
async fn count_now(client: &Client, sql: &str) -> i64 {
    let batches = client.query(sql).await.unwrap();
    batches
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .map(|a| a.value(0))
        .unwrap_or(0)
}

/// 幂等键的**粒度是请求**，不是批次。
///
/// 一次 `insert` = 一个 DoPut 流 = 一个请求级键，但流里可以有多个批次。
/// 这两条断言缺一不可：
/// 1. 同流的第 2..N 批**必须落库** —— 若所有批次共用一个键，批次级预筛会把
///    它们当成"第 1 批的重试"丢掉（静默丢数据）；
/// 2. 整条请求重试**必须一行不加** —— 若键根本不去重，客户端超时重试就写双份
///    （静默重复计数）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replayed_request_is_deduplicated_batch_by_batch() {
    let (client, _lakehouse, shutdown, server) = start_server().await;
    client
        .execute("CREATE TABLE idem (ts BIGINT, host TEXT, usage DOUBLE)")
        .await
        .unwrap();

    let key = "req-multi-1".to_string();
    let rows = vec![batch(1, "a", 0.5), batch(2, "b", 0.75)];

    // ① 首次：两个批次都必须落库（派生键保证不被彼此判重）
    let r1 = client
        .insert_batches("idem", yuntun_client::DEFAULT_SHARD, rows.clone(), Some(key.clone()))
        .await
        .unwrap();
    assert_eq!(r1.len(), 2, "每批一个回执");
    assert_eq!(
        r1.iter().map(|r| r.row_count).sum::<u64>(),
        2,
        "同一请求的多个批次不得互相判重（否则静默丢数据）"
    );
    wait_count(&client, "SELECT count(*) AS c FROM idem", 2).await;

    // ② 整条请求重试（同键 + 同批次序列）：全部判重，行数不变
    let r2 = client
        .insert_batches("idem", yuntun_client::DEFAULT_SHARD, rows, Some(key))
        .await
        .unwrap();
    assert_eq!(
        r2.iter().map(|r| r.row_count).sum::<u64>(),
        0,
        "重试不得写入任何新行"
    );
    assert!(
        r2.iter().all(|r| r.duplicate),
        "重试的每批都应标记 duplicate（供客户端区分'0 行'与'失败'）"
    );

    // 给"如果真写了"留出可见窗口，再断言行数没有增长
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        count_now(&client, "SELECT count(*) AS c FROM idem").await,
        2,
        "重试后不得出现重复行"
    );

    shutdown.cancel();
    let _ = server.await;
}

// ---------------------------------------------------------------- `§174`：降级走到用户面前

/// **读不到的来源**：连接被拒（不是超时 —— 两者在错误里要能分开）。
#[derive(Debug)]
struct Unreachable;

#[async_trait::async_trait]
impl yuntun_store::ShardReader for Unreachable {
    fn tier(&self) -> yuntun_store::ShardTier {
        yuntun_store::ShardTier::Memory
    }
    fn version(&self) -> u64 {
        0
    }
    async fn shards_of(
        &self,
        _table: &str,
    ) -> Result<Vec<yuntun_store::ShardId>, yuntun_model::error::LakeError> {
        Err(yuntun_model::error::LakeError::Other("connection refused".into()))
    }
    async fn read_shard(
        &self,
        _id: &yuntun_store::ShardId,
        _known_manifest_ver: u64,
    ) -> Result<yuntun_store::ShardRead, yuntun_model::error::LakeError> {
        Err(yuntun_model::error::LakeError::Other("connection refused".into()))
    }
    async fn watermark(
        &self,
        _known_manifest_ver: u64,
    ) -> Result<yuntun_store::ShardRead, yuntun_model::error::LakeError> {
        Err(yuntun_model::error::LakeError::Other("connection refused".into()))
    }
}

/// `§174`：**降级必须走到用户面前** —— partial 标记从服务端到 SDK 的最后一跳。
///
/// 服务端在"有来源读不到"时照常返回可用结果并标记 partial（**故意的**降级，不是失败）。
/// 但那个标记如果没人读，降级就等于**静默少数据**：`count(*)` 会小一点却看不出来。
///
/// 这不是假设：容器 rig 曾在压缩提交期间观测到"行数从 24 瞬时掉到 21"（台账 `D-23`），
/// 当时**无法判断**那是"可见性缺口"还是"某个来源被降级"—— 因为这条链上没人读那个标记。
///
/// 两个方向都要钉（假警报同样是错：一个恒存在的标记会让调用方无视它）：
/// - 有来源读不到 ⇒ `query_full` 报 partial 并**点名**，同时照常给出可用结果；
/// - 全都读得到 ⇒ **不许**报。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_degraded_query_is_flagged_as_partial() {
    let (client, lakehouse, shutdown, server) = start_server().await;
    client
        .execute("CREATE TABLE cpu (ts BIGINT, host TEXT, usage DOUBLE)")
        .await
        .unwrap();
    client
        .insert("cpu", vec![batch(1, "a", 0.5), batch(2, "b", 0.75)])
        .await
        .unwrap();
    wait_count(&client, "SELECT count(*) AS c FROM yuntun.public.cpu", 2).await;

    // ① 完整：唯一来源读得到 ⇒ **不许**标 partial
    let ok = client
        .query_full("SELECT count(*) AS c FROM yuntun.public.cpu")
        .await
        .unwrap();
    assert!(
        ok.partial.is_none(),
        "完整结果被标成 partial = 假警报：{:?}",
        ok.partial
    );

    // ② 注入一个**读不到的来源**（名录 + 热读器）⇒ 下一个快照里它就是热来源之一
    lakehouse
        .catalog
        .register_datanode(yuntun_model::meta::DatanodeMember {
            instance_id: "inst-ghost".into(),
            address: "127.0.0.1:1".into(),
            registered_at_ms: 0,
        })
        .await
        .unwrap();
    lakehouse
        .query
        .catalog()
        .set_hot_shards("inst-ghost", Arc::new(Unreachable));
    lakehouse
        .query
        .catalog()
        .refresh(&lakehouse.catalog)
        .await
        .unwrap();

    let degraded = client
        .query_full("SELECT count(*) AS c FROM yuntun.public.cpu")
        .await
        .unwrap();
    let p = degraded
        .partial
        .expect("有来源读不到却没标记 = 静默少数据（这正是 `D-23` 当时缺的那层信息）");
    let text = p.to_string();
    assert!(
        text.contains("inst-ghost"),
        "缺失来源必须**点名**（否则排障的人不知道该去看谁）：{p}"
    );
    assert!(
        text.contains("connection refused"),
        "原因要能区分\"连接被拒\"与\"没响应\"：{p}"
    );
    // 降级 ≠ 失败：可用那部分照常返回
    assert_eq!(
        degraded.batches[0].num_rows(),
        1,
        "降级后仍必须给出可用结果"
    );

    // ③ **CLI 这一跳**（容器 rig 的判据 ⑥ 就架在它上面）：脚本必须能把
    //    "少行且是降级"与"少行且是缺口"分开 —— 所以警告要以**稳定标记**出现在 **stderr** 上，
    //    且**不许污染 stdout**（CSV/JSON 是管道输出）。
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_yuntun-cli"))
        .args([
            "--addr",
            client.addr(),
            "query",
            "--format",
            "csv",
            "SELECT count(*) AS c FROM yuntun.public.cpu",
        ])
        .output()
        .expect("跑 yuntun-cli");
    let err = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "降级不是失败：CLI 必须退 0；stderr={err}");
    assert!(
        err.contains("WARN: partial result"),
        "脚本要能认出这件事（判据 ⑥ 靠它区分降级与缺口）：{err}"
    );
    assert!(err.contains("inst-ghost"), "点名同样要走到 stderr：{err}");
    assert!(
        !stdout.contains("WARN"),
        "警告不许污染 stdout（CSV/JSON 是管道输出）：{stdout}"
    );

    shutdown.cancel();
    let _ = server.await;
}
