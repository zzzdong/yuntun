//! **schema 演进之后的 DML**（台账 `D-13` / `operation-log §155`）。
//!
//! `F.2`（`ALTER TABLE ADD COLUMN`）与 `F.3`（DELETE / UPDATE）各自独立都验过了，
//! 这一刀验的是**两者的交叉**：老文件里没有新列，而 DML 要按谓词动这些行。
//!
//! 判据只有一条但很硬：**谓词与表达式看到的东西 = 用户 `SELECT` 看到的东西**。
//! 老文件里缺的列在查询侧是 `NULL`（`ParquetSource` 的 `TableSchemaBuilder`），
//! 所以 DML 的定位扫描也必须按 `NULL` 求值 —— 否则：
//!
//! * `DELETE FROM t WHERE 新列 IS NULL` 会**整条失败**（`D-13` 修复前的形态）；
//! * 更糟的是"能跑但答案不同"：DML 与 SELECT 对同一批行给出不同判断（静默不一致）。
//!
//! 值得说明的是：修复前的行为是**响亮失败**而不是错结果，所以没有留下脏数据；
//! 但"加了一列之后就改不动/删不动"是实打实的可用性断层，且它正好是本仓
//! "两条独立通路必须同语义"纪律的一个盲点（读路径与写路径早就用 `align_batch` 对齐了，
//! 定位路径漏了）。

use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use arrow::array::{Array, Int32Array, Int64Array};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementUpdate, DoPutUpdateResult};
use arrow_flight::{FlightData, FlightDescriptor, PutResult};
use futures::StreamExt;
use prost::Message;
use yuntun_catalog::CatalogOps;
use yuntun_server::Lakehouse;
use yuntun_sql::session::SessionCtx;

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

async fn col_i64(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    sql: &str,
) -> Vec<i64> {
    let batches = collect_do_get(client, sql).await;
    let mut out = Vec::new();
    for b in &batches {
        let x = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("期望 Int64，实际 {:?}", b.column(0).data_type()));
        out.extend((0..x.len()).map(|i| x.value(i)));
    }
    out
}

/// `SELECT v, c ORDER BY v` 的两列结果（`c` 是 `ADD COLUMN c INT` 落的 `Int32`，可为 NULL）。
async fn rows_v_c(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
) -> Vec<(i64, Option<i32>)> {
    let batches = collect_do_get(client, "SELECT v, c FROM t ORDER BY v").await;
    let mut out = Vec::new();
    for b in &batches {
        let v = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let c = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((v.value(i), if c.is_null(i) { None } else { Some(c.value(i)) }));
        }
    }
    out
}

async fn wait_for_rows_in_files(lakehouse: &Lakehouse, table: &str, rows: u64) {
    for _ in 0..60 {
        let files = lakehouse
            .catalog
            .list_visible_files(table, lakehouse.catalog.current_snapshot().await, None)
            .await
            .unwrap();
        if files.iter().map(|f| f.row_count).sum::<u64>() >= rows {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("{table} 的 {rows} 行始终没落成文件");
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

// ---------------------------------------------------------------- 用例

/// `ADD COLUMN` 之后：老文件（缺列 ⇒ NULL）上的 **UPDATE 与 DELETE 都要按用户看到的那样生效**。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dml_respects_schema_evolution_on_old_files() {
    let guard = yuntun_testkit::TestDir::tmpfs("sql-dml-evolve");
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
    let mut session = SessionCtx::default();

    execute_update_sql(&mut client, "CREATE TABLE t (v BIGINT NOT NULL)").await;
    assert_eq!(
        execute_update_sql(&mut client, "INSERT INTO t VALUES (1),(2),(3),(4)").await,
        4
    );
    // 老文件先落成（这里只有 v 一列）
    wait_for_rows_in_files(&lakehouse, "public.t", 4).await;

    // ---- 加一列：表 schema 变宽，老文件里没有 c ----
    assert_eq!(
        execute_update_sql(&mut client, "ALTER TABLE t ADD COLUMN c INT").await,
        0,
        "DDL 的影响行数是 0"
    );
    assert_eq!(
        rows_v_c(&mut client).await,
        vec![(1, None), (2, None), (3, None), (4, None)],
        "**前提**：查询侧老文件的 c 就是 NULL（读路径的 schema 映射）"
    );

    // ① 缺列文件上的 UPDATE（`D-13` 修复前的形态：这里会以 schema 错误整条失败）
    assert_eq!(
        execute_update_sql(&mut client, "UPDATE t SET c = 7 WHERE v <= 2").await,
        2,
        "老文件里没有 c，但只有 v 的谓词照样命中"
    );
    assert_eq!(
        rows_v_c(&mut client).await,
        vec![(1, Some(7)), (2, Some(7)), (3, None), (4, None)],
        "被改的两行有新值；其余行仍是 NULL"
    );

    // ② 谓词看见的必须与用户看见的一致：`c = 7` 命中刚改的两行，`c IS NULL` 命中另外两行
    assert_eq!(
        execute_update_sql(&mut client, "UPDATE t SET c = 8 WHERE c = 7").await,
        2
    );
    assert_eq!(
        col_i64(&mut client, "SELECT v FROM t WHERE c IS NULL ORDER BY v").await,
        vec![3, 4],
        "NULL 语义在 DML 里与在查询里一致"
    );
    assert_eq!(
        rows_v_c(&mut client).await,
        vec![(1, Some(8)), (2, Some(8)), (3, None), (4, None)]
    );

    // ③ 新文件（带 c）与老文件（缺 c）**混合**：一次 UPDATE 跨两类文件
    assert_eq!(
        execute_update_sql(&mut client, "INSERT INTO t VALUES (5, 50)").await,
        1
    );
    assert_eq!(
        execute_update_sql(&mut client, "UPDATE t SET c = c + 1 WHERE c IS NOT NULL").await,
        3,
        "两个老文件里的行（8）+ 一个新文件里的行（50）"
    );
    assert_eq!(
        rows_v_c(&mut client).await,
        vec![(1, Some(9)), (2, Some(9)), (3, None), (4, None), (5, Some(51))]
    );

    // ④ 缺列文件上的 DELETE：老文件那两行（c 仍是 NULL）要被删掉
    assert_eq!(
        execute_update_sql(&mut client, "DELETE FROM t WHERE c IS NULL").await,
        2,
        "缺列文件的行也要能被谓词命中（同样靠对齐后的 NULL）"
    );
    assert_eq!(
        rows_v_c(&mut client).await,
        vec![(1, Some(9)), (2, Some(9)), (5, Some(51))],
        "删掉的正是 c IS NULL 的两行；其余原样"
    );

    // ⑤ 归纳：整个过程中**没有一次**"查询与 DML 说法不同"
    assert_eq!(
        col_i64(&mut client, "SELECT v FROM t WHERE c = 9 ORDER BY v").await,
        vec![1, 2]
    );
    assert_eq!(execute_update_sql(&mut client, "UPDATE t SET c = 9 WHERE c = 9").await, 2);
    let _ = &mut session;
    shutdown.cancel();
}
