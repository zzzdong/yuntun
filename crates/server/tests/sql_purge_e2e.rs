//! **`F.3f`：无 `WHERE` 的全表删（`purge_table_files`，设计 §7）** 的端到端验收。
//!
//! 设计 §7 对这一形态的口径是**文件级下线**（"文件级与行级分属两层，非第二形态"）：
//! 不生成任何删除向量，而是把此刻**可见**的全部文件标墓碑 + 悬挂 DV 同步 revoke。
//!
//! 本用例守四件事：
//!
//! ① **语义**：`DELETE FROM t` 之后一行都查不到，且只有**一个**快照号前进；
//! ② **行数口径**：受影响行数 = **用户原本看得见**的行数（被 DV 藏起来的行不算 ——
//!    与行级删一致：先 `DELETE WHERE v = 3` 再 `DELETE FROM t` 应报 4 而不是 5）；
//! ③ **不许误伤历史**：清除**之前**的快照仍然看得见那些文件（快照隔离），
//!    悬挂 DV 在同一个快照号上被 revoke；
//! ④ **清除之后写入的数据必须活着**（清除不是"把表变成黑洞"），重启之后也是。
//!
//! 外加两条"业务上会立刻撞到"的检查：空表上再删是 0 行且不推版本号；
//! 不存在的表报 `NotFound`、只读装配给可读的拒绝。

use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use arrow::array::Int64Array;
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

async fn col_v(
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

async fn col_v_until(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    sql: &str,
    expect: &[i64],
) -> Vec<i64> {
    let mut last = Vec::new();
    for _ in 0..40 {
        last = col_v(client, sql).await;
        if last == expect {
            return last;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    last
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn purge_deletes_the_whole_table_and_never_eats_later_writes() {
    let guard = yuntun_testkit::TestDir::tmpfs("sql-purge-e2e");
    let base = guard.string();
    let wal_dir = format!("{base}/wal");
    let store_root = format!("{base}/store");
    let cfg = config(&wal_dir, &store_root);

    {
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
            execute_update_sql(&mut client, "INSERT INTO t VALUES (1),(2),(3),(4),(5)").await,
            5
        );
        wait_for_rows_in_files(&lakehouse, "public.t", 5).await;
        assert_eq!(
            col_v_until(&mut client, "SELECT v FROM t ORDER BY v", &[1, 2, 3, 4, 5]).await,
            vec![1, 2, 3, 4, 5]
        );

        // 先做一次**行级**删（留下一个悬挂 DV，用来看"同步 revoke"）
        assert_eq!(
            execute_update_sql(&mut client, "DELETE FROM t WHERE v = 3").await,
            1
        );
        assert_eq!(
            col_v(&mut client, "SELECT v FROM t ORDER BY v").await,
            vec![1, 2, 4, 5]
        );
        let before_purge = lakehouse.catalog.current_snapshot().await;
        let files_before = lakehouse
            .catalog
            .list_visible_files("public.t", before_purge, None)
            .await
            .unwrap();
        assert!(!files_before.is_empty(), "前提：清除之前得有已提交文件");

        // ---- 全表删（无 WHERE）----
        assert_eq!(
            execute_update_sql(&mut client, "DELETE FROM t").await,
            4,
            "受影响行数 = **用户原本看得见**的行数（5 行里有 1 行已被 DV 藏起来）"
        );
        let after = lakehouse.catalog.current_snapshot().await;
        assert_eq!(after, before_purge + 1, "一次整表清除只推**一个**快照号");
        assert!(
            col_v(&mut client, "SELECT v FROM t ORDER BY v").await.is_empty(),
            "清除之后一行都查不到"
        );

        // ① 文件级下线（不是逐行标 DV）
        assert!(
            lakehouse
                .catalog
                .list_visible_files("public.t", after, None)
                .await
                .unwrap()
                .is_empty(),
            "清除之后没有可见文件"
        );
        // ② 悬挂 DV 在**同一个快照号**上被 revoke
        let dvs_new = lakehouse
            .catalog
            .list_deletions("public.t", after)
            .await
            .unwrap();
        assert!(dvs_new.is_empty(), "清除之后没有生效中的删除向量");
        let dvs_old = lakehouse
            .catalog
            .list_deletions("public.t", before_purge)
            .await
            .unwrap();
        assert_eq!(
            dvs_old.len(),
            1,
            "清除**之前**的快照仍看得见那份 DV（历史可回答）"
        );
        // ③ 快照隔离的结构性检查：旧快照的文件与 DV 都还在
        assert_eq!(
            lakehouse
                .catalog
                .list_visible_files("public.t", before_purge, None)
                .await
                .unwrap()
                .len(),
            files_before.len(),
            "清除之前开始的查询看不到这次清除"
        );

        // ④ **清除之后写入的数据必须活着**（否则"删表"变成了"表变黑洞"）
        assert_eq!(
            execute_update_sql(&mut client, "INSERT INTO t VALUES (7),(8)").await,
            2
        );
        assert_eq!(
            col_v_until(&mut client, "SELECT v FROM t ORDER BY v", &[7, 8]).await,
            vec![7, 8],
            "清除之后新写入的数据必须查得到"
        );

        // ⑤ 空表上再删：0 行、**不推版本号**
        let empty_snap = lakehouse.catalog.current_snapshot().await;
        assert_eq!(
            execute_update_sql(&mut client, "DELETE FROM t").await,
            2,
            "这一次把刚才那两行也清掉"
        );
        let after_second = lakehouse.catalog.current_snapshot().await;
        assert_eq!(
            execute_update_sql(&mut client, "DELETE FROM t").await,
            0,
            "空表上再删是 0 行"
        );
        assert_eq!(
            lakehouse.catalog.current_snapshot().await,
            after_second,
            "空操作不推快照号"
        );
        assert!(empty_snap < after_second);

        // ⑥ 只读装配 + 不存在的表：都是**可读的拒绝**
        let mut session = SessionCtx::default();
        let ro = yuntun_sql::SqlEngine::new_readonly(
            lakehouse.query.clone(),
            lakehouse.catalog.clone(),
        );
        match ro.execute("DELETE FROM t", &mut session).await {
            Err(yuntun_sql::SqlError::ReadOnly) => {}
            Err(other) => panic!("只读形态应当报 ReadOnly，实际：{other}"),
            Ok(_) => panic!("只读形态下的全表删不许成功"),
        }
        let msg = match lakehouse
            .sql
            .execute("DELETE FROM no_such_table", &mut session)
            .await
        {
            Err(e) => e.to_string(),
            Ok(_) => panic!("不存在的表必须报错（而不是「清了个空」）"),
        };
        assert!(
            msg.contains("no_such_table") || msg.contains("not found") || msg.contains("不存在"),
            "拒绝要说清是哪张表：{msg}"
        );
        assert!(
            col_v(&mut client, "SELECT v FROM t ORDER BY v").await.is_empty(),
            "前提：重启之前表是空的"
        );
        shutdown.cancel();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // ---- 重启（memory 形态 ⇒ 目录是空的，全靠 WAL 重放）----
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let _bg = lakehouse.spawn_background(&cfg);
    let mut client = serve(&lakehouse, shutdown.clone()).await;
    // （重启后的第一次查询**不需要**显式刷缓存：读路径自己会保鲜，`§163`）
    assert!(
        col_v(&mut client, "SELECT v FROM t ORDER BY v").await.is_empty(),
        "**重启之后表仍然是空的**（整表清除只改目录 ⇒ 必须靠 WAL 重放，见 `PurgePayload` 的文档）"
    );
    // 而且重启之后表还能用
    assert_eq!(
        execute_update_sql(&mut client, "INSERT INTO t VALUES (9)").await,
        1
    );
    assert_eq!(
        col_v_until(&mut client, "SELECT v FROM t ORDER BY v", &[9]).await,
        vec![9],
        "重启之后写入照样生效"
    );
    shutdown.cancel();
}
