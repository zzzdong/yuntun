//! **F.3e-2 `UPDATE t SET … WHERE …`** 的端到端验收（`plan.md` F.3 / `operation-log §154`）。
//!
//! 守 `plan.md` F.3 验收③（"UPDATE 后旧值没了、新值在"）并补四条这个仓最看重的反面：
//!
//! ① **旧值没了、新值在、谓词外的行一行不动**；
//! ② **原子可见的判据**（`F.7` 决策 5）：一次状态转换只推**一个**快照号，
//!    且"新行文件"与"被删行"落在**同一个**快照号上 —— 读侧不存在"删了没插"或"插了没删"的中间态；
//! ③ **已删的行不会被"更新回来"**（它们对用户不可见；连它们一起改 = 复活已删数据）；
//! ④ **重启后仍然生效**（`[meta] mode = "memory"`：目录重启即空，只能靠 WAL 重放
//!    —— `replay_wal_dml` 处理 `UpdatePayload` 时也必须**一条 op** 重建两半）；
//! ⑤ 形状不支持的一律**明确拒绝**（无 WHERE / `FROM` / `RETURNING` / 未知列 / 同列两次 / 只读）。

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

async fn refresh_cache(lakehouse: &Lakehouse) {
    lakehouse
        .query
        .catalog()
        .refresh(&(lakehouse.catalog.clone()))
        .await
        .unwrap();
}

async fn col_v(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    sql: &str,
) -> Vec<i64> {
    let batches = collect_do_get(client, sql).await;
    let mut out = Vec::new();
    for b in &batches {
        let a = b.column(0);
        let x = a
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("期望 Int64，实际 {:?}", a.data_type()));
        out.extend((0..x.len()).map(|i| x.value(i)));
    }
    out
}

/// 等到查询结果等于 `expect`（刚写入的行要等一个攒批扫描周期才"两头都看得见"）。
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

/// 等到该表的**已提交文件**覆盖了 `rows` 行。
///
/// 为什么要等：UPDATE 的第一步是 `force_flush`（把在途数据落盘）—— 那**也是一次状态转换**
/// （`commit_files`）。不等它，后面那条"一次状态转换只推一个快照号"就会把 flush 的那一个
/// 也算到 UPDATE 头上（测错了对象）。
async fn wait_for_rows_in_files(lakehouse: &Lakehouse, table: &str, rows: u64) {
    for _ in 0..50 {
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
    panic!("{table} 的 {rows} 行始终没落成文件：UPDATE 的快照增量断言无从谈起");
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
# `memory`：元数据不落盘 ⇒ 重启后**只能**靠 WAL 重放（本用例要证的正是那条路）
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
async fn update_rewrites_the_matching_rows_and_nothing_else() {
    let guard = yuntun_testkit::TestDir::tmpfs("sqlupdate-e2e");
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
        execute_update_sql(
            &mut client,
            "INSERT INTO t VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)"
        )
        .await,
        10
    );
    assert_eq!(
        col_v_until(
            &mut client,
            "SELECT v FROM t ORDER BY v",
            &(1..=10).collect::<Vec<i64>>()
        )
        .await,
        (1..=10).collect::<Vec<i64>>(),
        "前提：10 行都查得到"
    );

    // 先等数据落成文件（否则 UPDATE 的 force flush 会先推一个快照号 —— 那是 flush 的，
    // 不是 UPDATE 的）
    wait_for_rows_in_files(&lakehouse, "public.t", 10).await;
    // 快照号：一次 UPDATE 只能推**一个**（原子性判据的一半）
    let before = lakehouse.catalog.current_snapshot().await;
    assert_eq!(
        execute_update_sql(&mut client, "UPDATE t SET v = v + 100 WHERE v <= 3").await,
        3,
        "受影响行数 = 谓词命中的行数"
    );
    let after = lakehouse.catalog.current_snapshot().await;
    assert_eq!(after, before + 1, "**一次状态转换只推一个快照号**");

    assert_eq!(
        col_v(&mut client, "SELECT v FROM t ORDER BY v").await,
        vec![4, 5, 6, 7, 8, 9, 10, 101, 102, 103],
        "旧值（1/2/3）没了、新值（101/102/103）在、其余**逐行不变**"
    );

    // 原子性判据的另一半：新行文件与删除向量**同号**
    let files = lakehouse
        .catalog
        .list_visible_files("public.t", after, None)
        .await
        .unwrap();
    let new_file = files
        .iter()
        .find(|f| f.row_count == 3)
        .expect("必须有一个 3 行的产物文件");
    assert_eq!(new_file.valid_from, after, "新行文件在 `after` 生效");
    let dvs = lakehouse
        .catalog
        .list_deletions("public.t", after)
        .await
        .unwrap();
    assert_eq!(dvs.len(), 1, "被命中的源文件 ⇒ 一份删除向量");
    assert_eq!(dvs[0].card, 3);
    assert_eq!(dvs[0].applied_at, after, "删除向量与产物**同号** ⇒ 原子可见");

    // 旧快照看到的是**旧世界**（两半都不可见）
    let old_files = lakehouse
        .catalog
        .list_visible_files("public.t", before, None)
        .await
        .unwrap();
    assert!(
        !old_files.iter().any(|f| f.valid_from == after),
        "旧快照不许看到产物"
    );
    assert!(
        lakehouse
            .catalog
            .list_deletions("public.t", before)
            .await
            .unwrap()
            .is_empty(),
        "旧快照不许看到删除（否则就是「删了没插」= 少数据）"
    );

    // 谓词一行都没命中 ⇒ 什么都不发生（不推快照号）
    let snap = lakehouse.catalog.current_snapshot().await;
    assert_eq!(
        execute_update_sql(&mut client, "UPDATE t SET v = 0 WHERE v = 999").await,
        0
    );
    assert_eq!(
        lakehouse.catalog.current_snapshot().await,
        snap,
        "没命中任何行就不该有版本痕迹"
    );

    shutdown.cancel();
}

/// **已删的行不会被"更新回来"**：它们对用户不可见，连它们一起改 = 把删掉的行复活。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_never_resurrects_deleted_rows() {
    let guard = yuntun_testkit::TestDir::tmpfs("sqlupdate-deleted");
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
        execute_update_sql(&mut client, "INSERT INTO t VALUES (1),(2),(3),(4),(5)").await,
        5
    );
    assert_eq!(
        col_v_until(&mut client, "SELECT v FROM t ORDER BY v", &[1, 2, 3, 4, 5]).await,
        vec![1, 2, 3, 4, 5]
    );
    // 先删掉 v = 2
    execute_update_sql(&mut client, "DELETE FROM t WHERE v = 2").await;
    assert_eq!(
        col_v(&mut client, "SELECT v FROM t ORDER BY v").await,
        vec![1, 3, 4, 5]
    );

    // 再更新 v <= 3：谓词**看起来**命中 1/2/3，但 2 已经删了 ⇒ 只能改 1 和 3
    assert_eq!(
        execute_update_sql(&mut client, "UPDATE t SET v = v + 100 WHERE v <= 3").await,
        2,
        "已删的行不算受影响（它不可见）"
    );
    assert_eq!(
        col_v(&mut client, "SELECT v FROM t ORDER BY v").await,
        vec![4, 5, 101, 103],
        "**2 不许回来**（102 一旦出现就是复活已删数据）"
    );
    shutdown.cancel();
}

/// `[meta] mode="memory"` 下**重启**：两半都必须靠 WAL 重放收敛（且仍然原子）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_survives_restart_in_memory_meta_mode() {
    let guard = yuntun_testkit::TestDir::tmpfs("sqlupdate-restart");
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
            execute_update_sql(&mut client, "INSERT INTO t VALUES (1),(2),(3),(4)").await,
            4
        );
        assert_eq!(
            col_v_until(&mut client, "SELECT v FROM t ORDER BY v", &[1, 2, 3, 4]).await,
            vec![1, 2, 3, 4]
        );
        assert_eq!(
            execute_update_sql(&mut client, "UPDATE t SET v = v * 10 WHERE v IN (2, 4)").await,
            2
        );
        assert_eq!(
            col_v(&mut client, "SELECT v FROM t ORDER BY v").await,
            vec![1, 3, 20, 40]
        );
        shutdown.cancel();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // 第二次运行：同一个 WAL + 同一个 store（memory 形态 ⇒ 目录是空的，全靠重放）
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let _bg = lakehouse.spawn_background(&cfg);
    let mut client = serve(&lakehouse, shutdown.clone()).await;
    refresh_cache(&lakehouse).await;
    assert_eq!(
        col_v(&mut client, "SELECT v FROM t ORDER BY v").await,
        vec![1, 3, 20, 40],
        "重启之后：新值在、旧值不在（`replay_wal_dml` 一条 op 重建了两半）"
    );
    // 重放也必须保持原子：两半在**同一个**快照号上
    let snap = lakehouse.catalog.current_snapshot().await;
    let dvs = lakehouse
        .catalog
        .list_deletions("public.t", snap)
        .await
        .unwrap();
    assert_eq!(dvs.len(), 1);
    let files = lakehouse
        .catalog
        .list_visible_files("public.t", snap, None)
        .await
        .unwrap();
    assert!(
        files
            .iter()
            .any(|f| f.valid_from == dvs[0].applied_at),
        "重放之后两半仍然同号（原子可见不许在重放路径上退化）"
    );
    shutdown.cancel();
}

/// 形状不支持的一律明确拒绝（不猜用户想要什么）+ 只读形态的可读拒绝。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_refusals_are_explicit() {
    let guard = yuntun_testkit::TestDir::tmpfs("sqlupdate-refuse");
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
    execute_update_sql(&mut client, "INSERT INTO t VALUES (1)").await;

    let mut session = SessionCtx::default();
    // 只读装配：UPDATE 必须给**可读拒绝**
    let ro = yuntun_sql::SqlEngine::new_readonly(lakehouse.query.clone(), lakehouse.catalog.clone());
    match ro.execute("UPDATE t SET v = 1 WHERE v = 1", &mut session).await {
        Err(yuntun_sql::SqlError::ReadOnly) => {}
        Err(other) => panic!("只读形态应当报 ReadOnly，实际：{other}"),
        Ok(_) => panic!("只读形态下的 UPDATE 不许成功"),
    }

    for sql in [
        "UPDATE t SET v = 1",                        // 无 WHERE：等于整表重写
        "UPDATE t SET v = 1 FROM s WHERE t.v = 1",   // FROM
        "UPDATE t SET v = no_such = 1 WHERE v = 1",  // 表达式里引用不存在的列
        "UPDATE t SET v = 1, v = 2 WHERE v = 1",     // 同一列两次
        "UPDATE t SET v = 1 WHERE v = 1 RETURNING v",// RETURNING
    ] {
        let msg = match lakehouse.sql.execute(sql, &mut session).await {
            Err(e) => e.to_string(),
            Ok(_) => panic!("`{sql}` 应当被拒绝"),
        };
        // 表达式里的列名由 DataFusion 在**执行期**校验（"定位扫描求谓词失败 + No field"）——
        // 同样是响亮失败，只是类别不同；这里只要求"说清了原因"
        assert!(
            msg.contains("未支持")
                || msg.contains("没有列")
                || msg.contains("两次")
                || msg.contains("parse")
                || msg.contains("No field"),
            "拒绝必须说清是什么没支持（`{sql}`）：{msg}"
        );
    }
    // 拒绝不留副作用：数据还是原样
    assert_eq!(
        col_v_until(&mut client, "SELECT v FROM t ORDER BY v", &[1]).await,
        vec![1]
    );
    shutdown.cancel();
}
