//! **F.3 `DELETE FROM t WHERE …`** 的端到端验收（`plan.md` F.3 / `operation-log §149`）。
//!
//! 守 `plan.md` F.3 的验收（① 删后查不到且谓词外的行还在）并补三条这个仓最看重的反面：
//!
//! ① **删后查不到、谓词外的行一行不少**（少了是错、多了也是错）；
//! ② **"删刚插的行"不会复活**：行还在 chunk 里就被删，删除必须**先把它落盘**再定位
//!    （`delta-dml-design §4.3`；这条是 F.3 最容易被忽略的坑）；
//! ③ **重启后删除仍然生效**（`[meta] mode="memory"` 下元数据不落盘 ⇒ 只有 `replay_wal_dml` 这条路）；
//! ④ **只读形态**给**可读拒绝**；多表 / `USING` / `RETURNING` / 无 `WHERE` **明确拒绝**。
//!
//! > 本用例走 `mode = "memory"`：`embedded`（默认）形态的目录写路径（metanode op）
//! > 还没接线，那边的 DELETE 会**明确报错**而不是静默不生效（`§148.3` 的抵押①）。

use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use arrow::array::Int64Array;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementUpdate, DoPutUpdateResult};
use arrow_flight::{FlightData, FlightDescriptor, PutResult};
use futures::StreamExt;
use prost::Message;
use yuntun_catalog::CatalogOps;
use yuntun_server::Lakehouse;
use yuntun_sql::SqlEngine;
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

async fn execute_update(
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

/// `v` 列升序的整数值（查询都带 `ORDER BY`）。
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

/// 等到查询结果等于 `expect`（刚写入的行要等一个攒批扫描周期才"两头都看得见"：
/// 回执只承诺"一个扫描周期内可查"，见 `flush_e2e` 的 `expected_visible_in_secs`）。
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
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    last
}

fn config(wal_dir: &str, store_root: &str, eager: bool) -> yuntun_server::Config {
    // `eager` = 写入很快落盘；否则数据会**留在 chunk 里**（用来证明"删除会先强制落盘"）
    let ingest = if eager {
        "rows_threshold = 1\ntime_threshold_secs = 5\nmax_flush_delay_secs = 1"
    } else {
        "rows_threshold = 10000000\ntime_threshold_secs = 3600\nmax_flush_delay_secs = 3600"
    };
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
{ingest}
flush_phase_spread_secs = 0
scan_interval_ms = 20
"#
    ))
    .unwrap()
}

/// 起一次 `Lakehouse`（embedded 形态专用）：**重试**到 fjall 目录锁可用。
///
/// ⚠️ 实测：**同一进程内**重新打开同一个 meta 目录会一直 `FjallError: Locked`
/// （`MetaNode::drop` 之后锁也没释放，重试 5 秒无效 —— `§151.3`）。
/// 所以本用例**不做**"同进程重启"；这个助手留着是为了"起第一次"时的时序抖动。
async fn build_embedded(cfg: &yuntun_server::Config, shutdown: CancellationToken) -> Arc<Lakehouse> {
    for i in 0..25 {
        match Lakehouse::build_with_shutdown(cfg, shutdown.clone()).await {
            Ok(lh) => return Arc::new(lh),
            Err(e) if format!("{e}").contains("Locked") => {
                if i == 0 {
                    // 记一条：看到它说明"上一次的锁还没放"，不是失败
                    eprintln!("[test] meta 目录仍被上一次运行锁着，重试…");
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => panic!("起 Lakehouse 失败：{e}"),
        }
    }
    panic!("重试 5 秒仍拿不到 meta 目录（fjall 锁没释放）");
}

/// **默认形态**（`[meta] mode = "embedded"`：进程内 1 节点 raft + fjall 落盘）。
///
/// 与 `config()` 的唯一差别是目录形态 —— 这正是 `F.3c-3` 要证的那一件事：
/// 删除向量的登记/读取**走目录（raft）**，而不是"某个进程的内存"。
fn embedded_config(wal_dir: &str, store_root: &str, meta_dir: &str) -> yuntun_server::Config {
    yuntun_server::Config::from_toml(&format!(
        r#"
[store]
type = "local"
root = "{store_root}"

[wal]
dir = "{wal_dir}"

[meta]
mode = "embedded"
dir = "{meta_dir}"

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

/// 目录里该表**生效中**的 DV 覆盖了多少行（内部事实的断言，不是"看起来对不对"）。
async fn deleted_card(lakehouse: &Lakehouse, table: &str) -> u64 {
    let snap = lakehouse.catalog.current_snapshot().await;
    lakehouse
        .catalog
        .list_deletions(table, snap)
        .await
        .unwrap()
        .iter()
        .map(|d| u64::from(d.card))
        .sum()
}

// ---------------------------------------------------------------- 用例

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_removes_only_the_matching_rows() {
    let base_guard = yuntun_testkit::TestDir::tmpfs("sqldelete-e2e");
    let base = base_guard.string();
    let cfg = config(&format!("{base}/wal"), &format!("{base}/store"), true);
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let _bg = lakehouse.spawn_background(&cfg);
    let mut client = serve(&lakehouse, shutdown.clone()).await;

    execute_update(&mut client, "CREATE TABLE t (v BIGINT NOT NULL)").await;
    assert_eq!(
        execute_update(
            &mut client,
            "INSERT INTO t VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)"
        )
        .await,
        10
    );
    assert_eq!(
        col_v_until(&mut client, "SELECT v FROM t ORDER BY v", &(1..=10).collect::<Vec<i64>>())
            .await,
        (1..=10).collect::<Vec<i64>>(),
        "前提：写入的 10 行都查得到"
    );

    // ① 删除命中 3 行
    assert_eq!(execute_update(&mut client, "DELETE FROM t WHERE v <= 3").await, 3);
    assert_eq!(
        col_v(&mut client, "SELECT v FROM t ORDER BY v").await,
        (4..=10).collect::<Vec<i64>>(),
        "删后：被删的 3 行查不到，其余的**一行不少**"
    );
    // 谓词只用了一次；再做一次**不相邻**的删除，验证多份 DV 叠加
    assert_eq!(
        execute_update(&mut client, "DELETE FROM t WHERE v >= 9").await,
        2
    );
    assert_eq!(
        col_v(&mut client, "SELECT v FROM t ORDER BY v").await,
        (4..=8).collect::<Vec<i64>>(),
        "两次删除必须**叠加**（第二次不许把第一次盖掉）"
    );
    assert_eq!(deleted_card(&lakehouse, "public.t").await, 5, "目录里的 DV 覆盖 5 行");

    // 谓词没命中任何行 ⇒ 受影响 0，且**不留痕迹**（不推快照号）
    let snap_before = lakehouse.catalog.current_snapshot().await;
    assert_eq!(
        execute_update(&mut client, "DELETE FROM t WHERE v = 999").await,
        0
    );
    assert_eq!(
        lakehouse.catalog.current_snapshot().await,
        snap_before,
        "没命中任何行就不该有版本痕迹"
    );

    // 行还在（不多不少）—— 缓存刷新后再确认一次，防止"只是缓存里删了"
    refresh_cache(&lakehouse).await;
    assert_eq!(
        col_v(&mut client, "SELECT v FROM t ORDER BY v").await,
        (4..=8).collect::<Vec<i64>>()
    );

    shutdown.cancel();
}

/// **删刚插的行**：数据还在 chunk 里，删除必须先把它落盘再定位 ——
/// 否则那批行落盘之后会以"没被删掉"的样子出现（复活）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_of_just_inserted_rows_flushes_first() {
    let base_guard = yuntun_testkit::TestDir::tmpfs("sqldelete-flush");
    let base = base_guard.string();
    // `eager = false`：攒批循环**自己不会** flush，只有 DELETE 的强制落盘能把它送下去
    let cfg = config(&format!("{base}/wal"), &format!("{base}/store"), false);
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let _bg = lakehouse.spawn_background(&cfg);
    let mut client = serve(&lakehouse, shutdown.clone()).await;

    execute_update(&mut client, "CREATE TABLE t (v BIGINT NOT NULL)").await;
    assert_eq!(
        execute_update(&mut client, "INSERT INTO t VALUES (1),(2),(3)").await,
        3
    );
    // 前提：此刻还没有任何文件（数据确实在途）
    assert!(
        lakehouse
            .catalog
            .list_visible_files("public.t", lakehouse.catalog.current_snapshot().await, None)
            .await
            .unwrap()
            .is_empty(),
        "前提：惰性配置下数据应当还在 chunk 里"
    );

    assert_eq!(execute_update(&mut client, "DELETE FROM t WHERE v = 2").await, 1);
    assert_eq!(
        col_v(&mut client, "SELECT v FROM t ORDER BY v").await,
        vec![1, 3],
        "刚插进来的行也要删得掉（强制落盘 → 定位 → 标记）"
    );
    assert!(
        !lakehouse
            .catalog
            .list_visible_files("public.t", lakehouse.catalog.current_snapshot().await, None)
            .await
            .unwrap()
            .is_empty(),
        "删除必须先把在途数据落盘（否则删不到那些行）"
    );

    // 再多等几轮 + 刷缓存：**不许复活**
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        refresh_cache(&lakehouse).await;
    }
    assert_eq!(
        col_v(&mut client, "SELECT v FROM t ORDER BY v").await,
        vec![1, 3],
        "延迟再查也不许把删掉的行放回来"
    );

    shutdown.cancel();
}

/// `[meta] mode="memory"` 下**重启**：表清单靠 `replay_wal_ddl`、删除靠 `replay_wal_dml`
/// —— 删除不得因为重启而消失（那正是"用户以为删了"的形态）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_survives_restart_in_memory_meta_mode() {
    let base_guard = yuntun_testkit::TestDir::tmpfs("sqldelete-restart");
    let base = base_guard.string();
    let wal_dir = format!("{base}/wal");
    let store_root = format!("{base}/store");
    let cfg = config(&wal_dir, &store_root, true);

    // ============ 第一次运行：建表 + 写入 + 删除 ============
    {
        let shutdown = CancellationToken::new();
        let lakehouse = Arc::new(
            Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
                .await
                .unwrap(),
        );
        let _bg = lakehouse.spawn_background(&cfg);
        let mut client = serve(&lakehouse, shutdown.clone()).await;
        execute_update(&mut client, "CREATE TABLE t (v BIGINT NOT NULL)").await;
        assert_eq!(
            execute_update(&mut client, "INSERT INTO t VALUES (1),(2),(3),(4)").await,
            4
        );
        assert_eq!(execute_update(&mut client, "DELETE FROM t WHERE v IN (2, 4)").await, 2);
        assert_eq!(col_v(&mut client, "SELECT v FROM t ORDER BY v").await, vec![1, 3]);
        shutdown.cancel();
        // 给后台任务一点时间退出（WAL 已 fsync，删除已提交）
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // ============ 第二次运行：同一个 WAL + 同一个 store ============
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
        vec![1, 3],
        "重启之后：表在、数据在、**删除仍然生效**（WAL 重放收敛回来）"
    );
    assert!(deleted_card(&lakehouse, "public.t").await > 0, "目录里重建了 DV");
    shutdown.cancel();
}

/// 明确拒绝的形状（不猜用户想要什么）+ 只读形态的可读拒绝。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_refusals_are_explicit() {
    let base_guard = yuntun_testkit::TestDir::tmpfs("sqldelete-refuse");
    let base = base_guard.string();
    let cfg = config(&format!("{base}/wal"), &format!("{base}/store"), true);
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let _bg = lakehouse.spawn_background(&cfg);
    let mut client = serve(&lakehouse, shutdown.clone()).await;
    execute_update(&mut client, "CREATE TABLE t (v BIGINT NOT NULL)").await;
    execute_update(&mut client, "INSERT INTO t VALUES (1)").await;

    // 只读装配：DELETE 必须给**可读拒绝**（而不是看起来成功）
    let ro = SqlEngine::new_readonly(lakehouse.query.clone(), lakehouse.catalog.clone());
    let mut session = SessionCtx::default();
    match ro.execute("DELETE FROM t WHERE v = 1", &mut session).await {
        Err(yuntun_sql::SqlError::ReadOnly) => {}
        Err(other) => panic!("只读形态应当报 ReadOnly，实际：{other}"),
        Ok(_) => panic!("只读形态下的 DELETE 不许成功"),
    }

    // 形状不支持的一律明确拒绝（本刀的范围写在 `dml` 模块文档里）
    //
    // 注意：**无 `WHERE` 的全表删已经支持**（`F.3f`：设计 §7 的文件级下线，见
    // `sql_purge_e2e.rs`）—— 所以它不在下面这张"拒绝清单"里。
    for sql in [
        "DELETE t FROM t WHERE v = 1",          // 多表
        "DELETE FROM t USING s WHERE t.v = 1",  // USING
    ] {
        let msg = match lakehouse.sql.execute(sql, &mut session).await {
            Err(e) => e.to_string(),
            Ok(_) => panic!("`{sql}` 应当被拒绝"),
        };
        assert!(
            msg.contains("未") || msg.contains("拒绝") || msg.contains("parse"),
            "拒绝必须说清是什么没支持（`{sql}`）：{msg}"
        );
    }
    // 但数据还在（拒绝不留副作用）
    assert_eq!(
        col_v_until(&mut client, "SELECT v FROM t ORDER BY v", &[1]).await,
        vec![1]
    );

    // 【F.3d】合并正在跑（**表级 DML 租约**被占）时，DELETE **当场拒绝** ——
    // 两者必须在同一份基文件上串行：合并会消费 DV，交错会让已删的行复活（或把 DV 挂到墓碑上）。
    let dml_purpose = yuntun_model::meta::dml_lease_purpose("public.t");
    let held = lakehouse
        .catalog
        .acquire_lease(&dml_purpose, "someone-else", yuntun_ingest::now_ms(), 30_000)
        .await
        .unwrap();
    assert!(held.granted, "前提：这把租约现在被占着");
    let msg = match lakehouse
        .sql
        .execute("DELETE FROM t WHERE v = 1", &mut session)
        .await
    {
        Err(e) => e.to_string(),
        Ok(_) => panic!("合并在跑时 DELETE 不许成功"),
    };
    assert!(
        msg.contains("合并") && msg.contains("租约"),
        "拒绝必须点名「和谁冲突」：{msg}"
    );
    // 归还租约之后又能删了（拒绝只是"此刻不行"，不是"永久不行"）
    assert!(
        lakehouse
            .catalog
            .release_lease(&dml_purpose, "someone-else", held.epoch)
            .await
            .unwrap(),
        "归还必须成功（代次取自刚才那次授予）"
    );
    assert_eq!(
        execute_update(&mut client, "DELETE FROM t WHERE v = 1").await,
        1
    );
    shutdown.cancel();
}

/// **默认形态（`embedded` metanode + raft）也能删**（`F.3c-3`）。
///
/// 三条：
/// ① 登记走目录（`ApplyDeletions` op）—— 不是"某个进程的内存里删掉了"；
/// ② 删后立刻查不到（缓存刷新那一步在 SQL 层同步做了）；
/// ③ **读侧看到的也是目录里的那一份**：`deleted_card` 与查询结果都经
///    `RemoteCatalog::list_deletions`（它从缓存视图读，而缓存视图来自 `PrefetchPayload.deletions`）
///    —— 这正是**抵押①**要证的那件事："别的进程也看得见删除"。
///
/// > **为什么这条用例没有"重启"那一段**：fjall 的目录锁在**同一个测试进程内**上一次
/// > `Lakehouse` 被 drop 之后**不释放**（实测重试 5 秒仍然 `Locked`，见 `§151.3`）——
/// > 那是夹具的限制（产品形态下重启是**另一个进程**）。重启持久性由 `memory` 形态那条
/// > （靠 WAL 重放）覆盖；`embedded` 形态的"元数据落盘"是本仓既有性质（fjall）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_works_in_the_default_embedded_assembly() {
    let base_guard = yuntun_testkit::TestDir::tmpfs("sqldelete-embedded");
    let base = base_guard.string();
    let wal_dir = format!("{base}/wal");
    let store_root = format!("{base}/store");
    let meta_dir = format!("{base}/meta");
    let cfg = embedded_config(&wal_dir, &store_root, &meta_dir);

    // ============ 第一次运行 ============
    {
        let shutdown = CancellationToken::new();
        let lakehouse = build_embedded(&cfg, shutdown.clone()).await;
        let _bg = lakehouse.spawn_background(&cfg);
        let mut client = serve(&lakehouse, shutdown.clone()).await;
        execute_update(&mut client, "CREATE TABLE t (v BIGINT NOT NULL)").await;
        assert_eq!(
            execute_update(&mut client, "INSERT INTO t VALUES (1),(2),(3),(4)").await,
            4
        );
        assert_eq!(
            col_v_until(&mut client, "SELECT v FROM t ORDER BY v", &[1, 2, 3, 4]).await,
            vec![1, 2, 3, 4]
        );
        assert_eq!(
            execute_update(&mut client, "DELETE FROM t WHERE v IN (2, 4)").await,
            2,
            "默认形态下删除必须成功（不再是「尚未接线」）"
        );
        assert_eq!(col_v(&mut client, "SELECT v FROM t ORDER BY v").await, vec![1, 3]);
        // 目录里真的有了（内部事实：不是只有查询缓存知道）
        assert_eq!(deleted_card(&lakehouse, "public.t").await, 2);
        shutdown.cancel();
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

}
