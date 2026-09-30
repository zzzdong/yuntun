//! **台账 `D-11` 闭环：默认（`embedded`）形态的"重启后删除还在"**（`operation-log §159`）。
//!
//! 缺口是什么：`[meta] mode = "memory"` 形态的重启持久性有端到端用例（靠数据 WAL 重放），
//! 而**默认形态**（`embedded`：目录落 fjall）只有"结构性证据"（fjall 落盘 + 读侧走目录）。
//! 想做同进程重启却撞上一个**易被误诊**的现象：`FjallError: Locked` —— 看起来像"fjall 没落盘"，
//! 其实是**目录锁还没放**：fjall 的锁由 `FjallStorage` 里那份 `Database` 持有，而
//! **每个 `NodeHandle` 都克隆了一份 `FjallStorage`** ⇒ 只有"停线程 + 把整个装配（含句柄）丢掉"
//! 之后锁才释放。
//!
//! 本用例因此干两件事：
//!
//! 1. **给出可复制的停机配方**：`cancel → join 全部后台任务 → drop（Lakehouse）`，
//!    然后**重开必须一次成功**（不做"看到 Locked 就重试"的兜底 —— 那会把锁残留**藏起来**，
//!    这个用例的价值恰恰在于不允许藏）；
//! 2. **把证据来源钉死**：第二次运行之前**把数据 WAL 目录挪走** ⇒
//!    状态不可能来自"重放数据 WAL"，只能来自 **fjall**（这正是 `D-11` 缺的那半证据）。
//!
//! 顺带验两条"重启之后还得是个能用的库"：删除向量**逐字段**还在（`card` 对得上）、
//! 而且新实例能继续写入。

use tokio_util::sync::CancellationToken;

use arrow::array::Int64Array;
use yuntun_server::Lakehouse;
use yuntun_sql::SqlResult;
use yuntun_sql::session::SessionCtx;

// ---------------------------------------------------------------- 夹具

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

/// 执行一条语句，返回是否成功（写路径用它）。
async fn run(lakehouse: &Lakehouse, session: &mut SessionCtx, sql: &str) -> i64 {
    match lakehouse.sql.execute(sql, session).await {
        Ok(SqlResult::Affected(n)) => n,
        Ok(SqlResult::Rows { batches, .. }) => batches.iter().map(|b| b.num_rows() as i64).sum(),
        Err(e) => panic!("`{sql}` 失败：{e}"),
    }
}

/// `SELECT` 一列整数（**进程内**执行：不经过 Flight ⇒ 不留下任何持有目录句柄的任务）。
async fn col_v(lakehouse: &Lakehouse, session: &mut SessionCtx, sql: &str) -> Vec<i64> {
    match col_v_opt(lakehouse, session, sql).await {
        Some(v) => v,
        None => panic!("`{sql}` 失败"),
    }
}

/// 同 `col_v`，但**失败返回 `None`**：轮询时"冷缓存里还没有这张表"是正常中间态
/// （查询缓存的刷新 TTL 是 30s；写路径会主动刷，重启后的第一次读不一定有）。
async fn col_v_opt(
    lakehouse: &Lakehouse,
    session: &mut SessionCtx,
    sql: &str,
) -> Option<Vec<i64>> {
    match lakehouse.sql.execute(sql, session).await {
        Ok(SqlResult::Rows { batches, .. }) => {
            let mut out = Vec::new();
            for b in &batches {
                let a = b.column(0).as_any().downcast_ref::<Int64Array>()?;
                out.extend((0..a.len()).map(|i| a.value(i)));
            }
            Some(out)
        }
        Ok(SqlResult::Affected(_)) => None,
        Err(_) => None,
    }
}

/// 等到查询结果等于 `expect`（刚写入的行要等一个攒批扫描周期）。
async fn col_v_until(
    lakehouse: &Lakehouse,
    session: &mut SessionCtx,
    sql: &str,
    expect: &[i64],
) -> Vec<i64> {
    let mut last = Vec::new();
    for _ in 0..60 {
        if let Some(v) = col_v_opt(lakehouse, session, sql).await {
            last = v;
            if last == expect {
                return last;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    last
}

/// **优雅停机**（本用例要证明的那套配方）：取消 → **join 全部后台任务** → drop 装配。
///
/// 为什么要 join 而不是"睡一会儿"：后台任务各自握着目录句柄（`spawn_refresh(catalog.clone())`），
/// 它们没结束 ⇒ `FjallStorage` 的克隆还在 ⇒ 目录锁还在。join 之后才是**确定的**。
async fn graceful_stop(
    lakehouse: Lakehouse,
    bg: Vec<tokio::task::JoinHandle<()>>,
    shutdown: CancellationToken,
) {
    shutdown.cancel();
    for h in bg {
        h.await.expect("后台任务不该 panic");
    }
    drop(lakehouse);
}

// ---------------------------------------------------------------- 用例

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn embedded_mode_persists_dml_across_an_in_process_restart() {
    let guard = yuntun_testkit::TestDir::tmpfs("embedded-restart");
    let base = guard.string();
    let wal_dir = format!("{base}/wal");
    let store_root = format!("{base}/store");
    let meta_dir = format!("{base}/meta");
    let cfg = embedded_config(&wal_dir, &store_root, &meta_dir);

    // ============ 第一次运行 ============
    let mut session = SessionCtx::default();
    {
        let shutdown = CancellationToken::new();
        let lakehouse = Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap();
        let bg = lakehouse.spawn_background(&cfg);

        run(&lakehouse, &mut session, "CREATE TABLE t (v BIGINT NOT NULL)").await;
        assert_eq!(
            run(
                &lakehouse,
                &mut session,
                "INSERT INTO t VALUES (1),(2),(3),(4)"
            )
            .await,
            4
        );
        assert_eq!(
            col_v_until(&lakehouse, &mut session, "SELECT v FROM t ORDER BY v", &[1, 2, 3, 4]).await,
            vec![1, 2, 3, 4]
        );
        assert_eq!(
            run(&lakehouse, &mut session, "DELETE FROM t WHERE v IN (2, 4)").await,
            2
        );
        assert_eq!(
            col_v(&lakehouse, &mut session, "SELECT v FROM t ORDER BY v").await,
            vec![1, 3]
        );
        // 内部事实：删除向量真的登记了（不是只有查询缓存知道）
        let snap = lakehouse.catalog.current_snapshot().await;
        let dvs = lakehouse
            .catalog
            .list_deletions("public.t", snap)
            .await
            .unwrap();
        assert_eq!(dvs.len(), 1);
        assert_eq!(dvs[0].card, 2, "前提：删除向量覆盖 2 行");

        graceful_stop(lakehouse, bg, shutdown).await;
    }

    // ============ 把"数据 WAL"挪走：把证据来源钉死在 fjall 上 ============
    //
    // 不这么做的话，"重启后删除还在"也可能是 `replay_wal_dml` 把数据 WAL 重放出来的 ——
    // 那是 `memory` 形态已经在验的那条路（`§154`/`§157`），不是本用例要证的**落盘**。
    std::fs::rename(&wal_dir, format!("{wal_dir}.moved")).expect("挪走 WAL 目录");

    // ============ 第二次运行：**一次成功**（不许靠"Locked 就重试"把锁残留藏起来）============
    let shutdown = CancellationToken::new();
    let lakehouse = Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
        .await
        .expect(
            "重开必须一次成功：上面的停机配方若正确（含 join 掉 metanode 的 gRPC 服务任务），\
             fjall 的目录锁此时已经释放 —— 不许用「看到 Locked 就重试」把锁残留藏起来",
        );
    let bg = lakehouse.spawn_background(&cfg);
    // 重启后的第一次查询：**显式刷一次缓存**（与 `§154`/`§157` 的重启用例同款做法）——
    // 后台刷新的 TTL 是 30s，而这里要的是"新进程一上来就能读到落盘的状态"。
    lakehouse
        .query
        .catalog()
        .refresh(&(lakehouse.catalog.clone()))
        .await
        .expect("重启后刷新查询缓存");

    assert_eq!(
        col_v(&lakehouse, &mut session, "SELECT v FROM t ORDER BY v").await,
        vec![1, 3],
        "**重启之后删除仍然生效**（数据 WAL 已挪走 ⇒ 这个状态只能来自 fjall 落盘的目录）"
    );
    let snap = lakehouse.catalog.current_snapshot().await;
    let dvs = lakehouse
        .catalog
        .list_deletions("public.t", snap)
        .await
        .unwrap();
    assert_eq!(dvs.len(), 1, "删除向量逐字段落盘了（不是「恰好查询不到」）");
    assert_eq!(dvs[0].card, 2);
    assert_eq!(dvs[0].table, "public.t");
    assert!(
        !dvs[0].store_path.is_empty(),
        "DV 对象的路径也要落盘（读侧据此去取位图）：{:?}",
        dvs[0]
    );

    // 重启之后还得是个**能继续用的库**
    assert_eq!(
        run(&lakehouse, &mut session, "INSERT INTO t VALUES (5)").await,
        1
    );
    assert_eq!(
        col_v_until(&lakehouse, &mut session, "SELECT v FROM t ORDER BY v", &[1, 3, 5]).await,
        vec![1, 3, 5],
        "新实例继续写入，且已删的行（2/4）不会因为重启而回来"
    );

    graceful_stop(lakehouse, bg, shutdown).await;
}
