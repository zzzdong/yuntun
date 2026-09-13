//! 写后可见性回归（本文件覆盖两个 0.1 blocker）：
//!
//! 1. **读己之写**：默认攒批 jitter（≤60s）与查询缓存 TTL（30s）下，写入 fsync 后
//!    应立刻可从内存视图查到 —— 而不是等 flush/缓存刷新（此前最坏 ~48s "写了查不到"）。
//! 2. **DROP 语义**：DROP 掉的数据不得在"重启重放 + 重建同名表"后复活（此前会复活）。

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

use arrow::array::Int64Array;
use yuntun_catalog::CatalogOps;
use yuntun_server::Lakehouse;
use yuntun_sql::session::SessionCtx;
use yuntun_sql::SqlResult;

fn sec_of_minute() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        % 60
}

fn config(base: &str, rows_threshold: u64, cache_ttl_secs: u64) -> yuntun_server::Config {
    yuntun_server::Config::from_toml(&format!(
        r#"
[store]
type = "local"
root = "{base}/store"

[wal]
dir = "{base}/wal"

[query]
cache_ttl_secs = {cache_ttl_secs}

[ingest]
rows_threshold = {rows_threshold}
time_threshold_secs = 5
flush_jitter_secs = 60
scan_interval_ms = 20
"#
    ))
    .unwrap()
}

async fn exec(lakehouse: &Lakehouse, sql: &str) -> SqlResult {
    let mut session = SessionCtx::default();
    lakehouse.sql.execute(sql, &mut session).await.unwrap()
}

async fn count(lakehouse: &Lakehouse, table: &str) -> i64 {
    match exec(lakehouse, &format!("SELECT count(*) AS c FROM {table}")).await {
        SqlResult::Rows { batches, .. } => {
            if batches.is_empty() || batches[0].num_rows() == 0 {
                0
            } else {
                batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            }
        }
        _ => panic!("expected rows"),
    }
}

/// 已提交（Manifest）可见文件数：0 表示"还没落盘"，即查询只能靠内存视图。
async fn committed_files(lakehouse: &Lakehouse, table: &str) -> usize {
    let snap = lakehouse.catalog.current_snapshot().await;
    lakehouse
        .catalog
        .list_visible_files(table, snap, None)
        .await
        .unwrap()
        .len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_your_writes_visible_before_flush() {
    let base_guard = yuntun_testkit::TestDir::tmpfs("write-then-read-ryw");
    let base = base_guard.string();
    // rows_threshold 默认（不触发行数阈值）+ jitter 60s + **缓存 TTL 默认 30s**：
    // 数据只可能来自"未落盘内存视图"
    let cfg = config(&base, 10_000, 30);
    let shutdown = CancellationToken::new();
    let lh = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let bg = lh.spawn_background(&cfg);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 表名 f：`public.f` 的 jitter 偏移 = 59s，给测试留足"flush 尚未发生"的窗口
    exec(&lh, "CREATE TABLE f (a BIGINT)").await;
    while sec_of_minute() >= 52 {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let t0 = std::time::Instant::now();
    exec(&lh, "INSERT INTO f VALUES (1)").await;

    let mut latency = None;
    for _ in 0..30 {
        if count(&lh, "f").await == 1 {
            latency = Some(t0.elapsed().as_secs_f64());
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let latency = latency.expect("写入后应立刻可查（读己之写）");
    let files = committed_files(&lh, "public.f").await;
    eprintln!("read-your-writes: latency={latency:.3}s committed_files={files}");
    assert!(
        files == 0,
        "本用例要求 flush 尚未发生（committed_files={files}）——否则测不出内存可见性"
    );

    shutdown.cancel();
    for h in bg {
        let _ = h.await;
    }
    let _ = std::fs::remove_dir_all(&base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_table_not_resurrected_by_same_name_create() {
    let base_guard = yuntun_testkit::TestDir::tmpfs("write-then-read-drop");
    let base = base_guard.string();
    // rows_threshold=1 → 攒批组立刻可 flush，最大化"重放旧 Data 并通过 flush 复活"的机会
    let cfg = config(&base, 1, 1);

    // ============ 第一轮：建表 + 写入 + DROP（WAL 留下 Data + Ddl(DROP)）============
    let shutdown = CancellationToken::new();
    let lh = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let bg = lh.spawn_background(&cfg);
    exec(&lh, "CREATE TABLE t (a BIGINT)").await;
    exec(&lh, "INSERT INTO t VALUES (42)").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(count(&lh, "t").await, 1, "第一轮写入可见");
    exec(&lh, "DROP TABLE t").await;
    shutdown.cancel();
    for h in bg {
        let _ = h.await;
    }
    drop(lh);

    // ============ 第二轮：重启 —— 攒批线程重读 WAL 会看到已 DROP 表的 Data ============
    let shutdown = CancellationToken::new();
    let lh = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let bg = lh.spawn_background(&cfg);
    tokio::time::sleep(Duration::from_millis(600)).await;

    let tables: Vec<String> = lh
        .catalog
        .list_tables()
        .await
        .unwrap()
        .iter()
        .map(|t| t.qualified_name())
        .collect();
    assert!(tables.is_empty(), "DROP 后重启不应有表: {tables:?}");
    assert_eq!(
        committed_files(&lh, "public.t").await,
        0,
        "已 DROP 表的数据不得留下悬挂 Manifest"
    );

    // ============ 重建同名表：旧数据必须不可见 ============
    exec(&lh, "CREATE TABLE t (a BIGINT)").await;
    let c = count(&lh, "t").await;
    eprintln!("重建同名表后 count(*) = {c}（应为 0）");
    assert_eq!(c, 0, "DROP 掉的数据不得在重建同名表后复活");

    shutdown.cancel();
    for h in bg {
        let _ = h.await;
    }
    let _ = std::fs::remove_dir_all(&base);
}
