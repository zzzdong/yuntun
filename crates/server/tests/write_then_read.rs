//! 写后可见性回归（本文件覆盖两个 0.1 blocker）：
//!
//! 1. **读己之写**：持久化上界（`max_flush_delay_secs`）与查询缓存 TTL 都很长时，
//!    写入 fsync 后应立刻可从 chunk 读到 —— 而不是等 flush / 缓存刷新
//!    （架构 §5.2：**可见性上界绑 WAL fsync，与持久化上界分离**）。
//! 2. **DROP 语义**：DROP 掉的数据不得在"重启重放 + 重建同名表"后复活。

use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use arrow::array::Int64Array;
use yuntun_server::Lakehouse;
use yuntun_sql::session::SessionCtx;
use yuntun_sql::SqlResult;

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
# 持久化上界设为 1 小时：本文件的两个用例都要求"flush 尚未发生"，
# 从而证明可见性来自 chunk 而非落盘文件（架构 §5.2 双阈值分离）
max_flush_delay_secs = 3600
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
    // rows_threshold 10_000（不触发行数阈值）+ max_flush_delay 1h + 缓存 TTL 30s：
    // 数据只可能来自 chunk（未落盘热数据），不可能是 Manifest
    let cfg = config(&base, 10_000, 30);
    let shutdown = CancellationToken::new();
    let lh = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let bg = lh.spawn_background(&cfg);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 无需再等 jitter 窗口：持久化上界已是 1h，本用例期间 flush 不会发生
    exec(&lh, "CREATE TABLE f (a BIGINT)").await;
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
