//! 观测能力回归（`plan.md` T6.12）：**故障现场三件套**必须可读。
//!
//! 这三项是压力类故障唯一能定位的抓手：
//! - **chunk 内存水位**：内存为什么涨；
//! - **WAL 积压**：攒批是不是落后了（内存越限的缓冲池）；
//! - **背压水位**：离"拒写"还有多远；
//! 再叠加 **Catalog 版本 + 全量/增量刷新计数**（R2 的成效可量化）。

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use yuntun_server::Lakehouse;
use yuntun_sql::session::SessionCtx;
use yuntun_sql::SqlResult;

fn config(base: &str) -> yuntun_server::Config {
    yuntun_server::Config::from_toml(&format!(
        r#"
[store]
type = "memory"

[wal]
dir = "{base}/wal"

[chunk]
mem_budget_mb = 16
query_mem_budget_mb = 8

[ingest]
rows_threshold = 1
time_threshold_secs = 0
max_flush_delay_secs = 3600
scan_interval_ms = 20
"#
    ))
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_expose_memory_backlog_pressure_and_catalog_versions() {
    let dir = yuntun_testkit::TestDir::tmpfs("metrics-e2e");
    let cfg = config(&dir.string());
    let shutdown = CancellationToken::new();
    let lh = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let bg = lh.spawn_background(&cfg);
    tokio::time::sleep(Duration::from_millis(150)).await;

    // 写入一点数据（走 SQL 写入路径，等同于真实客户端）
    let mut session = SessionCtx::default();
    lh.sql
        .execute("CREATE TABLE cpu (ts BIGINT, usage DOUBLE)", &mut session)
        .await
        .unwrap();
    let r = lh
        .sql
        .execute("INSERT INTO cpu VALUES (1, 0.5), (2, 0.6)", &mut session)
        .await
        .unwrap();
    assert!(matches!(r, SqlResult::Affected(2)));

    // 等**积压排空**（而不是"吸收过一次"）：WAL 里既有 DDL 记录也有 Data 记录，
    // 只看到第一条被吸收就断言，会读到真实存在的积压（这就是指标本身的含义）。
    let mut m = lh.metrics().await;
    for _ in 0..100 {
        if m.wal.backlog_records == 0 && m.chunk.chunks >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        m = lh.metrics().await;
    }

    // ① 写入侧水位：WAL 已 fsync，且积压已被攒批线程吸收干净
    assert!(m.wal.synced_seq >= 1, "写入应已 fsync: {m:?}");
    assert_eq!(m.wal.backlog_records, 0, "吸收完成后不应有积压: {m:?}");
    assert!(m.wal.absorbed_seq > m.wal.synced_seq, "吸收位置应越过 synced: {m:?}");
    assert!(m.wal.dir_bytes > 0, "WAL 目录应有字节占用");

    // ② chunk 内存水位：可见性承诺优先，未落盘数据必须驻留
    assert!(m.chunk.budget_bytes == 16 * 1024 * 1024, "预算来自配置");
    assert!(m.chunk.chunks >= 1, "应有 chunk（max_flush_delay 很长，不落盘）");
    assert!(m.chunk.resident_bytes > 0, "内存账本应非零");
    assert!(
        m.chunk.pressure_ratio >= 0.0 && m.chunk.pressure_ratio < 1.0,
        "水位应可读且未到拒写: {m:?}"
    );

    // ③ 背压水位可读（字符串形式便于日志/告警解析）
    assert!(!m.chunk.pressure.is_empty());

    // ④ Catalog 版本与刷新形态：schema 已建表、刷新走全量至少一次
    assert!(m.catalog.schema_ver >= 1, "建表推 schema_ver: {m:?}");
    assert!(m.catalog.tables >= 1, "本地视图应有表: {m:?}");
    assert!(m.catalog.refreshes >= 1);
    assert!(m.catalog.full_reloads >= 1);
    assert!(m.catalog.last_error.is_none(), "刷新不应有错误: {m:?}");

    // ⑤ query 执行区上限来自配置（硬分区，另一块独立预算）
    assert_eq!(m.query.limit_bytes, Some(8 * 1024 * 1024));

    // ⑥ 指标可序列化（日志/将来接 HTTP/监控系统的前提）
    let json = serde_json::to_string(&m).unwrap();
    assert!(json.contains("\"backlog_records\""), "{json}");
    assert!(json.contains("\"pressure\""), "{json}");

    shutdown.cancel();
    for h in bg {
        let _ = h.await;
    }
    let _ = std::fs::remove_dir_all(&dir.string());
}
