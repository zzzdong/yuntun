//! **整机对拍**（`operation-log §56.4` ①）：同一条操作序列分别跑在
//! `[meta] mode = "memory"` 与 `"embedded"` 上，比较**经过 SQL → catalog → ingest → WAL → store
//! 之后**的可观测结果。
//!
//! # 为什么它比组件级对拍（`§55`）强
//!
//! `§55` 比的是 `CatalogOps` 的行为；这里比的是**整机**：SQL 解析 → DDL/DML 分派 →
//! 攒批 → flush 落盘 → 元数据提交 —— 这一整条链在两种装配下必须给出**同样的结果**。
//! 这才是"切装配点不回归"的真正口径，也是 **R4**「与单节点串行精确相等」那条判据的现成模板。
//!
//! # 刻意**不比**的东西
//!
//! 时间戳（`created_at`/`committed_at`）、路径（UUID/时间戳命名的文件名）、
//! raft 坐标（`revision`/`commit_index`）—— 它们本就该不同，拉进来只会得到噪声断言。

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use yuntun_catalog::CatalogOps;
use yuntun_server::{Config, Lakehouse};
use yuntun_sql::session::SessionCtx;

/// 整机可观测结果（**只放两种装配都该一致的东西**）。
#[derive(Debug, PartialEq)]
struct Observed {
    schemas: Vec<String>,
    /// (全限定名, schema 版本, 字段数)
    tables: Vec<(String, u64, usize)>,
    version: (u64, u64),
    /// 每表：(可见文件数, 可见文件里的行数之和) —— 证明 ingest→flush→提交 这条链走通了
    files: BTreeMap<String, (usize, u64)>,
    /// `created_at` 是否都在**秒级**（`< 10^11` ≈ 公元 5138 年）
    ///
    /// 不比较具体值（两次运行本就该不同），只比**单位** —— 单位不一致是 1000× 的静默错误：
    /// 内存路径传 `now_secs()`、op 路径传 `now_ms` 时，两边差 1000 倍而**没有任何报错**
    /// （整机对拍抓出来的第一件事，`§58`）。
    created_at_is_seconds: Vec<bool>,
}

fn temp_base(tag: &str) -> std::path::PathBuf {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!(
        "yuntun-assembly-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&p).expect("建临时目录");
    p
}

fn config(embedded: bool, base: &Path) -> Config {
    let mut cfg = Config::default();
    // 两种形态只有**这一处**不同 —— 这正是"装配层开关"的意思
    cfg.meta.mode = if embedded {
        yuntun_server::config::MetaMode::Embedded
    } else {
        yuntun_server::config::MetaMode::Memory
    };
    cfg.meta.dir = Some(base.join("meta"));
    cfg.store = yuntun_server::config::StoreSection::Local {
        root: base.join("store"),
    };
    cfg.wal.dir = base.join("wal");
    // 尽快 seal（与既有 e2e 同款）：对拍要的是"能落盘"，不是吞吐
    cfg.ingest.rows_threshold = 1;
    cfg.ingest.max_flush_delay_secs = 1;
    cfg.ingest.flush_phase_spread_secs = 0;
    cfg.ingest.scan_interval_ms = 20;
    cfg
}

async fn observe(lh: &Lakehouse) -> Observed {
    let metas = lh.catalog.list_tables().await.expect("list_tables");
    let mut tables: Vec<(String, u64, usize)> = metas
        .iter()
        .map(|m| {
            (
                m.qualified_name(),
                m.current_schema_version,
                m.schema().map(|s| s.fields().len()).unwrap_or(0),
            )
        })
        .collect();
    tables.sort();

    let mut files = BTreeMap::new();
    for m in &metas {
        let f = lh
            .catalog
            .list_visible_files(&m.qualified_name(), u64::MAX, None)
            .await
            .expect("list_visible_files");
        let rows: u64 = f.iter().map(|x| x.row_count).sum();
        files.insert(m.qualified_name(), (f.len(), rows));
    }

    let v = lh.catalog.version().await;
    Observed {
        schemas: lh.catalog.list_schemas().await.expect("list_schemas"),
        tables,
        version: (v.schema_ver, v.manifest_ver),
        files,
        created_at_is_seconds: metas.iter().map(|m| m.created_at < 100_000_000_000).collect(),
    }
}

/// 跑一遍整机序列：建表 → 插数据 → 等落盘 → 观测。
async fn run_once(embedded: bool) -> Observed {
    let base = temp_base(if embedded { "emb" } else { "mem" });
    let cfg = config(embedded, &base);
    let shutdown = CancellationToken::new();
    let lh = Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
        .await
        .expect("装配应当成功");
    let _bg = lh.spawn_background(&cfg); // 持有后台任务句柄（drop 会取消它们）

    let mut session = SessionCtx::mysql();
    // DDL：走 SQL 层（General 模板 → 表自带 ingest 配置）
    lh.sql
        .execute(
            "CREATE TABLE events (ts BIGINT NOT NULL, evt VARCHAR)",
            &mut session,
        )
        .await
        .expect("CREATE TABLE");
    // DML：显式幂等键（General 模板强制要求）
    lh.sql
        .execute_with_key(
            "INSERT INTO events (ts, evt) VALUES (1, 'a'), (2, 'b')",
            &mut session,
            Some("k-parity-1".into()),
        )
        .await
        .expect("INSERT");

    // 等 flush → commit（幂等键 → 文件清单出现即说明整条链走完）
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let f = lh
            .catalog
            .list_visible_files("public.events", u64::MAX, None)
            .await
            .expect("list_visible_files");
        if !f.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "30s 内没看到已提交的文件（flush/commit 链断了）"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drop(shutdown);
    observe(&lh).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_and_embedded_assemblies_agree_end_to_end() {
    let memory = run_once(false).await;
    let embedded = run_once(true).await;
    assert_eq!(
        memory, embedded,
        "两种装配（memory vs embedded metanode）在整机序列后的可观测结果必须一致"
    );
    // 顺带确认这次对拍**确实**经过了写入路径（否则两边都是空的，对拍毫无意义）
    assert_eq!(
        memory.files.get("public.events").map(|(n, _)| *n),
        Some(1),
        "应当恰好一个已提交文件：{memory:?}"
    );
    assert_eq!(
        memory.files.get("public.events").map(|(_, rows)| *rows),
        Some(2),
        "两行数据必须落进可见文件：{memory:?}"
    );
    assert_eq!(
        memory.created_at_is_seconds,
        vec![true],
        "`created_at` 必须是**秒级**（内存路径与 op 路径的单位必须一致）：{memory:?}"
    );
}
