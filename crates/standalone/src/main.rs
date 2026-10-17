//! yuntun Standalone 单机进程（计划任务书 v2.0 阶段 1；原 bins/all-in-one）。
//!
//! 单进程承载全部职责（Flight 写入 + WAL + 攒批 + S3 + Catalog + Compaction + Query 缓存），
//! 接口层已按 gRPC / 线性一致性语义设计，阶段 3 拆分服务时零业务改动。
//! 本 crate 是"全组件参考装配"，分布式阶段的独立 crate 全部复用此处已验证的组件。
//!
//! 用法：
//! ```text
//! yuntun --config yuntun.toml
//! yuntun --version
//! ```

use std::path::PathBuf;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// 启动参数（clap）。
///
/// `#[command(version)]` 直接接管 `--version`（打印 `yuntun <版本>`，与原手写行为一致，
/// 并额外支持 `-V`）；`--help` 由 clap 从本结构的 doc 注释生成 —— 手写的那套
/// "unknown arg + usage" 只在拼错时给一行含糊提示，clap 会指出**是哪个参数**。
#[derive(clap::Parser, Debug)]
#[command(
    name = "yuntun",
    version,
    about = "yuntun 单机进程（阶段 1；原 bins/all-in-one）"
)]
struct Args {
    /// 配置文件（TOML）；省略则用默认配置（本地 ./data + 内存 catalog）
    #[arg(long)]
    config: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // **控制面余量**（`§191`）：默认 `#[tokio::main]` 的 worker 数 = 核数，
    // 而 DataFusion 的并行度（`target_partitions`）也默认 = 核数 ⇒ 一条
    // CPU 打满的大查询会把**所有** worker 都占住，`SHOW PROCESSLIST` / `KILL`
    // 这些控制面命令要排队到查询结束（实测 11.5s 的查询让 `SELECT 1` 等了 10.9s，
    // 期间 `KILL` 根本递不进来 —— 那等于没有取消能力）。
    //
    // 多给 2 个 worker 是**便宜且有效**的：查询仍用满核数（不损吞吐），控制面
    // 有独立 OS 线程可调度。这不是"调优彩蛋"，而是 U4 的成立条件之一。
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        + 2;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?;
    rt.block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // ---- 参数（clap：help/version/用法错的文案与退出码全仓统一）----
    let config_path = <Args as clap::Parser>::parse().config;

    // ---- 日志 ----
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,yuntun=debug")),
        )
        .init();
    tracing::info!("yuntun starting, version={VERSION}");

    // ---- 配置 ----
    let cfg = match &config_path {
        Some(p) => yuntun_server::Config::from_path(p)?,
        None => {
            tracing::warn!(
                "no --config given, using defaults (local store ./data, meta = embedded metanode)"
            );
            let mut c = yuntun_server::Config::default();
            // **没有配置文件也要让元数据活过重启**：`Config::default()` 的
            // `[meta] dir` 是 None（= 进程内临时目录，那是给测试用的），
            // 生产默认必须落在一个稳定目录上，否则"重启即换了一个新集群"
            // —— 而 `[meta] dir` 省略时 `warnings()` 已经会吼，这里把它补上。
            if c.meta.dir.is_none() {
                c.meta.dir = Some(std::path::PathBuf::from("./data/meta"));
            }
            c
        }
    };
    tracing::info!(listen = %cfg.server.listen, "config loaded");

    // ---- 优雅关闭 ----
    let shutdown = tokio_util::sync::CancellationToken::new();
    {
        let token = shutdown.clone();
        tokio::spawn(async move {
            // **双信号**（`D-36`）：systemd / docker stop 发的是 **SIGTERM**，
            // 只处理 SIGINT 会让进程在"直接死"与"优雅关闭"之间**选前者** ——
            // WAL 不封口、后台任务不 join、WAL/spill 目录租约不释放
            // （下一次启动还要等锁）。两条信号共用同一份收尾流程。
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("install SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("SIGINT received, shutting down gracefully");
                }
                _ = term.recv() => {
                    tracing::info!("SIGTERM received, shutting down gracefully");
                }
            }
            token.cancel();
        });
    }

    // ---- 装配 + 后台任务 ----
    let lakehouse = yuntun_server::Lakehouse::build_with_shutdown(&cfg, shutdown.clone()).await?;
    let bg = lakehouse.spawn_background(&cfg);

    // ---- MySQL wire（:3306，配置 [sql.mysql]；bind 失败即启动报错）----
    let mysql = yuntun_server::spawn_mysql(&lakehouse, &cfg.sql.mysql).await?;

    // ---- 指标 HTTP（[metrics]；默认关，bind 失败即启动报错）----
    let metrics = yuntun_server::spawn_metrics(&lakehouse, &cfg.metrics).await?;

    // ---- Flight gRPC（阻塞至 shutdown）----
    let listen = cfg.server.listen.clone();
    yuntun_server::serve_flight(&lakehouse, &listen, cfg.server.auth_token.as_deref()).await?;

    // ---- 收尾 ----
    shutdown.cancel();
    if let Some(h) = mysql {
        let _ = h.await;
    }
    if let Some((addr, h)) = metrics {
        // 与 mysql 同款：**能 join 的任务都要 join**（`§159` 的教训）
        let _ = h.await;
        tracing::info!(%addr, "metrics http stopped");
    }
    for h in bg {
        let _ = h.await;
    }
    tracing::info!("yuntun stopped");
    Ok(())
}
