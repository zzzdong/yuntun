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
#[command(name = "yuntun", version, about = "yuntun 单机进程（阶段 1；原 bins/all-in-one）")]
struct Args {
    /// 配置文件（TOML）；省略则用默认配置（本地 ./data + 内存 catalog）
    #[arg(long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("SIGINT received, shutting down gracefully");
            token.cancel();
        });
    }

    // ---- 装配 + 后台任务 ----
    let lakehouse = yuntun_server::Lakehouse::build_with_shutdown(&cfg, shutdown.clone()).await?;
    let bg = lakehouse.spawn_background(&cfg);

    // ---- MySQL wire（:3306，配置 [sql.mysql]；bind 失败即启动报错）----
    let mysql = yuntun_server::spawn_mysql(&lakehouse, &cfg.sql.mysql).await?;

    // ---- Flight gRPC（阻塞至 shutdown）----
    let listen = cfg.server.listen.clone();
    yuntun_server::serve_flight(&lakehouse, &listen).await?;

    // ---- 收尾 ----
    shutdown.cancel();
    if let Some(h) = mysql {
        let _ = h.await;
    }
    for h in bg {
        let _ = h.await;
    }
    tracing::info!("yuntun stopped");
    Ok(())
}
