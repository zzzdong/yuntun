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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // ---- 参数（cmd 入口规范：flag 包）----
    let mut config_path: Option<PathBuf> = None;
    let mut show_version = false;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(args.next().unwrap_or_default()));
            }
            "--version" => show_version = true,
            other => {
                eprintln!("unknown arg: {other}\nusage: yuntun [--config <file>] [--version]");
                std::process::exit(2);
            }
        }
    }
    if show_version {
        println!("yuntun {VERSION}");
        return Ok(());
    }

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
                "no --config given, using defaults (local store ./data, memory catalog)"
            );
            yuntun_server::Config::default()
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
