//! `yuntun-queryd` —— **查询节点进程**的薄壳：解析参数 → `yuntun_queryd::start` → 打印真实地址。
//!
//! 只读语义见 lib 的文档：SQL 侧对写语句回 `ReadOnly`，Flight 侧对 `DoPut` 回
//! `failed_precondition` —— 两条路径都给**可读的拒绝**。

use std::path::PathBuf;

use clap::Parser;

use yuntun_queryd::{QuerydConfig, start};

#[derive(Parser, Debug)]
#[command(
    name = "yuntun-queryd",
    version,
    about = "查询节点：只读元数据面 + 按名录拉各数据节点的热数据"
)]
struct Args {
    /// metanode 地址（如 `127.0.0.1:50051`）。查询节点**必须**有它：元数据全来自那里。
    #[arg(long)]
    meta: String,
    /// Flight SQL 监听地址；`127.0.0.1:0` = 内核分配，真实地址打印到 stdout
    #[arg(long, default_value = "127.0.0.1:0")]
    listen: String,
    /// 冷数据根目录（必须与数据节点写的是**同一份共享存储**）
    #[arg(long, default_value = "./data/cold")]
    cold_root: PathBuf,
    /// 名录巡检间隔（秒）：发现新数据节点，以及被摘除的成员
    #[arg(long, default_value_t = 5)]
    reconcile_secs: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = <Args as Parser>::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let (addr, serving) = start(QuerydConfig {
        meta: args.meta.clone(),
        listen: args.listen.clone(),
        cold_root: args.cold_root.clone(),
        reconcile_secs: args.reconcile_secs,
    })
    .await?;

    // 这一行是**接口**（不是日志）：编排/测试靠它拿真实地址（`--listen 127.0.0.1:0` 时
    // 端口由内核分配）。改格式等于破坏调用方，别随手改。
    println!("LISTEN {addr}");
    tracing::info!(%addr, meta = %args.meta, cold_root = %args.cold_root.display(), "queryd up");

    serving.await?;
    Ok(())
}
