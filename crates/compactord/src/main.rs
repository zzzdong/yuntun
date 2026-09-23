//! `yuntun-compactord` —— **压缩节点进程**（R4 T12.1 第三刀）。
//!
//! ## 它为什么是最"正交"的那个进程
//!
//! 压缩只做两件事：**读**共享对象存储里的数据文件、**写**合并产物，然后把这次合并
//! 作为一条 op 提交到元数据面（`commit_compaction` → raft）。它：
//!
//! - **不持有**热数据（没有 WAL、没有 chunk、没有私有目录）；
//! - **不接受**客户端写入；
//! - 不需要知道有哪些数据节点（名录与它无关）。
//!
//! 于是"压缩"从一个**跟着某个节点跑的副作用**变成了一个**可独立扩缩的角色** ——
//! 这正是 `plan.md` 里"压缩与 Catalog 同进程只是部署事实"那句话的兑现。
//!
//! ## 部署不变量（必须写下来，因为没有代码强制它）
//!
//! **一个集群只应有一个 compactor（或至少：同一个 shard 不被两个 compactor 同时合并）。**
//! `commit_compaction` 是幂等的 op，但**"读文件 → 合并 → 提交"这段窗口没有租约保护**：
//! 两个 compactor 同时合并同一个 shard，会各自产出一份合并文件、并各自把老文件标删 ——
//! 行数不会错（老文件只被删一次，删除本身幂等），但会**多出一份孤儿产物**，
//! 由孤儿清理在静置期后回收。这条留作后续（要么加 shard 级租约，要么让归属方负责压缩）。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio_util::sync::CancellationToken;

use yuntun_catalog::CatalogOps;
use yuntun_meta::RemoteCatalog;
use yuntun_store::{StoreConfig, create_store};

#[derive(Parser, Debug)]
#[command(
    name = "yuntun-compactord",
    version,
    about = "压缩节点：合并共享对象存储里的数据文件，把合并结果提交到元数据面"
)]
struct Args {
    /// metanode 地址（如 `127.0.0.1:50051`）。合并提交走它的 raft。
    #[arg(long)]
    meta: String,
    /// 冷数据根目录（必须与数据节点写的是**同一份共享存储**）
    #[arg(long, default_value = "./data/cold")]
    cold_root: PathBuf,
    /// 同一 shard 触发合并的最少文件数
    #[arg(long, default_value_t = 5)]
    min_files: usize,
    /// 压缩作业间隔（秒）
    #[arg(long, default_value_t = 60)]
    interval_secs: u64,
    /// 数据格式（`parquet` / `vortex`）—— 必须与写入侧一致
    #[arg(long, default_value = "parquet")]
    format: String,
    /// 孤儿文件静置期（秒）：早于它、又不在目录里的产物才会被删
    #[arg(long, default_value_t = 3600)]
    gc_grace_secs: u64,
    /// 孤儿对账间隔（秒）
    #[arg(long, default_value_t = 60)]
    gc_interval_secs: u64,
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

    // ① 元数据面：压缩的提交（`commit_compaction`）与对账（`known_batch_ids`）都走这里。
    //    只读 + 一种写（提交合并）—— 它不需要任何本地目录，所以没有租约、没有 WAL。
    let catalog: Arc<dyn CatalogOps> =
        Arc::new(RemoteCatalog::connect(vec![args.meta.clone()])?);

    // ② 共享对象存储
    let store = create_store(&StoreConfig::Local {
        root: args.cold_root.to_string_lossy().into_owned(),
    })?;

    let shutdown = CancellationToken::new();

    // ③ 压缩循环：按 shard 找文件够多的表，合并并提交
    let compactor = Arc::new(yuntun_compaction::Compactor {
        cfg: yuntun_compaction::CompactionConfig {
            min_files: args.min_files,
            interval: Duration::from_secs(args.interval_secs),
            orphan_grace: Duration::from_secs(args.gc_grace_secs),
            ..Default::default()
        },
        catalog: catalog.clone(),
        store: store.clone(),
        format: yuntun_format::DataFormat::parse(&args.format),
    });
    let _compaction = yuntun_compaction::spawn_compaction_loop(compactor, shutdown.clone());

    // ④ 孤儿清理（§9.1）：S3 ↔ Meta 对账 + 静置期。
    //    ⚠️ 它**只删"不在目录里且已过静置期"的产物** —— 所以单靠它并不能替代上面的部署不变量。
    let _gc = yuntun_compaction::spawn_orphan_cleanup_with_interval(
        store,
        catalog,
        "yuntun/".to_string(),
        Duration::from_secs(args.gc_grace_secs),
        Duration::from_secs(args.gc_interval_secs),
        shutdown.clone(),
    );

    // 这一行是**接口**（不是日志）：编排/测试靠它确认"装配完成、开始干活"。
    // 压缩节点没有监听端口，所以用 READY 而不是 LISTEN。改格式等于破坏调用方。
    println!(
        "READY meta={} cold_root={} min_files={} interval_secs={}",
        args.meta,
        args.cold_root.display(),
        args.min_files,
        args.interval_secs
    );
    tracing::info!(
        meta = %args.meta,
        cold_root = %args.cold_root.display(),
        min_files = args.min_files,
        "compactord up"
    );

    // 一直跑到被停掉：两个循环都在后台，这里只是把进程吊住
    shutdown.cancelled().await;
    Ok(())
}
