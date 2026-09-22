//! `yuntun-ingestor` —— **数据节点进程**（R4 T12.1 第一刀）。
//!
//! ## 它是什么
//!
//! 单进程形态下"吸收 WAL / 持有热数据 / 对外提供热读"三件事都挤在 `standalone` 里，
//! 于是热读只能是**进程内函数调用**（装配层把 `Arc<ChunkStore>` 直接交给查询侧）。
//! 本二进制把**数据节点**摘出来：它独占自己的私有目录（WAL + spill），吸收自己的 WAL
//! 得到热数据，并把热数据经数据面 gRPC（`yuntun-shardrpc`）对外提供。
//!
//! ## 与单进程形态**不是两套实现**
//!
//! 用的是同一批组件与同一条契约：
//!
//! - `Ingestor::new` **自带** chunk store —— 写侧与热读侧是同一实例，就是"读己之写"；
//! - `private_dir` 租约（T12.4/T12.5）：**被拒的进程不会先动盘**（`operation-log §28.2` 的
//!   R-13 教训 —— 同一私有目录同一时刻只能有一个消费者）；
//! - `ShardReader`：服务端**只转发**本地的水位/STALE 判断，不重算（`§67`）。
//!
//! 差别只在**传输**：查询侧从"拿到 `Arc<ChunkStore>`"变成"拿到 `GrpcShardFetch` 包出来的
//! `RemoteShard`"。所以 `§66`/`§67` 那条对拍逻辑**一行都不用改**就能复用（见集成用例）。
//!
//! ## 第一刀的边界（记在 `§68`）
//!
//! - 元数据面仍用**本地** `MemoryCatalog`（接 metanode 属 T12.3）；
//! - 没有 WAL 超时监控 / compaction / 孤儿清理 —— 那些属**别的**进程（`compactor`）；
//! - 写入面（客户端如何把数据交给它）**尚未定**：本刀由"它自己的 WAL"喂
//!   （回放 = 崩溃恢复路径，最诚实的一种：数据节点重启后热数据必须自己长回来）。
//!
//! ## 关键约束：先占租约、再动盘
//!
//! 租约检查放在**所有会改文件的操作之前** —— 更要紧的是它排在 WAL 打开之前，
//! 于是"被拒的那个进程"不会碰别人正在用的目录。

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_ingest::{Ingestor, IngestorConfig};
use yuntun_model::private_dir::{self, DirOwner};
use yuntun_store::{StoreConfig, create_store};
use yuntun_wal::config::WalConfig;
use yuntun_wal::writer::WalWriter;

#[derive(Parser, Debug)]
#[command(
    name = "yuntun-ingestor",
    version,
    about = "数据节点：吸收自己的 WAL、持有热数据、对外提供热读"
)]
struct Args {
    /// 实例标识（= `FileManifest.source_instance`；热数据按它归属，`§65`）
    #[arg(long)]
    instance_id: String,
    /// 私有数据根目录（其下 `wal/` + `spill/` + `cold/` 各自独立）
    #[arg(long)]
    dir: PathBuf,
    /// 热读服务监听地址；`127.0.0.1:0` = 内核分配，真实地址打印到 stdout（供上层发现）
    #[arg(long, default_value = "127.0.0.1:0")]
    listen: String,
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

    let wal_root = args.dir.join("wal");
    let spill_dir = args.dir.join("spill");
    let cold_root = args.dir.join("cold");
    for d in [&wal_root, &spill_dir, &cold_root] {
        std::fs::create_dir_all(d)?;
    }

    // ① 私有目录租约 —— **排在最前**：被拒的进程连 WAL 都不该打开。
    //    （错误信息会点名 instance_id / role / pid，见 `§62`）
    let _wal_lease = private_dir::acquire(
        &wal_root,
        DirOwner {
            instance_id: args.instance_id.clone(),
            role: "wal-root".into(),
        },
    )?;
    let _spill_lease = private_dir::acquire(
        &spill_dir,
        DirOwner {
            instance_id: args.instance_id.clone(),
            role: "chunk-spill".into(),
        },
    )?;

    // ② WAL：数据节点热数据的真相来源（打开时自带 recovery，`synced_seq` 从盘上恢复）
    let wal = WalWriter::open(
        WalConfig {
            dir: wal_root.clone(),
            ..Default::default()
        },
        0,
    )
    .await?;

    // ③ 元数据面 + 冷存根
    //    （本地内存 catalog：T12.3 接 metanode；冷文件先落本机目录）
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    let store = create_store(&StoreConfig::Local {
        root: cold_root.to_string_lossy().into_owned(),
    })?;

    // ④ 吸收循环：`Ingestor::new` 自带 chunk store ⇒ 写侧与热读侧同一实例
    let ingestor = Arc::new(Ingestor::new(
        IngestorConfig {
            instance_id: args.instance_id.clone(),
            spill_dir: spill_dir.clone(),
            ..Default::default()
        },
        wal,
        catalog,
        store,
    ));
    let shutdown = CancellationToken::new();
    let _accumulator = ingestor.clone().spawn_accumulator(shutdown.clone());

    // ⑤ 热读服务：本地 chunk store 作为 `ShardReader` 暴露。
    //    先 bind 再打印 ⇒ 上层拿到的是**真实**地址（`127.0.0.1:0` 的端口由内核分配）
    let listener = TcpListener::bind(&args.listen).await?;
    let addr = listener.local_addr()?;
    println!("LISTEN {addr}");
    tracing::info!(
        %addr,
        instance_id = %args.instance_id,
        dir = %args.dir.display(),
        "ingestor serving hot shards"
    );

    yuntun_shardrpc::serve(ingestor.chunks(), listener).await?;
    Ok(())
}
