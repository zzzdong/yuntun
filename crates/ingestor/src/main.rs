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
    /// metanode 地址（如 `127.0.0.1:50051`）。给了就**入册**（注册走 raft）并起**心跳保活**；
    /// 不给则只在本进程内登记（单机形态，不涉及成员发现）。
    #[arg(long)]
    meta: Option<String>,
    /// 心跳间隔（秒）。**必须与 metanode 的巡检口径配套**：metanode 默认 15s 没见到就摘，
    /// 这里默认 5s（三次机会，抗一次网络抖动）。测试/调优时可改小。
    #[arg(long, default_value_t = 5)]
    heartbeat_secs: u64,
}

/// 入册（**best-effort**）：失败只告警，不返回错误。
///
/// 理由：数据节点的核心职责是"吸收自己的 WAL + 服务热读"，那件事不依赖元数据面。
/// 元数据面晚一点起来，只意味着"暂时不在名录里"，而不是"数据节点起不来"。
async fn register_datanode(catalog: &Arc<dyn CatalogOps>, id: &str, address: &str) {
    match catalog
        .register_datanode(yuntun_model::meta::DatanodeMember {
            instance_id: id.to_string(),
            address: address.to_string(),
            registered_at_ms: 0,
        })
        .await
    {
        Ok(()) => tracing::info!(instance_id = %id, address = %address, "数据节点已入册"),
        Err(e) => tracing::warn!(error = %e, "入册失败（元数据面不可达？）—— 心跳轮会重试"),
    }
}

/// **心跳循环**（T12.3）：每 `EVERY` 报一次活；三种结果的处置与 metanode 侧一一对应。
///
/// - `Ok(true)`：正常（说明还在名录里）；
/// - `Ok(false)`：**不在名录里** —— 被超时摘除、或从未入册成功 ⇒ **重新注册**（摘除可恢复）；
/// - `Err(_)`：元数据面不可达 ⇒ 告警 + 下一轮重试（**不退出**：数据节点本地照常工作）。
///
/// 频率与 metanode 的巡检口径**必须配套**：默认这里 5s 一次、metanode 15s 没见到才摘
/// （三次机会，抗一次网络抖动）。`--heartbeat-secs` 改这里时，那边要一起改。
fn spawn_heartbeat(
    catalog: Arc<dyn CatalogOps>,
    id: String,
    address: String,
    every: std::time::Duration,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if shutdown.is_cancelled() {
                break;
            }
            tokio::time::sleep(every).await;
            match catalog.heartbeat(&id).await {
                Ok(true) => {}
                Ok(false) => {
                    tracing::warn!(instance_id = %id, "不在名录里（被摘除或未入册）—— 重新注册");
                    register_datanode(&catalog, &id, &address).await;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "心跳失败（元数据面不可达？），下一轮重试")
                }
            }
        }
    })
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

    // ③ 元数据面：给了 `--meta` 就接 metanode（注册经 raft、心跳只碰内存），
    //    否则只在本进程内登记（单机形态）。
    //    冷存根两种形态一致：先落本机目录。
    let catalog: Arc<dyn CatalogOps> = match &args.meta {
        Some(addr) => Arc::new(yuntun_meta::RemoteCatalog::connect(vec![addr.clone()])?),
        None => Arc::new(MemoryCatalog::new()),
    };
    let store = create_store(&StoreConfig::Local {
        root: cold_root.to_string_lossy().into_owned(),
    })?;

    // ③b **重放 WAL 里的 DDL**（启动恢复）：数据节点自己的建表/删表历史要回到目录里。
    //     给了 `--meta` 时这一步**经 raft 写进 metanode**（同一个 trait 方法），于是
    //     "数据节点重启后表还认不认得"不再依赖客户端再建一次。
    //     幂等：每次启动都整扫一遍 WAL，"已存在/已删除"当成功（`replay_wal_ddl` 的纪律）。
    //     ⚠️ 必须在 `Ingestor::new` **之前**：那一步会把 `wal` 的所有权拿走。
    yuntun_ingest::replay_wal_ddl(&catalog, &wal).await?;

    // ④ 吸收循环：`Ingestor::new` 自带 chunk store ⇒ 写侧与热读侧同一实例
    let ingestor = Arc::new(Ingestor::new(
        IngestorConfig {
            instance_id: args.instance_id.clone(),
            spill_dir: spill_dir.clone(),
            ..Default::default()
        },
        wal,
        // 克隆 Arc（廉价）：后面还要用它把本实例登记进名录
        catalog.clone(),
        store,
    ));
    let shutdown = CancellationToken::new();
    let _accumulator = ingestor.clone().spawn_accumulator(shutdown.clone());

    // ⑤ 热读服务：本地 chunk store 作为 `ShardReader` 暴露。
    //    先 bind 再打印 ⇒ 上层拿到的是**真实**地址（`127.0.0.1:0` 的端口由内核分配）
    let listener = TcpListener::bind(&args.listen).await?;
    let addr = listener.local_addr()?;

    // ⑤ **入册 + 心跳保活**（T12.3 的收口）。
    //    ⚠️ 入册必须排在 bind 之后：`--listen 127.0.0.1:0` 时只有 bind 完才知道真实端口。
    //    入册是 **best-effort**：元数据面暂时不可达不该让数据节点停摆（它仍能本地吸收 WAL、
    //    服务热读）；心跳轮会持续重试直到入册成功。
    let (id, addr_s) = (args.instance_id.clone(), addr.to_string());
    register_datanode(&catalog, &id, &addr_s).await;
    let _heartbeat = spawn_heartbeat(
        catalog.clone(),
        id.clone(),
        addr_s.clone(),
        std::time::Duration::from_secs(args.heartbeat_secs),
        shutdown.clone(),
    );
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
