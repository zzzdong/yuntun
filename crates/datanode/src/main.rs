//! `yuntun-datanode` —— **数据进程**（角色二之一：`meta` / `data`）。
//!
//! ## 角色模型（`operation-log §79` 的更正）
//!
//! | 角色 | 进程 | 有什么 |
//! |---|---|---|
//! | **meta** | `yuntun-meta` | raft + 元数据权威（**不含数据**） |
//! | **data** | **本进程** | 私有 WAL + 热 chunk + 共享冷存储 +（可选）**压缩/GC** +（可选）**SQL 服务面** |
//!
//! 曾经的 `yuntun-queryd` / `yuntun-compactor` 是**把角色当成了进程**：
//!
//! - "只查询、不吃 WAL"是**数据进程的特例**（`architecture §4.2`：接到 SQL 的 datanode
//!   充当协调者）—— 同一角色的另一种开关组合，不是第三类进程；
//! - 压缩是**数据进程的第二职能**（`plan.md §7.5` T14.1 明说它今天是"单进程后台任务"，
//!   R6 才升格为带 meta 租约的全局作业）。
//!
//! ## 三个开关决定本进程的形态
//!
//! | 形态 | `--no-ingest` | `--sql-listen` | `--compaction` |
//! |---|---|---|---|
//! | 纯数据节点（写 + 热读服务） | — | — | 可选 |
//! | 数据节点 + 协调者（`§4.2` 默认形态） | — | ✓ | 可选 |
//! | **只查询**的数据进程（K4 的"需要时再加"） | ✓ | 必须 | 禁止 |
//!
//! 三处**启动即校验**（宁可起不来，也别起成一个语义含糊的进程）：只查询形态必须给
//! `--sql-listen`（否则没有任何对外职责）、必须给 `--meta`（没有本地数据，热数据全靠名录发现）、
//! 不得承担压缩（它可能被扩多份，而压缩作业每集群只应有一个）。
//!
//! ## 它是什么
//!
//! 单进程形态下"吸收 WAL / 持有热数据 / 对外提供热读"三件事都挤在 `standalone` 里，
//! 于是热读只能是**进程内函数调用**（装配层把 `Arc<ChunkStore>` 直接交给查询侧）。
//! 本二进制把**数据进程**摘出来：它独占自己的私有目录（WAL + spill），吸收自己的 WAL
//! 得到热数据，并把热数据经数据面 gRPC（`yuntun-shardrpc`）对外提供；再给上 `--sql-listen`
//! 它就同时是**协调者**（拉别人的热数据，`§4.2`）。
//!
//! ## 它不是什么
//!
//! - **不是两套实现**：用的是同一批组件与同一条契约（`Ingestor::new` 自带 chunk store、
//!   `private_dir` 租约、`ShardReader` 只转发本地水位判断）；
//! - **本轮的 SQL 面只读**：把写面（SQL DML → 本进程的 ingestor）接上是下一步；
//!   跨进程写入面本来就还没定（`§79` 的遗留）。
//!
//! ## 关键约束：先占租约、再动盘
//!
//! 租约检查放在**所有会改文件的操作之前** —— 更要紧的是它排在 WAL 打开之前，
//! 于是"被拒的那个进程"不会碰别人正在用的目录。（只查询形态不占任何租约：它没有私有状态。）
//!
//! ## 接口行（**是接口，不是日志**；改格式等于破坏调用方）
//!
//! - `LISTEN <addr>`：数据面（热读）服务就绪 —— 仅 ingest 形态
//! - `SQL-LISTEN <addr>`：Flight SQL 服务就绪 —— 仅给了 `--sql-listen` 时

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_ingest::{Ingestor, IngestorConfig};
use yuntun_model::private_dir::{self, DirOwner};
use yuntun_query::{LocalCatalog, PartialPolicy, QueryEngine};
use yuntun_store::{ShardReader, StoreConfig, create_store};
use yuntun_wal::config::WalConfig;
use yuntun_wal::writer::WalWriter;

mod query;

#[derive(Parser, Debug)]
#[command(
    name = "yuntun-datanode",
    version,
    about = "数据进程：吸收自己的 WAL、持有热数据、对外提供热读；可选服务 SQL 与压缩/GC"
)]
struct Args {
    /// 实例标识（= `FileManifest.source_instance`；热数据按它归属，`§65`）
    #[arg(long)]
    instance_id: String,
    /// 私有数据根目录（其下 `wal/`+`spill/` 仅 ingest 形态；`cold/` 是它的默认冷存储）
    #[arg(long)]
    dir: PathBuf,
    /// 冷存储根目录（默认 `<dir>/cold`）。
    ///
    /// 为什么要能单独指：**多节点必须共享同一份冷存储，而私有目录必须各归各的**
    /// （WAL/spill 被租约独占，同一目录第二个进程启动即被拒）。本机形态下这就意味着
    /// "各给一个 `--dir`，但指同一个 `--cold-root`"——真实部署里它是 S3。
    #[arg(long)]
    cold_root: Option<PathBuf>,
    /// 数据面（热读）服务监听地址；仅 ingest 形态。`127.0.0.1:0` = 内核分配
    #[arg(long, default_value = "127.0.0.1:0")]
    listen: String,
    /// **关掉 ingest** ⇒ 只查询的数据进程：不吃 WAL、不留本地数据，只作为协调者拉别人的热数据
    #[arg(long)]
    no_ingest: bool,
    /// Flight SQL 监听地址：给了就**对外提供 SQL**（`architecture §1.1`：datanode 可服务 SQL）
    #[arg(long)]
    sql_listen: Option<String>,
    /// metanode 地址（如 `127.0.0.1:50051`）。给了就**入册**（注册走 raft）并起**心跳保活**；
    /// 只查询形态**必须**给（它没有本地数据，热数据全靠名录发现）。
    #[arg(long)]
    meta: Option<String>,
    /// 心跳间隔（秒）。**必须与 metanode 的巡检口径配套**：metanode 默认 15s 没见到就摘，
    /// 这里默认 5s（三次机会，抗一次网络抖动）。测试/调优时可改小。
    #[arg(long, default_value_t = 5)]
    heartbeat_secs: u64,
    /// 名录巡检间隔（秒）：发现新数据节点，以及被摘除的成员
    #[arg(long, default_value_t = 5)]
    reconcile_secs: u64,
    /// 部分结果策略（`architecture §4.2`）：`allow`（默认，返回部分结果 + 标记）或 `reject`
    #[arg(long, default_value = "allow")]
    partial: String,
    /// 数据面 RPC 超时（秒）：超过它即视为"无响应"，该来源按 `§4.3` 降级
    #[arg(long, default_value_t = 5)]
    hot_read_timeout_secs: u64,
    /// **整段热读的总预算**（秒，`§88`）：一次查询在热数据上最多等多久（0 = 非法）。
    ///
    /// 与 `--hot-read-timeout-secs` 是两层：那个是"**一个 RPC** 最多等多久"，
    /// 这个是"**这次查询**愿意为热数据等多久"。扇出已并发（等待取最大而非相加），
    /// 预算就是这个最大之上的硬上界。
    #[arg(long, default_value_t = 10)]
    hot_read_budget_secs: u64,
    /// 额外承担**压缩 + 孤儿 GC**（仅 ingest 形态；默认关）。
    ///
    /// ⚠️ 多数据节点时**只应有一个**打开它：合并的"读 → 合并 → 提交"窗口没有租约，
    /// 两个节点同时合并同一 shard 会各产出一份产物（行数不会错，多出的那份会被孤儿清理回收）。
    /// R6 的 meta 租约（T14.1/T14.2）会把这条部署约束变成机制。
    #[arg(long)]
    compaction: bool,
    /// 压缩触发阈值：同一 shard 的可见文件数 ≥ 它才合并
    #[arg(long, default_value_t = 5)]
    compaction_min_files: usize,
    /// 压缩作业间隔（秒）
    #[arg(long, default_value_t = 60)]
    compaction_interval_secs: u64,
    /// 孤儿文件静置期（秒）—— **它同时就是墓碑期的时长**（T14.5）。
    ///
    /// 一个对象要满足两件事才被删：**不在保护集合里**（活着 / 墓碑期未过 / 在途，三者都不是）
    /// 且**静置超过它**。默认 3600 是保守取值；设计的推荐运行值是 **10–60s** ——
    /// 调小它是安全的，因为写者安全自 T14.3 起由**在途登记**（结构保证）承担，
    /// grace 只剩"给还在读旧快照的读者一个窗口"。
    #[arg(long, default_value_t = 3600)]
    compaction_gc_grace_secs: u64,
    /// 压缩租约的期限（秒）：到期即失效，接手方不必等它点头（`§81`）。
    ///
    /// 短 = 接管快、续租（一次 raft 写）频；长 = 反之。默认 30s 与"巡检/心跳都是秒级"配套。
    #[arg(long, default_value_t = 30)]
    compaction_ttl_secs: u64,
}

/// **启动即校验**（`§79` 的三种形态）：宁可起不来，也别起成一个语义含糊的进程。
fn validate(args: &Args) -> Result<(), String> {
    if args.no_ingest && args.sql_listen.is_none() {
        return Err(
            "--no-ingest 是「只查询的形态」：必须给 --sql-listen，否则这个进程没有任何对外职责"
                .into(),
        );
    }
    if args.no_ingest && args.meta.is_none() {
        return Err(
            "--no-ingest 的形态没有本地数据：必须给 --meta（热数据全靠名录发现）".into(),
        );
    }
    if args.no_ingest && args.compaction {
        return Err(
            "--compaction 是数据进程（ingest 形态）的职能：只查询的形态可能被扩多份，\
             而压缩作业每集群只应有一个"
                .into(),
        );
    }
    Ok(())
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
    every: Duration,
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
    if let Err(e) = validate(&args) {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, e).into());
    }

    let shutdown = CancellationToken::new();
    let wal_root = args.dir.join("wal");
    let spill_dir = args.dir.join("spill");
    // 冷存储：默认在私有目录下（单机形态），可显式指向别处（多节点共享同一份）
    let cold_root = args
        .cold_root
        .clone()
        .unwrap_or_else(|| args.dir.join("cold"));
    std::fs::create_dir_all(&cold_root)?;

    // ① 私有目录租约 —— **排在最前**：被拒的进程连 WAL 都不该打开。
    //    （错误信息会点名 instance_id / role / pid，见 `§62`）
    //
    //    ⚠️ 这两个守卫必须**活到进程结束**：它们若被放进下面的 `if` 块里，块一结束就释放，
    //    于是"租约"在进程还在跑的时候就已经没了 —— 第二个消费者能大摇大摆进来。
    //    绑在 `main` 的局部变量上（`if` 表达式赋值）是刻意的。
    let _wal_lease = if args.no_ingest {
        None
    } else {
        std::fs::create_dir_all(&wal_root)?;
        Some(private_dir::acquire(
            &wal_root,
            DirOwner {
                instance_id: args.instance_id.clone(),
                role: "wal-root".into(),
            },
        )?)
    };
    let _spill_lease = if args.no_ingest {
        None
    } else {
        std::fs::create_dir_all(&spill_dir)?;
        Some(private_dir::acquire(
            &spill_dir,
            DirOwner {
                instance_id: args.instance_id.clone(),
                role: "chunk-spill".into(),
            },
        )?)
    };

    // ② 元数据面（两种形态都要）：给了 `--meta` 就接 metanode（注册经 raft、心跳只碰内存），
    //    否则只在本进程内登记（单机形态）。冷存根两种形态一致：先落本机目录。
    let catalog: Arc<dyn CatalogOps> = match &args.meta {
        Some(addr) => Arc::new(yuntun_meta::RemoteCatalog::connect(vec![addr.clone()])?),
        None => Arc::new(MemoryCatalog::new()),
    };
    let store = create_store(&StoreConfig::Local {
        root: cold_root.to_string_lossy().into_owned(),
    })?;

    // ③ 本地热读器：ingest 形态下**本进程自己**就是一个来源（读己之写）。
    //    查询侧装配时先把它塞进 `hot_shards` ⇒ 名录巡检会跳过自连（`query::reconcile_hot_readers`）。
    let mut local_reader: Option<Arc<dyn ShardReader>> = None;
    // **接受写入的面**（`§96`）：ingest 形态下，这个进程本来就握着 WAL + chunk store
    // （"任意 datanode 收到写入（无路由）"，`architecture §5`）—— 所以它的 SQL 面**可写**。
    // 只查询形态没有本地数据、也没有 WAL ⇒ 保持只读（它写不了，不该假装能写）。
    let mut sql_ingestor: Option<Arc<Ingestor>> = None;

    if !args.no_ingest {
        // ---- 数据侧：WAL → DDL 重放 → Ingestor → 数据面服务 + 入册/心跳 ----
        let wal = WalWriter::open(
            WalConfig {
                dir: wal_root.clone(),
                ..Default::default()
            },
            0,
        )
        .await?;

        // 重放 WAL 里的 DDL（启动恢复）：数据进程自己的建表/删表历史要回到目录里。
        // 给了 `--meta` 时这一步**经 raft 写进 metanode**（同一个 trait 方法），于是
        // "数据节点重启后表还认不认得"不再依赖客户端再建一次。
        // 幂等：每次启动都整扫一遍 WAL，"已存在/已删除"当成功（`replay_wal_ddl` 的纪律）。
        // ⚠️ 必须在 `Ingestor::new` **之前**：那一步会把 `wal` 的所有权拿走。
        yuntun_ingest::replay_wal_ddl(&catalog, &wal).await?;

        // 吸收循环：`Ingestor::new` 自带 chunk store ⇒ 写侧与热读侧同一实例
        let ingestor = Arc::new(Ingestor::new(
            IngestorConfig {
                instance_id: args.instance_id.clone(),
                spill_dir: spill_dir.clone(),
                ..Default::default()
            },
            wal,
            // 克隆 Arc（廉价）：后面还要用它入册、起压缩作业
            catalog.clone(),
            store.clone(),
        ));
        sql_ingestor = Some(ingestor.clone());
        let _accumulator = ingestor.clone().spawn_accumulator(shutdown.clone());

        // **压缩 + 孤儿 GC 角色**（数据进程的第二职能，`--compaction` 打开）。
        // 放在吸收循环之后：本进程已经握着共享冷存储与目录句柄，压缩不需要新输入。
        if args.compaction {
            let compactor = Arc::new(yuntun_compaction::Compactor {
                lease_holder: args.instance_id.clone(),
                cfg: yuntun_compaction::CompactionConfig {
                    min_files: args.compaction_min_files,
                    interval: Duration::from_secs(args.compaction_interval_secs),
                    orphan_grace: Duration::from_secs(args.compaction_gc_grace_secs),
                    // 租约期限：拿不到/续不上就停手（`§81`）
                    lease_ttl: Duration::from_secs(args.compaction_ttl_secs),
                    ..Default::default()
                },
                catalog: catalog.clone(),
                store: store.clone(),
                // 与写入侧**同一个格式**：合并必须读得懂自己写出去的东西
                format: ingestor.cfg.default_format,
            });
            let _compaction =
                yuntun_compaction::spawn_compaction_loop(compactor, shutdown.clone());
            let _gc = yuntun_compaction::spawn_orphan_cleanup(
                store.clone(),
                catalog.clone(),
                "yuntun/".to_string(),
                Duration::from_secs(args.compaction_gc_grace_secs),
                shutdown.clone(),
            );
            tracing::info!(
                min_files = args.compaction_min_files,
                interval_secs = args.compaction_interval_secs,
                "本数据节点额外承担压缩 + 孤儿 GC"
            );
        }

        // 热读服务：本地 chunk store 作为 `ShardReader` 暴露。
        // 先 bind 再打印 ⇒ 上层拿到的是**真实**地址（`127.0.0.1:0` 的端口由内核分配）
        let chunks = ingestor.chunks();
        let listener = TcpListener::bind(&args.listen).await?;
        let addr = listener.local_addr()?;

        // 入册 + 心跳保活（T12.3 的收口）。⚠️ 入册必须排在 bind 之后：
        // `--listen 127.0.0.1:0` 时只有 bind 完才知道真实端口。
        // 入册是 **best-effort**：元数据面暂时不可达不该让数据节点停摆；心跳轮会持续重试。
        let addr_s = addr.to_string();
        register_datanode(&catalog, &args.instance_id, &addr_s).await;
        let _heartbeat = spawn_heartbeat(
            catalog.clone(),
            args.instance_id.clone(),
            addr_s.clone(),
            Duration::from_secs(args.heartbeat_secs),
            shutdown.clone(),
        );

        local_reader = Some(chunks.clone());
        tokio::spawn(async move {
            if let Err(e) = yuntun_shardrpc::serve(chunks, listener).await {
                tracing::error!(error = %e, "数据面服务退出");
            }
        });
        println!("LISTEN {addr}");
        tracing::info!(
            %addr,
            instance_id = %args.instance_id,
            dir = %args.dir.display(),
            "datanode serving hot shards"
        );
    }

    // ④ 查询侧（协调者 / 只查询形态）：接热读器 + 对外提供 SQL。
    //    `architecture §4.2`：**接到 SQL 的 datanode 充当协调者** —— 所以这一面在数据进程里，
    //    而不是某一类独立的"查询进程"（`§79`）。
    if let Some(sql_listen) = &args.sql_listen {
        // 配置写错**启动即报错**：静默取默认会让"我明明配了 reject"变成一句谎言
        let partial = PartialPolicy::parse(&args.partial).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "invalid --partial {:?}: 只接受 \"allow\"（默认）或 \"reject\"",
                    args.partial
                ),
            )
        })?;
        let hot_read_timeout = Duration::from_secs(args.hot_read_timeout_secs);
        // 0 = 非法（等于把所有热读都判成超时）⇒ 启动即报错，不静默取默认
        if args.hot_read_budget_secs == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid --hot-read-budget-secs 0: 它等于\"所有热读都超时\"，请给正整数（默认 10）",
            )
            .into());
        }
        let hot_read_budget = Duration::from_secs(args.hot_read_budget_secs);

        let cache = Arc::new(LocalCatalog::new());
        cache.set_catalog_ops(catalog.clone());
        // 本进程自己（ingest 形态）就是一个来源：先把本地热读器塞进去，巡检会跳过自连
        if let Some(local) = &local_reader {
            cache.set_hot_shards(args.instance_id.clone(), local.clone());
        }
        // 其余的按名录发现（`§71.5`：名录带地址 ⇒ 建 `GrpcShardFetch`）
        query::reconcile_hot_readers(&cache, &catalog, hot_read_timeout).await?;
        let _reconcile = query::spawn_reconcile(
            cache.clone(),
            catalog.clone(),
            Duration::from_secs(args.reconcile_secs),
            hot_read_timeout,
            shutdown.clone(),
        );

        let engine = Arc::new(
            QueryEngine::new(store.clone(), cache)
                .with_partial_policy(partial)
                .with_hot_read_budget(hot_read_budget),
        );
        let listener = TcpListener::bind(sql_listen).await?;
        let addr = listener.local_addr()?;
        // 【`§96`】**把"收到写入"的面补上**：此前这里**恒为** `new_readonly` ⇒
        // 数据进程根本没有接受写入的网络面（WAL 只能靠启动回放或外部写文件）。
        // 形态决定能力，而不是一刀切：
        // - ingest 形态：本进程握着 WAL + chunk store ⇒ **可写**（`FlightServer::new`，
        //   与 `standalone`/`serve_flight` 跑的是同一条已验路径）⇒ "任意 datanode 收到写入（无路由）"
        //   从设计里的一句话变成可调用的行为；
        // - 只查询形态（`--no-ingest`）：没有本地数据、没有 WAL ⇒ **只读**（保持原样）。
        let flight = match &sql_ingestor {
            Some(ing) => {
                yuntun_server::FlightServer::new(ing.clone(), engine, catalog.clone())
            }
            None => yuntun_server::FlightServer::new_readonly(engine, catalog.clone()),
        };
        let svc = arrow_flight::flight_service_server::FlightServiceServer::new(flight);
        // 与 metanode / shardrpc 同一手法：`futures::stream::unfold` 把 accept 循环包成 Stream
        let incoming = futures::stream::unfold(listener, |l| async move {
            match l.accept().await {
                Ok((sock, _addr)) => Some((Ok::<_, std::io::Error>(sock), l)),
                Err(e) => Some((Err(e), l)),
            }
        });
        tokio::spawn(async move {
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(incoming)
                .await
            {
                tracing::error!(error = %e, "Flight SQL 服务退出");
            }
        });
        println!("SQL-LISTEN {addr}");
        tracing::info!(%addr, "datanode serving readonly flight sql");
    }

    // ⑤ 吊住进程：各服务都在后台跑，各自的错误各自记录（都记了转出点，不会被静默吞掉）。
    shutdown.cancelled().await;
    Ok(())
}
