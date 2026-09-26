//! `metanode` 进程入口（S3-3 收尾）。
//!
//! 只做三件事：**解析参数 → 起节点 → 起服务**。业务语义一处都不在这（模块地图见 `lib.rs`）：
//!
//! | 关注点 | 在哪 |
//! |---|---|
//! | 一致性（选主/复制/快照） | `lib.rs`（raft 驱动） |
//! | 状态与业务语义 | `yuntun-catalog::CatalogState` |
//! | op 的解码与应用 | `op.rs` |
//! | 错误码映射 | `error.rs` |
//! | RPC 面 | `service.rs` |
//!
//! 入口层薄的好处：**进程行为可以在测试里被完整复现**（`MetaNode` 就是同一套），
//! 于是"进程起不来"这类问题不需要靠 `--nocapture` 打日志去猜。

use std::time::Duration;

use yuntun_meta::{cli, MetaNode, MetaOptions};

fn main() {
    // clap 统一处理 `--help`/`--version`（退 0）与用法错（退 2）；语义错也归"用法错"。
    let args = cli::Args::parse_checked();

    if let Err(e) = args.check_bootstrap() {
        eprintln!("拒绝启动：{e}");
        std::process::exit(2);
    }

    if let Err(e) = run(&args) {
        eprintln!("metanode 启动失败：{e}");
        std::process::exit(1);
    }
}

fn run(args: &cli::Args) -> Result<(), Box<dyn std::error::Error>> {
    // ⓪ **先建运行时**：多节点传输要 `Handle::current()` 给每个 peer 起发送任务
    //    （单节点不需要网络，但把顺序统一成"先运行时"更少一个分支 —— 少一个分支就少一种
    //    "单机能跑、多机报错"的差异）。`enter()` 的 guard 要活到 `open` 之后。
    let rt = tokio::runtime::Runtime::new()?;

    // ⓪' **监听 + `--join`**（`§119`）：这两件事都在 `enter()` **之前**做完 ——
    //     `Runtime::block_on` 不能在"已进入运行时"的线程里调（会 panic）。
    //
    //     顺序是刻意的：**先 bind，再 join**。这样集群一知道我们的地址，我们的 socket 就已经在
    //     `listen` 了 —— 它发来的首批 append（很可能是一条**快照**）会排在 accept backlog 里，
    //     而不是因为"服务还没起"被丢掉。丢一条快照是致命的（`§107`：raft 不会重发），
    //     而"排一会儿队"完全无害。
    let (listener, joined) = rt.block_on(async {
        let listener = tokio::net::TcpListener::bind(args.listen).await?;
        let joined = match &args.join {
            Some(addrs) => Some(join_cluster(addrs, args.id, &args.advertise_addr()).await?),
            None => None,
        };
        Ok::<_, Box<dyn std::error::Error>>((listener, joined))
    })?;

    let _guard = rt.enter();

    // ① 起节点：打开存储 → 按盘上状态重建状态机 → 拉起 raft 线程（+ 多节点传输）
    //
    //    成员表与地址：`--join` 时**全部来自集群回包**（`§119`）；否则来自命令行。
    let (voters, peers) = match &joined {
        Some(r) => (
            r.voter_ids.clone(),
            r.members
                .iter()
                .map(|m| (m.node_id, m.address.clone()))
                .collect::<std::collections::HashMap<u64, String>>(),
        ),
        None => (args.voters.clone(), args.peer_map()),
    };
    if let Some(r) = &joined {
        println!(
            "已加入集群：voters={:?} learners={:?}（起步配置来自 {:?}，共 {} 个成员地址）",
            r.voter_ids,
            r.learner_ids,
            args.join,
            r.members.len()
        );
    }
    let node = MetaNode::open_with(
        &args.dir,
        args.id,
        voters,
        peers,
        MetaOptions {
            compact_log_entries: args.snapshot_log_entries,
            self_addr: args.advertise_addr(),
        },
    )?;

    // ② 起服务 + 等选主
    rt.block_on(async move {
        // ⚠️ **顺序是硬要求（`§103`）：先起服务，再等选主。**
        //    raft 选主靠节点间**互相投票**，而投票走的就是这个 gRPC 服务 —— 把"等选主"排在
        //    "起服务"之前，每个节点都在等别人的票、而谁的服务器都还没起 ⇒ 三个进程各自等到
        //    超时、**集体退出**（实证：3 个真进程 13s 后一个不剩）。单节点组踩不到（自己一票
        //    就够），所以这个坑此前一直没暴露。
        let addr = listener.local_addr()?;
        let served = node.handle();
        tokio::spawn(async move {
            if let Err(e) = yuntun_meta::serve(served, listener).await {
                // 服务退出要让运维看得见（否则表现为"进程还在、就是连不上"）
                eprintln!("metanode 服务退出：{e}");
            }
        });

        // ③ 等**集群里**有 leader —— 判据是 `leader_id != 0`，**不是**"等自己当选"。
        //    多节点组里 follower 合法地不是 leader（用 `MetaNode::wait_leader` 的判据，
        //    follower 永远等不到、启动即失败，这正是多节点真部署此前起不来的原因之一）。
        if node.wait_any_leader(Duration::from_secs(10)).is_none() {
            return Err(format!(
                "10s 内集群里没有 leader（成员表 {:?}）—— 单节点组正常应在几十毫秒内选出来；\
                 多节点组超时通常是：**别的节点没在这个窗口内起来**（它们要互相投票）\
                 或 --peer 里的地址彼此不可达",
                args.voters
            )
            .into());
        }

        // ④ 接口行：**就绪之后**才打印（编排/测试靠它拿真实地址，也靠它判断"可以连了"）。
        //    `--listen 127.0.0.1:0` 时端口由内核分配，所以只有 bind 完才知道真实地址。
        //    这一行是**接口**（不是日志），改格式等于破坏调用方，别随手改。
        println!(
            "metanode id={} listening on {addr} dir={} voters={:?}",
            args.id,
            args.dir.display(),
            args.voters
        );
        std::io::Write::flush(&mut std::io::stdout())?;

        // T12.3：**存活巡检**（leader-only）。心跳超时的数据节点会被摘出名录，
        // 而摘除走的是 raft 的 op（心跳本身走内存，`§3.2`）。
        // 默认口径：数据节点每 5s 心跳一次，**15s** 未见到即摘（3 次机会，抗一次抖动）。
        let _sweep = yuntun_meta::spawn_liveness_sweep(
            node.handle(),
            std::time::Duration::from_secs(15),
            std::time::Duration::from_secs(5),
            tokio_util::sync::CancellationToken::new(),
        );

        // ⑤ 吊住进程：服务挂在后台任务上，`node` 留在这个作用域里（它的 `Drop` 停 raft）。
        std::future::pending::<()>().await;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

/// 问一个已有集群要"起步配置"（`Meta.Join`，`§119`）。
///
/// # 为什么支持逗号分隔的多个接触点
///
/// 只有 **leader** 受理成员变更；非 leader 会回 `NotLeader` + hint，而 hint 是个 **id**
/// —— 调用方（新节点）手里只有地址。所以给多个地址、逐个试到 leader 为止即可
/// （与 `bench --meta`、`RemoteCatalog` 同一套做法：接触点是**列表**，不是单点）。
///
/// # 为什么用现成的 `MetaClient`
///
/// 不加新的协议面：这就是 `metanode` 之间传 raft 消息用的那个客户端，只是调的另一个方法。
async fn join_cluster(
    addrs: &str,
    id: u64,
    advertise: &str,
) -> Result<yuntun_proto::meta::JoinResponse, Box<dyn std::error::Error>> {
    let mut tried: Vec<String> = Vec::new();
    for addr in addrs.split(',').map(str::trim).filter(|a| !a.is_empty()) {
        let ep = format!("http://{addr}");
        let Ok(mut client) =
            yuntun_proto::meta::meta_client::MetaClient::connect(ep).await
        else {
            tried.push(format!("{addr}: 连不上"));
            continue;
        };
        let req = yuntun_proto::meta::JoinRequest {
            node_id: id,
            address: advertise.to_string(),
            learner_only: true,
        };
        match client.join(req).await {
            Ok(r) => return Ok(r.into_inner()),
            // 非 leader（或 leader 还没选出来）：换下一个接触点
            Err(e) => tried.push(format!("{addr}: {e}")),
        }
    }
    Err(format!(
        "没有任何接触点受理了加入请求（--join {addrs:?}）：\n  {}",
        tried.join("\n  ")
    )
    .into())
}
