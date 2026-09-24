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
    let _guard = rt.enter();

    // ① 起节点：打开存储 → 按盘上状态重建状态机 → 拉起 raft 线程（+ 多节点传输）
    let node = MetaNode::open_with(
        &args.dir,
        args.id,
        args.voters.clone(),
        args.peer_map(),
        MetaOptions {
            compact_log_entries: args.snapshot_log_entries,
        },
    )?;

    // ② 起服务 + 等选主
    rt.block_on(async move {
        // ⚠️ **顺序是硬要求（`§103`）：先起服务，再等选主。**
        //    raft 选主靠节点间**互相投票**，而投票走的就是这个 gRPC 服务 —— 把"等选主"排在
        //    "起服务"之前，每个节点都在等别人的票、而谁的服务器都还没起 ⇒ 三个进程各自等到
        //    超时、**集体退出**（实证：3 个真进程 13s 后一个不剩）。单节点组踩不到（自己一票
        //    就够），所以这个坑此前一直没暴露。
        let listener = tokio::net::TcpListener::bind(args.listen).await?;
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
