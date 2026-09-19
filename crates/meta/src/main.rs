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

use yuntun_meta::{cli, MetaNode};

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
    // ① 起节点：打开存储 → 按盘上状态重建状态机 → 拉起 raft 线程
    let node = MetaNode::open(&args.dir, args.id, args.voters.clone())?;

    // ② 等选主。**必须先等**：在选出 leader 之前接请求只会全部收到 `NotLeader`，
    //    调用方会以为"服务起来了但一直失败"（比等几百毫秒难查得多）。
    if !node.wait_leader(Duration::from_secs(10)) {
        return Err(format!(
            "10s 内未当选 leader（成员表 {:?}）—— 单节点组正常应在几十毫秒内选出来；\
             超时通常意味着成员表里有本 build 不认识的节点",
            args.voters
        )
        .into());
    }

    // ③ 起服务
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        let listener = tokio::net::TcpListener::bind(args.listen).await?;
        let addr = listener.local_addr()?;
        // 这一行是**接口**（不是日志）：编排/测试靠它拿真实地址（`--listen 127.0.0.1:0` 时
        // 端口由内核分配）。改格式等于破坏调用方，别随手改。
        println!(
            "metanode id={} listening on {addr} dir={} voters={:?}",
            args.id,
            args.dir.display(),
            args.voters
        );
        std::io::Write::flush(&mut std::io::stdout())?;
        yuntun_meta::serve(node.handle(), listener).await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
