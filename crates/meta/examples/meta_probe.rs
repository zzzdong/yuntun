//! 容器化多节点测试用的**极小 Meta 客户端**（`tests/cluster.sh` 的断言靠它）。
//!
//! # 为什么需要它
//!
//! `§103` 记过一个缺口：全仓只有 Flight SQL 的 `yuntun-cli`，**没有任何 Meta gRPC 的客户端** ——
//! 所以"等选主 / 经网络写 / 换主后仍能写 / 断网后少数派写不进"这类断言只能写在**进程内**测试里。
//! 容器化的集群（进程在别的网络命名空间、地址由编排给）没法用进程内夹具，于是补这个最小可用的。
//!
//! 刻意不做交互、不读配置、不做花活，只做四件事（退出码 0 = 成功，非 0 = 失败，编排脚本据此判定）：
//!
//! ```text
//! meta_probe status <addr>...          每个节点的 role/term/leader_id/first/last/applied/commit
//! meta_probe leader <addr>...          等出 leader（读各节点 Status，不猜），打印它的 id
//! meta_probe write  <key> <addr>...    找到 leader 提交一条 CommitFiles（重试到成功）
//! meta_probe verify <key> <addr>...    **重放同一个幂等键**：必须被拒（`accepted=false`）
//! ```
//!
//! 最后一条是故意的：幂等记录**只在状态机里**，所以"重放被拒"证明**那条提交真的在状态机里**，
//! 而不是"日志在、状态机没有"（与 `§50`/`§103` 的用例同一条判据）。

use std::time::{Duration, Instant};

use yuntun_proto::meta as pb;
use yuntun_proto::meta::meta_client::MetaClient;

type Client = MetaClient<tonic::transport::Channel>;

/// 连不上就重试：容器刚起来时端口可能还没监听（比"一上来就报错"友好）。
async fn connect_one(addr: &str) -> Client {
    let ep = format!("http://{addr}");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match MetaClient::connect(ep.clone()).await {
            Ok(c) => return c,
            Err(e) => {
                if Instant::now() >= deadline {
                    eprintln!("连不上 {addr}：{e}");
                    std::process::exit(2);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn connect_all(addrs: &[String]) -> Vec<Client> {
    let mut out = Vec::with_capacity(addrs.len());
    for a in addrs {
        out.push(connect_one(a).await);
    }
    out
}

async fn status(c: &Client) -> Option<pb::StatusResponse> {
    c.clone()
        .status(pb::StatusRequest {})
        .await
        .ok()
        .map(|r| r.into_inner())
}

/// 等出 leader（**读各节点的 Status**，不是客户端猜），返回 (下标, leader_id)。
async fn wait_leader(clients: &[Client], secs: u64) -> (usize, u64) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        for (i, c) in clients.iter().enumerate() {
            if let Some(st) = status(c).await
                && st.role == "Leader"
            {
                return (i, st.node_id);
            }
        }
        if Instant::now() >= deadline {
            eprintln!("{secs}s 内没有任何节点自称 leader");
            std::process::exit(3);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 与进程内测试用的 `commit_op` 同形（`tests/multi_node_grpc_e2e.rs`）：一条最小的 CommitFiles。
fn commit_op(key: &str, batch_id: &str) -> pb::Op {
    pb::Op {
        now_ms: 1_700_000_000_000,
        kind: Some(pb::op::Kind::CommitFiles(pb::CommitFilesOp {
            request: Some(pb::CommitFilesRequestMsg {
                table: "public.cpu".into(),
                batch_id: batch_id.into(),
                client_request_id: Some(key.into()),
                client_request_ids: vec![key.into()],
                shard: "s0".into(),
                time_window: "w1".into(),
                files: vec![pb::FileManifestMsg {
                    file_path: format!("p/{batch_id}.parquet"),
                    batch_id: batch_id.into(),
                    row_count: 7,
                    ..Default::default()
                }],
                schema_version: 1,
                row_count: 7,
            }),
        })),
    }
}

/// 每个 key 用自己的 `batch_id`（`b-<key>`）。
///
/// 为什么不能共用一个：`batch_id` 是**批次的幂等键**，与请求级的 `client_request_id` 是两回事 ——
/// 共用会被状态机当成"这个批次已经提交过"而**正当拒绝**（`accepted=false`），
/// 于是探针就会把"集群写不进去"和"我自己重用了批次号"混成一件事（本文件第一版就踩了）。
/// 而 `verify` 要的正是"**同一个 key + 同一个批次**再提交一次"⇒ 命中幂等记录 ⇒ 必须被拒。
fn batch_id_for(key: &str) -> String {
    format!("b-{key}")
}

/// 提交一条 op 并**等到答复**（换主/选主期间重试；写不进就超时退出）。
async fn propose_with_retry(clients: &[Client], op: pb::Op, secs: u64) -> pb::ProposeResponse {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let (i, leader) = wait_leader(clients, 30).await;
        let r = clients[i]
            .clone()
            .propose(pb::ProposeRequest {
                op: Some(op.clone()),
                request_id: b"probe".to_vec(),
                schema_ver: 0,
            })
            .await;
        match r {
            Ok(resp) => return resp.into_inner(),
            Err(e) => {
                if Instant::now() >= deadline {
                    eprintln!("提交失败（leader={leader}）：{e}");
                    std::process::exit(4);
                }
                // `NotLeader` / `Unavailable` 都是**可重试**的（见 crate::error 的约定 3）
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some((cmd, rest)) = args.split_first() else {
        eprintln!("用法：meta_probe <status|leader|write|verify> [key] <addr>...");
        std::process::exit(64);
    };

    // `write` / `verify` 的第一个参数是 key，其余是地址
    let (key, addrs): (Option<String>, Vec<String>) = if cmd == "write" || cmd == "verify" {
        let Some((k, a)) = rest.split_first() else {
            eprintln!("{cmd} 需要 <key> <addr>...");
            std::process::exit(64);
        };
        (Some(k.clone()), a.to_vec())
    } else {
        (None, rest.to_vec())
    };
    if addrs.is_empty() {
        eprintln!("至少要给一个地址");
        std::process::exit(64);
    }

    let clients = connect_all(&addrs).await;

    match cmd.as_str() {
        "status" => {
            for (i, c) in clients.iter().enumerate() {
                match status(c).await {
                    Some(st) => println!(
                        "{addr}: id={} role={} term={} leader={} commit={} applied={} first={} last={}",
                        st.node_id,
                        st.role,
                        st.term,
                        st.leader_id,
                        st.commit_index,
                        st.applied_index,
                        st.first_index,
                        st.last_index,
                        addr = addrs[i],
                    ),
                    None => println!("{}: Status 调用失败", addrs[i]),
                }
            }
        }
        "leader" => {
            let (_, id) = wait_leader(&clients, 30).await;
            println!("{id}");
        }
        "write" => {
            let key = key.expect("write 需要 key");
            let r = propose_with_retry(&clients, commit_op(&key, &batch_id_for(&key)), 30).await;
            println!("accepted={} manifest_ver={}", r.accepted, r.manifest_ver);
            if !r.accepted {
                eprintln!("❌ 写没被接受（key={key}）—— 集群写不进去");
                std::process::exit(1);
            }
        }
        "verify" => {
            let key = key.expect("verify 需要 key");
            // 同一个幂等键**再提交一次**：必须被拒（幂等记录只在状态机里 ⇒ 命中 = 数据真的在）
            let r = propose_with_retry(&clients, commit_op(&key, &batch_id_for(&key)), 30).await;
            println!("accepted={} manifest_ver={}", r.accepted, r.manifest_ver);
            if r.accepted {
                eprintln!("❌ 重放同一个幂等键竟然被接受了（{key}）—— 幂等记录不在状态机里");
                std::process::exit(1);
            }
        }
        other => {
            eprintln!("未知子命令：{other}");
            std::process::exit(64);
        }
    }
}
