//! S3-1 选型闸门用例（raft-rs）。
//!
//! 判据见 `crates/meta/src/lib.rs` 文件头。这两个用例必须证明：
//! **三节点能选主、能收敛、kill leader 后已提交数据不丢，且状态机接缝不需要为 raft 改动**。
//!
//! 注意（**诚实边界**）：跑的是 `MemStorage` + 进程内消息传递，所以
//! "落盘/崩溃恢复"不在本轮范围（那是 S3-3 的 fjall 后端 + S3-1b 的快照安装）。

use std::time::Duration;

use yuntun_meta::{PocOp, PEERS};

const T: Duration = Duration::from_secs(10);

fn ops() -> Vec<PocOp> {
    vec![
        PocOp::CreateSchema {
            name: "analytics".into(),
            now_ms: 1_000,
        },
        PocOp::CreateTable {
            name: "cpu".into(),
            now_ms: 1_001,
        },
        PocOp::Commit {
            table: "public.cpu".into(),
            batch_id: "b1".into(),
            rows: 10,
            now_ms: 1_002,
        },
        PocOp::Commit {
            table: "public.cpu".into(),
            batch_id: "b2".into(),
            rows: 20,
            now_ms: 1_003,
        },
    ]
}

/// 判据 1 + 3：三节点选主成功，提议的 op 在**所有节点**上收敛到同一状态，
/// 且状态机（`CatalogState`）原样被驱动 —— 没有为 raft 加任何分支。
#[test]
fn three_node_cluster_converges_on_proposed_ops() {
    let cluster = yuntun_meta::Cluster::start();
    let leader = cluster
        .wait_leader(T)
        .expect("三节点应在超时内选出 leader");

    for op in ops() {
        cluster.propose(op, T).expect("leader 在位时提议应成功");
    }

    // 收敛：每个节点的规范编码必须**逐字节相同**（这就是"副本一致"的口径）
    //
    // ⚠️ 等待条件是**语义的**（状态里出现第 2 个文件），不是 `applied >= 4`：
    // raft 的 **entry index ≠ 已应用 op 数** —— leader 就位会先写一条**空 no-op 条目**
    // 占掉 index 1，于是"4 个 op"对应 index 2..5。第一版按索引等待，在 follower
    // 只应用了 3 个 op（index=4）时就提前 break，得到"副本未收敛"的**假失败**。
    // 这个坑值得记：凡是把 raft index 当业务进度用的地方都会错位。
    let mut canon = Vec::new();
    for id in PEERS {
        let deadline = std::time::Instant::now() + T;
        loop {
            let c = cluster.canonical(id).expect("节点应存在");
            let text = String::from_utf8_lossy(&c);
            if text.contains("file b2") || std::time::Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        canon.push((id, cluster.canonical(id).expect("节点应存在")));
    }
    for (id, c) in &canon {
        assert_eq!(
            c, &canon[0].1,
            "节点 {id} 的状态与节点 {} 不一致 —— raft 副本未收敛（比对规范编码）",
            canon[0].0
        );
    }
    // 语义断言（不止编码相等：要确认 op 真的落进了状态机）
    let c0 = &canon[0].1;
    let text = String::from_utf8_lossy(c0);
    assert!(text.contains("ns analytics"), "create_schema 未生效：{text}");
    assert!(text.contains("table public.cpu"), "create_table 未生效");
    assert!(text.contains("file b1") && text.contains("file b2"), "commit_files 未生效");
    assert_eq!(leader, cluster.wait_leader(T).unwrap(), "leader 应保持稳定");
}

/// 判据 2：kill leader 后**重新选主**，且此前已提交的数据**一条不少**。
///
/// 这是 R3 的核心承诺（G2：写入不中断）在 raft 层的证据：
/// 提交 = 多数派持久化，所以杀掉一个节点不会丢已提交数据。
#[test]
fn leader_kill_reelects_and_keeps_committed_ops() {
    let mut cluster = yuntun_meta::Cluster::start();
    let old_leader = cluster.wait_leader(T).expect("先选出 leader");
    for op in ops() {
        cluster.propose(op, T).expect("初始 4 个 op 应提交");
    }
    assert_eq!(cluster.alive(), 3);

    // 杀掉 leader（线程退出 + 从路由表移除 = 它再也收不到消息）
    cluster.kill(old_leader);
    assert_eq!(cluster.alive(), 2, "剩两个节点 = 多数派仍在");

    let survivors: Vec<u64> = PEERS.iter().copied().filter(|id| *id != old_leader).collect();
    // 重新选主：必须由**存活节点**当选
    let deadline = std::time::Instant::now() + T;
    let new_leader = loop {
        if let Some(l) = cluster.wait_leader(Duration::from_millis(200)) {
            assert!(
                survivors.contains(&l),
                "被杀的节点 {l} 不可能当选（它已收不到消息）"
            );
            break l;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "kill leader 后未在超时内重新选主 —— 可用性承诺不成立"
        );
    };
    assert_ne!(new_leader, old_leader);

    // 新 leader 必须能继续接活（写入不中断）
    cluster
        .propose(
            PocOp::Commit {
                table: "public.cpu".into(),
                batch_id: "b3".into(),
                rows: 30,
                now_ms: 1_004,
            },
            T,
        )
        .expect("新 leader 必须能继续提交");

    // 已提交数据（含换主前的 b1/b2）在两个存活节点上都必须在
    for id in survivors {
        let deadline = std::time::Instant::now() + T;
        let text = loop {
            let c = cluster.canonical(id).expect("存活节点");
            let text = String::from_utf8_lossy(&c).to_string();
            // 同样按**语义**等待（b3 出现 = 新 leader 的提交已被本节点应用）
            if text.contains("file b3") || std::time::Instant::now() >= deadline {
                break text;
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        for b in ["b1", "b2", "b3"] {
            assert!(
                text.contains(&format!("file {b}")),
                "节点 {id} 丢了已提交数据 {b} —— raft 多数派语义未生效"
            );
        }
    }
}
