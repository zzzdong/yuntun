//! **成员变更运维面**（`§128`）的契约：三条**只有这层才管**的事。
//!
//! 提升/移除的**语义**（门槛、幂等、conf change 走 raft）已经由服务端那侧用例守着
//! （`meta/tests/*` 的 `§120`/`§121`）；这里只验"**包成运维命令**"引入的那几条：
//!
//! 1. **接触点逐个试**：第一个地址连不上 ⇒ 换下一个（运维手里只有地址，没有 leader id）；
//! 2. **真拒绝不被藏**：服务端的判据（"不在成员表里"）必须**原样上抛**，
//!    而不是被"再试试别的节点"盖掉 —— 后者会让运维一直试到放弃却不知道为什么；
//! 3. **幂等**：移除一个本来就不在册的 id 成功（那正是移除的目的）—— 命令层面也得看得到。
//!
//! 服务端是**真的**：起一个单节点 metanode + 真的 gRPC 服务（不是 mock）。

use std::collections::HashMap;
use std::time::Duration;

use yuntun_client::admin::{self, AdminAction, is_retryable};

/// 起一个单节点 metanode 服务，返回 `(地址, 节点)`。
///
/// ⚠️ **必须把 `MetaNode` 还回去**：驱动线程跟着它的邮箱活着，丢了它 = 驱动线程退出
/// （现象是 `Status` 还答得出来 —— 它读的是快照 —— 而 `Promote` 报"命令通道断开"。
/// 本用例第一次就是踩在这上面）。
async fn serve_single_node(
    dir: &std::path::Path,
) -> (std::net::SocketAddr, yuntun_meta::MetaNode) {
    let node = yuntun_meta::MetaNode::open_with(
        dir,
        1,
        vec![1],
        HashMap::new(),
        yuntun_meta::MetaOptions::default(),
    )
    .expect("起单节点 metanode");
    let handle = node.handle();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    // 服务随测试进程退出而结束（不显式 abort：与其它用例同款）
    tokio::spawn(async move {
        let _ = yuntun_meta::serve(handle, listener).await;
    });
    // **就绪判据要严一点**：连接建立 ≠ 服务就绪，也 ≠ 节点的诊断快照已经刷过一轮。
    // 这里等到"节点能报出自己的成员表"为止 —— 否则第一个断言可能在跟一个还没准备好的
    // 快照赛跑（`Status` 的成员表由驱动循环每轮刷新，冷启动头几毫秒是空的）。
    let contact = addr.to_string();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        // 两件事都要等：① 诊断快照已刷过一轮（成员表非空）；② **已经选出 leader** ——
        // 前者来自盘上的 conf state（比选举早得多），只等它会让 `Promote` 撞上"还没选出来"。
        if let Ok((ms, _)) =
            admin::run(AdminAction::Members, std::slice::from_ref(&contact), Duration::from_secs(2))
                .await
            && !ms.voters.is_empty()
            && ms.leader_id.is_some_and(|l| l != 0)
        {
            return (addr, node);
        }
        assert!(std::time::Instant::now() < deadline, "10s 内服务没起来（或成员表一直空）");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contacts_are_tried_in_order_and_real_rejections_are_not_hidden() {
    let dir = yuntun_testkit::TestDir::tmpfs("cli-admin");
    let (real, _node) = serve_single_node(dir.path()).await; // `_node` 必须活到测试结束

    // ---- ① 第一个接触点连不上 ⇒ 换下一个，并且**明确回报用了谁** ----
    // `127.0.0.1:1` 基本必然是拒绝连接（这不是"等超时"，是把失败路径走一遍）
    let contacts = vec!["127.0.0.1:1".to_string(), real.to_string()];
    let (ms, via) = admin::run(AdminAction::Members, &contacts, Duration::from_secs(5))
        .await
        .expect("应当换到第二个接触点并成功");
    assert_eq!(via, real.to_string(), "必须回报**实际受理的**那个接触点");
    assert_eq!(ms.voters, vec![1], "单节点成员表：{ms}");
    assert!(ms.learners.is_empty(), "{ms}");

    // ---- ② 真拒绝原样上抛：提升一个**不在成员表里**的 id ----
    let err = admin::run(AdminAction::Promote(9), &[real.to_string()], Duration::from_secs(5))
        .await
        .expect_err("提升不在册的节点必须失败");
    assert!(
        err.contains("不在成员表"),
        "必须**把服务端的判据原样带出来**（而不是'没有任何接触点受理'）：{err}"
    );
    assert!(
        !err.contains("没有任何接触点受理"),
        "真拒绝被当成'没人受理'了 —— 这正是这条纪律要防的：{err}"
    );

    // ---- ③ 幂等：移除一个本来就不在册的 id ⇒ 成功（那就是移除的目的）----
    let (ms, _) = admin::run(AdminAction::Remove(9), &[real.to_string()], Duration::from_secs(5))
        .await
        .expect("移除不在册的 id 是幂等成功");
    assert_eq!(ms.voters, vec![1], "幂等操作不该改动成员表：{ms}");
}

/// 判据本身：**只有 `UNAVAILABLE` 算"没人受理"**。
///
/// 单独测它是因为它决定上面那条纪律的方向 —— 弄错两个方向都很糟。
#[test]
fn only_unavailable_counts_as_no_one_accepted() {
    use tonic::Status;
    assert!(is_retryable(&Status::unavailable("not leader, hint=2")));
    for st in [
        Status::invalid_argument("节点 9 不在成员表里"),
        Status::deadline_exceeded("成员变更未在 10s 内生效"),
        Status::internal("存储故障"),
        Status::resource_exhausted("写不动"),
    ] {
        assert!(
            !is_retryable(&st),
            "{} 不该被当成'没人受理'（那会把真拒绝藏起来）",
            st.code()
        );
    }
}
