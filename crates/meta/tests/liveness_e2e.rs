//! T12.3 下半第 2 步：**心跳保活 + 超时摘除**（心跳不进 raft，摘除走 raft）。
//!
//! 用**进程内**的真节点（`MetaNode::open` 单节点 = 天然 leader）而不是子进程：
//! 要验的是"心跳 → 存活判定 → 超时 → 经 raft 摘除"这条语义链，跨进程传输另有
//! `metanode_process_e2e` 覆盖。

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use yuntun_proto::meta as pb;

fn tmpdir(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "yuntun-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn register_op(id: &str, addr: &str) -> pb::Op {
    pb::Op {
        now_ms: yuntun_model::batch::now_ms(),
        kind: Some(pb::op::Kind::RegisterDatanode(pb::RegisterDatanodeOp {
            instance_id: id.to_string(),
            address: addr.to_string(),
        })),
    }
}

/// 从元数据读回名录（走的就是查询侧那条路：`Prefetch` 的载荷）。
fn roster(h: &yuntun_meta::NodeHandle) -> Vec<String> {
    let resp = h.prefetch(&pb::PrefetchRequest {
        since_schema_ver: 0,
        since_manifest_ver: 0,
        full: true,
        tables: Vec::new(),
        since_snapshot: 0,
    });
    resp.payload
        .map(|p| p.datanodes.into_iter().map(|m| m.instance_id).collect())
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeat_keeps_a_datanode_alive_and_silence_evicts_it() {
    let dir = tmpdir("liveness");
    let node = yuntun_meta::MetaNode::open(&dir, 1, vec![1], HashMap::new()).expect("起单节点");
    let h = node.handle();

    // ⓪ 等选主：单节点也要走完一次选举才能提交（否则提议会 `NoQuorum`）。
    //    判据用**对外**可见的 `leader_id`（1 = 本节点）；这也是客户端判断该找谁的方式。
    let deadline = Instant::now() + Duration::from_secs(10);
    while h.status().leader_id != 1 {
        assert!(Instant::now() < deadline, "等待选主超时");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // ① 注册（走 raft 的 op）⇒ 进名录
    h.propose(register_op("inst-a", "10.0.0.7:50051"), Duration::from_secs(5))
        .expect("注册应提交");
    assert_eq!(roster(&h), vec!["inst-a".to_string()]);

    // ② 心跳的返回值：在名录里 = true；不在 = false（**它就是"该重新注册"的信号**）
    assert!(h.heartbeat("inst-a"), "名录里的实例 ⇒ known=true");
    assert!(
        !h.heartbeat("ghost"),
        "不在名录里的实例 ⇒ known=false：调用方应重新注册，而不是继续空发心跳"
    );

    // ③ 起巡检：超时 300ms / 每 50ms 巡检一次
    let shutdown = tokio_util::sync::CancellationToken::new();
    let _sweep = yuntun_meta::spawn_liveness_sweep(
        h.clone(),
        Duration::from_millis(300),
        Duration::from_millis(50),
        shutdown.clone(),
    );

    // ④ 持续心跳 ⇒ 活得很好（这是"保活"那一半）
    for _ in 0..8 {
        assert!(h.heartbeat("inst-a"));
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    assert_eq!(
        roster(&h),
        vec!["inst-a".to_string()],
        "有心跳的成员不得被摘除（否则巡检就是在随机删节点）"
    );

    // ⑤ 停心跳 ⇒ 超时后被摘（**经 raft**：状态真的变了，不只是内存标记）
    let deadline = Instant::now() + Duration::from_secs(10);
    while roster(&h).contains(&"inst-a".to_string()) {
        assert!(
            Instant::now() < deadline,
            "等待心跳超时摘除超时（超时 300ms + 巡检 50ms）"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // ⑥ 摘除之后再心跳 ⇒ known=false（于是它会重新注册 —— 这条让"摘除"可恢复）
    assert!(
        !h.heartbeat("inst-a"),
        "被摘除的实例再心跳应得到 known=false"
    );

    shutdown.cancel();
}
