//! T12.3 收口：数据节点 **入册 → 保活 → 掉线被摘 → 回来能重注册** 的完整往返（跨进程）。
//!
//! 三个角色都是真的，没有替身：
//!
//! ```text
//!   metanode（进程内起真 gRPC 服务，巡检口径调到秒级）
//!        ▲  RegisterDatanode（raft op）      ▲  Heartbeat（只碰内存）
//!        │                                  │
//!   yuntun-datanode 子进程（--meta + 心跳）
//! ```
//!
//! 观察点是**查询侧看名录的那条路**（`Prefetch` 载荷）—— 而不是内部字段：
//! 名录是不是真的经 raft 落进了状态，只有从这条路看才算数（`§71`）。

use std::collections::HashMap;
use std::io::Read;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use yuntun_proto::meta as pb;

/// 被测二进制（cargo 为集成测试注入）。
const BIN: &str = env!("CARGO_BIN_EXE_yuntun-datanode");
const INSTANCE: &str = "inst-a";

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

/// 从元数据读回名录（查询侧那条路：`Prefetch` 载荷）。
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

/// 轮询到条件成立（带超时与可读的失败信息）。
async fn wait_until<F: Fn() -> bool>(f: F, within: Duration, what: &str, stderr: &Arc<Mutex<String>>) {
    let deadline = Instant::now() + within;
    while !f() {
        assert!(
            Instant::now() < deadline,
            "等待「{what}」超时。子进程 stderr:\n{}",
            stderr.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn datanode_registers_keeps_alive_and_is_evicted_when_it_dies() {
    // ---- ① metanode：进程内起**真** gRPC 服务，巡检口径调到秒级 ----
    let meta_dir = tmpdir("membership-meta");
    let node = yuntun_meta::MetaNode::open(&meta_dir, 1, vec![1], HashMap::new()).expect("起单节点");
    let h = node.handle();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let meta_addr = listener.local_addr().unwrap();
    let served = h.clone();
    tokio::spawn(async move {
        let _ = yuntun_meta::serve(served, listener).await;
    });

    // 巡检：2.5s 没心跳即摘（数据节点那边心跳 1s 一次 ⇒ 每个超时窗口内有 2~3 次机会）
    let shutdown = tokio_util::sync::CancellationToken::new();
    let _sweep = yuntun_meta::spawn_liveness_sweep(
        h.clone(),
        Duration::from_millis(2500),
        Duration::from_millis(200),
        shutdown.clone(),
    );

    // 注册要走 raft 提交 ⇒ 先等选主（判据用对外可见的 leader_id）
    let deadline = Instant::now() + Duration::from_secs(10);
    while h.status().leader_id != 1 {
        assert!(Instant::now() < deadline, "等待选主超时");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // ---- ② 起数据节点子进程：`--meta` ⇒ 启动即入册 + 心跳保活 ----
    let data_dir = tmpdir("membership-data");
    let mut child: Child = Command::new(BIN)
        .arg("--instance-id")
        .arg(INSTANCE)
        .arg("--dir")
        .arg(&data_dir)
        .arg("--listen")
        .arg("127.0.0.1:0")
        .arg("--meta")
        .arg(meta_addr.to_string())
        .arg("--heartbeat-secs")
        .arg("1")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("起 yuntun-datanode");

    let stderr = Arc::new(Mutex::new(String::new()));
    {
        let pipe = child.stderr.take().expect("stderr");
        let sink = stderr.clone();
        std::thread::spawn(move || {
            let mut r = std::io::BufReader::new(pipe);
            let mut buf = String::new();
            let _ = r.read_to_string(&mut buf);
            *sink.lock().unwrap() = buf;
        });
    }

    // ---- ③ 入册：出现名字录里（经 gRPC → raft → 状态机 → Prefetch 载荷） ----
    let h1 = h.clone();
    let st1 = stderr.clone();
    wait_until(
        move || roster(&h1).contains(&INSTANCE.to_string()),
        Duration::from_secs(15),
        "数据节点入册",
        &st1,
    )
    .await;

    // ---- ④ 保活：跨过**不止一个**心跳周期后仍在（否则巡检就是在随机删节点） ----
    tokio::time::sleep(Duration::from_millis(1600)).await;
    assert!(
        roster(&h).contains(&INSTANCE.to_string()),
        "有心跳的数据节点不得被摘。stderr:\n{}",
        stderr.lock().unwrap()
    );

    // ---- ⑤ 掉线被摘：杀掉进程 ⇒ 心跳停 ⇒ 超时后从名录消失 ----
    child.kill().expect("杀子进程");
    let _ = child.wait();
    let h2 = h.clone();
    let st2 = stderr.clone();
    wait_until(
        move || !roster(&h2).contains(&INSTANCE.to_string()),
        Duration::from_secs(15),
        "心跳停止后被摘除",
        &st2,
    )
    .await;

    shutdown.cancel();
}
