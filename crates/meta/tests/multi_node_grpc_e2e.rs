//! 3 节点 **gRPC 传输**端到端（S3-3 收尾）：真网络路径 + 换主不丢已提交。
//!
//! # 与其它用例的分工（三层，互不替代）
//!
//! | 用例 | 传输 | 证的是 |
//! |---|---|---|
//! | `raft_poc.rs` | 进程内 `mpsc` | raft 机制本身（选主/收敛/快照） |
//! | **本文件** | **真 gRPC（loopback TCP）** | **网络路径**：序列化、成员寻址、对端不可达、3 节点复制、**换主不丢已提交** |
//! | `metanode_process_e2e.rs` | 真进程（单节点） | 启动路径 + `kill -9` 后从盘恢复 |
//!
//! # 端口为什么不用猜
//!
//! 先 `bind(127.0.0.1:0)` 拿到内核分配的端口、**且不释放**，再让节点用它起服务 ——
//! 全程没有"探测空闲端口再交给别人"的 TOCTOU 竞态（`metanode` 的启动路径不能这样，
//! 因为它要把地址写进别的节点的 `--peer`，只能由部署方指定）。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use prost::Message as _;
use yuntun_meta::MetaNode;
use yuntun_proto::meta as pb;
use yuntun_proto::meta::meta_client::MetaClient;

const IDS: [u64; 3] = [1, 2, 3];

type Client = MetaClient<tonic::transport::Channel>;

// ---------------------------------------------------------------- 夹具

struct TempDir(PathBuf);
impl TempDir {
    fn new(tag: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!("yuntun-{tag}-{nanos}"));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct NodeProc {
    id: u64,
    /// `MetaNode::shutdown` 吃 `self`，所以用 `Option` 让 `Drop` 能把它取出来
    node: Option<MetaNode>,
    server: tokio::task::JoinHandle<()>,
}

/// **杀死**本节点：停 raft 线程 + 停 gRPC 服务。
///
/// 用 `Drop` 实现（而不是 `kill(self)`）：用例中途断言失败时也能停干净，
/// 否则泄漏的 raft 线程会继续跑、让后续用例/退出变得不确定。
impl Drop for NodeProc {
    fn drop(&mut self) {
        if let Some(n) = self.node.take() {
            n.shutdown();
        }
        self.server.abort();
    }
}

// ---------------------------------------------------------------- op 构造

fn create_schema_op(name: &str, now: u64) -> pb::Op {
    pb::Op {
        now_ms: now,
        kind: Some(pb::op::Kind::CreateSchema(pb::CreateSchemaOp {
            name: name.into(),
        })),
    }
}

fn create_table_op(name: &str, now: u64) -> pb::Op {
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("ts", arrow::datatypes::DataType::Int64, false),
    ]));
    pb::Op {
        now_ms: now,
        kind: Some(pb::op::Kind::CreateTable(pb::CreateTableOp {
            name: name.into(),
            namespace: "public".into(),
            arrow_schema_ipc: yuntun_model::meta::serialize_schema(&schema),
            default_format: "parquet".into(),
            partition_cols: vec![],
            ingest_config: yuntun_model::meta::IngestConfig::standard().encode_to_vec(),
        })),
    }
}

fn commit_op(batch_id: &str, key: &str, now: u64) -> pb::Op {
    pb::Op {
        now_ms: now,
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

// ---------------------------------------------------------------- 客户端辅助

async fn connect(addr: SocketAddr) -> Client {
    let ep = format!("http://{addr}");
    for _ in 0..200 {
        if let Ok(c) = MetaClient::connect(ep.clone()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("连不上节点：{ep}");
}

async fn status(c: &Client, id: u64) -> pb::StatusResponse {
    c.clone()
        .status(pb::StatusRequest {})
        .await
        .unwrap_or_else(|e| panic!("节点 {id} 的 Status 失败：{e}"))
        .into_inner()
}

/// 找出当前的 leader（**从各节点的 Status 里读**，而不是靠客户端猜）。
async fn leader_of(clients: &HashMap<u64, Client>) -> Option<u64> {
    for (id, c) in clients {
        let st = status(c, *id).await;
        if st.role == "Leader" {
            return Some(*id);
        }
    }
    None
}

/// 等所有 `ids` 里的节点都同意 leader 是 `want`（**共识**，不是"我问到了一个"）。
async fn wait_leader_agreement(clients: &HashMap<u64, Client>, ids: &[u64], want: u64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let all = {
            let mut ok = true;
            for id in ids {
                let st = status(&clients[id], *id).await;
                ok &= st.role == if *id == want { "Leader" } else { "Follower" };
                ok &= st.leader_id == want;
            }
            ok
        };
        if all {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "30s 内没能就 leader={want} 达成一致（实际：{:?}）",
            futures::future::join_all(ids.iter().map(|id| status(&clients[id], *id))).await
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

/// 向**当前** leader 提议（自动跟随 leader 变化：写失败就重新问谁是 leader 再试）。
/// 这正是"写入不中断"对客户端的要求 —— 客户端不该缓存 leader。
async fn propose_following_leader(
    clients: &HashMap<u64, Client>,
    live: &[u64],
    op: pb::Op,
) -> pb::ProposeResponse {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(l) = leader_of(clients).await {
            if live.contains(&l) {
                let r = clients[&l]
                    .clone()
                    .propose(pb::ProposeRequest {
                        op: Some(op.clone()),
                        request_id: b"rid".to_vec(),
                        schema_ver: 0,
                    })
                    .await;
                if let Ok(resp) = r {
                    return resp.into_inner();
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "30s 内没能写入（没有 leader 或一直失败）"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// 等 `ids` 里的节点**都**追平（`applied == last`），返回它们的 `last_index`。
async fn wait_converged(
    clients: &HashMap<u64, Client>,
    ids: &[u64],
) -> HashMap<u64, u64> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut last = HashMap::new();
        let mut settled = true;
        for id in ids {
            let st = status(&clients[id], *id).await;
            settled &= st.applied_index == st.last_index;
            last.insert(*id, st.last_index);
        }
        let same = last.values().all(|v| *v == last[&ids[0]]);
        if settled && same {
            return last;
        }
        assert!(
            Instant::now() < deadline,
            "30s 内未收敛：{last:?}（同一份已提交日志应当让各节点 last_index 相同）"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

// ---------------------------------------------------------------- 用例

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_replicate_over_grpc_and_survive_leader_loss() {
    let root = TempDir::new("meta-net");
    let dir = |id: u64| -> PathBuf { Path::new(&root.0).join(format!("node{id}")) };

    // ---- ① 先 bind 三个监听器（拿到端口**且不释放**，避免 TOCTOU）----
    // `Option` 是为了下面能**移出**（`TcpListener` 不可 `Copy`：一个监听器只能服务一次）
    let mut listeners = Vec::new();
    for _ in IDS {
        listeners.push(Some(
            tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind 127.0.0.1:0"),
        ));
    }
    let addrs: HashMap<u64, SocketAddr> = IDS
        .iter()
        .zip(&listeners)
        .map(|(id, l)| (*id, l.as_ref().expect("已 bind").local_addr().expect("local_addr")))
        .collect();

    // ---- ② 起三个节点（peer 表 = 别的两个节点）+ 三个 gRPC 服务 ----
    let mut nodes: Vec<NodeProc> = Vec::new();
    for (i, id) in IDS.iter().enumerate() {
        let peers: HashMap<u64, String> = IDS
            .iter()
            .filter(|o| *o != id)
            .map(|o| (*o, addrs[o].to_string()))
            .collect();
        let node = MetaNode::open(dir(*id), *id, IDS.to_vec(), peers).expect("起节点");
        // 起服务前：传输还没用过
        let st = node.transport_stats();
        assert_eq!(
            st.delivered + st.failed + st.rejected + st.dropped,
            0,
            "启动时不该已经发过消息"
        );
        let served = node.handle();
        let listener = listeners[i].take().expect("监听器只取一次");
        let server = tokio::spawn(async move {
            let _ = yuntun_meta::serve(served, listener).await;
        });
        nodes.push(NodeProc {
            id: *id,
            node: Some(node),
            server,
        });
    }

    let clients: HashMap<u64, Client> = {
        let mut m = HashMap::new();
        for id in IDS {
            m.insert(id, connect(addrs[&id]).await);
        }
        m
    };

    // ---- ③ 选主 + 达成一致 ----
    let deadline = Instant::now() + Duration::from_secs(30);
    let leader = loop {
        if let Some(l) = leader_of(&clients).await {
            break l;
        }
        assert!(Instant::now() < deadline, "30s 内没选出 leader");
        tokio::time::sleep(Duration::from_millis(30)).await;
    };
    wait_leader_agreement(&clients, &IDS, leader).await;

    // ---- ④ 写：3 个 op 经过**网络复制**到多数派 ----
    let r_schema = propose_following_leader(&clients, &IDS, create_schema_op("analytics", 1_000)).await;
    assert!(r_schema.accepted);
    assert!(propose_following_leader(&clients, &IDS, create_table_op("cpu", 1_001)).await.accepted);
    let r_commit = propose_following_leader(&clients, &IDS, commit_op("b1", "key-b1", 1_002)).await;
    assert!(r_commit.accepted);
    let manifest_ver = r_commit.manifest_ver;

    // ---- ⑤ 收敛：三个节点的已提交索引一致（这就是"副本一致"在进程外的口径）----
    let last_before = wait_converged(&clients, &IDS).await;

    // ---- ⑥ **网络路径确实被用到**（否则这条链路的证据是假的）----
    // 统计来自各个节点的**出站**传输：至少 leader 必须成功送达过 follower。
    let mut delivered_total = 0u64;
    for n in &nodes {
        delivered_total += n.node.as_ref().unwrap().transport_stats().delivered;
    }
    assert!(
        delivered_total > 0,
        "没有任何消息被对端确认收到 —— 说明复制不是走 gRPC 完成的"
    );

    // ---- ⑦ 杀掉 leader（停它的 raft 线程与服务）----
    let dead = leader;
    let dead_node = nodes.remove(
        nodes
            .iter()
            .position(|n| n.id == dead)
            .expect("leader 在节点表里"),
    );
    drop(dead_node);
    let survivors: Vec<u64> = IDS.iter().copied().filter(|o| *o != dead).collect();
    let live_clients: HashMap<u64, Client> = survivors
        .iter()
        .map(|id| (*id, clients[id].clone()))
        .collect();

    // ---- ⑧ 存活节点**重新选出** leader（换主不中断）----
    let deadline = Instant::now() + Duration::from_secs(30);
    let new_leader = loop {
        if let Some(l) = leader_of(&live_clients).await {
            assert_ne!(l, dead, "死掉的节点不可能再是 leader");
            break l;
        }
        assert!(Instant::now() < deadline, "30s 内没选出新 leader");
        tokio::time::sleep(Duration::from_millis(30)).await;
    };
    wait_leader_agreement(&live_clients, &survivors, new_leader).await;

    // ---- ⑨ 换主后仍能写（M3 的"写入不中断"）----
    let r_new = propose_following_leader(&live_clients, &survivors, commit_op("b2", "key-b2", 2_000)).await;
    assert!(r_new.accepted, "换主后必须还能写入");
    assert!(r_new.manifest_ver > manifest_ver, "新提交必须推进 manifest_ver");

    // ---- ⑩ 换主前**已提交**的数据不丢：同幂等键重放必须命中 ----
    // 这条最硬：幂等记录只存在于**状态机**里，而状态机是靠"重放已提交日志"重建的。
    // 命中 = 换主前的提交在新 leader 上确实还在（不是"日志里在、状态机里没有"）。
    let replay = propose_following_leader(&live_clients, &survivors, commit_op("b1", "key-b1", 1_002)).await;
    assert!(
        !replay.accepted,
        "换主后重放旧的幂等键必须命中（accepted=false）—— 否则说明已提交数据丢了"
    );
    assert_eq!(
        replay.manifest_ver, r_new.manifest_ver,
        "重放不该改变版本号（幂等命中 = 状态没变）"
    );

    // ---- ⑪ 存活节点收敛到同一份日志，且**不落后于换主前** ----
    let last_after = wait_converged(&live_clients, &survivors).await;
    for id in &survivors {
        assert!(
            last_after[id] >= last_before[id],
            "节点 {id} 的日志倒退了：换主前 {} → 现在 {}",
            last_before[id],
            last_after[id]
        );
    }

    // 收尾：停掉存活节点（`Drop` 会停；显式 drop 让"谁在什么时候停"一目了然）
    for n in nodes {
        drop(n);
    }
}
