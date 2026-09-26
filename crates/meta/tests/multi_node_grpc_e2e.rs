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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use prost::Message as _;
use yuntun_meta::{MetaNode, MetaOptions};
use yuntun_proto::meta as pb;
use yuntun_proto::meta::meta_client::MetaClient;
use yuntun_meta::NodeHandle;

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

/// 起一个**进程内**节点 + 它的真 gRPC 服务任务。
///
/// `peers` = "**发往**各对端的地址"：直连时就是对端的监听地址；分区用例里指向**可切断的链路**
/// （[`Links`]）。这个参数化正是分区用例能成立的原因 —— **切链路不需要动任何生产代码**：
/// 节点只认 `peers` 里那个地址，我们把那个地址指到自己的转发器上即可。
///
/// `opts` = 运行期策略（快照用例把压缩阈值调小，让快照**由策略自然产生**）。
async fn spawn_node(
    id: u64,
    dir: PathBuf,
    peers: HashMap<u64, String>,
    listener: tokio::net::TcpListener,
    opts: MetaOptions,
) -> NodeProc {
    let node = MetaNode::open_with(dir, id, IDS.to_vec(), peers, opts).expect("起节点");
    // 起服务前：传输还没用过
    let st = node.transport_stats();
    assert_eq!(
        st.delivered + st.failed + st.rejected + st.dropped,
        0,
        "启动时不该已经发过消息"
    );
    let served = node.handle();
    let server = tokio::spawn(async move {
        let _ = yuntun_meta::serve(served, listener).await;
    });
    NodeProc {
        id,
        node: Some(node),
        server,
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
        if let Some(l) = leader_of(clients).await
            && live.contains(&l)
        {
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
        let listener = listeners[i].take().expect("监听器只取一次");
        nodes.push(spawn_node(*id, dir(*id), peers, listener, MetaOptions::default()).await);
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

// ---------------------------------------------------------------------------
// 网络分区（`operation-log §104`）：可切断的链路 ⇒ 少数派不能提交 + 愈合后不丢不裂
// ---------------------------------------------------------------------------

/// 一条**可切断的有向链路**：在 `addr` 上监听，把每条进来的连接转发到 `target`。
///
/// 为什么用"TCP 转发 + 开关"而不是 `iptables` / 网络命名空间：后者要 root、进不了 CI；
/// 而它**不需要动任何生产代码** —— 节点只认 `peers` 里那个地址，我们把它指向这里即可。
/// 断了还能自己接上，是因为 `GrpcTransport` 用 `connect_lazy` + tonic 自带重连
/// （`transport.rs` 模块文档里"对端没起来/重启中不需要我们写重连逻辑"就是这个意思）。
struct Link {
    addr: SocketAddr,
    open: Arc<AtomicBool>,
    /// 已建立的转发任务：切断时必须 `abort` 掉 —— 只拒绝新连接不够，**老连接还在替双方送消息**
    live: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    /// 被拒的连接数（**证据**：证明这个开关真被触发过，而不是"以为切了"）
    refused: Arc<AtomicU64>,
    _accept: tokio::task::JoinHandle<()>,
}

impl Link {
    async fn start(target: SocketAddr) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind 链路");
        let addr = listener.local_addr().expect("local_addr");
        let open = Arc::new(AtomicBool::new(true));
        let live: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));
        let refused = Arc::new(AtomicU64::new(0));
        let (o, l, r) = (open.clone(), live.clone(), refused.clone());
        let accept = tokio::spawn(async move {
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else {
                    break;
                };
                if !o.load(Ordering::SeqCst) {
                    // 断开：对端看到"连不上/连接被关"（= 链路断）
                    r.fetch_add(1, Ordering::SeqCst);
                    drop(inbound);
                    continue;
                }
                let h = tokio::spawn(async move {
                    if let Ok(mut outbound) = tokio::net::TcpStream::connect(target).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
                let mut v = l.lock().unwrap();
                v.retain(|h| !h.is_finished()); // 顺手回收，别让它无限长
                v.push(h);
            }
        });
        Self {
            addr,
            open,
            live,
            refused,
            _accept: accept,
        }
    }

    /// **切断**：关掉已建立的连接，并拒绝后续连接。
    fn cut(&self) {
        self.open.store(false, Ordering::SeqCst);
        for h in self.live.lock().unwrap().drain(..) {
            h.abort();
        }
    }

    fn heal(&self) {
        self.open.store(true, Ordering::SeqCst);
    }
}

/// 三个节点**两两有序**共六条链路（`i→j` 各一条）。
///
/// 为什么不是"每节点一条"（三条）：那样切"通往 j 的链路"会连**别的节点到 j** 一起切断，
/// 于是**多数派内部也断了**、根本选不出新 leader —— 就测不出"多数派照常工作"。
/// 有序对是必须的：`i→j` 与 `j→i` 是两条独立的路。
struct Links {
    map: HashMap<(u64, u64), Link>,
}

impl Links {
    async fn start(addrs: &HashMap<u64, SocketAddr>) -> Self {
        let mut map = HashMap::new();
        for i in IDS {
            for j in IDS {
                if i != j {
                    map.insert((i, j), Link::start(addrs[&j]).await);
                }
            }
        }
        Self { map }
    }

    fn addr(&self, from: u64, to: u64) -> SocketAddr {
        self.map[&(from, to)].addr
    }

    /// **隔离**某节点：切断**所有**与它相关的链路（两个方向），其余链路不动
    /// ⇒ 本例里 `(2,3)`/`(3,2)` 仍然通，多数派还能投票。
    fn isolate(&self, id: u64) {
        for ((i, j), l) in &self.map {
            if *i == id || *j == id {
                l.cut();
            }
        }
    }

    fn heal_all(&self) {
        for l in self.map.values() {
            l.heal();
        }
    }

    fn refused_total(&self) -> u64 {
        self.map
            .values()
            .map(|l| l.refused.load(Ordering::SeqCst))
            .sum()
    }
}

/// 等**某一个**节点自称 leader（`live` 限定候选，避免把被隔离的节点算进来）。
///
/// ⚠️ **不能**拿它当"集群的 leader 就是它"来用（那要 [`wait_settled_leader`]）：
/// 分区**愈合后**，被隔离过的旧 leader 会在自己那侧**仍然自认 leader**（它还没从多数派
/// 那里学到更高的 term）—— 这时候"我问到一个自称 leader 的"答的是旧答案。
async fn wait_some_leader(clients: &HashMap<u64, Client>, live: &[u64]) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        for id in live {
            if status(&clients[id], *id).await.role == "Leader" {
                return *id;
            }
        }
        assert!(Instant::now() < deadline, "30s 内没选出 leader");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

/// 等**所有** `ids` 就**同一个** leader 达成一致，返回它。
///
/// 与 [`wait_some_leader`] 的差别是愈合用例的必需品：只要还有节点"自认 leader"或"指着一个
/// 不是 leader 的 id"，就还不算settled —— 那个窗口里任何"leader 是谁"的结论都可能是旧答案。
async fn wait_settled_leader(clients: &HashMap<u64, Client>, ids: &[u64]) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut all = Vec::with_capacity(ids.len());
        for id in ids {
            all.push(status(&clients[id], *id).await);
        }
        let leader = all
            .iter()
            .find(|s| s.role == "Leader" && s.leader_id == s.node_id)
            .map(|s| s.node_id);
        if let Some(l) = leader
            && all
                .iter()
                .all(|s| s.leader_id == l && (s.role == "Leader") == (s.node_id == l))
        {
            return l;
        }
        assert!(
            Instant::now() < deadline,
            "30s 内没能就同一个 leader 达成一致：{all:?}"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

/// 向**被隔离**的节点提议一次，断言它在 `within` 内**没有被接受**。
///
/// 这是分区的**安全性**断言（脑裂防线）。三种表现都算通过：
/// - 它已自知不是 leader ⇒ 服务端回 `NotLeader`（映射成可重试的 `UNAVAILABLE`）；
/// - 它仍自认 leader，于是收下提案、append 进自己的日志，但**永远等不到应用**
///   （没有多数派 ⇒ 提交不了）⇒ 我们这边的 `within` 先超时（服务端自己等 `PROPOSE_TIMEOUT` = 10s）；
/// - 回执到了但 `accepted=false`。
///
/// **只有收到 `accepted=true` 才是错的** —— 那说明少数派自己提交了，脑裂成立。
async fn assert_cannot_commit(client: &Client, id: u64, op: pb::Op, within: Duration) {
    // 克隆要先落成变量：`client.clone().propose(..)` 的临时值活不过这个 `let`
    // （我们要把 future 存起来再交给 `timeout`，不是在原地 `.await`）
    let mut c = client.clone();
    let call = c.propose(pb::ProposeRequest {
        op: Some(op),
        request_id: b"rid".to_vec(),
        schema_ver: 0,
    });
    match tokio::time::timeout(within, call).await {
        Ok(Err(_)) => {} // 服务端回绝（NotLeader / NoQuorum）
        Ok(Ok(r)) => assert!(
            !r.into_inner().accepted,
            "被隔离的节点 {id} **接受了提交** —— 这就是脑裂"
        ),
        Err(_) => {} // 等不到回执：它收下了提案，但没有多数派、提交不了
    }
}

/// **网络分区**：把 leader 与另外两个节点**双向切断** ⇒
/// ① 少数派（被隔离者）**不能提交**（安全性：脑裂会让"换主不丢已提交"变成一句假话）；
/// ② 多数派（另两个）**选出新 leader 并照常提交**（可用性）；
/// ③ **愈合**后三方收敛：多数派在分区期间的提交**不丢**，少数派那条在途记录**不留**。
///
/// 形态（`i→j` 是"节点 i 发往节点 j"走的那条**可切断链路**）：
///
/// ```text
///   node1 ──link(1,2)──► 转发 ──► node2      切断 = 关掉已建立的连接 + 拒绝新连接
///   node1 ──link(1,3)──► 转发 ──► node3      隔离 1 ⇒ 切 (1,2)(1,3)(2,1)(3,1) 四条，
///   node2 ◄──link(2,3)──► 转发 ──► node3            **(2,3)/(3,2) 保持通畅 ⇒ 多数派还能选主**
/// ```
///
/// 客户端读 `Status` 走的是**直连**（不经过链路），所以分区期间仍能同时观察两边各自的状态
/// —— 这正是本用例能"对着两边分别断言"的原因。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitioned_leader_cannot_commit_and_heals_without_loss() {
    let root = TempDir::new("meta-partition");
    let dir = |id: u64| -> PathBuf { Path::new(&root.0).join(format!("node{id}")) };

    // ---- ① 监听器先 bind 且**不释放**（`:0` + 不松手 ⇒ 无 TOCTOU）----
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

    // ---- ② 六条链路；各节点的 peer 表指向**链路**而不是对端 ----
    let links = Links::start(&addrs).await;
    let mut nodes: Vec<NodeProc> = Vec::new();
    for (i, id) in IDS.iter().enumerate() {
        let peers: HashMap<u64, String> = IDS
            .iter()
            .filter(|o| *o != id)
            .map(|o| (*o, links.addr(*id, *o).to_string()))
            .collect();
        let listener = listeners[i].take().expect("监听器只取一次");
        nodes.push(spawn_node(*id, dir(*id), peers, listener, MetaOptions::default()).await);
    }

    let clients: HashMap<u64, Client> = {
        let mut m = HashMap::new();
        for id in IDS {
            m.insert(id, connect(addrs[&id]).await);
        }
        m
    };

    // ---- ③ 选主 ----
    let leader = wait_some_leader(&clients, &IDS).await;
    wait_leader_agreement(&clients, &IDS, leader).await;

    // ---- ④ 分区**之前**先提交一份：它要跨过"换主 + 分区 + 愈合"活下来 ----
    assert!(
        propose_following_leader(&clients, &IDS, create_schema_op("analytics", 1_000))
            .await
            .accepted
    );
    assert!(
        propose_following_leader(&clients, &IDS, create_table_op("cpu", 1_001))
            .await
            .accepted
    );
    let r1 = propose_following_leader(&clients, &IDS, commit_op("b1", "key-b1", 1_002)).await;
    assert!(r1.accepted, "分区前的提交必须成功");
    let ver_before = r1.manifest_ver;
    wait_converged(&clients, &IDS).await;

    // ---- ⑤ 反证：**未分区**时，同一个调用**必须被接受** ----
    //      没有这一条，⑦ 的"没被接受"什么都证明不了（可能是"这个调用天生就成不了"）。
    //      同一次运行里把"健康 ⇒ 接受"与"被隔离 ⇒ 不接受"都摆出来，⑦ 才有鉴别力。
    {
        let mut c = clients[&leader].clone();
        let r = c
            .propose(pb::ProposeRequest {
                op: Some(commit_op("probe", "key-probe", 1_500)),
                request_id: b"probe".to_vec(),
                schema_ver: 0,
            })
            .await
            .expect("健康集群里提议应当成功");
        assert!(
            r.into_inner().accepted,
            "未分区时 leader 必须接受提交（否则这条用例的'分区下不接受'毫无意义）"
        );
    }

    // ---- 切断 leader 的四条链路 ⇒ 它成了少数派 ----
    links.isolate(leader);
    let survivors: Vec<u64> = IDS.iter().copied().filter(|i| *i != leader).collect();
    let live: HashMap<u64, Client> = survivors
        .iter()
        .map(|id| (*id, clients[id].clone()))
        .collect();

    // ---- ⑥ 多数派：选出**新** leader 并照常提交（可用性）----
    let new_leader = wait_some_leader(&live, &survivors).await;
    assert_ne!(new_leader, leader, "被隔离的节点拉不到票，不可能当选");
    wait_leader_agreement(&live, &survivors, new_leader).await;
    let r2 = propose_following_leader(&live, &survivors, commit_op("b2", "key-b2", 2_000)).await;
    assert!(r2.accepted, "多数派（2/3）必须能提交 —— 少数派失联不该让集群停写");
    assert!(
        r2.manifest_ver > ver_before,
        "多数派的提交必须推进版本号（{ver_before} → {}）",
        r2.manifest_ver
    );

    // ---- ⑦ 少数派：**不能提交**（安全性，本用例的核心）----
    assert_cannot_commit(
        &clients[&leader],
        leader,
        commit_op("b3", "key-b3", 3_000),
        Duration::from_secs(3),
    )
    .await;

    // ---- ⑧ 证据：链路**真的**被切过（否则"少数派不能提交"可能只是因为别的原因）----
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let failed: u64 = nodes
            .iter()
            .map(|n| n.node.as_ref().expect("节点还活着").transport_stats().failed)
            .sum();
        let refused = links.refused_total();
        if failed > 0 && refused > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "10s 内没拿到「链路被切断」的证据：failed={failed} refused={refused}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ---- ⑨ 愈合 ----
    links.heal_all();

    // ---- ⑩ 收敛：三方同一条日志（被隔离者的分歧尾部会被新 leader 覆盖）----
    //      用 `wait_settled_leader`（三方就同一个 leader 一致），**不能**用 `wait_some_leader`：
    //      愈合后原 leader 会在自己那侧仍自认 leader 一小会儿（它还没学到更高 term），
    //      那时"我问到一个自称 leader 的"会答出旧答案 —— 这是本用例自己踩过的坑。
    let healed_leader = wait_settled_leader(&clients, &IDS).await;
    assert_ne!(
        healed_leader, leader,
        "被隔离过的旧 leader 不可能仍然是 leader（它的 term 落后）"
    );
    let converged = wait_converged(&clients, &IDS).await;
    // `wait_converged` 要求**每个**节点 `applied == last` 且三者的 `last_index` 相同 ⇒
    // 被隔离者那条「append 了但没提交」的记录要么被覆盖、要么它压根没 append：
    // 两种情况的共同点是「它日志里没有未应用（= 未提交）的尾巴」，而「提交了什么」由下面三条断言钉住。
    let first_last = converged[&IDS[0]];
    assert!(
        converged.values().all(|v| *v == first_last),
        "愈合后三方必须同一条日志，实际：{converged:?}"
    );

    // ---- ⑪ 分区期间多数派的提交：**不丢**（在被隔离者身上也能重放命中）----
    let replay_b2 = propose_following_leader(&clients, &IDS, commit_op("b2", "key-b2", 2_000)).await;
    assert!(
        !replay_b2.accepted,
        "分区期间多数派提交的记录丢了 —— 这正是「愈合后少数派把多数派的成果顶掉」的失败模样"
    );

    // ---- ⑫ 少数派那条在途记录：**不该**被提交过 ⇒ 现在提交必须能成功 ----
    // 这条是"没有脑裂"的正向证据：当时若被接受过，这里就会命中幂等键（accepted=false）。
    let b3 = propose_following_leader(&clients, &IDS, commit_op("b3", "key-b3", 3_000)).await;
    assert!(
        b3.accepted,
        "少数派那条在分区期间**不该**生效；现在提交成功 ⇒ 它确实没被提交（无脑裂）"
    );

    // ---- ⑬ 分区**之前**那条也还在 ----
    let replay_b1 = propose_following_leader(&clients, &IDS, commit_op("b1", "key-b1", 1_002)).await;
    assert!(!replay_b1.accepted, "分区前提交的记录跨过换主 + 分区丢了");

    // 收尾：停掉三个节点（`Drop` 会停）
    for n in nodes {
        drop(n);
    }
}

// ---------------------------------------------------------------------------
// 压缩触发策略（`operation-log §105`，`plan T11.5` 第三项的前置）
// ---------------------------------------------------------------------------

/// **压缩由策略自然触发**（设计 §4.4：日志条数 > N）—— 这是"快照"这一整套机制的第一环。
///
/// # 为什么是单节点形态
///
/// 多节点 + 极小阈值会踩到一个**尚未根因**的写停摆（`§105.3`：阈值 4 + 有 follower 落后 ⇒
/// 集群 30s 写不进去；已排除传输层、缓存 leader、raft 线程 fatal 等，复现步骤见那一节）。
/// 在把它查清之前，本用例只在**单节点**形态下钉住"策略真的会触发"这一条 ——
/// 它可证、可复现，且**不依赖**那条坏路径。
///
/// # 反证
///
/// 把 `compact_log_entries` 设成 0（或不设策略）⇒ `snapshot_index` 恒为 0 ⇒ 这条断言有鉴别力。
#[test]
fn snapshot_trigger_compacts_the_log_by_policy() {
    let root = TempDir::new("meta-compact");
    let dir = Path::new(&root.0).join("node1");
    let opts = MetaOptions {
        compact_log_entries: 4,
        ..MetaOptions::default()
    };
    let node = MetaNode::open_with(&dir, 1, vec![1], HashMap::new(), opts).expect("起单节点");
    // 用 `NodeHandle`（= gRPC 服务层用的那个句柄）：它同时给 Status 与 Propose
    let h = node.handle();

    // 单节点自选不需要网络；等它就位（`open` 起来时已经是 leader，这里只是不赌时序）
    let deadline = Instant::now() + Duration::from_secs(10);
    while h.status().role != "Leader" {
        assert!(Instant::now() < deadline, "单节点 10s 内没当选");
        std::thread::sleep(Duration::from_millis(10));
    }

    // 建 schema/表（`commit_op` 要求表已存在）
    for op in [
        create_schema_op("analytics", 1_000),
        create_table_op("cpu", 1_001),
    ] {
        let r = h.propose(op, Duration::from_secs(5)).expect("提议应成功");
        assert!(r.accepted, "DDL 必须被接受");
    }

    // 写够条数 ⇒ 按阈值（4）触发压缩
    for k in 0..10u64 {
        let r = h
            .propose(
                commit_op(&format!("c{k}"), &format!("key-c{k}"), 2_000 + k),
                Duration::from_secs(5),
            )
            .expect("提议应成功");
        assert!(r.accepted, "第 {k} 条必须被接受（压缩不该影响写入）");
    }

    let st = h.status();
    assert!(
        st.snapshot_index > 0,
        "阈值 4、写了 12 条 op，压缩**必须**已经发生（`snapshot_index` 仍为 0 ⇒ 触发策略没生效）"
    );
    assert!(
        st.applied_index >= st.snapshot_index,
        "压缩位置不该超过已应用位置（`storage.rs` 的坐标纪律：只能压已应用的）"
    );
    assert_eq!(
        st.first_index,
        st.snapshot_index + 1,
        "`first_index` 必须紧跟压缩位置（两者是同一个坐标，`storage.rs` 的坐标纪律）"
    );
}

/// **回归用例**（原是 `§106` 的复现探针；`§111` 修好后按纪律转正、拿掉 `#[ignore]`）：
/// 小压缩阈值 + 有 follower 落后 ⇒ 小批量提交必须**一直**能被接受。
///
/// # 它当初复现的是什么（留档）
///
/// k=0..4 全绿，**k=5 起 30s 拿不到被接受的响应**：leader `last` 前进而 `commit` 卡住，
/// 目标 peer 的 `next` **远超** `matched`，且**只发空 append**；follower 对每条 append 都回
/// `MsgAppendResponse reject=false`（窗口内 306 条响应、**0 条 reject**）；传输层
/// `failed=0/rejected=0`；没有 panic，也没有任何 snapshot 尝试。
///
/// # 真凶与修法（`§110` 定位、`§111` 修）
///
/// 真凶**不是** raft，而是**传输层出站队列的队头阻塞**：一条 FIFO 队列里，
/// **过时的心跳/空追加堵在队头**（队头那条还要等满 2s 的 RPC 超时），于是**唯一携带条目的那条
/// append 一次都没出去**（实测：raft 交出 537 条、`send_loop` 只出队 298 条，那条救场的 append
/// 到达次数 = **0**）。`§107` 讲的"乐观推进的 `next_idx` 再也补不上"是**后果**：那条 append 一旦
/// 出不去，follower 就永远缺它，而 leader 只会发空 append、follower 恰好能接受 ⇒ 停摆。
///
/// 修法：出站分**两条队列**（携带条目的 = 重要；心跳/空追加 = 可取代，容量 1），
/// `send_loop` 用 `biased` **优先发重要的** —— 见 `transport.rs` 的模块文档"两条队列"。
///
/// # 出问题时的观察配方
///
/// ```text
/// YUNTUN_META_TRACE=1 RUST_LOG=raft=debug cargo test -p yuntun-meta \
///     --test multi_node_grpc_e2e -- --nocapture
/// ```
///
/// - `RUST_LOG=raft=debug`：raft-rs 的 `default_logger()` 是 `slog_envlogger`，一直可用；
/// - `YUNTUN_META_TRACE=1`：`[meta:send]`（raft 交出什么）/ `[meta:transport]`（真正出队什么）/
///   入站 / `entries()` / 每秒一行的 `Progress` 视图快照（含 `paused` 与 inflight）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_with_lagging_follower_should_keep_committing() {
    const THRESHOLD: usize = 4;
    let root = TempDir::new("meta-stall-regression");
    let dir = |id: u64| -> PathBuf { Path::new(&root.0).join(format!("node{id}")) };
    let opts = MetaOptions {
        compact_log_entries: THRESHOLD,
        ..MetaOptions::default()
    };

    let mut listeners = Vec::new();
    for _ in IDS {
        listeners.push(Some(
            tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind"),
        ));
    }
    let addrs: HashMap<u64, SocketAddr> = IDS
        .iter()
        .zip(&listeners)
        .map(|(id, l)| (*id, l.as_ref().unwrap().local_addr().unwrap()))
        .collect();
    let links = Links::start(&addrs).await;
    let peers_of = |id: u64| -> HashMap<u64, String> {
        IDS.iter()
            .filter(|o| **o != id)
            .map(|o| (*o, links.addr(id, *o).to_string()))
            .collect()
    };
    let mut nodes: Vec<NodeProc> = Vec::new();
    for (i, id) in IDS.iter().enumerate() {
        let listener = listeners[i].take().expect("只取一次");
        nodes.push(spawn_node(*id, dir(*id), peers_of(*id), listener, opts.clone()).await);
    }
    let clients: HashMap<u64, Client> = {
        let mut m = HashMap::new();
        for id in IDS {
            m.insert(id, connect(addrs[&id]).await);
        }
        m
    };

    let leader = wait_some_leader(&clients, &IDS).await;
    wait_leader_agreement(&clients, &IDS, leader).await;
    assert!(
        propose_following_leader(&clients, &IDS, create_schema_op("analytics", 1_000))
            .await
            .accepted
    );
    assert!(
        propose_following_leader(&clients, &IDS, create_table_op("cpu", 1_001))
            .await
            .accepted
    );
    wait_converged(&clients, &IDS).await;

    // 隔离一个 follower（不是 leader）
    let victim = IDS.iter().copied().find(|i| *i != leader).expect("有 follower");
    links.isolate(victim);
    let majority: Vec<u64> = IDS.iter().copied().filter(|i| *i != victim).collect();
    let live: HashMap<u64, Client> = majority
        .iter()
        .map(|id| (*id, clients[id].clone()))
        .collect();
    eprintln!("复现：隔离 follower {victim}；leader={leader}；阈值={THRESHOLD}");

    for k in 0..12u64 {
        let op = commit_op(&format!("s{k}"), &format!("key-s{k}"), 2_000 + k);
        let cur = wait_some_leader(&live, &majority).await;
        let mut c = live[&cur].clone();
        let call = c.propose(pb::ProposeRequest {
            op: Some(op),
            request_id: b"rid".to_vec(),
            schema_ver: 0,
        });
        let accepted = match tokio::time::timeout(Duration::from_secs(12), call).await {
            Ok(Ok(r)) => r.into_inner().accepted,
            _ => false,
        };
        if !accepted {
            for id in IDS {
                eprintln!("  节点 {id}: {:?}", status(&clients[&id], id).await);
            }
            for n in &nodes {
                let s = n.node.as_ref().unwrap().transport_stats();
                eprintln!(
                    "  节点 {} 传输: delivered={} failed={} rejected={} dropped={} queued={}",
                    n.id, s.delivered, s.failed, s.rejected, s.dropped, s.queued
                );
            }
            panic!("④ 第 {k} 条提交失败（12s 内没被接受）");
        }
        let st = status(&clients[&cur], cur).await;
        eprintln!(
            "  ④ k={k} ok：leader={cur} first={} last={} applied={} commit={}",
            st.first_index, st.last_index, st.applied_index, st.commit_index
        );
        let _ = cur;
    }

    for n in nodes {
        drop(n);
    }
}


// ---------------------------------------------------------------------------
// 成员变更（`§118`）：在线加一个 learner
// ---------------------------------------------------------------------------

/// **集群能在线加一个 learner，且它的地址随 conf change 复制到每个成员。**
///
/// 为什么这几条断言就够了：learner **不参与多数派** ⇒ "加它"这件事**不需要那个节点真的存在**
/// （它还不存在 —— 这正是"在线加节点"的起点：集群先接纳它，它再来追平，见 `§119`）。
/// 所以这一刀能在**进程内**验：
///
/// 1. leader 把 9 号加为 learner（提议一条 `ConfChange`，地址放在它的 `context` 里）；
/// 2. 那条 conf change 提交后，**三方**的成员表都要认它（`voters` 不变、`learners = [9]`）；
/// 3. 而且三方的**地址表**里都有它的地址 —— 地址是随 `context` **复制**过去的，
///    不是"只有当初那个 leader 知道"（换主之后新 leader 也得连得上它，所以这一条必须测）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adding_a_learner_is_replicated_with_its_address() {
    const LEARNER: u64 = 9;
    const LEARNER_ADDR: &str = "127.0.0.1:19999";

    let root = TempDir::new("meta-add-learner");
    let dir = |id: u64| -> PathBuf { Path::new(&root.0).join(format!("node{id}")) };

    // ---- ① 先 bind（拿到端口且不释放：`§37` 那个 TOCTOU）----
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
        .map(|(id, l)| (*id, l.as_ref().expect("已 bind").local_addr().expect("addr")))
        .collect();

    // ---- ② 起三个节点 ----
    let mut nodes: Vec<NodeProc> = Vec::new();
    for (i, id) in IDS.iter().enumerate() {
        let peers: HashMap<u64, String> = IDS
            .iter()
            .filter(|o| *o != id)
            .map(|o| (*o, addrs[o].to_string()))
            .collect();
        let listener = listeners[i].take().expect("监听器只取一次");
        nodes.push(spawn_node(*id, dir(*id), peers, listener, MetaOptions::default()).await);
    }

    // ---- ③ 等出 leader（**读句柄的 status**，不猜）----
    let leader = {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let found = nodes.iter().find(|n| {
                n.node
                    .as_ref()
                    .map(|m| m.handle().status().role == "Leader")
                    .unwrap_or(false)
            });
            if let Some(n) = found {
                break n.id;
            }
            assert!(Instant::now() < deadline, "10s 内没有 leader");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    eprintln!("  leader = {leader}");

    // ---- ④ leader 提议：把 9 号加为 learner ----
    let handle_of = |id: u64| -> NodeHandle {
        nodes
            .iter()
            .find(|n| n.id == id)
            .expect("节点在")
            .node
            .as_ref()
            .expect("节点活着")
            .handle()
    };
    handle_of(leader)
        .add_learner(LEARNER, LEARNER_ADDR, Duration::from_secs(5))
        .expect("提议加 learner 应当成功");

    // ---- ⑤ 等**三方**成员表都认下它（含地址）----
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut not_yet: Vec<u64> = Vec::new();
        for n in &nodes {
            let mv = handle_of(n.id)
                .members(Duration::from_secs(2))
                .expect("members 命令");
            let has_addr = mv
                .addrs
                .get(&LEARNER)
                .map(|a| a == LEARNER_ADDR)
                .unwrap_or(false);
            if mv.learners != vec![LEARNER] || !has_addr {
                not_yet.push(n.id);
            }
        }
        if not_yet.is_empty() {
            break;
        }
        if Instant::now() >= deadline {
            panic!("10s 内这些节点的成员表还没认下 learner {LEARNER}：{not_yet:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // ---- ⑥ 顺带：voter 集合**一个都不该变**（learner 不参与多数派）----
    for n in &nodes {
        let mv = handle_of(n.id)
            .members(Duration::from_secs(2))
            .expect("members 命令");
        assert_eq!(mv.voters, vec![1, 2, 3], "加 learner 不该动 voter 集合");
        eprintln!(
            "  节点{}: voters={:?} learners={:?} addrs[9]={:?}",
            n.id,
            mv.voters,
            mv.learners,
            mv.addrs.get(&LEARNER)
        );
    }

    // ---- ⑦ 提升的三道门槛（`§120`）：逐条试一遍 ----
    //
    // 这里能把"落后太多"测出来，靠的是**给 learner 一个永远追不上的处境**：
    // `LEARNER_ADDR` 指向没人监听的端口 ⇒ 它的 `matched` **永远**是 0（`§118` 刻意用了个假地址）。
    // 再垫够 > `PROMOTE_MAX_LAG`(64) 条，日志末尾与它拉开距离，门槛就会拦。
    let leader = {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(id) = IDS
                .iter()
                .copied()
                .find(|id| handle_of(*id).status().role == "Leader")
            {
                break id;
            }
            assert!(Instant::now() < deadline, "10s 内没有 leader");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    let h = handle_of(leader);
    for k in 0..80u64 {
        h.propose(
            commit_op(&format!("pad{k}"), &format!("k-pad{k}"), 5_000 + k),
            Duration::from_secs(5),
        )
        .unwrap_or_else(|e| panic!("垫高日志的写入 k={k} 应当被接受：{e:?}"));
    }
    let e = h
        .promote(LEARNER, Duration::from_secs(5))
        .expect_err("落后太多的 learner 不该被提升");
    assert!(
        format!("{e:?}").contains("落后"),
        "拒绝理由要说清**是落后**（不是别的错）：{e:?}"
    );
    h.promote(leader, Duration::from_secs(5))
        .expect("已经是 voter ⇒ 幂等成功");
    let e = h
        .promote(77, Duration::from_secs(5))
        .expect_err("不在成员表里的节点不该被提升");
    assert!(
        format!("{e:?}").contains("不在成员表"),
        "拒绝理由要说清**是不在册**：{e:?}"
    );
    eprintln!("  ⑦ 三道门槛都按预期：落后 → 拒；已是 voter → 幂等；不在册 → 拒");
}
