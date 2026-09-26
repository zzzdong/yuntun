//! **3 个真 `metanode` 进程**的端到端（`operation-log §103`）：进程边界 + 部署接线 + 换主 + 重启追平。
//!
//! # 与 `multi_node_grpc_e2e.rs` 的分工（不是重复）
//!
//! | 用例 | 节点形态 | 证的是 |
//! |---|---|---|
//! | `raft_poc.rs` | 进程内节点 + 进程内 `mpsc` 传输 | raft 机制本身（选主/收敛/快照） |
//! | `multi_node_grpc_e2e.rs` | **同进程内**三个 `MetaNode` + 三个真 gRPC 服务 | 网络路径（序列化、寻址、3 节点复制、换主不丢已提交） |
//! | `metanode_process_e2e.rs` | **真进程**，但**单节点** | 启动路径 + `kill -9` 后从盘恢复 |
//! | **本文件** | **真进程 × 3** | 上面两个各自证不了的三件事：① 每个节点自己的目录/存储/端口（`MetaNode::open` 各写各的）；② `--voters/--peer/--init` 这套**部署接线**真的能把集群组起来；③ **杀掉一个进程再从盘恢复并重新加入**（进程内测试里"重启"只是一次函数调用） |
//!
//! # 端口为什么必须"先知道"（这条用例此前没做的原因）
//!
//! `--peer` 要把**对端地址在启动前**交给每个节点，而 `MetaNode::open` 会拒绝"成员表里有节点、
//! 却没给它地址"（`cli::normalize` 同样会拦）。所以三个节点的端口只能由**部署方指定** ——
//! `multi_node_grpc_e2e.rs` 正是为了绕开这点才用"进程内节点 + 已 bind 的 listener"。
//!
//! **`§119` 起这条路有正门了**：`metanode --join <接触点列表>` 让新节点**在线加入** ——
//! 集群不用预先知道它（地址随 `Join` 请求进去、随 conf change 复制给每个成员），
//! 它也不用预先知道集群里所有地址（`Join` 回包给，**含 leader 自己**）。
//! 本文件最后一条用例 `a_fourth_process_joins_a_running_cluster_and_catches_up` 证的就是它；
//! 上面那段"端口必须先知道"因此只剩**初始 3 个 voter** 需要（它们确实在 `--init` 时就要配好）。
//!
//! 于是这里：`bind(127.0.0.1:0)` 取三个端口后**立刻释放**再交给子进程。这是 `§37` 批评过的
//! TOCTOU（"探测空闲端口再交给别人"），本用例用**整组重试**兜住那个极小窗口，并把这件事写在
//! 这里而不是假装它不存在。
//!
//! # 本用例顺出来的两处**真缺陷**（已在 `§103` 修掉）
//!
//! 它第一次跑就红了，而且红得很有价值 —— 3 个真进程**一个都活不下来**：
//!
//! 1. **启动顺序反了**：`metanode` 当时是「① `open` → ② 等选主 → ③ 起 gRPC 服务」，而
//!    raft 选主要靠节点间**互相投票**、投票走的就是那个服务 ⇒ 谁都在等别人的票、谁的服务
//!    都还没起，三个进程各自等到 10s 超时**集体退出**。单节点组踩不到（自己一票就够）。
//! 2. **判据用错**：启动闸门调的是 `MetaNode::wait_leader`（**"等本节点成为 leader"**），
//!    而 raft 同一时刻只有一个 leader ⇒ follower 永远等不到"自己当选"。多节点要用
//!    `wait_any_leader`（"等集群里有 leader"）。
//!
//! 附带一条**部署约束**：每个节点都要等「集群里有 leader」才打印接口行，所以编排必须
//! **并行起**（或在 10s 窗口内起齐）——"起一个等一个"会僵住。

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use prost::Message as _;
use yuntun_proto::meta as pb;
use yuntun_proto::meta::meta_client::MetaClient;

/// 被测二进制（cargo 为集成测试注入的路径变量；名字 = `[[bin]] name`）。
const BIN: &str = env!("CARGO_BIN_EXE_metanode");
const IDS: [u64; 3] = [1, 2, 3];

type Client = MetaClient<tonic::transport::Channel>;

// ---------------------------------------------------------------- 临时目录

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

// ---------------------------------------------------------------- 子进程夹具

struct Proc {
    id: u64,
    listen: SocketAddr,
    child: Child,
    /// stdout 由**线程 + 通道**喂过来：`read_line` 会阻塞，而"进程卡住不打印接口行"必须变成
    /// **超时**（`None`）而不是把 CI 挂死 —— 真进程用例最容易踩的就是这个。
    lines: std::sync::mpsc::Receiver<String>,
    stderr: Arc<Mutex<String>>,
}

impl Proc {
    /// 读 stdout 直到 `listening on <addr>`（metanode 的**接口行**）。
    ///
    /// `None` = 超时，或进程已退出（发送端断开）—— 两者都表示"这个节点没起来"，
    /// 交由调用方决定重试还是失败。
    fn wait_listening(&self) -> Option<SocketAddr> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return None;
            }
            let line = match self.lines.recv_timeout(left) {
                Ok(l) => l,
                Err(_) => return None,
            };
            if let Some(rest) = line.split("listening on ").nth(1) {
                let addr: SocketAddr = rest
                    .split_whitespace()
                    .next()
                    .expect("接口行里应当有地址")
                    .parse()
                    .expect("监听地址应当可解析");
                // 我们**指定**了端口（`--peer` 要求地址先知道），所以它必须绑在指定端口上；
                // 否则"对端按 --peer 里的地址连过来"就是错的。
                assert_eq!(
                    addr, self.listen,
                    "节点 {} 必须绑在预先分配的 {}（否则 --peer 里的地址是错的）",
                    self.id, self.listen
                );
                return Some(addr);
            }
        }
    }

    /// **真 SIGKILL**（不是优雅退出）：确认换主/重启走的是崩溃恢复路径。
    fn kill9(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// 断言失败也不能留孤儿进程（真进程用例的基本纪律）。
impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// 起一个真进程。`init` 只在**首次**启动时给：空目录必须给、有数据的目录必须不给
/// （`cli::check_bootstrap` 的两条安全闸门，搞错会静默各自成组）。
fn start(
    id: u64,
    dir: &Path,
    listen: SocketAddr,
    peers: &HashMap<u64, SocketAddr>,
    init: bool,
) -> Proc {
    // 只列**别的**节点（列了自己会被忽略，见 `cli::normalize`）
    let peer_csv = peers
        .iter()
        .filter(|(o, _)| **o != id)
        .map(|(o, a)| format!("{o}@{a}"))
        .collect::<Vec<_>>()
        .join(",");

    let mut cmd = Command::new(BIN);
    cmd.arg("--id")
        .arg(id.to_string())
        .arg("--dir")
        .arg(dir)
        .arg("--listen")
        .arg(listen.to_string())
        .arg("--voters")
        .arg(
            IDS.iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(","),
        )
        .arg("--peer")
        .arg(peer_csv)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if init {
        cmd.arg("--init");
    }
    let mut child = cmd.spawn().expect("启动 metanode 真进程");

    let stdout = child.stdout.take().expect("stdout");
    let (tx, lines) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut r = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match r.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    if tx.send(line.clone()).is_err() {
                        break;
                    }
                }
            }
        }
    });
    // stderr 后台收集：失败时能整段打出来（否则只剩"没起来"三个字）
    let stderr = Arc::new(Mutex::new(String::new()));
    let sink = stderr.clone();
    let mut e = child.stderr.take().expect("stderr");
    std::thread::spawn(move || {
        let mut s = String::new();
        let _ = e.read_to_string(&mut s);
        *sink.lock().unwrap() = s;
    });

    Proc {
        id,
        listen,
        child,
        lines,
        stderr,
    }
}

/// 取 `n` 个**当前空闲**的端口：`bind(127.0.0.1:0)` 拿内核分配的端口后**立刻释放**
/// （listener 在这里 drop）。见文件头"端口为什么必须先知道"。
fn free_ports(n: usize) -> Vec<SocketAddr> {
    (0..n)
        .map(|_| {
            let l = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
            l.local_addr().expect("local_addr")
        })
        .collect()
}

struct Cluster {
    procs: HashMap<u64, Proc>,
    addrs: HashMap<u64, SocketAddr>,
    /// 本次尝试的目录（重试时换新的：**部分启动过的节点已经写过自己的目录**，
    /// 而 `--init` 对"已有数据"的目录会被拒绝）
    run: PathBuf,
}

/// 起三个真进程；某个节点没打印接口行就退出 ⇒ **整组重试**（换新端口 + 新目录）。
fn start_cluster(root: &Path) -> Cluster {
    for attempt in 1..=5 {
        let run = root.join(format!("run{attempt}"));
        let addrs: HashMap<u64, SocketAddr> =
            IDS.iter().copied().zip(free_ports(IDS.len())).collect();
        // **先把三个都起起来，再等接口行** —— 不能"起一个等一个"：成员表是 [1,2,3]，
        // 而每个节点都要等「集群里有 leader」才打印接口行，leader 又需要**多数派投票**
        // ⇒ 串行起会僵住（第一个节点的票永远凑不齐，因为后面的还没起）。
        // 这也是部署约束：**所有节点要在 10s 窗口内起来**（`§103`）。
        let mut procs: HashMap<u64, Proc> = HashMap::new();
        for id in IDS {
            let dir = run.join(format!("node{id}"));
            procs.insert(id, start(id, &dir, addrs[&id], &addrs, true));
        }
        let mut ok = true;
        for id in IDS {
            if procs[&id].wait_listening().is_none() {
                eprintln!(
                    "第 {attempt} 次尝试：节点 {id} 没在 20s 内就绪 —— 整组重试。stderr:\n{}",
                    procs[&id].stderr.lock().unwrap()
                );
                ok = false;
                break;
            }
        }
        if ok {
            return Cluster { procs, addrs, run };
        }
        // 已经起来的那些随 `procs` 一起 drop ⇒ 被杀掉，不留孤儿
    }
    panic!("5 次尝试都没能把三个真 metanode 进程组起来");
}

// ---------------------------------------------------------------- op 构造
//
// 与 `multi_node_grpc_e2e.rs` 同形（proto `Op` 是唯一权威编码）。

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

async fn connect_all(addrs: &HashMap<u64, SocketAddr>, ids: &[u64]) -> HashMap<u64, Client> {
    let mut m = HashMap::new();
    for id in ids {
        m.insert(*id, connect(addrs[id]).await);
    }
    m
}

async fn status(c: &Client, id: u64) -> pb::StatusResponse {
    c.clone()
        .status(pb::StatusRequest {})
        .await
        .unwrap_or_else(|e| panic!("节点 {id} 的 Status 失败：{e}"))
        .into_inner()
}

/// 等**某一个**节点自称 leader（`live` 限定候选，避免把死掉的节点算进来）。
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

/// 等**所有** `ids` 就 leader 达成一致（共识，不是"我问到了一个"）。
async fn wait_leader_agreement(clients: &HashMap<u64, Client>, ids: &[u64], want: u64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut ok = true;
        for id in ids {
            let st = status(&clients[id], *id).await;
            ok &= st.role == if *id == want { "Leader" } else { "Follower" };
            ok &= st.leader_id == want;
        }
        if ok {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "30s 内没能就 leader={want} 达成一致：{:?}",
            futures::future::join_all(ids.iter().map(|id| status(&clients[id], *id))).await
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

/// 向**当前** leader 提议（自动跟随 leader 变化：写失败就重新问谁是 leader 再试）。
async fn propose_following_leader(
    clients: &HashMap<u64, Client>,
    live: &[u64],
    op: pb::Op,
) -> pb::ProposeResponse {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let leader = {
            let mut found = None;
            for id in live {
                if status(&clients[id], *id).await.role == "Leader" {
                    found = Some(*id);
                    break;
                }
            }
            found
        };
        if let Some(l) = leader {
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

/// 等 `ids` **都**追平（`applied == last`）**且** `last_index` 相同，返回它们各自的
/// `(applied, last)`。`last_index` 相同 = 三份 raft 日志是同一条。
async fn wait_converged(
    clients: &HashMap<u64, Client>,
    ids: &[u64],
    what: &str,
) -> HashMap<u64, (u64, u64)> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut seen = HashMap::new();
        let mut settled = true;
        for id in ids {
            let st = status(&clients[id], *id).await;
            settled &= st.applied_index == st.last_index;
            seen.insert(*id, (st.applied_index, st.last_index));
        }
        let first_last = seen[&ids[0]].1;
        let same_log = seen.values().all(|(_, last)| *last == first_last);
        if settled && same_log {
            return seen;
        }
        assert!(
            Instant::now() < deadline,
            "30s 内「{what}」未收敛：{seen:?}"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

// ---------------------------------------------------------------- 用例

/// **3 个真进程组集群**：起 → 选主 → 经网络提交 → **杀 leader** → 重选 + 仍能写 →
/// **重启被杀者**（同端口/同目录/不带 `--init`）→ 从盘恢复并追平。
#[test]
fn three_real_processes_elect_replicate_and_survive_leader_kill_and_restart() {
    let root = TempDir::new("meta3-proc");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");

    // ---- ① 三个真进程起得来 ----
    let mut cluster = start_cluster(&root.0);
    eprintln!("三个 metanode 真进程已起：{:?}", cluster.addrs);

    let clients = rt.block_on(async { connect_all(&cluster.addrs, &IDS).await });

    // ---- ② 选主，且三个进程**就同一个人**达成一致 ----
    let leader = rt.block_on(async { wait_some_leader(&clients, &IDS).await });
    rt.block_on(async { wait_leader_agreement(&clients, &IDS, leader).await });
    eprintln!("leader = {leader}（三个进程一致）");

    // ---- ③ 经**网络**复制：3 个 op（DDL 也走 raft）→ 三个进程都收敛 ----
    let (manifest_ver, before) = rt.block_on(async {
        let r =
            propose_following_leader(&clients, &IDS, create_schema_op("analytics", 1_000)).await;
        assert!(r.accepted, "建 schema 必须被接受");
        let r = propose_following_leader(&clients, &IDS, create_table_op("cpu", 1_001)).await;
        assert!(r.accepted, "建表必须被接受");
        let r = propose_following_leader(&clients, &IDS, commit_op("b1", "key-b1", 1_002)).await;
        assert!(r.accepted, "提交必须被接受");
        let converged = wait_converged(&clients, &IDS, "提交后三进程收敛").await;
        (r.manifest_ver, converged)
    });
    for id in IDS {
        assert_eq!(
            before[&id].0, before[&id].1,
            "节点 {id} 自己没追平（applied != last）"
        );
    }

    // ---- ④ 杀掉 leader（真 SIGKILL）→ 存活两个重选 ----
    let dead = leader;
    let mut dead_proc = cluster.procs.remove(&dead).expect("leader 在进程表里");
    dead_proc.kill9();
    let survivors: Vec<u64> = IDS.iter().copied().filter(|i| *i != dead).collect();
    let live: HashMap<u64, Client> = survivors
        .iter()
        .map(|id| (*id, clients[id].clone()))
        .collect();

    let new_leader = rt.block_on(async { wait_some_leader(&live, &survivors).await });
    assert_ne!(new_leader, dead, "死掉的进程不可能再是 leader");
    rt.block_on(async { wait_leader_agreement(&live, &survivors, new_leader).await });
    eprintln!("杀 {dead} 后，存活两个选出新 leader = {new_leader}");

    // ---- ⑤ 换主后仍能写（M3 的"写入不中断"），且**已提交的不丢** ----
    let after = rt.block_on(async {
        let r = propose_following_leader(&live, &survivors, commit_op("b2", "key-b2", 2_000)).await;
        assert!(r.accepted, "换主后必须还能写入");
        assert!(
            r.manifest_ver > manifest_ver,
            "新提交必须推进 manifest_ver（{manifest_ver} → {}）",
            r.manifest_ver
        );
        // 最硬的一条：幂等记录**只存在于状态机**里，而状态机是靠"重放已提交日志"重建的。
        // 命中（accepted=false）= 换主前那份提交在新 leader 上确实还在，不是"日志里在、状态机里没有"。
        let replay = propose_following_leader(&live, &survivors, commit_op("b1", "key-b1", 1_002)).await;
        assert!(
            !replay.accepted,
            "换主后重放旧的幂等键必须命中（accepted=false）—— 否则说明已提交数据丢了"
        );
        assert_eq!(
            replay.manifest_ver, r.manifest_ver,
            "幂等命中 = 状态没变，版本号不该动"
        );
        wait_converged(&live, &survivors, "换主后存活的两个收敛").await
    });
    for id in &survivors {
        assert!(
            after[id].1 >= before[id].1,
            "节点 {id} 的日志倒退了：换主前 {} → 现在 {}",
            before[id].1,
            after[id].1
        );
    }

    // ---- ⑥ **重启被杀者**：同端口、同目录、**不带 `--init`**（那次是重启，不是新建集群）----
    //      这是"真进程组"独有的：进程内测试里"重启"只是一次函数调用，验不到"从盘恢复 + 重新加入"。
    //      "不带 `--init` 还能起来"本身就是证据：启动闸门对**空目录**会退 2（`cli::check_bootstrap`），
    //      所以它能起来 ⇒ 盘上那份成员表/日志被复用了（走的是"重启"路径）。
    let dir = cluster.run.join(format!("node{dead}"));
    let revived = start(dead, &dir, cluster.addrs[&dead], &cluster.addrs, false);
    assert!(
        revived.wait_listening().is_some(),
        "重启的节点必须在**同一端口**上重新监听（存活者的 --peer 里是那个地址）。stderr:\n{}",
        revived.stderr.lock().unwrap()
    );
    cluster.procs.insert(dead, revived);

    // ---- ⑦ 它从盘恢复并追平：三个进程再次收敛到同一条日志 ----
    let clients_all = rt.block_on(async { connect_all(&cluster.addrs, &IDS).await });
    // 重启的节点要先经过一轮选举/追日志；这里等**三个**都 applied==last 且 last_index 相同
    let after_restart =
        rt.block_on(async { wait_converged(&clients_all, &IDS, "重启后三进程再收敛").await });
    for id in IDS {
        assert_eq!(
            after_restart[&id].0, after_restart[&id].1,
            "节点 {id} 未追平（applied != last）"
        );
    }
    assert_eq!(
        after_restart[&dead].1, after[&survivors[0]].1,
        "重启的节点必须追到与存活者**同一条日志**（否则它是靠自己的旧盘在自说自话）"
    );
    // 幂等键在重启后依然命中 ⇒ 换主 + 重启这一整轮之后，"重启前提交的记录"仍在**集群**里
    // （说明：follower 恢复有两条合法路径 —— 从盘重放自己的日志、或由 leader 补发；
    //  从外面**观测不到**是哪一条，也不该硬断言某一条。这里断言的是结果不丢。）
    let replay_after_restart = rt.block_on(async {
        propose_following_leader(&clients_all, &IDS, commit_op("b1", "key-b1", 1_002)).await
    });
    assert!(
        !replay_after_restart.accepted,
        "重启后重放旧幂等键必须仍然命中 —— 否则说明换主 + 重启把已提交的记录弄丢了"
    );

    // 收尾：显式停掉三个进程（`Drop` 会再兜一次，无害）
    for id in IDS {
        if let Some(p) = cluster.procs.get_mut(&id) {
            p.kill9();
        }
    }
}

// ---------------------------------------------------------------------------
// 动态成员变更：`--join`（`§119`）
// ---------------------------------------------------------------------------

/// 起一个**加入模式**的进程：不给 `--voters` / `--peer` / `--init`，只给 `--join` 与 `--advertise`。
///
/// 这正是 `cli` 那两道启动闸门要放行的意图（`check_bootstrap`）：**空目录 + 没给 `--init`
/// + 给了 `--join`** —— 前两条单独出现时恰恰是被拒绝的组合。
fn start_join(
    id: u64,
    dir: &Path,
    listen: SocketAddr,
    contacts: &str,
    advertise: SocketAddr,
) -> Proc {
    let mut cmd = Command::new(BIN);
    cmd.arg("--id")
        .arg(id.to_string())
        .arg("--dir")
        .arg(dir)
        .arg("--listen")
        .arg(listen.to_string())
        .arg("--advertise")
        .arg(advertise.to_string())
        .arg("--join")
        .arg(contacts)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("启动 metanode 真进程（--join）");

    let stdout = child.stdout.take().expect("stdout");
    let (tx, lines) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut r = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match r.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    if tx.send(line.clone()).is_err() {
                        break;
                    }
                }
            }
        }
    });
    let stderr = Arc::new(Mutex::new(String::new()));
    let sink = stderr.clone();
    let mut e = child.stderr.take().expect("stderr");
    std::thread::spawn(move || {
        let mut s = String::new();
        let _ = e.read_to_string(&mut s);
        *sink.lock().unwrap() = s;
    });

    Proc {
        id,
        listen,
        child,
        lines,
        stderr,
    }
}

/// **第 4 个进程 `--join` 进一个正在跑的 3 节点集群，并追上（含追上之后的新写入）。**
///
/// # 它证的三件事（进程内测不出来 —— 那三个用例的成员表都是写死的）
///
/// 1. **集群不用预先知道新节点**：`--voters`/`--peer` 里都没有它。它的地址随 `Meta.Join`
///    的**请求**进去，再随 conf change **复制**给每个成员（`§118` 的 `ConfChange.context`）；
/// 2. **新节点不用预先知道所有地址**：成员表与各节点地址全部来自 `Join` **回包**
///    （含 leader 自己 —— 否则它连 `AppendResponse` 都发不回去）；
/// 3. **启动闸门对"加入"这个意图放行**：新节点是"空目录 + 没给 `--init` + 给了 `--join`"，
///    而 `check_bootstrap` 恰好把前两条单独出现判成"忘了 --init"。
///
/// # 为什么只用 3 个接触点里任一个都能成
///
/// 只有 leader 受理成员变更，非 leader 回 `NotLeader` + hint，而 **hint 是 id 不是地址** ——
/// 所以 `--join` 收的是**接触点列表**（本用例把三个地址都给它），逐个试到 leader 为止。
#[test]
fn a_fourth_process_joins_a_running_cluster_and_catches_up() {
    let root = TempDir::new("meta-join");
    let mut cluster = start_cluster(std::path::Path::new(&root.0));
    let rt = tokio::runtime::Runtime::new().expect("运行时");

    let clients = rt.block_on(connect_all(&cluster.addrs, &IDS));
    let leader = rt.block_on(wait_some_leader(&clients, &IDS));
    eprintln!("  集群 leader = {leader}，地址 {:?}", cluster.addrs[&leader]);

    // ---- ① 先写几条：让新节点"有东西可追" ----
    for k in 0..3u64 {
        let r = rt.block_on(propose_following_leader(
            &clients,
            &IDS,
            commit_op(&format!("pre{k}"), &format!("k-pre{k}"), 1_000 + k),
        ));
        assert!(r.accepted, "预备写入 k={k} 应当被接受");
    }
    let st0 = rt.block_on(status(&clients[&leader], leader));
    eprintln!("  加入前：leader commit={} last={}", st0.commit_index, st0.last_index);

    // ---- ② 起第 4 个进程（**空目录 + 不给 --init**，只给 --join 与 --advertise）----
    let addr4 = free_ports(1)[0];
    let dir4 = cluster.run.join("node4");
    let contacts = IDS
        .iter()
        .map(|i| cluster.addrs[i].to_string())
        .collect::<Vec<_>>()
        .join(",");
    let p4 = start_join(4, &dir4, addr4, &contacts, addr4);
    assert!(
        p4.wait_listening().is_some(),
        "第 4 个进程没在 20s 内就绪（要么 Join 没成，要么它没等到 leader）。stderr:\n{}",
        p4.stderr.lock().unwrap()
    );
    eprintln!("  节点 4 已就绪：{addr4}（通过 {contacts} 加入）");

    // ---- ③ 它必须**追平**（last 追上集群的 commit，applied 也跟上）----
    let c4 = rt.block_on(connect(addr4));
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let s4 = rt.block_on(status(&c4, 4));
        let sl = rt.block_on(status(&clients[&leader], leader));
        if s4.last_index >= sl.commit_index && s4.applied_index >= st0.commit_index {
            eprintln!(
                "  ✅ 已追平：节点4 last={} applied={}（集群 commit={}）",
                s4.last_index, s4.applied_index, sl.commit_index
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "30s 内没追平：节点4 last={} applied={}，集群 commit={}",
            s4.last_index,
            s4.applied_index,
            sl.commit_index
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    // ---- ④ 加入**之后**的新写入也要到它（说明 leader 真的把它算进复制了）----
    for k in 10..13u64 {
        let r = rt.block_on(propose_following_leader(
            &clients,
            &IDS,
            commit_op(&format!("post{k}"), &format!("k-post{k}"), 2_000 + k),
        ));
        assert!(r.accepted, "加入后的写入 k={k} 应当被接受");
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let s4 = rt.block_on(status(&c4, 4));
        let sl = rt.block_on(status(&clients[&leader], leader));
        if s4.applied_index >= sl.commit_index {
            eprintln!(
                "  ✅ 新写入也跟上了：节点4 applied={} 集群 commit={}",
                s4.applied_index, sl.commit_index
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "30s 内新写入没到节点 4：applied={} commit={}",
            s4.applied_index,
            sl.commit_index
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    // ---- ⑤ **重启整个集群**：新 leader 必须仍然知道节点 4 的地址（`§119` 的地址表落盘）----
    //
    // 这一段证的是"**重启后新 leader 仍能把日志送到 learner**"，而这次的 `--peer` 里
    // **只有原来那三个 voter**（运维并不知道后来加入的节点 4 —— 那正是 `Join` 要解决的事）：
    // 新 leader 若不知道 4 的地址，`transport.send(4, …)` 会落进"peer 表里没有它"那条分支被
    // **静默丢弃**，节点 4 就再也收不到任何东西。
    //
    // ⚠️ **但它不能证明"地址表落盘"**（`§119.6` 实测：把落盘关掉它**照样绿**）——
    // 因为 raft 的 `applied` 是从**快照**恢复的，我们自己的 `applied_index` 只服务于本地压缩；
    // 于是重启后**已提交未压缩的条目会重新投递**，那条 conf change 被**重放** ⇒ `add_peer` 再来一次
    // ⇒ 地址就回来了。（`§118.5` 原先写的"重启后 applied 之下不重放"是**错的**，`§119.6` 更正。）
    //
    // 落盘真正管的是**压缩之后**那一格：一旦 conf change 条目被压掉，重放就再也给不出地址，
    // 只剩盘上那份。把这一格端到端盖住需要"小压缩阈值 + 压缩发生在加入之后"，留作后续。
    for id in IDS {
        cluster.procs.get_mut(&id).expect("进程在").kill9();
    }
    for id in IDS {
        let dir = cluster.run.join(format!("node{id}"));
        // 重启：**不带 `--init`**（目录里有数据，闸门会拒绝），`--peer` 仍是原来那三个
        cluster
            .procs
            .insert(id, start(id, &dir, cluster.addrs[&id], &cluster.addrs, false));
    }
    for id in IDS {
        assert!(
            cluster.procs[&id].wait_listening().is_some(),
            "重启后的节点 {id} 没在 20s 内就绪。stderr:\n{}",
            cluster.procs[&id].stderr.lock().unwrap()
        );
    }
    let clients = rt.block_on(connect_all(&cluster.addrs, &IDS));
    let leader = rt.block_on(wait_some_leader(&clients, &IDS));
    eprintln!("  集群已整组重启，新 leader = {leader}（--peer 里没有节点 4）");

    let s4_before = rt.block_on(status(&c4, 4));
    for k in 20..22u64 {
        let r = rt.block_on(propose_following_leader(
            &clients,
            &IDS,
            commit_op(&format!("after{k}"), &format!("k-after{k}"), 3_000 + k),
        ));
        assert!(r.accepted, "重启后的写入 k={k} 应当被接受");
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let s4 = rt.block_on(status(&c4, 4));
        let sl = rt.block_on(status(&clients[&leader], leader));
        if s4.applied_index > s4_before.applied_index && s4.applied_index >= sl.commit_index {
            eprintln!(
                "  ✅ 集群整组重启后，节点 4 仍在被复制：applied {} → {}（集群 commit={}）",
                s4_before.applied_index, s4.applied_index, sl.commit_index
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "重启后新 leader 复制不到节点 4（applied 停在 {}，集群 commit={}）—— \
             多半是**地址表没落盘**：新 leader 手里没有节点 4 的地址（`§119`）",
            s4.applied_index,
            sl.commit_index
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}


// ---------------------------------------------------------------------------
// 成员变更的第二半：把 learner **提升为 voter**（`§120`）
// ---------------------------------------------------------------------------

/// **提升 learner → voter，并证明它真的进了多数派**（`§120`）。
///
/// # 为什么必须有"提升前 / 提升后"的对照
///
/// "提升成功"本身很容易证（看一眼成员表就行）。难的是证明它**改变了 quorum** ——
/// 这才是这件事的全部意义，也是它**危险**的地方（多一个 voter，集群就更紧）。
/// 本用例让**同一批存活 voter**在提升前后得到**相反**的结果：
///
/// | 阶段 | 集群成员 | 谁死了 | 存活 voter | quorum | 写入 |
/// |---|---|---|---|---|---|
/// | 提升前 | voters `[1,2,3]` + learner `4` | `2`（`4` 活着但**只是个 learner**） | `{1,3}` = 2 | 2（3 个 voter） | ✅ 成功 |
/// | 提升后 | voters `[1,2,3,4]` | `2`、`4` | `{1,3}` = 2 | 3（4 个 voter） | ❌ **停住** |
///
/// 两边**是同一个存活 voter 集合** `{1,3}` ⇒ 结果相反只可能来自那次提升。
/// （learner 的死活不影响 quorum，这是定义；所以"4 死了"**不是**停住的原因。）
#[test]
fn promoting_a_learner_makes_it_count_for_the_quorum() {
    let root = TempDir::new("meta-promote");
    let mut cluster = start_cluster(std::path::Path::new(&root.0));
    let rt = tokio::runtime::Runtime::new().expect("运行时");

    let clients = rt.block_on(connect_all(&cluster.addrs, &IDS));
    let leader0 = rt.block_on(wait_some_leader(&clients, &IDS));

    // ---- ① 第 4 个节点 Join 并追平（`§119` 的能力，这里当提升的前置）----
    let addr4 = free_ports(1)[0];
    let dir4 = cluster.run.join("node4");
    let contacts = IDS
        .iter()
        .map(|i| cluster.addrs[i].to_string())
        .collect::<Vec<_>>()
        .join(",");
    let mut p4 = start_join(4, &dir4, addr4, &contacts, addr4);
    assert!(
        p4.wait_listening().is_some(),
        "节点 4 没在 20s 内就绪。stderr:\n{}",
        p4.stderr.lock().unwrap()
    );
    let c4 = rt.block_on(connect(addr4));
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let s4 = rt.block_on(status(&c4, 4));
        let sl = rt.block_on(status(&clients[&leader0], leader0));
        if sl.commit_index > 0 && s4.applied_index >= sl.commit_index {
            eprintln!("  ① 节点 4 已追平：applied={}（集群 commit={}）", s4.applied_index, sl.commit_index);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "30s 内节点 4 没追平：applied={} commit={}\n  节点4 全量: {s4:?}\n  集群(leader={leader0}) 全量: {sl:?}",
            s4.applied_index,
            sl.commit_index
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    // ---- ② 基线：杀一个 voter，**写入照常成功**（learner 不算数）----
    cluster.procs.get_mut(&2).expect("进程在").kill9();
    let live: [u64; 2] = [1, 3];
    let r = rt.block_on(propose_following_leader(
        &clients,
        &live,
        commit_op("before", "k-before", 6_000),
    ));
    assert!(
        r.accepted,
        "提升前：3 个 voter 里剩 2 个就是多数派（learner 不参与）⇒ 写入应当成功"
    );
    eprintln!("  ② 提升前：杀掉 voter 2 后写入**照常成功**（存活 voter {{1,3}} = 2）");

    // ---- ③ 提升：把它从 learner 变成 voter ----
    let leader = rt.block_on(wait_some_leader(&clients, &live));
    let resp = rt.block_on(async {
        clients[&leader]
            .clone()
            .promote(pb::PromoteRequest { node_id: 4 })
            .await
            .expect("提升应当成功（§120）")
            .into_inner()
    });
    assert_eq!(resp.voter_ids, vec![1, 2, 3, 4], "提升后成员表必须含 4：{resp:?}");
    assert!(
        resp.learner_ids.is_empty(),
        "它已经不再是 learner：{:?}",
        resp.learner_ids
    );
    // 成员变更是**走 raft 的** ⇒ 从**别的**节点读一遍也必须一致（否则就是没复制到）
    let other = if leader == 1 { 3 } else { 1 };
    let st_other = rt.block_on(status(&clients[&other], other));
    assert_eq!(
        st_other.voter_ids,
        vec![1, 2, 3, 4],
        "成员变更必须复制到每个成员（Status 的 voter_ids 可直接观测）"
    );
    eprintln!("  ③ 已提升：voters=[1,2,3,4]（leader={leader} 与 节点{other} 都认下了）");

    // ---- ④ 对照：杀 node 4 ⇒ 存活 voter **仍是 {1,3}**，但写入必须**停住** ----
    p4.kill9();
    let leader = rt.block_on(wait_some_leader(&clients, &live));
    assert_eq!(
        rt.block_on(status(&clients[&leader], leader)).role,
        "Leader",
        "先确认我们问的还是一个**活着的 leader** —— 否则下面的超时说明不了任何事"
    );
    let res = rt.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(4),
            clients[&leader].clone().propose(pb::ProposeRequest {
                op: Some(commit_op("after", "k-after", 7_000)),
                request_id: b"rid".to_vec(),
                schema_ver: 0,
            }),
        )
        .await
    });
    assert!(
        res.is_err(),
        "4 个 voter 只活 2 个（{{1,3}}）⇒ 不到 quorum，**不该**被接受：{res:?}"
    );
    eprintln!("  ④ 提升后：同一批存活 voter {{1,3}} 已不构成多数派 ⇒ 写入**停住**（4s 内未被接受）");
}
