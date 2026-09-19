//! `yuntun-meta` —— metanode（R3）。
//!
//! # 当前状态：**S3-1 PoC（选型闸门）**
//!
//! 本 crate 此刻的内容是 **raft-rs 的可行性验证**，不是生产实现。它的存在要回答
//! [`docs/metanode-design.md`](../../../docs/metanode-design.md) §4.2 的闸门问题：
//!
//! > raft-rs 的样板成本（Storage/Ready 循环/传输/快照）是否失控到值得冒
//! > "openraft 生产验证较少"的风险？
//!
//! 闸门判据（**结论要写进 `operation-log §40`**）：
//!
//! | # | 判据 | 怎么看 |
//! |---|---|---|
//! | 1 | 三节点能选主并**收敛到同一状态** | `three_node_cluster_converges_on_proposed_ops` |
//! | 2 | kill leader 后**已提交数据不丢** | `leader_kill_reelects_and_keeps_committed_ops` |
//! | 3 | **状态机接缝干净**：`CatalogState` 原样被驱动，不需要为 raft 改它 | 本文件 `apply_op`（无分支、无时钟、无 IO） |
//! | 4 | 样板规模可接受 | 本文件行数 + 需自实现的 Storage/传输量 |
//!
//! # 为什么用 `MemStorage` 而不是 fjall
//!
//! 选型闸门关心的是**集成成本**（Ready 循环、消息路由、状态机接缝、成员与快照接口），
//! 而不是存储引擎。真正要自实现的 `Storage`（fjall 后端）在**两个候选下工作量相同**，
//! 所以 PoC 用 raft-rs 自带的 `MemStorage` 把闸门问题隔离出来。
//! （生产实现里 `Storage` 必须落盘：`RaftState`（term/vote/commit）+ 日志条目 + 快照。）
//!
//! # 进程内传输
//!
//! raft 消息直接经 `std::sync::mpsc` 传递（**不序列化**）—— 生产实现要走 gRPC，
//! 但那是 S3-0/S3-3 的事，与选型无关。

use std::collections::{HashMap, VecDeque};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use raft::eraftpb::{Entry, EntryType, Message, Snapshot};
use raft::prelude::*;
use raft::storage::MemStorage;
use raft::StateRole;
// `ConfChange::merge_from_bytes` 来自 protobuf trait（raft-rs 0.7 默认 protobuf 编解码）
use protobuf::Message as PbMessage;

use yuntun_catalog::CatalogState;
use yuntun_model::ops::{CreateTableRequest, DEFAULT_SCHEMA};

/// 三节点的固定成员表（PoC 用常量；生产由 `--init` / `Join` 决定）。
pub const PEERS: [u64; 3] = [1, 2, 3];

/// PoC 的 op 编码（**S3-0 会换成 proto 定义的 `CatalogOp`**）。
///
/// 关键点不在编码，而在**时间戳由 op 携带**：状态机内不得读钟，否则各副本
/// apply 同一串 op 会得到不同状态（`catalog/src/state.rs` 纪律 1）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PocOp {
    CreateSchema { name: String, now_ms: u64 },
    CreateTable { name: String, now_ms: u64 },
    Commit { table: String, batch_id: String, rows: u64, now_ms: u64 },
}

impl PocOp {
    /// 文本编码（PoC 够用；生产用 proto）。
    pub fn encode(&self) -> Vec<u8> {
        let s = match self {
            PocOp::CreateSchema { name, now_ms } => format!("schema\t{name}\t{now_ms}"),
            PocOp::CreateTable { name, now_ms } => format!("table\t{name}\t{now_ms}"),
            PocOp::Commit {
                table,
                batch_id,
                rows,
                now_ms,
            } => format!("commit\t{table}\t{batch_id}\t{rows}\t{now_ms}"),
        };
        s.into_bytes()
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        let s = std::str::from_utf8(data).ok()?;
        let mut it = s.split('\t');
        match it.next()? {
            "schema" => Some(PocOp::CreateSchema {
                name: it.next()?.to_string(),
                now_ms: it.next()?.parse().ok()?,
            }),
            "table" => Some(PocOp::CreateTable {
                name: it.next()?.to_string(),
                now_ms: it.next()?.parse().ok()?,
            }),
            "commit" => Some(PocOp::Commit {
                table: it.next()?.to_string(),
                batch_id: it.next()?.to_string(),
                rows: it.next()?.parse().ok()?,
                now_ms: it.next()?.parse().ok()?,
            }),
            _ => None,
        }
    }
}

/// **状态机接缝**：把 op 应用到 [`CatalogState`]。
///
/// 这个函数就是 R3 的全部"业务" —— 它是纯的（无锁、无时钟、无 IO），
/// 因此**两个候选 raft 库都不需要为它改一行**。返回 `false` 表示该 op 在语义上被拒
/// （如重复键/已存在），但仍属于"已提交"（幂等语义由状态机自己保证）。
pub fn apply_op(state: &mut CatalogState, op: &PocOp) -> bool {
    match op {
        PocOp::CreateSchema { name, now_ms } => {
            // 已存在视为成功（幂等重试语义，与 CatalogOps 一致：重复 DDL 报错，
            // 但这里 PoC 只关心"确定性 apply"，故忽略 AlreadyExists）
            let _ = now_ms;
            let exists = state.schema_exists(name);
            if exists {
                return true;
            }
            state.create_schema(name).is_ok()
        }
        PocOp::CreateTable { name, now_ms } => {
            if state.get_table(name).is_some() {
                return true;
            }
            state
                .create_table(
                    CreateTableRequest {
                        name: name.clone(),
                        namespace: DEFAULT_SCHEMA.into(),
                        schema: poc_schema(),
                        partition_cols: vec![],
                        default_format: "parquet".into(),
                        ingest_config: yuntun_model::meta::IngestConfig::standard(),
                    },
                    *now_ms,
                )
                .is_ok()
        }
        PocOp::Commit {
            table,
            batch_id,
            rows,
            now_ms,
        } => {
            let req = yuntun_model::ops::CommitFilesRequest {
                table: table.clone(),
                batch_id: batch_id.clone(),
                client_request_id: None,
                client_request_ids: vec![],
                shard: "s0".into(),
                time_window: "w0".into(),
                files: vec![yuntun_model::meta::FileManifest {
                    file_path: format!("p/{batch_id}.parquet"),
                    row_count: *rows,
                    ..Default::default()
                }],
                schema_version: 1,
                row_count: *rows,
            };
            state.commit_files(req, *now_ms).map(|r| r.accepted).unwrap_or(false)
        }
    }
}

fn poc_schema() -> arrow::datatypes::SchemaRef {
    Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("ts", arrow::datatypes::DataType::Int64, false),
    ]))
}

/// 发给节点的命令。
enum Command {
    /// 提议一个 op；`reply` 在**该 op 被应用到状态机**时收到它。
    Propose {
        op: PocOp,
        reply: SyncSender<Result<(), String>>,
    },
    /// 停止该节点（模拟崩溃：线程退出、消息不再收发）。
    Stop,
}

/// 节点运行时的可观测句柄（测试用）。
struct NodeState {
    /// 该节点的状态机（**唯一**被 raft 提交序驱动的实例）
    sm: Arc<Mutex<CatalogState>>,
    /// 当前角色（Leader/Follower/Candidate）—— 由运行线程更新
    role: Arc<Mutex<StateRole>>,
    /// 已应用的 raft 索引
    applied: Arc<Mutex<u64>>,
    cmd_tx: Sender<Command>,
}

impl NodeState {
    fn is_leader(&self) -> bool {
        *self.role.lock().unwrap() == StateRole::Leader
    }
    fn canonical(&self) -> Vec<u8> {
        self.sm.lock().unwrap().encode_canonical()
    }
}

/// 三节点 raft 集群（进程内、内存存储）。
pub struct Cluster {
    nodes: HashMap<u64, NodeState>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl Cluster {
    /// 启动 `PEERS` 里的所有节点（1 起三节点组：`create_raft_leader` 语义手工构造）。
    pub fn start() -> Self {
        // 每个节点一个邮箱（不序列化，直接传 `Message`）
        let mut txs: HashMap<u64, Sender<Message>> = HashMap::new();
        let mut rxs: HashMap<u64, Receiver<Message>> = HashMap::new();
        for id in PEERS {
            let (tx, rx) = mpsc::channel();
            txs.insert(id, tx);
            rxs.insert(id, rx);
        }

        let mut nodes = HashMap::new();
        let mut handles = Vec::new();
        for id in PEERS {
            let rx = rxs.remove(&id).unwrap();
            let mailboxes = txs.clone();
            let sm = Arc::new(Mutex::new(CatalogState::new()));
            let role = Arc::new(Mutex::new(StateRole::Follower));
            let applied = Arc::new(Mutex::new(0u64));
            let (cmd_tx, cmd_rx) = mpsc::channel::<Command>();

            nodes.insert(
                id,
                NodeState {
                    sm: sm.clone(),
                    role: role.clone(),
                    applied: applied.clone(),
                    cmd_tx,
                },
            );
            handles.push(spawn_node(id, rx, mailboxes, sm, role, applied, cmd_rx));
        }
        // 等选主（心跳 3 tick × 10ms = 30ms；选举超时 10 tick = 100ms 量级）
        Cluster { nodes, handles }
    }

    /// 等到选出 leader（返回其 id）；超时返回 `None`。
    pub fn wait_leader(&self, timeout: Duration) -> Option<u64> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Some(id) = self.nodes.iter().find(|(_, n)| n.is_leader()).map(|(id, _)| *id) {
                return Some(id);
            }
            thread::sleep(Duration::from_millis(10));
        }
        None
    }

    /// 向当前 leader 提议一个 op，等它被**应用**（超时返回 Err）。
    pub fn propose(&self, op: PocOp, timeout: Duration) -> Result<(), String> {
        // 直接问每个节点"你是不是 leader"，把命令投给 leader 的邮箱。
        // 生产实现里这里就是 `Meta.Propose` RPC（非 leader 返回带 leader hint 的错误）。
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(id) = self
                .nodes
                .iter()
                .find(|(_, n)| n.is_leader())
                .map(|(id, _)| *id)
            {
                let (reply_tx, reply_rx) = mpsc::sync_channel(1);
                self.nodes[&id]
                    .cmd_tx
                    .send(Command::Propose {
                        op: op.clone(),
                        reply: reply_tx,
                    })
                    .map_err(|e| e.to_string())?;
                return match reply_rx.recv_timeout(timeout) {
                    Ok(r) => r,
                    Err(e) => Err(format!("等应用超时/断开：{e}")),
                };
            }
            if Instant::now() >= deadline {
                return Err("没有 leader".into());
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    /// 杀掉一个节点（线程退出 = 进程内最接近"节点崩溃"的形态）。
    pub fn kill(&mut self, id: u64) {
        if let Some(n) = self.nodes.remove(&id) {
            let _ = n.cmd_tx.send(Command::Stop);
        }
    }

    /// 存活节点数（= 多数判定用）。
    pub fn alive(&self) -> usize {
        self.nodes.len()
    }

    /// 某节点的状态机规范编码（跨节点比对 = 收敛证据）。
    pub fn canonical(&self, id: u64) -> Option<Vec<u8>> {
        self.nodes.get(&id).map(|n| n.canonical())
    }

    /// 某节点的已应用索引。
    pub fn applied(&self, id: u64) -> Option<u64> {
        self.nodes.get(&id).map(|n| *n.applied.lock().unwrap())
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for n in self.nodes.values() {
            let _ = n.cmd_tx.send(Command::Stop);
        }
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
    }
}

fn spawn_node(
    id: u64,
    mailbox: Receiver<Message>,
    mailboxes: HashMap<u64, Sender<Message>>,
    sm: Arc<Mutex<CatalogState>>,
    role: Arc<Mutex<StateRole>>,
    applied: Arc<Mutex<u64>>,
    cmd_rx: Receiver<Command>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let logger = raft::default_logger();
        let cfg = Config {
            id,
            election_tick: 10,
            heartbeat_tick: 3,
            // 预投票：减少被隔离节点反复打断 leader（生产实现必须开）
            pre_vote: true,
            ..Default::default()
        };
        // 初始成员：三节点组（生产由 `--init` 决定，扩容走 learner → voter）
        let storage = MemStorage::new_with_conf_state((PEERS.to_vec(), vec![]));
        let mut raw = RawNode::new(&cfg, storage, &logger).expect("raw node");
        *role.lock().unwrap() = raw.raft.state;

        // 已提议但尚未应用的 op（按提议顺序配对；只有 leader 会有内容）
        let mut pending: VecDeque<SyncSender<Result<(), String>>> = VecDeque::new();
        let mut last_tick = Instant::now();
        let tick_every = Duration::from_millis(10);

        loop {
            // ① 收消息
            loop {
                match mailbox.try_recv() {
                    Ok(msg) => {
                        let _ = raw.step(msg);
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }
            // ② 命令
            loop {
                match cmd_rx.try_recv() {
                    Ok(Command::Stop) => return,
                    Ok(Command::Propose { op, reply }) => {
                        if raw.raft.state == StateRole::Leader {
                            // 顺序很重要：**先提议成功再入队**，否则队列与日志条目会错位
                            // （错位会让"应用 A 的 op 却回复了 B 的等待者" —— 静默错配）。
                            match raw.propose(vec![], op.encode()) {
                                Ok(()) => pending.push_back(reply),
                                Err(e) => {
                                    let _ = reply.send(Err(e.to_string()));
                                }
                            }
                        } else {
                            // 生产实现：这里返回带 leader hint 的错误，客户端重试（design §5 约定 4）
                            let _ = reply.send(Err("not leader".into()));
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }
            // ③ tick
            if last_tick.elapsed() >= tick_every {
                raw.tick();
                last_tick = Instant::now();
            }
            *role.lock().unwrap() = raw.raft.state;

            // ④ Ready 循环
            if raw.has_ready() {
                let mut rd = raw.ready();
                // 出站消息
                for msg in rd.take_messages() {
                    if let Some(tx) = mailboxes.get(&msg.to) {
                        let _ = tx.send(msg);
                    }
                }
                // 快照：**必须**在 advance 前安装（若为默认快照则跳过）。
                //
                // 本 PoC 不触发 compaction（没有新节点追日志的场景），所以这里只断言形状：
                // 真正的"快照安装"在 S3-1b / S3-3 测（需要 `compact()` + 新成员加入）。
                // 说明：`CatalogState` 的**反序列化**属 S3-3（现只有 `encode_canonical`，
                // 快照格式要用 prost + 分块 CRC，见 metanode-design §4.4）。
                if *rd.snapshot() != Snapshot::default() {
                    eprintln!("[meta:{id}] 收到快照（PoC 未实现安装；见 S3-1b/S3-3）");
                }
                // 落盘（这里进 MemStorage；生产 = fjall append + hardstate）
                let store = raw.raft.raft_log.store.clone();
                if let Err(e) = store.wl().append(rd.entries()) {
                    eprintln!("[meta:{id}] persist entries failed: {e}");
                    return;
                }
                if let Some(hs) = rd.hs() {
                    store.wl().set_hardstate(hs.clone());
                }
                // 应用已提交条目
                let committed = rd.take_committed_entries();
                apply_committed(id, &mut raw, committed, &sm, &applied, &mut pending);
                // 持久化后的消息（follower 的 append 响应等）
                for msg in rd.take_persisted_messages() {
                    if let Some(tx) = mailboxes.get(&msg.to) {
                        let _ = tx.send(msg);
                    }
                }
                // ② advance：拿到 commit index 与下一批可应用条目
                let mut light = raw.advance(rd);
                if let Some(commit) = light.commit_index() {
                    store.wl().mut_hard_state().set_commit(commit);
                }
                for msg in light.take_messages() {
                    if let Some(tx) = mailboxes.get(&msg.to) {
                        let _ = tx.send(msg);
                    }
                }
                let committed = light.take_committed_entries();
                apply_committed(id, &mut raw, committed, &sm, &applied, &mut pending);
                raw.advance_apply();
            }
            thread::sleep(Duration::from_millis(2));
        }
    })
}

fn apply_committed(
    id: u64,
    raw: &mut RawNode<MemStorage>,
    entries: Vec<Entry>,
    sm: &Arc<Mutex<CatalogState>>,
    applied: &Arc<Mutex<u64>>,
    pending: &mut VecDeque<SyncSender<Result<(), String>>>,
) {
    for entry in entries {
        // 空条目 = 新 leader 的就位条目（无 op）
        if entry.data.is_empty() {
            continue;
        }
        if let EntryType::EntryConfChange = entry.get_entry_type() {
            let mut cc = ConfChange::default();
            if let Err(e) = cc.merge_from_bytes(&entry.data) {
                eprintln!("[meta:{id}] bad conf change: {e}");
                continue;
            }
            if let Ok(cs) = raw.apply_conf_change(&cc) {
                let store = raw.raft.raft_log.store.clone();
                store.wl().set_conf_state(cs);
            }
            continue;
        }
        let Some(op) = PocOp::decode(&entry.data) else {
            eprintln!("[meta:{id}] undecodable op {:?}", entry.data);
            continue;
        };
        let ok = apply_op(&mut sm.lock().unwrap(), &op);
        *applied.lock().unwrap() = entry.index;
        // leader 才持有等待者；换主后旧队列为空 → 跳过
        if let Some(reply) = pending.pop_front() {
            let _ = reply.send(if ok { Ok(()) } else { Err("op rejected".into()) });
        }
    }
}
