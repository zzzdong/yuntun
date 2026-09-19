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
//! | 5 | **快照可安装**（字段落后 + 日志已压缩时靠快照追上） | `follower_behind_catches_up_via_snapshot`（S3-1b） |
//!
//! # 为什么不用 `MemStorage`
//!
//! 第一版 PoC 用的是 raft-rs 自带 `MemStorage`。做快照安装时发现它**不能用于真实场景**：
//!
//! 1. 它的 `snapshot()` 造出的快照 **data 为空**（元数据取自 `hard_state.commit`）→
//!    follower 装上等于**把状态机清空**，且不报错；
//! 2. 它的 `compact()` 只丢日志、**不认识状态机** —— 快照内容必须来自 `CatalogState`，
//!    这是存储实现方的责任。
//!
//! 于是改为自实现 [`storage::MetaStorage`]（内存版；S3-3 换 fjall，结构不变）。
//! 这本身也是闸门的收获：**真正要自写的 Storage 不是"可选优化"，是快照能力的硬前提**。
//!
//! # 进程内传输
//!
//! raft 消息直接经 `std::sync::mpsc` 传递（**不序列化**）—— 生产实现要走 gRPC，
//! 但那是 S3-0/S3-3 的事，与选型无关。

pub mod error;
pub mod fjall_storage;
pub mod op;
pub mod service;
pub mod storage;

pub use error::MetaError;
pub use fjall_storage::FjallStorage;
pub use op::{apply, decode_op, StateOp};
pub use service::{serve, MetaService};
pub use storage::MetaStorage;

use std::collections::{HashMap, VecDeque};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use raft::eraftpb::{Entry, EntryType, Message, Snapshot};
use raft::prelude::*;
use raft::StateRole;
// `ConfChange::merge_from_bytes` 来自 protobuf trait（raft-rs 0.7 默认 protobuf 编解码）
use protobuf::Message as PbMessage;

use yuntun_catalog::CatalogState;
use prost::Message as _;

/// 三节点的固定成员表（PoC 用常量；生产由 `--init` / `Join` 决定）。
pub const PEERS: [u64; 3] = [1, 2, 3];

// op 的**编码与应用**已迁到生产路径：见 [`crate::op`]（proto `Op` → `StateOp` → 状态机）。
//
// 这里原本是 PoC 的文本编码（`PocOp` + `apply_op`）。S3-0/S3-3 之后它被真正的协议取代，
// 故**删除**而不是留着 —— 两套 op 编码并存，久了没人分得清哪套是权威（而且日志里的
// payload 只能有一套：它必须与 gRPC 面同构，否则"线上发的"和"盘上存的"会对不上）。

/// 发给节点的命令。
///
/// `Propose` 与 `Stop` 大小差异大（前者带完整 op + 回复通道），但这是**控制通道**：
/// 节点每秒至多收到几十条，为省几十字节而 `Box` 反而多一次分配与间接跳转。
/// 故显式放行该 lint（而不是默默让它挂着）。
#[allow(clippy::large_enum_variant)]
enum Command {
    /// 提议一个 op；`reply` 在**该 op 被应用到状态机**时收到结果。
    ///
    /// 回复里带 raft index 与两组版本号（`metanode-design §5` 的 `ProposeResponse`），
    /// 错误则按约定 3 映射（见 [`crate::MetaError`] 的 `From<MetaError> for tonic::Status`）。
    Propose {
        op: yuntun_proto::meta::Op,
        reply: SyncSender<Result<yuntun_proto::meta::ProposeResponse, MetaError>>,
    },
    /// 停止该节点（模拟崩溃：线程退出、消息不再收发）。
    Stop,
}

/// 节点运行时的可观测句柄（测试用）。
struct NodeState {
    /// 该节点的状态机（**唯一**被 raft 提交序驱动的实例）
    sm: Arc<Mutex<CatalogState>>,
    /// 该节点的 raft 存储（日志 + 硬状态 + 快照产物，**落盘**）
    storage: FjallStorage,
    /// 当前角色（Leader/Follower/Candidate）—— 由运行线程更新
    role: Arc<Mutex<StateRole>>,
    /// 已应用的 raft 索引
    applied: Arc<Mutex<u64>>,
    /// 内部状态快照（测试诊断用；打印给人看的字符串）
    debug: Arc<Mutex<String>>,
    /// **结构化**状态（给程序读：`Status` RPC 直接由它构造，见 [`NodeHandle::status`]）
    status: Arc<Mutex<NodeStatus>>,
    cmd_tx: Sender<Command>,
}

/// 节点的**结构化**状态（每轮循环刷新）。
///
/// 与 `debug: String` 的分工：那个是给人看的诊断串（失败时打印），这个是**给程序读**的字段。
/// 别去 parse 诊断串 —— 那会随打印格式改名而悄悄坏掉。
#[derive(Debug, Clone, Default)]
pub struct NodeStatus {
    pub node_id: u64,
    pub role: String,
    pub term: u64,
    /// 0 = 未知（客户端据此重试到正确节点）
    pub leader_id: u64,
    pub commit_index: u64,
    pub applied_index: u64,
    pub snapshot_index: u64,
    pub first_index: u64,
    pub last_index: u64,
}

/// 单个节点的句柄（gRPC 服务层用；`Clone` 只克隆几个 `Arc`/`Sender`，很便宜）。
#[derive(Clone)]
pub struct NodeHandle {
    id: u64,
    status: Arc<Mutex<NodeStatus>>,
    sm: Arc<Mutex<CatalogState>>,
    storage: FjallStorage,
    cmd_tx: Sender<Command>,
}

impl NodeHandle {
    pub fn id(&self) -> u64 {
        self.id
    }

    /// 运维状态（`Status` RPC 的返回）。
    pub fn status(&self) -> yuntun_proto::meta::StatusResponse {
        let s = self.status.lock().unwrap();
        yuntun_proto::meta::StatusResponse {
            node_id: s.node_id,
            role: s.role.clone(),
            term: s.term,
            leader_id: s.leader_id,
            commit_index: s.commit_index,
            applied_index: s.applied_index,
            snapshot_index: s.snapshot_index,
            first_index: s.first_index,
            last_index: s.last_index,
            version: yuntun_proto::PROTO_VERSION.into(),
        }
    }

    /// 向**本节点**提议一个 op。非 leader → [`MetaError::NotLeader`]（带 leader hint，可重试）。
    pub fn propose(
        &self,
        op: yuntun_proto::meta::Op,
        timeout: Duration,
    ) -> Result<yuntun_proto::meta::ProposeResponse, MetaError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.cmd_tx
            .send(Command::Propose {
                op,
                reply: reply_tx,
            })
            .map_err(|e| MetaError::Storage(format!("命令通道断开：{e}")))?;
        match reply_rx.recv_timeout(timeout) {
            Ok(r) => r,
            Err(_) => Err(MetaError::NoQuorum),
        }
    }

    /// 清单增量（`Delta` RPC）：只报"自 `since` 起变过的表"。
    pub fn delta(&self, since_manifest_ver: u64) -> yuntun_proto::meta::DeltaResponse {
        let st = self.sm.lock().unwrap();
        let v = st.version();
        let d = st.manifest_delta(since_manifest_ver);
        yuntun_proto::meta::DeltaResponse {
            schema_ver: v.schema_ver,
            manifest_ver: v.manifest_ver,
            changed_tables: d.changed_tables,
            full_reload: d.full_reload_required,
        }
    }

    /// 本节点的存储（诊断；S3-6 观测会用）
    pub fn storage(&self) -> &FjallStorage {
        &self.storage
    }
}

impl NodeState {
    fn is_leader(&self) -> bool {
        *self.role.lock().unwrap() == StateRole::Leader
    }
    fn canonical(&self) -> Vec<u8> {
        self.sm.lock().unwrap().encode_canonical()
    }
    fn debug(&self) -> String {
        self.debug.lock().unwrap().clone()
    }
}

/// 三节点 raft 集群（进程内；存储 = [`FjallStorage`] **落盘版**）。
///
/// # 为什么用落盘版而不是内存版
///
/// 内存版（[`MetaStorage`]）是**语义的定义处**（它的单测把不变量钉死），但用它做集群就无法
/// 验证真正重要的一条：**进程重启后状态从盘上重建**（M3 的 G1）。切到落盘版后，`kill` 再
/// `restart` 就是**真的崩溃恢复**：释放存储句柄（连带释放 fjall 目录锁）→ 重新打开目录 →
/// 状态机由盘上的快照 + 日志重放**重建**（见 [`FjallStorage::open_with_state`]）。
///
/// 为什么要把 `receivers` / `handles` 收在结构里：S3-1b 的用例需要**杀掉一个 follower
/// 再用空存储重启**（模拟"落后到只能靠快照追赶"）。邮箱是稳定身份，线程与存储才是可重建的。
pub struct Cluster {
    nodes: HashMap<u64, NodeState>,
    mailboxes: HashMap<u64, Sender<Message>>,
    receivers: HashMap<u64, Receiver<Message>>,
    /// 线程退出时会把**邮箱交还**（见 `spawn_node` 返回值）——
    /// 否则 `kill` 之后就再也起不回同一个 id（receiver 随线程一起被丢掉）。
    handles: HashMap<u64, thread::JoinHandle<Receiver<Message>>>,
    /// 各节点存储的根目录（每节点一个子目录；`Drop` 时清理）
    root: std::path::PathBuf,
}

impl Cluster {
    /// 启动 `PEERS` 里的所有节点（三节点组）。
    pub fn start() -> Self {
        // 每个节点一个邮箱（不序列化，直接传 `Message`）
        let mut mailboxes: HashMap<u64, Sender<Message>> = HashMap::new();
        let mut receivers: HashMap<u64, Receiver<Message>> = HashMap::new();
        for id in PEERS {
            let (tx, rx) = mpsc::channel();
            mailboxes.insert(id, tx);
            receivers.insert(id, rx);
        }
        let root = temp_root();
        let mut c = Cluster {
            nodes: HashMap::new(),
            mailboxes,
            receivers,
            handles: HashMap::new(),
            root,
        };
        for id in PEERS {
            c.spawn(id);
        }
        c
    }

    /// 起一个节点：**从盘打开存储**（首次 = 空；重启 = 从快照 + 日志重建状态机）。
    fn spawn(&mut self, id: u64) {
        let (storage, sm) = FjallStorage::open_with_state(self.node_dir(id), id, PEERS.to_vec())
            .unwrap_or_else(|e| panic!("打开节点 {id} 的存储失败：{e}"));
        let role = Arc::new(Mutex::new(StateRole::Follower));
        let applied = Arc::new(Mutex::new(0u64));
        let debug = Arc::new(Mutex::new(String::new()));
        let status = Arc::new(Mutex::new(NodeStatus {
            node_id: id,
            ..Default::default()
        }));
        self.spawn_with(id, sm, storage, role, applied, debug, status);
    }

    /// 某节点的存储目录。
    pub fn node_dir(&self, id: u64) -> std::path::PathBuf {
        self.root.join(format!("node{id}"))
    }

    /// 用给定的状态机与存储起线程。
    ///
    /// 参数多是**有意**的：这里就是把一个节点的全部运行期句柄显式交出去，
    /// 收进结构体反而会掩盖"谁共享了什么"。与 `spawn_node` 同款处理。
    #[allow(clippy::too_many_arguments)]
    fn spawn_with(
        &mut self,
        id: u64,
        sm: Arc<Mutex<CatalogState>>,
        storage: FjallStorage,
        role: Arc<Mutex<StateRole>>,
        applied: Arc<Mutex<u64>>,
        debug: Arc<Mutex<String>>,
        status: Arc<Mutex<NodeStatus>>,
    ) {
        let rx = self
            .receivers
            .remove(&id)
            .unwrap_or_else(|| panic!("节点 {id} 已在运行（邮箱被占用）"));
        let (cmd_tx, cmd_rx) = mpsc::channel::<Command>();
        self.handles.insert(
            id,
            spawn_node(
                id,
                rx,
                self.mailboxes.clone(),
                sm.clone(),
                storage.handle(),
                role.clone(),
                applied.clone(),
                debug.clone(),
                status.clone(),
                cmd_rx,
            ),
        );
        self.nodes.insert(
            id,
            NodeState {
                sm,
                storage,
                role,
                applied,
                debug,
                status,
                cmd_tx,
            },
        );
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

    /// 向当前 leader 提议一个 op，等它被**应用**后返回结果。
    ///
    /// 找不到 leader / 等应用超时 → [`MetaError::NoQuorum`]（**可重试**：客户端应换节点重试
    /// —— 设计 §5 约定 3 明确要求，G2「写入不中断」依赖它）。
    pub fn propose(
        &self,
        op: yuntun_proto::meta::Op,
        timeout: Duration,
    ) -> Result<yuntun_proto::meta::ProposeResponse, MetaError> {
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
                    .map_err(|e| MetaError::Storage(format!("命令通道断开：{e}")))?;
                return match reply_rx.recv_timeout(timeout) {
                    Ok(r) => r,
                    Err(_) => Err(MetaError::NoQuorum),
                };
            }
            if Instant::now() >= deadline {
                // 谁都不是 leader：尽可能给出已知的 leader hint（0 = 未知）
                let hint = self
                    .nodes
                    .values()
                    .map(|n| n.status.lock().unwrap().leader_id)
                    .find(|id| *id != 0)
                    .unwrap_or(0);
                return Err(MetaError::NotLeader { leader_hint: hint });
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    /// 杀掉一个节点线程（进程内最接近"节点崩溃"的形态）。**保留其存储与状态机** ——
    /// 真实崩溃后重启也是从自己的盘上恢复，而不是"从零开始"。
    pub fn kill(&mut self, id: u64) {
        if let Some(n) = self.nodes.remove(&id) {
            let _ = n.cmd_tx.send(Command::Stop);
            drop(n); // 释放存储句柄（含 fjall 目录锁），否则重启打不开同一目录
        }
        if let Some(h) = self.handles.remove(&id) {
            if let Ok(rx) = h.join() {
                self.receivers.insert(id, rx);
            }
        }
    }

    /// 重启一个已 kill 的节点：**带着它自己的日志与状态机**回来（崩溃恢复的形态）。
    ///
    /// # ⚠️ 为什么不是"空存储 + 同 id"
    ///
    /// 那个做法是 **raft 的非法操作**，会直接撞上 raft 的断言：
    /// `to_commit N is out of range [last_index 0]`（etcd 那句 "Was the raft log corrupted,
    /// truncated, or lost?"）。原因是 leader 侧仍记着该 peer 的 `matched`（旧位置），
    /// 于是发**心跳**带 commit=N —— 空日志的 follower 在 `handleHeartbeat` 里
    /// 无条件 `commit_to(N)`，越界即 panic。
    ///
    /// 结论（写进文档，影响 S3-6 的运维）：**掉盘的节点不能复用原 id 直接空启**；
    /// 要么从备份/快照恢复后回来，要么以**新 id 重新加入**（成员变更）。
    pub fn restart(&mut self, id: u64) {
        assert!(
            !self.nodes.contains_key(&id),
            "节点 {id} 还在运行，先 kill 再 restart"
        );
        // 从自己的目录重新打开：状态机由盘上重建（不是「留着内存」）
        self.spawn(id);
    }

    /// 触发某节点**压缩到已应用位置**（会生成快照产物并丢掉老日志）。
    pub fn compact(&self, id: u64) -> Option<u64> {
        self.nodes.get(&id).map(|n| {
            n.storage
                .compact_applied()
                .unwrap_or_else(|e| panic!("压缩落盘失败：{e}"))
        })
    }

    /// 取某节点的句柄（gRPC 服务层用）。
    pub fn handle(&self, id: u64) -> Option<NodeHandle> {
        self.nodes.get(&id).map(|n| NodeHandle {
            id,
            status: n.status.clone(),
            sm: n.sm.clone(),
            storage: n.storage.handle(),
            cmd_tx: n.cmd_tx.clone(),
        })
    }

    /// 某节点安装过的快照数（>0 说明它**确实靠快照**追上，而不是靠日志）。
    pub fn snapshot_installs(&self, id: u64) -> Option<usize> {
        self.nodes.get(&id).map(|n| n.storage.installs())
    }

    /// 打印所有存活节点的内部状态（失败诊断用）。
    pub fn dump(&self) -> String {
        let mut ids: Vec<u64> = self.nodes.keys().copied().collect();
        ids.sort_unstable();
        ids.iter()
            .map(|id| {
                format!(
                    "  节点 {id}: {}\n",
                    self.nodes[id].debug().replace('\n', " | ")
                )
            })
            .collect::<Vec<_>>()
            .join("")
    }

    /// 某节点已压缩到的 index。
    pub fn compacted_index(&self, id: u64) -> Option<u64> {
        self.nodes.get(&id).map(|n| n.storage.compacted_index())
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
        for (_, h) in self.handles.drain() {
            let _ = h.join(); // 退出时交还的邮箱在此丢弃（集群正在析构）
        }
        self.nodes.clear(); // 先放掉存储句柄（释放 fjall 目录锁），再删目录
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_node(
    id: u64,
    mailbox: Receiver<Message>,
    mailboxes: HashMap<u64, Sender<Message>>,
    sm: Arc<Mutex<CatalogState>>,
    storage: FjallStorage,
    role: Arc<Mutex<StateRole>>,
    applied: Arc<Mutex<u64>>,
    debug: Arc<Mutex<String>>,
    status: Arc<Mutex<NodeStatus>>,
    cmd_rx: Receiver<Command>,
) -> thread::JoinHandle<Receiver<Message>> {
    thread::spawn(move || {
        let logger = raft::default_logger();
        let cfg = Config {
            id,
            election_tick: 10,
            heartbeat_tick: 3,
            // 预投票：减少被隔离节点反复打断 leader（生产实现必须开）
            pre_vote: true,
            // ⚠️ **重启必须告诉 raft 已应用到哪**，否则它会重放已应用条目
            // （raft-rs `Config::applied` 文档原话："If Applied is unset when restarting,
            // raft might return previous applied entries"）。重放的后果**不报错**：
            // 状态机的计数器被重复推进（幂等 op 的状态不变，但 `last_applied`/版本号会多走），
            // 于是重启过的副本与其他副本**静默分叉** —— 本轮实测就是被 `set_applied` 的
            // 单调断言拦下的（storage.rs `assert!(index >= inner.applied_index)`）。
            applied: storage.snapshot_index(),
            ..Default::default()
        };
        // `RawNode` 拿走存储所有权；应用侧继续用 `storage` 句柄（同一份内部状态）
        let mut raw = RawNode::new(&cfg, storage.handle(), &logger).expect("raw node");
        *role.lock().unwrap() = raw.raft.state;

        // 已提议但尚未应用的 op（按提议顺序配对；只有 leader 会有内容）
        let mut pending: VecDeque<
            SyncSender<Result<yuntun_proto::meta::ProposeResponse, MetaError>>,
        > = VecDeque::new();
        let mut last_tick = Instant::now();
        let tick_every = Duration::from_millis(10);

        'node: loop {
            // ① 收消息
            loop {
                match mailbox.try_recv() {
                    Ok(msg) => {
                        let _ = raw.step(msg);
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break 'node,
                }
            }
            // ② 命令
            loop {
                match cmd_rx.try_recv() {
                    Ok(Command::Stop) => break 'node,
                    Ok(Command::Propose { op, reply }) => {
                        if raw.raft.state == StateRole::Leader {
                            // 顺序很重要：**先提议成功再入队**，否则队列与日志条目会错位
                            // （错位会让"应用 A 的 op 却回复了 B 的等待者" —— 静默错配）。
                            // 日志 payload = proto `Op` 的编码（**权威 op 格式**：
                            // 与 gRPC 面同构，"线上发的"和"盘上存的"是同一份定义）
                            match raw.propose(vec![], op.encode_to_vec()) {
                                Ok(()) => pending.push_back(reply),
                                Err(e) => {
                                    let _ = reply.send(Err(MetaError::Storage(
                                        format!("raft propose 失败：{e}"),
                                    )));
                                }
                            }
                        } else {
                            // 非 leader：必须可重试（约定 3），并带上 **leader hint** ——
                            // 客户端据此直接重试到正确节点，而不是盲目轮询（G2 的"写入不中断"靠它）。
                            let _ = reply.send(Err(MetaError::NotLeader {
                                leader_hint: raw.raft.leader_id,
                            }));
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break 'node,
                }
            }
            // ③ tick
            if last_tick.elapsed() >= tick_every {
                raw.tick();
                last_tick = Instant::now();
            }
            *role.lock().unwrap() = raw.raft.state;

            // 诊断快照（每轮刷新；PoC 用，S3-6 会有正式的 metrics 接口）
            {
                let mut prs: Vec<String> = raw
                    .raft
                    .prs()
                    .iter()
                    .map(|(id, pr)| {
                        format!(
                            "peer{id}(next={},matched={},state={:?},active={})",
                            pr.next_idx,
                            pr.matched,
                            pr.state,
                            pr.recent_active
                        )
                    })
                    .collect();
                prs.sort();
                {
                    let mut st = status.lock().unwrap();
                    st.node_id = id;
                    st.role = format!("{:?}", raw.raft.state);
                    st.term = raw.raft.term;
                    st.leader_id = raw.raft.leader_id;
                    st.commit_index = raw.raft.raft_log.committed;
                    st.applied_index = *applied.lock().unwrap();
                    st.snapshot_index = storage.compacted_index();
                    st.first_index = raw.raft.raft_log.first_index();
                    st.last_index = raw.raft.raft_log.last_index();
                }
                *debug.lock().unwrap() = format!(
                    "role={:?} term={} first={} last={} commit={} applied={} log_applied={} {}",
                    raw.raft.state,
                    raw.raft.term,
                    raw.raft.raft_log.first_index(),
                    raw.raft.raft_log.last_index(),
                    raw.raft.raft_log.committed,
                    *applied.lock().unwrap(),
                    raw.raft.raft_log.applied,
                    prs.join(" ")
                );
            }

            // ④ Ready 循环
            if raw.has_ready() {
                let mut rd = raw.ready();
                // 出站消息
                for msg in rd.take_messages() {
                    if let Some(tx) = mailboxes.get(&msg.to) {
                        let _ = tx.send(msg);
                    }
                }
                // 快照安装：**必须**在 advance 前完成（else raft 会认为已稳定）。
                //
                // 两步缺一不可：
                //   ① 状态机 ← 快照内容（`restore_snapshot` 做帧+载荷双重校验；
                //      损坏/截断**拒绝安装**而不是装半个）；
                //   ② 存储 ← 快照元数据（此后 `first_index`/`term` 由它决定）。
                if *rd.snapshot() != Snapshot::default() {
                    let snap = rd.snapshot().clone();
                    let restored = CatalogState::restore_snapshot(snap.get_data())
                        .unwrap_or_else(|e| panic!("[meta:{id}] 快照损坏，拒绝安装：{e}"));
                    *sm.lock().unwrap() = restored;
                    storage
                        .apply_snapshot(snap)
                        .unwrap_or_else(|e| fatal(id, "安装快照", e));
                }
                // 落盘（内存版；生产 = fjall：条目追加 + 硬状态）
                storage
                    .append(rd.entries())
                    .unwrap_or_else(|e| fatal(id, "追加日志", e));
                if let Some(hs) = rd.hs() {
                    storage
                        .set_hard_state(hs.clone())
                        .unwrap_or_else(|e| fatal(id, "持久化硬状态", e));
                }
                // 应用已提交条目
                let committed = rd.take_committed_entries();
                apply_committed(
                    id,
                    &mut raw,
                    committed,
                    &sm,
                    &storage,
                    &applied,
                    &mut pending,
                );
                // 持久化后的消息（follower 的 append 响应等）
                for msg in rd.take_persisted_messages() {
                    if let Some(tx) = mailboxes.get(&msg.to) {
                        let _ = tx.send(msg);
                    }
                }
                // ② advance：拿到 commit index 与下一批可应用条目
                let mut light = raw.advance(rd);
                if let Some(commit) = light.commit_index() {
                    storage
                        .set_commit(commit)
                        .unwrap_or_else(|e| fatal(id, "提交点落盘", e));
                }
                for msg in light.take_messages() {
                    if let Some(tx) = mailboxes.get(&msg.to) {
                        let _ = tx.send(msg);
                    }
                }
                let committed = light.take_committed_entries();
                apply_committed(
                    id,
                    &mut raw,
                    committed,
                    &sm,
                    &storage,
                    &applied,
                    &mut pending,
                );
                raw.advance_apply();
            }
            thread::sleep(Duration::from_millis(2));
        }
        // 退出时交还邮箱：让同一 id 可以被重新启动（S3-1b 的"落后节点重启"场景）
        mailbox
    })
}

/// 持久化失败的处理：**停机**，不是重试也不是忽略。
///
/// raft 层"已持久化"的假设一旦被打破，继续跑就会把"已 ack 但没落盘"的数据当成安全的
/// —— 那比停机危险得多。真实实现应把错误交给上层（记录 + 退出码），由运维决定恢复动作；
/// PoC 用 panic 表达"绝不带病继续"。
fn fatal(id: u64, what: &str, e: impl std::fmt::Debug) -> ! {
    panic!("[meta:{id}] {what} 落盘失败：{e:?} —— 持久化失败必须停机，不能带病继续")
}

/// 集群临时根目录（每节点一个子目录；`Cluster::drop` 清理）。
fn temp_root() -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("yuntun-meta-cluster-{nanos}"));
    std::fs::create_dir_all(&root).expect("建集群目录");
    root
}

fn apply_committed(
    id: u64,
    raw: &mut RawNode<FjallStorage>,
    entries: Vec<Entry>,
    sm: &Arc<Mutex<CatalogState>>,
    storage: &FjallStorage,
    applied: &Arc<Mutex<u64>>,
    pending: &mut VecDeque<
        SyncSender<Result<yuntun_proto::meta::ProposeResponse, MetaError>>,
    >,
) {
    for entry in entries {
        // **先按 raft 索引报告已应用位置**（含 no-op / ConfChange —— 它们也占索引，
        // 漏报会让压缩位置与日志错开一格）。刻意放在 `continue` 之前。
        storage
            .set_applied(entry.index)
            .unwrap_or_else(|e| fatal(id, "记录已应用位置", e));
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
                storage
                    .set_conf_state(cs)
                    .unwrap_or_else(|e| fatal(id, "成员表落盘", e));
            }
            continue;
        }
        // 日志 payload = proto `Op`（**权威 op 格式**：与 gRPC 面同构）。
        // "已提交但解不开"是致命配置错误：各副本都会在这里同样失败（确定性 ✓），
        // 但必须**大声记录**——静默跳过会让状态机落后于日志而没人知道。
        let op = match prost::Message::decode(&entry.data[..]) {
            Ok(o) => o,
            Err(e) => {
                eprintln!(
                    "[meta:{id}] 已提交的 op 无法解码（entry {}）：{e}",
                    entry.index
                );
                if let Some(reply) = pending.pop_front() {
                    let _ = reply.send(Err(MetaError::BadRequest(format!("op 解码失败：{e}"))));
                }
                continue;
            }
        };
        let state_op = match crate::op::decode_op(&op) {
            Ok(o) => o,
            Err(e) => {
                eprintln!("[meta:{id}] op 语义非法（entry {}）：{e}", entry.index);
                if let Some(reply) = pending.pop_front() {
                    let _ = reply.send(Err(e));
                }
                continue;
            }
        };
        let mut st = sm.lock().unwrap();
        let outcome = crate::op::apply(&mut st, &state_op);
        // 响应：revision = raft index（权威位置）+ **两组**版本号（约定 2）+ 快照号。
        // 注意：状态机内部仍按 op 计数自增 `last_applied`（standalone 语义），
        // 副本层的坐标由 `storage.set_applied(entry.index)` 单独维护 —— 两者别混用（§42.3）。
        let resp = match outcome {
            Ok(o) => Ok(yuntun_proto::meta::ProposeResponse {
                accepted: o.accepted,
                revision: entry.index,
                schema_ver: st.version().schema_ver,
                manifest_ver: st.version().manifest_ver,
                // 逐 op 的结果形状待定（operation-log §45.4）；关键数字已在上面几个字段
                result: Vec::new(),
                snapshot: st.current_snapshot(),
            }),
            Err(e) => Err(e),
        };
        drop(st);
        *applied.lock().unwrap() = entry.index;
        // leader 才持有等待者；换主后旧队列为空 → 跳过
        if let Some(reply) = pending.pop_front() {
            let _ = reply.send(resp);
        }
    }
}
