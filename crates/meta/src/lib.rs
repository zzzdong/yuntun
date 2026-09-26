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

pub mod cli;
pub mod error;
pub mod fjall_storage;
pub mod op;
pub mod remote_catalog;
pub mod service;
pub mod storage;
pub mod transport;

pub use cli::Args;
pub use error::{MetaError, MetaNodeError};
pub use fjall_storage::FjallStorage;
pub use op::{apply, decode_op, StateOp};
pub use remote_catalog::RemoteCatalog;
pub use service::{serve, MetaService};
pub use storage::MetaStorage;
pub use transport::{
    GrpcTransport, MpscTransport, NoTransport, PeerTransport, TransportStats, TransportStatsView,
};

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
use yuntun_model::meta::{FileManifest, TableMeta};
use prost::Message as _;

/// 三节点的固定成员表（PoC 用常量；生产由 `--init` / `Join` 决定）。
pub const PEERS: [u64; 3] = [1, 2, 3];

/// **提升 learner 时"落后多少算太多"**（`§120`）：`matched` 距日志末尾超过它就拒绝提升。
///
/// 取值是**工程判断**，不是定理：它不是正确性阈值（哪怕落后 1 条，提升也只是让 quorum 略紧），
/// 而是"**别把可用性押在一个还没追上的节点上**"的旋钮。取 64 ≈ 一次快照之后正常的追赶量级：
/// 正常复制（本机 µs、跨机 ms）几乎瞬间就落在范围内，真落后的节点会明显超出。
/// 生产应当由配置给（与 `--snapshot-log-entries` 同类），PoC 用常量。
const PROMOTE_MAX_LAG: u64 = 64;

/// 逐条轨迹的总开关（`YUNTUN_META_TRACE`）。**全局只在**这里定义一次，各模块都用它。
///
/// ⚠️ **空值 / `0` 都算关**：编排（podman-compose / k8s）常写成 `- YUNTUN_META_TRACE=${VAR:-}`
/// —— 那种写法会给出"**已设置但为空**"的变量，用 `is_ok()` 判就会被当成"要打轨迹"，
/// 于是容器里每条消息一行 `eprintln!`，真实集群的 stdout 管道很快塞满 ⇒ **进程被写阻塞**、
/// 集群根本起不来（`§108` 实测踩过）。所以判据是"非空且不是 0"。
///
/// ⚠️ 也**别在真实集群里默认打开**：它的量级是"每条消息一行"，只适合在需要时临时开
/// （`tests/cluster.sh` 里用 `YUNTUN_META_TRACE=1` 按需注入）。
pub(crate) fn trace_on() -> bool {
    matches!(std::env::var("YUNTUN_META_TRACE").as_deref(), Ok(v) if !v.is_empty() && v != "0")
}

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
    /// 把一个节点作为 **learner** 加进集群（成员变更，`§118`）。
    ///
    /// 回复 = **提议到的 raft index**（不是"已应用"）：成员变更要等那条 conf change 提交并应用
    /// 才生效，调用方随后用 [`NodeHandle::members`] 看结果（learner 不参与多数派 ⇒ 加入本身
    /// 不改 quorum，所以"集群里已经有 3 个 voter"时它照样能提交）。
    AddLearner {
        id: u64,
        addr: String,
        reply: SyncSender<Result<(), MetaError>>,
    },
    /// 把一个 **learner 提升为 voter**（`§120`）。
    ///
    /// 与 [`Command::AddLearner`] 走**同一条通路**（conf change），区别只在 `ConfChangeType`
    /// 与**两道门槛**（见 [`promote_checked`]）。回复 = 提议成功与否；生效要等应用。
    Promote {
        id: u64,
        reply: SyncSender<Result<(), MetaError>>,
    },
    /// 当前成员表快照（[`MembersView`]）。
    Members {
        reply: SyncSender<MembersView>,
    },
    /// 停止该节点（模拟崩溃：线程退出、消息不再收发）。
    Stop,
}

/// 成员表快照（`Members` 命令的回复，`§118`）。
#[derive(Debug, Clone, Default)]
pub struct MembersView {
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    /// **已知的** `id → 地址`：来自启动配置（`--peer`）**以及**后来每一次 conf change 带过来的地址。
    /// 可能不全（没配过地址的成员不在这里）—— 别当权威成员表用，权威是 `voters`/`learners`。
    pub addrs: HashMap<u64, String>,
}

/// 节点运行时的可观测句柄（测试用）。
struct NodeState {
    /// 本节点的 raft 收件箱（`Cluster::handle` 构造 `NodeHandle` 时要用）
    inbox_tx: Sender<Message>,
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
    /// **数据节点存活表**（T12.3）：`instance_id` → 最后一次心跳。
    ///
    /// 三条纪律都体现在这里：
    /// 1. **只在内存**：秒级心跳写 raft 会把写路径压垮（`§3.2`）；每个副本各记一份；
    /// 2. **不参与快照/状态机**：存活是瞬时信息，进快照就等于把"此刻谁活着"固化下来 ——
    ///    与"名录必须与 schema/manifest 同版本"是两码事（名录走 raft，存活走内存）；
    /// 3. 用 `Instant`（单调钟）：存活判定不该被墙钟跳变影响。
    pub last_seen: std::collections::HashMap<String, std::time::Instant>,
    pub role: String,
    pub term: u64,
    /// 0 = 未知（客户端据此重试到正确节点）
    pub leader_id: u64,
    pub commit_index: u64,
    pub applied_index: u64,
    pub snapshot_index: u64,
    /// **成员表**（`§120`）：voters / learners（权威来源是 raft 的 `ConfState`）。
    ///
    /// 放进 Status 的理由与租约同款：**"谁在集群里"必须可观测** —— 提升/移除这类成员变更
    /// 没有它只能靠猜（进程级用例也靠它断言）。
    /// 刻意**不放地址表**：那玩意儿会变、而且对高频快照没意义（要看地址用 `Meta.Join`
    /// 的回包或 `Members` 命令）。
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
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
    /// raft 收件箱：`Meta.Raft`（节点间）收到的消息从这里进 raft 线程。
    raft_inbox: Sender<Message>,
}

impl NodeHandle {
    pub fn id(&self) -> u64 {
        self.id
    }

    /// **心跳**：登记某数据节点还活着；返回它是否**在名录里**。
    ///
    /// `false` 的语义很重要：调用方应当**重新注册**（而不是继续发心跳）——
    /// 否则一个被摘除的节点会永远安静地"心跳"下去，却不在任何人的名录里。
    pub fn heartbeat(&self, instance_id: &str) -> bool {
        let known = self
            .sm
            .lock()
            .unwrap()
            .datanodes()
            .contains_key(instance_id);
        self.status
            .lock()
            .unwrap()
            .last_seen
            .insert(instance_id.to_string(), std::time::Instant::now());
        known
    }

    /// 巡检用：每个成员的最后一次心跳（缺席 = 本节点从未见过它）。
    pub fn last_seen(&self) -> std::collections::HashMap<String, std::time::Instant> {
        self.status.lock().unwrap().last_seen.clone()
    }

    /// **本节点是否当前 leader**（心跳巡检用：只有 leader 才提议摘除，follower 提议会撞一致性）。
    ///
    /// 用 `leader_id == self.id` 判定，而不是比 `role` 字符串：字符串是给人看的，
    /// 且 `leader_id == 0` 表示"未知" ⇒ 未知时一律不提议（宁可晚摘，不可错摘）。
    pub fn is_leader(&self) -> bool {
        let st = self.status.lock().unwrap();
        st.leader_id != 0 && st.leader_id == self.id
    }

    /// 运维状态（`Status` RPC 的返回）。
    pub fn status(&self) -> yuntun_proto::meta::StatusResponse {
        // 先读**状态机**（另一个锁），再读 status 锁：锁序固定为 sm → status。
        // 在持有 status 时去取 sm，会和别处相反的取法撞成死锁。
        let leases = self
            .sm
            .lock()
            .unwrap()
            .leases()
            .values()
            .map(|l| yuntun_proto::meta::LeaseView {
                purpose: l.purpose.clone(),
                holder: l.holder.clone(),
                epoch: l.epoch,
                expires_at_ms: l.expires_at_ms,
            })
            .collect();
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
            leases,
            // 成员表（`§120`）：取自驱动循环维护的快照，**不在这里现算** ——
            // Status 是高频只读路径（心跳/排障/客户端落点都在用），不该顺带加锁去读 raft。
            voter_ids: s.voters.clone(),
            learner_ids: s.learners.clone(),
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

    /// 把一个节点作为 **learner** 加进集群（成员变更，`§118`）。**提议成功**即返回。
    ///
    /// 要等那条 conf change 提交并应用才生效 —— 用 [`Self::members`] 看结果。
    /// ⚠️ 同一时刻**只允许一条未提交的 conf change** 在途（raft 的纪律）：本接口不排队，
    /// 由调用方（`Meta.Join` 是低频人工操作、测试亦然）自己保证串行。
    pub fn add_learner(&self, id: u64, addr: &str, timeout: Duration) -> Result<(), MetaError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.cmd_tx
            .send(Command::AddLearner {
                id,
                addr: addr.to_string(),
                reply: reply_tx,
            })
            .map_err(|e| MetaError::Storage(format!("命令通道断开：{e}")))?;
        match reply_rx.recv_timeout(timeout) {
            Ok(r) => r,
            Err(_) => Err(MetaError::NoQuorum),
        }
    }

    /// 把一个 **learner 提升为 voter**（`§120`）。**提议成功**即返回，生效要等应用
    /// （用 [`Self::members`] 看结果）。
    ///
    /// ⚠️ 与 [`Self::add_learner`] 同一条纪律：同一时刻**只允许一条未提交的 conf change**
    /// 在途，本接口不排队。
    pub fn promote(&self, id: u64, timeout: Duration) -> Result<(), MetaError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.cmd_tx
            .send(Command::Promote { id, reply: reply_tx })
            .map_err(|e| MetaError::Storage(format!("命令通道断开：{e}")))?;
        match reply_rx.recv_timeout(timeout) {
            Ok(r) => r,
            Err(_) => Err(MetaError::NoQuorum),
        }
    }

    /// 当前成员表快照（`§118`）：voters / learners / **已知地址**。
    pub fn members(&self, timeout: Duration) -> Result<MembersView, MetaError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.cmd_tx
            .send(Command::Members { reply: reply_tx })
            .map_err(|e| MetaError::Storage(format!("命令通道断开：{e}")))?;
        match reply_rx.recv_timeout(timeout) {
            Ok(v) => Ok(v),
            Err(_) => Err(MetaError::NoQuorum),
        }
    }

    /// 清单增量（`Delta` RPC）：只报"自 `since` 起变过的表"。
    /// 清单增量（`Delta` RPC）。
    ///
    /// ⚠️ **结构变更也要算"要全量"**：`changed_tables` 只覆盖**manifest 级**变化
    /// （每表 `manifest_ver` 推进），而 `create_table`/`drop_table` 只动 `schema_ver`
    /// —— 新建的表**不在 `changed_tables` 里**。少这一条，客户端做增量刷新就会
    /// **静默丢掉刚建的表**（`RemoteCatalog` 实测踩过，见 `§55.1`）。
    pub fn delta(
        &self,
        since_manifest_ver: u64,
        since_schema_ver: u64,
    ) -> yuntun_proto::meta::DeltaResponse {
        let st = self.sm.lock().unwrap();
        let v = st.version();
        let d = st.manifest_delta(since_manifest_ver);
        yuntun_proto::meta::DeltaResponse {
            schema_ver: v.schema_ver,
            manifest_ver: v.manifest_ver,
            changed_tables: d.changed_tables,
            full_reload: d.full_reload_required || since_schema_ver != v.schema_ver,
        }
    }

    /// 把一条**节点间** raft 消息交给本节点的 raft 线程。
    ///
    /// 返回 `false` = 线程已退出（正常关停或崩溃）—— 调用方（`Meta.Raft`）据此回
    /// `delivered=false`，让对端能统计出"发了但没人收"（见 `transport` 模块的计数说明）。
    pub fn deliver(&self, msg: Message) -> bool {
        self.raft_inbox.send(msg).is_ok()
    }

    /// 读：**版本探测 + 刷新载荷**（`Prefetch` RPC）。
    ///
    /// 语义的权威口径写在 proto（`PrefetchRequest` 上方那张表）—— 这里只落实它。
    pub fn prefetch(
        &self,
        req: &yuntun_proto::meta::PrefetchRequest,
    ) -> yuntun_proto::meta::PrefetchResponse {
        let st = self.sm.lock().unwrap();
        let v = st.version();

        // 客户端版本**超前** = 它手里的不是本集群的状态（换过集群 / 被回滚 / 数据被换过）。
        // 此时**必须**要求全量重建：静默返回空会让客户端以为"一切照旧"，
        // 于是继续拿一份不属于本集群的缓存去规划查询 —— 错得**不报错**。
        let ahead = req.since_schema_ver > v.schema_ver || req.since_manifest_ver > v.manifest_ver;
        let full = req.full || ahead;

        let tables: Vec<TableMeta> = if full {
            st.list_tables()
        } else {
            // 只回**请求里存在**的表：请求了却不在载荷里 = 已删（这是契约的一部分，
            // 所以不能"补空占位"—— 补了客户端就分不清"没请求"与"已删"）
            req.tables.iter().filter_map(|t| st.get_table(t)).collect()
        };

        // 文件级增量（含墓碑）。
        //
        // 口径与表载荷一致：`tables` 空且不是全量 → **零载荷**（纯版本探测）。
        // 否则按请求的表逐个取「自 `since_snapshot` 起变过的文件」。
        let mut files: Vec<FileManifest> = if req.tables.is_empty() && !full {
            Vec::new()
        } else if full {
            st.files_since(None, req.since_snapshot)
        } else {
            req.tables
                .iter()
                .flat_map(|t| st.files_since(Some(t), req.since_snapshot))
                .collect()
        };
        // 规范化顺序（同一请求 → 同一字节）：便于对拍与断言，
        // 也让"客户端缓存打补丁"的路径可复现。
        files.sort_by(|a, b| (&a.table, &a.batch_id).cmp(&(&b.table, &b.batch_id)));

        // 幂等键集合：**只在全量刷新时给**，并有条数上限。
        // 上限是必须的：键集合随写入增长（TTL 内），全量重放一个十万级的列表会把
        // "刷新"变成一次大传输。超限时**明确告警**并截断（截断只影响快路径命中率，不影响正确性）。
        const MAX_KEYS: usize = 10_000;
        let keys = if full {
            let mut k = st.idempotency_keys();
            if k.len() > MAX_KEYS {
                eprintln!(
                    "[meta] 幂等键 {} 条超过载荷上限 {MAX_KEYS}：本次只发前 {MAX_KEYS} 条                      （只影响客户端快路径命中率，不影响正确性 —— 权威仍在状态机）",
                    k.len()
                );
                k.truncate(MAX_KEYS);
            }
            k
        } else {
            Vec::new()
        };

        yuntun_proto::meta::PrefetchResponse {
            schema_ver: v.schema_ver,
            manifest_ver: v.manifest_ver,
            snapshot: st.current_snapshot(),
            payload: Some(yuntun_proto::meta::PrefetchPayload {
                tables: tables.iter().map(crate::op::table_meta_to_proto).collect(),
                files: files
                    .iter()
                    .map(|f| yuntun_proto::meta::FileEntry {
                        batch_id: f.batch_id.clone(),
                        manifest: Some(crate::op::manifest_to_proto_pub(f)),
                    })
                    .collect(),
                // 整体替换语义（客户端据此丢弃已删 schema）
                namespaces: st.list_schemas(),
                // 名录与 schema/manifest **同一次响应**（`§3.1`）：分成两次读会出现
                // "新文件清单 + 旧节点集合"的拼计划窗口。
                datanodes: st
                    .datanodes()
                    .values()
                    .map(crate::op::datanode_member_to_proto)
                    .collect(),
                idempotency_keys: keys,
            }),
            full_reload: full,
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
                // 进程内簇 = "本地传输"（设计 §3.3 说的那种）：仍然走 `PeerTransport`
                // 抽象，所以"换成网络"不需要改 raft 循环一行。
                Arc::new(MpscTransport::new(self.mailboxes.clone())),
                sm.clone(),
                storage.handle(),
                role.clone(),
                applied.clone(),
                debug.clone(),
                status.clone(),
                cmd_rx,
                // 进程内簇没有地址（`MpscTransport` 按邮箱接线）⇒ 成员表里先只放 id、地址留空
                self.mailboxes
                    .keys()
                    .map(|id| (*id, String::new()))
                    .collect(),
                // 进程内测试簇**不自动压缩**：它的用例靠手动 `Cluster::compact` 精确控制
                // "压到哪一条"，自动触发会让那些用例的确定性变差（且它们写的条目远少于阈值）。
                MetaOptions {
                    compact_log_entries: 0,
                    // 进程内簇没有地址（`MpscTransport` 按邮箱接线，`§118`/`§119`）
                    self_addr: String::new(),
                },
            ),
        );
        // 本节点的收件箱 = `Cluster::start` 为它建的邮箱发送端
        //（必须在 `insert` 前取出来：`self.nodes` 的可变借用与 `self.mailboxes` 冲突）
        let inbox_tx = self
            .mailboxes
            .get(&id)
            .cloned()
            .expect("邮箱必须已建（Cluster::start 为每个 peer 建过）");
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
                inbox_tx,
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
        if let Some(h) = self.handles.remove(&id)
            && let Ok(rx) = h.join()
        {
            self.receivers.insert(id, rx);
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
            raft_inbox: n.inbox_tx.clone(),
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
/// **单节点 metanode 运行时**（进程用；S3-3 的启动路径）。
///
/// 与 [`Cluster`]（测试用的进程内三节点）的区别：
///
/// | | `Cluster` | `MetaNode` |
/// |---|---|---|
/// | 成员表 | 硬编码 `PEERS` | 调用方给（CLI/配置） |
/// | 目录 | 临时目录，`Drop` 清理 | 调用方给，**永不自动删** |
/// | 规模 | 3 节点（进程内邮箱） | **只支持单 voter**（多节点要网络传输，下一步） |
/// | 用途 | 验证机制 | 真正跑起来 |
///
/// **存活巡检**（T12.3）：把"连续超时没心跳"的成员从名录里摘掉（**走 raft 的 op**）。
///
/// 三条纪律：
/// 1. **只有 leader 提议**（follower 提议会撞一致性）；并且每次提议前重新判一次 ——
///    巡检整轮期间可能已经换主；
/// 2. **先播种、后判定**：名录里第一次见到的成员按"此刻还活着"记一笔。这一条防的是
///    换主/巡检刚落地的瞬间把**全集群**一起摘掉（新 leader 的内存存活表是空的）；
/// 3. **摘除走 op**：名录变更必须与 schema/manifest 同版本传下去（`§3.1`）——
///    而**触发它的心跳**不进 raft（`§3.2`）。这两件事在代码里必须分开，否则要么写爆 raft，
///    要么让各副本对"谁存在"产生分歧。
///
/// **在途登记的 TTL**（毫秒）：这是 grace 原本承担的那个职责（`§83`）。
///
/// 取 1h（与旧 grace 同量级）—— 它要盖住"写者从登记到提交"的**最坏**时长
/// （大文件 / S3 抖动 / 长 GC）。太长 ⇒ 崩溃写者的残局保护过头（只是留着垃圾）；
/// 太短 ⇒ 可能把**正在写**的文件放开给 GC（**真丢数据**）。两个方向不对称，
/// 所以宁长勿短。
const IN_FLIGHT_TTL_MS: u64 = 3_600_000;

/// 墙钟毫秒（**发起方打点**：状态机不读钟 —— 时刻随 op 过线）。
fn now_ms_wall() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn spawn_liveness_sweep(
    handle: NodeHandle,
    timeout: std::time::Duration,
    interval: std::time::Duration,
    shutdown: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if shutdown.is_cancelled() {
                break;
            }
            tokio::time::sleep(interval).await;

            let hb = handle.clone();
            // `propose` 是**阻塞**的（等提交），挪到阻塞线程池，别占着异步 worker
            let _ = tokio::task::spawn_blocking(move || {
                if !hb.is_leader() {
                    return;
                }

                // 在途登记的 TTL 清扫（`§83.6` 遗留①）：与"摘除数据节点"**同一条巡检、
                // 同一个 leader 闸门** —— 触发者是巡检（不进 raft），但结果（撤销保护）
                // 必须进 raft（否则各节点对"还算不算在途"各说各话 ⇒ GC 按自己的理解删 ✗）。
                //
                // ⚠️ 只在**确实有过期条目**时才提议：否则每个 tick 都白写一次 raft。
                let expired = hb
                    .sm
                    .lock()
                    .unwrap()
                    .count_expired_in_flight(IN_FLIGHT_TTL_MS, now_ms_wall());
                if expired > 0 {
                    let _ = hb.propose(
                        yuntun_proto::meta::Op {
                            now_ms: now_ms_wall(),
                            kind: Some(yuntun_proto::meta::op::Kind::SweepInFlight(
                                yuntun_proto::meta::SweepInFlightOp {
                                    ttl_ms: IN_FLIGHT_TTL_MS,
                                },
                            )),
                        },
                        std::time::Duration::from_secs(5),
                    );
                }
                let roster: Vec<String> = hb
                    .sm
                    .lock()
                    .unwrap()
                    .datanodes()
                    .keys()
                    .cloned()
                    .collect();
                for id in roster {
                    let stale = {
                        let mut st = hb.status.lock().unwrap();
                        let now = std::time::Instant::now();
                        let seen = st.last_seen.entry(id.clone()).or_insert(now);
                        now.duration_since(*seen) > timeout
                    };
                    if !stale {
                        continue;
                    }
                    let op = yuntun_proto::meta::Op {
                        now_ms: yuntun_model::batch::now_ms(),
                        kind: Some(yuntun_proto::meta::op::Kind::RemoveDatanode(
                            yuntun_proto::meta::RemoveDatanodeOp {
                                instance_id: id.clone(),
                                reason: format!("心跳超时（>{:?} 未收到）", timeout),
                            },
                        )),
                    };
                    match hb.propose(op, std::time::Duration::from_secs(5)) {
                        Ok(_) => {
                            // 摘掉存活记录：它若重新注册，会重新开始心跳
                            hb.status.lock().unwrap().last_seen.remove(&id);
                            tracing::warn!(instance = %id, "datanode evicted: heartbeat timeout");
                        }
                        // **不**在失败时清存活记录：下一轮还会看到它仍超时，于是重试
                        Err(e) => tracing::warn!(instance = %id, error = %e, "摘除提议失败，下轮重试"),
                    }
                }
            })
            .await;
        }
    })
}

/// 关键点：落盘与恢复**完全共用**同一条链路（`FjallStorage::open_with_state` + `spawn_node`），
/// 所以集群用例里验过的"重启后逐字节一致"对进程同样成立 —— 这也是进程级用例
/// （`tests/metanode_process_e2e.rs`）能直接断言状态恢复、而不必重新证明一遍机制的原因。
pub struct MetaNode {
    id: u64,
    handle: NodeHandle,
    cmd_tx: Sender<Command>,
    thread: Option<thread::JoinHandle<Receiver<Message>>>,
    /// 出站传输计数（多节点时才有意义；单节点恒为 0）。
    /// 暴露它是为了让"传输到底有没有work"**可断言**，而不是只能看日志。
    stats: Arc<TransportStats>,
}

/// 节点的运行期策略（`Default` = 按设计 `metanode-design §4.4` 取值）。
///
/// 为什么要有这个、而不是把常数写死在驱动循环里：**压缩触发必须在集成层可复现**。
/// `§42.4b` 记的"稳定触发快照安装未拿到"，根因就是驱动层**根本没有触发**
/// （`compact_applied` 只被测试用的 `Cluster::compact` 调过）—— 于是进程形态的日志只增不减、
/// "落后节点靠快照追上"这条路径在真实部署里**永远走不到**（`operation-log §105`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaOptions {
    /// 日志条数（`last_index - first_index + 1`）超过它就**压缩到已应用位置**
    /// （生成快照产物 + 丢弃老日志）。`0` = 关（测试簇改用手动 `Cluster::compact`）。
    ///
    /// ⚠️ 数的是**日志条数**，不是 op 数：no-op 与 ConfChange 也占索引（`storage.rs` 的坐标纪律）。
    /// 设计 §4.4 的另一个判据（状态 > 256MB）**未实现**，见 `operation-log §105.4`。
    pub compact_log_entries: usize,

    /// **本节点的对外地址**（`§119`）：成员表里"自己"那一项用它。
    ///
    /// 为什么需要：新节点从 `Meta.Join` 回包里拿到的成员表**必须包含 leader 自己的地址** ——
    /// 否则它连"回一条 `AppendResponse`"都发不出去（learner 不发主消息，但要回执）。
    /// 空串 = 未知（进程内测试簇就是空的：它按邮箱接线，没有地址这回事）。
    pub self_addr: String,
}

impl Default for MetaOptions {
    fn default() -> Self {
        Self {
            // `§119` 之前的调用点没有"自己的地址"这回事（进程内测试簇按邮箱接线）；
            // 生产由 `metanode` 从 `--advertise` / `--listen` 填进来。
            self_addr: String::new(),
            // 设计 §4.4 定的上界：日志条数 > 10 万
            compact_log_entries: 100_000,
        }
    }
}

impl MetaNode {
    /// 打开（或按盘上状态恢复）一个 metanode（**按设计的默认策略**，见 [`MetaOptions`]）。
    ///
    /// 要调策略（比如测试里把压缩阈值调小，以便**稳定复现**快照路径）用 [`Self::open_with`]。
    pub fn open(
        dir: impl AsRef<std::path::Path>,
        id: u64,
        voters: Vec<u64>,
        peers: HashMap<u64, String>,
    ) -> Result<Self, MetaNodeError> {
        Self::open_with(dir, id, voters, peers, MetaOptions::default())
    }

    /// 打开（或按盘上状态恢复）一个 metanode，**带运行期策略**。
    ///
    /// 四道检查都在**动手之前**做完，任何一道不过就拒绝启动：
    ///
    /// 1. 存储能打开（目录权限/被占用/快照损坏）；
    /// 2. 盘上成员表与本次配置一致（否则本节点会在一个凑不齐的组里静默空转）；
    /// 3. 成员表里的**其他** voter 都有地址（缺了 → [`MetaNodeError::MissingPeer`]）；
    /// 4. 多节点时处于 tokio 运行时上下文（要起发送任务）。
    ///
    /// # 参数
    ///
    /// - `voters`：成员表（**持久化状态**，只在首次写入；之后必须与盘上一致）。
    /// - `peers`：`节点 id → "host:port"`。**只需列别的节点**（列了自己会被忽略）。
    ///   单节点传空表：这时不碰网络、也不需要 tokio 上下文。
    /// - `opts`：运行期策略（默认 = 设计取值）。
    pub fn open_with(
        dir: impl AsRef<std::path::Path>,
        id: u64,
        voters: Vec<u64>,
        peers: HashMap<u64, String>,
        opts: MetaOptions,
    ) -> Result<Self, MetaNodeError> {
        let dir = dir.as_ref();
        let (storage, sm) = FjallStorage::open_with_state(dir, id, voters.clone())
            .map_err(|e| MetaNodeError::Storage(e.to_string()))?;

        // ② 成员表必须一致（成员表是**持久化状态**，不随启动参数改变）
        let mut requested = voters;
        requested.sort_unstable();
        requested.dedup();
        let stored = storage.voters();
        if stored != requested {
            return Err(MetaNodeError::MembershipMismatch { stored, requested });
        }

        // ③ 每个"别的 voter"都必须有地址。
        //    缺地址的后果**不报错**：消息发不出去 → 永远选不出 leader（最难查的那种故障），
        //    所以宁可拒绝启动，并把**缺哪个**列出来。
        let mut peers = peers;
        peers.remove(&id); // 列了自己也无妨（常见的复制粘贴写法），忽略即可
        // 成员表的**初始地址**：`peers` 后来会被搬进传输，这里先留一份（`§118`）
        let members_init = peers.clone();
        let missing: Vec<u64> = stored
            .iter()
            .copied()
            .filter(|v| *v != id && !peers.contains_key(v))
            .collect();
        if !missing.is_empty() {
            return Err(MetaNodeError::MissingPeer { missing });
        }

        // ④ 传输层：单节点不需要网络（且**不要求** tokio 上下文）；多节点必须有运行时。
        let (inbox_tx, rx) = mpsc::channel::<Message>();
        let stats = Arc::new(TransportStats::default());
        let transport: Arc<dyn PeerTransport> = if peers.is_empty() {
            Arc::new(NoTransport)
        } else {
            let handle = tokio::runtime::Handle::try_current().map_err(|_| {
                MetaNodeError::Transport(
                    "多节点传输需要 tokio 运行时上下文：请在 runtime 内调用 MetaNode::open\
                     （单节点不需要，见 main.rs 的启动顺序）"
                        .into(),
                )
            })?;
            Arc::new(
                GrpcTransport::new(&handle, peers, stats.clone())
                    .map_err(MetaNodeError::Transport)?,
            )
        };

        let role = Arc::new(Mutex::new(StateRole::Follower));
        let applied = Arc::new(Mutex::new(0u64));
        let debug = Arc::new(Mutex::new(String::new()));
        let status = Arc::new(Mutex::new(NodeStatus {
            node_id: id,
            ..Default::default()
        }));
        let (cmd_tx, cmd_rx) = mpsc::channel::<Command>();
        let handle = NodeHandle {
            id,
            status: status.clone(),
            sm: sm.clone(),
            storage: storage.handle(),
            cmd_tx: cmd_tx.clone(),
            raft_inbox: inbox_tx,
        };
        let thread = spawn_node(
            id, rx, transport, sm, storage, role, applied, debug, status, cmd_rx, members_init, opts,
        );
        Ok(Self {
            id,
            handle,
            cmd_tx,
            thread: Some(thread),
            stats,
        })
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// 出站传输计数快照（多节点才有意义）。
    ///
    /// 用例/运维靠它区分"根本没发"与"发了但对方没收到" —— 这两种在现象上都是
    /// "集群不动"，原因却完全不同。
    pub fn transport_stats(&self) -> TransportStatsView {
        self.stats.view()
    }

    /// 节点句柄（给 gRPC 服务层用）。
    pub fn handle(&self) -> NodeHandle {
        self.handle.clone()
    }

    /// 等本节点成为 leader（单节点组启动后几十毫秒内必然发生）。
    ///
    /// 启动路径需要它：**在选出 leader 之前接受请求只会全部收到 `NotLeader`**，
    /// 客户端会以为"服务起来了但一直失败"。
    ///
    /// ⚠️ **只对单节点组成立**：raft 同一时刻只有一个 leader，多节点组里的 follower
    /// **永远**等不到"自己当选" ⇒ 拿它当多节点的启动闸门会让每个 follower 超时退出
    /// （`§103` 就是踩了这个坑才顺出来）。多节点用 [`Self::wait_any_leader`]。
    pub fn wait_leader(&self, timeout: std::time::Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.handle.status().role == "Leader" {
                return true;
            }
            thread::sleep(Duration::from_millis(10));
        }
        false
    }

    /// 等**集群里出现 leader**（不要求是自己），返回它的 id；超时返回 `None`。
    ///
    /// 与 [`Self::wait_leader`] 的分工是硬的：`leader_id == 0` 表示"未知"
    /// （客户端据此重试到正确节点），所以这里的判据是 `leader_id != 0` ——
    /// 多节点组里 follower 合法地不是 leader，但"集群已经有了 leader"对**每个**节点都成立。
    pub fn wait_any_leader(&self, timeout: std::time::Duration) -> Option<u64> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            let leader = self.handle.status().leader_id;
            if leader != 0 {
                return Some(leader);
            }
            thread::sleep(Duration::from_millis(10));
        }
        None
    }

    /// 停机：让 raft 线程退出并 join（模拟"干净关闭"；`kill -9` 走的是另一条路）。
    pub fn shutdown(mut self) {
        let _ = self.cmd_tx.send(Command::Stop);
        if let Some(h) = self.thread.take() {
            let _ = h.join();
        }
    }
}

impl Drop for MetaNode {
    fn drop(&mut self) {
        // 不 join 就等于进程退出时线程被强杀 —— 测试里会造成"偶发"的 fjall 目录锁残留。
        // 所以 Drop 也走一遍停止流程（幂等：thread 取走后为 None）。
        let _ = self.cmd_tx.send(Command::Stop);
        if let Some(h) = self.thread.take() {
            let _ = h.join();
        }
    }
}

/// 起一个 raft 驱动线程。
///
/// 参数多是**有意**的（与 `Cluster::spawn_with` 同款处理）：这里就是把一个节点的全部
/// 运行期句柄显式交出去 —— 收进结构体反而会掩盖"谁共享了什么"。
#[allow(clippy::too_many_arguments)]
fn spawn_node(
    id: u64,
    mailbox: Receiver<Message>,
    transport: Arc<dyn PeerTransport>,
    sm: Arc<Mutex<CatalogState>>,
    storage: FjallStorage,
    role: Arc<Mutex<StateRole>>,
    applied: Arc<Mutex<u64>>,
    debug: Arc<Mutex<String>>,
    status: Arc<Mutex<NodeStatus>>,
    cmd_rx: Receiver<Command>,
    // 初始 `id → 地址`（来自 `--peer` / 进程内簇；`§118` 起成员变更会在运行期往里加）
    members_init: HashMap<u64, String>,
    opts: MetaOptions,
) -> thread::JoinHandle<Receiver<Message>> {
    // 成员表（`§119`）：**先吃盘上的**（历次 conf change 带过来的地址 —— 那里面才有"后来加入的
    // 成员"），再用启动配置覆盖（`--peer` 只列初始 voter）。冲突时以**运维显式给的**为准。
    let mut members = storage.peer_addrs();
    members.extend(members_init);
    // **把自己也算进成员表**（`§119`）：地址来自 `--advertise`（见 `MetaOptions::self_addr`）。
    if !opts.self_addr.is_empty() {
        members.insert(id, opts.self_addr.clone());
    }
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

        // 【视图快照】上次打印的时刻（见循环末尾；`YUNTUN_META_TRACE=1` 时每秒一行）
        let mut last_snap: Option<Instant> = None;
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
                    Ok(Command::AddLearner { id: nid, addr, reply }) => {
                        if raw.raft.state == StateRole::Leader {
                            let cc = raft::eraftpb::ConfChange {
                                change_type: raft::eraftpb::ConfChangeType::AddLearnerNode,
                                node_id: nid,
                                // **地址随条目复制**：每个节点应用这条 conf change 时都学到它
                                context: addr.clone().into_bytes().into(),
                                ..Default::default()
                            };
                            match raw.propose_conf_change(vec![], cc) {
                                Ok(()) => {
                                    let _ = reply.send(Ok(()));
                                }
                                Err(e) => {
                                    let _ = reply.send(Err(MetaError::Storage(format!(
                                        "conf change 提议失败：{e}"
                                    ))));
                                }
                            }
                        } else {
                            let _ = reply.send(Err(MetaError::NotLeader {
                                leader_hint: raw.raft.leader_id,
                            }));
                        }
                    }
                    Ok(Command::Promote { id: nid, reply }) => {
                        let r = if raw.raft.state != StateRole::Leader {
                            Err(MetaError::NotLeader {
                                leader_hint: raw.raft.leader_id,
                            })
                        } else {
                            promote_checked(&mut raw, &storage, &members, nid)
                        };
                        let _ = reply.send(r);
                    }
                    Ok(Command::Members { reply }) => {
                        let cs = storage.conf_state();
                        let _ = reply.send(MembersView {
                            voters: sorted_ids(cs.voters.clone()),
                            learners: sorted_ids(cs.learners.clone()),
                            addrs: members.clone(),
                        });
                    }
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
                    // 成员表（`§120`）：每次刷新都从**权威**的 `ConfState` 取一份（几毫秒一次、
                    // 几个编号，代价可以忽略；换来的是"提升/移除之后 Status 立刻能看见"）。
                    let cs = storage.conf_state();
                    st.voters = sorted_ids(cs.voters.clone());
                    st.learners = sorted_ids(cs.learners.clone());
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
                // 出站消息 → 传输层。
                //
                // ⚠️ 这里仍在 **raft 线程** 上：传输层的 `send` 必须非阻塞
                // （`GrpcTransport` 只做出站队列 `try_send`，满了就丢 —— raft 会重发）。
                // 若在这里 await 网络，tick 会被拖住 = 别人选你当 leader 时你没反应。
                for msg in rd.take_messages() {
                    transport.send(msg.to, msg);
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
                    &transport,
                    &mut members,
                );
                // 持久化后的消息（follower 的 append 响应等）—— 同样走后端传输、
                // 同样**不阻塞**（这几条必须在落盘后才发，晚一点没关系，卡住才致命）
                for msg in rd.take_persisted_messages() {
                    transport.send(msg.to, msg);
                }
                // ② advance：拿到 commit index 与下一批可应用条目
                let mut light = raw.advance(rd);
                if let Some(commit) = light.commit_index() {
                    storage
                        .set_commit(commit)
                        .unwrap_or_else(|e| fatal(id, "提交点落盘", e));
                }
                // LightReady 的消息**必须**发出去：`advance` 的轻量批次里带的是
                // "落盘后生成"的响应（如 follower 的 append 回复）——漏发会表现为
                // "leader 一直等不到多数派确认"（写不进去，且没有任何错误）
                for msg in light.take_messages() {
                    transport.send(msg.to, msg);
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
                    &transport,
                    &mut members,
                );
                raw.advance_apply();

                // ⑤ **压缩触发**（设计 §4.4：日志条数 > N）。
                //
                // 为什么在这里：`compact_applied` 的纪律是"只允许应用线程调用"
                // （产物与 `applied` 必须同一瞬间取到），而这里正是应用线程、且刚 apply 完。
                // 为什么要触发：没有它，进程形态的日志**只增不减** —— `§42.4b` 的"稳定触发快照
                // 安装未拿到"根因就在这（当时 `compact_applied` 只被测试用的 `Cluster::compact`
                // 调过，进程/驱动层没有任何触发）。
                //
                // ⚠️ 压到 `applied` 意味着**可能压掉 follower 还需要的那段**，那就得靠快照补 ——
                // 实测那个组合会让**写停摆**（`§105.3` 有复现与已排除项；尚未根因），
                // 所以**别把 `--snapshot-log-entries` 调到很小**（默认 10 万不受影响）。
                let retained = storage
                    .last_index()
                    .unwrap_or(0)
                    .saturating_sub(storage.first_index().unwrap_or(1))
                    + 1;
                if opts.compact_log_entries > 0 && retained > opts.compact_log_entries as u64 {
                    storage
                        .compact_applied()
                        .unwrap_or_else(|e| fatal(id, "压缩日志", e));
                }
            }
            // 【视图快照】每秒一行，把**两个视图并排**打出来（`YUNTUN_META_TRACE=1`，默认关）。
            //
            // `§107.4` 要回答的问题正是"**leader 自己日志的末尾**"为何会在 `RaftLog` 视图与
            // `Storage` 视图之间不一致（一个说 10、一个说 9）。**必须同一瞬间并排**才有意义 ——
            // 跨两次 run 各看一个数得出的"矛盾"是假的（`§107.4` 的初版结论就犯了这个错）。
            {
                if trace_on()
                    && last_snap.is_none_or(|t: Instant| t.elapsed() >= Duration::from_secs(1))
                {
                    last_snap = Some(Instant::now());
                    // 打**整个 `Progress`**（Debug）：`matched/next_idx/state` 之外，`§107.6` 怀疑的
                    // 两处（`paused` 与 inflight `ins`）只有整打才看得见 —— 「leader 不再发」要么是
                    // `Probe+paused` 卡住，要么是 `ins` 满了（而 `matched` 不前进时 `ins.free_to`
                    // 不会被调用 ⇒ 那条"乐观发出"留下的 inflight 永远占着）。
                    let prs = raw
                        .raft
                        .prs()
                        .iter()
                        .map(|(p, pr)| format!("{p}:{pr:?}"))
                        .collect::<Vec<_>>()
                        .join(" ");
                    eprintln!(
                        "[meta:{id}] 视图 raft[first={} last={} committed={} applied={}] \
                         store[first={} last={} compact={} applied={}] peers {prs}",
                        raw.raft.raft_log.first_index(),
                        raw.raft.raft_log.last_index(),
                        raw.raft.raft_log.committed,
                        raw.raft.raft_log.applied,
                        storage.first_index().unwrap_or(1),
                        storage.last_index().unwrap_or(0),
                        storage.compacted_index(),
                        storage.applied_index(),
                    );
                }
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

/// 成员编号**对外展示**时统一排序（`§120`）。
///
/// 为什么必须在**展示层**做，而不是直接改 raft 的状态：`ConfState.voters` 的顺序取决于
/// 加入/提升的历史（实测：三次 voter + 一次提升之后是 `[2,4,1,3]`），而它**语义上是集合**
/// —— 顺序没有任何含义。原样透出去只会让每个消费方各自踩一次"看起来不一样但其实是同一批人"。
/// raft 自己的那份**原样保存**（`set_conf_state`），只在 `Members` / `Status` 这两个视图里排序。
fn sorted_ids(mut v: Vec<u64>) -> Vec<u64> {
    v.sort_unstable();
    v.dedup();
    v
}

/// **提升 learner → voter 的两道门槛**（`§120`），检查过了才提议那条 conf change。
///
/// 两道都对应"提升错人"的**真实**代价：
///
/// 1. **必须在册且是 learner**：不在册的 id 走这条路 = 静默把一个陌生节点加进来；
///    已经在 `voters` 里 = **幂等成功**（客户端超时重发是常见形态）；
/// 2. **必须追得够近**（`matched` 距日志末尾 ≤ [`PROMOTE_MAX_LAG`]）：把一个几乎没数据的
///    learner 提成 voter，等于把 quorum 交给它 —— 它一慢/一掉线，集群跟着停，而且
///    **没有任何报错**（现象只是"写不进去"）。etcd 的 `IsLearnerReady` 就是为这件事存在的。
fn promote_checked(
    raw: &mut RawNode<FjallStorage>,
    storage: &FjallStorage,
    members: &HashMap<u64, String>,
    id: u64,
) -> Result<(), MetaError> {
    let cs = storage.conf_state();
    if cs.voters.contains(&id) {
        return Ok(()); // 幂等：已经是 voter
    }
    if !cs.learners.contains(&id) {
        return Err(MetaError::BadRequest(format!(
            "节点 {id} 不在成员表里（voters={:?} learners={:?}）—— 先让它 Join 成为 learner 再提升",
            cs.voters, cs.learners
        )));
    }
    let last = raw.raft.raft_log.last_index();
    match raw
        .raft
        .prs()
        .get(id)
        .map(|p| last.saturating_sub(p.matched))
    {
        Some(lag) if lag > PROMOTE_MAX_LAG => {
            return Err(MetaError::BadRequest(format!(
                "learner {id} 落后太多（差 {lag} 条，上限 {PROMOTE_MAX_LAG}）—— 等它追平再提升；\
                 否则 quorum 会含进一个几乎没数据的节点，写会随它一起卡住"
            )));
        }
        // 本 leader 手里应当**总有**它的 `Progress`（它刚被 conf change 加进来）。没有 =
        // 我们对它的进度一无所知（典型是"刚当选、那条 conf change 还没应用"）——
        // 与"落后太多"是**同一类**风险（把 quorum 交给一个我们不了解的节点），一样拒绝。
        None => {
            return Err(MetaError::BadRequest(format!(
                "本 leader 没有 learner {id} 的复制进度，拒绝提升（等它的 conf change 应用后再试）"
            )));
        }
        Some(_) => {}
    }
    let cc = ConfChange {
        change_type: raft::eraftpb::ConfChangeType::AddNode,
        node_id: id,
        // 地址随条目复制（`§118`）：提升不改地址，但带上它能让**还没学到**的成员一并补上
        context: members
            .get(&id)
            .cloned()
            .unwrap_or_default()
            .into_bytes()
            .into(),
        ..Default::default()
    };
    raw.propose_conf_change(vec![], cc)
        .map_err(|e| MetaError::Storage(format!("conf change 提议失败：{e}")))
}

/// 参数多是**有意**的（与 `spawn_node` / `Cluster::spawn_with` 同款处理）：这里就是把本次要应用的
/// 那一批条目所需的全部运行期句柄显式交出去 —— 收进结构体反而会掩盖"谁共享了什么"。
/// `§118` 又多了两个（`transport` 用来登记新 peer、`members` 用来记地址表）。
#[allow(clippy::too_many_arguments)]
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
    transport: &Arc<dyn PeerTransport>,
    members: &mut HashMap<u64, String>,
) {
    for entry in entries {
        // **先按 raft 索引报告已应用位置**（含 no-op / ConfChange —— 它们也占索引，
        // 漏报会让压缩位置与日志错开一格）。刻意放在 `continue` 之前。
        storage
            .set_applied(entry.index)
            .unwrap_or_else(|e| fatal(id, "记录已应用位置", e));
        // 同步更新**对外报告**的已应用位置（`Status.applied_index` / `NodeHandle::applied`）。
        //
        // ⚠️ 这行是补的：原先只有"应用一条 op"那条分支才写它，于是 **no-op 与 conf change
        // 应用完之后，对外报的 `applied_index` 会停在原地**。后果不是理论上的 —— `§120` 的
        // 进程级用例拿它判"新节点追平了没有"，实测被它骗过：节点 4 的 `last_index=2`、
        // `commit_index=2`（**确实追平了**），而 `applied_index` 报 0 ⇒ 判据看走眼、白等 30s。
        // 按"每条条目都算已应用"记（与 `storage.set_applied` 同一个位置，
        // 因为它对空条目也记 —— 见上面的注释）。
        *applied.lock().unwrap() = entry.index;
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
            // **地址随 conf change 复制**（`§118`）：每个节点都从 `context` 里学到新成员的地址
            // ⇒ 换主之后新 leader 也知道怎么连它（否则只有"当初那个 leader"知道）。
            if let Ok(addr) = String::from_utf8(cc.context.to_vec())
                && !addr.is_empty()
            {
                members.insert(cc.node_id, addr.clone());
                // **落盘**（`§119`）：重启后 `applied` 之下的条目不会重放，光靠日志补不回地址表
                storage
                    .set_peer_addrs(members)
                    .unwrap_or_else(|e| fatal(id, "地址表落盘", e));
                if let Err(e) = transport.add_peer(cc.node_id, &addr) {
                    // 不致命（进程内传输就不支持），但必须**响亮**：加不进来 = 复制不到它
                    eprintln!("[meta:{id}] 加 peer {}（{addr}）失败：{e}", cc.node_id);
                }
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
                // 逐 op 的结果字节（形状由各 op 定；今天只有租约用它 —— `§81`）
                result: o.result.clone(),
                snapshot: st.current_snapshot(),
                affected: o.affected,
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
