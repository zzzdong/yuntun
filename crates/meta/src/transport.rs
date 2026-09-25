//! 节点间消息传输（S3-3 收尾）：把 raft 的**出站消息**送到对端。
//!
//! # 为什么必须有这一层
//!
//! raft 算法本身**不含传输** —— raft-rs 只吐 `eraftpb::Message`，库文档明确把"怎么送"留给使用者。
//! 之前两个节点靠进程内 `mpsc` 直传，所以"3 节点"只能在**同一个进程**里成立。
//! 换成这一层之后，`metanode` 才可能真正多节点部署。
//!
//! # 传输语义：**尽力而为** —— 不阻塞、不重试、不保序
//!
//! | 性质 | raft 是否要求 | 本实现 |
//! |---|---|---|
//! | 不丢 | **不要求**（⚠️ 但这条假设在本仓被证伪，见下） | 队列满 → **丢**（计数），绝不阻塞 raft 线程 |
//! | 保序 | **不要求**：靠 term/index 自愈乱序 | 不保证（多连接 + 并发） |
//! | 不重 | 要求：重复消息会让 raft 反复 `step` | gRPC 不重复；且**我们不做重发** |
//!
//! # ⚠️ 已知缺陷：这里"丢一条"的代价被低估了（`§106` 复现 / `§107` 定位，**未修**）
//!
//! 上面那句"raft 不要求不丢、丢了它自己会重发"**是假的**：raft-rs 在把消息**交给传输**时
//! 就**乐观推进** `Progress.next_idx`（`prepare_send_entries` → `update_state`），而它
//! **不会**因为"这条 append 没送达"而回退。实测（`§107`，同一瞬间的视图与逐条轨迹）：
//!
//! 1. leader 的 `next_idx` 已经在前面（它认为发出去了），follower 的 `matched` 停在原地
//!    —— 视图原样：`peers 3:m8/n10`，而 leader 自己 `last=9`；
//! 2. leader 没有更新的条目要发 ⇒ 它一遍遍发**空** append（`prev_index = next_idx - 1`）
//!    —— 逐条轨迹里 160 条全是 `index=9 entries=0`；
//! 3. follower 的日志里**正好有**那个 prev ⇒ 它**接受**并回 `index = 自己的 last`；
//! 4. `matched` 只能单调前进 ⇒ **leader 永远不会再发那条丢掉的条目** ⇒ 写入永久停摆。
//!
//! **本刀只把证据留在这里，没改行为**：三种改法都被实测否掉（无界队列 / 送达为止重试 /
//! 每次丢包就 `report_unreachable`），逐条见 `operation-log §107.5–§107.7`。
//! 已确认的方向是 raft 自己留的入口 —— `RawNode::report_unreachable`（raft 收到
//! `MsgUnreachable` 会把该 peer 从 Replicate 退回 Probe ⇒ `next_idx = matched + 1`）——
//! 但必须**限流**（每次丢包都报会把复制限流在 Probe 节奏上），并且要先有一个能证明
//! "它真能救回来"的用例。
//!
//! # 对端换地址（`§108`：容器 / pod 重启会换 IP）—— 这条**已修**
//!
//! 通道把地址 pin 在**建通道那一刻**解析出的结果上；对端换 IP（容器 / pod 重启、重新接入网络、
//! k8s 重建 pod）之后，leader 侧对它**一直超时**，而那个落后的节点因此永远收不到 append ⇒
//! **永久停摆**（`tests/cluster.sh smoke` 的收敛断言在容器里稳定复现）。
//! 所以 `send_loop` 连续失败 [`REBUILD_AFTER`] 次就**重建通道**（名字重新解析一遍）——
//! 地址一旦回到可达就自愈。这是"多节点真部署"必须处理的一格，`§104` 的进程内链路夹具测不到它。
//!
//! 结论：`send()` 必须**非阻塞**（调用方是 raft 线程，它还要 tick 别人的选举）。
//! 这就是把发送放进后台任务（每 peer 一个）而不是"在 raft 线程里 await"的原因。
//!
//! # 为什么用 gRPC（而不是另开一个裸 TCP 端口）
//!
//! | 方案 | 代价 |
//! |---|---|
//! | **gRPC（本实现）** | 每消息一次 HTTP/2 往返（心跳 3 tick ≈ 30ms，量级完全够）；换来**一个端口**、复用 TLS/鉴权/可观测，不必再写一套分帧 + 握手 |
//! | 裸 TCP + 长度前缀 | 少一层开销，但多一个端口、多一套协议/分帧/超时，TLS 还得再写一遍 |
//!
//! 若将来压测证明 HTTP/2 成了瓶颈（**大日志批量追赶**是唯一可能的场景），换成裸 TCP 只需替换本模块
//! —— `spawn_node` 只依赖 [`PeerTransport`] trait。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;

use protobuf::Message as PbMessage;
use raft::eraftpb::Message;

use yuntun_proto::meta as pb;

use crate::trace_on;

/// 每个 peer 的出站队列深度。满了就丢（raft 会重发）——
/// 这个值是"能吸收多少瞬时突发"的旋钮：追赶大日志时会打满，但**丢的是可重发的消息**。
///
/// ⚠️ `§107`：上面那句"丢的是可重发的消息"**不成立** —— 丢一条**携带条目的** append 会让
/// leader 永久停摆（见模块文档"已知缺陷"）。本刀**没有**动这个策略：无界队列与"送达为止重试"
/// 都试过，副作用（长期分区下内存无界 / 队头阻塞把新 append 也堵住）都被实测证实。
const SEND_QUEUE: usize = 1024;

/// 单次 `Meta.Raft` 调用的超时。对端卡住时不能把发送任务永久占住
/// （否则这个 peer 的消息会一直堆积 → 一直丢，等价于断链但**没有信号**）。
const RPC_TIMEOUT: Duration = Duration::from_secs(2);

/// 连续投递失败多少次就**重建**到该 peer 的通道（= 把它的名字重新解析一遍）。
///
/// 为什么需要（`§108`，**容器化集群里实测出来的**）：容器 / pod 重启、或重新接入网络会**换 IP**，
/// 而通道把地址 pin 在**建通道那一刻**解析出来的结果上。实测：把三节点里的一台
/// `podman network disconnect` 再 `connect`（IP 从 `10.89.0.2` 变成 `10.89.0.10`）之后，
/// leader 对它就**一直 `Timeout expired`**（12 次），而**同一时刻新起的**客户端连它完全正常 ——
/// 那个落后的节点因此永远收不到 append，**永久停摆**（在容器里稳定复现，`smoke` 的收敛断言会红）。
/// 重建通道会重新解析名字，对端地址一旦回到可达就自愈。
const REBUILD_AFTER: u32 = 3;

/// 造一个到 `addr` 的客户端（`connect_lazy`：真正建立连接是第一次发消息的时候）。
///
/// 单独抽出来是为了**重建**：`send_loop` 在对端连续失败后会调它拿一个**全新**的通道，
/// 新通道会把 `addr` 重新解析一遍（见 [`REBUILD_AFTER`]）。
fn make_client(addr: &str) -> pb::meta_client::MetaClient<tonic::transport::Channel> {
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .expect("地址在 GrpcTransport::new 里已经校验过，这里不会再失败")
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(RPC_TIMEOUT);
    pb::meta_client::MetaClient::new(endpoint.connect_lazy())
}

/// 连接超时：对端没起来时快速失败，交给下一 tick 重发。
const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);

/// 节点间消息传输。
///
/// 实现必须是**线程安全**的（raft 线程调用 `send`），且 `send` 非阻塞。
pub trait PeerTransport: Send + Sync + 'static {
    /// 把消息交给 `to`。**允许丢**（见模块文档）。
    fn send(&self, to: u64, msg: Message);
}

/// 传输计数（诊断用）。
///
/// 为什么值得单独做：**"永远选不出 leader"是最难查的故障**。
/// 有了这几个计数，"消息发出去了但对端拒收"（`rejected`）、"对端不可达"（`failed`）、
/// "队列打满丢包"（`dropped`）就能一眼分开 —— 否则只能靠猜。
#[derive(Debug, Default)]
pub struct TransportStats {
    /// 成功交给出站队列
    pub queued: AtomicU64,
    /// 队列满或对端未知 → 丢弃
    pub dropped: AtomicU64,
    /// RPC 失败（对端不可达/超时）
    pub failed: AtomicU64,
    /// 对端确认已交给它的 raft 线程
    pub delivered: AtomicU64,
    /// 对端拒收（未运行/不是成员）
    pub rejected: AtomicU64,
}

/// 计数快照（可打印、可断言）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TransportStatsView {
    pub queued: u64,
    pub dropped: u64,
    pub failed: u64,
    pub delivered: u64,
    pub rejected: u64,
}

impl TransportStats {
    pub fn view(&self) -> TransportStatsView {
        let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
        TransportStatsView {
            queued: g(&self.queued),
            dropped: g(&self.dropped),
            failed: g(&self.failed),
            delivered: g(&self.delivered),
            rejected: g(&self.rejected),
        }
    }
}

/// **进程内直投**：把消息塞进对端邮箱（测试簇 / 单机多节点同进程用）。
pub struct MpscTransport {
    mailboxes: HashMap<u64, Sender<Message>>,
}

impl MpscTransport {
    pub fn new(mailboxes: HashMap<u64, Sender<Message>>) -> Self {
        Self { mailboxes }
    }
}

impl PeerTransport for MpscTransport {
    fn send(&self, to: u64, msg: Message) {
        if let Some(tx) = self.mailboxes.get(&to) {
            let _ = tx.send(msg);
        }
    }
}

/// **什么都不做**：单节点组没有任何对端（raft 在单 voter 下不产生出站消息）。
///
/// 有这个类型而不是"用空 `MpscTransport`"，是为了让"单节点"这件事在代码里**显式**：
/// 单节点启动不需要 tokio 运行时上下文，多节点才需要（见 [`GrpcTransport::new`]）。
pub struct NoTransport;

impl PeerTransport for NoTransport {
    fn send(&self, _to: u64, _msg: Message) {}
}

/// 经 `Meta.Raft`（gRPC）把消息送给对端。
pub struct GrpcTransport {
    peers: HashMap<u64, tokio::sync::mpsc::Sender<Message>>,
    stats: Arc<TransportStats>,
}

impl GrpcTransport {
    /// 为每个 peer 起一个后台发送任务。
    ///
    /// ⚠️ **必须在 tokio 运行时的上下文里调用**（要 `Handle::current()` 起任务）。
    /// 地址非法 → 返回 `Err` 而不是"静默发不出去"：一个拼错的 `--peer` 若只表现为
    /// "集群永远选不出 leader"，排查成本极高。
    pub fn new(
        handle: &tokio::runtime::Handle,
        peers: HashMap<u64, String>,
        stats: Arc<TransportStats>,
    ) -> Result<Self, String> {
        let mut tx_map = HashMap::with_capacity(peers.len());
        for (id, addr) in peers {
            // 早失败：端点串非法就报错（`connect_lazy` 会推迟到首次发送，
            // 那时错误只能变成计数 —— 启动期报出来更有用）。
            // 真正的端点由 `send_loop` 自己造（它要能**重建**，见那里的注释）。
            tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                .map_err(|e| format!("节点 {id} 的地址 {addr:?} 非法（期望 host:port）：{e}"))?;
            let (tx, rx) = tokio::sync::mpsc::channel::<Message>(SEND_QUEUE);
            handle.spawn(send_loop(id, addr, rx, stats.clone()));
            tx_map.insert(id, tx);
        }
        Ok(Self {
            peers: tx_map,
            stats,
        })
    }

    pub fn stats(&self) -> Arc<TransportStats> {
        self.stats.clone()
    }
}

impl PeerTransport for GrpcTransport {
    fn send(&self, to: u64, msg: Message) {
        // 轨迹（`YUNTUN_META_TRACE=1`）：**raft 刚交给传输的那一刻**（`§107`）。
        // 与 `send_loop` 里出队时的 `[meta:transport] →` 配对，就能把「raft 根本没生成」与
        // 「生成了但没发出去」分开 —— `§107` 正是靠这一对配出"raft 已交出 161 条、出队停在 99 条"的。
        if trace_on() {
            eprintln!(
                "[meta:send] →{to} {:?} index={} entries={} commit={}",
                msg.get_msg_type(),
                msg.index,
                msg.entries.len(),
                msg.commit
            );
        }
        let Some(tx) = self.peers.get(&to) else {
            // 目标不是本节点认识的 peer：丢 + 计数（不是 panic —— 成员表变化时会短暂出现）
            self.stats.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        // `try_send`：满就丢。**绝不阻塞 raft 线程**（它还要 tick）。
        // ⚠️ `§107`：这里"丢"是**有代价**的（见模块文档"已知缺陷"），但改成无界队列被实测否掉了
        // （长期分区下积压无界），所以保持原样、把问题记在文档里。
        if tx.try_send(msg).is_err() {
            self.stats.dropped.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.queued.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// 每个 peer 一个发送任务：出队 → 编码 → `Meta.Raft`。
///
/// 用 `connect_lazy` + 端点超时：对端没起来/重启中**不需要**我们写重连逻辑
/// （tonic 自己会重连），这段时间丢的消息由 raft 的下一 tick 补上。
async fn send_loop(
    to: u64,
    addr: String,
    mut rx: tokio::sync::mpsc::Receiver<Message>,
    stats: Arc<TransportStats>,
) {
    let mut client = make_client(&addr);
    // 连续投递失败计数：到 `REBUILD_AFTER` 就**重建通道**（见下面 `Err` 分支）。
    let mut fail_streak: u32 = 0;
    while let Some(msg) = rx.recv().await {
        let payload = match msg.write_to_bytes() {
            Ok(b) => b,
            Err(e) => {
                // 编不出来 = 本进程的 raft 消息有 encode 错误（不是网络问题）→ 计数并继续
                stats.failed.fetch_add(1, Ordering::Relaxed);
                eprintln!("[meta:transport] 发往 {to}({addr}) 的消息编码失败：{e}");
                continue;
            }
        };
        let req = pb::RaftRequest {
            from: msg.from,
            message: payload,
        };
        // 逐条出站轨迹（`YUNTUN_META_TRACE=1`）：与 `service.rs` 的入站轨迹配对，
        // 用来回答"**发出去的东西到底有没有到对端**" —— 这是 `§106` 那个写停摆的卡点。
        if trace_on() {
            eprintln!(
                "[meta:transport] →{to} {:?} from={} index={} log_term={} entries={} commit={} \
                 reject={} hint={}",
                msg.get_msg_type(),
                msg.from,
                msg.index,
                msg.log_term,
                msg.entries.len(),
                msg.commit,
                msg.reject,
                msg.reject_hint
            );
        }
        match client.raft(req).await {
            Ok(resp) => {
                let r = resp.into_inner();
                fail_streak = 0;
                if r.delivered {
                    stats.delivered.fetch_add(1, Ordering::Relaxed);
                } else {
                    stats.rejected.fetch_add(1, Ordering::Relaxed);
                    eprintln!("[meta:transport] {to}({addr}) 拒收：{}", r.reason);
                }
            }
            Err(e) => {
                // 对端不可达/重启中/超时：只计数（raft 会重发）。
                // ⚠️ `§107`：这句"raft 会重发"**不成立** —— 丢掉一条**携带条目的** append 会让
                // leader 永久停摆（模块文档"已知缺陷"）。
                stats.failed.fetch_add(1, Ordering::Relaxed);
                fail_streak += 1;
                // 连续失败 ⇒ **重建通道**（`§108`）：对端可能换了 IP（容器/pod 重启、重新接入网络），
                // 而通道里 pin 的是旧地址 —— 不重建就永远连不上它（实测：leader 一直 `Timeout expired`，
                // 而新客户端连得上）。这里**无条件打一行**：这是个该被运维看见的事件，不是噪声。
                if fail_streak >= REBUILD_AFTER {
                    client = make_client(&addr);
                    fail_streak = 0;
                    eprintln!(
                        "[meta:transport] →{to}({addr}) 连续 {REBUILD_AFTER} 次投递失败：\
                         已重建通道（重新解析对端地址；最近一次错误：{e}）"
                    );
                } else if trace_on() {
                    eprintln!("[meta:transport] →{to} 投递失败（本条丢弃）：{e}");
                }
            }
        }
    }
}
