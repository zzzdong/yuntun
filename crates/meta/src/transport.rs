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
//! | 不丢 | **不要求**：心跳/选举/追加按 tick 重发，丢失只是慢一点 | 队列满 → **丢**（计数），绝不阻塞 raft 线程 |
//! | 保序 | **不要求**：靠 term/index 自愈乱序 | 不保证（多连接 + 并发） |
//! | 不重 | 要求：重复消息会让 raft 反复 `step` | gRPC 不重复；且**我们不做重发** |
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

/// 每个 peer 的出站队列深度。满了就丢（raft 会重发）——
/// 这个值是"能吸收多少瞬时突发"的旋钮：追赶大日志时会打满，但**丢的是可重发的消息**。
const SEND_QUEUE: usize = 1024;

/// 单次 `Meta.Raft` 调用的超时。对端卡住时不能把发送任务永久占住
/// （否则这个 peer 的消息会一直堆积 → 一直丢，等价于断链但**没有信号**）。
const RPC_TIMEOUT: Duration = Duration::from_secs(2);

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
            // 那时错误只能变成计数 —— 启动期报出来更有用）
            let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                .map_err(|e| format!("节点 {id} 的地址 {addr:?} 非法（期望 host:port）：{e}"))?;
            let endpoint = endpoint
                .connect_timeout(CONNECT_TIMEOUT)
                .timeout(RPC_TIMEOUT);
            let (tx, rx) = tokio::sync::mpsc::channel::<Message>(SEND_QUEUE);
            handle.spawn(send_loop(id, addr, endpoint, rx, stats.clone()));
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
        let Some(tx) = self.peers.get(&to) else {
            // 目标不是本节点认识的 peer：丢 + 计数（不是 panic —— 成员表变化时会短暂出现）
            self.stats.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        // `try_send`：满就丢。**绝不阻塞 raft 线程**（它还要 tick）。
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
    endpoint: tonic::transport::Endpoint,
    mut rx: tokio::sync::mpsc::Receiver<Message>,
    stats: Arc<TransportStats>,
) {
    let mut client = pb::meta_client::MetaClient::new(endpoint.connect_lazy());
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
        match client.raft(req).await {
            Ok(resp) => {
                let r = resp.into_inner();
                if r.delivered {
                    stats.delivered.fetch_add(1, Ordering::Relaxed);
                } else {
                    stats.rejected.fetch_add(1, Ordering::Relaxed);
                    eprintln!("[meta:transport] {to}({addr}) 拒收：{}", r.reason);
                }
            }
            Err(_) => {
                // 对端不可达/重启中/超时：**不是错误路径**（raft 会重发），只计数。
                // 不打日志：心跳 30ms 一次，对端挂掉会刷屏（要看就看计数）。
                stats.failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}
