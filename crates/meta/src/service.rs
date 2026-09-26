//! metanode 的 **gRPC 服务层**（S3-3）：把 proto 面接到节点句柄上。
//!
//! # 这一层只做三件事
//!
//! 1. **拆参数**（proto → 句柄调用）；
//! 2. **错误映射**（[`MetaError`] → `tonic::Status`，按 `metanode-design §5` 约定 3 ——
//!    非 leader 与 OCC 冲突必须**可重试**，详见 `crate::error`）；
//! 3. **线程模型**：raft 的"等应用到状态机"是**阻塞**的，必须丢进阻塞线程池
//!    （见 [`MetaService::propose`] 的注释 —— 这一条写错会把整个服务拖停）。
//!
//! 业务语义**不在这里**：op 的解码/应用在 `crate::op`，一致性在 raft，状态在 `CatalogState`。
//! 服务层越薄，"RPC 面"与"状态机"就越不会各自漂移。

use std::time::Duration;

use tonic::{Request, Response, Status};

use raft::eraftpb::Message;

use crate::{trace_on, MetaError, NodeHandle};
use yuntun_proto::meta as pb;

/// 等一个提案被 raft 应用的上限。
///
/// 超时返回 `UNAVAILABLE`（**可重试**）而不是让客户端裸等：客户端换节点重试比挂死好。
const PROPOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// `Join` 等"成员变更真的生效"的上限（`§119`）：提议 + 提交 + 应用（同一机房通常是毫秒级）。
const JOIN_TIMEOUT: Duration = Duration::from_secs(10);

/// gRPC 服务实现。
pub struct MetaService {
    node: NodeHandle,
}

impl MetaService {
    pub fn new(node: NodeHandle) -> Self {
        Self { node }
    }
}

#[tonic::async_trait]
impl pb::meta_server::Meta for MetaService {
    async fn propose(
        &self,
        req: Request<pb::ProposeRequest>,
    ) -> Result<Response<pb::ProposeResponse>, Status> {
        let r = req.into_inner();
        let op = r
            .op
            .ok_or_else(|| Status::invalid_argument("op 为空（ProposeRequest.op 必填）"))?;

        // ⚠️ raft 的"等应用到状态机"是**阻塞**的（`recv_timeout`）。直接在 async 函数里等，
        // 会占住 tokio 的工作线程 —— 一个被拖住的提案就能把整个服务拖停。
        // 所以必须丢进阻塞线程池；这也让"提案慢"不会传染给 Status/Prefetch 这类快读。
        let node = self.node.clone();
        let resp = tokio::task::spawn_blocking(move || node.propose(op, PROPOSE_TIMEOUT))
            .await
            .map_err(|e| Status::internal(format!("阻塞任务失败：{e}")))??;
        Ok(Response::new(resp))
    }

    async fn prefetch(
        &self,
        req: Request<pb::PrefetchRequest>,
    ) -> Result<Response<pb::PrefetchResponse>, Status> {
        // 只读内存状态（O(表数)，且载荷只含被请求的表）—— 和 Delta/Status 一样不需要
        // 阻塞线程池。**注意**：这里不查盘、不 raft、不等待，所以"读旧窗口"的口径
        // 由客户端的 `cache_ttl` 决定（设计 §3.2），不在本层。
        Ok(Response::new(self.node.prefetch(&req.into_inner())))
    }

    async fn delta(
        &self,
        req: Request<pb::DeltaRequest>,
    ) -> Result<Response<pb::DeltaResponse>, Status> {
        // 只查内存里的版本表（O(表数)），非常快，不需要阻塞线程池
        let r = req.into_inner();
        Ok(Response::new(
            self.node.delta(r.since_manifest_ver, r.since_schema_ver),
        ))
    }

    async fn status(
        &self,
        _req: Request<pb::StatusRequest>,
    ) -> Result<Response<pb::StatusResponse>, Status> {
        Ok(Response::new(self.node.status()))
    }

    async fn heartbeat(
        &self,
        req: Request<pb::HeartbeatRequest>,
    ) -> Result<Response<pb::HeartbeatResponse>, Status> {
        // 只碰内存（`§3.2`）：心跳走 raft 会把写路径压垮
        let known = self.node.heartbeat(&req.into_inner().instance_id);
        Ok(Response::new(pb::HeartbeatResponse { known }))
    }

    /// 成员变更（`§119`）：把一个节点作为 **learner** 加进集群，并把**起步配置**回给它。
    ///
    /// # 两半合起来才是"在线加节点"
    ///
    /// * **集群不用预先知道新节点**：它的地址由本请求带进来，随 conf change 复制给每个成员
    ///   （`§118` 用 `ConfChange.context` 带地址）；
    /// * **新节点不用预先知道所有地址**：本回包把成员表与各自地址给它（**含 leader 自己**）——
    ///   它据此起自己的 raft，并知道该把 `AppendResponse` 发回哪。
    ///
    /// 于是它替掉了"改 `--voters`/`--peer` 再全量重启"那套；`cli` 的启动闸门也据此开了
    /// `--join` 这个正门（`§119`）。
    ///
    /// # 幂等
    ///
    /// 已经是成员（voter 或 learner）就**不再提议**、直接回当前配置 —— 客户端超时重发不该被罚。
    ///
    /// # 只 leader 受理
    ///
    /// 非 leader 回 `NotLeader` + hint（与 `Propose` 同一约定）。⚠️ hint 是个 **id**，
    /// 而调用方（新节点）手里只有地址 —— 所以 `--join` 支持逗号分隔的多个接触点，
    /// 逐个试到 leader 为止（与 `bench --meta`、`RemoteCatalog` 同一套做法）。
    ///
    /// # 为什么等生效才回包
    ///
    /// 回包里给的是**新节点的起步配置**：如果 conf change 还没提交就回，新节点会拿一份
    /// "集群不认"的配置去起步（它不在任何人的成员表里，没人会给它发日志）。等它进成员表
    /// 才算"接纳完成"。
    async fn join(
        &self,
        req: Request<pb::JoinRequest>,
    ) -> Result<Response<pb::JoinResponse>, Status> {
        let r = req.into_inner();
        if r.node_id == 0 || r.address.is_empty() {
            return Err(Status::invalid_argument(
                "node_id 与 address 都必填；address 要填**别的节点能连到它**的地址\
                 （不是 --listen 的 0.0.0.0，见 --advertise）",
            ));
        }
        let st = self.node.status();
        if st.role != "Leader" {
            return Err(MetaError::NotLeader {
                leader_hint: st.leader_id,
            }
            .into());
        }

        // ⚠️ `add_learner` / `members` 都是**阻塞**的（内部 `recv_timeout` 等 raft 线程回话）。
        // 直接在 async 函数里调会占住 tokio 工作线程 —— 与 `propose` 同一条纪律，走阻塞池。
        let blocking = |node: NodeHandle, what: &'static str| async move {
            let r = tokio::task::spawn_blocking(move || node.members(Duration::from_secs(2)))
                .await
                .map_err(|e| Status::internal(format!("阻塞任务失败（{what}）：{e}")))?;
            r.map_err(Status::from)
        };

        let mv = blocking(self.node.clone(), "members").await?;
        let already = mv.voters.contains(&r.node_id) || mv.learners.contains(&r.node_id);
        if !already {
            let node = self.node.clone();
            let (id, addr) = (r.node_id, r.address.clone());
            tokio::task::spawn_blocking(move || node.add_learner(id, &addr, PROPOSE_TIMEOUT))
                .await
                .map_err(|e| Status::internal(format!("阻塞任务失败（add_learner）：{e}")))??;
        }

        let deadline = tokio::time::Instant::now() + JOIN_TIMEOUT;
        loop {
            let mv = blocking(self.node.clone(), "members").await?;
            if mv.voters.contains(&r.node_id) || mv.learners.contains(&r.node_id) {
                return Ok(Response::new(pb::JoinResponse {
                    voters: mv.voters.len() as u64,
                    learners: mv.learners.len() as u64,
                    voter_ids: mv.voters.clone(),
                    learner_ids: mv.learners.clone(),
                    members: mv
                        .addrs
                        .iter()
                        .map(|(id, a)| pb::NodeAddr {
                            node_id: *id,
                            address: a.clone(),
                        })
                        .collect(),
                }));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Status::deadline_exceeded(format!(
                    "成员变更未在 {JOIN_TIMEOUT:?} 内生效（raft 选不出 leader？看 Status）"
                )));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// 节点间：一条 raft 消息（`Meta.Raft`）。**不是客户端接口**。
    ///
    /// 这一层刻意只做"解码 + 入箱"，**不做**：去重、排序、鉴权、地址校验 ——
    /// 前两者由 raft 自己保证/容忍（见 `transport` 模块文档），后两者是部署层的事
    /// （真实环境要 mTLS，登记在 operation-log §50.6）。
    async fn raft(
        &self,
        req: Request<pb::RaftRequest>,
    ) -> Result<Response<pb::RaftResponse>, Status> {
        let r = req.into_inner();
        // 解不开 = 版本不匹配或线上被截断。**必须报错**（不回 delivered=false 的"软失败"）：
        // 软失败会让对端以为"对端收到了但拒收"，真相却是**协议不一致**，两者的处置完全不同。
        let msg = <Message as protobuf::Message>::parse_from_bytes(&r.message).map_err(|e| {
            Status::invalid_argument(format!(
                "raft 消息解不开（节点 {} 发来 {} 字节）：{e}",
                r.from,
                r.message.len()
            ))
        })?;
        // 逐条入站轨迹（`YUNTUN_META_TRACE=1`）：与 `transport.rs` 的出站轨迹配对。
        // `delivered=false` 表示"本节点不是成员/已停" —— 与"根本没到"是两回事，
        // 这两者在现象上都是"集群不动"，所以必须分开记（`§106`）。
        if trace_on() {
            eprintln!(
                "[meta:service] ←id={} from={} {:?} index={} log_term={} entries={}",
                self.node.id,
                msg.from,
                msg.get_msg_type(),
                msg.index,
                msg.log_term,
                msg.entries.len()
            );
        }
        let delivered = self.node.deliver(msg);
        Ok(Response::new(pb::RaftResponse {
            delivered,
            reason: if delivered {
                String::new()
            } else {
                "本节点的 raft 线程已退出（未运行）".into()
            },
        }))
    }
}

/// 在已绑定的监听器上起服务（端口由调用方决定 —— 测试可用 `:0` 让内核分配）。
///
/// 用 `serve_with_incoming` 而不是 `serve(addr)`：后者会**自己**去 bind，
/// 于是"先探测空闲端口再交给它"存在 TOCTOU 竞态（两个测试可能选中同一端口）。
pub async fn serve(
    node: NodeHandle,
    listener: tokio::net::TcpListener,
) -> Result<(), tonic::transport::Error> {
    // 把 accept 循环包成 Stream（tonic 的 `serve_with_incoming` 需要它）。
    // 用 `futures::stream::unfold` 而不是额外引入 tokio-stream：少一个依赖，行为一样。
    let incoming = futures::stream::unfold(listener, |l| async move {
        match l.accept().await {
            Ok((sock, _addr)) => Some((Ok::<_, std::io::Error>(sock), l)),
            Err(e) => Some((Err(e), l)),
        }
    });
    tonic::transport::Server::builder()
        .add_service(pb::meta_server::MetaServer::new(MetaService::new(node)))
        .serve_with_incoming(incoming)
        .await
}

/// 便捷：把错误转成 gRPC 状态（服务层各处统一走它，避免漏用 `From`）。
pub fn to_status(e: MetaError) -> Status {
    e.into()
}
