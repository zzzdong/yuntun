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

use crate::{MetaError, NodeHandle};
use yuntun_proto::meta as pb;

/// 等一个提案被 raft 应用的上限。
///
/// 超时返回 `UNAVAILABLE`（**可重试**）而不是让客户端裸等：客户端换节点重试比挂死好。
const PROPOSE_TIMEOUT: Duration = Duration::from_secs(10);

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
        Ok(Response::new(self.node.delta(req.into_inner().since_manifest_ver)))
    }

    async fn status(
        &self,
        _req: Request<pb::StatusRequest>,
    ) -> Result<Response<pb::StatusResponse>, Status> {
        Ok(Response::new(self.node.status()))
    }

    async fn join(
        &self,
        _req: Request<pb::JoinRequest>,
    ) -> Result<Response<pb::JoinResponse>, Status> {
        Err(Status::unimplemented(
            "成员变更（learner → voter）属 S3-6；现在加节点必须改初始成员表并全量重启",
        ))
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
