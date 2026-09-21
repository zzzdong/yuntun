//! **数据面 gRPC 适配**：热数据分片拉取的服务端 + 客户端（S5-6；R4 T12.1 的前置）。
//!
//! ## 这个 crate 解决什么
//!
//! `yuntun-store` 的 [`ShardReader`] / [`ShardFetch`] 是**纯接口**（缝早就留好了），但在此之前
//! 只有"进程内实现"是真的：`RemoteShard` 的传输在测试里是个假实现。本 crate 把它变成**能跑在
//! 网络上的东西** —— 于是"查询节点读另一个 datanode 的未落盘数据"第一次可验证。
//!
//! ## 为什么单独一个 crate（而不是塞进 `store`/`chunk`）
//!
//! `yuntun-proto` 的定位是"只放接口，**不把 tonic 拖进查询/写入路径**"。而 `store` 正被查询路径
//! 依赖、`chunk` 正被写入路径依赖 —— 放进去就违背了那条边界。单开一个 crate 后：
//! 只有**装配层**（数据节点进程 / 查询节点进程）才引入 tonic。
//!
//! ## 契约的两处要点
//!
//! 1. **水位随 pull 响应回来**（`architecture §4.4`）：响应必须带 `flushed_watermark` 与
//!    `stale`，否则协调者会在「实例已放弃副本、自己 manifest 还没追上」的窗口里**静默少数据**
//!    （`operation-log §63.3`）。服务端**不重算**它们 —— 它只转发本地 [`ShardReader`] 的判断
//!    （谁能比自己更清楚"我放弃到哪了"）。
//! 2. **批次编码复用 arrow IPC stream**（与 WAL 记录 / spill 同一形态）：系统里只该有一种
//!    批次线上编码，多一套就多一处会漂移的地方。

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use tokio::net::TcpListener;
use tonic::{Request, Response, Status};

use yuntun_model::error::LakeError;
use yuntun_proto::shard as pb;
use yuntun_store::{ShardFetch, ShardId, ShardRead, ShardReader};

// ---------------------------------------------------------------- 批次编解码

/// 单批 → arrow IPC stream 字节（**与 WAL 记录 / spill 同一种编码**）。
pub fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>, LakeError> {
    let mut buf = Vec::new();
    let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
        .map_err(|e| LakeError::Other(format!("shard rpc: ipc writer: {e}")))?;
    w.write(batch)
        .map_err(|e| LakeError::Other(format!("shard rpc: ipc write: {e}")))?;
    w.finish()
        .map_err(|e| LakeError::Other(format!("shard rpc: ipc finish: {e}")))?;
    Ok(buf)
}

/// arrow IPC stream 字节 → 单批。
pub fn decode_batch(bytes: &[u8]) -> Result<RecordBatch, LakeError> {
    let mut reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| LakeError::Other(format!("shard rpc: ipc reader: {e}")))?;
    match reader.next() {
        Some(Ok(b)) => Ok(b),
        Some(Err(e)) => Err(LakeError::Other(format!("shard rpc: ipc decode: {e}"))),
        None => Err(LakeError::Other("shard rpc: empty ipc payload".into())),
    }
}

fn to_msg(id: &ShardId) -> pb::ShardIdMsg {
    pb::ShardIdMsg {
        table: id.table.clone(),
        shard: id.shard.clone(),
        window: id.window.clone(),
    }
}

fn from_msg(m: &pb::ShardIdMsg) -> ShardId {
    ShardId::new(m.table.clone(), m.shard.clone(), m.window.clone())
}

fn internal(e: LakeError) -> Status {
    Status::internal(e.to_string())
}

// ---------------------------------------------------------------- 服务端（数据节点侧）

/// 数据节点侧的服务：把**本地** [`ShardReader`]（进程内 chunk store）暴露成 gRPC。
pub struct ShardService {
    reader: Arc<dyn ShardReader>,
}

impl ShardService {
    pub fn new(reader: Arc<dyn ShardReader>) -> Self {
        Self { reader }
    }
}

#[tonic::async_trait]
impl pb::shard_fetch_server::ShardFetch for ShardService {
    async fn fetch_shards(
        &self,
        req: Request<pb::FetchShardsRequest>,
    ) -> Result<Response<pb::FetchShardsResponse>, Status> {
        let table = req.into_inner().table;
        let shards = self.reader.shards_of(&table).await.map_err(internal)?;
        Ok(Response::new(pb::FetchShardsResponse {
            shards: shards.iter().map(to_msg).collect(),
        }))
    }

    async fn fetch_watermark(
        &self,
        req: Request<pb::FetchWatermarkRequest>,
    ) -> Result<Response<pb::FetchWatermarkResponse>, Status> {
        let known = req.into_inner().known_manifest_ver;
        // 与 `fetch_shard` 同理：水位**由本地 reader 判定后原样转发**，服务端不重算
        let wm = self.reader.watermark(known).await.map_err(internal)?;
        Ok(Response::new(pb::FetchWatermarkResponse {
            flushed_watermark: wm.flushed_watermark,
            stale: wm.stale,
        }))
    }

    async fn fetch_shard(
        &self,
        req: Request<pb::FetchShardRequest>,
    ) -> Result<Response<pb::FetchShardResponse>, Status> {
        let req = req.into_inner();
        let id = from_msg(
            req.shard
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("shard 缺失"))?,
        );
        // 水位 / STALE **由本地 reader 判定后原样转发**，服务端不重算：
        // 只有持有副本的那一方知道"我放弃到哪了"（`§63.2`）。
        let read = self
            .reader
            .read_shard(&id, req.known_manifest_ver)
            .await
            .map_err(internal)?;

        let mut batches_ipc = Vec::with_capacity(read.batches.len());
        for b in &read.batches {
            batches_ipc.push(encode_batch(b).map_err(internal)?);
        }
        Ok(Response::new(pb::FetchShardResponse {
            batches_ipc,
            flushed_watermark: read.flushed_watermark,
            stale: read.stale,
        }))
    }
}

/// 起服务。**接收已 bind 的监听器**（与 `yuntun_meta::serve` 同形）：调用方先拿到端口再移交，
/// 避免"先探测端口、再起服务"之间被别人抢走的 TOCTOU。
pub async fn serve(
    reader: Arc<dyn ShardReader>,
    listener: TcpListener,
) -> Result<(), tonic::transport::Error> {
    // 把 accept 循环包成 Stream（`serve_with_incoming` 需要）；用 `futures::stream::unfold`
    // 而不是引入 tokio-stream —— 与 `yuntun_meta::serve` 同一手法，少一个依赖。
    let incoming = futures::stream::unfold(listener, |l| async move {
        match l.accept().await {
            Ok((sock, _addr)) => Some((Ok::<_, std::io::Error>(sock), l)),
            Err(e) => Some((Err(e), l)),
        }
    });
    tonic::transport::Server::builder()
        .add_service(pb::shard_fetch_server::ShardFetchServer::new(ShardService::new(
            reader,
        )))
        .serve_with_incoming(incoming)
        .await
}

// ---------------------------------------------------------------- 客户端（查询节点侧）

/// 查询节点侧：把远端数据节点的 gRPC 服务适配成 [`ShardFetch`]（→ 再包成 `RemoteShard`）。
pub struct GrpcShardFetch {
    client: pb::shard_fetch_client::ShardFetchClient<tonic::transport::Channel>,
}

impl std::fmt::Debug for GrpcShardFetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcShardFetch").finish()
    }
}

impl GrpcShardFetch {
    /// 连一个数据节点（`addr` 形如 `127.0.0.1:50051`，不带 scheme）。
    pub async fn connect(addr: &str) -> Result<Self, LakeError> {
        let ep = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|e| LakeError::Other(format!("shard rpc: bad addr {addr}: {e}")))?;
        let channel = ep
            .connect()
            .await
            .map_err(|e| LakeError::Other(format!("shard rpc: connect {addr}: {e}")))?;
        Ok(Self {
            client: pb::shard_fetch_client::ShardFetchClient::new(channel),
        })
    }
}

impl ShardFetch for GrpcShardFetch {
    fn fetch_shards<'a>(
        &'a self,
        table: &'a str,
    ) -> BoxFuture<'a, Result<Vec<ShardId>, LakeError>> {
        Box::pin(async move {
            // tonic 的 client 克隆很便宜（内部是 channel 句柄），所以每次调用 clone 一份
            let mut c = self.client.clone();
            let resp = c
                .fetch_shards(pb::FetchShardsRequest {
                    table: table.to_string(),
                })
                .await
                .map_err(|s| LakeError::Other(format!("shard rpc: fetch_shards: {s}")))?;
            Ok(resp.into_inner().shards.iter().map(from_msg).collect())
        })
    }

    fn fetch_watermark<'a>(
        &'a self,
        known_manifest_ver: u64,
    ) -> BoxFuture<'a, Result<ShardRead, LakeError>> {
        Box::pin(async move {
            let mut c = self.client.clone();
            let resp = c
                .fetch_watermark(pb::FetchWatermarkRequest { known_manifest_ver })
                .await
                .map_err(|s| LakeError::Other(format!("shard rpc: fetch_watermark: {s}")))?;
            let resp = resp.into_inner();
            Ok(ShardRead {
                batches: Vec::new(),
                flushed_watermark: resp.flushed_watermark,
                stale: resp.stale,
            })
        })
    }

    fn fetch_shard<'a>(
        &'a self,
        id: &'a ShardId,
        known_manifest_ver: u64,
    ) -> BoxFuture<'a, Result<ShardRead, LakeError>> {
        Box::pin(async move {
            let mut c = self.client.clone();
            let resp = c
                .fetch_shard(pb::FetchShardRequest {
                    shard: Some(to_msg(id)),
                    known_manifest_ver,
                })
                .await
                .map_err(|s| LakeError::Other(format!("shard rpc: fetch_shard: {s}")))?;
            let resp = resp.into_inner();

            let mut batches = Vec::with_capacity(resp.batches_ipc.len());
            for b in &resp.batches_ipc {
                batches.push(decode_batch(b)?);
            }
            // 水位与 stale **原样带回去** —— 这是契约的一部分，不是可选的装饰
            Ok(ShardRead {
                batches,
                flushed_watermark: resp.flushed_watermark,
                stale: resp.stale,
            })
        })
    }
}
