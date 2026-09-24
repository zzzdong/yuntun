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
//!
//! ## 超时：`§4.3` 的另一半（`operation-log §78`）
//!
//! `architecture-with-chunk §4.3` 写的是"**失败 / 超时** ⇒ 协调者退化为只读冷数据，标记 partial"。
//! 这两件事**不是一回事**：
//!
//! | | 谁来发现 | 症状 |
//! |---|---|---|
//! | **失败**（连接拒绝 / 服务端错误） | 传输层**立刻**返回 `Err` | 早就被当成失败处理 |
//! | **超时**（无响应） | **只有调用方能发现** —— 对方什么都没说 | 查询**一直等**下去 |
//!
//! "无响应"的现实形态：进程还在但被 CPU 抢光 / 长 GC 停顿 / 网络黑洞 / 假死。
//! 客户端若不设上限，一个这样的节点就能把查询挂住 —— 而 `§77` 的降级路径**只对 `Err` 生效**，
//! 所以必须由**传输层**把"等够了"变成 `Err`（只有它知道多久算等够）。

use std::sync::Arc;
use std::time::Duration;

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
        //
        // 【`§28.1` 读侧栅栏】`known_batch_ids` = 调用方已能读到的批次，**原样喂给本地 reader**
        // 做过滤：服务端不自己查 catalog —— "哪些数据已进调用方的 manifest"只有调用方知道，
        // 而且服务端查会在每次分片拉取上多一跳（`RemoteCatalog::list_visible_files` 会 refresh）。
        let read = self
            .reader
            .read_shard_excluding(&id, req.known_manifest_ver, &req.known_batch_ids)
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

/// 数据面 RPC 的**默认超时**。
///
/// 5 秒是权衡：热数据一次拉取通常远快于此（同机房 RTT 亚毫秒级），而查询端宁可降级为
/// **带标记的部分结果**，也不该被一个假死节点拖住。
///
/// 注意粒度是"**每个 RPC**"：`RemoteShard::read_table` 的默认实现会走三个 RPC
/// （枚举分片 → 读分片 → 取水位），所以**单个来源**最坏约 `3 × timeout`。
/// 要收窄就得覆写 `read_table`（trait 允许 —— 合并成一次 RPC，见 `§67`）。
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// 查询节点侧：把远端数据节点的 gRPC 服务适配成 [`ShardFetch`]（→ 再包成 `RemoteShard`）。
pub struct GrpcShardFetch {
    client: pb::shard_fetch_client::ShardFetchClient<tonic::transport::Channel>,
    /// 每次 RPC 的上限（**含响应体**：热数据是随响应一起回来的）
    timeout: Duration,
}

impl std::fmt::Debug for GrpcShardFetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcShardFetch").finish()
    }
}

impl GrpcShardFetch {
    /// 连一个数据节点（`addr` 形如 `127.0.0.1:50051`，不带 scheme），用 [`DEFAULT_TIMEOUT`]。
    pub async fn connect(addr: &str) -> Result<Self, LakeError> {
        Self::connect_with_timeout(addr, DEFAULT_TIMEOUT).await
    }

    /// 同 [`Self::connect`]，但指定**每次 RPC 的超时**。
    ///
    /// **建连也受它约束**（`connect_timeout`）：否则"节点不存在 / 端口是黑洞"时，
    /// 查询侧会挂在**建连**这一步 —— 那也是"无响应"，不能只护住已建好的连接。
    pub async fn connect_with_timeout(addr: &str, timeout: Duration) -> Result<Self, LakeError> {
        let ep = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|e| LakeError::Other(format!("shard rpc: bad addr {addr}: {e}")))?
            .connect_timeout(timeout);
        let channel = ep
            .connect()
            .await
            .map_err(|e| LakeError::Other(format!("shard rpc: connect {addr}: {e}")))?;
        Ok(Self {
            client: pb::shard_fetch_client::ShardFetchClient::new(channel),
            timeout,
        })
    }

    /// 当前超时（诊断用）。
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// 统一的"带超时的 RPC"包装。
    ///
    /// 超时**必须与其它失败可区分**：`§77` 的降级会把原因带进"缺失来源"里给排障的人看，
    /// 若超时只是又一个 `deadline exceeded` 字符串，看的人分不清"节点拒绝了连接"（进程没了）
    /// 与"节点没响应"（进程还在但卡住了）—— 这两者的处置完全不同。
    async fn call<T>(
        &self,
        what: &str,
        fut: impl std::future::Future<Output = Result<T, Status>>,
    ) -> Result<T, LakeError> {
        match tokio::time::timeout(self.timeout, fut).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(s)) => Err(LakeError::Other(format!("shard rpc: {what}: {s}"))),
            Err(_) => Err(LakeError::Other(format!(
                "shard rpc: {what}: 超时（{:?} 内无响应）",
                self.timeout
            ))),
        }
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
            let resp = self
                .call(
                    "fetch_shards",
                    c.fetch_shards(pb::FetchShardsRequest {
                        table: table.to_string(),
                    }),
                )
                .await?;
            Ok(resp.into_inner().shards.iter().map(from_msg).collect())
        })
    }

    fn fetch_watermark<'a>(
        &'a self,
        known_manifest_ver: u64,
    ) -> BoxFuture<'a, Result<ShardRead, LakeError>> {
        Box::pin(async move {
            let mut c = self.client.clone();
            let resp = self
                .call(
                    "fetch_watermark",
                    c.fetch_watermark(pb::FetchWatermarkRequest { known_manifest_ver }),
                )
                .await?;
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
        known_batch_ids: Vec<String>,
    ) -> BoxFuture<'a, Result<ShardRead, LakeError>> {
        Box::pin(async move {
            let mut c = self.client.clone();
            let resp = self
                .call(
                    "fetch_shard",
                    c.fetch_shard(pb::FetchShardRequest {
                        shard: Some(to_msg(id)),
                        known_manifest_ver,
                        known_batch_ids,
                    }),
                )
                .await?;
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
