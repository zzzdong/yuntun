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
use yuntun_store::{ShardAggs, ShardFetch, ShardId, ShardRead, ShardReader};

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

    /// `§173`：**聚合拉取** —— 在数据所在处折，只回**标量**。
    ///
    /// 实现手法是刻意的"不额外发明一套"：直接调本地 reader 的 `read_table_aggs`，
    /// 而它在服务端的默认实现就是"读行 + 折叠"（服务端本来就在数据旁边）。
    /// ⇒ 服务端算出来的部分状态与协调者兜底路径算出来的**逐字相同**（同一份代码）。
    async fn fetch_shard_aggs(
        &self,
        req: Request<pb::FetchShardAggsRequest>,
    ) -> Result<Response<pb::FetchShardAggsResponse>, Status> {
        let req = req.into_inner();
        let specs = agg_specs_from_msg(&req.aggs)?;
        // `§176`：谓词与聚合一起在**数据所在处**生效（缺席 = 不过滤）
        let predicate = req
            .predicate
            .as_ref()
            .map(predicate_from_msg)
            .transpose()?
            .flatten();
        // `§177`：分组键也一起送下去 —— 分组是在**数据所在处**做的，
        // 响应的形状（一个组 vs 每组一行）由"带不带 group_by"决定。
        let query = yuntun_model::agg::AggQuery {
            specs,
            group_by: req.group_by.clone(),
            predicate,
        };
        let aggs = self
            .reader
            .read_table_aggs(
                &req.table,
                req.known_manifest_ver,
                &req.known_batch_ids,
                &query,
            )
            .await
            .map_err(internal)?;
        // 形状必须与请求一致（**响亮失败**："少了一个组/少了一个聚合"看起来都像正常答案）
        let (partials, groups) = match &aggs.states {
            yuntun_model::agg::AggStates::Plain(p) => {
                if !query.group_by.is_empty() {
                    return Err(Status::internal(
                        "聚合形状不符：请求带了 group_by，却算出不分组的结果".to_string(),
                    ));
                }
                if p.len() != query.specs.len() {
                    return Err(Status::internal(format!(
                        "聚合部分状态个数（{}）与请求（{}）不符",
                        p.len(),
                        query.specs.len()
                    )));
                }
                (p.iter().map(partial_to_msg).collect(), Vec::new())
            }
            yuntun_model::agg::AggStates::Grouped(gs) => {
                if query.group_by.is_empty() {
                    return Err(Status::internal(
                        "聚合形状不符：请求没带 group_by，却算出分组结果".to_string(),
                    ));
                }
                let mut out = Vec::with_capacity(gs.len());
                for g in gs {
                    if g.key.len() != query.group_by.len() || g.states.len() != query.specs.len() {
                        return Err(Status::internal(format!(
                            "分组状态的形状不符：键 {} 个（请求 {}）、状态 {} 个（请求 {}）",
                            g.key.len(),
                            query.group_by.len(),
                            g.states.len(),
                            query.specs.len()
                        )));
                    }
                    out.push(group_to_msg(g));
                }
                (Vec::new(), out)
            }
        };
        Ok(Response::new(pb::FetchShardAggsResponse {
            partials,
            groups,
            flushed_watermark: aggs.flushed_watermark,
            stale: aggs.stale,
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

    /// `§173`：**聚合拉取**（**覆写**了 trait 的默认实现）—— 这一步才是"下推"的落点。
    ///
    /// 默认实现会把行拉回来再折（那等价于没下推，网络一点没省）；这里把 specs 送到对端，
    /// 只把**标量**带回来。
    fn fetch_shard_aggs<'a>(
        &'a self,
        table: &'a str,
        known_manifest_ver: u64,
        known_batch_ids: Vec<String>,
        query: yuntun_model::agg::AggQuery,
    ) -> BoxFuture<'a, Result<ShardAggs, LakeError>> {
        Box::pin(async move {
            let mut c = self.client.clone();
            let grouped = query.is_grouped();
            let specs = query.specs.clone();
            let resp = self
                .call(
                    "fetch_shard_aggs",
                    c.fetch_shard_aggs(pb::FetchShardAggsRequest {
                        table: table.to_string(),
                        known_manifest_ver,
                        known_batch_ids,
                        aggs: specs.iter().map(agg_spec_to_msg).collect(),
                        // `§176`：谓词过网（`None` = 不过滤）
                        predicate: query.predicate.as_ref().map(predicate_to_msg),
                        // `§177`：分组键过网（空 = 不分组 ⇒ 响应走 `partials`）
                        group_by: query.group_by.clone(),
                    }),
                )
                .await?;
            let resp = resp.into_inner();
            let states = if grouped {
                let mut gs = Vec::with_capacity(resp.groups.len());
                for m in &resp.groups {
                    gs.push(group_from_msg(m, &query)?);
                }
                yuntun_model::agg::AggStates::Grouped(gs)
            } else {
                // 长度是契约：**不许**"按短的来"（少一个聚合的结果是错的，而它看起来很正常）
                if resp.partials.len() != specs.len() {
                    return Err(LakeError::Other(format!(
                        "shard rpc: fetch_shard_aggs: 部分状态个数（{}）与请求（{}）不符",
                        resp.partials.len(),
                        specs.len()
                    )));
                }
                let mut partials = Vec::with_capacity(resp.partials.len());
                for m in &resp.partials {
                    partials.push(partial_from_msg(m)?);
                }
                yuntun_model::agg::AggStates::Plain(partials)
            };
            Ok(ShardAggs {
                states,
                flushed_watermark: resp.flushed_watermark,
                stale: resp.stale,
            })
        })
    }
}

// ---------------------------------------------------------------- 聚合的消息转换（`§173`）

fn agg_spec_to_msg(s: &yuntun_model::agg::AggSpec) -> pb::AggSpecMsg {
    pb::AggSpecMsg {
        kind: s.kind as u32,
        column: s.column.clone().unwrap_or_default(),
    }
}

/// 请求方向：**不懂的 kind 直接报错**（协议里加了新聚合而本节点不认识时，
/// 静默忽略会变成"少算一个聚合"——那是最坏的一种成功）。
fn agg_specs_from_msg(
    msgs: &[pb::AggSpecMsg],
) -> Result<Vec<yuntun_model::agg::AggSpec>, Status> {
    let mut out = Vec::with_capacity(msgs.len());
    for m in msgs {
        let kind = yuntun_model::agg::AggKind::from_u32(m.kind).ok_or_else(|| {
            Status::invalid_argument(format!("不认识的聚合种类：{}", m.kind))
        })?;
        if kind.needs_column() && m.column.is_empty() {
            return Err(Status::invalid_argument(format!("{kind:?} 缺列名")));
        }
        out.push(yuntun_model::agg::AggSpec {
            kind,
            column: (!m.column.is_empty()).then(|| m.column.clone()),
        });
    }
    Ok(out)
}

fn partial_to_msg(p: &yuntun_model::agg::PartialAgg) -> pb::PartialAggMsg {
    use yuntun_model::agg::{AggScalar, PartialAgg};
    let mut msg = pb::PartialAggMsg::default();
    match p {
        PartialAgg::Count(c) => {
            msg.kind = 0;
            msg.count = *c;
        }
        PartialAgg::SumInt(s) => {
            msg.kind = 1;
            msg.has_sum = s.is_some();
            msg.sum = s.unwrap_or(0);
        }
        PartialAgg::Min(s) | PartialAgg::Max(s) => {
            msg.kind = if matches!(p, PartialAgg::Min(_)) { 2 } else { 3 };
            msg.has_scalar = s.is_some();
            if let Some(v) = s {
                match v {
                    AggScalar::Int(i) => msg.scalar_int = *i,
                    AggScalar::Text(t) => {
                        msg.scalar_text = t.clone();
                        msg.scalar_is_text = true;
                    }
                }
            }
        }
    }
    msg
}

fn partial_from_msg(m: &pb::PartialAggMsg) -> Result<yuntun_model::agg::PartialAgg, LakeError> {
    use yuntun_model::agg::{AggScalar, PartialAgg};
    Ok(match m.kind {
        0 => PartialAgg::Count(m.count),
        1 => PartialAgg::SumInt(m.has_sum.then_some(m.sum)),
        2 | 3 => {
            let s = if !m.has_scalar {
                None
            } else if m.scalar_is_text {
                Some(AggScalar::Text(m.scalar_text.clone()))
            } else {
                Some(AggScalar::Int(m.scalar_int))
            };
            if m.kind == 2 {
                PartialAgg::Min(s)
            } else {
                PartialAgg::Max(s)
            }
        }
        other => {
            return Err(LakeError::Other(format!(
                "远端返回了不认识的聚合部分状态：kind={other}"
            )))
        }
    })
}

// ---------------------------------------------------------------- 谓词的消息转换（`§176`）

fn predicate_to_msg(p: &yuntun_model::predicate::Predicate) -> pb::PredicateMsg {
    use yuntun_model::predicate::{Literal, Predicate};
    let mut msg = pb::PredicateMsg::default();
    match p {
        Predicate::Cmp { column, op, value } => {
            msg.kind = 0;
            msg.column = column.clone();
            msg.op = *op as u32;
            match value {
                Literal::Int(i) => {
                    msg.literal_kind = 1;
                    msg.literal_int = *i;
                }
                Literal::Text(t) => {
                    msg.literal_kind = 2;
                    msg.literal_text = t.clone();
                }
            }
        }
        Predicate::IsNull { column, negated } => {
            msg.kind = 1;
            msg.column = column.clone();
            msg.negated = *negated;
        }
        Predicate::And(parts) => {
            msg.kind = 2;
            msg.parts = parts.iter().map(predicate_to_msg).collect();
        }
        Predicate::Or(parts) => {
            msg.kind = 3;
            msg.parts = parts.iter().map(predicate_to_msg).collect();
        }
    }
    msg
}

/// 请求方向：**不认识的形状一律报错**（与 `agg_specs_from_msg` 同一条纪律 ——
/// 静默忽略一个谓词 = 把"筛过的数据"当成"没筛"，那是最坏的一种成功）。
fn predicate_from_msg(
    m: &pb::PredicateMsg,
) -> Result<Option<yuntun_model::predicate::Predicate>, Status> {
    use yuntun_model::predicate::{CmpOp, Literal, Predicate};
    Ok(Some(match m.kind {
        0 => {
            let op = CmpOp::from_u32(m.op)
                .ok_or_else(|| Status::invalid_argument(format!("不认识的比较符：{}", m.op)))?;
            let value = match m.literal_kind {
                1 => Literal::Int(m.literal_int),
                2 => Literal::Text(m.literal_text.clone()),
                other => {
                    return Err(Status::invalid_argument(format!(
                        "不认识的谓词字面量：{other}"
                    )))
                }
            };
            if m.column.is_empty() {
                return Err(Status::invalid_argument("比较谓词缺列名"));
            }
            Predicate::Cmp {
                column: m.column.clone(),
                op,
                value,
            }
        }
        1 => {
            if m.column.is_empty() {
                return Err(Status::invalid_argument("IS NULL 谓词缺列名"));
            }
            Predicate::IsNull {
                column: m.column.clone(),
                negated: m.negated,
            }
        }
        2 | 3 => {
            let mut parts = Vec::with_capacity(m.parts.len());
            for sub in &m.parts {
                match predicate_from_msg(sub)? {
                    Some(p) => parts.push(p),
                    None => return Err(Status::invalid_argument("子谓词为空")),
                }
            }
            if parts.is_empty() {
                return Err(Status::invalid_argument("AND/OR 谓词没有子项"));
            }
            if m.kind == 2 {
                Predicate::And(parts)
            } else {
                Predicate::Or(parts)
            }
        }
        other => {
            return Err(Status::invalid_argument(format!(
                "不认识的谓词种类：{other}"
            )))
        }
    }))
}

/// `§177`：一组（键 + 状态）→ 消息。
fn group_to_msg(g: &yuntun_model::agg::GroupStates) -> pb::GroupStatesMsg {
    pb::GroupStatesMsg {
        key: g.key.iter().map(scalar_to_msg).collect(),
        states: g.states.iter().map(partial_to_msg).collect(),
    }
}

/// `§177`：消息 → 一组。**形状不符一律报错**（少一个键列 = 把不同的组并成一组）。
fn group_from_msg(
    m: &pb::GroupStatesMsg,
    query: &yuntun_model::agg::AggQuery,
) -> Result<yuntun_model::agg::GroupStates, LakeError> {
    if m.key.len() != query.group_by.len() {
        return Err(LakeError::Other(format!(
            "shard rpc: fetch_shard_aggs: 分组键个数（{}）与请求（{}）不符",
            m.key.len(),
            query.group_by.len()
        )));
    }
    if m.states.len() != query.specs.len() {
        return Err(LakeError::Other(format!(
            "shard rpc: fetch_shard_aggs: 组内状态个数（{}）与请求（{}）不符",
            m.states.len(),
            query.specs.len()
        )));
    }
    let key: Result<Vec<yuntun_model::agg::KeyPart>, LakeError> =
        m.key.iter().map(scalar_from_msg).collect();
    let states: Result<Vec<yuntun_model::agg::PartialAgg>, LakeError> =
        m.states.iter().map(partial_from_msg).collect();
    Ok(yuntun_model::agg::GroupStates {
        key: key?,
        states: states?,
    })
}

/// `has_value = false` ⇒ NULL（分组里 NULL 是**一个组**）。
fn scalar_to_msg(v: &yuntun_model::agg::KeyPart) -> pb::ScalarMsg {
    let mut m = pb::ScalarMsg::default();
    match v {
        None => m.has_value = false,
        Some(yuntun_model::agg::AggScalar::Int(i)) => {
            m.has_value = true;
            m.int_val = *i;
        }
        Some(yuntun_model::agg::AggScalar::Text(t)) => {
            m.has_value = true;
            m.is_text = true;
            m.text_val = t.clone();
        }
    }
    m
}

fn scalar_from_msg(
    m: &pb::ScalarMsg,
) -> Result<yuntun_model::agg::KeyPart, LakeError> {
    if !m.has_value {
        return Ok(None);
    }
    Ok(Some(if m.is_text {
        yuntun_model::agg::AggScalar::Text(m.text_val.clone())
    } else {
        yuntun_model::agg::AggScalar::Int(m.int_val)
    }))
}
