//! R5 / **T13.4 第二刀**：数据面 RPC 的**客户端超时** —— "无响应"也必须变成一个 `Err`。
//!
//! `architecture-with-chunk §4.3` 写的是"**失败 / 超时** ⇒ 协调者退化为只读冷数据，标记 partial"。
//! `§77` 兑现了前半句（失败 ⇒ 降级），但**超时**它兑现不了：`Err` 是对方给的，
//! 而"对方什么都没说"只有**调用方**能发现 —— 只能由传输层把"等够了"变成 `Err`。
//!
//! 本文件测两件事：
//!
//! ① **假死节点**（接受连接、永不回话）：三个 RPC 都必须在**超时上限内**返回 `Err`，
//!    且错误**可诊断**（写明"超时"，而不是又一个含糊的 deadline exceeded）；
//! ② **正常节点不被误判**：宽松超时下照常成功（超时不能变成"偶尔把好节点当坏的"）。

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::net::TcpListener;

use yuntun_model::error::LakeError;
use yuntun_shardrpc::{DEFAULT_TIMEOUT, GrpcShardFetch};
use yuntun_store::{ShardFetch, ShardId, ShardRead, ShardReader, ShardTier};

/// **假死**的数据节点：接受连接、但永远不回话。
///
/// 这与"连接拒绝"是**两回事**：拒绝会立刻返回错误（传输层自己就能发现），
/// 而假死的进程什么都没说 —— 现实形态是长 GC 停顿 / 被 CPU 抢光 / 网络黑洞。
#[derive(Debug)]
struct HungReader;

#[async_trait]
impl ShardReader for HungReader {
    fn tier(&self) -> ShardTier {
        ShardTier::Memory
    }
    fn version(&self) -> u64 {
        0
    }
    async fn shards_of(&self, _table: &str) -> Result<Vec<ShardId>, LakeError> {
        std::future::pending::<()>().await;
        unreachable!("pending 永不返回")
    }
    async fn read_shard(&self, _id: &ShardId, _known: u64) -> Result<ShardRead, LakeError> {
        std::future::pending::<()>().await;
        unreachable!("pending 永不返回")
    }
    async fn watermark(&self, _known: u64) -> Result<ShardRead, LakeError> {
        std::future::pending::<()>().await;
        unreachable!("pending 永不返回")
    }
}

/// 一个**健康但空**的读侧：用来验"正常节点不被误判"。
#[derive(Debug)]
struct EmptyReader;

#[async_trait]
impl ShardReader for EmptyReader {
    fn tier(&self) -> ShardTier {
        ShardTier::Memory
    }
    fn version(&self) -> u64 {
        0
    }
    async fn shards_of(&self, _table: &str) -> Result<Vec<ShardId>, LakeError> {
        Ok(Vec::new())
    }
    async fn read_shard(&self, _id: &ShardId, _known: u64) -> Result<ShardRead, LakeError> {
        Ok(ShardRead {
            batches: Vec::new(),
            flushed_watermark: 0,
            stale: false,
        })
    }
    async fn watermark(&self, known: u64) -> Result<ShardRead, LakeError> {
        Ok(ShardRead::empty(0, known))
    }
}

/// 起一个数据面服务，返回地址（先 bind 再移交监听器 ⇒ 没有 TOCTOU）。
async fn serve(reader: Arc<dyn ShardReader>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = yuntun_shardrpc::serve(reader, listener).await;
    });
    addr
}

/// ① 假死节点：三个 RPC 都在上限内失败，且错误写明"超时"。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hung_node_fails_within_the_bound_and_says_it_timed_out() {
    let addr = serve(Arc::new(HungReader)).await;
    let timeout = Duration::from_millis(300);
    let fetch = GrpcShardFetch::connect_with_timeout(&addr, timeout)
        .await
        .expect("建连本身没被超时（服务在 accept）");

    // 三个 RPC 都要有上限：read_table 的默认实现会走它们（枚举 → 读分片 → 水位）
    let id = ShardId::new("public.t", "default", "2026-09-23T10:00");

    let t0 = Instant::now();
    let e = fetch.fetch_shards("public.t").await.expect_err("假死 ⇒ 必须失败");
    let d = t0.elapsed();
    assert!(
        d >= Duration::from_millis(200),
        "应当**等满**超时才放弃（而不是立刻误判）：{d:?}"
    );
    assert!(d < Duration::from_secs(3), "必须**有界**，不能一直等：{d:?}");
    assert!(
        e.to_string().contains("超时"),
        "错误要能区分『没响应』与『拒绝连接』：{e}"
    );

    let e = fetch
        .fetch_shard(&id, 0, Vec::new())
        .await
        .expect_err("假死 ⇒ 必须失败");
    assert!(e.to_string().contains("超时"), "{e}");
    let e = fetch
        .fetch_watermark(0)
        .await
        .expect_err("假死 ⇒ 必须失败");
    assert!(e.to_string().contains("超时"), "{e}");
}

/// ② 正常节点**不被误判**：宽松超时下照常成功。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_healthy_node_is_not_misjudged() {
    let addr = serve(Arc::new(EmptyReader)).await;
    let fetch = GrpcShardFetch::connect_with_timeout(&addr, Duration::from_secs(5))
        .await
        .unwrap();

    let shards = fetch.fetch_shards("public.t").await.expect("健康节点应当成功");
    assert!(shards.is_empty());
    let wm = fetch.fetch_watermark(7).await.expect("水位应当成功");
    assert_eq!(wm.flushed_watermark, 0);
    assert!(!wm.stale);
}

/// 默认超时是"有限且够用"的：0 会让每次调用立刻失败，无穷会让查询挂死。
#[test]
fn default_timeout_is_finite_and_not_absurd() {
    assert!(DEFAULT_TIMEOUT >= Duration::from_secs(1));
    assert!(DEFAULT_TIMEOUT <= Duration::from_secs(60));
}
