//! R5 / **T13.4 第二刀（组合）**：**挂住的来源 ⇒ 查询降级，并点名"超时"**。
//!
//! 这一条把本刀与 `§77` **接起来**：
//!
//! ```text
//!   假死数据节点（真 gRPC，永不回话）
//!        │  ① 传输层：等满超时 ⇒ Err（写明"超时"，§78）
//!        ▼
//!   查询扇出：Err ⇒ 降级为部分结果 + 记下缺失来源（§77）
//!        │
//!        ▼
//!   调用方：拿到健康来源的**完整**数据 + `missing` 里点名那个假死节点
//! ```
//!
//! 两半各自有专门用例（`timeout_bounds_the_wait.rs` 与 `query/tests/partial_fanout.rs`），
//! 但**接起来**才是 `architecture §4.3` 那句"失败/**超时** ⇒ 协调者退化为只读冷数据"的完整兑现 ——
//! 所以这里用真 chunk store、真 gRPC、真查询引擎跑一遍。
//!
//! 还有一个不显眼但重要的断言：**查询必须在超时上限内返回**。若超时只让错误"可诊断"
//! 却没让查询提前结束，那等于没修 —— 用户要的是"不被拖住"。

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::net::TcpListener;

use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_chunk::{ChunkKey, ChunkStore, ChunkStoreConfig, MemoryLedger, SealPolicy};
use yuntun_model::error::LakeError;
use yuntun_model::ops::CreateTableRequest;
use yuntun_query::{LocalCatalog, PartialPolicy, QueryEngine};
use yuntun_shardrpc::{GrpcShardFetch, serve};
use yuntun_store::{RemoteShard, ShardId, ShardRead, ShardReader, ShardTier, StoreConfig};

const TABLE: &str = "public.hung";
const SHARD: &str = "default";
const WINDOW: &str = "2026-09-23T10:00";
/// 假死来源的超时：短到用例跑得快，长到足以证明"确实等了"
const HUNG_TIMEOUT: Duration = Duration::from_millis(300);

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
}

fn batch(vals: &[i64]) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
}

fn shard_id() -> ShardId {
    ShardId::new(TABLE, SHARD, WINDOW)
}

/// **假死**的数据节点：接受连接、但永远不回话（长 GC / 被抢光 / 网络黑洞的现实形态）。
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

/// 健康的本地实例（真 chunk store，数据留在热副本里）。
fn instance(dir: &yuntun_testkit::TestDir, instance_id: &str) -> Arc<ChunkStore> {
    ChunkStore::new(
        ChunkStoreConfig {
            policy: SealPolicy {
                rows_threshold: usize::MAX,
                bytes_threshold: usize::MAX,
                min_resident: Duration::from_secs(3600),
                max_flush_delay: Duration::from_secs(3600),
                max_resident: Duration::from_secs(3600),
                phase_spread: Duration::ZERO,
            },
            spill_dir: dir.join("spill"),
            instance_id: instance_id.into(),
            wal_segment: 0,
        },
        MemoryLedger::new("chunk", 1 << 24),
    )
}

fn write(store: &ChunkStore, vals: &[i64]) {
    store
        .append(ChunkKey::new(shard_id(), 0), 1, schema(), 0, vec![batch(vals)], 0)
        .unwrap();
}

/// 起一个**假死**的数据面服务，返回地址。
async fn serve_hung() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = serve(Arc::new(HungReader), listener).await;
    });
    addr
}

async fn engine_with(policy: PartialPolicy, hung_addr: &str, healthy: Arc<ChunkStore>) -> QueryEngine {
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    catalog
        .create_table(CreateTableRequest {
            name: "hung".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: Default::default(),
        })
        .await
        .unwrap();

    let cache = Arc::new(LocalCatalog::new());
    cache.set_catalog_ops(catalog.clone());
    for id in ["inst-a", "inst-b"] {
        catalog
            .register_datanode(yuntun_model::meta::DatanodeMember {
                instance_id: id.to_string(),
                address: String::new(),
                registered_at_ms: 0,
            })
            .await
            .unwrap();
    }
    // inst-a：进程内真 chunk store；inst-b：**真 gRPC** 但服务端永不回话
    cache.set_hot_shards("inst-a", healthy);
    let fetch = Arc::new(
        GrpcShardFetch::connect_with_timeout(hung_addr, HUNG_TIMEOUT)
            .await
            .expect("假死服务仍在 accept，建连本身会成功"),
    );
    cache.set_hot_shards("inst-b", Arc::new(RemoteShard::new(fetch)));
    cache.refresh(&catalog).await.unwrap();

    QueryEngine::new(yuntun_store::create_store(&StoreConfig::Memory).unwrap(), cache)
        .with_partial_policy(policy)
}

fn values(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push(col.value(i));
        }
    }
    out
}

const QUERY: &str = "SELECT a FROM yuntun.public.hung ORDER BY a";

/// **挂住的来源 ⇒ 降级**：健康来源的数据完整返回，缺失来源点名"超时"，且查询**有界结束**。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_source_degrades_the_query_and_names_the_timeout() {
    let dir = yuntun_testkit::TestDir::tmpfs("hung-degrade");
    let healthy = instance(&dir, "inst-a");
    write(&healthy, &[1, 2, 3]);

    let hung_addr = serve_hung().await;
    let engine = engine_with(PartialPolicy::Allow, &hung_addr, healthy).await;

    let t0 = Instant::now();
    let out = engine
        .sql_partial(QUERY)
        .await
        .expect("挂住的来源必须**降级**，而不是让查询失败或一直挂住");
    let elapsed = t0.elapsed();

    assert_eq!(
        values(&out.batches),
        vec![1, 2, 3],
        "健康来源的数据必须完整返回"
    );
    assert!(out.is_partial(), "挂住的来源没被标记 = 静默少数据");
    let missing = out.partial.missing();
    assert_eq!(missing.len(), 1, "只该缺 inst-b：{missing:?}");
    assert_eq!(missing[0].instance, "inst-b");
    assert!(
        missing[0].reason.contains("超时"),
        "原因要能区分『没响应』与『拒绝连接』——处置完全不同：{}",
        missing[0].reason
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "查询必须**有界**（超时上限 {HUNG_TIMEOUT:?}，不是一直等）：{elapsed:?}"
    );
}

/// `reject` 策略对**超时**同样生效：当场失败并点名（策略不能只覆盖"失败"那一半）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reject_policy_also_rejects_a_timeout() {
    let dir = yuntun_testkit::TestDir::tmpfs("hung-reject");
    let healthy = instance(&dir, "inst-a");
    write(&healthy, &[1, 2, 3]);

    let hung_addr = serve_hung().await;
    let engine = engine_with(PartialPolicy::Reject, &hung_addr, healthy).await;

    let e = engine
        .sql_partial(QUERY)
        .await
        .expect_err("reject 策略下超时也必须失败");
    let msg = e.to_string();
    assert!(msg.contains("inst-b"), "错误要点名来源：{msg}");
    assert!(msg.contains("拒绝部分结果"), "错误要说清是为什么失败：{msg}");
}
