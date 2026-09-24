//! **§89：把"结果不完整"交给客户端** —— Flight SQL 侧。
//!
//! `architecture §4.2` 第三条要的是"返回可用结果 + **`partial: true`** + **缺失来源列表**"。
//! 引擎早就算出来了（`§77`），但从引擎到协议层这一段**一路丢掉** —— 用户只能从服务端日志里猜。
//! 本用例钉住这条链真的通了，并且两个方向都钉：
//!
//! | # | 场景 | 期望 |
//! |---|---|---|
//! | ① | 某个来源**读不到** | 结果照常返回（降级），且 **schema 消息的 `app_metadata`** 里显式 `partial: true` + **点名缺了谁** |
//! | ② | 所有来源都读得到 | **空 `app_metadata`**（假警报同样是错：它会让调用方不敢信任何结果） |
//!
//! 为什么看**第一条**消息（schema）：Flight SQL 里 schema 消息是先发的那条，`app_metadata`
//! 就挂在它身上 —— 而它**来得及**带上结论，因为热读（`scan` 里的 fanout）发生在**物理计划期**：
//! `execute_stream` 返回时，sink 里已经是结论了（见 `crates/server/src/flight.rs::do_get_sql`）。

use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::CommandStatementQuery;
use arrow_flight::{FlightData, FlightDescriptor};
use async_trait::async_trait;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_chunk::{ChunkKey, ChunkStore, ChunkStoreConfig, MemoryLedger, SealPolicy};
use yuntun_model::error::LakeError;
use yuntun_model::ops::CreateTableRequest;
use yuntun_query::{LocalCatalog, PartialPolicy, QueryEngine};
use yuntun_server::{flight::command_bytes, FlightServer};
use yuntun_store::{create_store, ShardId, ShardRead, ShardReader, ShardTier, StoreConfig};

const TABLE: &str = "public.partialwire";
const SHARD: &str = "default";
const WINDOW: &str = "2026-09-24T10:00";
const QUERY: &str = "SELECT a FROM yuntun.public.partialwire";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
}

fn batch(vals: &[i64]) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
}

fn shard_id() -> ShardId {
    ShardId::new(TABLE, SHARD, WINDOW)
}

// ---------------------------------------------------------------- 假实例

/// **读不到**的实例：连接拒绝（`GrpcShardFetch` 也是第一次调用就建连）。
#[derive(Debug)]
struct Unreachable;

#[async_trait]
impl ShardReader for Unreachable {
    fn tier(&self) -> ShardTier {
        ShardTier::Memory
    }
    fn version(&self) -> u64 {
        0
    }
    async fn shards_of(&self, _table: &str) -> Result<Vec<ShardId>, LakeError> {
        Err(LakeError::Other("connection refused".into()))
    }
    async fn read_shard(&self, _id: &ShardId, _known: u64) -> Result<ShardRead, LakeError> {
        Err(LakeError::Other("connection refused".into()))
    }
    async fn watermark(&self, _known: u64) -> Result<ShardRead, LakeError> {
        Err(LakeError::Other("connection refused".into()))
    }
}

// ---------------------------------------------------------------- 夹具

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

async fn create_table(catalog: &Arc<dyn CatalogOps>) {
    catalog
        .create_table(CreateTableRequest {
            name: "partialwire".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: Default::default(),
        })
        .await
        .unwrap();
}

/// 装配引擎（若干来源）并**在进程内起一个真 Flight 服务**，返回连好的客户端。
async fn serve_with_sources(
    sources: Vec<(&str, Arc<dyn ShardReader>)>,
) -> (
    FlightServiceClient<tonic::transport::Channel>,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;

    let cache = Arc::new(LocalCatalog::new());
    cache.set_catalog_ops(catalog.clone());
    for (id, reader) in sources {
        // 名录是唯一真相：来源必须登记，否则一次刷新就会把它摘掉
        catalog
            .register_datanode(yuntun_model::meta::DatanodeMember {
                instance_id: id.to_string(),
                address: String::new(),
                registered_at_ms: 0,
            })
            .await
            .unwrap();
        cache.set_hot_shards(id, reader);
    }
    cache.refresh(&catalog).await.unwrap();

    let engine = Arc::new(
        QueryEngine::new(create_store(&StoreConfig::Memory).unwrap(), cache)
            .with_partial_policy(PartialPolicy::Allow),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let svc = arrow_flight::flight_service_server::FlightServiceServer::new(
        FlightServer::new_readonly(engine, catalog.clone()),
    );
    let server_shutdown = shutdown.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move { server_shutdown.cancelled().await },
            )
            .await
            .unwrap();
    });

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    (FlightServiceClient::new(channel), shutdown, server)
}

/// `SELECT` → 收下**全部** `FlightData`（第一条是 schema 消息，`app_metadata` 挂在它身上）。
async fn collect_flight_data(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
) -> Vec<FlightData> {
    let cmd = CommandStatementQuery {
        query: QUERY.to_string(),
        transaction_id: None,
    };
    let desc = FlightDescriptor::new_cmd(command_bytes(&cmd));
    let info = client
        .get_flight_info(desc)
        .await
        .unwrap()
        .into_inner();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let mut stream = client.do_get(ticket).await.unwrap().into_inner();
    let mut datas = Vec::new();
    while let Some(fd) = stream.next().await {
        datas.push(fd.unwrap());
    }
    datas
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

// ---------------------------------------------------------------- ① 缺失 ⇒ 显式标记 + 点名

/// 一个来源读不到 ⇒ 结果照常返回（降级），但 **schema 消息的 `app_metadata` 必须显式
/// 标记 `partial: true` 并点名缺了谁** —— 否则用户拿到的是"看起来完整"的错觉。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_result_is_visible_in_flight_metadata() {
    let dir = yuntun_testkit::TestDir::tmpfs("partial-wire-degrade");
    let healthy = instance(&dir, "inst-a");
    write(&healthy, &[1, 2, 3]);

    let (mut client, shutdown, server) = serve_with_sources(vec![
        ("inst-a", healthy.clone() as Arc<dyn ShardReader>),
        ("inst-b", Arc::new(Unreachable) as Arc<dyn ShardReader>),
    ])
    .await;

    let datas = collect_flight_data(&mut client).await;

    // ① 可用部分照常给出：降级 ≠ 失败
    let batches = arrow_flight::utils::flight_data_to_batches(&datas).unwrap();
    assert_eq!(values(&batches), vec![1, 2, 3], "健康来源的数据必须完整返回");

    // ② **schema 消息**的 app_metadata 必须带着"结果不完整"这个事实
    let meta = std::str::from_utf8(&datas[0].app_metadata).unwrap();
    assert!(
        meta.contains("\"partial\":true"),
        "缺了来源却不在 wire 上标记 = 静默少数据：{meta:?}"
    );
    assert!(
        meta.contains("inst-b") && meta.contains("public.partialwire"),
        "缺失来源必须点名（哪张表、哪个实例）：{meta}"
    );
    assert!(
        meta.contains("connection refused"),
        "原因要能排障：{meta}"
    );

    shutdown.cancel();
    let _ = server.await;
}

// ---------------------------------------------------------------- ② 完整 ⇒ 空 metadata

/// 全都读得到 ⇒ **空 `app_metadata`**。假警报同样是错：一个恒存在的 `partial` 字段会让
/// 调用方要么无视它、要么不敢相信任何结果 —— 两种情况都让这条链失去意义。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn complete_result_carries_no_partial_metadata() {
    let dir_a = yuntun_testkit::TestDir::tmpfs("partial-wire-full-a");
    let dir_b = yuntun_testkit::TestDir::tmpfs("partial-wire-full-b");
    let a = instance(&dir_a, "inst-a");
    let b = instance(&dir_b, "inst-b");
    write(&a, &[1, 2]);
    write(&b, &[3, 4]);

    let (mut client, shutdown, server) = serve_with_sources(vec![
        ("inst-a", a.clone() as Arc<dyn ShardReader>),
        ("inst-b", b.clone() as Arc<dyn ShardReader>),
    ])
    .await;

    let datas = collect_flight_data(&mut client).await;
    let batches = arrow_flight::utils::flight_data_to_batches(&datas).unwrap();
    assert_eq!(values(&batches), vec![1, 2, 3, 4]);

    assert!(
        datas[0].app_metadata.is_empty(),
        "完整结果的 app_metadata 必须是空的（不许假警报）：{:?}",
        std::str::from_utf8(&datas[0].app_metadata)
    );

    shutdown.cancel();
    let _ = server.await;
}
