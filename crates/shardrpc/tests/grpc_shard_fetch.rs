//! 数据面 gRPC 的**真往返**与**跨网络对拍**（S5-6；R4 T12.1 的前置）。
//!
//! 两条用例各自对应一个此前只存在于"缝"上的承诺：
//!
//! 1. `batches_watermark_and_stale_survive_the_wire`：批次、**水位**、**STALE** 必须原样过网络。
//!    `§63.4` 留的坑正是"远端形态必须给真实水位" —— 远端若用 0 冒充水位，等于把 STALE 静默关掉，
//!    于是"实例已放弃副本、协调者 manifest 还没追上"的窗口会**静默少数据**（`§63.3`）。
//! 2. `parity_holds_with_instances_behind_grpc`：`§66` 那条**对拍**在远端形态下同样成立 ——
//!    单节点串行 vs 两个数据节点各写一半，结果逐行相等。这是 `§66.5` 说的"跨进程形态复用同一条
//!    对拍逻辑"的兑现（这里走的是**真 gRPC + TCP + IPC 编解码**，不是进程内的函数调用）。

use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::net::TcpListener;

use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_chunk::{
    ChunkId, ChunkKey, ChunkStore, ChunkStoreConfig, MemoryLedger, SealPolicy,
};
use yuntun_model::ops::CreateTableRequest;
use yuntun_query::{LocalCatalog, QueryEngine};
use yuntun_shardrpc::GrpcShardFetch;
use yuntun_store::{RemoteShard, ShardId, ShardReader, StoreConfig, create_store};

const TABLE: &str = "public.wire";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
}

fn batch(vals: &[i64]) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
}

/// 造一个"数据节点"的 chunk store（数据直接 append：本用例测的是**数据面**，不是写入路径）。
fn instance(dir: &yuntun_testkit::TestDir, instance_id: &str) -> Arc<ChunkStore> {
    ChunkStore::new(
        ChunkStoreConfig {
            // 行数阈值 3：本文件每个实例只 append 一批 3 行，于是落盘前**先 seal**。
            // 为什么必须 seal：`mark_committed` 只接受 `Sealed`（状态机 `open → flushed` 非法），
            // 而"已 commit 且已放弃副本"正是要验的水位场景（`§63.2`）。
            // 时间维度全部放长：本用例要的是"留在热副本里"，不是按时落盘。
            policy: SealPolicy {
                rows_threshold: 3,
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

fn write(store: &ChunkStore, vals: &[i64]) -> ChunkId {
    store
        .append(
            ChunkKey::new(ShardId::new(TABLE, "default", "2026-09-21T10:00"), 0),
            1,
            schema(),
            0,
            vec![batch(vals)],
            0,
        )
        .unwrap()
        .chunk_id
}

/// 起一个数据节点的 gRPC 服务，返回地址。
///
/// 先 bind 再移交监听器 ⇒ 调用方拿到端口且**不释放**，没有"探测端口 → 起服务"之间被抢的窗口。
async fn serve(reader: Arc<dyn ShardReader>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = yuntun_shardrpc::serve(reader, listener).await;
    });
    addr
}

/// 查询节点侧：把远端数据节点包成热读器（`GrpcShardFetch` → `RemoteShard`）。
async fn remote_reader(addr: &str) -> Arc<dyn ShardReader> {
    Arc::new(RemoteShard::new(Arc::new(
        GrpcShardFetch::connect(addr).await.unwrap(),
    )))
}

async fn create_table(catalog: &Arc<dyn CatalogOps>) {
    catalog
        .create_table(CreateTableRequest {
            name: "wire".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: Default::default(),
        })
        .await
        .unwrap();
}

/// 装配"若干实例 + 查询引擎"，返回排序后的结果。
async fn engine_with(instances: Vec<(&str, Arc<dyn ShardReader>)>) -> Vec<i64> {
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;

    let cache = Arc::new(LocalCatalog::new());
    cache.set_catalog_ops(catalog.clone());
    for (id, reader) in instances {
        // 名录是唯一真相（T12.3）：实例必须**登记进名录**，否则一次刷新就会用名录
        // 整体替换成员表、把这个实例（连同它的热读器）摘掉。
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

    let engine = QueryEngine::new(create_store(&StoreConfig::Memory).unwrap(), cache);
    let batches = engine
        .sql(&format!("SELECT a FROM yuntun.{TABLE} ORDER BY a"))
        .await
        .expect("查询应成功");
    let mut out = Vec::new();
    for b in batches {
        let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push(col.value(i));
        }
    }
    out
}

/// ① 批次、水位、STALE **原样过网络**。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batches_watermark_and_stale_survive_the_wire() {
    let dir = yuntun_testkit::TestDir::tmpfs("shardrpc-roundtrip");
    let store = instance(&dir, "inst-a");
    let chunk = write(&store, &[1, 2, 3]);
    store.mark_committed(chunk, 5).unwrap();

    let addr = serve(store.clone()).await;
    let reader = remote_reader(&addr).await;

    // ①-a 数据过去，且"还没放弃副本"时不报 STALE
    let fresh = reader.read_table(TABLE, 4).await.unwrap();
    assert_eq!(fresh.rows(), 3, "批次必须完好过网络（IPC 编解码 + gRPC）");
    assert_eq!(fresh.flushed_watermark, 0, "还没放弃副本 ⇒ 水位仍是 0");
    assert!(!fresh.stale, "没放弃副本 ⇒ 不可能丢数据 ⇒ 不该报 STALE");

    // ①-b 放弃本地副本之后再问：热数据确实没了，但**水位与 STALE 必须一起回来**
    assert!(store.reclaim(6) >= 1, "应回收那个已 commit 的 chunk");
    let behind = reader.read_table(TABLE, 4).await.unwrap();
    assert_eq!(behind.rows(), 0, "副本已放弃，热路径拿不到数据");
    assert!(
        behind.stale,
        "远端必须把 STALE 报回来 —— 否则协调者会把这批数据静默丢掉（§63.3/§63.4）"
    );
    assert_eq!(behind.flushed_watermark, 5, "水位要带得回来");
}

/// ② `§66` 的对拍在**远端形态**下同样成立：单节点串行 vs 两个 gRPC 数据节点。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parity_holds_with_instances_behind_grpc() {
    let all: Vec<i64> = (1..=6).collect();
    let (left, right) = all.split_at(3);

    // ① 单节点串行（也走网络，排除"远端路径本身少读"的干扰）
    let single = {
        let dir = yuntun_testkit::TestDir::tmpfs("wire-single");
        let s = instance(&dir, "solo");
        write(&s, &all);
        let addr = serve(s.clone()).await;
        engine_with(vec![("solo", remote_reader(&addr).await)]).await
    };

    // ② 两个数据节点各写一半（同名 shard：设计上"多 datanode 可同时写同一 partition"）
    let split = {
        let dir_a = yuntun_testkit::TestDir::tmpfs("wire-a");
        let dir_b = yuntun_testkit::TestDir::tmpfs("wire-b");
        let a = instance(&dir_a, "inst-a");
        let b = instance(&dir_b, "inst-b");
        write(&a, left);
        write(&b, right);
        let (addr_a, addr_b) = (serve(a.clone()).await, serve(b.clone()).await);
        engine_with(vec![
            ("inst-a", remote_reader(&addr_a).await),
            ("inst-b", remote_reader(&addr_b).await),
        ])
        .await
    };

    assert_eq!(single, all, "单节点（远端形态）应读到全部 6 行");
    assert_eq!(
        split, all,
        "远端形态的对拍必须同样成立：少一行 = 漏读某个数据节点；多一行 = 同一份数据读两次"
    );
}
