//! T12.3 第一刀：**成员表作为唯一真相**。
//!
//! 此前"有哪些实例"（快照的 `nodes`，一个 `Vec<String>`）与"能读谁的热数据"
//! （`HotShards`，一个 `BTreeMap<instance, reader>`）是**两份各自更新的真相** ——
//! `§65.5` 记为"同源但**未强制**一致"。本文件的用例就是那条强制：
//!
//! 1. 登记热读器 ⇒ 该实例**必然**进成员表，且会出现在下一份快照的 `nodes` 里
//!    （旧形态下这两处可以互相矛盾，而矛盾会让查询按错误的实例集合算归属）；
//! 2. 成员表带**地址** —— 只有 ID 时"有哪些实例"变不成"怎么连"（T12.3 要给出的东西）；
//! 3. **摘除成员 ⇒ 它的热数据不再参与查询**（可观测的行为变化，不是纯结构改动）。

use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_chunk::{ChunkKey, ChunkStore, ChunkStoreConfig, MemoryLedger, SealPolicy};
use yuntun_model::ops::CreateTableRequest;
use yuntun_query::{LocalCatalog, Member, QueryEngine};
use yuntun_store::{ShardId, ShardReader, StoreConfig, create_store};

/// 全限定表名：必须与 `ChunkKey.shard.table` 一致，否则热读按表名过滤会扑空
const TABLE: &str = "public.members";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
}

fn batch(vals: &[i64]) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
}

/// 造一个"实例"的 chunk store（数据直接 append：本用例测读侧，不是写入路径）。
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
        .append(
            ChunkKey::new(ShardId::new(TABLE, "default", "2026-09-23T10:00"), 0),
            1,
            schema(),
            0,
            vec![batch(vals)],
            0,
        )
        .unwrap();
}

async fn create_table(catalog: &Arc<dyn CatalogOps>) {
    catalog
        .create_table(CreateTableRequest {
            name: "members".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: Default::default(),
        })
        .await
        .unwrap();
}

/// 装配"若干实例 + 查询引擎"。
async fn setup(
    instances: Vec<(&str, Arc<ChunkStore>)>,
) -> (Arc<LocalCatalog>, Arc<dyn CatalogOps>, QueryEngine) {
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;

    let cache = Arc::new(LocalCatalog::new());
    cache.set_catalog_ops(catalog.clone());
    for (id, store) in instances {
        let reader: Arc<dyn ShardReader> = store;
        cache.set_hot_shards(id, reader);
    }
    cache.refresh(&catalog).await.unwrap();

    let engine = QueryEngine::new(create_store(&StoreConfig::Memory).unwrap(), cache.clone());
    (cache, catalog, engine)
}

async fn sorted_values(engine: &QueryEngine) -> Vec<i64> {
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

/// ① 登记热读器 ⇒ 成员表与快照 `nodes` **不可能再漂移**（`§65.5` 的坑就此关闭）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registering_a_hot_reader_also_registers_the_member() {
    let dir = yuntun_testkit::TestDir::tmpfs("member-register");
    let a = instance(&dir, "inst-a");
    write(&a, &[1]);

    let (cache, catalog, _engine) = setup(vec![("inst-a", a)]).await;

    let ids: Vec<String> = cache.members().into_iter().map(|m| m.instance_id).collect();
    assert!(
        ids.contains(&"inst-a".to_string()),
        "登记热读器必须同时把它登记为成员，否则\"名录\"与\"能读谁\"会各说各话：{ids:?}"
    );

    // 快照的 `nodes`（查询规划用的"分片归属"）同样必须包含它 —— 这是**强制**的那一半
    cache.refresh(&catalog).await.unwrap();
    assert!(
        cache.snapshot().nodes.contains(&"inst-a".to_string()),
        "快照 nodes 必须由成员表派生；实际 {:?}",
        cache.snapshot().nodes
    );
}

/// ② 成员表带地址（T12.3 的载荷：没有地址，"有哪些实例"变不成"怎么连"）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn members_carry_data_plane_addresses() {
    let cache = LocalCatalog::new();
    cache.set_members(vec![
        Member::at("inst-b", "10.0.0.7:50051"),
        Member::local("inst-a"),
    ]);

    let got = cache.members();
    let ids: Vec<&str> = got.iter().map(|m| m.instance_id.as_str()).collect();
    assert_eq!(ids, vec!["inst-a", "inst-b"], "成员表按 ID 有序（取值确定）");
    assert_eq!(got[0].address, None, "同进程实例没有网络地址");
    assert_eq!(
        got[1].address.as_deref(),
        Some("10.0.0.7:50051"),
        "远端实例的地址必须存得住 —— 查询侧要按它装配热读器"
    );
}

/// ③ **摘除成员 ⇒ 它的热数据不再参与查询**（行为可观测，不是纯结构改动）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removing_a_member_takes_its_hot_shards_out_of_queries() {
    let dir = yuntun_testkit::TestDir::tmpfs("member-remove");
    let a = instance(&dir, "inst-a");
    let b = instance(&dir, "inst-b");
    write(&a, &[1]);
    write(&b, &[2]);

    let (cache, catalog, engine) = setup(vec![("inst-a", a), ("inst-b", b)]).await;
    assert_eq!(
        sorted_values(&engine).await,
        vec![1, 2],
        "两个实例各一行 ⇒ 两行都该读到（否则反证不成立）"
    );

    // 成员发现宣布 inst-b 已下线（真实系统里：走 raft 的成员变更，且**在它文件已提交之后**）
    cache.set_members(vec![Member::local("inst-a")]);
    cache.refresh(&catalog).await.unwrap();

    assert_eq!(
        cache.hot_shards().len(),
        1,
        "被摘除成员的热读器必须一并移除：留着它 = 读一个已经不存在的实例"
    );
    assert_eq!(
        sorted_values(&engine).await,
        vec![1],
        "摘除后 inst-b 的热数据不该再参与查询"
    );
}
