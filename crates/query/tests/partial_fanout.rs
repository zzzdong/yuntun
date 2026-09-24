//! R5 / **T13.4 第一刀**：**部分结果** —— 把"**拿不到**"与"**还没拿到**"分开。
//!
//! 四件事，各有一条用例：
//!
//! | # | 场景 | 期望 |
//! |---|---|---|
//! | ① | 某个来源**读不到**（连接拒绝） | **降级**：返回可用部分 + **标记缺了谁、为什么** |
//! | ② | 同上 + `partial = reject` | **当场失败**，错误里点名缺了谁 |
//! | ③ | 所有来源都读得到 | **不许**误标 partial（假警报也是错） |
//! | ④ | 某个来源**永远 STALE** | **仍然刷新重试 → 响亮失败**，**绝不**降级成 partial |
//!
//! `§88` 又加了三条（扇出的形状本身）：
//!
//! | # | 场景 | 期望 |
//! |---|---|---|
//! | ⑤ | 多个来源都**慢** | 等待是 `max(各来源)` 而不是**相加**（并发扇出） |
//! | ⑥ | 来源慢过**总预算** | 按"拿不到"降级 + 点名"超出热读预算"（预算是查询说了算的上界） |
//! | ⑦ | 多个来源同时 STALE | 报错**顺序确定**（取 `BTreeMap` 键序里的第一个）—— 并发不许改语义 |
//!
//! ④ 是这四条的护栏：把"拿不到"降级为可标记的部分结果**是**设计要的（`architecture §4.2`），
//! 但**顺手把 STALE 也降级**就等于把"可修复的落后"当成永久缺失 —— 静默少数据，正是
//! `§63.3` / `§67` 反复抓到的那个错误形状。STALE 与失败在代码里**本来就分处两个通道**
//! （`ShardRead.stale` vs `Err`），本用例把这条边界钉住。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_chunk::{ChunkKey, ChunkStore, ChunkStoreConfig, MemoryLedger, SealPolicy};
use yuntun_model::error::LakeError;
use yuntun_model::ops::CreateTableRequest;
use yuntun_query::{LocalCatalog, PartialPolicy, QueryEngine};
use yuntun_store::{create_store, ShardId, ShardRead, ShardReader, ShardTier, StoreConfig};

/// 全限定表名：必须与 `ChunkKey.shard.table` 一致，否则热读按表名过滤会扑空
const TABLE: &str = "public.partial";
const SHARD: &str = "default";
const WINDOW: &str = "2026-09-23T10:00";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
}

fn batch(vals: &[i64]) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
}

fn shard_id() -> ShardId {
    ShardId::new(TABLE, SHARD, WINDOW)
}

// ---------------------------------------------------------------- 两个假实例

/// **读不到**的实例：连接拒绝 / 超时 / 内部错误 —— 反正"没能答上来"。
///
/// 失败点选在 `shards_of`：远端实现的第一次调用就是建连（`GrpcShardFetch` 亦然），
/// 所以"节点挂了"最早就死在这里。
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

/// **永远 STALE** 的实例：它**答上来了**，答的是"我的水位超前于你的 manifest"
/// （水位取 `u64::MAX` ⇒ 任何 manifest 版本都追不上 ⇒ 永久 STALE）。
#[derive(Debug)]
struct AlwaysStale;

#[async_trait]
impl ShardReader for AlwaysStale {
    fn tier(&self) -> ShardTier {
        ShardTier::Memory
    }
    fn version(&self) -> u64 {
        0
    }
    async fn shards_of(&self, _table: &str) -> Result<Vec<ShardId>, LakeError> {
        Ok(vec![shard_id()])
    }
    async fn read_shard(&self, _id: &ShardId, known: u64) -> Result<ShardRead, LakeError> {
        Ok(ShardRead::empty(u64::MAX, known))
    }
    async fn watermark(&self, known: u64) -> Result<ShardRead, LakeError> {
        Ok(ShardRead::empty(u64::MAX, known))
    }
}

/// **慢但会答**的来源：`delay` 之后返回 `rows` 行。
///
/// 覆写 `read_table_excluding`（**查询路径走的正是它**；`read_table` 只是它 `exclude = []` 的
/// 特例）：这里要的是一个**可控的等待**，用来观察"多个来源的等待是**相加**还是**取最大**"。
#[derive(Debug)]
struct SlowButAnswers {
    delay: Duration,
    rows: Vec<i64>,
}

#[async_trait]
impl ShardReader for SlowButAnswers {
    fn tier(&self) -> ShardTier {
        ShardTier::Memory
    }
    fn version(&self) -> u64 {
        0
    }
    async fn shards_of(&self, _table: &str) -> Result<Vec<ShardId>, LakeError> {
        Ok(vec![shard_id()])
    }
    async fn read_shard(&self, _id: &ShardId, known: u64) -> Result<ShardRead, LakeError> {
        Ok(ShardRead::empty(known, known))
    }
    async fn watermark(&self, known: u64) -> Result<ShardRead, LakeError> {
        Ok(ShardRead::empty(known, known))
    }
    async fn read_table_excluding(
        &self,
        _table: &str,
        known: u64,
        _exclude: &[String],
    ) -> Result<ShardRead, LakeError> {
        tokio::time::sleep(self.delay).await;
        Ok(ShardRead {
            batches: vec![batch(&self.rows)],
            flushed_watermark: known,
            stale: false,
        })
    }
}

// ---------------------------------------------------------------- 夹具

/// 造一个"实例"的 chunk store（宽松策略：数据**留在热副本里**，被测的是读侧）。
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
            name: "partial".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: Default::default(),
        })
        .await
        .unwrap();
}

/// 装配"若干来源 + 引擎"（策略可指定）。目录守卫由调用方持有。
async fn engine_with(
    policy: PartialPolicy,
    sources: Vec<(&str, Arc<dyn ShardReader>)>,
) -> QueryEngine {
    engine_with_budget(policy, sources, yuntun_query::DEFAULT_HOT_READ_BUDGET).await
}

/// 同 [`engine_with`]，但显式给**整段热读的总预算**（`§88`）。
async fn engine_with_budget(
    policy: PartialPolicy,
    sources: Vec<(&str, Arc<dyn ShardReader>)>,
    budget: Duration,
) -> QueryEngine {
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;

    let cache = Arc::new(LocalCatalog::new());
    cache.set_catalog_ops(catalog.clone());
    for (id, reader) in sources {
        // 名录是唯一真相（T12.3）：来源必须登记，否则一次刷新就会把它摘掉
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

    QueryEngine::new(create_store(&StoreConfig::Memory).unwrap(), cache)
        .with_partial_policy(policy)
        .with_hot_read_budget(budget)
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

const QUERY: &str = "SELECT a FROM yuntun.public.partial ORDER BY a";

// ---------------------------------------------------------------- ① 降级

/// 一个来源读不到 ⇒ **不整体失败**，而是返回可用部分 + **明确标记缺了谁**。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unreachable_source_degrades_to_a_marked_partial_result() {
    let dir = yuntun_testkit::TestDir::tmpfs("partial-degrade");
    let healthy = instance(&dir, "inst-a");
    write(&healthy, &[1, 2, 3]);

    let engine = engine_with(
        PartialPolicy::Allow,
        vec![
            ("inst-a", healthy.clone() as Arc<dyn ShardReader>),
            ("inst-b", Arc::new(Unreachable) as Arc<dyn ShardReader>),
        ],
    )
    .await;

    let out = engine
        .sql_partial(QUERY)
        .await
        .expect("默认策略下应当**降级**，而不是整体失败");

    // 可用部分照常给出
    assert_eq!(values(&out.batches), vec![1, 2, 3], "健康来源的数据必须完整返回");
    // 但**必须**标记为部分，并点名缺了谁、为什么
    assert!(out.is_partial(), "缺了来源却没标记 = 静默少数据");
    let missing = out.partial.missing();
    assert_eq!(missing.len(), 1, "只该缺 inst-b：{missing:?}");
    assert_eq!(missing[0].instance, "inst-b");
    assert_eq!(missing[0].table, TABLE);
    assert!(
        missing[0].reason.contains("connection refused"),
        "原因要能排障：{}",
        missing[0].reason
    );
    // 摘要给人和日志用，至少要能看出"谁缺了"
    let d = out.partial.describe();
    assert!(d.contains("inst-b") && d.contains("部分"), "{d}");
}

// ---------------------------------------------------------------- ② 拒绝

/// `partial = reject` ⇒ **当场失败**，且错误里点名缺了谁（而不是给一份"看起来完整"的结果）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reject_policy_fails_loudly_and_names_the_missing_source() {
    let dir = yuntun_testkit::TestDir::tmpfs("partial-reject");
    let healthy = instance(&dir, "inst-a");
    write(&healthy, &[1, 2, 3]);

    let engine = engine_with(
        PartialPolicy::Reject,
        vec![
            ("inst-a", healthy.clone() as Arc<dyn ShardReader>),
            ("inst-b", Arc::new(Unreachable) as Arc<dyn ShardReader>),
        ],
    )
    .await;

    let e = engine
        .sql_partial(QUERY)
        .await
        .expect_err("reject 策略下必须失败");
    let msg = e.to_string();
    assert!(msg.contains("inst-b"), "错误要点名缺失来源：{msg}");
    assert!(msg.contains("拒绝部分结果"), "错误要说清是为什么失败：{msg}");
}

// ---------------------------------------------------------------- ③ 不许误标

/// 所有来源都读得到 ⇒ **不许**标 partial（假警报同样是错：它会让调用方不敢信任何结果）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complete_reads_are_not_marked_partial() {
    let dir_a = yuntun_testkit::TestDir::tmpfs("partial-full-a");
    let dir_b = yuntun_testkit::TestDir::tmpfs("partial-full-b");
    let a = instance(&dir_a, "inst-a");
    let b = instance(&dir_b, "inst-b");
    write(&a, &[1, 2]);
    write(&b, &[3, 4]);

    let engine = engine_with(
        PartialPolicy::Allow,
        vec![
            ("inst-a", a.clone() as Arc<dyn ShardReader>),
            ("inst-b", b.clone() as Arc<dyn ShardReader>),
        ],
    )
    .await;

    let out = engine.sql_partial(QUERY).await.expect("两个来源都健康");
    assert_eq!(values(&out.batches), vec![1, 2, 3, 4]);
    assert!(!out.is_partial(), "完整结果被标成部分：{}", out.partial.describe());
}

// ---------------------------------------------------------------- ④ 护栏：STALE 不降级

/// **STALE 绝不是 partial**：它意味着"成员答上来了、只是我们的目录落后" ——
/// 必须刷新重试，追不上就**响亮失败**，**不得**降级成一份带标记的部分结果。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_is_retried_then_fails_loudly_never_downgraded_to_partial() {
    let dir = yuntun_testkit::TestDir::tmpfs("partial-stale");
    let healthy = instance(&dir, "inst-a");
    write(&healthy, &[1, 2, 3]);

    let engine = engine_with(
        PartialPolicy::Allow, // 即使策略是 allow，STALE 也不许降级
        vec![
            ("inst-a", healthy.clone() as Arc<dyn ShardReader>),
            ("inst-b", Arc::new(AlwaysStale) as Arc<dyn ShardReader>),
        ],
    )
    .await;

    let e = engine
        .sql_partial(QUERY)
        .await
        .expect_err("永久 STALE ⇒ 必须失败，不能返回不完整结果");
    let msg = e.to_string();
    assert!(msg.contains("STALE"), "错误要说清是 STALE（可重试信号）：{msg}");
    assert!(
        msg.contains("不返回不完整结果"),
        "STALE 追不上时的语义是**失败**，不是部分结果：{msg}"
    );
    assert!(
        !msg.contains("拒绝部分结果"),
        "STALE 被当成了 partial（两条通道被混为一谈）：{msg}"
    );
}

// ---------------------------------------------------------------- ⑤ 并发扇出

/// **多个慢来源的等待必须取最大，而不是相加**（`§88`）。
///
/// 串行 `for` 的时代，3 个各 200ms 的来源要让查询等 600ms —— 而它们之间**毫无依赖**。
/// 这条用例直接把"扇出的形状"钉在时间上：串行版本必然失败（3×200ms > 400ms 阈值）。
///
/// 阈值取得宽（200ms 的并行实现 vs 400ms 的界）：它要的是**区分相加与取最大**，
/// 不是测某台机器的快。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_sources_wait_in_parallel_not_in_sequence() {
    let slow = |rows: Vec<i64>| -> Arc<dyn ShardReader> {
        Arc::new(SlowButAnswers {
            delay: Duration::from_millis(200),
            rows,
        })
    };

    let engine = engine_with_budget(
        PartialPolicy::Allow,
        vec![
            ("inst-a", slow(vec![1])),
            ("inst-b", slow(vec![2])),
            ("inst-c", slow(vec![3])),
        ],
        Duration::from_secs(5), // 预算不参与：这条只测"并行 vs 串行"
    )
    .await;

    let t0 = std::time::Instant::now();
    let out = engine.sql_partial(QUERY).await.expect("三个来源最终都会答");
    let elapsed = t0.elapsed();

    assert_eq!(values(&out.batches), vec![1, 2, 3], "并发不许改变结果");
    assert!(!out.is_partial(), "都答上来了，不该标 partial");
    assert!(
        elapsed < Duration::from_millis(400),
        "等待应当是 max(各来源) 而不是相加：3 × 200ms 串行会到 600ms，实测 {elapsed:?}"
    );
}

// ---------------------------------------------------------------- ⑥ 总预算

/// **来源慢过总预算** ⇒ 按"拿不到"降级 + 点名"超出热读预算"（`§88`）。
///
/// 并发只把 `N ×` 变成 `max(·)`；**单个**来源仍可能慢到自己的传输超时。预算就是
/// `max(·)` 之上的硬上界 —— 由**查询**说了算，而不是由传输层或对端决定。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn budget_above_which_sources_degrade_to_partial() {
    let slow = |rows: Vec<i64>| -> Arc<dyn ShardReader> {
        Arc::new(SlowButAnswers {
            delay: Duration::from_secs(2), // 远慢于预算
            rows,
        })
    };

    let engine = engine_with_budget(
        PartialPolicy::Allow,
        vec![
            ("inst-a", slow(vec![1])),
            ("inst-b", slow(vec![2])),
        ],
        Duration::from_millis(150),
    )
    .await;

    let t0 = std::time::Instant::now();
    let out = engine
        .sql_partial(QUERY)
        .await
        .expect("预算耗尽 ⇒ 降级（Allow），不是整体失败");
    let elapsed = t0.elapsed();

    assert!(
        elapsed < Duration::from_secs(1),
        "预算必须真的截断等待（来源要 2s，预算 150ms）：实测 {elapsed:?}"
    );
    assert!(out.is_partial(), "超预算的来源是“拿不到” ⇒ 必须标记");
    let missing = out.partial.missing();
    assert_eq!(missing.len(), 2, "两个来源都超预算：{missing:?}");
    assert!(
        missing.iter().all(|m| m.reason.contains("热读预算")),
        "原因要说清是预算截断，而不是“连接失败”：{missing:?}"
    );
}

// ---------------------------------------------------------------- ⑦ 并发下的语义不变

/// **并发不许改语义**：多个来源同时 STALE 时，报错取 `BTreeMap` 键序里的**第一个**
/// （与串行时代逐字一致）—— 否则同一个查询在两次运行里会报出不同的来源名。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_error_is_deterministic_under_concurrency() {
    let engine = engine_with(
        PartialPolicy::Allow,
        vec![
            ("inst-a", Arc::new(AlwaysStale) as Arc<dyn ShardReader>),
            ("inst-b", Arc::new(AlwaysStale) as Arc<dyn ShardReader>),
        ],
    )
    .await;

    let e = engine
        .sql_partial(QUERY)
        .await
        .expect_err("永久 STALE ⇒ 失败");
    let msg = e.to_string();
    assert!(msg.contains("STALE"), "{msg}");
    assert!(
        msg.contains("inst-a") && !msg.contains("inst-b"),
        "两个来源同时 STALE 时，错误必须**确定地**取键序第一个（inst-a）：{msg}"
    );
}
