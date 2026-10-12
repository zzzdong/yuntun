//! `§173`：**可分解聚合的下推**（G.2 / 归档 `S5-3`）—— 四条判据。
//!
//! | 判据 | 为什么它是判据而不是"顺手测一下" |
//! |---|---|
//! | **对拍**（下推 vs 原路，逐位） | 聚合算错的形态是"**看起来很正常**的数字偏小/偏大"（少一个来源、NULL 当 0、合并顺序变了）—— 只有两条路跑同一批数据逐位比才抓得住 |
//! | **网络量下降一个量级** | 这是这一刀存在的**理由**（`ROADMAP` 的 N4 验收）：没把"行"换成"标量"就等于没做 |
//! | **降级语义与行路径一致** | 拿不到来源时聚合**更危险**（计数偏小却像个正常答案）：必须按同一套 partial 策略标记/拒绝 |
//! | **STALE 必须响亮失败** | 拿旧 manifest 去拉 ⇒ 对方可能已放弃副本，聚合在那个窗口里会**静默少算** |
//!
//! 夹具刻意用**真数据**：真 `ChunkStore`（热）+ 真 parquet 文件（冷）——
//! 这样"冷热两半合起来对不对"才是真的被验过，而不是拿假批次自说自话。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_chunk::{ChunkKey, ChunkStore, ChunkStoreConfig, MemoryLedger, SealPolicy};
use yuntun_model::agg::{AggQuery, AggStates, PartialAgg};
use yuntun_model::error::LakeError;
use yuntun_model::ops::CreateTableRequest;
use yuntun_query::{LocalCatalog, QueryEngine};
use yuntun_store::{ColdReader,
    RemoteShard, ShardAggs, ShardFetch, ShardId, ShardRead, ShardReader, ShardTier, create_store,
    StoreConfig,
};

const TABLE: &str = "public.agg";
const SHARD: &str = "s0";
const WINDOW: &str = "2026-10-11T10:00";

// ---------------------------------------------------------------- 夹具

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, true),
        // `§177`：字符串列 —— 看板里最常见的分组键是"主机名 / 租户名"
        Field::new("s", DataType::Utf8, true),
    ]))
}

/// 带 NULL 的批次（NULL 语义是聚合最容易错的地方之一）。
///
/// `s` 由 `v` **派生**（`NULL ⇒ NULL`）：这样一条用例的数据同时覆盖整数分组键、
/// 字符串分组键与"NULL 是一个组"这三条，而不必为每种都另起一套夹具。
fn batch(vals: &[Option<i64>]) -> RecordBatch {
    let s: Vec<Option<String>> = vals.iter().map(|v| v.map(|x| format!("h{x}"))).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vals.to_vec())),
            Arc::new(StringArray::from(s)),
        ],
    )
    .unwrap()
}

/// 一个"实例"的热数据（数据留在 chunk 里：本用例测的是读侧）。
fn instance(dir: &yuntun_testkit::TestDir, id: &str, vals: &[Option<i64>]) -> Arc<ChunkStore> {
    let store = ChunkStore::new(
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
            instance_id: id.into(),
            wal_segment: 0,
        },
        MemoryLedger::new("chunk", 1 << 24),
    );
    if !vals.is_empty() {
        store
            .append(
                ChunkKey::new(ShardId::new(TABLE, SHARD, WINDOW), 0),
                1,
                schema(),
                0,
                vec![batch(vals)],
                0,
            )
            .unwrap();
    }
    store
}

/// 写一个**真 parquet 文件**并提交（冷数据）。
async fn commit_cold(
    store: &Arc<dyn object_store::ObjectStore>,
    catalog: &Arc<dyn CatalogOps>,
    id: &str,
    vals: &[Option<i64>],
) {
    let bid = format!("cold-{id}");
    let (path, size, rows) = yuntun_format::write_batch(
        store,
        TABLE,
        SHARD,
        WINDOW,
        &bid,
        &batch(vals),
        yuntun_format::DataFormat::Parquet,
    )
    .await
    .unwrap();
    catalog
        .commit_files(yuntun_model::ops::CommitFilesRequest {
            table: TABLE.into(),
            batch_id: bid.clone(),
            client_request_id: None,
            client_request_ids: vec![],
            shard: SHARD.into(),
            time_window: WINDOW.into(),
            files: vec![yuntun_model::meta::FileManifest {
                file_path: path,
                batch_id: bid,
                file_size: size,
                row_count: rows,
                table: TABLE.into(),
                shard: SHARD.into(),
                time_window: WINDOW.into(),
                source_instance: "cold".into(),
                ..Default::default()
            }],
            schema_version: 1,
            row_count: rows,
        })
        .await
        .unwrap();
}

async fn create_table(catalog: &Arc<dyn CatalogOps>) {
    catalog
        .create_table(CreateTableRequest {
            name: "agg".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: Default::default(),
        })
        .await
        .unwrap();
}

/// 装配引擎：真 store + 真 catalog + 若干热来源（来源可以是任何 [`ShardReader`]）。
async fn engine(
    store: Arc<dyn object_store::ObjectStore>,
    catalog: Arc<dyn CatalogOps>,
    sources: Vec<(&str, Arc<dyn ShardReader>)>,
) -> QueryEngine {
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
    QueryEngine::new(store, cache).with_hot_read_budget(Duration::from_secs(5))
}

/// 把 ChunkStore 包一层，**记录"聚合拉取真的发生了"** —— 否则"对拍通过"可能只是原路跑了两遍，
/// 那种绿没有意义。
#[derive(Debug)]
struct CountingReader {
    inner: Arc<ChunkStore>,
    agg_pulls: AtomicUsize,
}

impl CountingReader {
    fn new(inner: Arc<ChunkStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            agg_pulls: AtomicUsize::new(0),
        })
    }

    fn pulls(&self) -> usize {
        self.agg_pulls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ShardReader for CountingReader {
    fn tier(&self) -> ShardTier {
        self.inner.tier()
    }
    fn version(&self) -> u64 {
        self.inner.version()
    }
    async fn shards_of(&self, table: &str) -> Result<Vec<ShardId>, LakeError> {
        self.inner.shards_of(table).await
    }
    async fn read_shard(&self, id: &ShardId, known: u64) -> Result<ShardRead, LakeError> {
        self.inner.read_shard(id, known).await
    }
    async fn read_shard_excluding(
        &self,
        id: &ShardId,
        known: u64,
        exclude: &[String],
    ) -> Result<ShardRead, LakeError> {
        self.inner.read_shard_excluding(id, known, exclude).await
    }
    async fn watermark(&self, known: u64) -> Result<ShardRead, LakeError> {
        self.inner.watermark(known).await
    }
    async fn read_table_aggs(
        &self,
        table: &str,
        known: u64,
        exclude: &[String],
        query: &AggQuery,
    ) -> Result<ShardAggs, LakeError> {
        self.agg_pulls.fetch_add(1, Ordering::SeqCst);
        self.inner
            .read_table_aggs(table, known, exclude, query)
            .await
    }
}


/// 部分状态在网上的**定长表示**有多大（真实实现是几十字节的 proto）。
///
/// 分组时是"每组的标量" ⇒ 组越多越大，但**仍然不是行**（这正是这一刀要测的那件事）。
fn agg_bytes_of(states: &yuntun_model::agg::AggStates) -> usize {
    let n = match states {
        yuntun_model::agg::AggStates::Plain(_) => 1,
        yuntun_model::agg::AggStates::Grouped(g) => g.len(),
    };
    n * std::mem::size_of::<PartialAgg>()
}

/// 一个**假的远端**：`fetch_shard` 按真实编码（arrow IPC）计"上网的字节"，
/// `fetch_shard_aggs` 在**数据旁边**折完只把标量送回来。
///
/// 它模拟的正是 `GrpcShardFetch` + 服务端的形状。
#[derive(Debug)]
struct WireFetch {
    inner: Arc<ChunkStore>,
    row_bytes: AtomicUsize,
    agg_bytes: AtomicUsize,
}

impl WireFetch {
    fn new(inner: Arc<ChunkStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            row_bytes: AtomicUsize::new(0),
            agg_bytes: AtomicUsize::new(0),
        })
    }
}

impl ShardFetch for WireFetch {
    fn fetch_shards<'a>(
        &'a self,
        table: &'a str,
    ) -> BoxFuture<'a, Result<Vec<ShardId>, LakeError>> {
        Box::pin(async move { self.inner.shards_of(table).await })
    }

    fn fetch_shard<'a>(
        &'a self,
        id: &'a ShardId,
        known_manifest_ver: u64,
        known_batch_ids: Vec<String>,
    ) -> BoxFuture<'a, Result<ShardRead, LakeError>> {
        Box::pin(async move {
            let read = self
                .inner
                .read_shard_excluding(id, known_manifest_ver, &known_batch_ids)
                .await?;
            // **上网的字节**：与真实服务端同一份编码（`shardrpc::encode_batch`）
            let mut n = 0usize;
            for b in &read.batches {
                n += yuntun_shardrpc::encode_batch(b)?.len();
            }
            self.row_bytes.fetch_add(n, Ordering::SeqCst);
            Ok(read)
        })
    }

    fn fetch_watermark<'a>(
        &'a self,
        known_manifest_ver: u64,
    ) -> BoxFuture<'a, Result<ShardRead, LakeError>> {
        Box::pin(async move { self.inner.watermark(known_manifest_ver).await })
    }

    /// **服务端那一侧**：在数据旁边折（不过网），只回标量。
    fn fetch_shard_aggs<'a>(
        &'a self,
        table: &'a str,
        known_manifest_ver: u64,
        known_batch_ids: Vec<String>,
        query: AggQuery,
    ) -> BoxFuture<'a, Result<ShardAggs, LakeError>> {
        Box::pin(async move {
            let read = self
                .inner
                .read_table_excluding(table, known_manifest_ver, &known_batch_ids)
                .await?;
            // `§176`/`§177`：谓词与分组**都**在**数据旁边**生效（与真实服务端同一条代码路径）
            let states = yuntun_model::agg::fold_query(&query, &read.batches)?;
            // 上网的字节 = 部分状态那点标量（真实实现是几十字节的 proto）
            self.agg_bytes.fetch_add(
                agg_bytes_of(&states),
                Ordering::SeqCst,
            );
            Ok(ShardAggs {
                states,
                flushed_watermark: read.flushed_watermark,
                stale: read.stale,
            })
        })
    }
}

/// 永远答不上的来源（拿不到）。
#[derive(Debug)]
struct Unreachable;

#[async_trait::async_trait]
impl ShardReader for Unreachable {
    fn tier(&self) -> ShardTier {
        ShardTier::Memory
    }
    fn version(&self) -> u64 {
        0
    }
    async fn shards_of(&self, _table: &str) -> Result<Vec<ShardId>, LakeError> {
        Err(LakeError::Other("连接被拒绝".into()))
    }
    async fn read_shard(&self, _id: &ShardId, _known: u64) -> Result<ShardRead, LakeError> {
        Err(LakeError::Other("连接被拒绝".into()))
    }
    async fn watermark(&self, _known: u64) -> Result<ShardRead, LakeError> {
        Err(LakeError::Other("连接被拒绝".into()))
    }
}

/// 永远 STALE 的来源（水位超前于调用方的 manifest）。
#[derive(Debug)]
struct AlwaysStale;

#[async_trait::async_trait]
impl ShardReader for AlwaysStale {
    fn tier(&self) -> ShardTier {
        ShardTier::Memory
    }
    fn version(&self) -> u64 {
        0
    }
    async fn shards_of(&self, _table: &str) -> Result<Vec<ShardId>, LakeError> {
        Ok(vec![ShardId::new(TABLE, SHARD, WINDOW)])
    }
    async fn read_shard(&self, _id: &ShardId, _known: u64) -> Result<ShardRead, LakeError> {
        Ok(ShardRead {
            batches: Vec::new(),
            flushed_watermark: 99,
            stale: true,
        })
    }
    async fn watermark(&self, _known: u64) -> Result<ShardRead, LakeError> {
        Ok(ShardRead {
            batches: Vec::new(),
            flushed_watermark: 99,
            stale: true,
        })
    }
    async fn read_table_aggs(
        &self,
        _table: &str,
        _known: u64,
        _exclude: &[String],
        query: &AggQuery,
    ) -> Result<ShardAggs, LakeError> {
        Ok(ShardAggs {
            // 形状必须**跟着请求走**（不分组 ⇒ 单位元；分组 ⇒ 没有组）
            states: if query.is_grouped() {
                yuntun_model::agg::AggStates::Grouped(Vec::new())
            } else {
                yuntun_model::agg::AggStates::Plain(
                    query
                        .specs
                        .iter()
                        .map(|s| PartialAgg::empty(s.kind))
                        .collect(),
                )
            },
            flushed_watermark: 99,
            stale: true,
        })
    }
}

// ---------------------------------------------------------------- 判据 ①：对拍

/// 把结果规范化成"**有序的行字符串**"再比。
///
/// 为什么不直接比 `Vec<RecordBatch>`：① 批次的**切分**不在契约里（DataFusion 的 GROUP BY
/// 会一组一批）；② 没有 `ORDER BY` 时行序也不在契约里。这两条都不是"值不同"，
/// 拿它们当判据会让对拍变成抖的 —— 而**值**（含 NULL）才是要逐位比的东西。
fn rows_of(batches: &[RecordBatch]) -> Vec<String> {
    let joined = arrow::compute::concat_batches(&batches[0].schema(), batches).unwrap();
    let mut out: Vec<String> = (0..joined.num_rows())
        .map(|r| {
            (0..joined.num_columns())
                .map(|c| cell(joined.column(c), r))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    out.sort();
    out
}

fn cell(col: &arrow::array::ArrayRef, r: usize) -> String {
    if col.is_null(r) {
        return "NULL".to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return a.value(r).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
        return a.value(r).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow::array::Float64Array>() {
        return format!("{:?}", a.value(r));
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
        return a.value(r).to_string();
    }
    panic!("对拍用了没覆盖的列类型：{:?}", col.data_type());
}

/// **下推路径与行路径逐位相同**（`ROADMAP` N4 的硬要求）。
///
/// 覆盖：冷 + 热合并、`count(*)` / `count(col)` / `sum` / `min` / `max`、NULL 语义、空来源，
/// 以及**必须退回原路**的形状（翻译不出来的谓词 / `avg` / 形状对不上的 `GROUP BY` / 表达式）——
/// 退回的那些也必须逐位相同（退错了同样是错），而且**不许**偷偷打聚合拉取。
#[tokio::test]
async fn pushed_down_aggregates_match_the_row_path_bit_for_bit() {
    let dir = yuntun_testkit::TestDir::tmpfs("agg-parity");
    let store: Arc<dyn object_store::ObjectStore> = create_store(&StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;
    // 冷：一个真 parquet 文件（含 NULL）；热：两个真 chunk + 一个空来源
    commit_cold(&store, &catalog, "1", &[Some(10), Some(20), None]).await;
    let a = CountingReader::new(instance(&dir, "inst-a", &[Some(1), None, Some(3)]));
    let b = CountingReader::new(instance(&dir, "inst-b", &[Some(-5), Some(7)]));
    let empty = CountingReader::new(instance(&dir, "inst-c", &[]));

    let sources: Vec<(&str, Arc<dyn ShardReader>)> = vec![
        ("inst-a", a.clone()),
        ("inst-b", b.clone()),
        ("inst-c", empty.clone()),
    ];
    let subject = engine(store.clone(), catalog.clone(), sources.clone()).await;
    // 参照：同一批数据、**关掉下推**（行路径 = DataFusion 自己算）
    let reference = engine(store.clone(), catalog.clone(), sources)
        .await
        .with_agg_pushdown(false);

    let cases = [
        ("SELECT count(*) FROM agg", true),
        ("SELECT count(v) FROM agg", true),
        ("SELECT sum(v) FROM agg", true),
        ("SELECT min(v), max(v) FROM agg", true),
        ("SELECT count(v), sum(v), min(v), max(v) FROM agg", true),
        // `§176`：带 `WHERE` 的形状（谓词在**数据所在处**生效，网络里仍只走标量）
        ("SELECT count(*) FROM agg WHERE v > 0", true),
        ("SELECT count(v), sum(v) FROM agg WHERE v = 5", true),
        // NULL 的三值逻辑：两边必须一致（`v != 5` 在 NULL 行上不成立）
        ("SELECT count(*) FROM agg WHERE v != 5", true),
        ("SELECT count(*) FROM agg WHERE v IS NULL", true),
        ("SELECT sum(v) FROM agg WHERE v IS NOT NULL", true),
        ("SELECT count(*) FROM agg WHERE v > 1 AND v < 10", true),
        ("SELECT count(*) FROM agg WHERE v < -1 OR v > 5", true),
        // 字面量在左 ⇒ 比较符必须翻转（不翻就是静默错）
        ("SELECT count(*) FROM agg WHERE 3 <= v", true),
        // `§177`：**分组**也能下推（每组的标量过网，不是行）
        ("SELECT v, count(*) FROM agg GROUP BY v", true),
        ("SELECT v, count(*), sum(v), min(v) FROM agg GROUP BY v", true),
        // 字符串分组键（看板里最常见的"按主机/租户分组"）
        ("SELECT s, count(*) FROM agg GROUP BY s", true),
        // 多列键 + "键在前、聚合在后"
        ("SELECT v, s, count(*) FROM agg GROUP BY v, s", true),
        // 谓词与分组一起：先筛行、再分组
        ("SELECT v, count(*) FROM agg WHERE v > 0 GROUP BY v", true),
        // 必须退回原路：翻译不出来的谓词、avg，以及**形状对不上**的分组
        // 注：`v LIKE 'a%'` 是**非法的**（Int64 对字符串模式，DataFusion 直接报类型错）
        // ⇒ 换几个**合法但翻译不出来**的形状
        ("SELECT count(*) FROM agg WHERE v BETWEEN 1 AND 3", false),
        ("SELECT count(*) FROM agg WHERE v IN (1, 5)", false),
        ("SELECT count(*) FROM agg WHERE v > 1.5", false),
        ("SELECT count(*) FROM agg WHERE abs(v) > 1", false),
        ("SELECT count(*) FROM agg WHERE NOT (v > 1)", false),
        // 分组里"不能下推"的：投影顺序不符、表达式键、ORDER BY、HAVING
        ("SELECT count(*), v FROM agg GROUP BY v", false),
        ("SELECT v + 1, count(*) FROM agg GROUP BY v + 1", false),
        ("SELECT v, count(*) FROM agg GROUP BY v ORDER BY v", false),
        ("SELECT v, count(*) FROM agg GROUP BY v HAVING count(*) > 1", false),
        ("SELECT avg(v) FROM agg", false),
        ("SELECT count(*) + 1 FROM agg", false),
    ];
    for (sql, pushable) in cases {
        let before = a.pulls();
        let got = subject
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: 下推路径失败：{e}"));
        let want = reference
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: 参照路径失败：{e}"));
        // **列名与类型也要一致**：下推路径的结果是"在 DataFusion 的冷结果上改值"，
        // schema 若被我们动过，客户端拿到的列名/类型就变了（那也是一种静默破坏）。
        assert_eq!(got[0].schema(), want[0].schema(), "{sql}: schema 必须一致");
        assert_eq!(rows_of(&got), rows_of(&want), "{sql}: 值必须逐位相同");
        let after = a.pulls();
        if pushable {
            assert!(after > before, "{sql}: 该走聚合下推却没走（拉取次数没变）");
        } else {
            assert_eq!(after, before, "{sql}: 不该下推却打了聚合拉取");
        }
    }
}

// ---------------------------------------------------------------- 判据 ②：网络量

/// **下推把"行"换成了"标量"**（`ROADMAP` N4 的验收：下降一个量级）。
///
/// 量法用**真实的编码函数**（`shardrpc::encode_batch`，服务端实际发的那种字节），
/// 而不是"行数 × 8"这种估算。
#[tokio::test]
async fn pushed_down_pull_moves_scalars_not_rows() {
    let dir = yuntun_testkit::TestDir::tmpfs("agg-wire");
    let store: Arc<dyn object_store::ObjectStore> = create_store(&StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;

    // 3 万行整数（够大：行路径的 IPC 是几百 KB 量级）
    let rows: Vec<Option<i64>> = (0..30_000).map(|i| Some((i % 1000) as i64)).collect();
    let wire = WireFetch::new(instance(&dir, "inst-r", &rows));
    let source: Arc<dyn ShardReader> = Arc::new(RemoteShard::new(wire.clone()));

    let subject = engine(store.clone(), catalog.clone(), vec![("inst-r", source.clone())]).await;
    let reference = engine(store.clone(), catalog.clone(), vec![("inst-r", source)])
        .await
        .with_agg_pushdown(false);

    let sql = "SELECT count(*), sum(v) FROM agg";
    let pushed = subject.sql(sql).await.expect("下推路径");
    let rowed = reference.sql(sql).await.expect("行路径");
    assert_eq!(pushed, rowed, "省字节的前提是答案一样");

    let agg = wire.agg_bytes.load(Ordering::SeqCst);
    let rows_bytes = wire.row_bytes.load(Ordering::SeqCst);
    assert!(agg > 0, "聚合路径必须真的过了一次响应（否则这个对比是假的）");
    assert!(rows_bytes > 0, "行路径必须真的传过字节（否则这个对比是假的）");
    eprintln!(
        "【§173 实测】无谓词：行 {rows_bytes} 字节 vs 聚合 {agg} 字节（{:.0}×）",
        rows_bytes as f64 / agg.max(1) as f64
    );
    assert!(
        rows_bytes >= agg * 10,
        "网络量必须下降一个量级：行路径 {rows_bytes} 字节 vs 聚合路径 {agg} 字节"
    );

    // `§176`：**带 `WHERE` 的看板查询**同样只走标量 —— 这一刀的全部意义就在这里
    //（原路的 `WHERE` 是在协调者**拉到全部行之后**才生效的）。
    let (row0, agg0) = (
        wire.row_bytes.load(Ordering::SeqCst),
        wire.agg_bytes.load(Ordering::SeqCst),
    );
    let filtered = "SELECT count(*), sum(v) FROM agg WHERE v > 500";
    let pushed = subject.sql(filtered).await.expect("带谓词的下推路径");
    let rowed = reference.sql(filtered).await.expect("带谓词的行路径");
    assert_eq!(pushed, rowed, "带谓词的两条路也必须给出同一个答案");
    let row_delta = wire.row_bytes.load(Ordering::SeqCst) - row0;
    let agg_delta = wire.agg_bytes.load(Ordering::SeqCst) - agg0;
    eprintln!(
        "【§176 实测】带 WHERE：行 {row_delta} 字节 vs 聚合 {agg_delta} 字节（{:.0}×）",
        row_delta as f64 / agg_delta.max(1) as f64
    );
    assert!(
        row_delta >= agg_delta * 10,
        "带谓词时网络量同样要下降一个量级：行路径 {row_delta} 字节 vs 聚合路径 {agg_delta} 字节"
    );
}

// ---------------------------------------------------------------- 判据 ③：降级语义

/// 拿不到来源时，聚合路径的**降级语义与行路径一致**：`Allow` ⇒ 标记 partial 并点名；
/// `Reject` ⇒ 当场失败。而且降级后的计数是**下界**（不许看起来像个正常答案）。
#[tokio::test]
async fn missing_sources_degrade_the_same_way_for_aggregates() {
    use yuntun_query::PartialPolicy;
    let dir = yuntun_testkit::TestDir::tmpfs("agg-partial");
    let store: Arc<dyn object_store::ObjectStore> = create_store(&StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;

    let ok: Arc<dyn ShardReader> =
        CountingReader::new(instance(&dir, "inst-ok", &[Some(1), Some(2), Some(3)]));
    let bad: Arc<dyn ShardReader> = Arc::new(Unreachable);
    let sources = vec![("inst-ok", ok), ("inst-bad", bad)];

    // Allow：降级 + 标记（点名缺了谁）
    let allow = engine(store.clone(), catalog.clone(), sources.clone())
        .await
        .with_partial_policy(PartialPolicy::Allow);
    let out = allow
        .sql_partial("SELECT count(*) FROM agg")
        .await
        .expect("Allow 策略下应当返回带标记的部分结果");
    assert!(out.is_partial(), "缺了来源却没标记 = 静默少数据");
    let missing = out.partial.missing();
    assert_eq!(missing.len(), 1, "只该缺 inst-bad：{missing:?}");
    assert_eq!(missing[0].instance, "inst-bad");
    // 计数 = 只算到拿得到的那部分（3 行）——**下界**，这正是 partial 标记要说的
    let counted = out.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(counted, 3, "降级后的计数只能是拿得到的那部分");

    // Reject：当场失败（不是返回一个偏小的数字）
    let reject = engine(store.clone(), catalog.clone(), sources)
        .await
        .with_partial_policy(PartialPolicy::Reject);
    let e = reject
        .sql_partial("SELECT count(*) FROM agg")
        .await
        .expect_err("Reject 策略下应当失败");
    assert!(
        e.to_string().contains("inst-bad"),
        "拒绝时要点名缺了谁：{e}"
    );
}

// ---------------------------------------------------------------- 判据 ④：STALE

/// **STALE 绝不进答案**（聚合比行更怕它：少算的来源会让计数偏小而看起来正常）。
///
/// 语义与行路径逐字相同：刷新后重试，重试耗尽 ⇒ **响亮失败**（不降级成 partial）。
#[tokio::test]
async fn stale_sources_never_leak_into_a_pushed_down_answer() {
    let dir = yuntun_testkit::TestDir::tmpfs("agg-stale");
    let store: Arc<dyn object_store::ObjectStore> = create_store(&StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;
    let ok: Arc<dyn ShardReader> =
        CountingReader::new(instance(&dir, "inst-ok", &[Some(1), Some(2)]));
    let stale: Arc<dyn ShardReader> = Arc::new(AlwaysStale);

    let e = engine(
        store,
        catalog,
        vec![("inst-ok", ok), ("inst-stale", stale)],
    )
    .await
    .sql_partial("SELECT count(*) FROM agg")
    .await
    .expect_err("STALE 必须失败（刷新重试上限后照样失败）");
    let msg = e.to_string();
    assert!(
        msg.contains("stale") && msg.contains("inst-stale"),
        "失败必须点名 STALE 与实例（否则用户看到的是莫名失败）：{msg}"
    );
}

// ---------------------------------------------------------------- 判据 ⑤：冷文件被分出去（`§182`）

/// 一个**真的会折**的冷折叠实现（与 `RemoteCold` 同一条代码路径：`query::cold`），
/// 但可以**被打断**（用来验"拿不到就回退本地读"）。
#[derive(Debug)]
struct FoldingCold {
    store: Arc<dyn object_store::ObjectStore>,
    fail: bool,
    calls: AtomicUsize,
}

impl FoldingCold {
    /// 被调了几次（判据用：证明**真的分出去折了**，而不是本地算了两遍）
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn new(store: Arc<dyn object_store::ObjectStore>, fail: bool) -> Arc<Self> {
        Arc::new(Self {
            store,
            fail,
            calls: AtomicUsize::new(0),
        })
    }
}

impl ColdReader for FoldingCold {
    fn fold(
        &self,
        req: yuntun_store::ColdReadRequest,
    ) -> BoxFuture<'_, Result<AggStates, LakeError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(LakeError::Other("注入的冷折叠失败".into()));
            }
            // 与真实远端**同一条代码路径**（协议/服务端那侧由 `cold_fold_rpc.rs` 另验）
            yuntun_query::cold::fold_cold_files(
                self.store.clone(),
                &req.table,
                req.schema,
                req.files,
                req.deletions,
                &req.query,
            )
            .await
        })
    }
}

/// 一个**能接冷折叠**的热读器：热的一半照旧（`ChunkStore`），冷的一半透出 `as_cold()`。
#[derive(Debug)]
struct FoldingReader {
    inner: Arc<ChunkStore>,
    cold: Option<Arc<dyn ColdReader>>,
}

#[async_trait::async_trait]
impl ShardReader for FoldingReader {
    fn tier(&self) -> ShardTier {
        self.inner.tier()
    }
    fn version(&self) -> u64 {
        self.inner.version()
    }
    async fn shards_of(&self, table: &str) -> Result<Vec<ShardId>, LakeError> {
        self.inner.shards_of(table).await
    }
    async fn read_shard(&self, id: &ShardId, known: u64) -> Result<ShardRead, LakeError> {
        self.inner.read_shard(id, known).await
    }
    async fn read_shard_excluding(
        &self,
        id: &ShardId,
        known: u64,
        exclude: &[String],
    ) -> Result<ShardRead, LakeError> {
        self.inner.read_shard_excluding(id, known, exclude).await
    }
    async fn read_table_excluding(
        &self,
        table: &str,
        known: u64,
        exclude: &[String],
    ) -> Result<ShardRead, LakeError> {
        self.inner
            .read_table_excluding(table, known, exclude)
            .await
    }
    async fn watermark(&self, known: u64) -> Result<ShardRead, LakeError> {
        self.inner.watermark(known).await
    }
    async fn read_table_aggs(
        &self,
        table: &str,
        known: u64,
        exclude: &[String],
        query: &AggQuery,
    ) -> Result<ShardAggs, LakeError> {
        self.inner.read_table_aggs(table, known, exclude, query).await
    }
    fn as_cold(&self) -> Option<Arc<dyn ColdReader>> {
        self.cold.clone()
    }
}

/// `§182`：**冷文件被分给各节点折**，而答案必须与原路**逐位相同**。
///
/// 三条形状各跑一遍（不分组 / 带谓词 / 分组）—— 它们走的是同一条"分配 → 折 → 合并"链，
/// 但合并的形态不同（并进那一行 / 按键并入每一组），漏一种就是漏一类错。
#[tokio::test]
async fn cold_files_are_shared_out_and_the_answer_is_unchanged() {
    let dir = yuntun_testkit::TestDir::tmpfs("agg-cold-share");
    let store: Arc<dyn object_store::ObjectStore> = create_store(&StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;
    // 两个冷文件（分配要把它们切开）
    commit_cold(&store, &catalog, "s1", &[Some(1), Some(2)]).await;
    commit_cold(&store, &catalog, "s2", &[Some(10), Some(20)]).await;

    // 两侧的**热数据必须相同**（否则差的那一行分不清是热数据还是冷折叠的错）——
    // 差别只在"冷的那一半由谁折"。
    let a = instance(&dir, "inst-a", &[Some(5)]);
    let b = instance(&dir, "inst-b", &[]);
    let a_ref = instance(&dir, "inst-a", &[Some(5)]);
    let b_ref = instance(&dir, "inst-b", &[]);
    let cold_a = FoldingCold::new(store.clone(), false);
    let cold_b = FoldingCold::new(store.clone(), false);
    let reference = engine(
        store.clone(),
        catalog.clone(),
        vec![
            ("inst-a", a_ref as Arc<dyn ShardReader>),
            ("inst-b", b_ref as Arc<dyn ShardReader>),
        ],
    )
    .await;
    let engine = engine(
        store.clone(),
        catalog.clone(),
        vec![
            (
                "inst-a",
                Arc::new(FoldingReader {
                    inner: a,
                    cold: Some(cold_a.clone() as Arc<dyn ColdReader>),
                }) as Arc<dyn ShardReader>,
            ),
            (
                "inst-b",
                Arc::new(FoldingReader {
                    inner: b,
                    cold: Some(cold_b.clone() as Arc<dyn ColdReader>),
                }) as Arc<dyn ShardReader>,
            ),
        ],
    )
    .await;

    for sql in [
        "SELECT count(*), sum(v) FROM agg",
        "SELECT count(*) FROM agg WHERE v > 3",
        "SELECT v, count(*) FROM agg GROUP BY v",
    ] {
        let before: usize = [cold_a.calls(), cold_b.calls()].into_iter().sum();
        let got = engine.sql(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        let want = reference
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(got[0].schema(), want[0].schema(), "{sql}: schema");
        assert_eq!(rows_of(&got), rows_of(&want), "{sql}: 值必须逐位相同");
        let after: usize = [cold_a.calls(), cold_b.calls()].into_iter().sum();
        assert!(
            after > before,
            "{sql}: 冷折叠一次都没被调用 ⇒ 这条绿只是「本地算了两遍」（§173 的同一条纪律）"
        );
    }
}

/// `§182`：**接不上的节点 ⇒ 回退本地读**，不是降级。
///
/// 为什么必须是"回退"而不是 partial：那份数据本来就在**共享存储**上、协调者自己就能读，
/// 把它标成 partial 等于告诉用户"结果可能少"—— 而真相只是"换个人读"。
#[tokio::test]
async fn a_node_that_cannot_fold_falls_back_to_local_reading() {
    let dir = yuntun_testkit::TestDir::tmpfs("agg-cold-fallback");
    let store: Arc<dyn object_store::ObjectStore> = create_store(&StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;
    commit_cold(&store, &catalog, "s1", &[Some(1), Some(2)]).await;
    commit_cold(&store, &catalog, "s2", &[Some(10), Some(20)]).await;

    let a = instance(&dir, "inst-a", &[]);
    let cold_a = FoldingCold::new(store.clone(), true); // 永远失败
    let reference = engine(store.clone(), catalog.clone(), vec![]).await;
    let engine = engine(
        store.clone(),
        catalog.clone(),
        vec![(
            "inst-a",
            Arc::new(FoldingReader {
                inner: a,
                cold: Some(cold_a.clone() as Arc<dyn ColdReader>),
            }) as Arc<dyn ShardReader>,
        )],
    )
    .await;

    let got = engine.sql("SELECT count(*), sum(v) FROM agg").await.unwrap();
    let want = reference
        .sql("SELECT count(*), sum(v) FROM agg")
        .await
        .unwrap();
    assert_eq!(rows_of(&got), rows_of(&want), "回退本地读后答案必须不变");
    assert!(cold_a.calls() > 0, "回退路径要真的调用过远端（否则没验到回退）");
    // 回退不是降级：**不**该出现 partial 标记（那份数据本地就能读到）
    let out = engine
        .sql_partial("SELECT count(*), sum(v) FROM agg")
        .await
        .unwrap();
    assert!(
        !out.partial.is_partial(),
        "回退本地读不许留下 partial 标记：{:?}",
        out.partial.missing()
    );
}

/// `§182`：**没接冷折叠的节点**（`as_cold() == None`）⇒ 全本地算，答案不变。
///
/// 这条盯的是"老二进制 / 未接线"的混合集群：不能因为一部分节点不会新 RPC 就改变结果。
#[tokio::test]
async fn nodes_without_cold_folding_do_not_change_the_answer() {
    let dir = yuntun_testkit::TestDir::tmpfs("agg-cold-nocold");
    let store: Arc<dyn object_store::ObjectStore> = create_store(&StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;
    commit_cold(&store, &catalog, "s1", &[Some(1), Some(2)]).await;

    let a = instance(&dir, "inst-a", &[Some(5)]);
    let a_ref = instance(&dir, "inst-a", &[Some(5)]);
    // 参照：**同样的热数据**，同样没有冷折叠能力 ⇒ 两侧应当逐位相同
    let reference = engine(
        store.clone(),
        catalog.clone(),
        vec![("inst-a", a_ref as Arc<dyn ShardReader>)],
    )
    .await;
    let engine = engine(
        store.clone(),
        catalog.clone(),
        vec![("inst-a", a as Arc<dyn ShardReader>)],
    )
    .await;

    let got = engine.sql("SELECT count(*), sum(v) FROM agg").await.unwrap();
    let want = reference
        .sql("SELECT count(*), sum(v) FROM agg")
        .await
        .unwrap();
    assert_eq!(rows_of(&got), rows_of(&want));
}
