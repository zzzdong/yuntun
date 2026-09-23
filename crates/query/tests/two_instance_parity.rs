//! **T12.2 第三刀：双实例进程内对拍**（M4 判据的第一形态）。
//!
//! 两个 `ChunkStore` 当两个 datanode：**同一批逻辑写入**分给两个实例，查询结果必须与
//! **单节点串行**（同一批行全写进一个实例）**逐行精确相等** —— 也就是每行**只出一次**。
//!
//! 为什么这条是 M4 的模板（`plan.md` §八）：`architecture-with-chunk §4.4` 的冷热边界按实例
//! 切分一旦实现错，症状都是**结果变了但不报错**：
//!
//! | 实现错法 | 症状 |
//! |---|---|
//! | 只读某一个实例 | **少**另一台未落盘的数据（旧结构下就是如此：只有一个热读器槽位） |
//! | 把同一份数据读两次 | **多**行（重复计数，`plan.md §5.3-1`） |
//!
//! 两种错都只能靠"两种装配跑同一批写入再逐行比"抓住 —— 这正是 M4/R5 要的**对拍**。
//!
//! 两个实例刻意用**相同的 `(table, shard, window, epoch)`**：设计上"多个 datanode 可能同时写
//! 同一 partition，各自出各自的文件"（`architecture §5.1`），所以同名 key 才是真实形态。

use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_chunk::{ChunkKey, ChunkStore, ChunkStoreConfig, MemoryLedger, SealPolicy};
use yuntun_model::ops::CreateTableRequest;
use yuntun_query::{LocalCatalog, QueryEngine};
use yuntun_store::{create_store, ShardId, StoreConfig};

/// 全限定表名：必须与 `ChunkKey.shard.table` 一致，否则热读按表名过滤会扑空
const TABLE: &str = "public.parity";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
}

fn batch(vals: &[i64]) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
}

/// 造一个"实例"的 chunk store。数据直接 `append` 进去：本用例测的是**读侧**的按实例切分，
/// 不是写入路径（写路径有自己的对拍与 chaos）。
fn instance(dir: &yuntun_testkit::TestDir, instance_id: &str) -> Arc<ChunkStore> {
    ChunkStore::new(
        ChunkStoreConfig {
            // 宽松策略：本用例要的是"数据**留在热副本里**"（热读路径才是被测对象），
            // 所以阈值放极大 —— 不 seal、不 spill、不 flush。
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

/// 往实例里写一批行（两个实例刻意用**同一个 shard**，见模块注释）。
fn write(store: &ChunkStore, vals: &[i64]) {
    store
        .append(
            ChunkKey::new(ShardId::new(TABLE, "default", "2026-09-21T10:00"), 0),
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
            name: "parity".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: Default::default(),
        })
        .await
        .unwrap();
}

/// 查一列并**排序输出**：对拍比的是内容，不是批次边界或行序（那些本就不保证一致）。
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

/// 装配"某几个实例 + 查询引擎"，返回排序后的结果。
///
/// 目录守卫由调用方以局部变量持有（活到本函数返回之后）—— 这是刻意的：`TestDir` 一 drop
/// 目录就没了，而热数据正在里面。
async fn engine_with(instances: Vec<(&str, Arc<ChunkStore>)>) -> Vec<i64> {
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;

    let cache = Arc::new(LocalCatalog::new());
    cache.set_catalog_ops(catalog.clone());
    for (id, store) in instances {
        // 名录是唯一真相（T12.3）：实例必须**登记进名录**，否则一次刷新
        // 就会用名录整体替换成员表、把这个实例（连同它的热读器）摘掉。
        catalog
            .register_datanode(yuntun_model::meta::DatanodeMember {
                instance_id: id.to_string(),
                address: String::new(),
                registered_at_ms: 0,
            })
            .await
            .unwrap();
        cache.set_hot_shards(id, store);
    }
    cache.refresh(&catalog).await.unwrap();

    let engine = QueryEngine::new(create_store(&StoreConfig::Memory).unwrap(), cache);
    sorted_values(&engine).await
}

/// **核心对拍**：`双实例（各写一半）` 的结果必须与 `单节点串行（全写一个）` **逐行相等**。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_instances_parity_with_single_node_serial() {
    let all: Vec<i64> = (1..=6).collect();
    let (left, right) = all.split_at(3);

    // ① 单节点串行：一个实例收到全部 6 行
    let single = {
        let dir = yuntun_testkit::TestDir::tmpfs("parity-single");
        let s = instance(&dir, "solo");
        write(&s, &all);
        engine_with(vec![("solo", s)]).await
    };

    // ② 双实例：同一批写入分给两个 datanode（各 3 行）
    let split = {
        let dir_a = yuntun_testkit::TestDir::tmpfs("parity-a");
        let dir_b = yuntun_testkit::TestDir::tmpfs("parity-b");
        let a = instance(&dir_a, "inst-a");
        let b = instance(&dir_b, "inst-b");
        write(&a, left);
        write(&b, right);
        engine_with(vec![("inst-a", a), ("inst-b", b)]).await
    };

    assert_eq!(single, all, "单节点串行：应读到全部 6 行");
    assert_eq!(
        split, all,
        "双实例必须与单节点串行**逐行精确相等**：少一行 = 漏读某个实例；多一行 = 同一份数据读两次"
    );
}

/// **反证**：只注册一个实例时，结果**确实会少**（证明上一条断言不是"碰巧相等"）。
///
/// 这条钉住"测试本身有效"：如果两实例与一实例的结果都一样，那核心对拍就什么都证明不了。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_registered_instance_sees_only_its_own_rows() {
    let dir_a = yuntun_testkit::TestDir::tmpfs("parity-only-a");
    let dir_b = yuntun_testkit::TestDir::tmpfs("parity-only-b");
    let a = instance(&dir_a, "inst-a");
    let b = instance(&dir_b, "inst-b");
    write(&a, &[1, 2, 3]);
    write(&b, &[4, 5, 6]);

    // 只把 inst-a 接进来：inst-b 的行**不该出现**（它们还没落盘、也没人拉）
    let only_a = engine_with(vec![("inst-a", a)]).await;
    assert_eq!(only_a, vec![1, 2, 3], "只注册 inst-a 时只能看到它自己的行");
}

// ---------------------------------------------------------------------------
// T14.4：跨实例文件合并（`source_instance` 的多节点形态）
// ---------------------------------------------------------------------------

/// 把一个实例的**已落盘文件**写进共享对象存储并提交进目录。
///
/// `source_instance` 刻意填**该实例自己的 id** —— 这正是真实写入路径的形态
/// （`ingest/src/flush.rs` 填的就是本实例 id），也是 `§4.4` 冷热切分的依据。
async fn commit_file(
    store: &Arc<dyn object_store::ObjectStore>,
    catalog: &Arc<dyn CatalogOps>,
    instance_id: &str,
    vals: &[i64],
) -> String {
    const WINDOW: &str = "2026-09-21T10:00";
    const SHARD: &str = "default";
    let bid = format!("{instance_id}-{}", uuid::Uuid::now_v7());
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
                batch_id: bid.clone(),
                file_size: size,
                row_count: rows,
                table: TABLE.into(),
                shard: SHARD.into(),
                time_window: WINDOW.into(),
                source_instance: instance_id.into(),
                ..Default::default()
            }],
            schema_version: 1,
            row_count: rows,
        })
        .await
        .unwrap();
    bid
}

/// 装配"共享冷存储 + 共享目录 + 若干实例的热数据"。
///
/// 与 [`engine_with`] 的差别：这个版本让**冷数据（真 parquet 文件）与热数据（真 chunk）
/// 同时存在**，并且把 `store`/`catalog` 交给调用方 —— 便于"合并之后再查一遍"。
async fn engine_with_cold_and_hot(
    instances: Vec<(&str, Arc<ChunkStore>)>,
    store: Arc<dyn object_store::ObjectStore>,
    catalog: Arc<dyn CatalogOps>,
) -> QueryEngine {
    let cache = Arc::new(LocalCatalog::new());
    cache.set_catalog_ops(catalog.clone());
    for (id, s) in instances {
        catalog
            .register_datanode(yuntun_model::meta::DatanodeMember {
                instance_id: id.to_string(),
                address: String::new(),
                registered_at_ms: 0,
            })
            .await
            .unwrap();
        cache.set_hot_shards(id, s);
    }
    cache.refresh(&catalog).await.unwrap();
    QueryEngine::new(store, cache)
}

/// **T14.4 核心**：把**别人的**文件合并掉之后，结果必须一字不差。
///
/// 真实形态（`architecture §5.1`：多个 datanode 可能同时写同一 partition、各出各的文件）：
/// 同一 shard 上，`inst-a` 有**已落盘文件** + **没落盘的热数据**，`inst-b` 同理。
///
/// 合并把 a、b 的文件融成一个产物，于是冒出一个只在多节点才出现的问题：
/// **这个产物属于谁？** 答案是**谁也不属于** —— 因为按实例二维切分冷热（`§4.4`）时，
/// 归给任何一方都会让那一方的热数据范围被误当成"覆盖了这些行"（**重复计数**）。
///
/// 所以这条用例钉三件事：
/// ① 合并前后行集**逐行相等**（多 = 重复计数，少 = 漏读）；
/// ② 产物 `source_instance` **为空**（中立实例）；
/// ③ 老文件真的被替换（否则"合并"根本没发生，用例是空转）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_instance_merge_keeps_row_set_exact() {
    let dir_a = yuntun_testkit::TestDir::tmpfs("xmerge-a");
    let dir_b = yuntun_testkit::TestDir::tmpfs("xmerge-b");
    let a = instance(&dir_a, "inst-a");
    let b = instance(&dir_b, "inst-b");

    let store = create_store(&StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    create_table(&catalog).await;

    // 每个实例：**已落盘的文件**（3 行）+ **还没落盘的热数据**（1 行）
    commit_file(&store, &catalog, "inst-a", &[1, 2, 3]).await;
    commit_file(&store, &catalog, "inst-b", &[4, 5, 6]).await;
    write(&a, &[7]);
    write(&b, &[8]);

    let expected: Vec<i64> = (1..=8).collect();

    // ① 合并前：两边的冷文件 + 两边的热数据，一个不少
    let before = {
        let engine = engine_with_cold_and_hot(
            vec![("inst-a", a.clone()), ("inst-b", b.clone())],
            store.clone(),
            catalog.clone(),
        )
        .await;
        sorted_values(&engine).await
    };
    assert_eq!(before, expected, "合并前应看到两边的文件与两边的热数据");

    // ② **跨实例合并**：inst-a 与 inst-b 的文件融成一个产物
    let compactor = Arc::new(yuntun_compaction::Compactor {
        lease_holder: "inst-a".into(),
        cfg: yuntun_compaction::CompactionConfig {
            min_files: 1,
            ..Default::default()
        },
        catalog: catalog.clone(),
        store: store.clone(),
        format: yuntun_format::DataFormat::Parquet,
    });
    let snap = catalog.current_snapshot().await;
    let merged = yuntun_compaction::compact_shard(&compactor, TABLE, "default", snap, 0)
        .await
        .unwrap();
    assert!(merged.is_some(), "两个文件应当真的被合并");

    // ③ 产物是**中立实例**（`§4.4` 冷热切分的前提），行数是输入之和
    let after_snap = catalog.current_snapshot().await;
    let files = catalog
        .list_visible_files(TABLE, after_snap, None)
        .await
        .unwrap();
    assert_eq!(files.len(), 1, "两个老文件应被合并产物替换");
    assert_eq!(
        files[0].source_instance, "",
        "合并产物必须**不属于任何实例**：归给谁，谁的热数据范围就会被误当成覆盖了这些行"
    );
    assert_eq!(files[0].row_count, 6, "产物行数 = 两侧输入之和（3 + 3）");

    // ④ 合并后：热数据照旧、冷数据换了身份，行集**逐行不变**
    let after = {
        let engine = engine_with_cold_and_hot(
            vec![("inst-a", a.clone()), ("inst-b", b.clone())],
            store.clone(),
            catalog.clone(),
        )
        .await;
        sorted_values(&engine).await
    };
    assert_eq!(
        after, expected,
        "跨实例合并**不得改变行集**：多一行 = 重复计数，少一行 = 漏读"
    );
}
