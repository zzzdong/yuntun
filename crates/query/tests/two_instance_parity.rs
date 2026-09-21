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
