//! **`§166`：后台作业的多节点分领**（压缩按 `(table, shard)` 分租约）。
//!
//! 这一刀之前：全集群只有**一把全局 `compaction` 租约** ⇒ 同一时刻只有一个节点在合并，
//! 多 datanode 的算力一点都用不上（合并偏偏是"每个分片各写一份产物"、天然可并行的作业）。
//! 孤儿清理更糟：**连租约都没有** ⇒ 多节点各扫一遍全 bucket。
//!
//! 五条断言，一半是"能用上并行"，一半是"并行不能变成错"：
//!
//! 1. **两个节点同时跑，分到不同的分片** ⇒ 两个分片都被合并、行数守恒、且**没人重复合并**；
//! 2. **同一个分片绝不合并两次**（租约互斥 + 提交栅栏）；
//! 3. **栅栏按 purpose 核**：分片租约的水位拦得住分片提交（否则被接管的持有者会再产一份）；
//! 4. **孤儿清理全局单跑**（`gc` 租约：同一时刻只有一个持有者）；
//! 5. **DV 触发在循环里可达**（预筛是超集，`min_files` 不再是唯一入口）。

use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_compaction::{CompactionConfig, Compactor, compaction_round};
use yuntun_format::DataFormat;
use yuntun_model::dv::{DeletionEntry, DvBitmap, dv_object_path};
use yuntun_model::meta::{FileManifest, GC_LEASE, compaction_shard_lease_purpose};
use yuntun_model::ops::{CommitFilesRequest, CreateTableRequest};

const TABLE: &str = "public.dist";
const WINDOW: &str = "1970-01-01T00:00";

fn schema() -> arrow::datatypes::SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]))
}

/// `n` 行、值为 `0..n` 的批次。
fn batch(n: i64) -> arrow::record_batch::RecordBatch {
    arrow::record_batch::RecordBatch::try_new(
        schema(),
        vec![Arc::new(Int64Array::from((0..n).collect::<Vec<i64>>()))],
    )
    .unwrap()
}

struct Fx {
    store: Arc<dyn object_store::ObjectStore>,
    catalog: Arc<MemoryCatalog>,
    /// 落盘根目录（本地形态；两个"节点"共享它 —— 就是多 datanode 共冷存储的形态）
    root: String,
}

impl Drop for Fx {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

impl Fx {
    async fn new() -> Self {
        // cargo 给的测试临时根（`CARGO_TARGET_TMPDIR`，在 `target/` 里 ⇒ 不会被提交）。
        // ⚠️ 早先自己建在 `crates/compaction/target/` 下，**把测试产物提交进了仓库**（`§166` 的
        // 这次教训）—— 夹具的临时目录必须落在已被忽略的地方，并在 `Drop` 里自己收掉。
        let base = std::env::var("CARGO_TARGET_TMPDIR")
            .unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().to_string());
        let root = format!("{base}/dist-{}-{}", std::process::id(), uuid::Uuid::now_v7());
        std::fs::create_dir_all(&root).unwrap();
        let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Local {
            root: root.clone(),
        })
        .unwrap();
        let catalog = Arc::new(MemoryCatalog::new());
        catalog
            .create_table(CreateTableRequest {
                name: "dist".into(),
                namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
                schema: schema(),
                partition_cols: vec![],
                default_format: "parquet".into(),
                ingest_config: yuntun_model::meta::IngestConfig::standard(),
            })
            .await
            .unwrap();
        Self { store, catalog, root }
    }

    /// 在 `shard` 上提交 `files` 个文件（每个 `rows` 行）。
    async fn commit(&self, shard: &str, files: usize, rows: i64) {
        for i in 0..files {
            let bid = uuid::Uuid::now_v7().to_string();
            let (path, size, _rows) = yuntun_format::write_batch(
                &self.store,
                "dist",
                shard,
                WINDOW,
                &bid,
                &batch(rows),
                DataFormat::Parquet,
            )
            .await
            .unwrap();
            self.catalog
                .commit_files(CommitFilesRequest {
                    table: TABLE.into(),
                    batch_id: bid.clone(),
                    client_request_id: None,
                    client_request_ids: vec![],
                    shard: shard.into(),
                    time_window: WINDOW.into(),
                    files: vec![FileManifest {
                        file_path: path,
                        batch_id: bid,
                        file_size: size,
                        row_count: rows as u64,
                        ..Default::default()
                    }],
                    schema_version: 1,
                    row_count: rows as u64,
                })
                .await
                .unwrap();
            let _ = i;
        }
    }

    /// 一个"节点"：唯一身份 + 独立 Compactor（共享目录与存储，就是多 datanode 的形态）。
    fn node(&self, id: &str, min_files: usize) -> Compactor {
        Compactor {
            lease_holder: format!("node-{id}"),
            cfg: CompactionConfig {
                min_files,
                ..Default::default()
            },
            catalog: self.catalog.clone(),
            store: self.store.clone(),
            format: DataFormat::Parquet,
        }
    }

    async fn visible_rows(&self, shard: &str) -> u64 {
        let snap = self.catalog.current_snapshot().await;
        self.catalog
            .list_visible_files(TABLE, snap, Some(shard))
            .await
            .unwrap()
            .iter()
            .map(|f| f.row_count)
            .sum()
    }

    async fn visible_files(&self, shard: &str) -> usize {
        let snap = self.catalog.current_snapshot().await;
        self.catalog
            .list_visible_files(TABLE, snap, Some(shard))
            .await
            .unwrap()
            .len()
    }
}

/// ① 两个节点**同时**跑一轮：各自分到不同的分片，两个分片都被合并且行数守恒。
#[tokio::test]
async fn two_nodes_share_the_work_by_shard() {
    let fx = Fx::new().await;
    fx.commit("s0", 3, 4).await;
    fx.commit("s1", 3, 4).await;

    let a = fx.node("a", 3);
    let b = fx.node("b", 3);
    // 真并发：两个"节点"同时开一轮
    let (ra, rb) = tokio::join!(compaction_round(&a), compaction_round(&b));
    let (ra, rb) = (ra.unwrap(), rb.unwrap());

    assert_eq!(
        ra.compacted + rb.compacted,
        2,
        "两个分片各合并一次（合计 2）；多一个就是「同一批输入产出了两份」：{ra:?} {rb:?}"
    );
    assert_eq!(
        ra.failed + rb.failed,
        0,
        "不该有真失败（用旧快照抢到租约的那一方应被状态机的冲突检查拦下，记为 stale）：{ra:?} {rb:?}"
    );
    assert!(
        ra.lease_busy + rb.lease_busy >= 1,
        "应当出现过「别人正持有」的跳过 —— 这才是分领真的发生了：{ra:?} {rb:?}"
    );
    assert_eq!(fx.visible_files("s0").await, 1, "s0 应被合并成 1 个文件");
    assert_eq!(fx.visible_files("s1").await, 1, "s1 应被合并成 1 个文件");
    assert_eq!(fx.visible_rows("s0").await, 12, "合并前后行数必须守恒");
    assert_eq!(fx.visible_rows("s1").await, 12);
}

/// ② **同一个分片**绝不合并两次：两个节点同时盯上它，只有一个做成了。
#[tokio::test]
async fn one_shard_is_never_merged_twice() {
    let fx = Fx::new().await;
    fx.commit("s0", 3, 4).await;

    let a = fx.node("a", 3);
    let b = fx.node("b", 3);
    let (ra, rb) = tokio::join!(compaction_round(&a), compaction_round(&b));
    let (ra, rb) = (ra.unwrap(), rb.unwrap());

    assert_eq!(
        ra.compacted + rb.compacted,
        1,
        "一个分片只能有一个产出：{ra:?} {rb:?}"
    );
    assert_eq!(fx.visible_files("s0").await, 1);
    assert_eq!(fx.visible_rows("s0").await, 12, "行数守恒（没重写第二份）");
}

/// ③ **栅栏按提交方声明的 purpose 核**（`§166` 的关键正确性改动）。
///
/// 分片分领之后，栅栏若仍只认全局 `compaction`，那把锁根本没人持有 ⇒ 栅栏**静默失配**
/// （被接管的持有者照样提交成功 = 两个节点各产一份）。这条用例把两个方向都钉住。
#[tokio::test]
async fn fence_follows_the_declared_lease_purpose() {
    let fx = Fx::new().await;
    let shard_purpose = compaction_shard_lease_purpose("compaction", TABLE, "s0");
    let g = fx
        .catalog
        .acquire_lease(&shard_purpose, "someone-else", 60_000, 1_000)
        .await
        .unwrap();
    assert!(g.granted);

    // ① 用**落后**的代次 + 分片 purpose 提交 ⇒ 被栅栏拒绝
    let fenced = fx
        .catalog
        .commit_compaction(&[], vec![FileManifest::default()], 0, &shard_purpose)
        .await;
    assert!(fenced.is_err(), "分片租约的水位必须拦住分片提交：{fenced:?}");

    // ② 用**不低于**水位的代次提交 ⇒ 放行（栅栏是"代次落后"而不是"有人在持有"）
    let ok = fx
        .catalog
        .commit_compaction(&[], vec![FileManifest::default()], g.epoch, &shard_purpose)
        .await;
    assert!(ok.is_ok(), "代次不低于水位就该放行：{ok:?}");

    // ③ 全局键与分片键是**两把锁**：全局水位不该误伤分片提交（否则分片作业会被 unrelated 的
    //    全局持有着卡死）。安全性由分片租约自己保证，不靠全局。
    let fx2 = Fx::new().await;
    fx2.catalog
        .acquire_lease("compaction", "global-holder", 60_000, 1_000)
        .await
        .unwrap();
    let not_fenced = fx2
        .catalog
        .commit_compaction(&[], vec![FileManifest::default()], 0, &shard_purpose)
        .await;
    assert!(
        not_fenced.is_ok(),
        "全局水位不该误伤分片提交（两把锁各自独立）：{not_fenced:?}"
    );
}

/// ④ 孤儿清理是**全局**作业 ⇒ 同一时刻只有一个持有者（今天多节点会各扫一遍全 bucket）。
#[tokio::test]
async fn gc_lease_is_exclusive_and_can_be_handed_over() {
    let fx = Fx::new().await;
    let a = fx
        .catalog
        .acquire_lease(GC_LEASE, "gc-a", 60_000, 1_000)
        .await
        .unwrap();
    assert!(a.granted);

    let b = fx
        .catalog
        .acquire_lease(GC_LEASE, "gc-b", 60_000, 1_000)
        .await
        .unwrap();
    assert!(!b.granted, "同一时刻只能有一个节点在清孤儿");

    // 归还之后别人可以接（接手方不必干等 TTL）
    fx.catalog.release_lease(GC_LEASE, "gc-a", a.epoch).await.unwrap();
    let b2 = fx
        .catalog
        .acquire_lease(GC_LEASE, "gc-b", 60_000, 1_000)
        .await
        .unwrap();
    assert!(b2.granted, "归还后必须能被接管");
}

/// ⑤ **DV 触发在循环里可达**：只有 1 个文件（`min_files = 5` 远不够）但删除占比足够
/// ⇒ 循环也要把它合并掉。预筛必须是超集，"文件数 ≥ min_files"不能是唯一入口。
#[tokio::test]
async fn deletion_vector_trigger_is_reachable_from_the_loop() {
    let fx = Fx::new().await;
    let rows = 2_000i64;
    fx.commit("s0", 1, rows).await;

    let snap = fx.catalog.current_snapshot().await;
    let file = fx
        .catalog
        .list_visible_files(TABLE, snap, Some("s0"))
        .await
        .unwrap()
        .first()
        .cloned()
        .expect("一个已提交的文件");

    // 删掉一半（1000 行 ≥ `dv_min_card` 默认 1000，占比 50% ≥ 10%）
    let dv = DvBitmap::from_positions((0..1_000u32).collect::<Vec<u32>>());
    let entry = DeletionEntry {
        dv_id: "dv-loop".into(),
        table: TABLE.into(),
        file_path: file.file_path.clone(),
        batch_id: file.batch_id.clone(),
        applied_at: 0,
        revoked_at: 0,
        card: dv.card() as u32,
        store_path: dv_object_path(&file.file_path, "dv-loop"),
    };
    yuntun_store::put_bytes(&*fx.store, &entry.store_path, dv.to_bytes())
        .await
        .unwrap();
    fx.catalog.apply_deletions(vec![entry]).await.unwrap();

    let node = fx.node("a", 5); // 文件数 1 < 5 ⇒ 只可能由 DV 那条路触发
    let stats = compaction_round(&node).await.unwrap();
    assert!(
        stats.candidates >= 1,
        "带 DV 的分片必须进预筛（否则 DV 触发在循环里永远不可达）：{stats:?}"
    );
    assert_eq!(stats.compacted, 1, "这个分片应当被合并：{stats:?}");

    // 合并 = 消费 DV ⇒ 存活行数 = 原行数 − 删除数
    assert_eq!(
        fx.visible_rows("s0").await,
        (rows - 1_000) as u64,
        "合并必须把已删的行丢掉（DV 被消费）"
    );
    let _ = Duration::ZERO;
}
