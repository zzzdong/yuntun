//! **分区键（`WITH (partition_by=…)`）的端到端判据**（`§189/U1`）。
//!
//! # 它守的是什么
//!
//! 时间语义曾经**硬编码在列名 `event_time`** 上：`extract_event_time_ms`（窗口归属）
//! 与 `default_sort_columns`（stats + `.idx`）都只认这一个名字。后果是 ——
//! 时间列叫 `ts` / `time` / `created_at` 的表：
//!
//! * 窗口（`dt=<window>` 目录）退化为**到达时间**（数据时间语义丢失）；
//! * `FileManifest.stats` **为空** ⇒ 查询侧文件级 min/max 剪枝不生效；
//! * **不写 `.idx`** ⇒ 行组级 zone map + XOR 剪枝不生效。
//!
//! 三件事都不报错 —— 静默劣化，正是本仓最忌的失败形态。
//!
//! # 判据形状：**声明 vs 未声明 的同表对拍**
//!
//! 两张表**同 schema、同数据、同一时刻写入**，唯一差别是建表时有没有声明分区键。
//! 于是任何差异都只能由"声明"这一个变量解释（避免"碰巧 flush 了"这种假绿）。

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_ingest::{Ingestor, IngestorConfig};
use yuntun_model::ops::CreateTableRequest;
use yuntun_model::IngestBatch;

/// 事件时间列叫 `ts`（**不是** `event_time`），类型 `Timestamp(ns)` —— SQL `TIMESTAMP`
/// 建出来就是这一档，正是老实现"认不出来"的那一档。
fn table_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("v", DataType::Int64, true),
    ]))
}

/// 一个**固定的过去时刻**：2021-03-04T05:06:07Z（与"到达时间"相差数年 ⇒ 对拍有鉴别力）。
const DATA_TS_NS: i64 = 1_614_834_367_000_000_000;
/// 该时刻对齐到整分钟后的窗口串（`timeutil::format_window` 的同款口径）。
const WANT_WINDOW: &str = "2021-03-04T05:06";

fn eager_config(wal_dir: &std::path::Path) -> IngestorConfig {
    IngestorConfig {
        rows_threshold: 1,
        bytes_threshold: 1,
        time_threshold_secs: 0,
        max_flush_delay_secs: 0,
        flush_phase_spread_secs: 0,
        scan_interval: Duration::from_millis(20),
        spill_dir: wal_dir.join("spill"),
        chunk_mem_budget: 64 * 1024 * 1024,
        // parquet：`.idx`（行组级）只对它有定义（vortex 侧无组级剪枝，`D-20`）
        default_format: yuntun_format::DataFormat::Parquet,
        ..Default::default()
    }
}

async fn create(
    catalog: &Arc<dyn CatalogOps>,
    name: &str,
    partition_cols: Vec<String>,
) {
    catalog
        .create_table(CreateTableRequest {
            name: name.into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: table_schema(),
            partition_cols,
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig {
                require_idempotency_key: false,
                ..Default::default()
            },
        })
        .await
        .unwrap();
}

async fn ingest_one(ingestor: &Arc<Ingestor>, table: &str) {
    ingestor
        .ingest(IngestBatch {
            table: table.into(),
            shard_key: "s0".into(),
            record_batch: arrow::record_batch::RecordBatch::try_new(
                table_schema(),
                vec![
                    Arc::new(TimestampNanosecondArray::from(vec![DATA_TS_NS, DATA_TS_NS])),
                    Arc::new(Int64Array::from(vec![1, 2])),
                ],
            )
            .unwrap(),
            idempotency_key: None,
            received_at: std::time::SystemTime::now(),
        })
        .await
        .unwrap();
}

/// 表当前可见的文件清单（`FileManifest` 全字段）。
async fn files_of(
    catalog: &Arc<dyn CatalogOps>,
    table: &str,
) -> Vec<yuntun_model::meta::FileManifest> {
    let snap = catalog.current_snapshot().await;
    catalog
        .list_visible_files(table, snap, None)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declared_partition_key_drives_window_stats_and_index() {
    let wal_guard = yuntun_testkit::TestDir::tmpfs("partition-key-e2e");
    let wal_dir = wal_guard.path().to_path_buf();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory).unwrap();

    // 同 schema、同数据；**唯一变量** = 有没有声明分区键
    create(&catalog, "pk", vec!["ts".into()]).await;
    create(&catalog, "nopk", vec![]).await;

    let wal = yuntun_wal::writer::WalWriter::open(yuntun_wal::WalConfig::for_dir(&wal_dir), 0)
        .await
        .unwrap();
    let ingestor = Arc::new(Ingestor::new(
        eager_config(&wal_dir),
        wal.clone(),
        catalog.clone(),
        store.clone(),
    ));
    let _acc = ingestor.clone().spawn_accumulator(Default::default());

    ingest_one(&ingestor, "pk").await;
    ingest_one(&ingestor, "nopk").await;

    // 驱动到 flush 落盘（事件判据，不睡墙钟）：强制 flush 两张表
    for t in ["pk", "nopk"] {
        ingestor.force_flush(t).await.expect("force flush");
    }

    // ---- ① 声明了：窗口 = **数据时间**，stats 有 ts 的 min/max，且有 .idx ----
    let declared = files_of(&catalog, "pk").await;
    assert!(!declared.is_empty(), "声明了分区键的表应当有已提交文件");
    let f = &declared[0];
    assert_eq!(
        f.time_window, WANT_WINDOW,
        "窗口必须按**声明列的数据时间**归属（实际 {}）—— 退化成到达时间说明声明没生效",
        f.time_window
    );
    let stats = f.stats.as_ref().expect("声明了分区键 ⇒ stats 必须有");
    assert!(
        stats.columns.iter().any(|c| c.name == "ts"),
        "stats 里必须有 `ts`（实际列：{:?}）—— 没有它查询侧剪不了文件",
        stats.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
    );
    assert!(
        !f.index_path.is_empty(),
        "声明了分区键 ⇒ 必须写出行组级索引 .idx（实际 index_path 为空）"
    );

    // ---- ② 反证：没声明 ⇒ 窗口**不是**数据时间，且没有 .idx ----
    let plain = files_of(&catalog, "nopk").await;
    if !plain.is_empty() {
        let g = &plain[0];
        assert_ne!(
            g.time_window, WANT_WINDOW,
            "未声明分区键时窗口应是**到达时间**；若也是数据时间，说明①的断言没有鉴别力"
        );
        assert!(
            g.index_path.is_empty(),
            "未声明分区键 ⇒ 排序列为空 ⇒ 不该有 .idx（实际 {}）",
            g.index_path
        );
    }
}
