//! chaos 工具与验收测试（详细设计 §12 / 阶段 0.5 验收 E2/E3）。
//!
//! ## 模拟崩溃的方式
//! 阶段 0 单节点"硬崩溃" = **直接丢弃 Lakehouse 全部组件（不等待 flush、不 cancel）**
//! 然后全新重建（空 Catalog + 同一 WAL 目录 + 同一 local store 目录）。
//! 这与 `kill -9` 的可观测后果一致：
//! - 已 fsync 的 WAL 记录存活 → 恢复依据
//! - 已写对象存储的文件存活（local store）
//! - MemoryCatalog 清零（C5）→ 由 `resume_recovered` 从 WAL 重建
//!
//! ## 验收映射
//! - **E3（恢复后数据无丢失）**：[`crash_recovery_no_data_loss`] —— 每轮
//!   "写入若干批（拿到 ack）→ 硬崩溃 → 重建 → resume → 查询计数 == acked 行数"，
//!   共 5 轮，全部在**同一份 WAL/store 目录**上滚动，验证跨崩溃累积无丢失。
//! - **E2（chaos 轮次无失败）**：由 E3 的轮次循环覆盖（每轮即一次 kill/restart）。
//! - **T6.4（schema 并发冲突）**：[`concurrent_schema_evolution`] —— N 个并发
//!   writer 各带独立新列，OCC 冲突由 §5.2 重试循环消化，最终 schema 包含全部列。
//! - **T6.8（查询侧多版本对齐）**：[`query_multi_version_alignment`] ——
//!   v1/v2 文件共存，查询统一到最新 schema（缺失列 null 填充）。
//! - **T6.5（幂等键 + Compaction）**：[`idempotency_survives_compaction`] ——
//!   合并删除原文件后，同键重试仍幂等（§7.3.1 独立存储）。
//!
//! ## 场景清单与进度（`design.md` §12.3 的 11 项）
//!
//! | # | 场景 | chaos 层 | 备注 |
//! |---|---|---|---|
//! | 1 | Compaction 期间查询 | ❌ | 单测有快照隔离，无并发查询 |
//! | 2 | 分片移除期间查询 | ❌ | — |
//! | 3 | 孤儿清理不误删已知文件 | ❌ | 单测仅覆盖纯函数判定 |
//! | 4 | Schema 变更 + EXPLAIN 谓词下推 | ❌ | — |
//! | 5 | 幂等键 + Compaction | ✅ | `idempotency_survives_compaction` |
//! | 6 | 崩溃恢复（各状态点） | ✅ | `crash_recovery_no_data_loss`（5 轮硬崩溃） |
//! | 7 | WAL 撕裂 | ⚠️ 仅解码层 | `wal::segment::torn_write_detected_by_crc`；缺端到端 recover |
//! | 8 | 并发写 + fsync 前/中/后 kill | ❌ | 需要 fsync 注入点 |
//! | 9 | `synced_seq`（write 后 fsync 前 kill） | ⚠️ 仅读取层 | `wal::reader::scan_range_reads_only_synced` |
//! | 10 | Batch 超时 + segment 释放 | ⚠️ 仅监控层 | `wal::cleanup::monitor_aborts_timed_out_batches`（未断言 segment 释放） |
//! | 11 | 磁盘水位强制 abort | ⚠️ 仅监控层 | `wal::cleanup::monitor_force_aborts_on_disk_watermark` |
//!
//! **"单测有"不等于"通过"**：单测覆盖的是解码/判定的**纯逻辑**，
//! chaos 层要的是真实磁盘 + 跨重启 + 并发下的端到端证据。表里标 ⚠️ 的都还欠这一层。

// 本 crate 当前只含验收测试（E2/E3/T6.4/T6.8）与压测示例；以下导入均为测试专用。
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;
#[cfg(test)]
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use arrow::array::{Int64Array, StringArray};
#[cfg(test)]
use arrow::datatypes::{DataType, Field, Schema};
#[cfg(test)]
use yuntun_catalog::{CatalogOps, MemoryCatalog};
#[cfg(test)]
use yuntun_ingest::{Ingestor, IngestorConfig};
#[cfg(test)]
use yuntun_model::ops::CreateTableRequest;
#[cfg(test)]
use yuntun_model::IngestBatch;
#[cfg(test)]
use yuntun_query::QueryEngine;

/// 一套可重建的 Lakehouse 组件（chaos 轮次间共享目录）。
#[cfg(test)]
struct Setup {
    catalog: Arc<MemoryCatalog>,
    ingestor: Arc<Ingestor>,
    engine: Arc<QueryEngine>,
}

#[cfg(test)]
fn schema() -> arrow::datatypes::SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_time", DataType::Int64, false),
        Field::new("user", DataType::Utf8, true),
    ]))
}

#[cfg(test)]
fn batch(rows: i64) -> arrow::record_batch::RecordBatch {
    arrow::record_batch::RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![rows; 3])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])),
        ],
    )
    .unwrap()
}

#[cfg(test)]
fn tmpdir(name: &str) -> std::path::PathBuf {
    // 故障注入/崩溃恢复场景需要**真实落盘语义**（fsync 等待、重启后目录仍在）
    // → 使用真实磁盘；常规 tmpfs 加速不适用于 chaos
    yuntun_testkit::TestDir::disk(&format!("chaos-{name}")).into_path()
}

/// chaos 用例**串行执行**：重 IO（真实磁盘 fsync）+ 共享目录/水位语义，
/// 并行会互相拖慢恢复等待窗口并放大时序噪声。
#[cfg(test)]
static CHAOS_GATE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// 构建一套组件（每轮重建 = 模拟进程重启）。
///
/// `tables`：重启后需要恢复的表定义（第 3 个元素 = 是否强制幂等键）。
/// 阶段 1 表定义由 raft snapshot 恢复（C5/§5.4.2）；
/// 阶段 0 chaos harness 中由调用方重放（模拟 snapshot 已含表定义）。
#[cfg(test)]
async fn build(
    wal_dir: &std::path::Path,
    store_root: &std::path::Path,
    tables: &[(&str, arrow::datatypes::SchemaRef, bool)],
) -> Setup {
    let catalog = Arc::new(MemoryCatalog::new());
    // 恢复表定义（模拟 raft snapshot；已存在则跳过）
    for (name, schema, require_key) in tables {
        if catalog.get_table(name).await.unwrap().is_none() {
            catalog
                .create_table(CreateTableRequest {
                    name: name.to_string(),
                    namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
                    schema: schema.clone(),
                    partition_cols: vec![],
                    default_format: "parquet".into(),
                    ingest_config: yuntun_model::meta::IngestConfig {
                        require_idempotency_key: *require_key,
                        ..yuntun_model::meta::IngestConfig::standard()
                    },
                })
                .await
                .unwrap();
        }
    }
    let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Local {
        root: store_root.to_string_lossy().to_string(),
    })
    .unwrap();
    let wal = yuntun_wal::writer::WalWriter::open(yuntun_wal::WalConfig::for_dir(wal_dir), 0)
        .await
        .unwrap();
    let ingestor = Arc::new(Ingestor::new(
        IngestorConfig {
            rows_threshold: 1,
            time_threshold_secs: 0,
            max_flush_delay_secs: 0,
            flush_phase_spread_secs: 0,
            scan_interval: Duration::from_millis(20),
            spill_dir: wal_dir.join("spill"),
            ..Default::default()
        },
        wal,
        catalog.clone(),
        store,
    ));
    // 崩溃恢复分流（build 阶段，模拟 Lakehouse::build_with_shutdown 行为）
    ingestor.resume_recovered().await.unwrap();
    let cache = Arc::new(yuntun_query::LocalCatalog::new());
    let engine = Arc::new(QueryEngine::new(
        yuntun_store::create_store(&yuntun_store::StoreConfig::Local {
            root: store_root.to_string_lossy().to_string(),
        })
        .unwrap(),
        cache,
    ));
    Setup {
        catalog,
        ingestor,
        engine,
    }
}

/// E3 / E2：crash 恢复无数据丢失（5 轮硬崩溃，同一目录滚动）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_recovery_no_data_loss() {
    let _gate = CHAOS_GATE.lock().await;
    let _ = tracing_subscriber::fmt()
        .with_env_filter("yuntun=debug")
        .with_test_writer()
        .try_init();
    let wal_dir = tmpdir("wal");
    let store_root = tmpdir("store");

    // 表定义随每轮 build 恢复（模拟 raft snapshot）
    let tables = [("audit", schema(), false)];
    let mut setup: Option<Setup> = Some(build(&wal_dir, &store_root, &tables).await);

    let mut acked_rows = 0u64;
    let rounds = 5;
    for round in 0..rounds {
        // ① 本轮写入 3 批（拿到 ack = 已 fsync），每批 3 行
        let batch_rows = [
            (100 + round as i64 * 10, format!("r{round}-k0")),
            (200 + round as i64 * 10, format!("r{round}-k1")),
            (300 + round as i64 * 10, format!("r{round}-k2")),
        ];
        for (ts, key) in &batch_rows {
            let receipt = setup
                .as_ref()
                .unwrap()
                .ingestor
                .ingest(IngestBatch {
                    table: "audit".into(),
                    shard_key: format!("s{}", ts % 2),
                    record_batch: batch(*ts),
                    idempotency_key: Some(key.clone()),
                    received_at: std::time::SystemTime::now(),
                })
                .await
                .unwrap();
            acked_rows += receipt.row_count;
        }

        // ② 硬崩溃：不等 flush，直接丢弃全部组件（模拟 kill -9）
        drop(setup.take());

        // ③ 重启：空 Catalog + 同目录 WAL/store → resume → 攒批循环处理 Pending
        setup = Some(build(&wal_dir, &store_root, &tables).await);
        let cur = setup.as_ref().unwrap();
        let shutdown = CancellationToken::new();
        // accumulator 必须在轮询期间保持运行（它负责重读 WAL → flush → commit）
        let _acc = cur.ingestor.clone().spawn_accumulator(shutdown.clone());

        // ④ 查询计数 == 累计 acked 行数（无丢失、无重复）。
        // 真实磁盘上"WAL 重放 → flush → commit"可能超过固定等待 → 轮询；
        // tmpfs 上通常首轮即满足。
        //
        // 上限 60s：本断言验证的是**数据不丢**（正确性），不是恢复延迟（性能）。
        // cargo 默认**并行跑所有 test binary**，本套件会与另外 46 个 binary 争 CPU/IO，
        // 实测并行负载下 30s 可能不足（单跑 1.3s）。恢复延迟的指标留给阶段 2 的压测，
        // 不在这里用超时冒充（去 flaky 的根治见 plan.md T6.1：chaos 独立跑）。
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut got: u64;
        loop {
            cur.engine
                .catalog()
                .refresh(&(cur.catalog.clone() as Arc<dyn yuntun_catalog::CatalogOps>))
                .await
                .unwrap();
            let batches = cur
                .engine
                .sql("SELECT count(*) FROM yuntun.public.audit")
                .await
                .unwrap();
            got = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0) as u64;
            if got == acked_rows {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "round {round}: 恢复超时（acked={acked_rows}, got={got}）"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        // 恢复完成后再停 accumulator（避免中断后续 flush）
        shutdown.cancel();
        assert_eq!(
            got, acked_rows,
            "round {round}: 恢复后行数必须等于累计 acked 行数（无丢失无重复）"
        );
        tracing::info!(round, acked_rows, "crash round OK");
    }
}

/// T6.4：schema 并发演进 —— N 个并发 writer 各带独立新列，OCC + 重试消化冲突。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_schema_evolution() {
    let _gate = CHAOS_GATE.lock().await;
    let wal_dir = tmpdir("wal-evo");
    let store_root = tmpdir("store-evo");
    let tables = [(
        "evo",
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
            as arrow::datatypes::SchemaRef,
        false,
    )];
    let setup = build(&wal_dir, &store_root, &tables).await;

    const N: usize = 8;
    let mut handles = Vec::new();
    for i in 0..N {
        let ingestor = setup.ingestor.clone();
        let col = format!("extra_{i}");
        handles.push(tokio::spawn(async move {
            let s = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new(&col, DataType::Utf8, true),
            ]));
            let b = arrow::record_batch::RecordBatch::try_new(
                s,
                vec![
                    Arc::new(Int64Array::from(vec![i as i64; 2])),
                    Arc::new(StringArray::from(vec![Some("x"), Some("y")])),
                ],
            )
            .unwrap();
            ingestor
                .ingest(IngestBatch {
                    table: "evo".into(),
                    shard_key: "s0".into(),
                    record_batch: b,
                    idempotency_key: None,
                    received_at: std::time::SystemTime::now(),
                })
                .await
        }));
    }
    for h in handles {
        let r = h.await.unwrap();
        assert!(r.is_ok(), "并发演进 + 重试后所有写入应成功: {r:?}");
    }

    // 最终 schema 包含全部 N 个新列
    let (_, version) = setup.catalog.table_schema("evo").await.unwrap().unwrap();
    let final_schema = setup.catalog.table_schema("evo").await.unwrap().unwrap().0;
    for i in 0..N {
        assert!(
            final_schema.field_with_name(&format!("extra_{i}")).is_ok(),
            "列 extra_{i} 必须在最终 schema 中"
        );
    }
    assert!(version > N as u64);
    let _ = store_root;
}

/// T6.8：查询侧多版本对齐 —— v1/v2 文件共存，查询统一到最新 schema。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_multi_version_alignment() {
    let _gate = CHAOS_GATE.lock().await;
    let wal_dir = tmpdir("wal-mv");
    let store_root = tmpdir("store-mv");
    let tables = [(
        "mv",
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
            as arrow::datatypes::SchemaRef,
        false,
    )];
    let setup = build(&wal_dir, &store_root, &tables).await;

    // v1 批次（只有 a 列）
    setup
        .ingestor
        .ingest(IngestBatch {
            table: "mv".into(),
            shard_key: "s0".into(),
            record_batch: arrow::record_batch::RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)])),
                vec![Arc::new(Int64Array::from(vec![1, 2]))],
            )
            .unwrap(),
            idempotency_key: None,
            received_at: std::time::SystemTime::now(),
        })
        .await
        .unwrap();

    // v2 批次（a + b 列 → 自动演进）
    setup
        .ingestor
        .ingest(IngestBatch {
            table: "mv".into(),
            shard_key: "s0".into(),
            record_batch: arrow::record_batch::RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("a", DataType::Int64, true),
                    Field::new("b", DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![3, 4])),
                    Arc::new(StringArray::from(vec![Some("x"), None])),
                ],
            )
            .unwrap(),
            idempotency_key: None,
            received_at: std::time::SystemTime::now(),
        })
        .await
        .unwrap();

    // flush 两个批次（轮询等待落盘；真实磁盘 + 并发负载下固定 sleep 不可靠）
    let shutdown = CancellationToken::new();
    let _acc = setup.ingestor.clone().spawn_accumulator(shutdown.clone());
    // 同上：多 binary 并行下的负载余量（该断言关注"最终一致"，不关注延迟）
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let snap = setup.catalog.current_snapshot().await;
        let n = setup
            .catalog
            .list_visible_files("public.mv", snap, None)
            .await
            .map(|f| f.len())
            .unwrap_or(0);
        if n >= 2 || std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    shutdown.cancel();

    // 查询：统一到 v2 schema；v1 文件缺失 b 列 → null 填充
    setup
        .engine
        .catalog()
        .refresh(&(setup.catalog.clone() as Arc<dyn yuntun_catalog::CatalogOps>))
        .await
        .unwrap();

    let batches = setup
        .engine
        .sql("SELECT count(*) FROM yuntun.public.mv")
        .await
        .unwrap();
    let got = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(got, 4, "两个 schema_version 的文件行数合并");

    // b 列：v2 行有值，v1 行 null
    let batches = setup
        .engine
        .sql("SELECT count(*) FROM yuntun.public.mv WHERE b IS NOT NULL")
        .await
        .unwrap();
    let got = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(got, 1, "v1 文件缺失列应填 null（b IS NOT NULL 仅 1 行）");
}

// ---------------------------------------------------------------- 测试辅助
//
// 真实磁盘 + 并行 test binary 下固定 sleep 不可靠（恢复/flush 耗时随负载漂移），
// 一律用"轮询到条件成立 + 60s 上界"。上界只兜"永不成立"，不冒充延迟指标。

/// 等某个 shard 的可见文件数达到 `n`（返回当前清单）。
#[cfg(test)]
async fn wait_visible_files(
    setup: &Setup,
    table: &str,
    shard: &str,
    n: usize,
) -> Vec<yuntun_model::meta::FileManifest> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let snap = setup.catalog.current_snapshot().await;
        let files = setup
            .catalog
            .list_visible_files(table, snap, Some(shard))
            .await
            .unwrap_or_default();
        if files.len() >= n || tokio::time::Instant::now() >= deadline {
            return files;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// 查询 `count(*)`（先刷新查询侧缓存，否则读到的是空快照）。
#[cfg(test)]
async fn count_rows(setup: &Setup, qualified_table: &str) -> u64 {
    setup
        .engine
        .catalog()
        .refresh(&(setup.catalog.clone() as Arc<dyn CatalogOps>))
        .await
        .unwrap();
    let batches = setup
        .engine
        .sql(&format!("SELECT count(*) FROM yuntun.{qualified_table}"))
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0) as u64
}

// ---------------------------------------------------------------- 目录语义场景

/// T6.5（`design.md` §12.3 #5）：**幂等键 + Compaction** ——
/// 合并把原文件 `deleted_at` 之后，同一幂等键的重试**仍然幂等**。
///
/// 对应 §7.3.1 的"核心修正"：幂等键是**独立存储**，不随 FileManifest 生命周期消失。
/// 为什么值得单独跑一遍端到端：入口预筛若（直接或间接）依赖"文件/批次还在"，
/// compaction 之后就会"忘掉"这个键，客户端重试会**再写一份数据** ——
/// 静默重复计数，而所有功能测试照样全绿。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idempotency_survives_compaction() {
    let _gate = CHAOS_GATE.lock().await;
    let wal_dir = tmpdir("wal-idem");
    let store_root = tmpdir("store-idem");
    // 强制幂等键（standard 模板 = require）→ 走完整的"必须带键"路径
    let tables = [("idem", schema(), true)];
    let setup = build(&wal_dir, &store_root, &tables).await;
    let shutdown = CancellationToken::new();
    let _acc = setup.ingestor.clone().spawn_accumulator(shutdown.clone());

    let ingest = |key: &'static str, ts: i64| {
        let ingestor = setup.ingestor.clone();
        async move {
            ingestor
                .ingest(IngestBatch {
                    table: "idem".into(),
                    shard_key: "s0".into(),
                    record_batch: batch(ts),
                    idempotency_key: Some(key.into()),
                    received_at: std::time::SystemTime::now(),
                })
                .await
                .unwrap()
        }
    };

    // ① 三个不同键各写一批（每批 3 行）
    let mut acked_rows = 0u64;
    for (i, k) in ["idem-k0", "idem-k1", "idem-k2"].iter().enumerate() {
        let r = ingest(k, 100 + i as i64).await;
        assert!(!r.duplicate, "首次写入不得被判重: {k}");
        acked_rows += r.row_count;
    }
    assert_eq!(acked_rows, 9);

    // 等落盘：rows_threshold=1 → 每批独立 chunk → 3 个文件
    let files = wait_visible_files(&setup, "public.idem", "s0", 3).await;
    assert_eq!(files.len(), 3, "三个批次应产出三个文件（后续 compaction 才有意义）");

    // ② 重试（compaction 之前）：入口预筛命中 —— 不写 WAL、不加行
    let dup = ingest("idem-k0", 999).await;
    assert!(dup.duplicate, "同键重试必须被判重（否则静默重复）");
    assert_eq!(dup.row_count, 0);
    assert_eq!(dup.wal_seq, 0, "判重请求不得写 WAL");

    // ③ compaction：3 文件 → 1 文件，旧文件 deleted_at
    let catalog: Arc<dyn CatalogOps> = setup.catalog.clone();
    let compactor = yuntun_compaction::Compactor {
        cfg: yuntun_compaction::CompactionConfig {
            min_files: 3,
            ..Default::default()
        },
        catalog: catalog.clone(),
        store: yuntun_store::create_store(&yuntun_store::StoreConfig::Local {
            root: store_root.to_string_lossy().to_string(),
        })
        .unwrap(),
        format: yuntun_format::DataFormat::Parquet,
    };
    let snap = catalog.current_snapshot().await;
    let new_snap = yuntun_compaction::compact_shard(&compactor, "public.idem", "s0", snap)
        .await
        .unwrap()
        .expect("文件数达阈值应触发合并");
    let merged = catalog
        .list_visible_files("public.idem", new_snap, Some("s0"))
        .await
        .unwrap();
    assert_eq!(merged.len(), 1, "合并后只剩一个文件");
    assert_eq!(merged[0].row_count, 9, "合并文件含全部 9 行");
    // 旧快照仍见 3 个（快照隔离）——被测数据没被"已删"污染
    assert_eq!(
        catalog
            .list_visible_files("public.idem", snap, Some("s0"))
            .await
            .unwrap()
            .len(),
        3
    );

    // ④ compaction 之后同键重试 → **仍然幂等**（键独立于文件生命周期）
    let after = ingest("idem-k0", 1000).await;
    assert!(
        after.duplicate,
        "合并删除原文件后，同键重试仍必须幂等（§7.3.1 独立存储）"
    );
    assert_eq!(after.row_count, 0);

    // ⑤ 端到端：总行数恒为 9（无重复、无丢失）
    assert_eq!(
        count_rows(&setup, "public.idem").await,
        acked_rows,
        "compaction + 重试后行数不得增长"
    );
    shutdown.cancel();
}
