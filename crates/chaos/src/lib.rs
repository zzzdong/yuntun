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
//! ## 场景清单与进度（`design.md` §12.3 的 11 项）—— **11/11 已完成**
//!
//! 11 项全部具备"真实磁盘 + 跨重启 + 并发"下的端到端证据。
//! 过程中抓出五个真缺陷、修掉四个（`operation-log` §27 / §28.2 / §29.1 / §30 已修，
//! §28.1 待与 R3 同批）。
//!
//! | # | 场景 | chaos 层 | 备注 |
//! |---|---|---|---|
//! | 1 | Compaction 期间查询 | ✅ | `compaction_during_query_keeps_counts_monotonic`（并发采样 + 静默点严格断言） |
//! | 2 | 分片移除期间查询 | ✅ | `shard_removal_during_query_filters_by_deleted_at`（含 R2 增量 delta 消费"文件消失"） |
//! | 3 | 孤儿清理不误删已知文件 | ✅ | `orphan_cleanup_spares_known_and_inflight_files`（含静置期在途文件防线） |
//! | 4 | Schema 变更 + EXPLAIN 谓词下推 | ✅ | `schema_evolution_keeps_predicate_pushed_down`（断言谓词下推；`FilterExec` 未消除属已知差距，`operation-log §29.2`） |
//! | 5 | 幂等键 + Compaction | ✅ | `idempotency_survives_compaction` |
//! | 6 | 崩溃恢复（各状态点） | ✅ | `crash_recovery_no_data_loss`（5 轮硬崩溃；曾抓出"同一 WAL 目录两个消费者"的重复文件缺陷，已修，见 `operation-log §28.2`） |
//! | 7 | WAL 撕裂 | ✅ | `torn_wal_tail_is_rejected_and_prefix_survives`（曾抓出"撕裂后无法自愈"的缺陷，已在 `WalWriter::open` 截断修复，见 `operation-log §29.1` / R-14） |
//! | 8 | 并发写 + fsync 前/中/后 kill | ✅ | `kill_before_fsync_keeps_acked_data_and_drops_unacked` / `kill_after_fsync_keeps_fsynced_data_without_ack`（靠 `WalConfig.fsync_hook` 注入，掉电模型 = 只有 fsync 过的字节算落盘） |
//! | 9 | `synced_seq`（write 后 fsync 前 kill） | ✅ | `accumulator_never_reads_beyond_synced_seq`（未 fsync 的记录物理在盘上但绝不被吸收） |
//! | 10 | Batch 超时 + segment 释放 | ✅ | `batch_timeout_releases_segment_after_object_store_failure`（对象存储不可写 → 批次卡 Pending → 超时 abort → **segment 回收**） |
//! | 11 | 磁盘水位强制 abort | ✅ | `disk_watermark_aborts_oldest_batch_then_releases_segments`（优先 abort **最老**批次；全部终态后回收） |
//!
//! **"单测有"不等于"通过"**：单测覆盖的是解码/判定的**纯逻辑**，
//! chaos 层要的是真实磁盘 + 跨重启 + 并发下的端到端证据。表里标 ⚠️ 的都还欠这一层。
//!
//! ## 两条写用例的规矩（`operation-log §28.4`）
//!
//! 1. **断言"最终行数"前先 [`wait_hot_drained`]**：`§28.1` 的"提交→标记"窗口内，
//!    同一批数据会同时从文件与热数据两处可见 —— 不等窗口关闭，断言的是瞬时中间态，
//!    会把**系统缺陷**误报成"用例不稳定"。
//! 2. **已知缺陷标 `#[ignore]` + 保留确定性探针**：不写"当前行为"的断言（等于把缺陷
//!    固化成规格），也不静默删掉用例。

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
// 供 `tracker.non_terminal()` 的方法解析（trait 实现已在 ingest 侧）
#[cfg(test)]
use yuntun_wal::cleanup::BatchStateView as _;

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
    build_tuned(wal_dir, store_root, tables, 1, 0).await
}

/// 完整夹具：`rows_threshold` 与 `max_flush_delay_secs` 均可控。
///
/// 为什么需要可调 `rows_threshold`：夹具默认 1 —— 每条批次一进 chunk 就 seal，
/// 于是"**一个 chunk 聚合多个幂等键**"这种形态**根本造不出来**（每条 Data 记录各自成 chunk）。
/// 要验证提交层的键集合去重（`operation-log §39`），必须让多条不同键的记录落进同一个 chunk。
#[cfg(test)]
async fn build_tuned(
    wal_dir: &std::path::Path,
    store_root: &std::path::Path,
    tables: &[(&str, arrow::datatypes::SchemaRef, bool)],
    rows_threshold: usize,
    max_flush_delay_secs: u64,
) -> Setup {
    build_full_with_wal(
        wal_dir,
        store_root,
        tables,
        rows_threshold,
        max_flush_delay_secs,
        yuntun_wal::WalConfig::for_dir(wal_dir),
    )
    .await
}

/// 兼容入口：默认 `rows_threshold = 1`（既有用例的语义），转调 [`build_full_with_wal`]。
#[cfg(test)]
#[allow(dead_code)]
async fn build_full(
    wal_dir: &std::path::Path,
    store_root: &std::path::Path,
    tables: &[(&str, arrow::datatypes::SchemaRef, bool)],
    max_flush_delay_secs: u64,
    wal_cfg: yuntun_wal::WalConfig,
) -> Setup {
    build_full_with_wal(wal_dir, store_root, tables, 1, max_flush_delay_secs, wal_cfg).await
}

/// 同 [`build`]，但可指定"seal → flush 宽限期"。
///
/// 默认 0 = 到期即 flush（既有场景的行为）。**探针需要它足够长**：
/// 这样攒批循环只做"吸收 → seal"，chunk 停在内存里不落盘，
/// 由测试自己驱动 flush —— 才能停在"已提交、未 mark_committed"的窗口内观察。
#[cfg(test)]
async fn build_with_delay(
    wal_dir: &std::path::Path,
    store_root: &std::path::Path,
    tables: &[(&str, arrow::datatypes::SchemaRef, bool)],
    max_flush_delay_secs: u64,
) -> Setup {
    build_full_with_wal(
        wal_dir,
        store_root,
        tables,
        1,
        max_flush_delay_secs,
        yuntun_wal::WalConfig::for_dir(wal_dir),
    )
    .await
}

/// 全参数版本：额外可覆盖 WAL 配置（segment 轮转大小 / 监控间隔 / 水位 / 批次超时）。
///
/// 磁盘水位与批次超时这两类场景**必须**能压小 `segment_max_size`（逼出轮转）
/// 与 `monitor_interval`（否则要等 60s 一轮），所以需要这个入口。
#[cfg(test)]
async fn build_full_with_wal(
    wal_dir: &std::path::Path,
    store_root: &std::path::Path,
    tables: &[(&str, arrow::datatypes::SchemaRef, bool)],
    rows_threshold: usize,
    max_flush_delay_secs: u64,
    wal_cfg: yuntun_wal::WalConfig,
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
    let wal = yuntun_wal::writer::WalWriter::open(wal_cfg, 0).await.unwrap();
    let cfg = IngestorConfig {
        rows_threshold,
        time_threshold_secs: 0,
        max_flush_delay_secs,
        // 不变量：驻留硬兜底必须晚于正常 flush 到期（plan.md §2.2）
        chunk_max_resident_secs: max_flush_delay_secs + 70,
        flush_phase_spread_secs: 0,
        scan_interval: Duration::from_millis(20),
        spill_dir: wal_dir.join("spill"),
        ..Default::default()
    };
    // chunk 层**显式构造**（而不是让 Ingestor 自建）：查询侧必须共享同一实例，
    // 否则"读己之写"根本没接线 —— 夹具与 `Lakehouse::build_with_shutdown` 的装配不一致，
    // 会让所有涉及"未 flush 数据可见性"的断言失真（曾因此漏掉一个重复计数窗口）。
    let chunks = yuntun_chunk::ChunkStore::new(
        cfg.chunk_store_config(),
        yuntun_chunk::MemoryLedger::new("chunk", cfg.chunk_mem_budget),
    );
    let ingestor = Arc::new(Ingestor::with_chunks(
        cfg,
        wal,
        catalog.clone(),
        store,
        chunks.clone(),
    ));
    // 崩溃恢复分流（build 阶段，模拟 Lakehouse::build_with_shutdown 行为）
    ingestor.resume_recovered().await.unwrap();
    let cache = Arc::new(yuntun_query::LocalCatalog::new());
    // 读己之写：查询侧接热数据读侧（同 Lakehouse）
    cache.set_hot_shards("chaos".to_string(), chunks);
    cache.set_nodes(vec!["chaos".to_string()]);
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
///
/// ## 本轮在这里抓到的缺陷（`operation-log §28.2`，已修）
///
/// 夹具接上"热数据读侧"后本用例稳定失败：9 个批次（27 行）恢复后变成
/// **11 个文件（33 行）**，且持久不回落到 27。
///
/// **根因不是恢复逻辑，而是"轮次之间换了消费者"**：上一轮结束时只
/// `shutdown.cancel()`（发信号，**不 await**），而 cancel 不会打断已在执行的一轮
/// （含对象存储写 + WAL fsync）；下一轮随即 `build` 并**在同一份 WAL 目录上起了
/// 新的攒批循环** —— 两个消费者把同一条 Data 各吸收一次、各自 flush，
/// 于是同一条 Data 产出两个文件（flush 日志实测同一 seq 被 flush 2–3 次）。
///
/// 修法两条，缺一不可：
/// 1. **夹具**：`cancel()` 后 `await` 到循环真正退出（见函数尾部注释）；
/// 2. **生产**：`run_accumulator` 增加"退出闸门"——cancel 已置位就不再开新一轮，
///    把"退出瞬间仍落盘一批"的窗口关掉（`plan.md` R-13）。
///
/// 一般化：**节点私有状态（WAL 目录）同一时刻只能有一个消费者**。
/// 恢复/交接流程里最容易违反它，而症状是"静默重复"。
///
/// 诊断开关：`YUNTUN_CHAOS_TRACE=1` 会打印认领集与 WAL 中 Data 的位置。
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
        // [诊断] 恢复后的"认领集" vs WAL 中的 Data seq：认领不覆盖的 Data 会被重放成**第二个文件**
        // [诊断] 恢复后的"认领集" vs WAL 中的 Data seq（`YUNTUN_CHAOS_TRACE=1` 开启）。
        //
        // 保留它的价值：定位"恢复产出重复文件"（`operation-log §28.2`）就是靠这里 ——
        // 直接看到"哪些 Data 没有认领"以及"两个批次共享同一区间"。
        // 平时不开，避免在 CI 输出里刷屏。
        if std::env::var("YUNTUN_CHAOS_TRACE").is_ok() {
            let claims = cur.ingestor.replay_skip.lock().unwrap().clone();
            let data_seqs: Vec<u64> =
                yuntun_wal::reader::WalReader::new(cur.ingestor.wal.shard_dir())
                    .scan_from(0)
                    .unwrap()
                    .into_iter()
                    .filter(|(_, r)| matches!(r, yuntun_model::wal_record::Record::Data(_)))
                    .map(|(s, _)| s)
                    .collect();
            eprintln!(
                "[诊断] round={round} claims={:?} data_seqs={data_seqs:?}",
                claims
                    .iter()
                    .map(|c| format!("{}..{}", c.start, c.end))
                    .collect::<Vec<_>>()
            );
            // WAL 原始记录：看 BatchPending 携带的区间是否与 Data 的真实位置一致
            let dump: Vec<String> = yuntun_wal::reader::WalReader::new(
                cur.ingestor.wal.shard_dir(),
            )
            .scan_from(0)
            .unwrap()
            .iter()
            .map(|(s, r)| {
                use yuntun_model::wal_record::Record as R;
                let b = |id: &str| id[..6.min(id.len())].to_string();
                match r {
                    R::Data(p) => format!("{s}:Data({})", p.table),
                    R::BatchPending(p) => format!(
                        "{s}:Pending({} {}..{})",
                        b(&p.batch_id),
                        p.wal_seq_start,
                        p.wal_seq_end
                    ),
                    R::BatchS3Written(p) => format!("{s}:S3W({})", b(&p.batch_id)),
                    R::BatchCommitted(p) => format!("{s}:Commit({})", b(&p.batch_id)),
                    R::BatchAbort(p) => format!("{s}:Abort({})", b(&p.batch_id)),
                    _ => format!("{s}:other"),
                }
            })
            .collect();
            eprintln!("[诊断] WAL: {dump:?}");
            // 恢复后的批次状态：batch_id 全量 + 区间 + 组键（定位区间为何重复/漏覆盖）
            let rec = cur.ingestor.wal.full_recovery().unwrap();
            let mut states: Vec<String> = rec
                .states
                .states
                .values()
                .map(|st| {
                    format!(
                        "{} {:?} range={:?} shard={} win={} rows={}",
                        &st.batch_id[..8.min(st.batch_id.len())],
                        st.status,
                        st.wal_seq_range,
                        st.shard,
                        st.time_window,
                        st.row_count
                    )
                })
                .collect();
            states.sort();
            eprintln!("[诊断] states: {states:?}");
        }
        let shutdown = CancellationToken::new();
        // accumulator 必须在轮询期间保持运行（它负责重读 WAL → flush → commit）
        let acc = cur.ingestor.clone().spawn_accumulator(shutdown.clone());

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
            // 超时诊断：把"多出来的行"定位到具体承载者（文件 vs 热数据）
            let snap = cur.catalog.current_snapshot().await;
            let files = cur
                .catalog
                .list_visible_files("public.audit", snap, None)
                .await
                .unwrap_or_default();
            let file_rows: u64 = files.iter().map(|f| f.row_count).sum();
            let hot_rows: usize = cur
                .ingestor
                .chunks()
                .read_table_sync("public.audit", snap)
                .iter()
                .map(|b| b.num_rows())
                .sum();
            let listing: Vec<String> = files
                .iter()
                .map(|f| {
                    format!(
                        "{}..{} rows={} shard={}",
                        &f.batch_id[..8.min(f.batch_id.len())],
                        &f.file_path[f.file_path.len().saturating_sub(12)..],
                        f.row_count,
                        f.shard
                    )
                })
                .collect();
            assert!(
                tokio::time::Instant::now() < deadline,
                "round {round}: 恢复超时（acked={acked_rows}, got={got}）\
                 —— files={file_rows} hot={hot_rows} 清单={listing:?}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        // 恢复完成后再停 accumulator（避免中断后续 flush）。
        //
        // ⚠️ **必须 await 到它真正退出**：`cancel()` 只发信号，循环可能正跑完一轮
        // （扫描 → 吸收 → 落盘）；而下一轮会重新 `build` 并起**新的**攒批循环。
        // 两个循环消费**同一份 WAL 目录**时，同一条 Data 会被各吸收一次、各自 flush
        // → **两个文件 = 重复计数**（本轮实测 9 批次产 11 文件 / 33 行）。
        // 这正是"节点私有状态（WAL 目录）同一时刻只能有一个消费者"的具体体现。
        shutdown.cancel();
        let _ = acc.await;
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
    // 精确行数断言前先等热数据退场（否则会撞上 §28.1 的瞬时窗口）
    wait_hot_drained(&setup).await;

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
    count_rows_parts(&setup.catalog, &setup.engine, qualified_table).await
}

/// 同 [`count_rows`]，但按句柄取（供并发任务 spawn 使用）。
#[cfg(test)]
async fn count_rows_parts(
    catalog: &Arc<MemoryCatalog>,
    engine: &Arc<QueryEngine>,
    qualified_table: &str,
) -> u64 {
    engine
        .catalog()
        .refresh(&(catalog.clone() as Arc<dyn CatalogOps>))
        .await
        .unwrap();
    let batches = engine
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

/// 等某 shard 可见文件的**行数合计**达到 `n`（flush 完成判据，比文件数稳）。
#[cfg(test)]
async fn wait_visible_rows(setup: &Setup, table: &str, shard: &str, n: u64) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let snap = setup.catalog.current_snapshot().await;
        let rows: u64 = setup
            .catalog
            .list_visible_files(table, snap, Some(shard))
            .await
            .unwrap_or_default()
            .iter()
            .map(|f| f.row_count)
            .sum();
        if rows >= n || tokio::time::Instant::now() >= deadline {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// 构造 compactor（独立 store 句柄，与 ingestor 指向同一 root）。
#[cfg(test)]
fn compactor(
    catalog: Arc<dyn CatalogOps>,
    store_root: &std::path::Path,
    min_files: usize,
) -> yuntun_compaction::Compactor {
    yuntun_compaction::Compactor {
        cfg: yuntun_compaction::CompactionConfig {
            min_files,
            ..Default::default()
        },
        catalog,
        store: yuntun_store::create_store(&yuntun_store::StoreConfig::Local {
            root: store_root.to_string_lossy().to_string(),
        })
        .unwrap(),
        format: yuntun_format::DataFormat::Parquet,
    }
}

/// 等热数据完全退场：所有 chunk 已提交、且被查询侧 `reclaim` 回收。
///
/// **为什么断言"最终行数"前必须等它**：`operation-log §28.1` 的"提交→标记"窗口内，
/// 同一批数据会**同时**以"已提交文件"和"热数据"两处可见 —— 此刻读到的是瞬时中间态。
/// 断言最终一致性必须等窗口关闭，否则用例会随负载偶发多计（实测 9 行读成 12 行）。
#[cfg(test)]
async fn wait_hot_drained(setup: &Setup) {
    // 上界 120s：这不是延迟断言，只兜"永不退场"。
    // 满负载并行（cargo 默认同时跑所有 test binary）时 flush 可能撞上对象存储 IO 竞争、
    // 失败后走 `note_flush_failure` 的退避重试（最多 30s/次），60s 会不够。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        // 刷新 + **显式回收**。
        //
        // 生产里 `reclaim` 由查询侧刷新调用（`query/src/cache.rs`），但它挂在"版本变化"
        // 的分支上；夹具没有周期刷新任务（`spawn_refresh`），一旦最后一次提交发生在
        // 最后一次刷新之后，chunk 会以 `Flushed` 状态一直驻留 —— 用例就会永久等待
        // （实测 `chunks: 1, flushed: 1` 卡到超时）。这里按生产的语义显式推进一次。
        setup
            .engine
            .catalog()
            .refresh(&(setup.catalog.clone() as Arc<dyn CatalogOps>))
            .await
            .unwrap();
        let snap = setup.catalog.current_snapshot().await;
        setup.ingestor.chunks().reclaim(snap);
        if setup.ingestor.chunks().is_empty() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "热数据未在期限内退场：{:?}",
            setup.ingestor.chunk_stats()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// 该表的 ingest 帮助闭包：固定 shard / 行数，自带时间戳。
#[cfg(test)]
async fn ingest_into(setup: &Setup, table: &str, shard: &str, ts: i64) -> u64 {
    ingest_with_key(setup, table, shard, ts, None).await
}

/// 同 [`ingest_into`]，但可带幂等键。
#[cfg(test)]
async fn ingest_with_key(
    setup: &Setup,
    table: &str,
    shard: &str,
    ts: i64,
    key: Option<&str>,
) -> u64 {
    setup
        .ingestor
        .ingest(IngestBatch {
            table: table.into(),
            shard_key: shard.into(),
            record_batch: batch(ts),
            idempotency_key: key.map(|k| k.to_string()),
            received_at: std::time::SystemTime::now(),
        })
        .await
        .unwrap()
        .row_count
}

/// 取 `EXPLAIN` 的物理计划文本。
#[cfg(test)]
async fn explain_text(setup: &Setup, sql: &str) -> String {
    setup
        .engine
        .catalog()
        .refresh(&(setup.catalog.clone() as Arc<dyn CatalogOps>))
        .await
        .unwrap();
    let batches = setup
        .engine
        .sql(&format!("EXPLAIN {sql}"))
        .await
        .unwrap();
    let mut out = String::new();
    for b in &batches {
        if let Some(col) = b
            .column(b.num_columns() - 1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
        {
            #[allow(unused_imports)]
            use arrow::array::Array as _;
            for i in 0..col.len() {
                out.push_str(col.value(i));
                out.push('\n');
            }
        }
    }
    out
}

/// 把一条 Data 记录**直接写进 segment 文件但不 fsync**（模拟"已 write、未 fsync"）。
///
/// 走 `SegmentWriter`（而非 `WalWriter::append`）才能绕过组提交 fsync —— 这是
/// `§5.3.5.1` 那条边界的唯一可测形态：记录**物理上在盘上**，但**不在**
/// `wal.synced_seq()` 里。返回该记录被 reader 读到的 seq。
#[cfg(test)]
fn write_unsynced_data_record(
    shard_dir: &std::path::Path,
    table: &str,
    shard: &str,
    rows: i64,
    first_seq: u64,
) -> u64 {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    let s = Schema::new(vec![
        Field::new("event_time", DataType::Int64, false),
        Field::new("user", DataType::Utf8, true),
    ]);
    let b = arrow::record_batch::RecordBatch::try_new(
        Arc::new(s.clone()),
        vec![
            Arc::new(Int64Array::from(vec![rows; 3])),
            Arc::new(StringArray::from(vec![Some("x"), None, Some("y")])),
        ],
    )
    .unwrap();
    let mut ipc = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut ipc, &Arc::new(s)).unwrap();
        w.write(&b).unwrap();
        w.finish().unwrap();
    }
    let rec = yuntun_model::wal_record::Record::Data(yuntun_model::wal_record::DataPayload {
        table: table.to_string(),
        shard: shard.to_string(),
        schema_version: 1,
        batch_ipc: ipc,
        client_request_id: String::new(),
        time_window: "1970-01-01T00:00".to_string(),
    });
    // 独立的 segment 文件：不干扰 WalWriter 自己的追加偏移
    let seg_seq = 900_000 + first_seq;
    let mut w = yuntun_wal::segment::SegmentWriter::create(shard_dir, seg_seq, 0, first_seq).unwrap();
    w.append(&rec).unwrap();
    // ⚠️ 刻意**不** `sync_all()`：这正是被测边界
    first_seq
}

/// 最新（序号最大）的 segment 文件路径。
#[cfg(test)]
fn newest_segment(shard_dir: &std::path::Path) -> std::path::PathBuf {
    yuntun_wal::segment::list_segments(shard_dir)
        .unwrap()
        .into_iter()
        .map(|(_, p)| p)
        .max_by_key(|p| {
            p.file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default()
        })
        .expect("至少应有一个 segment")
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
    wait_hot_drained(&setup).await;
    assert_eq!(
        count_rows(&setup, "public.idem").await,
        acked_rows,
        "compaction + 重试后行数不得增长"
    );
    shutdown.cancel();
}

/// T6.1（`design.md` §12.3 #1）：**Compaction 期间查询** —— 无重复、无已删数据。
///
/// 断言用的是两条**全程不变量**，而不是只看首尾：
/// 1. 观测到的行数**单调不减** —— 下降意味着"合并把老文件标删、新文件还没可见"
///    的可见性空洞（架构 §4.5 明令禁止）；
/// 2. 观测到的行数**永不超过已 ack 行数** —— 超出就是重复计数（老文件与新文件、
///    或热数据与已提交文件被算了两遍）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_during_query_keeps_counts_monotonic() {
    let _gate = CHAOS_GATE.lock().await;
    let wal_dir = tmpdir("wal-compact-q");
    let store_root = tmpdir("store-compact-q");
    let setup = build(&wal_dir, &store_root, &[("cq", schema(), false)]).await;
    let shutdown = CancellationToken::new();
    let _acc = setup.ingestor.clone().spawn_accumulator(shutdown.clone());

    // 查询任务：全程持续查询（compaction 就落在这个窗口里）
    let (stop, seen) = (
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
        Arc::new(std::sync::Mutex::new(Vec::<u64>::new())),
    );
    let query_task = {
        let (catalog, engine) = (setup.catalog.clone(), setup.engine.clone());
        let (stop, seen) = (stop.clone(), seen.clone());
        tokio::spawn(async move {
            loop {
                if stop.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                let n = count_rows_parts(&catalog, &engine, "public.cq").await;
                seen.lock().unwrap().push(n);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };

    let c = compactor(setup.catalog.clone(), &store_root, 3);
    let mut acked = 0u64;
    for round in 0..3 {
        // 每轮 3 批 → 3 个文件（rows_threshold=1，每批一个 chunk）
        for i in 0..3 {
            acked += ingest_into(&setup, "cq", "s0", 1_000 + (round * 10 + i) as i64).await;
        }
        assert_eq!(
            wait_visible_rows(&setup, "public.cq", "s0", acked).await,
            acked,
            "round {round}: flush 未在期限内完成"
        );

        let snap = setup.catalog.current_snapshot().await;
        let files_before = setup
            .catalog
            .list_visible_files("public.cq", snap, Some("s0"))
            .await
            .unwrap()
            .len();
        assert_eq!(
            files_before,
            if round == 0 { 3 } else { 4 },
            "round {round}: 每轮 3 批 + 上轮合并产物 1 个"
        );
        let merged = yuntun_compaction::compact_shard(&c, "public.cq", "s0", snap)
            .await
            .unwrap();
        assert!(merged.is_some(), "round {round}: 文件数达阈值应触发合并");
        // 合并后行数必须**立刻**不变（老文件 deleted_at + 新文件 valid_from 原子提交）
        assert_eq!(
            wait_visible_rows(&setup, "public.cq", "s0", acked).await,
            acked,
            "round {round}: 合并后行数不得变化"
        );
        // 老快照仍见合并前的文件（快照隔离：合并不能"删掉"老快照能看到的数据）
        assert_eq!(
            setup
                .catalog
                .list_visible_files("public.cq", snap, Some("s0"))
                .await
                .unwrap()
                .len(),
            files_before,
            "round {round}: 老快照的文件清单不得变化"
        );
        // 新快照只剩合并产物（老文件 deleted_at 生效）
        assert_eq!(
            setup
                .catalog
                .list_visible_files("public.cq", merged.unwrap(), Some("s0"))
                .await
                .unwrap()
                .len(),
            1,
            "round {round}: 新快照应只见合并产物"
        );
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    query_task.await.unwrap();
    let observed = seen.lock().unwrap().clone();
    // 门槛只保证"确实采样到了并发窗口"，不追求样本量：
    // 满负载机器上 10ms 间隔的采样次数会明显变少，卡死数量会把机器快慢当成失败。
    assert!(
        observed.len() >= 3,
        "查询样本太少（{})，无法支撑并发断言",
        observed.len()
    );

    // 并发采样期间**只做有松弛的断言**：`§28.1`（提交→标记窗口）会让采样值瞬时多计
    // 一批；松弛量 = 一轮 3 批 × 3 行 = 9（理论上限 = 同时在飞的 flush 数 × 批行数）。
    // ⚠️ `§28.1` 修好后这里必须收紧为 `max <= acked`。
    if let Some(&max) = observed.iter().max() {
        assert!(
            max <= acked + 9,
            "并发采样超出理论上限：观测 {max} 行 > acked {acked} + 9：{observed:?}"
        );
    }

    // 严格断言放在**静默点**（并发任务已停 + 热数据退场后）：
    // 持久性重复/丢失会在这里露出来（瞬时窗口不会，它由下面这个等待排除掉）。
    wait_hot_drained(&setup).await;
    let settled = count_rows(&setup, "public.cq").await;
    assert_eq!(
        settled, acked,
        "静默后行数必须等于 acked（持久重复或无谓丢失）；并发采样序列={observed:?}"
    );
    shutdown.cancel();
}

/// T6.1 的**确定性探针**：flush 提交成功到 `mark_committed` 之间的可见性。
///
/// `Chunk::visible` 对 `committed_snapshot = None` 返回 `true`（"还没提交，所以对
/// 所有快照可见"）。而 flush 的提交（`commit_files`，产生新快照 S）到调用方
/// `mark_committed(S)` 之间有一段时间 —— 这段时间里同一批数据**同时**可从
/// "已提交文件"（`valid_from = S`）与"热数据"（chunk）读到。
///
/// 这里用 `flush_now`（提交但不标记）把这个窗口**固定下来**，比靠并发去撞它可靠。
///
/// ⚠️ **当前是已知缺陷**（`operation-log §28`）：窗口内查询会把同一批数据算两遍
/// （实测 3 行 → 6 行）。窗口 = 一次 WAL fsync（`BatchCommitted` 的 append），
/// 量级 0.1–5ms，所以症状是**偶发多计**而非稳定错误。
/// 修复需要动 `ShardReader` 接缝（读侧要知道"这个快照里已经有哪些文件"），
/// 与 R3/R4 的 `pull(table, range, known_manifest_ver)`（`refactor.md` S5-4）同向，
/// 故不在本轮打补丁 —— 但**测试先留着**，修好前它必须变绿。
#[ignore = "已知缺陷：提交→标记窗口内重复计数（operation-log §28）"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_to_mark_window_must_not_double_count() {
    let _gate = CHAOS_GATE.lock().await;
    let wal_dir = tmpdir("wal-commit-window");
    let store_root = tmpdir("store-commit-window");
    // 宽限期拉长：攒批循环只做"吸收 + seal"，chunk 留在内存，由测试驱动 flush
    let setup = build_with_delay(&wal_dir, &store_root, &[("cw", schema(), false)], 3600).await;
    let chunks = setup.ingestor.chunks();

    // ① 启动攒批 → 等 chunk 被吸收并 seal → 停掉（此刻还不会 flush）
    let acc_shutdown = CancellationToken::new();
    let acc = setup.ingestor.clone().spawn_accumulator(acc_shutdown.clone());
    assert_eq!(ingest_into(&setup, "cw", "s0", 42).await, 3);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while chunks.stats().chunks == 0 {
        assert!(tokio::time::Instant::now() < deadline, "chunk 未被吸收");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    acc_shutdown.cancel();
    let _ = acc.await;
    assert_eq!(chunks.stats().sealed, 1, "rows_threshold=1 → 应已 seal");
    // 用**当前**时刻（不是远未来）判断：宽限期 3600s 内不应到期
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    assert!(
        chunks.plan_flush(now).flush.is_empty(),
        "宽限期内不得进入 flush 计划（否则测试无法停在窗口里）"
    );

    // ② 取该 chunk 的 flush 输入（需要 chunk id：按热分片键取）
    use yuntun_store::ShardReader;
    let shards = chunks.shards_of("public.cw").await.unwrap();
    assert_eq!(shards.len(), 1, "应只有一个热分片: {shards:?}");
    let key = yuntun_chunk::chunk::ChunkKey::new(
        shards[0].clone(),
        chunks.liveness("public.cw").epoch,
    );
    let ids = chunks.chunk_ids_of(&key);
    assert_eq!(ids.len(), 1, "应只有一个 chunk: {ids:?}");
    let input = chunks.flush_input(ids[0]).unwrap().unwrap();

    // ③ 走完整 flush（含 commit_files）但**不** `mark_committed` —— 固定住窗口
    let out = setup.ingestor.flush_now(input).await.unwrap();

    // ① 文件已可见：快照 S 上能读到 3 行
    let file_rows: u64 = setup
        .catalog
        .list_visible_files("public.cw", out.snapshot, Some("s0"))
        .await
        .unwrap()
        .iter()
        .map(|f| f.row_count)
        .sum();
    assert_eq!(file_rows, 3, "提交后文件应在快照 S 可见");

    // ② 端到端症状（用户可见）：此刻查询只能看到 3 行
    let observed = count_rows(&setup, "public.cw").await;
    assert_eq!(
        observed, 3,
        "提交窗口内查询重复计数：文件 {file_rows} 行 + 热数据被算了第二遍"
    );

    // ③ 机制：同一快照上热数据必须**已经退场**
    let hot_at_s: usize = chunks
        .read_table_sync("public.cw", out.snapshot)
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(
        hot_at_s, 0,
        "快照 S 已包含该批数据的文件，热数据不得同时可见（重复计数窗口）"
    );
}

// ---------------------------------------------------------------- fsync 点位故障（#8）
//
// 断电只会落在两个瞬时状态之一，二者对"数据还在不在"的答案**相反**：
//
// | 点位 | 盘上有什么 | 客户端拿到什么 |
// |---|---|---|
// | `BeforeSync` | 字节**可能从未落盘** | 没有 ack |
// | `AfterSync`  | 字节**确定落盘** | 仍然没有 ack |
//
// 后一行是"至少一次"的来源：客户端没拿到 ack 会重试，而数据其实已在盘上
// —— 所以写入方必须有幂等键（§7.3），否则重试即重复计数。
// 这两个用例把两种断电分别固定下来。

/// 构造"在第 `nth` 次 fsync 的 `point` 点位模拟掉电"的注入钩子。
///
/// **掉电模型：只有 fsync 过的字节算落盘。** `BeforeSync` 时把文件截回
/// `synced_len`（未 fsync 的写入视为从未落盘）；`AfterSync` 时什么都不做（已落盘）。
/// 随后 `panic!` 让提交线程死掉 —— 进程内最接近"进程被杀"的形态：此后 append 全部失败。
#[cfg(test)]
fn power_loss_hook(
    nth: usize,
    point: yuntun_wal::config::FsyncPoint,
) -> (
    yuntun_wal::config::FsyncHook,
    Arc<std::sync::atomic::AtomicUsize>,
) {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let c = calls.clone();
    let hook = yuntun_wal::config::FsyncHook::new(move |ev| {
        if ev.point != point {
            return;
        }
        let n = c.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        if std::env::var("YUNTUN_CHAOS_TRACE").is_ok() {
            eprintln!(
                "[hook] n={n} point={point:?} synced_len={} file_len={} batch_len={}",
                ev.synced_len, ev.file_len, ev.batch_len
            );
        }
        if n != nth {
            return;
        }
        if point == yuntun_wal::config::FsyncPoint::BeforeSync {
            // 未 fsync 的字节从未落盘（掉电的物理后果）
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&ev.path)
                .unwrap();
            f.set_len(ev.synced_len).unwrap();
            f.sync_all().unwrap();
        }
        panic!("simulated power loss at {point:?}");
    });
    (hook, calls)
}

/// 并发写若干批，返回 (已 ack 行数, 失败次数)。
#[cfg(test)]
async fn concurrent_ingest(
    setup: &Setup,
    table: &str,
    n: usize,
) -> (u64, usize) {
    let mut handles = Vec::new();
    for i in 0..n {
        let ing = setup.ingestor.clone();
        let t = table.to_string();
        handles.push(tokio::spawn(async move {
            ing.ingest(IngestBatch {
                table: t,
                shard_key: "s0".into(),
                record_batch: batch(1000 + i as i64),
                idempotency_key: None,
                received_at: std::time::SystemTime::now(),
            })
            .await
        }));
    }
    let (mut acked, mut failed) = (0u64, 0usize);
    for h in handles {
        match h.await.unwrap() {
            Ok(r) => acked += r.row_count,
            Err(_) => failed += 1,
        }
    }
    (acked, failed)
}

/// 重建后等到"数据可见"（或超时），返回观测到的行数。
#[cfg(test)]
async fn recovered_rows(setup: &Setup, table: &str) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        // 攒批循环负责把 WAL 里的 Data 重放出来
        let n = count_rows_parts(&setup.catalog, &setup.engine, table).await;
        if n > 0 || tokio::time::Instant::now() >= deadline {
            return n;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// 恢复出的 **Data** 记录 seq 必须是 `0..n-1` 的连续前缀（中间不能有洞）。
///
/// 只数 `Data`：重启后攒批循环会把它们落盘，其间写入的
/// `BatchPending/S3Written/Committed` 是**控制记录**，会插在 Data 之后。
/// 把它们一起数会得到"条数对不上"的假失败（这正是第一版断言的错误）。
#[cfg(test)]
fn assert_data_seq_prefix(shard_dir: &std::path::Path, expected: usize) {
    let data_seqs: Vec<u64> = yuntun_wal::reader::WalReader::new(shard_dir)
        .scan_from(0)
        .unwrap()
        .into_iter()
        .filter(|(_, r)| matches!(r, yuntun_model::wal_record::Record::Data(_)))
        .map(|(s, _)| s)
        .collect();
    assert_eq!(
        data_seqs.len(),
        expected,
        "恢复出的 Data 记录条数不符：{data_seqs:?}"
    );
    assert_eq!(
        data_seqs,
        (0..expected as u64).collect::<Vec<u64>>(),
        "Data 记录的 seq 必须是连续前缀（有洞 = 中间的数据丢了）"
    );
}

/// T6.8 ①（`design.md` §12.3 #8）：**kill 在 fsync 之前** —— 未 fsync 的写入必须消失，
/// 但**已 ack 的数据一条都不能少**。
///
/// 这是"客户端拿到成功 = 已 fsync"这条契约的**反面断言**：
/// 若实现先 ack 再 fsync（或根本没 fsync），这里的"已 ack 数据全在"就会失败。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_before_fsync_keeps_acked_data_and_drops_unacked() {
    let _gate = CHAOS_GATE.lock().await;
    let wal_dir = tmpdir("wal-kill-before");
    let store_root = tmpdir("store-kill-before");
    let (hook, calls) = power_loss_hook(3, yuntun_wal::config::FsyncPoint::BeforeSync);
    let mut wcfg = yuntun_wal::WalConfig::for_dir(&wal_dir);
    wcfg.group_commit_max_batch = 1; // 每条记录自成一批 → 一次 append 一次 fsync，点位可数
    wcfg.fsync_hook = Some(hook);
    let setup = build_full(
        &wal_dir,
        &store_root,
        &[("k8", schema(), false)],
        0,
        wcfg,
    )
    .await;
    let shard_dir = setup.ingestor.wal.shard_dir();

    let (acked, failed) = concurrent_ingest(&setup, "k8", 6).await;
    assert!(failed > 0, "注入点必须让至少一次写入失败，否则没测到 kill");
    assert!(
        calls.load(std::sync::atomic::Ordering::SeqCst) >= 3,
        "钩子至少应被触发 3 次"
    );
    assert_eq!(acked, 6, "前两批（各 3 行）应已 ack");
    drop(setup);

    // 重启：只有 fsync 过的字节能恢复
    let setup = build_full(
        &wal_dir,
        &store_root,
        &[("k8", schema(), false)],
        0,
        yuntun_wal::WalConfig::for_dir(&wal_dir),
    )
    .await;
    let shutdown = CancellationToken::new();
    let _acc = setup.ingestor.clone().spawn_accumulator(shutdown.clone());
    let got = recovered_rows(&setup, "public.k8").await;
    assert_eq!(
        got, acked,
        "掉电后**已 ack 的数据一条都不能少**；未 ack 的字节从未落盘，不该出现（acked={acked} got={got}）"
    );
    // 无空洞：恢复出的 Data 记录必须是 0..n-1 的连续前缀
    assert_data_seq_prefix(&shard_dir, 2);
    // 重建后的系统必须仍可写入（注入点在重启后已卸掉）
    let more = ingest_into(&setup, "k8", "s1", 2000).await;
    assert_eq!(wait_visible_rows(&setup, "public.k8", "s1", more).await, more);
    shutdown.cancel();
}

/// T6.8 ②（`design.md` §12.3 #8）：**kill 在 fsync 之后、ack 之前** ——
/// 数据**确定在盘上**（必须恢复），但客户端**没拿到成功**。
///
/// 这正是"至少一次 + 幂等键"的来源：客户端会重试这笔写入，而数据其实已经落盘 ——
/// 幂等键（§7.3）就是为此存在的。用例把两侧都钉住：
/// 恢复必须含这批数据，且该次写入必须**没有** ack（否则"至少一次"就不成立）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_after_fsync_keeps_fsynced_data_without_ack() {
    let _gate = CHAOS_GATE.lock().await;
    let wal_dir = tmpdir("wal-kill-after");
    let store_root = tmpdir("store-kill-after");
    let (hook, calls) = power_loss_hook(3, yuntun_wal::config::FsyncPoint::AfterSync);
    let mut wcfg = yuntun_wal::WalConfig::for_dir(&wal_dir);
    wcfg.group_commit_max_batch = 1;
    wcfg.fsync_hook = Some(hook);
    let setup = build_full(
        &wal_dir,
        &store_root,
        &[("k8", schema(), false)],
        0,
        wcfg,
    )
    .await;
    let shard_dir = setup.ingestor.wal.shard_dir();

    let (acked, failed) = concurrent_ingest(&setup, "k8", 6).await;
    assert!(failed > 0, "被 kill 的那一次写入必须没有 ack（客户端会重试）");
    assert!(calls.load(std::sync::atomic::Ordering::SeqCst) >= 3);
    assert_eq!(acked, 6, "前两批应已 ack；第 3 批已 fsync 但未 ack");
    drop(setup);

    let setup = build_full(
        &wal_dir,
        &store_root,
        &[("k8", schema(), false)],
        0,
        yuntun_wal::WalConfig::for_dir(&wal_dir),
    )
    .await;
    let shutdown = CancellationToken::new();
    let _acc = setup.ingestor.clone().spawn_accumulator(shutdown.clone());
    let got = recovered_rows(&setup, "public.k8").await;
    assert_eq!(
        got,
        acked + 3,
        "已 fsync 的数据必须恢复（哪怕客户端没拿到 ack）：acked={acked} got={got}"
    );
    assert_data_seq_prefix(&shard_dir, 3);
    shutdown.cancel();
}

// ---------------------------------------------------------------- 磁盘保护与批次超时

/// 把目录改成只读（**模拟对象存储不可用**）。
///
/// 用权限而不是 mock：路径与"真实落盘语义"一致（chaos 本就用真实磁盘），
/// 且不需要给 `object_store` 写一整套失败替身。
/// ⚠️ 以 root 运行时 chmod 不生效 —— 因此用例里有"必须产生非终态批次"的前置断言，
/// 夹具失效会立刻报出来，而不是让用例悄悄变成空测试。
#[cfg(test)]
fn set_readonly(dir: &std::path::Path, read_only: bool) {
    use std::os::unix::fs::PermissionsExt;
    let mode = if read_only { 0o555 } else { 0o755 };
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(mode)).unwrap();
}

/// 固定水位的盘用量探针（模拟"磁盘填到 X%"）。
#[cfg(test)]
struct FixedUsage(f64);

#[cfg(test)]
impl yuntun_wal::cleanup::DiskUsage for FixedUsage {
    fn usage(&self) -> f64 {
        self.0
    }
}

/// 等 `cond` 成立（超时即失败）。cond 为真时返回它。
#[cfg(test)]
async fn wait_until<T>(what: &str, mut cond: impl FnMut() -> Option<T>) -> T {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(v) = cond() {
            return v;
        }
        assert!(tokio::time::Instant::now() < deadline, "等待超时：{what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// T6.10（`design.md` §12.3 #10）：**Batch 超时 + segment 释放**。
///
/// 场景：**对象存储写不进去**（把 store 根目录置为只读，等价于"S3 永久不可用"）。
/// flush 走在"写 S3"这一步失败 → 批次停在 `Pending`（非终态）。
/// 之后监控线程应按 `batch_timeout` 写 `BatchAbort`，把批次变成终态，
/// 并**回收其 segment** —— `§5.3.6.1` 要的正是"垃圾桶（segment）别一直涨"。
///
/// ⚠️ 这里同时验证一个曾经的缺口：监控线程写了 `BatchAbort` 却**不更新视图**，
/// 于是"已放弃的批次"仍留在 `non_terminal()` 里，它的 `wal_seq_range` 会永远挡住
/// segment 释放 —— 磁盘只增不减（已修：`BatchStateView::note_abort`）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_timeout_releases_segment_after_object_store_failure() {
    let _gate = CHAOS_GATE.lock().await;
    let wal_dir = tmpdir("wal-batch-timeout");
    let store_root = tmpdir("store-batch-timeout");
    let mut wal_cfg = yuntun_wal::WalConfig::for_dir(&wal_dir);
    wal_cfg.segment_max_size = 2048; // 小 segment → 强制轮转（否则无从验证"释放"）
    wal_cfg.batch_timeout = Duration::from_millis(150);
    wal_cfg.monitor_interval = Duration::from_millis(20);
    let setup = build_full(
        &wal_dir,
        &store_root,
        &[("bt", schema(), false)],
        0,
        wal_cfg,
    )
    .await;
    let shard_dir = setup.ingestor.wal.shard_dir();
    let shutdown = CancellationToken::new();
    let acc = setup.ingestor.clone().spawn_accumulator(shutdown.clone());

    // ① 让对象存储不可写（= S3 永久不可用），再写入几批
    set_readonly(&store_root, true);
    for i in 0..3 {
        ingest_into(&setup, "bt", "s0", 900 + i).await;
    }

    // ② 前置：flush 必须真的卡成非终态（否则夹具失效，用例会变成空测试）
    let tracker = setup.ingestor.tracker.clone();
    wait_until("flush 未产生非终态批次（S3 失败路径没跑到）", || {
        let nt = tracker.non_terminal();
        (!nt.is_empty()).then_some(nt)
    })
    .await;

    let before = yuntun_wal::segment::list_segments(&shard_dir).unwrap().len();
    assert!(
        before >= 2,
        "需要轮转出多个 segment 才能验证释放（实测 {before} 个，检查 segment_max_size）"
    );

    // ③ 启动超时监控（真实装配里由 `Lakehouse::spawn_background` 启动）
    let segments = setup.ingestor.wal.full_recovery().unwrap().segments;
    let monitor = yuntun_wal::cleanup::spawn_timeout_monitor(
        setup.ingestor.wal.clone(),
        tracker.clone(),
        None, // 水位不参与本用例
        segments,
        shutdown.clone(),
    );

    // ④ 批次超时 → 全部终态（视图同步）
    wait_until("批次未被 abort", || {
        tracker.non_terminal().is_empty().then_some(())
    })
    .await;

    // ⑤ segment 回收：所有批次终态 → 非活跃 segment 可删（再等几轮监控 tick 执行清理）
    wait_until("非活跃 segment 未被回收（磁盘会只增不减）", || {
        let n = yuntun_wal::segment::list_segments(&shard_dir).unwrap().len();
        (n == 1).then_some(n)
    })
    .await;

    shutdown.cancel();
    let _ = monitor.await;
    let _ = acc.await;
    set_readonly(&store_root, false); // 复原，便于目录清理
}

/// T6.11（`design.md` §12.3 #11）：**磁盘水位** —— 人为把水位抬到 95%，
/// 验证"强制 abort **最老**的未完成批次"，且最终所有批次终态后 segment 被回收。
///
/// 与 `#10` 共用夹具（对象存储不可用 → 批次停在 Pending），
/// 区别在触发条件：这里是**水位**（batch_timeout 设成 1h，确保只走水位这条路）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disk_watermark_aborts_oldest_batch_then_releases_segments() {
    let _gate = CHAOS_GATE.lock().await;
    let wal_dir = tmpdir("wal-watermark");
    let store_root = tmpdir("store-watermark");
    let mut wal_cfg = yuntun_wal::WalConfig::for_dir(&wal_dir);
    wal_cfg.segment_max_size = 2048;
    wal_cfg.batch_timeout = Duration::from_secs(3600); // 只测水位，隔离批次超时
    wal_cfg.monitor_interval = Duration::from_millis(20);
    wal_cfg.disk_high_watermark = 0.80;
    let setup = build_full(
        &wal_dir,
        &store_root,
        &[("wm", schema(), false)],
        0,
        wal_cfg,
    )
    .await;
    let shard_dir = setup.ingestor.wal.shard_dir();
    let shutdown = CancellationToken::new();
    let acc = setup.ingestor.clone().spawn_accumulator(shutdown.clone());

    set_readonly(&store_root, true);
    for i in 0..3 {
        ingest_into(&setup, "wm", "s0", 950 + i).await;
    }
    let tracker = setup.ingestor.tracker.clone();
    let initial = wait_until("flush 未产生非终态批次", || {
        let nt = tracker.non_terminal();
        (nt.len() >= 2).then_some(nt)
    })
    .await;

    // 最老的批次 = created_at_ms 最小者（监控线程也按这个排序挑人）
    let oldest_id = initial
        .iter()
        .min_by_key(|s| s.created_at_ms)
        .unwrap()
        .batch_id
        .clone();

    let segments = setup.ingestor.wal.full_recovery().unwrap().segments;
    let monitor = yuntun_wal::cleanup::spawn_timeout_monitor(
        setup.ingestor.wal.clone(),
        tracker.clone(),
        Some(Arc::new(FixedUsage(0.95))), // > 80% 水位
        segments,
        shutdown.clone(),
    );

    // ① 第一个被 abort 的必须是**最老的**那个批次
    let after_first = wait_until("水位未触发任何 abort", || {
        let nt = tracker.non_terminal();
        (nt.len() < initial.len()).then_some(nt)
    })
    .await;
    assert!(
        !after_first.iter().any(|s| s.batch_id == oldest_id),
        "水位应优先 abort 最老的批次（{oldest_id} 仍在：{:?}）",
        after_first.iter().map(|s| &s.batch_id).collect::<Vec<_>>()
    );

    // ② 水位持续超限 → 逐个清空；全部终态后 segment 应被回收
    wait_until("批次未被全部 abort", || {
        tracker.non_terminal().is_empty().then_some(())
    })
    .await;
    wait_until("非活跃 segment 未被回收", || {
        let n = yuntun_wal::segment::list_segments(&shard_dir).unwrap().len();
        (n == 1).then_some(n)
    })
    .await;

    // ③ WAL 侧的终态必须与视图一致（视图同步不是"只改内存"）
    let rec = setup.ingestor.wal.full_recovery().unwrap();
    assert!(
        rec.states.states.values().all(|s| s.is_terminal()),
        "WAL 恢复出的批次应全部终态：{:?}",
        rec.states
            .states
            .iter()
            .map(|(id, s)| (id.clone(), s.status))
            .collect::<Vec<_>>()
    );

    shutdown.cancel();
    let _ = monitor.await;
    let _ = acc.await;
    set_readonly(&store_root, false);
}

// ---------------------------------------------------------------- WAL 故障语义

/// T6.7（`design.md` §12.3 #7）：**WAL 撕裂** —— 截断文件尾部，CRC 必须拦下它。
///
/// # 这个用例断言的是"崩溃一致性"，不是"持久性"
/// 截断已经 fsync 过的字节 = 人为违反 fsync 承诺（模拟介质损坏/写入丢失）。
/// 此时**允许**丢掉截断点之后的那条记录，但绝不允许：
/// 1. 恢复报错 / panic；
/// 2. 把一条半截记录当成完整记录应用（半个批次入账）；
/// 3. 撕裂点之前的完整记录缺失；
/// 4. **恢复后系统不可用**。
///
/// # 本用例抓到的缺陷（已修，`operation-log §29.1` / R-14）
/// 修复前：截断尾部后重启，再写入一批（fsync 成功、ack 正常），该数据
/// **永远不可见** —— 因为 `SegmentWriter::open_append` 以 `metadata().len()`
/// 作追加偏移，新记录被写在**撕裂的垃圾字节之后**，而 replay 扫到撕裂点即停止。
/// 节点表现为「写入全部成功、数据全部消失」，静默且永久。
///
/// 修法：接管 WAL 目录时（`WalWriter::open`）先把每个 segment
/// **截断到最后一条完整记录的边界**（WAL 标准修复步骤）。
/// 本用例现在断言的就是"修复后 prefix 全恢复 + 新写入可见"。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn torn_wal_tail_is_rejected_and_prefix_survives() {
    let _gate = CHAOS_GATE.lock().await;
    let wal_dir = tmpdir("wal-torn");
    let store_root = tmpdir("store-torn");
    let setup = build(&wal_dir, &store_root, &[("torn", schema(), false)]).await;

    // ① 只写 3 批、**不启动攒批循环** —— WAL 里就只有 3 条 Data 记录，
    //    撕裂点与期望值因此完全确定（不掺 flush 记录，也就没有"撕到哪条"的歧义）。
    let mut acked = 0u64;
    for i in 0..3 {
        acked += ingest_into(&setup, "torn", "s0", 500 + i).await;
    }
    assert_eq!(acked, 9);
    let shard_dir = setup.ingestor.wal.shard_dir();
    drop(setup); // 模拟崩溃：不 flush、不等 fsync

    // ③ 撕裂：砍掉最新 segment 的尾部若干字节
    let seg = newest_segment(&shard_dir);
    let len_before = std::fs::metadata(&seg).unwrap().len();
    let data = std::fs::read(&seg).unwrap();
    std::fs::write(&seg, &data[..data.len() - 10]).unwrap();
    assert_eq!(std::fs::metadata(&seg).unwrap().len(), len_before - 10);

    // ④ 恢复：必须"能恢复"，而不是"恢复失败"
    let cfg = yuntun_wal::config::WalConfig::for_dir(&wal_dir);
    let rec = yuntun_wal::recovery::recover(&cfg, 0, false);
    assert!(
        rec.is_ok(),
        "撕裂应由 CRC 拦截并停在边界，而不是让恢复整体失败：{:?}",
        rec.err()
    );

    // ⑤ 撕裂后"仍可读的 Data 记录数"：恢复出的行数必须严格等于它 × 每批行数。
    //    这是本用例的核心不变式 —— **不允许出现半个批次**（CRC 拦截点必在记录边界）。
    let readable_data = yuntun_wal::reader::WalReader::new(&shard_dir)
        .scan_from(0)
        .unwrap()
        .into_iter()
        .filter(|(_, r)| matches!(r, yuntun_model::wal_record::Record::Data(_)))
        .count() as u64;
    assert!(
        readable_data * 3 < acked,
        "截断必须真的撕掉至少一条记录（否则用例没测到撕裂）：readable={readable_data}"
    );
    assert!(readable_data >= 1, "截断不应把整条 WAL 都废掉");

    // 重建（`WalWriter::open` 会先修复撕裂尾部）：
    let setup = build(&wal_dir, &store_root, &[("torn", schema(), false)]).await;

    // ④ 撕裂点之前的完整记录**一条都不能少**（修复后 prefix 必须全部恢复）
    let shutdown2 = CancellationToken::new();
    let acc2 = setup.ingestor.clone().spawn_accumulator(shutdown2.clone());
    assert_eq!(
        wait_visible_rows(&setup, "public.torn", "s0", readable_data * 3).await,
        readable_data * 3,
        "撕裂点之前的完整记录必须全部恢复（readable={readable_data}）"
    );

    // ⑤ 系统仍可用：继续写入 → 可见。
    //    修复前这里必然失败：新记录被写在撕裂的垃圾字节之后，replay 扫不到
    //    →「写入成功、数据不可见」（`operation-log §29.1`）。
    let more = ingest_into(&setup, "torn", "s1", 700).await;
    assert_eq!(
        wait_visible_rows(&setup, "public.torn", "s1", more).await,
        more,
        "撕裂恢复后必须能继续正常写入并可见"
    );
    wait_hot_drained(&setup).await;
    assert_eq!(
        count_rows_parts(&setup.catalog, &setup.engine, "public.torn").await,
        readable_data * 3 + more,
        "总数 = 撕裂前的完整记录 + 新写入；撕裂那条不允许以半个批次形式出现"
    );

    shutdown2.cancel();
    let _ = acc2.await;
}

/// T6.9（`design.md` §12.3 #9）：**`synced_seq` 边界** —— `write()` 之后、`fsync()`
/// 之前崩溃（或此刻仍在进行中），攒批线程**绝不**读到它。
///
/// # 可测形态
/// 记录**物理上已经在 segment 文件里**（reader 能读到），但**不在**
/// `wal.synced_seq()` 里 —— 攒批循环的读上界是 `synced_seq + 1`，
/// 因此它必须对这条记录视而不见（`§5.3.5.1` / C2）。
///
/// 这正是"可见性"与"持久性"分离的那条线：**只有 fsync 成功的字节才算数**。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accumulator_never_reads_beyond_synced_seq() {
    let _gate = CHAOS_GATE.lock().await;
    let wal_dir = tmpdir("wal-synced");
    let store_root = tmpdir("store-synced");
    // 宽限期拉长：攒批循环**只吸收不落盘** —— 于是它不会追加新记录、`synced_seq`
    // 不再前进，幽灵记录就稳稳停在**读窗口之外**（而不是因为 seq 错位才被跳过）。
    let setup = build_with_delay(&wal_dir, &store_root, &[("sq", schema(), false)], 3600).await;

    // ① 先正常写 3 批（fsync 成功）—— 这是"算数"的部分
    let mut acked = 0u64;
    for i in 0..3 {
        acked += ingest_into(&setup, "sq", "s0", 800 + i).await;
    }
    assert_eq!(acked, 9);
    let synced = setup.ingestor.wal.synced_seq();

    // ② 再"写一条但绝不 fsync"：走 SegmentWriter，绕过组提交，落在**真实尾部**
    let shard_dir = setup.ingestor.wal.shard_dir();
    let ghost_seq = synced + 1;
    write_unsynced_data_record(&shard_dir, "public.sq", "s0", 999, ghost_seq);

    // 前提校验：它**确实在盘上**（否则这个用例什么都没测到）
    let on_disk: Vec<u64> = yuntun_wal::reader::WalReader::new(&shard_dir)
        .scan_from(0)
        .unwrap()
        .into_iter()
        .map(|(s, _)| s)
        .collect();
    assert!(
        on_disk.contains(&ghost_seq),
        "未 fsync 的记录应能被 reader 物理读到（否则测的不是 fsync 边界）：{on_disk:?}"
    );
    assert_eq!(
        setup.ingestor.wal.synced_seq(),
        synced,
        "writer 不得把未 fsync 的记录计入 synced_seq"
    );

    // ③ 攒批循环跑起来：只应吸收 [0, synced] 的部分
    let shutdown = CancellationToken::new();
    let acc = setup.ingestor.clone().spawn_accumulator(shutdown.clone());
    // 宽限期内**不会有文件**，所以等的是"chunk 吸收完成"而不是文件可见
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while setup.ingestor.chunk_stats().chunks == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "已 fsync 的 3 批未被吸收进 chunk：{:?}",
            setup.ingestor.chunk_stats()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // 多跑几轮，给"误读"充分机会
    tokio::time::sleep(Duration::from_millis(300)).await;

    // 正常数据走**热数据读侧**可见（宽限期内还没落盘）—— 证明"没读到幽灵"不是"什么都没读到"
    let got = count_rows_parts(&setup.catalog, &setup.engine, "public.sq").await;
    assert_eq!(
        got, acked,
        "攒批线程读到了未 fsync 的数据（多出 {} 行）—— synced_seq 边界失守",
        got as i64 - acked as i64
    );
    // 更锐的断言：幽灵记录**自己的数据**（event_time = 999）一行都不能出现。
    // 只比总数会漏掉"恰好一进一出"的情形，这里直接盯住那批数据。
    let ghost_rows = setup
        .engine
        .sql("SELECT count(*) FROM yuntun.public.sq WHERE event_time = 999")
        .await
        .unwrap();
    assert_eq!(
        ghost_rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0,
        "未 fsync 的幽灵批次被吸收了"
    );
    assert_eq!(
        setup.ingestor.wal.synced_seq(),
        synced,
        "本用例期间不应有任何新 fsync（否则幽灵记录会滑进读窗口）"
    );
    assert!(
        setup.ingestor.absorbed_seq() <= synced + 1,
        "吸收游标越过 fsync 边界：absorbed={} synced={synced}",
        setup.ingestor.absorbed_seq()
    );
    // 而正常数据（已 fsync）必须可见 —— 否则"没读到幽灵"可能是"什么都没读到"
    assert_eq!(got, acked, "已 fsync 的数据必须可见（读己之写）");
    shutdown.cancel();
    let _ = acc.await;
}

/// T6.4（`design.md` §12.3 #4）：**Schema 变更 + 谓词下推** ——
/// 加列后查询不崩，且过滤**确实下推到扫描**（而不是拿回来再过滤）。
///
/// 为什么必须用 `EXPLAIN` 而不是"结果对"：谓词没下推时结果**也是对的**，
/// 只是把整个文件读回来再过滤 —— 在分布式阶段这就是"每个节点搬全量数据"。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schema_evolution_keeps_predicate_pushed_down() {
    let _gate = CHAOS_GATE.lock().await;
    let wal_dir = tmpdir("wal-pushdown");
    let store_root = tmpdir("store-pushdown");
    let setup = build(&wal_dir, &store_root, &[("pd", schema(), false)]).await;
    let shutdown = CancellationToken::new();
    let _acc = setup.ingestor.clone().spawn_accumulator(shutdown.clone());

    // ① v1：只有 event_time / user（即 `schema()`）
    for i in 0..3 {
        ingest_into(&setup, "pd", "s0", 100 + i).await;
    }
    // ② v2：加一列 extra（写入侧 OCC 演进）—— 旧文件缺列，查询侧应 null 填充
    for i in 0..3 {
        setup
            .ingestor
            .ingest(IngestBatch {
                table: "pd".into(),
                shard_key: "s0".into(),
                record_batch: arrow::record_batch::RecordBatch::try_new(
                    Arc::new(arrow::datatypes::Schema::new(vec![
                        arrow::datatypes::Field::new(
                            "event_time",
                            arrow::datatypes::DataType::Int64,
                            false,
                        ),
                        arrow::datatypes::Field::new("user", arrow::datatypes::DataType::Utf8, true),
                        arrow::datatypes::Field::new("extra", arrow::datatypes::DataType::Utf8, true),
                    ])),
                    vec![
                        Arc::new(Int64Array::from(vec![200 + i; 3])),
                        Arc::new(arrow::array::StringArray::from(vec![
                            Some("x"),
                            None,
                            Some("y"),
                        ])),
                        Arc::new(arrow::array::StringArray::from(vec![
                            Some("e0"),
                            None,
                            Some("e1"),
                        ])),
                    ],
                )
                .unwrap(),
                idempotency_key: None,
                received_at: std::time::SystemTime::now(),
            })
            .await
            .unwrap();
    }
    assert_eq!(wait_visible_rows(&setup, "public.pd", "s0", 18).await, 18);
    wait_hot_drained(&setup).await;

    // ③ 结果正确：两个 schema_version 的文件都读得到，缺失列 null 填充
    assert_eq!(
        count_rows_parts(&setup.catalog, &setup.engine, "public.pd").await,
        18,
        "两个 schema_version 的文件都应可见"
    );
    let extra = setup
        .engine
        .sql("SELECT count(*) FROM yuntun.public.pd WHERE extra IS NOT NULL")
        .await
        .unwrap();
    assert_eq!(
        extra[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        6,
        "v1 文件缺列应填 null（v2 的 3 批 × 每批 2 行非空 = 6）"
    );

    // ④ 物理计划：过滤必须**下推到扫描**（而不是把整个文件读回来再过滤）。
    let plan = explain_text(
        &setup,
        "SELECT count(*) FROM public.pd WHERE event_time > 150",
    )
    .await;
    assert!(
        plan.contains("partial_filters=[") || plan.contains("predicate="),
        "谓词未下推：扫描节点上应出现 `partial_filters=[...]`（或 `predicate=`）：\n{plan}"
    );
    assert!(
        plan.contains("event_time > Int64(150)") || plan.contains("event_time@0 > 150"),
        "下推的应是**我们的谓词本身**，不能只是别的表达式：\n{plan}"
    );
    // 说明（不写成断言，免得把现状固化）：`table.rs` 的
    // `supports_filters_pushdown` 目前一律返回 `Inexact`（注释写明是 MVP 的保守选择），
    // 因此计划里**仍会保留 `FilterExec`** —— 与 `design.md` §12.3 #4 期望的
    // "FilterExec 被消除" 有差距。
    //
    // ⚠️ 想消除它，必须让 `scan` 把 `filters` **转发给 Parquet 源**（由 Parquet 做行级过滤），
    // 否则直接改成 `Exact` 会让 DataFusion 撤掉 FilterExec 而没人过滤 → **静默漏过滤**。
    // 登记为后续项（与 R5 的块级剪枝/谓词下推同批）。
    shutdown.cancel();
}

/// T6.2（`design.md` §12.3 #2）：**分片移除期间查询** —— `valid_from`/`deleted_at`
/// 过滤正确，且老快照仍受快照隔离保护。
///
/// 断言：观测值只能是"移除前"或"移除后"两个**应有值**之一 ——
/// 出现任何中间值都意味着过滤用错了字段（例如把 `deleted_at` 当成"立即不可见"
/// 而让老快照也看不见，或反过来让新快照仍看得见）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shard_removal_during_query_filters_by_deleted_at() {
    let _gate = CHAOS_GATE.lock().await;
    let wal_dir = tmpdir("wal-drop-shard");
    let store_root = tmpdir("store-drop-shard");
    let setup = build(&wal_dir, &store_root, &[("ds", schema(), false)]).await;
    let shutdown = CancellationToken::new();
    let _acc = setup.ingestor.clone().spawn_accumulator(shutdown.clone());

    // 两个 shard 各 3 个文件（各 9 行）
    let mut total = 0u64;
    for (shard, base) in [("s0", 100i64), ("s1", 200i64)] {
        for i in 0..3 {
            total += ingest_into(&setup, "ds", shard, base + i).await;
        }
    }
    assert_eq!(total, 18);
    assert_eq!(wait_visible_rows(&setup, "public.ds", "s0", 9).await, 9);
    assert_eq!(wait_visible_rows(&setup, "public.ds", "s1", 9).await, 9);

    wait_hot_drained(&setup).await; // 精确行数断言前先等热数据退场（§28.1）
    let before = count_rows(&setup, "public.ds").await;
    assert_eq!(before, 18, "移除前两个 shard 都应可见");

    // 移除期间持续查询
    let (stop, seen) = (
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
        Arc::new(std::sync::Mutex::new(Vec::<u64>::new())),
    );
    let query_task = {
        let (catalog, engine) = (setup.catalog.clone(), setup.engine.clone());
        let (stop, seen) = (stop.clone(), seen.clone());
        tokio::spawn(async move {
            loop {
                if stop.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                let n = count_rows_parts(&catalog, &engine, "public.ds").await;
                seen.lock().unwrap().push(n);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };

    let snap_before = setup.catalog.current_snapshot().await;
    let removed = setup.catalog.drop_shard("public.ds", "s0").await.unwrap();
    assert_eq!(removed, 3, "应标记 3 个 s0 文件 deleted_at");
    tokio::time::sleep(Duration::from_millis(200)).await; // 让查询采样到移除后的状态
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    query_task.await.unwrap();

    let observed = seen.lock().unwrap().clone();
    // 观测值必须落在两个应有值之内（18 = 移除前，9 = 移除后）
    for n in &observed {
        assert!(
            *n == 18 || *n == 9,
            "分片移除期间出现非法可见行数 {n}（应为 18 或 9）：{observed:?}"
        );
    }
    assert!(
        observed.iter().filter(|n| **n == 9).count() > 0,
        "移除后应至少有一次查询看到 9 行：{observed:?}"
    );

    // 老快照仍见 s0 的三个文件（快照隔离 + deleted_at 只对未来生效）
    assert_eq!(
        setup
            .catalog
            .list_visible_files("public.ds", snap_before, Some("s0"))
            .await
            .unwrap()
            .len(),
        3
    );
    let now_snap = setup.catalog.current_snapshot().await;
    assert!(
        setup
            .catalog
            .list_visible_files("public.ds", now_snap, Some("s0"))
            .await
            .unwrap()
            .is_empty(),
        "新快照不得再看到已移除的 shard"
    );
    assert_eq!(
        setup
            .catalog
            .list_visible_files("public.ds", now_snap, Some("s1"))
            .await
            .unwrap()
            .len(),
        3,
        "另一个 shard 不得被误伤"
    );
    assert_eq!(count_rows(&setup, "public.ds").await, 9);
    shutdown.cancel();
}

/// T6.3（`design.md` §12.3 #3）：**孤儿清理不误删** —— 两条防线。
///
/// 1. 对账：Meta 已知的 batch_id 一个都不能删（删了就是丢数据），
///    未知的文件（"写 S3 成功但 CommitFiles 未落地"的残留）要回收；
/// 2. **静置期**：`grace` 内的孤儿**绝不**删除 —— 多写者场景下那是别家
///    "已上传、还没提交"的在途文件（`plan.md §5.3-4` / R6 的 T14.3）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orphan_cleanup_spares_known_and_inflight_files() {
    let _gate = CHAOS_GATE.lock().await;
    let wal_dir = tmpdir("wal-orphan");
    let store_root = tmpdir("store-orphan");
    let setup = build(&wal_dir, &store_root, &[("orph", schema(), false)]).await;
    let shutdown = CancellationToken::new();
    let _acc = setup.ingestor.clone().spawn_accumulator(shutdown.clone());

    // ① 两个"已知"文件（batch_id 已进 Meta）
    for i in 0..2 {
        ingest_into(&setup, "orph", "s0", 300 + i).await;
    }
    assert_eq!(wait_visible_rows(&setup, "public.orph", "s0", 6).await, 6);
    let known: Vec<String> = setup.catalog.known_batch_ids().await.unwrap().into_iter().collect();
    assert_eq!(known.len(), 2, "应有两个已提交批次: {known:?}");

    let store = yuntun_store::create_store(&yuntun_store::StoreConfig::Local {
        root: store_root.to_string_lossy().to_string(),
    })
    .unwrap();
    let catalog: Arc<dyn CatalogOps> = setup.catalog.clone();

    // ② 残留文件（模拟"写 S3 成功、CommitFiles 前崩溃"）
    let orphan_id = format!("orphan-{}", uuid::Uuid::now_v7());
    let (orphan_path, _, _) = yuntun_format::write_batch(
        &store,
        "public.orph",
        "s0",
        "w",
        &orphan_id,
        &batch(1),
        yuntun_format::DataFormat::Parquet,
    )
    .await
    .unwrap();

    // ③ grace=0 的清理：孤儿回收，已知文件一个不少
    let cleanup_shutdown = CancellationToken::new();
    let cleaner = yuntun_compaction::spawn_orphan_cleanup_with_interval(
        store.clone(),
        catalog.clone(),
        "yuntun/".to_string(),
        Duration::ZERO,
        Duration::from_millis(20),
        cleanup_shutdown.clone(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let objs = yuntun_format::list_objects(&store, "yuntun/").await.unwrap();
        if !objs.iter().any(|(p, _)| p == &orphan_path) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "孤儿文件未被回收: {orphan_path}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let objs = yuntun_format::list_objects(&store, "yuntun/").await.unwrap();
    for id in &known {
        assert!(
            objs.iter().any(|(p, _)| p.contains(id.as_str())),
            "**误删已知文件**（batch_id={id}）—— 这是丢数据，不是垃圾回收；剩余对象: {objs:?}"
        );
    }
    cleanup_shutdown.cancel();
    let _ = cleaner.await;

    // ④ 防线：静置期内不得删除（在途文件保护）
    let inflight_id = format!("inflight-{}", uuid::Uuid::now_v7());
    let (inflight_path, _, _) = yuntun_format::write_batch(
        &store,
        "public.orph",
        "s1",
        "w",
        &inflight_id,
        &batch(2),
        yuntun_format::DataFormat::Parquet,
    )
    .await
    .unwrap();
    let grace_shutdown = CancellationToken::new();
    let grace_cleaner = yuntun_compaction::spawn_orphan_cleanup_with_interval(
        store.clone(),
        catalog.clone(),
        "yuntun/".to_string(),
        Duration::from_secs(3600), // 1h 静置期
        Duration::from_millis(20),
        grace_shutdown.clone(),
    );
    // 跑足够多轮（>> interval），静置期内的文件必须原封不动
    tokio::time::sleep(Duration::from_millis(300)).await;
    let objs = yuntun_format::list_objects(&store, "yuntun/").await.unwrap();
    assert!(
        objs.iter().any(|(p, _)| p == &inflight_path),
        "静置期内的在途文件被删除 —— 多写者下即丢数据（T14.3）"
    );
    grace_shutdown.cancel();
    let _ = grace_cleaner.await;
    shutdown.cancel();
}

// ---------------------------------------------------------------- 幂等键集合（S3-5）

/// 造一个 N 行的批次（真实 payload 无关，只关心行数与键）。
#[cfg(test)]
fn batch_of(rows: usize, v: i64) -> arrow::record_batch::RecordBatch {
    arrow::record_batch::RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![v; rows])),
            Arc::new(StringArray::from(vec![Some("a"); rows])),
        ],
    )
    .unwrap()
}

/// `operation-log §27.5` 遗留 #1 的关闭验证（R3 S3-5）：**一个 chunk 聚合多个幂等键**时，
/// 提交层必须把**每一个**键都登记进 Catalog。
///
/// 为什么重要：重启后 `resume_recovered` 会**从 WAL 重建键索引**（§27）。若提交时只登记了
/// 一部分，未被登记的键在重建后就"消失"了 —— 同键重试会**再写一份数据**（静默重复计数）。
///
/// 构造是**确定性**的（不靠时序碰运气）：
/// 1. `rows_threshold = 100`：第一条（3 行）进 chunk 后**不会** seal；
/// 2. **先写两条（不同键）再启动攒批循环** → 循环一次吸收两条 → 必然落在**同一个 chunk**
///    （合计 123 行 ≥ 100 → 在第二条后 seal）；
/// 3. flush 到期即执行（夹具 `max_flush_delay = 0`、`spread = 0`）。
///
/// 断言的是**权威侧**（Catalog 的幂等索引），不是"没报错"。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_registers_every_key_of_a_multi_key_chunk() {
    let _gate = CHAOS_GATE.lock().await;
    let wal_dir = tmpdir("wal-multikey");
    let store_root = tmpdir("store-multikey");
    let setup = build_tuned(&wal_dir, &store_root, &[("mk", schema(), false)], 100, 0).await;

    // ① 攒批循环**尚未启动**：两条批次会被同一次吸收
    setup
        .ingestor
        .ingest(IngestBatch {
            table: "mk".into(),
            shard_key: "s0".into(),
            record_batch: batch_of(3, 1_000),
            idempotency_key: Some("mk-a".into()),
            received_at: std::time::SystemTime::now(),
        })
        .await
        .unwrap();
    setup
        .ingestor
        .ingest(IngestBatch {
            table: "mk".into(),
            shard_key: "s0".into(),
            record_batch: batch_of(120, 1_001),
            idempotency_key: Some("mk-b".into()),
            received_at: std::time::SystemTime::now(),
        })
        .await
        .unwrap();

    // ② 启动攒批 → 一次吸收两条 → 一个 chunk（123 行 ≥ 阈值）→ 一次提交携带两个键
    let shutdown = CancellationToken::new();
    let _acc = setup.ingestor.clone().spawn_accumulator(shutdown.clone());

    let files = wait_visible_files(&setup, "public.mk", "s0", 1).await;
    assert_eq!(files.len(), 1, "两条批次必须落在同一个 chunk（否则本用例没测到键集合）");
    assert_eq!(files[0].row_count, 123, "该文件应含两个批次的全部行");

    // ③ 权威断言：**两个键都在 Catalog 里**（提交层按键集合登记）
    for k in ["mk-a", "mk-b"] {
        assert!(
            setup.catalog.check_idempotency(k).await.unwrap().is_some(),
            "键 {k} 必须由**提交层**登记：否则重启按 WAL 重建索引时会漏掉它，同键重试=重复写"
        );
    }
    // ④ 反面：不在集合里的键不该命中（防止"任何键都判重"这种过头实现）
    assert!(
        setup
            .catalog
            .check_idempotency("mk-not-in-set")
            .await
            .unwrap()
            .is_none(),
        "未参与本次提交的键不得被判重"
    );
    shutdown.cancel();
}
