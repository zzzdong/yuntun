//! Ingestor 主流程（架构 §5 写入路径 + §2 chunk 层；详细设计 §5.2 / §5.3 / §5.4）。
//!
//! ## 严格时序（C8 / §5.2）
//! ① Schema 解析/演进（OCC，**必须在写对象存储之前**）
//! ② 写 WAL（Data，组提交 fsync）→ 数据已持久化，**可查**
//! ③ chunk 吸收（seal / spill / flush 计划由 `ChunkStore` 统一决策）
//! ④ flush：chunk → 对象存储 → `commit_files` → `mark_committed`
//!
//! ## 与旧实现的关键差异（架构 §2 / §5.2）
//! | 维度 | 旧 | 新 |
//! |---|---|---|
//! | 内存缓冲 | `MemoryShard`（可见性） + `WindowGroup`（分组）两套 | **chunk 一处**（seal / spill / 可见性同源） |
//! | flush 时刻 | `time_threshold + random jitter(0..60s)` 不可预测 | `seal_time + max_flush_delay + hash(instance)%spread` **确定** |
//! | 内存压力 | 无（靠进程 OOM） | 背压阶梯 60/80/95%（spill → 强制 seal → 拒写） |
//! | 可见性上界 | 攒批窗口 | **WAL fsync**（~一个扫描周期，架构 §5.2） |
//! | 持久化上界 | 与可见性混为一谈 | 独立常量 `max_flush_delay`（§5.2 双阈值） |

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use yuntun_catalog::CatalogOps;
use yuntun_chunk::chunk::{ChunkId, ChunkKey, TableLiveness};
use yuntun_chunk::{ChunkStore, ChunkStoreConfig, SealPolicy};
use yuntun_model::error::LakeError;
use yuntun_model::wal_record::{DataPayload, Record};
use yuntun_store::ShardId;
use yuntun_wal::writer::WalWriter;

use crate::accumulator::{extract_event_time_ms, now_ms};
use crate::flush::{flush_chunk, flush_chunk_with_id, recommit_into_catalog, FlushDeps, LiveBatchTracker};
use crate::schema_cache::{resolve_schema_version, SchemaCache};
use crate::source::{IngestBatch, IngestSource, Receipt};

/// Ingestor 配置（详细设计 §11 `[ingest]` + 架构 §5.2/§5.3/§5.4 + §2 阈值）。
///
/// **双阈值必须分离**（架构 §5.2）：
/// - `time_threshold_secs` 是**可见性**相关的攒批软目标，绑 WAL fsync；
/// - `max_flush_delay_secs` 是**持久化**硬上界，绑 WAL 回收与文件数。
#[derive(Debug, Clone)]
pub struct IngestorConfig {
    /// vortex | parquet（ADR-1 FormatSwitch 回退开关）
    pub default_format: yuntun_format::DataFormat,
    /// 实例标识：确定性 flush 相位的 hash 输入 + `FileManifest.source_instance`
    pub instance_id: String,
    /// spill 目录（**本地磁盘**，架构 §2.5）
    pub spill_dir: PathBuf,
    /// seal 触发：行数阈值（S1-10：50–100 万行量级，让 RowGroup 一次成型）
    pub rows_threshold: usize,
    /// seal 触发：字节阈值（内存口径）
    pub bytes_threshold: usize,
    /// **最短驻留地板**（秒）：创建后至少这么久才允许因"窗口关闭"而 seal。
    ///
    /// ⚠️ 它**不是** seal 时刻：时间维度的 seal 触发是**到达分钟窗口关闭**（ADR-10
    /// 明文否决"达到阈值后 N 秒"——那会让低吞吐表在一个窗口内产出十余个小文件）。
    pub time_threshold_secs: u64,
    /// seal → flush 的宽限期（秒）：`flush_at = sealed_at + max_flush_delay + phase`
    ///
    /// **默认 0（T8 基线定案）**：实测该宽限期**不减少文件数**（低吞吐表同一窗口仍是
    /// 1 文件/shard），只把持久化上界从 `spread` 推迟到 `max_flush_delay + spread`
    /// （实测 5.2s → 35.1s）。既然削峰靠相位分散，就不该再用持久化延迟买它。
    pub max_flush_delay_secs: u64,
    /// **强制 seal + flush 的最大驻留秒数**（S1-9：防慢写入流把 WAL 撑爆）
    pub chunk_max_resident_secs: u64,
    /// 确定性相位偏移上限（秒，S2-9：替代随机 jitter）
    ///
    /// **默认 30（T8 基线定案）**：实测提交带宽 ≈ spread（配 5s → 实测 4.86s），
    /// 峰值提交数 ≈ shards / spread。`spread=5` 时 100 个 shard 的提交挤在 5s 带内，
    /// 实测峰值 87 次/秒（均值 2.4 的 36 倍）；`spread=30` 降到 10 次/秒。
    /// 上限受不变量约束：`chunk_max_resident_secs(60) > max_flush_delay(0) + spread`
    /// → spread ≤ 59；取 30 留一倍余量。
    pub flush_phase_spread_secs: u64,
    /// 攒批扫描间隔（同时是**可见性上界**：fsync 后最长一个周期即可查）
    pub scan_interval: Duration,
    /// chunk 区内存上限（字节，架构 §2.8 硬分区之一）
    pub chunk_mem_budget: usize,
    /// 幂等键默认要求（表模板可覆盖，§7.3.2）
    pub require_idempotency_by_default: bool,
    /// 幂等键 TTL（默认 24h）
    pub idempotency_ttl: Duration,
}

impl Default for IngestorConfig {
    fn default() -> Self {
        Self {
            default_format: yuntun_format::DataFormat::Parquet,
            instance_id: "standalone".into(),
            spill_dir: PathBuf::from("./data/spill"),
            rows_threshold: 500_000,
            bytes_threshold: 128 * 1024 * 1024,
            time_threshold_secs: 5,
            max_flush_delay_secs: 0,
            chunk_max_resident_secs: 60,
            flush_phase_spread_secs: 30,
            scan_interval: Duration::from_millis(100),
            chunk_mem_budget: 512 * 1024 * 1024,
            require_idempotency_by_default: true,
            idempotency_ttl: Duration::from_secs(24 * 3600),
        }
    }
}

impl IngestorConfig {
    /// 展开为 chunk 层的 seal 策略（**唯一的 seal 判定处**）。
    pub fn seal_policy(&self) -> SealPolicy {
        SealPolicy {
            rows_threshold: self.rows_threshold,
            bytes_threshold: self.bytes_threshold,
            // 时间维度：seal 时刻由**窗口关闭**决定（ADR-10），本值是地板
            min_resident: Duration::from_secs(self.time_threshold_secs),
            max_flush_delay: Duration::from_secs(self.max_flush_delay_secs),
            max_resident: Duration::from_secs(self.chunk_max_resident_secs),
            phase_spread: Duration::from_secs(self.flush_phase_spread_secs),
        }
    }

    /// chunk store 配置（装配层用它构造共享实例时同样使用）。
    pub fn chunk_store_config(&self) -> ChunkStoreConfig {
        ChunkStoreConfig {
            policy: self.seal_policy(),
            spill_dir: self.spill_dir.clone(),
            instance_id: self.instance_id.clone(),
            wal_segment: 0,
        }
    }
}

/// All-in-One Ingestor。
pub struct Ingestor {
    pub cfg: IngestorConfig,
    pub wal: WalWriter,
    pub catalog: Arc<dyn CatalogOps>,
    pub store: Arc<dyn object_store::ObjectStore>,
    pub tracker: Arc<LiveBatchTracker>,
    pub schema_cache: Arc<SchemaCache>,
    /// chunk 层：写入侧热缓冲 + 读侧热数据视图 + 内存背压（架构 §2）。
    /// 与查询侧共享同一实例（读己之写）；分离部署时查询侧改注入 `RemoteShard`。
    pub chunks: Arc<ChunkStore>,
    /// M0：攒批重放跳过集——已被终态批次"认领"的 (组键, 半开区间)。
    pub replay_skip: Mutex<Vec<ReplaySkip>>,
    /// 攒批线程已吸收到哪条 WAL seq（观测用，T6.12）。
    ///
    /// `wal.synced_seq() - absorbed_seq` = **WAL 积压**：已 fsync 但尚未进 chunk 的记录数。
    /// 它同时是"内存越限"的缓冲池大小 —— 没有任何指标比它更能解释"为什么内存超了"。
    absorbed_seq: AtomicU64,
}

/// 攒批重放的跳过声明：一个已重提交/重做的批次对其 Data 区间的"认领"。
/// 判定为二维（组键 + 半开区间）——纯整数区间会把别的组落在组内空洞里的
/// 未成批 Data 误判为已覆盖（丢数，delta-dml-design §1.1 R15）。
#[derive(Debug, Clone)]
pub struct ReplaySkip {
    pub table: String,
    pub shard: String,
    pub time_window: String,
    pub epoch: u64,
    /// 半开 [start, end)：end = 该组最后一条 Data 的 seq + 1
    pub start: u64,
    pub end: u64,
}

impl ReplaySkip {
    fn claims(&self, table: &str, shard: &str, window: &str, epoch: u64, seq: u64) -> bool {
        self.table == table
            && self.shard == shard
            && self.time_window == window
            && self.epoch == epoch
            && self.start <= seq
            && seq < self.end
    }
}

impl Ingestor {
    /// 自建 chunk store（spill 目录 / 内存预算来自配置）。
    ///
    /// 装配层若要查询侧共享同一热数据视图，请用 [`Ingestor::with_chunks`]。
    pub fn new(
        cfg: IngestorConfig,
        wal: WalWriter,
        catalog: Arc<dyn CatalogOps>,
        store: Arc<dyn object_store::ObjectStore>,
    ) -> Self {
        let ledger = yuntun_chunk::MemoryLedger::new("chunk", cfg.chunk_mem_budget);
        let chunks = ChunkStore::new(cfg.chunk_store_config(), ledger);
        Self::with_chunks(cfg, wal, catalog, store, chunks)
    }

    /// 注入外部共享的 chunk store（装配层：同一实例交给查询侧，查询可读未落盘数据）。
    pub fn with_chunks(
        cfg: IngestorConfig,
        wal: WalWriter,
        catalog: Arc<dyn CatalogOps>,
        store: Arc<dyn object_store::ObjectStore>,
        chunks: Arc<ChunkStore>,
    ) -> Self {
        Self {
            cfg,
            wal,
            catalog,
            store,
            tracker: Arc::new(LiveBatchTracker::new()),
            schema_cache: Arc::new(SchemaCache::default()),
            chunks,
            replay_skip: Mutex::new(Vec::new()),
            absorbed_seq: AtomicU64::new(0),
        }
    }

    /// 攒批线程已吸收到的 WAL seq（观测用）。
    pub fn absorbed_seq(&self) -> u64 {
        self.absorbed_seq.load(Ordering::SeqCst)
    }

    /// **WAL 积压**：已 fsync 但尚未吸收进 chunk 的记录数（≥0）。
    ///
    /// 这是 T6.12 三项指标之一：它解释了"内存为什么会超预算"——
    /// chunk 记账是无条件的（可见性优先），越限部分由这里吸收。
    ///
    /// # 口径（**半开区间，差一会报出错误的积压**）
    /// - `wal.synced_seq()` = 已 fsync 的**最高 seq（含）** → 可读记录是 `[0, synced_seq]`
    /// - `absorbed_seq()` = 已吸收的**下一条 seq** → 已吸收记录是 `[0, absorbed_seq)`
    /// - 因此积压 = `synced_seq + 1 - absorbed_seq`
    ///
    /// 空 WAL 时 `synced_seq = 0`、`absorbed_seq = 0` → 积压为 0（`+1` 与 `-1` 相抵）。
    pub fn wal_backlog(&self) -> u64 {
        (self.wal.synced_seq() + 1).saturating_sub(self.absorbed_seq())
    }

    /// chunk 层观测快照（内存水位 / 压力 / 各态数量）。
    pub fn chunk_stats(&self) -> yuntun_chunk::store::ChunkStoreStats {
        self.chunks.stats()
    }

    /// 当前背压水位。
    pub fn pressure(&self) -> yuntun_chunk::Pressure {
        self.chunks.pressure()
    }

    /// chunk 层句柄（热数据读侧接缝：查询侧 `set_hot_shards(instance_id, chunks)`）。
    pub fn chunks(&self) -> Arc<ChunkStore> {
        self.chunks.clone()
    }

    fn deps(&self) -> FlushDeps {
        FlushDeps {
            wal: self.wal.clone(),
            catalog: self.catalog.clone(),
            store: self.store.clone(),
            format: self.cfg.default_format,
            tracker: self.tracker.clone(),
            instance_id: self.cfg.instance_id.clone(),
        }
    }

    /// 写入主流程（详细设计 §5.2）：
    /// 幂等键校验 → Schema 解析/演进（OCC）→ 写 WAL（fsync）→ 回执。
    ///
    /// **可见性上界绑 WAL**（架构 §5.2）：await 返回即已 fsync，
    /// 最长一个扫描周期后进入 chunk（可查），不等 flush。
    pub async fn ingest(&self, b: IngestBatch) -> Result<Receipt, LakeError> {
        // 【跨层唯一表标识】（`ops.rs` 约定）：裸名按 `public` 归一后进入 WAL / chunk / Manifest。
        // 写入侧若保留裸名、查询侧用全限定名（`meta.qualified_name()`），
        // 热数据读取就永远匹配不上 → 只能等 flush 落盘才可见（"写了很久查不到"）。
        let table = qualify_table(&b.table);
        // 表存在性检查（TableNotFound 不重试，§10.2）
        let table_meta = self
            .catalog
            .get_table(&table)
            .await?
            .ok_or_else(|| LakeError::TableNotFound(b.table.clone()))?;
        let ingest_cfg = table_meta
            .ingest_config
            .clone()
            .unwrap_or_else(yuntun_model::meta::IngestConfig::standard);
        // 幂等键开关以表配置为权威（§7.3.2；IngestConfig::standard 为缺省模板）
        let require_key = ingest_cfg.require_idempotency_key;

        // 幂等键处理矩阵（§7.3.2：【关键】强制表未传键必须拒绝）
        let client_key = crate::resolve_idempotency(require_key, b.idempotency_key.as_deref())?;

        // 幂等预筛（§7.3 三层防护第一层）：该键已登记 → 返回重复回执，**不写 WAL**。
        //
        // 【为什么拦点必须在入口】一个 chunk 会聚合多个 Data 记录、各自带键，
        // 而 flush 提交时 `commit_files` 只接受**单个** `client_request_id`
        //（"提交键集合"粒度属 R3 状态机，`refactor.md` S3-5）。所以 Meta 层去重
        // 兜不住"一个文件含多个键"的批次粒度 —— 入口是唯一正确的拦点。
        // 漏掉它的后果是静默重复计数（客户端超时重试即命中）。
        if let Some(k) = &client_key
            && self.catalog.check_idempotency(k).await?.is_some()
        {
            tracing::debug!(key = %k, table = %table, "idempotency key already claimed, skipping write");
            return Ok(Receipt {
                table: b.table,
                shard: b.shard_key,
                wal_seq: 0,
                row_count: 0,
                schema_version: 0,
                expected_visible_at: now_ms(),
                expected_visible_in_secs: 0,
                duplicate: true,
            });
        }

        // ①-④ Schema 解析 / OCC 演进（必须在写对象存储之前，C8）
        let schema_version = resolve_schema_version(
            &self.catalog,
            &self.schema_cache,
            &table,
            &b.record_batch.schema(),
        )
        .await?;

        // ⑤ 写 WAL（组提交 fsync）—— await 完成 = 已 fsync = 数据已持久化
        let event_time = extract_event_time_ms(&b.record_batch);
        let received_ms = b
            .received_at
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let (_wms, window) = crate::accumulator::window_of(event_time, received_ms);
        let mut ipc = Vec::new();
        {
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut ipc, &b.record_batch.schema())
                    .map_err(|e| LakeError::Other(format!("ipc encode: {e}")))?;
            writer
                .write(&b.record_batch)
                .map_err(|e| LakeError::Other(format!("ipc write: {e}")))?;
            writer
                .finish()
                .map_err(|e| LakeError::Other(format!("ipc finish: {e}")))?;
        }
        // 【背压】水位 >= 95% 时在这里就拒绝（架构 §2.7 第三级）：
        // 先于 WAL 追加拒绝，避免"已 fsync 但无法驻留"的两难。
        if self.chunks.pressure().rejects_writes() {
            return Err(LakeError::ResourceExhausted(format!(
                "chunk memory pressure at {:.0}% — retry after backoff",
                self.chunks.ledger().ratio() * 100.0
            )));
        }
        let ack = self
            .wal
            .append(yuntun_model::wal_record::Record::Data(DataPayload {
                table: table.clone(),
                shard: b.shard_key.clone(),
                schema_version,
                batch_ipc: ipc,
                client_request_id: b.idempotency_key.clone().unwrap_or_default(),
                time_window: window,
            }))
            .await?;

        // 幂等登记：WAL fsync 成功 = 该键的写入已持久（WAL 是权威），立刻登记
        // —— 否则**并发同键请求**会双双通过上面的预筛、各写一份 Data。
        //
        // `batch_id` 此刻未知（flush 时才生成 UUIDv7），登记为空串：
        // 幂等判定只需要"键存在"这一事实；空串 = 已认领、批次尚未落盘。
        if let Some(k) = &client_key {
            self.catalog
                .record_idempotency(yuntun_model::meta::IdempotencyRecord {
                    client_request_id: k.clone(),
                    batch_id: String::new(),
                    committed_at: now_ms() / 1000,
                })
                .await?;
        }

        // 可见性上界 = WAL fsync + 一个扫描周期（不再是"攒批窗口 + jitter"）。
        let visible_ms = self.cfg.scan_interval.as_millis() as u64;
        Ok(Receipt {
            table: b.table,
            shard: b.shard_key,
            wal_seq: ack.seq,
            row_count: b.record_batch.num_rows() as u64,
            schema_version,
            expected_visible_at: now_ms() + visible_ms,
            expected_visible_in_secs: visible_ms.div_ceil(1000).max(1),
            duplicate: false,
        })
    }

    /// 启动 Source（ADR-13 trait 抽象路径，无逐批回执）。
    pub async fn run_source(
        &self,
        source: Arc<dyn IngestSource>,
        shutdown: CancellationToken,
    ) -> Result<(), LakeError> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<IngestBatch>(4096);
        let src = source.clone();
        let src_task = tokio::spawn(async move { src.run(tx, shutdown.clone()).await });
        while let Some(b) = rx.recv().await {
            if let Err(e) = self.ingest(b).await {
                tracing::error!(error = %e, source = %source.name(), "ingest failed");
            }
        }
        let _ = src_task.await;
        Ok(())
    }

    /// 以 tokio task 启动攒批循环（返回 JoinHandle，供优雅关闭 join）。
    pub fn spawn_accumulator(
        self: Arc<Self>,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(self.run_accumulator(shutdown))
    }

    /// 攒批循环（详细设计 §5.3 accumulator_loop；架构 §2.7 / §5.3）。
    ///
    /// 每轮：① 扫描已 fsync 的 WAL → ② 施加背压阶梯 → ③ 执行 seal / spill / flush 计划。
    ///
    /// 【C2】严格只读已 fsync 的数据：`to = wal.synced_seq() + 1`。
    pub async fn run_accumulator(self: Arc<Self>, shutdown: CancellationToken) {
        let reader = yuntun_wal::reader::WalReader::new(self.wal.shard_dir());
        // M0：取走恢复阶段建立的"重放跳过集"（已被终态批次认领的 Data 不再重放入账）
        let replay_skip = std::mem::take(&mut *self.replay_skip.lock().unwrap());
        let mut last_read: u64 = 0;
        let mut interval = tokio::time::interval(self.cfg.scan_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }

            // 【退出闸门】cancel 只是发信号，**不会打断已在执行的一轮** ——
            // 本轮"吸收 + flush"要用掉可观时间（含对象存储写与 WAL fsync）。
            // 若调用方在这期间已用同一份 WAL 目录起了新的攒批循环（重启/交接/测试重建），
            // 两个循环会把**同一条 Data 各吸收一次**并各自 flush → 两个文件、重复计数。
            // 动手前再确认一次：宁可少做一轮，不可重复落盘。
            if shutdown.is_cancelled() {
                break;
            }

            // ---------- ① 扫描已 fsync 的 WAL ----------
            // 【关键】last_read 只按实际扫描到的最后一条 seq 推进 ——
            // 空扫描（synced 未变）不得推进，否则后续到达的 seq 会被永久跳过。
            let synced = self.wal.synced_seq();
            if synced >= last_read {
                match reader.scan_range(last_read, synced + 1) {
                    Ok(records) => {
                        for (seq, rec) in records {
                            match rec {
                                // 维护表存活/世代：DROP 后重建同名表 → 老世代数据必须被丢弃
                                Record::Ddl(d) => self.chunks.observe_ddl(d.op, &d.table),
                                Record::Data(p) => {
                                    if let Err(e) = self.absorb(&p, seq, &replay_skip) {
                                        tracing::error!(seq, error = %e, "chunk absorb failed");
                                    }
                                }
                                _ => {}
                            }
                            last_read = seq + 1;
                            self.absorbed_seq.store(last_read, Ordering::SeqCst);
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "wal scan failed");
                        continue;
                    }
                }
            }

            // ---------- ② 背压阶梯（架构 §2.7）----------
            let now = now_ms();
            self.chunks.set_wal_segment(self.wal.current_segment());
            let actions = self.chunks.enforce_pressure(now);
            if !actions.is_empty() {
                tracing::debug!(actions = ?actions, "chunk backpressure actions");
            }

            // ---------- ③ seal / spill / flush 计划（架构 §5.3 确定性到期）----------
            let plan = self.chunks.plan_flush(now);
            for id in plan.seal {
                if let Err(e) = self.chunks.seal(id, now) {
                    tracing::warn!(chunk = %id, error = %e, "seal failed");
                }
            }
            for id in plan.spill {
                if let Err(e) = self.chunks.spill(id) {
                    tracing::warn!(chunk = %id, error = %e, "spill failed");
                }
            }
            for id in plan.flush {
                self.flush_chunk_by_id(id).await;
            }
        }
    }

    /// 把一条已 fsync 的 WAL Data 记录吸收进 chunk（读己之写 + 攒批）。
    fn absorb(
        &self,
        p: &DataPayload,
        seq: u64,
        replay_skip: &[ReplaySkip],
    ) -> Result<(), LakeError> {
        let epoch = self.chunks.liveness(&p.table).epoch;
        // DROP 语义：已 DROP / 重建的世代不再入账（否则重启重放会把老数据"复活"）
        if self.chunks.is_stale(&p.table, epoch) {
            return Ok(());
        }
        // M0：已被终态批次认领（组键 + 半开区间）的 Data 跳过 ——
        // 恢复重提交已让原文件对查询可见，重放 flush 会产生双份数据
        if replay_skip
            .iter()
            .any(|c| c.claims(&p.table, &p.shard, &p.time_window, epoch, seq))
        {
            return Ok(());
        }
        let batches = decode_batches(p);
        if batches.is_empty() {
            return Ok(());
        }
        let schema = batches[0].schema();
        let key = ChunkKey::new(
            ShardId::new(p.table.clone(), p.shard.clone(), p.time_window.clone()),
            epoch,
        );
        let out = self
            .chunks
            .append(key, p.schema_version, schema, seq, batches, now_ms())?;
        // 越限已由 append 内部即时缓解；这里只兜一层水位告警
        if out.pressure.needs_spill() {
            tracing::debug!(
                chunk = %out.chunk_id,
                pressure = ?out.pressure,
                "chunk memory pressure after append"
            );
        }
        Ok(())
    }

    /// flush 单个 chunk（到期执行体）。
    async fn flush_chunk_by_id(self: &Arc<Self>, id: ChunkId) {
        let input = match self.chunks.flush_input(id) {
            Ok(Some(x)) => x,
            Ok(None) => return,
            Err(e) => {
                // spill 读回失败：批次保持非终态，WAL 是权威（架构 §2.6）
                tracing::error!(chunk = %id, error = %e, "chunk read failed, defer flush");
                self.chunks.note_flush_failure(id, now_ms());
                return;
            }
        };
        // DROP 语义：陈旧世代的数据不得提交（否则会挂到重建的同名表上，R9）
        if self.chunks.is_stale(&input.shard.table, input.epoch) {
            tracing::warn!(
                table = %input.shard.table,
                epoch = input.epoch,
                "discarding stale chunk instead of flushing"
            );
            self.chunks.discard_chunk(id);
            return;
        }

        let deps = self.deps();
        match flush_chunk(&input, &deps).await {
            Ok(out) => {
                tracing::info!(
                    batch_id = %out.batch_id,
                    path = %out.file_path,
                    rows = out.row_count,
                    "chunk flushed"
                );
                // release-after-commit（架构 §4.5 / I4）：commit 成功才标记，且**不立即释放数据**
                if let Err(e) = self.chunks.mark_committed(id, out.snapshot) {
                    tracing::error!(chunk = %id, error = %e, "mark_committed failed");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "flush failed (batch left non-terminal, monitor will abort)");
                self.chunks.note_flush_failure(id, now_ms());
            }
        }
    }

    /// 启动崩溃恢复分流（详细设计 §5.6 完整版，E3 验收核心）。
    ///
    /// 单节点重启后 MemoryCatalog 是空的（C5），因此：
    /// - `Committed`：重新提交 Meta（batch_id 幂等，重放安全）—— 恢复"Meta 已记录"状态
    /// - `S3Written`：同上（S3 文件已在，只补 Commit）
    /// - `Pending`：从 WAL 重读其 wal_seq_range 内的 Data → **复用原 batch_id** 重做
    ///   S3 写入 + Commit（`flush_chunk_with_id`）
    /// - `Abort`：跳过（其 S3 文件成为孤儿，由孤儿清理回收）
    ///
    /// 返回 (重做 flush 数, 重新 Commit 数)。
    pub async fn resume_recovered(&self) -> Result<(usize, usize), LakeError> {
        let recovery = self.wal.full_recovery()?;
        tracing::info!(
            synced = self.wal.synced_seq(),
            batches = recovery.states.states.len(),
            "resume_recovered: wal state loaded"
        );
        self.tracker.load_from(&recovery);
        let deps = self.deps();
        let reader = yuntun_wal::reader::WalReader::new(self.wal.shard_dir());
        let mut redone = 0usize;
        let mut committed = 0usize;
        // 攒批重放跳过集（M0）：被终态批次认领的 (组键, 半开区间)。
        let mut claims: Vec<ReplaySkip> = Vec::new();
        // DDL 时间线：(seq, op, table)。Pending 批次重做前必须确认其 Data 属于**当前**表世代，
        // 否则"崩溃前已 DROP 的表"的重做数据会挂到不存在的表上（悬挂 Manifest），
        // 之后重建同名表即复活旧数据。
        // 全量扫一遍 WAL：DDL 时间线与幂等键索引都要用（不扫两遍）
        let all_records = reader.scan_from(0)?;
        let timeline: Vec<(u64, u32, String)> = all_records
            .iter()
            .filter_map(|(seq, rec)| match rec {
                Record::Ddl(d) => Some((*seq, d.op, d.table.clone())),
                _ => None,
            })
            .collect();

        // 幂等键索引重建（"独立存储"在单机形态的持久化方式）。
        //
        // `MemoryCatalog` 重启即空（C5），键索引只能从 WAL Data 记录恢复
        // （Data 的 `client_request_id` 由 `ingest` 写入）。不重建的后果是
        // **重启后同一幂等键的客户端重试会再写一份数据** —— 静默重复。
        //
        // TTL 以重启时刻起算（保守方向：宁可多去重一次，不可重复写入一次）。
        let mut reindexed = 0usize;
        for (_, rec) in &all_records {
            let Record::Data(p) = rec else { continue };
            if p.client_request_id.is_empty() {
                continue;
            }
            self.catalog
                .record_idempotency(yuntun_model::meta::IdempotencyRecord {
                    client_request_id: p.client_request_id.clone(),
                    batch_id: String::new(),
                    committed_at: now_ms() / 1000,
                })
                .await?;
            reindexed += 1;
        }
        if reindexed > 0 {
            tracing::info!(reindexed, "rebuilt idempotency key index from WAL");
        }

        // 按创建顺序处理（稳定输出）
        let mut states: Vec<_> = recovery.states.states.values().cloned().collect();
        states.sort_by_key(|s| s.created_at_ms);
        for st in &states {
            use yuntun_model::batch::BatchStatus::*;
            match st.status {
                Committed | S3Written => {
                    // 【M0 恢复改造】终态批次按原 batch_id 与既有对象重提交内存 Catalog
                    //（不追加 WAL、不重写文件）——恢复"Meta 已记录"状态
                    let (s, e) = st.wal_seq_range;
                    // 表名：payload 增补字段优先；老 WAL 为空 → 从对象路径反解（R18 回退）
                    let table = if !st.table.is_empty() {
                        qualify_table(&st.table)
                    } else {
                        match st.s3_paths.first().and_then(|p| table_from_object_path(p)) {
                            Some(t) => {
                                tracing::warn!(batch_id = %st.batch_id, table = %t,
                                    "legacy WAL without table field: table resolved from object path");
                                qualify_table(&t)
                            }
                            None => {
                                tracing::error!(batch_id = %st.batch_id,
                                    "cannot resolve table for terminal batch, skipping re-commit");
                                continue;
                            }
                        }
                    };
                    // 世代闸门：批次属于已 DROP / DROP 后重建的表世代 → 不重提交
                    let (alive_at, epoch_at) = liveness_at(&timeline, s, &table);
                    let (alive_now, epoch_now) = liveness_at(&timeline, u64::MAX, &table);
                    if !alive_at || !alive_now || epoch_at != epoch_now {
                        tracing::warn!(
                            batch_id = %st.batch_id,
                            table = %table,
                            "committed batch belongs to a dropped/re-created table era, skipping re-commit"
                        );
                        crate::flush::abort_batch(&deps, &st.batch_id).await?;
                        continue;
                    }
                    recommit_into_catalog(&deps, st, &table).await?;
                    committed += 1;
                    claims.push(ReplaySkip {
                        table,
                        shard: st.shard.clone(),
                        time_window: st.time_window.clone(),
                        epoch: epoch_now,
                        start: s,
                        end: e,
                    });
                }
                Pending => {
                    // 从 WAL 重读 Data → 复用原 batch_id 重做对象存储写 + Commit。
                    // 区间为半开 [s, e)（M0 口径），且必须按表过滤——交错写入下
                    // 组内 seq 空洞属于别的组，混入会把别的表的行写进本批文件。
                    let (s, e) = st.wal_seq_range;
                    let records = reader.scan_range(s, e)?;
                    let mut payloads = Vec::new();
                    let mut seqs = Vec::new();
                    for (seq, rec) in records {
                        if let Record::Data(p) = rec {
                            if !st.table.is_empty() && p.table != st.table {
                                continue;
                            }
                            seqs.push(seq);
                            payloads.push(p);
                        }
                    }
                    if payloads.is_empty() {
                        tracing::warn!(batch_id = %st.batch_id, "pending batch has no data in WAL, aborting");
                        crate::flush::abort_batch(&deps, &st.batch_id).await?;
                        continue;
                    }
                    let table = if !st.table.is_empty() {
                        qualify_table(&st.table)
                    } else {
                        qualify_table(&payloads[0].table)
                    };
                    // 世代校验：批次 Data 写入时刻 vs WAL 末尾的最终状态
                    let (alive_at, epoch_at) = liveness_at(&timeline, s, &table);
                    let (alive_now, epoch_now) = liveness_at(&timeline, u64::MAX, &table);
                    if !alive_at || !alive_now || epoch_at != epoch_now {
                        tracing::warn!(
                            batch_id = %st.batch_id,
                            table = %table,
                            "pending batch belongs to a dropped/re-created table era, aborting"
                        );
                        crate::flush::abort_batch(&deps, &st.batch_id).await?;
                        continue;
                    }
                    let (batches, max_version) = crate::flush::payloads_to_batches(&payloads)?;
                    let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
                    let schema = batches[0].schema();
                    let input = yuntun_chunk::store::ChunkFlushInput {
                        id: None, // 恢复路径：不对应任何在世的 chunk
                        shard: ShardId::new(table.clone(), st.shard.clone(), st.time_window.clone()),
                        epoch: epoch_now,
                        schema_version: if st.schema_version > 0 {
                            st.schema_version
                        } else {
                            max_version
                        },
                        schema,
                        batches,
                        seqs,
                        wal_seq_range: s..e,
                        rows,
                        // 恢复重做：原封口时刻已不可知，用 0（调用方不得把恢复路径当延迟样本）
                        sealed_at_ms: 0,
                        seal_reason: None,
                        pressure_at_seal: None,
                    };
                    flush_chunk_with_id(&input, &deps, Some(st.batch_id.clone())).await?;
                    redone += 1;
                    claims.push(ReplaySkip {
                        table,
                        shard: st.shard.clone(),
                        time_window: st.time_window.clone(),
                        epoch: epoch_now,
                        start: s,
                        end: e,
                    });
                }
                Abort => {}
            }
        }
        tracing::info!(redone, committed, "resume_recovered done");
        // 认领集交给攒批循环：重放时跳过已认领的 Data
        *self.replay_skip.lock().unwrap() = claims;
        Ok((redone, committed))
    }

    /// 供测试/运维：手动 flush 指定 chunk 快照。
    pub async fn flush_now(
        &self,
        input: yuntun_chunk::store::ChunkFlushInput,
    ) -> Result<crate::flush::FlushOutcome, LakeError> {
        flush_chunk(&input, &self.deps()).await
    }
}

/// 解码 DataPayload 的 Arrow IPC → RecordBatch（chunk 吸收与行数统计共用）。
fn decode_batches(p: &DataPayload) -> Vec<arrow::record_batch::RecordBatch> {
    match arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(&p.batch_ipc), None) {
        Ok(reader) => reader.filter_map(|r| r.ok()).collect(),
        Err(_) => Vec::new(),
    }
}

/// 在 DDL 时间线上求：位置 `seq`（含）之前，表 `table` 的 (存活, 世代)。
///
/// 世代 = 该表被 CREATE 的累计次数。没有任何 DDL 记录时视为"存活、世代 0"
/// （兼容不经 WAL DDL 直接建表的调用方）。恢复路径用它判定
/// "该批次的数据是否属于当前表世代"。
fn liveness_at(timeline: &[(u64, u32, String)], seq: u64, table: &str) -> (bool, u64) {
    use yuntun_model::wal_record::ddl_op;
    let mut exists = true;
    let mut epoch = 0u64;
    for (s, op, t) in timeline {
        if *s > seq || t != table {
            continue;
        }
        match *op {
            ddl_op::CREATE_TABLE => {
                exists = true;
                epoch += 1;
            }
            ddl_op::DROP_TABLE => exists = false,
            _ => {}
        }
    }
    (exists, epoch)
}

/// 归一化为跨层唯一表标识（裸名 → `public.<name>`；已限定则原样）。
///
/// `yuntun_model::ops` 的约定：Catalog 内部键 / WAL `table` 字段 / 对象路径派生 /
/// Query 缓存键**必须同一身份**，裸表名只出现在 SQL 表面与 `TableMeta.name`。
pub fn qualify_table(table: &str) -> String {
    let (ns, bare) = yuntun_model::ops::split_qualified(table);
    yuntun_model::ops::qualified_name(ns, bare)
}

/// 从对象存储路径反解全限定表名（M0 升级回退：老 WAL 的 BatchPendingPayload
/// 无 `table` 字段时使用）。路径形如 `yuntun/<schema>/<table>/dt=.../...`。
fn table_from_object_path(path: &str) -> Option<String> {
    let rest = path.strip_prefix("yuntun/")?;
    let mut it = rest.split('/');
    let schema = it.next()?;
    let table = it.next()?;
    if schema.is_empty() || table.is_empty() {
        return None;
    }
    Some(format!("{schema}.{table}"))
}

/// 表世代查询（`yuntun_chunk::TableLiveness` 的转发，供调用方少导一个 crate）。
pub fn table_liveness(chunks: &ChunkStore, table: &str) -> TableLiveness {
    chunks.liveness(table)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timeline() -> Vec<(u64, u32, String)> {
        use yuntun_model::wal_record::ddl_op;
        vec![
            (0, ddl_op::CREATE_TABLE, "public.t".into()),
            (5, ddl_op::DROP_TABLE, "public.t".into()),
            (9, ddl_op::CREATE_TABLE, "public.t".into()),
            (3, ddl_op::CREATE_TABLE, "public.other".into()),
        ]
    }

    #[test]
    fn liveness_tracks_epoch_across_drop_create() {
        let tl = timeline();
        assert_eq!(liveness_at(&tl, 0, "public.t"), (true, 1));
        assert_eq!(liveness_at(&tl, 6, "public.t"), (false, 1), "DROP 后不存在");
        assert_eq!(liveness_at(&tl, 20, "public.t"), (true, 2), "重建 → 世代 2");
        assert_eq!(liveness_at(&tl, 20, "public.other"), (true, 1));
    }

    #[test]
    fn liveness_defaults_to_alive_epoch_zero() {
        assert_eq!(liveness_at(&[], 100, "public.any"), (true, 0));
    }

    #[test]
    fn qualify_table_normalizes_bare_names() {
        // 跨层唯一标识：裸名 → public.<name>；已限定原样保留
        assert_eq!(qualify_table("cpu"), "public.cpu");
        assert_eq!(qualify_table("sales.orders"), "sales.orders");
        // 写入侧与查询侧必须得到同一 identity（否则热数据读不到）
        assert_eq!(
            qualify_table("cpu"),
            yuntun_model::ops::qualified_name(yuntun_model::ops::DEFAULT_SCHEMA, "cpu")
        );
    }

    #[test]
    fn table_from_object_path_demangles_qualified_names() {
        assert_eq!(
            table_from_object_path("yuntun/sales/orders/dt=2026-01-01/shard=s0/b.parquet"),
            Some("sales.orders".into())
        );
        assert_eq!(table_from_object_path("other/x/y"), None);
    }

    #[test]
    fn replay_skip_claims_are_two_dimensional() {
        let c = ReplaySkip {
            table: "public.t".into(),
            shard: "s0".into(),
            time_window: "w1".into(),
            epoch: 1,
            start: 10,
            end: 20,
        };
        assert!(c.claims("public.t", "s0", "w1", 1, 10));
        assert!(c.claims("public.t", "s0", "w1", 1, 19));
        assert!(!c.claims("public.t", "s0", "w1", 1, 20), "右界为开");
        assert!(!c.claims("public.t", "s1", "w1", 1, 10), "别的组不算已覆盖");
        assert!(!c.claims("public.t", "s0", "w1", 2, 10), "别的世代不算已覆盖");
    }

    #[test]
    fn config_expands_to_seal_policy_with_separate_bounds() {
        let cfg = IngestorConfig::default();
        let p = cfg.seal_policy();
        // 架构 §5.2：可见性软目标与持久化硬上界必须分离，且后者更大
        assert_eq!(p.min_resident, Duration::from_secs(cfg.time_threshold_secs));
        assert_eq!(
            p.max_flush_delay,
            Duration::from_secs(cfg.max_flush_delay_secs)
        );
        assert!(
            p.max_resident > p.max_flush_delay,
            "驻留硬兜底必须晚于正常 flush 到期，否则兜底会变成常态路径"
        );
        // P0 定案（T8 基线，operation-log §32）：spread 30s、宽限期 0s
        assert_eq!(p.phase_spread, Duration::from_secs(30));
        assert_eq!(p.rows_threshold, 500_000, "S1-10：RowGroup 一次成型");
        // ADR-10：时间维度的 seal 是"窗口关闭"，地板必须短于一个窗口，
        // 否则 seal 时刻又会退化成"创建后 N 秒"
        assert!(
            p.min_resident < Duration::from_secs(60),
            "min_resident 必须短于分钟窗口，否则窗口对齐失去意义"
        );
    }
}
