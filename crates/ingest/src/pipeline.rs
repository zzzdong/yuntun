//! Ingestor 主流程（详细设计 §5.2 写入主流程 + §5.3 攒批循环）。

use crate::accumulator::{extract_event_time_ms, now_ms, BatchAccumulator};
use crate::flush::{flush_batch, flush_batch_with_id, FlushDeps, LiveBatchTracker};
use crate::schema_cache::{resolve_schema_version, SchemaCache};
use crate::source::IngestBatch;
use crate::source::{IngestSource, Receipt};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use yuntun_catalog::CatalogOps;
use yuntun_model::error::LakeError;
use yuntun_model::wal_record::DataPayload;
use yuntun_wal::writer::WalWriter;

/// Ingestor 配置（详细设计 §11 [ingest] 节）。
#[derive(Debug, Clone)]
pub struct IngestorConfig {
    /// vortex | parquet（ADR-1 FormatSwitch 回退开关）
    pub default_format: yuntun_format::DataFormat,
    /// 攒批行数阈值（默认 10000）
    pub rows_threshold: u64,
    /// 攒批时间阈值秒（默认 5）
    pub time_threshold_secs: u64,
    /// 绝对空闲超时兜底（默认 5min）
    pub idle_timeout: Duration,
    /// Flush Jitter 秒数上限（默认 60，ADR-10）
    pub flush_jitter_secs: u64,
    /// 攒批扫描间隔
    pub scan_interval: Duration,
    /// 幂等键默认要求（表模板可覆盖，§7.3.2）
    pub require_idempotency_by_default: bool,
    /// 幂等键 TTL（默认 24h）
    pub idempotency_ttl: Duration,
}

impl Default for IngestorConfig {
    fn default() -> Self {
        Self {
            default_format: yuntun_format::DataFormat::Parquet,
            rows_threshold: 10_000,
            time_threshold_secs: 5,
            idle_timeout: Duration::from_secs(300),
            flush_jitter_secs: 60,
            scan_interval: Duration::from_millis(100),
            require_idempotency_by_default: true,
            idempotency_ttl: Duration::from_secs(24 * 3600),
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
}

impl Ingestor {
    pub fn new(
        cfg: IngestorConfig,
        wal: WalWriter,
        catalog: Arc<dyn CatalogOps>,
        store: Arc<dyn object_store::ObjectStore>,
    ) -> Self {
        Self {
            cfg,
            wal,
            catalog,
            store,
            tracker: Arc::new(LiveBatchTracker::new()),
            schema_cache: Arc::new(SchemaCache::default()),
        }
    }

    fn deps(&self) -> FlushDeps {
        FlushDeps {
            wal: self.wal.clone(),
            catalog: self.catalog.clone(),
            store: self.store.clone(),
            format: self.cfg.default_format,
            tracker: self.tracker.clone(),
        }
    }

    /// 写入主流程（详细设计 §5.2）：
    /// 幂等键校验 → Schema 解析/演进（OCC）→ 写 WAL（fsync）→ 回执。
    pub async fn ingest(&self, b: IngestBatch) -> Result<Receipt, LakeError> {
        // 表存在性检查（TableNotFound 不重试，§10.2）
        let table_meta = self
            .catalog
            .get_table(&b.table)
            .await?
            .ok_or_else(|| LakeError::TableNotFound(b.table.clone()))?;
        let ingest_cfg = table_meta
            .ingest_config
            .clone()
            .unwrap_or_else(yuntun_model::meta::IngestConfig::standard);
        // 幂等键开关以表配置为权威（§7.3.2；IngestConfig::standard 为缺省模板）
        let require_key = ingest_cfg.require_idempotency_key;

        // 幂等键处理矩阵（§7.3.2：【关键】强制表未传键必须拒绝）
        let _client_key = crate::resolve_idempotency(require_key, b.idempotency_key.as_deref())?;

        // ①-④ Schema 解析 / OCC 演进（必须在写 S3 之前，C8）
        let schema_version = resolve_schema_version(
            &self.catalog,
            &self.schema_cache,
            &b.table,
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
        let ack = self
            .wal
            .append(yuntun_model::wal_record::Record::Data(DataPayload {
                table: b.table.clone(),
                shard: b.shard_key.clone(),
                schema_version,
                batch_ipc: ipc,
                client_request_id: b.idempotency_key.clone().unwrap_or_default(),
                time_window: window,
            }))
            .await?;

        Ok(Receipt {
            table: b.table,
            shard: b.shard_key,
            wal_seq: ack.seq,
            row_count: b.record_batch.num_rows() as u64,
            schema_version,
            expected_visible_at: now_ms() + self.cfg.time_threshold_secs * 1000,
            expected_visible_in_secs: self.cfg.time_threshold_secs,
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

    /// 攒批循环（详细设计 §5.3 accumulator_loop）。
    ///
    /// 【C2】严格只读已 fsync 的数据：`to = wal.synced_seq() + 1`。
    pub async fn run_accumulator(self: Arc<Self>, shutdown: CancellationToken) {
        let reader = yuntun_wal::reader::WalReader::new(self.wal.shard_dir());
        let mut last_read: u64 = 0;
        let mut acc = BatchAccumulator::new();
        let mut interval = tokio::time::interval(self.cfg.scan_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }

            // 【C2】只读已 fsync 的数据：synced = 已 fsync 的最高 seq，
            // 可读记录区间 [last_read, synced]（含端点），scan_range 半开 → to = synced + 1。
            // 【关键】last_read 只按实际扫描到的最后一条 seq 推进 ——
            // 空扫描（synced 未变）不得推进，否则后续到达的 seq 会被永久跳过。
            let synced = self.wal.synced_seq();
            if synced >= last_read {
                match reader.scan_range(last_read, synced + 1) {
                    Ok(records) => {
                        for (seq, rec) in records {
                            if let yuntun_model::wal_record::Record::Data(p) = rec {
                                let rows = decode_row_count(&p);
                                acc.push(p, seq, rows, now_ms());
                            }
                            last_read = seq + 1;
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "wal scan failed");
                        continue;
                    }
                }
            }

            // 检查各 (shard, window) 是否满足 flush
            for group in acc.drain_ready(now_ms(), &self.cfg) {
                let deps = self.deps();
                match flush_batch(group, &deps).await {
                    Ok(out) => {
                        tracing::info!(
                            batch_id = %out.batch_id,
                            path = %out.file_path,
                            rows = out.row_count,
                            "batch flushed"
                        );
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "flush failed (batch left non-terminal, monitor will abort)");
                    }
                }
            }
        }
    }

    /// 启动崩溃恢复分流（详细设计 §5.6 完整版，E3 验收核心）。
    ///
    /// 单节点重启后 MemoryCatalog 是空的（C5），因此：
    /// - `Committed`：重新提交 Meta（batch_id 幂等，重放安全）—— 恢复"Meta 已记录"状态
    /// - `S3Written`：同上（S3 文件已在，只补 Commit）
    /// - `Pending`：从 WAL 重读其 wal_seq_range 内的 Data → **复用原 batch_id** 重做
    ///   S3 写入 + Commit（flush_batch_with_id）
    /// - `Abort`：跳过（其 S3 文件成为孤儿，由孤儿清理回收）
    ///
    /// 返回 (重做 flush 数, 重新 Commit 数)。
    pub async fn resume_recovered(&self) -> Result<(usize, usize), LakeError> {
        let recovery = self.wal.full_recovery()?;
        self.tracker.load_from(&recovery);
        let deps = self.deps();
        let reader = yuntun_wal::reader::WalReader::new(self.wal.shard_dir());
        let mut redone = 0usize;
        let mut committed = 0usize;

        // 按创建顺序处理（稳定输出）
        let mut states: Vec<_> = recovery.states.states.values().cloned().collect();
        states.sort_by_key(|s| s.created_at_ms);
        for st in &states {
            use yuntun_model::batch::BatchStatus::*;
            match st.status {
                Committed | S3Written => {
                    // 【阶段 0 语义】Meta（MemoryCatalog）不持久（C5），恢复的可见性
                    // 由攒批线程全量重读 WAL 重做 flush 提供（operation-log §2.2-6）。
                    // 此处【不得】重提交 —— 否则与重读 flush 产生双份可见数据
                    // （chaos E3 回归验证）。
                    // `commit_recovered_batch`（带正确 table/file_size）留给
                    // 阶段 1 持久 Meta 的 "Committed → 只补 Commit" 分流。
                    tracing::debug!(batch_id = %st.batch_id, status = ?st.status,
                        "committed/s3written batch: visibility re-provided by accumulator re-scan");
                    committed += 1;
                }
                Pending => {
                    // 从 WAL 重读 Data → 复用原 batch_id 重做 S3 + Commit
                    let (s, e) = st.wal_seq_range;
                    let records = reader.scan_range(s, e + 1)?;
                    let mut payloads = Vec::new();
                    let mut rows = 0u64;
                    for (_, rec) in records {
                        if let yuntun_model::wal_record::Record::Data(p) = rec {
                            rows += decode_row_count(&p);
                            payloads.push(p);
                        }
                    }
                    if payloads.is_empty() {
                        // WAL 数据已丢失（如被误清理）→ abort 该批次，避免卡死
                        tracing::warn!(batch_id = %st.batch_id, "pending batch has no data in WAL, aborting");
                        crate::flush::abort_batch(&deps, &st.batch_id).await?;
                        continue;
                    }
                    let table = payloads[0].table.clone();
                    let group = crate::accumulator::WindowGroup {
                        table,
                        shard: st.shard.clone(),
                        window_ms: 0, // 仅 flush 时用于触发判定；恢复路径直接 flush
                        window: st.time_window.clone(),
                        payloads,
                        first_seq: s,
                        rows,
                        created_at_ms: st.created_at_ms,
                    };
                    flush_batch_with_id(group, &deps, Some(st.batch_id.clone())).await?;
                    redone += 1;
                }
                Abort => {}
            }
        }
        Ok((redone, committed))
    }

    /// 供测试/运维：手动 flush 指定组。
    pub async fn flush_now(
        &self,
        group: crate::accumulator::WindowGroup,
    ) -> Result<crate::flush::FlushOutcome, LakeError> {
        flush_batch(group, &self.deps()).await
    }
}

/// 快速计算 DataPayload 的行数（不解码全量：MVP 直接 IPC 解码统计，
/// 若想省 CPU 可在写入时把行数写入 payload —— 留作优化）。
fn decode_row_count(p: &DataPayload) -> u64 {
    match arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(&p.batch_ipc), None) {
        Ok(reader) => reader
            .filter_map(|r| r.ok())
            .map(|b| b.num_rows() as u64)
            .sum(),
        Err(_) => 0,
    }
}

/// 编译期引用占位（resume Pending 路径在阶段 0.5 chaos 扩展时使用）。
#[allow(unused_imports)]
use crate::flush::{abort_batch as _abort_batch, commit_recovered_batch as _commit_recovered};
