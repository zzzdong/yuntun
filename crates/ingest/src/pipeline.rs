//! Ingestor 主流程（详细设计 §5.2 写入主流程 + §5.3 攒批循环）。

use crate::accumulator::{extract_event_time_ms, now_ms, BatchAccumulator};
use crate::flush::{
    flush_batch, flush_batch_with_id, recommit_into_catalog, FlushDeps, LiveBatchTracker,
};
use crate::schema_cache::{resolve_schema_version, SchemaCache};
use crate::source::IngestBatch;
use crate::source::{IngestSource, Receipt};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use yuntun_catalog::CatalogOps;
use yuntun_model::error::LakeError;
use yuntun_model::wal_record::{DataPayload, Record};
use yuntun_store::{ShardId, ShardStore};
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
    /// 分片存储门面（store 层）：写入侧把"已 fsync、未落盘"的热数据放进**内存分片**，
    /// 提交后交棒给**磁盘分片**。查询侧共享同一实例（读己之写）。
    pub shards: Arc<ShardStore>,
    /// M0：攒批重放跳过集——已被终态批次"认领"的 (组键, 半开区间)。
    /// `resume_recovered` 填充，`run_accumulator` 消费（取走后清空）。
    /// 认领命中的 Data 不再重放入账（重提交已让原文件对查询可见）。
    pub replay_skip: Mutex<Vec<ReplaySkip>>,
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
    pub fn new(
        cfg: IngestorConfig,
        wal: WalWriter,
        catalog: Arc<dyn CatalogOps>,
        store: Arc<dyn object_store::ObjectStore>,
    ) -> Self {
        let shards = ShardStore::local(store.clone());
        Self::with_shards(cfg, wal, catalog, store, shards)
    }

    /// 与 [`Ingestor::new`] 相同，但注入外部共享的分片存储门面
    /// （server 装配：同一实例交给查询侧，查询从内存分片补齐尚未落盘的数据）。
    pub fn with_shards(
        cfg: IngestorConfig,
        wal: WalWriter,
        catalog: Arc<dyn CatalogOps>,
        store: Arc<dyn object_store::ObjectStore>,
        shards: Arc<ShardStore>,
    ) -> Self {
        Self {
            cfg,
            wal,
            catalog,
            store,
            tracker: Arc::new(LiveBatchTracker::new()),
            schema_cache: Arc::new(SchemaCache::default()),
            shards,
            replay_skip: Mutex::new(Vec::new()),
        }
    }

    /// 分片存储门面句柄（内存分片 + 磁盘分片）。
    pub fn shards(&self) -> Arc<ShardStore> {
        self.shards.clone()
    }

    fn deps(&self) -> FlushDeps {
        FlushDeps {
            wal: self.wal.clone(),
            catalog: self.catalog.clone(),
            store: self.store.clone(),
            format: self.cfg.default_format,
            tracker: self.tracker.clone(),
            hot: self.shards.memory().clone(),
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

        // 【修复】可见性上界不再是"攒批窗口"：写入 fsync 后由攒批线程在**一个扫描周期**内
        // 发布到内存视图（读己之写），查询侧即时可读；此前回执写死的 5s 与实际
        // 最坏 ~60s（jitter）+30s（缓存 TTL）严重不符。
        let visible_ms = self.cfg.scan_interval.as_millis() as u64;
        Ok(Receipt {
            table: b.table,
            shard: b.shard_key,
            wal_seq: ack.seq,
            row_count: b.record_batch.num_rows() as u64,
            schema_version,
            expected_visible_at: now_ms() + visible_ms,
            expected_visible_in_secs: visible_ms.div_ceil(1000).max(1),
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
        // M0：取走恢复阶段建立的"重放跳过集"（已被终态批次认领的 Data 不再重放入账）
        let replay_skip = std::mem::take(&mut *self.replay_skip.lock().unwrap());
        let mut last_read: u64 = 0;
        let mut acc = BatchAccumulator::new();
        let mut interval = tokio::time::interval(self.cfg.scan_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // "本进程新写入"的 seq 下界：只有 >= 此值的数据才发布到未落盘内存视图（读己之写），
        // 历史数据（重启重放的旧 WAL 内容）不驻留内存，避免把整个 WAL 历史搬进内存。
        let live_from_seq = self.wal.next_seq();

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
                            match rec {
                                // 维护表存活/世代：DROP 后重建同名表 → 老世代数据必须被丢弃，
                                // 否则重启重放会把已 DROP 表的数据"复活"到新表里。
                                Record::Ddl(d) => self.shards.memory().observe_ddl(d.op, &d.table),
                                Record::Data(p) => {
                                    let epoch = self.shards.memory().liveness(&p.table).epoch;
                                    // M0：已被终态批次认领（组键 + 半开区间）的 Data 跳过——
                                    // 恢复重提交已让原文件对查询可见，重放 flush 会产生双份数据
                                    let claimed = replay_skip.iter().any(|c| {
                                        c.claims(&p.table, &p.shard, &p.time_window, epoch, seq)
                                    });
                                    if !claimed {
                                        let batches = decode_batches(&p);
                                        let rows: u64 =
                                            batches.iter().map(|b| b.num_rows() as u64).sum();
                                        // 读己之写：已 fsync 的实时数据放进 store 层内存分片，立刻可查
                                        // （不等 flush/jitter/缓存 TTL）
                                        if seq >= live_from_seq {
                                            self.shards.memory().push(
                                                &ShardId::new(
                                                    p.table.clone(),
                                                    p.shard.clone(),
                                                    p.time_window.clone(),
                                                ),
                                                seq,
                                                epoch,
                                                batches,
                                            );
                                        }
                                        acc.push_with_epoch(p, seq, rows, now_ms(), epoch);
                                    }
                                }
                                _ => {}
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

            // 检查各 (shard, window, epoch) 是否满足 flush
            for group in acc.drain_ready(now_ms(), &self.cfg) {
                // 【DROP 语义】分组数据属于已 DROP（或 DROP 后重建）的表世代 → 丢弃不提交。
                // 否则会出现"已 DROP 表的数据在重建同名表后复活"（静默错误结果）。
                if self.shards.memory().is_stale(&group.table, group.epoch) {
                    tracing::warn!(
                        table = %group.table,
                        epoch = group.epoch,
                        records = group.seqs.len(),
                        "discarding stale batch group: table dropped or re-created with same name"
                    );
                    self.shards.memory().remove(&group.shard_id(), &group.seqs);
                    continue;
                }
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
        let timeline: Vec<(u64, u32, String)> = reader
            .scan_from(0)?
            .into_iter()
            .filter_map(|(seq, rec)| match rec {
                Record::Ddl(d) => Some((seq, d.op, d.table)),
                _ => None,
            })
            .collect();

        // 按创建顺序处理（稳定输出）
        let mut states: Vec<_> = recovery.states.states.values().cloned().collect();
        states.sort_by_key(|s| s.created_at_ms);
        for st in &states {
            use yuntun_model::batch::BatchStatus::*;
            match st.status {
                Committed | S3Written => {
                    // 【M0 恢复改造】终态批次按原 batch_id 与既有对象重提交内存 Catalog
                    //（不追加 WAL、不重写文件）——恢复"Meta 已记录"状态，替代旧语义的
                    // "攒批全量重放重写全部历史"（每次重启产生全量新文件，delta-dml-design §1.1）。
                    let (s, e) = st.wal_seq_range;
                    // 表名：payload 增补字段优先；老 WAL 为空 → 从对象路径反解（R18 回退）
                    let table = if !st.table.is_empty() {
                        st.table.clone()
                    } else {
                        match st.s3_paths.first().and_then(|p| table_from_object_path(p)) {
                            Some(t) => {
                                tracing::warn!(batch_id = %st.batch_id, table = %t,
                                    "legacy WAL without table field: table resolved from object path");
                                t
                            }
                            None => {
                                tracing::error!(batch_id = %st.batch_id,
                                    "cannot resolve table for terminal batch, skipping re-commit");
                                continue;
                            }
                        }
                    };
                    // 世代闸门（与 Pending 分支同款）：批次属于已 DROP / DROP 后重建的
                    // 表世代 → 不重提交（其文件转孤儿，由孤儿清理回收；
                    // 旧世代数据不得静默挂到重建的同名表上，R9）
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
                    // 从 WAL 重读 Data → 复用原 batch_id 重做 S3 + Commit。
                    // 区间为半开 [s, e)（M0 口径），且必须按表过滤——交错写入下
                    // 组内 seq 空洞属于别的组，混入会把别的表的行写进本批文件。
                    let (s, e) = st.wal_seq_range;
                    let records = reader.scan_range(s, e)?;
                    let mut payloads = Vec::new();
                    let mut seqs = Vec::new();
                    let mut rows = 0u64;
                    for (seq, rec) in records {
                        if let yuntun_model::wal_record::Record::Data(p) = rec {
                            if !st.table.is_empty() && p.table != st.table {
                                // 组内空洞里其他组（别的表）的 Data：不属于本批次
                                continue;
                            }
                            rows += decode_row_count(&p);
                            seqs.push(seq);
                            payloads.push(p);
                        }
                    }
                    if payloads.is_empty() {
                        // WAL 数据已丢失（如被误清理）→ abort 该批次，避免卡死
                        tracing::warn!(batch_id = %st.batch_id, "pending batch has no data in WAL, aborting");
                        crate::flush::abort_batch(&deps, &st.batch_id).await?;
                        continue;
                    }
                    let table = if !st.table.is_empty() {
                        st.table.clone()
                    } else {
                        payloads[0].table.clone()
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
                    let group = crate::accumulator::WindowGroup {
                        table: table.clone(),
                        shard: st.shard.clone(),
                        window_ms: 0, // 仅 flush 时用于触发判定；恢复路径直接 flush
                        window: st.time_window.clone(),
                        payloads,
                        seqs: seqs.clone(),
                        first_seq: s,
                        rows,
                        created_at_ms: st.created_at_ms,
                        epoch: epoch_at,
                    };
                    flush_batch_with_id(group, &deps, Some(st.batch_id.clone())).await?;
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
        // 认领集交给攒批循环：重放时跳过已认领的 Data（重提交已让原文件可见，
        // 重放 flush 会产生双份数据）
        *self.replay_skip.lock().unwrap() = claims;
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

/// 解码 DataPayload 的 Arrow IPC → RecordBatch（未落盘内存视图与行数统计共用）。
fn decode_batches(p: &DataPayload) -> Vec<arrow::record_batch::RecordBatch> {
    match arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(&p.batch_ipc), None) {
        Ok(reader) => reader.filter_map(|r| r.ok()).collect(),
        Err(_) => Vec::new(),
    }
}

/// 快速计算 DataPayload 的行数（MVP 直接 IPC 解码统计，
/// 若想省 CPU 可在写入时把行数写入 payload —— 留作优化）。
fn decode_row_count(p: &DataPayload) -> u64 {
    decode_batches(p).iter().map(|b| b.num_rows() as u64).sum()
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

/// 编译期引用占位（resume Pending 路径在阶段 0.5 chaos 扩展时使用）。
#[allow(unused_imports)]
use crate::flush::{abort_batch as _abort_batch, recommit_into_catalog as _recommit_into_catalog};
