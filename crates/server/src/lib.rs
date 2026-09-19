//! 装配层：Standalone 单机进程（详细设计 §2.4 / §5.7；原 all-in-one，v2.0 更名）。
//!
//! 组装：MemoryCatalog + ObjectStore + WAL + Ingestor + QueryEngine
//! + Compactor + WAL 超时监控，并启动 Arrow Flight gRPC 服务。

pub mod config;
pub mod flight;

pub use config::Config;
pub use flight::FlightServer;

use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_ingest::Ingestor;
use yuntun_model::meta::{deserialize_schema, IngestConfig};
use yuntun_model::ops::CreateTableRequest;
use yuntun_model::wal_record::{ddl_op, Record};
use yuntun_query::QueryEngine;
use yuntun_sql::SqlEngine;
use yuntun_wal::writer::WalWriter;

/// 运行中的各组件句柄（供运维查询/测试断言）。
pub struct Lakehouse {
    /// **只暴露 trait**（`Arc<dyn CatalogOps>`）：全仓只有装配点知道用的是哪个实现。
    ///
    /// 这就是「**`if distributed` 分支为零**」的实现方式 —— 不靠纪律，靠**类型**：
    /// 别处拿不到 `MemoryCatalog`（要拿得先在这里 `as` 下去，一眼可见）。
    /// 分布式形态切换时，改的只有装配点那一行。
    pub catalog: Arc<dyn CatalogOps>,
    pub ingestor: Arc<Ingestor>,
    pub query: Arc<QueryEngine>,
    /// SQL 处理层（W-4：MySQL wire 端口与 FlightServer 各自持有句柄；
    /// 引擎无状态，仅 write_policy 为实例级配置）
    pub sql: Arc<SqlEngine>,
    pub wal: WalWriter,
    pub store: Arc<dyn object_store::ObjectStore>,
    pub shutdown: CancellationToken,
}

// ---------------------------------------------------------------- 观测（T6.12）

/// chunk 区指标。
#[derive(Debug, Clone, serde::Serialize)]
pub struct ChunkMetrics {
    pub chunks: usize,
    pub open: usize,
    pub sealed: usize,
    pub spilled: usize,
    pub flushed: usize,
    /// 账本口径的内存占用（字节）
    pub resident_bytes: usize,
    pub budget_bytes: usize,
    /// 背压水位（比例）
    pub pressure_ratio: f64,
    pub pressure: String,
    /// 因内存水位跳过相位分散而提前 flush 的累计次数。
    ///
    /// > 0 = **ADR-10 的削峰正在让位**（文件数与窗口的对应关系仍在，但提交时刻不再均匀分散）。
    /// 运维据此区分"Meta/S3 尖峰是配置问题还是负载超设计"，不暴露它就只能在尖峰里猜。
    pub phase_yielded_flushes: u64,
}

/// WAL 指标。
#[derive(Debug, Clone, serde::Serialize)]
pub struct WalMetrics {
    /// 已 fsync 的最高 seq（可见性上界）
    pub synced_seq: u64,
    pub next_seq: u64,
    pub current_segment: u64,
    /// 攒批线程已吸收到的 seq
    pub absorbed_seq: u64,
    /// **WAL 积压记录数**：已 fsync 但尚未进 chunk —— 内存越限的缓冲池
    pub backlog_records: u64,
    /// WAL 目录字节数（磁盘占用）
    pub dir_bytes: u64,
}

/// Catalog 本地物化视图指标。
#[derive(Debug, Clone, serde::Serialize)]
pub struct CatalogMetrics {
    pub schema_ver: u64,
    pub manifest_ver: u64,
    pub snapshot: u64,
    pub tables: usize,
    pub refreshes: u64,
    pub full_reloads: u64,
    /// 增量路径累计重拉的表数（对比 `full_reloads` 可看出增量的收益）
    pub delta_tables: u64,
    /// 最近一次刷新失败原因（None = 健康；刷新失败会保留旧快照）
    pub last_error: Option<String>,
}

/// query 执行区指标。
#[derive(Debug, Clone, serde::Serialize)]
pub struct QueryMetrics {
    pub reserved_bytes: Option<usize>,
    pub limit_bytes: Option<usize>,
}

/// **运行期可观测快照**（`plan.md` T6.12）。
///
/// 三项"故障现场三件套"：**内存水位**（chunk）、**WAL 积压**（吸收是否落后）、
/// **背压水位**（写入被拒的边缘）。没有这三项时，压力类问题只能复现不能定位。
#[derive(Debug, Clone, serde::Serialize)]
pub struct LakehouseMetrics {
    pub chunk: ChunkMetrics,
    pub wal: WalMetrics,
    pub catalog: CatalogMetrics,
    pub query: QueryMetrics,
}

impl Lakehouse {
    /// 采集一次指标快照（`plan.md` T6.12 的单一实现；后台打点复用同一函数）。
    pub async fn metrics(&self) -> LakehouseMetrics {
        collect_metrics(&self.ingestor, &self.query, &self.catalog, &self.wal).await
    }
}

/// 指标采集的**唯一实现**（同步/异步调用方共用，避免两处口径漂移）。
///
/// 四个句柄即可覆盖全部指标，因此不持有整个 `Lakehouse`：
/// 后台打点任务只 clone 这四样，不必让 `Lakehouse` 变成 `'static`/`Arc`。
pub async fn collect_metrics(
    ingestor: &Arc<Ingestor>,
    query: &Arc<QueryEngine>,
    catalog: &Arc<dyn CatalogOps>,
    wal: &WalWriter,
) -> LakehouseMetrics {
    let cs = ingestor.chunk_stats();
    let chunks = ingestor.chunks();
    let ledger = chunks.ledger();
    LakehouseMetrics {
        chunk: ChunkMetrics {
            chunks: cs.chunks,
            open: cs.open,
            sealed: cs.sealed,
            spilled: cs.spilled,
            flushed: cs.flushed,
            resident_bytes: cs.resident_bytes,
            budget_bytes: ledger.limit(),
            pressure_ratio: ledger.ratio(),
            pressure: format!("{:?}", cs.pressure),
            phase_yielded_flushes: cs.phase_yielded_flushes,
        },
        wal: WalMetrics {
            synced_seq: wal.synced_seq(),
            next_seq: wal.next_seq(),
            current_segment: wal.current_segment(),
            absorbed_seq: ingestor.absorbed_seq(),
            backlog_records: ingestor.wal_backlog(),
            dir_bytes: dir_bytes(&wal.shard_dir()),
        },
        catalog: {
            let v = catalog.version().await;
            let stats = query.catalog().stats();
            CatalogMetrics {
                schema_ver: v.schema_ver,
                manifest_ver: v.manifest_ver,
                snapshot: stats.snapshot,
                tables: stats.tables,
                refreshes: stats.refreshes,
                full_reloads: stats.full_reloads,
                delta_tables: stats.delta_tables,
                last_error: query.catalog().last_error(),
            }
        },
        query: QueryMetrics {
            reserved_bytes: query.query_memory_reserved(),
            limit_bytes: query.query_memory_limit(),
        },
    }
}

/// 目录内文件字节合计（WAL 磁盘占用；失败返回 0，不因观测失败影响主流程）。
fn dir_bytes(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|it| {
            it.flatten()
                .filter_map(|e| e.metadata().ok())
                .filter(|m| m.is_file())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}


impl Lakehouse {
    /// 按配置装配全部组件（不启动服务）。
    pub async fn build(cfg: &Config) -> Result<Self, yuntun_model::error::LakeError> {
        Self::build_with_shutdown(cfg, CancellationToken::new()).await
    }

    /// 同 build，但注入外部 shutdown（standalone 主循环使用）。
    pub async fn build_with_shutdown(
        cfg: &Config,
        shutdown: CancellationToken,
    ) -> Result<Self, yuntun_model::error::LakeError> {
        // ① Catalog —— **装配点**：全仓只有这一处知道具体实现是谁。
        //    分布式形态（S3-4 第二件）就是在这里换成 `RemoteCatalog`，别处一行不改。
        //    C5：阶段 0 是内存实现（重启后靠 WAL 的 DDL 重放重建）。
        let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());

        // ② ObjectStore
        let store_cfg = match &cfg.store {
            config::StoreSection::Local { root } => yuntun_store::StoreConfig::Local {
                root: root.to_string_lossy().to_string(),
            },
            config::StoreSection::Memory => yuntun_store::StoreConfig::Memory,
            config::StoreSection::S3 {
                bucket,
                endpoint,
                access_key_id,
                secret_access_key,
                allow_http,
            } => yuntun_store::StoreConfig::S3 {
                bucket: bucket.clone(),
                endpoint: endpoint.clone(),
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                allow_http: *allow_http,
            },
        };
        let store = yuntun_store::create_store(&store_cfg)?;

        // ③ WAL（MVP 单 shard 0）
        let wal_cfg = cfg.wal_config();
        let wal = WalWriter::open(wal_cfg.clone(), 0).await?;

        // ③.5 WAL DDL 重放（S1.7）：先重建表清单，再分流数据批次恢复（§5.6）。
        // DDL 记录由 yuntun-sql::SqlEngine 在 Catalog apply 成功后追加（顺序即因果）；
        // 重放幂等（create 已存在 / drop 不存在均忽略），保证 SQL 写入的数据
        // 崩溃重启后表存在、可恢复（S1.6 验收）。
        replay_wal_ddl(&catalog, &wal).await?;

        // ③.9 配置自检：能启动但会静默劣化的项必须显式告警（不阻断启动）
        for w in cfg.warnings() {
            tracing::warn!("config: {w}");
        }

        // ④ 内存硬分区（架构 §2.8）：chunk 区与 query 执行区**互不抢占**
        let partition = yuntun_chunk::MemoryPartition::with_thresholds(
            cfg.chunk.chunk_mem_bytes(),
            cfg.chunk.query_mem_bytes(),
            cfg.chunk.pressure_thresholds(),
        );

        // ④.1 chunk 层：写入侧热缓冲 + 读侧热数据视图 + 内存背压（架构 §2）
        let ingest_cfg = cfg.ingestor_config();
        let chunks = yuntun_chunk::ChunkStore::new(
            ingest_cfg.chunk_store_config(),
            partition.chunk().clone(),
        );
        // 写入侧与查询侧共享**同一 chunk store 实例**（读己之写）
        let ingestor = Arc::new(Ingestor::with_chunks(
            ingest_cfg,
            wal.clone(),
            catalog.clone(),
            store.clone(),
            chunks.clone(),
        ));

        // ⑤ 崩溃恢复分流（§5.6）
        let (redone, committed) = ingestor.resume_recovered().await?;
        if redone + committed > 0 {
            tracing::info!(redone, committed, "recovered batches from WAL");
        }

        // ⑥ QueryEngine（缓存刷新在 spawn_background 中启动）
        let cache = Arc::new(yuntun_query::LocalCatalog::new());
        // 读己之写：查询侧接线热数据读侧（进程内 chunk；分离部署换成 `RemoteShard`，零改动）
        cache.set_hot_shards(chunks.clone());
        // 节点列表进入快照（S2-4）：standalone = 本节点；R4 起由成员发现提供。
        // 放在快照里是为了让"分片归属"与 schema/manifest 同一版本，避免跨版本拼计划。
        cache.set_nodes(vec![cfg.chunk.instance_id.clone()]);
        // query 执行区内存池 = 另一块独立预算，超限直接报错而不抢 chunk 内存（架构 §2.8）
        let query = Arc::new(
            QueryEngine::with_query_memory_limit(
                store.clone(),
                cache.clone(),
                cfg.chunk.query_mem_bytes(),
            )
            .map_err(|e| {
                yuntun_model::error::LakeError::Other(format!("query memory partition: {e}"))
            })?,
        );

        // ⑦ SqlEngine（SQL 语义唯一实现；MySQL wire / Flight 共用同一份能力句柄）
        let sql = Arc::new(SqlEngine::new(
            ingestor.clone(),
            query.clone(),
            catalog.clone(),
        ));

        Ok(Self {
            catalog,
            ingestor,
            query,
            sql,
            wal,
            store,
            shutdown,
        })
    }

    /// 启动全部后台任务（缓存刷新 / 攒批 / WAL 监控 / Compaction）。
    pub fn spawn_background(&self, cfg: &Config) -> Vec<tokio::task::JoinHandle<()>> {
        let mut handles = Vec::new();

        // Query 缓存刷新（TTL 30s，§11 [query]）
        handles.push(self.query.spawn_refresh(
            self.catalog.clone(),
            Duration::from_secs(cfg.query.cache_ttl_secs),
            self.shutdown.clone(),
        ));

        // 攒批循环
        handles.push(
            self.ingestor
                .clone()
                .spawn_accumulator(self.shutdown.clone()),
        );

        // WAL 超时监控（§5.3.6.1：批次超时 abort + 磁盘水位 + segment 清理）
        // segment 清单来自 recovery 元信息（§4.8）：监控依据"区间相交的非终态 batch"判定可删
        let wal_cfg = self.wal.config().clone();
        let segments_info = self
            .wal
            .full_recovery()
            .map(|r| r.segments)
            .unwrap_or_default();
        let disk = Arc::new(yuntun_wal::cleanup::DirSizeUsage {
            dir: self.wal.shard_dir(),
            max_bytes: wal_cfg.segment_max_size * 100, // 近似：100 个 segment 配额
        });
        handles.push(yuntun_wal::cleanup::spawn_timeout_monitor(
            self.wal.clone(),
            self.ingestor.tracker.clone(),
            Some(disk),
            segments_info,
            self.shutdown.clone(),
        ));

        // Compaction
        let compactor = Arc::new(yuntun_compaction::Compactor {
            cfg: yuntun_compaction::CompactionConfig {
                min_files: cfg.compaction.min_files,
                interval: Duration::from_secs(cfg.compaction.interval_secs),
                ..Default::default()
            },
            catalog: self.catalog.clone(),
            store: self.store.clone(),
            format: yuntun_format::DataFormat::parse(&cfg.ingest.default_format),
        });
        handles.push(yuntun_compaction::spawn_compaction_loop(
            compactor,
            self.shutdown.clone(),
        ));

        // 孤儿清理（§9.1：S3 ↔ Meta 对账 + 静置期 1h）
        handles.push(yuntun_compaction::spawn_orphan_cleanup(
            self.store.clone(),
            self.catalog.clone(),
            "yuntun/".to_string(),
            Duration::from_secs(3600),
            self.shutdown.clone(),
        ));

        // 指标周期打点（T6.12）：内存水位 / WAL 积压 / 背压 / Catalog 版本
        if cfg.chunk.metrics_log_interval_secs > 0 {
            handles.push(spawn_metrics_log(
                self,
                Duration::from_secs(cfg.chunk.metrics_log_interval_secs),
                self.shutdown.clone(),
            ));
        }

        handles
    }
}

/// 指标周期打点（`plan.md` T6.12）。
///
/// **为什么单独做这件事**：压力类故障（内存越限、写入被拒、恢复变慢）在缺少指标时
/// 只能靠复现，无法定位。这里把"内存水位 / WAL 积压 / 背压 / Catalog 版本增量"打成
/// 一条结构化日志 —— standalone 阶段它就是运维的第一手现场。
pub fn spawn_metrics_log(
    lh: &Lakehouse,
    interval: Duration,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let ingestor = lh.ingestor.clone();
    let query = lh.query.clone();
    let catalog = lh.catalog.clone();
    let wal = lh.wal.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = ticker.tick() => {}
            }
            let m = collect_metrics(&ingestor, &query, &catalog, &wal).await;
            tracing::info!(
                chunk_chunks = m.chunk.chunks,
                chunk_open = m.chunk.open,
                chunk_sealed = m.chunk.sealed,
                chunk_spilled = m.chunk.spilled,
                chunk_flushed = m.chunk.flushed,
                chunk_resident_mb = m.chunk.resident_bytes / (1024 * 1024),
                chunk_budget_mb = m.chunk.budget_bytes / (1024 * 1024),
                pressure = %m.chunk.pressure,
                pressure_pct = (m.chunk.pressure_ratio * 100.0) as u64,
                phase_yielded = m.chunk.phase_yielded_flushes,
                wal_synced_seq = m.wal.synced_seq,
                wal_absorbed_seq = m.wal.absorbed_seq,
                wal_backlog = m.wal.backlog_records,
                wal_dir_mb = m.wal.dir_bytes / (1024 * 1024),
                catalog_schema_ver = m.catalog.schema_ver,
                catalog_manifest_ver = m.catalog.manifest_ver,
                catalog_tables = m.catalog.tables,
                catalog_refreshes = m.catalog.refreshes,
                catalog_full_reloads = m.catalog.full_reloads,
                catalog_delta_tables = m.catalog.delta_tables,
                query_mem_reserved_mb = m.query.reserved_bytes.map(|b| b / (1024 * 1024)),
                "metrics"
            );
        }
    })
}

/// 启动时重放 WAL 中的 DDL 记录（S1.7）。
///
/// 单节点重启后 MemoryCatalog 为空（C5），SQL CREATE/DROP 的表清单由 WAL Ddl
/// 记录重建；重放幂等（TableAlreadyExists / TableNotFound 忽略），保证 SQL 写入的数据
/// 崩溃重启后表存在、可恢复（S1.6 验收）。
async fn replay_wal_ddl(
    catalog: &Arc<dyn CatalogOps>,
    wal: &WalWriter,
) -> Result<(), yuntun_model::error::LakeError> {
    let reader = yuntun_wal::reader::WalReader::new(wal.shard_dir());
    let records = reader.scan_from(0)?;
    let mut created = 0usize;
    let mut dropped = 0usize;
    for (_, rec) in records {
        let Record::Ddl(p) = rec else { continue };
        let res = match p.op {
            ddl_op::CREATE_TABLE => {
                let schema = deserialize_schema(&p.arrow_schema)?;
                // 多 schema：WAL 中的表标识为全限定 `schema.table`
                let (ns, bare) = yuntun_model::ops::split_qualified(&p.table);
                let req = CreateTableRequest {
                    name: bare.to_string(),
                    namespace: ns.to_string(),
                    schema,
                    partition_cols: vec![],
                    default_format: if p.default_format.is_empty() {
                        "parquet".to_string()
                    } else {
                        p.default_format.clone()
                    },
                    ingest_config: IngestConfig::standard(),
                };
                match catalog.create_table(req).await {
                    Ok(_) => {
                        created += 1;
                        Ok(())
                    }
                    Err(yuntun_model::error::LakeError::TableAlreadyExists(_)) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            ddl_op::DROP_TABLE => match catalog.drop_table(&p.table).await {
                Ok(()) => {
                    dropped += 1;
                    Ok(())
                }
                Err(yuntun_model::error::LakeError::TableNotFound(_)) => Ok(()),
                Err(e) => Err(e),
            },
            // 多 schema：schema 事件（`DdlPayload.table` = schema 名）
            ddl_op::CREATE_SCHEMA => match catalog.create_schema(&p.table).await {
                Ok(()) => {
                    created += 1;
                    Ok(())
                }
                Err(yuntun_model::error::LakeError::SchemaAlreadyExists(_)) => Ok(()),
                Err(e) => Err(e),
            },
            ddl_op::DROP_SCHEMA => match catalog.drop_schema(&p.table).await {
                Ok(()) => {
                    dropped += 1;
                    Ok(())
                }
                Err(yuntun_model::error::LakeError::SchemaNotFound(_)) => Ok(()),
                Err(e) => Err(e),
            },
            other => {
                tracing::warn!(op = other, table = %p.table, "unknown ddl op in WAL, skipping");
                Ok(())
            }
        };
        if let Err(e) = res {
            tracing::warn!(table = %p.table, error = %e, "replay WAL DDL failed");
        }
    }
    if created > 0 || dropped > 0 {
        tracing::info!(created, dropped, "replayed WAL DDL records");
    }
    Ok(())
}

/// 启动 MySQL wire 协议监听（设计 §6.3 `[sql.mysql]`，标准端口 :3306）。
///
/// **bind 在本函数内同步完成**：端口被占用（如本机已有 MySQL）在启动阶段即以
/// `Err` 返回（不静默降级）；成功后接管循环交给后台任务，随 shutdown 优雅退出。
/// `enabled = false` → 返回 `Ok(None)`（不监听）。
pub async fn spawn_mysql(
    lakehouse: &Lakehouse,
    cfg: &config::MysqlSection,
) -> Result<Option<tokio::task::JoinHandle<()>>, Box<dyn std::error::Error + Send + Sync>> {
    if !cfg.enabled {
        tracing::info!("mysql wire disabled by config ([sql.mysql].enabled = false)");
        return Ok(None);
    }
    if !cfg.users.is_empty() {
        // R-1/§6.3：users 非空应切 native_password；当前仅实现 trust（无鉴权）
        tracing::warn!(
            users = cfg.users.len(),
            "mysql wire auth users configured, but only trust (no auth) is implemented — \
             connections are accepted without credential check"
        );
    }
    let listener = tokio::net::TcpListener::bind(&cfg.listen)
        .await
        .map_err(|e| format!("mysql wire bind {}: {e}", cfg.listen))?;
    tracing::info!(listen = %cfg.listen, "mysql wire protocol listening");
    let engine = lakehouse.sql.clone();
    let shutdown = lakehouse.shutdown.clone();
    Ok(Some(tokio::spawn(async move {
        if let Err(e) = yuntun_sqlwire::serve_mysql_on(engine, listener, shutdown).await {
            tracing::error!(error = %e, "mysql wire server stopped with error");
        }
    })))
}

/// 启动 Arrow Flight gRPC 服务（单端点多轨：FlightSQL 标准轨 + 简易写入/查询轨）。
pub async fn serve_flight(
    lakehouse: &Lakehouse,
    listen: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let catalog: Arc<dyn CatalogOps> = lakehouse.catalog.clone();
    let svc = arrow_flight::flight_service_server::FlightServiceServer::new(
        FlightServer::new(
            lakehouse.ingestor.clone(),
            lakehouse.query.clone(),
            catalog,
        )
        .with_sql(lakehouse.sql.clone()),
    );
    let addr = listen
        .parse::<std::net::SocketAddr>()
        .map_err(|e| format!("invalid listen addr {listen}: {e}"))?;
    tracing::info!(%addr, "flight server listening (do_put ingest + do_get sql)");
    tonic::transport::Server::builder()
        .add_service(svc)
        .serve_with_shutdown(addr, {
            let token = lakehouse.shutdown.clone();
            async move { token.cancelled().await }
        })
        .await?;
    Ok(())
}
