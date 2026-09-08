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
use yuntun_catalog::MemoryCatalog;
use yuntun_ingest::{Ingestor, IngestorConfig};
use yuntun_query::QueryEngine;
use yuntun_wal::writer::WalWriter;

/// 运行中的各组件句柄（供运维查询/测试断言）。
pub struct Lakehouse {
    pub catalog: Arc<MemoryCatalog>,
    pub ingestor: Arc<Ingestor>,
    pub query: Arc<QueryEngine>,
    pub wal: WalWriter,
    pub store: Arc<dyn object_store::ObjectStore>,
    pub shutdown: CancellationToken,
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
        // ① Catalog（C5：阶段 0 内存实现）
        let catalog = Arc::new(MemoryCatalog::new());

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

        // ④ Ingestor
        let ingest_cfg = IngestorConfig {
            default_format: yuntun_format::DataFormat::parse(&cfg.ingest.default_format),
            rows_threshold: cfg.ingest.rows_threshold,
            time_threshold_secs: cfg.ingest.time_threshold_secs,
            flush_jitter_secs: cfg.ingest.flush_jitter_secs,
            scan_interval: Duration::from_millis(cfg.ingest.scan_interval_ms),
            idempotency_ttl: Duration::from_secs(cfg.ingest.idempotency_ttl_hours * 3600),
            ..Default::default()
        };
        let ingestor = Arc::new(Ingestor::new(
            ingest_cfg,
            wal.clone(),
            catalog.clone(),
            store.clone(),
        ));

        // ⑤ 崩溃恢复分流（§5.6）
        let (redone, committed) = ingestor.resume_recovered().await?;
        if redone + committed > 0 {
            tracing::info!(redone, committed, "recovered batches from WAL");
        }

        // ⑥ QueryEngine（缓存刷新在 spawn_background 中启动）
        let cache = Arc::new(yuntun_query::LocalCatalogCache::new());
        let query = Arc::new(QueryEngine::new(store.clone(), cache.clone()));

        Ok(Self {
            catalog,
            ingestor,
            query,
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

        handles
    }
}

/// 启动 Arrow Flight gRPC 服务（单端点多轨：FlightSQL 标准轨 + 简易写入/查询轨）。
pub async fn serve_flight(
    lakehouse: &Lakehouse,
    listen: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let svc = arrow_flight::flight_service_server::FlightServiceServer::new(FlightServer::new(
        lakehouse.ingestor.clone(),
        lakehouse.query.clone(),
    ));
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
