//! `yuntun-queryd` 的装配（R4 T12.1 第二刀）—— **查询节点**。
//!
//! 拆成 lib + 薄 bin 的原因很实际：端到端用例要**同时**驱动数据节点（真子进程）与查询节点
//! （真 Flight 服务），而集成测试只能引用**自己 crate 的** `CARGO_BIN_EXE_*`。
//! 于是查询节点以 lib 形式被用例起在进程内（Flight 仍然是真的：真 TCP、真协议），
//! 数据节点仍是真子进程 —— 这样"跨进程"这件事一处也不少。
//!
//! ## 与数据节点（`yuntun-ingestor`）的分工
//!
//! | | 数据节点 | **查询节点** |
//! |---|---|---|
//! | 私有状态 | WAL + chunk（**写**） | 无（**不持有任何本地数据**） |
//! | 元数据 | 向 metanode 注册 + 心跳 | 只读 metanode |
//! | 热数据 | 自己持有，对外提供 | 向各数据节点**拉**（gRPC 数据面） |
//! | 冷数据 | 写 | 读（共享对象存储） |
//! | 写入面 | 接受 | **明确拒绝**（只读节点） |
//!
//! ## 这一刀补上的那一环
//!
//! `§71` 让名录（含**数据面地址**）随元数据下发，但"**谁按地址建 `GrpcShardFetch`**"当时
//! 没有答案（`§71.5` 遗留第 3 条）。[`reconcile_hot_readers`] 就是那个答案：
//!
//! ```text
//!   名录（instance_id + address）
//!        → GrpcShardFetch::connect(address) → RemoteShard
//!        → LocalCatalog::set_hot_shards(instance_id, …)
//!        → 查询引擎按实例拉热数据（§65 的按实例切分）
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use yuntun_catalog::CatalogOps;
use yuntun_meta::RemoteCatalog;
use yuntun_query::{LocalCatalog, QueryEngine};
use yuntun_shardrpc::GrpcShardFetch;
use yuntun_store::{RemoteShard, StoreConfig, create_store};

/// 查询节点进程的装配参数（CLI 与进程内用例共用）。
#[derive(Debug, Clone)]
pub struct QuerydConfig {
    /// metanode 地址。查询节点**必须**有它：它的元数据全来自那里。
    pub meta: String,
    /// Flight SQL 监听地址（`127.0.0.1:0` = 内核分配）。
    pub listen: String,
    /// 冷数据根目录（必须与数据节点写的是**同一份共享存储**）。
    pub cold_root: PathBuf,
    /// 名录巡检间隔（秒）。
    pub reconcile_secs: u64,
}

/// 起一个查询节点：绑定监听、装配好一切、**在后台开始服务**，返回真实地址与任务句柄。
///
/// 返回真实地址是刻意的（与 metanode / ingestor 同一约定）：`--listen 127.0.0.1:0` 时端口由
/// 内核分配，上层只能从进程那里问 —— 先 bind 再移交，天然没有 TOCTOU。
pub async fn start(
    cfg: QuerydConfig,
) -> Result<(SocketAddr, tokio::task::JoinHandle<()>), Box<dyn std::error::Error + Send + Sync>> {
    // ① 元数据面：查询节点的一切元数据都来自这里（自己**不留**任何权威状态）
    let remote = Arc::new(RemoteCatalog::connect(vec![cfg.meta.clone()])?);
    let catalog: Arc<dyn CatalogOps> = remote;

    // ② 本地快照 + 名录装配（含 `§71.5` 那一步：按地址建热读器）
    let cache = Arc::new(LocalCatalog::new());
    cache.set_catalog_ops(catalog.clone());
    reconcile_hot_readers(&cache, &catalog).await?;

    let shutdown = CancellationToken::new();
    let _reconcile = spawn_reconcile(
        cache.clone(),
        catalog.clone(),
        Duration::from_secs(cfg.reconcile_secs),
        shutdown.clone(),
    );

    // ③ 查询引擎 + **只读** SQL 面（冷数据读同一份共享存储）
    let store = create_store(&StoreConfig::Local {
        root: cfg.cold_root.to_string_lossy().into_owned(),
    })?;
    let query = Arc::new(QueryEngine::new(store, cache));

    // ④ 起服务：自己 bind，才能把**真实**端口交出去
    let listener = TcpListener::bind(&cfg.listen).await?;
    let addr = listener.local_addr()?;
    let svc = arrow_flight::flight_service_server::FlightServiceServer::new(
        yuntun_server::FlightServer::new_readonly(query, catalog),
    );
    tracing::info!(%addr, meta = %cfg.meta, "queryd serving readonly flight sql");

    // 与 metanode / ingestor 同一手法：`futures::stream::unfold` 把 accept 循环包成 Stream
    let incoming = futures::stream::unfold(listener, |l| async move {
        match l.accept().await {
            Ok((sock, _addr)) => Some((Ok::<_, std::io::Error>(sock), l)),
            Err(e) => Some((Err(e), l)),
        }
    });
    let handle = tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(incoming)
            .await
        {
            tracing::error!(error = %e, "queryd flight server 退出");
        }
    });
    Ok((addr, handle))
}

/// **按名录装配热读器**（`§71.5` 遗留第 3 条）。
///
/// 两件事，顺序刻意：
/// 1. `cache.refresh(catalog)` —— 让**成员表**先落地（摘除也发生在这一步：名录里没有的实例
///    会被 `set_members` 连热读器一起摘掉，`§69`）；
/// 2. 给"有名录但**还没接线**"的成员装 `GrpcShardFetch` 包出来的 `RemoteShard`。
///
/// **只补缺、不重建**：已接线的实例保住已有连接（否则连接数会变成名录巡检频率的函数）。
pub async fn reconcile_hot_readers(
    cache: &Arc<LocalCatalog>,
    catalog: &Arc<dyn CatalogOps>,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    cache.refresh(catalog).await?;
    let wired = cache.hot_shards();
    let mut added = 0usize;
    for m in catalog.datanodes().await? {
        // 地址为空 = 同进程实例：查询节点的热读必须过网络，没有地址就接不上
        if m.address.is_empty() || wired.contains_key(&m.instance_id) {
            continue;
        }
        let fetch = Arc::new(GrpcShardFetch::connect(&m.address).await?);
        cache.set_hot_shards(m.instance_id.clone(), Arc::new(RemoteShard::new(fetch)));
        tracing::info!(instance = %m.instance_id, address = %m.address, "热读器已接线");
        added += 1;
    }
    Ok(added)
}

/// 名录巡检循环：定期 `reconcile_hot_readers`。失败只告警（元数据面抖动不该让查询节点停摆）。
fn spawn_reconcile(
    cache: Arc<LocalCatalog>,
    catalog: Arc<dyn CatalogOps>,
    every: Duration,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if shutdown.is_cancelled() {
                break;
            }
            tokio::time::sleep(every).await;
            match reconcile_hot_readers(&cache, &catalog).await {
                Ok(n) if n > 0 => tracing::info!(added = n, "名录有新增数据节点"),
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "名录巡检失败，下一轮重试"),
            }
        }
    })
}
