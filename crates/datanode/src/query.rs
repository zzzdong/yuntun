//! **查询侧装配**：按名录把别的数据节点接成热读器（`GrpcShardFetch` → `RemoteShard`）。
//!
//! 这段代码原来住在 `yuntun-queryd` 里。`operation-log §79` 把角色模型更正为
//! **只有 meta / data 两类**之后，它归位到数据进程 —— 理由不是"搬家方便"，而是语义：
//!
//! - `architecture-with-chunk §4.2`：**接到 SQL 的 datanode 充当协调者** ——
//!   协调者要**同时**具备"服务自己的热数据"（服务端）与"拉别人的热数据"（客户端）两面；
//! - K4 里那个"需要时再加的 queryd"，准确说法是**不吃 WAL 的数据进程**（`--no-ingest`）——
//!   它是同一角色的开关组合，所以共用这套装配。
//!
//! `§71` 让名录（含**数据面地址**）随元数据下发，但"**谁按地址建 `GrpcShardFetch`**"当时
//! 没有答案（`§71.5` 遗留第 3 条）。这个模块就是那个答案：
//!
//! ```text
//!   名录（instance_id + address）
//!        → GrpcShardFetch::connect(address) → RemoteShard
//!        → LocalCatalog::set_hot_shards(instance_id, …)
//!        → 查询引擎按实例拉热数据（§65 的按实例切分）
//! ```

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use yuntun_catalog::CatalogOps;
use yuntun_query::LocalCatalog;
use yuntun_shardrpc::GrpcShardFetch;
use yuntun_store::RemoteShard;

/// **按名录装配热读器**（`§71.5` 遗留第 3 条）。
///
/// 两件事，顺序刻意：
/// 1. `cache.refresh(catalog)` —— 让**成员表**先落地（摘除也发生在这一步：名录里没有的实例
///    会被 `set_members` 连热读器一起摘掉，`§69`）；
/// 2. 给"有名录但**还没接线**"的成员装 `GrpcShardFetch` 包出来的 `RemoteShard`。
///
/// **只补缺、不重建**：已接线的实例保住已有连接（否则连接数会变成名录巡检频率的函数），
/// 也让调用方可以**先**把"本进程自己"（ingest 形态下的本地 chunk store）塞进去，跳过自连。
pub(crate) async fn reconcile_hot_readers(
    cache: &Arc<LocalCatalog>,
    catalog: &Arc<dyn CatalogOps>,
    hot_read_timeout: Duration,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    cache.refresh(catalog).await?;
    let wired = cache.hot_shards();
    let mut added = 0usize;
    for m in catalog.datanodes().await? {
        // 地址为空 = 同进程实例：跨进程的热读必须过网络，没有地址就接不上
        if m.address.is_empty() || wired.contains_key(&m.instance_id) {
            continue;
        }
        // 建连失败 / 超时**只影响这一个成员**：一个地址不通不该让**别的**成员接不上线
        // （此前是 `?` 直接中断整轮巡检 —— 一个坏成员就能让新成员永远不上线）。
        // 巡检是幂等的：下一轮会补，期间该成员按"读不到"参与 partial 判定。
        let fetch =
            match GrpcShardFetch::connect_with_timeout(&m.address, hot_read_timeout).await {
                Ok(f) => Arc::new(f),
                Err(e) => {
                    tracing::warn!(
                        instance = %m.instance_id,
                        address = %m.address,
                        error = %e,
                        "热读器建连失败或超时；跳过该成员，下一轮重试"
                    );
                    continue;
                }
            };
        cache.set_hot_shards(m.instance_id.clone(), Arc::new(RemoteShard::new(fetch)));
        tracing::info!(instance = %m.instance_id, address = %m.address, "热读器已接线");
        added += 1;
    }
    Ok(added)
}

/// 名录巡检循环：定期 [`reconcile_hot_readers`]。失败只告警（元数据面抖动不该让数据进程停摆）。
pub(crate) fn spawn_reconcile(
    cache: Arc<LocalCatalog>,
    catalog: Arc<dyn CatalogOps>,
    every: Duration,
    hot_read_timeout: Duration,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if shutdown.is_cancelled() {
                break;
            }
            tokio::time::sleep(every).await;
            match reconcile_hot_readers(&cache, &catalog, hot_read_timeout).await {
                Ok(n) if n > 0 => tracing::info!(added = n, "名录有新增数据节点"),
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "名录巡检失败，下一轮重试"),
            }
        }
    })
}
