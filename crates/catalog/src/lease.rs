//! **一组** DML 租约（`(table, shard)` 粒度）—— `F.3d-3`，设计 §6.1。
//!
//! 设计 §6.1 的两条纪律都在这里：
//!
//! 1. **粒度 = `(table, shard)`**（评审四 R11）：合并的工作单元是 `compact_shard`
//!    （分钟级任务），表级互斥会让"同表另一个分片的删除"白等几分钟 —— 而两者碰的是
//!    不同的文件；
//! 2. **多 shard 按用途键排序、一次性获取（全或无）**（评审五 R19）：两个并发 DML 以
//!    不同顺序**增量**获取会**死锁**。所以这里是"先排序去重、再逐个取，任何一个拿不到
//!    就把已经拿到的**全部放掉**"。
//!
//! 设计里另一半（"原 CAS 校验降级为断言：lease 失效等异常路径 fail-loud，而非静默复活"）
//! 落在状态机：`CatalogState::apply_deletions` / `apply_update` 会**拒绝锚定在已下线文件上**
//! 的删除向量 —— 那种 DV 读侧按行号根本找不到行，等于一个**静默不生效的删除**。

use std::sync::Arc;

use yuntun_model::error::LakeError;
use yuntun_model::meta::dml_shard_lease_purpose;

use crate::CatalogOps;

/// 一组 DML 租约：`(table, shard)` 粒度、按用途键排序、全或无。
pub struct LeaseSet {
    catalog: Arc<dyn CatalogOps>,
    /// **已排序去重**的用途键（释放按相反顺序，对称）
    purposes: Vec<String>,
    holder: String,
    /// 每把锁的代次：`release_lease` 要求 `(holder, epoch)` 都对上才放
    /// （防"我已经被接管了，却把别人的锁放掉"）
    epochs: Vec<u64>,
}

impl std::fmt::Debug for LeaseSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseSet")
            .field("purposes", &self.purposes)
            .field("epochs", &self.epochs)
            .field("holder", &self.holder)
            .finish_non_exhaustive()
    }
}

impl LeaseSet {
    /// 获取 `table` 在 `shards` 上的全部 DML 租约。
    ///
    /// `shards` 可以是空集（例如"整表清除一张没有文件的表"）——那是一次**成功的空集合**，
    /// 不是错误：没有任何文件 ⇒ 没有任何文件会被改 ⇒ 不需要任何租约。
    ///
    /// 拿不到（别人正持有且未过期）**当场失败**，不排队：DML 是短操作，
    /// 让调用方重试比在这里阻塞好（`§149` 的既定取舍）。
    pub async fn acquire(
        catalog: Arc<dyn CatalogOps>,
        table: &str,
        shards: &[String],
        holder: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<Self, LakeError> {
        let mut purposes: Vec<String> = shards
            .iter()
            .map(|s| dml_shard_lease_purpose(table, s))
            .collect();
        // **排序去重**：两个并发 DML 因此总以同一顺序取锁 ⇒ 不会环形等待（R19）
        purposes.sort();
        purposes.dedup();
        let mut held: Vec<(String, u64)> = Vec::with_capacity(purposes.len());
        for purpose in &purposes {
            let grant = catalog.acquire_lease(purpose, holder, now_ms, ttl_ms).await?;
            if !grant.granted {
                // **全或无**：把已经拿到的放掉再报错 —— 留着一半会让"重试"越试越糟
                for (p, epoch) in held.iter().rev() {
                    if let Err(e) = catalog.release_lease(p, holder, *epoch).await {
                        tracing::warn!(purpose = %p, error = %e,
                            "回滚半途的 DML 租约失败（只影响接手方要不要等 TTL）");
                    }
                }
                return Err(LakeError::Other(format!(
                    "分片租约 {purpose} 被占用（对方代次 {}）：\
                     合并正拿着它改这个分片的基文件 —— 现在动手会让删除向量锚在被改掉的文件上\
                     （惰性失效 = 静默少删）。这是短操作，重试即可",
                    grant.epoch
                )));
            }
            held.push((purpose.clone(), grant.epoch));
        }
        Ok(Self {
            catalog,
            purposes,
            holder: holder.to_string(),
            epochs: held.into_iter().map(|(_, e)| e).collect(),
        })
    }

    /// 已持有的用途键（已排序去重）—— 调用方据此校验"我列出的文件都在我的租约覆盖内"。
    pub fn purposes(&self) -> &[String] {
        &self.purposes
    }

    pub fn holder(&self) -> &str {
        &self.holder
    }

    /// 归还全部（best-effort，**逆序**）：还失败也只影响"接手方要不要等 TTL"。
    pub async fn release(self) {
        for (purpose, epoch) in self.purposes.iter().zip(self.epochs.iter()).rev() {
            if let Err(e) = self.catalog.release_lease(purpose, &self.holder, *epoch).await {
                tracing::warn!(purpose = %purpose, error = %e, "归还 DML 租约失败（等 TTL 自动过期）");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yuntun_model::meta::dml_shard_lease_purpose as purpose;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    /// 固定时钟：同一个值取/放租约 ⇒ 不会因为"过期"把用例变成时间依赖的。
    const NOW: u64 = 1_700_000_000_000;

    /// **不同分片互不阻塞**（R11 的理由）：同表 s0 被占，s1 照样能取。
    #[tokio::test]
    async fn different_shards_of_the_same_table_do_not_block_each_other() {
        let catalog: Arc<dyn CatalogOps> = Arc::new(crate::MemoryCatalog::new());
        catalog.create_table(yuntun_model::ops::CreateTableRequest {
            name: "t".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, true),
            ])),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig::standard(),
        }).await.unwrap();
        let a = LeaseSet::acquire(catalog.clone(), "public.t", &s(&["s0"]), "a", 30_000, NOW)
            .await
            .unwrap();
        let b = LeaseSet::acquire(catalog.clone(), "public.t", &s(&["s1"]), "b", 30_000, NOW)
            .await
            .expect("不同分片必须能同时持有（这就是细粒度的意义）");
        // 同一分片则互斥
        let e = LeaseSet::acquire(catalog.clone(), "public.t", &s(&["s0"]), "c", 30_000, NOW)
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains(&purpose("public.t", "s0")), "要点名是哪把锁：{e}");
        assert!(e.contains("重试"), "短操作：要让调用方知道重试即可：{e}");
        b.release().await;
        a.release().await;
        // 放掉之后能取到
        LeaseSet::acquire(catalog.clone(), "public.t", &s(&["s0"]), "d", 30_000, NOW)
            .await
            .expect("释放之后应当能取到")
            .release()
            .await;
    }

    /// **全或无**：多 shard 里有一把拿不到 ⇒ 整个集合失败，**且不许留下半把**。
    #[tokio::test]
    async fn a_blocked_shard_fails_the_whole_set_without_leaking_leases() {
        let catalog: Arc<dyn CatalogOps> = Arc::new(crate::MemoryCatalog::new());
        catalog.create_table(yuntun_model::ops::CreateTableRequest {
            name: "t".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, true),
            ])),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig::standard(),
        }).await.unwrap();
        // 别人拿着 s1
        let other = LeaseSet::acquire(catalog.clone(), "public.t", &s(&["s1"]), "other", 30_000, NOW)
            .await
            .unwrap();
        // 我要 s0+s1+s2（排序后是 s0, s1, s2）：s0 会先拿到，s1 失败 ⇒ 必须把 s0 放掉
        let e = LeaseSet::acquire(
            catalog.clone(),
            "public.t",
            &s(&["s2", "s0", "s1"]),
            "me",
            30_000,
            NOW,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(e.contains(&purpose("public.t", "s1")), "{e}");
        // s0 必须**没被漏在我手里**：别人现在能拿到它
        LeaseSet::acquire(catalog.clone(), "public.t", &s(&["s0"]), "second", 30_000, NOW)
            .await
            .expect("失败的集合获取不许留下半把锁（漏了它，重试就会永远失败）")
            .release()
            .await;
        // s2 同样没被漏
        LeaseSet::acquire(catalog.clone(), "public.t", &s(&["s2"]), "third", 30_000, NOW)
            .await
            .expect("s2 也不许被漏")
            .release()
            .await;
        other.release().await;
    }

    /// 用途键**排序去重**：顺序不同、重复给出的两个集合等价（这是"不死锁"的机制）。
    #[tokio::test]
    async fn purposes_are_sorted_and_deduped() {
        let catalog: Arc<dyn CatalogOps> = Arc::new(crate::MemoryCatalog::new());
        catalog.create_table(yuntun_model::ops::CreateTableRequest {
            name: "t".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, true),
            ])),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig::standard(),
        }).await.unwrap();
        let set = LeaseSet::acquire(
            catalog.clone(),
            "public.t",
            &s(&["s1", "s0", "s1", "s0"]),
            "a",
            30_000,
            NOW,
        )
        .await
        .unwrap();
        assert_eq!(
            set.purposes(),
            vec![purpose("public.t", "s0"), purpose("public.t", "s1")].as_slice(),
            "排序去重之后才是稳定的获取顺序（R19）"
        );
        set.release().await;
        // 空集合是**成功的空集合**（没有文件 ⇒ 没有要改的东西 ⇒ 不需要租约）
        let empty = LeaseSet::acquire(catalog.clone(), "public.t", &[], "b", 30_000, NOW)
            .await
            .expect("空集合必须成功（否则'清空空表'这种正常路径会报租约错误）");
        assert!(empty.purposes().is_empty());
        empty.release().await;
    }
}
