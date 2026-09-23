//! 本地 Catalog 物化视图的语义回归（`refactor.md` S2-4 / S2-5 / S2-7）。
//!
//! 这组测试守的是**三类容易静默退化**的性质：
//! 1. **不可变快照**（S2-4）：一次查询取到的快照，在后续刷新后内容不得变化；
//! 2. **增量而非全量**（S2-5/S2-7）：只提交文件时，**不得**重拉无关表；
//! 3. **刷新失败不清空**：宁可读稍旧的数据，也不要查询不可用。

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_model::meta::FileManifest;
use yuntun_model::ops::{CommitFilesRequest, CreateTableRequest, ManifestDelta, CatalogVersion};
use yuntun_query::LocalCatalog;

fn schema() -> arrow::datatypes::SchemaRef {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
}

async fn make_table(c: &MemoryCatalog, name: &str) {
    c.create_table(CreateTableRequest {
        name: name.into(),
        namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
        schema: schema(),
        partition_cols: vec![],
        default_format: "parquet".into(),
        ingest_config: yuntun_model::meta::IngestConfig::standard(),
    })
    .await
    .unwrap();
}

async fn commit(c: &MemoryCatalog, table: &str, batch_id: &str) {
    c.commit_files(CommitFilesRequest {
        table: table.into(),
        batch_id: batch_id.into(),
        client_request_id: None,
        client_request_ids: vec![],
        shard: "s0".into(),
        time_window: "w".into(),
        files: vec![FileManifest {
            file_path: format!("yuntun/public/{table}/dt=w/shard=s0/{batch_id}.parquet"),
            ..Default::default()
        }],
        schema_version: 1,
        row_count: 1,
    })
    .await
    .unwrap();
}

fn catalog_ref(c: &Arc<MemoryCatalog>) -> Arc<dyn CatalogOps> {
    c.clone()
}

#[tokio::test]
async fn full_reload_then_incremental_touches_only_changed_table() {
    let cat = Arc::new(MemoryCatalog::new());
    make_table(&cat, "t1").await;
    make_table(&cat, "t2").await;
    commit(&cat, "t1", "b1").await;
    commit(&cat, "t2", "b2").await;

    let view = Arc::new(LocalCatalog::new());
    let out = view.refresh(&catalog_ref(&cat)).await.unwrap();
    assert!(out.full, "首次必须全量（本地视图为空）");
    assert_eq!(out.tables_refreshed, 2);
    assert_eq!(view.snapshot().table_count(), 2);

    // 记下无关表的条目指针：增量路径**不得**重建它
    let t2_before = view.snapshot().tables["public.t2"].clone();

    // 只给 t1 提交新文件
    commit(&cat, "t1", "b3").await;
    let out = view.refresh(&catalog_ref(&cat)).await.unwrap();
    assert!(!out.full, "仅 manifest 变化应走增量，绝不全量重建");
    assert_eq!(out.tables_refreshed, 1, "只重拉 t1");

    let snap = view.snapshot();
    assert_eq!(
        snap.tables["public.t1"].files.len(),
        2,
        "t1 拉到新文件"
    );
    let t2_after = snap.tables["public.t2"].clone();
    assert!(
        Arc::ptr_eq(&t2_before, &t2_after),
        "t2 未被触碰（同一个 Arc）—— 这是'增量'的硬证据，而不是'恰好结果一样'"
    );

    // 统计口径：1 次全量 + 1 次增量 1 张表
    let stats = view.stats();
    assert_eq!(stats.full_reloads, 1);
    assert_eq!(stats.delta_tables, 1);
}

#[tokio::test]
async fn schema_change_forces_full_reload() {
    let cat = Arc::new(MemoryCatalog::new());
    make_table(&cat, "t1").await;
    let view = Arc::new(LocalCatalog::new());
    view.refresh(&catalog_ref(&cat)).await.unwrap();
    assert_eq!(view.stats().full_reloads, 1);

    // DDL 是低频事件 → 全量重建代价可接受（换来"缓存不会带着过期 schema 干活"）
    make_table(&cat, "t2").await;
    let out = view.refresh(&catalog_ref(&cat)).await.unwrap();
    assert!(out.full, "schema_ver 变化必须全量重建");
    assert_eq!(view.snapshot().table_count(), 2);
}

#[tokio::test]
async fn snapshot_is_immutable_across_refreshes() {
    let cat = Arc::new(MemoryCatalog::new());
    make_table(&cat, "t1").await;
    let view = Arc::new(LocalCatalog::new());
    view.refresh(&catalog_ref(&cat)).await.unwrap();

    // 查询 A 取走快照
    let during_query = view.snapshot();
    assert!(during_query.get("public.t1").unwrap().files.is_empty());

    // 期间 t1 提交了文件并刷新
    commit(&cat, "t1", "b1").await;
    view.refresh(&catalog_ref(&cat)).await.unwrap();

    // 查询 A 的快照**不受影响**（S2-4：同一次查询不会看到两个版本）
    assert!(
        during_query.get("public.t1").unwrap().files.is_empty(),
        "已取走的快照必须保持不可变"
    );
    assert_eq!(
        view.snapshot().get("public.t1").unwrap().files.len(),
        1,
        "新查询看到新文件"
    );
}

#[tokio::test]
async fn no_version_change_is_zero_cost_and_keeps_same_snapshot() {
    let cat = Arc::new(MemoryCatalog::new());
    make_table(&cat, "t1").await;
    let view = Arc::new(LocalCatalog::new());
    view.refresh(&catalog_ref(&cat)).await.unwrap();

    let before = view.snapshot();
    let out = view.refresh(&catalog_ref(&cat)).await.unwrap();
    assert!(!out.full);
    assert_eq!(out.tables_refreshed, 0, "无版本变化不得重拉任何表");
    assert!(
        Arc::ptr_eq(&before, &view.snapshot()),
        "无变化时连快照都不重建（S2-6：无变化零开销返回）"
    );
}

/// 刷新失败时**保留旧快照**：查询可继续用稍旧的数据，而不是直接不可用。
#[tokio::test]
async fn failed_refresh_keeps_previous_snapshot_and_records_error() {
    let cat = Arc::new(MemoryCatalog::new());
    make_table(&cat, "t1").await;
    commit(&cat, "t1", "b1").await;

    let view = Arc::new(LocalCatalog::new());
    view.refresh(&(cat.clone() as Arc<dyn CatalogOps>)).await.unwrap();
    let good = view.snapshot();
    assert_eq!(good.get("public.t1").unwrap().files.len(), 1);
    assert!(view.last_error().is_none());

    // 换成一个"读表失败"的 Catalog（模拟 metanode 短暂不可用）
    let broken: Arc<dyn CatalogOps> = Arc::new(BrokenCatalog {
        inner: cat.clone(),
    });
    let res = view.refresh(&broken).await;
    assert!(res.is_err(), "刷新应返回错误给调用方（便于打点/告警）");
    assert!(view.last_error().is_some(), "错误必须被记录");
    assert!(
        Arc::ptr_eq(&good, &view.snapshot()),
        "失败不得清空/替换旧快照 —— 查询继续可用"
    );
}

/// 一个"除 list_tables 外全部转发"的 Catalog：用于制造刷新失败。
struct BrokenCatalog {
    inner: Arc<MemoryCatalog>,
}

#[async_trait::async_trait]
impl CatalogOps for BrokenCatalog {
    async fn create_schema(&self, n: &str) -> Result<(), yuntun_model::LakeError> {
        self.inner.create_schema(n).await
    }
    async fn register_datanode(
        &self,
        m: yuntun_model::meta::DatanodeMember,
    ) -> Result<(), yuntun_model::LakeError> {
        self.inner.register_datanode(m).await
    }
    async fn datanodes(
        &self,
    ) -> Result<Vec<yuntun_model::meta::DatanodeMember>, yuntun_model::LakeError> {
        self.inner.datanodes().await
    }
    async fn acquire_lease(
        &self,
        purpose: &str,
        holder: &str,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<yuntun_model::meta::LeaseGrant, yuntun_model::LakeError> {
        self.inner.acquire_lease(purpose, holder, now_ms, ttl_ms).await
    }
    async fn renew_lease(
        &self,
        purpose: &str,
        holder: &str,
        epoch: u64,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<bool, yuntun_model::LakeError> {
        self.inner.renew_lease(purpose, holder, epoch, now_ms, ttl_ms).await
    }
    async fn release_lease(
        &self,
        purpose: &str,
        holder: &str,
        epoch: u64,
    ) -> Result<bool, yuntun_model::LakeError> {
        self.inner.release_lease(purpose, holder, epoch).await
    }
    async fn heartbeat(&self, id: &str) -> Result<bool, yuntun_model::LakeError> {
        self.inner.heartbeat(id).await
    }
    async fn drop_schema(&self, n: &str) -> Result<(), yuntun_model::LakeError> {
        self.inner.drop_schema(n).await
    }
    async fn list_schemas(&self) -> Result<Vec<String>, yuntun_model::LakeError> {
        self.inner.list_schemas().await
    }
    async fn schema_exists(&self, n: &str) -> Result<bool, yuntun_model::LakeError> {
        self.inner.schema_exists(n).await
    }
    async fn create_table(
        &self,
        req: CreateTableRequest,
    ) -> Result<yuntun_model::meta::TableMeta, yuntun_model::LakeError> {
        self.inner.create_table(req).await
    }
    async fn get_table(
        &self,
        name: &str,
    ) -> Result<Option<yuntun_model::meta::TableMeta>, yuntun_model::LakeError> {
        self.inner.get_table(name).await
    }
    async fn list_tables(
        &self,
    ) -> Result<Vec<yuntun_model::meta::TableMeta>, yuntun_model::LakeError> {
        Err(yuntun_model::LakeError::Other("metanode unavailable".into()))
    }
    async fn evolve_schema(
        &self,
        req: yuntun_model::ops::EvolveSchemaRequest,
    ) -> Result<yuntun_model::ops::EvolveSchemaResponse, yuntun_model::LakeError> {
        self.inner.evolve_schema(req).await
    }
    async fn table_schema(
        &self,
        name: &str,
    ) -> Result<Option<(arrow::datatypes::SchemaRef, u64)>, yuntun_model::LakeError> {
        self.inner.table_schema(name).await
    }
    async fn commit_files(
        &self,
        req: CommitFilesRequest,
    ) -> Result<yuntun_model::ops::CommitFilesResponse, yuntun_model::LakeError> {
        self.inner.commit_files(req).await
    }
    async fn list_visible_files(
        &self,
        table: &str,
        snapshot: u64,
        shard_filter: Option<&str>,
    ) -> Result<Vec<FileManifest>, yuntun_model::LakeError> {
        self.inner.list_visible_files(table, snapshot, shard_filter).await
    }
    async fn drop_shard(&self, t: &str, s: &str) -> Result<u64, yuntun_model::LakeError> {
        self.inner.drop_shard(t, s).await
    }
    async fn drop_table(&self, n: &str) -> Result<(), yuntun_model::LakeError> {
        self.inner.drop_table(n).await
    }
    async fn check_idempotency(
        &self,
        k: &str,
    ) -> Result<Option<String>, yuntun_model::LakeError> {
        self.inner.check_idempotency(k).await
    }
    async fn record_idempotency(
        &self,
        rec: yuntun_model::meta::IdempotencyRecord,
    ) -> Result<(), yuntun_model::LakeError> {
        self.inner.record_idempotency(rec).await
    }
    async fn current_snapshot(&self) -> u64 {
        self.inner.current_snapshot().await
    }
    async fn read_index(&self) -> u64 {
        self.inner.read_index().await
    }
    async fn version(&self) -> CatalogVersion {
        // 版本前进（触发刷新），但下面的 list_tables 必然失败
        CatalogVersion {
            schema_ver: u64::MAX,
            manifest_ver: u64::MAX,
        }
    }
    async fn manifest_delta(
        &self,
        since: u64,
    ) -> Result<ManifestDelta, yuntun_model::LakeError> {
        self.inner.manifest_delta(since).await
    }
    async fn commit_compaction(
        &self,
        old_batch_ids: &[String],
        new_files: Vec<FileManifest>,
    ) -> Result<u64, yuntun_model::LakeError> {
        self.inner.commit_compaction(old_batch_ids, new_files).await
    }
    async fn known_batch_ids(&self) -> Result<Vec<String>, yuntun_model::LakeError> {
        self.inner.known_batch_ids().await
    }
}
