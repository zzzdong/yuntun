//! Catalog 纯逻辑（详细设计 §6）：表 / Schema 版本链 / 文件 Manifest / 快照隔离 / 幂等。
//!
//! **阶段 0 用 `MemoryCatalog`，阶段 1 用 gRPC 实现，业务代码零修改。**
//!
//! 关键约束：
//! - **C5**：Catalog 仅存内存，不独立落盘（阶段 1 通过 raft snapshot 持久化，§5.4.2）
//! - **C8**：OCC 仅作用于 `EvolveSchema`，不作用于 `CommitFiles`（§8.2）
//! - 接口按 Raft 线性一致性语义抽象（`apply` / `read_index`，§13.2），
//!   阶段 0 单节点实现为自增序号，阶段 1 切换零业务改动

use arrow::datatypes::SchemaRef;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};
use yuntun_model::error::LakeError;
use yuntun_model::meta::{
    DatanodeMember, FileManifest, IdempotencyRecord, TableMeta,
    compute_stats_lite,
};
use yuntun_model::ops::{
    qualified_name, split_qualified, CatalogVersion, CommitFilesRequest, CommitFilesResponse,
    CreateTableRequest, EvolveSchemaRequest, EvolveSchemaResponse, ManifestDelta,
};
use yuntun_model::schema::SchemaChangeKind;

pub mod state;

pub use state::CatalogState;

/// Catalog 操作契约（详细设计 §3.3）。
/// 阶段 1：同一 trait 由 `GrpcCatalogClient` 实现。
#[async_trait::async_trait]
pub trait CatalogOps: Send + Sync {
    // ---- schema（MySQL 的 database 概念；多 schema 支持）----
    /// 新建 schema（幂等语义：已存在 → `SchemaAlreadyExists`）。
    async fn create_schema(&self, name: &str) -> Result<(), LakeError>;
    /// 删除空 schema（`public` 不可删；schema 下仍有表 → `SchemaNotEmpty`）。
    async fn drop_schema(&self, name: &str) -> Result<(), LakeError>;
    /// 全部 schema 名（排序；至少含 `public`）。
    async fn list_schemas(&self) -> Result<Vec<String>, LakeError>;
    /// schema 是否存在（`USE db` / handshake 校验用）。
    async fn schema_exists(&self, name: &str) -> Result<bool, LakeError>;

    // ---- 表 / Schema ----
    /// 建表：`req.name` 为裸表名，归属 `req.schema_name()`。
    async fn create_table(&self, req: CreateTableRequest) -> Result<TableMeta, LakeError>;
    /// 取表：`name` 为**全限定标识** `schema.table`（[`qualified_name`]）。
    async fn get_table(&self, name: &str) -> Result<Option<TableMeta>, LakeError>;
    /// 全部表（跨 schema；`TableMeta.namespace` 标归属）。
    async fn list_tables(&self) -> Result<Vec<TableMeta>, LakeError>;

    /// Schema 演进（OCC）—— 唯一的乐观锁作用点（C8）
    async fn evolve_schema(
        &self,
        req: EvolveSchemaRequest,
    ) -> Result<EvolveSchemaResponse, LakeError>;

    /// 当前生效的表 schema（Ingestor 攒批线程缓存刷新用）。
    async fn table_schema(&self, name: &str) -> Result<Option<(SchemaRef, u64)>, LakeError>;

    // ---- 文件 ----
    /// 提交文件清单（幂等，按 batch_id / client_request_id 去重）
    async fn commit_files(&self, req: CommitFilesRequest)
        -> Result<CommitFilesResponse, LakeError>;

    /// 查询某表在某快照下的可见文件（Manifest 驱动，C7）
    async fn list_visible_files(
        &self,
        table: &str,
        snapshot: u64,
        shard_filter: Option<&str>,
    ) -> Result<Vec<FileManifest>, LakeError>;

    // ---- 删除 ----
    /// L1 分片移除：整 shard 的文件标记 deleted_at（§6.3）
    async fn drop_shard(&self, table: &str, shard: &str) -> Result<u64, LakeError>;

    /// 删除表（S1.7 新增 meta 能力）：表 + Schema 版本链移除，
    /// 文件 Manifest 移除（数据文件转孤儿，由孤儿清理回收，plan §4.3）。
    async fn drop_table(&self, name: &str) -> Result<(), LakeError>;

    // ---- Compaction / 运维（**必须在 trait 上**）----
    /// Compaction 提交（L2，§6.3）：旧文件 `deleted_at`、新文件 `valid_from = snapshot+1`，一次原子完成。
    ///
    /// 【为什么必须在 trait 上】Compaction 与 Catalog 同进程是**部署事实**，
    /// 但**不能因此绑具体类型**：Catalog 转 gRPC（R3）时 `Arc<MemoryCatalog>` 会编译不过
    /// （`plan.md §5.1-B` 实测）。这是"单机可跑、分布式不返工"的关键接缝。
    async fn commit_compaction(
        &self,
        old_batch_ids: &[String],
        new_files: Vec<FileManifest>,
    ) -> Result<u64, LakeError>;

    /// 全部已知 batch_id（孤儿清理对账用，§12.2.1）——同上，不得绑具体实现。
    async fn known_batch_ids(&self) -> Result<Vec<String>, LakeError>;

    // ---- 幂等 ----
    async fn check_idempotency(&self, key: &str) -> Result<Option<String>, LakeError>;
    async fn record_idempotency(&self, rec: IdempotencyRecord) -> Result<(), LakeError>;

    // ---- 快照 / 线性化 / 版本 ----
    /// 当前可见快照号（Query 读取用；**快照隔离**语义，与版本号不是一回事）
    async fn current_snapshot(&self) -> u64;
    /// 已 apply 的变更序号（Raft 线性化抽象，阶段 0 单调自增）
    async fn read_index(&self) -> u64;

    /// 缓存失效用的**分组版本号**（S2-5）。
    async fn version(&self) -> CatalogVersion;

    /// 自 `since_manifest_ver` 以来文件清单变化的表（S2-7 增量接口）。
    ///
    /// 消费方只重拉 `changed_tables`，其余表缓存原样有效；
    /// 无法表达时返回 [`ManifestDelta::full`]（保守，宁可全量也不漏变更）。
    async fn manifest_delta(&self, since_manifest_ver: u64) -> Result<ManifestDelta, LakeError>;

    // ---- 数据节点名录（T12.3）----
    /// **注册数据节点**：本地实现写自己的状态；远端实现走 `Propose`（raft 的 op）。
    ///
    /// **必须是 op**：名录要与 schema/manifest **同版本**读出去（`architecture §3.1`）。
    /// **存活状态（心跳）不走这里** —— 秒级心跳会把 raft 写爆（`§3.2`）。
    async fn register_datanode(&self, m: DatanodeMember) -> Result<(), LakeError>;

    /// **数据节点名录**：必须与 [`Self::version`] / 快照**同版本**读出来。
    ///
    /// **没有默认实现是刻意的**：任何默认值（包括"空表"）都等于"没有数据节点" ——
    /// 查询会据此按空成员表算归属（`§69` 那类**静默少数据**）。
    async fn datanodes(&self) -> Result<Vec<DatanodeMember>, LakeError>;
}

/// 归一化表标识：裸名 → `public.<name>`；限定名原样（兼容 v1 单 schema 数据/调用）。
fn normalize_table(name: &str) -> String {
    let (ns, table) = split_qualified(name);
    qualified_name(ns, table)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 内存 Catalog（详细设计 §6.1）。
///
/// 即使阶段 0 单节点，写操作也走 `apply` 语义（last_applied_index 单调递增），
/// 读操作走 `read_index` 语义，为阶段 1 切换 Raft 铺路（§13.2）。
/// 单进程 Catalog **宿主**：`RwLock<CatalogState>` + 取时钟 + trait 转发。
///
/// 语义全部在 [`CatalogState`]（纯状态机，`metanode-design.md §4.1`）：本类型只做三件事
/// ——加锁、取时间、把结果发出去。R3 的 metanode 会把**同一份** `CatalogState` 交给 raft 驱动，
/// 所以这里**不得出现任何业务分支**（否则 standalone 与分布式会分叉，R2 的 `if distributed` 禁令）。
pub struct MemoryCatalog {
    state: RwLock<CatalogState>,
}

impl Default for MemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryCatalog {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(CatalogState::new()),
        }
    }

    /// 只读借用状态机（测试 / 运维；变更一律走 [`CatalogOps`]）。
    pub fn with_state<R>(&self, f: impl FnOnce(&CatalogState) -> R) -> R {
        f(&self.state.read().unwrap())
    }

    /// 幂等键 TTL 清理（后台定时调用；TTL 自 `committed_at` 起算，§7.3.1 修正 2）。
    ///
    /// `now` 由**宿主**取：R3 下这会变成一个**显式 op** —— 否则各副本按各自墙钟在不同时刻清理
    /// → 状态分叉（而这种分叉极难复现）。
    pub fn sweep_expired_idempotency(&self, ttl_secs: u64) -> usize {
        self.state
            .write()
            .unwrap()
            .sweep_expired_idempotency(ttl_secs, now_secs())
    }

    /// 状态机的规范编码（快照口径 + **确定性对拍**用，见 [`CatalogState::encode_canonical`]）。
    pub fn encode_canonical(&self) -> Vec<u8> {
        self.state.read().unwrap().encode_canonical()
    }
}

#[async_trait::async_trait]
impl CatalogOps for MemoryCatalog {
    // ---------------------------------------------------------- schema

    async fn create_schema(&self, name: &str) -> Result<(), LakeError> {
        self.state.write().unwrap().create_schema(name)
    }

    async fn drop_schema(&self, name: &str) -> Result<(), LakeError> {
        self.state.write().unwrap().drop_schema(name)
    }

    async fn list_schemas(&self) -> Result<Vec<String>, LakeError> {
        Ok(self.state.read().unwrap().list_schemas())
    }

    async fn schema_exists(&self, name: &str) -> Result<bool, LakeError> {
        Ok(self.state.read().unwrap().schema_exists(name))
    }

    // ---------------------------------------------------------- 表

    async fn create_table(&self, req: CreateTableRequest) -> Result<TableMeta, LakeError> {
        // ⚠️ 时钟在**宿主**取：状态机内读钟会让副本状态分叉（`state.rs` 纪律 1）
        let now = now_secs();
        self.state.write().unwrap().create_table(req, now)
    }

    async fn get_table(&self, name: &str) -> Result<Option<TableMeta>, LakeError> {
        Ok(self.state.read().unwrap().get_table(name))
    }

    async fn list_tables(&self) -> Result<Vec<TableMeta>, LakeError> {
        Ok(self.state.read().unwrap().list_tables())
    }

    async fn evolve_schema(
        &self,
        req: EvolveSchemaRequest,
    ) -> Result<EvolveSchemaResponse, LakeError> {
        let now = now_secs();
        self.state.write().unwrap().evolve_schema(req, now)
    }

    async fn table_schema(&self, name: &str) -> Result<Option<(SchemaRef, u64)>, LakeError> {
        self.state.read().unwrap().table_schema(name)
    }

    // ---------------------------------------------------------- 文件清单

    async fn commit_files(
        &self,
        req: CommitFilesRequest,
    ) -> Result<CommitFilesResponse, LakeError> {
        let now = now_secs();
        self.state.write().unwrap().commit_files(req, now)
    }

    async fn list_visible_files(
        &self,
        table: &str,
        snapshot: u64,
        shard_filter: Option<&str>,
    ) -> Result<Vec<FileManifest>, LakeError> {
        Ok(self
            .state
            .read()
            .unwrap()
            .list_visible_files(table, snapshot, shard_filter))
    }

    async fn drop_shard(&self, table: &str, shard: &str) -> Result<u64, LakeError> {
        Ok(self.state.write().unwrap().drop_shard(table, shard))
    }

    async fn drop_table(&self, name: &str) -> Result<(), LakeError> {
        self.state.write().unwrap().drop_table(name)
    }

    async fn check_idempotency(&self, key: &str) -> Result<Option<String>, LakeError> {
        Ok(self.state.read().unwrap().check_idempotency(key))
    }

    async fn record_idempotency(&self, rec: IdempotencyRecord) -> Result<(), LakeError> {
        self.state.write().unwrap().record_idempotency(rec);
        Ok(())
    }

    async fn current_snapshot(&self) -> u64 {
        self.state.read().unwrap().current_snapshot()
    }

    async fn read_index(&self) -> u64 {
        self.state.read().unwrap().read_index()
    }

    async fn version(&self) -> CatalogVersion {
        self.state.read().unwrap().version()
    }

    async fn register_datanode(&self, m: DatanodeMember) -> Result<(), LakeError> {
        self.state.write().unwrap().register_datanode(m);
        Ok(())
    }

    async fn datanodes(&self) -> Result<Vec<DatanodeMember>, LakeError> {
        Ok(self
            .state
            .read()
            .unwrap()
            .datanodes()
            .values()
            .cloned()
            .collect())
    }

    async fn manifest_delta(&self, since_manifest_ver: u64) -> Result<ManifestDelta, LakeError> {
        Ok(self.state.read().unwrap().manifest_delta(since_manifest_ver))
    }

    // ---------------------------------------------------------- compaction / 运维

    async fn commit_compaction(
        &self,
        old_batch_ids: &[String],
        new_files: Vec<FileManifest>,
    ) -> Result<u64, LakeError> {
        Ok(self
            .state
            .write()
            .unwrap()
            .commit_compaction(old_batch_ids, new_files))
    }

    async fn known_batch_ids(&self) -> Result<Vec<String>, LakeError> {
        Ok(self.state.read().unwrap().known_batch_ids())
    }
}

/// 从 RecordBatch 计算 CommitFiles 请求的精简统计（§5.2）。
pub fn build_files_with_stats(
    batch: &arrow::record_batch::RecordBatch,
    file_paths: &[String],
    sort_and_partition_cols: &[String],
) -> Result<Vec<FileManifest>, LakeError> {
    let stats = compute_stats_lite(batch, sort_and_partition_cols)?;
    Ok(file_paths
        .iter()
        .map(|p| FileManifest {
            file_path: p.clone(),
            ..Default::default()
        })
        .map(|mut f| {
            f.stats = Some(stats.clone());
            f
        })
        .collect())
}

/// SchemaChangeKind 数值化（proto 对齐）。
pub fn change_kind_value(kind: SchemaChangeKind) -> u32 {
    kind as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use yuntun_model::ops::DEFAULT_SCHEMA;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use yuntun_model::ops::CreateTableRequest;
    use yuntun_model::schema::{classify, SchemaChange, SchemaCompatibility};

    fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
        let fs: Vec<Field> = fields
            .iter()
            .map(|(n, t)| Field::new(*n, t.clone(), true))
            .collect();
        Arc::new(Schema::new(fs))
    }

    async fn catalog_with_table() -> MemoryCatalog {
        let c = MemoryCatalog::new();
        c.create_table(CreateTableRequest {
            name: "audit".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(&[("ts", DataType::Int64), ("user", DataType::Utf8)]),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig::standard(),
        })
        .await
        .unwrap();
        c
    }

    // ---------------- 版本分组与增量（S2-5 / S2-7）----------------

    /// 建表 / 演进 = 结构变更（`schema_ver`）；提交文件 = 清单变更（`manifest_ver`）。
    ///
    /// **两组的价值**：flush 是最高频的写。若共用一个版本号，每次 flush 都会让全表
    /// schema 缓存失效 → 缓存退化为全量重建。
    #[tokio::test]
    async fn version_splits_schema_and_manifest_changes() {
        let c = MemoryCatalog::new();
        let v0 = c.version().await;
        assert_eq!((v0.schema_ver, v0.manifest_ver), (0, 0));

        c.create_table(CreateTableRequest {
            name: "t".into(),
            namespace: DEFAULT_SCHEMA.into(),
            schema: schema(&[("a", DataType::Int64)]),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig::standard(),
        })
        .await
        .unwrap();
        let v1 = c.version().await;
        assert_eq!(v1.schema_ver, 1, "建表推 schema_ver");
        assert_eq!(v1.manifest_ver, 0, "建表不推 manifest_ver");

        commit_one(&c, "t", "b1", 1).await;
        let v2 = c.version().await;
        assert_eq!(v2.schema_ver, 1, "提交文件**不得**推 schema_ver");
        assert_eq!(v2.manifest_ver, 1);

        c.evolve_schema(EvolveSchemaRequest {
            table: "t".into(),
            change: SchemaChange::AddColumn {
                field: Field::new("b", DataType::Int64, true),
            },
            expected_version: 1,
        })
        .await
        .unwrap();
        let v3 = c.version().await;
        assert_eq!(v3.schema_ver, 2, "schema 演进推 schema_ver");
        assert_eq!(v3.manifest_ver, 1, "schema 演进不动 manifest_ver");
    }

    /// 增量接口只报"变过的表"——这是避免"每 flush 全表重拉"的关键。
    #[tokio::test]
    async fn manifest_delta_reports_only_changed_tables() {
        let c = MemoryCatalog::new();
        for name in ["t1", "t2"] {
            c.create_table(CreateTableRequest {
                name: name.into(),
                namespace: DEFAULT_SCHEMA.into(),
                schema: schema(&[("a", DataType::Int64)]),
                partition_cols: vec![],
                default_format: "parquet".into(),
                ingest_config: yuntun_model::meta::IngestConfig::standard(),
            })
            .await
            .unwrap();
        }
        commit_one(&c, "t1", "b1", 1).await;
        let after_t1 = c.version().await.manifest_ver;

        // 无变更 → 空增量（零开销返回）
        let d = c.manifest_delta(after_t1).await.unwrap();
        assert!(d.is_empty(), "无变更应返回空增量: {d:?}");

        commit_one(&c, "t2", "b2", 2).await;
        let d = c.manifest_delta(after_t1).await.unwrap();
        assert_eq!(d.changed_tables, vec!["public.t2".to_string()]);
        assert!(!d.full_reload_required);

        // since 落后到起点 → 两张表都在（首次刷新必须拿到全部）
        let d = c.manifest_delta(0).await.unwrap();
        assert_eq!(
            d.changed_tables,
            vec!["public.t1".to_string(), "public.t2".to_string()]
        );
    }

    /// 删表：结构与清单同时变（schema_ver 让调用方全量重建，增量也报该表）。
    #[tokio::test]
    async fn drop_table_advances_both_versions() {
        let c = catalog_with_table().await;
        commit_one(&c, "audit", "b1", 1).await;
        let before = c.version().await;

        c.drop_table("audit").await.unwrap();
        let after = c.version().await;
        assert!(after.schema_ver > before.schema_ver, "删表推 schema_ver");
        assert!(after.manifest_ver > before.manifest_ver, "删表推 manifest_ver");

        let d = c.manifest_delta(before.manifest_ver).await.unwrap();
        assert!(
            d.changed_tables.contains(&"public.audit".to_string()),
            "增量必须报出被删的表，否则调用方会留着过期缓存: {d:?}"
        );
    }

    /// compaction 提交同样推 manifest_ver（否则查询缓存看不到合并结果）。
    #[tokio::test]
    async fn commit_compaction_advances_manifest_version_of_old_and_new_tables() {
        let c = catalog_with_table().await;
        commit_one(&c, "audit", "b1", 1).await;
        let v0 = c.version().await.manifest_ver;

        let new_file = FileManifest {
            file_path: "yuntun/public/audit/dt=w/shard=s0/merged.parquet".into(),
            batch_id: "merged1".into(),
            table: "public.audit".into(),
            shard: "s0".into(),
            time_window: "w".into(),
            row_count: 3,
            ..Default::default()
        };
        c.commit_compaction(&["b1".to_string()], vec![new_file])
            .await
            .unwrap();
        assert!(c.version().await.manifest_ver > v0);
        let d = c.manifest_delta(v0).await.unwrap();
        assert_eq!(d.changed_tables, vec!["public.audit".to_string()]);
    }

    async fn commit_one(c: &MemoryCatalog, table: &str, batch_id: &str, snapshot_hint: u64) {
        let _ = snapshot_hint;
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

    fn manifest(batch_id: &str, path: &str) -> FileManifest {
        FileManifest {
            file_path: path.into(),
            batch_id: batch_id.into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn create_and_get_table() {
        let c = catalog_with_table().await;
        let t = c.get_table("audit").await.unwrap().unwrap();
        assert_eq!(t.current_schema_version, 1);
        assert_eq!(t.schema().unwrap().fields().len(), 2);
        assert!(c.get_table("nope").await.unwrap().is_none());
    }

    // 多 schema：建库 / 隔离 / 非空库不可删 / 默认库不可删
    #[tokio::test]
    async fn multi_schema_isolation_and_drop_rules() {
        let c = MemoryCatalog::new();
        assert_eq!(c.list_schemas().await.unwrap(), vec!["public".to_string()]);
        c.create_schema("sales").await.unwrap();
        assert!(c.schema_exists("sales").await.unwrap());
        assert!(matches!(
            c.create_schema("sales").await,
            Err(LakeError::SchemaAlreadyExists(_))
        ));
        assert_eq!(
            c.list_schemas().await.unwrap(),
            vec!["public".to_string(), "sales".to_string()]
        );

        // 未知 schema 建表 → SchemaNotFound（不再静默落到 public）
        let req_to = |ns: &str, name: &str| CreateTableRequest {
            name: name.into(),
            namespace: ns.into(),
            schema: schema(&[("v", DataType::Int64)]),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: Default::default(),
        };
        assert!(matches!(
            c.create_table(req_to("nope", "t")).await,
            Err(LakeError::SchemaNotFound(_))
        ));

        // 跨 schema 同名表：互相隔离
        c.create_table(req_to("public", "orders")).await.unwrap();
        c.create_table(req_to("sales", "orders")).await.unwrap();
        assert_eq!(
            c.get_table("public.orders")
                .await
                .unwrap()
                .unwrap()
                .schema_name(),
            "public"
        );
        assert_eq!(
            c.get_table("sales.orders")
                .await
                .unwrap()
                .unwrap()
                .schema_name(),
            "sales"
        );
        assert!(c.get_table("other.orders").await.unwrap().is_none());

        // 非空 schema 不可删；删表后可删
        assert!(matches!(
            c.drop_schema("sales").await,
            Err(LakeError::SchemaNotEmpty(_))
        ));
        c.drop_table("sales.orders").await.unwrap();
        c.drop_schema("sales").await.unwrap();
        assert!(!c.schema_exists("sales").await.unwrap());
        // 默认 schema 不可删
        assert!(c.drop_schema("public").await.is_err());
    }

    #[tokio::test]
    async fn create_duplicate_table_rejected() {
        let c = MemoryCatalog::new();
        let req = CreateTableRequest {
            name: "t".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(&[("a", DataType::Int64)]),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: Default::default(),
        };
        c.create_table(req.clone()).await.unwrap();
        assert!(matches!(
            c.create_table(req).await,
            Err(LakeError::TableAlreadyExists(_))
        ));
    }

    // T3.2：并发演进只有一个成功；CommitFiles 不校验版本（C8）
    #[tokio::test]
    async fn evolve_schema_occ_single_winner() {
        let c = catalog_with_table().await;
        let req1 = EvolveSchemaRequest {
            table: "audit".into(),
            change: SchemaChange::AddColumn {
                field: Field::new("extra", DataType::Utf8, true),
            },
            expected_version: 1,
        };
        let req2 = EvolveSchemaRequest {
            table: "audit".into(),
            change: SchemaChange::AddColumn {
                field: Field::new("other", DataType::Int64, true),
            },
            expected_version: 1, // 同一基线版本 → 只有一个能赢
        };

        let (r1, r2) = tokio::join!(c.evolve_schema(req1), c.evolve_schema(req2));
        let winners = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
        assert_eq!(winners, 1, "OCC 下并发演进只能有一个成功");
        let loser = if r1.is_err() { r1 } else { r2 };
        match loser.unwrap_err() {
            LakeError::SchemaChanged { actual_version, .. } => assert_eq!(actual_version, 2),
            e => panic!("expected SchemaChanged, got {e}"),
        }

        // 最终版本 2
        let t = c.get_table("audit").await.unwrap().unwrap();
        assert_eq!(t.current_schema_version, 2);
    }

    #[tokio::test]
    async fn evolve_widen_and_narrowing_rejected() {
        let c = catalog_with_table().await;
        // 宽化 OK
        let resp = c
            .evolve_schema(EvolveSchemaRequest {
                table: "audit".into(),
                change: SchemaChange::WidenType {
                    column: "ts".into(),
                    to: DataType::Float64,
                },
                expected_version: 1,
            })
            .await
            .unwrap();
        assert_eq!(resp.version, 2);

        // 窄化被 apply_change 拒绝
        assert!(c
            .evolve_schema(EvolveSchemaRequest {
                table: "audit".into(),
                change: SchemaChange::WidenType {
                    column: "ts".into(),
                    to: DataType::Int32,
                },
                expected_version: 2,
            })
            .await
            .is_err());
    }

    // T3.1/T3.4：快照可见性过滤
    #[tokio::test]
    async fn snapshot_isolation_and_drop_shard() {
        let c = catalog_with_table().await;
        let resp = c
            .commit_files(CommitFilesRequest {
                table: "audit".into(),
                batch_id: "b1".into(),
                client_request_id: None,
                client_request_ids: vec![],
                shard: "s0".into(),
                time_window: "w1".into(),
                files: vec![manifest("b1", "yuntun/audit/f1.parquet")],
                schema_version: 1,
                row_count: 10,
            })
            .await
            .unwrap();
        assert!(resp.accepted);
        let snap1 = resp.snapshot;

        // drop_shard → 新快照后不可见
        let n = c.drop_shard("audit", "s0").await.unwrap();
        assert_eq!(n, 1);

        // 旧快照仍可见（快照隔离），新快照不可见
        let old = c.list_visible_files("audit", snap1, None).await.unwrap();
        assert_eq!(old.len(), 1);
        let new = c
            .list_visible_files("audit", c.current_snapshot().await, None)
            .await
            .unwrap();
        assert_eq!(new.len(), 0);
    }

    // S1.7：drop_table —— 表/Schema 链/Manifest 移除，缺失表报 TableNotFound
    #[tokio::test]
    async fn drop_table_removes_table_and_files() {
        let c = catalog_with_table().await;
        c.commit_files(CommitFilesRequest {
            table: "audit".into(),
            batch_id: "b1".into(),
            client_request_id: None,
            client_request_ids: vec![],
            shard: "s0".into(),
            time_window: "w".into(),
            files: vec![manifest("b1", "yuntun/audit/f1.parquet")],
            schema_version: 1,
            row_count: 10,
        })
        .await
        .unwrap();

        let snap0 = c.current_snapshot().await;
        c.drop_table("audit").await.unwrap();
        assert!(c.current_snapshot().await > snap0, "drop 走 apply 语义");

        assert!(c.get_table("audit").await.unwrap().is_none());
        assert!(c.table_schema("audit").await.unwrap().is_none());
        let files = c
            .list_visible_files("audit", c.current_snapshot().await, None)
            .await
            .unwrap();
        assert!(files.is_empty(), "Manifest 已移除（数据文件转孤儿）");
        assert!(
            !c.known_batch_ids()
                .await
                .unwrap()
                .contains(&"b1".to_string()),
            "batch 不再已知 → 孤儿清理可回收"
        );
        assert!(matches!(
            c.drop_table("audit").await,
            Err(LakeError::TableNotFound(_))
        ));
    }

    // T3.3：幂等键独立存储 + 去重
    #[tokio::test]
    async fn commit_files_idempotent_by_batch_id_and_request_key() {
        let c = catalog_with_table().await;

        let req = CommitFilesRequest {
            table: "audit".into(),
            batch_id: "b1".into(),
            client_request_id: Some("client-key-1".into()),
            client_request_ids: vec![],
            shard: "s0".into(),
            time_window: "w1".into(),
            files: vec![manifest("b1", "yuntun/audit/f1.parquet")],
            schema_version: 1,
            row_count: 10,
        };

        let r1 = c.commit_files(req.clone()).await.unwrap();
        assert!(r1.accepted);

        // 同 batch_id 重提 → accepted=false（幂等成功，不报错）
        let r2 = c.commit_files(req.clone()).await.unwrap();
        assert!(!r2.accepted);

        // 同 client_request_id、不同 batch_id → accepted=false（防跨节点重试重复，§7.3）
        let r3 = c
            .commit_files(CommitFilesRequest {
                batch_id: "b2".into(),
                client_request_id: Some("client-key-1".into()),
                client_request_ids: vec![],
                files: vec![manifest("b2", "yuntun/audit/f2.parquet")],
                ..req
            })
            .await
            .unwrap();
        assert!(!r3.accepted);

        // Meta 只有一份数据
        let files = c
            .list_visible_files("audit", c.current_snapshot().await, None)
            .await
            .unwrap();
        assert_eq!(files.len(), 1);

        // check_idempotency
        assert_eq!(
            c.check_idempotency("client-key-1")
                .await
                .unwrap()
                .as_deref(),
            Some("b1")
        );
    }

    // T3.3：幂等键 TTL 24h（§7.3.1 修正 2）
    #[tokio::test]
    async fn idempotency_ttl_expiry() {
        let c = catalog_with_table().await;
        c.record_idempotency(IdempotencyRecord {
            client_request_id: "k".into(),
            batch_id: "b".into(),
            committed_at: now_secs() - 25 * 3600, // 25h 前 → 已过期
        })
        .await
        .unwrap();
        // TTL 24h：sweep 清掉过期记录
        let swept = c.sweep_expired_idempotency(24 * 3600);
        assert_eq!(swept, 1);
        assert!(c.check_idempotency("k").await.unwrap().is_none());
    }

    // C5：Catalog 仅存内存（结构上无落盘代码即满足；此处验证 snapshot 语义）
    #[tokio::test]
    async fn snapshot_monotonic_and_read_index() {
        let c = catalog_with_table().await;
        let s0 = c.current_snapshot().await;
        c.commit_files(CommitFilesRequest {
            table: "audit".into(),
            batch_id: "b1".into(),
            client_request_id: None,
            client_request_ids: vec![],
            shard: "s0".into(),
            time_window: "w".into(),
            files: vec![manifest("b1", "p1")],
            schema_version: 1,
            row_count: 1,
        })
        .await
        .unwrap();
        let s1 = c.current_snapshot().await;
        assert!(s1 > s0);
        assert!(c.read_index().await > 0);
    }

    // classify 集成：加列 → 演进 → 老文件仍可见（§6.2 三层模型）
    #[tokio::test]
    async fn schema_evolution_end_to_end_visibility() {
        let c = catalog_with_table().await;
        let snap0 = c.current_snapshot().await;
        c.commit_files(CommitFilesRequest {
            table: "audit".into(),
            batch_id: "old".into(),
            client_request_id: None,
            client_request_ids: vec![],
            shard: "s0".into(),
            time_window: "w".into(),
            files: vec![manifest("old", "yuntun/audit/old.parquet")],
            schema_version: 1,
            row_count: 5,
        })
        .await
        .unwrap();

        // 新 schema 到达（含新列）→ classify 判定 NeedsEvolve
        let table_schema = c.table_schema("audit").await.unwrap().unwrap().0;
        let incoming = schema(&[
            ("ts", DataType::Int64),
            ("user", DataType::Utf8),
            ("ua", DataType::Utf8),
        ]);
        match classify(&table_schema, &incoming) {
            SchemaCompatibility::NeedsEvolve(change) => {
                let v = c
                    .get_table("audit")
                    .await
                    .unwrap()
                    .unwrap()
                    .current_schema_version;
                let resp = c
                    .evolve_schema(EvolveSchemaRequest {
                        table: "audit".into(),
                        change,
                        expected_version: v,
                    })
                    .await
                    .unwrap();
                assert_eq!(resp.version, 2);
            }
            other => panic!("expected NeedsEvolve, got {other:?}"),
        }

        // 用新 schema 提交新文件
        c.commit_files(CommitFilesRequest {
            table: "audit".into(),
            batch_id: "new".into(),
            client_request_id: None,
            client_request_ids: vec![],
            shard: "s0".into(),
            time_window: "w".into(),
            files: vec![manifest("new", "yuntun/audit/new.parquet")],
            schema_version: 2,
            row_count: 5,
        })
        .await
        .unwrap();

        // 当前快照下新旧文件都可见（不同 schema_version 的文件共存是设计常态，C8）
        let files = c
            .list_visible_files("audit", c.current_snapshot().await, None)
            .await
            .unwrap();
        assert_eq!(files.len(), 2);
        let mut versions: Vec<u64> = files.iter().map(|f| f.schema_version).collect();
        versions.sort_unstable();
        assert_eq!(versions, vec![1, 2]);
        let _ = snap0;
    }

    // 【回归】快照号严格单调：并发 commit 与 drop 交错时不得回退 ——
    // 回退会让已提交文件（valid_from > snapshot）在中途"永久不可见"。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn snapshot_monotonic_under_concurrent_commit_and_drop() {
        let c = Arc::new(MemoryCatalog::new());
        let req = |name: &str| CreateTableRequest {
            name: name.into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(&[("a", DataType::Int64)]),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: Default::default(),
        };
        c.create_table(req("keep")).await.unwrap();
        c.create_table(req("churn")).await.unwrap();

        let mut handles = Vec::new();
        for i in 0..200u64 {
            let c = c.clone();
            handles.push(tokio::spawn(async move {
                if i % 2 == 0 {
                    c.commit_files(CommitFilesRequest {
                        table: "keep".into(),
                        batch_id: format!("b{i}"),
                        client_request_id: None,
                        client_request_ids: vec![],
                        shard: "s0".into(),
                        time_window: "w".into(),
                        files: vec![manifest(&format!("b{i}"), &format!("p{i}"))],
                        schema_version: 1,
                        row_count: 1,
                    })
                    .await
                    .unwrap();
                } else {
                    let _ = c.drop_table("churn").await;
                    let _ = c.create_table(req("churn")).await;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let snap = c.current_snapshot().await;
        let visible = c.list_visible_files("keep", snap, None).await.unwrap();
        assert_eq!(
            visible.len(),
            100,
            "并发 drop 不得让已提交文件因快照回退而不可见（snapshot={snap}）"
        );
    }
}
