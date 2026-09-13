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
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};
use yuntun_model::error::LakeError;
use yuntun_model::meta::{
    compute_stats_lite, FileManifest, FileStatus, IdempotencyRecord, SchemaVersion, TableMeta,
};
use yuntun_model::ops::{
    qualified_name, split_qualified, validate_schema_name, validate_table_name, CommitFilesRequest,
    CommitFilesResponse, CreateTableRequest, EvolveSchemaRequest, EvolveSchemaResponse,
    DEFAULT_SCHEMA,
};
use yuntun_model::schema::{apply_change, SchemaChangeKind};

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

    // ---- 幂等 ----
    async fn check_idempotency(&self, key: &str) -> Result<Option<String>, LakeError>;
    async fn record_idempotency(&self, rec: IdempotencyRecord) -> Result<(), LakeError>;

    // ---- 快照 / 线性化 ----
    /// 当前可见快照号（Query 读取用）
    async fn current_snapshot(&self) -> u64;
    /// 已 apply 的变更序号（Raft 线性化抽象，阶段 0 单调自增）
    async fn read_index(&self) -> u64;
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
pub struct MemoryCatalog {
    /// 表标识（全限定 `schema.table`）→ TableMeta
    tables: RwLock<HashMap<String, TableMeta>>,
    /// schema（MySQL 的 database）注册表；至少含 `public`
    namespaces: RwLock<HashSet<String>>,
    /// (qualified_table, version) -> SchemaVersion（版本链）
    schemas: RwLock<HashMap<(String, u64), SchemaVersion>>,
    /// batch_id -> FileManifest
    files: RwLock<HashMap<String, FileManifest>>,
    /// 幂等键独立存储（【v8 修正 1】与 FileManifest 生命周期解耦，§7.3.1）
    idempotency: RwLock<HashMap<String, IdempotencyRecord>>,
    /// 单调递增快照号（§6.3）
    snapshot_version: AtomicU64,
    /// 已 apply 的变更数（Raft 线性化抽象，T3.5）
    last_applied: AtomicU64,
}

impl Default for MemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryCatalog {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            namespaces: RwLock::new(HashSet::from([DEFAULT_SCHEMA.to_string()])),
            schemas: RwLock::new(HashMap::new()),
            files: RwLock::new(HashMap::new()),
            idempotency: RwLock::new(HashMap::new()),
            snapshot_version: AtomicU64::new(1),
            last_applied: AtomicU64::new(0),
        }
    }

    /// 幂等键 TTL 清理（后台定时调用；TTL 自 committed_at 起算，§7.3.1 修正 2）。
    pub fn sweep_expired_idempotency(&self, ttl_secs: u64) -> usize {
        let now = now_secs();
        let mut map = self.idempotency.write().unwrap();
        let before = map.len();
        map.retain(|_, r| now.saturating_sub(r.committed_at) < ttl_secs);
        before - map.len()
    }

    /// 全部已知 batch_id（孤儿清理用，§12.2.1：先排除 Meta 已知文件）。
    pub fn known_batch_ids(&self) -> Vec<String> {
        self.files.read().unwrap().keys().cloned().collect()
    }

    /// 【修复】原子推进快照号并返回新值。
    ///
    /// 快照号**必须严格单调**：文件可见性依赖 `valid_from <= snapshot`。
    /// 此前 `commit_files / commit_compaction / drop_shard` 用 `load() + 1` 再 `store()`，
    /// 与并发的 `drop_table`（`fetch_add`）交错时，晚到的 `store` 会把更大的快照号
    /// **覆盖回小值** → 已提交文件（`valid_from > snapshot`）在中途"永久不可见"，
    /// 直到下一次推进快照。统一改为原子的 `fetch_add`，返回各操作唯一的递增值。
    fn next_snapshot(&self) -> u64 {
        self.snapshot_version.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Compaction 提交：旧文件标记 deleted_at，新文件 valid_from = snapshot+1（L2，§6.3）。
    pub fn commit_compaction(
        &self,
        old_batch_ids: &[String],
        new_files: Vec<FileManifest>,
    ) -> Result<u64, LakeError> {
        let next = self.next_snapshot();
        let mut files = self.files.write().unwrap();
        // 先确认旧文件仍可见（避免与并发 drop_shard 冲突时错误复活数据）
        for id in old_batch_ids {
            if let Some(f) = files.get_mut(id) {
                if f.deleted_at == 0 {
                    f.deleted_at = next;
                }
            }
        }
        for mut nf in new_files {
            nf.valid_from = next;
            nf.status = FileStatus::Active as u32;
            files.insert(nf.batch_id.clone(), nf);
        }
        drop(files);
        self.last_applied.fetch_add(1, Ordering::SeqCst);
        Ok(next)
    }
}

#[async_trait::async_trait]
impl CatalogOps for MemoryCatalog {
    // ---------------------------------------------------------- schema

    async fn create_schema(&self, name: &str) -> Result<(), LakeError> {
        validate_schema_name(name)?;
        let mut ns = self.namespaces.write().unwrap();
        if !ns.insert(name.to_string()) {
            return Err(LakeError::SchemaAlreadyExists(name.to_string()));
        }
        drop(ns);
        self.last_applied.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn drop_schema(&self, name: &str) -> Result<(), LakeError> {
        if name == DEFAULT_SCHEMA {
            return Err(LakeError::Other(format!(
                "default schema {DEFAULT_SCHEMA:?} cannot be dropped"
            )));
        }
        if !self.schema_exists(name).await? {
            return Err(LakeError::SchemaNotFound(name.to_string()));
        }
        // 非空 schema 拒绝删除（MySQL ER_DB_DROP_EXISTS 语义）
        if self
            .tables
            .read()
            .unwrap()
            .values()
            .any(|t| t.schema_name() == name)
        {
            return Err(LakeError::SchemaNotEmpty(name.to_string()));
        }
        self.namespaces.write().unwrap().remove(name);
        self.last_applied.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn list_schemas(&self) -> Result<Vec<String>, LakeError> {
        let mut v: Vec<String> = self.namespaces.read().unwrap().iter().cloned().collect();
        v.sort();
        Ok(v)
    }

    async fn schema_exists(&self, name: &str) -> Result<bool, LakeError> {
        Ok(self.namespaces.read().unwrap().contains(name))
    }

    // ---------------------------------------------------------- 表

    async fn create_table(&self, req: CreateTableRequest) -> Result<TableMeta, LakeError> {
        validate_table_name(&req.name)?;
        let ns_name = req.schema_name().to_string();
        validate_schema_name(&ns_name)?;
        if !self.schema_exists(&ns_name).await? {
            return Err(LakeError::SchemaNotFound(ns_name));
        }
        let qualified = req.qualified_name();
        let mut tables = self.tables.write().unwrap();
        if tables.contains_key(&qualified) {
            return Err(LakeError::TableAlreadyExists(qualified));
        }
        let meta = TableMeta {
            name: req.name.clone(),
            namespace: ns_name,
            current_schema_version: 1,
            partition_cols: req.partition_cols,
            default_format: req.default_format,
            ingest_config: Some(req.ingest_config),
            created_at: now_secs(),
            arrow_schema: yuntun_model::meta::serialize_schema(&req.schema),
            table_template: 1, // General
        };
        self.schemas.write().unwrap().insert(
            (qualified.clone(), 1),
            SchemaVersion {
                version: 1,
                arrow_schema: meta.arrow_schema.clone(),
                change_kind: 0,
                created_at: now_secs(),
                change_desc: "initial schema".into(),
            },
        );
        tables.insert(qualified, meta.clone());
        self.last_applied.fetch_add(1, Ordering::SeqCst);
        Ok(meta)
    }

    /// `name` = 全限定标识 `schema.table`（无 `.` 时按默认 schema 解析，兼容旧数据）。
    async fn get_table(&self, name: &str) -> Result<Option<TableMeta>, LakeError> {
        let (ns, table) = split_qualified(name);
        Ok(self
            .tables
            .read()
            .unwrap()
            .get(&qualified_name(ns, table))
            .cloned())
    }

    async fn list_tables(&self) -> Result<Vec<TableMeta>, LakeError> {
        Ok(self.tables.read().unwrap().values().cloned().collect())
    }

    /// Schema 演进（OCC，C8 —— 唯一的乐观锁作用点，详细设计 §8.2）。
    async fn evolve_schema(
        &self,
        req: EvolveSchemaRequest,
    ) -> Result<EvolveSchemaResponse, LakeError> {
        let key = normalize_table(&req.table);
        let mut tables = self.tables.write().unwrap();
        let table = tables
            .get_mut(&key)
            .ok_or_else(|| LakeError::TableNotFound(key.clone()))?;

        // 【唯一乐观锁点】
        if table.current_schema_version != req.expected_version {
            let actual = table.current_schema_version;
            let new_schema = table.schema()?;
            return Err(LakeError::SchemaChanged {
                actual_version: actual,
                new_schema,
            });
        }

        // 按类型提升格应用变更（§8.1）
        let old_schema = table.schema()?;
        let new_schema = apply_change(&old_schema, &req.change)?;
        let new_version = table.current_schema_version + 1;

        self.schemas.write().unwrap().insert(
            (key.clone(), new_version),
            SchemaVersion {
                version: new_version,
                arrow_schema: yuntun_model::meta::serialize_schema(&new_schema),
                change_kind: req.change.kind() as u32,
                created_at: now_secs(),
                change_desc: req.change.describe(),
            },
        );
        table.current_schema_version = new_version;
        table.arrow_schema = yuntun_model::meta::serialize_schema(&new_schema);
        self.last_applied.fetch_add(1, Ordering::SeqCst);

        Ok(EvolveSchemaResponse {
            new_schema,
            version: new_version,
        })
    }

    async fn table_schema(&self, name: &str) -> Result<Option<(SchemaRef, u64)>, LakeError> {
        let tables = self.tables.read().unwrap();
        Ok(match tables.get(&normalize_table(name)) {
            Some(t) => Some((t.schema()?, t.current_schema_version)),
            None => None,
        })
    }

    /// CommitFiles 幂等实现（详细设计 §6.4 / §7.3）。
    ///
    /// C8：不校验 schema version —— 不同文件可有不同 schema_version，是设计允许的常态。
    async fn commit_files(
        &self,
        req: CommitFilesRequest,
    ) -> Result<CommitFilesResponse, LakeError> {
        // ① batch_id 幂等检查
        {
            let files = self.files.read().unwrap();
            if files.contains_key(&req.batch_id) {
                // 幂等：已提交过，返回成功（不报错）
                return Ok(CommitFilesResponse {
                    accepted: false,
                    snapshot: self.snapshot_version.load(Ordering::SeqCst),
                    commit_index: self.last_applied.load(Ordering::SeqCst),
                });
            }
        }

        // ② client_request_id 唯一索引检查（§7.3 Meta 层全局去重）
        if let Some(key) = &req.client_request_id {
            let mut idem = self.idempotency.write().unwrap();
            if idem.contains_key(key) {
                return Ok(CommitFilesResponse {
                    accepted: false,
                    snapshot: self.snapshot_version.load(Ordering::SeqCst),
                    commit_index: self.last_applied.load(Ordering::SeqCst),
                });
            }
            idem.insert(
                key.clone(),
                IdempotencyRecord {
                    client_request_id: key.clone(),
                    batch_id: req.batch_id.clone(),
                    committed_at: now_secs(),
                },
            );
        }

        // ③ 分配快照号并落 Manifest（原子推进，见 next_snapshot）
        let next = self.next_snapshot();
        let mut files = self.files.write().unwrap();
        if files.contains_key(&req.batch_id) {
            // 并发下 batch_id 重复（写锁竞态）—— 幂等返回
            return Ok(CommitFilesResponse {
                accepted: false,
                snapshot: self.snapshot_version.load(Ordering::SeqCst),
                commit_index: self.last_applied.load(Ordering::SeqCst),
            });
        }
        for mut f in req.files {
            f.valid_from = next;
            f.status = FileStatus::Active as u32;
            f.batch_id = req.batch_id.clone();
            f.schema_version = req.schema_version;
            f.shard = req.shard.clone();
            f.time_window = req.time_window.clone();
            // 归一化为全限定表标识（多 schema：跨 schema 同名表必须区分）
            f.table = normalize_table(&req.table);
            f.client_request_id = req.client_request_id.clone().unwrap_or_default();
            files.insert(req.batch_id.clone(), f);
        }
        drop(files);
        self.last_applied.fetch_add(1, Ordering::SeqCst);

        Ok(CommitFilesResponse {
            accepted: true,
            snapshot: next,
            commit_index: self.last_applied.load(Ordering::SeqCst),
        })
    }

    /// 快照可见性过滤（详细设计 §6.3）：
    /// 文件可见 ⟺ valid_from <= query_snapshot AND (deleted_at == 0 OR query_snapshot < deleted_at)
    async fn list_visible_files(
        &self,
        table: &str,
        snapshot: u64,
        shard_filter: Option<&str>,
    ) -> Result<Vec<FileManifest>, LakeError> {
        let key = normalize_table(table);
        let files = self.files.read().unwrap();
        Ok(files
            .values()
            .filter(|f| normalize_table(&f.table) == key)
            .filter(|f| shard_filter.is_none_or(|s| f.shard == s))
            .filter(|f| f.visible_at(snapshot))
            .cloned()
            .collect())
    }

    /// L1 分片移除（§6.3）：整 shard 文件 deleted_at = current_snapshot + 1。
    async fn drop_shard(&self, table: &str, shard: &str) -> Result<u64, LakeError> {
        let next = self.next_snapshot();
        let key = normalize_table(table);
        let mut files = self.files.write().unwrap();
        let mut n = 0u64;
        for f in files.values_mut() {
            if normalize_table(&f.table) == key && f.shard == shard && f.deleted_at == 0 {
                f.deleted_at = next;
                n += 1;
            }
        }
        drop(files);
        self.last_applied.fetch_add(1, Ordering::SeqCst);
        Ok(n)
    }

    /// 删除表（S1.7）：表 + Schema 版本链 + 文件 Manifest 一并移除。
    ///
    /// 数据文件本身不动 —— Manifest 移除后 S3 对象成为孤儿，
    /// 由孤儿清理循环（batch_id 对账 + 静置期）回收（plan §4.3）。
    async fn drop_table(&self, name: &str) -> Result<(), LakeError> {
        let key = normalize_table(name);
        let mut tables = self.tables.write().unwrap();
        if tables.remove(&key).is_none() {
            return Err(LakeError::TableNotFound(key));
        }
        drop(tables);
        self.schemas
            .write()
            .unwrap()
            .retain(|(t, _), _| *t != key);
        self.files
            .write()
            .unwrap()
            .retain(|_, f| normalize_table(&f.table) != key);
        self.snapshot_version.fetch_add(1, Ordering::SeqCst);
        self.last_applied.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn check_idempotency(&self, key: &str) -> Result<Option<String>, LakeError> {
        Ok(self
            .idempotency
            .read()
            .unwrap()
            .get(key)
            .map(|r| r.batch_id.clone()))
    }

    async fn record_idempotency(&self, rec: IdempotencyRecord) -> Result<(), LakeError> {
        let mut idem = self.idempotency.write().unwrap();
        idem.entry(rec.client_request_id.clone()).or_insert(rec);
        Ok(())
    }

    async fn current_snapshot(&self) -> u64 {
        self.snapshot_version.load(Ordering::SeqCst)
    }

    async fn read_index(&self) -> u64 {
        self.last_applied.load(Ordering::SeqCst)
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
            !c.known_batch_ids().contains(&"b1".to_string()),
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
