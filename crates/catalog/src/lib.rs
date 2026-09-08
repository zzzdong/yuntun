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
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};
use yuntun_model::error::LakeError;
use yuntun_model::meta::{
    compute_stats_lite, FileManifest, FileStatus, IdempotencyRecord, SchemaVersion, TableMeta,
};
use yuntun_model::ops::{
    validate_table_name, CommitFilesRequest, CommitFilesResponse, CreateTableRequest,
    EvolveSchemaRequest, EvolveSchemaResponse,
};
use yuntun_model::schema::{apply_change, SchemaChangeKind};

/// Catalog 操作契约（详细设计 §3.3）。
/// 阶段 1：同一 trait 由 `GrpcCatalogClient` 实现。
#[async_trait::async_trait]
pub trait CatalogOps: Send + Sync {
    // ---- 表 / Schema ----
    async fn create_table(&self, req: CreateTableRequest) -> Result<TableMeta, LakeError>;
    async fn get_table(&self, name: &str) -> Result<Option<TableMeta>, LakeError>;
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

    // ---- 幂等 ----
    async fn check_idempotency(&self, key: &str) -> Result<Option<String>, LakeError>;
    async fn record_idempotency(&self, rec: IdempotencyRecord) -> Result<(), LakeError>;

    // ---- 快照 / 线性化 ----
    /// 当前可见快照号（Query 读取用）
    async fn current_snapshot(&self) -> u64;
    /// 已 apply 的变更序号（Raft 线性化抽象，阶段 0 单调自增）
    async fn read_index(&self) -> u64;
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
    tables: RwLock<HashMap<String, TableMeta>>,
    /// (table, version) -> SchemaVersion（版本链）
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

    /// Compaction 提交：旧文件标记 deleted_at，新文件 valid_from = snapshot+1（L2，§6.3）。
    pub fn commit_compaction(
        &self,
        old_batch_ids: &[String],
        new_files: Vec<FileManifest>,
    ) -> Result<u64, LakeError> {
        let next = self.snapshot_version.load(Ordering::SeqCst) + 1;
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
        self.snapshot_version.store(next, Ordering::SeqCst);
        self.last_applied.fetch_add(1, Ordering::SeqCst);
        Ok(next)
    }
}

#[async_trait::async_trait]
impl CatalogOps for MemoryCatalog {
    async fn create_table(&self, req: CreateTableRequest) -> Result<TableMeta, LakeError> {
        validate_table_name(&req.name)?;
        let mut tables = self.tables.write().unwrap();
        if tables.contains_key(&req.name) {
            return Err(LakeError::TableAlreadyExists(req.name.clone()));
        }
        let meta = TableMeta {
            name: req.name.clone(),
            current_schema_version: 1,
            partition_cols: req.partition_cols,
            default_format: req.default_format,
            ingest_config: Some(req.ingest_config),
            created_at: now_secs(),
            arrow_schema: yuntun_model::meta::serialize_schema(&req.schema),
            table_template: 1, // General
        };
        self.schemas.write().unwrap().insert(
            (req.name.clone(), 1),
            SchemaVersion {
                version: 1,
                arrow_schema: meta.arrow_schema.clone(),
                change_kind: 0,
                created_at: now_secs(),
                change_desc: "initial schema".into(),
            },
        );
        tables.insert(req.name.clone(), meta.clone());
        self.last_applied.fetch_add(1, Ordering::SeqCst);
        Ok(meta)
    }

    async fn get_table(&self, name: &str) -> Result<Option<TableMeta>, LakeError> {
        Ok(self.tables.read().unwrap().get(name).cloned())
    }

    async fn list_tables(&self) -> Result<Vec<TableMeta>, LakeError> {
        Ok(self.tables.read().unwrap().values().cloned().collect())
    }

    /// Schema 演进（OCC，C8 —— 唯一的乐观锁作用点，详细设计 §8.2）。
    async fn evolve_schema(
        &self,
        req: EvolveSchemaRequest,
    ) -> Result<EvolveSchemaResponse, LakeError> {
        let mut tables = self.tables.write().unwrap();
        let table = tables
            .get_mut(&req.table)
            .ok_or_else(|| LakeError::TableNotFound(req.table.clone()))?;

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
            (req.table.clone(), new_version),
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
        Ok(match tables.get(name) {
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

        // ③ 分配快照号并落 Manifest
        let next = self.snapshot_version.load(Ordering::SeqCst) + 1;
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
            f.table = req.table.clone();
            f.client_request_id = req.client_request_id.clone().unwrap_or_default();
            files.insert(req.batch_id.clone(), f);
        }
        drop(files);
        self.snapshot_version.store(next, Ordering::SeqCst);
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
        let files = self.files.read().unwrap();
        Ok(files
            .values()
            .filter(|f| f.table == table)
            .filter(|f| shard_filter.is_none_or(|s| f.shard == s))
            .filter(|f| f.visible_at(snapshot))
            .cloned()
            .collect())
    }

    /// L1 分片移除（§6.3）：整 shard 文件 deleted_at = current_snapshot + 1。
    async fn drop_shard(&self, table: &str, shard: &str) -> Result<u64, LakeError> {
        let next = self.snapshot_version.load(Ordering::SeqCst) + 1;
        let mut files = self.files.write().unwrap();
        let mut n = 0u64;
        for f in files.values_mut() {
            if f.table == table && f.shard == shard && f.deleted_at == 0 {
                f.deleted_at = next;
                n += 1;
            }
        }
        drop(files);
        self.snapshot_version.store(next, Ordering::SeqCst);
        self.last_applied.fetch_add(1, Ordering::SeqCst);
        Ok(n)
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

    #[tokio::test]
    async fn create_duplicate_table_rejected() {
        let c = MemoryCatalog::new();
        let req = CreateTableRequest {
            name: "t".into(),
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
}
