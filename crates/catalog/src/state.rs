//! Catalog 的**纯状态机**（R3 `S3-2`）：只有数据 + 确定性变更。
//!
//! 设计文档：[`docs/metanode-design.md`](../../../docs/metanode-design.md) §4.1。
//!
//! # 为什么要有这一层
//!
//! R3 要把它交给 raft：**每个副本独立 apply 同一串 op，结果必须逐字节相同**。
//! 而"逐字节相同"的敌人全都是静默的 —— 违反时**常规测试仍然全绿**，
//! 只是各副本的状态随时间缓慢分叉（`metanode-design.md` R3-1）。
//!
//! 因此本模块有**四条硬纪律**（改动前先读）：
//!
//! | # | 纪律 | 反例（本文件抽出时**实际存在**的） |
//! |---|---|---|
//! | 1 | **不得读本地时钟**：所有时间由调用方传入 | `create_table`/`evolve_schema`/`commit_files` 曾直接 `now_secs()` 进状态 |
//! | 2 | **不得依赖 `HashMap`/`HashSet` 迭代序**：状态与输出一律有序容器 | `commit_compaction` 曾用 `HashSet<String>` 收集受影响表，再据其**迭代序分配版本号** → 副本间"哪个表拿到哪个 `manifest_ver`"可能不同 |
//! | 3 | **不得用随机数/UUID**（ID 由写入方生成，ADR-4） | —（现状已合规） |
//! | 4 | **版本号由状态自身分配**（不得依赖外部计数器） | `schema_ver`/`manifest_ver` 曾是 `AtomicU64`（多副本各自 ++ 会在乱序 apply 时分叉） |
//!
//! 这些纪律**无法靠代码审查长期守住**，所以本模块自带 [`CatalogState::encode_canonical`]
//! 与确定性对拍用例：**同一串 op → 逐字节相同的编码**。
//!
//! **锁、时钟、IO 都不在这里**：宿主（[`crate::MemoryCatalog`] 或 R3 的 raft 状态机）
//! 负责取时间、加锁、把结果发出去。

use std::collections::{BTreeMap, BTreeSet};

use arrow::datatypes::SchemaRef;
use yuntun_model::error::LakeError;
use yuntun_model::meta::{
    FileManifest, FileStatus, IdempotencyRecord, SchemaVersion, TableMeta,
};
use yuntun_model::ops::{
    qualified_name, split_qualified, validate_schema_name, validate_table_name, CatalogVersion,
    CommitFilesRequest, CommitFilesResponse, CreateTableRequest, EvolveSchemaRequest,
    EvolveSchemaResponse, ManifestDelta, DEFAULT_SCHEMA,
};
use yuntun_model::schema::apply_change;

use crate::normalize_table;
use yuntun_model::error::SnapshotError;
use yuntun_model::snapshot::{
    CatalogStateSnapshot, SnapshotFileEntry, SnapshotIdempotencyEntry, SnapshotSchemaEntry,
    SnapshotTableEntry, SnapshotTableVerEntry, SNAPSHOT_FORMAT_VERSION,
};

/// Catalog 的全部可变状态。
///
/// **所有容器都是 `BTreeMap`/`BTreeSet`**（纪律 2）：既让快照编码稳定，
/// 也让"按表分配版本号"这类操作在所有副本上得到同一结果。
#[derive(Debug, Clone, Default)]
pub struct CatalogState {
    /// 表标识（全限定 `schema.table`）→ TableMeta
    tables: BTreeMap<String, TableMeta>,
    /// schema（MySQL 的 database）注册表；至少含 `public`
    namespaces: BTreeSet<String>,
    /// (qualified_table, version) -> SchemaVersion（版本链）
    schemas: BTreeMap<(String, u64), SchemaVersion>,
    /// batch_id -> FileManifest
    files: BTreeMap<String, FileManifest>,
    /// 幂等键独立存储（【v8 修正 1】与 FileManifest 生命周期解耦，§7.3.1）
    idempotency: BTreeMap<String, IdempotencyRecord>,
    /// 单调递增快照号（§6.3）
    snapshot_version: u64,
    /// 已 apply 的变更数（Raft 线性化抽象）
    last_applied: u64,
    /// 结构变更计数（表 / schema / schema 演进）—— 缓存**全量重建**的触发器（S2-5）
    schema_ver: u64,
    /// 文件清单变更计数 —— 缓存**增量刷新**的触发器（S2-5）
    manifest_ver: u64,
    /// 每个表最后一次文件清单变更的 `manifest_ver`（S2-7 增量接口的数据来源）。
    ///
    /// 用"每表最后变更版本"而不是 append-only 日志：查询是 O(表数) 而不是 O(变更数)，
    /// 且不会无界增长；表被删时同步移除（删表走 schema_ver → 调用方全量重建）。
    table_manifest_ver: BTreeMap<String, u64>,
}

impl CatalogState {
    pub fn new() -> Self {
        Self {
            namespaces: BTreeSet::from([DEFAULT_SCHEMA.to_string()]),
            snapshot_version: 1,
            ..Default::default()
        }
    }

    // ---------------------------------------------------------------- 内部：版本推进

    /// 结构变更：推进 `schema_ver`（缓存全量重建）。
    fn bump_schema_ver(&mut self) -> u64 {
        self.schema_ver += 1;
        self.schema_ver
    }

    /// 文件清单变更：推进 `manifest_ver` 并记下该表的最后变更版本（缓存增量刷新）。
    fn bump_manifest_ver(&mut self, table: &str) -> u64 {
        self.manifest_ver += 1;
        let v = self.manifest_ver;
        self.table_manifest_ver
            .insert(normalize_table(table), v);
        v
    }

    /// 原子推进快照号并返回新值（快照号**必须严格单调**：文件可见性依赖 `valid_from <= snapshot`）。
    fn next_snapshot(&mut self) -> u64 {
        self.snapshot_version += 1;
        self.snapshot_version
    }

    // ---------------------------------------------------------------- schema

    pub fn create_schema(&mut self, name: &str) -> Result<(), LakeError> {
        validate_schema_name(name)?;
        if !self.namespaces.insert(name.to_string()) {
            return Err(LakeError::SchemaAlreadyExists(name.to_string()));
        }
        self.bump_schema_ver();
        self.last_applied += 1;
        Ok(())
    }

    pub fn drop_schema(&mut self, name: &str) -> Result<(), LakeError> {
        if name == DEFAULT_SCHEMA {
            return Err(LakeError::Other(format!(
                "default schema {DEFAULT_SCHEMA:?} cannot be dropped"
            )));
        }
        if !self.namespaces.contains(name) {
            return Err(LakeError::SchemaNotFound(name.to_string()));
        }
        // 非空 schema 拒绝删除（MySQL ER_DB_DROP_EXISTS 语义）
        if self.tables.values().any(|t| t.schema_name() == name) {
            return Err(LakeError::SchemaNotEmpty(name.to_string()));
        }
        self.namespaces.remove(name);
        self.bump_schema_ver();
        self.last_applied += 1;
        Ok(())
    }

    pub fn list_schemas(&self) -> Vec<String> {
        self.namespaces.iter().cloned().collect()
    }

    pub fn schema_exists(&self, name: &str) -> bool {
        self.namespaces.contains(name)
    }

    // ---------------------------------------------------------------- 表

    /// 建表。`now_secs` **由调用方传入**（纪律 1）：单机由宿主取钟，R3 由 op 携带。
    pub fn create_table(
        &mut self,
        req: CreateTableRequest,
        now_secs: u64,
    ) -> Result<TableMeta, LakeError> {
        validate_table_name(&req.name)?;
        let ns_name = req.schema_name().to_string();
        validate_schema_name(&ns_name)?;
        if !self.schema_exists(&ns_name) {
            return Err(LakeError::SchemaNotFound(ns_name));
        }
        let qualified = req.qualified_name();
        if self.tables.contains_key(&qualified) {
            return Err(LakeError::TableAlreadyExists(qualified));
        }
        let meta = TableMeta {
            name: req.name.clone(),
            namespace: ns_name,
            current_schema_version: 1,
            partition_cols: req.partition_cols,
            default_format: req.default_format,
            ingest_config: Some(req.ingest_config),
            created_at: now_secs,
            arrow_schema: yuntun_model::meta::serialize_schema(&req.schema),
            table_template: 1, // General
        };
        self.schemas.insert(
            (qualified.clone(), 1),
            SchemaVersion {
                version: 1,
                arrow_schema: meta.arrow_schema.clone(),
                change_kind: 0,
                created_at: now_secs,
                change_desc: "initial schema".into(),
            },
        );
        self.tables.insert(qualified, meta.clone());
        self.bump_schema_ver();
        self.last_applied += 1;
        Ok(meta)
    }

    /// `name` = 全限定标识 `schema.table`（无 `.` 时按默认 schema 解析，兼容旧数据）。
    pub fn get_table(&self, name: &str) -> Option<TableMeta> {
        let (ns, table) = split_qualified(name);
        self.tables.get(&qualified_name(ns, table)).cloned()
    }

    /// 列表**按表名排序**（纪律 2）：无序返回会让"重建缓存"这类消费方行为不稳定。
    pub fn list_tables(&self) -> Vec<TableMeta> {
        self.tables.values().cloned().collect()
    }

    /// Schema 演进（OCC，C8 —— 唯一的乐观锁作用点，详细设计 §8.2）。
    pub fn evolve_schema(
        &mut self,
        req: EvolveSchemaRequest,
        now_secs: u64,
    ) -> Result<EvolveSchemaResponse, LakeError> {
        let key = normalize_table(&req.table);
        let table = self
            .tables
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

        self.schemas.insert(
            (key.clone(), new_version),
            SchemaVersion {
                version: new_version,
                arrow_schema: yuntun_model::meta::serialize_schema(&new_schema),
                change_kind: req.change.kind() as u32,
                created_at: now_secs,
                change_desc: req.change.describe(),
            },
        );
        table.current_schema_version = new_version;
        table.arrow_schema = yuntun_model::meta::serialize_schema(&new_schema);
        self.bump_schema_ver();
        self.last_applied += 1;

        Ok(EvolveSchemaResponse {
            new_schema,
            version: new_version,
        })
    }

    pub fn table_schema(&self, name: &str) -> Result<Option<(SchemaRef, u64)>, LakeError> {
        Ok(match self.tables.get(&normalize_table(name)) {
            Some(t) => Some((t.schema()?, t.current_schema_version)),
            None => None,
        })
    }

    // ---------------------------------------------------------------- 文件清单

    /// CommitFiles 幂等实现（详细设计 §6.4 / §7.3）。
    ///
    /// C8：不校验 schema version —— 不同文件可有不同 schema_version，是设计允许的常态。
    pub fn commit_files(
        &mut self,
        req: CommitFilesRequest,
        now_secs: u64,
    ) -> Result<CommitFilesResponse, LakeError> {
        // ① batch_id 幂等检查（已提交过 → 成功返回，不报错）
        if self.files.contains_key(&req.batch_id) {
            return Ok(self.dup_commit_response());
        }

        // ② 幂等键唯一索引检查（§7.3 Meta 层全局去重）；
        //    `client_request_ids` 是**键集合**（一个 chunk 可聚合多个键，§27.5 遗留 #1）
        //
        // ⚠️ **"已被认领" ≠ "重复提交"** —— 这里必须区分两种存在形态（否则会把正常写入判重、
        // 数据永远不落 manifest；R3 S3-5 接线时正是被 chaos 用例当场抓到）：
        //
        // | 记录形态 | 含义 | 处理 |
        // |---|---|---|
        // | `batch_id` **为空** | ingest 入口在 WAL fsync 后**认领**了该键（§27：防并发同键双写），或**重启后从 WAL 重建**的索引 | **补全**为本批次 —— 这正是本次提交要落盘的数据 |
        // | `batch_id` 非空且**不是本批次** | 另一个**已提交**批次占用了该键 | 整次判重（`accepted=false`） |
        let keys: Vec<String> = req
            .client_request_ids
            .iter()
            .cloned()
            .chain(req.client_request_id.clone())
            .collect();
        for key in &keys {
            if let Some(rec) = self.idempotency.get(key) {
                if !rec.batch_id.is_empty() && rec.batch_id != req.batch_id {
                    return Ok(self.dup_commit_response());
                }
            }
        }
        for key in &keys {
            // 认领 → 补全 `committed_at` 以提交时刻为准（TTL 自提交起算，§7.3.1 修正 2）
            self.idempotency.insert(
                key.clone(),
                IdempotencyRecord {
                    client_request_id: key.clone(),
                    batch_id: req.batch_id.clone(),
                    committed_at: now_secs,
                },
            );
        }

        // ③ 分配快照号并落 Manifest
        let next = self.next_snapshot();
        for mut f in req.files {
            f.valid_from = next;
            f.status = FileStatus::Active as u32;
            f.batch_id = req.batch_id.clone();
            f.schema_version = req.schema_version;
            f.shard = req.shard.clone();
            f.time_window = req.time_window.clone();
            // 归一化为全限定表标识（多 schema：跨 schema 同名表必须区分）
            f.table = normalize_table(&req.table);
            f.client_request_id = req
                .client_request_ids
                .first()
                .cloned()
                .or_else(|| req.client_request_id.clone())
                .unwrap_or_default();
            self.files.insert(req.batch_id.clone(), f);
        }
        self.bump_manifest_ver(&req.table);
        self.last_applied += 1;

        Ok(CommitFilesResponse {
            accepted: true,
            snapshot: next,
            commit_index: self.last_applied,
        })
    }

    /// 幂等命中时的回执（`accepted=false` + 当前版本，不推进任何版本号）。
    fn dup_commit_response(&self) -> CommitFilesResponse {
        CommitFilesResponse {
            accepted: false,
            snapshot: self.snapshot_version,
            commit_index: self.last_applied,
        }
    }

    /// 快照可见性过滤（详细设计 §6.3）：
    /// 文件可见 ⟺ valid_from <= query_snapshot AND (deleted_at == 0 OR query_snapshot < deleted_at)
    pub fn list_visible_files(
        &self,
        table: &str,
        snapshot: u64,
        shard_filter: Option<&str>,
    ) -> Vec<FileManifest> {
        let key = normalize_table(table);
        self.files
            .values()
            .filter(|f| normalize_table(&f.table) == key)
            .filter(|f| shard_filter.is_none_or(|s| f.shard == s))
            .filter(|f| f.visible_at(snapshot))
            .cloned()
            .collect()
    }

    /// L1 分片移除（§6.3）：整 shard 文件 `deleted_at = current_snapshot + 1`。
    pub fn drop_shard(&mut self, table: &str, shard: &str) -> u64 {
        let next = self.next_snapshot();
        let key = normalize_table(table);
        let mut n = 0u64;
        for f in self.files.values_mut() {
            if normalize_table(&f.table) == key && f.shard == shard && f.deleted_at == 0 {
                f.deleted_at = next;
                n += 1;
            }
        }
        self.bump_manifest_ver(&key);
        self.last_applied += 1;
        n
    }

    /// 删除表（S1.7）：表 + Schema 版本链 + 文件 Manifest 一并移除。
    ///
    /// 数据文件本身不动 —— Manifest 移除后 S3 对象成为孤儿，
    /// 由孤儿清理循环（batch_id 对账 + 静置期）回收（plan §4.3）。
    pub fn drop_table(&mut self, name: &str) -> Result<(), LakeError> {
        let key = normalize_table(name);
        if self.tables.remove(&key).is_none() {
            return Err(LakeError::TableNotFound(key));
        }
        self.schemas.retain(|(t, _), _| *t != key);
        self.files.retain(|_, f| normalize_table(&f.table) != key);
        self.snapshot_version += 1;
        // 结构与清单都变了：schema_ver 让缓存全量重建，manifest_ver 兜一层增量消费者
        self.bump_schema_ver();
        self.bump_manifest_ver(&key);
        self.last_applied += 1;
        Ok(())
    }

    // ---------------------------------------------------------------- 幂等

    pub fn check_idempotency(&self, key: &str) -> Option<String> {
        self.idempotency.get(key).map(|r| r.batch_id.clone())
    }

    /// 登记幂等键（**已存在则保留首次** —— 重试不覆盖权威记录）。
    pub fn record_idempotency(&mut self, rec: IdempotencyRecord) {
        self.idempotency
            .entry(rec.client_request_id.clone())
            .or_insert(rec);
    }

    /// 幂等键 TTL 清理（§7.3.1 修正 2：TTL 自 `committed_at` 起算）。
    ///
    /// `now_secs` 由调用方传入（纪律 1）：R3 下这必须是一个**显式 op**，
    /// 否则各个副本会按各自的墙钟在不同时刻清理 → 状态分叉（且很难复现）。
    pub fn sweep_expired_idempotency(&mut self, ttl_secs: u64, now_secs: u64) -> usize {
        let before = self.idempotency.len();
        self.idempotency
            .retain(|_, r| now_secs.saturating_sub(r.committed_at) < ttl_secs);
        before - self.idempotency.len()
    }

    // ---------------------------------------------------------------- 版本 / 读索引

    pub fn current_snapshot(&self) -> u64 {
        self.snapshot_version
    }

    pub fn read_index(&self) -> u64 {
        self.last_applied
    }

    pub fn version(&self) -> CatalogVersion {
        CatalogVersion {
            schema_ver: self.schema_ver,
            manifest_ver: self.manifest_ver,
        }
    }

    pub fn manifest_delta(&self, since_manifest_ver: u64) -> ManifestDelta {
        if since_manifest_ver >= self.manifest_ver {
            return ManifestDelta::default();
        }
        ManifestDelta {
            changed_tables: self
                .table_manifest_ver
                .iter()
                .filter(|(_, v)| **v > since_manifest_ver)
                .map(|(t, _)| t.clone())
                .collect(),
            full_reload_required: false,
        }
    }

    // ---------------------------------------------------------------- compaction / 运维

    /// Compaction 提交（L2，§6.3）：旧文件标记 `deleted_at`，新文件 `valid_from = snapshot+1`，
    /// 一次原子完成；并推进受影响表的 `manifest_ver`（供缓存增量刷新）。
    pub fn commit_compaction(
        &mut self,
        old_batch_ids: &[String],
        new_files: Vec<FileManifest>,
    ) -> u64 {
        let next = self.next_snapshot();
        // ⚠️ `BTreeSet` 而不是 `HashSet`（纪律 2）：下面的 `bump_manifest_ver` 会**按迭代序分配
        // 版本号**，用 `HashSet` 会让不同副本把同一个 `manifest_ver` 分给不同的表 → 静默分叉。
        let mut touched: BTreeSet<String> = BTreeSet::new();
        for id in old_batch_ids {
            if let Some(f) = self.files.get_mut(id) {
                if f.deleted_at == 0 {
                    f.deleted_at = next;
                }
                touched.insert(normalize_table(&f.table));
            }
        }
        for mut nf in new_files {
            nf.valid_from = next;
            nf.status = FileStatus::Active as u32;
            touched.insert(normalize_table(&nf.table));
            self.files.insert(nf.batch_id.clone(), nf);
        }
        self.last_applied += 1;
        for t in &touched {
            self.bump_manifest_ver(t);
        }
        next
    }

    /// 已知 batch_id 列表（孤儿清理用）—— **排序**返回（纪律 2）。
    pub fn known_batch_ids(&self) -> Vec<String> {
        self.files.keys().cloned().collect()
    }

    // ---------------------------------------------------------------- 确定性编码

    // ---------------------------------------------------------------- 快照（metanode-design §4.4）

    /// 导出**完整**快照消息（逐字段；不做校验）。
    ///
    /// 与 [`Self::encode_canonical`] 的区别是**目的不同**：那个是给"对拍"用的可读文本
    /// （只含能区分状态的字段），这个是给"换主/重启/新节点追日志"用的**无损**载荷。
    /// 少一个字段 = 状态静默回退，所以这里必须逐字段搬，并靠
    /// `snapshot_covers_every_state_dimension` 测试兜住"加字段忘加进快照"。
    pub fn to_snapshot_msg(&self) -> CatalogStateSnapshot {
        CatalogStateSnapshot {
            format_version: SNAPSHOT_FORMAT_VERSION as u64,
            revision: self.snapshot_version,
            last_applied: self.last_applied,
            schema_ver: self.schema_ver,
            manifest_ver: self.manifest_ver,
            namespaces: self.namespaces.iter().cloned().collect(),
            tables: self
                .tables
                .iter()
                .map(|(k, v)| SnapshotTableEntry {
                    name: k.clone(),
                    meta: Some(v.clone()),
                })
                .collect(),
            schemas: self
                .schemas
                .iter()
                .map(|((t, ver), sv)| SnapshotSchemaEntry {
                    table: t.clone(),
                    version: *ver,
                    value: Some(sv.clone()),
                })
                .collect(),
            files: self
                .files
                .iter()
                .map(|(k, v)| SnapshotFileEntry {
                    batch_id: k.clone(),
                    manifest: Some(v.clone()),
                })
                .collect(),
            idempotency: self
                .idempotency
                .iter()
                .map(|(k, v)| SnapshotIdempotencyEntry {
                    key: k.clone(),
                    record: Some(v.clone()),
                })
                .collect(),
            table_manifest_ver: self
                .table_manifest_ver
                .iter()
                .map(|(t, v)| SnapshotTableVerEntry {
                    table: t.clone(),
                    manifest_ver: *v,
                })
                .collect(),
        }
    }

    /// 从快照消息**严格**重建。
    ///
    /// 校验规则（**拒绝**而非"尽力恢复" —— 静默取"最后一个"会让副本间状态分歧）：
    ///
    /// | 规则 | 理由 |
    /// |---|---|
    /// | `format_version` 必须等于本实现版本 | 载荷布局变了就必须显式拒绝，不能猜 |
    /// | `revision > 0` | 0 表示"快照号未初始化"，不是合法状态 |
    /// | `namespaces` 必须含 `public` | 状态机不变量（[`Self::new`] 保证） |
    /// | 各列表**键不得重复** | 重复键 → "谁生效"取决于遍历顺序 = 不确定性 |
    /// | 条目值不得为 `None` | 空条目是构造错误，不是合法状态 |
    ///
    /// **不**校验的（有意）：文件与表的引用完整性。事件到达顺序可能让快照里出现
    /// "文件引用了已删表"的中间态（`drop_table` 与 `commit_files` 的交错），
    /// 硬校验会让**合法快照装不进去** —— 那比少校验危险得多。
    pub fn from_snapshot_msg(msg: CatalogStateSnapshot) -> Result<Self, SnapshotError> {
        if msg.format_version != SNAPSHOT_FORMAT_VERSION as u64 {
            return Err(SnapshotError::UnsupportedVersion(msg.format_version as u32));
        }
        if msg.revision == 0 {
            return Err(SnapshotError::InvalidState(
                "revision 必须 > 0（0 = 快照号未初始化）".into(),
            ));
        }
        if !msg.namespaces.iter().any(|n| n == DEFAULT_SCHEMA) {
            return Err(SnapshotError::InvalidState(format!(
                "namespaces 必须含 {DEFAULT_SCHEMA}"
            )));
        }
        let mut namespaces = BTreeSet::new();
        for n in &msg.namespaces {
            if !namespaces.insert(n.clone()) {
                return Err(SnapshotError::InvalidState(format!("schema {n} 重复")));
            }
        }
        let mut tables: BTreeMap<String, TableMeta> = BTreeMap::new();
        for e in &msg.tables {
            let v = e
                .meta
                .clone()
                .ok_or_else(|| SnapshotError::InvalidState(format!("表 {} 缺 meta", e.name)))?;
            if tables.insert(e.name.clone(), v).is_some() {
                return Err(SnapshotError::InvalidState(format!("表 {} 重复", e.name)));
            }
        }
        let mut schemas: BTreeMap<(String, u64), SchemaVersion> = BTreeMap::new();
        for e in &msg.schemas {
            let v = e.value.clone().ok_or_else(|| {
                SnapshotError::InvalidState(format!("schema {} v{} 缺值", e.table, e.version))
            })?;
            if schemas.insert((e.table.clone(), e.version), v).is_some() {
                return Err(SnapshotError::InvalidState(format!(
                    "schema {} v{} 重复",
                    e.table, e.version
                )));
            }
        }
        let mut files: BTreeMap<String, FileManifest> = BTreeMap::new();
        for e in &msg.files {
            let v = e.manifest.clone().ok_or_else(|| {
                SnapshotError::InvalidState(format!("文件 {} 缺 manifest", e.batch_id))
            })?;
            if files.insert(e.batch_id.clone(), v).is_some() {
                return Err(SnapshotError::InvalidState(format!(
                    "文件 {} 重复",
                    e.batch_id
                )));
            }
        }
        let mut idempotency: BTreeMap<String, IdempotencyRecord> = BTreeMap::new();
        for e in &msg.idempotency {
            let v = e
                .record
                .clone()
                .ok_or_else(|| SnapshotError::InvalidState(format!("幂等键 {} 缺记录", e.key)))?;
            if idempotency.insert(e.key.clone(), v).is_some() {
                return Err(SnapshotError::InvalidState(format!("幂等键 {} 重复", e.key)));
            }
        }
        let mut table_manifest_ver: BTreeMap<String, u64> = BTreeMap::new();
        for e in &msg.table_manifest_ver {
            if table_manifest_ver
                .insert(e.table.clone(), e.manifest_ver)
                .is_some()
            {
                return Err(SnapshotError::InvalidState(format!(
                    "table_manifest_ver {} 重复",
                    e.table
                )));
            }
        }
        Ok(Self {
            tables,
            namespaces,
            schemas,
            files,
            idempotency,
            snapshot_version: msg.revision,
            last_applied: msg.last_applied,
            schema_ver: msg.schema_ver,
            manifest_ver: msg.manifest_ver,
            table_manifest_ver,
        })
    }

    /// **快照产物** = 无损载荷打上帧（分块 + CRC）。
    ///
    /// 这是可直接写进 fjall / 对象存储 / 经 gRPC 传输的字节。
    pub fn snapshot_artifact(&self) -> Vec<u8> {
        let payload = yuntun_model::snapshot::encode_payload(&self.to_snapshot_msg());
        yuntun_model::snapshot::frame(&payload, self.snapshot_version, 0)
    }

    /// 从快照产物恢复（解帧 → 解载荷 → 严格重建）。
    ///
    /// 帧头的 `revision` 必须与载荷里的 `revision` 一致：不一致说明帧与载荷
    /// **不是同一次快照产生的**（拼接/回滚搞混），装上去必错。
    pub fn restore_snapshot(bytes: &[u8]) -> Result<Self, SnapshotError> {
        let un = yuntun_model::snapshot::unframe(bytes)?;
        let msg = yuntun_model::snapshot::decode_payload(&un.payload)?;
        if msg.revision != un.revision {
            return Err(SnapshotError::RevisionMismatch {
                framed: un.revision,
                payload: msg.revision,
            });
        }
        Self::from_snapshot_msg(msg)
    }

    /// 状态机的**规范编码**：**确定性对拍**用（同一串 op → 同一段字节），人可读。
    ///
    /// 编码里必须包含**所有**会影响后续行为的字段 —— 少一个字段，对拍就会漏掉一类分叉：
    /// 两组版本号、每表最后变更版本、快照号、幂等索引、版本链的 `created_at` …
    ///
    /// ⚠️ 本函数**不是**快照格式：它只编码能区分状态的字段（够对拍、人可读）。
    /// 换主/重启/新节点追日志要用**无损**的 [`Self::snapshot_artifact`]，两者别混用。
    pub fn encode_canonical(&self) -> Vec<u8> {
        use std::fmt::Write as _;
        let mut s = String::new();
        let _ = writeln!(
            s,
            "v1 snapshot={} applied={} schema_ver={} manifest_ver={}",
            self.snapshot_version, self.last_applied, self.schema_ver, self.manifest_ver
        );
        // 顺序全部来自 BTreeMap/BTreeSet 的键序 → 与插入历史无关，只与最终状态有关
        for ns in &self.namespaces {
            let _ = writeln!(s, "ns {ns}");
        }
        for (k, t) in &self.tables {
            let _ = writeln!(
                s,
                "table {k} v={} created={} fmt={} arrow_len={}",
                t.current_schema_version,
                t.created_at,
                t.default_format,
                t.arrow_schema.len()
            );
        }
        for ((t, v), sv) in &self.schemas {
            let _ = writeln!(
                s,
                "schema {t} v={v} kind={} created={} desc={} arrow_len={}",
                sv.change_kind,
                sv.created_at,
                sv.change_desc,
                sv.arrow_schema.len()
            );
        }
        for (id, f) in &self.files {
            let _ = writeln!(
                s,
                "file {id} table={} shard={} status={} from={} del={} rows={} size={} schema_v={} window={}",
                f.table,
                f.shard,
                f.status,
                f.valid_from,
                f.deleted_at,
                f.row_count,
                f.file_size,
                f.schema_version,
                f.time_window
            );
        }
        for (k, r) in &self.idempotency {
            let _ = writeln!(
                s,
                "idem {k} batch={} at={}",
                r.batch_id, r.committed_at
            );
        }
        for (t, v) in &self.table_manifest_ver {
            let _ = writeln!(s, "tmv {t} {v}");
        }
        s.into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use yuntun_model::meta::IngestConfig;
    use yuntun_model::schema::SchemaChange;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]))
    }

    fn create_table_req(name: &str) -> CreateTableRequest {
        CreateTableRequest {
            name: name.into(),
            namespace: DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: IngestConfig::standard(),
        }
    }

    fn commit_req(table: &str, batch_id: &str, keys: &[&str]) -> CommitFilesRequest {
        CommitFilesRequest {
            table: table.into(),
            batch_id: batch_id.into(),
            client_request_id: keys.first().map(|s| s.to_string()),
            client_request_ids: keys.iter().map(|s| s.to_string()).collect(),
            shard: "s0".into(),
            time_window: "w1".into(),
            files: vec![FileManifest {
                file_path: format!("p/{batch_id}.parquet"),
                row_count: 1,
                ..Default::default()
            }],
            schema_version: 1,
            row_count: 1,
        }
    }

    fn merged_file(table: &str, batch_id: &str, rows: u64) -> FileManifest {
        FileManifest {
            file_path: format!("p/{batch_id}.parquet"),
            batch_id: batch_id.into(),
            table: table.into(),
            shard: "s0".into(),
            time_window: "w1".into(),
            row_count: rows,
            ..Default::default()
        }
    }

    /// **R3-1 的防线**：同一串 op（含相同的时间戳）在两个独立状态机上 →
    /// 规范编码**逐字节相同**。不成立就意味着副本会随时间静默分叉。
    #[test]
    fn same_op_sequence_yields_byte_identical_state() {
        let run = || {
            let mut st = CatalogState::new();
            st.create_schema("analytics").unwrap();
            st.create_table(create_table_req("cpu"), 1_000).unwrap();
            st.create_table(create_table_req("mem"), 1_001).unwrap();
            st.evolve_schema(
                EvolveSchemaRequest {
                    table: format!("{DEFAULT_SCHEMA}.cpu"),
                    expected_version: 1,
                    change: SchemaChange::AddColumn {
                        field: Field::new("host", DataType::Utf8, true),
                    },
                },
                1_002,
            )
            .unwrap();
            st.commit_files(commit_req("public.cpu", "b1", &["k1"]), 1_003)
                .unwrap();
            st.commit_files(commit_req("public.mem", "b2", &["k2"]), 1_004)
                .unwrap();
            st.drop_shard("public.cpu", "s0");
            st.commit_compaction(&["b2".to_string()], vec![merged_file("public.mem", "m1", 1)]);
            st.sweep_expired_idempotency(24 * 3600, 1_010);
            st
        };
        let a = run();
        let b = run();
        assert_eq!(
            a.encode_canonical(),
            b.encode_canonical(),
            "同一串 op 必须得到逐字节相同的状态（否则 raft 副本会静默分叉）"
        );
        // 编码本身也要可复现（同一状态编两次必须一样）
        assert_eq!(a.encode_canonical(), a.encode_canonical());
    }

    /// 纪律 1：**时间由调用方传入**，状态机内不得读钟。
    /// 断言"记录下来的就是传入的值" —— 只要有人把 `now_secs()` 塞回状态机，这条立刻红。
    #[test]
    fn state_records_carried_timestamps_not_local_clock() {
        let mut st = CatalogState::new();
        let meta = st.create_table(create_table_req("cpu"), 42).unwrap();
        assert_eq!(meta.created_at, 42, "TableMeta.created_at 必须是传入值");
        let sv = st
            .schemas
            .get(&(format!("{DEFAULT_SCHEMA}.cpu"), 1))
            .unwrap();
        assert_eq!(sv.created_at, 42, "版本链的 created_at 必须是传入值");

        st.commit_files(commit_req("public.cpu", "b1", &["k"]), 99)
            .unwrap();
        assert_eq!(
            st.idempotency.get("k").unwrap().committed_at,
            99,
            "幂等记录的 committed_at 必须是传入值（否则 TTL 清理各副本不一致）"
        );
    }

    /// 纪律 2 的**尖锐用例**：`commit_compaction` 按受影响表的迭代序分配 `manifest_ver`。
    /// 两个状态机建表顺序不同（最终内容相同）→ 同一 compaction op 必须得到**同一份**
    /// `table_manifest_ver`。用 `HashSet` 时这条会随哈希种子随机失败（抽出前正是 `HashSet`）。
    #[test]
    fn compaction_version_assignment_is_order_independent() {
        let build = |order: [&str; 2]| {
            let mut st = CatalogState::new();
            // 注意：时间戳要**跟表走**而不是跟建表顺序走 —— 否则两个状态在语义上就不同了
            // （第一版就是这么写的，于是对拍"正确地"失败了：它测的其实是 created_at 不同）
            for t in order {
                let at = if t == "aaa" { 1_000 } else { 1_001 };
                st.create_table(create_table_req(t), at).unwrap();
            }
            st.commit_files(commit_req("public.aaa", "b_a", &[]), 2_000)
                .unwrap();
            st.commit_files(commit_req("public.bbb", "b_b", &[]), 2_001)
                .unwrap();
            st.commit_compaction(
                &["b_b".to_string(), "b_a".to_string()],
                vec![merged_file("public.aaa", "m", 2)],
            );
            st
        };
        let a = build(["aaa", "bbb"]);
        let b = build(["bbb", "aaa"]);
        assert_eq!(
            a.encode_canonical(),
            b.encode_canonical(),
            "建表顺序不同不影响最终状态（版本号分配必须有序）"
        );
        assert_eq!(
            a.manifest_delta(0).changed_tables,
            vec!["public.aaa".to_string(), "public.bbb".to_string()],
            "delta 的表名必须排序返回"
        );
    }

    /// S3-5：提交层按**键集合**去重（一个 chunk 可聚合多个键，§27.5 遗留 #1）。
    #[test]
    fn commit_files_dedups_on_key_set() {
        let mut st = CatalogState::new();
        st.create_table(create_table_req("cpu"), 1).unwrap();
        // 一次提交带两个键 → 两个键都被登记
        let r = st
            .commit_files(commit_req("public.cpu", "b1", &["k1", "k2"]), 10)
            .unwrap();
        assert!(r.accepted);
        assert!(st.check_idempotency("k1").is_some());
        assert!(st.check_idempotency("k2").is_some());

        // 重试：只带集合中的**任一**键 → 整次提交判为重复，且不落新文件
        let before = st.files.len();
        let r2 = st
            .commit_files(commit_req("public.cpu", "b_other", &["k2"]), 11)
            .unwrap();
        assert!(!r2.accepted, "键集合中任一键命中即整次判重");
        assert_eq!(st.files.len(), before, "判重不得落 manifest");
    }

    /// **"已被认领" ≠ "重复提交"**（S3-5 接线时被 chaos 用例当场抓到的语义坑）。
    ///
    /// ingest 入口在 WAL fsync 后会**认领**键（`batch_id` 留空，§27：防并发同键双写）；
    /// 重启后从 WAL 重建的索引也是空 `batch_id`。若把"已认领"当"重复"，
    /// **manifest 永不落盘**（写入返回成功但数据永远不可见），且恢复路径 100% 失败。
    #[test]
    fn claimed_key_is_completed_at_commit_not_rejected() {
        let mut st = CatalogState::new();
        st.create_table(create_table_req("cpu"), 1).unwrap();
        // 认领（模拟 ingest 入口在 fsync 后的登记：batch_id 为空）
        st.record_idempotency(IdempotencyRecord {
            client_request_id: "k".into(),
            batch_id: String::new(),
            committed_at: 5,
        });
        // 提交：必须**成功**，并把记录补全为真实 batch_id
        let r = st
            .commit_files(commit_req("public.cpu", "b1", &["k"]), 10)
            .unwrap();
        assert!(r.accepted, "已被认领 ≠ 重复：必须补全并落 manifest");
        assert_eq!(st.check_idempotency("k").unwrap(), "b1");
        assert_eq!(st.files.len(), 1, "文件必须真的落下来");

        // 另一个批次再用同一个键 → 这次**才是**重复
        let r2 = st
            .commit_files(commit_req("public.cpu", "b2", &["k"]), 11)
            .unwrap();
        assert!(!r2.accepted, "被已提交批次占用的键才算重复");
        assert_eq!(st.files.len(), 1, "判重不得落第二个文件");
    }

    /// 幂等键 TTL 清理的时间也由调用方传入（R3 下必须是显式 op，否则各副本清理时刻不同）。
    #[test]
    fn idempotency_sweep_uses_carried_now() {
        let mut st = CatalogState::new();
        st.record_idempotency(IdempotencyRecord {
            client_request_id: "old".into(),
            batch_id: "b".into(),
            committed_at: 100,
        });
        // now = 100 + 23h → 未过期
        assert_eq!(st.sweep_expired_idempotency(24 * 3600, 100 + 23 * 3600), 0);
        // now = 100 + 25h → 过期
        assert_eq!(st.sweep_expired_idempotency(24 * 3600, 100 + 25 * 3600), 1);
    }
}


#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use yuntun_model::error::SnapshotError;
    use yuntun_model::meta::IngestConfig;
    use yuntun_model::schema::SchemaChange;
    use yuntun_model::snapshot::{encode_payload, frame};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]))
    }

    fn create_table_req(name: &str) -> CreateTableRequest {
        CreateTableRequest {
            name: name.into(),
            namespace: DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: IngestConfig::standard(),
        }
    }

    fn commit_req(table: &str, batch_id: &str, keys: &[&str]) -> CommitFilesRequest {
        CommitFilesRequest {
            table: table.into(),
            batch_id: batch_id.into(),
            client_request_id: keys.first().map(|s| s.to_string()),
            client_request_ids: keys.iter().map(|s| s.to_string()).collect(),
            shard: "s0".into(),
            time_window: "w1".into(),
            files: vec![FileManifest {
                file_path: format!("p/{batch_id}.parquet"),
                batch_id: batch_id.into(),
                table: table.into(),
                shard: "s0".into(),
                time_window: "w1".into(),
                row_count: 7,
                ..Default::default()
            }],
            schema_version: 1,
            row_count: 7,
        }
    }

    /// 一个"每个维度都非空"的状态：快照测试必须有东西可丢，否则测不出漏字段。
    fn rich_state() -> CatalogState {
        let mut st = CatalogState::new();
        st.create_schema("analytics").unwrap();
        st.create_table(create_table_req("cpu"), 1_000).unwrap();
        st.evolve_schema(
            EvolveSchemaRequest {
                table: format!("{DEFAULT_SCHEMA}.cpu"),
                expected_version: 1,
                change: SchemaChange::AddColumn {
                    field: Field::new("host", DataType::Utf8, true),
                },
            },
            1_001,
        )
        .unwrap();
        st.commit_files(commit_req("public.cpu", "b1", &["k1"]), 1_002)
            .unwrap();
        st.record_idempotency(IdempotencyRecord {
            client_request_id: "k9".into(),
            batch_id: "b1".into(),
            committed_at: 1_002,
        });
        st
    }

    /// **无损往返**：产物 → 恢复 → 规范编码与再编码都必须**逐字节相同**。
    ///
    /// 再编码相同这一条很关键：它同时证明"快照编码是确定性的"
    /// （否则同一个状态会产出不同字节，副本间无法比对、去重、缓存）。
    #[test]
    fn snapshot_roundtrip_is_lossless_and_deterministic() {
        let a = rich_state();
        let bytes = a.snapshot_artifact();
        let b = CatalogState::restore_snapshot(&bytes).expect("恢复应成功");

        assert_eq!(
            a.encode_canonical(),
            b.encode_canonical(),
            "恢复出的状态与源状态不等价（规范编码不同）"
        );
        assert_eq!(
            a.snapshot_artifact(),
            bytes,
            "恢复后重新产出快照的字节变了 → 快照编码不确定"
        );
        assert_eq!(b.current_snapshot(), a.current_snapshot(), "快照号丢失");
        assert_eq!(b.read_index(), a.read_index(), "applied index 丢失");
        assert_eq!(b.version(), a.version(), "schema/manifest 版本丢失");
    }

    /// **往返之后行为一致**：恢复出的状态**继续 apply 同一个 op**，必须与源状态收敛到同一结果。
    ///
    /// 比"编码相同"更强：它证明快照没有丢掉任何**影响未来应用结果**的东西
    /// （例如漏掉 `idempotency` 会让同一幂等键在恢复后**重复生效**，
    /// 而静态编码比对在某些字段上未必看得出来）。
    #[test]
    fn snapshot_roundtrip_preserves_future_behavior() {
        let mut a = rich_state();
        let bytes = a.snapshot_artifact();
        let mut b = CatalogState::restore_snapshot(&bytes).expect("恢复应成功");

        // 幂等键 k1 已用过：恢复后必须仍然"命中"，而不是再次提交
        let ra = a.commit_files(commit_req("public.cpu", "b2", &["k1"]), 2_000);
        let rb = b.commit_files(commit_req("public.cpu", "b2", &["k1"]), 2_000);
        assert_eq!(
            format!("{ra:?}").contains("duplicate"),
            format!("{rb:?}").contains("duplicate"),
            "幂等判定在恢复前后不一致：源={ra:?} 恢复={rb:?}"
        );
        // 再提交一个新键，两边必须都接受且状态一致
        a.commit_files(commit_req("public.cpu", "b3", &["k3"]), 2_001)
            .unwrap();
        b.commit_files(commit_req("public.cpu", "b3", &["k3"]), 2_001)
            .unwrap();
        assert_eq!(
            a.encode_canonical(),
            b.encode_canonical(),
            "恢复后的状态机与源状态机对同一后续 op 反应不同"
        );
    }

    /// **维度覆盖**：状态机的**每个**维度都必须体现在快照里。
    ///
    /// 这是专防"加字段忘了加进快照"的回归测试 —— 那种漏法不会让任何测试变红，
    /// 只会让换主/重启后状态**静默回退**。所以每个维度都断言"改了它，产物必须变"。
    #[test]
    fn snapshot_covers_every_state_dimension() {
        let base = rich_state();
        let base_bytes = base.snapshot_artifact();
        let mut cases: Vec<(&str, CatalogState)> = Vec::new();

        let mut s = rich_state();
        s.create_schema("another").unwrap();
        cases.push(("namespaces", s));

        let mut s = rich_state();
        s.create_table(create_table_req("mem"), 3_000).unwrap();
        cases.push(("tables", s));

        let mut s = rich_state();
        s.evolve_schema(
            EvolveSchemaRequest {
                table: format!("{DEFAULT_SCHEMA}.cpu"),
                expected_version: 2,
                change: SchemaChange::AddColumn {
                    field: Field::new("region", DataType::Utf8, true),
                },
            },
            3_001,
        )
        .unwrap();
        cases.push(("schemas", s));

        let mut s = rich_state();
        s.commit_files(commit_req("public.cpu", "b4", &["k4"]), 3_002)
            .unwrap();
        cases.push(("files", s));

        let mut s = rich_state();
        s.record_idempotency(IdempotencyRecord {
            client_request_id: "kz".into(),
            batch_id: "b1".into(),
            committed_at: 3_003,
        });
        cases.push(("idempotency", s));

        // 计数器是**白盒**改的：它们是状态的一部分，但没有任何公开 op 只改计数器
        let mut s = rich_state();
        s.snapshot_version += 1;
        cases.push(("snapshot_version(revision)", s));
        let mut s = rich_state();
        s.last_applied += 1;
        cases.push(("last_applied", s));
        let mut s = rich_state();
        s.schema_ver += 1;
        cases.push(("schema_ver", s));
        let mut s = rich_state();
        s.manifest_ver += 1;
        cases.push(("manifest_ver", s));
        let mut s = rich_state();
        s.table_manifest_ver
            .insert("public.cpu".into(), 9_999);
        cases.push(("table_manifest_ver", s));

        for (name, st) in cases {
            assert_ne!(
                st.snapshot_artifact(),
                base_bytes,
                "维度 `{name}` 变了但快照字节没变 → 快照漏了这个字段（换主/重启后会静默回退）"
            );
        }
    }

    /// 重复键必须**拒绝**：重复键下"谁生效"取决于遍历顺序 = 不确定性。
    #[test]
    fn snapshot_rejects_duplicate_keys() {
        let st = rich_state();
        let mut msg = st.to_snapshot_msg();
        let dup = msg.tables[0].clone();
        msg.tables.push(dup);
        let err = CatalogState::from_snapshot_msg(msg).unwrap_err();
        assert!(
            matches!(err, SnapshotError::InvalidState(ref m) if m.contains("重复")),
            "重复表键未被拒绝：{err:?}"
        );
    }

    /// 缺 `public` 的状态非法（状态机不变量）。
    #[test]
    fn snapshot_rejects_missing_public_namespace() {
        let st = rich_state();
        let mut msg = st.to_snapshot_msg();
        msg.namespaces.retain(|n| n != DEFAULT_SCHEMA);
        let err = CatalogState::from_snapshot_msg(msg).unwrap_err();
        assert!(
            matches!(err, SnapshotError::InvalidState(ref m) if m.contains("namespaces")),
            "缺 public 未被拒绝：{err:?}"
        );
    }

    /// 帧头 revision 与载荷 revision 不一致 = 帧与载荷不是同一次快照 → 必须拒绝。
    #[test]
    fn snapshot_rejects_frame_payload_revision_mismatch() {
        let st = rich_state();
        let msg = st.to_snapshot_msg();
        let payload = encode_payload(&msg);
        let bad = frame(&payload, msg.revision + 7, 0);
        let err = CatalogState::restore_snapshot(&bad).unwrap_err();
        assert!(
            matches!(err, SnapshotError::RevisionMismatch { .. }),
            "帧/载荷 revision 不一致未被拒绝：{err:?}"
        );
    }

    /// 截断的产物必须**装不进去**（快照被截半 = 半状态，绝不能"尽力恢复"）。
    #[test]
    fn snapshot_rejects_truncated_artifact() {
        let st = rich_state();
        let bytes = st.snapshot_artifact();
        for cut in [0, 1, 20, 35, 36, 40, bytes.len() - 1] {
            assert!(
                CatalogState::restore_snapshot(&bytes[..cut]).is_err(),
                "截断到 {cut} 字节竟被接受（产物共 {} 字节）",
                bytes.len()
            );
        }
    }
}
