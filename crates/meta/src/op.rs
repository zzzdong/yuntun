//! op 的**应用路径**与**边界转换器**（S3-3）。
//!
//! # 这一层解决什么
//!
//! 入站是 gRPC 的 `meta::Op`（proto，自描述 ✓），落到状态机需要 `yuntun-model` 的手写结构。
//! 中间这段"翻译"必须只有一份，并且**漏字段就报错**（不能静默丢），否则线上才会发现
//! —— 这正是 `crates/proto/tests/wire_compat.rs` 守的那条边界；这里放**生产实现**。
//!
//! # 为什么 `now_ms` 在 op 上而不是函数参数
//!
//! 状态机 apply 时**不读钟**（`operation-log §38` 抓到的四类非确定性之一）。时间由发起方
//! 打点、随 op 传播，各副本 apply 同一串 op 得到同一状态。所以 `now_ms` 是 op 的字段，
//! 不是 `apply()` 的参数 —— 后者会诱导实现者在里面取"现在"。

use std::sync::Arc;

use yuntun_catalog::CatalogState;
use arrow::datatypes::{DataType, Field, Schema};
use yuntun_model::meta::{
    ColumnStatLite, FileManifest, IdempotencyRecord, IngestConfig, StatisticsLite, TableMeta,
};
use yuntun_model::ops::EvolveSchemaRequest;
use yuntun_model::schema::SchemaChange;
use yuntun_model::ops::{CommitFilesRequest, CreateTableRequest};

// `encode_to_vec` / `decode`：prost 的 trait 方法（匿名导入，避免与 pb 别名混淆）
use prost::Message as _;

use crate::MetaError;
use yuntun_proto::meta as pb;

/// 解码后的 op（**已经是状态机能直接吃的形状**）。
///
/// `now_ms` 与 op 绑在一起，防止"apply 时读钟"。
#[derive(Debug, Clone)]
pub enum StateOp {
    CreateSchema {
        name: String,
        now_ms: u64,
    },
    CreateTable {
        request: CreateTableRequest,
        now_ms: u64,
    },
    DropTable {
        name: String,
        now_ms: u64,
    },
    CommitFiles {
        request: CommitFilesRequest,
        now_ms: u64,
    },
    DropSchema {
        name: String,
        now_ms: u64,
    },
    EvolveSchema {
        request: EvolveSchemaRequest,
        now_ms: u64,
    },
    DropShard {
        table: String,
        shard: String,
        now_ms: u64,
    },
    Compaction {
        old_batch_ids: Vec<String>,
        new_files: Vec<FileManifest>,
        now_ms: u64,
    },
    /// 幂等**认领**（写入前的登记）。
    RecordIdempotency {
        record: IdempotencyRecord,
        now_ms: u64,
    },
}

// `CreateTableRequest` 里有 `SchemaRef`（`Arc<Schema>`）—— `Arc` 未使用会告警
#[allow(unused_imports)]
use Arc as _ArcAlias;

impl StateOp {
    pub fn now_ms(&self) -> u64 {
        match self {
            // ⚠️ 每个 op **都必须**自带 `now_ms`（纪律 1：状态机不读钟）。
            //    这条 match 是穷尽的 —— 加新 op 时编译器会强制你把它带进来，
            //    漏带的话"apply 读墙钟"就会在各副本间静默分叉。
            StateOp::CreateSchema { now_ms, .. }
            | StateOp::CreateTable { now_ms, .. }
            | StateOp::DropTable { now_ms, .. }
            | StateOp::CommitFiles { now_ms, .. }
            | StateOp::DropSchema { now_ms, .. }
            | StateOp::EvolveSchema { now_ms, .. }
            | StateOp::DropShard { now_ms, .. }
            | StateOp::Compaction { now_ms, .. }
            | StateOp::RecordIdempotency { now_ms, .. } => *now_ms,
        }
    }
}

/// 应用结果。
#[derive(Debug, Clone, PartialEq)]
pub struct ApplyOutcome {
    /// `false` = 幂等命中（不重复应用；**不是**错误）
    pub accepted: bool,
}

// ---------------------------------------------------------------- proto → 进程内

/// `meta::Op` → [`StateOp`]。
///
/// 未知分支（proto 里将来加了、本 build 还不认识）→ `BadRequest`：**不能猜**，
/// 也不能当空操作（那会让客户端以为写成功了）。
pub fn decode_op(op: &pb::Op) -> Result<StateOp, MetaError> {
    let now_ms = op.now_ms;
    let kind = op
        .kind
        .as_ref()
        .ok_or_else(|| MetaError::BadRequest("op.kind 为空".into()))?;
    Ok(match kind {
        pb::op::Kind::CreateSchema(c) => StateOp::CreateSchema {
            name: c.name.clone(),
            now_ms,
        },
        pb::op::Kind::CreateTable(c) => StateOp::CreateTable {
            request: create_table_from_proto(c)?,
            now_ms,
        },
        pb::op::Kind::DropTable(d) => StateOp::DropTable {
            name: d.name.clone(),
            now_ms,
        },
        pb::op::Kind::CommitFiles(c) => {
            let msg = c
                .request
                .as_ref()
                .ok_or_else(|| MetaError::BadRequest("commit_files.request 为空".into()))?;
            StateOp::CommitFiles {
                request: commit_request_from_proto(msg)?,
                now_ms,
            }
        }
        pb::op::Kind::DropSchema(d) => StateOp::DropSchema {
            name: d.name.clone(),
            now_ms,
        },
        pb::op::Kind::EvolveSchema(e) => StateOp::EvolveSchema {
            request: evolve_schema_from_proto(e)?,
            now_ms,
        },
        pb::op::Kind::DropShard(d) => StateOp::DropShard {
            table: d.table.clone(),
            shard: d.shard.clone(),
            now_ms,
        },
        pb::op::Kind::Compaction(c) => StateOp::Compaction {
            old_batch_ids: c.old_batch_ids.clone(),
            new_files: c.new_files.iter().map(manifest_from_proto).collect(),
            now_ms,
        },
        pb::op::Kind::Idempotency(i) => {
            let msg = i
                .record
                .as_ref()
                .ok_or_else(|| MetaError::BadRequest("idempotency.record 为空".into()))?;
            StateOp::RecordIdempotency {
                record: idempotency_record_from_proto(msg),
                now_ms,
            }
        }
    })
}

/// `CreateTableOp` → `CreateTableRequest`（**逐字段**）。
///
/// `ingest_config` 以 prost 字节携带（`IngestConfig` 本身就是 prost 消息），
/// 缺省时用标准配置 —— **不静默用默认值**是这条的关键：解码失败要报错。
pub fn create_table_from_proto(c: &pb::CreateTableOp) -> Result<CreateTableRequest, MetaError> {
    let ingest_config = if c.ingest_config.is_empty() {
        IngestConfig::standard()
    } else {
        use prost::Message as _;
        IngestConfig::decode(c.ingest_config.as_slice()).map_err(|e| {
            MetaError::BadRequest(format!("ingest_config 解码失败：{e}"))
        })?
    };
    let schema = yuntun_model::meta::deserialize_schema(&c.arrow_schema_ipc)
        .map_err(|e| MetaError::BadRequest(format!("arrow schema 解码失败：{e}")))?;
    Ok(CreateTableRequest {
        name: c.name.clone(),
        namespace: c.namespace.clone(),
        schema,
        partition_cols: c.partition_cols.clone(),
        default_format: c.default_format.clone(),
        ingest_config,
    })
}

/// 反向：`CreateTableRequest` → proto（给测试与重启重提交用）。
pub fn create_table_to_proto(r: &CreateTableRequest) -> pb::CreateTableOp {
    use prost::Message as _;
    pb::CreateTableOp {
        name: r.name.clone(),
        namespace: r.namespace.clone(),
        arrow_schema_ipc: yuntun_model::meta::serialize_schema(&r.schema),
        default_format: r.default_format.clone(),
        partition_cols: r.partition_cols.clone(),
        ingest_config: r.ingest_config.encode_to_vec(),
    }
}

/// `CommitFilesRequestMsg` → `CommitFilesRequest`（**逐字段**，漏字段在编译期就会被发现）。
pub fn commit_request_from_proto(m: &pb::CommitFilesRequestMsg) -> Result<CommitFilesRequest, MetaError> {
    Ok(CommitFilesRequest {
        table: m.table.clone(),
        batch_id: m.batch_id.clone(),
        client_request_id: m.client_request_id.clone(),
        client_request_ids: m.client_request_ids.clone(),
        shard: m.shard.clone(),
        time_window: m.time_window.clone(),
        files: m.files.iter().map(manifest_from_proto).collect(),
        schema_version: m.schema_version,
        row_count: m.row_count,
    })
}

/// 公开包装：`RemoteCatalog` 把载荷还原成模型对象时要用
/// （逐字段镜像的**唯一实现**留在本模块，别处不再写第二份）。
pub fn manifest_from_proto_pub(m: &pb::FileManifestMsg) -> FileManifest {
    manifest_from_proto(m)
}

fn manifest_from_proto(m: &pb::FileManifestMsg) -> FileManifest {
    FileManifest {
        file_path: m.file_path.clone(),
        batch_id: m.batch_id.clone(),
        client_request_id: m.client_request_id.clone(),
        schema_version: m.schema_version,
        status: m.status,
        valid_from: m.valid_from,
        deleted_at: m.deleted_at,
        stats: m.stats.as_ref().map(|s| StatisticsLite {
            columns: s
                .columns
                .iter()
                .map(|c| ColumnStatLite {
                    name: c.name.clone(),
                    min: c.min.clone(),
                    max: c.max.clone(),
                    null_count: c.null_count,
                })
                .collect(),
        }),
        row_count: m.row_count,
        file_size: m.file_size,
        table: m.table.clone(),
        shard: m.shard.clone(),
        time_window: m.time_window.clone(),
        partition_key: m.partition_key.clone(),
        source_instance: m.source_instance.clone(),
        sealed_at_ms: m.sealed_at_ms,
        committed_at_ms: m.committed_at_ms,
        seal_reason: m.seal_reason.clone(),
        seal_pressure: m.seal_pressure.clone(),
    }
}

/// 反向（`CommitFilesRequest` → proto）：给**重启重提交**与测试用。
///
/// 两个方向都留着不是冗余：只写一个方向，另一个方向的字段很容易悄悄漂移
/// （`wire_compat` 的教训就是"漏字段只有测到才知道"）。
pub fn commit_request_to_proto(r: &CommitFilesRequest) -> pb::CommitFilesRequestMsg {
    pb::CommitFilesRequestMsg {
        table: r.table.clone(),
        batch_id: r.batch_id.clone(),
        client_request_id: r.client_request_id.clone(),
        client_request_ids: r.client_request_ids.clone(),
        shard: r.shard.clone(),
        time_window: r.time_window.clone(),
        files: r.files.iter().map(manifest_to_proto).collect(),
        schema_version: r.schema_version,
        row_count: r.row_count,
    }
}

/// 公开包装：`Prefetch` 载荷要用（把逐字段镜像的**唯一实现**留在本模块，
/// 避免"载荷那边再写一份"导致两份定义漂移）。
pub fn manifest_to_proto_pub(f: &FileManifest) -> pb::FileManifestMsg {
    manifest_to_proto(f)
}

fn manifest_to_proto(f: &FileManifest) -> pb::FileManifestMsg {
    pb::FileManifestMsg {
        file_path: f.file_path.clone(),
        batch_id: f.batch_id.clone(),
        client_request_id: f.client_request_id.clone(),
        schema_version: f.schema_version,
        status: f.status,
        valid_from: f.valid_from,
        deleted_at: f.deleted_at,
        stats: f.stats.as_ref().map(|s| pb::StatisticsLiteMsg {
            columns: s
                .columns
                .iter()
                .map(|c| pb::ColumnStatLiteMsg {
                    name: c.name.clone(),
                    min: c.min.clone(),
                    max: c.max.clone(),
                    null_count: c.null_count,
                })
                .collect(),
        }),
        row_count: f.row_count,
        file_size: f.file_size,
        table: f.table.clone(),
        shard: f.shard.clone(),
        time_window: f.time_window.clone(),
        partition_key: f.partition_key.clone(),
        source_instance: f.source_instance.clone(),
        sealed_at_ms: f.sealed_at_ms,
        committed_at_ms: f.committed_at_ms,
        seal_reason: f.seal_reason.clone(),
        seal_pressure: f.seal_pressure.clone(),
    }
}

// ---------------------------------------------------------------- 应用

/// 把 [`StateOp`] 应用到状态机（**纯函数**：无锁、无钟、无 IO）。
///
/// 幂等语义与 standalone 路径一致：重复 DDL 视为已生效、重复 `commit_files` 返回
/// `accepted = false`（幂等命中）—— **不是错误**（客户端重试必须成功）。
/// `TableMeta`（状态机）→ proto（`Prefetch` 载荷）。
///
/// 逐字段搬，**不许省**：少一个字段意味着客户端的本地缓存里少一样东西，
/// 而**不会有任何报错** —— 表现是"某个功能悄悄不生效"（本地难查、线上更难查）。
pub fn table_meta_to_proto(m: &TableMeta) -> pb::TableMeta {
    pb::TableMeta {
        // ⚠️ 线上以**全限定名**为权威：模型里 `name` 是裸名、schema 在另一个字段，
        // 而载荷的消费者拿这个名字当缓存键 —— 裸名会让 `public.cpu` 与 `analytics.cpu`
        // 撞在一起（**静默**串表）。这条是实测抓出来的：第一版直接搬 `m.name`，
        // 用例立刻在"请求 public.cpu 拿回名为 cpu 的条目"上变红。
        name: m.qualified_name(),
        current_schema_version: m.current_schema_version,
        partition_cols: m.partition_cols.clone(),
        default_format: m.default_format.clone(),
        arrow_schema: m.arrow_schema.clone(),
        // `Some(x)` → 编码字节；`None` → **不设字段**（`optional`，保住"没配"与"空配置"的区别）
        ingest_config: m.ingest_config.as_ref().map(|c| c.encode_to_vec()),
        created_at: m.created_at,
        table_template: m.table_template,
        // 冗余副本（模型里有这个字段），填**归一化后**的值，好让解码侧能交叉校验
        namespace: m.schema_name().to_string(),
    }
}

/// proto → `TableMeta`（`table_meta_to_proto` 的逆）。
pub fn table_meta_from_proto(m: &pb::TableMeta) -> Result<TableMeta, MetaError> {
    let (ns, name) = yuntun_model::ops::split_qualified(&m.name);
    // 全限定名与 `namespace` 写的是**同一个事实**。不一致时必须报错：
    // 静默取一个会让"名字"和"namespace"指向不同的表，而这种错**不报错**地传播。
    if !m.namespace.is_empty() && m.namespace != ns {
        return Err(MetaError::BadRequest(format!(
            "TableMeta 的 name({}) 与 namespace({}) 不一致（两者必须是同一张表）",
            m.name, m.namespace
        )));
    }
    let ingest_config = match &m.ingest_config {
        Some(bytes) => Some(IngestConfig::decode(bytes.as_slice()).map_err(|e| {
            MetaError::BadRequest(format!("TableMeta.ingest_config 解不开：{e}"))
        })?),
        None => None,
    };
    Ok(TableMeta {
        name: name.to_string(),
        current_schema_version: m.current_schema_version,
        partition_cols: m.partition_cols.clone(),
        default_format: m.default_format.clone(),
        arrow_schema: m.arrow_schema.clone(),
        ingest_config,
        created_at: m.created_at,
        table_template: m.table_template,
        namespace: ns.to_string(),
    })
}

// ---------------------------------------------------------------- 新 op 的逐字段转换

/// `arrow::Field` → Arrow IPC（**单字段 schema**）。
///
/// 为什么不发明"字段编码"：`CreateTableOp.arrow_schema_ipc` 已经是 Arrow IPC，
/// 再引入一套只会得到"两份必然漂移的定义"。代价是编码里带了 schema 名这类无意义信息，
/// 解码侧忽略它。
fn field_to_ipc(f: &Field) -> Vec<u8> {
    yuntun_model::meta::serialize_schema(&Schema::new(vec![f.clone()]).into())
}

/// IPC → `arrow::Field`。**必须恰好 1 个字段**：0 或 2 个都是协议层垃圾，
/// 放过去会让"增列"变成一个说不清的动作。
fn field_from_ipc(bytes: &[u8]) -> Result<Field, MetaError> {
    let s = yuntun_model::meta::deserialize_schema(bytes)
        .map_err(|e| MetaError::BadRequest(format!("add_column 的字段 IPC 解不开：{e}")))?;
    if s.fields().len() != 1 {
        return Err(MetaError::BadRequest(format!(
            "add_column 的字段 IPC 必须含恰好 1 个字段，实际 {}",
            s.fields().len()
        )));
    }
    Ok(s.field(0).clone())
}

/// `arrow::DataType` → IPC（单字段 schema 携带）。
fn type_to_ipc(t: &DataType) -> Vec<u8> {
    field_to_ipc(&Field::new("_", t.clone(), true))
}

fn type_from_ipc(bytes: &[u8]) -> Result<DataType, MetaError> {
    Ok(field_from_ipc(bytes)?.data_type().clone())
}

pub fn schema_change_to_proto(c: &SchemaChange) -> pb::SchemaChangeMsg {
    use pb::schema_change_msg::Kind;
    pb::SchemaChangeMsg {
        kind: Some(match c {
            SchemaChange::AddColumn { field } => Kind::AddColumnFieldIpc(field_to_ipc(field)),
            SchemaChange::WidenType { column, to } => Kind::WidenType(pb::WidenTypeMsg {
                column: column.clone(),
                to_type_ipc: type_to_ipc(to),
            }),
            SchemaChange::DropColumn { column } => {
                Kind::DropColumn(pb::DropColumnMsg { column: column.clone() })
            }
        }),
    }
}

pub fn schema_change_from_proto(m: &pb::SchemaChangeMsg) -> Result<SchemaChange, MetaError> {
    use pb::schema_change_msg::Kind;
    let kind = m
        .kind
        .as_ref()
        .ok_or_else(|| MetaError::BadRequest("SchemaChangeMsg.kind 为空".into()))?;
    Ok(match kind {
        Kind::AddColumnFieldIpc(b) => SchemaChange::AddColumn {
            field: field_from_ipc(b)?,
        },
        Kind::WidenType(w) => SchemaChange::WidenType {
            column: w.column.clone(),
            to: type_from_ipc(&w.to_type_ipc)?,
        },
        Kind::DropColumn(d) => SchemaChange::DropColumn {
            column: d.column.clone(),
        },
    })
}

pub fn evolve_schema_to_proto(r: &EvolveSchemaRequest) -> pb::EvolveSchemaOp {
    pb::EvolveSchemaOp {
        table: r.table.clone(),
        change: Some(schema_change_to_proto(&r.change)),
        expected_version: r.expected_version,
    }
}

pub fn evolve_schema_from_proto(p: &pb::EvolveSchemaOp) -> Result<EvolveSchemaRequest, MetaError> {
    let change = p
        .change
        .as_ref()
        .ok_or_else(|| MetaError::BadRequest("evolve_schema.change 为空".into()))?;
    Ok(EvolveSchemaRequest {
        table: p.table.clone(),
        change: schema_change_from_proto(change)?,
        expected_version: p.expected_version,
    })
}

pub fn idempotency_record_to_proto(r: &IdempotencyRecord) -> pb::IdempotencyRecordMsg {
    pb::IdempotencyRecordMsg {
        client_request_id: r.client_request_id.clone(),
        batch_id: r.batch_id.clone(),
        committed_at: r.committed_at,
    }
}

pub fn idempotency_record_from_proto(m: &pb::IdempotencyRecordMsg) -> IdempotencyRecord {
    IdempotencyRecord {
        client_request_id: m.client_request_id.clone(),
        batch_id: m.batch_id.clone(),
        committed_at: m.committed_at,
    }
}

pub fn apply(state: &mut CatalogState, op: &StateOp) -> Result<ApplyOutcome, MetaError> {
    let now = op.now_ms();
    match op {
        StateOp::CreateSchema { name, .. } => {
            if state.schema_exists(name) {
                return Ok(ApplyOutcome { accepted: false });
            }
            state
                .create_schema(name)
                .map_err(MetaError::from_lake)?;
            Ok(ApplyOutcome { accepted: true })
        }
        StateOp::CreateTable { request, .. } => {
            // ⚠️ 幂等判断必须用**归一化后的表身份**（`schema.table`）：
            // 状态机内部存的是全限定名，而请求里的 `name` 可能是裸名 ——
            // 拿裸名去查会永远查不到，于是重复建表变成 `TableAlreadyExists` 错误
            // （而幂等语义要求它是 `accepted=false`）。
            let qualified = if request.name.contains('.') {
                request.name.clone()
            } else {
                format!("{}.{}", request.namespace, request.name)
            };
            if state.get_table(&qualified).is_some() {
                return Ok(ApplyOutcome { accepted: false });
            }
            state
                .create_table(request.clone(), now)
                .map_err(MetaError::from_lake)?;
            Ok(ApplyOutcome { accepted: true })
        }
        StateOp::DropTable { name, .. } => {
            if state.get_table(name).is_none() {
                return Ok(ApplyOutcome { accepted: false });
            }
            state.drop_table(name).map_err(MetaError::from_lake)?;
            Ok(ApplyOutcome { accepted: true })
        }
        StateOp::DropSchema { name, .. } => {
            // 幂等：schema 不存在 = 目标状态已达成（`accepted=false` 而不是错误）
            if !state.schema_exists(name) {
                return Ok(ApplyOutcome { accepted: false });
            }
            state.drop_schema(name).map_err(MetaError::from_lake)?;
            Ok(ApplyOutcome { accepted: true })
        }
        StateOp::EvolveSchema { request, .. } => {
            // OCC：`expected_version` 不符 → 报错（**不能**静默当成成功 —— 那是 DDL 丢失）
            state
                .evolve_schema(request.clone(), now / 1000)
                .map_err(MetaError::from_lake)?;
            Ok(ApplyOutcome { accepted: true })
        }
        StateOp::DropShard { table, shard, .. } => {
            // 幂等：没有任何文件被标记删除（`n == 0`）= 无事可做
            let n = state.drop_shard(table, shard);
            Ok(ApplyOutcome { accepted: n > 0 })
        }
        StateOp::Compaction {
            old_batch_ids,
            new_files,
            ..
        } => {
            state.commit_compaction(old_batch_ids, new_files.clone());
            Ok(ApplyOutcome { accepted: true })
        }
        StateOp::RecordIdempotency { record, .. } => {
            // 幂等：键已存在 → 不覆盖（`or_insert` 语义），并如实回 `accepted=false`
            if state.check_idempotency(&record.client_request_id).is_some() {
                return Ok(ApplyOutcome { accepted: false });
            }
            state.record_idempotency(record.clone());
            Ok(ApplyOutcome { accepted: true })
        }
        StateOp::CommitFiles { request, .. } => {
            let resp = state
                .commit_files(request.clone(), now)
                .map_err(MetaError::from_lake)?;
            Ok(ApplyOutcome {
                accepted: resp.accepted,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use yuntun_model::meta::IngestConfig;
    use yuntun_model::ops::{CreateTableRequest, DEFAULT_SCHEMA};

    fn sm_with_table() -> CatalogState {
        let mut st = CatalogState::new();
        st.create_table(
            CreateTableRequest {
                name: "cpu".into(),
                namespace: DEFAULT_SCHEMA.into(),
                schema: Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)])),
                partition_cols: vec![],
                default_format: "parquet".into(),
                ingest_config: IngestConfig::standard(),
            },
            1_000,
        )
        .unwrap();
        st
    }

    fn rich_request() -> CommitFilesRequest {
        CommitFilesRequest {
            table: "public.cpu".into(),
            batch_id: "b-1".into(),
            client_request_id: Some("k-legacy".into()),
            client_request_ids: vec!["k1".into(), "k2".into()],
            shard: "s0".into(),
            time_window: "w1".into(),
            files: vec![FileManifest {
                file_path: "p/b-1.parquet".into(),
                batch_id: "b-1".into(),
                client_request_id: "k1".into(),
                schema_version: 1,
                status: 1,
                valid_from: 3,
                deleted_at: 4,
                stats: Some(StatisticsLite {
                    columns: vec![ColumnStatLite {
                        name: "ts".into(),
                        min: vec![1],
                        max: vec![2],
                        null_count: 5,
                    }],
                }),
                row_count: 7,
                file_size: 8,
                table: "public.cpu".into(),
                shard: "s0".into(),
                time_window: "w1".into(),
                partition_key: "p1".into(),
                source_instance: "inst-9".into(),
                sealed_at_ms: 11,
                committed_at_ms: 12,
                seal_reason: "time_threshold".into(),
                seal_pressure: "memory:0.1".into(),
            }],
            schema_version: 1,
            row_count: 7,
        }
    }

    /// 双向转换必须**无损**（任一方向漏字段都会在这里被逮到）。
    #[test]
    fn commit_request_conversion_is_lossless_both_ways() {
        let original = rich_request();
        let msg = commit_request_to_proto(&original);
        let back = commit_request_from_proto(&msg).unwrap();
        // 手写结构没实现 `PartialEq`，逐字段比（断言名即"哪个字段丢了"）
        assert_eq!(back.table, original.table);
        assert_eq!(back.batch_id, original.batch_id);
        assert_eq!(back.client_request_id, original.client_request_id);
        assert_eq!(back.client_request_ids, original.client_request_ids);
        assert_eq!(back.shard, original.shard);
        assert_eq!(back.time_window, original.time_window);
        assert_eq!(back.schema_version, original.schema_version);
        assert_eq!(back.row_count, original.row_count);
        let (a, b) = (&back.files[0], &original.files[0]);
        assert_eq!(a.file_path, b.file_path);
        assert_eq!(a.batch_id, b.batch_id);
        assert_eq!(a.client_request_id, b.client_request_id);
        assert_eq!(a.schema_version, b.schema_version);
        assert_eq!(a.status, b.status);
        assert_eq!(a.valid_from, b.valid_from);
        assert_eq!(a.deleted_at, b.deleted_at);
        assert_eq!(a.row_count, b.row_count);
        assert_eq!(a.file_size, b.file_size);
        assert_eq!(a.table, b.table);
        assert_eq!(a.shard, b.shard);
        assert_eq!(a.time_window, b.time_window);
        assert_eq!(a.partition_key, b.partition_key);
        assert_eq!(a.source_instance, b.source_instance);
        assert_eq!(a.sealed_at_ms, b.sealed_at_ms);
        assert_eq!(a.committed_at_ms, b.committed_at_ms);
        assert_eq!(a.seal_reason, b.seal_reason, "可观测性字段最容易漏");
        assert_eq!(a.seal_pressure, b.seal_pressure);
        let (sa, sb) = (a.stats.as_ref().unwrap(), b.stats.as_ref().unwrap());
        assert_eq!(sa.columns[0].name, sb.columns[0].name);
        assert_eq!(sa.columns[0].min, sb.columns[0].min);
        assert_eq!(sa.columns[0].max, sb.columns[0].max);
        assert_eq!(sa.columns[0].null_count, sb.columns[0].null_count);
    }

    /// 空 `kind` / 未知形状 → `BadRequest`（**不能**当空操作，那会让客户端以为写成功了）。
    #[test]
    fn malformed_op_is_rejected_not_ignored() {
        let op = pb::Op {
            now_ms: 1,
            kind: None,
        };
        assert!(matches!(decode_op(&op), Err(MetaError::BadRequest(_))));
        let op = pb::Op {
            now_ms: 1,
            kind: Some(pb::op::Kind::CommitFiles(pb::CommitFilesOp {
                request: None,
            })),
        };
        assert!(matches!(decode_op(&op), Err(MetaError::BadRequest(_))));
    }

    /// 幂等：重复 DDL 与重复提交都**不是错误**（客户端重试必须成功）。
    #[test]
    fn repeated_ops_are_idempotent_not_errors() {
        let mut st = sm_with_table();
        let schema = StateOp::CreateSchema {
            name: "analytics".into(),
            now_ms: 10,
        };
        assert!(apply(&mut st, &schema).unwrap().accepted);
        assert!(
            !apply(&mut st, &schema).unwrap().accepted,
            "重复建 schema 应报 accepted=false（幂等命中），而不是 Err"
        );

        let commit = StateOp::CommitFiles {
            request: rich_request(),
            now_ms: 11,
        };
        assert!(apply(&mut st, &commit).unwrap().accepted);
        assert!(
            !apply(&mut st, &commit).unwrap().accepted,
            "同 batch_id 重复提交应报 accepted=false"
        );
    }

    /// op 的 `now_ms` 必须**进状态**：同一 op、不同时间 → 状态必须不同。
    ///
    /// 用 `commit_files`（幂等记录里落 `committed_at`）而不是 `create_schema` ——
    /// 后者**不记录时间**（schema 注册表只有名字），拿它测会得到"两个状态相同"的**假失败**。
    /// 这条同时证明"时间确实来自 op，而不是 apply 里读钟"。
    #[test]
    fn now_ms_is_carried_into_the_state() {
        let mut a = sm_with_table();
        let mut b = sm_with_table();
        let early = StateOp::CommitFiles {
            request: rich_request(),
            now_ms: 100,
        };
        let late = StateOp::CommitFiles {
            request: rich_request(),
            now_ms: 200,
        };
        apply(&mut a, &early).unwrap();
        apply(&mut b, &late).unwrap();
        assert_ne!(
            a.encode_canonical(),
            b.encode_canonical(),
            "同一 op 不同时间戳应产生不同状态（说明时间来自 op 而非读钟）"
        );
        // 反向确认：**同一时间**的同一 op 必须得到同一状态（否则就是读了钟）
        let mut c = sm_with_table();
        apply(&mut c, &early).unwrap();
        assert_eq!(
            a.encode_canonical(),
            c.encode_canonical(),
            "同 op 同时刻必须确定（状态机内不得读钟）"
        );
    }

    /// `TableMeta` 的线上镜像必须**逐字段无损**（它是 `Prefetch` 载荷的内容）。
    ///
    /// 为什么单独立一条：少一个字段 = 客户端本地缓存里少一样东西，而**不会有任何报错** ——
    /// 表现是"某个功能悄悄不生效"。另外两条语义也在这里钉住：
    /// ① 线上以**全限定名**为权威（裸名会让 `public.cpu`/`analytics.cpu` 撞车）；
    /// ② `ingest_config` 用 `optional` 保住"没配"与"空配置"的区别。
    #[test]
    fn table_meta_mirror_is_lossless_and_qualified() {
        let mut m = TableMeta {
            name: "cpu".into(),
            namespace: "analytics".into(),
            current_schema_version: 7,
            partition_cols: vec!["dt".into(), "region".into()],
            default_format: "vortex".into(),
            arrow_schema: vec![1, 2, 3, 4],
            ingest_config: Some(yuntun_model::meta::IngestConfig::standard()),
            created_at: 1_700_000_000_000,
            table_template: 2,
        };

        let p = table_meta_to_proto(&m);
        assert_eq!(p.name, "analytics.cpu", "线上必须是全限定名（缓存键靠它唯一）");
        assert_eq!(p.namespace, "analytics", "冗余副本要填归一化后的值（供解码侧交叉校验）");

        let back = table_meta_from_proto(&p).expect("round-trip");
        assert_eq!(back.name, m.name);
        assert_eq!(back.namespace, m.namespace);
        assert_eq!(back.current_schema_version, m.current_schema_version);
        assert_eq!(back.partition_cols, m.partition_cols);
        assert_eq!(back.default_format, m.default_format);
        assert_eq!(back.arrow_schema, m.arrow_schema);
        assert_eq!(back.created_at, m.created_at);
        assert_eq!(back.table_template, m.table_template);
        assert_eq!(back.ingest_config, m.ingest_config, "ingest_config 必须无损");

        // ② `None` 不能被"补成"默认配置（optional 的存在性就是为这个）
        m.ingest_config = None;
        let p2 = table_meta_to_proto(&m);
        assert!(
            p2.ingest_config.is_none(),
            "没配 ingest_config 时线上字段必须**不设**，而不是给一段空字节"
        );
        assert_eq!(
            table_meta_from_proto(&p2).unwrap().ingest_config,
            None,
            "没配就要还原成 None（补一个 default 会让'没配'与'配了默认'再也分不开）"
        );

        // ③ 两个字段写同一个事实 → 不一致必须报错（静默取一个会让名字与 namespace 指向不同表）
        let bad = pb::TableMeta {
            name: "public.cpu".into(),
            namespace: "analytics".into(),
            ..Default::default()
        };
        let e = table_meta_from_proto(&bad).expect_err("不一致必须拒绝");
        assert!(format!("{e}").contains("不一致"), "{e}");
    }


    /// `SchemaChange` 三个变体的镜像必须**逐字段无损**。
    ///
    /// 为什么挑「带时区的 Timestamp」和「Decimal」这两种：它们最容易被"看起来合理"的
    /// 简化编码搞坏（丢掉时区/精度），而丢掉之后**照样能跑**，只是语义悄悄变了。
    #[test]
    fn schema_change_mirror_covers_all_three_variants() {
        use arrow::datatypes::TimeUnit;
        let cases = vec![
            SchemaChange::AddColumn {
                field: Field::new("score", DataType::Decimal128(18, 4), true),
            },
            SchemaChange::WidenType {
                column: "ts".into(),
                to: DataType::Timestamp(TimeUnit::Microsecond, Some("Asia/Shanghai".into())),
            },
            SchemaChange::DropColumn {
                column: "old".into(),
            },
        ];
        for c in cases {
            let back = schema_change_from_proto(&schema_change_to_proto(&c))
                .unwrap_or_else(|e| panic!("往返失败：{c:?} → {e}"));
            assert_eq!(back, c, "SchemaChange 往返不一致");
        }

        // ---- 非法载荷必须**拒绝**，不能"尽力解释" ----
        // ① 空 kind
        let e = schema_change_from_proto(&pb::SchemaChangeMsg { kind: None }).unwrap_err();
        assert!(format!("{e}").contains("kind"), "{e}");

        // ② add_column 的载荷不是「恰好 1 个字段」（0 字段）
        let empty_schema: std::sync::Arc<Schema> = std::sync::Arc::new(Schema::new(Vec::<Field>::new()));
        let empty = yuntun_model::meta::serialize_schema(&empty_schema);
        let e = schema_change_from_proto(&pb::SchemaChangeMsg {
            kind: Some(pb::schema_change_msg::Kind::AddColumnFieldIpc(empty)),
        })
        .unwrap_err();
        assert!(format!("{e}").contains("恰好 1 个字段"), "{e}");
    }

    /// 另外 4 个新 op（`drop_schema`/`drop_shard`/`compaction`/`idempotency`）的镜像与解码。
    #[test]
    fn new_op_mirrors_are_lossless_and_decodable() {
        let now = 1_700_000_000_000u64;

        // ① evolve_schema：OCC 版本必须带过去（丢了就退化成"永远成功"）
        let req = EvolveSchemaRequest {
            table: "public.cpu".into(),
            change: SchemaChange::DropColumn {
                column: "x".into(),
            },
            expected_version: 7,
        };
        let back = evolve_schema_from_proto(&evolve_schema_to_proto(&req)).unwrap();
        assert_eq!(back.table, req.table);
        assert_eq!(back.change, req.change);
        assert_eq!(back.expected_version, 7, "OCC 版本丢了 → DDL 变成无条件覆盖");

        // ② idempotency：**空 batch_id 有语义**（已认领、批次未落盘），不能被"补默认值"
        let rec = IdempotencyRecord {
            client_request_id: "key-1".into(),
            batch_id: String::new(),
            committed_at: 1_700_000_000,
        };
        assert_eq!(idempotency_record_from_proto(&idempotency_record_to_proto(&rec)), rec);

        // ③ 五个新分支都要能被 `decode_op` 解出来，且带上 op 的时间（纪律 1）
        let decode = |kind: pb::op::Kind, what: &str| {
            let op = pb::Op {
                now_ms: now,
                kind: Some(kind),
            };
            let d = decode_op(&op).unwrap_or_else(|e| panic!("{what} 解码失败：{e}"));
            assert_eq!(d.now_ms(), now, "{what} 丢了 op 时间 → apply 会去读墙钟");
            d
        };
        assert!(matches!(
            decode(pb::op::Kind::DropSchema(pb::DropSchemaOp { name: "a".into() }), "drop_schema"),
            StateOp::DropSchema { .. }
        ));
        assert!(matches!(
            decode(
                pb::op::Kind::DropShard(pb::DropShardOp {
                    table: "public.cpu".into(),
                    shard: "s0".into()
                }),
                "drop_shard"
            ),
            StateOp::DropShard { .. }
        ));
        let f = rich_request().files[0].clone();
        match decode(
            pb::op::Kind::Compaction(pb::CompactionOp {
                old_batch_ids: vec!["b1".into(), "b2".into()],
                new_files: vec![manifest_to_proto(&f)],
            }),
            "compaction",
        ) {
            StateOp::Compaction {
                old_batch_ids,
                new_files,
                ..
            } => {
                assert_eq!(old_batch_ids, vec!["b1".to_string(), "b2".to_string()]);
                assert_eq!(new_files, vec![f], "compaction 的新文件必须逐字段无损");
            }
            other => panic!("解错了分支：{other:?}"),
        }
        assert!(matches!(
            decode(
                pb::op::Kind::Idempotency(pb::IdempotencyOp {
                    record: Some(idempotency_record_to_proto(&rec))
                }),
                "idempotency"
            ),
            StateOp::RecordIdempotency { .. }
        ));

        // ④ 缺 `record` 的幂等 op 必须报错（不能当空操作 —— 那等于让客户端"认领成功"）
        let e = decode_op(&pb::Op {
            now_ms: now,
            kind: Some(pb::op::Kind::Idempotency(pb::IdempotencyOp { record: None })),
        })
        .unwrap_err();
        assert!(format!("{e}").contains("record"), "{e}");
    }


    /// 新 op 的**幂等语义**：重复执行 → `accepted=false`（**不是**错误）。
    ///
    /// 为什么这条重要：客户端会重试，raft 重启后还会**重放**已提交日志 ——
    /// 重放时若把"已经做过"当错误，副本会在重启后直接起不来（fatal）。
    #[test]
    fn new_ops_are_idempotent_on_replay() {
        let mut sm = sm_with_table();
        let now = 1_700_000_000_000u64;
        let rec = IdempotencyRecord {
            client_request_id: "k1".into(),
            batch_id: String::new(),
            committed_at: 1,
        };

        // ① 幂等认领：第一次 true，第二次命中（false），且**不覆盖**已有记录
        let first = apply(
            &mut sm,
            &StateOp::RecordIdempotency {
                record: rec.clone(),
                now_ms: now,
            },
        )
        .unwrap();
        assert!(first.accepted);
        let again = apply(
            &mut sm,
            &StateOp::RecordIdempotency {
                record: IdempotencyRecord {
                    batch_id: "later-batch".into(),
                    ..rec.clone()
                },
                now_ms: now,
            },
        )
        .unwrap();
        assert!(!again.accepted, "重复认领必须命中（accepted=false）");
        assert_eq!(
            sm.check_idempotency("k1"),
            Some(String::new()),
            "命中时**不得覆盖**已有记录（否则认领会变成\"刷新\"，掩盖并发双写）"
        );

        // ② drop_shard：没有文件可删 → false（幂等，不是错误）
        let d = apply(
            &mut sm,
            &StateOp::DropShard {
                table: "public.cpu".into(),
                shard: "s-nope".into(),
                now_ms: now,
            },
        )
        .unwrap();
        assert!(!d.accepted);

        // ③ drop_schema：不存在 → false；删除存在且为空的 → true；再删 → false
        let f = apply(
            &mut sm,
            &StateOp::DropSchema {
                name: "nope".into(),
                now_ms: now,
            },
        )
        .unwrap();
        assert!(!f.accepted, "删不存在的 schema 必须幂等（false 而非错误）");
        sm.create_schema("tmp").unwrap();
        assert!(apply(
            &mut sm,
            &StateOp::DropSchema {
                name: "tmp".into(),
                now_ms: now
            }
        )
        .unwrap()
        .accepted);
        assert!(!apply(
            &mut sm,
            &StateOp::DropSchema {
                name: "tmp".into(),
                now_ms: now
            }
        )
        .unwrap()
        .accepted);
    }

}
