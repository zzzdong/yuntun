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
use yuntun_model::meta::{ColumnStatLite, FileManifest, IngestConfig, StatisticsLite};
use yuntun_model::ops::{CommitFilesRequest, CreateTableRequest};

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
}

// `CreateTableRequest` 里有 `SchemaRef`（`Arc<Schema>`）—— `Arc` 未使用会告警
#[allow(unused_imports)]
use Arc as _ArcAlias;

impl StateOp {
    pub fn now_ms(&self) -> u64 {
        match self {
            StateOp::CreateSchema { now_ms, .. }
            | StateOp::CreateTable { now_ms, .. }
            | StateOp::DropTable { now_ms, .. }
            | StateOp::CommitFiles { now_ms, .. } => *now_ms,
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
}
