//! S3-0 的验收：**接口面生成正确 + op 载荷逐字段不丢**（`metanode-design.md §6`）。
//!
//! 这里守的是"迁移中的那条边界"：`yuntun-model` 的请求结构是**手写普通 Rust 结构**（不是 prost），
//! 所以 proto 必须**逐字段镜像**它们。镜像一旦漏字段，就只有线上（或更糟：换主之后）才会发现。
//!
//! 映射函数目前写在测试里（作为"边界转换器"的原型）；生产实现会把它放进 `yuntun-meta`，
//! 紧挨着状态机的 apply 路径。

use prost::Message;
use yuntun_model::meta::{ColumnStatLite, FileManifest, StatisticsLite};
use yuntun_model::ops::CommitFilesRequest;
use yuntun_proto::meta::*;

fn roundtrip<M: Message + Default + PartialEq + std::fmt::Debug>(m: &M) -> M {
    let bytes = m.encode_to_vec();
    M::decode(bytes.as_slice()).expect("解码应当成功")
}

// ---------------------------------------------------------------- 边界转换器（手写结构 ↔ proto）

fn to_manifest(f: &FileManifest) -> FileManifestMsg {
    FileManifestMsg {
        file_path: f.file_path.clone(),
        batch_id: f.batch_id.clone(),
        client_request_id: f.client_request_id.clone(),
        schema_version: f.schema_version,
        status: f.status,
        valid_from: f.valid_from,
        deleted_at: f.deleted_at,
        stats: f.stats.as_ref().map(|s| StatisticsLiteMsg {
            columns: s
                .columns
                .iter()
                .map(|c| ColumnStatLiteMsg {
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

fn from_manifest(m: &FileManifestMsg) -> FileManifest {
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

fn to_msg(r: &CommitFilesRequest) -> CommitFilesRequestMsg {
    CommitFilesRequestMsg {
        table: r.table.clone(),
        batch_id: r.batch_id.clone(),
        client_request_id: r.client_request_id.clone(),
        client_request_ids: r.client_request_ids.clone(),
        shard: r.shard.clone(),
        time_window: r.time_window.clone(),
        files: r.files.iter().map(to_manifest).collect(),
        schema_version: r.schema_version,
        row_count: r.row_count,
    }
}

fn from_msg(m: &CommitFilesRequestMsg) -> CommitFilesRequest {
    CommitFilesRequest {
        table: m.table.clone(),
        batch_id: m.batch_id.clone(),
        client_request_id: m.client_request_id.clone(),
        client_request_ids: m.client_request_ids.clone(),
        shard: m.shard.clone(),
        time_window: m.time_window.clone(),
        files: m.files.iter().map(from_manifest).collect(),
        schema_version: m.schema_version,
        row_count: m.row_count,
    }
}

/// **每个字段都非默认值**的样本（否则"漏字段"会被默认值掩盖，测试变空转）。
fn rich_commit_request() -> CommitFilesRequest {
    CommitFilesRequest {
        table: "public.cpu".into(),
        batch_id: "b-42".into(),
        client_request_id: Some("req-legacy".into()),
        client_request_ids: vec!["k1".into(), "k2".into()],
        shard: "s3".into(),
        time_window: "2026-09-19T10:00".into(),
        files: vec![
            FileManifest {
                file_path: "p/b-42-0.parquet".into(),
                batch_id: "b-42".into(),
                client_request_id: "k1".into(),
                schema_version: 2,
                status: 1,
                valid_from: 11,
                deleted_at: 12,
                stats: Some(StatisticsLite {
                    columns: vec![
                        ColumnStatLite {
                            name: "ts".into(),
                            min: vec![1, 2, 3],
                            max: vec![4, 5, 6],
                            null_count: 7,
                        },
                        ColumnStatLite {
                            name: "host".into(),
                            min: vec![9],
                            max: vec![10],
                            null_count: 0,
                        },
                    ],
                }),
                row_count: 12345,
                file_size: 67890,
                table: "public.cpu".into(),
                shard: "s3".into(),
                time_window: "2026-09-19T10:00".into(),
                partition_key: "2026-09-19T10".into(),
                source_instance: "inst-7".into(),
                sealed_at_ms: 111,
                committed_at_ms: 222,
                seal_reason: "time_threshold".into(),
                seal_pressure: "memory:0.42".into(),
            },
            FileManifest {
                file_path: "p/b-42-1.parquet".into(),
                batch_id: "b-42".into(),
                client_request_id: "k2".into(),
                schema_version: 2,
                status: 1,
                valid_from: 11,
                deleted_at: 0,
                stats: None, // 另一条：**没有**统计，防止"只有 Some 才被测到"
                row_count: 1,
                file_size: 2,
                table: "public.cpu".into(),
                shard: "s3".into(),
                time_window: "2026-09-19T10:00".into(),
                partition_key: "2026-09-19T10".into(),
                source_instance: "inst-7".into(),
                sealed_at_ms: 333,
                committed_at_ms: 444,
                seal_reason: "rows_threshold".into(),
                seal_pressure: "disk:0.80".into(),
            },
        ],
        schema_version: 2,
        row_count: 12346,
    }
}

// ---------------------------------------------------------------- 用例

/// 设计 §6 **点名的那条**：与 `CommitFilesRequest` **逐字段对齐**（无损往返）。
#[test]
fn commit_files_is_field_for_field_lossless() {
    let original = rich_commit_request();
    let req = ProposeRequest {
        op: Some(Op {
            now_ms: 1_700_000_000_000,
            kind: Some(op::Kind::CommitFiles(CommitFilesOp {
                request: Some(to_msg(&original)),
            })),
        }),
        request_id: b"rid".to_vec(),
        schema_ver: 2,
    };
    let back = roundtrip(&req);
    assert_eq!(back.op.as_ref().unwrap().now_ms, 1_700_000_000_000, "时间戳必须随 op 传播（约定 1）");
    let decoded = match back.op.unwrap().kind.unwrap() {
        op::Kind::CommitFiles(c) => from_msg(c.request.as_ref().expect("request 必须存在")),
        other => panic!("op 类型变了：{other:?}"),
    };
    // `CommitFilesRequest` 没实现 `PartialEq`（手写结构），所以**逐字段**断言 ——
    // 这比整体相等更贴题：它把"每个字段都必须活下来"写进了断言名里。
    assert_eq!(decoded.table, original.table, "table 丢了");
    assert_eq!(decoded.batch_id, original.batch_id, "batch_id 丢了");
    assert_eq!(
        decoded.client_request_id, original.client_request_id,
        "单键（历史字段）丢了"
    );
    assert_eq!(
        decoded.client_request_ids, original.client_request_ids,
        "幂等键集合丢了"
    );
    assert_eq!(decoded.shard, original.shard, "shard 丢了");
    assert_eq!(decoded.time_window, original.time_window, "time_window 丢了");
    assert_eq!(decoded.schema_version, original.schema_version, "schema_version 丢了");
    assert_eq!(decoded.row_count, original.row_count, "row_count 丢了");
    assert_eq!(decoded.files.len(), original.files.len(), "文件数变了");
    for (i, (got, want)) in decoded.files.iter().zip(original.files.iter()).enumerate() {
        assert_eq!(got.file_path, want.file_path, "files[{i}].file_path");
        assert_eq!(got.batch_id, want.batch_id, "files[{i}].batch_id");
        assert_eq!(got.client_request_id, want.client_request_id, "files[{i}].client_request_id");
        assert_eq!(got.schema_version, want.schema_version, "files[{i}].schema_version");
        assert_eq!(got.status, want.status, "files[{i}].status");
        assert_eq!(got.valid_from, want.valid_from, "files[{i}].valid_from");
        assert_eq!(got.deleted_at, want.deleted_at, "files[{i}].deleted_at");
        assert_eq!(got.row_count, want.row_count, "files[{i}].row_count");
        assert_eq!(got.file_size, want.file_size, "files[{i}].file_size");
        assert_eq!(got.table, want.table, "files[{i}].table");
        assert_eq!(got.shard, want.shard, "files[{i}].shard");
        assert_eq!(got.time_window, want.time_window, "files[{i}].time_window");
        assert_eq!(got.partition_key, want.partition_key, "files[{i}].partition_key");
        assert_eq!(got.source_instance, want.source_instance, "files[{i}].source_instance");
        assert_eq!(got.sealed_at_ms, want.sealed_at_ms, "files[{i}].sealed_at_ms");
        assert_eq!(got.committed_at_ms, want.committed_at_ms, "files[{i}].committed_at_ms");
        // 后加的可观测性字段（T8 / 封口原因）最容易被漏 —— 单独点名
        assert_eq!(got.seal_reason, want.seal_reason, "files[{i}].seal_reason 丢了");
        assert_eq!(got.seal_pressure, want.seal_pressure, "files[{i}].seal_pressure 丢了");
        match (&got.stats, &want.stats) {
            (None, None) => {}
            (Some(g), Some(w)) => {
                assert_eq!(g.columns.len(), w.columns.len(), "files[{i}].stats.columns 数变了");
                for (j, (gc, wc)) in g.columns.iter().zip(w.columns.iter()).enumerate() {
                    assert_eq!(gc.name, wc.name, "files[{i}].stats[{j}].name");
                    assert_eq!(gc.min, wc.min, "files[{i}].stats[{j}].min");
                    assert_eq!(gc.max, wc.max, "files[{i}].stats[{j}].max");
                    assert_eq!(gc.null_count, wc.null_count, "files[{i}].stats[{j}].null_count");
                }
            }
            (a, b) => panic!("files[{i}].stats 有无不一致：{a:?} vs {b:?}"),
        }
    }
}

/// 每个 op 分支都要能往返；**分支数变化必须显式确认**（防"加了分支没人测"）。
#[test]
fn propose_request_roundtrips_for_every_op_variant() {
    let variants: Vec<op::Kind> = vec![
        op::Kind::CreateSchema(CreateSchemaOp {
            name: "analytics".into(),
        }),
        op::Kind::DropTable(DropTableOp {
            name: "public.cpu".into(),
        }),
        op::Kind::CommitFiles(CommitFilesOp {
            request: Some(to_msg(&rich_commit_request())),
        }),
        op::Kind::CreateTable(CreateTableOp {
            name: "cpu".into(),
            namespace: "public".into(),
            arrow_schema_ipc: vec![1, 2, 3],
            default_format: "parquet".into(),
            partition_cols: vec!["dt".into()],
            ingest_config: vec![],
        }),
        // ---- S3-4 第二批（`operation-log §53`）----
        op::Kind::DropSchema(DropSchemaOp {
            name: "analytics".into(),
        }),
        op::Kind::EvolveSchema(EvolveSchemaOp {
            table: "public.cpu".into(),
            change: Some(SchemaChangeMsg {
                kind: Some(schema_change_msg::Kind::DropColumn(DropColumnMsg {
                    column: "x".into(),
                })),
            }),
            expected_version: 7,
        }),
        op::Kind::DropShard(DropShardOp {
            table: "public.cpu".into(),
            shard: "s0".into(),
        }),
        op::Kind::Compaction(CompactionOp {
            old_batch_ids: vec!["b1".into()],
            new_files: vec![to_manifest(&FileManifest::default())],
            // 非零值：这一条是**逐字段无损**测试，0 会让"忘了搬这个字段"看不出来
            lease_epoch: 7,
        }),
        op::Kind::Idempotency(IdempotencyOp {
            record: Some(IdempotencyRecordMsg {
                client_request_id: "k1".into(),
                // 空 batch_id 有语义（已认领、批次未落盘）
                batch_id: String::new(),
                committed_at: 1_700_000_000,
            }),
        }),
    ];
    assert_eq!(
        variants.len(),
        9,
        "分支数变了：请把新分支加进来，并更新 meta.proto 的迁移进度表"
    );
    for kind in variants {
        let req = ProposeRequest {
            op: Some(Op {
                now_ms: 1,
                kind: Some(kind),
            }),
            request_id: b"r".to_vec(),
            schema_ver: 0,
        };
        assert_eq!(roundtrip(&req), req, "ProposeRequest 往返不一致");
    }
}

/// 约定 2 的守卫：响应必须**同时**给出两组版本号（少一个会让别的节点缓存静默失效）。
#[test]
fn propose_response_carries_both_version_numbers() {
    let resp = ProposeResponse {
        accepted: true,
        revision: 42,
        schema_ver: 7,
        manifest_ver: 9,
        result: vec![1],
        snapshot: 100,
        affected: 3,
    };
    let back = roundtrip(&resp);
    assert_eq!(back.schema_ver, 7, "schema_ver 必须带");
    assert_eq!(back.manifest_ver, 9, "manifest_ver 必须带");
    assert_eq!(back.revision, 42);
    assert_eq!(back.snapshot, 100);
    assert_eq!(
        back.affected, 3,
        "`affected` 必须过线：远端客户端算不出这个数（删分片标记了几个文件），\
         缺了它 `drop_shard -> u64` 只能给 1/0"
    );
    assert!(back.accepted);
}

/// 运维字段一个都不能少 —— 这些是"排查 follower 追不上"的第一手信息。
#[test]
fn status_response_exposes_all_operational_fields() {
    let s = StatusResponse {
        node_id: 2,
        role: "follower".into(),
        term: 5,
        leader_id: 1,
        commit_index: 30,
        applied_index: 28,
        snapshot_index: 20,
        first_index: 21,
        last_index: 30,
        version: yuntun_proto::PROTO_VERSION.into(),
                leases: Vec::new(),
        voter_ids: vec![1, 2, 3],
        learner_ids: vec![4],
        };
    let back = roundtrip(&s);
    assert_eq!(back, s);
    // 快照健康度的三个量必须都在（缺任一个都无法判断"该发快照了吗"）
    assert!(back.snapshot_index > 0 && back.first_index == back.snapshot_index + 1);
    assert!(back.last_index >= back.applied_index);
    // 成员表（`§120`）也**必须过线**：成员变更（提升 learner→voter、移除）没有它对外就是
    // **不可观测**的 —— 运维只能靠"写入还通不通"倒推，而那条路既慢又不可靠。
    assert_eq!(back.voter_ids, vec![1, 2, 3]);
    assert_eq!(back.learner_ids, vec![4]);
}

/// 生成的**服务面**存在（client + server 两侧），且包名与 `PROTO_VERSION` 一致。
#[test]
fn service_surface_is_generated() {
    let server = std::any::type_name::<meta_server::MetaServer<()>>();
    let client = std::any::type_name::<meta_client::MetaClient<()>>();
    assert!(server.contains("MetaServer"), "{server}");
    assert!(client.contains("MetaClient"), "{client}");
    assert_eq!(yuntun_proto::PROTO_VERSION, "yuntun.meta.v1");
}

/// 幂等键**集合**必须在（S3-5 的跨进程去重靠它；退回单键会让多键 chunk 重复落盘）。
#[test]
fn idempotency_key_set_survives_the_envelope() {
    let r = rich_commit_request();
    let msg = to_msg(&r);
    let back = roundtrip(&msg);
    assert_eq!(back.client_request_ids, vec!["k1", "k2"], "键集合丢了");
    assert_eq!(back.client_request_id.as_deref(), Some("req-legacy"));
    assert_eq!(from_msg(&back).client_request_ids, r.client_request_ids);
}

/// T12.3：数据节点注册 op 的载荷往返。
///
/// 这个 op 的载荷**只有身份与地址**（没有时间戳）：注册时刻由 `apply` 用 op 的 `now_ms`
/// 落章 —— 时间戳若随载荷自述，同一串 op 在不同副本上会得到不同的名录时刻。
#[test]
fn register_datanode_op_roundtrips_field_by_field() {
    let op = Op {
        now_ms: 1_700_000_000_000,
        kind: Some(op::Kind::RegisterDatanode(RegisterDatanodeOp {
            instance_id: "inst-a".into(),
            address: "10.0.0.7:50051".into(),
        })),
    };
    let back = roundtrip(&op);
    assert_eq!(back.now_ms, op.now_ms);
    match back.kind {
        Some(op::Kind::RegisterDatanode(r)) => {
            assert_eq!(r.instance_id, "inst-a");
            assert_eq!(r.address, "10.0.0.7:50051");
        }
        other => panic!("kind 往返后变了：{other:?}"),
    }
}

/// T12.3：名录条目（下发载荷）往返逐字段不变。
#[test]
fn datanode_member_msg_roundtrips_field_by_field() {
    let m = DatanodeMemberMsg {
        instance_id: "inst-a".into(),
        address: "10.0.0.7:50051".into(),
        registered_at_ms: 1_700_000_000_000,
    };
    let back = roundtrip(&m);
    assert_eq!(back, m, "名录条目必须逐字段无损（含注册时刻）");
}
