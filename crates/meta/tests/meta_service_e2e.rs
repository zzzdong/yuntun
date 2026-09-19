//! S3-3 的**端到端**验收：真起 gRPC 服务、真客户端调用（不是进程内直调）。
//!
//! 前面那些用例证明的是"状态机/raft/存储/op 路径"各自正确；这一条证明的是**它们接起来
//! 能被远端用**：从 `MetaClient` 发 `ProposeRequest`，走 TCP + HTTP/2，落到 raft，
//! 应用到状态机，再把 `ProposeResponse` 送回客户端 —— 中间任何一环断了都会红。
//!
//! 刻意覆盖三类交互：
//!
//! | 交互 | 期望 |
//! |---|---|
//! | 正常写入（建 schema / 建表 / 提交） | 成功，且回应带 raft index 与版本号 |
//! | **幂等重试**（同键重复提交） | 成功但 `accepted=false`（**不是**错误 —— 客户端重试必须成功） |
//! | 未实现的方法（Join） | 明确 `UNIMPLEMENTED`（**不许**假装成功） |
//! | `Prefetch`（S3-4 已实现） | 见 `prefetch_payload_semantics`（版本探测 / 差量 / 全量 / 超前保护） |

use std::time::Duration;

use prost::Message as _;
use yuntun_meta::Cluster;
use yuntun_proto::meta as pb;
use yuntun_proto::meta::meta_client::MetaClient;

const T: Duration = Duration::from_secs(60);

fn create_schema_op(name: &str, now: u64) -> pb::Op {
    pb::Op {
        now_ms: now,
        kind: Some(pb::op::Kind::CreateSchema(pb::CreateSchemaOp {
            name: name.into(),
        })),
    }
}

fn create_table_op(name: &str, now: u64) -> pb::Op {
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("ts", arrow::datatypes::DataType::Int64, false),
    ]));
    pb::Op {
        now_ms: now,
        kind: Some(pb::op::Kind::CreateTable(pb::CreateTableOp {
            name: name.into(),
            namespace: "public".into(),
            arrow_schema_ipc: yuntun_model::meta::serialize_schema(&schema),
            default_format: "parquet".into(),
            partition_cols: vec![],
            ingest_config: yuntun_model::meta::IngestConfig::standard().encode_to_vec(),
        })),
    }
}

/// 带**幂等键**的提交（这样"重试"才是真正的幂等命中，而不是覆盖写）。
fn commit_op(batch_id: &str, key: &str, rows: u64, now: u64) -> pb::Op {
    pb::Op {
        now_ms: now,
        kind: Some(pb::op::Kind::CommitFiles(pb::CommitFilesOp {
            request: Some(pb::CommitFilesRequestMsg {
                table: "public.cpu".into(),
                batch_id: batch_id.into(),
                client_request_id: Some(key.into()),
                client_request_ids: vec![key.into()],
                shard: "s0".into(),
                time_window: "w1".into(),
                files: vec![pb::FileManifestMsg {
                    file_path: format!("p/{batch_id}.parquet"),
                    batch_id: batch_id.into(),
                    row_count: rows,
                    ..Default::default()
                }],
                schema_version: 1,
                row_count: rows,
            }),
        })),
    }
}

async fn connect(addr: std::net::SocketAddr) -> MetaClient<tonic::transport::Channel> {
    // 服务刚 spawn 时端口可能还没开始 accept：重试几次而不是直接失败
    let endpoint = format!("http://{addr}");
    for _ in 0..100 {
        if let Ok(c) = MetaClient::connect(endpoint.clone()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("连不上 metanode gRPC 服务：{endpoint}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn propose_status_delta_over_real_grpc() {
    let cluster = Cluster::start();
    let leader = cluster.wait_leader(T).expect("三节点应选出 leader");
    let node = cluster.handle(leader).expect("取 leader 句柄");

    // 起服务：`127.0.0.1:0` → 内核分配端口，测试之间不会抢端口
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { yuntun_meta::serve(node, listener).await });

    let mut client = connect(addr).await;

    // ---- 正常写入 ----
    let mut last = pb::ProposeResponse::default();
    for op in [
        create_schema_op("analytics", 1_000),
        create_table_op("cpu", 1_001),
        commit_op("b-e2e", "key-e2e", 42, 1_002),
    ] {
        let resp = client
            .propose(pb::ProposeRequest {
                op: Some(op),
                request_id: b"rid".to_vec(),
                schema_ver: 0,
            })
            .await
            .expect("提议应当成功")
            .into_inner();
        assert!(resp.accepted, "首次提议必须被接受：{resp:?}");
        assert!(resp.revision > 0, "必须带回 raft index（权威位置）");
        last = resp;
    }
    assert!(
        last.manifest_ver > 0,
        "提交文件必须推进 manifest_ver（约定 2：两组版本号都要给）"
    );

    // ---- 幂等重试：同键同 batch 再提一次 ----
    let dup = client
        .propose(pb::ProposeRequest {
            op: Some(commit_op("b-e2e", "key-e2e", 42, 1_002)),
            request_id: b"rid".to_vec(),
            schema_ver: 0,
        })
        .await
        .expect("幂等重试必须**成功**（不是错误）")
        .into_inner();
    assert!(
        !dup.accepted,
        "同幂等键重复提交应当是幂等命中（accepted=false），而不是重复落盘"
    );

    // ---- Status：字段齐全，且指向 leader 自己 ----
    let st = client
        .status(pb::StatusRequest {})
        .await
        .expect("status 应当成功")
        .into_inner();
    assert_eq!(st.node_id, leader);
    assert_eq!(st.leader_id, leader, "leader 必须指向自己");
    assert_eq!(st.role, "Leader");
    assert!(st.last_index >= st.applied_index, "{st:?}");
    assert!(st.commit_index >= st.applied_index, "{st:?}");
    assert_eq!(st.version, yuntun_proto::PROTO_VERSION);

    // ---- Delta：since=0 → 应报出变过的表 ----
    let d = client
        .delta(pb::DeltaRequest {
            since_manifest_ver: 0,
            since_schema_ver: 0,
        })
        .await
        .expect("delta 应当成功")
        .into_inner();
    assert!(d.manifest_ver > 0);
    assert!(
        d.changed_tables.iter().any(|t| t.contains("cpu")),
        "自版本 0 以来 public.cpu 变过：{d:?}"
    );

    // ---- Delta 的**结构变更**信号（`§57`）：水位给"当前"→ 无变化；给旧 schema 版本 → 要全量 ----
    //
    // 这条盯的是一个**静默丢表**的坑：`changed_tables` 只覆盖 manifest 级变化，
    // 新建/删表只体现在 `schema_ver` 上 —— 客户端照"有变化就增量"做，会丢掉刚建的表。
    let same = client
        .delta(pb::DeltaRequest {
            since_manifest_ver: d.manifest_ver,
            since_schema_ver: d.schema_ver,
        })
        .await
        .expect("delta 应当成功")
        .into_inner();
    assert!(
        !same.full_reload,
        "水位已是最新时不该要求全量重建：{same:?}"
    );
    let stale = client
        .delta(pb::DeltaRequest {
            since_manifest_ver: d.manifest_ver,
            since_schema_ver: 0, // schema 版本落后（少了建表这次结构变更）
        })
        .await
        .expect("delta 应当成功")
        .into_inner();
    assert!(
        stale.full_reload,
        "schema 版本落后时必须要求全量重建（否则会**静默丢掉结构变更**）：{stale:?}"
    );

    // ---- 未实现的方法必须明确 UNIMPLEMENTED ----
    let e = client
        .join(pb::JoinRequest {
            node_id: 9,
            address: "127.0.0.1:1".into(),
            learner_only: true,
        })
        .await
        .expect_err("Join 尚未实现（属 S3-6）");
    assert_eq!(e.code(), tonic::Code::Unimplemented);
    server.abort();
}

/// `Prefetch` 的**载荷语义**（S3-4 定形）。
///
/// 单独立一条用例的理由：这套语义是**契约**（客户端本地缓存靠它刷新），
/// 而它每一条错法都**不报错** —— 客户端只会"少刷新一次"或"拿错缓存"，
/// 现象是查询结果与元数据悄悄不一致。所以四条分支逐条钉住。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prefetch_payload_semantics() {
    let cluster = Cluster::start();
    let leader = cluster.wait_leader(T).expect("三节点应选出 leader");
    let node = cluster.handle(leader).expect("取 leader 句柄");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { yuntun_meta::serve(node, listener).await });
    let mut client = connect(addr).await;

    for op in [
        create_schema_op("analytics", 1_000),
        create_table_op("cpu", 1_001),
        commit_op("b-pf", "key-pf", 7, 1_002),
    ] {
        let r = client
            .propose(pb::ProposeRequest {
                op: Some(op),
                request_id: b"rid".to_vec(),
                schema_ver: 0,
            })
            .await
            .expect("提议成功")
            .into_inner();
        assert!(r.accepted);
    }

    let prefetch = |c: &mut MetaClient<tonic::transport::Channel>,
                    since_schema: u64,
                    since_manifest: u64,
                    full: bool,
                    tables: Vec<&'static str>| {
        let mut c = c.clone();
        let tables = tables.into_iter().map(|s| s.to_string()).collect();
        async move {
            c.prefetch(pb::PrefetchRequest {
                since_schema_ver: since_schema,
                since_manifest_ver: since_manifest,
                full,
                tables,
                since_snapshot: 0,
            })
            .await
            .expect("prefetch 应当成功")
            .into_inner()
        }
    };

    // ---- ① 纯版本探测（tables 空）→ **零载荷**（设计 §3.2 的"无变化零开销"路径）----
    let probe = prefetch(&mut client, 0, 0, false, vec![]).await;
    assert!(
        probe.payload.as_ref().expect("载荷字段应存在").tables.is_empty(),
        "只要版本号时不该带表载荷（那会让'零开销'变成'每次搬全部元数据'）"
    );
    assert!(!probe.full_reload, "版本探测不该要求重建缓存");
    // 两次结构变更（建 schema + 建表）→ schema_ver=2；提交只推 manifest_ver（设计 §6.3 两组版本号）
    assert_eq!(probe.schema_ver, 2, "两组版本号都要给：{probe:?}");
    assert!(probe.manifest_ver >= 1, "提交过文件 → manifest_ver >= 1");
    assert!(probe.snapshot > 0, "快照号也要给（客户端据此对齐 manifest 语义）");

    // ---- ② 版本没变（since = 当前）→ 版本号原样回，客户端据此**跳过刷新** ----
    let same = prefetch(
        &mut client,
        probe.schema_ver,
        probe.manifest_ver,
        false,
        vec![],
    )
    .await;
    assert_eq!((same.schema_ver, same.manifest_ver), (probe.schema_ver, probe.manifest_ver));
    assert!(same.payload.as_ref().unwrap().tables.is_empty());

    // ---- ③ 差量：请求存在的表 → 只回它；请求不存在的表 → **不在载荷里**（= 已删）----
    let delta = prefetch(
        &mut client,
        0,
        0,
        false,
        vec!["public.cpu", "public.gone"],
    )
    .await;
    let names: Vec<String> = delta
        .payload
        .as_ref()
        .unwrap()
        .tables
        .iter()
        .map(|t| t.name.clone())
        .collect();
    assert_eq!(names, vec!["public.cpu".to_string()], "只回请求里**存在**的表：{names:?}");
    assert!(!delta.full_reload);
    let cpu = &delta.payload.as_ref().unwrap().tables[0];
    assert_eq!(cpu.current_schema_version, 1, "建表即 version 1");
    assert_eq!(cpu.default_format, "parquet");
    assert_eq!(cpu.namespace, "public");
    assert!(!cpu.arrow_schema.is_empty(), "Arrow schema 必须随载荷走（缓存要能解出 schema）");
    assert!(cpu.ingest_config.is_some(), "有配置时必须带（optional 的存在性有语义）");

    // ---- ④ full=true → 全部表（忽略 tables）+ 要求重建缓存 ----
    let all = prefetch(&mut client, 0, 0, true, vec![]).await;
    assert!(all.full_reload, "full=true 必须置 full_reload（客户端要丢本地缓存）");
    let all_names: Vec<String> = all
        .payload
        .as_ref()
        .unwrap()
        .tables
        .iter()
        .map(|t| t.name.clone())
        .collect();
    assert_eq!(all_names, vec!["public.cpu".to_string()]);

    // ---- ⑤ 版本**超前**保护：客户端的版本比本集群新（换过集群/被回滚）----
    // 这时**必须**要求全量重建。若静默返回空，客户端会以为"一切照旧"，
    // 继续拿一份不属于本集群的缓存去规划查询 —— 错得不报错。
    let ahead = prefetch(
        &mut client,
        probe.schema_ver + 100,
        probe.manifest_ver,
        false,
        vec![],
    )
    .await;
    assert!(
        ahead.full_reload,
        "客户端版本超前时必须要求全量重建，而不是回空载荷：{ahead:?}"
    );
    assert!(
        !ahead.payload.as_ref().unwrap().tables.is_empty(),
        "要求全量重建时应当顺手把全量载荷带上（否则客户端还得再发一次请求）"
    );

    server.abort();
}

/// 文件级增量载荷：**必须带墓碑**（`deleted_at`）。
///
/// 为什么单独立一条：只发「当前可见」的文件，客户端就**永远删不掉已删文件** ——
/// 它那边的旧副本还在，查询会去读已删数据（**静默读到脏数据**，不报错）。
/// 这条用例就是拿「删分片」来逼出墓碑的。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prefetch_carries_file_deltas_including_tombstones() {
    let cluster = Cluster::start();
    let leader = cluster.wait_leader(T).expect("三节点应选出 leader");
    let node = cluster.handle(leader).expect("取 leader 句柄");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { yuntun_meta::serve(node, listener).await });
    let mut client = connect(addr).await;

    let propose = |c: &mut MetaClient<tonic::transport::Channel>, op: pb::Op| {
        let mut c = c.clone();
        async move {
            c.propose(pb::ProposeRequest {
                op: Some(op),
                request_id: b"rid".to_vec(),
                schema_ver: 0,
            })
            .await
            .expect("提议成功")
            .into_inner()
        }
    };
    let prefetch_at = |c: &mut MetaClient<tonic::transport::Channel>, since_snapshot: u64| {
        let mut c = c.clone();
        async move {
            c.prefetch(pb::PrefetchRequest {
                since_schema_ver: u64::MAX, // 只关心文件级
                since_manifest_ver: u64::MAX,
                full: false,
                tables: vec!["public.cpu".into()],
                since_snapshot,
            })
            .await
            .expect("prefetch 成功")
            .into_inner()
        }
    };

    for op in [
        create_schema_op("analytics", 1_000),
        create_table_op("cpu", 1_001),
        commit_op("b-file", "key-file", 7, 1_002),
    ] {
        assert!(propose(&mut client, op).await.accepted);
    }

    // ---- ① 全量（since_snapshot=0）→ 应当拿到那个文件，且它是"活的" ----
    let all = prefetch_at(&mut client, 0).await;
    let files = all.payload.as_ref().unwrap().files.clone();
    assert_eq!(files.len(), 1, "应当有一个文件：{files:?}");
    assert_eq!(files[0].batch_id, "b-file");
    let m = files[0].manifest.as_ref().expect("manifest 必填");
    assert!(m.valid_from > 0, "提交时分配的 valid_from 必须带出来");
    assert_eq!(m.deleted_at, 0, "刚提交的文件不是墓碑");

    // ---- ② 水位 = 当前 → **零文件**（客户端已是最新，零开销路径）----
    let now = prefetch_at(&mut client, all.snapshot).await;
    assert!(
        now.payload.as_ref().unwrap().files.is_empty(),
        "水位已是最新时不该回任何文件（否则每次刷新都要搬全量清单）"
    );

    // ---- ③ 删整个 shard → 文件变墓碑；**增量必须把它发回来** ----
    let before_delete = all.snapshot;
    let r = propose(
        &mut client,
        pb::Op {
            now_ms: 2_000,
            kind: Some(pb::op::Kind::DropShard(pb::DropShardOp {
                table: "public.cpu".into(),
                shard: "s0".into(),
            })),
        },
    )
    .await;
    assert!(r.accepted, "删分片应当生效（确有文件被标记）");
    assert_eq!(
        r.affected, 1,
        "**精确条数**必须随提交过线（`affected`）：远端自己算不出来，给 1/0 会让调用方对账失真"
    );

    let delta = prefetch_at(&mut client, before_delete).await;
    let files = delta.payload.as_ref().unwrap().files.clone();
    assert_eq!(
        files.len(),
        1,
        "删分片后必须把**墓碑**发回来（否则客户端永远删不掉它，会读到已删数据）：{files:?}"
    );
    let m = files[0].manifest.as_ref().unwrap();
    assert!(m.deleted_at != 0, "墓碑的判据就是 deleted_at != 0：{m:?}");
    assert!(
        m.valid_from <= before_delete,
        "这个文件的 valid_from 早于水位 —— 说明它**只**能被墓碑那条规则捞出来（这正是要测的）"
    );

    // ---- ④ 再往后推一代：墓碑也不该再出现（客户端已经处理过它）----
    let later = prefetch_at(&mut client, delta.snapshot).await;
    assert!(
        later.payload.as_ref().unwrap().files.is_empty(),
        "已发过的墓碑不该重复发（否则每次刷新都在搬历史）：{:?}",
        later.payload.as_ref().unwrap().files
    );

    server.abort();
}
