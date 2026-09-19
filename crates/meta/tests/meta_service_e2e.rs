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
//! | 未实现的方法（Prefetch/Join） | 明确 `UNIMPLEMENTED`（**不许**假装成功） |

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
        })
        .await
        .expect("delta 应当成功")
        .into_inner();
    assert!(d.manifest_ver > 0);
    assert!(
        d.changed_tables.iter().any(|t| t.contains("cpu")),
        "自版本 0 以来 public.cpu 变过：{d:?}"
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
    let e = client
        .prefetch(pb::PrefetchRequest {
            since_schema_ver: 0,
            since_manifest_ver: 0,
            full: true,
        })
        .await
        .expect_err("Prefetch 载荷形状未定（属 S3-4）");
    assert_eq!(e.code(), tonic::Code::Unimplemented);

    server.abort();
}
