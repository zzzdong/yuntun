//! S3-3 的**进程级**端到端：真起二进制、真 `kill -9`、真从盘恢复。
//!
//! 与 `meta_service_e2e.rs` 的分工：
//!
//! | 用例 | 验证 |
//! |---|---|
//! | `meta_service_e2e.rs` | **同一进程**内起服务 → RPC 面接线（快、能测未实现方法） |
//! | 本文件 | **真进程** + `kill -9` → 启动路径、CLI 闸门、**真崩溃恢复** |
//!
//! 后者正是 `operation-log §44.4` 登记的遗留（"现在 `kill` 是线程退出；真掉电需要子进程级注入"）。
//! 进程级还有一个同进程测不到的点：**状态机是内存里的，进程死了就没了** ——
//! 恢复只能靠"从盘重放日志/装快照"，这条路径只有真进程死了才走到。

use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use prost::Message as _;
use yuntun_proto::meta as pb;
use yuntun_proto::meta::meta_client::MetaClient;

/// 被测二进制（cargo 为集成测试注入的路径变量；名字 = `[[bin]] name`）。
const BIN: &str = env!("CARGO_BIN_EXE_metanode");

// ---------------------------------------------------------------- 子进程夹具

struct NodeProc {
    child: Child,
    stdout: BufReader<ChildStdout>,
    stderr: Arc<Mutex<String>>,
}

impl NodeProc {
    fn start(dir: &Path, id: u64, init: bool) -> Self {
        let mut cmd = Command::new(BIN);
        cmd.arg("--id")
            .arg(id.to_string())
            .arg("--dir")
            .arg(dir)
            // :0 → 内核分配端口，测试之间不会抢端口（真实地址由进程打印出来）
            .arg("--listen")
            .arg("127.0.0.1:0")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if init {
            cmd.arg("--init");
        }
        let mut child = cmd.spawn().expect("启动 metanode 二进制");
        let stdout = BufReader::new(child.stdout.take().expect("stdout"));
        let pipe = child.stderr.take().expect("stderr");
        // stderr 后台收集：断言失败时能把它打出来（否则只剩"提前退出"四个字）
        let stderr = Arc::new(Mutex::new(String::new()));
        let sink = stderr.clone();
        std::thread::spawn(move || {
            let mut r = BufReader::new(pipe);
            let mut buf = String::new();
            use std::io::Read as _;
            let _ = r.read_to_string(&mut buf);
            *sink.lock().unwrap() = buf;
        });
        Self {
            child,
            stdout,
            stderr,
        }
    }

    /// 读进程打印的 `listening on <addr>`（见 `main.rs`：这一行是**接口**）。
    fn wait_listening(&mut self) -> SocketAddr {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut line = String::new();
        loop {
            line.clear();
            let n = self.stdout.read_line(&mut line).expect("读子进程 stdout");
            if n == 0 {
                panic!(
                    "metanode 未打印监听地址就退出了。stderr:\n{}",
                    self.stderr.lock().unwrap()
                );
            }
            if let Some(rest) = line.split("listening on ").nth(1) {
                let addr = rest.split_whitespace().next().expect("地址");
                return addr.parse().expect("监听地址应当可解析");
            }
            assert!(
                Instant::now() < deadline,
                "等 listening 超时。stderr:\n{}",
                self.stderr.lock().unwrap()
            );
        }
    }

    /// `kill -9`：不给任何清理机会（这才是"掉电/被杀"的形态）。
    fn kill9(&mut self) {
        self.child.kill().expect("发送 SIGKILL");
        let st = self.child.wait().expect("回收子进程");
        assert!(
            !st.success(),
            "SIGKILL 之后不该是正常退出码：{st:?}；stderr:\n{}",
            self.stderr.lock().unwrap()
        );
    }
}

impl Drop for NodeProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct TempDir(PathBuf);
impl TempDir {
    fn new(tag: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!("yuntun-{tag}-{nanos}"));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn connect(addr: SocketAddr) -> MetaClient<tonic::transport::Channel> {
    let endpoint = format!("http://{addr}");
    for _ in 0..200 {
        if let Ok(c) = MetaClient::connect(endpoint.clone()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("连不上 metanode：{endpoint}");
}

// ---------------------------------------------------------------- op 构造

fn create_schema_op(name: &str, now: u64) -> pb::Op {
    pb::Op {
        now_ms: now,
        kind: Some(pb::op::Kind::CreateSchema(pb::CreateSchemaOp {
            name: name.into(),
        })),
    }
}

fn create_table_op(name: &str, now: u64) -> pb::Op {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
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

/// 带幂等键的提交（重启后重放同一个 op → 必须是幂等命中，而不是再落一次）。
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

async fn propose(
    c: &mut MetaClient<tonic::transport::Channel>,
    op: pb::Op,
) -> pb::ProposeResponse {
    c.propose(pb::ProposeRequest {
        op: Some(op),
        request_id: b"rid".to_vec(),
        schema_ver: 0,
    })
    .await
    .expect("提议应当成功")
    .into_inner()
}

// ---------------------------------------------------------------- 用例

/// **进程级崩溃恢复**：写 → `kill -9` → 重启（不带 `--init`）→ 状态机必须已恢复。
///
/// 断言链刻意从"**状态机**恢复"开始，而不是"进程起来了"：
/// 状态机是纯内存的，进程一死就没了 —— 它回来只能靠**从盘重放日志**（或装快照）。
/// 所以 `accepted=false` 这条幂等命中，实际证明的是"盘上的日志 + 恢复路径都对"。
#[test]
fn process_survives_sigkill_and_recovers_state() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");
    let dir = TempDir::new("metanode-proc");

    // ---- ① 首次启动（--init）并写入 ----
    let mut p1 = NodeProc::start(dir.path(), 1, true);
    let addr1 = p1.wait_listening();
    let (schema_ver, manifest_ver, last_before) = rt.block_on(async {
        let mut c = connect(addr1).await;
        propose(&mut c, create_schema_op("analytics", 1_000)).await;
        propose(&mut c, create_table_op("cpu", 1_001)).await;
        let r = propose(&mut c, commit_op("b1", "key-b1", 10, 1_002)).await;
        assert!(r.accepted);
        let st = c
            .status(pb::StatusRequest {})
            .await
            .expect("status")
            .into_inner();
        assert_eq!(st.role, "Leader", "单节点组必须当选");
        (r.schema_ver, r.manifest_ver, st.last_index)
    });

    // ---- ② SIGKILL（真崩溃，不给任何清理机会）----
    p1.kill9();

    // ---- ③ 从同一个目录重启（**不带 --init**）----
    let mut p2 = NodeProc::start(dir.path(), 1, false);
    let addr2 = p2.wait_listening();

    rt.block_on(async {
        let mut c = connect(addr2).await;

        // 状态机恢复：同幂等键再提一次，必须命中（`accepted=false`）。
        // 这条只有在"日志重放 + 状态机幂等表都回来了"时才成立。
        let dup = propose(&mut c, commit_op("b1", "key-b1", 10, 1_002)).await;
        assert!(
            !dup.accepted,
            "崩溃重启后同一个幂等键必须命中（accepted=false）—— 否则状态机没恢复干净"
        );
        assert_eq!(dup.manifest_ver, manifest_ver, "manifest_ver 必须与崩溃前一致");
        assert_eq!(dup.schema_ver, schema_ver, "schema_ver 必须与崩溃前一致");

        // 日志不倒退 + 追平到最新（重启后 raft 会重放；给它一点时间）
        let deadline = Instant::now() + Duration::from_secs(30);
        let st = loop {
            let st = c
                .status(pb::StatusRequest {})
                .await
                .expect("status")
                .into_inner();
            if st.applied_index == st.last_index {
                break st;
            }
            assert!(Instant::now() < deadline, "重启后未追平：{st:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert!(
            st.last_index >= last_before,
            "日志不能倒退：崩溃前 {last_before}，现在 {}",
            st.last_index
        );
        assert_eq!(st.role, "Leader");
        assert_eq!(st.node_id, 1);
        assert_eq!(st.leader_id, 1);

        // 仍然能服务：再写一笔必须是**新的**（不是幂等命中）
        let fresh = propose(&mut c, commit_op("b2", "key-b2", 5, 2_000)).await;
        assert!(fresh.accepted, "崩溃重启后必须还能接受新写入");
        assert!(fresh.manifest_ver > manifest_ver, "新提交必须推进 manifest_ver");
    });

    p2.kill9();
}

/// **CLI 闸门在进程层面的行为**：错配的 `--init` 用法必须以**退出码 2** 拒绝启动
/// （而不是"先跑起来看看"—— 那会让旧数据各自成组，事后极难查）。
#[test]
fn process_refuses_the_two_misuses_of_init() {
    let dir = TempDir::new("metanode-gate");

    // ① 空目录 + 没给 --init → 退出 2
    let out = Command::new(BIN)
        .args(["--id", "1", "--dir"])
        .arg(dir.path())
        .output()
        .expect("跑二进制");
    assert_eq!(out.status.code(), Some(2), "用法错应退出 2");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("首次启动必须显式 --init"), "{stderr}");

    // ② 有数据的目录 + 给了 --init → 退出 2
    //
    // ⚠️ `--init` 的成功路径是"**首次启动并持续服务**"，所以这一步不能 `.output()` 等它退出
    // （会一直挂着 —— 第一次写这用例时就踩了）。按真实形态：起 → 等就绪 → 停。
    {
        let mut p = NodeProc::start(dir.path(), 1, true);
        let _ = p.wait_listening();
        p.kill9();
    }
    let out = Command::new(BIN)
        .args(["--id", "1", "--dir"])
        .arg(dir.path())
        .arg("--init")
        .output()
        .expect("跑二进制");
    assert_eq!(out.status.code(), Some(2), "用法错应退出 2");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("已有数据"), "{stderr}");
}

/// **多节点的两道闸门**：
///
/// ① 盘上成员表与启动参数不一致 → 拒绝（否则本节点会在一个凑不齐的组里静默空转）；
/// ② 成员表里有别的节点、却没给它们地址 → 拒绝（发不出消息 = 永远选不出 leader）。
///
/// ② 现在由 **CLI 语义校验**在启动前拦住（退出码 2），而不是等运行期 —— 早失败、提示更直接。
#[test]
fn process_refuses_membership_mismatch_and_missing_peer() {
    let dir = TempDir::new("metanode-membership");
    // 先按单节点**真正起一次**：`--init` 的语义是"首次启动"，它会一直服务，
    // 所以必须像真部署那样 —— 起 → 等就绪 → 停（用 `.output()` 等它会一直挂着）。
    let mut p = NodeProc::start(dir.path(), 1, true);
    let _ = p.wait_listening();
    p.kill9(); // 此后盘上成员表 = [1]

    // ① 盘上说 [1]，启动参数说 [1,2,3]（地址给全了，所以能走到运行期检查）→ 拒绝
    let out = Command::new(BIN)
        .args(["--id", "1", "--dir"])
        .arg(dir.path())
        .args(["--voters", "1,2,3"])
        .args(["--peer", "2@127.0.0.1:9002,3@127.0.0.1:9003"])
        .output()
        .expect("跑二进制");
    assert_eq!(out.status.code(), Some(1), "启动失败应退出 1");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("盘上成员表"), "{stderr}");

    // ② 多节点却没给 --peer → **用法错**（退 2），且提示缺哪个节点
    let out = Command::new(BIN)
        .args(["--id", "1", "--dir"])
        .arg(dir.path().join("multi"))
        .args(["--voters", "1,2,3", "--init"])
        .output()
        .expect("跑二进制");
    assert_eq!(out.status.code(), Some(2), "用法错应退出 2");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("[2, 3]") || stderr.contains("没给它们地址"),
        "应说明缺哪些对端地址：{stderr}"
    );
}

/// clap 接管语法层后的**统一行为**：`--help`/`--version` 退 0，未知 flag 退 2 并**指出参数名**。
#[test]
fn cli_surface_is_clap_managed() {
    let out = Command::new(BIN).arg("--help").output().expect("跑二进制");
    assert!(out.status.success(), "--help 必须退 0");
    let help = String::from_utf8_lossy(&out.stdout);
    for flag in ["--id", "--dir", "--listen", "--voters", "--init"] {
        assert!(help.contains(flag), "--help 里应列出 {flag}：\n{help}");
    }

    let out = Command::new(BIN).arg("--version").output().expect("跑二进制");
    assert!(out.status.success(), "--version 必须退 0");
    assert!(String::from_utf8_lossy(&out.stdout).contains("metanode"));

    let out = Command::new(BIN)
        .args(["--id", "1", "--dir", "/tmp/x", "--wat"])
        .output()
        .expect("跑二进制");
    assert_eq!(out.status.code(), Some(2), "用法错应退 2");
    assert!(String::from_utf8_lossy(&out.stderr).contains("--wat"));
}
