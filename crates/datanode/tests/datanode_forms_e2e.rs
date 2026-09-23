//! **数据进程的两种形态**端到端（`operation-log §79` 的角色更正后重写）。
//!
//! ```text
//!   ① metanode（进程内，真 gRPC 服务）
//!        ▲ 注册/心跳                        ▲ 只读元数据（名录含**数据面地址**）
//!   ② yuntun-datanode --dir D            ③ yuntun-datanode --dir D --no-ingest --sql-listen
//!      （ingest 形态：WAL⇒热数据+热读服务）  （只查询形态：不吃 WAL，只做协调者）
//!        └──────── ④ 数据面 gRPC：RemoteShard 拉热数据 ────────┘
//!                          ▲
//!                     ⑤ 客户端发 SQL（经 Flight）
//! ```
//!
//! **两个形态是同一条命令的两个开关组合**（`--no-ingest` / `--sql-listen`），不是两类进程 ——
//! 所以这里两个都是**真子进程**（各自打自己的接口行：`LISTEN` / `SQL-LISTEN`）。
//! Flight 仍然是真的：真 TCP、真协议、真 DoGet。
//!
//! 覆盖的语义（都是前面几刀留下、本轮第一次连起来跑的）：
//! - `§71`：名录（含地址）下发 ⇒ `§71.5` 遗留第 3 条"谁按地址建 `GrpcShardFetch`"；
//! - `§65`：按实例拉热数据（只查询形态自己没有数据）；
//! - 只读：SQL 面对写入给出**可读的拒绝**，而不是假装成功。

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_flight::flight_service_client::FlightServiceClient;
use futures::StreamExt as _;
use arrow_flight::sql::CommandStatementQuery;
use tokio::net::TcpListener;

use yuntun_catalog::CatalogOps as _;
use yuntun_proto::meta as pb;

const DATANODE: &str = env!("CARGO_BIN_EXE_yuntun-datanode");
const INSTANCE: &str = "inst-a";
/// 只查询形态的实例名（它**不**入册：名录的语义是"谁持有热数据"）
const QUERY_ONLY: &str = "query-only";
const TABLE: &str = "public.qd";
const WINDOW: &str = "2026-09-23T10:00";

fn tmpdir(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "yuntun-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn schema() -> Arc<arrow::datatypes::Schema> {
    Arc::new(arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
        "a",
        arrow::datatypes::DataType::Int64,
        true,
    )]))
}

/// 预置数据节点的 WAL（它启动后回放 ⇒ 热数据在**另一个进程**里长出来）。
async fn seed_wal(dir: &std::path::Path, rows: &[i64]) {
    use yuntun_model::wal_record::{DataPayload, Record};
    use yuntun_wal::config::WalConfig;
    use yuntun_wal::writer::WalWriter;

    let wal_root = dir.join("wal");
    std::fs::create_dir_all(&wal_root).unwrap();
    let wal = WalWriter::open(
        WalConfig {
            dir: wal_root,
            ..Default::default()
        },
        0,
    )
    .await
    .expect("打开 WAL 以预置数据");

    // **先写 DDL**：数据节点启动时会重放它（`replay_wal_ddl`）⇒ 表进元数据面。
    // 这一条是"数据节点自己把表带进元数据"的证据 —— 用例不再靠客户端建表。
    let ddl = yuntun_model::meta::serialize_schema(&schema());
    wal.append(Record::Ddl(yuntun_model::wal_record::DdlPayload {
        op: yuntun_model::wal_record::ddl_op::CREATE_TABLE,
        table: TABLE.to_string(),
        arrow_schema: ddl,
        default_format: "parquet".to_string(),
    }))
    .await
    .expect("追加 DDL 记录");

    for (i, v) in rows.iter().enumerate() {
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema(),
            vec![Arc::new(arrow::array::Int64Array::from(vec![*v]))],
        )
        .unwrap();
        let ipc = yuntun_shardrpc::encode_batch(&batch).unwrap();
        wal.append(Record::Data(DataPayload {
            table: TABLE.to_string(),
            shard: "default".to_string(),
            schema_version: 1,
            batch_ipc: ipc,
            client_request_id: format!("seed-{i}"),
            time_window: WINDOW.to_string(),
        }))
        .await
        .expect("追加 Data 记录");
    }
}

/// 子进程夹具：piped stdout（读 `LISTEN <addr>`）+ 后台收集的 stderr。
struct Proc {
    child: Child,
    stdout: BufReader<ChildStdout>,
    stderr: Arc<Mutex<String>>,
}

impl Proc {
    /// 把已 spawn 的子进程包上 stdout/stderr 采集。
    fn wrap(mut child: Child) -> Self {
        let stdout = BufReader::new(child.stdout.take().expect("stdout"));
        let pipe = child.stderr.take().expect("stderr");
        let stderr = Arc::new(Mutex::new(String::new()));
        let sink = stderr.clone();
        std::thread::spawn(move || {
            let mut r = BufReader::new(pipe);
            let mut buf = String::new();
            let _ = r.read_to_string(&mut buf);
            *sink.lock().unwrap() = buf;
        });
        Self {
            child,
            stdout,
            stderr,
        }
    }

    /// **ingest 形态**：吸收自己的 WAL、持有热数据、对外提供热读，并**入册**。
    fn spawn_data_node(data_dir: &std::path::Path, meta: SocketAddr) -> Self {
        let child = Command::new(DATANODE)
            .arg("--instance-id")
            .arg(INSTANCE)
            .arg("--dir")
            .arg(data_dir)
            .arg("--listen")
            .arg("127.0.0.1:0")
            .arg("--meta")
            .arg(meta.to_string())
            .arg("--heartbeat-secs")
            .arg("1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("起 yuntun-datanode（ingest 形态）");
        Self::wrap(child)
    }

    /// **只查询形态**（`--no-ingest`）：不吃 WAL、不留本地数据，只作为协调者拉别人的热数据。
    ///
    /// 与数据节点**共用同一个 `--dir`** 是刻意的：只查询形态**不占任何租约**（它没有私有状态），
    /// 而 `<dir>/cold` 正是"同一份共享对象存储"在本机形态下的样子 —— 两个进程读同一份冷数据。
    fn spawn_query_only(data_dir: &std::path::Path, meta: SocketAddr) -> Self {
        let child = Command::new(DATANODE)
            .arg("--instance-id")
            .arg(QUERY_ONLY)
            .arg("--dir")
            .arg(data_dir)
            .arg("--no-ingest")
            .arg("--sql-listen")
            .arg("127.0.0.1:0")
            .arg("--meta")
            .arg(meta.to_string())
            .arg("--reconcile-secs")
            .arg("1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("起 yuntun-datanode（只查询形态）");
        Self::wrap(child)
    }

    /// 读 stdout 直到 `LISTEN <addr>`（= 它的**数据面**地址，名录里应当就是它）。
    fn wait_listening(&mut self) -> SocketAddr {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut line = String::new();
        loop {
            line.clear();
            let n = self.stdout.read_line(&mut line).expect("读子进程 stdout");
            if n == 0 {
                panic!(
                    "子进程未打印监听地址就退出了。stderr:\n{}",
                    self.stderr.lock().unwrap()
                );
            }
            if let Some(rest) = line.trim().strip_prefix("LISTEN ") {
                return rest.parse().expect("解析 LISTEN 地址");
            }
            assert!(Instant::now() < deadline, "等待监听地址超时");
        }
    }

    /// 读 stdout 直到 `SQL-LISTEN <addr>`（= 它的 **Flight SQL** 地址）。
    fn wait_sql_listening(&mut self) -> SocketAddr {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut line = String::new();
        loop {
            line.clear();
            let n = self.stdout.read_line(&mut line).expect("读子进程 stdout");
            if n == 0 {
                panic!(
                    "子进程未打印 SQL-LISTEN 就退出了。stderr:\n{}",
                    self.stderr.lock().unwrap()
                );
            }
            if let Some(rest) = line.trim().strip_prefix("SQL-LISTEN ") {
                return rest.parse().expect("解析 SQL-LISTEN 地址");
            }
            assert!(Instant::now() < deadline, "等待 SQL 监听地址超时");
        }
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// 名录（查询侧看它的那条路：`Prefetch` 载荷）。
fn roster(h: &yuntun_meta::NodeHandle) -> Vec<(String, String)> {
    let resp = h.prefetch(&pb::PrefetchRequest {
        since_schema_ver: 0,
        since_manifest_ver: 0,
        full: true,
        tables: Vec::new(),
        since_snapshot: 0,
    });
    resp.payload
        .map(|p| {
            p.datanodes
                .into_iter()
                .map(|m| (m.instance_id, m.address))
                .collect()
        })
        .unwrap_or_default()
}

async fn wait_until<F: Fn() -> bool>(f: F, within: Duration, what: &str, stderr: &Arc<Mutex<String>>) {
    let deadline = Instant::now() + within;
    while !f() {
        assert!(
            Instant::now() < deadline,
            "等待「{what}」超时。子进程 stderr:\n{}",
            stderr.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn flight_client(addr: SocketAddr) -> FlightServiceClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    FlightServiceClient::new(channel)
}

async fn collect_do_get(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    ticket: arrow_flight::Ticket,
) -> Result<Vec<i64>, String> {
    let mut stream = client
        .do_get(ticket)
        .await
        .map_err(|s| s.message().to_string())?
        .into_inner();
    let mut datas = Vec::new();
    while let Some(fd) = stream.next().await {
        datas.push(fd.map_err(|s| s.message().to_string())?);
    }
    let batches = arrow_flight::utils::flight_data_to_batches(&datas)
        .map_err(|e| format!("decode batches: {e}"))?;
    let mut out = Vec::new();
    for b in batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push(col.value(i));
        }
    }
    Ok(out)
}

/// 走标准轨发一条 SQL：`GetFlightInfo(CommandStatementQuery)` → `DoGet(ticket)`。
async fn sql(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    q: &str,
) -> Result<Vec<i64>, String> {
    let cmd = yuntun_server::flight::command_bytes(&CommandStatementQuery {
        query: q.to_string(),
        transaction_id: None,
    });
    let info = client
        .get_flight_info(arrow_flight::FlightDescriptor {
            r#type: 2,
            cmd: cmd.into(),
            path: vec![],
        })
        .await
        .map_err(|s| s.message().to_string())?
        .into_inner();
    let ticket = info
        .endpoint
        .first()
        .and_then(|e| e.ticket.clone())
        .ok_or_else(|| "GetFlightInfo 未返回 ticket".to_string())?;
    collect_do_get(client, ticket).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ingest_form_writes_and_query_form_reads_it_over_the_wire() {
    // ---- ① metanode（进程内 + 真 gRPC 服务）----
    let meta_dir = tmpdir("qd-meta");
    let node = yuntun_meta::MetaNode::open(&meta_dir, 1, vec![1], HashMap::new()).expect("起单节点");
    let h = node.handle();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let meta_addr = listener.local_addr().unwrap();
    let served = h.clone();
    tokio::spawn(async move {
        let _ = yuntun_meta::serve(served, listener).await;
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    while h.status().leader_id != 1 {
        assert!(Instant::now() < deadline, "等待选主超时");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // ---- ①b 只有客户端**连上**元数据面（不再由客户端建表）----
    //      建表这件事改由数据节点启动时**重放自己的 WAL DDL** 完成（见 `seed_wal`）：
    //      这正是要验的那条 —— 数据节点把表带进元数据面。
    let client_catalog =
        yuntun_meta::RemoteCatalog::connect(vec![meta_addr.to_string()]).expect("连 metanode");

    // ---- ② 数据节点：真子进程（预置 WAL ⇒ 它会回放出 3 行热数据）----
    let data_dir = tmpdir("qd-data");
    seed_wal(&data_dir, &[1, 2, 3]).await;
    let mut ingestor = Proc::spawn_data_node(&data_dir, meta_addr);
    let shard_addr = ingestor.wait_listening();

    // ---- ③ 名录里出现它，且**地址就是它打印的那个**（数据面地址这一环）----
    let h1 = h.clone();
    let st1 = ingestor.stderr.clone();
    let expect = shard_addr.to_string();
    wait_until(
        move || roster(&h1).contains(&(INSTANCE.to_string(), expect.clone())),
        Duration::from_secs(15),
        "数据节点入册（含数据面地址）",
        &st1,
    )
    .await;

    // ---- ③b 元数据面拿到了表：这就是 `replay_wal_ddl` 经 raft 写进去的 ----
    let t = client_catalog
        .get_table(TABLE)
        .await
        .expect("读表元数据");
    assert!(
        t.is_some(),
        "数据节点的 WAL DDL 必须重放进元数据面（否则查询节点根本不知道有这张表）"
    );

    // ---- ④ **只查询形态**的数据进程：真子进程（`--no-ingest`），启动即按名录接线热读器 ----
    let mut query_only = Proc::spawn_query_only(&data_dir, meta_addr);
    let qd_addr = query_only.wait_sql_listening();

    // ---- ⑤ 客户端发 SQL：数据在**另一个进程**里（只查询形态自己没有热数据） ----
    let mut client = flight_client(qd_addr).await;
    let got = sql(&mut client, &format!("SELECT a FROM yuntun.{TABLE} ORDER BY a"))
        .await
        .expect("查询应成功");
    assert_eq!(
        got,
        vec![1, 2, 3],
        "只查询的数据进程必须经数据面 gRPC 拉到另一个进程里的热数据"
    );

    // ---- ⑥ 只读：写入必须被**可读地拒绝**，而不是假装成功 ----
    let rejected = sql(
        &mut client,
        &format!("INSERT INTO yuntun.{TABLE} VALUES (9)"),
    )
    .await;
    assert!(
        rejected.is_err(),
        "SQL 面必须拒绝写入（本轮只读），实际：{rejected:?}"
    );
}
