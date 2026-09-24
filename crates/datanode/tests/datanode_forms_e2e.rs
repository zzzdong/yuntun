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
//! - 只读：SQL 面对写入给出**可读的拒绝**，而不是假装成功；
//! - `§98`：两个**可写**写进程并发真写（都开 `--sql-listen`）⇒ 各自查询与单节点串行逐行相等。

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, FlightDescriptor};
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

    /// **写进程**（可指定实例名 + 共享冷目录）：与 [`Self::spawn_data_node`] 同一形态，
    /// 只是把两个多写者必需的东西开出来（`plan §8.3` 的 M4 要用）。
    ///
    /// - `--instance-id` 必须**各不相同**：`source_instance` 是"这条热数据属于谁"的唯一标识；
    /// - `--dir` 必须**各不相同**：WAL / spill 是私有的，同一目录第二个消费者会被目录租约拒；
    /// - `--cold-root` 必须**相同**：冷存储是共享的那一份（本机形态；真实部署里它是 S3）。
    fn spawn_writer(
        data_dir: &std::path::Path,
        meta: SocketAddr,
        instance_id: &str,
        cold_root: &std::path::Path,
    ) -> Self {
        Self::spawn_writer_impl(data_dir, meta, instance_id, cold_root, false)
    }

    /// 与 [`Self::spawn_writer`] 同形态，但**同时开 SQL 面**（`--sql-listen 127.0.0.1:0`）。
    ///
    /// 这是 M4 最后那格（`operation-log §98`）的要件："数据节点 + 协调者"本就是
    /// **同一进程的两个面**（`architecture §4.2`）—— 两个写者要**既写又被查**，
    /// 所以写入面（`§96` 的 `FlightServer::new`）与查询面都得开。
    /// `--reconcile-secs 1` 让"看得见对方"更快稳定（数据面靠名录 + 巡检发现）。
    fn spawn_writer_with_sql(
        data_dir: &std::path::Path,
        meta: SocketAddr,
        instance_id: &str,
        cold_root: &std::path::Path,
    ) -> Self {
        Self::spawn_writer_impl(data_dir, meta, instance_id, cold_root, true)
    }

    fn spawn_writer_impl(
        data_dir: &std::path::Path,
        meta: SocketAddr,
        instance_id: &str,
        cold_root: &std::path::Path,
        sql: bool,
    ) -> Self {
        let mut cmd = Command::new(DATANODE);
        cmd.arg("--instance-id")
            .arg(instance_id)
            .arg("--dir")
            .arg(data_dir)
            .arg("--cold-root")
            .arg(cold_root)
            .arg("--listen")
            .arg("127.0.0.1:0")
            .arg("--meta")
            .arg(meta.to_string())
            .arg("--heartbeat-secs")
            .arg("1");
        if sql {
            cmd.arg("--sql-listen")
                .arg("127.0.0.1:0")
                .arg("--reconcile-secs")
                .arg("1");
        }
        let child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("起 yuntun-datanode（写进程）");
        Self::wrap(child)
    }

    /// **只查询形态**（`--no-ingest`）：不吃 WAL、不留本地数据，只作为协调者拉别人的热数据。
    ///
    /// 与数据节点**共用同一个 `--dir`** 是刻意的：只查询形态**不占任何租约**（它没有私有状态），
    /// 而 `<dir>/cold` 正是"同一份共享对象存储"在本机形态下的样子 —— 两个进程读同一份冷数据。
    fn spawn_query_only(data_dir: &std::path::Path, meta: SocketAddr) -> Self {
        Self::spawn_query_only_with_cold(data_dir, meta, None)
    }

    /// 同 [`Self::spawn_query_only`]，但可指定**共享冷存储**（多写者场景必需：
    /// 协调者必须看得见两个写者的落盘文件）。
    fn spawn_query_only_with_cold(
        data_dir: &std::path::Path,
        meta: SocketAddr,
        cold_root: Option<&std::path::Path>,
    ) -> Self {
        let child = Command::new(DATANODE)
            .arg("--instance-id")
            .arg(QUERY_ONLY)
            .arg("--dir")
            .arg(data_dir)
            .args(
                cold_root
                    .map(|c| vec!["--cold-root".to_string(), c.to_string_lossy().into_owned()])
                    .unwrap_or_default(),
            )
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

// ---------------------------------------------------------------------------
// M4 门槛的字面要求：**多** datanode 并发写
// ---------------------------------------------------------------------------

/// **两个写进程并发写同一张表、同一个 shard** ⇒ 查询结果必须与单节点串行**逐行精确相等**。
///
/// 为什么单独一条：`plan §8.3` 的 M4 门槛写的是"**多** datanode 并发写 + 查询"，
/// 而现有跨进程用例都是**单写者**（`ingest_form_writes_and_query_form_reads_it_over_the_wire`
/// 是"一写一查"）。这一格没实证之前，`§8.4` 的"分布式就绪"就还不能说。
///
/// 形态（`architecture §5.1` 的原话："多个 datanode 可能同时写同一 partition，各自出各自的
/// 文件"）—— 三个真子进程 + 一个进程内 metanode：
///
/// ```text
///   metanode（进程内，真 gRPC）
///     ▲ 注册/心跳（两个**不同**实例名）        ▲ 名录（含各自数据面地址）
///   datanode A ──┐                        ┌── datanode B
///   --dir A      ├── 同一个 --cold-root ───┤   --dir B
///   WAL: 1,2,3   ┘   （= 共享对象存储）    └── WAL: 4,5,6
///                     ▲
///           只查询进程（--no-ingest --sql-listen）→ SELECT ⇒ [1,2,3,4,5,6]
/// ```
///
/// **两个 `--dir` 必须不同、一个 `--cold-root` 必须相同**，这不是偷懒：
/// - 不同 `--dir`：WAL/spill 是私有的，同一目录第二个消费者会被目录租约当场拒绝（`T12.4`）；
/// - 相同 `--cold-root`：冷存储是共享的那一份（`--cold-root` 的文档原话就是
///   "各给一个 `--dir`，但指同一个 `--cold-root`"）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_writers_parity_with_single_node_serial() {
    // ---- ① metanode（进程内 + 真 gRPC 服务）----
    let meta_dir = tmpdir("mw-meta");
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
    let client_catalog =
        yuntun_meta::RemoteCatalog::connect(vec![meta_addr.to_string()]).expect("连 metanode");

    // ---- ② 两个写进程：私有 `--dir` + **共享 `--cold-root`** ----
    let cold = tmpdir("mw-cold");
    let dir_a = tmpdir("mw-a");
    let dir_b = tmpdir("mw-b");
    seed_wal(&dir_a, &[1, 2, 3]).await;
    seed_wal(&dir_b, &[4, 5, 6]).await;
    let mut a = Proc::spawn_writer(&dir_a, meta_addr, "inst-a", &cold);
    let mut b = Proc::spawn_writer(&dir_b, meta_addr, "inst-b", &cold);
    let addr_a = a.wait_listening();
    let addr_b = b.wait_listening();

    // ---- ③ 两个实例都入册（各自的数据面地址都在名录里）----
    for (inst, addr) in [("inst-a", addr_a), ("inst-b", addr_b)] {
        let h1 = h.clone();
        let st = if inst == "inst-a" {
            a.stderr.clone()
        } else {
            b.stderr.clone()
        };
        let want = (inst.to_string(), addr.to_string());
        wait_until(
            move || roster(&h1).contains(&want),
            Duration::from_secs(15),
            "写进程入册（含数据面地址）",
            &st,
        )
        .await;
    }

    // ---- ④ 表已进元数据面（两个节点的 WAL DDL 都重放过；重复的那次应被容忍）----
    let deadline = Instant::now() + Duration::from_secs(15);
    while client_catalog.get_table(TABLE).await.expect("读表元数据").is_none() {
        assert!(Instant::now() < deadline, "两个写进程的 WAL DDL 应把表带进元数据面");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // ---- ⑤ 只查询进程（协调者）：看得见两个写者的数据 ----
    let mut q = Proc::spawn_query_only_with_cold(&dir_a, meta_addr, Some(&cold));
    let qd_addr = q.wait_sql_listening();
    let mut client = flight_client(qd_addr).await;
    let got = sql(&mut client, &format!("SELECT a FROM yuntun.{TABLE} ORDER BY a"))
        .await
        .expect("查询应成功");
    assert_eq!(
        got,
        vec![1, 2, 3, 4, 5, 6],
        "两个写进程各写一半 ⇒ 必须与单节点串行**逐行精确相等**：\
         少行 = 漏读某个实例；多行 = 同一份数据被读两次"
    );

    // 收尾：显式停掉三个子进程（不留孤儿）
    let _ = a.child.kill();
    let _ = b.child.kill();
    let _ = q.child.kill();
}

// ---------------------------------------------------------------------------
// M4 最后一格：两个写进程**同时真写**（`§91.5` 的第二半，`operation-log §98`）
// ---------------------------------------------------------------------------

/// 单行 `RecordBatch`（列 `a`，与文件常量 `schema()` 配套）。
fn one_row(value: i64) -> arrow::record_batch::RecordBatch {
    arrow::record_batch::RecordBatch::try_new(
        schema(),
        vec![Arc::new(arrow::array::Int64Array::from(vec![value]))],
    )
    .expect("构造单行 batch")
}

/// **简易轨写入一行**（`flight_sql_e2e.rs` 同法）：schema 消息带
/// `FlightDescriptor{r#type:1, path=[table, shard]}`，数据消息带 `{"idempotency_key": key}`。
///
/// 与 [`seed_wal`] 的区别是**关键**：那条是把记录预先摆进 WAL，本函数让数据进程**通过
/// 自己的 SQL 面真的收到写入** —— 正是 `§96` 补上的那个面（`FlightServer::new`）。
async fn insert_row(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    table: &str,
    shard: &str,
    value: i64,
    key: &str,
) {
    let mut msgs =
        arrow_flight::utils::batches_to_flight_data(schema().as_ref(), vec![one_row(value)])
            .expect("flight msgs");
    let mut data = msgs.remove(1);
    data.app_metadata = format!(r#"{{"idempotency_key":"{key}"}}"#)
        .into_bytes()
        .into();
    let schema_msg = FlightData {
        flight_descriptor: Some(FlightDescriptor {
            r#type: 1,
            path: vec![table.into(), shard.into()],
            cmd: Default::default(),
        }),
        ..msgs.remove(0)
    };
    // 用本 crate 已有的 `futures`（不额外引 `tokio-stream`）：tonic 对**任意**
    // `Stream + Send + 'static` 都实现 `IntoStreamingRequest`，与 `tokio_stream::iter` 等价。
    let mut acks = client
        .do_put(futures::stream::iter(vec![schema_msg, data]))
        .await
        .expect("do_put")
        .into_inner();
    acks.next()
        .await
        .expect("DoPut 必须回执")
        .expect("DoPut 回执必须是 Ok（写入必须真的落进 WAL）");
}

/// 轮询直到查询结果**逐行精确等于** `expect`。
///
/// 需要轮询而不是睡固定时长，是因为"写进去了"与"查得到"之间隔着两段**异步传播**：
/// ① 写 WAL → 一个扫描周期后进 chunk（可见性上界，`architecture §5.2`，不等 flush）；
/// ② 名录巡检发现对方 → 数据面 gRPC 拉热数据。
///
/// 超时仍不等 ⇒ 带**最后一次实际结果**失败：把"少行（漏读某实例）/ 多行（同一份读两次）"
/// 直接摆进断言消息，而不是只说一句"超时"。
async fn wait_rows(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    q: &str,
    expect: &[i64],
    what: &str,
    stderr: &Arc<Mutex<String>>,
) -> Vec<i64> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let last = sql(client, q).await;
        if matches!(&last, Ok(v) if v.as_slice() == expect) {
            return expect.to_vec();
        }
        if Instant::now() >= deadline {
            panic!(
                "等待「{what}」得到 {expect:?} 超时；最后一次实际结果：{last:?}\n子进程 stderr:\n{}",
                stderr.lock().unwrap()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// **两个写进程同时真写**同一张表、同一个 shard ⇒ 各自查询都必须与单节点串行
/// **逐行精确相等**。
///
/// 这条与 [`two_writers_parity_with_single_node_serial`]（`§91`）**不是重复**：
/// - `§91` 的两个写者各自**回放自己的 WAL**（数据在启动前就摆好）⇒ 证的是元数据/合并层面的
///   "多实例不重不漏"；
/// - 本条的两个写者**都开 `--sql-listen`、并发 `INSERT`（真 `DoPut`）** ⇒ 数据是这一瞬间
///   **真的写进去的**，是 `§91.5` 的第二半，也是 `§96` 补上"接受写入的面"之后的**首条**用例。
///
/// 形态（两个写者**各写各的 WAL/私有目录、共用一份冷存储**；两边**都**是"数据节点 + 协调者"）：
///
/// ```text
///   metanode（进程内，真 gRPC）
///     ▲ 注册/心跳（inst-a / inst-b）        ▲ 名录（含各自数据面地址）
///   datanode A（--sql-listen）──┐      ┌── datanode B（--sql-listen）
///   --dir A / WAL 只有 DDL      ├─同一──┤   --dir B / WAL 只有 DDL
///   并发真写 1,2,3 ────────────┘ 冷存储 └─────────── 并发真写 4,5,6
///                     ▲                         ▲
///              A 查一次 [1..6]           B 查一次 [1..6]
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn two_concurrent_writers_real_ingest_parity() {
    // ---- ① metanode（进程内 + 真 gRPC 服务）----
    let meta_dir = tmpdir("cm-meta");
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
    let client_catalog =
        yuntun_meta::RemoteCatalog::connect(vec![meta_addr.to_string()]).expect("连 metanode");

    // ---- ② 两个**可写**写进程：私有 `--dir` + 共享 `--cold-root` + 各自 SQL 面 ----
    //      WAL **只预置 DDL**（空行切片）⇒ 表进元数据面，数据一行都不预置 —— 全靠真写入。
    let cold = tmpdir("cm-cold");
    let dir_a = tmpdir("cm-a");
    let dir_b = tmpdir("cm-b");
    seed_wal(&dir_a, &[]).await;
    seed_wal(&dir_b, &[]).await;
    let mut a = Proc::spawn_writer_with_sql(&dir_a, meta_addr, "inst-a", &cold);
    let mut b = Proc::spawn_writer_with_sql(&dir_b, meta_addr, "inst-b", &cold);
    let addr_a = a.wait_listening();
    let addr_b = b.wait_listening();
    let sql_a = a.wait_sql_listening();
    let sql_b = b.wait_sql_listening();

    // ---- ③ 两个实例都入册（各自的数据面地址都在名录里）----
    for (inst, addr, st) in [
        ("inst-a", addr_a, a.stderr.clone()),
        ("inst-b", addr_b, b.stderr.clone()),
    ] {
        let h1 = h.clone();
        let want = (inst.to_string(), addr.to_string());
        wait_until(
            move || roster(&h1).contains(&want),
            Duration::from_secs(15),
            "写进程入册（含数据面地址）",
            &st,
        )
        .await;
    }

    // ---- ④ 表已进元数据面（两个节点的 WAL DDL 都重放过；重复的那次应被容忍）----
    let deadline = Instant::now() + Duration::from_secs(15);
    while client_catalog.get_table(TABLE).await.expect("读表元数据").is_none() {
        assert!(Instant::now() < deadline, "两个写进程的 WAL DDL 应把表带进元数据面");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // ---- ⑤ **并发真写**：两个 `tokio::spawn` 各连**自己的** SQL 面，各写 3 行 ----
    //      值域不重叠 ⇒ 少行/多行都能一眼看出是谁的问题。
    let ta = tokio::spawn(async move {
        let mut c = flight_client(sql_a).await;
        for v in [1, 2, 3] {
            insert_row(&mut c, TABLE, "default", v, &format!("cm-a-{v}")).await;
        }
    });
    let tb = tokio::spawn(async move {
        let mut c = flight_client(sql_b).await;
        for v in [4, 5, 6] {
            insert_row(&mut c, TABLE, "default", v, &format!("cm-b-{v}")).await;
        }
    });
    ta.await.expect("写任务 A");
    tb.await.expect("写任务 B");

    // ---- ⑥ 对拍：两边各自查一次，都必须得到**全部 6 行**、每行只出一次 ----
    let expect = vec![1, 2, 3, 4, 5, 6];
    let q = format!("SELECT a FROM yuntun.{TABLE} ORDER BY a");
    let mut qa = flight_client(sql_a).await;
    let mut qb = flight_client(sql_b).await;
    let got_a = wait_rows(
        &mut qa,
        &q,
        &expect,
        "节点 A 的查询（自己 1..3 + 对方 4..6）",
        &a.stderr,
    )
    .await;
    let got_b = wait_rows(
        &mut qb,
        &q,
        &expect,
        "节点 B 的查询（自己 4..6 + 对方 1..3）",
        &b.stderr,
    )
    .await;
    assert_eq!(
        got_a, expect,
        "A 必须看到两个写者的全部 6 行（少行 = 漏读对方；多行 = 同一份被读两次）"
    );
    assert_eq!(
        got_b, expect,
        "B 必须看到两个写者的全部 6 行（少行 = 漏读对方；多行 = 同一份被读两次）"
    );

    // ---- ⑦ 反证：杀掉 B ⇒ A 只剩 [1,2,3] ----
    //      钉住"⑥ 里那 3 行确实经数据面从 B 拉来"，而不是 A 不知怎么自己就有了 6 行。
    //      杀的时刻远早于 B 的首次 flush（最早 = chunk 创建 + `min_resident` 默认 5s），
    //      所以 B 的数据只可能在它自己那**已死的热 chunk** 里 —— 不会因"已落盘共享冷存储"
    //      而仍然可见。（A 侧的降级/丢源由 `--partial allow` 默认兜住，结果就是 3 行。）
    let _ = b.child.kill();
    let got_after = wait_rows(
        &mut qa,
        &q,
        &[1, 2, 3],
        "节点 A 在 B 死后的查询（只应剩自己写的 3 行）",
        &a.stderr,
    )
    .await;
    assert_eq!(
        got_after,
        vec![1, 2, 3],
        "B 死后 A 仍能看到 4,5,6 ⇒ 那 3 行不是从 B 拉的（反证失败）"
    );

    // 收尾：显式停掉两个子进程（不留孤儿；`Drop` 会再兜一次，无害）
    let _ = a.child.kill();
}

// ---------------------------------------------------------------------------
// 冷存储形态的**启动校验**（`operation-log §102`：数据进程接 S3）
// ---------------------------------------------------------------------------

/// 两种冷存储配置错误必须**起不来**，而不是跑成一个语义含糊的进程：
///
/// ① 只给 `--s3-bucket`、不给 `--s3-endpoint` —— 那会以"连不上 AWS"收场（含糊）；
/// ② 同时给 `--cold-root` 与 `--s3-bucket` —— 冷存储只能有一个根，混着给说明没想清，
///    后果是"以为在写 S3，其实在写本地"（`§101` 那类"数据落哪了"的误判）。
///
/// 照 metanode 的 `process_refuses_*`：断言**退出码**与**点名的错误**，不只断言"失败"。
#[test]
fn s3_cold_store_misconfiguration_is_refused() {
    let dir = tmpdir("dn-s3-badcfg");

    // ① 缺 endpoint
    let out = Command::new(DATANODE)
        .args(["--instance-id", "inst-a", "--dir"])
        .arg(&dir)
        .args(["--s3-bucket", "b"])
        .output()
        .expect("跑二进制");
    assert_eq!(out.status.code(), Some(1), "配置错应以非零退出");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("必须一起给"),
        "错误要点名缺的是 endpoint：{err}"
    );

    // ② 冷存储两个根
    let out = Command::new(DATANODE)
        .args(["--instance-id", "inst-a", "--dir"])
        .arg(&dir)
        .arg("--cold-root")
        .arg(dir.join("cold"))
        .args(["--s3-bucket", "b", "--s3-endpoint", "http://127.0.0.1:8333"])
        .output()
        .expect("跑二进制");
    assert_eq!(out.status.code(), Some(1), "配置错应以非零退出");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("互斥"), "错误要点名冲突项：{err}");

    // 反证：S3 形态**不**在本地建 `cold/` —— 建了就会让人以为数据落在本地
    assert!(
        !dir.join("cold").exists(),
        "S3 形态下不该创建本地 cold/ 目录"
    );
}
