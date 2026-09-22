//! R4/T12.1 第一刀：**真跨进程**的热读与跨进程对拍。
//!
//! 起点是 `§66`（进程内双实例对拍）与 `§67`（数据面 gRPC 往返）。本用例把两者合起来，
//! 把被替换掉的那一层从"函数调用"换成"**另一个操作系统进程**"：
//!
//! ```text
//!   yuntun-ingestor 进程 ── 私有 WAL → 吸收 → chunk store（热数据）
//!          │  gRPC（shard.proto）+ arrow IPC
//!   查询侧（本用例进程）：GrpcShardFetch → RemoteShard → QueryEngine
//! ```
//!
//! 因此它同时是**三条既有承诺的兑现**：
//!
//! 1. `§66.5` 说"跨进程形态可复用同一条对拍逻辑" —— 这里复用，判定逻辑一行没改；
//! 2. `§67.4` 说"真跨进程随 T12.1" —— 这里就是；
//! 3. `§28.2` 的 R-13 教训（同一私有目录同一时刻只有一个消费者）—— 由进程侧的
//!    `private_dir` 租约承担，本用例的两个进程各占**自己的**目录。
//!
//! **数据怎么进进程**：预置它的 WAL，进程启动后**回放**它 —— 这正是数据节点重启后的真实
//! 恢复路径（不是为测试特设的通道）。跨进程的"写入面"本身尚未定，见 `§68` 的边界说明。

use std::io::{BufRead, BufReader, Read};
use std::net::SocketAddr;
use std::path::Path;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_model::ops::CreateTableRequest;
use yuntun_model::wal_record::{DataPayload, Record};
use yuntun_query::{LocalCatalog, QueryEngine};
use yuntun_shardrpc::{GrpcShardFetch, encode_batch};
use yuntun_store::{RemoteShard, ShardReader, StoreConfig, create_store};
use yuntun_wal::config::WalConfig;
use yuntun_wal::writer::WalWriter;

/// 被测二进制（cargo 为集成测试注入的路径变量；名字 = 包的 `[[bin]]`）。
const BIN: &str = env!("CARGO_BIN_EXE_yuntun-ingestor");

const TABLE: &str = "public.proc";
const WINDOW: &str = "2026-09-21T10:00";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
}

fn batch(vals: &[i64]) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
}

/// 预置数据节点的 WAL：它启动后会**回放**这些 Data 记录（崩溃恢复路径）。
///
/// 用一行一批（而非一批多行）：与写入路径的粒度一致，也让"哪一行没读到"在断言里看得见。
async fn seed_wal(dir: &Path, rows: &[i64]) {
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

    for (i, v) in rows.iter().enumerate() {
        // 编码复用数据面的同一个函数：批次在系统里只有一种线上形态（WAL / spill / shard rpc 同源）
        let ipc = encode_batch(&batch(&[*v])).unwrap();
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

// ---------------------------------------------------------------- 子进程夹具

struct IngestorProc {
    child: Child,
    stdout: BufReader<ChildStdout>,
    stderr: Arc<Mutex<String>>,
}

impl IngestorProc {
    fn start(dir: &Path, instance_id: &str) -> Self {
        let mut cmd = Command::new(BIN);
        cmd.arg("--instance-id")
            .arg(instance_id)
            .arg("--dir")
            .arg(dir)
            // :0 → 内核分配端口，测试之间不会抢端口（真实地址由进程打印出来）
            .arg("--listen")
            .arg("127.0.0.1:0")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().expect("启动 yuntun-ingestor");

        let stdout = BufReader::new(child.stdout.take().expect("stdout"));
        let pipe = child.stderr.take().expect("stderr");
        // stderr 后台收集：断言失败时能把它打出来（否则只剩"提前退出"四个字）
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

    /// 读 stdout 直到出现 `LISTEN <addr>` —— 拿到的是**真实**监听地址。
    fn wait_listening(&mut self) -> SocketAddr {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut line = String::new();
        loop {
            line.clear();
            let n = self.stdout.read_line(&mut line).expect("读子进程 stdout");
            if n == 0 {
                panic!(
                    "yuntun-ingestor 未打印监听地址就退出了。stderr:\n{}",
                    self.stderr.lock().unwrap()
                );
            }
            if let Some(rest) = line.trim().strip_prefix("LISTEN ") {
                return rest.parse().expect("解析 LISTEN 地址");
            }
            assert!(
                Instant::now() < deadline,
                "等待监听地址超时。stderr:\n{}",
                self.stderr.lock().unwrap()
            );
        }
    }
}

impl Drop for IngestorProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------- 查询侧

async fn remote_reader(addr: SocketAddr) -> Arc<dyn ShardReader> {
    Arc::new(RemoteShard::new(Arc::new(
        GrpcShardFetch::connect(&addr.to_string()).await.unwrap(),
    )))
}

/// 等热数据"长出来"：数据节点要先回放自己的 WAL（攒批扫描间隔是 100ms 量级）。
async fn wait_rows(reader: &Arc<dyn ShardReader>, expect: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let n = reader.read_table(TABLE, 0).await.unwrap().rows();
        if n == expect {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "等待 {expect} 行超时（当前 {n} 行）"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// 装配"若干实例 + 查询引擎"，返回排序后的结果（与 `§66` 的对拍**同一条判定逻辑**）。
async fn engine_with(instances: Vec<(&str, Arc<dyn ShardReader>)>) -> Vec<i64> {
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    catalog
        .create_table(CreateTableRequest {
            name: "proc".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: Default::default(),
        })
        .await
        .unwrap();

    let cache = Arc::new(LocalCatalog::new());
    cache.set_catalog_ops(catalog.clone());
    for (id, reader) in instances {
        cache.set_hot_shards(id, reader);
    }
    cache.refresh(&catalog).await.unwrap();

    let engine = QueryEngine::new(create_store(&StoreConfig::Memory).unwrap(), cache);
    let batches = engine
        .sql(&format!("SELECT a FROM yuntun.{TABLE} ORDER BY a"))
        .await
        .expect("查询应成功");
    let mut out = Vec::new();
    for b in batches {
        let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push(col.value(i));
        }
    }
    out
}

/// ① 单个数据节点：热数据必须**跨进程**长大、并被查询侧读到。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_rows_cross_a_real_process_boundary() {
    let dir = yuntun_testkit::TestDir::tmpfs("ingestor-solo");
    seed_wal(dir.path(), &[1, 2, 3]).await;

    let mut proc = IngestorProc::start(dir.path(), "solo");
    let addr = proc.wait_listening();
    let reader = remote_reader(addr).await;

    // 数据节点自己回放 WAL ⇒ 热数据出现在**另一个进程**里
    wait_rows(&reader, 3).await;

    let got = engine_with(vec![("solo", reader.clone())]).await;
    assert_eq!(
        got,
        vec![1, 2, 3],
        "查询侧必须能读到数据节点进程里的热数据（这正是 T12.1 要拆出来的那条路径）"
    );
}

/// ② R4 准出判据的**跨进程形态**：多数据节点并发写下 vs 单节点串行，逐行相等。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parity_holds_across_processes() {
    let all: Vec<i64> = (1..=6).collect();
    let (left, right) = all.split_at(3);

    // ① 单节点串行：一个进程持有全部 6 行
    let single = {
        let dir = yuntun_testkit::TestDir::tmpfs("proc-single");
        seed_wal(dir.path(), &all).await;
        let mut proc = IngestorProc::start(dir.path(), "solo");
        let addr = proc.wait_listening();
        let reader = remote_reader(addr).await;
        wait_rows(&reader, 6).await;
        engine_with(vec![("solo", reader.clone())]).await
    };

    // ② 两个数据节点各写一半（**同名 shard**：设计上多 datanode 可同时写同一 partition）
    let dir_a = yuntun_testkit::TestDir::tmpfs("proc-a");
    let dir_b = yuntun_testkit::TestDir::tmpfs("proc-b");
    seed_wal(dir_a.path(), left).await;
    seed_wal(dir_b.path(), right).await;
    let mut proc_a = IngestorProc::start(dir_a.path(), "inst-a");
    let mut proc_b = IngestorProc::start(dir_b.path(), "inst-b");
    let (addr_a, addr_b) = (proc_a.wait_listening(), proc_b.wait_listening());
    let (ra, rb) = (remote_reader(addr_a).await, remote_reader(addr_b).await);
    wait_rows(&ra, 3).await;
    wait_rows(&rb, 3).await;

    // 反证：只注册一个数据节点时**只该看到它自己那 3 行**（否则主断言可能是空转）
    let only_a = engine_with(vec![("inst-a", ra.clone())]).await;
    assert_eq!(
        only_a,
        left.to_vec(),
        "只注册 inst-a 却看到别的节点的数据 ⇒ 热读归属错了（`§65` 的按实例切分失效）"
    );

    let split = engine_with(vec![("inst-a", ra.clone()), ("inst-b", rb.clone())]).await;
    assert_eq!(single, all, "单节点（跨进程）应读到全部 6 行");
    assert_eq!(
        split, all,
        "跨进程对拍必须成立：少一行 = 漏读某个数据节点；多一行 = 同一份数据读两次"
    );
}

/// 顺带把"同一私有目录只能有一个消费者"在**进程形态**下验一遍：
/// 第二个进程必须**启动即拒**，且拒之前不能碰盘（`§62` / R-13）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_process_on_the_same_private_dir_is_rejected() {
    let dir = yuntun_testkit::TestDir::tmpfs("proc-lease");
    seed_wal(dir.path(), &[1]).await;

    let mut first = IngestorProc::start(dir.path(), "inst-a");
    let _addr = first.wait_listening();

    // 同一个目录、同一个 instance_id，起第二个
    let out = Command::new(BIN)
        .arg("--instance-id")
        .arg("inst-a")
        .arg("--dir")
        .arg(dir.string())
        .arg("--listen")
        .arg("127.0.0.1:0")
        .output()
        .expect("跑第二个 yuntun-ingestor");
    assert!(
        !out.status.success(),
        "同一私有目录的第二个消费者必须被拒（否则 R-13 那类重复持久化会重演）"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stderr.contains("instance_id") || stdout.contains("instance_id"),
        "拒绝信息应点名 instance_id（便于定位是谁占着）：\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    // 它不该已经开始服务
    assert!(
        !stdout.contains("LISTEN "),
        "被拒的进程不该打印监听地址（它连 WAL 都不该打开）"
    );
}

