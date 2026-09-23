//! R4 **T12.1 第三刀**：**压缩节点进程**端到端 —— 共享存储里的多个文件被合成一个。
//!
//! ```text
//!   ① metanode（进程内，真 gRPC 服务）
//!        ▲ commit_files（3 个真文件）        ▲ commit_compaction（合并结果）
//!   测试进程（客户端）                    ③ yuntun-compactord 子进程
//!        └──── ② 共享冷目录（真 parquet 文件）────┘
//! ```
//!
//! 三个角色都是真的：目录走真 raft、文件是真 parquet、合并由**独立进程**完成。
//! 观察点全在**目录**上（可见文件数、行数）—— 而不是压缩进程内部状态：
//! "合并到底有没有发生并且被提交"，只有从目录看得见才算数。
//!
//! 这一刀同时验证了压缩节点的**正交性**：它没有 WAL、没有 chunk、不知道有数据节点，
//! 只读共享存储 + 提交一条 op —— 于是"压缩"从某个节点上的副作用变成一个可独立扩缩的角色。

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::net::TcpListener;

use yuntun_catalog::CatalogOps as _;
use yuntun_model::meta::FileManifest;
use yuntun_model::ops::{CreateTableRequest, DEFAULT_SCHEMA};
use yuntun_store::{StoreConfig, create_store};

const COMPACTORD: &str = env!("CARGO_BIN_EXE_yuntun-compactord");
const TABLE: &str = "public.cq";
const SHARD: &str = "s0";
const WINDOW: &str = "2026-09-23T10:00";
/// 每个文件的行数（3 个文件 ⇒ 合并后必须是 9 行）
const ROWS_PER_FILE: i64 = 3;
const FILES: usize = 3;

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

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
}

fn batch(base: i64) -> RecordBatch {
    let vals: Vec<i64> = (0..ROWS_PER_FILE).map(|i| base + i).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals))]).unwrap()
}

/// 压缩节点子进程（读 stdout 的 `READY ...`；stderr 后台收集）。
struct Proc {
    child: Child,
    stdout: BufReader<ChildStdout>,
    stderr: Arc<Mutex<String>>,
}

impl Proc {
    fn start(meta: SocketAddr, cold_root: &std::path::Path) -> Self {
        let mut child = Command::new(COMPACTORD)
            .arg("--meta")
            .arg(meta.to_string())
            .arg("--cold-root")
            .arg(cold_root)
            .arg("--min-files")
            .arg(FILES.to_string())
            // 作业间隔压到 1s：用例不必等一分钟
            .arg("--interval-secs")
            .arg("1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("起 yuntun-compactord");

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

    /// 读 stdout 直到 `READY ...`（= 装配完成、循环已起）。
    fn wait_ready(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut line = String::new();
        loop {
            line.clear();
            let n = self.stdout.read_line(&mut line).expect("读子进程 stdout");
            if n == 0 {
                panic!(
                    "压缩节点未打印 READY 就退出了。stderr:\n{}",
                    self.stderr.lock().unwrap()
                );
            }
            if line.trim_start().starts_with("READY ") {
                return;
            }
            assert!(Instant::now() < deadline, "等待 READY 超时");
        }
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// 等待某个**异步**条件成立（读目录本身要 `await`，所以不能用同步闭包）。
async fn wait_for<F, Fut>(mut f: F, within: Duration, what: &str, stderr: &Arc<Mutex<String>>)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + within;
    loop {
        if f().await {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "等待「{what}」超时。压缩节点 stderr:\n{}",
            stderr.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compactord_merges_files_from_shared_storage_and_commits_via_raft() {
    // ---- ① metanode（进程内 + 真 gRPC 服务）----
    let meta_dir = tmpdir("compact-meta");
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

    // ---- ② 共享冷目录：建表 + 写 3 个**真文件**并提交（走客户端那条路）----
    let cold_root = tmpdir("compact-cold");
    let store = create_store(&StoreConfig::Local {
        root: cold_root.to_string_lossy().into_owned(),
    })
    .expect("开共享存储");
    let client = Arc::new(
        yuntun_meta::RemoteCatalog::connect(vec![meta_addr.to_string()]).expect("连 metanode"),
    );

    client
        .create_table(CreateTableRequest {
            name: "cq".into(),
            namespace: DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: Default::default(),
        })
        .await
        .expect("建表");

    for i in 0..FILES {
        let batch_id = format!("batch-{i}");
        let (path, size, rows) = yuntun_format::write_batch(
            &store,
            TABLE,
            SHARD,
            WINDOW,
            &batch_id,
            &batch(i as i64 * ROWS_PER_FILE),
            yuntun_format::DataFormat::parse("parquet"),
        )
        .await
        .expect("写数据文件");
        assert_eq!(rows, ROWS_PER_FILE as u64);
        client
            .commit_files(yuntun_model::ops::CommitFilesRequest {
                table: TABLE.into(),
                batch_id: batch_id.clone(),
                client_request_id: None,
                client_request_ids: vec![],
                shard: SHARD.into(),
                time_window: WINDOW.into(),
                files: vec![FileManifest {
                    file_path: path,
                    batch_id: batch_id.clone(),
                    file_size: size,
                    row_count: rows,
                    ..Default::default()
                }],
                schema_version: 1,
                row_count: rows,
            })
            .await
            .expect("提交文件");
    }

    let snap = client.current_snapshot().await;
    let visible = client
        .list_visible_files(TABLE, snap, Some(SHARD))
        .await
        .expect("读可见文件");
    assert_eq!(visible.len(), FILES, "合并前应当是 {FILES} 个可见文件");

    // ---- ③ 起压缩节点子进程（同一个共享目录 + 同一个元数据面）----
    let mut compactor = Proc::start(meta_addr, &cold_root);
    compactor.wait_ready();

    // ---- ④ 合并发生并被**提交**：目录里只剩 1 个可见文件，且行数是总和 ----
    let waiter = client.clone();
    wait_for(
        move || {
            let c = waiter.clone();
            async move {
                let snap = c.current_snapshot().await;
                c.list_visible_files(TABLE, snap, Some(SHARD))
                    .await
                    .map(|f| f.len() == 1)
                    .unwrap_or(false)
            }
        },
        Duration::from_secs(60),
        "合并结果被提交（可见文件 3 → 1）",
        &compactor.stderr,
    )
    .await;

    let snap = client.current_snapshot().await;
    let merged = client
        .list_visible_files(TABLE, snap, Some(SHARD))
        .await
        .expect("读合并后的可见文件");
    assert_eq!(merged.len(), 1, "合并后应当只剩 1 个可见文件");
    assert_eq!(
        merged[0].row_count,
        FILES as u64 * ROWS_PER_FILE as u64,
        "合并产物的行数必须是输入之和 —— 合并最经典的 bug 就是丢行"
    );
}
