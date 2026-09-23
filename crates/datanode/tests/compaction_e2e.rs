//! **压缩是数据进程的第二职能**（`operation-log §79` 的角色更正）。
//!
//! 这条用例存在的理由很具体：按"**meta + data**"两类角色部署时，压缩作业一度**没有任何载体**
//! （它只活在 `standalone` 的装配里，或被单开成一个 `compactord` 进程）。而
//! `plan.md §7.5` T14.1 明说它今天是"**单进程后台任务**"、R6 才升格为带 meta 租约的全局作业 ——
//! 所以它该落在**数据进程**里，用 `--compaction` 显式打开。
//!
//! ```text
//!   ① metanode（进程内，真 gRPC 服务）
//!        ▲ commit_files（3 个真文件）      ▲ commit_compaction（合并结果）
//!   测试进程（客户端）                    ③ yuntun-datanode 子进程（--compaction）
//!        └──── ② 共享冷目录 <dir>/cold（真 parquet 文件）────┘
//! ```
//!
//! 观察点全在**目录**上（可见文件数、行数）—— "合并有没有发生、有没有被提交"，
//! 只有从目录看得见才算数。

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

const DATANODE: &str = env!("CARGO_BIN_EXE_yuntun-datanode");
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

/// 数据进程子进程（读 stdout 的 `LISTEN ...`；stderr 后台收集）。
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
        Self { child, stdout, stderr }
    }

    fn start(meta: SocketAddr, dir: &std::path::Path) -> Self {
        let mut child = Command::new(DATANODE)
            .arg("--instance-id")
            .arg("d1")
            .arg("--dir")
            .arg(dir)
            .arg("--meta")
            .arg(meta.to_string())
            .arg("--compaction")
            .arg("--compaction-min-files")
            .arg(FILES.to_string())
            // 作业间隔压到 1s：用例不必等一分钟
            .arg("--compaction-interval-secs")
            .arg("1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("起 yuntun-datanode");

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

    /// 多节点形态：指定实例名、私有目录、**共享冷根**与租约期限。
    ///
    /// 私有目录必须**各归各的**（WAL/spill 被租约独占），而冷根必须**同一份**
    /// —— 这正是"共享对象存储 + 私有状态"在一台机器上的样子。
    fn start_node(
        meta: SocketAddr,
        dir: &std::path::Path,
        id: &str,
        cold_root: &std::path::Path,
        ttl_secs: u64,
    ) -> Self {
        let child = Command::new(DATANODE)
            .arg("--instance-id")
            .arg(id)
            .arg("--dir")
            .arg(dir)
            .arg("--cold-root")
            .arg(cold_root)
            .arg("--meta")
            .arg(meta.to_string())
            .arg("--compaction")
            .arg("--compaction-min-files")
            .arg(FILES.to_string())
            .arg("--compaction-interval-secs")
            .arg("1")
            .arg("--compaction-ttl-secs")
            .arg(ttl_secs.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("起 yuntun-datanode（多节点形态）");
        Self::wrap(child)
    }

    /// 读 stdout 直到 `LISTEN ...`（= 装配完成、开始服务）。
    fn wait_listen(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut line = String::new();
        loop {
            line.clear();
            let n = self.stdout.read_line(&mut line).expect("读子进程 stdout");
            if n == 0 {
                panic!(
                    "数据进程未打印 LISTEN 就退出了。stderr:\n{}",
                    self.stderr.lock().unwrap()
                );
            }
            if line.trim_start().starts_with("LISTEN ") {
                return;
            }
            assert!(Instant::now() < deadline, "等待 LISTEN 超时");
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
            "等待「{what}」超时。数据进程 stderr:\n{}",
            stderr.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn datanode_merges_files_from_shared_storage_and_commits_via_raft() {
    // ---- ① metanode（进程内 + 真 gRPC 服务）----
    let meta_dir = tmpdir("dn-compact-meta");
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

    // ---- ② 共享冷目录 = <dir>/cold：建表 + 写 3 个**真文件**并提交（走客户端那条路）----
    let dir = tmpdir("dn-compact");
    let cold_root = dir.join("cold");
    let store = create_store(&StoreConfig::Local {
        root: cold_root.to_string_lossy().into_owned(),
    })
    .expect("开共享存储");
    // `RemoteCatalog` 不是 `Clone` ⇒ 用 `Arc` 共享（trait 方法经 Deref 照常可用）
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

    // ---- ③ 起**数据进程**子进程（带 --compaction）----
    let mut datanode = Proc::start(meta_addr, &dir);
    datanode.wait_listen();

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
        &datanode.stderr,
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

/// **仲裁与接管（T14.2 判据）**：两个都开 `--compaction`，但**只有一个**持有租约；
/// 杀掉持有者 ⇒ 过 TTL 后另一个接管。
///
/// 观察点刻意选在 **metanode 的 Status（租约表）** 而不是"文件有没有被合并"：
/// 后者在"一个干、一个空转"与"两个都干但幂等"之间**区分不出来** ——
/// 而"谁在干全局作业"本来就是该被看见的东西（`§81`）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_one_compactor_holds_the_lease_and_it_is_taken_over_when_it_dies() {
    use yuntun_proto::meta as pb;

    // ---- metanode（进程内）----
    let meta_dir = tmpdir("lease-meta");
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

    /// 当前压缩租约的持有者（空 = 空闲/无人）
    fn holder(h: &yuntun_meta::NodeHandle) -> Option<String> {
        h.status()
            .leases
            .into_iter()
            .find(|l| l.purpose == "compaction" && !l.holder.is_empty())
            .map(|l| l.holder)
    }

    let cold_root = tmpdir("lease-cold");
    let d1 = tmpdir("lease-d1");
    let d2 = tmpdir("lease-d2");
    // 两个数据进程：**私有目录各归各的，冷根同一份**（共享对象存储的本机形态）
    let _a = Proc::start_node(meta_addr, &d1, "d1", &cold_root, 2);
    let _b = Proc::start_node(meta_addr, &d2, "d2", &cold_root, 2);

    // ---- 只有一个持有租约（这就是"meta 仲裁出唯一 compactor"的全部机制）----
    let hw = h.clone();
    wait_for(
        move || {
            let h = hw.clone();
            async move { holder(&h).is_some() }
        },
        Duration::from_secs(30),
        "有一个 compactor 取得租约",
        &Arc::new(Mutex::new(String::new())),
    )
    .await;
    let first = holder(&h).expect("应当有持有者");
    assert!(first == "d1" || first == "d2", "持有者必须是本用例的两个节点之一：{first}");

    // ---- 杀掉持有者 ⇒ 另一个在 TTL 之后接管 ----
    let survivor = if first == "d1" { "d2" } else { "d1" };
    drop(if first == "d1" { _a } else { _b }); // drop = kill + wait（见 Drop 实现）

    let hw2 = h.clone();
    let survivor_owned = survivor.to_string();
    wait_for(
        move || {
            let h = hw2.clone();
            let want = survivor_owned.clone();
            async move { holder(&h).as_deref() == Some(want.as_str()) }
        },
        Duration::from_secs(30),
        "幸存者接管了租约（过 TTL 后）",
        &Arc::new(Mutex::new(String::new())),
    )
    .await;

    // 顺带确认租约是**可见的**（运维角度：Status 里能读到它，而不是只能靠信任）
    let view = h
        .status()
        .leases
        .into_iter()
        .find(|l| l.purpose == "compaction")
        .expect("Status 里应当能看到压缩租约");
    assert!(!view.holder.is_empty());
    assert_eq!(view.holder, survivor);
    assert!(view.epoch >= 2, "接管必须推进代次（当前 {}）", view.epoch);
    let _ = pb::LeaseView::default();
}
