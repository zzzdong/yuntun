//! **M3 ③ 的二进制层证据**：真起 `yuntun`（standalone）+ 连上去**真答一条 SQL**。
//!
//! `plan §8.3` 的 M3 第三格是"standalone 仍可单机运行"。此前 `cli.rs` 只测
//! `--version/--help`，且自己写着"真启动会绑端口…那是端到端测试的事" —— **而那条端到端不在**
//! （`§90` 记为 ⚠️）。`§92` 给出了补法的前提：先让 `yuntun` 打一行**接口行** `LISTEN <addr>`。
//!
//! 这一行是**接口而不是日志**（与数据进程 `LISTEN`、metanode 同形），它同时解掉了一个
//! 真实障碍：standalone 的监听地址来自 TOML 配置，若靠"猜一个空闲端口"再交给子进程，
//! 就会落进 `§37` 批评过的 TOCTOU。改成"配置里写 `:0`（内核分配）+ 从接口行读真实地址"，
//! 全程没有"探测—释放—再交给别人"的窗口。
//!
//! 断言刻意保守但**有分量**：进程真起来、接口行真打、Flight 真连上、SQL 真答回来。

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::CommandStatementQuery;
use futures::StreamExt as _;

const BIN: &str = env!("CARGO_BIN_EXE_yuntun");

fn tmpdir(tag: &str) -> std::path::PathBuf {
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

/// 起 `yuntun`，**从接口行**读回真实绑定地址。
async fn spawn_yuntun(dir: &std::path::Path) -> (std::process::Child, String) {
    let cfg = dir.join("yuntun.toml");
    // 只写**必须**的那一项：监听地址交给内核分配（`:0`）。
    // 其余走默认（`main` 会把 `[meta] dir` 补成 `./data/meta`，所以子进程的 CWD 必须在本目录，
    // 否则测试会往仓库里写数据）。
    std::fs::write(
        &cfg,
        r#"
[server]
listen = "127.0.0.1:0"

[sql.mysql]
enabled = false
"#,
    )
    .unwrap();

    let mut child = Command::new(BIN)
        .arg("--config")
        .arg(&cfg)
        .current_dir(dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("起 yuntun（standalone）");

    // ---- 接口行：`LISTEN <addr>` ----
    let stdout = child.stdout.take().expect("stdout");
    let addr = tokio::task::spawn_blocking(move || {
        let mut r = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            let n = r.read_line(&mut line).expect("读接口行");
            assert!(
                n > 0,
                "还没读到 LISTEN 接口行，进程就退了（stderr 见上）"
            );
            if let Some(a) = line.trim().strip_prefix("LISTEN ") {
                return a.to_string();
            }
        }
    })
    .await
    .expect("读接口行的线程");

    assert!(
        addr.starts_with("127.0.0.1:"),
        "接口行必须给出**真实绑定地址**：{addr:?}"
    );
    (child, addr)
}

async fn collect_do_get(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    ticket: arrow_flight::Ticket,
) -> Vec<arrow::record_batch::RecordBatch> {
    let mut stream = client.do_get(ticket).await.unwrap().into_inner();
    let mut datas = Vec::new();
    while let Some(fd) = stream.next().await {
        datas.push(fd.unwrap());
    }
    arrow_flight::utils::flight_data_to_batches(&datas).unwrap()
}

async fn sql(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    q: &str,
) -> Vec<arrow::record_batch::RecordBatch> {
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
        .expect("GetFlightInfo")
        .into_inner();
    let ticket = info
        .endpoint
        .first()
        .and_then(|e| e.ticket.clone())
        .expect("GetFlightInfo 未返回 ticket");
    collect_do_get(client, ticket).await
}

/// **standalone 真能单机运行**：真二进制起来 ⇒ 接口行给地址 ⇒ Flight 连得上 ⇒ SQL 答得回。
///
/// 查询刻意选一条**不依赖任何表**的（`SHOW DATABASES` 走方言 shim）：
/// 这条用例要证的是"这个二进制起得来、能对外服务"，不是再来一遍 SQL 语义（那是别的用例的事）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standalone_binary_serves_flight_end_to_end() {
    let dir = tmpdir("standalone-smoke");
    let (mut child, addr) = spawn_yuntun(&dir).await;

    // 连上（带重试：接口行打出来时 gRPC 可能还没完全就绪）
    let mut client = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while client.is_none() {
        match tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .expect("endpoint")
            .connect()
            .await
        {
            Ok(c) => client = Some(FlightServiceClient::new(c)),
            Err(e) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "连不上 standalone 的 Flight 面（{addr}）：{e}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    let mut client = client.unwrap();

    let batches = sql(&mut client, "SHOW DATABASES").await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(rows >= 1, "standalone 必须真的答回一条 SQL（SHOW DATABASES）");

    let _ = child.kill();
    let _ = child.wait();
}
