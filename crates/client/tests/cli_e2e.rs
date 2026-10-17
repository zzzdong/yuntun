//! **`yuntun-cli` 的进程级 e2e**：真起服务 + 真跑 CLI 二进制（子进程）。
//!
//! 为什么单开这一份：`cli.rs` 只测**参数解析**（不需要服务端），
//! `client_e2e.rs` 只测 **SDK**（不经过 shell 那一层）—— 而用户真正用的路是
//! "shell 里敲 `yuntun-cli`"，那条路此前的证据只有手工冒烟。
//! 本文件把它钉成用例：DDL → CSV 导入 → 三种格式查询 → `tables` / `schema`。
//!
//! 全部走**子进程**（`CARGO_BIN_EXE_yuntun-cli`）：exit code、stdout/stderr
//! 分流、`--format` 输出形状都是 CLI 契约的一部分。

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use yuntun_server::Lakehouse;

const BIN: &str = env!("CARGO_BIN_EXE_yuntun-cli");

/// 跑一次 CLI，断言退 0，返回 stdout。
fn cli_ok(addr: &str, args: &[&str]) -> String {
    let out = Command::new(BIN)
        .arg("--addr")
        .arg(addr)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("跑 yuntun-cli {args:?} 失败：{e}"));
    assert!(
        out.status.success(),
        "yuntun-cli {args:?} 应退 0，实际 {:?}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// 起一个 in-process Flight 服务，返回 `(地址, 关停 token, 服务任务, Lakehouse)`。
///
/// ⚠️ **Lakehouse 必须交出去保活**：它持有 ingestor/WAL 的命令通道，夹具函数
/// 结束时一丢，"命令通道断开"（客户端拿到 Internal error 而非连不上）。
async fn start_server() -> (
    String,
    CancellationToken,
    tokio::task::JoinHandle<()>,
    Arc<Lakehouse>,
) {
    let wal_dir = yuntun_testkit::TestDir::tmpfs("cli-e2e-wal")
        .into_path()
        .to_string_lossy()
        .to_string();
    let cfg = yuntun_server::Config::from_toml(&format!(
        r#"
[server]
listen = "127.0.0.1:0"

[store]
type = "memory"

[wal]
dir = "{wal_dir}"

[chunk]
spill_dir = "{wal_dir}/spill"

[ingest]
rows_threshold = 1
time_threshold_secs = 1
max_flush_delay_secs = 1
flush_phase_spread_secs = 0
scan_interval_ms = 20

[query]
cache_ttl_secs = 1
"#
    ))
    .unwrap();
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let _bg = lakehouse.spawn_background(&cfg);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let svc = arrow_flight::flight_service_server::FlightServiceServer::new(
        yuntun_server::FlightServer::new(
            lakehouse.ingestor.clone(),
            lakehouse.query.clone(),
            lakehouse.catalog.clone(),
        )
        .with_sql(lakehouse.sql.clone()),
    );
    let sd = shutdown.clone();
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move { sd.cancelled().await },
            )
            .await
            .unwrap();
    });
    (addr, shutdown, handle, lakehouse)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_round_trips_ddl_insert_query_against_a_real_server() {
    let (addr, shutdown, server, _lakehouse) = start_server().await;

    // ① DDL：无结果集语句打印 OK
    let out = cli_ok(
        &addr,
        &[
            "query",
            "CREATE TABLE cpu (ts BIGINT, host TEXT, usage DOUBLE)",
        ],
    );
    assert!(out.contains("OK"), "DDL 应打印 OK：{out}");

    // ② CSV 导入（回执行数 = 实际写入行数）
    let dir = yuntun_testkit::TestDir::tmpfs("cli-e2e-csv").into_path();
    let csv = dir.join("cpu.csv");
    std::fs::write(
        &csv,
        "ts,host,usage\n1,web-01,0.5\n2,web-01,0.7\n3,web-02,0.9\n",
    )
    .unwrap();
    let out = cli_ok(&addr, &["insert", "-t", "cpu", "-f", csv.to_str().unwrap()]);
    assert!(out.contains("inserted 3 rows"), "回执应报 3 行：{out}");

    // ③ 聚合查询（读己之写：轮询到可见，上界 = 1 个 scan 周期 + 余量）
    let mut last = String::new();
    for _ in 0..60 {
        last = cli_ok(
            &addr,
            &[
                "query",
                "SELECT host, count(*) AS cnt FROM cpu GROUP BY host ORDER BY host",
                "--format",
                "csv",
            ],
        );
        if last.contains("web-02,1") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        last.contains("web-01,2") && last.contains("web-02,1"),
        "聚合结果应含两组（table/csv 两种输出都要对）：{last}"
    );

    // ④ table 格式（默认）：管道符号表格
    let out = cli_ok(&addr, &["query", "SELECT count(*) AS c FROM cpu"]);
    assert!(
        out.contains('|') && out.contains('3'),
        "table 输出应含 3 行计数：{out}"
    );

    // ⑤ json 格式：行式 JSON
    let out = cli_ok(
        &addr,
        &[
            "query",
            "SELECT host FROM cpu ORDER BY ts LIMIT 1",
            "--format",
            "json",
        ],
    );
    assert!(out.contains("web-01"), "json 输出应含 host：{out}");

    // ⑥ tables / schema
    let out = cli_ok(&addr, &["tables"]);
    assert!(out.contains("cpu"), "tables 应列出 cpu：{out}");
    let out = cli_ok(&addr, &["schema", "cpu"]);
    for col in ["ts", "host", "usage"] {
        assert!(out.contains(col), "schema 应含列 {col}：{out}");
    }

    // ⑦ 不存在的表：退 1 + stderr 有错（不是静默成功）
    let bad = Command::new(BIN)
        .arg("--addr")
        .arg(&addr)
        .args(["query", "SELECT * FROM nope"])
        .output()
        .expect("跑 yuntun-cli");
    assert_eq!(bad.status.code(), Some(1), "查不存在的表应退 1");
    assert!(
        String::from_utf8_lossy(&bad.stderr).contains("error"),
        "stderr 应有错误信息：{}",
        String::from_utf8_lossy(&bad.stderr)
    );

    shutdown.cancel();
    let _ = server.await;
}
