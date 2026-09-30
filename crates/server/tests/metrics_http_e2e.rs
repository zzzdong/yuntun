//! **指标 HTTP 导出**的端到端验收（`plan.md` T6.12 的"HTTP 导出"那一格，`operation-log §160`）。
//!
//! `T6.12` 的三项水位（内存 / WAL 积压 / 背压）早就接好了，只有导出形态一直是"结构化日志"。
//! 这一刀补上 HTTP，本用例守五件事：
//!
//! ① **三个端点各自对**：`/metrics` 的数值与 `Lakehouse::metrics()` 对得上、`/metrics.json`
//!    与同一份快照**逐字段**一致、`/healthz` 回 `ok`；
//! ② **协议面诚实地小**：未知路径 404、非 `GET` 405（都不 panic、都有关闭连接）；
//! ③ **端口占用启动即报错**（与 `[sql.mysql]` 同一条纪律：不静默降级 —— 否则运维会以为
//!    指标在跑）；
//! ④ **能确定地停掉**（`§159` 的教训）：`cancel + join` 之后端口不再接受连接；
//! ⑤ 配置里 `[metrics]` 段能读、且**默认关**（默认开会让每个进程去抢同一个端口）。

use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use yuntun_server::config::MetricsSection;
use yuntun_server::Lakehouse;

fn config(wal_dir: &str, store_root: &str) -> yuntun_server::Config {
    yuntun_server::Config::from_toml(&format!(
        r#"
[store]
type = "local"
root = "{store_root}"

[wal]
dir = "{wal_dir}"

[meta]
mode = "memory"

[chunk]
spill_dir = "{wal_dir}/spill"

[ingest]
rows_threshold = 1
time_threshold_secs = 5
max_flush_delay_secs = 1
flush_phase_spread_secs = 0
scan_interval_ms = 20
"#
    ))
    .unwrap()
}

/// 发一个**原始** HTTP 请求（不引客户端库：本用例要验的正是那层薄协议）。
async fn http_request(addr: SocketAddr, raw: &str) -> (u16, String) {
    let mut s = tokio::net::TcpStream::connect(addr)
        .await
        .unwrap_or_else(|e| panic!("连 {addr} 失败：{e}"));
    s.write_all(raw.as_bytes()).await.unwrap();
    s.flush().await.unwrap();
    let mut buf = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(5), s.read_to_end(&mut buf))
        .await
        .expect("读响应超时")
        .unwrap();
    let text = String::from_utf8_lossy(&buf).to_string();
    let status: u16 = text
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .unwrap_or_else(|| panic!("响应缺状态行：{text}"));
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

async fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    http_request(addr, &format!("GET {path} HTTP/1.1\r\nHost: t\r\n\r\n")).await
}

/// 从 Prometheus 文本里取一个指标的值。
fn gauge(text: &str, name: &str) -> f64 {
    text.lines()
        .find_map(|l| l.strip_prefix(&format!("{name} ")))
        .unwrap_or_else(|| panic!("文本里没有指标 {name}：\n{text}"))
        .trim()
        .parse()
        .unwrap_or_else(|e| panic!("指标 {name} 不是数：{e}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_http_serves_the_three_endpoints_and_stops_on_shutdown() {
    // ⑤ 配置：默认关 + TOML 段能读
    let default_section = MetricsSection::default();
    assert!(!default_section.enabled, "默认必须是关（否则每个进程都去抢端口）");
    let cfg_with_metrics = yuntun_server::Config::from_toml(
        r#"
[store]
type = "memory"
[wal]
dir = "/tmp/yuntun-metrics-cfg"
[metrics]
enabled = true
listen = "127.0.0.1:19999"
"#,
    )
    .expect("带 [metrics] 段的配置必须能读");
    assert!(cfg_with_metrics.metrics.enabled);
    assert_eq!(cfg_with_metrics.metrics.listen, "127.0.0.1:19999");
    // 老配置（没有 [metrics] 段）照样能读 ⇒ 落到默认（关）
    assert!(!config("/tmp/yuntun-metrics-wal", "/tmp/yuntun-metrics-store")
        .metrics
        .enabled);

    let guard = yuntun_testkit::TestDir::tmpfs("metrics-http");
    let base = guard.string();
    let cfg = config(&format!("{base}/wal"), &format!("{base}/store"));
    let shutdown = CancellationToken::new();
    let lakehouse = Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
        .await
        .unwrap();
    let bg = lakehouse.spawn_background(&cfg);
    let mut session = yuntun_sql::session::SessionCtx::default();

    // 让指标动起来：建表 + 插几行（水位/seq 不再是全 0）
    lakehouse
        .sql
        .execute("CREATE TABLE t (v BIGINT NOT NULL)", &mut session)
        .await
        .unwrap();
    for _ in 0..3 {
        lakehouse
            .sql
            .execute("INSERT INTO t VALUES (1)", &mut session)
            .await
            .unwrap();
    }

    // 起导出（`:0` ⇒ 由内核给端口，测试不抢固定端口）
    let section = MetricsSection {
        enabled: true,
        listen: "127.0.0.1:0".into(),
    };
    let (addr, join) = yuntun_server::spawn_metrics(&lakehouse, &section)
        .await
        .expect("起指标 HTTP 服务")
        .expect("enabled = true 时必须有句柄");

    // ① /metrics：数值与同一次采集对得上
    let (status, body) = http_get(addr, "/metrics").await;
    assert_eq!(status, 200);
    assert!(
        body.contains("# TYPE yuntun_chunk_resident_bytes gauge"),
        "Prometheus 文本要带 TYPE 行：\n{body}"
    );
    let now = lakehouse.metrics().await;
    assert_eq!(
        gauge(&body, "yuntun_chunk_budget_bytes") as u64,
        now.chunk.budget_bytes as u64,
        "/metrics 的数值必须与 `Lakehouse::metrics()` 同源"
    );
    assert!(
        gauge(&body, "yuntun_wal_next_seq") > 0.0,
        "插过数据之后 WAL 的 seq 应该往前走（指标不是摆设）"
    );
    assert!(
        body.contains("yuntun_chunk_pressure_code "),
        "背压等级的数值码必须导出（告警规则建在它上面）：\n{body}"
    );

    // /metrics.json：与同一份快照**逐字段**一致
    let (status, body) = http_get(addr, "/metrics.json").await;
    assert_eq!(status, 200);
    let served: serde_json::Value = serde_json::from_str(&body).expect("JSON 要能解析");
    let direct: serde_json::Value = serde_json::to_value(&now).unwrap();
    let keys = |v: &serde_json::Value| {
        let mut k: Vec<String> = v
            .as_object()
            .unwrap()
            .iter()
            .flat_map(|(a, sec)| {
                sec.as_object()
                    .unwrap()
                    .keys()
                    .map(|f| format!("{a}.{f}"))
                    .collect::<Vec<_>>()
            })
            .collect();
        k.sort();
        k
    };
    assert_eq!(
        keys(&served),
        keys(&direct),
        "HTTP 出来的字段集必须与结构体一致（否则就是少了一个指标）"
    );
    assert_eq!(served["chunk"]["budget_bytes"], direct["chunk"]["budget_bytes"]);
    assert_eq!(served["query"]["limit_bytes"], direct["query"]["limit_bytes"]);

    // ② 协议面：healthz / 404 / 405
    let (status, body) = http_get(addr, "/healthz").await;
    assert_eq!((status, body.as_str()), (200, "ok\n"));
    let (status, body) = http_get(addr, "/nope").await;
    assert_eq!(status, 404);
    assert!(body.contains("/metrics"), "404 要把可用路径列出来：{body}");
    let (status, body) = http_request(
        addr,
        "POST /metrics HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\n\r\n",
    )
    .await;
    assert_eq!(status, 405);
    assert!(body.contains("GET"), "{body}");

    // ③ 端口占用：**启动即报错**（不静默降级）
    let e = yuntun_server::spawn_metrics(
        &lakehouse,
        &MetricsSection {
            enabled: true,
            listen: addr.to_string(),
        },
    )
    .await
    .expect_err("端口被占用时必须报错");
    assert!(
        e.to_string().contains("metrics http bind"),
        "错误要点名是谁的端口：{e}"
    );

    // ④ 停得掉：cancel + join ⇒ 端口不再接受连接
    shutdown.cancel();
    for h in bg {
        let _ = h.await;
    }
    join.await.unwrap();
    assert!(
        tokio::net::TcpStream::connect(addr).await.is_err(),
        "join 之后监听必须真的关掉（`§159`：能 join 的任务才算停干净）"
    );
}
