//! **CLI/SDK 侧**的 Flight 令牌判据（`§190` 的收尾）。
//!
//! 背景：服务端加 `[server].auth_token` 之后，如果客户端不会带令牌，那个开关一旦打开
//! 就等于把自家 CLI / SDK 锁在门外 —— 这不是假设，`§190` 第一版只做了服务端，
//! 本仓的 `Client::connect` 与 `yuntun-cli` 都没有入口。
//!
//! # 判据形状：三态对拍（与 `server/tests/flight_auth_e2e.rs` 同构，但走 SDK）
//!
//! ① 对令牌 ⇒ 真答回 SQL；② 缺令牌 ⇒ `Unauthenticated`；③ 错令牌 ⇒ `Unauthenticated`。
//!
//! ⚠️ 服务端走**生产路径** `serve_flight_on`（不在用例里复刻拦截器，`§190.3` 的教训）。

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use yuntun_client::{Client, ClientError};
use yuntun_server::Lakehouse;

const TOKEN: &str = "tok-cli-e2e";

/// 起一个带令牌的 in-process Flight 服务，返回 `(地址, Lakehouse)`。
async fn start() -> (String, Arc<Lakehouse>) {
    let wal_dir = yuntun_testkit::TestDir::tmpfs("flight-token-e2e")
        .into_path()
        .to_string_lossy()
        .to_string();
    let cfg = yuntun_server::Config::from_toml(&format!(
        r#"
[server]
listen = "127.0.0.1:0"
auth_token = "{TOKEN}"

[store]
type = "memory"

[wal]
dir = "{wal_dir}"

[chunk]
spill_dir = "{wal_dir}/spill"

[ingest]
rows_threshold = 1
time_threshold_secs = 0
max_flush_delay_secs = 0
flush_phase_spread_secs = 0
scan_interval_ms = 20

[sql.mysql]
enabled = false
"#
    ))
    .unwrap();
    let cfg = Arc::new(cfg);
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let _bg = lakehouse.spawn_background(&cfg);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let lh = lakehouse.clone();
    tokio::spawn(async move {
        let _ = yuntun_server::serve_flight_on(&lh, listener, Some(TOKEN)).await;
    });
    (addr, lakehouse)
}

/// 重试到服务就绪：第一发 RPC 可能撞上监听器还没接客的窗口。
async fn ready(addr: &str) -> Client {
    let mut last = None;
    for _ in 0..200 {
        match Client::connect_with_token(addr, Some(TOKEN)).await {
            Ok(c) => match c.query("SHOW DATABASES").await {
                Ok(_) => return c,
                Err(e) => last = Some(e.to_string()),
            },
            Err(e) => last = Some(e.to_string()),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("服务没就绪：{last:?}");
}

fn assert_unauthenticated(e: ClientError) {
    match e {
        ClientError::Rpc(status) => {
            assert_eq!(
                status.code(),
                tonic::Code::Unauthenticated,
                "实际：{status}"
            );
        }
        other => panic!("期望 Unauthenticated，实际：{other}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sdk_sends_the_token_on_every_rpc() {
    let (addr, keep) = start().await;
    let client = ready(&addr).await;

    // ① 对令牌 ⇒ 真答回 SQL（不只 `get_flight_info` 通了：`query` 会走 `do_get`，
    // 两个 RPC 都必须带上头 —— `§190.3` 的第二个坑）
    let batches = client
        .query("SHOW DATABASES")
        .await
        .expect("对令牌必须放行");
    assert!(!batches.is_empty(), "应当答回至少一行");

    // ② 缺令牌
    let anon = Client::connect(&addr).await.unwrap();
    assert_unauthenticated(
        anon.query("SHOW DATABASES")
            .await
            .expect_err("缺令牌必须被拒"),
    );

    // ③ 错令牌
    let wrong = Client::connect_with_token(&addr, Some("definitely-wrong"))
        .await
        .unwrap();
    assert_unauthenticated(
        wrong
            .query("SHOW DATABASES")
            .await
            .expect_err("错令牌必须被拒"),
    );

    // ④ 非法令牌字符 ⇒ 连接阶段就报错（不是每次 RPC 都失败）
    assert!(
        matches!(
            Client::connect_with_token(&addr, Some("bad\ntoken")).await,
            Err(ClientError::Other(_))
        ),
        "含换行的令牌必须被拒"
    );

    keep.shutdown.cancel();
}

/// **反证**：同一个 `Client::connect`（不带令牌）在服务端**未启用**鉴权时必须成功 ——
/// 否则上面那些"被拒"可能只是客户端根本没连对。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_token_client_still_works_when_auth_is_off() {
    let wal_dir = yuntun_testkit::TestDir::tmpfs("flight-noauth-e2e")
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
time_threshold_secs = 0
max_flush_delay_secs = 0
flush_phase_spread_secs = 0
scan_interval_ms = 20

[sql.mysql]
enabled = false
"#
    ))
    .unwrap();
    let cfg = Arc::new(cfg);
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    let _bg = lakehouse.spawn_background(&cfg);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let lh = lakehouse.clone();
    tokio::spawn(async move {
        let _ = yuntun_server::serve_flight_on(&lh, listener, None).await;
    });

    let mut ok = false;
    for _ in 0..200 {
        if let Ok(c) = Client::connect(&addr).await
            && c.query("SHOW DATABASES").await.is_ok()
        {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(ok, "未启用鉴权时无令牌的 SDK 必须能查");
    lakehouse.shutdown.cancel();
}
