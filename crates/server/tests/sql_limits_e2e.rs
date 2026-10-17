//! **准入闸门**的进程内判据（`§192`）：超限连接被关、配额会归还、计数器说真话。
//!
//! # 为什么在 socket 层测
//!
//! 闸门的失败形态有两种，而且都很难在别处发现：
//!
//! * **拒了但没计数**：`SHOW STATUS` 说 `Connection_errors_max_connections = 0`，
//!   运维以为"没人被拒过"；
//! * **拒了但没归还配额**：第一次被拒之后，所有新连接都连不上（服务实际上死了），
//!   而日志里只有一条 warn。
//!
//! 所以这里不用 MySQL 客户端，而是**直接读握手包**：前两条必须收到字节，第三条必须
//! 立刻看到 EOF（不是挂着等超时），断开一条之后新连接必须重新收到握手包。
//!
//! 偏差（写在这里免得下一位以为是 bug）：MySQL 超限时回 **ERR 1040** 包；opensrv
//! 0.7 在握手前没有"写包"的接缝，这里只能关 socket。`SHOW STATUS` 不掩盖这件事。

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use yuntun_server::Lakehouse;

fn config(wal_dir: &str) -> yuntun_server::Config {
    yuntun_server::Config::from_toml(&format!(
        r#"
[server]
listen = "127.0.0.1:0"

[store]
type = "memory"

[wal]
dir = "{wal_dir}"

[chunk]
spill_dir = "{wal_dir}/spill"

[sql.mysql]
max_connections = 2
"#
    ))
    .unwrap()
}

/// 读到握手包即成功（opensrv 一接手就发 greeting）。
async fn greeting(sock: &mut TcpStream) -> bool {
    let mut buf = [0u8; 1];
    matches!(
        tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf)).await,
        Ok(Ok(n)) if n > 0
    )
}

/// 连上并等到握手包（配额归还可能有极短的调度延迟 —— 轮询到收敛，不赌一次成功）。
async fn connect_with_greeting(addr: std::net::SocketAddr) -> TcpStream {
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        let mut s = TcpStream::connect(addr).await.expect("connect");
        if greeting(&mut s).await {
            return s;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "3s 内没拿到握手包（配额没归还？）"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_connections_rejects_then_releases() {
    let wal_guard = yuntun_testkit::TestDir::tmpfs("sql-limits-e2e");
    let cfg = config(&wal_guard.string());
    assert_eq!(cfg.sql.mysql.max_connections, 2);

    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    // 配置值必须真的进了注册表（否则闸门按 151 放行，`SHOW VARIABLES` 却答 2）
    assert_eq!(lakehouse.sql.registry().max_connections(), 2);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let engine = lakehouse.sql.clone();
    let sd = shutdown.clone();
    let server = tokio::spawn(async move {
        let _ = yuntun_sqlwire::serve_mysql_on(
            engine,
            listener,
            yuntun_sqlwire::auth::AuthPolicy::trust(),
            sd,
        )
        .await;
    });

    // ①② 两条都能拿到握手包（`connect_with_greeting` 已断言）
    let c1 = connect_with_greeting(addr).await;
    let c2 = connect_with_greeting(addr).await;

    // ③ 第三条：**立刻**被关（读到 0 字节），且计数 +1
    let mut c3 = TcpStream::connect(addr).await.unwrap();
    let mut buf = [0u8; 1];
    let n = tokio::time::timeout(Duration::from_secs(2), c3.read(&mut buf))
        .await
        .expect("超限连接应当立刻有结论，而不是挂着等握手")
        .unwrap_or(0);
    assert_eq!(n, 0, "超限连接不应收到任何握手字节");
    let status = lakehouse.sql.registry().status();
    assert_eq!(
        status.connection_errors_max_connections, 1,
        "被拒连接必须计数（否则监控看不见）"
    );
    assert_eq!(status.threads_connected, 2, "只有两条被接纳");

    // ④ 断开一条 ⇒ 配额归还（反证：拆掉 `_permit` 的持有，这里会永远拿不到握手）
    drop(c1);
    let _c4 = connect_with_greeting(addr).await;
    let status = lakehouse.sql.registry().status();
    assert_eq!(status.connections_total, 3, "三次被接纳（c1/c2/c4）");
    assert_eq!(status.threads_connected, 2, "当前两条（c2 + c4）");
    assert_eq!(
        status.max_used_connections, 2,
        "峰值 = 上限（没有越过闸门）"
    );
    assert_eq!(status.max_connections, 2);

    drop(c2);
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), server).await;
}
