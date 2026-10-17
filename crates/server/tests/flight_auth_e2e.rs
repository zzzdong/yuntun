//! **Flight 端口 token 鉴权**的端到端判据（U3 / `§190`）。
//!
//! MySQL 端口有口令握手（`sqlwire::auth` 的单测），而 Flight 端口的惯用做法是
//! **请求头带令牌** —— 两条链路的机制不同，所以各有一条判据。
//!
//! # 判据形状：三态对拍 + 反证
//!
//! ① 不带令牌 ⇒ `UNAUTHENTICATED`；② 错令牌 ⇒ `UNAUTHENTICATED`；
//! ③ 对令牌 ⇒ 真答回一条 SQL。
//! 然后**反证**：同一份代码在 `auth_token = None` 下重跑 ① ⇒ 必须成功
//! （否则"被拒"可能只是服务没起来 / 别的原因）。
//!
//! ⚠️ 走的是**生产路径**（[`yuntun_server::serve_flight_on`]）：用例自己复刻拦截器
//! 的话，生产代码写错照样绿 —— 这类"关掉就静默放行"的东西，判据必须在真实现上。

use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::CommandStatementQuery;
use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;
use yuntun_server::Lakehouse;

const TOKEN: &str = "tok-e2e-123";

/// 起一个 in-process Flight 服务（真 `serve_flight_on`），返回 `(地址, Lakehouse)`。
///
/// `Lakehouse` 必须交出去保活：它持有 ingestor/WAL 的命令通道，一丢就是
/// "命令通道断开"而不是"鉴权失败"（本仓踩过一次，见 `§188`）。
async fn start(token: Option<&'static str>) -> (String, Arc<Lakehouse>) {
    let wal_dir = yuntun_testkit::TestDir::tmpfs("flight-auth-e2e")
        .into_path()
        .to_string_lossy()
        .to_string();
    let cfg = yuntun_server::Config::from_toml(&format!(
        r#"
[server]
listen = "127.0.0.1:0"
{auth}

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
"#,
        auth = token
            .map(|t| format!("auth_token = \"{t}\""))
            .unwrap_or_default(),
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
    let lh = lakehouse.clone();
    let tok = token.map(|s| s.to_string());
    tokio::spawn(async move {
        let _ = yuntun_server::serve_flight_on(&lh, listener, tok.as_deref()).await;
    });
    (addr, lakehouse)
}

async fn connect(addr: &str) -> FlightServiceClient<tonic::transport::Channel> {
    let mut last = None;
    for _ in 0..200 {
        match tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
        {
            Ok(c) => return FlightServiceClient::new(c),
            Err(e) => {
                last = Some(e);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    panic!("连不上 {addr}：{last:?}");
}

/// 发一条 `SHOW DATABASES`（不依赖任何表），成功返回行数。
async fn query(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    bearer: Option<&str>,
) -> Result<usize, tonic::Status> {
    let cmd = yuntun_server::flight::command_bytes(&CommandStatementQuery {
        query: "SHOW DATABASES".to_string(),
        transaction_id: None,
    });
    let mut req = tonic::Request::new(arrow_flight::FlightDescriptor {
        r#type: 2,
        cmd: cmd.into(),
        path: vec![],
    });
    if let Some(t) = bearer {
        req.metadata_mut()
            .insert("authorization", format!("Bearer {t}").parse().unwrap());
    }
    let info = client.get_flight_info(req).await?.into_inner();
    let ticket = info
        .endpoint
        .first()
        .and_then(|e| e.ticket.clone())
        .expect("endpoint ticket");
    // ⚠️ `do_get` 是**第二个 RPC**：令牌要单独带（生产实现是每请求校验，
    // 这里漏了就会看到"get_flight_info 过了、do_get 被拒"——本用例第一次就栽在这）
    let mut req2 = tonic::Request::new(ticket);
    if let Some(t) = bearer {
        req2.metadata_mut()
            .insert("authorization", format!("Bearer {t}").parse().unwrap());
    }
    let mut s = client.do_get(req2).await?.into_inner();
    let mut datas = Vec::new();
    while let Some(fd) = s.next().await {
        datas.push(fd?);
    }
    let batches = arrow_flight::utils::flight_data_to_batches(&datas).unwrap();
    Ok(batches.iter().map(|b| b.num_rows()).sum())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flight_token_auth_rejects_missing_and_wrong_and_accepts_right() {
    let (addr, keep) = start(Some(TOKEN)).await;
    let mut c = connect(&addr).await;

    // ① 不带令牌
    let e = query(&mut c, None).await.expect_err("无令牌必须被拒");
    assert_eq!(e.code(), tonic::Code::Unauthenticated, "实际：{e}");
    // ② 错令牌
    let e = query(&mut c, Some("wrong"))
        .await
        .expect_err("错令牌必须被拒");
    assert_eq!(e.code(), tonic::Code::Unauthenticated, "实际：{e}");
    // ③ 对令牌 ⇒ 真答回一条 SQL
    let rows = query(&mut c, Some(TOKEN)).await.expect("对令牌必须放行");
    assert!(rows >= 1, "应当答回至少一行（SHOW DATABASES）");
    keep.shutdown.cancel();

    // ---- 反证：不配令牌 ⇒ 同一个"无令牌"请求必须成功 ----
    let (addr2, keep2) = start(None).await;
    let mut c2 = connect(&addr2).await;
    let rows = query(&mut c2, None)
        .await
        .expect("未启用鉴权时无令牌也应当放行");
    assert!(rows >= 1);
    keep2.shutdown.cancel();
}
