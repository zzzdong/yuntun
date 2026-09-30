//! 指标 HTTP 导出（`plan.md` T6.12 的"HTTP 导出"那一格，`operation-log §160`）。
//!
//! 三个端点，只有三个：
//!
//! | 路径 | 形态 | 给谁用 |
//! |---|---|---|
//! | `GET /metrics` | **Prometheus 文本**（0.0.4） | 抓取端（告警规则建在它上面） |
//! | `GET /metrics.json` | 指标快照的 JSON（`serde_json`） | 人、脚本、对拍 |
//! | `GET /healthz` | `ok` | 存活探针（**不查依赖**，语义见下） |
//!
//! # 为什么手写 HTTP/1.1，而不是引一个 HTTP 框架
//!
//! `§26.6` 当年把这件事挂在"需要新增依赖（离线环境不可加）"上。回头看：这里只需要**一个
//! 只读端点** —— 解析请求行、回一段文本、关闭连接。引框架要动依赖树（编译时间、供应链、
//! 以及"这个仓只有一个 HTTP 服务"这件事本身），而手写的那部分不到一百行、**协议面小到
//! 可以一眼看完**：只认 `GET`，不认 keep-alive、不认 chunked、不碰 header 语义。
//!
//! # 边界（写在明处）
//!
//! * **无鉴权**：绑定地址就是访问控制（默认 `127.0.0.1`）。要暴露到公网请放在反向代理后面；
//! * **串行处理 + 读超时**：抓取是低频、无状态的动作，不为它引入连接池；一个卡住的客户端
//!   最多占住 [`READ_TIMEOUT`]（超时即断开并回 408）；
//! * **`/healthz` 只证明"进程活着"**（liveness）。"现在能不能服务请求"是就绪探针（readiness）——
//!   语义不同，**本刀不做**：做了却只查一半依赖，比不做更误导运维；
//! * 只支持 `GET`（其它方法 405）；未知路径 404 且**把可用路径列在响应体里**。
//!
//! # Prometheus 文本与 JSON 的一致性
//!
//! Prometheus 的映射是**从 JSON 快照生成**的（`serde_json` 展开一层）⇒ 只有一份字段清单，
//! 不可能"JSON 里有、指标里没有"。字符串字段（如 `catalog.last_error`）Prometheus 装不下，
//! 因此只报 `<name>_available`（有没有值）；背压等级另给 `chunk.pressure_code`（数值码，
//! 见 `ChunkMetrics` 的字段文档）。这条规则由本模块的单测**逐字段**守着。

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::{Lakehouse, LakehouseMetrics, collect_metrics};

/// 请求头上限（防"半连接灌垃圾"）：只认 `GET <path> HTTP/1.1` 的服务不需要更多。
const MAX_HEAD_BYTES: usize = 8 * 1024;

/// 单次请求的读超时（见模块文档：不引连接池，靠超时兜住慢客户端）。
const READ_TIMEOUT: Duration = Duration::from_secs(2);

/// 启动指标 HTTP 服务。
///
/// `enabled = false` ⇒ 返回 `Ok(None)`（不打日志刷屏，只一条 info）。
/// 绑定失败**当场报错**（端口被占用是部署错误，静默降级会让人以为指标在跑）。
///
/// 返回 `(实际绑定的地址, JoinHandle)`：地址给日志与测试用（配置写 `:0` 时才有意义），
/// join 句柄让调用方**能确定地停掉它**（`§159` 的教训：拿不到 join 句柄的常驻任务会拖住资源）。
pub async fn spawn_metrics(
    lakehouse: &Lakehouse,
    cfg: &crate::config::MetricsSection,
) -> Result<Option<(SocketAddr, tokio::task::JoinHandle<()>)>, Box<dyn std::error::Error + Send + Sync>>
{
    if !cfg.enabled {
        tracing::info!("metrics http disabled by config ([metrics].enabled = false)");
        return Ok(None);
    }
    let listener = TcpListener::bind(&cfg.listen)
        .await
        .map_err(|e| format!("metrics http bind {}: {e}", cfg.listen))?;
    let addr = listener.local_addr()?;
    tracing::info!(%addr, "metrics http listening (/metrics, /metrics.json, /healthz)");

    // 与周期打点同款：只 clone 采集所需的四个句柄（`collect_metrics` 的签名就是为这个留的）
    let ingestor = lakehouse.ingestor.clone();
    let query = lakehouse.query.clone();
    let catalog = lakehouse.catalog.clone();
    let wal = lakehouse.wal.clone();
    let shutdown = lakehouse.shutdown.clone();

    let join = tokio::spawn(async move {
        loop {
            let (stream, peer) = tokio::select! {
                _ = shutdown.cancelled() => break,
                r = listener.accept() => match r {
                    Ok(x) => x,
                    Err(e) => {
                        tracing::warn!(error = %e, "metrics http accept 失败（继续监听）");
                        continue;
                    }
                },
            };
            // 每次请求现采一次（指标是"此刻"的水位，不是缓存值）
            let m = collect_metrics(&ingestor, &query, &catalog, &wal).await;
            if let Err(e) = handle(stream, &m).await {
                // 客户端早退是常态（curl 被 Ctrl-C、抓取端超时），不刷 warn
                tracing::debug!(peer = %peer, error = %e, "metrics http 连接处理失败");
            }
        }
        tracing::info!("metrics http 随 shutdown 退出");
    });
    Ok(Some((addr, join)))
}

/// 处理一个连接：读请求头 → 按 `(method, path)` 回文本 → 关闭。
async fn handle(mut stream: TcpStream, m: &LakehouseMetrics) -> std::io::Result<()> {
    let head = match tokio::time::timeout(READ_TIMEOUT, read_head(&mut stream)).await {
        Ok(Ok(h)) => h,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return respond(&mut stream, 408, "text/plain", "read timeout\n").await;
        }
    };
    let request_line = head.lines().next().unwrap_or_default().trim().to_string();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    // 查询串对这三个端点没有意义，去掉（`/metrics?x=1` 与 `/metrics` 同义）
    let raw_path = parts.next().unwrap_or_default();
    let path = raw_path.split('?').next().unwrap_or(raw_path);

    match (method, path) {
        ("GET", "/metrics") => {
            respond(
                &mut stream,
                200,
                "text/plain; version=0.0.4",
                &prometheus_text(m),
            )
            .await
        }
        ("GET", "/metrics.json") => {
            let body = serde_json::to_string_pretty(m).unwrap_or_else(|_| "{}".into());
            respond(&mut stream, 200, "application/json", &body).await
        }
        ("GET", "/healthz") => respond(&mut stream, 200, "text/plain", "ok\n").await,
        ("GET", _) => {
            respond(
                &mut stream,
                404,
                "text/plain",
                "not found：可用 /metrics、/metrics.json、/healthz\n",
            )
            .await
        }
        _ => {
            respond(&mut stream, 405, "text/plain", "only GET is supported\n").await
        }
    }
}

/// 读请求头直到空行（或超上限）。
async fn read_head(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 512];
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() >= MAX_HEAD_BYTES {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&buf).to_string())
}

/// 回一段文本并关闭连接（`Connection: close`：不实现 keep-alive，协议面刻意小）。
async fn respond(
    stream: &mut TcpStream,
    code: u16,
    content_type: &str,
    body: &str,
) -> std::io::Result<()> {
    let reason = match code {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        _ => "OK",
    };
    let head = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.flush().await
}

/// 指标快照 → **扁平**的 `(指标名, 值)` 列表。
///
/// 刻意**从 JSON 展开**（而不是手写一张字段表）：字段清单只有一份，
/// "加了字段忘了加指标"这类漂移就不可能出现（单测再逐字段钉一遍）。
fn flat_gauges(m: &LakehouseMetrics) -> Vec<(String, f64)> {
    let mut out = Vec::new();
    let Ok(serde_json::Value::Object(areas)) = serde_json::to_value(m) else {
        return out;
    };
    for (area, section) in areas {
        let serde_json::Value::Object(fields) = section else {
            continue;
        };
        for (field, value) in fields {
            let name = format!("yuntun_{area}_{field}");
            match value {
                serde_json::Value::Number(n) => out.push((name, n.as_f64().unwrap_or(0.0))),
                serde_json::Value::Bool(b) => out.push((name, if b { 1.0 } else { 0.0 })),
                // 字符串（`chunk.pressure` / `catalog.last_error`）与 `null`：Prometheus 装不下
                // ⇒ 只报"有没有值"（等级的**数值码**由 `chunk.pressure_code` 承担）
                serde_json::Value::Null => out.push((format!("{name}_available"), 0.0)),
                _ => out.push((format!("{name}_available"), 1.0)),
            }
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// 渲染 Prometheus 文本（0.0.4）。
pub fn prometheus_text(m: &LakehouseMetrics) -> String {
    let mut out = String::with_capacity(2048);
    for (name, value) in flat_gauges(m) {
        out.push_str(&format!("# TYPE {name} gauge\n"));
        out.push_str(&format!("{name} {value}\n"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CatalogMetrics, ChunkMetrics, QueryMetrics, WalMetrics};

    /// 一个人造快照：**每个字段都给不同的值**，免得"忘了搬字段"看不出来。
    fn snapshot() -> LakehouseMetrics {
        LakehouseMetrics {
            chunk: ChunkMetrics {
                chunks: 1,
                open: 2,
                sealed: 3,
                spilled: 4,
                flushed: 5,
                resident_bytes: 6,
                budget_bytes: 7,
                pressure_ratio: 0.5,
                pressure: "Soft".into(),
                pressure_code: 1,
                phase_yielded_flushes: 8,
            },
            wal: WalMetrics {
                synced_seq: 9,
                next_seq: 10,
                current_segment: 11,
                absorbed_seq: 12,
                backlog_records: 13,
                dir_bytes: 14,
            },
            catalog: CatalogMetrics {
                schema_ver: 15,
                manifest_ver: 16,
                snapshot: 17,
                tables: 18,
                refreshes: 19,
                full_reloads: 20,
                delta_tables: 21,
                last_error: None,
                freshness_checks: 23,
                lazy_refreshes: 24,
            },
            query: QueryMetrics {
                reserved_bytes: Some(22),
                limit_bytes: None,
            },
        }
    }

    /// **判据：JSON 快照里的每个字段都必须出现在 Prometheus 文本里**（`§160`）。
    ///
    /// 为什么要有它：Prometheus 的映射虽然是从 JSON 生成的，但"字符串只报 `_available`、
    /// 其余报数值"这张规则表是**手写**的 —— 少一条规则就会静默少一个指标，
    /// 而那正是"故障现场少一个数"的形状（不会报错，只会让人查不出来）。
    #[test]
    fn every_json_field_shows_up_in_the_prometheus_text() {
        let m = snapshot();
        let text = prometheus_text(&m);
        let serde_json::Value::Object(areas) = serde_json::to_value(&m).unwrap() else {
            panic!("指标快照必须是对象");
        };
        let mut checked = 0;
        for (area, section) in areas {
            let serde_json::Value::Object(fields) = section else {
                continue;
            };
            for (field, value) in fields {
                let base = format!("yuntun_{area}_{field}");
                let want = match value {
                    serde_json::Value::Number(_) | serde_json::Value::Bool(_) => base.clone(),
                    // 字符串 / null：只报 availability
                    _ => format!("{base}_available"),
                };
                assert!(
                    text.contains(&format!("{want} ")),
                    "字段 {area}.{field} 没有对应的指标 `{want}`：\n{text}"
                );
                checked += 1;
            }
        }
        assert!(checked >= 24, "字段数不对（{checked}）—— 判据本身要跟着字段增长");
    }

    /// 数值要**对得上**（不是"有没有这一行"就算过）。
    #[test]
    fn values_are_rendered_verbatim() {
        let text = prometheus_text(&snapshot());
        assert!(text.contains("yuntun_wal_backlog_records 13"), "{text}");
        assert!(text.contains("yuntun_chunk_pressure_code 1"), "{text}");
        // 数组/字符串不装数值；null 的 query.limit_bytes 报 0（available）
        assert!(text.contains("yuntun_query_limit_bytes_available 0"), "{text}");
        assert!(
            text.contains("yuntun_query_reserved_bytes 22"),
            "有值的那一项要出数值：{text}"
        );
    }
}
