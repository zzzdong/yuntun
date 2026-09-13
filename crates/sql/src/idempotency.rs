//! 幂等键透传（S1.8）：SQL 文本 → 写入管线的 `client_request_id`。
//!
//! **通道**：SQL 注释标记 `/* idempotency_key=<k> */`（块注释）或 `-- idempotency_key=<k>`
//! （行注释，需在语句之前）。注释被 SQL 解析器忽略，因此对语句语义零影响，
//! 且对三条路径同时生效：
//!
//! | 路径 | 键来源 |
//! |---|---|
//! | SQL `INSERT ... VALUES / SELECT`（任意协议） | 本模块（SQL 注释） |
//! | Flight SQL prepared（`ActionCreatePreparedStatementRequest`） | prepared 语句上保存的键（同样由注释解析） |
//! | Flight DoPut 装载（简易轨 / prepared update） | `FlightData.app_metadata`（协议层，见 flight.rs） |
//!
//! 未提供键时由调用方按**语句级**生成（`dml-<uuid>` / `flightsql-<uuid>`）；
//! require 表（`IngestConfig::standard()`）缺键直接拒绝，绝不静默降级（架构 §7.3.2）。

/// 注释中识别幂等键的标记词。
pub const KEY_MARKER: &str = "idempotency_key";
/// 键长度上限（与 `yuntun_model::validate_idempotency_key` 对齐）。
const MAX_KEY_LEN: usize = 256;

/// 从 SQL 文本的注释中提取幂等键；未命中或取值非法 → `None`。
///
/// 取值字符集限定 `[A-Za-z0-9._:-]`（其余字符截断），避免注释内容意外变成键。
pub fn extract(sql: &str) -> Option<String> {
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        // 块注释 /* ... */
        if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            let Some(end) = find(bytes, i + 2, b"*/") else {
                return None; // 未闭合注释：交给 SQL 解析器报错
            };
            if let Some(k) = parse_body(&sql[i + 2..end]) {
                return Some(k);
            }
            i = end + 2;
            continue;
        }
        // 行注释 -- ... 或 # ...
        if (bytes[i] == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-') || bytes[i] == b'#' {
            let start = if bytes[i] == b'#' { i + 1 } else { i + 2 };
            let end = sql[start..]
                .find('\n')
                .map(|p| start + p)
                .unwrap_or(sql.len());
            if let Some(k) = parse_body(&sql[start..end]) {
                return Some(k);
            }
            i = end;
            continue;
        }
        i += 1;
    }
    None
}

fn find(hay: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    hay.get(from..)?
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|p| p + from)
}

/// 注释体内解析 `idempotency_key = <value>`。
fn parse_body(body: &str) -> Option<String> {
    let lower = body.to_ascii_lowercase();
    let start = lower.find(KEY_MARKER)?;
    // 前边界：避免匹配 `not_idempotency_key` 之类
    if start > 0 {
        let prev = body.as_bytes()[start - 1];
        if prev.is_ascii_alphanumeric() || prev == b'_' {
            return None;
        }
    }
    let rest = body[start + KEY_MARKER.len()..]
        .trim_start_matches(|c: char| c == '=' || c == ':' || c.is_whitespace());
    let value: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ':'))
        .collect();
    if value.is_empty() || value.len() > MAX_KEY_LEN {
        return None;
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_from_block_and_line_comments() {
        assert_eq!(
            extract("INSERT /* idempotency_key=abc-1 */ INTO t VALUES (1)").as_deref(),
            Some("abc-1")
        );
        assert_eq!(
            extract("-- idempotency_key: batch.7\nINSERT INTO t VALUES (1)").as_deref(),
            Some("batch.7")
        );
        assert_eq!(
            extract("# idempotency_key=abc\nSELECT 1").as_deref(),
            Some("abc")
        );
        // 大小写不敏感 / 多样分隔
        assert_eq!(
            extract("/* IDEMPOTENCY_KEY = K_9 */ SELECT 1").as_deref(),
            Some("K_9")
        );
    }

    #[test]
    fn absent_or_invalid_yields_none() {
        assert!(extract("INSERT INTO t VALUES (1)").is_none());
        assert!(extract("/* note: nothing here */ SELECT 1").is_none());
        // 前边界不满足（不误匹配）
        assert!(extract("/* not_idempotency_key=x */ SELECT 1").is_none());
        // 空值 / 超长值
        assert!(extract("/* idempotency_key= */ SELECT 1").is_none());
        assert!(extract(&format!("/* idempotency_key={} */ SELECT 1", "a".repeat(300))).is_none());
        // 未闭合注释 → None（交给解析器报错）
        assert!(extract("/* idempotency_key=abc").is_none());
    }

    #[test]
    fn value_stops_at_non_key_char() {
        // 逗号等非键字符截断（`abc`）
        assert_eq!(
            extract("/* idempotency_key=abc, other=1 */ SELECT 1").as_deref(),
            Some("abc")
        );
    }
}
