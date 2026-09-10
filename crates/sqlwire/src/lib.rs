//! MySQL wire 协议适配层（sql-access-design.md §四/§五，W-3）。
//!
//! 职责边界：**只做协议编码/解码**——SQL 语义全部委托 [`yuntun_sql::SqlEngine`]：
//! - `COM_QUERY` → `on_query` → `engine.execute`（MySql 方言 shim 拦截 SET/USE/
//!   SHOW/@@var/事务 no-op）；
//! - `COM_STMT_PREPARE` → `engine.prepare`（占位符计数）；
//! - `COM_STMT_EXECUTE` → 参数解码（opensrv）→ [`yuntun_sql::SqlValue`] →
//!   `engine.execute_prepared`（AST 级替换，G3 无注入面）。
//!
//! 鉴权：trust（users 为空，R-1：网络层防火墙限制，设计 §6.1）。
//! 每连接一个 [`MysqlBackend`]（会话隔离：方言 / prepared 语句表，S-4）。

pub mod encode;

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind,
    InitWriter, OkResponse, ParamParser, QueryResultWriter, StatementMetaWriter, ValueInner,
};
use tokio::io::AsyncWrite;
use tokio_util::sync::CancellationToken;
use yuntun_sql::session::SessionCtx;
use yuntun_sql::{PreparedStatement, SqlEngine, SqlResult, SqlValue};

/// 每连接后端：持有引擎句柄 + 会话 + prepared 语句表。
pub struct MysqlBackend {
    engine: Arc<SqlEngine>,
    session: SessionCtx,
    /// stmt_id → 预编译语句（opensrv 分配 id 由我们维护）
    prepared: HashMap<u32, PreparedStatement>,
    next_stmt_id: u32,
}

impl MysqlBackend {
    pub fn new(engine: Arc<SqlEngine>) -> Self {
        Self {
            engine,
            session: SessionCtx::mysql(),
            prepared: HashMap::new(),
            next_stmt_id: 1,
        }
    }
}

/// SqlError → io::Error（opensrv 的 Error 类型；连接保持，ERR 包由调用方
/// 视路径决定：查询路径用 `results.error`，此处用于 prepare 等无 writer 场景）。
fn sql_io_err(e: yuntun_sql::SqlError) -> io::Error {
    io::Error::other(e.to_string())
}

/// 查询路径错误 → MySQL ERR 包（连接不断开，客户端可继续发下一条）。
async fn query_error<W: AsyncWrite + Send + Unpin>(
    results: QueryResultWriter<'_, W>,
    e: yuntun_sql::SqlError,
) -> io::Result<()> {
    let kind = match &e {
        yuntun_sql::SqlError::ReadOnly => ErrorKind::ER_OPTION_PREVENTS_STATEMENT,
        yuntun_sql::SqlError::NotFound(_) => ErrorKind::ER_NO_SUCH_TABLE,
        yuntun_sql::SqlError::TableExists(_) => ErrorKind::ER_TABLE_EXISTS_ERROR,
        yuntun_sql::SqlError::Parse(_) | yuntun_sql::SqlError::Unsupported(_) => {
            ErrorKind::ER_SYNTAX_ERROR
        }
        _ => ErrorKind::ER_UNKNOWN_ERROR,
    };
    results.error(kind, &format!("{e}").into_bytes()).await
}

#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for MysqlBackend {
    type Error = io::Error;

    /// COM_QUERY：文本协议（CLI / 手工客户端 / 简单驱动）。
    async fn on_query<'a>(
        &'a mut self,
        sql: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        match self.engine.execute(sql, &mut self.session).await {
            Ok(SqlResult::Rows { schema, batches }) => {
                let cols = encode::columns_of(&schema);
                let mut rw = results.start(&cols).await?;
                for batch in &batches {
                    for row in 0..batch.num_rows() {
                        encode::write_row(&mut rw, batch, &schema, row).await?;
                    }
                }
                rw.finish().await
            }
            Ok(SqlResult::Affected(n)) => {
                results
                    .completed(OkResponse {
                        affected_rows: n.unsigned_abs(),
                        ..Default::default()
                    })
                    .await
            }
            Err(e) => query_error(results, e).await,
        }
    }

    /// COM_STMT_PREPARE：占位符计数 + 语句缓存。
    async fn on_prepare<'a>(
        &'a mut self,
        sql: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        let stmt = self
            .engine
            .prepare(sql, &self.session)
            .await
            .map_err(sql_io_err)?;
        // 参数类型声明：统一 VAR_STRING（MySQL 二进制协议按客户端实际类型发送，
        // 服务端不依赖此声明；databend 同款做法）
        let params: Vec<Column> = (0..stmt.param_count)
            .map(|_| Column {
                table: String::new(),
                column: "?".to_string(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            })
            .collect();
        let id = self.next_stmt_id;
        self.next_stmt_id += 1;
        self.prepared.insert(id, stmt);
        info.reply(id, &params, &[]).await
    }

    /// COM_STMT_EXECUTE：参数解码 → SqlValue → execute_prepared。
    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        // clone 避免 prepared 表与 session 的借用冲突（PreparedStatement: Clone）
        let Some(stmt) = self.prepared.get(&id).cloned() else {
            return Err(io::Error::other(format!(
                "unknown prepared statement id {id}"
            )));
        };
        let vals = params
            .into_iter()
            .map(|v| param_value(v.value.into_inner()))
            .collect::<io::Result<Vec<_>>>()?;
        match self
            .engine
            .execute_prepared(&stmt, &vals, &mut self.session)
            .await
        {
            Ok(SqlResult::Rows { schema, batches }) => {
                let cols = encode::columns_of(&schema);
                let mut rw = results.start(&cols).await?;
                for batch in &batches {
                    for row in 0..batch.num_rows() {
                        encode::write_row(&mut rw, batch, &schema, row).await?;
                    }
                }
                rw.finish().await
            }
            Ok(SqlResult::Affected(n)) => {
                results
                    .completed(OkResponse {
                        affected_rows: n.unsigned_abs(),
                        ..Default::default()
                    })
                    .await
            }
            Err(e) => query_error(results, e).await,
        }
    }

    /// COM_STMT_CLOSE：清理语句缓存。
    async fn on_close<'a>(&'a mut self, id: u32) {
        self.prepared.remove(&id);
    }

    /// 连接初始化（handshake 中的 database 名 / opensrv 侧 USE 语句转发）：
    /// 校验后仅记录（R-3：MVP 单 schema yuntun.public）。
    async fn on_init<'a>(
        &'a mut self,
        database: &'a str,
        writer: InitWriter<'a, W>,
    ) -> io::Result<()> {
        if !database.is_empty() {
            // MVP：任意库名接受并记录（USE 同语义，R-3 偏差留档）
            self.session.default_db = Some(database.to_string());
        }
        writer.ok().await
    }
}

/// opensrv 参数（二进制值）→ SqlValue（类型由客户端实际发送值决定，非 prepare 声明）。
fn param_value(v: ValueInner) -> io::Result<SqlValue> {
    Ok(match v {
        ValueInner::NULL => SqlValue::Null,
        ValueInner::Int(i) => SqlValue::Int(i),
        ValueInner::UInt(u) => SqlValue::UInt(u),
        ValueInner::Double(f) => SqlValue::Float(f),
        ValueInner::Bytes(b) => {
            // 字节流优先按 UTF-8 文本解释（SQL 字面量路径）；二进制保留 Bytes
            match std::str::from_utf8(b) {
                Ok(s) => SqlValue::Str(s.to_string()),
                Err(_) => SqlValue::Bytes(b.to_vec()),
            }
        }
        // MySQL 二进制日期/时间编码（pymysql 等驱动对 datetime 参数走此路径）：
        // 解码为 ISO 文本，与 params.rs 的 TypedString 解析路径互逆
        ValueInner::Date(b) | ValueInner::Datetime(b) => SqlValue::Str(decode_datetime(b)?),
        ValueInner::Time(b) => SqlValue::Str(decode_time(b)?),
    })
}

/// MySQL 二进制 DATE/DATETIME 解码 → ISO 文本 `YYYY-MM-DD[ HH:MM:SS[.ffffff]]`。
///
/// **布局（opensrv 已消费长度前缀）**：`[year:u16][month][day][hour][min][sec][micros:u32]`，
/// 由负载长度决定字段数（0 = 零值 / 4 = 仅日期 / 7 = 秒精度 / 11 = 微秒精度）。
/// 该错误的"首字节即长度"假设会读到年份低字节（如 2026 → 0xEA = 234）。
fn decode_datetime(b: &[u8]) -> io::Result<String> {
    let len = b.len();
    if !matches!(len, 0 | 4 | 7 | 11) {
        return Err(io::Error::other(format!(
            "invalid binary datetime payload length {len}"
        )));
    }
    if len == 0 {
        return Ok("0000-00-00 00:00:00".to_string());
    }
    let y = i64::from(u16::from_le_bytes([b[0], b[1]]));
    let (m, d) = (u32::from(b[2]), u32::from(b[3]));
    // 4 字节仅日期部分；7/11 字节含时间部分
    let (hh, mm, ss) = if len >= 7 {
        (u32::from(b[4]), u32::from(b[5]), u32::from(b[6]))
    } else {
        (0, 0, 0)
    };
    let micros = if len == 11 {
        u32::from_le_bytes([b[7], b[8], b[9], b[10]])
    } else {
        0
    };
    Ok(if micros == 0 {
        format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}")
    } else {
        format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}.{micros:06}")
    })
}

/// MySQL 二进制 TIME 解码 → `[-]HH:MM:SS[.ffffff]`。
///
/// 布局（同样无长度前缀）：`[neg][days:u32][hour][min][sec][micros:u32]`；
/// hours 含 days 扩位（可 > 24），micros 位于 sec 之后。
fn decode_time(b: &[u8]) -> io::Result<String> {
    let len = b.len();
    if !matches!(len, 0 | 8 | 12) {
        return Err(io::Error::other(format!(
            "invalid binary time payload length {len}"
        )));
    }
    if len == 0 {
        return Ok("00:00:00".to_string());
    }
    let neg = b[0] != 0;
    let days = u32::from_le_bytes([b[1], b[2], b[3], b[4]]);
    let hours = days * 24 + u32::from(b[5]);
    let sign = if neg { "-" } else { "" };
    let (mm, ss) = (u32::from(b[6]), u32::from(b[7]));
    Ok(if len == 12 {
        let micros = u32::from_le_bytes([b[8], b[9], b[10], b[11]]);
        format!("{sign}{hours:02}:{mm:02}:{ss:02}.{micros:06}")
    } else {
        format!("{sign}{hours:02}:{mm:02}:{ss:02}")
    })
}

/// 启动 MySQL wire 监听（:3306 标准端口，R-2）。
///
/// 每连接 spawn 一个 `AsyncMysqlIntermediary`（trust 鉴权，无 TLS——MVP 网络层隔离）。
pub async fn serve_mysql(
    engine: Arc<SqlEngine>,
    listen: &str,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(%listen, "mysql wire protocol listening");
    serve_mysql_on(engine, listener, shutdown).await
}

/// 同 [`serve_mysql`]，但接管**外部已绑定**的 listener。
///
/// 节点层（server/standalone）先 bind 再 spawn 本函数：bind 失败（如 3306 被占）
/// 在启动阶段即明确报错，不静默降级（设计 §6.3 端口冲突约定）。
pub async fn serve_mysql_on(
    engine: Arc<SqlEngine>,
    listener: tokio::net::TcpListener,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("mysql wire shutting down");
                return Ok(());
            }
            conn = listener.accept() => {
                let (stream, peer) = match conn {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(error = %e, "mysql accept failed");
                        continue;
                    }
                };
                let engine = engine.clone();
                let shutdown = shutdown.clone();
                tokio::spawn(async move {
                    if shutdown.is_cancelled() {
                        return;
                    }
                    tracing::debug!(%peer, "mysql connection accepted");
                    let (r, w) = stream.into_split();
                    let backend = MysqlBackend::new(engine);
                    if let Err(e) = AsyncMysqlIntermediary::run_on(backend, r, w).await {
                        tracing::debug!(%peer, error = %e, "mysql connection ended");
                    }
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn param_decoding() {
        use ValueInner as V;
        assert!(matches!(param_value(V::NULL).unwrap(), SqlValue::Null));
        assert!(matches!(
            param_value(V::Int(-5)).unwrap(),
            SqlValue::Int(-5)
        ));
        assert!(matches!(
            param_value(V::UInt(7)).unwrap(),
            SqlValue::UInt(7)
        ));
        assert!(matches!(
            param_value(V::Double(1.5)).unwrap(),
            SqlValue::Float(f) if f == 1.5
        ));
        assert!(matches!(
            param_value(V::Bytes(b"abc".as_slice())).unwrap(),
            SqlValue::Str(s) if s == "abc"
        ));
        // 非 UTF-8 字节保留 Bytes
        assert!(matches!(
            param_value(V::Bytes(vec![0xff, 0xfe].as_slice())).unwrap(),
            SqlValue::Bytes(_)
        ));
    }

    #[test]
    fn binary_datetime_decoding() {
        // 注意：opensrv 的 ParamParser 已消费长度前缀，负载即字段序列
        // 零值
        assert_eq!(decode_datetime(&[]).unwrap(), "0000-00-00 00:00:00");
        // 仅日期：2022-01-08（year = 0x07E6 → LE [E6, 07]）
        assert_eq!(decode_datetime(&[0xE6, 0x07, 1, 8]).unwrap(), "2022-01-08 00:00:00");
        // 完整：2022-01-08 12:34:56
        assert_eq!(
            decode_datetime(&[0xE6, 0x07, 1, 8, 12, 34, 56]).unwrap(),
            "2022-01-08 12:34:56"
        );
        // 带微秒：2022-01-08 12:34:56.000500（micros = 0x000001F4）
        assert_eq!(
            decode_datetime(&[0xE6, 0x07, 1, 8, 12, 34, 56, 0xF4, 0x01, 0x00, 0x00]).unwrap(),
            "2022-01-08 12:34:56.000500"
        );
        // TIME：1 天 2 小时（[neg][days:u32][h][m][s]）
        assert_eq!(decode_time(&[0, 1, 0, 0, 0, 2, 0, 0]).unwrap(), "26:00:00");
        // TIME 负值
        assert_eq!(decode_time(&[1, 0, 0, 0, 0, 1, 0, 0]).unwrap(), "-01:00:00");
        // TIME 带微秒（micros 位于 sec 之后）：01:02:03.500000
        assert_eq!(
            decode_time(&[0, 0, 0, 0, 0, 1, 2, 3, 0x20, 0xA1, 0x07, 0x00]).unwrap(),
            "01:02:03.500000"
        );
        // 零值 TIME
        assert_eq!(decode_time(&[]).unwrap(), "00:00:00");
    }

    #[test]
    fn malformed_binary_params_error() {
        // 负载长度非法（非 0/4/7/11 与 0/8/12）
        assert!(decode_datetime(&[1, 2, 3]).is_err());
        assert!(decode_time(&[0, 0, 0]).is_err());
        assert!(decode_datetime(&[0xE6, 0x07, 1, 8, 12]).is_err());
    }
}
