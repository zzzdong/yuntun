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

/// 日志用的 SQL 摘要（首 60 字节，压空白）。
fn sql_snippet_short(sql: &str) -> String {
    let collapsed: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    collapsed.chars().take(60).collect()
}

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
        // 多 schema：unknown database / 库已存在 / 库非空（MySQL 1049 / 1007 / 1008）
        yuntun_sql::SqlError::SchemaNotFound(_) => ErrorKind::ER_BAD_DB_ERROR,
        yuntun_sql::SqlError::SchemaExists(_) => ErrorKind::ER_DB_CREATE_EXISTS,
        yuntun_sql::SqlError::SchemaNotEmpty(_) => ErrorKind::ER_DB_DROP_EXISTS,
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

    /// 握手自报版本串。
    ///
    /// opensrv 默认回 `5.1.10-alpha-msql-proxy`，与 SQL 层 canned 的
    /// `SELECT @@version`（8.0.32-yuntun）**不一致**：客户端会拿到两个"版本"，
    /// 且 5.1.10 会让按版本分支的驱动/ORM 走老路径（如降级功能探测）。
    /// 统一取 [`yuntun_sql::shim::MYSQL_VERSION`]（单一事实源）。
    fn version(&self) -> String {
        yuntun_sql::shim::MYSQL_VERSION.to_string()
    }

    /// COM_QUERY：文本协议（CLI / 手工客户端 / 简单驱动）。
    async fn on_query<'a>(
        &'a mut self,
        sql: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        tracing::debug!(sql = %sql_snippet_short(sql), "wire cmd: COM_QUERY");
        match self.engine.execute(sql, &mut self.session).await {
            // 【§89 的遗留，写在这里免得下一个人重新发现一遍】
            //
            // `rows.partial` 是"这份结果缺了来源"的结论（引擎已经把它算出来了），但
            // **MySQL 的结果集里没有地方放它**：warning 计数在**结果集结束包（EOF）**里，
            // 而 `opensrv` 的 `ResultSetWriter::finish()` 把它**写死为 0**
            // （`opensrv-mysql-0.7.0/src/writers.rs`：`w.write_all(&[0x00, 0x00])?; // no warnings`）。
            // 只有 OK 包（`OkResponse { warnings, .. }`）能带 —— 那是"无结果集"的语句才走的路径。
            //
            // 所以今天只能：**日志里有**（引擎侧已 warn）、**wire 上不给**。
            // 要做全只有三条路：① 换/升级 opensrv（它给了带计数的 finish）；② 自己写结束包；
            // ③ 走 `SHOW WARNINGS` + 客户端主动查（但客户端拿不到计数，就不会主动查）。
            // 在这三条里选之前，**不假装已经交付**。
            Ok(SqlResult::Rows {
                schema,
                batches,
                partial: _,
            }) => {
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
        // 结果列 schema（MySQL 语义）：非查询语句（INSERT/DDL）无结果集 → num_columns = 0；
        // SELECT **必须**回真实列数与列定义。
        //
        // 【关键】libmysql 系客户端（mysql-connector C 扩展 / JDBC 二进制轨）依据 prepare
        // 响应里的 num_columns 决定 `COM_STMT_EXECUTE` 是否期待**二进制结果集**：
        // 报 0 会让它不消费随后的结果集 → 结果集包残留 → 错位到下一次 prepare 的响应
        // （表现为间歇 `1210 Incorrect number of arguments executing prepared statement`，
        // 且 `fetch*` 取不到行）。注：opensrv 对 execute 路径本就发二进制行
        // （`QueryResultWriter::new(..., is_bin = true)`），无需也不能在此做文本化处理。
        let columns: Vec<Column> = match self.engine.schema_of_in(sql, self.session.schema()).await {
            Some(schema) => encode::columns_of(&schema),
            None => Vec::new(),
        };
        let id = self.next_stmt_id;
        self.next_stmt_id += 1;
        tracing::debug!(
            stmt_id = id,
            param_count = stmt.param_count,
            result_columns = columns.len(),
            sql = %sql_snippet_short(sql),
            "stmt prepared"
        );
        self.prepared.insert(id, stmt);
        info.reply(id, &params, &columns).await
    }

    /// COM_STMT_EXECUTE：参数解码 → SqlValue → execute_prepared。
    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        tracing::debug!(stmt_id = id, "wire cmd: COM_STMT_EXECUTE");
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
        // 参数解码结果可见化：prepared 路径"查不到自己刚写的数据"时，第一个要区分的是
        // "服务端拿到错误的绑定值 → 查询本就为空" 与 "服务端结果正确、客户端解码失败"。
        tracing::debug!(stmt_id = id, params = ?vals, "stmt execute params decoded");
        match self
            .engine
            .execute_prepared(&stmt, &vals, &mut self.session)
            .await
        {
            Ok(SqlResult::Rows {
                schema,
                batches,
                partial: _, // 同上：结果集结束包放不下 warning 计数（opensrv 写死 0）
            }) => {
                let n: usize = batches.iter().map(|b| b.num_rows()).sum();
                tracing::debug!(stmt_id = id, rows = n, "stmt execute result rows");
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
        tracing::debug!(stmt_id = id, "wire cmd: COM_STMT_CLOSE（规范：不回包）");
        self.prepared.remove(&id);
    }

    /// 连接初始化（handshake 中的 database 名 / opensrv 侧 USE 语句转发）：
    /// 校验后仅记录（R-3：MVP 单 schema yuntun.public）。
    async fn on_init<'a>(
        &'a mut self,
        database: &'a str,
        writer: InitWriter<'a, W>,
    ) -> io::Result<()> {
        tracing::debug!(database = %database, "wire cmd: COM_INIT_DB / handshake db");
        if !database.is_empty() {
            // 多 schema：handshake 的 database 即 schema（MySQL 语义），
            // 校验存在后**真实切换**会话；未知库回 ER_BAD_DB_ERROR(1049)
            match self.engine.schema_exists(database).await {
                Ok(true) => self.session.set_schema(database),
                Ok(false) => {
                    return writer
                        .error(
                            ErrorKind::ER_BAD_DB_ERROR,
                            &format!("Unknown database '{database}'").into_bytes(),
                        )
                        .await;
                }
                Err(e) => {
                    return writer
                        .error(ErrorKind::ER_UNKNOWN_ERROR, &format!("{e}").into_bytes())
                        .await;
                }
            }
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

/// 调试用写入侧探针：把服务端**真正写到 socket 的字节**逐次记录（分帧由 MySQL
/// 长度前缀天然给出），再原样转发。用于排查 wire 协议层的包错位问题：
/// 客户端可见流与 `stmt prepared` 之类的回调日志无法直接对齐时，这里能给出
/// "服务端到底发了哪些包"的权威记录。
///
/// 仅当环境变量 `YUNTUN_WIRE_TRACE` 存在时启用（默认零开销直通）。
struct WireTee<W> {
    inner: W,
    enabled: bool,
}

impl<W: AsyncWrite + Unpin> AsyncWrite for WireTee<W> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        let enabled = self.enabled;
        let head: String = buf
            .iter()
            .take(32)
            .map(|b| format!("{b:02x} "))
            .collect();
        let offered = buf.len();
        let res = std::pin::Pin::new(&mut self.inner).poll_write(cx, buf);
        if enabled {
            let accepted = match &res {
                std::task::Poll::Ready(Ok(n)) => format!("ok({n})"),
                std::task::Poll::Ready(Err(e)) => format!("err({e})"),
                std::task::Poll::Pending => "pending".to_string(),
            };
            tracing::debug!(offered, accepted = %accepted, head = %head.trim(), "wire write");
        }
        res
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> std::task::Poll<io::Result<usize>> {
        let enabled = self.enabled;
        let offered: usize = bufs.iter().map(|b| b.len()).sum();
        let head: String = bufs
            .iter()
            .flat_map(|b| b.iter())
            .take(32)
            .map(|b| format!("{b:02x} "))
            .collect();
        let res = std::pin::Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        if enabled {
            let accepted = match &res {
                std::task::Poll::Ready(Ok(n)) => format!("ok({n})"),
                std::task::Poll::Ready(Err(e)) => format!("err({e})"),
                std::task::Poll::Pending => "pending".to_string(),
            };
            tracing::debug!(offered, accepted = %accepted, head = %head.trim(), "wire write_vectored");
        }
        res
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// **MySQL 包级读取适配器**（必须包在 opensrv 读取侧）。
///
/// # 为什么必需：opensrv 0.7 的释放后使用（UAF）
/// `opensrv-mysql 0.7.0` 的 `packet_reader.rs::PacketReader::next_async` 在
/// **同一读缓冲里还有剩余字节**时的处理有缺陷：
///
/// ```text
/// Ok((rest, p)) => {
///     self.remaining = rest.len();
///     if self.remaining > 0 {
///         self.bytes = rest.to_vec();   // ← 旧 buffer 在这里被释放
///     }
///     return Ok(Some(p));               // ← 但 p（Packet）仍指向旧 buffer
/// }
/// ```
/// `Packet` 是 `&[u8]`（+ 可选 Vec），`p` 借的是旧 `Vec<u8>` 的分配；赋值后旧分配被
/// 释放 → `commands::parse(&packet)` 读到**已释放内存** → 解析失败（或解析出随机命令）
/// → 走 opensrv 的兜底分支 `Err(_) => write_ok_packet(default)` 回一个**裸 OK 包** →
/// 客户端可见流多一个包 → 后续响应错位。
///
/// 触发条件：客户端把多条命令写进同一个 TCP 段（libmysql 的 `COM_STMT_CLOSE` 紧跟
/// `COM_STMT_PREPARE` 正是如此）。是否真的解析失败取决于释放内存是否已被复用 ——
/// 这就是该 bug 表现为"间歇（60~100%）"、且被代理/日志掩盖（改变分配时序）的原因；
/// 客户端一侧的症状是 `1210 Incorrect number of arguments executing prepared statement`。
///
/// # 修法
/// 本适配器**每次 `poll_read` 最多交付一个 MySQL 包**（按长度前缀分帧）。于是 opensrv
/// 的内部缓冲在成功解析后必然为空（`remaining == 0`），不会走到上面那条分支；
/// 同时保证"一个包一次解析"，不会再出现 phantom/兜底包。
///
/// ⚠️ 两个都做到才算数（踩过一次坑）：
/// 1. 一次只交付**一个**包；
/// 2. 交付时**按包边界截断**，而不是按调用方缓冲区剩余容量截断（`min(buf.len(),
///    out.remaining())` 是不够的）——否则底层一次 read 带回多条命令时，第二个包的
///    字节会被顺带交付，opensrv 内部又出现剩余字节，等于没修。
///
/// 它同时充当 wire 探针：`YUNTUN_WIRE_TRACE=1` 时按包记录客户端命令（不设时零开销）。
struct PacketFramedReader<R> {
    inner: R,
    buf: Vec<u8>,
    traced: bool,
}

impl<R: tokio::io::AsyncRead + Unpin> PacketFramedReader<R> {
    fn new(inner: R, traced: bool) -> Self {
        Self {
            inner,
            buf: Vec::new(),
            traced,
        }
    }

    /// 缓冲里是否已有**一个完整 MySQL 包**；返回其总字节数（4 头 + payload）。
    fn queued_packet_len(&self) -> Option<usize> {
        if self.buf.len() < 4 {
            return None;
        }
        let len = u32::from_le_bytes([self.buf[0], self.buf[1], self.buf[2], 0]) as usize;
        let total = 4 + len;
        (self.buf.len() >= total).then_some(total)
    }
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for PacketFramedReader<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        out: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        use std::task::Poll;
        let this = self.get_mut();

        // ① 先攒够一个完整包（或读到 EOF）
        while this.queued_packet_len().is_none() {
            let mut tmp = [0u8; 8192];
            let mut rb = tokio::io::ReadBuf::new(&mut tmp);
            match std::pin::Pin::new(&mut this.inner).poll_read(cx, &mut rb) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {
                    let n = rb.filled().len();
                    if n == 0 {
                        break; // 对端关闭：把已有字节交付完（AsyncRead 语义）
                    }
                    this.buf.extend_from_slice(rb.filled());
                }
            }
        }

        // ② 只交付**一个包**（按包边界截断，绝不在同一次交付里混入下一个包的字节）
        //
        // ★ 这里必须按包边界而不是按 `out.remaining()` 截断：底层一次 `read` 带回多条
        // 命令时（libmysql 常把 `COM_STMT_CLOSE` / `COM_STMT_RESET` / 下一条 `PREPARE`
        // 压进同一个 TCP 段），若把第二个包的字节一并交付，opensrv 内部缓冲在解析完
        // 第一个包后仍有剩余字节 → 又走回 `packet_reader.rs` 的 UAF 分支（`self.bytes =
        // rest.to_vec()`）→ 兜底裸 OK 包 → 客户端流错位。**分帧只有分到包边界才算数。**
        let want = this.queued_packet_len().unwrap_or(this.buf.len());
        let n = want.min(out.remaining());
        if n > 0 {
            // 探针：整包交付完成时记录一次（分包交付只在最后一片记录，避免重复）
            if this.traced && n == want && this.buf.len() >= 4 {
                let seq = this.buf[3];
                let head: String = this.buf[4..]
                    .iter()
                    .take(32)
                    .map(|b| format!("{b:02x} "))
                    .collect();
                tracing::debug!(
                    seq,
                    len = want - 4,
                    head = %head.trim(),
                    "wire recv (client → server)"
                );
            }
            out.put_slice(&this.buf[..n]);
            this.buf.drain(0..n);
        }
        Poll::Ready(Ok(()))
    }
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
                    // 读取侧必须包 `PacketFramedReader`（规避 opensrv 0.7 的 UAF，见其文档）；
                    // 写入侧探针仅诊断用（YUNTUN_WIRE_TRACE=1 时记录服务端发出的包）。
                    let trace = std::env::var_os("YUNTUN_WIRE_TRACE").is_some();
                    let r = PacketFramedReader::new(r, trace);
                    let w = WireTee {
                        inner: w,
                        enabled: trace,
                    };
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
