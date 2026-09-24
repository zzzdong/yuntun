//! yuntun Rust SDK（S1.9）：Arrow Flight **简易轨**客户端。
//!
//! 协议契约（`crates/server/src/flight.rs`，与 `tests/flight_e2e.rs` 一致）：
//! - **查询**：`do_get(Ticket{ ticket = SQL 文本 })` → IPC `FlightData` 流；
//!   `get_schema(descriptor{ cmd = SQL })` → 结果集 schema（逻辑计划，不执行）；
//! - **写入**：`do_put`，首条消息携带
//!   `FlightDescriptor{ type = PATH(1), path = [table, shard] }`，
//!   数据消息可选 `app_metadata = {"idempotency_key": "..."}`，ack 为 JSON 回执。
//!
//! 标准客户端（pyarrow / ADBC / JDBC / DBeaver）走**标准轨**（Flight SQL），
//! 自有客户端走简易轨（零开销直查）——plan §4.2 双轨设计。
//!
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let client = yuntun_client::Client::connect("127.0.0.1:50051").await?;
//! let batches = client.query("SELECT count(*) AS c FROM yuntun.public.cpu").await?;
//! println!("{:?}", batches[0].num_rows());
//! # Ok(()) }
//! ```

pub mod input;

use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightDescriptor, Ticket};
use futures::{Stream, StreamExt};
use tonic::transport::Channel;

pub use input::InputFormat;

/// 默认 shard（与 SqlEngine 的语句级写入一致）。
pub const DEFAULT_SHARD: &str = "default";

/// 客户端错误。
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("connect {0}: {1}")]
    Connect(String, String),
    #[error("rpc: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("decode: {0}")]
    Decode(String),
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, ClientError>;

/// 写入回执（server `Receipt` 的 JSON 形态，架构 §11.3）。
#[derive(Debug, Clone, serde::Deserialize)]
pub struct InsertReceipt {
    #[serde(default)]
    pub table: String,
    #[serde(default)]
    pub shard: String,
    /// 本批数据在 WAL 中的确认 seq
    #[serde(default)]
    pub wal_seq: u64,
    #[serde(default)]
    pub row_count: u64,
    #[serde(default)]
    pub schema_version: u64,
    /// 预计可见时间（Unix 毫秒）
    #[serde(default)]
    pub expected_visible_at: u64,
    /// 攒批时间窗（秒）
    #[serde(default)]
    pub expected_visible_in_secs: u64,
    /// 该批次被**幂等去重**（此前同键请求已落库）：`row_count` 为 0，
    /// 客户端应视为成功而非错误（重试不产生重复，§7.3）。
    #[serde(default)]
    pub duplicate: bool,
}

/// yuntun 客户端（可 clone 共享；tonic channel 内部为多路复用）。
#[derive(Clone)]
pub struct Client {
    inner: FlightServiceClient<Channel>,
    addr: String,
}

impl Client {
    /// 连接 Flight 端点；`addr` 支持 `host:port` 或 `http://host:port`。
    ///
    /// **连任一"能接 SQL"的端点即可**：standalone 的 Flight 端口，或某个数据进程的
    /// `--sql-listen`。多节点时**扇出 / 合并由该节点（协调者）负责**（`architecture §4.2`），
    /// 客户端**不需要**知道其余数据进程。
    ///
    /// 落点由**部署显式给定**（**contact point**，设计如此）——客户端**不做**名录发现、
    /// 一致性哈希软路由或存活感知（决策 `D13` / `operation-log §100`）。
    pub async fn connect(addr: &str) -> Result<Self> {
        let url = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr.to_string()
        } else {
            format!("http://{addr}")
        };
        let channel = Channel::from_shared(url.clone())
            .map_err(|e| ClientError::Connect(url.clone(), e.to_string()))?
            .connect()
            .await
            .map_err(|e| ClientError::Connect(url, e.to_string()))?;
        Ok(Self {
            inner: FlightServiceClient::new(channel),
            addr: addr.to_string(),
        })
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// 一次性查询：返回全部批次（eager，大结果集请用 [`Client::query_stream`]）。
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let mut stream = self.query_stream(sql).await?;
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item?);
        }
        Ok(out)
    }

    /// 流式查询：结果集边到边用（CLI / SDK 大结果集路径）。
    pub async fn query_stream(
        &self,
        sql: &str,
    ) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + 'static> {
        let mut client = self.inner.clone();
        let stream = client
            .do_get(Ticket {
                ticket: sql.as_bytes().to_vec().into(),
            })
            .await?
            .into_inner();
        let decoded =
            FlightRecordBatchStream::new_from_flight_data(stream.map(|r| r.map_err(to_flight_error)));
        Ok(decoded.map(|r| r.map_err(|e| ClientError::Decode(e.to_string()))))
    }

    /// 结果集 schema（逻辑计划，不执行查询）。
    pub async fn schema_of(&self, sql: &str) -> Result<SchemaRef> {
        let mut client = self.inner.clone();
        let res = client
            .get_schema(FlightDescriptor {
                r#type: 2, // CMD
                cmd: sql.as_bytes().to_vec().into(),
                path: vec![],
            })
            .await?
            .into_inner();
        let msg = arrow::ipc::root_as_message(&res.schema)
            .map_err(|e| ClientError::Decode(format!("schema ipc: {e}")))?;
        let fb = msg
            .header_as_schema()
            .ok_or_else(|| ClientError::Decode("not a schema message".into()))?;
        Ok(Arc::new(arrow::ipc::convert::fb_to_schema(fb)))
    }

    /// 表 schema（等价 `SELECT * FROM t LIMIT 0` 的 schema）。
    pub async fn table_schema(&self, table: &str) -> Result<SchemaRef> {
        self.schema_of(&format!("SELECT * FROM {table} LIMIT 0"))
            .await
    }

    /// 执行无结果集语句（DDL / INSERT）：简易轨 `do_get` 亦承载写语句，
    /// 返回空批次即成功（错误以 RPC 状态返回）。
    pub async fn execute(&self, sql: &str) -> Result<()> {
        let _ = self.query(sql).await?;
        Ok(())
    }

    /// 表清单（`SHOW TABLES`，取 `table_name` 列）。
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        let batches = self.query("SHOW TABLES").await?;
        let mut names = Vec::new();
        for b in &batches {
            if b.num_rows() == 0 {
                continue;
            }
            // Generic 方言：三列（catalog/schema/table_name）；MySql 方言：单列
            let idx = b
                .schema()
                .fields()
                .iter()
                .position(|f| f.name() == "table_name")
                .unwrap_or(b.num_columns() - 1);
            let col = b
                .column(idx)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| ClientError::Other("SHOW TABLES 返回非字符串列".into()))?;
            for i in 0..col.len() {
                if !col.is_null(i) {
                    names.push(col.value(i).to_string());
                }
            }
        }
        names.sort();
        names.dedup();
        Ok(names)
    }

    /// 批量写入（`do_put` 简易轨）：一个 `RecordBatch` 一次 WAL fsync，逐批回执。
    ///
    /// `batches` 需同 schema；`shard` 默认 [`DEFAULT_SHARD`]。
    /// `idempotency_key` 为 `None` 时自动生成（表可能要求幂等键——
    /// `IngestConfig::standard()` 为 require，缺键会被服务端拒绝）。
    pub async fn insert_batches(
        &self,
        table: &str,
        shard: &str,
        batches: Vec<RecordBatch>,
        idempotency_key: Option<String>,
    ) -> Result<Vec<InsertReceipt>> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        let schema = batches[0].schema();
        let mut msgs = arrow_flight::utils::batches_to_flight_data(schema.as_ref(), batches)?;
        // 首条（schema 消息）携带 descriptor：path = [table, shard]
        msgs[0].flight_descriptor = Some(FlightDescriptor {
            r#type: 1, // PATH
            path: vec![table.to_string(), shard.to_string()],
            cmd: Default::default(),
        });
        // 幂等键随数据消息下发（§7.3）
        let key = idempotency_key.unwrap_or_else(generated_key);
        let meta = serde_json::json!({ "idempotency_key": key })
            .to_string()
            .into_bytes();
        for m in msgs.iter_mut().skip(1) {
            m.app_metadata = meta.clone().into();
        }
        let mut client = self.inner.clone();
        let mut ack = client.do_put(tokio_stream::iter(msgs)).await?.into_inner();
        let mut receipts = Vec::new();
        while let Some(r) = ack.next().await {
            let r = r?;
            let receipt: InsertReceipt = serde_json::from_slice(&r.app_metadata)
                .map_err(|e| ClientError::Decode(format!("put ack: {e}")))?;
            receipts.push(receipt);
        }
        Ok(receipts)
    }

    /// 便捷写入：默认 shard，服务端生成幂等键。
    pub async fn insert(&self, table: &str, batches: Vec<RecordBatch>) -> Result<Vec<InsertReceipt>> {
        self.insert_batches(table, DEFAULT_SHARD, batches, None).await
    }
}

/// 客户端侧幂等键（未显式提供时使用）：`cli-<毫秒>-<pid>-<序号>`。
///
/// 同一进程内单调递增，便于服务端幂等去重与排查（跨进程时间戳区分）。
pub fn generated_key() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!(
        "cli-{ms}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// tonic Status → arrow-flight FlightError（`FlightDataDecoder` 的输入流要求）。
fn to_flight_error(status: tonic::Status) -> FlightError {
    FlightError::Tonic(Box::new(status))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn insert_receipt_parses_engine_json() {
        // 与 ingest::Receipt 的 serde 形态保持一致
        let json = br#"{"table":"cpu","shard":"default","wal_seq":7,"row_count":2,
            "schema_version":1,"expected_visible_at":1700000000000,
            "expected_visible_in_secs":5}"#;
        let r: InsertReceipt = serde_json::from_slice(json).unwrap();
        assert_eq!(r.row_count, 2);
        assert_eq!(r.wal_seq, 7);
        assert_eq!(r.expected_visible_in_secs, 5);
    }

    #[test]
    fn batches_to_flight_data_carries_descriptor_contract() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as arrow::array::ArrayRef,
                Arc::new(StringArray::from(vec![Some("a"), None])) as arrow::array::ArrayRef,
            ],
        )
        .unwrap();
        // 与 server 简易轨约定一致：首条 = schema 消息（可挂 descriptor），其余为数据
        let mut msgs =
            arrow_flight::utils::batches_to_flight_data(schema.as_ref(), vec![batch]).unwrap();
        assert_eq!(msgs.len(), 2);
        msgs[0].flight_descriptor = Some(FlightDescriptor {
            r#type: 1,
            path: vec!["cpu".into(), DEFAULT_SHARD.into()],
            cmd: Default::default(),
        });
        let d = msgs[0].flight_descriptor.as_ref().unwrap();
        assert_eq!(d.path, vec!["cpu".to_string(), "default".to_string()]);
        assert!(!msgs[1].data_body.is_empty());
    }
}
