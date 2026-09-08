//! Arrow Flight Source（MVP 唯一写入协议，ADR-13 / T1.3）+ SQL 查询出口（do_get）。
//!
//! DoPut 约定：
//! - FlightDescriptor path = `[table, shard]`（shard_key 由客户端指定，§3.1）
//! - 首条 FlightData 携带 Arrow Schema；后续每条为 RecordBatch
//! - app_metadata（按批）可选传 JSON `{"idempotency_key": "..."}`（§7.3）
//!
//! DoGet 约定（外部查询出口，阶段 0.5 补齐）：
//! - `Ticket.ticket` = UTF-8 SQL 语句（只读）
//! - 响应为 IPC 流：首条 Schema 消息 + 数据消息
//! - `get_flight_info`：`descriptor.cmd` = SQL → 返回带相同 Ticket 的 endpoint
//!   （兼容"必须先 GetFlightInfo"的客户端生态，如 pyarrow.flight）

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, Location, PollInfo, PutResult, SchemaAsIpc, SchemaResult,
    Ticket,
};
use futures::StreamExt;
use yuntun_model::error::LakeError;
use yuntun_model::IngestBatch;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status, Streaming};

/// Flight → Ingestor 桥接服务。
/// DoPut 调用 [`crate::pipeline::Ingestor::ingest`]（严格时序 + WAL 回执）；
/// DoGet 委托 [`FlightQueryHook`]（DataFusion SQL）。
pub struct FlightIngestService {
    pub ingest: Arc<dyn FlightIngestHook>,
    /// 查询钩子：None 时 do_get 返回 unimplemented
    pub query: Option<Arc<dyn FlightQueryHook>>,
}

/// DoPut → Ingestor 的解耦钩子（server crate 注入真实 Ingestor，测试注入 mock）。
#[async_trait::async_trait]
pub trait FlightIngestHook: Send + Sync + 'static {
    async fn ingest(&self, batch: IngestBatch) -> Result<crate::source::Receipt, LakeError>;
}

/// DoGet → QueryEngine 的解耦钩子（只读 SQL）。
#[async_trait::async_trait]
pub trait FlightQueryHook: Send + Sync + 'static {
    async fn query(&self, sql: &str) -> Result<Vec<arrow::record_batch::RecordBatch>, LakeError>;
}

impl FlightIngestService {
    pub fn new(ingest: Arc<dyn FlightIngestHook>) -> Self {
        Self { ingest, query: None }
    }

    /// 挂载查询钩子（server 装配时调用）。
    pub fn with_query(mut self, query: Arc<dyn FlightQueryHook>) -> Self {
        self.query = Some(query);
        self
    }
}

fn status(e: LakeError) -> Status {
    match e {
        LakeError::SchemaIncompatible(_) => Status::invalid_argument(e.to_string()),
        LakeError::IdempotencyKeyRequired => Status::failed_precondition(e.to_string()),
        LakeError::IdempotencyKeyTooLong => Status::invalid_argument(e.to_string()),
        LakeError::TableNotFound(_) => Status::not_found(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}

#[tonic::async_trait]
impl FlightService for FlightIngestService {
    type HandshakeStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<HandshakeResponse, Status>> + Send>,
    >;
    type DoGetStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoPutStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<PutResult, Status>> + Send>>;
    type DoActionStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<arrow_flight::Result, Status>> + Send>>;
    type ListActionsStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<ActionType, Status>> + Send>>;
    type ListFlightsStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<FlightInfo, Status>> + Send>>;
    type DoExchangeStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not required (phase 0)"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        // 支持：descriptor.cmd = SQL → 返回结果集 schema（不发数据）
        let desc = request.into_inner();
        let sql = sql_from_descriptor(&desc)
            .ok_or_else(|| Status::invalid_argument("descriptor.cmd must carry SQL"))?;
        let Some(q) = &self.query else {
            return Err(Status::unimplemented("query not enabled"));
        };
        // limit 0：只拿 schema
        let sql_limited = format!("SELECT * FROM ({sql}) LIMIT 0");
        let batches = q.query(&sql_limited).await.map_err(status)?;
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let fd: FlightData = SchemaAsIpc::new(schema.as_ref(), &options)
            .try_into()
            .map_err(|e| Status::internal(format!("schema encode: {e}")))?;
        Ok(Response::new(SchemaResult {
            schema: fd.data_header,
        }))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let sql = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|_| Status::invalid_argument("ticket must be UTF-8 SQL"))?;
        if sql.trim().is_empty() {
            return Err(Status::invalid_argument("empty SQL in ticket"));
        }
        let Some(q) = &self.query else {
            return Err(Status::unimplemented("query not enabled"));
        };
        let batches = q.query(&sql).await.map_err(status)?;

        // IPC 流：首条 Schema 消息 + 数据消息（阶段 0 全量缓冲；流式 chunking 留阶段 1）
        let mut flights: Vec<Result<FlightData, Status>> = Vec::new();
        if batches.is_empty() {
            // 空结果也必须给 schema（客户端依赖）
            let schema = Arc::new(arrow::datatypes::Schema::empty());
            let options = arrow::ipc::writer::IpcWriteOptions::default();
            let fd: FlightData = SchemaAsIpc::new(schema.as_ref(), &options)
                .try_into()
                .expect("infallible: SchemaAsIpc -> FlightData");
            flights.push(Ok(fd));
        } else {
            let schema = batches[0].schema();
            let datas = arrow_flight::utils::batches_to_flight_data(
                schema.as_ref(),
                batches,
            )
            .map_err(|e| Status::internal(format!("flight encode: {e}")))?;
            flights.extend(datas.into_iter().map(Ok));
        }
        Ok(Response::new(Box::pin(tokio_stream::iter(flights))))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut stream = request.into_inner();

        // ---- 首条消息：descriptor + schema ----
        let first = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty do_put stream"))??;

        let (table, shard) = match &first.flight_descriptor {
            Some(d) => crate::source::extract_table_shard(&d.path).map_err(|e| status(e))?,
            None => return Err(Status::invalid_argument("missing flight descriptor")),
        };

        // 幂等键按批次解析（app_metadata 每条消息均可携带，§7.3）
        let schema = flight_schema_of(&first)
            .ok_or_else(|| Status::invalid_argument("first FlightData must carry schema"))?;

        let (tx, rx) = mpsc::channel::<Result<PutResult, Status>>(64);
        let hook = self.ingest.clone();

        tokio::spawn(async move {
            let mut dict_ids: HashMap<i64, arrow::array::ArrayRef> = HashMap::new();
            let ack_stream = tx.clone();
            // 逐批处理：schema 解析 → WAL fsync → 回执
            loop {
                let fd = tokio::select! {
                    f = stream.next() => match f {
                        Some(Ok(fd)) => fd,
                        Some(Err(s)) => { let _ = ack_stream.send(Err(s)).await; break; }
                        None => break,
                    },
                    _ = ack_stream.closed() => break,
                };
                if fd.data_header.is_empty() && fd.data_body.is_empty() {
                    continue;
                }
                let idempotency_key = parse_idempotency_key(&fd.app_metadata);
                let batch = match arrow_flight::utils::flight_data_to_arrow_batch(
                    &fd,
                    schema.clone(),
                    &mut dict_ids,
                ) {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = ack_stream
                            .send(Err(Status::invalid_argument(format!("decode batch: {e}"))))
                            .await;
                        break;
                    }
                };

                let ib = IngestBatch {
                    table: table.clone(),
                    shard_key: shard.clone(),
                    record_batch: batch,
                    idempotency_key: idempotency_key.clone(),
                    received_at: SystemTime::now(),
                };
                match hook.ingest(ib).await {
                    Ok(receipt) => {
                        let payload = serde_json::to_vec(&receipt)
                            .unwrap_or_default();
                        if ack_stream
                            .send(Ok(PutResult {
                                app_metadata: payload.into(),
                            }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = ack_stream.send(Err(status(e))).await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not implemented (phase 0)"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not implemented (phase 0)"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights not implemented (phase 0)"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented (phase 0)"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // 约定：descriptor.cmd = SQL → 返回一个 endpoint（Ticket = 相同 SQL）
        let desc = request.into_inner();
        let sql = sql_from_descriptor(&desc)
            .ok_or_else(|| Status::invalid_argument("descriptor.cmd must carry SQL"))?;
        let info = FlightInfo {
            schema: Default::default(), // 客户端可再 get_schema 取精确 schema
            flight_descriptor: Some(FlightDescriptor {
                r#type: 2, // CMD
                cmd: sql.clone().into_bytes().into(),
                path: vec![],
            }),
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: sql.into_bytes().into(),
                }),
                location: vec![Location {
                    uri: String::new(), // 空位置 = 使用本服务
                }],
                expiration_time: None,
                app_metadata: Default::default(),
            }],
            total_records: -1, // 未知（Manifest 延迟统计）
            total_bytes: -1,
            ordered: false,
            app_metadata: Default::default(),
        };
        Ok(Response::new(info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not implemented (phase 0)"))
    }
}

/// 从首条 FlightData 的 data_header 解析 Arrow Schema（IPC Message → Schema）。
fn flight_schema_of(fd: &FlightData) -> Option<arrow::datatypes::SchemaRef> {
    let ipc = arrow::ipc::root_as_message(&fd.data_header).ok()?;
    let fb_schema = ipc.header_as_schema()?;
    let schema = arrow::ipc::convert::fb_to_schema(fb_schema);
    Some(Arc::new(schema))
}

fn parse_idempotency_key(meta: &[u8]) -> Option<String> {
    if meta.is_empty() {
        return None;
    }
    #[derive(serde::Deserialize)]
    struct M {
        #[serde(rename = "idempotency_key")]
        idempotency_key: Option<String>,
    }
    serde_json::from_slice::<M>(meta).ok()?.idempotency_key
}

/// 从 descriptor 提取 SQL：`cmd`（UTF-8）或单元素 `path`。
fn sql_from_descriptor(desc: &FlightDescriptor) -> Option<String> {
    if !desc.cmd.is_empty() {
        return String::from_utf8(desc.cmd.to_vec()).ok().map(|s| s.trim().to_string());
    }
    match desc.path.as_slice() {
        [one] if !one.is_empty() => Some(one.clone()),
        _ => None,
    }
}

/// 供 server 装配：把 schema 响应编码为 SchemaResult。
pub fn schema_result_of(schema: &arrow::datatypes::SchemaRef) -> SchemaResult {
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let fd: FlightData = SchemaAsIpc::new(schema, &options)
        .try_into()
        .unwrap_or_else(|_| FlightData::default());
    SchemaResult {
        schema: fd.data_header,
    }
}
