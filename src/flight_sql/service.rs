//! Arrow Flight service implementation

use std::sync::Arc;
use arrow_flight::{Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket};
use arrow_flight::flight_service_server::FlightService;
use tonic::{Request, Response, Status, Streaming};
use futures::stream::{BoxStream, once, iter};
use futures::StreamExt;
use tonic::codegen::Bytes;

use crate::query::query_service::QueryService;
use crate::meta::service::MetaService;
use crate::store::store_manager::StoreManager;

/// Flight service implementation
#[derive(Debug, Clone)]
pub struct YuntunFlightService {
    query_service: Arc<QueryService>,
    meta_service: Arc<MetaService>,
    store_manager: Arc<StoreManager>,
}

impl YuntunFlightService {
    /// Create a new Flight service
    pub fn new(
        query_service: Arc<QueryService>,
        meta_service: Arc<MetaService>,
        store_manager: Arc<StoreManager>
    ) -> Self {
        Self {
            query_service,
            meta_service,
            store_manager,
        }
    }
}

#[tonic::async_trait]
impl FlightService for YuntunFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(&self, _request: Request<Streaming<HandshakeRequest>>) -> Result<Response<Self::HandshakeStream>, Status> {
        // 简单的握手实现
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: Bytes::from(vec![]),
        };
        
        let stream = once(async move {
            Ok(response)
        }).boxed();
        
        Ok(Response::new(stream))
    }

    async fn list_flights(&self, _request: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {
        // 暂不实现
        Err(Status::unimplemented("list_flights not implemented"))
    }

    async fn get_flight_info(&self, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        // 暂不实现
        Err(Status::unimplemented("get_flight_info not implemented"))
    }

    async fn do_get(&self, _request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        // 暂不实现
        Err(Status::unimplemented("do_get not implemented"))
    }

    async fn do_put(&self, _request: Request<Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        // 暂不实现
        Err(Status::unimplemented("do_put not implemented"))
    }

    async fn do_action(&self, request: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        // 处理SQL查询action
        let action = request.into_inner();
        match action.r#type.as_str() {
            "execute_sql" => {
                // 从payload中获取SQL语句
                let sql = String::from_utf8_lossy(&action.body).to_string();
                
                // 执行SQL查询
                match self.query_service.execute_sql(&sql).await {
                    Ok(batches) => {
                        // 构建结果
                        let result = arrow_flight::Result {
                            body: Bytes::from(format!("Executed query successfully, returned {} batches", batches.len()).as_bytes().to_vec()),
                        };
                        
                        let stream = once(async move {
                            Ok(result)
                        }).boxed();
                        
                        Ok(Response::new(stream))
                    }
                    Err(e) => {
                        let result = arrow_flight::Result {
                            body: Bytes::from(format!("Error executing query: {}", e).as_bytes().to_vec()),
                        };
                        
                        let stream = once(async move {
                            Ok(result)
                        }).boxed();
                        
                        Ok(Response::new(stream))
                    }
                }
            }
            _ => {
                Err(Status::invalid_argument(format!("Unknown action type: {}", action.r#type)))
            }
        }
    }

    async fn list_actions(&self, _request: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        // 列出支持的action类型
        let actions = vec![
            ActionType {
                r#type: "execute_sql".to_string(),
                description: "Execute SQL query".to_string(),
            },
        ];
        
        let stream = iter(actions.into_iter().map(Ok)).boxed();
        Ok(Response::new(stream))
    }

    async fn get_schema(&self, _request: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        // 暂不实现
        Err(Status::unimplemented("get_schema not implemented"))
    }

    async fn poll_flight_info(&self, _request: Request<FlightDescriptor>) -> Result<Response<arrow_flight::PollInfo>, Status> {
        // 暂不实现
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }

    async fn do_exchange(&self, _request: Request<Streaming<FlightData>>) -> Result<Response<Self::DoExchangeStream>, Status> {
        // 暂不实现
        Err(Status::unimplemented("do_exchange not implemented"))
    }
}