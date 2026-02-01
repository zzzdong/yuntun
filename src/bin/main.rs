use yuntun::meta::service::MetaService;
use yuntun::meta::catalog_adapter::MetaSchemaProvider;
use yuntun::ingest::ingest_service::IngestService;
use yuntun::query::query_service::QueryService;
use yuntun::store::store_manager::StoreManager;
use yuntun::flight_sql::server::YuntunFlightServer;
use yuntun::pgwire::service::YuntunPgWireServer;
use axum::{extract::{Json, State}, body::Body, http::StatusCode, Router, routing::post};
use anyhow::Result;
use arrow::json::writer::{Writer, LineDelimited};
use std::sync::Arc;
use std::net::SocketAddr;
use futures_util::StreamExt;

#[derive(Debug, Clone)]
struct AppState {
    ingest_service: Arc<IngestService>,
    query_service: Arc<QueryService>,
}

async fn health_check() -> StatusCode {
    StatusCode::OK
}

#[axum::debug_handler]
async fn ingest_handler(State(state): State<AppState>, body: Body) -> (StatusCode, String) {
    let mut bytes = Vec::new();
    let mut body_stream = body.into_data_stream();
    while let Some(chunk) = body_stream.next().await {
        let chunk = chunk.unwrap();
        bytes.extend_from_slice(&chunk);
    }
    let data_str = String::from_utf8_lossy(&bytes);
    
    match state.ingest_service.ingest_line_protocol(&data_str).await {
        Ok(_) => (StatusCode::OK, "Data ingested successfully".to_string()),
        Err(e) => (StatusCode::BAD_REQUEST, format!("Error ingesting data: {}", e)),
    }
}

#[axum::debug_handler]
async fn query_handler(State(state): State<AppState>, Json(query): Json<serde_json::Value>) -> (StatusCode, axum::Json<serde_json::Value>) {
    if let Some(sql) = query.get("sql").and_then(|v| v.as_str()) {
        match state.query_service.execute_sql(sql).await {
            Ok(results) => {
                // 将RecordBatch转换为JSON格式
                let mut json_results = vec![];
                for batch in results {
                    // 使用Writer将RecordBatch转换为JSON
                    let mut writer = Writer::<_, LineDelimited>::new(vec![]);
                    writer.write(&batch).unwrap();
                    writer.finish().unwrap();
                    let json_str = String::from_utf8(writer.into_inner()).unwrap();
                    // 解析JSON字符串为serde_json::Value
                    for line in json_str.lines() {
                        if !line.trim().is_empty() {
                            let json: serde_json::Value = serde_json::from_str(line).unwrap();
                            json_results.push(json);
                        }
                    }
                }
                (StatusCode::OK, axum::Json(serde_json::Value::Array(json_results)))
            }
            Err(e) => (StatusCode::BAD_REQUEST, axum::Json(serde_json::Value::String(format!("Error executing query: {}", e)))),
        }
    } else {
        (StatusCode::BAD_REQUEST, axum::Json(serde_json::Value::String("Missing 'sql' field in request".to_string())))
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // 初始化存储管理器
    let store_manager = Arc::new(StoreManager::new("./storage".to_string()));
    
    // 初始化meta服务
    let meta_service = Arc::new(MetaService::new());
    meta_service.init().await.unwrap();
    
    // 初始化schema提供者
    let schema_provider = Arc::new(MetaSchemaProvider::new(meta_service.clone()));
    
    // 初始化摄入服务
    let ingest_service = Arc::new(IngestService::new(schema_provider.clone(), store_manager.clone()));
    
    // 初始化查询服务
    let query_service = Arc::new(QueryService::new(
        schema_provider.clone(),
        store_manager.clone()
    ).await);
    
    // 为Flight服务器创建查询服务克隆
    let query_service_clone = query_service.clone();
    
    // 初始化应用状态
    let app_state = AppState {
        ingest_service,
        query_service,
    };
    
    // 创建路由
    let app = Router::new()
        .route("/health", post(health_check))
        .route("/ingest", post(ingest_handler))
        .route("/query", post(query_handler))
        .with_state(app_state);
    
    // 启动Flight服务器
    let flight_addr = SocketAddr::from(([127, 0, 0, 1], 50051));
    let flight_server = YuntunFlightServer::new(
        query_service_clone.clone(),
        meta_service.clone(),
        store_manager.clone(),
        flight_addr
    );
    
    // 在后台启动Flight服务器
    tokio::spawn(async move {
        if let Err(e) = flight_server.start().await {
            eprintln!("Error starting Flight server: {}", e);
        }
    });
    
    // 启动PgWire服务器
    let pgwire_addr = SocketAddr::from(([127, 0, 0, 1], 5432));
    let pgwire_server = YuntunPgWireServer::new(
        query_service_clone.clone(),
        meta_service.clone(),
        store_manager.clone(),
        pgwire_addr
    );
    
    // 在后台启动PgWire服务器
    tokio::spawn(async move {
        if let Err(e) = pgwire_server.start().await {
            eprintln!("Error starting PgWire server: {}", e);
        }
    });
    
    // 启动HTTP服务器
    println!("Starting server on http://localhost:8080");
    println!("Health check endpoint: POST http://localhost:8080/health");
    println!("Ingest endpoint: POST http://localhost:8080/ingest");
    println!("Query endpoint: POST http://localhost:8080/query with JSON body: {{\"sql\": \"SELECT * FROM table\"}}");
    println!("Flight endpoint: grpc://localhost:50051");
    println!("PgWire endpoint: postgresql://localhost:5432");
    
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    axum::serve(listener, app.into_make_service()).await
}
