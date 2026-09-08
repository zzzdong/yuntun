use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, UInt64Builder, TimestampNanosecondBuilder, ArrayBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use influxdb_line_protocol::parse_lines;
use std::collections::HashMap;
use std::sync::Arc;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use super::super::ingest_service::IngestService;

#[derive(Debug, Clone, Copy)]
pub enum ValueType {
    F64,
    I64,
    U64,
    String,
    Boolean,
}

#[derive(Debug, Clone)]
pub enum FieldValue {
    F64(f64),
    I64(i64),
    U64(u64),
    String(String),
    Boolean(bool),
}

#[derive(Debug, Clone)]
pub struct FieldData {
    pub name: String,
    pub value: FieldValue,
}

#[derive(Debug, Clone)]
pub struct ParsedLine {
    pub measurement: String,
    pub tags: HashMap<String, String>,
    pub fields: Vec<FieldData>,
    pub timestamp: i64,
}



/// 从 influxdb_line_protocol::FieldValue 转换到我们的 FieldValue
fn convert_field_value(fv: &influxdb_line_protocol::FieldValue) -> FieldValue {
    match fv {
        influxdb_line_protocol::FieldValue::F64(v) => FieldValue::F64(*v),
        influxdb_line_protocol::FieldValue::I64(v) => FieldValue::I64(*v),
        influxdb_line_protocol::FieldValue::U64(v) => FieldValue::U64(*v),
        influxdb_line_protocol::FieldValue::String(s) => FieldValue::String(s.to_string()),
        influxdb_line_protocol::FieldValue::Boolean(b) => FieldValue::Boolean(*b),
    }
}

/// 从多行 line protocol 数据创建 RecordBatch（按 measurement 分组）
pub fn from_lines(lines: &str) -> HashMap<String, RecordBatch> {
    // 解析所有行到 ParsedLine 向量中
    let mut parsed_lines = Vec::new();
    let mut parser = parse_lines(lines);
    
    while let Some(result) = parser.next() {
        if let Ok(parsed_line) = result {
            // 提取 measurement
            let measurement = parsed_line.series.measurement.to_string();
            
            // 提取 tags
            let mut tags = HashMap::new();
            if let Some(tag_set) = parsed_line.series.tag_set {
                for tag in tag_set {
                    tags.insert(tag.0.to_string(), tag.1.to_string());
                }
            }
            
            // 提取 fields
            let mut fields = Vec::new();
            for (name, value) in &parsed_line.field_set {
                fields.push(FieldData {
                    name: name.to_string(),
                    value: convert_field_value(value),
                });
            }
            
            // 提取 timestamp
            let timestamp = parsed_line.timestamp.unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as i64
            });
            
            parsed_lines.push(ParsedLine {
                measurement,
                tags,
                fields,
                timestamp,
            });
        }
    }
    
    if parsed_lines.is_empty() {
        return HashMap::new();
    }

    // 按 measurement 分组
    let mut grouped: HashMap<String, Vec<ParsedLine>> = HashMap::new();
    for line in parsed_lines {
        grouped.entry(line.measurement.clone()).or_default().push(line);
    }

    // 为每个 measurement 创建 RecordBatch
    let mut batches: HashMap<String, RecordBatch> = HashMap::new();
    
    for (measurement, lines) in grouped {
        // 收集所有唯一的 tag 键和 field 键
        let mut all_tag_keys = std::collections::HashSet::new();
        let mut all_field_keys = std::collections::HashSet::new();
        
        for line in &lines {
            for tag_key in line.tags.keys() {
                all_tag_keys.insert(tag_key.clone());
            }
            for field in &line.fields {
                all_field_keys.insert(field.name.clone());
            }
        }
        
        // 将 HashSet 转换为排序后的 Vec，确保列顺序一致
        let mut tag_keys: Vec<String> = all_tag_keys.into_iter().collect();
        tag_keys.sort();
        let mut field_keys: Vec<String> = all_field_keys.into_iter().collect();
        field_keys.sort();
        
        // 构建 schema
        let mut fields_vec = vec![
            Field::new("time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        ];
        
        // 添加标签字段
        for tag_key in &tag_keys {
            fields_vec.push(Field::new(format!("tag_{}", tag_key), DataType::Utf8, true));
        }
        
        // 添加字段列
        for field_key in &field_keys {
            // 我们需要确定字段类型。由于同一字段在不同行可能有不同类型，
            // 但 InfluxDB line protocol 要求同一字段类型一致。我们取第一行的类型。
            let field_type = lines.iter()
                .find_map(|line| line.fields.iter().find(|f| &f.name == field_key))
                .map(|field| match field.value {
                    FieldValue::F64(_) => DataType::Float64,
                    FieldValue::I64(_) => DataType::Int64,
                    FieldValue::U64(_) => DataType::UInt64,
                    FieldValue::String(_) => DataType::Utf8,
                    FieldValue::Boolean(_) => DataType::Boolean,
                })
                .unwrap_or(DataType::Utf8); // 默认类型
            
            fields_vec.push(Field::new(field_key, field_type, true));
        }
        
        let schema = Arc::new(Schema::new(fields_vec));
        
        // 构建数组
        let mut time_array = TimestampNanosecondBuilder::with_capacity(lines.len());
        let mut tag_arrays: HashMap<String, StringBuilder> = HashMap::new();
        let mut field_arrays: HashMap<String, Box<dyn arrow::array::ArrayBuilder>> = HashMap::new();
        
        // 初始化数组构建器
        for tag_key in &tag_keys {
            tag_arrays.insert(tag_key.clone(), StringBuilder::new());
        }
        
        for field_key in &field_keys {
            let builder: Box<dyn arrow::array::ArrayBuilder> = match lines.iter()
                .find_map(|line| line.fields.iter().find(|f| &f.name == field_key))
                .map(|field| match field.value {
                    FieldValue::F64(_) => Box::new(Float64Builder::with_capacity(lines.len())) as Box<dyn arrow::array::ArrayBuilder>,
                    FieldValue::I64(_) => Box::new(Int64Builder::with_capacity(lines.len())) as Box<dyn arrow::array::ArrayBuilder>,
                    FieldValue::U64(_) => Box::new(UInt64Builder::with_capacity(lines.len())) as Box<dyn arrow::array::ArrayBuilder>,
                    FieldValue::String(_) => Box::new(StringBuilder::new()) as Box<dyn arrow::array::ArrayBuilder>,
                    FieldValue::Boolean(_) => Box::new(BooleanBuilder::with_capacity(lines.len())) as Box<dyn arrow::array::ArrayBuilder>,
                }) {
                Some(builder) => builder,
                None => Box::new(StringBuilder::new()) as Box<dyn arrow::array::ArrayBuilder>,
            };
            field_arrays.insert(field_key.clone(), builder);
        }
        
        // 填充数据
        for line in &lines {
            time_array.append_value(line.timestamp);
            
            // 填充标签值
            for tag_key in &tag_keys {
                if let Some(builder) = tag_arrays.get_mut(tag_key) {
                    if let Some(value) = line.tags.get(tag_key) {
                        builder.append_value(value);
                    } else {
                        builder.append_null();
                    }
                }
            }
            
            // 填充字段值
            for field_key in &field_keys {
                if let Some(builder) = field_arrays.get_mut(field_key) {
                    if let Some(field) = line.fields.iter().find(|f| &f.name == field_key) {
                        match (&field.value, builder.as_any_mut()) {
                            (FieldValue::F64(v), b) => {
                                let b = b.downcast_mut::<Float64Builder>().unwrap();
                                b.append_value(*v);
                            }
                            (FieldValue::I64(v), b) => {
                                let b = b.downcast_mut::<Int64Builder>().unwrap();
                                b.append_value(*v);
                            }
                            (FieldValue::U64(v), b) => {
                                let b = b.downcast_mut::<UInt64Builder>().unwrap();
                                b.append_value(*v);
                            }
                            (FieldValue::String(v), b) => {
                                let b = b.downcast_mut::<StringBuilder>().unwrap();
                                b.append_value(v);
                            }
                            (FieldValue::Boolean(v), b) => {
                                let b = b.downcast_mut::<BooleanBuilder>().unwrap();
                                b.append_value(*v);
                            }
                        }
                    } else {
                        // 该行没有此字段，追加 null
                        match builder.as_any_mut() {
                            b if b.is::<Float64Builder>() => {
                                b.downcast_mut::<Float64Builder>().unwrap().append_null();
                            }
                            b if b.is::<Int64Builder>() => {
                                b.downcast_mut::<Int64Builder>().unwrap().append_null();
                            }
                            b if b.is::<UInt64Builder>() => {
                                b.downcast_mut::<UInt64Builder>().unwrap().append_null();
                            }
                            b if b.is::<StringBuilder>() => {
                                b.downcast_mut::<StringBuilder>().unwrap().append_null();
                            }
                            b if b.is::<BooleanBuilder>() => {
                                b.downcast_mut::<BooleanBuilder>().unwrap().append_null();
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        
        // 构建所有数组
        let mut arrays: Vec<ArrayRef> = vec![
            Arc::new(time_array.finish()),
        ];
        
        // 添加标签数组
        for tag_key in &tag_keys {
            if let Some(mut builder) = tag_arrays.remove(tag_key) {
                arrays.push(Arc::new(builder.finish()));
            }
        }
        
        // 添加字段数组
        for field_key in &field_keys {
            if let Some(mut builder) = field_arrays.remove(field_key) {
                arrays.push(builder.finish());
            }
        }
        
        // 创建 RecordBatch
        match RecordBatch::try_new(schema.clone(), arrays) {
            Ok(batch) => {
                batches.insert(measurement, batch);
            }
            Err(e) => {
                eprintln!("Error creating RecordBatch for measurement {}: {}", measurement, e);
            }
        }
    }

    batches
}

/// 启动 HTTP 服务器监听 InfluxDB line protocol 数据
pub async fn start_server(
    addr: &str,
    ingest_service: Arc<IngestService>,
) -> anyhow::Result<()> {
    // 创建路由
    let app = Router::new()
        .route("/write", post(handle_write))
        .route("/write/{:db}", post(handle_write_db))
        .route("/ping", axum::routing::get(handle_ping))
        .with_state(ingest_service);

    // 启动服务器
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("InfluxDB line protocol HTTP server listening on {}", addr);
    
    axum::serve(listener, app).await?;
    
    Ok(())
}

/// 处理 /write 端点
async fn handle_write(
    State(ingest_service): State<Arc<IngestService>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    handle_write_impl(ingest_service, params, headers, body).await
}

/// 处理 /write/:db 端点
async fn handle_write_db(
    Path(db): Path<String>,
    State(ingest_service): State<Arc<IngestService>>,
    Query(mut params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    // 将数据库路径参数添加到查询参数中
    params.insert("db".to_string(), db);
    handle_write_impl(ingest_service, params, headers, body).await
}

/// 实际的写入处理实现
async fn handle_write_impl(
    ingest_service: Arc<IngestService>,
    params: HashMap<String, String>,
    headers: HeaderMap,
    body: String,
) -> Response {
    // 检查必需的参数
    let db = match params.get("db") {
        Some(db) => db,
        None => {
            return (StatusCode::BAD_REQUEST, "Missing 'db' parameter").into_response();
        }
    };

    // 获取精度参数（可选）
    let precision = params.get("precision").cloned().unwrap_or_else(|| "ns".to_string());

    // 记录请求信息（用于调试）
    println!("Received write request for database: {}, precision: {}", db, precision);
    
    // 检查认证头部（可选）
    if let Some(auth_header) = headers.get("authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            println!("Authorization header present: {}", auth_str);
            // 这里可以添加令牌验证逻辑
        }
    }

    // 解析 line protocol 数据
    let batches = from_lines(&body);

    if batches.is_empty() {
        println!("No valid line protocol data found in request body");
        return StatusCode::NO_CONTENT.into_response();
    }

    println!("Successfully parsed {} record batches", batches.len());

    // 调用摄入服务处理数据
    match ingest_service.ingest_batches(db, batches).await {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("Error ingesting data: {}", e)).into_response(),
    }
}

/// 处理 /ping 端点（健康检查）
async fn handle_ping() -> impl IntoResponse {
    (StatusCode::OK, "pong")
}