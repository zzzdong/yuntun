use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::array::{ArrayRef, StringArray, TimestampNanosecondArray, Float64Array};
use std::collections::HashMap;
use std::sync::Arc;

/// 解析influx Line protocol格式的数据，转换为RecordBatch
pub fn parse_line_protocol(line: &str) -> Result<(String, HashMap<String, String>, HashMap<String, f64>, i64), anyhow::Error> {
    // 简化处理，暂时使用硬编码的解析逻辑
    // 实际项目中应该使用公开的API或自己实现解析
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 2 {
        return Err(anyhow::anyhow!("Invalid line protocol format"));
    }
    
    let measurement_part = parts[0];
    let field_part = parts[1];
    let timestamp_part = if parts.len() > 2 {
        parts[2].parse::<i64>().ok()
    } else {
        None
    };
    
    // 解析measurement和tags
    let (measurement, tags) = if let Some((meas, tag_str)) = measurement_part.split_once(',') {
        let mut tag_map = HashMap::new();
        for tag in tag_str.split(',') {
            if let Some((k, v)) = tag.split_once('=') {
                tag_map.insert(k.to_string(), v.to_string());
            }
        }
        (meas.to_string(), tag_map)
    } else {
        (measurement_part.to_string(), HashMap::new())
    };
    
    // 解析fields
    let mut fields = HashMap::new();
    for field in field_part.split(',') {
        if let Some((k, v)) = field.split_once('=') {
            if let Ok(value) = v.parse::<f64>() {
                fields.insert(k.to_string(), value);
            }
        }
    }
    
    // 解析timestamp
    let timestamp = timestamp_part.unwrap_or_else(|| std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64);
    
    Ok((measurement, tags, fields, timestamp))
}

/// 将解析后的数据转换为RecordBatch
pub fn to_record_batch(
    measurement: &str,
    tags: &HashMap<String, String>,
    fields: &HashMap<String, f64>,
    timestamp: i64,
) -> RecordBatch {
    // 构建schema
    let mut fields_vec = vec![
        Field::new("measurement", DataType::Utf8, false),
        Field::new("time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
    ];
    
    // 添加标签字段
    for (key, _) in tags {
        fields_vec.push(Field::new(format!("tag_{}", key), DataType::Utf8, true));
    }
    
    // 添加字段
    for (key, _) in fields {
        fields_vec.push(Field::new(key, DataType::Float64, true));
    }
    
    let schema = Arc::new(Schema::new(fields_vec));
    
    // 构建数组
    let mut arrays: Vec<ArrayRef> = vec![];
    
    // measurement
    arrays.push(Arc::new(StringArray::from(vec![measurement])));
    
    // time
    arrays.push(Arc::new(TimestampNanosecondArray::from(vec![timestamp])));
    
    // 标签值
    for (_, value) in tags {
        // 创建包含单个字符串的StringArray
        let value_str = value.clone();
        arrays.push(Arc::new(StringArray::from(vec![value_str])));
    }
    
    // 字段值
    for (_, value) in fields {
        arrays.push(Arc::new(Float64Array::from(vec![*value])));
    }
    
    RecordBatch::try_new(schema, arrays).unwrap()
}

/// 从多行line protocol数据创建RecordBatch
pub fn from_lines(lines: &str) -> Vec<RecordBatch> {
    let mut batches = vec![];
    
    for line in lines.lines() {
        if line.trim().is_empty() {
            continue;
        }
        
        if let Ok((measurement, tags, fields, timestamp)) = parse_line_protocol(line) {
            let batch = to_record_batch(&measurement, &tags, &fields, timestamp);
            batches.push(batch);
        }
    }
    
    batches
}
