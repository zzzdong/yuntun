//! JSON 函数（datafusion-functions-json 0.55）行为回归。
//!
//! 两个关键约定（踩过的坑）：
//! 1. **路径语法不带 `$`**：`json_get_int(doc, 'n')` ✓；`'$.n'` 是 miss（返回
//!    类型默认值/NULL），`json_length(doc)` 不带 path 所以不受影响；
//! 2. `json_get` 返回 Arrow **Union**（JSON 变体联合）——Flight/ADBC 原生可读；
//!    MySQL wire 由 sqlwire::encode 的 Union 分支取底层值按文本输出。

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray, UnionArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;

#[tokio::test]
async fn json_functions_on_utf8_column() {
    let mut ctx = SessionContext::new();
    datafusion_functions_json::register_all(&mut ctx).unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("doc", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![Some(
            r#"{"k": "v", "n": 7}"#,
        )]))],
    )
    .unwrap();
    ctx.register_batch("t", batch).unwrap();

    // ① 类型化取值（路径不带 $）
    let v = scalar_int(&mut ctx, "SELECT json_get_int(doc, 'n') AS v FROM t").await;
    assert_eq!(v, Some(7));
    let s = scalar_str(&mut ctx, "SELECT json_get_str(doc, 'k') AS v FROM t").await;
    assert_eq!(s.as_deref(), Some("v"));

    // ② 长度 / 包含（json_contains 返回 Boolean）
    let v = scalar_int(&mut ctx, "SELECT json_length(doc) AS v FROM t").await;
    assert_eq!(v, Some(2));
    let s = scalar_display(&mut ctx, "SELECT json_contains(doc, 'k') AS v FROM t").await;
    assert!(s == "1" || s == "true", "{s}");

    // ③ json_get → Union：取底层 str 变体的值
    let batches = ctx
        .sql("SELECT json_get(doc, 'k') AS v FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let col = batches[0].column(0);
    assert!(matches!(col.data_type(), DataType::Union(_, _)), "{:?}", col.data_type());
    let u = col.as_any().downcast_ref::<UnionArray>().unwrap();
    let child = u.child(u.type_id(0));
    assert_eq!(
        datafusion::arrow::util::display::array_value_to_string(child.as_ref(), u.value_offset(0))
            .unwrap(),
        "v"
    );

    // ④ miss 语义：类型不匹配返回 NULL（而不是报错）
    let v = scalar_int(&mut ctx, "SELECT json_get_int(doc, 'k') AS v FROM t").await;
    assert_eq!(v, None, "str 键取 int → NULL");
}

async fn scalar_display(ctx: &mut SessionContext, sql: &str) -> String {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let col = batches[0].column(0);
    datafusion::arrow::util::display::array_value_to_string(col, 0).unwrap_or_default()
}

async fn scalar_int(ctx: &mut SessionContext, sql: &str) -> Option<i64> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let col = batches[0].column(0);
    // miss 语义：path 未命中/类型不匹配时可能返回 DataType::Null 列或 null 行
    if *col.data_type() == DataType::Null || col.is_null(0) {
        return None;
    }
    // 命中时 json_get_int 返回 UInt64（miss 可能是 Int64/Null 列），两者都接受
    if let Some(a) = col.as_any().downcast_ref::<datafusion::arrow::array::UInt64Array>() {
        return Some(a.value(0) as i64);
    }
    Some(
        col.as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("int column, got {}", col.data_type()))
            .value(0),
    )
}

async fn scalar_str(ctx: &mut SessionContext, sql: &str) -> Option<String> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let col = batches[0].column(0);
    if *col.data_type() == DataType::Null || col.is_null(0) {
        return None;
    }
    Some(
        col.as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 column")
            .value(0)
            .to_string(),
    )
}
