//! **`D-32` 的判据**：`SHOW TABLES [FULL] FROM/IN <db>` 必须按**限定库**过滤。
//!
//! 反证形态（本用例存在的理由）：修好之前，`SHOW TABLES FROM sales` 答的是
//! **当前会话 schema** 的表 —— 连在 `public` 上时永远列出 public 的表。
//! 它**不报错**，只是答非所问 ⇒ 属于最难被发现的那一类（脚本把结果当正确用）。
//!
//! 两条轨都要覆盖（它们各有各的形状，改动点是两处）：
//! - **MySQL wire 轨**（`shim::intercept`）：单列 `Tables_in_<限定库>`；
//! - **Flight / Generic 轨**（`dispatch`）：三列 `table_catalog / table_schema / table_name`
//!   （与 DataFusion `SHOW TABLES` 同形，客户端输出可互换）。

use std::sync::Arc;

use arrow::array::Array as _;
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_model::ops::CreateTableRequest;
use yuntun_query::{LocalCatalog, QueryEngine};
use yuntun_sql::SqlEngine;
use yuntun_sql::session::SessionCtx;
use yuntun_store::{StoreConfig, create_store};

async fn mk_table(catalog: &Arc<MemoryCatalog>, ns: &str, name: &str) {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, true),
    ]));
    catalog
        .create_table(CreateTableRequest {
            name: name.to_string(),
            namespace: ns.to_string(),
            schema,
            partition_cols: vec![],
            default_format: "vortex".to_string(),
            ingest_config: yuntun_model::meta::IngestConfig::standard(),
        })
        .await
        .unwrap_or_else(|e| panic!("建表 {ns}.{name} 失败：{e}"));
}

/// 跑一条 SQL，把第一列收成字符串 Vec（元数据结果都是字符串列）。
async fn first_column(engine: &SqlEngine, sql: &str, session: &mut SessionCtx) -> Vec<String> {
    let out = engine
        .execute(sql, session)
        .await
        .unwrap_or_else(|e| panic!("{sql} 失败：{e}"));
    let yuntun_sql::SqlResult::Rows { batches, .. } = out else {
        panic!("{sql} 应返回结果集");
    };
    let mut names = Vec::new();
    for b in &batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap_or_else(|| {
                panic!(
                    "第一列应是 Utf8，实际 {:?}",
                    b.schema().field(0).data_type()
                )
            });
        for i in 0..col.len() {
            names.push(col.value(i).to_string());
        }
    }
    names
}

/// 结果集的列名（`Tables_in_<db>` 断言用）。
async fn column_names(engine: &SqlEngine, sql: &str, session: &mut SessionCtx) -> Vec<String> {
    let out = engine.execute(sql, session).await.expect("execute");
    let yuntun_sql::SqlResult::Rows { schema, .. } = out else {
        panic!("应返回结果集")
    };
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn show_tables_from_filters_by_the_qualified_schema() {
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.create_schema("sales").await.expect("create schema");
    catalog.create_schema("ops").await.expect("create schema");
    mk_table(&catalog, "sales", "orders").await;
    mk_table(&catalog, "sales", "items").await;
    mk_table(&catalog, "ops", "alerts").await;
    mk_table(&catalog, "public", "cpu").await;

    let engine = SqlEngine::new_readonly(
        Arc::new(QueryEngine::new(
            create_store(&StoreConfig::Memory).expect("memory store"),
            Arc::new(LocalCatalog::new()),
        )),
        catalog.clone(),
    );
    // 会话**停在 public**（未 USE）—— 这正是 bug 的触发条件
    let mut mysql_session = SessionCtx::mysql();
    let mut generic_session = SessionCtx::default();

    // ================= MySQL wire 轨（shim：单列 Tables_in_<db>）=================
    // ① FROM 限定：只答 sales 的（**按表名排序**，list_tables_in 的既有契约）
    assert_eq!(
        first_column(&engine, "SHOW TABLES FROM sales", &mut mysql_session).await,
        vec!["items".to_string(), "orders".to_string()],
        "SHOW TABLES FROM sales 必须只列 sales 的表（旧实现答成 public 的）"
    );
    // ② IN 限定同语义
    assert_eq!(
        first_column(&engine, "SHOW TABLES IN ops", &mut mysql_session).await,
        vec!["alerts".to_string()],
    );
    // ③ 无限定 → 会话 schema（public）
    assert_eq!(
        first_column(&engine, "SHOW TABLES", &mut mysql_session).await,
        vec!["cpu".to_string()],
    );
    // ④ 反证：两个限定库的答案**不同**（否则 ① 可能只是"恰好一样"）
    assert_ne!(
        first_column(&engine, "SHOW TABLES FROM sales", &mut mysql_session).await,
        first_column(&engine, "SHOW TABLES FROM ops", &mut mysql_session).await,
    );
    // ⑤ MySQL 列名跟着限定库走（JDBC/CLI 按列名取值）
    assert_eq!(
        column_names(&engine, "SHOW TABLES FROM sales", &mut mysql_session).await,
        vec!["Tables_in_sales".to_string()],
    );
    // ⑥ 不存在的库 → 空结果（MySQL 同款：不报错）
    assert!(
        first_column(&engine, "SHOW TABLES FROM nope", &mut mysql_session)
            .await
            .is_empty(),
    );
    // ⑦ SHOW FULL TABLES FROM <db> = 两列（MySQL 同款：Table_type）
    let out = engine
        .execute("SHOW FULL TABLES FROM sales", &mut mysql_session)
        .await
        .expect("execute");
    let yuntun_sql::SqlResult::Rows {
        schema, batches, ..
    } = out
    else {
        panic!("应返回结果集")
    };
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        vec!["Tables_in_sales", "Table_type"],
    );
    assert_eq!(batches[0].num_rows(), 2, "sales 两张表");
    assert_eq!(batches[0].num_columns(), 2);
    // Table_type 恒 BASE TABLE（无视图概念）
    let tt = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    for i in 0..tt.len() {
        assert_eq!(tt.value(i), "BASE TABLE");
    }

    // ================= Flight / Generic 轨（dispatch：三列 DataFusion 形）=================
    // ⑧ 同一修复在流式轨也要在：table_schema 列必须是**限定库**
    let out = engine
        .execute("SHOW TABLES FROM sales", &mut generic_session)
        .await
        .expect("execute");
    let yuntun_sql::SqlResult::Rows {
        schema, batches, ..
    } = out
    else {
        panic!("应返回结果集")
    };
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        vec!["table_catalog", "table_schema", "table_name"],
    );
    let schemas = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    for i in 0..schemas.len() {
        assert_eq!(
            schemas.value(i),
            "sales",
            "Generic 轨的 table_schema 必须是限定库"
        );
    }
    // ⑨ 无限定 → 会话 schema（public 的表）：第三列是表名
    let out = engine
        .execute("SHOW TABLES", &mut generic_session)
        .await
        .expect("execute");
    let yuntun_sql::SqlResult::Rows { batches, .. } = out else {
        panic!("应返回结果集")
    };
    let table_names = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let names: Vec<&str> = (0..table_names.len())
        .map(|i| table_names.value(i))
        .collect();
    assert_eq!(names, vec!["cpu"], "无限定 → 会话 schema（public）的表");
}
