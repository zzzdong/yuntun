//! SQL 处理层（sql-access-design.md §三/§四，v13）：SQL 语义的**唯一实现**。
//!
//! 三种访问协议（Flight SQL / MySQL wire / 后续 PG wire）共用本层：
//! - 前置分流（v12.4，operation-log §9）：Query→query / Insert→ingest /
//!   DDL·Show→catalog，只有 SELECT 让 DataFusion 处理；
//! - prepare / execute_prepared：参数绑定（AST 级替换，无注入面）；
//! - 元数据 API（tables/columns，catalog 支撑）——wire 方言 shim 的数据出口；
//! - G2 修复：DataFusion 会话 default catalog/schema = yuntun/public，
//!   非限定表名 `FROM t` 对 wire 客户端直接可用。
//!
//! **边界**：无 listener、无 wire 格式、无连接概念——会话（方言 / prepared
//! 语句表）由协议适配层持有，以 [`SessionCtx`] 显式传入（裁决 S-2/S-4）。

pub mod params;
pub mod session;
pub mod shim;
pub mod sql;

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use session::{SessionCtx, SqlDialect};
use yuntun_catalog::CatalogOps;
use yuntun_ingest::Ingestor;
use yuntun_model::error::LakeError;
use yuntun_model::meta::serialize_schema;
use yuntun_model::ops::CreateTableRequest;
use yuntun_model::wal_record::{ddl_op, DdlPayload};
pub use params::SqlValue;
use yuntun_query::QueryEngine;

/// SQL 执行错误（协议适配层负责映射到各自的回执：
/// MySQL ERROR 码 / Flight Status / 未来 PG SQLSTATE）。
#[derive(Debug, Clone, thiserror::Error)]
pub enum SqlError {
    #[error("SQL parse error: {0}")]
    Parse(String),
    /// 明确拒绝（不支持语句 / 类型），附支持列表提示
    #[error("{0}")]
    Unsupported(String),
    #[error("table not found: {0}")]
    NotFound(String),
    #[error("table already exists: {0}")]
    TableExists(String),
    /// 前置条件不满足（如强制幂等键缺失）
    #[error("{0}")]
    Precondition(String),
    /// 只读节点拒绝写入（sqld readonly）
    #[error("SQL server is configured read-only")]
    ReadOnly,
    /// 瞬时/内部错误（WAL / S3 / Catalog 超时等，客户端可重试）
    #[error("{0}")]
    Internal(String),
}

impl SqlError {
    /// LakeError → SqlError（§10.1 客户端错误 / 瞬时错误分类对齐）。
    pub fn from_lake(e: LakeError) -> Self {
        match e {
            LakeError::TableNotFound(t) => SqlError::NotFound(t),
            LakeError::TableAlreadyExists(t) => SqlError::TableExists(t),
            LakeError::IdempotencyKeyRequired => {
                SqlError::Precondition("idempotency key is required for this table".into())
            }
            LakeError::IdempotencyKeyTooLong => {
                SqlError::Internal("idempotency key too long".into())
            }
            other => SqlError::Internal(other.to_string()),
        }
    }
}

/// 一次 SQL 执行的结果（协议适配层据此编码行 / 回执）。
pub enum SqlResult {
    /// 结果集：schema 恒有值（空结果集也返回查询 schema，S1.5 一致性语义）
    Rows { schema: SchemaRef, batches: Vec<RecordBatch> },
    /// 受影响行数（INSERT / DDL / shim no-op）
    Affected(i64),
}

/// 节点写策略（§6.2：sqld readonly 注入 ReadOnly；分流命中写语句即拒绝）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WritePolicy {
    AllowWrite,
    ReadOnly,
}

/// 预编译语句（prepare 缓存 AST；execute_prepared 参数替换后走同一分流）。
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub sql: String,
    pub statement: sqlparser::ast::Statement,
    pub param_count: usize,
    pub dialect: SqlDialect,
}

/// SQL 执行引擎：编排 query / ingest / catalog 三项能力（无自身状态）。
pub struct SqlEngine {
    pub(crate) ingest: Arc<Ingestor>,
    pub(crate) query: Arc<QueryEngine>,
    pub(crate) catalog: Arc<dyn CatalogOps>,
    pub write_policy: WritePolicy,
}

impl SqlEngine {
    pub fn new(
        ingest: Arc<Ingestor>,
        query: Arc<QueryEngine>,
        catalog: Arc<dyn CatalogOps>,
    ) -> Self {
        Self {
            ingest,
            query,
            catalog,
            write_policy: WritePolicy::AllowWrite,
        }
    }

    pub fn with_write_policy(mut self, policy: WritePolicy) -> Self {
        self.write_policy = policy;
        self
    }

    /// 一次性执行（简单协议：MySQL COM_QUERY 文本 / PG simple Query /
    /// Flight 简易轨与 StatementUpdate）。
    pub async fn execute(&self, sql: &str, session: &mut SessionCtx) -> Result<SqlResult, SqlError> {
        let stmt = sql::parse_single_with(session.dialect.parser_dialect(), sql)
            .map_err(|e| SqlError::Parse(e.to_string()))?;
        // 方言 shim：wire 客户端的非数据语句（SHOW/SET/USE/@@var/事务 no-op）
        if let Some(res) = shim::intercept(&stmt, session, self).await? {
            return Ok(res);
        }
        let outcome = self.dispatch(stmt).await?;
        Ok(self.finish(outcome, sql).await)
    }

    /// 预编译：方言感知解析 + 占位符计数 + 列 schema（尽力而为）。
    pub async fn prepare(&self, sql: &str, session: &SessionCtx) -> Result<PreparedStatement, SqlError> {
        let mut stmt = sql::parse_single_with(session.dialect.parser_dialect(), sql)
            .map_err(|e| SqlError::Parse(e.to_string()))?;
        let param_count = params::count_placeholders(&mut stmt);
        Ok(PreparedStatement {
            sql: sql.to_string(),
            statement: stmt,
            param_count,
            dialect: session.dialect,
        })
    }

    /// 绑定执行：参数 AST 级替换 → 渲染 → 与 execute 同一分流（shim 仍生效）。
    pub async fn execute_prepared(
        &self,
        stmt: &PreparedStatement,
        params: &[SqlValue],
        session: &mut SessionCtx,
    ) -> Result<SqlResult, SqlError> {
        let mut ast = stmt.statement.clone();
        params::substitute(&mut ast, params).map_err(SqlError::from_lake)?;
        let rendered = ast.to_string();
        self.execute(&rendered, session).await
    }

    /// 元数据 API：表名清单（SHOW TABLES / wire 元数据探测的数据出口）。
    pub async fn list_tables(&self) -> Result<Vec<String>, SqlError> {
        let tables = self.catalog.list_tables().await.map_err(SqlError::from_lake)?;
        let mut names: Vec<String> = tables.into_iter().map(|t| t.name).collect();
        names.sort();
        Ok(names)
    }

    /// 元数据 API：表列描述（SHOW COLUMNS / DESCRIBE / wire RowDescription）。
    pub async fn describe_table(&self, name: &str) -> Result<Option<TableDesc>, SqlError> {
        let Some(meta) = self.catalog.get_table(name).await.map_err(SqlError::from_lake)? else {
            return Ok(None);
        };
        let schema = meta.schema().map_err(SqlError::from_lake)?;
        let columns = schema
            .fields()
            .iter()
            .map(|f| ColumnDesc {
                name: f.name().clone(),
                mysql_type: Self::arrow_to_mysql_type(f.data_type()).to_string(),
                nullable: f.is_nullable(),
            })
            .collect();
        Ok(Some(TableDesc {
            name: meta.name,
            columns,
            schema,
        }))
    }

    /// 逻辑计划 schema（S1.5 一致性语义：空结果集也返回查询 schema）。
    pub async fn schema_of(&self, sql: &str) -> Option<SchemaRef> {
        self.query
            .schema_of(sql)
            .await
            .ok()
            .filter(|s| !s.fields().is_empty())
    }

    /// 结果集收尾：空批次时回填查询 schema（G7 兼容：与 GetFlightInfo 一致）。
    async fn finish(&self, outcome: RawOutcome, sql: &str) -> SqlResult {
        match outcome {
            RawOutcome::Rows(b) if !b.is_empty() => SqlResult::Rows {
                schema: b[0].schema(),
                batches: b,
            },
            RawOutcome::Rows(_) => SqlResult::Rows {
                schema: self
                    .schema_of(sql)
                    .await
                    .unwrap_or_else(|| Arc::new(Schema::empty())),
                batches: Vec::new(),
            },
            RawOutcome::Affected(n) => SqlResult::Affected(n),
        }
    }

    fn check_write(&self) -> Result<(), SqlError> {
        match self.write_policy {
            WritePolicy::AllowWrite => Ok(()),
            WritePolicy::ReadOnly => Err(SqlError::ReadOnly),
        }
    }

    /// 批次序列送入 ingest 管线（WAL 权威，§4.4）。
    pub(crate) async fn ingest_batches(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
        idempotency_key: Option<String>,
    ) -> Result<(), SqlError> {
        for batch in batches {
            let ib = yuntun_model::IngestBatch {
                table: table.to_string(),
                shard_key: "default".to_string(),
                record_batch: batch,
                idempotency_key: idempotency_key.clone(),
                received_at: std::time::SystemTime::now(),
            };
            self.ingest.ingest(ib).await.map_err(SqlError::from_lake)?;
        }
        Ok(())
    }

    /// DDL 事件追加 WAL（Catalog apply 成功后 append，顺序即因果，S1.7）。
    pub(crate) async fn append_ddl(&self, p: DdlPayload) -> Result<(), SqlError> {
        self.ingest
            .wal
            .append(yuntun_model::wal_record::Record::Ddl(p))
            .await
            .map_err(SqlError::from_lake)?;
        Ok(())
    }

    /// Arrow → MySQL 类型声明字符串（SHOW COLUMNS / wire 列描述共用）。
    pub fn arrow_to_mysql_type(dt: &arrow::datatypes::DataType) -> &'static str {
        use arrow::datatypes::DataType as D;
        match dt {
            D::Boolean => "tinyint(1)",
            D::Int8 => "tinyint",
            D::Int16 => "smallint",
            D::Int32 => "int",
            D::Int64 => "bigint",
            D::UInt8 => "tinyint unsigned",
            D::UInt16 => "smallint unsigned",
            D::UInt32 => "int unsigned",
            D::UInt64 => "bigint unsigned",
            D::Float32 => "float",
            D::Float64 => "double",
            D::Utf8 | D::LargeUtf8 => "text",
            D::Binary | D::LargeBinary => "blob",
            D::Date32 => "date",
            D::Timestamp(_, _) => "datetime(6)",
            D::Decimal128(_, _) => "decimal(38,10)",
            _ => "text",
        }
    }

    /// CREATE TABLE / INSERT / DROP / SHOW TABLES 分流（S1.6 语义，自 server 迁入）。
    async fn dispatch(&self, stmt: sqlparser::ast::Statement) -> Result<RawOutcome, SqlError> {
        use sqlparser::ast::{Expr, ObjectType, SetExpr, Statement, TableObject};
        match stmt {
            Statement::Query(q) => {
                // 只读查询 → DataFusion（默认 catalog/schema = yuntun/public，G2）
                let text = q.to_string();
                let batches = self
                    .query
                    .sql(&text)
                    .await
                    .map_err(|e| SqlError::Internal(format!("query: {e}")))?;
                Ok(RawOutcome::Rows(batches))
            }
            Statement::ShowTables { .. } => {
                let names = self.list_tables().await?;
                Ok(RawOutcome::Rows(vec![sql::show_tables_batch(&names)]))
            }
            Statement::Insert(ins) => {
                self.check_write()?;
                let table = match &ins.table {
                    TableObject::TableName(name) => sql::table_name_of(name),
                    _ => {
                        return Err(SqlError::Unsupported(
                            "INSERT target must be a plain table name".into(),
                        ))
                    }
                };
                if table.is_empty() {
                    return Err(SqlError::Unsupported("invalid INSERT target table".into()));
                }
                let Some(src) = &ins.source else {
                    return Err(SqlError::Unsupported(
                        "INSERT requires a source (VALUES or SELECT)".into(),
                    ));
                };
                // 表 schema 来自 Catalog（写入事实以 ingest 管线校验为准，双保险）
                let meta = self
                    .catalog
                    .get_table(&table)
                    .await
                    .map_err(SqlError::from_lake)?
                    .ok_or_else(|| SqlError::NotFound(table.clone()))?;
                let schema = meta.schema().map_err(SqlError::from_lake)?;
                // 语句级幂等键（plan §4.3：满足 require 表强制检查；S1.8 前服务端生成）
                let stmt_key = Some(format!("dml-{}", uuid::Uuid::now_v7()));

                match src.body.as_ref() {
                    // INSERT ... VALUES：AST 字面量 → RecordBatch → ingest
                    SetExpr::Values(values) => {
                        let rows: Vec<Vec<Expr>> =
                            values.rows.iter().map(|r| r.content.clone()).collect();
                        let cols = if ins.columns.is_empty() {
                            None
                        } else {
                            Some(ins.columns.clone())
                        };
                        let (batch, n) = sql::build_values_batch(&schema, cols.as_deref(), &rows)
                            .map_err(SqlError::from_lake)?;
                        self.ingest_batches(&table, vec![batch], stmt_key).await?;
                        Ok(RawOutcome::Affected(n as i64))
                    }
                    // INSERT ... SELECT：DataFusion 执行 SELECT 源（读）→ cast → ingest
                    _ => {
                        if !ins.columns.is_empty() {
                            return Err(SqlError::Unsupported(
                                "INSERT ... SELECT with column list is not supported \
                                 (positional alignment only)"
                                    .into(),
                            ));
                        }
                        let select_sql = src.body.to_string();
                        // 读源前刷新本地缓存：尽量覆盖最近已 commit 的文件（§4.5 可见性）
                        self.query
                            .cache()
                            .refresh(&self.catalog)
                            .await
                            .map_err(|e| SqlError::Internal(e.to_string()))?;
                        let src_batches = self.query.sql(&select_sql).await.map_err(|e| {
                            SqlError::Internal(format!("query: {e}"))
                        })?;
                        let (batches, n) =
                            sql::cast_batches_to_table(&schema, &src_batches)
                                .map_err(SqlError::from_lake)?;
                        if n > 0 {
                            self.ingest_batches(&table, batches, stmt_key).await?;
                        }
                        Ok(RawOutcome::Affected(n as i64))
                    }
                }
            }
            Statement::CreateTable(ct) => {
                self.check_write()?;
                let parsed = sql::parse_create_table(&ct).map_err(SqlError::from_lake)?;
                // CREATE TABLE 的 ingest 配置使用 General 模板（强制幂等键，plan §4.3）
                let default_format = self.ingest.cfg.default_format.ext().to_string();
                let req = CreateTableRequest {
                    name: parsed.name.clone(),
                    schema: parsed.schema.clone(),
                    partition_cols: vec![],
                    default_format: default_format.clone(),
                    ingest_config: yuntun_model::meta::IngestConfig::standard(),
                };
                match self.catalog.create_table(req).await {
                    Ok(_) => {}
                    Err(LakeError::TableAlreadyExists(_)) if parsed.if_not_exists => {}
                    Err(e) => return Err(SqlError::from_lake(e)),
                }
                // DDL WAL 权威记录：崩溃重启后重放重建表清单（S1.6/S1.7 验收）
                self.append_ddl(DdlPayload {
                    op: ddl_op::CREATE_TABLE,
                    table: parsed.name,
                    arrow_schema: serialize_schema(&parsed.schema),
                    default_format,
                })
                .await?;
                // DataFusion 本地缓存立即感知新表（INSERT ... SELECT 源可立即引用）
                self.query
                    .cache()
                    .refresh(&self.catalog)
                    .await
                    .map_err(|e| SqlError::Internal(e.to_string()))?;
                Ok(RawOutcome::Affected(0))
            }
            Statement::Drop {
                object_type: ObjectType::Table,
                if_exists,
                names,
                ..
            } => {
                self.check_write()?;
                for name in names {
                    let table = sql::table_name_of(&name);
                    if table.is_empty() {
                        return Err(SqlError::Unsupported("invalid table name in DROP".into()));
                    }
                    match self.catalog.drop_table(&table).await {
                        // 数据文件转孤儿，由孤儿清理回收（plan §4.3）
                        Ok(()) => {
                            self.append_ddl(DdlPayload {
                                op: ddl_op::DROP_TABLE,
                                table,
                                arrow_schema: vec![],
                                default_format: String::new(),
                            })
                            .await?;
                            // DataFusion 本地缓存立即感知表移除
                            self.query
                                .cache()
                                .refresh(&self.catalog)
                                .await
                                .map_err(|e| SqlError::Internal(e.to_string()))?;
                        }
                        Err(LakeError::TableNotFound(_)) if if_exists => {}
                        Err(e) => return Err(SqlError::from_lake(e)),
                    }
                }
                Ok(RawOutcome::Affected(0))
            }
            other => Err(SqlError::Unsupported(format!(
                "only SELECT / SHOW TABLES / INSERT / CREATE TABLE / DROP TABLE are \
                 supported, got: {}",
                crate::sql::sql_snippet(&other.to_string())
            ))),
        }
    }
}

/// dispatch 的原始产出（execute 负责补 schema / 包成 SqlResult）。
pub(crate) enum RawOutcome {
    Rows(Vec<RecordBatch>),
    Affected(i64),
}

/// 表列描述（元数据 API 输出）。
#[derive(Debug, Clone)]
pub struct ColumnDesc {
    pub name: String,
    /// MySQL 类型声明字符串（SHOW COLUMNS / wire 列描述）
    pub mysql_type: String,
    pub nullable: bool,
}

/// 表描述（describe_table 输出）。
#[derive(Debug, Clone)]
pub struct TableDesc {
    pub name: String,
    pub columns: Vec<ColumnDesc>,
    pub schema: SchemaRef,
}
