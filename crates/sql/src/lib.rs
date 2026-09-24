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

pub mod idempotency;
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
use yuntun_query::{PartialRead, PartialSink, QueryEngine};

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
    /// 多 schema：目标 schema 不存在（MySQL 1049 / unknown database）
    #[error("schema not found: {0}")]
    SchemaNotFound(String),
    #[error("schema already exists: {0}")]
    SchemaExists(String),
    /// DROP DATABASE 时 schema 下仍有表（MySQL 1008）
    #[error("schema is not empty: {0}")]
    SchemaNotEmpty(String),
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
            LakeError::SchemaNotFound(s) => SqlError::SchemaNotFound(s),
            LakeError::SchemaAlreadyExists(s) => SqlError::SchemaExists(s),
            LakeError::SchemaNotEmpty(s) => SqlError::SchemaNotEmpty(s),
            LakeError::IdempotencyKeyRequired => {
                SqlError::Precondition("idempotency key is required for this table".into())
            }
            LakeError::IdempotencyKeyTooLong => {
                SqlError::Internal("idempotency key too long".into())
            }
            // 背压阶梯第三级（架构 §2.7）：瞬时错误，客户端退避后重试（非语义错误）
            LakeError::ResourceExhausted(msg) => SqlError::Internal(format!(
                "server is under memory pressure ({msg}); retry after backoff"
            )),
            other => SqlError::Internal(other.to_string()),
        }
    }
}

/// 一次 SQL 执行的结果（协议适配层据此编码行 / 回执）。
pub enum SqlResult {
    /// 结果集：schema 恒有值（空结果集也返回查询 schema，S1.5 一致性语义）
    Rows {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        /// **结果完不完整**（`§89`）：eager 路径在返回前就把批次收齐了，
        /// 所以这里给的是**结论**（缺了谁、为什么）。协议层负责把它交给用户。
        partial: PartialRead,
    },
    /// 受影响行数（INSERT / DDL / shim no-op）
    Affected(i64),
}

/// 流式执行结果（S1.10）：`Rows` 的主体为 DataFusion 流（边算边发），
/// 供 `do_get` 等大结果集路径使用；MySQL wire 走 [`SqlResult`] 的 eager 路径。
/// 流式结果里"完不完整"这件事的载体（`§89`）。
///
/// 两条路拿到的**时机**不同，所以不能共用一个类型：
///
/// - **eager 路径**（MySQL wire / shim / 小结果集）：批次已经收齐 ⇒ 直接给结论 [`PartialRead`]；
/// - **流式路径**（Flight `do_get`）：热读发生在**流被消费时** ⇒ 只能给一个**句柄**，
///   等流读完再读它。**流没跑之前读它一定会得到"完整"** —— 那是"还不知道"，不是"没问题"。
#[derive(Debug, Clone)]
pub enum PartialWatch {
    /// 已是结论（eager 路径）。
    Known(PartialRead),
    /// 还没成形：读它之前必须先把流消费掉（流式路径）。
    Pending(std::sync::Arc<PartialSink>),
}

impl PartialWatch {
    /// 现在能给出的结论。`Pending` 且流未读完时它会**偏"完整"** —— 调用方须自己保证
    /// "先消费流、再读它"（`§89` 的 Flight 路径正是先 `peek` 一步再读）。
    pub fn read(&self) -> PartialRead {
        match self {
            PartialWatch::Known(r) => r.clone(),
            PartialWatch::Pending(s) => s.read(),
        }
    }
}

pub enum SqlStreamResult {
    /// 结果集：schema 恒有值；空结果集时流不产出批次但 schema 可用
    Rows {
        schema: SchemaRef,
        stream: yuntun_query::SendableRecordBatchStream,
        /// 完不完整的载体（见 [`PartialWatch`]）
        partial: PartialWatch,
    },
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
    /// S1.8：prepare 时从 SQL 注释解析的幂等键（参数替换后的 SQL 文本不再含注释，
    /// 因此必须随语句缓存，在 execute_prepared 时显式传入分流）。
    pub idempotency_key: Option<String>,
}

/// SQL 执行引擎：编排 query / ingest / catalog 三项能力（无自身状态）。
pub struct SqlEngine {
    /// 写入侧。**`None` = 本节点只读**（查询节点：不持有 WAL/chunk，只查）。
    ///
    /// 只读不是"把方法调用藏起来"，而是**装配事实**：查询节点上根本没有 Ingestor
    /// （没有私有目录、没有 WAL）。`write_policy` 是它对外的声明，这里是没有能力。
    pub(crate) ingest: Option<Arc<Ingestor>>,
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
            ingest: Some(ingest),
            query,
            catalog,
            write_policy: WritePolicy::AllowWrite,
        }
    }

    /// **只读装配**（查询节点）：没有写入侧，且 `write_policy` 明确声明只读。
    ///
    /// 两者一起才完整：策略让"写语句"得到**可读的拒绝**（`SqlError::ReadOnly`），
    /// `None` 则保证即使策略被绕过也不会有人真的去写（下方各处 `ok_or` 兜底）。
    pub fn new_readonly(query: Arc<QueryEngine>, catalog: Arc<dyn CatalogOps>) -> Self {
        Self {
            ingest: None,
            query,
            catalog,
            write_policy: WritePolicy::ReadOnly,
        }
    }

    /// 写入侧（只读装配下报 `ReadOnly`，而不是 panic）。
    pub(crate) fn ingest_side(&self) -> Result<&Arc<Ingestor>, SqlError> {
        self.ingest.as_ref().ok_or(SqlError::ReadOnly)
    }

    pub fn with_write_policy(mut self, policy: WritePolicy) -> Self {
        self.write_policy = policy;
        self
    }

    /// 一次性执行（简单协议：MySQL COM_QUERY 文本 / PG simple Query /
    /// Flight 简易轨与 StatementUpdate）。
    pub async fn execute(&self, sql: &str, session: &mut SessionCtx) -> Result<SqlResult, SqlError> {
        // S1.8：幂等键透传（SQL 注释 `/* idempotency_key=... */`）
        let key = idempotency::extract(sql);
        self.execute_with_key(sql, session, key).await
    }

    /// 带显式幂等键执行（prepared 路径：键在 prepare 时解析并随语句缓存，
    /// 参数替换后的 SQL 文本已不含注释）。
    pub async fn execute_with_key(
        &self,
        sql: &str,
        session: &mut SessionCtx,
        key: Option<String>,
    ) -> Result<SqlResult, SqlError> {
        let stmt = sql::parse_single_with(session.dialect.parser_dialect(), sql)
            .map_err(|e| SqlError::Parse(e.to_string()))?;
        // 方言 shim：wire 客户端的非数据语句（SHOW/SET/USE/@@var/事务 no-op）
        if let Some(res) = shim::intercept(&stmt, session, self).await? {
            return Ok(res);
        }
        let outcome = self.dispatch(stmt, session, key).await?;
        Ok(self.finish(outcome, sql, session.schema()).await)
    }

    // ---------------------------------------------------------------- schema（多库）

    /// schema 清单（`SHOW DATABASES` / FlightSQL `GetDbSchemas`）。
    pub async fn list_schemas(&self) -> Result<Vec<String>, SqlError> {
        self.catalog.list_schemas().await.map_err(SqlError::from_lake)
    }

    /// schema 是否存在（`USE db` / handshake 校验）。
    pub async fn schema_exists(&self, name: &str) -> Result<bool, SqlError> {
        self.catalog
            .schema_exists(name)
            .await
            .map_err(SqlError::from_lake)
    }

    /// 新建 schema（`CREATE DATABASE` / `CREATE SCHEMA`）。
    pub async fn create_schema(&self, name: &str) -> Result<(), SqlError> {
        self.check_write()?;
        self.catalog
            .create_schema(name)
            .await
            .map_err(SqlError::from_lake)?;
        // DDL WAL 权威记录（重启后重放重建 schema；`DdlPayload.table` = schema 名）
        self.append_ddl(DdlPayload {
            op: ddl_op::CREATE_SCHEMA,
            table: name.to_string(),
            arrow_schema: vec![],
            default_format: String::new(),
        })
        .await?;
        Ok(())
    }

    /// 删除空 schema（`DROP DATABASE` / `DROP SCHEMA`）。
    pub async fn drop_schema(&self, name: &str) -> Result<(), SqlError> {
        self.check_write()?;
        self.catalog
            .drop_schema(name)
            .await
            .map_err(SqlError::from_lake)?;
        self.append_ddl(DdlPayload {
            op: ddl_op::DROP_SCHEMA,
            table: name.to_string(),
            arrow_schema: vec![],
            default_format: String::new(),
        })
        .await?;
        Ok(())
    }

    /// 某 schema 下的表名（裸名，已排序）——`SHOW TABLES`。
    pub async fn list_tables_in(&self, schema: &str) -> Result<Vec<String>, SqlError> {
        let all = self.catalog.list_tables().await.map_err(SqlError::from_lake)?;
        let mut names: Vec<String> = all
            .into_iter()
            .filter(|t| t.schema_name() == schema)
            .map(|t| t.name)
            .collect();
        names.sort();
        Ok(names)
    }

    /// 流式执行（S1.10）：与 [`SqlEngine::execute`] 同一分流，但 SELECT 分支
    /// **不 collect**——直接把 DataFusion 物理计划流交给协议层编码。
    ///
    /// 非查询语句（shim canned / SHOW TABLES / INSERT / DDL）产出批量很小，
    /// 统一经 `stream_from_batches` 转流，协议层只有一个出口。
    pub async fn execute_stream(
        &self,
        sql: &str,
        session: &mut SessionCtx,
    ) -> Result<SqlStreamResult, SqlError> {
        let stmt = sql::parse_single_with(session.dialect.parser_dialect(), sql)
            .map_err(|e| SqlError::Parse(e.to_string()))?;
        // 方言 shim：canned 结果（SHOW/SET/USE/@@var 探测）体量小，转流即可
        if let Some(res) = shim::intercept(&stmt, session, self).await? {
            return Ok(eager_to_stream(res));
        }
        // 只读查询 → DataFusion 物理计划流（真正的流式；非限定表名按会话 schema 解析）
        if let sqlparser::ast::Statement::Query(q) = &stmt {
            let text = q.to_string();
            let (stream, partial) = self
                .query
                .sql_stream_with_partial(&text, session.schema())
                .await
                .map_err(query_error)?;
            let schema = stream.schema();
            return Ok(SqlStreamResult::Rows {
                schema,
                stream,
                // 句柄：**流读完**之后它才是结论（调用方负责时机）
                partial: PartialWatch::Pending(partial),
            });
        }
        // INSERT / DDL / SHOW TABLES：走既有 eager 分流（结果集小）
        let key = idempotency::extract(sql);
        match self.dispatch(stmt, session, key).await? {
            RawOutcome::Rows { batches, partial } => Ok(eager_to_stream(SqlResult::Rows {
                schema: batches
                    .first()
                    .map(|b| b.schema())
                    .unwrap_or_else(|| Arc::new(Schema::empty())),
                batches,
                partial,
            })),
            RawOutcome::Affected(n) => Ok(SqlStreamResult::Affected(n)),
        }
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
            idempotency_key: idempotency::extract(sql),
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
        self.execute_with_key(&rendered, session, stmt.idempotency_key.clone())
            .await
    }

    /// 元数据 API：全部表的**全限定标识**（跨 schema；调试/内部用）。
    pub async fn list_tables(&self) -> Result<Vec<String>, SqlError> {
        let tables = self.catalog.list_tables().await.map_err(SqlError::from_lake)?;
        let mut names: Vec<String> = tables.into_iter().map(|t| t.qualified_name()).collect();
        names.sort();
        Ok(names)
    }

    /// 元数据 API：表列描述（SHOW COLUMNS / DESCRIBE / wire RowDescription）。
    ///
    /// `name` 为全限定标识 `schema.table`（[`sql::resolve_table_ref`] 解析而来）。
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

    /// 逻辑计划 schema（S1.5 一致性语义：空结果集也返回查询 schema，默认 schema）。
    pub async fn schema_of(&self, sql: &str) -> Option<SchemaRef> {
        self.schema_of_in(sql, yuntun_model::ops::DEFAULT_SCHEMA).await
    }

    /// 逻辑计划 schema（指定会话 schema：非限定表名按该 schema 解析）。
    pub async fn schema_of_in(&self, sql: &str, schema: &str) -> Option<SchemaRef> {
        self.query
            .schema_of_with_schema(sql, schema)
            .await
            .ok()
            .filter(|s| !s.fields().is_empty())
    }

    /// 结果集收尾：空批次时回填查询 schema（G7 兼容：与 GetFlightInfo 一致）。
    async fn finish(&self, outcome: RawOutcome, sql: &str, schema: &str) -> SqlResult {
        match outcome {
            RawOutcome::Rows { batches, partial } if !batches.is_empty() => SqlResult::Rows {
                schema: batches[0].schema(),
                batches,
                partial,
            },
            RawOutcome::Rows { partial, .. } => SqlResult::Rows {
                schema: self
                    .schema_of_in(sql, schema)
                    .await
                    .unwrap_or_else(|| Arc::new(Schema::empty())),
                batches: Vec::new(),
                partial,
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
    ///
    /// ⚠️ 幂等键是**语句级**的，而一条语句可能产出多个批次（`INSERT ... SELECT`
    /// 的结果批次）。必须按批次派生键（[`yuntun_ingest::derive_batch_key`]），
    /// 否则第 2..N 批会带着同一个键撞上第 1 批的幂等记录 → **静默丢数据**。
    pub(crate) async fn ingest_batches(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
        idempotency_key: Option<String>,
    ) -> Result<(), SqlError> {
        for (idx, batch) in batches.into_iter().enumerate() {
            let ib = yuntun_model::IngestBatch {
                table: table.to_string(),
                shard_key: "default".to_string(),
                record_batch: batch,
                idempotency_key: idempotency_key
                    .as_deref()
                    .map(|k| yuntun_ingest::derive_batch_key(k, idx as u64)),
                received_at: std::time::SystemTime::now(),
            };
            self.ingest_side()?
                .ingest(ib)
                .await
                .map_err(SqlError::from_lake)?;
        }
        Ok(())
    }

    /// DDL 事件追加 WAL（Catalog apply 成功后 append，顺序即因果，S1.7）。
    pub(crate) async fn append_ddl(&self, p: DdlPayload) -> Result<(), SqlError> {
        self.ingest_side()?
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

    /// CREATE/DROP DATABASE · CREATE TABLE / INSERT / DROP / SHOW 分流（S1.6 语义 + 多 schema）。
    async fn dispatch(
        &self,
        stmt: sqlparser::ast::Statement,
        session: &SessionCtx,
        stmt_key: Option<String>,
    ) -> Result<RawOutcome, SqlError> {
        use sqlparser::ast::{Expr, ObjectType, SetExpr, Statement, TableObject};
        match stmt {
            Statement::Query(q) => {
                // 只读查询 → DataFusion（非限定表名按会话 schema 解析，G2 + 多 schema）
                let text = q.to_string();
                // 【§89】用**带 partial 的版本**：`sql_with_schema` 会把"缺了来源"这件事
                // 只写进日志，协议层就永远拿不到它（这正是本刀要修的那条断路）。
                let out = self
                    .query
                    .sql_with_partial(&text, session.schema())
                    .await
                    .map_err(query_error)?;
                Ok(RawOutcome::Rows {
                    batches: out.batches,
                    partial: out.partial,
                })
            }
            Statement::ShowTables { .. } => {
                let names = self.list_tables_in(session.schema()).await?;
                Ok(RawOutcome::Rows {
                    batches: vec![sql::show_tables_batch(session.schema(), &names)],
                    // 元数据类结果不读热数据 ⇒ 恒完整
                    partial: PartialRead::default(),
                })
            }
            Statement::ShowDatabases { .. } => {
                let rows: Vec<Vec<String>> = self
                    .list_schemas()
                    .await?
                    .into_iter()
                    .map(|s| vec![s])
                    .collect();
                Ok(RawOutcome::Rows {
                    batches: vec![sql::strings_batch(&["Database"], &rows)],
                    partial: PartialRead::default(),
                })
            }
            // CREATE DATABASE / CREATE SCHEMA（多 schema）
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                ..
            } => {
                self.check_write()?;
                let name = sql::table_name_of(&db_name);
                if name.is_empty() {
                    return Err(SqlError::Unsupported("invalid database name".into()));
                }
                match self.create_schema(&name).await {
                    Ok(()) => Ok(RawOutcome::Affected(0)),
                    Err(SqlError::SchemaExists(_)) if if_not_exists => Ok(RawOutcome::Affected(0)),
                    Err(e) => Err(e),
                }
            }
            Statement::Insert(ins) => {
                self.check_write()?;
                // 目标表 → 全限定标识（`sales.orders` / 非限定名取会话 schema）
                let table = match &ins.table {
                    TableObject::TableName(name) => sql::resolve_table_ref(name, session),
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
                // 幂等键（S1.8 透传）：客户端 SQL 注释提供 > 服务端语句级生成
                // （plan §4.3：满足 require 表强制检查）
                let stmt_key =
                    Some(stmt_key.unwrap_or_else(|| format!("dml-{}", uuid::Uuid::now_v7())));

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
                            .catalog()
                            .refresh(&self.catalog)
                            .await
                            .map_err(|e| SqlError::Internal(e.to_string()))?;
                        let src_batches = self
                            .query
                            .sql_with_schema(&select_sql, session.schema())
                            .await
                            .map_err(query_error)?;
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
                // 归属 schema：`CREATE TABLE sales.orders` → sales；非限定名 → 会话 schema
                let qualified = sql::resolve_table_ref(&ct.name, session);
                let (ns, bare) = yuntun_model::ops::split_qualified(&qualified);
                if bare.is_empty() {
                    return Err(SqlError::Unsupported("invalid table name".into()));
                }
                if !self.schema_exists(ns).await? {
                    return Err(SqlError::SchemaNotFound(ns.to_string()));
                }
                // CREATE TABLE 的 ingest 配置使用 General 模板（强制幂等键，plan §4.3）
                // 建表默认格式：只读节点没有写入侧配置，退化为 parquet（建表本身也会被策略拒绝）
                let default_format = self
                    .ingest
                    .as_ref()
                    .map(|i| i.cfg.default_format.ext().to_string())
                    .unwrap_or_else(|| "parquet".to_string());
                let req = CreateTableRequest {
                    name: bare.to_string(),
                    namespace: ns.to_string(),
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
                // DDL WAL 权威记录（**全限定名**）：崩溃重启后重放重建表清单（S1.6/S1.7）
                self.append_ddl(DdlPayload {
                    op: ddl_op::CREATE_TABLE,
                    table: qualified,
                    arrow_schema: serialize_schema(&parsed.schema),
                    default_format,
                })
                .await?;
                // DataFusion 本地缓存立即感知新表（INSERT ... SELECT 源可立即引用）
                self.query
                    .catalog()
                    .refresh(&self.catalog)
                    .await
                    .map_err(|e| SqlError::Internal(e.to_string()))?;
                Ok(RawOutcome::Affected(0))
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => {
                self.check_write()?;
                match object_type {
                    ObjectType::Table => {
                        for name in names {
                            // 全限定标识（`sales.orders` / 非限定名取会话 schema）
                            let table = sql::resolve_table_ref(&name, session);
                            if table.is_empty() {
                                return Err(SqlError::Unsupported(
                                    "invalid table name in DROP".into(),
                                ));
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
                                        .catalog()
                                        .refresh(&self.catalog)
                                        .await
                                        .map_err(|e| SqlError::Internal(e.to_string()))?;
                                }
                                Err(LakeError::TableNotFound(_)) if if_exists => {}
                                Err(e) => return Err(SqlError::from_lake(e)),
                            }
                        }
                    }
                    // DROP DATABASE / DROP SCHEMA（空 schema 才允许删除）
                    ObjectType::Database | ObjectType::Schema => {
                        for name in names {
                            let schema = sql::table_name_of(&name);
                            if schema.is_empty() {
                                return Err(SqlError::Unsupported(
                                    "invalid database name in DROP".into(),
                                ));
                            }
                            match self.drop_schema(&schema).await {
                                Ok(()) => {}
                                Err(SqlError::SchemaNotFound(_)) if if_exists => {}
                                Err(e) => return Err(e),
                            }
                        }
                    }
                    other => {
                        return Err(SqlError::Unsupported(format!(
                            "DROP {other} is not supported (only TABLE / DATABASE)"
                        )))
                    }
                }
                Ok(RawOutcome::Affected(0))
            }
            other => Err(SqlError::Unsupported(format!(
                "only SELECT / SHOW TABLES / SHOW DATABASES / INSERT / CREATE TABLE / \
                 DROP TABLE / CREATE DATABASE / DROP DATABASE are supported, got: {}",
                crate::sql::sql_snippet(&other.to_string())
            ))),
        }
    }
}

/// DataFusion 查询错误 → SqlError。
///
/// 表不存在以**字符串**形式到达（本 crate 不直接依赖 datafusion，无法匹配
/// `DataFusionError::Plan` 变体）：按 "not found" 归类 [`SqlError::NotFound`]，
/// 协议层才能映射客户端错误码（MySQL 1146 / Flight NOT_FOUND），而非内部错误。
fn query_error(e: impl std::fmt::Display) -> SqlError {
    let msg = e.to_string();
    if !msg.contains("not found") {
        return SqlError::Internal(format!("query: {msg}"));
    }
    // 形如 `table 'yuntun.public.foo' not found` → 取引号内限定名
    let table = msg
        .find('\'')
        .and_then(|s| msg[s + 1..].find('\'').map(|e| msg[s + 1..s + 1 + e].to_string()))
        .unwrap_or(msg);
    SqlError::NotFound(table)
}

/// eager 结果 → 流式结果（shim canned / SHOW TABLES / 小结果集的统一出口）。
fn eager_to_stream(res: SqlResult) -> SqlStreamResult {
    match res {
        SqlResult::Rows {
            schema,
            batches,
            partial,
        } => SqlStreamResult::Rows {
            stream: QueryEngine::stream_from_batches(schema.clone(), batches),
            schema,
            // eager 路径已经有结论 ⇒ 直接给结论（不必等流跑完）
            partial: PartialWatch::Known(partial),
        },
        SqlResult::Affected(n) => SqlStreamResult::Affected(n),
    }
}

/// dispatch 的原始产出（execute 负责补 schema / 包成 SqlResult）。
pub(crate) enum RawOutcome {
    Rows {
        batches: Vec<RecordBatch>,
        /// 结果完不完整（`§89`）：非查询类产出恒完整
        partial: PartialRead,
    },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_error_classifies_missing_table() {
        // DataFusion 规划错误（字符串形态）→ NotFound（协议层映射 1146 / NOT_FOUND）
        let e = query_error("Error during planning: table 'yuntun.public.foo' not found");
        assert!(matches!(&e, SqlError::NotFound(t) if t == "yuntun.public.foo"), "{e}");

        // 其他查询错误仍为内部错误（客户端可重试）
        let e = query_error("Schema error: No field named x");
        assert!(matches!(&e, SqlError::Internal(m) if m.contains("No field named x")), "{e}");
    }
}
