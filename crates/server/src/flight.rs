//! Flight gRPC 服务端（唯一协议端口，架构 §3.2 v12.3）。
//!
//! **设计原则**：
//! 1. **所有写入都走 ingest 管线**——它是唯一的数据写入事实（WAL 权威，禁绕过）；
//! 2. server 直接组合底层能力：`Arc<Ingestor>`（写）+ `Arc<QueryEngine>`（读）+
//!    `Arc<dyn CatalogOps>`（meta），不引入 trait 间接层；协议解析与路由都在本模块完成。
//! 3. **SQL 前置解析拦截（S1.6/S1.7，plan §4.3）**：所有 SQL 入口（简易轨 do_get /
//!    FlightSQL StatementQuery / StatementUpdate）统一经 [`FlightServer::run_sql`] 按
//!    AST 分流——只有 SELECT 交给 DataFusion；INSERT → ingest；CREATE/DROP/SHOW → catalog。
//!
//! 单端点承载多轨（未来单节点可扩展更多协议端口，如 InfluxDB LP / MySQL wire）：
//! - **FlightSQL 标准轨**：cmd/ticket 为 FlightSQL protobuf Any 命令
//!   - 读：StatementQuery / PreparedStatementQuery → `query.sql()`（经 run_sql 分流）
//!   - 写：StatementIngest / PreparedStatementUpdate(INSERT+绑定数据) → `ingest.ingest()`
//!   - 写：StatementUpdate（INSERT/DDL）→ run_sql 前置分流
//! - **简易轨（写入）**：DoPut `path=[table,shard]` → `ingest.ingest()`
//! - **简易轨（查询）**：ticket / get_flight_info cmd = 裸 UTF-8 SQL → run_sql 前置分流
//!
//! Ticket 编码：`Any(TicketStatementQuery{ statement_handle })`；
//! statement_handle = SQL 本体（无状态）或 `ps:<uuid>`（prepared，服务端内存映射）。

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::SystemTime;

use arrow::array::{ArrayRef, BinaryArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::metadata::{
    SqlInfoData, SqlInfoDataBuilder, XdbcTypeInfo, XdbcTypeInfoData, XdbcTypeInfoDataBuilder,
};
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, Any, Command, CommandGetTables, Nullable, ProstMessageExt,
    Searchable, SqlInfo, SqlSupportedTransaction, TicketStatementQuery, XdbcDataType,
};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use futures::StreamExt;
use prost::Message;
use tonic::{Request, Response, Status, Streaming};
use yuntun_catalog::CatalogOps;
use yuntun_ingest::source::{extract_table_shard, IngestBatch};
use yuntun_ingest::Ingestor;
use yuntun_model::error::LakeError;
use yuntun_model::meta::serialize_schema;
use yuntun_model::ops::CreateTableRequest;
use yuntun_model::wal_record::{ddl_op, DdlPayload, Record};
use yuntun_query::QueryEngine;

/// 固定 catalog / schema 名（与 yuntun_query 常量一致）。
pub(crate) const CATALOG_NAME: &str = yuntun_query::CATALOG_NAME;
pub(crate) const SCHEMA_NAME: &str = yuntun_query::SCHEMA_NAME;
/// prepared statement handle 前缀（区别于内嵌 SQL 的无状态 handle）。
const PS_PREFIX: &str = "ps:";

/// Flight gRPC 服务端：直接组合 ingest（写）/ query（读）/ catalog（meta）底层能力。
pub struct FlightServer {
    pub(crate) ingest: Arc<Ingestor>,
    pub(crate) query: Arc<QueryEngine>,
    pub(crate) catalog: Arc<dyn CatalogOps>,
    /// prepared statement：handle（剥 `ps:` 前缀）→ SQL
    pub(crate) prepared: Mutex<HashMap<String, String>>,
}

impl FlightServer {
    pub fn new(
        ingest: Arc<Ingestor>,
        query: Arc<QueryEngine>,
        catalog: Arc<dyn CatalogOps>,
    ) -> Self {
        Self {
            ingest,
            query,
            catalog,
            prepared: Mutex::new(HashMap::new()),
        }
    }

    // ------------------------------------------------- FlightSQL 命令解码

    /// 把 cmd / ticket 字节解码为 FlightSQL 命令；**非 FlightSQL 编码 → None**
    /// （回落到简易轨。简易轨的 cmd 是裸 UTF-8 SQL，不会通过 Any 解码）。
    fn decode_command(cmd: &[u8]) -> Option<Command> {
        if cmd.is_empty() {
            return None;
        }
        let any = Any::decode(cmd).ok()?;
        match Command::try_from(any) {
            Ok(Command::Unknown(_)) | Err(_) => None,
            Ok(c) => Some(c),
        }
    }

    fn resolve_handle(&self, handle: &[u8]) -> Result<String, Status> {
        let s = std::str::from_utf8(handle)
            .map_err(|_| Status::invalid_argument("statement_handle must be UTF-8"))?;
        if let Some(id) = s.strip_prefix(PS_PREFIX) {
            return self
                .prepared
                .lock()
                .expect("prepared lock poisoned")
                .get(id)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("unknown prepared statement: {id}")));
        }
        // 无状态 handle = SQL 本体（标准轨/简易轨共享无状态查询）
        Ok(s.to_string())
    }

    /// DoPut 同步分流：SQL 更新/装载命令 → Some(路由)；否则 None（简易轨）。
    fn put_route(cmd: &[u8]) -> Option<PutRoute> {
        match Self::decode_command(cmd)? {
            Command::CommandStatementUpdate(c) => Some(PutRoute::StatementUpdate(c.query)),
            Command::CommandPreparedStatementUpdate(c) => Some(PutRoute::PreparedStatementUpdate(
                c.prepared_statement_handle.to_vec(),
            )),
            Command::CommandStatementIngest(c) => Some(PutRoute::StatementIngest(c.table)),
            _ => None,
        }
    }

    // --------------------------------------------- FlightSQL 读（→ query）

    /// 语句查询的 FlightInfo：endpoint ticket = Any(TicketStatementQuery{ handle })，
    /// schema 取逻辑计划（尽力而为，失败则空 schema，客户端可再 get_schema）。
    async fn query_info(&self, sql: String) -> Result<FlightInfo, Status> {
        let ticket = TicketStatementQuery {
            statement_handle: sql.clone().into_bytes().into(),
        }
        .as_any();
        let schema = self
            .query
            .schema_of(&sql)
            .await
            .ok()
            .filter(|s| !s.fields().is_empty());
        Ok(endpoint_info(ticket.encode_to_vec(), -1, schema))
    }

    /// GetTables：DataFusion `information_schema.tables` → FlightSQL 元数据 schema。
    async fn tables_batches(&self, c: &CommandGetTables) -> Result<Vec<RecordBatch>, Status> {
        let mut fields = vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ];
        if c.include_schema {
            // 【官方 schema】列名为 table_schema（arrow-flight metadata/tables.rs）
            fields.push(Field::new("table_schema", DataType::Binary, false));
        }
        let schema = Arc::new(Schema::new(fields));

        let sql = "SELECT table_catalog, table_schema, table_name, table_type \
                   FROM information_schema.tables";
        let rows = self.query.sql(sql).await.map_err(query_status)?;

        let mut catalogs: Vec<Option<String>> = Vec::new();
        let mut schemas_col: Vec<Option<String>> = Vec::new();
        let mut names: Vec<String> = Vec::new();
        let mut types: Vec<String> = Vec::new();
        let mut schemas_bin: Vec<Option<Vec<u8>>> = Vec::new();

        for batch in &rows {
            let cat = column_str(batch, 0);
            let sch = column_str(batch, 1);
            let name = column_str(batch, 2);
            let ty = column_str(batch, 3);
            for i in 0..batch.num_rows() {
                let cat_i = cat.get(i).map(String::as_str).unwrap_or("");
                let sch_i = sch.get(i).map(String::as_str).unwrap_or("");
                // 只暴露 yuntun catalog 的 public schema（information_schema/system 不暴露）
                if cat_i != CATALOG_NAME || sch_i == "information_schema" {
                    continue;
                }
                let name_i = name.get(i).map(String::as_str).unwrap_or("").to_string();
                let ty_i = match ty.get(i).map(String::as_str).unwrap_or("") {
                    "BASE TABLE" => "TABLE",
                    other => other,
                }
                .to_string();
                catalogs.push(Some(cat_i.to_string()));
                schemas_col.push(Some(sch_i.to_string()));
                names.push(name_i.clone());
                types.push(ty_i);
                if c.include_schema {
                    let table_ref = format!("{CATALOG_NAME}.{sch_i}.{name_i}");
                    let bytes = self
                        .query
                        .schema_of(&format!("SELECT * FROM {table_ref}"))
                        .await
                        .ok()
                        .and_then(|s| schema_ipc_bytes(&s))
                        .unwrap_or_default();
                    schemas_bin.push(Some(bytes));
                } else {
                    schemas_bin.push(None);
                }
            }
        }

        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(catalogs)),
            Arc::new(StringArray::from(schemas_col)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
        ];
        if c.include_schema {
            let refs: Vec<Option<&[u8]>> = schemas_bin.iter().map(|o| o.as_deref()).collect();
            cols.push(Arc::new(BinaryArray::from_opt_vec(refs)));
        }
        let batch = RecordBatch::try_new(schema, cols)
            .map_err(|e| Status::internal(format!("tables metadata: {e}")))?;
        Ok(vec![batch])
    }

    // --------------------------------------------- SQL 前置解析分流（S1.6/S1.7）

    /// SQL 前置解析分流入口（plan §4.3 路由表）。所有 SQL 入口统一经过：
    /// 简易轨 do_get / FlightSQL StatementQuery / StatementUpdate。
    ///
    /// | AST 变体 | 处理 |
    /// |---|---|
    /// | Query（SELECT/CTE） | → DataFusion（query 能力，只读） |
    /// | ShowTables | → Catalog `list_tables`（meta 能力） |
    /// | Insert + Values | 按表 schema 把字面量构造 RecordBatch → `ingest.ingest` |
    /// | Insert + Select | DataFusion 执行 SELECT 源 → cast 到表 schema → ingest |
    /// | CreateTable | ColumnDef DataType → Arrow → Catalog `create_table` |
    /// | Drop(Table) | → Catalog `drop_table`（数据文件转孤儿） |
    /// | 其他 | 明确拒绝（NotImplemented + 支持列表） |
    pub(crate) async fn run_sql(&self, sql: &str) -> Result<SqlOutcome, Status> {
        use sqlparser::ast::{Expr, ObjectType, SetExpr, Statement, TableObject};
        match crate::sql::parse_single(sql).map_err(lake_status)? {
            Statement::Query(_) => {
                // 只读查询 → DataFusion
                let batches = self.query.sql(sql).await.map_err(query_status)?;
                Ok(SqlOutcome::Rows(batches))
            }
            Statement::ShowTables { .. } => {
                let tables = self.catalog.list_tables().await.map_err(lake_status)?;
                let names: Vec<String> = tables.into_iter().map(|t| t.name).collect();
                Ok(SqlOutcome::Rows(vec![crate::sql::show_tables_batch(
                    &names,
                )]))
            }
            Statement::Insert(ins) => {
                let table = match &ins.table {
                    TableObject::TableName(name) => crate::sql::table_name_of(name),
                    _ => {
                        return Err(Status::unimplemented(
                            "INSERT target must be a plain table name",
                        ))
                    }
                };
                if table.is_empty() {
                    return Err(Status::invalid_argument("invalid INSERT target table"));
                }
                let Some(src) = &ins.source else {
                    return Err(Status::invalid_argument(
                        "INSERT requires a source (VALUES or SELECT)",
                    ));
                };
                // 表 schema 来自 Catalog（写入事实以 ingest 管线校验为准，双保险）
                let meta = self
                    .catalog
                    .get_table(&table)
                    .await
                    .map_err(lake_status)?
                    .ok_or_else(|| Status::not_found(format!("table not found: {table}")))?;
                let schema = meta.schema().map_err(lake_status)?;
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
                        let (batch, n) =
                            crate::sql::build_values_batch(&schema, cols.as_deref(), &rows)
                                .map_err(lake_status)?;
                        self.ingest_batches(&table, vec![batch], stmt_key).await?;
                        Ok(SqlOutcome::Affected(n as i64))
                    }
                    // INSERT ... SELECT：DataFusion 执行 SELECT 源（读）→ cast → ingest
                    _ => {
                        if !ins.columns.is_empty() {
                            return Err(Status::unimplemented(
                                "INSERT ... SELECT with column list is not supported \
                                 (positional alignment only)",
                            ));
                        }
                        let select_sql = src.body.to_string();
                        // 读源前刷新本地缓存：尽量覆盖最近已 commit 的文件（§4.5 可见性语义）
                        self.query
                            .cache()
                            .refresh(&self.catalog)
                            .await
                            .map_err(query_status)?;
                        let src_batches =
                            self.query.sql(&select_sql).await.map_err(query_status)?;
                        let (batches, n) = crate::sql::cast_batches_to_table(&schema, &src_batches)
                            .map_err(lake_status)?;
                        if n > 0 {
                            self.ingest_batches(&table, batches, stmt_key).await?;
                        }
                        Ok(SqlOutcome::Affected(n as i64))
                    }
                }
            }
            Statement::CreateTable(ct) => {
                let parsed = crate::sql::parse_create_table(&ct).map_err(lake_status)?;
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
                    Err(e) => return Err(lake_status(e)),
                }
                // DDL WAL 权威记录：崩溃重启后重放重建表清单（S1.6/S1.7 验收）
                self.append_ddl(DdlPayload {
                    op: ddl_op::CREATE_TABLE,
                    table: parsed.name,
                    arrow_schema: serialize_schema(&parsed.schema),
                    default_format,
                })
                .await?;
                // DataFusion 本地缓存立即感知新表（后续 INSERT ... SELECT 源可立即引用）
                self.query
                    .cache()
                    .refresh(&self.catalog)
                    .await
                    .map_err(query_status)?;
                Ok(SqlOutcome::Affected(0))
            }
            Statement::Drop {
                object_type: ObjectType::Table,
                if_exists,
                names,
                ..
            } => {
                for name in names {
                    let table = crate::sql::table_name_of(&name);
                    if table.is_empty() {
                        return Err(Status::invalid_argument("invalid table name in DROP"));
                    }
                    match self.catalog.drop_table(&table).await {
                        // 数据文件转孤儿，由孤儿清理回收（plan §4.3）
                        Ok(()) => {
                            self.append_ddl(DdlPayload {
                                op: ddl_op::DROP_TABLE,
                                table: table.clone(),
                                arrow_schema: vec![],
                                default_format: String::new(),
                            })
                            .await?;
                            // DataFusion 本地缓存立即感知表移除
                            self.query
                                .cache()
                                .refresh(&self.catalog)
                                .await
                                .map_err(query_status)?;
                        }
                        Err(LakeError::TableNotFound(_)) if if_exists => {}
                        Err(e) => return Err(lake_status(e)),
                    }
                }
                Ok(SqlOutcome::Affected(0))
            }
            other => Err(Status::unimplemented(format!(
                "only SELECT / SHOW TABLES / INSERT / CREATE TABLE / DROP TABLE are \
                 supported, got: {}",
                sql_snippet(&other.to_string())
            ))),
        }
    }

    /// 批次序列送入 ingest 管线（WAL 权威，与 DoPut 同等持久性，§4.4）。
    async fn ingest_batches(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
        idempotency_key: Option<String>,
    ) -> Result<(), Status> {
        for batch in batches {
            let ib = IngestBatch {
                table: table.to_string(),
                shard_key: "default".to_string(),
                record_batch: batch,
                idempotency_key: idempotency_key.clone(),
                received_at: SystemTime::now(),
            };
            self.ingest.ingest(ib).await.map_err(lake_status)?;
        }
        Ok(())
    }

    /// DDL 事件追加 WAL（Catalog apply 成功后 append，顺序即因果）。
    async fn append_ddl(&self, p: DdlPayload) -> Result<(), Status> {
        self.ingest
            .wal
            .append(Record::Ddl(p))
            .await
            .map_err(lake_status)?;
        Ok(())
    }

    /// 执行无数据 SQL 更新（StatementUpdate / prepared update 的非装载分支）。
    /// 前置分流后 INSERT / DDL 不再打 DataFusion。
    async fn execute_update(&self, sql: &str) -> Result<i64, Status> {
        match self.run_sql(sql).await? {
            SqlOutcome::Rows(_) => Ok(0),
            SqlOutcome::Affected(n) => Ok(n),
        }
    }

    /// FlightSQL 标准轨 DoPut：更新（DDL）或批量装载（数据 append → ingest 管线）。
    async fn sql_do_put(
        &self,
        route: PutRoute,
        first: FlightData,
        mut stream: Streaming<FlightData>,
        tx: tokio::sync::mpsc::Sender<Result<PutResult, Status>>,
    ) -> Result<(), Status> {
        match route {
            PutRoute::StatementUpdate(query) => {
                // 无数据流：排空剩余消息（传播错误），执行 SQL，返回 record_count
                while let Some(f) = stream.next().await {
                    f?;
                }
                let count = self.execute_update(&query).await?;
                let _ = tx.send(Ok(update_put_result(count))).await;
            }
            PutRoute::PreparedStatementUpdate(handle) => {
                let sql = self.prepared_sql(&handle)?;
                match crate::sql::insert_target(&sql) {
                    // INSERT prepared + 绑定数据 → 批量 append（FlightSQL 标准写入路径）
                    Some(table) => {
                        let schema = flight_schema_of(&first).ok_or_else(|| {
                            Status::invalid_argument("first FlightData must carry schema")
                        })?;
                        spawn_sql_ingest(self.ingest.clone(), table, schema, stream, tx);
                    }
                    // 非 INSERT（DDL）：排空流后执行
                    None => {
                        while let Some(f) = stream.next().await {
                            f?;
                        }
                        let count = self.execute_update(&sql).await?;
                        let _ = tx.send(Ok(update_put_result(count))).await;
                    }
                }
            }
            PutRoute::StatementIngest(table) => {
                if table.is_empty() {
                    return Err(Status::invalid_argument(
                        "CommandStatementIngest.table is empty",
                    ));
                }
                let schema = flight_schema_of(&first).ok_or_else(|| {
                    Status::invalid_argument("first FlightData must carry schema")
                })?;
                spawn_sql_ingest(self.ingest.clone(), table, schema, stream, tx);
            }
        }
        Ok(())
    }

    // ------------------------------------------------------ prepared 管理

    fn prepared_sql(&self, handle: &[u8]) -> Result<String, Status> {
        let s = std::str::from_utf8(handle)
            .map_err(|_| Status::invalid_argument("prepared_statement_handle must be UTF-8"))?;
        let id = s
            .strip_prefix(PS_PREFIX)
            .ok_or_else(|| Status::invalid_argument("handle is not a prepared statement handle"))?;
        self.prepared
            .lock()
            .expect("prepared lock poisoned")
            .get(id)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("unknown prepared statement: {id}")))
    }

    /// SELECT 语句的结果集 schema（逻辑计划，尽力而为）。
    /// 空结果集时用于保证 do_get 首条 Schema 消息与 GetFlightInfo 声明一致。
    async fn sql_schema_of(&self, sql: &str) -> Option<SchemaRef> {
        self.query
            .schema_of(sql)
            .await
            .ok()
            .filter(|s| !s.fields().is_empty())
    }
}

/// DoPut 分流结果。
enum PutRoute {
    /// 无数据的 SQL 更新（INSERT / DDL——经 run_sql 前置分流）
    StatementUpdate(String),
    /// 绑定数据 + prepared SQL：INSERT 语句 → 批量 append；其他 → 无数据执行
    PreparedStatementUpdate(Vec<u8>),
    /// FlightSQL 批量装载：数据 append 到指定表
    StatementIngest(String),
}

#[tonic::async_trait]
impl FlightService for FlightServer {
    type HandshakeStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<HandshakeResponse, Status>> + Send>>;
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
        // 无鉴权：返回空 token（FlightSQL 客户端如 ADBC/JDBC 会先握手）
        Ok(Response::new(Box::pin(tokio_stream::iter(vec![Ok(
            HandshakeResponse::default(),
        )]))))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        // cmd = FlightSQL 命令（标准轨）或裸 SQL（简易轨）→ 结果集 schema
        let desc = request.into_inner();
        if let Some(Command::CommandStatementQuery(c)) = Self::decode_command(&desc.cmd) {
            let schema = self
                .query
                .schema_of(&c.query)
                .await
                .ok()
                .unwrap_or_else(|| Arc::new(Schema::empty()));
            let options = arrow::ipc::writer::IpcWriteOptions::default();
            let fd: FlightData = SchemaAsIpc::new(schema.as_ref(), &options).into();
            return Ok(Response::new(SchemaResult {
                schema: fd.data_header,
            }));
        }
        let sql = sql_from_descriptor(&desc)
            .ok_or_else(|| Status::invalid_argument("descriptor.cmd must carry SQL"))?;
        let schema = self
            .query
            .schema_of(&sql)
            .await
            .ok()
            .unwrap_or_else(|| Arc::new(Schema::empty()));
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let fd: FlightData = SchemaAsIpc::new(schema.as_ref(), &options).into();
        Ok(Response::new(SchemaResult {
            schema: fd.data_header,
        }))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        // 标准轨：FlightSQL ticket（Any 编码）
        if let Some(command) = Self::decode_command(&ticket.ticket) {
            // (batches, fallback_schema)：空结果集必须返回查询 schema——
            // 与 GetFlightInfo 声明的 schema 一致（ADBC/JDBC 校验，0 字段空 schema
            // 会被判 "inconsistent schema"）
            let (batches, fallback) = match command {
                Command::TicketStatementQuery(t) => {
                    // S1.6/S1.7：统一经 run_sql 前置分流（INSERT/DDL 亦可通过 do_get 执行）
                    let sql = self.resolve_handle(&t.statement_handle)?;
                    match self.run_sql(&sql).await? {
                        SqlOutcome::Rows(b) if !b.is_empty() => (b, None),
                        SqlOutcome::Rows(_) => (Vec::new(), self.sql_schema_of(&sql).await),
                        SqlOutcome::Affected(_) => (Vec::new(), None),
                    }
                }
                Command::CommandGetCatalogs(_) => (vec![catalogs_batch()], None),
                Command::CommandGetDbSchemas(_) => (vec![schemas_batch()], None),
                Command::CommandGetTableTypes(_) => (vec![table_types_batch()], None),
                Command::CommandGetTables(c) => (self.tables_batches(&c).await?, None),
                // 能力元数据（JDBC/DBeaver 兼容，S1.5 收尾）
                Command::CommandGetSqlInfo(c) => {
                    let batch = c
                        .into_builder(&SQL_INFO_DATA)
                        .build()
                        .map_err(|e| Status::internal(format!("sql info: {e}")))?;
                    (vec![batch], None)
                }
                Command::CommandGetXdbcTypeInfo(c) => {
                    let batch = XDBC_TYPE_INFO
                        .record_batch(c.data_type)
                        .map_err(|e| Status::internal(format!("xdbc type info: {e}")))?;
                    (vec![batch], None)
                }
                _ => {
                    return Err(Status::unimplemented(format!(
                        "do_get for {} not implemented",
                        command.type_url()
                    )));
                }
            };
            let flights = encode_stream_with_schema(batches, fallback)?;
            return Ok(Response::new(Box::pin(tokio_stream::iter(
                flights.into_iter().map(Ok),
            ))));
        }
        // 简易轨：ticket = 裸 UTF-8 SQL（经 run_sql 前置分流）
        let sql = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|_| Status::invalid_argument("ticket must be UTF-8 SQL"))?;
        if sql.trim().is_empty() {
            return Err(Status::invalid_argument("empty SQL in ticket"));
        }
        let (batches, fallback) = match self.run_sql(&sql).await? {
            SqlOutcome::Rows(b) if !b.is_empty() => (b, None),
            SqlOutcome::Rows(_) => (Vec::new(), self.sql_schema_of(&sql).await),
            // 写入/DDL：空结果（仅 schema）
            SqlOutcome::Affected(_) => (Vec::new(), None),
        };
        let flights = encode_stream_with_schema(batches, fallback)?;
        Ok(Response::new(Box::pin(tokio_stream::iter(
            flights.into_iter().map(Ok),
        ))))
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

        // ---- 标准轨路由：FlightSQL 更新/装载命令（全部汇入 ingest 管线）----
        let cmd = first
            .flight_descriptor
            .as_ref()
            .map(|d| d.cmd.to_vec())
            .unwrap_or_default();
        if let Some(route) = Self::put_route(&cmd) {
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<PutResult, Status>>(4);
            self.sql_do_put(route, first, stream, tx).await?;
            return Ok(Response::new(Box::pin(
                tokio_stream::wrappers::ReceiverStream::new(rx),
            )));
        }

        // ---- 简易轨（写入）：descriptor.path = [table, shard] ----
        let (table, shard) = match &first.flight_descriptor {
            Some(d) => extract_table_shard(&d.path).map_err(lake_status)?,
            None => return Err(Status::invalid_argument("missing flight descriptor")),
        };

        // 幂等键按批次解析（app_metadata 每条消息均可携带，§7.3）
        let schema = flight_schema_of(&first)
            .ok_or_else(|| Status::invalid_argument("first FlightData must carry schema"))?;

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<PutResult, Status>>(64);
        let ingest = self.ingest.clone();

        tokio::spawn(async move {
            let dict_ids: HashMap<i64, ArrayRef> = HashMap::new();
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
                    &dict_ids,
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
                match ingest.ingest(ib).await {
                    Ok(receipt) => {
                        let payload = serde_json::to_vec(&receipt).unwrap_or_default();
                        tracing::debug!(rows = receipt.row_count, "simple-track ack sending");
                        if ack_stream
                            .send(Ok(PutResult {
                                app_metadata: payload.into(),
                            }))
                            .await
                            .is_err()
                        {
                            tracing::debug!("simple-track ack send failed (receiver closed)");
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = ack_stream.send(Err(lake_status(e))).await;
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
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        match action.r#type.as_str() {
            // 【协议约定】action 请求/响应均为 Any 包装（与官方 FlightSqlService
            // blanket 实现一致；ADBC/Go 驱动按 Any 解码）
            "CreatePreparedStatement" => {
                let any = Any::decode(&*action.body)
                    .map_err(|e| Status::invalid_argument(format!("decode Any: {e}")))?;
                let req = any
                    .unpack::<ActionCreatePreparedStatementRequest>()
                    .map_err(|e| Status::invalid_argument(format!("unpack: {e}")))?
                    .ok_or_else(|| {
                        Status::invalid_argument("expected ActionCreatePreparedStatementRequest")
                    })?;
                let handle = format!("{PS_PREFIX}{}", uuid::Uuid::now_v7());
                // dataset_schema：优先逻辑计划；INSERT 等 DML 无法规划 → 回退目标表 schema
                let schema = match self.query.schema_of(&req.query).await.ok() {
                    Some(s) if !s.fields().is_empty() => Some(s),
                    _ => match crate::sql::insert_target(&req.query) {
                        Some(t) => self
                            .query
                            .schema_of(&format!("SELECT * FROM {t}"))
                            .await
                            .ok(),
                        None => None,
                    },
                };
                let dataset_schema = schema
                    .and_then(|s| schema_ipc_bytes(&s))
                    .unwrap_or_default();
                let result = ActionCreatePreparedStatementResult {
                    prepared_statement_handle: handle.clone().into_bytes().into(),
                    dataset_schema: dataset_schema.into(),
                    parameter_schema: Vec::new().into(), // MVP 不支持绑定参数
                };
                self.prepared
                    .lock()
                    .expect("prepared lock poisoned")
                    .insert(handle.trim_start_matches(PS_PREFIX).to_string(), req.query);
                return Ok(Response::new(Box::pin(tokio_stream::iter(vec![Ok(
                    arrow_flight::Result {
                        body: result.as_any().encode_to_vec().into(),
                    },
                )]))));
            }
            "ClosePreparedStatement" => {
                let any = Any::decode(&*action.body)
                    .map_err(|e| Status::invalid_argument(format!("decode Any: {e}")))?;
                let req = any
                    .unpack::<ActionClosePreparedStatementRequest>()
                    .map_err(|e| Status::invalid_argument(format!("unpack: {e}")))?
                    .ok_or_else(|| {
                        Status::invalid_argument("expected ActionClosePreparedStatementRequest")
                    })?;
                let s = String::from_utf8(req.prepared_statement_handle.to_vec())
                    .map_err(|_| Status::invalid_argument("handle must be UTF-8"))?;
                if let Some(id) = s.strip_prefix(PS_PREFIX) {
                    self.prepared
                        .lock()
                        .expect("prepared lock poisoned")
                        .remove(id);
                }
                return Ok(Response::new(Box::pin(tokio_stream::iter(Vec::<
                    Result<arrow_flight::Result, Status>,
                >::new(
                )))));
            }
            _ => {}
        }
        Err(Status::unimplemented("do_action not implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            ActionType {
                r#type: "CreatePreparedStatement".to_string(),
                description: "Create a prepared statement from a SQL query".to_string(),
            },
            ActionType {
                r#type: "ClosePreparedStatement".to_string(),
                description: "Close a prepared statement".to_string(),
            },
        ];
        Ok(Response::new(Box::pin(tokio_stream::iter(
            actions.into_iter().map(Ok),
        ))))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights not implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // cmd = FlightSQL 命令（标准轨）或裸 SQL（简易轨）
        let desc = request.into_inner();
        if let Some(command) = Self::decode_command(&desc.cmd) {
            match command {
                Command::CommandStatementQuery(c) => {
                    let info = self.query_info(c.query).await?;
                    return Ok(Response::new(info));
                }
                Command::CommandPreparedStatementQuery(c) => {
                    let sql = self.resolve_handle(&c.prepared_statement_handle)?;
                    let info = self.query_info(sql).await?;
                    return Ok(Response::new(info));
                }
                Command::CommandGetCatalogs(_)
                | Command::CommandGetDbSchemas(_)
                | Command::CommandGetTables(_)
                | Command::CommandGetTableTypes(_)
                | Command::CommandGetSqlInfo(_)
                | Command::CommandGetXdbcTypeInfo(_) => {
                    // 元数据：ticket = 原命令原样回传（do_get 侧再解码执行）
                    return Ok(Response::new(endpoint_info(desc.cmd.to_vec(), -1, None)));
                }
                _ => {}
            }
        }
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
                location: vec![], // 空 = 使用当前连接（Go 驱动不接受 uri=""）
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
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }
}

// ------------------------------------------------------------- 路由辅助

/// run_sql 前置分流结果。
pub(crate) enum SqlOutcome {
    /// 结果集（SELECT / SHOW TABLES）
    Rows(Vec<RecordBatch>),
    /// 受影响行数（INSERT / DDL）
    Affected(i64),
}

// ------------------------------------------------------ SQL 能力元数据（JDBC/DBeaver 兼容）

/// 服务器能力元数据（`CommandGetSqlInfo` 响应，静态构建）。
///
/// JDBC 驱动（flight-sql-jdbc-driver / DBeaver 自定义驱动）握手后会拉取这些
/// 元数据决定客户端行为（是否只读 / 是否支持事务 / 超时等）。
static SQL_INFO_DATA: LazyLock<SqlInfoData> = LazyLock::new(|| {
    let mut b = SqlInfoDataBuilder::new();
    b.append(SqlInfo::FlightSqlServerName, "yuntun");
    b.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    // Arrow（arrow-rs）版本：build.rs 从 workspace Cargo.lock 编译期提取
    // （arrow-rs 不导出版本常量；未提取到时回退 workspace 声明的主线版本）
    b.append(
        SqlInfo::FlightSqlServerArrowVersion,
        option_env!("YUNTUN_ARROW_VERSION").unwrap_or("59"),
    );
    b.append(SqlInfo::FlightSqlServerReadOnly, false);
    b.append(SqlInfo::FlightSqlServerSql, true);
    b.append(SqlInfo::FlightSqlServerSubstrait, false);
    // 无事务支持（单语句自动提交语义；JDBC 端不发起 BeginTransaction）
    b.append(
        SqlInfo::FlightSqlServerTransaction,
        SqlSupportedTransaction::None as i32,
    );
    b.append(SqlInfo::FlightSqlServerCancel, false);
    b.append(SqlInfo::FlightSqlServerBulkIngestion, false);
    b.append(SqlInfo::FlightSqlServerStatementTimeout, 0i64);
    b.append(SqlInfo::FlightSqlServerTransactionTimeout, 0i64);
    b.build().expect("static sql info data")
});

/// 类型字典行：(name, xdbc 类型, column_size, case_sensitive, unsigned, create_params, min_scale, max_scale)
type XdbcTypeRow = (
    &'static str,
    XdbcDataType,
    Option<i32>,
    bool,
    Option<bool>,
    Option<&'static str>,
    Option<i32>,
    Option<i32>,
);

/// 类型字典（`CommandGetXdbcTypeInfo` 响应）：对齐 `CREATE TABLE` 的类型映射。
static XDBC_TYPE_INFO: LazyLock<XdbcTypeInfoData> = LazyLock::new(|| {
    let mut b = XdbcTypeInfoDataBuilder::new();
    let types: &[XdbcTypeRow] = &[
        (
            "BOOLEAN",
            XdbcDataType::XdbcBit,
            Some(1),
            false,
            None,
            None,
            None,
            None,
        ),
        (
            "TINYINT",
            XdbcDataType::XdbcTinyint,
            Some(8),
            false,
            Some(false),
            None,
            None,
            None,
        ),
        (
            "SMALLINT",
            XdbcDataType::XdbcSmallint,
            Some(16),
            false,
            Some(false),
            None,
            None,
            None,
        ),
        (
            "INTEGER",
            XdbcDataType::XdbcInteger,
            Some(32),
            false,
            Some(false),
            None,
            None,
            None,
        ),
        (
            "BIGINT",
            XdbcDataType::XdbcBigint,
            Some(64),
            false,
            Some(false),
            None,
            None,
            None,
        ),
        (
            "REAL",
            XdbcDataType::XdbcReal,
            Some(24),
            false,
            Some(false),
            None,
            None,
            None,
        ),
        (
            "DOUBLE",
            XdbcDataType::XdbcDouble,
            Some(53),
            false,
            Some(false),
            None,
            None,
            None,
        ),
        (
            "DECIMAL",
            XdbcDataType::XdbcDecimal,
            Some(38),
            false,
            Some(false),
            Some("precision, scale"),
            Some(0),
            Some(38),
        ),
        (
            "VARCHAR",
            XdbcDataType::XdbcVarchar,
            None,
            true,
            None,
            None,
            None,
            None,
        ),
        (
            "BINARY",
            XdbcDataType::XdbcBinary,
            None,
            false,
            None,
            None,
            None,
            None,
        ),
        (
            "DATE",
            XdbcDataType::XdbcDate,
            Some(10),
            false,
            None,
            None,
            None,
            None,
        ),
        (
            "TIMESTAMP",
            XdbcDataType::XdbcTimestamp,
            Some(29),
            false,
            None,
            None,
            Some(0),
            Some(9),
        ),
    ];
    let quoted = |dt: &XdbcDataType| {
        matches!(
            dt,
            XdbcDataType::XdbcVarchar
                | XdbcDataType::XdbcBinary
                | XdbcDataType::XdbcVarbinary
                | XdbcDataType::XdbcDate
                | XdbcDataType::XdbcTimestamp
        )
    };
    for (name, dt, size, case_sensitive, unsigned, create_params, min_scale, max_scale) in types {
        let literal = quoted(dt).then(|| "'".to_string());
        b.append(XdbcTypeInfo {
            type_name: (*name).into(),
            data_type: *dt,
            column_size: *size,
            literal_prefix: literal.clone(),
            literal_suffix: literal,
            create_params: create_params.map(|s| vec![s.to_string()]),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: *case_sensitive,
            searchable: if *case_sensitive {
                Searchable::Char
            } else {
                Searchable::Full
            },
            unsigned_attribute: *unsigned,
            fixed_prec_scale: *dt == XdbcDataType::XdbcDecimal,
            auto_increment: Some(false),
            local_type_name: Some((*name).into()),
            minimum_scale: *min_scale,
            maximum_scale: *max_scale,
            sql_data_type: *dt,
            datetime_subcode: None,
            num_prec_radix: matches!(
                dt,
                XdbcDataType::XdbcTinyint
                    | XdbcDataType::XdbcSmallint
                    | XdbcDataType::XdbcInteger
                    | XdbcDataType::XdbcBigint
                    | XdbcDataType::XdbcReal
                    | XdbcDataType::XdbcDouble
                    | XdbcDataType::XdbcBit
            )
            .then_some(2),
            interval_precision: None,
        });
    }
    b.build().expect("static xdbc type info")
});

/// 错误信息用 SQL 片段（截断防刷屏）。
fn sql_snippet(s: &str) -> String {
    let t = s.trim();
    if t.chars().count() > 80 {
        format!("{}...", t.chars().take(80).collect::<String>())
    } else {
        t.to_string()
    }
}

/// INSERT 语句目标表名解析（双引号 / 反引号 / 裸名）。解析失败 → None。
///
/// 仅作 [`crate::sql::insert_target`]（AST 优先）的**回落**：
/// prepared statement 允许不完整形态 `INSERT INTO t (a, b)`（无 VALUES/SELECT 源），
/// sqlparser 解析失败时由此字符串匹配兜底（S1.6 后保留该回落路径）。
pub(crate) fn insert_target_table(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    let pos = lower.find("insert into")?;
    let rest = sql[pos + "insert into".len()..].trim_start();
    let bytes = rest.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    let (name, _) = match bytes[0] {
        b'"' => {
            let end = rest[1..].find('"')? + 1;
            (&rest[1..end], end + 1)
        }
        b'`' => {
            let end = rest[1..].find('`')? + 1;
            (&rest[1..end], end + 1)
        }
        _ => {
            let end = rest
                .find(|c: char| c.is_whitespace() || c == '(' || c == ';')
                .unwrap_or(rest.len());
            (rest[..end].trim_end(), end)
        }
    };
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// 从首条 FlightData 的 data_header 解析 Arrow Schema（IPC Message → Schema）。
fn flight_schema_of(fd: &FlightData) -> Option<SchemaRef> {
    let ipc = arrow::ipc::root_as_message(&fd.data_header).ok()?;
    let fb_schema = ipc.header_as_schema()?;
    Some(Arc::new(arrow::ipc::convert::fb_to_schema(fb_schema)))
}

/// 批次 → IPC 流（首条 Schema 消息 + 数据消息）。
/// `batches` 为空且提供 `fallback` 时，用 fallback schema（查询逻辑计划）
/// 而非 0 字段空 schema——客户端（ADBC/JDBC）会校验与 GetFlightInfo 一致性。
fn encode_stream_with_schema(
    batches: Vec<RecordBatch>,
    fallback: Option<SchemaRef>,
) -> Result<Vec<FlightData>, Status> {
    if batches.is_empty() {
        let schema = fallback.unwrap_or_else(|| Arc::new(Schema::empty()));
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let fd: FlightData = SchemaAsIpc::new(schema.as_ref(), &options).into();
        return Ok(vec![fd]);
    }
    let schema = batches[0].schema();
    arrow_flight::utils::batches_to_flight_data(schema.as_ref(), batches)
        .map_err(|e| Status::internal(format!("flight encode: {e}")))
}

/// schema → **IPC 封装消息格式**字节（continuation + length + flatbuffer）。
///
/// 【协议约定】嵌入 protobuf 的 schema 字段（dataset_schema /
/// GetTables.table_schema 列）按规范为 "Arrow IPC-encapsulated message format"——
/// 必须带 0xFFFFFFFF continuation + u32 长度前缀。Go/ADBC 驱动按此解析；
/// 裸 flatbuffer 会报 "invalid message metadata"。
/// （注意：FlightData.data_header 本身仍是裸 flatbuffer，不适用此处。）
fn schema_ipc_bytes(schema: &SchemaRef) -> Option<Vec<u8>> {
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let fd: FlightData = SchemaAsIpc::new(schema.as_ref(), &options).into();
    let header = fd.data_header;
    let mut out = Vec::with_capacity(8 + header.len());
    out.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
    out.extend_from_slice(&(header.len() as u32).to_le_bytes());
    out.extend_from_slice(&header);
    Some(out)
}

/// FlightSQL 更新响应：单条 PutResult（app_metadata = DoPutUpdateResult）。
fn update_put_result(record_count: i64) -> PutResult {
    use arrow_flight::sql::DoPutUpdateResult;
    PutResult {
        app_metadata: DoPutUpdateResult { record_count }.encode_to_vec().into(),
    }
}

/// 构造带单 endpoint 的 FlightInfo（ticket 已编码）。
fn endpoint_info(ticket: Vec<u8>, total_records: i64, schema: Option<SchemaRef>) -> FlightInfo {
    let schema_bytes = schema
        .and_then(|s| schema_ipc_bytes(&s))
        .unwrap_or_default();
    FlightInfo {
        schema: schema_bytes.into(),
        flight_descriptor: Some(FlightDescriptor {
            r#type: 2, // CMD
            cmd: ticket.clone().into(),
            path: vec![],
        }),
        endpoint: vec![FlightEndpoint {
            ticket: Some(Ticket {
                ticket: ticket.into(),
            }),
            // 空 location 列表 = 使用当前连接（Go/ADBC 驱动不接受 uri=""，
            // 会当作字面地址拨号失败；pyarrow 两者皆可）
            location: vec![],
            expiration_time: None,
            app_metadata: Default::default(),
        }],
        total_records,
        total_bytes: -1,
        ordered: false,
        app_metadata: Default::default(),
    }
}

fn column_str(batch: &RecordBatch, idx: usize) -> Vec<String> {
    (0..batch.num_rows())
        .map(|i| {
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .map(|a| a.value(i).to_string())
                .unwrap_or_default()
        })
        .collect()
}

/// LakeError → tonic Status（错误语义映射）。
fn lake_status(e: LakeError) -> Status {
    match e {
        LakeError::SchemaIncompatible(_) => Status::invalid_argument(e.to_string()),
        LakeError::IdempotencyKeyRequired => Status::failed_precondition(e.to_string()),
        LakeError::IdempotencyKeyTooLong => Status::invalid_argument(e.to_string()),
        LakeError::TableNotFound(_) => Status::not_found(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}

/// QueryEngine（DataFusion）错误 → tonic Status。
fn query_status(e: impl std::fmt::Display) -> Status {
    Status::internal(format!("query: {e}"))
}

/// FlightSQL 装载任务：解码剩余数据流逐批 append（ingest 管线），结束返回累计行数。
fn spawn_sql_ingest(
    ingest: Arc<Ingestor>,
    table: String,
    schema: SchemaRef,
    mut stream: Streaming<FlightData>,
    tx: tokio::sync::mpsc::Sender<Result<PutResult, Status>>,
) {
    tokio::spawn(async move {
        let dict_ids: HashMap<i64, ArrayRef> = HashMap::new();
        let mut rows: i64 = 0;
        // 语句级幂等键：同一次 do_put 的批次共享一个键（满足 require 表的强制检查；
        // Meta 层去重仍以 batch_id 为准，S1.8 再做客户端透传）
        let stmt_key = Some(format!("flightsql-{}", uuid::Uuid::now_v7()));
        loop {
            let fd = tokio::select! {
                f = stream.next() => match f {
                    Some(Ok(f)) => f,
                    Some(Err(s)) => { let _ = tx.send(Err(s)).await; return; }
                    None => break,
                },
                _ = tx.closed() => return,
            };
            if fd.data_header.is_empty() && fd.data_body.is_empty() {
                continue;
            }
            let batch = match arrow_flight::utils::flight_data_to_arrow_batch(
                &fd,
                schema.clone(),
                &dict_ids,
            ) {
                Ok(b) => b,
                Err(e) => {
                    let _ = tx
                        .send(Err(Status::invalid_argument(format!("decode batch: {e}"))))
                        .await;
                    return;
                }
            };
            rows += batch.num_rows() as i64;
            let ib = IngestBatch {
                table: table.clone(),
                shard_key: "default".into(),
                record_batch: batch,
                idempotency_key: stmt_key.clone(),
                received_at: SystemTime::now(),
            };
            if let Err(e) = ingest.ingest(ib).await {
                let _ = tx.send(Err(lake_status(e))).await;
                return;
            }
        }
        let _ = tx.send(Ok(update_put_result(rows))).await;
    });
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
        return String::from_utf8(desc.cmd.to_vec())
            .ok()
            .map(|s| s.trim().to_string());
    }
    match desc.path.as_slice() {
        [one] if !one.is_empty() => Some(one.clone()),
        _ => None,
    }
}

// ------------------------------------------------------ metadata batches

fn catalogs_batch() -> RecordBatch {
    // 【官方 schema】catalog_name: utf8 NOT NULL（metadata/catalogs.rs）
    let schema = Arc::new(Schema::new(vec![Field::new(
        "catalog_name",
        DataType::Utf8,
        false,
    )]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![CATALOG_NAME])) as ArrayRef],
    )
    .expect("static metadata batch")
}

fn schemas_batch() -> RecordBatch {
    // 【官方 schema】catalog_name nullable / db_schema_name NOT NULL（metadata/db_schemas.rs）
    let schema = Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new("db_schema_name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some(CATALOG_NAME)])) as ArrayRef,
            Arc::new(StringArray::from(vec![SCHEMA_NAME])) as ArrayRef,
        ],
    )
    .expect("static metadata batch")
}

fn table_types_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "table_type",
        DataType::Utf8,
        false,
    )]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["TABLE", "VIEW"])) as ArrayRef],
    )
    .expect("static metadata batch")
}

/// 供测试/客户端构造 Command Any 编码。
pub fn command_bytes<C: ProstMessageExt>(c: &C) -> Vec<u8> {
    c.as_any().encode_to_vec()
}
