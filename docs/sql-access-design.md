# SQL 访问协议设计（MySQL / Flight SQL 多协议接入）

> **版本**：v1.1（设计稿；v1.1 评审修订 2026-09-09，见文末变更记录）
> **日期**：2026-09-09
> **适用范围**：阶段 1 收尾（基础能力 + Flight SQL + MySQL 两个协议端口）至
> 阶段 3（独立 SQL 节点）；PostgreSQL wire 为后续扩展（设计预留，§5.5）
> **关联文档**：`architecture.md`（§3.2 v12.3/v12.4）、`ingestor-design.md`（灌入域，
> 本文档与其对称）、`operation-log.md`（§7.5 分层定稿、§8/§9 SQL 前置拦截）
>
> **本文定位**：把"SQL 处理"从 Flight 协议中**显式解耦为能力层**，使 MySQL wire
> 与 Flight SQL 共用同一套 SQL 语义；定义 SQL 处理层与协议适配层（端口监听 /
> 协议解析 / 数据编码）的边界；给出 standalone 集成与独立 SQL 节点两种组合形态。

---

## 一、背景与动机

### 1.1 问题

当前外部 SQL 访问只有 Flight SQL 一条通道，且存在两个结构性问题：

1. **SQL 语义与协议耦合**：v12.4 的 SQL 前置分流（`run_sql`：SELECT→DataFusion /
   INSERT→ingest / DDL→catalog）实现在 `yuntun-server::flight::FlightServer` 内部。
   这套逻辑本质是 **SQL 执行能力**，与 Flight 传输无关——MySQL/PG wire 协议需要
   **同一套语义**，不能复制一份或反向依赖 Flight；
2. **wire 客户端的会话语义缺口**：MySQL 客户端（JDBC、mysql CLI、DBeaver）的行为
   与 Flight 客户端截然不同——非限定表名（`FROM t` 而非 `FROM yuntun.public.t`）、
   预编译语句 + 参数绑定（`?`/`$1`）、方言 SQL（backtick 引用、`SHOW`、`SET`、
   `SELECT @@var`）、元数据探测（information_schema / SHOW COLUMNS）。这些都需要
   协议适配层与 SQL 处理层配合解决。

### 1.2 目标

- **一套 SQL 语义，多协议共用**：SQL 处理层是唯一权威实现；MySQL / Flight SQL
  适配层（后续 PG 等）只做协议翻译（解码 / 编码 / 会话），零语义复制；
- **分层清晰**：SQL 处理层（能力 crate，无传输）⇄ 协议适配层（wire 协议实现 +
  数据编码）⇄ 节点层（端口监听装配）；
- **可组合**：standalone 按配置挂载端口；阶段 3 可装配出独立的 SQL 节点
  （`yuntun-sqld`），与独立 ingestor 节点对称；
- **兼容性分级明确**：MVP 兼容到什么程度（CLI/驱动直连、DBeaver 元数据浏览）
  有清晰的验收定义，避免"无限兼容"吞噬范围。

### 1.3 非目标

- 不实现 MySQL/PG 的完整服务器语义：**无事务**（单语句自动提交）、无用户
  权限体系、无复制、无存储过程 / 触发器 / 视图 DDL；
- 不做 SQL 方言的完全翻译（客户端方言与 DataFusion 语法的长尾差异按
  兼容分级处理，见 §5.4）；
- 不引入 MySQL/PG 的 binlog / 逻辑复制接入（那是灌入域的事，见
  `ingestor-design.md`）。

---

## 二、现状分析

### 2.1 已有的能力底盘（可复用）

| 能力 | 位置 | 说明 |
|---|---|---|
| SELECT 执行 | `yuntun-query::QueryEngine` | DataFusion，`sql()` / `schema_of()`；每次查询新建 SessionContext（会话隔离）；information_schema 已开启 |
| 写入 | `yuntun-ingest::Ingestor::ingest()` | await = WAL fsync；Schema 演进；幂等矩阵 |
| 元数据 | `yuntun-catalog::CatalogOps` | create/drop_table/list_tables/table_schema |
| SQL 前置分流 | `yuntun-server::flight` + `server/src/sql.rs` | **run_sql 嵌在 FlightServer**（问题所在）；`sql.rs` 已是纯函数模块（sqlparser 解析 / CREATE 映射 / VALUES 构造 / cast） |
| 幂等键 | 语句级 `dml-<uuid>`（S1.8 将支持客户端透传） | — |

### 2.2 wire 客户端的缺口清单（逐项对应设计动作）

| # | 缺口 | 影响 | 设计动作 |
|---|---|---|---|
| G1 | `run_sql` 嵌在 FlightServer | MySQL/PG 无法共用 | 提取为 `yuntun-sql` 能力 crate（§四） |
| G2 | DataFusion 会话默认 catalog 是 `datafusion` | 非限定表名 `FROM t` 失败——wire 客户端必然踩中 | SessionConfig 设置 default_catalog/schema（§4.2） |
| G3 | 不支持参数绑定（`?` / `$1`） | JDBC/驱动几乎必用 prepared statement | prepare / execute_prepared + AST 级参数替换（§4.3） |
| G4 | 无 MySQL wire 协议实现 | 无法用 mysql 客户端 / DBeaver(MySQL) 直连 | 协议适配层（§五）：opensrv-mysql（PG wire 后续，§5.5） |
| G5 | 无方言适配（backtick、SHOW、SET、SELECT @@var、USE） | 客户端握手/元数据探测失败 | 方言 shim 层（§4.4） |
| G6 | 元数据浏览缺 API | DBeaver 表/列浏览 | SqlEngine 元数据 API（catalog 支撑）+ information_schema |
| G7 | 结果全量缓冲 | 大结果集内存（既有 S1.10 遗留） | 接口预留流式；MVP 维持 eager（S1.10 统一清偿） |

---

## 三、核心裁决：SQL 处理层与协议适配层

### 3.1 分层总览

```
┌────────────────────────── 节点层（端口监听 + 装配） ──────────────────────────┐
│  standalone / yuntun-sqld(阶段3)：按配置挂载端口，管理连接会话与生命周期      │
│                                                                              │
│  ┌───────────── 协议适配层（yuntun-sqlwire） ─────────────┐                 │
│  │ MySQL wire（3306）      │ Flight SQL（gRPC，已有）      │                 │
│  │ 端口监听·握手·COM_* 解析 │ FlightServer                  │ ← 协议解析      │
│  │ 文本/二进制编码          │ IPC 编码                      │ ← 数据编码      │
│  │ （PG wire：后续扩展，§5.5）                             │                 │
│  └───────┬───────────────────────┬────────────────────────┘                 │
│          │   SqlValue 参数 + 统一结果表示（SqlResult）                      │
└──────────┼───────────────────────┼──────────────────────────────────────────┘
           ▼                     ▼                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                    SQL 处理层（yuntun-sql 能力 crate）                        │
│  SqlEngine：execute(sql, dialect, session) → SqlResult                       │
│   · 前置分流（v12.4）：Query→query / Insert→ingest / DDL·Show→catalog        │
│   · prepare / execute_prepared（参数绑定，AST 级替换）                        │
│   · 方言参数化（MySql/PostgreSql/Generic → sqlparser）                        │
│   · 元数据 API（tables/columns，catalog 支撑）                                │
│   无 listener、无 wire 格式、无连接概念（会话由适配层持有并显式传入）          │
└──────────────────────────────────────────────────────────────────────────────┘
           │                    │                  │
           ▼                    ▼                  ▼
   query 能力(DataFusion)   ingest 能力(WAL)   catalog 能力(Meta)
```

### 3.2 裁决表

| # | 裁决 | 内容 | 理由 |
|---|---|---|---|
| **S-1** | **SQL 处理层是能力 crate** | 新建 `yuntun-sql`：SQL 前置分流 + 参数绑定 + 方言参数化 + 元数据 API。迁移 flight.rs 现有 run_sql，`sql.rs` 纯函数模块随迁 | 两协议（后续更多）共用；v12.4 逻辑本就与 Flight 无关 |
| **S-2** | **协议适配层独立 crate** | 新建 `yuntun-sqlwire`：**MySQL wire**（opensrv-mysql）协议解析 + 数据编码 + 方言 shim。feature 门（mysql）。**依赖 yuntun-sql，不依赖 server**；PG wire 为后续扩展（§5.5） | 与 ingestor 域同构（解码器在域能力 crate，端点在节点层）；wire 适配可脱离 listener 单测 |
| **S-3** | **Flight SQL 归 SQL 域管理** | FlightServer 保留在 server（gRPC 端口特殊性：读写一体），内部改调 `yuntun-sql::SqlEngine`，删除内嵌 run_sql | 端口不迁移（既有互操作资产），语义实现唯一化 |
| **S-4** | **会话在适配层，引擎无状态** | `SqlEngine` 共享单例；连接会话（prepared stmt 表、当前 db、方言）由适配层持有，以 SessionCtx 显式传入 | 引擎可跨连接复用；会话语义本就是协议概念（COM_STMT / PG unnamed statement） |
| **S-5** | **写入直通 ingest** | wire 协议的 INSERT/DDL 与 Flight 完全同路径：SqlEngine → ingest / catalog（铁律①不破） | 双协议写入合流同一管线，持久性/幂等语义一致 |
| **S-6** | **无事务 MVP** | BEGIN/COMMIT/SET autocommit 由 shim 接受为 no-op（autocommit 恒 true） | DataFusion 无事务；JDBC 驱动默认 autocommit=true 可直连；显式事务报明确错误 |
| **S-7** | **鉴权 MVP = trust + 静态账号** | 配置 `users = [{user, password}]`（mysql native password）；无鉴权模式（users 空）与 Flight handshake 空 token 一致起步 | DBeaver 需要账号字段可填；SCRAM/更强认证后续 |

---

## 四、SQL 处理层设计（yuntun-sql 能力 crate）

### 4.1 crate 结构

```
crates/sql/
├── src/
│   ├── lib.rs          # SqlEngine（编排：分流 + 会话参数注入）
│   ├── engine.rs       # execute / prepare / execute_prepared / 元数据 API
│   ├── session.rs      # SessionCtx { dialect, default_db }（由适配层构造传入）
│   ├── result.rs       # SqlResult / SqlValue / PreparedStatement 定义
│   ├── dispatch.rs     # 现 flight.rs run_sql 分流逻辑迁移（Query/Insert/DDL/Show）
│   ├── params.rs       # AST 级参数替换（Placeholder → 字面量 AST）
│   ├── shim/           # 方言 shim：SHOW/SET/USE/SELECT @@var → canned / catalog
│   └── sql.rs 现有纯函数模块随迁（解析/CREATE 映射/VALUES 构造/cast/时间解析）
└── Cargo.toml          # deps: yuntun-ingest, yuntun-query, yuntun-catalog, model, sqlparser, arrow
```

依赖方向：`sql → ingest, query, catalog, model`；**不依赖 server / wire 协议**。
server 改为依赖 `sql`（flight.rs 的 run_sql / SqlOutcome 删除，改调 SqlEngine）。

### 4.2 SqlEngine API

```rust
pub struct SqlEngine {
    ingest: Arc<Ingestor>,
    query:  Arc<QueryEngine>,
    catalog: Arc<dyn CatalogOps>,
}

pub struct SessionCtx {
    pub dialect: Dialect,           // MySql | PostgreSql | Generic（sqlparser 方言）
    pub default_db: Option<String>, // USE db 预留；MVP 恒 yuntun.public
}

pub enum SqlResult {
    /// 结果集：schema + 批次（eager；S1.10 流式化时内部换 Stream，接口形态不变）
    Rows { schema: SchemaRef, batches: Vec<RecordBatch> },
    /// 受影响行数（INSERT / DDL，S1.6 语义）
    Affected(i64),
}

impl SqlEngine {
    pub async fn execute(&self, sql: &str, session: &SessionCtx)
        -> Result<SqlResult, SqlError>;                       // 简单协议一次性执行
    pub async fn prepare(&self, sql: &str, session: &SessionCtx)
        -> Result<PreparedStatement, SqlError>;               // 方言感知预编译
    pub async fn execute_prepared(&self, stmt: &PreparedStatement,
        params: &[SqlValue], session: &SessionCtx)
        -> Result<SqlResult, SqlError>;                       // 绑定执行
    pub async fn list_tables(&self) -> Result<Vec<TableInfo>, SqlError>;
    pub async fn describe_table(&self, name: &str) -> Result<TableSchema, SqlError>;
    pub async fn schema_of(&self, sql: &str) -> Option<SchemaRef>; // 空结果 schema（S1.5 语义迁入）
}
```

执行流程（与 S1.6 分流一致，**迁移而非重写**）：

```
parse(方言) → 单语句校验（多语句拒绝）
  ├─ Query          → query.sql（SessionConfig: default_catalog=yuntun, default_schema=public）
  ├─ ShowTables     → catalog.list_tables（三列批次，表结构同 DataFusion）
  ├─ Insert+Values  → 表 schema 校验 → 字面量/参数 → RecordBatch → ingest（dml-<uuid>）
  ├─ Insert+Select  → query 执行源 → cast → ingest
  ├─ CreateTable    → ColumnDef→Arrow → catalog.create_table → WAL Ddl
  ├─ Drop(Table)    → catalog.drop_table → WAL Ddl
  └─ 其他           → 拒绝（支持列表提示）——shim 层先拦截的语句到不了这里
```

**关键点 G2 修复**：QueryEngine 的 SessionConfig 增加
`with_default_catalog_and_schema(CATALOG_NAME, SCHEMA_NAME)`（yuntun/public），
非限定表名 `FROM t`、`FROM information_schema.tables` 对 wire 客户端直接可用。
QueryEngine 增加带 SessionConfig 的执行入口（既有 `sql()` 缺省行为不变，向后兼容）。

### 4.3 参数绑定（G3）

协议层把 wire 参数解码为统一的 SqlValue：

```rust
pub enum SqlValue {
    Null, Bool(bool), Int(i64), UInt(u64), Float(f64),
    Str(String), Bytes(Vec<u8>), TsNs(i64), Date(i32),
}
```

替换策略（**AST 级，非字符串拼接**）：prepare 阶段解析出 AST，定位
`Expr::Value(Value::Placeholder("?"))` / `$n`，按序号生成字面量 AST 节点
（Number / SingleQuotedString / TypedString(Timestamp)）替换并缓存"已绑定 AST"；
execute_prepared 直接走分流。类型校验复用 INSERT VALUES 的 Literal 中间表示与
范围检查——`sql.rs` 既有代码直接复用。值不进 SQL 文本（无注入面）。

### 4.4 方言 shim（G5，shim/ 模块）

wire 客户端（尤其 DBeaver/JDBC 握手期）发出的"非数据语句"在分流前拦截：

| 客户端语句 | 处理 | 响应 |
|---|---|---|
| SET NAMES / SET autocommit=1 / SET sql_mode=... | no-op | OK（0 行） |
| USE db | 记入会话（MVP 校验后记 default_db） | OK |
| SELECT @@version / @@autocommit / DATABASE() | canned：yuntun-0.1.0 / 1 / public | 结果集 |
| SHOW VARIABLES [LIKE ...] | canned 小表（version/autocommit/max_allowed_packet…） | 结果集 |
| SHOW COLLATION / CHARSET | canned 最小集 | 结果集 |
| SHOW TABLES / SHOW FULL TABLES | SqlEngine.list_tables | 结果集 |
| SHOW COLUMNS FROM t / DESCRIBE t | SqlEngine.describe_table → 列描述 | 结果集 |
| SHOW CREATE TABLE t | describe_table → 反推 DDL | 结果集 |
| BEGIN / COMMIT / ROLLBACK | S-6：no-op / ROLLBACK 报"不支持事务" | OK / 明确错误 |
| SELECT ... FROM information_schema.* | **不拦截**——DataFusion 原生支持（表/列清单够 DBeaver 建模） | DataFusion |

shim 是**适配层组件**（方言相关），但表/列数据产出走 SqlEngine 元数据 API
（S-2 边界：shim 不直接查 catalog）。

---

## 五、协议适配层设计（MVP：Flight SQL + MySQL；PG wire 后续扩展）

### 5.1 MySQL wire（feature `mysql`，**本期实施**）

| 项 | 设计 |
|---|---|
| 协议库 | **opensrv-mysql**（Databend 系，Apache-2.0，server 侧协议实现成熟；前身 mysql_wire）。备选自写（~1500 行，R-1） |
| 端口 | TCP :3306（可配），listener 在节点层；opensrv conn loop + 适配层实现 shim 回调 |
| 握手/认证 | greeting → mysql_native_password / **无鉴权模式**（users 配置为空即 trust） |
| 简单查询 | COM_QUERY 文本协议：execute → 文本行编码（§5.4） |
| 预编译 | COM_STMT_PREPARE → prepare（返回 stmt_id + 参数数/列描述）；COM_STMT_EXECUTE → 二进制参数解码为 SqlValue → execute_prepared → 二进制行编码；COM_STMT_CLOSE → 丢弃会话映射 |
| 会话 | 每连接：HashMap<u32, PreparedStatement>、方言=MySql、shim 状态（USE 等） |
| 明确不支持 | COM_STMT_SEND_LONG_DATA / multi-statement / COM_RESET_CONNECTION（报错） |

### 5.2 Flight SQL（已有，改接 SqlEngine）

- flight.rs 删除 run_sql / SqlOutcome / execute_update 内嵌逻辑，改为构造
  `SessionCtx { dialect: Generic }` 调 SqlEngine；
- sql.rs 纯函数与 insert_target 随迁 yuntun-sql；
- prepared statement 的 Action/绑定数据装载路径**保持现状**（批量装载走
  spawn_sql_ingest，不经过 SqlEngine——它不是 SQL 文本执行）；
- 回归基线：flight_e2e / flight_sql_e2e / sql_dml_e2e 全部保持绿（重构不改语义）。

### 5.3 数据编码（RecordBatch → wire 行）与类型映射

统一编码器（yuntun-sqlwire 内共享模块 encode.rs，MySQL 为主、PG 后续复用行列
遍历逻辑；PG 列保留作设计预留）：

| Arrow | MySQL 类型声明 | PG 类型声明 | 文本编码 |
|---|---|---|---|
| Null | NULL | NULL | NULL 标记 |
| Boolean | TINYINT(1) | bool | 1/0、t/f |
| Int8..Int64 | TINYINT/SMALLINT/INT/BIGINT | int2/int4/int8 | 十进制 |
| UInt* | INT/BIGINT UNSIGNED | int8 | 十进制 |
| Float32/64 | FLOAT/DOUBLE | float4/float8 | 最短往返表示 |
| Utf8 | VARCHAR/TEXT | text | 原文（按协议转义） |
| Binary | BLOB/VARBINARY | bytea | hex（\x...） |
| Date32 | DATE | date | YYYY-MM-DD |
| Timestamp(ns, None) | DATETIME(6) | timestamp | YYYY-MM-DD HH:MM:SS.ffffff |
| Decimal128 | DECIMAL(p,s) | numeric | 字符串 |
| Dictionary(Utf8) | VARCHAR | text | 解引用取值 |

列声明（COM_STMT_PREPARE 响应 / RowDescription）用上表；列名/可空性来自
Arrow schema（SqlEngine 已返回）。

### 5.4 兼容性分级（验收口径）

| 级别 | 目标 | 验收 |
|---|---|---|
| **T1（MVP 必须）** | mysql CLI 直连：SELECT / SHOW TABLES / INSERT（文本协议） | CLI 冒烟脚本入 CI |
| **T2（MVP 必须）** | 驱动 prepared statement 写入与查询（JDBC mysql、Go database/sql） | 驱动冒烟（python mysql-connector，复用 pyarrow 冒烟骨架） |
| **T3（MVP 必须）** | DBeaver 连接（MySQL）：连接测试、表/列浏览、数据预览（LIMIT 查询） | 手工清单入 operation-log |
| **T4（非目标）** | 事务、权限、视图、存储过程、全部 SHOW/SET 变体、PG wire | 报明确错误或 no-op |

### 5.5 PostgreSQL wire（后续扩展，本期不实施——设计预留）

| 项 | 设计 |
|---|---|
| 协议库 | **pgwire** crate（Apache-2.0，spiceai 等生产使用） |
| 端口 | TCP :5432（可配，标准端口） |
| 启动/认证 | Startup → trust / cleartext / md5（pgwire 内建）；SCRAM 后续 |
| simple query | Query 报文 → execute → RowDescription + DataRow（文本格式） |
| extended | Parse（→prepare）→ Describe（参数/列描述）→ Bind（参数文本/二进制 → SqlValue）→ Execute → Sync；unnamed statement 按 PG 规范（会话缓存最近 unnamed） |
| 事务语义 | BEGIN… 由 shim no-op；portal 单语句即时执行 |
| 编码 | 文本格式（psql/驱动兼容）；二进制 Row 编码留后期（大结果集性能优化） |

> PG 接入时 SqlEngine/编码器零改动（方言=PostgreSql 已参数化），仅新增适配层——
> 这是 S-1/S-2 分层的直接收益。


---

## 六、组合形态：能力 × 端口矩阵

### 6.1 节点 = 能力组合 + 端口组合

与 ingestor 域（灌入）对称，SQL 域节点化后，全部节点都由**同一套能力 crate**
组合而成——"按状态切分"最终收敛为一张装配矩阵：

| 节点（阶段） | 能力 | 端口 |
|---|---|---|
| **standalone**（阶段 1/2） | ingest + query + catalog + sql | Flight SQL（读写一体，已有）+ **MySQL :3306（本期新增，标准端口）** + 灌入端口（配置开关） |
| **yuntun-sqld**（阶段 3） | query + catalog（gRPC）+ sql；role = readonly（默认） | Flight SQL / MySQL :3306（全只读语义：SqlEngine 拒绝 Insert/DDL） |
| **yuntun-sqld rw**（阶段 3 可选） | 上者 + ingest（本地 WAL） | 同上（写入本地落 WAL）——本质 = sqld ∪ ingestor |
| **yuntun-ingestor**（阶段 3） | ingest | Flight DoPut / LP / OTLP / Kafka（见 ingestor-design.md） |
| **yuntun-meta**（阶段 3） | Catalog Raft | gRPC |

### 6.2 readonly 语义的实现

SqlEngine 构造时注入 `write_policy: AllowWrite | ReadOnly`（sqld readonly 节点
不持有 Ingestor）——分流命中 Insert/DDL 时返回明确错误
（"SQL server is configured read-only"），而非依赖 ingest 缺席的运行时错误。
MySQL 只读节点可回 ERROR 1290 (read-only) 语义码，便于驱动提示。

### 6.3 配置模型（[sql] 节）

```toml
[sql.mysql]        # feature = mysql（本期实施）
enabled = true
listen = "0.0.0.0:3306"   # 标准端口，客户端默认连接串直用
auth = "trust"            # trust | native_password（users 非空时自动切换）
# users = [{ user = "yuntun", password = "..." }]

# [sql.postgres]          # PG wire：后续扩展（§5.5），本期不实施
```

MySQL 端口默认开启（本期范围）。**标准端口冲突提示**：若 3306 被本机既有
MySQL 占用，需显式改 `listen`（启动时 AddrInUse 明确报错，不静默降级）。

---

## 七、演进路线与 WBS

**早期计划范围 = 基础能力（yuntun-sql）+ 两个协议端口（Flight SQL 收尾 + MySQL 新建）**；
PG wire 为后续扩展（§5.5 设计预留）。

### 早期计划（阶段 1 收尾，与 S1.9 并行）

| ID | 任务 | 验收 |
|---|---|---|
| **Q1** | yuntun-sql 基础能力提取：run_sql/sql.rs 迁移 + SqlEngine/SessionCtx + default catalog/schema 修复（G2） | 三套既有 e2e 全绿（纯重构不改语义） |
| **Q2** | prepare / execute_prepared + AST 参数替换（G3）+ 单测 | 参数绑定单测 ≥15 |
| **Q3** | 元数据 API + 方言 shim 骨架 | shim 单测（canned 语句集） |
| **Q4** | **Flight SQL 端口收尾**：改接 SqlEngine（语义唯一化），回归 + pyarrow/ADBC 冒烟 | 三套 e2e + 冒烟脚本全绿 |
| **Q5** | **MySQL wire 端口**：yuntun-sqlwire 骨架 + opensrv-mysql（:3306，COM_QUERY 文本协议） | mysql CLI T1 冒烟 |
| **Q6** | MySQL 预编译链路（COM_STMT_PREPARE/EXECUTE 二进制参数与行编码） | 驱动 T2 冒烟（mysql-connector） |
| **Q7** | DBeaver 实测矩阵（T3）+ shim 补齐 + standalone 集成收尾 + README（mysql 连接示例） | T3 清单全过；配置开 → 端口即起 |

> 早期交付口径：`yuntun` 同时具备 **Flight SQL 与 MySQL 两个协议端口**，
> 共用同一套 yuntun-sql 语义。

### 阶段 3（分布式）

| ID | 任务 | 验收 |
|---|---|---|
| **Q8** | yuntun-sqld bin（readonly 默认；Catalog gRPC 注入） | 独立 SQL 节点查询 → 数据可见 |
| **Q9** | sqld rw 形态（∪ ingest 能力） | SQL 写入 → 其他节点可见 |

### 后续扩展（本计划外）

| ID | 任务 | 前置 |
|---|---|---|
| **Q10** | PG wire（pgwire，§5.5 设计预留） | MySQL 端口稳定后 |

### 排期原则

- Q1 基础能力先行（解耦 + G2 修复），是 Q4/Q5 两个端口共同的地基；
- Q4（Flight 收尾，纯重构）与 Q5–Q6（MySQL 新增）可并行推进；
- 每步以既有 e2e 为回归基线，禁止语义漂移。


---

## 八、风险与待决策项

### 8.1 风险

| # | 风险 | 缓解 |
|---|---|---|
| 1 | DBeaver 元数据探测深不可测（驱动版本差异大） | T3 手工清单 + shim 迭代；兼容分级明示边界（§5.5） |
| 2 | opensrv-mysql 维护活跃度（Databend 内部产出） | 评估后必要时自写最小协议集（R-1 备选，COM_QUERY+PREPARE 子集 ~1500 行可控） |
| 3 | prepared 二进制协议类型编解码遗漏（DATETIME 精度/无符号/NULL 位图） | 参数/结果类型单测矩阵；先文本协议后二进制 |
| 4 | 方言 SQL 长尾（LIMIT 语法差异、函数名、反引号 vs 双引号） | 不做静默改写；不支持即明确报错（绝不返回错误结果） |
| 5 | PG extended protocol 状态机复杂（unnamed/named statement、portal） | pgwire crate 承担状态机，适配层只实现回调；对照 psql/JDBC 行为测试 |
| 6 | 结果全量缓冲（G7） | S1.10 统一流式化；wire 层已按批次迭代设计，切换零改协议层 |
| 7 | 鉴权薄弱（trust/native password） | 文档明示 MVP 边界；网络隔离部署；SCRAM 入阶段 2 后期 |

### 8.2 待决策项

| # | 决策点 | 倾向 |
|---|---|---|
| **R-1** | MySQL 协议实现：opensrv-mysql vs 自写最小子集 | **已裁决：opensrv-mysql**（v1.1）；不可行再自写 |
| **R-2** | MySQL 端口默认值 | **已裁决：标准端口 3306**（v1.1）；端口冲突时显式改配置 |
| **R-3** | USE db / \c db 语义：多 schema 支持路径 | MVP 单 schema（yuntun.public）；多 schema 映射留阶段 4 |
| **R-4** | 结果集二进制编码优先级 | MySQL binary 随 prepare 必做；PG binary 随 PG 接入延后 |
| **R-5** | PG 接入启动时机与认证升级（SCRAM-SHA-256） | 后续扩展（§5.5 设计预留），按需求启动 |
| **R-6** | sqld readonly 的 query 级限流/超时 | 阶段 3 与多租户一起裁决 |

---

## 九、对既有文档的修订清单

| 文档 | 位置 | 修订 |
|---|---|---|
| `architecture.md` | §3.2 crate 边界 | 增加 yuntun-sql（SQL 处理能力）与 yuntun-sqlwire（SQL wire 协议适配）；依赖方向 sql → ingest,query,catalog、yuntun-sqlwire → sql |
| `architecture.md` | §3.2 协议端口归属 | 补注："SQL wire 协议（MySQL/PG）端口由 server（阶段 1/2）或独立 sqld 节点（阶段 3）挂载；SQL 语义唯一实现在 yuntun-sql（sql-access-design.md §三/§四）" |
| `architecture.md` | §13.1.1 | SQL 多协议接入指向本文；yuntun-sqld 加入阶段 3 节点清单 |
| `ingestor-design.md` | §六组合形态 | 交叉引用：节点装配矩阵统一视图（两域共享"能力×端口"模型） |
| `plan.md` | 阶段 2/3 WBS | SQL 多协议 WBS 引用本文 §七（Q1–Q9） |
| `README`（S1.11） | quickstart | 增加 mysql 连接示例（Q7 交付） |

---

### v1.1 变更记录（评审意见采纳，2026-09-09）

| # | 评审意见 | 处理 |
|---|---|---|
| 1 | `yuntun-sqlports` 命名不好 | 更名 **`yuntun-sqlwire`**（SQL wire 协议适配层） |
| 2 | 先只做 MySQL，采用 opensrv-mysql | §五/§七 收窄：PG wire 移出本期（设计保留于 §5.5 作后续扩展）；opensrv-mysql 定为裁决（R-1） |
| 3 | 监听标准端口 | MySQL :3306 标准端口（R-2 已裁决）；standalone 默认开启 |
| 4 | 早期计划 = 基础能力 + Flight SQL 与 MySQL 两个协议端口 | §七 WBS 重排：Q1–Q3（yuntun-sql 基础能力）+ **Q4（Flight SQL 端口收尾）** + Q5–Q6（MySQL 端口新建）+ Q7（DBeaver/集成收尾） |

---

**文档结束 · SQL 访问协议设计 v1.1**
