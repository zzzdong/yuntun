# yuntun — 开发计划任务书 v2.0（Standalone 优先路线）

> **依据**：《通用直写数据湖架构设计 v11》+《详细设计文档 v1.0》+《阶段 0 实现操作日志》（2026-09-08）
> **版本**：v2.0
> **日期**：2026-09-08
> **适用范围**：阶段 1（Standalone 完备）至阶段 4（规模化）
>
> **v2.0 相对 v1.0 的核心变更**：**分布式整体后移**。先把 standalone 做成一个功能完备、可独立交付的
> 本地时序/可观测数据库，分布式（Meta Raft 分离、多节点）作为最后一公里的自然扩展。
> 原则延续 v9 评审结论：核心逻辑先在单进程验证，分布式只做网络化与扩展性，不重写逻辑。

---

## 目录

1. [路线调整说明](#一v20-路线调整说明)
2. [阶段划分](#二阶段划分)
3. [阶段 1 WBS：Standalone 完备](#三阶段-1-wbsstandalone-完备)
4. [Flight SQL 接入设计（阶段 1 核心）](#四flight-sql-接入设计阶段-1-核心)
5. [阶段 2 WBS：质量与性能](#五阶段-2-wbs质量与性能)
6. [阶段 3 WBS：分布式化](#六阶段-3-wbs分布式化)
7. [风险与应对](#七风险与应对)
8. [验收标准](#八验收标准)

---

## 一、v2.0 路线调整说明

### 1.1 三条决策

| # | 决策 | 理由 |
|---|---|---|
| **D-1** | **工程结构**：`bins/` 撤销并入 `crates/`，`all-in-one` 更名 **`standalone`**（bin 名 `yuntun`） | 架构 §3.2 本就要求"按状态切分"；每个可执行体独立成 crate，分布式阶段直接增 `yuntun-meta` / `yuntun-ingestor` 等 crate，`standalone` 保留为"全组件参考装配" |
| **D-2** | **先把 standalone 做成"能用"的数据库**：数据写入 + 查询闭环，支持**标准 Flight SQL 客户端**或**自有客户端**完成 `INSERT` / `SELECT` | 阶段 0 已有 Flight DoPut + 自定义 ticket 查询，但标准 Flight SQL 客户端（pyarrow / ADBC / JDBC）无法接入；"可用性"优先于"架构完备性" |
| **D-3** | **除分布式外的一切能力在 standalone 内完成**：Flight SQL、SQL DML、Vortex、Compaction 完整闭环、GC、Chaos 压测、性能调优 | 分布式化的收益依赖单机功能正确性；单机阶段做完，分布式只剩网络化改造 |

### 1.2 与 v1.0 的阶段对照

| v1.0 阶段 | v2.0 阶段 | 状态 |
|---|---|---|
| 阶段 0：All-in-One（PoC + 写入查询链路） | 阶段 0（不变） | ✅ **已完成**（2026-09-08，66 tests / 0 failed） |
| —（无） | **阶段 1：Standalone 完备**（crate 重构、Flight SQL、SQL 写入、自有客户端、遗留清偿） | 🆕 本任务书新增 |
| 阶段 0.5：核心逻辑压测（Chaos） | 阶段 2：质量与性能（范围不变，仍在 standalone 内） | 顺序后移 |
| 阶段 1：Meta 分离 Raft | 阶段 3：分布式化 | **整体后移** |
| 阶段 1.5：分布式压测 | 并入阶段 3 | 后移 |
| 阶段 2–3：全面分布式 / 规模化 | 阶段 4：规模化 | 后移 |

### 1.3 Crate 结构调整（D-1）

```
yuntun/                                  # 现在 → 目标
├── crates/
│   ├── yuntun-model/       # 核心数据模型（最底层）
│   ├── yuntun-proto/       # 元数据 / WAL 消息（阶段 3 启用 tonic-build）
│   ├── yuntun-wal/         # 自实现 WAL
│   ├── yuntun-store/       # 对象存储抽象（local / memory / s3）
│   ├── yuntun-format/      # Parquet（默认）/ Vortex（feature）
│   ├── yuntun-catalog/     # 内存 Catalog（快照隔离 / OCC / 幂等）
│   ├── yuntun-ingest/      # 写入管线（RecordBatch → WAL → 攒批 → flush）
│   ├── yuntun-query/       # DataFusion 桥接 + Manifest 驱动 scan
│   ├── yuntun-compaction/  # 合并 + 孤儿判定
│   ├── yuntun-server/      # 节点层：协议端口（Flight/FlightSQL）+ 装配 + 路由
│   ├── yuntun-chaos/       # 故障注入工具
│   ├── yuntun-standalone/  # ★ 原 bins/all-in-one，bin 名 yuntun
│   └── yuntun-client/      # ★ 新增：Rust SDK + CLI（bin 名 yuntun-cli）
└── docs/
```

**分布式阶段（阶段 3）再增**：`yuntun-meta`（Raft 服务）、`yuntun-ingestor`、`yuntun-queryd`、
`yuntun-compactor`——全部复用 `standalone` 已验证的组件，只替换装配与网络层。

**Workspace 成员变更清单**（立即执行）：

```toml
# 移除 "bins/all-in-one"，新增：
"crates/standalone",
"crates/client",
```

---

## 二、阶段划分

```
阶段 0  ✅ 写入/查询链路贯通（All-in-One）
  │
阶段 1  Standalone 完备 —— 做成"能用的数据库"
  │      S1.1 crate 重构（D-1）
  │      S1.2 Flight SQL 标准服务端
  │      S1.3 SQL 写入路径（INSERT / DDL）
  │      S1.4 自有客户端 yuntun-cli
  │      S1.5 阶段 0 遗留事项清偿
  │
阶段 2  质量与性能 —— 原阶段 0.5（Chaos 11 场景 + 压测 + Vortex）
  │
阶段 3  分布式化 —— 原阶段 1 / 1.5（Meta Raft + 多节点）
  │
阶段 4  规模化 —— 原阶段 2 / 3（外部索引、Iceberg 等）
```

---

## 三、阶段 1 WBS：Standalone 完备

> 定位：交付一个**单二进制、单命令启动**的本地数据库，
> 标准客户端与自有 CLI 均可完成 `INSERT` / `SELECT` / 建表 / 看 schema。

### Sprint A：结构与协议底座（先行，半天~1 天）

| ID | 任务 | 验收标准 |
|---|---|---|
| **S1.1** | crate 重构：`bins/all-in-one` → `crates/standalone`（包名 `yuntun-standalone`，bin 名 `yuntun`）；workspace members 更新；CI/脚本路径同步 | `cargo build --workspace` 通过；`cargo run -p yuntun-standalone` 冒烟启动 Flight 监听；全部 66 tests 通过 |
| **S1.2** | 依赖准备：启用 `arrow-flight` 的 `flight_sql` feature（protoc 已确认存在） | `arrow_flight::sql` 模块可用，`FlightSqlService` trait 可实现 |

### Sprint B：Flight SQL 服务端（核心）

| ID | 任务 | 验收标准 |
|---|---|---|
| **S1.3** | `yuntun-server::flight::FlightServer` 实现 FlightSQL 协议（语句/元数据/prepared statement，get_flight_info_statement / do_get_statement / do_put_statement_ingest / do_put_prepared_statement_update 等） | ADBC 独立客户端能列出表、执行 SELECT |
| **S1.4** | Ticket 体系切换到标准 `StatementQueryTicket`（protobuf）；简易 ticket 模式（`do_get(ticket=SQL)`）**保留**为自有客户端快速通道，两套并存 | 两种 ticket 各有 e2e 测试 |
| **S1.5** | 客户端兼容性验证矩阵：pyarrow.flight.sql、ADBC（Go/Python）、JDBC（Dremio/Arrow Flight SQL JDBC） | 每个客户端 insert + select 冒烟通过（至少 pyarrow + ADBC） |

### Sprint C：SQL 写入路径

| ID | 任务 | 验收标准 |
|---|---|---|
| **S1.6** | `INSERT INTO ... VALUES / SELECT`：**server 基于 sqlparser AST 前置解析**（§4.3），按表 schema 构造 `RecordBatch` 送入 ingest 管线（**不使用 DataFusion DML**） | SQL 写入的数据崩溃重启后可恢复；与 DoPut 同等持久性 |
| **S1.7** | DDL：server 基于 sqlparser AST 拦截 `CREATE TABLE / DROP TABLE` → Catalog（新增 `drop_table` 能力）；`SHOW TABLES` → Catalog `list_tables` | DDL 结果持久化（跨会话/重启可见） |
| **S1.8** | 幂等键在 SQL 路径的透传：Prepared statement options 携带 `client_request_id`；INSERT 路径按语句级生成 | 幂等矩阵单测覆盖 |

### Sprint D：自有客户端 + 收尾

| ID | 任务 | 验收标准 |
|---|---|---|
| **S1.9** | `crates/yuntun-client`：Rust SDK（连接、insert batch、query 流式迭代、get schema）+ `yuntun-cli`（子命令：`query` / `insert --format csv|jsonl|parquet` / `tables` / `schema`） | `yuntun-cli insert -f data.csv -t cpu && yuntun-cli query "SELECT ..." ` 端到端可用 |
| **S1.10** | do_get 流式化（清偿遗留）：结果集不再全量缓冲，边读边发 | 100 万行查询内存平稳 |
| **S1.11** | 文档：README（quickstart：`yuntun serve` → pyarrow 三行代码读写）、客户端兼容矩阵、CLI 手册 | — |

### 遗留事项清偿（S1.12，穿插进行）

| 遗留（操作日志 §4） | 归属 |
|---|---|
| Vortex 锁 Git Commit + feature + 压测对比 | 阶段 2（压测前） |
| Multipart Upload 续传 | 阶段 2 |
| 孤儿清理完整循环（list ↔ 对账 + 静置 + 删除） | 阶段 2 |
| Pending 恢复复用原 batch_id | 阶段 2（Chaos 依赖） |
| 磁盘水位 statvfs 真实实现 | 阶段 2 |
| WAL segment 清理清单接入 recovery | 阶段 2 |
| Flight 背压 | 阶段 2 |
| proto tonic-build codegen | 阶段 3（分布式 gRPC 时） |

**阶段 1 准出条件**：

1. `yuntun serve` 单命令启动；pyarrow FlightSQL 客户端完成建表→写入→查询闭环；
2. `yuntun-cli` 完成 CSV/JSONL/Parquet 导入 + SQL 查询；
3. 全量测试通过、零警告；SQL 写入的数据经崩溃重启不丢失；
4. 结构调整后无 `bins/` 目录。

---

## 四、Flight SQL 接入设计（阶段 1 核心）

### 4.1 选型与归属（v2.0.2 修订）

- 直接采用 **arrow-flight 自带的 `flight_sql` 模块**（含 protoc 生成的 FlightSql protobuf 与
  `FlightSqlService` server trait），不手写 protobuf——与官方客户端字节级兼容是硬指标。
- **归属（架构 §3.2 v12.3，简化版）**：协议端口统一在 **`yuntun-server`**——
  一个节点上的 server 承载多类协议端口（Flight SQL、InfluxDB LP、未来 MySQL/PG wire），
  每个协议内部把写路由到 ingest 能力、读路由到 query 能力；
  域 crate（ingest / query）保持纯能力，不含协议与 Hook 间接层。
- **统一端点**：`yuntun-server::flight::FlightServer` 直接持有
  `Arc<Ingestor>` + `Arc<QueryEngine>`，实现 FlightService 并做三轨路由
  （FlightSQL 标准轨 / 简易写入轨 / 简易查询轨）。

### 4.2 双轨接口

| 轨道 | 协议 | 客户端 | 用途 |
|---|---|---|---|
| **标准轨** | Flight SQL（StatementQuery / PreparedStatement） | pyarrow、ADBC、JDBC、Grafana 插件生态 | 生态兼容 |
| **简易轨** | ticket = SQL 文本（既有实现，保留） | `yuntun-cli`、自有 SDK | 零开销直查 |

### 4.3 SQL 前置解析拦截（v2.0.3 设计定稿，S1.6/S1.7）

**原则**：server 做 SQL 的**前置解析拦截**——只有 SELECT 查询让 DataFusion 处理。
DataFusion 不触碰 DML / DDL（避免会话级副作用，能力边界清晰）。

**技术选型：`sqlparser`**（与 DataFusion 同源的 SQL 解析器，Apache-2.0）——
server 用 `Parser::parse_sql` 把语句解析为 AST（`sqlparser::ast::Statement`）后按变体分流，
不手写 tokenizer。与 DataFusion 同源意味着 SQL 方言（标识符引用、类型名、字面量语法）
天然一致；版本对齐 DataFusion 55 所依赖的 sqlparser 版本（避免双版本共存）。
方言 MVP 用 `GenericDialect`（宽松），后续可按协议/会话配置。

`FlightServer::run_sql` 按 AST `Statement` 变体分流（所有 SQL 入口统一经过：
简易轨 do_get / FlightSQL StatementQuery / StatementUpdate / prepared update）：

| AST `Statement` 变体 | 处理 |
|---|---|
| `Query`（SELECT / CTE） | → DataFusion（**query 能力**，只读） |
| `ShowTables` | → Catalog `list_tables`（meta 能力） |
| `Insert` + `SetExpr::Values` | server 按表 schema 把 AST 字面量（`Value`）构造 `RecordBatch` → `ingest.ingest`（**ingest 能力**） |
| `Insert` + `SetExpr::Select` | server 先经 DataFusion 执行 SELECT 源（读），结果列 cast 到表 schema → ingest |
| `CreateTable` | server 映射列定义（`ColumnDef` 的 `DataType` → Arrow DataType）→ Catalog `create_table` |
| `Drop { object_type: Table }` | → Catalog `drop_table`（**新增 meta 能力**；数据文件转孤儿，由孤儿清理回收） |
| 其他（`Update` / `Delete` / `Explain` / `Set` ...） | 明确拒绝（NotImplemented + 支持列表提示） |

**INSERT 解析规则（MVP 限制）**：

- VALUES 仅支持字面量（AST `Value`：字符串、数值、`NULL`、`TRUE/FALSE`）；
  表达式与函数 → 明确拒绝
- 列清单 `INSERT INTO t (a, b)` 支持指定列，缺省列填 NULL（NOT NULL 列缺失则报错）；
  无列清单按表 schema 全列按序
- 类型按表 schema 逐列转换（含范围检查）；`TIMESTAMP` 字面量支持 ISO8601（UTC）与整型毫秒
- `INSERT INTO t SELECT ...` 列按位置对齐并 cast 到表 schema 类型（不支持指定列清单的 MVP）
- 每条语句一个语句级幂等键（满足 require 表强制检查；Meta 层去重仍以 batch_id 为准）
- 单语句：多语句输入（`;` 分隔）明确拒绝（避免部分执行的语义复杂度）

**DDL 说明**：`CREATE TABLE` 的 ingest 配置使用 General 模板（强制幂等键）；
分区列 / Vortex 格式 / 模板选择的 SQL 语法留待后续按需扩展。

### 4.4 写入的统一约束

无论 Prepared statement `do_put`（批量 append）还是 `INSERT INTO ... VALUES`（server 前置解析），
最终**必须汇入同一条 ingest 管线**（WAL append → 攒批 → flush → CommitFiles）：

```
Flight SQL do_put(prepared stmt)   ─┐
SQL INSERT（server 前置解析为 RB） ─┼→ ingest 管线（WAL 权威）→ 攒批后可见
Flight DoPut（简易轨，既有链路）    ┘
```

**禁止** SQL 写入路径直接写 S3 / 直写 Catalog —— 绕过 WAL 就绕过了崩溃恢复（C1）。

### 4.5 一致性说明

SQL `INSERT` 返回成功时数据仅落 WAL（与 Flight DoPut 语义一致），攒批窗口后可见；
如需同步可见，由 MemTable 直读路径提供"写后立即可查"（读己之写），实现方式与既有查询路径复用。

---

## 五、阶段 2 WBS：质量与性能

> 即 v1.0 阶段 0.5，范围不变：11 个 Chaos 场景 + WAL 专项 + 压测，全部在 standalone 内完成。

| ID | 内容 | 说明 |
|---|---|---|
| T6.1–6.11 | Chaos 11 场景（v1.0 §5.2 全表沿用） | 含并发崩溃、`synced_seq` 验证、Batch 超时、磁盘水位 |
| T7.x | **SQL 写入路径专项 Chaos**（v2.0 新增） | Flight SQL / INSERT 写入 × 崩溃点组合，验证与 DoPut 同等持久性 |
| T8.x | 压测报告 | 吞吐 / P99 / 剪枝效果 |
| T9.x | Vortex：锁 commit + feature 打开 + Parquet 对比（遗留清偿大头） | ADR-1 |

---

## 六、阶段 3 WBS：分布式化

> 即 v1.0 阶段 1 / 1.5。单机功能与质量已完备，此处只做网络化：

| ID | 内容 | 说明 |
|---|---|---|
| T10.1 | `yuntun-meta`：fjall 实现 `raft-rs::Storage` + Catalog 内存 snapshot（架构 §5.4） | Catalog 逻辑零改动，只换状态机宿主 |
| T10.2 | proto 启用 tonic-build；Catalog 包 gRPC（架构 §3.3 同签名演进） | 依赖注入切换，业务代码不动 |
| T10.3 | 拆分 `yuntun-ingestor` / `yuntun-queryd` / `yuntun-compactor` 独立 crate | 从 `standalone` 装配中逐组件摘出 |
| T10.4 | 分布式压测（v1.0 阶段 1.5 七场景 + Snapshot Leader 稳定性） | — |

**自然扩展的保证**（v1.0 已内置、v2.0 继续生效）：

- `IngestSource` trait（ADR-13）、Meta Raft 语义抽象（§5.4）、Catalog 纯逻辑无网络（C7）、
  幂等键独立存储（§7.3.1）——这些抽象使阶段 3 只做装配与网络层，不重写存储/写入/查询逻辑。

---

## 七、风险与应对

| # | 风险 | 概率 | 影响 | 应对 |
|---|---|---|---|---|
| R-1 | arrow-flight `flight_sql` 模块 API 与目标客户端版本不匹配（protobuf 兼容性） | 中 | 高 | S1.5 第一时间做 pyarrow 冒烟；不匹配则从官方 proto 手动 codegen |
| R-1b | sqlparser 版本与 DataFusion 55 依赖不一致（双版本共存） | 低 | 低 | 对齐 Cargo.lock 中 DF 依赖的 sqlparser 版本；AST 仅用于 server 前置分流，双版本本也可共存 |
| R-2 | ~~DataFusion `insert_into` DML 集成坑~~ | — | — | **已消除**（v2.0.3）：INSERT 改为 server 前置解析，不使用 DataFusion DML |
| R-3 | 双轨 ticket 路由冲突（FlightSql cmd 与自定义 cmd 混淆） | 低 | 中 | cmd 头部加 1 字节轨标识；两轨各有 e2e |
| R-4 | 结构重构引入回归 | 低 | 中 | 纯移动 + 改名，重构后立即全量回归 66 tests |
| R-5 | 阶段 3 时 Catalog gRPC 改造面超预期 | 中 | 中 | 架构 §6.4 已同签名预留；Compaction 依赖具体类型需先抽象 `CommitCompaction`（操作日志 §2.2-5） |

---

## 八、验收标准

### 阶段 1（功能）

| # | 标准 | 验证方式 |
|---|---|---|
| 1 | pyarrow FlightSQL：建表 → insert → select 闭环 | e2e 脚本入库 CI |
| 2 | `yuntun-cli` 导入 CSV/JSONL/Parquet 并查询 | e2e |
| 3 | SQL `INSERT` 数据崩溃重启不丢（WAL 权威） | 测试注入 kill |
| 4 | 单二进制 `yuntun` 单命令启动，零额外依赖 | 冒烟 |
| 5 | workspace 无 `bins/`；`cargo clippy -D warnings` 干净 | CI |

### 阶段 2（质量）

沿用 v1.0 §9.2（Chaos 100% / 核心模块覆盖率 ≥80% / 零丢失零重复）+ §9.3 性能指标。

### 阶段 3（分布式）

沿用 v1.0 M4/M5 准出（Raft 3 节点运行写入不中断、7 个分布式场景通过）。

---

**文档结束 · 开发计划任务书 v2.0**
