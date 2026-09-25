# yuntun 实现操作日志（阶段 0 → 阶段 2）

> 起记时间：2026-09-08（最新：**§32，2026-09-18**）。对应分支：main。
> 本日志记录实际操作顺序、**临时调整**、**与原计划（docs/design.md / architecture.md / plan.md）的偏差点**，
> 以及**每个结论的证据**（缺陷根因、实测数据、反证过程）。
>
> **定位**：全文最高优先级的**证据来源**；`status.md` / `plan.md` / `architecture.md` 里的"已完成/已具备"
> 都应能指回本文某段。冲突裁决顺序见 [`README.md`](README.md)。

---

## 1. 实施顺序

| 步骤 | 内容 | 结果 |
|---|---|---|
| T1.0 | workspace 骨架（11 crates）+ 预先 `cargo check` 验证依赖可解析 | ✅ |
| T1.4 | model：错误类型、元数据 prost 消息、WAL Record、类型提升格、BatchState 状态机 | ✅ 17 tests |
| T1.1+T2.1~2.3 | wal：segment / CRC / 组提交 / synced_seq / 轮转 / 恢复 / 分级超时监控 | ✅ 14 tests |
| store | object_store 抽象（local/memory/s3） | ✅ 2 tests |
| format | Parquet 读写 + Vortex feature flag（ADR-1） | ✅ 2 tests |
| T3.1~3.5 | catalog：MemoryCatalog 快照隔离 / OCC 演进 / 幂等键 / L2 合并提交 | ✅ 9 tests |
| T1.3+T2.4~2.6 | ingest：Flight 源、幂等矩阵、SchemaCache OCC 重试、攒批、flush 三级状态机、恢复分流 | ✅ 15 tests |
| T4.x | query：DataFusion 桥接 + 本地缓存 + Manifest 驱动 scan | ✅ 2 e2e |
| compaction | 合并（L2 原子提交）+ 孤儿判定 | ✅ 3 tests |
| server + all-in-one | TOML 配置、装配、Flight gRPC、优雅关闭 | ✅ 4 tests |
| 验收 | 全量测试 66 passed / 0 failed，零警告；`yuntun --version` 与冒烟启动（Flight 监听 :50051）通过 | ✅ |

## 2. 与原计划的偏差点

### 2.1 依赖版本（文档 §2.3 → 实际）

| 文档指定 | 实际采用 | 原因 |
|---|---|---|
| arrow 55 | **arrow 59.3.0** | datafusion 55.0.0 实际依赖 arrow 59，双版本会导致 Schema 类型不兼容；文档"55"实指 DF 主版本号 |
| datafusion 55 | datafusion 55.0.0（一致） | — |
| object_store 0.11 | **0.13.2** | DF55 依赖 0.13.2；且 0.13 起 `put/get/delete` 移入 `ObjectStoreExt` 扩展 trait（需显式 import） |
| tonic 0.12 / prost 0.13 | **tonic 0.14.6 / prost 0.14.1** | arrow-flight 59 强制 tonic 0.14 + prost 0.14 |
| — | 新增 `datafusion-datasource`、`datafusion-datasource-parquet` 55（ParquetSource/FileScanConfigBuilder 独立 crate） | — |

**DataFusion/Flight 55 API 与文档 assumed 版本差异**（后续阶段编码注意）：
- `TableProvider/SchemaProvider/CatalogProvider` 已无 `as_any`；`SchemaProvider::table_type` 为 `async fn -> Result<Option<TableType>>`
- Arrow Schema IPC 序列化：`IpcSchemaEncoder::schema_to_fb` + `root_as_schema`/`fb_to_schema`（旧 `schema_to_bytes` 已移除）
- `ParquetSource::new(TableSchema)` + `FileScanConfigBuilder::new(url, Arc<dyn FileSource>)`；`with_projection_indices` 返回 Result
- `FlightService` 在 `arrow_flight::flight_service_server`；客户端无 `connect()`，用 `Endpoint::connect() -> Channel -> Client::new()`
- `FlightDescriptor` 无 `endpoint` 字段；`do_put` 请求流消息为 `FlightData`（非 `Result<FlightData>`）
- 批次编码用 `arrow_flight::utils::batches_to_flight_data`（`flight_data_from_arrow_batch` 已移除）
- tonic 0.14 server：`serve_with_incoming_shutdown(Stream, shutdown)`；测试中随机端口用 `TcpListenerStream`

### 2.2 工程决策偏差

1. **Vortex 未引入**（ADR-1 要求"必须锁定 Git Commit"）：
   - 调整：Vortex 置于 `yuntun-format` 的 `vortex` feature flag 之后，`encode_vortex/decode_vortex` 显式返回错误；默认走 **Parquet 回退路径**（FormatSwitch 语义本就允许）。
   - 锁 commit + 打开 feature 推迟到 Phase 0.5 压测前。
2. **WAL offset 语义**：文档写"synced_offset = 跨 segment 累积的字节偏移"，实现采用**记录序号 seq**（per-shard 单调计数）——与 `BatchPending.wal_seq_range` 语义一致、跨 segment 连续，避免字节偏移在轮转处的歧义；C2 语义等价（只读已 fsync 记录）。
3. **proto crate 未走 protoc codegen**：元数据/WAL 消息直接用 `prost::Message` derive 手写在 model crate（protoc 已确认存在，但阶段 0 无 gRPC 自定义服务；阶段 1 加 tonic-build codegen）。
4. **Multipart Upload 未实现**（§7.4）：`s3_upload_id` 恒空串，S3 写入为单段 put；续传逻辑留阶段 1。
5. **Compaction 依赖具体类型**：`Compactor.catalog: Arc<MemoryCatalog>`（L2 的 `commit_compaction` 需要具体类型）；阶段 1 Catalog 转 gRPC 时需新增 `CommitCompaction` RPC。
6. **恢复分流简化**（§5.6）：`Pending` 状态的恢复 = 数据留在 WAL 由攒批线程重读（batch_id 重新生成）；复用原 batch_id 的完整重做留 Phase 0.5 chaos。`S3Written → 只 Commit` 已实现。
7. **磁盘水位近似实现**：`DirSizeUsage = WAL 目录字节数 / (segment 配额近似)`，未接 statvfs；生产需替换。
8. **孤儿清理**：仅有判定函数（`classify_orphans`，按 batch_id 对账）+ S3 list；静置期（1h）删除循环未接（Phase 0.5 chaos）。
9. **合并产物拆分**：`max_rows_per_output` 配置存在但 MVP 单文件全量输出，未按行数拆分。
10. **CommitFiles 的 client_request_id**：flush 层不传（None），幂等去重靠 batch_id + Meta 层唯一索引；Flight 层的幂等键已写入 WAL Data payload 但 flush 聚合后未透传（多来源合并语义待定）。
11. **时间窗口分区**：文件路径 `dt={整分钟窗口}`（如 `dt=2026-09-08T07:01`），文档回执示例为日期级 dt；窗口字符串无 chrono 依赖，手写 civil 算法。

### 2.3 临时调整（操作层面）

1. **网络**：crates.io 直连 TLS 失败（GFW 环境）→ 项目级 `.cargo/config.toml` 启用阿里云 sparse 镜像（`source.crates-io replace-with = 'aliyun'`），未改动用户全局 cargo 配置。
2. **crate 命名**（用户指令）：`lake-*` → `yuntun-*` 全量重命名（sed 批量）；WAL magic `"LAKE"(0x4C414B45)` → `"YUNT"(0x59554E54)`；存储 prefix `lake/` → `yuntun/`；二进制 `all-in-one` → `yuntun`。
3. **rustup 工具链中断**：实现途中 stable 工具链 cargo 组件丢失（用户升级中），短暂改用系统 cargo；用户修复后恢复（cargo 1.98.1）。
4. **测试基础设施**：query/server 的 e2e 需 `dev-dependencies`（ingest/wal/store/tokio-stream/futures/serde_json/tracing-subscriber）。

## 3. 实现中发现并修复的 bug（值得留档）

1. **prost `Default` 陷阱**：prost 派生的 `Default` 对 uint64 字段全 0（不受 `#[prost(..., default=...)]` 影响？实际 default 属性只作用于 decode）。`IngestConfig::default()` 的 `idempotency_ttl_secs=0` 触发了 pipeline 里 `ttl==0 → 用全局默认` 的错误 fallback，导致强制幂等误判。
   → **修复**：删除该 fallback，`require_key` 以表配置字段为权威；缺省用 `IngestConfig::standard()`。
   → **教训**：prost 消息永远不要依赖 `Default::default()` 做业务默认值，提供显式 `standard()` 类构造器。
2. **攒批扫描 off-by-one**：`scan_range(last_read, synced)` 丢了 seq=synced 的记录（半开区间）；改 `to = synced + 1`。
3. **空扫描推进水位**：修复 2 后空扫描（synced 未变）仍把 `last_read` 推进到 `synced+1`，后续到达的 seq 被永久跳过（数据丢失级 bug，查询 e2e 捕获）。
   → **修复**：`last_read` 只按实际扫描到的最后一条 seq +1 推进。
   → **教训**：`synced_seq=0` 存在"无记录"与"seq 0 已同步"的歧义，水位推进必须以实际扫描结果为准。
4. **Flight 幂等键位置**：最初只从首条（schema）消息解析 `app_metadata`，批量消息携带的幂等键被忽略（"idempotency key required" 误报）→ 改为循环内按批次解析。
5. **测试侧**：tokio `#[tokio::test]` 内嵌套 `Runtime::new()` panic；monitor 测试 `created_at_ms` 误用 epoch 小整数导致恒超时；后台 future 未 spawn 导致从未执行（`run_accumulator` 补了 `spawn_accumulator` 包装）。
6. **tonic 端口绑定**：测试里手动 bind listener 后再 `serve(addr)` 会 AddrInUse → 用 `serve_with_incoming_shutdown(TcpListenerStream)` 复用。

## 4. 遗留事项（移交阶段 0.5 / 1）

- [ ] Vortex 依赖锁 Git Commit + feature 打开 + FormatSwitch 压测对比（ADR-1）
- [ ] Multipart Upload 续传（§7.4）
- [ ] 孤儿清理完整循环（S3 list ↔ Meta 对账 + 1h 静置 + 删除，§12.2.1）
- [ ] Pending 恢复时复用原 batch_id 的重做路径（§5.6 完整版）
- [ ] 合并产物按 `max_rows_per_output` 拆分
- [ ] 磁盘水位接 statvfs 真实实现
- [ ] WAL segment 清理清单接入 recovery 的 `segments` 元信息（当前 monitor 传空列表，只做超时/水位 abort）
- [ ] proto crate 启用 tonic-build codegen（阶段 1 gRPC 服务）
- [ ] 多 shard（ShardMapper）与 raft（raft-rs / fjall 已在 workspace 注释预留）
- [ ] Flight 侧流控背压（当前 mpsc 64 + ingest 逐批 await）
- [ ] do_get 结果集全量缓冲（阶段 0 简化）：结果先聚合为 Vec<FlightData> 再流式返回，大结果集需改为流式 chunking + 分批读取

## 5. 追加：外部查询支持（do_get / get_flight_info，2026-09-08 补齐）

阶段 0 收尾时按用户要求补上了对外 SQL 查询出口，写入与查询现在**全部经由同一个 gRPC 端点**（`server.listen`）：

| 接口 | 约定 | 说明 |
|---|---|---|
| `do_get(Ticket)` | `ticket` = UTF-8 SQL | 返回 IPC 流（首条 Schema 消息 + 数据消息）；空结果也返回 schema |
| `get_flight_info(descriptor)` | `descriptor.cmd` = SQL | 返回 endpoint（Ticket = 相同 SQL），兼容 pyarrow.flight "先 GetFlightInfo 再 DoGet" 生态 |
| `get_schema(descriptor)` | `descriptor.cmd` = SQL | 以 `LIMIT 0` 子查询取结果集 schema，不返回数据 |

实现结构：
- `yuntun-ingest/flight.rs`：新增 `FlightQueryHook` trait（解耦 QueryEngine）+ `FlightIngestService::with_query()`；服务无 query hook 时 do_get 返回 Unimplemented
- `yuntun-server/hook.rs`：`QueryHook` 包装 QueryEngine；`serve_flight` 同时挂 ingest + query hook
- SQL 经由每次查询新建的 SessionContext 执行（会话级状态隔离）；Catalog 仍走本地缓存（C7）

验证：
- demo（`cargo run -p yuntun-server --example demo`）：DoPut 写入 + do_get 查询全部走 gRPC，`SELECT *` / `GROUP BY` 聚合 / `WHERE` 过滤均正确
- `flight_e2e` 新增断言：do_get 查到 2 行 + get_flight_info → do_get 路径可用
- 全量回归：66 passed / 0 failed，零警告

## 6. 追加：S1.1 crate 重构 + Windows fsync 修复（2026-09-08，计划任务书 v2.0）

按《开发计划任务书 v2.0》（Standalone 优先路线）执行阶段 1 第一个任务 S1.1：

### 6.1 结构调整（D-1）

- `bins/all-in-one` → **`crates/standalone`**（包名 `yuntun-standalone`，bin 名仍为 `yuntun`），原目录删除
- workspace members：`"bins/all-in-one"` → `"crates/standalone"`
- 同步清理残留命名：`server/lib.rs`、`server/config.rs`、`yuntun.toml.example` 注释中 all-in-one → standalone
- 依赖路径 `../../crates/*` → `../server` / `../store`

### 6.2 重构中发现并修复的 Windows 平台 bug（与重构无关，pre-existing）

**现象**：全量测试在 Windows 上 wal/chaos 大面积失败，报 `write CURRENT: 拒绝访问 (os error 5)`。

**根因**：`wal/segment.rs::fsync_dir` 用 `std::fs::File::open(dir)` 打开目录——Windows 上目录句柄
必须带 `FILE_FLAG_BACKUP_SEMANTICS`，且 `FlushFileBuffers` 要求写权限，否则一律 Access Denied。
该 bug 会导致 WAL 在 Windows 上无法创建（二进制也起不来）；此前"66 passed"应在非 Windows 环境验证。

**修复**：`fsync_dir` 按 `#[cfg]` 分平台——unix 保持原实现；windows 用
`OpenOptions::new().read(true).write(true).custom_flags(FILE_FLAG_BACKUP_SEMANTICS)` 打开目录后
`sync_all()`；FAT/exFAT 等不支持目录 flush 时（PermissionDenied）降级为 no-op。

### 6.3 验证

- `cargo test --workspace`：**全部通过 / 0 failed**（含 chaos 3 个场景——此前在 Windows 从未跑绿）
- `yuntun --version` → `yuntun 0.1.0`；非法参数处理正常
- clippy：本次改动文件无新警告（store/catalog/wal 存量警告待专项清理）

## 7. 追加：Flight SQL 标准协议 + 协议端口归属重构（2026-09-09，S1.2–S1.5）

### 7.1 协议端口归属重构（架构 §3.2 v12.1，用户裁决）

按用户模型调整：**协议适配跟随域，核心与端口分层**。

- `yuntun-query` 新增 `flight_sql.rs`（**query 域协议端口**）：
  `Gateway`（FlightSQL 网关）+ `FlightQueryHook`（SQL 执行，从 ingest 迁来）
  + `SqlAppendHook`（**新增**，写入回调 ingest——query 不依赖 ingest）。
  未来 MySQL / PG wire 端口同层；InfluxDB LP 端口属 ingest 域（阶段 2+）
- `yuntun-ingest`：删除 flight.rs 中的 FlightService 实现与 SQL 网关，
  只留 `FlightIngestHook` trait；核心（Ingestor / WAL / 攒批）不变
- `yuntun-server`：新增 `flight.rs` `UnifiedFlightService`（单端点三轨路由：
  FlightSQL 标准轨 / 简易写入轨 / 简易查询轨）；`hook.rs` 适配三个钩子
- QueryEngine 开启 `information_schema`（GetTables / SHOW TABLES / S1.7 DDL 依赖）；
  新增 `schema_of()`（逻辑计划取 schema，替代 "LIMIT 0 + collect"——后者空结果
  返回零批次会丢 schema，曾致 dataset_schema 为空）

### 7.2 Flight SQL 实现范围（S1.3–S1.5）

- 查询：CommandStatementQuery / CommandPreparedStatementQuery（GetFlightInfo + DoGet + GetSchema）
- 写入：CommandStatementIngest（批量装载）、CommandPreparedStatementUpdate
  （绑定数据 + INSERT → 批量 append，语句级幂等键）、CommandStatementUpdate（DDL/非数据 SQL）
- 元数据：Catalogs / DbSchemas / Tables（含 table_schema 列）/ TableTypes / SqlInfo（空）/ XdbcTypeInfo（空）
- Prepared statement：CreatePreparedStatement / ClosePreparedStatement（内存映射，handle 前缀 `ps:`）
- handshake 返回空 token（无鉴权，兼容 ADBC/JDBC 先握手行为）

### 7.3 实现中发现并修复的互操作 bug（ADBC Go 驱动实测暴露）

| # | 问题 | 修复 |
|---|---|---|
| 1 | action 请求/响应未按官方约定 **Any 包装**（`FlightSqlService` blanket 实现为准），Go 驱动报 "mismatched message type" | 请求 `Any::decode + unpack`；响应 `result.as_any().encode_to_vec()` |
| 2 | `dataset_schema` 用裸 flatbuffer——规范要求 **IPC 封装消息格式**（0xFFFFFFFF continuation + u32 len + flatbuffer），Go 驱动报 "invalid message metadata" | `schema_ipc_bytes` 加封装前缀（GetTables.table_schema 列同） |
| 3 | endpoint `location = [uri:""]`——pyarrow 容忍，Go 驱动把 "" 当字面地址拨号失败 | **省略 location**（空列表 = 使用当前连接） |
| 4 | 元数据 schema 与官方定义不一致（catalog_name 可空性 / GetTables 列名） | 对齐 `arrow_flight::sql::metadata`：catalog_name NOT NULL、db_schema_name NOT NULL、GetTables 列名 `table_schema` |
| 5 | prepared INSERT 无法规划 dataset_schema（DataFusion 不支持 DML 规划） | 回退目标表 `SELECT * FROM <t>` schema（S1.6 INSERT sink 落地后可走逻辑计划） |

### 7.4 验证

- `cargo test --workspace` 全部通过（含 flight_sql_e2e：语句查询 / 元数据 / prepared 写入 / 双轨合流）
- **独立客户端冒烟**（`scripts/pyarrow_smoke.py`）：ADBC FlightSQL Go 驱动
  （`adbc-driver-flightsql` 1.12）SELECT / GROUP BY / get_objects 元数据 ✓；
  手写 protobuf wire + pyarrow 原始 FlightClient：CreatePreparedStatement →
  DoPut(CommandPreparedStatementUpdate) 绑定数据 → 装载 1 行 → ADBC 复核可见 ✓
- 冒烟脚本用法：`YUNTUN_DEMO_SERVE=1 cargo run -p yuntun-server --example demo` 后
  `python scripts/pyarrow_smoke.py <addr>`（demo 新增 serve 模式）

### 7.5 【最终裁决】协议端口简化：撤销 Hook / Gateway 间接层（架构 §3.2 v12.3）

用户两条原则定稿：

1. **所有写入都走 ingest 管线——唯一的数据写入事实**（WAL 权威，禁绕过）；
2. 协议端口不按"读域/写域"拆分——**一个节点上的 server 承载全部协议端口**
   （Flight SQL、InfluxDB LP、未来 MySQL/PG wire），每个协议内部把
   **写路由到 ingest 能力、读路由到 query 能力**；§7.1 的"FlightSQL 归 query 域"
   中间方案被此裁决取代（FlightSQL 同时服务读写，放任何域都别扭）。

**实施（v12.3）**：

- 删除 `FlightIngestHook` / `FlightQueryHook` / `SqlAppendHook` / `Gateway` /
  `UnifiedFlightService` 五个间接概念及 `server/hook.rs`；
  ingest / query 的 arrow-flight、tonic、prost 依赖移除（回归纯能力 crate）
- `yuntun-server::flight::FlightServer` 直接持有 `Arc<Ingestor>` + `Arc<QueryEngine>`，
  单文件实现 FlightService（协议解码 + 三轨路由 + 元数据批构建）
- `QueryEngine` 错误（DataFusionError）在 server 层直接转 Status（query_status）；
  QueryEngine 新增 `schema_of()`（逻辑计划取 schema）、开启 information_schema——保留
- 文档同步：架构 §3.2 / §13.1.1 / 附录 B、计划书 §1.3 / §四 / S1.3、设计 §2.1

**分层定稿**：能力层（ingest / query / compaction，纯逻辑）→ 节点层（server：协议端口 +
装配 + 路由）→ 入口（standalone / 未来按角色裁剪的分布式节点）。

## 8. 设计变更：SQL 前置解析拦截（2026-09-09，v12.4，S1.6/S1.7 实施前定稿）

### 8.1 用户裁决

1. **所有写入都走 ingest 管线**——唯一的数据写入事实；
2. 后期分布式/多节点时，**单个 server 提供各类协议通信（含读和写）**：
   写由 ingest 底层能力负责，读由 query 底层能力负责；
3. **server 做 SQL 前置解析拦截，只有 SELECT 让 DataFusion 处理**。

### 8.2 设计定稿（计划书 §4.3 路由表）

| 语句 | 处理 |
|---|---|
| SELECT / CTE | → DataFusion（query 能力，只读） |
| SHOW TABLES | → Catalog `list_tables` |
| INSERT INTO t VALUES ... | server 按表 schema 解析字面量 → RecordBatch → `ingest.ingest` |
| INSERT INTO t SELECT ... | server 经 DataFusion 执行 SELECT（读），结果 cast 到表 schema → ingest |
| CREATE TABLE [IF NOT EXISTS] | server 解析列定义 → Catalog `create_table` |
| DROP TABLE [IF EXISTS] | → Catalog `drop_table`（**需新增 meta 能力**） |
| 其他 | 明确拒绝 |

MVP 限制：VALUES 仅字面量；列清单支持指定列（缺省填 NULL）；类型按表 schema 逐列转换；
TIMESTAMP 字面量 ISO8601（UTC）/ 整型毫秒；`INSERT ... SELECT` 位置对齐 + cast；
语句级幂等键（`dml-<uuid>`）。

### 8.3 被否决的方案（留档防回潮）

**DataFusion DML sink 方案**（曾写入计划书 v2.0.2 S1.6）：在 `YuntunTableProvider` 实现
`insert_into` + `DmlSink` trait + `IngestSinkExec` ExecutionPlan，让 DataFusion 处理
INSERT。**否决理由**：① DML 走 DataFusion 会让写路径穿过查询能力，边界模糊——
INSERT 是写入请求，理应在节点层被拦截后直送 ingest；② DataFusion 会话级 DDL/DML
副作用与持久化 Catalog 语义冲突；③ 需要把 sink 句柄穿透 cache→provider→table 三层
plumbing。④ 用户明确要求"只有 SELECT 让 DataFusion 处理"。

**同样否决**：手写 tokenizer 做前置解析——改用 **`sqlparser`**（与 DataFusion 同源
解析器，Apache-2.0），方言兼容性（标识符引用/类型名/字面量语法）与 DataFusion 天然一致；
版本对齐 DataFusion 55 所依赖版本，方言 MVP 用 `GenericDialect`。

**状态**：设计已定稿，**S1.6/S1.7 尚未实施**（下一步编码）。实施清单：
① catalog 新增 `drop_table`（CatalogOps + MemoryCatalog）；② server 新增 SQL 前置
解析模块：`Parser::parse_sql(GenericDialect)` → 按 `Statement` 变体分流
（Query→query / Insert(Values→按 schema 构造 RecordBatch、Select→query 执行后
cast)→ingest / CreateTable(Deprecated DataType→Arrow 映射)→catalog /
Drop→catalog / ShowTables→catalog），字面量→Arrow 类型转换含
civil 算法时间解析；③ `FlightServer` 增加 `run_sql` 前置分发并接入三轨入口
（简易 do_get / StatementQuery / StatementUpdate / prepared update）；
④ FlightServer 增加 Catalog 依赖；⑤ e2e：CREATE → INSERT（VALUES/SELECT）→
查询验证 → DROP。

## 9. 追加：S1.6 / S1.7 实施（2026-09-09，SQL 写入路径 + DDL）

### 9.1 实现内容（按 §8.3 清单）

| # | 内容 | 位置 |
|---|---|---|
| ① | `CatalogOps::drop_table` + MemoryCatalog 实现（表 + Schema 版本链 + 文件 Manifest 移除 → 数据文件转孤儿由孤儿清理回收；快照/apply 推进；缺失表报 TableNotFound） | catalog |
| ② | SQL 前置解析模块（纯函数）：单语句解析（多语句拒绝）/ INSERT 目标表名（AST 优先 + 不完整形态字符串回落）/ SHOW TABLES 批次 / CREATE TABLE DataType→Arrow 映射（INT/BIGINT/VARCHAR/TIMESTAMP/DECIMAL/BLOB/DATE/整型族…，NOT NULL → nullable=false，CTAS/OR REPLACE 明确拒绝）/ VALUES 字面量→RecordBatch（列清单、NULL 填充、NOT NULL 缺失报错、范围检查）/ SELECT 结果位置对齐 cast（arrow-cast）/ ISO8601 civil 算法时间解析（epoch ns，支持 T/空格分隔、Z/±HH:MM、1-9 位小数秒） | `server/src/sql.rs`（18 单测） |
| ③ | `FlightServer::run_sql` 前置分流，接入简易轨 do_get（ticket=SQL）、FlightSQL TicketStatementQuery、StatementUpdate(do_put)；INSERT 语句级幂等键 `dml-<uuid>`；FlightServer 增加 `catalog: Arc<dyn CatalogOps>` 依赖 | server/flight.rs |
| ④ | prepared statement 目标表解析改 AST 优先（`insert_target`），保留字符串匹配回落（兼容 `INSERT INTO t (a,b)` 不完整形态） | server/flight.rs |
| ⑤ | e2e `sql_dml_e2e.rs`：CREATE(IF NOT EXISTS) → INSERT VALUES（do_put StatementUpdate / do_get 两入口、列清单 + NULL 填充、TIMESTAMP ISO/整型毫秒）→ 查询验证 → INSERT SELECT（Int64→Int32 cast）→ SHOW TABLES → DROP(IF EXISTS + 缺失表报错) → UPDATE/DELETE/CTAS 拒绝 → **硬崩溃重启后 DDL + 数据均恢复** | server/tests |

### 9.2 设计定稿之外的关键决策（偏差留档）

1. **WAL 新增 `Ddl` 记录类型（type=5）**——§8.3 清单未含此项，但 S1.7 验收要求
   "DDL 跨重启可见"、S1.6 要求"SQL 写入的数据崩溃重启可恢复"，而 MemoryCatalog
   重启清零（C5）。方案：run_sql 在 Catalog apply 成功后 append `Record::Ddl
   {op, table, arrow_schema, default_format}`（顺序即因果）；`Lakehouse::build`
   在 resume_recovered **之前**重放（create 已存在 / drop 不存在均幂等忽略）。
   阶段 1 切 Raft 后 DDL 走 Meta 状态机，此通道可退役。
2. **恢复语义修正（阶段 0）**：`resume_recovered` 原对 Committed/S3Written 做
   "重新提交"——但阶段 0 Meta 重启即空、可见性实际由**攒批线程全量重读 WAL 重做
   flush** 提供（§2.2-6），两者叠加会产生**双份可见数据**（chaos E3 回归捕获，
   27 vs 18）。修正：阶段 0 跳过 Committed/S3Written 重提交（debug 日志留痕）；
   `commit_recovered_batch` 修正为携带正确 `table`（从 WAL Data 记录恢复）与
   `file_size`，留给阶段 1 持久 Meta。
3. **BatchState 新增 `file_size`**：恢复/重提交路径的 Manifest 原为
   `Default::default()`（file_size=0），而查询 scan 用 Manifest 的 file_size 做
   Parquet footer 范围读 → "file length is 0" 错误。现在 BatchS3Written 携带的
   file_size 随状态机重建（MVP 单文件全量输出，语义成立）。
4. **DataFusion 本地缓存感知**：缓存 TTL 30s，SQL CREATE 后立即 INSERT SELECT 会
   "table not found"。run_sql 在 DDL 成功后、INSERT SELECT 读源前强制
   `cache.refresh`（只读本地 Catalog，不破坏 C7）。
5. **INSERT ... SELECT 可见性**：源数据须已 commit（§4.5 攒批窗口后可见）。
   server 侧已尽量刷新缓存；测试/demo 中在写后等待 flush 再执行。
6. **DROP 后的遗留 Data**：WAL 中被 DROP 表的 Data 记录在重启重读时仍会 flush
   （commit 不检查表存在性）——Manifest 挂在不存在的表下、查询不可见；若之后
   **重建同名表**会复活旧数据。阶段 0 已知工件，阶段 2 chaos / 阶段 3 Meta
   状态机时收敛。
   > **已修复（2026-09-12，见 §21）**：攒批器按 WAL DDL 维护表存活/世代，flush 前丢弃
   > 陈旧世代分组；恢复路径同样校验，不再产生悬挂 Manifest。

### 9.3 MVP 限制（后续扩展点）

- `INSERT ... SELECT` 不支持列清单（仅位置对齐）；`UPDATE` / `DELETE` / `CREATE
  TABLE AS` / `OR REPLACE` / 多语句明确拒绝（NotImplemented + 支持列表）
- CREATE TABLE 类型映射为 MVP 子集（不支持 ARRAY/STRUCT/MAP/UUID/带时区时间戳/
  INTERVAL）；DECIMAL → Decimal128，未指定精度 → (38,10)
- TIMESTAMP 字面量：ISO8601（无时区按 UTC）与整型毫秒；列必须是无时区 Timestamp
- 幂等键由服务端按语句生成（`dml-<uuid>`）；Prepared statement options 携带
  `client_request_id` 的透传归 S1.8
- `SHOW TABLES` 输出对齐 DataFusion 列结构（table_catalog/table_schema/table_name）

### 9.4 验证

- `cargo test --workspace`：**88 passed / 0 failed**（新增 sql.rs 18 单测 +
  sql_dml_e2e 2 用例；chaos E3 5 轮滚动崩溃回归通过）
- `cargo clippy --workspace --all-targets`：0 警告
- demo（`cargo run -p yuntun-server --example demo`）新增 §6b：SQL CREATE /
  INSERT VALUES / INSERT SELECT / SHOW TABLES 全链路冒烟通过
- 依赖：workspace 新增 `sqlparser = "0.62"`（对齐 Cargo.lock 中 DataFusion 55
  依赖版本，无双版本共存）；server 新增 `arrow-cast = "59"`（SELECT 源 cast；注：
  arrow 59 元 crate 无 compute feature，cast 内核在独立的 arrow-cast crate）

## 10. 追加：S1.5 收尾——SQL 能力元数据（JDBC/DBeaver 兼容，2026-09-09）

### 10.1 实现

- **GetSqlInfo**：静态 `SqlInfoData`（arrow-flight `metadata::SqlInfoDataBuilder`）——
  SERVER_NAME=yuntun / SERVER_VERSION=env!(CARGO_PKG_VERSION) /
  **ARROW_VERSION=build.rs 从 workspace Cargo.lock 编译期提取**（arrow-rs 不导出
  版本常量；`option_env!("YUNTUN_ARROW_VERSION")` 回退 "59"）/ READ_ONLY=false /
  SQL=true / SUBSTRAIT=false / TRANSACTION=None（单语句自动提交语义）/
  CANCEL=false / BULK_INGESTION=false / 两类 Timeout=0。按请求过滤由
  `CommandGetSqlInfo::into_builder` 完成。
- **GetXdbcTypeInfo**：静态 `XdbcTypeInfoData`，12 种类型对齐 CREATE TABLE 的
  DataType 映射（BOOLEAN/TINYINT/SMALLINT/INTEGER/BIGINT/REAL/DOUBLE/DECIMAL/
  VARCHAR/BINARY/DATE/TIMESTAMP），含 column_size（数值型按 bit 位宽惯例）、
  literal 前后缀（引号型）、DECIMAL 的 create_params/scale、NUM_PREC_RADIX=2；
  支持 `data_type` 过滤。

### 10.2 真实客户端冒烟发现并修复的互操作 bug

**空结果集返回 0 字段 schema**——`SELECT ... WHERE` 无匹配行时，do_get 首条
Schema 消息为空 schema，与 GetFlightInfo 声明的查询 schema 不一致，ADBC 1.12
直接报 "FlightSQL endpoint returned inconsistent schema"（JDBC/DBeaver 浏览
空表必踩）。修复：`do_get` 空结果时用 `QueryEngine::schema_of`（逻辑计划）作为
首条 Schema 消息（`encode_stream_with_schema` + `FlightServer::sql_schema_of`），
标准轨与简易轨同时覆盖。

### 10.3 冒烟脚本与客户端行为记录

- 新增 `scripts/pyarrow_sqlinfo_smoke.py`：手写 protobuf wire + pyarrow 原始
  FlightClient（GetSqlInfo 过滤 / GetXdbcTypeInfo 全量+过滤）+ ADBC 1.12 复核
  （SELECT / get_objects）+ executeUpdate 落库验证。全链路 ALL OK。
- **pyarrow C++ 客户端 quirk**（非服务端 bug，留档）：
  1. `do_put` 的 PutResult 回执需**并发读取**——`writer.close()` 后再
     `reader.read()` 返回 None（C++ 客户端行为；服务端已确认发送，Rust tonic
     e2e 阻塞式读回执正常）；
  2. 并发读回执时 `writer.close()` 可能阻塞——冒烟脚本改为不读回执、以 ADBC
     落库结果断言。
- 验证：`cargo test --workspace` 全过（flight_sql_e2e 新增空结果 schema 一致性
  断言）；clippy 0 警告；`yuntun.toml.example` 未动（server.listen 默认 50051）。

## 11. 追加：SQL 访问协议实施启动（sql-access-design.md v1.1 → Q1 部分完成，2026-09-09）

> **状态**：⚠️ 半成品提交（workspace 编译通过、clippy 待跑、**全量测试未跑**——时间
> 紧迫先落盘，接手者先执行 §11.3 待办 W-0 回归基线再继续）。

### 11.1 设计文档（已定稿，随本提交入库）

- `docs/sql-access-design.md` v1.1：SQL 处理层（yuntun-sql 能力 crate）与协议适配层
  （yuntun-sqlwire）分界；早期计划 = 基础能力 + Flight SQL（收尾）+ MySQL（opensrv-mysql
  :3306 新建）两个协议端口；PG wire 设计预留 §5.5。

### 11.2 本次已完成（Q1 部分完成）

| 项 | 内容 |
|---|---|
| `crates/sql`（yuntun-sql 能力 crate）新建 | **SQL 处理层唯一实现**：`SqlEngine{ingest,query,catalog}`（裁决 S-1/S-4，引擎无状态） |
| `sql.rs` 迁入 | server/src/sql.rs 的纯函数模块 git mv 迁入并适配（`yuntun_query::CATALOG_NAME/SCHEMA_NAME`；新增 `parse_single_with(方言)`、`insert_target_table_str` 回落函数随迁、`sql_snippet`）；**18 个既有单测随迁** |
| `params.rs`（G3） | `SqlValue` 参数模型 + **AST 级占位符替换**（sqlparser visitor feature 的 `visit_expressions_mut`，非字符串拼接）+ `TIMESTAMP '...'`/`DATE '...'` TypedString 渲染（civil 算法，无 chrono）+ 计数/替换单测（含注入转义 `'a''b'`） |
| `session.rs`（S-4） | `SessionCtx{dialect, default_db}` + `SqlDialect{Generic,MySql,PostgreSql}` → sqlparser 方言静态映射 |
| `shim.rs`（G5） | MySQL 方言 shim：SET/USE/事务 no-op（含 ROLLBACK，偏差留档）、SHOW VARIABLES/Variable（canned：version=8.0.32-yuntun 等）、SHOW TABLES（MySQL 单列语义）、SHOW COLUMNS（6 列格式）、SHOW CREATE TABLE、SELECT @@var / DATABASE() 探测（无 FROM 查询） |
| `lib.rs` | `SqlEngine::execute/prepare/execute_prepared/list_tables/describe_table/schema_of` + `WritePolicy::AllowWrite|ReadOnly`（§6.2 sqld readonly 注入点）+ `ingest_batches`/`append_ddl`（自 flight.rs 迁入，语义不变） |
| 过渡兼容 | `server/src/sql.rs` 改为 `pub use yuntun_sql::sql::*` 薄重导出——flight.rs 行为不变，**workspace 编译通过** |

### 11.3 待办（接手者按序执行；设计依据 sql-access-design.md §七 WBS）

| ID | 任务 | 关键位置/要点 |
|---|---|---|
| **W-0** | 回归基线确认（本次提交未跑测试） | `cargo test --workspace`（预期全绿：sql crate 19+18 单测 + 既有 88） |
| **W-1** | flight.rs 改调 SqlEngine（Q4）：删除 `run_sql`/`SqlOutcome`/`execute_update` 内嵌分流/`ingest_batches`/`append_ddl`/`sql_schema_of`/`insert_target_table`，改 `self.sql.execute(&sql, &mut SessionCtx::default())`；`FlightServer::new(ingest,query,catalog)` **内部构造** `Arc<SqlEngine>`（签名不变 → 4 个调用点与测试零改动）；`do_get` 空结果集回填 schema 用 `SqlEngine::schema_of`；prepared dataset fallback 的 `insert_target` 用 `yuntun_sql::insert_target` | crates/server/src/flight.rs；删 server/src/sql.rs 过渡文件；**回归：flight_e2e/flight_sql_e2e/sql_dml_e2e + pyarrow 冒烟全绿** |
| **W-2** | QueryEngine G2 修复 | crates/query：`session()` 的 SessionConfig 加 `with_default_catalog_and_schema(CATALOG_NAME, SCHEMA_NAME)`——注意回归：非限定表名行为变化是修复目标，既有 e2e 用限定名不受影响 |
| **W-3** | yuntun-sqlwire crate（Q5/Q6，裁决：opensrv-mysql） | 先 `cargo add opensrv-mysql` 查实际 API（版本间差异大），实现 `AsyncMysqlShim` 回调：on_query→SqlEngine.execute→文本行编码；on_prepare/on_execute→prepare/execute_prepared（opensrv 负责二进制参数解码则直接用，否则自解码）；auth=trust（users 空）；标准端口 :3306 |
| **W-4** | server Config [sql.mysql] + 挂载 | config.rs 加节（enabled/listen/auth/users）；serve 流程 spawn mysql listener（sqlwire 提供 serve_mysql(engine, listen, shutdown)）；standalone yuntun.toml.example 更新 |
| **W-5** | MySQL 冒烟（T1/T2） | pymysql 纯 python（aliyun pip 装）连 :3306：SELECT/SHOW TABLES/INSERT + prepared；DBeaver 手工 T3 清单 |
| **W-6** | 收尾 | clippy --workspace --all-targets 0 警告；README mysql 连接示例（S1.11） |
| **W-7** | 已知注意点 | ① shim 仅对 MySql 方言生效（Flight Generic 不拦截，保持既有 e2e 行为）；② `SELECT 1, @@x` 混合投影会退化为 DataFusion 报错（可接受）；③ ROLLBACK no-op 与设计"明确错误"有偏差（CLI 友好优先，已留档）；④ 混合写入 `USE` 后 session.default_db 记录但表名解析仍 yuntun.public（R-3） |

### 11.4 未提交文件说明

- `docs/ingestor-design.md`：保持未跟踪（前次指示：评审后单独提交）。

## 12. 追加：Q4 Flight 接线完成 + G2 修复（W-1/W-2 落地，2026-09-09）

### 12.1 完成内容

| 项 | 内容 |
|---|---|
| **W-1（flight → SqlEngine）** | `FlightServer` 增加 `sql: Arc<SqlEngine>`（`new(ingest,query,catalog)` 内部构造，**签名不变** → 4 个调用点零改动）；`run_sql` 变薄委托 `SqlEngine::execute`（默认 Generic 方言，shim 仅 MySql 方言生效，Flight 轨行为不变）；删除内嵌 `run_sql` 分流体 / `SqlOutcome` / `ingest_batches` / `append_ddl` / `sql_schema_of` / `sql_snippet` / `insert_target_table`；过渡 `server/src/sql.rs` 删除；server 依赖移除 `sqlparser`/`arrow-cast` |
| 错误映射 | 新增 `sql_status`：SqlError → Status（Parse/Unsupported/ReadOnly→invalid_argument；NotFound→not_found；TableExists→**already_exists**（较 v12 内部错误更准确）；Precondition→failed_precondition（幂等键强制）；Internal→internal） |
| do_get 空结果集 | `SqlResult::Rows{schema}` 自带回填 schema（引擎侧完成，G7 语义），fallback 逻辑简化 |
| **W-2（G2）** | `QueryEngine::session()` 的 SessionConfig 加 `with_default_catalog_and_schema("yuntun","public")`——非限定表名 `FROM t` 对 wire 客户端直接可用 |
| 验证 | 全量回归 **93 测试全绿**（与 W0 基线一致：flight_e2e 3 / flight_sql_e2e 2 / sql_dml_e2e 2 / yuntun-sql 20）；clippy -p server/query/sql --all-targets **0 警告** |

### 12.2 下一步（§11.3 余项）

- **W-3**：yuntun-sqlwire crate + opensrv-mysql（先 `cargo add opensrv-mysql` 查实际 API）
- **W-4**：server Config `[sql.mysql]`（enabled/listen :3306）+ 挂载
- **W-5**：pymysql 冒烟 T1/T2 → **W-6** clippy 全 workspace / README（S1.11）

## 13. 追加：W-3 yuntun-sqlwire crate 初版（WIP 提交，2026-09-10）

> **状态**：⚠️ WIP——`cargo check -p yuntun-sqlwire` **0 错误**；单元测试与
> 全量回归**未跑**（接手先跑：`cargo test -p yuntun-sqlwire && cargo test --workspace`）。

### 13.1 已完成（crates/sqlwire 新建，workspace members 已加）

| 文件 | 内容 |
|---|---|
| `lib.rs` | `MysqlBackend`（实现 `opensrv_mysql::AsyncMysqlShim`）：`on_query`→`SqlEngine::execute`（SessionCtx::mysql() 方言，shim 拦截 SET/USE/SHOW 生效）；`on_prepare`→`engine.prepare` + 参数 Column 声明（统一 VAR_STRING）+ stmt_id 缓存；`on_execute`→参数解码→`execute_prepared`；`on_close` 清缓存；`on_init`（handshake 库名/USE 转发）→ 记录 default_db + `writer.ok()`。错误路径：`query_error` 用 `results.error(ErrorKind, msg)` 回 **ERR 包**（连接不断开）——NotFound→ER_NO_SUCH_TABLE、ReadOnly→ER_OPTION_PREVENTS_STATEMENT、Parse/Unsupported→ER_SYNTAX_ERROR。`serve_mysql(engine, listen, shutdown)`：TcpListener + 每连接 spawn `AsyncMysqlIntermediary::run_on`（trust 鉴权默认）+ CancellationToken 优雅关停 |
| `encode.rs` | Arrow→MySQL 编码：`columns_of`（schema→Column，nullable→flags）、`arrow_to_mysql_type`（同源映射 SqlEngine::arrow_to_mysql_type）、`write_row`（逐列 downcast + **write_col 同步 + end_row().await**（0.7 API 关键差异））、`format_date/format_ts/civil_from_days`（无 chrono，与 params.rs 解析互逆）、`format_decimal` |
| 参数解码 | `param_value(ValueInner)→SqlValue`：NULL/Int/UInt/Double/Bytes（UTF-8 优先）/ **Date·Datetime·Time 二进制手动解码**（pymysql datetime 参数路径：`[len][y:u16][m][d][h][m][s][micros:u32]`；time 含负值/天数扩位） |

### 13.2 opensrv-mysql 0.7 API 踩坑记录（对接手者重要）

1. **行写入模式**：`RowWriter::write_col` 是**同步** fn；每行写完必须 `end_row().await`，收尾 `finish().await`；
2. **错误回包**：`QueryResultWriter::error(kind, msg)` 只有两个参数且 `msg: Borrow<[u8]>`（传 `&format!(..).into_bytes()`）；
3. **参数迭代**：`ParamParser` 实现 `IntoIterator<Item=ParamValue{value: Value, coltype}>`；`Value` 是私有字段 newtype，取 `ValueInner` 用 `v.value.into_inner()`（**无公开构造器** → 单测直接构造 `ValueInner`）；
4. **on_init** 签名含 `InitWriter`，必须调用 `writer.ok()` 或 `writer.error(..)`；
5. `process_use_statement_on_query` 默认 false → USE 走 `on_init`（不进 on_query，方言 shim 的 USE 拦截仅兜底）；
6. **修正（§14.1）**：`ValueInner::Date/Datetime/Time` 的负载**不含**长度前缀——opensrv 在
   `src/value/decode.rs` 里已 `read_u8()` 消费掉长度并按其截取负载。初版"首字节即长度"的
   假设会把年份低字节当长度（2026 → 0xEA = 234）并中断连接，实测后已重写。

### 13.3 待办（接续 §11.3）

| ID | 任务 |
|---|---|
| **W-3.5** | `cargo test -p yuntun-sqlwire`（含 encode 日期/decimal/二进制日期单测已写未跑）+ 全量回归 |
| **W-4** | server `Config [sql.mysql]`（enabled/listen :3306/auth users 预留）+ serve 流程挂载 `serve_mysql`（standalone yuntun.toml.example 更新） |
| **W-5** | pymysql 冒烟 T1/T2（aliyun pip 装 pymysql）：SELECT/SHOW TABLES/INSERT + prepared |
| **W-6** | clippy --workspace --all-targets 0 警告；README MySQL 连接示例（S1.11） |

## 14. 追加：W-3.5 / W-4 / W-5 / W-6 完成（2026-09-11）

> 至此 §13.3 待办全部收口；【W-3.5 测试】→【W-4 挂载】→【W-5 冒烟】→【W-6 收尾】
> 全量回归 **103 测试全绿**、`clippy --workspace --all-targets` **0 警告**。

### 14.1 W-3.5：sqlwire 单测修复（`cargo test -p yuntun-sqlwire` 7 全绿）

| 项 | 结论 |
|---|---|
| `encode.rs` 日期/时间戳 | **测例常量错、实现对**：19723 天 = 2024-01-01（非 02-29，应为 **19782**）；1_641_619_200 = 2022-01-08 **05:20:00**（应为 1_641_600_000）。`civil_from_days` 仍以 19000 = 2022-01-08 为锚验证 |
| `decode_time` 微秒偏移 | 真 bug：micros 原按 `[8..12]` 读取，实际在 sec 之后（`[9..13]`）→ 已修并补带微秒用例 |
| **二进制日期布局** | 见 §13.2 第 6 条：opensrv 已消费长度前缀，负载长度即字段数（0/4/7/11 与 0/8/12）。重写 `decode_datetime` / `decode_time`：**无长度前缀**、按 `b.len()` 分派、畸形长度返回 `io::Error`（不再 panic 杀 worker） |
| 新增用例 | `malformed_binary_params_error`（非法负载长度）+ TIME 带微秒；`param_decoding` / `binary_datetime_decoding` 随布局重算 |

### 14.2 W-4：`[sql.mysql]` 配置与挂载

| 位置 | 内容 |
|---|---|
| `server/src/config.rs` | 新增 `SqlSection{ mysql: MysqlSection }` / `MysqlSection{ enabled=true, listen="0.0.0.0:3306", auth="trust", users=[] }` / `MysqlUser`（设计 §6.3；R-2 标准端口）。users 非空仅 `warn`（当前只实现 trust，R-1）;单测 `mysql_section_defaults_and_override` |
| `server/src/lib.rs` | `Lakehouse` 增 `sql: Arc<SqlEngine>`（装配第 ⑦ 步）；`FlightServer::with_sql()` 复用同一实例（`serve_flight` 已接，write_policy 单点生效）；新增 **`spawn_mysql(lakehouse, cfg) -> Result<Option<JoinHandle>>`**：`enabled=false` → `Ok(None)`，**先 bind 再 spawn**（3306 被占时启动即 `Err`，不静默降级） |
| `sqlwire` | 新增 `serve_mysql_on(engine, listener, shutdown)`（接管外部已绑定 listener）；`serve_mysql` 保留为 bind + 转发 |
| `standalone/src/main.rs` | 装配后 `spawn_mysql`，flight 退出（shutdown）后 await mysql 句柄 |
| `yuntun.toml.example` | 增 `[sql.mysql]` 段（含 auth/users 注释说明） |

### 14.3 W-5：MySQL wire 独立客户端冒烟（`scripts/pymysql_smoke.py`）

环境：**项目内虚拟环境 `scripts/.venv`**（已 gitignore，阿里云镜像）：
`python3 -m venv scripts/.venv` + `scripts/.venv/bin/pip install -r scripts/requirements.txt`。
`requirements.txt` 的 MySQL 轨补齐 `mysql-connector-python`（T2 预编译依赖，此前只在临时环境里装过）。
standalone 用 `/tmp/yuntun-smoke/yuntun.toml`（flight 50077 / mysql 33060 / 攒批 1s / 查询缓存 1s）。

| # | 验证 | 结果 |
|---|---|---|
| T1 | pymysql（COM_QUERY 文本）：SHOW VARIABLES(`version=8.0.32-yuntun`) → CREATE TABLE → INSERT → SELECT → SHOW TABLES / SHOW COLUMNS → 错误路径 **1146**（ERR 包，连接保持，`SELECT 1` 仍可用） | 全绿 |
| T2 | mysql-connector-python `prepared=True`（COM_STMT_PREPARE/EXECUTE）：参数绑定写入 ×2（含 **datetime 二进制参数**）→ 文本轨复核 3 行可见 | 全绿 |

冒烟暴露并已修的两处实现问题：

1. **表不存在被包成 1105**：`SELECT * FROM no_such` 走 DataFusion → `SqlError::Internal`。
   `yuntun-sql` 新增 `query_error()`：消息含 "not found" 即归类 `NotFound`（取引号内限定名）
   → MySQL 1146 / Flight NOT_FOUND（单测 `query_error_classifies_missing_table`）。
2. **TIMESTAMP 列不接受 ISO 文本字面量**：prepared datetime 参数解码后即 `'2026-01-02 03:04:05'`，
   `build_values_batch` 的 Timestamp 分支只收 `TimestampNs`/`Num` → 补 `L::Str` → `parse_iso8601_ns`
   分支（同时让 `VALUES ('2026-01-02 03:04:05')` 这类常规写法可用；单测
   `values_timestamp_accepts_iso_string`）。

**已知限制（R-4 偏差，留档；→ 已在 §16.4 修正定位并修复）**：当时误判为
"opensrv-mysql 0.7 没有二进制结果集编码"。实际 `COM_STMT_EXECUTE` 走的就是二进制行
（`QueryResultWriter::new(..., is_bin = true)`）；prepared SELECT 取不到行的真因是
opensrv 0.7 `PacketReader::next_async` 的释放后使用（上游 #66/#67，仅修在 git、
未随 crates.io 的 0.7.0 发布），详见 §16.4。JDBC 默认 `useServerPrepStmts=false`、
mysql CLI / pymysql 走文本轨，均不受影响。

### 14.4 W-6：收尾

- `cargo clippy --workspace --all-targets` **0 警告**（修 `SqlSection` 手工 `Default` → `derive(Default)`）；
  rust-analyzer 侧另有 `overflow evaluating the requirement`（`#[async_trait]` 展开 + DataFusion
  深嵌套 future 的 `Pin<Box<dyn Future + Send>>` 强转）提示——`flight.rs:393` 同款、
  属既有现象，非编译错误也未阻断构建；
- `cargo test --workspace --no-fail-fast` **103 通过 / 0 失败**（基线 93 + sqlwire 7 + sql 2 + server config 1）。
  `yuntun-wal::cleanup::tests::monitor_aborts_timed_out_batches` 为**时间敏感 flaky**
  （全量并发跑时偶发 "fresh batch must survive"，单跑通过；与本次改动无关，未处理）；
- 遗留：README 与 DBeaver 实测见 §15（本次一并补上）。

## 15. 追加：Q7 DBeaver / JDBC 实测（T3）+ shim 补齐 + README（2026-09-11）

### 15.1 实测环境

| 项 | 值 |
|---|---|
| 客户端 | 本机 `/usr/bin/dbeaver` + `java/javac`；驱动取 DBeaver 自带 **Connector/J 8.0.29**（`~/.local/share/DBeaverData/drivers/maven/...`） |
| 服务端 | standalone：flight `127.0.0.1:50078` + mysql `0.0.0.0:3306`（**标准端口实测可用，R-2**） |
| 数据 | `api_audit(event_time TIMESTAMP, user TEXT, endpoint TEXT, cost_ms INT)` 3 行 |

### 15.2 T3 脚本化验证：`scripts/dbeaver_jdbc_probe.java`（22 项全过）

| # | 项 | 结果 |
|---|---|---|
| 1 | 握手（驱动识别服务端版本） | ✅ MySQL 8.0.32-yuntun |
| 2 / 5 | `getCatalogs` / `getTables` | ✅ public / api_audit |
| 6 | `getColumns` | ✅ 4 列（DATETIME / TEXT / TEXT / INT） |
| 7 / 8 | `getPrimaryKeys` / `getIndexInfo` | ✅ 空结果集（无主键/索引语义正确） |
| 10–12 | 数据预览 / count / group by | ✅ |
| 13–22 | `@@version_comment`、`DATABASE()`、`information_schema.tables`、`SHOW FULL TABLES`、`DESCRIBE`、`SHOW COLLATION/CHARSET/ENGINES/KEYS`、`SHOW CREATE TABLE` | ✅ |

> 编译/运行：`javac -cp $JAR scripts/dbeaver_jdbc_probe.java` → `java -cp "$JAR:scripts" dbeaver_jdbc_probe`。
> 该脚本即 T3 的 CI 化替代（GUI 手工清单已在 README §2.2 给出）。

### 15.3 实测暴露并补齐的 shim 缺口（`crates/sql/src/shim.rs`）

| # | 现象 | 修复 |
|---|---|---|
| 1 | 握手即失败：`SELECT @@session.auto_increment_increment` 落 DataFusion（"variable has no type information"） | sqlparser 把 `@@var` 解析为 **Identifier / CompoundIdentifier**（非 Placeholder）→ `classify_probe_expr` 增补两种形态，变量名取末段并去 `@@`；同时支持 `ExprWithAlias`（驱动普遍带别名） |
| 2 | JDBC 取不到部分变量 | `canned_variable` 补 `auto_increment_*`、`character_set_server`、`system_time_zone`/`time_zone`、`wait_timeout`、`net_*_timeout`、`max_connections`、`query_cache_*`、`transaction_read_only`、`have_ssl`、`version_compile_*`、`port` |
| 3 | `getColumns` 抛 `Column 'Collation' not found` | `SHOW FULL COLUMNS` 改 **9 列**（Field/Type/Collation/Null/Key/Default/Extra/Privileges/Comment）；普通 `SHOW COLUMNS` 仍 6 列 |
| 4 | `DESCRIBE / DESC t` 报 1149 不支持 | 拦截 `Statement::ExplainTable`（DESCRIBE 语义 = SHOW COLUMNS，6 列）；EXPLAIN 同变体，MVP 不做执行计划（留档） |
| 5 | `getPrimaryKeys/getIndexInfo` 抛 `Column 'Key_name'/'Table' not found` | sqlparser 0.62 **无** `SHOW KEYS/INDEX/ENGINES` 变体，它们落到 `ShowVariable`，且 `SHOW KEYS FROM t` 的表名也被收进 `variable` → 按**首段**特判，返回列名齐全的**空结果集**/engines 表（原来返回 `Variable_name/Value` 两列，驱动按名取值必炸） |
| 6 | `SHOW COLLATION / SHOW CHARSET` 报 1149 | canned 结果集（utf8mb4_general_ci / utf8mb4） |
| 7 | Q3 遗留的 shim 单测缺失 | 新增 `shim::tests`（探测表达式形态 × canned 值、SHOW COLUMNS 列形状、SHOW KEYS 列名） |

### 15.4 文档：新建 `README.md`（S1.11 / Q7 交付）

仓库此前**无 README**：补齐 quickstart（构建 / 配置 / 启动）、**Flight SQL 与 MySQL
双协议最小闭环示例**、**DBeaver 连接步骤与驱动属性**（`useServerPrepStmts=false`、
`useSSL=false`、`allowPublicKeyRetrieval=true`）、兼容矩阵 T1–T4、已知限制、
冒烟脚本（`scripts/.venv`）、crate 结构。

### 15.5 DBeaver GUI 手工验证暴露：`information_schema` 补洞

脚本化探测（§15.2）之外，DBeaver GUI 浏览时还会查一批 DataFusion
`information_schema` **未提供**的 MySQL 元数据表，逐条弹

```text
SQL Error [1146] [42S02]: table not found: yuntun.information_schema.key_column_usage
  （同批：referential_constraints / triggers / statistics / partitions）
```

修法（`crates/sql/src/shim.rs`）：MySql 方言下拦截 FROM 命中
`information_schema.<缺失表>` 的查询，按 **MySQL 8 的列定义返回 0 行的空结果集**
——yuntun 无主键/外键/索引/触发器/分区概念，空集即正确语义；关键是**列名要齐**
（DBeaver / JDBC 按列名取值，缺列会抛 "Column not found"）。

补入清单：`key_column_usage`、`referential_constraints`、`table_constraints`、
`check_constraints`、`statistics`、`triggers`、`partitions`、`events`、
`processlist`、`engines`（此外 `tables` / `columns` / `schemata` / `routines` 等
DataFusion 已提供 → **不拦截**，保持原生实现）。
单测 `shim::tests::information_schema_gaps_are_filled`；JDBC 探测脚本加 [23]–[28] 全过。

### 15.6 本轮验证与遗留

- `cargo test --workspace` **106 通过 / 0 失败**；`cargo clippy --workspace --all-targets` **0 警告**；
- 遗留：`DatabaseMetaData::getSchemas` 返回 0 行（走 `information_schema.schemata` 路径未覆盖）
  ——DBeaver 按 catalog=`public` 浏览不受影响，留档不修；
- 遗留阶段 1 准出项：**S1.9 `yuntun-client` / `yuntun-cli`、S1.10 do_get 流式化、
  S1.8 幂等键透传**（README 的 CLI 手册待 CLI 落地后补）。

## 16. 追加：S1.9 `yuntun-client`（Rust SDK + CLI）落地（2026-09-11）

### 16.1 交付（新 crate `crates/client`，包名 `yuntun-client`，bin `yuntun-cli`）

| 文件 | 内容 |
|---|---|
| `src/lib.rs` | [`Client`]：`connect` / `query`（eager）/ `query_stream`（`FlightRecordBatchStream` 流式）/ `schema_of`（`get_schema` 的 IPC 解析）/ `table_schema` / `execute` / `list_tables` / `insert_batches` / `insert`；`InsertReceipt`（server `Receipt` 的 JSON 形态）；`generated_key()`——未显式给键时自动生成 `cli-<ms>-<pid>-<seq>`（表多为 `IngestConfig::standard()` **require** 幂等键，否则服务端回 FailedPrecondition） |
| `src/input.rs` | 导入解析：CSV（`Format::infer_schema` 推断后重读）/ JSONL（按字段名） / Parquet（读后 cast）；统一**按列名对齐 + `arrow::compute::cast`**，列顺序无关、缺字段填 NULL；stdin 仅支持 CSV/JSONL |
| `src/main.rs` | CLI：`query`（`--format table|csv|json`）、`insert`（`-t/-f/--format/--shard/--key`，无 `-f` 读 stdin）、`tables`、`schema`；`--addr` 或环境变量 `YUNTUN_ADDR`（默认 `127.0.0.1:50051`） |
| `tests/client_e2e.rs` | in-process FlightServer + SDK 全流程：DDL → schema 探测 → `do_put` 回执 → `list_tables` → 可见性 → 一次性/流式查询 → `INSERT ... VALUES` 与 DoPut 汇入同一管线 |

**协议复用（未新增协议）**：查询 `do_get(ticket = SQL)` + `get_schema(cmd = SQL)`；
写入 `do_put`（`path = [table, shard]` + 数据消息 `app_metadata` 幂等键）——即 plan §4.2 的
「简易轨 = 自有客户端快速通道」。

### 16.2 真实服务端 CLI 冒烟（S1.9 验收句式）

环境：standalone（flight `127.0.0.1:50078` / mysql `:3306`）+ `yuntun-cli`。

| 步骤 | 结果 |
|---|---|
| `query 'CREATE TABLE cpu (ts BIGINT, host TEXT, usage DOUBLE)'` → `schema cpu` → `tables` | ✅ OK / 3 列 / cpu 列出 |
| `insert -t cpu -f cpu.csv`（**列顺序打乱** host,usage,ts） | ✅ 2 行，按名对齐正确 |
| `insert -t cpu -f cpu.jsonl`（缺 `usage`） | ✅ 2 行，缺字段 → NULL |
| `insert -t cpu -f cpu.parquet`（`ts` 为 int32） | ✅ 2 行，读取时 cast 到 Int64 |
| `query 'SELECT * FROM cpu ORDER BY ts'`（table 输出） | ✅ 6 行；`--format csv` / `json` 输出正确 |
| `query 'INSERT INTO cpu VALUES (700, (1 + 1), 2.0)'` | ✅ 明确报错（INSERT VALUES 仅字面量，设计内限制） |

### 16.3 回归与稳定化

- `cargo test --workspace` **114 通过 / 0 失败**（client：6 单测 + 1 e2e + 1 doctest；基线 106）；
  `cargo clippy --workspace --all-targets` **0 警告**；
- e2e 原用固定 `sleep 2500ms` 等可见性，并发跑（叠加 clippy/其它 e2e）时会偶发超时
  → 改为**轮询 `wait_count`（≤10s）**，稳定且更快（5.16s → 2.10s）；
- 遗留：CLI/SDK 尚未入 CI 脚本；`yuntun-wal` 的 `monitor_aborts_timed_out_batches`
  仍是时间敏感 flaky（阶段 2 卫生项，未处理）。

## 17. 追加：S1.10 `do_get` 流式化 + S1.8 幂等键透传（2026-09-11）

### 17.1 S1.10：结果集流式化（验收：大结果集内存平稳）

| 层 | 改动 |
|---|---|
| `yuntun-query` | `sql_stream()`（`DataFrame::execute_stream`，不 collect）+ `stream_from_batches()`（canned/小结果集 → 流适配）；`SendableRecordBatchStream` re-export（协议层无需直接依赖 datafusion）；`futures` 复用 |
| `yuntun-sql` | `SqlStreamResult { Rows { schema, stream }, Affected }` + `SqlEngine::execute_stream()`：与 `execute` 同一分流，SELECT 直接交物理计划流，shim/INSERT/DDL 经 `stream_from_batches` 转流（协议层单一出口） |
| `yuntun-server` | `do_get`：标准轨 `TicketStatementQuery` 与简易轨 SQL 统一走 `do_get_sql()` → `FlightDataEncoderBuilder::with_schema(schema)` 边算边发（0 批也先发 schema，ADBC/JDBC 一致性校验）；INSERT/DDL 回空结果集；查询期错误在流中途以 `Status` 返回。元数据命令（GetTables/GetSqlInfo/XDBC）仍走批量路径 |

**实测**（`scripts/flight_stream_smoke.py`，真实服务端 + ADBC）：

| 规模 | 结果 |
|---|---|
| 100 万行写入（10×10 万，DoPut 简易轨） | 2.6s |
| 100 万行流式读取 | 0.3s |
| **500 万行**（列数据理论下界 ≈ 114 MB）流式读取 | 0.6s，**服务端 RSS 增量 2.2 MB**（基线 499.0 → 峰值 501.1） |

> eager 路径需一次性持有整个结果集（≥114 MB）；流式路径内存与结果集规模解耦。

### 17.2 S1.8：幂等键透传（验收：幂等矩阵单测覆盖）

**通道**（`yuntun-sql/src/idempotency.rs` + flight 层）：

| # | 通道 | 说明 |
|---|---|---|
| 1 | **SQL 注释**（任意协议） | `INSERT /* idempotency_key=<k> */ INTO t ...`（兼容 `-- idempotency_key=<k>` 行注释）；注释被解析器忽略，语义零影响 |
| 2 | **prepared 语句上的键** | `PreparedStatement.idempotency_key`（prepare 时解析并随语句缓存——参数替换后的 SQL 文本已不含注释）；MySQL COM_STMT_PREPARE 与 FlightSQL `ActionCreatePreparedStatementRequest` 共用 |
| 3 | **Flight `DoPut` 装载** | 键优先级：**每条 `FlightData.app_metadata` > prepared 上的键 > 语句级生成 `flightsql-<uuid>`**（`spawn_sql_ingest` 新增 `default_key`） |
| 4 | 兜底 | SQL 路径未给键 → `dml-<uuid>`；require 表**缺键拒绝**不变（架构 §7.3.2） |

**顺带修的真实缺陷**：`insert_target_table_str` 的前缀回落不认注释
（`INSERT /* ... */ INTO t` 匹配不到 `insert into`）→ 装载被误判为非 INSERT 走 SQL 执行并报解析错。
新增 `strip_comments()`（块/行注释 → 空格）+ 空白折叠后再匹配。

**测试**：
- `yuntun-sql`：`idempotency::extract` 3 组单测（形态/非法/未闭合）、`insert_target_tolerates_comments`（注释位置 ×3）、`strip_comments_keeps_word_boundaries`；
- `crates/server/tests/sql_idempotency_e2e.rs`：三通道端到端（SQL 注释 INSERT → 1 行；prepared 注释键装载 → +2；无键兜底 → +2，require 表全程 accept）；
- **真实协议冒烟**：CLI（`query 'INSERT /* idempotency_key=... */ ...'`）、MySQL 文本协议（pymysql）与 prepared（mysql-connector）带注释插入均成功且数据可见。

### 17.3 回归

- `cargo test --workspace` **120 通过 / 0 失败**（新增：sql 4 + server e2e 1）；`clippy --workspace --all-targets` **0 警告**；
- 文档：README 新增「幂等键」小节与流式说明（原「结果集 eager」限制已更新）、冒烟脚本表增加 `flight_stream_smoke.py`；
- 遗留：MySQL wire 轨结果集仍为 eager（逐行写）；CLI/SDK 未入 CI。

## 18. 追加：测试临时目录策略（tmpfs 加速）——`yuntun-testkit`（2026-09-11）

**动机**：写盘类测试（WAL / Parquet / local ObjectStore）在真实磁盘上要付 fsync +
元数据写开销；Arch/systemd 下 `/tmp` 本身就是 **tmpfs（内存盘）**，`fsync` 近似 no-op。

**本机基准**（500 × 64KB 文件 + 每文件 `fsync`）：

| 位置 | 耗时 |
|---|---|
| tmpfs（`/tmp`） | **38 ms** |
| 真实磁盘（`/home`，sda3） | **2422 ms**（≈64×） |

### 18.1 实现：新 dev-only crate `crates/testkit`（`publish = false`）

| API | 说明 |
|---|---|
| `tmp_root()` | 内存盘根：`YUNTUN_TEST_TMPDIR` > `$TMPDIR` > `/tmp` > `/dev/shm`（读 `/proc/mounts` 判定 tmpfs，取首个命中） |
| `disk_root()` | 真实磁盘根：`YUNTUN_TEST_DISKDIR` > `<workspace>/target/test-disk` |
| `is_tmpfs(path)` | 挂载点最长前缀匹配判定（非 Linux → false，保守当磁盘） |
| `TestDir::tmpfs/disk/at(name)` | 唯一目录（pid+序号）+ **Drop 自动清理**；`.string()` 便于塞进 TOML |
| `TestDir::into_path()` | 交出路径并放弃清理（供 `fn tmpdir() -> PathBuf` 形态的 helper） |

### 18.2 接入与分工

| 用例 | 位置 | 选择 |
|---|---|---|
| WAL 单测（segment / reader / recovery / cleanup） | `crates/wal` | **tmpfs** |
| ingest flush、query e2e、client e2e、client 导入单测 | `crates/{ingest,query,client}` | **tmpfs** |
| server e2e（flight / flightsql / sqldml / idempotency） | `crates/server/tests` | **tmpfs** |
| 故障注入（重启/崩溃恢复） | `crates/chaos` | **真实磁盘**（fsync 等待与重启后目录语义必须真实） |
| 压测 | `chaos/examples/bench` | **真实磁盘**（tmpfs 会让吞吐/延迟失真） |
| 手动示例 | `server/examples/demo` | **真实磁盘**（数据便于观察/复用） |

> 踩坑记录（值得保留）：`TestDir` 是 Drop-guard，**不能跨函数返回**——
> `client_e2e::start_server()` 内建目录后 return 给调用方，guard 在函数返回时
> 即删除目录，服务端后台攒批扫描报 `wal scan failed: No such file or directory`，
> 表现为"写入成功但查询恒为 0 行"。该 helper 已改用 `into_path()`。
> 教训：目录生命周期必须覆盖"服务/线程仍在使用"的整个区间。

### 18.3 效果

- `cargo test --workspace`（热态）：**124 通过 / 0 失败，约 16s**；
  写盘较重子集（wal+ingest+server+client）约 10s；
- 对照：同一子集把 `YUNTUN_TEST_TMPDIR` 指向真实磁盘，**首次冷跑 66s**（含大量文件
  创建 + 冷 page cache），热跑回落到 ~10s——差异主要来自 fsync 等待与文件系统元数据；
- 测试目录随 Drop 清理，重跑无残留（此前部分用例只删不建、或不清理）。

## 19. 追加：多 schema（MySQL 的 database）支持（S1.13，R-3 提前清偿）（2026-09-11）

**背景**：R-3 原定阶段 4；因阶段 2（Chaos/压测）会锁定存储路径与 WAL 格式，
提前到阶段 1 末尾实现，避免日后迁移成本。

### 19.1 核心约定

**全限定表标识 `schema.table`**（`yuntun_model::ops::qualified_name`）贯穿所有层：
Catalog 内部键、IngestBatch/WAL 的 `table` 字段、Manifest、Query 缓存键、对象路径派生；
裸表名只在 SQL 表面与 `TableMeta.name` 出现。默认 schema = `public`（旧数据自动归属）。

| 层 | 改动 |
|---|---|
| `yuntun-model` | `TableMeta.namespace`（protobuf tag 9，空值兼容旧数据）+ `CreateTableRequest.namespace`；`qualified_name / split_qualified / validate_schema_name`；`LakeError::Schema{NotFound,AlreadyExists,NotEmpty}` |
| `yuntun-catalog` | `namespaces` 注册表 + `create/drop/list_schemas/schema_exists`；建表校验 schema 存在；`list_visible_files/drop_table/evolve_schema` 等对表标识做 `normalize_table` 归一（裸名兼容） |
| `yuntun-query` | `LocalCatalogCache`（schemas 清单 + 限定名键 + `get_in`）；`YuntunCatalogProvider` 多 schema 视图；`QueryEngine::session_with_schema / sql_with_schema / sql_stream_with_schema / schema_of_with_schema` |
| `yuntun-sql` | `resolve_table_ref`（1 段→会话 schema；2 段 catalog 前缀特判；3 段取后两段）；dispatch 全链路限定名；`CREATE/DROP DATABASE(SCHEMA)`、`SHOW DATABASES`；MySql shim 的 `USE` **真实切换**（校验存在）；`SessionCtx::schema()/set_schema` |
| `yuntun-sqlwire` | handshake database 校验 + 切换；错误码 1049（unknown database）/ 1007 / 1008 |
| `yuntun-server` | `GetDbSchemas` 返回真实 schema 清单（此前固定 public） |
| `yuntun-format` | 对象路径按 schema 分层：`yuntun/<schema>/<table>/dt=.../...`（读取以 Manifest 为准，旧布局兼容） |
| WAL | `ddl_op::CREATE_SCHEMA/DROP_SCHEMA`（`DdlPayload.table` = schema 名）；CREATE/DROP TABLE 记录**全限定名**；重放恢复 schema 与表 |

### 19.2 验证

| 层 | 测试 |
|---|---|
| 单测 | `catalog::multi_schema_isolation_and_drop_rules`（建库/隔离/非空库拒绝/默认库不可删）、`format::file_path` 分层断言、`sql::resolve_table_ref_rules`（1/2/3 段 + catalog 前缀特判） |
| e2e | `crates/server/tests/multi_schema_e2e.rs`：建库 → 跨 schema **同名表**隔离（各查各的）→ 未知库报 404 → 非空库 DROP 拒绝 → 删表后 DROP → **崩溃重启后 schema 事件与表定义经 WAL 重放恢复** |
| 真实客户端 | `scripts/pymysql_smoke.py` 新增 [8.2]/[8.3]：`CREATE DATABASE` → `USE`（`DATABASE()` 反映当前库）→ 跨库隔离 → 限定名查询 → `USE no_such_db` 回 **1049** → 非空库删库回 **1008** → 清理后 `SHOW DATABASES` 不再出现 —— **ALL OK** |

### 19.3 回归与附带修复

- `cargo test --workspace` **126 通过 / 0 失败**；`clippy --workspace --all-targets` **0 警告**；
- 附带修复（§18 tmpfs→磁盘改造暴露）：chaos `crash_recovery_no_data_loss` 的恢复断言
  原依赖固定 500ms 等待（tmpfs 上足够、真实磁盘上"WAL 重放→flush→commit"超时）→ 改为
  **轮询等待（≤30s）**；并修正重排时 accumulator 被立即 cancel 的错误（它必须在轮询期间
  保持运行）；chaos 三用例经 `CHAOS_GATE` 串行（重 IO + 共享环境，消除时序噪声）；
- 遗留：`USE` 在 Flight/PG wire 的等价语义（`SET search_path`）未做（PG 客户端可用
  限定名）；`SHOW TABLES FROM db` 语法未支持（可用 `SHOW TABLES` + `USE` 或限定名查询）。

## 20. 协议决策：撤销无源 INSERT 形态，只支持标准完整语句（2026-09-12）

**考古**：无源形态 `INSERT INTO t (a, b)`（无 VALUES/SELECT 源）源于 S1.1-S1.5 手写
pyarrow protobuf 冒烟时的省事写法（数据反正走 Arrow 流），S1.6 实现时服务端为它
配了字符串匹配回落（`insert_target_table_str` + `strip_comments`）。它不是任何标准
或第三方客户端的要求：自研 SDK 批量写入走简易轨（PATH descriptor，无 SQL）；
ADBC 冒烟仅查询。

**决策**：FlightSQL `DoPut` 轨道统一约定客户端发送**标准完整语句**
`INSERT INTO t [(cols)] VALUES (?, ?)`——bind 数据是 Arrow batch，占位符被服务端
忽略；`insert_target` 退化为纯 AST 路径，删除字符串兜底与 `strip_comments`。
（曾评估自定义 sqlparser Dialect 支持 bind 模板：0.62 具备 `parse_statement` 钩子
与 `Insert.source: Option` 两个前提，技术上可行；但该形态本无标准依据，不值得
维护包装方言的委托成本与执行路径防呆——撤。未来标准路线是 `CommandStatementIngest`
，表名走 proto 结构化字段，零解析。）

| 改动 | 文件 |
|---|---|
| `insert_target` 纯 AST 化；删 `strip_comments` / `insert_target_table_str` 及单测 | `crates/sql/src/sql.rs` |
| 无源 INSERT 改完整形态（`VALUES (?, ?)`） | `flight_sql_e2e.rs` / `sql_idempotency_e2e.rs` / `pyarrow_smoke.py` |

回归：全量通过；mysql/flight 既有行为不变（MySQL wire `COM_STMT_PREPARE` 一直要求
完整语句，本决策只是把 Flight 轨道对齐到同一标准）。

## 21. 修复：写后可见性（读己之写）+ DROP 语义 + WAL/Catalog 一致性（2026-09-12）

### 21.1 冒烟现象与根因（先纠正归因）

冒烟报"DROP TABLE + 同名 CREATE TABLE 后新写入数据查不到（15s 轮询），重启后又能查到"。
实测（默认配置、**无 DROP、无重启、干净目录**）复现同一现象：

| 观测点 | 延迟 |
|---|---|
| INSERT（分钟第 0 秒）→ catalog 可见（flush+commit 落盘） | **29.1s**（= `public.mysql_smoke` 的 jitter 偏移） |
| → SQL 查询可见（走本地缓存） | **47.9s**（再叠一次缓存 TTL 落点） |

根因是**两条异步窗口**，与 DROP/CREATE 无关（`pymysql_smoke.py` 每次运行都先 DROP+CREATE，
因此被误当变量）：

1. Flush 时刻 = `window_start + hash(shard+table) % flush_jitter_secs`（ADR-10 削峰），
   默认 60s 内任意秒；写入发生在分钟前段时最长等 ≈jitter 秒；
2. 查询走 `LocalCatalogCache`，原只在 TTL（默认 30s）到点刷新。
   两者叠加最坏 ≈60s+30s，而回执 `expected_visible_in_secs` 写死 `time_threshold_secs`（5s）。

"重启后可见"= 重启触发立即缓存刷新 + 攒批线程全量重读 WAL（`window_ms` 变为重启所在分钟，
若已越过 jitter 秒则立即 flush）；"孤儿清理删旧文件"= DROP 后文件转孤儿，属设计语义。

### 21.2 修复项

| # | 修复 | 位置 |
|---|---|---|
| 1 | **读己之写 = store 层分片的两级形态**：`yuntun-store::shard` 定义 `ShardTier{Memory,Disk}` / `ShardId(table, shard, window)` / `MemoryShard`（内存分片，Live / Committed(snapshot) 两态）/ `DiskShard`（对象存储上的分片）/ `ShardStore` 门面。写入侧把已 fsync 未提交的批次写进内存分片、提交后交棒；`YuntunTableProvider::scan` 用 **内存分片 ∪ 磁盘分片（Parquet 文件组）**（`UnionExec`）做无空洞/无重复交接，缓存刷到该快照后 `sweep` 回收。**查询只依赖 store 层，不依赖 Ingestor 进程**；分离部署时仅替换本层实现 | `crates/store/src/{shard.rs,lib.rs}`、`crates/query/src/{table,provider,cache}.rs`、`crates/ingest/src/{pipeline,flush,accumulator}.rs`、`crates/server/src/lib.rs` |
| 2 | 回执 `expected_visible_in_secs` 改为真实上界（扫描周期），不再写死 5s | `crates/ingest/src/pipeline.rs` |
| 3 | **提交驱动缓存刷新**：内存分片变更计数一变即刷新（200ms 轮询），TTL 仅兜底 | `crates/query/src/cache.rs` |
| 4 | **DROP 语义**：攒批器按 WAL DDL 维护表存活/世代（epoch 进分组键），flush 前丢弃陈旧世代分组；恢复路径（Pending 批次重做）用 DDL 时间线校验世代，陈旧则 `BatchAbort` —— 不再产生悬挂 Manifest / 旧数据复活 | `crates/ingest/src/{accumulator,pipeline}.rs` |
| 5 | **`snapshot_version` 严格单调**：`commit_files/commit_compaction/drop_shard` 由 `load()+1`+`store()` 改为原子 `fetch_add`（原实现与 `drop_table` 交错时会把快照号**回退**，让已提交文件永久不可见） | `crates/catalog/src/lib.rs` |
| 6 | **WAL 水位/回执**：`WalWriter::open` 水位初值由 `last_seq` 改为 `synced_seq`（不再超出真实 fsync 边界）；组提交内每条记录回各自的 seq（此前整批都回 `last_seq`）；新增 `next_seq()` 用于区分"历史数据/本进程新写入" | `crates/wal/src/writer.rs` |

### 21.3 读侧接缝（`ShardReader` / `ShardFetch`）

为避免"热数据 = Ingestor 的进程内缓冲"这一错误耦合，读侧抽成 trait：

| 抽象 | 位置 | 实现 |
|---|---|---|
| `ShardReader`（`shards_of / read_shard / read_table / reclaim`） | `crates/store/src/shard.rs` | ① `MemoryShard`（进程内，阶段 0）；② `RemoteShard`（远端分片服务，传输由 `ShardFetch` 注入） |
| `ShardFetch`（`fetch_shards / fetch_shard / fetch_version`） | 同上 | 阶段 1：gRPC / HTTP；单测：假实现 |

查询侧只持有 `Arc<dyn ShardReader>`（`LocalCatalogCache::set_hot_shards` / `TableProvider.hot`），
`crates/query` 对 `crates/ingest` **无（非 dev）依赖**；分离部署只需把 `ShardStore::local(...)`
换成分片服务的 `RemoteShard`，SQL 侧零改动（验证见 `crates/query/tests/hot_shard_reader.rs`）。

### 21.4 验证

- 新增回归：`crates/server/tests/write_then_read.rs`（读己之写 latency < 1s 且此时 **零个已提交文件**；
  DROP + 重启 + 同名重建 → `count(*) = 0` 且无悬挂 Manifest）、
  `crates/ingest/tests/recovery_guard_e2e.rs`（陈旧世代 Pending 批次被 abort）、
  `crates/query/tests/hot_shard_reader.rs`（经 `ShardReader` 的**远端**实现读热数据，接缝可替换）、
  `catalog::snapshot_monotonic_under_concurrent_commit_and_drop`、`wal::writer::tests`（水位/逐条 ack）、
  `store::shard::tests`（分片隔离 / 世代 / prefix 对齐 / 两个 `ShardReader` 实现）；
- `cargo test --workspace` 全绿；`cargo clippy --workspace --all-targets` 0 警告；
- README「已知限制」与「最小闭环」的可见性描述同步更新；
- 副作用：接管了 `chaos::query_multi_version_alignment` 的固定 `sleep(500ms)`（改轮询 ≤30s，
  真实磁盘 + 并发负载下固定等待会偶发超时）。

## 22. 修复：prepared SELECT 取不到行（0.7.0 UAF 触发条件）+ 握手版本串（2026-09-13）

### 22.1 现象与误判

release 冒烟里 prepared 路径间歇故障：`1210 Incorrect number of arguments executing
prepared statement`、垃圾 stmt_id（`RESET(0)` / 随机 id）、`MySQL server has gone away`、
prepared SELECT `fetch` 为空——单次运行 0~100% 失败，被日志/代理"修好"（改变分配时序）。
此前记录的已知限制把它归因为"opensrv-mysql 0.7 没有二进制结果集编码"，**有误**：
`lib.rs:654` 的 `QueryResultWriter::new(..., is_bin = true)` 表明 COM_STMT_EXECUTE
本就走二进制行，元数据块收尾（DEPRECATE_EOF 下不写 EOF）也符合官方协议文档。

### 22.2 真因（两层）

1. **上游**：opensrv-mysql **0.7.0** 的 `PacketReader::next_async`
   （`packet_reader.rs:137-140`）：一次读缓冲带回剩余字节时
   `self.bytes = rest.to_vec()` 释放旧分配，而返回的 `Packet` 仍指向旧内存
   （上游 #66 / PR #67 修复，**仅存在于 git，crates.io 最新发布仍是 2024-02 的 0.7.0**）。
   后果：客户端把多条命令压进同一 TCP 段（libmysql 常态）→ 命令字节被错解 →
   opensrv 兜底分支回**裸 OK 包** → 客户端响应流错位（上游 #71 仍开放：
   `PacketWriter` 不公开，适配层无法自救）。
2. **我方适配器**：`PacketFramedReader` 虽"每次只交付一个包"，但交付量按
   `min(buf.len(), out.remaining())` 截断——底层一次 read 带回多条命令时，
   第二个包的字节被顺带交付，opensrv 内部缓冲仍出现剩余 → 重新踩 UAF 分支。
   **分帧必须按包边界截断，只做到①不等价于修好。**

### 22.3 修复（均在 sqlwire，不改 opensrv）

- `PacketFramedReader::poll_read`：交付量按 `queued_packet_len()`（包边界）截断，
  绝不混入下一个包的字节；探针日志移到整包交付点（消除旧实现"多包一次读时丢日志"）。
> 补充（`§49`）：仓里曾有一份 `vendor/opensrv-mysql` —— 它与 crates.io 的 0.7.0 **逐字节相同**、
> 也**从未接线**，已于 2026-09-19 删除。**上游这个 UAF 的绕过一直只在本节的 sqlwire 侧**，
> 不存在"fork 过 opensrv"这回事。

- 握手版本串单一事实源：`yuntun-sql::shim::MYSQL_VERSION`（`8.0.32-yuntun`），
  `MysqlBackend::version()` 覆写之（原先回 opensrv 默认 `5.1.10-alpha-msql-proxy`，
  与 `SHOW VARIABLES` 不一致，且 5.1.x 会让按版本分支的驱动/ORM 误判能力）。
- `on_execute` 增加"参数解码值 + 结果行数"debug 日志：区分"服务端拿到错误绑定值"
  与"服务端正确、客户端解码失败"（本次定位的关键一步）。
- 冒烟脚本：T2 [10] 从"只验证不崩"改为**真实断言**（轮询可见性后比对
  `[(2, "bob")]`）；T1 新增 [1.1] 握手版本一致性；修正错误注释。

### 22.4 验证

- mysql-connector **C 扩展**（libmysqlclient 二进制协议栈）连续 3×10 + 30 次迭代
  prepared INSERT+SELECT 全部通过（修复前同口径 10/10 失败）；纯 Python 栈 30 次通过。
- `scripts/pymysql_smoke.py` 全绿（含新的 T2 [10] 行内容断言）。
- Flight 冒烟（pyarrow_smoke / pyarrow_sqlinfo_smoke / flight_stream_smoke 50 万行）
  在当前构建全绿；standalone `kill -9` → 重启 → `resume_recovered`（30 批重提交）
  → t+0s 数据全量可见。

### 22.5 顺带发现（遗留，已记入 README §4）

1. **SQL DDL 时间精度**：`TIMESTAMP(3)` 不产生 `Timestamp(ms)` schema（一律 ns）——
   SQL 建的表无法满足 pyarrow_smoke prepared 绑定要求的 ms 精度，脚本只能跑在
   demo 种子实例上；DDL 精度透传待办。
2. **chaos 用例并行 flake**：`crash_recovery_no_data_loss` 全量并行时偶发失败
   （85s vs 单跑 1.1s，真实磁盘 I/O 竞争），单跑稳定；断言固定等待待改轮询。
3. **opensrv 依赖决策**：上游修复仅在 git（crates.io 停更于 0.7.0），后续切 git
   固定 rev 后可移除 `PacketFramedReader`；`vendor/opensrv-mysql/` 为分析用拷贝，
   是否保留待定。

### 22.6 建表能力盘点（应评审追问补做，发现并修复一个缺口）

SQL DDL 逐类型实测（pymysql → MySQL wire，`CREATE TABLE` 11 列全类型 + INSERT + 回读）：

- `TINYINT/INT/BIGINT/VARCHAR(n)/DOUBLE/BOOLEAN/DATE/TIMESTAMP/TEXT/BLOB` 全链路 ✅；
- **DECIMAL(p,s)：建表 ✅ 但 `INSERT VALUES` 报 "unsupported type"** —— VALUES 构造器
  缺 `Decimal128` 分支。已补（`crates/sql/src/sql.rs`）：
  - 字面量按**精确缩放**解析（`parse_decimal_unscaled`，不走 f64，避免 12.34 × 10^scale
    二进制舍入）；小数位超出 scale 拒绝（不静默截断）；
  - 数组必须按列声明的 precision/scale 构建（`with_precision_and_scale`；
    `Decimal128Array::from` 默认 (38,10)，会被批次类型校验拒绝——第一版踩到）；
  - 回读 `Decimal(12.34)` ✓，DataFusion 计算（`i * 100` → `Decimal(1234.00)`）✓。
- 拒绝边界复核（均明确报错而非静默）：`ALTER TABLE` / `CREATE TABLE AS SELECT` /
  `CREATE OR REPLACE` / `ARRAY<INT>` 复杂类型 ✅；
- `_BINARY ab` 字面量不支持（INSERT VALUES 仅普通字面量；BLOB 列以字符串字面量写入）；
- README §2.1「SQL 支持面」表同步补齐：DDL 类型清单、`CREATE/DROP DATABASE`、
  不支持行补 `ALTER TABLE` / CTAS / `CREATE OR REPLACE` / 复杂类型。

- 类型面补测（应评审追问）：`JSON` / `JSONB` 建列 ✅（`sql.rs` 映射 `Utf8` 文本存储）——
  实测**不校验 JSON 合法性**（`not-json-at-all` 照存）、客户端 SHOW COLUMNS 显示为 `text`、
  `->`/`->>`/`json_extract`/`from_json` 等全部明确拒绝（DataFusion 55 默认函数集无 JSON 能力，
  社区 `datafusion-functions-json` 未引入）→ JSON 仅"可存可查原文"，半结构化访问不在 0.1 范围。
  README §2.1 类型清单与不支持行已同步。

## 23. 新增：ARRAY / MAP 列 + JSON 查询函数（0.1 追加，2026-09-13）

### 23.1 落地内容

- **DDL 映射**（`crates/sql/src/sql.rs::map_column_type`）：
  - `ARRAY<T>` / `T[]` → Arrow List（元素递归映射，支持 `ARRAY<ARRAY<INT>>`）；
  - `Map(K, V)`（ClickHouse 形态）→ Arrow Map（key 不可空）；
  - **方言坑**：MySQL wire 会话用 `MySqlDialect`，sqlparser 的 MAP 关键字分支只对
    ClickHouse/Generic 开放 → `MAP(K,V)` 落为 `Custom`。`map_column_type` 识别之：
    重组为 Generic 可解析形态二次解析，复用同一映射（`map_type_map`）。
- **INSERT 字面量**：
  - `[1, 2, 3]`（`Expr::Array`，全方言可解析）；
  - `MAP("k", 1, ...)` 函数形态（`MAP {..}` 花括号字面量仅 DuckDB/Generic，
    MySQL 方言不解析）——`literal_of` 识别 `ARRAY(...)`/`MAP(...)` 函数调用，
    元素仍须为字面量（G3 无注入面）；
  - `build_column` 新增 List/Map 分支：子元素走同一构造器递归
    （签名改 `&dyn Fn`，避免递归单态化爆炸）；`Decimal128Array::from` 默认 (38,10)
    的教训同理适用于按列声明构建。
- **JSON 查询函数**：引入 `datafusion-functions-json = 0.55`（与 DF 55 一一对应），
  `session_with_schema` 注册 `register_all`。函数集：`json_get / json_get_{str,int,
  float,bool,json,array} / json_as_text / json_contains / json_length /
  json_object_keys / json_from_scalar`（**没有 `json_extract`**，0.55 已改名）。
- **wire 编码**（`crates/sqlwire/src/encode.rs`）：新增 Union 分支——`json_get`
  返回 Arrow Union（JSON 变体联合），取选中变体的底层值按文本输出（arrow 的
  Union display 会带 `{变体名=}` 包装）。List/Map 列走既有文本兜底
  （`[10, 20, 30]` / `{a: 1, b: 2}`）。

### 23.2 关键约定（踩坑留档）

1. **JSON 路径不带 `$`**：`json_get_int(doc, "n")` ✓；`"$.n"` 是 miss
   （返回 NULL/空列）；`json_length(doc)` 不带 path 所以不受影响——这让我们一度
   误判函数注册失败。miss 语义：类型不匹配/未命中返回 NULL 列
   （可能为 `DataType::Null` 或默认类型），不报错。
2. `json_get_int` 命中返回 **UInt64**，miss 可能是 Int64/Null——类型不跨行稳定。
3. 方言矩阵（Generic/MySQL 下可解析性）：`ARRAY<T>` ✓、`T[]` ✓、`Array(T)` ✗、
   `Map(K,V)` ✓、`MAP<K,V>` ✗（仅 DuckDB）、`MAP {..}` 字面量 ✗（仅 DuckDB）、
   `[..]` ✓、`STRUCT` ✗（明确拒绝）。

### 23.3 验证（端到端，MySQL wire 轨）

- `CREATE TABLE (id INT NOT NULL, tags ARRAY<INT>, props MAP(VARCHAR, INT), doc JSON)`
  + INSERT `[10,20,30]` / `MAP("a",1,"b",2)` / JSON 文本 →
  回读 `[10, 20, 30]`、`{a: 1, b: 2}`、原文 JSON ✓；
- 查询：`tags[1]` → 10、`unnest(tags)` → 3 行、`props["a"]` → 1 ✓；
- JSON：`json_get_int(doc,"n")=7`、`json_get_str(doc,"k")="v"`、
  `json_length=2`、`json_contains=1`、`json_get(doc,"k")` 经 wire Union 分支 → "v" ✓；
- **Parquet round-trip**：flush/commit 后全量回读无损，JSON 函数照常可用 ✓；
- 回归：`crates/query/tests/json_functions.rs`（路径/miss/Union 行为固化）、
  `crates/sql` 数组映射与字面量单测；`cargo test --workspace` 全绿、clippy 0 警告。
- 遗留：`STRUCT` 列、Variant（DF #16116，等生态成熟）不在 0.1。

## 24. M0a 落地：恢复语义改造（delta-dml-design §1.1 前置项，2026-09-13）

依 delta 设计五轮评审定稿的 M0a 开工，清偿"重启重写全部历史数据"的既有债务，
为 DV（行位删除向量）建立 batch_id 稳定性前提。

### 24.1 改动

1. **`BatchPendingPayload` 增补 `table`**（prost tag 10，向后兼容）→ `BatchState.table`；
   老 WAL 为空时从 `s3_paths[0]` 反解（`table_from_object_path`，R18 回退）。
2. **`wal_seq_end` 精确化（R8）**：`flush_batch` 改为 `*group.seqs.last() + 1`
   （旧 `first_seq + len` 在交错写入下少算右界）；全链路统一半开 `[start, end)`：
   - `resume_recovered` Pending 重做 `scan_range(s, e+1)` → `(s, e)`（R16 清单）；
   - `segment_batches` 相交判定改半开（`e > lo`）；
   - Pending 重做**按表过滤** payload（组内空洞属于别的表，旧行为会跨表串数据）
     —— `BatchState.table` 可用后顺手修复；
3. **重提交（R9/R12）**：`resume_recovered` 对 `Committed | S3Written` 走新增的
   `recommit_into_catalog`：复用原 batch_id 与既有对象**只重建内存 Catalog、不追加 WAL**
   （原 BatchCommitted 已表达终态）；**世代闸门** `liveness_at(start) vs liveness_at(EOF)`
   （与 Pending 分支同款）——DROP/同名重建的旧世代批次 abort，不挂新表。
4. **攒批重放跳过集 `ReplaySkip`**（组键 table/shard/window/epoch + 半开区间，
   二维判定）：`resume_recovered` 建立、`run_accumulator` 消费——认领命中的 Data
   不再重放入账（重提交已让原文件可见，重放 flush 会双份）。epoch 进组键用于区分
   DROP/重建前后同名同窗口批次（评审五 R15 的二维判定落地）。

### 24.2 验证

- 新增 `crates/ingest/tests/m0a_recommit.rs`：
  ① 终态批次重提交（committed=1/redone=0）+ **对象存储零新增** + 文件可见 + 认领集就位；
  ② 世代闸门：DROP→同名重建→重启，旧世代数据不挂新表（R9）；
  ③ 交错写入 Pending 重做按表过滤（R8）：t2 的交错行不混入 t1 批文件，row_count=2。
- `cargo test --workspace` 全绿、clippy 0 警告（顺带修掉 ARRAY/MAP 遗留的 3 处）。

### 24.3 M0b 待办（不含在本批）

`replay_wal_dml`（重建 DeletionEntry，四段顺序之 ③）、segment 清理闸门（R21）、
abort 区间保留（`apply_record` 对 BatchAbort 保留区间进跳过集，修复"显式放弃数据
重启后复活"的既有行为）。

## 25. 架构调整：chunk 层（数据平面地基）+ 与既有设计的对照审查（2026-09-15）

> 依据：`architecture.md`（架构设计 v11）、`design.md`（详细设计 v1.0）、
> `docs/architecture-with-chunk.md`（架构设计含 chunk 层，下文简称 **chunk 版架构**）、
> `docs/refactor.md`（重构路线 S0–S6）。
>
> 本节记录 **S1（chunk 层）** 的落地、**与既有设计的逐条对照结论**、
> **审查中发现并回改的偏差**，以及由此对 `plan.md` 的调整。
>
> 一句话结论：**方向正确（数据平面先行的次序判断是对的），但按 chunk 版架构 §5.3 字面实现会
> 与既有 ADR-10 冲突；已按"两者取并集"回改，该文档需补一句限定语。**

### 25.1 落地内容（对应 refactor.md 的 S1-1 ~ S1-10）

| 项 | 内容 | 落点 |
|---|---|---|
| S1-1 | 新 crate `yuntun-chunk`（`Chunk` = 尚未成型的 RowGroup；内存直接持 `RecordBatch`，**不转 RowGroup 布局**） | `crates/chunk/src/{chunk,stats}.rs` |
| S1-2 | 五态机 `Open → Sealed → Spilled → Flushed → Released`，非法转移**报错**而非静默 | `chunk.rs` |
| S1-3 | seal 策略单点：行数 / 字节 / **窗口关闭** / `schema_version` 变化 | `store.rs::SealPolicy` |
| S1-4 | spill = **本地磁盘** + Arrow IPC(LZ4) + 自描述头（`wal_segment`/`seq_range`/`crc32`/schema） | `spill.rs` |
| S1-5 | 内存账本 + 60/80/95 三级背压（spill → 强制 seal → 拒写） | `budget.rs` |
| S1-6 | 内存硬分区：chunk 区与 query 区两块独立预算（query 侧接 DataFusion 内存池） | `budget.rs`、`query/src/lib.rs` |
| S1-7 | 读侧接缝：`ChunkStore` 实现 `store::ShardReader`（查询侧零改动，"读己之写"保住） | `store.rs` |
| S1-8 | WAL 回收点语义：chunk 记录 `[seq_start, seq_end)` 半开区间 + spill 头写 WAL 引用 | `chunk.rs`、`spill.rs` |
| S1-9 | 强制 seal **且 flush** 的最大驻留（防慢写入流撑爆 WAL），取代旧 `idle_timeout` | `store.rs::plan_flush` |
| S1-10 | `rows_threshold` 10 000 → **500 000**（让 RowGroup 一次成型） | `config.rs`、`yuntun.toml.example` |
| 附带 | `FileManifest` 增 `partition_key` / `source_instance`（架构 §2.3 / §4.4） | `model/src/meta.rs` |

### 25.2 对照既有设计：逐条结论

| 维度 | 既有设计出处 | 本次做法 | 判定 |
|---|---|---|---|
| 铁律①"所有写入走 ingest" | §3.2 | chunk 只是 WAL 与对象存储之间的**驻留层**，仍无第二条写入路径 | ✅ 保持 |
| 铁律②"协议端口在 server" | §3.2 | 未动（chunk 无协议面） | ✅ 保持 |
| 依赖方向 | §3.2 | 新增 `model → store → chunk → ingest → server`，`query → store` 不变，无环 | ✅ 保持（**§3.2 图需补 chunk 一行**） |
| 读侧接缝 `ShardReader`/`ShardFetch` | §21.3（本日志） | trait 仍留在 `yuntun-store`；**实现**从 `store::MemoryShard` 下移到 `chunk::ChunkStore` | ✅ **继承**（"查询不依赖 Ingestor 进程"更强：`query` 对 `ingest`/`chunk` 均无依赖） |
| C1「WAL 是唯一事实」 | 详设 §4 | spill 头记 WAL 引用 + CRC，校验失败即判定副本不可信（丢弃重来） | ✅ **强化**（多了一个可丢弃副本，未新增权威） |
| C8「OCC 先于写 S3」 | 详设 §5.2 | 未动（仍在写对象存储之前演进 schema） | ✅ 保持 |
| ADR-3「Ingestor 互不感知 / WAL 本地独占」 | §4 | **新增一类节点私有状态：spill 目录** | ⚠️ 需在文档中显式列出（见 25.4） |
| ADR-4「batch_id 随机 UUIDv7」 | §4 | 未动（仍随机；文件名即 batch_id，全局唯一） | ✅ 保持 |
| ADR-9「SLA 分级」 | §4 | `best_effort` 的 RPO 从"≤jitter(60s)"变为"窗口关闭 + max_flush_delay" | ⚠️ 量级变化，需在 README/文档同步（见 25.4） |
| **ADR-10「整分钟对齐 + jitter」** | §4 | 首版实现**违例**（见 25.3-1），已回改为窗口对齐；jitter 锚点/量级待 S2 正式修订 | ❌→✅ **发现冲突并回改** |
| §7.2 攒批阈值 + 5min 空闲兜底 | §7.2 | `time_threshold` 降级为**地板**（seal 时刻由窗口关闭决定）；5min 兜底收紧为 `chunk_max_resident_secs`(60s，且强制 seal+flush) | ✅ 语义澄清 + 收紧（旧实现"每秒 1 条也会 flush"仍成立） |
| §8.6/8.7 热数据 = 按分钟切分的 MemTable + `UnionExec` | §8.6/8.7 | chunk 即"按时间窗口切分的 MemTable"；scan 仍 `UnionExec(热 ∪ 冷)` | ✅ **等价替换**（且补齐了内存账本/背压/spill，旧设计未覆盖） |
| 详设 §11 配置项 | §11 | 新增 `[chunk]` 段；移除 `flush_jitter_secs`（随机 → 确定性相位） | ⚠️ 破坏性配置变更（旧 TOML 仍可解析，未知键忽略） |

**结论**：本次调整属于**同一架构内的"补齐与收敛"**，不是换架构——
它把既有设计里"应该有但没写"的一层（内存热数据如何有界、如何卸载、flush 时刻如何确定）
补上，并把散在两处的分组语义收敛成一处。**没有推翻任何一条既有铁律**；
唯一真正冲突的是 ADR-10 的实现方式（且是本次首版自己引入的）。

### 25.3 审查中发现的问题（4 项已回改 + 3 项留观）

#### 1.【严重｜已回改】首版实现违反 ADR-10："创建后 N 秒"取代了窗口对齐

`architecture.md` §ADR-10 明文：

> 攒批窗口按**整分钟对齐**（**而非**"达到阈值后 5 秒"）……同时保持
> **每窗口每 shard 最多 1 个文件**的小文件控制目标。

而 S1 首版把时间维度的 seal 触发写成 `now - created_at >= time_threshold(5s)`——
正是 ADR-10 否决的那一种。后果（低吞吐表，如告警/审计：1 条/秒）：

| 方案 | 一个窗口（分钟）产出的文件数 |
|---|---|
| ADR-10 窗口对齐（旧实现 `window_start + jitter`） | 1 |
| S1 首版"创建后 5 秒" | **≤12** |

即：把 ADR-10 专门用来防的"小文件 / Meta 条目爆炸"重新引入，且**功能测试全绿**——
只有对照 ADR 原文才能发现。

**回改**：`Chunk::window_end_ms`（**到达分钟**窗口结束，与旧实现锚点同源）+ `SealPolicy::min_resident`
只作地板；新增 `SealReason::WindowClosed`。回归用例
`chunk::store::tests::time_seal_is_window_aligned_not_creation_offset`
（同窗口 5 次 append → 仍 1 个 chunk；创建后 5s 但窗口未关 → 不 seal）。

> 📌 **对 chunk 版架构的修订建议**：§5.3 写 `flush_deadline = seal_time + max_flush_delay`
> 时，必须补一句"**seal 触发是窗口对齐的**"，否则读者（包括本实现）会自然地
> 退回"创建后 N 秒"，与 ADR-10 冲突。这是该文档的**表述缺陷**，不是决策错误。

#### 2.【已修】写入侧与查询侧的表标识不一致（**被本次改造暴露的既有隐患**）

`do_put` 简易轨用**裸表名**（`cpu`）写 WAL 与内存视图，查询侧用**全限定名**
（`public.cpu`，`TableMeta::qualified_name()`）读热数据——两者永不匹配，于是
"读己之写"实际一直靠 **flush 落盘**兜底：

| 时期 | 持久化上界 | 表现 |
|---|---|---|
| 旧实现 | `window_start + jitter`（默认 ≤60s，冒烟实测 29.1s） | 掩盖了标识不一致（写后 ~30s 仍能查到） |
| S1 首版 | `seal + max_flush_delay`(30s) | 暴露为**可见性回归**：`client_e2e` 等 10s 超时（`got=Some(0)`） |

`yuntun_model::ops` 本就写明"**跨层唯一表标识**：Catalog 内部键、Ingest/WAL 的 `table`
字段、对象存储路径派生、Query 缓存键全部使用它；裸表名只在 SQL 表面与 `TableMeta.name`
出现"——实现没跟上文档。

**回改**：ingest 边界一次归一化（`qualify_table`），WAL / chunk / Manifest / 查询键同身份；
恢复路径同样归一化（老 WAL 的裸名不再被世代闸门误判）。副作用是裸名写入的对象路径
由 `yuntun/<table>/…` 变为 `yuntun/<schema>/<table>/…`，**与 `DiskShard::prefix` 的既有约定
终于一致**（此前两处对裸名的处理不同，是同一根因的另一面）。已有数据以 Manifest 的
`file_path` 为准，读取不受影响。

#### 3.【已修】spill 目录跨重启泄漏

spill 是**进程内**热数据的落盘副本，重启后 registry 为空 → 残留文件永远不会被引用，
而崩溃重启循环会让它们无限堆积。原实现只在 `release`/`discard` 时删除。

**回改**：`ChunkStore::new` 启动即 `purge_leftover_spills()`（只清 `*.ipc.lz4`/`*.tmp`）。
架构 §2.6 的"校验 WAL 引用一致则**复用**副本"是**后续优化**（需把 chunk 与 WAL 区间重新配对）：
当前选择"丢弃重来"，正确性不受影响，代价是重启后重新编码；已在代码注释与本日志留档。

#### 4.【已修】"强制 seal + flush"只做了一半

`plan_flush` 原先把"超 `max_resident`"只当作 seal 条件：chunk 被强制 seal 后，
还要再等 `max_flush_delay` 才落盘——**WAL 仍被拖住**，S1-9 的目的（防慢写入流撑爆 WAL）
没有达成。**回改**：超驻留时同一轮既 seal 又 flush（`plan_flush` 的 `due` 并入
`open_too_long`，调用方顺序 seal → spill → flush 保证可行）；用例断言"强制 seal 必须同轮 flush"。

#### 5.【已修】驻留硬兜底会静默绕过相位分散（→ 惊群复活）

`flush_due_at` 原先锚定 `sealed_at.unwrap_or(created_at)`。窗口对齐后 chunk 在**窗口关闭时**
才封口，此时"按创建时刻算的到期"早已过去 → **封口即到期** → 所有实例又回到同一秒 flush，
相位分散形同虚设。**回改**：锚点固定为 `sealed_at`（未 seal 返回 `u64::MAX`），
并明确不变量 `max_resident > max_flush_delay + phase_spread`
（后者由 `Config::warnings()` 自检并在启动时告警——这类问题**功能测试全绿**，
只能靠不变量守护）。这是 ADR-10 惊群约束在"窗口对齐 + 确定性到期"下的**新失效模式**。

#### 6.【留观】chunk 区内存可越限（有意为之，需可观测）

`append` 的记账是**无条件**的（`reserve`，可见性承诺优先于预算，I1）：
真正的硬闸门在 `ingest()` 入口的 95% 拒写。中间态（WAL 已 fsync、攒批尚未吸收完）由
**WAL 积压**吸收。风险是"吸收慢 → 内存短时超预算"，需要把
`ledger.used / WAL 积压字节 / 背压水位` 三个量纳入观测（→ refactor 的 S1-11 验收）。
**未改**：改成硬拒绝会把已 fsync 的数据退回给客户端，语义更糟。

#### 7.【按设计保留】墓碑期不跳过、多文件粒度

- 触达 95% 时**不**强制释放 `Flushed` chunk（架构 §4.6 允许"内存压力时跳过墓碑期"）：
  跳过会出现最长一个缓存刷新周期（~200ms）的可见性空洞，当前选择不跳；
- 高吞吐表由 `rows_threshold` 决定文件粒度（可 >1 文件/窗口）——这是 ADR-10
  在小文件控制上的**既有边界**（旧实现同样如此），由 compaction 兜底。

#### 8.【待定｜P2】相位分散的量级需要实测定案

窗口对齐后，低吞吐表的 flush 时刻 = `窗口关闭 + max_flush_delay + phase(≤spread)`。
ADR-10 的削峰是"60 秒内均匀分散"，而 `spread` 默认 5s → **分散面收窄 12 倍**；
若把 spread 提到 60s（= 一个窗口）则恢复 ADR-10 的分散度，但持久化上界变为
`60 + max_flush_delay + 60`（≈2 分钟，该文档允许"放宽到分钟级"）。

两者都要实测（S0 baseline 的文件数与 P99 写入延迟）才能定案，**不在本批拍板**：
`max_flush_delay_secs` 与 `flush_phase_spread_secs` 的默认值列入 P2 定案清单
（并受 25.3-5 的不变量约束）。详见 `plan.md` §2.2 / `refactor.md` 的 P2。

### 25.4 文档层面待同步（本次未改：需求是"先分析再改计划"）

1. `architecture.md` §3.2 crate 图补 `yuntun-chunk`（`model → store → chunk → ingest`）；
2. `architecture.md` §ADR-10 需正式修订（jitter 锚点：`window_start` → `seal_time`；
   量级：待 25.3-8 定案），并在 §"节点私有状态"中列明 **WAL 目录 + spill 目录**（ADR-3 边界）；
3. `architecture.md` §ADR-9 / README「已知限制」同步新的 RPO 量级
   （`best_effort`：窗口关闭 + `max_flush_delay`，不再是 ≤jitter 60s）；
4. `architecture-with-chunk.md` §5.3 补"seal 触发窗口对齐"限定语（见 25.3-1 的 📌）；
5. `design.md` §11 配置清单同步 `[chunk]` 段与移除的 `flush_jitter_secs`/`idle_timeout`；
6. `CHANGELOG.md` 不逐条记改造过程（本次曾误记后撤回）——发布时从本日志汇总。

### 25.5 验证

- `cargo test --workspace`：**189 passed / 0 failed**；`cargo clippy --workspace --all-targets` **0 警告**；
- 新增用例（按语义分组）：
  - chunk：窗口对齐 seal（同窗口 1 chunk / 创建后 5s 但窗口未关不 seal）、
    五态机非法转移报错、spill 往返 + CRC 篡改/截断/魔数拒绝、spill 后账本归零且数据仍可读、
    `commit → 缓存追上 → 回收`（I4 无空洞）、陈旧世代隐藏与回收、背压三级阶梯、
    相位偏移确定且跨实例分散、强制 seal 同轮 flush、启动清理残留 spill；
  - ingest：表标识归一化、`config → SealPolicy` 映射（含"地板必须短于一个窗口"）、
    flush e2e 驱动真实攒批循环（Manifest 出现 1 文件 + WAL 终态 Committed + 未落盘即可读）；
  - server：`[chunk]` 配置解析/水位退化、示例配置与解析器同步、
    不变量自检告警（`max_resident` 绕过相位分散 / 零预算）；
- 既有回归全绿（含 `write_then_read`（读己之写 latency + 零已提交文件、DROP 不复活）、
  `hot_shard_reader`（远端 `ShardReader` 可替换）、`m0a_recommit`、chaos 3 场景）。

### 25.6 遗留与后续（已同步到 plan.md）

> 同步位置：`plan.md §2.2`（待定决策登记表）、`plan.md §2.3`（文档同步项）、
> `plan.md §五`（分布式就绪度基线：记分卡 / 接缝现状 / 四类静默错数据）、
> `plan.md §七`（R2–R6 WBS）、`plan.md §八`（关键路径与里程碑）。

| # | 项 | 归属 |
|---|---|---|
| 1 | 相位分散量级 + `max_flush_delay` 默认值实测定案（25.3-8） | P0（阶段 2 首轮） |
| 2 | spill 复用（校验 WAL 引用一致则沿用副本，替代"丢弃重来"） | S1 收尾 / P2 |
| 3 | 三个内存/积压指标接入观测（25.3-6） | S1-11（阶段 2 T6.12） |
| 4 | `architecture.md` 的 ADR-10 / §3.2 / 节点私有状态修订（25.4） | 阶段 2 末（阶段 3 准入） |
| 5 | chunk 级 ZoneMap 剪枝接入 query（`ColumnStats` 已就绪，尚未用于跳过） | R5（T13.2） |
| 6 | query 区内存池的**可观测性**（当前只在上限处失败，无水位指标） | S1-11（阶段 2 T6.12） |
| 7 | **规划缺口（本轮核查新发现）**：`Compactor` / 孤儿清理绑具体 `MemoryCatalog`（R2 必补 `CommitCompaction`）；`source_instance` 只写不读（R4）；孤儿 GC 非多写者安全（R6） | `plan.md §5.1`/`§7` |

## 26. R2：Catalog 访问形态改造 + 观测能力（单进程 standalone 形态，2026-09-17）

> 承接 §25 与 `plan.md §五`（就绪度基线）。本轮只做**不需要拆进程**的部分：
> 控制平面的**形态与接缝**（refactor S2-1 ~ S2-8）+ 观测（T6.12），
> 不碰 raft / 进程拆分 / fanout / 全局 compaction。
>
> 一句话：**控制平面的接口已经按"远程形态"定义好了，现在还是同进程实现；
> 换成 gRPC 客户端时业务代码一行不改。**

### 26.1 落地内容

| 项 | 内容 | 落点 |
|---|---|---|
| **S2-5 版本分两组** | `CatalogVersion { schema_ver, manifest_ver }`；DDL 推 schema、flush/compaction 推 manifest | `model/ops.rs`、`catalog/lib.rs` |
| **S2-7 增量接口** | `manifest_delta(since)` → 变更表清单；实现用"每表最后变更版本"（O(表数)，无界日志） | `catalog/lib.rs` |
| **S2-4 每查询一次快照** | `CatalogSnapshot`（不可变 + `Arc`），provider/scan 全程复用同一份；**不再每次 `table()` 深拷贝文件清单** | `query/src/{cache,provider,table}.rs` |
| **S2-1/2/6 本地物化视图** | `LocalCatalogCache` → `LocalCatalog`：版本驱动 + 增量刷新 + "无变化零开销" + 失败保留旧快照 | `query/src/cache.rs` |
| **S2-3/R2 抽象补位（T10.7）** | `commit_compaction` / `known_batch_ids` **上 trait**；`Compactor` / 孤儿清理改依赖 `Arc<dyn CatalogOps>` | `catalog/lib.rs`、`compaction/lib.rs` |
| **T6.12 观测** | `Lakehouse::metrics()` + 周期结构化打点：chunk 内存水位 / WAL 积压 / 背压水位 + Catalog 版本与增量统计 | `server/src/lib.rs`、`ingest/pipeline.rs` |
| **S2-4 附带** | 节点列表进快照（`snapshot.nodes`，standalone = 单节点）——"分片归属"必须与 schema/manifest 同版本 | `query/src/cache.rs`、`server/src/lib.rs` |

### 26.2 关键设计点（为什么这样做）

1. **不可变快照不是"性能优化"，是正确性要求。**
   DataFusion 的 `CatalogProvider`/`SchemaProvider`/`TableProvider` 是**同步 trait**，规划期
   会被反复调用；若每次都读一个会变的结构，**同一次查询的 plan 与 scan 可能看到两个版本**
   （漏读/幻读）。改为"构建新快照 → 原子替换"后，读侧只拿 `Arc`。
   `CatalogSnapshot.tables` 用 `HashMap<String, Arc<CachedTable>>`：写时复制的代价与
   **文件数无关**（否则每次增量刷新都要克隆全表文件清单，增量就白做了）。

2. **版本分两组是"让增量成为可能"的前提。**
   flush 是最高频的写。若与 schema 共用版本号，**每次 flush 都会让全表 schema 失效**
   → 缓存退化为全量重建。分开后：DDL（低频）→ 全量；写入（高频）→ 只拉变化的表。
   回归证据：`full_reload_then_incremental_touches_only_changed_table` 用 `Arc::ptr_eq`
   断言"无关表**没有被触碰**"—— 而不是"结果恰好一样"。

3. **增量接口必须能报"消失的表"。**
   删表若只靠 schema_ver 触发全量，一旦调用方漏判就会**留着过期缓存**。
   因此删表同时推两组版本，且增量把被删表也报出来（`drop_table_advances_both_versions`）。

4. **`commit_compaction` / `known_batch_ids` 必须在 trait 上。**
   它们此前是 `MemoryCatalog` 的固有方法，导致 `Compactor.catalog: Arc<MemoryCatalog>` ——
   "同进程"是部署事实，但**不能因此把类型写死**：R3 把 Catalog 换成 gRPC 客户端时编译不过
   （`plan.md §5.1-B` 的本轮实测）。这是"单机可跑、分布式不返工"最便宜的一处接缝。

5. **观测的三项是"故障现场三件套"。**
   内存水位 / WAL 积压 / 背压水位：没有它们，压力类故障只能复现不能定位。
   尤其 **WAL 积压**——chunk 记账是无条件的（可见性优先于预算），越限部分正是靠它吸收；
   它也是唯一能解释"内存为什么超预算"的量。打点内容可 `serde_json` 序列化（将来接 HTTP/监控）。

### 26.3 本轮暴露/发现的问题

1. **`wal_backlog` 的口径极易写错（差一即误导运维）**：
   实现时第一版把 `synced_seq` 当成"下一条待写 seq"，报出多 1 的积压 ——
   而这**不会让任何功能测试变红**，只在运维判断上出错。
   已用契约化的单测钉住（`wal_backlog_counts_fsynced_but_unabsorbed_records`：
   写入 2 条未吸收 → 积压 = 2；吸收后 → 0），并在代码注释里写明半开区间公式。
   *教训*：**指标也是接口**，必须像接口一样有契约测试。
2. **刷新失败语义必须显式化**：刷新失败**保留旧快照**（宁可读稍旧数据，也不要查询不可用），
   错误记入 `last_error` 供告警（`failed_refresh_keeps_previous_snapshot_and_records_error`）。
   这条在单进程时看不出价值，但在 R3（metanode 偶发不可用）时是可用性底线。
3. **chaos 并行 flake 根因确认**：`cargo test` 默认**并行跑所有 test binary**，
   `yuntun-chaos` 与另外 46 个 binary 争 CPU/IO；实测并行下 30s 恢复上限不足
   （单跑 1.3s / 3 passed）。本轮把该上限放宽到 60s 并写明理由
   ——**正确性断言不用超时冒充延迟指标**；根治（chaos 独立跑 / 去 flaky）属 `plan.md` T6.1。

### 26.4 与既有设计的对照

| 维度 | 出处 | 本轮做法 | 判定 |
|---|---|---|---|
| **C7「Query 无网络」** | 详设 §6.7 / ADR-6 | 读路径仍全部走本地快照（同步、无锁、无网络）；`LocalCatalog` 只依赖 `CatalogOps` trait | ✅ 保持 |
| **ADR-6「本地缓存 + 变更通知」** | §4 | 由**TTL 轮询**演进为**版本驱动 + 增量 + watch 形态**（`version()` 无变化零开销返回 = 带版本号请求） | ✅ 演进（方向与 ADR-6 一致） |
| S2-4「每查询一次预取」 | `refactor.md` | `CatalogSnapshot` 每会话取一次 | ✅ 达成 |
| S2-5「版本号分两组」 | `refactor.md` | `CatalogVersion{schema_ver, manifest_ver}` | ✅ 达成 |
| S2-7「manifest delta」 | `refactor.md` | `manifest_delta(since)` + `full_reload_required` 保守回退 | ✅ 达成 |
| T6.12「三项指标」 | `plan.md` | metrics + 周期日志（HTTP 导出未做，见 26.6） | ✅ 基本达成 |
| C5「Catalog 仅存内存」 | 详设 §6.1 | 未动（内存 + 由 WAL 重放 DDL 重建；raft snapshot 属 R3） | ✅ 保持 |

### 26.5 验证

- `cargo test --workspace`：**200 passed / 0 failed**（本轮新增 11 个用例）；
  `cargo clippy --workspace --all-targets` **0 警告**。
- 新增用例：
  - `catalog`（4）：版本分组（建表/提交文件各推哪一组）、增量只报变更表、
    删表同时推两组版本、compaction 推新旧两表的 manifest_ver；
  - `query/tests/catalog_snapshot.rs`（5）：全量→增量只碰变更表（`Arc::ptr_eq` 硬证据）、
    DDL 强制全量、**快照跨刷新不可变**、无变化零开销、**刷新失败保留旧快照并记错**；
  - `ingest`（1）：`wal_backlog` 半开区间口径；
  - `server/tests/metrics_e2e.rs`（1）：三项指标 + Catalog 版本 + query 区上限 + 可序列化。
- 既有回归全绿（含 chaos 3 场景：单跑 1.3s）。

### 26.6 遗留（对应 plan.md 相应条目）

| # | 项 | 归属 | 说明 |
|---|---|---|---|
| 1 | proto 启用 tonic-build（T10.8） | R3 前 | 与 raft 服务是同一批 codegen 工作，单独做没有收益 |
| 2 | 指标 HTTP 导出 | 阶段 2 尾 | 需要新增依赖（当前离线环境不可加），先以结构化日志交付 |
| 3 | chaos 与其余 binary 并行导致的 flake 根治 | T6.1 | 建议 CI 分两步：`cargo test --workspace --exclude yuntun-chaos` + `cargo test -p yuntun-chaos` |
| 4 | `cache_ttl_secs` 降级为纯兜底（S2-10） | 阶段 2 | 版本驱动已上线，TTL 目前仍作为兜底保留 |
| 5 | 本地缓存持久化（S2-8） | R3 后 | metanode 不可用时降级服务，需要序列化格式 |

---

## 27. 阶段 2 起步：chaos 场景 #5 抓出一个真 bug —— 幂等键在写入路径上不生效（2026-09-17）

**触发**：着手补 `design.md` §12.3 的 chaos 场景 #5（幂等键 + Compaction）时，
先确认"同键重试会被去重"这一前提是否成立 —— 结果**不成立**。
场景写不出来，因为被测功能没实现（文档与 README 都认为它已实现）。

### 27.1 证据（修改前的三处代码）

| 位置 | 代码 | 后果 |
|---|---|---|
| `ingest/src/pipeline.rs` | `let _client_key = resolve_idempotency(...)` | 解析出的键**被丢弃**（下划线），只留下"强制表必须传键"的校验 |
| `ingest/src/flush.rs` | `commit_files{ client_request_id: None }` | Meta 层唯一索引去重（`commit_files` 里那一段）**永不触发** |
| 全仓 | `check_idempotency` 无任何业务调用方 | 入口预筛不存在 |

净效果：**同一幂等键提交两次 = 两份数据**。客户端超时重试即命中，且完全静默。

**为什么既有测试全绿**：`sql_idempotency_e2e` 系列断言的是"键被提取/强制/透传"
（回执行数、`require` 表拒绝无键请求），**没有一条断言"同键重试不产生重复"**。
这正是 `plan.md §5.3` 说的那类"不报错、只产出错数据"的缺口 ——
功能测试绿 ≠ 语义正确。

### 27.2 根因有两层（第二层是修完第一层才暴露的）

**第一层：键根本没被使用**（见 27.1）。

**第二层：键的粒度错配。** 幂等键标识的是**一次客户端请求**
（一个 DoPut 流 / 一条语句），而一次请求可以产出**多个批次**：
- 简易轨 `do_put`：多个 `FlightData` 消息；
- FlightSQL prepared 装载：同上；
- SQL `INSERT ... SELECT`：多结果批（`SqlEngine::ingest_batches` 逐批 ingest）。

这些生产点把**同一个请求级键**发给了每一个批次。第一层修好之后，
批次级预筛会把同流的第 2..N 批当成"第 1 批的重试"而**判重丢掉** ——
从"静默重复"变成"静默丢数据"。
（`client_sdk_end_to_end` 先炸出来：一次 `insert` 2 个批次，回执行数从 2 变 1。）

### 27.3 修复

**语义契约（本次定下）**：

> 幂等键标识**一次客户端请求**。请求内第 `idx` 个批次的键 = `derive_batch_key(请求键, idx)`
> （形如 `req-1#0`）。派生是确定的，因此幂等是**逐批**成立的 ——
> 上次只成功了一部分时，重试**只补缺失的批次**，而不是"整流跳过"。

| # | 改动 | 位置 |
|---|---|---|
| 1 | 新增 `derive_batch_key(request_key, idx)`（长度受 256 约束，截断主体保留后缀） | `ingest/src/lib.rs` |
| 2 | 入口预筛：命中即返回 `duplicate = true`、`wal_seq = 0`、`row_count = 0`，**不写 WAL** | `ingest/src/pipeline.rs` |
| 3 | WAL fsync 成功后登记键（防**并发**同键请求双双通过预筛）；`batch_id` 留空 = "已认领、批次未落盘" | 同上 |
| 4 | 恢复时从 WAL Data 记录**重建键索引**（`MemoryCatalog` 重启即空），TTL 以重启时刻起算（保守方向） | 同上 |
| 5 | 三个多批次生产点改用派生键 | `server/src/flight.rs`（简易轨 + prepared 装载）、`sql/src/lib.rs`（`INSERT ... SELECT`） |
| 6 | `Receipt.duplicate` 透出到 SDK（`InsertReceipt`）与 CLI（"其中 N 个批次被幂等去重"，且 `inserted` 改报**实际写入行数**而非输入行数） | `client/` |

**为什么预筛必须在写 WAL 之前**：一个 chunk 会聚合多条 Data 记录、各带自己的键，
而 `commit_files` 只接受**单个** `client_request_id` —— 填任何一个都是错的
（会把别的键的数据标记成那个键的批次）。所以"提交时按键集合去重"
要等 R3 状态机（`refactor.md` S3-5），当前唯一正确的拦点是入口。
这一点已写进 `flush.rs` 的代码注释，避免后来者"顺手补上"。

### 27.4 验证

- `cargo test --workspace`：**203 passed / 0 failed**；`clippy --all-targets` 0 警告。
- **反证（防止写出空测试）**：把入口预筛条件临时改为 `false`
  → `idempotency_survives_compaction` 立即失败在"同键重试必须被判重"。已恢复。
- `client_sdk_end_to_end`（既有用例）在第二层修好之前是红的 ——
  即"多批次共用一键"这条路径本来就有既有测试兜着，改动没有绕过它。
- 新增用例：
  - `chaos::idempotency_survives_compaction`：3 键 3 文件 → 合并成 1 文件（旧文件
    `deleted_at`、旧快照仍见 3 个）→ 同键重试仍判重 → 总行数恒为 9；
  - `client::replayed_request_is_deduplicated_batch_by_batch`：一次请求 2 批次
    全部落库 + 整条请求重试**一行不加**；
  - `ingest::batch_key_derivation_is_deterministic_and_distinct`（含 256 长度边界）。

### 27.5 遗留

| # | 项 | 归属 | 说明 |
|---|---|---|---|
| 1 | 提交层按"键集合"去重 | R3 | 需状态机支持一次提交多个键（`refactor.md` S3-5）；当前由入口预筛唯一兜底 |
| 2 | 幂等键索引的独立持久化（fjall） | R3 | 现状靠 WAL 重建（单机正确）；跨进程共享需随 Catalog 进 metanode |
| 3 | chaos 场景 #1/#2/#3/#4/#7/#8/#9/#10/#11 | 阶段 2 | 进度表见 `crates/chaos/src/lib.rs` 模块文档（区分"单测层有"与"chaos 层有"） |

---

## 28. 阶段 2 续：chaos #1/#2/#3 落地 —— 夹具保真度修复，抓出两个重复计数缺陷（2026-09-18）

### 28.0 本轮落地

| # | 场景 | 用例 | 结果 |
|---|---|---|---|
| 1 | Compaction 期间查询 | `compaction_during_query_keeps_counts_monotonic` | ✅ |
| 2 | 分片移除期间查询 | `shard_removal_during_query_filters_by_deleted_at` | ✅ |
| 3 | 孤儿清理不误删 | `orphan_cleanup_spares_known_and_inflight_files` | ✅（含"静置期内的在途文件绝不删"防线） |

生产代码改动只有一处（加法式）：`compaction::spawn_orphan_cleanup_with_interval`——
原来轮询间隔硬编码 60s，导致"不误删"这条用例要跑一分钟以上，
**没人会跑的用例等于没有防线**；新函数保留原签名（包装器传 60s）。

`#2` 顺带覆盖了 R2 的增量刷新：`drop_shard` 推 `manifest_ver`，
查询侧必须靠增量 delta 把"文件消失"消费掉，否则会继续读已移除的分片。

### 28.1 发现 A：提交 → `mark_committed` 窗口内**重复计数**（已确认，未修）

**机制**：`Chunk::visible` 对 `committed_snapshot = None` 返回 `true`（"还没提交 → 对所有快照可见"）。
而 flush 的 `commit_files`（产生快照 S，文件 `valid_from = S`）到调用方
`chunks.mark_committed(id, S)` 之间隔了一次 **WAL fsync**（`BatchCommitted` 的 append）。
这段时间里，快照 ≥ S 的查询会**同时**读到"已提交文件"与"热数据"。

**确定性复现**（不靠并发去撞）：用 `Ingestor::flush_now` 走完 `commit_files` 但不 `mark_committed`，
把这个窗口固定下来：

```
crates/chaos: commit_to_mark_window_must_not_double_count
  文件在快照 S 可见（3 行） 且 chunks.read_table_sync(table, S) 仍返回 3 行
  → 端到端查询实测 6 行（应为 3）
```

**症状特征**：窗口 ≈ 0.1–5ms（一跳 fsync），所以表现为**偶发多计**而非稳定错误。
本轮实测到的污染：`query_multi_version_alignment`（4 → 6）、
`idempotency_survives_compaction`（9 → 12）、并发采样序列 `[..., 21, 18, ...]`。
**推论：任何"精确行数"断言在 flush 在飞时都可能偶发失败** —— 这类 flake 过去会被误判为
"测试不稳定"，实际是被测系统的真实缺陷。

**为什么不在本轮打补丁**：查过的三条路都不成立或不可靠 ——
① 把 `mark_committed` 提到 `commit_files` 紧后面：窗口从"一跳 fsync"缩到几条指令，**仍然存在**，
   把"偶发"变成"极偶发"反而更难查；
② 用锁把 [commit, mark] 与"快照钉取"串起来：本地形态可行，但 **R3 换成远端 Catalog 后
   commit 与本地 mark 天然非原子**，窗口必然回来 —— 不能把本地锁当成终局方案；
③ 用粗粒度水位（"q > 预提交快照就隐藏热数据"）：并发 flush（A 在 S_A=6、B 在 S_B=7）时，
   q=6 会看到 A 的文件但看不到 B 的数据 → **可见性空洞**（比重复更糟）。

**正确方向（读侧栅栏）**：热数据是否可见，应由"该快照的 Manifest 里是否已含这批数据"决定，
而不是由 chunk 的本地标志决定 —— 即把接缝从 `read_table(table, snapshot)` 演进为
"带上调用方已知的文件/batch 集合"，与 R4/R5 的 `pull(table, range, known_manifest_ver)`
（`refactor.md` S5-4）**同向**。建议与 R3 的 Catalog gRPC 一起设计，避免做两遍。

**测试状态**：`commit_to_mark_window_must_not_double_count` 已就位并标 `#[ignore]`（一键复现）；
其余用例在精确断言前统一调用 `wait_hot_drained()`（等热数据退场，把瞬时窗口排除掉）。

> **更新（`§99`，2026-09-24）**：**读侧栅栏已落地** —— `batch_id` 在 `commit_files` 之前登记到 chunk，
> 热读接缝带上"调用方已知的 batch 集合"（本地与远端都过滤）。本条**已修复**：探针取消 `#[ignore]` 并转绿，
> `compaction_during_query_keeps_counts_monotonic` 的松弛量（`+9`）也已收紧为 0。

### 28.2 发现 B：崩溃恢复产出**重复文件**（已定位并修复）

**症状**：夹具接上热数据读侧后（见 28.3），`crash_recovery_no_data_loss` **稳定失败**：
9 个批次（27 行）在恢复后变成 **11 个文件（33 行）**，且行数不会回落（持久重复，非瞬时）。
`files=33 / hot=0` → 重复落在 **Manifest 层**，与 28.1 的瞬时窗口无关。

**先说被否掉的假设（留档，避免下次重走）**：最初的诊断只看到
`claims=["0..1","1..2","2..3","2..3","5..6","5..6"]` 与 `data_seqs=[0,1,2,5,8,9]`，
于是怀疑"认领集漏覆盖 + 区间重复"出在 `BatchState.wal_seq_range`。
**这个方向是错的** —— 加探针打印每次 flush 的"声明区间 vs 实际读到的 seq"后，真相是：

```
[flush] reuse=false range=16..17 seqs=[16] rows=3     ← 同一条 Data 被 flush 了两次
[flush] reuse=false range=16..17 seqs=[16] rows=3     ← 两个不同 batch_id
```

**区间自始至终是正确的**（`range == seqs`）；症状是**同一条 Data 被吸收并落了两次盘**。

**根因：轮次之间换了消费者，而同一份 WAL 目录被两个循环消费。**

用例每轮 `drop(setup)` 模拟崩溃、再 `build` 重建；上一轮结束时只做
`shutdown.cancel()` —— **发信号，不 await**。而 cancel **不会打断已在执行的一轮**
（本轮含对象存储写 + WAL fsync，可观耗时）。下一轮随即在同一份 WAL 目录上起了
**新的攒批循环**：`last_read` 从 0 重新扫描，看到上一轮尚未收尾、以及本轮新写入的
Data，于是把它**再吸收一次**并各自 flush（新 chunk → 新 batch_id → 新文件）。
一条 `Data` → 两个文件，行数就此翻倍，而且**持久**（两个文件都进了 Manifest）。

**为什么以前没被发现**：① 夹具没接热数据读侧（28.3），系统性少测一条路径；
② 触发条件是与时序赛跑 —— 两轮之间的重叠窗口很小，接上热数据读侧后轮询变慢，
重叠概率显著上升，于是从"偶发"变成"稳定复现"。

**修复（两条，缺一不可）**：

| # | 层 | 改动 | 作用 |
|---|---|---|---|
| 1 | 夹具 | `cancel()` 后 **`await` 到循环真正退出** 再进入下一轮 | 消除"两个消费者"这个前提 |
| 2 | 生产 | `run_accumulator` 增**退出闸门**：`select!` 之后、动手之前再查一次 `is_cancelled()`，已置位则本轮不做 | cancel 与 tick 同时就绪时 `select!` 可能选到 tick，闸门把"退出瞬间仍落盘一批"的窗口关掉 |

**一般化（值得记住的一条）**：**节点私有状态（WAL 目录）同一时刻只能有一个消费者**。
恢复 / 交接 / 滚动重启流程最容易违反它，而症状是**静默重复**而非报错。
R4 拆进程时必须有显式约束（启动即拒绝重复 `instance_id`，`plan.md` T12.5 已登记）。

**测试状态**：用例已**取消 `#[ignore]` 并转绿**（连跑 3 次稳定）；
诊断打印改为 `YUNTUN_CHAOS_TRACE=1` 开关，不在 CI 刷屏。

### 28.3 夹具保真度修复：chaos 必须接热数据读侧

`Setup` 此前用 `Ingestor::new(...)` 自建 chunk store，而查询侧**没有** `set_hot_shards` ——
于是 chaos 里的"读己之写"根本没接线。这与 `Lakehouse::build_with_shutdown` 的装配不一致：
**夹具比生产少接一条线，等于系统性地少测一条路径**（28.2 就是这么被掩盖的）。

现在改为显式构造 `ChunkStore` + `Ingestor::with_chunks` + `cache.set_hot_shards(chunks)`，
与生产装配一致。副作用是立刻暴露了 28.2 —— 这正是夹具的价值。

### 28.4 测试侧沉淀（两条规则）

1. **断言"最终行数"前先 `wait_hot_drained()`**（等所有 chunk 提交并被 reclaim）：
   否则断言的是瞬时中间态，会把"系统缺陷"误报成"用例不稳定"。
2. **已知缺陷标 `#[ignore]` + 保留一键复现的确定性探针**，不写"当前行为"的断言
   （那等于把缺陷固化成规格），也不静默删掉用例。

### 28.5 验证与本轮遗留

- `cargo test --workspace`：**206 passed / 0 failed**（连跑多轮，含满负载并行）；
  `clippy --all-targets` 0 警告；chaos 单包 7 passed / 1 ignored（仅剩 28.1）。
- 遗留：

| # | 项 | 归属 | 说明 |
|---|---|---|---|
| 1 | ~~修复 28.2（恢复产出重复文件）~~ | ✅ **已修** | 夹具"等循环退出" + 生产"退出闸门"，见 28.2 表；用例已取消 `#[ignore]` 并转绿 |
| 2 | 修复 28.1（读侧栅栏） | 与 R3 同批设计 | 接缝演进方向见 28.1；单独打补丁会做两遍 |
| 3 | chaos #4/#7/#8/#9/#10/#11 | 阶段 2 | 进度表见 `crates/chaos` 模块文档 |
| 4 | R4 前的显式约束：**同一 WAL 目录只允许一个消费者** | R4（T12.5） | 28.2 的教训；启动即拒绝重复 `instance_id` |

---

---

## 29. chaos #4/#7/#9 落地 —— 发现：WAL 撕裂后**无法自愈**（新写入永不可见）

### 29.0 本轮落地

| # | 场景 | 用例 | 结果 |
|---|---|---|---|
| 4 | Schema 变更 + 谓词下推 | `schema_evolution_keeps_predicate_pushed_down` | ✅（但**未达设计期望**，见 29.2） |
| 7 | WAL 撕裂 | `torn_wal_tail_is_rejected_and_prefix_survives` | ⚠️ **抓到缺陷**（见 29.1） |
| 9 | `synced_seq` 边界 | `accumulator_never_reads_beyond_synced_seq` | ✅ |

生产代码本轮**零改动**（三个场景都是检验既有行为）。

### 29.1 缺陷：WAL 撕裂后无法自愈 → **写入全部成功、数据全部消失**（已修）

**实测**：把最新 segment 的尾部截掉 10 字节（模拟掉电撕裂），重启后：
- 恢复本身成功（CRC 拦下撕裂，不报错）✅
- 截断点之前的记录能解出来（诊断：`segs=["1:2679B"]`、`records=["0:Data","1:Data"]`）
- 但**恢复出的行数是 0**；
- 更严重的是：**再写入一批（fsync 成功、拿到 ack）后，该数据 60 秒轮询仍不可见**。

**根因**（代码核实，非推测）：

| 环节 | 行为 | 问题 |
|---|---|---|
| `SegmentWriter::open_append` | `bytes_written = f.metadata()?.len()` | 以**文件物理长度**作为追加偏移 → 新记录被写在**撕裂的垃圾字节之后** |
| `decode_segment_records` | 遇到 CRC 失败/length 越界即**停止 replay** | 读不到垃圾之后的任何记录 |
| 全仓 | `crates/wal/src/*` **没有任何 `set_len`/truncate 修复** | 撕裂点永远不会被清理 |

净效果：节点表现为"**写入全部成功、数据全部消失**" ——
恰恰是 WAL 本该防止的那类故障，而且**完全静默**（ack 正常、无错误日志）。

**为什么值得排在很前面**：撕裂写入是 WAL 存在的理由（`§4.9` / C3）。
现在"检测到了撕裂"却"不能恢复使用"，等于把最需要自愈的场景做成了不可用状态。

**已修**（R-14）：接管 WAL 目录时先把每个 segment **截断到最后一条完整记录的边界**
（WAL 标准修复步骤），再让 writer 从该偏移继续追加。三处改动：

| # | 位置 | 改动 |
|---|---|---|
| 1 | `wal/src/segment.rs` | 抽出 `decode_with_stop`（多返回"停止偏移 = 撕裂记录起始字节"），`decode_segment_records` 保持原签名不变；新增 `repair_torn_tail(path)`：无撕裂返回 `Ok(None)` 不动文件，有撕裂则 `set_len(stop)` + **fsync**（修复本身也要落盘） |
| 2 | `wal/src/writer.rs` | `WalWriter::open` **在 `open_active_segment` 之前**调用 `repair_torn_tails`（顺序关键：追加偏移取自文件物理长度）。修复覆盖**全部** segment，不只活跃的 |
| 3 | 测试 | `wal::writer::open_repairs_torn_tail_so_new_appends_are_readable`（最小回归：8 条 → 撕尾 → 接管 → 文件长度回到 7 条边界、`synced_seq == 6`、**新 append 的 seq 7 必须可被 `scan_from` 读到**、不再报告 torn）；chaos 场景去掉 `#[ignore]` 并恢复原设计断言 |

**验证**：`cargo test --workspace` **210 passed / 0 failed**（连跑两轮）；
chaos 单包 **10 passed / 1 ignored**（只剩 §28.1）；`clippy` 0 警告。
撕裂场景现在同时满足：前缀全恢复 + 不产生半个批次 + **恢复后能继续写入且可见**。

### 29.2 差距：谓词下推只到 `Inexact`，`FilterExec` 未被消除

`design.md` §12.3 #4 的期望是"**FilterExec 被消除**"。实测物理计划仍有
`FilterExec: event_time@0 > 150`；逻辑计划显示 `TableScan: ... partial_filters=[...]`
—— 说明谓词**确实下推到了扫描**（`partial_filters`），但表 provider 声明的是 `Inexact`：

```rust
// crates/query/src/table.rs:171
// MVP：谓词交给 Parquet row-group 统计在 scan 内部处理，表级先声明 Inexact —— 正确且保守。
Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
```

**⚠️ 不能直接把 `Inexact` 改成 `Exact`**：那会让 DataFusion 撤掉 `FilterExec`，
而 `YuntunTableProvider::scan` **忽略 `_filters`**（不转发给 Parquet 源）
→ **静默漏过滤**（结果多出行）。正确顺序是：先让 `scan` 把谓词转发给 Parquet 源
（由 Parquet 做行级过滤），再声明 `Exact`。

用例只断言"谓词确实下推"（不断言 `FilterExec` 存在与否），
因此**未来修好后无需改用例**。登记为后续项（与 R5 的块级剪枝/谓词下推同批）。

### 29.3 验证与遗留

- `cargo test --workspace`：**208 passed / 0 failed**；`clippy --all-targets` 0 警告；
  chaos 单包 9 passed / **2 ignored**（§28.1 提交窗口、§29.1 撕裂自愈）。
- 遗留（优先级）：

| # | 项 | 归属 | 说明 |
|---|---|---|---|
| 1 | ~~修复 §29.1（撕裂后截断修复）~~ | ✅ **已修** | `WalWriter::open` 接管时 `set_len` 到最后一条完整记录边界（三处改动见 29.1）；用例已取消 `#[ignore]` 并转绿 |
| 2 | 修复 §28.1（读侧栅栏） | 与 R3 同批 | 见 28.1 |
| 3 | 谓词下推转 `Exact`（§29.2） | 与 R5 同批 | **必须先转发 filters 到 Parquet 源**，否则静默漏过滤 |
| 4 | chaos #8（fsync 前/中/后 kill） | 阶段 2 | 需要 `WalWriter` 的 fsync 注入点 |
| 5 | chaos #10/#11（Batch 超时 / 磁盘水位） | 阶段 2 | 需要"S3 不可用"与"磁盘水位"的注入夹具 |

---

---

## 30. chaos #10/#11 落地 —— 发现：监控线程 abort 后**不同步视图**，segment 永不释放

### 30.0 本轮落地

| # | 场景 | 用例 | 结果 |
|---|---|---|---|
| 10 | Batch 超时 + segment 释放（S3 不可用） | `batch_timeout_releases_segment_after_object_store_failure` | ✅ |
| 11 | 磁盘水位强制 abort 最老批次 | `disk_watermark_aborts_oldest_batch_then_releases_segments` | ✅ |

**夹具新增**（都在测试侧）：

1. `build_full(...)`：可覆盖 `WalConfig` —— 水位/超时场景必须能压小
   `segment_max_size`（逼出轮转，否则无从验证"释放"）与 `monitor_interval`
   （否则一轮要等 60s）；
2. **"S3 永久不可用"用权限实现**：把 store 根目录 `chmod 0o555`，
   不用给 `object_store` 写整套失败替身。⚠️ 以 root 运行时 chmod 不生效 ——
   因此用例里有"**必须产生非终态批次**"的前置断言，夹具失效会立即报错，
   而不是让用例悄悄变成空测试。

### 30.1 缺陷：abort 只写 WAL，不同步视图 → segment 只增不减 + BatchAbort 重复写

`spawn_timeout_monitor` 在两条路径上写 `BatchAbort`（批次超时 / 磁盘水位）后
**只 append WAL，不通知 `BatchStateView`**。而随后的 segment 清理依赖
`view.non_terminal()` 给出的 `wal_seq_range` 判断"有没有批次还引用这个 segment"：

```rust
for st in view.non_terminal() { if st.is_timed_out(..) { wal.append(BatchAbort) } }   // ① 写 WAL
let non_terminal = view.non_terminal();                                             // ② 仍含刚 abort 的批次
let ranges = non_terminal.iter().map(|s| s.wal_seq_range);                           // ③ 区间仍在
if segment_is_removable(.., &ranges) { remove_file(..) }                             // ④ 永远不删
```

两个后果：
1. **segment 永不回收** —— 与 §5.3.6.1 期望的"abort 后 segment 立刻可回收"相反，
   WAL 磁盘只增不减；批次明明已被放弃，它的 segment 却一直留着；
2. **每轮重复写 `BatchAbort`** —— 视图永远非空 → 下一个 tick 又把同一条 abort 写一遍
   （生产 `monitor_interval` 是 60s，即每分钟一条无谓记录）。

**修法**：给 `BatchStateView` 加一个默认空实现的 `note_abort(&self, batch_id)`，
监控线程在 append 成功后调用；`LiveBatchTracker` 的实现就是
`observe(&Record::BatchAbort{..})` —— 与 WAL 同一份语义（C4：Abort 是终态、状态被移除），
不是"另设一套内存标记"。默认空实现保证只读视图（测试桩等）无需改动。

**两条路径都要修**：批次超时（①）与磁盘水位（②）各有一处 append。

### 30.2 反证（确认用例不是空测试）

把 `view.note_abort(..)` 临时撤掉 → `batch_timeout_releases_segment_after_object_store_failure`
**立即失败**在"批次未被 abort"（视图永远非空，60s 超时），且失败点比"segment 未回收"更早
—— 恰好说明"视图与 WAL 不一致"是最先炸出来的症状。已恢复。

### 30.3 验证与遗留

- `cargo test --workspace`：**212 passed / 0 failed**；`clippy --all-targets` 0 警告；
  chaos 单包 **12 passed / 1 ignored**（只剩 §28.1）。
- chaos 进度：**10/11**（`design.md` §12.3 的 11 个场景里只剩 #8）。
- 遗留：

| # | 项 | 归属 | 说明 |
|---|---|---|---|
| 1 | chaos #8（并发写 + fsync 前/中/后 kill） | 阶段 2 | 需要给 `WalWriter` 加 **fsync 注入点**（唯一的场景，必须动生产代码才可测） |
| 2 | 修复 §28.1（读侧栅栏） | 与 R3 同批 | 见 28.1 |
| 3 | 谓词下推转 `Exact`（§29.2） | 与 R5 同批 | 必须先转发 filters 到 Parquet 源 |
| 4 | T8 基线压测 + P0 定案 | 阶段 2 | chaos 11/11 齐了之后的另一半门槛 |

---

---

## 31. chaos #8 落地 —— fsync 点位故障注入，chaos 场景 11/11 齐（2026-09-18）

### 31.0 本轮落地

| # | 场景 | 用例 | 结果 |
|---|---|---|---|
| 8① | 并发写 + kill 在 **fsync 之前** | `kill_before_fsync_keeps_acked_data_and_drops_unacked` | ✅ |
| 8② | 并发写 + kill 在 **fsync 之后、ack 之前** | `kill_after_fsync_keeps_fsynced_data_without_ack` | ✅ |

**这是唯一必须动生产代码才可测的场景** —— 断电只落在两个瞬时状态之一，
二者对"数据还在不在"的答案**相反**，靠并发去撞是不可靠的。

### 31.1 生产改动：fsync 事件注入点（生产零开销）

| 位置 | 改动 |
|---|---|
| `wal/src/config.rs` | 新增 `FsyncPoint`（`BeforeSync` / `AfterSync`）、`FsyncEvent { point, path, synced_len, file_len, batch_len }`、`FsyncHook`（包一层 `Arc<dyn Fn>` 只为让 `WalConfig` 保持 `Debug`/`Clone`）；`WalConfig.fsync_hook: Option<FsyncHook>`，默认 `None` |
| `wal/src/writer.rs` | `commit_loop` 在 `append_batch` 之后、`sync_all` 之前打 `BeforeSync`；在 `sync_all` 之后、推进水位/ack 之前打 `AfterSync`。生产路径只多一次 `Option` 判断 |

**`FsyncEvent.synced_len` 是关键字段**：它是本次批量写入**之前**的文件长度，
即此刻"确定已持久化"的字节边界。有了它，测试才能真正模拟掉电 ——

> **掉电模型：只有 fsync 过的字节算落盘。** `BeforeSync` 时把文件
> `set_len(synced_len)`（未 fsync 的写入视为从未落盘）再 `panic!` 让提交线程死掉
> （进程内最接近"进程被杀"的形态：此后 append 全部失败）；`AfterSync` 时什么都不做。

### 31.2 两个用例分别钉住什么

| 用例 | 断言 | 顺手钉住的反面后果 |
|---|---|---|
| ① fsync 前 kill | `recovered == acked`（未 fsync 的字节不出现；**已 ack 的一条不少**）+ Data seq 是 `0..n-1` 连续前缀 | **ack 早于 fsync**（先 ack 后 fsync / 不 fsync）会让 acked 集合大于恢复集合 → 立刻红 |
| ② fsync 后 ack 前 kill | `recovered == acked + 3`（已 fsync 必不丢，**哪怕客户端没拿到 ack**） | 这也正是**幂等键存在的理由**：客户端会重试这笔"没拿到 ack 但已经落盘"的写入（§7.3），没有幂等键就是重复计数 |

**"Data seq 必须是连续前缀"** 是"无空洞"的直接断言：中间丢一条就是数据丢失。
（第一版断言数了**全部**记录，把重启后攒批循环写入的 `BatchPending/S3Written/Committed`
也数进去了 → 假失败。已改为只数 `Data`，理由写进了 helper 注释。）

### 31.3 反证（确认不是空测试）

把掉电模型撤掉（`BeforeSync` 时不截断，"未 fsync 的字节侥幸留了下来"）
→ ① 立刻失败：`acked=6 got=9`（恢复了本该消失的那一批）。已恢复。

### 31.4 诚实说明：这两个用例钉的是**顺序契约**，不是"真的调了 sync_all"

持久性由钩子**建模**（截断到 `synced_len`），所以：

- ✅ 能证明：ack 必须在 fsync 之后、水位不得越过 fsync 边界、恢复不会读到撕裂点之后、
  已 fsync 的数据在 reopen 后仍在；
- ❌ 不能证明：`sync_all()` 真的被调用了（把 `sync_all()` 换成空实现，这两个用例照样绿
  —— 因为 tmpfs 上字节本来就在）。

补这一块的正确做法是给注入点加**第三态**：`fsync 返回错误`（钩子返回
`Result`，提交循环按写失败处理：seq 回滚、不 ack、水位不动）。已登记为遗留。

### 31.5 chaos 进度：11/11

`design.md` §12.3 的 11 个场景**全部**在 chaos 层有了真实磁盘 + 跨重启 + 并发的证据：

| 阶段 | 进度 | 抓到的真缺陷 |
|---|---|---|
| 起步 | 3/11 | — |
| 本轮之前 | 10/11 | §27 幂等键不生效、§28.2 恢复重复文件、§29.1 撕裂不可自愈、§30 abort 不同步视图 |
| **现在** | **11/11** | （#8 未发现新缺陷；注入点按设计工作） |

**下一步 = 阶段 2 的另一半门槛：T8 基线压测 + P0 定案**
（相位分散量级 / `max_flush_delay` / `rows_threshold`）+ ADR-10 修订，
现有 `chaos/examples/bench` 只测吞吐与 WAL ack 延迟，P0 要的是
**CommitFiles 瞬时并发 / 文件数·天 / 持久化 P99**，需先扩测量口径。

### 31.6 遗留

| # | 项 | 归属 | 说明 |
|---|---|---|---|
| 1 | 注入点第三态：**fsync 返回错误** | 阶段 2 | 钩子返回 `Result`；断言"不 ack + seq 回滚 + 水位不动 + 系统仍可用" |
| 2 | T8 基线压测 + P0 定案 | 阶段 2 | chaos 已 11/11，这是剩下的那一半 |
| 3 | 修复 §28.1（读侧栅栏） | 与 R3 同批 | 见 28.1 |
| 4 | 谓词下推转 `Exact`（§29.2） | 与 R5 同批 | 必须先转发 filters 到 Parquet 源 |

---

---

## 32. T8 基线压测入库 + P0 ①/③ 定案（含 ADR-10 正式修订）（2026-09-18）

### 32.0 本轮落地

| # | 事项 | 结果 |
|---|---|---|
| 1 | T8 基线压测程序 | `crates/chaos/examples/bench_baseline.rs`（**不是** `bench.rs`：那个测吞吐，这个测 flush 时刻分布） |
| 2 | 度量口径所需的生产改动 | `FileManifest.sealed_at_ms` / `committed_at_ms`（tag 16/17）+ `ChunkFlushInput.sealed_at_ms` |
| 3 | **P0 ① 定案** | `max_flush_delay_secs: 30 → 0`、`flush_phase_spread_secs: 5 → 30`（三处默认值 + 示例配置 + ADR-10 原文） |
| 4 | **P0 ③ 定案** | 持久化上界口径 = **窗口关闭 + `max_flush_delay` + `spread`**，实测吻合、无随机项 |
| 5 | P0 ② | **未定案**（缺 RowGroup 实测，按"先有实测再改默认值"保持 50 万） |
| 6 | ADR-10 正式修订（v12） | 锚点 `seal_time`、确定性相位、量级表 + "每窗口每 shard ≤1 文件"不是不变量的澄清 |

### 32.1 为什么必须给 manifest 加时间戳

P0 的三项证据都要"提交发生的时刻"：**提交时刻分布**（惊群尖峰）、**文件数/天**、
**持久化 P99**。此前这些只能靠推算（用 `valid_from`/`deleted_at` 推墙上时钟是错的 ——
那两个是快照语义）。加两个字段后它们是 manifest 自带的事实的：

| 字段 | 语义 | 为什么这样定义 |
|---|---|---|
| `sealed_at_ms` | **chunk 真实封口时刻**（写侧结束） | 与 `committed_at_ms` 之差 = `max_flush_delay + phase + PUT + CommitFiles` —— 这正是对外承诺的上界口径。**必须由 chunk 层带出**（新加 `ChunkFlushInput.sealed_at_ms`）：在 flush 里取"现在"会把 `max_flush_delay + phase` 从延迟里**抹掉**，指标看起来永远达标 |
| `committed_at_ms` | 提交 Meta 成功的时刻 | 由**发起方**打点（不是 state machine 内取现在）：R3 换 raft 后状态机要在所有副本上确定性应用同一份 manifest，时间戳不能各自取现在。恢复重提交路径留 0（该路径无真实封口时刻，调用方不得当延迟样本） |

### 32.2 实测数据（100 shard 低吞吐表，500 行/秒 = 每 shard 300 行/分钟）

| 配置 | 提交偏移（关窗后）p50 / p99 / max | **带宽** | `seal→committed` p99 | **峰值提交/秒**（均值 2.4） | 文件数 |
|---|---|---|---|---|---|
| A：`md=30 spread=5`（**旧默认**） | 33.9s / 35.25s / 35.27s | **4.86s** | 35.06s | **87**（36× 均值） | 300 |
| B：`md=0 spread=5` | 3.1s / 5.16s / 63.3sⓘ | 4.80s | 5.03s | 77 | 301 |
| **D：`md=0 spread=30`（新默认）** | 15.6s / 31.95s / 68.5sⓘ | 29.4s | **31.35s** | **10** | 253 |
| C：`md=0 spread=60` | 28.8s / 61.7s / 76.6sⓘ | 57s | 59.41s | 7 | 250 |

ⓘ 离群值成因未完成归因，见 §32.4 遗留 #1。

**四条结论（都有数据，不是推断）**：

1. **带宽 ≈ `spread`**（配 5s 实测 4.86s；配 30s 实测 29.4s）→ 相位分散按文档工作，
   且锚点确实是 `seal_time`（若是 `window_start` 锚点，带宽会随窗口对齐塌缩）。
2. **峰值提交数 ≈ shards / spread**：spread=5 时 100 个 shard 的提交挤在 5s 带内 →
   **87 次/秒**（均值 2.4 的 36 倍）；spread=30 → 10 次/秒；spread=60 → 7 次/秒。
   `plan.md §2.2` 说的"分散面比 ADR-10 收窄"**实测成立**。
3. **`max_flush_delay` 不控制文件数，只推迟持久化**：`md=30` 与 `md=0` 的文件数
   300 vs 301（同一窗口都是 1 文件/shard）；而 `seal→committed` 从 35.06s 降到 5.03s。
   → 用 30s 持久化延迟换来的东西**不存在**，这就是把它改成 0 的依据。
4. **上界口径 = 窗口关闭 + `md` + `spread`**，实测与理论一致、**无随机项**
   （A：30+5=35s ↔ 实测 35.06s；B：0+5 ↔ 5.03s；C：0+60 ↔ 59.41s；D：0+30 ↔ 31.35s）。

**定案 = D（`md=0` / `spread=30`），两个维度都优于旧默认**：
上界 35.06s → **31.35s**（更好），峰值 87 → **10 次/秒**（8.7× 削峰）。
上限还有一条硬约束：不变量 `chunk_max_resident(60) > md + spread` → `spread ≤ 59`；
取 30 留一倍余量（`spread=60` 会逼着把驻留兜底抬到 120s+，那就削弱了防 WAL 撑爆的作用）。

### 32.3 顺带测到的两件事实（各自影响后续决策）

| 事实 | 数据 | 影响 |
|---|---|---|
| **低吞吐表的小文件主因是 `shard × 窗口`，不是阈值** | 100 shard × 1 文件/窗口 → **20.7 万文件/天**（新默认 17.5 万）；单文件行数 p50 = 200~300 | 调 `rows_threshold` 对低吞吐表**完全无效**（300 行/窗口/shard ≪ 50 万）。要治只能靠"多 shard 收敛/合并写"，属架构级（P2/T9） |
| **`rows_threshold` 触发会打破"每窗口每 shard ≤1 文件"** | T=5 万 + 5000 行/秒/shard（65s）→ 单文件行数 = 50000 = 阈值、**4 文件/窗口/shard**、单 shard 10.6k 文件/天 | ADR-10 的"≤1 文件"是**窗口驱动的结果**，不是不变量 —— 已在 ADR-10 修订里写明 |

### 32.4 遗留

| # | 项 | 归属 | 说明 |
|---|---|---|---|
| 1 | **离群提交归因** | 阶段 2 | 三档都出现超出 `md+spread` 的离群（63s/68.5s/76.6s）。已知：这些文件的 `sealed_at_ms` 落在**下一个分钟边界**（说明其 chunk 创建时刻晚于自身窗口关闭），候选原因是"写入侧延迟到达"或"`max_resident` 强制封口"。**需要写侧打点（批次到达/被吸收时刻）才能定论，本轮不臆断** |
| 2 | 真多节点 / 真实 S3 | R4 前 | 本轮是单进程 100 shard：把"节点内 flush 串行"与"跨节点并发"混在一起了（C 档理论上限 1.7 次/秒、实测 7 次/秒 → 7 是**本进程 flush 速率地板**）。跨配置的**比值**可迁移，绝对量级不可 |
| 3 | `rows_threshold` + RowGroup（P0 ②） | 阶段 2 | 需要 1KB 行 schema + Parquet RowGroup 大小/内存峰值实测（50 万行 ≈ 500MB 单文件上界是否可接受） |
| 4 | 内存曲线 | 阶段 2 | `metrics()` 已能报水位，但压测未采集时序 |
| 5 | 压测方法论 | — | **教训入册**：第一版排水判据是"连续 3s 无新增提交" → 在最后一个窗口关闭前判空退出，**少算一整个窗口**（写入 65000 行只统计到 37450 行）。正确判据必须叠加"已过最后写入窗口关闭 + md + spread" |

---

---

## 33. P0 ② 专项：`rows_threshold` 定案 —— 结论是"它不是瓶颈"，并抓出**第五个 seal 触发器**（2026-09-18）

### 33.0 定案

| 项 | 结论 |
|---|---|
| `rows_threshold` | **保持 50 万（不改）**。理由不是"50 万合适"，而是**实测它从未触发**：宽行表先撞字节阈值、再被**内存水位**接管；窄行表（<256B/行）才是它的作用域，而那也是合理的行数上界 |
| `bytes_threshold` | **保持 128MB（不改）**。反证：改成 32MB 后文件**更小**（4.2MB vs 16.4MB）—— 直觉"降阈值=控文件大小"是错的，因为压力比阈值先介入 |
| **新登记（P1）** | ① **内存水位是未被声明的第五个 seal 触发器**，高吞吐下它会**顶掉**窗口对齐承诺；② `chunk_mem_budget` 需要定容规则；③ Parquet 未设 `max_row_group_size` → 整文件 1 个 RowGroup |

### 33.1 度量口径（先量清楚，再谈阈值）

`bytes_threshold` 是 **Arrow 内存口径**（`get_array_memory_size`），不是编码后字节。实测：

| 行宽 | 账本口径 | `bytes_threshold=128MB` 触发点 | `rows_threshold=50万` 触发点 | 谁先 |
|---|---|---|---|---|
| 1KB（800B payload + 3 列） | **885 B/行** | ~**151,658** 行 | 500,000 行 | **字节** |
| ~30B（窄行） | ~40 B/行 | ~3.35M 行 | 500,000 行 | **行数** |

→ **"50 万行"只对窄行表有意义**；宽行表（metrics/traces）的实际上界是 128MB ÷ 885B ≈ 15 万行。

### 33.2 实测：真正决定文件大小的是**内存水位**，不是阈值

同一进程、单 shard、1KB 行（`bench_baseline`，md=0/spread=30）：

| 场景 | 速率 | chunk 水位峰值 | 背压档 | **单文件行数** | 单文件字节 | **每(shard,窗口)文件数** | `seal→committed` p99 |
|---|---|---|---|---|---|---|---|
| 低吞吐 | 2 MB/s | 186 MiB / 512 | **Normal** | 19,500~20,500 | ~12 MB | **1** ✅ | 9.8 s ✅ |
| 高吞吐 | 20 MB/s | 463 MiB / 512 | **Hard(80%)** | 27,000 | 16.4 MB | **31** ❌ | **37.5 s** ❌ |
| 高吞吐 + `bytes=32MB` | 20 MB/s | 472 MiB / 512 | Hard | 7,000 | 4.2 MB | **92** ❌ | 30.5 s ❌ |
| 高吞吐 + 无读者 | 20 MB/s | （未采样，见 §33.3） | Hard | 27,000 | （payload 可压，字节不可信） | 37 | **49.3 s** ❌ |

**低速档完全符合设计**（窗口驱动、每窗口每 shard 1 文件、上界 9.8s ≪ md+spread=30s）；
**高速档两条架构承诺同时失效**：

1. **"每窗口每 shard ≤1 文件"（ADR-10 目标）失效**：一个窗口产出 31~92 个文件；
2. **"持久化上界 = 窗口关闭 + md + spread"失效**：实测 p99 37.5s（>30s），无读者时 49.3s。

原因是 `plan_flush` 里那条**从未在文档里出现过**的触发路径：

```
should_seal:  rows>=T  |  bytes>=B  |  now>=window_end          ← 文档只写这三个
plan_flush:   open_too_long(=max_resident) | sealed_too_long     ← 兜底，文档提过
enforce_pressure: 水位 >= Hard(80%) → **强制 seal 所有 open chunk**  ← **实际主导者，文档未提**
```

### 33.3 为什么水位会顶到 90%：**内存归还只由读侧触发**

`ChunkStore::reclaim`（归还 `Flushed` chunk 的内存，I4："数据要等查询缓存追上才能丢"）
**全仓只有一个调用方**：`query/src/cache.rs`（按"查询缓存已追上的快照"回收）。

- 没有读者（或读者跟不上）→ 账本只增不减 → 水位爬到 Hard → `enforce_pressure` 每个周期强制 seal 当前 open chunk
  → **文件大小由"水位与速率的比"决定**，与阈值/窗口无关；
- 极端情形还会连带 spill IO（Hard 档会 spill）与拒绝写入（95% 档）。

**由此得到一条定容规则（本轮的实用产出）**：

```
chunk_mem_budget  ≳  写入速率 × (seal→committed + 读侧 reclaim 周期)
```

实测支撑：20 MB/s 下 resident 稳定在 ~470 MB → 数据从"驻留"到"归还"约 **23.5 秒**
（470 MB ÷ 20 MB/s）；而承诺的上界 `md + spread = 30s` 意味着最坏驻留 30 秒
→ 20 MB/s 时需要的预算约 **600 MB > 512 MB**，压力必然先于阈值介入。

### 33.4 RowGroup 实测：**每个文件 1 个 RowGroup**

`yuntun-format` 的 Parquet 写入只设了 ZSTD 压缩，**没有设 `max_row_group_size`**
（crate 默认远大于单文件行数）→ 实测 5,500 / 7,000 / 20,500 / 27,000 行的文件**都是 1 个 RowGroup**。

后果：**剪枝粒度 = 文件**。当前文件 12~16MB 尚可；一旦按 §33.1 的 128MB 触发，
单文件 128MB 只有 1 个 RowGroup → 文件内无法跳读、写入期内存 = 输入批 + 编码缓冲（实测 VmHWM 477~486 MiB）。

### 33.5 残余疑点（登记，不臆断）

高速档水位一直在 ~92%（463~472/512 MiB），**即使我让"读者"每 200ms 用最新快照调 `reclaim`**。
两种可能未区分：**(a)** 读侧任务与写入/编码争同一个 runtime 被饿死（归还跟不上）；
**(b)** `reclaim` 的 `caught_up` 条件在某些状态下不成立（实现问题）。低速档 Normal 说明它**能**归还
—— 所以更可能是 (a)，但**需要有独立的回收线程或打点证明**，不作为结论。

### 33.6 压测方法学的两条教训（都会静默给出错数据）

| # | 教训 | 后果 |
|---|---|---|
| 1 | **payload 必须不可压缩** | 第一版用 `(seed*7+i*3+j)%64` 生成 → 每 64 字符一周期，ZSTD 压掉 95%+，`file_size` 报出 **0.0MB** → P0 ②（文件大小问题）会被彻底误导 |
| 2 | **必须有读侧消费** | 没有读者时 `reclaim` 永不触发 → 测到的是**背压路径**（小文件 + 49s 延迟），却会被当成"阈值行为"（第一版就是这么得出"27k 行/文件"的假结论） |

### 33.7 遗留

| # | 项 | 归属 | 说明 |
|---|---|---|---|
| 1 | **内存水位驱动 seal** 的定位与处置 | 阶段 2 / R4 前 | 要么提高 `chunk_mem_budget`（按 §33.3 定容）、要么让 reclaim 不依赖读侧（独立回收线程/按时间归还）、要么显式承认"高吞吐下文件数与窗口解耦"并修正 ADR-10 的措辞 |
| 2 | §33.5 的 (a)/(b) 区分 | 阶段 2 | 需要独立回收线程或 reclaim 打点 |
| 3 | `max_row_group_size` 显式设置 | 阶段 2 | 与 `rows_threshold` 一起定（文件内剪枝 vs RowGroup 数）；需先测 RowGroup 大小对扫描/压缩的影响 |
| 4 | 真多节点 + 真实 S3 的绝对速率 | R4 前 | 本轮的**机制**与**定容规则**可迁移，绝对速率（2 MB/s vs 20 MB/s 的分界）不可 |

---

---

## 34. 内存水位处置（第一步）：相位分散让位 + **下调 §33 的结论**（2026-09-18）

### 34.0 本轮落地

| # | 内容 |
|---|---|
| 1 | **已实现**：内存水位 ≥ Soft 时"相位分散让位"——已 sealed 的 chunk 立即 flush，不再等到 Hard 档去 `enforce_pressure` **强制 seal open chunk**（后者才会把同一窗口拆成多个文件）+ spill（无谓本地 IO） |
| 2 | **可观测**：新增 `ChunkStoreStats::phase_yielded_flushes` + `FlushPlan::phase_yielded` + metrics 打点字段 `phase_yielded` + 压测输出 |
| 3 | **测试**：`chunk::phase_yield_flushes_sealed_chunk_early_only_under_pressure`（两个分支都断言：Normal 时**不得**让位、≥Soft 时**必须**立即 flush 且计数 +1；反向验证已做——关掉让位即红） |
| 4 | ⚠️ **但它在真实高吞吐场景里没有触发**（见 §34.2），因此**没有**解决"27000 行/文件"现象 |

### 34.1 先纠正 §33 的一处结论（我自己的判断被实测推翻）

§33.3 我写"水位 ≈ 写入速率 × `spread`（相位窗口内数据）"。**这个因果是错的**，依据是本轮补做的对照实验
（同一负载 20 MB/s、单 shard、1KB 行）：

| 配置 | `seal→committed` p99 | 水位峰值 / 档位 | 单文件行数 | 每(shard,窗口)文件数 |
|---|---|---|---|---|
| `spread=30` | **28.6 ~ 37.5 s** | 437~470 MiB / **Hard** | 27,000 | 26 ~ 31 |
| `spread=5` | **6.6 s** | 403 MiB / **Soft** | **27,000（没变）** | **32（没变）** |

→ `spread` 只影响**延迟与压力档位**（这两项变好很明显：6.6s vs 30+s、Soft vs Hard），
**完全不影响文件大小**。所以"相位窗口内数据"不是文件大小的决定因素；
而**低速档（2 MB/s）一切正常**（1 文件/窗口、Normal、9.8s）说明机制与速率强相关。

**结论下调**：`resident ≈ 写入速率 × (写入→flush 的滞留)` 这个量级判断仍成立
（滞留含"窗口对齐等待 + md + spread"），但**"谁把 chunk 切小到 24 MB"仍未证实** ——
`rows_threshold`(50万) 与 `bytes_threshold`(128MB，账本口径 885B/行 ⇒ 15.2 万行)
**都解释不了 27000 行**，`enforce_pressure` 是候选但 §34.2 显示它也不是直接原因。

### 34.2 为什么"相位让位"没触发（`phase_yielded = 0`）

`plan_flush` 里的 `phase_yields` 取自**调用瞬间**的水位。实测该场景最高档是 Hard，
但 `plan_flush` 的取样点常落在 `enforce_pressure` 刚刚 spill 完、水位回落到 Normal 之后
→ `phase_yields = false` → 让位逻辑不生效。

**这个"没触发"本身就是证据**：水位在 Soft/Hard 与 Normal 之间高频振荡（spill 立刻把水位压回去，
写入又立刻填上来），而振荡的**周期**决定了 open chunk 能长多大 —— 与"27000 行"这个稳定的数字
是否同源，**需要下一步的观测才能回答**（见 §34.3）。

### 34.3 下一步（唯一能终结这个问题的动作）：**把 seal 原因变成可观测量**

现在"为什么这个 chunk 被 seal"**在运行时不可见**：`SealReason { Rows, Bytes, WindowClosed, SchemaChanged }`
只在 `store.rs` 内部产生，不落 WAL、不落 manifest、不进指标。于是所有推断都只能靠"排除法 + 速率算术"，
这正是本轮两次结论修正的原因。

具体做法（下一步第一件事）：
1. `FileManifest` 增 `seal_reason`（tag 18）+ `sealed_rows`（或复用 `row_count`）；
2. 顺带记录**封口时的水位档位**（`pressure_at_seal`），这是区分"阈值触发"与"压力触发"的唯一硬证据；
3. 压测按 seal 原因分组统计 → **直接读出**各原因的占比与行数分布，不再靠排除法。

> 在这之前，`status.md` 里"内存水位是第五个 seal 触发器"应标注为**候选**而非结论。

### 34.4 本轮仍成立的事实（可直接用）

| # | 事实 | 依据 |
|---|---|---|
| 1 | **`spread` 是延迟/压力的强杠杆**：20 MB/s 下 5s → 6.6s、30s → 28.6~37.5s | §34.1 对照实验 |
| 2 | **低速（2 MB/s）完全符合设计**：1 文件/窗口、Normal、上界 9.8s | §33.2 |
| 3 | **内存紧张时相位应当让位**（而不是等 Hard 强封 open chunk）——逻辑正确、已测、已可观测，只是本轮负载没触发 | §34.0 |
| 4 | **`reclaim` 本身没问题**（有确定性测试断言 `freed > 0`，且 server 侧有后台 `spawn_cache_refresh` 会推进缓存并回收）→ §33.5 的"归还失效"疑虑**排除** | 本轮核查 |
| 5 | RowGroup = 1 个/文件（未设 `max_row_group_size`） | §33.4 |

---

---

## 35. seal 原因落地观测 → 真相是 `bytes_threshold`（第三次、也是最后一次修正）（2026-09-18）

### 35.0 落地

| 位置 | 改动 |
|---|---|
| `chunk/src/chunk.rs` | `Chunk.seal_reason` / `pressure_at_seal` + `seal_tagged(reason, pressure, now)` |
| `chunk/src/store.rs` | `SealReason` 扩充（+`ResidentCap` / `Pressure` / `Manual` + `as_str()` 稳定字符串）；`pending_seal` 登记表（`plan_flush` / `enforce_pressure` 决定要 seal 时登记原因，`seal()` 取用 —— 这样 `plan.seal: Vec<ChunkId>` 的公开签名不用改）；`flush_input` 带出 |
| `model/src/meta.rs` | `FileManifest.seal_reason`（tag 18）/ `seal_pressure`（tag 19） |
| `ingest/src/flush.rs` | 封口点打点（恢复重做路径留空） |
| `chaos/examples/bench_baseline.rs` | 按 seal 原因 / 水位档位分组统计 + 实测每行占用 |
| 测试 | `seal_reason_and_pressure_are_recorded_and_carried_to_flush_input`（Rows / WindowClosed / Pressure 三种原因各一条断言） |

### 35.1 实测：`bytes_threshold` 在 **Normal 水位**下正常触发

20 MB/s、单 shard、1KB 行、`md=0/spread=30`、`rows=50万`、`bytes=128MB`：

```
---- seal 原因分布 ----
bytes_threshold  files= 35  rows min/p50/max = 27000 / 27000 / 27000
pressure         files=  2  rows min/p50/max = 14000 / 14000 / 17000
window_closed    files=  3  rows min/p50/max = 12000 / 22500 / 25000
封口时水位档位        : {"Normal": 33, "Soft": 5, "Hard": 2}
```

### 35.2 结论（含对我前两次判断的纠正）

| # | 结论 | 依据 |
|---|---|---|
| 1 | **"内存压力主导 seal"不成立**（§33.2 与 §34.2 的候选被否） | 35/40 文件是 `bytes_threshold`，其中 33/40 水位 **Normal** |
| 2 | **有效账本口径 ≈ 4.9 KB/行**（1KB 行的现实形态） | 128MB ÷ 27000 行 = 4.97 KB/行；`bytes=32MB` 档 → 7000 行（32/128×27000 ≈ 6750 ✓ 线性吻合） |
| 3 | **"新建批探针"量错了对象**（885 B/行，差 5.6×） | chunk 持有的是 **WAL 解码后**的 Arrow 形态（偏移/容量/属性与新建批不同）。以后**一律用实测**：`文件行数 ≈ bytes_threshold ÷ 有效口径` |
| 4 | **"每窗口每 shard ≤1 文件"只在"窗口内数据量 ≤ `bytes_threshold`"时成立** | 20 MB/s × 60s = **1.2 GB ≫ 128MB** → 必然多文件（实测 26~92 个/窗口）。**这不是缺陷，是阈值与速率的算术关系**；128MB 对应 1KB 行 ≈ 2.7 万行/文件 |
| 5 | 压力触发是**次要因素**（2/40），且它是"水位顶掉窗口承诺"的真实路径，相位让位（§34）仍应保留 | 同上 |

**对 ADR-10 的影响**：其"每窗口每 shard 最多 1 个文件"是**低/中速率下**的结论。
高吞吐下要么接受多文件（并写明条件），要么**按速率放大 `bytes_threshold`**（= 用内存换文件数，
因为要让一个窗口的数据不封口，必须在内存里hold 住 `速率 × 60s`）。

### 35.3 遗留

| # | 项 | 归属 | 说明 |
|---|---|---|---|
| 1 | ADR-10 措辞修正（加"当窗口内数据量 > `bytes_threshold` 时为多个文件"） | 阶段 2 | 与本条同批 |
| 2 | 是否让 `bytes_threshold` 随速率自适应 / 给用户一个"目标文件行数"配置 | 阶段 2 | 目前用户只能设字节阈值，却观测到行数 —— 口径不直观 |
| 3 | "有效口径 5× 于新建批"的根因（WAL 解码形态的具体构成） | 阶段 2 | 影响内存预算定容（D11）的精度；先记为已标定常数 4.9 KB/行（1KB 行） |
| 4 | 内存水位路径的定位精度（`enforce_pressure` 何时真的触发） | 阶段 2 | 本轮已有 `seal_pressure` 字段可统计，样本还少（2/40） |

---

---

## 36. `max_row_group_size` 专项：显式设定 65,536 行 + 端到端验证（2026-09-19）

### 36.0 落地

| 位置 | 改动 |
|---|---|
| `format/src/lib.rs` | 新增 `MAX_ROWS_PER_ROW_GROUP = 65_536`；`encode_parquet` 显式 `set_max_row_group_size`（此前**只设了 ZSTD** → 用 crate 默认约 100 万行） |
| `format` 测试 | `parquet_row_groups_are_bounded_by_explicit_setting`：写 `65536×3+1234` 行 → 断言**分组数 = ceil(rows/上限) = 4**，且**每组不超过上限**（防实现被换回"整批一组"仍绿）；顺便断言解码后行数不变 |

### 36.1 为什么必须显式设（两个方向的后果）

不设 → 默认上限远大于单文件行数 → **整文件恰好 1 个 RowGroup**（§33.4 实测 5.5k~27k 行都是 1 组）：

| 方向 | 后果 |
|---|---|
| 查询 | **剪枝粒度 = 文件**。当前文件小（1KB 行约 2.7 万行 / 16MB）时无害；一旦 `bytes_threshold` 上调或换窄 schema 使文件变大，就变成"文件内无法跳读" |
| 写入 | 一个 RowGroup 的所有列缓冲要**同时驻留**才能落盘 —— 组越大峰值内存越高（§33.2 实测 VmHWM 里编码缓冲占相当一部分） |

取 **65,536 行**的理由：1KB 行时约 64MB/组（chunk 预算可承受）；且对**当前**文件规模**不改变行为**
（27k 行 < 64k → 仍是 1 组），只作为"文件变大时不要退化成单组"的**显式上界**。

### 36.2 端到端验证（窄行 + 大 `rows_threshold`，让文件超过 64k 行）

```text
cargo run --release -p yuntun-chaos --example bench_baseline -- 45 1 500 40 0 30 150000 0 1024
（pad=0 → 窄行；rows_threshold=15 万；bytes_threshold=1GB 确保字节阈值不抢先）

RowGroup 实测 : rows=150000 row_groups=3 | rows=150000 row_groups=3 | rows=60500 row_groups=1
```

**15 万行的文件 = 3 组**（= ceil(150000/65536) ✓），**60,500 行 = 1 组**（< 上限 ✓）
—— 与上限一致。改动前这两种文件都只会是 1 组。

### 36.3 遗留

| # | 项 | 说明 |
|---|---|---|
| 1 | 改 `MAX_ROWS_PER_ROW_GROUP` 的门槛 | 注释里写明：必须同时给**文件大小 / 扫描剪枝 / 写入峰值内存**三组数据 |
| 2 | 剪枝收益的定量测量 | 本轮只证明了"分组数符合上限"；"组级剪枝省了多少扫描"需要带谓词的扫描基准（属 T8 扩展） |
| 3 | Vortex 路径 | `encode_vortex` 仍是 feature-flag 占位（ADR-1 锁定 Git Commit 后启用），届时需单独确认其 RowGroup 语义 |

---

---

## 37. 多节点基线压测（本机多进程）：全局提交时间线 + 争用下的 seal 构成（2026-09-19）

### 37.0 先划清边界（否则结论会被误用）

| | |
|---|---|
| ✅ **测了** | **N 个独立节点并行**时的**全局**提交时间线（峰值提交/秒、文件数·天、seal 原因分布），以及 CPU/IO 争用下每节点的退化 |
| ❌ **没测** | "N 个节点打**同一个 Meta** 的 CommitFiles 并发" —— 当前 Catalog 是**进程内**的（`MemoryCatalog`），没有共享 Meta，**要等 R3**（metanode + raft/gRPC）。本结果对未来 Meta 是**上界**：真并发只会更集中（各节点各自的 flush 线程同时打） |
| ❌ **没测** | 真实对象存储的 PUT 延迟（本机无 S3/MinIO，走本地 FS store）；跨机网络与时钟偏移 |

工具：`scripts/bench_multi.sh`（每节点独立 WAL/spill/store + 独立 CSV 导出；
**聚合用 `sort` 合并后做滑动窗口分桶，而不是把各节点峰值相加** —— 后者会把不同时刻的峰叠加，虚高）。

### 37.1 结果（4 节点 × 5 shard × 5k rows/s = 20k rows/s；12 核机器；90s）

| 量 | 单节点（§35，1 shard @20k rows/s） | **4 节点（5 shard @5k rows/s each）** |
|---|---|---|
| seal 原因构成 | `bytes_threshold` **87%**（35/40）、pressure 5% | **`pressure` 60%**（15~16/节点）、`window_closed` 40%（10/节点） |
| 单文件行数 p50 | 27,000（≈16.4MB） | **17,125（≈15MB）** |
| chunk 水位峰值 / 档位 | 437~470 MiB / **Hard** | 405~407 MiB / **Soft** |
| **相位让位次数** | **0** | **2~3 / 节点** |
| `seal→committed` p50 / p99 / max | 22.6s / 26.0s / 28.3s | 15.2s / 29.8s / **33.2s** |

**全局（4 节点合并时间线）**：101 个文件；均值 **0.85 次/秒**；**峰值 9 次/秒**（6 次/100ms）；
总写入 ≈ 118 万行。

### 37.2 结论

1. **争用会改变 seal 原因构成**：同样是 20k rows/s 的总速率，
   单进程时 `bytes_threshold` 主导（87%），4 进程时 **`pressure` 主导（60%）** ——
   因为 flush 与编码在同一批核上抢 CPU，水位留在 Soft 的时间变长。
   所以 §35 的"文件大小由 `bytes_threshold` 决定"**只在节点不吃紧时成立**；
   吃紧时压力提前介入（文件更小：17k 行 vs 27k 行）。
2. **§34 的"相位让位"修复首次在真实场景触发**（2~3 次/节点）：单节点 20MB/s 时它是 0
   （水位振荡太快、取样点踩不到），4 进程争用时水位稳定在 Soft → 逻辑生效。
   这同时说明"修复没触发"通常意味着**路径没被走到**，而不是逻辑错。
3. **峰值提交 9 次/秒（4 节点）与单节点 20MB/s 的 5~9 次/秒同量级** ——
   提交速率 ≈ `总写入量 ÷ 单文件行数`，与节点数关系不大；未来 Meta 的承载需求按"总速率"估，
   而不是按节点数线性放大。
4. **上界的诚实形式**：实测 max 33.2s > `md + spread = 30s` →
   上界应表述为 **`封口 + md + spread + 提交路径耗时`**（实测 30s + ~3s）。
   D3/ADR-10 的"确定无随机项"依然成立（相位是确定性的），但要带上这一段固定开销。

### 37.3 遗留

| # | 项 | 归属 | 说明 |
|---|---|---|---|
| 1 | **R3 后打同一 Meta 的真并发** | R3 | 本轮的"上界"需要换成实测（也才能验证 raft 写入路径的尖峰承受力） |
| 2 | 真实 S3 / MinIO 的 PUT 延迟与 503 退避 | 有环境时 | 脚本已支持 S3 端点（store 抽象），需要环境变量与凭证 |
| 3 | 跨机（网络、时钟偏移、机架） | R4 前 | 本机 12 核 4 进程已见明显争用 |
| 4 | 内存曲线**时序**（本轮到目前只有峰值） | 阶段 2 | 打点已具备（`chunk_stats`），需要按 1s 采样导出 |

---

---

## 38. R3 开工第一切片：`CatalogState` 纯状态机抽出 + **四处非确定性**（2026-09-19）

按 [`metanode-design.md`](metanode-design.md) 的开工顺序，先做最高风险项 **S3-2**（`CatalogState` 抽取），
它无外部依赖且能在早期暴露"副本静默分叉"。

### 38.1 落地

| 位置 | 改动 |
|---|---|
| **新** `catalog/src/state.rs` | `CatalogState`：纯状态机（无锁 / 无时钟 / 无 IO）。容器全部 `BTreeMap`/`BTreeSet`，版本号是**普通 `u64` 字段**（不再是 `AtomicU64`：多副本各自 `++` 会在乱序 apply 时分叉）；变更方法签名一律 `(&mut self, …, now_secs: u64)` —— **时间由调用方传入** |
| `catalog/src/lib.rs` | `MemoryCatalog` 退化为**宿主**：`RwLock<CatalogState>` + 取钟 + trait 转发（语义零改动）。R3 的 metanode 会把同一份 `CatalogState` 交给 raft 驱动 |
| `model/src/ops.rs` | `CommitFilesRequest.client_request_ids: Vec<String>`（**键集合**，S3-5 的接口部分）：集合中**任一**键已登记即整次判重（关闭 `§27.5` 遗留 #1 的接口侧） |
| `format/src/lib.rs` | `set_max_row_group_size` → `set_max_row_group_row_count(Some(_))`（§36 的上游弃用更名） |

### 38.2 抽出的过程中抓到**四处真实存在的非确定性**（都已修）

| # | 位置（抽出前） | 后果 | 修法 |
|---|---|---|---|
| 1 | `create_table`：`TableMeta.created_at = now_secs()` | 各副本 `created_at` 不同 → **状态逐字节不一致**（M3 直接失败） | 时间由调用方传入 |
| 2 | `create_table`：`SchemaVersion.created_at = now_secs()` | 同上（版本链分叉） | 同上 |
| 3 | `evolve_schema`：`SchemaVersion.created_at = now_secs()` | 同上 | 同上 |
| 4 | `commit_compaction`：用 **`HashSet<String>`** 收集受影响表，再**按其迭代序分配 `manifest_ver`** | **不同副本把同一个 `manifest_ver` 分给不同的表** → delta 语义静默错位（最难查的一类） | 改 `BTreeSet`（有序） |

> 值得强调的是：这四处**在今天也是"能跑、测试全绿"的** —— 它们只在"多副本 apply 同一串 op"时才暴露。
> 这正是设计里把"确定性纪律"排在第一风险（R3-1）的原因。

### 38.3 确定性防线：规范编码 + 对拍用例

- `CatalogState::encode_canonical()`：把**所有影响后续行为的字段**（两组版本号、快照号、每表最后变更版本、
  版本链与幂等记录的时间戳、文件清单）按**键序**编码。少一个字段，对拍就会漏掉一类分叉。
- 5 个用例（`catalog/src/state.rs::tests`）：
  1. `same_op_sequence_yields_byte_identical_state` —— 同一串 op（含相同时间戳）在两台状态机上 → **逐字节相同**（R3-1 的防线）；
  2. `state_records_carried_timestamps_not_local_clock` —— 断言记录的是**传入值**；
  3. `compaction_version_assignment_is_order_independent` —— 建表顺序不同但内容相同 → 状态编码相同（专打第 4 处 `HashSet`）；
  4. `commit_files_dedups_on_key_set` —— 键集合去重（任一键命中即整次判重、不落 manifest）；
  5. `idempotency_sweep_uses_carried_now` —— TTL 清理的时间也由调用方传入。
- **反证已做**：把 `create_table` 改回"状态机自己取钟"（写死 `999_999_999`）→ 用例 2 立刻失败
  （`left: 999999999, right: 42`），已恢复。
- ⚠️ **诚实说明**：用例 3 对第 4 处的捕获是**概率性**的（`HashSet` 两个元素、随机种子下的迭代序约各半），
  所以那条纪律**不能只靠测试守**，必须靠"状态里禁用无序容器"的结构性约束（`state.rs` 文件头四条纪律）。

### 38.4 本轮**没做**（明确记账，避免"看起来完成了"）

| # | 未做项 | 归属 | 说明 |
|---|---|---|---|
| 1 | **键集合的实际接线** | S3-5 剩余 | `flush.rs` 仍传空集合（chunk 目前不记录它聚合了哪些幂等键）。**权威去重仍靠 ingest 入口预筛** —— 已在 `flush.rs` 写明，不留静默空值 |
| 2 | proto / tonic-build | S3-0 | 未开始（需新增依赖） |
| 3 | raft 接入 / metanode 进程 | S3-1 / S3-3 | 未开始 |
| 4 | 快照的 prost 版本 | S3-3 | 现为文本规范编码（语义等价，S3-3 换 prost 时用例可继续用） |

### 38.5 验证

`cargo test --workspace`：**222 passed / 0 failed**（新增 5 个用例）；`clippy --all-targets` 0 警告。

---

---

## 39. R3 S3-5 接线：键集合去重真正生效 —— 并抓到"认领 ≠ 重复"这个语义坑（2026-09-19）

### 39.0 落地

| 位置 | 改动 |
|---|---|
| `ingest/src/flush.rs` | 新增 `collect_idempotency_keys(deps, from, to)`：从 **WAL** 派生本批次的幂等键集合（去重 + 排序）；`flush_chunk` 与 `recommit_into_catalog` 两条提交路径都接上；`client_request_id`（单键字段）保持 `None`，权威语义移到键集合 |
| `catalog/src/state.rs` | `commit_files` 的键集合判定改为**区分三种形态**（见 §39.1） |
| `chaos/src/lib.rs` | 夹具加 `build_tuned(rows_threshold, delay)`：默认夹具 `rows_threshold=1` → 每条批次一进 chunk 就 seal，**造不出"一个 chunk 聚合多个键"**，键集合路径根本测不到 |

**为什么从 WAL 派生而不是让 chunk 记着**：WAL 是**唯一写入事实**（ADR-3），chunk 只是它的内存视图。
在 chunk 里再存一份键集合就出现**第二个真相来源** —— 两者不一致时没有任何依据判定谁对
（本项目 §27/§28 那类缺陷全是"两份状态"造成的）。代价是该批次 WAL 区间的一次扫描
（段文件刚写过，通常都在页缓存里），收益是**不可能不一致**。

### 39.1 抓到的语义坑：**"已被认领" ≠ "重复提交"**

接线后**既有用例 `idempotency_survives_compaction` 当场变红**（0 文件 —— flush 再也没落盘）。
根因是两层幂等机制的语义冲突：

| 层 | 时机 | 记录形态 |
|---|---|---|
| 入口预筛（§27） | ingest **WAL fsync 之后**立刻登记（防并发同键双写） | `batch_id` **留空** = "已认领、批次尚未落盘" |
| 提交层（本次接线） | flush 提交 manifest | 期望"键不存在" |

若提交层把"键已存在"一律当重复 → **manifest 永不落盘**：写入返回成功，但数据**永远不可见**；
更糟的是**恢复路径 100% 失败**（重启后从 WAL 重建的索引 `batch_id` 也是空的）。

**修法（三种形态分开处理）**：

| 记录形态 | 含义 | 处理 |
|---|---|---|
| `batch_id` **为空** | ingest 认领（§27），或重启后从 WAL 重建的索引 | **补全**为本批次并落盘 —— 这正是本次提交要写的**数据** |
| `batch_id` 非空且 ≠ 本批次 | 另一个**已提交**批次占用该键 | 整次判重（`accepted=false`） |
| 不存在 | 正常路径 | 登记并落盘 |

**恰好一次由状态机串行化保证**：两个并发同键请求都写出了 Data（§27 的竞态窗口），
它们各自提交时，**先提交的那个补全认领**，后提交的看到非空 `batch_id` → 判重 ✓
（单机靠 `RwLock`，R3 靠 raft 日志序 —— 两种宿主都串行）。

### 39.2 测试

| 用例 | 断言 |
|---|---|
| `chaos::commit_registers_every_key_of_a_multi_key_chunk`（端到端） | **构造确定性**：`rows_threshold=100` → 先写两条不同键（3 行 + 120 行）**再启动攒批循环** → 必然同一次吸收 → **一个 chunk、两个键**；断言**两个键都在 Catalog 里** + 不在集合里的键**不**命中 |
| `catalog::claimed_key_is_completed_at_commit_not_rejected`（状态机） | 认领态提交必须**成功且补全** `batch_id`；另一个批次再用同键才判重；判重不得落第二个文件 |
| 既有 `idempotency_survives_compaction` | 恢复绿（它变红正是本次的发现手段） |

### 39.3 遗留

| # | 项 | 说明 |
|---|---|---|
| 1 | WAL 段回收后无法恢复键集合 | `recommit_into_catalog` 尽力而为：段被回收时退回 `batch_id` 幂等（数据不会重复写，只是"同键重试"可能漏判）——已知边界，已就地注明 |
| 2 | 键集合的**可观测性** | 尚未进指标（可考虑在 metrics 里加"本次提交携带键数"的分布，用于判断 chunk 聚合度） |
| 3 | `BatchPendingPayload.client_request_id` 仍是单键 | 它只用于恢复期的批次追踪；权威键集合在 Catalog。若将来需要按批次反查键集合，应改为从 WAL 派生（同一原则） |

---

---

## 40. R3 S3-1 选型闸门：raft-rs PoC 通过 —— **保留 raft-rs**（2026-09-19）

### 40.0 闸门结论

| # | 判据 | 结果 |
|---|---|---|
| 1 | 三节点选主 + **收敛到同一状态** | ✅ `three_node_cluster_converges_on_proposed_ops`（三个节点的规范编码**逐字节相同**） |
| 2 | kill leader → 重新选主 + **已提交数据不丢** | ✅ `leader_kill_reelects_and_keeps_committed_ops`（新 leader 由存活节点当选、b1/b2/b3 全在） |
| 3 | **状态机接缝干净**：`CatalogState` 原样被驱动 | ✅ `apply_op()` 是纯函数（无锁/无时钟/无 IO），两个候选都不需要为它改一行 |
| 4 | 样板规模可接受（设计 §4.2 的逃生门：>50% 总工作 → 换 openraft） | ✅ **raft 集成核心 124 行**（Ready 循环 49 行）；整个 PoC crate 482 行含注释、测试 163 行 |

**结论：保留 raft-rs**（正确性证据优先：它有 Jepsen 验证，openraft 生产验证较少）。
逃生门仍未关闭但**降级为备选**：若 S3-3 实现 fjall `Storage` + 快照安装 + 成员变更时样板失控，
再评估 openraft —— 届时换的只是 `yuntun-meta` 内部实现，`CatalogState` 与上层接口不动
（这正是先做 S3-2 的价值）。

### 40.1 交付物

| 位置 | 内容 |
|---|---|
| **新 crate** `yuntun-meta` | S3-3 的落点；本轮内含 PoC |
| `crates/meta/src/lib.rs` | 三节点集群（进程内 `mpsc` 传 `Message`，**不序列化**）+ `RawNode` 驱动循环 + `apply_op` 接缝 + `Cluster`（wait_leader/propose/kill/canonical） |
| `crates/meta/tests/raft_poc.rs` | 两个闸门用例（163 行） |

**边界（诚实说明，避免被当成"R3 已完成"）**：
- 用 raft-rs 自带 `MemStorage` —— 落盘/崩溃恢复**没测**（真 Storage 是 S3-3 的 fjall 后端；
  它在两个候选下工作量相同，故与选型无关）；
- 进程内消息**不序列化** —— gRPC 编解码/超时/重试是 S3-0 + S3-3；
- **快照安装与日志压缩没测**（PoC 无 compaction、无新成员加入）→ 登记为 S3-1b；
- 成员变更、pre-vote 隔离行为、读写一致性（ReadIndex/线性化读）未测。

### 40.2 两个值得记的坑（都是实测撞出来的）

| # | 坑 | 现象 | 教训 |
|---|---|---|---|
| 1 | **raft entry index ≠ 已应用 op 数** | 第一版用例按 `applied >= 4` 等收敛 → **假失败**："节点 3 与节点 1 不一致"。真因：**leader 就位会先写一条空 no-op 条目**占掉 index 1，4 个 op 对应 index 2..5；follower 应用到 index 4 时其实只应用了 3 个 op | 等待条件必须**语义化**（"状态里出现 X"），不要把 raft index 当业务进度 |
| 2 | **待回复队列必须在 `propose()` 成功之后入队** | 若先入队再提议、提议失败时再弹出，队列与日志条目会**错位** → 应用 A 的 op 却回复了 B 的等待者（静默错配，客户端以为自己的写成功了） | 顺序：`propose()` 成功 → 入队；失败 → 立刻回错不入队 |

### 40.3 反证（证明用例不是自说自话）

把节点间的消息路由切断（`take_messages` 不外发）→ **两条用例都失败**（10s 内选不出 leader）。
已恢复。即：这两个用例依赖**真实的消息复制**，不是"本地自转"。

### 40.4 遗留

| # | 项 | 归属 |
|---|---|---|
| 1 | 快照安装 + 日志压缩（`compact()` + 新成员追快照） | **S3-1b** |
| 2 | fjall `Storage`（term/vote/commit + 日志 + 快照落盘）→ 崩溃恢复 | S3-3 |
| 3 | gRPC 传输（`Meta.Propose/Prefetch/Delta`）+ 非 leader 带 leader hint | S3-0 + S3-3 |
| 4 | 线性化读（ReadIndex）与"读旧窗口 ≤ `cache_ttl`"的边界 | S3-4 |
| 5 | 成员变更（learner → voter）与 `--init` bootstrap | S3-6 |

---

---

## 41. R3 快照编解码（S3-1b / S3-3 的共同前置）—— 帧格式 + 状态机无损载荷（2026-09-19）

### 41.0 为什么先做这个

S3-1 闸门留下一项（**快照安装**），S3-3 的 fjall `Storage` 也要写快照 —— 两者的**共同前置**
是"状态机能**确定性**编解码"。所以先把它做成一块能独立验证的东西，而不是等 S3-3 一起动。

### 41.1 交付

| 位置 | 内容 |
|---|---|
| `model/src/snapshot.rs`（**新**） | 快照**载荷**（prost，11 字段）+ **帧格式**（magic / version / 帧头 CRC / 分块 CRC）+ 载荷编解码 |
| `model/src/error.rs` | `SnapshotError` —— **独立于 `LakeError`**：这些全是**致命一致性错误**，混进 `LakeError` 会诱导调用方按"可重试"处理 |
| `catalog/src/state.rs` | `to_snapshot_msg()` / `from_snapshot_msg()`（严格）/ `snapshot_artifact()` / `restore_snapshot()` |

### 41.2 帧格式：三层保护（`metanode-design §4.4` 落地）

| 层 | 保护 | 抓什么 |
|---|---|---|
| 帧头 | magic + format_version + **帧头 CRC** | 截断到 36 字节内、`revision`/`payload_len`/`chunk_size` 被改 |
| 每块 | **块 CRC** + 显式块长 | 块内任意字节损坏、块被截半 |
| 整体 | `payload_len` = 实收长度、末尾无残留 | 丢块、多块 |

**为什么不能只靠 protobuf**：**截断后的字节前缀仍然是合法 protobuf**（字段可选、未知数据被忽略），
于是"半个快照"会被静默解码成一个**看起来正常但缺了一半数据的状态**并装到副本上。
这与 WAL tearing（`§29.1`）是同一类错误，只是载体从 segment 换成了快照。

**为什么帧头也要 CRC**：`revision` 只出现在帧头；而单字节翻转 `chunk_size` 会让"块长上限"检查变**松**
（`<=` 判定），块 CRC 依旧通过 —— 只有帧头 CRC 能抓到。

### 41.3 实测：两条"任何位置都检出"的用例

| 用例 | 做法 | 结果 |
|---|---|---|
| `truncation_at_any_offset_is_detected` | 产物**每一个**偏移都截断一次 | 全部 `Err` |
| `single_byte_corruption_anywhere_is_detected` | 产物**每一个**字节都翻转一次 | 全部 `Err`（帧头/块头/块数据三区都覆盖） |

### 41.4 状态机侧：无损 + 维度覆盖

| 用例 | 钉住什么 |
|---|---|
| `snapshot_roundtrip_is_lossless_and_deterministic` | 恢复后规范编码相同；**再产出快照的字节也相同**（= 快照编码确定性，否则副本间无法比对/去重） |
| `snapshot_roundtrip_preserves_future_behavior` | **恢复出的状态继续 apply 同一 op**，幂等命中行为与后续结果都必须与源状态一致 —— 比静态编码比对更强 |
| `snapshot_covers_every_state_dimension` | **11 个维度**逐一改动，快照字节必须都变（专防"加字段忘加进快照"） |

最后一类是**故意的回归防线**：忘字段不会让任何其它测试变红，只会让换主/重启后状态**静默回退**。

### 41.5 反证

把 `idempotency` 从快照里漏掉（改成 `Vec::new()`）→ 维度覆盖测试**立刻失败**，报
`维度 idempotency 变了但快照字节没变 → 快照漏了这个字段（换主/重启后会静默回退）`。已恢复。

### 41.6 一个校验顺序的坑（错误信息的**指向性**）

第一版把"版本检查"放在帧头 CRC **之前** → 帧头被改坏（版本字段恰好变成别的数）时报
`UnsupportedVersion`，把运维引向"去升级 build"，而真相是**文件损坏**。

正确顺序 **magic → 帧头 CRC → 版本**：

| 顺序 | 理由 |
|---|---|
| magic 先 | 根本不是快照时，报 `BadMagic` 比"CRC 不符"有用 |
| 帧头 CRC 次 | 帧头坏了就说"坏了"，不要说成"版本不支持"（**换 build 解决不了损坏**） |
| 版本最后 | 帧头**完好**而版本不同 = 确实是别的版本写的**合法**快照 → 这时 `UnsupportedVersion` 才准确 |

用例同时钉住两种情形（"改版本号但 CRC 未同步" → CRC 错；"CRC 正确的未来版本" → 版本错）。

### 41.7 严格重建：**拒绝**而非"尽力恢复"

| 规则 | 理由 |
|---|---|
| `format_version` 必须等于本实现版本 | 载荷布局变了必须显式拒绝，不能猜 |
| `revision > 0` | 0 = 快照号未初始化，不是合法状态 |
| `namespaces` 必须含 `public` | 状态机不变量（`CatalogState::new` 保证） |
| 各列表**键不得重复** | 重复键下"谁生效"取决于遍历顺序 = 不确定性 |
| 条目值不得为 `None` | 空条目是构造错误 |

**有意不校验**：文件与表的**引用完整性**。`drop_table` 与 `commit_files` 交错可能产生
"文件引用了已删表"的中间态，硬校验会让**合法快照装不进去** —— 比少校验危险得多。

另有一条跨层校验：**帧头 revision 必须等于载荷 revision**（`RevisionMismatch`）——
不一致说明帧与载荷不是同一次快照产生的（拼接/回滚搞混），装上去必错。

### 41.8 边界与遗留

| # | 项 | 归属 |
|---|---|---|
| 1 | **快照安装进 raft**（`Storage::snapshot` + compaction + follower 落后追快照） | **S3-1b**（前置已完成，可直接做） |
| 2 | `§4.4` 的**保留策略**（按表保留 checkpoint + 归档旧条目到对象存储） | S3-3（R3-4 快照膨胀风险） |
| 3 | fjall 落盘（快照字节已是可落盘形态） | S3-3 |
| 4 | 大快照的**传输/内存曲线**（10 万条 manifest 规模） | S3-6 压测 |

---

---

## 42. R3 S3-1b：快照安装 + 日志压缩 —— **选型闸门全部判据通过**（2026-09-19）

### 42.0 交付

| 位置 | 内容 |
|---|---|
| `crates/meta/src/storage.rs`（**新**） | 自实现的 raft `Storage`（`MetaStorage`）：硬状态 / 日志 / 压缩位置 / **状态机产物** |
| `crates/meta/src/lib.rs` | PoC 接到 `MetaStorage`；Ready 循环里的**快照安装**（还原状态机 + 固化元数据）；集群支持 kill → **空存储重启** |
| `crates/meta/tests/raft_poc.rs` | 判据 5：`follower_behind_catches_up_via_snapshot` |

### 42.1 为什么**不能**用 raft-rs 自带的 `MemStorage`（这是本轮的硬发现）

第一版 PoC 用 `MemStorage` 跑得挺好，直到要实现快照安装才发现它根本不能用：

| # | 问题 | 后果 |
|---|---|---|
| 1 | `MemStorageCore::snapshot()` 造的快照 **data 为空**（元数据取自 `hard_state.commit`） | follower 装上等于**把状态机清空** —— 而且**不报错**（raft 只看元数据） |
| 2 | 它的 `compact()` 只丢日志、**不认识状态机** | 快照内容必须来自 `CatalogState`，这是存储实现方的责任，它无从知道 |

所以快照能力**不是可选优化**：它是"必须自己写 `Storage`"的硬前提。生产实现换 fjall 时结构不变（`MetaInner` 的字段变成表/键值）。

### 42.2 核心不变量：产物 ↔ 压缩位置**严格对应**

> `artifact` 必须是"应用到 `compacted_index` 那一刻"的状态机产物。

两种错误写法都**静默**出错（都不报错、测试也未必红）：

| 错误写法 | 后果 |
|---|---|
| `snapshot()` 里**现场**从当前状态机取产物 | 元数据说"状态停在 I"，内容却是 J>I 的状态 → follower 把 I..J 的 op **当没做过**（丢）或**再 apply 一次**（重复） |
| 先 `compact` 再取产物 | 同上（取到的产物已含压缩点之后的 op） |

正确做法（`compact_applied`）：**在应用线程里、先取产物、再截日志**。
单测 `snapshot_data_matches_compacted_index_not_current_state` 专门钉住它 —— 断言方式刻意"绕开状态机计数"：
用**与 op 计数无关**的索引（10）压缩，随后继续推进状态机，再断言产物**不含**之后的批次。

### 42.3 另一个硬发现：**raft 索引归副本层**，不是状态机的 op 计数

`CatalogState::last_applied` 记的是"已 apply 的 **op 数**"，而 raft 日志里 **no-op**（新 leader 就位会写一条空条目）与 **ConfChange** 都占索引 —— 两者每遇到一条非 op 条目就**错开一格**。

用 op 计数当压缩坐标的后果同样静默：压缩位置偏小 → 元数据与内容不一致；
安装快照后 `advance_apply_to(applied)` 拿到错索引 → 重放已应用条目。

因此本轮把已应用位置放在**副本层**（`MetaStorage.applied_index`，由驱动方按 `entry.index` 设置，
且**对含 no-op/ConfChange 在内的每个条目都报**），并在用例里直接断言
`compacted == cluster.applied(leader)`（两个都是 raft 索引）—— 用错坐标这条断言立刻红。

> ⚠️ 由此留下**两个时钟并存**：快照**载荷**里的 `applied` 仍是状态机 op 计数，
> 快照**元数据** index 是 raft 索引。单测里两者都被显式断言（把现状写清楚，而不是含糊过去）。
> **S3-3 必须统一**：`CatalogState::last_applied` 作为读栅栏（`read_index()`）必须可比于 raft 索引
> —— 要么它也按 `entry.index` 设置，要么读栅栏改走副本层。

### 42.4 集成层：重启语义（**已确定**）+ 快照触发（**未稳定**）

**先说结论**：本轮把"快照安装"的**机制**钉住了，但**没能**在 3 节点 PoC 里**稳定地强制**走快照路径。
两件事必须分开算账（上一版草稿把它们混成"判据 5 通过"，已改正）。

**（a）已确定：重启必须把已应用位置告诉 raft**

用例 `follower_restart_recovers_from_own_log_and_converges`：杀一个 follower（**保留其日志**）→
继续写 → leader 压缩 → victim 带自己的日志重启 → 断言**与 leader 逐字节相同** + 还能继续跟随。

它钉住的是 `Config.applied`：raft-rs 文档原话 —— *"If Applied is unset when restarting,
raft might return previous applied entries"*。不设置时 raft 会**重放已应用条目**，后果**不报错**：
状态机的计数器被重复推进（幂等 op 的状态不变，但 `last_applied`/版本号多走），重启过的副本与其他副本
**静默分叉**。本轮就是被 `set_applied` 的单调断言当场拦下的（`已应用索引不得回退：5 -> 1`）。
**反证**：把 `applied` 改成 0 → 该断言立刻触发、用例失败 ✓。

**（b）未稳定：leader 是否发快照**

同一场景下实测出现过 `snapshot_installs=0 但已收敛`。取证（`MetaStorage` 的 env-gated 轨迹 + 节点诊断 dump）：

| 观察到的事实 | 说明 |
|---|---|
| leader 侧 `first=8 last=7`、`compact applied=7` | 日志**确实**压缩了（不是"没压成"） |
| **没有** `send_snapshot?` 轨迹 | `Storage::snapshot()` **从未被调用** —— leader 没走快照分支 |
| victim 收到 `append 6..7` | leader 把已被压缩的 6..7 当成可用条目发了出去 ✗ 与"first=8"矛盾 |
| victim 状态与 leader 一致、`installs=0` | 因此它靠条目补齐，而非快照 |

结论：**归因未完成**（`§42.7` 遗留 1）。已排除：日志未截断、陈旧快照（无 `recv_snapshot` 轨迹）、
"空存储同 id 重启"（那是另一条非法路径，见 42.5）。

### 42.5 两条"非法场景"与两次反证

**非法场景 ①：同 id + 抹掉存储重启。** 第一版用例就是这么写的，结果撞上 raft 的断言：

```text
to_commit 5 is out of range [last_index 0]   （etcd 那句 "Was the raft log corrupted, truncated, or lost?"）
```

原因：leader 侧仍记着该 peer 的旧 `matched`，于是发 `commit=N` 的**心跳**；空日志的 follower 在
`handleHeartbeat` 里无条件 `commit_to(N)` → 越界即 panic。

**结论（影响 S3-6 运维）**：**掉盘的节点不能复用原 id 直接空启**。要么从备份/快照恢复后回来，
要么以**新 id 重新加入**（成员变更）。这条比"能跑通"更值得记 —— 生产上很容易踩。

**非法场景 ②（本轮自己抓到）**：第一次跑"快照"用例时 `状态一致=true` 但 `installs=0` ——
计数器字段与 getter 都写了，**忘了在 `apply_snapshot` 里自增**。当时那条断言是发现它的唯一地方。
教训：**凡是断言依赖的计数器，都必须有测试证明它真的会被触发**（否则断言等于空转）。

**反证 A（存储层机制）**：把 Ready 循环里的"状态机 ← 快照"去掉（只装元数据、不还原状态机 ——
这正是 `MemStorage` 的静默形态）→ 用例失败：`installs=1 状态一致=false`。已恢复。
**反证 B（重启语义）**：把 `Config.applied` 改成 0 → `已应用索引不得回退：5 -> 1`，用例失败 ✓。

### 42.6 选型闸门：4 项通过 + 第 5 项**机制通过、集成层未稳定**

| # | 判据 | 结果 |
|---|---|---|
| 1 | 三节点选主 + 收敛 | ✅ §40 |
| 2 | kill leader 不丢已提交 | ✅ §40 |
| 3 | 状态机接缝干净 | ✅ §40 |
| 4 | 样板规模可接受（124 行） | ✅ §40 |
| 5 | 快照可安装 | 🟡 **机制已证明**（自写 `Storage` + 帧/载荷双重校验 + 安装路径 + 反证）；**集成层强制触发未稳定**（42.4b，遗留 1） |

**结论不变：保留 raft-rs**。逃生门现在有更实的信息：真正贵的是**自写 `Storage` + 快照正确性**
（本项目必须自己写，两个候选都躲不掉），而不是某个库的样板量。

**诚实说明**：判据 5 我**没有**拿到"稳定复现的快照安装"这颗证据就收尾了。原因是本 PoC 的
3 节点进程内 harness 缺乏"新节点加入"能力（成员变更属 S3-6），而用"保留日志的 follower"去逼快照
依赖 leader 的 Progress 时序 —— 这条路我试了 4 种变体仍未稳定，继续投入的性价比低于**把它登记清楚**。
机制层的证据（存储层单测 + 反证）是充分的；集成层的强制路径留给 S3-3（那里有 fjall + 真实触发策略，
会自然产生"落后节点追快照"的路径）。

### 42.7 遗留

| # | 项 | 归属 |
|---|---|---|
| 1 | **leader 为何能发出已被压缩的条目（`append 6..7` 而 `first=8`）** | **S3-3** —— 需带时间戳的 raft 事件轨迹（`Ready`/`MsgAppend` 收发 + `raft_log` 视图）才能定论。已排除的假设见 42.4b 表 |
| 2 | **两个时钟统一**（`CatalogState.last_applied` = raft 索引，或读栅栏改走副本层） | S3-3（阻塞"读旧窗口"验收） |
| 3 | fjall 落盘版 `Storage`（结构不变，换 `MetaInner` 的持久化） | S3-3 |
| 4 | 快照**触发策略**（§4.4：日志条数 > N / 状态 > M）+ **保留策略**（按表 checkpoint + 归档） | S3-3（R3-4 快照膨胀） |
| 5 | 大快照规模（10 万条 manifest 的传输/内存曲线） | S3-6 |
| 6 | 成员变更（learner → voter）—— **也是稳定复现"新节点追快照"的正路** | S3-6 |

---

---

## 43. R3 S3-3（第一件）：fjall 落盘版 `Storage` —— 把不变量交给存储保证（2026-09-19）

### 43.0 交付

| 位置 | 内容 |
|---|---|
| `crates/meta/src/fjall_storage.rs`（**新**） | `FjallStorage`：raft `Storage` 的落盘实现 + 5 条测试 |
| `crates/meta/Cargo.toml` | `fjall = "3"` |

**与内存版的关系**：内存版（`MetaStorage`）是**语义的定义处**（单测把它钉死），本实现逐条对齐它 ——
两个文件的方法名与不变量注释刻意保持一致，便于对读。差别只有"真相在哪"：

| | 内存版 | fjall 版 |
|---|---|---|
| 真相 | 进程内存 | **fjall**（每节点一个目录） |
| 崩溃后 | 全丢（PoC 用它换测试速度与确定性） | 从盘恢复，含 `applied_index` → 喂 `Config.applied` |
| PoC 是否用 | ✅（快、无 IO 抖动） | ⏳ 节点装配待进程化（S3-3 后续） |

### 43.1 三个设计决定

**① 先盘、后缓存。** 每一步都**先写 fjall 成功、再更新缓存**。反过来会让缓存领先于盘，
崩溃后就出现"内存说有一份快照、盘上没有"—— 最难查的一类不一致。

**② 压缩用 `batch(...).durability(SyncAll)` —— 让"不变量"变成"存储保证"。**

压缩要同时做四件事：写新产物、更新压缩位置与任期、删被覆盖的日志、删旧产物。
逐条写的话，**任意中间一刻掉电**都会留下"索引指向不存在/对不上的产物"。同一原子批里做完，
就变成要么全成、要么全不成 —— 上一轮靠**应用层纪律**维持的不变量
（"产物必须与 `compacted_index` 严格对应"，`§42.2`），在这里由**存储层**兜住。
这比"每处都记得小心"可靠得多。

**③ fsync 分级**（不是所有写都值得付 fsync）：

| 写 | 模式 | 理由 |
|---|---|---|
| `set_hard_state`（term/vote） | **SyncAll** | 投过票却没落盘 → 重启可能重复投票 → 破坏"一任期一票" |
| `append` | **SyncAll** | 日志是恢复的权威来源 |
| `apply_snapshot` / `compact_applied` | **SyncAll** | 同 + 不变量 |
| `set_commit` | `Buffer` | commit 是**派生**值：重启后由 term/vote + 日志重新推出 |
| `set_applied` | `Buffer` | 只影响"从哪继续 apply"；保守重放是安全的（幂等） |

### 43.2 键布局

```text
"cs"  ConfState        "hs"  HardState        "ap"  applied_index
"ci" / "ct"  compacted_index / compacted_term  "li"  last_index
"L" + idx(u64 BE)  日志条目      "S" + idx(u64 BE)  快照产物（只保留最新一份）
```

首日志索引由 `ci + 1` 推出（压缩时把 ≤ ci 的条目真删掉），所以只存最后一个索引，
避免启动时全表扫描。索引用**大端**：范围扫描即日志序。

### 43.3 测试（5 条，含三条"跨 reopen"）

| 用例 | 钉住什么 |
|---|---|
| `log_hardstate_and_applied_survive_reopen` | 日志两端/任期、`vote`（重复投票防线）、成员表、**`applied`**（要喂 `Config.applied`）跨重启存活 |
| `artifact_and_compacted_index_stay_consistent_across_reopen` | 压缩 → **重开** → `snapshot()` 返回压缩那一刻的产物（含 b1、**不含** b2）、压缩掉的日志真的没了、`entries` 边界正确 |
| `snapshot_is_not_faked_when_artifact_missing` | 白盒删掉产物（模拟盘上不一致）→ 必须报**可重试**，**绝不**返回对不上的快照 |
| `stale_snapshot_is_ignored_and_old_artifact_dropped` | 旧快照忽略且不计入安装数；旧产物被删（无界增长防线） |
| `overwrite_truncates_old_tail` | raft 允许的"截断重写尾部"不留旧条目残留、不产生空洞 |

**反证**：把 `snapshot()` 改成"现场从当前状态机取产物"（正是 `§42.2` 记录的错误写法）
→ 两条用例失败，报 `产物必须是压缩那一刻的（含 b1、不含 b2）`。已恢复。

### 43.4 边界与遗留

| # | 项 | 归属 |
|---|---|---|
| 1 | **节点装配**：让 metanode 进程用 `FjallStorage`（现 PoC 仍用内存版） | S3-3 后续 |
| 2 | **真崩溃测试**：本轮的"重启"是 drop 后 reopen（干净关闭）。真掉电/`kill -9` 需要**子进程**级注入（参照 `§31` 的 chaos 手法） | S3-3 后续 |
| 3 | **合并 fsync**：现在每个 Ready 一次 fsync。metanode 提交速率是"文件数/秒"量级，够用；合并留给压测后再定 | S3-6 |
| 4 | fjall 调优（LSM 参数 / 是否分离大 value）：快照产物是大 value，值得按 `KvSeparationOptions` 调 | S3-6 |
| 5 | `memtable`/compaction 对 **P99 持久化延迟**的影响（metanode 的 ack 依赖 fsync） | S3-6 |
| 6 | leader 为何能发出已被压缩的条目（`§42.7` 遗留 1） | S3-3 后续 |

---

---

## 44. R3 S3-3（第二件）：节点装配到落盘存储 —— **M3 的 G1 判据通过**（2026-09-19）

### 44.0 交付

| 位置 | 内容 |
|---|---|
| `crates/meta/src/lib.rs` | 集群 harness 从**内存版**切到 **fjall 版**；`kill` = 释放存储句柄（含 fjall 目录锁）、`restart` = **从盘重开**；持久化失败一律 `fatal`（停机） |
| `crates/meta/src/fjall_storage.rs` | `open_with_state`（**状态机从盘重建**）+ `reset_applied`（见 44.2 发现 1） |
| `crates/meta/tests/raft_poc.rs` | **M3 判据**：`full_cluster_restart_keeps_state_byte_identical` |

**为什么要切**：内存版是**语义的定义处**（单测把不变量钉死），但用它做集群就无法验证真正重要的一条
——**进程重启后状态从盘上重建**。切过去之后，`kill` 再 `restart` 就是**真的崩溃恢复**：
进程没了，只剩盘上的文件。

### 44.1 M3 的 G1 判据：全量重启后 Catalog **逐字节一致**

用例 `full_cluster_restart_keeps_state_byte_identical`：写 op → 全部收敛 → **在 leader 上压缩一次**
→ 记下三个节点的规范编码 → **杀掉全部节点** → 全部从各自目录重启 → 断言**逐字节等于重启前**
→ 再写一笔，确认**还能继续服务**。

它一次压到四件事：

| # | 压到什么 | 错了会怎样 |
|---|---|---|
| 1 | 日志与硬状态真落盘（`SyncAll`） | 重启后数据倒退 |
| 2 | 状态机由盘上**重建**（快照 + 其后日志重放） | 状态凭空少一半（或整份空） |
| 3 | `Config.applied` 取对（= **快照 index**） | raft 重放已应用条目 → 计数器多走 → **静默分叉** |
| 4 | 成员表恢复 | 重启后组不成立（选不出 leader） |

刻意**先在 leader 上压缩**：于是 leader 走「从快照恢复 + 重放其后条目」，其余节点走
「空状态机 + 全量日志重放」—— **两条重建路径都被覆盖**。
（这也顺带在**恢复路径**上补了 §42.4b 那个「怎么稳定走到快照」的遗留：发送路径仍不稳，
但**恢复路径**现在有稳定证据了。）

**反证**：把「从快照恢复状态机」改成「一律空状态机」→ 用例失败：
`全量重启后状态与重启前不一致（M3 的 G1 不成立）`。已恢复。

### 44.2 落盘之后立刻暴露的两个真问题

**发现 1：`applied_index` 是「随状态机走的派生量」，不能当权威持久化状态。**

盘上留着的 `applied` 属于**上一个进程那份内存状态**（「我当时应用到 5」）。而重启后状态机是
**重建**出来的（从快照，或从空 + 日志重放），它的位置由**快照 index** 决定，与旧值无关。
沿用旧值就会撞上单调断言 —— 实测原话：

```text
已应用索引不得回退：5 -> 1
```

修法：`open_with_state` 里按权威链**重置** `applied`（`reset_applied`，只给启动路径用）。
**权威链只有一条**：

> **快照（index + 产物）→ 其后日志重放**。

也从这里**修正/细化了 `§42.4a` 的结论**：`Config.applied` 应取 **`snapshot_index()`**，不是
`storage.applied_index()` —— 快照 index = 0 时它就是 0，raft 会从日志第 1 条重放，正是重建所需。

**发现 2：持久化失败必须停机，不能带病继续。**

落盘版的每个写都会返回 `Result`（内存版是无条件成功，所以这个分支以前不存在）。
把它处理成「忽略/重试」是最危险的写法：raft 层「已持久化」的假设一旦被打破，继续跑就会把
「已 ack 但没落盘」的数据当成安全的。所以统一走 `fatal(id, what, e) -> !`：记录 + 停机。
真实实现把错误交给上层（退出码），由运维决定恢复动作。

### 44.3 §42.7 遗留 1 的新证据（仍未归因，但更具体了）

切到落盘版后，那条「leader 发出了已被压缩的条目」的现象有了新观察：受害节点的**盘上确实有 6..7**
（所以它重启后能靠自己的日志/快照推出完整状态），而 leader 侧对它的 `matched` 停在 5。
即：**发送方认为没送到、接收方盘上却有** —— 这更像是我这个 PoC harness 的消息/响应路径问题
（不是 raft 语义问题）。

### 44.4 遗留

| # | 项 | 归属 |
|---|---|---|
| 1 | **真崩溃注入**：现在 `kill` 是线程退出（进程内）。真 `kill -9` / 掉电需要**子进程**级注入（参照 `§31` 的 chaos 手法）；fjall 有目录锁 ⇒ 必须等子进程死后才允许重开 | S3-3 后续 |
| 2 | 两类时钟统一（快照载荷里的 `applied` 仍是状态机 op 计数；读栅栏要可比于 raft 索引） | S3-3 后续（阻塞「读旧窗口」验收） |
| 3 | §42.7 遗留 1（leader 发已压缩条目），与 44.3 的证据一起查 | S3-3 后续 |
| 4 | 合并 fsync / fjall 调优（大 value 分离）/ P99 持久化延迟 | S3-6 |

---

---

## 45. R3 S3-0：gRPC 面落地（proto + tonic-build）+ 逐字段兼容测试（2026-09-19）

### 45.0 交付

| 位置 | 内容 |
|---|---|
| `crates/proto/proto/meta.proto`（**新**） | `Meta` 服务（Propose/Prefetch/Delta/Status/Join）+ op 面（本轮迁移 3 个 op） |
| `crates/proto/build.rs`（**新**） | `tonic-prost-build` 生成 server + client |
| `crates/proto/tests/wire_compat.rs`（**新**，6 条） | 设计 §6 的 S3-0 验收：round-trip + **与 `CommitFilesRequest` 逐字段对齐** |

**验收对照（设计 §6）**：

| 设计要求 | 本轮结果 |
|---|---|
| 编解码 round-trip | ✅ 每个 op 分支 + 请求/响应/状态消息 |
| 与现有 `CommitFilesRequest` **字段逐个对齐** | ✅ `commit_files_is_field_for_field_lossless`（9 + 17 + 4 个字段逐一断言，含 `stats` 的有/无两种形态） |
| 回滚点：保留手写 struct（双份并存一个 commit） | ✅ 手写结构**一个没动**，接口面独立 |

### 45.1 一个依赖拆包坑（升级 tonic 时会撞同一类）

crate 里原写着 `tonic-build = "0.12"`、`tonic = "0.14"`。tonic **0.14 把 prost 支持拆出去了**：
运行期是 `tonic-prost`、build 期是 `tonic-prost-build`。于是生成代码引用了
`tonic::codec::ProstCodec`（0.14 已无此路径）→ 编译失败：

```text
error[E0433]: cannot find `ProstCodec` in `codec`
```

修法：build-dep 换 `tonic-prost-build = "0.14"`、运行期加 `tonic-prost = "0.14"`。
**教训**：`tonic` 与 `tonic-build` 的版本必须成对升级，且 0.14 起要认识这两个拆出来的包。

### 45.2 设计层面的两个发现（决定了 proto 怎么写）

**① `ops.rs` 的请求结构是普通 Rust 结构，不是 prost 消息。**
所以接口面**不能**"把手写结构编码成 bytes 塞进去"（那种做法看起来很省事，却没有来源）。
必须**逐字段镜像**成 proto —— 这正是设计 §6 说"逐字段对齐"的原因。

**② WAL 的 `Record` 不能当 op 编码复用。**
它是既有的、已被测试覆盖的 op 编码，看起来很诱人；但 `DdlPayload` 只覆盖
`CreateTable/DropTable/CreateSchema/DropSchema`，**没有 `EvolveSchema`**，也没有文件提交
（`Batch*` 是数据面批次生命周期，不是 catalog 提交）。所以 metanode 的 op 面只能自己定。

**③ 未迁移的 op 不放 `bytes` 占位。**
留一个没有编解码的 `bytes` 字段会让下一位实现者以为"格式已经有了"。所以未迁移的 op
**先不进 proto**，并在文件头与 `wire_compat.rs` 里显式登记进度（分支数变化必须更新断言）。

### 45.3 测试写法：**逐字段断言**而不是整体相等

`CommitFilesRequest` 没实现 `PartialEq`（手写结构），所以测试逐字段比 —— 这反而更贴题：
断言名里写着"哪个字段丢了"。它的价值**当场兑现**：第一版镜像漏了 `seal_reason` 与
`seal_pressure`（T8/封口原因两轮加的可观测性字段），测试直接报 `files[0].seal_reason 丢了`。

**反证**：把 `to_manifest` 里的 `seal_reason` 改成空串 → 失败（`files[0].seal_reason 丢了`）。已恢复。

### 45.4 迁移进度（**诚实登记**）

| op | 状态 |
|---|---|
| `CreateSchema` / `DropTable` / `CommitFiles`（含 `FileManifest`/统计两结构） | ✅ |
| `CreateTable`（`partition_cols` / `ingest_config` 的形状待定）/ `EvolveSchema`（`SchemaChange` 三个变体）/ `DropShard` / `Compaction` / 租约 | ⏳ 后续增量 |
| `ProposeResponse.result` 的逐 op 形状 | S3-3（与 `MetaService` 实现一起定形） |
| `PrefetchResponse.payload` 的形状 | S3-4 |

### 45.5 遗留

| # | 项 | 归属 |
|---|---|---|
| 1 | 其余 op 的镜像 + 生产侧映射（`yuntun-meta` 内的边界转换器，现暂在测试里做原型） | S3-3 |
| 2 | `protoc` 依赖：本轮环境有 `libprotoc 36.1`；CI/新机器需装 `protobuf-compiler` 或改用 `protoc-bin-vendored` | S3-3 前 |
| 3 | 错误码映射（约定 3：`FAILED_PRECONDITION` / `UNAVAILABLE` + leader hint）需要 `MetaService` 实现时逐条落 | S3-3 |

---

---

## 46. R3 S3-3（第三件）：op 生产路径接线 —— proto `Op` 成为唯一权威编码（2026-09-19）

### 46.0 交付

| 位置 | 内容 |
|---|---|
| `crates/meta/src/op.rs`（**新**） | `StateOp` + `decode_op`（proto `Op` → 进程内）+ `apply`（纯函数）+ **双向**边界转换器 |
| `crates/meta/src/error.rs`（**新**） | `MetaError` + **gRPC 错误码映射**（约定 3）+ `retryable()` |
| `crates/meta/src/lib.rs` | 集群的 op 路径换成 proto：`Command::Propose{op: meta::Op, reply: ProposeResponse \| MetaError}`；日志 payload = **proto `Op` 编码**；`Cluster::propose` 返回 `ProposeResponse` |
| `crates/proto/proto/meta.proto` | 补 `CreateTableOp`（迁移进度 4/… ） |

**并且**：三个集群用例已改为直接构造 proto `Op`（不再是测试专用的 op 编码）——
也就是说 `converges` / `kill leader` / **M3 全量重启** 这些用例现在跑的就是**生产路径**。

### 46.1 三个决定

**① 日志 payload 必须与 gRPC 面同构，所以删掉了 PoC 的文本编码（`PocOp`）。**
留着它会变成"线上发的"与"盘上存的"两套定义 —— 换主或跨版本重启时就是灾难。
现在日志里存的就是 proto `Op` 的编码：**一份定义、一处演进**。

**② `now_ms` 放在 op 上，不是 `apply()` 的参数。**
后者会诱导实现者在里面取"现在"（S3-2 抓到的四类非确定性之一）。放在 op 上，
时间随 op 传播，各副本 apply 同一串 op 得到同一状态 —— 且这条有测试守着。

**③ 状态机内的 op 级计数与副本层坐标**（呼应 `§42.3`）**别混用**：
`storage.set_applied(entry.index)` 维护的是**副本层**（raft 索引，压缩/快照元数据用它），
而 `CatalogState.last_applied` 仍是 **op 计数**（standalone 语义）。代码里点明了这一点，
免得后来者以为两者是一回事。

### 46.2 实测撞到的一个真问题：幂等判断必须用**归一化表身份**

`CreateTable` 的幂等判断原本写成 `state.get_table(&request.name)` —— 而请求里的 `name`
可能是裸名（`cpu`），状态机内部存的是全限定名（`public.cpu`）→ **永远查不到** →
重复建表不再是 `accepted=false`（幂等命中），而是 `TableAlreadyExists` **错误**。
幂等语义要求前者（客户端重试必须成功）。已按状态机的归一化规则（裸名补 `namespace`）
对齐。**这类"身份没归一化"的错，只有在重复提交时才会暴露** —— 也就是崩溃重试那条路。

### 46.3 错误码映射（约定 3 的落地）

| 情形 | 码 | 可重试 | 附加信息 |
|---|---|---|---|
| 非 leader | `UNAVAILABLE` | ✅ | metadata `leader-hint` |
| 无 quorum / 等应用超时 | `UNAVAILABLE` | ✅ | — |
| OCC 版本不符（含 `SchemaChanged`） | `FAILED_PRECONDITION` | ✅ | metadata `actual-version` |
| 请求不合法（op 解不开、键缺失/too long） | `INVALID_ARGUMENT` | ❌ | — |
| 表/schema 不存在 | `NOT_FOUND` | ❌ | — |
| 已存在 | `ALREADY_EXISTS` | ❌ | — |
| 背压（内存/磁盘水位） | `RESOURCE_EXHAUSTED` | ✅（退避） | — |
| 落盘/内部故障 | `INTERNAL` | ❌ | 节点应**停机** |

映射用**穷尽 `match`**（新加错误变体若不映射，编译就过不去），另有一条用例专门遍历
若干错误断言"绝不能映射成 `Ok`"。

### 46.4 一条被我写错的测试（记录下来）

我原本想用 `CreateSchema` 证明"`now_ms` 进状态"，结果**失败**：
`create_schema` **不记录时间**（schema 注册表只有名字）→ 两个不同时间戳的状态**相同**，
断言自然不成立。改用 `commit_files`（幂等记录里落 `committed_at`）后，且加了反向断言
（**同一时刻**的同一 op 必须得到同一状态）——后者顺带证明"状态机里没读钟"。

教训：**要证明"A 进入状态"，得先确认 A 在该路径上真的被记录**；否则测的是空气。

### 46.5 遗留

| # | 项 | 归属 |
|---|---|---|
| 1 | `MetaService`（tonic trait 实现）+ 服务启动/`--init` bootstrap + 真 gRPC 端到端用例 | S3-3 下一步（**RPC 面与 op 路径都已就绪**） |
| 2 | 其余 op 的镜像（`EvolveSchema` 三个变体 / `DropShard` / `Compaction` / 租约） | S3-3 |
| 3 | `ProposeResponse.result` 的逐 op 形状 | S3-3 |
| 4 | 生产侧把 `NodeHandle` 暴露给 gRPC 层（现集群内部结构已具备） | S3-3 下一步 |

---

---

## 47. R3 S3-3（第四件）：`MetaService` 落地 —— metanode 可被远端调用（2026-09-19）

### 47.0 交付

| 位置 | 内容 |
|---|---|
| `crates/meta/src/service.rs`（**新**） | `MetaService`（tonic 实现）+ `serve()`（可绑定 `127.0.0.1:0` 让内核分配端口） |
| `crates/meta/src/lib.rs` | `NodeStatus`（**结构化**节点状态）+ `NodeHandle`（提案/状态/增量）+ `Cluster::handle()` |
| `crates/meta/tests/meta_service_e2e.rs`（**新**） | **真** gRPC 端到端：TCP + HTTP/2 + raft + 状态机 + 回包 |

至此 metanode 的链路是通的：`MetaClient` → HTTP/2 → `MetaService` → `NodeHandle` → raft 提案
→ 应用到 `CatalogState` → `ProposeResponse` 回客户端。**RPC 面 + op 路径 + 服务层都齐了**。

### 47.1 三条写进代码的纪律

**① 阻塞的提案必须丢进阻塞线程池。**
raft 的"等应用到状态机"是**阻塞**的（`recv_timeout`）。直接在一个 `async fn` 里等，会占住 tokio
工作线程 —— **一个被拖住的提案就能把整个服务拖停**（连 `Status` 这种快读都进不来）。
所以 `propose` 走 `tokio::task::spawn_blocking`；`status`/`delta` 只查内存，留在 async 里。

**② 结构化状态与诊断串分开。**
`NodeStatus`（字段）给程序读，`debug: String` 给人读。**别去 parse 诊断串** ——
那个字符串的格式会随打印习惯改名，而调用方不会知道。

**③ 未实现的方法必须明确 `UNIMPLEMENTED`，不许假装成功。**
`Prefetch`（载荷形状属 S3-4）与 `Join`（成员变更属 S3-6）都返回 `Unimplemented`，
并说明"归属哪一步"。用例专门断言这一点（**反证**：让 `Join` 返回一个空的成功响应 →
用例立刻红，客户端会以为成员已变更）。

### 47.2 端到端用例覆盖了什么

| 交互 | 断言 |
|---|---|
| 正常写入（建 schema / 建表 / 提交） | `accepted=true`、回包带 **raft index** 与两组版本号 |
| **幂等重试**（同键同 batch 再提一次） | HTTP 成功但 `accepted=false`（**不是**错误 —— 客户端重试必须成功） |
| `Status` | `node_id`/`leader_id` 指向 leader、`last_index >= applied_index >= 0`、版本串一致 |
| `Delta`（`since=0`） | 报出变过的表（`public.cpu`） |
| `Prefetch` / `Join` | `UNIMPLEMENTED`（不许假装成功） |

### 47.3 leader hint 现在是真的

提案路径的非 leader 分支改为回报 `raw.raft.leader_id`（而不是 0）；集群层找不到 leader 时
也会尽力从各节点状态里挑一个已知 hint。客户端据此**直接重试到正确节点**（约定 3 的可重试性
才有意义 —— 否则"可重试"等于"盲试"）。

### 47.4 遗留

| # | 项 | 归属 |
|---|---|---|
| 1 | **CLI / `--init` bootstrap**：现在能"程序内起服务"，还不能"作为进程独立启动并指定端口/目录" | S3-3 收尾 |
| 2 | 多节点部署形态（每节点地址表 / 静态成员 / 加入流程） | S3-3 收尾 + S3-6 |
| 3 | `Prefetch` 载荷形状 | S3-4 |
| 4 | `Join` 与成员变更 | S3-6 |
| 5 | 其余 op 的镜像（`EvolveSchema` 三变体 / `DropShard` / `Compaction` / 租约） | S3-3 收尾 |
| 6 | 真崩溃注入（子进程）/ 两个时钟统一 / 快照触发保留策略 | 见 §44.4 / §42.3 |

---

---

## 48. R3 S3-3（第五件）：全仓 CLI 统一到 clap —— metanode 可作**进程**独立启动（2026-09-19）

### 48.0 本轮两件事

| # | 事项 | 判据 |
|---|---|---|
| 1 | `metanode` 成为**可独立启动的进程**（CLI + `--init` + 单节点服务） | `tests/metanode_process_e2e.rs` **4 条**：真二进制 → 真 `kill -9` → 重启后**状态机恢复**；两条启动安全闸门；成员表不一致/多节点拒绝 |
| 2 | 全仓 **4 个 CLI 程序统一到 clap** | `metanode` / `yuntun-cli` / `yuntun` / `bench`+`bench_baseline` 示例；新增 3 套 CLI 契约测试（11 条） |

### 48.1 为什么改回 clap（我上一轮刚手写了一份，理由当场被推翻）

上一轮我为 `metanode` 手写了参数解析，写在文件头的理由是"参数少，引依赖不划算"。这是**局部**最优：
它只算了"把值取出来"这一件事，没算下面这些真实成本 ——

1. `--help`、拼错的 flag、`--flag=value`、缺值**都得自己兜**，而且是**每个程序各兜一遍**；
2. 全仓有 4 个 CLI 程序，各写一套 → help 文案与错误形式**必然漂移**（改一处忘一处，没人会发现）；
3. 手写版遇到拼错只回 `unknown arg: --wat` + 一行 usage —— **不指出是哪个参数、该给什么**。
   运维脚本靠错误信息定位问题，这属于**接口质量**，不是"锦上添花"。

结论：语法层统一交给 clap，**语义层仍归本 crate**（分工见 48.2）。

### 48.2 分工：clap 管**语法**，crate 管**语义**

| 层 | 谁管 | 例子 |
|---|---|---|
| 语法 | clap（`derive`） | 有哪些 flag、`--id` 是不是整数、`--listen` 能否解析成 `ip:port`、必填项缺没缺 |
| 语义 | `Args::normalize` / `Args::check_bootstrap` | 成员表必须含自己；空目录必须 `--init`；有数据的目录不许 `--init` |

语义错误也按**用法错**退出。**退出码分层是给编排脚本的接口**：

| 码 | 含义 | 脚本该怎么反应 |
|---|---|---|
| 0 | 正常（含 `--help`/`--version` —— 它们不是错误） | 继续 |
| 2 | **用法/配置错**（改参数就有救） | 报给人改参数 |
| 1 | **启动/运行失败**（环境/数据问题） | 查目录权限、目录锁、成员表 |

### 48.3 迁移时**刻意**保住的"接受的输入是原来的超集"

| 能力 | 原手写版 | clap 版怎么保住 |
|---|---|---|
| `--addr a` 与 `--addr=a` | 都支持 | clap 原生 |
| `YUNTUN_ADDR` 环境变量 | 支持 | `#[arg(env = "YUNTUN_ADDR")]` |
| `--addr` 放在子命令**前或后** | 支持（全 argv 扫描） | `global = true` |
| `--format` 大小写 | 不敏感 | `ignore_case = true` |
| `--format json` / `ndjson` 等价 `jsonl` | 支持（`InputFormat::parse`） | `#[value(alias = "json", alias = "ndjson")]` |
| `bench_baseline` 的 **9 个位置参数顺序** | 固定顺序 | 顺序**逐一对应**（文档与历史命令不用改） |
| `YUNTUN_BENCH_NO_READER` | 环境变量 | 保留 + 新增 `--no-reader`（等价，且能 `--help` 看到） |

**教训**：迁移解析器最危险的不是"新写法不能用"，而是**静默收窄**接受的输入 ——
用户脚本里某个别名突然失效，报的却是"参数非法"（他会去查自己的脚本，而不是查我们的迁移）。
所以先把"原来接受什么"逐条列出来，再用 clap 的特性逐条保住，**并用测试钉住**（48.5）。

### 48.4 实测撞到的三个坑

| # | 坑 | 现象 | 教训 |
|---|---|---|---|
| 1 | **`--init` 的成功路径是"持续服务"** | 进程级测试里用 `.output()` 等它退出 → **永久挂住**（我是在 60s 无输出时才注意到） | 长驻进程的测试必须"起 → 等就绪（读它打印的地址）→ 停"；`.output()` 只适用于**会退出**的命令 |
| 2 | `[workspace.dependencies]` 加了 clap，`Cargo.lock` 里却没有 | `cargo fetch` 不报错、也没拉包 | workspace 依赖只有**被某个 crate 引用**后才参与解析 |
| 3 | （环境）链接器 `ld terminated with signal 7 [Bus error]` | 全量测试 `exit=101`，日志里**没有任何编译错误** | 这是环境偶发（磁盘 78G 可用、内存 25G 可用都充足），**重试即过**（287 passed）；别照着它改代码 |

### 48.5 交付物与验证

| 位置 | 内容 |
|---|---|
| `crates/meta/src/cli.rs` | clap derive 参数 + `normalize`（成员表自洽）+ `check_bootstrap`（两条启动闸门）+ **8 条单测** |
| `crates/meta/src/lib.rs` | `MetaNode`：进程运行时；**三道启动检查**（存储可开 / 盘上成员表与配置一致 / 只有单 voter）；与 `Cluster` **完全共用**同一条落盘与恢复链路 |
| `crates/meta/src/main.rs` | 薄入口：解析 → 起节点 → **等选主** → 起服务；打印 `metanode id=N listening on <addr> ...`（**这行是接口**：`--listen 127.0.0.1:0` 时靠它拿真实端口，改格式等于破坏调用方） |
| `crates/meta/tests/metanode_process_e2e.rs` | **4 条**：① `kill -9` 后状态机恢复；② 两条 `--init` 误用；③ 成员表不一致 / 多节点拒绝；④ clap 面（help/version/未知 flag） |
| `crates/client/tests/cli.rs`、`crates/standalone/tests/cli.rs` | CLI 契约：help 列出全部子命令与 flag、用法错退 2 且**指出参数名**、格式别名仍被接受、`--version` 输出格式 |

验证：`metanode` 4 + `yuntun-cli` 4 + `yuntun` 3 + `meta::cli` 8 全绿；
**全量 287 passed / 0 failed**；`yuntun-meta` clippy 0（全仓余 1 处历史告警：`chunk/store.rs`
的 `doc_lazy_continuation`，与本次无关）。

### 48.6 为什么"进程级恢复"的断言口径更强

`kill -9` 之后，状态机是**内存里的、已经没了** —— 它回来只能靠**从盘重放日志（或装快照）**。
所以用例的**第一条断言**是"同一个幂等键再提一次必须 `accepted=false`"：

- 它证明的是**盘上日志 + 恢复路径都对**（日志在、`Config.applied` 复位正确、重放按序 apply、
  幂等表重建），而不是"进程起来了"；
- 比 `Status` 里的 "applied_index 追平" **更强**：后者在"重放时走错分支"的情况下也可能成立
  （比如把已应用条目再应用一遍 —— 幂等 op 的状态不变，但计数器会多走）。

## 48.7 遗留

| # | 项 | 归属 |
|---|---|---|
| 1 | **多节点部署形态**：节点间 raft 消息的**网络传输**（现在单 voter 才能起，多 voter 会被明确拒绝而不是静默空转） | **S3-3 收尾（下一件）** |
| 2 | `Meta.Join` 的成员变更（learner → voter）与多节点 `--init` 引导 | S3-6 |
| 3 | 其余 op 的 proto 镜像（`result`/`payload` 逐 op 定形） | S3-0 余 |
| 4 | 两个时钟统一（`now_ms` 与状态机时钟） | S3-4 |
| 5 | 快照**触发与保留**策略（现在只有"按需压缩"） | S3-3 余 |

---

---

## 49. 清理：删除未接线的 `vendor/opensrv-mysql`（2026-09-19）

### 49.1 结论

`vendor/opensrv-mysql`（544K，`.gitignore` 已忽略、未被 git 跟踪）已删除。**零功能影响**，
两条独立证据：

| 证据 | 结果 |
|---|---|
| 是否被构建引用 | 全仓唯一的 opensrv 依赖是 `crates/sqlwire/Cargo.toml` 的 `opensrv-mysql = "0.7"`（**走 registry**）。根 `Cargo.toml`、`.cargo/`（不存在）、用户级 cargo 配置里**都没有** `[patch.crates-io]` / `paths` / `directory = "vendor"` 之类的接线 |
| 与上游是否一致 | `diff -rq` 与"文件清单 md5"**双双**显示它与 `~/.cargo/registry/src/*/opensrv-mysql-0.7.0` **逐字节相同** —— 即这份 vendor **连一处补丁都没有** |

### 49.2 为什么值得记（避免后来者误判）

`§22` 记着上游 opensrv-mysql **0.7.0** 的 `PacketReader::next_async` 释放后使用
（上游 #66 / PR #67，**只修在 git**，crates.io 仍是 2024-02 的 0.7.0）。看到 `vendor/opensrv-mysql`
很容易以为"我们 fork 了一份来打这个补丁"——**并不是**：`§22.3` 的绕过**全部在 `sqlwire`**
（`PacketFramedReader::poll_read` 按**包边界**截断），一行都没改 opensrv。

所以这次删除**不会**让 §22 的上游问题回来 —— 它从来没被这份 vendor 挡住过（删除前后，
构建用的都是 registry 上那份未打补丁的 0.7.0）。

若将来真要 patch 上游，正确形态是 **`[patch.crates-io]` + 在文档里写明补了什么**
（上游问题在 0.7 已定位到具体文件行，patch 是可做的；本次只是不做）。
`.gitignore` 的 `/vendor/` **保留**：真 vendor 时它仍是忽略目录，记得 `git add -f`，
否则补丁不会进仓库（那等于"本地修好了、别人拉下来还是坏的"）。

### 49.3 验证

`cargo test -p yuntun-sqlwire -p yuntun-server -p yuntun-standalone`：**30 passed / 0 failed**
（覆盖 MySQL wire 链路：握手 / prepared / 二进制行编码 / `SET`·`USE`·`SHOW` 拦截 / 错误码映射）；
`git status` 干净（该目录本就未跟踪）。

---

---

## 50. R3 S3-3 收尾：节点间 **gRPC 传输** —— 3 节点复制 + 换主不丢已提交（2026-09-19）

### 50.0 本轮结论

M3 的 G1 判据（**kill leader 自动选主、提交不丢失**）第一次跑在**真网络路径**上并通过：
`crates/meta/tests/multi_node_grpc_e2e.rs` 一条用例串起
**3 节点选主 → 经 gRPC 复制 → 三节点收敛 → 杀 leader → 重选 → 继续写 → 换主前的幂等键仍命中**（0.46s）。

至此 S3-3 的**运行形态**齐了：单节点可作进程独立启动（`§48`）、多节点经网络复制（本轮）、
崩溃后从盘恢复（`§44`/`§48`）。剩下的是"其余 op 的镜像 / 快照触发策略 / 成员变更 / 鉴权"（50.6）。

### 50.1 为什么传输得自己写；以及为什么用 gRPC 承载

raft-rs 只吐 `eraftpb::Message`，**传输是使用者的责任**（库文档明说）。选型对比：

| 方案 | 代价 |
|---|---|
| **gRPC（本轮）** | 每消息一次 HTTP/2 往返。心跳 3 tick ≈ 30ms，量级完全够；换来**一个端口**、复用 TLS/鉴权/可观测，不必再写一套分帧 + 握手 |
| 裸 TCP + 长度前缀 | 少一层开销，但**多一个端口**、多一套协议/分帧/超时，TLS 还得再写一遍 |

**判定**：先用 gRPC。唯一可能让它成为瓶颈的场景是"**大日志批量追赶**"（每秒上百 MB 的追加），
那时换成裸 TCP **只需替换 `transport.rs`** —— `spawn_node` 只依赖 `PeerTransport` trait，
raft 循环一行不用改（这正是本轮先抽 trait 的原因）。

### 50.2 传输语义与"必须留下信号"

| 性质 | raft 是否要求 | 本实现 |
|---|---|---|
| 不丢 | **不要求**（心跳/选举/追加按 tick 重发） | 队列满 → **丢** + 计数；**绝不阻塞** raft 线程 |
| 保序 | **不要求**（靠 term/index 自愈） | 不保证（多连接并发） |
| 不重 | 要求（重复会让 raft 反复 `step`） | gRPC 不重复；**我们不做重发** |

`send()` 非阻塞是硬要求：调用点在 **raft 线程**上，阻塞它 = 拖住 tick = 别人选你当 leader 时你没反应。

因此每 peer 一个后台发送任务 + 计数：

| 计数 | 含义 | 为什么要它 |
|---|---|---|
| `queued` | 成功进队列 | —— |
| `dropped` | 队列满/对端未知 | 区分"我们没发"与"网络丢了" |
| `failed` | RPC 失败/超时 | 对端不可达（重启中属正常） |
| `delivered` | 对端确认已入它的 raft 线程 | **唯一能证明"复制真走了网络"的信号** |
| `rejected` | 对端拒收（线程已退出） | 对端其实死了但连接还在 |

**"永远选不出 leader"是最难查的故障** —— 没有这几个数，就只能靠猜。

### 50.3 载荷为什么是**不透明 bytes**

`Meta.Raft` 的 `message` 字段直接装 `eraftpb::Message` 的 protobuf 编码，**不在 proto 里重写一遍**：
重写会得到"两份必然漂移的定义 + 每升一次 raft 就要同步改 proto"。
代价是它**不可自描述** → 跨版本混跑不能被 proto 挡住，要靠发布约束（登记为 50.6-③）。

### 50.4 三道启动检查 + 一个"提前到 CLI"的闸门

| # | 检查 | 不做会怎样 |
|---|---|---|
| 1 | 盘上成员表 == 启动参数里的成员表 | 各节点各自成组，数据永远合不回来 |
| 2 | 每个**别的 voter** 都有地址（缺哪个列哪个） | 发不出消息 → 永远选不出 leader，且**不报错** |
| 3 | 多节点时处于 tokio 运行时上下文（要起发送任务） | 启动即失败，但错误信息会含糊（现在明说"要在运行时内调用"） |

检查 ② 同时在 **CLI 层**拦一遍（退出码 2，提示可直接照做）。这不是重复：
CLI 层拦的是"人手打错"，`MetaNode::open` 拦的是"API 调用方漏了"。

**实测这条闸门立刻见效**：改完语义后，`cli::tests::explicit_voters_are_sorted_and_deduped`
（3 voter 却没给 `--peer`）当场变红 —— 老用例就是被新语义抓出来的。

### 50.5 反证（证明用例不是自说自话）

| 做法 | 结果 |
|---|---|
| 把各节点的 `--peer` 指到**错端口**（网络不通） | ✅ 用例在 30s 内以"没选出 leader"失败 |
| 用例内断言"各节点 `delivered` 之和 > 0" | ✅ 不成立就失败 —— 防"复制其实没走 gRPC 却过了"（比如误退回进程内邮箱） |

### 50.6 遗留

| # | 项 | 归属 |
|---|---|---|
| 1 | **成员变更**（`Meta.Join`：learner → voter）、加节点不重启 | S3-6 |
| 2 | **鉴权**：`Meta.Raft` 现在是**裸的** —— 任何能连上端口的人都能往里塞 raft 消息 | S3-6 / 部署（真实环境必须 mTLS 或内网隔离） |
| 3 | **版本混跑保护**：载荷不可自描述，跨版本节点混跑需要版本门（当前靠发布约束） | S3-6 |
| 4 | 大日志追赶的性能（HTTP/2 vs 裸 TCP）与抓包级观测 | 性能阶段 |
| 5 | 其余 op 的 proto 镜像 / 两个时钟统一 / 快照触发与保留策略 | S3-0 余 / S3-4 / S3-3 余 |

### 50.7 交付物

| 位置 | 内容 |
|---|---|
| `crates/proto/proto/meta.proto` | `rpc Raft(RaftRequest) returns (RaftResponse)`（`from`/`message` → `delivered`/`reason`） |
| `crates/meta/src/transport.rs`（新） | `PeerTransport` trait + `Mpsc`/`No`/`Grpc` 三个实现 + `TransportStats`（5 个计数） |
| `crates/meta/src/lib.rs` | `spawn_node` 改收 `Arc<dyn PeerTransport>`；**三处出站**（`take_messages`/`take_persisted_messages`/`light.take_messages`）全走它；`NodeHandle::deliver`（收件箱）；`MetaNode::open(.., peers)`、`transport_stats()`、`MetaNodeError::{MissingPeer,Transport}` |
| `crates/meta/src/service.rs` | `Meta.Raft` 处理：只做"解码 + 入箱"；解不开**报错**（不是 `delivered=false` 的软失败 —— 那是"协议不一致"被伪装成"对端拒收"） |
| `crates/meta/src/cli.rs` | `--peer <id>@<host:port>`（端点串校验、重复 id 拒绝、缺地址点名） |
| `crates/meta/tests/multi_node_grpc_e2e.rs`（新） | 3 节点真 gRPC：选主/复制/收敛/杀 leader/重选/继续写/幂等命中（**0.46s**） |

---

---

## 51. R3 S3-4（第一件）：读路径载荷定形 —— `Prefetch` 的语义与镜像（2026-09-20）

### 51.0 本轮结论

`§45.4` 明确归给 S3-4 的那一项（**`PrefetchResponse.payload` 的形状**）已落地，并**逐条有用例**：

| 语义 | 用例断言 |
|---|---|
| 只要版本号（`tables` 空）→ **零载荷** | `payload.tables.is_empty()` |
| 版本没变（`since` = 当前）→ 版本号原样回，客户端据此跳过刷新 | 版本相等 + 零载荷 |
| 差量（`tables` 非空）→ 只回**请求里存在**的表；请求了却不在 = **已删** | `[public.cpu, public.gone]` → 只回 `public.cpu` |
| `full=true` → 全部表 + `full_reload` | 载荷含全部表 |
| **版本超前**（客户端比本集群新）→ 全量 + `full_reload` | `since = 当前+100` → 仍要求重建 |

### 51.1 两条「为什么」（这套语义的错法都不报错，所以要写下来）

1. **「请求了却不在载荷里」必须解释为「已删」** —— 所以载荷**只**能包含存在的表，
   **不能**「补一个空占位」。补了客户端就再也分不清「我没请求它」与「它被删了」。
2. **版本超前必须要求全量重建**，不能静默返回空：静默返回空会让客户端以为「一切照旧」，
   继续拿一份**不属于本集群**的缓存去规划查询 —— 错得**不报错**。

### 51.2 实测抓到的真问题：载荷里的 `name` 必须是**全限定**

第一版转换直接搬 `model::TableMeta.name` —— 那是**裸名**（`cpu`），schema 在另一个字段里；
而契约里写的是全限定名（`public.cpu`）。用例当场变红：
**「请求 `public.cpu`，拿回名为 `cpu` 的条目」**。

后果不是「名字不好看」：载荷的消费者拿它当**缓存键**，裸名会让
`public.cpu` 与 `analytics.cpu` **撞成一条**（多 schema 下静默串表）。

修法：

- 编码：`name = m.qualified_name()`（全限定）+ `namespace = m.schema_name()`（冗余副本，供交叉校验）；
- 解码：`split_qualified(&name)` 还原，并**校验** `namespace` 与全限定名一致 ——
  不一致**报错**（两个字段写的是同一个事实，静默取一个会让「名字」与「namespace」指向不同的表）。

### 51.3 `ingest_config` 为什么用 proto3 `optional`

模型里是 `Option<IngestConfig>`，**存在性有语义**（「没配」≠「配了默认值」）。
用普通 `bytes` 会把 `None` 与「空配置」混成同一个值 ✗ →
改用 `optional bytes`（proto3 presence），并用一条「`None` 必须还原成 `None`」的用例钉住。

### 51.4 交付物与验证

| 位置 | 内容 |
|---|---|
| `crates/proto/proto/meta.proto` | `PrefetchRequest.tables`、`PrefetchPayload`（一层壳，便于以后加 manifest/文件级增量）、`TableMeta` 镜像 |
| `crates/meta/src/op.rs` | `table_meta_to_proto` / `table_meta_from_proto` + **无损 round-trip 用例**（含 `optional` 存在性与「namespace 不一致必须拒绝」） |
| `crates/meta/src/lib.rs` | `NodeHandle::prefetch`（落实语义：`full = req.full 或 版本超前`） |
| `crates/meta/src/service.rs` | `Prefetch` 从 `UNIMPLEMENTED` 变为实现（只读内存状态，不需要阻塞线程池） |
| `crates/meta/tests/meta_service_e2e.rs` | `prefetch_payload_semantics`（真 gRPC，五分支） |

**反证**：去掉「版本超前保护」（`let ahead = false`）→ 用例 ⑤ 立刻红
（客户端拿到「一切照旧」的空载荷 + `full_reload=false`）—— 正是要防的那种错。

**验证**：meta 全绿；全量 **292 passed / 1 failed**，失败的是**已登记的 chaos 并行 flake**
（全量里 37.5s，**隔离复跑 0.11s 通过**，`status.md` 已登记，根治属 T6.1）。

### 51.5 一个观测口径的坑（差点让我误判）

`cargo test --workspace` 的日志**会混**：被取消的命令留下的**后台 cargo 进程**
会与后一次运行**同时写同一份日志**。据此我一度以为「某个 target 失败」
（其实那个 target 是 passed，失败来自另一个进程的视角）。

**判据**：日志开头出现 `Blocking waiting for file lock on build directory` = 当时**不止一个 cargo** 在跑
→ 这份日志**不能**作为验收证据（本次实测：混过的日志 98 targets / 421 用例；干净全量是
**59 targets / 293 用例**）。

### 51.6 遗留

| # | 项 | 归属 |
|---|---|---|
| 1 | **`RemoteCatalog`（`CatalogOps` 的 gRPC 实现）+ standalone 装配** —— 判据：既有用例全绿、`if distributed` 分支为零 | **S3-4 第二件（下一件）** |
| 2 | 清单/文件级刷新进载荷（现在载荷只到「表元数据 + schema」，文件级走 `Delta`） | S3-4 余 |
| 3 | 「落后太多必须全量」的**保留窗口**判定（现在只有 `full=true` 与「超前」两种信号） | §6.3 保留策略 |
| 4 | 其余 op 的 proto 镜像（`DropSchema`/`EvolveSchema`/`DropShard`/`Compaction`） | S3-0 余 |

---

---

## 52. R3 S3-4（第二件·上半）：拆掉 catalog 接缝上的**具体类型**（2026-09-20）

### 52.0 本轮做了什么

把"装配层之外还能看见 `MemoryCatalog`"这件事，从**纪律**变成**类型**：

| 位置 | 之前 | 现在 |
|---|---|---|
| `Lakehouse.catalog` | `Arc<MemoryCatalog>` | **`Arc<dyn CatalogOps>`** |
| `collect_metrics` / `replay_wal_ddl` 的参数 | `&Arc<MemoryCatalog>` | `&Arc<dyn CatalogOps>` |
| 测试与夹具里的 `x.clone() as Arc<dyn CatalogOps>` | 转型（因为源是具体类型） | 已随之清理（只有"源类型确实是具体类型"的 4 处保留转型：chaos 夹具 / query 测试助手） |

**这一轮是纯类型级改动，零行为变化** —— 所以它的验收判据就是"**用例必须一模一样地全绿**"。

### 52.1 判据（S3-4 原话的两条）

| 判据 | 结果 |
|---|---|
| **既有用例全绿**（standalone 不回归） | ✅ **293 passed / 0 failed**（59 targets；日志无 `Blocking waiting for file lock` = 可采信，见 `§51.5`） |
| **`if distributed` 分支为零** | ✅ **代码命中 0**（只有两处**注释**提到这句话） |
| 生产代码里谁还知道具体实现 | ✅ 全仓**只有装配点一处**：`crates/server/src/lib.rs` 的 `let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());`。其余 `MemoryCatalog` 出现处全在**测试模块**或注释里 |

**为什么靠类型而不是靠纪律**：别处拿不到 `MemoryCatalog` —— 要拿得先在装配点 `as` 下去，一眼可见。
分布式形态切换时（下一件）改的就是那一行。

### 52.2 探明的"下一件要补什么"（这轮的真正价值）

量了一遍生产代码对 catalog 的**全部**方法依赖（`server`/`query`/`ingest`/`compaction`），结论：

- **全部落在 trait 上** ✅ —— 包括一开始担心的 `current_snapshot` / `known_batch_ids` /
  `check_idempotency` / `record_idempotency`（它们早就在 trait 上，`compaction` 与 `ingest` 一直在用）
  → **接缝没有"只有具体类型才有"的隐藏依赖**，这是最好的结果。
- 所以 `RemoteCatalog` 要覆盖的就是 `CatalogOps` 的 **21 个方法**，其中：
  | 类别 | 线上路径 | 状态 |
  |---|---|---|
  | 写（DDL/提交/分片/Compaction/幂等记录） | `Propose(op)` | 4 个 op 已镜像（CreateSchema/DropTable/CommitFiles/CreateTable）；**余** DropSchema/EvolveSchema/DropShard/Compaction/幂等记录 |
  | 读（版本/表元数据/schema/命名空间） | `Prefetch` / `Delta` | ✅ `§51` 已定形 |
  | 读（文件/清单级） | `Prefetch` 载荷（**待扩**） | ⏳ 载荷现在只到"表 + schema" |
  | 线性化读位置 | `Status.applied_index` | ⏳ 需在 `§5` 约定 4 的口径下定形（ReadIndex） |

### 52.3 遗留（下一件 = S3-4 第二件·下半）

| # | 项 | 说明 |
|---|---|---|
| 1 | `RemoteCatalog`（`CatalogOps` 的 gRPC 实现） | 写走 `Propose`、读走 `Prefetch`；按 52.2 的表补齐缺口 |
| 2 | 其余 op 镜像：`DropSchema`/`EvolveSchema`/`DropShard`/`Compaction`/幂等记录 | 机械工作，但**必须先有**（`RemoteCatalog` 的写路径要用） |
| 3 | 文件/清单级读载荷（`list_visible_files`） | 现在 `Prefetch` 载荷不含文件级条目 |
| 4 | 切装配点 | 一行；但只有 1–3 完成后才可能全绿 |

---

---

## 53. R3 S3-4（第二件·下半之一）：补齐其余 **5 个 op 的 proto 镜像**（2026-09-20）

### 53.0 本轮做了什么

`RemoteCatalog` 的写路径必须先有「op 的线上形状」。补齐 5 个：
**DropSchema / EvolveSchema / DropShard / Compaction / Idempotency（认领）**；
`Op.kind` 的分支数 **4 → 9**，并同步更新了分支守护测试（它如期要求「显式确认」）。

### 53.1 两个设计决定（都有理由，不是风格）

1. **`SchemaChange` 用 `oneof`**，而不是「`kind` + 松散字段」：后者允许**非法组合**
   （如 `kind=AddColumn` 却带 `to`），而那种消息**照样编码成功** —— 错误被推到最晚才发现
   （应用时才炸，且可能只在某一个副本上炸）。
2. **`Field` / `DataType` 用 Arrow IPC 携带**（编成「单字段 schema」）：
   `CreateTableOp.arrow_schema_ipc` 已经是 Arrow IPC，再发明一套「字段编码」只会得到
   **两份必然漂移的定义**。代价是编码里带了 schema 名这类无意义信息 —— 解码侧忽略它，
   并**强制恰好 1 个字段**（0 或 2 个都是协议层垃圾，放过去会让「增列」变成一个说不清的动作）。

### 53.2 幂等「认领」为什么**必须**是 op

`pipeline.rs` 在 WAL fsync 成功后立刻 `record_idempotency`（`batch_id` 为**空串** =
「已认领、批次尚未落盘」）—— 这是拦住「**并发同键请求双双通过预筛、各写一份 Data**」的那道闸。
它若只存在本地，换主/日志重放后认领就没了，闸门形同虚设 → **必须是 op**（`IdempotencyOp`）。
镜像里**空 `batch_id` 的语义被保住**（用例钉住：不能被补成默认值）。

### 53.3 实测抓到的三处（都是编译器/用例主动拦下的）

| # | 现象 | 教训 |
|---|---|---|
| 1 | 加 `StateOp` 变体后，`now_ms()` 的 `match` **立刻编译失败** | 穷尽 `match` 是「**每个 op 必须自带请求时间**」的强制器（纪律 1：状态机不读钟）—— 漏带就会让各副本按各自墙钟分叉 |
| 2 | 反证：把 `expected_version` 写死 0 | 用例精确报「OCC 版本丢了 → DDL 变成无条件覆盖」（丢版本 = DDL 退化成无条件覆盖，静默） |
| 3 | `Schema::new(vec![])` 类型无法推断（`Fields` 有两个 `From<Vec<_>>`） | 类型标注要写清（`Vec::<Field>::new()`） |

### 53.4 验证

- `op` 模块 **8 条**（3 条新增：`SchemaChange` 三变体无损 + 非法载荷拒绝；5 个新 op 的镜像与解码；
  新 op 的**幂等语义** —— 重放/重试必须 `accepted=false`，否则重启重放会把「已经做过」当错误 → 起不来）
- `wire_compat` 分支守护 **4 → 9**（守护测试如期要求显式确认 —— 这就是它存在的意义）
- 全量 **296 passed / 0 failed**（59 targets；日志无 `Blocking waiting for file lock`，可采信）；
  clippy 0（改动 crate）

### 53.5 遗留（下一件）

| # | 项 | 说明 |
|---|---|---|
| 1 | **`RemoteCatalog` 本体** | 写走 `Propose`（现在 9 个 op 分支都有线形了）、读走 `Prefetch`/`Delta` |
| 2 | 文件/清单级读载荷（`list_visible_files` 等） | `Prefetch` 载荷现在只到「表 + schema」 |
| 3 | 幂等的**读**路径（`check_idempotency`/`known_batch_ids`） | 按 §3.2 是「本地键集合快路径」，需定形它与 SM 权威的关系（S3-5 已把键集合从 WAL 派生） |
| 4 | 切装配点 | 最后一行，改完靠既有用例全绿验收 |

---

---

## 54. R3 S3-4（第二件·下半之二）：**文件级读载荷** —— 墓碑必须发（2026-09-20）

### 54.0 本轮做了什么

`Prefetch` 载荷补齐**文件级**内容，读路径的缺口到此为止：

| 位置 | 内容 |
|---|---|
| proto | `PrefetchRequest.since_snapshot`（客户端清单水位）+ `PrefetchPayload.files`（`FileEntry{batch_id, manifest}`，复用已有的 `FileManifestMsg` 逐字段镜像） |
| `CatalogState` | 新增 `files_since(table, since)`（文件级增量；**含墓碑**） |
| `NodeHandle::prefetch` | 落实口径（与表载荷一致：纯版本探测 = 零载荷；顺序规范化便于对拍） |

### 54.1 为什么「只发当前可见的文件」是**错的**（本轮的核心）

客户端的本地清单是按**版本增量**刷新的，它要的是「我错过的那部分变化」——
**包括墓碑**（`deleted_at > since` 的文件）。只发「当前可见」会让客户端
**永远删不掉已删文件**：它那边的旧副本还在，查询就会去读已删数据
（**静默读到脏数据**，不报错、只在结果里少/多几行）。

**实测**：把 `files_since` 里的墓碑规则去掉（`|| (deleted_at != 0 && deleted_at > since)`）
→ 用例立刻红，报「删分片后必须把**墓碑**发回来……：[]」——删除信息**完全丢失**。

### 54.2 增量条件（MVCC 语义）

```text
valid_from > since                      → 水位之后**新增**的
|| (deleted_at != 0 && deleted_at > since) → 水位之后**被删**的（墓碑）
```

- `since_snapshot == 0` → 全量（含墓碑，客户端据此重建清单）；
- `table == None` → 所有表；
- 结果按 `(table, batch_id)` 排序 → **同一请求同一字节**（便于对拍与缓存打补丁的可复现）。

### 54.3 「表已删」与「表没文件」怎么区分（契约的一部分）

表的存在性只看 `tables` 载荷；文件只为**请求的表**发。于是：

| 现象 | 含义 |
|---|---|
| 表在 `tables` 里 + 该表无文件条目 | **空表**（不是错误） |
| 表**不在** `tables` 里 | 该表**已删**（客户端要丢掉整表条目，包括它的文件） |

所以文件载荷**不能**为已删表「补空占位」——与 `§51.1` 是同一条规则（补了就没法区分「没请求」与「已删」）。

### 54.4 验证

- e2e 新增 `prefetch_carries_file_deltas_including_tombstones`（四段：全量 → **零增量** →
  **墓碑** → 墓碑不重复发）。它顺带把上一轮新加的 `DropShard` op 跑通了**真 gRPC 端到端**
  （propose → apply → 载荷观测），等于给新 op 也补了一条集成证据。
- **反证**：去掉墓碑规则 → 用例红（见 54.1）。
- 全量 **297 passed / 0 failed**（59 targets；日志无锁等待）；clippy 0（改动 crate）。

### 54.5 遗留（下一件）

| # | 项 | 说明 |
|---|---|---|
| 1 | **`RemoteCatalog` 本体** | 读载荷（表+schema+**文件增量**+版本）与写线形（9 个 op）**都齐了** → 只剩本体 |
| 2 | 幂等的**读**路径（`check_idempotency`/`known_batch_ids`） | 需定形「本地键集合快路径」（S3-5 已从 WAL 派生）与 SM 权威的关系 |
| 3 | 线性化读位置（`read_index`） | 按 `§5` 约定 4 |
| 4 | 切装配点 | 最后一行，靠既有用例全绿验收 |

---

---

## 55. R3 S3-4（第二件·下半之三）：`RemoteCatalog` 落地 —— **与 `MemoryCatalog` 对拍一致**（2026-09-20）

### 55.0 本轮做了什么

| # | 交付物 | 说明 |
|---|---|---|
| 1 | `RemoteCatalog` | `CatalogOps` 的 gRPC 实现：**写** = `Propose`（+ 换主重试）／**读** = 本地缓存 + **版本驱动刷新** |
| 2 | 错误保真 | `MetaError → Status` 补**机器可读** `err-kind`/`err-subject` metadata + 反向映射 `map_status` |
| 3 | **对拍** | 同一串操作打两个实现，逐项比较可观测状态（9 步） |

### 55.1 对拍抓到的真 bug：**结构变更必须走全量**

`Delta.changed_tables` 是 **manifest 级**的（只列 `table_manifest_ver` 推进过的表），
而 `create_table`/`drop_table` 只动 `schema_ver` —— **新建的表根本不在 `changed_tables` 里**。

照「版本变了就走增量」的直觉写，客户端会**静默丢掉刚建的表**：
现象就是"远端建表后读不到"（对拍里一建表就红，而单跑 `MetaService` 的用例是绿的 ——
因为那条路径从没走过"增量刷新"）。

**修法**：客户端把 `schema_ver` 变化当作「结构变更」信号 → 走全量。
**为什么不让服务端判断**：`DeltaRequest` 只带 `since_manifest_ver`，**不带客户端的 `schema_ver`**，
服务端无从知道对面缺哪些结构变更 → 信号只能由客户端从探测结果里取（它本来就知道自己的版本）。
（登记遗留：给 `DeltaRequest` 加 `since_schema_ver`，省掉客户端每次结构变更的全量。）

### 55.2 错误保真为什么必须（不是锦上添花）

远程路径上 `Status → LakeError` 一旦退化成 `Other`，SQL 层的错误码会**整体退化**
（用户看到 500 而不是 1051「表不存在」），而**没有任何测试会红**。所以这一层单独加固：

| 侧 | 做法 | 用哪种测试钉住 |
|---|---|---|
| 服务端 | 错误身份放进 **metadata**（`err-kind` + `err-subject`），而不是只塞 message | 「每个可分支错误都带 kind」 |
| 客户端 | 用 metadata 反查（**不 parse 诊断串** —— 文案一改就静默坏）| 「`LakeError → Status → LakeError` **保形**」|

`SchemaChanged` 是特例：它的 `new_schema` 是 `Arc<Schema>`，**过不了线** →
远端在**冲突时多读一次**当前 schema 还原成同形错误（额外往返只发生在冲突路径上）。

### 55.3 幂等快路径的口径（"安全"要可验证）

本地只记**自己写过的键**（含 `commit_files` 自带的键）。**漏判是安全的**：走到 `Propose`，由 SM 去重。
对拍把这条写成可执行断言：重放同一个提交 → `accepted=false` —— **快路径答错不影响正确性**。

### 55.4 对拍比什么（判据清单）

schemas｜tables（全限定名 + 格式 + schema 版本 + 字段数）｜两组版本号｜快照号｜
`u64::MAX` 下可见文件｜**旧快照**下可见文件（MVCC：墓碑必须还在）｜OCC 冲突的形状｜
「表已存在」「表不存在」的错误类别 ✓

刻意**不比**的：`created_at`/`committed_at` 等时间戳 —— 本地实现读自己的钟，远端用 op 里带的时间，
**本就该不同**（纪律 1：时间随 op 走），拉进对拍只会得到噪声断言。

### 55.5 验证

- `RemoteCatalog` 单测 2（错误保形 / 表名归一化）+ `error.rs` 新用例（kind+subject 全覆盖）
- 对拍用例 1（9 步 × 6 个维度）
- 全量 **301 passed / 0 failed**（60 targets；日志无锁等待）；clippy 0（改动 crate）

### 55.6 遗留（**R3 收口的最后一步**）

| # | 项 | 说明 |
|---|---|---|
| 1 | **切装配点** | `server` 里那一行：起一个**进程内 1 节点 `MetaNode`**（设计 §3.3 的"本地传输"）+ `RemoteCatalog::connect(loopback)`；保留**装配层开关**（设计指定的回滚点：切回 `MemoryCatalog`）。判据：既有用例全绿 |
| 2 | `DeltaRequest` 加 `since_schema_ver` | 让服务端能判断结构变更 → 免掉客户端每次结构变更的全量（见 55.1）|
| 3 | `ProposeResponse` 加 `affected` | 精确 `drop_shard` 条数（现在远端只能给 1/0）|
| 4 | 幂等键集合进载荷 | 现在本地快路径只知道自己的键（安全但会多走一次 `Propose`）|
| 5 | 文件缓存的保留窗口/GC | 墓碑只增不减（MVCC 需要它们，但要有界）|

---

---

## 56. R3 S3-4 收口：**切装配点** —— standalone 的 catalog 换成嵌入式 metanode（2026-09-20）

### 56.0 本轮做了什么

`crates/server` 的装配点从「写死 `MemoryCatalog`」变成**按配置装配**：

| 位置 | 内容 |
|---|---|
| `[meta]` 段（新） | `mode`（`embedded` 默认 / `memory` 回滚）· `listen` · `dir` —— **这就是设计指定的回滚点** |
| `mode = "embedded"`（默认） | 进程内起 **1 节点 metanode**（raft + fjall **落盘**）+ **loopback gRPC**，再用 `RemoteCatalog` 连它（设计 §3.3 的 standalone 形态）|
| `Lakehouse` | 持有 `MetaNode`（它的 `Drop` 停 raft 线程 —— 丢了它 = 后台线程失控）|

回滚 = 配置里写一行 `[meta] mode = "memory"`（段级 `#[serde(default)]`：字段可省略）。

### 56.1 为什么走 loopback gRPC（而不是直接调 `NodeHandle`）

让 standalone 与分布式**走同一条代码路径**。直接调句柄会得到"本地一条路、远端另一条路"，
两者的差异只会在上线时暴露 —— 那正是设计禁止的分叉（`§3.3`：禁止分叉）。
代价是每个写多一次同机往返（量级可忽略），换来的是**同一份代码被两种形态验证**。

### 56.2 目录策略（这次切换最容易踩的地方）

| 场景 | `dir` | 后果 |
|---|---|---|
| 生产（`yuntun` 无 `--config` 时） | `./data/meta`（standalone 补的默认） | 元数据跨重启保留 |
| 配置里显式给 | 用户指定 | ✓ |
| **省略**（测试用 `Config::default()`） | **进程内临时目录** | 重启即新集群 → 所以 `warnings()` **会吼一声** |

临时目录必须**每实例一个**：同一进程里会起多个 `Lakehouse`（测试正是这样），
共用一个目录会让它们互相看到对方的表 —— 现象是莫名其妙的 `TableAlreadyExists`。

### 56.3 判据（S3-4 的验收原话）

| 判据 | 结果 |
|---|---|
| **既有用例全绿**（standalone 不回归） | ✅ **300 passed / 1 failed** —— 失败的是**已登记的 chaos 并行 flake**（全量里 131.68s = CPU 饥饿；chaos 自建装配用的仍是 `MemoryCatalog`，与本次改动无关）；**隔离复跑 0.21s 通过** |
| **`if distributed` 分支为零** | ✅ 代码命中 0（自 `§52` 起） |
| 回滚点 | ✅ `[meta] mode = "memory"` 一行，业务代码零改动 |

### 56.4 R3 收口状态

骨架完整：确定性状态机（`§38`）→ raft 选型闸门（`§40`）→ 落盘存储与快照（`§43`/`§44`）→
gRPC 面（`§45`）→ **多节点网络传输**（`§50`）→ **读路径载荷**（`§51`/`§54`）→ **op 镜像**（`§53`）→
**接缝拆净**（`§52`）→ **`RemoteCatalog` 与对拍**（`§55`）→ **装配点切换**（本节）。

**遗留**（按重要性）：

| # | 项 | 说明 |
|---|---|---|
| 1 | 在两种形态之间做**整机对拍** | 本轮做了"既有用例在 embedded 下全绿" + **组件级**对拍（`§55`）；"同一条脚本跑 memory 与 embedded 比可观测状态"**没做** |
| 2 | `DeltaRequest` 加 `since_schema_ver` | 免掉客户端每次结构变更的全量（`§55.1` 那个 bug 的根治）|
| 3 | `ProposeResponse` 加 `affected` | 精确 `drop_shard` 条数（现在只能给 1/0）|
| 4 | 幂等键集合进载荷 | 本地快路径现在只知道自己的键（安全，但会多走一次 `Propose`）|
| 5 | 文件缓存的保留窗口/GC | 墓碑 MVCC 需要，但要有界 |
| 6 | `[meta] dir` 的运维说明 | 省略即"重启换集群"，要写进运维文档 |

---

---

## 57. R3 遗留清偿：`affected` / `since_schema_ver` / 幂等键过线 / 文件保留窗口（2026-09-20）

### 57.0 清偿清单

| # | 遗留（`§56.4`） | 状态 |
|---|---|---|
| ① | **整机对拍**（memory ↔ embedded 比可观测结果） | ⏳ **仍余**（见 57.6）|
| ② | `DeltaRequest.since_schema_ver` | ✅ 57.2 |
| ③ | `ProposeResponse.affected`（精确条数） | ✅ 57.1 |
| ④ | 幂等键集合进载荷 | ✅ 57.3 |
| ⑤ | 文件缓存保留窗口/GC | ✅ 57.4 |
| ⑥ | `[meta] dir` 的运维说明 | ✅ 示例配置 + `§56.2` 已写明 |

### 57.1 `affected`：**远端算不出来的数，必须在服务端算好跟着提交过线**

`ApplyOutcome` 加 `affected: u64`（配 `hit()`/`one()`/`with_count()` 三个构造器，避免十几处字面量各写一遍），
`ProposeResponse.affected = 7`。于是远端 `drop_shard` 返回**精确**条数 —— 之前只能给 1/0，
调用方按它做对账会**失真**（看起来能用，所以最难发现）。

对拍同步升级：`drop_shard` 从「两边都 > 0」改为 **`assert_eq!(rd, md)`** —— 精确比较才是对拍该有的强度。

### 57.2 `DeltaRequest.since_schema_ver`：把结构变更信号移到服务端

`§55.1` 那个坑（新建表**不在** `changed_tables` 里 → 客户端增量刷新**静默丢表**）的服务端修复：
`NodeHandle::delta(since_manifest_ver, since_schema_ver)` 现在比对客户端的 schema 版本，
不一致就回 `full_reload = true`。客户端仍保留本地判断（探测里本来就有 schema 版本，零成本 → 双保险）。

e2e 断言：`delta(当前, 当前)` → `full_reload == false`；`delta(当前, 0)` → **true**。

### 57.3 幂等键集合过线（**只在全量刷新给**，且有上限）

`CatalogState::idempotency_keys()` + `PrefetchPayload.idempotency_keys`：
- **只在全量刷新时发**（增量刷新没有"键的版本号"，逐次全发会把刷新变成大传输）；
- 上限 10 000 条 + **超限告警**（截断只影响快路径命中率，不影响正确性 —— 权威仍在 SM）；
- 客户端**只并、不替换**（替换会忘掉"自己刚写过、服务端还没全量刷到"的键）。

**过程中发现的契约**：`check_idempotency` **刻意不打网络**（它在每个批次的写入口被调用，
为它加一次探测就抵消了它自己的意义）→ 键集合靠**刷新**装填 → 新进程刚起时集合可能是空的，
"未命中"会走到 `Propose` 由 SM 去重（**正确性不受影响**，只是白写一次 WAL）。
这条已写进方法文档，并让用例**先触发刷新再断言**（把契约写进测试，而不是写进注释）。

### 57.4 文件缓存保留窗口（**默认不回收**）

`RemoteCatalog::with_file_retention(n)`（默认 `u64::MAX` = 保留全部墓碑，安全优先）。

关键不变量：**回收必须与"拒绝过旧快照"成对出现**。少一个墓碑会让 `visible_at(旧快照)`
从 `false` 翻成 `true` → 旧快照查询**读到已删数据**（静默错结果）。
所以 `list_visible_files(snapshot < floor)` **返回错误**（宁可显式拒绝，不可少读）。

- 单测（纯缓存逻辑，不经网络）：窗口内保留 ✓／窗口外回收 ✓／下界随快照抬升 ✓／默认永不回收 ✓
- e2e：过旧快照被拒（错误里说清是"快照过旧"）✓，而"当前"仍可查 ✓

### 57.5 顺带修的三处

| 现象 | 处置 |
|---|---|
| `RemoteCatalog::connect` 的 lazy connector **需要 tokio 运行时上下文**（哪怕不真连）—— 单测用 `#[test]` 会 panic（"there is no reactor running"）| 写进 `connect` 文档（这条约束是**实测**得到的）|
| `commit_files` 返回的快照号取自**缓存** | 改用**响应里**的（权威）：缓存可能已被别的写推进 → commit 的"读己之写"水位偏高 → 查询会以为数据已可见 |
| `wire_compat` 的 `ProposeResponse` 字面量缺字段（编译失败）| 补 `affected` 并断言它过线（**新字段立刻被既有守卫测试照出来** —— 那是它该干的事）|

### 57.6 验证与仍余

全量 **302 passed / 0 failed**（60 targets；日志无锁等待）；clippy 0（改动 crate）。

**仍余 ①「整机对拍」**：组件级对拍（`§55`）比的是 catalog；
整机对拍要比的是**经过 SQL/ingest/query 之后**的可观测结果（表清单、行数、schema 版本、
文件可见性），也就是"切装配点不回归"的真正口径。它同时是 **R4** 那个
「与单节点串行精确相等」判据的现成模板 —— 所以值得先做它再做 R4。

---

---

## 58. R3 遗留清偿（6/6）：**整机对拍** —— memory ↔ embedded 端到端一致（2026-09-20）

### 58.0 本轮做了什么

新增 `crates/server/tests/assembly_parity.rs`：同一条序列
（`CREATE TABLE` → `INSERT` → 等 flush → 观测）分别跑在 `[meta] mode = memory` 与 `embedded` 上，
比较**经过 SQL → catalog → ingest → WAL → store 之后**的可观测结果：

| 比什么 | 为什么 |
|---|---|
| `schemas` | 多 schema 语义一致 |
| `tables`（全限定名 + schema 版本 + 字段数） | DDL 结果一致 |
| 两组版本号 | 缓存失效信号一致 |
| 每表**可见文件数与行数** | **数据真的落盘了**，且落法一致（证明 ingest→flush→提交 整条链走通）|
| `created_at` 的**单位**（不是值） | 见 58.2 —— 这条是对拍自己抓出来的 |

### 58.1 为什么它比组件级对拍（`§55`）强

`§55` 比的是 `CatalogOps`；这里比的是**整机**：SQL 解析 → DDL/DML 分派 → 攒批 → flush 落盘 →
元数据提交，一整条链。这才是"切装配点不回归"的真正口径，也是 **R4**
「与单节点串行精确相等」那条判据的现成模板。

### 58.2 反证抓到的东西（本轮最值钱的部分）

先做了一个反证：把"刻意不比"的时间戳临时拉进比较 → **立刻红**，而且输出把差异钉死了：

| 形态 | `created_at` |
|---|---|
| memory | `1789843409`（**秒**）|
| embedded | `1789843411111`（**毫秒**）|

**1000× 的单位不一致，平时完全不报错。** 真因：状态机 `create_table(req, now_secs)` 的
单位由**调用方**决定 —— `MemoryCatalog` 传 `now_secs()`，而 `op::apply` 传的是 `op.now_ms()`。

修法：`now / 1000`（与 SM 其它时间参数一致），并在对拍里加一条守卫：
**`created_at` 必须在秒级**（不比较具体值 —— 两次运行本就该不同；只比**单位**）。

> 教训：**「把一个时间塞进状态机」之前先确认单位**。这类错在任何单形态测试里都看不见 ——
> 只有"两种装配跑同一条序列"才会撞出来。

### 58.3 验证

- 整机对拍：`memory == embedded` ✓，且断言"恰好 1 个已提交文件、2 行数据" ✓
  （否则两边都空也能"相等"，那是对拍最常见的自欺）
- 全量 **303 passed / 0 failed**（61 targets；日志无锁等待）；clippy 0（改动 crate）

### 58.4 至此

**R3 的遗留全部清偿（6/6）**：① 整机对拍 ✅ ② `Delta.since_schema_ver` ✅ ③ `affected` ✅
④ 幂等键过线 ✅ ⑤ 文件保留窗口 ✅ ⑥ `[meta] dir` 说明 ✅。

下一步按路线图是 **R4（datanode 化 + 冷热边界）**，判据是「多 datanode 并发写 +
查询结果**与单节点串行精确相等**（对拍，硬要求）」—— `§58` 这条用例就是它的模板。

---

---

## 59. 工具链：全仓升 **Rust 2024 edition**（+ 纠正 clippy 基线读数）（2026-09-20）

### 59.1 改动是"根清单一行"

`[workspace.package] edition = "2021" → "2024"`。18 个 crate 全写 `edition.workspace = true`，
所以**只有一个声明点** —— 这是当初把 `edition` 放进 `workspace.package` 的收益。
同时 `resolver = "2" → "3"`（2024 的默认解析器）。

**为什么 `resolver` 必须显式写**：根是**虚拟清单**（无 `[package]`），解析器不会被 edition
推断出来。`resolver = "3"` 实测 `Cargo.lock` **逐字节无变化**（见 59.6）。

### 59.2 为什么一行就能过：先按"2024 会破坏什么"扫一遍

升级前的**可复用步骤** —— 先 grep 五类破坏点，命中为 0 才动手：

| 2024 变更 | grep 目标 | 命中 |
|---|---|---|
| `gen` 成保留字 | `\bgen\b` | 0 |
| `static mut` 引用（现在报错） | `static mut` | 0 |
| `unsafe extern` / 属性须 `unsafe(...)` | `no_mangle\|export_name\|link_section` | 0 |
| `std::env::set_var` 变 unsafe | `set_var\|remove_var` | 0 |
| RPIT 捕获全部生命周期 | `-> impl ` | 0 |

行为类变更（`tail_expr_drop_order` / `if_let_rescope` / never-type fallback）编译期
**一个都没触发**；全仓**零 `unsafe` 块**，`unsafe_op_in_unsafe_fn` 这类新 lint 也无从下手。

> 结论：**edition 升级的难度取决于代码风格，不是 crate 数量**。本仓把"显式类型 / 无 unsafe /
> 无 `impl Trait` 返回 / 无全局可变状态"当纪律，于是升级成本≈一行。

### 59.3 顺带抓出：`status.md` 那句"clippy 0 警告"是**热缓存读数**

edition 一改，**所有 crate 全部重建**，clippy 这才把全仓告警吐全：**15 + 7 = 22 条**。
此前历次 `cargo clippy --workspace --all-targets` 读到的"0 警告"，是**增量编译只报被重建 crate**
的产物；`status.md` 原文那句"全仓余 1 处历史告警"也是同一个读数来源。

- **15 条 `collapsible_if`**：edition 2024 让 **let-chains** 可用，clippy 于是开始建议
  `if a { if let Some(x) = b { … } }` → `if a && let Some(x) = b { … }`。2021 下这条建议
  **无法表达**（语法不成立），所以以前不提 —— **不是以前更干净，是以前没得选**。
- **7 条其他**（与 edition 无关，属纯热缓存遮蔽）：3 处 doc（2 处 blockquote 续行漏 `>`、
  1 处列表项后缺空行）、2 处 `now_ms().max(0) as u64`（`now_ms() -> u64` → `.max(0)` 与
  `as u64` 都是恒等，实测签名与字段类型确认为 `u64` ✓）、1 处 `&String` 冗余借用、
  1 处未用 import（`lh.catalog` 是 `dyn CatalogOps` trait 对象，方法直接可调，不必引入 trait）。

**修完**：`cargo clippy --workspace --all-targets` 全仓只剩 **1 条外部依赖告警**
（`proc-macro-error2 v2.0.1`，来自 `opensrv-mysql`，非本仓代码；见 `§49` 那条线）。

### 59.4 两个坑：都会让人"改一个文件、动全仓"

**坑 1 —— `rustfmt <crate 根>` 会递归格式化它的全部子模块。**
把 `crates/meta/src/lib.rs`、`crates/server/src/lib.rs` 传给 rustfmt 后，**meta 与 server 两个
crate 被整仓重排**：diff 从预期的 ~60 行炸到 **469/365 行、25 个文件**（`op.rs` 120 行、
`server/lib.rs` 148 行）。已**全部回退**。要只动一个文件，就**不要**传 crate 根。

**坑 2 —— rustfmt 的宽度按 *CJK 占 2 列* 算。**

```rust
// rustfmt 眼里"超宽" → 被拆成 5 行（作者显然是有意保留单行）
self.trace("recv_snapshot", format!("忽略（旧于本地 first={}）", inner.first_index()));
```

这类"单行调用"全仓 `cargo fmt --check` 报 **338 处、78 个文件**。所以本仓**不是** rustfmt-clean，
而且**不能**整仓 `cargo fmt`：那会把中文注释附近的行大量拆开，淹没真实改动、且**可读性变差**。
本轮 15 处 let-chain 全部**手工**按 rustfmt 2024 style 落，再用"每个文件的 `rustfmt --check`
差异条数是否与基线一致"验证没跑偏（18 个文件里 16 个与基线**完全一致**；`table.rs` 少 1 处 ——
塌一层后原本超宽的那行正好放得下，属改善；`build.rs` 初版猜错，已按 rustfmt 意见改正）。

**顺带实测的 rustfmt 2024 规则**（探针文件，省得后来者再猜）：let-chain **一律**拆多行
（首项留 `if` 行、后续 `&&` 缩进 +4、`{` 独占一行）—— 连 `if let Some(v) = x && y {` 也拆；
但 **`else if` 上的链保留单行**。

### 59.5 验证

- `cargo test --workspace --no-fail-fast` → **303 passed / 0 failed**（44 个二进制 + 17 个 doctest），
  与 `status.md` 记的 303 测试函数 / 303 用例**逐一对上**
- `cargo check --workspace --all-targets` → **0 error**
- `cargo clippy --workspace --all-targets` → 本仓 **0**，仅余外部 `proc-macro-error2`
- `resolver = "3"` 后 `Cargo.lock` **无变化**

> 顺带修了 `status.md` 三处陈旧数字：§1 与 §4 表里的"214 用例 / 184 个测试函数"（实为 303），
> 以及"全仓余 1 处历史告警"（实为 0 + 1 条外部）。

### 59.6 更正与遗留

- **更正**：上一个提交（`6675043`）的信息里把本章误写为 `§51` —— 当时 `§49` 之后直接是
  `§50…§58`（S3-4 那批），实际章节号是 **§59**。
- ~~**决定不做：暂不声明 `rust-version`（MSRV）**~~ —— **本条已被 `§60` 取代**（写 `§59` 时判为
  "不该顺手加"，随即按要求补上）。保留原文以示转折，结论以 `§60` 为准：**MSRV = 1.94**，
  **不是** policy 想取的 1.92 —— 下界被 `datafusion 55.0.0`（声明 `1.94.0`）顶住。

---

## 60. MSRV 声明：**1.94**（policy 想取 1.92，被 datafusion 顶住）（2026-09-20）

### 60.1 意图 vs 事实：MSRV **不是自由可选项**

意图是官方"latest stable − 6"策略：当时工具链 1.98 → **1.92**。但 MSRV 的真实取值是
**全依赖链的 max** —— 量法：解析一次，取所有包声明的 `rust-version` 最大值。

```
已解析包 524；其中声明了 rust-version 的 398
依赖侧有效下界（最高要求）= 1.94.0
要求 > 1.92 的：31 个 —— **全部**是 datafusion-*（`datafusion 55.0.0` → 需要 rustc 1.94.0）
把这 31 个去掉后，要求最高的那档里**没有任何包** → 下界完全由 datafusion 决定
```

→ 声明 1.92 是**假承诺**：在 1.92 上 cargo 会直接拒绝：

```
package `datafusion v55.0.0` cannot be built because it requires rustc 1.94.0 or newer,
while the currently active rustc version is 1.92.0
```

**有效 MSRV = max(policy, 依赖侧最高要求) = 1.94**。datafusion 是本仓不可替代的查询引擎，
所以"**以 datafusion 为基准**"是唯一不牺牲功能的取法；另一条路是降到 `datafusion ≤ 54.x`
换来 1.92 —— 代价是查询引擎与 arrow 版本联动、要重跑全部对拍，属独立决策，本轮未做。

### 60.2 两处都要写，否则等于没声明

虚拟清单的 `[workspace.package]` **只对显式继承的成员生效**（同 `edition`/`license`）：

- `[workspace.package] rust-version = "1.94"`
- 18 个成员各加一行 `rust-version.workspace = true`

实测（`cargo metadata`）：18 个 `yuntun-*` 的 `rust_version` **全为 `1.94`** ✓。

### 60.3 声明它是**有实际作用**的

1. **`resolver = "3"` 的 MSRV-aware 解析以它为输入。** `§59.6` 那句"resolver=3 目前等价 v2"
   **到此才真正失效** —— 现在 v3 有输入了（这也是 `§59` 与本节要连起来读的原因）。
2. 依赖要求更高 rustc 时会在**下游报出来**：这条只有在真·1.94 机器上才看得见。

### 60.4 验证

- 加 MSRV 后重新解析：`Cargo.lock` **零变化**（无依赖需要降级）
- `cargo clippy --workspace --all-targets`：本仓 **0 告警**，且 MSRV 提到 1.94 **没有解锁任何
  MSRV-gated lint**（clippy 把 `rust-version` 当 `msrv` 输入，数字一提就可能开始建议"换用更新的
  std API" —— 这次没有）
- `cargo test --workspace --no-fail-fast` → **303 passed / 0 failed**

### 60.5 遗留：这个数是**推导值**，不是"实测能编过"

**没有做真·1.94 构建验证** —— 本机镜像（tuna）`1.92.0` / `1.94.0` 都返回 **404**，
且已定"不折腾工具链"。所以 `1.94` 是**从依赖侧下界推出来的**，逻辑上成立、但**未经编译器确认**。

> **CI 应补一步 `cargo +1.94 build`** —— 这才是 MSRV 承诺的兑付方式；在补上之前，
> 把 `rust-version` 当"已保证"是过度解读。

---

## 61. R4 开工前闸门 **P2**：冷热边界的协议形态 —— 结论是**拉取**，不是推送（2026-09-20）

`plan.md` 把 P2 定为"**R4 开工前**"的硬闸门：各实例的 flush watermark 如何传播 / 消费
（推送 vs 查询时拉取），并要一份**两种形态的查询放大对比**。本节给出结论与契约定死。

### 61.1 设计侧已有的答案（先把原文摆出来）

`architecture-with-chunk.md §4.4` 已经把做法写死：

> manifest 中每个文件记 `source_instance`；**pull 响应带该实例的 `flushed_watermark`**；
> 协调者按实例二维切分（**冷读该实例 ≤watermark 的文件，热 pull (watermark, now]**）。
> **此契约必须在实现前定死**，否则多实例后会冒出重复计数这类极难排查的 bug。

也就是说设计选的就是**拉取**。本节的任务是把它**验证成决策**（给放大对比），并补齐契约里
没写死的四处（见 61.4）。

### 61.2 为什么必须是拉取：定界要**精确**，而推送必然陈旧

`§4.5` 给了一条正确性依赖的不变量：

> 任何已 seal 的数据，要么在 datanode 的 chunk 中可 pull，**要么**在 manifest 中可 read，
> **不会两边都不在**。实现：`commit_files` 成功、manifest 版本推进后，才允许释放对应 chunk。

这条不变量要求边界是**精确**的。而推送形态的水位必然陈旧（传播延迟 + 进程暂停 + metanode
重启丢内存态），陈旧水位在边界上只有一个后果：**要么重复读**（水位说"还没 flush"，其实已落盘）
**要么漏读**（水位说"已 flush"，其实还没落盘）—— 两者都是静默错数据，正是本仓最怕的那类。

更根本的一点：**只有 owner 自己知道自己的边界**，而且它是在**同一个锁**下知道 commit 与
reclaim 的相对顺序。把边界判定交给"另一个进程在另一个时刻写下的一个数"，等于把精确性
换成时延——而拉取只多一次往返，却把判定放在唯一知道真相的地方。

> **结论：拉取为权威**（水位随 pull 响应回来）。推送只能做**可选优化**（如 metanode 侧缓存
> "上次看到的水位"用于提前 fanout），**不得**作为切分依据 —— 它一旦被当作权威，上面两个
> 静默错数据就会回来，而且是概率性的。

### 61.3 两种形态的查询放大对比

| 维度 | **拉取（选定）** | 推送（仅可作提示） |
|---|---|---|
| 每次查询的额外 RPC | **O(N)**：向 N 个实例各发一次 pull（可合并为一次 fanout） | O(1)：读一份水位表（还需 TTL 刷新） |
| 额外网络数据量 | **热数据本身**要跨网络（量 ∝ 未落盘部分） | 只传水位数字（极小） |
| 写入侧代价 | 0（水位由响应顺带带回） | N×心跳频率写 metanode（或内存态，重启即丢） |
| 边界精确性 | **精确**（owner 自答，与 commit/reclaim 同锁） | **不精确**（陈旧窗口内必然错，方向随机） |
| 失败模式 | 拉不到 → 可**显式** STALE / 报错（`partial` 标记） | 水位陈旧 → **静默**重复或漏读 |
| 代价可控性 | 可用 `known_manifest_ver` 命中"无事发生"的快速路径 | 省了 RPC，但省错了地方（错在数据） |

**放大在哪、怎么压**：拉取的 O(N) 是**实例数**量级，而查询本身（R5 的 fanout）已经是 O(N)，
所以拉取**没有引入新的量级**；真正要盯的是"热数据跨网络"这一项 —— 它是 `S5-3`
（partial aggregate 下推）存在的理由：**绝不回传原始行**，只回 partial 聚合结果。

### 61.4 契约定死（4 条，实现前）

1. **水位单位 = manifest 版本号**，与现有 `Chunk::visible(cached_snapshot)` /
   `mark_committed(snapshot)` **同一个号** —— **不引入第二套序号**（两套号一定会漂移）。
2. pull 请求带 `known_manifest_ver`；owner 若自己的 `flushed_watermark > known_manifest_ver`
   → 返回 **STALE**，协调者刷新 manifest 后重试。**不得**返回空结果顶替 —— 那会把
   "你看的是旧版本"伪装成"没有热数据"，即静默丢数据。
3. `release-after-commit`：**manifest 版本推进之后**才允许释放 chunk（现有 I4 已满足）。
4. 切分口径：**冷读该实例 ≤watermark 的文件；热 pull (watermark, now]**。

### 61.5 一个反直觉的实测结论：**现有机制今天是对的**

把现有代码摊开看（`crates/chunk/src/chunk.rs`）：

```rust
pub fn visible(&self, cached_snapshot: u64) -> bool {
    match self.committed_snapshot {
        None => true,                        // 未提交 → 永远是热数据
        Some(s) => cached_snapshot < s,      // 已提交 → 读者版本 ≥ s 时不再当热数据
    }
}
```

而文件的可见性是 `valid_from <= query_snapshot`。**两个判断用的是同一个号**，于是对任意
`V`，chunk 与文件里**恰好一个可见** —— 单机形态下"重复计数"在结构上不会发生。

它会在下面三种情况下坏，这也是 T12.2 要解决的**全部**：

| # | 会坏的情况 | 后果 |
|---|---|---|
| ① | **提交者≠持有者**（分布式下由协调者/别的节点代理 `commit_files`） | 本地 `committed_snapshot` 不再可信 → 文件已可见、chunk 仍当热数据 → **重复计数** |
| ② | **release 早于协调者刷新 manifest** | chunk 已释放、文件在对方眼里还不可见 → **漏读**（`§4.5` 的"两头都没有"） |
| ③ | 用**全局 min(watermark)** 当边界 | 一个慢实例拖住所有实例的热边界（`§4.4` 第二条已点名） |

### 61.6 T12.2 的落地顺序（下一轮的三刀）

1. `ShardReader` 契约加水位与 STALE（trait 改动，所有实现一起改）：
   `read_shard(id, known_manifest_ver) -> { batches, flushed_watermark, stale }`；
2. 查询侧 `Cache::hot` 从**单实例** `Option<Arc<dyn ShardReader>>` 变成**按实例**
   （`HashMap<InstanceId, Arc<dyn ShardReader>>`），`table.rs` 按实例切分（`source_instance`
   的**首个消费者**）；
3. **进程内双实例对拍**（两个 `ChunkStore` 当两个实例，不引入网络）：
   断言"每行只出一次"且与单节点串行**精确相等** —— 与 R3 在进程内先做 `Cluster`、
   再换 gRPC 传输是同一个套路。


---

## 62. R4 **T12.4/T12.5**：节点私有状态目录的排他所有权（`§28.2` 的显式化）（2026-09-20）

### 62.1 做什么：把"一个目录一个消费者"变成**启动期硬拒绝**

`§28.2` 记的事故是：同一份 WAL 目录被两个攒批循环消费 → 同一条 `Data` 被各吸收一次、
各自 flush → **9 批次 27 行变成 11 个文件 33 行**（不报错、只是持久重复）。当时修的是
**同进程内**的竞态（攒批循环的"退出闸门"）；R4 把 datanode 拆成进程后，跨进程**零防线**。

本轮落地：

| 件 | 内容 |
|---|---|
| `crates/model/src/private_dir.rs`（新） | `PrivateDirLease`：取目录排他所有权的租约 + owner 文件（`pid`/`instance_id`/`role`）+ 7 条单测 |
| `Lakehouse::build_with_shutdown` 第 **⓪** 步 | 装配**最前面**取 WAL 根目录 + spill 目录的租约，持有到进程退出 |
| 8 个 server 测试夹具 | 每个测试 = 一个节点：`spill_dir` 各用各的（原因见 62.3-①） |
| `crates/server/tests/private_dir_gate.rs`（新） | 2 条用例：第二个节点被拒且**点名谁占着**；拒绝**发生在动盘之前** |

### 62.2 实现选择：`std::fs::File::try_lock`（Rust 1.89 稳定）

- **零 unsafe、零新依赖** —— 全仓 `unsafe` 仍是 0（`flock` 走 libc 就得引 unsafe，不做）；
- 锁按**打开的文件描述**算：实测同进程再开一个 fd 同样 `WouldBlock` —— 所以它约束的是
  "**目录的消费者**"，不是"进程"，正是需要的语义（同进程起第二个攒批循环也必须拦）；
- **顺带成为本仓第一个真正需要 MSRV > 1.85 的 API**（`1.89`），与 `§60` 声明的 `1.94` 相容 ——
  文档里那句"没有 API 要求超过 1.85"从此有了一条反例，但方向是安全的。

**错误类型单列** `LakeError::DirBusy`，不塞进 `Io`：调用方必须能区分"目录被占"（停机 / 换目录）
与"磁盘坏了"（换盘）。报错文本由 `DirLeaseError` 生成，含占用者 `pid`/`instance_id`/`role`
与 `lsof <dir>/.yuntun-owner.lock` —— **这类故障的排查成本全在"谁占的"**。

**锁文件名的选择是有理由的**（`private_dir.rs` 的模块文档里写死）：`.yuntun-owner.lock`
必须避开两处清理逻辑的文件名模式（`purge_leftover_spills` 只删 `*.ipc.lz4`；WAL segment 清理
只删能解出段号的文件）。否则锁文件被删 → 已持有的锁**不会失效**，但下一个消费者会在**新 inode**
上拿到锁 → 保护**静默失效**。

### 62.3 落点选择与**已知盲区**

租约放在**节点装配层**（`Lakehouse`），不是 `WalWriter::open` / `ChunkStore::new`：

- 匹配 T12.4 的原话（"**每节点**私有状态初始化与校验"）：这是**节点级**不变量；
- 零签名改动（装配函数已是 `Result`）；下移到 store 层要 `ChunkStore::new` / `Ingestor::new`
  一起 `Result` 化，**波及约 14 个调用点**。

**盲区（明写出来，别装作没有）**：

1. **写者比节点活得久**的情形管不住 —— 例如 `drop` 掉 `Lakehouse` 后仍有 detached 任务持有
   `WalWriter`。跨进程的真实部署不受影响（进程退出 = 租约释放 = 写者也没了）；
2. 直接构造 `WalWriter`/`ChunkStore` 的**库用户**（chaos 夹具、ingest 测试）不走这个闸门
   —— 它们各自用自己的目录，属"没走节点装配"，不是漏洞但也不是保护；
3. 跨机器的 `instance_id` 重名**不在这里**（属成员注册 T12.3 / `refactor.md` S4-2）。

> 更强的形态（把租约放进 `WalWriter::open` 与 `ChunkStore::new`，让"目录的消费者"与"锁的持有者"
> 严格同一）登记为 T12.4 的后续项 —— 它需要上面那 14 个调用点 `Result` 化，与本轮的
> "先立节点级闸门"分开做。

### 62.4 闸门一开就抓出两处**真实的**不忠实（这是本轮最有价值的产出）

**① server 测试**全部**共用一份 spill 目录**。8 个测试都没配 `[chunk] spill_dir` → 沿用默认
`./data/spill`（CWD = `crates/server`）→ **多进程多线程共用一个私有目录**，而每个
`ChunkStore::new` 启动还会**清理**它（`purge_leftover_spills`）。闸门上线后 3 条用例立刻变红。

> **反例就在本仓里**：`meta.dir` 省略时早就用"**每实例一个临时目录**"解决同一问题了 ——
> `build_embedded_catalog` 的注释原话："同一个进程里可能起多个 Lakehouse（测试就是），
> 共用一个目录会让它们互相看到对方的表"。**spill 目录是唯一还没享受这个待遇的私有状态。**

**② 两处"崩溃重启"用例的收尾是错的**：`drop(_bg)`（`JoinHandle`）只是 **detach**，攒批循环
**还在跑** —— 于是"旧消费者没退出、新消费者已起来"，正是 `§28.2` 的形态本身。
改成 `cancel` 后 **`await` 句柄**（`metrics_e2e` 一直是这么写的）+ 显式 `drop` 旧节点。

> 两条都不是"闸门太严"，而是闸门**替我们把不忠实的地方找出来了**。修的是用例，
> 不是把闸门放宽。

### 62.5 验证

- `private_dir` 单测 **7 条**：同一目录第二个消费者被拒（同进程）/ 报错点名占用者 /
  释放后可接管 / 换实例接管会覆盖记录 / 记录格式向前兼容 / 读不出来不 panic / 自动建目录
- 闸门用例 **2 条**：第二个 `Lakehouse` 被拒（含"拒绝发生在动盘之前"：假遗留 spill 必须还在）/
  **两份**私有状态任一处冲突都拒
- 全量 `cargo test --workspace --no-fail-fast` → **312 passed / 0 failed**（含 62.4 修掉的那两类不忠实用例）

---

## 63. R4 **T12.2 第一刀**：把"水位 + STALE"变成契约（顺带抓出一个**真实存在**的竞态）（2026-09-20）

### 63.1 改了什么（`§61.4` 契约的代码化）

| 位置 | 改动 |
|---|---|
| `store::shard` | 新增 `ShardRead { batches, flushed_watermark, stale }`；`read_shard` / `read_table` 参数 `cached_snapshot` → **`known_manifest_ver`**（与 §61.4 用词一致），返回 `ShardRead` |
| `store::ShardFetch` | `fetch_shard` 的**响应**改为 `ShardRead` —— 水位**随 pull 响应回来**（`architecture §4.4` 原话），而不是靠推送 |
| `chunk::ChunkStore` | 新增单调水位 `released_watermark`（`AtomicU64`，只增不减）+ `flushed_watermark()`；`reclaim` 放弃副本时把它计入 |
| `query::table` | 适配新返回；`stale` 报 `warn!`（**`§64` 起改为消费**：报可识别的 `HotReadStale` → 刷新 → 重试） |
| 测试 | `store` 的远端接缝用例 + 查询侧 `hot_shard_reader` 用例适配；`chunk` 新增一条**定向复现竞态**的用例 |

合并口径写进了 trait 文档：**数据拼接、水位取 max、`stale` 取 or** —— 只要有一部分不可信，整个答案就不可信。

### 63.2 契约精化（本轮实测得出）：水位按"**已放弃本地副本**"记，不是"已 commit"

`§61.4` 第 1 条只说"水位单位 = manifest 版本号"。落到实现时发现还要定一件事：**记到哪一步**。

- 若按"**已 commit**"记：本仓的回收是**惰性**的（由查询缓存 swap 驱动），于是"已 commit、
  但本地副本还在"的窗口里，`stale` 会**每次查询都报** —— 而那个窗口里数据**读得到**，
  报的是**误报**；
- 按"**已放弃本地副本**"记则**精确**：没放弃副本 ⇒ 数据一定在本地 ⇒ **不可能丢** ⇒ 不该报警。

所以 `flushed_watermark` 的语义定为：**该实例已 flush 且已放弃本地副本的最高 manifest 版本**。
这是对 `§61.4` 的**细化**（安全方向不变：只在真有丢数据风险时报警）。

### 63.3 本刀真正的价值：`§4.5` 的"两头都没有"**今天就发生**（并已钉住）

`architecture-with-chunk §4.5` 说"任何已 seal 的数据，要么在 chunk 里可 pull，要么在 manifest
里可 read，**不会两边都不在**"。写这条契约时才发现：**它今天就会被违反**，而且不是分布式的特例：

```text
① 某批数据 commit 在 manifest 版本 5        （ChunkStore::mark_committed(id, 5)）
② 本地副本被回收                            （reclaim(6)：缓存已推进到 6，副本没用了）
③ 读者仍拿快照 4 来读                       （查询在 ① 之前取的可变快照，规划期还没结束）
   → 热数据拉不到（副本已放弃）
   → 它的 manifest（4）里也没有 valid_from=5 的文件（快照隔离按 4 判定）
   → **这批数据两头都没有** —— 而调用方只看到"空"，无从分辨"没有热数据"与"数据丢了"
```

**旧行为**：`read_table(t, 4)` 返回空 `Vec` ⇒ 查询**静默少这批数据**（正是本仓最怕的"静默错数据"）。
**新行为**：`ShardRead { batches: [], flushed_watermark: 5, stale: true }` ⇒ 调用方拿到了信号，
可以刷新 manifest 后用版本 5 重读（那时 `valid_from=5` 的文件可见 ⇒ 数据回来）。

用例 `chunk::store::tests::stale_is_reported_after_a_committed_chunk_was_released` 把三步全钉住，
并**同时钉住误报方向**：

| 断言 | 防的是什么 |
|---|---|
| 未回收时 `stale == false` 且能读到 3 行 | **误报**（若按"已 commit"记，这里就会报 stale） |
| 回收后 `stale == true` 且 `batches` 为空 | **静默丢数据**（旧行为在这里回一个空，谁都不知道） |
| `known` 到水位后 `stale == false` | 重试之后必须能恢复正常，否则 STALE 会变成永久状态 |

> 结论：**这条契约不是为分布式准备的"将来时"，而是把一个今天就能触发的静默错误变成可观测信号。**

### 63.4 遗留（下一刀 T12.2 第二刀）

1. **查询侧只记录不消费**（**已在 `§64` 落地**：刷新 manifest → 有界重试。以下为写 `§63` 时的原文）：`table.rs` 收到 `stale` 只打 `warn!`。**不当场报错**是刻意的 ——
   STALE 在"快照落后于提交"的**正常**窗口里也会出现（缓存 TTL 内提交过就会），报错会把常态变成故障。
   消费它需要"**刷新 manifest → 用新版本重试**"，落在 `Cache` / `QueryEngine` 层（与"按实例持有
   热读器"是同一层改动，见下一刀）。
2. **远端形态必须给真实水位**：`RemoteShard` 拿到什么就传什么。已写进 `ShardFetch::fetch_shard`
   文档：远端实现**不能**用 0 装作"没有已放弃的数据"——那等于把 STALE 静默关掉（S5-6 的活）。
3. **重启后水位归 0**：进程重启后本实例没有 chunk，水位从 0 重新计；它对"重启前提交的数据"不再
   给 STALE 信号（那部分数据的可见性由读者自己的 manifest 版本决定）。

---

## 64. R4 **T12.2 第二刀（上半）**：消费 STALE —— 刷新 manifest 后重试（`§63.3` 的窗口被真正补上）（2026-09-20）

`§63.4` 遗留①写着"查询侧只记录不消费"。本节把它消费掉：拿到 STALE **必须**刷新 manifest 后
用新版本重试（`§61.4` 第 2 条），修补 `§63.3` 钉住的那个"静默少一批数据"。

### 64.1 改了什么

| 位置 | 改动 |
|---|---|
| `query::table` | `HotReadStale { table, known_manifest_ver, flushed_watermark }`：把 STALE 报成**可识别的可重试错误**（`DataFusionError::External`） |
| `query::lib` | `with_stale_retry`：**有界**重试（上限 3），认出 `HotReadStale` ⇒ 刷新 ⇒ 重试；`sql` 与 `sql_stream` **两条路径都过**它 |
| `query::cache` | `LocalCatalog` 新增 `CatalogOps` 注入点（`set_catalog_ops`）与**同步刷新**入口（`refresh_now`），复用后台刷新那条 `refresh` 路径 |
| `server` | 装配层接线：`cache.set_catalog_ops(catalog.clone())` |
| 用例（新） | `query/tests/stale_retry.rs` **4 条**：能追上的重试成功 / 追不上的明确失败 / 没接线报配置错 / **已到位时不重试** |

### 64.2 三个设计点（都不是随便选的）

**① 用类型传"该重试"，不用字符串。** `HotReadStale` 是一个独立错误类型，`with_stale_retry`
从 `DataFusionError` 链里 `downcast` 出来。字符串匹配认不出来；而这类错误一旦被当成普通失败
上报，用户看到的就是**莫名失败** —— 比"少数据"好，但仍然是错的方向。

**必须沿链下钻**：DataFusion 会用 `Context` 把计划期错误包一层（"failed to create physical plan"之类），
只认最外层会漏掉它 —— 于是 STALE 又变回静默路径。

**② 重试包住"建会话 + 计划 + 执行"，而不是只包执行。** `TableProvider::scan` 在**物理计划期**被调用
（热读就在那里，见 `query/src/table.rs`），所以 STALE 从 `collect()` / `execute_stream()` 冒出来；
而 provider 持有的是**当次快照**，刷新 manifest 之后**必须重建会话**换新快照，否则重试还是拿旧的读。

**③ 刷新能力挂在 `LocalCatalog` 上。** 查询路径上只有 `Arc<LocalCatalog>`（`QueryEngine` 拿不到
`CatalogOps`），所以刷新所需的 ops 由装配层注入给它。`Arc<dyn CatalogOps>` 用薄包装 `OpsHandle`
存（trait 对象没有 `Debug`，而它出现在 Debug 输出里也没意义）。

三条约束（`§61.4` 第 2 条）：**必须重试** / **有界**（耗尽即报错，绝不静默降级）/ **刷新失败直接冒泡**
（刷不动就别硬撑——宁可失败也别给错数据）。

### 64.3 验证：四条用例，方向两两相对

| 用例 | 钉住的性质 |
|---|---|
| `stale_hot_read_is_retried_after_refreshing_manifest` | ①数据拿到（不是静默少行）②热读**至少两次**③**刷新计数真的涨了**④**第二次热读发生在刷新之后**（用"每次热读时的刷新计数"证，不只是"重试了"） |
| `permanent_stale_fails_loudly_instead_of_returning_partial_result` | 永远 STALE ⇒ **恰好撞 3 次上限**后明确报错（防"用不完整结果顶替"——那比失败更坏） |
| `stale_without_wired_ops_fails_with_a_config_error` | 装配漏了 `set_catalog_ops` ⇒ STALE 只能**明确失败**，不静默降级 |
| `caught_up_reader_does_not_trigger_retry` | **防误报**：`known` 已到位 ⇒ 不重试、不多刷一次 manifest（否则"每次查询都重试"会变成新的坑） |

后两条与前两条**方向相反**：只测"该重试时重试"会留下"不该重试时也重试"的隐患，那同样是坑。

### 64.4 遗留

1. **按实例持有热读器**（本刀的"下半"）：现在 `LocalCatalog::hot` 仍是**单实例**
   `Option<Arc<dyn ShardReader>>`，而水位/STALE 已按实例语义定义 —— 下一步把它变成
   `HashMap<InstanceId, Arc<dyn ShardReader>>`，`source_instance` 才真正有消费者；
2. **双实例进程内对拍**（第三刀）：两个 `ChunkStore` 当两个实例，断言"每行只出一次"且与单节点串行**精确相等**；
3. **远端形态必须给真实水位**（S5-6）与 **重启后水位归 0**（`§63.4` 的 2、3 条）未变。

---

## 65. R4 **T12.2 第二刀（下半）**：热读器按**实例**持有 —— `source_instance` 的第一个消费者（2026-09-20）

### 65.1 改了什么

`LocalCatalog::hot` 从**单实例**（`Option<Arc<dyn ShardReader>>`）变成**按实例**：

```rust
pub type HotShards = BTreeMap<String, Arc<dyn ShardReader>>;   // instance_id → 热读器
```

| 位置 | 改动 |
|---|---|
| `query::cache` | 字段类型 + `set_hot_shards(instance_id, reader)`（**签名变更**：不再有"唯一那个热读器"）+ `hot_shards() -> HotShards` |
| `query::provider` / `query::table` | 三个 provider 结构体与 `YuntunTableProvider` 都改持 `HotShards`（空 map = 未接线，替代原来的 `None`） |
| `query::table::scan` | **逐实例**拉：每个实例用自己的水位回答是否 STALE；`HotReadStale` 增加 `instance` 字段（"谁没追上"是多实例下最关键的诊断信息） |
| 装配 | `server` 用 `cfg.chunk.instance_id` 注册、`chaos` 用 `"chaos"` 注册（**就这一处**知道自己是哪个实例） |

**为什么必须是这一步**：`§61.4` 的水位语义**本来就是按实例定义的**（"每个实例自己的已 flush 水位"），
而在此之前查询侧只有一个"热读器"槽位 —— 那时 `source_instance` 在元数据里躺着，**读路径上无人消费**。
本刀之后，`FileManifest.source_instance` 与配置里的 `instance_id` 第一次成为**读路径的键**。

### 65.2 两个不是随便选的决定

**① 用 `BTreeMap` 而不是 `HashMap`：遍历顺序必须确定。**

多实例读出来的批次要拼在一起，拼接顺序会影响结果的**批次顺序**（乃至上层算子的行为）。
本仓的确定性纪律（`§38`：同一串输入 → 同结果）要求迭代有序 —— `HashMap` 的随机序会让
"同一份数据、同样的查询"偶尔产出不同顺序的批次，那是最难查的一类不确定性。

**② `shard_version()` 取各实例**之和**，不取 max。**

它的用途是"提交驱动刷新"的变更探测（"有没有东西变了"）。取 max 在**实例集合被替换**时
可能出现"看起来没变"（旧实例的高水位换成了新实例的低水位）；求和则对"是否有变化"是单调可靠的。
实例集合是**装配期固定**的，所以求和没有稳定性问题。

### 65.3 新用例：钉住"按实例"这个**结构**本身

`query/tests/hot_shard_reader.rs::hot_data_from_every_registered_instance_is_read`：
两个 `instance_id` 各注册一个热读器（各持一个分片、各一行），断言 `count(*) == 2`。

> 防的是：只读"某一个"实例 ⇒ 另一台的**未落盘数据静默消失**。这类错比报错难查得多 ——
> 它不违反任何 manifest 约束，只是"少了一层数据来源"。旧结构（单槽位）下写不出这条断言。

### 65.4 验证

- `cargo test -p yuntun-query`：含新用例与既有接缝用例全绿
- 全量：见 `status.md`（本轮跑完整 workspace）
- 装配点只有两处（`server` 与 `chaos`），各自的 `instance_id` 与写入侧**同源**（`cfg.chunk.instance_id` / `"chaos"`）

### 65.5 遗留

1. **第三刀**：两个 `ChunkStore` 当两个实例的**进程内对拍** —— 断言"每行只出一次"且与单节点串行
   **精确相等**（M4 判据的第一形态）；
2. 快照里的 `nodes`（"分片归属"）与 `hot` 的键集**目前同源但未强制一致** —— R5 做 fanout 时
   由成员发现统一（T12.3）；
3. 远端必须给真实水位（S5-6）/ 重启后水位归 0（`§63.4`）未变。

---

## 66. R4 **T12.2 第三刀**：双实例进程内对拍（M4 判据的第一形态）+ **T12.2 收口**（2026-09-21）

### 66.1 用例：两种装配跑同一批写入，逐行比

`crates/query/tests/two_instance_parity.rs`：

- **单节点串行**：一个 `ChunkStore`（实例 `solo`）收到全部 6 行；
- **双实例**：两个 `ChunkStore`（`inst-a` / `inst-b`）各收 3 行 —— 模拟两个 datanode 各写一半；
- 两边都跑 `SELECT a FROM ... ORDER BY a`，断言**逐行精确相等**且**恰好 6 行**。

两个实例刻意用**相同的 `(table, shard, window, epoch)`**：设计上"多个 datanode 可能同时写同一
partition，各自出各自的文件"（`architecture §5.1` 无归属写入），所以同名 key 才是真实形态 ——
用不同 shard 反而回避了 Hard 情况。

**比的是排序后的值**：批次边界与行序本就不在契约里（`§65.2` 的 `BTreeMap` 保证的是**确定性**，
不是某个特定的拼接顺序）；对拍要比的是内容。

### 66.2 为什么这条是 M4/R5 对拍的模板

`§4.4` 的"按实例切分"一旦实现错，**两种错法都不报错**：

| 实现错法 | 症状 | 本用例如何抓住 |
|---|---|---|
| 只读某一个实例 | **少**另一台未落盘的数据 | `split != all`（会得到 3 行） |
| 把同一份数据读两次 | **多**行（`plan.md §5.3-1` 的重复计数） | `split != all`（会得到 9 行） |

两条断言其实是同一句 `split == all`：**6 行不多不少**。这正是"对拍"比"单元断言"强的地方 ——
它不需要事先想到具体的错法。

> **它在上一刀之前是写不出来的**：那时只有一个热读器槽位，两个实例里只能注册一个 ⇒ 结果必然 3 行
> ⇒ 断言失败。所以这条用例是 `§65` 那次结构变更的**回归测试**，而不是它的装饰。

### 66.3 反证：证明主断言不是"碰巧相等"

`single_registered_instance_sees_only_its_own_rows`：**只**把 `inst-a` 接进来，
断言结果恰好 `[1,2,3]`。

> 必要性：如果"只注册一个实例"与"注册两个实例"结果一样，那主对拍就什么都没证明
> （`§58` 记过同款自欺："否则两边都空也能'相等'"）。反证把"两实例**确实**多读到了东西"钉住。

### 66.4 T12.2 收口：三刀齐

| 刀 | 内容 | 出处 |
|---|---|---|
| 一 | **契约**：`ShardRead { batches, flushed_watermark, stale }`；水位单位 = manifest 版本号 | `§63` |
| 二（上） | **消费**：`HotReadStale` + 有界重试（刷新 manifest → 用新版本重试） | `§64` |
| 二（下） | **按实例**：`HotShards = BTreeMap<instance, ShardReader>`，逐实例拉 | `§65` |
| 三 | **对拍**：双实例 vs 单节点串行，逐行相等 + 反证 | 本节 |

**T12.2 的语义闭环（进程内形态）到此完成**：`FileManifest.source_instance` 从"只写不读的元数据"
变成了**读路径的键**，并且"按实例切分错了会怎样"有了可执行的判据。

### 66.5 遗留（R4 的下一批）

1. **T12.1 拆进程**（`yuntun-ingestor` / `yuntun-queryd` / `yuntun-compactor`）：本节的"双实例"
   仍是**同进程内的两个 chunk store** —— 跨进程形态要等它，届时同一条对拍逻辑可直接复用
   （与 R3 先做进程内 `Cluster`、再换 gRPC 传输是同一个套路）；
2. **T12.3 成员发现 + 分片归属**：把"快照里的 `nodes`"与 `HotShards` 的键集统一为成员表（`§65.5`）；
3. 远端必须给真实水位（S5-6）/ 重启后水位归 0（`§63.4`）。

### 66.6 顺带观察：`chaos::compaction_during_query_keeps_counts_monotonic` 是**负载敏感**的

本轮第一次全量跑时它报 `查询样本太少（1)`；**单独跑该二进制（15 个 chaos 用例）立刻全绿**，
且 **2.60s vs 全量里同一二进制的 75.32s（29 倍差距）** —— 这就是"机器被拖慢"的指纹。
用例自己的注释已经声明门槛（`observed.len() >= 3`）对负载敏感：满负载机器上 10ms 间隔的采样
次数会明显变少，卡死数量会把机器快慢当成失败。

**不是本刀的回归**：本刀对单实例路径功能等价（`HotShards` 只有一项、不 STALE 就不重试、每次查询多
一次 BTreeMap 克隆）。

> 留作观察：该断言更稳的写法是"**等到采够样本（带超时）**"而不是"固定窗口里计数" ——
> 那样机器快慢只影响耗时，不影响判定。本轮不改（属 chaos 夹具的事，与 T12.2 无关）。

---

## 67. 数据面 gRPC 面：热数据分片拉取走网络（S5-6；R4 T12.1 的前置）（2026-09-21）

### 67.1 做了什么

`ShardReader` / `ShardFetch` 这两条缝早就留好了，但在此之前**只有进程内实现是真的** ——
`RemoteShard` 的传输在测试里是个假实现（`CannedFetch`）。本次把它变成能跑在网络上的东西：

| 件 | 内容 |
|---|---|
| `crates/proto/proto/shard.proto`（新） | `ShardFetch` 服务：`FetchShards` / `FetchShard` / **`FetchWatermark`**；`FetchShardResponse` 带 `batches_ipc` + `flushed_watermark` + `stale` |
| **`crates/shardrpc`（新 crate，第 19 个）** | 服务端 `ShardService`（包**任意** `Arc<dyn ShardReader>`）+ 客户端 `GrpcShardFetch`（→ `RemoteShard`）+ 批次编解码 + `serve()` |
| 用例 | `batches_watermark_and_stale_survive_the_wire`、`parity_holds_with_instances_behind_grpc` |

**为什么单独一个 crate**：`yuntun-proto` 的定位写着"只放接口，**不把 tonic 拖进查询/写入路径**"，
而 `store` 正被查询路径依赖、`chunk` 正被写入路径依赖 —— 谁都不能加 tonic。单开一个 crate 后，
只有**装配层**（将来的数据节点 / 查询节点进程）才引入它。

**批次编码复用 arrow IPC stream**（与 WAL 记录、spill 同一形态）：批次在系统里只该有一种线上编码。

### 67.2 用例当场抓到一个**契约级** bug：水位被当成了"分片的派生量"

第一版跑起来后，往返用例的 ①-b 断言失败：

```text
远端必须把 STALE 报回来 —— 否则协调者会把这批数据静默丢掉
```

**根因**：`RemoteShard` 用的是 trait 的**默认** `read_table` —— "枚举分片 + 逐片合并"，水位取
各片水位的 `max`。而 `reclaim` 之后那个 chunk 已经没了 ⇒ **分片枚举为空** ⇒ 合并出来的水位是
**0**、`stale = false`。也就是说：

> 实例明明已经放弃了一批数据的本地副本，却告诉协调者"我这儿没有已放弃的数据" ——
> 协调者于是**静默丢掉那批数据**。

这与 `§63.3` 是**同一个错误形状**（"数据两头都没有，而调用方只看到空"），只是从进程内挪到了
网络另一侧 —— 契约写对了，**默认实现把它悄悄推翻**。

**修法是结构性的，不是补一句文档**：

1. `ShardReader` 新增**必实现**的 `async fn watermark(&self, known) -> Result<ShardRead, _>` ——
   专门回答"本实例已放弃副本到哪了"，**与有没有分片无关**；
2. `ShardFetch` 新增**必实现**的 `fetch_watermark(...)`（proto 里对应 `FetchWatermark`）——
   **没有默认实现是刻意的**：任何默认值（包括 0）都等于**静默关掉 STALE**，而那正是本节的 bug；
3. 默认 `read_table` 改为**先读分片、后取实例水位**，水位与 `stale` **一律取实例级的**
   （分片级的值只是它的投影）。顺序也是刻意的：若在两者之间有数据被放弃，**后取**的水位能覆盖它
   （反过来会漏）。

> 一句话总结这条契约：**水位是实例级属性，不是分片级属性。** 分片枚举恰好为空，正是"刚放弃完
> 副本"的时刻 —— 那时它最不能报 0。

### 67.3 用例验什么

`batches_watermark_and_stale_survive_the_wire`（真 TCP + gRPC + IPC 编解码）：

| 阶段 | 断言 |
|---|---|
| 未放弃副本 | 3 行完整过网络；水位 0；**不**报 STALE（防误报方向） |
| `reclaim(6)` 之后 | 热路径 0 行（副本确实没了）；**报 STALE**；水位 **5** 带得回来 |

`parity_holds_with_instances_behind_grpc`：`§66` 那条**对拍**在远端形态下同样成立 ——
单节点串行 vs 两个 gRPC 数据节点各写一半，**逐行相等**。这正是 `§66.5` 承诺的
"跨进程形态复用同一条对拍逻辑"的兑现（差别只在**传输**，判定逻辑一行没改）。

### 67.4 遗留

1. **尚未接线**：standalone 仍是进程内热读（`set_hot_shards(instance_id, chunks)`）—— 本节的
   客户端是**为 T12.1 拆进程准备的**；届时装配点把 `GrpcShardFetch` 包成 `RemoteShard` 注册即可；
2. **`version()` 还没上 wire**：`RemoteShard::version()` 仍走 `ShardFetch::fetch_version` 的默认 0，
   于是"提交驱动刷新"在远端形态下退化成 TTL（`shard.rs` 的 trait 文档已写明这一退化）；
3. 本节的"跨网络"仍是同进程内的 gRPC 往返；**真跨进程**随 T12.1。

---

## 68. T12.1 第一刀：**数据节点进程** `yuntun-ingestor`（2026-09-21）

### 68.1 拆的是什么

单进程形态下，"吸收 WAL / 持有热数据 / 对外提供热读"三件事都挤在 `standalone` 里，
于是热读只能是**进程内函数调用**（装配层把 `Arc<ChunkStore>` 直接交给查询侧）。本刀把
**数据节点**摘出来成独立进程（t12.1 要拆三个：`ingestor` / `queryd` / `compactor`，这是第一个）：

```text
yuntun-ingestor 进程 ── 私有 WAL → 吸收 → chunk store（热数据）
       │  gRPC（shard.proto）+ arrow IPC
查询侧：GrpcShardFetch → RemoteShard → QueryEngine
```

它启动时做的事，顺序是刻意的：

| 序 | 动作 | 为什么是这个位置 |
|---|---|---|
| ① | `private_dir` 租约（`wal-root` / `chunk-spill`） | **排在所有会改文件的操作之前**：被拒的进程连 WAL 都不该打开（`§62` / R-13） |
| ② | `WalWriter::open`（自带 recovery） | 数据节点热数据的**真相来源**：`synced_seq` 从盘上恢复，回放即恢复热数据 |
| ③ | `MemoryCatalog` + 本地冷存根 | 元数据面接 metanode 属 T12.3；冷文件先落本机目录 |
| ④ | `Ingestor::new(...)` + `spawn_accumulator` | **`Ingestor` 自带 chunk store** ⇒ 写侧与热读侧同一实例 = "读己之写" |
| ⑤ | `shardrpc::serve(chunks, listener)` | 先 bind 再打印 `LISTEN <addr>`：上层拿到的是**真实**地址（`:0` 由内核分配） |

**与单进程形态不是两套实现**：组件、契约全同（`Ingestor` / 私有目录租约 / `ShardReader`），
差别只在**传输** —— 查询侧从"拿到 `Arc<ChunkStore>`"换成"拿到 `GrpcShardFetch` 包出来的
`RemoteShard`"。所以 `§66`/`§67` 的对拍逻辑**一行都不用改**（见 68.3）。

### 68.2 数据怎么进进程：**回放它自己的 WAL**

跨进程的"写入面"（客户端如何把数据交给数据节点）**尚未定**，本轮不去发明它。用例的喂法是
**预置它的 WAL，让进程启动后自己回放** —— 这不是为测试特设的通道，而是数据节点**重启后的
真实恢复路径**（`§28.2` 的 R-13 教训正在此处继续成立：同一私有目录同一时刻只有一个消费者）。

副产品：这条用例顺带把"崩溃恢复后热数据必须自己长回来"也验了。

### 68.3 三条既有承诺的兑现（全部由用例钉住）

| 承诺出处 | 用例 | 结果 |
|---|---|---|
| `§66.5`"跨进程形态可复用同一条对拍逻辑" | `parity_holds_across_processes` | 复用，判定逻辑一行没改 |
| `§67.4`"真跨进程随 T12.1" | `hot_rows_cross_a_real_process_boundary` | 热数据在**另一个进程**里长大，经 gRPC 被查到 |
| `§28.2`/`§62`"同一私有目录只有一个消费者" | `second_process_on_the_same_private_dir_is_rejected` | 第二个进程**启动即拒**，且不打印 `LISTEN`（连 WAL 都没开） |

对拍仍然是 `§66` 那一条：**单节点串行 vs 两个数据节点各写一半，逐行相等**，
外加**反证**（只注册 `inst-a` 时只看到它自己那 3 行）—— 防"主断言空转"。

### 68.4 用例怎么起进程（可复用手法）

沿用 `metanode_process_e2e` 的范式（`crates/ingestor/tests/cross_process_hot_read.rs`）：

- `env!("CARGO_BIN_EXE_yuntun-ingestor")` 拿被测二进制路径；`--listen 127.0.0.1:0` 由内核分配端口，
  **进程把真实地址打到 stdout**（`LISTEN <addr>`），用例读这一行 —— 既避免端口互抢，也不需要
  "先探测端口再起服务"的 TOCTOU 窗口；
- stderr 后台收进 `String`：断言失败时能打出来（否则只剩"提前退出"四个字）；
- `Drop` 里 `kill + wait`：用例失败也不留孤儿进程；
- 热数据"长出来"是**有延迟**的（攒批扫描间隔 100ms 量级）⇒ 用例 `wait_rows` 轮询而不是 `sleep` 固定值。

### 68.5 遗留

1. **写入面未定**：客户端如何把数据交给数据节点（新 RPC？还是数据节点自己消费源？）——
   本刀只走"自己的 WAL"，属恢复路径；
2. **`queryd` / `compactor` 未拆**：`standalone` 仍是可用的全功能形态，三者尚未对等；
3. **元数据面仍是本地 `MemoryCatalog`**：跨进程共享（成员发现 / 分片归属 / DDL 可见性）属 T12.3；
4. 数据节点缺 WAL 超时监控 / compaction / 孤儿清理（按设计归别的进程）。

---

## 69. T12.3 第一刀：**成员表作为唯一真相**（2026-09-23）

### 69.1 关掉的是 `§65.5` 那个坑

在此之前，"有哪些实例"与"能读谁的热数据"是**两份各自更新的真相**：

```text
LocalCatalog
├── nodes: RwLock<Vec<String>>                  ← 快照的"分片归属"（装配层 set_nodes 写）
└── hot:   RwLock<HotShards>（instance→reader）  ← 查询实际拉热数据的键集（set_hot_shards 写）
```

`§65.5` 已经点明它们"**同源但未强制一致**"。漂移的后果不是风格问题：
按错误的实例集合去算归属，就是**查询静默少数据**（名录里有、读不到）或归属算错。

**设计依据**（不是自创）：

- `architecture-with-chunk §3.2`：成员发现分两层 —— **成员名录走 raft**（只在上/下线时变）；
  **存活状态秒级心跳，绝不进 raft**（会把写爆）；
- `§3.1`：成员名录是元数据面的事；而"chunk 位置（谁持有哪个分片）**无需存储**"（直接问 datanode）。

### 69.2 改成什么

```rust
pub struct Member {
    pub instance_id: String,
    /// 数据面地址（host:port）。None = 同进程 / 尚未上报
    pub address: Option<String>,
}
```

- `LocalCatalog` 内部只剩一张 `members: BTreeMap<String, Member>`；
- **快照的 `nodes` = 成员表 ID 清单**（`member_ids()`）——不再是另一份 `Vec`；
- `set_hot_shards(instance, reader)` **顺带登记成员** ⇒ 不变式：**热读器键集 ⊆ 成员表**，
  且 `nodes` 由成员表派生 ⇒ 两者**结构上不可能再漂移**；
- `set_members(Vec<Member>)` 是**权威替换**（成员发现 / 装配层调用）；
  被摘除的成员，其热读器**一并移除**，并 `warn!` 留痕（摘除是有语义的，不该无声发生）。

**为什么地址必须进成员表**：只有 `instance_id` 时，"有哪些实例"变不成"怎么连" —— 而 R4 的查询侧
恰恰要**按发现到的成员装配热读器**（这正是 `§67` 那个 `GrpcShardFetch` 的用武之地）。
R5 的 fanout（T13.1）也要从成员表拿"活跃 datanode 列表"。

**摘除为什么是安全的**（不是靠本函数保证）：成员摘除走 raft，且按设计只在它的文件已提交
**之后**发生 —— 所以摘除那一刻，它那份数据必然已能从冷侧读到。用例 ③ 把这条语义钉住了。

### 69.3 实现中修掉的一个隐患：**锁序必须单向**

两处写入都会碰两张表：`set_hot_shards`（先成员、后热读器）与 `set_members`（同样先成员、后热读器）。
第一版 `set_members` 在**持有热读器写锁**时又去取成员表读锁 ⇒ 两个方向的加锁路径同时存在，
并发下会**互相死锁**。已统一为**成员表 → 热读器表**单向加锁，并在两处注释里写明顺序
（否则下一个人很容易再写出反向路径）。

### 69.4 用例（`crates/query/tests/member_table.rs`）

| 用例 | 钉住什么 |
|---|---|
| `registering_a_hot_reader_also_registers_the_member` | 登记热读器 ⇒ 成员表与**快照 `nodes`** 都必须含它（旧形态下这两处可以互相矛盾） |
| `members_carry_data_plane_addresses` | 地址存得住、取的回，且成员表按 ID 有序（取值确定） |
| `removing_a_member_takes_its_hot_shards_out_of_queries` | 摘除成员 ⇒ 它的热数据**不再参与查询**（两行→一行的可观测变化），并带**反证**（摘除前两行都在） |

### 69.5 遗留

1. **成员表目前只由装配层写**（`set_members`）：向 metanode 注册 / 心跳保活 / 超时摘除
   是 T12.3 的后续（`§3.2` 的两层机制）；
2. **归属不存储**：谁持有哪个 `(table, shard)` 按 `§3.1` 直接问（`ShardFetch::fetch_shards`
   这条缝已在），查询侧尚未用它来收窄拉取范围 —— 属 R5 的 fanout（T13.1）；
3. 数据节点进程（`§68`）尚未向任何成员表注册，地址仍由装配层手工给出。

### 69.6 顺带修掉一条**时序 flake**（`§66.5` 预言过的那个）

全量跑到 chaos 时挂了一条：

```text
test compaction_during_query_keeps_counts_monotonic ... FAILED
  查询样本太少（1)，无法支撑并发断言
```

**不是本刀的回归**：该用例的查询采样任务每 10ms 采一次、而压缩循环固定跑 3 轮 —— 满负载机器上
3 轮可能整个落在两次采样**之间**，于是"样本数"成了**机器快慢的函数**（当时负载 115）。
`§66.5` 早就把它记成"留作观察：更稳的写法是**等到采够样本（带超时）**，而不是固定窗口里计数"，
只是当时判为"属 chaos 夹具的事"。它现在真的触发了，就按那条记录改掉：

- 循环条件由 `for round in 0..3` 改为「**轮数下限 3 且 样本数下限 3**，带 30s 超时」；
- 断言消息带上轮数与超时说明，便于下次一眼区分"机器慢"与"真缺陷"。

修完连跑 3 遍稳定（每次 0.22s，样本数足够）。**教训**：夹具里任何"固定时间窗内计数"的断言，
都在把机器快慢偷偷变成判定的一部分 —— 要么等到条件成立（带超时），要么断言与时间无关的量。

---

## 70. 构建提速 + T12.3 第二刀（上半）：数据节点注册走 raft（2026-09-23）

### 70.1 先纠正一个假设：**磁盘不是瓶颈**

怀疑是"`rust-lld` 慢、磁盘读写满"。实测数据不支持这个归因：

| 观测 | 值 | 含义 |
|---|---|---|
| `vmstat` 的 `wa` | 1–7% | **I/O 等待几乎为零** —— 磁盘没满 |
| `us` / `sy` | 92% / 7% | 瓶颈在 **CPU** |
| 磁盘型号 | `sda` `ROTA=0` | SATA **SSD**（另有 NVMe 只挂了 `/boot`，用不上） |
| 同时在跑的链接器 | **24**（12 核机器） | 3 倍超订 → `cs` 11 万次/秒的**上下文切换风暴** |
| 当时的 load | **115** | 就是这场风暴 |

真正的开销是**链接输入体量**：`[profile.release]` 早已把 `debug` 降到 `line-tables-only`
（注释里写着"完整调试信息会让二进制达到 2.3GB"），但 **`[profile.dev]` 是默认的 `debug = true`**
—— 每个测试二进制都塞满完整 DWARF。

### 70.2 改了什么

```toml
[profile.dev]
debug = "line-tables-only"      # 保留文件/行号：backtrace 仍能定位失败用例

[profile.dev.package."*"]
debug = false                   # 依赖（datafusion/arrow…）是体积大头
```

外加**构建时 `-j 8`**（12 核机器上让 12 个任务各再叉多线程 lld，只会互相抢核）。

效果：同样的全量构建，load 从 **115 → 6.65**（不再有上下文切换风暴）。
代价与用法：

- **改 profile 会让构建缓存一次性失效**（要全量重建一遍，之后一直快）；
- 调试只剩行号：需要变量/类型时用 `CARGO_PROFILE_DEV_DEBUG=2 cargo test …` 单次覆盖；
- `target/debug` 现在 **18G**（含全部测试二进制）。

### 70.3 T12.3 第二刀（上半）：注册 op 走 raft，名录进状态机与快照

**为什么是 op 而不是旁路注册接口**：`architecture-with-chunk §3.1` 要求成员名录与
schema/manifest **同版本**读出去 —— 否则查询侧会拿"新的文件清单 + 旧的节点集合"拼计划。
而**心跳绝不走这条路**：秒级心跳会把 raft 写爆（`§3.2`），它属第二刀的下半。

| 件 | 内容 |
|---|---|
| `meta.proto` | `Op.kind.register_datanode = 11` + `RegisterDatanodeOp { instance_id, address }` |
| `yuntun-model` | `DatanodeMember { instance_id, address, registered_at_ms }`（做成 prost 消息，才能进快照） |
| `CatalogState` | `datanodes` + `register_datanode()`（**同 ID 同地址 ⇒ 不推进版本**） |
| `meta op` | `StateOp::RegisterDatanode` + decode + apply |
| 快照载荷 | `CatalogStateSnapshot.datanodes = 12`，**并把 `SNAPSHOT_FORMAT_VERSION` 1 → 2** |

**幂等为什么是关键**：节点每次重启都会重注册（同地址）。若每次都推进 `schema_ver`，
全体客户端会被反复拉去**全量重建快照** —— 一个健康检查式的动作变成集群级抖动。用例把这条钉住了。

**格式版本必须 bump**：旧构建读到新载荷会**明确拒绝**，而不是默默忽略未知字段 ——
名录被静默丢掉，正是 `§69` 那类"查询静默少数据"。

### 70.4 用例当场抓到的设计含糊：注册时刻有**两个来源**

初版把 `registered_at_ms` 既放在载荷里、又在 `decode_op` 里填 `now_ms` ⇒ 测试直接构造 op 时
两者不一致，断言立刻失败。改为**由 `apply` 用 op 的 `now_ms` 落章**（载荷不再自述时间）：
时间戳是状态的一部分，若允许调用方随 op 带进来，同一串 op 在不同副本上会得到不同的名录时刻。

**另一个教训（本仓第二次踩）**：往文件里插新类型时，锚点若选在**结构体行**，容易插进
`#[derive(...)]` 与结构体之间 —— 于是 derive 挂到了新类型上、原类型丢掉 derive
（上次是 `cache.rs` 的文档归属，这次是 `model/src/meta.rs` 的 `prost::Message`）。
锚点一律选 `#[derive]`/注释行，别选结构体行。

### 70.5 验证与遗留

- `cargo test -p yuntun-model -p yuntun-catalog -p yuntun-meta` → 29 / 39 / 进程 e2e 全绿
- 全量 `cargo test --workspace --no-fail-fast` → **329 passed / 0 failed**；clippy 本仓 **0**
- 新增 `wire_compat` 用例：注册 op 载荷往返逐字段不变（仓库规矩：每加一种 op 都要有）

**下半的遗留**：

1. **读路径**：把名录下发出去（`PrefetchPayload.datanodes`）+ `LocalCatalog` 消费它
   （`set_members`）—— 到那时"成员表自动发现"才真正闭环（`§69` 的成员表至今仍由装配层手填）；
2. **心跳与超时摘除**：metanode 内存 + 独立 RPC（**永不进 raft**，`§3.2`）；
3. **ingestor 自注册**：`yuntun-ingestor --meta <addr>` + 启动注册（`register_datanode_to_proto` 随它落地）；
4. 改 profile 后**旧快照载荷（v1）会被拒绝** —— 本地开发数据需重建（`§70.2`）。

---

## 71. T12.3 下半（第 1 步）：名录下发闭环 —— 成员表第一次**自己长出来**（2026-09-23）

### 71.1 缺的那一环

`§69` 把成员表立成"唯一真相"，`§70` 让数据节点能**注册进 raft**。但名录还停在元数据里 ——
查询侧的成员表仍由装配层**手填**。本步把它接上：

```text
数据节点注册（op，走 raft）
   → CatalogState.datanodes
   → PrefetchPayload.datanodes（与 schema/manifest **同一次**响应，§3.1）
   → LocalCatalog::refresh 消费 → set_members（含**数据面地址**）
```

**同一次响应**是刻意的：分两个接口读就会出现"新文件清单 + 旧节点集合"的拼计划窗口。
而名录变化会推进 `schema_ver`（`§70.3`）⇒ 必然伴随一次全量刷新，所以只需 `Prefetch` 带它，
不必让 `Delta` 也带 —— 设计在这里是自洽的。

### 71.2 改了什么

| 件 | 内容 |
|---|---|
| `meta.proto` | `PrefetchPayload.datanodes = 5` + `DatanodeMemberMsg{instance_id, address, registered_at_ms}` |
| `CatalogOps` | `register_datanode`（写）+ `datanodes`（读）—— **都没有默认实现** |
| `MemoryCatalog` | 直接读写自己的状态 |
| `RemoteCatalog` | 写 = `Propose`（raft op）；读 = **先 `refresh()` 再读缓存** |
| `LocalCatalog::refresh` | 名录**先落地**（在构造快照之前，`§3.1`）；**读失败 = 本次刷新失败** |
| 装配层 + 6 个测试夹具 | 本实例登记进名录（见 71.4） |

**为什么两个方法都不能有默认实现**：任何默认值（含"空表"）都等于"没有数据节点" ——
查询会据此按空成员表算归属，正是 `§69` 那类**静默少数据**。

### 71.3 用例揪出的坑：远端读必须在**已 prime** 的缓存上

改完全量测试挂了 14 条，症状一律是"查询返回 **0 行**"。定位用了两步硬证据（不是猜）：

1. 临时 `eprintln!` 打出名录与热读器键集 —— 立刻看到：

   ```text
   名录 ids=[]；当前 hot 键=["standalone"]     ← 刷新先来 ⇒ 把热读器摘了
   名录 ids=["standalone"]；当前 hot 键=[]      ← 登记后也补不回来
   ```

   （同时印证：`tracing::warn!` 在测试里**无处可去**，所以"没看到告警"不能当证据。）
2. 临时打 `Backtrace::force_capture()` —— 看到第一次刷新来自**后台刷新任务**，
   而装配默认走 `MetaMode::Embedded` ⇒ 查询侧拿到的是 **`RemoteCatalog`**。

**根因**：`RemoteCatalog::datanodes()` 读的是**客户端缓存**，而缓存由它自己的 `Prefetch` 填充；
查询侧刷新把名录读放在了**最前面**，那一刻缓存还没 prime ⇒ 读到空名录 ⇒ 把自己的成员表清空。
**修法**：`datanodes()` 与 `list_tables` 等读方法同形 —— **先 `self.refresh().await?`** 再读缓存。

> 教训：一个"读缓存"的方法必须和它的兄弟方法走**同一条** prime 路径。
> 少了这一步不会报错，只会静默给出"空"，而空的语义在这里恰好是**最危险**的那个。

### 71.4 顺带的语义后果：夹具也得登记

"名录是唯一真相"意味着**不登记 = 不存在**：装配层与 6 个测试夹具都补了本实例登记
（`address` 空 = 同进程实例）。这不是测试的将就，而是这条语义的直接推论 ——
新用例 `roster_from_metadata_populates_the_member_table` 把它写成正面断言：

- 名录里的 `inst-b` **没接线读侧也出现在成员表**（这就是"发现"），且**带数据面地址**；
- 名录里的地址**覆盖**本地登记（`inst-a` 从"无地址"变成 `10.0.0.7:50051`）—— 名录是权威；
- `inst-a` 在名录里 ⇒ 刷新**不得摘掉**它的热读器（`§70` 那个坑的反面），读己之写不受影响。

### 71.5 验证与遗留

- 全量 `cargo test --workspace --no-fail-fast` → **331 passed / 0 failed**；clippy 本仓 **0**
- 新增 `wire_compat` 用例：名录条目往返逐字段不变（仓库规矩：每加一种载荷都要有）

**遗留**：

1. **心跳与超时摘除**：metanode 内存 + 独立 RPC（**永不进 raft**，`§3.2`）；
2. **ingestor 接 metanode**：`--meta <addr>`，让注册走真 raft（现在仍是本地 catalog；
   同一个 trait 方法，装配点换一行即可）；
3. 成员表里的成员若**没有接线热读器**，目前只是"存在但不参与拉取" —— 谁来接线（谁负责
   按地址建 `GrpcShardFetch`）属 R5 的 fanout（T13.1）。

---

## 72. T12.3 下半（第 2 步）：心跳保活 + 超时摘除（2026-09-23）

### 72.1 两条纪律必须在**代码里分开**

`architecture-with-chunk §3.2` 把成员发现切成两层，这一刀落的就是它：

| 层 | 内容 | 机制 | 频率 |
|---|---|---|---|
| 成员名录 | 有哪些 datanode、ID 与地址 | **raft**（`register` / `remove` 两个 op） | 上下线才变 |
| 存活状态 | 谁还活着 | metanode **内存** + 心跳 | 秒级 |

**心跳绝不进 raft**：秒级心跳会把写路径压垮。但**摘除必须进 raft**：名录要与
schema/manifest 同版本传下去（`§3.1`）。两者混在一起会二选一地坏掉 —— 要么写爆 raft，
要么让各副本对"谁存在"产生分歧。

### 72.2 落点

| 件 | 内容 |
|---|---|
| `NodeStatus.last_seen` | `HashMap<String, Instant>` —— 存活表（**内存**，不进状态机/快照） |
| `NodeHandle::heartbeat(id)` | 更新存活 + 返回 **`known`**（是否还在名录里） |
| `spawn_liveness_sweep(handle, timeout, interval, shutdown)` | leader-only 巡检；超时者经 `propose` 摘除 |
| `meta.proto` | `rpc Heartbeat` + `RemoveDatanodeOp{instance_id, reason}`（oneof arm 12） |
| `CatalogState::remove_datanode` | 摘除 + 推进 `schema_ver`（与注册同源） |
| `metanode` 进程 | 装配巡检：**15s** 未心跳即摘（数据节点每 5s 一次，抗一次抖动） |

三个刻意的选择：

1. **存活表放 `NodeStatus`**（`Arc<Mutex<..>>`，两处构造点都在用）⇒ **构造点零改动** ——
   `..Default::default()` 的老实好处；
2. **用 `Instant`（单调钟）**：墙钟跳变不该改变"谁还活着"的判定；
3. **`heartbeat` 返回 `known`**：`false` 的语义是"你已被摘除，**请重新注册**"——
   少了它，一个被摘掉的节点会永远安静地空发心跳，而谁的名录里都没有它（另一种静默）。

### 72.3 巡检的三条纪律（都有具体的坏后果）

1. **只有 leader 提议**：判据是 `leader_id == self.id` **且 `leader_id != 0`** ——
   未知时一律不提议（宁可晚摘，不可错摘）；
2. **先播种、后判定**：名录里**第一次见到**的成员按"此刻还活着"记一笔。没有这一条，
   新 leader（内存存活表是空的）第一轮巡检就会把**整个集群**摘掉；
3. **摘除失败不清存活记录**：下一轮还会看到它仍超时，于是**重试**；
   清掉就等于"提议失败 = 它活着"（把失败当成功）。

`propose` 是**阻塞**的（等提交）⇒ 整个巡检体放在 `spawn_blocking` 里，不占异步 worker。

### 72.4 用例（`crates/meta/tests/liveness_e2e.rs`）

用**进程内**真节点（`MetaNode::open` 单节点 = 天然 leader），验的是语义链而不是传输：

| 步 | 断言 |
|---|---|
| ⓪ | 等选主：单节点也要走完一次选举（判据用**对外**可见的 `leader_id`，1 = 本节点） |
| ① | 注册（走 raft 的 op）⇒ 名录里有它 |
| ② | `heartbeat` 的两种返回：在名录里 `true`；不在 `false`（**这就是"该重新注册"的信号**） |
| ③ | 起巡检（超时 300ms / 巡检 50ms） |
| ④ | 持续心跳 ⇒ **不被摘**（否则巡检就是在随机删节点） |
| ⑤ | 停心跳 ⇒ 超时后**从名录消失**（读的是 `Prefetch` 载荷 ⇒ 确实经 raft 落了状态） |
| ⑥ | 摘除后再心跳 ⇒ `known = false`（**摘除是可恢复的**） |

### 72.5 验证与遗留

- 全量 `cargo test --workspace --no-fail-fast` → **333 passed / 0 failed（+1 ignored）**；clippy 本仓 **0**
- 规模：41,213 行 / 20 个 crate / 333 测试函数

**遗留**：

1. **数据节点侧的心跳循环还没写**：`yuntun-ingestor --meta <addr>` + 每 5s 一次 `Heartbeat`
   （收到 `known=false` 就重新注册）—— 这是 T12.3 的最后一块拼图；
2. **多 metanode 时存活表是「每副本各记一份」**：本刀只在 leader 上判定，follower 的表空着；
   换主靠"先播种"兜住（不会误摘），但**换主后到播种之间的失联**要等下一轮才被发现 ——
   真要收紧，得让心跳带上 leader 提示或让 follower 也参与判定（属多 metanode 的后续）；
3. R5 的 fanout（T13.1）应当**只向"活跃"成员拉数据** —— 本刀给出的存活表就是它的输入。

---

## 73. T12.3 收口：数据节点侧的心跳循环（2026-09-23）

### 73.1 这一刀把 T12.3 合上

`§70`（注册走 raft）→ `§71`（名录下发，成员表自动发现）→ `§72`（心跳保活 + 超时摘除）
→ **本节**（数据节点自己会入册、会保活、被摘了会自己回来）。至此 T12.3 的四处拼图齐了。

| 件 | 内容 |
|---|---|
| `CatalogOps::heartbeat(id) -> bool` | **无默认实现**（同 `register_datanode`/`datanodes`） |
| `MemoryCatalog` | 名录就在自己手里 ⇒ 本地判断（规则与 metanode 一致） |
| `RemoteCatalog` | 走 `Heartbeat` RPC：轮换地址 + 超时，与 `status`/`prefetch` 同形 |
| `yuntun-ingestor` | `--meta <addr>`（给了就接 metanode）+ `--heartbeat-secs`（默认 5） + 心跳循环 |

### 73.2 心跳循环的三种结果（与 metanode 侧一一对应）

| 结果 | 含义 | 处置 |
|---|---|---|
| `Ok(true)` | 还在名录里 | 什么都不做 |
| `Ok(false)` | **不在名录里**：被超时摘除 / 从未入册成功 | **重新注册**（⇒ 摘除可恢复，`§72.2`） |
| `Err(_)` | 元数据面不可达 | 告警 + 下一轮重试（**不退出**） |

**入册是 best-effort**：元数据面晚起来只意味着"暂时不在名录里"，而不是"数据节点起不来" ——
数据节点的核心职责（吸收自己的 WAL、服务热读）不依赖元数据面。这条让启动顺序不再是约束。

### 73.3 一个被显式化的耦合：心跳间隔 ↔ 巡检超时

两者**必须配套**（metanode 15s 没见到就摘 ⇒ 数据节点 5s 一次，三次机会抗一次抖动）。
写测试时这一点立刻咬人：用例要把巡检调到秒级，而心跳间隔原本是写死的常量 ⇒
**摘除会在第一次心跳到来之前发生**。于是把间隔做成 `--heartbeat-secs`（默认 5）：
不只是为了测试，运维本来就需要在"元数据面压力"与"掉线发现速度"之间取舍。

### 73.4 用例：`crates/ingestor/tests/meta_membership_e2e.rs`

三个角色全是真的，没有替身：

```text
  metanode（进程内起真 gRPC 服务，巡检口径调到秒级）
       ▲ RegisterDatanode（raft op）   ▲ Heartbeat（只碰内存）
  yuntun-ingestor 子进程（--meta + --heartbeat-secs 1）
```

| 步 | 断言 |
|---|---|
| ① | 起 metanode + `serve`（真 gRPC）+ 巡检（2.5s 超时 / 200ms 巡检） |
| ② | 起数据节点子进程：`--meta` ⇒ 启动即入册 + 心跳 |
| ③ | **名录里出现它**（经 gRPC → raft → 状态机 → `Prefetch` 载荷） |
| ④ | 跨过**不止一个**心跳周期后仍在（否则巡检就是在随机删节点） |
| ⑤ | 杀掉子进程 ⇒ 心跳停 ⇒ 超时后**从名录消失** |

**观察点是查询侧看名录的那条路**（`Prefetch` 载荷），不是内部字段 ——
"名录是不是真的经 raft 落进了状态"，只有从这条路看才算数（`§71` 的同一条理由）。
用例 3.85s 跑完。

### 73.5 T12.3 收口与遗留

**T12.3（成员发现 + 分片归属）到此完成**：注册（raft）→ 下发（同一次元数据响应）→
保活（内存心跳）→ 摘除（raft）→ 自愈（`known=false` 重注册），且每一步都有用例钉住。

仍然遗留（都记在各自的章节里）：

1. **多 metanode 的存活判定一致性**（`§72.5` 第 2 条）：本刀只在 leader 上判定；
2. **R5 的 fanout（T13.1）**：只向"活跃"成员拉数据 —— 存活表与名录就是它的输入；
3. **T12.1 的其余两个进程**（`queryd` / `compactor`）与写入面仍未拆。

---

## 74. T12.1 第二刀：**查询节点进程** `yuntun-queryd`（2026-09-23）

### 74.1 分工：数据节点写、查询节点查

| | 数据节点（`yuntun-ingestor`） | **查询节点（`yuntun-queryd`）** |
|---|---|---|
| 私有状态 | WAL + chunk（**写**） | **无**（不持有任何本地数据） |
| 元数据 | 向 metanode 注册 + 心跳 | **只读** metanode |
| 热数据 | 自己持有、对外提供 | 向各数据节点**拉**（gRPC 数据面） |
| 冷数据 | 写 | 读（共享对象存储） |
| 写入面 | 接受 | **明确拒绝**（只读节点） |

### 74.2 补上的那一环：谁按地址建 `GrpcShardFetch`

`§71` 让名录（含**数据面地址**）随元数据下发，但当时没有答案的是——**谁拿地址去建连接**
（`§71.5` 遗留第 3 条）。`reconcile_hot_readers` 就是那个答案，两步顺序刻意：

1. `cache.refresh(catalog)` —— **成员表先落地**（摘除也在这里发生：名录里没有的实例会被
   `set_members` 连热读器一起摘掉，`§69`）；
2. 给"有名录但**还没接线**"的成员装 `GrpcShardFetch` → `RemoteShard` → `set_hot_shards`。

**只补缺、不重建**：已接线的实例保住已有连接 —— 否则连接数会变成名录巡检频率的函数。

### 74.3 "只读"写进**类型**里，而不是写在注释里

查询节点没有写入侧不是策略选择，而是**装配事实**（没有 WAL、没有私有目录）：

| 件 | 改动 |
|---|---|
| `SqlEngine.ingest` | `Option<Arc<Ingestor>>` + `new_readonly`（`WritePolicy::ReadOnly` 早在，`SqlError::ReadOnly` 也早在） |
| `FlightServer.ingest` | `Option<Arc<Ingestor>>` + `new_readonly`；`DoPut` ⇒ `failed_precondition`（**可读的拒绝**） |

两条路径（SQL 语句 / Flight DoPut）都给**可读的拒绝** —— 而不是让调用方以为写成功了。

### 74.4 用例：`crates/ingestor/tests/queryd_e2e.rs`

```text
  ① metanode（进程内，真 gRPC 服务）
       ▲ 注册/心跳                        ▲ 只读元数据（名录含数据面地址）
  ② yuntun-ingestor 子进程            ③ yuntun-queryd（真 Flight 服务）
       └────── ④ 数据面 gRPC：RemoteShard 拉热数据 ──────┘
                          ▲ ⑤ 客户端发 SQL
```

① 建表（走**客户端那条路**：`RemoteCatalog` → Propose → raft）→ ② 起数据节点子进程
（预置 WAL ⇒ 热数据在**另一个进程**里长出来）→ ③ 断言名录里的**地址 == 它打印的 LISTEN**
（数据面地址这一环）→ ④ 起查询节点（进程内，启动即按名录接线）→ ⑤ `SELECT` 读到 3 行
（**查询节点自己没有数据**）→ ⑥ `INSERT` 被拒（只读）。0.52s 跑完。

**为什么查询节点跑在进程内**：集成测试只能引用**自己 crate 的** `CARGO_BIN_EXE_*`，
而**数据节点必须是真子进程**（那才是"跨进程"的关键一半）。Flight 仍然是真的：真 TCP、
真协议、真 `DoGet`。为此把 `queryd` 拆成 lib + 薄 bin（`Queryd::start` 返回真实地址）。

### 74.5 验证、观察与遗留

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **335 passed / 0 failed（+1 ignored）**；
  clippy 本仓 **0**
- 规模：42,106 行 / 21 个 crate / 335 测试函数

**观察（不是本次改动引起，但值得记）**：`-j 8` 那一轮里 chaos 的
`disk_watermark_aborts_oldest_batch_then_releases_segments` 失败（耗时 54.76s），
而**单独跑 0.11s 通过**、`-j 4` 全量也通过 —— 与前一条 flake 同一类：**负载把时序余量吃掉**。
它的等待口径值得照 `§69.6` 的做法再收一遍（等到条件成立 + 宽松超时，而不是靠"机器够快"）。

**遗留**：

1. **`--meta` 的数据节点还没把 WAL 里的 DDL 重放进 metanode**（`§68` 的老账）——
   所以用例里的建表是走客户端路径做的。数据节点重启后的"表在不在"要靠它，属元数据面 DDL 可见性；
2. **`compactor` 未拆**（T12.1 的第三个进程）；
3. 跨进程写入面仍未定（客户端把数据交给哪个数据节点 / 怎么路由）—— R5 的 fanout 与它相邻。

---

## 75. 数据节点**自己带表进元数据面**：WAL 里的 DDL 重放（2026-09-23）

### 75.1 缺口是上一轮的用例逼出来的

`§74` 的三进程用例里，建表只能由**客户端**做 —— 因为数据节点（`--meta`）虽然把数据送进了
热读路径，却**没有把它的 WAL DDL 送进元数据面**。后果很实在：数据节点重启之后，
"表还在不在"取决于有没有人再建一次，而不是取决于它自己的 WAL。

### 75.2 搬家：`replay_wal_ddl` 从装配层移到写入路径

它原本是 `yuntun-server` 的**私有**函数（只有 standalone 用）。现在移到 `yuntun-ingest` 并导出：

**为什么是这个归属**：它**读**的是 WAL 的语义、**写**的是目录的语义 —— 两样都是写入路径的知识。
放在装配层，`standalone` 与数据节点进程就只能各自复制一份，而复制必然漂移。

两条纪律（原实现里就有，搬家时一并继承，值得写下来）：

1. **幂等**：重放会重复执行（每次启动都整扫一遍 WAL）⇒ "表已存在" / "表不存在" / "schema 已存在"
   必须当成**成功**而不是错误。少了这条，节点重启会因为"表已存在"而拒绝启动 —— 一个自愈路径
   变成启动阻塞；
2. **失败不致命**：单条 DDL 失败只告警（数据面的重放另有路径兜底），一个坏记录不该挡住整个启动。

装配顺序有个所有权细节：数据节点必须在 `Ingestor::new` **之前**调用它 —— 那一步会把 `wal`
的所有权拿走（注释里写明了，免得下次有人挪位置）。

### 75.3 用例现在证明了什么

`queryd_e2e` 里：

- **删掉**了客户端的 `create_table`；
- WAL 预置改为"**先一条 DDL，再三条数据**"；
- 数据节点启动 ⇒ `replay_wal_ddl` 把 CREATE TABLE **经 raft 写进 metanode**；
- 用例直接断言：`client_catalog.get_table("public.qd")` **读得到** —— 这就是"DDL 可见性"；
- 于是查询节点根本不需要谁替它建表，`SELECT` 就出 3 行。

一句话：**"数据节点自己带表进目录"** 从注释变成了断言。

### 75.4 验证与遗留

- `cargo test -p yuntun-ingestor --test queryd_e2e` → 1 passed（0.46s）
- 全量 `cargo test --workspace --no-fail-fast -j 4` → **335 passed / 0 failed（+1 ignored）**；
  clippy 本仓 **0**
- 规模：42,144 行 / 21 个 crate / 335 测试函数

**遗留**：

1. **每次启动都整扫 WAL 重放 DDL**（幂等但啰嗦）：可优化为"目录里缺哪个才重放哪个"，
   或者从"已消费 seq"起扫 —— 属性能项，不是正确性项；
2. **`compactor` 未拆**（T12.1 的第三个进程）；
3. **跨进程写入面未定**：客户端把数据交给哪个数据节点、怎么路由 —— 与 R5 的 fanout 相邻。

---

## 76. T12.1 第三刀：**压缩节点进程** `yuntun-compactord`（2026-09-23）

### 76.1 三个进程，三种"没有什么"

R4 的进程拆分到此成型（写入面另说）：

| 进程 | 有什么 | **没有什么** |
|---|---|---|
| `yuntun-ingestor`（数据节点） | WAL + chunk（写侧与热读侧同一实例） | 没有元数据权威 |
| `yuntun-queryd`（查询节点） | 元数据只读 + 按名录拉热数据 | **没有任何本地数据** |
| **`yuntun-compactord`（压缩节点）** | 共享对象存储 + 一次 op 提交 | **没有 WAL、没有 chunk、没有私有目录、不知道有数据节点** |

压缩只做两件事：**读**共享存储里的文件、**写**合并产物，然后把这次合并作为一条 op 提交到
元数据面（`commit_compaction` → raft）。于是"压缩"从一个**跟着某个节点跑的副作用**，
变成了一个**可独立扩缩的角色** —— 这正是"压缩与 Catalog 同进程只是部署事实"那句话的兑现。

### 76.2 一个必须写下来的部署不变量（因为没有代码强制它）

**一个集群只应有一个 compactor（或至少：同一个 shard 不被两个同时合并）。**

`commit_compaction` 本身是幂等的 op，但"**读文件 → 合并 → 提交**"这段窗口**没有租约保护**：
两个 compactor 同时合并同一个 shard，会各自产出一份合并文件、各自把老文件标删 ——
**行数不会错**（删除幂等，老文件只被标删一次），但会**多出一份孤儿产物**，
由孤儿清理在静置期后回收。所以这是"浪费"而不是"错数据"，但它仍然是个隐患。

留作后续两条路：给 shard 加压缩租约，或者把压缩交给**归属方**（谁持有分片谁压缩）。
本刀只把它**写清楚**，不假装解决了。

### 76.3 用例：`crates/compactord/tests/compaction_e2e.rs`（0.47s）

```text
  ① metanode（进程内，真 gRPC 服务）
       ▲ commit_files（3 个真文件）        ▲ commit_compaction（合并结果）
  测试进程（客户端）                    ③ yuntun-compactord 子进程
       └──── ② 共享冷目录（真 parquet 文件）────┘
```

- ② 用 `yuntun_format::write_batch` 写 **3 个真 parquet 文件**（各 3 行），逐个
  `commit_files`（走客户端那条路 ⇒ 经 raft 进状态机）；
- ③ 起压缩节点**子进程**（同一个共享目录 + 同一个元数据面，`--min-files 3 --interval-secs 1`）；
- ④ 观察点**全在目录上**：可见文件数 **3 → 1**，且合并产物的 `row_count == 9`
  —— 行数必须是输入之和，因为**合并最经典的 bug 就是丢行**。

除行数外没有断言压缩进程内部状态："合并有没有发生、有没有被提交"，只有从目录看得见才算数。

顺带一个接口约定：压缩节点**没有监听端口**，所以它的启动信号是 `READY ...` 而不是
`LISTEN <addr>`（与另两个进程同一性质：**是接口，不是日志**）。

### 76.4 验证与遗留

- `cargo test -p yuntun-compactord --test compaction_e2e` → 1 passed（0.47s）
- 全量 `cargo test --workspace --no-fail-fast -j 4` → **336 passed / 0 failed（+1 ignored）**；
  clippy 本仓 **0**
- 规模：42,546 行 / 22 个 crate / 336 测试函数

**遗留**：

1. **压缩租约 / 单一 compactor**（见 76.2）—— 部署不变量，代码尚未强制；
2. **跨进程写入面未定**：客户端把数据交给哪个数据节点、怎么路由 —— 与 R5 的 fanout 相邻；
3. `replay_wal_ddl` 每次启动整扫 WAL（幂等但啰嗦，`§75.4`）—— 性能项。

---

## 77. R5 / **T13.4 第一刀**：部分结果 —— 把"拿不到"与"还没拿到"分开（2026-09-23）

### 77.1 症状：一个节点挂了，整条查询失败

扇出（`crates/query/src/table.rs`）以前长这样：

```rust
let read = reader.read_table(&self.ident, self.snapshot.snapshot).await
    .map_err(|e| ...)?;          // ← 任何失败 = 整条查询失败
```

而设计（`architecture-with-chunk §4.2` 第三条）写的是：

> **partial response 默认允许**：节点失败时返回可用结果 + `partial: true` + 缺失来源列表。
> 监控场景下"90% 数据 + 明确标记"远好过整体报错（**可配置拒绝**）。

于是"一个监控节点挂了"变成"整个看板打不开"—— 与设计相反的默认。

### 77.2 关键认识：两个通道**本来就是分开的**（错在把它们合并了）

| | 信号从哪来 | 语义 | 该怎么办 |
|---|---|---|---|
| **STALE**（还没拿到） | `ShardRead.stale`（**读成功了**，携带水位） | 成员答上来了，答的是"我的水位超前于你的 manifest" | **刷新 + 重试**；追不上 ⇒ **响亮失败** |
| **不可达 / 读失败**（拿不到） | `Err` | 成员**没能答**（连接拒绝、超时、内部错误） | **降级** + **明确标记缺了谁** |

两者在类型层面一直是分开的，此前只是**被同一个 `?` 汇成了一条路**：把"**某个来源的事实**"
当成了"**整条查询的结论**"。本刀只改后者那一侧的处理方式，前者一个字不动。

### 77.3 最容易写错的地方：顺手把 STALE 也降级

**绝不可以。** STALE 是"**拿得到**，只是我们的目录落后了"—— 对它降级 = 把**可修复的落后**
当成**永久缺失** = 静默少数据，正是 `§63.3` / `§67` 反复抓到的那个错误形状（只是换了层）。

所以：`partial` 只挂在 `Err` 通道上；STALE 仍然走 `HotReadStale` → `with_stale_retry` →
三次追不上就**报错**（"不返回不完整结果"）。`partial_fanout` 的第 ④ 条用例专门守着这条边界，
断言"永久 STALE ⇒ 失败，**且错误里不得出现 partial 的措辞**"。

### 77.4 实现

| 件 | 内容 |
|---|---|
| `crates/query/src/partial.rs`（新） | `MissingSource{table,instance,reason}`、`PartialRead`（`is_partial` / `missing` / `describe`）、`PartialPolicy{Allow,Reject}`、`PartialSink`（每查询一个）、`PartialRejected`（**类型**，与 `HotReadStale` 同一考虑：字符串匹配认不出来） |
| `table.rs` | 扇出错 ⇒ `record` + `continue`（**降级**）；`Reject` 时 `record` 直接返回错误（**当场失败并点名**） |
| `lib.rs` | `QueryOutcome{batches, partial}` + `sql_partial` / `sql_with_partial`；`sql()`/`sql_stream()` 保持签名（warn 日志），**完整性这个事实另有出口** |
| `config.rs` / 装配 | `[query] partial = "allow"`（默认）/ `"reject"`；**配置写错启动即报错** |

两个刻意的细节：

1. **配置写错不许静默取默认**：那会让"我配了 `reject`"变成一句谎言，而它的后果正是
   "用户拿到一份自己以为完整、其实缺了来源的结果" —— 本项目里最不该静默的那件事。
2. **sink 每次尝试重建**（STALE 刷新重试时）：上一次尝试缺的来源，如果这次读到了就不该继续算缺，
   否则会把**完整结果误标为部分**（假警报同样是错 —— 它会让调用方不敢信任何结果）。
   用例 ③ 守着这条。

### 77.5 用例（`crates/query/tests/partial_fanout.rs` + `partial.rs` 单测）

| # | 场景 | 断言 |
|---|---|---|
| ① | 一个来源**连接拒绝** | 查询**成功**、健康来源的数据**完整**返回、`is_partial()` 为真、缺失列表**点名** `inst-b` 且带原因 |
| ② | 同上 + `Reject` | **失败**，错误里含 `inst-b` 与"拒绝部分结果" |
| ③ | 两个来源都健康 | **不许**标 partial（假警报也是错） |
| ④ | 一个来源**永远 STALE** | **失败**、错误含 `STALE` 与"不返回不完整结果"、**不含** partial 措辞 |

### 77.6 验证与遗留

- `cargo test -p yuntun-query --test partial_fanout` → 4 passed；`partial` 单测 4 passed
- 全量 `cargo test --workspace --no-fail-fast -j 4` → **344 passed / 0 failed（+1 ignored）**
- 规模：43,275 行 / 22 个 crate / 344 测试函数

**遗留**（按重要度）：

1. **"超时"这一半还没兑现**：今天只有**显式错误**会被降级（连接拒绝立刻失败 ✓），而**挂住**的
   节点会让查询一直等 ✗ —— 要真正实现 `§4.3` 的"失败/超时 ⇒ 退化为只读冷数据"，
   还得给数据面 RPC 加**客户端超时**（否则第 ① 条用例覆盖的只是"拒绝"，不是"无响应"）；
2. **wire 层还没把 `partial` 交给用户**：MySQL 的 warning / Flight SQL 的 metadata 都还没有这个
   信息 —— 现在只有日志与 `sql_partial()` 的返回值。设计要求"返回 `partial: true`"，
   这一步还差一层协议出口；
3. 设计的另两条（**下推 partial aggregate**、**冷数据文件按 datanode 分配**）属 R5 正题
   （T13.1 的另一半 / T13.2）。

---

## 78. R5 / **T13.4 第二刀**：数据面 RPC 的**客户端超时**（"无响应"也要走降级）（2026-09-23）

### 78.1 症状：节点没挂，但查询永远不结束

`GrpcShardFetch` 此前**完全没有超时**。于是：

| 对方的形态 | 传输层给什么 | 查询的结果 |
|---|---|---|
| 进程不在了（连接拒绝） | 立刻 `Err` | `§77` 降级为部分结果 ✓ |
| **进程还在但不回话** | **什么都不给** | **一直等** ✗ |

第二种才是更常见的生产形态：长 GC 停顿、被 CPU 抢光、网络黑洞、假死。
`§77` 的降级路径只对 `Err` 生效 ⇒ 对"没响应"它无能为力。

### 78.2 为什么必须由**传输层**来加这个上限

"失败"是**对方给的**（传输层能立刻发现）；"超时"**只有调用方能发现** —— 因为对方什么都没说。
所以"等够了"这件事只能由调用方自己判定，而调用方就是数据面客户端。
这与"幂等键要对齐、水位要与 manifest 同号"是同一类判断：**把语义放在唯一知道答案的那一层**。

### 78.3 实现

| 件 | 内容 |
|---|---|
| `GrpcShardFetch` | 带 `timeout` 字段；`connect_with_timeout(addr, d)`；`connect(addr)` 用 `DEFAULT_TIMEOUT = 5s` |
| 统一包装 `call()` | 三个 RPC（`fetch_shards` / `fetch_shard` / `fetch_watermark`）都过它 ⇒ 超时**可诊断**："超时（Ns 内无响应）" |
| 建连 | 也受同一超时约束（`connect_timeout`）—— 否则"端口是黑洞"会挂在**建连**那一步 |
| `queryd` | `--hot-read-timeout-secs`（默认 5）⇒ `reconcile_hot_readers(..., timeout)` |

三个刻意的点：

1. **超时错误必须可与"拒绝连接"区分**：`§77` 会把原因带进"缺失来源"交给排障的人 ——
   "进程没了"与"进程还在但卡住"的处置完全不同，不能都印成一个含糊的 deadline exceeded；
2. **默认要有上限，但也不能太小**：5s 是权衡（同机房热读远快于此；而查询端宁可降级也不该被拖住）；
3. **粒度是"每个 RPC"**：`RemoteShard::read_table` 的默认实现走三个 RPC ⇒ **单个来源**最坏
   约 `3 × timeout`。要收窄得覆写 `read_table` 合并成一次 RPC（trait 允许，见 `§67`）。

顺带修掉一个相邻问题：`reconcile_hot_readers` 里建连失败原本是 `?` ⇒ **一个坏地址让整轮巡检中断**，
别的成员再也接不上线 ✗。改为"记 warn + `continue`"：巡检是幂等的，下一轮补上，
期间该成员按"读不到"参与 partial 判定。

### 78.4 用例

| 文件 | 断言 |
|---|---|
| `shardrpc/tests/timeout_bounds_the_wait.rs` | 假死服务（handler `pending()`）⇒ 三个 RPC 都在**上限内**失败、错误含"超时"、**等满了超时**（不是立刻误判）；正常节点不被误判；默认超时有限且不荒谬 |
| `shardrpc/tests/hung_source_degrades.rs`（**组合**） | 真 chunk store（健康）+ 真 gRPC 假死节点 ⇒ 查询**成功**、健康来源数据**完整**、`missing` 点名 `inst-b` 且原因含"超时"、**查询耗时 < 3s**；`Reject` 策略对超时同样生效 |

组合那条是这条链的完整形态：**"无响应"（本刀）⇒ `Err` ⇒ 降级 + 标记（`§77`）**。
其中"查询耗时 < 3s"这一条不显眼但关键：若超时只让错误可诊断、却没让查询提前结束，那等于没修。

### 78.5 验证与遗留

- `cargo test -p yuntun-shardrpc` → 7 passed（5 条新）
- 全量 `cargo test --workspace --no-fail-fast -j 4` → **349 passed / 0 failed（+1 ignored）**
- 规模：43,740 行 / 22 个 crate / 349 测试函数

**遗留**（按重要度）：

1. **扇出是串行的**（`table.rs` 的 `for`）⇒ N 个假死来源的最坏耗时是 `N × timeout × RPC 数`。
   单个来源已经有界（本刀），但**整体**还缺一个"每查询的热读预算"（全局 deadline）或把扇出并行化
   —— 这是本刀之后最该做的一刀；
2. wire 层仍未把 `partial` 交给用户（MySQL warning / Flight SQL metadata，`§77.6` 第 2 条）；
3. 超时是**客户端感知**：它不会终止对端的计算（对端可能仍在跑）。对"幂等的热读"无害，
   但将来若有副作用型 RPC，这条就得重新审。

---

## 79. **架构更正**：角色只有两类（meta / data）—— 压缩归数据进程、删 `compactord`、更名 `yuntun-datanode`（2026-09-23）

### 79.1 我错在哪

`plan.md` T12.1 那行写着"拆 `yuntun-ingestor` / `yuntun-queryd` / `yuntun-compactor`"，
我照着做了三刀，还在 `§68`/`§74`/`§76` 里写"**进程拆分至此成型**" —— **错的**。

设计文档 `architecture-with-chunk §1.1` 写得比我清楚（我该先读它）：

| 角色 | 原文 |
|---|---|
| **metanode** | 有状态，raft 组，纯元数据服务，**永不中转数据** |
| **datanode** | 有状态，WAL + 内存 chunk + 本地缓存；直接读写对象存储；**可对外提供 SQL** |
| **compactor** | 全局作业，通过 meta 租约独占文件批次；**可作为 datanode 内后台任务或独立进程（后续决定）** |
| **standalone** | **1 个 datanode + 内嵌 metanode** |

配合 `§4.2`（"**接到 SQL 的 datanode 充当协调者**"）与 K4（"**需要时再加 queryd（本文不建**，
触发条件：查询负载明显挤占写入）"），结论很明确：**角色只有 meta / data 两类**；
`compactor` 是**作业**不是进程；`queryd` 是**同一角色的开关组合**。

三处具体错误：

1. 把 `plan.md` 一行的"二进制清单"当成了拓扑 —— 那行把**可选形态**与**角色**混在一起；
2. `compactord` 独立进程把"压缩与 Catalog 同进程只是部署事实"**反着做**成了一种新拓扑，
   于是 `§76.2` 那条"一个集群只能有一个 compactor"**不是设计事实，是这个拆法制造出来的**；
3. 更实际的后果：按设计的形态部署（metanode + datanode）时，**没有任何东西做压缩** ——
   这是**功能缺口**，不是措辞问题。

### 79.2 代码怎么改

| 动作 | 内容 |
|---|---|
| **压缩归位** | `yuntun-datanode --compaction`（**默认关**）：数据进程自己跑压缩 + 孤儿 GC —— 它已经握着共享冷存储与目录句柄，**不需要任何新输入** |
| **删 `compactord`** | crate 与其用例删除（压缩代码留在 `yuntun-compaction`，需要时随时可复用） |
| **更名** | `yuntun-ingestor` → **`yuntun-datanode`**：它从来不只做 ingest（读写数据 + 压缩） |
| **多节点约束** | `--compaction` 默认**关**，因为多数据节点时只应有一个打开（R6 的 meta 租约 T14.1/T14.2 会把这条**部署约束**变成机制）。这是**显式部署约束**，不是"架构不变量" |

**默认关是刻意的**：`commit_compaction` 本身幂等，但"读文件 → 合并 → 提交"这段窗口**没有租约**，
两个节点同时合并同一 shard 会各产出一份产物（**行数不会错**，多出来的那份由孤儿清理在静置期后回收
—— 是浪费，不是脏数据）。

用例：`crates/datanode/tests/compaction_e2e.rs` —— 3 个真 parquet 文件写进 `<dir>/cold`，
数据进程子进程（`--compaction`）合并后目录里**可见文件 3 → 1**、且合并产物**行数为输入之和**
（合并最经典的 bug 就是丢行）。

### 79.3 `queryd` 的去向（下一步）

`queryd` **不是第三类进程**，而是**数据进程的特例**：`--ingest off`（不吃 WAL、不留本地数据，
只作为协调者拉别人的热数据 —— `§4.2` 的形态）。它的代码（Flight SQL 服务面 + 按名录建
`GrpcShardFetch`）随下一刀并进 `yuntun-datanode`，然后删除 `yuntun-queryd` crate。

### 79.4 教训

**`plan.md` 的"拆哪些二进制"不能替代设计里的"有哪几类角色"。**

角色是**语义**（谁持有什么状态、谁对什么负责）；二进制是**部署**（同一角色可以有多种开关组合）。
把清单当拓扑，就会造出"只应有一个 compactor"这种**自己给自己挖的坑** ——
而真正的解法（meta 租约）本来就在 R6 的计划里。

**另一条**：这次是用户当场指出的。凡是我在文档里写下"**至此成型**/不变量"这类**收口式断言**，
都该先回到设计文档核对**角色与语义**，而不是顺着自己的实现往下推。

### 79.5 验证

- `cargo test -p yuntun-datanode` → 6 passed（含新增的 `compaction_e2e`）
- 全量 `cargo test --workspace --no-fail-fast -j 4` → **349 passed / 0 failed（+1 ignored）**
- 规模：43,687 行 / 21 个 crate / 349 测试函数

---

## 80. 架构更正（续）：`queryd` 并入数据进程 —— 形态 `--no-ingest`，删除 `yuntun-queryd`（2026-09-23）

### 80.1 兑现 `§79.3`

`§79` 定下的方向是：**`queryd` 不是第三类进程，而是"不吃 WAL 的数据进程"**。本刀把它落地：

| 动作 | 内容 |
|---|---|
| **并进来** | `crates/datanode/src/query.rs`：原 `queryd` 的"按名录装配热读器"（`reconcile_hot_readers` + 巡检循环）|
| **新开关** | `--no-ingest`（关掉 ingest ⇒ 只查询形态）、`--sql-listen <addr>`（给了才对外提供 SQL）|
| **删 crate** | `crates/queryd` 整个删除（20 个 crate）|
| **用例迁移** | `queryd_e2e` → `datanode_forms_e2e`：**两个真子进程**（`--dir D` 与 `--dir D --no-ingest --sql-listen`）|

### 80.2 三种形态（**启动即校验**，宁可起不来也不起成语义含糊的进程）

| 形态 | `--no-ingest` | `--sql-listen` | `--compaction` |
|---|---|---|---|
| 纯数据节点（写 + 热读服务） | — | — | 可选 |
| 数据节点 + 协调者（`§4.2` 默认形态） | — | ✓ | 可选 |
| **只查询**的数据进程（K4 的"需要时再加"） | ✓ | **必须** | **禁止** |

三条校验各自防一种"起得来但语义含糊"的进程：

1. 只查询形态**必须**给 `--sql-listen` —— 否则这个进程没有任何对外职责；
2. 只查询形态**必须**给 `--meta` —— 它没有本地数据，热数据全靠名录发现；
3. 只查询形态**禁止**承担压缩 —— 它可能被扩多份，而压缩作业每集群只应有一个（`§79.2`）。

### 80.3 两处语义细节（都写进代码注释）

1. **只查询形态不入册**：名录的语义是"**谁持有热数据**"，而它不持有任何本地数据 ——
   入册只会让协调者向它拉一份空数据（`§79` 的角色模型自洽性）；
2. **两个进程可以共用同一个 `--dir`**：只查询形态**不占任何租约**（没有私有状态），
   而 `<dir>/cold` 正是"同一份共享对象存储"在本机形态下的样子。用例就用了同一个目录 ——
   顺带把"数据节点在写、只查询进程在读同一份冷存储"这条日常形态跑通了。

### 80.4 本轮的 SQL 面**仍是只读**（明确写下来）

`FlightServer::new_readonly`：写入（INSERT/DDL）得到**可读的拒绝**，而不是假装成功。
把写面（SQL DML → 本进程的 ingestor）接上是**下一步**；而"客户端把数据交给哪个数据进程"
（跨进程写入面）本来就还没定（`§79.5` 遗留）。

### 80.5 验证与遗留

- `cargo test -p yuntun-datanode` → 6 passed（含 `datanode_forms_e2e`：ingest 形态写入 →
  只查询形态经数据面 gRPC 拉热数据 → SQL 写入被拒）
- 全量 `cargo test --workspace --no-fail-fast -j 4` → **349 passed / 0 failed（+1 ignored）**
- 规模：43,697 行 / 20 个 crate / 349 测试函数

**遗留**：

1. **跨进程写入面**：客户端把数据交给哪个数据进程、怎么路由（R4 最后一块大语义）；
2. **SQL 写面**：把 `--sql-listen` 背后的写路径接到本进程的 ingestor（今天只读）；
3. **按归属收窄拉取范围**（R5 / T13.1）与**扇出并行化 / 每查询热读预算**（`§78.5`）。

---

## 81. T14.1 / T14.2：**全局压缩租约** —— meta 只仲裁，数据进程执行（2026-09-23）

### 81.1 放在哪：这个问题有两半，答案不同

| 半 | 答案 | 理由 |
|---|---|---|
| **执行**（读文件、合并、写新文件、提交） | **数据进程** | `§1.1`：metanode「**永不中转数据**」；且压缩是**非确定性副作用**（新 batch_id、合并结果），放进 raft 状态机会让副本发散（`§38`/`§58` 是同类教训） |
| **仲裁**（"现在谁有权合并"） | **metanode** | 授权必须**线性一致** —— 两个节点各信各的内存视图就会**同时合并** |

`metanode-design` 早就写着「不做 compaction/GC 全局租约（R6）—— 但 R3 的 SM **预留**租约条目」，
顺带核对出那句话是**超前**的（代码里原本没有 ✗）。本刀把它补上。

### 81.2 租约**必须进 raft**，而心跳**绝不能**（两者是一条线的两端）

| | 性质 | 频率 | 进 raft？ |
|---|---|---|---|
| **存活心跳** | **发现**（"它还在吗"）—— 晚一点无所谓 | 秒级 | **绝不**（`§3.2`） |
| **租约** | **授权**（"谁有权合并"）—— 错一点就是重复 | 每 TTL/3 一次 | **必须** |

到期判定用**随 op 携带的 `now_ms`**（纪律 1：状态机不读钟）⇒ 同一串 op 在所有副本上得到同一状态。

### 81.3 语义（四条 + 一个当场踩到的洞）

| 动作 | 规则 |
|---|---|
| **取**（含接管） | 空闲（`holder` 空 = 已释放）或**已过期** ⇒ 授予，**代次 +1**；别人正持有且未过期 ⇒ 拒绝（**这就是"只启动一个"的全部机制**） |
| **取**（本人重复） | **代次不变**、期限顺延（它可能只是重启了，别让它把自己踢掉） |
| **续** | 持有者 + `epoch` + **未过期** 三者齐备；失败 = "**你已经不是持有者，立刻停手**" |
| **放**（优雅停机） | 置为空闲 + **保留代次水位** |

**那个洞（我自己的用例先撞出来的）**：释放若把条目**删掉**，下一次授予又得到 `epoch = 1`
—— 而重启前那份"**同名旧身**"手里正是 `epoch = 1`，它的续租会因"持有者与代次都匹配"被**接受**
⇒ 两个持有者同时干活。修法：释放 = 置空闲 + **代次单调**。

### 81.4 wire：`ProposeResponse.result` 第一次被真正用起来

三个 op（`AcquireLease` / `RenewLease` / `ReleaseLease`）的结果统一是 `LeaseGrant` 编码后的字节。
proto 早就留了 `bytes result = 5`（注释："逐 op 的结果形状待定"）—— 本刀补上第一块：
**"授没授予、新代次是多少"这种判定，客户端无法从 `accepted`/`affected` 重建**，
猜出来的东西在并发下一定是错的。空结果 ⇒ **报错**（不能当"没授予"糊过去）。

### 81.5 压缩循环：先拿租约再干活（`yuntun-compaction`）

三条纪律（`LeaseGate`）：

1. **拿不到就空转** —— 别人在干，重复合并只是浪费；
2. **续租被拒 ⇒ 立刻停手** —— 代次被别人推进了，这是时钟偏斜下**唯一**的防线；
3. **元数据面不可达 ⇒ 也不敢干** —— 拿不到权威回执时，宁可停一轮。

优雅退出时把租约**还回去**（接手方不必干等 TTL）。
于是 data 进程的 `--compaction` 从**部署约束**（"多节点只开一个"）变成了**意愿**
（"我愿承担，拿不到就让位"）—— `§79.2` 那条自己写下的约束，到此由机制接管。

### 81.6 可观测：租约进 `Status`

`StatusResponse.leases`（`LeaseView{purpose,holder,epoch,expires_at_ms}`）。
放进 Status 是刻意的：**"谁在干全局作业"必须看得见**，否则"只有一个 compactor"只能靠信任。

### 81.7 e2e（`crates/datanode/tests/compaction_e2e.rs`，2.22s）

两个 `--compaction` 数据进程（**私有目录各归各的、冷根同一份** —— 共享存储的本机形态，
为此数据进程新增 `--cold-root`）：

1. 两个都开 ⇒ **只有一个**拿到租约（读 metanode 的 `Status.leases` 确认）；
2. **杀掉持有者** ⇒ 过 TTL（用例里 2s）后**幸存者接管**，且 `epoch` 推进。

观察点刻意选在**租约表**而不是"文件有没有被合并"：后者在"一个干、一个空转"与
"两个都干但幂等"之间**区分不出来**。

### 81.8 验证与遗留

- `cargo test -p yuntun-datanode` → 8 passed（含仲裁/接管）；全量 → **354 passed / 0 failed（+1 ignored）**
- 规模：44,532 行 / 20 个 crate / 355 测试函数

**遗留**：

1. **分片粒度**：今天全局单持有者（一个集群一个 compactor）。设计原话是"独占**文件批次**"
   —— 扩成 `purpose = compaction:{table}:{shard}` 即可**并行**压缩不同分片，
   **协议不用动**（`purpose` 本来就是字符串）；
2. **栅栏**：`commit_compaction` 还不带 `epoch` ⇒ 极端时钟偏斜下，**在途的那个作业**仍可能重复产出
   （浪费，不是脏数据 —— `§79.2`）。彻底消灭要给提交加 epoch 栅栏；
3. **保活复用现有心跳**（今天续租是一次 raft op，每 TTL/3 一次，可接受）；
4. `T14.3` 孤儿 GC 的多写者安全仍待做（本刀只解决"谁合并"，不解决"谁删"）。

---

## 82. R6 第二刀：`commit_compaction` 的 **epoch 栅栏**（2026-09-24）

### 82.1 补的是哪一半

`§81` 解决了"**谁有权合并**"，但**已经在途的那次合并**仍可能在被罢黜之后落地 ⇒
与新持有者各产一份产物（浪费，且孤儿 GC 还得收）。栅栏把这一半也堵上。

### 82.2 机制

| 件 | 内容 |
|---|---|
| `CompactionOp.lease_epoch` | 提交时携带"我干活时持有的租约代次"（无租约传 0） |
| 状态机判据 | **代次落后于当前水位 ⇒ 拒绝**（返回错误 ⇒ op 失败 ⇒ 客户端**看得见**"你被接管了"，而不是静默丢弃） |
| 判据用**代次水位** | 不是"当前有没有持有者" —— 释放后水位仍留着，正是为此。`§81.3` 那条"释放 = 置空闲 + 保留代次"在这里兑现 |
| 无租约记录 ⇒ 不设栅栏 | 进程内直连形态与既有用例照常（向后兼容） |
| 用途键的唯一来源 | `yuntun_model::meta::COMPACTION_LEASE` —— 状态机与压缩侧各写一遍字符串会**静默失配**（栅栏白写） |

### 82.3 用例

- **状态机**（`compaction_fence_rejects_a_deposed_holder`）：无租约可提交 ✅ / 携带正确代次可提交 ✅ /
  携带 0 被拒 ✅ / **被接管后旧代次的在途提交被拒** ✅ / 新持有者照常 ✅；
- **压缩侧**（`a_deposed_holder_cannot_commit_its_in_flight_merge`）：真租约序列（1 → 过期 → 接管成 2）
  ⇒ 带 1 的提交被拒、带 2 的通过 —— 把"状态机的判据"与"压缩侧真的把代次带下去"接起来。

### 82.4 一个顺带修掉的**静默风险**（不是本刀的，是本刀暴露的）

清理时发现 `snapshot_covers_every_state_dimension` 上方多出一个 `#[test]`（`§81` 插入租约用例时
把它的文档注释与属性挤开了）。查清后确认**维度测试没有丢属性**（它自己那个 `#[test]` 还在），
只是属性重复告警 + 注释错位 —— 已归位。

值得记下来的是**发现它的方式**：一条 `warning: duplicated attribute`。这类信号很容易被"反正编译过了"
忽略过去，但它离"**某个用例静默不跑了**"只有一步 —— 而那正是本项目最忌讳的失效模式。

### 82.5 验证与遗留

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **356 passed / 0 failed（+1 ignored）**
- clippy 本仓 **0**；规模：44,646 行 / 20 个 crate / 356 测试函数

**遗留**：

1. **直连调用的代次是 0**：`compact_shard(.., 0)` 在"该集群**曾**授予过租约"时会被拒
   —— 这是**期望**的（只有持有者能提交），但意味着**绕过租约门的直连调用不再是合法路径**；
2. `T14.3` 孤儿 GC 的多写者安全（"谁删"）仍待做 —— 它要比"谁合并"更谨慎：
   删错文件是**真丢数据**，而重复合并只是浪费。

---

## 83. T14.3：**在途文件对孤儿 GC 显式可见** —— 把"零误删"从时间假设变成结构保证（2026-09-24）

### 83.1 症状：GC 的唯一防线是一个**时间假设**

| 件 | T14.3 之前 |
|---|---|
| 判据 | `known_batch_ids()` = `files.keys()` —— **只有已提交的文件** |
| 保护 | "**grace 期内不删**"（默认 1h） |
| 洞 | **上传慢于 grace 的写者**（大文件 / S3 抖动 / 长 GC）⇒ 它刚上传、还没 `commit_files` 的文件，静置期一过就被当孤儿删掉 ⇒ **真丢数据**（`R-9`） |
| 多写者 | 每个 GC 进程各有一份 `first_seen` ⇒ 时间假设还被稀释 |

删错文件是**真丢数据** —— 比"重复合并"（只是浪费）严重一个量级，所以这条要按**正确性**处理，
不能用参数（grace）去搪塞。

### 83.2 机制：**先声明、后上传**

1. 写者在上传对象存储**之前**登记 `batch_id`（`CatalogOps::record_in_flight` ⇒ op 走 raft）；
2. 提交时撤销（`commit_files` 内 `clear_in_flight`）—— 而文件已进 `files` ⇒ **保护不断档**；
3. `known_batch_ids()` = **已提交 ∪ 在途** ⇒ GC 的判据从"时间"变成"**结构可见**"；
4. 写者崩在半路 ⇒ 登记由 TTL 清扫（`sweep_expired_in_flight`）兜底 ⇒ 那份文件退回真孤儿 ⇒ 可回收。

**顺序是刻意的**：先声明、后写文件 —— 反过来就有一个窗口（GC 恰好在此期间扫到它）。
登记失败 ⇒ **不写文件**（宁可不落这个批次，也不落一个 GC 看不见的在途文件）。

### 83.3 为什么必须走目录（而不是各节点自己记）

孤儿 GC 在**别的**节点上跑 —— 它唯一能问的权威就是目录。
这与"租约进 raft、心跳不进"是同一条判断：**凡是"别人要据此做不可逆决定"的状态，必须线性一致地可见**
（删文件不可逆）。

### 83.4 代价（写下来）

写入路径**每个文件多一次元数据 op**（`record_in_flight` + `commit_files`）。
按"零误删"这个目标我认为值；将来可以做成**批量登记**（一次 op 声明多个 batch_id）。

### 83.5 用例

- **回归用例**（`gc_never_deletes_an_in_flight_file_even_with_zero_grace`）：
  `grace = ZERO`（**不靠时间假设**）⇒ 在途文件**不删**；外加**对照**：真孤儿**必须被回收**
  （否则用例可能只是"GC 根本没跑"）。这条在 T14.3 之前**必然失败**；
- **状态机**：登记即对 GC 可见 / 重复登记保留**首次**时刻（重试不该无限续命）/
  超时撤销保护 / 提交撤销登记而**保护不断档**。

### 83.6 验证与遗留

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **359 passed / 0 failed（+1 ignored）**
- clippy 本仓 **0**；规模：44,889 行 / 20 个 crate / 359 测试函数

**遗留**：

1. **在途 TTL 还没有驱动**：`sweep_expired_in_flight` 已就位（与幂等清扫同形），
   但"谁来定期调它"（metanode 的 leader 巡检 / 写者启动时自清）尚未接线 ——
   在此之前，**崩溃写者留下的登记会一直保护**那份孤儿文件（偏保守：是"留着垃圾"，不是"删错数据"）；
2. **删除本身仍不是原子的**：`delete` 是单次对象存储调用；"零误删"已由判据保证，
   但"删到一半失败"仍会出现（幂等重试可收敛）；
3. `T14.2` 遗留的"保活复用现有心跳"（省掉一次 raft 写）仍未做。

---

## 84. R6 收口：在途 TTL 接上驱动 +「多写者 + GC ⇒ 零误删」专项（2026-09-24）

### 84.1 补掉 `§83.6` 遗留①：谁来过期在途登记

| 件 | 做法 |
|---|---|
| 驱动 | 挂在与"摘除数据节点"**同一条 leader 巡检**上 —— 同一形状：**触发者是巡检（不进 raft），结果必须进** |
| 开销 | 只在**确实有过期条目**时才提议（`count_expired_in_flight` 是只读谓词）⇒ 常见情况下**零额外 raft 写** |
| TTL | **1h**（与旧 grace 同量级）：两个方向**不对称** —— 太长只是"留着垃圾"，太短可能把**正在写**的文件放开给 GC（**真丢数据**）⇒ 宁长勿短 |

### 84.2 R6 准出专项：多写者持续写 + GC ⇒ 零误删

两个写者并发（**先登记 → 写文件 → 提交**）+ 一个 GC（`grace = ZERO`、20ms 一轮），三条判据**一起**：

1. 每个**可见文件**的对象都**真的存在**（删错 = 目录指向空气）；
2. 可见文件行数之和 == 写入行数之和；
3. **预埋的真孤儿必须消失**（否则用例可能只是"GC 没跑"）。

**一个刻意的测试钩子**：写者在写文件之后、提交之前插 50ms `sleep`。理由不是"抖时序"，而是
**让在途窗口真实存在** —— 内存存储的 PUT 几乎是瞬时的，窗口只有微秒级 ⇒ GC（20ms 一轮）未必
扫得到 ⇒ 用例会**假通过**（只证明 GC 跑过，没证明它有过可乘之机）。50ms > GC 间隔 ⇒
**每个文件都必然在"文件已存在、目录还不知道"的状态下被扫到过**。

### 84.3 反证（这一步才是关键）

把 `known_batch_ids` 临时退回 T14.3 **之前**的判据（只认已提交文件）：

```text
  gc_never_deletes_an_in_flight_file_even_with_zero_grace ... FAILED
  multi_writer_plus_gc_never_deletes_a_committed_file     ... FAILED
```

还原后 **6 passed**。没有这次反证，上面的"通过"只说明用例不崩，**不说明它们守着机制**。

### 84.4 验证

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **360 passed / 0 failed（+1 ignored）**
- clippy 本仓 **0**；规模：45,107 行 / 20 个 crate / 360 测试函数

### 84.5 R6 的账（对照 `plan §7.5` 准出判据）

| 判据 | 状态 |
|---|---|
| T14.1 作业全局化：meta 租约独占 | ✅ `§81` |
| T14.2 租约 + 心跳 + 过期接管（杀掉执行者可被接管、**无重复合并**） | ✅ `§81`（接管 e2e：过 TTL 被接管、代次推进）+ `§82`（epoch 栅栏 ⇒ 被罢黜者的**在途提交作废**） |
| T14.3 孤儿 GC 的多写者安全 | ✅ `§83`（在途显式可见）+ `§84`（TTL 驱动 + 专项 + **反证**） |
| T14.4 跨节点文件合并（不同 `source_instance` 产出的文件） | ❌ **未做** |
| T14.5 与 `deleted_at` 联动：墓碑期 + 无在途引用才真删 | ❌ **未做** —— 但本刀把"**有没有在途引用**"变成了**可判定**的（`in_flight` 表），它现在可做 |

**保活仍未复用现有心跳**（续租仍是一次 raft op，每 TTL/3 一次）—— 这是"能省一次写"的优化，
不是正确性缺口。

---

## 85. T14.5：墓碑回收 —— 保护集合从"全部已知"收窄成"仍在保护期"（2026-09-24）

### 85.1 症状：墓碑永远不会被物理回收

链条很短：`commit_compaction` 把旧文件标 `deleted_at` 之后**仍留在 `files` 里** ⇒
`known_batch_ids()`（= `files.keys()` ∪ 在途）**永远含墓碑** ⇒ 孤儿 GC 的判据
`!known.contains(batch_id)` 对它**永远为假** ⇒ 被合并替换掉的旧对象**只增不减**，
集群越跑越大。而 `architecture §4.6` 末句要的正是相反的事：

> 产出新文件、标记旧文件 `deleted_at`，等**墓碑期 + 无在途引用**才真正删除。

### 85.2 这一刀

| 件 | 改动 |
|---|---|
| `FileManifest::protects_at(snapshot)` | **新判据**（与 `visible_at` 并排）：**活着**（`deleted_at == 0`）或**墓碑期未过**（`snapshot < deleted_at`） |
| `LakeState::known_batch_ids` | = `protects_at(当前快照)` 的文件 ∪ 在途 ⇒ **过期的墓碑退出保护集合**，物理回收才可能有对象可收 |
| `orphan_grace` | 语义变清楚：T14.3 之后写者安全**不再依赖它** ⇒ 它**就是墓碑期的时长**（设计推荐 **10–60s**；默认 3600 是保守取值） |
| **wire** | **零改动** —— 收窄发生在状态机**内部**（它自己知道当前快照），协议一个字没动 |

### 85.2.1 为什么 `protects_at` 不等同于 `visible_at`

只差一个 `valid_from`，但这个差别是**故意的**：合并产物的 `valid_from = snapshot + 1`
此刻**不可见**，但它是**已提交的真数据** —— **看不见 ≠ 可以删**。判据表用例单钉了这一格。

### 85.3 镜像反证（与 `§84` 互为镜像）

"删除正确性"有两个方向，缺一个都不算对：

| 刀 | 反证做法 | 期望与实测 |
|---|---|---|
| `§84` | 判据退回"只认已提交"（去掉在途） | 两条用例 **FAILED** ⇒ 守着"**不误删**" |
| `§85` | 判据退回"全部已知"（含墓碑） | 回收用例 **FAILED**，而 `§84` 两条**仍 ok** ⇒ 守着"**真会删**" |

```text
  expired_tombstones_are_reclaimed_by_gc                  ... FAILED
  gc_never_deletes_an_in_flight_file_even_with_zero_grace ... ok
  multi_writer_plus_gc_never_deletes_a_committed_file     ... ok
```

方向相反、互不干扰 —— 这正是"两个方向都钉住"的样子。

### 85.4 验证

- 用例：判据表（纯函数，3 格）+ `expired_tombstones_are_reclaimed_by_gc`
  （**真文件 → 真合并 → 旧对象消失 + 合并产物还在 + 行数不变**）
- 全量 `cargo test --workspace --no-fail-fast -j 4` → **362 passed / 0 failed（+1 ignored）**
- clippy 本仓 **0**；规模：45,311 行 / 20 个 crate / 362 测试函数

### 85.5 R6 的账（更新）

| 判据 | 状态 |
|---|---|
| T14.1 meta 租约独占 | ✅ `§81` |
| T14.2 租约 + 过期接管（**无重复合并**） | ✅ `§81`（接管 e2e）+ `§82`（epoch 栅栏） |
| T14.3 孤儿 GC 的多写者安全 | ✅ `§83` + `§84`（TTL 驱动 + 专项 + 反证） |
| **T14.5 与 `deleted_at` 联动** | ✅ **本刀**：墓碑期（= `orphan_grace`）过后即可物理回收；"无在途引用"由 T14.3 的**在途登记**挡住 |
| T14.4 跨节点文件合并（不同 `source_instance`） | ❌ **R6 只剩这一项** |

---

## 86. T14.4：跨实例文件合并 —— 合并产物**不属于任何实例**（2026-09-24）

### 86.1 这一项问的不是"能不能合并"，而是"**产物归谁**"

合并早就跨实例了（选文件只按 `(table, shard, snapshot)`，不看实例）。真正只在多节点才冒出来的
问题是：产物把**多个实例**的行融在一起，它的 `source_instance` 该填什么？

`§4.4` 定的是冷热边界**按实例**二维切分（"冷读该实例 ≤watermark 的文件，热 pull (watermark, now]"）：

| 填法 | 后果 |
|---|---|
| 继承某个输入实例 | 那一方的热数据范围被误当成"已覆盖这些行" ⇒ **重复计数**（不报错、只是结果变多 —— `plan §5.3-1` 那种最难查的错） |
| **留空（中立）** | 谁也不认领 ⇒ 它永远只从**冷数据**读 ✓ |

### 86.2 改动很小，但把"巧合"变成了"决定"

产物此前是 `..Default::default()` ⇒ `source_instance` **恰好**是空串。行为是对的，但这是
**默认值的巧合**：下一个人给 `Default` 加个非空默认、或复制这段代码时顺手带上
`source_instance`，就会静默引入重复计数。本刀把它写成**显式空串 + 一段为什么**。

### 86.3 用例：合并**别人**的文件之后，结果必须一字不差

真实形态（`§5.1`：多个 datanode 可能同时写同一 partition、各出各的文件）：同一 shard 上
`inst-a` 有**已落盘文件** + **未落盘热数据**，`inst-b` 同理 ⇒ 跨实例合并后钉三件事：

1. 行集**逐行相等**（多 = 重复计数，少 = 漏读）；
2. 产物 `source_instance` **为空**；
3. 老文件真被替换（否则"合并"根本没发生，用例是空转）。

挂在既有的 `two_instance_parity.rs` 上 —— 那里本来就有"两个真 chunk store 当两个 datanode"
的夹具，本条只是给它加上**冷数据（真 parquet 文件）**与**一次真合并**。

### 86.4 反证（两条，各自独立）

| 反证 | 改法 | 实测 |
|---|---|---|
| 产物被**认领** | `source_instance: files[0].source_instance` | 断言 ② **FAILED** |
| **忘了给输入转墓碑** | `old_ids = vec![]` | 断言 ③ **FAILED**（老文件与产物同时可见 ⇒ 行翻倍） |

还原 ⇒ 3 passed。

### 86.5 验证

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **363 passed / 0 failed（+1 ignored）**
- clippy 本仓 **0**；规模：45,490 行 / 20 个 crate / 363 测试函数

### 86.6 R6 的账（五项全部落地）

| 判据 | 状态 |
|---|---|
| T14.1 作业全局化（meta 租约独占） | ✅ `§81` |
| T14.2 租约 + 心跳 + 过期接管 | ✅ `§81` + `§82`（栅栏 ⇒ 无重复合并） |
| T14.3 孤儿 GC 的多写者安全 | ✅ `§83` + `§84`（TTL 驱动 + 专项 + 反证） |
| T14.4 跨节点文件合并 | ✅ **本刀** |
| T14.5 与 `deleted_at` 联动 | ✅ `§85`（墓碑回收 + 镜像反证） |

**R6 准出（`plan §7.5`）**："多节点持续写入下文件数收敛到稳定区间；compaction 期间查询不受影响"。
两个前提现已齐备：① 只有**一个** compactor 在合并（租约 + 栅栏）；② 合并产物既**不会被误删**
（§83/§84）也**不会永不回收**（§85）；③ 跨实例合并**不改变行集、不破坏按实例切分**（§86）。
其中"compaction 期间查询不受影响"由 §86.3 的"合并前后逐行相等"直接钉住。

**仍缺一条**：把"**多节点持续写入 + 压缩 + GC 三者同时跑 ⇒ 文件数收敛到稳定区间**"做成
一条**专项压测**（现有各条都是单点性质，没有"持续跑一段后看收敛"的那一条）。这是 R6 关闭前的最后一项。

---

## 87. R6 关闭：压缩产物也必须"先登记、后上传" + 收敛专项压测（2026-09-24）

### 87.1 顺手的发现：`R-9` 在压缩侧还有一个口子

`compact_shard` 写产物的顺序是 `write_batch` → `commit_compaction`，期间**没有任何在途登记**。
于是孤儿 GC（`orphan_grace` 取小的时候）一旦把扫描落进这段窗口，就会把**产物**当孤儿删掉；
而它一旦提交，**输入就转了墓碑** ⇒ **目录指向空气 = 真丢数据**。

这与 T14.3 修的口子**同形**，只是主体从"写者"换成了"压缩器"：产物同样是"先有对象、后有目录"
的文件。修法一字不差 —— **先登记、再上传**（提交成功时撤销；被栅栏拒绝或中途失败则留在集合里，
由在途 TTL 清扫兜底）。

### 87.2 一个我先做错、再修正的观察方式（值得单独记）

我原本打算用**压测**反证这个口子：`grace = 0` + 亚毫秒 GC ⇒ "必现"。**实测三次全绿** ——
因为窗口是"PUT 完成 → 提交"之间的**毫秒级**缝隙（真正决定它有多长的是提交那一次元数据往返），
压测撞不上 ⇒ 会**假通过**。把证据强度寄托在"撞概率"上，本身就错了。

改成**确定性观察**：让提交**必然失败**（用落后于水位的代次 ⇒ 被栅栏拒绝），此时产物的登记
**不会**被撤销，于是

- 产物对象**真的在存储里**（说明窗口确实形成过）；
- 它的 `batch_id` **仍在保护集合里**（⇒ GC 不会在窗口里删它）。

这条用例去掉登记就**必然失败**（反证实测两次皆红、0.00s）。
**一个只靠概率才会红的反证，等于没有反证** —— 这条比那个洞本身更值得记住。

### 87.3 收敛专项压测（`plan §7.5` 准出）

两个写者（两个 `source_instance`、**同一 shard** —— 多节点写同一 partition 的真实形态）持续写，
**真的**压缩循环（带租约）与**真的**孤儿 GC 同时跑。收尾后同时满足四条：

1. **行数一个不少**（可见文件 `row_count` 之和 == 写入总行数）；
2. **文件数收敛**到 `min_files + 1` 以内（写 32 次 → ≤ 4 个文件）；
3. 每个可见文件的**对象真的存在**；
4. **空间真回收**（对象数远小于写过的批次数）+ **保护集合 == 可见文件**（在途归零、无过期墓碑赖着）。

它同时钉住一件以前没钉的事：**压缩循环在租约下真的会持续干活**（而不只是"拿不到租约时不动"）。

### 87.4 验证

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **365 passed / 0 failed（+1 ignored）**
- clippy 本仓 **0**；规模：45,819 行 / 20 个 crate / 365 测试函数

### 87.5 R6 关闭

| 判据（`plan §7.5`） | 状态 |
|---|---|
| T14.1 作业全局化（meta 租约独占） | ✅ `§81` |
| T14.2 租约 + 心跳 + 过期接管 | ✅ `§81` + `§82`（栅栏 ⇒ 无重复合并） |
| T14.3 孤儿 GC 的多写者安全 | ✅ `§83` + `§84` |
| T14.4 跨节点文件合并 | ✅ `§86` |
| T14.5 与 `deleted_at` 联动 | ✅ `§85` |
| **准出：文件数收敛到稳定区间** | ✅ **本刀**（专项压测：32 次写入 → 收敛到 ≤4 个文件，行数一个不少） |
| **准出：compaction 期间查询不受影响** | ✅ `§86`（合并前后逐行相等 + 对象都在） |

**R6 关闭。** 压缩至此是一个**全局化的、可独立扩缩的角色**：只有一个持有者在干活（租约 + 栅栏）、
产物不会被误删也不会永不回收（在途登记 + 墓碑回收）、跨实例合并不改变行集（中立实例）。

---

## 88. R5 收尾：扇出**并发化** + 每查询**热读预算**（2026-09-24）

### 88.1 `§78` 的遗留：单个来源已有界，**整体**没有

`table.rs` 的扇出是串行 `for`：`N` 个慢来源的等待**相加**（`N × 单次传输超时`，而一个来源最坏
数个 RPC）。`§78` 只给"**一个 RPC** 等多久"上了界 —— 那是**连接的现实**；而"**这次查询**愿意为
热数据等多久"是**查询的策略**，得由调用方说了算。两个都缺，所以两个一起做：

| 件 | 作用 |
|---|---|
| **并发**（`join_all`） | 等待从 `Σ` 变成 `max(·)`，**与实例数无关** |
| **预算**（`hot_read_budget`） | `max(·)` 之上的**硬上界**（默认 10s：大于传输超时 5s，小于"一个来源把每个 RPC 都等满"） |

### 88.2 并发**不许**改语义

装配严格按 `self.hot` 的 `BTreeMap` 键序进行 —— 谁先返回只影响"什么时候"，不影响"拼成什么样"；
STALE 也取**顺序上的第一个**（与串行时代逐字一致）。"错误顺序可复现"本身就是一条用例（⑦）。

### 88.3 三条用例 + 反证

| # | 场景 | 断言 |
|---|---|---|
| ⑤ | 3 个来源各慢 200ms | 整段等待 **< 400ms**（取 max 而不是相加）+ 结果完整 |
| ⑥ | 来源慢过预算（2s vs 预算 150ms） | 预算内返回 + `is_partial` + 点名**"超出热读预算"** |
| ⑦ | 两个来源同时 STALE | 报错**确定地**点名键序第一个，不因并发而漂 |

**反证**（把扇出用一个全局锁串行化，其余逻辑一字不改）：⑤ **FAILED，实测 `764.9ms`**
（3 × 200ms 串行 + 锁切换开销）；还原后同一条 **ok（~200ms）**。
这条反证有力之处在于它把"并行"这个性质**量**了出来 —— 而不是只证明"代码没写错"。

### 88.4 配置

`[query] hot_read_budget_secs`（默认 10）/ `yuntun-datanode --hot-read-budget-secs`；
**0 视为非法，启动即报错** —— 它等于"所有热读都超时"（partial 恒真），静默接受它会让用户
以为自己配的是"完整"。

### 88.5 验证

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **368 passed / 0 failed（+1 ignored）**
- clippy 本仓 **0**；规模：46,117 行 / 20 个 crate / 368 测试函数

### 88.6 遗留

- **wire 层仍未把 `partial` 交给用户**（MySQL warning / Flight SQL metadata）—— 现在只有日志与
  `sql_partial()` 的返回值；
- **预算只覆盖热读**：冷 parquet 读（对象存储）不在其中，慢对象存储仍会拖住查询。

---

## 89. 把"结果不完整"交给用户：Flight SQL metadata（MySQL 被 opensrv 卡住）（2026-09-24）

### 89.1 事实早就有了，只是一路被丢掉

`§77` 起引擎就算得出"这次结果缺了哪些来源"。但从引擎到协议层这条路上，它被丢了三次：

| 层 | 当时的状态 |
|---|---|
| `QueryEngine::sql_stream_with_schema` | 读 sink 的**时机**不对（流还没跑）⇒ 那句 `if partial.is_partial()` 的告警**恒不成立**（**一句恒假的日志比没有日志更糟**：它让人以为"流式路径从没缺过来源"） |
| `SqlResult` / `SqlStreamResult` | **没有 partial 字段** ⇒ 结论到 SQL 层就消失 |
| `flight.rs` / `sqlwire` | 只调 `sql_with_schema` ⇒ 协议层根本拿不到 |

### 89.2 这一刀

- `QueryEngine::sql_stream_with_partial` 交出 sink；
- `SqlResult::Rows` 带 `PartialRead`、`SqlStreamResult::Rows` 带 `PartialWatch`
  （**两种载体**，因为两条路拿到结论的**时机**不同：eager 路径"发第一行之前就知道"，
  流式路径"流读完才知道"；用一个类型硬凑会让调用方在错误的时刻读到"完整"）；
- **Flight**：结论挂到 **schema 消息**的 `app_metadata`
  （`{"partial":true,"missing":[{"instance","table","reason"}],"detail":"…"}`；**完整时为空** —— 不许假警报）。

### 89.3 一个我先写错、被反证纠正的解释（值得记）

我原以为"读 sink 必须在流被第一次轮询之后"，于是加了一步 `peek` 并写了一整段注释解释这个时机。
**反证把 `peek` 删掉 ⇒ 用例照样通过** ⇒ 我的解释是错的：`TableProvider::scan`（fanout 就在里面）
是**规划期**调用的 ⇒ `execute_stream` 返回时结论**已经是事实**。
于是删掉多余的一步、把注释改成真的。

> **教训**：反证不只是"证明用例有效"，它也会**证伪你的解释**。
> 先按机制写断言，再让反证告诉你机制是不是你以为的那样。

### 89.4 MySQL 那一半：卡在 `opensrv`（如实记录，不假装交付）

MySQL 的 warning 计数在**结果集结束包（EOF）**里，而 `opensrv-mysql-0.7.0` 的
`ResultSetWriter::finish()` 把它**写死为 0**：
`writers.rs`：`w.write_all(&[0x00, 0x00])?; // no warnings`。
只有 OK 包（`OkResponse { warnings, .. }`）能带 —— 那是"无结果集"的语句才走的路径。

三条出路（**都还没做**）：① 换/升级 `opensrv`；② 自己写结束包；③ 走 `SHOW WARNINGS` +
客户端主动查（但客户端拿不到计数，就不会主动查）。
**在那之前不假装已交付** —— 结论留在 `sqlwire` 的注释里，谁要做谁看得见。

### 89.5 用例（真 gRPC + 真 Flight 客户端）

| # | 场景 | 断言 |
|---|---|---|
| ① | 一个来源**读不到** | 结果照常返回（健康来源 3 行都在），**schema 消息** `app_metadata` 含 `"partial":true` + 点名 `inst-b` + 表名 + `connection refused` |
| ② | 所有来源都读得到 | `app_metadata` **为空**（假警报同样是错：它会让调用方不敢信任何结果） |

### 89.6 验证

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **370 passed / 0 failed（+1 ignored）**
- clippy 本仓 **0**；规模：46,548 行 / 20 个 crate / 370 测试函数

### 89.7 遗留

- **MySQL 结果集的 warning**（89.4，被 `opensrv` 卡住）；
- `GetFlightInfo` 阶段还声明不了"完不完整"（那时确实还不知道 —— schema 消息已经是最早的时机）。

---

## 90. 里程碑门槛审计：把"能对外说什么"逐条对到证据上（2026-09-24）

`plan §8.4` 的判据是硬的："只有同时满足 **M1–M4 全部达成 + M5 对拍通过 + M6 零误删**"
才能对外说"分布式就绪"。所以这一节不做新功能，只做一件事：**把每一格对到具体用例上**，
点出**没有实证**的地方 —— 门槛靠"感觉已经做了"是不算的。

| 里程碑 | 门槛（`§8.3`） | 实证 | 判定 |
|---|---|---|---|
| **M2** R2 | 200 passed + 预取/delta/版本失效 + compaction **不绑具体类型** | 已记录（2026-09-17，含 `§5.1-B` 的"不补则编译不过"实测） | ✅ |
| **M3** R3 | ① 3 节点 raft 写入不中断；② metanode 重启后 Catalog 一致；③ **standalone 仍可单机运行** | ① `multi_node_grpc_e2e`（**真 gRPC**，3 节点复制 + **换主不丢已提交**）<br>② `metanode_process_e2e`（真进程 + `kill -9` + **从盘恢复**）<br>③ ⚠️ **缺**：`standalone/tests/cli.rs` 只测 `--version/--help`，且自己写明"真启动是端到端的事" —— 而**那条端到端不在** | ⚠️ ① ② ✅ / ③ 缺 |
| **M4** R4 | 多 datanode **并发写** + 查询，与单节点串行**逐行精确相等**；节点重启不丢不重 | 跨进程：`cross_process_hot_read`（一写一查）+ `datanode_forms_e2e`（两种形态，真子进程）<br>进程内：`two_instance_parity`（双实例对拍，加断言"只注册一个就少行"作反证）<br>重启不丢不重：chaos #6 崩溃恢复 + `recovery_guard_e2e` + `m0a_recommit`<br>⚠️ **缺**：门槛写的是"**多** datanode 并发写" —— 现有跨进程用例都是**单写者** | ⚠️ 缺一条 |
| **M5** R5 | 对拍通过（硬要求）+ 查询中节点故障行为**符合声明** | 对拍：`two_instance_parity`（逐行相等）<br>故障：`partial_fanout`（4 条边界 + `§88` 3 条）+ `hung_source_degrades`（真 gRPC 假死）+ `timeout_bounds_the_wait` + `flight_partial_metadata`（**用户看得见**）<br>声明与行为一致：STALE **绝不**降级、超时/拒绝都降级、完整**不**误标 | ✅ |
| **M6** R6 | 文件数收敛 + **开 GC 的多节点压测零误删** + 租约可接管 | 收敛：`§87` 专项（多写 + 压缩 + GC 同跑：32 次写入 → ≤4 文件，行数一个不少）<br>零误删：`§84` 专项（多写者 + GC，含**反证**：退回旧判据 ⇒ 双双变红）<br>接管：`§81` e2e（过 TTL 被接管、epoch 推进）+ `§82` 栅栏（在途提交作废） | ✅ |

### 90.1 两处缺口（都极小，且已定位到改造点）

**① M4 的"多 datanode 并发写"**：现有跨进程用例都是**单写者**。
改造点已定位：`datanode_forms_e2e.rs` 的 `seed_wal(dir, rows)`（往某实例的 WAL 预置数据 ⇒
它启动后回放 ⇒ 热数据在**另一个进程**里长出来）+ `Proc`（子进程夹具）+ Flight 客户端。
⇒ **两个 datanode 各预置一半行、都指向同一个 metanode、再查一次**，
断言 `结果 == 单节点串行`（逐行相等、**每行只出一次**）—— 这正是 M4 的字面要求，
也是"多个 datanode 同时写同一 partition、各出各的文件"（`architecture §5.1`）的真实形态。

**② M3 的"standalone 仍可单机运行"**：`standalone/tests/cli.rs` 自己写了"真启动会绑端口…
那是端到端测试的事"，而那条端到端**不在**。两条路：补一条"真起 `yuntun` 二进制 + 查一次"的
冒烟用例；或用一句**能站住的话**替代 —— 即证明 standalone 与其它形态**共用同一个装配**
（`Lakehouse`），从而 `flight_e2e` / `assembly_parity` 的覆盖可以**转移**过来。
**在没有其中之一之前，这一格不算达成。**

### 90.2 结论

`§8.4` 的六格：**M2 ✅、M3 ⚠️（③ 缺）、M4 ⚠️（并发写缺）、M5 ✅、M6 ✅**。
⇒ 现在**还不能**对外说"分布式就绪"，缺的正是上面两条**证据**（不是功能 —— 功能都已落地）。
把它们补上之后，`§8.4` 才算真的满足。

### 90.3 验证（本轮无代码改动，跑一遍全量确认基线）

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **370 passed / 0 failed（+1 ignored）**
- clippy 本仓 **0**；规模：46,548 行 / 20 个 crate / 370 测试函数

---

## 91. M4 的门槛证据：**多** datanode 并发写 ⇒ 与单节点串行逐行相等（2026-09-24）

### 91.1 缺的是什么

`§90` 把 M4 标成 ⚠️：门槛（`plan §8.3`）写的是"**多** datanode 并发写 + 查询，与单节点串行
**逐行精确相等**"，而现有跨进程用例都是**单写者**（`cross_process_hot_read` 是一写一查，
`datanode_forms_e2e` 是 ingest 形态 + 只查询形态）。功能都在，**缺的是这一格自己的证据**。

### 91.2 拦路问题（也是这一格的全部难点）

两个写进程**不能共用 `--dir`** —— WAL / spill 是私有的，同一目录第二个消费者会被**目录租约**
当场拒绝（`T12.4`，这是对的）；可它们的**冷存储必须是同一份**，否则协调者只看得见自己那份。
答案在 `--cold-root`，它的文档原话就是：

> "各给一个 `--dir`，但指同一个 `--cold-root`" —— 真实部署里它是 S3。

### 91.3 用例：三个真子进程 + 进程内 metanode

```text
  metanode（进程内，真 gRPC）
    ▲ 注册/心跳（两个**不同**实例名）        ▲ 名录（含各自数据面地址）
  datanode A ──┐                          ┌── datanode B
  --dir A      ├── 同一个 --cold-root ────┤   --dir B
  WAL: 1,2,3   ┘   （= 共享对象存储）      └── WAL: 4,5,6
                    ▲
          只查询进程（--no-ingest --sql-listen）→ SELECT ⇒ [1,2,3,4,5,6]
```

断言是"**逐行精确相等**"：**少行 = 漏读某个实例；多行 = 同一份数据被读两次**。
两个方向的错法分别由 `two_instance_parity` 的反证钉住（只注册一个实例 ⇒ 只看得见它的行）。

### 91.4 这一格覆盖到哪、没覆盖到哪（如实说）

- ✅ **覆盖**：两个写者同时在位、各自出各自的数据 ⇒ 查询**不重不漏**（M4 的字面要求）。
- ⚠️ **未覆盖**：两个节点**同时**对同一 shard 做**在途**写入的竞态 —— 本例是两个进程各自
  回放自己的 WAL（真实的"各出各的文件"形态 ✓，但不是"同一瞬间都在写"）。

### 91.5 遗留

- **并发在途写**：让两个 ingest 节点**都开 `--sql-listen`**（"数据节点 + 协调者"本就是同一进程
  的两个面），并发 INSERT ⇒ 再查一次对拍。这是 91.4 那条缺口的补法；
- M3 ③ **standalone 真启动**（`§90.1`）。

### 91.6 验证

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **371 passed / 0 failed（+1 ignored）**
- clippy 本仓 **0**；规模：46,698 行 / 20 个 crate / 371 测试函数

---

## 92. M3 ③ 的处置：standalone 与其它形态**共用同一个装配**（2026-09-24）

### 92.1 门槛

M3 的第三格是"**standalone 仍可单机运行**"。而 `standalone/tests/cli.rs` 只测
`--version` / `--help`，且自己写明"真启动会绑端口、建目录、起后台任务 —— 那是端到端测试的事"，
**而那条端到端不在** ⇒ 这一格此前是空的（`§90` 记为 ⚠️）。

### 92.2 能站住的那句话（先给证据，再说还缺什么）

`crates/standalone/src/main.rs` 只有 **96 行**，它的全部工作就是：

```text
Config(TOML) → Lakehouse 装配 → yuntun_server::serve_flight(&lakehouse, &listen)
```

⇒ **standalone 没有任何自己的装配逻辑**：它与 `server/tests/flight_e2e.rs`、
`server/tests/assembly_parity.rs` 跑的是**同一个 `Lakehouse`、同一个 `serve_flight`**。
因此那两条用例的覆盖**合法地转移**到 standalone 上：它们证明的装配，**就是** standalone 的装配。

也就是说："standalone 能单机运行"在**装配层**已经有证据了。

### 92.3 还缺的那一半（二进制层冒烟），以及为什么它不该"顺手补"

缺的是"**真起 `yuntun` 二进制 + 连上去查一次**"。

做它之前要先解决一个**真实障碍**：standalone 的监听地址来自 **TOML 配置**
（不像数据进程会打印一行 `LISTEN <addr>`），于是测试只有两条路：

1. **选一个固定端口** ⇒ 并发/重复跑会撞端口，且"空闲"是猜的（`§37` 的 TOCTOU 批评）；
2. `bind(127.0.0.1:0)` 拿到端口 → 释放 → 把端口交给子进程 ⇒ 同样有竞态窗口（`§37` 明确批评过这个写法）。

⇒ 干净的做法是**先给 `yuntun` 加一行接口行**（`LISTEN <addr>`，与数据进程/metanode 同形 ——
"**是接口，不是日志**"，`§38`/`§73` 一路的纪律），然后照 `datanode_forms_e2e` 的夹具写冒烟。
这是**下一步**要做的一件事，不是"顺手补一下"。

### 92.4 结论：`§8.4` 的账（更新）

| 里程碑 | 状态 |
|---|---|
| M2 / M4 / M5 / M6 | ✅ |
| M3 ①②（3 节点真 gRPC raft / `kill -9` 恢复） | ✅ |
| **M3 ③（standalone 可单机运行）** | 🟡 **装配层已有证据**（92.2）；**二进制层冒烟未做**（92.3 给了无竞态的补法） |

⇒ 严格按 `§8.4` 的判据，还差 M3 ③ 这一格；但它**不再是一句空话** ——
装配证据已在，剩下的部分有明确且**无竞态**的补法（先加 `LISTEN <addr>` 接口行）。

---

## 93. M3 ③ 关闭：standalone 的**接口行** + 二进制冒烟（`§8.4` 的门槛至此齐了）（2026-09-24）

### 93.1 先补接口，再写冒烟

`§92` 说了顺序与理由：standalone 的监听地址来自 TOML，若测试"猜一个空闲端口再交给子进程"，
就落进 `§37` 批评过的 TOCTOU。所以先把 `serve_flight` 从
`serve_with_shutdown(addr, ..)`（**它自己 bind、只在日志里回显配置值**）改成
**自己 `TcpListener::bind` + 打一行 `LISTEN <addr>`**：

- 这行是**接口，不是日志** —— 与数据进程 `LISTEN`、metanode 同形，编排/测试靠它拿地址；
- 它必须是**真实绑定地址**：配置里写 `:0` 时那个 "0" 对调用方毫无用处。

### 93.2 冒烟用例（`crates/standalone/tests/standalone_e2e.rs`）

```text
  写一份只含 [server] listen = "127.0.0.1:0" 的配置（其余走默认）
  → 真起 yuntun 子进程（CWD 指向临时目录，别往仓库里写 ./data）
  → **从接口行读回真实地址**（内核分配，全程没有"探测—释放—再交给别人"的窗口）
  → 真连 Flight → 真答一条 SQL（SHOW DATABASES：不依赖任何表，走方言 shim）
```

断言刻意保守但有分量：**进程真起来 + 接口行真打 + Flight 真连上 + SQL 真答回来**。
（再往下测 SQL 语义是别的用例的事；这条要证的是"这个二进制起得来、能对外服务"。）

### 93.3 `§8.4` 的账：六格齐了

| 里程碑 | 证据 |
|---|---|
| M2 R2 | 已记录（含 `§5.1-B` 实测） |
| **M3 R3** | ① `multi_node_grpc_e2e`（3 节点真 gRPC、换主不丢已提交）② `metanode_process_e2e`（`kill -9` 恢复）③ **本刀**（二进制真起 + 真答一条 SQL）+ `§92.2`（装配层：standalone = 同一 `Lakehouse`/`serve_flight`） |
| M4 R4 | `§91`（两个真写进程 + 共享 `--cold-root` ⇒ 逐行精确相等）+ 跨进程/进程内对拍 + 崩溃恢复 |
| M5 R5 | 对拍 + `§77`/`§78`/`§88`/`§89` 的故障语义（含"用户看得见"） |
| M6 R6 | `§87` 收敛 + `§84` 零误删（含反证）+ `§81` 接管 + `§82` 栅栏 |

⇒ **`plan §8.4` 的判据（M1–M4 达成 + M5 对拍通过 + M6 零误删）至此齐了** ——
可以对外说"分布式就绪"（M1 见 `§8.3` 与 chaos 的 11/11）。

**仍未做、但已明确记账的**（不影响上述判据，属"更强"而非"缺失"）：
① 两个节点**同时**对同一 shard 做**在途**写入的竞态（`§91.5`）；
② MySQL 结果集的 warning（被 `opensrv` 卡住，`§89.4`）；
③ 冷 parquet 读未纳入预算（`§88.6`）。

### 93.4 验证

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **372 passed / 0 failed（+1 ignored）**
- clippy 本仓 **0**；规模：46,875 行 / 20 个 crate / 372 测试函数

---

## 94. `§91.5` 的第一半：**跨节点重试的幂等**（2026-09-24）

### 94.1 为什么先做这一半

`§91.5` 记的"两个节点同时在途写"里，有一个**更容易被忽略、后果更重**的分支：

> 客户端超时了，把**同一批**数据重发到了**另一个** datanode。

这不是"各出各的文件"（那是设计要的正常形态，由查询侧合并），而是**同一批数据的第二次落地**：
一旦幂等键按实例隔离，就会**悄悄写两份** —— 不报错，只是结果变多。

### 94.2 结论：幂等键在**目录**这一层就是全局的

`LakeState.idempotency: BTreeMap<String, IdempotencyRecord>` —— **扁平、没有实例维度**。
所以跨节点重试天然去重。这条性质**此前没有用例钉它**，本刀补上，并且**两个方向都钉**：

| 用例 | 断言 |
|---|---|
| `same_client_key_from_another_instance_writes_only_once` | 同一个 `client_request_id` 从**另一个实例**重试（不同 `batch_id`、不同 `source_instance`）⇒ 只有**一个赢家**（`accepted=false`），目录里只有赢家那一份，且它的 `source_instance` 是**第一次**那个 |
| `different_keys_from_two_instances_both_land` | **不同** key、不同实例 ⇒ 两份**都**合法存在 |

第二条件是第一条件的**护栏**：把"跨实例去重"写成"跨实例一律拒绝"同样是错的 ——
那会堵死 `architecture §5.1` 的"多个 datanode 各出各的文件"这条正常路径。

### 94.3 仍未做（`§91.5` 的另一半）

两个 ingest 节点**都开 `--sql-listen`**（"数据节点 + 协调者"本就是同一进程的两个面），
**并发 INSERT、同一瞬间在途写** ⇒ 再对拍。
配方已定：两节点**同一 `--cold-root`**、各自 `--dir`/`--instance-id`、都开 `--sql-listen`；
夹具沿用 `§91.3`（`spawn_writer` / `spawn_query_only_with_cold` / `wait_until` / `sql`）。

### 94.4 验证

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **374 passed / 0 failed（+1 ignored）**
- clippy 本仓 **0**；规模：46,994 行 / 20 个 crate / 374 测试函数

---

## 95. 更正：数据进程**没有写入面** —— `§8.4` 的"就绪"要收回一格（2026-09-24）

### 95.1 起因：给"并发在途写"写用例之前先核前提

`§91.5` 的第二半打算让"两个 ingest 节点都开 `--sql-listen` 并发 INSERT"。核前提时发现
`crates/datanode/src/main.rs` 的 SQL 面是：

```rust
yuntun_server::FlightServer::new_readonly(engine, catalog.clone())
```

⇒ **只读**（`new_readonly` 连 `ingest` 都不带 ⇒ `do_put` 会被"本节点只读"拒绝）。
也就是说：**数据进程今天没有任何"接受写入"的网络面** —— 它的 WAL 只能由
① 自己启动时的回放、或 ② 外部直接写文件（测试里就是这么做的）产生。

### 95.2 把话说准：`§91` 证的是哪一半

- `§91` 的 M4 证据是"两个写进程各自**回放自己的 WAL**" ⇒ 它证明的是**元数据/合并**层面的
  "多写者不重不漏" ✓；
- 它**没有**证明"客户端能把数据写到某个数据进程" ✗ —— 而且**在写入面补上之前不可能证明**；
- 设计 §5 写的是"任意 datanode 收到写入（**无路由**）" ⇒ 那个"收到"的**面**还没做。

⇒ 这正是前几轮列在候选里的"**跨进程写入面（R4 的最后一块大语义）**"。
它不是"更强"，而是 **M4 字面要求里缺的一环**。

### 95.3 对 `§8.4` 的更正（收回 `§93.3` 的一句话）

`§93.3` 写了"六格齐了 ⇒ 可以对外说分布式就绪"。按这个发现，**那句话要收回一格**：

| 里程碑 | 更正后 |
|---|---|
| M2 / M3 / M5 / M6 | ✅ |
| **M4** | 🟡 **"多 datanode 并发写"只有"回放式"证据**；"客户端真写进数据进程"**没有面** |

⇒ 结论更正为：**M2 ✅ / M3 ✅ / M4 🟡（写入面未做）/ M5 ✅ / M6 ✅**。

### 95.4 下一步：先把写入面的**形态**定下来（这要一段设计对话，不是顺手补）

1. **DoPut（Flight）**：数据进程的 SQL 面从 `new_readonly` 换成**可写**（带 `ingest`）
   ⇒ 复用 `standalone` / `serve_flight` 那条已经在跑的路径（**最省**，且与"无路由"一致）；
2. **数据面 RPC**：`shard.proto` 加 `Append` —— 与热读同一个通道，但要先定"谁负责 WAL / fsync"；
3. **客户端"给谁"**：设计说"无路由" ⇒ 客户端**任选一个**数据进程即可 ⇒
   "选哪个"**不该进数据模型**；但要让"任选"这件事有据可依（名录 + 存活），而不是碰运气。

形态定了再写用例（那时 `§91.5` 的第二半才有意义）。

---

## 96. 补上"接受写入的面"：ingest 形态下数据进程的 SQL 面**可写**（2026-09-24）

### 96.1 改了什么

`crates/datanode/src/main.rs` 的 SQL 面此前**恒为** `FlightServer::new_readonly(engine, catalog)`
⇒ 数据进程**没有接受写入的网络面**（`§95`）。现在按**形态**决定能力，而不是一刀切：

| 形态 | SQL 面 | 为什么 |
|---|---|---|
| **ingest**（本进程握着 WAL + chunk store） | **`FlightServer::new(ingestor, engine, catalog)`** | 与 `standalone` / `serve_flight` **同一条已验路径** |
| `--no-ingest`（只查询） | 保持 `new_readonly` | 没有本地数据、没有 WAL ⇒ **写不了就不假装能写** |

### 96.2 这让哪句话从设计变成行为

`architecture §5`："**任意 datanode 收到写入（无路由）**" ⇒ 现在任一 ingest 形态的数据进程
都能通过自己的 SQL 面（Flight `DoPut`）接受写入。`§91.3` 的夹具（`spawn_writer` 已经有
`--sql-listen` 的位置）因此可以直接升级成"**真在途写**"。

### 96.3 还没做的（如实）

- **没有对拍用例**：`§91.5` 的第二半（两个节点并发**在途**写 ⇒ 逐行相等）还没写。
  形态已具备，但夹具要新增（DoPut 写入流 + 并发驱动）；
- **"客户端给谁"仍无据**：设计说无路由 ⇒ 任选；但"任选"的依据（名录 + 存活）还没接进客户端；
- 只读形态仍**拒绝**写入（既有用例继续钉着这一点：`datanode_forms_e2e` 第 ⑥ 步）。

### 96.4 验证

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **374 passed / 0 failed**
- clippy 本仓 **0**；规模：47,010 行 / 20 个 crate / 374 测试函数

---

## 98. M4 关闭：两个写进程**并发真写** ⇒ 逐行精确相等（`§91.5` 的第二半落地）（2026-09-24）

### 98.1 缺的是什么（`§95` / `§96` 留下的那一格）

- `§91` 证的是"两个写者各自**回放自己的 WAL**"（数据在启动前就摆好）⇒ 它证明的是**元数据/合并**
  层面的"多实例不重不漏"✓，**不是**同一段时间里两个进程**真在写**；
- `§95` 把这个话说准并**收回一格**：数据进程当时的 SQL 面是 `FlightServer::new_readonly`
  ⇒ **它没有任何"接受写入"的网络面**，"客户端真写进数据进程"在写入面补上之前**不可能证**；
- `§96` 补上了那个面：ingest 形态的 SQL 面换成 `FlightServer::new`（`standalone`/`serve_flight`
  同一条已验路径），只查询形态保持只读。

本刀把 `§96` 补的面**用起来**：两个写进程都开 `--sql-listen`，**并发真写（`DoPut`）**，
各自查询对拍。这是 M4 门槛（`plan §8.3`）字面要求的直接证据。

> 如实记一笔：任务书里提到的 `operation-log §97` 在本仓库**并不存在**（HEAD 的日志止于 `§96`）。
> 本条依据的"配方"实际来自 `§91.5` 的遗留、`§94.3`、`§95.4`、`§96.3` 三处 —— 不另造一个 §97。

### 98.2 用例（`crates/datanode/tests/datanode_forms_e2e.rs`，复用既有夹具）

形态（三个真子进程 + 一个进程内 metanode）：

```text
  metanode（进程内，真 gRPC）
    ▲ 注册/心跳（inst-a / inst-b）        ▲ 名录（含各自数据面地址）
  datanode A（--sql-listen）──┐      ┌── datanode B（--sql-listen）
  --dir A / WAL 只有 DDL      ├─同一──┤   --dir B / WAL 只有 DDL
  并发真写 1,2,3 ────────────┘ 冷存储 └─────────── 并发真写 4,5,6
                    ▲                         ▲
             A 查一次 [1..6]           B 查一次 [1..6]
```

- **夹具升级**：新增 `Proc::spawn_writer_with_sql`（在既有 `spawn_writer` 上加
  `--sql-listen 127.0.0.1:0` 与 `--reconcile-secs 1`）。两个写进程**各 `--dir`、各
  `--instance-id`、同一 `--cold-root`** —— 前两条是 `§91.2`/`T12.4` 的硬约束（私有目录被租约独占），
  后一条让协调者看得见两个写者；
- **数据全靠真写入**：`seed_wal(&dir, &[])` **只预置 DDL**（空行切片，表进元数据面），
  两边的行都由客户端经各自的 SQL 面 `DoPut` 进去（简易轨，与 `flight_sql_e2e.rs` 同法：
  schema 消息带 `FlightDescriptor{r#type:1, path=[table, shard]}`，数据消息带
  `{"idempotency_key": ...}`）。值域**不重叠**（A 写 1,2,3；B 写 4,5,6）；
- **并发**：两个 `tokio::spawn` 任务各连**自己的** SQL 面写；
- **对拍**：两个节点**各自**查一次 `SELECT a FROM yuntun.public.qd ORDER BY a` ⇒
  两边都必须得到**全部 6 行**，`[1,2,3,4,5,6]` 逐行精确相等（少行 = 漏读某个实例；
  多行 = 同一份被读两次）；
- **为什么是轮询而不是睡固定时长**："写进去了"与"查得到"之间隔着两段异步传播
  （① 写 WAL → 一个扫描周期后进 chunk；② 名录巡检发现对方 → 数据面 gRPC 拉热数据），
  所以用例轮询到精确相等为止（超时则带**最后一次实际结果**失败）。

### 98.3 反证（本仓纪律）：杀掉 B ⇒ A 只剩自己那 3 行

对拍通过后**杀掉 B**，再查 A ⇒ 必须只剩 `[1,2,3]`。它钉住的是："⑥ 里 A 看到的那 3 行
（4,5,6）**确实经数据面从 B 拉来的**"，而不是 A 不知怎么就有了 6 行。

这条反证**是确定的、不是碰运气**：flush 的最早时刻是"chunk 创建 + `min_resident`
（`IngestConfig` 默认 5s）"，而杀 B 发生在写入后一两秒内 ⇒ B 的数据只可能在它自己那
**已死的热 chunk** 里，**不可能**因为"已经落盘共享冷存储"而仍然可见。A 侧的降级/丢源由
`--partial allow`（默认）兜住，结果就是 3 行。

### 98.4 覆盖到哪、没覆盖到哪（如实说）

- ✅ **覆盖**：两个数据进程**在同一段时间里真的各自接受写入**（不是回放），且两者**各自**的
  查询都与单节点串行**逐行精确相等** —— 这正是 M4 的字面要求；
- ⚠️ **未覆盖（更强而非缺失）**：同一瞬间两个节点对**同一分片**的在途写入的**细粒度时序/交错**
  （本用例只断言最终结果相等，没有对"交错提交的线性化点"下断言）；跨节点重试的幂等已在 `§94`
  用**目录层全局幂等键**钉住（两个方向都在）；
- ⚠️ **"客户端给谁"仍无据**（`§96.3`）：设计说"任意 datanode 收到写入（无路由）"，本用例是
  **显式**把 A 的行发给 A、B 的行发给 B。把"任选一个数据进程即可"接进客户端仍是遗留。

### 98.5 顺带：清掉 3 处**既存** clippy 告警（如实，非本刀语义）

本刀验收要求"本仓 clippy 告警 0"，但实测**基线并非 0**（rustc 1.98.1 / clippy 0.1.98），
有 3 处与本刀无关的既存告警，已做**行为等价**的最小修复：

| 位置 | lint | 修法 |
|---|---|---|
| `crates/catalog/src/state.rs`（`commit_compaction` 栅栏） | `collapsible_if` | 嵌套 `if let` + `if` → `if let … && …`（let-chain） |
| `crates/meta/src/lib.rs`（`IN_FLIGHT_TTL_MS` 文档） | `doc_lazy_continuation` | 段落间补空 `///` 行 |
| `crates/compaction/src/lib.rs`（收敛等待循环，测试内） | `unused_assignments` | `loop` 改为带 break 值的表达式，去掉"先初始化再覆盖" |

> 说明：此前 `§9x` 记的"clippy 本仓 0"与本次实测不符；此处以**本次实测**为准，并顺手清干净。

### 98.6 验证

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **375 passed / 0 failed（+1 ignored）**
  （基线 374 + 本刀用例 1）；新用例连跑 5 次均绿（含反证，约 0.8–1.5s）；
- `cargo clippy --workspace --all-targets -j 8` → 本仓告警 **0**
  （`grep -cE '^\s+--> crates/'` = 0）；
- 规模：47,276 行 / 20 个 crate / 375 个测试函数。

---

## 99. 关闭最后一个真缺陷：**读侧栅栏** —— 「提交→标记」窗口不再重复计数（`§28.1`）（2026-09-24）

### 99.1 缺陷与它为什么必须这么修

`§28.1` 记录的机制：`Chunk::visible` 对 `committed_snapshot = None` 返回 `true`（"还没提交 ⇒
对所有快照可见"），而 flush 的 `commit_files`（快照 S，文件 `valid_from = S`）到调用方
`chunks.mark_committed(id, S)` 之间隔着一次 WAL fsync —— 这段时间里快照 ≥ S 的查询**同时**
读到"已提交文件"与"热数据"，把同一批算两遍（实测 4→6、9→12）。

它当时被记为"与 R3 同批设计"，并明确**否掉三条快修**（提前 mark / 本地锁串行化 / 粗粒度水位），
只留一条正路：**读侧栅栏** —— 热数据是否可见，应由"**调用方那份 manifest 快照里是否已含这批
数据**"决定，而不是由 chunk 的本地标志决定。本轮把这条正路做完。

### 99.2 做法：`batch_id` 提前登记 + 接缝带上「已知 batch 集合」

四处改动（缺一不可）：

| # | 层 | 改动 |
|---|---|---|
| 1 | `chunk` | `Chunk` 增 `batch_id: Option<String>` + `ChunkStore::note_batch_id`；读路径增 `visible_chunks_excluding` / `read_table_excluding_sync`：**属于 `exclude` 的 chunk 一律不出** |
| 2 | `ingest` | `flush_chunk_by_id` / `flush_now` 在 `commit_files` **之前**生成 `batch_id` 并 `note_batch_id`（恢复路径没有在世 chunk，无需登记） |
| 3 | `store` | `ShardReader` 增 `read_shard_excluding` / `read_table_excluding`（默认**忽略** `exclude` —— 只有"本地热数据即权威"的 `ChunkStore` 覆写）；`ShardFetch::fetch_shard` 增 `known_batch_ids` |
| 4 | `proto` + `shardrpc` + `query` | `FetchShardRequest.known_batch_ids`；服务端把集合**原样喂给本地 reader** 过滤（不自己查 catalog）；provider 把"本次快照可见文件的 `batch_id`"交给每个热读器 |

**为什么 `batch_id` 必须提前**：栅栏要的是"**与时机无关**"。若等 `mark_committed` 才登记，
窗口只缩到几条指令、**仍然存在**（`§28.1` 已论证）；提前到 commit 之前登记后，
"调用方在 manifest 里看到该 batch ⇒ 隐藏热副本"这条判据与时序**完全解耦**。

**为什么不按 `source_instance` 分实例**：`batch_id` 全局唯一（ADR-4 随机 UUIDv7），
别家的 id 落进某实例的读集合是**空匹配**；按实例分反而把"热读器键 == 写入侧 `instance_id`"
这条隐式一致性变成了正确性前提 —— 本刀**第一版就是这么写坏的**：chaos 夹具里
`IngestorConfig.instance_id` 是默认值 `"standalone"`、而热读器键是 `"chaos"`，过滤后集合为空、
栅栏静默失效（探针当场抓到，4→6）。按表给全量即可。

**为什么远端在服务端过滤、而不是服务端查 catalog**：`RemoteCatalog::list_visible_files` 会先
`refresh()`（一次网络往返），放进每次分片拉取的热路径上代价太大；而"哪些数据已进调用方的
manifest"**只有调用方知道** —— 让它把集合带过来最省也最准。

### 99.3 证据（两个方向都钉）

| 用例 | 断言 |
|---|---|
| `chaos::commit_to_mark_window_must_not_double_count`（**取消 `#[ignore]`**） | 用 `flush_now`（提交但不标记）把窗口固定住：**无栅栏读仍 3 行**（缺陷机制本身还在）、**带栅栏读 0 行**、**端到端查询 3 行**（原来 6） |
| `shardrpc::known_batch_ids_fence_survives_the_wire`（新） | 真 gRPC 往返：无栅栏 3 行、带 `known_batch_ids` 0 行、**带不相干 id 仍 3 行**（反证不误伤） |
| `chaos::compaction_during_query_keeps_counts_monotonic`（**收紧**） | 并发采样的松弛 `max <= acked + 9` 收紧为 `max <= acked` —— 那 +9 正是本窗口的松弛量，现在不再需要 |

### 99.4 覆盖到哪、没覆盖到哪（如实说）

- ✅ **覆盖**：单机（`ChunkStore` 本地读）与远端（`RemoteShard` + gRPC + proto）两条路都过栅栏；
  `§28.1` 里被否掉的三条快修留下的"不可证窗口"不存在了 —— 判据是"manifest 里有没有"，不是"标没标记"。
- ⚠️ **未覆盖（更强而非缺失）**：`known_batch_ids` 是**该表在当前快照下的全部可见文件**（按表全量、
  不按实例/分片细分）。文件数经 compaction 收敛后通常很小，但**极端多文件**时这个集合会随请求变大 ——
  本轮没做"按分片细分 / 用摘要替代"的优化。
- ⚠️ **未覆盖（约定，非缺陷）**：`ShardReader` 现在有两个"读整表"入口 ——
  `read_table` 只是 `read_table_excluding(…, &[])` 的特例。**新实现必须覆写 `read_table_excluding`**，
  否则查询路径的行为会被绕过（一个测试假实现踩过：`query/tests/partial_fanout.rs::SlowButAnswers`）。

### 99.5 顺带

- `crates/chaos` 模块文档与"两条写用例的规矩"同步改写：**五个真缺陷全部已修、当前已无 `#[ignore]` 用例**。
- `oplog §28.1` 原文保留（历史），末尾加一行指向本节。

### 99.6 验证

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **377 passed / 0 failed / 0 ignored**
  （375 + 探针转正 1 + 新远端用例 1）；
- `cargo clippy --workspace --all-targets -j 8` → 本仓告警 **0**；
- 规模：47,520 行 / 20 个 crate / 376 个测试函数。

---

## 100. 决策 C：**客户端落点 = 显式给定的 contact point**（不做客户端侧发现/路由）（2026-09-24）

### 100.1 起因：把"客户端给谁"从遗留改成决策

`§95.4` / `§96.3` / `§98.4` 一直把"客户端给谁"记为**遗留**，措辞是"设计说无路由 ⇒ 任选；
但'任选'的依据（名录 + 存活）还没接进客户端"。核对代码后，这条要说准（`§98.4` 的写法偏重了）：

- **"客户端必须知道一个数据进程地址"是设计本身**（`architecture §5` 的无路由），等价于数据库的
  **contact point / seed** 模型；客户端只连**一个**即可 —— 扇出 / 合并 / partial / STALE 由那个
  节点（协调者，`architecture §4.2`）在内部完成；
- 真正缺的只是"**不靠人给的地址、自己挑一个**" —— 那是**便利**，不是正确性。

### 100.2 三条候选，为什么采纳 C

| 方案 | 客户端怎么知道连谁 | 结论 |
|---|---|---|
| **A. 客户端读 meta 名录** | 连 metanode，从成员表挑 | ❌ 否。① 名录里那条 `address` 是**数据面**地址（`--listen`，供热读 fanout），**不是 SQL 面地址**（`--sql-listen`）——两个面两个端口，复用它会让字段变成"两种真相"；② 要新增"SQL 端点注册"并要求**固定端口**；③ 客户端要多背一份 meta gRPC 依赖与一个 meta 地址 |
| **B. 种子列表** | 部署给一串地址，客户端自己轮询挑活的 | 可行但**现在不做**：多一份"挑哪个"的策略与故障语义要维护，收益只是省一次配置 |
| **C. 显式接触点（**采纳**）** | 调用方/运维给出一个能接 SQL 的端点 | ✅ 与"无路由"完全一致：`architecture §5` 的"任选一个"由**调用方**完成，零新语义 |

### 100.3 决策边界（写清楚，避免以后再被当成"缺口"）

- **落点 = 显式给定**：`yuntun-cli --addr` / `YUNTUN_ADDR`（默认 `127.0.0.1:50051`）、
  `Client::connect`；多节点部署给**某个** datanode 的 `--sql-listen`。
- **明确不做**：客户端侧一致性哈希软路由（`architecture §4 ADR-10` 的第 3 条）、客户端读名录选落点、
  客户端感知存活。

### 100.4 影响面

- `architecture.md §4 ADR-10` 的"客户端软路由"条目已加**现状注**（未实现、当前不计划）；
- `status.md`：§2.3 该行由 ⚠️ 改为 ✅（设计如此），§5.2 从"剩余缺口"删除，§6 移出该优先级项，
  决策表新增 **D13**；
- 代码侧**零行为改动**（今天就是 C）；只把契约写进接口文档（`yuntun-client::Client::connect` 与
  `yuntun-cli --addr` 的 doc）。

### 100.5 验证

- 纯文档 + 文档注释：全量 `cargo test --workspace --no-fail-fast -j 4` → 377 passed / 0 failed；
  `cargo clippy --workspace --all-targets` 本仓告警 0。

---

## 101. 真实对象存储（S3）验证：本地 SeaweedFS 上的**网络读写**（2026-09-24）

### 101.1 为什么要做

`§37` 的多节点基线是**本机多进程共享目录**（`--cold-root`）——它恰好绕过了对象存储真实存在的
那部分：HTTP PUT/GET、寻址/鉴权、重试、以及"flush 的持久化上界"里的真实延迟。
`status.md §6` 因此把「真实 S3/MinIO + 跨机」列为里程碑门槛之外的**第一项**。
本刀先把其中**"真实 S3 的读写"这一半**做掉（跨机仍留）。

### 101.2 形态：本机 SeaweedFS（S3 兼容），**代码零改动**

```text
weed server -s3 -s3.autoCreateBucket -dir=/tmp/yuntun-seaweedfs \
     -ip=127.0.0.1 -ip.bind=127.0.0.1      # 8333=S3 / 9333=master / 8080=volume / 8888=filer
yuntun --config <[store] type="s3" endpoint="http://127.0.0.1:8333" …>
```

`StoreConfig::S3` 早已支持自定义 `endpoint`，`create_store` 用的是 **path-style**
（`with_virtual_hosted_style_request(false)`）+ `allow_http` —— 正是 SeaweedFS/MinIO 需要的形态；
`object_store` 的 region 缺省 `us-east-1`（builder 实测），无需配置。

### 101.3 踩到的坑：`-ip` 与 `-ip.bind` 必须一致（**运维向，不是本仓 bug**）

第一次 PUT **全部 500**。SeaweedFS 日志给出真因：

```text
chunked upload failed: upload 517 bytes to http://192.168.3.55:8080/8,8d28a91a7d:
  dial tcp 192.168.3.55:8080: connect: connection refused
```

只给 `-ip.bind=127.0.0.1`、**没给 `-ip`** ⇒ volume server **绑 127.0.0.1、却向 master 广播
`<机器IP>:8080`**，S3 网关按广播地址传 chunk → 拒连。**修法：两者一致**（本机测试都给 `127.0.0.1`）。

顺带两条观察，都是**正确行为**（留档，免得下次误判成缺陷）：
- S3 持续 500 时 yuntun 的行为：`flush failed (batch left non-terminal…)`，**批次保持非终态**
  （WAL 是权威），带退避重试（10 次 / ~5.4s）——**没有静默丢或重**；
- 残留实例会占住 `[meta] dir` 的 fjall 锁 ⇒ 第二个实例**响亮失败**（`FjallError: Locked`）。

### 101.4 结果：写与读**都走网络**

| 方向 | 证据 |
|---|---|
| **写（HTTP PUT）** | `INSERT 3 行` → flush 后桶里**新增** `yuntun/public/s3smoke/dt=…/shard=default/<uuid>.parquet`（517 bytes），直接列举桶可见 |
| **热读** | 写后 ≤1 个 scan 周期（本配置 50ms）查得 3 行（立刻查会看到空——文档承诺的可见性上界，不是缺陷） |
| **冷读（HTTP GET）** | **删掉 WAL + spill**、保留 meta 后重启（chunk store 全空）⇒ 仍查得 **3 行** —— 只可能来自 S3 |

### 101.5 沉淀：`scripts/s3_smoke.sh`

上面四步固化成脚本（`S3_ENDPOINT` / `S3_BUCKET` 可配；跑完自动清理进程）：

```bash
S3_ENDPOINT=http://127.0.0.1:8333 S3_BUCKET=yuntun-lake scripts/s3_smoke.sh
```

**脚本自身踩过一个坑（留档）**：`( cd … && nohup … & echo $! )` 里的 `$!` 可能拿到**中途退出的
子 shell** 的 PID ⇒ `cleanup` 杀不掉真进程 ⇒ 残留实例占住 fjall 锁、下一次启动失败（上一条现象）。
改成直接后台起 + 按"配置路径" `pkill` 兜底。

### 101.6 覆盖到哪、没覆盖到哪（如实说）

- ✅ **覆盖**：path-style 寻址、HTTP 明文、`PUT` / `GET` / `ListObjectsV2`、单节点「写入 → 冷读」闭环；
  「flush 真的走网络」与「冷读真的走网络」各有独立证据；
- ⚠️ **未覆盖**：**真云 S3**（TLS / 签名 / 区域 endpoint / 503 退避语义）、**跨机**（本刀同机）、
  Multipart（本仓仍是单段 PUT，`§7.4` 遗留）、大对象与 S3 PUT 的**绝对延迟/P99**（本刀 517 B）、
  S3 侧限流的长期行为（只观察到一次持续 500 的正确降级）。

### 101.7 验证

- `scripts/s3_smoke.sh` 连跑 2 次：写 / 热读 / 冷读三段全 PASS，退出后无残留进程；
- 纯脚本 + 文档，无 Rust 代码改动 ⇒ 全量 `cargo test` 不受影响（377 passed / 0 failed 未变）。

---

## 102. 数据进程接 **S3 冷存储** + 多节点在真实 S3 上的对拍（`§98` / `§101` 合流）（2026-09-24）

### 102.1 缺口：数据进程的冷存储只能指本地目录

`§101` 把"单机 ↔ 真实 S3"验掉了，但 `§98` 的 M4 对拍用的仍是**本机多进程共享一个
`--cold-root` 目录**（`status.md §5.2` 一直把它记作"共享目录"的替身）。查代码：`yuntun-datanode`
的冷存储是 `args.cold_root → StoreConfig::Local`，**CLI 上没有任何 S3 入口** ——
它的注释自己写着"真实部署里它是 S3"，也就是这一格从来没被接上。

### 102.2 改动：形态在**装配点**判一次，之后各处共享同一份

- `yuntun-datanode` 新增 `--s3-bucket` / `--s3-endpoint` / `--s3-access-key` /
  `--s3-secret-key` / `--s3-allow-http`；
- **一处创建、多处共享**：`store` 本来就由 `Ingestor` / `Compactor` / 孤儿 GC / `QueryEngine`
  接同一份（`main.rs` ②③④），所以形态只判一次 ⇒ 写入面与查询面**必然**看同一个桶；
- **启动即校验**（`§79` 的纪律：宁可起不来）：

  | 配置 | 处置 |
  |---|---|
  | `--s3-bucket` 与 `--s3-endpoint` 只给一个 | 拒绝（少 endpoint 会以"连不上 AWS"收场，含糊） |
  | `--cold-root` 与 `--s3-bucket` 同时给 | 拒绝（冷存储只能有一个根；混着给 = "以为在写 S3，其实在写本地"） |

- **S3 形态不再预建本地 `cold/`**：原来那次 `create_dir_all(cold_root)` 无条件发生在最前面，
  S3 部署里会凭空多一个空目录，让人以为数据落了本地。现在只有本地形态才建。

零新增依赖：`StoreConfig::S3` 早已支持自定义 `endpoint`（path-style + `allow_http`，`§101`）。

### 102.3 用例（`crates/datanode/tests/datanode_forms_e2e.rs`，进程层）

`s3_cold_store_misconfiguration_is_refused`：照 metanode 的 `process_refuses_*` 写法，
断言**退出码 + 点名的错误**（不只断言"失败"），并反证"没有建本地 `cold/`"。

### 102.4 多节点 + 真实 S3：`scripts/s3_multinode_smoke.sh`

把 `§98` 的形态（metanode 真进程 + 两个**可写**数据进程 + 并发真写 + 对拍）的冷存储换成
**S3 端点**，并补"删本地 WAL 后冷读"一段：

```text
metanode（真进程，raft 落盘）
  ▲ 注册/心跳                      ▲ 名录（含各自数据面地址）
datanode A ──┐  各 --dir 私有    ┌── datanode B
--sql-listen │  ┌──── S3 ────┐   │   --sql-listen
真写 1,2,3 ──┴─►│ 同一个桶    │◄──┴─ 真写 4,5,6
                └────────────┘
 A 查 [1..6]                    B 查 [1..6]
```

实测（本机 SeaweedFS `http://127.0.0.1:8333`，桶 `yuntun-lake`）：

| 段 | 结果 |
|---|---|
| ③ 目录同步 | A 建表（DDL 经 raft）后，B **1s 内**也看到该表（`spawn_reconcile` 每次都 `cache.refresh`） |
| ⑤ 对拍 | A=[1,2,3,4,5,6]，B=[1,2,3,4,5,6]（**逐行相等、每行只出一次**；对方 3 行经数据面 gRPC 拉） |
| ⑥ 写 S3 | 该表前缀下**恰好 2 个** parquet；两节点各自日志各 1 次 `chunk flushed` ⇒ **两个写者都 PUT 了** |
| ⑦ 冷读 | 删各自 `wal/`+`spill/`、保留 metanode 目录重启 ⇒ 两边仍 `[1..6]` ⇒ 只能来自 S3 的 HTTP GET |

一个必须写清楚的等待：数据进程用**生产默认的 seal/flush 节奏**（窗口关闭 seal + 确定性相位），
3 行的批次要等窗口关闭或 `max_resident`(60s) 才落盘，脚本因此把"等 S3 出现对象"的上限放到 180s。
**不为此加测试专用旋钮** —— 那会把"默认配置下真的能落盘"这条证据换掉。

### 102.5 覆盖到哪、没覆盖到哪（如实说）

- ✅ **覆盖**：多进程 + **真实 S3** 冷存储的"并发真写对拍"与"删 WAL 后冷读"；两个写者的文件
  确实都进了同一个桶；
- ⚠️ **未覆盖**：**跨机**（本刀与 `§101` 都是同机多进程；跨机差的是网络与时钟，不是这条链路）、
  真云 S3（TLS / 签名 / 限流）、Multipart、S3 PUT 的绝对延迟与 P99；
- ⚠️ **未覆盖**：脚本靠 `metanode` + 两个 `datanode` 真进程编排，**未进 CI**（需要外部 S3）——
  与 `scripts/bench_multi.sh` 同属"手动 / 按需"档。

### 102.6 验证

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **378 passed / 0 failed / 0 ignored**
  （基线 377 + 本刀 1）；
- `cargo clippy --workspace --all-targets -j 8` → 本仓告警 **0**；
- `scripts/s3_multinode_smoke.sh` 对本地 SeaweedFS 连跑通过（三段 PASS，退出后无残留进程）；
- 规模：47,642 行 / 20 个 crate / 377 个测试函数。

---

## 103. **3 个真 metanode 进程**：多节点真部署此前根本起不来（两处真缺陷）（2026-09-24）

### 103.1 缺口：3 个真进程从没跑过

`status.md` 的 M3 一直记着"3 节点真 gRPC raft + 换主不丢已提交（`§50`）"—— 但那条用例
（`multi_node_grpc_e2e.rs`）是**同进程内**三个 `MetaNode` + 三个真 gRPC 服务：网络路径是真的，
**进程边界是假的**。真进程组独有的三件事它一样都证不了：① 每个节点自己的目录/存储/端口；
② `--voters/--peer/--init` 这套**部署接线**真能把集群组起来；③ **杀掉一个进程再从盘恢复并重新加入**。

为什么一直没做：`--peer` 要求对端地址在**启动前**就知道 ⇒ 端口只能由部署方指定（那条用例正是
为了绕开这点才走"进程内节点"）；正路是动态成员变更，而 proto 里的 `Join` 至今 **UNIMPLEMENTED**。

### 103.2 本刀最值钱的部分：它一跑就红了，**而且红得是对的**

第一次跑，3 个真进程**一个都活不下来**，各自在 10s 后打印：

> metanode 启动失败：10s 内未当选 leader（成员表 [1, 2, 3]）

两处**真缺陷**（都在 `crates/meta/src/main.rs`）：

| # | 缺陷 | 为什么单节点踩不到 |
|---|---|---|
| 1 | **启动顺序反了**：当时是「① `open` → ② 等选主 → ③ 起 gRPC 服务」。可 raft 选主靠节点间**互相投票**，而投票走的就是那个服务 ⇒ 每个节点都在等别人的票、而谁的服务器都还没起，**集体等到超时退出** | 单节点自己一票就够，根本不发消息 |
| 2 | **判据用错**：启动闸门调的是 `MetaNode::wait_leader`（**"等本节点成为 leader"**）。raft 同一时刻只有一个 leader ⇒ **follower 永远等不到"自己当选"** | 单节点自己必然是 leader |

而且旧错误信息**指错了方向**（"超时通常意味着成员表里有本 build 不认识的节点" —— 成员表并没问题），
把排查往错误的方向带。两处一起修：

- **顺序**：先 `bind` + 起服务，**再**等选主（接口行仍按原契约在"就绪"之后打印，契约语义不变）；
- **判据**：新增 `MetaNode::wait_any_leader`（判据 `leader_id != 0` = "集群里有 leader"），
  main 路径改用它；`MetaNode::wait_leader`（等自己当选）保留给**单节点**组（`standalone` 的
  embedded metanode 仍是单 voter），并在文档里写明"多节点别用这个"。

**实证**（同机、同参数，只有代码不同）：

| | 13s 后 |
|---|---|
| 修复前 | 3 个进程**全部退出**（各自 "10s 内未当选 leader"） |
| 修复后 | 3 个进程**全部存活**，各自打印 `metanode id=N listening on 127.0.0.1:…` |

### 103.3 用例：`crates/meta/tests/metanode_cluster_process_e2e.rs`

真进程 ×3（`--voters 1,2,3` + `--peer` 填对端 + 各自空目录 `--init`），断言链：

1. 三个进程起来 → **就 leader 达成一致**（三方各自的 `Status` 都说同一个人）；
2. 3 个 op（含 DDL）经**网络**复制 → 三个进程 `applied==last` 且 `last_index` **相同**；
3. **SIGKILL 掉 leader 进程** → 存活两个重选（断言新 leader ≠ 死掉那个）+ 再次一致；
4. 换主后**仍能写入**、`manifest_ver` 推进；**旧幂等键重放必须命中**（幂等记录只在状态机里，
   命中 = 换主前的提交在新 leader 上确实还在，不是"日志里在、状态机里没有"）；
5. **重启被杀者**（同端口、同目录、**不带 `--init`**）→ 三个进程再次收敛到**同一条日志**
   （`last_index` 相同）。"不带 `--init` 还能起来"本身就是"走的是重启路径"的证据：
   启动闸门对**空目录**会退 2（`cli::check_bootstrap`）；
6. 收尾 SIGKILL 全部（`Drop` 兜底，不留孤儿）。

**端口怎么来的（如实说）**：`bind(127.0.0.1:0)` 取三个端口后**立刻释放**再交给子进程 ——
这是 `§37` 批评过的 TOCTOU。这里用**整组重试**（最多 5 次，每次换新端口 + **新目录**：
部分启动过的节点已经写过它的目录，而 `--init` 对"已有数据"的目录会被拒绝）兜住那个极小窗口。

用例 8/8 绿，单次约 **0.5s**。

### 103.4 顺带定下一条**部署约束**（必须写清楚）

每个节点都要等「集群里有 leader」才打印接口行，所以编排**必须并行起**（或至少在 10s 窗口内起齐）：
"起一个、等它就绪、再起下一个"会**僵住** —— 第一个节点的票永远凑不齐，因为后面的还没起。
（`s3_multinode_smoke.sh` 里那台 metanode 是**单节点**，不受影响。）

### 103.5 覆盖到哪、没覆盖到哪（如实说）

- ✅ **覆盖**：真进程边界（各自存储/端口）、部署接线、换主不丢已提交、**进程级重启后追平**；
- ⚠️ **未覆盖**：**网络分区**（`plan T11.5` 列了"杀 leader / 网络分区 / snapshot 重建"，
  本刀只做了第一个）；**动态成员变更**（`Join` UNIMPLEMENTED，仍是"端口必须先知道"的根因）；
  多机（本刀仍是同机多进程）；
- ⚠️ **一个没动的设计问题**：没有多数派时节点仍会在 10s 后**启动失败退出**（而不是"起得来、
  只回 `NotLeader`"）。哪个更对取决于运维口径（"起不来"更容易被发现、"起得来"更抗抖动），
  本刀**没有改**，只把它记在这里。

### 103.6 验证

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **379 passed / 0 failed / 0 ignored**
  （基线 378 + 本刀 1）；新用例连跑 8 次均绿（约 0.45–0.55s）；
- `cargo clippy --workspace --all-targets -j 8` → 本仓告警 **0**；
- 回归：`scripts/s3_multinode_smoke.sh` 对本地 SeaweedFS 仍三段 PASS、退出无残留进程；
  手工起 3 个真 metanode 进程 13s 后全部存活（修复前全灭）；
- 规模：48,267 行 / 20 个 crate / 378 个测试函数。

---

## 104. **网络分区**：少数派不能提交 + 多数派照常 + 愈合不丢不裂（`plan T11.5` 第二项）（2026-09-24）

### 104.1 缺的是什么

`plan T11.5` 那格写的是"3 节点 raft：杀 leader / **网络分区** / snapshot 重建"。上一刀（`§103`）
把"杀 leader"补到了**真进程**层，但**分区**一直没做 —— 而它才是 raft 最要命的那条：
"换主不丢已提交"在**脑裂**面前会变成一句假话。

### 104.2 怎么在**不碰生产代码**的前提下造分区

节点只认 peer 表里那个地址（`--peer` / `peers`），所以在节点之间插一层**可切断的 TCP 转发**
（`Link`），把 peer 表指向转发器即可：

```text
  node1 ──link(1,2)──► 转发 ──► node2     切断 = 关掉已建立的连接 + 拒绝新连接
  node1 ──link(1,3)──► 转发 ──► node3     隔离 1 ⇒ 切 (1,2)(1,3)(2,1)(3,1) 四条，
  node2 ◄──link(2,3)──► 转发 ──► node3           **(2,3)/(3,2) 保持通畅 ⇒ 多数派还能选主**
```

- **为什么是六个有序对、不是三条**：三条"每节点一条"会把"i→j"与"j→i"混在一起 ——
  隔离一个节点时**多数派内部也断了**，根本选不出新 leader，就测不出"多数派照常工作"。
- **为什么不用 `iptables`/网络命名空间**：要 root、进不了 CI。
- **愈合为什么不用自己写重连**：`GrpcTransport` 用 `connect_lazy` + tonic 自带重连
  （`transport.rs` 早有此说明）—— 断→通之后消息自己接上。
- **客户端读 `Status` 走直连**（不经过链路），所以分区期间仍能同时观察两边各自的状态 ——
  这正是本用例能"对着两边分别断言"的原因。

沿用 `multi_node_grpc_e2e.rs` 的夹具（进程内真节点 + 真 gRPC），只加了链路这一层；
把 `peers` 参数化之后，**生产代码一行没动**。

### 104.3 断言链（`multi_node_grpc_e2e.rs::partitioned_leader_cannot_commit_and_heals_without_loss`）

| # | 断言 | 它管哪一半 |
|---|---|---|
| ⑤ | **反证（常驻）**：未分区时，同一个调用**必须被接受** | 没有它，⑦ 的"没被接受"什么都证明不了 |
| ⑥ | 多数派（2/3）**选出新 leader 并提交成功**、`manifest_ver` 推进 | **可用性** |
| ⑦ | 少数派**不能提交**（`assert_cannot_commit`：服务端回绝 / 客户端等不到回执都算对，**只有 `accepted=true` 是错**） | **安全性（脑裂防线）** |
| ⑧ | 证据：链路**真的**被切过（传输 `failed > 0` **且** 转发器 `refused > 0`） | 防"以为切了" |
| ⑩ | 愈合后**三方同一条日志**（每个节点 `applied == last` 且三者 `last_index` 相同） | 不裂 |
| ⑪ | 多数派在**分区期间**的提交：愈合后重放**命中** | 不丢 |
| ⑫ | 少数派那条在途记录：愈合后提交**成功** ⇒ 它当时**确实没被提交**（无脑裂的正向证据） | 不裂 |
| ⑬ | 分区**之前**的提交跨过"换主 + 分区"**还在** | 不丢 |

⑩ 与 ⑫ 合起来才是"分歧尾部被覆盖"的证明：`applied == last` ⇒ 谁的日志里都没有**未应用**的尾巴
（少数派那条若还留着，就会以未应用的形式存在）；⑫ ⇒ 它也没进状态机。

### 104.4 用例自己踩出来的一个坑（值得写下来）

第一版用 `wait_some_leader`（"等某一个节点自称 leader"）判愈合后的 leader，**偶发失败**：
愈合后**被隔离过的旧 leader 会在自己那侧仍然自认 leader**一小会儿（它还没从多数派学到更高的
term），那时"我问到一个自称 leader 的"会答出**旧答案**。改成 `wait_settled_leader`
（等**三方就同一个 leader 一致**）后才稳定。这个坑是**反证那一跑**顺出来的（若不跑反证，
它会以"偶尔挂一次"的形式长期潜伏）。

顺带记一个**客户端可见的性质**（不是缺陷，是 raft 的本来面目）：分区愈合后的那个窗口里，
若客户端正连着旧 leader，它的 `Status` 会**短暂地自称 leader**。写不会出错（没有多数派 ⇒
提交不了，`NoQuorum` 可重试），但**读可能偏旧** —— 这是"元数据读要不要线性化"的问题，
本仓目前**没做**读屏障/会话，如实记在这里。

### 104.5 覆盖到哪、没覆盖到哪（如实说）

- ✅ **覆盖**：3 节点、真 gRPC、双向切链路的**分区**下的安全性与可用性；愈合后的收敛与幂等；
  链路切断的**证据**（传输计数 + 转发器拒连计数）；含**常驻反证**（⑤）；
- ⚠️ **未覆盖**：**真进程层的分区**（`§103` 那套真进程夹具没有链路层；本刀在"进程内真节点"层
  做 —— 分区分的是 raft 语义，与"节点是不是独立 OS 进程"无关，但**跨机分区**仍未测）；
  **非对称分区 / 丢包率 / 延迟**（链路只有"通/断"两档）；**`snapshot 重建`**（T11.5 第三项）；
- ⚠️ **未覆盖**：分区期间的**客户端**行为（"连到少数派应换节点重试"只验到"少数派不提交"，
  没有接客户端上去）。

### 104.6 验证

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **380 passed / 0 failed / 0 ignored**
  （基线 379 + 本刀 1）；新用例连跑 5 次均绿（8–12s）；同文件原有 3 节点用例不回归；
- 反证实验：把 `links.isolate(leader)` 注释掉 ⇒ 用例**失败**（多数派选不出新 leader，走不到 ⑦）；
  常驻反证见上表 ⑤；
- `cargo clippy --workspace --all-targets -j 8` → 本仓告警 **0**；
- 规模：48,671 行 / 20 个 crate / 379 个测试函数。

---

## 105. 压缩**触发策略**（`--snapshot-log-entries`）：补上"快照从没被触发过"这一环 + 一个**未根因**的写停摆（2026-09-24）

### 105.1 缺口：驱动层根本没有压缩触发

`plan T11.5` 的第三项是"snapshot 重建"，而 `§42.4b` / `§42.7` 遗留 4 一直写着
"快照**触发策略**（§4.4：日志条数 > N / 状态 > M）未做"。查代码：
`compact_applied` 在整个仓库里**只被测试用的 `Cluster::compact(id)` 调过** —— 也就是说
**进程形态的 metanode 永远不会产生快照**，日志只增不减，"落后节点追快照"这条路在真实部署里
根本走不到。**这就是 `§42.4b`"稳定触发快照安装未拿到"的根因**（不是 raft 的问题）。

### 105.2 交付

| 位置 | 内容 |
|---|---|
| `crates/meta/src/lib.rs` | `MetaOptions { compact_log_entries }`（默认 **100_000** = 设计 §4.4）+ `MetaNode::open_with`（`open` 委托默认值）；驱动循环里"应用后按阈值压到已应用位置" |
| `crates/meta/src/cli.rs` | `--snapshot-log-entries`（`0` = 关） |
| `crates/meta/tests/multi_node_grpc_e2e.rs` | `snapshot_trigger_compacts_the_log_by_policy` |

触发点为什么放在应用线程：`compact_applied` 的纪律是"**产物与 `applied` 必须同一瞬间取到**"，
只有应用线程能保证（`storage.rs` 的坐标纪律）。**反证**：阈值设 0 ⇒ `snapshot_index` 恒为 0
⇒ 用例失败 ✓（实测）。

### 105.3 ⚠️ 一个**未根因**的写停摆（如实登记，不粉饰）

把阈值调到很小（4）**并且**有 follower 落后时，会出现：**集群 30s+ 写不进去**
（`propose_following_leader` 拿不到被接受的响应）。同一时刻的旁证：

- 传输层**健康**：三个节点 `failed=0 / rejected=0`（唯一失败来自被隔离的那个节点）；
- leader 的 `role` 仍是 **Leader**（不是"没选主"）：它的 `last_index` 在前进，
  而**两个 follower 的 `committed` 卡住不动**；
- **没有** raft 线程 panic（raft-rs `fatal!` 那条路可排除）；
- 消息轨迹里 leader 反复发**空 append**（`ents=0`）；
- 与"是否隔离"无关：三方全通时同样复现。

**已排除**：① 传输层；② "没有 leader"（leader 在，只是提交不了）；③ raft 线程 fatal；
④ **缓存 leader**（这是我第一次误诊的原因：测试缓存了 ② 那一刻的 leader，小阈值下选主会抖动，
于是把 `NotLeader` 读成了"集群停摆" —— 改成跟随 leader 之后**仍在 30s 层面复现**，所以不是同一个原因）。

**尚未试**：给 `Raft` 换一个真的会输出的 slog logger（现在的 `default_logger()` 不输出，
所以看不到 "Skipping sending to X, it's paused" / "sent snapshot" 这类关键判定）；
以及把 `Progress` 的 `next_idx/matched` 与 `raft_log` 视图逐条对齐。**这是下一刀的事。**

**一条试过并撤掉的修法**（如实记）：给压缩加"**安全水位**"（只在**所有 peer 都拿到**已应用位置
时才压 —— TiKV 的 `min_matched` 思路）。实测**没能**止住上面那个停摆（阈值 4 + 隔离 follower
仍 30s 写不进），所以我**没有**把它留在代码里：不留没被证实有效的东西。它另有一个坏处 ——
follower 长期不在线会把日志拖大，而"落后 → 走快照"恰恰是设计想要的路径。

### 105.4 覆盖到哪、没覆盖到哪（如实说）

- ✅ **覆盖**：**触发策略本身**（`snapshot_index` 由策略推进，反证成立）；设计默认值（10 万）下行为
  与从前一致（既有用例全绿）；
- ❌ **没覆盖（T11.5 第三项因此仍未关闭）**：**快照安装的集成层稳定复现**。要在 3 节点里稳定压出
  "追快照"，必须让 leader 压掉 follower 还需要的那一段 —— 而那里正是 105.3 那个停摆所在区间。
  这与 `§42.4b` 自己的结论一致：稳定复现的正路是**成员变更**（新节点加入，S3-6 / `Join`）；
- ❌ **未实现**：设计 §4.4 的另一个判据（**状态 > 256MB**）；快照的**保留策略**
  （按表 checkpoint + 归档旧条目，`§42.7` 遗留 4 的后半）；
- ⚠️ **一条与本刀无关的观察**：本轮全量**第一次**跑时，chaos 的
  `disk_watermark_aborts_oldest_batch_then_releases_segments` 失败一次
  （`WAL 恢复出的批次应全部终态：[("…", Pending)]`，`-j 4` 并行负载下）；**单跑 3/3 绿**、
  重跑全量 381/0 绿 ⇒ 登记为**负载敏感的抖动**，本刀**未修**。

### 105.5 验证

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **381 passed / 0 failed / 0 ignored**
  （基线 380 + 本刀 1）；第一次跑出现上述 chaos 抖动 1 次，重跑干净；
- 新用例连跑 3 次均绿（约 0.2s）；反证（阈值 0）实测失败 ✓；
- `cargo clippy --workspace --all-targets -j 8` → 本仓告警 **0**；
- 规模：48,827 行 / 20 个 crate / 380 个测试函数。

---

## 106. 压缩 + 落后 follower ⇒ 集群写停摆：**复现 + 收窄**（探针留下，根因未钉死）（2026-09-25）

### 106.0 这一刀做了什么、没做什么

`§105.3` 记了一个"尚未根因"的写停摆。本刀把它变成**可稳定复现的最小用例**、拿到了 **raft 自己的
一手日志**（关键技术发现见 106.1），并把症状收窄到**一条互相矛盾的事实**（106.4）——
**但根因仍未钉死**。所以：用 `#[ignore]` 探针把复现留在仓库里（先例 `§28.1` 的
`commit_to_mark_window_must_not_double_count`），**不假装它已解决**。

### 106.1 关键技术发现：raft-rs 的日志**一直可用**，只是没人设环境变量

`§105` 我以为"`raft::default_logger()` 不输出"。事实：它是 `slog_term::CompactFormat`
+ **`slog_envlogger`**，而 `slog_envlogger` 读 **`RUST_LOG`** ✓。所以

```text
RUST_LOG=raft=debug cargo test -p yuntun-meta --test multi_node_grpc_e2e -- --ignored --nocapture
```

就能拿到 raft 的完整内部独白（选主、每条收发的消息、暂停判定、快照判定）—— **不用改一行代码**。
配上存储层的 `YUNTUN_META_TRACE=1`（`append`/`compact`/`recv_snapshot` 轨迹），两条 env 就是
metanode 的全部诊断开关。这条配方值钱，已写进探针的文档注释。

### 106.2 复现（`compaction_with_lagging_follower_should_keep_committing`，`#[ignore]`）

3 节点真 gRPC + 小压缩阈值（4）+ 隔离一个 follower ⇒ `k=0..4` 写成功，**`k=5` 起 30s 拿不到
被接受的响应**。稳定复现（多次一致），且**与"是否隔离"无关**（三方全通时同样复现）。

### 106.3 症状（一手证据，逐条）

| 观察 | 证据 |
|---|---|
| leader 仍是 Leader，`last` 前进而 `commit` 卡住 | `role=Leader first=9 last=11 commit=9 applied=9` |
| leader 对目标 peer 的 `next` **远超** `matched` | `peer3(next=12, matched=9)`（正常应 `next == matched+1`） |
| leader 只发**空** append | 窗口内 `Sending from 2, msg_type: MsgAppend` 里**带 entries 的 0 条** |
| follower 对每条 append 都回**接受** | 窗口内 306 条 `MsgAppendResponse`，其中 `reject: true` **0 条** |
| 传输层"不报错" | `failed=0 / rejected=0`；`active=true`（leader 收得到 follower 的消息） |
| **没有**被暂停、**没有**快照尝试 | raft 日志里无 `Skipping sending ... it's paused`、无 snapshot 相关行 |
| **没有** raft 线程 panic | 输出里除测试自身外无 panic |

### 106.4 矛盾点 —— 这就是下一刀的入口

leader 反复发 `MsgAppend log_term: 1 index: 11`（`prev=11`），而 follower 的 `last=9`
—— 按 raft 它**必须**回 `reject: true`（`match_term(11, 1)` 不可能成立），实际却回
`reject=false, index=9`。**唯一自洽的解释**：follower **根本没收到那些 `prev=11` 的 append**，
它一直在回**更旧的、它确实能接受的那条**。

⇒ **下一步查传输层，不是 raft**：给每个 peer 的 `send_loop` 加"发出去的
`(msg_type, index, entries)` ↔ 对端回的 `(index, reject)`"**逐条配对计数**，先回答
"leader 发的东西到底有没有到对端"。⚠️ 注意 `GrpcTransport::send` 是 `try_send` + 满了就丢，
`dropped` 此前一直是 0 —— 但那只是**入队时**的丢弃统计，**不覆盖**"发出去了但对端看不见"。

### 106.5 已排除

① 网络/传输"报错"（`failed/rejected` 全 0）；② "没选主"（leader 在位、`active=true`）；
③ raft 线程 fatal；④ leader 被暂停（inflights 满）或走了快照；⑤ **测试自己缓存 leader**
（`§105.3` 记的误诊；本刀的探针已改成跟随 leader）。

### 106.6 验证

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **381 passed / 0 failed / 1 ignored**
  （基线 381；新增的探针**按设计 ignore**）；单跑探针：
  `cargo test -p yuntun-meta --test multi_node_grpc_e2e -- --ignored`；
- `cargo clippy --workspace --all-targets -j 8` → 本仓告警 **0**；
- 另记一条**负载敏感抖动**：本轮全量在"机器刚重启 + 连跑多轮"的负载下，
  `compaction` 的 `concurrent_writers_compaction_and_gc_converge` 与 `query` 的
  `slow_sources_wait_in_parallel_not_in_sequence` **各失败过一次**（都是**带 deadline 的时序用例**），
  **单跑全绿**、负载降下来后重跑全量 381/0 ⇒ 登记为负载敏感抖动（与 `§105.4` 那条同类），本刀未修；
- 规模：48,945 行 / 20 个 crate / 381 个测试函数（含 1 个 `#[ignore]` 探针）。

---

## 107. 顺着 `§106` 查下去：**丢一条 append 就永久停摆**（机制钉死；三种修法都被实测否掉）（2026-09-25）

### 107.0 结果先说清（含一次**自我更正**）

- ✅ **机制钉死了**（`§107.2`）：传输丢一条**携带条目的** append ⇒ leader 的 `next_idx` 已乐观
  推进在前、而 `matched` 留在原地 ⇒ 它只会发**空** append ⇒ follower 恰好能接受 ⇒ **永久停摆**。
  这次是**同一瞬间**的视图 + 逐条轨迹配出来的（不是跨 run 猜的）。
- ❌ **修法全部撤回**：本刀先后改过三处行为（无界队列 / 送达为止重试 / 每次丢包
  `report_unreachable`），**都被自己的实测否掉**（`§107.3`），已全部回退 —— 只留诊断能力。
- 🧹 **更正上一版 `§107` 的一处错误结论**（`§107.5`）：先前写"leader 的 `Status` 报 `last_index=10`、
  而 `entries` 调用里 `last=9`，两者不一致" —— **那是跨两次 run 各看一个数得出的假矛盾**。
  同一瞬间并排打出来（`§107.1` 的"视图快照"）后：两个视图**完全一致**。
- 🧭 **环境已排除**（`§107.4`）：停摆**不是**连接抖动的产物 —— 健康 3 节点用例的连接失败数 = **0**。

### 107.1 诊断能力（本刀唯一的产出，留在代码里、默认全关）

| 轨迹 | 开关 | 回答什么 |
|---|---|---|
| **raft → 传输**（`transport.rs` 的 `send()`） | `YUNTUN_META_TRACE=1` | **raft 交给了传输什么**（就在这一刻） |
| 出站出队（`send_loop`） | 同上 | **真正发出去什么**（`index`/`entries`/`commit`/`reject`/`hint`） |
| 入站（`service.rs`） | 同上 | 对端**收到什么**（含 `delivered`） |
| 存储 `entries()`（`fjall_storage.rs`） | 同上 | leader **被问要哪些条目**、给了几条 |
| **视图快照**（每秒一行） | 同上 | **同一瞬间**并排：`raft[first/last/committed/applied]`、`store[…]`、各 peer 的 `matched/next/state` |
| raft 内部 | `RUST_LOG=raft=debug` | 选主 / 收发 / 暂停 / 快照的判定（`§106.1`） |

前两条**配对**用，是本刀最值钱的那一步（见 `§107.2` 的表）。

### 107.2 机制（已钉死）：**一条 append 丢了，就再也补不上**

同一瞬间的视图（leader=`2`，停摆中、稳定持续十几秒）：

```text
[meta:2] 视图 raft[first=5 last=8 committed=7 applied=7] store[first=5 last=8 compact=4 applied=7]
         peers 1:m3/n9/Replicate  2:m8/n9/Replicate  3:m7/n9/Replicate
```

`§107.1` 前两条轨迹的配对（同一窗口）：

| 轨迹 | 内容 |
|---|---|
| `[meta:send]`（raft 交出） | `index=9 entries=0` ×160，**外加 1 条 `index=8 entries=1`**（携带 entry 9 的那条） |
| `[meta:transport]`（真正出队） | 99 条**全是旧的** `index=8 entries=0 commit=7` |

于是链条是：

1. 那条**唯一**携带 entry 9 的 append 只发过一次、之后再没被发（`entries` 轨迹里**从未**为
   `[9,10)` 被问过 —— 因为 `next_idx` 已被**乐观推进**到 10）；
2. leader 此后反复发**空** append（`prev_index = next_idx - 1 = 9`）；
3. follower 的日志里**正好有** 9 ⇒ 它**接受**并回 `index=9`（`reject=false`）；
4. `matched` 只能单调前进 ⇒ **leader 永远不会再发那条丢掉的条目** ⇒ 写入停摆，**且没有任何错误**。

### 107.3 试过并**全部撤回**的三处改动（逐条给实测理由）

| 改法 | 为什么撤回 |
|---|---|
| **入队无界**（不再 `try_send` 满就丢） | 只堵住"队列满"这一条丢包途径，而实测丢点在 `client.raft()` 报错那条分支；代价是长期分区下**积压无界**（积压量 ∝ 来不及送的日志） |
| **送达为止重试**（RPC 失败无限重试，退避 5→500ms） | **队头阻塞**：一条送不出去的消息把后面**所有**消息压在队列里 —— 实测 raft 已交出 161 条（含那条携带条目的），出队停在 99 条旧的，那条**永远没出去** |
| **每次丢包就 `report_unreachable`** | 方向对（`RawNode::report_unreachable` 是真入口：raft 收到 `MsgUnreachable` 会把该 peer 从 Replicate 退回 Probe ⇒ `next_idx = matched + 1`），但**没限流**：实测触发 **1308 次/30s**（等于每次丢包都退回 Probe，而 Probe 一次只发一条 ⇒ 把复制限流到 ~1 条/秒），且**没能救回停摆** |

纪律同 `§105.3`（撤"安全水位"）与本刀上一版（撤"搁浅看门狗"）：
**不留没被证实有效、且有副作用的改动**。

### 107.4 环境已排除：停摆不是连接抖动的产物

`§106` 的探针用 `§104` 的可切断链路夹具隔离一个 follower。先怀疑"是不是链路抖动造成的丢包风暴"，
于是量了两件事（同一套轨迹）：

| 场景 | 连接失败次数 | `[meta:send]` : `[meta:transport]` |
|---|---|---|
| **不做任何隔离**（健康 3 节点） | **0** | **74 : 74（完全配对）** |
| 隔离一个 follower（探针） | 1080–2943（≈43/s） | 明显不配对（见 `§107.2` 的表） |

再按 peer 拆开那 1080–2943 次：`→1` 上千次、`→2`/`→3` 各百余次 —— 而本 run 里**被隔离的正是 1**，
所以 `→2`/`→3` 那些是**它自己发给别人的**（走它那两条被切断的链路）。
**结论：风暴全部可归因到被隔离节点自己的两条链路；多数派那条是干净的**（否则不会这么对称）。
⇒ 停摆发生在**链路健康**的多数派上，**不是**环境噪声。

### 107.5 我被证伪的两个假设（记下来，免得下一刀重走）

1. **"两个视图不一致"**：上一版写"`Status` 报 `last_index=10`、`entries` 调用里 `last=9`"——
   那是**跨两次 run 各看一个数**得出的假矛盾。同一瞬间并排后：`raft[first=9 last=9 …]` 与
   `store[first=9 last=9 …]` **完全一致**。
2. **"是存储 `entries()` 返回了空"**：本 run 的轨迹显示 `[8,9) first=5 last=8 → 1 条` ——
   该给的时候**都给了**；leader 之所以发空 append，是因为 `next_idx(3) > last(9)`，
   raft 在 `RaftLog::entries` 里**早退**（`idx > last` ⇒ 空），**根本没问存储**。

### 107.6 下一刀（明确到可执行）

1. **先做判别实验**，把两个变量分开：`丢包即丢（无重试）` **+** `限流的 report_unreachable`
   （≤1 次/秒/peer）。已知：无重试单独用仍停摆；`report_unreachable` 无限流时触发 1308 次且无效。
   所以要么限流后有效，要么这条入口救不了**这个**停摆 —— 那就说明 `matched` 之外还有一层。
2. 若限流版仍无效：查 **inflight 释放**。`Progress::maybe_update` 只在 `index > matched` 时才
   `ins.free_to(index)`，而停摆时 follower 回的 `index == matched` ⇒ **那条"乐观发出"留下的
   inflight 永远不会被释放**。这和"乐观推进"是**同源**的，值得先证伪/证实。
3. 验收形态：把探针的 `#[ignore]` 拿掉（它现在稳定复现，25–45s）。

### 107.7 验证

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **381 passed / 0 failed / 1 ignored**
  （ignored 即 `§106` 的探针，本刀**未**让它转绿，故仍 ignore）；
- `cargo clippy --workspace --all-targets -j 8` → 本仓告警 **0**；
- 重点回归：`multi_node_grpc_e2e`（3 节点 / 分区 / 压缩策略）3 绿 1 ignored、
  `metanode_cluster_process_e2e`（真进程组）1 绿、`raft_poc` 4 绿；
- **行为回到原状**：传输层仍是"队列满丢 + RPC 失败丢"（三处实验改动全部撤回），只新增默认关闭的轨迹；
- 规模：49104 行 / 20 个 crate / 381 个测试函数（含 1 个 `#[ignore]` 探针）。

---

## 108. **容器化三节点真集群**：一跑就抓到一个真 bug（对端换 IP 后 leader 再也送不到）已修（2026-09-25）

### 108.1 动机：把"环境"这个变量拿掉

`§104` 的"网络分区"是**进程内夹具**（可切断的 TCP 转发）做的，`§106`/`§107` 那个写停摆一直卡在
"**是不是夹具造出来的**"上：夹具断链后会拒绝新连接、`copy_bidirectional` 被 abort，行为与真断网
并不等价。本刀用容器（真网络命名空间、真 TCP、`podman network disconnect` = 真断网）把那个变量
拿掉 —— 顺带补上 `§103` 记过的缺口：**全仓没有 Meta gRPC 客户端**，所以容器里的集群没法断言。

### 108.2 rig 是什么

| 文件 | 作用 |
|---|---|
| `tests/compose.yaml` | 三个 `metanode` 容器（`--voters 1,2,3` + `--peer 2@mn2:9311,…`）。**用容器名当地址**：名字在编排时就固定，顺手满足 `§103` 的"对端地址必须先知道" |
| `tests/cluster.sh` | `up / wait / probe / partition <N> / heal <N> / smoke / logs / ps / down`；`partition` = `podman network disconnect`，`heal` = `network connect --alias` |
| `crates/meta/examples/meta_probe.rs` | 极小 Meta 客户端：`status / leader / write <key> / verify <key>`。**`verify` 是重放同一个幂等键、必须被拒** —— 命中幂等记录 = 那条提交真的在状态机里（同 `§50`/`§103` 的判据） |

**不在容器里编译**：镜像就用本机已有的 `debian:trixie-slim`，二进制由宿主 cargo 编好挂进去 ——
实测宿主 glibc（2.44）编出的二进制在这个 2.41 的镜像里**能直接跑**（无缺失符号）。20 个 crate 在
容器里编一遍又慢又要拉 crates.io，而这里要验的是**运行期**行为。

### 108.3 它一跑就抓到一个**真 bug**（已修）

`smoke` 第一版是：healthy 写 → 断 mn3 → 多数派写 → 接回 → 等收敛。前两段就过了，**第三段红**：

```text
⛔ 断掉 mn3：多数派（1+2）accepted=true manifest_ver=2   ← 真断网下多数派照常提交 ✓
   （重放 k2 被拒 ✓ ⇒ 那条真的进了状态机）
🔌 接回 mn3：
   mn1: Leader  term=1 commit=4 last=4
   mn2: Follower term=1 commit=4 last=4
   mn3: PreCandidate term=1 commit=2 last=2      ← 30s 都不动
```

轨迹（`YUNTUN_META_TRACE=1` + `RUST_LOG=raft=debug`）把根因指出来了：

* leader 侧对 mn3 **一直 `Timeout expired`**（12 次），而**同一时刻新起的**探针容器连 mn3 完全正常；
* mn3 自己 → mn2 是通的（它在广播 pre-vote、收到 mn2 的响应，但被**拒** ⇒ 永远选不上）；
* mn3 因为收不到 append，就一直当 **PreCandidate**；leader 因为以为它在，就不再补日志。

**根因**：容器/pod **重新接入网络会换 IP**（实测 `10.89.0.2` → `10.89.0.10`，旧 IP 还可能被别的
容器复用），而 tonic 的通道把地址 **pin 在建通道那一刻**解析出的结果上 ⇒ leader 一直在往旧地址发。
这正是"多节点真部署"必然踩的一格，**`§104` 的进程内夹具测不到它**（夹具里 IP 从不变化）。

**修法**（`transport.rs`）：`send_loop` 连续投递失败 `REBUILD_AFTER = 3` 次就**重建通道**
（`make_client(addr)` 造一个全新通道 ⇒ 名字重新解析一遍），成功一次即清零；重建**无条件打一行**
（这是该被运维看见的事件）。**对照**：

| | 接回后 |
|---|---|
| 修前 | 30s 不收敛（mn3 停在 `last=2`，一直 PreCandidate） |
| 修后 | **2s 内收敛到 `last=4`**（`⋯ 还没收敛` ×2 → `✅ 三方已收敛`），随后写 k3 再收敛到 `last=6` |

顺手把 `smoke` 的**收敛断言**做成硬失败（`await_convergence`）——脚本第一版只是"打印三方 Status
就宣布应当收敛"，而那次 mn3 其实还落后 2 条：**嘴上说收敛、实际没验**。

### 108.4 两条踩坑（都写进了 rig 的注释）

1. **`podman network connect` 必须带 `--alias`**：不带的话，容器在这张网络的 DNS 里只剩容器哈希,
   别的节点**再也解析不到** `mn3` 这个名字（它们连的是名字）。第一版没带 ⇒ "接回来自愈"整段连不上。
2. **轨迹别在真实集群里默认打开**：`YUNTUN_META_TRACE=1` 是"每条消息一行 `eprintln!`"，实测把它
   默认打开后三个容器**根本起不来** —— stdout 管道被塞满 ⇒ **进程被写阻塞**。于是
   `trace_on()` 收紧为"**非空且不是 `0`**"（编排里常见的 `- VAR=${VAR:-}` 会给出"已设置但为空"，
   用 `is_ok()` 判会被误当成要打轨迹），compose 里改成**按需注入**：
   `YUNTUN_META_TRACE=1 bash tests/cluster.sh up`。

### 108.5 覆盖到哪、没覆盖到哪（如实说）

- ✅ **覆盖**：三节点真集群（真网络命名空间 / 真进程 / 真容器）；**真断网**下的"多数派照常提交"、
  "少数派写不进"；接回后的**收敛**（带硬断言）；**对端换 IP** 这一格；
- ⚠️ **未覆盖**：**跨机**（本刀仍是同机多容器；跨机差的是网络与时钟）；**非对称分区 / 丢包 / 延迟**
  （容器上可加 `tc netem`，本刀没做）；`Join`（动态成员）仍是 UNIMPLEMENTED ⇒ 地址仍要**先知道**；
- ❗ **两回事要说清**：本刀修的是"**对端换了地址、leader 再也送不到**"。`§106`/`§107` 那个
  "**一条携带条目的 append 丢了、乐观推进的 `next_idx` 再也补不上**"是**另一个**机制，
  **仍未修**（`§107.6` 的下一刀不变：限流的 `report_unreachable`，再不行查 inflight 释放）。

### 108.6 验证

- 全量 `cargo test --workspace --no-fail-fast -j 4` → **381 passed / 0 failed / 1 ignored**
  （`ignored` 仍是 `§106` 的进程内探针 —— 它测的是上面那个"另一回事"，本刀**没有**让它转绿）；
- `cargo clippy --workspace --all-targets -j 8` → 本仓告警 **0**；
- `tests/cluster.sh smoke` 通过（其中"接回后必须收敛"是**硬断言**，修前它会红）；
- 规模：49361 行 / 20 个 crate / 381 个测试函数（含 1 个 `#[ignore]` 探针）。
