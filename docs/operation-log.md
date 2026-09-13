# yuntun 阶段 0 实现操作日志

> 记录时间：2026-09-08。对应分支：v2（旧 Rust 原型代码已删除，全新实现）。
> 本日志记录实际操作顺序、**临时调整**以及**与原计划（docs/design.md / architecture.md / plan.md）的偏差点**，供后续阶段（0.5 chaos / 1 分布式）核对。

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
