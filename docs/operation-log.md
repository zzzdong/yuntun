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
5. `process_use_statement_on_query` 默认 false → USE 走 `on_init`（不进 on_query，方言 shim 的 USE 拦截仅兜底）。

### 13.3 待办（接续 §11.3）

| ID | 任务 |
|---|---|
| **W-3.5** | `cargo test -p yuntun-sqlwire`（含 encode 日期/decimal/二进制日期单测已写未跑）+ 全量回归 |
| **W-4** | server `Config [sql.mysql]`（enabled/listen :3306/auth users 预留）+ serve 流程挂载 `serve_mysql`（standalone yuntun.toml.example 更新） |
| **W-5** | pymysql 冒烟 T1/T2（aliyun pip 装 pymysql）：SELECT/SHOW TABLES/INSERT + prepared |
| **W-6** | clippy --workspace --all-targets 0 警告；README MySQL 连接示例（S1.11） |
