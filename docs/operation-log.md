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
