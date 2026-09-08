# 通用直写数据湖架构设计

> **版本**：v10（实现路径收敛版）
> **日期**：2026-08-31
> **状态**：已整合四轮外部评审意见（6 份评审报告），并落实**组件选型收敛**，**可进入阶段 0 编码**
>
> **版本演进**
> - **v7 相对 v6**：整合评审修正（幂等键、SLA 分级等）+ 新增 DataFusion 集成技术路线（第 8 章）+ Schema 演进设计（第 6 章）
> - **v8 相对 v7**（详见附录 E）：
>   1. **修正幂等键生命周期耦合**（独立发现，P0）—— 幂等记录必须与 FileManifest 解耦
>   2. **修正整分钟对齐的惊群效应**（P0）—— 引入 Flush Jitter 削峰
>   3. **幂等键 TTL 24h + 语义边界**（P0）
>   4. **InfluxDB Tag/Field 语义映射**（P1）
>   5. **Schema 演进乐观并发控制**（P1，作用点已修正评审建议）
>   6. **澄清两层 Adapter 配合使用**（非二选一）
> - **v9 相对 v8**（详见附录 F）：
>   1. **幂等键默认值 + 表模板**（P0）—— 默认开启，`Metrics`/`Traces` 模板可关闭，R12' 闭环
>   2. **压测提前**（P1）—— 新增阶段 0.5（核心逻辑，Mock S3），阶段 1.5 收窄为分布式专项
>   3. **Compaction Schema Snapshot**（P1）—— 启动时锁定 schema 版本
>   4. **S3 Multipart 7 天超时**处理、**Meta Raft 吞吐目标** 10K/sec（P2）
>   5. **GC 水位澄清**、**不丢弃旧 schema 文件**、**ADR 注释规范**
> - **v10 相对 v9**（本文档，详见附录 G）：**组件选型收敛**
>   1. **Ingestor WAL 自实现**（移除 fjall）—— 仅需 append-only，约 500 行；`BatchState` 写入同一 WAL 事件流，原子性天然保证（§5.3）
>   2. **MVP 仅 Arrow Flight 写入** —— `IngestSource` trait 抽象保留，InfluxDB / Kafka 延后至阶段 2+（§7.1.1）
>   3. **Meta 存储澄清**（§5.4）—— **fjall 只存 raft 数据**；Catalog 是 raft state machine，放**内存 + snapshot**，**不落 fjall 作权威**（避免一致性陷阱）
>
> **v11 相对 v10**（工程补丁，详见附录 H）：终审提出 3 个工程级缺陷，**全部采纳，其中 2 处实现细节已修正**
>   1. **`synced_offset` 水位线**（§5.3.5.1）—— 修复组提交下攒批线程可能读到未 fsync 数据的正确性缺陷；且该水位**由 CRC 自然确定，无需单独持久化**
>   2. **Batch 超时 + 终态含 ABORT**（§5.3.6.1）—— 修复 batch 卡死导致 segment 永不释放、磁盘写满的 P0 风险；取 30 分钟 + 80% 磁盘水位分级
>   3. **Snapshot 异步生成**（§5.4.3.1）—— 修复同步序列化阻塞 apply 线程引发 Leader flapping；**修正评审的"克隆 Arc"建议**为持久化数据结构 O(1) 快照
>
> **v12 相对 v11**（2026-09-08，Standalone 优先路线，详见《开发计划任务书 v2.0》）：
>   1. **工程结构**：撤销 `bins/`，`all-in-one` 更名 `standalone`（bin 名 `yuntun`）独立成 crate；新增 `yuntun-client`（SDK + CLI）crate
>   2. **阶段重排**：新增**阶段 1 Standalone 完备**（Flight SQL 标准协议 + SQL 写入 + 自有客户端）；原阶段 0.5（Chaos 压测）后移为阶段 2；**原阶段 1（Meta Raft 分离）整体后移为阶段 3** —— 除分布式外的一切能力先在 standalone 内完成
>   3. **接口演进**：在既有自定义 ticket 模式（保留）之外，新增 **Flight SQL 标准协议**双轨接入（§3.2 / 计划书 §四）
>   4. **【v12.3，2026-09-09】架构简化**：撤销 Hook / Gateway 间接层；协议端口统一在 **server 节点层**，`FlightServer` 直接组合 `Arc<Ingestor>` + `Arc<QueryEngine>`；两条铁律成文——**所有写入走 ingest 管线（唯一写入事实）**、**协议端口在 server 层，域 crate 纯能力**（§3.2）

---

## 目录

1. [文档目的与评审焦点](#一文档目的与评审焦点)
2. [目标与非目标](#二目标与非目标)
3. [架构总览](#三架构总览)
4. [核心设计决策（ADR）](#四核心设计决策adr)
5. [数据模型](#五数据模型) —— §5.3 **自实现 WAL**、§5.4 **Meta 存储**（v10）
6. **[Schema 演进设计](#六schema-演进设计)**
7. [写入路径](#七写入路径) —— §7.1.1 **Source 可插拔抽象**（v10）
8. **[DataFusion 集成技术路线](#八datafusion-集成技术路线)**
9. [查询路径](#九查询路径)
10. [删除与更新语义](#十删除与更新语义)
11. [崩溃恢复与一致性模型](#十一崩溃恢复与一致性模型)
12. [后台作业](#十二后台作业)
13. [分布式设计与部署演进](#十三分布式设计与部署演进)
14. [风险、权衡与待决策项](#十四风险权衡与待决策项)
15. [演进路线图](#十五演进路线图)
16. [附录](#十六附录)

---

## 一、文档目的与评审焦点

### 1.1 v7 修订说明

v6 经历了两轮外部评审。两轮评审**互补性极强**：评审 A 抓"架构灵魂"（识别出客户端幂等这一致命漏洞），评审 B 抓"落地骨架"（Schema 演进、S3 Multipart、资源隔离等细节）。

**两轮评审的分歧点及裁决**：

| 分歧 | 评审 A | 评审 B | v7 裁决 |
|---|---|---|---|
| R1/R2 无分布式 WAL、无 failover | 完全可接受 | 通用场景不可接受 | **SLA 分级**（表级配置），非架构分支 |
| R8 小文件 | 未识别此代价 | 一致性哈希路由 | **时间窗口对齐 + 客户端软路由**，不破坏 ADR-3 |
| R6 读己之写 | 维持回执方案 | Query 广播拉 MemTable | **维持回执方案**（广播在 100 节点不可行） |

### 1.2 请评审重点关注

| # | 焦点 | 章节 |
|---|---|---|
| **R1'** | SLA 分级（best_effort / durable）是否化解"通用"定位歧义 | §4-ADR-9 |
| **R2'** | **Schema 演进模型**（三层 Schema + 类型提升格）是否完备 | §6 |
| **R3'** | DataFusion 集成路线中 `PhysicalExprAdapter` 的使用是否正确 | §8.4 |
| **R4'** | 幂等键三层防护是否覆盖所有重复场景 | §7.3 |
| **R5'** | 时间窗口对齐 + 软路由，能否在不引入硬路由的前提下控制小文件 | §4-ADR-10 |

---

## 二、目标与非目标

### 2.1 目标

| 目标 | 指标 |
|---|---|
| **【v10 调整】直写摄入（MVP）** | **仅 Arrow Flight**（InfluxDB 线协议 / Kafka Source 延后至阶段 2+） |
| **Schema 演进** | 支持加列、列类型宽化；窄化需显式 DDL |
| 分片级数据移除 | 按 shard/partition 整体删除 |
| 交互式分析 | 秒~十秒级 |
| 开放格式 | Vortex / Parquet + Arrow |
| 可演进 | v1 单进程，v2+ 平滑分布式 |
| **【v9 新增】Meta 写入吞吐** | 单 Leader **10,000 CommitFiles/sec**（批量提交，每批 ~100 文件） |

**【v9 新增】Meta 吞吐的扩展性说明**

Meta Raft 的写入吞吐是阶段 2+ 的主要扩展性上限。若业务峰值超过 10K commits/sec，演进路径为：

| 阶段 | 方案 | 说明 |
|---|---|---|
| 10K 以内 | 单 Raft Group | 批量提交 + 读靠 Query 缓存卸载 |
| 10K–100K | **按 shard 分片 Meta** | 不同 shard 的 Manifest 存不同 Raft Group，线性扩展 |
| >100K | 引入二级索引层 | 高频变更 Manifest 外置（如 DynamoDB），Raft 只管表结构与快照 |

**为何不在 v1 就分片**：分片会引入跨 shard 查询与分布式事务复杂度。单 Group 10K/sec 已能支撑**每日 8.6 亿次提交**，远超阶段 0–2 的目标规模。此为**预留路径**，非当前设计。

### 2.2 非目标

- ❌ 行级 UPDATE/DELETE（远期，依赖 Iceberg MoR）
- ❌ 分布式事务 / 跨表 ACID
- ❌ 亚秒级实时告警（可选扩展）
- ❌ 多租户强隔离（靠分区 + 应用层行过滤）

---

## 三、架构总览

### 3.1 逻辑分层

```
┌──────────────────────────────────────────────────────────────────┐
│                    接入层（可插拔 Source）                         │
│  【MVP】Arrow Flight (DoPut)                                      │
│  【阶段 2+】InfluxDB Line Protocol │ Kafka Source                  │
└────────────────────────────┬─────────────────────────────────────┘
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                        Ingestor（写入路径）                        │
│  协议适配 → Schema 解析/演进 → WAL(自实现) → 攒批 → 写 Vortex→S3 │
│                     ↓                                            │
│              BatchStateStore（批次状态追踪）                      │
│                     ↓                                            │
│              CommitFiles → Meta                                  │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│              Meta 服务（Catalog + 协调层，Raft / 单节点）           │
│  表元数据 │ Schema 版本链 │ 文件 Manifest │ 快照 │ 作业租约        │
└────────────────────────────┬─────────────────────────────────────┘
                             │ 变更通知
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                        Query（查询路径）                           │
│  LocalCatalogCache → LakeCatalogProvider → LakeTableProvider     │
│         ↓                                                        │
│  Schema 适配（PhysicalExprAdapter）+ 快照过滤 + 索引下推           │
│         ↓                                                        │
│  DataFusion 向量化执行                                            │
└──────────────────────────────────────────────────────────────────┘
```

### 3.2 Crate 边界（按状态切分，非按功能；【v12 更新】实际结构）

```
yuntun/
├── crates/
│   ├── yuntun-model/       # 核心数据模型（最底层，无依赖）
│   ├── yuntun-proto/       # 元数据 / WAL 消息（prost 手写；阶段 3 启用 tonic-build）
│   ├── yuntun-wal/         # 自实现 WAL（segment+CRC）+ 事件流重建 BatchState
│   ├── yuntun-store/       # 对象存储抽象（S3/MinIO/本地 FS）
│   ├── yuntun-format/      # Parquet（默认）/ Vortex（feature flag）+ Schema 适配
│   ├── yuntun-catalog/     # Catalog 纯逻辑（无网络）
│   ├── yuntun-ingest/       # 写入管线（RecordBatch → WAL → 攒批 → flush 状态机）
│   ├── yuntun-query/        # DataFusion 桥接（CatalogProvider/TableProvider）
│   ├── yuntun-compaction/  # 后台作业
│   ├── yuntun-server/      # 节点层：协议端口（Flight/FlightSQL）+ 装配 + 路由
│   ├── yuntun-chaos/       # 故障注入工具
│   ├── yuntun-standalone/  # ★ 单机二进制（bin 名 yuntun）—— v12：原 bins/all-in-one
│   └── yuntun-client/      # ★ Rust SDK + CLI（bin 名 yuntun-cli）—— v12 新增
└── docs/
```

> **v12 结构决策**：不再保留顶层 `bins/` 目录。每个可执行体独立成 crate
> （standalone / client），分布式阶段（计划书阶段 3）再增 `yuntun-meta` /
> `yuntun-ingestor` / `yuntun-queryd` / `yuntun-compactor`，全部复用同一组件，
> `standalone` 保留为全组件参考装配。

**依赖方向严格单向**：

```
standalone → server → ingest  → wal     → model
                    → query   → catalog → model
                    → catalog → store, format
                    → compaction → catalog, store, format
client → （仅依赖 arrow-flight，可独立编译，不依赖 server）
```

`catalog` 为纯逻辑 crate，不含网络代码 —— standalone 与分布式共用同一份逻辑的关键。

**协议端口归属（v12.3 澄清，简化版）**：

- **两条铁律**：
  1. **所有写入都走 ingest 管线**——它是唯一的数据写入事实（WAL 权威，禁绕过）；
  2. **协议端口统一在 server 装配层**——一个节点上的 server 可承载多类协议端口
     （Flight SQL、InfluxDB LP、未来 MySQL/PG wire），每个协议内部把
     **写路由到 ingest 能力、读路由到 query 能力**；域 crate（ingest / query）
     保持纯能力，不含协议、不含 Hook trait 间接层。
- `yuntun-server::flight::FlightServer` 直接持有 `Arc<Ingestor>` + `Arc<QueryEngine>`，
  实现 FlightService：FlightSQL 标准轨（读→query / 写→ingest）+ 简易读写轨。
- 阶段 3 分布式时节点按角色裁剪装配（如查询节点只接 query 能力 + 各协议端口的读路由），
  协议端口代码不随域拆分，天然可复用。

---

## 四、核心设计决策（ADR）

### ADR-1：DataFusion 查询内核，Vortex 主 / Parquet 辅

**代价**：Vortex 是文件格式非表格式，无 ACID、无行级删除；小批量（<1000 行）写入比 Parquet 慢 20–30×；库 API 在演进 → **锁定 Git Commit Hash** + Feature Flag 回退。

### ADR-2：不引入分布式 WAL

**代价**：见 ADR-9（SLA 分级后此代价变为可选）。

### ADR-3：多节点 Ingestor 互不感知

**理由**：Ingestor 的 WAL 是**本地独占**的（自实现 segment 文件，单进程访问）。与其自建跨节点复制协议，不如明确"节点独立"模型。

### ADR-4：batch_id 用随机 UUIDv7 + BatchStateStore 状态机

- `batch_id = UUIDv7`（随机，全局唯一，**不参与幂等判断**）
- 幂等由 **BatchStateStore** 保证
- 不引入 AllocateBatch（Meta 不维护"已分配 ID"，无孤儿记录）

**代价**：相同内容产生不同 ID → 需**客户端幂等键**（§7.3）在全局层去重。

### ADR-5：快照隔离统一删除语义

`snapshot_id + valid_from + deleted_at`，删除与 Compaction 共用。详见 §10。

### ADR-6：Catalog 本地缓存 + 变更通知

**理由**：DataFusion 的 `CatalogProvider::schema()` / `table()` 是**同步 API**，内部 `block_on` 发 gRPC 会阻塞 tokio 线程；每次查询走网络会让 Meta 承受 5 万次/秒压力；且可用性隔离（Meta 全挂时 Query 仍可查历史）。

### ADR-7：协调层复用 Meta Raft，不引入 etcd

### ADR-11（v10 新增）：WAL 自实现，不使用 fjall

**背景**：原设计用 fjall（LSM-tree KV）作本地 WAL。

**决策**：**自实现轻量 WAL** —— append-only segment 文件 + CRC + 组提交 fsync。`BatchState` 作为特殊 Record 类型写入**同一条 WAL 事件流**。

**理由**：WAL 实际需求仅"append + 顺序 replay"，fjall 的 MemTable / SSTable / Compaction / 随机读 / range query 全部用不上。多余能力即负担。自实现约 500 行，无黑盒、完全可控，且 Data 与 BatchState 在同一 append-only 流中——**顺序即因果**，原子性天然保证。

**代价**：需自行维护（约 500 行），并用阶段 0.5 专项压测覆盖撕裂写入、fsync 持久性、segment 清理正确性。

### ADR-12（v10 新增）：Meta 的 fjall 只存 raft 数据，Catalog 走内存 + snapshot

**背景**：需确定 Meta 服务如何用 fjall 存储。

**决策**：
- **fjall → raft 数据**（log entries + hard state），实现 `raft-rs::Storage`
- **Catalog（含 Schema）→ 内存**（权威），通过 **raft snapshot** 持久化

**理由**：Catalog 是 raft state machine，权威来源是 raft log（可重放）。若独立落 fjall 作权威，会出现**双写一致性陷阱**（raft apply 与 fjall fsync 不同步，崩溃后无法判断谁更新）。内存 + snapshot 保证**只有一份权威数据**。

**代价**：v1 Catalog 受内存限制。容量估算：100 万文件 ≈ 200MB；超 5000 万（~10GB）时引入 fjall CF 作**加速缓存**（恢复仍以 snapshot + log 为准）。

### ADR-13（v10 新增）：MVP 仅 Arrow Flight，但 `IngestSource` 抽象先行

**背景**：需收缩 MVP 范围。

**决策**：阶段 0 仅实现 Arrow Flight（`DoPut`）；但**从第一天就定义 `IngestSource` trait**，所有协议输出统一的 `IngestBatch`。

**理由**：若不做 trait 抽象，阶段 2+ 加 InfluxDB / Kafka 时需重构写入路径。trait 使下游（攒批 / WAL / S3 / Meta）完全不感知协议差异。

**代价**：多一层抽象（约 100 行），换来后续零重构接入新协议。

### ADR-8：Ingestor 不做显式 shard 路由（软路由优化局部性）

### ADR-9（v7 新增）：SLA 分级取代全局承诺

**背景**：评审 A 认为"无 WAL 可接受"，评审 B 认为"通用场景不可接受"。分歧源于"通用"定位的歧义。

**决策**：将持久性从**架构承诺**降级为**表级配置项**：

```rust
enum Durability {
    BestEffort,  // 默认：本地 WAL，节点磁盘故障=未提交数据丢失
    Durable,     // WAL 异步归档 S3，节点可从 S3 重建
}
```

| 模式 | 适用场景 | 写入路径 | RPO |
|---|---|---|---|
| `best_effort` | metrics / 可重放数据 | 本地 WAL → 攒批 → S3 | 秒级（攒批窗口） |
| `durable` | 审计日志 / 交易流水 | 本地 WAL **+ S3 WAL 归档** → 攒批 → S3 | ≈0 |

**关键**：默认轻量，按需开启。不为所有数据付 S3 归档成本，同时"通用"定位不再有歧义。

### ADR-10（v7 新增）：时间窗口对齐攒批 + 客户端软路由

**背景**：评审 B 指出无路由时 100 个 Ingestor 写同一 shard 会产生 100 个小文件，Meta 条目爆炸。

**决策**：

1. **时间窗口对齐**：所有 Ingestor 的攒批窗口按**整分钟对齐**（而非"达到阈值后 5 秒"）。同一时间窗口的文件数 ≤ Ingestor 节点数，时间上可预测。

2. **【v8 新增】Flush Jitter 削峰**：在整分钟对齐的基础上，为 flush 动作引入基于 hash 的随机抖动：

```
flush_moment = window_start + (hash(shard_id + table_name) % 60) seconds
```

**⚠️ 关键澄清**：Jitter 打散的是 **flush 动作发生的时刻**，**不改变** `time_window` 的数据归属。

- 数据仍按 `event_time` 归属 `14:00` 窗口
- 但各 shard 的 flush 分散在 `14:00:00` ~ `14:00:59` 之间的固定时刻

**为什么必需**：若不加 Jitter，100 个节点会在每分钟第 0 秒**同时** flush —— 同时触发 S3 Multipart Upload 与 `CommitFiles` gRPC，导致：
- Meta Raft Leader 承受周期性写入尖峰，可能触发选举超时（Leader 假死切换）
- S3 API 瞬时并发过高返回 `503 Slow Down`

**效果**：由于 `hash(shard)` 固定，同一 shard 的 flush 时刻**稳定可预测**；不同 shard 均匀分散到 60 秒，实现削峰，同时保持"每窗口每 shard 最多 1 个文件"的小文件控制目标。

3. **客户端软路由**：SDK 侧一致性哈希，**仅为优化局部性，不保证**。节点不可用时自动 fallback 到其他节点，**不阻塞写入**。

**与 ADR-8 的关系**：软路由是**优化**而非**约束**——任何时候任何节点仍可写任何数据，保持 ADR-3 的"互不感知"。

> 📌 **v8 修正说明**：v7 原设计仅有"整分钟对齐"，未意识到它与系统自身的削峰目标相冲突（100 节点同秒 flush 的惊群效应）。此为 v7 的设计缺陷，v8 通过 Jitter 修正，同时保持了 ADR-10 控制小文件的初衷不变。

---

## 五、数据模型

### 5.1 Catalog 元数据（Meta 持久化）

```protobuf
message TableMeta {
    string name = 1;
    uint64 current_schema_version = 2;  // 当前表 Schema 版本
    repeated string partition_cols = 3;
    string default_format = 4;          // "vortex" | "parquet"
    IngestConfig ingest_config = 5;
}

// 【v7 新增】Schema 版本链
message SchemaVersion {
    uint64 version = 1;                 // 单调递增
    bytes arrow_schema = 2;             // 该版本的完整 Arrow Schema
    SchemaChangeKind change_kind = 3;   // ADD_COLUMN / WIDEN_TYPE / DROP_COLUMN
    uint64 created_at = 4;
    string change_desc = 5;             // 人类可读，如 "add column user_agent: Utf8"
}

message FileManifest {
    string file_path = 1;
    string batch_id = 2;                // 幂等主键
    string client_request_id = 3;       // 【v7 新增】客户端幂等键，唯一索引
    uint64 schema_version = 4;          // 【v7 新增】该文件写入时的 Schema 版本
    FileStatus status = 5;
    uint64 valid_from = 6;
    uint64 deleted_at = 7;              // 0 = 未删除
    StatisticsLite stats = 8;           // 【v7 修改】精简统计，详见 §5.2
    uint64 row_count = 9;
    uint64 file_size = 10;
}
```

### 5.2（v7 修改）Meta 只存精简 Statistics

**问题**（评审 B 提出）：每个文件的完整统计（多列 NDV、直方图）会让 Raft Log/Snapshot 迅速膨胀。

**v7 决策**：

| 统计级别 | 存储位置 | 用途 |
|---|---|---|
| **精简统计**（min/max/null_count，仅排序列与分区列） | **Meta (Raft)** | 文件级剪枝、Catalog 缓存 |
| **完整统计**（所有列、直方图、NDV） | **Vortex footer**（文件自身） | 段级剪枝、精确过滤 |

**理由**：Vortex footer 本来就存完整统计，读文件时自然获得，无需额外的 S3 `.stats` 伴随文件（评审 B 原建议），避免剪枝时的额外 IO。

### 5.3 Ingestor 本地状态：自实现 WAL（【v10 重写】）

> **v10 决策**：不再使用 fjall，改为**自实现的轻量 WAL**。

#### 5.3.1 为什么放弃 fjall

fjall 是完整的 LSM-tree KV 引擎（MemTable + 多层 SSTable + Compaction + 随机读 + range query）。而我们的 WAL 实际需求只有：

- ✅ append-only 写入 RecordBatch
- ✅ fsync 持久化
- ✅ **顺序** replay（崩溃恢复）
- ✅ 已提交区间可清理

**用不上的能力**：MemTable / SSTable 多层 / Compaction / 按 key 随机读 / 复杂 range query。

| 维度 | fjall | **自实现 WAL（采纳）** |
|---|---|---|
| 依赖 | 外部 crate | **无，约 500 行** |
| 访问模式 | 通用 KV | **仅 append + 顺序 replay** |
| 原子性 | 需两 CF 共享 WAL | **单流天然原子** |
| 可控性 | 黑盒 | **完全可控、可针对性优化** |
| 清理 | 自动 Compaction | 按 batch 状态手动清理 |

**额外收益**：`BatchState` 可直接写入**同一条 WAL 流**（作为特殊记录类型），无需独立的 KV 存储——比原"两个 CF"方案更简单。

#### 5.3.2 文件布局

```
/var/lib/ingestor/wal/
  shard=0/
    00000000000000000001.wal    # segment（固定宽度序号）
    00000000000000000002.wal
    CURRENT                      # 活跃 segment 名（原子 rename 切换）
  shard=1/
    ...
```

**Shard 级独立 WAL**：不同 shard 写不同目录，避免单文件锁竞争与清理时的相互阻塞。

#### 5.3.3 Segment 与 Record 格式

```
Segment 文件:
  [FileHeader][Record][Record]...[Record]
  FileHeader: magic(4B) | version(2B) | shard_id(8B) | first_seq(8B)

Record:
  length(u32) | crc32(u32) | type(u8) | payload(variable)
   └ length = payload 字节数
   └ crc32  覆盖 type + payload（不含 length）
```

**CRC 位置的关键作用**：读取时先读 `length`，再读 `crc32 + type + payload` 并校验。**校验失败即判定为尾部撕裂写入（torn write），停止 replay**——这是崩溃恢复正确性的核心。

**Record 类型**（即状态机事件）：

| type | 名称 | payload |
|---|---|---|
| 0 | `Data` | 序列化的 Arrow RecordBatch（IPC） |
| 1 | `BatchPending` | `{batch_id, shard, window, wal_seq_range, schema_version, client_request_id}` |
| 2 | `BatchS3Written` | `{batch_id, s3_paths, s3_upload_id}` |
| 3 | `BatchCommitted` | `{batch_id}` |
| 4 | `BatchAbort` | `{batch_id}`（演进失败等场景，主动放弃） |

#### 5.3.4 BatchState：由 WAL 事件重建

**不再单独存储**，而是恢复时顺序重放 WAL 事件重建：

```rust
struct BatchState {
    batch_id: String,
    client_request_id: Option<String>,
    shard: String,
    time_window: String,
    status: BatchStatus,          // PENDING | S3_WRITTEN | COMMITTED
    s3_paths: Vec<String>,
    s3_upload_id: Option<String>,
    row_count: u64,
    wal_seq_range: (u64, u64),
    schema_version: u64,
    created_at: SystemTime,
}
```

**重放逻辑**：
```
for record in wal_replay():
    match record.type:
        Data            → 累积到 (shard, window) 的攒批缓冲
        BatchPending    → 插入 BatchState { status: PENDING }
        BatchS3Written  → 更新 BatchState { status: S3_WRITTEN, s3_paths, upload_id }
        BatchCommitted  → 更新 BatchState { status: COMMITTED }
        BatchAbort      → 移除 BatchState，丢弃其 Data 记录
```

**优势**：Data 与 BatchState 在同一 append-only 流中，**顺序即因果**，无需额外的原子性保证机制。

#### 5.3.5 fsync 策略与组提交

**WAL 语义要求**：客户端收到确认前，数据必须已 fsync。

```
写入请求 → 追加到内存 buffer
       → 加入当前 fsync 批次
       → [等待批次窗口：N 条 或 T 毫秒]
       → 一次性 write() + fsync()
       → 批次内所有请求一起返回确认
```

| 策略 | 吞吐 | 延迟 | 说明 |
|---|---|---|---|
| 每条 fsync | 低 | 低 | 最安全，fsync 开销大 |
| **组提交（采纳）** | **高** | 略高 | 窗口内合并 fsync，吞吐提升数倍 |

> ⚠️ 组提交**不降低持久性**——它只是把多条 fsync 合并成一次，确认仍在 fsync 之后返回。这与数据库 group commit 同理。

#### 5.3.5.1 【v11 新增】`synced_offset` 水位线（组提交的必要配套）

> 本节修复组提交引入的一个**真实正确性缺陷**（评审 B 补丁 3 发现）。

**问题**：组提交时，数据 `write()` 后进入 OS Page Cache，但**尚未 fsync**。若攒批线程此时读取了这部分未持久化的数据并生成 `BatchPending`：

```
① Data 写入 page cache（未 fsync）
② 攒批线程读到该 Data → 生成 BatchPending → 也写入 WAL
③ BatchPending 所在批次先 fsync 成功，Data 所在批次尚未 fsync
④ 断电 → Data 丢失，但 BatchPending 存在
⑤ 恢复：BatchPending 指向的 Data 区间不存在 → 状态机错乱
```

**决策**：引入 `synced_offset` 水位线，**攒批线程只读已 fsync 的数据**。

```rust
struct WalWriter {
    synced_offset: AtomicU64,   // 全局单调递增（跨 segment 累积的字节偏移）
    // ...
}

// 写入线程：仅在 fsync 成功返回后推进水位
fn on_fsync_complete(&self, new_offset: u64) {
    self.synced_offset.store(new_offset, Ordering::SeqCst);
    // 然后才通知等待中的请求返回客户端确认
}

// 攒批线程：严格只读 < synced_offset 的数据
fn read_for_batch(&self) -> impl Iterator<Item = Record> {
    self.scan_range(.., self.synced_offset.load(Ordering::SeqCst))
}
```

**关键约束**：`synced_offset` 的推进必须发生在 `fsync()` 返回之后、**且在返回客户端确认之前**。这保证"客户端确认 = 已 fsync = 攒批线程可见"三者一致。

**★ 优雅性质：`synced_offset` 不需要单独持久化**

恢复时 replay WAL，直到 **CRC 校验失败**即停止——那个位置就是 `synced_offset`。因为：

- fsync 成功的数据 → 一定完整写入 → **一定能通过 CRC**
- 未 fsync 的尾部 → 可能撕裂 → **CRC 必然失败**

即 **CRC 校验边界 = fsync 边界**，`synced_offset` 由 CRC 自然确定，无需额外的元数据文件或检查点。这是 §5.3.3 中"CRC 放在 length 之后"这一设计的**第二次收益**。

#### 5.3.6 Segment 轮转与清理

**轮转**（任一满足）：
- segment 大小 > 64MB
- 存在时间 > 1 小时
- 创建新 segment → fsync → **原子 rename 更新 `CURRENT`**

**【v11 修正】`CURRENT` 的原子切换**（评审 A 建议 1，采纳）

原子 rename 的正确姿势（**先写临时文件，再 rename**）：

```rust
// ✅ 正确
let tmp = wal_dir.join("CURRENT.tmp");
fs::write(&tmp, new_segment_name.as_bytes())?;
fs::File::open(&tmp)?.sync_all()?;      // 文件 fsync
fs::File::open(wal_dir)?.sync_all()?;   // 【关键】目录 fsync，确保元数据落盘
fs::rename(&tmp, wal_dir.join("CURRENT"))?;  // 原子替换

// ❌ 错误：直接 write CURRENT（非原子，崩溃后可能半截内容）
fs::write(wal_dir.join("CURRENT"), new_segment_name.as_bytes())?;
```

> **目录 fsync 不可省略**：`rename` 的原子性由文件系统保证，但"新文件名已写入目录项"这一元数据需 `fsync(dir)` 才持久化。省略则崩溃后可能回到旧 `CURRENT`。

**清理**：基于 batch **终态**的安全删除

```rust
// 恢复时构建映射
segment_batches: HashMap<SegmentId, Set<BatchId>>

fn is_terminal(status: BatchStatus) -> bool {
    matches!(status, COMMITTED | ABORT)   // 【v11 修正】终态包含 ABORT
}

// 后台清理线程
for (seg_id, batch_ids) in segment_batches {
    if seg_id == current_segment { continue; }
    if batch_ids.iter().all(|b| is_terminal(state[b].status)) {
        fs::remove_file(seg_id.to_path())?;
    }
}
```

**安全性论证**：`COMMITTED` 意味着 Meta 已记录且 S3 文件存在；`ABORT` 意味着该批次已明确放弃（见下）。二者都表示"其 Data 记录不再需要" → 可安全删除。

#### 5.3.6.1 【v11 新增】Batch 超时机制（修复 Segment 泄漏）

> 本节修复一个 **P0 级磁盘写满风险**（评审 B 补丁 1 发现）。

**问题**：清理条件要求所有 batch 达到终态。若某 batch 因 **S3 凭证过期、网络永久隔离、代码 Bug** 等原因**永远卡在 `PENDING` / `S3_WRITTEN`**，则包含它的 segment 永不释放 → **WAL 无限增长 → 磁盘写满 → Ingestor 宕机**。

**决策**：引入**分级超时**，确保任何 batch 最终都会进入终态。

| 级别 | 触发条件 | 动作 | 默认值 |
|---|---|---|---|
| **批次级超时** | 处于非终态超过 `batch_timeout` | 记录 `BatchAbort` → 进入终态 | **30 分钟** |
| **磁盘保护** | WAL 磁盘使用率 > `high_watermark` | **强制** abort 最老的未完成 batch | **80%** |

```rust
// 后台监控线程
async fn batch_timeout_monitor() {
    loop {
        sleep(Duration::from_secs(60)).await;

        // ① 批次级超时
        for (id, st) in state.iter() {
            if !is_terminal(st.status)
               && st.created_at.elapsed() > config.batch_timeout {
                wal.append(Record::BatchAbort { batch_id: id.clone() })?;
                wal.fsync()?;
                // 该 batch 若已写 S3 → 其文件成为孤儿，由 §12.2 孤儿清理回收
            }
        }

        // ② 磁盘保护（更激进）
        if wal_disk_usage() > config.high_watermark {
            let oldest = state.iter()
                .filter(|(_, st)| !is_terminal(st.status))
                .min_by_key(|(_, st)| st.created_at);
            if let Some((id, _)) = oldest { force_abort(id)?; }
        }
    }
}
```

**关键权衡 —— 超时阈值怎么定**：

| 阈值 | 风险 |
|---|---|
| 太短（如 1 分钟） | 正常的慢写入（S3 抖动、大文件 Multipart）被**误 abort**，造成不必要的数据丢失 |
| 太长（如 24 小时） | 期间 WAL 持续累积，**可能在超时前就写满磁盘** |

**v11 取值**：批次级 30 分钟（容忍 S3 抖动与重试），配合 **80% 磁盘水位强制 abort** 作为兜底。二者结合既避免误 abort，又确保磁盘不会写满。

**⚠️ 必须明确的语义**：`BatchAbort` 意味着**该批次数据被明确放弃**（数据丢失）。这是 `best_effort` SLA 模式（§4 ADR-9）下已接受的取舍；使用 `durable` 模式的表应从 S3 WAL 归档重建，而非 abort。

**与幂等键的配合**：abort 后客户端重试同一 `client_request_id` → Meta 无该记录（从未 Commit）→ 视为新请求 → **不会误判为重复**。逻辑自洽。

#### 5.3.7 与 fjall 方案的对比总结

自实现 WAL 在本场景下**更简单、更可控、无外部依赖**，且天然满足"Data 与 BatchState 原子持久化"的需求。

---

### 5.4 Meta 服务存储设计（【v10 新增】）

> 用户提问："用 fjall 做数据存储，raft 数据还是 schema 数据？"
> **答案：fjall 存 raft 数据；Catalog（含 schema）是 raft state machine，放内存 + snapshot。**

#### 5.4.1 两类数据的本质区分

| 数据类型 | 内容 | 性质 | 持久化方式 |
|---|---|---|---|
| **Raft 数据** | log entries、hard state（term/vote/commit）、conf state | 共识层基础设施 | **✅ fjall** |
| **Catalog 数据** | TableMeta、SchemaVersion、FileManifest、ShardMeta、IdempotencyRecord、ExternalIndexRef | **raft state machine 的物化结果** | **内存 + raft snapshot** |

#### 5.4.2 为什么 Catalog 不能直接落 fjall 当权威数据（一致性陷阱）

**问题**：若 Catalog 独立落 fjall 并作为权威数据源：

```
raft apply log index N  →  更新内存 Catalog
                        →  写 fjall（异步 fsync）
                        →  崩溃
```

此时 raft log 已 apply 到 N，但 fjall 可能只持久化到 N-3。恢复时**无法判断哪个更新** —— 这是典型的双写一致性陷阱。

#### 5.4.3.1 【v11 新增】Snapshot 必须异步生成（避免 Leader Flapping）

> 本节修复一个 **Raft 稳定性风险**（评审 B 补丁 2 发现方向正确，但实现方式需修正）。

**问题**：Catalog 约 200MB（100 万文件）。若在 raft apply 线程中**同步序列化**，会阻塞状态机几十~上百毫秒 → **Raft 心跳超时 → 不必要的 Leader 切换（flapping）**。

**⚠️ 修正评审的实现建议**：评审说"快速克隆 `Arc` 指针，派发给后台线程序列化"。**这在 Rust 中并不安全**：

```
Arc 克隆只是共享同一份数据，后台线程序列化期间 Catalog 仍在被 apply 线程修改
→ 序列化过程中读到"半新半旧"的状态 → snapshot 内容不是一致点
```

**v11 正确做法**：关键是**在持锁瞬间取出不可变快照**，而非克隆指针。

| 方案 | 取快照开销 | 说明 |
|---|---|---|
| 深拷贝 + 持读锁 | O(n)，200MB 拷贝阻塞写 | ❌ 仍有 flapping 风险 |
| **持久化数据结构（推荐）** | **O(1)** | 用 `im::HashMap` 等，克隆即不可变快照 |
| **版本号 + 不可变节点** | **O(1)** | 自行实现 COW |

```rust
struct CatalogState {
    // 持久化数据结构：克隆是 O(1)，且克隆后与原数据隔离
    inner: im::HashMap<String, TableMeta>,
    files: im::OrdMap<FileKey, FileManifest>,
    // ...
}

impl raft::Storage for MetaStorage {
    fn snapshot(&self, request_index: u64) -> Result<Snapshot> {
        // ① 取读锁（极短）
        let guard = self.state.read();
        // ② O(1) 克隆出不可变快照（持久化数据结构保证）
        let snap: CatalogState = guard.clone();
        let last_applied = guard.last_applied_index;
        drop(guard);                      // ③ 立即释放锁
        // ④ 后台阻塞线程池做序列化，不占用 apply 线程
        let handle = tokio::task::spawn_blocking(move || {
            serialize_catalog(&snap, last_applied)
        });
        // ⑤ 通过 channel 交还 raft 层
        Ok(Snapshot::new_async(handle))
    }
}
```

**关键点**：
- 锁持有时间只有"克隆 O(1) 指针"这一瞬，**apply 线程几乎不被阻塞**
- 序列化在 `spawn_blocking` 中进行，且作用于**不可变快照**，无数据竞争
- snapshot 内容与 `last_applied_index` 严格对应，是**一致的状态点**

**验证要求**：阶段 1.5 压测增加"Snapshot 生成期间的 Leader 稳定性"场景——在持续写入下强制触发 snapshot，验证无 Leader 切换。

**正确做法**：Catalog 的权威来源是 **raft log（可重放）**，因此：

- **Catalog 放内存**（权威）
- 通过 **raft snapshot**（定期全量序列化）持久化
- 恢复时：加载最新 snapshot → 重放后续 raft log → 得到一致状态

**只有一份权威数据，无一致性歧义。**

#### 5.4.3 fjall 实现 raft-rs `Storage` trait

```rust
impl Storage for FjallRaftStorage {
    fn initial_state(&self) -> Result<RaftState>;              // hard state + conf state
    fn entries(&self, low: u64, high: u64, max_size: u64) -> Result<Vec<Entry>>;
    fn term(&self, idx: u64) -> Result<u64>;
    fn first_index(&self) -> Result<u64>;                      // compact 后的最小 index
    fn last_index(&self) -> Result<u64>;
    fn snapshot(&self, request_index: u64) -> Result<Snapshot>;
}
```

**Key 设计**：`index` 用**大端序 u64** 编码 → LSM-tree 中按 index 物理有序，`entries(low, high)` 的 range query 高效顺序扫描。

fjall 相比 RocksDB 的优势：**纯 Rust，无 C++ 工具链**，交叉编译友好。

#### 5.4.4 Snapshot 与 Log Compaction

```
定期（log 条数 > 阈值 或 时间间隔）：
  ① 序列化内存 Catalog 全量 → snapshot 文件
  ② 记录 snapshot 对应的 last_applied_index
  ③ 通知 raft compact：删除 index <= last_applied 的 log entries
  ④ 从 fjall 中删除对应 entries
```

#### 5.4.5 容量估算与演进路径

| 阶段 | 文件数 | Catalog 内存占用 | 方案 |
|---|---|---|---|
| 阶段 0–2 | 100 万 | ~200 MB（每条 ~200B） | ✅ **纯内存 + snapshot** |
| 规模化 | 5000 万 | ~10 GB | ⚠️ 引入 fjall CF "catalog" |
| 超大规模 | >1 亿 | >20 GB | 按 shard 分片 Meta（§2.1） |

**规模化时引入 fjall catalog 的正确姿势**：

> 即便引入 fjall 存 Catalog，**恢复逻辑仍必须以 raft snapshot + log 为准**，fjall catalog 仅作为"加速加载的缓存"，不作为权威来源。

这样可彻底规避双写一致性问题。

#### 5.4.6 Ingestor 侧已移除 fjall

注意：**fjall 仅用于 Meta 服务**。Ingestor 侧已改为自实现 WAL（§5.3），不再依赖 fjall。

| 组件 | fjall 使用 | 用途 |
|---|---|---|
| **Ingestor** | ❌ 不使用 | 自实现 WAL（§5.3） |
| **Meta** | ✅ 使用 | raft log + hard state（§5.4.3） |

---

## 六、Schema 演进设计

> **v7 新增章节**。这是通用数据湖的硬骨头：InfluxDB 等无 schema 输入 + 客户端持续加字段，必然导致同一表内存在多种 schema 的文件。

### 6.1 问题本质

**关键事实**：Vortex / Parquet 的**每个文件 footer 自带完整 Arrow Schema**。文件格式层**天然允许**同一表的不同文件有不同 schema。

矛盾全部集中在**查询层**：DataFusion 拿到 N 个 schema 不同的文件，如何拼成一张逻辑表？

### 6.2 三层 Schema 模型

| 层级 | 位置 | 生命周期 | 作用 |
|---|---|---|---|
| **物理 Schema** | 每个 Vortex 文件 footer | 文件生命周期 | 描述该文件真实存储结构 |
| **表 Schema** | Catalog（`current_schema_version`） | 表生命周期 | 表对外的最新契约 |
| **查询 Schema** | Query 运行时构造 | 单次查询 | 本次查询涉及的文件的 schema 并集 |

```
物理 Schema (V1) ─┐
物理 Schema (V2) ─┼─→ 查询 Schema = union(V1, V2, V3)
物理 Schema (V3) ─┘        ↓
                    表 Schema (V3, current)
```

### 6.3 变更类型与处理策略

| 变更类型 | 兼容性 | 处理策略 | 理由 |
|---|---|---|---|
| **加列** | ✅ 向后兼容 | **自动演进**：更新表 schema，老文件该列读为 `null` | 无风险，最常见 |
| **列类型宽化**（Int32→Int64, Int→Float64） | ✅ 安全 | **自动演进** | 无损转换 |
| **列类型窄化**（Float64→Int64） | ⚠️ 可能丢精度 | **拒绝**，需显式 DDL + `ALTER TABLE ... TYPE` | 防止静默数据损坏 |
| **删列** | ✅ 逻辑兼容 | **逻辑删除**：标记 `hidden`，物理文件不变，查询不返回 | 保留可恢复性 |
| **改列名** | ⚠️ | 视为**删+加**组合 | 语义清晰 |
| **NOT NULL → NULL** | ✅ 安全 | 自动演进 | 放宽约束 |
| **NULL → NOT NULL** | ⚠️ | **拒绝**（老数据可能含 null） | 防止约束违反 |

### 6.4 类型提升格（Type Promotion Lattice）

当同一列在不同文件中有不同类型时，用**最小公共上界**统一：

```
        Float64
       /       \
    Int64     Utf8
      |         |
    Int32    LargeUtf8
      |
    Int16
      |
    Int8
```

**提升规则**：
- 数值型：向更宽的数值类型提升（Int8 → Int32 → Int64 → Float64）
- 数值 vs 字符串：**默认拒绝**（歧义太大），除非表配置 `coerce_mixed_types = true`（此时统一为 Utf8）
- Boolean 不参与提升（与数值/字符串均不兼容）

### 6.5 【关键】查询层的两级适配

DataFusion 提供两个 Schema 适配扩展点，**选择错误会导致查询性能灾难性退化**：

| 机制 | 作用层 | 工作流程 | 性能影响 |
|---|---|---|---|
| `SchemaAdapter` | **RecordBatch 级** | 按文件 schema 读出 → cast 成表 schema | ❌ **谓词无法下推**，全量读再过滤 |
| `PhysicalExprAdapter` | **表达式级** | 把表 schema 的谓词**改写**成文件 schema 的谓词 | ✅ **谓词可下推**，剪枝生效 |

**v7 决策：必须使用 `PhysicalExprAdapter`。**

**工作原理**：

```
用户查询：SELECT * FROM t WHERE user = 'admin' AND ts > '2026-08-28'
         （表 schema V3：user:Utf8, ts:Timestamp, extra_col:Utf8）

文件 A（V1: user:Utf8, ts:Timestamp）
   → PhysicalExprAdapter 改写谓词：user = 'admin' AND ts > '...'  （无需改写）
   → 下推到 Vortex，文件级剪枝生效

文件 B（V2: user:Utf8, ts:Int64）  ← ts 类型不同！
   → PhysicalExprAdapter 改写谓词：user = 'admin' AND ts > 1756339200
   → 下推到 Vortex，文件级剪枝仍生效

文件 C（V3: user:Utf8, ts:Timestamp, extra_col:Utf8）
   → 谓词直接下推
```

**如果只用 SchemaAdapter**：所有文件先全量读出（含不满足条件的行），cast 成 V3，再过滤 —— **Sort Pushdown 与文件级剪枝全部失效**。

### 6.6 InfluxDB 映射：类型策略 + Tag/Field 语义

**问题**：InfluxDB 线协议有两个层面的语义需要映射：

1. **数值类型**：`cpu value=42`（Int）与 `cpu value=42.5`（Float）可在不同时间点出现，与 Arrow 静态 schema 冲突
2. **【v8 新增】Tag vs Field 语义**：InfluxDB 的核心语义区分是
   - **Tags**：低基数、用于分组与索引的字符串（如 `host`, `region`）
   - **Fields**：高基数、实际存储的数值（如 `value`, `usage`）
   
   若统一映射为普通 Arrow 列，Query 层无法区分哪些列适合 `GROUP BY`、哪些该建索引

**v8 决策**：分两个配置项

**（1）数值类型映射** `influx_type_mapping`

| 策略 | 映射 | 优点 | 缺点 |
|---|---|---|---|
| **`Float64`（默认）** | 所有数值 Field → `Float64` | 无损覆盖 Int/Float，查询无需转换 | 整数存储略冗余 |
| **`String`** | 所有数值 Field → `LargeUtf8` | 完全保真，永不冲突 | 查询需 `CAST`，性能下降 |
| **`Strict`** | 首次出现类型锁定 | 存储最紧凑 | 类型变化时写入失败 |

默认 `Float64`：整数在 Float64 中可精确表示到 2^53，对绝大多数场景无损。

**（2）【v8 新增】Tag / Field 列映射** `influx_tag_mapping`

| InfluxDB 语义 | Arrow 类型 | 索引策略 | 理由 |
|---|---|---|---|
| **Tag** | `Dictionary<UInt32, Utf8>` | **自动建 MinMax + Bloom** | 低基数，是高选择性过滤与 `GROUP BY` 的主要维度 |
| **Field** | `Float64` / 按 type_mapping | 仅 MinMax（Vortex footer 自带） | 高基数，建 Bloom 收益低、成本高 |

**为什么 Tag 用 Dictionary**：Vortex 对低基数列有字典编码优化，使用 `Dictionary<UInt32, Utf8>` 与 Vortex 原生能力契合；同时低基数列的 Bloom Filter 选择性好，能显著提升文件级剪枝率。

### 6.7 Schema 演进与写入路径

```
写入到达 → 解析出本次写入的 schema
         ↓
    与表 current_schema 比对
         ↓
    ┌────┴────┬─────────────┬──────────────┐
    │完全匹配  │可自动演进    │不兼容         │
    ↓         ↓             ↓
  直接写入   ① 发起 EvolveSchema  ① strict 模式：拒绝
            ② 版本号 +1          ② evolve 模式：按类型提升格
            ③ 写入，文件记录       ③ permissive：Utf8 兜底
              schema_version
```

**【v8 新增】Schema 演进的乐观并发控制（OCC）**

**问题**：两个 Ingestor（A、B）同时收到含新字段的数据，各自基于本地缓存的旧 schema 判定"需要加列"，并发发起演进 → **两个不同的 schema 都申请 version N**，产生版本冲突。

**决策**：乐观锁**仅作用于 `EvolveSchema` 请求**，不作用于 `CommitFiles`。

```protobuf
message EvolveSchemaRequest {
    string table = 1;
    SchemaChange change = 2;      // 期望的变更（加列/宽化）
    uint64 expected_version = 3;  // 【乐观锁】Ingestor 本地缓存的版本
}

// Meta 侧
fn evolve_schema(req) -> Result<EvolveResponse> {
    if table.current_version != req.expected_version {
        return Err(SCHEMA_CHANGED { 
            actual_version: table.current_version,
            new_schema: table.current_schema,
        });
    }
    // 版本匹配 → 应用变更，version += 1
}
```

**【v9 新增】写入路径的严格时序**（澄清评审 B 风险 4 的场景）

```
写入到达
  ↓
① 解析出本次写入的 schema
  ↓
② 与本地缓存的表 schema 比对
  ↓
③ 判定是否需要演进
  ├─ 不需要 → 直接走 ⑤
  └─ 需要   → ④ 发起 EvolveSchema（OCC）
  ↓
④ EvolveSchema
  ├─ 成功 → schema version +1，走 ⑤
  └─ 失败（SCHEMA_CHANGED）→ 拉取新 schema → 回到 ② 重新判定
  ↓
⑤ 编码为 Vortex → 写 S3 → 生成 batch_id
  ↓
⑥ CommitFiles（携带本次实际使用的 schema_version）
```

**关键约束：`EvolveSchema` 必须在写 S3 之前完成。** 这保证不会出现"已写 S3 但 schema 冲突"的中间态。

**Ingestor 收到 `SCHEMA_CHANGED` 后的处理**：
1. 不重试写入（数据仍在 WAL，未丢失）
2. 拉取新 schema，重新解析/转换内存中的 RecordBatch
3. 重新判定（新 schema 下可能已无需演进，或需演进到更高版本）
4. 走 ⑤ 正常写入

**【v9 澄清】不要丢弃已用旧 schema 写入的文件**

评审 B 风险 4 建议"收到 `SCHEMA_CHANGED` 后必须丢弃已写入的 S3 文件"。**此建议过于激进，不予采纳**，理由：

- 在严格时序下（④ 在 ⑤ 之前），正常情况下不存在"已写 S3 才发现冲突"的场景
- 即便发生，**用旧 schema 写入的文件是完全合法的**（§6.7 关键澄清：同一表不同文件本就可以有不同 `schema_version`）
- 丢弃会浪费已完成的 S3 写入，并产生额外的孤儿文件清理负担
- 查询层用 `PhysicalExprAdapter` 适配即可，**schema 碎片是设计允许的常态，不是错误**

**正确的取舍**：schema 碎片由 **Compaction 收敛**（§6.8），而非在写入路径上通过丢弃文件来避免。写入路径只负责正确性，不负责优化碎片率。

**关键澄清 —— 为什么 `CommitFiles` 不需要乐观锁**：

v7 的设计中，**每个文件在 `FileManifest` 中记录自己的 `schema_version`**（§5.1），查询层用 `PhysicalExprAdapter` 做跨版本适配。这意味着：

> **同一表的不同文件，本来就可以有不同的 `schema_version`** —— 这是设计允许的常态，不是冲突。

因此，Ingestor 用旧 schema 写入的文件**完全合法**，Meta 应当接受。若按部分评审建议"每次 Commit 都校验 schema version"，会导致 schema 变更瞬间，大量**合法**的老 schema 写入被无谓拒绝，反而损害吞吐。

**真正的竞态只在"并发演进"这一动作上**，OCC 只需守住这一点。

### 6.8 Schema 演进与 Compaction

**Compaction 是收敛 Schema 碎片的自然时机**：

合并不同 schema 的文件时，用**目标 schema** 重新编码：

- 缺失列 → 填 `null`
- 类型不同 → 按类型提升格 cast 到目标类型
- 多余列（已逻辑删除）→ **按 §6.8.1 的安全约束丢弃**

这样 Compaction 后，历史文件逐步收敛到统一 schema，**减少查询期的适配开销**。

**【v9 新增】Compaction Schema Snapshot 机制**（评审 B 风险 2）

**问题**：Compaction 是长耗时作业（分钟~小时级），期间表 schema 可能继续演进：

```
T0  Compaction 启动，读取文件 A(V1) + B(V2)，计划用当前表 schema V3 重新编码
T1  用户执行 ALTER TABLE，表 schema 演进到 V4
T2  Compaction 完成，写入新文件 C
    → 若用"当前 schema"编码，C 的 schema_version 是不确定的（V3 还是 V4？）
```

**决策**：Compaction **启动时锁定 schema 版本**，整个作业期间不再变化。

```rust
struct CompactionJob {
    target_files: Vec<FileManifest>,
    compaction_schema_version: u64,  // 【关键】启动时锁定
    compaction_schema: SchemaRef,
    lease_id: String,
}

// Compaction 完成时
fn commit_compaction(job: &CompactionJob) {
    // 新文件的 schema_version = job.compaction_schema_version
    // 而非 current_schema_version（可能已在作业期间演进）
}
```

**收益**：
- 新文件的 `schema_version` **确定性**，与"当前 schema"解耦
- 即使作业期间 schema 继续演进到 V4，新文件仍是 V3 —— **完全合法**，查询层用 `PhysicalExprAdapter` 适配
- 作业可复现：同样的输入文件 + 同样的锁定版本 = 同样的输出

> 📌 这不是"追上最新 schema"的机制，而是**保证合并结果确定性**的机制。Schema 收敛是概率性的（每次 Compaction 都会减少碎片），不需要精确同步到最新版本。

**【v8 新增】6.8.1 丢弃逻辑删除列的安全约束**

**问题**：§6.3 规定"删列 = 逻辑删除，物理文件不变"；若 Compaction 立即物理丢弃该列，则"撤销删列"将无法恢复数据。

**决策**：Compaction 物理丢弃列前，必须检查该列的 `deleted_at` 快照是否已**超过 24h 物理清理窗口**（与 §10.3 统一）：

```
if column.deleted_at is not None
   and now() - column.deleted_at > 24h:
    物理丢弃该列
else:
    保留（仍占用存储，但支持撤销删列）
```

**理由**：与 §10.3"物理清理窗口统一 24h"保持**同一套机制**，不引入新的租约或状态。用户在该窗口内撤销删列，数据可完整恢复；超过窗口则彻底清除。

> 📌 说明：Time Travel（历史快照查询）在 v7/v8 中均为**非目标**（§2.2），故此约束的目的是**支持"撤销删列"的可恢复性**，而非 Time Travel。

---

## 七、写入路径

### 7.1 六个阶段（【v10 更新】WAL 改为自实现）

```
① 客户端写入（Arrow Flight DoPut）→ Ingestor
     解析 schema（见 §6.7）
     → append WAL Record(type=Data)          # §5.3 自实现 WAL
     → 组提交 fsync                           # §5.3.5
     → 返回确认（数据在 WAL，崩溃可恢复）

② 后台攒批触发（行数/时间阈值，任一满足即触发；整分钟对齐 + Jitter）
     从 WAL 读区间 → 反序列化 → 合并 → 排序 → 分组

③ 生成 batch_id
     batch_id = Uuid7::new()
     append WAL Record(type=BatchPending, {batch_id, wal_seq_range, ...})
     → fsync

④ 写 S3
     Vortex 编码 → S3（大文件用 Multipart，持久化 upload_id）
     append WAL Record(type=BatchS3Written, {batch_id, s3_paths, upload_id})
     → fsync

⑤ 提交 Meta
     CommitFiles(batch_id, client_request_id, s3_paths, schema_version, ...)
     append WAL Record(type=BatchCommitted, {batch_id})
     → fsync

⑥ 清理
     WAL segment 在其所有 batch 均 COMMITTED 后可删（§5.3.6）
```

**关键变化**：原"写 BatchStateStore"改为 **append 对应类型的 WAL Record**。
`BatchState` 不再单独存储，而是由 WAL 事件流**重建**（§5.3.4）。

### 7.1.1 【v10 新增】Source 可插拔抽象（为后续协议预留）

MVP 仅实现 Arrow Flight，但 **trait 抽象从第一天就设计好**，避免阶段 2+ 加协议时重构：

```rust
#[async_trait]
pub trait IngestSource: Send + Sync {
    /// 协议类型标识
    fn name(&self) -> &str;

    /// 启动服务，产出归一化的 RecordBatch + 可选幂等键
    async fn run(
        &self,
        tx: mpsc::Sender<IngestBatch>,
        shutdown: CancellationToken,
    ) -> Result<()>;
}

/// 归一化的写入单元（与具体协议无关）
pub struct IngestBatch {
    pub table: String,
    pub shard_key: String,
    pub record_batch: RecordBatch,
    pub idempotency_key: Option<String>,   // §7.3
    pub received_at: SystemTime,
}
```

**实现路线**：

| 阶段 | Source 实现 |
|---|---|
| **阶段 0（MVP）** | `ArrowFlightSource`（`DoPut`） |
| 阶段 2+ | `InfluxLineProtocolSource` |
| 阶段 2+ | `KafkaSource`（消费者，非 WAL） |

**设计要点**：所有 Source 输出统一的 `IngestBatch`，**下游攒批、WAL、S3 写入逻辑完全不感知协议差异**。这是"先只做 Arrow Flight"却不牺牲扩展性的关键。

### 7.1.2 【v10 更新】InfluxDB 映射延后说明

§6.6 的 InfluxDB 类型 / Tag-Field 映射设计**保留**，但实现延后至阶段 2+（接入 InfluxDB 线协议时生效）。

对 MVP（纯 Arrow Flight）而言：
- **Schema 由客户端的 Arrow Schema 直接决定**（Flight 传输自带 schema）
- 无"无类型输入"问题，§6.6 的 `influx_type_mapping` 暂不生效
- **但 Schema 演进机制（§6）完全适用** —— 不同 Flight 客户端可能发来不同 schema，仍需类型提升格与 `PhysicalExprAdapter` 适配

### 7.2 攒批阈值（按表可配）

| 数据源类型 | 行数阈值 | 时间阈值 |
|---|---|---|
| 高吞吐（metrics/traces） | 100,000 | 10s |
| 中吞吐（通用） | 10,000 | 5s |
| 低吞吐（告警/审计） | 1,000 | 30s |

**明确语义**：行数与时间阈值**任一满足即触发**。

**【v7 新增】绝对空闲超时兜底**（评审 A 建议）：增加**绝对最大滞留时间**（如 5 分钟）。即使某个 source 每秒只有 1 条、永远达不到行数阈值，也会因时间阈值触发 flush；若时间阈值因某种原因未触发（如定时器 bug），5 分钟兜底强制 flush，防止数据无限期滞留。

### 7.3 【v7 新增】幂等键三层防护

**问题**（评审 A P0 发现，本次评审最大贡献）：

> 客户端写 Ingestor-A，A 写 WAL 成功但在返回 200 前**网络超时**。客户端重试到 Ingestor-B。
> B 生成新 UUID → 写 S3 → Commit 成功。
> A 崩溃恢复线程读取自己 WAL → 生成另一个 UUID → 也写 S3 → 也 Commit。
> **结果：Meta 中两份相同内容的文件，查询数据翻倍。**

**关键洞察**：v6 的 BatchStateStore 幂等**仅覆盖"同一进程内崩溃重启"**，无法防御"客户端重试到另一节点"。

**v7 三层防护**：

```
① 客户端幂等键 → Meta 层 client_request_id 唯一索引（全局去重，防重复提交）
② 客户端软路由 → 重试优先到同一节点（WAL 层直接去重，避免重复写 S3）
③ 孤儿清理    → 未被 Meta 接受的文件延迟回收（兜底）
```

**协议设计**：

```protobuf
message WriteRequest {
    string idempotency_key = 1;  // 客户端生成（如 UUIDv4 或 source_${ts}_${seq}）
    bytes record_batch = 2;
}
```

通过 Arrow Flight 的 **FlightDescriptor / 自定义 metadata** 或 HTTP Header 传递。

**Meta 侧去重**：

```rust
fn commit_files(req: CommitFilesRequest) -> CommitResponse {
    // client_request_id 作唯一索引
    match db.insert_if_not_exists("client_requests", &req.client_request_id, &req.batch_id) {
        Ok(()) => CommitResponse { accepted: true, .. },
        Err(AlreadyExists) => CommitResponse {
            accepted: false,
            duplicate: true,
            existing_batch_id: Some(db.get(...)),
        },
    }
}
```

**⚠️ 修正评审 A 的建议**：评审 A 主推"用幂等键做 UUID 生成种子（`Uuid::from_name`）"。**我选用 Meta 唯一索引方案**，理由：

1. `Uuid::from_name` 生成的是 **UUIDv5**，丢失 UUIDv7 的时间有序性（S3 对象按时间聚拢的好处消失）
2. 更关键：客户端重试到**不同节点 B** 时，B 的 WAL 里没有这个 key，**幂等键在 WAL 层无效**，必须在全局层（Meta）去重

### 7.3.1 【v8 新增】幂等键记录的存储设计（两处关键修正）

两轮评审均指出幂等键需 TTL 以防 Raft 状态无限膨胀（共识，采纳）。但**两轮评审均未发现一个更致命的问题：幂等键记录的生命周期与 `FileManifest` 耦合**。

#### 修正 1：幂等记录必须独立存储，与 FileManifest 解耦

**问题推演**（v7 的隐含缺陷）：

```
① 客户端写入，client_request_id = K，Commit 成功
   → v7 设计：K 记录在 FileManifest A 中

② 数小时后，Compaction 合并文件 A → A 被标记 deleted_at
   → 最终 FileManifest A 被物理删除

③ 此时客户端用同一个 K 重试
   → Meta 查询 K，发现 FileManifest A 已不存在 → 视为新请求 → 接受
   → 数据重复！
```

**Compaction 可能在数小时内发生，远早于任何合理的重试窗口结束。** 若幂等键依附于 `FileManifest`，其生命周期完全不可控。

**v8 决策**：幂等键存储在**独立的 `idempotency_records` 表**，拥有自己的 TTL，与 `FileManifest` 的生命周期完全无关：

```protobuf
message IdempotencyRecord {
    string client_request_id = 1;  // 主键
    string batch_id = 2;           // 首次成功提交对应的 batch
    uint64 committed_at = 3;       // 用于 TTL 清理
    // 注意：不引用 FileManifest，不随文件删除而删除
}
```

#### 修正 2：TTL 策略与语义边界

| 项目 | 决策 |
|---|---|
| **TTL 时长** | **24 小时**（自 `committed_at` 起算） |
| **清理方式** | 后台定时任务 / Raft 状态机内部过期清理 |
| **覆盖范围** | 网络超时、节点重启、短时离线重试（秒级到小时级）远超足够 |

**⚠️ 必须明确的语义边界**（两轮评审均未点明）：

> **幂等保证仅在 TTL 窗口内有效。** 超过 24h 后，同一个 `client_request_id` 的重试将被视为全新请求，产生重复数据。

这是有意接受的取舍：幂等键用于防御**短时重放**（网络抖动、进程重启、短暂离线），而非永久记录所有请求历史。若业务需要更长的幂等窗口，可按表配置 `idempotency_ttl`。

#### 存储量估算

假设 100 万次提交/秒（极端高估），24h TTL：

```
1e6 × 86400 = 8.64e10 条记录
```

显然不可行 —— 因此**幂等键仅对需要严格幂等的表开启**（表级配置 `require_idempotency_key`）。对高吞吐的 metrics 类数据，可关闭幂等键（接受极低概率的重复），由 Compaction 或查询层去重兜底。

### 7.3.2 【v9 新增】幂等键的默认值、模板与校验规则

> 本节回应两份评审在"客户端不传幂等键时如何处理"上的**表面矛盾**，并给出统一裁决。

#### 澄清：`batch_id` 与幂等键是两个独立概念

两份评审的建议看似矛盾——评审 A 说"不传幂等键时自动生成 UUIDv7 作为 batch_id"，评审 B 说"空键视为不启用幂等，直接写入不去重"。

**两者实际上并不冲突，因为它们说的是不同层次**：

| 概念 | 生成方式 | 是否可选 | 作用层 |
|---|---|---|---|
| **`batch_id`** | **永远由 Ingestor 自动生成 UUIDv7** | **必选，永远存在** | 文件命名、BatchStateStore 状态追踪 |
| **`client_request_id`** | 客户端提供 | **可选** | Meta 层全局去重 |

> 📌 **关键**：`batch_id` 的生成**与幂等键完全无关**。无论客户端是否传幂等键，`batch_id` 都按 ADR-4 自动生成 UUIDv7。评审 A 的表述易被误解为"用幂等键缺失做 fallback 来生成 batch_id"，此处明确澄清。

#### 决策：默认开启 + 表模板（而非简单 true/false）

**评审 A、B 均建议默认开启**。采纳其方向，但给出比"简单默认 true"更可落地的方案——**表模板**：

```rust
struct IngestConfig {
    // 默认 = true（安全优先：数据重复比存储开销严重得多）
    require_idempotency_key: bool,
    idempotency_ttl: Duration,  // 默认 24h
}

// 建表时通过模板选择，无需理解底层细节
enum TableTemplate {
    Audit,     // require_idempotency_key = true  （审计/交易流水）
    General,   // require_idempotency_key = true  （默认）
    Metrics,   // require_idempotency_key = false （高吞吐，容忍极低概率重复）
    Traces,    // require_idempotency_key = false
}
```

**处理矩阵**（关键）：

| 表配置 | 客户端是否传幂等键 | Ingestor 行为 |
|---|---|---|
| `true`（强制） | ✅ 传了 | 正常去重写入 |
| `true`（强制） | ❌ **未传** | **拒绝**（`IdempotencyKeyRequired`）——这才是"强制"的语义 |
| `false`（可选） | ✅ 传了 | **仍然去重**（客户端可主动要求幂等） |
| `false`（可选） | ❌ 未传 | 正常写入，**无去重**，接受极低概率重复 |

> ⚠️ 注意区分评审 B 的表述：评审 B 说"空键 = 不启用幂等"。这在 **`false` 表**下正确；但在 **`true` 表**下，未传幂等键应当**拒绝**而非静默降级为"不启用"——否则"强制"配置形同虚设。

#### 幂等键校验规则（评审 B 风险 1，采纳）

```rust
fn validate_idempotency_key(key: &str) -> Result<()> {
    if key.len() > 256 {
        return Err(InvalidRequest::IdempotencyKeyTooLong);
    }
    // 空字符串在 false 表下合法（=不启用幂等），在 true 表下已被前置拒绝
    Ok(())
}
```

| 校验项 | 规则 | 理由 |
|---|---|---|
| 长度上限 | 256 字节 | 防止超长键（如 10KB JSON）撑爆 Raft Snapshot |
| 空字符串 | `false` 表：合法（不启用幂等）<br>`true` 表：前置拒绝 | 见上方处理矩阵 |
| 字符集 | 不限制（客户端可用 UUID 或 `source_${ts}_${seq}`） | 保持客户端灵活性 |

#### R12' 结论

> **幂等键默认开启（`require_idempotency_key = true`）**，通过 `TableTemplate` 提供 `Metrics`/`Traces` 等关闭模板。关闭时需在文档中标注"接受极低概率的数据重复"。

**R12' 已闭环，不再是待确认项。**

### 7.4 【v7 新增】S3 Multipart Upload ID 持久化

**问题**（评审 B 提出）：大文件用 S3 Multipart Upload 时，崩溃恢复若只复用 `batch_id` 而不复用 `upload_id`，S3 会视为两个不同上传任务，产生孤儿 Parts 与重复数据。

**v7 决策**：

1. `BatchStateStore` 增加 `s3_upload_id` 字段（§5.3）
2. 恢复时：若 `upload_id` 存在且 upload 未完成，调用 `ListParts` 检查已上传分片，**续传**而非重传
3. 配 **S3 Lifecycle Rule**：自动清理 7 天后仍未 complete 的 orphan multipart uploads（这是 S3 侧的兜底，防止程序 bug 导致 Parts 泄漏）

**【v9 新增】S3 Multipart 的 7 天超时约束**（评审 B 风险 3，采纳）

**问题**：S3 Multipart Upload 有 **7 天有效期**限制。若 Ingestor 崩溃后 8 天才恢复，持久化的 `upload_id` 已失效，`ListParts` 将返回 `NoSuchUpload`。

**决策**：恢复时区分两种失败：

```rust
match s3.list_parts(&upload_id).await {
    Ok(parts) => {
        // upload 仍有效 → 续传未完成的 parts
        resume_upload(parts).await
    }
    Err(NoSuchUpload) => {
        // upload 已过期或被 abort → 放弃续传，重新发起
        let new_upload_id = s3.create_multipart_upload().await?;
        batch_state.s3_upload_id = Some(new_upload_id);
        batch_state.status = PENDING;   // 回退到 PENDING，重新走阶段④
        restart_upload().await
    }
    Err(e) => return Err(e),  // 其他错误（网络等）→ 重试
}
```

**与 §12.2 的对齐**：S3 Lifecycle Rule 设为 **7 天**清理未完成 parts，恰好匹配 S3 自身的 Multipart 有效期——程序侧的 `upload_id` 失效与 S3 侧的自动清理**同步发生**，无需额外协调。

> 📌 此场景已在 §11.2 崩溃恢复表中补充对应行。

---

## 八、DataFusion 集成技术路线

> **v7 新增章节**。这是本架构最核心的技术实现路线。

### 8.1 三级扩展点体系

DataFusion 的目录是三层嵌套结构，每层都有对应 trait：

```
CatalogProvider        （对应"数据库/Root Catalog"）
  └── SchemaProvider   （对应"schema/命名空间"）
        └── TableProvider  （对应"一张表"）
```

每个 trait 都是**用户可实现**的 —— 这是整个集成的基础。

### 8.2 CatalogProvider / SchemaProvider 实现

```rust
pub struct LakeCatalogProvider {
    cache: Arc<LocalCatalogCache>,   // 本地缓存，派生状态
}

impl CatalogProvider for LakeCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.cache.schema_names()      // 同步读缓存，纳秒级
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.cache.schema(name)        // 同步，无网络调用
    }
}

impl SchemaProvider for LakeSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.cache.table_names()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let meta = self.cache.table(name)?;
        Some(Arc::new(LakeTableProvider::new(meta, self.cache.clone())))
    }
}
```

**⚠️ 关键约束**：这两个方法都是**同步签名**。若内部发起 gRPC 并 `block_on`，会阻塞 tokio worker 线程，高并发下线程池耗尽。**这正是 ADR-6 选择本地缓存的根本原因**（不只是性能优化，更是正确性要求）。

**注册方式**：

```rust
let catalog = Arc::new(LakeCatalogProvider::new(cache));
let state = SessionStateBuilder::new()
    .with_default_features()
    .with_catalog_list(...)   // 或直接 register_catalog
    .build();
let ctx = SessionContext::new_with_state(state);
```

### 8.3 TableProvider：核心方法

```rust
impl TableProvider for LakeTableProvider {
    fn schema(&self) -> SchemaRef { /* 表当前 schema（V_current） */ }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&[usize]>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // 【重要】scan() 在规划期调用一次，必须轻量：
        //   - 不做 IO、不开连接、不读取文件 footer
        //   - 只描述"数据将如何产生"
        // 所有实际工作在执行阶段的 stream 里做
        ...
    }

    fn supports_filters_pushdown(
        &self, filters: &[&Expr]
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Exact   = 我完全处理该谓词（优化器可移除 FilterExec）
        // Inexact = 我处理一部分，剩余交给 FilterExec
        // Unsupported = 我不处理
        ...
    }

    fn statistics(&self) -> Option<Statistics> {
        // 提供行数、列统计 → 优化器用于 join 顺序、并行度决策
        ...
    }
}
```

**`scan()` 的职责边界**（易错点）：

- ✅ 做：解析 filters、时间范围路由、查索引、快照过滤、构造 ExecutionPlan
- ❌ 不做：读 S3、解析 footer、建立连接 —— 这些留给 `ExecutionPlan::execute()`

### 8.4 【关键】FileFormat / FileSource 与 Schema 适配

DataFusion 读取文件走 `FileFormat` → `FileSource` → `FileOpener` 三级：

```
FileFormatFactory （注册入口）
   └── FileFormat   （schema 推断、创建执行计划）
         └── FileSource    （描述"如何从文件产生数据"）
               └── FileOpener   （打开单个文件，产出 RecordBatch 流）
```

**Vortex 集成**（复用 `vortex-datafusion` crate，**不重复造轮子**）：

```rust
let factory = Arc::new(VortexFormatFactory::new());
let state = SessionStateBuilder::new()
    .with_default_features()
    .with_file_formats(vec![factory])
    .build();
```

**【关键】Schema 适配需要两层 Adapter 配合**（§6.5 已论证方向，此处明确层级）：

```rust
// 在 FileSource 上配置 adapter factory
let source = VortexSource::default()
    .with_schema_adapter_factory(Arc::new(LakeSchemaAdapterFactory {
        table_schema: table_schema.clone(),       // 表当前 schema
        file_schema_versions: file_versions,      // 各文件的 schema version
    }));
```

**两层 Adapter 的分工**（v8 澄清 —— 二者是**配合使用**，不是二选一）：

| Adapter | 层级 | 职责 | 处理的问题 |
|---|---|---|---|
| **`SchemaAdapter`** | 列/结构级 | `map_schema()` / `map_column_index()`：文件列索引 ↔ 表列索引 | **加列、删列、列顺序变化** |
| **`PhysicalExprAdapter`** | 表达式级 | `rewrite()`：把表 schema 上的谓词表达式，改写成目标文件 schema 上的谓词表达式 | **列类型差异**（如 `ts: Int64` vs `ts: Timestamp`） |

**为什么两层都需要**：
- 只有 `SchemaAdapter` → 列能对上，但类型不同的列（如 `ts: Int64` vs `Timestamp`）**谓词无法下推**，退化成"全量读出 → cast → 过滤"，Sort Pushdown 与文件剪枝全部失效
- 只有 `PhysicalExprAdapter` → 表达式能改写，但列索引映射缺失时无法定位列

**这一步做对，谓词才能下推到 Vortex 做文件级/段级剪枝；做错则退化为全表扫描。**

> 📌 **v8 澄清**：部分评审意见认为应"使用 `SchemaAdapter` 而非 `PhysicalExprAdapter`"，这是对 v7 论述的误读。v7 强调的是**不能只用 `SchemaAdapter`**（那会导致谓词无法下推），而非"用 `PhysicalExprAdapter` 替代 `SchemaAdapter`"。二者是不同层级，需配合使用。

⚠️ **API 版本对齐（v8 新增，需在阶段 0 验证）**：

DataFusion 的 Schema 适配 API 在版本演进中有所调整（如 `SchemaAdapter` trait 的方法签名、`PhysicalExprAdapter` 的引入版本、`FileScanConfig` 的构造方式）。**阶段 0 第一周必须完成以下 PoC 验证**：

1. 对照 `datafusion::datasource::schema_adapter` 模块，确认当前 DataFusion 版本（55.x）的实际 API 形态
2. 验证 `Expr` 能否正确转换为 Vortex 所需的过滤表达式（如 `vortex::expr::VortexExpr`）
3. 用"同一表内两个不同 schema 的文件"构造测试，验证谓词**确实下推**（通过 EXPLAIN 检查 `FilterExec` 是否被消除）

**这是查询层最硬的技术骨头，需尽早排除技术地雷。**

### 8.5 文件定位：Meta Manifest 驱动（修正 v6 内部矛盾）

**v6 的矛盾**（两轮评审均未直接指出）：

- §4 ADR-6 说：Query 从 `LocalCatalogCache` 拿文件清单
- §7.2 却用 `ListingTable` 扫描冷数据 —— 而 `ListingTable` 会**自己去 list S3**，绕过 Meta Manifest

**v7 统一为 Meta Manifest 驱动**：

```rust
// ① 从 Catalog 缓存拿文件清单（含 snapshot 过滤）
let files: Vec<FileManifest> = cache
    .files_for_table(table_name)
    .filter(|f| f.valid_from <= snapshot && f.deleted_at > snapshot);

// ② 构造 PartitionedFile
let partitioned: Vec<PartitionedFile> = files.iter()
    .map(|f| PartitionedFile::new(f.file_path.clone(), f.file_size))
    .collect();

// ③ 交给 VortexFormat 建 plan（不再走 ListingTable）
let plan = vortex_format
    .create_physical_plan(state, file_groups, &table_schema, projection, filters, limit)
    .await?;
```

**收益**：
- 消除内部矛盾
- 不依赖 S3 `ListObjects` 发现文件（评审 B 关注的点）
- 文件清单已在内存，省去 list 开销（大量文件下显著）

> 📌 澄清：AWS S3 自 2020-12 起 GET/PUT/LIST 均为强一致，评审 B 所述"List 最终一致"已不成立。但**性能**理由仍然成立（大量文件下 List 慢且贵），故采纳其建议。

### 8.6 热数据：MemTable 与 StreamingTable

| 方式 | 适用 | 说明 |
|---|---|---|
| `MemTable` | 有界热数据 | 包一层 `Vec<RecordBatch>`，实现 TableProvider |
| `StreamingTable` | 无限流 | 接收 `PartitionStream`，可标记 `infinite` |

**v7 决策**：热数据用 **按时间窗口切分的多个 MemTable**（每 1 分钟一个），查询按 `event_time` 路由到特定窗口，**避免全量扫描**。

### 8.7 热冷合并

`scan()` 返回 `UnionExec` 合并多个子计划：

```rust
UnionExec::new(vec![
    hot_memtable_scan,     // 最近 5 分钟
    warm_ssd_scan,         // 本地 SSD
    cold_s3_scan,          // S3 Vortex
])
```

### 8.8 自定义优化规则（可选）

可通过 `OptimizerRule` 在逻辑计划阶段注入自定义改写，例如把"过滤 + 索引可用"的模式重写为 IndexScan。v7 作为可选能力，非必需路径。

---

## 九、查询路径

### 9.1 完整流程

```
SQL 请求
  ↓
DataFusion 解析 → 逻辑计划 → 优化
  ↓
LakeCatalogProvider.schema() / table()   ← 同步读本地缓存，纳秒级
  ↓
LakeTableProvider::scan(filters, ...)
  ├─ ① 从缓存取 snapshot_id
  ├─ ② 解析 filters 提取时间范围
  ├─ ③ 查外部索引 → 候选文件集（文件级）
  ├─ ④ Meta Manifest 过滤（valid_from/deleted_at）→ 有效文件
  ├─ ⑤ 索引候选 ∩ Manifest 有效文件 = 最终文件集
  ├─ ⑥ 按 schema_version 分组 → 为每组配置 PhysicalExprAdapter
  ├─ ⑦ 热/温/冷路由
  └─ ⑧ UnionExec 合并
  ↓
执行阶段：VortexSource 读文件 → 段级剪枝 → 向量化过滤
  ↓
Arrow 结果
```

### 9.2 外部索引：文件级筛选器

**定位**：倒排索引**只决定"读哪些文件"**，不做文件内行级跳过。

**原因**：Tantivy 的 DocId 是 Lucene 段内局部 ID，**与 Vortex 文件行偏移无天然映射**。行级跳过待 Vortex 支持 `RowSelector`。

| 索引类型 | 粒度 |
|---|---|
| 倒排索引（Tantivy） | 文件级 |
| Bloom Filter | 文件级 |
| MinMax | Vortex footer 原生 |
| Bitmap | 文件级 |

### 9.3 四层剪枝

| 层级 | 机制 |
|---|---|
| 分区剪枝 | Meta Manifest 分片键 |
| 文件级 | Meta 精简统计 + Bloom |
| 段级 | Vortex footer 完整统计 |
| 索引级 | 外部倒排/Bloom |

---

## 十、删除与更新语义

### 10.1 三个层次

| 层次 | 粒度 | 优先级 |
|---|---|---|
| **L1 分片级移除** | shard/partition 整体 | **P0（主要形态）** |
| **L2 文件级合并** | Compaction | P0 |
| **L3 行级 U/D** | 单行/谓词 | 远期（P2） |

### 10.2 快照隔离

```
删除/Compaction：
  ① snapshot_id += 1
  ② 目标文件 deleted_at = snapshot_id
  ③ 新文件 valid_from = snapshot_id

Query：只读取 valid_from <= query_snapshot < deleted_at 的文件
```

### 10.3 【v7 修改】物理清理窗口统一 24h

**评审 B 建议**：Query 向 Meta 注册 active_snapshot_id，物理删除前检查长查询租约。

**v7 裁决：不引入租约，改为统一延长物理清理窗口至 24h。**

**理由**：
- v6 §10.2 的孤儿清理**已经是延迟 24h**，将 L1 分片删除的物理清理窗口从 1 小时延长到 24h 即可覆盖绝大多数长查询
- 租约机制给 Meta 增加状态与负担；**存储成本远低于复杂度成本**
- 租约只在"必须快速释放存储"的场景才值得，当前非硬需求

---

## 十一、崩溃恢复与一致性模型

### 11.1 一致性分级

| 数据 | 一致性 |
|---|---|
| WAL 写入 | 强一致（本地），`SyncAll` |
| S3 文件 | immutable |
| Meta 提交 | Raft 强一致 |
| Catalog 缓存 | 最终一致（秒级），可选强一致路由 |
| 查询结果 | 快照隔离 |

### 11.2 三级状态机崩溃恢复

| 崩溃点 | BatchStateStore 状态 | 恢复动作 |
|---|---|---|
| ① 之后、③ 之前 | 无记录（WAL 有数据） | 重新走 ②③④⑤⑥ |
| ③ 之后、④ 之前 | `PENDING` | 重读 WAL → 写 S3 → Commit |
| ④ 之后、⑤ 之前 | `S3_WRITTEN` | 用存储 batch_id 直接 Commit（Meta 去重） |
| ⑤ 之后、⑥ 之前 | `COMMITTED` | 仅清理 |
| S3 Multipart 中途 | `S3_WRITTEN`（含 upload_id） | `ListParts` 检查 → **续传**而非重传 |
| **S3 Multipart 已过期（>7 天）** | `S3_WRITTEN`（含过期 upload_id） | `ListParts` 返回 `NoSuchUpload` → **放弃续传**，重置为 `PENDING`，重新发起新 Multipart Upload（§7.4） |
| fsync 前 | 由自实现 WAL 的 **CRC 校验 + 组提交 fsync** 保证（§5.3） | replay 后按上表分流 |

### 11.3 写入回执

```json
{
  "batch_id": "018f4e2a-3b5c-7d1e-8f9a-2b3c4d5e6f7a",
  "file_path": "s3://lake/metrics/cpu/dt=2026-08-28/hour=14/018f4e2a....vortex",
  "row_count": 10000,
  "ingest_timestamp": "2026-08-28T14:05:00Z",
  "expected_visible_at": "2026-08-28T14:05:30Z",
  "schema_version": 3,
  "snapshot_id": 42
}
```

`expected_visible_at` 用于前端提示"数据将在 X 秒后可见"。

---

## 十二、后台作业

### 12.1 Compaction

- **触发**：文件数/总大小超阈值，或定时
- **租约保护**：向 Meta 申请租约（Raft 写），持有期间独占该批文件
- **【v7 新增】Schema 收敛**：合并时用**当前表 schema** 重新编码（§6.8）
- **【v7 新增】资源隔离**：独立 tokio blocking pool（评审 B 建议），避免挤占 Ingestor 攒批与 Query 响应

### 12.2 孤儿清理

- S3 孤儿文件：延迟 24h 清理
- **【v7 新增】S3 Multipart 孤儿 Parts**：配 S3 Lifecycle Rule，7 天自动清理

### 12.2.1 【v9 新增】三类垃圾回收的时间基准澄清

> 本节回应评审 A 边界 3（GC 水位统一建议），并说明其担忧的竞态为何不成立。

**评审 A 的担忧**：系统中有三个 24h 窗口，时间基准不同步：

| 清理对象 | 时间基准 | 窗口 |
|---|---|---|
| Compaction / 分片移除的物理删除 | `deleted_at` | 24h（§10.3） |
| 孤儿文件清理 | 文件 `created_at` | 24h |
| 幂等键记录 | `committed_at` | 24h（§7.3.1） |

评审 A 建议统一为 `deleted_at`，并指出"若文件创建后立即被 Compaction 标记删除，24h 后物理删除时，孤儿清理的 24h 窗口可能尚未到期，产生竞态"。

**v9 裁决：担忧不成立，但发现了值得澄清的设计点。**

**核心理由 —— 两类清理的操作对象严格互斥**：

| 清理类型 | 操作对象 | Meta 中是否有记录 |
|---|---|---|
| **Compaction / 分片移除清理** | Meta **有**记录、且 `deleted_at` 已过期的文件 | ✅ 有 |
| **孤儿清理** | Meta **没有**记录的文件（写 S3 后未 Commit） | ❌ 无 |

**关键推论**：孤儿文件按定义就是"Meta 不知道的文件"，因此**根本不存在 `deleted_at` 字段**。评审 A 建议的"统一基准为 `deleted_at`"在孤儿清理场景下**无法适用**——没有这个字段可用。

同理，一个文件不可能同时处于"Meta 有记录且已标记删除"和"Meta 无记录"两种状态，因此**两者不会产生竞态**。

**v9 补充的明确约束**（防止实现时混淆）：

```rust
async fn orphan_sweeper() {
    let known_batches = meta_client.list_all_batches().await?;
    let s3_objects = s3_client.list_objects("s3://lake/").await?;

    for obj in s3_objects {
        let batch_id = extract_batch_id(&obj.key);

        // 【关键】先判"是否已知"，再判时间
        if known_batches.contains(&batch_id) {
            continue;   // 已知文件 → 不属于孤儿清理职责，交给 Compaction/分片移除流程
        }
        // 至此才是真正的孤儿：Meta 完全不知道，用 created_at 判断
        if obj.created_at < now() - Duration::from_hours(24) {
            s3_client.delete_object(&obj.key).await?;
        }
    }
}
```

> 📌 **实现要点**：孤儿清理必须**先排除 Meta 已知的文件**，再按 `created_at` 判断。若跳过这一步，可能误删"已被 Meta 接受但 Compaction 尚未完成"的文件。

**幂等键 TTL 为何独立**：幂等键（§7.3.1）存储在独立的 `idempotency_records` 表，与文件生命周期完全解耦，其 `committed_at` 基准与文件清理无关，三者不共享时间轴是**正确的设计**，无需统一。

### 12.3 TTL 分片移除

按时间分区自动触发 L1 分片移除。

---

## 十三、分布式设计与部署演进

### 13.1 阶段 0：All-in-One（✅ 已完成，2026-09-08）

单进程包含所有模块，Meta 用内存 HashMap 或 BoltDB，Compaction 为后台 tokio task。
实际实现以《阶段 0 实现操作日志》为准（crate 命名 `yuntun-*`、二进制 `yuntun`、依赖版本以操作日志 §2.1 为准）。

### 13.1.1 【v12 新增】阶段 1：Standalone 完备（数据库能力收敛）

在进入分布式之前，standalone 先补齐"可交付数据库"能力：

- **Flight SQL 标准协议**（`yuntun-server::flight::FlightServer` 直接实现，基于 arrow-flight 自带模块），
  与既有自定义 ticket 模式双轨并存
- **SQL 写入路径**：Prepared statement `do_put` 与 `INSERT INTO`（DataFusion DML sink）
  全部汇入既有 ingest 管线（WAL 权威），禁止绕过 WAL 直写
- **自有客户端**：`yuntun-client`（SDK + `yuntun-cli`）
- 阶段 0 遗留事项清偿、Chaos 压测、Vortex 接入（详见计划任务书 v2.0 阶段 1/2）

**对分布式设计的影响**：无。本阶段全部工作位于接入层（协议适配）与客户端，
`catalog` 纯逻辑、Meta Raft 语义抽象、`IngestSource` trait 均不动，阶段 3 切换面不变。

### 13.2 阶段 1：Meta 分离（Raft 3 节点）

**【v10 新增】Meta 存储方案**（详见 §5.4）：

| 数据类型 | 存储 | 说明 |
|---|---|---|
| **Raft log + hard state** | **fjall** | 实现 `raft-rs::Storage`（key = 大端序 index） |
| **Catalog（含 Schema）** | **内存 + raft snapshot** | state machine，**不落 fjall 作权威**（§5.4.2 一致性陷阱） |

> ⚠️ **关键区分**：fjall 在 Meta 侧**只存 raft 数据**，不存 Catalog。Catalog 的权威来源是 raft log（可重放）。规模化后才可引入 fjall CF 作缓存加速（§5.4.5）。

**【v7 新增】BoltDB → Raft 迁移**（评审 B 建议）：

`meta` crate 提供 `migrate_from_bolt()`，在阶段 1 首次启动 Raft 集群时，自动将本地 BoltDB 的 Manifest 转换为 Raft 初始 Snapshot。

**【v7 新增】阶段 0 Meta 接口即按 Raft 线性一致性语义抽象**（评审 B 建议）：

即使阶段 0 是单节点，Meta 的接口设计也遵循 `Apply` / `ReadIndex` 抽象，避免阶段 1 切换时重写业务逻辑。

### 13.3 阶段 2：全面分布式

多 Ingestor（独立 WAL）+ 多 Query（无状态）+ Compactor 独立（租约协调）。

### 13.4 关于路由的权衡

| 方案 | 优点 | 缺点 | v7 决策 |
|---|---|---|---|
| 无路由 | 简单，L4 负载均衡 | 小文件多 | — |
| 一致性哈希硬路由 | 文件少 | 节点下线时该 shard 暂停写入 | ❌ 破坏 ADR-3 |
| **时间窗口对齐 + 软路由** | 收敛小文件；节点不可用自动 fallback | 局部性弱于硬路由 | ✅ **采纳** |

---

## 十四、风险、权衡与待决策项

### 14.1 已知风险

| # | 风险 | 缓解 |
|---|---|---|
| 1 | 分离后"读己之写"延迟 = 攒批阈值 | 回执返回 `expected_visible_at`；按表配置阈值；远期考虑 Query 定向拉取 Ingestor 热数据（非广播） |
| 2 | 节点磁盘故障丢数据（`best_effort` 模式） | SLA 分级 → 核心数据用 `durable`；本地盘 RAID |
| 3 | 无 failover（`best_effort` 模式） | `durable` 模式可从 S3 WAL 重建 |
| 4 | Vortex API 演进 | 锁定 Git Commit + Parquet 回退 |
| 5 | 小文件膨胀 | 时间窗口对齐 + 软路由 + Compaction |
| 6 | 索引无法行级剪枝 | 待 Vortex `RowSelector` |
| 7 | Meta Raft 写性能 | 批量提交 + 精简统计（§5.2）+ 读靠缓存卸载 |
| **16（v11 新增）** | **Segment 泄漏导致磁盘写满**：batch 卡在非终态（S3 不可用等）使 segment 永不释放 | 分级超时：批次级 30 分钟 abort + 磁盘 80% 水位强制 abort（§5.3.6.1） |
| **17（v11 新增）** | **组提交读到未 fsync 数据**导致状态机错乱 | `synced_offset` 水位线，攒批线程只读已 fsync 数据（§5.3.5.1） |
| **18（v11 新增）** | **Snapshot 同步序列化阻塞 apply 线程**引发 Leader flapping | 持久化数据结构 O(1) 快照 + `spawn_blocking` 异步序列化（§5.4.3.1） |
| **13（v10 新增）** | **自实现 WAL 的 bug 导致数据丢失** | 阶段 0.5 专项压测：撕裂写入（截断尾部验证 CRC 拦截）、组提交 fsync 持久性、segment 清理不误删未完成 batch（§15） |
| **14（v10 新增）** | **组提交增加写入延迟** | fsync 窗口可配（默认 1–5ms）；高吞吐场景调大 |
| **15（v10 新增）** | **Catalog 内存增长**（内存 + snapshot 方案） | 100 万文件 ≈ 200MB；超 5000 万（~10GB）引入 fjall CF 作缓存（§5.4.5） |
| **8（v7 新增）** | **UUIDv7 时钟回拨** | 引入时钟回拨保护（检测到时间倒退时自旋等待或复用上一毫秒 sequence）；监控暴露 `clock_drift_ms` |
| **9（v7 新增）** | **Schema 碎片过多导致查询适配开销大** | Compaction 收敛（§6.8）；监控 schema_version 分布 |
| **10（v8 新增）** | **惊群效应**：整分钟对齐导致所有节点同秒 flush，Meta Raft 与 S3 承受周期性尖峰 | Flush Jitter：按 `hash(shard+table) % 60` 打散 flush 时刻（§4 ADR-10） |
| **11（v8 新增）** | **幂等键存储膨胀**：千万级写入导致 Raft Snapshot 无限增长 | TTL 24h + 按表配置 `require_idempotency_key`（高吞吐表可关闭）（§7.3.1） |
| **12（v8 新增）** | **幂等键生命周期耦合（v7 隐含缺陷）**：依附 FileManifest 时，Compaction 删除文件会导致幂等失效 | 独立 `idempotency_records` 表，自有 TTL（§7.3.1 修正 1） |

### 14.2 待决策项

| # | 决策点 | v7 倾向 |
|---|---|---|
| R1' | SLA 分级是否化解"通用"定位歧义 | 是，需业务确认哪些表用 `durable` |
| R2' | Schema 演进模型是否完备 | 待评审验证 §6 |
| R3' | `PhysicalExprAdapter` 用法是否正确 | 待评审验证 §8.4 |
| R4' | 幂等键三层防护是否覆盖所有场景 | 待评审验证 §7.3 |
| R5' | 时间窗口对齐能否控制小文件 | 需压测验证 |
| R8' | 混合类型（数值 vs 字符串）默认策略 | 默认拒绝，`coerce_mixed_types` 可选开启 |
| R9' | Meta 何时上 Raft | 阶段 1 |
| R10' | Compactor 何时独立 | 阶段 2+（阶段 0-1 内嵌但需资源隔离） |
| **R11'** | **Kafka Source 插件优先级** | 阶段 2+，作为可插拔 Source（非 WAL） |

---

## 十五、演进路线图

> **【v12 重排说明（2026-09-08）】**：本节保留 v11 原始路线作历史参考；**现行路线以
> 《开发计划任务书 v2.0》为准**——阶段 0 已完成 → 阶段 1 Standalone 完备（Flight SQL +
> SQL 写入 + 客户端）→ 阶段 2 质量与性能（原阶段 0.5）→ 阶段 3 分布式化（原阶段 1/1.5
> 整体后移）→ 阶段 4 规模化（原阶段 2/3）。以下原文中"阶段 1 / 1.5"等编号均按此映射阅读。

### 阶段 0：All-in-One 起步（0–2 月）
- 单进程：API + Ingestor + Query + Meta(内存) + 本地对象存储
- **【v10 调整】仅 Arrow Flight 写入**（`IngestSource` trait 已抽象，§7.1.1）
- **【v10 调整】自实现 WAL**（segment + CRC + 组提交 fsync，§5.3）→ 攒批 → 写 Vortex
- **【v7 新增】Schema 演进基础框架**（Schema Registry 雏形）
- **【v7 新增】Compaction 资源隔离**（独立 tokio blocking pool）
- **【v7 新增】Meta 接口按 Raft 线性一致性语义抽象**（为阶段 1 铺路）
- **【v9 新增】幂等键独立存储 + TTL + 表模板**（§7.3.1 / §7.3.2）

### 阶段 0.5：核心逻辑压测（2–2.5 月）【v9 新增，提前】

> **变更理由**（评审 A 边界 2，采纳）：快照隔离、Compaction、分片移除、Schema 变更、幂等键等**核心逻辑在阶段 0 已全部存在**——Meta 是内存/BoltDB 而非 Raft，不影响这些逻辑的正确性验证。若等到阶段 1 才压测，届时修复成本远高于单进程环境。

**验证方式**：单进程 + **Mock S3**（本地文件系统模拟），注入故障。

| 压测场景 | 验证目标 |
|---|---|
| Compaction 期间查询 | 快照隔离生效，无数据重复、无已删数据 |
| 分片移除期间查询 | `valid_from`/`deleted_at` 过滤正确 |
| 孤儿文件清理 | 不误删已知文件（§12.2.1 约束） |
| Schema 变更（加列/宽化） | 查询不崩溃，谓词**确实下推**（EXPLAIN 验证 `FilterExec` 消除） |
| 幂等键去重 | Compaction 删除文件后，24h 内重试仍幂等（§7.3.1 核心修正） |
| 崩溃恢复（各状态点） | 三级状态机 + Multipart 续传/超时均正确 |
| **【v10 新增】WAL 专项压测** | 撕裂写入检测（截断文件尾部，验证 CRC 拦截）；组提交 fsync 持久性；segment 清理不误删未完成 batch |
| **【v11 新增】并发写入 + 崩溃恢复** | 多 Source 并发写不同 shard，在组提交 fsync **之前 / 期间 / 之后**分别 `kill -9`，三种情况恢复后均须无丢失、无重复、WAL 无撕裂 |
| **【v11 新增】`synced_offset` 验证** | 在 `write()` 后、`fsync()` 前 kill，验证攒批线程**绝不**读到未 fsync 数据（§5.3.5.1） |
| **【v11 新增】Batch 超时与 Segment 释放** | 模拟 S3 永久不可用，验证 30 分钟后 `BatchAbort` 正确触发、segment 被释放、磁盘不写满（§5.3.6.1） |
| **【v11 新增】磁盘保护水位** | 人为填高 WAL 使用率至 80%，验证强制 abort 最老未完成 batch 生效 |

- **验证目标**：100% 平滑无 500，无数据重复

### 阶段 1：Meta 分离（2.5–4 月）
- Meta 独立为 Raft 3 节点
- **【v10 新增】fjall 实现 raft-rs `Storage`**（raft log + hard state，§5.4.3）
- **【v10 新增】Catalog 内存 + raft snapshot**（不作为权威落 fjall，§5.4.2）
- **【v7 新增】BoltDB → Raft 自动迁移工具**
- 快照隔离 + 分片级移除 + 客户端幂等键（**逻辑已在阶段 0.5 验证**，此处仅验证网络序列化）

### 阶段 1.5：分布式压测（4–5 月）【v9 调整：范围收窄】

> **v9 变更**：核心逻辑已在阶段 0.5 验证完毕，本阶段**只验证分布式特有的问题**，不再重复验证核心逻辑。

| 压测场景 | 验证目标 |
|---|---|
| Catalog 缓存落后 30 秒 | 最终一致性 + 版本校验兜底 |
| Meta Raft Leader 切换 | 写入不中断，无脑裂 |
| 时钟回拨（手动调时间） | UUIDv7 与 `time_window` 不错乱 |
| **惊群效应压测** | 100 节点同窗口，Flush Jitter 是否有效削峰（§4 ADR-10） |
| 网络分区恢复 | Ingestor 重连后 BatchStateStore 状态正确恢复 |
| **【v11 新增】Snapshot 期间 Leader 稳定性** | 持续写入下强制触发 snapshot，验证无 Leader 切换（§5.4.3.1 异步序列化生效） |
| Meta 吞吐 | 达到 10K CommitFiles/sec（§2.1） |

### 阶段 2：全面分布式（4–8 月）
- 多 Ingestor（独立 WAL，幂等键去重就绪）
- 多 Query（无状态水平扩展）
- Compactor 独立（租约协调）
- 外部索引 + 热温冷分层
- **Kafka Source 插件**

### 阶段 3：规模化（8–12 月）
- Arrow Flight 查询分发
- 行级 UPDATE/DELETE（L3，基于 Iceberg MoR）
- Iceberg 表格式适配

---

## 十六、附录

### 附录 A：技术选型

| 组件 | 选型 | 许可证 | 备注 |
|---|---|---|---|
| 查询引擎 | DataFusion 55.x | Apache-2.0 | 查询内核 |
| 主存储格式 | Vortex | Apache-2.0 | 锁 Git Commit |
| 兼容格式 | Parquet | Apache-2.0 | 回退备胎 |
| **Ingestor WAL** | **自实现（segment + CRC）** | — | **v10：移除 fjall，约 500 行（§5.3）** |
| **Meta 存储** | **fjall（仅 raft 数据）** | MIT/Apache-2.0 | **v10：raft log + hard state；Catalog 走内存 + snapshot（§5.4）** |
| ID 生成 | UUIDv7 | — | 随机，时间戳有序 |
| Meta 共识 | raft-rs | Apache-2.0 | 自研 Storage 实现 |
| **摄入协议（MVP）** | **Arrow Flight（DoPut）** | Apache-2.0 | **v10：仅此一种（§7.1.1）** |
| 摄入协议（阶段 2+） | InfluxDB Line Protocol / Kafka | MIT / Apache-2.0 | 可插拔 Source |
| 变更通知 | NATS JetStream / 内嵌 | Apache-2.0 | Kafka 可选 |
| 倒排索引 | Tantivy | MIT/Apache-2.0 | 文件级筛选 |

### 附录 B：设计原则速查

- **【v12.3】所有写入都走 ingest 管线**——唯一的数据写入事实（WAL 权威，禁绕过）
- **【v12.3】协议端口统一在 server 节点层**，域 crate（ingest/query）纯能力、无协议无间接层
- **按状态切分服务**，而非按功能
- **Vortex 只做 append + 微批**
- **batch_id 随机**，幂等由 BatchStateStore + 客户端幂等键保证
- **Schema 适配必须走 PhysicalExprAdapter**（否则谓词无法下推）
- **Ingestor WAL 自实现**（append-only + CRC，不用 KV 引擎）
- **Meta 的 fjall 只存 raft 数据**，Catalog 是 state machine（内存 + snapshot）
- **删除是快照级语义**
- **查询路径零 gRPC**，元数据读本地缓存
- **文件定位由 Meta Manifest 驱动**，不依赖 S3 List
- **协调层复用 Meta Raft**
- **持久性是可配置的 SLA**，非全局承诺

### 附录 C：v6 → v7 变更清单

| 类别 | 变更项 | 来源 |
|---|---|---|
| **P0** | 客户端幂等键 + Meta 唯一索引 | 评审 A |
| **P0** | Schema 演进设计（三层模型 + 类型提升格） | 评审 B 盲区 + 独立设计 |
| **P0** | InfluxDB 类型映射策略 | 评审 A |
| **P0** | SLA 分级（best_effort / durable） | 两轮评审分歧裁决 |
| **P1** | S3 Multipart upload_id 持久化 + Lifecycle 清理 | 评审 B |
| **P1** | Meta 只存精简 Statistics | 评审 B（简化其方案） |
| **P1** | Compaction 资源隔离 | 评审 B |
| **P1** | BoltDB → Raft 迁移工具 | 评审 B |
| **P1** | 阶段 0 Meta 接口按 Raft 语义抽象 | 评审 B |
| **P1** | 时间窗口对齐 + 软路由（替代硬路由） | 评审 B 问题 + 独立裁决 |
| **P1** | 统一 Meta Manifest 驱动（修正 v6 内部矛盾） | 独立修正 |
| **P2** | 物理清理窗口统一 24h（替代长查询租约） | 简化评审 B 方案 |
| **P2** | 绝对空闲超时兜底 | 评审 A |
| **P2** | UUIDv7 时钟回拨保护 | 评审 B |
| **P2** | Kafka Source 插件（非 WAL） | 评审 B |
| **新增** | DataFusion 集成技术路线（第 8 章） | — |
| **新增** | Schema 演进设计（第 6 章） | — |
| **修正** | S3 强一致性说明（评审 B 信息过时） | 独立修正 |

### 附录 D：前三轮评审贡献总结

| 评审 | 最大贡献 | 主要盲区 |
|---|---|---|
| **评审 A（第一轮）** | 发现"客户端重试 + 随机 UUID = 数据翻倍"这一 P0 逻辑漏洞 | 未识别小文件代价；未覆盖 Schema 演进 |
| **评审 B（第二轮）** | Schema 演进、S3 Multipart、资源隔离等落地细节；质疑"通用"定位歧义 | S3 一致性信息过时；租约/Statistics 方案偏重 |
| **评审 A（第三轮）** | Schema 演进的分布式竞态；幂等键 TTL | 乐观锁作用点建议有误（见 §6.7 修正） |
| **评审 B（第三轮）** | **惊群效应（整分钟对齐的副作用）**；InfluxDB Tag/Field 语义 | 对 `PhysicalExprAdapter` 与 `SchemaAdapter` 关系理解有偏差（见 §8.4 澄清） |

四份评审**互补性极强**：A 抓架构灵魂与一致性漏洞，B 抓落地骨架与运维细节。

---

## 附录 E：v7 → v8 变更清单与评审响应

### E.1 变更清单

| 类别 | 变更项 | 来源 | 优先级 |
|---|---|---|---|
| **修正** | **幂等键独立存储**（与 FileManifest 解耦，避免 Compaction 导致幂等失效） | **独立发现**（两轮评审均未识别） | **P0** |
| **修正** | Flush **Jitter 削峰**（修正 v7 整分钟对齐引发的惊群效应） | 评审 B（第三轮） | **P0** |
| **新增** | 幂等键 **TTL 24h** + 语义边界说明 + 存储量估算 | 评审 A、B 共识 | P0 |
| **新增** | InfluxDB **Tag / Field 语义映射**（Tag → Dictionary + 自动索引） | 评审 B（第三轮） | P1 |
| **新增** | Schema 演进**乐观并发控制**（OCC） | 评审 A（第三轮），**作用点已修正** | P1 |
| **新增** | Compaction 丢弃逻辑删除列的 **24h 安全窗口** | 评审 B（第三轮） | P2 |
| **澄清** | 两层 Adapter（`SchemaAdapter` + `PhysicalExprAdapter`）**配合使用**，非二选一 | 独立澄清 | P1 |
| **新增** | §8.4 **API 版本对齐 PoC 验证**（阶段 0 第一周） | 评审 B 提醒 | P1 |

### E.2 对评审建议的鉴别与修正（3 处）

#### 修正 1：Schema 乐观锁的作用点（评审 A 建议有误）

| | 评审 A 建议 | v8 采纳方案 |
|---|---|---|
| 作用点 | 每次 `CommitFiles` 携带 `If-Schema-Match` | **仅 `EvolveSchema` 请求**携带乐观锁 |
| `CommitFiles` 校验 | 校验 schema version | **不校验** |

**理由**：v7 设计中每个文件记录自己的 `schema_version`，**同一表的不同文件本就可以有不同版本**（查询层用 `PhysicalExprAdapter` 适配）。用旧 schema 写入的文件**完全合法**。

若按评审 A 建议在每次 Commit 校验，会导致 schema 变更瞬间大量**合法**的老 schema 写入被无谓拒绝，损害吞吐。真正的竞态只在"并发演进"这一动作上。

#### 修正 2：两层 Adapter 不是二选一（评审 B 理解有偏差）

评审 B 认为应使用 `SchemaAdapter` 而非 `PhysicalExprAdapter`。这是对 v7 论述的误读：

- v7 强调的是**不能只用 `SchemaAdapter`**（会导致谓词无法下推、剪枝失效）
- 而非"用 `PhysicalExprAdapter` 替代 `SchemaAdapter`"

**实际需要两者配合**：`SchemaAdapter` 处理列映射（加/删列），`PhysicalExprAdapter` 处理表达式改写（类型差异让谓词可下推）。

#### 修正 3：Compaction 丢弃列的理由（评审 B 归因偏差）

评审 B 担心的是 **Time Travel 冲突**，但 Time Travel 在 v7/v8 中均为**非目标**（§2.2）。

v8 采纳该建议，但理由更正为**支持"撤销删列"的可恢复性**；且实现上复用 §10.3 已有的 24h 物理清理窗口，**不引入新机制**。

### E.3 独立发现（两份评审均未识别）

> **幂等键生命周期与 FileManifest 耦合**

这是 v7 的隐含缺陷，两轮评审只关注了"幂等键膨胀"（存储量问题），未发现"生命周期耦合"（正确性问题）：

```
客户端 K 写入成功 → K 记录在 FileManifest A
     ↓ 数小时后
Compaction 合并 A → FileManifest A 被物理删除
     ↓
客户端用 K 重试 → Meta 查不到 K → 视为新请求 → 数据重复
```

**Compaction 可能在数小时内发生，远早于合理重试窗口结束。** 此为比"膨胀"更严重的问题，v8 通过独立 `idempotency_records` 表 + 自有 TTL 修正（§7.3.1）。

---

### E.4 编码规范：ADR 决策的内嵌引用

评审 A 建议在代码中为每个 ADR 决策添加注释引用。**采纳**：

```rust
// ADR-4: batch_id is random UUIDv7, NOT derived from content.
// Idempotency is guaranteed by BatchStateStore + client_request_id (§7.3),
// NOT by batch_id determinism. Do not "optimize" this into a content hash.
let batch_id = Uuid7::new().to_string();

// ADR-3: This Ingestor is fully independent. NEVER add cross-node
// coordination here. See §4 ADR-3 for why.
async fn commit_files(...) { ... }
```

**规范要点**：
- 每个非显然的设计决策，在代码处用 `// ADR-N: <决策> + <为什么>` 注释
- 特别标注**容易被"优化"掉的决策**（如 ADR-4 的随机 batch_id）
- 注释中指向文档章节，便于维护者追溯完整论证

---

## 附录 F：v8 → v9 变更清单与评审响应

### F.1 变更清单

| 类别 | 变更项 | 来源 | 优先级 |
|---|---|---|---|
| **新增** | 幂等键**默认值 + 表模板**（默认 true，`Metrics`/`Traces` 模板关闭） | 评审 A 边界 1、评审 B R12' 共识 | **P0** |
| **新增** | **幂等键校验规则**（长度 ≤256，空键语义） | 评审 B 风险 1 | P1 |
| **澄清** | **`batch_id` 与幂等键是两个独立概念**（厘清两份评审的表面矛盾） | 独立澄清 | **P1** |
| **新增** | **Compaction Schema Snapshot**（启动时锁定 schema 版本） | 评审 B 风险 2 | P1 |
| **新增** | **S3 Multipart 7 天超时**处理（`NoSuchUpload` → 重新发起） | 评审 B 风险 3 | P2 |
| **新增** | **Meta Raft 吞吐目标** 10K/sec + 分片演进路径 | 评审 B 风险 5 | P2 |
| **调整** | **压测提前**：新增阶段 0.5（核心逻辑，Mock S3）；阶段 1.5 收窄为分布式专项 | 评审 A 边界 2 | **P1** |
| **澄清** | **GC 水位**：三类清理时间基准无需统一（操作对象互斥） | 评审 A 边界 3，担忧不成立 | P2 |
| **修正** | **不丢弃已用旧 schema 写入的文件**（澄清写入时序） | 评审 B 风险 4，建议过激 | P1 |
| **新增** | **ADR 注释编码规范** | 评审 A 建议 | P2 |

### F.2 对评审建议的鉴别与修正（3 处）

#### 修正 1：GC 水位无需统一 —— 竞态不成立（评审 A 边界 3）

评审 A 建议将三类 GC 的时间基准统一为 `deleted_at`，并担忧"孤儿清理窗口与 Compaction 清理窗口不同步产生竞态"。

**v9 裁决：担忧不成立。** 核心理由：

> **孤儿文件按定义就是"Meta 不知道的文件"，因此根本不存在 `deleted_at` 字段。**

两类清理的操作对象**严格互斥**：

| 清理类型 | 操作对象 | Meta 记录 |
|---|---|---|
| Compaction / 分片移除 | Meta **有**记录且 `deleted_at` 过期 | ✅ 有 |
| 孤儿清理 | Meta **无**记录 | ❌ 无 |

一个文件不可能同时处于两种状态，故无竞态。v9 补充了 §12.2.1 明确实现约束（**先排除已知文件，再按 `created_at` 判断**），防止实现时误删。

#### 修正 2：不丢弃已用旧 schema 写入的文件（评审 B 风险 4）

评审 B 建议"收到 `SCHEMA_CHANGED` 后必须丢弃已写入的 S3 文件"。

**v9 裁决：过于激进，不予采纳。** 理由：

1. 严格时序下（`EvolveSchema` 在写 S3 **之前**完成），不存在"已写 S3 才发现冲突"的常态场景
2. 即便发生，用旧 schema 写入的文件**完全合法**（不同文件本就可以有不同 `schema_version`）
3. 丢弃会浪费已完成的 S3 写入，并增加孤儿清理负担
4. **schema 碎片由 Compaction 收敛（§6.8），而非在写入路径上通过丢弃来避免**

v9 补充了 §6.7 的严格时序图，从源头避免该场景。

#### 修正 3：幂等键默认值的深层澄清（两份评审表述易混）

两份评审对"客户端不传幂等键"的处理给出看似矛盾的建议：

- **评审 A**：自动生成 UUIDv7 作为 `batch_id`
- **评审 B**：空键视为不启用幂等，直接写入不去重

**v9 澄清：两者说的是不同层次，并不冲突。**

- `batch_id` **永远**由 Ingestor 自动生成 UUIDv7，与幂等键**完全无关**（ADR-4）
- `client_request_id` 是**独立可选字段**，仅用于 Meta 层去重

同时，v9 给出比"简单默认 true"更可落地的**表模板方案**，并明确了处理矩阵（§7.3.2）——特别注意：在 `require_idempotency_key = true` 的表中，未传幂等键应当**拒绝**而非静默降级，否则"强制"形同虚设。

### F.3 R12' 已闭环

| 待确认项 | v9 结论 |
|---|---|
| R12'：幂等键是否默认开启？高吞吐表是否关闭？ | **默认开启**（`require_idempotency_key = true`），通过 `TableTemplate` 提供 `Metrics`/`Traces` 关闭模板。详见 §7.3.2 |

---

## 附录 G：v9 → v10 变更清单（组件选型收敛）

### G.1 变更清单

| # | 变更项 | 章节 | 影响 |
|---|---|---|---|
| 1 | **Ingestor WAL 自实现**（移除 fjall） | §5.3 | 无外部依赖，约 500 行 |
| 2 | **`BatchState` 写入同一 WAL 事件流** | §5.3.4 | 原子性天然保证，无需独立 KV |
| 3 | **WAL segment + CRC + 组提交 fsync** | §5.3.3 / §5.3.5 | 撕裂写入可检测；吞吐提升 |
| 4 | **segment 按 batch 状态安全清理** | §5.3.6 | 无丢失风险的清理策略 |
| 5 | **MVP 仅 Arrow Flight 写入** | §7.1 / §2.1 | 范围收缩 |
| 6 | **`IngestSource` trait 抽象** | §7.1.1 | 保证后续加协议不重构 |
| 7 | **Meta：fjall 只存 raft 数据** | §5.4 | 澄清职责边界 |
| 8 | **Catalog 内存 + snapshot（不落 fjall）** | §5.4.2 | 规避一致性陷阱 |
| 9 | **fjall 实现 raft-rs `Storage`** | §5.4.3 | key = 大端序 index |

### G.2 关键设计判断

#### 判断 1：自实现 WAL 优于 fjall

| 需求 | fjall 提供 | 实际需要 |
|---|---|---|
| 写入 | LSM-tree 全套 | **仅 append** |
| 读取 | 随机读 + range query | **仅顺序 replay** |
| 维护 | 自动 Compaction | **按 batch 状态清理** |

**多余能力即负担**。自实现后无黑盒、完全可控，且 `BatchState` 与 `Data` 在同一 append-only 流中——**顺序即因果**，比"两 CF 共享 WAL"更简洁。

#### 判断 2：Meta 的两类数据必须分开对待（**最易踩的坑**）

> **Catalog 是 raft state machine，其权威来源是 raft log（可重放），不是独立的持久化层。**

若把 Catalog 落 fjall 当权威数据，会出现**双写一致性陷阱**：

```
raft apply 到 index N  →  更新内存 Catalog  →  异步写 fjall  →  崩溃
                                              ↑ 可能只到 N-3
恢复时：无法判断 raft log 与 fjall 谁更新
```

**v10 决策**：
- **fjall → raft 数据**（log / hard state）
- **内存 → Catalog**（权威）
- **raft snapshot → 持久化**
- 恢复 = 加载 snapshot + 重放 log（**唯一权威路径**）

规模化后即便引入 fjall CF 存 Catalog，也只能作为**加速缓存**，恢复仍以 raft snapshot + log 为准。

#### 判断 3：Arrow Flight 先行，但抽象必须提前

"先只做 Arrow Flight"是合理的 MVP 收缩，但若不提前设计 `IngestSource` trait，阶段 2+ 加 InfluxDB / Kafka 时需要重构写入路径。

**trait 的核心是输出统一的 `IngestBatch`**（表、shard_key、RecordBatch、幂等键），使下游（攒批 / WAL / S3 / Meta）完全不感知协议差异。

### G.3 v10 新增风险

| # | 风险 | 缓解 |
|---|---|---|
| 1 | **自实现 WAL 的 bug 导致数据丢失** | 阶段 0.5 专项压测：撕裂写入（截断尾部，验证 CRC 拦截）、组提交 fsync 持久性、segment 清理不误删 |
| 2 | **组提交延迟**：fsync 批次窗口增加写入延迟 | 窗口可配（默认 1–5ms）；高吞吐场景调大 |
| 3 | **Catalog 内存增长**（v1 方案） | 容量估算：100 万文件 ≈ 200MB；超 5000 万（10GB）时引入 fjall CF 作缓存（§5.4.5） |

---

## 附录 H：v10 → v11 变更清单（终审工程补丁）

终审两份报告均给出 **GO / 无条件通过**，并提出 3 个工程级补丁。**全部采纳**，其中 2 处的实现方式与评审原文不同（已修正）。

### H.1 变更清单

| # | 补丁 | 章节 | 严重性 | 处理 |
|---|---|---|---|---|
| 1 | **`synced_offset` 水位线** | §5.3.5.1 | **正确性缺陷** | ✅ 采纳 |
| 2 | **Batch 超时 + 终态含 ABORT** | §5.3.6.1 | **P0 磁盘写满** | ✅ 采纳（阈值调整） |
| 3 | **Snapshot 异步生成** | §5.4.3.1 | P1 Leader flapping | ✅ 采纳（**实现方式修正**） |
| 4 | `CURRENT` 原子切换（目录 fsync） | §5.3.6 | P1 | ✅ 采纳 |

### H.2 对评审建议的修正（2 处）

#### 修正 1：Snapshot 不能用"克隆 Arc + 后台序列化"

评审 B 建议"快速克隆 `Arc` 指针，将序列化派发给后台线程"。**这在 Rust 中会产生数据竞争**：

```
Arc 克隆 = 共享同一份数据，而非拷贝
→ 后台线程序列化期间，apply 线程仍在修改 Catalog
→ 序列化读到"半新半旧"状态 → snapshot 不是一致的状态点
```

**v11 修正**：核心是**在持锁的极短时间内取出不可变快照**：

| 方案 | 取快照开销 | 评价 |
|---|---|---|
| 深拷贝（持读锁） | O(n)，200MB | ❌ 仍阻塞写 |
| **持久化数据结构（`im` crate）** | **O(1)** | ✅ **推荐** |
| 版本号 + COW | O(1) | ✅ 备选 |

用 `im::HashMap` 后，`guard.clone()` 是 O(1) 且与原数据隔离——**锁只持有"克隆指针"一瞬**，序列化在 `spawn_blocking` 中对不可变快照进行，既无竞争也不阻塞 apply 线程。

#### 修正 2：Batch 超时阈值不能取 24 小时

评审 B 建议阈值 24 小时。**若真取 24 小时，WAL 很可能在此之前就写满磁盘**——补丁的目的是防止磁盘写满，24 小时窗口对此无效。

**v11 修正为分级策略**：

| 级别 | 条件 | 阈值 |
|---|---|---|
| 批次级 | 非终态持续时间 | **30 分钟**（容忍 S3 抖动与正常重试） |
| 磁盘保护 | WAL 使用率 | **80%**（强制 abort 最老未完成 batch） |

二者结合：既不会误 abort 正常的慢写入，又确保磁盘绝不写满。

### H.3 一个额外的设计收益（评审未指出）

> **`synced_offset` 无需单独持久化。**

引入该水位线后，最初担心需要额外的检查点文件来记录它。但实际上：

- fsync 成功的数据 → 完整写入 → **必然通过 CRC**
- 未 fsync 的尾部 → 可能撕裂 → **CRC 必然失败**

因此 **CRC 校验边界 = fsync 边界**。恢复时 replay 到 CRC 失败处即停止，该位置天然就是 `synced_offset`。

这是 §5.3.3 中"CRC 置于 length 之后"这一设计的**第二次收益**——用一个机制同时解决了撕裂检测与水位推断，无需任何额外元数据。

### H.4 阶段 0.5 / 1.5 压测新增场景

| 阶段 | 新增场景 |
|---|---|
| 0.5 | 并发写入 + 崩溃恢复（fsync 前/中/后三种 kill 点） |
| 0.5 | `synced_offset` 验证（write 后 fsync 前 kill） |
| 0.5 | Batch 超时与 segment 释放（模拟 S3 永久不可用） |
| 0.5 | 磁盘保护水位（人为填至 80%） |
| 1.5 | Snapshot 期间 Leader 稳定性 |

---

**文档结束 · v11（终审通过，GO for 阶段 0）**

> **阶段 0 第一周 PoC（4 项）**：
> 1. §5.3 **自实现 WAL 原型** —— segment、CRC、**`synced_offset`**、组提交 fsync、崩溃恢复
> 2. §8.4 **DataFusion Schema 适配 PoC** —— 两层 Adapter，EXPLAIN 验证 `FilterExec` 消除
> 3. §6.7 **Schema Registry 雏形** —— 含 OCC
> 4. §7.3 **幂等键独立存储 + TTL + 表模板**
>
> **阶段 0.5（Chaos 压测）**：见 §15，WAL 专项已扩至 5 项（含并发崩溃、水位验证、Batch 超时、磁盘保护）
>
> **依赖锁定**：DataFusion 55.x、**Vortex 锁定 Git Commit Hash**（ADR-1）
>
> ✅ **无剩余待确认项** —— 终审 GO

> **阶段 0 第一周必须完成（PoC）**：
> 1. §8.4 **DataFusion Schema 适配 API PoC** —— 验证两层 Adapter 与谓词下推确实生效（EXPLAIN 检查 `FilterExec` 消除）
> 2. §6.7 **Schema Registry 雏形** —— 含 OCC 与严格时序
> 3. §7.3.1 / §7.3.2 **幂等键独立存储 + TTL + 表模板 + 校验**
> 4. **【v10 新增】§5.3 自实现 WAL 原型** —— segment 读写、CRC 校验、组提交 fsync、崩溃恢复
>
> **阶段 0.5 必须完成（核心逻辑压测）**：见 §15，用 Mock S3 验证快照隔离、Compaction、Schema 变更、幂等键、崩溃恢复、**WAL 专项**
>
> **阶段 1 必须完成**（Meta 分离）：§5.4 fjall 实现 raft-rs `Storage` + Catalog snapshot
>
> ✅ **无剩余待确认项**
