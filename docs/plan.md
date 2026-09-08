# 通用直写数据湖 — 开发计划任务书

> **依据**：《通用直写数据湖架构设计 v11》+《详细设计文档 v1.0》
> **版本**：v1.0
> **日期**：2026-08-31
> **适用范围**：阶段 0（All-in-One）至阶段 3（规模化），共 12 个月
>
> **本任务书定义**：做什么（WBS）、谁做、何时做、依赖什么、什么算做完。

---

## 目录

1. [项目概述](#一项目概述)
2. [交付物清单](#二交付物清单)
3. [团队与角色](#三团队与角色)
4. [里程碑](#四里程碑)
5. [WBS 任务分解](#五wbs-任务分解)
6. [排期甘特](#六排期甘特)
7. [关键路径与依赖](#七关键路径与依赖)
8. [风险与应对](#八风险与应对)
9. [验收标准](#九验收标准)
10. [工程规范](#十工程规范)

---

## 一、项目概述

### 1.1 目标

构建**通用直写数据湖**：支持 Arrow Flight 直写摄入、Vortex/Parquet 开放存储、分片级数据移除、SQL 交互式分析的存储底座。

### 1.2 本期（阶段 0–0.5）核心范围

| 维度 | 内容 |
|---|---|
| 部署形态 | 单进程 All-in-One |
| 摄入协议 | Arrow Flight（唯一） |
| 持久化 | 自实现 WAL（segment + CRC + 组提交 fsync） |
| 存储格式 | Vortex 主 + Parquet 回退开关 |
| 元数据 | 内存 Catalog（按 Raft 语义抽象） |
| 能力 | Schema 演进、快照隔离、分片移除、幂等写入、崩溃恢复 |

### 1.3 非目标（本期不做）

分布式多节点、Raft 共识、InfluxDB/Kafka Source、行级 UPDATE/DELETE、Time Travel、外部倒排索引、热温冷分层。

---

## 二、交付物清单

| # | 交付物 | 类型 | 阶段 |
|---|---|---|---|
| D1 | `lakehouse` Cargo workspace 源码 | 代码 | 0 |
| D2 | `all-in-one` 可运行二进制 | 二进制 | 0 |
| D3 | 详细设计文档 | 文档 | 0 |
| D4 | 单元测试 + 集成测试套件 | 测试 | 0 |
| D5 | Chaos 测试套件（11 场景） | 测试 | 0.5 |
| D6 | 压测报告 | 报告 | 0.5 |
| D7 | Meta 独立服务（Raft 3 节点） | 代码 | 1 |
| D8 | 分布式压测报告 | 报告 | 1.5 |
| D9 | 多节点部署文档与运维手册 | 文档 | 2 |
| D10 | API / 协议文档（Flight + gRPC） | 文档 | 2 |

---

## 三、团队与角色

| 角色 | 代号 | 人数 | 职责 | 技能要求 |
|---|---|---|---|---|
| 资深 Rust 工程师 | **R1** | 1 | WAL 核心、并发与崩溃恢复、架构把关 | Rust 异步、系统编程、存储引擎 |
| Rust 工程师 | **R2** | 1 | Ingestor、Meta、Schema 演进 | Rust、对象存储、状态机 |
| Rust / DataFusion 工程师 | **R3** | 1 | Query 桥接、两层 Adapter、查询优化 | DataFusion / Arrow 深入 |
| 测试工程师 | **Q1** | 1 | Chaos 测试、压测、故障注入 | 测试自动化、Jepsen 风格 |
| DevOps（兼职） | **O1** | 0.5 | CI/CD、环境、监控 | K8s、Prometheus、S3/MinIO |

**总投入**：3.5 人（阶段 0–0.5）；阶段 1 起可视需要增补 1 人。

---

## 四、里程碑

| 里程碑 | 时间点 | 交付内容 | 准出条件 |
|---|---|---|---|
| **M0** | W1 末 | 4 个 PoC | WAL 崩溃恢复、谓词下推、Flight 链路、CI 全部通过 |
| **M1** | W4 末 | 写入链路贯通 | Flight → WAL → S3 → Meta 端到端可写 |
| **M2** | W8 末 | 阶段 0 功能完成 | 可写入、可查询、可删除分片 |
| **M3** | W10 末 | **阶段 0.5 准出** | 11 个 Chaos 场景 100% 通过 |
| **M4** | W16 末 | Meta 分离 Raft | 3 节点 Raft 运行，写入不中断 |
| **M5** | W20 末 | 分布式压测通过 | 7 个分布式场景通过 |
| **M6** | M8 | 全面分布式 | 多 Ingestor / Query / Compactor |
| **M7** | M12 | 规模化 | Flight 查询分发、Iceberg 适配启动 |

---

## 五、WBS 任务分解

### 5.1 阶段 0：All-in-One（W1–W8）

#### Sprint 0：技术验证（W1）

| ID | 任务 | 负责 | 人日 | 依赖 | 验收标准 |
|---|---|---|---|---|---|
| **T1.0** | 项目骨架：workspace + CI + 依赖锁定 | O1 | 2 | — | `cargo build` 通过；Vortex 锁 Git Commit |
| **T1.1** | **WAL 原型**：segment 读写、CRC、组提交 fsync、崩溃恢复 | R1 | 3 | T1.0 | write 后 fsync 前 `kill -9`，重启后 CRC 拦截撕裂写入，无脏数据 |
| **T1.2** | **DataFusion Schema 适配 PoC**：两层 Adapter | R3 | 3 | T1.0 | 两个不同 schema 的 Vortex 文件，`EXPLAIN` 中 `FilterExec` 被消除 |
| **T1.3** | **Arrow Flight Source** 跑通 | R2 | 2 | T1.0 | Flight Client → `IngestBatch` 链路打通 |
| **T1.4** | `model` / `proto` 基础数据结构 | R2 | 1 | T1.0 | TableMeta / FileManifest / BatchState 定义完成 |

> **M0 准出**：4 个 PoC 全部通过。**任一 PoC 失败即进入技术攻关，不进入下一 Sprint。**

#### Sprint 1–2：WAL 与写入链路（W2–W4）

| ID | 任务 | 负责 | 人日 | 依赖 | 验收标准 |
|---|---|---|---|---|---|
| **T2.1** | WAL 完整实现：轮转（CURRENT 原子切换 + 目录 fsync） | R1 | 3 | T1.1 | 轮转后 `CURRENT` 正确；断电不回退 |
| **T2.2** | `synced_offset` 水位线 | R1 | 1 | T2.1 | 攒批线程只读 `<= synced_offset`；单测覆盖 |
| **T2.3** | **Batch 超时 + segment 清理** | R1 | 3 | T2.1 | 模拟 S3 不可用 → 30min 后 BatchAbort，segment 释放；80% 水位强制 abort |
| **T2.4** | 攒批器（按 shard+window 分组、Jitter 对齐） | R2 | 3 | T1.3 | 阈值触发正确；100 节点模拟无同秒惊群 |
| **T2.5** | Vortex 编码 + S3 写入（Multipart + upload_id） | R2 | 4 | T1.2 | 大文件 Multipart；upload_id 持久化；7 天过期能重建 |
| **T2.6** | **Ingestor 主流程**（严格时序：演进在写 S3 前） | R2 | 4 | T2.4, T2.5 | §5.2 时序实现；单测覆盖 OCC 冲突分支 |

#### Sprint 3–4：Meta 与 Schema（W4–W6）

| ID | 任务 | 负责 | 人日 | 依赖 | 验收标准 |
|---|---|---|---|---|---|
| **T3.1** | MemoryCatalog：表/文件/幂等存储 | R2 | 3 | T1.4 | CRUD + 快照可见性过滤正确 |
| **T3.2** | **Schema 演进 + OCC** | R2 | 4 | T3.1 | 并发演进只有一个成功；`CommitFiles` 不校验版本 |
| **T3.3** | **幂等键**：独立存储 + TTL 24h + 表模板 | R2 | 3 | T3.1 | Compaction 删除文件后 24h 内重试仍幂等；`true` 表未传键被拒 |
| **T3.4** | 快照隔离（`valid_from` / `deleted_at`） | R2 | 2 | T3.1 | 删除语义单测覆盖 L1/L2 |
| **T3.5** | Meta 接口按 Raft 线性语义抽象 | R2 | 2 | T3.1 | `apply`/`read_index` 抽象就绪，阶段 1 可零改动切换 |

#### Sprint 5–6：Query 与删除（W6–W8）

| ID | 任务 | 负责 | 人日 | 依赖 | 验收标准 |
|---|---|---|---|---|---|
| **T4.1** | **CatalogProvider / SchemaProvider**（本地缓存，同步） | R3 | 3 | T3.1 | `schema()` 无 gRPC；单测验证不阻塞 tokio |
| **T4.2** | **TableProvider：Manifest 驱动**（不用 ListingTable） | R3 | 4 | T4.1 | 文件来自 Manifest 缓存；不调用 S3 List |
| **T4.3** | **两层 Adapter 落地**（SchemaAdapter + PhysicalExprAdapter） | R3 | 4 | T1.2 | 跨 schema 版本查询谓词下推生效 |
| **T4.4** | 统计信息（`statistics()`） | R3 | 1 | T4.2 | 优化器能拿到行数/列统计 |
| **T4.5** | 分片级移除（L1） | R2 | 2 | T3.4 | 移除后查询不可见；物理删除延迟 24h |

#### Sprint 7：后台作业与收尾（W8）

| ID | 任务 | 负责 | 人日 | 依赖 | 验收标准 |
|---|---|---|---|---|---|
| **T5.1** | **Compaction**（schema 版本锁定 + 快照提交） | R2 | 4 | T3.4 | 合并后无数据重复；作业期间 schema 演进不影响结果 |
| **T5.2** | 孤儿清理 + GC（先排除已知文件） | R1 | 3 | T2.3 | 不误删 Meta 已知文件 |
| **T5.3** | 资源隔离（独立 blocking pool） | R1 | 1 | T5.1 | Compaction 不影响写入延迟毛刺 |
| **T5.4** | 指标 + 健康检查 + 日志 | O1 | 2 | — | Prometheus 指标；`/healthz` |

**阶段 0 小计**：约 **60 人日**（3.5 人 × 8 周 ≈ 140 人日容量，含缓冲与联调）。

---

### 5.2 阶段 0.5：核心逻辑压测（W9–W10）

| ID | 场景 | 负责 | 人日 | 验收标准 |
|---|---|---|---|---|
| **T6.1** | Compaction 期间查询 | Q1 | 2 | 无重复、无已删数据 |
| **T6.2** | 分片移除期间查询 | Q1 | 1 | `valid_from`/`deleted_at` 过滤正确 |
| **T6.3** | 孤儿清理不误删 | Q1 | 1 | Meta 已知文件不被删 |
| **T6.4** | Schema 变更 + EXPLAIN 下推 | Q1+R3 | 2 | `FilterExec` 消除 |
| **T6.5** | 幂等键 + Compaction 后重试 | Q1 | 2 | 24h 内仍幂等 |
| **T6.6** | 崩溃恢复各状态点 | Q1 | 3 | 三级状态机全部正确 |
| **T6.7** | **WAL 撕裂写入** | Q1+R1 | 2 | CRC 拦截，停止 replay |
| **T6.8** | **并发写 + fsync 前/中/后 kill** | Q1+R1 | 3 | 三种情况无丢失、无重复 |
| **T6.9** | **`synced_offset` 验证** | Q1+R1 | 1 | 攒批线程读不到未 fsync 数据 |
| **T6.10** | **Batch 超时 + segment 释放** | Q1+R1 | 2 | S3 不可用 → 30min abort，磁盘不写满 |
| **T6.11** | **磁盘水位保护** | Q1+R1 | 1 | 80% 触发强制 abort |

**阶段 0.5 小计**：约 **20 人日**。
**准出**：11 个场景 100% 通过，输出压测报告（D6）。

---

### 5.3 阶段 1：Meta 分离（W11–W16）

| ID | 任务 | 负责 | 人日 | 依赖 | 验收标准 |
|---|---|---|---|---|---|
| **T7.1** | **fjall 实现 `raft-rs::Storage`** | R1 | 8 | M3 | key 大端序 index；entries range query 高效 |
| **T7.2** | **Catalog 内存 + snapshot（异步）** | R1 | 6 | T7.1 | 持久化数据结构 O(1) 快照；`spawn_blocking` 序列化 |
| **T7.3** | gRPC 服务化（`CatalogService`） | R2 | 5 | T3.5 | 业务代码零改动切换 |
| **T7.4** | BoltDB → Raft 迁移工具 | R2 | 3 | T7.3 | 首次启动自动转换为初始 Snapshot |
| **T7.5** | 3 节点部署与联调 | O1 | 3 | T7.3 | 集群正常运行 |

**阶段 1 小计**：约 **25 人日**。

---

### 5.4 阶段 1.5：分布式压测（W17–W20）

| ID | 场景 | 负责 | 人日 |
|---|---|---|---|
| **T8.1** | Catalog 缓存落后 30s | Q1 | 2 |
| **T8.2** | Meta Raft Leader 切换 | Q1 | 2 |
| **T8.3** | 时钟回拨（手动调时间） | Q1 | 2 |
| **T8.4** | 惊群效应（100 节点 Jitter 削峰） | Q1+R1 | 3 |
| **T8.5** | 网络分区恢复 | Q1 | 2 |
| **T8.6** | Snapshot 期间 Leader 稳定性 | Q1+R1 | 2 |
| **T8.7** | Meta 吞吐 10K CommitFiles/sec | Q1+R2 | 3 |

**阶段 1.5 小计**：约 **16 人日**。

---

### 5.5 阶段 2–3（M5–M12，规划）

| ID | 任务 | 说明 |
|---|---|---|
| T9.x | 多 Ingestor 独立部署 | 幂等键已就绪，无需额外协调 |
| T9.x | 多 Query 无状态扩展 | 本地 Catalog 缓存 |
| T9.x | Compactor 独立 + 租约 | 复用 Meta Raft 协调 |
| T9.x | InfluxDB / Kafka Source | `IngestSource` trait 实现 |
| T9.x | 外部索引（Tantivy）+ 热温冷分层 | 文件级筛选 |
| T10.x | Arrow Flight 查询分发 | — |
| T10.x | 行级 UPDATE/DELETE（Iceberg MoR） | 远期 |
| T10.x | Iceberg 表格式适配 | 远期 |

---

## 六、排期甘特

| 任务 | W1 | W2 | W3 | W4 | W5 | W6 | W7 | W8 | W9 | W10 | W11–16 | W17–20 |
|---|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| T1.0 骨架 + CI | ██ | | | | | | | | | | | |
| T1.1 WAL 原型 | ██ | | | | | | | | | | | |
| T1.2 Schema 适配 PoC | ██ | | | | | | | | | | | |
| T1.3 Flight Source | ██ | | | | | | | | | | | |
| T2.1–2.3 WAL 完善 | | ██ | ██ | | | | | | | | | |
| T2.4–2.6 写入链路 | | ██ | ██ | ██ | | | | | | | | |
| T3.1–3.5 Meta + Schema | | | | ██ | ██ | ██ | | | | | | |
| T4.1–4.5 Query + 删除 | | | | | | ██ | ██ | | | | | |
| T5.1–5.4 Compaction + GC | | | | | | | ██ | ██ | | | | |
| **M0 / M1 / M2** | ◆M0 | | | ◆M1 | | | | ◆M2 | | | | |
| T6.1–6.11 Chaos 压测 | | | | | | | | | ██ | ██ | | |
| **M3 准出** | | | | | | | | | | ◆M3 | | |
| T7.1–7.5 Meta Raft | | | | | | | | | | | ██ | |
| T8.1–8.7 分布式压测 | | | | | | | | | | | | ██ |
| **M4 / M5** | | | | | | | | | | | ◆M4 | ◆M5 |

**容量校验**：
- 阶段 0 任务量约 60 人日，团队容量 3.5 人 × 8 周 = **140 人日**（含周末与缓冲），**余量充足**，可吸收技术攻关与返工。
- 阶段 0.5 约 20 人日 / 容量 35 人日，**余量 43%**（压测通常需要反复迭代）。

---

## 七、关键路径与依赖

### 7.1 关键路径

```
T1.0 骨架
  → T1.1 WAL 原型 ──┬─→ T2.1–2.3 WAL 完善 ─→ T2.6 Ingestor 主流程 ─┐
  → T1.3 Flight ────┘                                                │
                                                                      ├→ T3.1 Meta
  → T1.2 Schema PoC ──→ T4.3 两层 Adapter ──────────────────────────┘  → T6 Chaos → M3
```

**最长链**：T1.1（WAL）→ T2.x（写入）→ T3.x（Meta）→ T4.x（Query）→ T6（压测）

### 7.2 关键依赖

| 依赖方 | 被依赖方 | 说明 |
|---|---|---|
| T2.6 Ingestor | T3.2 Schema OCC | 写入需先演进 schema（严格时序） |
| T4.2 TableProvider | T3.1 Catalog | 文件清单来自 Meta Manifest |
| T5.1 Compaction | T3.4 快照隔离 | 合并依赖 `valid_from`/`deleted_at` |
| T6 压测 | 全部功能 | 必须在功能完成后 |
| T7.1 Raft Storage | M3 准出 | 核心逻辑先验证再分布式 |

### 7.3 可并行任务

- T1.1（WAL，R1）∥ T1.2（Adapter，R3）∥ T1.3（Flight，R2）—— Sprint 0 三人完全并行
- T5.1（Compaction，R2）∥ T5.2（GC，R1）—— W7 并行
- T5.4（指标，O1）∥ 任何阶段

---

## 八、风险与应对

| # | 风险 | 概率 | 影响 | 应对措施 | 责任人 |
|---|---|---|---|---|---|
| **R-1** | **DataFusion 55.x 的 Schema 适配 API 与预期不符** | 中 | **高**（谓词不下推 → 全表扫描） | W1 即做 T1.2 PoC；不符则查 `datafusion::datasource::schema_adapter` 实际签名，必要时自定义 FileSource | R3 |
| **R-2** | **Vortex API 不稳定 / 解码 bug** | 中 | 中 | 锁 Git Commit；实现 `FormatSwitch` 回退 Parquet；阶段 0 保持回退路径可用 | R2 |
| **R-3** | **自实现 WAL 并发 bug 导致数据丢失** | 中 | **高** | 阶段 0.5 专项压测（T6.7–6.11，5 个场景）；`kill -9` 真实注入；CRC 边界单测 | R1 |
| **R-4** | **组提交 fsync 顺序错误** | 低 | **高** | 代码注释标注"先推进水位，再 ack 客户端"；T6.9 专项验证 | R1 |
| **R-5** | **Batch 卡死导致磁盘写满** | 中 | **高** | 分级超时（30min + 80% 水位）；T6.10/6.11 验证 | R1 |
| **R-6** | **Snapshot 同步序列化引发 Leader flapping** | 低 | 中 | 阶段 1 用持久化数据结构 + `spawn_blocking`；T8.6 验证 | R1 |
| **R-7** | Schema 演进 OCC 实现有误 | 中 | 中 | T3.2 单测覆盖并发分支；T6.4 端到端验证 | R2 |
| **R-8** | 人力投入不足 / 成员离职 | 中 | 中 | 文档化 ADR 注释；关键模块双人熟悉（WAL 由 R1+R2 交叉 review） | 全体 |
| **R-9** | 压测发现架构级缺陷需返工 | 低 | **高** | 阶段 0.5 提前（不等阶段 1）；预留 43% 容量余量 | 全体 |

---

## 九、验收标准

### 9.1 功能验收（M2）

| # | 标准 | 验证方式 |
|---|---|---|
| 1 | Flight 写入数据，SQL 可查 | 端到端集成测试 |
| 2 | 崩溃后数据不丢（best_effort 语义内） | Chaos T6.6–6.8 |
| 3 | 重复提交不产生重复数据 | T6.5 |
| 4 | 加列后老文件仍可查，谓词下推 | T6.4 + EXPLAIN |
| 5 | 分片移除后数据不可见 | T6.2 |
| 6 | Compaction 后无数据重复/丢失 | T6.1 |

### 9.2 质量验收（M3）

| # | 标准 | 目标 |
|---|---|---|
| 1 | Chaos 场景通过率 | **100%**（11/11） |
| 2 | 单元测试覆盖率（核心模块 wal/catalog/ingest） | **≥ 80%** |
| 3 | 查询无 500 错误 | 100% |
| 4 | 数据零丢失、零重复（压测场景内） | 100% |
| 5 | 关键 ADR 均有代码注释引用 | 100%（ADR-1,3,4,6,11,12,13 + C1–C8） |

### 9.3 性能验收（参考，阶段 0 可放宽）

| 指标 | 目标 | 备注 |
|---|---|---|
| 单 Ingestor 写入吞吐 | ≥ 100K rows/s | 组提交开启 |
| 写入 P99 延迟 | ≤ 50ms | 含 WAL fsync |
| 冷查询（1TB，命中剪枝） | ≤ 5s | 依赖 Vortex 剪枝 |
| Meta 吞吐（阶段 1） | 10K CommitFiles/sec | §2.1 |

---

## 十、工程规范

### 10.1 代码规范：ADR 注释（强制）

每个非显然决策必须在代码处注释，**尤其是容易被"优化"掉的**：

```rust
// ADR-4: batch_id is random UUIDv7, NOT derived from content.
// Idempotency is guaranteed by BatchStateStore + client_request_id,
// NOT by batch_id determinism. Do NOT "optimize" this into a content hash.
let batch_id = Uuid::now_v7().to_string();

// C2: Only read WAL data below synced_offset.
// Reading beyond it may return un-fsynced data → state machine corruption
// after power loss. See detailed design §4.4.
let records = wal.scan_range(last, wal.synced_offset())?;

// C5: Catalog is the Raft state machine's materialized view.
// It lives in memory and is recovered from snapshot + log.
// Do NOT persist Catalog independently to fjall — dual-write trap (§5.4.2).
```

### 10.2 分支与评审

| 项 | 规范 |
|---|---|
| 分支 | `main`（保护）+ `feat/*` + `fix/*` |
| PR | 必须 1 人 review；WAL / Catalog / Adapter 模块需 **R1 或 R3** 复核 |
| CI | `cargo fmt` + `clippy -D warnings` + 单测 + 集成测试 |
| 提交信息 | `feat(wal): ...` / `fix(ingest): ...` / `refactor(catalog): ...` |

### 10.3 依赖管理

- **Vortex 锁 Git Commit Hash**（禁止用版本号）
- 新增依赖需说明必要性 + 许可证（**禁用 GPL/AGPL**）
- `cargo audit` 纳入 CI

### 10.4 测试分层

| 层 | 范围 | 频率 |
|---|---|---|
| 单测 | wal / catalog / format / schema | 每次 PR |
| 集成 | 端到端写入查询、幂等、演进 | 每次 PR |
| Chaos | 11 个故障场景 | 阶段 0.5 专项 + 月度回归 |
| 压测 | 吞吐、延迟 | 里程碑前 |

### 10.5 文档同步

- 实现与详细设计冲突 → **先更新详细设计文档**，再改代码
- 详细设计与架构文档冲突 → **以架构 v11 为准**，并反馈架构负责人
- 每个 Sprint 末同步一次进度与风险

---

## 附录：任务总览表（阶段 0–1.5）

| 阶段 | 任务组 | 任务数 | 人日 | 负责人 |
|---|---|---|---|---|
| 0 / Sprint 0 | T1.0–T1.4 | 5 | 11 | R1/R2/R3/O1 |
| 0 / Sprint 1–2 | T2.1–T2.6 | 6 | 18 | R1/R2 |
| 0 / Sprint 3–4 | T3.1–T3.5 | 5 | 14 | R2 |
| 0 / Sprint 5–6 | T4.1–T4.5 | 5 | 14 | R3/R2 |
| 0 / Sprint 7 | T5.1–T5.4 | 4 | 10 | R1/R2/O1 |
| **0.5** | T6.1–T6.11 | 11 | 20 | Q1 + R1/R3 |
| **1** | T7.1–T7.5 | 5 | 25 | R1/R2/O1 |
| **1.5** | T8.1–T8.7 | 7 | 16 | Q1 + R1/R2 |
| **合计** | | **48** | **128** | |

> 容量校验：128 人日 / 3.5 人 ≈ **37 工作日 ≈ 7.5 周**（纯编码），排期 20 周（5 个月）含联调、返工、压测迭代，**余量合理**。

---

**文档结束 · 开发计划任务书 v1.0**

> **启动条件**：M0 的 4 个 PoC（W1）通过后方可全面铺开。任一 PoC 阻塞即进入技术攻关，不强行推进。
