# yuntun 现状基线（Status）
> **版本**：v1.0  ｜ **日期**：2026-09-18  ｜ **维护**：里程碑 / 决策 / 缺陷状态变更时必须更新本文件
> **定位**：**唯一现状入口**。其它文档说的是"设计意图 / 任务 / 历史"，本文件说的是
> **今天实际是什么样、还缺什么、下一步做什么**。
>
> **冲突裁决顺序**：`status.md` + `operation-log.md`（证据）**>** `plan.md`（任务）
> **>** `architecture.md` / `design.md`（设计）**>** `refactor.md`（改造指南）。
> 裁决后**必须回改上游文档** —— 不允许"实现已改、文档照旧"（这是本项目最大的返工来源，
> 见 `plan.md §2.3` 与 `operation-log §25/§27`）。

---

## 1. 一句话现状

- **Standalone**：**可交付的本地时序/可观测数据库**（单二进制 + 一份 TOML 即可用，两个 SQL 协议端口）。
- **分布式**：**数据平面地基已打完**（拆进程零返工的部分）；**控制平面为 0 起步**
  （不是"把本地调用换成 gRPC"，而是新增语义：成员/租约/快照/水位传播）。
- **质量网**：214 用例全绿 / clippy 0 警告 / chaos **11/11** / T8 基线已入库 / 五个真缺陷已抓出（4 修 1 待）。
- **对外口径**：单机可实际使用；**多节点只能算实验性部署**（必须单写者 + 不开后台作业竞争）。

---

## 2. 能力矩阵（standalone 今天能做到什么）

规模：**26,765 行 Rust / 17 个 crate / 184 个测试函数 / 214 个用例通过 / clippy 0 警告**。

### 2.1 能用

| 维度 | 现状 |
|---|---|
| 交付形态 | 单二进制 `yuntun` + 一份 TOML，单命令启动；Flight SQL `:50051` + MySQL wire `:3306` 同开 |
| SQL 语义 | `crates/sql` 是**唯一实现**，两端口共用（不是两套代码）；`SELECT`/CTE/聚合/`information_schema`/8 个 JSON 函数，DataFusion 55 执行 |
| 写入 | `INSERT ... VALUES` / `INSERT ... SELECT` / Flight `DoPut` / CLI（CSV/JSONL/Parquet）；**三条入口汇入同一 ingest 管线** |
| DDL | `CREATE/DROP TABLE`、`CREATE/DROP DATABASE`；类型覆盖 `DECIMAL`/`ARRAY<T>`/`MAP(K,V)`/`JSON` |
| 元数据 | `SHOW TABLES/COLUMNS/DATABASES`、`DESCRIBE`、`SHOW CREATE TABLE` |
| 客户端 | mysql CLI / 驱动预编译 / DBeaver+JDBC / ADBC+pyarrow 均已验（T1–T3） |
| 幂等 | 三通道（SQL 注释 / CLI `--key` / `FlightData.app_metadata`）；**语义 = 一次请求一次键 + 批次派生键**（§3） |
| 持久性 | WAL 权威（CRC + 组提交 + fsync），崩溃恢复含 DDL 重放 + 非终态批次重提交 |
| 可见性 | 读己之写：fsync 后 ≤1 个 scan 周期（默认 100ms）可查，此时零已提交文件 |
| 内存 | chunk 区 / query 区**硬分区互不抢占**；60/80/95 阶梯；spill（IPC+LZ4+CRC）；95% 明确拒写 |
| 持久化上界 | **确定可预测**：`seal + max_flush_delay + phase`（默认 0 + ≤30s，实测见 §4） |
| 运维 | 周期结构化打点：内存水位 / WAL 积压 / 背压 / Catalog 版本与增量统计（`metrics()`） |
| 后台作业 | Compaction + 孤儿清理（1h 静置）+ WAL 超时监控 |

### 2.2 明确不能（**都是明确报错，不是静默错**）

`UPDATE` / `DELETE` / `ALTER TABLE` / 事务 / 视图 / 存储过程 → 报错；
MySQL 端口 **trust 无鉴权**（按网络隔离部署）；MySQL 轨结果集仍先收集后逐行写（Flight 轨已流式）；
指标只有日志、无 HTTP 导出；SQL DDL 的 `TIMESTAMP(p)` 精度不入 schema。

> `DELETE/UPDATE` 已有设计草案（`delta-dml-design.md`），M0a 前置项（终态批次重提交）已落地，**功能未实现**。

### 2.3 只有实验性可用的部分（写清楚边界，避免误用）

| 场景 | 现在能不能用 | 前提 |
|---|---|---|
| 多节点写同一分片 | ❌ | 会出现**静默重复计数**（冷热边界只在进程内，`source_instance` 只写不读） |
| 多节点跑 compaction / 孤儿 GC | ❌ | 无租约（重复合并）；GC 的"在途窗口"非多写者安全（**可能删掉别家已上传未提交的文件**） |
| 单写者多读 | ✅ | 单进程或"只有一个 datanode 在写" |

---

## 3. 已定案决策（**禁止顺手改**；改必附实测）

| # | 决策 | 值 / 契约 | 依据 |
|---|---|---|---|
| D1 | 相位分散量级 | `flush_phase_spread_secs = 30`（原 5） | T8 实测：带宽 ≈ spread；峰值提交 ≈ shards/spread（5s → **87 次/秒**，30s → **10 次/秒**）`operation-log §32` |
| D2 | flush 宽限期 | `max_flush_delay_secs = 0`（原 30） | 实测它**不减少文件数**（300 vs 301），只把上界从 5.03s 推到 35.06s → 拿持久化延迟换不到东西 |
| D3 | 持久化上界口径 | **窗口关闭 + `max_flush_delay` + `spread` + 提交路径耗时**，无随机项 | 实测四档吻合（35.06 / 5.03 / 59.41 / 31.35s）；**多节点实测 max 33.2s > 30s** → 上界须含"提交路径耗时"（实测约 +3s）；相位仍确定性（`operation-log §37.2-4`） |
| D4 | 相位分散锚点 | **`seal_time`**（不是 `window_start`） | 窗口对齐使 seal 发生在窗口关闭时刻；用 `window_start` 锚点会把该时刻重新对齐回窗口内固定偏移，分散失效 |
| D5 | 不变量 | `chunk_max_resident_secs > max_flush_delay_secs + flush_phase_spread_secs`（默认 60 > 0+30） | 违反则硬兜底先触发 → 绕过相位分散（惊群复活）而功能测试全绿；已进 `Config::warnings()` 启动自检 |
| D6 | 幂等键粒度 | **一次客户端请求一个键**，请求内第 `i` 个批次用 `derive_batch_key(键, i)`（`键#i`） | 不派生 → 同请求第 2..N 批被第 1 批判重丢掉（静默丢数据）；派生确定 → 重试**逐批**幂等 |
| D7 | 幂等预筛位置 | **ingest 入口**（写 WAL 之前）；命中即 `duplicate=true`、不写 WAL | `commit_files` 只接受单个 `client_request_id`，而一个 chunk 聚合多个键 → 提交层无法按键集合去重（属 R3 状态机） |
| D8 | Catalog 快照 | **每查询取一次不可变快照**（`Arc` 共享），规划到 scan 全程复用 | 不是性能优化而是**正确性要求**：DataFusion 的 provider 是同步 trait、规划期反复调用，读会变的结构会让同一次查询的 plan 与 scan 看到两个版本 |
| D9 | 幂等键独立存储 | 键**不随 FileManifest 生命周期消失**（compaction 删除原文件后重试仍幂等） | 否则合并后客户端重试 = 再写一份（静默重复） |
| D10 | `rows_threshold` / `bytes_threshold` | **均保持 50 万 / 128MB（定案：不改）**，但**必须知道实际效果** | `seal_reason` 实测（`operation-log §35`）：1KB 行时**有效账本口径 ≈ 4.9 KB/行** → 128MB 只够 **~2.7 万行/文件**（不是按 885B/行推出的 15 万行）；**"每窗口每 shard ≤1 文件"仅在"窗口内数据量 ≤ `bytes_threshold`"时成立**（20MB/s × 60s = 1.2GB ≫ 128MB → 26~92 文件/窗口）。`rows_threshold` 在窄行表才可能触发 |
| D11 | **chunk 预算定容规则** | `chunk_mem_budget ≳ 写入速率 × (写入→flush 的滞留)`，其中滞留含"窗口对齐等待 + `md` + `spread`" | 实测 20MB/s resident ~437~470MB（滞留 ≈22s）；低速 2MB/s → Normal。**⚠️ 本轮修正**：`spread` 只影响延迟与压力档位（5s→6.6s、30s→28.6~37.5s），**不影响文件大小** → §33 的"水位 ≈ 速率 × spread"因果**已被推翻**（`operation-log §34.1`） |

**规则**：以上任何一项要改，必须先在 `operation-log` 里附**实测数据**（`plan.md §2.2` 的"先有实测再改默认值"）。

---

## 4. 质量与证据网（今天有什么）

| 层 | 证据 | 位置 |
|---|---|---|
| 单元 / 集成 | **214 passed / 0 failed**；184 个测试函数；`clippy --workspace --all-targets` 0 警告 | `cargo test --workspace` |
| chaos（真实磁盘 + 跨重启 + 并发） | **11/11** 场景；进程中抓出**五个真缺陷** | `crates/chaos` 模块文档 + `operation-log §27–§31` |
| 性能基线 | 提交时刻分布 / 峰值提交数 / `seal→committed` / 文件数·天 / 单文件行数 | `operation-log §32`（`bench_baseline`） |
| 阈值与背压基线 | 账本口径 B/行 / 内存水位峰值 / 背压档 / RowGroup 数 / 单文件字节 | `operation-log §33`（同程序，`pad`/`bytes_threshold`/读者开关） |
| 多节点基线 | 全局提交时间线（峰值 9 次/秒 @4 节点）、争用下的 seal 构成、相位让位触发 | `operation-log §37`（`scripts/bench_multi.sh`） |
| 吞吐 | `bench.rs`（E1 目标 8w 行/秒） | `crates/chaos/examples/bench.rs` |

**五个真缺陷（都"不报错、只产出错数据"，功能测试全绿时抓到的）**：

| # | 缺陷 | 状态 |
|---|---|---|
| 1 | 幂等键完全不生效（键被丢弃 / 提交不带键 / 无入口预筛）→ 重试 = 静默重复 | ✅ §27 已修（并修出第二层"键粒度错配"） |
| 2 | 恢复产出重复文件 | ✅ §28.2 已修 |
| 3 | WAL 撕裂不可自愈 | ✅ §29.1 已修 |
| 4 | 监控 abort 后不同步视图 → segment 永不释放 / 重复写 | ✅ §30 已修 |
| 5 | 提交成功后、标记前崩溃 → 重复计数 | ⏳ 待修（需读侧栅栏，与 R3 同批） |

**已知 flaky**：`yuntun-chaos` 与其余 46 个 test binary 并行争 CPU/IO → 30s 恢复上限不足（单跑 1.3s）。
已放宽到 60s 并写明理由；根治（chaos 独立跑 / 去 flaky）属 `plan.md` T6.1。

---

## 5. 缺口（按"证据强度 × 后果"分层）

### 5.1 四类**静默错数据**（今天拆进程就会发生）

| # | 失败模式 | 现有防线 |
|---|---|---|
| 1 | 重复计数（多实例各自 flush 同一分片） | ❌ 无（冷热边界只在进程内） |
| 2 | 元数据风暴 / Catalog 丢状态 | ❌ 无（权威仍在单机内存，重启靠本地 WAL 重放 DDL） |
| 3 | Compaction 互相踩 | ❌ 无租约 |
| 4 | 孤儿 GC 误删别家"已上传未提交"的文件 | ⚠️ 部分（1h 静置 + **进程内** `first_seen`） |

### 5.2 分布式语义缺口（详见 `plan.md §5.1` 记分卡）

控制平面不存在（`MemoryCatalog` 纯内存）｜`source_instance` **只写不读**｜无 fanout / partial agg /
`epoch`·watermark 校验｜`deleted_at` 未启用｜后台作业无全局化。

### 5.3 度量与刻画缺口

chaos 已 11/11，但**基线压测只覆盖单进程**：真多节点 CommitFiles 瞬时并发、真实 S3 PUT 绝对延迟、
内存曲线时序、离群提交归因（`operation-log §32.4`）均未测。

### 5.4 已收敛：文件数由"窗口内数据量 ÷ `bytes_threshold`"决定（不再是疑点）

`FileManifest.seal_reason` / `seal_pressure` 落地后实测（`operation-log §35`，20MB/s、1KB 行）：

```
bytes_threshold  files=35（水位 Normal 33 / Soft 5 / Hard 2）
pressure         files= 2
window_closed    files= 3
```

**结论**：`bytes_threshold` 是主因，**"内存压力主导"不成立**（§33/§34 的候选被否）。
1KB 行的**有效账本口径 ≈ 4.9 KB/行** → 128MB ≈ **2.7 万行/文件**；
`文件/窗口/shard ≈ max(1, 窗口内数据量 ÷ bytes_threshold)`。
所以"每窗口每 shard ≤1 文件"（ADR-10 目标）**只在 `窗口内数据量 ≤ bytes_threshold` 时成立** ——
高吞吐下必然多文件（20MB/s × 60s = 1.2GB）。**这是阈值与速率的算术关系，不是缺陷**，
但 **ADR-10 的措辞需要加这个限定语**。

~~另：Parquet 写入未设 `max_row_group_size`~~ ✅ **已修**（`operation-log §36`）：显式设为 **65,536 行**
（此前用 crate 默认约 100 万行 → 整文件 1 个 RowGroup，剪枝粒度=文件）。
端到端实测：15 万行的文件 = **3 组**、6 万行 = 1 组；单测钉住"分组数 = ceil(行数/上限) 且每组不超上限"。
对当前文件规模（1KB 行 ≈ 2.7 万行/文件）**行为不变**，只在大文件时防止退化成单组。

---

## 6. 下一步（顺序不可颠倒；每步附准入）

| # | 事项 | 准入 / 验收 | 为什么是这个顺序 |
|---|---|---|---|
| 1 | **ADR-10 措辞修正**：加"当窗口内数据量 > `bytes_threshold` 时为多个文件/窗口"的限定语 | 与实测一致（§5.4） | 它是当前唯一"文档与现实不符"的架构承诺；不改会让后来者以为实现有 bug |
| 2 | 让文件数可预期：`bytes_threshold` 是否随速率自适应，或暴露"目标文件行数"配置 | 用户设的是字节、观测到的是行数，口径不直观 | 属易用性/可运维性 |
| 3 | `max_row_group_size` 专项 | 显式设定（实测现状 = 整文件 1 组）→ 需测 RowGroup 大小对扫描剪枝/压缩率/写入内存的影响 | 与文件大小互为约束 |
| 4 | ~~真多节点基线压测~~ ✅ **本机多进程已完成**（`operation-log §37`） | 4 节点 × 20k rows/s：全局峰值 **9 次/秒**、`pressure` 主导 60%、相位让位 2~3 次/节点生效 | **剩余**：R3 后打同一 Meta 的真并发（唯一的硬门槛）+ 真实 S3/MinIO + 跨机 + 内存曲线时序 |
| 3 | **R3：metanode 独立 + raft**（**设计已定稿** → [`metanode-design.md`](metanode-design.md)） | M3：3 节点写入不中断 + metanode 全量重启后 Catalog **逐字节一致** + standalone 不回归（217 用例全绿、`if distributed` 零命中） | 语义零改动（R2 已把访问形态按远程定义），只换状态机宿主；**开工第一件事 = S3-0（proto/tonic）+ S3-2（`CatalogState` 抽取 + 确定性对拍）并行** —— 后者是 R3 最高风险的落地处（非确定性会让副本静默分叉） |
| 4 | **R4：datanode 化 + 冷热边界** | M4：多 datanode 并发写 + 查询结果**与单节点串行精确相等**（对拍，硬要求） | 这一步才消费 `source_instance` → 消灭缺口 §5.1-1（重复计数） |
| 5 | **R5：分布式并发查询** | M5：fanout 下对拍继续成立；查询中杀节点行为符合声明 | 依赖 R4 的分片归属 |
| 6 | **R6：compaction / GC 全局化** | M6：文件数收敛 + **开 GC 的多节点压测零误删** + 租约可接管 | 最后做：它需要前四步提供的一致性基础 |

**工期参考**：到"可以对外说分布式就绪"（M1–M6 全达成）约 **3.5–5 个月**，其中
**实验墙钟时间（chaos / 压测 / 对拍 / 误删专项）占一半**，不可用编码速度压缩（`plan.md §8.5`）。

---

## 7. 文档地图（谁是权威、哪里已过时）

| 文档 | 定位 | 时效（2026-09-18） | 本轮动作 / 欠账 |
|---|---|---|---|
| **`status.md`（本文）** | 现状基线 | ✅ 最新 | 新建 |
| `operation-log.md` | 实施日志 + **偏差与证据**（§1–§32） | ✅ 最新 | 标题/定位已扩到阶段 0–2 |
| `plan.md` | 开发计划任务书（阶段 WBS / 里程碑 / 风险） | ✅ 已同步 | v2.2：依据改 v12、T6.13/T8/ADR-10 状态、P0 表 |
| `architecture.md` | 架构设计（12 个 ADR + 域设计） | ✅ 已修版本漂移（原头部 v10 / 正文 v12 / 文末 v11） | v12：头部+文末统一、ADR-10 正式修订、§3.2 补 `yuntun-chunk` 依赖图、ADR-3 补**节点私有状态清单**（WAL + spill）；**欠**：§4 ADR-9 的 RPO 口径表述 |
| `design.md` | 详细设计（模块/接口/状态机/配置） | ⚠️ 部分章节是阶段 0 意图，实现有偏差 | v1.2：指向 status + 声明"实现偏差以 operation-log 为准"；**欠**：§11 `[chunk]` 段与 `idle_timeout` 整段重写 |
| `refactor.md` | 分布式改造指南（S0–S6 / R0–R6 怎么改） | ✅ S1/S2 已完成，S3–S6 未开始 | 加版本头与状态列 |
| `architecture-with-chunk.md` | chunk 层目标态架构（v2） | ✅ 有效（`plan.md` 依据之一） | **部分实现**：已标注与 `architecture.md` v12 的关系；§5.3 补"seal 是窗口对齐"限定语 + 相位量级定案 |
| `sql-access-design.md` | 多协议接入设计 | ✅ 有效 | — |
| `delta-dml-design.md` | DELETE/UPDATE 设计草案 | ✅ 有效（未实现） | — |
| `README.md` | 对外门面 | ⚠️ 引用 `plan.md v2.0` | 引用改 v2.2 + 指向 status.md |

---

## 8. 维护规则（防止再次漂移）

1. **改默认值必须附实测**（`plan.md §2.2`）。已发生过的反例：ADR-10 原文写"随机 jitter + `window_start` 锚点"，
   实现是"确定性相位 + `seal_time`"→ 若不改原文，下一位实现者会**按文档改回违例版本**。
2. **凡"已具备"的能力，必须有一条断言其反面后果的用例**（§27 幂等键的教训：功能测试全绿 ≠ 语义正确）。
3. **新增事实先写 `operation-log`（含证据），再回改 `status.md` / `plan.md` / `architecture.md`**。
   顺序颠倒会出现"计划已绿、实现没做"。
4. **版本号规则**：每个文档的头部版本、正文版本演进、文末版本必须一致（`architecture.md` 曾三处不一致）。
5. **不新建整份平行文档**：设计与计划就地升版本；只有"现状"这类新维度才新开文件（本文）。
