# yuntun — 开发计划任务书 v2.2（质量与性能收尾 → 分布式化）

> **依据**：《通用直写数据湖架构设计 v12》（`architecture.md`）+《详细设计文档 v1.2》（`design.md`）
> + **《架构设计（含 chunk 层）》（`architecture-with-chunk.md`，下文简称 **chunk 版架构**）**
> + **《重构路线 S0–S6》（`refactor.md`）**
> + **《阶段 0 实现操作日志》§25**（2026-09-15，chunk 层落地与对既有设计的逐条对照审查）
> **版本**：v2.2
> **日期**：2026-09-18
>
> **v2.2 相对 v2.1 的核心变更**：T8 基线压测入库 + **P0 ①/③ 定案**（`max_flush_delay_secs=0`、
> `flush_phase_spread_secs=30`）并**正式修订 ADR-10**（v12）；chaos **11/11**；P0 ② `rows_threshold`
> P0 ② 亦**已定案**（保持 50 万/128MB；`operation-log §35` 用 `seal_reason` 实测定性）。
**现状总览见 [`status.md`](status.md)**。
> **适用范围**：阶段 1（Standalone 完备）、**阶段 1.5（数据平面地基，已完成）**、
> 阶段 2（质量与性能）、阶段 3（分布式化）、阶段 4（规模化）
>
> **v2.1 相对 v2.0 的核心变更**：
>
> 1. **插入「阶段 1.5：数据平面地基（chunk 层）」并已完成**（对应 `refactor.md` 的 S1-1 ~ S1-10）。
>    次序判断（**先数据平面（chunk）再做控制平面（raft）**）经 `operation-log §25.2`
>    逐条对照既有铁律与 ADR，结论是**同一架构内的补齐与收敛，未推翻任何铁律**；
>    唯一真冲突是 chunk 版架构 §5.3 与 `ADR-10` 的表述冲突，**已在实现层修正**（§2.4）。
> 2. **待定决策（P0/P2）显式入册**（§2.2）：相位分散量级与 `max_flush_delay` 默认值
>    必须实测后定案——现在拍板会把 ADR-10 的削峰目标静默改掉。
> 3. **文档同步项入册**（§2.3）：ADR-10 正式修订、crate 图补位、节点私有状态清单
>    （WAL 目录 + **spill 目录**）——这些是"不阻断功能、但会让后来者误判"的漂移。
> 4. **命名澄清**：`refactor.md` 的重构步骤 `S0–S6` 在本文记为 **R0–R6**，
>    避免与本文既有编号（阶段 1 任务 `S1.x`、阶段 2 `T6.x/T7.x/T8.x/T9.x`、
>    阶段 3 `T10.x`）混淆——两套 "S1" 曾同时存在，是本轮实测到的真实歧义源。
>
> **v2.0 相对 v1.0 的核心变更（保留）**：**分布式整体后移**。先把 standalone 做成一个
> 功能完备、可独立交付的本地时序/可观测数据库，分布式（Meta Raft 分离、多节点）作为
> 最后一公里的自然扩展。原则延续 v9 评审结论：核心逻辑先在单进程验证，
> 分布式只做网络化与扩展性，不重写逻辑。
> **v2.1 对该原则的修订**：*不重写逻辑* 仍然成立，但**存储/写入路径的中间层需要先补齐**
> ——chunk 层（内存热数据的有界化 / 卸载 / 确定性落盘）是分布式阶段"多 datanode 各自
> flush"的前提，故提前到阶段 1.5；catalog / metanode 仍按 v2.0 的次序后移。

---

## 目录

1. [路线调整说明](#一路线调整说明)
   - [1.4 三条边界澄清](#14-v21-的三条边界澄清避免后续阶段误判)
2. [阶段划分](#二阶段划分)
   - [2.1 阶段总览（旧阶段 ↔ R0–R6）](#21-阶段总览旧阶段--重构步骤-r0r6--状态)
   - [2.2 待定决策（P0/P2）](#22-待定决策定案门槛先有实测再改默认值)
   - [2.3 文档同步项](#23-文档同步项不阻断功能但会让后来者误判)
   - [2.4 阶段 1.5 WBS：数据平面地基（chunk 层）✅](#24-阶段-15-wbs数据平面地基chunk-层--已完成)
3. [阶段 1 WBS：Standalone 完备](#三阶段-1-wbsstandalone-完备)
4. [Flight SQL 接入设计（阶段 1 核心）](#四flight-sql-接入设计阶段-1-核心)
5. [**分布式就绪度基线（现状核查）**](#五分布式就绪度基线现状核查)
   - [5.1 记分卡](#51-记分卡四组)
   - [5.2 接缝现状](#52-接缝现状哪些抽象已到位哪些是旁路)
   - [5.3 如果今天拆进程：四类静默错数据](#53-如果今天拆进程四类静默错数据)
6. [阶段 2 WBS：质量与性能](#六阶段-2-wbs质量与性能)
7. [阶段 3 WBS：分布式化（R2–R6）](#七阶段-3-wbs分布式化r2r6)
8. [**关键路径与执行顺序**](#八关键路径与执行顺序)
   - [8.1 依赖关系](#81-依赖关系不可颠倒的部分)
   - [8.2 可并行项](#82-可并行项)
   - [8.3 里程碑与门槛](#83-里程碑与门槛)
   - [8.4 何时可以对外说"分布式就绪"](#84-何时可以对外说分布式就绪)
   - [8.5 工期重估](#85-工期重估refactormd-14-要求s1-完成后重新评估)
9. [风险与应对](#九风险与应对)
10. [验收标准](#十验收标准)

---

## 一、路线调整说明

> v2.0 的三条决策（D-1/D-2/D-3）持续有效；v2.1 在其上补充阶段 1.5 与三条边界澄清（§1.4）。

### 1.1 三条决策（v2.0，持续有效）

| # | 决策 | 理由 |
|---|---|---|
| **D-1** | **工程结构**：`bins/` 撤销并入 `crates/`，`all-in-one` 更名 **`standalone`**（bin 名 `yuntun`） | 架构 §3.2 本就要求"按状态切分"；每个可执行体独立成 crate，分布式阶段直接增 `yuntun-meta` / `yuntun-ingestor` 等 crate，`standalone` 保留为"全组件参考装配" |
| **D-2** | **先把 standalone 做成"能用"的数据库**：数据写入 + 查询闭环，支持**标准 Flight SQL 客户端**或**自有客户端**完成 `INSERT` / `SELECT` | 阶段 0 已有 Flight DoPut + 自定义 ticket 查询，但标准 Flight SQL 客户端（pyarrow / ADBC / JDBC）无法接入；"可用性"优先于"架构完备性" |
| **D-3** | **除分布式外的一切能力在 standalone 内完成**：Flight SQL、SQL DML、Vortex、Compaction 完整闭环、GC、Chaos 压测、性能调优 | 分布式化的收益依赖单机功能正确性；单机阶段做完，分布式只剩网络化改造 |

### 1.2 与 v1.0 的阶段对照

| v1.0 阶段 | v2.0 阶段 | 状态 |
|---|---|---|
| 阶段 0：All-in-One（PoC + 写入查询链路） | 阶段 0（不变） | ✅ **已完成**（2026-09-08，66 tests / 0 failed） |
| —（无） | **阶段 1：Standalone 完备**（crate 重构、Flight SQL、SQL 写入、自有客户端、遗留清偿） | ✅ 主要项已完成 |
| —（无） | **阶段 1.5：数据平面地基（chunk 层）** | ✅ **已完成**（2026-09-15，189 tests / 0 failed；§2.4） |
| 阶段 0.5：核心逻辑压测（Chaos） | 阶段 2：质量与性能（范围不变，仍在 standalone 内） | 顺序后移；**并承担 §2.2 的定案** |
| 阶段 1：Meta 分离 Raft | 阶段 3：分布式化 | **整体后移**（先做 Catalog 冻结 R2） |
| 阶段 1.5：分布式压测 | 并入阶段 3 | 后移 |
| 阶段 2–3：全面分布式 / 规模化 | 阶段 4：规模化 | 后移 |

### 1.3 Crate 结构调整（D-1）

```
yuntun/                                  # 现在 → 目标
├── crates/
│   ├── yuntun-model/       # 核心数据模型（最底层）
│   ├── yuntun-proto/       # 元数据 / WAL 消息（阶段 3 启用 tonic-build）
│   ├── yuntun-wal/         # 自实现 WAL
│   ├── yuntun-store/       # 对象存储抽象 + 分片**读侧接缝**（ShardReader/ShardFetch/DiskShard）
│   ├── yuntun-chunk/       # ★ 阶段 1.5 新增：chunk 层（热数据/内存账本/背压/spill/确定性 flush）
│   ├── yuntun-format/      # Parquet（默认）/ Vortex（feature）
│   ├── yuntun-catalog/     # 内存 Catalog（快照隔离 / OCC / 幂等）
│   ├── yuntun-ingest/      # 写入管线（RecordBatch → WAL → chunk → flush）
│   ├── yuntun-query/       # DataFusion 桥接 + Manifest 驱动 scan
│   ├── yuntun-compaction/  # 合并 + 孤儿判定
│   ├── yuntun-server/      # 节点层：协议端口（Flight/FlightSQL）+ 装配 + 路由
│   ├── yuntun-chaos/       # 故障注入工具
│   ├── yuntun-standalone/  # ★ 原 bins/all-in-one，bin 名 yuntun
│   └── yuntun-client/      # ★ 新增：Rust SDK + CLI（bin 名 yuntun-cli）
└── docs/
```

**依赖方向（严格单向，`operation-log §25.2` 已核）**：

```
standalone → server → ingest → chunk  → store → model
                             → wal    → model
                    → query  → store, catalog → model
                    → compaction, sql, sqlwire → …
```

`query` **不依赖** `ingest` / `chunk`：热数据经 `store::ShardReader` 注入
（分离部署换成 `RemoteShard`，调用方零改动）。

**分布式阶段（阶段 3）再增**：`yuntun-meta`（Raft 服务）、`yuntun-ingestor`、`yuntun-queryd`、
`yuntun-compactor`——全部复用 `standalone` 已验证的组件，只替换装配与网络层。

**Workspace 成员变更清单**：

```toml
# 已完成（v2.0）：移除 "bins/all-in-one"，新增
"crates/standalone",
"crates/client",
# 已完成（v2.1 / 阶段 1.5）：
"crates/chunk",
```

### 1.4 v2.1 的三条边界澄清（避免后续阶段误判）

1. **chunk 不是"第二个真相源"**：WAL 仍是唯一权威，chunk/spill 都是**可丢弃副本**
   （spill 头记 WAL 引用 + CRC，校验失败即丢弃重来）。任何"用 chunk 对账"的设计都是错的。
2. **节点私有状态有两处**：WAL 目录 **+ spill 目录**。备份、迁移、磁盘水位、盘满排查
   都必须同时覆盖（§2.3-3）。
3. **flush 时刻是"确定 + 相位分散"**，不是"随机"：`seal_time + max_flush_delay + phase(instance)`。
   `phase` 的量级决定 ADR-10 的削峰效果（§2.2 的 P0），**改它等于改架构承诺**。

---

## 二、阶段划分

### 2.1 阶段总览（旧阶段 ↔ 重构步骤 R0–R6 ↔ 状态）

| 阶段（本文） | 重构步骤（`refactor.md`，本文记 R*） | 内容 | 状态 |
|---|---|---|---|
| 阶段 0 | — | 写入/查询链路贯通（All-in-One） | ✅ 2026-09-08 |
| 阶段 1 | — | Standalone 完备（crate 重构 / Flight SQL / SQL 写入 / CLI / 遗留清偿） | ✅ 主要项已完成 |
| **阶段 1.5** | **R1（← R0）** | **数据平面地基：chunk 层**（内存热数据有界化 / spill / 背压 / 确定性落盘 / scan 接缝） | ✅ **2026-09-15**（§2.4） |
| 阶段 2 | R0 收尾、R1 收尾 | 质量与性能：Chaos 11 场景 + 压测 baseline + Vortex + **观测指标**；**本阶段同时承担 §2.2 的定案**（阶段 3 的准入门槛） | 部分：观测已落地（T6.12 ✅）；**chaos 已 11/11 齐**（§27 / §28.2 / §29.1 / §30 已修，§28.1 待与 R3 同批）；**T8 基线已入库、P0 ①/③ 已定案、ADR-10 已修订**（`operation-log §32`）；**剩余 = 真多节点压测**（P1 已定性：文件数由"窗口内数据量 ÷ `bytes_threshold`"决定，`.operation-log §35`） |
| 阶段 3 | **R2 → R3 → R4 → R5 → R6** | 分布式化：Catalog 访问形态与抽象补位（R2）→ metanode/raft（R3）→ datanode 化 + **冷热边界按实例**（R4）→ 分布查询/对拍（R5）→ **compaction/GC 全局化**（R6） | **R2 已完成（7/8，见 §7.1）**；R3–R6 待 R2 余项 + 阶段 2 准出（就绪度基线见 §五） |
| 阶段 4 | R6 之后 | 规模化：外部索引、Iceberg 等（`refactor.md` 未覆盖，留待重新评估） | 待定 |

```
阶段 0  ✅ 写入/查询链路贯通（All-in-One）
  │
阶段 1  ✅ Standalone 完备 —— "能用的数据库"
  │      S1.1 crate 重构（D-1）
  │      S1.2 Flight SQL 标准服务端
  │      S1.3 SQL 写入路径（INSERT / DDL）
  │      S1.4 自有客户端 yuntun-cli
  │      S1.5 阶段 0 遗留事项清偿
  │
阶段 1.5 ✅ 数据平面地基（chunk 层）—— 分布式的前置；见 §2.4
  │
阶段 2  质量与性能 —— 原阶段 0.5（Chaos 11 场景 + 压测 + Vortex）
  │      T6.12 观测指标 / T6.13 P0 定案 / T6.14 chunk 压力专项
  │      ⚠️ 本阶段是阶段 3 的**准入门槛**，不是"打磨"
  │
阶段 3  分布式化 —— 原阶段 1 / 1.5，拆成 5 步（见 §七）
  │      R2 Catalog 冻结（含抽象补位 T10.7）
  │      R3 metanode + raft
  │      R4 datanode 化 + 冷热边界按实例（消费 source_instance）
  │      R5 分布查询 + 对拍
  │      R6 compaction / GC 全局化
  │
阶段 4  规模化 —— 原阶段 2 / 3（外部索引、Iceberg 等；refactor.md 未覆盖，留待重估）
```

### 2.2 待定决策（**定案门槛：先有实测，再改默认值**）

> 这些项**当前不拍板**：它们的取值会静默改变既有架构承诺（ADR-10 削峰 / ADR-9 RPO），
> 必须由阶段 2 的 baseline 数据决定。在此之前保持现有默认值 + 不变量守护。

| # | 决策项 | 现状 | 定案所需证据 | 期限 |
|---|---|---|---|---|
| **P0** ✅**已定案** | `flush_phase_spread_secs`（5s → 30s）与 `max_flush_delay_secs`（30s → 0） | **已改为 30s / 0**（`ingest` + `chunk` + `server` 默认值 + `yuntun.toml.example` + ADR-10 原文） | 100 shard / 500 行/秒（每 shard 300 行/分钟）实测（`operation-log §32`）：**提交带宽 ≈ spread**（配 5s 实测 4.86s）；**峰值提交 ≈ shards/spread**：spread=5 → **87 次/秒**（均值 2.4 的 36 倍），spread=30 → **10 次/秒**；`seal→committed` p99：现值 35.1s → **新值 31.4s（更好）**；宽限期不减少文件数（300 vs 301）→ 定案 `md=0/spread=30` **两个维度都优于现默认** | ✅ 阶段 2 首轮完成 |
| **P0** ✅**已定案** | `rows_threshold` 默认 50 万是否合适 | **保持 50 万 / 128MB（均不改）** | `seal_reason` 实测定性（`operation-log §35`，20MB/s、1KB 行）：40 个文件里 **35 个是 `bytes_threshold`**（水位 **Normal** 33/40）、2 个 pressure、3 个 window_closed → **不是内存压力，是字节阈值**；1KB 行的**有效账本口径 ≈4.9 KB/行** → 128MB ≈ **2.7 万行/文件**；`文件/窗口/shard ≈ max(1, 窗口内数据量 ÷ bytes_threshold)`。⚠️ 早先"885 B/行 ⇒ 15.2 万行"与"内存水位接管"两处判断**已被 §34/§35 推翻** | ✅ 阶段 2 完成 |
| **P1** ✅**已定性** | 高吞吐下每窗口每 shard 26~92 文件、`seal→committed` 28.6~49.3s（承诺 30s） | `FileManifest.seal_reason` + `seal_pressure` 已落地（tag 18/19） | **实测定性**（`operation-log §35`）：40 个文件里 **35 个是 `bytes_threshold`**（水位 **Normal** 33/40）、2 个 pressure、3 个 window_closed → **不是内存压力，是字节阈值**；1KB 行的**有效账本口径 ≈4.9 KB/行** → 128MB ≈ 2.7 万行/文件；`文件/窗口/shard ≈ max(1, 窗口内数据量 ÷ bytes_threshold)` → 20MB/s×60s=1.2GB 必然多文件。**剩余动作 = 修 ADR-10 措辞 + 口径易用性**（不再是"缺陷待查"） | 修饰语阶段 2 |
| **P2** | spill 复用（校验 WAL 引用一致则沿用副本）替代"丢弃重来" | 当前丢弃重来（正确但重启后重新编码） | 重启恢复耗时占比（大 WAL 场景） | 阶段 2 |
| **P2** | `chunk_max_resident_secs`（60s）与 WAL 物理回收的关系 | 强制 seal+flush 已实现；segment 清理闸门仍是 R21 遗留 | WAL 磁盘占用峰值 vs 写入速率 | 阶段 2 |
| **P2** | 内存压力下是否跳过墓碑期（架构 §4.6 允许） | 当前不跳（换来零可见性空洞） | 极端内存压力下的可用性测试 | 阶段 3 前 |
| **P2** | **冷热边界按实例的协议形态**：各实例的 flush watermark 如何传播 / 消费（推送 vs 查询时拉取） | `FileManifest.source_instance` 已写但**无消费者**（§5.1-C） | R4 设计评审（两种形态的查询放大对比） | **R4 开工前** |
| **P2** | **compaction 租约方案**：meta 租约 vs 独立 coordinator | 无租约（单进程后台任务） | R6 设计评审 + 双执行者注入测试 | **R6 开工前** |
| **P2** | 孤儿 GC 的"在途窗口"判定方式（上传时间 + 提交时间 vs GC 侧标记） | 1h 静置 + 进程内 `first_seen`（非多写者安全） | R6 误删专项设计 | **R6 开工前** |

**不变量（必须始终成立，`Config::warnings()` 启动自检）**：

```
chunk_max_resident_secs > max_flush_delay_secs + flush_phase_spread_secs
```

违反时"驻留硬兜底"会早于正常到期触发，**绕过相位分散** → 所有实例重新在同一秒 flush
（ADR-10 的惊群问题复活），而功能测试全绿。

### 2.3 文档同步项（不阻断功能，但会让后来者误判）

| # | 项 | 目标文档 | 期限 |
|---|---|---|---|
| 1 | ~~**ADR-10 正式修订**~~ ✅ **已完成**（v12）：锚点 `seal_time`、确定性相位、量级 `md=0`/`spread=30s` + 定案实测表；并明确"每窗口每 shard ≤1 文件"不是硬不变量（阈值触发/延迟到达会破） | `architecture.md` §4 + `design.md` §5.3/§11 | ✅ 已完成 |
| 2 | ~~crate 图补 `yuntun-chunk`~~ ✅ 已完成（`architecture.md` §3.2 已补 crate 列表 + 依赖方向） | `architecture.md` §3.2 | ✅ |
| 3 | ~~**节点私有状态清单**~~ ✅ 已完成（ADR-3 处补了 WAL 目录 + spill 目录表，并写明"其余一切可重建"） | `architecture.md` §4 ADR-3 | ✅ |
| 4 | **ADR-9 / README 已知限制**：`best_effort` 的 RPO 口径 = "窗口关闭 + `max_flush_delay` + `spread`"（默认 ≤90s；实测见 `operation-log §32`），并加限定语"延迟到达的批次可能再多一个窗口" | `architecture.md` §4 / README | 阶段 2 |
| 5 | ~~`architecture-with-chunk.md` §5.3 补限定语"seal 触发是窗口对齐的"~~ ✅ 已完成（顺便标了量级定案与与 `architecture.md` v12 的关系） | `architecture-with-chunk.md` | ✅ |
| 6 | ~~详细设计 §11 配置清单同步~~ ✅ `[ingest]` 段已改（含删除 `flush_jitter_seconds`）；**欠**：`[chunk]` 段与 `idle_timeout` 待整段重写 | `design.md` §11 | 部分完成 |

### 2.4 阶段 1.5 WBS：数据平面地基（chunk 层）—— ✅ 已完成

> 对应 `refactor.md` 的 S1-1 ~ S1-10；落地细节与对照审查见 `operation-log §25`。
> **次序判断**：先数据平面（chunk）再做控制平面（raft）——理由是多 datanode 各自 flush
> 需要"每实例的冷热边界"（`FileManifest.source_instance`）与"内存有界"这两个前提，
> 否则分布式阶段会把单机阶段的内存/小文件问题放大 N 倍。

| ID | 内容 | 验收 |
|---|---|---|
| S1-1/2 | `yuntun-chunk`：`Chunk`（内存直接持 `RecordBatch`）+ 五态机（非法转移报错） | ✅ 32 项单测 |
| S1-3 | seal 策略**单点**：行数 / 字节 / **窗口关闭** / `schema_version` 变化 | ✅ 窗口对齐用例 |
| S1-4 | spill = 本地磁盘 + Arrow IPC(LZ4) + 头记 `(wal_segment, seq_range, crc32, schema)` | ✅ CRC 篡改/截断/魔数拒绝 |
| S1-5/6 | 内存账本 + 60/80/95 背压阶梯 + chunk/query **硬分区**（query 侧接 DataFusion 内存池） | ✅ 阶梯与硬分区用例 |
| S1-7 | 读侧接缝：`ChunkStore: ShardReader`（查询侧零改动，"读己之写"保住） | ✅ seam 用例 + 既有 `hot_shard_reader` |
| S1-8 | WAL 回收点语义：chunk 持 `[seq_start, seq_end)` 半开区间 | ✅ flush e2e |
| S1-9 | 强制 seal **且 flush** 的最大驻留（取代旧 `idle_timeout`） | ✅ 同轮 seal+flush 用例 |
| S1-10 | `rows_threshold` 1 万 → 50 万 | ✅ 配置默认 + 映射用例 |
| 附带 | `FileManifest.partition_key` / `source_instance`（架构 §2.3 / §4.4，**必须提前加**） | ✅ Manifest 字段 |
| 附带 | 收敛 `MemoryShard`/`WindowGroup` 两套分组语义为一套（chunk） | ✅ 删除旧实现与用例迁移 |
| 附带 | 修复 3 项审查发现（表标识不一致 / spill 跨重启泄漏 / 强制 seal 只做一半） | ✅ `operation-log §25.3` |

**准出证据**：`cargo test --workspace` 189 passed / 0 failed；`clippy --all-targets` 0 警告；
`write_then_read`（读己之写 latency + 零已提交文件）、`hot_shard_reader`（远端 `ShardReader`
可替换）、`m0a_recommit`、chaos 3 场景全绿。

**本阶段未做（明确留待）**：相位分散量级定案（P0）、spill 复用（P2）、
`chunk::ColumnStats` 尚未接入 query 的块级跳过（阶段 3/4）、
内存/WAL 积压三项指标未接观测（`refactor.md` S1-11）。


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

## 五、分布式就绪度基线（现状核查）

> **本节是 v2.1 的核心新增**：不问"做了什么"，只问"**现在能不能拆**"。
> 核查时点：2026-09-16（阶段 1.5 完成后）。依据 `operation-log §25` 与代码实读。

**一句话结论**：**拆进程的准备工作做完了，可以开始写控制平面了**——
但"可以分布式"还差 **R2~R6**（`refactor.md` 的 S2~S6）。
**数据平面 ✅ / 控制平面 ❌ / 分布式语义 ❌ / 质量刻画 ❌**。

### 5.1 记分卡（四组）

**A. 数据平面（写入 / 读侧）—— ✅ 就绪**

| 能力 | 状态 | 证据 |
|---|---|---|
| 写入唯一路径（WAL 权威）+ 内存有界 + 确定性落盘 | ✅ | `yuntun-chunk`；189 tests |
| 节点私有状态边界清晰（WAL 目录 + spill 目录） | ✅ 代码 / ⏳ 文档 | `plan.md §2.3-3` |
| 读侧接缝可替换（`query` 对 `ingest`/`chunk` 零依赖） | ✅ | `hot_shard_reader`（远端实现） |
| 文件全局唯一命名（`batch_id` = UUIDv7） | ✅ | 已满足 refactor.md 的 S4-5（本文 R4-5）要求，无需事后改名 |
| Manifest 预留 `partition_key` / `source_instance` | ✅ | `model/src/meta.rs`（tag 14/15） |

**B. 控制平面（Catalog / Meta）—— ❌ 未开始（真瓶颈）**

| 能力 | 现状 | 阻塞点 | 归属 |
|---|---|---|---|
| Catalog 抽象 | `CatalogOps`（15 方法）**是好缝**，ingest/query/sql/flight 全走它 | — | — |
| Catalog 访问形态 | `LocalCatalogCache` 每 200ms **全量拉取**（`list_tables` + 逐表 `list_visible_files`） | 换共享 metanode 即 O(表×文件) 风暴；且 `schema_ver`/`manifest_ver` 未分离 | R2 |
| Catalog 持久化 | `MemoryCatalog` 纯内存；重启靠**本地 WAL 重放 DDL** 重建 | 分离后 WAL 在 datanode、Catalog 在 metanode → 必须 raft snapshot | R3 |
| Compaction 走抽象 | `Compactor.catalog: Arc<MemoryCatalog>`，`commit_compaction` 是**固有方法不在 trait** | 具体类型旁路：Catalog 转 gRPC 时编译不过 | **R2 必做** |
| 孤儿清理走抽象 | `spawn_orphan_cleanup(catalog: Arc<MemoryCatalog>)` | 同上 | **R2 必做** |

**C. 分布式语义—— ❌ 未开始**

| 能力 | 现状 | 不做的后果 | 归属 |
|---|---|---|---|
| 冷热边界**按实例**切分 | `source_instance` **只写不读**（全仓库无消费点，已核） | 多 datanode 各自 flush 后查询无法判断"该实例 flush 到哪" → **同一批数据读两次**（静默重复计数） | R4 |
| 查询 fanout + 块级剪枝 | 单节点 scan；`chunk::ColumnStats` 已算出**但未接入跳过** | 无分布式查询能力；单节点也浪费了块级剪枝 | R5 |
| Compaction 全局化 | 单进程后台任务、**无租约** | 两个执行者重复合并同一批文件 → 悬挂文件 / 快照异常 | R6 |
| 孤儿 GC 多写者安全 | 进程内 `first_seen` map + 1h 静置；按 S3 ↔ Meta 对账 | 把别家"刚上传未提交"的文件当孤儿删除 → **数据丢失** | R6 |

**D. 质量与决策—— ❌ 未开始**

| 能力 | 现状 | 阻塞点 | 归属 |
|---|---|---|---|
| 故障刻画（chaos 11 场景） | 仅 3 场景（并发崩溃 / kill -9 等） | 未刻画就上 raft：故障组合指数级放大（`refactor.md §3` 明确"不要跳过"） | 阶段 2 |
| 基线压测 | ✅ **已入库** | `chaos/examples/bench_baseline.rs`（T8）+ `operation-log §32/§33/§35` 数据表：提交时刻分布/带宽、峰值提交数、`seal→committed`、文件数·天、单文件行数、**seal 原因分布与封口水位**、内存水位曲线、RowGroup 数。**仍缺**：真多节点（跨进程/跨机）CommitFiles 瞬时并发、真实 S3 PUT 绝对延迟、**组级剪枝收益** | 阶段 2（剩余部分） |
| 观测指标（S1-11） | 未接 | 内存水位 / WAL 积压 / 背压水位三项不可见 → 压力问题只能复现不能定位 | 阶段 2 |
| 文档同步（ADR-10 等） | 未修订 | ADR 原文与实现不一致，下一位实现者会按原文改回**违例实现** | 阶段 2 末 |

### 5.2 接缝现状（哪些抽象已到位、哪些是旁路）

| 抽象 | 位置 | 评价 |
|---|---|---|
| `CatalogOps`（15 方法） | `yuntun-catalog` | ✅ **最有价值的缝**：`MemoryCatalog` → `GrpcCatalogClient` 只需换注入 |
| `ShardReader` / `ShardFetch` / `RemoteShard` | `yuntun-store` | ✅ 热数据读侧可替换，调用方零改动 |
| `IngestSource`（ADR-13） | `yuntun-ingest` | ✅ 协议可插拔 |
| `object_store` 抽象 | `yuntun-store` | ✅ 已有 local/memory/s3 |
| `Compactor` / `spawn_orphan_cleanup` | `yuntun-compaction` | ❌ **具体类型旁路**（`Arc<MemoryCatalog>` + 固有方法），R2 必须补 `CommitCompaction` |
| `FileManifest.source_instance` | `yuntun-model` | ⚠️ 字段就绪但**无消费者**，R4 才闭环 |
| 幂等键存储 | `yuntun-catalog`（内存） | ⚠️ 随 Catalog 一起走，R3 后自动进 metanode；需确认 TTL 扫描不会变成全表扫描 |

### 5.3 如果今天拆进程：四类**静默错数据**

> 强调"静默"：它们**不报错**，只产出错的查询结果或丢数据。
> 这正是 `refactor.md` 把 S0/S2 放在 S3 之前的原因。

| # | 失败模式 | 触发条件 | 现状防线 | 结论 |
|---|---|---|---|---|
| 1 | **重复计数** | 多 datanode 各自 flush 后查询 | 冷热边界只有进程内的 chunk store | ❌ 无防线，R4 才解决 |
| 2 | **元数据风暴 / Catalog 丢状态** | 所有节点 200ms 全量拉 Catalog；metanode 重启 | 无 | ❌ R2 + R3 |
| 3 | **Compaction 互相踩** | 两个执行者合并同一批文件 | 无租约 | ❌ R6 |
| 4 | **孤儿 GC 误删** | 多写者 + GC 开启 | 1h 静置（缓解，非根治）；进程内 `first_seen` | ⚠️ 部分缓解，R6 根治 |

**因此**：R4/R5/R6 不是"网络化改造"，而是**新增分布式语义**。
"单机做完，分布式只剩网络化"（D-3）这句话对**写入路径**成立，对**冷热边界与后台作业**不成立
——这是 v2.1 对 v2.0 判断的**最重要修正**，已登记为风险 `R-8`。

---

## 六、阶段 2 WBS：质量与性能

> 即 v1.0 阶段 0.5 + **v2.1 新增的"定案与观测"职责**。
> **本阶段不是"打磨"，而是阶段 3 的准入门槛**：chaos 与基线未完成就上 raft，
> 会把"已知故障"与"新故障"混在一起，无法归因。

| ID | 内容 | 说明 | 为什么现在做 |
|---|---|---|---|
| T6.1–6.11 | Chaos 11 场景（v1.0 §5.2 全表沿用） | 含并发崩溃、`synced_seq` 验证、Batch 超时、磁盘水位 | 阶段 3 前置：`refactor.md §3` 明文"不要跳过"。**✅ 11/11 全部在 chaos 层有真实磁盘 + 跨重启 + 并发的证据**（`operation-log §31`）。过程中抓出**五个**真缺陷、修掉四个：幂等键不生效（§27 ✅）、恢复重复文件（§28.2 ✅）、WAL 撕裂不可自愈（§29.1 ✅）、监控 abort 不同步视图（§30 ✅）、提交窗口重复计数（§28.1，待与 R3 同批）。**下一步 = T8 基线压测 + P0 定案**（相位分散量级 / `max_flush_delay` / `rows_threshold`）+ ADR-10 修订 |
| T6.12 | **S1-11 观测三项指标** ✅ **已完成（2026-09-17）** | `Lakehouse::metrics()` + 周期打点：chunk 内存水位 / **WAL 积压记录数** / 背压水位 + Catalog 版本与增量统计（可序列化，HTTP 导出待阶段 2 尾） | 没有它，R-7（内存越限）只能复现不能定位 |
| T6.13 | **P0 定案**（v2.1 新增） | 用 T8 数据定 `flush_phase_spread_secs` / `max_flush_delay_secs` / `rows_threshold`，并**正式修订 ADR-10** | §2.2；未定案就不该改默认值。**进度 3/3 ✅**：①相位分散+宽限期 ✅ 定案（30s/0）、③持久化上界口径 ✅（= 窗口关闭 + md + spread）、②`rows_threshold` ✅ 定案（保持，§35 实测定性）；ADR-10 已按定量表述改写 |
| T6.14 | **chunk 压力与恢复专项**（v2.1 新增） | 触发 spill 的写入压力；spill 读回失败降级；大基数 `GROUP BY` 挤压 chunk 区（验证硬分区）；崩溃后 spill 清理 | S1 的"真实压力曲线"缺口 |
| T6.15 | TTL 分片移除 + segment 清理闸门（R21） | 阶段 0 遗留 | 影响 WAL 磁盘占用（与 §2.2 的 P2 联动） |
| T7.x | SQL 写入路径专项 Chaos | Flight SQL / INSERT × 崩溃点组合 | 验证与 DoPut 同等持久性 |
| T8.x | 压测与基线入库 | 吞吐 / P99 / 内存曲线 / **CommitFiles 瞬时并发** / 文件数·天 | P0 的证据来源。**✅ 已入库**：`bench_baseline`（提交时刻分布 + 峰值并发 + 文件数·天 + seal→committed）→ `operation-log §32`；**遗留**：**R3 后打同一 Meta 的真并发**（本轮已给本机多进程上界，§37）、真实 S3/MinIO、跨机、组级剪枝收益、内存曲线时序 |
| T9.x | Vortex：锁 commit + feature 打开 + Parquet 对比 | ADR-1 | 遗留清偿大头 |

**准出（阶段 3 的准入）**

1. chaos 11 场景 100% 通过、无 flaky（含并行执行）；
2. 基线数据入库（✅ T8 已入库），且 **P0 三项全部定案**（✅ 3/3，默认值已按定案调整/或明确保持）；
3. ✅ T6.12 指标已可观测；T6.14 的内存曲线不高于基线（**待压测**）；
4. §2.3 的文档同步项 1/2/3/4/6 完成（**尤其 ADR-10 必须与实现一致**）。

---

## 七、阶段 3 WBS：分布式化（R2–R6）

> 即 v1.0 阶段 1 / 1.5，但按 `refactor.md` 拆成 5 步，每步都有**独立准出**。
> 顺序不可颠倒：R2（抽象就位）→ R3（元数据权威）→ R4（进程拆分 + 分布式语义）
> → R5（并发查询）→ R6（后台作业全局化）。

### 7.1 R2：Catalog 访问形态与抽象补位 —— ✅ 已完成（7/8，2026-09-17）

> 落地细节与对照审查见 `docs/operation-log.md §26`。
> **本轮刻意只做单进程形态**：接口按远程形态定义，实现仍是同进程
> —— 换成 gRPC 客户端时业务代码一行不改。

| ID | 内容 | 状态 |
|---|---|---|
| T10.1 | 定义 `CatalogProvider` 抽象（同步、无网络读） | ✅ `LocalCatalog` + `CatalogSnapshot`（符合 DataFusion 同步 API 约束） |
| T10.2 | 实现 `LocalCatalog`（直读本地内存） | ✅ 物化视图，只依赖 `CatalogOps` trait |
| T10.3 | **版本号分两组**：`schema_ver` / `manifest_ver` 分离 | ✅ `CatalogVersion`；DDL 推前者、flush/compaction 推后者 |
| T10.4 | 每查询一次预取（immutable 快照：schema + manifest + 节点列表） | ✅ `CatalogSnapshot`（`Arc` 共享；写时复制代价与文件数无关） |
| T10.5 | watch 后台任务 + manifest delta 接口 | ✅ `version()` 无变化零开销 + `manifest_delta(since)` 只重拉变更表 |
| T10.6 | 本地缓存持久化（可选降级） | ⏳ 留 R3 后（需序列化格式；先有"刷新失败保留旧快照"的可用性底线） |
| T10.7 | **抽象 `CommitCompaction` + Compactor/孤儿清理只依赖 `CatalogOps`** | ✅ 已上 trait；`Arc<MemoryCatalog>` 已消除 |
| T10.8 | proto 启用 tonic-build（同签名演进） | ⏳ 随 R3 一起（与 raft 服务同一批 codegen，单独做无收益） |

**准出（已达成）**：

1. ✅ standalone 全量回归绿（200 passed / 0 failed，clippy 0 警告）；
2. ✅ Catalog 调用具备"预取 + delta + 版本失效"形态
   （证据：`full_reload_then_incremental_touches_only_changed_table` 用 `Arc::ptr_eq`
   断言无关表**未被触碰**；`snapshot_is_immutable_across_refreshes` 断言快照跨刷新不变）；
3. ✅ `yuntun-compaction` 不再依赖 `MemoryCatalog` 具体类型。

**R2 的两个副产品（都属"分布式地基"）**：

- **观测三项指标**（T6.12）已落地：chunk 内存水位 / WAL 积压 / 背压水位 + Catalog 版本与增量统计；
- **刷新失败保留旧快照**：单进程看不出价值，R3（metanode 偶发不可用）时是可用性底线。

### 7.2 R3：metanode 独立 + raft（≈3–4 周）

> 📌 **设计与分步计划已定稿**（2026-09-19）：[`metanode-design.md`](metanode-design.md)
> —— raft 库选型与逃生门、`CatalogState` 抽取纪律、快照与保留策略、proto 草案、
> S3-0~S3-7 步骤/回滚点、M3 验收矩阵、8 条风险（按"是否静默错数据"排序）。
> **进度**：**S3-2 第一切片已落地**（`operation-log §38`）—— `CatalogState` 纯状态机抽出、
> 四处非确定性（3 处状态机读钟 + 1 处 `HashSet` 定版本分配序）已修、`encode_canonical` + 5 个对拍用例（含反证）。
> **S3-5 键集合去重已接线**（`operation-log §39`）：键集合从 WAL 派生（不引入第二个真相来源），
> 并修掉"认领 ≠ 重复"语义坑（把认领当重复会让 manifest 永不落盘、恢复 100% 失败）。
> **S3-1 选型闸门已过**（`operation-log §40`）：三节点收敛 + kill leader 不丢已提交数据 + 样板 124 行
> → **保留 raft-rs**（openraft 降级为备选）；余 S3-1b 快照安装 + 日志压缩。
> **S3-1 选型闸门已过**（`operation-log §40`）：三节点收敛 + kill leader 不丢已提交数据 + 样板 124 行
> → **保留 raft-rs**（openraft 降级为备选）；余 S3-1b 快照安装 + 日志压缩。

| ID | 内容 | 说明 |
|---|---|---|
| T11.1 | 新增 `yuntun-meta`：fjall 实现 `raft-rs::Storage` | Catalog 逻辑零改动，只换状态机宿主 |
| T11.2 | Catalog 内存快照 + **raft snapshot 持久化** | 只有一份权威数据（ADR-12：避免双写陷阱） |
| T11.3 | Catalog gRPC 服务 + 客户端（`GrpcCatalogClient: CatalogOps`） | 注入切换，业务代码不动 |
| T11.4 | 幂等键 TTL 扫描不进热路径（分片/定期） | §5.2 遗留风险 |
| T11.5 | 3 节点 raft：杀 leader / 网络分区 / snapshot 重建 | 阶段 3 的 chaos |

**准出**：3 节点 raft 写入不中断；metanode 全量重启后 Catalog 与重启前一致；
standalone 仍可单机运行（同一份装配的裁剪）。

### 7.3 R4：datanode 化 + 冷热边界按实例（≈2–3 周）

| ID | 内容 | 说明 |
|---|---|---|
| T12.1 | 拆 `yuntun-ingestor` / `yuntun-queryd` / `yuntun-compactor` | 从 `standalone` 逐组件摘出 |
| T12.2 | **消费 `source_instance`：冷热边界按实例二维切分** | **v2.1 新增**：这是"重复计数"的解药（§5.3-1） |
| T12.3 | 成员发现 + 分片归属（谁持有哪个 `(table, shard)` 的热数据） | 与 `ShardReader`/`ShardFetch` 对接（缝已在） |
| T12.4 | 每节点私有状态初始化与校验（WAL 目录 + spill 目录） | §2.3-3 的代码化 |
| T12.5 | `instance_id` 唯一性校验（启动即拒绝重复） | 重复 `instance_id` 会直接破坏 T12.2 的前提 |

**准出**：多 datanode 并发写入 + 查询，**与单节点串行结果精确相等**（对拍，见 §8）；
杀掉任一 datanode 后重启，其未 flush 数据由 WAL 恢复且不重复。

### 7.4 R5：分布式并发查询（≈2–3 周）

| ID | 内容 | 说明 |
|---|---|---|
| T13.1 | 查询 fanout（按分片归属分发）+ 结果合并 | owner 侧过滤（架构 §4.3） |
| T13.2 | 块级剪枝接入（`chunk::ColumnStats` + ZoneMap） | **v2.1 新增**：统计已算出但未消费，单节点同样受益 |
| T13.3 | 对拍测试（硬要求，不可抽样） | 分布式并发结果 == 单节点串行结果 |
| T13.4 | 查询中节点故障的降级语义 | 明确"失败"还是"部分结果"（不得静默返回不完整结果） |

**准出**：T13.3 对拍通过；查询中杀节点，行为符合 T13.4 的声明。

### 7.5 R6：compaction 与 GC 全局化（≈2–3 周）

| ID | 内容 | 说明 |
|---|---|---|
| T14.1 | 作业全局化：meta 租约独占 | 从单进程后台任务改为全局作业 |
| T14.2 | 租约 + 心跳 + 过期接管 | 杀掉执行者可被接管，无重复合并 |
| T14.3 | **孤儿 GC 的多写者安全** | **v2.1 新增**：不能把别家"在途（已上传未提交）"文件当孤儿（§5.3-4） |
| T14.4 | 跨节点文件合并（不同 `source_instance` 产出的文件） | — |
| T14.5 | 与 `deleted_at` 联动：墓碑期 + 无在途引用才真删 | — |

**准出**：多节点持续写入下文件数收敛到稳定区间；compaction 期间查询不受影响；
**开启孤儿 GC 的多节点压测零误删**（专项，见 §10）。

**自然扩展的保证**（v1.0 内置、v2.0 继续生效，R2 起继续受益）：
`IngestSource`（ADR-13）、Catalog 纯逻辑无网络（C7）、幂等键独立存储（`design.md` §7.3.1）、
新增的 `ShardReader` 与 `chunk` 边界——**R4/R5/R6 只做装配与分布式语义**，
不重写存储 / 写入 / 查询逻辑。

---

## 八、关键路径与执行顺序

### 8.1 依赖关系（不可颠倒的部分）

```
阶段 2（S0 收尾 + 定案 + 观测）   [T6.12 ✅]
   ├── T6.1–6.11 chaos 11 场景 ────────────────┐   (阶段 3 的全部前置)
   ├── T8 基线压测 ──► T6.13 P0 定案 ──► ADR-10 修订
   ├── T6.12 观测指标 ──► T6.14 chunk 压力专项
   └── T6.15 / T9.x（可与上并行）
                        │
                        ▼
R2 Catalog 冻结（✅ 已完成）──► R3 metanode + raft
                        │                      │
                        └──────────────────────┴──► R4 datanode + 冷热边界按实例
                                                            │
                                                            ├──► R5 分布查询（对拍）
                                                            └──► R6 compaction/GC 全局化
```

**三条硬顺序**：

1. **chaos/基线 → raft**：未刻画的系统上引入网络分区，故障无法归因（`refactor.md §3`）。
2. **R2（抽象）→ R3（raft）**：`Compactor`/孤儿清理仍绑 `Arc<MemoryCatalog>`，
   不先补 `CommitCompaction` 抽象，Catalog 转 gRPC 时编译不过（§5.1-B 实测）。
3. **R4（冷热边界按实例）→ R5/R6**：`source_instance` 无消费者时做分布式查询 = 重复计数；
   先做 compaction 全局化 = 在错的边界上合并文件。

### 8.2 可并行项

| 并行组 | 内容 | 备注 |
|---|---|---|
| P-A | T6.12（观测指标）、T6.14（chunk 压力专项）、T6.15（TTL/segment 清理） | 都在阶段 2 内，互不阻塞 |
| P-B | T9.x（Vortex 锁 commit + 对比） | 与 chaos/压测可并行；Vortex 结果影响 `rows_threshold` 定案 |
| P-C | §2.3 文档同步项 1/2/3/4/6 | **必须在阶段 2 末完成**，不占用关键路径 |
| P-D | T10.1–T10.4（R2 前四项）与 T10.7（抽象补位） | 同一阶段内可并行，但 T10.8 需在 R3 前 |

### 8.3 里程碑与门槛

| 里程碑 | 门槛（未达成不得进入下一格） |
|---|---|
| M1 = 阶段 2 准出 | chaos 100% 无 flaky + 基线入库 + **P0 全部定案** + 指标可观测 + 文档同步完成 |
| M2 = R2 准出 | ✅ **已达成**（2026-09-17）：200 passed/0 failed + 预取/delta/版本失效形态 + compaction 不依赖具体类型（余 T10.6/T10.8 随 R3） |
| M3 = R3 准出 | 3 节点 raft 写入不中断 + metanode 重启后 Catalog 一致 + standalone 仍可单机运行 |
| M4 = R4 准出 | 多 datanode 并发写 + 查询，**与单节点串行结果精确相等**；节点重启不丢不重 |
| M5 = R5 准出 | 对拍通过（硬要求）+ 查询中节点故障行为符合声明 |
| M6 = R6 准出 | 文件数收敛 + **开 GC 的多节点压测零误删** + 租约可接管 |

### 8.4 何时可以对外说"分布式就绪"

只有同时满足：M1–M4 全部达成 + M5 对拍通过 + M6 零误删。
在此之前，多节点只能作为**实验性部署**（单写者 + 无后台作业竞争）。

### 8.5 工期重估（`refactor.md §14` 要求"S1 完成后重新评估"）

| 阶段 | 原估（`refactor.md`） | **v2.1 重估** | 差异原因 |
|---|---|---|---|
| S0 收尾（chaos + 基线） | 1–2 周 | **1–2 周**（不变） | 纯实验墙钟时间，不可压缩 |
| S1 chunk 层 | 3–4 周 | ✅ **已完成**（实际远快于估时） | 但估时含"真实压力曲线 / 观测指标"，这两项**未做**，已挪到阶段 2 |
| S1 收尾 | — | **0.5–1 周** | T6.12 观测 + spill 复用 |
| S2 Catalog 形态 | 2 周 | **2–2.5 周**（+0.5） | 新增 T10.7 抽象补位（`CommitCompaction`），实测为"不补则编译不过" |
| S3 metanode + raft | 3–4 周 | **3–4 周**（不变） | 逻辑零改动，但 raft snapshot 调试耗时 |
| S4 datanode + 进程拆分 | 2 周 | **2–3 周**（+1） | 新增 T12.2 **冷热边界按实例**（消费 `source_instance`）——原估为"纯拆分"，实为新增语义 |
| S5 分布查询 | 3–4 周 | **3–4 周**（不变） | 对拍测试是硬要求，不可抽样 |
| S6 compaction 全局化 | 2 周 | **2–3 周**（+1） | 新增 T14.3 **孤儿 GC 多写者安全**（误删专项） |
| **合计（剩余）** | 约 4–5 个月 | **≈3.5–5 个月** | 总量基本持平；**S4/S6 各上调，S1 大幅节省** |

**两条重要提示**：

1. **实验墙钟时间不可压缩**：chaos（11 场景 × 多轮）、基线压测、R5 对拍、R6 误删专项
   合计约占一半工期，且必须串行观察——**不要用"编码速度"推断总工期**。
2. **S1 的估时偏差值得记录**：原估 3–4 周，实际编码部分快得多，但**估时里包含的"真实压力
   曲线 / 观测指标 / spill 复用"恰恰没做**。这印证 §2.2 的立场：
   *性能与可靠性结论必须来自实测，不能从"代码写完了"推断*。

---

## 九、风险与应对

| # | 风险 | 概率 | 影响 | 应对 |
|---|---|---|---|---|
| R-1 | arrow-flight `flight_sql` 模块 API 与目标客户端版本不匹配（protobuf 兼容性） | 中 | 高 | S1.5 第一时间做 pyarrow 冒烟；不匹配则从官方 proto 手动 codegen |
| R-1b | sqlparser 版本与 DataFusion 55 依赖不一致（双版本共存） | 低 | 低 | 对齐 Cargo.lock 中 DF 依赖的 sqlparser 版本；AST 仅用于 server 前置分流，双版本本也可共存 |
| R-2 | ~~DataFusion `insert_into` DML 集成坑~~ | — | — | **已消除**（v2.0.3）：INSERT 改为 server 前置解析，不使用 DataFusion DML |
| R-3 | 双轨 ticket 路由冲突（FlightSql cmd 与自定义 cmd 混淆） | 低 | 中 | cmd 头部加 1 字节轨标识；两轨各有 e2e |
| R-4 | 结构重构引入回归 | 低 | 中 | ~~纯移动 + 改名，重构后立即全量回归 66 tests~~ **v2.1 修订**：chunk 层不是纯移动（新增状态机/账本/spill），回归口径改为**全量 189 tests + clippy 0 警告**，且**必须逐条对照 ADR 原文**——本轮已验证"功能全绿 ≠ 不违架构"（`operation-log §25.3-1` ADR-10 违例） |
| R-5 | 阶段 3 时 Catalog gRPC 改造面超预期 | 中 | 中 | 架构 §6.4 已同签名预留；Compaction 依赖具体类型需先抽象 `CommitCompaction`（操作日志 §2.2-5）**→ 已升格为 T10.7 必做项，并实测确认为"不补则编译不过"** |
| **R-6** | **配置项语义漂移**（改了默认值/语义但文档与运维认知未同步） | 中 | 中 | 示例配置由单测守护（与解析器同步）＋ `Config::warnings()` 启动自检不变量＋ §2.3 文档同步清单 |
| **R-7** | **"内存硬上限"被可见性优先级击穿**（`chunk` 记账无条件，靠 WAL 积压吸收） | 中 | 中 | 入口 95% 拒写 + 观测三项指标（内存占用 / WAL 积压 / 水位）；阶段 2 压测确认积压峰值有界（§2.2-6） |
| **R-8** | **"单机做完只剩网络化"的判断被夸大**（D-3）：冷热边界按实例、compaction 租约、孤儿 GC 多写者安全都是**新增语义**，不是网络化 | **高** | 高 | v2.1 修正已入册：R4 的 T12.2（消费 `source_instance`）/ R6 的 T14.1–T14.3 单列；准入顺序见 §8.1 三条硬顺序 |
| **R-9** | **孤儿 GC 误删在途文件**（多写者 + 进程内 `first_seen`）→ 静默丢数据 | **高** | 高 | T14.3 专项：GC 必须能识别"已上传未提交"（在途窗口），并**以多节点并发压测零误删作为 R6 准出**（§10） |
| **R-10** | **相位分散量级未定案就被顺手改**（等于静默改掉 ADR-10 削峰） | ✅ **已定案**（30s/0，附 T8 实测） | 中 | ✅ 已缓解：P0 ① 定案 + ADR-10 原文已改（防按旧原文改回 `window_start` 锚点）+ 不变量自检 `max_resident > md + spread`（`warnings()`）+ `yuntun.toml.example` 写明"改默认值必须附 T8 数据" |
| **R-11** | **幂等键"看起来有、实际不生效"**（键被丢弃 / 提交不带键 / 无入口预筛）→ 客户端超时重试=静默重复计数；反向修过头则同请求的多批次互相判重=静默丢数据 | **已发生** | 高 | ✅ 已修（`operation-log §27`）：入口预筛 + fsync 后登记 + 恢复重建索引 + 多批次生产点**派生批次键**；新增 chaos #5 与客户端整请求重试用例；缺一条断言就会被"键已透传"的绿色误导 —— **凡是"已具备"的能力都要有一条断言其反面后果的用例** |
| **R-12** | **提交 → `mark_committed` 窗口内重复计数**：`commit_files` 到 `mark_committed` 之间隔一跳 WAL fsync，此间同一批数据既在文件里又在热数据里 → 快照 ≥ S 的查询**偶发多计**（实测 4→6、9→12） | **已发生** | 中 | 测试已钉（`chaos::commit_to_mark_window_must_not_double_count`，`#[ignore]` 保留一键复现）＋ 精确断言前统一 `wait_hot_drained()`（`operation-log §28.1`）。**修复必须走读侧栅栏**（接缝带上"已知文件/batch 集合"，与 `refactor.md` S5-4 的 `known_manifest_ver` 同向）——锁/提前标记/粗水位三种快修都已论证不成立（§28.1），与 R3 同批设计 |
| **R-15** | ~~监控线程 abort 后不同步视图~~：`spawn_timeout_monitor` 写 `BatchAbort` 只进 WAL，不通知 `BatchStateView` → 已放弃的批次仍留在 `non_terminal()`、其 `wal_seq_range` 永远挡住 segment 释放（**WAL 磁盘只增不减**），并且每轮重复写一条 `BatchAbort` | **已发生** | 中 | ✅ **已修**（`operation-log §30`）：`BatchStateView` 增默认空实现的 `note_abort`，两条 abort 路径（批次超时 / 磁盘水位）append 成功后调用；`LiveBatchTracker` 用 `observe(BatchAbort)` 实现（与 WAL 同一语义，不另设内存标记）。用例 `chaos::batch_timeout_releases_segment_after_object_store_failure`，撤掉同步即变红 |
| **R-14** | ~~WAL 撕裂后无法自愈~~：`open_append` 以文件物理长度作追加偏移，新记录写在撕裂的垃圾字节之后，而 replay 扫到撕裂点即停止 → **写入全部成功（ack 正常）但数据全部不可见**，静默、永久 | **已发生** | 高 | ✅ **已修**（`operation-log §29.1`）：`WalWriter::open` 接管目录时（**在打开活跃 segment 之前**）把每个 segment 截断到最后一条完整记录的边界（`set_len` + fsync）；覆盖全部 segment。新增 `segment::repair_torn_tail` + `decode_with_stop`（原 `decode_segment_records` 签名不变）。回归用例：`wal::writer::open_repairs_torn_tail_so_new_appends_are_readable` + chaos #7 转正 |
| **R-13** | ~~崩溃恢复产出重复文件~~：**同一份 WAL 目录被两个攒批循环消费** —— 轮次间只 `cancel()` 不 await，新循环在旧循环未退出时就开写，同一条 Data 被各吸收一次并各自 flush → **两个文件、持久重复**（9 批次 27 行 → 11 文件 33 行） | **已发生** | 高 | ✅ **已修**（`operation-log §28.2`）：① 夹具 `cancel()` 后 await 循环退出；② 生产 `run_accumulator` 增"退出闸门"（cancel 已置位则本轮不做）。教训升格为通用约束：**节点私有状态（WAL 目录）同一时刻只能有一个消费者** —— R4 的 T12.5（启动即拒绝重复 `instance_id`）是它的显式化 |

---

## 十、验收标准

### 阶段 1（功能）

| # | 标准 | 验证方式 |
|---|---|---|
| 1 | pyarrow FlightSQL：建表 → insert → select 闭环 | e2e 脚本入库 CI |
| 2 | `yuntun-cli` 导入 CSV/JSONL/Parquet 并查询 | e2e |
| 3 | SQL `INSERT` 数据崩溃重启不丢（WAL 权威） | 测试注入 kill |
| 4 | 单二进制 `yuntun` 单命令启动，零额外依赖 | 冒烟 |
| 5 | workspace 无 `bins/`；`cargo clippy -D warnings` 干净 | CI |

### 阶段 1.5（数据平面）—— ✅ 已达成

| # | 标准 | 验证方式 | 证据 |
|---|---|---|---|
| 1 | **读己之写**：写入 fsync 后一个扫描周期内可读，且**零已提交文件** | `write_then_read::read_your_writes_visible_before_flush` | ✅ |
| 2 | **持久化上界确定**：`seal_time + max_flush_delay + phase`，无随机项 | `chunk::flush_deadline_is_deterministic_and_bounded` | ✅ |
| 3 | **ADR-10 窗口对齐**：同窗口多批写入不裂成多 chunk；"创建后 N 秒"不得触发 seal | `chunk::time_seal_is_window_aligned_not_creation_offset` | ✅ |
| 4 | **内存有界**：60/80/95 三级阶梯可卸载且不收窄可见性；chunk/query 硬分区互不抢占 | `chunk::budget` + `pressure_ladder_*` | ✅ |
| 5 | **spill 可信性**：CRC 篡改/截断/魔数错误必须被发现并丢弃（WAL 仍是权威） | `spill::corrupted_payload_is_detected_by_crc` 等 | ✅ |
| 6 | **release-after-commit（I4）**：提交后缓存追上之前不得释放（零可见性空洞） | `chunk::committed_chunk_visible_until_cache_catches_up_then_reclaimed` | ✅ |
| 7 | **接缝可替换**：查询侧只依赖 `ShardReader` | `hot_shard_reader`（远端实现）+ `chunk::store_exposes_shard_reader_seam` | ✅ |
| 8 | 崩溃恢复不回归（重提交/世代闸门/交错写入） | `m0a_recommit` 3 用例 + chaos 3 场景 | ✅（2026-09-18 复查：曾因"**同一份 WAL 目录被两个攒批循环消费**"产出重复文件，两处修复后转绿 —— 见 `operation-log §28.2` / R-13） |
| 9 | 全量测试 + 零告警 | `cargo test --workspace`（189 passed / 0 failed）+ clippy | ✅ |

### 阶段 2（质量）

沿用 v1.0 §9.2（Chaos 100% / 核心模块覆盖率 ≥80% / 零丢失零重复）+ §9.3 性能指标，
**并新增**：

| # | 标准 | 验证方式 |
|---|---|---|
| 1 | chaos **11 场景 100%** 无 flaky（含并行执行） | CI 连续 N 轮 |
| 2 | 基线数据入库（吞吐 / P99 / 内存曲线 / **CommitFiles 瞬时并发** / 文件数·天） | 压测报告 |
| 3 | **P0 三项全部定案**，默认值已按定案调整，ADR-10 已修订（⏳ 2/3：`rows_threshold` 待 RowGroup 实测） | §2.2 登记表逐项签署 |
| 4 | 观测三项指标（内存水位 / WAL 积压 / 背压水位）可读 | 日志 + 指标导出 |
| 5 | chunk 压力专项：触发 spill、spill 读回失败降级、大基数 `GROUP BY` 下写入不受影响、崩溃后 spill 清理 | T6.14 |
| 6 | 内存曲线不高于基线 | 压测报告 |

### 阶段 3（分布式）

**准入前提（M1）**：阶段 2 准出全部达成（尤其 chaos 与 P0 定案）。

**逐里程碑验收**（M2–M6，定义见 §8.3）：

| 里程碑 | 标准 | 验证方式 |
|---|---|---|
| M2 (R2) | standalone 全量回归绿；Catalog 具备预取 + delta + 版本失效形态；`yuntun-compaction` **不再依赖具体类型** | 编译期（依赖图）+ 回归 |
| M3 (R3) | 3 节点 raft 写入不中断；metanode 全量重启后 Catalog **与重启前逐字节一致** | 杀 leader / 网络分区 / snapshot 重建 |
| M4 (R4) | 多 datanode 并发写 + 查询结果**与单节点串行结果精确相等**；节点重启不丢不重 | **对拍测试（硬要求，不可抽样）** |
| M5 (R5) | 对拍在 fanout 下继续成立；查询中杀节点行为符合声明（不得静默返回不完整结果） | 对拍 + 故障注入 |
| M6 (R6) | 文件数收敛到稳定区间；租约可被接管、无重复合并；**开启孤儿 GC 的多节点并发写入压测零误删** | 文件数曲线 + 误删专项 |

**四类"静默错数据"的专项反证**（对应 §5.3，必须逐条给出通过证据，不能靠"测试全绿"推断）：

| # | 失败模式 | 反证方式 |
|---|---|---|
| 1 | 重复计数 | M4 对拍：分布式结果 == 单节点串行结果（**含各 datanode 分别 flush 同一分片的场景**） |
| 2 | 元数据风暴 / Catalog 丢状态 | 压测观察 metanode QPS 不随文件数线性增长；M3 重启一致性 |
| 3 | Compaction 互相踩 | 双执行者并发注入 → 无悬挂文件、快照单调、结果不变 |
| 4 | 孤儿 GC 误删 | 多节点持续写入 + GC 开启 → 全程零文件丢失（对象清单与 Manifest 对账） |

---

**文档结束 · 开发计划任务书 v2.1**
