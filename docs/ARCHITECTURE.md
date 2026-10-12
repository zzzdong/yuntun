# yuntun 架构设计（ARCHITECTURE）

> **版本**：v2.0 ｜ **日期**：2026-10-11 ｜ **依据**：归档架构文档
> [`archive/architecture.md`](archive/architecture.md)（12 个 ADR + 域设计）、
> [`archive/architecture-with-chunk.md`](archive/architecture-with-chunk.md)（chunk 层目标态）、
> [`archive/design.md`](archive/design.md)（详细设计）—— **本文件是它们的"实现后的收敛版"**：
> 结构原因保留，已被现实推翻/已落地的部分按事实重写。现状见 [`OVERVIEW.md`](OVERVIEW.md)。
>
> 引用约定：`§1…168` 指向 `archive/operation-log.md`，`§169` 起指向 [`operation-log.md`](operation-log.md)。

---

## 1. 分层总览

```
┌─────────────────────────────────────────────────────────────────────────┐
│ 接入层（协议端口，全部装配在 server / standalone / datanode 里）          │
│   Arrow Flight SQL :50051 │ MySQL wire :3306 │ CLI(yuntun-cli) │ 指标 HTTP │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │ 写：INSERT / DoPut / CLI
                                │ 读：SELECT（前置分流后交 DataFusion）
┌───────────────────────────────▼─────────────────────────────────────────┐
│ 写入路径 ingest                                                          │
│  幂等预筛（入口，写 WAL 之前）→ WAL（权威：CRC + 组提交 + fsync）          │
│  → chunk 层（热数据：Open→Sealed→Spilled→Flushed→Released；内存账本/背压） │
│  → 窗口对齐 seal + 确定性相位分散 flush → 对象存储 + Manifest            │
│  → commit_files（元数据过 raft）                                         │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────────┐
│ 控制平面 meta（metanode：raft-rs + fjall；standalone 默认嵌入式 1 节点）   │
│  表/schema 版本链 │ 文件 Manifest │ 删除向量事件 │ 幂等键 │ 成员名录 │      │
│  快照 │ **作业租约** │ **任务目录（派生）**                                │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │ 预取快照 / 增量 delta / 版本驱动刷新
┌───────────────────────────────▼─────────────────────────────────────────┐
│ 查询路径 query（DataFusion 55）                                          │
│  每查询一次不可变 Catalog 快照 → 文件级/组级剪枝（`.idx`，parquet）        │
│  → 冷热边界按实例：冷文件直读共享存储 + 热数据向**产出该数据的实例**定向拉取 │
│  → 扇出（并发 + 每查询热读预算）→ 合并 → partial/STALE 语义交回用户        │
└─────────────────────────────────────────────────────────────────────────┘
```

**两条铁律**（改任何东西前先确认不违反）：

1. **所有写入都走 ingest 管线** —— WAL 是唯一的数据写入事实，禁绕过；
2. **协议端口统一在装配层** —— 域 crate（ingest/query/catalog/compaction）保持纯能力，
   不含协议、不含"分布式分支"；差异只体现在装配层。

---

## 2. crate 边界与依赖方向

**20 个 crate**（`cargo metadata` 的 workspace 成员）；依赖严格单向：

```
standalone ─► server ─► ingest ─► chunk ─► store ─► model
                    │        └─► wal ─► model
                    ├─► query ─► chunk, store, catalog, format
                    └─► catalog ─► store, format
datanode  ─► server（+ meta 客户端）
meta      ─► proto, model, catalog, wal
compaction─► catalog, store, format（**经 CatalogOps trait，不依赖具体 catalog 类型**）
client    ─► arrow-flight（可独立编译，不依赖 server）
```

| crate | 职责 | 关键约束 |
|---|---|---|
| `model` | 核心数据模型（最底层，无依赖）：`FileManifest` / `BatchState` / 删除向量 `dv` / 行组索引 `index` / **任务目录 `task`** / snapshot 编解码 | 不含 IO；`encode_canonical` 是**确定性对拍的基准** |
| `proto` | 元数据 / 数据面消息（prost 手写 + tonic-build） | **`oneof` 里的 op 是唯一权威编码**；字段只增不改号 |
| `wal` | 自实现 WAL：segment + CRC + 组提交 fsync + 事件流重建 BatchState | 只有 append / 顺序 replay；**seq 由 `header.first_seq` + 位置推导** |
| `store` | 对象存储抽象（local / memory / s3）+ 分片**读侧接缝**（`ShardReader`/`ShardFetch`） | 热数据实现由 `chunk` 提供 |
| `chunk` | 热数据缓冲：生命周期状态机 + 内存账本/背压阶梯 + 内存硬分区 + spill | spill **必须在本地磁盘**；spill 不参与权威判定 |
| `format` | vortex（默认）/ parquet + schema 适配 + 行组索引读写 | 读侧**按文件自身格式**分发 |
| `catalog` | Catalog 纯逻辑（无网络）：快照可见性 / OCC / 幂等 / DV / 任务目录 | **`CatalogOps` trait 是唯一接缝**（进程内 `MemoryCatalog` ⟷ 远端 `RemoteCatalog`） |
| `ingest` | 写入管线：幂等预筛 → WAL → chunk → flush → commit_files；崩溃恢复；`durable` 归档 | 跨层表标识一律 **`schema.table`** 全限定名 |
| `query` | DataFusion 桥接 + Manifest 驱动 scan + 扇出/合并 + DML 定位（`locate`） | 每查询**一次**不可变快照 |
| `compaction` | 合并 + 孤儿判定/GC + **按分片分租约的后台循环**（待办来自任务目录） | 提交带 `lease_purpose` 过线；输入已被合并即拒绝 |
| `sql` / `sqlwire` | SQL 语义唯一实现 / MySQL wire 协议适配 | 两端口共用同一份语义 |
| `server` | 节点层：协议端口 + 装配（`Lakehouse`）+ 配置解析 | 只装配，不写业务逻辑 |
| `meta` | metanode：raft + fjall、状态机、快照、成员变更、`RemoteCatalog`、运维 CLI | 状态机里只放"事实"；**派生态（任务目录）不进 `encode_canonical`** |
| `datanode` / `standalone` / `client` / `chaos` / `testkit` / `shardrpc` | 角色装配 / 单机装配 / SDK+CLI / 故障注入 / 测试目录 / 分片 RPC | — |

---

## 3. 写入路径

**顺序（不可颠倒）**：

```
客户端请求 → 幂等预筛（入口，命中即 duplicate=true 且不写 WAL）
          → WAL append（组提交 + fsync）→ 返回回执（含 expected_visible_in_secs）
          → 攒批循环（scan_interval）把批次发布到 chunk 内存分片 ⇒ **读己之写**
          → seal（窗口关闭 / 行数 / 字节 / 驻留上限 / schema 版本变化）
          → flush（seal_time + max_flush_delay + 确定性相位偏移）
          → 写对象存储 + Manifest → commit_files（过 raft）→ 释放 chunk
```

**几个必须记住的机制**（它们都是"不这样做会静默错"的类型）：

| 机制 | 规则 | 不这样会怎样 |
|---|---|---|
| **幂等键粒度** | 一次客户端请求一个键；请求内第 `i` 批用派生键 `键#i`；预筛在 **ingest 入口**（WAL 之前） | 不派生 ⇒ 同请求第 2..N 批被第 1 批判重丢掉（**静默丢数据**）；判重在提交层做 ⇒ 一个 chunk 聚合多个键时无法按集合去重 |
| **幂等键独立存储** | 键不随 `FileManifest` 生命周期消失 | compaction 删除原文件后客户端重试 = **再写一份**（静默重复） |
| **窗口对齐 seal** | 时间维度的 seal 是**到达分钟窗口关闭**，不是"创建后 N 秒" | 低吞吐表一个窗口内产出十余个小文件（ADR-10 要防的小文件/条目爆炸） |
| **相位分散锚点** | `flush_at = seal_time + max_flush_delay + hash(instance,table,shard,window) % spread`，锚点是 **`seal_time`** | 用 `window_start` 锚点会重新对齐回窗口内固定偏移 ⇒ 分散失效、惊群复活 |
| **驻留硬兜底不变量** | `chunk_max_resident_secs > max_flush_delay_secs + flush_phase_spread_secs`（默认 60 > 0 + 30） | 硬兜底会早于正常到期触发 ⇒ 绕过相位分散；**功能测试全绿**，只能靠启动自检守 |
| **表标识归一** | 跨层标识一律 `schema.table`（`TableMeta.name` 是裸名，禁止拿它去列文件） | 远端目录下合并永远看不到文件（`§167` 真集群撞到：`candidates=0` 而盘上有 4 个文件） |
| **`commit_files` 一批一文件** | 请求里 `files.len() > 1` 直接拒绝（先于任何写入） | `CatalogState.files` 以 `batch_id` 为键 ⇒ 后一个**静默覆盖**前一个 |
| **DML 租约** | DELETE/UPDATE 与 compaction 的表级（细到 `(table, shard)`）租约互斥；**force flush 必须在取锁之前** | "合并复活已删行"的窗口会重新打开 |
| **UPDATE 原子可见** | 删除向量与新行文件由**同一个 op、同一个快照号**承载 | 读侧会看到"删了没插"（少数据）或"插了没删"（重复计数） |

---

## 4. 读路径与查询

- **每查询一次不可变快照**：不是性能优化而是**正确性要求** —— DataFusion 的 provider 是同步 trait、
  规划期反复调用，读会变的结构会让同一次查询的 plan 与 scan 看到两个版本。
- **冷热边界按实例**：每个实例只对"自己产出的、且已提交到清单"的热数据负责；
  读侧栅栏用"调用方那份快照里是否含这个 `batch_id`"决定可见性（**不由 chunk 本地标志决定**），
  结构性关闭"提交成功→标记完成"之间的崩溃窗口（`§99`）。
- **剪枝**：文件级（min/max + 版本 + 墓碑）→ 组级（`{batch}.idx` 的 zone map + XOR filter）。
  拿不准**不剪**；无索引 / 坏了 / 形状对不上 ⇒ 退回全读（只影响读多少，不影响读到什么）。
  带 DV 的文件走 parquet **整体行选择**，与组级索引计划**互斥且 DV 优先**。
- **扇出与降级**：并发扇出 + 每查询热读预算；失败降级为**带标记的**部分结果；
  `STALE` 必须"刷新后重试"而不是当失败；结果不完整通过 Flight SQL metadata 交给用户。
- **分布式聚合下推**（`§173`，G.2 / 归档 `S5-3`）：形状**只有最窄的一类**
  （`SELECT count(*) / count(c) / sum(整数列) / min / max FROM t`，且**无** `WHERE` / `GROUP BY` /
  `LIMIT` / `ORDER BY` / `DISTINCT` / JOIN / 子查询 / CTE / 别名）：冷的一半仍由 DataFusion 算
  （schema/列名/类型由它给），热的一半由每个来源**在数据旁边折**（`FetchShardAggs`，只回标量），
  协调者把部分状态**合并进冷结果的那一行**。
  - **只收"可分解 **且** 精确"的聚合**：`sum` 只收整数（浮点求和不满足结合律 ⇒ 对拍不可能逐位相等）、
    `min/max` 只收整数与字符串（浮点 `NaN` 的口径要先复刻）。`avg` / `stddev` / `count(distinct)`
    **不可分解或不可精确** ⇒ 一律原路。
  - **分组（`GROUP BY`）也能下推**（`§177`）：只支持**普通列**键（整数 / 字符串，含"NULL 是一组"、
    多列键），且投影必须是"**键在前、聚合在后**"；结果按**键升序**。不支持表达式键、
    `ROLLUP`/`CUBE`、`HAVING` ⇒ 一律原路。
  - **谓词（`WHERE`）也能下推**（`§176`）：支持面窄到可以**逐字复刻** —— 列与字面量的比较
    （含**负数字面量**、**字面量在左时比较符翻转**）、`IS [NOT] NULL`、`AND` / `OR` / 括号；
    语义按 SQL 的**三值逻辑**（`NULL` 参与的比较是 `UNKNOWN` ⇒ 不放行），
    且按 `align_batch` 的口径把**缺失列当全 NULL**。`NOT` / `LIKE` / `IN` / `BETWEEN` / 函数 /
    算术 / 浮点 / 跨类型**一律不下推**（`NOT` 要先有真正的三值求值，`§176.2`）。
    翻译不出来 ⇒ **整体不下推**：半个谓词会让热的一半算进不该算的行。
  - **拿不准就不下推**：不下推永远正确（只是稍慢），推错了是**结果悄悄错**。
  - **冷的一半按 `placement::assign` 分给各节点**（`§178` 规则 + `§179`/`§180` 远端折叠 + `§182` 接线）：
    本地一份仍由 DataFusion 算（**schema/列名/类型由它给**），其余由被分配到的节点**就地折叠、只回标量**；
    拿不到就**回退本地读**（不是 partial）。远端**不剪枝不索引** ⇒ 省的是解码 CPU 与协调者带宽。按节点分配见 `ROADMAP` 的 **N4b**（`S5-2`，未做）。
  - 热度语义与行路径**逐字相同**：STALE 失败并刷新重试、拿不到来源按 partial 策略标记/拒绝。
  - 对拍开关 `QueryEngine::with_agg_pushdown(false)` 只用来跑"两条路径逐位比"的断言。
- **DML 的读**：`DELETE`/`UPDATE` 的定位是**扫描 + 文件内行号**；
  读文件后先按**表 schema** 对齐（缺列补 NULL / 多余列丢掉 / 类型宽化，复用同一个 `align_batch`）
  再补行号列 ⇒ DML 与 `SELECT` 对同一批行**永远同一答案**。

---

## 5. 控制平面（metanode）

| 项 | 决策 | 原因 |
|---|---|---|
| 存储 | **fjall 只存 raft 数据**（log + hard state）；**Catalog 是内存状态机 + raft snapshot** | 若 fjall 也做 Catalog 权威 ⇒ 双写一致性陷阱（raft apply 与 fsync 不同步） |
| op 编码 | proto `oneof` 是**唯一权威编码**，日志 payload 同构 | 两套编码必然漂移 |
| 快照 | 帧头 + 块 CRC 三层保护；`Config.applied` 必须报（否则 raft 重放 ⇒ 静默分叉） | `§41`/`§42` |
| 成员变更 | `Join`（在线加 learner + 地址随 conf change 复制）→ 提升 voter → `Remove`（最后一个 voter 有硬门槛）；运维 CLI | `§118`/`§120`/`§121`/`§128` |
| **租约** | purpose + epoch + TTL + 可接管；提交带 `lease_purpose` 过线；**栅栏按 purpose 核** | 分片后若栅栏写死用途键 ⇒ 静默失配 |
| **任务目录** | **状态机派生**（输入只有"文件清单 + 删除向量"）⇒ 每个副本算出同一张表；随快照下发；**认领复用租约**（不另造认领协议）；不进 `encode_canonical` | 由某节点"扫出来写进去"就要先解决"谁来扫"（又一把租约 + 单点） |
| standalone 形态 | `[meta] mode = "embedded"`：进程内 1 节点 raft + fjall + loopback gRPC；`mode="memory"` 是回滚/测试开关 | `§56` |

**确定性纪律**：状态机的规范编码（`encode_canonical`）必须逐字节可对拍；
"派生结果"（任务目录、缓存）一律不进规范编码 —— 否则"派生逻辑变了"会被误报成"状态分叉"。

---

## 6. 后台作业

```
compaction_round（默认 60s 一轮，每轮 max_merges_per_round 上限）
  1. catalog.list_tasks()           ← 待办来自**任务目录**（状态机派生，全集群同一份）
  2. 逐条（按 key 排序，多节点顺序一致）：
       acquire 租约（purpose = `task:<table>:<shard>`）—— 抢不到就跳过（分领常态）
       compact_shard：按本节点 `[compaction]` 判定（任务只是"候选"）
       release 租约（立刻归还，接手方不必干等 TTL）
orphan_cleanup（全局 `gc` 租约单跑）：在途登记 + 静置期 + 两条件合取判据，零误删专项
```

**分领的安全底线**（不许因为"麻烦"绕过）：① 提交时的 **epoch 栅栏**；
② **输入已被合并即拒绝**（否则"旧快照 + 租约已还"会让同一批输入产出两份 = 行数翻倍）。

### 6.1 任务目录的**入口清单**（改代码前对照；`§172`）

任务目录是**派生量**（由"文件清单 + 删除向量"决定），所以**每一个会改变这两者的入口都必须重算
对应分片**。漏一个的后果不是"少干活"：残留的待办指向一个**空分片**，多节点每轮都会去抢一次它的
租约（"待办消失不了"），而运维看到的那张表是错的。今天这份清单（`CatalogState` 的方法）：

| 入口 | 为什么会让待办变 | 时钟 |
|---|---|---|
| `commit_files` | 加文件（可能跨过 `FILES` 阈值） | op 携带的 `now_ms` |
| `apply_deletions` | DV 变了（可能让**单文件**分片进任务表） | 同上 |
| `revoke_deletions_for_file` | DV 被撤销（该分片可能不再需要合并） | 同上 |
| `purge_table_files` | 整表文件下线 + 悬挂 DV 同步 revoke | 同上 |
| `apply_update` | **既加新文件、又改 DV**（两侧都要看） | `now_secs × 1000` |
| `drop_shard` | 整个分片下线 | op 携带的 `now_ms` |
| `commit_compaction` | 合并改了清单（合并完通常就不再满足） | **没有**墙钟（靠租约 epoch 表达时序）⇒ 传 0 |
| 快照还原 | 任务表**不进快照** ⇒ 按还原出来的清单重算一遍 | 0 = 年龄**未知** |

**`since_ms` 的口径**：= **首次登记**的时刻（Unix 毫秒）；`0 = 未知`（还原之后、还没被携带时钟的
入口触碰过）。刷新**只在原来未知时**补钟，**绝不重置**已有值 —— 否则 `age_ms` 永远是"刚登记"。
观测侧把 0 渲染成 `unknown`（`meta_probe tasks` / `/metrics`），**不许**算 `now - 0`（那会是 54 年）。

---

## 7. 已定案决策（**禁止顺手改**；改必附实测）

> 前 13 条来自归档 `status.md §3`，编号与数值**保持不变**（代码/注释引用着它们）。
> 改任何一条：先在 `operation-log` 里附**实测数据**，再回改本表与 `OVERVIEW.md`。

| # | 决策 | 值 / 契约 |
|---|---|---|
| D1 | 相位分散量级 | `flush_phase_spread_secs = 30`（原 5）：峰值提交 ≈ shards/spread（5s → 87 次/秒，30s → 10 次/秒） |
| D2 | flush 宽限期 | `max_flush_delay_secs = 0`（原 30）：实测它不减少文件数，只把持久化上界从 5s 推到 35s |
| D3 | 持久化上界口径 | 窗口关闭 + `max_flush_delay` + `spread` + **提交路径耗时**（实测约 +3s），无随机项 |
| D4 | 相位分散锚点 | **`seal_time`**（不是 `window_start`） |
| D5 | 不变量 | `chunk_max_resident_secs > max_flush_delay_secs + flush_phase_spread_secs`（默认 60 > 0+30），进启动自检 |
| D6 | 幂等键粒度 | 一次请求一个键，请求内第 `i` 批用 `derive_batch_key(键, i)` |
| D7 | 幂等预筛位置 | **ingest 入口**（写 WAL 之前），命中即 `duplicate=true` 且不写 WAL |
| D8 | Catalog 快照 | 每查询取一次不可变快照（`Arc` 共享），规划到 scan 全程复用 |
| D9 | 幂等键独立存储 | 键不随 `FileManifest` 生命周期消失 |
| D10 | `rows_threshold` / `bytes_threshold` | 保持 **50 万 / 128MB**；必须知道实际效果（1KB 行时账本口径 ≈4.9KB/行 ⇒ 128MB ≈2.7 万行/文件） |
| D11 | chunk 预算定容规则 | `chunk_mem_budget ≳ 写入速率 × (写入→flush 的滞留)`；`spread` 只影响延迟与压力档位，**不影响文件大小** |
| D12 | 热副本可见性 | **读侧栅栏**：由"调用方 manifest 快照里是否含这个 `batch_id`"决定；`batch_id` 在 `commit_files` **之前**登记 |
| D13 | 客户端落点 | **显式给定的 contact point**；**不做**客户端侧名录发现 / 一致性哈希软路由 / 存活感知 |

### ADR-10 的两个定案数值（判据钉着，见 `crates/testkit/tests/docs_consistency.rs`）

| 项 | 定案值 | 依据 |
|---|---|---|
| `max_flush_delay` | **0**（原 30） | T8 基线：它不减少文件数，只抬高持久化上界 |
| `phase_spread` | **30s**（原 5） | T8 基线：峰值提交 87 次/秒 → 10 次/秒；上界 31s 仍优于旧默认 35s |

### 其余 ADR（精要，全文见 `archive/architecture.md §4`）

| ADR | 结论 | 今天的补充 |
|---|---|---|
| ADR-1 | DataFusion 查询内核；vortex 主 / parquet 辅 | **vortex 已落地为默认**（先补齐读路径才翻默认）；代价：体积 3.5–4.5 倍、无组级剪枝 |
| ADR-2 | 不引入分布式 WAL | WAL 是**节点私有**；跨节点持久性靠 `durable` 档归档，不靠复制 |
| ADR-3 | 多节点 Ingestor 互不感知 | 节点私有状态只有两处：WAL 目录、spill 目录 |
| ADR-4 | `batch_id = UUIDv7` + BatchStateStore 状态机 | 幂等由**客户端键**在全局层保证 |
| ADR-5 | 快照隔离统一删除语义（`snapshot_id + valid_from + deleted_at`） | 今天 DV 与墓碑共用同一套可见性 |
| ADR-6 | Catalog 本地缓存 + 变更通知 | 版本驱动为主、`cache_ttl` 兜底 + 读路径**按需保鲜**（`ensure_fresh`） |
| ADR-7 | 协调层复用 Meta Raft，不引入 etcd | — |
| ADR-8 | Ingestor 不做显式 shard 路由 | 分区键是软路由，不是归属；**不做 rebalance** |
| ADR-9 | 表级 SLA 分级（`best_effort` / `durable`） | 真实 RPO 见 `OVERVIEW.md §7`（`durable` 不是 ≈0） |
| ADR-11 | WAL 自实现，不用 fjall | — |
| ADR-12 | meta 的 fjall 只存 raft 数据，Catalog 走内存 + snapshot | — |
| ADR-13 | 接入层可插拔（`IngestSource` 抽象先行） | 今天接了 Flight SQL + MySQL wire |

---

## 8. 配置项清单

> **权威是解析器**：`crates/server/src/config.rs` 头部的模板。**本清单里的每个键都必须在那里存在**
> （有判据守着：照着这份清单写配置必须能启动）。要加键：先改 `config.rs`，再回来加这一行。

```toml
# yuntun.toml（standalone：`yuntun --config yuntun.toml`）

[server]
listen = "0.0.0.0:50051"
shards = 1                          # MVP 单 shard；多 shard 需 ShardMapper（未做）

[meta]
mode = "embedded"                   # embedded（默认：进程内 1 节点 raft + fjall）| memory（测试/回滚）
listen = "127.0.0.1:50551"          # embedded 形态的 loopback gRPC 端口（0 = 内核分配）
dir = "./data/meta"                 # 省略 → 进程内临时目录（重启即新集群，会 warn）

[store]
type = "local"                      # local | memory | s3
root = "./data/store"               # type = "local"
# type = "s3" 时：bucket / endpoint / access_key_id / secret_access_key / allow_http

[wal]
dir = "./data/wal"                  # 节点私有状态之一（另一个是 spill 目录，ADR-3）
segment_max_mb = 64
batch_timeout_secs = 1800           # 批次超时 → BatchAbort
disk_high_watermark = 0.8           # 磁盘保护水位
archive_prefix = ""                 # ADR-9 的 durable 档：给了它才把 WAL 段归档到共享存储
archive_interval_secs = 1           # **它直接决定 RPO 的界**（连同一次上传时延）
archive_all_tables = true           # true = 全归档（默认）；false = 只归档 durable 表（粒度是段）

[chunk]
spill_dir = "./data/spill"          # 必须本地磁盘
instance_id = "standalone"          # 相位 hash 输入 + FileManifest.source_instance（多节点必须唯一）
mem_budget_mb = 512                 # chunk 区（硬分区之一）
query_mem_budget_mb = 512           # query 执行区（另一块，超限直接报错）
soft_pct = 60                       # 背压阶梯
hard_pct = 80
reject_pct = 95
metrics_log_interval_secs = 30      # 指标周期打点（0 = 关闭）

[ingest]
default_format = "vortex"           # vortex | parquet（vortex 已是默认；parquet 为回退/兼容）
rows_threshold = 500000             # seal 触发：让 RowGroup 一次成型
bytes_threshold_mb = 128            # seal 触发：字节阈值（Arrow 内存口径）
time_threshold_secs = 5             # 最短驻留地板（seal 时刻由**窗口关闭**决定，ADR-10）
max_flush_delay_secs = 0            # seal → flush 宽限期（0 = 封口即到期，D2）
chunk_max_resident_secs = 60        # 强制 seal+flush，防慢写入流撑爆 WAL
flush_phase_spread_secs = 30        # 确定性相位偏移上限（D1）
scan_interval_ms = 100              # 攒批线程扫描周期（决定读己之写的可见上界）
idempotency_ttl_hours = 24          # 幂等键 TTL

[compaction]
min_files = 5
interval_secs = 60

[query]
cache_ttl_secs = 30                 # 只是兜底（主要失效由版本驱动 + 读路径按需保鲜）
partial = false
hot_read_budget_secs = 2            # 热读（未落盘数据）的等待预算

[sql.mysql]
enabled = true
listen = "0.0.0.0:3306"
auth = "trust"                      # trust | native_password（users 非空时自动切换；今天只实现 trust）
# users = [{ name = "root", password = "..." }]

[metrics]
enabled = false                     # 指标 HTTP 导出；默认关：它会绑端口
listen = "127.0.0.1:9091"           # GET /metrics（Prometheus 文本）、/metrics.json、/healthz
```

**已被删掉的键（以及为什么）**：`flush_jitter_seconds`（随机 jitter 让持久化上界不可预测）、
`segment_max_size` / `segment_max_age` / `group_commit_window`（WAL 的真实键见上；组提交是内部行为）、
`idle_timeout`（从未实现，由 `chunk_max_resident_secs` 取代）、`[gc] orphan_delay`、
`[schema] default_policy`、`multipart_threshold` / `target_file_size`（均为未实现的设计项）。

---

## 9. 不变量与"改了会静默坏"清单

改代码前请逐条对照；这些都是**功能测试全绿也会坏**的类型：

| # | 不变量 | 违反的后果 |
|---|---|---|
| I1 | 所有写入经 ingest / WAL | 恢复语义失效，数据不可重建 |
| I2 | 跨层表标识 = `schema.table` | 远端目录下列文件为空，合并/查询看不到数据 |
| I3 | `chunk_max_resident > max_flush_delay + spread` | 相位分散失效（惊群复活），启动自检会告警 |
| I4 | `commit_files` 一批只带一个文件 | 同 `batch_id` 静默覆盖（少数据、零报错） |
| I5 | 幂等键独立于文件生命周期 + 入口预筛 | 合并后重试 = 静默重复 |
| I6 | 提交带 `lease_purpose`；栅栏按它核 | 分片后栅栏失配 ⇒ 并发合并互相踩 |
| I7 | 合并提交必须拒绝"输入已被合并" | 同一批输入产出两份 ⇒ 行数翻倍 |
| I8 | 任务目录 = 状态机派生，且不进 `encode_canonical`；**任何改"文件清单或删除向量"的入口都要重算对应分片**（入口清单见 §6.1） | 谁扫谁写 ⇒ 单点 + 各节点待办不一致；进规范编码 ⇒ 对拍误报分叉；漏一个入口 ⇒ 残留待办指向空分片（每轮白抢租约 + 运维表是错的，`D-25`/`D-29`） |
| I9 | `Config.applied` 必须如实报 | raft 重放 ⇒ 静默分叉 |
| I10 | 快照引用计数 / 每查询一次快照 | 同一次查询的 plan 与 scan 看到两个版本 |
| I11 | **测试判据不许是墙钟**（用事件：并发峰值 / 按轮驱动收敛） | 阈值变成"机器调度得多快"⇒ 同 binary 内并行时假失败（`D-26`）；确需"必须有界"的兜底要写理由 + `§171` 豁免标记，判据 `wall_clock_assertions_need_a_reviewed_waiver` 守着 |
| I12 | **聚合下推只用于"可分解 **且** 精确"的聚合**（判定在 `model::agg`）；形状拿不准 ⇒ 原路；`NULL` 与 `0` 必须能分开 | 浮点求和不满足结合律 ⇒ 逐位对拍不可能成立（下了推就是"结果悄悄错最后几位"）；把空集 `sum` 当 0 ⇒ 一个看起来很正常错值；判定放宽（如漏判 `DISTINCT` / `GROUP BY`）⇒ 少算/多算而零报错（`§173`） |
| I13 | **客户端缓存的 `(快照号, 文件清单)` 必须一次载荷落地**：号只在 `apply()` 里跟着载荷前进，**宁可落后、不许超前** | 刷新是两跳（探测 / 载荷），而增量请求带的 `since` 取缓存当前号；号超前而载荷失败（网络抖一下）⇒ 下一轮增量漏掉 `== since` 的墓碑与产物 ⇒ 那批变更**永久丢失**，查询给出偏小/偏大却"很正常"的数字（`D-23`，`§174`/`§175`） |

---

## 10. 部署与运行边界

- **端口**：Flight SQL `:50051`、MySQL wire `:3306`、metanode gRPC（`--listen`，容器内 9311）、
  datanode 数据面 + 可选 `--sql-listen`、指标 HTTP（默认关）。
- **客户端落点**：显式 contact point（D13）；连一个能接 SQL 的端点即可，扇出/合并由它负责。
- **部署包线**（量过的）：`connect_timeout = 500ms` / `RPC_TIMEOUT = 2s` / raft 选举超时 ~1s
  ⇒ **对称延迟到 1.5s 就会反复选举**（term 猛涨）。跨可用区 / 跨机部署前**必须**按 RTT 重定这三个量。
- **容器化 rig**：`tests/cluster.sh`（3 metanode，smoke/soak/lossy/netem）、
  `tests/datanode_cluster.sh`（3 metanode + 2 datanode，私有目录各一卷 + 共享冷存储）。
  ⚠️ datanode 镜像必须是 **glibc ≥ 2.44**（实测用 `archlinux`；`debian:trixie-slim` 2.41 /
  `fedora-minimal` 2.43 都跑不起宿主编的二进制）；跑 rig 时**不要并发跑别的 cargo**。
- **运维面**：`metanode` 的成员变更 CLI、`cargo run -p yuntun-meta --example meta_probe`
  （`status` / `leader` / `leases` / `tasks` / `write` / `verify`）、`[metrics]` HTTP 导出。
  `/metrics` 的 **`jobs` 段**（`§172`，G.4）报"**有哪些活 / 谁在做**"：
  `yuntun_jobs_tasks_total` / `tasks_unknown_age` / `leases_total` / `leases_held` 是标量，
  另有**每行带标签**的 `yuntun_compaction_task_{files,deleted_rows,age_seconds,age_unknown}{table,shard}`
  与 `yuntun_lease_{held_seconds,expires_in_seconds,epoch}{purpose,holder}`；
  两个 `*_error`（`tasks_error` / `leases_error`）把"读不到"与"没有"分开 ——
  否则元数据面故障会被读成"今天没活"。
- **数据进程的元数据面**：`--meta` 是**种子列表**（逗号分隔，`§172`）。
  生产**必须给多台**：只给一台时，那一台不是 leader ⇒ 该节点所有写失败（`D-30` 的真缺陷）。
  跟随 leader hint 需要 id→地址映射，**未做**。
