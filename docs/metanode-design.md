# R3 设计：metanode 独立进程 + raft（元数据控制平面）

> **版本**：v1.0（设计定稿，**待开工**）｜ **日期**：2026-09-19
> **上游文档**：[`status.md`](status.md)（现状）· [`plan.md`](plan.md) §7.2（R3 任务）· [`refactor.md`](refactor.md) §6（S3 清单）·
> [`architecture.md`](architecture.md)（ADR-2/3/10 等）· [`operation-log.md`](operation-log.md)（实施证据）
> **里程碑**：M3 = 3 节点写入不中断 + metanode 全量重启后 Catalog **与重启前逐字节一致** + standalone 不回归
> **工期参考**：≈3–4 周编码 + **≈40% 的实验墙钟**（chaos / 对拍 / 快照恢复专项），见 §9

---

## 1. 目标与非目标

### 1.1 目标（R3 做完必须成立）

| # | 目标 | 验收方式 |
|---|---|---|
| G1 | **元数据从"单机内存 + WAL 重放"变为 raft 强一致存储** | metanode 全量重启后 Catalog 与重启前**逐字节一致**（序列化对拍） |
| G2 | **3 节点 raft 组可跑通**：kill leader 自动选主、提交不丢失、写入不中断 | 杀 leader / 网络分区 / 慢 follower 注入 |
| G3 | **幂等去重跨进程生效**（S3-5）：两个 datanode 用同一幂等键提交，只生效一次 | 双写者用例（**必须断言反面**：不能只断言"没报错"） |
| G4 | **standalone 行为不回归**，且**不引入 `if distributed` 分支** | 既有 217 用例全绿；`grep -r "if distributed"` 为空 |
| G5 | snapshot 体积有上界、恢复时间可控 | 压出 N 天 manifest 后测 snapshot 大小与恢复耗时 |

### 1.2 非目标（**明确不做，避免范围蔓延**）

- ❌ **不拆 datanode**（R4）：本阶段 datanode 仍是"当前进程 + 远程 Catalog"，冷热边界、`source_instance` 消费**不在 R3**
- ❌ **不做分布式查询 fanout**（R5）、**不做 compaction/GC 全局租约**（R6）——但 R3 的 SM **预留**租约条目（S3-7 只做接口与条目，不做调度）
- ❌ **不做多租户 / 鉴权 / TLS**：metanode 走内网明文，登记为**已知限制**（阶段 4 前必须补）
- ❌ **不改对象存储路径**：Parquet 本体始终由 datanode **直连**对象存储（ADR-3，"raft 只存指针"）
- ❌ **不追求 raft 层性能**：目标是**正确**与**可恢复**，吞吐按"每节点提交速率 ≈ 总写入量 ÷ 文件行数"量级估（实测 4 节点峰值 9 次/秒，`operation-log §37`）

---

## 2. 现状基线（R3 的起点，来自 R2 的实际成果）

R2 已经把"访问形态"按远程形态定义好了（`operation-log §26`），R3 **不改语义、只换状态机宿主**：

| 已有 | 位置 | R3 如何用 |
|---|---|---|
| `CatalogOps` trait（21 方法，**所有调用方全走 trait**） | `crates/catalog` | 新增 `RemoteCatalog`（gRPC 实现）+ 保留 `MemoryCatalog`（standalone/测试） |
| 版本号**分两组**（`schema_ver` / `manifest_ver`） | 同上 | SM 内分配，进快照；delta 语义依赖它 |
| `manifest_delta(since)`（只报变过的表；无变化零开销） | 同上 | 由 SM 派生（"每表最后变更版本"表），**必须进快照** |
| `CatalogSnapshot`（每查询一次不可变快照） | 同上 | 不变（读侧仍在 datanode 本地） |
| `LocalCatalog`（版本驱动刷新） | 同上 | 不变；**唯一变化**是 refresh 的数据源从"本地 MemoryCatalog"变成"metanode" |
| `commit_compaction` / `known_batch_ids` 上 trait | 同上 | 不变 |

**唯一的新增语义**（真正的新代码，而非搬运）：

1. **状态机宿主**：把 `MemoryCatalog` 的变更逻辑抽成**纯函数式 `apply(op)`**（见 §4.1），
   由 raft 驱动；`MemoryCatalog` 变成"1 节点、本地传输"的同一个 SM 实例。
2. **权威幂等索引**：`IdempotencyRecord` 从"进程内 + WAL 重建"（§27 的临时方案）升级为 **SM 内状态**，
   并支持**一次提交多个键**（`client_request_ids: Vec<String>`）—— 这正是 `§27.5` 遗留 #1 与 `refactor.md S3-5`。
3. **快照与上界**：SM 状态可序列化 + 传输 + 安装；manifest 条目**保留策略**现在就定（§5.3）。

---

## 3. 架构

### 3.1 进程与角色

```text
                      ┌──────────────── metanode ×3（raft 组）──────────────┐
                      │  MetaService:  Propose(Op) / Prefetch / Delta /     │
                      │                Status / Join                        │
                      │  raft-rs: log storage(fjall) + state machine        │
                      └───────▲──────────────────────────▲──────────────────┘
                              │ ① 写：propose(op)
                              │ ② 读：Delta/Prefetch（本地缓存刷新）
        ┌─────────────────────┴──────────┐   ┌──────────────────────────────┐
        │ datanode（当前形态：单进程）      │   │ queryd（R5 前与 datanode 同进程）│
        │ WAL → chunk → flush → CommitFiles│   │ LocalCatalog（版本驱动刷新）     │
        │ LocalCatalog（读路径不变）        │   │ ShardReader/HotShards          │
        └──────────────────────────────────┘   └──────────────────────────────┘
                              │
                     对象存储（Parquet 只由 datanode 直连，metanode 不中转）
```

- **metanode**：新 crate `yuntun-meta`（bin `yuntun-metanode`），内含 raft 组与 SM；
- **datanode**：R3 阶段**仍是现在的进程**（`yuntun-server` + `yuntun-ingest`），只是 Catalog 由
  `LocalCatalog`（读）+ `RemoteCatalog`（写/刷新源）组合；
- **standalone**：同一份代码，装配成"1 节点 raft + 本地传输"（§3.3）。

### 3.2 读写路径

| 路径 | 走向 | 一致性 |
|---|---|---|
| 写入（`commit_files` / DDL / `commit_compaction`） | datanode → `Propose(op)` → raft 提交 → SM apply → 返回（revision, 结果） | **线性化**：propose 返回即已提交 |
| 读（查询规划用的 schema/manifest） | datanode 本地 `LocalCatalog`（**不每次打 metanode**） | **陈旧窗口 ≤ `cache_ttl`（默认 30s）**，由版本驱动刷新收敛 |
| 刷新 | `Prefetch`（版本号）→ 无变化零开销；有变化 → `Delta` | 单调：版本只增 |
| 幂等预筛 | 本地 `LocalCatalog` 的键集合（快路径）→ 未命中再 `Propose` | 权威在 SM；本地只是**加速**（§5.4） |

> **为什么读不走 metanode**：DataFusion 的 provider 是**同步 trait**、规划期反复调用（R2 §error）。
> 保持"本地不可变快照"是不改查询层的前提；代价是**允许读旧 ≤30s**，这是已声明的语义，不是缺陷。

### 3.3 standalone 的退化形态（**禁止分叉**）

```
distributed:  datanode ──gRPC──► metanode(raft, 3 节点)
standalone:   datanode ──本地传输──► metanode(raft, 1 节点，同进程)
```

- 两形态**共用** `RemoteCatalog` 的接口与 SM 代码，差异**只在装配层**（`R2` 原则：不得出现
  `if distributed` 业务分支）；
- 实现手段：`CatalogOps` 的远程实现持有一个 `trait MetaClient`，
  standalone 传 `LocalClient`（同进程直调 SM），distributed 传 `GrpcClient`；
- **启动自检**：`standalone` 下若配置里出现 metanode 地址，视为配置错误（fail fast）。

---

## 4. 关键设计决策

### 4.1 D1：把 `MemoryCatalog` 拆成「纯状态机 + 宿主」（**本阶段最大的重构**）

**现状**：`MemoryCatalog` 是一个 `Mutex<…>` 的具体类型，既做状态又做逻辑，还是测试的默认实现。

**R3 形态**：

```rust
/// 纯状态机：只有数据 + 确定性 apply，不含时钟、不含 IO、不含锁策略
pub struct CatalogState { /* schemas, tables, files, idempotency, versions, per-table last-change */ }

/// 一次变更（**必须是自描述、可序列化、确定性的**）
pub enum CatalogOp { CreateSchema{..}, CreateTable{..}, CommitFiles{..}, DropShard{..},
                     CommitCompaction{..}, EvolveSchema{..}, RegisterIdempotency{..}, … }

/// 确定性应用：同样的一串 op → 逐字节相同的 state（M3 的验收口径）
impl CatalogState { pub fn apply(&mut self, revision: u64, op: &CatalogOp) -> OpResult; }
```

- `MemoryCatalog` = `Mutex<CatalogState>` + 本地 `apply`（standalone 与**全部既有测试**继续用它）；
- metanode = raft 驱动 `CatalogState::apply`；
- **收益**：既有 20+ 个 catalog 用例仍然有效（它们测的就是 `apply` 的语义），且保证两形态不分叉。

**必须遵守的确定性纪律**（否则"逐字节一致"必然失败，而且**测试常常仍然全绿**）：

| 项 | 纪律 | 本项目的既有依据 |
|---|---|---|
| 时间戳 | **不得在 apply 内取本地时间**；一切时间由 op 携带 | `committed_at_ms` 已按此设计（`operation-log §32.1`："状态机要在所有副本上确定性应用同一份 manifest"） |
| 版本号 | 由 SM 按 revision 分配（不是各节点各自计数） | `schema_ver`/`manifest_ver` 必须全局单调 |
| 遍历顺序 | 任何"输出集合"用 `BTreeMap`/排序，**禁止 `HashMap` 迭代序** | delta/snapshot 编码必须可复现 |
| 随机 | apply 内**禁止** UUID/随机数（batch_id 由写入方生成 ✓ 已是现状） | ADR-4：UUIDv7 由 datanode 生成 |
| 浮点 | 统计量如需参与比对，固定精度与舍入 | — |

### 4.2 D2：raft 库选型 —— **raft-rs（TiKV）**，附 PoC 逃生门

| 候选 | 优点 | 风险 | 结论 |
|---|---|---|---|
| **raft-rs** | **有 Jepsen 验证**；TiKV 生产使用；与既有设计文档一致（`design.md` 已列 `raft-rs = "0.7"`） | API 低层：要自己实现 `Storage`、传输、`Ready` 循环、snapshot | ✅ **首选** |
| openraft | async 原生、样板少、成员变更/快照更省事 | **生产验证较少**（共识错误 = 静默数据不一致，代价不对称） | 备选：若 S3-1 PoC 显示样板成本失控，**在 S3-2 前**换（见下） |

**决策原则**：本项目的头号风险是"不报错、只产出错数据"（`plan.md §5.3`），
而共识实现错误的后果是**静默的数据不一致** —— 所以**正确性证据压倒开发便利**。

**逃生门（明确的时间点）**：S3-1 PoC 结束时评审一次；若 raft-rs 的样板（Storage/snapshot/传输）预计超过总工作量的 50%，
则切换到 openraft，并把"需要自建生产验证"作为**新增风险**入册。**S3-2 开始后不再切换**。

### 4.3 D3：日志与状态机存储 —— **fjall**

- `design.md` 已规划 `fjall = "3.1.8"`（LSM，纯 Rust，无 C 依赖）；
- raft 日志与 SM 状态**分两个 keyspace**：`raft_log`（可截断）与 `sm_state`（只在快照后压缩）；
- **不**复用我们自实现的 WAL（`yuntun-wal`）：那是**数据面**的段式日志（为 chunk/恢复语义定制），
  与 raft 日志的"索引 + 任期 + 截断"语义不同，强行复用会把两个不变量缠在一起。

### 4.4 D4：快照（snapshot）策略

| 项 | 决定 | 理由 |
|---|---|---|
| 内容 | `CatalogState` 的 prost 编码（含**两组版本号**、每表最后变更版本、幂等索引） | 少任何一项都会让 follower 的 delta 语义失真 |
| 格式 | 顶部带 `format_version` + `revision` 头，后跟分块 payload（每块可独立校验 CRC） | 与 WAL 的 tearing 教训一致（`§29.1`）：**部分接收必须能被识别** |
| 触发 | 日志条数 > `N`（默认 10 万）或状态 > `M`（默认 256MB） | "现在就定上界"（`refactor.md §6.2`） |
| 安装 | 落临时目录 → 校验 → 原子替换 → 更新 `applied_index` | 崩溃安全；安装期间**服务不停**（旧状态继续服务） |
| **保留策略** | manifest 条目**不无限增长**：按表保留 `checkpoint`（每窗口/每天一个基线）+ 近期条目；归档旧条目到对象存储（路径进 SM） | 否则 snapshot 必然膨胀到传不动（这是 `§6.2` 点名的高成本补救项） |

### 4.5 D5：幂等键（跨进程 + 键集合）

- **权威位置**：SM 内 `idempotency: BTreeMap<String, IdempotencyRecord>`（TTL 24h，按 revision 驱动淘汰）；
- **接口**：`CommitFilesRequest` 增 `client_request_ids: Vec<String>`（**键集合**，解决 `§27.5` 遗留 #1：
  一个 chunk 聚合多个键，提交层此前无法按键集合去重）；
- **去重语义**（与现有单机契约一致，不许改）：命中即 `accepted=false` + 返回既有 revision，
  **不重复写 manifest**；
- **本地快路径**：datanode 的 `LocalCatalog` 保留键集合用于**入口预筛**（省一次 raft 往返），
  但**权威只在 SM** —— 本地过期不会导致重复（只会多一次 propose，由 SM 拒绝）；
- **恢复**：进程重启不再需要"从 WAL 重建键索引"（SM 持久化），但**本地兜底仍有价值**
  （metanode 不可用时 gate 写入），保留并写明优先级。

### 4.6 D6：成员与选主

- **bootstrap**：`yuntun-metanode --init` 起单节点（`initial_members = {self}`）；
- **扩容**：`Join(node_id, addr)` → 先作为 learner 追日志，追上后 `change_membership`（raft-rs 提供）；
- **心跳/存活不进 raft**（`refactor.md §6.2`）：R3 只做成员名录，健康检查走独立 RPC（R4 的成员发现用它）；
- **时钟**：raft 只依赖**单调计数**，选主超时用本地 `Instant`（raft-rs 内部处理）——SM 侧**绝不用墙钟**。

### 4.7 D7：读一致性的对外声明（必须写进文档与 `metrics`）

- 查询侧读的是**本地缓存**（`cache_ttl` 默认 30s，版本驱动刷新）；
- 因此**"写入返回后立刻可查"仍成立**（读己之写靠 chunk 热数据），但**别的节点**可能在 ≤30s 内看不到；
- 这个窗口必须出现在 README/`status.md` 的对外语义里（**不能再让它只存在于代码注释里**）。

---

## 5. 接口契约（proto 草案）

```protobuf
// crates/proto/proto/meta.proto（阶段 3 启用 tonic-build；当前 prost 手写，见 S3-0）
service Meta {
  rpc Propose  (ProposeRequest)  returns (ProposeResponse);   // raft 写：op 已提交才返回
  rpc Prefetch (PrefetchRequest) returns (PrefetchResponse);   // 读：版本号 + 快照/差量
  rpc Delta    (DeltaRequest)    returns (DeltaResponse);      // 读：manifest_delta(since)
  rpc Status   (StatusRequest)   returns (StatusResponse);     // 运维：leader/term/applied/snapshot
  rpc Join     (JoinRequest)     returns (JoinResponse);       // 成员变更（learner → voter）
}

message ProposeRequest {
  Op op = 1;              // 自描述、确定性（§4.1）
  bytes request_id = 2;   // 幂等：同 request_id 重复 propose 必须返回同一结果
  uint64 schema_ver = 3;  // OCC：客户端读到的版本（DDL 用；版本不符 → FAILED_PRECONDITION）
}
message ProposeResponse {
  bool accepted = 1;      // false = 幂等命中（不重复应用）
  uint64 revision = 2;    // raft log index（SM 的权威版本）
  uint64 schema_ver = 3;  // 变更后的两组版本
  uint64 manifest_ver = 4;
  OpResult result = 5;
}
```

**约定**：

1. `Op` 必须**自包含**（含所有时间戳/ID）；schema 用现有 `serialize_schema`（Arrow IPC）避免双重编码；
2. **两组版本号语义不变**（DDL → `schema_ver`；files/compaction/drop → `manifest_ver`），
   delta 与快照必须**同时**带两者（R2 的 §26 语义，破坏它会让查询缓存静默失效）；
3. 错误码：`FAILED_PRECONDITION`（OCC 版本不符，可重试）/ `ALREADY_EXISTS` /
   `UNAVAILABLE`（非 leader 或无 quorum，**必须带 leader hint**）/ `RESOURCE_EXHAUSTED`；
4. **非 leader 的错误必须可重试**（客户端重试到 leader），而不是让写入失败 ——
   G2 的"写入不中断"依赖这条。

---

## 6. 实施计划（分步、**每步可回滚**）

| 步 | 内容 | 验收（必须能跑） | 回滚点 |
|---|---|---|---|
| **S3-0** | proto 定义 + `tonic-build`（`T10.8`）：把现有手写 prost struct 迁到 `.proto` 生成 | 编解码 round-trip；与现有 `CommitFilesRequest` 字段**逐个对齐**的兼容测试 | 保留手写 struct（双份并存一个 commit） |
| **S3-1** | **raft PoC（选型闸门）**：3 节点进程内集群，写/读/kill leader/快照/安装 | 3 节点写入不中断；kill leader 后 30s 内恢复；快照可安装 | 换 openraft（§4.2 逃生门） |
| **S3-2** | `CatalogState` 抽取（§4.1）+ **确定性对拍** | ✅ **第一切片已落地**（`operation-log §38`）：`CatalogState` 抽出、抓到并修掉**四处真实非确定性**（3 处状态机读钟 + 1 处 `HashSet` 决定版本分配序）、`encode_canonical` + 5 个对拍用例（含反证）。**余**：键集合接线（S3-5）、快照 prost 版（S3-3） | 已保留 `MemoryCatalog` 作为宿主（语义零改动） |
| **S3-3** | `yuntun-meta` 进程 + `MetaService`（Propose/Prefetch/Delta/Status/Join）+ fjall | 单节点 metanode 可独立启动；重启后状态一致 | — |
| **S3-4** | `RemoteCatalog`（`CatalogOps` 的 gRPC 实现）+ standalone 装配（本地传输、1 节点 raft） | **既有 217 用例全绿**（standalone 不回归）；`if distributed` 分支为零 | 切回 `MemoryCatalog`（装配层开关） |
| **S3-5** | 幂等权威迁 SM + **键集合**去重（§4.5，含 `§27.5` 遗留 #1） | 双写者同键 → 只生效一次（**含反面断言**） | — |
| **S3-6** | 3 节点集群运维：bootstrap / Join / 快照调参 / 观测（leader/term/applied/lag） | `Status` 可读；follower lag 可观测；快照安装不停服 | — |
| **S3-7** | **M3 验收 + 混沌**（§8） | 见 §8 矩阵 | — |

**关键路径**：S3-0 → S3-1（闸门）→ S3-2（最大重构）→ S3-3 → S3-4 → S3-5/6 → S3-7。
S3-0 与 S3-2 可并行（proto 与状态机抽取互不依赖）。

---

## 7. 风险与失败模式（按"是否会静默错数据"排序）

| # | 风险 | 后果 | 防线 |
|---|---|---|---|
| **R3-1** | **SM 非确定性**（取本地时间/哈希/`HashMap` 迭代序） | 各副本状态**缓慢分叉**，查询结果不一致；常规测试全绿 | §4.1 纪律表 + **对拍**：两实例 apply 同一 op 串 → 序列化逐字节比对（S3-2 验收） |
| **R3-2** | **版本号语义被破坏**（只带一组、或各节点自增） | 查询缓存静默失效/读到陈旧 manifest（R2 刚修好的病复发） | proto 与快照**同时**带两组版本；S3-0 兼容测试逐字段对齐 |
| **R3-3** | **幂等失效（跨进程）**：本地预筛过期、SM 未去重、或键集合被丢 | 客户端重试 = 静默重复计数（`§27` 的原始形态） | §4.5 + 双写者用例（**断言"只生效一次"**）+ `R-11` 的教训：凡"已具备"必须有反面断言 |
| **R3-4** | **快照膨胀**：manifest 条目无限增长 | snapshot 传不动 → follower 永远追不上；补救成本高 | §4.4 保留策略**现在就定**；S3-6 压出线性增长曲线 |
| **R3-5** | **standalone 分叉**：为了分布式在业务里加 `if distributed` | 两套路径，standalone 的测试不再保护分布式路径 | R2 原则 + `grep` 断言（S3-4 验收）+ 同一 SM |
| **R3-6** | **读旧被当成 bug 或被误当强一致** | 要么误修（加每次查询打 metanode → 查询性能崩），要么对外过度承诺 | §4.7：窗口写进 README/`status.md`/`metrics`，默认 30s |
| **R3-7** | **非 leader 报文被当成写失败** | 选主期间写入报错 → 违反 G2 | §5 约定 4：错误带 leader hint + 客户端重试 |
| **R3-8** | 单机资源（12 核）上跑 3 metanode + datanode | 实验时间被 IO/CPU 争用拉长、测量噪声大 | 混沌实验分档（先进程内 3 节点、再分机器）；结论只用"比值"（`§37` 的方法论） |

---

## 8. 验收矩阵（M3）

| 验收项 | 用例 / 方法 | 判据 |
|---|---|---|
| 3 节点写入不中断 | 持续写入 + 随机 kill leader（每 20s） | 无写入失败；总计提交数 = 成功数 + 幂等命中数 |
| 元数据不丢 | 全量重启 metanode（先 kill -9 leader） | 重启后 Catalog 序列化**逐字节等于**重启前（G1） |
| 幂等跨进程 | 两个 datanode 同键并发提交 | 只生效一次；行数不重复 |
| 快照可安装 | 新 follower 加入（日志已被截断） | 安装成功；状态与 leader 一致；安装期间**旧状态仍可服务** |
| snapshot 上界 | 造 N 天 manifest（或 10 万条） | snapshot 大小线性可控；恢复时间有上界 |
| standalone 不回归 | `cargo test --workspace` | **全绿**（当前 217）+ `if distributed` 零命中 |
| 读旧窗口可观测 | 写入后立刻在另一节点查 | 结果符合声明（≤30s 收敛），且 `metrics` 里有 lag 指标 |

**新增混沌场景**（补进 `crates/chaos`，与既有 11 个并列）：
C-1 leader kill 于 commit 提交中；C-2 follower 落后 + 快照安装；C-3 metanode 全部不可用时的写入行为
（**必须明确**：拒写还是降级？—— 建议**拒写**并带清晰错误，因为"写进本地 WAL 但元数据不可用"会让恢复语义复杂化）；
C-4 快照安装期间崩溃（安装原子性）。

---

## 9. 工期与依赖

| 阶段 | 内容 | 估时 | 依赖 |
|---|---|---|---|
| S3-0 | proto + tonic-build | 2–3 天 | 无 |
| S3-1 | raft PoC（选型闸门） | 3–4 天 | S3-0（可后置） |
| S3-2 | `CatalogState` 抽取 + 对拍 | 3–5 天 | 无（可与 S3-0 并行） |
| S3-3 | metanode 进程 | 3–4 天 | S3-1/S3-2 |
| S3-4 | `RemoteCatalog` + standalone 装配 | 2–3 天 | S3-3 |
| S3-5 | 幂等权威 + 键集合 | 2 天 | S3-3 |
| S3-6 | 集群运维 + 观测 | 2–3 天 | S3-4 |
| S3-7 | M3 验收 + 混沌 | **5–8 天（实验墙钟，不可压缩）** | 全部 |

---

## 10. 开工第一件事 & 待决项

**第一件事（S3-0 + S3-2 并行起步）**：

1. **S3-0**：`crates/proto/proto/meta.proto` 落地 + `build.rs`（`tonic-build`）+ 与现有手写 struct 的**逐字段兼容测试**；
2. **S3-2**：从 `MemoryCatalog` 抽出 `CatalogState` + `apply(op)`，并写第一个**确定性对拍**用例
   （同一 op 串在两实例 apply → 序列化逐字节比对）。**这是 R3 最高风险的落地处**，先做它能在早期就暴露分叉问题。

**待决项（需要 PoC/评审后才能定，**不阻塞开工**）**：

| # | 待决 | 何时定 | 判据 |
|---|---|---|---|
| 1 | raft 库最终选型（raft-rs vs openraft） | S3-1 结束 | §4.2 逃生门准则 |
| 2 | snapshot 保留策略的具体参数（保留 N 天 / 多少条） | S3-3 期间 | 压出增长曲线后再拍 |
| 3 | metanode 全不可用时的写入行为（**拒写 vs 降级**） | S3-4 前 | 建议拒写（恢复语义简单）；若业务要求可用性优先，则必须同时声明"元数据不可用期间的数据可见性不保证" |
| 4 | 是否在 R3 就引入 TLS/鉴权 | 阶段 4 前 | 现在只登记为已知限制 |
