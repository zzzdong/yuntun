# 偏差台账（closeout）：实现 vs 架构与计划

> **用途**：本仓的纪律是"改代码必须同步文档"（`README.md` 里那份清单），但**"哪些偏差还没闭环"**
> 此前没有一个可核对的清单 —— 它们散落在 `operation-log` 各节、`README.md` 的一句规则、
> 以及若干代码注释里。这份台账只做一件事：**把"未闭环的偏差"列成一张能核对、能划掉的表**。
> 它**不重复** `status.md`（现状入口）与 `operation-log`（证据）。
>
> **这不是一份"承诺书"**：凡**可机械核对**的部分，一律写成判据（见 §4），跑全量就会验。
> "以后记得同步"这条规则已经失败过一次 —— `operation-log` 的头部索引停在 `§32` 整整 90 节没人发现。

---

## 0. 一句话判定（2026-09-26，`§123` 审计）

**能力层相符、架构未被推翻；偏差集中在"登记与文档"这一层，另有 4 条未登记的实质漂移。**

- 核对方式：M2–M6 五条"标称完成"的能力**逐条到代码抽查**，无空壳；`crates/**` 里
  `todo!` / `unimplemented!` / `FIXME` **0 命中**。
- 架构（目标态 `architecture-with-chunk.md`）**没有被现实推翻** —— 差异全是"进度/登记"问题，
  所以本台账**不**提议重写架构文档（那只会再制造一份将来同样会漂移的东西）。

---

## 1. 未闭环偏差（**要决定的**）

> **2026-09-26 起有一条新发现（`D-7`）** —— 台账的意义就在这里：它不是"上次审计的存档"，
> 而是"任何时刻新发现的偏差都先进这儿"。

| # | 偏差 | 现状（证据） | 决定 | 闭环证据 |
|---|---|---|---|---|
| **D-7** | **`T9.x` Vortex 接不进来**：`vortex 0.84` 用 **arrow 58**，本仓被 DataFusion 55 钉在 **arrow 59** | 实测 `cargo add vortex` 后 `Cargo.lock` 里 **58.4.0 与 59.3.0 并存**，`vortex-arrow 0.84 → arrow-array 58.4.0` ⇒ `RecordBatch` **不互通**；另 `vortex 0.86` 要 rustc 1.95（我们 MSRV 1.94）⇒ 只能到 0.84 | **建议选"等"**（vortex 迁到 arrow 59）；另两条路：走 Arrow IPC 边界（有代价）或改 ADR-1 | `§130`（核实 + 已回退，工作区干净） |

> **本节的其余部分为空（2026-09-26，`§127`）。** `§123` 立台账时的四类差异（`D-1`~`D-4`）加上后续新开的
> `D-5`/`D-6` **全部闭环** —— 按本节自己的判据（"§1 空了，偏差才算清完了"），到这里算清完。
> 新发现照 §5 的纪律往这儿加行。

| # | 偏差 | 现状（证据） | 决定 | 闭环证据 |
|---|---|---|---|---|
| **D-1** | **ADR-9 的 `durable` 持久性 SLA 从未实现**（架构要求表级分 `best_effort` / `durable`＝本地 WAL **+ S3 归档**，RPO≈0） | `Durability` 枚举**全仓零引用**（`crates/model/src/lib.rs:69-78`）；`IngestConfig.durability` 恒 0、从不被读 | ✅ **已落地（v1）**：按 **①** 做了 —— WAL 段持续归档到共享存储（含"正在写的段"）+ 丢盘后**拉回本地再走既有恢复通路**；RPO 的界 = **归档间隔 + 一次上传时延**（默认 1s 间隔；"真正的 0"要同步归档，不选） | `§125`；`crates/ingest/tests/wal_archive_durable.rs`（**有对照组**：同场景开关一开一关 ⇒ 8/8 vs 0/0）；接入口 `[wal] archive_prefix`（standalone） |
| **D-2** | compaction 的"**独立 blocking pool** 资源隔离"未落地（设计明确要求"避免挤占 ingest 与 query"） | `docs/design.md:1208` 的要求 vs 实现用普通 `tokio::spawn`（`crates/compaction/src/lib.rs:430-487`）；全仓 `spawn_blocking` 只在 meta 服务层 | ✅ **已落地**：编解码（`format::{read_batch,write_batch}`，两者都是 `async fn` ⇒ 必在运行时里 ⇒ `spawn_blocking` 安全）与 compaction 的 `concat` 全部走**阻塞池** | `§124`；观测配方 `YUNTUN_FORMAT_TRACE=1`（实测 `encode` 的**执行线程**与提交方不同 ⇒ 活真的搬走了） |
| **D-3** | `ProposeRequest.request_id` / `schema_ver` 是**死字段**（设计 §5 规定它们是幂等／OCC 载体） | 客户端恒置空/0（`crates/meta/src/remote_catalog.rs:154-158`），服务端只读 `op`（`crates/meta/src/service.rs:57-74`）；实际由 op 层 `IdempotencyOp` / `EvolveSchemaOp.expected_version` 承担 | ✅ **已决定**：语义由 **op 层**承担（那两处才是权威），proto 保留字段号但**标注为历史字段**（删字段会让老客户端读出 0 而看不出区别） | `§124`：`meta.proto` 的 `ProposeRequest` 上那段注释 |
| **D-4** | R5 的"**partial aggregate 下推** + 冷数据按 datanode 分配"未兑现，**但 M5 已标 ✅** | 数据面回传的是**原始行**（`crates/proto/proto/shard.proto:67-70`）；协调者只 fanout 热数据 | ✅ **已决定**：**把 M5 降级为"部分"**（承诺不该比现实漂亮），两条欠账留在本表（不假装它们是 T13.1/T13.2 的进度） | `§124`：`status.md` 的 M5 行已标"✅（**部分**）"并列明两条欠账 |

> 判据：`§1` 的每一行**要么有"闭环证据"，要么"决定"列不是"待定"**。这张表空了，偏差才算清完了。

---

## 2. 已登记的偏离（只备查，**不行动**）

这些是"**已经决定这么干、并且写清了原因**"的 —— 列在这里就够，别重复登记到 §1：

- Vortex 未引入（feature 占位，`crates/format/src/lib.rs:189-202`；ADR-1 的"主格式"目前是 Parquet）；
- 客户端软路由不做（ADR-10 的另一半）；
- `ProposeResponse.result` 用 `bytes` 而非类型化 `OpResult`；
- datanode 拆分（R3 的**非目标**，R4 里做了，已登记）；
- MySQL wire 协议接入（超出 plan §4.1 当时的"未来"，已在 `sql-access-design.md` + `operation-log` 登记）；
- 容器化真集群 rig（`tests/cluster.sh`：smoke / soak / lossy / netem）+ netem 系列结论（`§112`/`§113`/`§117`）。

---

## 3. 待销账的文档（纯回填）

| 文档 | 欠账 | 状态 |
|---|---|---|
| `plan.md` 任务单元格 | `T10.8`（tonic-build 早已启用）、`T12.1` 跨进程写入面、`T12.3` 心跳循环、`T14.4` 跨节点合并、`T6.15` 清理清单接入 —— 代码已做，表里还标未做 | **待回填** |
| `refactor.md` 状态列 | 同一文件里 S3–S6 一处写"未开始"、一处标全 ✅（自相矛盾） | **待回填** |
| `docs/README.md` 的 `§1–§N` 范围串、`metanode-design.md` 的"待开工"标注 | 过期 | ✅ **已销**（`§123`） |
| `operation-log.md` 头部索引（§号 + 日期）与标题的阶段范围 | 停在 `§32`／"阶段 0 → 阶段 2" | ✅ **已销**（`§123`） |
| `meta.proto` 的"迁移进度"表 | 16 个 op 里有 12 个没列；已迁移的还挂在 ⏳ | ✅ **已销**（`§123`） |
| `plan.md` 版本号 | 头 v2.2 / 落款 v2.1 | ✅ **已销**（统一 v2.2） |
| `status.md` 规模行 | 行数 / crate 数 / 测试函数数 | ✅ **由判据守着**（改代码会红，照着报出来的数字改即可） |

---

## 4. 机制：判据清单（`crates/testkit/tests/docs_consistency.rs`）

| 判据 | 钉住什么 |
|---|---|
| `operation_log_header_index_matches_last_section` | 头部索引（§号 **+ 日期**）== 正文最后一节 |
| `section_ranges_in_docs_match_the_last_section` | 任何文档里的 `§1–§N` 范围串 == 最后一节 |
| `status_md_scale_line_matches_the_code` | 规模行 == 代码实况（**改代码必红**，这是设计） |
| `proto_migration_table_is_honest` | 迁移表覆盖 `oneof kind` 的每个 op；标 ⏳ 的**不得**已存在 |
| `plan_md_version_is_consistent` | `plan.md` 头部与落款版本一致 |

> ⚠️ **写判据的教训（三次都栽在同一处，所以单独记一条）**：判据只能依赖
> **"格式明确的声明行 / 表格行"**，**不能**依赖"文件里出现过某字符串" ——
> 那三条判据分别被**它们自己文档里**提到的 `oneof kind` / `⏳` / `EvolveSchema` 误伤过一遍。
> 散文里必然会提到这些词，判据一旦 grep 全文就会变成噪声。

---

## 5. 维护纪律

1. **发现"实现与文档不符"** → 先在 §1 加一行（**哪怕还没决定**）；决定之后填"决定"，
   闭环之后填"闭环证据"并把它划掉（保留行，便于追溯"当时是怎么定的"）。
2. **可机核的**一律写成判据（§4），而不是写"请记得同步"。
3. 只登记**偏差**：`status.md` 记现状、`operation-log` 记证据与过程、`plan.md` 记任务与门槛 ——
   台账不重复它们，否则它自己也会变成第三个会漂移的地方。

| **D-5** | **datanode 侧没接** `durable`：它缺 `resume_recovered` / `spawn_timeout_monitor` 接线（与 standalone 不一致） | 先查清了代价：**幂等键索引不重建**（重启后重发同一 `client_request_id` 会**再接受一次** ⇒ 数据重复，`§27`）+ `Pending` 批次用新 batch_id 重做（老对象变孤儿）+ **WAL 段永不清理**（磁盘只涨） | ✅ **已闭环**：按 standalone 接线（`resume_recovered` **必须在吸收循环之前** —— 否则 `replay_skip` 被取空等于没建；超时监控；`durable` 的拉回 + 归档循环 + `--wal-archive-prefix` 两个开关） | `§127`；datanode 三条 e2e 全绿；序列本身由 `§125`/`§126` 的 Ingestor 级用例（含对照组）守 |
| **D-6** | `durable` 的**端到端**验收缺：原先只有**机制级**（拉回 → 可重放） | `crates/ingest/tests/wal_archive_durable.rs` 只证到"能读回" | ✅ **已闭环**：补了端到端用例（拉回 → 既有恢复通路 → **重新提交成可见文件**），**带对照组**；并把"整盘"的边界**写成边界声明**（WAL 归档保护的是节点私有盘；连 meta 一起丢不是它的事，靠 meta 自己的 raft 多副本） | `§126`；`crates/ingest/tests/wal_archive_e2e.rs`（有归档 `redone=1` + 1 个可见文件；无归档 `redone=0` + 空） |

---

## 6. 决策备忘：`D-1` ADR-9 的 `durable` SLA（**唯一需要设计决策的一条**）

### 6.1 事实

- `architecture.md` 的 **ADR-9** 规定表级持久性分两档：`best_effort`（默认，本地 WAL）与
  `durable`（"本地 WAL **+ S3 WAL 归档** → RPO≈0"）；
- 代码里 `Durability` 枚举**存在但全仓零引用**（`crates/model/src/lib.rs:69-78`），
  `IngestConfig.durability` 恒为 `0` 且**从不被读** ⇒ `durable` 这条路径**一行都没实现**；
- 它**不在** `plan.md` 的任务表里（`T9.x` 是 Vortex，`T12.x` 是数据节点形态，都没有它）；
- 已登记的只有一句"**RPO 量级表述要同步**"（`operation-log §25`）—— 说的是**措辞**，不是"没实现"。

### 6.2 两个选项

| | ① 补实现（S3 WAL 归档路径） | ② 正式撤回该 ADR |
|---|---|---|
| **做什么** | `IngestConfig.durability = durable` 时：WAL 段**异步上传 S3** + 启动时能从 S3 重建 + RPO 判据（"节点整盘丢失后，已 ack 的数据还在"） | 在 `architecture.md` 的 ADR-9 上标注**已撤回**，写明理由（PoC 阶段不做异地持久性）；`Durability` 枚举与字段标 `reserved`／加"当前不接受 `durable`"的显式校验 |
| **代价** | 大：一条新的数据路径（ingest ↔ store ↔ 启动恢复），且**必须有"整盘丢失"的验收**才敢说 RPO≈0 —— 本仓的纪律不允许"写了就算" | 小：一次文档 + 一处字段校验（选 `durable` 直接报 `BadRequest`，而不是**静默降级成 best_effort** ← 后者才是最危险的形态） |
| **影响** | 推迟其它收尾项；但把"宣称的持久性"变成真的 | 把 ADR 表从"宣称"拉回"现实"：**架构文档不再比实现漂亮** |
| **风险** | 半成品（"能上传但不能重建"）比没做更糟 —— 它会让人**以为**有 RPO≈0 | 若将来真要做，ADR 要重新激活（可接受：届时按新事实重写，比留一句假话好） |

### 6.3 我的建议（不替你定）

**先做 ②，把"选 `durable` 就报错"的显式校验加上**（很小的改动，且**防止静默降级**这种最坏形态），
把 ① 作为**独立的、要排期的数据路径任务**（若确实需要 RPO≈0）。理由：本仓对"承诺 vs 现实"的
纪律，与"宁可少写"是同一条 —— 而 `durable` 现在的状态是**最坏的那种**：文档说有两档、
类型也在，但选它与选 `best_effort` **行为完全一样**，且**没人会收到任何提示**。

### 6.4 结论（2026-09-26）：**已按 ① 落地 v1**

`D-1` 的 D-1 行已闭环：走 ①，v1 = **机制 + standalone 接入 + 机制级验收**（`§125`）。
本节保留作为**取舍记录** —— 特别是"为什么没选同步归档"（那会拿写入时延换 RPO；
真正的 RPO=0 需要 ack 之前等 S3 PUT，是另一个取舍）。剩余两格见 `D-5`（datanode 接线）
与 `D-6`（端到端验收 + "整盘"边界）。
