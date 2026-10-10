# yuntun 实施操作日志（§169 起）

> 起记时间：2026-10-11（最新：**§170，2026-10-11**）。对应分支：main。
>
> **这是什么**：从今天起，**每一刀**（一次成体系的改造）在这里留一节 —— 写清"做了什么、
> 为什么这么做、边界在哪（**没做什么**）、怎么验的"。它是本项目的**证据出处**：
> `OVERVIEW.md` / `ARCHITECTURE.md` / `ROADMAP.md` 里凡是写"已具备"的地方，都应能指向这里的一节。
>
> **前 §1…168 在 [`archive/operation-log.md`](archive/operation-log.md)**（2026-10-11 重建时整体归档，
> 内容**不再维护**，冲突时以现状/架构/计划三份为准）。节号**续号不重号**：
> 因此代码与文档里的 `§166` / `§164` / `§88.6` 指归档，`§169` 起指本文件。
>
> **纪律**（来自归档文档，被四次漂移教训固化下来的）：
> 1. 先证据后结论；2. 改默认值必须附**实测**；3. 凡"已具备"的能力，必须有一条**断言其反面后果**的用例；
> 4. 新增事实先写这里，再回改上游文档；5. 行文必须留"为什么"（"不这样做会怎样"）。

---

## 169. **G.1 后台作业的任务目录：多节点看到同一份待办**（2026-10-11）

### 169.0 缺口是怎么定出来的

`§166` 把压缩从"全局一把锁"改成按 **`(table, shard)`** 分租约之后，多节点确实能**同时**干活了。
但"**要合并哪些分片**"这件事仍然是**每个节点各自从清单算出来的**：

- 每个节点每轮都要把全量文件清单拉一遍（元数据读放大，随分片数增长）；
- 彼此**看不见**对方的待办 ⇒ 优先级 / 公平 / 限流无从谈起（谁手快谁做）；
- 运维问"现在有哪些活、谁在做" ⇒ 只能从日志里猜。

归档台账 `D-21` 正是这一条；归档 `plan.md` G.1 给出的候选形态是
"任务表 + `Claim/Renew/Complete` op（proto tag 22+）"。

### 169.1 做了什么

| 层 | 内容 |
|---|---|
| `model`（新 `task.rs`） | `TaskKey`（全限定表 + 分片）/ `TaskReason` / `CompactionTask`（prost 消息）+ 派生阈值 `threshold::{FILES, DV_RATIO, DV_MIN_CARD}` |
| `catalog` | `CatalogState.tasks`（**派生**）；`refresh_task(table, shard, now_ms)` 挂在 `commit_files` / `apply_deletions` / `revoke` / `commit_compaction` 上；`refresh_task_for_file` 供"只知文件路径"的 DV 路径用；快照还原时按还原出来的清单**重算一遍**；`CatalogOps::list_tasks()` |
| `proto` / `meta` | `CompactionTaskMsg` + `PrefetchPayload.tasks = 7` + `StatusResponse.tasks = 14` + op 层双向转换（成对写，避免"过线后字段悄悄丢"）；`NodeHandle::status()` 与 `prefetch` 都把任务表带上 |
| `meta::RemoteCatalog` | 新增 `refresh_full()`（强制全量预取）；`list_tasks()` 走它取任务表 |
| `compaction` | `compaction_round` 的待办来源从"自己 `list_tables` + 拉全量清单 + 本地分组"改成 `catalog.list_tasks()`；认领复用 `§166` 的租约原语；`RoundStats.tables` → `tasks_seen` |
| `meta_probe` | 新增 `tasks` 子命令（输出 `table:shard files=N deleted=M reason=… age_ms=…`） |
| rig | `tests/datanode_cluster.sh` 增加判据 ④：**两个节点都从目录拿到过待办**（`tasks_seen=[1-9]`）；并修掉一条**会让 rig 假绿**的构建坑（见 169.2-4） |

### 169.2 五个决定（都是"不做会怎样"倒推出来的）

1. **任务由状态机派生，不由某个节点扫描产生**。
   派生的输入只有"文件清单 + 删除向量"，两者都是确定性事实 ⇒ **每个副本算出同一张任务表**
   （确定性纪律，归档 `§38`）。反过来若是"某节点扫出来写进去"，就要先解决"谁来扫" ——
   那是又一把租约和一个单点。**代价**：`refresh_task` 要挂在所有会改变"文件/DV"的路径上（见 169.4）。
2. **认领复用既有租约，不新造 `Claim/Renew/Complete` op**（**与归档 `plan.md` G.1 的候选形态不同**）。
   理由：`§166` 的分领原语（purpose + epoch + TTL + 可接管 + 栅栏按 purpose 核）**已经是**一套
   完整的认领协议；再造一套会得到两个真相（"谁在做"该信谁？）。今天的分工是：
   **任务目录回答"有哪些活"，租约回答"谁在做"**，两者拼起来就是运维要看的那张表。
   ⇒ 因此 proto 里**没有**新增 tag 22+ 的 op（这条要写进台账，免得下一位照着旧计划去找）。
3. **阈值取"超集"**（`FILES=2` / `DV_RATIO=0.05` / `DV_MIN_CARD=500` = 执行者默认值的一半）。
   任务只是"**候选**"：执行者仍按自己的 `[compaction]` 判定（更严就跳过）。
   漏登记 = 没人做这件事；多登记 = 有人白看一眼 —— **前者才是事故**。
   单一来源在 `model::task::threshold`（执行者默认值的说明写在同处注释里）。
4. **任务目录不进 `encode_canonical`（对拍基准）、也不进快照**。
   它完全由"文件 + DV"决定：进了规范编码会把"派生逻辑变了"误报成"状态分叉"；
   存进快照会多出一个"派生结果与存储值不一致"的可能。快照还原时**按清单重算**（那本来就是 O(文件数)）。
   **顺带修掉一个会让 rig 假绿的真坑**：`tests/datanode_cluster.sh` 的 build 步骤把
   `--bin/--example`（**全局目标筛选**）与 `-p yuntun-datanode` 写在同一条 `cargo build` 里 ⇒
   datanode 二进制**一直是旧的**（日志里还在打 `tables=` 而不是 `tasks_seen=`）。
   拆成两条命令后才真的验到新代码。
5. **`RemoteCatalog::list_tasks` 必须走全量刷新**（不能走增量 `refresh()`）。
   增量刷新有"什么都没变 ⇒ 直接返回"的零开销路径，而任务表只在**载荷**里下发 ⇒
   走它会让客户端永远抱着第一次看到的那张表（**待办消失不了** = 每轮空转去抢已经不存在的活）。
   代价：一轮压缩多一次全量预取（默认 60s 一轮，任务表很小）。

### 169.3 边界（这一刀**没有**做的）

- **`/metrics` 没有暴露任务与持有者**（归档计划的 G.4）：今天只有 `Status` 与 `meta_probe tasks` 能看到。
- **真集群判据没有实跑**：`tests/datanode_cluster.sh` 的判据 ④ 已写进脚本，但本刀**未跑**
  （需 podman + release 构建；跑它时不要并发跑别的 cargo）。
- **`purge_table_files` 之后不重算任务** ⇒ 残留指向空分片的待办（每轮白抢一次租约）。
  已开台账 **`D-25`**。
- **快照还原后的 `since_ms = 0`** ⇒ `meta_probe tasks` 的 `age_ms` 会显示成"54 年"
  （`now_ms - 0`）。只影响观测口径，不影响调度；归到 `N3`（运维面）一起修。
- **`compaction_round` 不再自己做 `min_files`/DV 预筛** ⇒ 预筛口径**单一来源**变成了任务阈值；
  执行者的判定仍在 `compact_shard` 里（这是刻意的：候选与判定分离）。

### 169.4 验证

- `cargo check --workspace --all-targets` 通过。
- `cargo test -p yuntun-compaction --test shard_lease_distribution` → **8/8**
  （新增 3 条：`task_directory_appears_and_disappears_with_the_work`（任务出现 **且合并完消失**）、
  `no_task_means_no_work`（目录里没有就不干活：单文件 + `min_files=1` 也不做）、
  `deletions_create_a_task_even_for_a_single_file`（DV 够大 ⇒ 单文件也进任务表））。
- `cargo test -p yuntun-meta --test remote_catalog_parity` → **3/3**
  （新增 `task_directory_is_derived_on_both_catalog_forms`：同一串 op 走 raft（远端）与进程内（内存）
  必须派生出**同一条待办** —— 否则"分领"不成立：各节点照自己那份干活）。
- `cargo test -p yuntun-proto --test wire_compat` → **8/8**（`StatusResponse.tasks` 逐字段无损）。
- **全量 `cargo test --workspace`**：除两条**计时断言**型用例在并行争抢下失败外全绿，
  二者单独重跑均 ok（`yuntun-compaction::tests::concurrent_writers_compaction_and_gc_converge`
  的 30s 墙钟收尾上限、`yuntun-query::partial_fanout::slow_sources_wait_in_parallel_not_in_sequence`
  的 400ms 墙钟）⇒ 开台账 **`D-26`**：**"并行全绿"这条口径今天不成立**（不要再用它当准出条件）。
- 反证面（"不这样做会错在哪"）：① 若任务不由状态机派生 ⇒ 两种 catalog 形态的待办会不一致（对拍会红）；
  ② 若 `list_tasks` 走增量刷新 ⇒ 待办消失不了（用例 `task_directory_appears_and_disappears_with_the_work`
  的第二段断言打的正是这条）；③ 若 `compact_shard` 不按自己的配置判定 ⇒ 阈值一变就会"多合并"。

---

## 170. **文档归档重建：现状 / 架构 / 计划 / 交接四份**（2026-10-11）

### 170.0 为什么要做

用户提出"从当前实现重新出发制定架构和计划"，直接原因是**上下文太长**：
`operation-log.md` 已到 10,796 行 / §168 节，`architecture.md` 2,455 行，
`status.md` / `plan.md` / `closeout.md` 三份互相引用且在**同一事实上有过四次漂移**
（`D-15`/`D-17`/`D-19`/`D-24`）。新读者（含下一个 AI 会话）要花很长时间才能分清
"哪句是现状、哪句是意图、哪句已经作废"。

### 170.1 做了什么

| 动作 | 内容 |
|---|---|
| **归档** | 12 份旧文档整体移入 `docs/archive/`（**含 §1…168 的操作日志**），内容冻结、不再维护 |
| **重建** | `docs/README.md`（索引 + 权威顺序 + §号映射 + 同步清单）、`OVERVIEW.md`（现状唯一入口）、`ARCHITECTURE.md`（结构 + 已定案决策 + 配置清单 + 不变量）、`ROADMAP.md`（阶段 + **台账** + 下一阶段 N 组）、`HANDOVER.md`（交接提示词） |
| **新日志** | 本文件从 §169 续号（不重号），§169 补记上一刀的代码（任务目录） |
| **台账搬家** | 归档 `closeout.md` 的**未闭环项**全部搬进 `ROADMAP.md §4`；已闭环项留在归档里作历史 |
| **判据适配** | `crates/testkit/tests/docs_consistency.rs` 按新结构重写（规模行 → `OVERVIEW.md`；ADR-10 定案值 / 配置清单 → `ARCHITECTURE.md`；归档日志索引 + §范围串 → 指向归档与新日志） |

### 170.2 四个决定

1. **归档而不是删除，也不是"就地升版本"**。
   本仓原来的规则是"不新建平行文档、就地升版本"（防止第三份会漂移的东西）。这次是**唯一例外**：
   旧文档的五份之间已经互相矛盾，就地改无法让读者分清"哪个版本是真的"。
   ⇒ 归档后**只有一处现状来源**；`archive/` 的定位写死为"历史，只备查"。
2. **节号续号不重号（§169 起）**，台账编号（`D-*`）**完全沿用**。
   理由很实际：**代码注释里引用着它们**（`D-20`/`D-21`/`D-23` 等）。
   重号会让"代码说 D-20，文档里有两个 D-20"变成新的漂移源。
3. **台账不重写成"任务清单"**，而是原地搬进 `ROADMAP.md §4` 并保持"偏差"语义。
   台账的价值是"发现不符先登记"，把它混进任务表就没人再往里加行了。
4. **欠账照实写**：本轮**没有**修 `D-25`（purge 后不重算任务）、**没有**实跑 rig、
   **没有**解决 `D-26`（并行计时 flaky）。它们各自在台账里有行、在 `ROADMAP.md §5` 有任务号（`N2`/`N3`）。
   —— "归档重建"很容易变成"顺手把账抹平"，这里刻意不这么做。

### 170.3 边界

- 归档文档里可能有**已经作废但未被本轮纠正**的段落（例如归档 `design.md` 的若干阶段 0 章节）。
  它们是"历史记录"，**不承诺准确**；判据只盯新文档与 `config.rs`/`proto` 这类**代码事实**。
- 本轮**没有**动任何产品代码（唯一例外是 `.gitignore` 加了一行挡误重定向产物 `=1`）。
- 归档文档里的 `§1–§N` 范围串不再有意义（它们当时是对的）；新文档一律不写区间串，避免再被自己的判据误伤。

### 170.4 验证

- `cargo test -p yuntun-testkit --test docs_consistency` → 全绿（规模行 / proto 迁移表 /
  ADR-10 定案值 ↔ 配置默认值 / 配置清单键 ↔ 解析器 / 归档日志索引 / 文档里的区间串）。
- 归档前后各跑一次全量测试，结果一致（本轮改动不含产品代码）。
- 直接核对：`docs/` 顶层只剩 6 份（`README.md` / `OVERVIEW.md` / `ARCHITECTURE.md` /
  `ROADMAP.md` / `HANDOVER.md` / `operation-log.md`）+ `archive/` 12 份。
