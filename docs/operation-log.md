# yuntun 实施操作日志（§169 起）

> 起记时间：2026-10-11（最新：**§187，2026-10-16**）。对应分支：main。
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

---

## 171. **N2 测试可信：两条计时 flaky 换成事件判据（并把这条纪律变成判据）**（2026-10-11）

### 171.0 缺口与**归因的更正**

台账 `D-26` 记的是"两条**计时断言**型用例在并行跑全量时失败、单独重跑都 ok"。本轮先复现再动手，
结果推翻了原归因里的一句话：

- **原归因**（`§170` 那版写的）："并行争抢 CPU/IO（chaos 与其余 test binary 互相饿死）"。
- **实测更正**：cargo 是**逐个 test binary 串行**跑的 —— 热态一轮全量：总墙钟 **90s**、各段用时之和
  **86.6s**（`round 1/2/3` 三次分别 93/89/87s vs 91.2/85.9/85.1s）。⇒ chaos 不可能在"饿死"别人。
- **真实原因**：同一个 binary 内**多条重负载用例并行**（`partial_fanout` 里 7 条用例、
  多条自带 `multi_thread worker_threads = 4`；`compaction` 的 lib 测试同理，还有后台循环在跑）。
  ⇒ N2 原本那条"**让 chaos 与其余 binary 隔离**"**不成立，撤回**（不是做完，而是前提错了）；
  真正要做的是把**断言本身**从墙钟改成事件。

### 171.1 做了什么

| # | 改动 | 关键点 |
|---|---|---|
| 1 | **`orphan_cleanup_round` 提成生产函数**（`crates/compaction/src/lib.rs`） | 原来它只存在于 `spawn_orphan_cleanup_with_interval` 的循环体里 ⇒ 用例只能"等墙钟看它跑了没有"。提出来（逐行同语义，三处"拿不到对账基准就整轮不删"由 `continue` 改成 `return None`）之后，用例可以**按轮驱动**到收敛 |
| 2 | 三个 compaction 用例改**按轮驱动** | `concurrent_writers_compaction_and_gc_converge`（两段各 30s 墙钟 ⇒ 改成 `drive_compaction_until_visible` / `drive_gc_until_settled`，上限是**轮数** 64）、`expired_tombstones_are_reclaimed_by_gc`（30s 轮询 ⇒ 32 轮）、`multi_writer_plus_gc_never_deletes_a_committed_file` 与 `gc_never_deletes_an_in_flight_file_even_with_zero_grace`（`sleep(120ms)`/`sleep(300ms)` 赌后台循环扫过 ⇒ 显式驱动一轮）。**写入期间后台循环照跑**（竞态窗口都在那一段），换掉的只是"最后那段等待" |
| 3 | `partial_fanout::slow_sources_wait_in_parallel_not_in_sequence` 改**事件判据** | 新增 `OverlapProbe`（数"此刻有几个来源在飞"并记峰值）：断言"峰值 == 3"。串行实现恒为 1 —— 与机器快慢无关。（原写法 `elapsed < 400ms` 测的是**机器调度速度**） |
| 4 | 两处"必须有界"的兜底**放宽并写明理由** | `partial_fanout` 的预算用例（来源延迟 2s→**5s**，界 1s→**2s**）、`shardrpc::hung_source_degrades`（`< 3s` → `< HUNG_TIMEOUT * 20`）。它们防的是**永不返回**，不是延迟度量 |
| 5 | **新判据** `wall_clock_assertions_need_a_reviewed_waiver` | 扫 `crates/**/*.rs` 的**代码行**（不认注释行、跳过判据文件自己）里出现 `elapsed <` / `elapsed >` 的地方：必须在本行或前 15 行内有 `§171` 标记（当 waiver 用，与 `#[allow]` 同理）。**没有它，下一位会再写一条** |
| 6 | 顺带修 `§169` 的收尾遗留（台账 **`D-27`**） | 全仓 clippy 跑出 **4 条告警**（catalog 2 条：`unnecessary_sort_by` + `empty_line_after_doc_comments`；meta 2 条：同型），根因是 §169 的插入把 `next_snapshot` / `refresh` 的**文档注释与函数拆散**了（`refresh_full` 被插在 `refresh` 的 doc 与函数体之间）⇒ 一并修正，全仓回到 **0 告警** |
| 7 | chaos 的 `compaction_during_query_keeps_counts_monotonic`：采样覆盖改成**构造性同步**（台账 **`D-28`**） | 它的门槛原是"3 轮 **且** 样本数 ≥3（带 30s 超时）"—— 看着与耗时无关，但**第 4 次全量真的失败了**：30s 只采到 **1** 份（单独跑同一条 0.38s）。改成：采样任务用 `tokio::sync::watch` 广播"已采到几份"，主循环在**每轮合并前/后各等一份** ⇒ 覆盖由构造保证（等待带 120s 防挂住）|
| 8 | `cache_lazy_refresh_e2e::query_path_refreshes_its_own_cache_before_planning`：节流窗口**显式设成 3600s**（`D-28`） | 第 4 次全量里同样失败：③ 的断言是"窗口内不问版本"，而它默认窗口只有 **200ms** ⇒ 断言实际在赌"两次查询间隔 < 200ms"（机器快慢的函数）。用**已有的** `set_freshness_check_interval` 把窗口拉远，④ 再调回 `ZERO` 覆盖另一半 |

### 171.2 三个决定

1. **修的是判据，不是阈值**。"把 400ms 放宽到 2s"能让用例变绿，但判据仍然是墙钟 ——
   下一位照样能写出下一条。所以：能表达成**事件**的（并发度、收敛轮数）一律换事件；
   只有"必须有界"这一类保留时间上界，并且**必须写明它在防什么**（waiver 机制把"必须解释"变成硬要求）。
2. **辅助函数提到生产代码里**，而不是在测试里复制一份清理逻辑。理由：复制一份就会漂移
   （"测试里的对账"与"生产里的对账"是两套），而提出来之后它还是个**可确定性驱动的单元**
   —— 本仓"删错文件是真丢数据"的那类不变量正需要这种颗粒度。
3. **"隔离 chaos"这条撤回，并保留撤回记录**。原计划写的是"chaos 与其余 binary 不再互相饿死"，
   而实测证明它们本来就串行跑 —— 把它做成一个脚本反而会**掩盖**真实原因（intra-binary 并行）。
   ⇒ 状态列写"❌ 撤回（前提不成立，见 `§171.0`）"，而不是"✅ 已做"。

### 171.3 边界（这一刀**没有**做的）

- chaos 里其它 **60s 轮询上限**没动：它们是"防挂住"的兜底（失败信息也够用）。
  ⚠️ **一条被证伪的判断要留档**：本刀第一次写这段时写的是"3 轮全量全绿 ⇒ 没有证据说要改"，
  而**第 4 次**全量就冒出两条同类失败（171.1-7/8）⇒ 教训是 **"N 轮全绿"不是"没有证据"**，
  它是"证据还不够"；对**同一类**断言，正确做法是**一次审完**（本轮最后就是把 compaction 测试模块
  整个审了一遍，而不是只修报出来的那两条）。
- `crates/meta/tests/*` 的 30s 轮询上限（多节点进程用例）同理未动（要起真进程，事件化成本高）。
- **测量类**用例（`chaos/examples/bench*`）里的时间是**被测对象本身**，不适用这条规则
  （`is_clock_assertion` 只认 `elapsed <`/`elapsed >` 这种"拿用时当判据"的形状）。
- 判据的**扫描范围是"代码行"**：形如 `if start.elapsed() > idle { … }` 的**生产逻辑**也在范围内
  （它同样会因机器慢而改变行为）—— 目前全仓只有两处豁免，都在测试里。

### 171.4 验证

- **N2 的验收**（`ROADMAP.md §3`）：热态 `cargo test --workspace` 全绿
  —— **`exit=0` / `FAILED` 0 行 / 每轮 104 段 `test result: ok`**，连续 **4 组**：
  改造前 3 轮墙钟 93/89/87s（各段之和 91.2/85.9/85.1s —— 这组数字本身就是 `§171.0` 的
  "逐 binary 串行"证据），收口后再 **3 轮 95/92/89s**。
- chaos 二进制：**17/17、3.15s**（此前并行跑 **30–73s**，且第 4 次跑出 1 失败）——
  串行闸门本来就是全覆盖的（17/17 都 `CHAOS_GATE`），慢的不是"抢锁"，是**采样覆盖**那条断言
  （171.1-7）。修完连跑 3 轮：3/4/3s，全绿。
- **反证 ①（判据有区分力）**：把 `probe.peak()` 的期望从 3 改成 1 ⇒ 用例失败并报出
  `峰值 3`（left: 3, right: 1）—— 说明"并发"是真的被观测到了，而不是恒真断言。
- **反证 ②（新判据有牙齿）**：故意注入一条无豁免的 `let _sanity = elapsed < Duration::from_secs(9);`
  ⇒ `wall_clock_assertions_need_a_reviewed_waiver` 红，并直接报出**文件:行号与那一行**；撤销后即绿。
- 时间收益：compaction 的 lib 测试段 **30.78s → 0.42s**（按轮驱动不再等墙钟）。
- `cargo clippy --workspace --all-targets`：**0 告警**（此前 4 条，见 171.1-6）；
  `cargo test -p yuntun-testkit --test docs_consistency` 全绿（10 条判据）。

---

## 172. **N3 / G.1 收尾：任务目录的接线补全 + 运维面（`/metrics` 的任务与租约）**（2026-10-11）

### 172.0 缺口（比计划里写的多两处 + rig 暴露一处）

`ROADMAP.md §5` 的 N3 写了四件：① `/metrics` 暴露任务与租约（G.4）；② rig 实跑判据④；
③ `D-25`（`purge` 之后重算任务）；④ `since_ms = 0` 的 `age_ms` 口径。动手前的审计又多出三处：

- **接线不完整**：`§169` 的 `refresh_task` 只挂在 `commit_files` / `apply_deletions` / `revoke` /
  `commit_compaction` 上，而**`apply_update`（UPDATE 既加新文件又改 DV）与 `drop_shard`（整个分片下线）**
  也会让"待办该出现/该消失"⇒ 台账 **`D-29`**。
- **时钟没贯通**：那几处刷新传的是 `0`（`since_ms` 永远是"未知"）——而 **op 上本来就带着 `now_ms`**
  （`StateOp::now_ms()`），只是没往下传。
- **rig 暴露的真缺陷**：`tests/datanode_cluster.sh smoke` 第一次跑**失败**在 ③
  （`error: rpc: code: 'Internal error', message: "非 leader（可重试）：not leader (leader hint 2)"`）
  —— 根因不是偶发：**datanode 的 `--meta` 只接一个 contact point**（`RemoteCatalog::connect(vec![addr])`），
  那一台不是 leader 时这个数据节点的**所有写都失败**，而重试只能在**已知地址集合**里换。
  `§167` 那次跑通只是因为当时**恰巧** leader 是 mn1 ⇒ 台账 **`D-30`**。

### 172.1 做了什么

| # | 改动 | 关键点 |
|---|---|---|
| 1 | **接线补全**（`catalog`） | `purge_table_files` 之后按**受影响的每个分片**重算（`D-25`）；`apply_update` 之后按"新文件的 (表,分片)"与"DV 锚定的文件"两路重算（`D-29`）；`drop_shard` 之后重算该分片（`D-29` 同族） |
| 2 | **时钟贯通**（`state` ← `op`） | `apply_deletions` / `revoke_deletions_for_file` / `purge_table_files` / `drop_shard` 都收 `now_ms`，op 层传 `now`（op 携带的钟，纪律 1）；`commit_compaction` 那条**没有**墙钟（靠租约 epoch 表达时序）⇒ 如实传 0 并写明理由 |
| 3 | **`since_ms` 口径**（`model`/`state`/`meta_probe`） | `0 = 未知`（快照不进任务目录 ⇒ 还原出来的任务不知道自己"挂了多久"）；`refresh_task` 只在"原来未知/不存在"时补钟、**已有值一律保留**；`meta_probe tasks` 把未知渲染成 `age_ms=unknown`，不再算 `now - 0`（那会是 54 年） |
| 4 | **租约快照接缝**（`catalog` + `proto` + `meta`） | 新增 `CatalogOps::list_leases()`：内存形态读状态机（BTreeMap 天然有序）；远端形态走 `Status` 现取（**不缓存** —— 租约有 TTL，缓存只会给过期答案）。`LeaseView` 补 `granted_at_ms`（tag 5）：运维的下一个问题是"这件事做了多久" |
| 5 | **`/metrics` 的作业面**（`server`） | `LakehouseMetrics.jobs`：`tasks_total` / `tasks_unknown_age` / 每条待办（表、分片、文件数、删除行数、原因、**年龄**）/ `leases_total` / `leases_held` / 每把租约（用途、持有者、代次、持有秒、剩余秒）+ **两个 `*_error`**（"读不到"与"没有"必须能分清）。Prometheus 侧**每行带标签**（`yuntun_compaction_task_*` / `yuntun_lease_*`），标签值转义、`# TYPE` 去重 |
| 6 | **`datanode --meta` 改成种子列表**（`datanode` + compose） | 逗号分隔、可给多台（compose 里给全三台）⇒ 写落到非 leader 时能在已知集合里换；帮助文本写清"只给一台 ⇒ 那台不是 leader 时所有写失败" |
| 7 | **rig 判据④的证据加强**（`tests/datanode_cluster.sh`） | 在等待循环里抓一份**非空任务快照**（抓不到就如实说明"两次轮询之间就被消费掉了"）+ 打印两个节点的**第一条 `tasks_seen=[1-9]` 轮次日志**；另修掉脚本里三处**反引号被 bash 当命令替换**（`command not found`，而 `say` 的输出本身就是判据 ⇒ 证据会被悄悄弄脏） |

### 172.2 五个决定

1. **把"任务目录的接线规则"写成一张入口清单**（`ARCHITECTURE.md §6`）。
   `§169` 的教训是"派生量要靠人手挂到每个入口上"——所以必须**列出入口**并加一条不变量（I8 扩写）：
   *任何改"文件清单或删除向量"的入口都必须重算对应分片*。今天这份清单是：
   `commit_files` / `apply_deletions` / `revoke_deletions_for_file` / `purge_table_files` /
   `apply_update` / `drop_shard` / `commit_compaction` / 快照还原。
2. **`since_ms` 用"0 = 未知"，而不是"补一个当时的本地钟"**。
   任务目录**不进快照**（它是派生态），所以还原之后确实**不知道**年龄；在还原路径读本地钟会让
   **不同副本**给出不同的年龄（副本间状态分叉的观测版）。宁可如实报 unknown —— 而且它**只影响观测**，
   不参与任何判定（所以"未知"是可接受的代价，而"编一个数字"不是）。
3. **任务走版本驱动刷新、租约走 `Status` 现取**（两种数据的时效性不同）。
   顺带**改正了 `§169` 的一条判断**：它写"`list_tasks` 必须走全量刷新，否则待办消失不了" ——
   复读后确认**不成立**（任务表是"文件 + DV"的**纯函数**，而任何改动都会推版本 ⇒ 探测必能看出变化；
   零开销路径只在"版本没变"时走，那时任务也不可能变）。代价差别很大：全量刷新 = 整个目录一次预取，
   而 `/metrics`（Prometheus 默认 15s）与每轮压缩都会调它。⇒ 改成 `refresh()`，
   并补一条**远端专属用例**钉住它（`remote_task_directory_follows_purge`）。删掉了因此变成死代码的 `refresh_full`。
4. **rig 判据的"证据"要能自解释**。原判据只看日志里出现过 `tasks_seen>=1`；现在同时抓
   任务表的**非空快照**（运维真正看的那张表）与两条轮次日志 —— 抓不到快照时**如实说明原因**
   （待办在两次轮询之间被消费掉），而不是让"(无待办)"看起来像"没有活"。
5. **`--meta` 单点不改协议、只改装配**（给种子列表），并把"跟随 leader hint"留在台账里。
   理由：hint 是个 **id**，客户端要有 id→地址的映射才能用（那是成员表的事，`§118` 之后才有）；
   而"给全三台"今天就能解决，且与 metanode 的 `--peer=a@b,c@d` 同一手感。

### 172.3 边界（这一刀**没有**做的）

- **任务与租约没有"关联视图"**：`/metrics` 里两张表各自独立，靠 `purpose` 前缀
  （`task:<表>:<分片>`）人工对应。要做成"谁在做哪条待办"需要一次联接（`G.4` 的下一格）。
- **没有跟随 leader hint**（见 172.2-5）：今天靠"给全地址"。故障形态是"所有已知地址都不是 leader"
  （多数派丢失）——那种情况本来就写不进去。
- **GC 租约的 TTL 下限 30s 没动**（生产决策："一轮扫得比间隔久也不至于被误接管"）⇒
  `crates/datanode/tests/compaction_e2e.rs` 里"过 TTL 后接管"这条**天生要跑 ~30s**。
- `/metrics.json` 的 `jobs.tasks` / `jobs.leases` 是数组，**未做截断/分页**：今天待办与租约的量级是
  "每分片一条 / 每用途一把"，可控；上万条时要再加"只报前 N 条 + 总数"。

### 172.4 验证

- **单测/集成**：`yuntun-catalog` **60/60**（新增 5 条：purge/update/drop_shard 各让任务消失或出现、
  `since_ms` 不许被刷新重置、还原后如实报未知）；`yuntun-meta::remote_catalog_parity` **4/4**
  （新增 `remote_task_directory_follows_purge`）；`yuntun-server` 指标单测 **3/3**
  （新增 `job_rows_carry_labels_and_never_invent_an_age`：带标签行 + 转义 + 未知年龄不许编数字 + TYPE 去重）；
  `yuntun-proto::wire_compat` **8/8**（`LeaseView` 含 `granted_at_ms` 与 `tasks` 一起逐字段无损）。
- **反证（用例有牙齿）**：临时把 `purge_table_files` 里的重算注掉 ⇒
  `purge_drops_the_task_of_every_touched_shard` **红**（报出残留的
  `CompactionTask { files: 2, since_ms: 1002000 }` 正是"空分片的待办永不消失"那个形状），恢复后绿。
- **真集群 rig**（podman 6.1.2，`tests/datanode_cluster.sh smoke`）**EXIT=0**，判据④的实测输出：

  ```
  任务目录（抓到过的第一份非空快照）：
    public.dist:s0 files=2 deleted=0 reason=files age_ms=0
    public.dist:s1 files=2 deleted=0 reason=files age_ms=0
    public.dist:s2 files=2 deleted=0 reason=files age_ms=0
    public.dist:s3 files=2 deleted=0 reason=files age_ms=0
  两个节点都拿到过待办 —— 各自的第一条轮次日志：
    dn1: compaction round tasks_seen=4 candidates=1 compacted=1 lease_busy=0 failed=0
    dn2: compaction round tasks_seen=3 candidates=1 compacted=1 lease_busy=0 failed=0
  ```

  （③ 两个节点各查 24 行；⑤ 两个节点都真的合并过；⑥ 行数守恒 24、**0 次瞬时 21**；
  ⑦ `gc=gc-dn1:1` 单持有；⑧ 杀掉干活的那台 ⇒ 另一台接手。另：修 `D-30` **之前**同一脚本失败在 ③，
  现场就是 `not leader (leader hint 2)` —— 这条失败的现场保留了，成了 `D-30` 的证据。）
- **全量回归**：`cargo test --workspace` **exit=0**（0 failed；文档判据同步后全绿）。
- **顺带审计了 `§171` 那一类的剩余风险**：扫了其它 `wait_for(30s)` 上界（`meta` 的进程用例等），
  它们等的是"选举/进程就绪"（秒级）而不是"必须等满一个 TTL" ⇒ 不存在"上界 == 被等待量"的形状；
  唯一一处正是本刀修的 `compaction_e2e`（GC 租约 TTL 下限 30s vs 30s 上界）。

---

## 173. **N4 上半刀：可分解聚合的下推**（G.2 / 归档 `S5-3`）（2026-10-11）

### 173.0 缺口与这一刀的范围

`ROADMAP.md` 的 N4 写的是「`(table, shard) → owner` + 协调者把冷文件（及谓词/聚合）下推给持有者」，
验收「对拍逐行相等 + **网络传输量下降一个量级**」。摸底（`code-explorer`）之后先摆清三件事：

- **热数据今天是怎么回来的**：协调者按名录逐个实例 `read_table_excluding` ⇒ `shard.proto` 的
  `batches_ipc` —— **原始行的 arrow IPC**（`shard.proto:67-70`）；
- **冷数据今天根本没"下推"**：协调者自己从**共享存储**读全部 manifest 文件
  （`crates/query/src/table.rs:552+`），`source_instance` **只**用于热读归属（不算文件归属）；
- **数据面没有聚合能力**：`ShardService` 只持一个 `Arc<dyn ShardReader>`（行语义）——
  datanode 进程里那个 DataFusion `QueryEngine` 与 RPC handler **够不着**（`main.rs:610` vs `shardrpc`）。

⇒ 这一刀只做**可交付、可验收的那一半**：**可分解聚合的下推**（把网络里的"行"换成"标量"）。
冷文件的"归属/分配"（S5-2）需要一条"读指定文件/替我这批文件算"的新 RPC，**留作 N4b**；
谓词与 `GROUP BY` 同样留作后续（见 173.3 的边界）。

### 173.1 做了什么

| # | 层 | 改动 |
|---|---|---|
| 1 | **判定层**（`model::agg`，新模块） | `AggKind` / `AggSpec` / `AggScalar` / `PartialAgg` + `fold_batch(es)` / `merge` / `merge_all`。**只收** `count(*)` / `count(col)` / `sum(整数)` / `min` / `max`（整数与字符串）——"为什么"写在模块文档里（可分解 + **精确** 两条） |
| 2 | **协议**（`shard.proto`） | 新 RPC `FetchShardAggs` + `AggSpecMsg` / `PartialAggMsg` / `FetchShardAggs{Request,Response}`。响应**与请求同序同长**（顺序是契约）；`PartialAggMsg` 用显式 `has_sum` / `has_scalar` 标志表达 NULL（proto3 的 `optional` 不够直白，而"NULL 与 0 必须能分开"） |
| 3 | **数据面**（`store` + `shardrpc`） | `ShardAggs`；`ShardReader::read_table_aggs`（**默认实现 = 读行 + 折叠**，进程内来源用它）；`ShardFetch::fetch_shard_aggs`（默认同上，**给假实现留路**）；`RemoteShard` 与 `GrpcShardFetch` **覆写**成走新 RPC（这一步才是"下推"的落点）；服务端 handler 直接调本地 `read_table_aggs`（**同一份折叠代码** ⇒ 两端语义逐字相同） |
| 4 | **协调者**（`query`） | `agg_pushdown::detect`（**形状判定**：拿不准一律不下推）+ `merge_into_cold`（把热的部分状态并进**冷结果的那一行**）；`ReadPath.hot_enabled`（只读冷数据的会话）；`QueryEngine::aggregate_pushdown`（冷的一半交给 DataFusion + 热的一半走聚合 RPC + 合并）；`sql_with_partial` 里先试它 |
| 5 | **对拍开关** | `with_agg_pushdown(bool)`（默认开）：关掉只为一件事 —— 两条路径跑同一批数据**逐位比**（与 `with_index_pruning` 同一性质，不是运维旋钮） |

### 173.2 四个决定

1. **冷的一半仍然交给 DataFusion，只把热的一半下推**。
   这样 **schema / 列名 / 类型 / 可空性**都由 DataFusion 给（它是"原路"的产出），我们**只改值**。
   自己拼一个"看起来一样"的结果集，就是在跟 DataFusion 的列名（`count(*)`）、`count` 的返回类型
   （`Int64` vs `UInt64`）赛跑 —— 那正是对拍会红的地方。
2. **下推边界由"可分解 **且** 精确"两条判定**，不是由"能算就算"：
   `sum` 只收整数（**浮点求和不满足结合律**：`(a+b)+c` 与分部相加的最后几位会不同 ⇒ 对拍不可能逐位相等）；
   `min/max` 只收整数与字符串（浮点的 `NaN` 在 arrow 里有"视作最大"的特殊口径，要跨节点合并就得先复刻它，
   而"复刻得差一点"的风险高于收益）。`avg` / `stddev` / `count(distinct)` 一律原路。
3. **判定只认最窄的形状，拿不准就返回 `None`**（`WHERE` / `GROUP BY` / `HAVING` / `LIMIT` / `ORDER BY` /
   `DISTINCT` / `QUALIFY` / `INTO` / JOIN / 子查询 / CTE / UNION / 别名 / 表函数 / 表达式参数…）。
   与状态机那边的纪律方向相反但道理相同：**不下推永远是正确且稍慢的；推错了是结果悄悄错**。
4. **部分状态必须带"NULL vs 0"的区分**（`has_sum` / `has_scalar`）：空集 `count(*)=0` 而 `sum=NULL`。
   把 NULL 当 0 会让"空表求和"变成 0 —— 一个**看起来很正常**的错值（这一刀最怕的形态）。

### 173.3 边界（这一刀**没有**做的）

- **谓词不下推**：有 `WHERE` 就整条原路（本刀热的一半只算整表）。⇒ `SELECT count(*) FROM t WHERE …`
  仍然拉全部行。要做需要把谓词也送过去（那是**另一条契约**：表达式过网）。
- **`GROUP BY` 不下推**：每个分组各自的部分状态需要一个"分组键 + 状态列表"的响应形状。
- **`avg` / 浮点 `sum` / `min/max` 浮点列不下推**（见 173.2-2）：这三类在原路里照样正确，只是不过网省不了。
- **冷文件不分配**（`S5-2`）：协调者仍自己从共享存储读全部冷文件。要做需要"读指定文件"的新 RPC
  （且要先定"谁来分配"——今天**没有**文件 owner 字段，`source_instance` 只是**热副本**归属）。
- **`LIMIT` 下推**（`count(*) … LIMIT 1` 这类）：不做，整条原路。

### 173.4 验证

- **对拍（逐位 + schema）**：`crates/query/tests/aggregate_pushdown.rs`
  `pushed_down_aggregates_match_the_row_path_bit_for_bit` —— 真冷文件（含 NULL）+ 三个真 chunk
  （含 NULL、含空来源），9 条查询：5 条可下推、4 条必须退回。断言三件事：
  ① 值与参照**逐位相同**（规范化后比：批次切分与行序不在契约里，值才是）；
  ② **schema 也相同**（列名/类型被我们动过同样是静默破坏）；
  ③ **该下推的真的下了推、不该下推的一次都没打**（否则"对拍通过"可能只是原路跑了两遍 —— 那种绿没有意义）。
- **网络量（`ROADMAP` N4 的验收）**：`pushed_down_pull_moves_scalars_not_rows` —— 假远端按**真实编码**
  （`shardrpc::encode_batch`）计上网字节：**行路径 244,104 字节 vs 聚合路径 64 字节（3814×）**，
  3 万行整数。断言 `行 ≥ 聚合 × 10`，并要求两条路的答案一致。
- **降级语义**（`missing_sources_degrade_the_same_way_for_aggregates`）：`Allow` ⇒ 带标记、点名 `inst-bad`、
  计数只算拿得到的那部分（**下界**）；`Reject` ⇒ 当场失败并点名。与行路径**逐字同策**。
- **STALE**（`stale_sources_never_leak_into_a_pushed_down_answer`）：整次尝试失败、点名实例与 STALE
  （**不降级成 partial**）—— 聚合少算一个来源会静默偏小，这条必须最严。
- **协议往返**：`crates/shardrpc/tests/grpc_shard_fetch.rs::aggregate_pull_survives_the_wire` ——
  真 gRPC + 真编码：过网与进程内**逐位相同**、长度/顺序是契约、水位与 `stale` 一起过网、空集是 NULL。
- **全量**：`cargo test --workspace` **exit=0**（0 failed / 85 段；文档规模行同步后全绿）；
  `cargo clippy --workspace --all-targets` **0 告警**。

---

## 174. **D-23 归因：观测缺的那一层信息（降级 vs 缺口）+ 状态机穷举排除**（2026-10-11）

### 174.0 缺口的真身：那条观测**无法归因**

台账 `D-23` 记的是"容器 rig 在压缩提交期间观测到**一次瞬时少行**（24 → 21），随后回到 24，未复现"。
本刀先问了一个更基本的问题：**那次观测凭什么能归因？** 答案是**不能** ——

- rig 的判据 ⑥ 当时只做两件事：轮询 `SELECT count(*)`、把 `== 21` 记成一次"dip"。
- 但它**没看**"结果是不是降级来的"：`[query] partial = "allow"` 时，**任何一个来源读不到**
  都会让这次查询**少算**（`D-23` 的 21 与"缺一个来源"在数字上无法区分）。
- 而整条链上**没人读**那个 partial 标记：服务端确实把它写进了 Flight 的 `app_metadata`
  （`flight_partial_metadata.rs` 守着），但**客户端 SDK 与 CLI 都没解析它** ——
  运维看到的只是"21"，拿不到"为什么是 21"。

⇒ 这次观测的缺口不是"少了一行"，而是**少了一层信息**。**先补信息，再谈复现。**

### 174.1 做了什么

| # | 层 | 改动 |
|---|---|---|
| 1 | **客户端 SDK**（`yuntun-client`） | 新增 `PartialNotice` / `QueryOutcome` 与 `query_full` / `query_stream_full`：**先看第一条 `FlightData` 的 `app_metadata`**（partial 标记就挂在那里），再往下流 ⇒ 不必等整个结果集就知道"这次不完整"。`query` / `query_stream` 保留原签名但会 `tracing::warn!`，不留"降级了却没人知道"的静默路径 |
| 2 | **CLI**（`yuntun-cli query`） | 有降级时往 **stderr** 打一行稳定标记 `WARN: partial result —— 部分结果：…`（点名缺失来源），**stdout 只放数据**（CSV/JSON 是管道输出）。脚本/rig 判据据此区分两类少行 |
| 3 | **rig 判据 ⑥**（`tests/datanode_cluster.sh`） | 把"瞬时少行"拆成两类：**降级**（有 `WARN: partial result`）计数并打印，**不算失败**；**缺口**（没有标记却少行）⇒ **当场失败**。另外两条流**分开**取（stderr 捕获、stdout 落文件）—— 警告文本里也可能出现数字 |
| 4 | **状态机穷举判据**（`crates/catalog/src/state.rs`） | `compaction_never_dips_or_double_counts_any_snapshot`：把压缩提交前后的**每一个快照号**都过一遍，断言 ①行数**单调不减**（不许缺口）②不许超过总数（不许双计）③**产物与输入不许在同一快照里同时可见**④提交前一个快照必须完整 |

### 174.2 决定：先补信息，再谈复现

1. **"降级"必须能被脚本区分**（判据 1-3）。否则"少行"这个词同时指两种完全不同的东西，
   而它们的处置相反：降级是**刻意的**（`partial = "allow"`），缺口是**数据丢了**。
2. **状态机那一半用穷举排除，而不是"再跑几次 rig 看看"**（判据 4）。
   可见性是 `valid_from <= snapshot < deleted_at` 这一条不等式，写错一处就只错**一个快照号** ——
   在真集群里等运气去撞那一个号，性价比极低；逐个号断言则是**确定性**的，
   而且反证（把产物推迟一个号）当场就能让它红。
3. **SDK 的默认行为选择"至少留痕"**：`query()` 仍返回批次（调用方要的就是这个），
   但它会把降级事实写进 `tracing::warn!` —— 想拿到结构化事实的调用方用 `query_full`。

### 174.3 新观测：**这次抓到的是 +3（双计方向），不是 -3**

补完判据后第一次跑 rig，判据 ⑥ 直接报红：

```
count(*) = 24（期间瞬时少行：**降级** 0 次 / **缺口** 2 次）
❌ 出现「没有 partial 标记却少行」的观测 ⇒ 那是**可见性缺口**（不是降级）：got=27；stderr=
```

**关键点**：观测到的是 **27 = 24 + 3**（**双计**），而**不是** 21。旧判据只数 `== 21`，
所以它对 +3 这个方向**完全盲** —— 这解释了为什么这类观测这些年只在日志里出现过一次。
（第二次复跑是 `0 降级 / 0 缺口` ⇒ 出现频率低，两次运行里命中一次。）

同一个"±3"家族，两个方向都观测到了 ⇒ 现象是"**某 3 行在某一段时间里被算了两次或没算**"。

### 174.4 已排除的两半（都带代码依据）

1. **状态机不可能**：`compaction_never_dips_or_double_counts_any_snapshot` 穷举所有快照号通过；
   反证（把 `nf.valid_from = next` 改成 `next + 1`）当场报出
   `快照 4 的行数比上一个还少（14 → 0）—— 可见性出现缺口`。
   ⇒ 提交的两半（旧文件 `deleted_at = next`、产物 `valid_from = next`）在**同一个号**上无缝交接。
2. **热副本不可能"多算一行已提交的批"**：`Chunk::visible(cached_snapshot)` 的判据是
   `cached_snapshot < committed_snapshot`（`crates/chunk/src/chunk.rs:456`）——
   **已提交进 manifest 的批在任何不早于它的快照上都不会从热副本出**；
   而"提交与标记之间的窗口"由 `scan` 的 `known_batch_ids` 栅栏兜住
   （`crates/query/src/table.rs`：exclude 取**本次快照可见文件**的 batch_id）。
   两层叠加后，这一侧没有漏出 3 行的形状。

### 174.5 尚无定论的候选（下一刀的入口）

排除上面两半之后，剩下的只能落在**客户端缓存视图**这一层。最可疑的一处已经定位到具体代码：

- **候选：`RemoteCatalog::refresh()` 把"快照号"**先于**载荷发布出去**
  （`crates/meta/src/remote_catalog.rs`：探测步里 `c.snapshot = probe.snapshot`，
  而文件清单要等第二步的载荷到达才 `apply()`）。
  于是存在一个窗口：**缓存对外报的版本号是新的，而它手里的文件清单还是旧的**。
  两次刷新交叠时更糟 —— 第二次会带着**新号**去问增量，而 `files_since` 用的是 `>` 判据
  （`valid_from > since || deleted_at > since`），**恰好等于 `since` 的墓碑会被漏掉**。
- 与观测的吻合度：一个"清单与版本号错半拍"的视图，正好能产生 **±3**（少一个墓碑 ⇒ 多 3 行；
  多一个墓碑 ⇒ 少 3 行）。而**纯 compactions 的行数守恒**（产物行数 == 输入行数之和）
  会把"整体错位"遮蔽掉 —— 只有**错到一半**（一个墓碑到位、另一个没到）才显形。
  这也解释了它为什么这么难抓。

**下一刀的计划**（尚未做，写在这里免得丢）：给 `RemoteCatalog` 加一条不变量
**"快照号与文件清单必须同版本落地"**（刷新期间不得对外发布半新半旧的视图），
先写一条能**确定性触发**该窗口的用例（在探测与载荷之间注入一个提交），再改实现。

### 174.6 验证

- **客户端/CLI 那一跳**（`crates/client/tests/client_e2e.rs::a_degraded_query_is_flagged_as_partial`）：
  in-process 服务端**注入一个读不到的来源**（`register_datanode` + 一个永失败的热读器）⇒
  断言 ①完整时 `partial.is_none()`（**假警报同样是错**）②降级时 SDK 报 partial、**点名 `inst-ghost`**、
  **原因可辨**（`connection refused`）③可用部分照常返回 ④**CLI 退 0**、stderr 含
  `WARN: partial result` + 点名、**stdout 不含 WARN**（不污染管道）。
- **解析边界单测**（`crates/client/src/lib.rs`）：空 metadata / `partial:false` / 非 JSON / 别的 JSON
  一律**不算**降级；`partial:true` 缺 `detail` 也要报（给兜底文案）。方向是刻意的：
  假警报会让调用方学会无视这个字段，于是真降级又变回静默。
- **状态机穷举**：`compaction_never_dips_or_double_counts_any_snapshot` 通过；
  **反证**（把 `nf.valid_from = next` 改成 `next + 1`）⇒ 报出
  `快照 4 的行数比上一个还少（14 → 0）—— 可见性出现缺口`，恢复后绿。
- **rig 实跑**（podman，`tests/datanode_cluster.sh smoke`）：第一次跑判据 ⑥ 报红并打印
  `降级 0 次 / 缺口 2 次`（`got=27`）——**这是真阳性，不是脚本噪声**（第二次复跑 0/0，
  说明频率低）。⇒ 判据保持在"缺口即失败"，**不掩盖**这条观测。
- 全套：`cargo test --workspace` 与 `cargo clippy --workspace --all-targets`（见提交信息）。

---

## 175. **N5 收口：快照号与文件清单必须同版本落地**（台账 `D-23` 闭环）（2026-10-11）

### 175.0 机制定案

`§174` 把范围收窄到"客户端缓存视图"这一层，并把最可疑的一处指到了
`RemoteCatalog::refresh()` —— 它把**快照号**先于**文件清单**发布出去。这一刀把机制钉死：

`refresh()` 是**两跳**：
1. **探测**（`prefetch{full=false, tables=[]}`）→ 拿 `schema_ver` / `manifest_ver` / `snapshot`；
2. **载荷**（全量或按 `changed_tables` 拉）→ 拿文件，`apply()` 里落 `c.snapshot = resp.snapshot`。

而**载荷请求**带的 `since_snapshot` 取的是**缓存当时的号**（`c_snapshot`，在探测前捕获）。
于是原代码里探测步那次 `c.snapshot = probe.snapshot` 造成一个致命的形状：

> **探测成功、载荷失败**（网络抖一下就够）⇒ 号已经前进、而清单没有 ⇒
> 下一轮增量带着**新号**去问，而服务端的判据是
> `valid_from > since || (deleted_at != 0 && deleted_at > since)`
> ⇒ **恰好等于那个号的墓碑与产物都不会再被下发** ⇒ 那批变更**永久丢失**（直到某次全量刷新）。

用户看到的就是 `§174` 观测到的那个形状（两个方向都由"清单与号错半拍"解释）：

| 客户端当时缺的是 | 结果 |
|---|---|
| 产物没到、输入还在（墓碑也没到） | 该分片按输入算 ⇒ 行数对不上（**21**：产物 6 行被算成输入的一半 + 另一份还在） |
| 产物到了、某个输入的墓碑没到 | 产物与输入同时可见 ⇒ **27**（双计） |

**关键认识**：这两条都**不是**"数据不在"或"提交不原子"（那两半 `§174` 已排除），
而是**客户端手里那对 `(号, 清单)` 不自洽**。

### 175.1 做了什么

| # | 改动 | 关键点 |
|---|---|---|
| 1 | **修复**（`crates/meta/src/remote_catalog.rs`） | 删掉探测步的 `c.snapshot = probe.snapshot`。`apply()` 里本来就有 `c.snapshot = resp.snapshot`（**载荷与号一起落地**）⇒ 那行在成功路径上是冗余的、只在失败路径上制造危害。`read_index` 仍留在探测步（它是**活性/诊断**信号，不是可见性边界） |
| 2 | **新用例**（`crates/meta/tests/catalog_version_landing.rs`） | 一个**假 metaserver**：探测答得出、载荷**可以被打断**，并且**逐字复刻**服务端 `files_since` 的判据（这一条是整条链的要害）；`delta` 也实现（增量路径是被测对象的一部分）。三步断言：①载荷失败 ⇒ **号不许前进** ②放开载荷 ⇒ 号到位、产物可见、输入不可见 ③旧快照仍看到合并前的输入（快照隔离的另一半） |

### 175.2 三个决定

1. **口径：宁可号落后，不许号超前**。
   号落后只意味着"这次查询看到的世界稍旧"（与刷新节奏同量级，且客户端自洽）；
   号超前意味着"某批变更永远不会被下发"。⇒ 失败路径上**必须**保持旧号。
2. **号只与载荷一起落地**（把这条写成不变量，而不只是"删一行"）。
   推论：客户端缓存的视图**永远是某个完整物化过的快照** ⇒ 查询拿到的 `(号, 清单)` 永远自洽
   ⇒ `§174` 观测到的 ±3 被**结构性**消除（不是"把窗口调小"）。
   附带好处：两跳落在**不同副本**上时（探测来自 A、载荷来自 B），只有"载荷响应里的号 + 载荷里的文件"
   这一对是自洽的 —— 只认载荷那一对，恰好也把这个情形处理掉了。
3. **用例必须能确定性触发那条缝**（假服务端），而不是"多跑几次 rig 看看"。
   真实 metanode 不会在两跳之间给测试留缝；而"载荷失败"这个注入是**一行**的事。
   ⇒ 反证（把旧行为放回去）当场红，这就是它能防回归的证明。

### 175.3 边界（这一刀**没有**做的）

- **没有**改 `read_index` 的发布时机（见 175.1-1 的区分）。
- **没有**给 `RemoteCatalog` 加"刷新中"的状态位或并发合并（两次并发 `refresh()` 现在各自
  捕获**同一个已物化的号** ⇒ 载荷区间相同、upsert 幂等 ⇒ 不需要互斥）。
- **没有**动 `LocalCatalog` 侧：它按 `catalog.current_snapshot()` 建视图，而那个号现在是"已物化"的 ✓。
- rig 若再现 ±3，判据 ⑥ 会当场报出（`§174`）⇒ 那时说明还有**另一个**窗口（这一刀只负责证明
  "货架上的这一处"已经关掉）。

### 175.4 验证

- 新用例 `a_failed_payload_must_not_advance_the_snapshot` **通过**。
- **反证（判据有牙齿）**：把 `c.snapshot = probe.snapshot;` 放回去 ⇒ 用例立刻红在核心断言上：
  `载荷失败却把号推进了 ⇒ 下一轮增量会漏掉这一批变更（`§175` / 台账 `D-23`）；left: 2, right: 1`；
  撤销后转绿。
- `cargo test -p yuntun-meta`：**41 + 26 条全绿**（含 `remote_catalog_parity` 与两个远端 e2e）——
  说明修复没有破坏正常的版本驱动刷新与对拍。
- 全量 `cargo test --workspace` 与 `cargo clippy --workspace --all-targets`：见提交信息。

---

## 176. **N4a-2：谓词（`WHERE`）下推**（2026-10-11）

### 176.0 缺口

`§173` 把"可分解聚合"下推到数据所在处，但**只覆盖整表**（`SELECT count(*) FROM t`）。
而真实的看板查询几乎都带 `WHERE` ⇒ 那类查询仍然要把**全部热数据**拉回协调者再筛
（`WHERE` 是在协调者**拉到行之后**才生效的）。这一刀把谓词也送过去。

### 176.1 做了什么

| # | 层 | 改动 |
|---|---|---|
| 1 | **判定层**（`model::predicate`，新模块） | `Predicate`（`Cmp` / `IsNull` / `And` / `Or`）+ `Literal`（整数 / 字符串）+ `CmpOp`；`mask()` 求值、`count_matching()`、`filter_batch()`；`agg` 增加 `fold_batch_with` / `fold_batches_with`（**先筛行再折叠**） |
| 2 | **协议**（`shard.proto`） | `PredicateMsg` + `FetchShardAggsRequest.predicate`（缺席 = 不过滤）。形状与 `AggSpecMsg` 同族：**按下标对齐的朴素结构**，不做通用表达式树 |
| 3 | **数据面**（`store` + `shardrpc`） | `read_table_aggs` / `fetch_shard_aggs` 各加一个 `predicate` 参数；服务端与 `WireFetch` 都走**同一条** `fold_batches_with`；`predicate_from_msg` 对不认识的形状**响亮报错**（静默忽略一个谓词 = 把筛过的数据当没筛） |
| 4 | **协调者**（`query::agg_pushdown`） | `Pushdown` 增加 `predicate`；`WHERE` **不再直接拒绝** —— 由 `predicate_of` 判定能否翻译，能就送下去、不能就**整体不下推** |

### 176.2 四个决定

1. **支持面窄到可以逐字复刻**：比较（`= != < <= > >=`）、`IS [NOT] NULL`、`AND` / `OR`、括号、
   **负数字面量**。其余一律拒（`NOT`、`LIKE`、`IN`、`BETWEEN`、函数、算术、浮点、跨类型）。
2. **`NOT` 刻意不做**：它需要对三值逻辑取反（`NOT UNKNOWN = UNKNOWN`），而"放行/不放行"这个二值
   投影下 `AND`/`OR` 可以直接用掩码组合、`NOT` 不能（`NULL` 的掩码位会被翻成"放行"）。
   要收它得先把求值改成真正的三值（`Option<bool>`），那是另一刀的收益/风险比。
3. **翻译不了 ⇒ 整体不下推**（而不是"下推一半"）：半个谓词会让热的一半算进不该算的行 ——
   那是**静默错**；不下推只是"这次不过网省"。
4. **缺失列 = 全 NULL**：读路径按 `arrow_util::align_batch` 对齐（缺失列补 NULL），
   所以谓词与聚合**都必须**这样处理（而不是报错）。顺带修了 `agg` 里原来"聚合列不存在就报错"
   的行为 —— 那是 schema 演进过的表在下推路径上的一个隐患（原路好好儿的，下推却报错）。

### 176.3 边界（这一刀**没有**做的）

- **`GROUP BY` 仍未下推**（每个分组各自的部分状态需要"分组键 + 状态列表"的响应形状）。
- **`NOT` / `LIKE` / `IN` / `BETWEEN` / 函数 / 算术 / 浮点 / 跨类型比较**一律原路（见 176.2-1/2）。
- **谓词不参与分片/文件剪枝**：它是**行级**过滤（在折叠前筛行），不去省 IO；
  文件级剪枝（`prune`）与行组级剪枝（`index`）仍只作用于**协调者自己读的冷文件**。
- 空 `AND` / 空 `OR` 在协议与判定层都不产生（`predicate_from_msg` 拒绝空子项）。

### 176.4 验证

- **对拍（逐位 + schema + "该下推的真的下了推"）**：`aggregate_pushdown` 的用例扩到 **22 条形状**
  —— 14 条可下推（其中 8 条带 `WHERE`：比较、`!=`、`IS NULL` / `IS NOT NULL`、`AND`、`OR`、
  **字面量在左的翻转** `3 <= v`），8 条必须退回（`BETWEEN`、`IN`、`float`、`CAST`、
  `NOT`、`avg`、`GROUP BY`、表达式）。两条路的值与 schema 逐位相同。
- **网络量**（`ROADMAP` N4 的验收）：`--nocapture` 实测两组 ——
  **无谓词：244,104 字节 → 64 字节（3814×）；带 `WHERE`：244,104 字节 → 64 字节（3814×）**。
- **数据面往返**：`aggregate_pull_survives_the_wire` 增加"谓词过网"的一段 ——
  用**具体数字**钉住它真的生效（`a > 1` 在 `[3,1,2]` 上只剩两行：`count=2 / sum=5 / min=2 / max=3`），
  并与进程内同一条路径逐位一致。
- **判定层单测 71 条**（`yuntun-model`）：三值逻辑（`!=` 在 NULL 行上**不放行**）、边界值、
  `AND`/`OR` 的 Kleene 组合、`IS NULL` 与 `IS NOT NULL` **互补**、缺失列 = 全 NULL、类型不符响亮失败。
- **两处"测试自身的缺陷"当场被抓住并修掉**（这是本刀最值得记的部分）：
  1. **对拍抓到真 bug**：`IsNull` 的取反辅助函数写成了"拷贝"（漏了 `!`）⇒ `IS NOT NULL` 退化成
     `IS NULL` ⇒ `sum(v) WHERE v IS NOT NULL` 给出 **30** 而原路 **36**（差 6 = 热侧两个来源的贡献）。
  2. **原来那条 `IS NULL` 单测证明不了任何事**：它只有两行（1 非空 + 1 空），
     于是"取反"与"忘了取反"恰好都数出 1。⇒ 改成三行 + **互补性**断言（两种实现必然分道扬镳）。
  3. 另外 `WHERE v < -1` 一开始被判"翻译不了"：`-1` 在 AST 里是**一元负号 + 字面量**，
     不是纯字面量。补上（内层必须是字面量 ⇒ 与 `v + 1` 那种真算术仍然区分得开）。

---

## 177. **N4a-2 后半：分组（`GROUP BY`）下推**（2026-10-12）

### 177.0 缺口

`§173` 的聚合下推只覆盖"整表一行"的形状，而看板里最常见的正是
`SELECT 维度, count(*) FROM t GROUP BY 维度`（"每台主机多少条"）。`§176` 补了 `WHERE`，
这一刀补 `GROUP BY` —— 让**每组的标量**过网，而不是把行拉回来再分组。

### 177.1 做了什么

| # | 层 | 改动 |
|---|---|---|
| 1 | **`model::agg`** | 新增 `AggQuery`（`specs` + `group_by` + `predicate` 打包成一个结构 —— trait 每加一个参数就要改一遍所有实现，已经改过两轮了）、`KeyPart = Option<AggScalar>`、`GroupStates`、`AggStates`（**和类型**：`Plain` / `Grouped`，而不是两个"只有一个有意义"的可选字段）；`fold_query`（分组时**逐行**累积到该组）、`merge_states`（按**键**并入） |
| 2 | **协议** | `FetchShardAggsRequest.group_by`（tag 6）+ `ScalarMsg` / `GroupStatesMsg` + `FetchShardAggsResponse.groups`（tag 4）。**形状由请求决定**：带 `group_by` ⇒ 走 `groups`，不带 ⇒ 走 `partials`；两侧都校验（形状不符**响亮失败**） |
| 3 | **数据面** | `read_table_aggs` / `fetch_shard_aggs` 改成收 `AggQuery`；`ShardAggs.partials` → `states: AggStates`；服务端与测试里的假远端都走**同一条** `fold_query` |
| 4 | **协调者** | `Pushdown` 加 `group_by`；`detect` 认"分组键是普通列 + 投影是**键在前、聚合在后**"；新增 `merge_grouped_into_cold`（冷结果的每行一组 + 热来源**按键并入** + 拼装时沿用 DataFusion 的 schema） |

### 177.2 四个决定

1. **分组键用 `Option<AggScalar>` 而不是"没有值"**：SQL 的 `GROUP BY` 把 NULL **当作一个组**
   （与 `=` 的语义不同 —— `NULL = NULL` 是 UNKNOWN，但分组时它们就是一组）。
   少这一个分量，症状是"NULL 行被丢掉"或"NULL 行各成一组"，而两种都**不报错**。
2. **结果做成和类型**（`AggStates`）：`partials` 与 `groups`"只有一个有意义"是可以**误用**的契约
   （哪个有意义要靠猜）。用类型表达后，形状不符在**编译期/运行期**都是响亮失败。
3. **合并的责任在协调者，且沿用 DataFusion 的 schema**：冷的一半仍由 DataFusion 算
   （列名/类型/可空性都由它给），我们只把状态写进它给的那些列 —— 与 `§173` 同一条纪律。
4. **输出按**键升序**：`GROUP BY` 的行序本来就不在契约里（原路也没保证），但可复现本身有价值
   （两个来源的同一组必须能合到一起，出问题时也要能对照）。

### 177.3 过程中抓到的两个"自己的坑"

1. **冷结果会被切成多批**：DataFusion 按分区产出批次，**分组时几乎必然是多批**，
   而协调者原来只认"一批" ⇒ 判定通过却悄悄退回原路（**没有任何报错**，只是不优化）。
   症状是"该走聚合下推却没走"。修法：先 `concat_batches` 再判形状。
2. **用例里留着过时的反例**：`§173` 时代把 `SELECT v, count(*) ... GROUP BY v` 列为"必须退回原路"，
   现在它能下推了 ⇒ 那条反例成了"假失败"。删掉并补上**真正**下推不了的形状
   （投影顺序不符、表达式键、`ORDER BY`、`HAVING`）。
   ⇒ 教训：**能力扩张时要回头审一遍"反例清单"**，它与实现一样会过期。

### 177.4 边界（这一刀**没有**做的）

- **只支持整数与字符串分组键**（判定层挡住别的类型）；多列键支持，但必须是普通列。
- **投影必须"键在前、聚合在后"**：`SELECT count(*), v FROM t GROUP BY v` 走原路
  （不是不能算，是不为它单独发明一套列对齐规则）。
- **不支持**表达式键、`ROLLUP`/`CUBE`/`GROUPING SETS`、`HAVING`、`ORDER BY`、`avg`、
  `count(DISTINCT …)` —— 一律原路（正确，只是不过网省）。
- **分组基数不受限**：维度很多时响应会变成"每组一条标量"，仍然远小于行，但没有**上限保护**
  （极端高基数的维度需要再加"只报前 N + 总数"）。

### 177.5 验证

- **对拍**（`query/tests/aggregate_pushdown`）：用例扩到 **31 条形状** —— 19 条可下推
  （含 5 条分组：整数键 / 字符串键 / 多列键 / 分组+谓词 / 键+多聚合），12 条必须退回
  （含 4 条"形状对不上"的分组）。每条都断言 ①值与参照**逐位相同** ②**schema 相同**
  ③**可下推的真的打了聚合拉取、不可下推的一次都没打**。
- **`model` 单测 15/15**（新增 6 条）：NULL 是一个组、按键合并（含 NULL 键）、
  字符串键与多列键、谓词先筛行再分组、空输入的两种形状（单位元 / 没有组）、形状不符响亮失败。
- **判定层单测 9/9**（新增 2 条分组合并：只存在于热数据的组要**新增一行**、
  形状不符响亮失败）。
- **数据面往返 5/5**（新增 `grouped_aggregate_pull_survives_the_wire`）：分组过网 +
  与进程内逐位一致 + **分组与谓词一起**过网（只剩两组）。
- 全量 `cargo test --workspace` 与 clippy：见提交信息。

---

## 178. **N4b 第一刀：冷文件的分配规则 + "只读我这一份"的接缝**（2026-10-12）

### 178.0 先做摸底，摸底推翻了我原设想的一半

动手前先问了四个问题（`code-explorer` 摸底 + 复读关键处），四条结论：

| 问题 | 结论（证据） |
|---|---|
| 冷读的"文件清单"在哪、白名单往哪插 | 唯一入口是 `YuntunTableProvider::scan`；冷集合 = `CachedTable.files` 经 `prune` 后的 `kept`（`table.rs:573-575`）。**白名单只需插一处**（`:576`，剪枝之后）：下面的 DV 匹配、vortex/parquet 分流、索引计划**全部**从 `kept` 派生 |
| 数据面能不能挂 DataFusion | `serve(chunks, listener)` 在 `datanode/src/main.rs:555` spawn；`store`/`catalog` 手上有，但 `QueryEngine`/`LocalCatalog` 在 `main.rs:594/610` 才建**且只在 `--sql-listen` 分支** ⇒ 要挪装配顺序或用 `OnceLock` |
| 远端怎么保证"在协调者那个快照上读" | `RemoteCatalog` **没有** `wait_for(version ≥ N)`：`current_snapshot()` 只是 best-effort 刷新后的缓存值（`remote_catalog.rs:1173`）⇒ 远端查自己的目录**可能读到不同快照的文件集/DV 集** ⇒ **静默错**。⇒ 请求必须**自足**（文件清单 + 每文件的 DV 路径 + schema 版本），协调者的快照是唯一权威 |
| 今天能拿到什么负载信号 | **几乎没有**：datanode 侧没有 `/metrics`，在途查询数也没上报 ⇒ 第一版只能**静态分配** |

**一条推翻原设想的发现**（最重要）：冷存储是**共享**的 ⇒ "节点替协调者扫冷文件、回传原始行"
**省不了字节**（同一份 S3 字节，只是换了个拉取者），还多一跳网络；parquet 那条路 DataFusion
本来就在协调者本地并行扫。这一刀真正值钱的只有**解码 CPU 的摊开**（尤其 vortex：整文件解码、
没有 row-group 跳读，`D-20`）。
⇒ 想**省字节**，方向是"读到它的节点**就地折叠**、回传标量" —— 那正是 `§173`/`§176`/`§177`
那套契约。两个目标（摊 CPU + 省字节）因此是**同一个机制**：下一刀把"远端扫描"改成"**远端折叠**"。

### 178.1 这一刀做了什么（**不改执行路径**，只落可测的两块）

| # | 改动 | 关键点 |
|---|---|---|
| 1 | **`query::placement::assign`**（新模块，纯函数） | 输入 `(files, nodes)` ⇒ 按**实例 id 升序**的 `Assignment{instance, files, bytes}`。确定性（节点排序去重 + 文件按 `(大小降序, 路径升序)`）+ **LPT 均衡**（每次把最大的给当前累计最小的）+ **完整**（每个文件恰好一次）。7 条单测 |
| 2 | **`ReadPath::only_files` / `QueryEngine::with_only_files`** | 接缝：provider 在 `prune` 之后按 `file_path` 过滤（`table.rs:576`）。与 `index_pruning` / `agg_pushdown` 同性质 —— **不是运维旋钮**，是对拍与下一刀的载体；不放进 TOML |
| 3 | **判据**（`tests/cold_file_placement.rs`） | 三条：①只给一个文件 ⇒ 只看到那几行；②一个都不给 ⇒ `count(*)`=0 / `sum`=NULL（不报错、也不退化成全表）；③**按分配切成两半分别查，加起来 == 全量查** |

第 ③ 条是这一刀的核心：**漏一个文件 = 少数据、重一个文件 = 双计**，两种都**不报错**；
它把 `assign` 的"完整"从纯函数性质推到了**数据**层面。

### 178.2 四个决定

1. **先落"分配"，不动执行**：远端冷读要跨 4 个 crate（协议 + 装配 + 协调者 + 判据），
   而"分给谁"这个决定**今天就可以测**。先把它变成纯函数，下一刀就只剩"怎么搬"。
2. **确定性优先于最优**：节点排序去重、文件按 `(大小降序, 路径升序)`、并列时取实例 id 小的。
   分配不可复现 ⇒ 问题**无法复现**，那比慢更糟。
3. **LPT 而不是"按个数轮流"**：文件大小差异极大（合并产物 vs 刚 flush 的批）。
4. **本地也参与分配**（调用方把自己放进 `nodes`；传空列表 = 全留给本地）：协调者也是一个
   能读冷文件的节点，分它一份才不必多跳一次。
   ⚠️ 这个约定我第一版写漏了（`assign` 不会自动加本地）⇒ 用例报出"只分出 1 份"才补上。

### 178.3 边界（**没有**做的）

- **执行路径没变**：仍然是协调者读全部冷文件（这一刀不交付性能）。
- **没有失败重分配**：下一刀谁读不到就**回退本地读**（不是降级成 partial）。
- **没有亲和性**（让写过这批文件的节点读它们）：需要确认 `source_instance` 的语义
  —— 它今天是**热副本**归属，**不是**文件 owner。
- **没有动态负载均衡**：先要有信号（datanode 侧的 `/metrics` 与在途计数），那是另一刀。
- **没有远端冷读本身**（下一刀）：`serve` 装配 + `FetchShardAggs` 带 `cold_files[]` +
  降级/STALE 三态。

### 178.4 验证

- `placement` 单测 **7/7**：完整且确定、LPT 的分配结果、`nodes` 空 ⇒ 全本地、单节点、
  无文件时每个节点一条空分配、**节点顺序与重复不影响结果**、大小相同时由路径打破并列。
- 接缝 e2e **3/3**（含上面第 ③ 条"加起来 == 全量"）。
- **两处算术错误是判据替我抓到的**（都写进了用例注释）：
  ① LPT 的期望我算成"1 给 dn2"，实际最小的是 dn3（`3+1`）—— 算法对、我的期望错；
  ② 全量求和我写成 283，实际 `6+10+50+220 = 286`。
  ⇒ 这与 `§171` 的教训同族：**判据要能替我算第二遍**。
- 全量 `cargo test --workspace` 与 clippy：见提交信息。

---

## 179. **N4b 第二刀：冷文件的就地折叠**（`§178` 的"远端冷读"那一半的第一层）（2026-10-12）

### 179.0 这一层解决什么

`§178` 落了"分配"与"只读我这一份"的接缝，但**执行仍在协调者本地**。这一层落的是
**"就地折叠"** —— 被分配到的那一份，在读它的人那儿就折成标量：

> **"给你这几个文件（和它们身上生效的 DV），按这个查询把它们折成部分状态"**

它存在的理由是 `§178` 摸底得到的那个结论：冷存储**共享** ⇒ 远端扫描后回传**原始行**省不了字节
（同一份 S3 字节，只是换了个拉取者）；要省字节就得**就地折叠、回传标量**。

### 179.1 三条纪律（决定了它长这样）

| 纪律 | 为什么 |
|---|---|
| **请求自足**：要读哪些文件、每份文件上生效哪几份 DV，全部由调用方给进来 | 被分配到的节点**不许**查自己的目录：`RemoteCatalog` 没有 `wait_for(version ≥ N)`，它可能读到**另一个快照**的文件集/DV 集 ⇒ **静默错**（漏一份 DV = 复活已删的行）。⇒ 新增 `CatalogSnapshot::from_parts`（一次性快照，不进缓存、不参与刷新） |
| **读路径与协调者同一套代码**：走 `YuntunTableProvider` | 不另写一个"简化版解码器" —— 两套实现会在某个边界上漂，而 DV 那一侧的漂移是"已删行复活" |
| **折叠与热数据同一套代码**：`agg::fold_query` | 聚合/谓词/分组的语义与 `§173`/`§176`/`§177` **逐字相同** |

### 179.2 边界（这一刀**没有**做的）

- **不剪枝、不索引**：这里读的是"分配给我这几个文件"的**全部行**（`index_pruning = false`）。
  文件级/行组级剪枝要过网才能剪，那是下一刀。
- **不读热数据**：热数据**归属绑定**，不参与分配（`hot_enabled = false`）。
- **DV 取不到 ⇒ 响亮失败**：与读路径同一条纪律（`file_deletions` 失败一律报错，绝不降级）。
- **还没过网**：这一层只是"就地折叠"的函数；把它变成 RPC、接到 `serve` 上是下一刀
  （`serve` 今天拿不到 `QueryEngine`，装配要挪）。

### 179.3 判据（`crates/query/tests/cold_fold.rs`，6 条）

1. **对拍（不分组）**：冷折叠的结果 == 协调者自己查出来的数（同一套读路径 ⇒ 天然成立，
   但还是要钉住 —— 它是对"以后谁改了读路径"的保险）。
2. **DV 生效**（删前/删后各一遍）：6 行/66 → 删掉一行后 **5 行/64**，并与协调者的查询一致。
   数字不变就说明 DV 被忽略了 —— 而那**不报错**。
3. **分组对拍**：`GROUP BY v` 逐键逐值（含 `min`）与协调者一致。
4. **谓词 + 分组一起**：`v > 5 GROUP BY v` ⇒ 只有 10/20/30 三组，与协调者一致。
5. **分配 + 就地折叠合起来 == 整份折叠**（`§178` 的判据在这条路上再走一遍：
   分成两份分别折、再 `merge_states` ⇒ 与整份逐位相同）。
6. **读不到 DV ⇒ 响亮失败**（删掉位图对象后必须报错）。

### 179.4 过程中的两处修正

1. `FileManifest.file_size` 一开始硬编码成 `1` ⇒ DataFusion 按它从对象存储取字节，
   报的是"parquet 至少 8 字节"——**看起来像文件坏了**，实际是清单填错了。
   ⇒ 改成 `write_batch` 返回的真实字节数，并把"这必须是真实字节数"写进注释。
2. `dv_object_path` 在 `model::dv` 而不在 `yuntun_format`；删对象是 `yuntun_store::delete`
   （不是 `delete_bytes`）—— 都是编译期的事，但记下来免得下次再找。

### 179.5 验证

- `crates/query/tests/cold_fold.rs` **6/6**（含上面第 2、5、6 条）；
- `cargo test -p yuntun-query` 全绿（46/4/9/3/6/7/2/4 段）；
- clippy 与文档判据：见提交信息。

---

## 180. **N4b 第三刀：冷折叠过网**（协议 + 数据面两侧 + 往返判据）（2026-10-12）

`§179` 落了"就地折叠"的**函数**；这一刀把它变成**能过网**的能力，并在两端都接上。

### 180.1 协议：`FetchColdAggs`（自足请求）

请求带 `table`（只作标识）+ **`schema_ipc`** + `files[]{file_path,file_size,row_count}`
+ `dvs[]{dv_id,file_path,batch_id,applied_at,revoked_at,card,store_path}` + `aggs[]` +
`predicate?` + `group_by[]`；响应复用 `FetchShardAggsResponse`（`partials` 或 `groups`，
**由请求带不带 `group_by` 决定**）。

**为什么单独一条 RPC 而不是塞进 `FetchShardAggs`**：那条要的是"这个来源的**热数据**"
（归属绑定，只带栅栏），这条要的是"这几个**冷文件**"（共享、可分配，要带文件清单 + DV + 表结构）
—— 两种归属规则不同；混在一起就会出现"两个字段只有一个有意义"。

**为什么请求要自足**：被分配到的节点**不许**查自己的目录 —— `RemoteCatalog` 没有
`wait_for(version ≥ N)`，它可能读到**另一个快照**的文件集/DV 集 ⇒ 静默错（漏一份 DV =
复活已删的行）。表结构也一样：每个节点自己 catalog 里的 schema 可能是另一个版本的。

### 180.2 两端接线

- **服务端**：`ShardService` 多持一个 `Option<Arc<dyn ColdReader>>`（`with_cold`）；
  **没接 = 响亮失败**（`unimplemented`）⇒ 协调者按"拿不到"回退，而不是以为它是空结果。
  `serve` 多一个参数；另拆出 `serve_service`（给"已装配好的服务"用）。
- **客户端**：`RemoteCold`（`ColdReader` 的远端实现）—— 与 `RemoteShard`（热数据）**分开**：
  归属规则不同，客户端也不该混。
- **生产实现**（`datanode::query::NodeCold`）：只需要 `store`，**不碰本节点的 catalog**
  （请求自足 ⇒ 不需要，也不该用）。装配点在 `main.rs` 的 shard 服务 spawn 处。
- 顺手把一个重复的"状态 → 消息"提成 `states_to_msg`（两个 handler 共用，不再写两遍）。

### 180.3 判据（`crates/shardrpc/tests/cold_fold_rpc.rs`，3 条）

1. **往返 == 进程内**：过网拿回的部分状态与本地同一条路径折出来的**逐位相同**；
   且 **DV 生效**（6 行/66 → 删一行后 5 行/64，两边一致）；
2. **分组也过网**：谓词 + `group_by` 一起 ⇒ 只剩 `10/20/30` 三组、每组 1 行；
3. **没接冷折叠的节点 ⇒ 响亮失败**（错误里能看出是"没接"，不是"读失败"）。

### 180.4 过程中的三处修正

1. **磁盘满导致链接失败**（`linking with cc failed`）—— 一度以为是代码问题。
   根因：`/dev/sda3` 只剩 2.8G（rig 的容器卷 + 10G 增量编译缓存）。清掉后 66G 可用。
   ⇒ 教训：**"构建失败"先看空间**，别急着改代码。
2. `Arc<ChunkStore>` 擦成 `Arc<dyn ShardReader>`：写在 `let` 注解里不灵，写在**函数调用点**才灵
   （推断顺序不同）⇒ 测试里改成直接调 `serve(chunk, listener, Some(cold))`。
3. arrow-ipc 59 的 schema 编码入口是 `schema_to_bytes_with_dictionary_tracker`（带字典追踪器），
   `DictionaryTracker::new(false)` 要一个参数；`fb_to_schema` 不会失败。

### 180.5 验证

`cold_fold_rpc` **3/3**；`yuntun-shardrpc` / `yuntun-query` / `yuntun-format` / `yuntun-store`
全绿；全量 `cargo test --workspace` exit=0（文档判据同步后）；clippy **0 告警**。

**仍然没做**：**协调者还没用它**（没有分配下发、没有失败回退本地读）—— 那是下一刀，
也是这一整条路"用户能看到收益"的那一刀。

---

## 181. **N4b 第四刀：协调者接线的两个前置**（互逆的"状态↔批次" + 每查询白名单）（2026-10-12）

`§180` 之后，"就地折叠"已经能过网了，但**协调者还没用它**。真正接线之前有两块必须先是
"对的形状"，否则接线那天会把两件事搅在一起改：

### 181.1 把"合并"重建在一对**互逆**函数上

- `states_of_cold_result(冷批次, specs) → AggStates`（DataFusion 算出的那一行/每一行**读回**状态）
- `states_to_batch(schema, specs, states) → 批次`（状态**拼回**批次；schema/列名由调用方给）

`merge_into_cold` / `merge_grouped_into_cold` 现在都是 `读回 → 合并 → 拼回` 三步
（分组那条原先自己抄了一份拼装代码，现在也走同一条）。

**为什么需要这个方向**：冷的一半可能被**分配**给多个节点分别算，而"哪一半在本地算、
哪一半送出去"是**每次查询**的决定 —— 但最终输出必须仍是**一个**带 DataFusion
schema/列名的批次。于是流程统一成：`本地那一份 → 状态 → 与各远端状态合并 → 拼回批次`。

### 181.2 白名单改成**每查询**的参数

`session_cold_files(schema, partial, files)`：只读冷数据、且只读这几个文件。
**不**用引擎上那个 `with_only_files`（它留给对拍/测试）：分配是"这次查询"的决定
（文件集随快照变、节点随名录变），挂在引擎上就是"一次查询改一个共享对象的状态"。

### 181.3 判据与验证

- 这是**行为不变**的重构：`yuntun-query` 全绿（46/4/9/3/6/7/2/4 段，含 §176/§177 的 31 条
  对拍形状与 §178/§179 的冷折叠判据）⇒ 重构没改语义。
- clippy：本仓 **0 告警**（修了一条 `loop variable used to index`）。
- 顺手删了因此变成死代码的 `cold_value_of`（都走带行号的 `cold_value_at`）。

**仍然没做**：分配下发与失败回退（下一刀 —— 也是这条路上"用户能看到收益"的那一刀）。

---

## 182. **N4b 第五刀：协调者接线 —— 冷文件真的被分出去了**（2026-10-13）

`§178`（分配规则）`§179`（就地折叠）`§180`（过网）之后，**协调者终于用上它了** —— 这是这条路上
"用户能看到收益"的那一刀。

### 182.1 做了什么

| # | 改动 | 关键点 |
|---|---|---|
| 1 | **接缝**：`ShardReader::as_cold()`（默认 `None`）+ `ShardFetch::as_cold()`（传输层，`GrpcShardFetch` 复用同一条连接给出 `RemoteCold`）+ `RemoteShard` 透出 | **不设新表、不管新生命周期**：冷文件在共享存储上，谁有传输接缝谁就能算 ⇒ 与热读器天然是同一批实例，成员摘除时一起消失（`set_members` 的裁剪自动覆盖） |
| 2 | **分配**：`aggregate_pushdown` 里按 `placement::assign(可见文件, [各热实例 + 本地])` 切分 | 引擎级白名单（§178 的接缝）**先**过滤，分配作用在它之后 ⇒ 不会"引擎说只读 A、分配却把 B 分进来" |
| 3 | **本地一份**：`session_cold_files`（§181 的每查询白名单）走 DataFusion ⇒ **schema/列名/类型仍由它给** | 那是"与对拍逐位相同"的保证 |
| 4 | **远端若干份**：并发下发 `ColdReadRequest`（文件 + 该文件的 DV + 表结构 + 聚合/谓词/分组），只回**标量** | 请求自足（§180）⇒ 远端不看自己的目录 |
| 5 | **合并**：`states_of_cold_result` → `merge_states`（远端 + 热）→ `states_to_batch`（§181 那对互逆函数） | 分组与不分组同构 |
| 6 | **回退**：远端拿不到 ⇒ 把它那一份加进本地清单**重算**，**不是降级成 partial** | 那份数据本来就在共享存储上、协调者自己能读 |

### 182.2 三个决定

1. **能力挂在 reader 上，而不是另设一张表**：归属性质不同（热 = 归属绑定，冷 = 谁都能读），
   但**生命周期相同** —— 复用热读器的登记与裁剪最省，也最不容易漂移。
2. **接不上 = 回退，不是 partial**：partial 的语义是"结果可能少"，而这里只是"换个人读"。
   把它标成 partial 会让运维去追一个并不存在的丢数据。
3. **没有任何节点接得上 ⇒ 全本地算**（单机 / 老二进制 / 未接线）：与改造前**逐字相同**，
   少一跳网络也少一处风险。

### 182.3 判据（`crates/query/tests/aggregate_pushdown.rs` +3 条）

1. **分配 + 远端折叠 == 原路**（不分组 / 带 `WHERE` / `GROUP BY` 三条形状各跑一遍），
   并断言**远端真的被调用过**（否则这条绿只是"本地算了两遍" —— §173 的同一条纪律）；
2. **接不上的节点 ⇒ 回退**：答案不变 **且不留 partial 标记**；
3. **没接冷折叠的节点**（`as_cold() == None`）⇒ 答案不变（盯"混合集群"）。

### 182.4 过程中修正的三处

1. **分层错了**：先加在了传输层（`ShardFetch`），而协调者手上是 `ShardReader`
   ⇒ 编译报出来，改成 `ShardReader::as_cold()` 默认 `None` + `RemoteShard` 透出传输层能力。
2. **引擎级白名单被绕过**：分配用的是快照全量，于是 `with_only_files` 在下推路径上失效
   （§178 的三条用例当场红）⇒ 分配改作用在**白名单过滤之后**的集合上。
3. **判据自己写错了两次**：参照引擎少了同样的热数据（差的那一行正是热行）
   ⇒ 两侧必须**热数据相同、只有冷折叠能力不同**，否则分不清是谁错。

### 182.5 边界（**没有**做的）

- **不剪枝、不索引**：远端折的是"分配给我的这几个文件"的**全部行**（§179 的边界）
  —— 所以这一刀省的是**解码 CPU 与协调者带宽**，不是远端 IO。
- **没有动态负载均衡**：信号为零（§178.3），分配仍是静态确定性规则。
- **没有亲和性**：谁写过这批文件并不参与分配（`source_instance` 是**热副本**归属，不是文件 owner）。

---

## 183. **N4b 第六刀：远端冷折叠也要剪枝 —— 修掉分配引入的回退**（2026-10-15）

### 183.0 这不是"再优化一点"，而是 §182 引入的回退

`§182` 把一部分冷读分给了别的节点。但 `cold::fold_cold_files` 是 `ctx.read_table(provider)`
—— **不带过滤** ⇒ 既不按 `FileManifest.stats` 做**文件级**剪枝，也不把条件交给 `ParquetSource`
做**行组级**跳读。于是同一条查询里：**留在本地的那半边会剪、分出去的那半边不剪**。

注意方向：这不是"少了个优化"，是**分配让人多读了数据**。而它**不报错**，只表现为
"远端 CPU/IO 变高" ⇒ 属于最难被发现的那一类。

### 183.1 改动

`cold::fold_cold_files` 把 `query.predicate` 翻成 DataFusion 表达式（`predicate_to_expr`），
再交给读的那一边（`df.filter(expr)` ⇒ provider 的 `scan` 拿到 filters ⇒ 文件级 + 行组级剪枝
都生效）。**不需要改协议**：谓词本来就在请求里（§176）。

翻不出来 ⇒ `None`（不剪、只折）：宁可多读，也不许剪错 —— 剪错是**静默少数据**。

### 183.2 判据的设计：`crates/query/tests/cold_fold_pruning.rs`（3 条）

不包装 store 数 `get` 调用，而是**让被剪掉的文件根本不存在**：
- ① 剪了 ⇒ 它永远不会被打开 ⇒ 折叠**成功**（A 值域 [1,2]、谓词 `v > 50`、A 从未写盘）；
- ② **反证**：同一个调用去掉谓词 ⇒ 必须**失败**，且错误信息**点到 A**（证明它真会被打开，
  否则①的"成功"可能只是碰巧没碰到 A）；
- ③ **不许剪错**：谓词放行 A 时（A 存在）⇒ A 必须被算进来（4 行 / 303）。

### 183.3 边界

- 只翻**可下推的那一族**（比较 / `IS [NOT] NULL` / `AND` / `OR`）⇒ 与 §176 同一条纪律。
- **vortex 没有行组剪枝**（`D-20`）：这一刀对 vortex 只带来文件级剪枝。
- 谓词在远端被**用了两次**（剪枝一次、折叠一次）—— 幂等且无害；不值得为此再拆一个标志位。

### 183.4 验证

`cold_fold_pruning` 3/3；`yuntun-query` / `yuntun-shardrpc` / `yuntun-model` 全绿
（含 §176/§177 的 31 条对拍与 §182 的 3 条分配判据）；全量与 clippy 见提交信息。

---

## 184. **S 组：Standalone 完备性收口 —— 单机即一个完整的简易列数据库**（2026-10-16）

### 184.0 缺口是怎么定出来的：先真跑一遍，再对着文档逐条核

`§183` 收在分布式查询上，而"单机交付"这一格从 `§90` 之后就没有被整体验过。
本轮的做法是**先跑后改**：起真二进制（`yuntun --config`），三条轨各打一遍 ——
MySQL wire（pymysql）、Flight SQL（ADBC）、CLI（Flight 简易轨）—— 做了一张
**60+ 条 SQL 面的行为普查**，再把普查结果与 README §2.1 的声明逐条对。

结论分两半：**宣称的能力基本都真**（CRUD/DDL/DML/多 schema/预编译/JSON 函数/
压缩/恢复/重启持久化全部实测通过）；但**有七处"文档说有、实际没有/不一样"或
"能跑但关不干净"**——全部登记进 `ROADMAP.md §4.0`（`D-32`…`D-37`）并本轮闭环。

### 184.1 七处缺口与修法（每条都有反证用例）

| # | 缺口 | 修法 | 判据 |
|---|---|---|---|
| `D-37` | **raft 选举竞态型 flaky**：`wait_some_leader` 抓瞬时 leader 后接 `wait_leader_agreement`，两次调用间二次选举 ⇒ 等一个已不是 leader 的 id 达成一致，30s 后红（全量首跑实测抓到一次：抓到的 `leader=1`，实际已换 `term 2 / leader 2`；第二轮全绿 ⇒ 竞态非回归） | 三处初始选主统一改 `wait_settled_leader`（**等全体一致**，不是"抓一个再等它一致"）——该文件 §104 的愈合用例早就用的是它，初始选主漏了 | `multi_node_grpc_e2e` 连跑 5 轮 5/5 |
| `D-32` | **`SHOW TABLES FROM <db>` 忽略限定符**：答的是**当前会话库**的表（连 public 时 `FROM sales` 也列 public 的表）。不报错、只答非所问 ⇒ 最难发现的那类 | `shim::schema_from_options`（取 `show_in.parent_name` 最后一段；无限定回落会话 schema）；两条轨都修（shim 单列 `Tables_in_<限定库>` + dispatch 三列 `table_schema`）。顺带把 `SHOW FULL TABLES` 补成 MySQL 两列（加 `Table_type`） | `sql/tests/show_tables_from.rs` 9 条断言（两库交叉 + 列名 + 不存在库空结果）；反证：注掉限定符提取 ⇒ 红（答出 public 的表） |
| `D-33` | **无符号整数全被拒**而 README 写着"含无符号"：sqlparser 0.62 把 `INT UNSIGNED` 解析成**独立变体**（`IntUnsigned`），映射表没接 ⇒ 建表即 1105 | 补齐 `*Unsigned` 家族（→ `UInt8/16/32/64`）。两个口径决定：`MEDIUMINT UNSIGNED → UInt32`（值域 1677 万超 `UInt16`，落小类型是**静默截断**）；浮点 `*Unsigned → Float64`（无符号浮点只是浮点） | `create_table_accepts_unsigned_integers` 9 种写法逐一对拍 |
| `D-35` | **`TRUNCATE TABLE` 不支持**（报 1149）：清表只能 `DELETE FROM t`（无 WHERE 走文件级下线）或 DROP+CREATE，而 TRUNCATE 是客户端/脚本最常用的清空形态 | 复用**同一条数据路径**：抽出 `dml_prologue`（check_write → force_flush → 分片租约）供 DELETE/TRUNCATE 共用，TRUNCATE 直落 `execute_purge`（文件级下线）。**不新造路径**——新造的那条最可能漏的就是 force_flush（"删完在途数据又回来"）。多表/分区/CASCADE/IDENTITY 选项明确拒绝（无分区表与级联概念，静默忽略比拒绝危险） | `server/tests/sql_truncate_e2e.rs` 2 条：语义+保留表定义+形态拒绝+NotFound；**在途数据不复活**（长攒批窗口下 TRUNCATE 后等 4s 不复活） |
| `D-36` | **standalone 只处理 SIGINT**：systemd / `docker stop` 发 SIGTERM ⇒ 进程**直接死**（WAL 不封口、后台任务不 join、WAL/spill/meta 目录租约不释放 ⇒ 下次启动等锁） | 信号处理改 SIGINT+SIGTERM 双路（`tokio::select!`），收尾流程不变 | `standalone_e2e.rs` 新增：SIGTERM 后 30s 内退出且**退 0**，且**同目录能再起实例**（三处租约都放了——`D-11` 的教训） |
| `D-34` | **README 的 JSON 路径示例是错的**：写着 `json_get_int(doc, 'a.b')`，而 `datafusion-functions-json` 的路径是**分步参数**（`json_get_int(doc, 'a', 'b')`）⇒ 照文档写嵌套取值恒 NULL（不报错 ⇒ 静默错答案） | 改文档（README §2.1 + 能力表），并补用例把两个事实都钉住：分步参数取得到；点号串是**单个键** `"a.b"`（miss ⇒ NULL，是**现状语义**不是缺陷——防下一位再去"修"一个没错的实现） | `json_nested_path_is_step_arguments_not_dot_string` |
| — | **`T[]` 数组语法只在高炉 Generic 轨可解析**（MySQL 轨的 MySqlDialect 不开 `supports_array_typedef_with_brackets`） | **决定不改代码**：MySQL 协议本身没有数组类型，`INT[]` 是 DuckDB/PG 写法；为它写 28 个方法的方言委派包装是纯维护负担。改文档口径：`ARRAY<T>` 两轨通用，`T[]` 仅 Flight/Generic 轨 | README §2.1 类型行写明；`ARRAY<INT>` 两轨均有既有用例 |

### 184.2 一键验收：`scripts/standalone_smoke.py`（S7）

把本轮的全部发现固化成**一条命令的冒烟**：MySQL wire 12 项
（CRUD / 聚合 / 元数据 / DELETE+UPDATE / TRUNCATE / 加列删列 / 无符号类型 /
多 schema / prepared / 错误码 1146 / 时间类型 / JSON 函数）+ Flight SQL 2 项
（查询 + `information_schema`），退出码即结论。

三条工程纪律写进脚本（都是本轮踩出来的）：
1. **读己之写用轮询不赌时序**：可见性上界 = 1 个攒批 scan 周期（文档化行为），
   写入后立刻查可能差最后一批；
2. **表名/库名带运行唯一后缀**：上一轮失败的运行会留下"非空 smoke_db"，
   把下一轮的 `DROP DATABASE IF EXISTS` 打成 1008（本项目设计：非空库不删）——
   唯一后缀让重复跑幂等；
3. **pymysql 的 `with conn` 退出即 close**：再调 `conn.close()` 报 "Already closed"
   （本轮第一个冒烟版本就是这么全红的——**脚本自己的 bug**，不是服务的）。

### 184.3 边界（**没有**做的）

- **没有动任何数据路径**：TRUNCATE 复用 purge、SHOW 修的是元数据分流、
  类型修的是映射表——写入/查询/压缩/恢复一行未改；
- **没有造方言委派包装**（`T[]`，见 184.1 最后一行的决定）；
- **`DROP DATABASE IF EXISTS` 对非空库仍报 1008**：这是 README 载明的设计
  （MySQL 实际是连表一起删）——口径未动，只把冒烟脚本改成先删表再删库；
- **raft 选举竞态只修了测试**（`D-37`）：产线多节点 raft 的选主行为未动
  （那部分有 `§104`/`§111` 的既有结论）。

### 184.4 验证

- `scripts/standalone_smoke.py`：**14/14**（连跑两轮，唯一后缀生效）；
- `multi_node_grpc_e2e` 连跑 **5 轮** 5/5（`D-37` 修复前首跑即红）；
- `yuntun-sql` 37 单测 + 1 集成；`yuntun-query` JSON 2/2；`yuntun-standalone` 5/5
  （含新 SIGTERM 判据）；全量与 clippy 见提交信息。

---

## 185. **SQL 面再补两刀：EXPLAIN / CREATE TABLE ... LIKE + 清理两处文档漂移**（2026-10-16）

### 185.0 为什么是这两个

`§184` 的 60+ 条普查把"明确拒绝"的那一族也列了出来（`EXPLAIN` / `TRUNCATE` /
`CREATE INDEX` / `RENAME` / `CTAS`）。`TRUNCATE` 已在 `§184` 闭环；剩下两条
**拒绝得没道理**的：

- `EXPLAIN <查询>`：一个连执行计划都看不到的数据库，没法回答"这条查询为什么慢"。
  而 DataFusion **原生**渲染计划（`plan_type` / `plan` 两列）—— 拒绝它等于把
  已经有的能力挡在门外；
- `CREATE TABLE new LIKE old`：MySQL 客户端/脚本最常见的建表形态之一，
  此前落在 `parse_create_table` 的 "requires column definitions" 上（1149）。

`CREATE INDEX` / `RENAME` / `CTAS` 继续明确拒绝（前者没有索引 DDL 概念——行组级
索引是自动的；后两者缺 model 层载体，README 已载明）。

### 185.1 EXPLAIN：转发 + 形态收窄

`Statement::Explain` 进 dispatch：内层是**查询**且无 `FORMAT`/`OPTIONS` 后缀 ⇒
`stmt.to_string()` 原样转给 DataFusion（`sql_with_partial`，与普通 SELECT 同一条路）。

收窄是刻意的（`§184` 的同一条纪律：不静默忽略选项）：
- 内层是 DML/DDL ⇒ 明确拒绝（本仓 DML 走自己的 ingest 管线，没有 DataFusion 计划）；
- `FORMAT JSON` 之类 ⇒ 明确拒绝（DataFusion 的 EXPLAIN 不认，静默忽略会让用户
  以为拿到了他要的格式；MySQL 方言下这类后缀在 sqlparser 层就报 parse error）。

`EXPLAIN ANALYZE` 可用（DataFusion 原生语义：真跑 + 带 metrics 的计划）。

### 185.2 CREATE TABLE ... LIKE：拷 schema、不拷数据

MySQL 语义三件事：列名/类型/**可空性**逐字段拷贝；新表**空**；源表不动。
实现落在 dispatch 的 `CreateTable` 臂：`like` 为 `Plain` 形态时，从 catalog 取源表
`arrow_schema` 直接构造 `ParsedCreateTable`，其余路径（`create_table` + DDL WAL +
归属 schema 解析）与常规建表**完全同一份**。源表不存在 ⇒ `NotFound`（不静默建空表）。

可空性必须带过来：漏了它，`NOT NULL` 的写入侧校验在新表上静默失效。

### 185.3 两处文档漂移（`D-38`，同类第四次）

1. README §4 写着 `chaos::crash_recovery_no_data_loss` "偶发失败……固定等待后续改
   轮询" —— `wait_hot_drained` 轮询早在 `§171` 同轮落地，且本轮两次全量
   （112 段）全绿 ⇒ **过期声明，删**（chaos 稳定性现状并进 OVERVIEW §6.1 的口径）；
2. OVERVIEW §6.1 写 chaos 恢复上界"60s"，代码是 **120s**（`wait_hot_drained`
   注释写明：满负载并行时 flush 撞对象存储 IO 竞争 + 退避最多 30s/次，60s 不够）
   ⇒ 改 120s 并补理由。

教训仍旧（`D-15`/`D-17`/`D-19`/`D-24` 之后第四次）：**"已知限制"区是最容易过期
的地方** —— 每条要么指向一个活着的判据，要么删。

### 185.4 判据与验证

- `sql/tests/explain.rs` 2 条：计划两行 + 聚合/扫描出现在计划里（证转发了**这条**
  查询而非 canned 答案）+ ANALYZE 可跑；DML/DDL 明确拒绝且理由点名 EXPLAIN；
- `server/tests/sql_create_like_e2e.rs` 1 条：schema 逐字段相同（含 NOT NULL）/
  新表空 / 源表不动 / 新表可写 / 源表不存在 NotFound；
- 全量与 clippy 见提交信息；`standalone_smoke` 14/14 不变。

---

## 186. **Binary 数据的三处断链 + 表达式面第二轮普查**（2026-10-16）

### 186.0 怎么发现的

`§184`/`§185` 之后 SQL 面已经补齐到"拒绝得有道理"。本轮做**第二轮普查** ——
专打**表达式 / 函数 / 查询形态**（子查询、JOIN、窗口、ROLLUP/CUBE、数学/字符串/
日期函数、类型构造，共 60+ 条）。结论：**DataFusion 55 的能力面基本全敞**——
八种 JOIN、标量/IN/EXISTS 子查询、派生表、lag/lead/rank/ntile、ROLLUP/CUBE、
array_agg/string_agg/median/percentile_cont、日期与数学函数全部可用。

但普查抓到一个**硬 bug**：`SELECT sha256('x')` 之后**连接直接死**。

### 186.1 根因：opensrv 写死 charset，Binary 值过不了文本协议

opensrv-mysql 0.7.0 的 `write_column_definitions_41` 把结果集**每一列**的
charset 硬编码成 `UTF8_GENERAL_CI`（上游没给 Column 留 charset 字段）。
于是客户端按 UTF-8 解 BLOB 的原始字节：

- pymysql：`UnicodeDecodeError`（`'utf-8' codec can't decode byte 0xb7`），
  **且错乱后续协议帧**（下一次命令 "Packet sequence number wrong"）；
- 任何 Binary 结果列（sha256/sha224/sha512 家族）或 Binary 表列都会踩到。

连带发现的第二个 bug：`x'DEADBEEF'` hex 字面量被当**文本**存
（`length(b) = 8` 而不是 4 —— 存进去的是 8 个 ASCII 字符，**静默改数据**）。

### 186.2 修法（两处，都不动数据路径）

1. **Binary 结果列按 HEX 文本回传**（`sqlwire/encode.rs: hex_upper`）：
   MySQL `HEX()` 的输出约定，信息不丢（可 UNHEX 还原）、任何客户端都解得动。
   Flight 轨是 Arrow 原生二进制，不受影响。
2. **hex 字面量落原始字节**（`sql.rs: Literal::Bytes` + `decode_hex_literal`）：
   奇数长 / 非 hex 字符明确拒绝（MySQL 同款）；二进制字面量进文本列按 UTF-8
   解释（非 UTF-8 明确拒绝，不静默替换）。

### 186.3 已知限制（写进 README，不硬修）

- `length()` / `octet_length()` **只收字符串**（DataFusion 55 的函数签名）⇒
  二进制列字节长度走 `length(encode(b,'hex'))/2`（`encode` 是 DF 的 hex 编码）。
  MySQL 的 `LENGTH(blob)` 是字节数，这是方言分歧，不是缺陷。
- `sha512('x')` **字面量**触发 DataFusion 常量折叠（`simplify_expressions`）的
  上游 bug（Arrow error: non UTF-8）；**列路径正常**（`sha512(col)` ✓），
  报错明确、不崩连接。不为它关掉优化器规则（代价大于收益）。

### 186.4 判据与验证

- `sqlwire/encode.rs`：`binary_values_are_hex_encoded_for_the_text_protocol`；
- `sql.rs`：`hex_literal_lands_as_raw_bytes`（含非法 hex 拒绝）；
- `format`：`vortex_round_trips_binary_columns`（Binary 列 vortex 往返 ——
  既有往返用例只覆盖 Utf8，这条路径此前是盲区；实测读回类型就是 Binary）；
- `standalone_smoke.py` 增 `binary_roundtrip`（hex 字面量 → 字节长度 →
  HEX 读回 → sha256 不崩连接）：**15/15**；
- 全量与 clippy 见提交信息。

---

## 187. **chaos 水位用例的一条竞态：视图空了不等于 WAL 落了**（2026-10-16）

### 187.0 现象

`§186` 之后的第三轮全量里，`disk_watermark_aborts_oldest_batch_then_releases_segments`
翻车（16/17）：断言 ③ "WAL 恢复出的批次应全部终态" 读到
`[("01a1459a-…", Pending)]`。前两轮全量都绿 ⇒ **竞态，不是回归**（且本轮改动
全在 sql/sqlwire/format/docs，没碰 wal/ingest/cleanup 一行）。

### 187.1 根因

用例 ② 的等待判据是**视图**（`tracker.non_terminal().is_empty()`）—— 而 monitor
的 abort 是"**先改视图、再异步写 WAL 记录**"。视图空的那一刻，最后一条
`BatchAbort` 可能还没落盘；立即 `full_recovery()` 就还是 Pending。
全量并行时 WAL 写入被真实磁盘 IO 竞争拖慢，窗口被放大到能撞上。

这与 `§171` 修的计时 flaky 是**同一族**（拿一个"此刻"的快照当终局），只是
这次的两个快照分属**两个存储**（内存视图 vs WAL）。

### 187.2 修法：判据还是事件，但事件改成"WAL 也终态"

断言 ③ 从"立即读一次 WAL"改成 `wait_until` **轮询到 WAL 恢复也全终态**
（上限仍是 `wait_until` 既有的 60s 兜底，防"永不落盘"），轮到了再复核一次完整
状态把事实钉住。**没有放宽任何正确性断言** —— 修的是"看得太早"。

### 187.3 验证

`yuntun-chaos --lib` **17/17**（3.05s）；`disk_watermark` 单跑连跑 3 轮 3/3。
