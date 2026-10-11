# yuntun 实施操作日志（§169 起）

> 起记时间：2026-10-11（最新：**§171，2026-10-11**）。对应分支：main。
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
