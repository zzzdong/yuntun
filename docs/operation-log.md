# yuntun 实施操作日志（§169 起）

> 起记时间：2026-10-11（最新：**§174，2026-10-11**）。对应分支：main。
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
