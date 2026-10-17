# 交接：给下一位开发者（含可直接使用的提示词）

> **用途**：本文回答"接手 yuntun 时先读什么、必须守哪些纪律、今天该干哪一刀、怎么算干完"。
> 现状见 [`OVERVIEW.md`](../OVERVIEW.md)，结构见 [`ARCHITECTURE.md`](ARCHITECTURE.md)，
> 任务与台账见 [`ROADMAP.md`](ROADMAP.md)，证据见 [`operation-log.md`](operation-log.md)。
>
> **这一页本身就是提示词素材**：把 §2 的代码块整段贴给下一个 AI 会话即可开工。

---

## 1. 五分钟上手

```bash
cargo build --release                       # 4 个二进制：yuntun / metanode / yuntun-datanode / yuntun-cli
./target/release/yuntun --config yuntun.toml # 单机跑起来（Flight SQL :50051 + MySQL wire :3306）
cargo test --workspace                      # 全量回归（⚠️ 见 §4 的 flaky 现状）
cargo clippy --workspace --all-targets      # 本仓应 0 告警
cargo test -p yuntun-testkit --test docs_consistency   # 文档判据（改代码/改文档后必跑）
```

最小闭环（MySQL 端口）与客户端用法见根 [`../README.md`](../../README.md)；集群 rig 见
`tests/cluster.sh` / `tests/datanode_cluster.sh`（需 podman；**跑 rig 时不要并发跑别的 cargo**）。

---

## 2. 提示词（**可直接复制**）

```text
你在 yuntun 仓库（Rust 2024 workspace，20 个 crate）里工作。这是一个单机优先的直写数据湖
（时序/可观测）：WAL 权威 → chunk 攒批 → 对象存储 + Manifest，元数据权威在 metanode/raft，
对外是 Arrow Flight SQL + MySQL wire 两个端口，两端口共用 crates/sql 这一套 SQL 语义。

【第一步：先读完这五份，再动手（这是硬要求，不要跳）】
1. docs/OVERVIEW.md       —— 今天实际能做到什么、明确不能做什么、已知限制
2. docs/dev/ARCHITECTURE.md   —— 分层/crate 边界/写入与读路径/已定案决策 §7/配置清单 §8/不变量 §9
3. docs/dev/ROADMAP.md        —— 已完成清单、准出门槛、§4 台账（未闭环偏差）、§5 下一阶段（N 组）
4. docs/dev/operation-log.md  —— §169 起的新日志；§1…168 在 docs/archive/operation-log.md（历史，只备查）
5. docs/README.md         —— 文档权威顺序、§号映射、改代码要同步什么

【纪律（违反这些的改动一律不接受）】
- 先证据后结论：任何"已具备/已修复"的说法，必须指向 operation-log 里的一节或一条能跑的用例；
  没有证据的写法只算"意图"。
- 改默认值/语义契约：必须先在 docs/dev/operation-log.md 附**实测数据**（新开一节），
  再回改 docs/dev/ARCHITECTURE.md §7/§8 与根 README.md；顺带更新 yuntun.toml.example。
- 凡"已具备"的能力，必须有一条**断言其反面后果**的用例（本项目五个真缺陷都是"不报错、只产出错数据"，
  功能测试全绿时抓到的）。
- 完成一个任务：更新 docs/dev/ROADMAP.md §5 状态列 + 台账销账；发现与文档不符：先在台账加一行（哪怕还没决定）。
- 不许顺手改 docs/dev/ARCHITECTURE.md §7 的已定案决策（D1–D13）；必须改时附实测并写明原因。
- 不要新建平行的设计/计划文档（就地升版本）；docs/archive/ 是冻结历史，不要改它。

【这一刀要做的事（按 docs/dev/ROADMAP.md §5，从最小的开始，做完再挑下一项）】
N1/N2/N3 已完成（§170 文档重建；§171 测试可靠；§172 任务目录收尾 + /metrics 的 jobs 段），
N4 的**上半刀也已落地**（`§173`：可分解聚合下推 —— 3 万行从 244KB 降到 64 字节），
N5 的归因推进到只剩一层（`§174`）。
N5 也已闭环（`§174` + `§175`：机制定案 + 修复 + 确定性用例；不变量已写进 `ARCHITECTURE.md §9` 的 I13）。
N4b（冷文件按节点分配，`S5-2`）**已收口（`§182`）**：分配 + 远端折叠 + 回退都通了。
**下一步（同一条路的下一格）**：**动态负载均衡** —— 今天负载信号为零 ⇒ 静态确定性分配。
  要先有信号（datanode 侧的在途查询数 / `/metrics`），再让 `placement::assign` 支持权重。
  - 冷文件（共享对象存储）**谁都能读** ⇒ 可以分配；热数据（chunk/WAL/本地）**归属绑定** ⇒ 只能自己读；
  - 要做：`shardrpc` 的 handler 接上 DataFusion（数据面进程里本就有，但今天的 handler 只持
    `Arc<dyn ShardReader>`）+ "替我读这几个文件"的 RPC（要能吃 DV、schema 对齐、行组剪枝）
    + 协调者的分配规则与降级（partial 语义已经通了）；
  - `§173`/`§176`/`§177` 的聚合/谓词/分组契约正好是那个 RPC 的**载荷**（叠上去，不是二选一）。
（N4a 与 N4a-2 都已完成：聚合、谓词、分组下推。）
  - **N4b**：**冷文件按节点分配**（归档 `S5-2`）：今天协调者自己读**全部**冷文件；
    要做需要"读指定文件（并替我算）"的新 RPC，以及"谁来分配"的确定性规则
    （`source_instance` 只是**热副本**归属，**不是**文件 owner）。
顺带（可选）：`tests/datanode_cluster.sh smoke` 的判据 ⑥ 现在会在"没有 partial 标记却少行/多行"时
判失败（`§174`）—— 再跑几轮 rig 是有价值的回归证据，但**不是**准出条件。
接着按顺序处理 N5（D-23：合并提交期间"行数瞬时不可见"的复现与归因）、N6（真云 S3 + 跨机、鉴权、冷读预算）、
N7（T6.15 / spill 复用 / MSRV 1.95 真编译 / D-18 定案）。
先做哪一项由你说，但**一次只做一刀**，且必须留下证据与文档同步。

【每一步的交付要求】
- 说明"改了什么、为什么这么做、边界在哪（**没做什么**）"；边界一栏必须写。
- 命令与结果照实报（跑了什么、绿了没有、哪条在什么条件下会红）；不许把"没跑"写成"应该没问题"。
- 改完跑：cargo check --workspace --all-targets、相关测试、cargo test -p yuntun-testkit --test docs_consistency、
  cargo clippy --workspace --all-targets（口径类声称只有这条命令能守）。
- 提交信息按本仓风格：`<type>(<crate 列表>): <一句话>（§N）`，正文写做了什么/决定/边界/验证。
```

---

## 3. 今天该知道的六件事（读文档时容易漏的）

| # | 事实 | 为什么重要 |
|---|---|---|
| 1 | **§169 起在新日志，§1…168 在归档** | 代码注释里的 `§166`/`§88.6` 指归档；别以为日志"丢了" |
| 2 | **台账编号 `D-*` 沿用不变**（`D-4`/`D-18`/`D-20`/`D-21`/`D-23`/`D-25`/`D-26`） | 代码注释里引用着它们；重号即漂移 |
| 3 | **测试里不许拿墙钟当判据**（`D-26` 已闭环，`§171`） | 用**事件**（并发峰值 / 按轮驱动到收敛）；确需"必须有界"的兜底要写理由 + `§171` 豁免标记 —— 否则新判据会拦你。另：cargo 是**逐个 test binary 串行**跑的，并行发生在**同一个 binary 内**（别再把锅甩给 chaos） |
| 4 | **任务目录 ≠ 任务队列**（`§169`/`§172`） | 待办由状态机**派生**、"谁在做"由**租约**回答；proto 里**没有** Claim/Renew/Complete op（与归档计划的候选形态不同，是刻意决定）。运维面：`/metrics` 的 `jobs` 段 + `meta_probe tasks/leases`。**加一个改"文件或 DV"的状态机入口 ⇒ 必须回填 `ARCHITECTURE.md §6.1` 的入口清单并重算任务**（漏了 = 残留待办指向空分片，`D-25`/`D-29`） |
| 7 | **datanode 的 `--meta` 必须给多台**（`§172`，台账 `D-30`） | 只给一台时，那一台不是 leader ⇒ 这个数据节点的**所有写都失败**（重试只能在已知地址集合里换）。容器 rig 里给全三台 |
| 5 | **vortex 是默认格式，且读路径是整文件解码** | 体积 3.5–4.5 倍、无组级剪枝、`.idx` 只对 parquet 有效（`D-20`）；写性能相关结论必须重新测，不能沿用 parquet 时代 |
| 8 | **聚合下推只覆盖最窄的一类形状**（`§173`/`§176`/`§177`） | `count` / `sum(整数)` / `min` / `max` **+ `WHERE` 谓词 + `GROUP BY`**（整数/字符串键、NULL 是一组、多列键、投影须"键在前聚合在后"）；`avg` / 浮点 / 复杂谓词 / 表达式键 / `HAVING` 一律原路（正确但不过网省）。**加一种形状要同时改四处**：`model::agg`（或 `model::predicate`）的判定与求值、`shard.proto` 的消息、`agg_pushdown` 的形状判定、以及**服务端与 `WireFetch` 都要走同一条折叠代码**（漏一处就是"判定说能、执行说不能"，或者"测试里的假服务端比真的宽松"）。扩谓词前先读 `§176.2`：`NOT` 要先有真正的三值求值才能收 |
| 9 | **`D-23` 已闭环，但口径要记住**（`§174`/`§175`） | 机制是"**快照号先于文件清单发布**"（探测成功、载荷失败 ⇒ 号超前 ⇒ 增量永远漏掉 `== since` 的墓碑与产物）。修复口径：**号只与载荷一起落地；宁可号落后，不许号超前**。⇒ ① 别再把 `count(*)` 的瞬时偏差当 flaky：rig 判据 ⑥ 会把"没有 `partial` 标记却少行/多行"**当场判失败**；② 动 `RemoteCatalog::refresh()` 的两跳时，先想"失败路径上号走到哪了" |
| 6 | **部署包线已量过**：对称延迟 1.5s 就反复选举 | 跨机/跨可用区前必须按 RTT 重定 `connect_timeout`/`RPC_TIMEOUT`/选举超时 |
| 10 | **冷折叠接不上的节点 ⇒ 回退本地读，不是 partial**（`§182`） | 冷文件在共享存储上、协调者自己就能读 ⇒ "远端拿不到"只是"换个人读"。同理：没有任何节点接得上 ⇒ **全本地算**（与改造前逐字相同） |
| 11 | **远端冷折叠必须和本地一样剪枝**（`§183`） | 谓词要一起过网并交给读的那一边（文件级 `FileManifest.stats` + 行组级 footer）。**不带过滤就会读整文件** ⇒ 那是"分配让人多读"的**回退**，而且不报错（只表现为远端 CPU/IO 变高） |

---

## 4. 已知坑清单（少踩一半）

- **跑 rig 时不要并发跑别的 cargo**：release 构建会与 `cargo test` 争 target（实测报过 `No such file or directory`）。
- **datanode 容器镜像必须 glibc ≥ 2.44**（用 `archlinux`）；`debian:trixie-slim` 2.41 / `fedora-minimal` 2.43 跑不起宿主编的二进制。
- **`cargo build --release -p X --bin Y -p Z` 是坑**：`--bin/--example` 是**全局目标筛选**，会悄悄把 `-p Z` 的二进制排除掉（`§169.2-4` 因此让 rig 假绿过一次）。
- **跨层表标识一律 `schema.table`**：`TableMeta.name` 是**裸名**；用裸名在 `MemoryCatalog` 里"碰巧能用"，远端目录下会静默列不到文件（`§167`）。
- **测试临时目录**：写盘类用例默认落 tmpfs（`testkit::TestDir::tmpfs`）；需要真 fsync/崩溃语义的用 `TestDir::disk`。
- **改配置键**：先改 `crates/server/src/config.rs` 的模板，再回来改 `ARCHITECTURE.md §8` —— 有判据盯着（照着文档写配置必须能启动）。
- **启动自检会告警的情况不要忽略**（如 `chunk_max_resident_secs ≤ max_flush_delay + spread` ⇒ 相位分散静默失效）。
- **写测试**：判据要表达**性质**（"三个来源同时在飞"、"驱动 N 轮后收敛"），不要表达**机器多快**
  （`elapsed < X`）—— 后者在 `cargo test --workspace` 下必然 flaky，且已加判据拦截
  （`wall_clock_assertions_need_a_reviewed_waiver`，见 `ARCHITECTURE.md §9` 的 I11）。
- **改完代码请跑全仓 clippy 而不只是定向测试**：口径类声称（如"本仓 0 告警"）只有那条命令能守；
  `§169` 就是这么留下 4 条告警的（台账 `D-27`）。
- **等待上界不许等于"被等待的东西本身"**：`§172` 又抓到一处 —— 用例等"GC 租约过 TTL 后接管"，
  而 GC 租约 TTL 的**下限就是 30s**，上界却写 30s ⇒ 必然抖（它单跑就要 30.24s）。
  写 `wait_for(…)` 前先问："这件事**最早**可能多快发生？"上界至少取它的数倍，并写明理由（§171 豁免）。
- **改状态机入口要顺手看任务目录**：见 §3 第 4 行；`ARCHITECTURE.md §6.1` 有入口清单与 `since_ms` 口径。
- **给聚合下推加一种形状**（`§173`）：先在 `model::agg` 定"可分解 + 精确"（写清对拍凭什么逐位相等），
  再动 `shard.proto`，最后放宽 `agg_pushdown::detect` —— 并且**先写对拍**（`with_agg_pushdown(false)`
  关掉下推跑原路，两条路必须逐位相同）。放宽判定比收紧判定危险得多：漏判 `DISTINCT`/`GROUP BY`
  就是"少算而零报错"。
- **运维面两句话**：`/metrics` 的 `jobs` 段（有哪些活 / 谁在做，带标签行）与 `meta_probe tasks/leases`；
  `tasks` 的 `age_ms=unknown` 是**正常**（快照还原后还没被携带时钟的 op 触碰过）。
