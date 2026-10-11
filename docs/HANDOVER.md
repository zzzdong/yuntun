# 交接：给下一位开发者（含可直接使用的提示词）

> **用途**：本文回答"接手 yuntun 时先读什么、必须守哪些纪律、今天该干哪一刀、怎么算干完"。
> 现状见 [`OVERVIEW.md`](OVERVIEW.md)，结构见 [`ARCHITECTURE.md`](ARCHITECTURE.md)，
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

最小闭环（MySQL 端口）与客户端用法见根 [`../README.md`](../README.md)；集群 rig 见
`tests/cluster.sh` / `tests/datanode_cluster.sh`（需 podman；**跑 rig 时不要并发跑别的 cargo**）。

---

## 2. 提示词（**可直接复制**）

```text
你在 yuntun 仓库（Rust 2024 workspace，20 个 crate）里工作。这是一个单机优先的直写数据湖
（时序/可观测）：WAL 权威 → chunk 攒批 → 对象存储 + Manifest，元数据权威在 metanode/raft，
对外是 Arrow Flight SQL + MySQL wire 两个端口，两端口共用 crates/sql 这一套 SQL 语义。

【第一步：先读完这五份，再动手（这是硬要求，不要跳）】
1. docs/OVERVIEW.md       —— 今天实际能做到什么、明确不能做什么、已知限制
2. docs/ARCHITECTURE.md   —— 分层/crate 边界/写入与读路径/已定案决策 §7/配置清单 §8/不变量 §9
3. docs/ROADMAP.md        —— 已完成清单、准出门槛、§4 台账（未闭环偏差）、§5 下一阶段（N 组）
4. docs/operation-log.md  —— §169 起的新日志；§1…168 在 docs/archive/operation-log.md（历史，只备查）
5. docs/README.md         —— 文档权威顺序、§号映射、改代码要同步什么

【纪律（违反这些的改动一律不接受）】
- 先证据后结论：任何"已具备/已修复"的说法，必须指向 operation-log 里的一节或一条能跑的用例；
  没有证据的写法只算"意图"。
- 改默认值/语义契约：必须先在 docs/operation-log.md 附**实测数据**（新开一节），
  再回改 docs/ARCHITECTURE.md §7/§8 与根 README.md；顺带更新 yuntun.toml.example。
- 凡"已具备"的能力，必须有一条**断言其反面后果**的用例（本项目五个真缺陷都是"不报错、只产出错数据"，
  功能测试全绿时抓到的）。
- 完成一个任务：更新 docs/ROADMAP.md §5 状态列 + 台账销账；发现与文档不符：先在台账加一行（哪怕还没决定）。
- 不许顺手改 docs/ARCHITECTURE.md §7 的已定案决策（D1–D13）；必须改时附实测并写明原因。
- 不要新建平行的设计/计划文档（就地升版本）；docs/archive/ 是冻结历史，不要改它。

【这一刀要做的事（按 docs/ROADMAP.md §5，从最小的开始，做完再挑下一项）】
N1/N2 已完成（§170 文档重建；§171 测试可信 —— 计时断言已换成事件判据，并有判据禁止新增墙钟断言）。
默认从 N3 开始（G.1 收尾：任务目录的运维面）：
  - /metrics 暴露任务数与租约持有者（G.4）；
  - tests/datanode_cluster.sh 实跑新判据（两个节点都从任务目录拿到过待办）并附输出；
  - 台账 D-25：purge_table_files 之后重算任务（+1 条用例）；
  - 快照还原后 since_ms = 0 导致的 age_ms 口径（§169.3）。
接着按顺序处理 N4（G.2 冷数据归属与下推，台账 D-4 第二半）、N5（D-23 行数瞬时不可见的复现与归因）。
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
| 4 | **任务目录 ≠ 任务队列**（`§169`） | 待办由状态机**派生**、"谁在做"由**租约**回答；proto 里**没有** Claim/Renew/Complete op（与归档计划的候选形态不同，是刻意决定） |
| 5 | **vortex 是默认格式，且读路径是整文件解码** | 体积 3.5–4.5 倍、无组级剪枝、`.idx` 只对 parquet 有效（`D-20`）；写性能相关结论必须重新测，不能沿用 parquet 时代 |
| 6 | **部署包线已量过**：对称延迟 1.5s 就反复选举 | 跨机/跨可用区前必须按 RTT 重定 `connect_timeout`/`RPC_TIMEOUT`/选举超时 |

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
