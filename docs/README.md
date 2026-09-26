# docs 索引与文档规则

> **先读这一页**：它回答"想了解 X 该读哪份、两份冲突听谁的、改了代码要同步哪些文档"。

## 1. 按目的选文档

| 我想…… | 读 |
|---|---|
| 知道**今天实际能做到什么 / 还缺什么 / 下一步做什么** | [`status.md`](status.md) ← **现状唯一入口** |
| 知道**某个结论的证据**、某个实现与原设计的**偏差及原因** | [`operation-log.md`](operation-log.md)（§1–§126，按时间） |
| 知道**任务怎么排、里程碑与准出门槛** | [`plan.md`](plan.md)（阶段 WBS / M1–M6 / 风险登记） |
| 知道**架构为什么这样设计**、有哪些 ADR | [`architecture.md`](architecture.md)（12 个 ADR + 域设计） |
| 知道**模块边界、接口契约、数据结构、状态机、配置项** | [`design.md`](design.md) |
| 知道**从现状到分布式目标态该怎么改、按什么顺序** | [`refactor.md`](refactor.md)（S0–S6 / R0–R6） |
| 想知道 **R3（metanode + raft）怎么设计、怎么排期、验收与风险** | [`metanode-design.md`](metanode-design.md)（R3 设计与计划；**R3 已完成** —— 现状看 `status.md`） |
| 知道 **chunk 层（数据平面）的目标态** | [`architecture-with-chunk.md`](architecture-with-chunk.md) |
| 知道**多协议 SQL 接入**怎么设计 | [`sql-access-design.md`](sql-access-design.md) |
| 知道 **DELETE/UPDATE（未实现）**怎么设计 | [`delta-dml-design.md`](delta-dml-design.md) |
| 知道**还存在哪些"实现 vs 文档"的未闭环偏差、各自打算怎么办** | [`closeout.md`](closeout.md)（偏差台账；**可机核的部分由 `crates/testkit/tests/docs_consistency.rs` 守着**） |
| 想**跑起来用起来** | 仓库根 [`README.md`](../README.md) |

**新读者建议顺序**：根 `README.md`（5 分钟）→ `docs/status.md`（10 分钟）→
`docs/plan.md §2.1/§8`（路线与里程碑）→ 需要细节时再进 `architecture.md` / `design.md`。

## 2. 权威性与冲突裁决

```
status.md（现状） + operation-log.md（证据）        ← 最高
      > plan.md（任务与门槛）
      > architecture.md / design.md（设计）
      > refactor.md（改造指南）
```

- **同一事实出现矛盾时，以上序为准**，并且**必须回改低优先级文档**。
  "实现已改、设计文档照旧"是本项目最大的返工来源（`operation-log §25/§27` 各有一例）。
- 文档写"已实现/已具备"时，必须能指向 `operation-log` 的证据段或一个能跑的用例；
  否则一律视为**意图**而非**现状**。

## 3. 改代码时要同步什么（清单）

| 你改了什么 | 必须同步 |
|---|---|
| 默认值 / 配置项 | `operation-log`（附**实测**）+ `status.md §3` + `plan.md §2.2` + 示例配置 `yuntun.toml.example` + 对应设计文档 |
| 新增/改变语义契约 | `operation-log` + `status.md §3`（决策表）+ `design.md` 对应章节 + 一条**断言其反面后果**的用例 |
| 完成某个任务 / 里程碑 | `plan.md`（状态列）+ `status.md §6` + `refactor.md`（若属 S/R 步骤） |
| 发现与设计不符（无论改哪边） | `operation-log`（偏差与理由）+ 回改设计文档对应段落 |
| 架构级决策（ADR） | 新增或**正式修订** ADR（版本演进写明"谁被谁取代、为什么"），并在 `status.md §7` 标注 |

## 4. 文档维护的硬规则

1. **就地升版本，不新建平行文档**。只有"新维度"才新开文件（`status.md` 就是新增的现状维度）。
2. **版本号三处一致**：头部声明 / 正文版本演进 / 文末落款（`architecture.md` 曾出现头部 v10、正文 v12、文末 v11）。
3. **决策不许顺手改**：`status.md §3` 的已定案项，改必附实测数据（`plan.md §2.2`）。
4. **先证据后结论**：结论写进 `status.md` / `plan.md` 之前，证据必须已在 `operation-log` 里。
5. **行文留"为什么"**：写清"不这样做会怎样"（本项目大量坑是"不报错、只产出错数据"，
   只写"怎么做"无法防止被下一个实现者改回去）。
