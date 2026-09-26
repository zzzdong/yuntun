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

| # | 偏差 | 现状（证据） | 决定 | 闭环证据 |
|---|---|---|---|---|
| **D-1** | **ADR-9 的 `durable` 持久性 SLA 从未实现**（架构要求表级分 `best_effort` / `durable`＝本地 WAL **+ S3 归档**，RPO≈0） | `Durability` 枚举**全仓零引用**（`crates/model/src/lib.rs:69-78`）；`IngestConfig.durability` 恒 0、从不被读 | **待定**：① 设计 S3 WAL 归档路径并补任务；或 ② **正式撤回该 ADR**（字段标 reserved） | — |
| **D-2** | compaction 的"**独立 blocking pool** 资源隔离"未落地（设计明确要求"避免挤占 ingest 与 query"） | `docs/design.md:1208` 的要求 vs 实现用普通 `tokio::spawn`（`crates/compaction/src/lib.rs:430-487`）；全仓 `spawn_blocking` 只在 meta 服务层 | **待定**：① 落地（改动不大）；或 ② 撤回该承诺 | — |
| **D-3** | `ProposeRequest.request_id` / `schema_ver` 是**死字段**（设计 §5 规定它们是幂等／OCC 载体） | 客户端恒置空/0（`crates/meta/src/remote_catalog.rs:154-158`），服务端只读 `op`（`crates/meta/src/service.rs:57-74`）；实际由 op 层 `IdempotencyOp` / `EvolveSchemaOp.expected_version` 承担 | **待定**：① 在 proto 里标 `reserved`／写清"已由 op 层承担"；或 ② 接线到服务端 | — |
| **D-4** | R5 的"**partial aggregate 下推** + 冷数据按 datanode 分配"未兑现，**但 M5 已标 ✅** | 数据面回传的是**原始行**（`crates/proto/proto/shard.proto:67-70`）；协调者只 fanout 热数据 | **待定**：① 写进任务表（T13.1/T13.2）；或 ② 把 M5 的 ✅ 降级为"部分" | — |

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
