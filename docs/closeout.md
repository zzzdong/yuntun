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

> **2026-09-26 起有一条新发现（`D-7`）**

| **D-8** | **ADR-1 的"Vortex 主格式"缺实测支撑**（`§140`）：实测 **Vortex 是 Parquet 的 3.5–4.5 倍体积**（递增整数 23800/6680、随机串 63348/14000），且**已是压缩后的默认配置**（`BtrBlocksCompressor` 缺省启用） | 用例 `vortex_tests::sizes_are_reported_for_two_data_shapes`（`--features vortex`）每次都把两个数字打出来 | **待决策**：继续投 Vortex / 改 ADR-1 回 Parquet 主 / 先做"压缩参数 + 读取侧零拷贝收益"专项测量 | `§140` | —— 台账的意义就在这里：它不是"上次审计的存档"，
> 而是"任何时刻新发现的偏差都先进这儿"。

| # | 偏差 | 现状（证据） | 决定 | 闭环证据 |
|---|---|---|---|---|
| **D-7** | ✅ **`T9.x` Vortex 已闭环**（`§133`–`§139`）：原判据"接不进来"是 MSRV 1.94 逼出的回退；抬到 1.95 后用 `vortex 0.86`（arrow 59.2+）⇒ codec 已实现并**往返逐值相同**（用例在 `--features vortex` 下） |

> **本节的其余部分为空（2026-09-26，`§127`）。** `§123` 立台账时的四类差异（`D-1`~`D-4`）加上后续新开的
> `D-5`/`D-6` **全部闭环** —— 按本节自己的判据（"§1 空了，偏差才算清完了"），到这里算清完。
> 新发现照 §5 的纪律往这儿加行。

| # | 偏差 | 现状（证据） | 决定 | 闭环证据 |
|---|---|---|---|---|
| **D-1** | **ADR-9 的 `durable` 持久性 SLA 从未实现**（架构要求表级分 `best_effort` / `durable`＝本地 WAL **+ S3 归档**，RPO≈0） | `Durability` 枚举**全仓零引用**（`crates/model/src/lib.rs:69-78`）；`IngestConfig.durability` 恒 0、从不被读 | ✅ **已落地（v1）**：按 **①** 做了 —— WAL 段持续归档到共享存储（含"正在写的段"）+ 丢盘后**拉回本地再走既有恢复通路**；RPO 的界 = **归档间隔 + 一次上传时延**（默认 1s 间隔；"真正的 0"要同步归档，不选） | `§125`；`crates/ingest/tests/wal_archive_durable.rs`（**有对照组**：同场景开关一开一关 ⇒ 8/8 vs 0/0）；接入口 `[wal] archive_prefix`（standalone） |
| **D-2** | compaction 的"**独立 blocking pool** 资源隔离"未落地（设计明确要求"避免挤占 ingest 与 query"） | `docs/design.md:1208` 的要求 vs 实现用普通 `tokio::spawn`（`crates/compaction/src/lib.rs:430-487`）；全仓 `spawn_blocking` 只在 meta 服务层 | ✅ **已落地**：编解码（`format::{read_batch,write_batch}`，两者都是 `async fn` ⇒ 必在运行时里 ⇒ `spawn_blocking` 安全）与 compaction 的 `concat` 全部走**阻塞池** | `§124`；观测配方 `YUNTUN_FORMAT_TRACE=1`（实测 `encode` 的**执行线程**与提交方不同 ⇒ 活真的搬走了） |
| **D-3** | `ProposeRequest.request_id` / `schema_ver` 是**死字段**（设计 §5 规定它们是幂等／OCC 载体） | 客户端恒置空/0（`crates/meta/src/remote_catalog.rs:154-158`），服务端只读 `op`（`crates/meta/src/service.rs:57-74`）；实际由 op 层 `IdempotencyOp` / `EvolveSchemaOp.expected_version` 承担 | ✅ **已决定**：语义由 **op 层**承担（那两处才是权威），proto 保留字段号但**标注为历史字段**（删字段会让老客户端读出 0 而看不出区别） | `§124`：`meta.proto` 的 `ProposeRequest` 上那段注释 |
| **D-9** | **`CatalogState.files` 以 `batch_id` 为键，而 `commit_files` 会把请求里每个文件都赋成同一个 `batch_id`** ⇒ 一次请求带两个文件时后一个**静默覆盖**前一个（文件从清单消失 = 静默少数据，零报错）。`delta-dml-design §6.2` 的孤儿对账推导 `dv → file_path → batch_id` 也以"一批次一个文件"为前提 | `§147` 造"两个文件"夹具时撞到（`EXPLAIN` 的 `file_groups` 只剩一个）；代码位置 `crates/catalog/src/state.rs::commit_files` | ✅ **已落地**：`commit_files` **最前面**拒绝 `files.len() > 1`（先于任何写入 ⇒ 拒绝是原子的）+ 单测 `one_batch_must_not_carry_two_files`。不改键：`files` 的键、幂等对账、孤儿对账**都**建立在"一个批次一个文件"上 | `§147.2` |
| **D-12** | ✅ **已裁决（2026-09-27，用户）**：`UPDATE` 采用 `plan.md` F.7 决策 5 的"**一个 op 承载、原子可见**"（`delta-dml-design §4.2` 的"可见性不原子"作废，设计文档已加修订块）。判据：文件 `valid_from` 与删除向量 `applied_at` **同号**、一次状态转换只推一个快照号、旧快照下两半都不可见 | 原文冲突：`plan.md:929` vs `delta-dml-design.md:167-168` | ✅ **已闭环**：`CatalogState::apply_update`（一个 `next_snapshot()` 服务两个事实）+ proto `UpdateOp`（tag 20）+ 两种形态对拍（`remote_catalog_parity` 阶段 ⑥b） | `§153` |
| **D-10** | ✅ **设计 §6.2 的"DV 占比触发合并"已落地**（`§152`）：`dv_ratio_threshold = 0.1` + `dv_min_card = 1000`（两个守卫都要满足，比原文的"或"更保守），单文件也能被 DV 触发消费 | `§150.3` 记的欠账：合并曾只按 `min_files` 判定 ⇒ 单个带 DV 的文件不会被自动消费、删除成本不收敛 | ✅ **已闭环**：触发条件与 `min_files` 解耦（两条路任一成立），判据 = `dv_triggered_compaction` 的 4 条用例（含两条"不该触发"的反面） | `§152` |
| **D-11** | **`embedded`（默认）形态的"重启持久性"缺端到端用例**：同一进程内重新打开 meta 目录会一直 `FjallError: Locked`（`MetaNode::drop` 之后锁也不释放，重试 5 秒无效）⇒ 现有夹具只能覆盖"另一个进程重启"这一形态而**做不了**它。产品侧不受影响（重启就是新进程），但这意味着"默认形态重启后删除还在"这句话目前只有**结构性**证据（fjall 落盘 + 读侧走目录），没有端到端用例 | `§151.3`（`sql_delete_e2e::delete_works_in_the_default_embedded_assembly` 里那段说明） | ✅ **已闭环**（`§159`）：根因**不是** `Drop`（它一直正确：发 `Stop` + join raft 线程），而是 `build_embedded_catalog` 里 `tokio::spawn` 的 **metanode loopback gRPC 服务任务**持有 `NodeHandle`（内含 `FjallStorage` 克隆）⇒ **它不停，fjall 的目录锁就不放**。修法：任务随 `shutdown` 退出 + `JoinHandle` 交给调用方（`spawn_background` 一并返回）⇒ 既有"cancel + join 全部后台任务"配方自动覆盖；**并删掉**了现场那个"看到 `Locked` 就重试 5 秒"的兜底（它把这个缺陷藏了起来）。新用例 `embedded_restart_e2e` 正面断言"重开一次成功"，且**挪走数据 WAL** 以钉死证据来源 = fjall | `§159`（2026-09-29） |
| **D-13** | **`UPDATE` 在"缺列文件"上会响亮失败**：schema 演进（`ALTER TABLE ADD COLUMN`）之后，老文件里没有新列，而 `UPDATE` 的定位+投影是按**当前表 schema** 生成的 SELECT 列表 ⇒ DataFusion 报 `No field named …`，整条 `UPDATE` 失败。现状是**失败而不是错结果**（可接受），但用户会问"为什么加了一列之后改不动了" | `§154.3`（`crates/sql/src/dml.rs::projection_sql` 用表 schema，而 `query::locate` 按文件真实列注册内存表） | ✅ **已闭环**：`§155` —— 定位路径（`query::locate::read_with_row_idx`）读文件后先按**表 schema** 对齐（复用**同一个** `arrow_util::align_batch`：缺列补 NULL / 多余列丢掉 / 类型宽化），再补行号列 ⇒ DML 与 `SELECT` 对同一批行永远同一答案。另一条备选（"跳过缺列文件并告警"）**明确不用**：跳过 = 那些行不被改，而用户的谓词明明命中它们 = 静默少改，比报错糟 | `§155`（2026-09-29） |
| **D-16** | **ADR-9 定的是"表级持久性"，而归档开关是节点级的**：`IngestConfig.durability`（`0=best_effort 1=durable`）字段存在且随表落盘，但**从未被读**；真正开归档的是 `[wal] archive_prefix` ⇒ 开了就归档**全部**表。⇒ 今天"给某张表开 `durable`"做不到，粒度只能是整节点 | `§161` 核 ADR-9 的 RPO 口径时发现（`grep durability` 只命中定义处，没有消费点） | ✅ **已闭环**（`§162`）：归档改为**按表**判定 —— `ArchiveConfig.all_tables`（默认 `true` = 老行为，不静默改既有部署的 RPO）+ `segment_is_archived`（段里出现过 `durable` 表的 `Data`/`BatchPending` ⇒ 归档；读不了 ⇒ 保守归档）+ 每轮解析 durable 表集合；用例覆盖"省钱的一半 / 不能丢数据的一半 / 共租代价"。⚠️ **粒度是段**（记录帧不带 seq ⇒ 过滤记录会让后续 seq 位移、破坏 `BatchPending.wal_seq_start/end`）⇒ 真正的按记录过滤见 `D-18` | `§162`（2026-09-30） |
| **D-18** | **按表归档只能"放宽"不能"收紧"**：`durable` 表与别的表共段时，同段数据一起进归档 ⇒ 成本节省在多租户节点上会趋近于零（"只归档 durable 表的数据"做不到） | `§162.0` 推演"过滤记录再编码"时发现：WAL 记录帧是 `length \| crc \| type \| payload`，**seq 由 `header.first_seq` + 位置推导**（`segment.rs::decode_with_stop`）；过滤会让后续记录的 seq 位移，而 `BatchPending.wal_seq_start/end` 记的是原区间 ⇒ 恢复时吸收错记录（静默错数据） | ⏳ **待做**（不属于阶段 2 的清单，先入册）：两条候选 —— ① **记录自带 seq**（格式变更：帧加 seq 字段 + 向后兼容读旧段，改动面在 `wal` 而非归档器）；② **按表分段**（写入路径分裂：每表/每 durability 类一个段集合，改动面在 `WalWriter` + 恢复 + 压缩口径）。两条都要单独定案 + 迁移计划 | `§162`（2026-09-30） |
| **D-17** | **`plan.md §5.1` 写着"文档同步（ADR-10 等）｜未修订"，而同一文件 `§2.3-1` 早已把 ADR-10 标 ✅** —— 同一份文档两行结论相反；`refactor.md` 的 S2-9 行也还写着"剩余：spread 量级定案 + ADR-10 原文正式修订"（两项都已在 2026-09-18 完成）；`bench_baseline.rs` 的注释把 ADR-10 的目标写成了"承诺"（而修订版明确它不是硬不变量） | `§161`（范围由 `plan.md §2.3` 与 `§5.1` 的冲突定出） | ✅ **已闭环**：三处 stale 声称全部改正并写明"曾写错"，`design.md §11` 整段重写、README 新增「已知限制」节；另加两条判据（ADR-10 定案数值 ↔ 配置默认值、§11 的键 ↔ 解析器）让它以后**能跑红** | `§161`（2026-09-30） |
| **D-15** | **`plan.md §5.1` 的"观测指标（S1-11）"一行长期写着"未接"，而同一文件的 `T6.12` 行早已是 ✅ 已完成** —— 同一份文档自相矛盾，读者（含下一位实现者）会按"未接"那行去重做一遍已经做完的事 | `§160` 补 HTTP 导出时顺手核对发现（`grep S1-11` 同时命中两行，结论相反） | ✅ **已闭环**：`§5.1` 那行改成"✅ 已接（`T6.12` + `§160` HTTP 导出）"并**把这次漂移写进表格**（让"曾经错过"本身可见）；`plan.md §4` 的"本阶段未做"列表同步删掉该条；`status.md` 的缺口列表同改 | `§160`（2026-09-30） |
| **D-14** | **`§150` 修"不误删 DV"时把"能回收 DV"一起堵死了**：对账键改成"锚定的数据文件"之后，"对象写了、`apply_*` 没落地"的幽灵 DV 因为锚定文件还活着，每轮都被当成"已知"放过 ⇒ **永不回收**（设计 §7 明说这种孤儿要回收 ⇒ 文档与代码不一致，空间只增不减） | `§156` 推演 `§150` 的判据时发现（`is_object_protected` 只有一个条件）；代码位置 `crates/compaction/src/lib.rs::classify_orphans` | ✅ **已闭环**：判据改**两条件合取**（锚定文件在保护集 **∧** 目录里有事件引用）+ `CatalogOps::dv_object_paths` 提供"被引用"集合 + 活体用例（活 DV 保住 / 幽灵回收 / 锚定丢失回收 / 数据文件不动） | `§156`（2026-09-29） |
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
