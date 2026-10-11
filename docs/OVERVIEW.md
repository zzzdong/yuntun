# yuntun 项目说明与现状（OVERVIEW）

> **版本**：v2.0 ｜ **日期**：2026-10-11 ｜ **定位**：**现状唯一入口**。
> 本文件说的是"**今天实际是什么样、还缺什么**"；结构原因见 [`ARCHITECTURE.md`](ARCHITECTURE.md)，
> 任务与台账见 [`ROADMAP.md`](ROADMAP.md)，证据见 [`operation-log.md`](operation-log.md)
> （§169 起）与 [`archive/operation-log.md`](archive/operation-log.md)（§1…168）。

---

## 1. 一句话

**yuntun 是一个单机优先的直写数据湖**（时序 / 可观测场景）：

```
客户端 → WAL（本地、权威）→ 攒批（chunk 层）→ 对象存储（vortex/parquet + Manifest）
                      ↘ 内存热数据（读己之写）        ↘ metanode/raft 管元数据
```

对外暴露 **Arrow Flight SQL（gRPC）** 与 **MySQL wire（:3306）** 两个 SQL 协议端口，
两者共用**同一套** SQL 实现（`crates/sql`）。单机是一个可交付的数据库；
多节点是同一套代码 + 进程拆分 + raft，**没有第二套数据路径**。

---

## 2. 目标与非目标

| | 内容 |
|---|---|
| **目标** | 直写摄入（Flight DoPut / SQL INSERT / CLI）→ 立即可查；时间窗口对齐攒批 + 确定性相位分散（削峰）；Schema 演进（加列/删列）；**行级删除与更新**（删除向量 + 原子 UPDATE）；开放格式（vortex 主 / parquet 回退）；行组级索引剪枝；单进程可交付、v2+ 平滑分布式 |
| **非目标（今天仍成立）** | 事务 / 跨表 ACID；视图 / 存储过程 / 触发器；多租户强隔离；独立 frontend 进程；客户端侧发现与软路由（落点由部署显式给定）；Kafka / InfluxDB LP 接入（延后） |

> ⚠️ 归档架构文档 `archive/architecture.md §2.2` 曾把"行级 UPDATE/DELETE"列为非目标 ——
> **已过时**：今天 `DELETE FROM t WHERE …` 与 `UPDATE t SET … WHERE …` 都已落地
> （见 §4；细节见 `ARCHITECTURE.md §4`）。改类型 / 可空性 / 重命名仍**明确拒绝**。

---

## 3. 交付形态与部署形态

**4 个二进制**（同一 workspace，20 个 crate）：

| 二进制 | crate | 角色 |
|---|---|---|
| `yuntun` | `standalone` | 单机全组件（Flight + MySQL + 内嵌 metanode + 压缩/GC） |
| `metanode` | `meta` | 控制平面进程（raft + fjall，可 3 节点成组） |
| `yuntun-datanode` | `datanode` | 数据平面进程（读写 + 压缩/GC + 可选 SQL 面） |
| `yuntun-cli` | `client` | Rust SDK + CLI |

| 部署形态 | 形态说明 | 证据 |
|---|---|---|
| **单机** | 单二进制 + 一份 TOML；`[meta] mode = "embedded"`（进程内 1 节点 raft + fjall + loopback gRPC） | `§56` / `§58` |
| **多节点** | N 个 `metanode`（raft）+ M 个 `yuntun-datanode`（各私有 WAL/spill 目录）+ **共享冷存储**；客户端连**一个**能接 SQL 的端点（contact point） | `§103` / `§108` / `§167` |
| **容器化 rig** | `tests/cluster.sh`（3 metanode，smoke/soak/lossy/netem）+ `tests/datanode_cluster.sh`（3 metanode + 2 datanode） | `§165` / `§167` |

**节点私有状态**只有两处（其余都可重建）：**WAL 目录** 与 **spill 目录**。
冷存储是共享的，元数据权威在 metanode/raft。

---

## 4. 能力矩阵（今天真能用什么）

### 4.1 能用

| 维度 | 现状 |
|---|---|
| SQL 语义 | `crates/sql` 是**唯一实现**，两端口共用；`SELECT` / CTE / 聚合 / `information_schema` / 8 个 JSON 函数，DataFusion 55 执行 |
| 写入 | `INSERT … VALUES` / `INSERT … SELECT` / Flight `DoPut` / CLI（CSV/JSONL/Parquet）；三条入口汇入**同一 ingest 管线** |
| DDL | `CREATE/DROP TABLE`、`CREATE/DROP DATABASE`、**`ALTER TABLE ADD/DROP COLUMN`**（OCC 演进、走 raft） |
| **行级删除** | `DELETE FROM t WHERE …`：删除向量（roaring 位图）表达 + 读侧行选择 + WAL `DeletePayload` + 重启重放 + compaction 消费（合并重写时**应用** DV，提交后 `revoke`）；**无 `WHERE` 的全表删**走文件级下线（`purge_table_files`） |
| **行级更新** | `UPDATE t SET … WHERE …`：**一个 op 承载、原子可见**（DV 与新行文件同一个快照号 ⇒ 看不到"删了没插/插了没删"） |
| 索引 | **行组级**：写时随 flush 生成 `{batch}.idx`（逐组 min/max + XOR，只给排序列）；读时文件级 → 组级两级剪枝。⚠️ **只对 parquet 生效**，vortex 路径无组级剪枝 |
| **分布式聚合下推** | `SELECT count(*) / count(c) / sum(整数列) / min / max FROM t`（**无 `WHERE`、无 `GROUP BY`**）在多节点下**只回标量**：热来源在它那边折（`FetchShardAggs`），冷的一半仍由 DataFusion 算，协调者合并 —— 实测 3 万行从 **244KB 降到 64 字节**（`§173`）。其余形状（谓词 / `GROUP BY` / `avg` / 浮点 `sum`）**原路**（正确但不过网省），见 `ARCHITECTURE.md §4` |
| 元数据 | `SHOW TABLES/COLUMNS/DATABASES`、`DESCRIBE`、`SHOW CREATE TABLE` |
| 客户端 | mysql CLI / 驱动预编译 / DBeaver+JDBC / ADBC+pyarrow 均已验 |
| 幂等 | 三通道（SQL 注释 / CLI `--key` / `FlightData.app_metadata`）；**一次请求一次键 + 批次派生键** |
| 持久性 | WAL 权威（CRC + 组提交 + fsync）；崩溃恢复含 DDL 重放 + 非终态批次重提交；`durable` 档 = WAL 段归档到共享存储（**RPO = 归档间隔 + 一次上传时延**，默认秒级） |
| 可见性 | 读己之写：fsync 后 ≤1 个 scan 周期（默认 100ms）可查，此时零已提交文件 |
| 内存 | chunk 区 / query 区**硬分区互不抢占**；60/80/95 阶梯；spill（IPC+LZ4+CRC）；95% 明确拒写 |
| 后台作业 | Compaction（**按 `(table, shard)` 分租约**，多节点可同时合并不同分片）+ 孤儿清理（全局 `gc` 租约）+ **任务目录**（待办是状态机派生的事实，多节点看到同一份；任何改"文件或删除向量"的入口都会重算，见 `ARCHITECTURE.md §6`） |
| 运维 | 周期结构化打点；`[metrics]` 可选 HTTP 导出（`/metrics` Prometheus 文本、`/metrics.json`、`/healthz`），其中 **`jobs` 段报"有哪些活 / 谁在做"**：每条待办（表·分片·文件数·删除行数·**年龄**）+ 每把租约（用途·持有者·代次·持有/剩余秒）+ 两个 `*_error`（"读不到"与"没有"能分清）；Prometheus 侧**每行带标签**；`metanode` 成员变更 + 运维 CLI；`meta_probe tasks/leases` |

### 4.2 明确不能（**都是明确报错，不是静默错**）

- 事务语义（单语句自动提交；`BEGIN/COMMIT/ROLLBACK` 为 no-op）、视图、存储过程；
- `ALTER TABLE` 只支持 `ADD COLUMN` / `DROP COLUMN`：**改类型（破坏性）、可空性变更、重命名**都是明确拒绝；
- `UPDATE` 不支持 `FROM` / `RETURNING`，也不支持无 `WHERE` 的全表更新；
- MySQL 端口 **trust 无鉴权**（按网络隔离部署）；metanode 端口同样无鉴权；
- MySQL 轨结果集仍先收集后逐行写（Flight 轨已流式）；MySQL 轨给不出 `partial` 标记（上游 `opensrv` 卡住）；
- SQL DDL 的 `TIMESTAMP(p)` 精度不入 schema（一律 `Timestamp(ns)`）。

---

## 5. 多节点边界（写清楚，避免误用）

| 场景 | 能不能用 | 依据 / 前提 |
|---|---|---|
| 多节点写同一分片 | ✅ | 设计即"各出各的文件 + 查询侧合并"；共享冷存储 + 按实例拉热数据 ⇒ **逐行精确相等**（`§91`/`§98`），"提交→标记"窗口不重复计数（读侧栅栏 `§99`） |
| 多节点跑 compaction / 孤儿 GC | ✅ | `(table, shard)` 分租约 + `commit_compaction` epoch 栅栏 + 在途登记；开 GC 的多节点并发压测零误删（`§84`）；**任务目录**让待办全集群可见（`§169`） |
| 客户端落点 | ✅ | **显式给定的 contact point**：连一个能接 SQL 的端点，扇出/合并由它负责；**客户端侧发现/软路由明确不做**（`§100`） |
| **降级的可见性** | ✅ | 有来源读不到时服务端照常返回可用结果并标 `partial`（`architecture §4.2` 第三条）；**调用方能看见它**（`§174`）：SDK `query_full` → `PartialNotice`（缺失来源 + 原因），CLI 把 `WARN: partial result` 打到 **stderr**、stdout 只放数据。⇒ 脚本能把"降级"与"可见性缺口"分开（台账 `D-31`）；`rig` 判据 ⑥ 就是这么用的 |
| **数据进程的元数据面** | ⚠️ **必须给多台** | `yuntun-datanode --meta` 支持逗号分隔的**种子列表**（`§172`）：只给一台时，那一台不是 leader ⇒ 这个节点的**所有写都失败**（重试只能在已知地址集合里换）。容器 rig 里给全三台 |
| 真实对象存储（本机 S3/MinIO） | ✅ | 单机读写两向（`§101`）+ 多节点真写对拍与冷读（`§102`） |
| **真云 S3（TLS/签名/限流）与跨机 / 跨可用区** | ⚠️ **未验** | 未测；且部署包线已有量过的数字，见 §7 |
| **鉴权 / 多租户** | ❌ | 端口 trust 无鉴权（metanode 的成员变更已做，鉴权没做） |

---

## 6. 质量与证据网

规模：**72,082 行 Rust / 20 个 crate / 580 个测试函数 / 用例全绿（⚠️ 并行跑时 2 条计时用例会 flaky，见 §6.1） / clippy 本仓 0 告警**
（工具链：本机 rustc 1.98.1 + edition 2024；**MSRV 声明 1.95**，由 `vortex 0.86` 顶出，
**尚未经真·1.95 编译验证**）

| 层 | 证据 | 位置 |
|---|---|---|
| 单元 / 集成 | `cargo test --workspace`：**连续 3 轮全绿**（各 104 段）／0 ignored／"拿墙钟当判据"有判据拦（§6.1） | 全仓 |
| chaos | **11/11** 场景（真实磁盘 + 跨重启 + 并发），进程中抓出**五个真缺陷**且全部已修 | `crates/chaos` + `§27…§31`/`§99` |
| 容器化真集群 | `tests/cluster.sh smoke`/`soak`（3 metanode，真断网 + 接回收敛硬断言） | `§165` |
| 多 datanode 容器 rig | `tests/datanode_cluster.sh smoke`（两节点都真的合并过 + 行数守恒 + `gc` 单持有 + 杀掉干活的那台另一台接手） | `§167` |
| 分布式对拍 | 两写进程并发真写 ⇒ 两边查询逐行相等（含反证） | `§98` |
| 文档判据 | 改代码会让规模行 / proto 迁移表 / ADR 定案数值 / 配置清单变红 | `crates/testkit/tests/docs_consistency.rs` |

### 6.1 测试稳定性（`§171` 已收口）

- **曾经**有**四条**"拿墙钟当判据"的用例在并行跑全量时失败、单独重跑都 ok：
  `yuntun-compaction::tests::concurrent_writers_compaction_and_gc_converge`（30s 墙钟收尾上限）、
  `yuntun-query::partial_fanout::slow_sources_wait_in_parallel_not_in_sequence`（400ms 墙钟）、
  `chaos::compaction_during_query_keeps_counts_monotonic`（30s 内要采到 3 份样本 ⇒ 实测只采到 1 份），
  server 的 `cache_lazy_refresh_e2e`（"节流窗口内不问版本"实际在赌"两次查询间隔 < 200ms"），
  以及 datanode 的 `compaction_e2e::only_one_compactor_..._when_it_dies`（等"过 TTL 后接管"，
  而 GC 租约 TTL **下限就是 30s** ⇒ 上界写 30s 等于必然抖，见 `§172`）。
- **归因（实测，`§171.0`）**：**不是** chaos 跨 binary 饿死 —— cargo **逐个 test binary 串行**跑
  （热态一轮：总墙钟 90s vs 各段用时之和 86.6s）；真因是**同一个 binary 内多条重负载用例并行**。
  ⇒ 原计划"隔离 chaos"那条子项**撤回**（前提不成立）。
- **已修**：判据换成**事件 / 构造性同步**（扇出 = 同时在飞的来源数峰值；压缩与回收 = 按轮驱动到收敛；
  采样覆盖 = watch 广播后"等到"样本；节流窗口 = 显式设定而不是依赖默认 200ms）。
  compaction 段 **30.78s → 0.42s**，chaos 段 **30–73s → 3.15s**（17/17）。
- **而且不会再回来**：新判据 `wall_clock_assertions_need_a_reviewed_waiver` 禁止"拿用时当判据"，
  除非在该处写明理由并带 `§171` 豁免标记。
- **验收**：收口后连续 **3 轮** `cargo test --workspace` 全绿（`exit=0`，每轮 104 段）。
- 历史遗留仍在：`yuntun-chaos` 的恢复上限在 CPU 争抢下曾不足，已放宽到 60s 并写明理由（`§28`）。

---

## 7. 已知限制（**与设计意图不同**的现状，别按设计意图读）

| 项 | 现状 | 依据 |
|---|---|---|
| `best_effort`（默认）RPO | **窗口关闭 + `max_flush_delay_secs` + `flush_phase_spread_secs`**，默认 **≤ 90s**；延迟到达的批次可能再多一个窗口 | `§32`；`ARCHITECTURE.md §5` |
| `durable` RPO | **不是 ≈0**，而是**归档间隔 + 一次上传时延**（默认 `archive_interval_secs = 1` ⇒ 秒级）；粒度是**段**（共段的邻居数据一起归档） | `§125`/`§162` |
| `[compaction]` 与任务表阈值 | 任务目录用的是**超集**阈值（`FILES=2` / `DV_RATIO=0.05` / `DV_MIN_CARD=500`），**执行者**仍按自己的 `[compaction]` 判定 —— 任务只是"候选" | `§169` |
| 存储格式 | 默认 **vortex**（ADR-1 落地）：**体积是 parquet 的 3.5–4.5 倍**，且**读路径无行组级剪枝**（整文件解码）；`.idx` 只对 parquet 有效 | `§164`/`§140`；台账 `D-20` |
| 冷读预算 | 热读有每查询预算，**冷扫描没有上界**（与上面"整文件解码"是同一缺口的两面） | `§88.6`；台账 `D-20` |
| "每窗口每 shard ≤1 文件" | **不是硬不变量**：仅当"窗口内数据量 ≤ `bytes_threshold_mb`"时成立 | `§35` |
| 部署包线（已量过） | `connect_timeout=500ms` / `RPC_TIMEOUT=2s` / raft 选举超时 ~1s ⇒ **对称延迟到 1.5s 就反复选举**；跨可用区 / 跨机前必须按 RTT 重定 | `§113.4` |
| 指标 | 已有 HTTP 导出（默认关）；但**任务目录与租约持有者还没进 `/metrics`** | `§160`；台账 `G.4` |
| 文档体系 | 2026-10-11 整体归档重建（旧文档在 `archive/`）；**归档内容不再维护**，与本文冲突时以本文为准 | `§170` |

---

## 8. 还缺什么（一句话版）

**语义**：`UPDATE` 的 `FROM`/`RETURNING`/无 `WHERE` 形态、`ALTER` 的改类型/可空性/重命名（缺载体）、事务、视图。

**分布式**：**聚合下推的其余形状**（谓词 / `GROUP BY` / `avg` / 浮点 `sum` —— 今天整条原路）、
**冷文件按节点分配**（`S5-2`：需要"读指定文件"的新 RPC；台账 `D-4` 第二半仍未兑现）、
**任务与租约的"关联视图"**（`/metrics` 把"有哪些活"与"谁在做"分两张表报，靠 `task:<表>:<分片>`
前缀人工对应；G.4 的下一格）、按记录过滤归档（`D-18`）。

**工程化**：真云 S3 + 跨机、端口鉴权、冷读预算、WAL segment 清理闸门（`T6.15`）、spill 复用、MSRV 1.95 真编译。

**正确性待查**：`D-23`（合并提交期间观测到一次行数瞬时不可见）。

**测试**：并行全量测试的计时 flaky（§6.1）。

> 逐条的任务化、优先级与验收见 [`ROADMAP.md`](ROADMAP.md)。
