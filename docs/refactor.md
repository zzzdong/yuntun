# Yuntun v2 分布式改造指南（从现状到目标态）

> 目标态定义见：`yuntun-v2-架构设计.md`
> 本文回答：**现有代码怎么改、按什么顺序改、每步怎么验收、哪里能回滚**
> 适用：v2 分支（standalone 已跑通、100+ 测试、WAL 权威/攒批/Parquet+Manifest 已闭环）

---

## 0. 改造原则（先立规矩）

| # | 原则 | 违反后果 |
|---|---|---|
| R1 | **standalone 是分布式的退化情形**，全程保持"1 datanode + 内嵌 metanode"可运行 | 出现两套代码路径，维护成本翻倍 |
| R2 | 任何 crate 内不得出现 `if distributed` 分支，差异只体现在**装配层** | 业务逻辑分叉 |
| R3 | 每个阶段结束都必须保证：**standalone 行为不回归**（现有测试全绿） | 无法定位回归来源 |
| R4 | **先做数据平面（chunk），再做控制平面（raft）** | 进程拆分时状态边界不清，反复返工 |
| R5 | **chaos 与压测前置**，不得跳过 | 分布式竞态无兜底网 |
| R6 | raft 只存指针，不存数据 | 状态机膨胀、snapshot 卡死 |

---

## 1. 现状盘点

### 1.1 已具备（可直接复用，改动小）

| 能力 | 现状 | 改造动作 |
|---|---|---|
| 存算分离 | object_store 抽象（local / s3 / memory） | 基本不动，仅增强缓存层 |
| WAL 权威 | segment + CRC + 组提交 + fsync，崩溃恢复已验证 | 扩展：回收点语义（§S1） |
| Parquet + Manifest | `valid_from` / `deleted_at` + 版本号 | **启用 `deleted_at`**（现状只写在 design 里） |
| schema 演进 | 写入侧 OCC + 追加列 | 迁到 raft（§S3） |
| 幂等去重 | 单进程内存 / 本地 fjall，TTL 24h | 权威迁到 raft（§S3） |
| compaction | `min_files > 5` 触发，`interval 60s` | 升级为全局作业（§S6） |
| SQL 双协议 | MySQL wire + Flight SQL，共用语义层 | 不变 |
| crate 依赖 | 严格单向，域 crate 为纯能力 | 保持，这是最大资产 |

### 1.2 缺失 / 需改造

| # | 缺口 | 影响 | 阶段 |
|---|---|---|---|
| G1 | 无 chunk 层，store 只是对象存储抽象 | 无内存账本、无背压、无 spill | S1 |
| G2 | catalog 访问是 TTL 缓存（30s），非实时非一致 | 分布式下无法保证查询内一致 | S2 |
| G3 | `flush_jitter_secs = 60` 随机抖动 | 持久化上界不可预测 | S2 |
| G4 | 无 metanode，元数据在本地 | 无法多节点共享 | S3 |
| G5 | 无 raft / 无 proto 定义 / 无 tonic-build | 无控制平面 | S3 |
| G6 | 无成员发现与心跳 | datanode 无法感知伙伴 | S4 |
| G7 | 无分布式查询（无 fanout / partial agg） | 无法水平扩展 | S5 |
| G8 | 文件命名未含 node_id | 多节点写同 partition 会覆盖 | S4 |
| G9 | compaction 非全局、无跨节点协调 | 小文件无法收敛 | S6 |
| G10 | chaos 用例存在 flaky（时间敏感） | 分布式竞态无可靠验证 | S0 |

### 1.3 明确不做（避免走回头路）

- ❌ shard 路由 / 归属 / 迁移 / rebalance
- ❌ chunk 信息进 raft
- ❌ 心跳走 raft
- ❌ spill 参与权威判定
- ❌ 独立 ingestor 进程（会导致 INSERT 跨服务转发）
- ❌ queryd（暂不建，触发条件见架构文档 K4）

---

## 2. 阶段路线图

```
S0 收尾 ──► S1 chunk ──► S2 catalog ──► S3 metanode ──► S4 datanode ──► S5 分布查询 ──► S6 compaction
(chaos/压测)  (数据平面)   (访问形态)    (raft/控制面)   (进程/成员)     (fanout/pull)    (全局作业)
   必做       ✅ 已完成    ⇩ standalone 全程可运行
```

| 步骤 | 状态 | 说明 |
|---|---|---|
| S0 前置收尾 | 部分（chaos 11 场景尚未补齐；基线压测未入库） | 与 S1 可并行；**S1 已在未完成 S0 的情况下先行落地**（理由：数据平面是地基，且 S1 的正确性由单测 + 既有回归覆盖，不依赖压测基线） |
| **S1 chunk 层** | ✅ **已完成 2026-09-15** | 落地 crate `yuntun-chunk`；189 tests / 0 failed；对照审查与 5 处回改见 `docs/operation-log.md §25` |
| S1 收尾（残余） | 待办 | S1-11 观测指标、spill 复用、相位分散量级定案（P0，见 `plan.md §2.2`） |
| S2 … S6 | 未开始 | — |

---

## 3. S0：前置收尾（不改架构）

**目标**：把验证网织好，后面每一步才有兜底。

| 任务 | 说明 |
|---|---|
| 修复 flaky | `monitor_aborts_timed_out_batches` 等时间敏感用例，改为事件驱动或放宽窗口 |
| 跑完 chaos 场景 | 原计划 11 个场景补齐 |
| 建立基线压测 | 单节点写入吞吐 / 查询延迟 / 内存曲线，作为后续对比基准 |

**验收**：chaos 连续 N 轮无 flaky；压测基线数据入库。

**⚠️ 不要跳过**。在没经过 chaos 洗礼的系统上引入 raft 与网络分区，故障组合指数级放大。

---

## 4. S1：chunk 层落地（数据平面）

**目标**：把"数据在哪活着"这条边界划清楚。这是所有后续工作的地基。

**为什么必须先做**：进程拆分时，chunk store 跟着 datanode 走；若先拆进程再补 chunk，datanode 会糊成一团，还要返工。

> **状态：✅ 已完成（2026-09-15）**。实现细节、与既有 ADR 的逐条对照、
> 以及审查中回改的 5 处偏差见 `docs/operation-log.md §25`。
> 落地 crate：`crates/chunk`（`yuntun-chunk`）。
> **执行本节的后续改动前，请先读 §4.2 的 7 条陷阱** —— 其中 1/4/5 三条都会让功能测试全绿
> 而架构承诺被静默破坏。

### 4.1 任务清单

| # | 任务 | 要点 |
|---|---|---|
| S1-1 | 新增 `Chunk` / `ChunkStore` | 见架构文档 §2.1；`ChunkData` 两态 |
| S1-2 | 生命周期状态机 | Open → Sealed → Spilled → Flushed → Released |
| S1-3 | seal 策略 | 行数 / 字节 / **窗口关闭** / **schema_version 变化**（时间维度见 §4.2-1） |
| S1-4 | spill 通道 | 本地磁盘 + Arrow IPC(LZ4) + mmap；spill 头记 `(wal_segment, offset, crc)` |
| S1-5 | 内存账本 + 背压阶梯 | 60% / 80% / 95% 三级，见架构文档 §2.7 |
| S1-6 | **内存硬分区** | chunk 区 与 query 执行区互相隔离，query 超限直接失败 |
| S1-7 | scan 接口 | 向 query 暴露未 flush 数据（保住"读己之写"） |
| S1-8 | WAL 回收点语义 | 仅在 flush 成功后截断；spill 不触发回收 |
| S1-9 | chunk 强制 flush 时间阈值 | 防慢写入流导致 WAL 无限膨胀（建议 60s）；**必须同轮 seal + flush**（见 §4.2-4） |
| S1-10 | 阈值调整 | `rows_threshold` 从 10000 提到 50–100 万行量级，让 RowGroup 一次成型 |
| S1-11 | 可观测性 | 内存账本水位 / WAL 积压字节 / 背压水位三项指标（S1-5 的实现前提，见 §4.2-6） |

### 4.2 关键陷阱

1. **时间维度的 seal 必须是"窗口关闭"，不是"创建后 N 秒"**（ADR-10 明文否决后者）。
   写成 `now - created_at >= 5s` 会让低吞吐表（1 条/秒）在一个窗口内产出 ≤12 个小文件，
   正是 ADR-10 要防的"小文件 / Meta 条目爆炸"，而且**功能测试全绿**。
   锚点用**到达分钟**（`window_start_ms(arrival)` + 60s），与旧实现 `window_start + jitter` 同源；
   用事件时间做锚点会在客户端回补历史时退化成"每条一批"。
2. **spill 与 WAL 双真相源**：必须以 WAL 为唯一权威，spill 只是加速副本（架构文档 §2.6）。不写死这条，后面会长出对账类 bug。
   **推论**：spill 目录是节点私有状态，进程重启后残留文件永不被引用 → **启动必须清理**
   （复用优化见架构 §2.6，属后续项）。
3. **WAL 回收点被推后**：chunk 持有越久 WAL 留存越久，所以强制 flush 时间阈值（S1-9）不是可选项。
4. **"强制 seal + flush" 只做一半等于没做**：超 `max_resident` 时只 seal 不 flush，WAL 仍被拖住；
   必须在**同一轮**既 seal 又 flush（调用方顺序 seal → spill → flush 保证可行）。
5. **驻留硬兜底会绕过相位分散**：`flush_due_at` 必须锚定 `sealed_at`（不是 `created_at`），
   且满足不变量 `max_resident > max_flush_delay + phase_spread`；
   否则窗口对齐后"封口即到期" → 所有实例重新在同一秒 flush，ADR-10 惊群复活。
   这类问题**功能全绿**，只能靠不变量 + 启动自检守护。
6. **内存硬分区不能省**：读写同进程，大查询挤掉 chunk 内存是最典型的翻车方式。
   注意 chunk 记账是**无条件**的（可见性优先于预算），真正的硬闸门在 `ingest()` 入口的 95% 拒写，
   中间态由 **WAL 积压**吸收 → 因此 S1-11 的三项指标是必需的，不是可选的。
7. **跨层表标识必须归一**：写入侧若用裸名（`cpu`）、查询侧用全限定名（`public.cpu`），
   热数据永远读不到——`ops.rs` 早已写明"跨层唯一标识"约定。**这个 bug 会被"快速 flush"掩盖**：
   一旦把持久化上界拉长（本次 30s），立即暴露为可见性回归。

### 4.3 验收（✅ 已完成，证据见 `operation-log §25`）

- 写入路径全部经由 chunk，现有测试全绿 ✅（189 passed / 0 failed；clippy 0 警告）
- 读己之写：fsync 后一个扫描周期内可读且**零已提交文件** ✅
- 持久化上界确定（`seal_time + max_flush_delay + phase`，无随机项） ✅
- 窗口对齐：同窗口多批不裂成多 chunk；"创建后 N 秒"不触发 seal ✅
- 内存压力下触发 spill，无 OOM，写入自动降速而非崩溃 —— 阶梯与硬分区单测 ✅；
  **真实压力曲线待阶段 2 压测**（含 S1-11 指标）
- 注入大基数 `GROUP BY`：写入不受影响（验证硬分区有效）—— 单测 ✅；**端到端待阶段 2**
- kill -9 后重启：数据不丢不重 ✅（`m0a_recommit` + chaos）；
  **spill 复用未实现**（当前丢弃重来，正确性不受影响）
- 压测内存曲线不高于基线 —— **待阶段 2**

**回滚点**：chunk 层独立 crate，可 feature flag 控制是否启用写入路径。

---

## 5. S2：catalog 访问形态改造

**目标**：把"TTL 缓存"改成"预取快照 + watch 增量"，接口按远程形态定义（即使此时 metanode 还没独立）。

### 5.1 任务清单

> **✅ 状态（2026-09-17）**：S2-1 ~ S2-7 **已在单进程形态落地**（`operation-log §26`），
> S2-8 留待 R3 之后。落地时**新增一项前置**（下表的 S2-0）：
> 抽象补位必须在 R3 之前做，否则 Compactor 绑具体类型会让 Catalog 转 gRPC 时编译不过。

| # | 任务 | 要点 | 状态 |
|---|---|---|---|
| **S2-0** | **抽象补位**：`commit_compaction` / `known_batch_ids` 上 `CatalogOps`，Compactor 与孤儿清理不再依赖 `MemoryCatalog` | **上 trait 是硬要求**（"同进程"是部署事实，不是类型约束） | ✅ |
| S2-1 | 定义 `CatalogProvider` 抽象 | 同步、无网络读（符合 DataFusion 同步 API 约束） | ✅ |
| S2-2 | 实现 `LocalCatalog` | 直读本地内存，standalone 与 datanode 自用 | ✅ |
| S2-3 | 预留 `CachedCatalog` 接口 | 本地物化 + 按版本失效，S4 后启用 | ✅（即 `LocalCatalog` 的形态） |
| S2-4 | 每查询一次预取 | 构建 immutable 快照（schema + manifest + 节点列表），规划期不再读可变结构 | ✅ |
| S2-5 | **版本号分两组** | schema_ver 与 manifest_ver 分离，否则每次 flush 都让全表 schema 失效 | ✅ |
| S2-6 | watch 后台任务 | 带版本号请求；无变化零开销返回，有变化拉 delta | ✅ |
| S2-7 | manifest delta 接口 | "自 version X 以来的变更"，而非全量 | ✅ |
| S2-8 | 本地缓存持久化 | 落磁盘，重启可用；metanode 不可用时降级服务 | ⏳ R3 后 |

**两条落地时才明确的约束**（写进代码注释与回归用例，避免后续被"优化"掉）：

1. **快照必须真的不可变**：`CatalogSnapshot.tables` 用 `Arc<CachedTable>`，写时复制的代价与
   文件数无关；否则"增量刷新"每次仍要克隆全表文件清单，等于没做。
2. **增量接口必须能报"消失的表"**：删表若只靠 schema_ver 兜底，一旦调用方漏判就会留着过期缓存。
| S2-9 | **flush jitter 重构** | 随机 jitter → 确定性相位偏移：`sealed_at + max_flush_delay + hash(instance,key) % flush_phase_spread`。**机制部分已在 S1 落地**；剩余的是 **spread 量级定案**（5s 会把 ADR-10 的 60s 分散面收窄 12 倍，属 P0 实测决策，见 `plan.md §2.2`）+ **ADR-10 原文正式修订**（`plan.md §2.3-1`） |
| S2-10 | `cache_ttl_secs` 降级为兜底 | 不再作为主要失效手段 |

### 5.2 关键陷阱

- **不要用 TTL 做主要失效手段**：既非实时，也不保证查询内一致。
- **查询内必须固定快照**：否则"扫到一半文件列表变了"。
- **flush jitter 不重构**，前面所有可见性/持久化承诺都是空谈（65s 不可预测）。

### 5.3 验收

- 查询路径 QPS 对比：catalog 网络调用次数从"每调用"降为"每查询一次"（此时尚无网络，用计数器验证本地快照复用）
- DDL 后立即可查（standalone 下）
- 人为让 catalog 源不可用：历史查询仍能返回（降级生效）
- flush 到期时间可预测（打点统计偏差 < 1s）

---

## 6. S3：metanode 独立 + raft

**目标**：把元数据从本地搬到 raft 强一致存储。

### 6.1 任务清单

| # | 任务 | 要点 |
|---|---|---|
| S3-1 | proto 定义 + tonic-build | `crates/proto`：Catalog / Manifest / Membership / Lease 四组 RPC |
| S3-2 | raft 接入 | raft-rs + fjall（现成的元数据库） |
| S3-3 | raft 状态机内容 | schema/DDL/schema_ver、**file manifest**、成员名录、作业租约 |
| S3-4 | `commit_files` 迁到 raft | flush 后提交 manifest |
| S3-5 | **幂等键权威迁到 raft** | 本地表降为快路径预筛；commit 时提交键集合由状态机去重（TTL 24h） |
| S3-6 | schema OCC 迁到 raft | 版本号由 raft 分配 |
| S3-7 | 申请/续约接口 | compaction 作业租约 |
| S3-8 | manifest delta 接口实现 | 供 S2-6 watch 使用 |
| S3-9 | **manifest 上界策略** | 定期 checkpoint 合并 + 归档旧条目（防 snapshot 膨胀） |
| S3-10 | metanode 可独立启动 | 3 节点 raft 组可跑通 |

### 6.2 关键陷阱

- **raft 只存指针**：manifest 存的是文件路径 + 统计 + 版本号，Parquet 本体的读写始终由 datanode 直连对象存储完成，metanode 绝不中转。
- **心跳/存活状态不进 raft**（S4 范畴，但接口设计时要分开）。
- **manifest 上界现在就要定**：等到 snapshot 传不动再补救成本高。

### 6.3 验收

- metanode 3 节点：leader 切换后状态一致
- kill leader：自动选主，提交不丢失
- 幂等去重跨进程生效（两个 datanode 写同一幂等键，只生效一次）
- snapshot 体积有上界，恢复时间可控
- standalone（内嵌单节点 raft）行为不回归

---

## 7. S4：datanode 化 + 成员发现

**目标**：把 standalone 重构成"datanode + 内嵌 metanode"，并让 datanode 能感知伙伴。

### 7.1 任务清单

| # | 任务 | 要点 |
|---|---|---|
| S4-1 | 装配层拆分 | `yuntun` binary = datanode 装配；内嵌 metanode 可选 |
| S4-2 | 成员注册 | datanode 启动向 metanode 注册（写 raft） |
| S4-3 | 心跳保活 | metanode 内存维护存活状态，**秒级，不走 raft** |
| S4-4 | 成员 watch | datanode 从 meta 获取活跃节点列表（复用 S2-6 watch） |
| S4-5 | **文件命名全局唯一** | `{table}/{dt}/{node_id}-{uuid}.parquet` |
| S4-6 | manifest 增加 `source_instance` 字段 | 供 S5 冷热边界切分使用，**此字段必须现在加** |
| S4-7 | chunk 本地化确认 | chunk 完全留在本地，metanode 不感知其存在 |

### 7.2 关键陷阱

- **文件命名不唯一会直接导致数据覆盖**，且是静默的、极难排查。
- **`source_instance` 必须在多节点写入前就加**，事后再加需要回填历史 manifest。
- 心跳走 raft 会把 metanode 写爆（S3-2 设计时就分开）。

### 7.3 验收

- 3 个 datanode 同时写入同一表同一 dt：文件不冲突，无覆盖
- 杀掉一个 datanode：成员列表在预期时间内更新，其余节点无感
- standalone 单进程模式全量测试通过（R1/R3）

---

## 8. S5：分布式并发查询

**目标**：scatter-gather + partial agg 下推 + 热数据 pull。

### 8.1 任务清单

| # | 任务 | 要点 |
|---|---|---|
| S5-1 | 协调者逻辑 | 接到 SQL 的 datanode 充当协调者（不引入独立 frontend） |
| S5-2 | 冷数据按文件分配 | manifest 文件列表均分给各 datanode，各自直读 S3 |
| S5-3 | **partial aggregate 下推** | 各节点算完 count/sum/min/max 再回传，**绝不返回原始行** |
| S5-4 | 热数据 pull 协议 | `pull(table, range, known_manifest_ver)`，范围语义不带 ID 列表 |
| S5-5 | owner 侧 chunk stats 过滤 | 不匹配返回空，避免拉回来再丢弃 |
| S5-6 | **epoch 校验** | 响应带 `flushed_watermark`；落后返回 STALE → 协调者刷新 manifest 重试 |
| S5-7 | release-after-commit 不变量 | `commit_files` 成功后才允许释放 chunk |
| S5-8 | 墓碑期 | 默认 10–60s，内存压力可跳过（不影响正确性） |
| S5-9 | partial response | 默认允许 + `partial: true` 标记 + 缺失来源列表；可配置拒绝 |
| S5-10 | 按实例二维切冷热 | 冷读该实例 ≤watermark 的文件，热 pull (watermark, now] |
| S5-11 | 启用 `deleted_at` | compaction 产出新文件、标记旧文件，墓碑期 + 无在途引用才真正删除 |

### 8.2 关键陷阱

1. **冷热边界不清会导致重复计数**——多实例各自 flush，watermark 不是全局的（架构文档 §4.4）。这是最隐蔽的 bug 来源。
2. **不返回原始行**——多实例 fanout 下网络传输量会吃掉全部收益。
3. **pull 失败要降级而非报错**——退化为只读冷数据 + 标记 partial。注意这与"常态随机跳过"不同：前者只在节点故障窗口发生，后者会让同一查询返回不同结果。

### 8.3 验收

- 3 节点并发查询：结果 = 单节点串行查询（**对拍测试必须有，且要比对精确值**）
- 注入节点故障：返回 `partial: true` 且列出缺失来源，不崩溃
- 重复注入同一查询：结果稳定一致（不出现随机差异）
- 大范围聚合：网络传输量对比原始行方案下降一个量级以上

---

## 9. S6：compaction 全局化

**目标**：compaction 从小文件优化手段升级为唯一合并机制。

| # | 任务 | 要点 |
|---|---|---|
| S6-1 | 作业全局化 | 从单进程后台任务改为 meta 租约独占的全局作业 |
| S6-2 | 租约 + 心跳 + 过期处理 | 防止多节点同时 compaction 同一批文件 |
| S6-3 | 孤儿文件 GC | 租约过期产生的中间文件清理 |
| S6-4 | 跨节点文件合并 | 合并不同 `source_instance` 产出的文件 |
| S6-5 | 与 `deleted_at` 联动 | 墓碑期 + 无在途引用才真正删除 |

**验收**：多节点持续写入下，文件数收敛到稳定区间；compaction 期间查询不受影响；杀掉 compaction 执行者后租约可被接管。

---

## 10. 配置迁移对照

| 现有配置 | 改造后 | 说明 |
|---|---|---|
| `rows_threshold = 10000` | 提升至 50 万行（S1 已落地） | 让 RowGroup 一次成型，减少小文件；**定案值待 P0 实测** |
| `time_threshold_secs = 5` | 保留为**最短驻留地板**（⚠️ 语义已澄清） | **不是 seal 时刻**：seal 由**窗口关闭**决定（ADR-10）。见 §4.2-1 |
| — | **新增 `max_flush_delay_secs`**（默认 30s） | seal → flush 宽限期（**不是**持久化上界本身） |
| `flush_jitter_secs = 60` | **移除随机 jitter**（已落地） | 改为确定性相位偏移 `hash(instance, key) % flush_phase_spread_secs`；⚠️ **spread 量级是 P0 决策**：5s 会把 ADR-10 的 60s 分散面收窄 12 倍 |
| — | **新增 `flush_phase_spread_secs`**（默认 5s） | 相位分散上限；与 `max_flush_delay` / `chunk_max_resident` 存在不变量，见 §4.2-5 |
| `cache_ttl_secs = 30` | 降级为兜底 | 主要失效改由 watch + 版本号驱动 |
| `disk_high_watermark = 0.80` | 保留 | 与背压阶梯 95% 联动 |
| `segment_max_mb = 64` | 保留 | WAL 回收点由 flush 成功决定 |
| `min_files = 5` / `interval_secs = 60` | 保留，重要性上升 | 无归属写入下 compaction 是唯一合并手段 |
| — | **新增 `chunk.mem_budget_mb`**（已落地） | chunk 区内存上限 |
| — | **新增 `chunk.query_mem_budget_mb`**（已落地） | query 执行区上限，**硬隔离**（接 DataFusion 内存池） |
| — | **新增 `chunk.instance_id`**（已落地） | 相位 hash 输入 + `FileManifest.source_instance` |
| — | **新增 `chunk.spill_dir`**（已落地） | 节点私有状态第二处（另见 §4.2-2） |
| `idle_timeout`（5min） | **移除**，由 `chunk_max_resident_secs`（默认 60s）取代 | 后者更强：强制 seal **且** flush |

---

## 11. 测试与验收矩阵

| 阶段 | 单元测试 | 集成测试 | chaos | 压测 |
|---|---|---|---|---|
| S0 | — | — | ⏳ 11 场景待补 | ⏳ 基线待入库 |
| S1 | ✅ 32 项（chunk）+ 配置/映射 | ✅ flush e2e / write_then_read / hot_shard_reader / m0a_recommit | ✅ 既有 3 场景（kill -9 等） | ⏳ **内存曲线待阶段 2**（依赖 S1-11 指标） |
| S2 | ✅ | ✅ | — | ✅ QPS 对比 |
| S3 | ✅ | ✅ | ✅ 杀 leader / 网络分区 | — |
| S4 | ✅ | ✅ | ✅ 节点上下线 | ✅ 多节点写入 |
| S5 | ✅ | ✅ **对拍测试** | ✅ 查询中节点故障 | ✅ fanout 开销 |
| S6 | ✅ | ✅ | ✅ compaction 中故障 | ✅ 文件数收敛 |

**S5 对拍测试是硬要求**：分布式并发查询结果必须与单节点串行结果精确相等，不能只做抽样比对。

---

## 12. 风险登记

| # | 风险 | 等级 | 缓解 |
|---|---|---|---|
| R1 | 冷热边界导致重复计数 | **高** | `source_instance` 提前加（S4-6）+ 对拍测试 |
| R2 | 文件命名冲突导致覆盖 | **高** | S4-5 强制 node_id + uuid |
| R3 | 大查询挤垮写入 | **高** | S1-6 内存硬分区 |
| R4 | raft snapshot 膨胀 | 中 | S3-9 manifest 上界策略 |
| R5 | WAL 无限膨胀 | 中 | S1-9 强制 flush 阈值 |
| R6 | manifest 全量拉取开销 | 中 | S3-8 delta 接口 |
| R7 | 共享存储带宽瓶颈 | 中 | 本地文件缓存（S1 后） |
| R8 | 分布式竞态难复现 | **高** | S0 chaos 前置 + 确定性测试注入 |
| R9 | **驻留硬兜底静默绕过相位分散**（flush 重新聚集在同一秒 → ADR-10 惊群复活） | 中 | S1 已修：`flush_due_at` 锚定 `sealed_at` + 不变量自检（§4.2-5）；**改阈值前必查** |
| R10 | **跨层表标识不一致**（写入侧裸名 vs 查询侧全限定名）导致热数据读不到，且被"快速 flush"掩盖 | 中 | S1 已修：ingest 边界归一（`ops.rs` 约定）；新增写入路径须复核（§4.2-7） |
| R11 | **配置语义漂移**（默认值/语义改了但文档与运维认知未同步） | 中 | 示例配置单测守护 + 启动自检 + `plan.md §2.3` 文档同步清单 |
| R12 | chunk 记账无条件导致内存短时越限（靠 WAL 积压吸收） | 中 | 入口 95% 拒写 + S1-11 三项指标观测；阶段 2 压测确认积压峰值有界 |

---

## 13. 待核对项（需按仓库实际代码确认）

以下为基于现有文档与讨论的推断，落地前需逐条与实际代码核对：

| # | 待核对 | 说明 |
|---|---|---|
| V1 | 各依赖实际版本（DataFusion / Arrow / object_store / raft-rs / fjall） | ✅ 已核：arrow 59.3 / datafusion 55.0 / object_store 0.13.2（`operation-log §2.1`） |
| V2 | 幂等键当前存储介质 | ✅ 已核：`MemoryCatalog` 内存（独立于 FileManifest，§7.3.1 语义已实现） |
| V3 | `flush_jitter_secs` 当前生效路径 | ✅ 已核**并已移除**（S1 落地）：旧实现为 `window_start + hash(shard+table) % jitter_secs`；现为确定性相位偏移（`operation-log §25.3-1/8`） |
| V4 | Manifest 现有字段 | ✅ 已核；并已按架构 §2.3/§4.4 增补 `partition_key` / `source_instance` |
| V5 | `crates/proto` 现有内容 | ✅ 已核：为空占位（元数据/WAL 消息手写在 `yuntun-model`，阶段 3 起用 tonic-build） |
| V6 | compaction 当前是否已有租约机制 | ✅ 已核：无租约（单进程后台任务），S6 需从零建 |
| V7 | 本地 catalog 缓存是否可序列化到磁盘 | ✅ 已核：`LocalCatalogCache` 为纯内存，无可序列化形态，S2-8 需新增 |
| V8 | **热数据读侧接缝是否已可替换** | ✅ 已核并**保持**：`store::ShardReader` + `ShardFetch`（`operation-log §21.3`/`§25.2`），实现现在由 `yuntun-chunk::ChunkStore` 提供 |
| V9 | **表标识（裸名 / 全限定名）在各层是否一致** | ⚠️ 曾是缺陷（写入侧裸名 vs 查询侧全限定名），S1 已归一（`operation-log §25.3-2`）；**新增写入路径时须继续遵守 `ops.rs` 约定** |

---

## 14. 工作量粗估（单人）

| 阶段 | 估时 | 说明 |
|---|---|---|
| S0 | 1–2 周 | chaos + 基线压测 |
| S1 | 3–4 周 | chunk 层，最核心 |
| S2 | 2 周 | catalog 改造 |
| S3 | 3–4 周 | raft + metanode |
| S4 | 2 周 | 进程拆分 + 成员发现 |
| S5 | 3–4 周 | 分布式查询 |
| S6 | 2 周 | compaction 全局化 |
| **合计** | **约 4–5 个月** | S1/S3/S5 为关键路径 |

> S1 / S3 / S5 是关键路径，任一延期会线性后推整体。建议 S1 完成后重新评估。
>
> **✅ 已重估（2026-09-16，S1 完成后）**：见 `plan.md §8.5`。
> 结论：剩余 ≈3.5–5 个月（总量持平），但 **S4/S6 各上调 1 周**
> ——因为"冷热边界按实例"（S4-6 的消费侧）与"孤儿 GC 多写者安全"（S6-3）
> 实为**新增语义**而非网络化改造（`plan.md §5.1-C`/`§5.3`/风险 R-8）。
> 另：S1 的估时里包含"真实压力曲线 / 观测指标 / spill 复用"，这三项**未完成**，
> 已挪到阶段 2（T6.12 / T6.14 / P2）。
