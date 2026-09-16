# Yuntun v2 分布式架构设计（目标态）

> 状态：已决策 / 待实现
> 关联：`docs/architecture.md`、`docs/design.md`、`docs/plan.md`、`chunk-store-design.md`
> 决策日期：2026-09-14

---

## 0. 四条已定决策

| # | 决策 | 说明 |
|---|---|---|
| D1 | 分离 **datanode** 与 **metanode** | 暂不建 queryd |
| D2 | datanode 从 metanode 的 raft 感知伙伴，查询走分布式并发 | raft 管成员名录，不是心跳 |
| D3 | datanode 同时承担 ingest 与 query | 读写同进程，INSERT 无需跨服务转发 |
| D4 | **不做 shard** | 无路由、无归属、无迁移、无 rebalance |

---

## 1. 角色划分

### 1.1 拓扑

```
                ┌─────────────────────────────┐
                │  metanode × 3 (raft)        │  控制平面
                │  schema / manifest / 成员    │
                │  / 作业租约                  │
                └──────────────┬──────────────┘
                               │ raft + watch
        ┌──────────┬───────────┼───────────┬──────────┐
        ▼          ▼           ▼           ▼          ▼
   datanode-A  datanode-B  datanode-C   ...      数据平面
   ingest+query  ingest+query  ingest+query
        │           │           │
        └───────────┴───────────┴──────► 对象存储（S3/MinIO）
                                          Parquet + Manifest（共享）
        └─────────── pull chunk (范围语义) ──────┘
```

- **metanode**：有状态，raft 组，纯元数据服务，**永不中转数据**
- **datanode**：有状态，WAL + 内存 chunk + 本地缓存；直接读写对象存储；可对外提供 SQL（MySQL wire / Flight SQL）
- **compactor**：全局作业，通过 meta 租约独占文件批次；可作为 datanode 内后台任务或独立进程（后续决定）
- **standalone**：**1 个 datanode + 内嵌 metanode**，同一套代码，不是两套

> 不变量：**standalone 是分布式的退化情形**。任何 crate 不得出现 `if standalone { ... } else { ... }` 的分支，差异只在装配层。

### 1.2 为什么不做 shard 也能分布式

存算分离白送的三条性质：

1. **数据本体在共享存储**，任何 datanode 都能扫全量已 flush 的 Parquet，无需归属
2. **并行单位是文件**，不是分片位置 —— 协调者按文件列表分配给各节点
3. **多节点写同一 partition 不冲突**，各自生成各自的文件，靠 compaction 合并

由此消掉的是整套 shard 管理体系：路由、归属、lease、迁移、rebalance。**迁移不搬数据**，是这套架构最大的红利。

---

## 2. Chunk 层（数据平面核心）

### 2.1 定义

**chunk ≡ 尚未成型的 RowGroup**：内存中是一组 `RecordBatch`，落 S3 后成为 Parquet 中的一个 RowGroup。

```rust
struct Chunk {
    id: ChunkId,
    table: TableId,
    partition_key: (Dt, /* 预留 */),
    schema_version: u32,
    state: ChunkState,        // Open | Sealed | Spilled | Flushed
    rows: usize,
    bytes: usize,             // 内存账本
    stats: ColumnStats,       // min/max，chunk 级跳过
    data: ChunkData,
}

enum ChunkData {
    Mem(Vec<RecordBatch>),    // Open / Sealed
    Spill(SpillHandle),       // Arrow IPC (LZ4)，可 mmap
}
```

- **内存不用 RowGroup 布局**：DoPut 入口本就是 Arrow 批次，转换是纯亏的复制；RowGroup 512MB 量级也不适合做内存管理单元
- **落盘时用 Arrow IPC 而非 Parquet**：spill 若用 Parquet，落盘 encode、flush 读回 decode、写 S3 再 encode，编码付三次；IPC 落盘/读回近乎零成本，flush 只付一次必需的编码
- **spill 后仍可查**，可见性承诺不破

### 2.2 状态机

```
Open --seal--> Sealed --spill--> Spilled --+
  |              |                          | flush → commit_files
  +--------------+--------------------------+        → Flushed → Released
```

seal 触发：行数 / 字节数 / 时间 / **schema_version 变化**（文件内同 schema 的硬约束）

### 2.3 层级

| 层 | 定义 | 作用 |
|---|---|---|
| table | 逻辑表 | 用户可见 |
| partition | 分区键 `dt`（**逻辑**） | 裁剪、按 dt 整体删除 |
| file | 物理 Parquet 文件 | Manifest 登记单位 |
| chunk | 文件内的 RowGroup | 内存/磁盘缓冲、跳过单元 |

> **partition 是逻辑身份，file 是物理身份，不得等同**。compaction 会合并文件，若两者等同则每次合并后 partition 集合都变化。做法：`FileManifest` 增加 `partition_key` 字段。

### 2.4 schema 约束

- **文件内同一 schema，跨文件允许不同 schema_version**（沿用现有 design：commit_files 不校验 schema version）
- 推论：**schema 演进粒度 = 文件**。加列强 seal 当前文件、开新文件，老文件保持老 schema，查询侧由 SchemaAdapter 兜底
- 副作用：频繁加列 → 小文件增多（写入文档）

### 2.5 存储分层与访问方式

| 层 | 位置 | 访问方式 |
|---|---|---|
| 内存 chunk | 进程内存 | 直接持有 RecordBatch |
| spill chunk | **本地磁盘（节点私有）** | mmap + Arrow IPC |
| 已 flush 文件 | 对象存储（共享） | range read + 列/RowGroup 裁剪 |
| 文件读缓存 | 本地磁盘 | 拉成本地副本后可 mmap |

> **mmap 只属于本地层，不得用于 S3**（依赖 page cache 与本地 fd）。推论：**spill 必须走本地磁盘**——卸载内存压力走网络更慢，且它是节点私有状态，与 WAL 同级。

### 2.6 WAL 与 spill：单一真相源

> **WAL 是唯一真相源，spill 只是可丢弃的加速副本。**

- 启动恢复以 WAL 重放为准重建 chunk
- spill 文件头记录 `(wal_segment, offset_range, crc)`，重放时校验一致则复用，否则丢弃重来
- **禁止**让 spill 参与权威判定

### 2.7 背压阶梯

| 水位 | 动作 |
|---|---|
| chunk 内存 60% | 后台 spill 最老的 sealed chunk |
| chunk 内存 80% | 强制 seal open chunk 并 spill |
| chunk 内存 95% 或磁盘达 watermark | DoPut 返回 `RESOURCE_EXHAUSTED` + `retry-after` |

### 2.8 ⚠️ 内存硬分区（读写同进程的头号风险）

datanode 内同时跑写入与查询，必须把内存预算切成**互不抢占**的两块：

| 区域 | 超限行为 |
|---|---|
| **chunk 区** | spill → flush → 背压拒写，**必须保底** |
| **query 执行区** | 超限直接返回错误（agg/sort 临时内存），**绝不抢占 chunk 区** |

不做硬分区，一个大基数 `GROUP BY` 就能把 chunk 内存挤光、把写入压垮。

---

## 3. metanode：raft 里存什么

### 3.1 职责边界

> **raft 只存指针，不存数据。**

| 进 raft（低频、强一致） | 不进 raft |
|---|---|
| schema / DDL / schema 版本 | chunk 数据（datanode 内存） |
| file manifest（flush 后提交） | chunk 位置（无需存储，见 §4.3） |
| **成员名录**（节点注册/注销） | 节点存活状态（metanode 内存 + 心跳） |
| compaction 作业租约 | 内存水位、在途查询（旁路 metrics） |

### 3.2 成员发现分两层（D2 的精确含义）

| 层 | 内容 | 机制 | 频率 |
|---|---|---|---|
| **成员名录** | 有哪些 datanode、ID 与地址 | **raft** | 节点上下线才变 |
| **存活状态** | 谁还活着 | metanode 内存 + 心跳 | 秒级 |

**心跳不得走 raft**——秒级心跳会把 raft 写爆。datanode 启动时向 metanode 注册，之后心跳保活；连续超时则由 metanode 从成员表摘除（走 raft）。

### 3.3 manifest 上界（隐患登记）

全量 manifest 常驻 raft 状态机内存，文件数随写入持续累积，snapshot 膨胀会拖慢传输与恢复。**必须尽早定上界策略**（定期 checkpoint 合并 + 归档旧条目）。

---

## 4. 查询路径

### 4.1 两条路径

| 路径 | 数据源 | fanout | 陈旧度 |
|---|---|---|---|
| **默认** | 对象存储已 flush Parquet | 按文件分配给各 datanode | ≤ flush 周期 |
| **read-latest** | + pull 其他 datanode 的 chunk | fanout 到全部 datanode | ~100ms |

**默认路径是主要优化目标**：共享存储使任意节点都能独立扫全量冷数据，无需协调，覆盖绝大多数分析型查询。

### 4.2 分布式并发查询（scatter-gather）

接到 SQL 的 datanode 充当协调者：

```
1. 解析 SQL → 表 + 分区裁剪
2. 从 meta 拿：manifest 文件列表 + 活跃 datanode 列表
3. 冷数据：文件列表均分给各 datanode，各自直读 S3
4. 热数据（read-latest）：fanout pull → 所有 datanode
5. 各节点返回 partial aggregate（不是原始行）
6. 协调者 merge → 返回
```

三条必须遵守：

- **下推 partial aggregate**：各节点算完 count/sum/min/max 再回传。返回原始行的网络传输量会吃掉全部收益
- **查询内固定快照**：查询开始时锁定 manifest 版本，执行期间不变，避免"扫到一半文件列表变了"
- **partial response 默认允许**：节点失败时返回可用结果 + `partial: true` + 缺失来源列表。监控场景下"90% 数据 + 明确标记"远好过整体报错（可配置拒绝）

### 4.3 热数据 pull：范围语义，不带 ID 列表

```
协调者 → datanode-X: pull(table, range, known_manifest_ver)
datanode-X 侧:
  ├─ 用本地 chunk stats 过滤（不匹配返回空）
  ├─ 比较本地 flushed_watermark 与 known_manifest_ver
  │    ├─ 落后 → 返回 STALE + 新版本号，协调者刷新后重试
  │    └─ 正常 → 返回 chunk 的 partial aggregate
  └─ 失败/超时 → 协调者退化为只读冷数据，标记 partial
```

**协调者不需要预知对方有哪些 chunk**——由对方自己回答。这正是"chunk 位置不进 raft"成立的原因：它是可推导信息，无需存储。

### 4.4 冷热边界：为什么必须标记 source_instance

多个 datanode 各自 flush 各自的 chunk，"已 flush 到哪"是**每实例各自的版本**，不是全局的。若不区分：

- 冷读全部 manifest + 热 pull 全部 → **同一批数据被读两次**
- 按全局 min(watermark) 切 → 某个慢实例会拖冷热边界

做法：manifest 中每个文件记 `source_instance`；pull 响应带该实例的 `flushed_watermark`；协调者按实例二维切分（冷读该实例 ≤watermark 的文件，热 pull (watermark, now]）。**此契约必须在实现前定死**，否则多实例后会冒出重复计数这类极难排查的 bug。

### 4.5 不变量：数据不会"两头都没有"

竞态场景：查询方拿 manifest V（不含 F1），owner flush C1 → commit 到 V2 → 释放 C1，此时 pull 已拿不到、manifest V 也读不到 F1。

> **不变量**：任何已 seal 的数据，要么在 datanode 的 chunk 中可 pull，**要么**在 manifest 中可 read，不会两边都不在。

实现：**`commit_files` 成功、manifest 版本推进后，才允许释放对应 chunk**。

配套 epoch 校验：pull 请求带 `known_manifest_ver`，owner 比较本地 `flushed_watermark`，落后则返回 STALE 让协调者刷新重试。重试幂等且不重复读（manifest 覆盖 `[0, V]`，chunk 严格覆盖 `(V, now]`）。

### 4.6 墓碑期是优化，不是正确性依赖

| 机制 | 是否正确性依赖 |
|---|---|
| commit 后才释放（不变量） | ✅ 是 |
| epoch 校验 + 重试 | ✅ 是 |
| 墓碑期（commit 后再保留 10–60s） | ❌ 否，仅减少重试 |

内存压力时可**直接跳过墓碑期释放**，正确性不受影响，代价只是触发一次刷新重试。

> 同一套机制适用于 compaction：产出新文件、标记旧文件 `deleted_at`，等墓碑期 + 无在途引用才真正删除。**现有 `deleted_at` 字段应正式启用**，而不是只写在 design 里。

---

## 5. 写入路径

```
任意 datanode 收到写入（无路由）
  → 校验 / schema 解析
  → WAL 追加（fsync / 组提交）
  → 内存 chunk（此时本地可查）
  → seal → flush → 写对象存储
  → commit_files（raft 提交 manifest + 幂等键去重）
  → 释放 chunk（遵守 §4.5 不变量）
```

### 5.1 无归属写入的代价与要求

多个 datanode 可能同时写同一 partition，各自出各自的文件：

- **文件名必须全局唯一**：`{table}/{dt}/{node_id}-{uuid}.parquet`，否则会互相覆盖
- **compaction 从小文件优化手段升级为唯一合并机制**，重要性上升一档
- 幂等去重的权威在 **raft 状态机**（commit_files 时），datanode 本地幂等表只是快路径预筛

### 5.2 双阈值（必须分离）

| 阈值 | 语义 | 绑定约束 | 建议值 |
|---|---|---|---|
| **可见性上界** | WAL fsync → chunk 可查 | 用户承诺 | **~100ms**（当前 `scan_interval_ms` 量级） |
| **持久化上界** | flush 到对象存储 | WAL 回收 + 文件数 | 10–30s，可放宽到分钟级 |

**两者是独立约束，不得合成一个**。可见性绑 WAL，持久化绑 WAL 回收与文件数。

### 5.3 ⚠️ flush jitter 必须重构

现有 `flush_jitter_secs = 60`（ADR-10 防惊群）意味着 flush 延迟在 `time_threshold_secs(5) + jitter(60)` ≈ 65s 内不可预测，会污染持久化上界。

改为**确定性相位偏移**：

```
flush_deadline = seal_time + max_flush_delay              // 确定
actual_flush   = flush_deadline + hash(instance) % 5s     // 相位分散，防惊群
```

到期时间确定、各实例仍分散、且可预测。

### 5.4 慢写入流保护

WAL 只能在数据真正落对象存储后截断。chunk 持有越久，WAL 留存越久、重放越慢。**必须为 chunk 增加强制 seal + flush 的最大时间阈值**，不能只看行数/字节，否则慢写入流会让 WAL 无限膨胀（`segment_max_mb` 与 `disk_high_watermark` 都只是事后兜底）。

---

## 6. catalog 访问

### 6.1 形态：每查询一次预取 + 本地物化

> 不是"查询零网络调用"，也不是"每次 catalog 调用走网络"，而是**每查询一次预取，之后本地物化**。

```
查询到达（异步阶段）
  ├─ 解析出涉及的表
  ├─ 一次性向 meta 拉取：schema + manifest + 活跃节点列表
  ├─ 构建 immutable 快照
  └─ 交给 DataFusion → 后续同步调用全在本地
```

QPS 从"每 catalog 调用"降到"每查询一次"，差两个数量级。且**优于现有 TTL 缓存**：TTL 既非实时（30s 陈旧），也不保证查询内一致；预取快照两者兼得。

### 6.2 watch 协议（后台常驻）

```
datanode ↔ metanode 保持 watch
  请求带 (schema_ver, manifest_ver)
    ├─ 无变化 → 零开销返回
    └─ 有变化 → 拉增量 delta，更新本地缓存
查询时直接读本地缓存，零网络
  └─ 除非显式 read-latest，才同步拉一次
```

三个必须遵守：

1. **版本号分两组**：schema 版本（极低频）与 manifest 版本（每次 flush/compaction 都变）分开维护，否则任何一次 flush 都会让全表 schema 缓存失效
2. **manifest 走增量**：提供"自 version X 以来的 delta"接口，而非全量快照
3. **查询内固定快照**（同 §4.2）

### 6.3 可用性降级（保留 ADR-6 原意）

- 本地缓存**持久化到磁盘**，重启后可用
- metanode 不可用时用 last-known-good 快照继续服务，**降级而非拒绝**
- 此时 DDL / schema 变更失败，但历史数据查询照常

### 6.4 唯一躲不掉的代价

DDL 跨节点可见性：CREATE 后在另一节点立即查可能查不到。DDL 走 raft 写 + 完成后广播失效，承诺 100ms 内收敛并写入 README §4。

---

## 7. 已知约束

| # | 约束 | 说明 |
|---|---|---|
| K1 | **热数据 fanout 规模上限** | 发给全部 datanode，3–10 台无问题；再大需裁剪策略 |
| K2 | 共享存储带宽成为瓶颈 | 所有节点从 S3 读，靠本地文件缓存缓解 |
| K3 | 小文件更多 | 多节点各写各的，compaction 压力上升，它是唯一合并手段 |
| K4 | 读写不能独立扩缩 | 需要时再加 queryd（本文不建，触发条件：查询负载明显挤占写入） |
| K5 | datanode 有状态 | 扩缩容需处理 chunk/WAL，但**不搬已 flush 数据** |

---

## 8. 待决策项

| # | 事项 | 建议 |
|---|---|---|
| P1 | compactor 独立进程 or datanode 内后台任务 | 先内嵌，独立化待触发 |
| P2 | 持久化上界具体值 | 先 30s，实测后按文件数调整 |
| P3 | chunk 强制 flush 时间阈值 | 防 WAL 膨胀，建议 60s |
| P4 | manifest 上界策略 | 定期 checkpoint 合并 + 归档 |
| P5 | 路由键 / partition 粒度 | 无 shard 下按 dt 分区即可，高基数场景再评估 |
| P6 | partial response 默认允许还是拒绝 | 建议默认允许 + 可配置拒绝 |
