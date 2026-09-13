# DML delta 设计草案：DELETE / UPDATE（0.2 规划）

> **版本演进**
> - **v1**（2026-09-13）：初版设计——DV 单形态收敛、DELETE 只作用于已提交快照（强制 flush 取舍）、
>   merge-on-read 落在 TableProvider scan 层、compaction 消费 DV、WAL 重放幂等。
> - **v2 相对 v1**（2026-09-13，依外部评审一；6 P0 详见 §0.1）：M0 恢复改造前置项、
>   DV 锚定文件路径、DV 事件表快照维度、UPDATE 单条记录、裁剪/下推取舍明确定性、
>   position-resolving scan、删除可见性时点、P1 九项。
> - **v3 相对 v2**（2026-09-13，依外部评审二；5 P0 详见 §0.2）：M0 终态区间集合跳过、
>   重提交补 table/epoch 来源、跨 shard DELETE 收敛单条记录（单物理 WAL）、
>   补 DELETE × compaction 并发处理、UpdatePayload 入账路径打通、P1 六项。
> - **v4 相对 v3**（2026-09-13，依外部评审三；3 P0 详见 §0.3）：S 包含 Abort 区间、
>   `replay_wal_dml` 四段启动顺序、表级 lease 替代 CAS+重试、S 计算时点与传递、
>   M0 断言修正、统计用合并位图基数、`upd_id` 回退语义、全表删 Catalog 能力。
> - **v5 相对 v4**（2026-09-13，依外部评审四；3 P0 详见 §0.4）：`wal_seq_end` 精确化
>   （交错写入下 `first_seq + len` 有洞）、M0 重提交世代闸门、读路径逐文件规划、
>   lease 粒度 (table, shard)、重提交不追加 WAL、P1 四项。
> - **v6 相对 v5**（2026-09-13，依外部评审五；2 P0 详见 §0.5）：
>   1. **S 从"整数区间并集"改为"组键 + 半开区间"二维判定**（R15，P0）—— v5 的包围盒
>      区间会把**别的组**落在组内空洞里的未成批 Data 判为"已覆盖"而跳过
>      （从重复数据变成丢数据）。`claimed(Data) ⟺ ∃ 终态/Abort 批次 b：
>      b.group_key == key(Data) ∧ b.start ≤ seq < b.end`；group_key =
>      (table, shard, window, **epoch**)——epoch 进键是为了区分 DROP/重建前后
>      同名同组的批次
>   2. **半开区间口径改造列出消费点清单**（R16，P0）—— `flush_batch`（写）、
>      `resume_recovered` 的 `scan_range(s, e+1)`（改 `(s, e)`）、
>      `segment_batches`（区间相交判定）、S 判定——四处同步改，漏一处即边界错误
>   3. **P1 修订**：无 DV 文件合并为单个原生 child（防计划膨胀）、老 WAL 无 `table`
>      字段的升级回退（s3_paths 反解）、多 shard lease 按名排序一次性获取（防死锁）、
>      `new_batch.client_request_id = upd_id`（重放去重）、segment 清理的
>      "未认领 Data 防误删"闸门、M0 拆 M0a/M0b

> **状态**：草案 v6（待评审定稿后进 plan.md 排期）。
> 参考实现：delta-rs deletion vectors（#1094 completed）、Iceberg V2/V3 position delete/DV。
> DataFusion 引擎层无原生支持（55 源码零命中），合并逻辑属 TableProvider 职责。

## 0. 评审修订记录

### 0.1 评审一（v1 → v2）

| # | 问题 | 处置 | 落点 |
|---|---|---|---|
| P0-1 | UPDATE 两条 WAL 记录无原子性；随机幂等键 | UPDATE 合并单条记录；确定性派生键 | §4.2 |
| P0-2 | 恢复重放导致 batch_id 全变，DV 锚断裂 | M0 恢复语义改造（前置项） | §1.1 |
| P0-3 | `FileEntry` 可变字段泄漏到所有快照 | DV 独立事件表（快照号维度） | §3.2 |
| P0-4 | 内存分片交棒窗口已删行仍可见 | 删除可见性 = 提交返回时同步刷缓存 | §5.2 |
| P0-5 | "DV 与裁剪/下推正交"错误；batch 多文件行号歧义 | 锚定文件路径；M1 禁用裁剪/下推 | §5.1 |
| P0-6 | "定位命中行"无机制 | position-resolving scan（M1 PoC） | §5.3 |

### 0.2 评审二（v2 → v3）

| # | 问题 | 处置 | 落点 |
|---|---|---|---|
| N1 | M0 单水位线跳过"已 fsync 未成批"的 Data | 终态区间逐区间跳过（v6 进一步二维化） | §1.1 |
| N2 | M0 重提交缺 `table`/`epoch` | payload 增补 `table`；epoch 时间线推导 | §1.1 |
| N3 | 单物理 WAL 与"每逻辑 shard 一条"不符 | 单条 `DeletePayload` 携带全部删除 | §3.3/§4.1 |
| N4 | DELETE × compaction：重写吞删除 → 复活 | lease 串行化 | §6.1 |
| N5 | `UpdatePayload.new_batch` 无人消费 | 入账路径打通；原子性表述修正 | §4.2 |
| P1-a~f, P1-g | 刷新计数/DV 读 API/force_flush 触达/扫描前提/读放大/全表删/行号引用 | 逐条落地 | §5.2/§5.4/§4.3/§5.3/§2/§7/全文 |

### 0.3 评审三（v3 → v4）

| # | 问题 | 处置 | 落点 |
|---|---|---|---|
| R1 | Abort 区间丢失 → 显式放弃的数据复活 | S 从 WAL 流重建，保留 abort 区间（v6 升级为组键+区间） | §1.1 |
| R2 | 无 DML 重放；重启后 DV 全丢 | `replay_wal_dml` + 四段启动顺序 | §1.1/§4.1 |
| R3 | CAS+重试两洞（多删/丢删除） | lease 串行化；CAS 降级为断言 | §6.1 |
| R4 | S 计算时点与传递 | resume_recovered 之后计算并回传注入 | §1.1 |
| R5 | M0 断言"无新增文件"有条件不成立 | 断言改为"已终态批次零新文件" | §8 |
| R6 | 统计 Σcard 破坏上界 | 合并位图基数（per-file） | §5.4 |
| R7 | `upd_id` 无幂等键时无来源 | 回退 `upd-<uuid>`，承诺仅有键时成立 | §4.2 |
| P2 | 全表删能力/增量刷新/幂等再灌入/dv 前缀 | 逐条落地 | §7/§5.2/§1.1/§6.2 |

### 0.4 评审四（v4 → v5）

| # | 问题 | 处置 | 落点 |
|---|---|---|---|
| R8 | `wal_seq_end = first_seq + len` 交错写入下不等于 last+1 | `*group.seqs.last() + 1`（v6 再升级为二维判定） | §1.1 |
| R9 | M0 重提交无世代闸门 → DROP/重建复活 | `liveness_at` 双点校验 | §1.1 |
| R10 | 读路径无法按文件应用 DV | 逐文件规划（v6 细化：无 DV 文件合并单 child） | §5.4 |
| R11 | lease 表级粒度过粗 | (table, shard) 粒度 | §6.1 |
| R12 | 重提交追加 WAL → 线性膨胀 | 只重建内存 Catalog，不追加 WAL | §1.1 |
| R13 | 四段顺序 ④ 理由不准确 | 改"DV 必须在查询可见前重建" | §1.1 |
| R14 | `purge_table_files` 悬挂 DV | 同步 revoke | §7 |
| P2 | 统计逐文件/replay 单快照/全表删 lease | 逐条落地 | §5.4/§4.1/§7 |

### 0.5 评审五（v5 → v6）

| # | 问题（纯逻辑推演即可证实） | 处置 | 落点 |
|---|---|---|---|
| R15 | S 的包围盒区间把**别的组**在组内空洞里的未成批 Data 判为已覆盖 → 跳过 → **丢数**（R8 修法的副作用：under-cover 修成 over-cover） | S 改为**二维判定**：`claimed(Data) ⟺ ∃ 终态/Abort 批次 b：b.group_key == (table, shard, window, epoch(Data)) ∧ b.start ≤ seq < b.end`；空洞里的他组/同组不同 epoch 的 Data 均不被认领 → 重放 | §1.1 |
| R16 | 半开化有多个消费点，只改 `flush_batch` 会留 off-by-one | 列出**四处消费点清单**：`flush_batch`（写）、`resume_recovered` 的 `scan_range(s, e+1)`→`(s, e)`、`segment_batches`（相交判定）、S 判定 | §1.1 |
| R17 | 逐文件规划若对无 DV 文件也逐文件 → 计划随文件数膨胀 | **无 DV 文件合并为单个原生 `DataSourceExec`**（维持 FileGroup）；计划规模 ≈ O(带 DV 文件数 + 1) | §5.4 |
| R18 | 老 WAL segment 解出的 `BatchPendingPayload.table` 为空 → 重提交/闸门无法工作 | 恢复 **s3_paths 路径反解回退**，触发条件 `table.is_empty()` | §1.1 |
| R19 | 多 shard DELETE 需多 lease，增量获取顺序未定义 → 死锁 | 按 shard 名**排序后一次性获取**；或跨 shard DML 退化为表级锁 | §6.1 |
| R20 | UPDATE 重放的 `new_batch` 若崩溃前已 flush → 重放产生第二个 batch_id 的同数据 | **`new_batch.client_request_id = upd_id`**，靠 `client_request_id` 唯一索引去重（接受孤儿文件） | §4.2 |
| R21 | `segment_batches` 对"含未成批 Data 的 segment"判定为可清理（无关联批次）→ 数据在 M0 恢复前即被物理删除 | recovery 扫描时维护**未认领 Data seq 清单**（按 segment 分组）；segment 清理加闸门：**含未认领 Data 的 segment 不可清理** | §6.2 |

## 1. 目标、非目标与前置项

### 1.1 M0 前置项：恢复语义改造

**现状**（`resume_recovered` 内 `Committed | S3Written` 分支）：这两类批次恢复时**不重提交**
（注释：Meta 不持久 C5，可见性由攒批全量重读 WAL 重做 flush 提供），而攒批从 0 重放 Data
并**以新随机 UUIDv7 重新 flush**——即**每次重启全部历史数据被重写一遍**。

**改造（六项）**：

1. **`wal_seq_end` 精确化（评审四 R8）**：`flush_batch` 改为
   **`wal_seq_end = *group.seqs.last() + 1`**（`WindowGroup.seqs` 已持有真实 seq）。
   ⚠️ 既有缺陷（交错写入下 Pending 重做 `scan_range(s, e+1)` 少读尾部），M0 必修；
2. **半开区间口径改造清单（评审五 R16）**——以下**四处消费点同步修改**，漏一处即
   仅在边界触发的错误：
   - `flush_batch`：写入侧（第 1 条）；
   - `resume_recovered` Pending 分支：`scan_range(s, e + 1)` → **`scan_range(s, e)`**；
   - `segment_batches`：区间相交判定 `s <= hi && e >= lo` → 半开 `s < hi && e > lo`
     （并复核语义）；
   - S 判定（§1.1 第 4 条）；
3. **重提交**：`resume_recovered` 对 `Committed | S3Written` 批次，**复用原 batch_id 与
   既有对象**重提交，**不追加 WAL**（评审四 R12：原 `BatchCommitted` 记录已表达终态，
   新函数 `recommit_into_catalog` 只重建内存 Catalog；与 `commit_recovered_batch`
   的区别必须保留）。`table` 来源：`BatchPendingPayload` **增补 `table` 字段**（prost
   追加 tag，向后兼容）；**升级回退（评审五 R18）**：老 segment 解出的 `table` 为空时，
   **从 `s3_paths[0]` 反解**（`yuntun/<schema>/<table>/...`），触发条件 `table.is_empty()`；
   **epoch 来源与闸门（评审四 R9）**：`liveness_at(wal_seq_range 起点)` vs
   `liveness_at(EOF)` 校验——`alive && epoch 一致`才重提交，否则跳过并告警；
   幂等记录再灌入保留（受既有 TTL 约束）；
4. **攒批跳过判定 S（评审五 R15 修正：二维判定，非整数区间并集）**：

   ```
   claimed(Data d) ⟺ ∃ 终态/Abort 批次 b：
       b.group_key == group_key(d)  且  b.start ≤ seq(d) < b.end
   group_key = (table, shard, window, epoch)     ← epoch 由 DDL 时间线按 seq 推导
   ```

   - **为什么必须二维**：v5 的包围盒 `[min, max+1)` 会把**别的组**落在组内空洞里的
     未成批 Data 判为已覆盖而跳过（丢数）——组键过滤后，包围盒只收敛到该组自身，
     既无 over-cover 也无 under-cover。epoch 进键是为了区分 DROP/重建前后
     同名同组的批次；
   - **abort 区间保留（评审三 R1 维持）**：`apply_record` 对 `BatchAbort` 把该批次的
     `(group_key, [start, end))` 移入 `aborted_ranges`（组键在 BatchPending 时可得，
     abort 时仍能取到）；
   - 攒批重放：`claimed(d)` 为真的 Data 跳过，为假的一律重放入账。**"空洞重放"的
     准确表述：空洞里的 Data 若组键不匹配任何终态批次则重放**（v5 的整数区间并集
     表述作废）；
5. **S 的计算时点与传递（评审三 R4 维持）**：在 `resume_recovered` **之后**计算，
   由其回传注入 accumulator；
6. **四段启动顺序（评审三 R2；④ 理由按评审四 R13 修正）**：
   ```
   ① replay_wal_ddl        （重建表清单，既有）
   ② resume_recovered      （批次重提交/重做 → 文件与快照就位；回传 S 与
                             未认领 Data 清单，见 §6.2）
   ③ replay_wal_dml        （新增：按 seq 顺序重建 DeletionEntry——从 WAL 内联位图
                             重建，dv_id 幂等去重，store_path 按 §3.1 公式回推，
                             世代校验失败 → abort；一条 DELETE 的全部 entries
                             分配同一 applied_at（单快照批量 apply）；
                             **失败（如 store 不可达）→ 启动中止**，
                             不得带着"缺失 DV 的 Catalog"对外服务）
   ④ spawn_accumulator     （携带 S 与未认领清单，跳过已认领区间）
   ```
   ③ 必须在 ② 之后（对空 Manifest 应用 DV 无意义）；④ 必须在 ③ 之后——
   **理由：DV 必须在数据对查询可见之前重建**。

### 1.2 目标（0.2）

- `DELETE FROM t [WHERE ...]`：写廉价（不重写基文件），读侧 merge-on-read；
- `UPDATE t SET ... [WHERE ...]`：单条原子 WAL 记录；**可见性不原子**（删即时、
  插经攒批，文档化边界）；
- delta 成本有界：compaction 消费 DV（收敛机制，取舍见 §2）；崩溃恢复确定性
  （DV 由 WAL 重放重建，幂等）。

### 1.3 非目标

- 主键 / 唯一性约束、并发 DML 冲突检测（OCC）；equality delete 形态；
  MVCC 多版本读；plan 级 anti-join（t2 路线）；多物理 WAL 事务标记（阶段 1）。

## 2. 形态决策：收敛到"行位删除向量（DV）"单形态

| 方案 | 定位方式 | 读成本 | 决策 |
|---|---|---|---|
| Copy-on-write | 谓词 | 查询零成本 | 备选快速路径，非主形态（写放大不可控） |
| **DV（行位）** | `(file_path, row_idx)` | O(活跃 DV 数) | ✅ 主形态 |
| equality delete | 等值键 | anti-join | ❌ 需主键；DataFusion 优化器黑盒风险 |

**读放大的明确取舍**：M1 的 DV 为**追加式**（同文件多次 DELETE 追加多个
`DeletionEntry`，不合并）——读放大 **O(活跃 DV 数)**，由两条机制闭合：① compaction
按 DV 占比阈值**独立触发**消费（§6）；② 阈值兜底前，上界 = 自上次 compaction 以来的
DELETE 次数（已知取舍，文档化）。"每文件单活跃 DV + RMW 合并"列 M2。

**核心取舍不变：DELETE 只作用于已提交快照**——命中未提交（内存分片 Live）批次时，
先同步强制 flush（§4.3），再对已提交状态生成 DV。UPDATE 同理。

## 3. 存储布局

### 3.1 DV 文件

```
yuntun/<schema>/<table>/dt=.../shard=.../dv/<数据文件名含扩展名>/<dv_id>.bin
```

- `roaring` crate 序列化（格式带版本头）；内容 = 该**数据文件**内被删行号；
- `card == 0` 不落盘；`数据文件名` 用完整文件名（含扩展名），与孤儿清理
  `extract_batch_id` 按文件名取 id 的口径对齐；
- `dv/` 前缀**纳入** `list_objects` 对账范围（`spawn_orphan_cleanup` 按 `dv/` 显式
  分流，见 §6.2）。

### 3.2 DV 事件表（快照维度）

**不**在 `FileManifest` 上加可变字段。DV 作为独立事件，生命周期用**快照号**表达：

```rust
pub struct DeletionEntry {
    pub dv_id: String,          // 幂等 id（= WAL DELETE/UPDATE 记录 id）
    pub file_path: String,      // 锚定的数据文件
    pub batch_id: String,       // 归属批次（清理对账用）
    pub applied_at: u64,        // 快照号：query_snapshot >= applied_at 才应用
    pub revoked_at: u64,        // 0 = 生效中；compaction 消费后置为快照号
    pub card: u32,
    pub store_path: String,
}
```

- 行可见性 = 文件可见 **AND** 行号 ∉ {applied_at ≤ query_snapshot < revoked_at 的 DV 并集}；
- 旧快照读者自动看到未删除状态（快照隔离成立）；
- **多次 DELETE 同一文件**：追加新 `DeletionEntry`（取舍见 §2）。

### 3.3 Catalog 能力补充

- **`apply_deletions(entries: Vec<DeletionEntry>) -> SnapshotId`**：本次 DELETE 的全部
  DV 单次快照原子提交（单快照批量 `applied_at`）；
- **MVP 事实**：单物理 WAL（`WalWriter::open(cfg, 0)`，`shard_key` 是逻辑分片）——
  一次 DELETE **只有一条 `DeletePayload`**（跨逻辑 shard 合并），天然原子。
  阶段 1 多物理 WAL 需事务标记（TxnBegin/TxnCommit），超出本文范围；
- **`purge_table_files(table)`**：全表删所需的"下线某表全部可见文件"能力
  （或枚举 shard 逐个 `drop_shard`）；同步 revoke 悬挂 DV（§7）。

## 4. WAL 记录

### 4.1 DELETE（单条，跨逻辑 shard 合并）

```rust
pub struct DeletePayload {
    pub table: String,
    pub dv_id: String,          // 幂等 id
    pub deletions: Vec<FileDeletion>,  // { file_path, bitmap(roaring+版本头) }
    pub schema_epoch: u64,      // 表世代：与 MemoryShard::liveness 同源；
                                // 重放校验失败 → abort（补专项测试）
}
```

- 执行流程（持所涉 (table, shard) lease，§6.1）：解析谓词 → position-resolving scan
  （§5.3）→ 命中未提交批次先强制 flush（§4.3）→ DV 写对象存储 → WAL append + fsync
  （单条）→ `apply_deletions` 单次快照提交 → 显式刷新受影响表缓存（§5.2）；
- 恢复：`replay_wal_dml`（§1.1 之 ③）从内联位图重建 `DeletionEntry`（dv_id 幂等去重，
  单快照批量 apply）；世代校验失败 → abort。

### 4.2 UPDATE（评审二 N5 / 评审三 R7 / 评审五 R20）

```rust
pub struct UpdatePayload {
    pub table: String,
    pub upd_id: String,         // 幂等键 = "upd-<client_request_id>"（提供键时，确定性）；
                                // 未提供键 → "upd-<uuid>" 回退，幂等承诺仅在提供键时成立
                                // （与 INSERT 的 "dml-<uuid>" 回退语义对齐）
    pub deletions: Vec<FileDeletion>,
    pub new_batch: DataPayload, // 新行；**new_batch.client_request_id = upd_id**
                                // （评审五 R20：重放去重——崩溃前新行已 flush 时，
                                // 重放经 client_request_id 唯一索引去重，
                                // 接受产生一个孤儿文件，由孤儿清理回收）
}
```

- **入账路径**：攒批循环与 `apply_record` 对 `Record::Update(up)` = 应用 `up.deletions`
  （与 Delete 同路径）+ 把 `up.new_batch` 交给既有 `Record::Data` 入账分支——
  杜绝"只删不插"路径（补专项回归）；
- **原子性表述**：WAL 原子（单条记录，恢复重放不会半程）；**可见性不原子**
  （删即时、插经攒批）——明确边界，不做隐含承诺；
- 跨逻辑 shard：单物理 WAL 下单条记录成立；恢复按 WAL seq 全序收敛。

### 4.3 强制 flush 的装配

- `Ingestor::force_flush(table, shard) -> Result<SnapshotId>`；
- 触达点是**攒批 accumulator**（含原始 payload；内存分片 `HotBatch` 无 payload）——
  命令通道（mpsc）或共享句柄，攒批循环下一扫描周期优先 drain 指定 (table, shard)
  并同步返回快照号；
- SQL 层经 `SqlEngine` → `Ingestor` 调用。

## 5. 查询执行（merge-on-read）

### 5.1 与 parquet 裁剪/下推的取舍（维持）

DV 过滤依赖"到达过滤层的行序 == 文件原始行序"。**M1：带 DV 的文件禁用 row-group
裁剪、不下推谓词**（`supports_filters_pushdown` 维持 `Inexact`）；**M2+**：DV →
`RowSelection` 在 FileOpener 层合并应用。

### 5.2 删除可见性时点（实现要求，非既有能力）

- **DELETE 提交路径显式调用受影响表的 `cache.refresh()`**（同步，返回前）；
- M1 补**按表增量刷新入口**（全量 `refresh` 重载所有表 Manifest+DV，高频小 DELETE
  下开销与删除次数成正比）；
- 验收项："删刚插的行 → 立即不可见"回归覆盖此路径。

### 5.3 position-resolving scan（M1 首个 PoC）

- 逐文件包装：每文件一个 child `DataSourceExec`，外包 `ProvenanceExec`：输出附加
  `__file_id`（常量）与 `__row_idx`（batch 内累计偏移）；
- **隐性前提（构建侧保证）**：每文件单 partition（禁按 row-group 拆 child）；
  child 不带 filters/limit；满足时 batch 行序 == 文件行序；
- 谓词求值：`ProvenanceExec → FilterExec(谓词)` → collect 命中行 → 聚合为 DV；
- 只服务 DML 定位，普通查询计划不含这两列。

### 5.4 DV 的读路径与统计（评审四 R10 / 评审五 R17 / 评审三 R6）

**逐文件规划，与 §5.3 的 provenance 扫描同构、复用同一 per-file 构建器**：

- **无 DV 文件**：**合并为单个原生 `DataSourceExec`**（维持 FileGroup——评审五 R17：
  若逐文件则 N 文件 → N 个 child 的 `UnionExec`，计划膨胀回归；合并后计划规模
  ≈ O(带 DV 文件数 + 1)）；
- **带 DV 文件**：逐文件 child 外包 per-file DV 应用节点（`__row_idx` → bitmap 判定 →
  `filter_record_batch`/`take`）；
- **不得**把带 DV 文件混入无 DV 的 FileGroup 后在"计划上层"应用 DV——batch 无来源
  标识，且无法表达逐文件策略（R10 根因）；
- 统计：**逐文件** `row_count − or_card`（同文件多 DV 先 OR 再计）后求和；
- DV 装载：经 `list_deletions(table, snapshot)`（`applied_at ≤ snapshot < revoked_at`）；
  缓存按 `dv_id` 内容寻址。

## 6. Compaction：delta 成本的收敛机制

### 6.1 DELETE × compaction 并发（lease 串行化，粒度 (table, shard)）

- **粒度 = `(table, shard)`**（评审四 R11：compaction 工作单元是 `compact_shard`，
  分钟级任务按表互斥会阻塞该表 DML 数分钟）；DML 的"定位 → DV → apply"全程持
  所涉 shard 的 lease；compaction 重写同一 shard 时同样持 lease；
- **多 shard DML 的 lease 获取（评审五 R19）**：按 shard 名**排序后一次性获取**
  （全或无），避免两个并发 DML 以不同顺序增量获取而死锁；跨 shard DML 也可退化为
  表级锁（实现择一，文档化选择）；
- 原 CAS 校验**降级为断言**（lease 失效等异常路径 fail-loud，而非静默复活）；
- 方向性：先 apply 后 rewrite 安全（rewrite 消费 DV）；先 rewrite 后 apply 危险，
  被 lease 排除。

### 6.2 触发、对账与未认领 Data 防误删（评审五 R21）

- 触发与 `min_files` **解耦**：`DV 占比 > 阈值`（建议 10%）**即可单文件触发**，加下界
  （`card ≥ dv_min_card`（建议 1000）或 `row_count ≥ 地板`）避免抖动；
- 重写 = 读 base ⊖ DV → 新文件 → 同一快照内原子：新文件上线 + 基块 `deleted_at` +
  DV `revoked_at`（全程持该 shard lease）；
- ⚠️ 行号随重写漂移——DV 必须在重写时**一并消费**；
- **segment 清理闸门（R21）**：`segment_batches` 只按 BatchState 关联判定——
  **含未成批 Data 的 segment 没有关联批次，会被判为"全部关联批次已终态"而整段清理**，
  数据在 M0 恢复前即物理丢失。修法：恢复扫描（§1.1 ②）顺带维护
  **未认领 Data seq 清单**（按 segment 分组——replay 本就逐条过 WAL，增量成本为零）；
  `spawn_orphan_cleanup` 的 segment 清理加闸门：**segment 含未认领 Data seq 则不可
  清理**（直至其被攒批入账并提交）；
- 孤儿清理：`list_objects` 覆盖 `dv/` 前缀并**显式分流**，对账键 = `batch_id`
  （`dv→file_path→batch_id` 推导）；不得把 `dv_id` 误当 batch_id；
  `drop_table/drop_shard` 时 DV 随文件对账下线。

## 7. 一致性与边界

| 场景 | 行为 |
|---|---|
| DELETE 命中未提交行 | 强制 flush 后删除（§4.3）；可见性 = 提交返回（§5.2） |
| **DELETE × compaction 并发** | **(table, shard) lease 串行化**（§6.1）；CAS 降级为断言 |
| 多 shard DELETE | 单条 `DeletePayload`；lease 按 shard 名排序一次性获取（§6.1） |
| 崩溃于"DV 已写、WAL 未提交" | 孤儿 DV，孤儿清理回收 |
| 崩溃于"WAL 已提交、快照未落" | `replay_wal_dml` 重放（DV 幂等）→ 收敛 |
| 表被 DROP / 同名重建 | M0 重提交世代闸门（§1.1 R9）+ DML 重放世代校验 abort——旧世代数据不挂新表 |
| `WHERE` 命中全部行 | 统一走 DV，由 compaction 收敛（行级口径） |
| 无 WHERE（全表删） | **先 force-flush** → **`purge_table_files`** 文件级下线 + 同步 revoke 悬挂
  DV（R14）+ 持该表全部 shard 的 lease；不生成 DV——文件级与行级分属两层，非第二形态 |
| UPDATE 可见性 | WAL 原子；可见性不原子（删即时、插经攒批）——明确边界 |
| abort 批次 / 未成批 Data | S 二维判定（§1.1）：abort 不复活、他组空洞不丢失（评审五 R15 专项） |
| 含未成批 Data 的 segment | 清理闸门拦截（§6.2 R21）——恢复前不物理丢失 |

## 8. 里程碑与工作量（评审五 P2：M0 拆两批）

| 里程碑 | 内容 | 量级 |
|---|---|---|
| **M0a** | `wal_seq_end` 精确化 + 半开口径四处消费点 + `resume_recovered` 重提交（payload 增补 table、epoch 闸门、s3_paths 反解回退、不追加 WAL） | 中（可独立验证：重启零新文件 + 不复活） |
| **M0b** | S 二维判定（组键 + 半开区间，含 abort 区间保留）+ accumulator 注入 + `replay_wal_dml` 四段顺序 + segment 清理闸门 | 大（风险最高，独立暴露） |
| M1 | DV 存储/事件表 + `apply_deletions`（lease 内）+ `DELETE`（provenance scan PoC → 全量）+ **逐文件 DV 应用（无 DV 合并单 child）** + force_flush + lease + 增量刷新 | 一个完整迭代；PoC 先行 |
| M2 | compaction 消费 DV（解耦触发 + 下界 + 原子重写）；DV→RowSelection；单活跃 DV RMW | 中 |
| M3 | `UPDATE`（UpdatePayload 入账 + `new_batch.client_request_id = upd_id` 去重 + 幂等键语义） | 小 |
| M4（远期） | 主键 + equality/anti-join；Iceberg V3 DV 兼容；多物理 WAL 事务标记 | 视需求 |

**M0 验收断言**：同一 WAL/store 目录上"写入 N 行拿到 ack → 硬崩溃 → 重建"后：
① `count(*) == acked 行数`；② **已终态批次零新增数据文件**（仅 Pending 批次被重做；
或测试显式等待全部 commit 后再崩）；③ 恢复耗时随历史长度次线性；④ **abort 过的批次
数据不复活**；⑤ **交错写入两类用例**（R15 扩展）：(a) 同组 seq 空洞 → 不翻倍；
(b) **空洞里是另一组的未成批 Data → 不丢失**；⑥ **DROP→同名重建→重启，旧世代数据
不挂新表**；⑦ 含未成批 Data 的 segment 不被清理（R21 专项）。

**M1 验收测试矩阵**：DV × compaction 交错（**(table, shard) lease 竞争注入**）、
DV × 崩溃恢复（各步骤 kill -9，含 `replay_wal_dml` 中途）、DV × 快照隔离、
DV × 表世代（含 M0 ⑥）、DV × 读己之写（强制 flush + 增量刷新路径）、多轮 DELETE
累积后 compaction 收敛（chaos 滚动）、DELETE 重放 epoch 校验失败 abort 专项、
`UpdatePayload` 入账与重放去重专项（R20）、abort/他组空洞复活防护专项、
**逐文件 DV 应用正确性**（同表混有无 DV 文件）。

## 9. 依赖与杂项（P2）

- `roaring` 版本与序列化格式版本头；`card == 0` 不落盘；
- 位图应用优先 `take`/RowSelection 形态（M2 与 RowSelection 一起做）；
- 文中"§21 世代机制"指 `docs/operation-log.md` 第 21 节；
- 引用一律函数名（`resume_recovered` / `commit_recovered_batch` / `recommit_into_catalog` /
  `apply_record` / `spawn_orphan_cleanup` / `replay_wal_ddl` / `replay_wal_dml` /
  `flush_batch` / `segment_batches`），不用行号。
