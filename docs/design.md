# 通用直写数据湖 — 详细设计文档

> **依据**：《通用直写数据湖架构设计 v11》（终审通过）
> **版本**：v1.0
> **日期**：2026-08-31
> **定位**：架构设计 → 工程实现的桥梁。定义**模块边界、接口契约、数据结构、状态机、错误码、配置项**，可直接指导编码。
>
> **阅读对象**：实施工程师（Rust）、测试工程师
> **与架构文档的关系**：本文不重复论证"为什么"，只定义"是什么"与"怎么做"。所有设计决策的论证见架构文档对应 ADR / 章节。

---

## 目录

1. [范围与实现目标](#一范围与实现目标)
2. [Cargo Workspace 与模块划分](#二cargo-workspace-与模块划分)
3. [核心 Trait 契约](#三核心-trait-契约)
4. [WAL 模块详细设计](#四wal-模块详细设计)
5. [Ingestor 详细设计](#五ingestor-详细设计)
6. [Meta 服务详细设计](#六meta-服务详细设计)
7. [Query 与 DataFusion 集成](#七query-与-datafusion-集成)
8. [Schema 演进实现](#八schema-演进实现)
9. [后台作业](#九后台作业)
10. [错误码与重试策略](#十错误码与重试策略)
11. [配置项清单](#十一配置项清单)
12. [测试策略](#十二测试策略)

---

## 一、范围与实现目标

### 1.1 本期范围（阶段 0 + 0.5）

| 包含 | 不包含 |
|---|---|
| 单进程 All-in-One 可运行系统 | 分布式多节点部署（阶段 1+） |
| Arrow Flight 写入（唯一 Source） | InfluxDB / Kafka Source（阶段 2+） |
| 自实现 WAL（segment + CRC + 组提交） | Raft 共识（阶段 1+） |
| 内存 Meta（按 Raft 语义抽象） | fjall / Raft Storage（阶段 1+） |
| Vortex 主格式 + Parquet 回退开关 | 行级 UPDATE/DELETE |
| Schema 演进（加列/宽化 + OCC） | Time Travel |
| 快照隔离 + 分片级移除 | 外部倒排索引（阶段 2+） |
| 幂等键 + TTL + 表模板 | 热温冷分层 |

### 1.2 关键设计约束（实现时不得违背）

以下 8 条来自架构文档，是**编译期契约**，违反将导致系统正确性或性能失效：

| # | 约束 | 违反后果 |
|---|---|---|
| C1 | `batch_id` 必须是**随机 UUIDv7**，不得改为内容哈希 | 幂等机制失效（ADR-4） |
| C2 | 攒批线程**只读 `< synced_offset`** 的 WAL 数据 | 断电后状态机错乱（§5.3.5.1） |
| C3 | CRC 覆盖 `type + payload`，**不覆盖 length** | 撕裂写入无法检测（§5.3.3） |
| C4 | `BatchAbort` 属于**终态** | segment 永不释放 → 磁盘写满（§5.3.6.1） |
| C5 | Catalog 仅存**内存**，不独立落盘 | 双写一致性陷阱（§5.4.2） |
| C6 | Schema 适配必须 `SchemaAdapter` + `PhysicalExprAdapter` **两层都用** | 谓词无法下推 → 全表扫描（§8.4） |
| C7 | 文件定位由 **Meta Manifest 驱动**，不用 `ListingTable` | 与 ADR-6 矛盾，性能退化（§8.5） |
| C8 | `EvolveSchema` OCC **仅作用于演进**，不作用于 `CommitFiles` | 合法写入被无谓拒绝（§6.7） |

---

## 二、Cargo Workspace 与模块划分

### 2.1 Workspace 结构

```
lakehouse/
├── Cargo.toml                 # workspace 根
├── crates/
│   ├── proto/                 # Protobuf 定义（Meta gRPC、WAL record）
│   ├── model/                 # 核心数据模型（无 IO，无网络）
│   ├── wal/                   # 自实现 WAL（segment + CRC + 组提交）
│   ├── store/                 # 对象存储抽象（S3 / 本地 / Mock）
│   ├── format/                # Vortex / Parquet 读写封装 + Feature Flag
│   ├── catalog/               # Catalog 纯逻辑（表/文件/快照/删除/幂等）
│   ├── ingest/                # 写入路径（Source → WAL → 攒批 → S3 → Meta）
│   ├── query/                 # DataFusion 桥接（CatalogProvider/TableProvider/Adapter）
│   ├── compaction/            # 后台作业（合并、孤儿清理、TTL、超时监控）
│   └── server/                # 服务框架（gRPC/Flight/HTTP、健康检查、指标）
├── bins/
│   └── all-in-one.rs          # 阶段 0 唯一二进制
└── tests/
    ├── integration/           # 端到端
    └── chaos/                 # 阶段 0.5 故障注入
```

### 2.2 依赖方向（严格单向，禁止循环）

```
        server
       ╱  │  │  ╲
 ingest  query  compaction  (可独立二进制)
       ╲  │  │  ╱
      catalog ──→ store, format
         │
        wal ──→ model
         │
       model   (最底层，零依赖)
```

**规则**：
- `model` 不依赖任何 crate
- `catalog` 是纯逻辑，**不含网络**（阶段 0 进程内调用，阶段 1 包一层 gRPC）
- `wal` 不依赖 `catalog`（WAL 只管字节流与 Record）

### 2.3 关键依赖（Cargo.toml）

```toml
[workspace.dependencies]
# 核心
arrow           = "55"
arrow-flight    = "55"
datafusion      = "55"
parquet         = "55"
object_store    = "0.11"

# Vortex —— 必须锁 Git Commit（ADR-1）
vortex-datafusion = { git = "https://github.com/vortex-data/vortex.git", rev = "<锁定 commit>" }

# 运行时
tokio           = { version = "1", features = ["full"] }
tonic           = "0.12"
prost           = "0.13"

# 工具
uuid            = { version = "1", features = ["v7"] }
crc32fast       = "1"
thiserror       = "2"
tracing         = "0.1"

# 阶段 1 才引入
# raft-rs  = "0.7"
# fjall    = "3.1.8"
```

> ⚠️ **Vortex 锁定 Git Commit Hash**（非版本号）。同时实现 `FormatSwitch` Feature Flag，可运行时切回 Parquet（见 §5.5）。

---

## 三、核心 Trait 契约

> 本节是**编译期契约**。任何修改需同步更新架构文档。

### 3.1 摄入 Source（ADR-13）

```rust
// crates/ingest/src/source.rs

#[async_trait]
pub trait IngestSource: Send + Sync + 'static {
    /// 协议标识，用于日志与指标
    fn name(&self) -> &'static str;

    /// 启动服务，持续产出归一化的 IngestBatch
    ///
    /// # 契约
    /// - `tx` 背压由下游控制，Source 不得无限缓冲
    /// - `shutdown` 触发后应在 `grace_period` 内退出
    /// - **shard_key 由 Source 负责提取**（各协议逻辑不同）
    async fn run(
        &self,
        tx: mpsc::Sender<IngestBatch>,
        shutdown: CancellationToken,
    ) -> Result<()>;
}

/// 归一化写入单元 —— 下游完全不感知协议差异
#[derive(Debug)]
pub struct IngestBatch {
    pub table: String,
    pub shard_key: String,          // 由 Source 提取
    pub record_batch: RecordBatch,  // 自带 schema
    pub idempotency_key: Option<String>,
    pub received_at: SystemTime,
}
```

**shard_key 提取规则**（各协议不同，故由 Source 负责）：

| Source | 提取方式 |
|---|---|
| Arrow Flight（MVP） | 客户端在 Flight descriptor / metadata 中指定，默认取首个分区列 |
| InfluxDB（阶段 2+） | 从 tags 中提取（如 `host`） |
| Kafka（阶段 2+） | 从消息 key 提取 |

### 3.2 WAL 读写

```rust
// crates/wal/src/lib.rs

/// WAL 写入器（每 shard 一个实例）
pub struct WalWriter {
    shard: String,
    current: SegmentWriter,
    synced_offset: Arc<AtomicU64>,   // §5.3.5.1 水位线
    config: WalConfig,
}

impl WalWriter {
    /// 追加一条记录，返回确认句柄
    ///
    /// # 持久性语义（关键）
    /// 返回的 `WalAck` 必须**等待组提交 fsync 完成**后才 resolve。
    /// 调用方在 await 完成后，方可向客户端返回写入成功。
    pub async fn append(&self, rec: Record) -> Result<WalAck>;

    /// 推进水位（内部调用，fsync 成功后）
    fn on_fsync_complete(&self, new_offset: u64) {
        self.synced_offset.store(new_offset, Ordering::SeqCst);
    }

    /// 当前已 fsync 的水位
    pub fn synced_offset(&self) -> u64 {
        self.synced_offset.load(Ordering::SeqCst)
    }
}

/// WAL 读取器（攒批线程 / 恢复线程使用）
pub struct WalReader {
    shard: String,
}

impl WalReader {
    /// 顺序扫描 `[from, to)` 区间的记录
    ///
    /// # 契约（C2）
    /// 攒批线程传入的 `to` **必须** <= `synced_offset`。
    /// 遇到 CRC 校验失败立即停止（撕裂写入边界）。
    pub fn scan_range(
        &self,
        from: u64,
        to: u64,
    ) -> Result<impl Iterator<Item = Record>>;
}
```

### 3.3 Catalog 操作（阶段 0 进程内，阶段 1 包 gRPC）

```rust
// crates/catalog/src/ops.rs

#[async_trait]
pub trait CatalogOps: Send + Sync {
    // ---- 表 / Schema ----
    async fn create_table(&self, req: CreateTableRequest) -> Result<TableMeta>;
    async fn get_table(&self, name: &str) -> Result<Option<TableMeta>>;

    /// Schema 演进（OCC）—— 唯一的乐观锁作用点（C8）
    async fn evolve_schema(&self, req: EvolveSchemaRequest)
        -> Result<EvolveSchemaResponse>;

    // ---- 文件 ----
    /// 提交文件清单（幂等，按 batch_id 去重）
    async fn commit_files(&self, req: CommitFilesRequest)
        -> Result<CommitFilesResponse>;

    /// 查询某表在某快照下的可见文件（Manifest 驱动，C7）
    async fn list_visible_files(
        &self,
        table: &str,
        snapshot: u64,
        shard_filter: Option<&str>,
    ) -> Result<Vec<FileManifest>>;

    // ---- 删除 ----
    async fn drop_shard(&self, table: &str, shard: &str) -> Result<u64>;

    // ---- 幂等 ----
    async fn check_idempotency(&self, key: &str)
        -> Result<Option<String>>;  // Some(batch_id) = 已存在
    async fn record_idempotency(&self, rec: IdempotencyRecord) -> Result<()>;
}

// 阶段 1：同一 trait，gRPC 实现
// pub struct GrpcCatalogClient { ... }
// impl CatalogOps for GrpcCatalogClient { ... }
```

**关键**：阶段 0 用 `MemoryCatalog`，阶段 1 用 `GrpcCatalogClient`，**业务代码零修改**（ADR 演进平滑性）。

### 3.4 DataFusion 桥接

```rust
// crates/query/src/catalog.rs

/// CatalogProvider —— 注意：这些方法都是**同步**签名
impl CatalogProvider for LakeCatalogProvider {
    fn schema_names(&self) -> Vec<String>;
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>>;
    // ⚠️ 内部禁止发起 gRPC！必须读本地缓存（ADR-6）
}

impl SchemaProvider for LakeSchemaProvider {
    fn table_names(&self) -> Vec<String>;
    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>>;
}

// crates/query/src/table.rs
impl TableProvider for LakeTableProvider {
    fn schema(&self) -> SchemaRef;

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&[usize]>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
    // ⚠️ 规划期调用，必须轻量：不做 IO、不开连接

    fn supports_filters_pushdown(
        &self, filters: &[&Expr]
    ) -> Result<Vec<TableProviderFilterPushDown>>;

    fn statistics(&self) -> Option<Statistics>;
}
```

---

## 四、WAL 模块详细设计

> 对应架构 §5.3。这是阶段 0 最硬的骨头，也是**崩溃恢复正确性的根基**。

### 4.1 文件布局

```
{wal_dir}/
  shard={shard_id}/
    {seq:020}.wal        # segment，20 位定宽十进制序号
    CURRENT              # 内容为当前活跃 segment 文件名
    CURRENT.tmp          # 切换时的临时文件（切换完成即被 rename 走）
```

**Shard 级独立**：每 shard 一个目录，避免锁竞争与清理相互阻塞。

### 4.2 二进制格式

```
┌─────────────────────────────────────────────────────┐
│ FileHeader (22 bytes)                               │
│   magic     : 4B   = 0x4C414B45 ("LAKE")            │
│   version   : 2B   = 1                              │
│   shard_id  : 8B   = u64                            │
│   first_seq : 8B   = 本 segment 首条记录的全局 seq   │
├─────────────────────────────────────────────────────┤
│ Record × N                                          │
│   ┌───────────────────────────────────────────────┐ │
│   │ length : u32  = payload 字节数                 │ │
│   │ crc32  : u32  = crc32(type || payload)         │ │
│   │ type   : u8                                    │ │
│   │ payload: [u8; length]                          │ │
│   └───────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
```

**C3 关键点**：`crc32` **不覆盖 `length`**。读取时先读 `length`（作为"信任锚点"定位记录边界），再读 `crc32 + type + payload` 校验。若 length 本身损坏，会读到非法长度而失败——同样安全。

### 4.3 Record 类型与 Payload

| type | 名称 | Payload 结构 |
|---|---|---|
| 0 | `Data` | Arrow IPC 序列化的 RecordBatch |
| 1 | `BatchPending` | `{batch_id: String, shard: String, window: String, wal_seq_range: (u64,u64), schema_version: u64, client_request_id: Option<String>}` |
| 2 | `BatchS3Written` | `{batch_id: String, s3_paths: Vec<String>, s3_upload_id: Option<String>}` |
| 3 | `BatchCommitted` | `{batch_id: String}` |
| 4 | `BatchAbort` | `{batch_id: String}` |

**序列化**：建议 `bincode` 或 `prost`（与 proto 共用）。选 `prost` 便于阶段 2 跨语言。

### 4.4 组提交与 `synced_offset`

```rust
struct GroupCommitter {
    pending: Vec<(Record, oneshot::Sender<WalAck>)>,
    window: Duration,      // 默认 1ms
    max_batch: usize,      // 默认 1024 条
}

async fn commit_loop(...) {
    loop {
        // 收集窗口内的所有请求
        let batch = collect_until(window, max_batch).await;

        // 一次性 write + fsync
        writer.write_all(&encoded)?;
        writer.sync_all()?;          // fsync

        let new_offset = writer.position();

        // 【关键顺序】先推进水位，再 ack 客户端
        wal.on_fsync_complete(new_offset);
        for (_, ack_tx) in batch {
            let _ = ack_tx.send(WalAck { offset: new_offset });
        }
    }
}
```

**顺序不可颠倒**（§5.3.5.1）：水位推进必须在 `fsync()` 之后、客户端 ack 之前。保证：

> 客户端确认 = 已 fsync = 攒批线程可见

### 4.5 崩溃恢复流程

```
recover(shard):
  ① 读取 CURRENT（若损坏/不存在 → 取目录内最大 seq 的 segment）
  ② 从 last_committed_offset 开始顺序扫描所有 segment
  ③ for record in replay:
       - 读 length → 读 crc32+type+payload
       - 校验 CRC
         ├─ 失败 → 【停止】，当前位置即 synced_offset
         └─ 成功 → 应用记录（见 §4.6 状态机）
  ④ 重建 BatchState 表与攒批缓冲
  ⑤ 对每个非终态 BatchState，按状态分流处理（见 §5.6）
```

**★ `synced_offset` 无需持久化**：CRC 校验边界 = fsync 边界，由 CRC 自然确定（架构 §5.3.5.1）。这是 C3 设计的额外收益。

### 4.6 状态机重建

```rust
enum BatchStatus { Pending, S3Written, Committed, Abort }

fn apply_record(state: &mut BatchStateMap, rec: Record) {
    match rec {
        Record::Data { batch } =>
            buffer.push(batch),                    // 累积到 (shard, window)

        Record::BatchPending { batch_id, .. } =>
            state.insert(batch_id, BatchState {
                status: Pending, ..
            }),

        Record::BatchS3Written { batch_id, s3_paths, upload_id } =>
            state.get_mut(batch_id).map(|s| {
                s.status = S3Written;
                s.s3_paths = s3_paths;
                s.s3_upload_id = upload_id;
            }),

        Record::BatchCommitted { batch_id } =>
            state.get_mut(batch_id).map(|s| s.status = Committed),

        Record::BatchAbort { batch_id } => {
            state.remove(batch_id);                // 进入终态
            buffer.discard(batch_id);              // 丢弃其 Data
        }
    }
}
```

### 4.7 Segment 轮转（CURRENT 原子切换）

```rust
fn rotate(&mut self) -> Result<()> {
    let new_seq = self.current_seq + 1;
    let new_path = self.shard_dir.join(format!("{:020}.wal", new_seq));

    // ① 创建新 segment 并写入 FileHeader
    let mut f = File::create(&new_path)?;
    f.write_all(&FileHeader::new(self.shard_id, new_seq).encode())?;
    f.sync_all()?;                                  // 文件 fsync

    // ② 原子切换 CURRENT（先写 tmp，再 rename）
    let tmp = self.shard_dir.join("CURRENT.tmp");
    fs::write(&tmp, format!("{:020}.wal", new_seq).as_bytes())?;
    File::open(&tmp)?.sync_all()?;                  // tmp 文件 fsync
    File::open(&self.shard_dir)?.sync_all()?;       // 【关键】目录 fsync
    fs::rename(&tmp, self.shard_dir.join("CURRENT"))?;
    File::open(&self.shard_dir)?.sync_all()?;       // rename 后再 fsync 目录

    self.current = SegmentWriter::new(new_path)?;
    Ok(())
}
```

> **目录 fsync 不可省略**（v11 修正）。`rename` 原子性由 FS 保证，但目录项元数据需 `fsync(dir)` 持久化。

### 4.8 Segment 清理与 Batch 超时

**终态定义（C4）**：

```rust
fn is_terminal(s: BatchStatus) -> bool {
    matches!(s, Committed | Abort)   // ⚠️ Abort 也是终态
}
```

**清理线程**（每 60s）：

```rust
async fn cleanup_loop(wal: Arc<Wal>, state: Arc<BatchStateMap>) {
    loop {
        sleep(Duration::from_secs(60)).await;

        // ① 批次级超时：非终态且超时 → 写 BatchAbort
        for (id, st) in state.iter() {
            if !is_terminal(st.status)
               && st.created_at.elapsed() > config.batch_timeout {   // 默认 30min
                wal.append(Record::BatchAbort { batch_id: id.clone() }).await?;
                // 若已写 S3 → 文件成为孤儿，由孤儿清理回收
            }
        }

        // ② 磁盘保护：>80% 强制 abort 最老的未完成 batch
        if wal_disk_usage() > config.high_watermark {
            if let Some(oldest) = state.oldest_non_terminal() {
                force_abort(&wal, oldest).await?;
            }
        }

        // ③ 删除全部 batch 已达终态的 segment
        for (seg_id, batch_ids) in wal.segment_batches() {
            if seg_id == wal.current_segment() { continue; }
            if batch_ids.iter().all(|b| state.is_terminal(b)) {
                fs::remove_file(seg_id.path())?;
            }
        }
    }
}
```

**阈值理由**（架构 §5.3.6.1）：30 分钟容忍 S3 抖动与正常重试；80% 磁盘水位兜底，防止超时前写满。

### 4.9 错误处理

| 错误 | 处理 |
|---|---|
| CRC 校验失败 | 停止 replay（正常，表示 fsync 边界） |
| `length` 超文件剩余长度 | 停止 replay（尾部截断） |
| FileHeader magic 不匹配 | 该 segment 损坏 → 告警 + 跳过（人工介入） |
| CURRENT 损坏 | fallback 到目录内最大 seq（保守） |

---

## 五、Ingestor 详细设计

### 5.1 组件构成

```
Ingestor
├── Source 层      ArrowFlightSource（MVP）
├── WAL 层         WalWriter / WalReader / GroupCommitter
├── 攒批层         BatchAccumulator（按 shard+window 分组）
├── Schema 层      SchemaResolver（比对 + OCC 演进）
├── 编码层         VortexEncoder（+ Parquet 回退）
├── 上传层         S3Writer（Multipart + upload_id 持久化）
└── 提交层         CatalogClient（CommitFiles，幂等）
```

### 5.2 写入主流程（严格时序，C8）

```rust
async fn ingest_loop(
    mut rx: mpsc::Receiver<IngestBatch>,
    wal: Arc<WalWriter>,
    catalog: Arc<dyn CatalogOps>,
    schema_cache: Arc<SchemaCache>,
) -> Result<()> {
    while let Some(b) = rx.recv().await {
        // ① 解析本次写入的 schema
        let incoming = b.record_batch.schema();

        // ② 与本地缓存的表 schema 比对
        // ③ 判定是否需要演进
        let schema_version = loop {
            let cached = schema_cache.get(&b.table)?;
            match classify(&cached.schema, &incoming)? {
                Compatible     => break cached.version,
                NeedsEvolve(ch) => {
                    // ④ OCC 演进（必须在写 S3 之前）
                    match catalog.evolve_schema(EvolveSchemaRequest {
                        table: b.table.clone(),
                        change: ch,
                        expected_version: cached.version,
                    }).await {
                        Ok(resp) => {
                            schema_cache.update(&b.table, resp.new_schema, resp.version)?;
                            break resp.version;
                        }
                        Err(SchemaChanged { new_schema, version }) => {
                            // 拉取新 schema，回到 ② 重新判定
                            schema_cache.update(&b.table, new_schema, version)?;
                            continue;
                        }
                        Err(e) => return Err(e),
                    }
                }
                Incompatible => return Err(IngestError::SchemaIncompatible),
            }
        };

        // ⑤ 写 WAL（组提交 fsync）—— 数据已持久化
        wal.append(Record::Data {
            batch: b.record_batch.clone(),
            table: b.table.clone(),
            shard: b.shard_key.clone(),
            schema_version,
        }).await?;   // await 完成 = 已 fsync

        // ⑥ 返回确认给客户端（Flight DoPut 响应）
        ack_to_client(&b).await;
    }
    Ok(())
}
```

### 5.3 攒批触发

**触发条件**（任一满足即 flush）：
- 行数 >= `rows_threshold`（按表配置，默认 10,000）
- 距窗口开始 >= `time_threshold`（默认 5s）
- **整分钟对齐 + Jitter**：`flush_at = window_start + hash(shard+table) % 60s`（防惊群，ADR-10）
- **绝对空闲超时兜底**：5 分钟（防定时器 bug 导致无限滞留）

```rust
async fn accumulator_loop(wal: Arc<WalReader>, wal_w: Arc<WalWriter>, ...) {
    loop {
        // 【C2】严格只读已 fsync 的数据
        let synced = wal_w.synced_offset();
        let records = wal.scan_range(last_read, synced)?;

        for rec in records {
            if let Record::Data { batch, shard, .. } = rec {
                buf.entry((shard, window_of(&batch)))
                   .or_default()
                   .push(batch);
            }
        }

        // 检查各 (shard, window) 是否满足 flush
        for ((shard, window), batches) in buf.iter_mut() {
            if should_flush(shard, window, batches) {
                let merged = concat_batches(batches)?;
                let sorted = sort_by_sort_key(&merged)?;    // (event_time, ...)
                flush_batch(shard, window, sorted).await?;
                batches.clear();
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
}
```

### 5.4 Flush 全流程（三级状态机）

```rust
async fn flush_batch(shard, window, rows) -> Result<()> {
    // ③ 生成 batch_id（C1：随机 UUIDv7，不是内容哈希）
    let batch_id = Uuid::now_v7().to_string();

    // 写 BatchPending
    wal.append(Record::BatchPending {
        batch_id: batch_id.clone(),
        shard, window,
        wal_seq_range: (start, end),
        schema_version,
        client_request_id,
    }).await?;

    // ④ 写 S3（Multipart，持久化 upload_id）
    let (s3_paths, upload_id) = s3_write(&rows, &batch_id).await?;
    wal.append(Record::BatchS3Written {
        batch_id: batch_id.clone(), s3_paths, s3_upload_id: upload_id,
    }).await?;

    // ⑤ 提交 Meta（幂等，按 batch_id + client_request_id 去重）
    let resp = catalog.commit_files(CommitFilesRequest {
        batch_id: batch_id.clone(),
        client_request_id,
        files: s3_paths.iter().map(to_manifest).collect(),
        schema_version,
        row_count: rows.num_rows() as u64,
        stats_lite: compute_stats_lite(&rows)?,
    }).await?;

    // 写 BatchCommitted（进入终态）
    wal.append(Record::BatchCommitted { batch_id }).await?;

    Ok(())
}
```

### 5.5 S3 Multipart 与 upload_id

**upload_id 持久化**：写入 `BatchS3Written` Record，崩溃恢复时复用续传。

**S3 7 天超时处理**（v11）：

```rust
match s3.list_parts(&upload_id).await {
    Ok(parts) => resume_upload(parts).await,           // 续传
    Err(NoSuchUpload) => {                             // 已过期/被 abort
        let new = s3.create_multipart_upload().await?; // 重新发起
        // 更新 BatchState：upload_id = new, status 回退 Pending
        restart_upload(&rows).await
    }
    Err(e) => retry_or_fail(e),
}
```

**S3 Lifecycle**：配 7 天规则自动清理未 complete 的 parts（与 S3 自身 Multipart 有效期对齐）。

### 5.6 崩溃恢复分流（三级状态机）

启动时扫描重建的 `BatchState`，按状态处理：

| 状态 | 含义 | 恢复动作 |
|---|---|---|
| `Pending` | 未写 S3 | 重新编码 → 写 S3 → Commit（batch_id 复用） |
| `S3Written` | 已写 S3，未 Commit | **只 Commit**（Meta 按 batch_id 幂等） |
| `Committed` | 已完成 | 仅清理 WAL / 状态 |
| `Abort` | 已放弃 | 丢弃；若已写 S3 → 文件由孤儿清理回收 |

### 5.7 幂等键处理（表模板）

```rust
pub enum TableTemplate { Audit, General, Metrics, Traces }

impl TableTemplate {
    fn require_idempotency_key(&self) -> bool {
        match self {
            Audit | General => true,     // 默认开启
            Metrics | Traces => false,   // 高吞吐，容忍极低概率重复
        }
    }
}

// 处理矩阵
match (table.require_key, key.is_some()) {
    (true,  true)  => dedup_and_write(key),        // 正常去重
    (true,  false) => Err(IdempotencyKeyRequired), // 【关键】拒绝，非静默降级
    (false, true)  => dedup_and_write(key),        // 客户端主动要求幂等
    (false, false) => write_without_dedup(),       // 不去重
}

// 校验
fn validate(key: &str) -> Result<()> {
    if key.len() > 256 { return Err(IdempotencyKeyTooLong); }
    Ok(())  // 字符集不限制（UUID 或 source_${ts}_${seq} 均可）
}
```

> ⚠️ `true` 表未传幂等键必须**拒绝**，否则"强制"形同虚设（架构 §7.3.2）。

---

## 六、Meta 服务详细设计

### 6.1 阶段 0：内存 Catalog

```rust
pub struct MemoryCatalog {
    tables: RwLock<HashMap<String, TableMeta>>,
    schemas: RwLock<HashMap<(String, u64), SchemaVersion>>,
    files: RwLock<HashMap<String /*batch_id*/, FileManifest>>,
    by_shard: RwLock<HashMap<(String, String), HashSet<String>>>, // (table,shard) -> batch_ids
    idempotency: RwLock<HashMap<String, IdempotencyRecord>>,
    snapshot_version: AtomicU64,   // 单调递增快照号
}
```

**即使阶段 0 单节点，接口也按 Raft 线性一致性语义设计**（`apply` / `read_index` 抽象），阶段 1 切换零业务改动。

### 6.2 数据模型（prost）

```protobuf
message TableMeta {
    string name = 1;
    uint64 current_schema_version = 2;
    repeated string partition_cols = 3;
    string default_format = 4;         // "vortex" | "parquet"
    IngestConfig ingest_config = 5;
    uint64 created_at = 6;
}

message SchemaVersion {
    uint64 version = 1;
    bytes arrow_schema = 2;            // Arrow IPC 序列化
    SchemaChangeKind change_kind = 3;  // ADD_COLUMN / WIDEN_TYPE / DROP_COLUMN
    uint64 created_at = 4;
    string change_desc = 5;
}

message FileManifest {
    string file_path = 1;
    string batch_id = 2;               // 幂等主键
    string client_request_id = 3;      // 唯一索引（可空）
    uint64 schema_version = 4;
    FileStatus status = 5;             // ACTIVE / STAGED / DELETED
    uint64 valid_from = 6;             // 快照号
    uint64 deleted_at = 7;             // 0 = 未删除
    StatisticsLite stats = 8;          // 仅排序列 + 分区列的 min/max/null_count
    uint64 row_count = 9;
    uint64 file_size = 10;
}

message IdempotencyRecord {            // 【v8】独立表，不随文件删除
    string client_request_id = 1;      // 主键
    string batch_id = 2;
    uint64 committed_at = 3;           // TTL 24h 起算点
}

message StatisticsLite {
    repeated ColumnStatLite columns = 1;   // 仅排序列 + 分区列
}
message ColumnStatLite {
    string name = 1;
    bytes min = 2;   // Arrow 标量序列化
    bytes max = 3;
    uint64 null_count = 4;
}
```

### 6.3 快照隔离（删除语义核心）

```
查询可见性规则：
  文件可见 ⟺  valid_from <= query_snapshot
            AND (deleted_at == 0 OR query_snapshot < deleted_at)
```

```rust
fn list_visible_files(&self, table: &str, snapshot: u64, shard: Option<&str>)
    -> Vec<FileManifest>
{
    self.files.values()
        .filter(|f| f.table == table)
        .filter(|f| shard.map_or(true, |s| f.shard == s))
        .filter(|f| f.valid_from <= snapshot
                    && (f.deleted_at == 0 || snapshot < f.deleted_at))
        .cloned()
        .collect()
}
```

**同一机制服务三种删除**（架构 §6）：
- **L1 分片移除**：整 shard 文件 `deleted_at = current_snapshot`
- **L2 Compaction**：旧文件 `deleted_at`，新文件 `valid_from = snapshot+1`
- **L3 行级删除**：非目标（远期）

### 6.4 gRPC 接口（阶段 1，阶段 0 进程内同签名）

```protobuf
service CatalogService {
    rpc CreateTable(CreateTableRequest) returns (CreateTableResponse);
    rpc GetTable(GetTableRequest) returns (GetTableResponse);

    // Schema 演进（唯一 OCC 点）
    rpc EvolveSchema(EvolveSchemaRequest) returns (EvolveSchemaResponse);

    // 文件提交（幂等）
    rpc CommitFiles(CommitFilesRequest) returns (CommitFilesResponse);
    rpc ListVisibleFiles(ListVisibleFilesRequest) returns (ListVisibleFilesResponse);

    // 删除
    rpc DropShard(DropShardRequest) returns (DropShardResponse);

    // 幂等
    rpc CheckIdempotency(CheckIdempotencyRequest) returns (CheckIdempotencyResponse);

    // 变更通知（阶段 2+）
    rpc WatchChanges(WatchRequest) returns (stream ChangeEvent);

    // Compaction 作业租约（阶段 2+）
    rpc AcquireLease(AcquireLeaseRequest) returns (AcquireLeaseResponse);
}

message CommitFilesRequest {
    string table = 1;
    string batch_id = 2;             // 幂等主键
    string client_request_id = 3;    // 唯一索引
    repeated FileManifest files = 4;
    uint64 schema_version = 5;
    uint64 row_count = 6;
}

message CommitFilesResponse {
    bool accepted = 1;               // false = 重复提交（幂等成功）
    uint64 snapshot = 2;             // 返回的可见快照号
    uint64 commit_index = 3;         // 阶段 1：Raft log index
}
```

**CommitFiles 幂等实现**：

```rust
async fn commit_files(&self, req) -> Result<CommitFilesResponse> {
    let mut files = self.files.write();
    if files.contains_key(&req.batch_id) {
        // 幂等：已提交过，返回成功（不报错）
        return Ok(CommitFilesResponse { accepted: false, .. });
    }
    // 幂等键唯一索引检查
    if let Some(key) = &req.client_request_id {
        if self.idempotency.contains_key(key) {
            return Ok(CommitFilesResponse { accepted: false, .. });
        }
        self.idempotency.insert(key.clone(), IdempotencyRecord {
            client_request_id: key.clone(),
            batch_id: req.batch_id.clone(),
            committed_at: now(),
        });
    }
    files.insert(req.batch_id.clone(), manifest);
    Ok(CommitFilesResponse { accepted: true, snapshot: self.next_snapshot(), .. })
}
```

### 6.5 阶段 1 预告：Raft 与 fjall

| 数据 | 存储 | 实现要点 |
|---|---|---|
| Raft log / hard state | **fjall** | `impl raft::Storage`，key = **大端序 u64 index**（range query 有序） |
| Catalog（含 Schema） | **内存** | state machine，**不落 fjall**（C5） |
| Catalog 持久化 | **raft snapshot** | 恢复 = snapshot + 重放 log |

**Snapshot 异步生成**（§5.4.3.1，Leader flapping 防护）：

```rust
// ⚠️ 不能"克隆 Arc 派发后台"（会产生半新半旧 snapshot）
// 必须用持久化数据结构，O(1) 取不可变快照
struct CatalogState {
    tables: im::HashMap<String, TableMeta>,
    files:  im::OrdMap<String, FileManifest>,
}

fn snapshot(&self, _: u64) -> Result<Snapshot> {
    let guard = self.state.read();
    let snap = guard.clone();          // O(1)，持久化数据结构
    let idx = guard.last_applied;
    drop(guard);                        // 立即释放锁
    let h = tokio::task::spawn_blocking(move || serialize(&snap, idx));
    Ok(Snapshot::new_async(h))
}
```

---

## 七、Query 与 DataFusion 集成

> 对应架构 §8。**C6 / C7 是关键**。

### 7.1 三级 Catalog 桥接

```rust
pub struct LakeCatalogProvider { cache: Arc<LocalCatalogCache> }

impl CatalogProvider for LakeCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.cache.schema_names()      // 同步读本地缓存，无 gRPC
    }
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.cache.schema(name)
    }
}
```

**⚠️ 同步签名约束**：`schema()` / `table()` 是同步方法。内部若发起 gRPC 需用 `block_on`，会耗尽 tokio 阻塞线程池 → **必须读本地缓存**（ADR-6）。

### 7.2 TableProvider：Manifest 驱动（C7）

```rust
async fn scan(&self, state, projection, filters, limit) -> Result<Arc<dyn ExecutionPlan>> {
    // ① 从 filters 提取时间范围 / shard 过滤
    let hint = extract_hint(filters);

    // ② 从本地 Manifest 缓存取可见文件（不 list S3！）
    let files = self.cache.list_visible_files(
        &self.table, self.current_snapshot(), hint.shard)?;

    // ③ 转成 PartitionedFile（读缓存中的精简统计）
    let partitioned: Vec<PartitionedFile> = files.into_iter()
        .map(|f| PartitionedFile::new(f.file_path, f.file_size)
                    .with_statistics(to_df_stats(&f.stats)))
        .collect();

    // ④ 分组到执行分区
    let file_groups = vec![partitioned];   // 或按大小均衡分组

    // ⑤ 用 VortexFormat 建 plan（复用 vortex-datafusion，不重写）
    let source = VortexSource::default()
        .with_schema_adapter_factory(Arc::new(LakeAdapterFactory {
            table_schema: self.table_schema.clone(),
            file_versions: file_schema_versions,
        }));

    let config = FileScanConfig::new(self.object_store_url, self.schema.clone(), source)
        .with_file_groups(file_groups)
        .with_projection(projection.cloned())
        .with_limit(limit);

    Ok(DataSourceExec::from_data_source(config))
}
```

### 7.3 两层 Adapter（C6，性能生死线）

```rust
pub struct LakeAdapterFactory {
    table_schema: SchemaRef,
    file_versions: HashMap<String, u64>,   // file_path -> schema_version
}

impl SchemaAdapterFactory for LakeAdapterFactory {
    fn create(&self, file_schema: SchemaRef) -> Box<dyn SchemaAdapter> {
        Box::new(LakeSchemaAdapter {
            table_schema: self.table_schema.clone(),
            file_schema,
        })
    }
}

// ① 列/结构级：处理加列、删列、列顺序
impl SchemaAdapter for LakeSchemaAdapter {
    fn map_column_index(&self, idx: usize, file_schema: &Schema) -> Option<usize> {
        file_schema.index_of(self.table_schema.field(idx).name()).ok()
    }
    fn map_schema(&self, file_schema: &Schema) -> Result<SchemaRef> { ... }
}

// ② 表达式级：处理类型差异，让谓词可下推
impl PhysicalExprAdapter for LakePhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        // 把表 schema 上的谓词（如 ts: Timestamp）
        // 改写成文件 schema 上的谓词（如 ts: Int64 的比较）
        // → 使 Vortex 能在文件级/段级剪枝
    }
}
```

**为什么两层缺一不可**：

| 只用 `SchemaAdapter` | 只用 `PhysicalExprAdapter` |
|---|---|
| 列能对上，但类型不同的列（如 `ts: Int64` vs `Timestamp`）**谓词无法下推** → 全量读出 → cast → 过滤 → **剪枝与 Sort Pushdown 全失效** | 表达式可改写，但列索引映射缺失时无法定位列 |

**验收标准**：`EXPLAIN SELECT * FROM t WHERE ts > X`，计划中**不应出现 `FilterExec`**（已被下推到 DataSourceExec）。

### 7.4 热数据（阶段 2+）

按时间窗口切分 MemTable（每 1 分钟一个），查询时按 `event_time` 路由，避免全量扫描热缓冲：

```rust
hot_windows: Vec<(TimeWindow, MemTable)>   // 内存中保留最近 5 个窗口
```

阶段 0（All-in-One）热数据可直接读 Ingestor 内存缓冲；分离后通过定向拉取（非广播）。

### 7.5 统计信息

```rust
fn statistics(&self) -> Option<Statistics> {
    Some(Statistics {
        num_rows: Precision::Inexact(self.cache.total_rows(&self.table)),
        total_byte_size: Precision::Inexact(self.cache.total_bytes(&self.table)),
        column_statistics: self.cache.column_stats(&self.table),
    })
}
```

---

## 八、Schema 演进实现

### 8.1 类型提升格

```
          Int8
           ↓
        Int16 → Int32 → Int64 → Float64
                                   ↑
   (Utf8 与数值不可互转 —— 拒绝)
```

| 变更 | 策略 |
|---|---|
| 加列 | **自动演进**（缺失列填 null） |
| 类型宽化（Int32→Int64→Float64） | **自动演进** |
| 类型窄化（Float64→Int32） | **拒绝**（需显式 DDL） |
| 数值 ↔ 字符串 | **拒绝** |
| 删列 | 逻辑删除（`deleted_at`），物理文件不变 |

### 8.2 OCC 实现（C8）

```rust
async fn evolve_schema(&self, req) -> Result<EvolveSchemaResponse> {
    let mut t = self.tables.write();
    let table = t.get_mut(&req.table).ok_or(NotFound)?;

    // 【唯一乐观锁点】
    if table.current_schema_version != req.expected_version {
        return Err(SchemaChanged {
            actual_version: table.current_schema_version,
            new_schema: table.schema.clone(),
        });
    }

    // 按类型提升格应用变更
    let new_schema = apply_change(&table.schema, req.change)?;
    table.current_schema_version += 1;
    table.schema = new_schema.clone();
    self.schemas.insert((req.table, version), SchemaVersion { ... });

    Ok(EvolveSchemaResponse { new_schema, version: table.current_schema_version })
}
```

**`CommitFiles` 不校验 schema version**（C8）——不同文件可有不同 `schema_version`，是设计允许的常态。

### 8.3 InfluxDB 映射（阶段 2+，本期仅预留）

```rust
pub enum InfluxTypeMapping { Float64 /*默认*/, String, Strict }
pub enum InfluxTagMapping  { Dictionary /*默认*/, PlainUtf8 }
```

| 语义 | Arrow 类型 | 索引 |
|---|---|---|
| Tag | `Dictionary<UInt32, Utf8>` | 自动 MinMax + Bloom |
| Field | `Float64` | 仅 MinMax（Vortex footer） |

---

## 九、后台作业

### 9.1 Compaction

```rust
pub struct CompactionJob {
    target_files: Vec<FileManifest>,
    compaction_schema_version: u64,   // 【启动时锁定】不随演进变化
    compaction_schema: SchemaRef,
    lease_id: String,
}
```

**流程**：
1. 向 Meta 申请租约（阶段 0 单进程可跳过）
2. **锁定 schema 版本**（读当前表 schema + version）
3. 读目标文件 → 用锁定 schema 重新编码（缺失列填 null、类型按提升格 cast、逻辑删除列按 24h 窗口丢弃）
4. 写新文件 C 到 S3
5. 提交快照：`C.valid_from = snapshot+1`，`A/B.deleted_at = snapshot+1`

**安全约束**（§6.8.1）：物理丢弃"已逻辑删除列"前，检查 `deleted_at` 已超 24h（复用 §10.3 窗口，不引入新机制）。

### 9.2 孤儿清理

```rust
async fn orphan_sweeper(meta, s3) {
    let known = meta.list_all_batch_ids().await?;
    for obj in s3.list_objects("lake/").await? {
        let batch_id = extract_batch_id(&obj.key);
        // 【关键】先排除 Meta 已知文件，再按 created_at 判断
        if known.contains(&batch_id) { continue; }
        if obj.created_at < now() - Duration::from_hours(24) {
            s3.delete_object(&obj.key).await?;
        }
    }
}
```

**GC 水位澄清**（架构 §12.2.1）：孤儿清理（Meta 无记录，按 `created_at`）与 Compaction 清理（Meta 有记录，按 `deleted_at`）**操作对象互斥**，不存在竞态。幂等键 TTL 独立（`committed_at`），三者不共享时间轴是正确设计。

### 9.3 资源隔离

Compaction 使用**独立 tokio blocking pool**，避免挤占 Ingestor 攒批与 Query 响应：

```rust
let compaction_pool = tokio::task::Builder::new()
    .name("compaction")
    .build()?;   // 或 spawn_blocking + semaphore 限流
```

---

## 十、错误码与重试策略

### 10.1 错误分类

```rust
#[derive(thiserror::Error, Debug)]
pub enum LakeError {
    // ---- 客户端错误（4xx，不重试）----
    #[error("schema incompatible: {0}")]
    SchemaIncompatible(String),
    #[error("idempotency key required")]
    IdempotencyKeyRequired,
    #[error("idempotency key too long (max 256)")]
    IdempotencyKeyTooLong,
    #[error("table not found: {0}")]
    TableNotFound(String),

    // ---- 可重试（5xx / 暂态）----
    #[error("schema changed, retry")]
    SchemaChanged { actual_version: u64, new_schema: SchemaRef },
    #[error("s3 error: {0}")]
    S3(#[from] object_store::Error),
    #[error("wal error: {0}")]
    Wal(#[from] WalError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
```

### 10.2 重试策略

| 错误 | 策略 |
|---|---|
| `SchemaChanged` | **立即重试**（拉新 schema 重新判定，最多 3 次） |
| `S3` / 网络 | 指数退避（100ms 起，上限 10s，最多 5 次） |
| `Wal` | **不重试**（WAL 故障是致命错误，进程退出） |
| `TableNotFound` | 不重试，返回客户端 |

**⚠️ 幂等键要求**：任何重试**必须携带同一 `client_request_id`**，否则重试会产生重复数据。

---

## 十一、配置项清单

```toml
# lakehouse.toml

[wal]
dir = "/var/lib/ingestor/wal"
segment_max_size = "64MB"
segment_max_age = "1h"
group_commit_window = "1ms"       # 组提交窗口
group_commit_max_batch = 1024
batch_timeout = "30m"             # 【v11】批次超时 → BatchAbort
disk_high_watermark = 0.80        # 【v11】磁盘保护水位

[ingest]
default_format = "vortex"          # vortex | parquet（回退开关）
rows_threshold = 10000
time_threshold = "5s"
idle_timeout = "5m"                # 绝对空闲兜底
flush_jitter_seconds = 60          # 【v8】防惊群

[ingest.idempotency]
ttl = "24h"
require_by_default = true          # 【v9】表模板可覆盖

[schema]
default_policy = "evolve"          # evolve | strict | permissive

[store]
type = "s3"                        # s3 | local | mock（测试）
bucket = "soc-lake"
endpoint = "http://minio:9000"
multipart_threshold = "8MB"

[compaction]
enabled = true
interval = "1h"
min_files = 10
target_file_size = "512MB"
schema_snapshot_lock = true        # 【v9】启动时锁定 schema 版本

[gc]
orphan_delay = "24h"               # 孤儿文件延迟清理
physical_delete_delay = "24h"      # deleted_at 后物理删除窗口

[query]
cache_ttl = "30s"                  # LocalCatalogCache TTL
```

---

## 十二、测试策略

### 12.1 单元测试

| 模块 | 重点 |
|---|---|
| `wal` | Record 编解码、CRC 校验、segment 轮转、状态机重建 |
| `catalog` | 快照可见性过滤、幂等去重、OCC 冲突 |
| `format` | Vortex/Parquet 读写往返、schema 适配 |

### 12.2 集成测试

- 端到端：Flight 写入 → 查询可见
- Schema 演进：加列后老文件仍可查，谓词下推
- 幂等键：重复提交不产生重复数据

### 12.3 Chaos 测试（阶段 0.5，必做）

| # | 场景 | 断言 |
|---|---|---|
| 1 | Compaction 期间查询 | 无重复、无已删数据 |
| 2 | 分片移除期间查询 | `valid_from`/`deleted_at` 过滤正确 |
| 3 | 孤儿清理 | 不误删 Meta 已知文件 |
| 4 | Schema 变更 + EXPLAIN | `FilterExec` 被消除（谓词下推生效） |
| 5 | 幂等键 + Compaction | 删除文件后 24h 内重试仍幂等 |
| 6 | 崩溃恢复（各状态点） | 三级状态机正确 |
| 7 | **WAL 撕裂** | 截断文件尾部 → CRC 拦截，停止 replay |
| 8 | **并发写 + fsync 前/中/后 kill** | 三种情况均无丢失、无重复 |
| 9 | **`synced_offset`** | write 后 fsync 前 kill → 攒批线程读不到 |
| 10 | **Batch 超时** | 模拟 S3 不可用 → 30min 后 BatchAbort，segment 释放 |
| 11 | **磁盘水位** | 填至 80% → 强制 abort 最老 batch |

**验证目标**：100% 平滑无 500，无数据重复，无数据丢失（best_effort 语义内）。

---

**文档结束 · 详细设计 v1.0**

> 实现时如遇与架构文档冲突，以**架构文档 v11** 为准，并反馈修订本文。
