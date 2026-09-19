//! `RemoteCatalog`：`CatalogOps` 的 **gRPC 实现**（S3-4）。
//!
//! # 它在 R3 里的位置
//!
//! 同一个 trait 的两个实现：standalone 用 `MemoryCatalog`（同进程），分布式用本实现 ——
//! 上层（`query`/`ingest`/`compaction`）**零感知**。这正是 `§52` 把接缝上的具体类型清掉的收益：
//! 切过去只需要在**装配点**换一行。
//!
//! # 三件事
//!
//! | 关注点 | 做法 |
//! |---|---|
//! | **写** | 每个方法 → 一个 `Op` → `Propose`（raft 提交后才返回，**线性化**） |
//! | **读** | 本地缓存 + **版本驱动刷新**（设计 §3.2）：先发纯版本探测（零载荷），变了才拉增量 |
//! | **换主** | 非 leader 的 `Propose` 返回 `Unavailable` + leader hint → 本实现**轮换下一个地址**再试 |
//!
//! # 陈旧窗口是**设计**，不是缺陷
//!
//! 读不打 metanode（设计 §3.2 明确说明理由：DataFusion 的 provider 是同步 trait、规划期反复调用），
//! 所以本地缓存允许**陈旧 ≤ `cache_ttl`**，由版本号驱动收敛。这里不发明第二个机制。
//!
//! # 幂等快路径为什么是"本地的"（且**可以**是局部的）
//!
//! 设计 §3.2：幂等预筛是**加速**（权威在 SM）。本实现只把**自己写过的键**记在本地：
//! 漏判的后果是"走到 `Propose`，由 SM 去重"（正确性由 SM 保证），代价是少省一次写 ——
//! **不会**出现"本地说没写过、SM 也放行"的双写（因为 `commit_files` 在 SM 里按键去重）。

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Duration;

use tonic::transport::Channel;
use yuntun_catalog::CatalogOps;
use yuntun_model::error::LakeError;
use yuntun_model::meta::{FileManifest, IdempotencyRecord, TableMeta};
use yuntun_model::ops::{
    qualified_name, split_qualified, CatalogVersion, CommitFilesRequest, CommitFilesResponse,
    CreateTableRequest, EvolveSchemaRequest, EvolveSchemaResponse, ManifestDelta,
};
use yuntun_proto::meta as pb;
use yuntun_proto::meta::meta_client::MetaClient;

use crate::op;

/// 写操作的超时（`Propose` 要等 raft 提交 + 应用，给足时间；超时**不重试** —— 见 [`RemoteCatalog::propose`]）。
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// 读/探测的超时（很短：探测本来就该便宜，慢了不如让它失败重来）。
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// 连接超时。
const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);

/// 本地缓存（**唯一**的读来源；由 [`RemoteCatalog::refresh`] 版本驱动更新）。
#[derive(Debug, Default)]
struct Cache {
    schema_ver: u64,
    manifest_ver: u64,
    snapshot: u64,
    read_index: u64,
    /// schema 名（每次刷新**整体替换**：删掉的 schema 必须消失，否则 `USE` 还能切进去）
    namespaces: BTreeSet<String>,
    /// 全限定名 → 表元数据
    tables: BTreeMap<String, TableMeta>,
    /// `batch_id` → 文件清单。**墓碑也留在里面**：查询会按**旧快照**读文件，
    /// 删掉墓碑就等于把"这个文件曾经存在"这件事抹了（`list_visible_files(旧快照)` 会少文件）。
    files: BTreeMap<String, FileManifest>,
    /// 做过一次全量刷新（在此之前缓存是不可信的，读必须等）
    primed: bool,
}

/// `CatalogOps` 的 gRPC 实现。
pub struct RemoteCatalog {
    addrs: Vec<String>,
    clients: Vec<MetaClient<Channel>>,
    /// 轮换游标：写失败就换下一个地址（不依赖任何"谁是 leader"的客户端状态）
    cursor: AtomicUsize,
    cache: RwLock<Cache>,
    /// 幂等**本地快路径**（只含本客户端写过的键；见模块文档）
    local_keys: Mutex<HashSet<String>>,
}

impl RemoteCatalog {
    /// 连一组 metanode（至少一个地址）。
    ///
    /// 用 `connect_lazy`：**不在构造期连接** —— 装配时 metanode 可能还没起来
    /// （standalone 就是同进程先后启动），连接交给第一次 RPC。
    pub fn connect(addrs: Vec<String>) -> Result<Self, LakeError> {
        if addrs.is_empty() {
            return Err(LakeError::Other(
                "RemoteCatalog 需要至少一个 metanode 地址".into(),
            ));
        }
        let mut clients = Vec::with_capacity(addrs.len());
        for a in &addrs {
            let ep = tonic::transport::Endpoint::from_shared(format!("http://{a}"))
                .map_err(|e| LakeError::Other(format!("metanode 地址 {a:?} 非法：{e}")))?
                .connect_timeout(CONNECT_TIMEOUT);
            clients.push(MetaClient::new(ep.connect_lazy()));
        }
        Ok(Self {
            addrs,
            clients,
            cursor: AtomicUsize::new(0),
            cache: RwLock::new(Cache::default()),
            local_keys: Mutex::new(HashSet::new()),
        })
    }

    pub fn addrs(&self) -> &[String] {
        &self.addrs
    }

    fn client(&self, i: usize) -> MetaClient<Channel> {
        self.clients[i % self.clients.len()].clone()
    }

    /// 写：`Propose` + **换主重试**。
    ///
    /// 只在 `Unavailable` / `Unknown` 上换地址重试：这两类表示"**请求没被这个节点处理**"
    /// （不是 leader / 连不上），换一个地址是安全的。`DeadlineExceeded` **不重试** ——
    /// 那时提案可能已经提交了，重试会变成"同 op 提两次"（写路径虽然幂等，但没必要冒险）。
    async fn propose(&self, op: pb::Op) -> Result<pb::ProposeResponse, LakeError> {
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        let mut last: Option<LakeError> = None;
        for k in 0..self.clients.len() {
            let i = (start + k) % self.clients.len();
            let mut c = self.client(i);
            let req = pb::ProposeRequest {
                op: Some(op.clone()),
                request_id: Vec::new(),
                schema_ver: 0,
            };
            match tokio::time::timeout(WRITE_TIMEOUT, c.propose(req)).await {
                Ok(Ok(r)) => return Ok(r.into_inner()),
                Ok(Err(st))
                    if matches!(st.code(), tonic::Code::Unavailable | tonic::Code::Unknown) =>
                {
                    last = Some(map_status(st));
                }
                Ok(Err(st)) => return Err(map_status(st)),
                Err(_) => {
                    return Err(LakeError::Other(format!(
                        "Propose 超时（{}s）：提案可能已提交，**不自动重试**（调用方按幂等键重试是安全的）",
                        WRITE_TIMEOUT.as_secs()
                    )))
                }
            }
        }
        Err(last.unwrap_or_else(|| LakeError::Other("没有可用的 metanode".into())))
    }

    async fn prefetch(&self, req: pb::PrefetchRequest) -> Result<pb::PrefetchResponse, LakeError> {
        let mut last: Option<LakeError> = None;
        for i in 0..self.clients.len() {
            let mut c = self.client(i);
            match tokio::time::timeout(READ_TIMEOUT, c.prefetch(req.clone())).await {
                Ok(Ok(r)) => return Ok(r.into_inner()),
                Ok(Err(st)) if st.code() == tonic::Code::Unavailable => last = Some(map_status(st)),
                Ok(Err(st)) => return Err(map_status(st)),
                Err(_) => last = Some(LakeError::Other("Prefetch 超时".into())),
            }
        }
        Err(last.unwrap_or_else(|| LakeError::Other("没有可用的 metanode（Prefetch）".into())))
    }

    async fn status(&self) -> Result<pb::StatusResponse, LakeError> {
        let mut last: Option<LakeError> = None;
        for i in 0..self.clients.len() {
            let mut c = self.client(i);
            match tokio::time::timeout(READ_TIMEOUT, c.status(pb::StatusRequest {})).await {
                Ok(Ok(r)) => return Ok(r.into_inner()),
                Ok(Err(st)) => last = Some(map_status(st)),
                Err(_) => last = Some(LakeError::Other("Status 超时".into())),
            }
        }
        Err(last.unwrap_or_else(|| LakeError::Other("没有可用的 metanode（Status）".into())))
    }

    async fn delta(&self, since_manifest_ver: u64) -> Result<pb::DeltaResponse, LakeError> {
        let mut last: Option<LakeError> = None;
        for i in 0..self.clients.len() {
            let mut c = self.client(i);
            let req = pb::DeltaRequest { since_manifest_ver };
            match tokio::time::timeout(READ_TIMEOUT, c.delta(req)).await {
                Ok(Ok(r)) => return Ok(r.into_inner()),
                Ok(Err(st)) => last = Some(map_status(st)),
                Err(_) => last = Some(LakeError::Other("Delta 超时".into())),
            }
        }
        Err(last.unwrap_or_else(|| LakeError::Other("没有可用的 metanode（Delta）".into())))
    }

    /// **刷新本地缓存**（版本驱动）。
    ///
    /// 两步（这条路径就是设计 §3.2 的「无变化零开销」）：
    /// 1. **纯版本探测**：`tables` 空 + `full=false` → 载荷为空，只回版本号/快照号；
    /// 2. 版本**变了**（或首次）才真的拉载荷：变了哪些表用 `Delta` 问，文件用 `since_snapshot` 拿增量。
    async fn refresh(&self) -> Result<(), LakeError> {
        let (c_schema, c_manifest, c_snapshot, primed) = {
            let c = self.cache.read().unwrap();
            (c.schema_ver, c.manifest_ver, c.snapshot, c.primed)
        };

        // ---- ① 探测 ----
        let probe = self
            .prefetch(pb::PrefetchRequest {
                since_schema_ver: c_schema,
                since_manifest_ver: c_manifest,
                full: false,
                tables: Vec::new(),
                since_snapshot: c_snapshot,
            })
            .await?;
        let st = self.status().await?;
        {
            let mut c = self.cache.write().unwrap();
            c.read_index = st.applied_index;
            c.snapshot = probe.snapshot;
        }
        let unchanged = primed && probe.schema_ver == c_schema && probe.manifest_ver == c_manifest;
        if unchanged && !probe.full_reload {
            return Ok(()); // 零开销路径
        }

        // ---- ② 拉载荷（全量或增量）----
        //
        // ⚠️ **结构变更必须走全量**。这是对拍抓出来的真 bug：`Delta.changed_tables` 是
        // **manifest 级**的（`table_manifest_ver` 推进过的表），而 `create_table`/`drop_table`
        // 只动 `schema_ver` —— **新建的表根本不在 `changed_tables` 里**。照"变了就走增量"的
        // 直觉写，客户端会**静默丢掉刚建的表**（现象是"远端建表后读不到"，而对拍里一建表就红）。
        //
        // 为什么不让服务端来判断：`DeltaRequest` 只带 `since_manifest_ver`，
        // **不带客户端的 schema_ver**，服务端无从知道"对面缺哪些结构变更" ✗
        // → 所以信号只能由客户端从探测结果里取（它本来就知道自己的版本）。
        let structure_changed = probe.schema_ver != c_schema;
        let full = !primed || probe.full_reload || structure_changed;
        let requested: Vec<String> = if full {
            Vec::new()
        } else {
            // 变了哪些表：`Delta` 只回名字（够用 —— 表元数据由下一步按名拉）
            self.delta(c_manifest).await?.changed_tables
        };
        let req = if full {
            pb::PrefetchRequest {
                since_schema_ver: 0,
                since_manifest_ver: 0,
                full: true,
                tables: Vec::new(),
                since_snapshot: 0, // 全量：文件也要全量（含墓碑）
            }
        } else {
            pb::PrefetchRequest {
                since_schema_ver: 0,
                since_manifest_ver: 0,
                full: false,
                tables: requested.clone(),
                since_snapshot: c_snapshot,
            }
        };
        let resp = self.prefetch(req).await?;
        if resp.full_reload && !full {
            // 版本超前 / 服务端要求重建：**按全量再来一次**（不是把增量塞进旧缓存）
            let full_resp = self
                .prefetch(pb::PrefetchRequest {
                    since_schema_ver: 0,
                    since_manifest_ver: 0,
                    full: true,
                    tables: Vec::new(),
                    since_snapshot: 0,
                })
                .await?;
            self.apply(full_resp, &[], true);
            return Ok(());
        }
        self.apply(resp, &requested, full);
        Ok(())
    }

    /// 把载荷落到本地缓存。
    ///
    /// 两条**契约**（`§51.1`/`§54.3`）在这里生效：
    ///
    /// | 现象 | 含义 |
    /// |---|---|
    /// | 请求的表不在 `tables` 里 | **已删**（丢本地条目 + 它的文件） |
    /// | 文件不在 `files` 里 | 没变（**不能**当成"已删"—— 文件是增量语义） |
    fn apply(&self, resp: pb::PrefetchResponse, requested: &[String], full: bool) {
        let payload = resp.payload.unwrap_or_default();
        let mut c = self.cache.write().unwrap();

        let mut seen: BTreeSet<String> = BTreeSet::new();
        for e in &payload.tables {
            let key = e.name.clone();
            seen.insert(key.clone());
            match op::table_meta_from_proto(e) {
                Ok(m) => {
                    c.tables.insert(key, m);
                }
                Err(err) => {
                    // 载荷解不开是**协议问题**（不该发生），但要留下痕迹：静默丢条目会让
                    // 客户端"看不到表"，排查时完全无迹可循。
                    eprintln!("[remote-catalog] 表载荷解不开（{}）：{err}", e.name);
                }
            }
        }
        // 请求了却不在载荷里 = 已删；全量刷新时"不在"同样意味着不存在
        if full {
            let stale: Vec<String> = c
                .tables
                .keys()
                .filter(|k| !seen.contains(*k))
                .cloned()
                .collect();
            for k in stale {
                c.tables.remove(&k);
                c.files.retain(|_, f| normalize(&f.table) != k);
            }
        } else {
            for r in requested {
                let key = normalize(r);
                if !seen.contains(&key) && c.tables.remove(&key).is_some() {
                    c.files.retain(|_, f| normalize(&f.table) != key);
                }
            }
        }

        // 文件：upsert（**墓碑保留** —— 见 `Cache::files` 的注释）
        for f in &payload.files {
            if let Some(m) = f.manifest.as_ref() {
                c.files.insert(f.batch_id.clone(), op::manifest_from_proto_pub(m));
            }
        }

        // schema 名：**整体替换**（删掉的必须消失）
        c.namespaces = payload.namespaces.iter().cloned().collect();

        c.schema_ver = resp.schema_ver;
        c.manifest_ver = resp.manifest_ver;
        c.snapshot = resp.snapshot;
        c.primed = true;
    }

    /// 读之前先刷新（最佳努力：失败不阻断读，用旧缓存 + 让上层按陈旧窗口处理）。
    async fn refresh_best_effort(&self) {
        if let Err(e) = self.refresh().await {
            eprintln!("[remote-catalog] 刷新失败（用本地缓存继续）：{e}");
        }
    }

    /// 读出当前缓存快照（供只读方法用）。
    fn cached<T>(&self, f: impl FnOnce(&Cache) -> T) -> T {
        f(&self.cache.read().unwrap())
    }
}

/// 归一化表标识（与 `MemoryCatalog` 同一套：裸名 → `public.<name>`）。
fn normalize(name: &str) -> String {
    let (ns, t) = split_qualified(name);
    qualified_name(ns, t)
}

/// `tonic::Status` → `LakeError`。
///
/// 靠 **`err-kind` / `err-subject` metadata**（`§55`）而不是 parse message：
/// 依赖诊断文案的映射会在文案改动时**静默**退化成"一律 internal"，
/// 而 SQL 层的错误码（`ER_NO_SUCH_TABLE` 等）就是从这些变体来的。
fn map_status(st: tonic::Status) -> LakeError {
    let md = st.metadata();
    let kind = md.get("err-kind").and_then(|v| v.to_str().ok()).unwrap_or("");
    let subject = md
        .get("err-subject")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let msg = || st.message().to_string();
    match kind {
        "table-not-found" => LakeError::TableNotFound(subject),
        "schema-not-found" => LakeError::SchemaNotFound(subject),
        "table-already-exists" => LakeError::TableAlreadyExists(subject),
        "schema-already-exists" => LakeError::SchemaAlreadyExists(subject),
        "schema-not-empty" => LakeError::SchemaNotEmpty(subject),
        "schema-incompatible" => LakeError::SchemaIncompatible(msg()),
        "invalid-schema-change" => LakeError::InvalidSchemaChange(msg()),
        "idempotency-key-required" => LakeError::IdempotencyKeyRequired,
        "idempotency-key-too-long" => LakeError::IdempotencyKeyTooLong,
        "resource-exhausted" => LakeError::ResourceExhausted(msg()),
        // 这些需要调用方补上下文（`schema-changed` 要**读一次当前 schema** 才能还原，
        // 见 `evolve_schema`；`occ-conflict` 的实际版本在 `actual-version` 里）
        "schema-changed" | "occ-conflict" => LakeError::Other(format!("版本冲突：{}", msg())),
        "not-leader" => LakeError::Other(format!("非 leader（可重试）：{}", msg())),
        "no-quorum" => LakeError::ResourceExhausted(format!("多数派不可用：{}", msg())),
        _ => LakeError::Other(format!("远端错误（{}）：{}", st.code(), msg())),
    }
}

fn actual_version_of(st: &tonic::Status) -> Option<u64> {
    st.metadata()
        .get("actual-version")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
}

#[async_trait::async_trait]
impl CatalogOps for RemoteCatalog {
    // ---------------------------------------------------------------- schema（写）
    async fn create_schema(&self, name: &str) -> Result<(), LakeError> {
        let r = self
            .propose(pb::Op {
                now_ms: now_ms(),
                kind: Some(pb::op::Kind::CreateSchema(pb::CreateSchemaOp {
                    name: name.into(),
                })),
            })
            .await?;
        if !r.accepted {
            // 幂等命中 = "已经存在"（与 `MemoryCatalog` 的语义对齐：那是**错误**，不是静默成功）
            return Err(LakeError::SchemaAlreadyExists(name.to_string()));
        }
        self.refresh_best_effort().await;
        Ok(())
    }

    async fn drop_schema(&self, name: &str) -> Result<(), LakeError> {
        let r = self
            .propose(pb::Op {
                now_ms: now_ms(),
                kind: Some(pb::op::Kind::DropSchema(pb::DropSchemaOp {
                    name: name.into(),
                })),
            })
            .await?;
        if !r.accepted {
            return Err(LakeError::SchemaNotFound(name.to_string()));
        }
        self.refresh_best_effort().await;
        Ok(())
    }

    // ---------------------------------------------------------------- schema（读）
    async fn list_schemas(&self) -> Result<Vec<String>, LakeError> {
        self.refresh().await?;
        Ok(self.cached(|c| c.namespaces.iter().cloned().collect()))
    }

    async fn schema_exists(&self, name: &str) -> Result<bool, LakeError> {
        self.refresh().await?;
        Ok(self.cached(|c| c.namespaces.contains(name)))
    }

    // ---------------------------------------------------------------- 表（写）
    async fn create_table(&self, req: CreateTableRequest) -> Result<TableMeta, LakeError> {
        let qualified = req.qualified_name();
        let r = self
            .propose(pb::Op {
                now_ms: now_ms(),
                kind: Some(pb::op::Kind::CreateTable(op::create_table_to_proto(&req))),
            })
            .await?;
        if !r.accepted {
            return Err(LakeError::TableAlreadyExists(qualified));
        }
        self.refresh_best_effort().await;
        self.get_table(&qualified)
            .await?
            .ok_or_else(|| LakeError::Other(format!("建表后读不到 {qualified}（刷新可能有延迟）")))
    }

    async fn drop_table(&self, name: &str) -> Result<(), LakeError> {
        let r = self
            .propose(pb::Op {
                now_ms: now_ms(),
                kind: Some(pb::op::Kind::DropTable(pb::DropTableOp {
                    name: name.into(),
                })),
            })
            .await?;
        if !r.accepted {
            return Err(LakeError::TableNotFound(name.to_string()));
        }
        self.refresh_best_effort().await;
        Ok(())
    }

    async fn evolve_schema(
        &self,
        req: EvolveSchemaRequest,
    ) -> Result<EvolveSchemaResponse, LakeError> {
        let table = normalize(&req.table);
        let op_msg = pb::Op {
            now_ms: now_ms(),
            kind: Some(pb::op::Kind::EvolveSchema(op::evolve_schema_to_proto(&req))),
        };
        // OCC 冲突要还原成 `SchemaChanged { actual_version, new_schema }`：
        // `new_schema` 不在错误里（`Arc<Schema>` 过不了线），所以**冲突时多读一次**当前 schema。
        // 这一次额外往返只发生在冲突路径上（正常路径零成本）。
        let mut last: Option<tonic::Status> = None;
        for i in 0..self.clients.len() {
            let mut c = self.client(i);
            let r = tokio::time::timeout(
                WRITE_TIMEOUT,
                c.propose(pb::ProposeRequest {
                    op: Some(op_msg.clone()),
                    request_id: Vec::new(),
                    schema_ver: req.expected_version,
                }),
            )
            .await;
            match r {
                Ok(Ok(resp)) => {
                    let resp = resp.into_inner();
                    if !resp.accepted {
                        // evolve 的幂等命中：目标 schema 已经是这个版本 → 直接读回结果
                        break;
                    }
                    self.refresh_best_effort().await;
                    let (new_schema, version) = self
                        .table_schema(&table)
                        .await?
                        .ok_or_else(|| LakeError::TableNotFound(table.clone()))?;
                    return Ok(EvolveSchemaResponse {
                        new_schema,
                        version,
                    });
                }
                Ok(Err(st)) => {
                    if matches!(st.code(), tonic::Code::Unavailable | tonic::Code::Unknown) {
                        last = Some(st);
                        continue;
                    }
                    if st.code() == tonic::Code::FailedPrecondition {
                        if let Some(v) = actual_version_of(&st) {
                            // 冲突：读一次当前 schema，还原成与 `MemoryCatalog` **同形**的错误
                            let new_schema = match self.table_schema(&table).await? {
                                Some((s, _)) => s,
                                None => return Err(LakeError::TableNotFound(table.clone())),
                            };
                            return Err(LakeError::SchemaChanged {
                                actual_version: v,
                                new_schema,
                            });
                        }
                    }
                    return Err(map_status(st));
                }
                Err(_) => {
                    return Err(LakeError::Other("evolve_schema 超时".into()));
                }
            }
        }
        // 幂等命中：读回当前 schema 作为响应
        self.refresh_best_effort().await;
        let (new_schema, version) = self
            .table_schema(&table)
            .await?
            .ok_or_else(|| LakeError::TableNotFound(table.clone()))?;
        let _ = last;
        Ok(EvolveSchemaResponse {
            new_schema,
            version,
        })
    }

    // ---------------------------------------------------------------- 表（读）
    async fn get_table(&self, name: &str) -> Result<Option<TableMeta>, LakeError> {
        self.refresh().await?;
        Ok(self.cached(|c| c.tables.get(&normalize(name)).cloned()))
    }

    async fn list_tables(&self) -> Result<Vec<TableMeta>, LakeError> {
        self.refresh().await?;
        Ok(self.cached(|c| c.tables.values().cloned().collect()))
    }

    async fn table_schema(&self, name: &str) -> Result<Option<(arrow::datatypes::SchemaRef, u64)>, LakeError> {
        self.refresh().await?;
        let meta = self.cached(|c| c.tables.get(&normalize(name)).cloned());
        match meta {
            Some(m) => Ok(Some((m.schema()?, m.current_schema_version))),
            None => Ok(None),
        }
    }

    // ---------------------------------------------------------------- 文件
    async fn commit_files(
        &self,
        req: CommitFilesRequest,
    ) -> Result<CommitFilesResponse, LakeError> {
        let r = self
            .propose(pb::Op {
                now_ms: now_ms(),
                kind: Some(pb::op::Kind::CommitFiles(pb::CommitFilesOp {
                    request: Some(op::commit_request_to_proto(&req)),
                })),
            })
            .await?;
        // 把自己提交带的键记进本地快路径：**本节点重试自己的写**是最常见的路径，
        // 记下来就能省掉一次 `Propose`。注意这只是加速 —— 别的节点提交的键我们不知道，
        // 那种情况会走到 SM 去重（权威在 SM，见模块文档）。漏判**不会**造成双写。
        {
            let mut keys = self.local_keys.lock().unwrap();
            if let Some(k) = &req.client_request_id {
                keys.insert(k.clone());
            }
            for k in &req.client_request_ids {
                keys.insert(k.clone());
            }
        }
        self.refresh_best_effort().await;
        let snapshot = self.cached(|c| c.snapshot);
        Ok(CommitFilesResponse {
            accepted: r.accepted,
            snapshot,
            commit_index: r.revision,
        })
    }

    async fn list_visible_files(
        &self,
        table: &str,
        snapshot: u64,
        shard_filter: Option<&str>,
    ) -> Result<Vec<FileManifest>, LakeError> {
        self.refresh().await?;
        let key = normalize(table);
        let mut out: Vec<FileManifest> = self.cached(|c| {
            c.files
                .values()
                .filter(|f| normalize(&f.table) == key)
                .filter(|f| shard_filter.is_none_or(|s| f.shard == s))
                .filter(|f| f.visible_at(snapshot))
                .cloned()
                .collect()
        });
        // 与状态机同序（那边是 `BTreeMap` 的键序）：对拍才有意义
        out.sort_by(|a, b| a.batch_id.cmp(&b.batch_id));
        Ok(out)
    }

    async fn drop_shard(&self, table: &str, shard: &str) -> Result<u64, LakeError> {
        let r = self
            .propose(pb::Op {
                now_ms: now_ms(),
                kind: Some(pb::op::Kind::DropShard(pb::DropShardOp {
                    table: table.into(),
                    shard: shard.into(),
                })),
            })
            .await?;
        self.refresh_best_effort().await;
        // ⚠️ 精确条数过不了线（`ProposeResponse` 只有 `accepted`）；调用方目前只判"有没有生效"。
        //    要精确条数得让 `ApplyOutcome` 带上计数（登记在 operation-log 遗留）。
        Ok(if r.accepted { 1 } else { 0 })
    }

    // ---------------------------------------------------------------- Compaction / 运维
    async fn commit_compaction(
        &self,
        old_batch_ids: &[String],
        new_files: Vec<FileManifest>,
    ) -> Result<u64, LakeError> {
        self.propose(pb::Op {
            now_ms: now_ms(),
            kind: Some(pb::op::Kind::Compaction(pb::CompactionOp {
                old_batch_ids: old_batch_ids.to_vec(),
                new_files: new_files.iter().map(op::manifest_to_proto_pub).collect(),
            })),
        })
        .await?;
        self.refresh_best_effort().await;
        // 新的快照号由状态机的 `commit_compaction` 推进 → 从刷新后的缓存读回
        Ok(self.cached(|c| c.snapshot))
    }

    async fn known_batch_ids(&self) -> Result<Vec<String>, LakeError> {
        self.refresh().await?;
        Ok(self.cached(|c| c.files.keys().cloned().collect()))
    }

    // ---------------------------------------------------------------- 幂等
    async fn check_idempotency(&self, key: &str) -> Result<Option<String>, LakeError> {
        // 本地快路径（权威在 SM）：只回"我知道的键"。
        // 返回空 `batch_id` 是刻意的 —— 调用方只用 `is_some()`（见 `pipeline.rs` 的预筛），
        // 真要 batch_id 得把它放进载荷（登记在遗留）。
        Ok(self
            .local_keys
            .lock()
            .unwrap()
            .contains(key)
            .then(String::new))
    }

    async fn record_idempotency(&self, rec: IdempotencyRecord) -> Result<(), LakeError> {
        self.propose(pb::Op {
            now_ms: now_ms(),
            kind: Some(pb::op::Kind::Idempotency(pb::IdempotencyOp {
                record: Some(op::idempotency_record_to_proto(&rec)),
            })),
        })
        .await?;
        self.local_keys
            .lock()
            .unwrap()
            .insert(rec.client_request_id.clone());
        Ok(())
    }

    // ---------------------------------------------------------------- 快照 / 版本
    async fn current_snapshot(&self) -> u64 {
        self.refresh_best_effort().await;
        self.cached(|c| c.snapshot)
    }

    async fn read_index(&self) -> u64 {
        self.refresh_best_effort().await;
        self.cached(|c| c.read_index)
    }

    async fn version(&self) -> CatalogVersion {
        self.refresh_best_effort().await;
        self.cached(|c| CatalogVersion {
            schema_ver: c.schema_ver,
            manifest_ver: c.manifest_ver,
        })
    }

    async fn manifest_delta(&self, since_manifest_ver: u64) -> Result<ManifestDelta, LakeError> {
        // `Delta` 是给客户端**定点问**用的（比拉全量载荷便宜），直接用
        let d = self.delta(since_manifest_ver).await?;
        Ok(ManifestDelta {
            changed_tables: d.changed_tables,
            full_reload_required: d.full_reload,
        })
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetaError;

    fn shape(e: &LakeError) -> String {
        match e {
            LakeError::TableNotFound(t) => format!("TableNotFound({t})"),
            LakeError::SchemaNotFound(s) => format!("SchemaNotFound({s})"),
            LakeError::TableAlreadyExists(t) => format!("TableAlreadyExists({t})"),
            LakeError::SchemaAlreadyExists(s) => format!("SchemaAlreadyExists({s})"),
            LakeError::SchemaNotEmpty(s) => format!("SchemaNotEmpty({s})"),
            LakeError::IdempotencyKeyRequired => "IdempotencyKeyRequired".into(),
            LakeError::IdempotencyKeyTooLong => "IdempotencyKeyTooLong".into(),
            LakeError::ResourceExhausted(_) => "ResourceExhausted".into(),
            LakeError::InvalidSchemaChange(_) => "InvalidSchemaChange".into(),
            other => format!("其他（**退化**）：{other}"),
        }
    }

    /// `LakeError → Status → LakeError` 必须**保形**。
    ///
    /// 为什么单独立一条：这条链一断，远端所有错误都会退化成 `Other` ——
    /// SQL 层再也分不清「表不存在」（1051）与「服务内部错」，用户看到的错误码整体变差，
    /// 而**没有任何测试会红**（除非专门测这一层）。
    #[test]
    fn status_roundtrips_back_to_lake_error() {
        let cases = vec![
            LakeError::TableNotFound("public.cpu".into()),
            LakeError::SchemaNotFound("analytics".into()),
            LakeError::TableAlreadyExists("public.cpu".into()),
            LakeError::SchemaAlreadyExists("analytics".into()),
            LakeError::SchemaNotEmpty("analytics".into()),
            LakeError::IdempotencyKeyRequired,
            LakeError::IdempotencyKeyTooLong,
            LakeError::ResourceExhausted("busy".into()),
            LakeError::InvalidSchemaChange("bad change".into()),
        ];
        for e in cases {
            let st: tonic::Status = MetaError::Lake(e.clone()).into();
            let back = map_status(st);
            assert_eq!(shape(&back), shape(&e), "往返丢形：{e} → {back}");
        }
    }

    /// 归一化必须与 `MemoryCatalog` 一致（裸名 = `public.<name>`），否则远端会"查不到自己刚建的表"。
    #[test]
    fn normalize_matches_the_local_semantics() {
        assert_eq!(normalize("cpu"), "public.cpu");
        assert_eq!(normalize("public.cpu"), "public.cpu");
        assert_eq!(normalize("analytics.cpu"), "analytics.cpu");
    }
}
