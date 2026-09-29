//! **DML 前端**：`DELETE FROM t WHERE …`（`plan.md` F.3 / `delta-dml-design §4.1`）。
//!
//! 落点为什么在 **SQL 层**（与 `F.2` 的 DDL 同一个理由）：SQL 语义、只读拒绝、
//! WAL 追加、缓存刷新都已经在这里；写入句柄（`Ingestor` / `CatalogOps`）本层已有。
//!
//! # 执行顺序（顺序即正确性，`§4.1`）
//!
//! ```text
//! ① 强制 flush（把该表的在途数据落盘并提交）
//! ② 定位：逐文件读 + 用查询那条语义求谓词 ⇒ 命中行的文件内行号
//! ③ 位图写对象存储（**先对象**）
//! ④ WAL append + fsync（单条 DeletePayload —— 权威）
//! ⑤ catalog.apply_deletions（单快照原子）
//! ⑥ 刷新本进程的读缓存（否则"删完立刻查"还看得见）
//! ```
//!
//! 每一步的"为什么"：
//!
//! * **① 必须在 ② 之前**：删除只作用于**已提交文件**。刚写进来的行还在 chunk 里，
//!   不落盘就扫不到它们 —— 等它们落盘后会以"没被删掉"的样子出现（**复活**）。
//!   设计 §4.3 就是冲这个来的（"删刚插的行"是它明写的验收项）；
//! * **③ 在 ④ 之前**：崩溃在"对象已写、WAL 未提交"只会留下孤儿对象（孤儿清理回收，§7）；
//!   反过来（WAL 已提交、对象没写）就是**读侧拿不到位图** —— 那会让查询失败或复活，
//!   两种都不可接受；
//! * **⑤ 失败 ≠ 白删**：WAL 已经是权威（`ADR-3`），重启时 `replay_wal_dml` 会收敛。
//!   所以这里的错误消息必须**说清这个状态**（不然运维会以为"没删成功"而重试）；
//! * **⑥ 是可见性的那一半**：`§5.2` 明确要求删除路径**同步**刷新受影响表的缓存 ——
//!   否则"DELETE 返回成功、再查还在"（用户看到的删除是"要不要生效看缓存 TTL"）。
//!
//! # 本刀的范围（写清楚，别让调用方猜）
//!
//! * 只支持 **`DELETE FROM <单表> WHERE <谓词>`**。多表 / `USING` / `RETURNING` /
//!   `ORDER BY` / `LIMIT` 一律**明确拒绝**（没实现就说没实现）；
//! * **无 `WHERE` 的全表删不做**：设计 §7 定的口径是走**文件级下线**（`purge_table_files`）
//!   且"不生成 DV"，那条通路还没接线 ⇒ 这里明确拒绝并指向 `F.3d`；
//! * **`UPDATE` 的可见性口径**（`plan.md` F.7 决策 5，用户裁决）：**一个 op 承载、原子可见** ——
//!   两半（删除向量 + 新行文件）由 `CatalogOps::apply_update` 给**同一个快照号**，
//!   读侧看不到"删了没插"（少数据）或"插了没删"（重复计数）的中间态。
//!   代价：新行**不走 `Data` 攒批**，而是在 op 之前先落成一个文件（每个源文件一个产物）。

use arrow::datatypes::SchemaRef;
use sqlparser::ast::{
    Assignment, AssignmentTarget, Delete, Expr, FromTable, TableFactor, Update,
};
use uuid::Uuid;

use yuntun_model::dv::{DeletionEntry, DvBitmap, dv_object_path};
use yuntun_model::ops::qualified_name;
use yuntun_model::meta::FileManifest;
use yuntun_model::wal_record::{
    DeletePayload, FileDeletion, NewFilePayload, PurgePayload, Record, UpdatePayload,
};

use crate::{RawOutcome, SqlEngine, SqlError, session::SessionCtx};

impl SqlEngine {
    /// `DELETE FROM t WHERE …`（见模块文档的六步）。
    ///
    /// 外层只做一件事：**先把表级 DML 租约拿到手**（与合并互斥，设计 §6.1）——
    /// 删除的"定位 → 写位图 → 登记"必须相对**同一份基文件**成立：
    ///
    /// * 合并先跑、删除后跑 ⇒ 删除的位图锚在**已经转墓碑**的旧文件名上（惰性，删不掉东西）；
    /// * 删除先跑、合并后跑 ⇒ 合并在读基文件时**看不到**这份位图 ⇒ **已删的行被写进新文件**（复活）。
    ///
    /// 两个方向都不能放任 ⇒ 两边用**同一把表级租约**（`yuntun_model::meta::dml_lease_purpose`）。
    /// 拿不到就**当场拒绝**（不排队）：DELETE 是短操作，让调用方重试比在这里阻塞好。
    ///
    /// （表名解析两次是刻意的：外壳要表名才能取租约，本体要表名才能干活；`parse_delete_target` 是纯函数。）
    pub(crate) async fn execute_delete(
        &self,
        del: &Delete,
        session: &SessionCtx,
    ) -> Result<RawOutcome, SqlError> {
        self.check_write()?;
        let table = parse_delete_target(del, session)?;
        let lease = DmlLease::acquire(self.catalog.clone(), &table).await?;
        // **无 `WHERE` ⇒ 整表清除**（设计 §7）：走**文件级下线**，不生成任何 DV。
        // 分流放在这里（拿到租约之后）：两条路的外部纪律是同一把租约，只是内部形态不同。
        let out = if del.selection.is_none() {
            self.execute_purge(&table).await
        } else {
            self.execute_delete_inner(del, session).await
        };
        lease.release().await;
        out
    }

    /// **整表清除**（`F.3f`，设计 §7 的"无 `WHERE` 全表删"）。
    ///
    /// # 为什么它不该退化成"给每一行标 DV"
    ///
    /// 设计 §7 的原话是"**文件级与行级分属两层，非第二形态**"。整表删若逐行标 DV，
    /// 代价是位图大小 = 行数（十亿行的表就是十亿位的位图），而且之后还要靠合并一行行重写；
    /// 文件级下线是**目录里改几个字段**：文件整体标墓碑，读侧立刻看不到。
    ///
    /// # 三步（顺序即正确性）
    ///
    /// ```text
    /// ① 强制 flush：在途数据先落成文件 —— 否则它们不属于任何文件，purge 之后
    ///    攒批落盘 = **刚删掉的数据又回来了**（这一步是本刀最容易踩的）
    /// ② WAL append（一条 `PurgePayload`）：purge 只改目录、**数据对象一个不动**
    ///    ⇒ "重启后表是空的"从对象存储**推不出来**，只能靠这条记录重放
    /// ③ `purge_table_files`：文件级下线 + 悬挂 DV 同步 revoke（**同一个快照号**）
    /// ```
    ///
    /// 并发：与合并互斥（外层已持表级 DML 租约）—— 否则合并会读到 purge 之前的基文件、
    /// 把那些行写进新文件（**复活**）。
    ///
    /// 幂等键（`purge_id`）**每次执行都新生成**：SQL 语句没有"请求 id"这个概念，
    /// 同一个 id 只用于**重放**（`replay_wal_dml`）—— 于是"用户再执行一次 `DELETE FROM t`"
    /// 是**一次新的清除**（`DELETE` 本就该如此），而"重放同一条 WAL 记录"是空操作。
    async fn execute_purge(&self, table: &str) -> Result<RawOutcome, SqlError> {
        // 表必须存在（与带 WHERE 那条路一致：不存在的表报 `NotFound`，而不是"清了个空"）
        if self
            .catalog
            .table_schema(table)
            .await
            .map_err(SqlError::from_lake)?
            .is_none()
        {
            return Err(SqlError::NotFound(table.to_string()));
        }
        let ingest = self.ingest_side()?.clone();
        // ① 在途数据先落盘
        let flushed = ingest.force_flush(table).await.map_err(SqlError::from_lake)?;
        if flushed.chunks > 0 {
            tracing::debug!(
                table = %table,
                chunks = flushed.chunks,
                rows = flushed.rows,
                "整表清除：先把在途数据落盘（否则刚落盘的那批会「删不掉」）"
            );
        }
        let purge_id = format!("purge-{}", Uuid::now_v7());
        let epoch = ingest.chunks().liveness(table).epoch;
        // ② WAL（权威）：purge 只改目录 ⇒ 少了这条记录，内存元数据形态重启后整表复活
        ingest
            .wal
            .append(Record::Purge(PurgePayload {
                table: table.to_string(),
                purge_id: purge_id.clone(),
                schema_epoch: epoch,
            }))
            .await
            .map_err(SqlError::from_lake)?;
        // ③ 目录：文件级下线 + 悬挂 DV 同步 revoke（一个 op、一个快照）
        let out = self
            .catalog
            .purge_table_files(table, &purge_id)
            .await
            .map_err(|e| {
                SqlError::from_lake(yuntun_model::error::LakeError::Other(format!(
                    "整表清除已写入 WAL（purge_id={purge_id}）但没进目录：{e} —— \
                     重启后 `replay_wal_dml` 会把它收敛进来，**不要再清一次**"
                )))
            })?;
        self.query
            .catalog()
            .refresh(&self.catalog)
            .await
            .map_err(|e| SqlError::Internal(e.to_string()))?;
        tracing::info!(
            table = %table,
            purge_id = %purge_id,
            files = out.files,
            revoked = out.revoked,
            rows = out.rows,
            snapshot = out.snapshot,
            "整表清除完成（文件级下线，未生成删除向量）"
        );
        Ok(RawOutcome::Affected(out.rows as i64))
    }

    /// 六步本体（见模块文档）。
    async fn execute_delete_inner(
        &self,
        del: &Delete,
        session: &SessionCtx,
    ) -> Result<RawOutcome, SqlError> {
        self.check_write()?;
        // ① 语句形态：本刀只认单表 + 可选 WHERE（其余形态明确拒绝，别猜用户想要什么）
        let table = parse_delete_target(del, session)?;
        // 无 `WHERE` 在**外层**就分流去 `execute_purge`（整表清除，设计 §7）：
        // 走到这里的一定带谓词 —— 这条断言把"两层分流"钉在一起，加了新入口也不会漏
        let Some(predicate) = del.selection.as_ref() else {
            return Err(SqlError::Internal(
                "无 WHERE 的 DELETE 必须走 execute_purge（内层不该收到它）".into(),
            ));
        };
        let Some((schema, _version)) = self
            .catalog
            .table_schema(&table)
            .await
            .map_err(SqlError::from_lake)?
        else {
            return Err(SqlError::NotFound(table));
        };
        let ingest = self.ingest_side()?.clone();
        let store = ingest.store.clone();

        // ② 在途数据先落盘（否则刚插的行扫不到 ⇒ 落盘后复活）
        let flushed = ingest.force_flush(&table).await.map_err(SqlError::from_lake)?;
        if flushed.chunks > 0 {
            tracing::debug!(
                table = %table,
                chunks = flushed.chunks,
                rows = flushed.rows,
                "DELETE：先把在途数据落盘（否则会漏删刚插入的行）"
            );
        }

        // ③ 定位：逐文件求谓词，命中行的**文件内行号**
        let snapshot = self.catalog.current_snapshot().await;
        let files = self
            .catalog
            .list_visible_files(&table, snapshot, None)
            .await
            .map_err(SqlError::from_lake)?;
        // 已生效的 DV：新位图里**不该重复记**已删过的行（否则 `card`/受影响行数虚高）
        let existing = self.existing_bitmaps(&table, snapshot).await?;

        let mut dv_id = String::new();
        let mut deletions: Vec<FileDeletion> = Vec::new();
        let mut entries: Vec<DeletionEntry> = Vec::new();
        let mut affected: i64 = 0;
        for f in &files {
            let positions = locate(&store, f, &schema, predicate, existing.get(&f.file_path)).await?;
            if positions.is_empty() {
                continue;
            }
            if dv_id.is_empty() {
                // 一条 DELETE = 一个 id（跨文件共享；DV 对象路径 = `dv/<文件名>/<dv_id>.bin`）
                dv_id = format!("dv-{}", Uuid::now_v7());
            }
            let dv = DvBitmap::from_positions(positions.iter().copied());
            let bytes = dv.to_bytes();
            let store_path = dv_object_path(&f.file_path, &dv_id);
            // ③ 先对象（崩溃只会留下孤儿对象，由孤儿清理回收）
            yuntun_store::put_bytes(store.as_ref(), &store_path, bytes.clone())
                .await
                .map_err(SqlError::from_lake)?;
            affected += dv.card() as i64;
            deletions.push(FileDeletion {
                file_path: f.file_path.clone(),
                batch_id: f.batch_id.clone(),
                bitmap: bytes,
            });
            entries.push(DeletionEntry {
                dv_id: dv_id.clone(),
                table: table.clone(),
                file_path: f.file_path.clone(),
                batch_id: f.batch_id.clone(),
                applied_at: 0, // 由目录分配（同一批共用一个快照号）
                revoked_at: 0,
                card: dv.card() as u32,
                store_path,
            });
        }
        // 谓词一行都没命中：不写 WAL、不推快照（"什么也没发生"就不该有痕迹）
        if deletions.is_empty() {
            return Ok(RawOutcome::Affected(0));
        }

        // ④ WAL（权威）：单条记录承载这次删除的全部文件
        //    世代与写入侧的 `ChunkStore::liveness` 同源 —— 重放据此判定"是不是同一个表世代"
        let epoch = ingest.chunks().liveness(&table).epoch;
        ingest
            .wal
            .append(Record::Delete(DeletePayload {
                table: table.clone(),
                dv_id: dv_id.clone(),
                deletions,
                schema_epoch: epoch,
            }))
            .await
            .map_err(SqlError::from_lake)?;

        // ⑤ 目录（单快照原子）
        let file_count = entries.len();
        let snap = self.catalog
            .apply_deletions(entries)
            .await
            .map_err(|e| {
                SqlError::from_lake(yuntun_model::error::LakeError::Other(format!(
                    "删除已写入 WAL（dv_id={dv_id}）但没进目录：{e} —— \
                     重启后 `replay_wal_dml` 会把它收敛进来，**不要再删一次**"
                )))
            })?;

        // ⑥ 本进程的读缓存：删除必须**立刻**可见（§5.2）
        self.query
            .catalog()
            .refresh(&self.catalog)
            .await
            .map_err(|e| SqlError::Internal(e.to_string()))?;
        tracing::info!(
            table = %table,
            dv_id = %dv_id,
            files = file_count,
            rows = affected,
            snapshot = snap,
            "DELETE applied"
        );
        Ok(RawOutcome::Affected(affected))
    }

    /// 该表在当前快照下**已生效**的 DV 位图（`file_path → DvBitmap`）。
    ///
    /// 读不出来就**报错**：拿不到"已经删了哪些行"的真相时，写一份新位图会把已删的行再记一遍
    /// （`card` 与"受影响行数"虚高，且掩盖了"这个表的 DV 已经坏了"这件事 —— 读侧马上就会撞上）。
    async fn existing_bitmaps(
        &self,
        table: &str,
        snapshot: u64,
    ) -> Result<std::collections::HashMap<String, DvBitmap>, SqlError> {
        let entries = self
            .catalog
            .list_deletions(table, snapshot)
            .await
            .map_err(SqlError::from_lake)?;
        let store = self.ingest_side()?.store.clone();
        let mut out: std::collections::HashMap<String, DvBitmap> = std::collections::HashMap::new();
        for e in entries {
            let bytes = yuntun_store::get_bytes(store.as_ref(), &e.store_path)
                .await
                .map_err(SqlError::from_lake)?;
            let dv = DvBitmap::from_bytes(&bytes)
                .map_err(|err| SqlError::from_lake(yuntun_model::error::LakeError::Other(
                    format!("{err}（表 {table} 的 {}）", e.dv_id),
                )))?;
            out.entry(e.file_path.clone())
                .and_modify(|cur| cur.union_with(&dv))
                .or_insert(dv);
        }
        Ok(out)
    }
}

/// DELETE 期间持有的**表级 DML 租约**（与合并互斥）。
///
/// ⚠️ **粒度是表**（不是 `(table, shard)`）：设计 §6.1 允许"退化为表级锁"，见
/// `yuntun_model::meta::dml_lease_purpose` 的注释 —— 细粒度另开一刀（`§150.3`）。
/// 现在的取舍：**宁可粗一点，也不要交错**。
struct DmlLease {
    catalog: std::sync::Arc<dyn yuntun_catalog::CatalogOps>,
    purpose: String,
    holder: String,
    epoch: u64,
}

/// 租约时长：DELETE 是短操作（定位 + 写几个小对象 + 一次目录提交）；进程死掉时靠 TTL 释放。
const DML_LEASE_TTL_MS: u64 = 30_000;

impl DmlLease {
    async fn acquire(
        catalog: std::sync::Arc<dyn yuntun_catalog::CatalogOps>,
        table: &str,
    ) -> Result<Self, SqlError> {
        let purpose = yuntun_model::meta::dml_lease_purpose(table);
        let holder = format!("dml-{}", Uuid::now_v7());
        let grant = catalog
            .acquire_lease(
                &purpose,
                &holder,
                yuntun_ingest::now_ms(),
                DML_LEASE_TTL_MS,
            )
            .await
            .map_err(SqlError::from_lake)?;
        if !grant.granted {
            return Err(SqlError::Unsupported(format!(
                "另一处正在对表 {table} 做合并（表级 DML 租约被占用）：删除要等它结束再试 —— \
                 两者必须在同一份基文件上串行（合并会消费删除向量，设计 §6.1）"
            )));
        }
        Ok(Self {
            catalog,
            purpose,
            holder,
            epoch: grant.epoch,
        })
    }

    /// 归还（best-effort）：还失败也只影响"接手方要不要等 TTL"，不影响这次删除的结果。
    async fn release(self) {
        if let Err(e) = self
            .catalog
            .release_lease(&self.purpose, &self.holder, self.epoch)
            .await
        {
            tracing::warn!(error = %e, "释放删除租约失败（等待 TTL 过期即可）");
        }
    }
}

/// 解析 `DELETE` 的目标表（本刀只认单表、无 JOIN）。
fn parse_delete_target(del: &Delete, session: &SessionCtx) -> Result<String, SqlError> {
    if !del.tables.is_empty() {
        return Err(SqlError::Unsupported(
            "多表 DELETE（`DELETE t1 FROM …`）未支持".into(),
        ));
    }
    if del.using.is_some() {
        return Err(SqlError::Unsupported("DELETE … USING 未支持".into()));
    }
    if del.returning.is_some() || del.output.is_some() {
        return Err(SqlError::Unsupported("DELETE … RETURNING/OUTPUT 未支持".into()));
    }
    if !del.order_by.is_empty() || del.limit.is_some() {
        return Err(SqlError::Unsupported(
            "DELETE … ORDER BY / LIMIT 未支持（删除的可见性口径是谓词，不是顺序）".into(),
        ));
    }
    let FromTable::WithFromKeyword(from) = &del.from else {
        return Err(SqlError::Unsupported(
            "DELETE 的目标必须写成 `DELETE FROM <表>`".into(),
        ));
    };
    if from.len() != 1 {
        return Err(SqlError::Unsupported(
            "一次只支持删一张表（多表请分开写）".into(),
        ));
    }
    let twj = &from[0];
    if !twj.joins.is_empty() {
        return Err(SqlError::Unsupported("DELETE 里的 JOIN 未支持".into()));
    }
    let TableFactor::Table { name, .. } = &twj.relation else {
        return Err(SqlError::Unsupported(
            "DELETE 的目标必须是普通表名".into(),
        ));
    };
    let qualified = crate::sql::resolve_table_ref(name, session);
    if qualified.is_empty() {
        return Err(SqlError::Unsupported("invalid table name in DELETE".into()));
    }
    // 归一化为全限定名（目录里一律是全限定）
    let (ns, bare) = yuntun_model::ops::split_qualified(&qualified);
    Ok(qualified_name(if ns.is_empty() { session.schema() } else { ns }, bare))
}

impl SqlEngine {
    /// `UPDATE t SET … WHERE …`（`F.3e-2`）。
    ///
    /// # 七步（顺序即正确性）
    ///
    /// ```text
    /// ① 表级 DML 租约（与合并互斥：合并会重写基文件并消费删除向量）
    /// ② 强制 flush（在途数据先落盘 —— 否则刚插的行改不到）
    /// ③ 逐文件：一次扫描同时拿"命中行号"与"这些行的新值"（`locate_and_project`）
    ///    ＋ 扣掉**已删**的行（它们对用户不可见，改它们 = 复活）
    /// ④ 新行写成一个文件（每个源文件一个产物，继承它的 shard / time_window）
    /// ⑤ 位图写对象存储（先对象）
    /// ⑥ WAL append（**一条** `UpdatePayload`：删 + 插两半）
    /// ⑦ `apply_update` —— 两半**同一个快照号**（这就是"原子可见"）
    /// ```
    ///
    /// 为什么两半要进**一条** op：`plan.md` F.7 决策 5 定的是"读侧不可能看到中间态"。
    /// 分两步（先 `apply_deletions` 再 `commit_files`）必然落在两个快照上 ——
    /// 中间态要么少数据、要么**重复计数**，两种都是静默错。
    pub(crate) async fn execute_update(
        &self,
        upd: &Update,
        session: &SessionCtx,
    ) -> Result<RawOutcome, SqlError> {
        self.check_write()?;
        let table = parse_update_target(upd, session)?;
        let Some((schema, schema_version)) = self
            .catalog
            .table_schema(&table)
            .await
            .map_err(SqlError::from_lake)?
        else {
            return Err(SqlError::NotFound(table));
        };
        let Some(predicate) = upd.selection.as_ref() else {
            return Err(SqlError::Unsupported(
                "无 WHERE 的全表 UPDATE 未支持：它等于把整表重写一遍（设计 §7 只定了「全表删」\
                 走文件级下线）。请带谓词分批改，或 CREATE TABLE AS SELECT 重建"
                    .into(),
            ));
        };
        let sets = parse_assignments(&upd.assignments, &schema)?;
        let projection = projection_sql(&schema, &sets);

        let ingest = self.ingest_side()?.clone();
        let store = ingest.store.clone();
        // ① 与合并互斥（同一把表级租约 —— 与 DELETE 共用）
        let lease = DmlLease::acquire(self.catalog.clone(), &table).await?;
        let result = async {
            // ② 在途数据先落盘
            let flushed = ingest.force_flush(&table).await.map_err(SqlError::from_lake)?;
            if flushed.chunks > 0 {
                tracing::debug!(
                    table = %table, chunks = flushed.chunks, rows = flushed.rows,
                    "UPDATE：先把在途数据落盘（否则刚插入的行改不到）"
                );
            }

            let snapshot = self.catalog.current_snapshot().await;
            let files = self
                .catalog
                .list_visible_files(&table, snapshot, None)
                .await
                .map_err(SqlError::from_lake)?;
            // **已删的行不许被改**（它们对用户不可见；改它们等于把删除的行"更新回来"）
            let existing = self.existing_bitmaps(&table, snapshot).await?;

            let upd_id = format!("upd-{}", Uuid::now_v7());
            let mut deletions: Vec<FileDeletion> = Vec::new();
            let mut entries: Vec<DeletionEntry> = Vec::new();
            let mut new_files: Vec<NewFilePayload> = Vec::new();
            let mut manifests: Vec<FileManifest> = Vec::new();
            let mut affected: i64 = 0;

            for f in &files {
                let fmt = match f.file_path.rsplit('.').next() {
                    Some("parquet") => yuntun_format::DataFormat::Parquet,
                    Some("vortex") => yuntun_format::DataFormat::Vortex,
                    other => {
                        return Err(SqlError::Unsupported(format!(
                            "文件 {} 的格式不认识（{other:?}）：UPDATE 需要按行号定位",
                            f.file_path
                        )));
                    }
                };
                // ③ 一次扫描拿两半
                let got = yuntun_query::locate::locate_and_project(
                    &store,
                    &f.file_path,
                    fmt,
                    &schema,
                    &predicate.to_string(),
                    &projection,
                )
                .await
                .map_err(|e| SqlError::Internal(e.to_string()))?;
                if f.row_count != 0 && f.row_count != got.rows {
                    return Err(SqlError::Internal(format!(
                        "文件 {} 的行数与清单不符：清单记 {}、文件里 {} ⇒ 拒绝按「文件内行号」做更新",
                        f.file_path, f.row_count, got.rows
                    )));
                }
                let Some(batch) = got.batch else { continue };

                // 扣掉已删的行（positions 与 batch **逐行对齐**，见 `ProjectedRows` 的契约）
                let (positions, batch) = drop_already_deleted(
                    got.positions,
                    batch,
                    existing.get(&f.file_path),
                    &f.file_path,
                )?;
                if positions.is_empty() {
                    continue;
                }
                affected += positions.len() as i64;

                // ④ 新行写成一个文件（继承源文件的 shard / time_window）
                let new_batch_id = Uuid::now_v7().to_string();
                let new_time_window = if f.time_window.is_empty() {
                    "w"
                } else {
                    f.time_window.as_str()
                };
                let new_shard = if f.shard.is_empty() { "s0" } else { f.shard.as_str() };
                let (new_path, new_size, new_rows) = yuntun_format::write_batch(
                    &store,
                    &table,
                    new_shard,
                    new_time_window,
                    &new_batch_id,
                    &batch,
                    fmt,
                )
                .await
                .map_err(SqlError::from_lake)?;

                // ⑤ 位图写对象（**先对象**，崩溃只留孤儿）
                let dv = DvBitmap::from_positions(positions.iter().copied());
                let bytes = dv.to_bytes();
                let store_path = dv_object_path(&f.file_path, &upd_id);
                yuntun_store::put_bytes(store.as_ref(), &store_path, bytes.clone())
                    .await
                    .map_err(SqlError::from_lake)?;

                deletions.push(FileDeletion {
                    file_path: f.file_path.clone(),
                    batch_id: f.batch_id.clone(),
                    bitmap: bytes,
                });
                entries.push(DeletionEntry {
                    dv_id: upd_id.clone(),
                    table: table.clone(),
                    file_path: f.file_path.clone(),
                    batch_id: f.batch_id.clone(),
                    applied_at: 0, // 由目录分配（两半**共用一个**快照号）
                    revoked_at: 0,
                    card: dv.card() as u32,
                    store_path,
                });
                new_files.push(NewFilePayload {
                    file_path: new_path.clone(),
                    batch_id: new_batch_id.clone(),
                    row_count: new_rows,
                    file_size: new_size,
                    shard: new_shard.into(),
                    time_window: new_time_window.into(),
                    schema_version,
                });
                manifests.push(FileManifest {
                    file_path: new_path,
                    batch_id: new_batch_id,
                    row_count: new_rows,
                    file_size: new_size,
                    table: table.clone(),
                    shard: new_shard.into(),
                    time_window: new_time_window.into(),
                    schema_version,
                    ..Default::default()
                });
            }

            // 一行都没命中：什么都不发生（不写 WAL、不推快照）
            if entries.is_empty() {
                return Ok(RawOutcome::Affected(0));
            }

            // ⑥ WAL（权威）：一条记录承载两半
            let epoch = ingest.chunks().liveness(&table).epoch;
            ingest
                .wal
                .append(Record::Update(UpdatePayload {
                    table: table.clone(),
                    upd_id: upd_id.clone(),
                    deletions,
                    new_files,
                    schema_epoch: epoch,
                }))
                .await
                .map_err(SqlError::from_lake)?;

            // ⑦ 目录：**一个 op、一个快照号**（原子可见）
            let file_count = manifests.len();
            let snap = self
                .catalog
                .apply_update(entries, manifests, &upd_id)
                .await
                .map_err(|e| {
                    SqlError::from_lake(yuntun_model::error::LakeError::Other(format!(
                        "更新已写入 WAL（upd_id={upd_id}）但没进目录：{e} —— \
                         重启后 `replay_wal_dml` 会把它收敛进来，**不要再改一次**"
                    )))
                })?;
            self.query
                .catalog()
                .refresh(&self.catalog)
                .await
                .map_err(|e| SqlError::Internal(e.to_string()))?;
            tracing::info!(
                table = %table,
                upd_id = %upd_id,
                files = file_count,
                rows = affected,
                snapshot = snap,
                "UPDATE applied（两半同一个快照）"
            );
            Ok(RawOutcome::Affected(affected))
        }
        .await;
        lease.release().await;
        result
    }
}

/// 解析 `UPDATE` 的目标表（本刀只认单表、无 `FROM`、无 `RETURNING`）。
fn parse_update_target(upd: &Update, session: &SessionCtx) -> Result<String, SqlError> {
    if upd.from.is_some() {
        return Err(SqlError::Unsupported("UPDATE … FROM 未支持".into()));
    }
    if upd.returning.is_some() || upd.output.is_some() {
        return Err(SqlError::Unsupported(
            "UPDATE … RETURNING/OUTPUT 未支持".into(),
        ));
    }
    if !upd.order_by.is_empty() || upd.limit.is_some() {
        return Err(SqlError::Unsupported(
            "UPDATE … ORDER BY / LIMIT 未支持（更新的可见性口径是谓词，不是顺序）".into(),
        ));
    }
    if !upd.table.joins.is_empty() {
        return Err(SqlError::Unsupported("UPDATE 里的 JOIN 未支持".into()));
    }
    let TableFactor::Table { name, .. } = &upd.table.relation else {
        return Err(SqlError::Unsupported("UPDATE 的目标必须是普通表名".into()));
    };
    let qualified = crate::sql::resolve_table_ref(name, session);
    if qualified.is_empty() {
        return Err(SqlError::Unsupported("invalid table name in UPDATE".into()));
    }
    let (ns, bare) = yuntun_model::ops::split_qualified(&qualified);
    Ok(qualified_name(
        if ns.is_empty() { session.schema() } else { ns },
        bare,
    ))
}

/// `SET` 列表 → `(列名, 表达式 SQL)`；形状不对**明确拒绝**（不猜用户想要什么）。
fn parse_assignments(
    assignments: &[Assignment],
    schema: &SchemaRef,
) -> Result<Vec<(String, String)>, SqlError> {
    if assignments.is_empty() {
        return Err(SqlError::Unsupported("UPDATE 必须有 SET 子句".into()));
    }
    let mut out: Vec<(String, String)> = Vec::with_capacity(assignments.len());
    for a in assignments {
        let AssignmentTarget::ColumnName(name) = &a.target else {
            return Err(SqlError::Unsupported(
                "UPDATE … SET (a, b) = … 未支持（只支持逐列赋值）".into(),
            ));
        };
        let parts: Vec<&str> = name
            .0
            .iter()
            .filter_map(|p| p.as_ident())
            .map(|i| i.value.as_str())
            .collect();
        let col = match parts.as_slice() {
            // 不带表限定的列名；带限定（`t.v`）也接受（单表 UPDATE 下语义相同）
            [c] | [_, c] => (*c).to_string(),
            _ => {
                return Err(SqlError::Unsupported(format!(
                    "UPDATE 的赋值目标不支持：{name}"
                )));
            }
        };
        if !schema.fields().iter().any(|f| f.name() == &col) {
            return Err(SqlError::Unsupported(format!(
                "表里没有列 {col}（可更新的列：{:?}）",
                schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
            )));
        }
        if out.iter().any(|(c, _)| *c == col) {
            // 同一列赋两次：SQL 里第二次会赢，但那多半是写错了 —— 明确拒绝
            return Err(SqlError::Unsupported(format!("列 {col} 被赋值了两次")));
        }
        out.push((col, a.value.to_string()));
    }
    Ok(out)
}

/// 按**表 schema** 的列序生成 SELECT 列表：SET 过的列换表达式、其余列原样。
///
/// 列序必须与表 schema 一致：产物的 schema 就是新表的行 schema（写出去的 parquet 要能被读回来）。
fn projection_sql(schema: &SchemaRef, sets: &[(String, String)]) -> String {
    schema
        .fields()
        .iter()
        .map(|f| match sets.iter().find(|(c, _)| c == f.name()) {
            Some((_, expr)) => format!("({expr}) AS \"{}\"", f.name()),
            None => format!("\"{}\"", f.name()),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// 从"命中行"里扣掉**已经删过**的行：`positions` 与 `batch` 逐行对齐（`ProjectedRows` 的契约），
/// 所以同一个掩码同时作用于两者。
///
/// 为什么必须扣：已删的行对用户**不可见**，如果连它们一起更新，更新后的值会作为**新行**写进产物
/// —— 等于把删掉的行**更新复活**了（用户没要求、也看不出来）。
fn drop_already_deleted(
    positions: Vec<u32>,
    batch: arrow::record_batch::RecordBatch,
    existing: Option<&DvBitmap>,
    file_path: &str,
) -> Result<(Vec<u32>, arrow::record_batch::RecordBatch), SqlError> {
    let Some(dv) = existing else {
        return Ok((positions, batch));
    };
    if positions.len() != batch.num_rows() {
        return Err(SqlError::Internal(format!(
            "内部不一致：命中行数 {} 与结果批行数 {} 不等（文件 {file_path}）—— \
             两半不对齐就无法安全地扣掉已删行",
            positions.len(),
            batch.num_rows()
        )));
    }
    let keep: arrow::array::BooleanArray = positions
        .iter()
        .map(|p| !dv.contains(*p))
        .collect();
    let kept_positions: Vec<u32> = positions
        .iter()
        .zip(keep.iter())
        .filter(|(_, k)| k.unwrap_or(false))
        .map(|(p, _)| *p)
        .collect();
    let kept_batch = arrow::compute::filter_record_batch(&batch, &keep)
        .map_err(|e| SqlError::Internal(format!("扣掉已删行失败（{file_path}）：{e}")))?;
    Ok((kept_positions, kept_batch))
}

/// 在一个文件上定位命中行（`yuntun_query::locate`），并在写位图前把口径核对清楚。
async fn locate(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    f: &yuntun_model::meta::FileManifest,
    schema: &SchemaRef,
    predicate: &Expr,
    existing: Option<&DvBitmap>,
) -> Result<Vec<u32>, SqlError> {
    let fmt = match f.file_path.rsplit('.').next() {
        Some("parquet") => yuntun_format::DataFormat::Parquet,
        Some("vortex") => yuntun_format::DataFormat::Vortex,
        other => {
            return Err(SqlError::Unsupported(format!(
                "文件 {} 的格式不认识（{other:?}）：定位扫描无法给出「文件内行号」",
                f.file_path
            )));
        }
    };
    let got = yuntun_query::locate::locate_matching_rows(
        store,
        &f.file_path,
        fmt,
        schema,
        &predicate.to_string(),
    )
    .await
    .map_err(|e| SqlError::Internal(e.to_string()))?;

    // 行数口径核对：位图是"文件内行号"，清单里的行数与文件实际不符时**必须报错**
    //（继续写下去就是"按错的位置删行"—— 删错比删不动糟得多）
    if f.row_count != 0 && f.row_count != got.rows {
        return Err(SqlError::Internal(format!(
            "文件 {} 的行数与清单不符：清单记 {}、文件里 {} ⇒ 拒绝按「文件内行号」写删除向量",
            f.file_path, f.row_count, got.rows
        )));
    }
    if f.row_count == 0 && got.rows > 0 {
        tracing::warn!(
            file = %f.file_path,
            rows = got.rows,
            "清单行数为 0 但文件里有行：按文件实际行数定位（读侧的行选择用的是清单数字，注意口径）"
        );
    }
    // 去掉"已经删过"的行：新位图只记这次真的删掉的行
    match existing {
        None => Ok(got.positions),
        Some(done) => Ok(got
            .positions
            .into_iter()
            .filter(|p| !done.contains(*p))
            .collect()),
    }
}
