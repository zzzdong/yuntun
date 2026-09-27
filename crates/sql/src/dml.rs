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
//! * **`UPDATE` 不做**（设计 M3：DELETE + INSERT 一个 op 承载）。

use arrow::datatypes::SchemaRef;
use sqlparser::ast::{Delete, Expr, FromTable, TableFactor};
use uuid::Uuid;

use yuntun_model::dv::{DeletionEntry, DvBitmap, dv_object_path};
use yuntun_model::ops::qualified_name;
use yuntun_model::wal_record::{DeletePayload, FileDeletion, Record};

use crate::{RawOutcome, SqlEngine, SqlError, session::SessionCtx};

impl SqlEngine {
    /// `DELETE FROM t WHERE …`（见模块文档的六步）。
    pub(crate) async fn execute_delete(
        &self,
        del: &Delete,
        session: &SessionCtx,
    ) -> Result<RawOutcome, SqlError> {
        self.check_write()?;
        // ① 语句形态：本刀只认单表 + 可选 WHERE（其余形态明确拒绝，别猜用户想要什么）
        let table = parse_delete_target(del, session)?;
        let Some(predicate) = del.selection.as_ref() else {
            return Err(SqlError::Unsupported(
                "无 WHERE 的全表删未接线（设计 §7 定的口径是**文件级下线** purge_table_files + \
                 撤销悬挂 DV，不生成删除向量）—— 现在请用带 WHERE 的删除，或 DROP TABLE 重建"
                    .into(),
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
