//! **DML 的 WAL 重放**（`plan.md` F.3 / `delta-dml-design §1.1` ③）：启动时把删除收敛回目录。
//!
//! # 为什么删除也必须能从 WAL 重建
//!
//! `ADR-3`：**WAL 是写入事实的权威**。内存形态的 Catalog 是空启动的（`C5`），
//! 表清单靠 `replay_wal_ddl` 重建 —— 如果删除只在 Catalog 里，重启之后**已删的行会回来**
//! （用户以为删掉了、审计也看不出来）。所以 DELETE 的对象（位图）**内联**在 WAL 记录里，
//! 重放时**只靠 WAL** 重建 `DeletionEntry`：
//!
//! * `card` 从内联位图**量**出来（不信记录里的数字）；
//! * `store_path` 按 `§3.1` 的公式回推（`dv/<数据文件名>/<dv_id>.bin`）；
//! * **对象不重写**：它在 WAL 之前就已经落到对象存储（`§4.1` 的顺序），重放只重建目录。
//!
//! # 幂等与世代
//!
//! * **幂等**：`dv_id` 相同 ⇒ 目录侧按 `(dv_id, file_path)` 去重（重放两遍不写两份）；
//! * **`UPDATE` 也走这里**（`F.3e-2`）：`UpdatePayload` 的两半（删除向量 + 新行文件清单）
//!   由**一条 `apply_update`** 重建 —— 重放也必须保持"原子可见"：
//!   分两次调用就会在两个快照上落下两半，读侧照样能撞见中间态（少数据 / 重复计数）；
//! * **整表清除也走这里**（`F.3f`）：purge 只改目录（数据对象一个不动）⇒
//!   "重启后表是空的"**推不出来**，只能靠这条记录重放。重放按 `purge_id` 幂等 ——
//!   否则重放一条旧 purge 会把 purge 之后新写入的数据一起清掉（静默丢数据）。
//! * **世代校验**：`DeletePayload.schema_epoch` 与"该 seq 点上该表的世代"不符 ⇒
//!   **跳过并告警**（`DROP` → 同名重建之后，旧世代的删除不得挂到新表上，`plan.md` M0 ⑥）。

use std::sync::Arc;

use yuntun_catalog::CatalogOps;
use yuntun_model::dv::{DeletionEntry, DvBitmap, dv_object_path};
use yuntun_model::error::LakeError;
use yuntun_model::wal_record::{Record, ddl_op};
use yuntun_wal::writer::WalWriter;

/// 重放结果（可观测：跳过了多少条、建了多少条目）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplayDmlStats {
    /// 真正应用了的 DELETE 记录数
    pub applied: usize,
    /// 因为**世代不符 / 表已不在**而跳过的记录数
    pub skipped: usize,
    /// 重建的 `DeletionEntry` 条数（一条记录可覆盖多个文件）
    pub entries: usize,
    /// 重建的**新行文件**条数（`UPDATE` 的产物两半中的另一半 —— `F.3e-2`）
    pub new_files: usize,
    /// 重建的**整表清除**条数（`F.3f`）
    pub purged: usize,
}

/// **重放 WAL 里的 DELETE**（四段启动顺序的第 ③ 段，`delta-dml-design §1.1`）。
///
/// 必须在 ② `resume_recovered` **之后**（对空 Manifest 应用 DV 无意义）与
/// ④ `spawn_accumulator` **之前**（DV 必须在数据对查询可见之前就位）。
pub async fn replay_wal_dml(
    catalog: &Arc<dyn CatalogOps>,
    wal: &WalWriter,
) -> Result<ReplayDmlStats, LakeError> {
    let reader = yuntun_wal::reader::WalReader::new(wal.shard_dir());
    let records = reader.scan_from(0)?;

    // ① DDL 时间线：算"某条 seq 上这张表的 (存活, 世代)"——
    //    与写入侧 `ChunkStore::observe_ddl` 同源（**都是 WAL 里的 DDL 记录**），
    //    所以两边的世代含义一致（创建一次 +1，DROP 之后不再存活）。
    let mut timeline: Vec<(u64, u32, String)> = Vec::new();
    for (seq, rec) in &records {
        if let Record::Ddl(d) = rec {
            timeline.push((*seq, d.op, d.table.clone()));
        }
    }

    let mut stats = ReplayDmlStats::default();
    for (seq, rec) in records {
        // 两类 DML 记录都表达"某张表某个世代的事实" ⇒ 走同一道世代校验
        let (table, want_epoch, what) = match &rec {
            Record::Delete(p) => (p.table.clone(), p.schema_epoch, "DELETE"),
            Record::Update(p) => (p.table.clone(), p.schema_epoch, "UPDATE"),
            Record::Purge(p) => (p.table.clone(), p.schema_epoch, "PURGE"),
            _ => continue,
        };
        let (alive, epoch) = liveness_at(&timeline, seq, &table);
        // `schema_epoch == 0` = 老记录（本字段出现之前）：不校验（兼容），仍然重放
        if !alive || (want_epoch != 0 && epoch != want_epoch) {
            stats.skipped += 1;
            tracing::warn!(
                seq,
                table = %table,
                what,
                want_epoch,
                got_epoch = epoch,
                alive,
                "WAL 里的 DML 属于别的世代（或表已不在）：跳过 —— 不把旧世代的事实挂到新表上"
            );
            continue;
        }
        match rec {
            Record::Delete(p) => {
                // ② 重建条目（一次 DELETE 的全部文件**同一批**提交 ⇒ 单快照原子）
                let mut entries = Vec::with_capacity(p.deletions.len());
                for fd in &p.deletions {
                    let dv = decode_bitmap(fd, "DELETE", &p.dv_id)?;
                    if dv.is_empty() {
                        continue;
                    }
                    entries.push(DeletionEntry {
                        dv_id: p.dv_id.clone(),
                        table: table.clone(),
                        file_path: fd.file_path.clone(),
                        batch_id: fd.batch_id.clone(),
                        applied_at: 0, // 由目录分配（本次批量的同一个快照号）
                        revoked_at: 0,
                        card: dv.card() as u32,
                        store_path: dv_object_path(&fd.file_path, &p.dv_id),
                    });
                }
                if entries.is_empty() {
                    continue;
                }
                let n = entries.len();
                catalog.apply_deletions(entries).await?;
                stats.applied += 1;
                stats.entries += n;
            }
            Record::Update(p) => {
                // ① 两半一起重建、**一次提交**（`apply_update` 给两半同一个快照号）——
                //    这正是 `F.7` 决策 5 要的"重放之后仍然原子"
                let mut entries = Vec::with_capacity(p.deletions.len());
                for fd in &p.deletions {
                    let dv = decode_bitmap(fd, "UPDATE", &p.upd_id)?;
                    if dv.is_empty() {
                        continue;
                    }
                    entries.push(DeletionEntry {
                        dv_id: p.upd_id.clone(),
                        table: table.clone(),
                        file_path: fd.file_path.clone(),
                        batch_id: fd.batch_id.clone(),
                        applied_at: 0,
                        revoked_at: 0,
                        card: dv.card() as u32,
                        store_path: dv_object_path(&fd.file_path, &p.upd_id),
                    });
                }
                let files: Vec<yuntun_model::meta::FileManifest> = p
                    .new_files
                    .iter()
                    .map(|nf| yuntun_model::meta::FileManifest {
                        file_path: nf.file_path.clone(),
                        batch_id: nf.batch_id.clone(),
                        row_count: nf.row_count,
                        file_size: nf.file_size,
                        table: table.clone(),
                        shard: nf.shard.clone(),
                        time_window: nf.time_window.clone(),
                        schema_version: nf.schema_version,
                        // 状态机在 `apply_update` 里补 `valid_from`/`status`（同一个快照号）
                        ..Default::default()
                    })
                    .collect();
                if entries.is_empty() && files.is_empty() {
                    continue;
                }
                let n_dv = entries.len();
                let n_files = files.len();
                catalog.apply_update(entries, files, &p.upd_id).await?;
                stats.applied += 1;
                stats.entries += n_dv;
                stats.new_files += n_files;
            }
            Record::Purge(p) => {
                // **一条 op、一个快照**：文件级下线 + 悬挂 DV 同步 revoke（状态机里做）。
                // 幂等靠 `purge_id`（状态机记着）—— 这条**必须**成立：重放一条旧 purge
                // 若再生效，会把 purge 之后新写入的数据一起清掉（静默丢数据）。
                let out = catalog.purge_table_files(&table, &p.purge_id).await?;
                stats.applied += 1;
                stats.purged += 1;
                tracing::info!(
                    seq,
                    table = %table,
                    purge_id = %p.purge_id,
                    files = out.files,
                    revoked = out.revoked,
                    rows = out.rows,
                    "重放整表清除"
                );
            }
            _ => continue,
        }
    }
    if stats.applied > 0 || stats.skipped > 0 {
        tracing::info!(
            applied = stats.applied,
            skipped = stats.skipped,
            entries = stats.entries,
            new_files = stats.new_files,
            purged = stats.purged,
            "replay_wal_dml done"
        );
    }
    Ok(stats)
}

/// 解一份内联位图；解不开**直接报错**（带着损坏的删除向量对外服务 = 已删的行复活）。
fn decode_bitmap(
    fd: &yuntun_model::wal_record::FileDeletion,
    what: &str,
    id: &str,
) -> Result<DvBitmap, LakeError> {
    DvBitmap::from_bytes(&fd.bitmap).map_err(|e| {
        LakeError::Other(format!(
            "重放 {what}（id={id}）时位图解不开：{e} —— \
             带着损坏的删除向量对外服务 = 已删的行复活，拒绝继续"
        ))
    })
}

/// 在 DDL 时间线上求：位置 `seq`（含）之前，表 `table` 的 (存活, 世代)。
///
/// 与 `pipeline.rs` 的同名函数**同一套语义**（那份服务于批次恢复，这份服务于 DML 重放）；
/// 没有任何 DDL 记录时视为"存活、世代 0"（兼容不经 WAL DDL 直接建表的调用方）。
fn liveness_at(timeline: &[(u64, u32, String)], seq: u64, table: &str) -> (bool, u64) {
    let mut exists = true;
    let mut epoch = 0u64;
    for (s, op, t) in timeline {
        if *s > seq || t != table {
            continue;
        }
        match *op {
            ddl_op::CREATE_TABLE => {
                exists = true;
                epoch += 1;
            }
            ddl_op::DROP_TABLE => exists = false,
            _ => {}
        }
    }
    (exists, epoch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn liveness_counts_creates_and_drops() {
        let tl = vec![
            (1u64, ddl_op::CREATE_TABLE, "public.t".to_string()),
            (5, ddl_op::DROP_TABLE, "public.t".to_string()),
            (9, ddl_op::CREATE_TABLE, "public.t".to_string()),
        ];
        assert_eq!(liveness_at(&tl, 0, "public.t"), (true, 0), "没有 DDL ⇒ 存活、世代 0");
        assert_eq!(liveness_at(&tl, 1, "public.t"), (true, 1));
        assert_eq!(liveness_at(&tl, 6, "public.t"), (false, 1), "DROP 之后不存活");
        assert_eq!(liveness_at(&tl, 9, "public.t"), (true, 2), "同名重建 ⇒ 世代 +1");
        assert_eq!(liveness_at(&tl, 9, "public.other"), (true, 0), "别的表不受影响");
    }
}
