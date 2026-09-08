//! Flush 全流程：三级状态机（详细设计 §5.4）。
//!
//! ```text
//! ③ 生成 batch_id（C1：随机 UUIDv7，不是内容哈希 —— ADR-4）
//!    → WAL: BatchPending
//! ④ 写 S3 → WAL: BatchS3Written
//! ⑤ 提交 Meta（幂等）→ WAL: BatchCommitted（进入终态）
//! ```

use crate::accumulator::WindowGroup;
use yuntun_catalog::CatalogOps;
use yuntun_model::batch::{apply_record, BatchStateMap};
use yuntun_model::error::LakeError;
use yuntun_model::meta::compute_stats_lite;
use yuntun_model::ops::CommitFilesRequest;
use yuntun_model::wal_record::{
    BatchAbortPayload, BatchCommittedPayload, BatchPendingPayload, BatchS3WrittenPayload, Record,
};
use yuntun_wal::writer::WalWriter;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// 运行期批次追踪器：维护 live BatchState（与 WAL 记录同步），
/// 同时作为 WAL 超时监控的 [`BatchStateView`]。
#[derive(Default)]
pub struct LiveBatchTracker {
    pub states: Mutex<BatchStateMap>,
}

impl LiveBatchTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// WAL 记录追加后同步更新追踪器（保证监控视图与 WAL 一致）。
    pub fn observe(&self, rec: &Record) {
        let mut m = self.states.lock().unwrap();
        apply_record(&mut m, rec, 0);
    }

    /// 从崩溃恢复结果填充初始状态。
    pub fn load_from(&self, recovery: &yuntun_wal::recovery::Recovery) {
        let mut m = self.states.lock().unwrap();
        m.states.extend(recovery.states.states.clone());
    }

    pub fn non_terminal_ids(&self) -> Vec<String> {
        self.states
            .lock()
            .unwrap()
            .states
            .values()
            .filter(|s| !s.is_terminal())
            .map(|s| s.batch_id.clone())
            .collect()
    }
}

impl yuntun_wal::cleanup::BatchStateView for LiveBatchTracker {
    fn non_terminal(&self) -> Vec<yuntun_model::batch::BatchState> {
        self.states
            .lock()
            .unwrap()
            .states
            .values()
            .filter(|s| !s.is_terminal())
            .cloned()
            .collect()
    }
}

/// flush 依赖集合。
pub struct FlushDeps {
    pub wal: WalWriter,
    pub catalog: Arc<dyn CatalogOps>,
    pub store: Arc<dyn object_store::ObjectStore>,
    pub format: yuntun_format::DataFormat,
    pub tracker: Arc<LiveBatchTracker>,
}

#[derive(Debug, Clone)]
pub struct FlushOutcome {
    pub batch_id: String,
    pub file_path: String,
    pub file_size: u64,
    pub row_count: u64,
    pub schema_version: u64,
    pub snapshot: u64,
}

/// flush 一个攒批组（详细设计 §5.4）。
///
/// # 失败语义
/// 任何中间步骤失败：批次保持非终态（Pending / S3Written），
/// 由 WAL 超时监控（§5.3.6.1）最终 abort；已写 S3 的文件成为孤儿，由孤儿清理回收。
/// **绝不**在中途写 BatchCommitted。
pub async fn flush_batch(
    group: WindowGroup,
    deps: &FlushDeps,
) -> Result<FlushOutcome, LakeError> {
    flush_batch_with_id(group, deps, None).await
}

/// 同 [`flush_batch`]，但允许指定 batch_id（恢复场景：§5.6 复用原 batch_id，
/// Meta 按 batch_id 幂等，保证重放安全）。正常路径必须传 None（ADR-4 随机 UUIDv7）。
pub async fn flush_batch_with_id(
    group: WindowGroup,
    deps: &FlushDeps,
    reuse_batch_id: Option<String>,
) -> Result<FlushOutcome, LakeError> {
    // ③ 生成 batch_id
    // ADR-4: batch_id is random UUIDv7, NOT derived from content.
    // Idempotency is guaranteed by BatchStateStore + client_request_id (§7.3),
    // NOT by batch_id determinism. Do NOT "optimize" this into a content hash.
    // 例外：恢复路径（§5.6）复用原 batch_id —— Meta 幂等保证重放安全。
    let batch_id = reuse_batch_id.unwrap_or_else(|| Uuid::now_v7().to_string());

    // 合并 payload（WAL 内 Arrow IPC 解码 + schema 对齐 + 排序留给查询层/Compaction）
    let (merged, schema_version) = merge_payloads(&group.payloads)?;
    let row_count = merged.num_rows() as u64;
    let now = crate::accumulator::now_ms();

    // WAL: BatchPending
    let pending = Record::BatchPending(BatchPendingPayload {
        batch_id: batch_id.clone(),
        shard: group.shard.clone(),
        window: group.window.clone(),
        wal_seq_start: group.first_seq,
        wal_seq_end: group.first_seq + group.payloads.len() as u64,
        schema_version,
        client_request_id: String::new(),
        created_at_ms: now,
        row_count,
    });
    deps.wal.append(pending.clone()).await?;
    deps.tracker.observe(&pending);

    // ④ 编码写 S3
    let s3_res = write_group_to_s3(deps, &group, &batch_id, &merged, schema_version).await;
    let (file_path, file_size) = match s3_res {
        Ok(x) => x,
        Err(e) => {
            // 保持非终态，等超时监控 abort（幂等重试场景见 §5.6）
            tracing::error!(batch_id = %batch_id, error = %e, "s3 write failed, batch left pending");
            return Err(e);
        }
    };

    // WAL: BatchS3Written
    let s3written = Record::BatchS3Written(BatchS3WrittenPayload {
        batch_id: batch_id.clone(),
        s3_paths: vec![file_path.clone()],
        s3_upload_id: String::new(), // MVP 单段写入；Multipart 见 §7.4（阶段 1 扩展）
        file_size,
    });
    deps.wal.append(s3written.clone()).await?;
    deps.tracker.observe(&s3written);

    // ⑤ 提交 Meta（幂等，§6.4）
    let files = vec![yuntun_model::meta::FileManifest {
        file_path: file_path.clone(),
        batch_id: batch_id.clone(),
        row_count,
        file_size,
        stats: Some(compute_stats_lite(
            &merged,
            &yuntun_model::meta::default_sort_columns(&merged.schema()),
        )?),
        ..Default::default()
    }];
    let resp = deps
        .catalog
        .commit_files(CommitFilesRequest {
            table: group.table.clone(),
            batch_id: batch_id.clone(),
            client_request_id: None,
            shard: group.shard.clone(),
            time_window: group.window.clone(),
            files,
            schema_version,
            row_count,
        })
        .await?;

    // WAL: BatchCommitted（进入终态）
    let committed = Record::BatchCommitted(BatchCommittedPayload {
        batch_id: batch_id.clone(),
    });
    deps.wal.append(committed.clone()).await?;
    deps.tracker.observe(&committed);

    Ok(FlushOutcome {
        batch_id,
        file_path,
        file_size,
        row_count,
        schema_version,
        snapshot: resp.snapshot,
    })
}

/// 恢复场景：S3Written 状态的批次只重新 Commit（Meta 按 batch_id 幂等，§5.6）。
pub async fn commit_recovered_batch(
    deps: &FlushDeps,
    st: &yuntun_model::batch::BatchState,
) -> Result<u64, LakeError> {
    let files = st
        .s3_paths
        .iter()
        .map(|p| yuntun_model::meta::FileManifest {
            file_path: p.clone(),
            batch_id: st.batch_id.clone(),
            row_count: st.row_count,
            ..Default::default()
        })
        .collect();
    let resp = deps
        .catalog
        .commit_files(CommitFilesRequest {
            table: String::new(), // 恢复路径：由 Meta 端 batch_id 幂等兜底
            batch_id: st.batch_id.clone(),
            client_request_id: st.client_request_id.clone(),
            shard: st.shard.clone(),
            time_window: st.time_window.clone(),
            files,
            schema_version: st.schema_version,
            row_count: st.row_count,
        })
        .await?;
    let rec = Record::BatchCommitted(BatchCommittedPayload {
        batch_id: st.batch_id.clone(),
    });
    deps.wal.append(rec.clone()).await?;
    deps.tracker.observe(&rec);
    Ok(resp.snapshot)
}

/// 恢复场景：显式放弃（batch_id 复用已不可能，数据由重试端重新提交）。
pub async fn abort_batch(deps: &FlushDeps, batch_id: &str) -> Result<(), LakeError> {
    let rec = Record::BatchAbort(BatchAbortPayload {
        batch_id: batch_id.to_string(),
    });
    deps.wal.append(rec.clone()).await?;
    deps.tracker.observe(&rec);
    Ok(())
}

/// 合并一组 WAL Data payload → 单个 RecordBatch。
/// 不同 schema_version 的 payload：按列名对齐，缺失列填 null（§6.8 的查询侧同理）。
fn merge_payloads(
    payloads: &[yuntun_model::wal_record::DataPayload],
) -> Result<(arrow::record_batch::RecordBatch, u64), LakeError> {
    if payloads.is_empty() {
        return Err(LakeError::Other("empty batch group".into()));
    }
    let mut batches = Vec::with_capacity(payloads.len());
    let mut max_version = 0u64;
    for p in payloads {
        let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(&p.batch_ipc), None)
            .map_err(|e| LakeError::Other(format!("wal ipc decode: {e}")))?;
        for b in reader {
            let b = b.map_err(|e| LakeError::Other(format!("wal ipc read: {e}")))?;
            max_version = max_version.max(p.schema_version);
            batches.push((p.schema_version, b));
        }
    }
    // 目标 schema：取最高版本的 schema
    batches.sort_by_key(|(v, _)| *v);
    let target_schema = batches.last().unwrap().1.schema();
    let aligned: Vec<arrow::record_batch::RecordBatch> = batches
        .into_iter()
        .map(|(_, b)| align_batch(&b, &target_schema))
        .collect::<Result<Vec<_>, _>>()?;

    let merged = arrow::compute::concat_batches(&target_schema, &aligned)
        .map_err(|e| LakeError::Other(format!("concat: {e}")))?;
    Ok((merged, max_version))
}

/// 列名对齐：缺失列填 null；列序重排到目标 schema；类型不一致时按提升格 cast。
fn align_batch(
    batch: &arrow::record_batch::RecordBatch,
    target: &arrow::datatypes::SchemaRef,
) -> Result<arrow::record_batch::RecordBatch, LakeError> {
    use arrow::array::new_null_array;
    use arrow::compute::cast;
    let mut cols = Vec::with_capacity(target.fields().len());
    for f in target.fields() {
        match batch.schema().column_with_name(f.name()) {
            Some((idx, bf)) => {
                let arr = batch.column(idx);
                if bf.data_type() == f.data_type() {
                    cols.push(arr.clone());
                } else {
                    // 类型宽化 cast（Int32→Int64→Float64）；不兼容时 cast 报错 → 整批失败
                    cols.push(cast(arr, f.data_type()).map_err(|e| {
                        LakeError::Other(format!(
                            "column {} cast {:?} -> {:?}: {e}",
                            f.name(),
                            bf.data_type(),
                            f.data_type()
                        ))
                    })?);
                }
            }
            None => cols.push(new_null_array(f.data_type(), batch.num_rows())),
        }
    }
    arrow::record_batch::RecordBatch::try_new(target.clone(), cols)
        .map_err(|e| LakeError::Other(format!("align: {e}")))
}

async fn write_group_to_s3(
    deps: &FlushDeps,
    group: &WindowGroup,
    batch_id: &str,
    merged: &arrow::record_batch::RecordBatch,
    schema_version: u64,
) -> Result<(String, u64), LakeError> {
    let _ = schema_version;
    let (path, size, _rows) = yuntun_format::write_batch(
        &deps.store,
        &group.table,
        &group.shard,
        &group.window,
        batch_id,
        merged,
        deps.format,
    )
    .await?;
    Ok((path, size))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(schema: &arrow::datatypes::SchemaRef, rows: i64) -> arrow::record_batch::RecordBatch {
        use arrow::array::Int64Array;
        arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![rows; 2]))],
        )
        .unwrap()
    }

    #[test]
    fn align_fills_missing_columns_with_null() {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc as SArc;
        let v1 = SArc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let v2 = SArc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let b1 = batch(&v1, 1);
        let merged_schema = v2.clone();
        let out = align_batch(&b1, &merged_schema).unwrap();
        assert_eq!(out.num_columns(), 2);
        use arrow::array::Array;
        assert_eq!(out.column(1).null_count(), out.num_rows()); // 新列全 null
    }

    #[test]
    fn align_widens_int32_to_int64() {
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::array::Int32Array;
        use std::sync::Arc as SArc;
        let s32 = SArc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let s64 = SArc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let b = arrow::record_batch::RecordBatch::try_new(
            s32,
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let out = align_batch(&b, &s64).unwrap();
        assert!(out.column(0).data_type() == &DataType::Int64);
    }
}
