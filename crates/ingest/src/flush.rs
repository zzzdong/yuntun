//! Flush 全流程：三级状态机（详细设计 §5.4）+ chunk 层衔接（架构 §5）。
//!
//! ```text
//! ③ 生成 batch_id（C1：随机 UUIDv7，不是内容哈希 —— ADR-4）
//!    → WAL: BatchPending
//! ④ 写对象存储 → WAL: BatchS3Written
//! ⑤ 提交 Meta（幂等）→ WAL: BatchCommitted（进入终态）
//! ```
//!
//! ## 与 chunk 层的边界
//! flush **不碰 chunk 状态**：入参是 [`ChunkFlushInput`]（chunk 的数据快照），
//! 成功与否由调用方（攒批循环）决定是否 `mark_committed`。
//! 这样单次 flush 失败**无需回滚** chunk 状态 —— 批次保持非终态、chunk 仍可查，
//! 排除了短暂"数据不可见"的窗口（架构 §4.5 无空洞）。

use crate::accumulator::now_ms;
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use yuntun_catalog::CatalogOps;
use yuntun_chunk::store::ChunkFlushInput;
use yuntun_model::arrow_util::align_batch;
use yuntun_model::batch::{apply_record, BatchStateMap};
use yuntun_model::error::LakeError;
use yuntun_model::meta::compute_stats_lite;
use yuntun_model::ops::CommitFilesRequest;
use yuntun_model::wal_record::{
    BatchAbortPayload, BatchCommittedPayload, BatchPendingPayload, BatchS3WrittenPayload, DataPayload,
    Record,
};
use yuntun_wal::writer::WalWriter;

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

    /// 与 WAL 保持一致：`BatchAbort` 是**终态**（C4），`apply_record` 会移除该状态。
    ///
    /// 少了这一步，"已被放弃的批次"会一直留在 `non_terminal()` 里，
    /// 它的 `wal_seq_range` 会永远挡住 segment 释放（见 trait 上的说明）。
    fn note_abort(&self, batch_id: &str) {
        self.observe(&Record::BatchAbort(BatchAbortPayload {
            batch_id: batch_id.to_string(),
        }));
    }
}

/// flush 依赖集合（**只含外部资源**：WAL / Catalog / 对象存储 / 批次追踪器）。
pub struct FlushDeps {
    pub wal: WalWriter,
    pub catalog: Arc<dyn CatalogOps>,
    pub store: Arc<dyn object_store::ObjectStore>,
    pub format: yuntun_format::DataFormat,
    pub tracker: Arc<LiveBatchTracker>,
    /// 本实例标识：写入 `FileManifest.source_instance`（架构 §4.4 冷热边界按实例切分）
    pub instance_id: String,
}

#[derive(Debug, Clone)]
pub struct FlushOutcome {
    pub batch_id: String,
    pub file_path: String,
    pub file_size: u64,
    pub row_count: u64,
    pub schema_version: u64,
    pub snapshot: u64,
    /// 该文件覆盖的 WAL seq 半开区间（WAL 回收点语义，S1-8）
    pub wal_seq_range: std::ops::Range<u64>,
}

/// flush 一个 chunk（详细设计 §5.4）。
///
/// # 失败语义
/// 任何中间步骤失败：批次保持非终态（Pending / S3Written），
/// 由 WAL 超时监控（§5.3.6.1）最终 abort；已写 S3 的文件成为孤儿，由孤儿清理回收。
/// **绝不**在中途写 BatchCommitted。
pub async fn flush_chunk(
    input: &ChunkFlushInput,
    deps: &FlushDeps,
) -> Result<FlushOutcome, LakeError> {
    flush_chunk_with_id(input, deps, None).await
}

/// 同 [`flush_chunk`]，但允许指定 batch_id（恢复场景：§5.6 复用原 batch_id，
/// Meta 按 batch_id 幂等，保证重放安全）。正常路径必须传 None（ADR-4 随机 UUIDv7）。
pub async fn flush_chunk_with_id(
    input: &ChunkFlushInput,
    deps: &FlushDeps,
    reuse_batch_id: Option<String>,
) -> Result<FlushOutcome, LakeError> {
    // ③ 生成 batch_id
    // ADR-4: batch_id is random UUIDv7, NOT derived from content.
    // Idempotency is guaranteed by BatchStateStore + client_request_id (§7.3),
    // NOT by batch_id determinism. Do NOT "optimize" this into a content hash.
    // 例外：恢复路径（§5.6）复用原 batch_id —— Meta 幂等保证重放安全。
    let batch_id = reuse_batch_id.unwrap_or_else(|| Uuid::now_v7().to_string());

    let merged = merge_batches(&input.batches, &input.schema)?;
    let row_count = merged.num_rows() as u64;
    let now = now_ms();

    // WAL: BatchPending
    // ⚠️ wal_seq_end 必须是**精确 exclusive 右界**（= 该 chunk 最后一条 Data 的 seq + 1）：
    // chunk 内 seq 因交错写入存在空洞，`first_seq + payloads.len()` 会把右界算小
    // → Pending 重做 `scan_range` 少读尾部 Data。全链路统一半开 [start, end)。
    let wal_seq_range = input.wal_seq_range.clone();
    let pending = Record::BatchPending(BatchPendingPayload {
        batch_id: batch_id.clone(),
        table: input.shard.table.clone(),
        shard: input.shard.shard.clone(),
        window: input.shard.window.clone(),
        wal_seq_start: wal_seq_range.start,
        wal_seq_end: wal_seq_range.end,
        schema_version: input.schema_version,
        client_request_id: String::new(),
        created_at_ms: now,
        row_count,
    });
    deps.wal.append(pending.clone()).await?;
    deps.tracker.observe(&pending);

    // ④ 编码写对象存储
    let (file_path, file_size) = match write_to_object_store(deps, input, &batch_id, &merged).await {
        Ok(x) => x,
        Err(e) => {
            // 保持非终态，等超时监控 abort（幂等重试场景见 §5.6）
            tracing::error!(batch_id = %batch_id, error = %e, "object store write failed, batch left pending");
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

    // ④.5 收集本批次的**幂等键集合**（§27.5 遗留 #1）
    let keys = collect_idempotency_keys(deps, wal_seq_range.start, wal_seq_range.end)?;

    // ⑤ 提交 Meta（幂等，§6.4）
    //
    // 提交时刻**在构造 manifest 时打点**（= 发出提交请求的时刻，误差 = CommitFiles 往返）；
    // 不这么做就没法把请求带出去。R3 换 gRPC 后仍须由**发起方**（leader）打点：
    // 状态机要在所有副本上确定性地应用同一份 manifest，时间戳不能各自取现在。
    let committed_at_ms = now_ms();
    let files = vec![yuntun_model::meta::FileManifest {
        file_path: file_path.clone(),
        batch_id: batch_id.clone(),
        row_count,
        file_size,
        schema_version: input.schema_version,
        table: input.shard.table.clone(),
        shard: input.shard.shard.clone(),
        time_window: input.shard.window.clone(),
        // 逻辑分区身份与物理文件身份**分开记录**（架构 §2.3）
        partition_key: input.shard.window.clone(),
        // 冷热边界按实例切分（架构 §4.4）
        source_instance: deps.instance_id.clone(),
        stats: Some(compute_stats_lite(
            &merged,
            &yuntun_model::meta::default_sort_columns(&merged.schema()),
        )?),
        // 写侧结束时刻（**chunk 的真实封口时刻**，由 chunk 层带出）
        // → 与 committed_at_ms 之差即"封口到持久化"的实际耗时，即对外承诺的上界口径
        sealed_at_ms: input.sealed_at_ms,
        // 封口原因 + 当时水位档位（`operation-log §34.3`：让"文件为什么这么小"可查，不再靠排除法）
        seal_reason: input
            .seal_reason
            .map(|r| r.as_str().to_string())
            .unwrap_or_default(),
        seal_pressure: input
            .pressure_at_seal
            .map(|p| format!("{p:?}"))
            .unwrap_or_default(),
        committed_at_ms,
        ..Default::default()
    }];
    let resp = deps
        .catalog
        .commit_files(CommitFilesRequest {
            table: input.shard.table.clone(),
            batch_id: batch_id.clone(),
            // 单键字段刻意留 None：**一个 chunk 通常聚合多个幂等键**（多条 Data 记录），
            // 没有"那个键"可填；权威语义在下面的**键集合**（R3 S3-5）。
            client_request_id: None,
            // **本批次覆盖的幂等键集合**（从 WAL 派生，见 `collect_idempotency_keys`）：
            // 集合中任一键已登记 → Catalog 侧整次判重（§27.5 遗留 #1 的关闭方式）。
            client_request_ids: keys,
            shard: input.shard.shard.clone(),
            time_window: input.shard.window.clone(),
            files,
            schema_version: input.schema_version,
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
        schema_version: input.schema_version,
        snapshot: resp.snapshot,
        wal_seq_range,
    })
}

/// 恢复场景（M0）：终态批次按**原 batch_id 与既有对象**重建内存 Catalog（§5.6）。
///
/// 与旧 `commit_recovered_batch` 的区别：**不追加 WAL** —— 原 `BatchCommitted` 记录
/// 已表达终态，重提交只是重建内存 Catalog（C5）；每次重启都重复 append 会让 WAL
/// 随历史线性膨胀（delta-dml-design §1.1 R12）。幂等：Meta 按 batch_id 幂等；
/// IdempotencyRecord 由 commit_files 按 client_request_id 重新登记（受 TTL 约束）。
pub async fn recommit_into_catalog(
    deps: &FlushDeps,
    st: &yuntun_model::batch::BatchState,
    table: &str,
) -> Result<u64, LakeError> {
    // MVP 单文件全量输出：唯一文件时可恢复真实 file_size
    // （Manifest file_size=0 会让 Parquet scan 的 footer 范围读失败）。
    let file_size = if st.s3_paths.len() == 1 {
        st.file_size
    } else {
        0
    };
    let files = st
        .s3_paths
        .iter()
        .map(|p| yuntun_model::meta::FileManifest {
            file_path: p.clone(),
            batch_id: st.batch_id.clone(),
            row_count: st.row_count,
            file_size,
            schema_version: st.schema_version,
            table: table.to_string(),
            shard: st.shard.clone(),
            time_window: st.time_window.clone(),
            partition_key: st.time_window.clone(),
            source_instance: deps.instance_id.clone(),
            // 恢复重提交：`sealed_at_ms` 是原 flush 的时刻，WAL 里没记（重提交不追加记录，
            // 见本函数文档），故留 0 —— 调用方不应把恢复路径的该项当延迟样本。
            committed_at_ms: now_ms(),
            ..Default::default()
        })
        .collect();
    let resp = deps
        .catalog
        .commit_files(CommitFilesRequest {
            // 阶段 0 重启后 MemoryCatalog 为空：重提交必须带正确表名，
            // 否则文件 Manifest table 为空、查询永不可见（阶段 1 gRPC Meta
            // 按 batch_id 幂等兜底后此字段仅作记录）。
            table: table.to_string(),
            batch_id: st.batch_id.clone(),
            client_request_id: st.client_request_id.clone(),
            // 恢复重提交同样从 WAL 派生键集合（`BatchState.wal_seq_range` 一直在记）。
            // ⚠️ 尽力而为：若该区间所在的 WAL 段已被回收，则只能退回 `batch_id` 幂等
            //（数据不会重复写，只是"同键重试"可能漏判 —— 属已知边界）。
            client_request_ids: collect_idempotency_keys(deps, st.wal_seq_range.0, st.wal_seq_range.1)
                .unwrap_or_default(),
            shard: st.shard.clone(),
            time_window: st.time_window.clone(),
            files,
            schema_version: st.schema_version,
            row_count: st.row_count,
        })
        .await?;
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

/// 合并一组批次为单个文件内容（对齐到 chunk schema 后 concat）。
///
/// chunk 内**只有一个 schema 版本**（架构 §2.4 / I5：版本变化强制 seal 开新文件），
/// 因此这里对齐到 `input.schema` 是恒等变换；保留对齐是为了防御性兜底
/// （如 chunk 内被外部注入异构批次）。
pub fn merge_batches(
    batches: &[arrow::record_batch::RecordBatch],
    schema: &arrow::datatypes::SchemaRef,
) -> Result<arrow::record_batch::RecordBatch, LakeError> {
    if batches.is_empty() {
        return Err(LakeError::Other("empty chunk".into()));
    }
    let aligned: Vec<arrow::record_batch::RecordBatch> = batches
        .iter()
        .map(|b| align_batch(b, schema))
        .collect::<Result<Vec<_>, _>>()?;
    arrow::compute::concat_batches(schema, &aligned)
        .map_err(|e| LakeError::Other(format!("concat: {e}")))
}

/// WAL Data payloads → 批次 + 目标 schema（**恢复路径专用**：数据不在 chunk 里，只能解码 WAL）。
///
/// 不同 schema_version 的 payload：按列名对齐到最高版本的 schema（缺失列填 null），
/// 与 §6.8 的查询侧兜底语义一致。
pub fn payloads_to_batches(
    payloads: &[DataPayload],
) -> Result<(Vec<arrow::record_batch::RecordBatch>, u64), LakeError> {
    if payloads.is_empty() {
        return Err(LakeError::Other("empty batch group".into()));
    }
    let mut batches: Vec<(u64, arrow::record_batch::RecordBatch)> = Vec::new();
    for p in payloads {
        let reader =
            arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(&p.batch_ipc), None)
                .map_err(|e| LakeError::Other(format!("wal ipc decode: {e}")))?;
        for b in reader {
            let b = b.map_err(|e| LakeError::Other(format!("wal ipc read: {e}")))?;
            batches.push((p.schema_version, b));
        }
    }
    batches.sort_by_key(|(v, _)| *v);
    let max_version = batches.last().map(|(v, _)| *v).unwrap_or(0);
    let target_schema = batches.last().unwrap().1.schema();
    let aligned: Vec<arrow::record_batch::RecordBatch> = batches
        .into_iter()
        .map(|(_, b)| align_batch(&b, &target_schema))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((aligned, max_version))
}

/// WAL Data payload 的行数（不落地即可统计）。
pub fn payload_row_count(p: &DataPayload) -> u64 {
    arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(&p.batch_ipc), None)
        .map(|r| r.filter_map(|b| b.ok()).map(|b| b.num_rows() as u64).sum())
        .unwrap_or(0)
}

/// 收集 `[from, to)` 区间内所有 Data 记录的**幂等键**（去重 + 排序）。
///
/// # 为什么从 WAL 派生，而不是让 chunk 记着
///
/// WAL 是**唯一写入事实**（ADR-3），chunk 只是它的内存视图。若在 chunk 里再存一份键集合，
/// 就出现了**第二个真相来源** —— 两者不一致时没有任何依据判定谁对（本项目的 §27/§28 类缺陷
/// 全都是这种"两份状态"造成的）。所以键集合在**提交这一刻**从 WAL 现算：代价是该批次 WAL
/// 区间的一次扫描（段文件刚写过，通常都在页缓存里），收益是不会不一致。
///
/// # 与 ingest 入口预筛的关系
///
/// 入口预筛（§7.3 第一层）是**快路径**：能在写 WAL 之前就判掉重复请求。
/// 这里是**权威层**：一个 chunk 聚合多条 Data 记录、各带自己的键，提交层必须按键集合去重
/// —— 否则"同一键被两个不同批次带到同一个 chunk"时无人拦得住（`commit_files` 此前只接受
/// 单个 `client_request_id`，填任何一个都是错的：会把别的键的数据标记成那个键的批次）。
fn collect_idempotency_keys(
    deps: &FlushDeps,
    from: u64,
    to: u64,
) -> Result<Vec<String>, LakeError> {
    use std::collections::BTreeSet;
    let reader = yuntun_wal::reader::WalReader::new(deps.wal.shard_dir());
    // BTreeSet：去重 + 有序 → 提交内容确定（不依赖哈希序，见 catalog/state.rs 纪律 2）
    let mut keys: BTreeSet<String> = BTreeSet::new();
    for (_, rec) in reader.scan_range(from, to)? {
        if let Record::Data(p) = rec {
            if !p.client_request_id.is_empty() {
                keys.insert(p.client_request_id);
            }
        }
    }
    Ok(keys.into_iter().collect())
}

async fn write_to_object_store(
    deps: &FlushDeps,
    input: &ChunkFlushInput,
    batch_id: &str,
    merged: &arrow::record_batch::RecordBatch,
) -> Result<(String, u64), LakeError> {
    let (path, size, _rows) = yuntun_format::write_batch(
        &deps.store,
        &input.shard.table,
        &input.shard.shard,
        &input.shard.window,
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
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as SArc;

    fn schema() -> arrow::datatypes::SchemaRef {
        SArc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
    }

    fn batch(rows: usize) -> arrow::record_batch::RecordBatch {
        arrow::record_batch::RecordBatch::try_new(
            schema(),
            vec![SArc::new(Int64Array::from(vec![1i64; rows]))],
        )
        .unwrap()
    }

    #[test]
    fn merge_batches_concatenates_in_order() {
        let batches = vec![batch(2), batch(3)];
        let merged = merge_batches(&batches, &schema()).unwrap();
        assert_eq!(merged.num_rows(), 5);
    }

    #[test]
    fn merge_batches_aligns_missing_columns() {
        use arrow::datatypes::Schema;
        let wider = SArc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let merged = merge_batches(&[batch(1)], &wider).unwrap();
        assert_eq!(merged.num_columns(), 2);
        assert_eq!(merged.column(1).null_count(), 1);
    }

    #[test]
    fn merge_batches_rejects_empty_chunk() {
        assert!(merge_batches(&[], &schema()).is_err());
    }
}
