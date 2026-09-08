//! WAL 写入器：组提交 fsync + `synced_seq` 水位线（详细设计 §4.4 / §5.3.5.1）。
//!
//! ## 持久性语义（关键）
//! [`WalWriter::append`] 返回的 `WalAck` **等待组提交 fsync 完成后**才 resolve。
//! 调用方在 await 完成后，方可向客户端返回写入成功。
//!
//! ## 水位推进顺序（不可颠倒，§5.3.5.1）
//! `fsync()` → 推进 `synced_seq` → ack 客户端。
//! 保证「客户端确认 = 已 fsync = 攒批线程可见」三者一致。
//! 违反顺序（先 ack 后推进水位）会让攒批线程读到未 fsync 数据，
//! 断电后出现 BatchPending 指向不存在 Data 的状态机错乱。
//!
//! ADR-3：本 WAL 是本地独占的（单进程访问）。绝不在此添加跨节点协调。

use crate::config::WalConfig;
use crate::recovery::{self, Recovery};
use crate::segment::{
    atomic_write_current, fsync_dir, list_segments, parse_segment_file_name, read_current,
    SegmentWriter,
};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;
use yuntun_model::error::{LakeError, WalError};
use yuntun_model::wal_record::Record;

/// append 的确认句柄：resolve 即表示该记录已 fsync。
#[derive(Debug, Clone, Copy)]
pub struct WalAck {
    /// 本记录的 seq（全局单调，per shard）
    pub seq: u64,
    /// fsync 完成时的 synced 水位
    pub synced_seq: u64,
}

struct CommitRequest {
    record: Record,
    ack_tx: tokio::sync::oneshot::Sender<Result<WalAck, LakeError>>,
}

/// 组提交线程独占的状态。
struct CommitterGuard {
    writer: SegmentWriter,
    /// 当前 segment 文件序号
    seg_seq: u64,
    /// 下一条记录的 seq（单调递增）
    next_seq: u64,
}

struct ShardState {
    cfg: WalConfig,
    shard_id: u64,
    /// 已 fsync 的最高记录 seq（§5.3.5.1 水位线）
    synced_seq: AtomicU64,
    /// 当前 segment 的 seq（清理时跳过）
    current_segment: AtomicU64,
}

impl ShardState {
    fn synced_seq(&self) -> u64 {
        self.synced_seq.load(Ordering::SeqCst)
    }
}

/// WAL 写入器（每 shard 一个实例）。
#[derive(Clone)]
pub struct WalWriter {
    state: Arc<ShardState>,
    /// commit 线程退出信号（Drop 时关闭 channel 让线程退出）
    _close: Arc<Mutex<Option<mpsc::Sender<CommitRequest>>>>,
}

impl std::fmt::Debug for WalWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalWriter")
            .field("shard_id", &self.state.shard_id)
            .field("synced_seq", &self.state.synced_seq())
            .finish()
    }
}

impl WalWriter {
    /// 打开（或创建）一个 shard 的 WAL。
    ///
    /// 启动时执行快速 recovery 以确定 seq 起点（CRC 边界），保证重启后 seq 单调不回退。
    pub async fn open(cfg: WalConfig, shard_id: u64) -> Result<Self, LakeError> {
        let shard_dir = cfg.shard_dir(shard_id);
        std::fs::create_dir_all(&shard_dir)?;
        fsync_dir(&cfg.dir).ok(); // 确保新建目录项持久化（best effort）

        // 快速 replay 确定 next_seq（只取 CRC 边界）
        let rec = recovery::recover(&cfg, shard_id, true)?;
        let next_seq = rec.last_seq; // recover 返回"下一条可用 seq"

        // 打开（或创建）活跃 segment
        let guard = open_active_segment(&cfg, shard_id, next_seq)?;

        let (tx, rx) = mpsc::channel::<CommitRequest>();
        let st = Arc::new(ShardState {
            synced_seq: AtomicU64::new(rec.last_seq),
            current_segment: AtomicU64::new(guard.seg_seq),
            cfg: cfg.clone(),
            shard_id,
        });

        let st2 = st.clone();
        std::thread::Builder::new()
            .name(format!("wal-commit-shard{shard_id}"))
            .spawn(move || commit_loop(rx, st2, guard))
            .map_err(|e| LakeError::Other(format!("spawn wal committer: {e}")))?;

        Ok(Self {
            state: st,
            _close: Arc::new(Mutex::new(Some(tx))),
        })
    }

    /// 追加一条记录，返回确认句柄。
    ///
    /// # 持久性语义（关键，§4.4）
    /// 返回的 `WalAck` 必须**等待组提交 fsync 完成**后才 resolve。
    pub async fn append(&self, record: Record) -> Result<WalAck, LakeError> {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        {
            let tx = self._close.lock().unwrap();
            let tx = tx
                .as_ref()
                .ok_or_else(|| LakeError::Wal(WalError::Other("wal closed".into())))?;
            tx.send(CommitRequest { record, ack_tx })
                .map_err(|_| LakeError::Wal(WalError::Other("wal committer thread died".into())))?;
        }
        ack_rx
            .await
            .map_err(|_| LakeError::Wal(WalError::Other("wal ack dropped".into())))?
    }

    /// 当前已 fsync 的水位（§5.3.5.1）。
    ///
    /// # 契约（C2）
    /// 攒批线程传入 `scan_range` 的 `to` **必须** ≤ 此值。
    pub fn synced_seq(&self) -> u64 {
        self.state.synced_seq()
    }

    pub fn shard_id(&self) -> u64 {
        self.state.shard_id
    }

    /// 当前活跃 segment 序号。
    pub fn current_segment(&self) -> u64 {
        self.state.current_segment.load(Ordering::SeqCst)
    }

    /// shard 的 WAL 目录。
    pub fn shard_dir(&self) -> PathBuf {
        self.state.cfg.shard_dir(self.state.shard_id)
    }

    pub fn config(&self) -> &WalConfig {
        &self.state.cfg
    }

    /// 完整 recovery（供启动流程使用）。
    pub fn full_recovery(&self) -> Result<Recovery, LakeError> {
        recovery::recover(&self.state.cfg, self.state.shard_id, false)
    }
}

/// 打开活跃 segment：
/// ① 优先读 CURRENT（§4.9）
/// ② CURRENT 损坏/不存在 → fallback 到目录内最大 seq（保守）
/// ③ 目录为空 → 创建 segment 1
fn open_active_segment(
    cfg: &WalConfig,
    shard_id: u64,
    next_seq: u64,
) -> Result<CommitterGuard, LakeError> {
    let shard_dir = cfg.shard_dir(shard_id);
    let segments = list_segments(&shard_dir)?;

    let target: Option<(u64, PathBuf)> = match read_current(&shard_dir) {
        Some(name) => {
            let seq = parse_segment_file_name(&name)
                .ok_or_else(|| LakeError::Wal(WalError::CorruptedCurrent(name.clone())))?;
            let p = shard_dir.join(&name);
            if p.exists() {
                Some((seq, p))
            } else {
                None
            }
        }
        None => None,
    }
    .or_else(|| segments.last().map(|(s, p)| (*s, p.clone())));

    match target {
        Some((seg_seq, path)) => {
            let writer = SegmentWriter::open_append(path, seg_seq)
                .map_err(|e| LakeError::Wal(WalError::Other(format!("open segment: {e}"))))?;
            // seq 续点：扫描活跃 segment 内已有记录（重启续写场景）
            let next = match crate::segment::load_segment(&writer.path) {
                Ok((header, records, _)) => records
                    .last()
                    .map(|(s, _)| s + 1)
                    .unwrap_or(header.first_seq)
                    .max(next_seq),
                Err(_) => next_seq,
            };
            Ok(CommitterGuard {
                writer,
                seg_seq,
                next_seq: next,
            })
        }
        None => {
            let writer = SegmentWriter::create(&shard_dir, 1, shard_id, next_seq)
                .map_err(|e| LakeError::Wal(WalError::Other(format!("create segment: {e}"))))?;
            atomic_write_current(&shard_dir, &crate::segment::segment_file_name(1))
                .map_err(|e| LakeError::Wal(WalError::Other(format!("write CURRENT: {e}"))))?;
            Ok(CommitterGuard {
                writer,
                seg_seq: 1,
                next_seq,
            })
        }
    }
}

/// 组提交循环（详细设计 §4.4 `commit_loop`）。
fn commit_loop(rx: mpsc::Receiver<CommitRequest>, st: Arc<ShardState>, mut guard: CommitterGuard) {
    let window = st.cfg.group_commit_window;
    let max_batch = st.cfg.group_commit_max_batch.max(1);

    'outer: loop {
        // 等待首个请求
        let mut batch: Vec<CommitRequest> = match rx.recv() {
            Ok(first) => vec![first],
            Err(_) => break, // 所有 WalWriter 句柄已 drop
        };

        // 收集窗口内的所有请求（§4.4 collect_until(window, max_batch)）
        let deadline = Instant::now() + window;
        while batch.len() < max_batch {
            match rx.try_recv() {
                Ok(req) => batch.push(req),
                Err(mpsc::TryRecvError::Empty) => {
                    if Instant::now() >= deadline {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_micros(50));
                }
                Err(mpsc::TryRecvError::Disconnected) => break,
            }
        }

        // 轮转检查：写入前若需轮转则先轮转（§4.7）
        let incoming: u64 = batch
            .iter()
            .map(|r| r.record.encode_payload().len() as u64 + 9)
            .sum();
        if guard
            .writer
            .should_rotate(incoming, st.cfg.segment_max_size, st.cfg.segment_max_age)
        {
            if let Err(e) = rotate(&mut guard, &st) {
                fail_batch(&mut batch, e);
                continue 'outer;
            }
        }

        // 追加写入 + 一次性 fsync
        let records: Vec<Record> = batch.iter().map(|r| r.record.clone()).collect();
        let first_seq = guard.next_seq;
        let write_result = (|| -> Result<(), LakeError> {
            guard.writer.append_batch(&records)?;
            guard.writer.sync_all()?; // fsync（组提交核心）
            Ok(())
        })();

        match write_result {
            Ok(()) => {
                let last_seq = first_seq + records.len() as u64 - 1;
                guard.next_seq = last_seq + 1;

                // 【关键顺序】先推进水位，再 ack 客户端（§5.3.5.1）
                st.synced_seq.store(last_seq, Ordering::SeqCst);
                st.current_segment.store(guard.seg_seq, Ordering::SeqCst);

                let ack = WalAck {
                    seq: last_seq,
                    synced_seq: last_seq,
                };
                for req in batch {
                    let _ = req.ack_tx.send(Ok(ack));
                }
            }
            Err(e) => {
                // WAL 写失败是致命错误（§10.2：不重试）。回滚 seq，通知失败方。
                tracing::error!(error = %e, shard = st.shard_id, "wal fsync failed");
                guard.next_seq = first_seq;
                fail_batch(&mut batch, e);
            }
        }
    }
}

/// segment 轮转（详细设计 §4.7）：
/// ① 旧 segment fsync → ② 创建新 segment（FileHeader + fsync）
/// ③ CURRENT 原子切换（tmp + fsync + 目录 fsync + rename + 目录 fsync）
fn rotate(guard: &mut CommitterGuard, st: &Arc<ShardState>) -> Result<(), LakeError> {
    guard.writer.sync_all()?;
    let shard_dir = st.cfg.shard_dir(st.shard_id);
    let new_seq = guard.seg_seq + 1;
    let writer = SegmentWriter::create(&shard_dir, new_seq, st.shard_id, guard.next_seq)
        .map_err(|e| LakeError::Wal(WalError::Other(format!("create segment: {e}"))))?;
    atomic_write_current(&shard_dir, &crate::segment::segment_file_name(new_seq))
        .map_err(|e| LakeError::Wal(WalError::Other(format!("write CURRENT: {e}"))))?;
    guard.writer = writer;
    guard.seg_seq = new_seq;
    Ok(())
}

fn fail_batch(batch: &mut Vec<CommitRequest>, e: LakeError) {
    for req in batch.drain(..) {
        let _ = req.ack_tx.send(Err(e.clone()));
    }
}
