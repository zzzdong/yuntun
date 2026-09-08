//! WAL 读取器（攒批线程 / 恢复线程使用，详细设计 §3.2 / §5.3）。

use crate::segment::{list_segments, load_segment};
use yuntun_model::error::{LakeError, WalError};
use yuntun_model::wal_record::Record;

/// WAL 读取器。
#[derive(Debug, Clone)]
pub struct WalReader {
    shard_dir: std::path::PathBuf,
}

impl WalReader {
    pub fn new(shard_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            shard_dir: shard_dir.into(),
        }
    }

    /// 顺序扫描 `[from, to)` 区间的记录。
    ///
    /// # 契约（C2 / §5.3.5.1）
    /// 攒批线程传入的 `to` **必须** <= `synced_seq`（只读已 fsync 的数据）。
    /// 读到 CRC 校验失败立即停止（撕裂写入边界）。
    ///
    /// 返回 `(seq, record)` 列表，按 seq 升序。
    pub fn scan_range(&self, from: u64, to: u64) -> Result<Vec<(u64, Record)>, LakeError> {
        if from >= to {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let segments = list_segments(&self.shard_dir)?;
        for (_, path) in segments {
            // 快速跳过：读 header 判断区间是否与本 segment 有交集
            let (header, records, _complete) = match load_segment(&path) {
                Ok(x) => x,
                // BadMagic → 该 segment 损坏 → 告警 + 跳过（人工介入，§4.9）
                Err(WalError::BadMagic(p)) => {
                    tracing::warn!(segment = %p, "segment header corrupted, skipping");
                    continue;
                }
                Err(e) => return Err(e.into()),
            };
            let last_seq = records.last().map(|(s, _)| *s).unwrap_or(header.first_seq);
            if last_seq < from {
                continue; // 本 segment 全部在区间之前
            }
            for (seq, rec) in records {
                if seq < from {
                    continue;
                }
                if seq >= to {
                    return Ok(out);
                }
                out.push((seq, rec));
            }
        }
        Ok(out)
    }

    /// 扫描从 `from` 到当前 CRC 边界的全部记录（replay 用）。
    pub fn scan_from(&self, from: u64) -> Result<Vec<(u64, Record)>, LakeError> {
        self.scan_range(from, u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WalConfig;
    use crate::writer::WalWriter;
    use yuntun_model::wal_record::{BatchPendingPayload, Record};

    fn tmpdir(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("yuntun-wal-reader-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn pending(i: u64) -> Record {
        Record::BatchPending(BatchPendingPayload {
            batch_id: format!("b{i}"),
            shard: "s0".into(),
            window: "w".into(),
            wal_seq_start: i,
            wal_seq_end: i + 1,
            schema_version: 1,
            client_request_id: String::new(),
            created_at_ms: 0,
            row_count: 1,
        })
    }

    #[tokio::test]
    async fn scan_range_reads_only_synced() {
        let dir = tmpdir("range");
        let cfg = WalConfig::for_dir(&dir);
        let wal = WalWriter::open(cfg, 0).await.unwrap();

        for i in 0..10 {
            wal.append(pending(i)).await.unwrap();
        }
        let synced = wal.synced_seq();
        assert_eq!(synced, 9); // seq 0..=9 已 fsync

        let reader = WalReader::new(wal.shard_dir());
        let recs = reader.scan_range(0, synced + 1).unwrap();
        assert_eq!(recs.len(), 10);
        assert_eq!(recs[0].0, 0);
        assert_eq!(recs[9].0, 9);

        // [from, to) 半开区间
        let recs = reader.scan_range(3, 6).unwrap();
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[0].0, 3);
        assert_eq!(recs.last().unwrap().0, 5);

        // 空/反区间
        assert!(reader.scan_range(5, 5).unwrap().is_empty());
        assert!(reader.scan_range(9, 3).unwrap().is_empty());
    }
}
