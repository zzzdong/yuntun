//! Segment 文件读写（详细设计 §4.1 / §4.2）。
//!
//! 文件布局：
//! ```text
//! {wal_dir}/shard={shard_id}/
//!   {seq:020}.wal        # segment，20 位定宽十进制序号
//!   CURRENT              # 当前活跃 segment 文件名
//!   CURRENT.tmp          # 切换临时文件
//! ```

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;
use yuntun_model::error::WalError;
use yuntun_model::wal_record::{
    record_crc, FileHeader, Record, FILE_HEADER_SIZE, RECORD_HEADER_SIZE, WAL_MAGIC,
};

/// segment 文件名：`{seq:020}.wal`
pub fn segment_file_name(seg_seq: u64) -> String {
    format!("{seg_seq:020}.wal")
}

/// 从文件名解析 segment 序号；非法文件名返回 None。
pub fn parse_segment_file_name(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".wal")?;
    if stem.len() == 20 && stem.chars().all(|c| c.is_ascii_digit()) {
        stem.parse().ok()
    } else {
        None
    }
}

/// 列出 shard 目录内全部 segment（按序号升序）。
pub fn list_segments(shard_dir: &Path) -> std::io::Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(shard_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(seq) = parse_segment_file_name(&name) {
            out.push((seq, entry.path()));
        }
    }
    out.sort_by_key(|(seq, _)| *seq);
    Ok(out)
}

/// 读取 CURRENT 文件内容（活跃 segment 名）。
/// 损坏/不存在 → None（调用方 fallback 到目录内最大 seq，保守，§4.9）。
pub fn read_current(shard_dir: &Path) -> Option<String> {
    let content = std::fs::read_to_string(shard_dir.join("CURRENT")).ok()?;
    let name = content.trim().to_string();
    if parse_segment_file_name(&name).is_some() {
        Some(name)
    } else {
        None
    }
}

/// 【v11 修正】CURRENT 原子切换（详细设计 §4.7 / 架构 §5.3.6）：
/// 先写 tmp 并 fsync，再 fsync 目录，最后 rename；rename 后再 fsync 目录。
/// 目录 fsync 不可省略 —— rename 的原子性由 FS 保证，
/// 但"新文件名已写入目录项"这一元数据需 fsync(dir) 才持久化。
pub fn atomic_write_current(shard_dir: &Path, segment_name: &str) -> std::io::Result<()> {
    let tmp = shard_dir.join("CURRENT.tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(segment_name.as_bytes())?;
        f.sync_all()?; // tmp 文件 fsync
    }
    fsync_dir(shard_dir)?;
    std::fs::rename(&tmp, shard_dir.join("CURRENT"))?;
    fsync_dir(shard_dir)?; // rename 后再 fsync 目录
    Ok(())
}

/// 目录 fsync（Linux：O_RDONLY 打开目录后 sync_all）。
///
/// Windows 注意：`File::open(dir)` 会返回 Access Denied（os error 5）——
/// 目录句柄必须带 `FILE_FLAG_BACKUP_SEMANTICS`，且 `FlushFileBuffers`
/// 要求句柄具有写权限。个别文件系统（FAT/exFAT）不支持目录 flush，
/// 此时降级为忽略（rename 的元数据持久性无保证，但比直接报错可用）。
#[cfg(unix)]
pub fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    let f = File::open(dir)?;
    f.sync_all()
}

#[cfg(windows)]
pub fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(dir)
    {
        Ok(f) => f.sync_all(),
        // FAT/exFAT 等不支持目录 FlushFileBuffers：降级为 no-op
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => Ok(()),
        Err(e) => Err(e),
    }
}

/// segment 追加写入器。
pub struct SegmentWriter {
    pub path: PathBuf,
    /// segment 文件序号（文件名中的定宽数字）
    pub seg_seq: u64,
    file: File,
    /// 本 segment 已写字节数（含 header）
    pub bytes_written: u64,
    /// 创建时刻（segment_max_age 轮转判断基准）
    pub created_at: Instant,
}

impl SegmentWriter {
    /// 创建新 segment 并写入 FileHeader，立即 fsync。
    pub fn create(
        shard_dir: &Path,
        seg_seq: u64,
        shard_id: u64,
        first_record_seq: u64,
    ) -> std::io::Result<Self> {
        let path = shard_dir.join(segment_file_name(seg_seq));
        let mut f = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&path)?;
        let header = FileHeader {
            shard_id,
            first_seq: first_record_seq,
        };
        f.write_all(&header.encode())?;
        f.sync_all()?; // 文件 fsync（详细设计 §4.7 步骤①）
        Ok(Self {
            path,
            seg_seq,
            file: f,
            bytes_written: FILE_HEADER_SIZE as u64,
            created_at: Instant::now(),
        })
    }

    /// 以追加模式打开已存在的 segment。
    pub fn open_append(path: PathBuf, seg_seq: u64) -> std::io::Result<Self> {
        let mut f = OpenOptions::new().append(true).read(true).open(&path)?;
        let mut head = [0u8; FILE_HEADER_SIZE];
        f.read_exact(&mut head)?;
        FileHeader::decode(&head)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        let bytes_written = f.metadata()?.len();
        Ok(Self {
            path,
            seg_seq,
            file: f,
            bytes_written,
            created_at: Instant::now(),
        })
    }

    /// 追加一条记录（编码 + 写入，**不 fsync** —— fsync 由组提交统一执行）。
    pub fn append(&mut self, rec: &Record) -> std::io::Result<u64> {
        let bytes = encode_record(rec);
        self.file.write_all(&bytes)?;
        self.bytes_written += bytes.len() as u64;
        Ok(bytes.len() as u64)
    }

    /// 批量追加（组提交路径）。
    pub fn append_batch(&mut self, records: &[Record]) -> std::io::Result<u64> {
        let mut total = 0u64;
        for rec in records {
            total += self.append(rec)?;
        }
        Ok(total)
    }

    pub fn sync_all(&self) -> std::io::Result<()> {
        self.file.sync_all()
    }

    pub fn should_rotate(
        &self,
        incoming_bytes: u64,
        cfg_max_size: u64,
        cfg_max_age: std::time::Duration,
    ) -> bool {
        self.bytes_written + incoming_bytes > cfg_max_size
            || self.created_at.elapsed() > cfg_max_age
    }
}

/// 编码一条 Record 为 `length | crc32 | type | payload` 字节（§4.2）。
pub fn encode_record(rec: &Record) -> Vec<u8> {
    let payload = rec.encode_payload();
    let ty = rec.record_type() as u8;
    let crc = record_crc(ty, &payload);
    let mut buf = Vec::with_capacity(RECORD_HEADER_SIZE + payload.len());
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.push(ty);
    buf.extend_from_slice(&payload);
    buf
}

/// 从 segment 字节流中顺序解出记录，返回 `(序号, Record)`。
///
/// 崩溃恢复正确性核心（§4.9 / C3）：
/// - 先读 length（信任锚点），再读 crc32+type+payload 校验
/// - CRC 失败 → 立即停止 replay（尾部撕裂写入边界 = fsync 边界）
/// - length 超文件剩余长度 → 停止（尾部截断）
/// - 不足一个 header → 停止
///
/// 返回 `(已解出记录, 是否在文件末尾完整结束)`。
pub fn decode_segment_records(header: &FileHeader, data: &[u8]) -> (Vec<(u64, Record)>, bool) {
    let mut records = Vec::new();
    let mut seq = header.first_seq;
    let mut pos = FILE_HEADER_SIZE;
    loop {
        if pos + RECORD_HEADER_SIZE > data.len() {
            // 不足一个 record header：剩余字节不足 9 → 若正好为 0 则完整结束
            return (records, pos == data.len());
        }
        let length = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap());
        let ty = data[pos + 8];
        let payload_start = pos + RECORD_HEADER_SIZE;
        if payload_start + length > data.len() {
            // 尾部截断（§4.9）：停止 replay
            return (records, false);
        }
        let payload = &data[payload_start..payload_start + length];
        if record_crc(ty, payload) != crc {
            // CRC 校验失败 = 撕裂写入边界（§4.9）：停止 replay
            return (records, false);
        }
        match Record::decode(ty, payload) {
            Ok(rec) => {
                records.push((seq, rec));
                seq += 1;
                pos = payload_start + length;
            }
            Err(_) => return (records, false),
        }
    }
}

/// load_segment 的返回形态：文件头 + (seq, Record) 列表 + 尾部是否撕裂。
pub type LoadedSegment = (FileHeader, Vec<(u64, Record)>, bool);

/// 读入整个 segment 并解码（阶段 0 简化：segment ≤ 64MB，整读可接受）。
pub fn load_segment(path: &Path) -> Result<LoadedSegment, WalError> {
    let mut f =
        File::open(path).map_err(|e| WalError::Other(format!("open {}: {e}", path.display())))?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)
        .map_err(|e| WalError::Other(format!("read {}: {e}", path.display())))?;
    if buf.len() < FILE_HEADER_SIZE {
        return Err(WalError::Other(format!(
            "segment {} too short ({})",
            path.display(),
            buf.len()
        )));
    }
    if buf[0..4] != WAL_MAGIC {
        return Err(WalError::BadMagic(path.display().to_string()));
    }
    let header = FileHeader::decode(&buf)?;
    let (records, complete) = decode_segment_records(&header, &buf);
    Ok((header, records, complete))
}

#[cfg(test)]
mod tests {
    use super::*;
    use yuntun_model::wal_record::{BatchCommittedPayload, BatchPendingPayload, Record};

    fn tmpdir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("yuntun-wal-test-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn sample_records(n: u64) -> Vec<Record> {
        (0..n)
            .map(|i| {
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
            })
            .collect()
    }

    #[test]
    fn write_and_decode_roundtrip() {
        let dir = tmpdir("roundtrip");
        let mut w = SegmentWriter::create(&dir, 1, 0, 100).unwrap();
        let recs = sample_records(10);
        w.append_batch(&recs).unwrap();
        w.sync_all().unwrap();
        drop(w);

        let (_, decoded, complete) = load_segment(&dir.join(segment_file_name(1))).unwrap();
        assert!(complete);
        assert_eq!(decoded.len(), 10);
        assert_eq!(decoded[0].0, 100); // seq 从 first_seq 起
        assert_eq!(decoded[9].0, 109);
        assert_eq!(decoded[3].1, recs[3]);
    }

    #[test]
    fn torn_write_detected_by_crc() {
        // Chaos T6.7：截断文件尾部 → CRC 拦截，停止 replay
        let dir = tmpdir("torn");
        let mut w = SegmentWriter::create(&dir, 1, 0, 0).unwrap();
        w.append_batch(&sample_records(10)).unwrap();
        w.sync_all().unwrap();
        drop(w);

        let path = dir.join(segment_file_name(1));
        let mut data = std::fs::read(&path).unwrap();
        // 模拟撕裂：截掉最后一条记录的后半部分
        data.truncate(data.len() - 10);
        std::fs::write(&path, &data).unwrap();

        let (_, records, complete) = load_segment(&path).unwrap();
        assert!(!complete);
        assert_eq!(records.len(), 9); // 完整的 9 条被解出，最后一条被 CRC 拦截
    }

    #[test]
    fn corrupted_middle_stops_replay() {
        // 中间字节被破坏（掉电 bit flip）：CRC 失败，该条及之后停止
        let dir = tmpdir("corrupt");
        let mut w = SegmentWriter::create(&dir, 1, 0, 0).unwrap();
        w.append_batch(&sample_records(5)).unwrap();
        w.sync_all().unwrap();
        drop(w);

        let path = dir.join(segment_file_name(1));
        let mut data = std::fs::read(&path).unwrap();
        let payload_off = FILE_HEADER_SIZE + RECORD_HEADER_SIZE;
        data[payload_off] ^= 0xFF; // 破坏第 1 条 payload
        std::fs::write(&path, &data).unwrap();

        let (_, records, _) = load_segment(&path).unwrap();
        assert_eq!(records.len(), 0);
    }

    #[test]
    fn current_atomic_switch() {
        let dir = tmpdir("current");
        atomic_write_current(&dir, &segment_file_name(2)).unwrap();
        assert_eq!(
            read_current(&dir).as_deref(),
            Some("00000000000000000002.wal")
        );
        atomic_write_current(&dir, &segment_file_name(3)).unwrap();
        assert_eq!(
            read_current(&dir).as_deref(),
            Some("00000000000000000003.wal")
        );
        // 不存在 → None
        let empty = tmpdir("empty");
        assert!(read_current(&empty).is_none());
    }

    #[test]
    fn committed_records_decode() {
        let dir = tmpdir("committed");
        let mut w = SegmentWriter::create(&dir, 1, 7, 5).unwrap();
        w.append(&Record::BatchCommitted(BatchCommittedPayload {
            batch_id: "x".into(),
        }))
        .unwrap();
        w.sync_all().unwrap();
        drop(w);
        let (header, records, _) = load_segment(&dir.join(segment_file_name(1))).unwrap();
        assert_eq!(header.first_seq, 5);
        assert_eq!(header.shard_id, 7);
        assert_eq!(records.len(), 1);
    }
}
