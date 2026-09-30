//! **按表归档（ADR-9 的表级 `durability`）**的端到端验收（`operation-log §162`）。
//!
//! 两半各守一个方向，缺一条就是错的：
//!
//! * **省钱的那一半**：段里**只有** `best_effort` 表的数据 ⇒ **不上传**（这正是 ADR-9
//!   "不为所有数据付 S3 归档成本"的那句话）；
//! * **不能丢数据的那一半**：段里出现过 `durable` 表的数据 ⇒ **一定上传**，
//!   而且拉回之后**逐条能读**（走既有 WAL 读取面 + CRC）。
//!
//! ⚠️ 粒度是**段**：`durable` 表与别的表共段时，同段里别的表的数据**也会**被归档 ——
//! 这是 WAL 格式的硬约束（记录帧不带 seq，seq 由 `header.first_seq` + 位置推导 ⇒
//! 过滤记录会让后续 seq 位移，破坏 `BatchPending.wal_seq_start/end` 的区间语义）。
//! 本用例**正面断言**这个代价（而不是假装它不存在）：共段的 `best_effort` 记录也在归档里。

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use yuntun_ingest::wal_archive::{archive_once, restore, ArchiveConfig};
use yuntun_model::wal_record::{DataPayload, Record};
use yuntun_wal::{WalConfig, WalReader, WalWriter};

struct TempDir(PathBuf);
impl TempDir {
    fn new(tag: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!("yuntun-{tag}-{nanos}"));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn data(table: &str, i: u64) -> Record {
    Record::Data(DataPayload {
        table: table.into(),
        shard: "s".into(),
        schema_version: 1,
        batch_ipc: vec![i as u8; 8],
        client_request_id: format!("rid-{i}"),
        time_window: "2026-09-30T00".into(),
    })
}

fn cfg() -> ArchiveConfig {
    ArchiveConfig {
        prefix: "wal-archive".into(),
        instance_id: "inst-1".into(),
        interval: Duration::from_secs(1),
        all_tables: false,
    }
}

/// 写一个段（每 `append` 一次 = 一次组提交 ack）。
async fn write_dir(tag: &str, records: Vec<Record>) -> TempDir {
    let dir = TempDir::new(tag);
    let wal = WalWriter::open(
        WalConfig {
            dir: dir.path().to_path_buf(),
            ..Default::default()
        },
        0,
    )
    .await
    .expect("开 WAL");
    for r in records {
        wal.append(r).await.expect("append 应当成功");
    }
    dir
}

fn durable_set(v: &[&str]) -> HashSet<String> {
    v.iter().map(|x| x.to_string()).collect()
}

/// 拉回之后能读回来的 `Data` 记录，按表分组计数。
async fn replayed_by_table(
    wal_dir: &Path,
    archive: &Arc<dyn ObjectStore>,
) -> HashMap<String, usize> {
    restore(&cfg(), wal_dir, 0, archive.as_ref())
        .await
        .expect("从归档拉回");
    let reader = WalReader::new(wal_dir.join("shard=0"));
    let recs = reader.scan_from(0).expect("拉回的字节必须能被既有读取面解析");
    let mut out: HashMap<String, usize> = HashMap::new();
    for (_, r) in recs {
        if let Record::Data(p) = r {
            *out.entry(p.table).or_insert(0) += 1;
        }
    }
    out
}

/// **只有 `best_effort` 表 ⇒ 一个段都不上传**（省钱的那一半）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn best_effort_only_segments_are_not_archived() {
    let dir = write_dir(
        "arch-effort",
        vec![data("best_effort_t", 0), data("best_effort_t", 1)],
    )
    .await;
    let archive: Arc<dyn ObjectStore> = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory)
        .unwrap();
    let durable = durable_set(&["durable_t"]);

    let mut uploaded = HashMap::new();
    let n = archive_once(
        &cfg(),
        dir.path(),
        0,
        archive.as_ref(),
        &mut uploaded,
        Some(&durable),
    )
    .await
    .expect("归档一轮");
    assert_eq!(n, 0, "段里只有 best_effort 表的数据 ⇒ 不上传");
    let objs = yuntun_store::list_all(archive.as_ref(), "wal-archive/")
        .await
        .unwrap();
    assert!(objs.is_empty(), "归档里不该有任何对象：{objs:?}");

    // 对照：同一份数据在**全归档**口径下会传（证明"0"来自过滤，不是来自别的毛病）
    let mut uploaded2 = HashMap::new();
    let n2 = archive_once(&cfg(), dir.path(), 0, archive.as_ref(), &mut uploaded2, None)
        .await
        .expect("全归档一轮");
    assert_eq!(n2, 1, "对照：不按表过滤时同一个段会上传");
}

/// **段里有 `durable` 表 ⇒ 整段上传**，且拉回后逐条能读；
/// 共段的 `best_effort` 数据也一起进归档（**诚实的代价**，正面断言）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_tables_pull_the_whole_segment_in() {
    // 一个段里混两个表（真实节点就是这样：段是按时间/大小轮转的，不按表分）
    let dir = write_dir(
        "arch-mixed",
        vec![
            data("best_effort_t", 0),
            data("durable_t", 1),
            data("best_effort_t", 2),
        ],
    )
    .await;
    let archive: Arc<dyn ObjectStore> = yuntun_store::create_store(&yuntun_store::StoreConfig::Memory)
        .unwrap();
    let durable = durable_set(&["durable_t"]);

    let mut uploaded = HashMap::new();
    let n = archive_once(
        &cfg(),
        dir.path(),
        0,
        archive.as_ref(),
        &mut uploaded,
        Some(&durable),
    )
    .await
    .expect("归档一轮");
    assert_eq!(n, 1, "段里有 durable 表的数据 ⇒ 必须上传（不能丢数据的那一半）");

    // 整盘丢失之后：从归档拉回，逐条读得回来
    let after = TempDir::new("arch-mixed-after");
    let by_table = replayed_by_table(after.path(), &archive).await;
    assert_eq!(
        by_table.get("durable_t").copied().unwrap_or(0),
        1,
        "**durable 表的数据一定在归档里**：{by_table:?}"
    );
    assert_eq!(
        by_table.get("best_effort_t").copied().unwrap_or(0),
        2,
        "共段的 best_effort 数据**也**被归档了 —— 这是段粒度的代价，不是 bug；\\
         要按记录过滤得先让记录自带 seq（台账 `D-18`）：{by_table:?}"
    );
}
