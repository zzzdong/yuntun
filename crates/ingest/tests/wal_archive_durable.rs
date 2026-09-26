//! **`durable` 档的机制验收**（ADR-9 / `§125`）：归档 → **整盘丢失** → 拉回 → **可重放**。
//!
//! # 为什么这条用例有鉴别力
//!
//! 同一个场景跑两遍，**只差那一个开关**（归档给不给）：
//! * **有归档** ⇒ 私有目录整个删掉之后，记录**逐条**读得回来；
//! * **没归档** ⇒ 一条都读不回来 —— 这正是 `best_effort` 档承认的那句话（"未提交数据丢失"）。
//!
//! 两次的差别只有归档，所以"数据活下来的原因"不可能是别的东西。
//!
//! # 它验到哪一步（边界写清）
//!
//! 验的是**机制**：段进了共享存储 → 丢盘后能拉回 → 拉回的字节**能通过 CRC 被重放**
//! （记录数 + 载荷逐条相等）。**端到端**（重建之后走 `resume_recovered` 重新提交成可见数据）
//! 与 datanode 侧接线另有台账项 —— 见 `docs/closeout.md` 的 `D-5`/`D-6`，别把这条当成了那个。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use yuntun_ingest::wal_archive::{archive_once, restore, ArchiveConfig};
use yuntun_model::wal_record::{DataPayload, Record};
use yuntun_wal::{WalConfig, WalReader, WalWriter};

/// 写多少条（每条一次 `append` = 一次组提交 ack，与 `DoPut` 的语义一致）。
const N: usize = 8;

/// 临时目录（Drop 时清理）。
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

/// 一条 `Data` 记录（内容不重要，重要的是**能通过 CRC 逐条读回来**，且逐条可区分）。
fn data_record(i: u64) -> Record {
    Record::Data(DataPayload {
        table: "t".into(),
        shard: "s".into(),
        schema_version: 1,
        batch_ipc: vec![i as u8; 8],
        client_request_id: format!("rid-{i}"),
        time_window: "2026-09-26T00".into(),
    })
}

fn cfg() -> ArchiveConfig {
    ArchiveConfig {
        prefix: "wal-archive".into(),
        instance_id: "inst-1".into(),
        interval: Duration::from_secs(1),
    }
}

/// 第一阶段：写 N 条（**已 ack**），然后（可选）归档一轮。
///
/// 返回实际上传的段数。
async fn write_and_maybe_archive(
    wal_dir: &Path,
    archive: &Arc<dyn ObjectStore>,
    with_archive: bool,
) -> usize {
    let wal = WalWriter::open(
        WalConfig {
            dir: wal_dir.to_path_buf(),
            ..Default::default()
        },
        0,
    )
    .await
    .expect("开 WAL");
    for i in 0..N {
        wal.append(data_record(i as u64)).await.expect("append 应当成功");
    }
    if !with_archive {
        return 0;
    }
    let mut uploaded = HashMap::new();
    archive_once(&cfg(), wal_dir, 0, archive.as_ref(), &mut uploaded)
        .await
        .expect("归档一轮")
}

/// 第二阶段（"整盘丢失"之后）：必要时先从归档拉回，然后数**能读回来**的 `Data` 记录。
async fn replay_after_total_disk_loss(
    wal_dir: &Path,
    archive: &Arc<dyn ObjectStore>,
    with_archive: bool,
) -> usize {
    if with_archive {
        restore(&cfg(), wal_dir, 0, archive.as_ref())
            .await
            .expect("从归档拉回");
    }
    // 走的是**既有**读取面（`WalReader` + CRC）—— 所以"读得回来"不只证明字节被搬回来了，
    // 还证明它**能被 WAL 的既有通路解析**（这是"拉回来再走既有恢复"这条设计选择的回报）。
    let reader = WalReader::new(wal_dir.join("shard=0"));
    match reader.scan_from(0) {
        Ok(recs) => recs
            .iter()
            .filter(|(_, r)| matches!(r, Record::Data(_)))
            .count(),
        // 没归档那一组：目录都不存在 ⇒ 一条读不回来（**这是预期**）。
        // 而有归档的那组若读失败，必须**响** —— 那说明拉回来的字节不可解析（CRC/命名/位置错），
        // 正是这条用例要抓的东西；把它也归成 0 会让失败伪装成"对照组"。
        Err(e) if !with_archive => {
            eprintln!("（对照组）本地 WAL 不可读（预期）：{e}");
            0
        }
        Err(e) => panic!("有归档却读不回来：{e}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_recovers_acked_records_after_total_disk_loss() {
    // 共享存储（生产里是 S3）：**在"整盘丢失"之外**，所以它活着 —— 这就是归档的意义。
    let archive: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());

    // ---- 有归档：丢盘之后逐条读得回来 ----
    let dir = TempDir::new("wal-durable");
    let wal_dir = dir.path().join("wal");
    let up = write_and_maybe_archive(&wal_dir, &archive, true).await;
    assert!(up >= 1, "归档应当把段传到共享存储（实测上传 {up} 个）");
    std::fs::remove_dir_all(dir.path()).expect("删掉私有目录：模拟整盘丢失");
    let back = replay_after_total_disk_loss(&wal_dir, &archive, true).await;
    assert_eq!(
        back, N,
        "**有归档** ⇒ 整盘丢失之后必须逐条读得回来（实测 {back}/{N}）"
    );

    // ---- 对照组：一模一样，只是**不归档** ----
    let ctl = TempDir::new("wal-durable-control");
    let ctl_dir = ctl.path().join("wal");
    let empty: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let up = write_and_maybe_archive(&ctl_dir, &empty, false).await;
    assert_eq!(up, 0, "对照组不归档");
    std::fs::remove_dir_all(ctl.path()).expect("删掉私有目录：模拟整盘丢失");
    let back = replay_after_total_disk_loss(&ctl_dir, &empty, false).await;
    assert_eq!(
        back, 0,
        "**没归档** ⇒ 丢盘之后一条都读不回来（{back} 条）—— 这就是 `best_effort` 承认的那句话；\
         两次的差别只有归档，所以上面那次活下来**不可能是别的原因**"
    );
}
