//! 节点私有状态目录的**排他所有权**（架构 §2.3-3；`plan.md` R4 的 T12.4–T12.5）。
//!
//! ## 为什么需要
//!
//! WAL 目录与 spill 目录是**节点私有状态**：同一时刻只能有一个消费者。违反它的事故
//! **已经发生过一次**（`operation-log §28.2`）—— 同一份 WAL 目录被两个攒批循环消费，
//! 同一条 `Data` 被各吸收一次、各自 flush，于是 **9 批次 27 行变成 11 个文件 33 行**：
//! 不报错，只是持久地重复。
//!
//! 当时修的是**同进程内**的竞态（攒批循环的"退出闸门"）。R4 把 datanode 拆成进程之后，
//! 跨进程**没有任何防线** —— 本模块把"一个目录一个消费者"从"约定"升级为**启动期硬拒绝**，
//! 也就是 `operation-log §28.2` 那条教训的显式化：
//! **节点私有状态（WAL 目录 / spill 目录）同一时刻只能有一个消费者**。
//!
//! ## 实现选择
//!
//! `std::fs::File::try_lock`（Rust 1.89 稳定）：**零 unsafe、零新依赖**。
//! 锁按**打开的文件描述**算 —— 实测同一进程再开一个 fd 同样 `WouldBlock`，
//! 所以它约束的是"**目录的消费者**"而不是"进程"，正是这里要的语义
//! （同进程内起第二个攒批循环同样必须被拦住）。
//!
//! ## 边界：**只防本机**
//!
//! 拦的是"同一台机器上两个消费者打开同一个目录"。**跨机器的 `instance_id` 重名**
//! （两处配置写成同一个名字）不在这里 —— 那属于成员注册（`refactor.md` S4-2 / R4 T12.3）。

use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

/// owner 文件名。
///
/// 点开头、独立后缀，**故意避开两处清理逻辑的文件名模式**：
/// - `ChunkStore::purge_leftover_spills` 只删 `*.ipc.lz4` / `*.ipc.lz4.tmp`；
/// - WAL 的 segment 清理只删能 `parse_segment_file_name` 解出段号的文件。
///
/// ⚠️ 这条不是审美问题：锁文件若被当垃圾删掉，**已持有的锁不会失效**，但下一个消费者会
/// 在**新 inode** 上拿到锁 → 保护**静默失效**。改这两处清理时务必回头确认本文件仍被排除。
pub const LOCK_FILE: &str = ".yuntun-owner.lock";

/// 目录的角色/占有者（写进 owner 文件；`role` 用于在错误信息里定位"是哪个目录"）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirOwner {
    /// 实例标识（= `FileManifest.source_instance` / 配置里的 `instance_id`）。
    /// WAL 层拿不到实例标识时传空串（见 [`DirOwner::role_only`]）。
    pub instance_id: String,
    /// 该目录的角色，如 `chunk-spill` / `wal-shard-3`
    pub role: String,
}

impl DirOwner {
    /// 只知道角色、不知道实例标识时的构造（WAL 目录用：实例标识在 `ChunkStoreConfig` 里，
    /// WAL crate 拿不到；靠 `role` 已足够定位）。
    pub fn role_only(role: impl Into<String>) -> Self {
        Self {
            instance_id: String::new(),
            role: role.into(),
        }
    }
}

/// owner 文件的记录。
///
/// 手写 `key=value` 而**不引 serde**：只有 4 个字段，唯一用途是"给人看 + 写进报错"，
/// 而任何序列化依赖都会进全部 crate 的传递闭包。解析时**未知键直接忽略**，
/// 所以以后加字段不会让旧版本读崩（有单测钉住）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerRecord {
    pub instance_id: String,
    pub role: String,
    pub pid: u32,
    /// 获取时刻（Unix 毫秒）
    pub acquired_at_ms: u64,
}

impl OwnerRecord {
    fn encode(&self) -> String {
        format!(
            "instance_id={}\nrole={}\npid={}\nacquired_at_ms={}\n",
            self.instance_id, self.role, self.pid, self.acquired_at_ms
        )
    }

    fn decode(s: &str) -> Option<Self> {
        let mut rec = OwnerRecord {
            instance_id: String::new(),
            role: String::new(),
            pid: 0,
            acquired_at_ms: 0,
        };
        let mut seen = false;
        for line in s.lines() {
            let Some((k, v)) = line.split_once('=') else {
                continue;
            };
            match k.trim() {
                "instance_id" => rec.instance_id = v.trim().to_string(),
                "role" => rec.role = v.trim().to_string(),
                "pid" => rec.pid = v.trim().parse().unwrap_or(0),
                "acquired_at_ms" => rec.acquired_at_ms = v.trim().parse().unwrap_or(0),
                // 未知键忽略：向前兼容（加字段不让旧版本读崩）
                _ => {}
            }
            seen = true;
        }
        seen.then_some(rec)
    }
}

impl std::fmt::Display for OwnerRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let who = if self.instance_id.is_empty() {
            "（未提供实例标识）".to_string()
        } else {
            format!("instance_id={}", self.instance_id)
        };
        write!(
            f,
            "{who}, role={}, pid={}, 自 {}ms 起",
            self.role, self.pid, self.acquired_at_ms
        )
    }
}

/// 获取目录所有权失败。
#[derive(Debug)]
pub enum DirLeaseError {
    /// 目录已被另一个消费者占用（`holder` = 对方写下的记录，可能读不出来）
    Busy {
        dir: PathBuf,
        holder: Option<OwnerRecord>,
    },
    /// 建目录 / 开文件 / 读记录失败
    Io {
        dir: PathBuf,
        source: std::io::Error,
    },
}

impl std::fmt::Display for DirLeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DirLeaseError::Busy { dir, holder } => {
                let who = match holder {
                    Some(h) => h.to_string(),
                    None => "（占用者没留下记录）".to_string(),
                };
                write!(
                    f,
                    "目录 {} 已被另一个消费者占用（{who}）。\n\
                     节点私有状态（WAL 目录 / spill 目录）同一时刻只能有一个消费者 —— \
                     两个消费者会让同一条数据被各 flush 一次，产出**重复文件**且不报错\
                     （operation-log §28.2：9 批次 27 行 → 11 文件 33 行）。\n\
                     处置：确认是否还有另一个进程在用该目录（`lsof {}/{}`），\
                     或为这个节点换一个目录；确实要换消费者，请先停掉旧的那个。",
                    dir.display(),
                    dir.display(),
                    LOCK_FILE
                )
            }
            DirLeaseError::Io { dir, source } => {
                write!(f, "无法取得目录 {} 的所有权：{source}", dir.display())
            }
        }
    }
}

impl std::error::Error for DirLeaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DirLeaseError::Busy { .. } => None,
            DirLeaseError::Io { source, .. } => Some(source),
        }
    }
}

/// 目录所有权的**租约**：持有期间该目录归本消费者独占。
///
/// 生命周期语义：**锁随文件描述符关闭而释放**，所以租约必须活到"不再消费该目录"为止
/// （进程退出时由 OS 兜底，包括 `kill -9`）。换言之：**不要提前 drop**。
#[derive(Debug)]
pub struct PrivateDirLease {
    dir: PathBuf,
    /// **不要移除这个字段**：它就是锁本身。提前 drop 会让目录立刻可被第二个消费者接管。
    _file: File,
    /// 写 owner 记录失败的原因（`None` = 写成功）。
    ///
    /// 记录只是**诊断信息**，保护由锁提供 —— 所以写不进去**不**升级成"获取失败"
    /// （否则一个只读的目录会让节点起不来，而它其实并不危险）；但也不静默丢弃，
    /// 交给调用方记日志。
    record_error: Option<String>,
}

impl PrivateDirLease {
    /// 本租约覆盖的目录。
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// 写 owner 记录失败的原因（正常为 `None`）。调用方拿到租约后应把它记进日志 ——
    /// 它是"下一个消费者看不到谁占着"的唯一线索。
    pub fn record_error(&self) -> Option<&str> {
        self.record_error.as_deref()
    }
}

/// 取得 `dir` 的排他所有权（目录不存在则创建）。
///
/// 成功后会在目录内留下 owner 文件（记录 `pid` / `instance_id` / `role`），
/// 作用只有一个：让**下一个**消费者能把"谁占着"直接报出来 —— 这类故障的排查成本
/// 全在"谁占的"。
pub fn acquire(dir: &Path, owner: DirOwner) -> Result<PrivateDirLease, DirLeaseError> {
    std::fs::create_dir_all(dir).map_err(|source| DirLeaseError::Io {
        dir: dir.to_path_buf(),
        source,
    })?;
    let path = dir.join(LOCK_FILE);
    // `truncate(false)`：**先别清空** —— 抢不到锁时，那份旧记录正是要报给用户的信息。
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .map_err(|source| DirLeaseError::Io {
            dir: dir.to_path_buf(),
            source,
        })?;

    match file.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            let holder = read_record(&mut file);
            return Err(DirLeaseError::Busy {
                dir: dir.to_path_buf(),
                holder,
            });
        }
        Err(TryLockError::Error(source)) => {
            return Err(DirLeaseError::Io {
                dir: dir.to_path_buf(),
                source,
            });
        }
    }

    // 拿到锁之后才写自己的记录（覆盖上一任的）。
    let record = OwnerRecord {
        instance_id: owner.instance_id,
        role: owner.role,
        pid: std::process::id(),
        acquired_at_ms: crate::batch::now_ms(),
    };
    let record_error = file
        .set_len(0)
        .and_then(|()| file.write_all(record.encode().as_bytes()))
        .and_then(|()| file.flush())
        .err()
        .map(|e| e.to_string());

    Ok(PrivateDirLease {
        dir: dir.to_path_buf(),
        _file: file,
        record_error,
    })
}

fn read_record(file: &mut File) -> Option<OwnerRecord> {
    let mut s = String::new();
    file.rewind().ok()?;
    file.read_to_string(&mut s).ok()?;
    OwnerRecord::decode(&s)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "yuntun-lease-{tag}-{}-{}",
            std::process::id(),
            crate::batch::now_ms()
        ));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    fn owner(inst: &str, role: &str) -> DirOwner {
        DirOwner {
            instance_id: inst.into(),
            role: role.into(),
        }
    }

    /// 核心契约：同一目录第二个消费者**必须被拒**（同进程也一样）。
    #[test]
    fn second_consumer_of_same_dir_is_refused() {
        let dir = tmp("busy");
        let _first = acquire(&dir, owner("node-a", "chunk-spill")).expect("第一个应成功");

        let err = acquire(&dir, owner("node-a", "chunk-spill")).expect_err("第二个必须被拒");
        match &err {
            DirLeaseError::Busy { holder, .. } => {
                let h = holder.as_ref().expect("应读到占用者记录");
                assert_eq!(h.instance_id, "node-a");
                assert_eq!(h.role, "chunk-spill");
                assert_eq!(h.pid, std::process::id());
            }
            other => panic!("错误的期望 Busy，实际 {other:?}"),
        }
    }

    /// 报错必须**可直接照做**：带上是谁占着（pid / instance_id / role）与 lsof 提示。
    #[test]
    fn busy_error_names_the_holder() {
        let dir = tmp("msg");
        let _first = acquire(&dir, owner("node-a", "chunk-spill")).unwrap();
        let msg = acquire(&dir, owner("node-b", "chunk-spill"))
            .unwrap_err()
            .to_string();

        for needle in ["node-a", "chunk-spill", "lsof", "§28.2"] {
            assert!(msg.contains(needle), "报错里应出现 {needle:?}：\n{msg}");
        }
    }

    /// 租约释放后目录可被接管（模拟正常重启；`kill -9` 由 OS 释放，属同一语义）。
    #[test]
    fn dir_is_takeable_after_lease_is_dropped() {
        let dir = tmp("reopen");
        drop(acquire(&dir, owner("node-a", "chunk-spill")).expect("第一个应成功"));
        let second = acquire(&dir, owner("node-a", "chunk-spill")).expect("释放后应能接管");
        assert_eq!(second.dir(), dir.as_path());
    }

    /// 换实例标识接管同一目录是**允许**的（残留 spill 由 chunk 侧启动清理负责），
    /// 记录被覆盖成新的占有者。
    #[test]
    fn takeover_by_another_instance_overwrites_record() {
        let dir = tmp("takeover");
        drop(acquire(&dir, owner("node-a", "chunk-spill")).unwrap());
        let _lease = acquire(&dir, owner("node-b", "chunk-spill")).unwrap();

        let s = std::fs::read_to_string(dir.join(LOCK_FILE)).unwrap();
        let rec = OwnerRecord::decode(&s).expect("记录应可解析");
        assert_eq!(rec.instance_id, "node-b");
    }

    /// 记录格式向前兼容：未知键忽略（否则以后加字段会让旧版本把目录判成"坏记录"）。
    #[test]
    fn owner_record_ignores_unknown_keys() {
        let rec = OwnerRecord::decode("instance_id=x\nrole=r\npid=7\nacquired_at_ms=9\nfuture=z\n")
            .expect("应能解析");
        assert_eq!(
            rec,
            OwnerRecord {
                instance_id: "x".into(),
                role: "r".into(),
                pid: 7,
                acquired_at_ms: 9
            }
        );
    }

    /// 空内容 / 无 `=` 的垃圾 —— 读不出来就报"没留下记录"，不该 panic。
    #[test]
    fn unreadable_record_yields_none() {
        assert_eq!(OwnerRecord::decode(""), None);
        assert_eq!(OwnerRecord::decode("只是噪音\n"), None);
    }

    /// 目录不存在时自动创建（`acquire` 是"用这个目录"的入口，不该要求调用方先建）。
    #[test]
    fn acquire_creates_missing_dir() {
        let dir = tmp("create").join("nested").join("spill");
        let _lease = acquire(&dir, owner("node-a", "chunk-spill")).unwrap();
        assert!(dir.join(LOCK_FILE).is_file());
    }
}
