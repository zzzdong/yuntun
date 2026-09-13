//! 测试基础设施（dev-only）：**临时目录策略**。
//!
//! 写盘类测试（WAL / Parquet / local ObjectStore）都是"写文件 + 读回"形态，
//! 放在 **tmpfs（内存盘）** 上可显著加速：
//! - `/tmp` 在多数 Linux 发行版（含 Arch）由 systemd 挂载为 tmpfs，`fsync` 基本为
//!   no-op（无真实落盘等待），页缓存零拷贝；
//! - 真实磁盘上同样的用例会付出 fsync + 元数据写开销（并发跑 e2e 时尤为明显）。
//!
//! **例外**：需要真实落盘语义的用例（大文件压测、磁盘水位 statvfs、崩溃注入、
//! 多进程重启）应显式使用 [`TestDir::disk`]——对应 `/` 或 `$HOME` 所在的真实磁盘。
//!
//! 环境变量：
//! | 变量 | 作用 | 默认 |
//! |---|---|---|
//! | `YUNTUN_TEST_TMPDIR` | 覆盖内存盘根 | `$TMPDIR` → `/tmp` → `/dev/shm` |
//! | `YUNTUN_TEST_DISKDIR` | 覆盖真实磁盘根 | `<workspace>/target/test-disk` |
//!
//! 用法：
//! ```no_run
//! let dir = yuntun_testkit::TestDir::tmpfs("wal-e2e");   // 自动创建 + Drop 清理
//! let wal_dir = dir.path();
//! ```
//!
//! 每个目录名带 pid + 进程内序号，测试并行/重跑互不冲突。

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

static SEQ: AtomicU32 = AtomicU32::new(0);

/// 内存盘（tmpfs）根目录：`YUNTUN_TEST_TMPDIR` > `$TMPDIR` > `/tmp` > `/dev/shm`。
///
/// `/tmp` 与 `/dev/shm` 都是 tmpfs 时才被选中（否则回退到下一个候选）；
/// 均不可用时回退到系统临时目录（功能仍正确，只是慢）。
pub fn tmp_root() -> PathBuf {
    if let Ok(p) = std::env::var("YUNTUN_TEST_TMPDIR") {
        return PathBuf::from(p);
    }
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Ok(t) = std::env::var("TMPDIR") {
        candidates.push(PathBuf::from(t));
    }
    candidates.push(PathBuf::from("/tmp"));
    candidates.push(PathBuf::from("/dev/shm"));
    for c in &candidates {
        if c.is_dir() && is_tmpfs(c) {
            return c.clone();
        }
    }
    // 全不是 tmpfs：仍用系统临时目录（正确性优先），可用环境变量强制指定
    std::env::temp_dir()
}

/// 真实磁盘根目录：`YUNTUN_TEST_DISKDIR` > `<workspace>/target/test-disk`。
///
/// 用于需要真实落盘语义（fsync 等待、磁盘水位、崩溃注入、大文件压测）的用例。
pub fn disk_root() -> PathBuf {
    if let Ok(p) = std::env::var("YUNTUN_TEST_DISKDIR") {
        return PathBuf::from(p);
    }
    // crates/testkit → workspace 根
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..");
    workspace.join("target").join("test-disk")
}

/// 该路径是否落在 tmpfs 挂载点内（读 `/proc/mounts`，取最长前缀匹配）。
///
/// 非 Linux / 读取失败 → `false`（保守：当作真实磁盘）。
pub fn is_tmpfs(path: &Path) -> bool {
    let Ok(mounts) = std::fs::read_to_string("/proc/mounts") else {
        return false;
    };
    let target = path
        .canonicalize()
        .unwrap_or_else(|_| path.to_path_buf());
    let mut best: Option<(usize, bool)> = None;
    for line in mounts.lines() {
        let mut it = line.split_whitespace();
        let (Some(mount_point), Some(fs_type)) = (it.next(), it.next()) else {
            continue;
        };
        let mp = Path::new(mount_point);
        if !target.starts_with(mp) {
            continue;
        }
        let len = mp.as_os_str().len();
        let is_tmp = fs_type == "tmpfs";
        if best.is_none_or(|(l, _)| len >= l) {
            best = Some((len, is_tmp));
        }
    }
    best.map(|(_, t)| t).unwrap_or(false)
}

/// 唯一测试目录 + 自动清理（Drop 时整目录删除）。
#[derive(Debug)]
pub struct TestDir {
    path: PathBuf,
}

impl TestDir {
    /// 内存盘目录（多数写盘测试用这个）。
    pub fn tmpfs(name: &str) -> Self {
        Self::at(tmp_root(), name)
    }

    /// 真实磁盘目录（fsync / 水位 / 崩溃 / 压测用）。
    pub fn disk(name: &str) -> Self {
        Self::at(disk_root(), name)
    }

    /// 指定根目录。
    pub fn at(root: PathBuf, name: &str) -> Self {
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let path = root.join(format!("yuntun-{name}-{}-{seq}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create test dir");
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 便于塞进 TOML / 命令行参数的字符串形式。
    pub fn string(&self) -> String {
        self.path.to_string_lossy().to_string()
    }

    /// 交出路径并**放弃自动清理**（供 `fn tmpdir() -> PathBuf` 这类 helper 使用：
    /// 目录留在 tmpfs 由系统回收，进程内重跑会先删后建）。
    pub fn into_path(self) -> PathBuf {
        let p = self.path.clone();
        std::mem::forget(self);
        p
    }

    pub fn join(&self, rel: &str) -> PathBuf {
        self.path.join(rel)
    }

    /// 当前目录是否在 tmpfs 上（测试可据此打印提示/调整断言）。
    pub fn on_tmpfs(&self) -> bool {
        is_tmpfs(&self.path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dir_is_created_and_cleaned() {
        let d = TestDir::tmpfs("unit");
        assert!(d.path().is_dir());
        let p = d.path().to_path_buf();
        drop(d);
        assert!(!p.exists(), "Drop 应清理目录");
    }

    #[test]
    fn dirs_are_unique() {
        let a = TestDir::tmpfs("uniq");
        let b = TestDir::tmpfs("uniq");
        assert_ne!(a.path(), b.path());
    }

    #[test]
    fn tmp_root_and_disk_root_are_usable() {
        let t = tmp_root();
        assert!(t.is_dir(), "tmp_root 必须存在: {t:?}");
        let d = TestDir::disk("probe");
        assert!(d.path().is_dir());
        // tmpfs 判定只作提示（CI 上 /tmp 也可能是磁盘）
        println!(
            "tmp_root={:?} (tmpfs={}), disk_root={:?}",
            t,
            is_tmpfs(&t),
            disk_root()
        );
    }
}
