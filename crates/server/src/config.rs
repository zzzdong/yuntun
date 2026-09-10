//! 配置（详细设计 §11 配置项清单）。
//!
//! TOML 配置文件（standalone 启动参数 `--config`）：
//! ```toml
//! [server]
//! listen = "0.0.0.0:50051"
//! shards = 1
//!
//! [store]
//! type = "local"
//! root = "./data/store"
//!
//! [wal]
//! dir = "./data/wal"
//!
//! [ingest]
//! rows_threshold = 10000
//! time_threshold_secs = 5
//! flush_jitter_secs = 60
//!
//! [compaction]
//! min_files = 5
//!
//! [query]
//! cache_ttl_secs = 30
//!
//! [sql.mysql]
//! enabled = true
//! listen = "0.0.0.0:3306"
//! auth = "trust"
//! ```

use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub listen: String,
    /// MVP：单 shard（0）；多 shard 在阶段 1（ShardMapper）
    pub shards: u32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:50051".into(),
            shards: 1,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StoreSection {
    Local {
        root: PathBuf,
    },
    Memory,
    S3 {
        bucket: String,
        endpoint: String,
        access_key_id: String,
        secret_access_key: String,
        #[serde(default)]
        allow_http: bool,
    },
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
pub struct WalSection {
    pub dir: PathBuf,
    /// segment 最大 64MB
    pub segment_max_mb: u64,
    /// 批次超时（秒，默认 1800）
    pub batch_timeout_secs: u64,
    /// 磁盘水位
    pub disk_high_watermark: f64,
}

impl Default for WalSection {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./data/wal"),
            segment_max_mb: 64,
            batch_timeout_secs: 1800,
            disk_high_watermark: 0.80,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
pub struct IngestSection {
    /// vortex | parquet（ADR-1 FormatSwitch）
    pub default_format: String,
    pub rows_threshold: u64,
    pub time_threshold_secs: u64,
    pub flush_jitter_secs: u64,
    pub scan_interval_ms: u64,
    /// 幂等键 TTL（小时）
    pub idempotency_ttl_hours: u64,
}

impl Default for IngestSection {
    fn default() -> Self {
        Self {
            default_format: "parquet".into(),
            rows_threshold: 10_000,
            time_threshold_secs: 5,
            flush_jitter_secs: 60,
            scan_interval_ms: 100,
            idempotency_ttl_hours: 24,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
pub struct CompactionSection {
    pub min_files: usize,
    pub interval_secs: u64,
}

impl Default for CompactionSection {
    fn default() -> Self {
        Self {
            min_files: 5,
            interval_secs: 60,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
pub struct QuerySection {
    pub cache_ttl_secs: u64,
}

impl Default for QuerySection {
    fn default() -> Self {
        Self { cache_ttl_secs: 30 }
    }
}

/// MySQL wire 账号（设计 §6.3：users 非空 → native_password）。
#[derive(Debug, Clone, serde::Deserialize)]
pub struct MysqlUser {
    pub user: String,
    pub password: String,
}

/// MySQL wire 端口（设计 §6.3 `[sql.mysql]`；R-2：默认标准端口 3306）。
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
pub struct MysqlSection {
    /// 关闭则不监听（默认开启）
    pub enabled: bool,
    /// 标准端口 3306；被占用时启动即报错（不静默降级）
    pub listen: String,
    /// trust | native_password（users 非空时自动切换）
    pub auth: String,
    pub users: Vec<MysqlUser>,
}

impl Default for MysqlSection {
    fn default() -> Self {
        Self {
            enabled: true,
            listen: "0.0.0.0:3306".into(),
            auth: "trust".into(),
            users: Vec::new(),
        }
    }
}

/// SQL 访问层端口配置（本期仅 mysql；PG wire 见设计 §5.5 后续扩展）。
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default)]
pub struct SqlSection {
    pub mysql: MysqlSection,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub store: StoreSection,
    pub wal: WalSection,
    pub ingest: IngestSection,
    pub compaction: CompactionSection,
    pub query: QuerySection,
    pub sql: SqlSection,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            store: StoreSection::Local {
                root: PathBuf::from("./data/store"),
            },
            wal: WalSection::default(),
            ingest: IngestSection::default(),
            compaction: CompactionSection::default(),
            query: QuerySection::default(),
            sql: SqlSection::default(),
        }
    }
}

impl Config {
    pub fn from_toml(s: &str) -> Result<Self, String> {
        toml::from_str(s).map_err(|e| format!("config parse: {e}"))
    }

    pub fn from_path(p: &std::path::Path) -> Result<Self, String> {
        let s = std::fs::read_to_string(p).map_err(|e| format!("read {}: {e}", p.display()))?;
        Self::from_toml(&s)
    }

    pub fn wal_config(&self) -> yuntun_wal::WalConfig {
        yuntun_wal::WalConfig {
            dir: self.wal.dir.clone(),
            segment_max_size: self.wal.segment_max_mb * 1024 * 1024,
            batch_timeout: Duration::from_secs(self.wal.batch_timeout_secs),
            disk_high_watermark: self.wal.disk_high_watermark,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_config() {
        let cfg = Config::from_toml(
            r#"
[server]
listen = "0.0.0.0:6000"
shards = 1

[store]
type = "local"
root = "/tmp/store"

[wal]
dir = "/tmp/wal"
batch_timeout_secs = 60

[ingest]
rows_threshold = 100
default_format = "parquet"

[compaction]
min_files = 3

[query]
cache_ttl_secs = 5
"#,
        )
        .unwrap();
        assert_eq!(cfg.server.listen, "0.0.0.0:6000");
        assert_eq!(cfg.ingest.rows_threshold, 100);
        assert_eq!(cfg.compaction.min_files, 3);
        assert_eq!(cfg.wal_config().batch_timeout, Duration::from_secs(60));
        assert!(matches!(cfg.store, StoreSection::Local { .. }));
    }

    #[test]
    fn parse_memory_store() {
        let cfg = Config::from_toml("[store]\ntype = \"memory\"").unwrap();
        assert!(matches!(cfg.store, StoreSection::Memory));
    }

    #[test]
    fn mysql_section_defaults_and_override() {
        // 默认：开启 + 标准端口（R-2）
        let cfg = Config::from_toml("").unwrap();
        assert!(cfg.sql.mysql.enabled);
        assert_eq!(cfg.sql.mysql.listen, "0.0.0.0:3306");
        assert_eq!(cfg.sql.mysql.auth, "trust");
        assert!(cfg.sql.mysql.users.is_empty());

        let cfg = Config::from_toml(
            r#"
[sql.mysql]
enabled = false
listen = "127.0.0.1:3307"
auth = "native_password"
users = [{ user = "yuntun", password = "secret" }]
"#,
        )
        .unwrap();
        assert!(!cfg.sql.mysql.enabled);
        assert_eq!(cfg.sql.mysql.listen, "127.0.0.1:3307");
        assert_eq!(cfg.sql.mysql.users.len(), 1);
        assert_eq!(cfg.sql.mysql.users[0].user, "yuntun");
    }

    #[test]
    fn defaults_work() {
        let cfg = Config::from_toml("").unwrap();
        assert_eq!(cfg.server.listen, "0.0.0.0:50051");
        assert_eq!(cfg.ingest.time_threshold_secs, 5);
    }
}
