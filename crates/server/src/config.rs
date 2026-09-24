//! 配置（详细设计 §11 配置项清单 + 架构 §2.7/§2.8/§5.2/§5.3 新增项）。
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
//! [chunk]
//! spill_dir = "./data/spill"          # 必须本地磁盘（架构 §2.5）
//! instance_id = "standalone"
//! mem_budget_mb = 512                 # chunk 区（架构 §2.8 硬分区之一）
//! query_mem_budget_mb = 512           # query 执行区（另一块，超限直接报错）
//! soft_pct = 60                       # 背压阶梯（架构 §2.7）
//! hard_pct = 80
//! reject_pct = 95
//! metrics_log_interval_secs = 30     # 指标周期打点（0 = 关闭，T6.12）
//!
//! [ingest]
//! default_format = "parquet"
//! rows_threshold = 500000             # seal 触发：让 RowGroup 一次成型（S1-10）
//! bytes_threshold_mb = 128            # seal 触发：字节阈值
//! time_threshold_secs = 5             # 最短驻留地板（seal 时刻由窗口关闭决定，ADR-10）
//! max_flush_delay_secs = 0            # seal → flush 宽限期（0 = 封口即到期，T8 定案）
//! chunk_max_resident_secs = 60        # 强制 seal+flush，防慢写入流撑爆 WAL（S1-9）
//! flush_phase_spread_secs = 30        # 确定性相位偏移上限（替代随机 jitter，S2-9；T8 定案）
//!
//! [compaction]
//! min_files = 5
//!
//! [query]
//! cache_ttl_secs = 30
//! partial = "allow"                  # 读不到某个来源时：allow（默认，返回部分结果 + 标记）
//!                                    # 或 reject（当场失败并点名缺了谁）
//! hot_read_budget_secs = 10          # 整段热读的总预算（超时即按"拿不到"降级；0 = 非法）
//!
//! [sql.mysql]
//! enabled = true
//! listen = "0.0.0.0:3306"
//! auth = "trust"
//! ```
//!
//! > **已移除**：`[ingest].flush_jitter_secs`（随机 jitter 会污染持久化上界，
//! > 改为确定性相位偏移，见架构 §5.3 / S2-9）。旧的 `idle_timeout` 语义由
//! > `chunk_max_resident_secs` 取代（后者更强：强制 seal **且** flush）。

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
    /// seal 触发：行数阈值（架构 §5.4；S1-10 默认 50 万行，让 RowGroup 一次成型）
    pub rows_threshold: u64,
    /// seal 触发：字节阈值（MB，内存口径）
    pub bytes_threshold_mb: u64,
    /// **最短驻留地板**（秒）：见 `IngestorConfig::time_threshold_secs`。
    /// 时间维度的 seal 时刻由**到达分钟窗口关闭**决定（ADR-10），本值是地板。
    pub time_threshold_secs: u64,
    /// seal → flush 的宽限期（秒）：`flush_at = sealed_at + max_flush_delay`
    pub max_flush_delay_secs: u64,
    /// 强制 seal + flush 的最大驻留秒数（S1-9：防慢写入流把 WAL 撑爆）
    pub chunk_max_resident_secs: u64,
    /// 确定性相位偏移上限（秒，S2-9：替代随机 jitter，防惊群）
    pub flush_phase_spread_secs: u64,
    pub scan_interval_ms: u64,
    /// 幂等键 TTL（小时）
    pub idempotency_ttl_hours: u64,
}

impl Default for IngestSection {
    fn default() -> Self {
        Self {
            default_format: "parquet".into(),
            rows_threshold: 500_000,
            bytes_threshold_mb: 128,
            time_threshold_secs: 5,
            max_flush_delay_secs: 0,
            chunk_max_resident_secs: 60,
            flush_phase_spread_secs: 30,
            scan_interval_ms: 100,
            idempotency_ttl_hours: 24,
        }
    }
}

/// chunk 层配置（架构 §2.2 / §2.5 / §2.7 / §2.8）。
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
pub struct ChunkSection {
    /// spill 目录（**必须本地磁盘**：spill 是节点私有状态，架构 §2.5）
    pub spill_dir: PathBuf,
    /// 实例标识：确定性 flush 相位 hash 输入 + `FileManifest.source_instance`（架构 §4.4）
    pub instance_id: String,
    /// chunk 区内存上限（MB）：**必须保底**（架构 §2.8）
    pub mem_budget_mb: u64,
    /// query 执行区内存上限（MB）：超限直接报错，**绝不抢占 chunk 区**
    pub query_mem_budget_mb: u64,
    /// 背压阶梯：>= soft 后台 spill（架构 §2.7）
    pub soft_pct: u8,
    /// 背压阶梯：>= hard 强制 seal 并 spill
    pub hard_pct: u8,
    /// 背压阶梯：>= reject 拒绝写入（RESOURCE_EXHAUSTED）
    pub reject_pct: u8,
    /// 指标周期打点间隔（秒，T6.12）；`0` = 关闭。
    ///
    /// 打点内容是"故障现场三件套"：内存水位 / WAL 积压 / 背压水位 + Catalog 版本，
    /// 用一条结构化日志输出（standalone 阶段即运维的第一手现场）。
    pub metrics_log_interval_secs: u64,
}

impl Default for ChunkSection {
    fn default() -> Self {
        Self {
            spill_dir: PathBuf::from("./data/spill"),
            instance_id: "standalone".into(),
            mem_budget_mb: 512,
            query_mem_budget_mb: 512,
            soft_pct: 60,
            hard_pct: 80,
            reject_pct: 95,
            metrics_log_interval_secs: 30,
        }
    }
}

impl ChunkSection {
    /// 背压阈值（百分比 → 比例；越界值退化为默认，避免配置写错就静默放大内存）。
    pub fn pressure_thresholds(&self) -> yuntun_chunk::PressureThresholds {
        let d = yuntun_chunk::PressureThresholds::default();
        let to_ratio = |pct: u8, fallback: f64| {
            if pct == 0 || pct > 100 {
                fallback
            } else {
                pct as f64 / 100.0
            }
        };
        let soft = to_ratio(self.soft_pct, d.soft);
        let hard = to_ratio(self.hard_pct, d.hard);
        let reject = to_ratio(self.reject_pct, d.reject);
        yuntun_chunk::PressureThresholds {
            soft,
            hard,
            reject,
        }
    }

    pub fn chunk_mem_bytes(&self) -> usize {
        (self.mem_budget_mb as usize).saturating_mul(1024 * 1024)
    }

    pub fn query_mem_bytes(&self) -> usize {
        (self.query_mem_budget_mb as usize).saturating_mul(1024 * 1024)
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

/// 元数据（catalog）的装配形态。
///
/// **这是设计指定的回滚点**（`metanode-design §3.3` + S3-4 验收）：
/// 换成 `Memory` 就退回"进程内内存实现"，不需要改任何业务代码 —— 因为业务侧只认
/// `Arc<dyn CatalogOps>`（`§52` 拆掉的接缝）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetaMode {
    /// 进程内内存实现（阶段 0 形态；重启即空，靠 WAL 的 DDL 重放重建）
    Memory,
    /// 进程内 **1 节点 metanode**（raft + fjall 落盘）+ **loopback gRPC**（设计 §3.3 的 standalone 形态）
    Embedded,
}

/// `[meta]` 段。
///
/// 段级 `#[serde(default)]`：字段可省略（各自取 `Default`）。这样**回滚开关只要一行**：
/// `[meta] mode = "memory"` —— 回滚操作越简单，真出事时才越敢按。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct MetaSection {
    pub mode: MetaMode,
    /// embedded 的 gRPC 监听地址；`127.0.0.1:0` = 内核分配（推荐：同机没必要固定端口）
    pub listen: String,
    /// embedded 的落盘目录。
    ///
    /// **省略 = 进程内临时目录**（测试友好；但**重启会丢掉元数据** → 会打 warn）。
    /// 生产（`yuntun` 二进制）默认给 `./data/meta`（见 `standalone`），请显式配置。
    pub dir: Option<std::path::PathBuf>,
}

impl Default for MetaSection {
    fn default() -> Self {
        Self {
            // 默认即"设想的形态"（设计 §3.3：standalone = 1 节点 raft + 本地传输）。
            // 回滚 = 配置里写 `mode = "memory"`。
            mode: MetaMode::Embedded,
            listen: "127.0.0.1:0".into(),
            dir: None,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
pub struct QuerySection {
    pub cache_ttl_secs: u64,
    /// 部分结果策略（`architecture §4.2`）：`"allow"`（默认）或 `"reject"`。
    ///
    /// `allow` = 某个来源读不到时**降级为部分结果并标记缺失来源**；
    /// `reject` = 当场失败并点名缺了谁。
    /// 用字符串而不是枚举：配置面保持窄（解析失败会在启动时**报错退出**，见装配层）。
    pub partial: String,
    /// **整段热读的总预算**（秒，`§88`）：一次查询在热数据上最多等多久。
    ///
    /// 与 `partial` 同一条纪律：**写错（0）启动即报错** —— 预算为 0 等于"所有热读都超时"，
    /// 那不是一个配置，是一个把 partial 恒真的开关，静默接受它会让用户以为"我配的是完整的"。
    pub hot_read_budget_secs: u64,
}

impl Default for QuerySection {
    fn default() -> Self {
        Self {
            cache_ttl_secs: 30,
            partial: "allow".into(),
            hot_read_budget_secs: 10,
        }
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
    /// 元数据（catalog）的装配形态 —— **S3-4 的装配层开关**（设计指定的回滚点）。
    /// `#[serde(default)]`：老配置文件（没有 `[meta]` 段）必须照样能读 ——
    /// 加一个必填段等于把所有既有部署一次性打挂。
    #[serde(default)]
    pub meta: MetaSection,
    pub store: StoreSection,
    pub wal: WalSection,
    pub chunk: ChunkSection,
    pub ingest: IngestSection,
    pub compaction: CompactionSection,
    pub query: QuerySection,
    pub sql: SqlSection,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            meta: MetaSection::default(),
            store: StoreSection::Local {
                root: PathBuf::from("./data/store"),
            },
            wal: WalSection::default(),
            chunk: ChunkSection::default(),
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

    /// 配置自检：列出"能启动但会静默劣化"的问题（**不阻断启动**，由装配层打日志）。
    ///
    /// 例：`chunk_max_resident_secs <= max_flush_delay_secs + flush_phase_spread_secs`
    /// 会让"驻留硬兜底"早于正常 flush 到期触发，从而**绕过相位分散**——
    /// 所有实例重新在同一秒 flush，ADR-10 的惊群问题复活，但功能测试全绿。
    pub fn warnings(&self) -> Vec<String> {
        let mut out = Vec::new();
        if self.meta.mode == MetaMode::Embedded && self.meta.dir.is_none() {
            out.push(
                "[meta] mode = embedded 但未配置 dir：元数据只写**进程内临时目录**，\
                 重启即等于换了一个新集群（生产请显式配置 [meta] dir）"
                    .into(),
            );
        }
        let max_resident = self.ingest.chunk_max_resident_secs;
        let normal_deadline = self.ingest.max_flush_delay_secs + self.ingest.flush_phase_spread_secs;
        if max_resident <= normal_deadline {
            out.push(format!(
                "[ingest] chunk_max_resident_secs({max_resident}) 必须 > max_flush_delay_secs({}) + \
                 flush_phase_spread_secs({})；否则驻留硬兜底绕过相位分散，flush 会重新聚集在同一秒",
                self.ingest.max_flush_delay_secs, self.ingest.flush_phase_spread_secs
            ));
        }
        if self.ingest.rows_threshold == 0 && self.ingest.bytes_threshold_mb == 0 {
            out.push(
                "[ingest] rows_threshold 与 bytes_threshold_mb 同时为 0：seal 只能靠窗口关闭"
                    .into(),
            );
        }
        let t = self.chunk.pressure_thresholds();
        if !(t.soft < t.hard && t.hard < t.reject) {
            out.push(format!(
                "[chunk] 背压水位必须 soft < hard < reject（当前 {}/{}/{}）",
                t.soft, t.hard, t.reject
            ));
        }
        if self.chunk.chunk_mem_bytes() == 0 {
            out.push("[chunk] mem_budget_mb = 0：chunk 区无内存预算，写入会被背压直接拒绝".into());
        }
        out
    }

    /// 展开为 Ingestor 配置（**配置 → 运行时映射的唯一入口**，避免装配处散落换算）。
    pub fn ingestor_config(&self) -> yuntun_ingest::IngestorConfig {
        yuntun_ingest::IngestorConfig {
            default_format: yuntun_format::DataFormat::parse(&self.ingest.default_format),
            instance_id: self.chunk.instance_id.clone(),
            spill_dir: self.chunk.spill_dir.clone(),
            rows_threshold: self.ingest.rows_threshold as usize,
            bytes_threshold: (self.ingest.bytes_threshold_mb as usize) * 1024 * 1024,
            time_threshold_secs: self.ingest.time_threshold_secs,
            max_flush_delay_secs: self.ingest.max_flush_delay_secs,
            chunk_max_resident_secs: self.ingest.chunk_max_resident_secs,
            flush_phase_spread_secs: self.ingest.flush_phase_spread_secs,
            scan_interval: Duration::from_millis(self.ingest.scan_interval_ms),
            chunk_mem_budget: self.chunk.chunk_mem_bytes(),
            idempotency_ttl: Duration::from_secs(self.ingest.idempotency_ttl_hours * 3600),
            ..Default::default()
        }
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

    #[test]
    fn chunk_section_defaults_follow_architecture_bounds() {
        let cfg = Config::from_toml("").unwrap();
        // 架构 §2.8：两块预算独立
        assert_eq!(cfg.chunk.mem_budget_mb, 512);
        assert_eq!(cfg.chunk.query_mem_budget_mb, 512);
        assert_eq!(cfg.chunk.chunk_mem_bytes(), 512 * 1024 * 1024);
        // 架构 §2.7：60/80/95 三级
        let t = cfg.chunk.pressure_thresholds();
        assert_eq!((t.soft, t.hard, t.reject), (0.60, 0.80, 0.95));
        // 架构 §5.2：持久化硬上界（= md + spread）与可见性软目标分离，且驻留兜底更晚
        //
        // ⚠️ 不得断言 `max_flush_delay_secs > time_threshold_secs` —— 这是**旧默认值
        // 遗留的错觉**：两者量纲无关（一个是"seal 后多久 flush"，一个是"窗口关闭前
        // 最短驻留"），T8 定案后前者为 0。真正的不变量只有下面这条（`warnings()` 同款）。
        assert_eq!(cfg.ingest.max_flush_delay_secs, 0);
        assert_eq!(cfg.ingest.flush_phase_spread_secs, 30);
        assert!(
            cfg.ingest.chunk_max_resident_secs
                > cfg.ingest.max_flush_delay_secs + cfg.ingest.flush_phase_spread_secs,
            "驻留兜底必须晚于正常到期（否则绕过相位分散）"
        );
    }

    #[test]
    fn chunk_section_parses_and_overrides() {
        let cfg = Config::from_toml(
            r#"
[chunk]
spill_dir = "/tmp/yuntun-spill"
instance_id = "datanode-1"
mem_budget_mb = 64
query_mem_budget_mb = 256
soft_pct = 50
hard_pct = 70
reject_pct = 90
"#,
        )
        .unwrap();
        assert_eq!(cfg.chunk.instance_id, "datanode-1");
        assert_eq!(cfg.chunk.chunk_mem_bytes(), 64 * 1024 * 1024);
        assert_eq!(cfg.chunk.query_mem_bytes(), 256 * 1024 * 1024);
        let t = cfg.chunk.pressure_thresholds();
        assert_eq!((t.soft, t.hard, t.reject), (0.50, 0.70, 0.90));
    }

    #[test]
    fn bogus_pressure_pct_falls_back_instead_of_silently_enlarging_budget() {
        // 0 / >100 是配置错误：退化到默认水位，而不是把阈值变成 0（等于永不 spill）
        let cfg = Config::from_toml("[chunk]\nsoft_pct = 0\nhard_pct = 200\nreject_pct = 0").unwrap();
        let t = cfg.chunk.pressure_thresholds();
        assert_eq!((t.soft, t.hard, t.reject), (0.60, 0.80, 0.95));
    }

    #[test]
    fn ingestor_config_maps_every_bound() {
        let cfg = Config::from_toml(
            r#"
[chunk]
instance_id = "node-7"
spill_dir = "/tmp/sp"
mem_budget_mb = 8

[ingest]
rows_threshold = 123
bytes_threshold_mb = 3
time_threshold_secs = 2
max_flush_delay_secs = 7
chunk_max_resident_secs = 9
flush_phase_spread_secs = 1
scan_interval_ms = 50
"#,
        )
        .unwrap();
        let ic = cfg.ingestor_config();
        assert_eq!(ic.instance_id, "node-7");
        assert_eq!(ic.rows_threshold, 123);
        assert_eq!(ic.bytes_threshold, 3 * 1024 * 1024);
        assert_eq!(ic.max_flush_delay_secs, 7);
        assert_eq!(ic.chunk_max_resident_secs, 9);
        assert_eq!(ic.chunk_mem_budget, 8 * 1024 * 1024);
        assert_eq!(ic.scan_interval, Duration::from_millis(50));
        // seal 策略由配置一处展开（不散落在装配代码里）
        let p = ic.seal_policy();
        assert_eq!(p.max_flush_delay, Duration::from_secs(7));
        assert_eq!(p.max_resident, Duration::from_secs(9));
        assert_eq!(p.phase_spread, Duration::from_secs(1));
    }

    #[test]
    fn legacy_flush_jitter_key_is_ignored_not_fatal() {
        // 旧配置里的 flush_jitter_secs 已移除：旧 TOML 仍应可解析（未知字段忽略），
        // 但**不再影响** flush 时刻（改为确定性相位偏移，架构 §5.3）
        let cfg = Config::from_toml("[ingest]\nrows_threshold = 5\nflush_jitter_secs = 60").unwrap();
        assert_eq!(cfg.ingest.rows_threshold, 5);
        assert_eq!(cfg.ingest.flush_phase_spread_secs, 30);
    }

    #[test]
    fn shipped_example_config_stays_in_sync_with_parser() {
        // 示例配置是运维的第一份文档：必须能被当前解析器完整理解，
        // 否则"注释里写着、代码已改名"的漂移会静默生效（未知键被忽略）。
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("yuntun.toml.example");
        let cfg = Config::from_path(&path).expect("yuntun.toml.example 必须可解析");
        assert_eq!(cfg.chunk.instance_id, "standalone");
        assert_eq!(cfg.chunk.pressure_thresholds().soft, 0.60);
        assert_eq!(cfg.ingest.rows_threshold, 500_000);
        assert_eq!(cfg.ingest.max_flush_delay_secs, 0);
        assert!(cfg.sql.mysql.enabled);
        assert!(cfg.warnings().is_empty(), "示例配置不得有潜在劣化项: {:?}", cfg.warnings());
    }

    #[test]
    fn warnings_flag_resident_ceiling_that_bypasses_phase_spread() {
        // 默认配置满足不变量
        // 默认配置只有一条告警：`[meta] mode = embedded` 但没配 `dir`
        //（= 元数据写在进程内临时目录，重启即新集群）。这是**刻意**的默认：
        // 测试/试跑不该往工作目录里写东西；生产请在配置里显式给 `[meta] dir`。
        let w = Config::default().warnings();
        assert_eq!(w.len(), 1, "默认配置应当只有 meta.dir 这一条告警：{w:?}");
        assert!(w[0].contains("[meta]"), "{w:?}");
        // 硬兜底 ≤ 正常到期 → 会绕过相位分散（功能全绿但惊群复活）
        // `[meta] dir` 显式给上：本用例只关心 ingest 那条告警，
        // 不该被"embedded 没配目录"那条干扰（否则加一条无关告警就会打红它）。
        let cfg = Config::from_toml(
            "[meta]\nmode = \"memory\"\n[ingest]\nmax_flush_delay_secs = 30\nflush_phase_spread_secs = 60\nchunk_max_resident_secs = 60",
        )
        .unwrap();
        let w = cfg.warnings();
        assert_eq!(w.len(), 1, "{w:?}");
        assert!(w[0].contains("绕过相位分散"), "{w:?}");
        // 零预算：写入必被拒
        let cfg = Config::from_toml("[chunk]\nmem_budget_mb = 0").unwrap();
        assert!(cfg.warnings().iter().any(|w| w.contains("内存预算")));
    }
}
