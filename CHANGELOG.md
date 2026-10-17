# Changelog

所有显著变更记录于此。格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [SemVer](https://semver.org/lang/zh-CN/)。

## [0.1.0-alpha.2] - 2026-10-18

**预发布**：用于验证发布流水线（4 目标构建 / 包 / Release）。流水线验证通过后发布
`0.1.0`，功能内容不预期再变。

`alpha.1` 的首次流水线暴露了两个构建前置（libclang、musl C++），已修复：
aarch64-gnu 改用 ARM 原生 runner，musl 改用 musl.cc 工具链。

单节点（standalone）分析型数据湖 —— 直写摄入、可恢复存储、
两条 SQL 协议（Flight SQL / MySQL wire）共用一套语义。

### Added

- **写入与存储**：Arrow 批次 → WAL（CRC、组提交、fsync、批次状态机）
  → 攒批（按 `dt`/`shard` 分区）→ **vortex（默认）/ parquet** → Manifest；
  幂等键三通道（`app_metadata` / SQL 注释 / 语句级生成）；写入侧 schema 演化（追加列）；
  读己之写（fsync 后 ≤1 个扫描周期，默认 100 ms）。
- **崩溃恢复**：WAL 重放 + 批次分流（Pending 重做 / Committed 补记 / Abort 跳过）；
  DDL 重放恢复表与库；chaos 故障注入覆盖多轮硬崩溃无丢失无重复。
- **SQL 面**（`crates/sql` 为唯一实现，两端口共用）：
  - 查询：`SELECT`（CTE / 聚合 / JOIN / 窗口 / `information_schema`）、`EXPLAIN [ANALYZE]`；
  - 写入：`INSERT ... VALUES` / `INSERT ... SELECT` / `TRUNCATE TABLE`；
  - **行级 `DELETE ... WHERE` / `UPDATE ... SET ... WHERE`**（删除向量；UPDATE 原子可见）；
  - DDL：`CREATE/DROP TABLE`（`IF [NOT] EXISTS`、`CREATE TABLE new LIKE old`、
    `WITH (partition_by='ts')`）、`CREATE/DROP DATABASE`、`ALTER TABLE ADD/DROP COLUMN`；
  - 多 schema（`USE` / 限定名 / handshake database）、prepared statement、
    8 个 JSON 函数、二进制/无符号/时间/`DECIMAL`/`ARRAY`/`MAP` 类型。
- **协议端口**：Arrow Flight SQL（`do_get` 流式回传、`do_put` 批量写入、prepared）；
  MySQL wire（文本协议 + `COM_STMT_PREPARE/EXECUTE` 二进制参数与结果集，
  DBeaver/JDBC 元数据兼容含 `information_schema` 补洞）；可选指标 HTTP
  （`/metrics`、`/metrics.json`、`/healthz`）。
- **客户端**：`yuntun-client` SDK（`query` / `query_full`（partial 可见）/
  `query_stream` / `schema_of` / `table_schema` / `insert` / `insert_batches`）
  与 `yuntun-cli`（`query` / `insert`（CSV/JSONL/Parquet）/ `tables` / `schema` /
  `meta`）；令牌经 `--token` / `YUNTUN_TOKEN`，拦截器统一附加。
- **鉴权**：MySQL `mysql_native_password`（`users` 非空自动启用、**每连接随机 salt**）；
  Flight `auth_token`（`authorization: Bearer`）；默认 trust（按网络隔离部署）。
- **查询管理**：`SHOW [FULL] PROCESSLIST`、`KILL [CONNECTION|QUERY]`（真取消，
  未知 Id ⇒ 1094 / 被杀 ⇒ 1317）、`SHOW [GLOBAL|SESSION] STATUS [LIKE]`
  （真实计数器）、`[sql.mysql].max_connections` 连接准入闸门。
- **冷读预算**：vortex 整文件解码按**解码字节**累计，`[query].cold_read_budget_mb`
  （默认 512 MB）超限整次查询失败并点名文件。
- **运维**：结构化日志、SIGINT/SIGTERM 优雅关闭（WAL 封口、任务 join、租约释放）、
  compaction（按 `(table, shard)` 分租约）、孤儿文件 GC、任务目录、WAL 磁盘水位保护、
  内存硬分区 + spill。
- **可选分布式**（`yuntun-datanode` / `metanode`）：多节点写同一分片（查询侧合并、
  逐行对拍）、聚合下推（只回标量）、冷文件按节点分配与远端折叠、真实 S3（SeaweedFS/MinIO）。
- **测试基建**：testkit（tmpfs 测试目录）、chaos 故障注入、`standalone_smoke.py`（17 项）、
  `auth_smoke.py`（7 项）、pymysql/JDBC/pyarrow(ADBC) 冒烟脚本。
- **发布与交付**：GitHub Actions 在 tag 上产出 **x86_64 / aarch64 × glibc / musl** 四个包
  （`bin/{yuntun,yuntun-cli,yuntun-datanode,metanode}` + 示例配置 + 文档 + 许可证），
  附 `sha256` 与合并的 `SHA256SUMS`；`scripts/release_check.sh` 是发布门禁
  （clippy + 全量测试 + 文档一致性 + release 二进制冒烟）。

### Changed

- 默认存储格式为 **vortex**（ADR-1）；`.idx` 行组级索引只对 parquet 生效。
- DDL 解析方言回退：会话方言不认的扩展语法（如 `ADD COLUMN IF NOT EXISTS`）
  回退到 Generic 再试；两者都不认才报会话方言的错误。
- 结果完整性（partial）经 Flight SQL metadata 传给调用方：SDK `query_full` 给
  `PartialNotice`，CLI 把 `WARN: partial result` 打到 stderr。
- 构建不再依赖系统 `protoc`（`protoc-bin-vendored` 自带），CI / 交叉编译容器可复现。

### Performance

- 100 万行 DoPut 写入 2.6 s（10×10 万批次，**debug** 构建）；
- 500 万行 / 114 MB 结果集流式回传，服务端 RSS 增量 ≈ 2 MB；
- 分布式聚合下推：3 万行查询的网络量从 244 KB 降到 64 字节（3814×）。

### Known limitations

- 无事务（单语句自动提交，`BEGIN/COMMIT/ROLLBACK` 为 no-op）、无视图/存储过程；
- 鉴权只有**登录**粒度：无 TLS、无 `GRANT` 权限矩阵、metanode 端口无鉴权；默认 trust；
- MySQL 轨结果集先收集后逐行写，且无法携带 partial 标记（上游 opensrv 0.7 限制）；
- vortex 读路径无行组级剪枝，单文件解码峰值 ≈ 该文件解码后大小（流式解码未做）；
- 查询管理只有**连接级**闸门（无按查询限流）；`SHOW STATUS WHERE` 不解释；
  超限连接被关闭而非收到 ERR 1040；Flight 轨无 PROCESSLIST/KILL/STATUS；
- `CREATE TABLE ... AS SELECT` 未支持（用 `CREATE TABLE` + `INSERT ... SELECT`）；
- `ALTER TABLE` 只支持增删列（改类型/可空性/重命名明确拒绝）；`TIMESTAMP(p)` 精度不入 schema；
- 客户端没有发现/软路由：连接显式给定的 contact point，扇出由服务端负责。

### 升级说明

0.1 之前没有对外产物，无需迁移。
