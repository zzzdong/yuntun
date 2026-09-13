# Changelog

所有显著变更记录于此。格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [SemVer](https://semver.org/lang/zh-CN/)。

## [0.1.0] - 2026-09-12

首个实验性发布：**单节点（standalone）分析型 lakehouse**——append-only 时序/事件数据
的写入、可恢复存储与 SQL 查询。

### Added

- **写入管线**：Arrow 批次 → WAL（fsync、组提交、批次状态机 Pending/S3Written/Committed）
  → Parquet（`dt`/`shard` 分区）→ Manifest；幂等键去重（app_metadata / SQL 注释 /
  语句级生成三通道）；schema 演进（追加列，OCC + 冲突重试）。
- **崩溃恢复**：WAL 重放 + 批次分流（Pending 重做 flush / Committed 补记 / Abort 跳过），
  chaos 故障注入用例覆盖硬崩溃多轮无丢失无重复。
- **查询**：DataFusion 55（`SELECT` / 聚合 / CTE），本地 catalog 缓存，小文件 compaction，
  Flight 轨结果集流式回传。
- **多 schema**：`CREATE/DROP DATABASE`、`USE` 真实切换（MySQL wire）、跨库限定名查询、
  对象路径按 schema 分层；schema 与表定义均由 WAL DDL 重放恢复。
- **协议端口**：
  - FlightSQL 标准轨：查询 / DDL / `INSERT ... VALUES` / DoPut 批量写入 / prepared statement；
  - MySQL wire（`:3306`）：文本协议、预编译写入与查询（COM_STMT_PREPARE/EXECUTE，
    二进制参数与二进制结果集）、DBeaver 全兼容元数据（DatabaseMetaData +
    `SHOW ...` + `information_schema` 补全）。
- **客户端**：`yuntun-client` SDK（Rust，简易轨批量写入 + 查询）与 `yuntun-cli`
  （`query` / `insert` / `tables` / `schema` 子命令）。
- **运维**：`yuntun.toml` 配置、写后可见性窗口可调、孤儿文件清理、WAL 磁盘水位保护。
- **测试基建**：testkit（tmpfs 测试目录，fsync 基准 64×）、chaos 故障注入套件、
  pymysql / JDBC / pyarrow(ADBC) 冒烟脚本。

### Performance

- 100 万行 DoPut 写入 2.6s（10×10 万批次，debug 构建）；
- 500 万行 / 114 MB 结果集流式回传，服务端 RSS 增量 ≈ 2 MB。

### Known limitations

详见 README §4：无事务（单语句自动提交）；trust 鉴权（需网络隔离部署）；
MySQL wire 结果集先收集后逐行写（Flight 轨已流式）。
（更正：早期版本此处曾写"预编译查询为文本结果集"——有误，opensrv 的
COM_STMT_EXECUTE 本就走二进制行；当时 prepared SELECT 取不到行的真因是
opensrv 0.7 PacketReader 的 UAF 触发条件，见 docs/operation-log.md §22。）
