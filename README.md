# yuntun

单机优先的**直写数据湖**（时序 / 可观测场景）：`WAL 权威 → 攒批 → 对象存储`，
对外暴露 **Arrow Flight SQL（gRPC）** 与 **MySQL wire（:3306）** 两个 SQL 协议端口，
两者共用同一套 SQL 语义实现（`crates/sql`）。

> **现状与下一步**见 [`docs/status.md`](docs/status.md)（唯一现状入口）；
> 文档索引与冲突裁决顺序见 [`docs/README.md`](docs/README.md)。
> 设计文档：[`plan.md`](docs/plan.md)（任务书 v2.2 / 阶段划分）、`architecture.md`（架构 + ADR）、
> `design.md`（详细设计）、`refactor.md`（分布式改造指南）、`sql-access-design.md`（多协议接入）、
> `operation-log.md`（实施日志与证据）。

---

## 1. 快速开始

### 构建与启动

```bash
cargo build --release                                   # 或 cargo build（debug）
./target/release/yuntun --config yuntun.toml            # bin 名 = yuntun
./target/release/yuntun --version
```

配置文件模板见 [`yuntun.toml.example`](yuntun.toml.example)（复制为 `yuntun.toml` 后改端口/目录即可）：

```toml
[server]
listen = "0.0.0.0:50051"      # Arrow Flight / Flight SQL

[store]
type = "local"                # local | memory | s3
root = "./data/store"

[sql.mysql]
enabled = true
listen = "0.0.0.0:3306"       # MySQL wire（标准端口，R-2）
auth = "trust"
# users = [{ user = "yuntun", password = "..." }]
```

启动日志出现两条监听即就绪：

```text
INFO  yuntun_server: mysql wire protocol listening listen=0.0.0.0:3306
INFO  yuntun_server: flight server listening (do_put ingest + do_get sql) addr=0.0.0.0:50051
```

### 最小闭环（MySQL 端口）

```python
import pymysql

conn = pymysql.connect(host="127.0.0.1", port=3306, user="yuntun", database="public")
with conn, conn.cursor() as cur:
    cur.execute("CREATE TABLE api_audit (event_time TIMESTAMP, user TEXT, endpoint TEXT, cost_ms INT)")
    cur.execute("INSERT INTO api_audit VALUES (TIMESTAMP '2026-09-11 01:00:00', 'alice', '/v1/put', 12)")
    # 写入先落 WAL；攒批线程在一个扫描周期内把它放进 store 层"内存分片" → 立即可查（读己之写，默认 ≤1s）
    cur.execute("SELECT user, count(*) AS cnt FROM api_audit GROUP BY user")
    print(cur.fetchall())          # [('alice', 1)]
```

### 最小闭环（Flight SQL 端口）

```python
from adbc_driver_flightsql import dbapi

with dbapi.connect("grpc+tcp://127.0.0.1:50051") as conn, conn.cursor() as cur:
    cur.execute("SELECT count(*) AS c FROM yuntun.public.api_audit")
    print(cur.fetchall())
```

### 最小闭环（`yuntun-cli`，自有客户端）

```bash
CLI=./target/release/yuntun-cli          # --addr 默认 127.0.0.1:50051（或环境变量 YUNTUN_ADDR）

$CLI query 'CREATE TABLE cpu (ts BIGINT, host TEXT, usage DOUBLE)'
$CLI insert -t cpu -f data.csv           # 支持 .csv / .jsonl / .parquet（按列名对齐 + 类型 cast）
$CLI insert -t cpu                       # 无 -f 时从 stdin 读 CSV（列顺序需与表一致）
$CLI query 'SELECT host, avg(usage) FROM cpu GROUP BY host ORDER BY host'
$CLI query 'SELECT * FROM cpu LIMIT 5' --format csv     # table（默认）| csv | json
$CLI tables
$CLI schema cpu
```

> 写入返回回执（行数 / WAL seq / 预计可见秒数）；SQL 入口与 DoPut 汇入同一 ingest 管线。

---

## 2. 连接方式

### 2.1 MySQL 端口（:3306）

| 客户端 | 连接参数 |
|---|---|
| PyMySQL / mysql-connector-python | `host=127.0.0.1, port=3306, user="yuntun", password="", database="public"` |
| DBeaver / JDBC | 见 §2.2 |
| mysql CLI | `mysql -h 127.0.0.1 -P 3306 -u yuntun public`（trust 鉴权：任意用户名、空密码） |

SQL 支持面（详见 `docs/sql-access-design.md` §4）：

| 类别 | 支持 |
|---|---|
| 查询 | `SELECT`（含 CTE / 聚合 / `information_schema.*`），非限定表名 `FROM t` 直接可用 |
| 写入 | `INSERT ... VALUES` / `INSERT ... SELECT` |
| DDL | `CREATE TABLE`（`IF NOT EXISTS` / `NOT NULL`；类型：`TINYINT/SMALLINT/INT/BIGINT`（含无符号）、`FLOAT/DOUBLE`、`BOOLEAN`、`DATE`、`DATETIME/TIMESTAMP`、`DECIMAL(p,s)`、`CHAR/VARCHAR(n)/TEXT/STRING`、`JSON/JSONB`（→ UTF8 文本存储）、`BINARY/VARBINARY/BLOB`、`ARRAY<T>` / `T[]`、`MAP(K,V)`（嵌套元素递归支持，Parquet round-trip 无损）/ `DROP TABLE`（`IF EXISTS`）/ `CREATE DATABASE` / `DROP DATABASE`（均 WAL 权威，重启可恢复） |
| 元数据 | `SHOW TABLES/FULL TABLES`、`SHOW COLUMNS/FULL COLUMNS`、`DESCRIBE`、`SHOW CREATE TABLE`、`SHOW DATABASES`、`SHOW VARIABLES`、`SHOW COLLATION/CHARSET/ENGINES/KEYS` |
| 方言兼容 | `SET` / `USE` / `BEGIN` / `COMMIT` / `ROLLBACK` = no-op（单语句自动提交） |
| 不支持 | `UPDATE` / `DELETE` / `ALTER TABLE` / `CREATE TABLE AS SELECT` / `CREATE OR REPLACE` / 复杂类型（`STRUCT` 等）/ `_BINARY` 字面量 / MySQL JSON 路径操作符 `->` `->>`（用 JSON 函数替代）/ 视图 / 存储过程 / 事务语义 → 明确报错（绝不静默返回错误结果）；加列走写入侧 schema 演化（ingest / Flight DoPut），无 SQL `ALTER` |
| JSON 函数 | `json_get / json_get_{str,int,float,bool,json,array}` / `json_as_text` / `json_contains` / `json_length` / `json_object_keys` / `json_from_scalar`（`datafusion-functions-json` 0.55）。**路径不带 `$`**：`json_get_int(doc, 'a.b')`；`json_get` 返回 JSON 变体联合（wire 按文本回传） |

### 2.2 DBeaver（MySQL）连接

1. `数据库` → `新建连接` → 选择 **MySQL** → 下一步；
2. **常规**：主机 `127.0.0.1`、端口 `3306`、数据库 `public`、用户名 `yuntun`、密码留空；
3. **驱动属性**（右键连接 → 编辑连接 → 驱动属性）：
   - `useServerPrepStmts` = **false**（文本轨最稳；服务端预编译已可用并覆盖 T2，
     见 §3 兼容矩阵）
   - `useSSL` = `false`、`allowPublicKeyRetrieval` = `true`
4. `测试连接` → 应看到 `MySQL 8.0.32-yuntun`；展开 `public` 即可浏览表 / 列、预览数据。

脚本化等价验证（用 DBeaver 自带的 Connector/J，无需 GUI）：

```bash
JAR=~/.local/share/DBeaverData/drivers/maven/maven-central/mysql/mysql-connector-java-8.0.29.jar
javac -cp $JAR scripts/dbeaver_jdbc_probe.java
java -cp "$JAR:scripts" dbeaver_jdbc_probe
```

覆盖 22 项：DatabaseMetaData（catalogs / tables / columns / primary keys / index info）
+ 数据预览 + `DESCRIBE` / `SHOW ...` / `information_schema`。

---

## 3. 客户端兼容矩阵

| 级别 | 目标 | 状态 |
|---|---|---|
| **T1** | mysql CLI / 文本协议：SELECT、SHOW TABLES、INSERT | ✅（`scripts/pymysql_smoke.py`） |
| **T2** | 驱动预编译（COM_STMT_PREPARE/EXECUTE）：写入 + **prepared SELECT 取回真实行** | ✅（mysql-connector C 扩展 / libmysqlclient 二进制协议栈；`scripts/pymysql_smoke.py` T2） |
| **T3** | DBeaver 连接 / 表·列浏览 / 数据预览 | ✅（`scripts/dbeaver_jdbc_probe.java` + 手工清单，见 `docs/operation-log.md` §15） |
| **T4**（非目标） | 事务、权限、视图、存储过程、PG wire | ❌ 明确报错或 no-op |

---

## 4. 已知限制（MVP）

- **无事务**：单语句自动提交，`BEGIN/COMMIT/ROLLBACK` 为 no-op；
- **trust 鉴权**：`[sql.mysql].users` 非空仅告警，当前不做口令校验——请按网络隔离部署；
- **写后可查（读己之写）**：`INSERT` 成功即落 WAL；攒批线程在一个扫描周期（`[ingest].scan_interval_ms`，
  默认 100ms）内把该批数据发布到 store 层的**内存分片**，查询立即可见 —— **不再受 flush 相位分散（默认 ≤30s）
  与查询缓存 TTL（默认 30s）影响**（`docs/plan.md` §4.5 / `docs/design.md` §7.4）；
  数据落对象存储（Parquet + Manifest）仍按攒批窗口进行，落盘后进入快照隔离 / Compaction 语义；
  回执的 `expected_visible_in_secs` = 该扫描周期上界；
- **服务端预编译（prepared）**：已可用（写入 + prepared SELECT 二进制结果集，见 T2）。
  历史故障根因是 opensrv-mysql **0.7.0** 的 `PacketReader::next_async` 释放后使用
  （上游 #66/#67，仅修在 git、未随 crates.io 发布）→ 已由 sqlwire 的
  `PacketFramedReader`（按包边界分帧）在协议层规避；上游发新版后可移除该适配器。
  握手版本串与 `SHOW VARIABLES` 同源（`8.0.32-yuntun`）；
- **结果集**：Flight 轨（`do_get`）**已流式**（S1.10：边算边发，500 万行 / 114 MB 级
  结果集服务端 RSS 增量 ≈ 2 MB）；MySQL wire 轨仍为逐行写但结果先收集（后续按需优化）；
- **SQL DDL 时间精度**：`TIMESTAMP(p)` 的精度 `p` 目前不落入 schema（一律 `Timestamp(ns)`）。
  需要毫秒精度的场景（如 `scripts/pyarrow_smoke.py` 的 prepared 绑定写入要求表的
  `event_time` 为 `Timestamp(ms)`）请用 demo 种子（Rust API）建表——该脚本因此
  **依赖 demo 种子实例**（`YUNTUN_DEMO_SERVE=1 cargo run -p yuntun-server --example demo`）；
- **opensrv-mysql 依赖**：crates.io 停在 **0.7.0**（2024-02），上游后续修复
  （UAF #66/#67、OK 包合规 #75/#78 等）仅存在于 git。0.1 以 sqlwire 的
  `PacketFramedReader`（按包边界分帧）规避 UAF；后续建议把依赖切到上游 git 固定 rev，
  届时可移除该适配器（`vendor/opensrv-mysql/` 仅为分析用拷贝，未接入构建）；
- **测试稳定性**：`chaos::crash_recovery_no_data_loss` 在全量并行（真实磁盘 I/O 竞争）
  下偶发失败，单独运行稳定；断言的固定等待后续改轮询。

### 多 schema（MySQL 的 database）

一个 catalog（`yuntun`）下支持任意多个 schema，MySQL 客户端的 `database` 即 schema：

```sql
CREATE DATABASE sales;                        -- 也可 CREATE SCHEMA
USE sales;                                    -- 切换会话默认 schema（MySQL wire）
CREATE TABLE orders (ts BIGINT, amt BIGINT);  -- 归属 sales
INSERT INTO orders VALUES (1, 100);
SELECT * FROM sales.orders;                   -- 任意会话可用限定名跨库查询
SHOW DATABASES;                               -- Flight/MySQL 均返回真实清单
DROP DATABASE sales;                          -- 仅空库可删（非空报 1008）
```

- 同名表跨 schema 隔离（表标识 = 全限定 `schema.table`，Catalog/WAL/对象路径统一）；
- 对象路径按 schema 分层：`yuntun/<schema>/<table>/dt=.../shard=.../<batch>.<ext>`；
- `USE` 不存在的库 / 未知库建表 → MySQL 1049；非空库删除 → 1008；重复建库 → 1007；
- 崩溃重启：schema 事件与表定义均由 WAL DDL 重放恢复；
- 旧数据（v1 单 schema）自动归属默认库 `public`。

### 幂等键（写入去重通道）

`require` 表（`CREATE TABLE` 默认模板）必须携带幂等键，右侧三种通道任选：

| 通道 | 用法 |
|---|---|
| SQL 注释（任意协议） | `INSERT /* idempotency_key=<k> */ INTO t VALUES (...)`（也支持 `-- idempotency_key=<k>` 行注释） |
| `yuntun-cli` | `yuntun-cli insert -t t -f data.csv --key <k>`（不传则客户端自动生成） |
| Flight `DoPut` | `FlightData.app_metadata = {"idempotency_key": "<k>"}` |

未提供键时：SQL 路径按语句级生成（`dml-<uuid>`），FlightSQL prepared/装载按
`app_metadata > prepared 语句上的注释键 > flightsql-<uuid>` 优先级取值。

**去重语义（粒度 = 一次请求）**：

- 键标识**一次客户端请求**（一个 DoPut 流 / 一条语句）。一次请求可能含多个批次，
  请求内第 `i` 个批次用派生键 `键#i` —— 因此重试是**逐批**幂等的：
  上次只成功了一部分时，重试**只补缺失的批次**（既不重复也不丢）。
- 被判重的批次回执为 `duplicate = true`、`row_count = 0`，`yuntun-cli insert` 会提示
  "N 个批次被幂等去重"，且 `inserted` 报的是**实际写入行数**而非输入行数 ——
  重试拿到这种回执**应视为成功**，不要再重试。
- `require` 模板的表未携带键 → 直接拒绝（不是静默降级）。

---

## 5. 冒烟脚本

Python 依赖统一用项目内虚拟环境（已 gitignore）：

```bash
python3 -m venv scripts/.venv
scripts/.venv/bin/pip install -r scripts/requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
```

| 脚本 | 用途 |
|---|---|
| `scripts/pyarrow_smoke.py <flight地址>` | Flight SQL：ADBC 查询 + 元数据 + 原始 Flight prepared 写入（**需 demo 种子实例**，见 §4 时间精度条目） |
| `scripts/pyarrow_sqlinfo_smoke.py <flight地址>` | Flight SQL：`GetSqlInfo` 信息项 |
| `scripts/pymysql_smoke.py <mysql地址>` | MySQL wire：T1 文本协议（DDL/INSERT/SELECT/SHOW/错误码 1146）+ T2 预编译 |
| `scripts/flight_stream_smoke.py <flight地址> <表> [批数 行数]` | Flight `do_get` 流式（S1.10）：灌数 + ADBC 流式读取 + 服务端 RSS 采样（`0 0` = 只读模式） |
| `scripts/dbeaver_jdbc_probe.java` | DBeaver / JDBC：T3 元数据与预览（需 `javac` + Connector/J） |

---

## 6. 仓库结构

```
crates/
  model proto wal store format catalog ingest query compaction
  sql        # SQL 语义唯一实现（分流 / 参数绑定 / 方言 shim / 元数据 API）
  sqlwire    # 协议适配层：MySQL wire（opensrv-mysql）+ Arrow→wire 编码
  server     # 节点层：协议端口 + 装配（Lakehouse）
  client     # Rust SDK + CLI（bin: yuntun-cli）
  standalone # bin: yuntun（全组件参考装配）
  chaos      # 故障注入工具
docs/        # 计划 / 架构 / 详细设计 / SQL 访问设计 / 操作日志
scripts/     # 独立客户端冒烟脚本
```

---

## 7. 开发

```bash
cargo test --workspace              # 全量回归（默认落在 tmpfs，热态 ~16s）
cargo clippy --workspace --all-targets
```

### 测试临时目录策略（`crates/testkit`）

写盘类测试（WAL / Parquet / local ObjectStore）都是"写文件 + 读回"形态，默认落在
**tmpfs（内存盘）**：Arch/systemd 下 `/tmp` 即 tmpfs，`fsync` 近似 no-op。
本机基准（500 个 64KB 文件 + `fsync`）：**tmpfs 38ms vs 真实磁盘 2422ms（≈64×）**。

需要**真实落盘语义**（fsync 等待、磁盘水位、崩溃注入、大文件压测）的用例显式使用
`TestDir::disk`——`chaos`、`chaos/examples/bench`、`server/examples/demo` 已如此。

| 环境变量 | 作用 | 默认 |
|---|---|---|
| `YUNTUN_TEST_TMPDIR` | 覆盖内存盘根 | `$TMPDIR` → `/tmp` → `/dev/shm`（取首个 tmpfs） |
| `YUNTUN_TEST_DISKDIR` | 覆盖真实磁盘根 | `<workspace>/target/test-disk` |

```rust
let dir = yuntun_testkit::TestDir::tmpfs("wal-e2e");   // 自动创建 + Drop 清理
let wal_dir = dir.string();                            // 直接塞进 TOML 配置
```

文档与阶段进度：`docs/operation-log.md`（最新进展在文末章节）。
