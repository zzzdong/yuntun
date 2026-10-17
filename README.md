# yuntun

单机优先的**直写数据湖**（时序 / 可观测场景）：`WAL 权威 → 攒批 → 对象存储`，
对外暴露 **Arrow Flight SQL（gRPC）** 与 **MySQL wire（:3306）** 两个 SQL 协议端口，
两者共用同一套 SQL 语义实现（`crates/sql`）。

> **项目文档**：[`docs/OVERVIEW.md`](docs/OVERVIEW.md)（现状唯一入口）、
> [`docs/RELEASE.md`](docs/RELEASE.md)（发布与验收）、[`docs/README.md`](docs/README.md)（索引与冲突裁决）。
> **开发文档**（[`docs/dev/`](docs/dev/)）：[架构](docs/dev/ARCHITECTURE.md) ·
> [任务与台账](docs/dev/ROADMAP.md) · [交接](docs/dev/HANDOVER.md) ·
> [实施日志](docs/dev/operation-log.md)（§169 起）。
> **历史**（[`docs/archive/`](docs/archive/)）：§1…168 的操作日志、ADR 全文、旧设计 —— 是历史，不是现状。

---

## 1. 快速开始

### 下载（0.1 起）

每个 tag 由 GitHub Actions 产出 4 个包（流程见 [`docs/RELEASE.md`](docs/RELEASE.md)）：

| 平台 | 包名 |
|---|---|
| Linux x86_64（glibc） | `yuntun-<ver>-x86_64-unknown-linux-gnu.tar.gz` |
| Linux x86_64（musl，静态） | `yuntun-<ver>-x86_64-unknown-linux-musl.tar.gz` |
| Linux aarch64（glibc） | `yuntun-<ver>-aarch64-unknown-linux-gnu.tar.gz` |
| Linux aarch64（musl，静态） | `yuntun-<ver>-aarch64-unknown-linux-musl.tar.gz` |

解包即用：`bin/yuntun`、`bin/yuntun-cli`、`bin/yuntun-datanode`、`bin/metanode`；
解包后先 `sha256sum -c *.sha256`（发布页另附合并的 `SHA256SUMS`）。

### 从源码构建与启动

```bash
cargo build --release                                   # 或 cargo build（debug）
./target/release/yuntun --config yuntun.toml            # bin 名 = yuntun
./target/release/yuntun --version
```

发布门禁与打包见 [`docs/RELEASE.md`](docs/RELEASE.md)（`scripts/release_check.sh`
一条命令跑完门禁；`scripts/package_release.sh` 产出带校验和的压缩包）。

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
    # 写入先落 WAL；攒批线程在一个扫描周期内把它放进 store 层"内存分片" → 立即可查（读己之写，默认 ≤100 ms）
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

SQL 支持面（详见 `docs/OVERVIEW.md` §4；多协议接入设计在 `docs/archive/sql-access-design.md`）：

| 类别 | 支持 |
|---|---|
| 查询 | `SELECT`（含 CTE / 聚合 / `information_schema.*`）、`EXPLAIN [ANALYZE] <查询>`（DataFusion 原生计划：`logical_plan` / `physical_plan` 两列），非限定表名 `FROM t` 直接可用 |
| 写入 | `INSERT ... VALUES` / `INSERT ... SELECT` / `TRUNCATE TABLE`（= 无 `WHERE` 全表删：文件级下线，表定义保留，`§184`）/ **行级 `UPDATE ... SET ... WHERE ...` / `DELETE FROM ... WHERE ...`**（删除向量 + 原子可见；无 `WHERE` 的 UPDATE 明确拒绝） |
| DDL | `CREATE TABLE`（`IF NOT EXISTS` / `NOT NULL` / `CREATE TABLE new LIKE old`（拷 schema 不拷数据，`§184`）；类型：`TINYINT/SMALLINT/INT/BIGINT`（含无符号）、`FLOAT/DOUBLE`、`BOOLEAN`、`DATE`、`DATETIME/TIMESTAMP`、`DECIMAL(p,s)`、`CHAR/VARCHAR(n)/TEXT/STRING`、`JSON/JSONB`（→ UTF8 文本存储）、`BINARY/VARBINARY/BLOB`（`x'DEADBEEF'` hex 字面量落**原始字节**；MySQL wire 轨读回按 **HEX 文本**回传 —— opensrv 0.7.0 把结果集 charset 写死成 utf8，原样发二进制会搞崩客户端，`§186`；Flight 轨是 Arrow 原生二进制。`length()`/`octet_length()` 只收字符串 ⇒ 二进制字节长度用 `length(encode(b,'hex'))/2`）、`ARRAY<T>`、`MAP(K,V)`（嵌套元素递归支持，Parquet round-trip 无损；`T[]` 方括号形态仅 Flight/Generic 轨可解析，MySQL 轨请写 `ARRAY<T>`）；**表选项 `WITH (partition_by = 'ts')`** = 声明事件时间/分区键（`§189`）/ `DROP TABLE`（`IF EXISTS`）/ `CREATE DATABASE` / `DROP DATABASE`（均 WAL 权威，重启可恢复；非空库删除报 1008）；**`ALTER TABLE ADD/DROP COLUMN`**（`ADD [COLUMN] IF NOT EXISTS` / `DROP [COLUMN] IF EXISTS` 均可（前者是 MariaDB/PG 语法，MySQL 轨经**方言回退**接受，`§189`）；OCC 演进、走 raft，旧文件缺列按 null 对齐 —— 改类型 / 可空性 / 重命名**明确拒绝**） |
| 元数据 | `SHOW TABLES/FULL TABLES`、`SHOW COLUMNS/FULL COLUMNS`、`DESCRIBE`、`SHOW CREATE TABLE`、`SHOW DATABASES`、`SHOW VARIABLES [LIKE]`、`SHOW COLLATION/CHARSET/ENGINES/KEYS`、`SHOW [FULL] PROCESSLIST`、`SHOW [GLOBAL\|SESSION] STATUS [LIKE]` |
| 方言兼容 | `SET` / `USE` / `BEGIN` / `COMMIT` / `ROLLBACK` = no-op（单语句自动提交） |
| 不支持 | **无 `WHERE` 的 `UPDATE`**（全表重写未支持；`DELETE` 无 `WHERE` 走文件级下线）/ `ALTER TABLE` 的**改类型·可空性·重命名** / `CREATE TABLE AS SELECT`（用 `CREATE TABLE` + `INSERT ... SELECT`）/ `CREATE OR REPLACE` / 复杂类型（`STRUCT` 等）/ `_BINARY` 字面量 / MySQL JSON 路径操作符 `->` `->>`（用 JSON 函数替代）/ `SHOW STATUS WHERE`（只解释 `LIKE`）/ 视图 / 存储过程 / 事务语义 → 明确报错（绝不静默返回错误结果）；加列除 SQL `ALTER TABLE ADD COLUMN` 外，也走写入侧 schema 演化（ingest / Flight DoPut） |
| JSON 函数 | `json_get / json_get_{str,int,float,bool,json,array}` / `json_as_text` / `json_contains` / `json_length` / `json_object_keys` / `json_from_scalar`（`datafusion-functions-json` 0.55）。**路径是分步参数、不带 `$`**：`json_get_int(doc, 'a', 'b')` 取嵌套键；`json_get_int(doc, 'a.b')` 是**单个键** `"a.b"`（miss ⇒ NULL，不是嵌套遍历，`§184` 钉住这个语义）；数组下标直接给数字：`json_get_int(arr, 1)`；`json_get` 返回 JSON 变体联合（wire 按文本回传） |

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
| **T3** | DBeaver 连接 / 表·列浏览 / 数据预览 | ✅（`scripts/dbeaver_jdbc_probe.java` + 手工清单，见 `docs/archive/operation-log.md` §15） |
| **T4**（非目标） | 事务、权限、视图、存储过程、PG wire | ❌ 明确报错或 no-op |

---

## 4. 已知限制（MVP）

- **无事务**：单语句自动提交，`BEGIN/COMMIT/ROLLBACK` 为 no-op；
- **鉴权**：默认 trust（不校验）；配 `users` 自动切 `native_password`，Flight 可配 `auth_token`。**无 TLS**、无 `GRANT` 权限矩阵（`§190`）；
- **查询管理**（`§191`/`§192`）：`SHOW [FULL] PROCESSLIST`、`KILL [CONNECTION|QUERY]`、`SHOW [GLOBAL|SESSION] STATUS [LIKE]` 可用；连接级闸门 `[sql.mysql].max_connections`（默认 151）。
  未做：**按查询限流**（只有连接级）、`SHOW STATUS WHERE`、超限时的 MySQL ERR 1040（opensrv 握手前没有写包接缝，见 `§192.4`）；
- **写后可查（读己之写）**：`INSERT` 成功即落 WAL；攒批线程在一个扫描周期（`[ingest].scan_interval_ms`，
  默认 100ms）内把该批数据发布到 store 层的**内存分片**，查询立即可见 —— **不再受 flush 相位分散（默认 ≤30s）
  与查询缓存 TTL（默认 30s）影响**（`docs/dev/ARCHITECTURE.md` §3）；
  数据落对象存储（vortex（默认）/ Parquet + Manifest）仍按攒批窗口进行，落盘后进入快照隔离 / Compaction 语义；
  回执的 `expected_visible_in_secs` = 该扫描周期上界；
- **服务端预编译（prepared）**：已可用（写入 + prepared SELECT 二进制结果集，见 T2）。
  历史故障根因是 opensrv-mysql **0.7.0** 的 `PacketReader::next_async` 释放后使用
  （上游 #66/#67，仅修在 git、未随 crates.io 发布）→ 已由 sqlwire 的
  `PacketFramedReader`（按包边界分帧）在协议层规避；上游发新版后可移除该适配器。
  握手版本串与 `SHOW VARIABLES` 同源（`8.0.32-yuntun`）；
- **冷读解码预算**（`§193`）：vortex 是整文件解码、内存**不进** DataFusion 内存池 ⇒ `[query].cold_read_budget_mb`（默认 512 MB）按**解码字节**逐文件累计，超了整次查询失败（不降级成 partial —— 静默少行与"部分结果"不可区分）；`0` = 不限（启动 warn）。单文件峰值仍 ≈ 该文件解码后大小（流式解码未做，台账 `D-20`）；
- **结果集**：Flight 轨（`do_get`）**已流式**（S1.10：边算边发，500 万行 / 114 MB 级
  结果集服务端 RSS 增量 ≈ 2 MB）；MySQL wire 轨仍为逐行写但结果先收集（后续按需优化）；
- **SQL DDL 时间精度**：`TIMESTAMP(p)` 的精度 `p` 目前不落入 schema（一律 `Timestamp(ns)`）。
  需要毫秒精度的场景（如 `scripts/pyarrow_smoke.py` 的 prepared 绑定写入要求表的
  `event_time` 为 `Timestamp(ms)`）请用 demo 种子（Rust API）建表——该脚本因此
  **依赖 demo 种子实例**（`YUNTUN_DEMO_SERVE=1 cargo run -p yuntun-server --example demo`）；
- **opensrv-mysql 依赖**：crates.io 停在 **0.7.0**（2024-02），上游后续修复
  （UAF #66/#67、OK 包合规 #75/#78 等）仅存在于 git。0.1 以 sqlwire 的
  `PacketFramedReader`（按包边界分帧）规避 UAF；后续建议把依赖切到上游 git 固定 rev，
  届时可移除该适配器（`vendor/opensrv-mysql/` 仅为分析用拷贝，未接入构建）。

### 鉴权（U3，§190）

默认 **trust**（不校验，按网络隔离部署）。两种端口各有一套机制：

```toml
[server]
auth_token = "change-me"          # Flight：要求 authorization: Bearer <token>

[sql.mysql]
auth = "trust"                    # trust | native_password
users = [{ user = "yuntun", password = "s3cret" }]   # 非空 ⇒ 自动切 native_password
```

- **MySQL 端口**：`mysql_native_password` 握手（SHA1 双次 + scramble XOR），口令不过网；
  **每连接随机 salt**（opensrv 默认是固定串 ⇒ 重放面）。
- **Flight 端口**：请求头 `authorization: Bearer <token>`（ADBC：`db_kwargs` 里
  `adbc.flight.sql.rpc.call_header.authorization`）。
- **自有客户端**：`yuntun-cli --token <token> query '…'`（或环境变量 `YUNTUN_TOKEN`；
  SDK 用 `Client::connect_with_token`）。令牌由客户端拦截器统一加到**每个** RPC 上。
- 该拒的都会拒：错口令 / 未知用户 / 空口令 / 缺 token / 错 token。
- **边界**：无 TLS（明文传输，与 MySQL 自己同款）；只有**登录**鉴权，没有
  `CREATE USER` / `GRANT` 权限矩阵（用户表走配置）。

一键验证：`scripts/.venv/bin/python scripts/auth_smoke.py --mysql 127.0.0.1:3306 --user yuntun --password s3cret --flight 127.0.0.1:50051 --token change-me`

### 查询管理（U4，§191 / §192）

```sql
mysql> SHOW PROCESSLIST;              -- Id / User / Host / db / Command / Time / State / Info
mysql> SHOW STATUS LIKE 'Threads%';   -- 真实计数器（Threads_connected / Threads_running / ...）
mysql> KILL QUERY 12;                 -- 取消 12 号连接上正在跑的查询（客户端收到 1317）
mysql> KILL 12;                       -- = KILL CONNECTION（取消 + 断开）
mysql> KILL QUERY 999999;             -- 1094 unknown thread（打错 Id 不会静默成功）
```

- `Id` 是**连接**（跨查询稳定），空闲连接显示 `Command=Sleep`；
- 取消是**协作式**的：命中引擎的执行段（含 Flight 流式结果），已发出的批次不撤回；
- 进程级判据（真·慢查询被打断）在 `scripts/standalone_smoke.py` 的 `processlist_and_kill`。
- **连接准入**（`§192`）：`[sql.mysql] max_connections = 151`（默认）。超限连接在握手**前**被关闭，
  并计入 `SHOW STATUS` 的 `Connection_errors_max_connections`；`0` ⇒ 启动报错（不是"无限"）。
  判据：`crates/server/tests/sql_limits_e2e.rs`（第 3 条被拒、断开后配额归还）。

### 分区键（事件时间列 / 时间分片）

列存的收益来自"跳过数据"。yuntun 的窗口归属（`dt=<window>` 目录）、文件级 min/max 剪枝、
行组级 `.idx`（zone map + XOR）三处**共用同一个列集** —— 它就是**分区键**：

```sql
CREATE TABLE logs (ts TIMESTAMP, level TEXT, msg TEXT) WITH (partition_by = 'ts');
```

- 声明后：`dt=` 目录按**数据时间**对齐（不是到达时间）、文件带 `ts` 的 min/max（查询可整文件跳过）、
  并生成 `.idx`（Parquet 可再跳行组）；
- **未声明** ⇒ 回退到"列名恰为 `event_time`"；对不上就**三处全关**（不报错 —— 静默劣化，
  所以生产表请显式声明）；
- 列名写错 / 键名写错 ⇒ **建表即报错**（点名缺哪个列），不会静默建成一张剪不了的表。

> 时间刻度支持 `TIMESTAMP`（ns）/ µs / ms / s 与 `BIGINT`（毫秒）、`DATE`。

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
| `scripts/auth_smoke.py --mysql … --user … --password … [--flight … --token …]` | MySQL 口令 + Flight token 鉴权：**三态对拍**（对的过、错的拒） |
| `scripts/pymysql_smoke.py <mysql地址>` | MySQL wire：T1 文本协议（DDL/INSERT/SELECT/SHOW/错误码 1146）+ T2 预编译 |
| `scripts/standalone_smoke.py <mysql地址> <flight地址>` | **standalone 全功能冒烟**（`§184`）：MySQL wire 14 项（CRUD/聚合/元数据/DML/TRUNCATE/加列删列/无符号类型/多 schema/prepared/错误码/**查询管理**/**STATUS**/时间类型/JSON 函数）+ Flight SQL 2 项（查询 + `information_schema`）；退出码即结论 |
| `scripts/flight_stream_smoke.py <flight地址> <表> [批数 行数]` | Flight `do_get` 流式（S1.10）：灌数 + ADBC 流式读取 + 服务端 RSS 采样（`0 0` = 只读模式） |
| `scripts/dbeaver_jdbc_probe.java` | DBeaver / JDBC：T3 元数据与预览（需 `javac` + Connector/J） |
| `scripts/s3_smoke.sh` | 真实 S3（SeaweedFS / MinIO）**网络读写**：flush → HTTP PUT 可见于桶；**删 WAL** 重启后冷读 → HTTP GET。`S3_ENDPOINT` / `S3_BUCKET` 可配（见 `docs/archive/operation-log.md` §101） |
| `tests/cluster.sh` | **容器化三节点真集群**（podman-compose + `debian:trixie-slim`）：真网络命名空间 / 真断网（`podman network disconnect`）/ 接回后**收敛硬断言**；配套极小 Meta 客户端 `cargo run -p yuntun-meta --example meta_probe`（见 `docs/archive/operation-log.md` §108） |
| `scripts/s3_multinode_smoke.sh` | **多节点 + 真实 S3**：起 metanode + 两个可写数据进程（各私有 `--dir`、**同一个桶**），并发真写各自的 SQL 面 ⇒ 两边查询对拍都 `[1..6]`；再删 WAL 重启验冷读（见 §102） |

---

## 6. 仓库结构

```
crates/
  model proto wal store format catalog          # 数据模型 / 消息 / WAL / 对象存储 / 格式
  chunk      # 热数据缓冲（内存账本 / 背压 / spill / 确定性 flush）
  ingest     # 写入管线（幂等预筛 → WAL → chunk → flush → Manifest）
  query      # DataFusion 桥接 + 扇出合并 + DML 定位
  compaction # 合并 / 孤儿 GC / 按分片分租约的后台作业
  meta       # 控制平面（raft + fjall，bin: metanode）
  sql        # SQL 语义唯一实现（分流 / 参数绑定 / 方言 shim / 元数据 API）
  sqlwire    # 协议适配层：MySQL wire（opensrv-mysql）+ Arrow→wire 编码
  shardrpc   # 数据面分片 RPC（热数据定向拉取）
  server     # 节点层：协议端口 + 装配（Lakehouse）
  datanode   # 数据平面进程（bin: yuntun-datanode）
  client     # Rust SDK + CLI（bin: yuntun-cli）
  standalone # bin: yuntun（全组件参考装配）
  chaos      # 故障注入工具
  testkit    # 测试基建（tmpfs/disk 测试目录 + 文档一致性判据）
docs/        # 项目文档（OVERVIEW / RELEASE / 索引）
docs/dev/    # 开发文档（架构 / 任务 / 交接 / 操作日志）
docs/archive/  # 历史（冻结，不维护）
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

文档：现状 [`docs/OVERVIEW.md`](docs/OVERVIEW.md) ｜ 架构 [`docs/dev/ARCHITECTURE.md`](docs/dev/ARCHITECTURE.md) ｜
任务与台账 [`docs/dev/ROADMAP.md`](docs/dev/ROADMAP.md) ｜ 交接与提示词 [`docs/dev/HANDOVER.md`](docs/dev/HANDOVER.md) ｜
实施日志 [`docs/dev/operation-log.md`](docs/dev/operation-log.md)（§169 起）。
