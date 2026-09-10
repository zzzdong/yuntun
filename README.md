# yuntun

单机优先的**直写数据湖**（时序 / 可观测场景）：`WAL 权威 → 攒批 → 对象存储`，
对外暴露 **Arrow Flight SQL（gRPC）** 与 **MySQL wire（:3306）** 两个 SQL 协议端口，
两者共用同一套 SQL 语义实现（`crates/sql`）。

> 详细设计见 [`docs/`](docs/)：`plan.md`（任务书 v2.0 / 阶段划分）、`architecture.md`、
> `design.md`、`sql-access-design.md`（多协议接入）、`operation-log.md`（实施日志）。

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
    # 写入先落 WAL，攒批后可见（默认 5s / 1 万行，可按 [ingest] 调小）
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
| DDL | `CREATE TABLE` / `DROP TABLE`（WAL 权威，重启可恢复） |
| 元数据 | `SHOW TABLES/FULL TABLES`、`SHOW COLUMNS/FULL COLUMNS`、`DESCRIBE`、`SHOW CREATE TABLE`、`SHOW DATABASES`、`SHOW VARIABLES`、`SHOW COLLATION/CHARSET/ENGINES/KEYS` |
| 方言兼容 | `SET` / `USE` / `BEGIN` / `COMMIT` / `ROLLBACK` = no-op（单语句自动提交） |
| 不支持 | `UPDATE` / `DELETE` / 视图 / 存储过程 / 事务语义 → 明确报错（绝不静默返回错误结果） |

### 2.2 DBeaver（MySQL）连接

1. `数据库` → `新建连接` → 选择 **MySQL** → 下一步；
2. **常规**：主机 `127.0.0.1`、端口 `3306`、数据库 `public`、用户名 `yuntun`、密码留空；
3. **驱动属性**（右键连接 → 编辑连接 → 驱动属性）：
   - `useServerPrepStmts` = **false**（yuntun 当前为文本结果集，见 §4 已知限制）
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
| **T2** | 驱动预编译（COM_STMT_PREPARE/EXECUTE）写入，含 datetime 二进制参数 | ✅（写入路径；`scripts/pymysql_smoke.py` T2） |
| **T3** | DBeaver 连接 / 表·列浏览 / 数据预览 | ✅（`scripts/dbeaver_jdbc_probe.java` + 手工清单，见 `docs/operation-log.md` §15） |
| **T4**（非目标） | 事务、权限、视图、存储过程、PG wire | ❌ 明确报错或 no-op |

---

## 4. 已知限制（MVP）

- **无事务**：单语句自动提交，`BEGIN/COMMIT/ROLLBACK` 为 no-op；
- **trust 鉴权**：`[sql.mysql].users` 非空仅告警，当前不做口令校验——请按网络隔离部署；
- **单 schema**：`yuntun.public`，`USE db` 仅记录不切换（设计 R-3）；
- **写后可见有延迟**：INSERT 成功即落 WAL，攒批窗口（默认 5s / 1 万行）后可见——
  冒烟/演示可在配置里把 `[ingest].time_threshold_secs` 与 `[query].cache_ttl_secs` 调小；
- **prepared SELECT 取不到结果行**：opensrv-mysql 0.7 只提供文本结果集编码，
  COM_STMT_EXECUTE 的结果行按文本回写 → 二进制协议客户端需关闭服务端预编译
  （JDBC `useServerPrepStmts=false`）；**写路径（OK 包）不受影响**；
- **结果集为 eager**（G7）：大结果集全量缓冲，流式化随 S1.10 清偿。

---

## 5. 冒烟脚本

Python 依赖统一用项目内虚拟环境（已 gitignore）：

```bash
python3 -m venv scripts/.venv
scripts/.venv/bin/pip install -r scripts/requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
```

| 脚本 | 用途 |
|---|---|
| `scripts/pyarrow_smoke.py <flight地址>` | Flight SQL：ADBC 查询 + 元数据 + 原始 Flight prepared 写入 |
| `scripts/pyarrow_sqlinfo_smoke.py <flight地址>` | Flight SQL：`GetSqlInfo` 信息项 |
| `scripts/pymysql_smoke.py <mysql地址>` | MySQL wire：T1 文本协议（DDL/INSERT/SELECT/SHOW/错误码 1146）+ T2 预编译 |
| `scripts/dbeaver_jdbc_probe.java` | DBeaver / JDBC：T3 元数据与预览（需 `javac` + Connector/J） |

---

## 6. 仓库结构

```
crates/
  model proto wal store format catalog ingest query compaction
  sql        # SQL 语义唯一实现（分流 / 参数绑定 / 方言 shim / 元数据 API）
  sqlwire    # 协议适配层：MySQL wire（opensrv-mysql）+ Arrow→wire 编码
  server     # 节点层：协议端口 + 装配（Lakehouse）
  standalone # bin: yuntun（全组件参考装配）
  chaos      # 故障注入工具
docs/        # 计划 / 架构 / 详细设计 / SQL 访问设计 / 操作日志
scripts/     # 独立客户端冒烟脚本
```

---

## 7. 开发

```bash
cargo test --workspace              # 全量回归
cargo clippy --workspace --all-targets
```

文档与阶段进度：`docs/operation-log.md`（最新进展在文末章节）。
