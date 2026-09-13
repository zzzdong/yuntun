"""MySQL wire 协议独立客户端冒烟（sql-access-design.md §七 Q5/Q6，W-5）。

T1 **文本协议**（pymysql 纯 Python，COM_QUERY）：
    版本探测 → DDL → INSERT → SELECT → SHOW TABLES/COLUMNS → 错误路径
    （表不存在 → ER_NO_SUCH_TABLE / 1146，且连接不断开）。

T2 **预编译**（mysql-connector-python prepared cursor，COM_STMT_PREPARE/EXECUTE）：
    参数绑定写入（含 datetime 二进制参数解码）→ 文本轨复核可见。

已知限制：opensrv-mysql 0.7 **没有二进制结果集编码**，COM_STMT_EXECUTE 的结果行
以文本行回写 → prepared SELECT 在二进制协议客户端侧拿不到正确行（本脚本 [10]
仅验证不崩，数据正确性由 [11] 文本轨复核）。写路径（OK 包）不受影响。

依赖（项目内虚拟环境 `scripts/.venv`，已 gitignore）：
    python3 -m venv scripts/.venv
    scripts/.venv/bin/pip install -r scripts/requirements.txt \
        -i https://mirrors.aliyun.com/pypi/simple/

用法：
    cargo run -p yuntun-standalone -- --config /tmp/yuntun-smoke/yuntun.toml &
    scripts/.venv/bin/python scripts/pymysql_smoke.py 127.0.0.1:33060
"""

import sys
import time
from datetime import datetime

import pymysql
from pymysql.err import MySQLError, ProgrammingError

TABLE = "mysql_smoke"
# 写入 → 可见：攒批 flush（ingest.time_threshold_secs）+ 查询缓存 TTL（query.cache_ttl_secs）。
# 固定 sleep 与攒批/commit 存在竞态 → 统一轮询等待（上限 15s）。
VISIBLE_TIMEOUT_SEC = 15


def wait_visible(cur, sql: str, check) -> None:
    """轮询直到 `check(rows)` 通过或超时（写后可见性是异步窗口，固定 sleep 不可靠）。"""
    deadline = time.monotonic() + VISIBLE_TIMEOUT_SEC
    while True:
        cur.execute(sql)
        rows = cur.fetchall()
        if check(rows):
            return rows
        if time.monotonic() >= deadline:
            raise AssertionError(f"visibility timeout after {VISIBLE_TIMEOUT_SEC}s: {sql} -> {rows}")
        time.sleep(0.3)

DDL = f"""
CREATE TABLE {TABLE} (
  id BIGINT,
  evt TIMESTAMP,
  name TEXT,
  cost_ms INT
)
"""


def t1_text_protocol(host: str, port: int) -> None:
    conn = pymysql.connect(
        host=host,
        port=port,
        user="yuntun",
        password="",
        database="public",
        connect_timeout=5,
        autocommit=True,
    )
    with conn, conn.cursor() as cur:
        # [1] 握手探测：canned 系统变量
        cur.execute("SHOW VARIABLES LIKE 'version'")
        rows = cur.fetchall()
        print("[1] SHOW VARIABLES LIKE 'version' ->", rows)
        assert any(r[0] == "version" and "8.0.32" in r[1] for r in rows), rows

        # [2] DDL（文本协议）
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        cur.execute(DDL)
        print(f"[2] CREATE TABLE {TABLE} ok")

        # [3] INSERT（文本协议）
        cur.execute(
            f"INSERT INTO {TABLE} (id, evt, name, cost_ms) VALUES "
            f"(1, TIMESTAMP '2026-01-01 00:00:00', 'alice', 10)"
        )
        print("[3] INSERT (text protocol) ok, affected =", cur.rowcount)

        # [4]/[5] SELECT（文本协议行编码）—— 轮询等写后可见
        rows = [tuple(r) for r in wait_visible(
            cur, f"SELECT id, name, cost_ms FROM {TABLE} ORDER BY id",
            lambda rs: [tuple(r) for r in rs] == [(1, "alice", 10)],
        )]
        print("[5] SELECT ->", rows)

        # [6] 元数据
        cur.execute("SHOW TABLES")
        names = [r[0] for r in cur.fetchall()]
        print("[6] SHOW TABLES ->", names)
        assert TABLE in names, names

        cur.execute(f"SHOW COLUMNS FROM {TABLE}")
        cols = [tuple(c) for c in cur.fetchall()]
        print("[7] SHOW COLUMNS ->", cols)
        assert [c[0] for c in cols] == ["id", "evt", "name", "cost_ms"], cols

        # [8] 错误路径：ERR 包（连接保持）
        try:
            cur.execute("SELECT * FROM definitely_no_such_table")
            raise AssertionError("expected ER_NO_SUCH_TABLE")
        except ProgrammingError as e:
            print("[8] error path ->", e.args)
            assert e.args[0] == 1146, e.args  # ER_NO_SUCH_TABLE

        # [8.1] 连接仍可用（ERR 包未断开）
        cur.execute("SELECT 1")
        print("[8.1] connection still usable after error ->", cur.fetchall())

    print("\nT1 (pymysql text protocol): OK")


def t1b_multi_schema(host: str, port: int) -> None:
    """多 schema（MySQL 的 database）：建库 / USE 切换 / 跨库隔离 / 限定名查询 / 清理。"""
    schema = "smoke_sales"
    conn = pymysql.connect(host=host, port=port, user="yuntun", connect_timeout=5, autocommit=True)
    with conn, conn.cursor() as cur:
        # 幂等清理：上一轮失败可能残留非空库（DROP DATABASE 对非空库报 1008）
        cur.execute(f"DROP TABLE IF EXISTS {schema}.orders")
        cur.execute(f"DROP DATABASE IF EXISTS {schema}")
        cur.execute(f"CREATE DATABASE {schema}")
        # USE 切换（handshake/USE → 服务端校验存在后真实切换会话 schema）
        cur.execute(f"USE {schema}")
        cur.execute("SELECT DATABASE()")
        assert cur.fetchone()[0] == schema, "USE 后 DATABASE() 应反映当前库"

        cur.execute("CREATE TABLE orders (ts BIGINT, amt BIGINT)")
        cur.execute("INSERT INTO orders VALUES (1, 100)")
        cur.execute("SHOW TABLES")
        assert [r[0] for r in cur.fetchall()] == ["orders"], "本库只有自己的表"

        # 跨库隔离：public 下没有 orders
        cur.execute("USE public")
        cur.execute("SHOW TABLES")
        assert "orders" not in [r[0] for r in cur.fetchall()], "public 不应看到 smoke_sales.orders"

        # 限定名跨库查询（无需 USE）—— 轮询等写后可见
        row = wait_visible(
            cur, f"SELECT count(*), sum(amt) FROM {schema}.orders",
            lambda rs: tuple(rs[0]) == (1, 100),
        )[0]
        assert (row[0], row[1]) == (1, 100), row
        print(f"[8.2] multi-schema: USE/{schema} + 隔离 + 限定名查询 ok")

        # 未知库：USE 报 ER_BAD_DB_ERROR = 1049（OperationalError）
        try:
            cur.execute("USE no_such_db")
            raise AssertionError("expected ER_BAD_DB_ERROR")
        except MySQLError as e:
            assert e.args[0] == 1049, e.args

        # 非空库不可删（ER_DB_DROP_EXISTS = 1008）
        try:
            cur.execute(f"DROP DATABASE {schema}")
            raise AssertionError("非空库 DROP 应失败")
        except MySQLError as e:
            assert e.args[0] == 1008, e.args

        # 清理：删表 → 删库 → 列表不含该库
        cur.execute(f"USE {schema}")
        cur.execute("DROP TABLE orders")
        cur.execute(f"DROP DATABASE {schema}")
        cur.execute("SHOW DATABASES")
        assert schema not in [r[0] for r in cur.fetchall()], "DROP 后不应再出现"
        print("[8.3] multi-schema: 非空库拒绝删除 / DROP DATABASE 清理 ok")


def t2_prepared(host: str, port: int) -> None:
    try:
        import mysql.connector
    except ImportError:  # pragma: no cover
        print("\nT2 skipped: mysql-connector-python not installed")
        return

    cnx = mysql.connector.connect(
        host=host, port=port, user="yuntun", password="", database="public"
    )
    try:
        cur = cnx.cursor(prepared=True)
        insert = (
            f"INSERT INTO {TABLE} (id, evt, name, cost_ms) VALUES (%s, %s, %s, %s)"
        )
        cur.execute(insert, (2, datetime(2026, 1, 2, 3, 4, 5), "bob", 20))
        cur.execute(insert, (3, datetime(2026, 1, 3, 0, 0, 0), "carol", 30))
        print("[9] prepared INSERT x2 ok (params bind via COM_STMT_EXECUTE)")
        cnx.commit()  # 服务端单语句自动提交；COMMIT 由方言 shim no-op

        # 已知限制（opensrv-mysql 0.7 无二进制结果集编码）：prepared 结果行以文本行
        # 回写，二进制协议客户端侧为空/错行 → 此处只验证不崩；数据正确性由 [11] 复核
        cur.execute(f"SELECT id, name FROM {TABLE} WHERE id = %s", (2,))
        print("[10] prepared SELECT ->", cur.fetchall(), "(text-only resultset: known gap)")
    finally:
        cnx.close()

    # [11] 文本轨复核 prepared 写入可见 —— 轮询等写后可见
    with pymysql.connect(
        host=host, port=port, user="yuntun", password="", database="public"
    ) as conn, conn.cursor() as cur:
        rows = wait_visible(
            cur, f"SELECT id, name FROM {TABLE} ORDER BY id",
            lambda rs: [r[0] for r in rs] == [1, 2, 3],
        )
        print("[11] SELECT after prepared inserts ->", rows)

    print("\nT2 (prepared statement): OK")


def main() -> None:
    addr = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:3306"
    host, _, port = addr.rpartition(":")
    port = int(port or "3306")
    t1_text_protocol(host, port)
    t1b_multi_schema(host, port)
    t2_prepared(host, port)
    print("\nMySQL wire independent-client smoke: ALL OK")


if __name__ == "__main__":
    main()
