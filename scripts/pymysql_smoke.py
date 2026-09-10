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
from pymysql.err import ProgrammingError

TABLE = "mysql_smoke"
# 写入 → 可见：攒批 flush（ingest.time_threshold_secs=1）+ 查询缓存 TTL（query.cache_ttl_secs=1）
VISIBLE_WAIT_SEC = 3

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

        # [4] 可见性等待
        time.sleep(VISIBLE_WAIT_SEC)

        # [5] SELECT（文本协议行编码）
        cur.execute(f"SELECT id, name, cost_ms FROM {TABLE} ORDER BY id")
        rows = [tuple(r) for r in cur.fetchall()]
        print("[5] SELECT ->", rows)
        assert rows == [(1, "alice", 10)], rows

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

    # [11] 文本轨复核 prepared 写入可见
    time.sleep(VISIBLE_WAIT_SEC)
    with pymysql.connect(
        host=host, port=port, user="yuntun", password="", database="public"
    ) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT id, name FROM {TABLE} ORDER BY id")
        rows = cur.fetchall()
        print("[11] SELECT after prepared inserts ->", rows)
        assert [r[0] for r in rows] == [1, 2, 3], rows

    print("\nT2 (prepared statement): OK")


def main() -> None:
    addr = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:3306"
    host, _, port = addr.rpartition(":")
    port = int(port or "3306")
    t1_text_protocol(host, port)
    t2_prepared(host, port)
    print("\nMySQL wire independent-client smoke: ALL OK")


if __name__ == "__main__":
    main()
