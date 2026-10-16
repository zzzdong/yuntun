#!/usr/bin/env python3
"""standalone 全功能冒烟：MySQL wire（pymysql）+ Flight SQL（ADBC）。

用法：scripts/.venv/bin/python scripts/standalone_smoke.py <mysql_addr> <flight_addr>
退出码 0 = 全部通过；非 0 = 有失败项（逐条打印）。

注意：
- 读己之写有文档化上界（≤1 个攒批 scan 周期，默认 100ms）⇒ 写入后的查询用
  `query_until` 轮询，不赌时序；
- pymysql 的 `with conn` 退出即 close ⇒ 不要重复 close（会报 Already closed）；
- 表名/库名带**运行唯一后缀**：上一次失败的运行不会留下"非空 smoke_db"把
  下一次的 `DROP DATABASE IF EXISTS` 打成 1008（本项目设计：非空库不删）。
"""
import sys
import time
import uuid
import datetime

import pymysql

MYSQL = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:13306"
FLIGHT = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1:50051"

# 运行唯一后缀（表名 / 库名都用它，见模块文档）
RUN = uuid.uuid4().hex[:8]
T = f"smoke_t_{RUN}"
DB = f"smoke_db_{RUN}"

PASS, FAIL = [], []


def check(name, fn):
    try:
        fn()
        PASS.append(name)
        print(f"  ok   {name}")
    except Exception as e:
        FAIL.append((name, e))
        print(f"  FAIL {name}: {e}")


def mysql_conn():
    host, port = MYSQL.split(":")
    return pymysql.connect(host=host, port=int(port), user="yuntun", database="public")


def query_until(cur, sql, want, timeout=5.0):
    """轮询到结果等于 want（读己之写的可见性上界内）。"""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        cur.execute(sql)
        last = cur.fetchall()
        if last == want:
            return last
        time.sleep(0.1)
    raise AssertionError(f"{timeout}s 内没等到期望结果\n  sql: {sql}\n  want: {want}\n  got:  {last}")


# ---------------- MySQL wire 轨 ----------------

def t_ddl_crud():
    with mysql_conn() as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {T}")
        cur.execute(f"CREATE TABLE {T} (id BIGINT, name TEXT, ts TIMESTAMP, val DOUBLE)")
        cur.execute(f"INSERT INTO {T} VALUES (1, 'a', TIMESTAMP '2026-01-01 00:00:00', 1.5)")
        cur.execute(f"INSERT INTO {T} VALUES (2, 'b', TIMESTAMP '2026-01-02 00:00:00', 2.5)")
        query_until(cur, f"SELECT id, name, val FROM {T} ORDER BY id",
                    ((1, "a", 1.5), (2, "b", 2.5)))


def t_aggregate():
    with mysql_conn() as conn, conn.cursor() as cur:
        query_until(cur, f"SELECT count(*) AS c, sum(val) AS s, min(val) AS mn, max(val) AS mx FROM {T}",
                    ((2, 4.0, 1.5, 2.5),))


def t_show_meta():
    with mysql_conn() as conn, conn.cursor() as cur:
        cur.execute("SHOW TABLES")
        tables = [r[0] for r in cur.fetchall()]
        assert T in tables, tables
        cur.execute(f"SHOW COLUMNS FROM {T}")
        cols = [r[0] for r in cur.fetchall()]
        assert cols == ["id", "name", "ts", "val"], cols
        cur.execute(f"DESCRIBE {T}")
        assert len(cur.fetchall()) == 4
        cur.execute(f"SHOW CREATE TABLE {T}")
        assert "CREATE TABLE" in cur.fetchone()[1]
        # D-32：限定库必须按限定库答（修好之前答成当前库的表）
        cur.execute(f"CREATE DATABASE {DB}")
        cur.execute(f"CREATE TABLE {DB}.other (k INT)")
        cur.execute(f"SHOW TABLES FROM {DB}")
        assert [r[0] for r in cur.fetchall()] == ["other"], "SHOW TABLES FROM <db> 必须只列该库的表"
        cur.execute("SHOW TABLES")
        assert T in [r[0] for r in cur.fetchall()], "无限定 → 当前库"


def t_dml_delete_update():
    with mysql_conn() as conn, conn.cursor() as cur:
        cur.execute(f"UPDATE {T} SET val = 9.5 WHERE id = 1")
        query_until(cur, f"SELECT val FROM {T} WHERE id = 1", ((9.5,),))
        cur.execute(f"DELETE FROM {T} WHERE id = 2")
        query_until(cur, f"SELECT count(*) FROM {T}", ((1,),))


def t_truncate():
    # D-35：TRUNCATE = 无 WHERE 全表删（文件级下线），表定义保留
    with mysql_conn() as conn, conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {T}_tr (v INT)")
        cur.execute(f"INSERT INTO {T}_tr VALUES (1),(2),(3)")
        query_until(cur, f"SELECT count(*) FROM {T}_tr", ((3,),))
        cur.execute(f"TRUNCATE TABLE {T}_tr")
        query_until(cur, f"SELECT count(*) FROM {T}_tr", ((0,),))
        cur.execute(f"INSERT INTO {T}_tr VALUES (7)")
        query_until(cur, f"SELECT v FROM {T}_tr", ((7,),), )
        cur.execute(f"DROP TABLE {T}_tr")


def t_schema_evolution():
    with mysql_conn() as conn, conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {T} ADD COLUMN extra TEXT")
        cur.execute(f"INSERT INTO {T} (id, name, ts, val, extra) VALUES (3, 'c', TIMESTAMP '2026-01-03 00:00:00', 3.5, 'x')")
        query_until(cur, f"SELECT extra FROM {T} WHERE id = 3", (("x",),))
        cur.execute(f"ALTER TABLE {T} DROP COLUMN extra")
        cur.execute(f"SHOW COLUMNS FROM {T}")
        cols = [r[0] for r in cur.fetchall()]
        assert "extra" not in cols, cols


def t_unsigned_types():
    # D-33：无符号整数（README §2.1 写着"含无符号"）
    with mysql_conn() as conn, conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {T}_u (a TINYINT UNSIGNED, b SMALLINT UNSIGNED, c INT UNSIGNED, d BIGINT UNSIGNED)")
        cur.execute(f"INSERT INTO {T}_u VALUES (200, 60000, 4000000000, 9000000000000000000)")
        query_until(cur, f"SELECT a, b, c, d FROM {T}_u",
                    ((200, 60000, 4000000000, 9000000000000000000),))
        cur.execute(f"DROP TABLE {T}_u")


def t_multi_schema():
    with mysql_conn() as conn, conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE {DB}_2")
        cur.execute("SHOW DATABASES")
        dbs = [r[0] for r in cur.fetchall()]
        assert f"{DB}_2" in dbs, dbs
        cur.execute(f"CREATE TABLE {DB}_2.t2 (k INT)")
        cur.execute(f"INSERT INTO {DB}_2.t2 VALUES (42)")
        query_until(cur, f"SELECT k FROM {DB}_2.t2", ((42,),))
        # 非空库不许删（设计：仅空库可删，非空报 1008）——先删表再删库
        cur.execute(f"DROP TABLE {DB}_2.t2")
        cur.execute(f"DROP DATABASE {DB}_2")
        # 前面 t_show_meta 建的 DB 也在这里收尾（唯一后缀，不留残留）。
        # 容错：万一 show_meta 半途失败没建全，清理不许把本用例也带红。
        try:
            cur.execute(f"DROP TABLE {DB}.other")
            cur.execute(f"DROP DATABASE {DB}")
        except pymysql.err.MySQLError:
            pass


def t_prepared():
    with mysql_conn() as conn, conn.cursor() as cur:
        # prepared 走二进制协议（参数绑定）
        cur.execute(f"SELECT id, name FROM {T} WHERE id = %s ORDER BY id", (1,))
        assert cur.fetchall() == ((1, "a"),)


def t_error_codes():
    with mysql_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute("SELECT * FROM no_such_table_xyz")
            raise AssertionError("查询不存在的表应当报错")
        except pymysql.err.ProgrammingError as e:
            assert e.args[0] == 1146, e.args
        try:
            cur.execute(f"UPDATE {T} SET x = 1")
            raise AssertionError("UPDATE 无 WHERE 应被拒绝")
        except pymysql.err.ProgrammingError:
            pass


def t_timestamp_types():
    with mysql_conn() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS smoke_ts")
        cur.execute("CREATE TABLE smoke_ts (t TIMESTAMP, d DATE)")
        cur.execute("INSERT INTO smoke_ts VALUES (TIMESTAMP '2026-03-04 05:06:07', DATE '2026-03-04')")
        query_until(cur, "SELECT t, d FROM smoke_ts",
                    ((datetime.datetime(2026, 3, 4, 5, 6, 7), datetime.date(2026, 3, 4)),))
        cur.execute("DROP TABLE smoke_ts")


def t_json_functions():
    # D-34：JSON 路径是**分步参数**（不是点号串）
    with mysql_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT json_get_int('{\"a\": {\"b\": 7}}', 'a', 'b')")
        assert cur.fetchone()[0] == 7
        cur.execute("SELECT json_get_int('[10,20,30]', 1)")
        assert cur.fetchone()[0] == 20


def t_binary_roundtrip():
    # §186：hex 字面量落原始字节；Binary 结果列按 HEX 文本回传（opensrv 写死
    # utf8 charset，原样发二进制会让客户端解码失败并错乱协议帧）。
    # 注意：length()/octet_length() 只收字符串（DataFusion 语义）—— 二进制列的
    # 字节长度走 length(encode(b,'hex'))/2（encode 是 DataFusion 的 hex 编码）。
    with mysql_conn() as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {T}_bin")
        cur.execute(f"CREATE TABLE {T}_bin (id INT, b BLOB)")
        cur.execute(f"INSERT INTO {T}_bin VALUES (1, x'DEADBEEF')")
        cur.execute(f"INSERT INTO {T}_bin VALUES (2, 'plain')")
        # 字节长度：hex 串长 / 2（4 字节 / 5 字节）
        query_until(cur, f"SELECT id, length(encode(b, 'hex')) / 2 FROM {T}_bin ORDER BY id",
                    ((1, 4), (2, 5)))
        # 读回 = HEX 文本（信息不丢，可 UNHEX 还原）
        query_until(cur, f"SELECT id, b FROM {T}_bin ORDER BY id",
                    ((1, "DEADBEEF"), (2, "706C61696E")))
        # Binary 结果列（sha256 家族）不许把连接搞崩
        cur.execute("SELECT sha256('x')")
        assert len(cur.fetchone()[0]) == 64
        cur.execute(f"DROP TABLE {T}_bin")


# ---------------- Flight SQL 轨 ----------------

def flight_conn():
    from adbc_driver_flightsql import dbapi
    return dbapi.connect(f"grpc+tcp://{FLIGHT}")


def t_flight_sql():
    with flight_conn() as conn, conn.cursor() as cur:
        # 此刻表里有 2 行：id=1（UPDATE 过）+ id=3（schema_evolution 加的）
        cur.execute(f"SELECT count(*) AS c FROM yuntun.public.{T}")
        row = cur.fetchone()
        assert row[0] == 2, row
        cur.execute(f"SELECT id, name FROM yuntun.public.{T} ORDER BY id")
        rows = cur.fetchall()
        assert rows == [(1, "a"), (3, "c")], rows


def t_flight_metadata():
    with flight_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.tables")
        names = [r[0] for r in cur.fetchall()]
        assert T in names, names


# ---------------- 汇总 ----------------

def main():
    print(f"run suffix: {RUN}  (mysql={MYSQL} flight={FLIGHT})")
    print("== MySQL wire ==")
    check("ddl_crud", t_ddl_crud)
    check("aggregate", t_aggregate)
    check("show_meta", t_show_meta)
    check("dml_delete_update", t_dml_delete_update)
    check("truncate", t_truncate)
    check("schema_evolution", t_schema_evolution)
    check("unsigned_types", t_unsigned_types)
    check("multi_schema", t_multi_schema)
    check("prepared", t_prepared)
    check("error_codes", t_error_codes)
    check("timestamp_types", t_timestamp_types)
    check("json_functions", t_json_functions)
    check("binary_roundtrip", t_binary_roundtrip)
    print("== Flight SQL ==")
    check("flight_sql", t_flight_sql)
    check("flight_metadata", t_flight_metadata)
    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    for name, e in FAIL:
        print(f"  FAIL {name}: {e}")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
