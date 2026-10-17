#!/usr/bin/env python3
"""鉴权冒烟（U3 / §190）：MySQL 口令 + Flight token。

用法：
  scripts/.venv/bin/python scripts/auth_smoke.py \\
      --mysql 127.0.0.1:13316 --user yuntun --password s3cret \\
      --flight 127.0.0.1:50351 --token tok-123

每项都是**三态对拍**：正确凭据必须成功，错误/缺失凭据必须被拒 ——
只有"该拒的拒了 + 该过的过了"才算过（只验拒绝的话，服务没起来也会绿）。
退出码 0 = 全部通过。
"""
import argparse
import sys

import pymysql

PASS, FAIL = [], []


def check(name, fn):
    try:
        fn()
        PASS.append(name)
        print(f"  ok   {name}")
    except Exception as e:
        FAIL.append((name, e))
        print(f"  FAIL {name}: {e}")


def mysql_login(addr, user, password):
    host, port = addr.split(":")
    conn = pymysql.connect(
        host=host, port=int(port), user=user, password=password,
        database="public", connect_timeout=5,
    )
    with conn, conn.cursor() as cur:
        cur.execute("SELECT 1+1")
        assert cur.fetchone()[0] == 2


def mysql_denied(addr, user, password):
    try:
        mysql_login(addr, user, password)
    except pymysql.err.OperationalError as e:
        assert e.args[0] in (1045, 1698), f"期望 access denied，实际 {e.args}"
        return
    raise AssertionError(f"用户 {user} 用错误凭据**登录成功**了 —— 鉴权没生效")


def flight_query(addr, token):
    from adbc_driver_flightsql import dbapi

    kw = {}
    if token is not None:
        kw["adbc.flight.sql.rpc.call_header.authorization"] = f"Bearer {token}"
    with dbapi.connect(f"grpc+tcp://{addr}", db_kwargs=kw) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1+1")
        return cur.fetchone()[0]


def flight_denied(addr, token):
    try:
        flight_query(addr, token)
    except Exception as e:
        msg = str(e)
        assert "UNAUTHENTICATED" in msg or "Unauthenticated" in msg, f"期望 UNAUTHENTICATED，实际 {msg[:120]}"
        return
    raise AssertionError(f"令牌 {token!r} **通过了** —— 鉴权没生效")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mysql", help="MySQL 端口地址（不给则跳过）")
    ap.add_argument("--user", default="yuntun")
    ap.add_argument("--password", default="")
    ap.add_argument("--flight", help="Flight 端口地址（不给则跳过）")
    ap.add_argument("--token", help="Flight token")
    a = ap.parse_args()
    if not a.mysql and not a.flight:
        ap.error("至少要给 --mysql 或 --flight 之一")

    if a.mysql:
        print("== MySQL 口令 ==")
        check("right password accepted", lambda: mysql_login(a.mysql, a.user, a.password))
        check("wrong password denied", lambda: mysql_denied(a.mysql, a.user, "definitely-wrong"))
        check("unknown user denied", lambda: mysql_denied(a.mysql, "no-such-user", a.password))
        check("empty password denied", lambda: mysql_denied(a.mysql, a.user, ""))

    if a.flight:
        print("== Flight token ==")
        check("right token accepted", lambda: flight_query(a.flight, a.token))
        check("missing token denied", lambda: flight_denied(a.flight, None))
        check("wrong token denied", lambda: flight_denied(a.flight, "definitely-wrong"))

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    for name, e in FAIL:
        print(f"  FAIL {name}: {e}")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
