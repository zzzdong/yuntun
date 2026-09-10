"""Flight SQL 标准协议独立客户端冒烟（计划任务书 v2.0 S1.5）。

两层验证：
1. **ADBC FlightSQL 驱动**（adbc-driver-flightsql，标准独立客户端）——
   查询 + 元数据（GetObjects → GetCatalogs/GetTables）；
2. **手写 protobuf wire + pyarrow 原始 FlightClient**——prepared 批量写入
   （CreatePreparedStatement → DoPut(CommandPreparedStatementUpdate) 绑定数据）。

用法：
    YUNTUN_DEMO_SERVE=1 cargo run -p yuntun-server --example demo
    scripts/.venv/bin/python scripts/pyarrow_smoke.py 127.0.0.1:<port>
"""

import sys
import time

import pyarrow as pa
import pyarrow.flight as fl
from adbc_driver_flightsql import dbapi

SQL_Q = "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
SQL_PS_UPDATE = "type.googleapis.com/arrow.flight.protocol.sql.CommandPreparedStatementUpdate"
SQL_PS_REQ = (
    "type.googleapis.com/arrow.flight.protocol.sql.ActionCreatePreparedStatementRequest"
)
SQL_PS_RESULT = (
    "type.googleapis.com/arrow.flight.protocol.sql.ActionCreatePreparedStatementResult"
)


def varint(n: int) -> bytes:
    out = b""
    while True:
        b = n & 0x7F
        n >>= 7
        out += bytes([b | (0x80 if n else 0)])
        if not n:
            return out


def field_bytes(num: int, payload: bytes) -> bytes:
    """protobuf wire: field num, wire type 2 (length-delimited)。"""
    return varint((num << 3) | 2) + varint(len(payload)) + payload


def any_of(type_url: str, value: bytes) -> bytes:
    """prost_types Any: field1 = type_url (string), field2 = value (bytes)。"""
    return field_bytes(1, type_url.encode()) + field_bytes(2, value)


def parse_varint(data: bytes, pos: int):
    result, shift = 0, 0
    while True:
        b = data[pos]
        result |= (b & 0x7F) << shift
        pos += 1
        if not (b & 0x80):
            return result, pos
        shift += 7


def first_len_field(msg: bytes, want_tag: bytes) -> bytes:
    """从消息中取第一个匹配 tag 的 length-delimited 字段。"""
    pos = 0
    while pos < len(msg):
        tag, pos = parse_varint(msg, pos)
        ln, pos = parse_varint(msg, pos)
        if varint(tag) == want_tag:
            return msg[pos : pos + ln]
        pos += ln
    raise ValueError(f"field {want_tag} not found")


def cmd_statement_query(sql: str) -> bytes:
    return any_of(SQL_Q, field_bytes(1, sql.encode()))


def any_of_nested(msg: bytes) -> tuple:
    """解析 Any(type_url, value) → (type_url, value)。"""
    pos = 0
    url = value = None
    while pos < len(msg):
        tag, pos = parse_varint(msg, pos)
        ln, pos = parse_varint(msg, pos)
        payload = msg[pos : pos + ln]
        if tag == (1 << 3) | 2:
            url = bytes(payload).decode()
        elif tag == (2 << 3) | 2:
            value = payload
        pos += ln
    return url, value


def ticket_handle(endpoint_ticket: bytes) -> str:
    """Any(TicketStatementQuery).statement_handle → UTF-8。"""
    assert endpoint_ticket.startswith(b"\x0a"), "Any.field1(type_url) expected"
    ln, pos = parse_varint(endpoint_ticket, 1)
    type_url = endpoint_ticket[pos : pos + ln].decode()
    assert type_url.endswith("TicketStatementQuery"), f"unexpected: {type_url}"
    value = endpoint_ticket[pos + ln :]
    return first_len_field(value, b"\x0a").decode()


def main():
    addr = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:50051"

    # ================= ① ADBC 标准客户端：查询 + 元数据 =================
    with dbapi.connect(f"grpc+tcp://{addr}") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) AS c FROM yuntun.public.api_audit")
            print("[1] ADBC SELECT count(*):", cur.fetchall())

            cur.execute(
                "SELECT \"user\", count(*) AS cnt FROM yuntun.public.api_audit "
                "GROUP BY \"user\" ORDER BY cnt DESC"
            )
            print("[2] ADBC GROUP BY:", cur.fetchall())

            objs = conn.adbc_get_objects(depth="tables").read_all()
            print("[3] ADBC get_objects catalogs:", objs.column("catalog_name").to_pylist())
            assert "yuntun" in objs.column("catalog_name").to_pylist()

    # ================= ② 原始 Flight + 手写 wire：prepared 写入 =================
    client = fl.FlightClient(f"grpc+tcp://{addr}")
    print(f"[4] raw FlightClient connected to {addr}")

    insert_sql = 'INSERT INTO api_audit (event_time, "user", endpoint, cost_ms)'
    # 【协议约定】action 请求/响应均为 Any 包装（与官方 blanket 实现一致）
    action = fl.Action(
        "CreatePreparedStatement",
        any_of(SQL_PS_REQ, field_bytes(1, insert_sql.encode())),
    )
    results = list(client.do_action(action))
    url, value = any_of_nested(results[0].body)
    assert url == SQL_PS_RESULT, f"unexpected action result type: {url}"
    ps_handle = bytes(first_len_field(value, b"\x0a")).decode()
    print(f"[5] prepared handle: {ps_handle[:24]}...")

    now = pa.timestamp("ms")
    tbl = pa.table(
        {
            "event_time": pa.array([int(time.time() * 1000)], now),
            "user": pa.array(["adbc-smoke"]),
            "endpoint": pa.array(["/smoke"]),
            "cost_ms": pa.array([42]),
        }
    )
    writer, _ = client.do_put(
        fl.FlightDescriptor.for_command(
            any_of(SQL_PS_UPDATE, field_bytes(1, ps_handle.encode()))
        ),
        tbl.schema,
    )
    writer.write_table(tbl)
    writer.close()
    print("[6] prepared bind write sent (rows=1)")

    # ================= ③ ADBC 复核写入可见 =================
    time.sleep(1.5)
    with dbapi.connect(f"grpc+tcp://{addr}") as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) AS c FROM yuntun.public.api_audit")
        rows = cur.fetchall()
        print("[7] count after prepared insert:", rows)
        assert rows[0][0] >= 6, "prepared 写入应已可见"

    print("\nFlightSQL independent-client smoke: ALL OK")


if __name__ == "__main__":
    main()
