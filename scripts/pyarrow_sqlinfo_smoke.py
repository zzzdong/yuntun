#!/usr/bin/env python3
"""S1.5 收尾冒烟：SQL 能力元数据（GetSqlInfo / GetXdbcTypeInfo）+ executeUpdate。

验证目标：Flight SQL JDBC / DBeaver 类客户端握手后必需的元数据命令可正常返回；
executeUpdate（INSERT）走 server SQL 前置分流 → ingest 管线，数据可查。

用法：
    YUNTUN_DEMO_SERVE=1 cargo run -p yuntun-server --example demo
    python scripts/pyarrow_sqlinfo_smoke.py 127.0.0.1:<port>
"""

import sys
import time

import pyarrow as pa
import pyarrow.flight as fl
from adbc_driver_flightsql import dbapi

SQL = "type.googleapis.com/arrow.flight.protocol.sql."
SQL_GET_SQL_INFO = SQL + "CommandGetSqlInfo"
SQL_GET_XDBC_INFO = SQL + "CommandGetXdbcTypeInfo"
SQL_STATEMENT_UPDATE = SQL + "CommandStatementUpdate"


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


def field_varint(num: int, value: int) -> bytes:
    """protobuf wire: field num, wire type 0 (varint)。"""
    return varint((num << 3) | 0) + varint(value)


def any_of(type_url: str, value: bytes) -> bytes:
    """prost_types Any: field1 = type_url (string), field2 = value (bytes)。"""
    return field_bytes(1, type_url.encode()) + field_bytes(2, value)


def do_get_cmd(client: fl.FlightClient, cmd: bytes):
    """get_flight_info(cmd) → endpoint ticket → do_get → RecordBatch 列表。"""
    info = client.get_flight_info(fl.FlightDescriptor.for_command(cmd))
    assert len(info.endpoints) == 1
    reader = client.do_get(info.endpoints[0].ticket)
    return reader.read_all()


def parse_update_result(meta: bytes) -> int:
    """DoPutUpdateResult.record_count（field 1, varint）。"""
    pos = 0
    while pos < len(meta):
        tag = meta[pos]
        if tag == 1 << 3:
            n, pos = 0, pos + 1
            shift = 0
            while True:
                b = meta[pos]
                n |= (b & 0x7F) << shift
                pos += 1
                if not b & 0x80:
                    return n
                shift += 7
        raise ValueError(f"unexpected tag {tag}")


assert parse_update_result(bytes([0x08, 0x05])) == 5


def main():
    addr = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:50051"
    client = fl.FlightClient(f"grpc+tcp://{addr}")
    print(f"[0] FlightClient connected to {addr}")

    # ================= ① GetSqlInfo（JDBC 握手后首先拉取能力元数据） =================
    cmd = any_of(
        SQL_GET_SQL_INFO,
        b"".join(
            [
                field_varint(1, 0),  # SERVER_NAME
                field_varint(1, 1),  # SERVER_VERSION
                field_varint(1, 2),  # ARROW_VERSION
                field_varint(1, 3),  # READ_ONLY
                field_varint(1, 4),  # SQL
                field_varint(1, 8),  # TRANSACTION
            ]
        ),
    )
    t = do_get_cmd(client, cmd)
    assert t.num_rows == 6, f"GetSqlInfo 应返回请求的 6 项，got {t.num_rows}"
    names = t.column(0).to_pylist()
    print(f"[1] GetSqlInfo rows={t.num_rows} info_names={names}")
    assert 0 in names and 4 in names

    # ================= ② GetXdbcTypeInfo：全量 + 按类型过滤 =================
    t = do_get_cmd(client, any_of(SQL_GET_XDBC_INFO, b""))
    types = t.column(0).to_pylist()
    print(f"[2] GetXdbcTypeInfo rows={t.num_rows} types={types}")
    assert "INTEGER" in types and "BIGINT" in types and "VARCHAR" in types

    t = do_get_cmd(
        client, any_of(SQL_GET_XDBC_INFO, field_varint(1, 4))  # XDBC_INTEGER
    )
    assert t.num_rows == 1 and t.column(0).to_pylist() == ["INTEGER"]
    print("[2b] filtered XDBC_INTEGER -> ['INTEGER']")

    # ================= ③ executeUpdate：INSERT 走 SQL 前置分流 =================
    # 【协议语义】PutResult 回执由服务端在语句完成后发送；arrow-rs/JDBC 客户端
    # 阻塞式读回执（server e2e 覆盖）。pyarrow C++ 客户端 close() 后响应流即终止
    # 且并发读会阻塞 close——故此处以落库结果验证（④）。
    ts = int(time.time() * 1000)
    sql = f"INSERT INTO sql_demo (ts, name, cost) VALUES ({ts}, 'pyarrow', 42)"
    cmd = any_of(SQL_STATEMENT_UPDATE, field_bytes(1, sql.encode()))
    writer, _reader = client.do_put(
        fl.FlightDescriptor.for_command(cmd),
        pa.schema([]),
    )
    writer.close()
    print("[3] executeUpdate(INSERT) sent")

    # ================= ④ ADBC 复核：元数据 + 数据可见 =================
    # 可见性链路：WAL(即时) → 攒批 flush(~5s) → 查询缓存刷新(TTL 30s)
    time.sleep(32)
    with dbapi.connect(f"grpc+tcp://{addr}") as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT ts, name, cost FROM yuntun.public.sql_demo WHERE ts = " + str(ts)
        )
        rows = cur.fetchall()
        print(f"[4] ADBC SELECT after INSERT: {rows}")
        assert rows == [(ts, "pyarrow", 42)]

        objs = conn.adbc_get_objects(depth="tables").read_all()
        tbls = []
        for cat in objs.column("catalog_db_schemas").to_pylist():
            for sch in cat or []:
                for tb in sch["db_schema_tables"] or []:
                    tbls.append(tb["table_name"])
        print(f"[5] ADBC get_objects tables: {tbls}")
        assert "sql_demo" in tbls

    print("\nSQL metadata + executeUpdate smoke: ALL OK")


if __name__ == "__main__":
    main()
