"""Flight `do_get` 流式验证（S1.10 验收：大结果集内存平稳）。

步骤：
1. 通过 Flight DoPut（简易轨，path=[table, shard]）灌入 N 批 × M 行；
2. 通过 ADBC（标准 Flight SQL 客户端）流式拉取全量结果集，统计行数；
3. 全程采样 yuntun 服务端进程 RSS 峰值，与"结果集理论大小"对比——
   流式生效时峰值应远小于全量缓冲（eager 需一次性持有整个结果集）。

用法：
    cargo run -p yuntun-standalone -- --config /tmp/yuntun-dbeaver/yuntun.toml &
    scripts/.venv/bin/python scripts/flight_stream_smoke.py 127.0.0.1:50078 big
"""

import json
import subprocess
import sys
import threading
import time

import pyarrow as pa
import pyarrow.flight as fl
from adbc_driver_flightsql import dbapi

ADDR = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:50078"
TABLE = sys.argv[2] if len(sys.argv) > 2 else "big"
BATCHES = int(sys.argv[3]) if len(sys.argv) > 3 else 10
ROWS_PER_BATCH = int(sys.argv[4]) if len(sys.argv) > 4 else 100_000

SCHEMA = pa.schema(
    [("id", pa.int64()), ("v", pa.float64()), ("s", pa.string())]
)


def server_pid() -> int | None:
    try:
        out = subprocess.check_output(
            ["pgrep", "-f", "target/debug/yuntun --config"], text=True
        )
        return int(out.split()[0])
    except Exception:  # noqa: BLE001
        return None


def rss_mb(pid: int) -> float:
    try:
        with open(f"/proc/{pid}/status", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        pass
    return 0.0


class RssSampler:
    """后台线程采样服务端 RSS，返回峰值。"""

    def __init__(self, pid: int):
        self.pid = pid
        self.peak = 0.0
        self.base = rss_mb(pid)
        self._stop = False
        self._t = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self._stop:
            self.peak = max(self.peak, rss_mb(self.pid))
            time.sleep(0.1)

    def __enter__(self):
        self._t.start()
        return self

    def __exit__(self, *exc):
        self._stop = True
        self._t.join(timeout=1)
        return False


def write_batches(addr: str, table: str) -> int:
    client = fl.FlightClient(f"grpc+tcp://{addr}")
    descriptor = fl.FlightDescriptor.for_path(table, "default")
    total = 0
    t0 = time.time()
    for b in range(BATCHES):
        base = b * ROWS_PER_BATCH
        ids = pa.array(range(base, base + ROWS_PER_BATCH), pa.int64())
        vals = pa.array([i * 0.5 for i in range(base, base + ROWS_PER_BATCH)], pa.float64())
        strs = pa.array([f"s{i % 1000}" for i in range(base, base + ROWS_PER_BATCH)])
        batch = pa.Table.from_arrays([ids, vals, strs], schema=SCHEMA)
        writer, _ = client.do_put(descriptor, SCHEMA)
        # 表为 require 幂等键：app_metadata 携带（server 简易轨约定）
        # 注意：write_with_metadata 接受 RecordBatch（非 Table）
        writer.write_with_metadata(
            batch.to_batches()[0],
            json.dumps({"idempotency_key": f"stream-smoke-{b}"}).encode(),
        )
        writer.close()
        total += batch.num_rows
        print(f"  put batch {b + 1}/{BATCHES} rows={batch.num_rows} total={total}")
    print(f"写入完成：{total} 行，耗时 {time.time() - t0:.1f}s")
    return total


def visible_count(addr: str, table: str) -> int:
    with dbapi.connect(f"grpc+tcp://{addr}") as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) AS c FROM {table}")
        return cur.fetchall()[0][0]


def wait_visible(addr: str, table: str, expect: int, timeout_s: int = 90) -> int:
    """等新写入全部可见（攒批 flush + 查询缓存刷新；大写入需要更久）。"""
    deadline = time.time() + timeout_s
    visible = 0
    while time.time() < deadline:
        visible = visible_count(addr, table)
        if visible >= expect:
            return visible
        time.sleep(1)
    print(f"警告：等待可见超时（{visible}/{expect}）")
    return visible


def stream_read(addr: str, table: str) -> int:
    """ADBC 流式拉取全量：`fetch_record_batch` 逐批消费（不整表装载）。"""
    rows = 0
    t0 = time.time()
    with dbapi.connect(f"grpc+tcp://{addr}") as conn, conn.cursor() as cur:
        cur.execute(f"SELECT id, v, s FROM {table}")
        reader = cur.fetch_record_batch()
        while True:
            try:
                batch = reader.read_next_batch()
            except StopIteration:
                break
            rows += batch.num_rows
    print(f"流式读取：{rows} 行，耗时 {time.time() - t0:.1f}s")
    return rows


def main() -> int:
    pid = server_pid()
    if pid is None:
        print("未找到运行中的 yuntun 服务端（pgrep 失败）")
        return 1

    print(f"服务端 pid={pid}，基线 RSS={rss_mb(pid):.1f} MB")
    with RssSampler(pid) as sampler:
        written = write_batches(ADDR, TABLE) if BATCHES > 0 else None
        if written is not None:
            visible = wait_visible(ADDR, TABLE, written)
            print("可见行数:", visible)
        else:  # BATCHES=0：只读模式（复用已有数据）
            written = visible_count(ADDR, TABLE)
            print("可见行数:", written)
        read = stream_read(ADDR, TABLE)

    # 列数据理论下界（int64 + float64 + 短字符串指针，不含字符串体与 Arrow 开销）
    theory_mb = written * (8 + 8 + 8) / 1024 / 1024
    print(
        f"\n结果集规模 ≈ {written} 行（列数据理论下界 ≈ {theory_mb:.0f} MB）\n"
        f"服务端 RSS：基线 {sampler.base:.1f} MB → 峰值 {sampler.peak:.1f} MB "
        f"（增量 {sampler.peak - sampler.base:.1f} MB）"
    )
    assert read >= written, f"读取行数 {read} < 已可见 {written}"
    print("Flight do_get 流式冒烟: OK（增量 RSS 应远小于结果集全量缓冲量级）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
