#!/usr/bin/env bash
# 本地 S3（SeaweedFS / MinIO）**网络读写**冒烟。
#
# 它证的是两件**必须走网络**的事（不靠进程内 memory/local 的假象）：
#   ① 写：`INSERT` → 攒批 → flush → **HTTP PUT 到 S3**（脚本直接列举桶里的对象来确认）；
#   ② 读：**删掉 WAL** + 重启（chunk store 全空）后仍查得到 ⇒ 数据只能来自 **HTTP GET**。
#
# 用法：
#   S3_ENDPOINT=http://127.0.0.1:8333 S3_BUCKET=yuntun-lake scripts/s3_smoke.sh
#   环境变量：S3_ENDPOINT / S3_BUCKET / S3_ACCESS_KEY / S3_SECRET_KEY / S3_SMOKE_DIR
#
# 前置：
#   - 一个跑着的 S3 兼容服务。本仓在 **SeaweedFS** 上验过：
#       weed server -s3 -s3.autoCreateBucket -dir=/tmp/yuntun-seaweedfs \
#            -ip=127.0.0.1 -ip.bind=127.0.0.1
#     ⚠️ **`-ip` 与 `-ip.bind` 必须同时给**：只给 `-ip.bind` 会让 volume server
#     「绑 127.0.0.1、却向 master 广播 <机器IP>」，S3 网关按广播地址传 chunk
#     → `connection refused` → PUT 返回 500（本仓实测踩过，见 `operation-log §101`）。
#   - 桶已存在（SeaweedFS 开 `-s3.autoCreateBucket`，或 `curl -X PUT $S3_ENDPOINT/<bucket>`）。
set -euo pipefail

ENDPOINT="${S3_ENDPOINT:-http://127.0.0.1:8333}"
BUCKET="${S3_BUCKET:-yuntun-lake}"
ACCESS_KEY="${S3_ACCESS_KEY:-any}"
SECRET_KEY="${S3_SECRET_KEY:-any}"
WORK="${S3_SMOKE_DIR:-/tmp/yuntun-s3-smoke}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/debug/yuntun"
CLI="$ROOT/target/debug/yuntun-cli"
TABLE="yuntun.public.s3smoke"
CONF="$WORK/yuntun.toml"
LOG="$WORK/yuntun.log"

YPID=""
cleanup() {
  # 直接按"配置路径"兜底：PID 捕获一旦不准，残留实例会占住 mariadb…
  # 更关键的是它会**占住 meta 目录的 fjall 锁**，让下一次启动响亮失败（`FjallError: Locked`）。
  if [[ -n "$YPID" ]] && kill -0 "$YPID" 2>/dev/null; then
    kill "$YPID" 2>/dev/null || true
  fi
  pkill -f "$BIN --config $CONF" 2>/dev/null || true
  local deadline=$((SECONDS + 20))
  while pgrep -f "$BIN --config $CONF" >/dev/null 2>&1; do
    (( SECONDS < deadline )) || break
    sleep 0.2
  done
  pkill -9 -f "$BIN --config $CONF" 2>/dev/null || true
  YPID=""
}
trap cleanup EXIT

say() { printf '\n=== %s ===\n' "$*"; }

# 解析 `SELECT count(*)` 的表格输出里的第一行数字（表/数据还没就绪时返回空串，不致命）
count_of() {
  { "$CLI" --addr "$1" query "SELECT count(*) AS c FROM $TABLE" 2>/dev/null \
      | grep -oE '\| *[0-9]+ *\|' | head -1 | tr -dc '0-9'; } || true
}

# 读 stdout 直到出现 `LISTEN <addr>`（与编排同一接口行，不猜端口）
wait_listen() {
  local deadline=$((SECONDS + ${1:-30}))
  while (( SECONDS < deadline )); do
    local a=""
    a="$( { sed -n 's/^LISTEN //p' "$LOG" | head -1; } || true )"
    if [[ -n "$a" ]]; then echo "$a"; return 0; fi
    sleep 0.2
  done
  echo "等待 LISTEN 超时；日志尾部：" >&2
  tail -20 "$LOG" >&2
  return 1
}

s3_keys() {
  { curl -sS -m 5 "$ENDPOINT/$BUCKET?list-type=2&prefix=yuntun/" \
      | tr '>' '>\n' | grep -oE '<Key>[^<]+' | sed 's/<Key>//'; } || true
}

start_yuntun() {
  # ⚠️ 不要写成 `( cd … && nohup … & echo $! )`：那样 `$!` 可能拿到**中途退出的子 shell**的
  # PID，cleanup 便杀不掉真进程 —— 残留实例占住 meta 的 fjall 锁，下一次启动会
  # "响亮失败"（`FjallError: Locked`）。路径全绝对，直接后台起即可。
  nohup "$BIN" --config "$CONF" > "$LOG" 2>&1 &
  YPID=$!
  echo "$YPID" > "$WORK/pid"
}

# ---------------------------------------------------------------- 前置
say "前置检查"
[[ -x "$BIN" ]] || { echo "缺 $BIN：先 cargo build -p yuntun-standalone -p yuntun-client"; exit 1; }
[[ -x "$CLI" ]] || { echo "缺 $CLI：先 cargo build -p yuntun-standalone -p yuntun-client"; exit 1; }
curl -sS -m 5 -o /dev/null -w "S3 端点 $ENDPOINT -> HTTP %{http_code}\n" "$ENDPOINT/"
curl -sS -m 5 "$ENDPOINT/" >/dev/null || { echo "S3 端点不可达"; exit 1; }
if ! curl -sS -m 5 -o /dev/null -w '%{http_code}' "$ENDPOINT/$BUCKET" | grep -qE '^2'; then
  echo "桶 $BUCKET 不存在（HEAD 非 2xx）。SeaweedFS 可用 -s3.autoCreateBucket，或 curl -X PUT $ENDPOINT/$BUCKET"
  exit 1
fi

rm -rf "$WORK"
mkdir -p "$WORK"

cat > "$CONF" <<EOF
[server]
listen = "127.0.0.1:0"
shards = 1

[store]
type = "s3"
bucket = "$BUCKET"
endpoint = "$ENDPOINT"
access_key_id = "$ACCESS_KEY"
secret_access_key = "$SECRET_KEY"
allow_http = true

[wal]
dir = "$WORK/wal"

[chunk]
spill_dir = "$WORK/spill"
instance_id = "s3-smoke"
mem_budget_mb = 128
query_mem_budget_mb = 128

[ingest]
default_format = "parquet"
# 让 seal/flush 立刻发生（不等分钟窗口），冒烟才快
rows_threshold = 1
bytes_threshold_mb = 1
time_threshold_secs = 0
max_flush_delay_secs = 0
chunk_max_resident_secs = 60
flush_phase_spread_secs = 0
scan_interval_ms = 50

[compaction]
interval_secs = 3600

[query]
cache_ttl_secs = 1

[sql.mysql]
enabled = false

[meta]
mode = "embedded"
dir = "$WORK/meta"
listen = "127.0.0.1:0"
EOF

# ---------------------------------------------------------------- ① 写：走网络 PUT
say "① 起 yuntun（store=s3）"
start_yuntun
ADDR="$(wait_listen 30)"
echo "LISTEN $ADDR"

"$CLI" --addr "$ADDR" query "CREATE TABLE $TABLE (a BIGINT)" >/dev/null
"$CLI" --addr "$ADDR" query "INSERT INTO $TABLE VALUES (1),(2),(3)" >/dev/null
echo "已 INSERT 3 行"

say "等 flush 落到 S3（最多 60s）"
# 取"**新出现**的那个对象"（桶里可能有历次运行的遗留；只报新键，证据才准）
BEFORE_KEYS="$(s3_keys | sort)"
KEY=""
for _ in $(seq 1 300); do
  KEY="$(comm -13 <(printf '%s\n' "$BEFORE_KEYS" | sed '/^$/d' | sort) <(s3_keys | sort) | head -1)"
  [[ -n "$KEY" ]] && break
  sleep 0.2
done
[[ -n "$KEY" ]] || { echo "FAIL: 60s 内 S3 桶里没新增对象（flush 没走网络 PUT？）"; tail -30 "$LOG"; exit 1; }
SIZE="$({ curl -sS -m 5 -I "$ENDPOINT/$BUCKET/$KEY" | tr -d '\r' | grep -i '^content-length:' | awk '{print $2}'; } || true)"
echo "PASS(写): S3 里出现 $KEY（${SIZE} bytes）"

# 读己之写：可见性上界 = 一个 scan 周期（本配置 50ms），给它一点余量
say "② 读己之写（热路径）"
GOT=""
for _ in $(seq 1 100); do
  GOT="$(count_of "$ADDR")"
  [[ "$GOT" == "3" ]] && break
  sleep 0.1
done
[[ "$GOT" == "3" ]] || { echo "FAIL: 热路径读到 $GOT 行（应为 3）"; exit 1; }
echo "PASS(热读): 3 行"

# ---------------------------------------------------------------- ② 读：删 WAL 后走网络 GET
say "③ 停 yuntun → 删 WAL/spill（保留 meta）→ 重启"
cleanup
rm -rf "$WORK/wal" "$WORK/spill"
start_yuntun
ADDR2="$(wait_listen 30)"
echo "LISTEN $ADDR2（chunk store 已空、WAL 已删 ⇒ 只能从 S3 冷读）"

GOT2=""
for _ in $(seq 1 100); do
  GOT2="$(count_of "$ADDR2")"
  [[ "$GOT2" == "3" ]] && break
  sleep 0.1
done
[[ "$GOT2" == "3" ]] || { echo "FAIL: 冷读读到 $GOT2 行（应为 3；数据没从 S3 读回来？）"; tail -30 "$LOG"; exit 1; }
echo "PASS(冷读): 3 行 —— 只能来自 $ENDPOINT 的网络 GET"

say "结果"
echo "写：HTTP PUT  → $KEY（${SIZE} bytes）"
echo "读：HTTP GET  → 删 WAL + 重启后仍 3 行"
echo "全部通过。实验目录：$WORK"
