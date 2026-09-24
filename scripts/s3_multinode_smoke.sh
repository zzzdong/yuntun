#!/usr/bin/env bash
# **多节点 + 真实 S3**：两个写进程**并发真写** ⇒ 各自查询逐行相等；且删本地 WAL 重启后
# 冷读只能来自 S3。
#
# 这是 `operation-log §98`（两个写进程并发真写对拍）的**真实对象存储形态**：
# `§98` 用的是"本机多进程共享一个 `--cold-root` 目录"（`status.md §5.2` 记为替身），
# 本脚本把冷存储换成 **S3 端点**（本机 SeaweedFS / MinIO），走真 HTTP。
#
# 用法：
#   S3_ENDPOINT=http://127.0.0.1:8333 S3_BUCKET=yuntun-lake scripts/s3_multinode_smoke.sh
#   环境变量：S3_ENDPOINT / S3_BUCKET / S3_ACCESS_KEY / S3_SECRET_KEY / S3_MT_DIR
#
# 前置：
#   - S3 服务在跑（SeaweedFS 配方见 `scripts/s3_smoke.sh` 头注释；⚠️ `-ip` 与 `-ip.bind` 必须一致），
#     且桶已存在（`-s3.autoCreateBucket` 或 `curl -X PUT $S3_ENDPOINT/<bucket>`）；
#   - 已构建：cargo build -p yuntun-datanode -p yuntun-meta -p yuntun-client
#
# 形态（metanode 真进程 + 两个"可写数据进程"）：
#
#   metanode（真进程，raft 落盘）
#     ▲ 注册/心跳                        ▲ 名录（含各自数据面地址）
#   datanode A ──┐  各 --dir 私有      ┌── datanode B
#   --sql-listen │  ┌────── S3 ──────┐ │   --sql-listen
#   真写 1,2,3 ──┴─►│ 同一个 bucket  │◄─┴─ 真写 4,5,6
#                   └────────────────┘
#    A 查 [1..6]                      B 查 [1..6]
set -euo pipefail

ENDPOINT="${S3_ENDPOINT:-http://127.0.0.1:8333}"
BUCKET="${S3_BUCKET:-yuntun-lake}"
ACCESS_KEY="${S3_ACCESS_KEY:-any}"
SECRET_KEY="${S3_SECRET_KEY:-any}"
WORK="${S3_MT_DIR:-/tmp/yuntun-s3-mt}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
META_BIN="$ROOT/target/debug/metanode"
DN_BIN="$ROOT/target/debug/yuntun-datanode"
CLI="$ROOT/target/debug/yuntun-cli"

# 每跑一次都换表名/实例名：S3 前缀与进程名都不与历史运行混淆（证据才干净）
SUF="$$"
TABLE="s3mt_$SUF"
TABLE_SQL="yuntun.public.$TABLE"
PREFIX="yuntun/public/$TABLE/"
INST_A="mt-a-$SUF"
INST_B="mt-b-$SUF"

META_PID=""
DN_PIDS=()
cleanup() {
  # 只按**记录下来的 PID** 收尾（这里都是 `&` 直接起的，`$!` 就是真进程 —— 不是
  # `§101` 那个"子 shell 截胡 `$!`"的坑）。不用 `pkill -f <宽 pattern>`：那既可能误伤
  # 同机上别人的进程，也是安全策略明确拦的写法。
  for p in "${DN_PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null || true; done
  if [[ -n "$META_PID" ]]; then kill -9 "$META_PID" 2>/dev/null || true; fi
  DN_PIDS=()
  META_PID=""
}
trap cleanup EXIT

say() { printf '\n=== %s ===\n' "$*"; }

s3_keys() {
  { curl -sS -m 5 "$ENDPOINT/$BUCKET?list-type=2&prefix=$PREFIX" \
      | tr '>' '>\n' | grep -oE '<Key>[^<]+' | sed 's/<Key>//'; } || true
}

# 从日志里等一行接口行（`LISTEN <addr>` / `SQL-LISTEN <addr>` / metanode 的 `listening on <addr>`）
wait_line() {
  local f=$1 sedexpr=$2 what=$3 deadline=$((SECONDS + 40))
  while (( SECONDS < deadline )); do
    local v=""
    v="$( { sed -n "$sedexpr" "$f" | head -1; } || true )"
    if [[ -n "$v" ]]; then echo "$v"; return 0; fi
    sleep 0.2
  done
  echo "等待「$what」超时；$f 尾部：" >&2
  tail -20 "$f" >&2
  return 1
}

# 查一行的结果（表格输出里的数字列）⇒ 逐行一个数字
rows_of() {
  { "$CLI" --addr "$1" query "SELECT a FROM $TABLE_SQL ORDER BY a" 2>/dev/null \
      | grep -oE '\| *-?[0-9]+ *\|' | tr -dc '0-9\n'; } || true
}

start_dn() { # name dir logfile
  local name=$1 dir=$2 log=$3
  "$DN_BIN" --instance-id "$name" --dir "$dir" \
    --s3-bucket "$BUCKET" --s3-endpoint "$ENDPOINT" --s3-allow-http \
    --s3-access-key "$ACCESS_KEY" --s3-secret-key "$SECRET_KEY" \
    --listen 127.0.0.1:0 --sql-listen 127.0.0.1:0 \
    --meta "$META_ADDR" --heartbeat-secs 1 --reconcile-secs 1 \
    > "$log" 2>&1 &
  local pid=$!
  DN_PIDS+=("$pid")
  echo "$pid" > "$log.pid"
}

# ---------------------------------------------------------------- 前置
say "前置检查"
for b in "$META_BIN" "$DN_BIN" "$CLI"; do
  [[ -x "$b" ]] || { echo "缺 $b —— 先 cargo build -p yuntun-datanode -p yuntun-meta -p yuntun-client"; exit 1; }
done
curl -sS -m 5 -o /dev/null -w "S3 端点 $ENDPOINT -> HTTP %{http_code}\n" "$ENDPOINT/"
if ! curl -sS -m 5 -o /dev/null -w '%{http_code}' "$ENDPOINT/$BUCKET" | grep -qE '^2'; then
  echo "桶 $BUCKET 不存在。SeaweedFS 可用 -s3.autoCreateBucket，或 curl -X PUT $ENDPOINT/$BUCKET"
  exit 1
fi

rm -rf "$WORK"
mkdir -p "$WORK"

# ---------------------------------------------------------------- ① metanode
say "① metanode（真进程 + raft 落盘）"
"$META_BIN" --id 1 --dir "$WORK/meta" --listen 127.0.0.1:0 --init \
  > "$WORK/metanode.log" 2>&1 &
META_PID=$!
META_ADDR="$(wait_line "$WORK/metanode.log" 's/.*listening on \([^ ]*\).*/\1/p' "metanode 监听地址")"
echo "metanode = $META_ADDR（pid $META_PID）"

# ---------------------------------------------------------------- ② 两个可写写进程
say "② 两个可写数据进程（私有 --dir，同一 S3 冷存储）"
start_dn "$INST_A" "$WORK/a" "$WORK/a.log"
start_dn "$INST_B" "$WORK/b" "$WORK/b.log"
DN_ADDR_A="$(wait_line "$WORK/a.log" 's/^LISTEN //p' "A 的数据面")"
DN_ADDR_B="$(wait_line "$WORK/b.log" 's/^LISTEN //p' "B 的数据面")"
SQL_A="$(wait_line "$WORK/a.log" 's/^SQL-LISTEN //p' "A 的 SQL 面")"
SQL_B="$(wait_line "$WORK/b.log" 's/^SQL-LISTEN //p' "B 的 SQL 面")"
echo "A: 数据面 $DN_ADDR_A / SQL $SQL_A"
echo "B: 数据面 $DN_ADDR_B / SQL $SQL_B"

# ---------------------------------------------------------------- ③ 建表（走 metanode，两边都看得见）
say "③ 建表（DDL 经 raft 进 metanode 目录）"
"$CLI" --addr "$SQL_A" query "CREATE TABLE $TABLE_SQL (a BIGINT)" >/dev/null
echo "已建 $TABLE_SQL"

# B 的查询缓存靠巡检刷新（`spawn_reconcile` 每次都 `cache.refresh`）—— 等它看见这张表
deadline=$((SECONDS + 30))
while :; do
  if "$CLI" --addr "$SQL_B" query "SELECT count(*) FROM $TABLE_SQL" >/dev/null 2>&1; then break; fi
  (( SECONDS < deadline )) || { echo "B 30s 内没看到这张表（目录同步？）"; tail -20 "$WORK/b.log"; exit 1; }
  sleep 0.3
done
echo "B 也看到这张表了（目录已同步）"

# ---------------------------------------------------------------- ④ 并发真写
say "④ 两个写进程**并发**真写（各自的 SQL 面；值域不重叠）"
( "$CLI" --addr "$SQL_A" query "INSERT INTO $TABLE_SQL VALUES (1),(2),(3)" > "$WORK/w-a.log" 2>&1 ) &
W_A=$!
( "$CLI" --addr "$SQL_B" query "INSERT INTO $TABLE_SQL VALUES (4),(5),(6)" > "$WORK/w-b.log" 2>&1 ) &
W_B=$!
wait "$W_A"
wait "$W_B"
echo "A 写 1,2,3；B 写 4,5,6（并发）"

# ---------------------------------------------------------------- ⑤ 对拍：两边各自查都得 [1..6]
say "⑤ 对拍：两边各自查一次，都必须得到全部 6 行"
EXPECT="$(printf '1\n2\n3\n4\n5\n6\n')"
deadline=$((SECONDS + 60))
GOT_A=""; GOT_B=""
while :; do
  GOT_A="$(rows_of "$SQL_A")"
  GOT_B="$(rows_of "$SQL_B")"
  if [[ "$GOT_A" == "$EXPECT" && "$GOT_B" == "$EXPECT" ]]; then break; fi
  if (( SECONDS >= deadline )); then
    echo "FAIL: 60s 内没等到两边都 [1..6]"
    echo "  A 实际: $(echo "$GOT_A" | paste -sd, -)"
    echo "  B 实际: $(echo "$GOT_B" | paste -sd, -)"
    tail -20 "$WORK/a.log" "$WORK/b.log"
    exit 1
  fi
  sleep 0.3
done
echo "PASS(对拍): A=[1,2,3,4,5,6]，B=[1,2,3,4,5,6]"
echo "  （每行只出一次；对方那 3 行经数据面 gRPC 拉来 —— 本地只有自己的 3 行）"

# ---------------------------------------------------------------- ⑥ S3 里要真的出现文件
say "⑥ 等 flush 落到 S3"
echo "  说明：数据进程用的是**生产默认的 seal/flush 节奏**（窗口关闭 seal + 确定性相位），"
echo "        3 行的批次要等窗口关闭或 max_resident(60s) ⇒ 这里最多等 180s。"
BEFORE="$(s3_keys | wc -l | tr -d ' ')"
deadline=$((SECONDS + 180))
while :; do
  NOW="$(s3_keys | wc -l | tr -d ' ')"
  if (( NOW >= 2 )); then break; fi
  (( SECONDS < deadline )) || {
    echo "FAIL: 180s 内 $PREFIX 下没出现 ≥2 个对象（当前 $NOW；flush 没走网络 PUT？）"
    echo "  A 的 chunk flushed 次数: $(grep -c 'chunk flushed' "$WORK/a.log" || true)"
    echo "  B 的 chunk flushed 次数: $(grep -c 'chunk flushed' "$WORK/b.log" || true)"
    tail -20 "$WORK/a.log"; exit 1
  }
  sleep 1
done
echo "PASS(写 S3): $PREFIX 下 $(s3_keys | wc -l | tr -d ' ') 个对象"
s3_keys | sed 's/^/    /'
echo "  各节点 flush 次数（来自各自日志）：A=$(grep -c 'chunk flushed' "$WORK/a.log" || true) B=$(grep -c 'chunk flushed' "$WORK/b.log" || true)"

# ---------------------------------------------------------------- ⑦ 冷读：删 WAL 重启 ⇒ 只能来自 S3
say "⑦ 冷读：停掉两个数据进程 → 删各自的 WAL/spill（保留 metanode 目录）→ 重启"
for p in "${DN_PIDS[@]}"; do kill -9 "$p" 2>/dev/null || true; done
# `wait` 一次把作业收掉：否则 shell 会在后面某处打印 "Killed"（看着像出了事，其实是我们杀的）
for p in "${DN_PIDS[@]}"; do wait "$p" 2>/dev/null || true; done
DN_PIDS=()
sleep 1
rm -rf "$WORK/a/wal" "$WORK/a/spill" "$WORK/b/wal" "$WORK/b/spill"
start_dn "$INST_A" "$WORK/a" "$WORK/a2.log"
start_dn "$INST_B" "$WORK/b" "$WORK/b2.log"
SQL_A2="$(wait_line "$WORK/a2.log" 's/^SQL-LISTEN //p' "A 重启后的 SQL 面")"
SQL_B2="$(wait_line "$WORK/b2.log" 's/^SQL-LISTEN //p' "B 重启后的 SQL 面")"
echo "重启后 A SQL=$SQL_A2 / B SQL=$SQL_B2（chunk 已空、WAL 已删）"

deadline=$((SECONDS + 60))
while :; do
  GOT_A2="$(rows_of "$SQL_A2")"
  GOT_B2="$(rows_of "$SQL_B2")"
  if [[ "$GOT_A2" == "$EXPECT" && "$GOT_B2" == "$EXPECT" ]]; then break; fi
  if (( SECONDS >= deadline )); then
    echo "FAIL: 重启后 60s 内没等到两边都 [1..6]"
    echo "  A 实际: $(echo "$GOT_A2" | paste -sd, -)"
    echo "  B 实际: $(echo "$GOT_B2" | paste -sd, -)"
    exit 1
  fi
  sleep 0.3
done
echo "PASS(冷读): 删 WAL 重启后两边仍 [1,2,3,4,5,6] —— 只能来自 $ENDPOINT 的网络 GET"

say "结果"
echo "写：两个写进程并发真写 ⇒ 各自查询逐行相等 [1..6]"
echo "存：$PREFIX 下 ≥2 个 parquet 对象（HTTP PUT，各写者都落了文件）"
echo "读：删本地 WAL 重启 ⇒ 6 行仍在（HTTP GET）"
echo "全部通过。实验目录：$WORK"
