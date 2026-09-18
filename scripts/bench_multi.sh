#!/usr/bin/env bash
# 多节点基线压测（T8 扩展；`operation-log §37`）
#
# 用法：scripts/bench_multi.sh [nodes] [secs] [shards_per_node] [rows_per_sec_per_node]
# 例：  scripts/bench_multi.sh 4 90 5 5000
#
# ## 它测什么、不测什么（必须写清楚，否则结论会被误用）
#
# ✅ 测：**N 个独立节点并行**时，全局提交时间线的形状 ——
#    峰值提交/秒、峰值字节/秒、文件数·天、seal 原因分布；
#    CPU/IO 争用下每节点相对独跑的性能退化。
#
# ❌ **不测**：N 个节点打**同一个 Meta** 的 CommitFiles 并发。
#    当前架构的 Catalog 是**进程内**的（`MemoryCatalog`），没有共享 Meta ——
#    要测那个必须等 R3（metanode + raft/gRPC）。本脚本给的是**上界**：
#    未来的 Meta 至少要吸收这里的 Σ 提交量，且节点内是串行 flush、
#    真实并发度只会更高（每个节点各自的 flush 线程同时打）。
set -euo pipefail

NODES="${1:-4}"
SECS="${2:-90}"
SHARDS="${3:-5}"
RPS="${4:-5000}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/release/examples/bench_baseline"
OUT="${YUNTUN_BENCH_OUT:-/tmp/yuntun-multi-$$}"

if [ ! -x "$BIN" ]; then
  echo "缺少 $BIN —— 先跑：cargo build --release -p yuntun-chaos --example bench_baseline" >&2
  exit 1
fi

mkdir -p "$OUT"
echo "== 多节点基线压测 =="
echo "nodes=$NODES secs=$SECS shards/node=$SHARDS rows/s/node=$RPS (合计 $((NODES * RPS)) rows/s)"
echo "输出目录：$OUT"
echo "说明：Catalog 仍是进程内（无共享 Meta）→ 本结果是 R3 前 Meta 承载量的**上界**"

# 每节点：独立 WAL / spill / store 目录（节点私有状态，ADR-3）+ 独立批量参数
# batches/s 固定 40（与单节点基线一致，便于横向比）
BATCHES=40
BATCH_ROWS=$((RPS / BATCHES))
[ "$BATCH_ROWS" -lt 1 ] && BATCH_ROWS=1

pids=()
for i in $(seq 0 $((NODES - 1))); do
  ( YUNTUN_BENCH_DUMP="$OUT/node-$i.csv" \
    "$BIN" "$SECS" "$SHARDS" "$BATCH_ROWS" "$BATCHES" 0 30 500000 800 128 \
    > "$OUT/node-$i.log" 2>&1 ) &
  pids+=($!)
done
echo "已启动 $NODES 个节点进程（pid: ${pids[*]}），等待结束…"

rc=0
for p in "${pids[@]}"; do
  wait "$p" || rc=1
done
echo "全部结束（exit=$rc）"

# ---------- 聚合 ----------
# 用 CSVs 合并后重新分桶：峰值必须**在全局时间线上**取，而不是把各节点的
# 峰值相加（那样会把不同时刻的峰叠加，虚高）。
# 先合并、再按时刻排序（用 sort 而不是 gawk 的 asort：mawk/busybox 上没有 asort）
{ for f in "$OUT"/node-*.csv; do [ -f "$f" ] && tail -n +2 "$f"; done; } \
  | sort -t, -k1,1n > "$OUT/all.csv"

awk -F, '
  { t[NR] = $1; rows += $2; bytes += $3; n++ }
  END {
    if (n == 0) { print "没有提交样本"; exit }
    # 每秒分桶（滑动窗口取最大）
    best = 0
    for (i = 1; i <= n; i++) {
      j = i
      while (j <= n && t[j] < t[i] + 1000) j++
      if (j - i > best) best = j - i
    }
    # 每 100ms 分桶（瞬时尖峰）
    best100 = 0
    for (i = 1; i <= n; i++) {
      j = i
      while (j <= n && t[j] < t[i] + 100) j++
      if (j - i > best100) best100 = j - i
    }
    span = (t[n] - t[1]) / 1000.0
    printf "全局提交样本      : %d 个文件\n", n
    printf "全局提交速率      : %.2f 次/秒（均值，跨度 %.1fs）\n", n / (span > 0 ? span : 1), span
    printf "全局峰值提交      : %d 次/秒；%d 次/100ms\n", best, best100
    printf "总写入行数        : %d（≈ %.1f 万行/秒）\n", rows, rows / (span > 0 ? span : 1) / 10000
  }
' "$OUT/all.csv"

echo
echo "-- 各节点 seal 原因分布（汇总）--"
for f in "$OUT"/node-*.csv; do
  [ -f "$f" ] || continue
  echo "$(basename "$f"):"
  awk -F, 'FNR>1 {c[$4]++} END {for (k in c) printf "    %-16s %d\n", k, c[k]}' "$f" | sort
done

echo
echo "-- 各节点文件数 / 单文件行数（p50）--"
for f in "$OUT"/node-*.log; do
  [ -f "$f" ] || continue
  printf "%s: " "$(basename "$f")"
  grep -E "^文件数|^单文件行数" "$f" | tr '\n' ' ' || true
  echo
done
echo
echo "完整日志：$OUT/node-*.log（RESULT 行可直接横向对比）"
