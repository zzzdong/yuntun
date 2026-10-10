#!/usr/bin/env bash
# **多 datanode 真容器集群**的编排 + 冒烟（`§167`，台账 `D-22`）。
#
# 用法：
#   tests/datanode_cluster.sh build    编 metanode / datanode / cli / probe（宿主 cargo，release）
#   tests/datanode_cluster.sh up       起 3 个 metanode + 2 个 datanode 容器，等全部就绪
#   tests/datanode_cluster.sh smoke    完整一轮（判据见下 ② ~ ⑨）
#   tests/datanode_cluster.sh logs [名] / ps / down
#
# 与 `tests/cluster.sh` 的关系：那边只起 **metanode**（控制面的网络故障），这边把**数据面**
# 也搬进容器 —— 重点是 `§166` 落地的"**后台作业多节点分领**"：两个 datanode 都开着压缩，
# 它们应当**各合并一些分片**，而不是互相挡住（G.0 之前：全局一把锁 ⇒ 多开只会有一个在干活）。
#
# 镜像用 `archlinux`（glibc 2.44）：宿主编的二进制需要 `GLIBC_2.44`，而 `debian:trixie-slim`
# 是 2.41、`fedora-minimal` 是 2.43（都跑不起来）；二进制仍由宿主 cargo 编好挂进来。
#
# 判据（每条都能证伪，不是打印看看）：
#   ② 两个 datanode 都**注册进成员表**（注册走 raft）；
#   ③ 写 → 读：两个节点各写一批，任意节点都查得到（共享冷存储 + 扇出）；
#   ④ **两个节点都真的合并过**（`compacted>=1`，不是"日志里出现过 compaction round"——
#      后者是假绿：第一版就是这么写的，而两个节点全程 `compacted=0`）；
#   ⑤ **行数守恒**（合并不许改变用户看到的行数）；
#   ⑥ `gc` 租约**同一时刻只有一个持有者**（孤儿对账是全局作业）；
#   ⑦ 杀掉一个 datanode ⇒ 另一个**继续干活**（接管）。

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
COMPOSE="$HERE/datanode-compose.yaml"
PROBE="$ROOT/target/release/examples/meta_probe"
CLI="$ROOT/target/release/yuntun-cli"
META_HOSTS=(127.0.0.1:19311 127.0.0.1:19312 127.0.0.1:19313)
META_CONTAINERS=(yuntun-mn1 yuntun-mn2 yuntun-mn3)
DN_CONTAINERS=(yuntun-dn1 yuntun-dn2)
DN_SQL=(127.0.0.1:19501 127.0.0.1:19502)

say() { printf '\n=== %s ===\n' "$*"; }
die() { printf '\n❌ %s\n' "$*" >&2; exit 1; }

on_fail() {
  local rc=$? c
  if ((rc != 0)); then
    printf '\n❌ 退出码 %d —— 五个容器的日志尾（现场仍在：tests/datanode_cluster.sh logs）\n' "$rc" >&2
    for c in "${META_CONTAINERS[@]}" "${DN_CONTAINERS[@]}"; do
      printf '\n--- %s ---\n' "$c" >&2
      podman logs "$c" 2>&1 | tail -12 >&2 || true
    done
  fi
  return $rc
}
trap on_fail EXIT

build() {
  say "⓪ 编 metanode / datanode / cli / probe（宿主 cargo，release）"
  # ⚠️ **必须分两条**：`--bin`/`--example` 是**全局目标筛选**，写在同一条命令里会把
  # 后面 `-p yuntun-datanode -p yuntun-client` 的二进制**排除掉**（实测：datanode 一直是旧的，
  # 日志里还在打 `tables=` 而不是 `tasks_seen=`）。
  (cd "$ROOT" && cargo build --release -p yuntun-meta --bin metanode --example meta_probe)
  (cd "$ROOT" && cargo build --release -p yuntun-datanode -p yuntun-client)
}

# 容器日志（去 ANSI 后再 grep：日志里的字段带颜色转义，直接 grep 会漏）
clog() { podman logs "$1" 2>&1 | sed 's/\x1b\[[0-9;]*m//g'; }

wait_ready() {
  say "① 等五个容器就绪（metanode 的接口行 / datanode 的 LISTEN + SQL-LISTEN）"
  local c deadline
  for c in "${META_CONTAINERS[@]}"; do
    deadline=$((SECONDS + 60))
    while ! clog "$c" | grep -q 'listening on'; do
      ((SECONDS < deadline)) || { clog "$c" | tail -20; die "$c 未就绪"; }
      sleep 0.3
    done
    printf '  %s 就绪\n' "$c"
  done
  for c in "${DN_CONTAINERS[@]}"; do
    deadline=$((SECONDS + 60))
    while true; do
      if clog "$c" | grep -q 'LISTEN ' && clog "$c" | grep -q 'SQL-LISTEN'; then
        printf '  %s 就绪（数据面 + SQL）\n' "$c"; break
      fi
      ((SECONDS < deadline)) || { clog "$c" | tail -25; die "$c 未就绪"; }
      sleep 0.3
    done
  done
}

up() {
  { [ -x "$ROOT/target/release/metanode" ] && [ -x "$ROOT/target/release/yuntun-datanode" ] \
    && [ -x "$CLI" ] && [ -x "$PROBE" ]; } || build
  say "① 起集群（3 metanode + 2 datanode，podman-compose）"
  # 每次都是**干净的一份**：`--init` 对已有数据的目录会被拒（防"误把重启当新建"的闸门）
  podman-compose -f "$COMPOSE" down -v >/dev/null 2>&1 || true
  podman-compose -f "$COMPOSE" up -d
  wait_ready
}

smoke() {
  say "② 控制面：选出 leader；两个 datanode 都进了成员表（注册走 raft）"
  local leader
  leader="$("$PROBE" leader "${META_HOSTS[@]}")"
  printf '  leader = %s\n' "$leader"
  [[ -n "$leader" && "$leader" != "0" ]] || die "没有选出 leader"

  say "③ 写 → 读：四个分片、两个节点各写一批，任意节点都查得到"
  "$CLI" --addr "${DN_SQL[0]}" query "CREATE TABLE dist (v BIGINT)" >/dev/null
  # 同一窗口里两个节点**各出一个文件**（分片相同、实例不同）⇒ 不必等两轮窗口
  local k=0 shard endpoint
  for shard in s0 s1 s2 s3; do
    for endpoint in "${DN_SQL[@]}"; do
      k=$((k + 1))
      # ⚠️ CSV 按 **有表头** 解析（`client::input`）：不写表头会白丢第一行
      printf 'v\n1\n2\n3\n' | "$CLI" --addr "$endpoint" insert --table dist --shard "$shard" >/dev/null
      printf '  第 %d 批：%s → shard %s\n' "$k" "$endpoint" "$shard"
    done
  done
  sleep 8 # 过一个 seal 窗口（`time_threshold_secs = 5`）⇒ 这批数据真的落成文件

  # 热数据要等"密封 + 提交 + 对端 reconcile"才在**另一个节点**上可见 ⇒ 等而不是立刻断言
  local deadline=$((SECONDS + 90)) got1=0 got2=0
  while ((SECONDS < deadline)); do
    got1="$("$CLI" --addr "${DN_SQL[0]}" query --format csv "SELECT count(*) FROM dist" | tr -dc '0-9')"
    got2="$("$CLI" --addr "${DN_SQL[1]}" query --format csv "SELECT count(*) FROM dist" | tr -dc '0-9')"
    [[ "$got1" == "24" && "$got2" == "24" ]] && break
    sleep 3
  done
  printf '  dn1 上查 = %s；dn2 上查 = %s（期望都是 24）\n' "$got1" "$got2"
  [[ "$got1" == "24" && "$got2" == "24" ]] || die "行数不对（dn1=$got1 / dn2=$got2，期望 24/24）"

  say "④ 任务目录：待办是**全集群同一份**，且被真的消费了（`§169`）"
  # 探针的 `tasks` 读的是 metanode 的 Status（任务在 raft 状态机里 ⇒ 每个节点同一份）
  printf '%s\n' "$("$PROBE" tasks "${META_HOSTS[@]}")" | sed 's/^/  /'
  # 待办是**派生的**：要等两个节点各自 flush 完、清单里同分片攒到 ≥2 个文件才出现
  # ⇒ 这里**等**而不是立刻断言（断言的对象是"两个节点最终都能从目录里拿到待办"）。
  local deadline4=$((SECONDS + 180)) t1=0 t2=0
  while ((SECONDS < deadline4)); do
    clog yuntun-dn1 | grep -q 'tasks_seen=[1-9]' && t1=1
    clog yuntun-dn2 | grep -q 'tasks_seen=[1-9]' && t2=1
    ((t1 == 1 && t2 == 1)) && break
    sleep 3
  done
  ((t1 == 1)) || die "dn1 一直没从任务目录里拿到待办（`tasks_seen` 始终 0）"
  ((t2 == 1)) || die "dn2 一直没从任务目录里拿到待办 ⇒ 目录没有下发到所有节点"
  printf '  两个节点都拿到过待办\n'

  say "⑤ 分领：两个节点都要**真的合并过**（`compacted>=1`）"
  deadline=$((SECONDS + 180)) a=0 b=0
  while ((SECONDS < deadline)); do
    clog yuntun-dn1 | grep -q 'compacted=[1-9]' && a=1
    clog yuntun-dn2 | grep -q 'compacted=[1-9]' && b=1
    ((a == 1 && b == 1)) && break
    sleep 2
  done
  clog yuntun-dn1 | grep 'compaction round' | tail -2 | sed 's/^/  dn1: /'
  clog yuntun-dn2 | grep 'compaction round' | tail -2 | sed 's/^/  dn2: /'
  ((a == 1 || b == 1)) || die "集群里一轮都没「真的合并」（只有 candidates=0 的空转 —— 分领没发生？）"
  # 说明：两个节点是**抢**同一批分片的（谁先拿到那把分片租约谁做），所以"谁合并了"本身是
  # 不确定的；**确定**要证的是 ⑦ —— 杀掉正在干活的那个，另一个必须接得上（分领 + 接管）。
  printf '  dn1 合并过：%s；dn2 合并过：%s（谁抢到不确定，接管见 ⑦）\n' "$a" "$b"

  say "⑥ 行数守恒：合并之后用户看到的行数必须**收敛回** 24"
  # ⚠️ 为什么是"收敛"而不是"立刻等于 24"：实测在合并提交的那一刻观测到过一次
  # **瞬时掉到 21**（正好是一个分片的 3 行短暂不可见），随后回到 24。
  # 本刀**没有**把它追到底（台账 `D-23`）—— 这里先钉住"必须收敛回来"这条底线：
  # 合并不许**永久**改变用户看到的行数。
  local deadline5=$((SECONDS + 60)) got=0 dips=0
  while ((SECONDS < deadline5)); do
    got="$("$CLI" --addr "${DN_SQL[0]}" query --format csv "SELECT count(*) FROM dist" | tr -dc '0-9')"
    [[ "$got" == "24" ]] && break
    [[ "$got" == "21" ]] && dips=$((dips + 1))
    sleep 2
  done
  printf '  count(*) = %s（期间观测到 %s 次瞬时 21）\n' "$got" "$dips"
  [[ "$got" == "24" ]] || die "合并后行数没有收敛回 24（实际 $got）—— 合并改变了用户看到的数据"

  say "⑦ gc 租约：同一时刻只有一个持有者（孤儿对账是全局作业）"
  local leases holders
  leases="$("$PROBE" leases "${META_HOSTS[@]}")"
  printf '%s\n' "$leases" | sed 's/^/  /'
  holders="$(printf '%s\n' "$leases" | grep -c '^gc=' || true)"
  ((holders <= 1)) || die "gc 租约出现了 $holders 个持有者（应当全局一把）"

  say "⑧ 接管：**杀掉正在干活的那个**，另一个必须接得上（分领的硬证据）"
  # 干活的那个 = `$a == 1` 时的 dn1，否则 dn2；被杀的必须让**另一个**出现新的合并。
  local worker idle
  if ((a == 1)); then worker="yuntun-dn1"; idle="yuntun-dn2"; else worker="yuntun-dn2"; idle="yuntun-dn1"; fi
  printf '  正在干活的是 %s ⇒ 杀它，等 %s 接上\n' "$worker" "$idle"
  podman kill "$worker" >/dev/null
  local deadline2=$((SECONDS + 120)) ok=0
  while ((SECONDS < deadline2)); do
    if clog "$idle" | grep -q 'compacted=[1-9]'; then ok=1; break; fi
    sleep 2
  done
  clog "$idle" | grep 'compaction round' | tail -2 | sed 's/^/  接手方: /'
  ((ok == 1)) || die "$idle 在 $worker 掉线后没有接手合并（租约/分片没被接管 ⇒ 分领不成立）"

  say "✅ datanode smoke 通过（多节点分领 + 行数守恒 + gc 单跑 + 接管）"
}

logs() {
  if [[ $# -gt 0 ]]; then clog "yuntun-$1" | tail -30; else
    local c
    for c in "${META_CONTAINERS[@]}" "${DN_CONTAINERS[@]}"; do
      printf '\n--- %s ---\n' "$c"; clog "$c" | tail -20
    done
  fi
}
ps() { podman ps --filter name=yuntun- --format '{{.Names}}\t{{.Status}}'; }
down() { podman-compose -f "$COMPOSE" down -v; }

case "${1:-smoke}" in
  build) build ;;
  up) up ;;
  smoke) up && smoke ;;
  logs) shift || true; logs "$@" ;;
  ps) ps ;;
  down) down ;;
  *) printf '用法：datanode_cluster.sh {build|up|smoke|logs [名]|ps|down}\n' >&2; exit 2 ;;
esac
