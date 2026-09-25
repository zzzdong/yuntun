#!/usr/bin/env bash
# 三节点 metanode **容器化真集群**的编排 + 冒烟（见 `tests/compose.yaml` 顶部的动机）。
#
# 用法：
#   tests/cluster.sh build            编出 `metanode` 与 `meta_probe`（宿主 cargo）
#   tests/cluster.sh up               起三个容器并等它们**全都就绪**（接口行）
#   tests/cluster.sh probe <子命令>…  在集群网络里跑一次 `meta_probe`（status/leader/write/verify）
#   tests/cluster.sh partition <N>    把 N 号容器**真断网**（`podman network disconnect`）
#   tests/cluster.sh heal <N>         把它接回去
#   tests/cluster.sh smoke            完整一轮：healthy 写 → 断 3 号 → 多数派仍能写 → 接回 → 复核
#   tests/cluster.sh logs [N] / ps / down
#
# 为什么断言要跑在**集群网络里的容器**中：这样探测路径与节点之间的路径完全一样（都用容器 DNS +
# 容器网络），不会引入"宿主的端口映射"这个额外变量。

set -euo pipefail

NET=yuntun-cluster
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
COMPOSE="$HERE/compose.yaml"
IMAGE=docker.io/library/debian:trixie-slim
PROBE="$ROOT/target/debug/examples/meta_probe"
ADDRS=(mn1:9311 mn2:9311 mn3:9311)
CONTAINERS=(yuntun-mn1 yuntun-mn2 yuntun-mn3)

say() { printf '\n=== %s ===\n' "$*"; }
die() { printf '\n❌ %s\n' "$*" >&2; exit 1; }

# 失败时把三个容器的日志尾打出来 —— 这个 rig 的用处之一就是"出错时现场还在"（容器不自动死）
on_fail() {
  local rc=$?
  if ((rc != 0)); then
    printf '\n❌ 退出码 %d —— 下面是三个容器的日志尾（现场仍在：tests/cluster.sh logs / ps）\n' "$rc" >&2
    for c in "${CONTAINERS[@]}"; do
      printf '\n--- %s ---\n' "$c" >&2
      podman logs "$c" 2>&1 | tail -15 >&2 || true
    done
  fi
  return $rc
}
trap on_fail EXIT

build() {
  say "① 编 metanode + meta_probe（宿主 cargo）"
  (cd "$ROOT" && cargo build -p yuntun-meta --bin metanode --example meta_probe)
}

# 等三个容器**各自**打印接口行。这一条同时证明了：集群起得来、且**已经选出 leader**
# （`§103`：接口行是在"集群里有 leader"之后才打印的）。
wait_ready() {
  say "② 等三个节点就绪（接口行）"
  for i in 0 1 2; do
    local c="${CONTAINERS[$i]}" deadline=$((SECONDS + 45))
    while true; do
      if podman logs "$c" 2>&1 | grep -q 'listening on'; then
        printf '  %s 就绪：%s\n' "$c" "$(podman logs "$c" 2>&1 | grep 'listening on' | tail -1)"
        break
      fi
      if ((SECONDS >= deadline)); then
        podman logs "$c" 2>&1 | tail -20
        die "$c 45s 内没就绪（上面是它的日志尾）"
      fi
      sleep 0.3
    done
  done
}

up() {
  [ -x "$ROOT/target/debug/metanode" ] || build
  say "① 起集群（podman-compose）"
  # 每次都是**干净的一份**：`--init` 对已有数据的目录会被拒（那是防"误把重启当新建"的闸门）
  podman-compose -f "$COMPOSE" down -v >/dev/null 2>&1 || true
  podman-compose -f "$COMPOSE" up -d
  wait_ready
}

probe() {
  [ -x "$PROBE" ] || build
  podman run --rm --network "$NET" \
    -v "$PROBE:/usr/local/bin/meta_probe:ro" "$IMAGE" \
    /usr/local/bin/meta_probe "$@"
}

addr_args() { printf '%s\n' "${ADDRS[@]}"; }

partition() {
  local n="${1:?usage: partition <1|2|3>}"
  say "⛔ 把 mn$n 真断网（podman network disconnect）"
  podman network disconnect "$NET" "yuntun-mn$n"
}

heal() {
  local n="${1:?usage: heal <1|2|3>}"
  say "🔌 把 mn$n 接回网络"
  # **必须带 `--alias`**：`podman network connect` 不带别名时，该容器在这张网络的 DNS 里只剩容器
  # 哈希 —— 别的节点就再也解析不到 `mn3` 这个名字（它们连的是名字，不是 IP）。本脚本第一版没带，
  # 结果"接回来自愈"整段连不上（现场可复现：`getent hosts mn3` 在网络里查不到）。
  # 顺带一提，重新接入通常会**换 IP**（实测 .2 → .10）—— 各节点地址写的是**名字**，所以没事。
  podman network connect --alias "mn$n" "$NET" "yuntun-mn$n"
}

# 等三方**真收敛**：都有同一个 leader，且 `last` 全等（没有 Candidate/PreCandidate）。
#
# 为什么这条断言必须有：脚本第一版只是**打印**了三方 Status 就宣布"应当收敛"，而那次接回后
# mn3 其实还停在 `PreCandidate/commit=2/last=2`（落后 2 条）—— 嘴上说收敛、实际没验。
# 而"多数派能写、落后的那个再也补不上"正是 `§106`/`§107` 那个写停摆的样子，
# 所以这条断言是这套 rig 的**核心**，不是装饰。
await_convergence() {
  local secs="${1:-30}"
  # ⚠️ 别写成同一个 `local`：`local a=1 b=$((a))` 里 `a` 还没生效（`set -u` 下直接报未绑定）
  local deadline=$((SECONDS + secs))
  local out="" lasts="" leaders=""
  say "⏳ 等三方收敛（同一个 leader 且 last 全等，最多 ${secs}s）"
  while true; do
    out="$(probe status "${ADDRS[@]}" 2>&1)" || true
    lasts="$(printf '%s\n' "$out" | grep -o 'last=[0-9]*' | sort -u | wc -l)"
    leaders="$(printf '%s\n' "$out" | grep -o 'leader=[0-9]*' | sort -u | wc -l)"
    if [ "$lasts" = "1" ] && [ "$leaders" = "1" ] && ! printf '%s\n' "$out" | grep -qE 'Candidate'; then
      printf '%s\n' "$out" | sed 's/^/  /'
      printf '  ✅ 三方已收敛（%s）\n' "$(printf '%s\n' "$out" | grep -o 'last=[0-9]*' | head -1)"
      return 0
    fi
    if ((SECONDS >= deadline)); then
      printf '%s\n' "$out" | sed 's/^/  /' >&2
      die "三方在 ${secs}s 内没收敛 —— 这就是'写停摆'的样子（多数派能写、落后的那个补不上）"
    fi
    printf '  ⋯ 还没收敛：%s / %s\n' \
      "$(printf '%s\n' "$out" | grep -o 'last=[0-9]*' | sort -u | tr '\n' ' ')" \
      "$(printf '%s\n' "$out" | grep -o 'role=[A-Za-z]*' | sort -u | tr '\n' ' ')"
    sleep 2
  done
}

smoke() {
  up
  say "③ 健康态：找出 leader + 写一条（幂等键 k1）"
  probe leader "${ADDRS[@]}"
  probe write k1 "${ADDRS[@]}"
  probe status "${ADDRS[@]}"

  say "④ 断掉 mn3，多数派（1+2）应当照常提交"
  partition 3
  sleep 2
  probe write k2 mn1:9311 mn2:9311
  say "   （再确认这条真的进了状态机：重放 k2 必须被拒）"
  probe verify k2 mn1:9311 mn2:9311

  say "⑤ 把 mn3 接回来，三个节点**必须真的收敛**（这条断言见函数注释）"
  heal 3
  await_convergence 30
  say "   收敛后再写一条（幂等键 k3），三个节点都要认得"
  probe write k3 "${ADDRS[@]}"
  probe verify k3 "${ADDRS[@]}"
  await_convergence 30

  say "✅ smoke 通过"
}

# **冻住 / 解冻**一个节点（`podman pause` = cgroup freezer 真实冻结进程）。
#
# 为什么用冻结而不是 `tc netem` / `iptables`：本机是 **rootless podman**，网络走 pasta/slirp，
# 宿主侧**没有可切的 veth**，容器里也没装 iproute2；而冻结是 podman 自带的能力，且它模拟的是
# 一种很真实的故障形态：**长 GC / 长 IO 停顿 / 宿主压力** —— 对端进程活着、TCP 连接也在，
# 但请求一律**超时**（不是"连接被拒"）。这正是造出"**leader 以为发出去了、follower 没应用**"
# （`§106`/`§107` 的 `next > matched+1`）最直接的办法。
freeze() {
  local n="${1:?usage: freeze <1|2|3> [secs]}"
  say "🧊 冻住 mn$n（podman pause）"
  podman pause "yuntun-mn$n" >/dev/null
}

unfreeze() {
  local n="${1:?usage: unfreeze <1|2|3>}"
  say "🔥 解冻 mn$n（podman unpause）"
  podman unpause "yuntun-mn$n" >/dev/null
}

# 刻意造 "**稳定的 follower 丢一条 append**"：冻住一个 follower 几秒，期间在多数派上写几条
# （leader 会照发、照乐观推进），再解冻 —— 然后看它到底能不能补上（`§109.4` 的第 1 步）。
gap() {
  local secs="${1:-3}"
  local writes="${2:-3}"
  say "① 起集群（压缩阈值 = ${YUNTUN_SNAPSHOT_LOG_ENTRIES:-100000}）"
  up
  local l frozen
  l="$(probe leader "${ADDRS[@]}")"
  frozen=$([ "$l" = "1" ] && echo 2 || echo 1)
  printf '  leader = %s，要冻的是 mn%s\n' "$l" "$frozen"

  say "② 冻住 mn$frozen ${secs}s，期间在多数派上写 $writes 条"
  freeze "$frozen"
  local alive=() n i
  for n in 1 2 3; do [ "$n" = "$frozen" ] || alive+=("mn$n:9311"); done
  for i in $(seq 1 "$writes"); do
    printf '  g%s: ' "$i"
    probe write "g$i" "${alive[@]}" || die "多数派写不进去（第 $i 条）—— 先查环境"
  done
  sleep "$secs"

  say "③ 解冻 mn$frozen，等三方收敛（**判据**：`§106` 那个 gap 会让它一直红）"
  unfreeze "$frozen"
  await_convergence 60
  say "✅ 三方收敛——这一形态下也没复现 §106 的停摆"
}

# 复现 `§106`/`§107` 的**写停摆**：小压缩阈值 + 一个有 follower 落后（**断的是 follower，不是 leader**
# —— 那样多数派还能提交，落后的那个才需要靠 leader 补日志）。
#
# 在容器里跑它的意义：一次回答"那个进程内探针**是不是夹具造出来的**"。红了 = 真 bug 有真环境的
# 复现（可以在真环境里改、真环境里验）；绿了 = 进程内那套夹具的产物，得回头改夹具。
stall() {
  export YUNTUN_SNAPSHOT_LOG_ENTRIES="${YUNTUN_SNAPSHOT_LOG_ENTRIES:-4}"
  say "① 起集群（压缩阈值 = ${YUNTUN_SNAPSHOT_LOG_ENTRIES} 条）"
  up

  say "② 看清谁是 leader，然后断掉**另一个** follower"
  local l victim
  l="$(probe leader "${ADDRS[@]}")"
  victim=$([ "$l" = "1" ] && echo 2 || echo 1)
  printf '  leader = %s，要断的是 mn%s\n' "$l" "$victim"
  partition "$victim"
  sleep 2

  say "③ 在多数派上连写 10 条（阈值小 ⇒ 一路触发压缩）"
  local alive=() n i
  for n in 1 2 3; do [ "$n" = "$victim" ] || alive+=("mn$n:9311"); done
  for i in $(seq 1 10); do
    printf '  s%s: ' "$i"
    probe write "s$i" "${alive[@]}" ||
      die "多数派写不进去（第 $i 条）—— 先查环境，这还不是本场景要测的东西"
  done

  say "④ 接回 mn$victim，等三方收敛（**这里就是判据**：`§106` 的停摆会让它一直红）"
  heal "$victim"
  await_convergence 60
  say "✅ 三方收敛——容器里**没能**复现 §106 的停摆"
}

case "${1:-}" in
  build) build ;;
  up) up ;;
  wait) wait_ready ;;
  probe) shift; probe "$@" ;;
  partition) shift; partition "$@" ;;
  heal) shift; heal "$@" ;;
  smoke) smoke ;;
  stall) stall ;;
  freeze) shift; freeze "$@" ;;
  unfreeze) shift; unfreeze "$@" ;;
  gap) shift; gap "$@" ;;
  logs)
    if [ $# -ge 2 ]; then podman logs "yuntun-mn$2" 2>&1 | tail -40
    else for c in "${CONTAINERS[@]}"; do printf '\n--- %s ---\n' "$c"; podman logs "$c" 2>&1 | tail -8; done; fi
    ;;
  ps) podman ps -a --filter name=yuntun-mn --format '{{.Names}}\t{{.Status}}\t{{.Networks}}' ;;
  down)
    podman-compose -f "$COMPOSE" down -v
    # 兜底：万一 compose 记不住（改过名字/手工起过），按名字清干净
    for c in "${CONTAINERS[@]}"; do podman rm -f "$c" >/dev/null 2>&1 || true; done
    podman network rm "$NET" >/dev/null 2>&1 || true
    ;;
  *)
    sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 64
    ;;
esac
