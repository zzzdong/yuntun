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

case "${1:-}" in
  build) build ;;
  up) up ;;
  wait) wait_ready ;;
  probe) shift; probe "$@" ;;
  partition) shift; partition "$@" ;;
  heal) shift; heal "$@" ;;
  smoke) smoke ;;
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
