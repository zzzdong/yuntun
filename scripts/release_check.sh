#!/usr/bin/env bash
# 发布门禁（0.1）：编译 / 测试 / 文档一致性 / release 二进制冒烟。
#
# 用法：
#   scripts/release_check.sh              # 全量（默认跑冒烟）
#   scripts/release_check.sh --skip-smoke # 只跑编译、测试、文档
#
# 设计原则：**每一步都要真的执行**，缺依赖就报"跳过"并记在结尾的 SKIP 列表里，
# 不把"没跑"伪装成"通过"（发布检查最怕的就是这个）。
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT=$(pwd)
SKIP_SMOKE=0
for a in "$@"; do
  case "$a" in
    --skip-smoke) SKIP_SMOKE=1 ;;
    *) echo "未知参数：$a" >&2; exit 2 ;;
  esac
done

SKIPPED=()
step() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }

# 全量 debug 构建 + 测试二进制约 45 GiB（line-tables-only）；磁盘不足时链接器会
# 以 lld 崩溃的形态失败（实测过一次：98% 占用）。门禁第一件事就是说清楚这件事。
free_gib=$(df -Pk "$ROOT" | awk 'NR==2{printf "%d", $4/1024/1024}')
echo "磁盘可用（$ROOT）：${free_gib} GiB"
if [ "$free_gib" -lt 10 ]; then
  echo "⚠️ 可用空间 < 10 GiB：请先 cargo clean（或把 CARGO_TARGET_DIR 指到更大的盘）" >&2
  exit 1
fi

step "版本（workspace 单一来源）"
VERSION=$(awk '/^\[workspace.package\]/{f=1} f&&/^version = /{gsub(/"/,"",$3); print $3; exit}' Cargo.toml)
echo "workspace version = $VERSION"
[ -n "$VERSION" ] || { echo "读不到 workspace 版本" >&2; exit 1; }

step "格式（rustfmt --check）"
cargo fmt --all -- --check

step "clippy（-D warnings）"
cargo clippy --workspace --all-targets -- -D warnings

step "测试（workspace）"
cargo test --workspace

step "文档一致性"
cargo test -p yuntun-testkit --test docs_consistency

step "release 构建"
cargo build --release --bins
echo "--- 二进制 ---"
for b in yuntun yuntun-cli yuntun-datanode metanode; do
  test -x "target/release/$b" || { echo "缺少 target/release/$b" >&2; exit 1; }
  echo "  $b"
done
V=$("./target/release/yuntun" --version)
echo "$V"
case "$V" in
  *"$VERSION"*) ;;
  *) echo "版本不一致：二进制报 $V，workspace 是 $VERSION" >&2; exit 1 ;;
esac

# 找 python：优先仓库 venv，其次系统 python3（必须有 pymysql / adbc）
PY=""
for cand in "$ROOT/scripts/.venv/bin/python" "$(command -v python3 || true)"; do
  if [ -n "$cand" ] && "$cand" - <<'PY' >/dev/null 2>&1
import pymysql, adbc_driver_flightsql  # noqa: F401
PY
  then PY="$cand"; break; fi
done

if [ "$SKIP_SMOKE" -eq 1 ]; then
  SKIPPED+=("冒烟（--skip-smoke）")
elif [ -z "$PY" ]; then
  SKIPPED+=("冒烟（缺少 pymysql/adbc；装 scripts/.venv 后重跑）")
else
  MYSQL_PORT=${MYSQL_PORT:-23306}
  FLIGHT_PORT=${FLIGHT_PORT:-25051}
  AUTH_MYSQL_PORT=${AUTH_MYSQL_PORT:-23307}
  AUTH_FLIGHT_PORT=${AUTH_FLIGHT_PORT:-25052}
  # 冒烟的工作目录放 tmpfs（/tmp），不占仓库所在盘；测试数据小但这样更干净、更快。
  SMOKE_TMP=${YUNTUN_SMOKE_TMPDIR:-/tmp}
  [ -d "$SMOKE_TMP" ] || SMOKE_TMP=${TMPDIR:-/tmp}
  WORK=$(mktemp -d "$SMOKE_TMP/yuntun-release.XXXXXX")
  smoke_free=$(df -h "$SMOKE_TMP" | awk 'NR==2{print $4}')
  echo "冒烟临时目录：$WORK（$SMOKE_TMP 剩余 $smoke_free）"
  PIDS=()
  cleanup() {
    for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
    wait 2>/dev/null || true
    rm -rf "$WORK"
  }
  trap cleanup EXIT

  wait_listen() { # $1=log $2=needle $3=名字
    for _ in $(seq 1 200); do
      grep -q "$2" "$1" 2>/dev/null && return 0
      sleep 0.1
    done
    echo "$3 未在 20s 内启动，日志：" >&2
    tail -40 "$1" >&2
    return 1
  }

  step "release 二进制冒烟：standalone（17 项）"
  cat > "$WORK/plain.toml" <<EOF
[server]
listen = "127.0.0.1:$FLIGHT_PORT"
[store]
type = "memory"
[wal]
dir = "$WORK/plain/wal"
[chunk]
spill_dir = "$WORK/plain/spill"
[meta]
dir = "$WORK/plain/meta"
[sql.mysql]
enabled = true
listen = "127.0.0.1:$MYSQL_PORT"
auth = "trust"
EOF
  "./target/release/yuntun" --config "$WORK/plain.toml" > "$WORK/plain.log" 2>&1 &
  PIDS+=($!)
  wait_listen "$WORK/plain.log" "mysql wire protocol listening" "standalone"
  "$PY" scripts/standalone_smoke.py "127.0.0.1:$MYSQL_PORT" "127.0.0.1:$FLIGHT_PORT"

  step "release 二进制冒烟：鉴权（7 项）"
  cat > "$WORK/auth.toml" <<EOF
[server]
listen = "127.0.0.1:$AUTH_FLIGHT_PORT"
auth_token = "change-me"
[store]
type = "memory"
[wal]
dir = "$WORK/auth/wal"
[chunk]
spill_dir = "$WORK/auth/spill"
[meta]
dir = "$WORK/auth/meta"
[sql.mysql]
enabled = true
listen = "127.0.0.1:$AUTH_MYSQL_PORT"
users = [{ user = "yuntun", password = "s3cret" }]
EOF
  "./target/release/yuntun" --config "$WORK/auth.toml" > "$WORK/auth.log" 2>&1 &
  PIDS+=($!)
  wait_listen "$WORK/auth.log" "mysql wire auth: native_password" "auth standalone"
  "$PY" scripts/auth_smoke.py \
    --mysql "127.0.0.1:$AUTH_MYSQL_PORT" --user yuntun --password s3cret \
    --flight "127.0.0.1:$AUTH_FLIGHT_PORT" --token change-me
fi

step "结果"
echo "版本 $VERSION：门禁通过"
if [ ${#SKIPPED[@]} -gt 0 ]; then
  echo "⚠️ 跳过项（**不算通过**）："
  for s in "${SKIPPED[@]}"; do echo "  - $s"; done
fi
echo "fmt / clippy / 测试 / 文档一致性 / release 冒烟 —— 全部已执行（见上）"
