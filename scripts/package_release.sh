#!/usr/bin/env bash
# 产出可分发的 release 包（0.1）：4 个二进制 + 示例配置 + 文档 + 许可证 + sha256。
#
# 用法：scripts/package_release.sh [--no-build] [--target <triple>] [--bin-dir <dir>]
# 产物：dist/yuntun-<version>-<target>.tar.gz(.sha256)
#
# `--target` 用于 CI 的交叉/多目标构建（二进制在 target/<triple>/release）；
# `--bin-dir` 用于"已经构建好、只打包"的场景（CI 里 cross 的产物不总在同一处）。
#
# 为什么不把 docs/ 全打进去：用户只需要"怎么跑"和"能力边界"；
# 完整设计/台账留在仓库（发布包里塞 20 个文档只会让人不知道先读哪个）。
set -euo pipefail

cd "$(dirname "$0")/.."

BUILD=1
TARGET=""
BIN_DIR=""
while [ $# -gt 0 ]; do
  case "$1" in
    --no-build) BUILD=0; shift ;;
    --target) TARGET=${2:-}; shift 2 ;;
    --bin-dir) BIN_DIR=${2:-}; shift 2 ;;
    *) echo "未知参数：$1" >&2; exit 2 ;;
  esac
done

HOST=$(rustc -vV | awk '/^host:/{print $2}')
VERSION=$(awk '/^\[workspace.package\]/{f=1} f&&/^version = /{gsub(/"/,"",$3); print $3; exit}' Cargo.toml)
TARGET=${TARGET:-$HOST}
[ -n "$VERSION" ] && [ -n "$TARGET" ] || { echo "读不到版本/target" >&2; exit 1; }
if [ -z "$BIN_DIR" ]; then
  if [ "$TARGET" = "$HOST" ]; then BIN_DIR="target/release"; else BIN_DIR="target/$TARGET/release"; fi
fi

NAME="yuntun-${VERSION}-${TARGET}"
DIST="dist"
STAGE="$DIST/$NAME"

if [ "$BUILD" -eq 1 ]; then
  echo "== cargo build --release --bins --target $TARGET =="
  cargo build --release --bins --target "$TARGET"
fi

for b in yuntun yuntun-cli yuntun-datanode metanode; do
  test -x "$BIN_DIR/$b" || { echo "缺少 $BIN_DIR/$b（先构建，或用 --bin-dir）" >&2; exit 1; }
done

rm -rf "$STAGE"
mkdir -p "$STAGE/bin" "$STAGE/docs/dev"
for b in yuntun yuntun-cli yuntun-datanode metanode; do
  cp "$BIN_DIR/$b" "$STAGE/bin/"
done
# 包里的二进制去掉**调试信息**（本机 target/release 那份保留 line-tables-only，便于排障）：
# 完整发布树 998 MB/个 → 约 229 MB；去符号（--strip-all）会更小，但崩溃回溯就只剩地址。
# 交叉编译时宿主 strip 不认别的架构 —— 交给 CI 的 `strip: true` / `CARGO_PROFILE_RELEASE_DEBUG=0`。
if [ "$TARGET" = "$HOST" ] && command -v strip >/dev/null 2>&1; then
  strip --strip-debug "$STAGE"/bin/* || echo "（strip 失败，保留原样）"
else
  echo "（跳过 strip：target=$TARGET host=$HOST；CI 用 CARGO_PROFILE_RELEASE_DEBUG=0 构建瘦身）"
fi
cp yuntun.toml.example README.md CHANGELOG.md LICENSE-MIT LICENSE-APACHE "$STAGE/"
cp docs/OVERVIEW.md "$STAGE/docs/"
cp docs/dev/ARCHITECTURE.md "$STAGE/docs/dev/"

# 可复现性：包里不留本机绝对路径；mtime 归一化（可复现不是本版承诺，但顺手做掉）
find "$STAGE" -exec touch -d "2026-10-17T00:00:00Z" {} +
TARBALL="$DIST/$NAME.tar.gz"
tar -C "$DIST" --sort=name --owner=0 --group=0 --numeric-owner -czf "$TARBALL" "$NAME"
( cd "$DIST" && sha256sum "$NAME.tar.gz" > "$NAME.tar.gz.sha256" )

echo
echo "== 产物 =="
ls -lh "$TARBALL" "$TARBALL.sha256"
echo
echo "== 包内 =="
tar tzf "$TARBALL"
