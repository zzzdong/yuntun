#!/usr/bin/env bash
# 产出可分发的 release 包（0.1）：4 个二进制 + 示例配置 + 文档 + 许可证 + sha256。
#
# 用法：scripts/package_release.sh [--no-build]
# 产物：dist/yuntun-<version>-<target>.tar.gz(.sha256)
#
# 为什么不把 docs/ 全打进去：用户只需要"怎么跑"和"能力边界"；
# 完整设计/台账留在仓库（发布包里塞 20 个文档只会让人不知道先读哪个）。
set -euo pipefail

cd "$(dirname "$0")/.."

BUILD=1
for a in "$@"; do
  case "$a" in
    --no-build) BUILD=0 ;;
    *) echo "未知参数：$a" >&2; exit 2 ;;
  esac
done

VERSION=$(awk '/^\[workspace.package\]/{f=1} f&&/^version = /{gsub(/"/,"",$3); print $3; exit}' Cargo.toml)
TARGET=$(rustc -vV | awk '/^host:/{print $2}')
[ -n "$VERSION" ] && [ -n "$TARGET" ] || { echo "读不到版本/target" >&2; exit 1; }

NAME="yuntun-${VERSION}-${TARGET}"
DIST="dist"
STAGE="$DIST/$NAME"

if [ "$BUILD" -eq 1 ]; then
  echo "== cargo build --release --bins =="
  cargo build --release --bins
fi

for b in yuntun yuntun-cli yuntun-datanode metanode; do
  test -x "target/release/$b" || { echo "缺少 target/release/$b（先构建）" >&2; exit 1; }
done

rm -rf "$STAGE"
mkdir -p "$STAGE/bin" "$STAGE/docs"
for b in yuntun yuntun-cli yuntun-datanode metanode; do
  cp "target/release/$b" "$STAGE/bin/"
done
# 包里的二进制去掉**调试信息**（target/release 那份保留 line-tables-only，便于本地排障）：
# 完整发布树 998 MB/个 → 约 229 MB；去符号（--strip-all）会更小，但崩溃回溯就只剩地址。
strip --strip-debug "$STAGE"/bin/* 2>/dev/null || echo "（strip 不可用，保留原样）"
cp yuntun.toml.example README.md CHANGELOG.md LICENSE-MIT LICENSE-APACHE "$STAGE/"
cp docs/OVERVIEW.md docs/ARCHITECTURE.md "$STAGE/docs/"

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
