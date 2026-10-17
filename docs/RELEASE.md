# 发布流程（0.1.0）

> **口径**：版本号只有一个来源 —— `Cargo.toml` 的 `[workspace.package].version`；
> 所有 crate 用 `version.workspace = true`。0.1 之前本仓没有对外产物，也从未打过 tag。
>
> **原则**：发布 = 一条命令跑完门禁 + 一条命令产出带校验和的包；不靠"我记得跑过"。

---

## 1. 预检门禁（必须全绿）

| # | 命令 | 期望 |
|---|---|---|
| 1 | `cargo clippy --workspace --all-targets -- -D warnings` | 0 告警 |
| 2 | `cargo test --workspace` | 全绿、0 ignored |
| 3 | `cargo test -p yuntun-testkit --test docs_consistency` | 10/10（已含在 2 里，单跑用于定位） |
| 4 | `cargo build --release --bins` | 产出 `yuntun` / `yuntun-cli` / `yuntun-datanode` / `metanode` |
| 5 | `./target/release/yuntun --version` | `yuntun 0.1.0`（与 workspace 版本一致） |
| 6 | release 二进制冒烟 | `standalone_smoke.py` **17/17**、`auth_smoke.py` **7/7** |
| 7 | `rustup run 1.95.0 cargo check --workspace --all-targets` | EXIT=0（MSRV，`§188`） |

一条命令：

```bash
scripts/release_check.sh              # 含 release 冒烟（需要 python + pymysql/adbc）
scripts/release_check.sh --skip-smoke # 只跑编译/测试/文档
```

已知债（**不是本版门禁**，但别装作没有）：

- `cargo fmt --all --check` 有约 884 处差异 —— 本仓从未以 rustfmt 为基线。
  若要对齐，必须**单独一笔**纯格式化提交（否则会把功能 diff 埋掉），并与发布提交分开。

## 2. 产物

```bash
scripts/package_release.sh
```

产出：

```
dist/yuntun-0.1.0-<target>.tar.gz
dist/yuntun-0.1.0-<target>.tar.gz.sha256
```

包内：`bin/{yuntun,yuntun-cli,yuntun-datanode,metanode}`、`yuntun.toml.example`、
`README.md`、`CHANGELOG.md`、`LICENSE-MIT`、`LICENSE-APACHE`、`docs/OVERVIEW.md`、
`docs/ARCHITECTURE.md`。

验收（换一个目录、模拟"用户拿到包"）：

```bash
tar xzf yuntun-0.1.0-*.tar.gz && cd yuntun-0.1.0-*
./bin/yuntun --version
cp yuntun.toml.example yuntun.toml   # 改端口/目录后启动
scripts/.venv/bin/python ../../scripts/standalone_smoke.py 127.0.0.1:3306 127.0.0.1:50051
```

## 3. 打 tag 与发布（人工一步）

```bash
git tag -a v0.1.0 -m "yuntun 0.1.0: standalone analytics lakehouse" && git push --tags
# 在托管平台创建 Release，附上 dist/ 下两个文件
```

发布说明直接用 `CHANGELOG.md` 的 0.1.0 段（**Known limitations 必须原样带上**）。

## 4. 发布后

- 用下载回来的包跑一次第 2 节的"验收"（证明产物本身可用，而不只是本机构建）；
- 记录 `sha256` 到发布页；
- 若发现打包/文档问题：**不移动、不删除 tag**，补 `0.1.1`（二进制可回退：0.1 没有
  数据格式迁移，WAL/清单格式未变）。

## 5. 发布记录（模板）

| 版本 | 日期 | commit | 校验和（sha256） | standalone 冒烟 | auth 冒烟 | 备注 |
|---|---|---|---|---|---|---|
| 0.1.0 | 2026-10-17 | （填 `git rev-parse --short HEAD`） | （填） | 17/17 | 7/7 | 首个对外版本 |
