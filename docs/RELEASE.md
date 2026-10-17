# 发布流程（0.1）

> **口径**：版本号只有一个来源 —— `Cargo.toml` 的 `[workspace.package].version`；
> 所有 crate 用 `version.workspace = true`。tag 与它不一致时发布流水线**直接失败**。
>
> **原则**：发布 = 一条命令跑完门禁 + 推一个 tag；产物由 CI 构建，不靠"我记得跑过"。

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

CI（`.github/workflows/ci.yml`）在每次 push / PR 上跑 1、2 与 MSRV；tag 流水线**不重复**
跑全量测试（时间 × 4 个目标）——所以**打 tag 前必须先在本地或 main 的 CI 上过第 1–6 项**。

已知债（**不是门禁**，但别装作没有）：

- `cargo fmt --all --check` 有约 884 处差异 —— 本仓从未以 rustfmt 为基线。
  若要对齐，必须**单独一笔**纯格式化提交，并与发布提交分开。

## 2. 产物（CI 自动构建）

`.github/workflows/release.yml` 在 `v*` tag 上构建 **4 个目标**并发布到 GitHub Release：

| 目标 | 方式 | 说明 |
|---|---|---|
| `x86_64-unknown-linux-gnu` | 原生 | 绝大多数 Linux 服务器 |
| `x86_64-unknown-linux-musl` | 原生 + `musl-tools` | 静态链接，无 glibc 依赖 |
| `aarch64-unknown-linux-gnu` | `cross`（Docker） | ARM64 服务器 / 云主机 |
| `aarch64-unknown-linux-musl` | `cross`（Docker） | ARM64 静态 |

每个包：`bin/{yuntun,yuntun-cli,yuntun-datanode,metanode}`、`yuntun.toml.example`、
`README.md`、`CHANGELOG.md`、两个 LICENSE、`docs/OVERVIEW.md` + `docs/dev/ARCHITECTURE.md`、
`<pkg>.sha256`；Release 页另附合并的 `SHA256SUMS` 与 CHANGELOG 当前版本段的说明。

本地产出同样的包（用于离线/预演）：

```bash
scripts/package_release.sh --target x86_64-unknown-linux-gnu
# 或只打包已构建的产物：
scripts/package_release.sh --no-build --bin-dir target/x86_64-unknown-linux-gnu/release
```

包内二进制去掉调试信息（`CARGO_PROFILE_RELEASE_DEBUG=0` 或 `strip --strip-debug`）；
本机 `target/release` 仍保留 line-tables-only，便于排障。

## 3. 打 tag（发布动作本身）

```bash
git tag -a v0.1.0 -m "yuntun 0.1.0: standalone analytics lakehouse" && git push --tags
```

推 tag 后：build（4 目标）→ 打包 → GitHub Release（说明取 `CHANGELOG.md` 的当前版本段，
**Known limitations 原样带上**）。手动触发（`workflow_dispatch`）只构建、不发布。

## 4. 发布后验收

- 从 Release 下载**至少 gnu 与 musl 各一个**包，`sha256sum -c` 通过；
- 解包后的 `bin/yuntun --version` 与 tag 一致；
- 用包里的二进制起实例，跑 `scripts/standalone_smoke.py`（**17/17**）；
- musl 包额外确认 `ldd bin/yuntun` 不是动态链接 glibc（`not a dynamic executable`）。

## 5. 记录（模板）

| 版本 | 日期 | commit | 四个目标 | standalone 冒烟 | auth 冒烟 | 备注 |
|---|---|---|---|---|---|---|
| 0.1.0 | 2026-10-17 | （填 `git rev-parse --short HEAD`） | x64/aarch64 × gnu/musl | 17/17（本机） | 7/7（本机） | 首个对外版本 |
