# 发布流程（0.1）

> **口径**：版本号只有一个来源 —— `Cargo.toml` 的 `[workspace.package].version`；
> 所有 crate 用 `version.workspace = true`。tag 与它不一致时发布流水线**直接失败**。
>
> **原则**：发布 = 一条命令跑完门禁 + 推一个 tag；产物由 CI 构建，不靠"我记得跑过"。

---

## 1. 预检门禁（必须全绿）

| # | 命令 | 期望 |
|---|---|---|
| 0 | `cargo fmt --all -- --check` | 无差异（0.1 起是门禁；历史 884 处差异已一次性基线化） |
| 1 | `cargo clippy --workspace --all-targets -- -D warnings` | 0 告警 |
| 2 | `cargo test --workspace` | 全绿、0 ignored |
| 3 | `cargo test -p yuntun-testkit --test docs_consistency` | 10/10（已含在 2 里，单跑用于定位） |
| 4 | `cargo build --release --bins` | 产出 `yuntun` / `yuntun-cli` / `yuntun-datanode` / `metanode` |
| 5 | `./target/release/yuntun --version` | 与 workspace 版本一致（如 `yuntun 0.1.0-alpha.1`） |
| 6 | release 二进制冒烟 | `standalone_smoke.py` **17/17**、`auth_smoke.py` **7/7** |
| 7 | `rustup run 1.95.0 cargo check --workspace --all-targets` | EXIT=0（MSRV，`§188`） |

一条命令：

```bash
scripts/release_check.sh              # 含 release 冒烟（需要 python + pymysql/adbc）
scripts/release_check.sh --skip-smoke # 只跑编译/测试/文档
```

CI（`.github/workflows/ci.yml`）在每次 push / PR 上跑 1、2 与 MSRV；tag 流水线**不重复**
跑全量测试（时间 × 4 个目标）——所以**打 tag 前必须先在本地或 main 的 CI 上过第 1–6 项**。

`scripts/release_check.sh` 目前覆盖 1–6；MSRV 由 CI 的独立 job 覆盖。
格式化是**门禁**：0.1 之前全仓从未跑过 rustfmt，已在发布前一次性基线化
（单独一笔纯格式化提交，避免埋掉功能 diff）。

### 1.1 磁盘与临时目录（踩过的坑）

- 全量 debug 测试树（`debug = "line-tables-only"`）约 **45 GiB**；发布包约 133 MiB。
  可用空间 < 10 GiB 时**先 `cargo clean`**（或把 `CARGO_TARGET_DIR` 指到更大的盘）——
  否则链接阶段会以 `lld` 崩溃的形态失败（本项目实测：盘 98% 时链接报
  "PLEASE submit a bug report"，看起来像编译器 bug，实际是没空间）。
- 冒烟与测试的临时目录放 **`/tmp`（tmpfs）**：独立于家目录所在盘、更快，也不留垃圾。
  `release_check.sh` 显式用 `mktemp -d /tmp/...`（可用 `YUNTUN_SMOKE_TMPDIR` 覆盖）；
  testkit 本身也优先选 tmpfs（`/tmp` → `/dev/shm`），全不是 tmpfs 才回退系统临时目录。
- CI 用 `CARGO_PROFILE_DEV_DEBUG=0` + `CARGO_INCREMENTAL=0`：runner 盘只有十几 GiB。

## 2. 预发布（alpha / beta / rc）

版本号用 SemVer 预发布段，tag 与 `Cargo.toml` 必须逐字一致：

```toml
[workspace.package]
version = "0.1.0-alpha.1"
```

```bash
git tag -a v0.1.0-alpha.1 -m "yuntun 0.1.0-alpha.1 (pipeline test)" && git push origin v0.1.0-alpha.1
```

tag 里含 `-` ⇒ Release 自动标成 **prerelease**（不会被当成稳定版）。预发布的目的
是验证四目标构建与安装包；验证通过后把版本改成 `0.1.0`、在 `CHANGELOG` 增加
`[0.1.0]` 段，再打 `v0.1.0`（`gh release` 里预发布与稳定版并存，不互相覆盖）。

## 3. 产物（CI 自动构建）

`.github/workflows/release.yml` 在 `v*` tag 上构建 **4 个目标**并发布到 GitHub Release：

| 目标 | runner | 工具链 | 说明 |
|---|---|---|---|
| `x86_64-unknown-linux-gnu` | `ubuntu-24.04` | 系统 cc | 绝大多数 Linux 服务器 |
| `aarch64-unknown-linux-gnu` | **`ubuntu-24.04-arm`** | 系统 cc + 手装 `clang/libclang/g++` | ARM64 服务器 / 云主机（原生编译，不用 cross） |
| `x86_64-unknown-linux-musl` | `ubuntu-24.04` | [musl.cc] `x86_64-linux-musl-cross` | 静态链接，无 glibc 依赖 |
| `aarch64-unknown-linux-musl` | `ubuntu-24.04` | [musl.cc] `aarch64-linux-musl-cross` | ARM64 静态 |

[musl.cc]: https://musl.cc/

两个硬前提（都踩过）：

- **libclang**：vortex → `custom-labels` 的 build.rs 用 bindgen 生成绑定，缺 libclang 会
  报 "Unable to generate bindings"；所有 job 都装 `clang libclang-dev llvm-dev`。
- **C++ 编译器**：同一个 build.rs 还要编 C++17。`musl-tools` 只有 `musl-gcc`，
  会报 `musl-g++ not found` ⇒ musl 用 musl.cc 的交叉工具链（自带 g++），并把
  `CC_/CXX_/AR_<target>` 与 `CARGO_TARGET_<TARGET>_LINKER` 指过去。

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

## 4. 打 tag（发布动作本身）

```bash
# 预发布（当前）
git tag -a v0.1.0-alpha.1 -m "yuntun 0.1.0-alpha.1 (pipeline test)" && git push origin v0.1.0-alpha.1
# 稳定版（预发布验证通过后）
git tag -a v0.1.0 -m "yuntun 0.1.0: standalone analytics lakehouse" && git push origin v0.1.0
```

推 tag 后：build（4 目标）→ 打包 → GitHub Release（说明取 `CHANGELOG.md` 的当前版本段，
**Known limitations 原样带上**）。手动触发（`workflow_dispatch`）只构建、不发布。

## 5. 发布后验收

- 从 Release 下载**至少 gnu 与 musl 各一个**包，`sha256sum -c` 通过；
- 解包后的 `bin/yuntun --version` 与 tag 一致；
- 用包里的二进制起实例，跑 `scripts/standalone_smoke.py`（**17/17**）；
- musl 包额外确认 `ldd bin/yuntun` 不是动态链接 glibc（`not a dynamic executable`）。

## 6. 记录（模板）

| 版本 | 日期 | commit | 四个目标 | standalone 冒烟 | auth 冒烟 | 备注 |
|---|---|---|---|---|---|---|
| 0.1.0-alpha.1 | 2026-10-17 | 8073410 | ❌ 3/4（缺 libclang / musl-g++） | 17/17（本机） | 7/7（本机） | 首次流水线，暴露构建前置 |
| 0.1.0-alpha.2 | 2026-10-18 | （填） | 待流水线 | 17/17（本机） | 7/7（本机） | ARM 原生 + musl.cc 工具链 |
| 0.1.0 | 待定 | — | 待定 | — | — | alpha 验证通过后发布 |
