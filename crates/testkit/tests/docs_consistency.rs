//! **文档一致性判据**（归档 `§123` 立，`§170` 重建时按新文档结构重写）。
//!
//! # 为什么需要它
//!
//! `docs/README.md` 定了纪律（改代码必须同步文档），但它**不是自执行的**。实测漂移过：
//! 归档日志的头部索引停在 §32 而正文已到 §122、`plan.md` 头部 v2.2 落款 v2.1、
//! `meta.proto` 的迁移进度表把已迁移的 op 标成 ⏳、`status.md` 的规模行落后于代码、
//! 以及**整组任务记分卡停在起点**（归档 `D-24`）。这些**都不是"读一遍能发现"的**。
//!
//! 2026-10-11 文档整体归档重建（`§170`）之后：
//! - 顶层只有 6 份（`README` / `OVERVIEW` / `ARCHITECTURE` / `ROADMAP` / `HANDOVER` / `operation-log`）；
//! - 旧文档（含 §1…168 的日志）在 `docs/archive/`，**冻结不再维护**；
//! - 本文件只钉**可机械核对**的那几项，且每条失败时的输出必须**直接告诉你改哪里**。
//!
//! # 有一条判据**故意**会在每次改代码之后变红
//!
//! [`overview_scale_line_matches_the_code`] 依赖"代码行数 / crate 数 / 测试函数数"。你只要动代码，
//! 它就会红 —— 直到你把 `docs/OVERVIEW.md` 的规模行更新成它报出来的数字。这是**有意**的。
//!
//! # 只放"确定不会误报"的判断
//!
//! 宁可少放几条，也不放会偶发红的。**写判据的教训**（归档 `§123`，三次栽在同一处）：
//! 判据只能依赖**格式明确的声明行 / 表格行**，**不能**依赖"文件里出现过某字符串" ——
//! 否则文档里解释"这条判据曾经误报过"的散文会把自己弄红。

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// 顶层文档集合（`§170` 之后就是这个集合；多一份 = 又开了一份会漂移的平行文档）。
const TOP_LEVEL_DOCS: [&str; 6] = [
    "README.md",
    "OVERVIEW.md",
    "ARCHITECTURE.md",
    "ROADMAP.md",
    "HANDOVER.md",
    "operation-log.md",
];

// ---------------------------------------------------------------- 基础设施

/// 仓库根 = 同时含 `docs/OVERVIEW.md` 与 `crates/` 的那一级。
fn repo_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        if p.join("docs/OVERVIEW.md").is_file() && p.join("crates").is_dir() {
            return p;
        }
        assert!(
            p.pop(),
            "从 {} 向上没找到含 docs/OVERVIEW.md 与 crates/ 的仓库根",
            env!("CARGO_MANIFEST_DIR")
        );
    }
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("读 {} 失败：{e}", path.display()))
}

fn docs_file(root: &Path, name: &str) -> String {
    read(&root.join("docs").join(name))
}

/// 顶层 `docs/*.md` + 根 `README.md`（"范围串"这类判据要跨文档扫）。
///
/// ⚠️ **不含 `docs/archive/`**：归档是历史，里面的旧串"当时是对的"，不是导航。
fn all_docs(root: &Path) -> Vec<(PathBuf, String)> {
    let mut out: Vec<(PathBuf, String)> = Vec::new();
    let dir = root.join("docs");
    for e in std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("读 {} 失败：{e}", dir.display())) {
        let p = e.expect("目录项").path();
        if p.extension().is_some_and(|x| x == "md") {
            out.push((p.clone(), read(&p)));
        }
    }
    let readme = root.join("README.md");
    if readme.is_file() {
        out.push((readme.clone(), read(&readme)));
    }
    out
}

/// 一行是不是章节标题（`## 122. 标题`）—— 三级的 `### 122.1` 不算。
fn section_no(line: &str) -> Option<u64> {
    let rest = line.strip_prefix("## ")?;
    let (n, _) = rest.split_once('.')?;
    n.trim().parse().ok()
}

/// 正文里最大的章节号。
fn max_section_no(log: &str) -> u64 {
    log.lines()
        .filter_map(section_no)
        .max()
        .expect("operation-log 里至少应有一节")
}

/// `（YYYY-MM-DD）` 里的日期（章节标题末尾那句）。
fn paren_date(line: &str) -> Option<String> {
    let start = line.rfind('（')? + '（'.len_utf8();
    let rest = &line[start..];
    let end = rest.find('）')?;
    let d = &rest[..end];
    let ok = d.len() == 10
        && d.chars().enumerate().all(|(i, c)| match i {
            4 | 7 => c == '-',
            _ => c.is_ascii_digit(),
        });
    ok.then(|| d.to_string())
}

/// 反引号里的名字（`` `EvolveSchema` `` → `EvolveSchema`）。
fn backticked(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = line;
    while let Some(i) = rest.find('`') {
        let after = &rest[i + 1..];
        let Some(j) = after.find('`') else { break };
        out.push(after[..j].to_string());
        rest = &after[j + 1..];
    }
    out
}

/// 某份日志的"头部索引 vs 正文最后一节"核对（新日志与归档日志共用）。
///
/// 归档日志的判定逻辑**保持不变** —— 它冻结了，但它当年漂过 90 节，这条判据仍要守着，
/// 免得后来者"顺手改归档"把历史改成自洽的样子。
fn check_log_index(root: &Path, name: &str) {
    let log = docs_file(root, name);
    let max = max_section_no(&log);

    let head = log
        .lines()
        .take(12)
        .find(|l| l.contains("最新：**§"))
        .unwrap_or_else(|| {
            panic!(
                "`docs/{name}` 头部没有 `最新：**§N，YYYY-MM-DD**` 这一行 —— \
                 没有它索引就无法核对，请补回"
            )
        })
        .to_string();

    let n: u64 = head
        .split("最新：**§")
        .nth(1)
        .and_then(|s| s.split('，').next())
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or_else(|| panic!("解析不了头部索引（期望 `最新：**§N，YYYY-MM-DD**`）：{head:?}"));

    let last = log
        .lines()
        .rfind(|l| section_no(l).is_some())
        .expect("至少一节");
    let date = paren_date(last);

    if n != max {
        panic!(
            "`docs/{name}` 的头部索引停在 §{n}，而正文已到 §{max}\n\
             → 把头部那行改成：最新：**§{max}，{}**",
            date.clone().unwrap_or_else(|| "<最后一节的日期>".into())
        );
    }
    if let Some(d) = date
        && !head.contains(&d)
    {
        panic!("`docs/{name}` 头部索引的日期不是最后一节的日期\n→ 头部应写 `…§{max}，{d}…`，实际是：{head}");
    }
}

/// 实况：`(Rust 行数, crate 数, 测试函数数)`。
fn measure(root: &Path) -> (u64, usize, usize) {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for e in std::fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("读 {} 失败：{e}", dir.display()))
            .flatten()
        {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }
    let crates_dir = root.join("crates");
    let mut files = Vec::new();
    walk(&crates_dir, &mut files);

    let mut lines = 0u64;
    let mut tests = 0usize;
    for f in &files {
        let s = read(f);
        lines += s.matches('\n').count() as u64;
        tests += s
            .lines()
            .filter(|l| l.contains("#[test]") || l.contains("#[tokio::test"))
            .count();
    }
    let crates = std::fs::read_dir(&crates_dir)
        .unwrap_or_else(|e| panic!("读 {} 失败：{e}", crates_dir.display()))
        .flatten()
        .filter(|e| e.path().join("Cargo.toml").is_file())
        .count();
    (lines, crates, tests)
}

/// 收集文本里所有 `D-<digits>` 的编号（代码注释与台账都用这个形式互相引用）。
fn ledger_ids(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while let Some(pos) = text[i..].find("D-") {
        let start = i + pos + 2;
        let digits: String = bytes[start..]
            .iter()
            .take_while(|b| b.is_ascii_digit())
            .map(|b| *b as char)
            .collect();
        if !digits.is_empty() {
            out.insert(format!("D-{digits}"));
        }
        i = start + digits.len().max(1);
        if i >= text.len() {
            break;
        }
    }
    out
}

// ---------------------------------------------------------------- 判据

/// **新日志**（`docs/operation-log.md`）的头部索引必须等于正文最后一节（号与日期都算）。
#[test]
fn new_operation_log_header_index_matches_last_section() {
    check_log_index(&repo_root(), "operation-log.md");
}

/// **归档日志**（`docs/archive/operation-log.md`）的头部索引必须等于它自己正文的最后一节。
///
/// 归档冻结之后这条判据的作用是"防止有人顺手改归档"：真改了正文而不改头部，它会红。
#[test]
fn archive_operation_log_header_index_matches_its_last_section() {
    let root = repo_root();
    check_log_index(&root, "archive/operation-log.md");
}

/// 顶层文档里的 `§1–§N` 范围串，`N` 必须等于**归档日志**的最后一节（§1…168 是归档的区间）。
///
/// 新文档一律用 `§1…168` 这种写法（区间串只属于归档）；本判据拦的是"有人又写回 `§1–§N` 且数字过期"。
#[test]
fn section_ranges_in_docs_match_the_archive() {
    let root = repo_root();
    let max = max_section_no(&docs_file(&root, "archive/operation-log.md"));
    let mut bad: Vec<String> = Vec::new();

    for (path, text) in all_docs(&root) {
        if path.file_name().is_some_and(|n| n == "operation-log.md") {
            // 新日志自身会引用归档节号，情形与其它文档一致，但它的"最新"由上面那条判据守；
            // 这里跳过是为了不与 `§169 起`这类续号说法打架。
            continue;
        }
        for (i, line) in text.lines().enumerate() {
            let mut rest = line;
            while let Some(pos) = rest.find("§1–§") {
                let after = &rest[pos + "§1–§".len()..];
                let digits: String = after.chars().take_while(char::is_ascii_digit).collect();
                if let Ok(n) = digits.parse::<u64>()
                    && n != max
                {
                    bad.push(format!("{}:{} 写的是 §1–§{n}", path.display(), i + 1));
                }
                rest = &after[digits.len()..];
            }
        }
    }
    assert!(
        bad.is_empty(),
        "这些地方的范围串与归档日志不符（归档最后一节是 §{max}）：\n  {}\n\
         → 要么改成 `§1…{max}`（区间串只属于归档），要么删掉区间只留节号",
        bad.join("\n  ")
    );
}

/// `docs/OVERVIEW.md` 的规模行必须与代码实况一致：行数 / crate 数 / 测试函数数。
///
/// 量法与文档里写的命令一致（`find crates -name '*.rs' | xargs wc -l`；数 `#[test]` / `#[tokio::test`）。
/// ⚠️ 改代码后这条**会红**，那是设计：照着它报出来的数字更新 `docs/OVERVIEW.md`。
#[test]
fn overview_scale_line_matches_the_code() {
    let root = repo_root();
    let overview = docs_file(&root, "OVERVIEW.md");

    let (lines, crates, tests) = measure(&root);
    let line = overview
        .lines()
        .find(|l| l.starts_with("规模：**"))
        .unwrap_or_else(|| panic!("`docs/OVERVIEW.md` 里找不到以 `规模：**` 开头的那行"));
    let reported: Vec<String> = line
        .split(|c: char| !c.is_ascii_digit() && c != ',')
        .filter(|s| !s.is_empty())
        .map(|s| s.replace(',', ""))
        .collect();
    assert!(
        reported.len() >= 3,
        "规模行的格式变了（期望 `规模：**N 行 Rust / M 个 crate / K 个测试函数 …`）：{line}"
    );
    let want = [
        (lines.to_string(), "行 Rust"),
        (crates.to_string(), "个 crate"),
        (tests.to_string(), "个测试函数"),
    ];
    for (i, (w, what)) in want.iter().enumerate() {
        if reported[i] != *w {
            panic!(
                "`docs/OVERVIEW.md` 的规模行与代码不符：它写 `{what}` = {}，实测 {w}\n\
                 → 把那一行改成：规模：**{lines} 行 Rust / {crates} 个 crate / {tests} 个测试函数 / …**\n\
                 （用例数与 clippy 警告数不在本条判据里：它们要跑一次全量才知道）",
                reported[i]
            );
        }
    }
}

/// 顶层文档集合必须**正好**是 `docs/README.md §1` 与 `§170` 约定的那 6 份。
///
/// 这条守的是本仓的硬规则："就地升版本，不新建平行文档"。多出来的每一份都是将来会漂移的地方；
/// 真需要新维度时，改 [`TOP_LEVEL_DOCS`] 并在 `docs/README.md §1` 的表里登记它。
#[test]
fn docs_top_level_set_is_the_documented_one() {
    let root = repo_root();
    let dir = root.join("docs");
    let mut found: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("读 {} 失败：{e}", dir.display()))
        .flatten()
        .filter_map(|e| {
            let p = e.path();
            (p.is_file() && p.extension().is_some_and(|x| x == "md"))
                .then(|| p.file_name().unwrap().to_string_lossy().into_owned())
        })
        .collect();
    found.sort();
    let mut want: Vec<String> = TOP_LEVEL_DOCS.iter().map(|s| s.to_string()).collect();
    want.sort();
    assert_eq!(
        found, want,
        "`docs/` 顶层文档集合与约定不符\n\
         → 期望 {:?}\n→ 实际 {:?}\n\
         （新维度要么并进这四份，要么就先改本判据与 `docs/README.md §1`；\
         历史文档放 `docs/archive/`，那里不受本判据约束）",
        want, found
    );

    // 归档必须还在（新文档里的 §号与台账靠它解释）。
    for need in ["operation-log.md", "closeout.md", "architecture.md", "design.md"] {
        let p = root.join("docs/archive").join(need);
        assert!(
            p.is_file(),
            "归档缺了 `docs/archive/{need}` —— 它被新文档引用（§号映射 / 台账索引），不许删"
        );
    }
}

/// `meta.proto` 的"迁移进度"表必须诚实：
/// ① 每个已在 `oneof op` 里的 op，都要在表里被点名；
/// ② 表里标成 ⏳ 的 op，**不得**已经存在于 `oneof kind`。
///
/// 本条只依据**表格行**与**声明行**，不看散文 —— 它被自己的文案弄红过三次（归档 `§123`）。
#[test]
fn proto_migration_table_is_honest() {
    let root = repo_root();
    let proto = read(&root.join("crates/proto/proto/meta.proto"));

    let start = proto
        .lines()
        .position(|l| {
            let t = l.trim();
            !t.starts_with("//") && t.starts_with("oneof ") && t.ends_with('{')
        })
        .expect("`meta.proto` 里应有 `oneof … {` 的**声明行**（注释不算）");
    let oneof: String = proto
        .lines()
        .skip(start + 1)
        .take_while(|l| l.trim() != "}")
        .collect::<Vec<_>>()
        .join("\n");

    let mut ops: Vec<String> = Vec::new();
    for line in oneof.lines() {
        let t = line.trim();
        if t.is_empty() || t.starts_with("//") {
            continue;
        }
        let Some(ty) = t.split_whitespace().next() else {
            continue;
        };
        if let Some(name) = ty.strip_suffix("Op") {
            ops.push(name.to_string());
        }
    }
    assert!(!ops.is_empty(), "没从 `oneof op` 里解析出任何 op —— 格式变了？");

    let missing: Vec<&String> = ops
        .iter()
        .filter(|o| !proto.contains(&format!("`{o}`")))
        .collect();
    assert!(
        missing.is_empty(),
        "`meta.proto` 头部\"迁移进度\"表漏了这些 op：{missing:?}\n\
         → 加一行 `// | `X` | ✅ |`（新 op 必须同时进表）"
    );

    let mut lied: Vec<String> = Vec::new();
    for line in proto
        .lines()
        .filter(|l| l.trim_start().starts_with("// |") && l.contains("⏳"))
    {
        for name in backticked(line) {
            if ops.contains(&name) {
                lied.push(name);
            }
        }
    }
    assert!(
        lied.is_empty(),
        "`meta.proto` 的迁移进度表把 {lied:?} 标成 ⏳，但它们**已经**在 `oneof op` 里\n\
         → 把那一行改成 ✅（写清落在哪个字段）；表在骗人比没有表更糟"
    );
}

/// **ADR-10 的定案数值必须与配置默认值一致**（归档 `§161`；文档现在是 `ARCHITECTURE.md`）。
///
/// 为什么值得钉：ADR-10 的两个量（`max_flush_delay` / `phase_spread`）是**实测定案**的（T8 基线），
/// 而它们在代码里就是两个默认值。"改默认值必须附实测"这条纪律靠它变成**能跑红**的。
#[test]
fn adr10_decided_numbers_match_the_config_defaults() {
    let root = repo_root();
    let arch = docs_file(&root, "ARCHITECTURE.md");
    let cfg = read(&root.join("crates/server/src/config.rs"));

    let adr_md = arch
        .lines()
        .find(|l| l.contains("| `max_flush_delay` |"))
        .unwrap_or_else(|| {
            panic!(
                "ARCHITECTURE.md 里找不到 ADR-10 的 `max_flush_delay` 定案行 —— \
                 若改了表格格式，请同步这条判据"
            )
        });
    let adr_spread = arch
        .lines()
        .find(|l| l.contains("| `phase_spread` |"))
        .unwrap_or_else(|| panic!("ARCHITECTURE.md 里找不到 ADR-10 的 `phase_spread` 定案行"));

    let pick = |field: &str| -> String {
        let mut out = None;
        for (i, l) in cfg.lines().enumerate() {
            if l.trim_start().starts_with(&format!("pub {field}:")) {
                for l2 in cfg.lines().skip(i) {
                    let t = l2.trim();
                    if let Some(rest) = t.strip_prefix(&format!("{field}:")) {
                        out = Some(rest.trim().trim_end_matches(',').to_string());
                        break;
                    }
                }
                break;
            }
        }
        out.unwrap_or_else(|| panic!("config.rs 里找不到字段 {field} 的默认值"))
    };

    let bold = |line: &str| -> u64 {
        let cell = line
            .split('|')
            .find(|c| c.contains("**"))
            .unwrap_or_else(|| panic!("找不到加粗的定案格：{line}"));
        let after = cell.split("**").nth(1).unwrap_or_default();
        let digits: String = after.chars().take_while(|ch| ch.is_ascii_digit()).collect();
        digits
            .parse()
            .unwrap_or_else(|_| panic!("加粗格里不是数字：{cell}"))
    };

    let md = pick("max_flush_delay_secs").parse::<u64>().unwrap();
    let spread = pick("flush_phase_spread_secs").parse::<u64>().unwrap();
    assert_eq!(
        bold(adr_md),
        md,
        "ADR-10 的 `max_flush_delay` 定案值与配置默认值不一致：\n  ADR: {}  ({adr_md})",
        bold(adr_md)
    );
    assert_eq!(
        bold(adr_spread),
        spread,
        "ADR-10 的 `phase_spread` 定案值与配置默认值不一致：\n  ADR: {}  ({adr_spread})",
        bold(adr_spread)
    );
}

/// **`ARCHITECTURE.md §8` 的配置清单里的每个键，都必须在解析器里真的存在**（归档 `§161`）。
///
/// 为什么值得钉：配置清单曾经列着 `[gc]` / `multipart_threshold` / `idle_timeout` 这类**从未实现**
/// 的键 —— 照着设计文档写配置会**启动失败**，而"文档里的配置清单不能用"是自查最难发现的一类漂移。
#[test]
fn architecture_config_keys_exist_in_the_parser() {
    let root = repo_root();
    let arch = docs_file(&root, "ARCHITECTURE.md");
    let cfg = read(&root.join("crates/server/src/config.rs"));

    let start = arch
        .find("配置项清单")
        .expect("`ARCHITECTURE.md` 里找不到 `配置项清单` 这一节");
    let rest = &arch[start..];
    let block_start = rest.find("```toml").expect("配置清单里应有 toml 代码块") + "```toml".len();
    let block_end = rest[block_start..]
        .find("```")
        .expect("配置清单的 toml 代码块没有结束")
        + block_start;
    let block = &rest[block_start..block_end];

    let mut checked = 0usize;
    for (i, line) in block.lines().enumerate() {
        let t = line.trim();
        if t.is_empty() || t.starts_with('#') || t.starts_with('[') {
            continue;
        }
        let Some((key, _)) = t.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty() || !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            continue;
        }
        assert!(
            cfg.contains(key),
            "ARCHITECTURE.md §8 的配置清单（第 {i} 行起）写了配置键 `{key}`，但 \
             `crates/server/src/config.rs` 里根本没有它 —— 照着这份清单写配置会直接启动失败。\n\
             要么删掉这一行，要么先在 `config.rs` 里加字段。"
        );
        checked += 1;
    }
    assert!(
        checked >= 20,
        "配置清单里只解析出 {checked} 个键 —— 清单或被删空、或格式变了，请同步这条判据"
    );
}

/// **代码注释里引用的台账编号（`D-*`）必须能在 `ROADMAP.md` 里找到**。
///
/// 为什么值得钉：`docs/ROADMAP.md §4` 的台账是"实现与文档不符"的唯一登记处，而代码注释里
/// 直接引用着 `D-13` / `D-18` 这类编号。归档重建时最容易出的事故就是"新台账没抄全"
/// ⇒ 代码里那句注释变成**悬空引用**（下一位看不懂，只能去翻 10k 行的归档）。
#[test]
fn ledger_ids_referenced_from_code_exist_in_the_roadmap() {
    let root = repo_root();
    let roadmap = docs_file(&root, "ROADMAP.md");
    let known = ledger_ids(&roadmap);

    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for e in std::fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("读 {} 失败：{e}", dir.display()))
            .flatten()
        {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }
    let mut files = Vec::new();
    walk(&root.join("crates"), &mut files);

    let mut dangling: Vec<String> = Vec::new();
    for f in &files {
        for id in ledger_ids(&read(f)) {
            if !known.contains(&id) {
                dangling.push(format!("{}: {id}", f.display()));
            }
        }
    }
    dangling.sort();
    dangling.dedup();
    assert!(
        dangling.is_empty(),
        "这些代码注释引用了 `ROADMAP.md` 里**不存在**的台账编号（悬空引用）：\n  {}\n\
         → 在 `docs/ROADMAP.md §4` 里补上（未闭环的进 §4 表，已闭环的进 §4.1 索引）",
        dangling.join("\n  ")
    );
}
