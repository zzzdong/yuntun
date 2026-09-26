//! **文档一致性判据**（`§123`）：把"文档漂移"从"靠自觉"变成"能跑红"。
//!
//! # 为什么需要它
//!
//! `docs/README.md §2/§3` 定了纪律（改代码必须同步文档），但它**不是自执行的**。实测漂移过：
//! `operation-log.md` 的头部索引停在 `§32` 而正文已到 `§122`、`docs/README.md` 还写着
//! `（§1–§32）`、`plan.md` 头部是 `v2.2` 而落款是 `v2.1`、`meta.proto` 的"迁移进度"表把
//! **已经迁移**的 op 标成 ⏳。这几处**都不是"读一遍能发现"的** —— 要正好读到那一行才会看见。
//!
//! 这里只钉**可机械核对**的那几项。判据失败时的输出必须**直接告诉你改哪里**（和判据本身同样
//! 重要 —— 否则它会变成噪声，最后被忽略）。
//!
//! # 有一条判据**故意**会在每次改代码之后变红
//!
//! [`status_md_scale_line_matches_the_code`] 依赖"代码行数 / crate 数 / 测试函数数"。你只要动
//! 代码，它就会红 —— 直到你把 `docs/status.md` 的规模行更新成它报出来的数字。这是**有意**的：
//! 那三个数字以前靠人记得改，而"记得改"正是会漂移的东西。
//!
//! # 只放"确定不会误报"的判断
//!
//! 宁可少放几条，也不放会偶发红的：每条判据都只依赖**格式明确**的字符串（见各函数的注释），
//! 不去猜自然语言。

use std::path::{Path, PathBuf};

// ---------------------------------------------------------------- 基础设施

/// 仓库根 = 含 `docs/operation-log.md` 的那一级（从 `CARGO_MANIFEST_DIR` 向上找）。
fn repo_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        if p.join("docs/operation-log.md").is_file() {
            return p;
        }
        assert!(
            p.pop(),
            "从 {} 向上没找到含 docs/operation-log.md 的仓库根",
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

/// `docs/*.md` + 根 `README.md`（"范围串"这类判据要跨文档扫）。
fn all_docs(root: &Path) -> Vec<(PathBuf, String)> {
    let mut out: Vec<(PathBuf, String)> = Vec::new();
    let dir = root.join("docs");
    for e in std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("读 {} 失败：{e}", dir.display())) {
        let p = e.expect("目录项").path();
        if p.extension().is_some_and(|x| x == "md") {
            let s = read(&p);
            out.push((p, s));
        }
    }
    let readme = root.join("README.md");
    if readme.is_file() {
        let s = read(&readme);
        out.push((readme, s));
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

// ---------------------------------------------------------------- 判据

/// `operation-log.md` 的**头部索引**必须等于正文最后一节（号和日期都算）。
///
/// 这条是实测漂移最严重的：头部写 `最新：§32`，正文已到 `§122`（差 90 节）。
#[test]
fn operation_log_header_index_matches_last_section() {
    let root = repo_root();
    let log = docs_file(&root, "operation-log.md");
    let max = max_section_no(&log);

    let head = log
        .lines()
        .take(8)
        .find(|l| l.contains("最新：**§"))
        .unwrap_or_else(|| {
            panic!(
                "`docs/operation-log.md` 头部没有 `最新：**§N，YYYY-MM-DD**` 这一行 —— \
                 没有它索引就无法核对，请补回（格式照 `§1–§32` 那版）"
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
            "`docs/operation-log.md` 的头部索引停在 §{n}，而正文已到 §{max}\n\
             → 把头部那行改成：最新：**§{max}，{}**",
            date.clone().unwrap_or_else(|| "<最后一节的日期>".into())
        );
    }
    if let Some(d) = date
        && !head.contains(&d)
    {
        panic!(
            "`docs/operation-log.md` 头部索引的日期不是最后一节的日期\n\
             → 头部应写 `…§{max}，{d}…`，实际是：{head}"
        );
    }
}

/// 任何文档里的 `§1–§N` 范围串，`N` 必须等于正文最后一节。
///
/// 实测漂移：`docs/README.md` 写着 `（§1–§32，按时间）`。
#[test]
fn section_ranges_in_docs_match_the_last_section() {
    let root = repo_root();
    let max = max_section_no(&docs_file(&root, "operation-log.md"));
    let mut bad: Vec<String> = Vec::new();

    for (path, text) in all_docs(&root) {
        // ⚠️ **跳过 `operation-log.md` 本身**：它是**只追加的历史**（本仓纪律不许改旧节），
        // 里面的旧范围串是"当时确实如此"的**记录**，不是导航。导航类文档（`status.md`、
        // `docs/README.md`、根 `README.md`、`closeout.md`…）才必须跟着最新走 ——
        // 而 `operation-log` 的头部索引另有一条判据守着。
        // （这条也是被自己弄红之后才想清楚的：`§123` 那节在讲"把 §1–§32 改成 §1–§122"，于是被判据拦住。）
        if path.file_name().is_some_and(|n| n == "operation-log.md") {
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
                    bad.push(format!(
                        "{}:{} 写的是 §1–§{n}",
                        path.display(),
                        i + 1
                    ));
                }
                rest = &after[digits.len()..];
            }
        }
    }
    assert!(
        bad.is_empty(),
        "这些地方的范围串落后于正文（最新是 §{max}）：\n  {}\n\
         → 统一改成 `§1–§{max}`（这类串是给人看的索引，过期就等于误导）",
        bad.join("\n  ")
    );
}

/// `status.md` 的规模行必须与代码实况一致：行数 / crate 数 / 测试函数数。
///
/// 量法与文档里写的命令一致（`find crates -name '*.rs' | xargs wc -l`；`grep -rh` 计数
/// `#[test]` / `#[tokio::test`）—— 刻意用**子串**计数，与 `grep` 的行为对齐。
/// ⚠️ 改代码后这条**会红**，那是设计：请照着它报出来的数字更新 `docs/status.md`。
#[test]
fn status_md_scale_line_matches_the_code() {
    let root = repo_root();
    let status = docs_file(&root, "status.md");

    let (lines, crates, tests) = measure(&root);
    let line = status
        .lines()
        .find(|l| l.starts_with("规模：**"))
        .unwrap_or_else(|| panic!("`docs/status.md` 里找不到以 `规模：**` 开头的那行"));
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
                "`docs/status.md` 的规模行与代码不符：它写 `{what}` = {}，实测 {w}\n\
                 → 把那一行改成：规模：**{lines} 行 Rust / {crates} 个 crate / {tests} 个测试函数 / …**\n\
                 （用例数与 clippy 警告数不在本条判据里：它们要跑一次全量才知道）",
                reported[i]
            );
        }
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

/// `meta.proto` 的"迁移进度"表必须诚实：
/// ① 每个已在 `oneof op` 里的 op，都要在表里被点名；
/// ② 表里标成 ⏳ 的 op，**不得**已经存在于 `oneof kind`（实测漂移：`EvolveSchema` 等已迁移却仍标 ⏳）。
///
/// 本条只依据**表格行**与**声明行**，不看散文 —— 这条判据被自己的文案弄红过三次，见各处的注释。
#[test]
fn proto_migration_table_is_honest() {
    let root = repo_root();
    let proto = read(&root.join("crates/proto/proto/meta.proto"));

    // ⚠️ 两处**判据自己的教训**（都实测踩过，所以写法这么啰嗦）：
    // 1. 不写死 oneof 的名字（它叫 `kind`，不是 `op`）；
    // 2. 只认**非注释的声明行** —— 一开始用"文件里第一个 `oneof ` 之后的文本"，
    //    结果被**本文件自己**加的一段注释（提到了 `oneof kind`）抢先命中，解析出 0 个 op。
    //    判据依赖"字符串出现过"就必然会被自己/别人顺手写的散文带偏。
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
        // 形如 `EvolveSchemaOp evolve_schema = 5;`
        let Some(ty) = t.split_whitespace().next() else {
            continue;
        };
        if let Some(name) = ty.strip_suffix("Op") {
            ops.push(name.to_string());
        }
    }
    assert!(!ops.is_empty(), "没从 `oneof op` 里解析出任何 op —— 格式变了？");

    // ① 表里必须点名每个 op（表是**注释**，所以只能按反引号里的名字找）
    let missing: Vec<&String> = ops
        .iter()
        .filter(|o| !proto.contains(&format!("`{o}`")))
        .collect();
    assert!(
        missing.is_empty(),
        "`meta.proto` 头部\"迁移进度\"表漏了这些 op：{missing:?}\n\
         → 加一行 `// | `X` | ✅ |`（新 op 必须同时进表）"
    );

    // ② ⏳ 那行点名的 op 不得已经存在
    let mut lied: Vec<String> = Vec::new();
    // ⚠️ 只扫**表格行**（`// | … |`），不扫散文 —— 第三处同类教训：注释里解释"这条曾经漂移过"
    // 时写了 `⏳` 和 `EvolveSchema`，于是被这条判据自己误报。判据依赖的不该是"文件里出现过"。
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

/// `plan.md` 的版本号：**头部**与**落款**必须一致（实测漂移：头 v2.2 / 落款 v2.1）。
#[test]
fn plan_md_version_is_consistent() {
    let root = repo_root();
    let plan = docs_file(&root, "plan.md");

    let ver = |s: &str| -> Option<String> {
        let i = s.find("任务书 v")? + "任务书 v".len();
        let v: String = s[i..]
            .chars()
            .take_while(|c| c.is_ascii_digit() || *c == '.')
            .collect();
        (!v.is_empty()).then_some(v)
    };

    let head = plan.lines().next().unwrap_or_default();
    let tail = plan
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .unwrap_or_default();
    let (Some(h), Some(t)) = (ver(head), ver(tail)) else {
        panic!("`docs/plan.md` 的头部或落款里找不到 `任务书 vX.Y`（头部：{head:?}；落款：{tail:?}）");
    };
    assert_eq!(
        h, t,
        "`docs/plan.md` 版本号不一致：头部 v{h} / 落款 v{t}\n→ 统一成同一个版本（改哪一处都行，别让它两说）"
    );
}
