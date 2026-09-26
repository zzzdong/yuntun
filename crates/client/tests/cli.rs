//! `yuntun-cli` 的 **CLI 契约**：这些行为是脚本与文档依赖的，迁移解析实现（手写 → clap）时
//! 必须保持。测的是**进程**而不是内部函数 —— 参数解析的消费者是 shell 脚本。
//!
//! 只测"不需要服务端"的部分（help/用法错）：真连服务端的流程在 `client_e2e.rs`（SDK 层）。

use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_yuntun-cli");

fn run(args: &[&str]) -> std::process::Output {
    Command::new(BIN).args(args).output().expect("跑 yuntun-cli")
}

#[test]
fn help_lists_every_subcommand_and_global_flag() {
    let out = run(&["--help"]);
    assert!(out.status.success(), "--help 必须退 0（它不是错误）");
    let s = String::from_utf8_lossy(&out.stdout);
    for needle in ["query", "insert", "tables", "schema", "meta", "--addr"] {
        assert!(s.contains(needle), "--help 里应出现 {needle}：\n{s}");
    }
}

/// 裸调用 = 想看用法（退 0，不是错误）。
#[test]
fn bare_invocation_prints_help() {
    let out = run(&[]);
    assert!(out.status.success(), "裸调用应退 0");
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("yuntun-cli"), "{s}");
}

/// 用法错必须退 2、走 stderr、并**指出是哪个参数** —— 这正是改用 clap 的收益，
/// 手写版在这种情况下只会留下一行含糊的提示。
#[test]
fn usage_errors_exit_two_and_name_the_offending_argument() {
    for (args, needle) in [
        (vec!["query", "select 1", "--format", "yaml"], "--format"),
        (vec!["bogus-cmd"], "bogus-cmd"),
        (vec!["insert"], "--table"),
        // 成员变更（`§128`）：`--node` 与 `--meta` 都必填，缺哪个就该点名哪个
        (vec!["meta", "promote"], "--node"),
        (vec!["meta", "remove"], "--node"),
    ] {
        let out = run(&args);
        assert_eq!(out.status.code(), Some(2), "args={args:?} 用法错应退 2");
        let e = String::from_utf8_lossy(&out.stderr);
        assert!(
            e.contains(needle),
            "args={args:?} stderr 里应指出 {needle}：\n{e}"
        );
    }
}

/// `--format` 的别名与大小写不敏感是**迁移前就有的行为**，不能因为换成 clap 而丢。
/// （这里只验证解析层不报错 —— 真执行要连服务端，故用"是否退 0/2"区分。）
#[test]
fn insert_format_aliases_are_accepted() {
    // 格式合法 → 不会退 2（连不上服务端才是退 1）
    for fmt in ["csv", "CSV", "jsonl", "json", "ndjson", "parquet"] {
        let out = run(&["insert", "-t", "t", "--format", fmt]);
        assert_ne!(
            out.status.code(),
            Some(2),
            "--format {fmt} 应当被接受（别名/大小写），stderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    // 非法格式 → 用法错
    let out = run(&["insert", "-t", "t", "--format", "yaml"]);
    assert_eq!(out.status.code(), Some(2));
}
