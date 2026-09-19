//! `yuntun`（单机进程）的 CLI 契约。
//!
//! 只测"不会真的把服务起起来"的路径：`--version` / `--help` / 用法错。
//! 真启动会绑端口、建目录、起后台任务 —— 那是端到端测试的事，不该在这里做。

use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_yuntun");

fn run(args: &[&str]) -> std::process::Output {
    Command::new(BIN).args(args).output().expect("跑 yuntun")
}

/// `--version` 的输出是**对外契约**（打包/发布脚本会读它）：必须是 `yuntun <版本>` 一行。
#[test]
fn version_prints_name_and_version() {
    let out = run(&["--version"]);
    assert!(out.status.success(), "--version 必须退 0");
    let s = String::from_utf8_lossy(&out.stdout);
    let expect = format!("yuntun {}", env!("CARGO_PKG_VERSION"));
    assert!(
        s.trim() == expect,
        "--version 输出应为 {expect:?}，实际 {s:?}"
    );
}

#[test]
fn help_documents_config_flag() {
    let out = run(&["--help"]);
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("--config"), "--help 里应说明 --config：\n{s}");
}

/// 用法错：退 2 + 指出参数（迁移到 clap 后的统一行为）。
#[test]
fn unknown_flag_exits_two() {
    let out = run(&["--wat"]);
    assert_eq!(out.status.code(), Some(2), "用法错应退 2");
    assert!(String::from_utf8_lossy(&out.stderr).contains("--wat"));
}
