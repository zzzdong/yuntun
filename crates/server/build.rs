//! 构建脚本：从 workspace `Cargo.lock` 提取 arrow 的实际版本，
//! 注入编译期环境变量 `YUNTUN_ARROW_VERSION`。
//!
//! `SqlInfo::FlightSqlServerArrowVersion` 使用该值（arrow-rs 自身不导出版本常量），
//! 编译期自动跟随 Cargo.lock，避免硬编码漂移。

use std::path::PathBuf;

fn main() {
    // workspace 根：crates/server → ../../Cargo.lock
    let lock = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
        .join("..")
        .join("..")
        .join("Cargo.lock");
    println!("cargo:rerun-if-changed={}", lock.display());

    let content = match std::fs::read_to_string(&lock) {
        Ok(c) => c,
        Err(_) => return, // 非 workspace 布局（罕见）：留空，运行时回退
    };

    // 行级解析 Cargo.lock：`[[package]]` 块内找 `name = "arrow"`（精确匹配，
    // 不含 arrow-*）→ 取同块 `version = "x.y.z"`。格式由 cargo 生成，稳定。
    let mut version: Option<String> = None;
    let mut matched = false;
    for line in content.lines() {
        let line = line.trim();
        if line == "[[package]]" {
            if version.is_some() {
                break;
            }
            matched = false;
        } else if let Some(v) = line.strip_prefix("name = ") {
            matched = v == "\"arrow\"";
        } else if matched {
            if let Some(v) = line.strip_prefix("version = ") {
                version = v.trim_matches('"').to_string().into();
            }
        }
    }
    if let Some(v) = version {
        println!("cargo:rustc-env=YUNTUN_ARROW_VERSION={v}");
    }
}
