//! **节点私有状态目录的所有权闸门**（`operation-log §28.2` 的显式化；R4 的 T12.4–T12.5）。
//!
//! 契约只有一条：**同一份私有状态（WAL 根目录 / spill 目录）同一时刻只能有一个消费者**。
//! 违反它**不报错**，只产出重复数据 —— `§28.2` 实测：同一份 WAL 目录被两个攒批循环消费，
//! 同一条 `Data` 被各吸收一次、各自 flush，于是 **9 批次 27 行变成 11 个文件 33 行**。
//!
//! 本文件验三件事：
//! 1. 同一份私有目录起第二个 `Lakehouse` → **启动即被拒**，且报错**点名谁占着**；
//! 2. 拒绝发生在**动盘之前**（用"遗留 spill 是否还在"来证）；
//! 3. 前一个被 drop（正常停机；`kill -9` 由 OS 释放锁，语义相同）后可以**接管**。
//!
//! 用 `[meta] mode = "memory"`：本文件只关心私有目录闸门，不要引入 metanode 这个变量
//! （`memory` 是设计指定的回滚形态，见 `config.rs` 的 `MetaMode`）。

use std::path::Path;
use std::sync::Arc;

use yuntun_server::{Config, Lakehouse};

fn config(base: &str) -> Config {
    Config::from_toml(&format!(
        r#"
[meta]
# 本文件只验私有目录闸门：用内存 catalog，不引入嵌入式 metanode 这个变量
mode = "memory"

[store]
type = "memory"

[wal]
dir = "{base}/wal"

[chunk]
# 每个测试 = 一个节点：私有目录各用各的（`operation-log §28.2` 的闸门会（正确地）拒绝
# 两个消费者共用一份私有状态）
spill_dir = "{base}/spill"
# 刻意取一个能一眼认出的实例标识：下面断言报错里要**点名**它
instance_id = "node-under-test"
"#
    ))
    .unwrap()
}

async fn build(cfg: &Config) -> Result<Arc<Lakehouse>, yuntun_model::error::LakeError> {
    Ok(Arc::new(Lakehouse::build(cfg).await?))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_node_on_the_same_private_dirs_is_refused() {
    let guard = yuntun_testkit::TestDir::tmpfs("private-dir-gate");
    let base = guard.string();
    let cfg = config(&base);

    let first = build(&cfg).await.expect("第一个节点应能启动");

    // 放一个"上次崩溃遗留的 spill"：若被拒的进程跑到了 `purge_leftover_spills`，
    // 它会被删掉 —— 于是这个文件的存在与否就证明了"拒绝是否发生在动盘之前"。
    let leftover = Path::new(&base).join("spill").join("leftover.ipc.lz4");
    std::fs::write(&leftover, b"pretend-this-is-a-leftover-spill").expect("放置假遗留 spill");

    // 用 `match` 而不是 `expect_err`：`expect_err` 要求 `Ok` 侧实现 `Debug`，而
    // `Lakehouse` 有意不实现（它拿着的都是句柄，Debug 只会误导）
    let err = match build(&cfg).await {
        Ok(_) => panic!("同一份私有目录上的第二个节点**必须**被拒绝启动"),
        Err(e) => e,
    };
    let msg = err.to_string();

    // 报错必须可直接照做：说清"被谁占了"（pid / instance_id / role）以及怎么查。
    for needle in [
        "已被另一个消费者占用",
        "node-under-test",
        "wal-root",
        "lsof",
        "§28.2",
    ] {
        assert!(msg.contains(needle), "报错里应出现 {needle:?}：\n{msg}");
    }

    // 动盘检查：被拒的进程不得清理别人的 spill（那会删掉对方正在用的热副本）
    assert!(
        leftover.is_file(),
        "被拒的进程**不得**动盘：遗留 spill 应原封不动"
    );

    // 停机 → 释放 → 可接管（模拟正常重启）
    drop(first);
    let second = build(&cfg).await.expect("前一个释放后应能接管");
    drop(second);

    // 接管之后这个目录**仍然可用**（不是"拒一次就永久锁死"）
    let third = build(&cfg).await.expect("反复启停不应把目录锁死");
    drop(third);
}

/// `wal` 与 `spill` 是**两份**独立的私有状态：只冲突其中一个也要被拒
/// （否则"WAL 目录配错、spill 目录配对"这种半对配置会静默放过）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflict_on_either_private_dir_is_enough_to_refuse() {
    let guard_a = yuntun_testkit::TestDir::tmpfs("private-dir-gate-a");
    let guard_b = yuntun_testkit::TestDir::tmpfs("private-dir-gate-b");
    let (base_a, base_b) = (guard_a.string(), guard_b.string());

    let first = build(&config(&base_a)).await.expect("第一个节点应能启动");

    // 只把 spill 目录指到 A 的（wal 用 B 的）→ 仍与 A 冲突
    let cfg = Config::from_toml(&format!(
        r#"
[meta]
mode = "memory"

[store]
type = "memory"

[wal]
dir = "{base_b}/wal"

[chunk]
spill_dir = "{base_a}/spill"
instance_id = "node-under-test"
"#
    ))
    .unwrap();

    let err = match build(&cfg).await {
        Ok(_) => panic!("spill 目录冲突也必须被拒"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("chunk-spill"),
        "应点名是 spill 目录冲突：{err}"
    );

    drop(first);
}
