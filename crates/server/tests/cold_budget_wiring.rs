//! 配置 → 引擎的接线判据（`§193`）：`[query].cold_read_budget_mb` 必须在
//! **查询引擎**上生效，不能只是配置里解析成功。
//!
//! 为什么单独钉这一条：本仓对"配了但不生效"的漂移有多次教训（`D-41`/`D-42`），
//! 而这条预算的失效形态正好是**静默**的 —— 配置写着 512，实际不限，直到某次全表扫
//! 把内存吃满才有人发现。`0` 的告警同理：它是"关掉闸门"，必须在启动时说出来。

use tokio_util::sync::CancellationToken;

fn config(wal_dir: &str, budget_mb: u64) -> yuntun_server::Config {
    yuntun_server::Config::from_toml(&format!(
        r#"
[server]
listen = "127.0.0.1:0"

[store]
type = "memory"

[wal]
dir = "{wal_dir}"

[chunk]
spill_dir = "{wal_dir}/spill"

[query]
cold_read_budget_mb = {budget_mb}
"#
    ))
    .unwrap()
}

#[tokio::test]
async fn configured_cold_budget_reaches_the_query_engine() {
    let dir = yuntun_testkit::TestDir::tmpfs("cold-budget-wiring");
    let cfg = config(&dir.string(), 3);
    assert_eq!(cfg.query.cold_read_budget_mb, 3);

    let shutdown = CancellationToken::new();
    let lakehouse = yuntun_server::Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
        .await
        .unwrap();
    assert_eq!(
        lakehouse.query.cold_read_budget_bytes(),
        3 * 1024 * 1024,
        "配置值必须真的进到查询引擎（否则闸门按默认 512MB 放行）"
    );
    shutdown.cancel();
}

#[test]
fn zero_budget_is_warned_about_at_startup() {
    let cfg = yuntun_server::Config::from_toml("[query]\ncold_read_budget_mb = 0").unwrap();
    assert_eq!(
        yuntun_server::Config::default().query.cold_read_budget_mb,
        512,
        "默认必须是 512（与 query_mem_budget_mb 一致）"
    );
    let w = cfg.warnings();
    assert!(
        w.iter().any(|m| m.contains("cold_read_budget_mb")),
        "0 = 关掉闸门，启动必须有一条 warn（否则没人知道自己回到了 D-20 形态）：{w:?}"
    );
}
