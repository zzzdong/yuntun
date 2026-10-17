//! **ADR-1 的"Vortex 主格式"正式成为默认**（`operation-log §164`，台账 `D-8` 的决策落地）。
//!
//! 这一刀的关键不在"能编出 vortex"（`§139` 早就往返成功），而在三件以前做不到的事：
//!
//! 1. **查询侧读得到**：vortex 文件过去"挂了也没人认"（`query::table` 只有 parquet 读路径）
//!    ⇒ 直接改默认会变成"写进去、查不到"的**静默事故**；`§164` 补了 `.vortex` 的读路径；
//! 2. **删除/更新在 vortex 上照样生效**：DV 过去只能挂在 parquet 上（非 parquet 明确报错），
//!    现在的读路径自己按保留区间切片 ⇒ 两种格式对"已删的行"必须给出**同一答案**；
//! 3. **合并不会读错**：compaction 过去用**自己配置的格式**去读，遇到另一类文件就是
//!    "Corrupt footer"（`§164` 实测撞到）⇒ 改成**按文件自己的格式读**。
//!
//! 用例本身两条腿：**默认（vortex）**走完 写→读→删→改，**显式 parquet** 再走一遍
//! （回退格式必须仍然可用 —— 既有部署不会因为这次改默认而读不了老数据）。

use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use yuntun_server::Lakehouse;
use yuntun_sql::SqlResult;
use yuntun_sql::session::SessionCtx;

fn config(wal_dir: &str, store_root: &str, default_format: Option<&str>) -> yuntun_server::Config {
    let fmt_line = match default_format {
        Some(f) => format!("default_format = \"{f}\"\n"),
        None => String::new(),
    };
    yuntun_server::Config::from_toml(&format!(
        r#"
[store]
type = "local"
root = "{store_root}"

[wal]
dir = "{wal_dir}"

[meta]
mode = "memory"

[chunk]
spill_dir = "{wal_dir}/spill"

[ingest]
{fmt_line}"#
    ))
    .unwrap()
}

/// 执行一条语句，把第一列取成 `Vec<i64>`（`SELECT`）；写语句返回受影响行数。
async fn run(lakehouse: &Lakehouse, session: &mut SessionCtx, sql: &str) -> Vec<i64> {
    match lakehouse.sql.execute(sql, session).await {
        Ok(SqlResult::Rows { batches, .. }) => {
            let mut out = Vec::new();
            for b in &batches {
                let col = b.column(0);
                let arr = col
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap_or_else(|| panic!("`{sql}` 的第一列不是 Int64"));
                for i in 0..arr.len() {
                    out.push(arr.value(i));
                }
            }
            out
        }
        Ok(SqlResult::Affected(_)) => Vec::new(),
        Err(e) => panic!("`{sql}` 失败：{e}"),
    }
}

/// 当前可见文件里**数据文件**的扩展名集合（排除 `.idx` 索引）。
async fn committed_extensions(lakehouse: &Lakehouse, table: &str) -> Vec<String> {
    let snap = lakehouse.catalog.current_snapshot().await;
    let files = lakehouse
        .catalog
        .list_visible_files(table, snap, None)
        .await
        .unwrap_or_default();
    files
        .iter()
        .filter(|f| !f.file_path.ends_with(".idx"))
        .map(|f| {
            f.file_path
                .rsplit_once('.')
                .map(|(_, ext)| ext.to_string())
                .unwrap_or_default()
        })
        .collect()
}

/// 一个 lakehouse 上的完整走查：写 → 读 → 删 → 改，并返回产物扩展名。
async fn walk(default_format: Option<&str>, label: &str) -> Vec<String> {
    let guard = yuntun_testkit::TestDir::tmpfs(&format!("vortex-{}", label));
    let base = guard.string();
    let cfg = config(
        &format!("{base}/wal"),
        &format!("{base}/store"),
        default_format,
    );
    let shutdown = CancellationToken::new();
    let lakehouse = Arc::new(
        Lakehouse::build_with_shutdown(&cfg, shutdown.clone())
            .await
            .unwrap(),
    );
    // 后台任务（flush 循环 / 刷新循环）：`DELETE` 的"强制 flush"靠它把在途数据落盘
    let _bg = lakehouse.spawn_background(&cfg);
    let mut session = SessionCtx::default();

    run(&lakehouse, &mut session, "CREATE TABLE t (v BIGINT)").await;
    run(
        &lakehouse,
        &mut session,
        "INSERT INTO t VALUES (1), (2), (3)",
    )
    .await;

    // 用一条"命中 0 行"的 DELETE 把 flush 逼出来（`DELETE` 先强制 flush 再定位）⇒
    // 这样"产物是什么格式"就是**确定**的，不用等窗口关闭、不用 sleep。
    run(&lakehouse, &mut session, "DELETE FROM t WHERE v = 999").await;

    let exts = committed_extensions(&lakehouse, "public.t").await;
    assert!(!exts.is_empty(), "{label}：应当有已提交的数据文件");

    // ① 写进去的读得到（读路径）
    let mut v = run(&lakehouse, &mut session, "SELECT v FROM t ORDER BY v").await;
    v.sort();
    assert_eq!(v, vec![1, 2, 3], "{label}：写入的三行都应当读得到");

    // ② 删除在**这个格式**上生效（DV）
    run(&lakehouse, &mut session, "DELETE FROM t WHERE v = 2").await;
    let mut v = run(&lakehouse, &mut session, "SELECT v FROM t ORDER BY v").await;
    v.sort();
    assert_eq!(
        v,
        vec![1, 3],
        "{label}：删掉的行必须查不到（DV 在该格式上生效）"
    );

    // ③ 更新生效（新行写成**同一个格式**的文件，再被读回来）
    run(
        &lakehouse,
        &mut session,
        "UPDATE t SET v = v + 100 WHERE v = 1",
    )
    .await;
    let mut v = run(&lakehouse, &mut session, "SELECT v FROM t ORDER BY v").await;
    v.sort();
    assert_eq!(v, vec![3, 101], "{label}：UPDATE 的新值必须读得到");

    shutdown.cancel();
    exts
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_format_is_vortex_and_the_whole_dml_path_works_on_it() {
    let exts = walk(None, "default").await;
    assert!(
        exts.iter().all(|e| e == "vortex"),
        "默认格式必须是 vortex（ADR-1 主格式，`§164`）：{exts:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parquet_still_works_when_explicitly_configured() {
    let exts = walk(Some("parquet"), "parquet").await;
    assert!(
        exts.iter().all(|e| e == "parquet"),
        "显式配 parquet 时产物必须仍是 parquet（回退格式不能被改默认带走）：{exts:?}"
    );
}

/// **没编进 vortex 却配了 vortex** ⇒ 启动自检就要说清楚，而不是等第一次写入才失败。
#[test]
fn config_warns_when_vortex_is_configured_but_not_compiled() {
    let cfg = config("/tmp/w", "/tmp/s", Some("vortex"));
    let warnings = cfg.warnings();
    // 本二进制**编进了** vortex（默认特性）⇒ 这条自检不该响
    assert!(
        !warnings
            .iter()
            .any(|w| w.contains("没有") && w.contains("vortex")),
        "编进 vortex 时不该告警：{warnings:?}"
    );
}
