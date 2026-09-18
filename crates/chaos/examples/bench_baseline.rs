//! T8 基线压测（`plan.md` §2.2 P0 定案的**证据来源**）。
//!
//! 与 `bench.rs` 的区别：那个测**吞吐**（E1：8w 行/秒，压 ingest 路径上限）；
//! 这个测**flush 时刻的分布**——P0 要回答的三个问题都不是吞吐问题：
//!
//! | P0 问题 | 本程序给的量 |
//! |---|---|
//! | `flush_phase_spread_secs` / `max_flush_delay_secs` 的量级 | **提交时刻相对窗口关闭的偏移分布**（P50/P99/max）+ **峰值提交数/秒**（惊群尖峰） |
//! | `rows_threshold` 是否合适 | 单文件行数/字节的分布（小文件放大的直接度量） |
//! | 持久化上界能否对外承诺 | **seal → committed 的延迟分布**（P50/P99/max），口径可核验 |
//!
//! 用法：
//! ```text
//! cargo run --release -p yuntun-chaos --example bench_baseline -- \
//!     <secs> <shards> <batch_rows> <batches_per_sec> <max_flush_delay> <phase_spread> <rows_threshold>
//! # 例：低吞吐表 100 shard，500 行/秒，跑 130s
//! cargo run --release -p yuntun-chaos --example bench_baseline -- 130 100 50 10 30 5 500000
//! ```
//!
//! **为什么至少跑 2 个窗口（≥130s）**：窗口是整分钟对齐的，1 个窗口只能看到
//! 一轮 flush，样本量为 0（分布没有意义）。跑 2 个窗口 = 2×shards 个提交样本。
//!
//! 注意：本地磁盘/单进程下，S3 PUT 与 CommitFiles 都被替换成"本地写 + 内存提交"，
//! 所以这里的**绝对延迟**乐观；**分布形状（分散面多宽、尖峰多高）与配置无关地成立**，
//! 而 P0 要定的正是配置。真实对象存储的绝对量级需在真实环境复测（已登记遗留）。

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use tokio_util::sync::CancellationToken;
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_ingest::{Ingestor, IngestorConfig};
use yuntun_model::meta::{FileManifest, IngestConfig};
use yuntun_model::ops::CreateTableRequest;
use yuntun_model::IngestBatch;

fn schema() -> arrow::datatypes::SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_time", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("host", DataType::Utf8, false),
    ]))
}

fn make_batch(seed: usize, rows: usize, base_ms: i64) -> arrow::record_batch::RecordBatch {
    arrow::record_batch::RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(
                (0..rows).map(|i| base_ms + i as i64).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..rows).map(|i| (i % 100) as f64 / 10.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec![format!("host-{seed}"); rows])),
        ],
    )
    .unwrap()
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// "YYYY-MM-DDTHH:MM"（UTC，`ingest::timeutil::format_window` 的产物）→ epoch 毫秒。
///
/// 手写逆变换（`days_from_civil`）而不是引 chrono：被测代码的窗口键是 UTC，
/// 用本地时区的解析器会静默差 8 小时 —— 那会让整个偏移分布看起来"正常"。
fn parse_window_ms(w: &str) -> Option<i64> {
    let (date, time) = w.split_once('T')?;
    let mut d = date.split('-');
    let y: i64 = d.next()?.parse().ok()?;
    let mo: i64 = d.next()?.parse().ok()?;
    let day: i64 = d.next()?.parse().ok()?;
    let mut t = time.split(':');
    let h: i64 = t.next()?.parse().ok()?;
    let mi: i64 = t.next()?.parse().ok()?;
    // days_from_civil（Howard Hinnant，与 civil_from_epoch_ms 互逆）
    let y2 = if mo <= 2 { y - 1 } else { y };
    let era = y2.div_euclid(400);
    let yoe = y2 - era * 400;
    let mp = (mo + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days * 86_400_000 + h * 3_600_000 + mi * 60_000)
}

fn pct(sorted: &[i64], p: f64) -> i64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(((sorted.len() - 1) as f64) * p) as usize]
}

/// 峰值：把时刻按 `bucket_ms` 分桶，返回（最大桶内计数, 桶起点）
fn peak(times: &[i64], bucket_ms: i64) -> (usize, i64) {
    if times.is_empty() {
        return (0, 0);
    }
    let mut sorted = times.to_vec();
    sorted.sort_unstable();
    let (mut best, mut best_at, mut i) = (0usize, sorted[0], 0usize);
    let mut j = 0usize;
    while i < sorted.len() {
        let start = sorted[i];
        while j < sorted.len() && sorted[j] < start + bucket_ms {
            j += 1;
        }
        if j - i > best {
            best = j - i;
            best_at = start;
        }
        i += 1;
    }
    (best, best_at)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let g = |i: usize, d: i64| a.get(i).and_then(|s| s.parse().ok()).unwrap_or(d);
    let secs = g(1, 130) as u64;
    let shards = g(2, 100) as usize;
    let batch_rows = g(3, 50) as usize;
    let batches_per_sec = g(4, 10) as u64;
    let max_flush_delay = g(5, 30) as u64;
    let phase_spread = g(6, 5) as u64;
    let rows_threshold = g(7, 500_000) as u64;

    let dir = yuntun_testkit::TestDir::disk("bench-baseline");
    let wal_dir = dir.join("wal").to_string_lossy().to_string();
    let store_root = dir.join("store").to_string_lossy().to_string();

    let catalog = Arc::new(MemoryCatalog::new());
    catalog
        .create_table(CreateTableRequest {
            name: "base".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: IngestConfig {
                require_idempotency_key: false,
                ..IngestConfig::standard()
            },
        })
        .await
        .unwrap();
    let store =
        yuntun_store::create_store(&yuntun_store::StoreConfig::Local { root: store_root }).unwrap();
    let wal = yuntun_wal::writer::WalWriter::open(yuntun_wal::WalConfig::for_dir(&wal_dir), 0)
        .await
        .unwrap();
    let ingestor = Arc::new(Ingestor::new(
        IngestorConfig {
            rows_threshold: rows_threshold as usize,
            time_threshold_secs: 5,
            max_flush_delay_secs: max_flush_delay,
            // 不变量：max_resident > max_flush_delay + phase_spread
            chunk_max_resident_secs: max_flush_delay + phase_spread + 60,
            flush_phase_spread_secs: phase_spread,
            scan_interval: Duration::from_millis(200),
            spill_dir: std::path::PathBuf::from(&wal_dir).join("spill"),
            ..Default::default()
        },
        wal,
        catalog.clone(),
        store,
    ));
    let shutdown = CancellationToken::new();
    let _acc = ingestor.clone().spawn_accumulator(shutdown.clone());

    let rows_per_sec = batch_rows as u64 * batches_per_sec;
    println!(
        "---- 配置 ----\nsecs={secs} shards={shards} batch_rows={batch_rows} batches/s={batches_per_sec} \
         => {rows_per_sec} rows/s\nmax_flush_delay={max_flush_delay}s phase_spread={phase_spread}s \
         rows_threshold={rows_threshold}\n(每 shard 约 {:.0} 行/分钟 → 典型低吞吐表)",
        rows_per_sec as f64 * 60.0 / shards as f64
    );

    let t_start = Instant::now();
    let deadline = t_start + Duration::from_secs(secs);
    let tick = Duration::from_millis((1000 / batches_per_sec.max(1)).max(1));
    let mut seed = 0usize;
    let mut sent_rows = 0u64;
    let mut next_at = Instant::now();
    let mut ingest_failed = 0usize;
    while Instant::now() < deadline {
        let shard = format!("s{}", seed % shards);
        let b = make_batch(seed, batch_rows, now_ms());
        match ingestor
            .ingest(IngestBatch {
                table: "base".into(),
                shard_key: shard,
                record_batch: b,
                idempotency_key: None,
                received_at: std::time::SystemTime::now(),
            })
            .await
        {
            Ok(r) => sent_rows += r.row_count,
            Err(e) => {
                eprintln!("ingest error: {e}");
                ingest_failed += 1;
                if ingest_failed > 10 {
                    break;
                }
            }
        }
        seed += 1;
        next_at += tick;
        if let Some(d) = next_at.checked_duration_since(Instant::now()) {
            tokio::time::sleep(d).await;
        }
    }
    let write_secs = t_start.elapsed().as_secs_f64();

    // 排水：等到提交数不再增长（窗口是整分钟的，最后一个窗口要在关闭后才 seal）
    //
    // ⚠️ 只判"连续无新增"会**提前退出**：最后一个窗口在写入停止时还没关闭，
    // 此刻计数当然不动 —— 于是那一窗口的数据全被漏掉（第一版就是这么错的：
    // 写入 65000 行、只统计到 37450 行）。必须同时满足"时间已过
    // 最后一个写入窗口关闭 + (md + spread) + 余量"。
    let mut last = usize::MAX;
    let mut stable = 0;
    let last_write_ms = now_ms();
    let last_window_close_ms = (last_write_ms / 60_000 + 1) * 60_000;
    let hard_until_ms = last_window_close_ms + (max_flush_delay + phase_spread) as i64 * 1000 + 10_000;
    let drain_deadline = Instant::now() + Duration::from_secs(max_flush_delay + phase_spread + 130);
    while Instant::now() < drain_deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let snap = catalog.current_snapshot().await;
        let n = catalog
            .list_visible_files("public.base", snap, None)
            .await
            .unwrap()
            .len();
        if n == last {
            stable += 1;
            // 连续 6 次（3s）无新增 **且** 已过最后一个窗口的提交时刻 → 才算排空
            if stable >= 6 && now_ms() >= hard_until_ms {
                break;
            }
        } else {
            stable = 0;
            last = n;
        }
    }
    let total_secs = t_start.elapsed().as_secs_f64();
    shutdown.cancel();

    // ---- 统计 ----
    let snap = catalog.current_snapshot().await;
    let files: Vec<FileManifest> = catalog
        .list_visible_files("public.base", snap, None)
        .await
        .unwrap();
    let bytes: u64 = files.iter().map(|f| f.file_size).sum();
    let rows: u64 = files.iter().map(|f| f.row_count).sum();
    let files_per_day = files.len() as f64 / write_secs * 86_400.0;
    let mb_per_day = bytes as f64 / 1e6 / write_secs * 86_400.0;

    // 提交时刻相对**窗口关闭**的偏移（P0 的核心量：分散面多宽）
    let mut offsets: Vec<i64> = Vec::new();
    let mut windows: std::collections::BTreeMap<String, usize> = Default::default();
    let mut commits_by_window: std::collections::BTreeMap<String, Vec<i64>> = Default::default();
    for f in &files {
        let Some(ws) = parse_window_ms(&f.time_window) else {
            continue;
        };
        let off = f.committed_at_ms as i64 - (ws + 60_000);
        offsets.push(off);
        *windows.entry(f.time_window.clone()).or_default() += 1;
        commits_by_window
            .entry(f.time_window.clone())
            .or_default()
            .push(f.committed_at_ms as i64);
    }
    offsets.sort_unstable();

    let mut seal_to_commit: Vec<i64> = files
        .iter()
        .filter(|f| f.sealed_at_ms > 0 && f.committed_at_ms > f.sealed_at_ms)
        .map(|f| (f.committed_at_ms - f.sealed_at_ms) as i64)
        .collect();
    seal_to_commit.sort_unstable();

    let mut rows_per_file: Vec<i64> = files.iter().map(|f| f.row_count as i64).collect();
    rows_per_file.sort_unstable();

    let commit_times: Vec<i64> = files.iter().map(|f| f.committed_at_ms as i64).collect();
    let (peak_1s, _peak_1s_at) = peak(&commit_times, 1000);
    let (peak_100ms, _) = peak(&commit_times, 100);

    // 每个窗口内：该窗口的提交落在多宽的带里（横向对比 spread 最直观）
    println!("\n---- 按窗口（提交时刻相对窗口关闭的偏移，ms）----");
    for (w, times) in &commits_by_window {
        let ws = parse_window_ms(w).unwrap();
        let mut offs: Vec<i64> = times.iter().map(|t| t - (ws + 60_000)).collect();
        offs.sort_unstable();
        println!(
            "{w}  files={:3}  offset p50={:>7} p99={:>7} max={:>7} 带宽={:>7}",
            times.len(),
            pct(&offs, 0.5),
            pct(&offs, 0.99),
            offs.last().copied().unwrap_or(0),
            offs.last().copied().unwrap_or(0) - offs[0]
        );
    }

    // 每 (shard, window) 的文件数：ADR-10 承诺"每窗口每 shard ≤1 文件"（小文件控制的根）。
    // >1 说明该 (shard,窗口) 被拆成了多个文件 —— 必须查清是被什么触发的。
    let mut per_key: std::collections::BTreeMap<(String, String), usize> = Default::default();
    for f in &files {
        *per_key
            .entry((f.shard.clone(), f.time_window.clone()))
            .or_default() += 1;
    }
    let dup_keys = per_key.values().filter(|n| **n > 1).count();
    let max_per_key = per_key.values().copied().max().unwrap_or(0);
    println!(
        "\n---- 小文件控制 ----\n每(shard,窗口)文件数 : max={max_per_key}，>1 的组合={dup_keys}/{}",
        per_key.len()
    );
    // 偏移最大的 5 个文件（离群值必须能定位到 shard/封口时刻，否则无法归因）
    let mut by_off: Vec<(i64, &FileManifest)> = files
        .iter()
        .filter_map(|f| {
            parse_window_ms(&f.time_window).map(|ws| (f.committed_at_ms as i64 - (ws + 60_000), f))
        })
        .collect();
    by_off.sort_by_key(|(o, _)| -o);
    for (o, f) in by_off.iter().take(5) {
        println!(
            "  偏移最大 {o:>7}ms: shard={} window={} rows={} sealed_at={} committed_at={}",
            f.shard, f.time_window, f.row_count, f.sealed_at_ms, f.committed_at_ms
        );
    }

    println!("---- P0 证据 ----");
    println!("写入                  : {sent_rows} rows in {write_secs:.1}s ({:.0} rows/s)，实际落盘 {rows} rows", sent_rows as f64 / write_secs);
    println!(
        "文件数                : {}（{:.0} 文件/天，{:.1} MB/天）",
        files.len(),
        files_per_day,
        mb_per_day
    );
    println!(
        "单文件行数            : min/p50/max = {} / {} / {}",
        rows_per_file.first().copied().unwrap_or(0),
        pct(&rows_per_file, 0.5),
        rows_per_file.last().copied().unwrap_or(0)
    );
    println!(
        "提交偏移(关窗后)      : min/p50/p99/max = {} / {} / {} / {} ms",
        offsets.first().copied().unwrap_or(0),
        pct(&offsets, 0.5),
        pct(&offsets, 0.99),
        offsets.last().copied().unwrap_or(0)
    );
    println!(
        "峰值提交              : {peak_1s} 次/秒（{peak_100ms} 次/100ms）；理论均值 {:.1} 次/秒",
        files.len() as f64 / write_secs
    );
    println!(
        "seal → committed      : p50/p99/max = {} / {} / {} ms（口径：flush 启动 → 提交成功）",
        pct(&seal_to_commit, 0.5),
        pct(&seal_to_commit, 0.99),
        seal_to_commit.last().copied().unwrap_or(0)
    );
    println!(
        "窗口数                : {}（{}）",
        windows.len(),
        windows.keys().cloned().collect::<Vec<_>>().join(", ")
    );
    // 机器可读一行，便于横向对比与写进文档
    println!(
        "RESULT {{\"secs\":{secs},\"shards\":{shards},\"rows_per_sec\":{rows_per_sec},\"md\":{max_flush_delay},\
         \"spread\":{phase_spread},\"rows_threshold\":{rows_threshold},\"files\":{},\"files_per_day\":{files_per_day:.0},\
         \"off_p50\":{},\"off_p99\":{},\"off_max\":{},\"peak_commit_1s\":{peak_1s},\"peak_commit_100ms\":{peak_100ms},\
         \"seal_to_commit_p99\":{},\"seal_to_commit_max\":{},\"rows_per_file_p50\":{},\"total_secs\":{total_secs:.1}}}",
        files.len(),
        pct(&offsets, 0.5),
        pct(&offsets, 0.99),
        offsets.last().copied().unwrap_or(0),
        pct(&seal_to_commit, 0.99),
        seal_to_commit.last().copied().unwrap_or(0),
        pct(&rows_per_file, 0.5),
    );
}
