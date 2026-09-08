//! 压测（阶段 0.5 验收 E1）：目标 8w 行/秒（10 列 × ~1KB 行）。
//! 运行：cargo run --release -p yuntun-chaos --example bench [-- duration_secs workers]
//!
//! 口径：进程内直调 Ingestor::ingest（隔离 Flight 网络层），
//! 多 worker 并发写单表多 shard，统计每秒实际写入行数与 WAL ack 延迟。

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema};
use tokio_util::sync::CancellationToken;
use yuntun_catalog::CatalogOps;
use yuntun_catalog::MemoryCatalog;
use yuntun_ingest::{Ingestor, IngestorConfig};
use yuntun_model::ops::CreateTableRequest;
use yuntun_model::IngestBatch;

fn schema() -> arrow::datatypes::SchemaRef {
    use DataType::*;
    Arc::new(Schema::new(vec![
        Field::new(
            "event_time",
            Timestamp(TimestampMillisecondArcUnit(), None),
            false,
        ),
        Field::new("req_id", Utf8, false),    // 36B
        Field::new("source_ip", Utf8, false), // 15B
        Field::new("endpoint", Utf8, false),  // 64B
        Field::new("actor", Utf8, true),      // 64B
        Field::new("method", Utf8, false),    // 8B
        Field::new("status_code", Int32, false),
        Field::new("cost_ms", Int64, false),
        Field::new("bytes_in", Int64, false),
        Field::new("bytes_out", Int64, false),
        Field::new("score", Float64, false),
        Field::new("payload", Utf8, false), // ~800B 撑起 1KB 行
    ]))
}
// 避免单元名冲突的小包装
#[allow(non_snake_case)]
fn TimestampMillisecondArcUnit() -> arrow::datatypes::TimeUnit {
    arrow::datatypes::TimeUnit::Millisecond
}

/// ~1KB 的行 payload
fn payload(seed: usize) -> String {
    let base = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_.";
    (0..800)
        .map(|i| base.as_bytes()[(seed + i) % base.len()] as char)
        .collect()
}

fn make_batch(seed: usize, rows: usize, base_ms: i64) -> arrow::record_batch::RecordBatch {
    let p = payload(seed);
    arrow::record_batch::RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampMillisecondArray::from(
                (0..rows).map(|i| base_ms + i as i64).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..rows)
                    .map(|i| format!("{:032x}-{}", seed, i))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..rows)
                    .map(|i| format!("10.0.{}.{}", seed % 250, i % 250))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..rows)
                    .map(|i| format!("/api/v1/resource/{}", i % 64))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..rows)
                    .map(|i| format!("user_{}", i % 1000))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec!["POST"; rows])),
            Arc::new(Int32Array::from(vec![200; rows])),
            Arc::new(Int64Array::from(
                (0..rows as i64).map(|i| 100 + i % 500).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(vec![1024; rows])),
            Arc::new(Int64Array::from(vec![4096; rows])),
            Arc::new(Float64Array::from(
                (0..rows)
                    .map(|i| (i % 100) as f64 / 10.0)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec![p.as_str(); rows])),
        ],
    )
    .unwrap()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let duration = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(10u64);
    let workers: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(8);
    let batch_rows = 500; // 每批 500 行 ≈ 500KB

    let wal_dir = format!("/tmp/yuntun-bench-wal-{}", std::process::id());
    let _ = std::fs::remove_dir_all(&wal_dir);
    let store_root = format!("/tmp/yuntun-bench-store-{}", std::process::id());
    let _ = std::fs::remove_dir_all(&store_root);

    let catalog = Arc::new(MemoryCatalog::new());
    catalog
        .create_table(CreateTableRequest {
            name: "bench".into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig {
                require_idempotency_key: false,
                ..yuntun_model::meta::IngestConfig::standard()
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
            rows_threshold: 10_000,
            time_threshold_secs: 5,
            idle_timeout: Duration::from_secs(60),
            flush_jitter_secs: 0,
            scan_interval: Duration::from_millis(200),
            ..Default::default()
        },
        wal,
        catalog.clone(),
        store,
    ));
    // 启动攒批（flush 与写入并行，计入总吞吐）
    let shutdown = CancellationToken::new();
    let _acc = ingestor.clone().spawn_accumulator(shutdown.clone());

    println!(
        "bench: workers={} batch_rows={} duration={}s (debug={} release 建议)",
        workers,
        batch_rows,
        duration,
        cfg!(debug_assertions)
    );

    let deadline = Instant::now() + Duration::from_secs(duration);
    let t_start = Instant::now();
    let mut handles = Vec::new();
    for w in 0..workers {
        let ingestor = ingestor.clone();
        handles.push(tokio::spawn(async move {
            let mut local_rows: u64 = 0;
            let mut seed = w;
            let mut latencies: Vec<u128> = Vec::new();
            while Instant::now() < deadline {
                seed += 1;
                let b = make_batch(seed, batch_rows, now_ms());
                let t0 = Instant::now();
                let r = ingestor
                    .ingest(IngestBatch {
                        table: "bench".into(),
                        shard_key: format!("s{}", w),
                        record_batch: b,
                        idempotency_key: None,
                        received_at: std::time::SystemTime::now(),
                    })
                    .await;
                match r {
                    Ok(receipt) => {
                        local_rows += receipt.row_count;
                        latencies.push(t0.elapsed().as_micros());
                    }
                    Err(e) => {
                        eprintln!("ingest error: {e}");
                        break;
                    }
                }
            }
            (local_rows, latencies)
        }));
    }

    let mut total_rows = 0u64;
    let mut all_lat = Vec::new();
    for h in handles {
        let (rows, lat) = h.await.unwrap();
        total_rows += rows;
        all_lat.extend(lat);
    }
    shutdown.cancel();
    let elapsed = t_start.elapsed().as_secs_f64().max(0.001);

    all_lat.sort_unstable();
    let pct = |p: f64| -> u128 {
        let idx = ((all_lat.len() as f64) * p) as usize;
        all_lat
            .get(idx.min(all_lat.len() - 1))
            .copied()
            .unwrap_or(0)
    };

    println!("---- 结果 ----");
    println!("total_rows        : {total_rows}");
    println!("elapsed           : {elapsed:.1}s");
    println!(
        "throughput        : {:.0} rows/s",
        total_rows as f64 / elapsed
    );
    println!(
        "wal ack p50/p95/p99 (µs): {} / {} / {}",
        pct(0.50),
        pct(0.95),
        pct(0.99)
    );
    println!("target            : 80000 rows/s (E1) —— release 模式 + Flight 并发评估为准");

    let _ = catalog; // catalog 保留供检查
                     // 清理
    tokio::time::sleep(Duration::from_millis(100)).await;
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
