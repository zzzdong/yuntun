//! `yuntun-cli`：yuntun 客户端 CLI（S1.9）。
//!
//! 子命令：`query` / `insert` / `tables` / `schema`；用法见 [`USAGE`]。

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use arrow::datatypes::SchemaRef;
use yuntun_client::input::{read_file, read_stream};
use yuntun_client::{Client, InputFormat};

const USAGE: &str = "\
yuntun-cli — yuntun 客户端（Arrow Flight 简易轨）

用法:
  yuntun-cli [--addr <host:port>] query <SQL> [--format table|csv|json]
  yuntun-cli [--addr <host:port>] insert -t <table> [-f <file>]
                                    [--format csv|jsonl|parquet] [--shard <shard>] [--key <k>]
  yuntun-cli [--addr <host:port>] tables
  yuntun-cli [--addr <host:port>] schema <table>

说明:
  --addr   默认取环境变量 YUNTUN_ADDR，否则 127.0.0.1:50051
  insert   无 -f 时从 stdin 读（CSV，列顺序需与表一致）；文件导入按列名对齐
  query    --format 默认 table（另外支持 csv / json 行式输出）

示例:
  yuntun-cli query 'SELECT count(*) AS c FROM yuntun.public.cpu'
  yuntun-cli insert -t cpu -f data.csv --shard s0
  yuntun-cli schema cpu
";

#[tokio::main]
async fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if let Err(e) = run(argv).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run(argv: Vec<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (addr, rest) = split_addr(&argv)?;
    let Some(cmd) = rest.first().map(String::as_str) else {
        print!("{USAGE}");
        return Ok(());
    };
    let (pos, opts) = parse_opts(&rest[1..]);
    match cmd {
        "help" | "--help" | "-h" => {
            print!("{USAGE}");
            Ok(())
        }
        "query" => {
            let sql = pos
                .first()
                .ok_or("query 需要 SQL 参数：yuntun-cli query 'SELECT ...'")?;
            let client = Client::connect(&addr).await?;
            let batches = client.query(sql).await?;
            if batches.is_empty() {
                // DDL / INSERT 等无结果集语句（服务端回 Affected，简易轨无行）
                println!("OK");
                return Ok(());
            }
            match opts.get("format").map(String::as_str).unwrap_or("table") {
                "table" => {
                    let mut out = std::io::stdout();
                    arrow::util::pretty::print_batches(&batches)?;
                    let _ = out.flush();
                }
                "csv" => {
                    let mut w = arrow::csv::Writer::new(std::io::stdout());
                    for b in &batches {
                        w.write(b)?;
                    }
                }
                "json" => {
                    let mut w = arrow::json::writer::LineDelimitedWriter::new(std::io::stdout());
                    for b in &batches {
                        w.write(b)?;
                    }
                    w.finish()?;
                }
                other => return Err(format!("未知输出格式 {other}（table|csv|json）").into()),
            }
            Ok(())
        }
        "insert" => {
            let table = opts
                .get("table")
                .ok_or("insert 需要 -t <table>")?
                .to_string();
            let client = Client::connect(&addr).await?;
            // 目标表 schema：取不到说明表不存在（CREATE TABLE 请先执行）
            let target = client.table_schema(&table).await.map_err(|e| {
                format!("读取表 {table} schema 失败（表是否已创建？）：{e}")
            })?;
            let file: Option<PathBuf> = opts.get("file").map(PathBuf::from);
            let format = match opts.get("format") {
                Some(f) => InputFormat::parse(f)
                    .ok_or_else(|| format!("未知导入格式 {f}（csv|jsonl|parquet）"))?,
                None => match &file {
                    Some(p) => InputFormat::from_path(p),
                    None => InputFormat::Csv,
                },
            };
            let batches = match &file {
                Some(p) => read_file(p, format, &target)?,
                None => {
                    let stdin = std::io::stdin();
                    read_stream(stdin.lock(), format, &target)?
                }
            };
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            let shard = opts
                .get("shard")
                .cloned()
                .unwrap_or_else(|| yuntun_client::DEFAULT_SHARD.to_string());
            let key = opts.get("key").cloned();
            let receipts = client
                .insert_batches(&table, &shard, batches.clone(), key)
                .await?;
            let visible = receipts
                .first()
                .map(|r| r.expected_visible_in_secs)
                .unwrap_or(0);
            println!(
                "inserted {rows} rows into {table} (shard={shard}, batches={}, wal_seq_last={})",
                receipts.len(),
                receipts.last().map(|r| r.wal_seq).unwrap_or(0)
            );
            println!("数据约 {visible} 秒内可查（读己之写：攒批内存视图；落对象存储另有攒批窗口）");
            Ok(())
        }
        "tables" => {
            let client = Client::connect(&addr).await?;
            for t in client.list_tables().await? {
                println!("{t}");
            }
            Ok(())
        }
        "schema" => {
            let table = pos.first().ok_or("schema 需要表名：yuntun-cli schema cpu")?;
            let client = Client::connect(&addr).await?;
            let schema = client.table_schema(table).await?;
            print_schema(&schema);
            Ok(())
        }
        other => Err(format!("未知子命令 {other}\n\n{USAGE}").into()),
    }
}

/// 取出全局 `--addr`（其余原样返回）。
fn split_addr(argv: &[String]) -> Result<(String, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
    let mut addr = std::env::var("YUNTUN_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".to_string());
    let mut rest = Vec::new();
    let mut it = argv.iter();
    while let Some(a) = it.next() {
        if a == "--addr" {
            addr = it.next().ok_or("--addr 缺少值")?.clone();
        } else if let Some(v) = a.strip_prefix("--addr=") {
            addr = v.to_string();
        } else {
            rest.push(a.clone());
        }
    }
    Ok((addr, rest))
}

/// 子命令选项解析：返回（位置参数，flag 表）。支持 `-t x` / `--table x` / `--table=x`。
fn parse_opts(args: &[String]) -> (Vec<String>, HashMap<String, String>) {
    const FLAGS: &[(&str, &str)] = &[
        ("-t", "table"),
        ("--table", "table"),
        ("-f", "file"),
        ("--file", "file"),
        ("--format", "format"),
        ("--shard", "shard"),
        ("--key", "key"),
    ];
    let mut pos = Vec::new();
    let mut opts = HashMap::new();
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let mut matched = false;
        for (flag, key) in FLAGS {
            if a == flag {
                if let Some(v) = it.next() {
                    opts.insert((*key).to_string(), v.clone());
                }
                matched = true;
                break;
            }
            if let Some(v) = a.strip_prefix(&format!("{flag}=")) {
                opts.insert((*key).to_string(), v.to_string());
                matched = true;
                break;
            }
        }
        if !matched {
            pos.push(a.clone());
        }
    }
    (pos, opts)
}

fn print_schema(schema: &SchemaRef) {
    println!("{:<24} {:<16} NULLABLE", "COLUMN", "TYPE");
    for f in schema.fields() {
        println!(
            "{:<24} {:<16} {}",
            f.name(),
            f.data_type(),
            if f.is_nullable() { "YES" } else { "NO" }
        );
    }
    let _ = Path::new("");
}
