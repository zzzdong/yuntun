//! `yuntun-cli`：yuntun 客户端 CLI（S1.9）。
//!
//! 子命令：`query` / `insert` / `tables` / `schema` —— 用法见 `yuntun-cli --help`
//! （由 clap 从本文件的 doc 注释生成，不再手写 USAGE 常量：手写的文案与实现迟早漂移）。
//!
//! 解析改由 clap 承担后，**接受的输入是原来的超集**：
//!
//! | 能力 | 之前 | 现在 |
//! |---|---|---|
//! | `--addr a` / `--addr=a` | 都支持（手写） | 都支持（clap） |
//! | `YUNTUN_ADDR` 环境变量 | 支持 | 支持（`#[arg(env)]`） |
//! | `--addr` 放在子命令前后 | 支持（全 argv 扫描） | 支持（`global = true`） |
//! | `--format` 大小写 | 不敏感 | 不敏感（`ignore_case`） |
//! | `-h/--help`、拼错的 flag、缺值 | 无 / 报错含糊 | clap 生成，**指出是哪个参数** |

use std::io::Write;
use std::path::PathBuf;

use arrow::datatypes::SchemaRef;
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use yuntun_client::input::{read_file, read_stream};
use yuntun_client::{Client, InputFormat};

#[derive(Parser, Debug)]
#[command(
    name = "yuntun-cli",
    version,
    about = "yuntun 客户端（Arrow Flight 简易轨）",
    after_help = "示例:\n  \
                  yuntun-cli query 'SELECT count(*) AS c FROM yuntun.public.cpu'\n  \
                  yuntun-cli insert -t cpu -f data.csv --shard s0\n  \
                  yuntun-cli schema cpu"
)]
struct Cli {
    /// 服务地址（默认取环境变量 YUNTUN_ADDR，否则 127.0.0.1:50051）
    ///
    /// `global`：可以放在子命令前面或后面，两种写法都行。
    #[arg(long, env = "YUNTUN_ADDR", default_value = "127.0.0.1:50051", global = true)]
    addr: String,

    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// 执行 SQL；有结果集时按 --format 输出（DDL/INSERT 等无结果集语句打印 OK）
    Query {
        /// SQL 文本
        sql: String,
        /// 结果输出格式
        #[arg(long, value_enum, ignore_case = true, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
    },

    /// 导入数据（无 -f 时从 stdin 读，列顺序需与表一致）
    Insert {
        /// 目标表
        #[arg(short, long)]
        table: String,
        /// 输入文件；省略则读 stdin（CSV）
        #[arg(short, long)]
        file: Option<PathBuf>,
        /// 输入格式；省略则按文件扩展名推断（stdin 默认 csv）
        #[arg(long, value_enum, ignore_case = true)]
        format: Option<InputFormatArg>,
        /// 分片名
        #[arg(long)]
        shard: Option<String>,
        /// 幂等键（同键重试会被服务端去重）
        #[arg(long)]
        key: Option<String>,
    },

    /// 列出所有表
    Tables,

    /// 打印表结构
    Schema {
        /// 表名
        table: String,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum OutputFormat {
    Table,
    Csv,
    Json,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum InputFormatArg {
    Csv,
    /// 行式 JSON（别名：json / ndjson —— 与 `InputFormat::parse` 接受的一致）
    #[value(alias = "json", alias = "ndjson")]
    Jsonl,
    Parquet,
}

impl From<InputFormatArg> for InputFormat {
    fn from(v: InputFormatArg) -> Self {
        match v {
            InputFormatArg::Csv => InputFormat::Csv,
            InputFormatArg::Jsonl => InputFormat::Jsonl,
            InputFormatArg::Parquet => InputFormat::Parquet,
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = cli.addr;
    let Some(cmd) = cli.cmd else {
        // 没给子命令 = 想看用法。这**不是错误**（退 0），但帮助走 stderr 更符合直觉？
        // 不：`--help` 走 stdout（可管道），裸调用与之保持一致。
        let mut help = Cli::command();
        help.print_help()?;
        println!();
        return Ok(());
    };

    match cmd {
        Cmd::Query { sql, format } => {
            let client = Client::connect(&addr).await?;
            let batches = client.query(&sql).await?;
            if batches.is_empty() {
                // DDL / INSERT 等无结果集语句（服务端回 Affected，简易轨无行）
                println!("OK");
                return Ok(());
            }
            match format {
                OutputFormat::Table => {
                    let mut out = std::io::stdout();
                    arrow::util::pretty::print_batches(&batches)?;
                    let _ = out.flush();
                }
                OutputFormat::Csv => {
                    let mut w = arrow::csv::Writer::new(std::io::stdout());
                    for b in &batches {
                        w.write(b)?;
                    }
                }
                OutputFormat::Json => {
                    let mut w = arrow::json::writer::LineDelimitedWriter::new(std::io::stdout());
                    for b in &batches {
                        w.write(b)?;
                    }
                    w.finish()?;
                }
            }
            Ok(())
        }

        Cmd::Insert {
            table,
            file,
            format,
            shard,
            key,
        } => {
            let client = Client::connect(&addr).await?;
            // 目标表 schema：取不到说明表不存在（CREATE TABLE 请先执行）
            let target = client.table_schema(&table).await.map_err(|e| {
                format!("读取表 {table} schema 失败（表是否已创建？）：{e}")
            })?;
            // 显式 --format 优先；否则按文件扩展名推断（stdin 默认 csv）
            let format = match format {
                Some(f) => f.into(),
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
            let shard = shard.unwrap_or_else(|| yuntun_client::DEFAULT_SHARD.to_string());
            let receipts = client
                .insert_batches(&table, &shard, batches.clone(), key)
                .await?;
            let visible = receipts
                .first()
                .map(|r| r.expected_visible_in_secs)
                .unwrap_or(0);
            // 实际写入行数来自回执（**不是**输入行数）：同键重试会被幂等去重，
            // 此时回执 row_count=0 且 duplicate=true —— 报"inserted N rows"会骗人。
            let written: u64 = receipts.iter().map(|r| r.row_count).sum();
            let deduped = receipts.iter().filter(|r| r.duplicate).count();
            println!(
                "inserted {written} rows into {table} (shard={shard}, batches={}, wal_seq_last={})",
                receipts.len(),
                receipts.last().map(|r| r.wal_seq).unwrap_or(0)
            );
            if deduped > 0 {
                println!(
                    "其中 {deduped} 个批次被幂等去重（同键请求已落库，{rows} 行输入未重复写入）"
                );
            }
            if written > 0 {
                println!("数据约 {visible} 秒内可查（读己之写：攒批内存视图；落对象存储另有攒批窗口）");
            }
            Ok(())
        }

        Cmd::Tables => {
            let client = Client::connect(&addr).await?;
            for t in client.list_tables().await? {
                println!("{t}");
            }
            Ok(())
        }

        Cmd::Schema { table } => {
            let client = Client::connect(&addr).await?;
            let schema = client.table_schema(&table).await?;
            print_schema(&schema);
            Ok(())
        }
    }
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
}
