//! 导入文件解析（S1.9）：CSV / JSONL / Parquet → `RecordBatch`。
//!
//! 统一约定：解析结果按**列名**对齐到目标表 schema 并做类型 cast
//! （`arrow::compute::cast`），因此输入文件的列顺序可以与表不同；
//! 缺列报错、多列忽略。

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::file::reader::ChunkReader;

use crate::{ClientError, Result};

/// 导入格式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputFormat {
    Csv,
    Jsonl,
    Parquet,
}

impl InputFormat {
    /// 解析 `--format` 取值。
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "csv" => Some(Self::Csv),
            "jsonl" | "json" | "ndjson" => Some(Self::Jsonl),
            "parquet" => Some(Self::Parquet),
            _ => None,
        }
    }

    /// 按扩展名猜测。
    pub fn from_path(p: &Path) -> Self {
        match p
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_ascii_lowercase())
            .as_deref()
        {
            Some("jsonl") | Some("ndjson") | Some("json") => Self::Jsonl,
            Some("parquet") => Self::Parquet,
            _ => Self::Csv,
        }
    }
}

/// 读取文件并解析为对齐 `target` 的批次。
pub fn read_file(path: &Path, format: InputFormat, target: &SchemaRef) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    read_seekable(file, format, target)
}

/// 可 Seek 输入（文件）：CSV 按 header 推断类型，列顺序无关。
pub fn read_seekable<R: Read + Seek + ChunkReader + 'static>(
    reader: R,
    format: InputFormat,
    target: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    match format {
        InputFormat::Csv => read_csv(reader, target),
        InputFormat::Jsonl => {
            let r =
                arrow::json::ReaderBuilder::new(target.clone()).build(BufReader::new(reader))?;
            align_all(r, target)
        }
        InputFormat::Parquet => read_parquet(reader, target),
    }
}

/// 不可 Seek 的流（stdin）：CSV 按 `target` 逐列解析（**列顺序须与表一致**）。
pub fn read_stream<R: Read>(
    reader: R,
    format: InputFormat,
    target: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    match format {
        InputFormat::Csv => {
            let r = arrow::csv::ReaderBuilder::new(target.clone())
                .with_header(true)
                .build(reader)?;
            align_all(r, target)
        }
        InputFormat::Jsonl => {
            let r =
                arrow::json::ReaderBuilder::new(target.clone()).build(BufReader::new(reader))?;
            align_all(r, target)
        }
        InputFormat::Parquet => Err(ClientError::Other(
            "parquet 导入需要文件路径（流式输入不支持随机读取）".into(),
        )),
    }
}

fn read_csv<R: Read + Seek>(mut reader: R, target: &SchemaRef) -> Result<Vec<RecordBatch>> {
    // 先按 header 推断列类型（与列顺序无关），再重置重读
    let (inferred, _) = arrow::csv::reader::Format::default()
        .with_header(true)
        .infer_schema(&mut reader, Some(1024))?;
    reader.seek(SeekFrom::Start(0))?;
    let r = arrow::csv::ReaderBuilder::new(Arc::new(inferred))
        .with_header(true)
        .build(reader)?;
    align_all(r, target)
}

fn read_parquet<R: Read + Seek + ChunkReader + 'static>(
    reader: R,
    target: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(reader)?;
    let reader = builder.build()?;
    let mut out = Vec::new();
    for b in reader {
        out.push(align_to_schema(&b?, target)?);
    }
    Ok(out)
}

fn align_all<I>(iter: I, target: &SchemaRef) -> Result<Vec<RecordBatch>>
where
    I: Iterator<Item = std::result::Result<RecordBatch, arrow::error::ArrowError>>,
{
    let mut out = Vec::new();
    for b in iter {
        out.push(align_to_schema(&b?, target)?);
    }
    Ok(out)
}

/// 按列名把 `batch` 对齐（并 cast）到 `target` schema。
pub fn align_to_schema(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    let mut cols = Vec::with_capacity(target.fields().len());
    let input_schema = batch.schema();
    for f in target.fields() {
        let idx = input_schema.index_of(f.name()).map_err(|_| {
            let have: Vec<&str> = input_schema
                .fields()
                .iter()
                .map(|x| x.name().as_str())
                .collect();
            ClientError::Other(format!(
                "输入缺少列 {}（输入列: {have:?}，表列: {:?}）",
                f.name(),
                target.fields().iter().map(|x| x.name()).collect::<Vec<_>>()
            ))
        })?;
        let col = batch.column(idx);
        let col = if col.data_type() == f.data_type() {
            col.clone()
        } else {
            arrow::compute::cast(col, f.data_type())?
        };
        cols.push(col);
    }
    Ok(RecordBatch::try_new(target.clone(), cols)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use arrow::array::{Array, Float64Array, Int64Array, StringArray};

    fn target() -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("ts", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("host", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("usage", arrow::datatypes::DataType::Float64, true),
        ]))
    }

    fn tmp(name: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("yuntun-client-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn csv_import_aligns_by_column_name() {
        let path = tmp("import.csv");
        let mut f = File::create(&path).unwrap();
        // 列顺序与表不同（host, usage, ts）→ 应按名对齐
        writeln!(f, "host,usage,ts").unwrap();
        writeln!(f, "a,1.5,100").unwrap();
        writeln!(f, "b,2.5,200").unwrap();
        drop(f);

        let batches = read_file(&path, InputFormat::Csv, &target()).unwrap();
        assert_eq!(batches.len(), 1);
        let b = &batches[0];
        assert_eq!(b.schema().field(0).name(), "ts");
        assert_eq!(
            b.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0),
            100
        );
        assert_eq!(
            b.column(1).as_any().downcast_ref::<StringArray>().unwrap().value(1),
            "b"
        );
        assert_eq!(
            b.column(2).as_any().downcast_ref::<Float64Array>().unwrap().value(1),
            2.5
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn jsonl_import_aligns_by_column_name() {
        let path = tmp("import.jsonl");
        let mut f = File::create(&path).unwrap();
        writeln!(f, r#"{{"ts":1,"host":"a","usage":0.5}}"#).unwrap();
        writeln!(f, r#"{{"host":"b","ts":2}}"#).unwrap(); // 缺 usage → NULL
        drop(f);

        let batches = read_file(&path, InputFormat::Jsonl, &target()).unwrap();
        let b = &batches[0];
        assert_eq!(b.num_rows(), 2);
        let usage = b.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(usage.value(0), 0.5);
        assert!(usage.is_null(1), "缺字段应填 NULL");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn parquet_import_casts_to_table_schema() {
        let path = tmp("import.parquet");
        // 写一个列顺序不同、且 usage 为 Int32 的 parquet（读取时需 cast 到 Float64）
        let src = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("host", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("ts", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("usage", arrow::datatypes::DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            src.clone(),
            vec![
                Arc::new(StringArray::from(vec!["x"])) as arrow::array::ArrayRef,
                Arc::new(Int64Array::from(vec![42])) as arrow::array::ArrayRef,
                Arc::new(arrow::array::Int32Array::from(vec![3])) as arrow::array::ArrayRef,
            ],
        )
        .unwrap();
        {
            let file = File::create(&path).unwrap();
            let mut w = parquet::arrow::ArrowWriter::try_new(file, src, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }

        let batches = read_file(&path, InputFormat::Parquet, &target()).unwrap();
        let b = &batches[0];
        assert_eq!(b.schema().field(0).name(), "ts");
        assert_eq!(
            b.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0),
            42
        );
        assert_eq!(
            b.column(2).as_any().downcast_ref::<Float64Array>().unwrap().value(0),
            3.0
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn format_parsing() {
        assert_eq!(InputFormat::parse("CSV"), Some(InputFormat::Csv));
        assert_eq!(InputFormat::parse("jsonl"), Some(InputFormat::Jsonl));
        assert_eq!(InputFormat::parse("parquet"), Some(InputFormat::Parquet));
        assert_eq!(InputFormat::parse("orc"), None);
        assert_eq!(
            InputFormat::from_path(Path::new("/tmp/a.parquet")),
            InputFormat::Parquet
        );
        assert_eq!(InputFormat::from_path(Path::new("a.jsonl")), InputFormat::Jsonl);
        assert_eq!(InputFormat::from_path(Path::new("a.csv")), InputFormat::Csv);
    }
}
