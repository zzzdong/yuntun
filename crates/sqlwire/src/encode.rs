//! Arrow → MySQL 协议值编码（sql-access-design.md §四：适配层职责）。
//!
//! 结果集列元数据（Column）与行值（write_col）都由 Arrow schema/数组驱动；
//! 日期/时间戳/decimal 以 **字符串** 形态回传（MySQL 文本协议自然支持，
//! 与 yuntun-sql `params.rs` 的 TypedString 解析路径互逆）。

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use opensrv_mysql::{Column, ColumnFlags, ColumnType, RowWriter};

/// Arrow schema → MySQL 结果集列元数据。
pub fn columns_of(schema: &SchemaRef) -> Vec<Column> {
    schema
        .fields()
        .iter()
        .map(|f| Column {
            table: String::new(),
            column: f.name().clone(),
            coltype: arrow_to_mysql_type(f.data_type()),
            colflags: if f.is_nullable() {
                ColumnFlags::empty()
            } else {
                ColumnFlags::NOT_NULL_FLAG
            },
        })
        .collect()
}

/// Arrow → MySQL 列类型声明（与 yuntun-sql `SqlEngine::arrow_to_mysql_type`
/// 的文本声明对应：同源映射，此处产出协议常量）。
pub fn arrow_to_mysql_type(dt: &DataType) -> ColumnType {
    match dt {
        DataType::Boolean
        | DataType::Int8
        | DataType::UInt8 => ColumnType::MYSQL_TYPE_TINY,
        DataType::Int16 | DataType::UInt16 => ColumnType::MYSQL_TYPE_SHORT,
        DataType::Int32 | DataType::UInt32 => ColumnType::MYSQL_TYPE_LONG,
        DataType::Int64 | DataType::UInt64 => ColumnType::MYSQL_TYPE_LONGLONG,
        DataType::Float32 => ColumnType::MYSQL_TYPE_FLOAT,
        DataType::Float64 => ColumnType::MYSQL_TYPE_DOUBLE,
        DataType::Utf8 | DataType::LargeUtf8 => ColumnType::MYSQL_TYPE_VAR_STRING,
        DataType::Binary | DataType::LargeBinary => ColumnType::MYSQL_TYPE_BLOB,
        DataType::Date32 | DataType::Date64 => ColumnType::MYSQL_TYPE_DATE,
        DataType::Timestamp(_, _) => ColumnType::MYSQL_TYPE_DATETIME,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            ColumnType::MYSQL_TYPE_NEWDECIMAL
        }
        _ => ColumnType::MYSQL_TYPE_VAR_STRING,
    }
}

/// 把单个批次的 `row` 行写入 wire 行写入器（列顺序 = schema 顺序）。
pub async fn write_row<W: tokio::io::AsyncWrite + Send + Unpin>(
    rw: &mut RowWriter<'_, W>,
    batch: &arrow::record_batch::RecordBatch,
    schema: &SchemaRef,
    row: usize,
) -> std::io::Result<()> {
    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        let dt = field.data_type();
        if col.is_null(row) {
            rw.write_col(Option::<i64>::None)?;
            continue;
        }
        match dt {
            DataType::Boolean => {
                let v = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                rw.write_col(v.value(row) as i64)?;
            }
            DataType::Int8 => {
                let v = col.as_any().downcast_ref::<Int8Array>().unwrap();
                rw.write_col(v.value(row) as i64)?;
            }
            DataType::Int16 => {
                let v = col.as_any().downcast_ref::<Int16Array>().unwrap();
                rw.write_col(v.value(row) as i64)?;
            }
            DataType::Int32 => {
                let v = col.as_any().downcast_ref::<Int32Array>().unwrap();
                rw.write_col(v.value(row) as i64)?;
            }
            DataType::Int64 => {
                let v = col.as_any().downcast_ref::<Int64Array>().unwrap();
                rw.write_col(v.value(row))?;
            }
            DataType::UInt8 => {
                let v = col.as_any().downcast_ref::<UInt8Array>().unwrap();
                rw.write_col(v.value(row) as u64)?;
            }
            DataType::UInt16 => {
                let v = col.as_any().downcast_ref::<UInt16Array>().unwrap();
                rw.write_col(v.value(row) as u64)?;
            }
            DataType::UInt32 => {
                let v = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                rw.write_col(v.value(row) as u64)?;
            }
            DataType::UInt64 => {
                let v = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                rw.write_col(v.value(row))?;
            }
            DataType::Float32 => {
                let v = col.as_any().downcast_ref::<Float32Array>().unwrap();
                rw.write_col(v.value(row) as f64)?;
            }
            DataType::Float64 => {
                let v = col.as_any().downcast_ref::<Float64Array>().unwrap();
                rw.write_col(v.value(row))?;
            }
            DataType::Utf8 => {
                let v = col.as_any().downcast_ref::<StringArray>().unwrap();
                rw.write_col(v.value(row).to_string())?;
            }
            DataType::LargeUtf8 => {
                let v = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
                rw.write_col(v.value(row).to_string())?;
            }
            DataType::Binary => {
                let v = col.as_any().downcast_ref::<BinaryArray>().unwrap();
                rw.write_col(v.value(row).to_vec())?;
            }
            DataType::LargeBinary => {
                let v = col.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                rw.write_col(v.value(row).to_vec())?;
            }
            DataType::Date32 => {
                let v = col.as_any().downcast_ref::<Date32Array>().unwrap();
                rw.write_col(format_date(v.value(row)))?;
            }
            DataType::Date64 => {
                // Date64 = epoch 毫秒（Date64Array）
                let v = col.as_any().downcast_ref::<Date64Array>().unwrap();
                rw.write_col(format_date((v.value(row) / 86_400_000) as i32))?;
            }
            DataType::Timestamp(unit, _) => {
                rw.write_col(format_timestamp_array(col.as_ref(), unit, row))?;
            }
            DataType::Decimal128(_, scale) => {
                let v = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
                rw.write_col(format_decimal(v.value(row), *scale))?;
            }
            // 兜底：Display 转字符串（与 SHOW COLUMNS 的 text 回退一致）
            _ => {
                let s = arrow::util::display::array_value_to_string(col, row)
                    .unwrap_or_default();
                rw.write_col(s)?;
            }
        }
    }
    rw.end_row().await?;
    Ok(())
}

fn format_timestamp_array(
    col: &dyn Array,
    unit: &TimeUnit,
    row: usize,
) -> String {
    match unit {
        TimeUnit::Second => {
            let a = col
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .expect("timestamp second array");
            format_ts(a.value(row), 0)
        }
        TimeUnit::Millisecond => {
            let a = col
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("timestamp millisecond array");
            let v = a.value(row);
            format_ts(v.div_euclid(1_000_000), (v.rem_euclid(1_000_000) * 1000) as u32)
        }
        TimeUnit::Microsecond => {
            let a = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("timestamp microsecond array");
            let v = a.value(row);
            format_ts(v.div_euclid(1_000_000), v.rem_euclid(1_000_000) as u32)
        }
        TimeUnit::Nanosecond => {
            let a = col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("timestamp nanosecond array");
            let v = a.value(row);
            format_ts(
                v.div_euclid(1_000_000_000),
                (v.rem_euclid(1_000_000_000) / 1000) as u32,
            )
        }
    }
}

/// epoch 秒 + 微秒余数 → `YYYY-MM-DD HH:MM:SS[.ffffff]`（无 chrono 依赖，
/// civil 算法与 yuntun-sql params.rs 的解析路径互逆）。
pub fn format_ts(secs: i64, micros: u32) -> String {
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    let tod = secs.rem_euclid(86_400);
    let (hh, mm, ss) = (tod / 3600, (tod % 3600) / 60, tod % 60);
    if micros == 0 {
        format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}")
    } else {
        format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}.{micros:06}")
    }
}

/// Date32（epoch 天数）→ `YYYY-MM-DD`。
pub fn format_date(days: i32) -> String {
    let (y, m, d) = civil_from_days(days as i64);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Howard Hinnant `civil_from_days`：epoch 天数 → (年, 月, 日)。
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Decimal128（未缩放值 + 小数位）→ 定点字符串。
pub fn format_decimal(v: i128, scale: i8) -> String {
    if scale <= 0 {
        return v.to_string();
    }
    let scale = scale as u32;
    let neg = v < 0;
    let a = v.unsigned_abs();
    let p = 10u128.pow(scale);
    let int = a / p;
    let frac = a % p;
    let sign = if neg { "-" } else { "" };
    format!("{sign}{int}.{frac:0scale$}", scale = scale as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_formatting() {
        assert_eq!(format_date(0), "1970-01-01");
        assert_eq!(format_date(19_000), "2022-01-08");
        assert_eq!(format_date(-1), "1969-12-31");
        // 闰日：2024-02-29 = 19782 天（1970-01-01 起算；19723 = 2024-01-01）
        assert_eq!(format_date(19_782), "2024-02-29");
    }

    #[test]
    fn timestamp_formatting() {
        assert_eq!(format_ts(0, 0), "1970-01-01 00:00:00");
        assert_eq!(
            format_ts(1_641_600_000, 500_000),
            "2022-01-08 00:00:00.500000"
        );
        assert_eq!(format_ts(86_399, 0), "1970-01-01 23:59:59");
        // 闰年 2024-02-29 12:34:56
        assert_eq!(
            format_ts(19_782 * 86_400 + 45_296, 0),
            "2024-02-29 12:34:56"
        );
    }

    #[test]
    fn decimal_formatting() {
        assert_eq!(format_decimal(12345, 3), "12.345");
        assert_eq!(format_decimal(-12345, 2), "-123.45");
        assert_eq!(format_decimal(42, 0), "42");
        assert_eq!(format_decimal(1, 6), "0.000001");
    }

    #[test]
    fn column_type_mapping() {
        assert_eq!(
            arrow_to_mysql_type(&DataType::Int64),
            ColumnType::MYSQL_TYPE_LONGLONG
        );
        assert_eq!(
            arrow_to_mysql_type(&DataType::Utf8),
            ColumnType::MYSQL_TYPE_VAR_STRING
        );
        assert_eq!(
            arrow_to_mysql_type(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            ColumnType::MYSQL_TYPE_DATETIME
        );
    }
}
