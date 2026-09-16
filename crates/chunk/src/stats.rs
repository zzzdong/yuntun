//! chunk 级列统计（架构 §2.1 `stats: ColumnStats`）：min / max / null_count。
//!
//! 用途是 **chunk 级跳过**：谓词与 chunk 的 `[min, max]` 不相交时整块跳过，
//! 不必把数据拉回来再丢弃（架构 §4.3 owner 侧过滤同理）。
//!
//! 只对能可靠给出全序的类型计算 min/max（数值 / 时间戳 / 字符串 / 布尔），
//! 其余类型（嵌套、二进制等）留空 —— **宁可不剪枝，也不给错答案**。

use arrow::array::{
    Array, BooleanArray, LargeStringArray, PrimitiveArray, StringArray, StringViewArray,
};
use arrow::compute::kernels::aggregate;
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow::record_batch::RecordBatch;

/// 可比较的统计标量。
#[derive(Debug, Clone, PartialEq)]
pub enum StatValue {
    Int(i64),
    Uint(u64),
    Float(f64),
    Str(String),
    Bool(bool),
}

impl std::fmt::Display for StatValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StatValue::Int(v) => write!(f, "{v}"),
            StatValue::Uint(v) => write!(f, "{v}"),
            StatValue::Float(v) => write!(f, "{v}"),
            StatValue::Str(v) => write!(f, "{v}"),
            StatValue::Bool(v) => write!(f, "{v}"),
        }
    }
}

/// 单列统计。
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStat {
    pub name: String,
    pub min: Option<StatValue>,
    pub max: Option<StatValue>,
    pub null_count: usize,
}

/// chunk 级统计集合。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ColumnStats {
    pub columns: Vec<ColumnStat>,
}

impl ColumnStats {
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn get(&self, name: &str) -> Option<&ColumnStat> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// 从一批数据计算统计。
    pub fn from_batch(batch: &RecordBatch) -> Self {
        let schema = batch.schema();
        let mut columns = Vec::with_capacity(batch.num_columns());
        for (i, field) in schema.fields().iter().enumerate() {
            let arr = batch.column(i);
            let (min, max) = min_max(arr.as_ref());
            columns.push(ColumnStat {
                name: field.name().clone(),
                min,
                max,
                null_count: arr.null_count(),
            });
        }
        Self { columns }
    }

    /// 合并另一份统计（同一 chunk 内多批次累加）：
    /// min 取更小、max 取更大、null_count 累加。
    pub fn merge(&mut self, other: &ColumnStats) {
        if self.columns.is_empty() {
            self.columns = other.columns.clone();
            return;
        }
        for incoming in &other.columns {
            match self.columns.iter_mut().find(|c| c.name == incoming.name) {
                Some(existing) => {
                    existing.min = merge_min(existing.min.take(), incoming.min.clone());
                    existing.max = merge_max(existing.max.take(), incoming.max.clone());
                    existing.null_count += incoming.null_count;
                }
                None => self.columns.push(incoming.clone()),
            }
        }
    }

    /// 一次批量输入的多批次合并（chunk append 的便捷入口）。
    pub fn merge_batch(&mut self, batch: &RecordBatch) {
        self.merge(&ColumnStats::from_batch(batch));
    }
}

/// 同类型可比才合并；类型不同（schema 演进）时保留原值，避免给出错误区间。
fn merge_min(a: Option<StatValue>, b: Option<StatValue>) -> Option<StatValue> {
    match (a, b) {
        (Some(a), Some(b)) => cmp_values(&a, &b).map(|ord| if ord.is_gt() { b } else { a }),
        (Some(a), None) => Some(a),
        (None, b) => b,
    }
}

fn merge_max(a: Option<StatValue>, b: Option<StatValue>) -> Option<StatValue> {
    match (a, b) {
        (Some(a), Some(b)) => cmp_values(&a, &b).map(|ord| if ord.is_lt() { b } else { a }),
        (Some(a), None) => Some(a),
        (None, b) => b,
    }
}

/// 同类型比较（`None` = 不可比）。
///
/// `NaN` 不参与比较（返回 `None`）：NaN 的 min/max 没有意义，
/// 给不出区间比给出错误区间安全。
pub fn cmp_values(a: &StatValue, b: &StatValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (StatValue::Int(a), StatValue::Int(b)) => Some(a.cmp(b)),
        (StatValue::Uint(a), StatValue::Uint(b)) => Some(a.cmp(b)),
        (StatValue::Int(a), StatValue::Uint(b)) => i64::try_from(*b).ok().map(|b| a.cmp(&b)),
        (StatValue::Uint(a), StatValue::Int(b)) => i64::try_from(*a).ok().map(|a| a.cmp(b)),
        (StatValue::Float(a), StatValue::Float(b)) => a.partial_cmp(b),
        (StatValue::Str(a), StatValue::Str(b)) => Some(a.cmp(b)),
        (StatValue::Bool(a), StatValue::Bool(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

macro_rules! prim_min_max {
    ($arr:expr, $ty:ty, $wrap:expr) => {{
        match $arr.as_any().downcast_ref::<PrimitiveArray<$ty>>() {
            Some(a) => (
                aggregate::min(a).map($wrap),
                aggregate::max(a).map($wrap),
            ),
            None => (None, None),
        }
    }};
}

fn min_max(arr: &dyn Array) -> (Option<StatValue>, Option<StatValue>) {
    match arr.data_type() {
        DataType::Int8 => prim_min_max!(arr, Int8Type, |v: i8| StatValue::Int(v as i64)),
        DataType::Int16 => prim_min_max!(arr, Int16Type, |v: i16| StatValue::Int(v as i64)),
        DataType::Int32 => prim_min_max!(arr, Int32Type, |v: i32| StatValue::Int(v as i64)),
        DataType::Int64 => prim_min_max!(arr, Int64Type, |v: i64| StatValue::Int(v)),
        DataType::UInt8 => prim_min_max!(arr, UInt8Type, |v: u8| StatValue::Uint(v as u64)),
        DataType::UInt16 => prim_min_max!(arr, UInt16Type, |v: u16| StatValue::Uint(v as u64)),
        DataType::UInt32 => prim_min_max!(arr, UInt32Type, |v: u32| StatValue::Uint(v as u64)),
        DataType::UInt64 => prim_min_max!(arr, UInt64Type, |v: u64| StatValue::Uint(v)),
        DataType::Float32 => prim_min_max!(arr, Float32Type, |v: f32| StatValue::Float(v as f64)),
        DataType::Float64 => prim_min_max!(arr, Float64Type, |v: f64| StatValue::Float(v)),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => prim_min_max!(arr, TimestampSecondType, |v: i64| StatValue::Int(v)),
            TimeUnit::Millisecond => {
                prim_min_max!(arr, TimestampMillisecondType, |v: i64| StatValue::Int(v))
            }
            TimeUnit::Microsecond => {
                prim_min_max!(arr, TimestampMicrosecondType, |v: i64| StatValue::Int(v))
            }
            TimeUnit::Nanosecond => {
                prim_min_max!(arr, TimestampNanosecondType, |v: i64| StatValue::Int(v))
            }
        },
        DataType::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>();
            (
                a.and_then(aggregate::min_boolean).map(StatValue::Bool),
                a.and_then(aggregate::max_boolean).map(StatValue::Bool),
            )
        }
        DataType::Utf8 => str_min_max(arr.as_any().downcast_ref::<StringArray>()),
        DataType::LargeUtf8 => str_min_max(arr.as_any().downcast_ref::<LargeStringArray>()),
        DataType::Utf8View => {
            let Some(a) = arr.as_any().downcast_ref::<StringViewArray>() else {
                return (None, None);
            };
            (
                aggregate::min_string_view(a).map(|s| StatValue::Str(s.to_string())),
                aggregate::max_string_view(a).map(|s| StatValue::Str(s.to_string())),
            )
        }
        _ => (None, None),
    }
}

fn str_min_max<T: arrow::array::OffsetSizeTrait>(
    arr: Option<&arrow::array::GenericStringArray<T>>,
) -> (Option<StatValue>, Option<StatValue>) {
    let Some(a) = arr else {
        return (None, None);
    };
    (
        aggregate::min_string(a).map(|s| StatValue::Str(s.to_string())),
        aggregate::max_string(a).map(|s| StatValue::Str(s.to_string())),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, TimestampMillisecondArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn batch_i64(vals: Vec<Option<i64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vals))]).unwrap()
    }

    #[test]
    fn numeric_min_max_and_null_count() {
        let b = batch_i64(vec![Some(5), None, Some(1), Some(9)]);
        let s = ColumnStats::from_batch(&b);
        assert_eq!(s.get("a").unwrap().min, Some(StatValue::Int(1)));
        assert_eq!(s.get("a").unwrap().max, Some(StatValue::Int(9)));
        assert_eq!(s.get("a").unwrap().null_count, 1);
    }

    #[test]
    fn merge_widens_range_and_sums_nulls() {
        let mut s = ColumnStats::from_batch(&batch_i64(vec![Some(5), Some(7)]));
        s.merge(&ColumnStats::from_batch(&batch_i64(vec![Some(1), None])));
        let c = s.get("a").unwrap();
        assert_eq!(c.min, Some(StatValue::Int(1)));
        assert_eq!(c.max, Some(StatValue::Int(7)));
        assert_eq!(c.null_count, 1);
    }

    #[test]
    fn all_null_column_has_no_range_but_counts_nulls() {
        let b = batch_i64(vec![None, None]);
        let s = ColumnStats::from_batch(&b);
        let c = s.get("a").unwrap();
        assert_eq!(c.min, None);
        assert_eq!(c.max, None);
        assert_eq!(c.null_count, 2);
    }

    #[test]
    fn string_min_max() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let b = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["b", "a", "c"]))],
        )
        .unwrap();
        let s = ColumnStats::from_batch(&b);
        assert_eq!(
            s.get("s").unwrap().min,
            Some(StatValue::Str("a".into()))
        );
        assert_eq!(
            s.get("s").unwrap().max,
            Some(StatValue::Str("c".into()))
        );
    }

    #[test]
    fn nested_type_has_no_stats_instead_of_wrong_stats() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        )]));
        let b = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::ListArray::from_iter_primitive::<
                arrow::datatypes::Int64Type,
                _,
                _,
            >(vec![Some(vec![Some(1i64)])]))],
        )
        .unwrap();
        let s = ColumnStats::from_batch(&b);
        assert_eq!(s.get("l").unwrap().min, None, "嵌套列不给统计而非给错统计");
    }

    #[test]
    fn nan_is_not_comparable() {
        assert_eq!(
            cmp_values(&StatValue::Float(f64::NAN), &StatValue::Float(1.0)),
            None
        );
    }

    #[test]
    fn timestamp_stats_are_computed() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]));
        let b = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampMillisecondArray::from(vec![10i64, 3, 7]))],
        )
        .unwrap();
        let s = ColumnStats::from_batch(&b);
        assert_eq!(s.get("t").unwrap().min, Some(StatValue::Int(3)));
        assert_eq!(s.get("t").unwrap().max, Some(StatValue::Int(10)));
    }
}
