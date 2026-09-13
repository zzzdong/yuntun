//! Arrow 批对齐工具：把任意 schema_version 的批次对齐到目标 schema。
//!
//! 缺失列填 null、列序重排到目标 schema、类型不一致时按提升格 cast（§6.8）。
//! 写入（flush）与查询（内存未落盘数据）两条路径共用同一实现，避免语义漂移。

use arrow::record_batch::RecordBatch;

use crate::error::LakeError;

/// 列名对齐：缺失列填 null；列序重排到目标 schema；类型不一致时按提升格 cast。
pub fn align_batch(
    batch: &RecordBatch,
    target: &arrow::datatypes::SchemaRef,
) -> Result<RecordBatch, LakeError> {
    use arrow::array::new_null_array;
    use arrow::compute::cast;
    let mut cols = Vec::with_capacity(target.fields().len());
    for f in target.fields() {
        match batch.schema().column_with_name(f.name()) {
            Some((idx, bf)) => {
                let arr = batch.column(idx);
                if bf.data_type() == f.data_type() {
                    cols.push(arr.clone());
                } else {
                    // 类型宽化 cast（Int32→Int64→Float64）；不兼容时 cast 报错 → 整批失败
                    cols.push(cast(arr, f.data_type()).map_err(|e| {
                        LakeError::Other(format!(
                            "column {} cast {:?} -> {:?}: {e}",
                            f.name(),
                            bf.data_type(),
                            f.data_type()
                        ))
                    })?);
                }
            }
            None => cols.push(new_null_array(f.data_type(), batch.num_rows())),
        }
    }
    RecordBatch::try_new(target.clone(), cols).map_err(|e| LakeError::Other(format!("align: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn b(schema: arrow::datatypes::SchemaRef, arr: Arc<dyn Array>) -> RecordBatch {
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    #[test]
    fn fills_missing_columns_with_null() {
        let v1 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let v2 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let out = align_batch(&b(v1, Arc::new(Int64Array::from(vec![1]))), &v2).unwrap();
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.column(1).null_count(), out.num_rows());
    }

    #[test]
    fn widens_int32_to_int64() {
        let s32 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let s64 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let out = align_batch(&b(s32, Arc::new(Int32Array::from(vec![1, 2]))), &s64).unwrap();
        assert_eq!(out.column(0).data_type(), &DataType::Int64);
    }
}
