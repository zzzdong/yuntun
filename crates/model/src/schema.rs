//! Schema 演进：变更分类 + 类型提升格（详细设计 §8）。
//!
//! 变更策略（§8.1）：
//! | 变更 | 策略 |
//! |---|---|
//! | 加列 | 自动演进（缺失列填 null） |
//! | 类型宽化（Int32→Int64→Float64） | 自动演进 |
//! | 类型窄化 | 拒绝（需显式 DDL） |
//! | 数值 ↔ 字符串 | 拒绝 |
//! | 删列 | 逻辑删除 |

use crate::error::LakeError;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum SchemaChangeKind {
    AddColumn = 0,
    WidenType = 1,
    DropColumn = 2,
}

impl SchemaChangeKind {
    pub fn from_u32(v: u32) -> Option<Self> {
        Some(match v {
            0 => Self::AddColumn,
            1 => Self::WidenType,
            2 => Self::DropColumn,
            _ => return None,
        })
    }
}

/// 单个 Schema 变更（EvolveSchema 请求的 change 字段，§8.2）
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaChange {
    AddColumn { field: Field },
    WidenType { column: String, to: DataType },
    DropColumn { column: String },
}

impl SchemaChange {
    pub fn kind(&self) -> SchemaChangeKind {
        match self {
            SchemaChange::AddColumn { .. } => SchemaChangeKind::AddColumn,
            SchemaChange::WidenType { .. } => SchemaChangeKind::WidenType,
            SchemaChange::DropColumn { .. } => SchemaChangeKind::DropColumn,
        }
    }

    /// 人类可读描述（SchemaVersion.change_desc）
    pub fn describe(&self) -> String {
        match self {
            SchemaChange::AddColumn { field } => {
                format!("add column {}: {:?}", field.name(), field.data_type())
            }
            SchemaChange::WidenType { column, to } => {
                format!("widen column {column}: {to:?}")
            }
            SchemaChange::DropColumn { column } => format!("drop column {column}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SchemaCompatibility {
    /// 与表 schema 完全兼容，无需演进
    Compatible,
    /// 需要先演进（OCC，必须在写 S3 之前完成，§5.2）
    NeedsEvolve(SchemaChange),
    /// 不兼容，拒绝写入
    Incompatible(String),
}

/// 比较传入 schema 与表 schema，判定兼容性（详细设计 §5.2 步骤②③）。
pub fn classify(table: &SchemaRef, incoming: &SchemaRef) -> SchemaCompatibility {
    // 逐列检查 incoming 中的字段是否被 table schema 覆盖
    for f in incoming.fields() {
        match table.field_with_name(f.name()) {
            // 完全相同 → OK
            Ok(tf) if tf.data_type() == f.data_type() => {}
            // 类型不同 → 走提升格判定
            Ok(tf) => {
                if let Some(change) = try_widen(f.name(), tf.data_type(), f.data_type()) {
                    return SchemaCompatibility::NeedsEvolve(change);
                }
                return SchemaCompatibility::Incompatible(format!(
                    "column {}: {:?} -> {:?} is not allowed (narrowing or incompatible)",
                    f.name(),
                    tf.data_type(),
                    f.data_type()
                ));
            }
            // 新列 → 加列（自动演进）
            Err(_) => {
                return SchemaCompatibility::NeedsEvolve(SchemaChange::AddColumn {
                    field: f.as_ref().clone(),
                });
            }
        }
    }
    SchemaCompatibility::Compatible
}

/// 类型提升格判定：能否从 `from` 宽化到 `to`（§8.1 格）。
fn try_widen(column: &str, from: &DataType, to: &DataType) -> Option<SchemaChange> {
    if from == to {
        return None;
    }
    let (rf, rt) = (crate::meta::promotion_rank(from), crate::meta::promotion_rank(to));
    if let (Some(a), Some(b)) = (rf, rt) {
        if b > a {
            return Some(SchemaChange::WidenType {
                column: column.to_string(),
                to: to.clone(),
            });
        }
    }
    None
}

/// 按类型提升格应用变更到 schema（详细设计 §8.2 `apply_change`）。
pub fn apply_change(schema: &SchemaRef, change: &SchemaChange) -> Result<SchemaRef, LakeError> {
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    match change {
        SchemaChange::AddColumn { field } => {
            if fields.iter().any(|f| f.name() == field.name()) {
                return Err(LakeError::InvalidSchemaChange(format!(
                    "column {} already exists",
                    field.name()
                )));
            }
            fields.push(field.clone());
        }
        SchemaChange::WidenType { column, to } => {
            let idx = fields
                .iter()
                .position(|f| f.name() == column)
                .ok_or_else(|| {
                    LakeError::InvalidSchemaChange(format!("column {column} not found"))
                })?;
            let from = fields[idx].data_type();
            // 只允许沿提升格向上
            if crate::meta::promotion_rank(from).is_none()
                || crate::meta::promotion_rank(to).is_none()
                || crate::meta::promotion_rank(from).unwrap()
                    >= crate::meta::promotion_rank(to).unwrap()
            {
                return Err(LakeError::InvalidSchemaChange(format!(
                    "cannot widen {from:?} -> {to:?} (narrowing or non-numeric)"
                )));
            }
            fields[idx] = Field::new(column, to.clone(), fields[idx].is_nullable());
        }
        SchemaChange::DropColumn { column } => {
            let before = fields.len();
            fields.retain(|f| f.name() != column);
            if fields.len() == before {
                return Err(LakeError::InvalidSchemaChange(format!(
                    "column {column} not found"
                )));
            }
            // 删列 = 逻辑删除：表 schema 移除，物理文件不变（§8.1）
        }
    }
    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::test_schema;
    use DataType as DT;

    #[test]
    fn identical_schemas_compatible() {
        let s = test_schema(&[("a", DT::Int64), ("b", DT::Utf8)]);
        assert_eq!(classify(&s, &s), SchemaCompatibility::Compatible);
    }

    #[test]
    fn new_column_needs_evolve() {
        let table = test_schema(&[("a", DT::Int64)]);
        let incoming = test_schema(&[("a", DT::Int64), ("extra", DT::Utf8)]);
        match classify(&table, &incoming) {
            SchemaCompatibility::NeedsEvolve(SchemaChange::AddColumn { field }) => {
                assert_eq!(field.name(), "extra");
            }
            other => panic!("expected NeedsEvolve(AddColumn), got {other:?}"),
        }
    }

    #[test]
    fn widening_needs_evolve() {
        let table = test_schema(&[("v", DT::Int32)]);
        let incoming = test_schema(&[("v", DT::Int64)]);
        assert!(matches!(
            classify(&table, &incoming),
            SchemaCompatibility::NeedsEvolve(SchemaChange::WidenType { .. })
        ));
    }

    #[test]
    fn narrowing_rejected() {
        let table = test_schema(&[("v", DT::Int64)]);
        let incoming = test_schema(&[("v", DT::Int32)]);
        assert!(matches!(
            classify(&table, &incoming),
            SchemaCompatibility::Incompatible(_)
        ));
    }

    #[test]
    fn numeric_to_string_rejected() {
        let table = test_schema(&[("v", DT::Int64)]);
        let incoming = test_schema(&[("v", DT::Utf8)]);
        assert!(matches!(
            classify(&table, &incoming),
            SchemaCompatibility::Incompatible(_)
        ));
    }

    #[test]
    fn apply_add_column() {
        let s = test_schema(&[("a", DT::Int64)]);
        let out = apply_change(
            &s,
            &SchemaChange::AddColumn {
                field: Field::new("b", DT::Utf8, true),
            },
        )
        .unwrap();
        assert_eq!(out.fields().len(), 2);
        assert_eq!(out.field_with_name("b").unwrap().data_type(), &DT::Utf8);
    }

    #[test]
    fn apply_widen_int32_to_int64() {
        let s = test_schema(&[("v", DT::Int32)]);
        let out = apply_change(
            &s,
            &SchemaChange::WidenType {
                column: "v".into(),
                to: DT::Int64,
            },
        )
        .unwrap();
        assert_eq!(out.field_with_name("v").unwrap().data_type(), &DT::Int64);
    }

    #[test]
    fn apply_widen_narrowing_rejected() {
        let s = test_schema(&[("v", DT::Int64)]);
        assert!(apply_change(
            &s,
            &SchemaChange::WidenType {
                column: "v".into(),
                to: DT::Int32,
            },
        )
        .is_err());
    }

    #[test]
    fn apply_drop_column() {
        let s = test_schema(&[("a", DT::Int64), ("b", DT::Utf8)]);
        let out = apply_change(&s, &SchemaChange::DropColumn { column: "b".into() }).unwrap();
        assert_eq!(out.fields().len(), 1);
    }
}
