//! **可下推的谓词**（`§176`，`plan.md` G.2 的下半：谓词下推）。
//!
//! # 为什么要单独一层
//!
//! `§173` 把"可分解聚合"下推到数据所在处算，但**只覆盖整表**（`SELECT count(*) FROM t`）。
//! 真实的看板查询几乎都带 `WHERE` ⇒ 那类查询仍然要把**全部热数据**拉回协调者。
//! 这一层给出可以**送过网**的谓词：协调者从 SQL 里认出它、数据端按它筛过再折聚合
//! ⇒ 网络里依旧只走标量。
//!
//! # 两条判定（与 `agg` 同一套纪律）
//!
//! 1. **小到可以逐字复刻**：只收"列 与 字面量 的比较"、`IS [NOT] NULL`、`AND` / `OR`。
//!    任何别的东西（算术、函数、`LIKE`、`IN`、`BETWEEN`、子查询、浮点列/字面量、
//!    跨类型比较）**一律拒绝** —— 拒绝的代价是"这次不过网省"，推错的代价是**结果悄悄错**。
//! 2. **语义与 DataFusion 逐字相同**（那是原路的判据）：三条要紧的
//!    - **三值逻辑**：`NULL` 参与的**任何**比较都是 `UNKNOWN`，而 `WHERE` 只放行 `TRUE`
//!      ⇒ `v != 5` 在 `v IS NULL` 的行上**不成立**（这一点最容易被写错）；
//!    - `AND`/`OR` 在"放行/不放行"这个二值投影上可组合（`UNKNOWN AND FALSE = FALSE`），
//!      所以掩码（mask）逐位与/或就是正确实现；
//!    - **列缺失 = 全 NULL**：读路径按 `arrow_util::align_batch` 对齐（缺失列补 NULL）
//!      ⇒ 这里对"这一批里没有这一列"必须**当成全 NULL**（而不是报错），
//!      否则 schema 演进过的表会在下推路径上失败，而原路好好儿的。

use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use arrow::compute::kernels::boolean::{and_kleene, or_kleene};
use arrow::record_batch::RecordBatch;

use crate::error::LakeError;

/// 比较运算符。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Eq = 0,
    Ne = 1,
    Lt = 2,
    Le = 3,
    Gt = 4,
    Ge = 5,
}

impl CmpOp {
    pub fn from_u32(v: u32) -> Option<Self> {
        Some(match v {
            0 => Self::Eq,
            1 => Self::Ne,
            2 => Self::Lt,
            3 => Self::Le,
            4 => Self::Gt,
            5 => Self::Ge,
            _ => return None,
        })
    }
}

/// 谓词里出现的字面量（**只有这两种**：见模块文档第 1 条）。
#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Int(i64),
    Text(String),
}

/// 一个小到可以逐字复刻的谓词。
#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    /// `column <op> literal`
    Cmp {
        column: String,
        op: CmpOp,
        value: Literal,
    },
    /// `column IS NULL`（`negated = true` ⇒ `IS NOT NULL`）
    IsNull { column: String, negated: bool },
    /// `a AND b AND ...`（空 = 恒真，不会由判定层产生）
    And(Vec<Predicate>),
    /// `a OR b OR ...`
    Or(Vec<Predicate>),
}

impl Predicate {
    /// 一批行上"放行"的掩码（`true` = 满足）。
    ///
    /// 缺失列按**全 NULL** 处理（见模块文档第 2 条），类型不符则**响亮失败**
    /// —— 那说明判定层与执行层对同一个谓词的理解不一致，不该静默取一边。
    pub fn mask(&self, batch: &RecordBatch) -> Result<BooleanArray, LakeError> {
        match self {
            Self::Cmp { column, op, value } => compare(batch, column, *op, value),
            Self::IsNull { column, negated } => {
                match batch.schema().index_of(column) {
                    // 列缺失 = 全 NULL ⇒ `IS NULL` 全真 / `IS NOT NULL` 全假
                    // 列不存在 ⇒ 这一批的该列**整体是 NULL** ⇒ `IS NULL` 全真、`IS NOT NULL` 全假
                    Err(_) => Ok(BooleanArray::from(vec![!*negated; batch.num_rows()])),
                    Ok(idx) => {
                        let col = batch.column(idx);
                        let m = BooleanArray::from(
                            (0..col.len()).map(|i| col.is_null(i)).collect::<Vec<_>>(),
                        );
                        Ok(if *negated { not(&m) } else { m })
                    }
                }
            }
            Self::And(parts) => {
                let mut acc: Option<BooleanArray> = None;
                for p in parts {
                    let m = p.mask(batch)?;
                    acc = Some(match acc {
                        None => m,
                        // Kleene：`UNKNOWN AND FALSE = FALSE`（`and_kleene` 正是这条）
                        Some(a) => and_kleene(&a, &m)
                            .map_err(|e| LakeError::Other(format!("谓词 AND 求值失败：{e}")))?,
                    });
                }
                acc.ok_or_else(|| LakeError::Other("空 AND 谓词".into()))
            }
            Self::Or(parts) => {
                let mut acc: Option<BooleanArray> = None;
                for p in parts {
                    let m = p.mask(batch)?;
                    acc = Some(match acc {
                        None => m,
                        Some(a) => or_kleene(&a, &m)
                            .map_err(|e| LakeError::Other(format!("谓词 OR 求值失败：{e}")))?,
                    });
                }
                acc.ok_or_else(|| LakeError::Other("空 OR 谓词".into()))
            }
        }
    }

    /// 这一批里放行多少行（`WHERE` 的语义：只算 `TRUE`）。
    pub fn count_matching(&self, batch: &RecordBatch) -> Result<usize, LakeError> {
        let m = self.mask(batch)?;
        Ok((0..m.len()).filter(|i| m.value(*i)).count())
    }
}

fn not(m: &BooleanArray) -> BooleanArray {
    BooleanArray::from((0..m.len()).map(|i| !m.value(i)).collect::<Vec<bool>>())
}

fn compare(
    batch: &RecordBatch,
    column: &str,
    op: CmpOp,
    value: &Literal,
) -> Result<BooleanArray, LakeError> {
    let idx = match batch.schema().index_of(column) {
        // 列缺失 = 全 NULL ⇒ 任何比较都是 UNKNOWN ⇒ **一行都不放行**
        Err(_) => return Ok(BooleanArray::from(vec![false; batch.num_rows()])),
        Ok(i) => i,
    };
    let col = batch.column(idx);
    match value {
        Literal::Int(v) => {
            let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                LakeError::Other(format!(
                    "谓词类型不符：列 `{column}` 是 {:?}，字面量是整数",
                    col.data_type()
                ))
            })?;
            Ok(BooleanArray::from(
                (0..arr.len())
                    .map(|i| {
                        if arr.is_null(i) {
                            false // UNKNOWN 不放行
                        } else {
                            cmp_ord(arr.value(i).cmp(v), op)
                        }
                    })
                    .collect::<Vec<bool>>(),
            ))
        }
        Literal::Text(v) => {
            let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                LakeError::Other(format!(
                    "谓词类型不符：列 `{column}` 是 {:?}，字面量是字符串",
                    col.data_type()
                ))
            })?;
            Ok(BooleanArray::from(
                (0..arr.len())
                    .map(|i| {
                        if arr.is_null(i) {
                            false
                        } else {
                            // 与 DataFusion 一致：Utf8 比较是**字节序**（`str::cmp`）
                            cmp_ord(arr.value(i).cmp(v.as_str()), op)
                        }
                    })
                    .collect::<Vec<bool>>(),
            ))
        }
    }
}

fn cmp_ord(ord: std::cmp::Ordering, op: CmpOp) -> bool {
    use std::cmp::Ordering::*;
    match op {
        CmpOp::Eq => ord == Equal,
        CmpOp::Ne => ord != Equal,
        CmpOp::Lt => ord == Less,
        CmpOp::Le => ord != Greater,
        CmpOp::Gt => ord == Greater,
        CmpOp::Ge => ord != Less,
    }
}

/// 把一批行按谓词**筛掉不放行的**（用于 `count(*)` 这类"数行"的聚合）。
pub fn filter_batch(batch: &RecordBatch, p: &Predicate) -> Result<RecordBatch, LakeError> {
    let m = p.mask(batch)?;
    let col: arrow::array::ArrayRef = Arc::new(m);
    arrow::compute::filter_record_batch(
        batch,
        col.as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| LakeError::Other("谓词掩码类型异常".into()))?,
    )
    .map_err(|e| LakeError::Other(format!("谓词过滤失败：{e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch(vals: Vec<Option<i64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vals))]).unwrap()
    }

    fn cmp(col: &str, op: CmpOp, v: i64) -> Predicate {
        Predicate::Cmp {
            column: col.into(),
            op,
            value: Literal::Int(v),
        }
    }

    /// **三值逻辑**：NULL 参与的任何比较都不放行 —— 连 `!=` 也是（最容易被写错的一条）。
    #[test]
    fn null_never_passes_any_comparison() {
        let b = batch(vec![Some(5), None]);
        // 两行的数据是 [5, NULL]，字面量 5 ⇒ 期望值只由**第一行**决定，NULL 行永不放行
        for (op, want) in [
            (CmpOp::Eq, 1),
            (CmpOp::Ne, 0),
            (CmpOp::Lt, 0),
            (CmpOp::Le, 1),
            (CmpOp::Gt, 0),
            (CmpOp::Ge, 1),
        ] {
            assert_eq!(
                cmp("v", op, 5).count_matching(&b).unwrap(),
                want,
                "`v {op:?} 5`：期望只数第一行（NULL 行不放行）"
            );
        }
    }

    #[test]
    fn comparisons_use_the_boundary_exactly() {
        let b = batch(vec![Some(1), Some(5), Some(9)]);
        assert_eq!(cmp("v", CmpOp::Gt, 5).count_matching(&b).unwrap(), 1);
        assert_eq!(cmp("v", CmpOp::Ge, 5).count_matching(&b).unwrap(), 2);
        assert_eq!(cmp("v", CmpOp::Lt, 5).count_matching(&b).unwrap(), 1);
        assert_eq!(cmp("v", CmpOp::Ne, 5).count_matching(&b).unwrap(), 2);
    }

    #[test]
    fn and_or_compose_with_kleene_semantics() {
        let b = batch(vec![Some(1), Some(5), None]);
        let gt0 = cmp("v", CmpOp::Gt, 0);
        let lt3 = cmp("v", CmpOp::Lt, 3);
        let and = Predicate::And(vec![gt0.clone(), lt3.clone()]);
        assert_eq!(and.count_matching(&b).unwrap(), 1, "只有 1 同时满足");
        let or = Predicate::Or(vec![gt0, lt3]);
        assert_eq!(
            or.count_matching(&b).unwrap(),
            2,
            "NULL 行两边都不真 ⇒ 不放行"
        );
    }

    /// `IS NULL` 与 `IS NOT NULL` 必须是**互补**的。
    ///
    /// ⚠️ 这里刻意用 3 行（2 非空 + 1 空）：两行时"取反"与"忘了取反"恰好都数出 1 ——
    /// 那种测试**证明不了任何事**。（这条正是对拍用例抓出 `not()` 写错后补的。）
    #[test]
    fn is_null_and_is_not_null_are_complementary() {
        let b = batch(vec![Some(1), Some(2), None]);
        let is_null = Predicate::IsNull {
            column: "v".into(),
            negated: false,
        };
        let not_null = Predicate::IsNull {
            column: "v".into(),
            negated: true,
        };
        assert_eq!(is_null.count_matching(&b).unwrap(), 1);
        assert_eq!(not_null.count_matching(&b).unwrap(), 2);
        // 并集必须是全部行（互补的另一半说法）
        assert_eq!(
            is_null.count_matching(&b).unwrap() + not_null.count_matching(&b).unwrap(),
            b.num_rows()
        );
    }

    /// **列缺失 = 全 NULL**（读路径的 `align_batch` 就是这么对齐的）：
    /// 不许报错，也不许"放行"。
    #[test]
    fn a_missing_column_behaves_as_all_null() {
        let b = batch(vec![Some(1), Some(2)]);
        assert_eq!(cmp("nope", CmpOp::Eq, 1).count_matching(&b).unwrap(), 0);
        assert_eq!(
            Predicate::IsNull {
                column: "nope".into(),
                negated: false
            }
            .count_matching(&b)
            .unwrap(),
            2,
            "缺失列 IS NULL 全真"
        );
        assert_eq!(
            Predicate::IsNull {
                column: "nope".into(),
                negated: true
            }
            .count_matching(&b)
            .unwrap(),
            0
        );
    }

    /// 类型不符**响亮失败**（判定层与执行层理解不一致时不许静默取一边）。
    #[test]
    fn a_type_mismatch_is_loud() {
        let b = batch(vec![Some(1)]);
        let e = Predicate::Cmp {
            column: "v".into(),
            op: CmpOp::Eq,
            value: Literal::Text("x".into()),
        }
        .count_matching(&b)
        .unwrap_err();
        assert!(e.to_string().contains("类型不符"), "{e}");
    }

    #[test]
    fn text_comparison_is_bytewise() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let b = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("abc"),
                Some("abd"),
                None,
            ]))],
        )
        .unwrap();
        let p = Predicate::Cmp {
            column: "s".into(),
            op: CmpOp::Lt,
            value: Literal::Text("abd".into()),
        };
        assert_eq!(p.count_matching(&b).unwrap(), 1);
    }

    #[test]
    fn filter_batch_drops_only_unmatched_rows() {
        let b = batch(vec![Some(1), Some(5), None]);
        let out = filter_batch(&b, &cmp("v", CmpOp::Gt, 3)).unwrap();
        assert_eq!(out.num_rows(), 1, "只剩 5");
    }
}
