//! **可分解聚合的部分状态**（`§173`，`plan.md` G.2 / 归档 `S5-3`）。
//!
//! # 为什么要有它
//!
//! 扇出今天把**原始行**拉回协调者（`shard.proto` 的 `batches_ipc`），再在协调者上算聚合 ——
//! 多节点时网络传的是"全部热数据"。这一层给出**可以下推的那一类聚合**：
//! 每个来源只回**标量**，协调者把标量合并成最终答案。
//!
//! # 两条判定（决定了这里只收哪几种聚合）
//!
//! 1. **可分解**：`partial(a ∪ b) = merge(partial(a), partial(b))`。
//!    `count` / `sum` / `min` / `max` 满足；`avg` 要用 `(sum, count)` 两个状态表示（今天不做）；
//!    `stddev` / `percentile` / `count(distinct)` **不满足** ⇒ 不下推（走原路）。
//! 2. **精确**：合并结果必须与原路（协调者单次扫描）**逐位相同**。
//!    整数加减与比较满足；**浮点求和不满足结合律**（`(a+b)+c ≠ a+(b+c)` 的最后几位）⇒
//!    `sum` **只收整数**，浮点列一律走原路。`min/max` 只收**整数与字符串**：
//!    浮点的 `NaN` 在 arrow 的 min/max 里有"NaN 视为最大"的特殊口径，跨节点合并要先复刻它，
//!    而"复刻得不完全一致"的风险高于收益（第一刀不碰，见 `§173.3`）。
//!
//! 这两条是**边界**而不是性能取舍：越过它们的后果是"结果悄悄错一点"。

use arrow::array::{Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;

use crate::error::LakeError;
use crate::predicate::Predicate;

/// 聚合种类（**只有这些能下推**）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggKind {
    /// `count(*)` —— 数行
    CountStar = 0,
    /// `count(col)` —— 数**非 NULL** 值
    Count = 1,
    /// `sum(col)` —— **只收整数列**（浮点求和不满足结合律）
    SumInt = 2,
    /// `min(col)` —— 只收整数 / 字符串列
    Min = 3,
    /// `max(col)` —— 同上
    Max = 4,
}

impl AggKind {
    pub fn from_u32(v: u32) -> Option<Self> {
        Some(match v {
            0 => Self::CountStar,
            1 => Self::Count,
            2 => Self::SumInt,
            3 => Self::Min,
            4 => Self::Max,
            _ => return None,
        })
    }

    /// 这一种是否需要一个列参数。
    pub fn needs_column(self) -> bool {
        !matches!(self, Self::CountStar)
    }
}

/// 一个要算的聚合。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggSpec {
    pub kind: AggKind,
    /// 列名；`count(*)` 时为 `None`
    pub column: Option<String>,
}

impl AggSpec {
    pub fn new(kind: AggKind, column: impl Into<String>) -> Self {
        Self {
            kind,
            column: Some(column.into()),
        }
    }

    pub fn count_star() -> Self {
        Self {
            kind: AggKind::CountStar,
            column: None,
        }
    }
}

/// `min`/`max` 的比较载体（今天只支持整数与字符串 —— 见模块文档第 2 条）。
#[derive(Clone, Debug, PartialEq)]
pub enum AggScalar {
    Int(i64),
    Text(String),
}

impl AggScalar {
    /// 同一个 `min`/`max` 状态里比较两个标量；**类型不同 = 出错**（不是"某边更小"）。
    ///
    /// 为什么响亮失败：类型不同只可能来自"把两个不同列的部分状态合并了"这种编程错误。
    /// 静默取一边会让这种错误以"结果少一点数据"的形式活下来。
    fn cmp(&self, other: &Self) -> Result<std::cmp::Ordering, LakeError> {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => Ok(a.cmp(b)),
            (Self::Text(a), Self::Text(b)) => Ok(a.cmp(b)),
            _ => Err(LakeError::Other(
                "聚合部分状态的类型不一致（把两个不同列的状态合并了？）".into(),
            )),
        }
    }
}

/// 一个聚合的**部分状态**。
///
/// `None` 的语义与 SQL 一致：`sum`/`min`/`max` 在**空集**（或全 NULL 列）上是 NULL，
/// 而 `count` 是 0。合并时必须保留这个差别（`None + Some(x) = Some(x)`，但
/// `None + None = None`）—— 把 NULL 当 0 会让"空表求和"变成 0 而不是 NULL。
#[derive(Clone, Debug, PartialEq)]
pub enum PartialAgg {
    Count(u64),
    SumInt(Option<i64>),
    Min(Option<AggScalar>),
    Max(Option<AggScalar>),
}

impl PartialAgg {
    pub fn kind(&self) -> AggKind {
        match self {
            Self::Count(_) => AggKind::Count,
            Self::SumInt(_) => AggKind::SumInt,
            Self::Min(_) => AggKind::Min,
            Self::Max(_) => AggKind::Max,
        }
    }

    /// 空状态（没读过任何行）—— 合并的**单位元**。
    pub fn empty(kind: AggKind) -> Self {
        match kind {
            // `count(*)` 与 `count(col)` 的形态相同（都是 `Count`），合并规则也相同
            AggKind::CountStar | AggKind::Count => Self::Count(0),
            AggKind::SumInt => Self::SumInt(None),
            AggKind::Min => Self::Min(None),
            AggKind::Max => Self::Max(None),
        }
    }
}

/// 在一批行上折叠出**每个 spec 的部分状态**。
///
/// 只支持 `Int64`（`sum`/`min`/`max`）、`Utf8`（`min`/`max`）与"任意类型的 `count`"。
/// 列不存在或类型不支持 ⇒ **响亮失败**（调用方本应在下推前就用表 schema 排除掉这些形状；
/// 走到这里出错说明"判定与执行"两处不一致，不该静默退回）。
pub fn fold_batch(specs: &[AggSpec], batch: &RecordBatch) -> Result<Vec<PartialAgg>, LakeError> {
    let mut out = Vec::with_capacity(specs.len());
    for spec in specs {
        // **列缺失 = 全 NULL**（`§176`）：读路径按 `arrow_util::align_batch` 对齐（缺失列补 NULL）
        // ⇒ 这里必须同样处理，否则 schema 演进过的表会在下推路径上报错、而原路好好儿的。
        if let Some(name) = spec.column.as_deref()
            && batch.schema().index_of(name).is_err()
        {
            out.push(match spec.kind {
                AggKind::Count => PartialAgg::Count(0),
                AggKind::SumInt => PartialAgg::SumInt(None),
                AggKind::Min => PartialAgg::Min(None),
                AggKind::Max => PartialAgg::Max(None),
                // `count(*)` 的列参数恒为 None，走不到这里
                AggKind::CountStar => PartialAgg::Count(batch.num_rows() as u64),
            });
            continue;
        }
        out.push(match spec.kind {
            AggKind::CountStar => PartialAgg::Count(batch.num_rows() as u64),
            AggKind::Count => {
                let col = column(batch, spec)?;
                PartialAgg::Count((col.len() - col.null_count()) as u64)
            }
            AggKind::SumInt => {
                let arr = int_column(batch, spec)?;
                let mut acc: Option<i64> = None;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    acc = Some(
                        acc.unwrap_or(0)
                            .checked_add(arr.value(i))
                            .ok_or_else(|| overflow_err(spec))?,
                    );
                }
                PartialAgg::SumInt(acc)
            }
            AggKind::Min | AggKind::Max => {
                let want_max = matches!(spec.kind, AggKind::Max);
                let best = fold_extreme(batch, spec, want_max)?;
                if want_max {
                    PartialAgg::Max(best)
                } else {
                    PartialAgg::Min(best)
                }
            }
        });
    }
    Ok(out)
}

/// 在**多批**上折叠（批间顺序不影响结果：这几种聚合都是可交换可结合的）。
pub fn fold_batches(
    specs: &[AggSpec],
    batches: &[RecordBatch],
) -> Result<Vec<PartialAgg>, LakeError> {
    fold_batches_with(specs, batches, None)
}

/// 同 [`fold_batch`]，但先按谓词**筛掉不放行的行**（`§176`：谓词下推）。
pub fn fold_batch_with(
    specs: &[AggSpec],
    batch: &RecordBatch,
    predicate: Option<&Predicate>,
) -> Result<Vec<PartialAgg>, LakeError> {
    match predicate {
        None => fold_batch(specs, batch),
        Some(p) => fold_batch(specs, &crate::predicate::filter_batch(batch, p)?),
    }
}

/// 同 [`fold_batches`]，但先按谓词筛行。
pub fn fold_batches_with(
    specs: &[AggSpec],
    batches: &[RecordBatch],
    predicate: Option<&Predicate>,
) -> Result<Vec<PartialAgg>, LakeError> {
    let mut acc: Vec<PartialAgg> = specs.iter().map(|s| PartialAgg::empty(s.kind)).collect();
    for b in batches {
        let part = fold_batch_with(specs, b, predicate)?;
        for i in 0..acc.len() {
            acc[i] = merge(&acc[i], &part[i])?;
        }
    }
    Ok(acc)
}

/// 合并两个部分状态（同一种聚合）。
pub fn merge(a: &PartialAgg, b: &PartialAgg) -> Result<PartialAgg, LakeError> {
    match (a, b) {
        (PartialAgg::Count(x), PartialAgg::Count(y)) => Ok(PartialAgg::Count(
            x.checked_add(*y)
                .ok_or_else(|| LakeError::Other("count 溢出 u64".into()))?,
        )),
        (PartialAgg::SumInt(x), PartialAgg::SumInt(y)) => Ok(PartialAgg::SumInt(match (x, y) {
            (None, None) => None,
            (Some(v), None) | (None, Some(v)) => Some(*v),
            (Some(p), Some(q)) => Some(
                p.checked_add(*q)
                    .ok_or_else(|| LakeError::Other("sum 合并溢出 i64".into()))?,
            ),
        })),
        (PartialAgg::Min(x), PartialAgg::Min(y)) => Ok(PartialAgg::Min(min_of(x, y)?)),
        (PartialAgg::Max(x), PartialAgg::Max(y)) => Ok(PartialAgg::Max(max_of(x, y)?)),
        _ => Err(LakeError::Other(format!(
            "合并了不同种类的聚合部分状态：{:?} vs {:?}",
            a.kind(),
            b.kind()
        ))),
    }
}

/// 合并一串（含单个来源的"单位元 + 一份"）。空输入 ⇒ 逐个的单位元。
pub fn merge_all(partials: &[PartialAgg]) -> Result<Option<PartialAgg>, LakeError> {
    let Some(first) = partials.first() else {
        return Ok(None);
    };
    let mut acc = first.clone();
    for p in &partials[1..] {
        acc = merge(&acc, p)?;
    }
    Ok(Some(acc))
}

fn min_of(
    a: &Option<AggScalar>,
    b: &Option<AggScalar>,
) -> Result<Option<AggScalar>, LakeError> {
    Ok(match (a, b) {
        (None, x) | (x, None) => x.clone(),
        (Some(x), Some(y)) => Some(if x.cmp(y)? == std::cmp::Ordering::Greater {
            y.clone()
        } else {
            x.clone()
        }),
    })
}

fn max_of(
    a: &Option<AggScalar>,
    b: &Option<AggScalar>,
) -> Result<Option<AggScalar>, LakeError> {
    Ok(match (a, b) {
        (None, x) | (x, None) => x.clone(),
        (Some(x), Some(y)) => Some(if x.cmp(y)? == std::cmp::Ordering::Less {
            y.clone()
        } else {
            x.clone()
        }),
    })
}

fn column<'a>(batch: &'a RecordBatch, spec: &AggSpec) -> Result<&'a dyn Array, LakeError> {
    let name = spec
        .column
        .as_deref()
        .ok_or_else(|| LakeError::Other(format!("{:?} 缺列名", spec.kind)))?;
    batch
        .column_by_name(name)
        .map(|c| c.as_ref())
        .ok_or_else(|| LakeError::Other(format!("聚合列不存在：{name}")))
}

fn int_column<'a>(batch: &'a RecordBatch, spec: &AggSpec) -> Result<&'a Int64Array, LakeError> {
    let col = column(batch, spec)?;
    col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        LakeError::Other(format!(
            "`sum` 只下沉整数列（浮点求和不满足结合律）—— 列 `{}` 是 {:?}",
            spec.column.as_deref().unwrap_or("?"),
            col.data_type()
        ))
    })
}

fn fold_extreme(
    batch: &RecordBatch,
    spec: &AggSpec,
    want_max: bool,
) -> Result<Option<AggScalar>, LakeError> {
    let col = column(batch, spec)?;
    let mut best: Option<AggScalar> = None;
    let take = |cand: AggScalar, best: &mut Option<AggScalar>| -> Result<(), LakeError> {
        *best = Some(match best.take() {
            None => cand,
            Some(cur) => {
                let ord = cand.cmp(&cur)?;
                let wins = if want_max {
                    ord == std::cmp::Ordering::Greater
                } else {
                    ord == std::cmp::Ordering::Less
                };
                if wins { cand } else { cur }
            }
        });
        Ok(())
    };
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                take(AggScalar::Int(arr.value(i)), &mut best)?;
            }
        }
        return Ok(best);
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                take(AggScalar::Text(arr.value(i).to_string()), &mut best)?;
            }
        }
        return Ok(best);
    }
    Err(LakeError::Other(format!(
        "`min`/`max` 只下沉整数与字符串列 —— 列 `{}` 是 {:?}",
        spec.column.as_deref().unwrap_or("?"),
        col.data_type()
    )))
}

fn overflow_err(spec: &AggSpec) -> LakeError {
    LakeError::Other(format!(
        "`sum({})` 溢出 i64（下推路径用受检加法；原路同样会报错，但报的位置不同）",
        spec.column.as_deref().unwrap_or("?")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(vals: Vec<Option<i64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vals))]).unwrap()
    }

    #[test]
    fn count_star_counts_rows_and_count_counts_non_null() {
        let specs = vec![AggSpec::count_star(), AggSpec::new(AggKind::Count, "v")];
        let got = fold_batch(&specs, &batch(vec![Some(1), None, Some(3)])).unwrap();
        assert_eq!(got[0], PartialAgg::Count(3), "count(*) 数行（含 NULL）");
        assert_eq!(got[1], PartialAgg::Count(2), "count(v) 只数非 NULL");
    }

    #[test]
    fn sum_min_max_are_null_on_the_empty_set_not_zero() {
        let specs = vec![
            AggSpec::new(AggKind::SumInt, "v"),
            AggSpec::new(AggKind::Min, "v"),
            AggSpec::new(AggKind::Max, "v"),
        ];
        let empty = fold_batch(&specs, &batch(vec![])).unwrap();
        assert_eq!(empty[0], PartialAgg::SumInt(None), "空集求和是 NULL，不是 0");
        assert_eq!(empty[1], PartialAgg::Min(None));
        assert_eq!(empty[2], PartialAgg::Max(None));

        // 全 NULL 列同理
        let all_null = fold_batch(&specs, &batch(vec![None, None])).unwrap();
        assert_eq!(all_null[0], PartialAgg::SumInt(None));
    }

    #[test]
    fn merge_is_exact_and_null_aware() {
        // `None + Some` = `Some`：一部分来源完全没有数据时，答案不该变成 NULL
        let m = merge(&PartialAgg::SumInt(None), &PartialAgg::SumInt(Some(7))).unwrap();
        assert_eq!(m, PartialAgg::SumInt(Some(7)));
        assert_eq!(
            merge(&PartialAgg::SumInt(None), &PartialAgg::SumInt(None)).unwrap(),
            PartialAgg::SumInt(None)
        );
        // 分批折叠与一次折叠相同（可交换可结合）
        let specs = vec![
            AggSpec::count_star(),
            AggSpec::new(AggKind::SumInt, "v"),
            AggSpec::new(AggKind::Min, "v"),
            AggSpec::new(AggKind::Max, "v"),
        ];
        let one = fold_batches(&specs, &[batch(vec![Some(5), Some(-2), None])]).unwrap();
        let split = fold_batches(
            &specs,
            &[batch(vec![Some(5)]), batch(vec![Some(-2), None])],
        )
        .unwrap();
        assert_eq!(one, split, "分批折叠必须与一次折叠相同");
        assert_eq!(one[1], PartialAgg::SumInt(Some(3)));
        assert_eq!(one[2], PartialAgg::Min(Some(AggScalar::Int(-2))));
        assert_eq!(one[3], PartialAgg::Max(Some(AggScalar::Int(5))));
    }

    #[test]
    fn merge_refuses_mismatched_kinds() {
        let e = merge(&PartialAgg::Count(1), &PartialAgg::SumInt(Some(1))).unwrap_err();
        assert!(e.to_string().contains("不同种类"), "{e}");
    }

    #[test]
    fn sum_overflow_is_loud_not_silent_wraparound() {
        let specs = vec![AggSpec::new(AggKind::SumInt, "v")];
        let e = fold_batch(&specs, &batch(vec![Some(i64::MAX), Some(1)])).unwrap_err();
        assert!(e.to_string().contains("溢出"), "{e}");
    }

    #[test]
    fn float_columns_are_refused_for_sum_but_allowed_for_min_max() {
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
        let b = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Float64Array::from(vec![1.5, 2.5]))],
        )
        .unwrap();
        let e = fold_batch(&[AggSpec::new(AggKind::SumInt, "f")], &b).unwrap_err();
        assert!(e.to_string().contains("浮点"), "浮点求和不许下推：{e}");
        // `min/max` 也不收浮点（NaN 口径要先复刻，第一刀不碰）
        let e = fold_batch(&[AggSpec::new(AggKind::Min, "f")], &b).unwrap_err();
        assert!(e.to_string().contains("整数与字符串"), "{e}");
    }

    /// **列缺失 = 全 NULL**（`§176`）：与读路径的 `align_batch` 同语义，不许报错。
    #[test]
    fn a_missing_column_folds_as_all_null() {
        let b = batch(vec![Some(1), Some(2)]);
        let specs = vec![
            AggSpec::new(AggKind::Count, "nope"),
            AggSpec::new(AggKind::SumInt, "nope"),
            AggSpec::new(AggKind::Min, "nope"),
            AggSpec::new(AggKind::Max, "nope"),
        ];
        let got = fold_batch(&specs, &b).unwrap();
        assert_eq!(got[0], PartialAgg::Count(0), "缺失列一个非空值都没有");
        assert_eq!(got[1], PartialAgg::SumInt(None));
        assert_eq!(got[2], PartialAgg::Min(None));
        assert_eq!(got[3], PartialAgg::Max(None));
    }

    /// 谓词在折叠**之前**生效（`§176`）：等价于"先筛行再算"，NULL 行不放行。
    #[test]
    fn a_predicate_filters_rows_before_folding() {
        use crate::predicate::{CmpOp, Literal, Predicate};
        let specs = vec![AggSpec::count_star(), AggSpec::new(AggKind::SumInt, "v")];
        let batches = vec![batch(vec![Some(1), None, Some(5)]), batch(vec![Some(9)])];
        let p = Predicate::Cmp {
            column: "v".into(),
            op: CmpOp::Gt,
            value: Literal::Int(1),
        };
        let got = fold_batches_with(&specs, &batches, Some(&p)).unwrap();
        assert_eq!(got[0], PartialAgg::Count(2), "只有 5 与 9 放行");
        assert_eq!(got[1], PartialAgg::SumInt(Some(14)));
        // 没有谓词时是同一套代码的另一条路径：必须看到全部（含 NULL 带来的差异）
        let all = fold_batches_with(&specs, &batches, None).unwrap();
        assert_eq!(all[0], PartialAgg::Count(4));
        assert_eq!(all[1], PartialAgg::SumInt(Some(15)));
    }

    #[test]
    fn merge_all_folds_every_source() {
        let got = merge_all(&[
            PartialAgg::Count(2),
            PartialAgg::Count(3),
            PartialAgg::Count(0),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(got, PartialAgg::Count(5));
        assert!(merge_all(&[]).unwrap().is_none());
    }
}
