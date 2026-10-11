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

use std::collections::BTreeMap;
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
///
/// `Ord` 只用于**分组键的排序**（确定性纪律：两个来源的同一组要能被合到一起），
/// **不**用于 `min`/`max` 的语义 —— 那是 [`Self::cmp`]（类型不一致时**响亮失败**）。
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
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
#[derive(Clone, Debug, PartialEq, Eq)]
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

/// 一次**聚合拉取**的请求（`§177`）：算哪些聚合、按哪些列分组、先按什么谓词筛行。
///
/// 三个参数打包成一个结构体的理由：它们**一起**决定"要算什么"，而 trait 方法每加一个
/// 参数就要改一遍所有实现（`read_table_aggs` 已经改过两轮了）。
#[derive(Clone, Debug, PartialEq, Default)]
pub struct AggQuery {
    pub specs: Vec<AggSpec>,
    /// 分组键列（**有序**）；空 = 不分组
    pub group_by: Vec<String>,
    pub predicate: Option<Predicate>,
}

impl AggQuery {
    /// 不分组的查询（`§173` 的形状）
    pub fn plain(specs: Vec<AggSpec>) -> Self {
        Self {
            specs,
            group_by: Vec::new(),
            predicate: None,
        }
    }

    pub fn is_grouped(&self) -> bool {
        !self.group_by.is_empty()
    }
}

/// 分组键的一个分量。
///
/// `None` = 该列的键是 **NULL**：SQL 的 `GROUP BY` 把 NULL **当作一个组**
/// （与 `=` 的语义不同 —— `NULL = NULL` 是 UNKNOWN，但分组时它们就是一组）
/// ⇒ 必须用一个**独立分量**表示它，而不是"没有值"。
pub type KeyPart = Option<AggScalar>;

/// 一组的状态（`§177`）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupStates {
    /// 与 [`AggQuery::group_by`] **同序同长**（顺序是契约的一部分）
    pub key: Vec<KeyPart>,
    /// 与 [`AggQuery::specs`] **同序同长**
    pub states: Vec<PartialAgg>,
}

/// 聚合拉取的结果（`§177`）。
///
/// **两种形状之一**，由请求带不带 `group_by` 决定。刻意做成**和类型**而不是两个可选字段：
/// "两个字段只有一个有意义"是**可以误用**的契约（而且哪个有意义要靠猜）⇒ 用类型表达。
#[derive(Clone, Debug, PartialEq)]
pub enum AggStates {
    /// 不分组：与 specs 同序同长（**空输入也是单位元，不是空 Vec**）
    Plain(Vec<PartialAgg>),
    /// 分组：**按 key 升序**（确定性：多来源的同一组必须能合到一起）
    Grouped(Vec<GroupStates>),
}

impl AggStates {
    /// 组的个数（不分组时恒为 1）。
    pub fn groups(&self) -> usize {
        match self {
            Self::Plain(_) => 1,
            Self::Grouped(g) => g.len(),
        }
    }
}

/// 按 [`AggQuery`] 在**多批**上折叠（`§177`）。
///
/// 分组时**逐行**累积到该组的状态上（批与来源之间的顺序都不影响结果 —— 这几种聚合
/// 都是可交换可结合的，而"哪个组"由**键**决定）。
pub fn fold_query(query: &AggQuery, batches: &[RecordBatch]) -> Result<AggStates, LakeError> {
    if !query.is_grouped() {
        return Ok(AggStates::Plain(fold_batches_with(
            &query.specs,
            batches,
            query.predicate.as_ref(),
        )?));
    }
    let mut acc: BTreeMap<Vec<KeyPart>, Vec<PartialAgg>> = BTreeMap::new();
    for batch in batches {
        // 谓词**先**筛行（`§176`），再按行分组
        let work = match query.predicate.as_ref() {
            None => std::borrow::Cow::Borrowed(batch),
            Some(p) => std::borrow::Cow::Owned(crate::predicate::filter_batch(batch, p)?),
        };
        for row in 0..work.num_rows() {
            let key = key_of(&work, &query.group_by, row)?;
            let states = acc
                .entry(key)
                .or_insert_with(|| query.specs.iter().map(|s| PartialAgg::empty(s.kind)).collect());
            for (i, spec) in query.specs.iter().enumerate() {
                states[i] = merge(&states[i], &fold_one(spec, &work, row)?)?;
            }
        }
    }
    Ok(AggStates::Grouped(
        acc.into_iter()
            .map(|(key, states)| GroupStates { key, states })
            .collect(),
    ))
}

/// 把两个来源的结果合到一起（`§177`）；形状不一致 ⇒ **响亮失败**（那是契约错，
/// 静默取一边会让"少了一个分组"以正常答案的形式活下来）。
pub fn merge_states(a: &AggStates, b: &AggStates) -> Result<AggStates, LakeError> {
    match (a, b) {
        (AggStates::Plain(x), AggStates::Plain(y)) => {
            let mut out = Vec::with_capacity(x.len());
            for i in 0..x.len().max(y.len()) {
                let lhs = x.get(i).ok_or_else(|| mismatch("plain", x.len(), y.len()))?;
                let rhs = y.get(i).ok_or_else(|| mismatch("plain", x.len(), y.len()))?;
                out.push(merge(lhs, rhs)?);
            }
            Ok(AggStates::Plain(out))
        }
        (AggStates::Grouped(x), AggStates::Grouped(y)) => {
            let mut acc: BTreeMap<Vec<KeyPart>, Vec<PartialAgg>> = BTreeMap::new();
            for g in x.iter().chain(y.iter()) {
                let states = acc.entry(g.key.clone()).or_insert_with(|| {
                    g.states
                        .iter()
                        .map(|p| PartialAgg::empty(p.kind()))
                        .collect()
                });
                for (i, p) in g.states.iter().enumerate() {
                    let cur = states
                        .get(i)
                        .ok_or_else(|| mismatch("grouped", g.states.len(), i))?;
                    states[i] = merge(cur, p)?;
                }
            }
            Ok(AggStates::Grouped(
                acc.into_iter()
                    .map(|(key, states)| GroupStates { key, states })
                    .collect(),
            ))
        }
        _ => Err(LakeError::Other(
            "聚合部分状态的形状不一致（一边分组、一边不分组）".into(),
        )),
    }
}

fn mismatch(what: &str, a: usize, b: usize) -> LakeError {
    LakeError::Other(format!(
        "聚合部分状态的长度不一致（{what}：{a} vs {b}）—— 两个来源说的不是同一件事"
    ))
}

/// 一行在该分组键上的取值（**列缺失 = NULL**，与 `fold_batch` 同口径）。
fn key_of(
    batch: &RecordBatch,
    group_by: &[String],
    row: usize,
) -> Result<Vec<KeyPart>, LakeError> {
    let mut out = Vec::with_capacity(group_by.len());
    for name in group_by {
        let Ok(idx) = batch.schema().index_of(name) else {
            out.push(None);
            continue;
        };
        let col = batch.column(idx);
        out.push(if col.is_null(row) {
            None
        } else {
            Some(scalar_at(col, row)?)
        });
    }
    Ok(out)
}

/// 一行的部分状态（单行版：分组时要逐行累积）。
fn fold_one(spec: &AggSpec, batch: &RecordBatch, row: usize) -> Result<PartialAgg, LakeError> {
    let Some(name) = spec.column.as_deref() else {
        return Ok(PartialAgg::Count(1)); // `count(*)`：一行就是 1
    };
    let empty = || match spec.kind {
        AggKind::Count | AggKind::CountStar => PartialAgg::Count(0),
        AggKind::SumInt => PartialAgg::SumInt(None),
        AggKind::Min => PartialAgg::Min(None),
        AggKind::Max => PartialAgg::Max(None),
    };
    // 列缺失 或 该行是 NULL ⇒ 这一行**不贡献**（`count` 不加、`sum/min/max` 保持 NULL）
    if batch.schema().index_of(name).is_err() || column(batch, spec)?.is_null(row) {
        return Ok(empty());
    }
    Ok(match spec.kind {
        AggKind::Count | AggKind::CountStar => PartialAgg::Count(1),
        AggKind::SumInt => PartialAgg::SumInt(Some(int_column(batch, spec)?.value(row))),
        AggKind::Min => PartialAgg::Min(Some(scalar_at(column(batch, spec)?, row)?)),
        AggKind::Max => PartialAgg::Max(Some(scalar_at(column(batch, spec)?, row)?)),
    })
}

/// 取一行上的标量（今天只有整数与字符串两种分组键 —— 判定层已经把别的类型挡在门外）。
fn scalar_at(col: &dyn Array, row: usize) -> Result<AggScalar, LakeError> {
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Ok(AggScalar::Int(a.value(row)));
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return Ok(AggScalar::Text(a.value(row).to_string()));
    }
    Err(LakeError::Other(format!(
        "分组键/极值的类型不认识：{:?}",
        col.data_type()
    )))
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

    // ------------------------------------------------------ 分组聚合（`§177`）

    /// 两列：`v` 整数（分组键用）、`s` 字符串（分组键用）。
    fn batch2(
        vals: Vec<Option<i64>>,
        names: Vec<Option<&str>>,
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vals)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    fn grouped(query: AggQuery, batches: Vec<RecordBatch>) -> Vec<GroupStates> {
        match fold_query(&query, &batches).unwrap() {
            AggStates::Grouped(g) => g,
            other => panic!("应当是分组结果：{other:?}"),
        }
    }

    /// **NULL 是一个组**（SQL 的分组语义：`NULL = NULL` 虽然是 UNKNOWN，但分组时它们是一组）。
    /// 写错这一条的形态是"NULL 行各成一组"或"NULL 行被丢掉" —— 两种都不报错。
    #[test]
    fn grouped_fold_puts_null_keys_in_one_group() {
        let mut q = AggQuery::plain(vec![AggSpec::count_star()]);
        q.group_by = vec!["v".into()];
        let got = grouped(q, vec![batch(vec![Some(1), None, Some(1), None])]);
        assert_eq!(got.len(), 2, "两组：1 与 NULL：{got:?}");
        assert_eq!(got[0].key, vec![None], "NULL 排在整数之前（键序只求确定）");
        assert_eq!(got[0].states[0], PartialAgg::Count(2));
        assert_eq!(got[1].key, vec![Some(AggScalar::Int(1))]);
        assert_eq!(got[1].states[0], PartialAgg::Count(2));
    }

    /// 两个来源**按键合并**（不是"按行对齐"），且 NULL 键在两边指的是同一个组。
    #[test]
    fn grouped_merge_is_key_wise_including_the_null_key() {
        let specs = vec![AggSpec::count_star(), AggSpec::new(AggKind::SumInt, "v")];
        let a = fold_query(
            &AggQuery {
                specs: specs.clone(),
                group_by: vec!["v".into()],
                predicate: None,
            },
            &[batch(vec![Some(1), None, Some(3)])],
        )
        .unwrap();
        let b = fold_query(
            &AggQuery {
                specs: specs.clone(),
                group_by: vec!["v".into()],
                predicate: None,
            },
            &[batch(vec![Some(1), None, Some(7)])],
        )
        .unwrap();
        let merged = merge_states(&a, &b).unwrap();
        let AggStates::Grouped(g) = merged else {
            panic!("应当是分组结果");
        };
        let got: Vec<_> = g
            .iter()
            .map(|x| (x.key.clone(), x.states.clone()))
            .collect();
        assert_eq!(
            got,
            vec![
                (
                    vec![None],
                    vec![PartialAgg::Count(2), PartialAgg::SumInt(None)]
                ),
                (
                    vec![Some(AggScalar::Int(1))],
                    vec![PartialAgg::Count(2), PartialAgg::SumInt(Some(2))]
                ),
                (
                    vec![Some(AggScalar::Int(3))],
                    vec![PartialAgg::Count(1), PartialAgg::SumInt(Some(3))]
                ),
                (
                    vec![Some(AggScalar::Int(7))],
                    vec![PartialAgg::Count(1), PartialAgg::SumInt(Some(7))]
                ),
            ],
            "按键合并 + 键升序：{got:?}"
        );
    }

    /// **字符串键**与**多列键**（看板里最常见的就是"按主机/租户分组"）。
    #[test]
    fn grouped_fold_handles_text_and_multi_column_keys() {
        let mut q = AggQuery::plain(vec![AggSpec::count_star()]);
        q.group_by = vec!["s".into(), "v".into()];
        let got = grouped(
            q,
            vec![batch2(
                vec![Some(1), Some(1), Some(2)],
                vec![Some("b"), Some("a"), Some("b")],
            )],
        );
        let keys: Vec<_> = got.iter().map(|g| g.key.clone()).collect();
        assert_eq!(
            keys,
            vec![
                vec![Some(AggScalar::Text("a".into())), Some(AggScalar::Int(1))],
                vec![Some(AggScalar::Text("b".into())), Some(AggScalar::Int(1))],
                vec![Some(AggScalar::Text("b".into())), Some(AggScalar::Int(2))],
            ],
            "多列键按**字典序**升序：{keys:?}"
        );
    }

    /// 分组与谓词**同时**生效（谓词先筛行，再分组）。
    #[test]
    fn grouped_fold_applies_the_predicate_before_grouping() {
        use crate::predicate::{CmpOp, Literal};
        let mut q = AggQuery::plain(vec![AggSpec::count_star()]);
        q.group_by = vec!["v".into()];
        q.predicate = Some(Predicate::Cmp {
            column: "v".into(),
            op: CmpOp::Gt,
            value: Literal::Int(1),
        });
        let got = grouped(q, vec![batch(vec![Some(1), Some(2), None, Some(2)])]);
        assert_eq!(
            got,
            vec![GroupStates {
                key: vec![Some(AggScalar::Int(2))],
                states: vec![PartialAgg::Count(2)],
            }],
            "只有 v=2 的两行（NULL 不放行、v=1 不满足）：{got:?}"
        );
    }

    /// 空输入的两种形状：**不分组 ⇒ 单位元**（不是空 Vec）；**分组 ⇒ 没有组**。
    #[test]
    fn empty_input_gives_identity_or_no_groups() {
        let plain = fold_query(
            &AggQuery::plain(vec![AggSpec::count_star(), AggSpec::new(AggKind::Min, "v")]),
            &[],
        )
        .unwrap();
        assert_eq!(
            plain,
            AggStates::Plain(vec![PartialAgg::Count(0), PartialAgg::Min(None)])
        );
        let mut q = AggQuery::plain(vec![AggSpec::count_star()]);
        q.group_by = vec!["v".into()];
        assert_eq!(fold_query(&q, &[]).unwrap(), AggStates::Grouped(vec![]));
    }

    /// 形状不一致 ⇒ **响亮失败**（"一边分组、一边不分组"是契约错，静默取一边会少算）。
    #[test]
    fn mismatched_states_are_loud() {
        let mut q = AggQuery::plain(vec![AggSpec::count_star()]);
        q.group_by = vec!["v".into()];
        let grouped_states = fold_query(&q, &[batch(vec![Some(1)])]).unwrap();
        let plain_states = fold_query(&AggQuery::plain(vec![AggSpec::count_star()]), &[]).unwrap();
        let e = merge_states(&grouped_states, &plain_states).unwrap_err();
        assert!(e.to_string().contains("形状不一致"), "{e}");
    }
}
