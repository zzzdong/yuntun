//! **文件级剪枝**（`§129`，plan `T13.2`）：让 `FileManifest.stats` 真的被查询侧用起来。
//!
//! # 之前是什么样
//!
//! `scan` 把 `filters` **整个丢掉**（形参名就叫 `_filters`），把所有已提交文件一股脑塞进一个
//! `FileGroup`；而 `compute_stats_lite` 那边 `min`/`max` 也**恒为空**（注释说"由文件 footer 提供"，
//! 但没人读 footer）⇒ **两头都缺**，"按统计剪枝"一行都没落地，只有一句自我感觉良好的注释。
//!
//! # 这一层只做一件**保守**的事
//!
//! 拿过滤器里能确定的那几条**单列比较**（`col <op> 字面量`），和文件自己的 `[min, max]` 比：
//! **能证明"这个文件不可能满足"就把它拿掉**；任何拿不准的情形一律**留下**
//! （"宁可少剪，不可错剪"——剪错的后果是**静默少数据**，那是本仓最不能接受的失败形态）。
//!
//! 具体地：
//! * `AND` 会**递归**（两侧都是必要条件 ⇒ 任一侧可剪则整条可剪）；
//! * `OR` / `NOT` / 其它形状 ⇒ **不做判断**（留下）；
//! * 列没有统计（空）、类型不可比、`NaN` ⇒ **不做判断**（留下）；
//! * 整列全 null（`null_count == row_count`）⇒ 比较谓词**必然不成立** ⇒ 可剪。

use arrow::datatypes::Schema;
use datafusion::logical_expr::{Expr, Operator};
use yuntun_model::meta::{decode_bound, StatBound, StatisticsLite};

/// 一条能从过滤器里读出来的**单列约束**。
///
/// `kind` 不是装饰：`StatBound` 把"天/秒/毫秒/纳秒"全压成 `I(x)`，而**单位不同的两个数
/// 直接比大小会剪错**（静默少数据）。所以凡"时间刻度"类的字面量，都必须与列类型**逐档核对**
/// （见 [`scalar_matches_column`]）。
#[derive(Debug, Clone)]
pub(crate) struct Constraint {
    pub(crate) column: String,
    pub(crate) op: Operator,
    pub(crate) value: StatBound,
    pub(crate) kind: ScalarKind,
}

/// 字面量的类型族（够用来判断"能不能和这一列比"）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScalarKind {
    Int,
    UInt,
    Float,
    Bytes,
    Date32,
    /// 时间戳：**四种刻度分开**（单位不同 ⇒ 数值不同 ⇒ 不可比）
    TsSecond,
    TsMilli,
    TsMicro,
    TsNano,
}

/// 把过滤器拆成"能用的单列约束"（`AND` 递归，其余形状放弃）。
///
/// `schema` 是必需的：约束能不能用，取决于"字面量与这一列是不是同一族"
/// —— 见 [`scalar_matches_column`]（`F.4` 的组级剪枝与文件级剪枝**共用**这一段）。
pub(crate) fn constraints(e: &Expr, schema: &Schema, out: &mut Vec<Constraint>) {
    match e {
        Expr::BinaryExpr(b) if b.op == Operator::And => {
            constraints(&b.left, schema, out);
            constraints(&b.right, schema, out);
        }
        Expr::BinaryExpr(b) => {
            let (col, op, lit) = match (b.left.as_ref(), b.right.as_ref()) {
                (Expr::Column(c), Expr::Literal(v, _)) => (c.name.clone(), b.op, v),
                // 字面量在左边 ⇒ 把操作符翻过来（`5 < a` 就是 `a > 5`）
                (Expr::Literal(v, _), Expr::Column(c)) => (c.name.clone(), flip(b.op), v),
                _ => return,
            };
            // 只有这六种能给出"文件级 / 组级"的确定性判断
            if !matches!(
                op,
                Operator::Gt
                    | Operator::GtEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Eq
                    | Operator::NotEq
            ) {
                return;
            }
            let Some((value, kind)) = literal_bound(lit) else {
                return;
            };
            // 列不在 schema 里、或字面量与列**不同族** ⇒ 这条约束**不可用**（宁可不剪）
            let Ok(f) = schema.field_with_name(&col) else {
                return;
            };
            if !scalar_matches_column(kind, f.data_type()) {
                return;
            }
            out.push(Constraint {
                column: col,
                op,
                value,
                kind,
            });
        }
        // 其它形状（OR / NOT / 函数 / IS NULL…）：不做判断
        _ => {}
    }
}

/// 字面量与这一列**能不能比**（不能 ⇒ 这条约束既不许剪文件、也不许剪组）。
///
/// 数值族之间放宽（位模式一致，DataFusion 本来也会先做类型强转），但**凡涉及"刻度"
/// （时间戳）就必须逐刻度相等**：`Timestamp(Millisecond)` 的列配上
/// `Timestamp(Nanosecond)` 的字面量，两边在 `StatBound` 里都是 `I(x)`，
/// 直接比大小会得出**完全错误**的结论 —— 后果是静默少数据。
pub(crate) fn scalar_matches_column(kind: ScalarKind, dt: &arrow::datatypes::DataType) -> bool {
    use arrow::datatypes::{DataType as D, TimeUnit};
    matches!(
        (kind, dt),
        (ScalarKind::Int | ScalarKind::UInt, D::Int8 | D::Int16 | D::Int32 | D::Int64)
            | (
                ScalarKind::Int | ScalarKind::UInt,
                D::UInt8 | D::UInt16 | D::UInt32 | D::UInt64
            )
            | (ScalarKind::Float, D::Float32 | D::Float64)
            | (
                ScalarKind::Bytes,
                D::Utf8 | D::LargeUtf8 | D::Binary | D::LargeBinary
            )
            | (ScalarKind::Date32, D::Date32)
            | (ScalarKind::TsSecond, D::Timestamp(TimeUnit::Second, _))
            | (ScalarKind::TsMilli, D::Timestamp(TimeUnit::Millisecond, _))
            | (ScalarKind::TsMicro, D::Timestamp(TimeUnit::Microsecond, _))
            | (ScalarKind::TsNano, D::Timestamp(TimeUnit::Nanosecond, _))
    )
}

fn flip(op: Operator) -> Operator {
    match op {
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        other => other,
    }
}

/// 字面量 → `(StatBound, 类型族)`（不认识的类型 ⇒ `None` ⇒ 不做判断）。
fn literal_bound(v: &datafusion::scalar::ScalarValue) -> Option<(StatBound, ScalarKind)> {
    use datafusion::scalar::ScalarValue as S;
    use ScalarKind as K;
    // ⚠️ `ScalarValue` 的数值变体带 `Option`（NULL 的可表示性）—— 这里只认**有值**的
    Some(match v {
        S::Int8(Some(x)) => (StatBound::I(*x as i64), K::Int),
        S::Int16(Some(x)) => (StatBound::I(*x as i64), K::Int),
        S::Int32(Some(x)) => (StatBound::I(*x as i64), K::Int),
        S::Int64(Some(x)) => (StatBound::I(*x), K::Int),
        S::UInt8(Some(x)) => (StatBound::U(*x as u64), K::UInt),
        S::UInt16(Some(x)) => (StatBound::U(*x as u64), K::UInt),
        S::UInt32(Some(x)) => (StatBound::U(*x as u64), K::UInt),
        S::UInt64(Some(x)) => (StatBound::U(*x), K::UInt),
        S::Float32(Some(x)) => (StatBound::F(*x as f64), K::Float),
        S::Float64(Some(x)) => (StatBound::F(*x), K::Float),
        S::Utf8(Some(x)) | S::LargeUtf8(Some(x)) => (StatBound::B(x.as_bytes().to_vec()), K::Bytes),
        S::Binary(Some(x)) | S::LargeBinary(Some(x)) => (StatBound::B(x.clone()), K::Bytes),
        // 时间刻度：`Date32` = 天；时间戳**四档分开标**（`scalar_matches_column` 逐档核对）
        S::Date32(Some(x)) => (StatBound::I(*x as i64), K::Date32),
        S::TimestampSecond(Some(x), _) => (StatBound::I(*x), K::TsSecond),
        S::TimestampMillisecond(Some(x), _) => (StatBound::I(*x), K::TsMilli),
        S::TimestampMicrosecond(Some(x), _) => (StatBound::I(*x), K::TsMicro),
        S::TimestampNanosecond(Some(x), _) => (StatBound::I(*x), K::TsNano),
        _ => return None,
    })
}

/// **可见文件 → 该读的文件**：`scan` 里那一步的**本体**（抽出来是为了能直接断言）。
///
/// 返回 `(保留的文件, 被剪掉的数量)`。
///
/// # 为什么把它抽成函数
///
/// `scan` 里原本只有三行接线，而"**哪些文件会留下**"这件事值得**直接**断言。
/// 抽出来之后，接线与判据共用同一段代码（编译器保证调用点就在那儿），不必靠
/// "跑一次查询看它快不快" —— 那是**测不出来**的（本地几百 KB 的夹具省下的 IO 不可观测）。
///
/// # 边界（`§129.4` 写明了）
///
/// **没有**断言"某次真实查询执行的**确没打开**那个文件"。做那件事需要真对象存储 +
/// 执行层插桩；目前靠两层单测把接缝守住：本函数（选对了文件）+ [`can_prune`]（判对了区间）。
pub fn prune_visible_files<'a>(
    schema: &Schema,
    files: &'a [yuntun_model::meta::FileManifest],
    filters: &[Expr],
) -> (Vec<&'a yuntun_model::meta::FileManifest>, usize) {
    let kept: Vec<&yuntun_model::meta::FileManifest> = files
        .iter()
        .filter(|f| {
            !can_prune(
                schema,
                &FileStats {
                    stats: f.stats.as_ref(),
                    row_count: f.row_count,
                },
                filters,
            )
        })
        .collect();
    let dropped = files.len() - kept.len();
    (kept, dropped)
}

/// 一个文件的统计 + 行数。
pub struct FileStats<'a> {
    pub stats: Option<&'a StatisticsLite>,
    pub row_count: u64,
}

/// **这个文件可以被剪掉吗**（返回 `true` = 不可能满足过滤器 ⇒ 读它纯属浪费）。
///
/// 任何拿不准的情形都返回 `false`（留下）。
pub fn can_prune(schema: &Schema, f: &FileStats<'_>, filters: &[Expr]) -> bool {
    let Some(stats) = f.stats else {
        return false; // 没有统计 ⇒ 不判断
    };
    let mut cs = Vec::new();
    for e in filters {
        constraints(e, schema, &mut cs);
    }
    can_prune_with(schema, stats, f.row_count, &cs)
}

/// 用**已解析的约束**判一次（组级剪枝要按组反复判，重复解析纯属浪费）。
///
/// `stats` / `row_count` 描述的是**被判定的那一块**：文件级传文件的，
/// 组级传那个组的（`F.4`）—— 同一套数学，两级共用。
pub(crate) fn can_prune_with(
    schema: &Schema,
    stats: &StatisticsLite,
    row_count: u64,
    cs: &[Constraint],
) -> bool {
    for c in cs {
        let Some(col) = stats.columns.iter().find(|s| s.name == c.column) else {
            continue; // 这一列没统计 ⇒ 不判断
        };
        // 整列全 null：比较谓词**必然不成立**（NULL 不满足任何比较）
        if row_count > 0 && col.null_count == row_count {
            return true;
        }
        let Ok(dt) = schema.field_with_name(&c.column).map(|fd| fd.data_type()) else {
            continue;
        };
        // 用**列类型**解出边界（两侧同一套编码，见 `yuntun_model::meta::decode_bound`）
        let (Some(lo), Some(hi)) = (
            decode_bound(dt, &col.min).filter(|b| b.comparable(&c.value)),
            decode_bound(dt, &col.max).filter(|b| b.comparable(&c.value)),
        ) else {
            continue;
        };
        if violates(&lo, &hi, c.op, &c.value) {
            return true;
        }
    }
    false
}

/// `[lo, hi]` 这个区间**不可能**满足 `col <op> value` 吗？
///
/// 每个分支都对着"区间与解集无交集"来读；`partial_cmp` 给 `None`（NaN / 不可比）
/// ⇒ 返回 `false`（不判断）。
fn violates(lo: &StatBound, hi: &StatBound, op: Operator, value: &StatBound) -> bool {
    use std::cmp::Ordering;
    match op {
        // col > value：整个区间都 <= value ⇒ 无解
        Operator::Gt => matches!(hi.partial_cmp(value), Some(Ordering::Less | Ordering::Equal)),
        // col >= value：整个区间都 < value ⇒ 无解
        Operator::GtEq => matches!(hi.partial_cmp(value), Some(Ordering::Less)),
        // col < value：整个区间都 >= value ⇒ 无解
        Operator::Lt => matches!(
            lo.partial_cmp(value),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        // col <= value：整个区间都 > value ⇒ 无解
        Operator::LtEq => matches!(lo.partial_cmp(value), Some(Ordering::Greater)),
        // col = value：value 落在区间外 ⇒ 无解
        Operator::Eq => matches!(
            value.partial_cmp(lo),
            Some(Ordering::Less)
        ) || matches!(hi.partial_cmp(value), Some(Ordering::Less)),
        // col != value：**只有**区间退化成这一点（min == max == value）时才无解
        Operator::NotEq => {
            matches!(lo.partial_cmp(value), Some(Ordering::Equal))
                && matches!(hi.partial_cmp(value), Some(Ordering::Equal))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use yuntun_model::meta::{encode_bound, ColumnStatLite};

    fn schema_i64() -> Schema {
        Schema::new(vec![arrow::datatypes::Field::new("a", DataType::Int64, true)])
    }

    fn stats(lo: i64, hi: i64, nulls: u64) -> StatisticsLite {
        StatisticsLite {
            columns: vec![ColumnStatLite {
                name: "a".into(),
                min: encode_bound(&DataType::Int64, &StatBound::I(lo)),
                max: encode_bound(&DataType::Int64, &StatBound::I(hi)),
                null_count: nulls,
            }],
        }
    }

    fn filter(op: Operator, v: i64) -> Expr {
        use datafusion::prelude::*;
        let col = col("a");
        let lit = lit(v);
        match op {
            Operator::Gt => col.gt(lit),
            Operator::GtEq => col.gt_eq(lit),
            Operator::Lt => col.lt(lit),
            Operator::LtEq => col.lt_eq(lit),
            Operator::Eq => col.eq(lit),
            Operator::NotEq => col.not_eq(lit),
            _ => unreachable!(),
        }
    }

    fn pruned(lo: i64, hi: i64, op: Operator, v: i64) -> bool {
        let s = schema_i64();
        let st = stats(lo, hi, 0);
        can_prune(
            &s,
            &FileStats {
                stats: Some(&st),
                row_count: 10,
            },
            &[filter(op, v)],
        )
    }

    /// 区间与解集**无交集**的四种基本情形：剪。
    #[test]
    fn disjoint_ranges_are_pruned() {
        assert!(pruned(1, 5, Operator::Gt, 9), "区间上界 5 <= 9 ⇒ 无解");
        assert!(pruned(1, 5, Operator::GtEq, 6), "上界 5 < 6 ⇒ 无解");
        assert!(pruned(10, 20, Operator::Lt, 10), "下界 10 >= 10 ⇒ 无解");
        assert!(pruned(10, 20, Operator::LtEq, 9), "下界 10 > 9 ⇒ 无解");
        assert!(pruned(10, 20, Operator::Eq, 9), "9 落在区间外 ⇒ 无解");
        assert!(pruned(10, 20, Operator::Eq, 21));
    }

    /// 区间与解集**有交集**：绝不剪（剪错 = 静默少数据）。
    #[test]
    fn overlapping_ranges_are_kept() {
        // ⚠️ 严格比较的"差一位"就在这里： 而文件上界**正好**是 5 ⇒ **无解**（该剪）。
        // 本用例第一版把这条写成了"保留"，是**测试错**（剪枝器是对的）—— 单测的价值就在这。
        assert!(pruned(1, 5, Operator::Gt, 5), "严格 > ：上界等于字面量 ⇒ 无解");
        assert!(!pruned(1, 5, Operator::Gt, 4), "上界 5 > 4 ⇒ 可能命中");
        assert!(pruned(1, 5, Operator::GtEq, 6));
        assert!(!pruned(1, 5, Operator::Lt, 5));
        assert!(!pruned(1, 5, Operator::Eq, 5));
        assert!(!pruned(1, 5, Operator::Eq, 1));
        assert!(!pruned(1, 5, Operator::GtEq, 1));
    }

    /// `!=` 只在区间退化成一点且等于该值时才能剪 —— 这是最容易写错的一条。
    #[test]
    fn not_eq_only_prunes_a_degenerate_range() {
        assert!(pruned(7, 7, Operator::NotEq, 7), "min==max==7 ⇒ 全是 7 ⇒ 无解");
        assert!(!pruned(7, 8, Operator::NotEq, 7), "区间里还有 8 ⇒ 可能有解");
    }

    /// 拿不准的一律留下：没统计、列不存在、类型不可比（这里用字符串统计配数值谓词）、全 null。
    #[test]
    fn uncertain_cases_are_kept() {
        let s = schema_i64();
        // ① 没有统计
        assert!(!can_prune(
            &s,
            &FileStats {
                stats: None,
                row_count: 10
            },
            &[filter(Operator::Gt, 100)]
        ));
        // ② 列的边界为空（`compute_stats_lite` 对不支持的列就是留空）
        let empty = StatisticsLite {
            columns: vec![ColumnStatLite {
                name: "a".into(),
                min: vec![],
                max: vec![],
                null_count: 0,
            }],
        };
        assert!(!can_prune(
            &s,
            &FileStats {
                stats: Some(&empty),
                row_count: 10
            },
            &[filter(Operator::Gt, 100)]
        ));
        // ③ 谓词形状不认识（`OR`）⇒ 不判断
        use datafusion::prelude::*;
        let or = col("a").gt(lit(100)).or(col("a").lt(lit(-100)));
        let st = stats(1, 5, 0);
        assert!(!can_prune(
            &s,
            &FileStats {
                stats: Some(&st),
                row_count: 10
            },
            &[or]
        ));
    }

    /// `AND` 递归：任一侧可剪 ⇒ 整条可剪（两侧都是必要条件）。
    #[test]
    fn and_is_recursive() {
        use datafusion::prelude::*;
        let s = schema_i64();
        let st = stats(1, 5, 0);
        let f = col("a").gt(lit(1)).and(col("a").gt(lit(9)));
        assert!(
            can_prune(
                &s,
                &FileStats {
                    stats: Some(&st),
                    row_count: 10
                },
                &[f]
            ),
            "右半边（a > 9）已经无解 ⇒ 整条 AND 无解"
        );
    }

    /// 整列全 null ⇒ 任何比较谓词都不成立 ⇒ 剪。
    #[test]
    fn all_null_column_is_pruned() {
        let s = schema_i64();
        let st = stats(0, 0, 10);
        assert!(can_prune(
            &s,
            &FileStats {
                stats: Some(&st),
                row_count: 10
            },
            &[filter(Operator::Gt, -1)]
        ));
    }

    // ---------------- 接线本体（`prune_visible_files`）----------------

    fn manifest(path: &str, lo: i64, hi: i64, rows: u64) -> yuntun_model::meta::FileManifest {
        yuntun_model::meta::FileManifest {
            file_path: path.into(),
            row_count: rows,
            stats: Some(stats(lo, hi, 0)),
            ..Default::default()
        }
    }

    /// **该留下的留下、该剪的剪掉** —— 这条断言的就是 `scan` 里那一步本身。
    #[test]
    fn visible_files_are_pruned_by_manifests() {
        use datafusion::prelude::*;
        let s = schema_i64();
        let files = vec![
            manifest("s3://bucket/f-old.parquet", 1, 5, 10),
            manifest("s3://bucket/f-new.parquet", 100, 200, 10),
        ];
        // `a > 50`：老文件的上界 5 <= 50 ⇒ 无解 ⇒ 剪掉
        let f = col("a").gt(lit(50));
        let (kept, dropped) = prune_visible_files(&s, &files, &[f]);
        assert_eq!(dropped, 1, "老文件应当被剪掉");
        assert_eq!(kept.len(), 1);
        assert_eq!(
            kept[0].file_path, "s3://bucket/f-new.parquet",
            "留下的必须是**可能命中**的那个：{kept:?}"
        );

        // 没有过滤 ⇒ 一个都不剪（**不能顺手剪**：没有谓词就没有依据）
        let (kept, dropped) = prune_visible_files(&s, &files, &[]);
        assert_eq!((kept.len(), dropped), (2, 0), "无谓词 ⇒ 全留");

        // 谓词覆盖两个文件 ⇒ 一个都不剪（只在**证明无解**时才剪）
        let f = col("a").gt(lit(-1_000));
        let (kept, dropped) = prune_visible_files(&s, &files, &[f]);
        assert_eq!((kept.len(), dropped), (2, 0), "都可能命中 ⇒ 全留");
    }

    /// 没有统计的文件**永远**留下（旧数据 / 嵌套类型列都是这种）。
    #[test]
    fn files_without_stats_are_never_pruned() {
        use datafusion::prelude::*;
        let s = schema_i64();
        let files = vec![yuntun_model::meta::FileManifest {
            file_path: "s3://bucket/no-stats.parquet".into(),
            row_count: 10,
            ..Default::default() // `stats: None`
        }];
        let (kept, dropped) = prune_visible_files(&s, &files, &[col("a").gt(lit(1_000))]);
        assert_eq!((kept.len(), dropped), (1, 0), "没有统计 ⇒ 不许剪");
    }
}
