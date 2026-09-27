//! **组级剪枝**（`plan.md` F.4）：把索引文件（`yuntun_model::index`）变成
//! "这个文件的哪些行组**可以不读**"，交给 DataFusion 的 parquet 读取器执行。
//!
//! # 两级剪枝的关系
//!
//! ```text
//! ① 文件级（`§129`，prune::prune_visible_files）  用 FileManifest.stats  → 决定"读哪些文件"
//! ② 组级  （本模块，F.4）                        用 {file}.idx 的逐组索引 → 决定"文件里读哪些组"
//! ```
//!
//! 两级的**判定数学是同一套**（`prune::can_prune_with`：min/max 与谓词区间求交），
//! 差别只在"被判定的块有多大"。这是刻意的：两套判据必然会漂移，而这里漂移的代价是**少数据**。
//!
//! # 新的那一层能力：XOR filter
//!
//! zone map 只能回答"该值落在这一组的**[min,max]** 之外吗"。本项目**写入侧并不排序**
//! （`event_time` 只是业务约定），于是组的 min/max 经常很宽 —— 这时只有**等值 filter**
//! 能证明"这个值不在这一组里"。反之，纯 zone map 的组级剪枝对 parquet 而言与
//! DataFusion 自己用 footer 统计做的那层**重复**（`§132` 已实测过那条线）。
//!
//! # 怎么让 DataFusion 真的跳过组
//!
//! 把 [`ParquetAccessPlan`] 放进 `PartitionedFile.extensions` —— 这是 DF 公开的扩展点
//! （`datafusion-datasource-parquet::access_plan` 的模块文档就是这么写的）：
//! DF 的 opener 会以它作为**初始**计划，再往上叠加它自己的 footer 统计剪枝。
//!
//! ⚠️ DF 要求计划长度**等于文件的行组数**，否则直接报错（`Invalid ParquetAccessPlan for …`）
//! ⇒ 挂之前必须核对形状（`IndexFile::matches_shape`：组大小 + 行数 + 组数三者一致），
//! 不一致就**不挂**。"硬凑"会把一次本来正确的查询弄成失败。

use crate::prune::{self, Constraint};
use arrow::datatypes::Schema;
use datafusion::logical_expr::{Expr, Operator};
use datafusion_datasource_parquet::ParquetAccessPlan;
use yuntun_model::index::{IndexFile, IndexKey, INDEX_GROUP_ROWS};
use yuntun_model::meta::{StatBound, StatisticsLite};

/// 一个候选文件的**行组访问计划**。
///
/// 返回 `None` = **不剪**（回调方原样全读）。四类"不剪"都在这条路上：
///
/// * 没有过滤器（没有依据就不剪 —— 与 `§129` 同一条纪律）；
/// * 索引形状对不上文件（组大小/行数/组数任一不符）；
/// * 过滤器里没有一条能用的单列约束（形状不认识 / 类型族对不上）；
/// * **一个组都没剪掉**。
///
/// ⚠️ 最后一条不只是省开销：它让"`Some(plan)` ⟺ **确实有组被跳过**"成立，
/// 于是用例可以**直接断言这件事**（而不是断言"挂了个计划"）。
pub(crate) fn group_access_plan(
    schema: &Schema,
    index: &IndexFile,
    row_count: u64,
    filters: &[Expr],
) -> Option<ParquetAccessPlan> {
    if filters.is_empty() || !index.matches_shape(row_count) {
        return None;
    }
    let mut cs: Vec<Constraint> = Vec::new();
    for e in filters {
        prune::constraints(e, schema, &mut cs);
    }
    if cs.is_empty() {
        return None;
    }

    let mut plan = ParquetAccessPlan::new_all(index.groups.len());
    let mut skipped = 0usize;
    for g in 0..index.groups.len() {
        let start = g * INDEX_GROUP_ROWS;
        let group_rows = (row_count as usize).saturating_sub(start).min(INDEX_GROUP_ROWS) as u64;
        // 组级 zone map：把这一组的列统计喂给**同一套**判定
        let stats = StatisticsLite {
            columns: index.groups[g].columns.clone(),
        };
        let mut skip = prune::can_prune_with(schema, &stats, group_rows, &cs);
        if !skip {
            // 等值路径（zone map 做不到的那半）
            skip = cs.iter().any(|c| {
                c.op == Operator::Eq
                    && xor_key(c).is_some_and(|k| {
                        index.definitely_absent(g, &c.column, &k) == Some(true)
                    })
            });
        }
        if skip {
            plan.skip(g);
            skipped += 1;
        }
    }
    (skipped > 0).then_some(plan)
}

/// 约束 → XOR 键（`None` = 这一条**不参与**等值剪枝）。
///
/// 这里能成立，靠的是 `prune::scalar_matches_column` 已经保证"字面量与列同族"：
/// 整型 ↔ 整型（位模式一致）、字节 ↔ 字节、**时间戳 ↔ 同刻度的列**（原始计数一致）。
/// 浮点列一律不参与 —— `=` 在浮点上有 `NaN` / `-0.0` 这类边界，用它删组风险大于收益。
fn xor_key(c: &Constraint) -> Option<IndexKey> {
    match (c.kind, &c.value) {
        (
            prune::ScalarKind::Int
            | prune::ScalarKind::Date32
            | prune::ScalarKind::TsSecond
            | prune::ScalarKind::TsMilli
            | prune::ScalarKind::TsMicro
            | prune::ScalarKind::TsNano,
            StatBound::I(x),
        ) => Some(IndexKey::Int(*x)),
        (prune::ScalarKind::UInt, StatBound::U(x)) => Some(IndexKey::UInt(*x)),
        (prune::ScalarKind::Bytes, StatBound::B(b)) => Some(IndexKey::Bytes(b.clone())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::*;
    use std::sync::Arc;

    /// 两个行组，每组 `INDEX_GROUP_ROWS` 行：
    /// 组 0 的 event_time 只有偶数（0..2N 的偶数），组 1 是另一段区间。
    ///
    /// 这样"组 0 里没有的奇数"就成了**只有等值 filter 能证明**的例子：
    /// 组 0 的 zone map 是 `[0, 2N-2]`，落在区间内的值（比如 1）区间判定**剪不掉**。
    fn two_group_batch() -> RecordBatch {
        let n = INDEX_GROUP_ROWS;
        let mut ts: Vec<i64> = Vec::with_capacity(2 * n);
        for i in 0..n {
            ts.push((i as i64) * 2); // 组 0：0,2,4,…
        }
        for i in 0..n {
            ts.push(10_000_000 + (i as i64) * 2); // 组 1：另一段
        }
        let user: Vec<String> = (0..2 * n).map(|i| format!("u{i}")).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_time", DataType::Int64, true),
            Field::new("user", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ts)),
                Arc::new(StringArray::from(user)),
            ],
        )
        .unwrap()
    }

    fn index_of(b: &RecordBatch) -> IndexFile {
        IndexFile::build(b, &["event_time".to_string(), "user".to_string()]).unwrap()
    }

    /// **核心用例**：区间内的"洞"（组 0 没有的奇数 1）只有 filter 能证明 ⇒ 只剪组 0。
    #[test]
    fn only_the_equality_filter_can_skip_a_group_with_a_wide_zone_map() {
        let b = two_group_batch();
        let idx = index_of(&b);
        let schema = b.schema();
        // `event_time = 1`：落在组 0 的 [0, 2N-2] 区间**内**（zone map 剪不掉），
        // 而 1 是奇数 ⇒ 组 0 里根本没有；组 1 区间离得更远（zone map 就能剪）
        let f = col("event_time").eq(lit(1i64));
        let plan = group_access_plan(&schema, &idx, b.num_rows() as u64, &[f])
            .expect("应当能剪掉组（否则索引白做）");
        assert!(!plan.should_scan(0), "组 0 的 zone map 剪不掉，必须由 filter 剪");
        assert_eq!(plan.len(), 2, "计划长度 = 行组数（DF 的硬要求）");
    }

    /// zone map 那一半：**区间外的组**照样剪（`event_time = 5_000_000` 落在两组之间）。
    #[test]
    fn zone_map_prunes_groups_outside_the_range() {
        let b = two_group_batch();
        let idx = index_of(&b);
        let f = col("event_time").gt(lit(i64::MAX / 2));
        let plan = group_access_plan(&b.schema(), &idx, b.num_rows() as u64, &[f])
            .expect("两组的上界都 < 字面量 ⇒ 都能剪");
        assert!(!plan.should_scan(0) && !plan.should_scan(1));
    }

    /// **拿不准一律不剪**：没有过滤器 / 形状对不上 / 形状不认识的谓词。
    #[test]
    fn uncertain_cases_yield_no_plan() {
        let b = two_group_batch();
        let idx = index_of(&b);
        let rows = b.num_rows() as u64;

        // ① 没有过滤：没有依据
        assert!(group_access_plan(&b.schema(), &idx, rows, &[]).is_none());
        // ② OR 形状不认识（`constraints` 放弃它）
        let f = col("event_time").eq(lit(1i64)).or(col("event_time").eq(lit(3i64)));
        assert!(group_access_plan(&b.schema(), &idx, rows, &[f]).is_none());
        // ③ 形状对不上（行数改了）
        assert!(group_access_plan(&b.schema(), &idx, rows + 1, &[col("event_time").eq(lit(1i64))]).is_none());
        // ④ 谓词类型与列不同族（字符串字面量配整型列）⇒ 约束被丢掉
        let f = col("event_time").eq(lit("1"));
        assert!(group_access_plan(&b.schema(), &idx, rows, &[f]).is_none());
        // ⑤ 一个组都剪不掉 ⇒ 不给计划（`Some` 必然意味着真剪了）
        let f = col("event_time").gt(lit(-1i64)); // 两组都可能命中
        assert!(group_access_plan(&b.schema(), &idx, rows, &[f]).is_none());
    }

    /// 列不在索引里 / 索引里没有该列的 filter ⇒ 不剪（`Option::None` 一路传下去）。
    #[test]
    fn unknown_column_cannot_skip() {
        let b = two_group_batch();
        let idx = IndexFile::build(&b, &["user".to_string()]).unwrap();
        // `event_time` 没进索引 ⇒ zone map 与 filter 都没有 ⇒ 不给计划
        let f = col("event_time").eq(lit(1i64));
        assert!(group_access_plan(&b.schema(), &idx, b.num_rows() as u64, &[f]).is_none());
    }

    /// 时间戳列：**同刻度**才能剪（这一条是"会不会剪错"的护栏）。
    #[test]
    fn timestamp_needs_matching_unit() {
        use arrow::array::TimestampMillisecondArray;
        use arrow::datatypes::TimeUnit;
        let n = INDEX_GROUP_ROWS;
        let ms: Vec<i64> = (0..2 * n as i64).map(|i| i * 1000).collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]));
        let b = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(TimestampMillisecondArray::from(ms))],
        )
        .unwrap();
        let idx = IndexFile::build(&b, &["event_time".to_string()]).unwrap();
        let rows = b.num_rows() as u64;

        // 同刻度（毫秒）且值落在组 0 的"洞"里：`i*1000` 全是 1000 的倍数 ⇒ 1001 不在组 0
        let f = col("event_time").eq(lit(
            datafusion::scalar::ScalarValue::TimestampMillisecond(Some(1001), None),
        ));
        let plan = group_access_plan(&schema, &idx, rows, &[f])
            .expect("同刻度的等值谓词应当能剪");
        assert!(!plan.should_scan(0));

        // **不同刻度**（纳秒字面量配毫秒列）：整条约束必须被丢掉（否则数值直接比大小 ⇒ 剪错）
        let f = col("event_time").eq(lit(
            datafusion::scalar::ScalarValue::TimestampNanosecond(Some(1), None),
        ));
        assert!(
            group_access_plan(&schema, &idx, rows, &[f]).is_none(),
            "刻度不同的字面量绝不许参与剪枝（`scalar_matches_column` 的护栏）"
        );
    }
}
