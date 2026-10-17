//! **冷文件的就地折叠**（`§178`，N4b / 归档 `S5-2` 的"远端冷读"那一半）。
//!
//! # 它做什么
//!
//! 冷文件在**共享**对象存储上 ⇒ 任何节点都能读。这一层回答的是：
//! **"给你这几个文件（和它们身上生效的 DV），按这个查询把它们折成部分状态"** ——
//! 网络里走的因此是**标量**，不是行（这是 §178 摸底得到的关键结论：远端扫描后回传原始行
//! **省不了字节**，要省字节就得就地折叠）。
//!
//! # 三条纪律
//!
//! 1. **请求自足**：要读哪些文件、每份文件上生效哪几份 DV，全部由调用方给进来
//!    （[`CatalogSnapshot::from_parts`]）。被分配到的节点**不许**去查自己的目录 ——
//!    `RemoteCatalog` 没有 `wait_for(version ≥ N)`，它可能读到**另一个快照**的文件集/DV 集，
//!    那是**静默错**（漏一份 DV = 复活已删的行）。
//! 2. **读路径与协调者同一套代码**：走 `YuntunTableProvider`（可见性 / DV / schema 对齐 /
//!    vortex-parquet 分流都是它），**不**另写一个"简化版解码器" ——
//!    两套实现会在某个边界上漂，而 DV 那一侧的漂移是"已删行复活"。
//! 3. **折叠与热数据同一套代码**：[`yuntun_model::agg::fold_query`] ⇒
//!    聚合/谓词/分组的语义与 `§173`/`§176`/`§177` 逐字相同。
//!
//! # 边界
//!
//! - **不剪枝、不索引**：这里读的是"分配给我这几个文件"的**全部行**（`index_pruning = false`），
//!   文件级/行组级剪枝是下一刀（需要把剪枝条件也过网）。
//! - **不读热数据**：`hot_enabled = false`，热的一半仍由归属节点单独算（归属绑定，不能分配）。
//! - DV 取不到 ⇒ **响亮失败**（与读路径同一条纪律：`file_deletions` 失败一律报错，绝不降级）。

use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use datafusion::prelude::{Expr, SessionContext, col, lit};
use yuntun_model::agg::{AggQuery, AggStates};
use yuntun_model::dv::DeletionEntry;
use yuntun_model::predicate::{CmpOp, Literal, Predicate};
use yuntun_model::error::LakeError;
use yuntun_model::meta::FileManifest;

use crate::cache::CatalogSnapshot;
use crate::partial::{PartialPolicy, PartialSink};
use crate::table::{ReadPath, YuntunTableProvider};

/// DV 的读取预算：与热读同一个概念（"这次愿意为它等多久"），取一个保守值。
const DV_BUDGET: Duration = Duration::from_secs(5);

/// 把**可下推的谓词**翻译成 DataFusion 表达式（`§183`）。
///
/// 为什么要它：`§182` 把一部分冷读分给了远端，而远端当时是**整文件读** —— 既不按
/// `FileManifest.stats` 做**文件级**剪枝，也不把条件交给 `ParquetSource` 做**行组级**跳读。
/// 那不是"少个优化"，而是**分配引入的回退**：同一条查询，分出去的那半边比留在本地的那半边
/// 多读了数据。把谓词交给 DataFusion 之后，两个半边走的是**同一条读路径**（含剪枝）。
///
/// 翻不出来 ⇒ `None`（不剪，只折）：宁可多读，也不许剪错（剪错 = 静默少数据）。
fn predicate_to_expr(p: &Predicate) -> Option<Expr> {
    Some(match p {
        Predicate::Cmp { column, op, value } => {
            let lhs = col(column);
            let rhs = match value {
                Literal::Int(i) => lit(*i),
                Literal::Text(t) => lit(t.clone()),
            };
            match op {
                CmpOp::Eq => lhs.eq(rhs),
                CmpOp::Ne => lhs.not_eq(rhs),
                CmpOp::Lt => lhs.lt(rhs),
                CmpOp::Le => lhs.lt_eq(rhs),
                CmpOp::Gt => lhs.gt(rhs),
                CmpOp::Ge => lhs.gt_eq(rhs),
            }
        }
        Predicate::IsNull { column, negated } => {
            if *negated {
                col(column).is_not_null()
            } else {
                col(column).is_null()
            }
        }
        Predicate::And(parts) => {
            let mut out: Option<Expr> = None;
            for p in parts {
                let e = predicate_to_expr(p)?;
                out = Some(match out {
                    None => e,
                    Some(acc) => acc.and(e),
                });
            }
            out?
        }
        Predicate::Or(parts) => {
            let mut out: Option<Expr> = None;
            for p in parts {
                let e = predicate_to_expr(p)?;
                out = Some(match out {
                    None => e,
                    Some(acc) => acc.or(e),
                });
            }
            out?
        }
    })
}

/// 把**给定的这几个冷文件**按 `query` 折成部分状态。
///
/// `ident` 是全限定表名（只用于日志/标识 —— **不**用它去查任何目录）；
/// `files` 是分配给这一支的文件；`deletions` 是在这次快照下、锚定在**这些文件**上且生效的 DV
/// （调用方负责筛好：本文件外的 DV 与本次无关）。
pub async fn fold_cold_files(
    store: Arc<dyn object_store::ObjectStore>,
    ident: &str,
    schema: SchemaRef,
    files: Vec<FileManifest>,
    deletions: Vec<DeletionEntry>,
    query: &AggQuery,
) -> Result<AggStates, LakeError> {
    // ① 一次性快照（**不**查目录 —— 见模块文档纪律 1）
    let snapshot = Arc::new(CatalogSnapshot::from_parts(
        ident,
        schema.clone(),
        files,
        deletions,
    ));
    let read = ReadPath {
        store: store.clone(),
        hot_read_budget: DV_BUDGET,
        // 远端折叠读的是"分配给我的这几个文件"的**全部行**：剪枝条件要过网才能剪，
        // 那是下一刀（见模块文档的边界）
        index_pruning: false,
        // 热数据**归属绑定**，不参与分配 ⇒ 这里只读冷的一半
        hot_enabled: false,
        only_files: None,
        // `§193`：冷读解码预算在**协调者**那一侧执行（`QueryEngine` 的扫描）；
        // 远端就地折叠读的是"分配给我的这几个文件"，由各自的节点按自己的内存说话 ——
        // 这里不重复套一层（拿协调者的数字去卡别的节点是错的）。
        cold_read_budget_bytes: 0,
    };
    let provider = Arc::new(YuntunTableProvider::new(
        snapshot,
        ident,
        schema,
        std::collections::BTreeMap::new(), // 没有热来源
        // 读不到 DV / 解不开文件 ⇒ **响亮失败**（降级会复活已删的行）
        Arc::new(PartialSink::new(PartialPolicy::Reject)),
        read,
    ));

    // ② 用 DataFusion 读（**同一条读路径** ⇒ 语义不会与协调者那侧分叉）
    let ctx = SessionContext::new();
    // 与协调者**同一个** URL 字符串（`STORE_URL`）：provider 那边用它做 `ObjectStoreUrl`，
    // 会话这边要的是 `url::Url` —— 两边必须对得上，否则 DataFusion 找不到这个 store。
    let url: url::Url = crate::STORE_URL
        .parse()
        .map_err(|e| LakeError::Other(format!("invalid store url: {e}")))?;
    ctx.register_object_store(&url, store);
    // `read_table` 收的是 trait 对象 ⇒ 这里显式擦一次类型
    let provider: Arc<dyn datafusion::catalog::TableProvider> = provider;
    let df = ctx
        .read_table(provider)
        .map_err(|e| LakeError::Other(format!("冷文件折叠：注册失败：{e}")))?;
    // `§183`：**把谓词交给读的那一边** ⇒ 文件级（`FileManifest.stats`）与行组级
    // （`ParquetSource` 的 footer 统计）剪枝都和本地那半边一样生效。
    // 翻不出来 ⇒ 不剪（只折）：剪错是静默少数据，多读只是慢。
    let df = match query.predicate.as_ref().and_then(predicate_to_expr) {
        Some(e) => df
            .filter(e)
            .map_err(|e| LakeError::Other(format!("冷文件折叠：过滤失败：{e}")))?,
        None => df,
    };
    let batches = df
        .collect()
        .await
        .map_err(|e| LakeError::Other(format!("冷文件折叠：读取失败：{e}")))?;

    // ③ 折叠（与热数据**同一份代码**）
    yuntun_model::agg::fold_query(query, &batches)
}
