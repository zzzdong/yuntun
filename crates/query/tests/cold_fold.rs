//! `§178`：**冷文件的就地折叠** —— 远端冷读那一半的第一层。
//!
//! 三条判据（第二条是这一刀存在的理由）：
//! ① 与**协调者自己的读路径对拍**：同样的文件、同样的查询 ⇒ 同样的部分状态
//!    （用 `fold_cold_files` 而不是另写一个解码器，就是为了让这条天然成立）；
//! ② **DV 必须生效**：删掉的行不许被算进来。漏一份 DV = 复活已删的行 —— 比少数据更糟，
//!    而它**不报错**，所以必须钉数字（删前/删后各一遍）；
//! ③ 谓词与分组都在这条路上生效（`§176`/`§177` 的契约在这里复用）。

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_model::agg::{AggKind, AggQuery, AggScalar, AggSpec, AggStates, PartialAgg};
use yuntun_model::dv::{DeletionEntry, DvBitmap};
use yuntun_model::error::LakeError;
use yuntun_model::meta::FileManifest;
use yuntun_model::ops::CreateTableRequest;
use yuntun_query::{QueryEngine, LocalCatalog, cold, placement};
use yuntun_store::{StoreConfig, create_store};

const TABLE: &str = "public.cold";
const SHARD: &str = "s0";
const WINDOW: &str = "w";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]))
}

fn batch(vals: &[Option<i64>]) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
}

struct Fx {
    store: Arc<dyn object_store::ObjectStore>,
    catalog: Arc<dyn CatalogOps>,
    engine: QueryEngine,
    a_path: String,
}

async fn setup() -> Fx {
    let store = create_store(&StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    catalog
        .create_table(CreateTableRequest {
            name: "cold".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig::standard(),
        })
        .await
        .unwrap();
    let (a_path, a_size, a_rows) = yuntun_format::write_batch(
        &store,
        TABLE,
        SHARD,
        WINDOW,
        "b-a",
        &batch(&[Some(1), Some(2), Some(3)]),
        yuntun_format::DataFormat::Parquet,
    )
    .await
    .unwrap();
    let (b_path, b_size, b_rows) = yuntun_format::write_batch(
        &store,
        TABLE,
        SHARD,
        WINDOW,
        "b-b",
        &batch(&[Some(10), Some(20), Some(30)]),
        yuntun_format::DataFormat::Parquet,
    )
    .await
    .unwrap();
    for (id, path, size, rows) in [
        ("b-a", &a_path, a_size, a_rows),
        ("b-b", &b_path, b_size, b_rows),
    ] {
        catalog
            .commit_files(yuntun_model::ops::CommitFilesRequest {
                table: TABLE.into(),
                batch_id: id.into(),
                client_request_id: None,
                client_request_ids: vec![],
                shard: SHARD.into(),
                time_window: WINDOW.into(),
                files: vec![FileManifest {
                    file_path: path.clone(),
                    batch_id: id.into(),
                    // ⚠️ **必须是真实字节数**：DataFusion 按它从对象存储取字节
                    // （填 1 会读到"至少 8 字节的 parquet"都不够，报错倒是会报，
                    //  但那种失败看起来像"文件坏了"，实际是清单填错了）
                    file_size: size,
                    row_count: rows,
                    table: TABLE.into(),
                    shard: SHARD.into(),
                    time_window: WINDOW.into(),
                    ..Default::default()
                }],
                schema_version: 1,
                row_count: rows,
            })
            .await
            .unwrap();
    }
    let cache = Arc::new(LocalCatalog::new());
    cache.set_catalog_ops(catalog.clone());
    cache.refresh(&catalog).await.unwrap();
    Fx {
        engine: QueryEngine::new(store.clone(), cache),
        a_path,
        store,
        catalog,
    }
}

impl Fx {
    /// 登记一批删除（**先落位图对象、再进目录** —— 与生产执行顺序一致）。
    async fn delete(&self, dv_id: &str, file: &str, positions: &[u32]) {
        let dv = DvBitmap::from_positions(positions.iter().copied());
        let entry = DeletionEntry {
            dv_id: dv_id.into(),
            table: TABLE.into(),
            file_path: file.into(),
            batch_id: if file == self.a_path { "b-a" } else { "b-b" }.into(),
            applied_at: 0,
            revoked_at: 0,
            card: dv.card() as u32,
            store_path: yuntun_model::dv::dv_object_path(file, dv_id),
        };
        yuntun_store::put_bytes(&*self.store, &entry.store_path, dv.to_bytes())
            .await
            .unwrap();
        self.catalog.apply_deletions(vec![entry]).await.unwrap();
        self.engine.catalog().refresh(&self.catalog).await.unwrap();
    }

    /// 把"这次查询要读的文件 + 这些文件上生效的 DV"按请求自足的方式拿出来
    /// （这正是下一刀要在协议里带过去的东西）。
    fn cold_request(&self) -> (Vec<FileManifest>, Vec<DeletionEntry>) {
        let snap = self.engine.snapshot();
        let t = snap.get(TABLE).expect("快照里要有这张表");
        (t.files.clone(), t.deletions.clone())
    }

    fn files_of(&self) -> Vec<FileManifest> {
        self.engine
            .snapshot()
            .get(TABLE)
            .expect("快照里要有这张表")
            .files
            .clone()
    }
}

/// 从引擎的**一行一组**结果里读出 `(键, count)` 对（升序）。
async fn grouped_pairs(engine: &QueryEngine, sql: &str) -> Vec<(i64, i64)> {
    let out = engine.sql(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    let b = &out[0];
    let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let c = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    let mut got: Vec<(i64, i64)> = (0..b.num_rows()).map(|i| (k.value(i), c.value(i))).collect();
    got.sort();
    got
}

/// ① 对拍：不分组 —— 冷折叠的结果 == 协调者自己查出来的数。
#[tokio::test]
async fn cold_fold_matches_the_coordinators_own_query() {
    let fx = setup().await;
    let (files, dvs) = fx.cold_request();
    let q = AggQuery::plain(vec![
        AggSpec::count_star(),
        AggSpec::new(AggKind::SumInt, "v"),
    ]);
    let states = cold::fold_cold_files(
        fx.store.clone(),
        TABLE,
        schema(),
        files.clone(),
        dvs.clone(),
        &q,
    )
    .await
    .expect("冷折叠");
    let AggStates::Plain(p) = &states else {
        panic!("应当是不分组结果");
    };
    let out = fx.engine.sql("SELECT count(*) AS c, sum(v) AS s FROM cold").await.unwrap();
    let c = out[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    let s = out[0].column(1).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(
        (p[0].clone(), p[1].clone()),
        (PartialAgg::Count(c as u64), PartialAgg::SumInt(Some(s))),
        "冷折叠必须与协调者的查询一致（同一套读路径 ⇒ 这条天然成立）"
    );
}

/// ② **DV 必须生效**（删前/删后各一遍 —— 数字不变就说明 DV 被忽略了）。
#[tokio::test]
async fn cold_fold_honours_the_deletion_vectors() {
    let fx = setup().await;

    // 删前：6 行、和 66
    let (files, dvs) = fx.cold_request();
    let q = AggQuery::plain(vec![
        AggSpec::count_star(),
        AggSpec::new(AggKind::SumInt, "v"),
    ]);
    let before = cold::fold_cold_files(
        fx.store.clone(),
        TABLE,
        schema(),
        files.clone(),
        dvs.clone(),
        &q,
    )
    .await
    .unwrap();
    let AggStates::Plain(b) = &before else {
        panic!()
    };
    assert_eq!(b[0].clone(), PartialAgg::Count(6), "删前：6 行");
    assert_eq!(b[1].clone(), PartialAgg::SumInt(Some(66)));

    // 删掉文件 a 的第 2 行（v = 2）
    fx.delete("dv-1", &fx.a_path.clone(), &[1]).await;

    let (files, dvs) = fx.cold_request();
    let after = cold::fold_cold_files(
        fx.store.clone(),
        TABLE,
        schema(),
        files.clone(),
        dvs.clone(),
        &q,
    )
    .await
    .unwrap();
    let AggStates::Plain(a) = &after else {
        panic!()
    };
    assert_eq!(a[0], PartialAgg::Count(5), "删后：少一行（DV 生效）");
    assert_eq!(a[1], PartialAgg::SumInt(Some(64)), "66 - 2");
    // 与协调者自己的查询对拍（它读同一条路径）
    let out = fx.engine.sql("SELECT count(*) AS c, sum(v) AS s FROM cold").await.unwrap();
    assert_eq!(
        out[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0),
        5
    );
}

/// ②b 分组：与协调者的 `GROUP BY` 对拍（逐键逐值）。
#[tokio::test]
async fn cold_fold_grouped_matches_the_coordinator() {
    let fx = setup().await;
    fx.delete("dv-1", &fx.a_path.clone(), &[1]).await; // 删掉 v = 2
    let (files, dvs) = fx.cold_request();
    let q = AggQuery {
        specs: vec![AggSpec::count_star(), AggSpec::new(AggKind::Min, "v")],
        group_by: vec!["v".into()],
        predicate: None,
    };
    let states = cold::fold_cold_files(fx.store.clone(), TABLE, schema(), files, dvs, &q)
        .await
        .unwrap();
    let AggStates::Grouped(g) = &states else {
        panic!("应当分组");
    };
    let got: Vec<(i64, u64, i64)> = g
        .iter()
        .map(|x| {
            let key = match x.key[0] {
                Some(AggScalar::Int(i)) => i,
                _ => panic!("整数键"),
            };
            let min = match x.states[1] {
                PartialAgg::Min(Some(AggScalar::Int(i))) => i,
                _ => panic!("整数极值"),
            };
            (key, match x.states[0] { PartialAgg::Count(c) => c, _ => panic!() }, min)
        })
        .collect();
    assert_eq!(got, vec![(1, 1, 1), (3, 1, 3), (10, 1, 10), (20, 1, 20), (30, 1, 30)]);

    // 对拍：协调者自己的分组查询（DV 也生效）
    assert_eq!(
        grouped_pairs(&fx.engine, "SELECT v, count(*) FROM cold GROUP BY v").await,
        vec![(1, 1), (3, 1), (10, 1), (20, 1), (30, 1)]
    );
}

/// ③ 谓词与分组一起（`§176` + `§177` 的契约在这里复用）。
#[tokio::test]
async fn cold_fold_applies_predicate_and_grouping_together() {
    let fx = setup().await;
    let (files, dvs) = fx.cold_request();
    let q = AggQuery {
        specs: vec![AggSpec::count_star()],
        group_by: vec!["v".into()],
        predicate: Some(yuntun_model::predicate::Predicate::Cmp {
            column: "v".into(),
            op: yuntun_model::predicate::CmpOp::Gt,
            value: yuntun_model::predicate::Literal::Int(5),
        }),
    };
    let states = cold::fold_cold_files(fx.store.clone(), TABLE, schema(), files, dvs, &q)
        .await
        .unwrap();
    let AggStates::Grouped(g) = &states else {
        panic!("应当分组");
    };
    let keys: Vec<i64> = g
        .iter()
        .map(|x| match x.key[0] {
            Some(AggScalar::Int(i)) => i,
            _ => panic!("整数键"),
        })
        .collect();
    assert_eq!(keys, vec![10, 20, 30], "只有 v > 5 的三组");
    for grp in g {
        assert_eq!(grp.states[0], PartialAgg::Count(1));
    }
    assert_eq!(
        grouped_pairs(&fx.engine, "SELECT v, count(*) FROM cold WHERE v > 5 GROUP BY v").await,
        vec![(10, 1), (20, 1), (30, 1)],
        "与协调者自己的查询一致"
    );
}

/// **分配 + 就地折叠合起来 == 全量查**（`§178` 的判据在这里再走一遍：
/// 冷折叠这一侧也不许漏文件/重文件）。
#[tokio::test]
async fn assigned_cold_folds_add_up_to_the_whole() {
    let fx = setup().await;
    let files = fx.files_of();
    let (all, dvs) = fx.cold_request();
    let q = AggQuery::plain(vec![
        AggSpec::count_star(),
        AggSpec::new(AggKind::SumInt, "v"),
    ]);
    let whole = cold::fold_cold_files(fx.store.clone(), TABLE, schema(), all, dvs, &q)
        .await
        .unwrap();
    let AggStates::Plain(whole) = whole else {
        panic!()
    };

    // 分成两份分别折叠，再合并
    let got = placement::assign(&files, &[String::new(), "dn1".into()]);
    let mut merged = whole_empty(&q);
    for a in &got {
        let (files, dvs) = fx.cold_request();
        let mine: Vec<FileManifest> = files
            .into_iter()
            .filter(|f| a.files.iter().any(|x| x.file_path == f.file_path))
            .collect();
        let part = cold::fold_cold_files(fx.store.clone(), TABLE, schema(), mine, dvs, &q)
            .await
            .unwrap();
        merged = yuntun_model::agg::merge_states(&merged, &part).unwrap();
    }
    assert_eq!(merged, AggStates::Plain(whole), "各份折叠后合并 == 整份折叠");
}

fn whole_empty(q: &AggQuery) -> AggStates {
    AggStates::Plain(q.specs.iter().map(|s| PartialAgg::empty(s.kind)).collect())
}

/// 读不到 DV ⇒ **响亮失败**（绝不降级：漏一份 DV = 复活已删的行）。
#[tokio::test]
async fn an_unreadable_dv_fails_loudly() {
    let fx = setup().await;
    fx.delete("dv-1", &fx.a_path.clone(), &[1]).await;
    // 把位图对象删掉 ⇒ 读侧必然取不到
    yuntun_store::delete(
        fx.store.as_ref(),
        &yuntun_model::dv::dv_object_path(&fx.a_path, "dv-1"),
    )
    .await
    .unwrap();
    let (files, dvs) = fx.cold_request();
    let q = AggQuery::plain(vec![AggSpec::count_star()]);
    let e = cold::fold_cold_files(fx.store.clone(), TABLE, schema(), files, dvs, &q)
        .await
        .expect_err("DV 取不到必须报错");
    assert!(
        matches!(e, LakeError::Other(_) | LakeError::S3(_)),
        "错误应当是 Other/S3 之一（响亮失败即可）：{e:?}"
    );
}
