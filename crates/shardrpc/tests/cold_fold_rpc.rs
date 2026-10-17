//! `§179`：**冷折叠的过网往返** —— 服务端真的读文件、真的折，网络里只走标量。
//!
//! 三条判据：
//! ① 往返后的部分状态 == **进程内同一条路径**折出来的（逐位）；
//! ② **DV 生效**（删掉的行不许被算进来 —— 漏一份 DV = 复活已删的行）；
//! ③ 没接冷折叠的节点 ⇒ **响亮失败**（不是"返回一个空结果"）。

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_model::agg::{AggKind, AggQuery, AggScalar, AggSpec, AggStates, PartialAgg};
use yuntun_model::dv::{DeletionEntry, DvBitmap};
use yuntun_model::meta::FileManifest;
use yuntun_model::ops::{CommitFilesRequest, CreateTableRequest};
use yuntun_query::{LocalCatalog, QueryEngine, cold};
use yuntun_shardrpc::RemoteCold;
use yuntun_store::{ColdReadRequest, ColdReader, StoreConfig, create_store};

const TABLE: &str = "public.coldrpc";
const SHARD: &str = "s0";
const WINDOW: &str = "w";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]))
}

fn batch(vals: &[Option<i64>]) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
}

/// 服务端那一侧的冷折叠实现（与 datanode 的 `NodeCold` 同形：只需要 store）。
#[derive(Debug)]
struct TestCold {
    store: Arc<dyn object_store::ObjectStore>,
}

impl ColdReader for TestCold {
    fn fold(
        &self,
        req: ColdReadRequest,
    ) -> futures::future::BoxFuture<'_, Result<AggStates, yuntun_model::error::LakeError>> {
        Box::pin(async move {
            cold::fold_cold_files(
                self.store.clone(),
                &req.table,
                req.schema,
                req.files,
                req.deletions,
                &req.query,
            )
            .await
        })
    }
}

struct Fx {
    store: Arc<dyn object_store::ObjectStore>,
    a_path: String,
    files: Vec<FileManifest>,
}

async fn setup() -> Fx {
    let store = create_store(&StoreConfig::Memory).unwrap();
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    catalog
        .create_table(CreateTableRequest {
            name: "coldrpc".into(),
            namespace: yuntun_model::ops::DEFAULT_SCHEMA.into(),
            schema: schema(),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig::standard(),
        })
        .await
        .unwrap();
    let (a_path, a_size, _) = yuntun_format::write_batch(
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
    let (b_path, b_size, _) = yuntun_format::write_batch(
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
    let mut files = Vec::new();
    for (id, path, size) in [("b-a", &a_path, a_size), ("b-b", &b_path, b_size)] {
        catalog
            .commit_files(CommitFilesRequest {
                table: TABLE.into(),
                batch_id: id.into(),
                client_request_id: None,
                client_request_ids: vec![],
                shard: SHARD.into(),
                time_window: WINDOW.into(),
                files: vec![FileManifest {
                    file_path: path.clone(),
                    batch_id: id.into(),
                    file_size: size,
                    row_count: 3,
                    table: TABLE.into(),
                    shard: SHARD.into(),
                    time_window: WINDOW.into(),
                    ..Default::default()
                }],
                schema_version: 1,
                row_count: 3,
            })
            .await
            .unwrap();
        files.push(FileManifest {
            file_path: path.clone(),
            batch_id: id.into(),
            file_size: size,
            row_count: 3,
            ..Default::default()
        });
    }
    let cache = Arc::new(LocalCatalog::new());
    cache.set_catalog_ops(catalog.clone());
    cache.refresh(&catalog).await.unwrap();
    // 协调者视角的引擎（用来对拍）
    let _engine = QueryEngine::new(store.clone(), cache);
    Fx {
        store,
        a_path,
        files,
    }
}

/// 起一个"接了冷折叠"的服务端，返回已连接的客户端。
async fn serve_with_cold(store: Arc<dyn object_store::ObjectStore>) -> RemoteCold {
    let chunk = yuntun_chunk::ChunkStore::new(
        yuntun_chunk::ChunkStoreConfig {
            policy: yuntun_chunk::SealPolicy {
                rows_threshold: usize::MAX,
                bytes_threshold: usize::MAX,
                min_resident: std::time::Duration::from_secs(3600),
                max_flush_delay: std::time::Duration::from_secs(3600),
                max_resident: std::time::Duration::from_secs(3600),
                phase_spread: std::time::Duration::ZERO,
            },
            spill_dir: yuntun_testkit::TestDir::tmpfs("coldrpc-spill").into_path(),
            instance_id: "inst-cold".into(),
            wal_segment: 0,
        },
        yuntun_chunk::MemoryLedger::new("chunk", 1 << 24),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cold = Arc::new(TestCold {
        store: store.clone(),
    });
    tokio::spawn(async move {
        let _ = yuntun_shardrpc::serve(chunk, listener, Some(cold)).await;
    });
    RemoteCold::connect(&addr.to_string()).await.unwrap()
}

/// ① + ②：往返 == 进程内同一条路径；DV 生效。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_fold_survives_the_wire_and_honours_dvs() {
    let fx = setup().await;
    let client = serve_with_cold(fx.store.clone()).await;

    let q = AggQuery::plain(vec![
        AggSpec::count_star(),
        AggSpec::new(AggKind::SumInt, "v"),
    ]);

    // 先不给 DV：6 行 / 66
    let req = ColdReadRequest {
        table: TABLE.to_string(),
        schema: schema(),
        files: fx.files.clone(),
        deletions: Vec::new(),
        query: q.clone(),
    };
    let over_wire = client.fold(req.clone()).await.expect("过网");
    let local = cold::fold_cold_files(
        fx.store.clone(),
        TABLE,
        schema(),
        fx.files.clone(),
        Vec::new(),
        &q,
    )
    .await
    .expect("进程内");
    assert_eq!(over_wire, local, "过网与进程内必须逐位相同");
    let AggStates::Plain(p) = over_wire else {
        panic!()
    };
    assert_eq!(p[0], PartialAgg::Count(6));
    assert_eq!(p[1], PartialAgg::SumInt(Some(66)));

    // 再给一份 DV：删掉 a 的第 2 行（v = 2）⇒ 5 行 / 64
    let dv = DvBitmap::from_positions([1u32].into_iter());
    let store_path = yuntun_model::dv::dv_object_path(&fx.a_path, "dv-1");
    yuntun_store::put_bytes(fx.store.as_ref(), &store_path, dv.to_bytes())
        .await
        .unwrap();
    let entry = DeletionEntry {
        dv_id: "dv-1".into(),
        table: TABLE.into(),
        file_path: fx.a_path.clone(),
        batch_id: "b-a".into(),
        applied_at: 0,
        revoked_at: 0,
        card: dv.card() as u32,
        store_path,
    };
    let req = ColdReadRequest {
        deletions: vec![entry],
        ..req
    };
    let over_wire = client.fold(req.clone()).await.expect("带 DV 过网");
    let local = cold::fold_cold_files(
        fx.store.clone(),
        TABLE,
        schema(),
        fx.files.clone(),
        vec![req.deletions[0].clone()],
        &q,
    )
    .await
    .expect("进程内（带 DV）");
    assert_eq!(over_wire, local, "带 DV 也必须逐位相同");
    let AggStates::Plain(p) = over_wire else {
        panic!()
    };
    assert_eq!(p[0], PartialAgg::Count(5), "DV 生效：少一行");
    assert_eq!(p[1], PartialAgg::SumInt(Some(64)), "66 - 2");
}

/// ③ 分组也要过网（每组的标量，不是行）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grouped_cold_fold_survives_the_wire() {
    let fx = setup().await;
    let client = serve_with_cold(fx.store.clone()).await;
    let q = AggQuery {
        specs: vec![AggSpec::count_star()],
        group_by: vec!["v".into()],
        predicate: Some(yuntun_model::predicate::Predicate::Cmp {
            column: "v".into(),
            op: yuntun_model::predicate::CmpOp::Gt,
            value: yuntun_model::predicate::Literal::Int(5),
        }),
    };
    let got = client
        .fold(ColdReadRequest {
            table: TABLE.to_string(),
            schema: schema(),
            files: fx.files.clone(),
            deletions: Vec::new(),
            query: q.clone(),
        })
        .await
        .expect("分组过网");
    let AggStates::Grouped(g) = got else {
        panic!("应当分组")
    };
    let keys: Vec<i64> = g
        .iter()
        .map(|x| match x.key[0] {
            Some(AggScalar::Int(i)) => i,
            _ => panic!("整数键"),
        })
        .collect();
    assert_eq!(keys, vec![10, 20, 30], "谓词 + 分组都生效");
    for grp in &g {
        assert_eq!(grp.states[0], PartialAgg::Count(1));
    }
}

/// ④ 没接冷折叠的节点 ⇒ **响亮失败**（不是一个"空结果"）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_without_cold_folding_fails_loudly() {
    let fx = setup().await;
    let chunk = yuntun_chunk::ChunkStore::new(
        yuntun_chunk::ChunkStoreConfig {
            policy: yuntun_chunk::SealPolicy {
                rows_threshold: usize::MAX,
                bytes_threshold: usize::MAX,
                min_resident: std::time::Duration::from_secs(3600),
                max_flush_delay: std::time::Duration::from_secs(3600),
                max_resident: std::time::Duration::from_secs(3600),
                phase_spread: std::time::Duration::ZERO,
            },
            spill_dir: yuntun_testkit::TestDir::tmpfs("coldrpc-spill2").into_path(),
            instance_id: "inst-nocold".into(),
            wal_segment: 0,
        },
        yuntun_chunk::MemoryLedger::new("chunk", 1 << 24),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = yuntun_shardrpc::serve(chunk, listener, None).await; // 不接冷折叠
    });
    let client = RemoteCold::connect(&addr.to_string()).await.unwrap();
    let e = client
        .fold(ColdReadRequest {
            table: TABLE.to_string(),
            schema: schema(),
            files: fx.files.clone(),
            deletions: Vec::new(),
            query: AggQuery::plain(vec![AggSpec::count_star()]),
        })
        .await
        .expect_err("没接 ⇒ 必须报错");
    assert!(
        e.to_string().contains("未装配") || e.to_string().contains("unimplemented"),
        "错误要能看出是「没接」而不是「读失败」：{e}"
    );
}
