//! `RemoteCatalog` ↔ `MemoryCatalog` **对拍**（S3-4 的核心验收）。
//!
//! 同一个 trait 的两个实现，对**同一串操作**必须给出同样的可观测结果 ——
//! 这就是「切过去不回归」的判据。对拍比"接口能编译"强得多：它抓的是**语义漂移**
//! （表名归一化、错误映射、MVCC 可见性、排序、两组版本号、快照号）。
//!
//! 比较范围刻意**排除**时间戳类字段（`created_at`/`committed_at`）：本地实现读自己的钟，
//! 远端用的是 op 里带的时间 —— 它们**本就该不同**（纪律 1：时间随 op 走），
//! 把它们拉进对拍只会得到一条噪声断言。

use std::sync::Arc;

use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_meta::{Cluster, RemoteCatalog};
use yuntun_model::error::LakeError;
use yuntun_model::meta::FileManifest;
use yuntun_model::ops::{CommitFilesRequest, CreateTableRequest, EvolveSchemaRequest};

const T: std::time::Duration = std::time::Duration::from_secs(60);

/// 可观测状态（**只放两边都该一致的东西**）。
#[derive(Debug, PartialEq)]
struct Observed {
    schemas: Vec<String>,
    /// (全限定名, 默认格式, schema 版本, 字段数)
    tables: Vec<(String, String, u64, usize)>,
    version: (u64, u64),
    snapshot: u64,
    /// `public.cpu` 在"现在"可见的 batch_id（升序）
    visible_now: Vec<String>,
    /// `public.cpu` 在**旧快照**下可见的 batch_id（这条盯的是 MVCC：墓碑必须还在）
    visible_old: Vec<String>,
}

async fn observed(c: &Arc<dyn CatalogOps>, old_snapshot: u64) -> Observed {
    let mut tables: Vec<(String, String, u64, usize)> = c
        .list_tables()
        .await
        .expect("list_tables")
        .iter()
        .map(|t| {
            (
                t.qualified_name(),
                t.default_format.clone(),
                t.current_schema_version,
                t.schema().map(|s| s.fields().len()).unwrap_or(0),
            )
        })
        .collect();
    tables.sort();
    let v = c.version().await;
    let ids = |mut fs: Vec<FileManifest>| {
        let mut v: Vec<String> = fs.drain(..).map(|f| f.batch_id).collect();
        v.sort();
        v
    };
    Observed {
        schemas: c.list_schemas().await.expect("list_schemas"),
        tables,
        version: (v.schema_ver, v.manifest_ver),
        snapshot: c.current_snapshot().await,
        visible_now: ids(c.list_visible_files("public.cpu", u64::MAX, None).await.unwrap()),
        visible_old: ids(c.list_visible_files("public.cpu", old_snapshot, None).await.unwrap()),
    }
}

macro_rules! agree {
    ($stage:expr, $remote:expr, $memory:expr, $old:expr) => {{
        let r = observed(&$remote, $old).await;
        let m = observed(&$memory, $old).await;
        assert_eq!(
            r, m,
            "步「{}」之后两个实现的可观测状态不一致（远端 vs 本地）",
            $stage
        );
    }};
}

fn schema(fields: &[&str]) -> arrow::datatypes::SchemaRef {
    Arc::new(arrow::datatypes::Schema::new(
        fields
            .iter()
            .map(|f| {
                arrow::datatypes::Field::new(*f, arrow::datatypes::DataType::Int64, true)
            })
            .collect::<Vec<_>>(),
    ))
}

fn commit(table: &str, batch: &str, key: &str, shard: &str) -> CommitFilesRequest {
    CommitFilesRequest {
        table: table.into(),
        batch_id: batch.into(),
        client_request_id: Some(key.into()),
        client_request_ids: vec![key.into()],
        shard: shard.into(),
        time_window: "w1".into(),
        files: vec![FileManifest {
            file_path: format!("p/{batch}.parquet"),
            batch_id: batch.into(),
            table: table.into(),
            shard: shard.into(),
            row_count: 7,
            ..Default::default()
        }],
        schema_version: 1,
        row_count: 7,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_and_memory_catalogs_agree_on_the_same_ops() {
    // ---- 起一个真 metanode（进程内 3 节点簇），并暴露 gRPC ----
    let cluster = Cluster::start();
    let leader = cluster.wait_leader(T).expect("三节点应选出 leader");
    let node = cluster.handle(leader).expect("取 leader 句柄");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { yuntun_meta::serve(node, listener).await });

    let remote: Arc<dyn CatalogOps> =
        Arc::new(RemoteCatalog::connect(vec![addr.to_string()]).expect("连接 metanode"));
    let memory: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());

    // ---- ① 建 schema ----
    remote.create_schema("analytics").await.expect("远端建 schema");
    memory.create_schema("analytics").await.expect("本地建 schema");
    agree!("create_schema", remote, memory, 0);

    // ---- ② 建两张表（跨 schema：这条抓的是"全限定名"处理）----
    for (ns, name, fields) in [
        ("public", "cpu", vec!["ts", "v"]),
        ("analytics", "metrics", vec!["ts"]),
    ] {
        let req = CreateTableRequest {
            name: name.into(),
            namespace: ns.into(),
            schema: schema(&fields),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig::standard(),
        };
        let rt = remote.create_table(req.clone()).await.expect("远端建表");
        let mt = memory.create_table(req).await.expect("本地建表");
        assert_eq!(rt.qualified_name(), mt.qualified_name(), "建表返回的表身份不一致");
    }
    agree!("create_table x2", remote, memory, 0);

    // ---- ③ 提交两个文件（不同 shard）----
    remote
        .commit_files(commit("public.cpu", "b1", "k-b1", "s0"))
        .await
        .expect("远端提交 b1");
    memory
        .commit_files(commit("public.cpu", "b1", "k-b1", "s0"))
        .await
        .expect("本地提交 b1");
    remote
        .commit_files(commit("public.cpu", "b2", "k-b2", "s1"))
        .await
        .expect("远端提交 b2");
    memory
        .commit_files(commit("public.cpu", "b2", "k-b2", "s1"))
        .await
        .expect("本地提交 b2");
    agree!("commit_files x2", remote, memory, 0);

    // 记下"旧快照"：下面删分片之后要用它验证 MVCC（墓碑必须仍能被旧快照看见）
    let old_snapshot = remote.current_snapshot().await;
    assert_eq!(
        old_snapshot,
        memory.current_snapshot().await,
        "两组快照号必须同步推进"
    );

    // ---- ④ Schema 演进（OCC）----
    let evolve = EvolveSchemaRequest {
        table: "public.cpu".into(),
        change: yuntun_model::schema::SchemaChange::AddColumn {
            field: arrow::datatypes::Field::new("n", arrow::datatypes::DataType::Int64, true),
        },
        expected_version: 1,
    };
    let rr = remote.evolve_schema(evolve.clone()).await.expect("远端演进");
    let mr = memory.evolve_schema(evolve).await.expect("本地演进");
    assert_eq!(rr.version, mr.version, "演进后的版本号不一致");
    assert_eq!(
        rr.new_schema.fields().len(),
        mr.new_schema.fields().len(),
        "演进后的字段数不一致"
    );
    agree!("evolve_schema", remote, memory, old_snapshot);

    // ---- ⑤ OCC 冲突：拿过期的 expected_version 再演进一次 ----
    let stale = EvolveSchemaRequest {
        table: "public.cpu".into(),
        change: yuntun_model::schema::SchemaChange::DropColumn {
            column: "n".into(),
        },
        expected_version: 1, // 已经是 2 了
    };
    let re = remote.evolve_schema(stale.clone()).await.expect_err("远端应报冲突");
    let me = memory.evolve_schema(stale).await.expect_err("本地应报冲突");
    assert!(
        matches!(re, LakeError::SchemaChanged { .. }),
        "远端冲突必须还原成 SchemaChanged（而不是退化成 Other）：{re:?}"
    );
    match (&re, &me) {
        (
            LakeError::SchemaChanged { actual_version: a, .. },
            LakeError::SchemaChanged { actual_version: b, .. },
        ) => assert_eq!(a, b, "冲突里的实际版本必须一致"),
        _ => panic!("两个实现的冲突错误形状不一致：{re:?} vs {me:?}"),
    }

    // ---- ⑥ 删分片 → b1 变墓碑 ----
    let rd = remote.drop_shard("public.cpu", "s0").await.expect("远端删分片");
    let md = memory.drop_shard("public.cpu", "s0").await.expect("本地删分片");
    assert_eq!(
        rd, md,
        "删分片的**精确条数**也必须一致（`affected` 随提交过线；远端的 1/0 那套已废弃）"
    );
    assert!(md > 0, "本次删分片确实应标记到文件：{md}");
    // 这一条是**对拍的关键**：现在看不到 b1，但**旧快照下仍要看到它**
    agree!("drop_shard", remote, memory, old_snapshot);
    let now = observed(&remote, old_snapshot).await;
    assert!(
        !now.visible_now.contains(&"b1".to_string()),
        "删分片后 b1 不该再可见：{now:?}"
    );
    assert!(
        now.visible_old.contains(&"b1".to_string()),
        "**旧快照**下必须仍能看到 b1（墓碑没被丢掉）：{now:?}"
    );

    // ---- ⑦ 删表 ----
    remote.drop_table("analytics.metrics").await.expect("远端删表");
    memory.drop_table("analytics.metrics").await.expect("本地删表");
    agree!("drop_table", remote, memory, old_snapshot);

    // ---- ⑧ 错误保真：重复建表 / 删不存在的表 / 删 public ----
    let re = remote
        .create_table(CreateTableRequest {
            name: "cpu".into(),
            namespace: "public".into(),
            schema: schema(&["ts", "v"]),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig::standard(),
        })
        .await
        .expect_err("远端重复建表应报错");
    assert!(
        matches!(re, LakeError::TableAlreadyExists(_)),
        "远端必须还原本地表已存在（否则 SQL 层的错误码会整体退化）：{re:?}"
    );
    let re = remote
        .drop_table("public.nope")
        .await
        .expect_err("远端删不存在的表应报错");
    assert!(
        matches!(re, LakeError::TableNotFound(_)),
        "远端必须还原 TableNotFound：{re:?}"
    );
    let re = remote.drop_schema("public").await.expect_err("public 不可删");
    let me = memory.drop_schema("public").await.expect_err("public 不可删");
    assert_eq!(
        std::mem::discriminant(&re),
        std::mem::discriminant(&me),
        "两边对「删 public」的错误类别必须一致（文案可以不同）：{re:?} vs {me:?}"
    );

    // ---- ⑨ 幂等：快路径语义 + **真正要保证的性质**（SM 仍然是权威）----
    let rk = remote.check_idempotency("k-b1").await.expect("远端查幂等");
    let mk = memory.check_idempotency("k-b1").await.expect("本地查幂等");
    assert_eq!(
        rk.is_some(),
        mk.is_some(),
        "自己提交过的键必须在本地快路径里命中（远端返回空 batch_id 是刻意的，见模块文档）"
    );
    assert!(
        remote
            .check_idempotency("never-seen")
            .await
            .unwrap()
            .is_none()
    );

    // ---- ⑩ 幂等键集合**过线**：新起的客户端没写过任何键，只能从全量载荷里学 ----
    //
    // 这条盯的是"新进程的预筛命中率"：学不到键，每个别人的重复请求都会白写一次 WAL
    // 再去 SM 去重（正确性不受影响，但那是纯浪费）。
    let fresh: Arc<dyn CatalogOps> =
        Arc::new(RemoteCatalog::connect(vec![addr.to_string()]).expect("新实例"));
    // 先触发一次刷新（真实路径里由首个读/周期刷新触发）—— 快路径**本身不打网络**，
    // 所以"刚构造完就问"必然空集：这是契约，不是 bug（见 `check_idempotency` 的文档）。
    let _ = fresh.version().await;
    assert!(
        fresh.check_idempotency("k-b1").await.unwrap().is_some(),
        "刷新之后，新实例必须能从**全量刷新载荷**里学到已有的幂等键"
    );

    // 快路径**漏判**也必须安全：远端直接重放同一个提交 → 由 SM 去重（`accepted=false`）。
    // 这条才是"快路径只是加速"的可验证形式 —— 快路径本身答错不影响正确性。
    let dup = remote
        .commit_files(commit("public.cpu", "b1", "k-b1", "s0"))
        .await
        .expect("重放提交必须**成功**（幂等不是错误）");
    assert!(
        !dup.accepted,
        "同键同批次的重复提交必须是幂等命中（accepted=false），而不是重复落盘"
    );

    // ---- ⑪ 保留窗口：过旧的快照必须**拒绝**（而不是少读几个文件）----
    let tight: Arc<dyn CatalogOps> = Arc::new(
        RemoteCatalog::connect(vec![addr.to_string()])
            .expect("连接")
            .with_file_retention(1),
    );
    // 先正常写一笔（把快照推上去），再让它刷新一次以推进下界
    tight
        .commit_files(commit("public.cpu", "b3", "k-b3", "s2"))
        .await
        .expect("提交 b3");
    let _ = tight.list_visible_files("public.cpu", u64::MAX, None).await;
    let e = tight
        .list_visible_files("public.cpu", 0, None)
        .await
        .expect_err("早于保留下界的快照必须被拒绝");
    assert!(
        format!("{e}").contains("早于"),
        "拒绝原因应当说清是「快照过旧」（而不是含糊的 internal）：{e}"
    );
    // 而"现在"仍然可查
    tight
        .list_visible_files("public.cpu", u64::MAX, None)
        .await
        .expect("当前快照必须可查");

    server.abort();
}
