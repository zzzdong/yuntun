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
    /// `public.cpu` 在"现在"生效的删除向量（`F.3c-3` 起进对拍）：`(dv_id, file_path, card)`
    deletions_now: Vec<(String, String, u32)>,
    /// 旧快照下生效的删除向量（撤销/生效的**窗口两端**都必须在两种形态下一致）
    deletions_old: Vec<(String, String, u32)>,
    /// `§169`：**任务目录**（`(表:分片, 文件数, 删除行数, 原因)`）。
    /// 它是**派生**的 ⇒ 同一串 op 之后两种形态必须算出**同一张表**（否则"分领"不成立：
    /// 各节点会照自己那份待办干活）。
    tasks: Vec<(String, u64, u64, u32)>,
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
        deletions_now: dvs(c, u64::MAX).await,
        deletions_old: dvs(c, old_snapshot).await,
        tasks: {
            let mut t: Vec<(String, u64, u64, u32)> = c
                .list_tasks()
                .await
                .expect("list_tasks")
                .iter()
                .map(|t| (t.key().id(), t.files, t.deleted_rows, t.reason))
                .collect();
            t.sort();
            t
        },
    }
}

/// 某快照下生效的删除向量（排序后比较 —— 两边必须逐条一致）。
async fn dvs(c: &Arc<dyn CatalogOps>, snapshot: u64) -> Vec<(String, String, u32)> {
    let mut v: Vec<(String, String, u32)> = c
        .list_deletions("public.cpu", snapshot)
        .await
        .expect("list_deletions")
        .into_iter()
        .map(|d| (d.dv_id, d.file_path, d.card))
        .collect();
    v.sort();
    v
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

    // ---- ⑥b UPDATE（`F.7` 决策 5：**一个 op 承载、原子可见**）----
    //
    // 断言两件事：① 两种形态一致（`Observed` 现在把 DV 也拉进了对拍）；
    // ② **原子性的判据** —— 新行文件与删除向量落在**同一个快照号**上：读侧按快照号切分，
    //    同号就意味着"要么都看得见、要么都看不见"，不存在"删了没插"或"插了没删"的中间态。
    let dv = yuntun_model::dv::DeletionEntry {
        dv_id: "dv-upd".into(),
        table: "public.cpu".into(),
        file_path: "p/b2.parquet".into(),
        batch_id: "b2".into(),
        applied_at: 0, // 由目录分配（两半共用一个）
        revoked_at: 0,
        card: 3,
        store_path: String::new(), // 由目录按 §3.1 公式补
    };
    let mut new_file = commit("public.cpu", "b-upd", "", "s1").files[0].clone();
    new_file.batch_id = "b-upd".into();
    let rsnap = remote
        .apply_update(vec![dv.clone()], vec![new_file.clone()], "upd-1")
        .await
        .expect("远端 UPDATE");
    let msnap = memory
        .apply_update(vec![dv.clone()], vec![new_file.clone()], "upd-1")
        .await
        .expect("本地 UPDATE");
    assert_eq!(rsnap, msnap, "两种形态分配的快照号必须一致");
    agree!("apply_update", remote, memory, old_snapshot);

    for (name, c, snap) in [("远端", &remote, rsnap), ("本地", &memory, msnap)] {
        let files = c.list_visible_files("public.cpu", snap, None).await.unwrap();
        let nf = files
            .iter()
            .find(|f| f.batch_id == "b-upd")
            .unwrap_or_else(|| panic!("{name}：新文件必须在 UPDATE 那一个快照上可见"));
        assert_eq!(nf.valid_from, snap, "{name}：新文件与删除向量必须同号");
        let live = c.list_deletions("public.cpu", snap).await.unwrap();
        assert_eq!(live.len(), 1, "{name}：删除向量也在同一快照生效");
        assert_eq!(live[0].applied_at, snap, "{name}：两者**同号** ⇒ 原子可见");
        assert!(
            !live[0].store_path.is_empty(),
            "{name}：`store_path` 必须按 §3.1 的公式补上（重放/读侧靠它取位图）"
        );
        // 旧快照：两半都不可见（"要么都看见、要么都看不见"）
        assert!(
            !c.list_visible_files("public.cpu", snap - 1, None)
                .await
                .unwrap()
                .iter()
                .any(|f| f.batch_id == "b-upd"),
            "{name}：旧快照不许看到新行"
        );
        assert!(
            c.list_deletions("public.cpu", snap - 1).await.unwrap().is_empty(),
            "{name}：旧快照不许看到删除（否则就是「删了没插」）"
        );
        // 重放同一份 op（幂等）：空操作，不推版本
        let again = c
            .apply_update(vec![dv.clone()], vec![new_file.clone()], "upd-1")
            .await
            .unwrap();
        assert_eq!(again, c.current_snapshot().await, "{name}：重放不推版本");
    }

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

/// **`list_tables()` 出来的标识必须能直接喂给 `list_visible_files`**（`§167`）。
///
/// 抓的是一个真 bug：`TableMeta.name` 是**裸名**（`cpu`），而全仓的标识是**全限定名**
/// （`public.cpu`：清单键 / 对象路径 / WAL 记录）。`compaction` 与 `wal_archive::durable_tables`
/// 曾拿裸名去查 ⇒ 在 `MemoryCatalog` 里"碰巧能用"（它内部归一化），**远端目录一律查不到**
/// ⇒ 真多节点部署下**合并永远不触发**（`candidates=0` 而磁盘上明明有文件）。
#[tokio::test]
async fn list_tables_identity_is_directly_usable_for_file_queries() {
    let cluster = Cluster::start();
    let leader = cluster.wait_leader(T).expect("三节点应选出 leader");
    let node = cluster.handle(leader).expect("取 leader 句柄");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { yuntun_meta::serve(node, listener).await });

    let remote: Arc<dyn CatalogOps> =
        Arc::new(RemoteCatalog::connect(vec![addr.to_string()]).expect("连接 metanode"));
    remote.create_table(CreateTableRequest {
        name: "cpu".into(),
        namespace: "public".into(),
        schema: schema(&["ts", "v"]),
        partition_cols: vec![],
        default_format: "parquet".into(),
        ingest_config: yuntun_model::meta::IngestConfig::standard(),
    })
    .await
    .expect("建表");
    remote
        .commit_files(commit("public.cpu", "b1", "k-b1", "s0"))
        .await
        .expect("提交文件");

    let metas = remote.list_tables().await.expect("list_tables");
    let cpu = metas.iter().find(|t| t.name == "cpu").expect("应当有 cpu");
    // ① 裸名 ≠ 全限定名（这条定义性断言让"名字是什么"一目了然）
    assert_eq!(cpu.name, "cpu");
    assert_eq!(cpu.qualified_name(), "public.cpu");

    // ② 用 `qualified_name()` 查询 ⇒ 必须查得到（后台作业就是这么用的）
    let snap = remote.current_snapshot().await;
    let files = remote
        .list_visible_files(&cpu.qualified_name(), snap, None)
        .await
        .expect("全限定名必须能列出文件");
    assert_eq!(files.len(), 1, "应当看到刚提交的那个文件：{files:?}");

    // ③ 裸名也查得到（目录两侧都会归一化）—— 所以这坑**不会**表现为报错，而是"碰巧能用"；
    //    真正的纪律是：**作业一律用 `qualified_name()`**（`FileManifest.table` 也是全限定名，
    //    见下面这条），别依赖"某个实现会帮你归一化"。
    let by_bare = remote
        .list_visible_files(&cpu.name, snap, None)
        .await
        .expect("裸名查询不该报错");
    assert_eq!(by_bare.len(), 1, "目录会归一化 ⇒ 裸名也查得到：{by_bare:?}");
    assert_eq!(
        by_bare[0].table, "public.cpu",
        "文件上记的表名必须是全限定名（跨层唯一标识）：{:?}",
        by_bare[0].table
    );

    server.abort();
}

/// `§169`：**任务目录必须在 raft 路径上也被派生出来**（不只是进程内直连形态）。
///
/// 这条把"任务表是状态机派生的事实"钉死：同一串 op 走一遍 raft ⇒ 远端与本地算出
/// **同一条待办**（否则多节点各照自己那份干活，"分领"就不成立）。
#[tokio::test]
async fn task_directory_is_derived_on_both_catalog_forms() {
    let cluster = Cluster::start();
    let leader = cluster.wait_leader(T).expect("三节点应选出 leader");
    let node = cluster.handle(leader).expect("取 leader 句柄");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { yuntun_meta::serve(node, listener).await });

    let remote: Arc<dyn CatalogOps> =
        Arc::new(RemoteCatalog::connect(vec![addr.to_string()]).expect("连接 metanode"));
    let memory: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    for c in [&remote, &memory] {
        c.create_schema("public").await.ok();
        c.create_table(CreateTableRequest {
            name: "cpu".into(),
            namespace: "public".into(),
            schema: schema(&["ts", "v"]),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig::standard(),
        })
        .await
        .expect("建表");
        // 同分片两个文件 ⇒ 达到"能合并"的必要条件（`threshold::FILES = 2`）
        for b in ["b1", "b2"] {
            c.commit_files(commit("public.cpu", b, &format!("k-{b}"), "s0"))
                .await
                .expect("提交文件");
        }
    }

    let rt = remote.list_tasks().await.expect("远端任务目录");
    let mt = memory.list_tasks().await.expect("本地任务目录");
    let fmt = |v: &[yuntun_model::task::CompactionTask]| -> Vec<String> {
        v.iter()
            .map(|t| format!("{}:{} files={} reason={}", t.table, t.shard, t.files, t.reason))
            .collect()
    };
    assert_eq!(
        fmt(&rt),
        vec!["public.cpu:s0 files=2 reason=0".to_string()],
        "远端（走 raft）应当派生出这条待办：{:?}",
        fmt(&rt)
    );
    assert_eq!(fmt(&rt), fmt(&mt), "两种形态的任务目录必须一致");

    server.abort();
}

/// `§172`：**远端形态的任务目录必须跟着版本走** —— 便宜的路径（`refresh()` 版本探测）
/// 也要能看到"待办消失"。
///
/// 这条钉的是 `§169` 当初那条判断（"必须走全量刷新，否则待办消失不了"）**不成立**：
/// 任务表是"文件 + DV"的纯函数，而下线这批文件（`purge_table_files`）会推 `manifest_ver`
/// ⇒ 探测就能看出变化、载荷随之把 `tasks` 整体替换掉。
///
/// 同时它也是台账 `D-25` 的远端证据：**整表清除之后待办必须消失**（否则每个节点每轮都会
/// 去抢一个空分片的租约）。
#[tokio::test]
async fn remote_task_directory_follows_purge() {
    let cluster = Cluster::start();
    let leader = cluster.wait_leader(T).expect("三节点应选出 leader");
    let node = cluster.handle(leader).expect("取 leader 句柄");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { yuntun_meta::serve(node, listener).await });

    let remote: Arc<dyn CatalogOps> =
        Arc::new(RemoteCatalog::connect(vec![addr.to_string()]).expect("连接 metanode"));
    remote.create_schema("public").await.ok();
    remote
        .create_table(CreateTableRequest {
            name: "cpu".into(),
            namespace: "public".into(),
            schema: schema(&["ts", "v"]),
            partition_cols: vec![],
            default_format: "parquet".into(),
            ingest_config: yuntun_model::meta::IngestConfig::standard(),
        })
        .await
        .expect("建表");
    for b in ["b1", "b2"] {
        remote
            .commit_files(commit("public.cpu", b, &format!("k-{b}"), "s0"))
            .await
            .expect("提交文件");
    }
    let before = remote.list_tasks().await.expect("远端任务目录");
    assert_eq!(
        before.len(),
        1,
        "两个文件 ⇒ 应当有一条待办：{before:?}"
    );

    // 整表清除：此刻可见文件全部下线（**走 raft**：`PurgeFiles` op）
    let out = remote
        .purge_table_files("public.cpu", "purge-1")
        .await
        .expect("整表清除");
    assert_eq!(out.files, 2, "两个可见文件都该下线");

    let after = remote.list_tasks().await.expect("远端任务目录（清除后）");
    assert!(
        after.is_empty(),
        "分片已经没有可见文件 ⇒ 待办必须消失（`D-25`）：{after:?}"
    );

    server.abort();
}
