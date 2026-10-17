//! `§175`：**快照号与文件清单必须同版本落地**（台账 `D-23` 的收口）。
//!
//! # 现象与机制（`§174` 观测到的 ±3）
//!
//! rig 上 `count(*)` 出现过**两个方向**的瞬时偏差（21 与 27），而状态机与热副本两侧都被排除
//! ⇒ 只剩"客户端视图"这一层。这一层里最要命的一条是：**号可能走在清单前面**。
//!
//! `refresh()` 是**两跳**：先探测（拿版本号/快照号）、再拉载荷（拿文件）。而载荷请求带的
//! `since_snapshot` 取的是**缓存当时的号**。于是只要"探测成功、载荷失败"（网络抖一下就够），
//! 而探测那一步已经把号推进了，下一轮增量就会带着**新号**去问 —— 服务端的判据是
//! `valid_from > since || deleted_at > since`，**恰好等于那个号的墓碑与产物都不会再被下发**
//! ⇒ 那批变更**永久丢失**（直到某次全量刷新）。
//!
//! 症状正是观测到的那个形状：客户端手里留着**已合并的输入**（`deleted_at` 停在 0）
//! 却永远等不到合并产物 ⇒ 少一行/多一行、但数字看起来很正常。
//!
//! # 这条用例怎么钉住它
//!
//! 用**假 metaserver**（真实 metanode 不会在两跳之间给测试留缝）：它的 `prefetch` 忠实复刻
//! 服务端的两条语义（探测只回号；载荷按上面那条判据筛文件），并且**可以被打断**。
//! 于是：
//!
//! 1. 第一次全量刷新 ⇒ 缓存里有 `A`，号 = 1；
//! 2. 服务端"提交一次合并"（`A` 打墓碑、`M` 入库、号 → 2），并让**载荷那一跳失败**；
//! 3. 断言 **号仍然是 1**（载荷没落地 ⇒ 号不许前进 —— 这正是修掉的那处）；
//! 4. 放开载荷再刷一次 ⇒ 断言 **号 = 2 且 `M` 可见、`A` 不可见**。
//!
//! 第 3 步未修前会是 2，于是第 4 步的增量带 `since=2`、什么都拿不到 ⇒
//! 第 4 步的断言当场失败（`A` 仍然可见 = **少 3 行**的那个形状）。
//! ⇒ 这条用例既能证明修复有效，也把"为什么必须这么修"写成了可执行的形式。

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tonic::{Request, Response, Status};
use yuntun_catalog::CatalogOps;
use yuntun_meta::RemoteCatalog;
use yuntun_model::meta::FileManifest;
use yuntun_proto::meta as pb;

const TABLE: &str = "public.landing";

/// 一个"已提交的文件"：`(batch_id, valid_from, deleted_at)`，行数固定为 1（本用例只关心可见性）。
fn file(batch_id: &str, valid_from: u64, deleted_at: u64) -> FileManifest {
    FileManifest {
        file_path: format!("p/{batch_id}.parquet"),
        batch_id: batch_id.into(),
        table: TABLE.into(),
        shard: "s0".into(),
        time_window: "2026-10-11T10:00".into(),
        row_count: 1,
        valid_from,
        deleted_at,
        ..Default::default()
    }
}

/// 假 metaserver（见模块文档）。字段都是可廉价克隆的句柄 ⇒ `FakeMeta` 本身可 `Clone`。
#[derive(Clone, Default)]
struct FakeMeta {
    snapshot: Arc<AtomicU64>,
    files: Arc<Mutex<Vec<FileManifest>>>,
    fail_payload: Arc<AtomicBool>,
}

impl FakeMeta {
    fn with(files: Vec<FileManifest>, snapshot: u64) -> Self {
        let me = Self::default();
        *me.files.lock().unwrap() = files;
        me.snapshot.store(snapshot, Ordering::SeqCst);
        me
    }

    /// 服务端"提交一次变更"：换掉文件集合并推进快照号。
    fn commit(&self, files: Vec<FileManifest>, snapshot: u64) {
        *self.files.lock().unwrap() = files;
        self.snapshot.store(snapshot, Ordering::SeqCst);
    }

    /// 载荷那一跳是否失败。
    fn fail_payload(&self, on: bool) {
        self.fail_payload.store(on, Ordering::SeqCst);
    }
}

#[tonic::async_trait]
impl pb::meta_server::Meta for FakeMeta {
    async fn prefetch(
        &self,
        req: Request<pb::PrefetchRequest>,
    ) -> Result<Response<pb::PrefetchResponse>, Status> {
        let req = req.into_inner();
        let snapshot = self.snapshot.load(Ordering::SeqCst);
        let is_probe = !req.full && req.tables.is_empty();
        if is_probe {
            // 探测：只回版本号与快照号（载荷为空 —— 客户端这一步只用它取名录）
            return Ok(Response::new(pb::PrefetchResponse {
                schema_ver: 1,
                manifest_ver: snapshot,
                snapshot,
                payload: Some(pb::PrefetchPayload::default()),
                full_reload: false,
            }));
        }
        if self.fail_payload.load(Ordering::SeqCst) {
            // 模拟"两跳之间网络抖了一下"：**载荷**失败
            return Err(Status::internal("注入的载荷失败（§175 用例）"));
        }
        // 载荷：用**与服务端逐字相同**的判据筛文件（这一条是整条链的要害）
        let since = req.since_snapshot;
        let files: Vec<pb::FileEntry> = self
            .files
            .lock()
            .unwrap()
            .iter()
            .filter(|f| f.valid_from > since || (f.deleted_at != 0 && f.deleted_at > since))
            .map(|f| pb::FileEntry {
                batch_id: f.batch_id.clone(),
                manifest: Some(yuntun_meta::op::manifest_to_proto_pub(f)),
            })
            .collect();
        Ok(Response::new(pb::PrefetchResponse {
            schema_ver: 1,
            manifest_ver: snapshot,
            snapshot,
            payload: Some(pb::PrefetchPayload {
                files,
                ..Default::default()
            }),
            full_reload: false,
        }))
    }

    async fn status(
        &self,
        _req: Request<pb::StatusRequest>,
    ) -> Result<Response<pb::StatusResponse>, Status> {
        Ok(Response::new(pb::StatusResponse::default()))
    }

    // 本用例用不到的接口：响亮地"未实现"，而不是 panic（panic 会变成连接级错误，更难读）。
    async fn propose(
        &self,
        _req: Request<pb::ProposeRequest>,
    ) -> Result<Response<pb::ProposeResponse>, Status> {
        Err(Status::unimplemented("假服务端不写"))
    }
    /// 增量路径**必须实现**：`refresh()` 在有变更时走的就是它（`changed_tables` → 载荷按名拉）。
    /// 这一步也是被测对象的一部分 —— 客户端拿 `since`（缓存的号）去问"变了哪些表"。
    async fn delta(
        &self,
        req: Request<pb::DeltaRequest>,
    ) -> Result<Response<pb::DeltaResponse>, Status> {
        let req = req.into_inner();
        let snapshot = self.snapshot.load(Ordering::SeqCst);
        // 本用例只有一张表：manifest 推进过就报它（真服务端按每表的 `table_manifest_ver` 判）
        let changed = if req.since_manifest_ver < snapshot {
            vec![TABLE.to_string()]
        } else {
            Vec::new()
        };
        Ok(Response::new(pb::DeltaResponse {
            schema_ver: 1,
            manifest_ver: snapshot,
            changed_tables: changed,
            full_reload: false,
        }))
    }
    async fn join(
        &self,
        _req: Request<pb::JoinRequest>,
    ) -> Result<Response<pb::JoinResponse>, Status> {
        Err(Status::unimplemented("假服务端不改成员"))
    }
    async fn promote(
        &self,
        _req: Request<pb::PromoteRequest>,
    ) -> Result<Response<pb::PromoteResponse>, Status> {
        Err(Status::unimplemented("假服务端不改成员"))
    }
    async fn remove(
        &self,
        _req: Request<pb::RemoveRequest>,
    ) -> Result<Response<pb::RemoveResponse>, Status> {
        Err(Status::unimplemented("假服务端不改成员"))
    }
    async fn heartbeat(
        &self,
        _req: Request<pb::HeartbeatRequest>,
    ) -> Result<Response<pb::HeartbeatResponse>, Status> {
        Err(Status::unimplemented("本用例不心跳"))
    }
    async fn raft(
        &self,
        _req: Request<pb::RaftRequest>,
    ) -> Result<Response<pb::RaftResponse>, Status> {
        Err(Status::unimplemented("本用例不分发 raft 消息"))
    }
}

async fn serve(fake: FakeMeta) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = futures::stream::unfold(listener, |l| async move {
        match l.accept().await {
            Ok((sock, _)) => Some((Ok::<_, std::io::Error>(sock), l)),
            Err(e) => Some((Err(e), l)),
        }
    });
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(pb::meta_server::MetaServer::new(fake))
            .serve_with_incoming(incoming)
            .await;
    });
    addr.to_string()
}

/// 读某快照下的可见集合。
///
/// ⚠️ 它同时是**刷新的触发器**：`RemoteCatalog::list_visible_files` 内部先 `refresh()` 并把错误
/// 抛出来（`current_snapshot()` 走 best-effort、会吞掉错误 ⇒ 只能用来*读号*，不能当触发器）。
async fn visible(
    catalog: &RemoteCatalog,
    snapshot: u64,
) -> Result<Vec<String>, yuntun_model::error::LakeError> {
    Ok(catalog
        .list_visible_files(TABLE, snapshot, None)
        .await?
        .into_iter()
        .map(|f| f.batch_id)
        .collect())
}

/// **载荷没落地 ⇒ 号不许前进**（`§175`）。
#[tokio::test]
async fn a_failed_payload_must_not_advance_the_snapshot() {
    // ① 起始：A 在快照 1 可见（首次刷新 = 全量，因为缓存还没 primed）
    let fake = FakeMeta::with(vec![file("A", 1, 0)], 1);
    let addr = serve(fake.clone()).await;
    let catalog = RemoteCatalog::connect(vec![addr]).expect("连接假服务端");

    assert_eq!(
        visible(&catalog, 1).await.expect("首次全量刷新"),
        vec!["A".to_string()]
    );
    assert_eq!(
        catalog.current_snapshot().await,
        1,
        "首次刷新后号应当落在载荷响应里的号上"
    );

    // ② 服务端提交一次"合并"：A 打墓碑（deleted_at = 2）、产物 M 入库（valid_from = 2），号 → 2，
    //    并且让**载荷**那一跳失败（模拟"两跳之间网络抖了一下"）。
    fake.commit(vec![file("A", 1, 2), file("M", 2, 0)], 2);
    fake.fail_payload(true);
    let err = visible(&catalog, 2)
        .await
        .expect_err("载荷失败 ⇒ 刷新必须失败");
    assert!(
        err.to_string().contains("注入的载荷失败"),
        "失败原因要能看出是**哪一跳**失败的：{err}"
    );

    // ③ **核心断言**：载荷没落地 ⇒ 号**不许**前进。
    //
    //    未修前这里会是 2 —— 于是下一轮增量带着 `since=2` 去问，而服务端的判据是
    //    `valid_from > since || deleted_at > since`：**恰好等于 2 的墓碑（A）与产物（M）
    //    都不会被下发** ⇒ 那批变更永久丢失，客户端永远停在"看得见 A"的世界里
    //    （一个偏小却很正常数字 —— `§174` 在 rig 上观测到的 ±3 就是这个形状）。
    assert_eq!(
        catalog.current_snapshot().await,
        1,
        "载荷失败却把号推进了 ⇒ 下一轮增量会漏掉这一批变更（`§175` / 台账 `D-23`）"
    );

    // ④ 放开载荷再刷一次：变更必须**完整**到达。
    fake.fail_payload(false);
    assert_eq!(
        visible(&catalog, 2).await.expect("载荷恢复后刷新"),
        vec!["M".to_string()],
        "合并产物必须可见、被合并掉的输入必须不可见（否则就是观测到的 ±3 那个形状）"
    );
    assert_eq!(catalog.current_snapshot().await, 2);
    assert_eq!(
        visible(&catalog, 1).await.expect("旧快照"),
        vec!["A".to_string()],
        "旧快照仍应看到合并前的输入（快照隔离的另一半）"
    );
}
