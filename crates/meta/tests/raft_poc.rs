//! S3-1 选型闸门用例（raft-rs）。
//!
//! 判据见 `crates/meta/src/lib.rs` 文件头。这两个用例必须证明：
//! **三节点能选主、能收敛、kill leader 后已提交数据不丢，且状态机接缝不需要为 raft 改动**。
//!
//! 注意（**诚实边界**）：跑的是 `MemStorage` + 进程内消息传递，所以
//! "落盘/崩溃恢复"不在本轮范围（那是 S3-3 的 fjall 后端 + S3-1b 的快照安装）。

use std::time::Duration;

use yuntun_meta::PEERS;
use yuntun_proto::meta as pb;

// ---------------------------------------------------------------- op 构造助手
//
// 用例现在直接用 **gRPC 面的 op 类型**（proto `Op`）—— 也就是说这些集群用例跑的就是
// 生产路径（日志 payload 与线上同构），不再有一套"只在测试里存在"的 op 编码。

fn create_schema_op(name: &str, now: u64) -> pb::Op {
    pb::Op {
        now_ms: now,
        kind: Some(pb::op::Kind::CreateSchema(pb::CreateSchemaOp {
            name: name.into(),
        })),
    }
}

fn create_table_op(name: &str, now: u64) -> pb::Op {
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("ts", arrow::datatypes::DataType::Int64, false),
    ]));
    pb::Op {
        now_ms: now,
        kind: Some(pb::op::Kind::CreateTable(pb::CreateTableOp {
            name: name.into(),
            namespace: "public".into(),
            arrow_schema_ipc: yuntun_model::meta::serialize_schema(&schema),
            default_format: "parquet".into(),
            partition_cols: vec![],
            ingest_config: {
                use prost::Message as _;
                yuntun_model::meta::IngestConfig::standard().encode_to_vec()
            },
        })),
    }
}

fn commit_op(table: &str, batch_id: &str, rows: u64, now: u64) -> pb::Op {
    use prost::Message as _;
    let req = pb::CommitFilesRequestMsg {
        table: table.into(),
        batch_id: batch_id.into(),
        client_request_id: None,
        client_request_ids: vec![],
        shard: "s0".into(),
        time_window: "w1".into(),
        files: vec![pb::FileManifestMsg {
            file_path: format!("p/{batch_id}.parquet"),
            batch_id: batch_id.into(),
            row_count: rows,
            ..Default::default()
        }],
        schema_version: 1,
        row_count: rows,
    };
    let _ = req.encode_to_vec();
    pb::Op {
        now_ms: now,
        kind: Some(pb::op::Kind::CommitFiles(pb::CommitFilesOp {
            request: Some(req),
        })),
    }
}

/// 等待上限。
///
/// ⚠️ 刻意宽松（**不是**延迟断言）：隔离跑这三个用例约 **0.2–0.3s**，但在
/// `cargo test --workspace` 下与其余 46 个 test binary 并行争 CPU 时，本进程的
/// tick/选举循环会被长时间抢占 —— 实测撞过 10s 上限（隔离 0.2s → 全量 10.4s 超时）。
/// 用例要证明的是"**能不能**靠快照追上"（正确性），不是"多快追上"（延迟属 S3-6 压测），
/// 所以放宽上限；与 `docs/status.md` 里 chaos 用例放宽到 60s 是同一处理。
const T: Duration = Duration::from_secs(60);

fn ops() -> Vec<pb::Op> {
    vec![
        create_schema_op("analytics", 1_000),
        create_table_op("cpu", 1_001),
        commit_op("public.cpu", "b1", 10, 1_002),
        commit_op("public.cpu", "b2", 20, 1_003),
    ]
}

/// 判据 1 + 3：三节点选主成功，提议的 op 在**所有节点**上收敛到同一状态，
/// 且状态机（`CatalogState`）原样被驱动 —— 没有为 raft 加任何分支。
#[test]
fn three_node_cluster_converges_on_proposed_ops() {
    let cluster = yuntun_meta::Cluster::start();
    let leader = cluster
        .wait_leader(T)
        .expect("三节点应在超时内选出 leader");

    for op in ops() {
        cluster.propose(op, T).expect("leader 在位时提议应成功");
    }

    // 收敛：每个节点的规范编码必须**逐字节相同**（这就是"副本一致"的口径）
    //
    // ⚠️ 等待条件是**语义的**（状态里出现第 2 个文件），不是 `applied >= 4`：
    // raft 的 **entry index ≠ 已应用 op 数** —— leader 就位会先写一条**空 no-op 条目**
    // 占掉 index 1，于是"4 个 op"对应 index 2..5。第一版按索引等待，在 follower
    // 只应用了 3 个 op（index=4）时就提前 break，得到"副本未收敛"的**假失败**。
    // 这个坑值得记：凡是把 raft index 当业务进度用的地方都会错位。
    let mut canon = Vec::new();
    for id in PEERS {
        let deadline = std::time::Instant::now() + T;
        loop {
            let c = cluster.canonical(id).expect("节点应存在");
            let text = String::from_utf8_lossy(&c);
            if text.contains("file b2") || std::time::Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        canon.push((id, cluster.canonical(id).expect("节点应存在")));
    }
    for (id, c) in &canon {
        assert_eq!(
            c, &canon[0].1,
            "节点 {id} 的状态与节点 {} 不一致 —— raft 副本未收敛（比对规范编码）",
            canon[0].0
        );
    }
    // 语义断言（不止编码相等：要确认 op 真的落进了状态机）
    let c0 = &canon[0].1;
    let text = String::from_utf8_lossy(c0);
    assert!(text.contains("ns analytics"), "create_schema 未生效：{text}");
    assert!(text.contains("table public.cpu"), "create_table 未生效");
    assert!(text.contains("file b1") && text.contains("file b2"), "commit_files 未生效");
    assert_eq!(leader, cluster.wait_leader(T).unwrap(), "leader 应保持稳定");
}

/// 判据 2：kill leader 后**重新选主**，且此前已提交的数据**一条不少**。
///
/// 这是 R3 的核心承诺（G2：写入不中断）在 raft 层的证据：
/// 提交 = 多数派持久化，所以杀掉一个节点不会丢已提交数据。
#[test]
fn leader_kill_reelects_and_keeps_committed_ops() {
    let mut cluster = yuntun_meta::Cluster::start();
    let old_leader = cluster.wait_leader(T).expect("先选出 leader");
    for op in ops() {
        cluster.propose(op, T).expect("初始 4 个 op 应提交");
    }
    assert_eq!(cluster.alive(), 3);

    // 杀掉 leader（线程退出 + 从路由表移除 = 它再也收不到消息）
    cluster.kill(old_leader);
    assert_eq!(cluster.alive(), 2, "剩两个节点 = 多数派仍在");

    let survivors: Vec<u64> = PEERS.iter().copied().filter(|id| *id != old_leader).collect();
    // 重新选主：必须由**存活节点**当选
    let deadline = std::time::Instant::now() + T;
    let new_leader = loop {
        if let Some(l) = cluster.wait_leader(Duration::from_millis(200)) {
            assert!(
                survivors.contains(&l),
                "被杀的节点 {l} 不可能当选（它已收不到消息）"
            );
            break l;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "kill leader 后未在超时内重新选主 —— 可用性承诺不成立"
        );
    };
    assert_ne!(new_leader, old_leader);

    // 新 leader 必须能继续接活（写入不中断）
    cluster
        .propose(
            commit_op("public.cpu", "b3", 30, 1_004),
            T,
        )
        .expect("新 leader 必须能继续提交");

    // 已提交数据（含换主前的 b1/b2）在两个存活节点上都必须在
    for id in survivors {
        let deadline = std::time::Instant::now() + T;
        let text = loop {
            let c = cluster.canonical(id).expect("存活节点");
            let text = String::from_utf8_lossy(&c).to_string();
            // 同样按**语义**等待（b3 出现 = 新 leader 的提交已被本节点应用）
            if text.contains("file b3") || std::time::Instant::now() >= deadline {
                break text;
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        for b in ["b1", "b2", "b3"] {
            assert!(
                text.contains(&format!("file {b}")),
                "节点 {id} 丢了已提交数据 {b} —— raft 多数派语义未生效"
            );
        }
    }
}

/// **S3-1b**：follower **崩溃重启**后（带自己的日志）能恢复并继续跟随，且**不重复应用**。
///
/// 场景（与设计 §6 验收表"follower 落后 + leader 已压缩"同构）：
///
/// 1. 三节点写若干 op，全部收敛；
/// 2. **杀一个 follower**（**保留它自己的日志**），leader 与另一节点继续写；
/// 3. leader **压缩**：`first_index` 越过 victim 的已应用位置 → victim 需要的那些条目**已被丢弃**；
/// 4. victim **带着自己的日志重启**（崩溃恢复的形态）→ 它从自己的 last 往后追，
///    而 leader 只剩压缩点之后的条目 → **只能靠快照**；
/// 5. 断言：重启后状态与 leader **逐字节相同**（**不重复应用** —— 这条钉住 `Config.applied`：
///    不给 raft 报已应用位置时，它会重放已应用条目，状态机的计数器被重复推进 →
///    重启过的副本与其他副本**静默分叉**）；
/// 6. 再写一笔：重启后必须还能继续跟随。
///
/// # 关于"是否真的走了快照"
///
/// 本用例**不再断言** `snapshot_installs >= 1`：实测 leader 是否发快照取决于它对该 peer 的
/// Progress 状态，同一场景下出现过 `installs=0 但已收敛`（详见 `operation-log §42.7` 遗留 1：
/// 轨迹显示 leader 日志确实已压缩到 first=8、且**没有**调用 `Storage::snapshot()`，
/// 但 victim 仍拿到了 6..7 —— 未归因）。快照安装的机制由**存储层单测** + **反证**
/// （不还原状态机 → `installs=1` 但状态不一致）固定，不依赖本用例。
/// 这里把 `installs` 当**观测数据**打印，供归因时取证。
///
/// 没有这一步，PoC 就只证明了"日志没截断时能复制" —— 而真实集群一定会截断。
///
/// ⚠️ 反面教训（本轮实测撞出来的）：**不要用"同 id + 抹掉存储"来造这个场景**。
/// 那是 raft 的非法操作：leader 仍记着该 peer 的旧 `matched`，于是发 `commit=N` 的心跳，
/// 空日志的 follower 在 `handleHeartbeat` 里无条件 `commit_to(N)` → 直接
/// `to_commit N is out of range [last_index 0]` panic（etcd 那句 "Was the raft log
/// corrupted, truncated, or lost?"）。掉盘节点只能**换新 id 重新加入**（S3-6 成员变更）。
#[test]
fn follower_behind_catches_up_via_snapshot() {
    let mut cluster = yuntun_meta::Cluster::start();
    let leader = cluster.wait_leader(T).expect("三节点应选出 leader");

    // 建 schema/表（PoC 的 op 语义：commit 要求表已存在）
    cluster
        .propose(
            create_schema_op("analytics", 1_000),
            T,
        )
        .expect("建 schema");
    cluster
        .propose(
            create_table_op("cpu", 1_001),
            T,
        )
        .expect("建表");

    let mut now = 2_000u64;
    let mut op = |batch: &str| {
        now += 1;
        commit_op("public.cpu", batch, 1, now)
    };
    for b in ["pre1", "pre2"] {
        cluster.propose(op(b), T).expect("压缩前写入");
    }
    // 等三节点都收敛（否则"落后"这件事说不清）
    wait_all_equal(&cluster, T);

    // ② 杀掉一个 follower（挑非 leader）
    let victim = PEERS
        .iter()
        .copied()
        .find(|id| *id != leader)
        .expect("应存在非 leader 节点");
    let victim_before = cluster.applied(victim).expect("victim 在运行");
    cluster.kill(victim);
    assert_eq!(cluster.alive(), 2, "两节点 = 多数派仍在，可继续写");
    for b in ["post1", "post2"] {
        cluster.propose(op(b), T).expect("leader 存活期间应能提交");
    }

    // ③ leader 压缩：此后 leader 再无 victim 需要的那些日志
    let compacted = cluster.compact(leader).expect("leader 在运行");
    assert!(
        compacted >= 1,
        "压缩位置必须 > 0（否则 raft 不会认为需要发快照，用例就没测到东西）"
    );
    // 压缩坐标必须是**已应用的 raft 索引**，而不是状态机的 op 计数
    // （no-op/ConfChange 也占索引；用 op 计数当坐标会错开一格 → 静默分叉）
    assert_eq!(
        compacted,
        cluster.applied(leader).unwrap(),
        "压缩坐标应等于已应用的 raft 索引"
    );
    assert_eq!(cluster.compacted_index(leader).unwrap(), compacted);

    // victim 需要的条目（它已应用位置之后的那些）必须**已被丢弃**，否则测不到快照
    assert!(
        victim_before < compacted,
        "victim 已应用 {victim_before} < leader 压缩位置 {compacted} 才说明它要的条目没了"
    );
    // ④ 带着自己的日志重启 victim（崩溃恢复的形态）
    cluster.restart(victim);

    // ⑤ 等状态与 leader 一致（不强制要求走快照，见上文说明）
    let deadline = std::time::Instant::now() + T;
    loop {
        let same = cluster.canonical(victim) == cluster.canonical(leader);
        if same {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "victim 重启后未在超时内恢复并收敛：installs={} （leader={leader} compacted={compacted} \
             victim={victim}）\n{}",
            cluster.snapshot_installs(victim).unwrap_or(0),
            cluster.dump()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    // 观测数据（归因用）：本次是靠快照还是靠条目追上的
    eprintln!(
        "[s3-1b] victim={victim} 重启后收敛；snapshot_installs={}（leader compacted={compacted}）",
        cluster.snapshot_installs(victim).unwrap_or(0)
    );
    // 对齐 ⑥：装完快照后继续跟随
    cluster
        .propose(op("after_restart"), T)
        .expect("重启后仍应能写入");
    let deadline = std::time::Instant::now() + T;
    while cluster.canonical(victim) != cluster.canonical(leader) {
        assert!(
            std::time::Instant::now() < deadline,
            "重启后 victim 跟不上新写入"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    // 三个节点的状态都相同（含被重启的那个）
    wait_all_equal(&cluster, T);
}

/// 等 `PEERS` 里**所有存活节点**的状态机规范编码一致。
fn wait_all_equal(cluster: &yuntun_meta::Cluster, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let alive: Vec<u64> = PEERS
            .iter()
            .copied()
            .filter(|id| cluster.applied(*id).is_some())
            .collect();
        let first = cluster.canonical(alive[0]);
        if alive.iter().all(|id| cluster.canonical(*id) == first) {
            // 不能只看"都等于第一个"：还要确认第一个不是空状态（否则会瞬间通过）
            if let Some(c) = &first {
                if !c.is_empty() && cluster.applied(alive[0]).unwrap_or(0) > 0 {
                    return;
                }
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "等待全部节点收敛超时（alive={alive:?}）"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// **M3 的 G1 判据**：metanode **全量重启**后 Catalog 与重启前**逐字节一致**。
///
/// 这是 R3 的目标 1（`metanode-design.md §1.1`），也是"能不能把元数据托付给这套存储"的总检验。
/// 它一次压到四件事：
///
/// | # | 压到什么 | 错了会怎样 |
/// |---|---|---|
/// | 1 | 日志与硬状态真落盘（`append`/`set_hard_state` 用 SyncAll） | 重启后数据倒退 |
/// | 2 | 状态机由盘上**重建**（快照 + 其后日志重放） | 状态凭空少一半（或整份空） |
/// | 3 | `Config.applied` 取对（= 快照 index） | raft 重放已应用条目 → 计数器多走 → **静默分叉** |
/// | 4 | 成员表恢复 | 重启后组不成立（选不出 leader） |
///
/// 场景里**先在 leader 上压缩一次**：于是 leader 重启走"从快照恢复 + 重放其后条目"，
/// 其余节点走"空状态机 + 全量日志重放"—— 两条重建路径都被覆盖（这是 42.4b 那个
/// "怎么稳定走到快照"的遗留在本层的替代验证：**恢复路径**而非**发送路径**）。
#[test]
fn full_cluster_restart_keeps_state_byte_identical() {
    let mut cluster = yuntun_meta::Cluster::start();
    let leader = cluster.wait_leader(T).expect("三节点应选出 leader");

    cluster
        .propose(
            create_schema_op("analytics", 1_000),
            T,
        )
        .expect("建 schema");
    cluster
        .propose(
            create_table_op("cpu", 1_001),
            T,
        )
        .expect("建表");
    let mut now = 2_000u64;
    for b in ["r1", "r2", "r3", "r4"] {
        now += 1;
        cluster
            .propose(
                commit_op("public.cpu", b, 1, now),
                T,
            )
            .expect("写入");
    }
    wait_all_equal(&cluster, T);

    // 让 leader 压缩一次：制造"从快照恢复"的重建路径
    let compacted = cluster.compact(leader).expect("leader 在运行");
    assert!(compacted > 0, "压缩必须真的发生（否则只覆盖了重放路径）");

    // 记下重启前的状态（逐字节）
    let before: Vec<(u64, Vec<u8>)> = PEERS
        .iter()
        .map(|id| (*id, cluster.canonical(*id).expect("节点在运行")))
        .collect();

    // **全量重启**（模拟"metanode 全量重启"：三个进程都死掉再起来）
    for id in PEERS {
        cluster.kill(id);
    }
    assert_eq!(cluster.alive(), 0, "全部停掉（此时只剩盘上的文件）");
    for id in PEERS {
        cluster.restart(id);
    }

    // 断言 G1：每个节点的状态与重启前**逐字节一致**
    let deadline = std::time::Instant::now() + T;
    loop {
        let all_same = before
            .iter()
            .all(|(id, want)| cluster.canonical(*id).as_ref() == Some(want));
        if all_same {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "全量重启后状态与重启前不一致（M3 的 G1 不成立）\n{}",
            cluster.dump()
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    // 重启后必须**还能服务**：再写一笔，三个节点继续收敛
    now += 1;
    cluster
        .propose(
            commit_op("public.cpu", "after_restart", 1, now),
            T,
        )
        .expect("重启后应能继续提交");
    wait_all_equal(&cluster, T);
}
