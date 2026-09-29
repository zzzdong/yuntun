//! **`F.3`：DML 的 WAL 重放**（`delta-dml-design §1.1` ③ / `§149`、`§154`）。
//!
//! 三件事必须成立，缺一条就是"删除会丢"或"删除会串表"：
//!
//! 1. **只靠 WAL 就能重建目录**：位图内联在记录里，重放不再去读对象存储
//!    （对象是副产品，WAL 才是权威 —— ADR-3）；
//! 2. **一条记录覆盖多个文件**：两个文件两个条目（键是「这次删除 × 这个文件」）；
//! 3. **世代不符就跳过**：`DROP` → 同名重建之后，旧世代的删除不得挂到新表上
//!    （`plan.md` M0 ⑥ 的 DML 版）；
//! 4. **`UPDATE` 重放之后仍然原子**（`F.3e-2`）：两半共用**同一个快照号** ——
//!    分两次提交就会在两个快照上落下两半，读侧照样能撞见中间态。

use std::sync::Arc;

use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_model::dv::DvBitmap;
use yuntun_model::wal_record::{
    DeletePayload, DdlPayload, FileDeletion, NewFilePayload, Record, UpdatePayload, ddl_op,
};
use yuntun_wal::writer::WalWriter;

const TABLE: &str = "public.t";

fn deletion(file: &str, batch: &str, positions: &[u32]) -> FileDeletion {
    FileDeletion {
        file_path: file.into(),
        batch_id: batch.into(),
        bitmap: DvBitmap::from_positions(positions.iter().copied()).to_bytes(),
    }
}

#[tokio::test]
async fn replay_rebuilds_deletions_from_the_wal_alone() {
    let guard = yuntun_testkit::TestDir::tmpfs("replay-dml-wal");
    let wal_dir = guard.path().to_path_buf();
    let wal = WalWriter::open(yuntun_wal::WalConfig::for_dir(&wal_dir), 0)
        .await
        .unwrap();

    // ① 建表（世代 1）
    wal.append(Record::Ddl(DdlPayload {
        op: ddl_op::CREATE_TABLE,
        table: TABLE.into(),
        arrow_schema: yuntun_model::meta::serialize_schema(&Arc::new(
            arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                "v",
                arrow::datatypes::DataType::Int64,
                true,
            )]),
        )),
        default_format: "parquet".into(),
    }))
    .await
    .unwrap();

    // ② 一条 DELETE：**两个文件**，同一个 dv_id
    wal.append(Record::Delete(DeletePayload {
        table: TABLE.into(),
        dv_id: "dv-1".into(),
        deletions: vec![
            deletion("yuntun/public/t/dt=w/shard=s0/a.parquet", "b-a", &[0, 2]),
            deletion("yuntun/public/t/dt=w/shard=s0/b.parquet", "b-b", &[7]),
        ],
        schema_epoch: 1,
    }))
    .await
    .unwrap();

    // ③ 一条**旧世代**的 DELETE（模拟 DROP→重建之前的删除）：必须被跳过
    wal.append(Record::Delete(DeletePayload {
        table: TABLE.into(),
        dv_id: "dv-stale".into(),
        deletions: vec![deletion("yuntun/public/t/dt=w/shard=s0/old.parquet", "b-old", &[1])],
        schema_epoch: 99,
    }))
    .await
    .unwrap();

    // 重放：表清单 + 删除
    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    yuntun_ingest::replay_wal_ddl(&catalog, &wal).await.unwrap();
    assert!(
        catalog.get_table(TABLE).await.unwrap().is_some(),
        "前提：DDL 重放先把表立起来（删除是对表的事实）"
    );
    let stats = yuntun_ingest::replay_wal_dml(&catalog, &wal).await.unwrap();
    assert_eq!(stats.applied, 1, "只有一条记录属于当前世代");
    assert_eq!(stats.skipped, 1, "旧世代的那条必须被跳过");
    assert_eq!(stats.entries, 2, "一条记录覆盖两个文件 ⇒ 两个条目");

    // 目录里：两条条目、card 从位图**量**出来、store_path 按公式回推
    let snap = catalog.current_snapshot().await;
    let mut live = catalog.list_deletions(TABLE, snap).await.unwrap();
    live.sort_by(|a, b| a.file_path.cmp(&b.file_path));
    assert_eq!(live.len(), 2);
    assert!(live.iter().all(|d| d.dv_id == "dv-1"), "同一 dv_id 覆盖两个文件");
    assert_eq!(live[0].card, 2, "card 来自位图（a 文件删 2 行）");
    assert_eq!(live[0].batch_id, "b-a", "对账键：dv → file_path → batch_id");
    assert_eq!(
        live[0].store_path,
        yuntun_model::dv::dv_object_path(&live[0].file_path, "dv-1"),
        "store_path 按 §3.1 的公式回推（重放不读对象）"
    );
    assert_eq!(live[1].card, 1);
    assert!(
        live.iter().all(|d| d.applied_at == snap && d.revoked_at == 0),
        "同一条记录的全部条目共用同一个快照号（单快照原子），且都生效中"
    );
    // 旧世代那条**没有**留下任何痕迹
    assert!(
        live.iter().all(|d| d.dv_id != "dv-stale" && !d.file_path.ends_with("old.parquet")),
        "旧世代的删除不得挂到新表上"
    );

    // 重放**幂等**：再来一遍不写两份、也不推快照号
    let again = yuntun_ingest::replay_wal_dml(&catalog, &wal).await.unwrap();
    assert_eq!(again.entries, 2, "重放照旧「重建」（目录侧按 (dv_id,file) 去重）");
    assert_eq!(catalog.current_snapshot().await, snap, "重复重放不推快照号");
    assert_eq!(catalog.list_deletions(TABLE, snap).await.unwrap().len(), 2);
}

/// **`UPDATE` 的 WAL 重放**（`F.3e-2`）：两半由**一条 `apply_update`** 重建 ——
/// 重放之后"删 + 插"仍然在**同一个快照**上生效（`F.7` 决策 5 在重放路径上的那一半）。
#[tokio::test]
async fn replay_rebuilds_an_update_atomically() {
    let guard = yuntun_testkit::TestDir::tmpfs("replay-update-wal");
    let wal_dir = guard.path().to_path_buf();
    let wal = WalWriter::open(yuntun_wal::WalConfig::for_dir(&wal_dir), 0)
        .await
        .unwrap();

    // ① 建表（世代 1）
    wal.append(Record::Ddl(DdlPayload {
        op: ddl_op::CREATE_TABLE,
        table: TABLE.into(),
        arrow_schema: yuntun_model::meta::serialize_schema(&Arc::new(
            arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                "v",
                arrow::datatypes::DataType::Int64,
                true,
            )]),
        )),
        default_format: "parquet".into(),
    }))
    .await
    .unwrap();

    // ② 一条 UPDATE：命中 b1 的第 0、2 行 ⇒ 删除向量；产物落成一个新文件
    wal.append(Record::Update(UpdatePayload {
        table: TABLE.into(),
        upd_id: "upd-1".into(),
        deletions: vec![deletion("yuntun/public/t/dt=w/shard=s0/a.parquet", "b-a", &[0, 2])],
        new_files: vec![NewFilePayload {
            file_path: "yuntun/public/t/dt=w/shard=s0/upd-1.parquet".into(),
            batch_id: "b-upd".into(),
            row_count: 2,
            file_size: 1234,
            shard: "s0".into(),
            time_window: "w".into(),
            schema_version: 1,
        }],
        schema_epoch: 1,
    }))
    .await
    .unwrap();

    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    yuntun_ingest::replay_wal_ddl(&catalog, &wal).await.unwrap();
    let stats = yuntun_ingest::replay_wal_dml(&catalog, &wal).await.unwrap();
    assert_eq!(stats.applied, 1, "这条 UPDATE 属于当前世代");
    assert_eq!(stats.entries, 1, "被命中的一个文件 ⇒ 一份删除向量");
    assert_eq!(stats.new_files, 1, "新行那半也要重建（否则数据少了一半）");

    let snap = catalog.current_snapshot().await;
    // 两半**同号**（原子性判据）
    let files = catalog.list_visible_files(TABLE, snap, None).await.unwrap();
    let nf = files
        .iter()
        .find(|f| f.batch_id == "b-upd")
        .expect("新行文件必须在重放后可见");
    assert_eq!(nf.valid_from, snap, "新文件与删除向量必须同号");
    assert_eq!(nf.row_count, 2);
    assert_eq!(nf.shard, "s0", "产物继承源文件的 shard（按分片读的语义不变）");
    let live = catalog.list_deletions(TABLE, snap).await.unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].dv_id, "upd-1");
    assert_eq!(live[0].card, 2);
    assert_eq!(live[0].applied_at, snap, "两半同号 ⇒ 重放之后仍然原子");
    assert_eq!(
        live[0].store_path,
        yuntun_model::dv::dv_object_path(&live[0].file_path, "upd-1"),
        "store_path 按 §3.1 的公式回推（重放不读对象）"
    );
    // 旧快照：两半都不可见
    assert!(
        catalog
            .list_deletions(TABLE, snap - 1)
            .await
            .unwrap()
            .is_empty(),
        "旧快照不许看到删除"
    );
    assert!(
        !catalog
            .list_visible_files(TABLE, snap - 1, None)
            .await
            .unwrap()
            .iter()
            .any(|f| f.batch_id == "b-upd"),
        "旧快照不许看到新行"
    );

    // 重放**幂等**：再来一遍不推版本、不写第二份
    let again = yuntun_ingest::replay_wal_dml(&catalog, &wal).await.unwrap();
    assert_eq!(again.applied, 1, "重放照旧重建");
    assert_eq!(catalog.current_snapshot().await, snap, "完全重复 ⇒ 不推快照号");
    assert_eq!(catalog.list_deletions(TABLE, snap).await.unwrap().len(), 1);
    assert_eq!(
        catalog.list_visible_files(TABLE, snap, None).await.unwrap().len(),
        1,
        "不会插出第二个文件"
    );
}

/// **旧世代的 `UPDATE` 必须被跳过**：只重放一半（删除生效、新行丢掉）就是少数据 ——
/// 比"整条跳过"糟得多。
#[tokio::test]
async fn replay_skips_an_update_from_another_epoch() {
    let guard = yuntun_testkit::TestDir::tmpfs("replay-update-stale");
    let wal_dir = guard.path().to_path_buf();
    let wal = WalWriter::open(yuntun_wal::WalConfig::for_dir(&wal_dir), 0)
        .await
        .unwrap();
    wal.append(Record::Ddl(DdlPayload {
        op: ddl_op::CREATE_TABLE,
        table: TABLE.into(),
        arrow_schema: yuntun_model::meta::serialize_schema(&Arc::new(
            arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                "v",
                arrow::datatypes::DataType::Int64,
                true,
            )]),
        )),
        default_format: "parquet".into(),
    }))
    .await
    .unwrap();
    wal.append(Record::Update(UpdatePayload {
        table: TABLE.into(),
        upd_id: "upd-stale".into(),
        deletions: vec![deletion("yuntun/public/t/dt=w/shard=s0/old.parquet", "b-old", &[1])],
        new_files: vec![NewFilePayload {
            file_path: "yuntun/public/t/dt=w/shard=s0/old-upd.parquet".into(),
            batch_id: "b-old-upd".into(),
            row_count: 1,
            file_size: 10,
            shard: "s0".into(),
            time_window: "w".into(),
            schema_version: 1,
        }],
        schema_epoch: 99, // 当前世代是 1
    }))
    .await
    .unwrap();

    let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
    yuntun_ingest::replay_wal_ddl(&catalog, &wal).await.unwrap();
    let stats = yuntun_ingest::replay_wal_dml(&catalog, &wal).await.unwrap();
    assert_eq!(stats.applied, 0);
    assert_eq!(stats.skipped, 1);
    let snap = catalog.current_snapshot().await;
    assert!(catalog.list_deletions(TABLE, snap).await.unwrap().is_empty());
    assert!(
        catalog
            .list_visible_files(TABLE, snap, None)
            .await
            .unwrap()
            .is_empty(),
        "旧世代的 UPDATE **两半都不许落地**（只落一半就是少数据）"
    );
}
