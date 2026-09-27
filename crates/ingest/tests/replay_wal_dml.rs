//! **`F.3`：DELETE 的 WAL 重放**（`delta-dml-design §1.1` ③ / `§149`）。
//!
//! 三件事必须成立，缺一条就是"删除会丢"或"删除会串表"：
//!
//! 1. **只靠 WAL 就能重建目录**：位图内联在记录里，重放不再去读对象存储
//!    （对象是副产品，WAL 才是权威 —— ADR-3）；
//! 2. **一条记录覆盖多个文件**：两个文件两个条目（键是「这次删除 × 这个文件」）；
//! 3. **世代不符就跳过**：`DROP` → 同名重建之后，旧世代的删除不得挂到新表上
//!    （`plan.md` M0 ⑥ 的 DML 版）。

use std::sync::Arc;

use yuntun_catalog::{CatalogOps, MemoryCatalog};
use yuntun_model::dv::DvBitmap;
use yuntun_model::wal_record::{DeletePayload, DdlPayload, FileDeletion, Record, ddl_op};
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
