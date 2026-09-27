//! **WAL 里的 DDL 重放**（启动时的恢复路径）。
//!
//! 为什么放在 `yuntun-ingest` 而不是装配层：它读的是**WAL 的语义**、写的是**目录的语义**
//! —— 这两样都是写入路径的知识。放在这里，`standalone` 与数据进程（`yuntun-datanode`）
//! 才可能共用同一份实现（各自复制一份必然漂移）。
//!
//! 两条纪律：
//! 1. **幂等**：重放会重复执行（每次启动都扫一遍 WAL）⇒ "已存在/已删除"必须当成成功，
//!    而不是错误。少了这条，节点重启就会因为"表已存在"而拒绝启动；
//! 2. **失败不致命**：单条 DDL 失败只告警（数据面的重放有别的路径兜底），
//!    不让一个坏记录挡住整个启动。

use std::sync::Arc;

use yuntun_catalog::CatalogOps;
use yuntun_model::meta::{deserialize_schema, IngestConfig};
use yuntun_model::ops::{CreateTableRequest, EvolveSchemaRequest};
use yuntun_model::schema::{classify, SchemaChange, SchemaCompatibility};
use yuntun_model::wal_record::{ddl_op, Record};
use yuntun_wal::writer::WalWriter;

pub async fn replay_wal_ddl(
    catalog: &Arc<dyn CatalogOps>,
    wal: &WalWriter,
) -> Result<(), yuntun_model::error::LakeError> {
    let reader = yuntun_wal::reader::WalReader::new(wal.shard_dir());
    let records = reader.scan_from(0)?;
    let mut created = 0usize;
    let mut dropped = 0usize;
    let mut altered = 0usize;
    for (_, rec) in records {
        let Record::Ddl(p) = rec else { continue };
        let res = match p.op {
            ddl_op::CREATE_TABLE => {
                let schema = deserialize_schema(&p.arrow_schema)?;
                // 多 schema：WAL 中的表标识为全限定 `schema.table`
                let (ns, bare) = yuntun_model::ops::split_qualified(&p.table);
                let req = CreateTableRequest {
                    name: bare.to_string(),
                    namespace: ns.to_string(),
                    schema,
                    partition_cols: vec![],
                    default_format: if p.default_format.is_empty() {
                        "parquet".to_string()
                    } else {
                        p.default_format.clone()
                    },
                    ingest_config: IngestConfig::standard(),
                };
                match catalog.create_table(req).await {
                    Ok(_) => {
                        created += 1;
                        Ok(())
                    }
                    Err(yuntun_model::error::LakeError::TableAlreadyExists(_)) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            ddl_op::DROP_TABLE => match catalog.drop_table(&p.table).await {
                Ok(()) => {
                    dropped += 1;
                    Ok(())
                }
                Err(yuntun_model::error::LakeError::TableNotFound(_)) => Ok(()),
                Err(e) => Err(e),
            },
            // 多 schema：schema 事件（`DdlPayload.table` = schema 名）
            ddl_op::CREATE_SCHEMA => match catalog.create_schema(&p.table).await {
                Ok(()) => {
                    created += 1;
                    Ok(())
                }
                Err(yuntun_model::error::LakeError::SchemaAlreadyExists(_)) => Ok(()),
                Err(e) => Err(e),
            },
            ddl_op::DROP_SCHEMA => match catalog.drop_schema(&p.table).await {
                Ok(()) => {
                    dropped += 1;
                    Ok(())
                }
                Err(yuntun_model::error::LakeError::SchemaNotFound(_)) => Ok(()),
                Err(e) => Err(e),
            },
            // F.2：`ALTER TABLE` —— 把表的 schema **收敛到记录里的目标态**（幂等）
            ddl_op::ALTER_TABLE => {
                let target = deserialize_schema(&p.arrow_schema)?;
                let n = converge_schema(catalog, &p.table, &target).await?;
                altered += n;
                Ok(())
            }
            other => {
                tracing::warn!(op = other, table = %p.table, "unknown ddl op in WAL, skipping");
                Ok(())
            }
        };
        if let Err(e) = res {
            tracing::warn!(table = %p.table, error = %e, "replay WAL DDL failed");
        }
    }
    if created > 0 || dropped > 0 || altered > 0 {
        tracing::info!(created, dropped, altered, "replayed WAL DDL records");
    }
    Ok(())
}

/// 把 `table` 的 schema **收敛到 `target`**（幂等；`ALTER TABLE` 的重放路径，F.2）。
///
/// 为什么记录里存的是**目标 schema** 而不是"变更本身"：重放时表可能**已经**演进过
/// （节点重启前就落过目录 / raft 快照；重放本身也会重复执行）—— 记目标态则只需
/// "还差什么就补什么"，天然幂等，与上面 CREATE/DROP 两条同为一条纪律。
///
/// 每轮只施加**一个** `SchemaChange`（`classify` 一次给一个），循环到没有差异为止 ——
/// 上限 32 轮防死循环（正常至多两三轮：加列 / 删列各一次）。
async fn converge_schema(
    catalog: &Arc<dyn CatalogOps>,
    table: &str,
    target: &arrow::datatypes::SchemaRef,
) -> Result<usize, yuntun_model::error::LakeError> {
    let mut applied = 0usize;
    for _ in 0..32 {
        let Some((current, version)) = catalog.table_schema(table).await? else {
            // 表不在（本记录之前那条 CREATE 没重放出来）⇒ 交给后面的记录，不在这里造表
            return Ok(applied);
        };
        let change = match classify(&current, target) {
            SchemaCompatibility::Compatible => {
                // `classify` 只回答"目标列都被覆盖了吗"，**看不见多余的列** ——
                // 删列要在这里自己找（否则重放一条 `DROP COLUMN` 会静默不生效）
                match current
                    .fields()
                    .iter()
                    .find(|f| target.field_with_name(f.name()).is_err())
                {
                    Some(extra) => SchemaChange::DropColumn {
                        column: extra.name().clone(),
                    },
                    None => return Ok(applied),
                }
            }
            SchemaCompatibility::NeedsEvolve(c) => c,
            SchemaCompatibility::Incompatible(reason) => {
                return Err(yuntun_model::error::LakeError::InvalidSchemaChange(format!(
                    "WAL DDL 重放：{table} 无法收敛到目标 schema（{reason}）"
                )))
            }
        };
        match catalog
            .evolve_schema(EvolveSchemaRequest {
                table: table.to_string(),
                change,
                expected_version: version,
            })
            .await
        {
            Ok(_) => applied += 1,
            // 与别的 DDL 撞了版本：**下一轮重读**（这个循环本来就以"重读当前态"开头）
            Err(yuntun_model::error::LakeError::SchemaChanged { .. }) => {}
            Err(e) => return Err(e),
        }
    }
    Err(yuntun_model::error::LakeError::Other(format!(
        "WAL DDL 重放：{table} 的 schema 收敛未在 32 轮内完成"
    )))
}
