//! **WAL 里的 DDL 重放**（启动时的恢复路径）。
//!
//! 为什么放在 `yuntun-ingest` 而不是装配层：它读的是**WAL 的语义**、写的是**目录的语义**
//! —— 这两样都是写入路径的知识。放在这里，`standalone` 与数据节点进程（`yuntun-ingestor`）
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
use yuntun_model::ops::CreateTableRequest;
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
            other => {
                tracing::warn!(op = other, table = %p.table, "unknown ddl op in WAL, skipping");
                Ok(())
            }
        };
        if let Err(e) = res {
            tracing::warn!(table = %p.table, error = %e, "replay WAL DDL failed");
        }
    }
    if created > 0 || dropped > 0 {
        tracing::info!(created, dropped, "replayed WAL DDL records");
    }
    Ok(())
}
