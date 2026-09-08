//! Schema 缓存与 OCC 演进循环（详细设计 §5.2 步骤①-④）。
//!
//! 【C8 关键】`EvolveSchema` 必须在写 S3 之前完成 —— 保证不出现
//! "已写 S3 但 schema 冲突"的中间态。
//! 收到 `SCHEMA_CHANGED` 后：不重试写入（数据仍在 WAL），
//! 拉取新 schema 重新判定（§6.7），最多 3 次（§10.2）。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use yuntun_catalog::CatalogOps;
use yuntun_model::error::LakeError;
use yuntun_model::schema::{classify, SchemaCompatibility};

/// 本地缓存的表 schema。
#[derive(Debug, Clone)]
pub struct CachedSchema {
    pub schema: arrow::datatypes::SchemaRef,
    pub version: u64,
}

/// Ingestor 本地 schema 缓存（避免每次写入都查 Catalog，ADR-6 同理）。
#[derive(Default)]
pub struct SchemaCache {
    inner: RwLock<HashMap<String, CachedSchema>>,
}

impl SchemaCache {
    pub async fn get(
        &self,
        catalog: &Arc<dyn CatalogOps>,
        table: &str,
    ) -> Result<CachedSchema, LakeError> {
        if let Some(c) = self.inner.read().await.get(table) {
            return Ok(c.clone());
        }
        let (schema, version) = catalog
            .table_schema(table)
            .await?
            .ok_or_else(|| LakeError::TableNotFound(table.to_string()))?;
        let c = CachedSchema { schema, version };
        self.inner
            .write()
            .await
            .insert(table.to_string(), c.clone());
        Ok(c)
    }

    pub async fn update(&self, table: &str, schema: arrow::datatypes::SchemaRef, version: u64) {
        self.inner
            .write()
            .await
            .insert(table.to_string(), CachedSchema { schema, version });
    }
}

/// Schema 判定 + OCC 演进循环（详细设计 §5.2 伪码的 `loop { ... }`）。
/// 返回本次写入应使用的 schema_version；成功时缓存已同步到最新。
///
/// 【阶段 0.5 修订（chaos T6.4 发现）】文档 §10.2 的"SchemaChanged 最多重试 3 次"
/// 在 N 个并发 writer 各自演进不同列的场景下不足 —— 每轮 OCC 只有一个 winner，
/// 最坏需要 N 轮竞争。修订为：上限 32 次 + 指数退避（1ms 起，上限 50ms），
/// 打散重试时刻，避免羊群效应。
pub async fn resolve_schema_version(
    catalog: &Arc<dyn CatalogOps>,
    cache: &SchemaCache,
    table: &str,
    incoming: &arrow::datatypes::SchemaRef,
) -> Result<u64, LakeError> {
    const MAX_RETRIES: usize = 32;
    let mut backoff = Duration::from_millis(1);
    for _attempt in 0..=MAX_RETRIES {
        let cached = cache.get(catalog, table).await?;
        match classify(&cached.schema, incoming) {
            SchemaCompatibility::Compatible => return Ok(cached.version),
            SchemaCompatibility::Incompatible(reason) => {
                return Err(LakeError::SchemaIncompatible(reason));
            }
            SchemaCompatibility::NeedsEvolve(change) => {
                // ④ OCC 演进（必须在写 S3 之前）
                match catalog
                    .evolve_schema(yuntun_model::ops::EvolveSchemaRequest {
                        table: table.to_string(),
                        change,
                        expected_version: cached.version,
                    })
                    .await
                {
                    Ok(resp) => {
                        cache
                            .update(table, resp.new_schema.clone(), resp.version)
                            .await;
                        return Ok(resp.version);
                    }
                    Err(LakeError::SchemaChanged {
                        actual_version,
                        new_schema,
                    }) => {
                        // 拉取新 schema，回到 ② 重新判定
                        cache.update(table, new_schema, actual_version).await;
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_millis(50));
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
    }
    Err(LakeError::Other(format!(
        "schema evolve retries exhausted (SchemaChanged > {MAX_RETRIES} times)"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as SArc;
    use yuntun_catalog::MemoryCatalog;
    use yuntun_model::ops::CreateTableRequest;

    fn sch(fields: &[(&str, DataType)]) -> arrow::datatypes::SchemaRef {
        let fs: Vec<Field> = fields
            .iter()
            .map(|(n, t)| Field::new(*n, t.clone(), true))
            .collect();
        SArc::new(Schema::new(fs))
    }

    // T2.6：OCC 冲突分支覆盖（写入时序中的 SchemaChanged 重试）
    #[tokio::test]
    async fn evolve_retry_after_conflict() {
        let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
        catalog
            .create_table(CreateTableRequest {
                name: "t".into(),
                schema: sch(&[("a", DataType::Int64)]),
                partition_cols: vec![],
                default_format: "parquet".into(),
                ingest_config: Default::default(),
            })
            .await
            .unwrap();

        // 模拟另一节点先把表演进到 v2（制造 OCC 冲突）
        catalog
            .evolve_schema(yuntun_model::ops::EvolveSchemaRequest {
                table: "t".into(),
                change: yuntun_model::schema::SchemaChange::AddColumn {
                    field: Field::new("other", DataType::Int64, true),
                },
                expected_version: 1,
            })
            .await
            .unwrap();

        // 本地缓存停在 v1（schema 也是 v1 的）→ resolve 循环应 OCC 重试并成功
        let cache = SchemaCache::default();
        {
            let (_s, v) = catalog.table_schema("t").await.unwrap().unwrap();
            assert_eq!(v, 2);
        }
        cache.update("t", sch(&[("a", DataType::Int64)]), 1).await; // 过期版本 + 过期 schema
                                                                    // 传入含新列的 schema：基于过期 v1 判定 NeedsEvolve → OCC 冲突（actual=2）
                                                                    // → 拉取新 schema 重新判定 → Compatible → 返回 2（§5.2 OCC 重试循环）
        let incoming = sch(&[("a", DataType::Int64), ("other", DataType::Int64)]);
        let version = resolve_schema_version(&catalog, &cache, "t", &incoming)
            .await
            .unwrap();
        assert_eq!(version, 2);
    }

    #[tokio::test]
    async fn incompatible_rejected() {
        let catalog: Arc<dyn CatalogOps> = Arc::new(MemoryCatalog::new());
        catalog
            .create_table(CreateTableRequest {
                name: "t".into(),
                schema: sch(&[("a", DataType::Int64)]),
                partition_cols: vec![],
                default_format: "parquet".into(),
                ingest_config: Default::default(),
            })
            .await
            .unwrap();
        let cache = SchemaCache::default();
        // 数值 ↔ 字符串：拒绝（§8.1）
        let res =
            resolve_schema_version(&catalog, &cache, "t", &sch(&[("a", DataType::Utf8)])).await;
        assert!(matches!(res, Err(LakeError::SchemaIncompatible(_))));
    }
}
