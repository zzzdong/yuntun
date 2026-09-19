//! metanode 的错误类型与 **gRPC 错误码映射**（`metanode-design.md §5` 约定 3）。
//!
//! # 为什么单列一层错误
//!
//! 状态机（`CatalogState`）返回的是 [`LakeError`]（业务语义），gRPC 层要的是状态码 +
//! **可重试性**。两者之间必须有一处**显式**的映射，否则每个 RPC 方法各写各的 `match`，
//! 迟早出现"某个错误被映射成不可重试"—— 而设计里 G2（写入不中断）**依赖**客户端能重试。
//!
//! 三条硬要求（来自约定 3）：
//!
//! | 情形 | 状态码 | 可重试 |
//! |---|---|---|
//! | 非 leader | `UNAVAILABLE` + **leader hint**（metadata） | ✅ 必须可重试 |
//! | 无 quorum | `UNAVAILABLE` | ✅ |
//! | OCC 版本不符（并发 DDL） | `FAILED_PRECONDITION` + 实际版本（metadata） | ✅（拉新版本重试） |
//! | 请求本身不合法（op 解不开/键缺失） | `INVALID_ARGUMENT` | ❌ |
//! | 存储故障（落盘失败等） | `INTERNAL` | ❌（节点应停机） |

use yuntun_model::error::LakeError;

/// metanode 的错误。
///
/// 刻意**不**实现 `PartialEq`：`LakeError` 没实现（它含 `Arc<Schema>` 等），
/// 而靠 `matches!` 断言更精确（断言的是"哪一类"，不是"整个相等"）。
#[derive(Debug, Clone)]
pub enum MetaError {
    /// 本节点不是 leader。`leader_hint` 用于让客户端**直接重试到正确节点**
    /// （0 = 未知，客户端可轮询其他节点）。
    NotLeader { leader_hint: u64 },
    /// 暂时无法达成多数派（网络分区 / 多数节点不可用）。
    NoQuorum,
    /// OCC 失败：客户端读到的版本已过期。
    OccConflict { actual_version: u64 },
    /// 请求不合法（op 解不开、幂等键缺失等）—— **不可重试**。
    BadRequest(String),
    /// 状态机返回的业务错误（保留原类型，映射时才决定状态码）。
    Lake(LakeError),
    /// 存储/内部故障：**节点应停机**，不该让客户端重试（避免"带病继续"）。
    Storage(String),
}

impl MetaError {
    /// 由状态机错误构造（业务语义保持原样，映射留给 [`Self::to_status`]）。
    pub fn from_lake(e: LakeError) -> Self {
        MetaError::Lake(e)
    }

    /// 是否值得客户端重试（另一节点/稍后可能成功）。
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            MetaError::NotLeader { .. } | MetaError::NoQuorum | MetaError::OccConflict { .. }
        ) || matches!(&self, MetaError::Lake(e) if matches!(e, LakeError::ResourceExhausted(_)))
    }
}

impl std::fmt::Display for MetaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetaError::NotLeader { leader_hint } => write!(f, "not leader (hint={leader_hint})"),
            MetaError::NoQuorum => write!(f, "no quorum"),
            MetaError::OccConflict { actual_version } => {
                write!(f, "occ conflict (actual version {actual_version})")
            }
            MetaError::BadRequest(m) => write!(f, "bad request: {m}"),
            MetaError::Lake(e) => write!(f, "{e}"),
            MetaError::Storage(m) => write!(f, "storage failure: {m}"),
        }
    }
}

impl std::error::Error for MetaError {}

impl From<MetaError> for tonic::Status {
    fn from(e: MetaError) -> Self {
        // 提示信息放进 metadata（客户端可编程读取），而不是只塞在 message 里
        let with_hint = |mut s: tonic::Status, key: &'static str, val: String| {
            if let Ok(v) = val.parse() {
                s.metadata_mut().insert(key, v);
            }
            s
        };
        match e {
            MetaError::NotLeader { leader_hint } => with_hint(
                tonic::Status::unavailable(format!("not leader (leader hint {leader_hint})")),
                "leader-hint",
                leader_hint.to_string(),
            ),
            MetaError::NoQuorum => tonic::Status::unavailable("no quorum"),
            MetaError::OccConflict { actual_version } => with_hint(
                tonic::Status::failed_precondition(format!(
                    "schema version changed (actual {actual_version})"
                )),
                "actual-version",
                actual_version.to_string(),
            ),
            MetaError::BadRequest(m) => tonic::Status::invalid_argument(m),
            MetaError::Storage(m) => tonic::Status::internal(m),
            MetaError::Lake(e) => match e {
                LakeError::TableNotFound(t) => tonic::Status::not_found(format!("table {t}")),
                LakeError::SchemaNotFound(s) => tonic::Status::not_found(format!("schema {s}")),
                LakeError::TableAlreadyExists(t) => {
                    tonic::Status::already_exists(format!("table {t}"))
                }
                LakeError::SchemaAlreadyExists(s) => {
                    tonic::Status::already_exists(format!("schema {s}"))
                }
                LakeError::SchemaNotEmpty(s) => {
                    tonic::Status::failed_precondition(format!("schema {s} is not empty"))
                }
                LakeError::SchemaIncompatible(m) | LakeError::InvalidSchemaChange(m) => {
                    tonic::Status::failed_precondition(m)
                }
                LakeError::SchemaChanged { actual_version, .. } => with_hint(
                    tonic::Status::failed_precondition(format!(
                        "schema changed, retry (actual {actual_version})"
                    )),
                    "actual-version",
                    actual_version.to_string(),
                ),
                LakeError::IdempotencyKeyRequired => {
                    tonic::Status::invalid_argument("idempotency key required")
                }
                LakeError::IdempotencyKeyTooLong => {
                    tonic::Status::invalid_argument("idempotency key too long (max 256)")
                }
                LakeError::ResourceExhausted(m) => {
                    // 背压：明确拒绝 + 可重试（客户端应退避）
                    tonic::Status::resource_exhausted(m)
                }
                other => tonic::Status::internal(other.to_string()),
            },
        }
    }
}

/// metanode **启动**路径的错误。
///
/// 与 [`MetaError`] 分开：那些是"处理请求失败"（有的可重试），这些是"**起不来**"
/// （配置/盘的问题，重试多少次都一样）。混在一起会诱导调用方去重试配置错误。
#[derive(Debug, Clone)]
pub enum MetaNodeError {
    /// 存储打不开：目录权限、被别的进程占着（fjall 有目录锁）、快照损坏…
    Storage(String),
    /// 成员表里不止一个节点。
    ///
    /// 多节点复制要走**网络传输**（下一步）；现在起起来各节点互相发不出消息，
    /// 只会静默空转（永远选不出 leader），所以明确拒绝而不是"先跑着看看"。
    MultiNodeUnsupported { voters: Vec<u64> },
    /// 盘上的成员表与本次配置不一致。
    MembershipMismatch { stored: Vec<u64>, requested: Vec<u64> },
}

impl std::fmt::Display for MetaNodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetaNodeError::Storage(m) => write!(f, "存储不可用：{m}"),
            MetaNodeError::MultiNodeUnsupported { voters } => write!(
                f,
                "成员表 {voters:?} 不止一个节点：多节点 raft 复制需要网络传输（下一步），\
                 现在启动会各说各话（永远选不出 leader）。单节点请用 --voters <自己的 id>。"
            ),
            MetaNodeError::MembershipMismatch { stored, requested } => write!(
                f,
                "盘上成员表是 {stored:?}，本次配置是 {requested:?}。\n\
                 成员表是持久化状态，不随启动参数改变：要改成员请走成员变更（S3-6）；\
                 确认这个目录属于**另一个集群**的话，请换 --dir 或手动清空该目录。"
            ),
        }
    }
}

impl std::error::Error for MetaNodeError {}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[test]
    fn not_leader_is_unavailable_and_carries_hint() {
        let s: tonic::Status = MetaError::NotLeader { leader_hint: 3 }.into();
        assert_eq!(s.code(), Code::Unavailable, "非 leader 必须可重试");
        assert_eq!(
            s.metadata().get("leader-hint").unwrap(),
            "3",
            "必须带 leader hint（客户端据此重试到正确节点）"
        );
        assert!(MetaError::NotLeader { leader_hint: 3 }.retryable());
    }

    #[test]
    fn occ_conflict_is_failed_precondition_with_actual_version() {
        let e = MetaError::OccConflict { actual_version: 7 };
        let s: tonic::Status = e.clone().into();
        assert_eq!(s.code(), Code::FailedPrecondition);
        assert_eq!(s.metadata().get("actual-version").unwrap(), "7");
        assert!(e.retryable(), "OCC 冲突要能重试（拉新版本再判）");
    }

    #[test]
    fn client_errors_are_not_retryable() {
        for e in [
            MetaError::BadRequest("x".into()),
            MetaError::Lake(LakeError::TableNotFound("t".into())),
            MetaError::Lake(LakeError::IdempotencyKeyRequired),
        ] {
            assert!(!e.retryable(), "{e} 不该被标成可重试");
        }
        assert_eq!(
            tonic::Status::from(MetaError::Lake(LakeError::TableNotFound("t".into()))).code(),
            Code::NotFound
        );
        assert_eq!(
            tonic::Status::from(MetaError::Lake(LakeError::TableAlreadyExists("t".into()))).code(),
            Code::AlreadyExists
        );
    }

    #[test]
    fn backpressure_is_resource_exhausted_and_retryable() {
        let e = MetaError::Lake(LakeError::ResourceExhausted("disk 0.9".into()));
        assert!(e.retryable(), "背压要可重试（退避后重试）");
        assert_eq!(tonic::Status::from(e).code(), Code::ResourceExhausted);
    }

    #[test]
    fn storage_failure_is_internal_and_not_retryable() {
        let e = MetaError::Storage("fjall write failed".into());
        assert!(!e.retryable(), "落盘失败不该让客户端重试（节点要停机）");
        assert_eq!(tonic::Status::from(e).code(), Code::Internal);
    }

    /// 每个错误都必须有**明确**的状态码（这条防"新加错误忘了映射"，因为 `match` 是穷尽的）。
    #[test]
    fn every_error_has_a_code() {
        let cases = vec![
            MetaError::NoQuorum,
            MetaError::Lake(LakeError::SchemaNotEmpty("s".into())),
            MetaError::Lake(LakeError::SchemaChanged {
                actual_version: 2,
                new_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
            }),
            MetaError::Lake(LakeError::S3("x".into())),
        ];
        for e in cases {
            let code = tonic::Status::from(e.clone()).code();
            assert_ne!(code, Code::Ok, "{e} 不能映射成 Ok");
        }
    }
}
