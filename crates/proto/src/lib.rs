//! `yuntun-proto` —— metanode 的 gRPC 面（S3-0）。
//!
//! 契约见 `crates/proto/proto/meta.proto` 与 `docs/metanode-design.md §5`。
//!
//! # 本 crate 的定位
//!
//! 只放**接口**（生成代码 + 少量约定），不含业务逻辑 —— 于是它可以被 server /
//! client / metanode 三方共享，而不会把 `tonic` 拖进查询或写入路径。
//!
//! # 与 `yuntun-model` 的关系（**迁移中**）
//!
//! `yuntun-model` 里的 prost 结构是**手写**的（阶段 0 的历史选择），它们现在仍是
//! 载荷语义的定义处；本文件的 `Op` 以 `bytes` 携带这些结构的编码，先冻结 RPC 面。
//! 逐字段展开成 proto 类型是后续增量（设计 §6 的回滚点即「双份并存」）。
//! `crates/proto/tests/wire_compat.rs` 就是这条边界的**守卫**：每加一种 op，
//! 都要有「载荷往返后逐字段不变」的用例。

/// metanode 的 gRPC 面（`proto/meta.proto`，package `yuntun.meta.v1`）。
pub mod meta {
    tonic::include_proto!("yuntun.meta.v1");
}

/// 本 crate 与 proto 包的版本声明（放进 `StatusResponse.version`，便于排查版本错配）。
pub const PROTO_VERSION: &str = "yuntun.meta.v1";
