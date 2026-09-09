//! 【v13 过渡态】SQL 前置解析纯函数已迁入 `yuntun-sql` 能力 crate
//! （sql-access-design.md §三 S-1/§四）。
//!
//! 本模块仅为兼容过渡：重导出 yuntun-sql 的纯函数，`flight.rs` 的 run_sql
//! 分流仍走旧路径。**待办**（operation-log §11 W-1）：FlightServer 改调
//! `yuntun_sql::SqlEngine`（execute/prepare/元数据 API），删除本文件与
//! flight.rs 内嵌分流逻辑。
pub use yuntun_sql::sql::*;
