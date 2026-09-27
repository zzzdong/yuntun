//! **SQL 语句路由**（`plan.md` F.1 / `§142`）：把"不是查询"的语句从 DataFusion 手里接过来。
//!
//! # 为什么必须先有这一层
//!
//! DataFusion 只处理查询类语句；`ALTER TABLE` / `DELETE` / `UPDATE` 到它那儿只会得到一个
//! "不支持 / 解析"的错误。而 F.2（SQL 形式的 schema 变更）与 F.3（DELETE·UPDATE）**要**由我们
//! 自己接住 ⇒ 于是先立一个**分流层**：用 `sqlparser` 判定语句类型，非查询类交给我们的处理，
//! 查询类**原样**交给 DataFusion（`§132` 已确认：块级剪枝是它的优化器给的，别抢）。
//!
//! # 这一层做到哪（`F.2` 之后的情况）
//!
//! 被分流的语句一律返回**可读**的拒绝（不是假成功，也不是 DataFusion 那句不知所云的解析错误），
//! 但**两类拒绝说的不是同一件事**（见 [`StmtKind::rejection`]）：
//!
//! - **DDL 已在 SQL 层落地**（`F.2`，落点 `yuntun-sql::SqlEngine`）⇒ 这里说的是"本引擎
//!   不执行 DDL、不持写句柄"；
//! - **DML（`DELETE` / `UPDATE`）是真未支持**（`F.3`）。

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// 语句类型（只有"我们要接的"和"交给 DataFusion 的"两类之分）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StmtKind {
    /// 查询类（含 `EXPLAIN` / `SHOW` / `COPY`…）⇒ **原样**交给 DataFusion。
    Query,
    /// 以下六种由我们接管（当前尚未实现，见本模块文档）。
    AlterTable,
    CreateTable,
    DropTable,
    Delete,
    Update,
}

impl StmtKind {
    /// 由我们接管吗（`true` ⇒ 不进 DataFusion）。
    pub fn routed(&self) -> bool {
        !matches!(self, StmtKind::Query)
    }

    /// 给错误用的名字（运维看得懂的那种）。
    pub fn as_str(&self) -> &'static str {
        match self {
            StmtKind::Query => "QUERY",
            StmtKind::AlterTable => "ALTER TABLE",
            StmtKind::CreateTable => "CREATE TABLE",
            StmtKind::DropTable => "DROP TABLE",
            StmtKind::Delete => "DELETE",
            StmtKind::Update => "UPDATE",
        }
    }

    /// 被接管语句的**可读拒绝**（由 `sql_with_partial` 使用）。
    ///
    /// 两类必须分开说（它们是不同的事实，混成一句就会有一半是假话）：
    ///
    /// - **DDL**（`F.2` 已落地）：`ALTER TABLE` / `CREATE TABLE` / `DROP TABLE` 的实现在
    ///   **SQL 层**（`yuntun-sql::SqlEngine`）—— 那里有 SQL→Arrow 的类型映射、只读拒绝、
    ///   WAL DDL 追加与重放、缓存刷新。所以正确说法是"**本引擎不执行 DDL**"，而不是"尚未支持"；
    /// - **DML**（`F.3` 未做）：`DELETE` / `UPDATE` 才是真的尚未支持。
    pub fn rejection(&self) -> String {
        match self {
            StmtKind::AlterTable | StmtKind::CreateTable | StmtKind::DropTable => format!(
                "{} 由 SQL 层执行（`yuntun-sql::SqlEngine`，plan.md F.2）：\
                 QueryEngine 只执行查询、不持 Catalog 写句柄",
                self.as_str()
            ),
            StmtKind::Delete | StmtKind::Update => format!(
                "{} 已被识别、但**尚未支持**：见 plan.md F.3（DELETE / UPDATE）",
                self.as_str()
            ),
            StmtKind::Query => "QUERY 不需要拒绝（原样交给 DataFusion）".to_string(),
        }
    }
}

/// 判定一条 SQL 的类型；**解析失败**时返回可读的原因（不是 sqlparser 的原始报错）。
pub fn classify(sql: &str) -> Result<StmtKind, String> {
    let stmts = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|e| format!("SQL 解析失败：{e}"))?;
    // 多语句：只按**第一条**判定（其余的留给后续一刀的"多语句事务"语义，现在不猜）
    let Some(first) = stmts.into_iter().next() else {
        return Err("空语句".to_string());
    };
    Ok(match first {
        sqlparser::ast::Statement::Query(_)
        | sqlparser::ast::Statement::Explain { .. }
        | sqlparser::ast::Statement::ExplainTable { .. }
        | sqlparser::ast::Statement::ShowTables { .. }
        | sqlparser::ast::Statement::ShowColumns { .. }
        | sqlparser::ast::Statement::ShowCreate { .. }
        | sqlparser::ast::Statement::ShowVariable { .. }
        | sqlparser::ast::Statement::ShowFunctions { .. }
        | sqlparser::ast::Statement::Copy { .. } => StmtKind::Query,
        sqlparser::ast::Statement::AlterTable { .. } => StmtKind::AlterTable,
        sqlparser::ast::Statement::CreateTable { .. } => StmtKind::CreateTable,
        sqlparser::ast::Statement::Drop { .. } => StmtKind::DropTable,
        sqlparser::ast::Statement::Delete { .. } => StmtKind::Delete,
        sqlparser::ast::Statement::Update { .. } => StmtKind::Update,
        // 其它形状（`INSERT` 等）：**不猜** ⇒ 交回 DataFusion，让它给出它自己的结论
        _ => StmtKind::Query,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queries_pass_through() {
        for q in [
            "SELECT 1",
            "SELECT count(*) FROM generate_series(1, 10)",
            "EXPLAIN SELECT 1",
            "SHOW TABLES",
        ] {
            assert_eq!(classify(q).unwrap(), StmtKind::Query, "{q} 应当原样交给 DataFusion");
            assert!(!classify(q).unwrap().routed(), "{q} 不该被分流");
        }
    }

    /// 六类由我们接管的语句都要被**认出来**（认出来才会进我们的处理，而不是 DataFusion）。
    #[test]
    fn routed_statements_are_recognized() {
        let cases = [
            ("ALTER TABLE t ADD COLUMN c INT", StmtKind::AlterTable),
            ("ALTER TABLE t DROP COLUMN c", StmtKind::AlterTable),
            ("CREATE TABLE t (a INT)", StmtKind::CreateTable),
            ("DROP TABLE t", StmtKind::DropTable),
            ("DELETE FROM t WHERE a = 1", StmtKind::Delete),
            ("UPDATE t SET a = 2 WHERE a = 1", StmtKind::Update),
        ];
        for (sql, want) in cases {
            let got = classify(sql).unwrap_or_else(|e| panic!("{sql} 解析失败：{e}"));
            assert_eq!(got, want, "{sql} 应当被认成 {want:?}");
            assert!(got.routed(), "{sql} 必须由我们接管");
        }
    }

    /// 解析失败给**可读**的原因（不是 sqlparser 的原始错误对象）。
    #[test]
    fn parse_errors_are_readable() {
        let e = classify("NOT SQL AT ALL ;;;").unwrap_err();
        assert!(e.starts_with("SQL 解析失败"), "应当是可读的原因，实际：{e}");
    }
}
