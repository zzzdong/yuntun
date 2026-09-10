import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * DBeaver / JDBC 元数据探测（sql-access-design.md §5.4 **T3** 的脚本化替代）。
 *
 * 用 DBeaver 自带的 MySQL Connector/J 驱动，按 DBeaver「连接测试 / 表浏览 /
 * 列浏览 / 数据预览」的实际调用序列（DatabaseMetaData + 典型 SQL）逐项验证；
 * 任一 FAILED 即 shim 缺口（多数是结果集**列名**与 MySQL 不一致）。
 *
 * 编译运行（jar 路径取本机 DBeaver 已下载的驱动，或用 ~/.m2 里的）：
 *
 * <pre>
 *   JAR=~/.local/share/DBeaverData/drivers/maven/maven-central/mysql/mysql-connector-java-8.0.29.jar
 *   javac -cp $JAR scripts/dbeaver_jdbc_probe.java
 *   java -cp "$JAR:scripts" dbeaver_jdbc_probe [jdbc-url]
 * </pre>
 *
 * 默认 URL 与 DBeaver 建连一致：useServerPrepStmts=false（opensrv-mysql 0.7
 * 无二进制结果集编码，见 operation-log §14.3）、useSSL=false、trust 鉴权。
 */
public class dbeaver_jdbc_probe {
    public static void main(String[] args) throws Exception {
        String url = args.length > 0
                ? args[0]
                : "jdbc:mysql://127.0.0.1:3306/public"
                        + "?useServerPrepStmts=false&useSSL=false&allowPublicKeyRetrieval=true";
        try (Connection c = DriverManager.getConnection(url, "yuntun", "")) {
            DatabaseMetaData md = c.getMetaData();
            System.out.println("[1] " + md.getDatabaseProductName() + " "
                    + md.getDatabaseProductVersion()
                    + " | driver " + md.getDriverName() + " " + md.getDriverVersion());

            meta("[2] catalogs", () -> md.getCatalogs());
            meta("[3] schemas", () -> md.getSchemas());
            meta("[4] table types", () -> md.getTableTypes());
            meta("[5] tables", () -> md.getTables("public", null, "%", new String[] {"TABLE"}));
            meta("[6] columns(api_audit)", () -> md.getColumns("public", null, "api_audit", "%"));
            meta("[7] primary keys", () -> md.getPrimaryKeys("public", null, "api_audit"));
            meta("[8] index info", () -> md.getIndexInfo("public", null, "api_audit", false, false));
            meta("[9] type info", () -> md.getTypeInfo());

            // 数据预览（DBeaver 表格页）：LIMIT 查询 + 计数 + 聚合
            q(c, "[10] preview", "SELECT * FROM api_audit LIMIT 10");
            q(c, "[11] count", "SELECT count(*) AS c FROM api_audit");
            q(c, "[12] group by",
                    "SELECT user, count(*) AS cnt FROM api_audit GROUP BY user ORDER BY cnt DESC");
            // DBeaver / 驱动常见探测语句
            q(c, "[13] version comment", "SELECT @@version_comment");
            q(c, "[14] database()", "SELECT DATABASE()");
            q(c, "[15] information_schema.tables",
                    "SELECT table_schema, table_name FROM information_schema.tables LIMIT 5");
            q(c, "[16] show full tables", "SHOW FULL TABLES");
            q(c, "[17] describe", "DESCRIBE api_audit");
            q(c, "[18] show collation", "SHOW COLLATION");
            q(c, "[19] show charset", "SHOW CHARSET");
            q(c, "[20] show engines", "SHOW ENGINES");
            q(c, "[21] show keys", "SHOW KEYS FROM api_audit");
            q(c, "[22] show create table", "SHOW CREATE TABLE api_audit");
            // DBeaver 浏览表/列时必查、而 DataFusion information_schema 未提供的元数据表
            q(c, "[23] is.key_column_usage", "SELECT * FROM information_schema.key_column_usage");
            q(c, "[24] is.referential_constraints", "SELECT * FROM information_schema.referential_constraints");
            q(c, "[25] is.triggers", "SELECT * FROM information_schema.triggers");
            q(c, "[26] is.statistics", "SELECT * FROM information_schema.statistics");
            q(c, "[27] is.partitions", "SELECT * FROM information_schema.partitions");
            q(c, "[28] is.table_constraints", "SELECT * FROM information_schema.table_constraints");
        }
    }

    interface RsSupplier {
        ResultSet get() throws SQLException;
    }

    static void meta(String label, RsSupplier s) {
        try (ResultSet rs = s.get()) {
            printRs(label, rs, 5);
        } catch (Exception e) {
            System.out.println(label + " FAILED: " + e.getMessage());
        }
    }

    static void q(Connection c, String label, String sql) {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            printRs(label + " [" + sql + "]", rs, 5);
        } catch (Exception e) {
            System.out.println(label + " FAILED [" + sql + "]: " + e.getMessage());
        }
    }

    static void printRs(String label, ResultSet rs, int maxRows) throws SQLException {
        ResultSetMetaData m = rs.getMetaData();
        StringBuilder head = new StringBuilder();
        for (int i = 1; i <= m.getColumnCount(); i++) {
            head.append(m.getColumnName(i)).append('|');
        }
        StringBuilder body = new StringBuilder();
        int n = 0;
        while (rs.next() && n < maxRows) {
            for (int i = 1; i <= m.getColumnCount(); i++) {
                body.append(rs.getString(i)).append(',');
            }
            body.append("; ");
            n++;
        }
        System.out.println(label + " -> cols[" + head + "] rows(" + n + "): " + body);
    }
}
