package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerCursorInfo;
import com.plsql.sqlserver.model.SqlServerDynamicSqlInfo;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import com.plsql.sqlserver.model.SqlServerTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Dynamic/string-based cursor + dynamic-SQL capture for sqlserver-parser
 * (§dynamic-cursor audit, dimension 1 + control #5).
 *
 * <p>2026-09-17 follow-up round: converted from a dump-and-print exploratory audit into hard
 * assertions once bugs 5, 6 and 8 (SQL Server portion) were fixed:
 * <ul>
 *   <li>Bug 5 — {@code DECLARE cur CURSOR FOR ...} severed from its own SELECT/EXEC body. Fixed by
 *       extending {@code SqlServerAnalysisVisitor#findNextSiblingStatement} (renamed from the
 *       prior-round "Bug 7" {@code findNextSiblingSelectStatement}) to also attach a sibling
 *       {@code EXEC(@sql)} statement's text, not just a sibling SELECT.</li>
 *   <li>Bug 6 — {@code FETCH NEXT FROM cur} mis-scanned as a table reference (cursor names are now
 *       excluded from the generic FROM/JOIN scan the same way CTE names already are).</li>
 *   <li>Bug 8 (SQL Server portion) — {@code SqlServerDynamicSqlInfo} now carries
 *       {@code unresolvedVarName} (mirroring {@code plsql-parser}'s {@code DynamicSqlInfo}),
 *       set when an {@code EXEC(@var)} / {@code EXEC sp_executesql @var} references a variable
 *       with no earlier literal {@code DECLARE}/{@code SET} assignment recorded in the routine.</li>
 * </ul>
 */
class SqlServerDynamicCursorAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private SqlServerCursorInfo cursor(SqlServerRoutineInfo r, String name) {
        return r.getCursors().stream()
                .filter(c -> name.equalsIgnoreCase(c.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("cursor " + name + " not found"));
    }

    private boolean hasTable(SqlServerRoutineInfo r, String name) {
        return r.getTableOps().stream()
                .anyMatch(t -> name.equalsIgnoreCase(t.getTableName()));
    }

    // 1. sp_executesql, fully literal @sql
    @Test
    @DisplayName("1: DECLARE @sql = literal SELECT; EXEC sp_executesql @sql")
    void spExecuteSql_literal() {
        String src =
                "CREATE PROCEDURE usp_dyn_literal\n" +
                "AS\nBEGIN\n" +
                "    DECLARE @sql NVARCHAR(MAX);\n" +
                "    DECLARE @p1 VARCHAR(10);\n" +
                "    SET @sql = 'SELECT * FROM orders WHERE status = @p1';\n" +
                "    SET @p1 = 'OPEN';\n" +
                "    EXEC sp_executesql @sql, N'@p1 VARCHAR(10)', @p1=@p1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        assertTrue(hasTable(r, "orders"), "literal SET_SQL should resolve orders table");
        List<SqlServerDynamicSqlInfo> dyn = r.getDynamicSql();
        SqlServerDynamicSqlInfo execEntry = dyn.stream()
                .filter(d -> d.getKind() == SqlServerDynamicSqlInfo.Kind.EXEC_SP_EXECUTESQL)
                .findFirst().orElseThrow();
        // Bug 8: @sql was assigned a literal earlier, so sp_executesql resolves it — not unresolved.
        assertNull(execEntry.getUnresolvedVarName());
        assertEquals("'SELECT * FROM orders WHERE status = @p1'", execEntry.getSqlSource());
    }

    // 2. sp_executesql, variable-interrupted (concatenated table name)
    @Test
    @DisplayName("2: DECLARE @sql = 'SELECT ... FROM ' + @table_name + ...; EXEC sp_executesql @sql")
    void spExecuteSql_variableInterrupted() {
        String src =
                "CREATE PROCEDURE usp_dyn_var\n" +
                "AS\nBEGIN\n" +
                "    DECLARE @sql NVARCHAR(MAX);\n" +
                "    DECLARE @table_name SYSNAME = 'orders';\n" +
                "    SET @sql = 'SELECT * FROM ' + @table_name + ' WHERE status = @p1';\n" +
                "    EXEC sp_executesql @sql, N'@p1 VARCHAR(10)', @p1='OPEN';\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        // The table name itself is runtime-concatenated, so no phantom table is invented.
        assertTrue(r.getTableOps().isEmpty());
        SqlServerDynamicSqlInfo execEntry = r.getDynamicSql().stream()
                .filter(d -> d.getKind() == SqlServerDynamicSqlInfo.Kind.EXEC_SP_EXECUTESQL)
                .findFirst().orElseThrow();
        // @sql itself WAS assigned (even though its value is a concatenation) so it resolves to
        // that assignment's text rather than being flagged unresolved.
        assertNull(execEntry.getUnresolvedVarName());
    }

    // 3a. EXEC(@var) — literal-only value set beforehand
    @Test
    @DisplayName("3a: EXEC(@dynamic_sql) where @dynamic_sql was set to a literal SELECT")
    void execString_literalVar() {
        String src =
                "CREATE PROCEDURE usp_exec_string_literal\n" +
                "AS\nBEGIN\n" +
                "    DECLARE @dynamic_sql NVARCHAR(MAX);\n" +
                "    SET @dynamic_sql = 'SELECT * FROM invoices';\n" +
                "    EXEC(@dynamic_sql);\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        assertTrue(hasTable(r, "invoices"));
        SqlServerDynamicSqlInfo execEntry = r.getDynamicSql().stream()
                .filter(d -> d.getKind() == SqlServerDynamicSqlInfo.Kind.EXEC_STRING)
                .findFirst().orElseThrow();
        assertNull(execEntry.getUnresolvedVarName());
        assertEquals("'SELECT * FROM invoices'", execEntry.getSqlSource());
    }

    // 3b. EXEC(@var) — concatenated table name
    @Test
    @DisplayName("3b: EXEC(@dynamic_sql) where @dynamic_sql was built via concatenation")
    void execString_concatenatedVar() {
        String src =
                "CREATE PROCEDURE usp_exec_string_concat\n" +
                "AS\nBEGIN\n" +
                "    DECLARE @dynamic_sql NVARCHAR(MAX);\n" +
                "    DECLARE @tbl SYSNAME = 'invoices';\n" +
                "    SET @dynamic_sql = 'SELECT * FROM ' + @tbl;\n" +
                "    EXEC(@dynamic_sql);\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        assertTrue(r.getTableOps().isEmpty());
        SqlServerDynamicSqlInfo execEntry = r.getDynamicSql().stream()
                .filter(d -> d.getKind() == SqlServerDynamicSqlInfo.Kind.EXEC_STRING)
                .findFirst().orElseThrow();
        assertNull(execEntry.getUnresolvedVarName());
    }

    // 3c. EXEC('literal') inline, no variable at all
    @Test
    @DisplayName("3c: EXEC('SELECT * FROM invoices') inline literal")
    void execString_inlineLiteral() {
        String src =
                "CREATE PROCEDURE usp_exec_string_inline\n" +
                "AS\nBEGIN\n" +
                "    EXEC('SELECT * FROM invoices');\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        assertTrue(hasTable(r, "invoices"));
    }

    // 3d (Bug 8, new): EXEC(@var) with no earlier assignment at all — genuinely unresolved.
    @Test
    @DisplayName("3d: EXEC(@sql) with no earlier DECLARE/SET literal — flagged unresolved")
    void execString_trulyUnresolvedVar() {
        String src =
                "CREATE PROCEDURE usp_exec_string_unresolved\n" +
                "AS\nBEGIN\n" +
                "    DECLARE @sql NVARCHAR(MAX);\n" +
                "    EXEC(@sql);\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        SqlServerDynamicSqlInfo execEntry = r.getDynamicSql().stream()
                .filter(d -> d.getKind() == SqlServerDynamicSqlInfo.Kind.EXEC_STRING)
                .findFirst().orElseThrow();
        assertEquals("@sql", execEntry.getUnresolvedVarName());
        assertNull(execEntry.getSqlSource());
    }

    // 4. DECLARE cursor CURSOR FOR EXEC(@sql) — dynamic-SQL-backed cursor
    @Test
    @DisplayName("4: DECLARE cur CURSOR FOR EXEC(@sql) — dynamic-SQL-backed cursor")
    void cursorOverDynamicSql() {
        String src =
                "CREATE PROCEDURE usp_cursor_dynamic\n" +
                "AS\nBEGIN\n" +
                "    DECLARE @sql NVARCHAR(MAX);\n" +
                "    SET @sql = 'SELECT * FROM orders WHERE status = ''OPEN''';\n" +
                "    DECLARE cur CURSOR FOR EXEC(@sql);\n" +
                "    OPEN cur;\n" +
                "    FETCH NEXT FROM cur;\n" +
                "    CLOSE cur;\n" +
                "    DEALLOCATE cur;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);

        // Bug 5: the cursor's query field must include the EXEC(@sql) body, not just "CURSOR FOR".
        SqlServerCursorInfo cur = cursor(r, "cur");
        assertTrue(cur.getQuery() != null && cur.getQuery().contains("EXEC"),
                "cursor query should include the EXEC(@sql) body: " + cur.getQuery());

        // Bug 6: "FETCH NEXT FROM cur" must not be mis-scanned as a table named "cur".
        assertFalse(hasTable(r, "cur"), "cursor name must not be captured as a table op");

        // The dynamic SQL behind the cursor is still resolved via the SET_SQL literal.
        assertTrue(hasTable(r, "orders"));

        // Lifecycle ops still tracked past DECLARE.
        List<String> ops = cur.getStmtOperations().stream().map(o -> o.getOperation()).toList();
        assertEquals(List.of("OPEN", "FETCH", "CLOSE", "DEALLOCATE"), ops);
    }

    // 5. Control: plain static cursor over a multi-table JOIN — does table capture happen at all?
    @Test
    @DisplayName("5 (control): DECLARE cur CURSOR FOR SELECT ... FROM a JOIN b — plain static cursor")
    void plainCursorControl() {
        String src =
                "CREATE PROCEDURE usp_cursor_plain\n" +
                "AS\nBEGIN\n" +
                "    DECLARE @OrderId INT, @CustName VARCHAR(100);\n" +
                "    DECLARE cur CURSOR FOR\n" +
                "        SELECT o.OrderId, c.Name\n" +
                "        FROM Orders o\n" +
                "        JOIN Customers c ON c.CustomerId = o.CustomerId;\n" +
                "    OPEN cur;\n" +
                "    FETCH NEXT FROM cur INTO @OrderId, @CustName;\n" +
                "    WHILE @@FETCH_STATUS = 0\n" +
                "    BEGIN\n" +
                "        FETCH NEXT FROM cur INTO @OrderId, @CustName;\n" +
                "    END\n" +
                "    CLOSE cur;\n" +
                "    DEALLOCATE cur;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);

        assertTrue(hasTable(r, "Orders"));
        assertTrue(hasTable(r, "Customers"));
        // Bug 6: cursor name must not itself show up as a table.
        assertFalse(hasTable(r, "cur"));

        SqlServerCursorInfo cur = cursor(r, "cur");
        assertTrue(cur.getQuery() != null && cur.getQuery().contains("SELECT")
                && cur.getQuery().contains("JOIN Customers"));
    }
}
