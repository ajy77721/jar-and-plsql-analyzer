package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT (read-only, evidence-gathering) test file. Not a bug-fix.
 *
 * SQL Server's real equivalent of Oracle's TABLE(fn()) construct is a table-valued function
 * (TVF) used directly in FROM/JOIN, e.g. {@code SELECT * FROM dbo.MyTableFunc(@p) t JOIN ...}.
 * SQL Server has no equivalent of Oracle's old-style {@code (+)} outer-join syntax (confirmed:
 * grep for "(+)"/"outer_join_sign" across sqlserver-parser found zero matches) — that concept is
 * Oracle-only and is intentionally NOT tested here.
 *
 * Per source inspection, sqlserver-parser's grammar has no structured tableSource/fromClause/
 * joinClause AST at all (SelectStatement is a permissive token-eater); all table-op/call
 * extraction for SELECT is done via regex over the raw statement text
 * (SqlServerAnalysisVisitor.visitSelectStatement / extractQualifiedCalls).
 */
@DisplayName("AUDIT: SQL Server table-valued function (TVF) usage in FROM/JOIN")
public class SqlServerTableFunctionAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    @Test
    @DisplayName("AUDIT: SELECT ... FROM dbo.MyTableFunc(@p) t JOIN real_table r ON ...")
    void auditTableValuedFunctionJoinedToRealTable() {
        String sql = "CREATE PROCEDURE list_items\n" +
                "  @p INT\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT r.id, t.col1\n" +
                "  FROM dbo.MyTableFunc(@p) t\n" +
                "  JOIN orders r ON r.id = t.order_id;\n" +
                "END";

        SqlServerRoutineInfo r = parse(sql);
        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(),
                "AUDIT: should parse without errors: " + r.getErrors());

        List<String> tableNames = r.getTableOps().stream()
                .map(SqlServerTableOpInfo::getTableName)
                .filter(java.util.Objects::nonNull)
                .map(String::toUpperCase)
                .toList();

        List<String> callNames = r.getCalls().stream()
                .map(SqlServerCallInfo::getName)
                .filter(java.util.Objects::nonNull)
                .toList();

        System.out.println("[AUDIT] tableOps=" + tableNames + " calls=" + callNames
                + " errors=" + r.getErrors());

        boolean foundOrders = tableNames.contains("ORDERS");
        boolean foundCallToFunc = callNames.stream().anyMatch(n -> n.equalsIgnoreCase("MyTableFunc"));

        assertTrue(foundOrders, "AUDIT: real table ORDERS must not be dropped when joined to a TVF");
        // Per source inspection (extractQualifiedCalls comment: "Only INTO is excluded; FROM/JOIN
        // may precede legit table-valued-function calls"), MyTableFunc(...) is expected to be
        // captured as an outbound CALL (not a table op) by the QUALIFIED_CALL_PATTERN regex.
        assertTrue(foundCallToFunc,
                "AUDIT: dbo.MyTableFunc(...) should be recorded as a call when used as a TVF in FROM");
        // Bug fix: a TVF used as a FROM/JOIN source must not ALSO be recorded as a table op —
        // it is a call, not a table.
        assertFalse(tableNames.contains("MYTABLEFUNC"),
                "MyTableFunc must not be double-recorded as a phantom table op");
        assertEquals(1, tableNames.size(), "only the real table ORDERS should be a table op");
    }

    @Test
    @DisplayName("AUDIT: bare SELECT * FROM dbo.MyTableFunc(@p) t (no other real table)")
    void auditBareSelectFromTableValuedFunction() {
        String sql = "CREATE PROCEDURE list_items2\n" +
                "  @p INT\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM dbo.MyTableFunc(@p) t;\n" +
                "END";

        SqlServerRoutineInfo r = parse(sql);
        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(),
                "AUDIT: should parse without errors: " + r.getErrors());

        List<String> tableNames = r.getTableOps().stream()
                .map(SqlServerTableOpInfo::getTableName)
                .filter(java.util.Objects::nonNull)
                .toList();
        List<String> callNames = r.getCalls().stream()
                .map(SqlServerCallInfo::getName)
                .filter(java.util.Objects::nonNull)
                .toList();

        System.out.println("[AUDIT] bare select tableOps=" + tableNames + " calls=" + callNames);

        boolean foundCallToFunc = callNames.stream().anyMatch(n -> n.equalsIgnoreCase("MyTableFunc"));
        assertTrue(foundCallToFunc, "AUDIT: MyTableFunc should be recorded as a call for a bare TVF SELECT");
        boolean foundPhantomTable = tableNames.stream().anyMatch(n -> n.equalsIgnoreCase("MyTableFunc"));
        assertFalse(foundPhantomTable, "MyTableFunc must not be double-recorded as a phantom table op");
    }

    @Test
    @DisplayName("AUDIT: schema-qualified multi-part TVF name in JOIN, e.g. mydb.dbo.GetOrders(@id)")
    void auditSchemaQualifiedTvfJoin() {
        String sql = "CREATE PROCEDURE list_items3\n" +
                "  @id INT\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT c.name, t.total\n" +
                "  FROM customers c\n" +
                "  JOIN mydb.dbo.GetOrders(@id) t ON t.customer_id = c.id;\n" +
                "END";

        SqlServerRoutineInfo r = parse(sql);
        List<String> tableNames = r.getTableOps().stream()
                .map(SqlServerTableOpInfo::getTableName)
                .filter(java.util.Objects::nonNull)
                .map(String::toUpperCase)
                .toList();
        List<String> callNames = r.getCalls().stream()
                .map(SqlServerCallInfo::getName)
                .filter(java.util.Objects::nonNull)
                .toList();

        System.out.println("[AUDIT] schema-qualified TVF: tableOps=" + tableNames + " calls=" + callNames
                + " errors=" + r.getErrors());

        assertTrue(tableNames.contains("CUSTOMERS"), "AUDIT: CUSTOMERS real table must be present");
        boolean foundCallToFunc = callNames.stream().anyMatch(n -> n.equalsIgnoreCase("GetOrders"));
        assertTrue(foundCallToFunc, "AUDIT: mydb.dbo.GetOrders(...) should be recorded as a call");
        // Bug fix: the 3-part qualified TVF name must not leave a stray fragment table-op behind
        // (e.g. a fake table literally named DBO, from TABLE_REF_PATTERN's single-dot limit
        // stopping mid-name) and must not be double-recorded as a table op at all.
        assertFalse(tableNames.contains("DBO"), "no phantom DBO fragment table op");
        assertFalse(tableNames.contains("MYDB"), "no phantom MYDB fragment table op");
        assertFalse(tableNames.contains("GETORDERS"), "GetOrders must not be double-recorded as a table op");
        assertEquals(1, tableNames.size(), "only the real table CUSTOMERS should be a table op");
    }
}
