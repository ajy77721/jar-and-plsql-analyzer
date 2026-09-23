package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import com.plsql.sqlserver.model.SqlServerTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT ONLY — investigates two SQL-Server-specific concept areas against the real
 * SqlServerAnalysisEngine / SqlServerAnalysisVisitor (no mocks):
 *
 *  1. ISNULL/COALESCE interacting with (a) dynamic SQL built via string concatenation and
 *     (b) CASE WHEN expressions in a real (non-dynamic) query.
 *  2. GROUP BY extensions: legacy "GROUP BY a, b WITH ROLLUP" and SQL2008+
 *     "GROUP BY ROLLUP(...)/CUBE(...)/GROUPING SETS(...)" — whether the grammar supports
 *     them and whether FROM-clause table capture survives.
 *
 * Does NOT touch main/ code. Prints dump() evidence for each pattern; documents actual
 * behavior including any gaps via assertions/println. Failing assertions here are expected
 * and intentional where they document a real bug.
 */
class SqlServerNullHandlingAndGroupByAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private void dump(String label, SqlServerRoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.qualifiedTable() + "@" + t.getLine()
                        + " joins=" + t.getJoins().stream().map(j -> j.getJoinType() + " " + j.getJoinedTable()).toList())
                .toList());
        System.out.println("dynamicSql: " + r.getDynamicSql().stream()
                .map(d -> d.getKind() + "@" + d.getLine() + " src=" + d.getSqlSource()).toList());
        System.out.println("statements: " + r.getStatements().stream()
                .map(s -> s.getType() + "@" + s.getLine()).toList());
        System.out.println("errors: " + r.getErrors());
    }

    // ── Concept 1a: ISNULL inside a dynamic-SQL string built via concatenation ─────────────
    @Test
    @DisplayName("1a: ISNULL() argument inside dynamic SQL concatenation still finds base table")
    void isnull_insideDynamicSqlConcatenation() {
        String src =
                "CREATE PROCEDURE usp_dyn_isnull\n" +
                "AS\nBEGIN\n" +
                "    DECLARE @id INT = 1;\n" +
                "    DECLARE @sql VARCHAR(400);\n" +
                "    SET @sql = 'SELECT ISNULL(status, ''X'') FROM orders WHERE id = ' + CAST(@id AS VARCHAR);\n" +
                "    EXEC(@sql);\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("isnull_insideDynamicSqlConcatenation", r);

        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("orders captured = " + ordersCaptured);
        assertTrue(ordersCaptured, "expected 'orders' table extracted from dynamic SQL string despite "
                + "embedded ISNULL(...) call — actual tableOps: " + r.getTableOps());
        assertFalse(r.getDynamicSql().isEmpty(), "expected dynamic SQL to be flagged for EXEC(@sql)");
    }

    // ── Concept 1a-variant: same, but via EXEC sp_executesql ────────────────────────────────
    @Test
    @DisplayName("1a-variant: ISNULL() inside dynamic SQL via sp_executesql still finds base table")
    void isnull_insideDynamicSqlConcatenation_spExecuteSql() {
        String src =
                "CREATE PROCEDURE usp_dyn_isnull_sp\n" +
                "AS\nBEGIN\n" +
                "    DECLARE @id INT = 1;\n" +
                "    DECLARE @sql NVARCHAR(400);\n" +
                "    SET @sql = N'SELECT COALESCE(status, ''X'') FROM orders WHERE id = ' + CAST(@id AS NVARCHAR);\n" +
                "    EXEC sp_executesql @sql;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("isnull_insideDynamicSqlConcatenation_spExecuteSql", r);

        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("orders captured (sp_executesql/COALESCE) = " + ordersCaptured);
        assertTrue(ordersCaptured, "expected 'orders' table extracted from sp_executesql literal despite "
                + "embedded COALESCE(...) call — actual tableOps: " + r.getTableOps());
    }

    // ── Concept 1b: ISNULL/COALESCE nested inside CASE WHEN in a real (non-dynamic) query ──
    @Test
    @DisplayName("1b: ISNULL nested in CASE WHEN inside real SELECT still captures table")
    void isnull_nestedInCaseWhen_realSelect() {
        String src =
                "CREATE PROCEDURE usp_case_isnull\n" +
                "AS\nBEGIN\n" +
                "    SELECT o.OrderId,\n" +
                "        CASE WHEN ISNULL(o.a, o.b) = 1 THEN 1 ELSE 0 END AS Flag\n" +
                "    FROM orders o\n" +
                "    WHERE CASE WHEN ISNULL(o.a, o.b) = 1 THEN 1 ELSE 0 END = 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("isnull_nestedInCaseWhen_realSelect", r);

        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("orders captured (CASE+ISNULL) = " + ordersCaptured);
        assertTrue(ordersCaptured, "expected 'orders' table captured despite CASE WHEN ISNULL(...) nesting — "
                + "actual tableOps: " + r.getTableOps() + " errors: " + r.getErrors());
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors caused by CASE/ISNULL nesting: " + r.getErrors());
    }

    // ── Concept 1b-variant: COALESCE nested in CASE WHEN, in the SELECT list only ───────────
    @Test
    @DisplayName("1b-variant: COALESCE nested in CASE WHEN (SELECT-list only) still captures table")
    void coalesce_nestedInCaseWhen_selectListOnly() {
        String src =
                "CREATE PROCEDURE usp_case_coalesce\n" +
                "AS\nBEGIN\n" +
                "    SELECT o.OrderId,\n" +
                "        CASE WHEN COALESCE(o.a, o.b, 0) > 10 THEN 'HIGH' ELSE 'LOW' END AS Bucket\n" +
                "    FROM orders o;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("coalesce_nestedInCaseWhen_selectListOnly", r);

        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("orders captured (SELECT-list CASE+COALESCE) = " + ordersCaptured);
        assertTrue(ordersCaptured, "expected 'orders' table captured — actual tableOps: " + r.getTableOps()
                + " errors: " + r.getErrors());
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
    }

    // ── Concept 2a: legacy "GROUP BY a, b WITH ROLLUP" ──────────────────────────────────────
    @Test
    @DisplayName("2a: GROUP BY ... WITH ROLLUP (legacy) still captures FROM table")
    void groupBy_legacyWithRollup() {
        String src =
                "CREATE PROCEDURE usp_rollup_legacy\n" +
                "AS\nBEGIN\n" +
                "    SELECT o.Region, o.Rep, SUM(o.Amount)\n" +
                "    FROM orders o\n" +
                "    GROUP BY o.Region, o.Rep WITH ROLLUP;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("groupBy_legacyWithRollup", r);

        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("orders captured (GROUP BY ... WITH ROLLUP) = " + ordersCaptured);
        assertTrue(ordersCaptured, "expected 'orders' table captured despite legacy WITH ROLLUP — "
                + "actual tableOps: " + r.getTableOps() + " errors: " + r.getErrors());
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors from WITH ROLLUP: " + r.getErrors());
    }

    // ── Concept 2b: SQL2008+ "GROUP BY ROLLUP(a,b)" ─────────────────────────────────────────
    @Test
    @DisplayName("2b: GROUP BY ROLLUP(a,b) still captures FROM table")
    void groupBy_rollupFunctionSyntax() {
        String src =
                "CREATE PROCEDURE usp_rollup_fn\n" +
                "AS\nBEGIN\n" +
                "    SELECT o.Region, o.Rep, SUM(o.Amount)\n" +
                "    FROM orders o\n" +
                "    GROUP BY ROLLUP(o.Region, o.Rep);\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("groupBy_rollupFunctionSyntax", r);

        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("orders captured (GROUP BY ROLLUP(...)) = " + ordersCaptured);
        assertTrue(ordersCaptured, "expected 'orders' table captured despite GROUP BY ROLLUP(...) — "
                + "actual tableOps: " + r.getTableOps() + " errors: " + r.getErrors());
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors from GROUP BY ROLLUP(...): " + r.getErrors());
    }

    // ── Concept 2c: SQL2008+ "GROUP BY CUBE(a,b)" ───────────────────────────────────────────
    @Test
    @DisplayName("2c: GROUP BY CUBE(a,b) still captures FROM table")
    void groupBy_cubeFunctionSyntax() {
        String src =
                "CREATE PROCEDURE usp_cube_fn\n" +
                "AS\nBEGIN\n" +
                "    SELECT o.Region, o.Rep, SUM(o.Amount)\n" +
                "    FROM orders o\n" +
                "    GROUP BY CUBE(o.Region, o.Rep);\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("groupBy_cubeFunctionSyntax", r);

        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("orders captured (GROUP BY CUBE(...)) = " + ordersCaptured);
        assertTrue(ordersCaptured, "expected 'orders' table captured despite GROUP BY CUBE(...) — "
                + "actual tableOps: " + r.getTableOps() + " errors: " + r.getErrors());
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors from GROUP BY CUBE(...): " + r.getErrors());
    }

    // ── Concept 2d: SQL2008+ "GROUP BY GROUPING SETS(...)" ──────────────────────────────────
    @Test
    @DisplayName("2d: GROUP BY GROUPING SETS((a),(b),()) still captures FROM table")
    void groupBy_groupingSetsSyntax() {
        String src =
                "CREATE PROCEDURE usp_grpsets_fn\n" +
                "AS\nBEGIN\n" +
                "    SELECT o.Region, o.Rep, SUM(o.Amount)\n" +
                "    FROM orders o\n" +
                "    GROUP BY GROUPING SETS((o.Region), (o.Rep), ());\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("groupBy_groupingSetsSyntax", r);

        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("orders captured (GROUP BY GROUPING SETS(...)) = " + ordersCaptured);
        assertTrue(ordersCaptured, "expected 'orders' table captured despite GROUP BY GROUPING SETS(...) — "
                + "actual tableOps: " + r.getTableOps() + " errors: " + r.getErrors());
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors from GROUP BY GROUPING SETS(...): " + r.getErrors());
    }
}
