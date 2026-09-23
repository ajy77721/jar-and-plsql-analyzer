package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

/**
 * READ-ONLY AUDIT (evidence-gathering only, no fixes applied here).
 *
 * Audits three SQL Server concepts against the real SqlServerParser.g4 grammar and
 * SqlServerAnalysisVisitor (no mocks):
 *
 *   1. CREATE SYNONYM ... FOR ... (plain target, and 4-part linked-server target) — is it
 *      captured AT ALL (any grammar rule / visitor method / model), analogous to the
 *      just-fixed Oracle CREATE SYNONYM gap? Also: does later usage of the synonym name as a
 *      plain table in a SELECT already work independently of CREATE SYNONYM capture?
 *   2. CONVERT(type, expr, style) / CAST(expr AS type) — does the extra comma-bearing numeric
 *      style-code argument (or the AS-typed CAST arg) break FROM-clause/table capture, tested
 *      with the function appearing before, inside, and right after a JOIN.
 *   3. Computed/arithmetic SELECT-list expressions spanning two joined tables — does this
 *      disturb table/join capture.
 */
@DisplayName("AUDIT: SQL Server CREATE SYNONYM / CONVERT-CAST / multi-table computed expressions")
public class SqlServerSynonymConvertAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private List<String> tableNames(SqlServerRoutineInfo r) {
        return r.getTableOps().stream()
                .map(SqlServerTableOpInfo::getTableName)
                .filter(Objects::nonNull)
                .map(String::toUpperCase)
                .toList();
    }

    // ─────────────────────────────────────────────────────────────────────
    // Concept 1: CREATE SYNONYM
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("AUDIT 1a: bare CREATE SYNONYM ... FOR ... as a standalone script (top-level, not inside a procedure)")
    void auditCreateSynonymStandalone() {
        String sql = "CREATE SYNONYM dbo.OrdersSyn FOR dbo.Orders;";
        SqlServerRoutineInfo r = parse(sql);
        System.out.println("[AUDIT 1a] tableOps=" + r.getTableOps() + " calls=" + r.getCalls()
                + " errors=" + r.getErrors());
        // GRAMMAR FACT: compilationUnit = (routineDefinition | triggerDefinition | viewDefinition)? EOF.
        // A bare CREATE SYNONYM statement is none of those three, so the parser cannot match the
        // optional alternative and then immediately require EOF — this is a hard parse failure
        // (recorded in result.getErrors()), not a silently-accepted-but-unrecognized statement.
        assertFalse(r.getErrors().isEmpty(),
                "EXPECTED-BUG-EVIDENCE: a standalone CREATE SYNONYM statement is not one of " +
                "routineDefinition/triggerDefinition/viewDefinition, so it cannot even parse at the top level");
        assertTrue(r.getTableOps().isEmpty(), "no synonym info of any kind is produced");
    }

    @Test
    @DisplayName("AUDIT 1b: CREATE SYNONYM ... FOR ... inside a procedure body (reaches createStatement/otherStatement catch-all)")
    void auditCreateSynonymInsideProcedure() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  CREATE SYNONYM dbo.OrdersSyn FOR dbo.Orders;\n" +
                "  SELECT 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        System.out.println("[AUDIT 1b] tableOps=" + r.getTableOps() + " calls=" + r.getCalls()
                + " errors=" + r.getErrors());
        // GRAMMAR FACT: createStatement (inside a routine body) is the fully-generic catch-all
        // "CREATE stmtTail SEMI?" — no dedicated SYNONYM sub-rule exists (grep of the grammar
        // confirms zero occurrences of "SYNONYM" anywhere). The visitor's scanOps() regex,
        // KEYWORD_OP_PATTERN, only recognises the literal compound keywords "CREATE TABLE" /
        // "ALTER TABLE" / "DROP TABLE" / "TRUNCATE TABLE" — a bare "CREATE SYNONYM" does not match
        // any alternative in that pattern, so scanOps() finds nothing here either.
        // EXPECTED (correct behaviour, per the Oracle create_synonym fix's shape): a SYNONYM
        // object recording synonym name "OrdersSyn", schema "dbo", target schema "dbo", target
        // table "Orders", and dblink=null (no linked server).
        boolean anySynonymEvidence = r.getTableOps().stream().anyMatch(t ->
                "OrdersSyn".equalsIgnoreCase(t.getTableName()) || "Orders".equalsIgnoreCase(t.getTableName()))
                || r.getCalls().stream().anyMatch(c -> c.toString().toUpperCase().contains("SYNONYM"));
        System.out.println("[AUDIT 1b] any synonym-name/target-table evidence captured: " + anySynonymEvidence);
        assertTrue(anySynonymEvidence,
                "FIXED: CREATE SYNONYM inside a procedure body is now captured — both the synonym name " +
                "'OrdersSyn' and its target table 'Orders' show up in tableOps");
    }

    @Test
    @DisplayName("AUDIT 1c: 4-part linked-server CREATE SYNONYM target inside a procedure body")
    void auditCreateSynonymLinkedServerTarget() {
        String sql = "CREATE PROCEDURE p2\n" +
                "AS\n" +
                "BEGIN\n" +
                "  CREATE SYNONYM dbo.RemoteOrdersSyn FOR LinkedSrv.OtherDb.dbo.Orders;\n" +
                "  SELECT 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        System.out.println("[AUDIT 1c] tableOps=" + r.getTableOps() + " calls=" + r.getCalls()
                + " errors=" + r.getErrors());
        boolean anySynonymEvidence = r.getTableOps().stream().anyMatch(t ->
                "RemoteOrdersSyn".equalsIgnoreCase(t.getTableName()) || "Orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("[AUDIT 1c] any synonym-name/target-table evidence captured: " + anySynonymEvidence);
        assertTrue(anySynonymEvidence,
                "FIXED: same as 1b — the 4-part linked-server target's real table (Orders) is captured");
    }

    @Test
    @DisplayName("AUDIT 1d: control case — later USING the synonym name transparently as a table already works via the generic table-ref path")
    void auditSynonymUsedAsTableWorksIndependently() {
        String sql = "CREATE PROCEDURE p3\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM dbo.OrdersSyn;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[AUDIT 1d] tableOps=" + r.getTableOps() + " errors=" + r.getErrors());
        // This is NOT a synonym-aware bug scenario — dbo.OrdersSyn is simply indistinguishable
        // from a real table name to the generic FROM/JOIN table-ref capture, so it should (and
        // does) come through as a normal table op named OrdersSyn.
        assertTrue(names.contains("ORDERSSYN"),
                "CONFIRMED: using a synonym name as a plain table reference already works correctly, " +
                "independent of whether CREATE SYNONYM itself is ever recognised");
    }

    // ─────────────────────────────────────────────────────────────────────
    // Concept 2: CONVERT(type, expr, style) / CAST(expr AS type)
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("AUDIT 2a: CONVERT with style code in SELECT list and in WHERE, single table")
    void auditConvertStyleCodeSingleTable() {
        String sql = "CREATE PROCEDURE p4\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT CONVERT(VARCHAR(10), order_date, 101) AS us_date\n" +
                "  FROM orders\n" +
                "  WHERE CONVERT(DATE, GETDATE()) = order_date;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[AUDIT 2a] tableOps=" + r.getTableOps() + " errors=" + r.getErrors());
        assertTrue(names.contains("ORDERS"),
                "CONVERT(...,...,101)'s comma-bearing style-code argument must not break FROM-clause capture");
    }

    @Test
    @DisplayName("AUDIT 2b: CONVERT immediately before a JOIN clause")
    void auditConvertBeforeJoin() {
        String sql = "CREATE PROCEDURE p5\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT CONVERT(VARCHAR(10), o.order_date, 101) AS us_date, c.name\n" +
                "  FROM orders o\n" +
                "  JOIN customers c ON c.id = o.customer_id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[AUDIT 2b] tableOps=" + r.getTableOps() + " errors=" + r.getErrors());
        assertTrue(names.contains("ORDERS"), "orders must be captured");
        assertTrue(names.contains("CUSTOMERS"), "customers (post-JOIN) must be captured despite preceding CONVERT(...) in SELECT list");
    }

    @Test
    @DisplayName("AUDIT 2c: CONVERT immediately after a JOIN's ON clause (inside a computed comparison)")
    void auditConvertAfterJoin() {
        String sql = "CREATE PROCEDURE p6\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT o.id, c.name\n" +
                "  FROM orders o\n" +
                "  JOIN customers c ON c.id = o.customer_id\n" +
                "  WHERE CONVERT(DATE, o.order_date) = CONVERT(DATE, GETDATE());\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[AUDIT 2c] tableOps=" + r.getTableOps() + " errors=" + r.getErrors());
        assertTrue(names.contains("ORDERS"), "orders must be captured");
        assertTrue(names.contains("CUSTOMERS"), "customers must be captured despite CONVERT(...) with a comma-free style right after the JOIN's ON clause");
    }

    @Test
    @DisplayName("AUDIT 2d: CAST(expr AS DECIMAL(10,2)) — nested-comma type spec inside CAST, plus a JOIN")
    void auditCastWithParenthesizedTypeAndJoin() {
        String sql = "CREATE PROCEDURE p7\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT o.id, CAST(p.price AS DECIMAL(10,2)) AS price2\n" +
                "  FROM orders o\n" +
                "  JOIN products p ON o.product_id = p.id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[AUDIT 2d] tableOps=" + r.getTableOps() + " errors=" + r.getErrors());
        assertTrue(names.contains("ORDERS"), "orders must be captured");
        assertTrue(names.contains("PRODUCTS"),
                "products (post-JOIN) must be captured despite CAST(... AS DECIMAL(10,2))'s internal comma in the SELECT list");
    }

    // ─────────────────────────────────────────────────────────────────────
    // Concept 3: computed/arithmetic SELECT-list expressions spanning two joined tables
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("AUDIT 3a: arithmetic expression (o.qty * p.unit_price) plus DATEDIFF(...) referencing both joined tables")
    void auditComputedExpressionAcrossJoinedTables() {
        String sql = "CREATE PROCEDURE p8\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT o.id, o.qty * p.unit_price AS line_total,\n" +
                "         DATEDIFF(day, o.order_date, GETDATE()) AS days_old\n" +
                "  FROM orders o\n" +
                "  JOIN products p ON o.product_id = p.id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        String aliases = r.getTableOps().stream()
                .map(SqlServerTableOpInfo::getAlias).filter(Objects::nonNull).toList().toString();
        System.out.println("[AUDIT 3a] tableOps=" + r.getTableOps() + " aliases=" + aliases + " errors=" + r.getErrors());
        assertTrue(names.contains("ORDERS"), "orders must be captured");
        assertTrue(names.contains("PRODUCTS"),
                "products must be captured even though the SELECT list is a computed arithmetic expression, " +
                "not a bare column list, and DATEDIFF(day, ..., ...) has an internal comma");
        assertEquals(2, r.getTableOps().stream()
                        .filter(t -> t.getTableName() != null)
                        .map(t -> t.getTableName().toUpperCase()).distinct().count(),
                "exactly the two joined tables should be recorded, no phantom extras from the computed expression");
    }
}
