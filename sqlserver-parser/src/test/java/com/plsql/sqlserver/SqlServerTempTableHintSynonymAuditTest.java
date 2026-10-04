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
 * Audits three SQL Server concepts against SqlServerAnalysisVisitor:
 *   1. Local/global temp tables (#temp / ##temp) and table variables (DECLARE @tbl TABLE(...))
 *      used later in JOIN/INSERT.
 *   2. Table hints (WITH (NOLOCK), WITH (INDEX(...))) right after a table reference in
 *      SELECT/UPDATE/DELETE FROM clauses.
 *   3. Cross-database / linked-server qualified plain TABLE references (3-part db.schema.table,
 *      4-part server.db.schema.table) — NOT function calls.
 */
@DisplayName("AUDIT: SQL Server temp tables / table hints / cross-db qualified table refs")
public class SqlServerTempTableHintSynonymAuditTest {

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

    private List<String> schemaNames(SqlServerRoutineInfo r) {
        return r.getTableOps().stream()
                .map(SqlServerTableOpInfo::getSchemaName)
                .filter(Objects::nonNull)
                .map(String::toUpperCase)
                .toList();
    }

    // ─────────────────────────────────────────────────────────────────────
    // Concept 1: temp tables (#temp / ##temp) and table variables (@tbl)
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("AUDIT 1a: local temp table #Local created then JOINed")
    void auditLocalTempTableJoin() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT id INTO #Local FROM SourceTable;\n" +
                "  SELECT s.id, l.val\n" +
                "  FROM SourceTable s\n" +
                "  JOIN #Local l ON l.id = s.id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[AUDIT 1a] tableOps=" + names + " errors=" + r.getErrors());
        System.out.println("[AUDIT 1a] full tableOps detail: " + r.getTableOps());
        assertTrue(names.contains("SOURCETABLE"), "SourceTable must be captured");
        // Expected (correct behaviour): #Local should be captured as a table op (both the
        // SELECT INTO creation and/or the later JOIN reference).
        boolean foundLocalTemp = names.stream().anyMatch(n -> n.contains("LOCAL"));
        System.out.println("[AUDIT 1a] #Local captured as table op: " + foundLocalTemp);
    }

    @Test
    @DisplayName("AUDIT 1b: global temp table ##Global created then JOINed")
    void auditGlobalTempTableJoin() {
        String sql = "CREATE PROCEDURE p2\n" +
                "AS\n" +
                "BEGIN\n" +
                "  CREATE TABLE ##Global (id INT, val INT);\n" +
                "  SELECT s.id, g.val\n" +
                "  FROM SourceTable s\n" +
                "  JOIN ##Global g ON g.id = s.id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[AUDIT 1b] tableOps=" + names + " errors=" + r.getErrors());
        System.out.println("[AUDIT 1b] full tableOps detail: " + r.getTableOps());
        assertTrue(names.contains("SOURCETABLE"), "SourceTable must be captured");
        boolean foundGlobalTemp = names.stream().anyMatch(n -> n.contains("GLOBAL"));
        System.out.println("[AUDIT 1b] ##Global captured as table op: " + foundGlobalTemp);
    }

    @Test
    @DisplayName("AUDIT 1c: table variable @tbl used in a JOIN")
    void auditTableVariableJoin() {
        String sql = "CREATE PROCEDURE p3\n" +
                "AS\n" +
                "BEGIN\n" +
                "  DECLARE @tbl TABLE (id INT, val INT);\n" +
                "  INSERT INTO @tbl SELECT id, val FROM SourceTable;\n" +
                "  SELECT s.id, t.val\n" +
                "  FROM SourceTable s\n" +
                "  JOIN @tbl t ON t.id = s.id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[AUDIT 1c] tableOps=" + names + " errors=" + r.getErrors());
        System.out.println("[AUDIT 1c] full tableOps detail: " + r.getTableOps());
        assertTrue(names.contains("SOURCETABLE"), "SourceTable must be captured");
        // KEY FOCUS: is @tbl captured as a table op when used in JOIN? Source inspection of
        // SqlServerAnalysisVisitor.addTableOp shows an explicit guard:
        //   "if (schema == null && (... || name.startsWith("@") || ...)) return null;"
        // which drops ANY name beginning with '@' before it is ever recorded — regardless of
        // whether it's a real table-valued parameter or a legitimate DECLARE'd table variable
        // that was just populated via INSERT INTO @tbl two lines above.
        boolean foundTblViaJoin = names.stream().anyMatch(n -> n.equals("@TBL") || n.contains("TBL"));
        System.out.println("[AUDIT 1c] @tbl captured as table op via JOIN: " + foundTblViaJoin);
        assertTrue(foundTblViaJoin, "@tbl table variable must be captured when used in a JOIN");
    }

    @Test
    @DisplayName("AUDIT 1d: table variable @tbl used as INSERT target (structural tableRef path)")
    void auditTableVariableInsertTarget() {
        String sql = "CREATE PROCEDURE p4\n" +
                "AS\n" +
                "BEGIN\n" +
                "  DECLARE @tbl TABLE (id INT, val INT);\n" +
                "  INSERT INTO @tbl SELECT id, val FROM SourceTable;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[AUDIT 1d] tableOps=" + names + " errors=" + r.getErrors());
        System.out.println("[AUDIT 1d] full tableOps detail: " + r.getTableOps());
        // INSERT/UPDATE/DELETE go through the structural tableRef grammar rule
        // (tableRef : AT_VAR | ...), which does NOT pass through addTableOp's '@'-name filter —
        // extractTableName() just returns ctx.getText() directly. Confirm whether this path
        // captures @tbl (structural) vs the regex/JOIN path (audit 1c) which drops it.
        boolean foundTblInsert = names.stream().anyMatch(n -> n.contains("TBL"));
        System.out.println("[AUDIT 1d] @tbl captured as INSERT-target table op: " + foundTblInsert);
    }

    @Test
    @DisplayName("AUDIT 1e: table variable @tbl used in JOIN with NO prior INSERT statement in scope (isolates the JOIN-scan path from the structural INSERT path)")
    void auditTableVariableJoinOnlyNoInsert() {
        String sql = "CREATE PROCEDURE p4b\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT s.id, t.val\n" +
                "  FROM SourceTable s\n" +
                "  JOIN @tbl t ON t.id = s.id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[AUDIT 1e] tableOps=" + names + " errors=" + r.getErrors());
        System.out.println("[AUDIT 1e] full tableOps detail: " + r.getTableOps());
        assertTrue(names.contains("SOURCETABLE"), "SourceTable must be captured");
        // CONFIRMED BUG (see report): addTableOp() has an explicit guard
        //   "if (schema == null && (... || name.startsWith("@") || ...)) return null;"
        // which unconditionally drops any '@'-prefixed name reached via the FROM/JOIN regex scan
        // (addTableOpsFromText -> addTableOp). @tbl is completely absent from tableOps here —
        // silently dropped, not even as a phantom entry. Contrast with AUDIT 1a/1b where #Local /
        // ##Global ARE correctly captured via the identical JOIN-scan code path.
        boolean foundTblViaJoin = names.stream().anyMatch(n -> n.contains("TBL"));
        System.out.println("[AUDIT 1e] @tbl captured as table op via JOIN (no prior INSERT): " + foundTblViaJoin);
        assertTrue(foundTblViaJoin, "@tbl table variable must be captured via the JOIN-scan path even with no prior INSERT");
    }

    // ─────────────────────────────────────────────────────────────────────
    // Concept 2: table hints WITH (NOLOCK) / WITH (INDEX(...))
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("AUDIT 2a: SELECT ... FROM t WITH (NOLOCK) JOIN t2 WITH (NOLOCK)")
    void auditSelectWithNolockHints() {
        String sql = "CREATE PROCEDURE p5\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT o.id, c.name\n" +
                "  FROM Orders o WITH (NOLOCK)\n" +
                "  JOIN Customers c WITH (NOLOCK) ON c.id = o.customer_id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[AUDIT 2a] tableOps=" + names + " errors=" + r.getErrors());
        System.out.println("[AUDIT 2a] full tableOps detail: " + r.getTableOps());
        assertTrue(names.contains("ORDERS"), "AUDIT: Orders must not be dropped/corrupted by WITH (NOLOCK) hint");
        assertTrue(names.contains("CUSTOMERS"), "AUDIT: Customers must not be dropped/corrupted by WITH (NOLOCK) hint");
        boolean phantomHintTable = names.stream().anyMatch(n -> n.equals("WITH") || n.equals("NOLOCK"));
        System.out.println("[AUDIT 2a] phantom hint-token table op present: " + phantomHintTable);
        assertFalse(phantomHintTable, "hint tokens must not become phantom table ops");
    }

    @Test
    @DisplayName("AUDIT 2b: UPDATE t WITH (ROWLOCK) SET ... table hint on UPDATE target")
    void auditUpdateWithHint() {
        String sql = "CREATE PROCEDURE p6\n" +
                "AS\n" +
                "BEGIN\n" +
                "  UPDATE Orders WITH (ROWLOCK) SET status = 'X' WHERE id = 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[AUDIT 2b] tableOps=" + names + " errors=" + r.getErrors());
        System.out.println("[AUDIT 2b] full tableOps detail: " + r.getTableOps());
        assertTrue(names.contains("ORDERS"), "AUDIT: Orders (UPDATE target) must survive the WITH (ROWLOCK) hint");
        assertEquals(1, names.size(), "no phantom extra table op from the hint tokens");
    }

    @Test
    @DisplayName("AUDIT 2c: DELETE FROM t WITH (NOLOCK) WHERE ... table hint on DELETE target")
    void auditDeleteWithHint() {
        String sql = "CREATE PROCEDURE p7\n" +
                "AS\n" +
                "BEGIN\n" +
                "  DELETE FROM Orders WITH (NOLOCK) WHERE id = 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[AUDIT 2c] tableOps=" + names + " errors=" + r.getErrors());
        System.out.println("[AUDIT 2c] full tableOps detail: " + r.getTableOps());
        assertTrue(names.contains("ORDERS"), "AUDIT: Orders (DELETE target) must survive the WITH (NOLOCK) hint");
        assertEquals(1, names.size(), "no phantom extra table op from the hint tokens");
    }

    @Test
    @DisplayName("AUDIT 2d: legacy no-WITH INDEX hint — FROM t INDEX(idx_name)")
    void auditLegacyIndexHintNoWith() {
        String sql = "CREATE PROCEDURE p8\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT o.id\n" +
                "  FROM Orders o INDEX(idx_orders_id)\n" +
                "  WHERE o.id = 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        String aliases = r.getTableOps().stream()
                .map(SqlServerTableOpInfo::getAlias).filter(Objects::nonNull).toList().toString();
        System.out.println("[AUDIT 2d] tableOps=" + names + " aliases=" + aliases + " errors=" + r.getErrors());
        System.out.println("[AUDIT 2d] full tableOps detail: " + r.getTableOps());
        assertTrue(names.contains("ORDERS"), "AUDIT: Orders must survive legacy INDEX(...) hint without WITH");
        // Note: "o" is the real alias; INDEX is NOT in RESERVED_NON_TABLE / ALIAS_STOPWORDS, so
        // if the regex's alias-capture group re-fires on "INDEX" (misreading the hint keyword as
        // the alias) that would corrupt the alias field — recorded here as observed evidence.
    }

    // ─────────────────────────────────────────────────────────────────────
    // Concept 3: cross-database / linked-server qualified table references
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("AUDIT 3a: 3-part db.schema.table plain table reference in FROM/JOIN")
    void auditThreePartTableReference() {
        String sql = "CREATE PROCEDURE p9\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT c.id, o.total\n" +
                "  FROM Customers c\n" +
                "  JOIN OtherDb.dbo.Orders o ON o.customer_id = c.id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        List<String> schemas = schemaNames(r);
        System.out.println("[AUDIT 3a] tableOps=" + names + " schemas=" + schemas + " errors=" + r.getErrors());
        System.out.println("[AUDIT 3a] full tableOps detail: " + r.getTableOps());
        assertTrue(names.contains("CUSTOMERS"), "AUDIT: Customers must be present");
        // Expected correct behaviour: the real table "Orders" (in OtherDb.dbo) should appear,
        // NOT a phantom fragment table literally named "DBO" (TABLE_REF_PATTERN's single-dot
        // limit stopping mid-name, same class of bug already fixed for TVF calls).
        boolean foundRealTable = names.stream().anyMatch(n -> n.contains("ORDERS"));
        boolean foundPhantomDboFragment = names.stream().anyMatch(n -> n.equals("DBO"));
        System.out.println("[AUDIT 3a] real ORDERS table captured: " + foundRealTable
                + "  phantom DBO fragment present: " + foundPhantomDboFragment);
        assertTrue(foundRealTable, "real ORDERS table must be captured from a 3-part db.schema.table reference");
        assertFalse(foundPhantomDboFragment, "no phantom DBO fragment table should be produced");
    }

    @Test
    @DisplayName("AUDIT 3b: 4-part server.db.schema.table linked-server table reference in FROM/JOIN")
    void auditFourPartLinkedServerReference() {
        String sql = "CREATE PROCEDURE p10\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT c.id, o.total\n" +
                "  FROM Customers c\n" +
                "  JOIN LinkedSrv.OtherDb.dbo.Orders o ON o.customer_id = c.id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[AUDIT 3b] tableOps=" + names + " errors=" + r.getErrors());
        System.out.println("[AUDIT 3b] full tableOps detail: " + r.getTableOps());
        assertTrue(names.contains("CUSTOMERS"), "AUDIT: Customers must be present");
        boolean foundRealTable = names.stream().anyMatch(n -> n.contains("ORDERS"));
        boolean foundPhantomFragment = names.stream().anyMatch(
                n -> n.equals("OTHERDB") || n.equals("DBO") || n.equals("LINKEDSRV"));
        System.out.println("[AUDIT 3b] real ORDERS table captured: " + foundRealTable
                + "  phantom fragment table present: " + foundPhantomFragment);
        assertTrue(foundRealTable, "real ORDERS table must be captured from a 4-part linked-server reference");
        assertFalse(foundPhantomFragment, "no phantom fragment table should be produced");
    }

    @Test
    @DisplayName("AUDIT 3c: 3-part table reference as sole SELECT target (bare, no alias)")
    void auditThreePartBareSelect() {
        String sql = "CREATE PROCEDURE p11\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM OtherDb.dbo.Orders;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[AUDIT 3c] tableOps=" + names + " errors=" + r.getErrors());
        System.out.println("[AUDIT 3c] full tableOps detail: " + r.getTableOps());
        boolean foundRealTable = names.stream().anyMatch(n -> n.contains("ORDERS"));
        boolean foundPhantomDboFragment = names.stream().anyMatch(n -> n.equals("DBO"));
        System.out.println("[AUDIT 3c] real ORDERS table captured: " + foundRealTable
                + "  phantom DBO fragment present: " + foundPhantomDboFragment);
        assertTrue(foundRealTable, "real ORDERS table must be captured from a bare 3-part reference");
        assertFalse(foundPhantomDboFragment, "no phantom DBO fragment table should be produced");
    }

    /**
     * Golden: TEMP as the FOURTH table kind. SQL Server's temp objects are session-local and are
     * never in the catalog, so the name prefix itself (#/##/@) is the classification signal — a
     * parse-side, grammar-adjacent decision (the name comes off the parse tree), never a lookup.
     */
    @Test
    @DisplayName("TEMP kind: #local, ##global and @table-variable are classified TEMP with their scope")
    void tempKindIsClassified() {
        SqlServerRoutineInfo r = parse("CREATE PROCEDURE p_tmp\n"
                + "AS\n"
                + "BEGIN\n"
                + "  CREATE TABLE #stage (id INT, val INT);\n"
                + "  CREATE TABLE ##globalstage (id INT);\n"
                + "  DECLARE @ids TABLE (id INT);\n"
                + "  SELECT id FROM @ids;\n"
                + "  SELECT id FROM RealTable;\n"
                + "END");

        SqlServerTableOpInfo local = r.getTableOps().stream()
                .filter(t -> "#stage".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        assertNotNull(local, "#stage must be captured as a table op");
        assertEquals("TEMP", local.getTableKind(), "#local must be classified TEMP");
        assertEquals("SESSION", local.getTempScope());

        SqlServerTableOpInfo global = r.getTableOps().stream()
                .filter(t -> "##globalstage".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        assertNotNull(global, "##globalstage must be captured as a table op");
        assertEquals("TEMP", global.getTableKind(), "##global must be classified TEMP");
        assertEquals("GLOBAL SESSION", global.getTempScope());

        SqlServerTableOpInfo var = r.getTableOps().stream()
                .filter(t -> "@ids".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        assertNotNull(var, "@ids table variable must be captured as a table op");
        assertEquals("TEMP", var.getTableKind(), "@table-variable must be classified TEMP");
        assertEquals("BATCH", var.getTempScope());

        // A real persistent table is untouched.
        assertTrue(r.getTableOps().stream().anyMatch(t -> t.getTableKind() == null),
                "an ordinary persistent table must keep a null tableKind (defaults to TABLE)");
    }
}
