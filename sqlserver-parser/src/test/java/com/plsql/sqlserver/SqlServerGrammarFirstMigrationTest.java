package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerCursorInfo;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import com.plsql.sqlserver.model.SqlServerSequenceInfo;
import com.plsql.sqlserver.model.SqlServerTableOpInfo;
import com.plsql.sqlserver.visitor.SqlServerAnalysisVisitor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Batch 2 (regex → grammar) migration tests.
 *
 * <p>Each construct migrated onto a real parse-tree walk is asserted to resolve correctly from
 * the grammar, and the one deliberately-unparseable case is asserted to fall through to the
 * COUNTED legacy regex — the same evidence shape Batch 1 used for dynamic SQL
 * ({@code PlSqlAnalysisVisitor.DYN_SQL_REGEX_FALLBACKS}).
 */
@DisplayName("SQL Server grammar-first migration (batch 2): structured extraction + counted fallbacks")
public class SqlServerGrammarFirstMigrationTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private static List<String> tableNames(SqlServerRoutineInfo r) {
        return r.getTableOps().stream()
                .map(SqlServerTableOpInfo::getTableName)
                .filter(Objects::nonNull)
                .map(String::toUpperCase)
                .toList();
    }

    @Test
    @DisplayName("NEXT VALUE FOR resolves from the nextValueFor parse-tree node")
    void nextValueForResolvedFromParseTree() {
        long before = SqlServerAnalysisVisitor.SEQUENCE_REGEX_FALLBACKS.get();
        String sql = "CREATE PROCEDURE p AS BEGIN SELECT NEXT VALUE FOR dbo.OrderSeq; END";
        SqlServerRoutineInfo r = parse(sql);
        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        SqlServerSequenceInfo seq = r.getSequences().stream()
                .filter(s -> !s.isIdentityColumn())
                .filter(s -> "OrderSeq".equalsIgnoreCase(s.getName()))
                .findFirst().orElse(null);
        assertNotNull(seq, "NEXT VALUE FOR usage must be captured: " + r.getSequences());
        assertEquals("dbo", seq.getSchema());
        assertEquals(before, SqlServerAnalysisVisitor.SEQUENCE_REGEX_FALLBACKS.get(),
                "grammar path must not touch the sequence regex fallback");
    }

    @Test
    @DisplayName("Cursor lifecycle (OPEN/FETCH/CLOSE/DEALLOCATE) resolves from cursorStatement nodes")
    void cursorLifecycleResolvedFromParseTree() {
        String sql = "CREATE PROCEDURE p AS\n" +
                "BEGIN\n" +
                "  DECLARE cur CURSOR FOR SELECT id FROM dbo.Orders;\n" +
                "  OPEN cur;\n" +
                "  FETCH NEXT FROM cur INTO @id;\n" +
                "  CLOSE cur;\n" +
                "  DEALLOCATE cur;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertEquals(1, r.getCursors().size(), "one cursor declared");
        SqlServerCursorInfo cur = r.getCursors().get(0);
        List<String> ops = cur.getStmtOperations().stream()
                .map(SqlServerCursorInfo.CursorStmtOp::getOperation)
                .toList();
        assertEquals(List.of("OPEN", "FETCH", "CLOSE", "DEALLOCATE"), ops,
                "full lifecycle must be captured in source order: " + ops);
    }

    @Test
    @DisplayName("CTE name resolves from cteDefinition nodes and is not recorded as a table")
    void cteNameResolvedFromParseTree() {
        String sql = "CREATE PROCEDURE p AS\n" +
                "BEGIN\n" +
                "  WITH recent AS (SELECT id FROM dbo.Orders)\n" +
                "  SELECT * FROM recent;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertTrue(tableNames(r).contains("ORDERS"), "real base table must be captured: " + tableNames(r));
        assertFalse(tableNames(r).contains("RECENT"), "CTE name must never be recorded as a table");
    }

    @Test
    @DisplayName("IDENTITY column resolves from createTableStatement/identitySpec nodes")
    void identityColumnResolvedFromParseTree() {
        String sql = "CREATE PROCEDURE p AS\n" +
                "BEGIN\n" +
                "  CREATE TABLE #t (Id INT IDENTITY(5,2), Name NVARCHAR(50));\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        SqlServerSequenceInfo id = r.getSequences().stream()
                .filter(SqlServerSequenceInfo::isIdentityColumn)
                .findFirst().orElse(null);
        assertNotNull(id, "IDENTITY column must be captured: " + r.getSequences());
        assertEquals("Id", id.getColumnName());
        assertEquals(5L, id.getSeed());
        assertEquals(2L, id.getIncrement());
    }

    @Test
    @DisplayName("In-body CREATE SYNONYM resolves from the createSynonym node")
    void synonymInBodyResolvedFromParseTree() {
        String sql = "CREATE PROCEDURE p AS\n" +
                "BEGIN\n" +
                "  CREATE SYNONYM dbo.OrdersSyn FOR dbo.Orders;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        List<String> names = tableNames(r);
        assertTrue(names.contains("ORDERSSYN"), "synonym object must be recorded: " + names);
        assertTrue(names.contains("ORDERS"), "synonym target must be recorded as a read: " + names);
    }

    @Test
    @DisplayName("EXISTS in a parenthesised IF condition is outside the grammar path → counted regex fallback")
    void existsFallbackIsCounted() {
        long before = SqlServerAnalysisVisitor.EXISTS_REGEX_FALLBACKS.get();
        // The extra parentheses mean the condition is an opaque parenGroup, so no existsPredicate
        // node is produced and the legacy scan must take over — and be counted.
        String sql = "CREATE PROCEDURE p AS\n" +
                "BEGIN\n" +
                "  IF (EXISTS (SELECT 1 FROM dbo.Orders)) BEGIN SELECT 1; END\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertTrue(SqlServerAnalysisVisitor.EXISTS_REGEX_FALLBACKS.get() > before,
                "the counted EXISTS regex fallback must have run");
        assertTrue(tableNames(r).contains("ORDERS"),
                "the fallback must still recover the subquery's table: " + tableNames(r));
    }

    // ── Task #30: qualified-call + CASE contexts ─────────────────────────────────

    @Test
    @DisplayName("Task #30: a schema-qualified call resolves from a callExpression node, no fallback")
    void qualifiedCallResolvedFromParseTree() {
        long before = SqlServerAnalysisVisitor.QUALIFIED_CALL_REGEX_FALLBACKS.get();
        String sql = "CREATE PROCEDURE p AS\n" +
                "BEGIN\n" +
                "  SET @total = sales.fn_OrderTotal(@orderId);\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertTrue(r.getCalls().stream().anyMatch(c -> "fn_OrderTotal".equalsIgnoreCase(c.getName())
                        && "sales".equalsIgnoreCase(c.getSchema())),
                "the qualified call must be captured from the parse tree: " + r.getCalls());
        assertEquals(before, SqlServerAnalysisVisitor.QUALIFIED_CALL_REGEX_FALLBACKS.get(),
                "the structured path must not touch the qualified-call regex fallback");
    }

    @Test
    @DisplayName("Task #30: a call the grammar cannot structure (PRINT body) → counted regex fallback")
    void qualifiedCallFallbackIsCounted() {
        long before = SqlServerAnalysisVisitor.QUALIFIED_CALL_REGEX_FALLBACKS.get();
        // PRINT falls into otherStatement's token-soup catch-all, so no callExpression node exists;
        // the legacy scan must still recover the call — and be counted.
        String sql = "CREATE PROCEDURE p AS\n" +
                "BEGIN\n" +
                "  PRINT dbo.fn_Log(@msg);\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertTrue(SqlServerAnalysisVisitor.QUALIFIED_CALL_REGEX_FALLBACKS.get() > before,
                "the counted qualified-call regex fallback must have run");
        assertTrue(r.getCalls().stream().anyMatch(c -> "fn_Log".equalsIgnoreCase(c.getName())),
                "the fallback must still recover the PRINT-body call: " + r.getCalls());
    }

    @Test
    @DisplayName("Task #30: a CASE expression resolves from a caseExpression node, no fallback")
    void caseResolvedFromParseTree() {
        long before = SqlServerAnalysisVisitor.CASE_REGEX_FALLBACKS.get();
        String sql = "CREATE PROCEDURE p AS\n" +
                "BEGIN\n" +
                "  RETURN CASE WHEN @x > 0 THEN 1 ELSE 0 END;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertEquals(1, r.getStatements().stream()
                        .filter(s -> "CASE".equals(s.getType())).count(),
                "exactly one CASE marker from the structured context: " + r.getStatements());
        assertEquals(before, SqlServerAnalysisVisitor.CASE_REGEX_FALLBACKS.get(),
                "the structured path must not touch the CASE regex fallback");
    }

    @Test
    @DisplayName("Task #30: a CASE the grammar cannot structure (nested subquery) → counted regex fallback")
    void caseFallbackIsCounted() {
        long before = SqlServerAnalysisVisitor.CASE_REGEX_FALLBACKS.get();
        // The parenthesised subquery is consumed as a balanced parenGroup, so its CASE is not a
        // caseExpression node; the legacy scan must still count it — and be counted.
        String sql = "CREATE PROCEDURE p AS\n" +
                "BEGIN\n" +
                "  SELECT (SELECT CASE WHEN x > 0 THEN 1 ELSE 0 END FROM dbo.T) AS c FROM dbo.Orders;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertTrue(SqlServerAnalysisVisitor.CASE_REGEX_FALLBACKS.get() > before,
                "the counted CASE regex fallback must have run");
        assertTrue(r.getStatements().stream().anyMatch(s -> "CASE".equals(s.getType())),
                "the fallback must still count the subquery CASE: " + r.getStatements());
    }

    @Test
    @DisplayName("Task #30: standalone CREATE SYNONYM is a valid compilation unit")
    void standaloneSynonymResolvedFromParseTree() {
        long before = SqlServerAnalysisVisitor.SYNONYM_REGEX_FALLBACKS.get();
        SqlServerRoutineInfo r = parse("CREATE SYNONYM dbo.OrdersSyn FOR dbo.Orders;");
        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(),
                "a standalone synonym must parse cleanly: " + r.getErrors());
        List<String> names = tableNames(r);
        assertTrue(names.contains("ORDERSSYN"), "synonym object recorded: " + names);
        assertTrue(names.contains("ORDERS"), "synonym target recorded: " + names);
        assertEquals(before, SqlServerAnalysisVisitor.SYNONYM_REGEX_FALLBACKS.get(),
                "the structured compilation-unit path must not touch the synonym regex fallback");
    }
}
