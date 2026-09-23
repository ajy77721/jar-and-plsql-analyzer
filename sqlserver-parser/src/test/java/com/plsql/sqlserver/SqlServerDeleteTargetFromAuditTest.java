package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import com.plsql.sqlserver.model.SqlServerTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * T-SQL `DELETE &lt;target&gt; FROM &lt;sources&gt; …` capture, and the safety boundary around it.
 *
 * <p>`DELETE t FROM dbo.Target t JOIN dbo.Other o ON …` names its delete target by ALIAS; the real
 * table appears only in the FROM clause. {@code visitDeleteStatement}'s Stage-8
 * {@code retargetDmlOpFromAlias} already handled that for a statement the grammar routes to it, but
 * a statement beginning with `WITH` is consumed whole by {@code otherStatement} and recovered only
 * by the {@code scanOps} text scan — where {@code KEYWORD_OP_PATTERN} knows `DELETE FROM` and not
 * bare `DELETE`, so NO delete op was recorded at all. {@code scanDeleteTargetFrom} closes that gap.
 *
 * <p>The tests below pin both halves: the gap is closed, AND the constructs the fix must not
 * disturb — above all MERGE's `WHEN NOT MATCHED … THEN DELETE`, where a bare `DELETE` carries no
 * target at all and must never fabricate one.
 */
class SqlServerDeleteTargetFromAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String body) {
        return engine.parse("CREATE PROCEDURE usp_del\nAS\nBEGIN\n" + body + "\nEND", "usp_del", null);
    }

    private SqlServerTableOpInfo deleteOp(SqlServerRoutineInfo r, String schema, String name) {
        return r.getTableOps().stream()
                .filter(t -> t.getDmlType() == SqlServerTableOpInfo.DmlType.DELETE
                        && name.equalsIgnoreCase(t.getTableName())
                        && (schema == null ? t.getSchemaName() == null : schema.equalsIgnoreCase(t.getSchemaName())))
                .findFirst().orElse(null);
    }

    private long deleteOps(SqlServerRoutineInfo r) {
        return r.getTableOps().stream()
                .filter(t -> t.getDmlType() == SqlServerTableOpInfo.DmlType.DELETE).count();
    }

    // ── Safety: MERGE's bare THEN DELETE has no target and must fabricate none ──────────────
    @Test
    @DisplayName("MERGE ... WHEN NOT MATCHED ... THEN DELETE records no delete-target table op")
    void mergeThenDelete_fabricatesNoTarget() {
        SqlServerRoutineInfo r = parse(
                "    MERGE INTO dbo.Inventory AS tgt\n" +
                "    USING dbo.Staging AS src\n" +
                "    ON tgt.ProductId = src.ProductId\n" +
                "    WHEN MATCHED THEN UPDATE SET tgt.Qty = src.Qty\n" +
                "    WHEN NOT MATCHED BY TARGET THEN INSERT (ProductId) VALUES (src.ProductId)\n" +
                "    WHEN NOT MATCHED BY SOURCE THEN DELETE;");
        assertEquals(0, deleteOps(r),
                "MERGE's bare THEN DELETE must not be read as a DML target: " + r.getTableOps());
        assertNotNull(r.getTableOps().stream()
                        .filter(t -> t.getDmlType() == SqlServerTableOpInfo.DmlType.MERGE).findFirst().orElse(null),
                "the MERGE target itself must still be captured");
    }

    // ── Unaffected: the two ordinary DELETE forms the grammar already routes to the visitor ──
    @Test
    @DisplayName("Plain DELETE FROM dbo.T keeps working")
    void plainDeleteFrom() {
        SqlServerRoutineInfo r = parse("    DELETE FROM dbo.T WHERE id = 1;");
        assertNotNull(deleteOp(r, "dbo", "T"), "expected DELETE dbo.T, got " + r.getTableOps());
        assertEquals(1, deleteOps(r), "exactly one delete op: " + r.getTableOps());
    }

    @Test
    @DisplayName("DELETE dbo.T (no FROM clause at all) keeps working")
    void plainDeleteNoFromKeyword() {
        // The grammar rule is `DELETE FROM? tableRef stmtTail`, so the FROM is optional and this
        // reaches visitDeleteStatement with dbo.T as its tableRef. Unchanged by the new scan,
        // which requires a FROM adjacent to the target name and therefore does not match here.
        SqlServerRoutineInfo r = parse("    DELETE dbo.T WHERE id = 1;");
        assertNotNull(deleteOp(r, "dbo", "T"), "expected DELETE dbo.T, got " + r.getTableOps());
        assertEquals(1, deleteOps(r), "exactly one delete op: " + r.getTableOps());
    }

    // ── THE GAP: WITH-prefixed DELETE naming its target by alias ────────────────────────────
    @Test
    @DisplayName("WITH cte ... DELETE t FROM dbo.Target t JOIN ... records DELETE dbo.Target")
    void withCte_deleteByAlias_fromJoin() {
        SqlServerRoutineInfo r = parse(
                "    WITH c AS (SELECT id FROM dbo.Src)\n" +
                "    DELETE t FROM dbo.Target t JOIN dbo.Other o ON t.id = o.id WHERE o.f = 1;");
        SqlServerTableOpInfo del = deleteOp(r, "dbo", "Target");
        assertNotNull(del, "the delete target dbo.Target must be recorded (was missing entirely): "
                + r.getTableOps());
        assertEquals("t", del.getAlias(), "the alias the statement used should be retained");
        assertFalse(r.getTableOps().stream().anyMatch(t -> "t".equalsIgnoreCase(t.getTableName())),
                "the alias must never be recorded as a table in its own right: " + r.getTableOps());
        // The FROM-side reads are unchanged.
        assertTrue(r.getTableOps().stream().anyMatch(t -> "Other".equalsIgnoreCase(t.getTableName())),
                "joined source dbo.Other still captured");
    }

    @Test
    @DisplayName("WITH cte ... DELETE FROM dbo.Target WHERE ... is unaffected")
    void withCte_qualifiedDeleteFrom() {
        SqlServerRoutineInfo r = parse(
                "    WITH c AS (SELECT id FROM dbo.Src)\n" +
                "    DELETE FROM dbo.Target WHERE id IN (SELECT id FROM c);");
        assertNotNull(deleteOp(r, "dbo", "Target"), "expected DELETE dbo.Target, got " + r.getTableOps());
        assertEquals(1, deleteOps(r), "exactly one delete op: " + r.getTableOps());
    }

    @Test
    @DisplayName("WITH cte ... DELETE dbo.Target FROM dbo.Target INNER JOIN ... (alias-free form)")
    void withCte_aliasFreeDeleteWithJoin() {
        SqlServerRoutineInfo r = parse(
                "    WITH c AS (SELECT id FROM dbo.Src)\n" +
                "    DELETE dbo.Target FROM dbo.Target INNER JOIN dbo.Other o ON dbo.Target.id = o.id;");
        SqlServerTableOpInfo del = deleteOp(r, "dbo", "Target");
        assertNotNull(del, "an explicitly-qualified delete target must be recorded as written: "
                + r.getTableOps());
        assertEquals(1, deleteOps(r), "exactly one delete op: " + r.getTableOps());
    }

    // ── The non-WITH form Stage 8 already covered stays covered (no double-recording) ────────
    @Test
    @DisplayName("Non-WITH DELETE t FROM dbo.Target t JOIN ... still resolves via Stage 8 only")
    void nonWithDeleteByAlias() {
        SqlServerRoutineInfo r = parse("    DELETE t FROM dbo.Target t JOIN dbo.Other o ON t.id = o.id;");
        SqlServerTableOpInfo del = deleteOp(r, "dbo", "Target");
        assertNotNull(del, "expected DELETE dbo.Target, got " + r.getTableOps());
        assertEquals("t", del.getAlias());
        assertEquals(1, deleteOps(r), "exactly one delete op: " + r.getTableOps());
    }
}
