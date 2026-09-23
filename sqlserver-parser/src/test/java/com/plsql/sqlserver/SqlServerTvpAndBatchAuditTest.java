package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerParamInfo;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import com.plsql.sqlserver.model.SqlServerTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * READ-ONLY AUDIT — does NOT touch main/ source. Verifies actual (fresh-read) behavior of
 * SqlServerAnalysisVisitor for:
 *
 *  1. Table-valued parameters (TVP, declared READONLY) passed into a procedure — whether the
 *     TVP itself is (correctly) excluded from the table-op inventory, whether a real table
 *     referenced alongside it is still captured, and whether the TVP's underlying user-defined
 *     type (dbo.MyTableType) is registered anywhere.
 *  2. SQL Server's multi-row VALUES batch INSERT — whether the target table is captured exactly
 *     once (not once per VALUES row-group).
 *
 * All findings are asserted against real engine.parse() output; some assertions are expected to
 * fail (or be exploratory println-only) and stand as bug evidence per the audit's own findings.
 */
class SqlServerTvpAndBatchAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private void dump(String label, SqlServerRoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("params: " + r.getParams().stream()
                .map(p -> p.getName() + " " + p.getDataType() + " dir=" + p.getDirection()).toList());
        System.out.println("tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.qualifiedTable() + "@" + t.getLine()).toList());
        System.out.println("errors: " + r.getErrors());
    }

    // ── Concept 1: TVP (READONLY) passed into a procedure ──────────────────────────────────
    @Test
    @DisplayName("Concept 1: table-valued parameter (READONLY) vs a real table in the same body")
    void tvpParameter_vsRealTable() {
        String src =
                "CREATE PROCEDURE usp_Test (@tvp dbo.MyTableType READONLY)\n" +
                "AS\n" +
                "BEGIN\n" +
                "    INSERT INTO real_table SELECT * FROM @tvp;\n" +
                "    SELECT col FROM @tvp;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("tvpParameter_vsRealTable", r);

        // (a) Parameter itself: should be recorded once, with its declared type dbo.MyTableType,
        // direction IN (READONLY has no dedicated Direction — it is not OUTPUT/OUT).
        assertEquals(1, r.getParams().size(), "expected exactly one param recorded");
        SqlServerParamInfo tvp = r.getParams().get(0);
        assertEquals("@tvp", tvp.getName());
        System.out.println("TVP param dataType text = [" + tvp.getDataType() + "]");
        // ACTUAL: dataType() grammar rule captures only the (ID|BRACKET_ID)(DOT...)? part, i.e.
        // "dbo.MyTableType" — READONLY is consumed separately as a paramModifier, so it does NOT
        // leak into dataType text.
        assertEquals("dbo.MyTableType", tvp.getDataType(),
                "TVP data type text should be the qualified user type name, not include READONLY");
        assertEquals(SqlServerParamInfo.Direction.IN, tvp.getDirection(),
                "READONLY TVPs have no OUT direction in this model; observed direction is IN");

        // (b) @tvp must NOT appear as a table op (it is a parameter reference, not a table).
        boolean tvpAsTableOp = r.getTableOps().stream()
                .anyMatch(t -> "tvp".equalsIgnoreCase(t.getTableName())
                        || "@tvp".equalsIgnoreCase(t.getTableName()));
        assertFalse(tvpAsTableOp, "TVP @tvp must not be mis-recorded as its own table op; tableOps="
                + r.getTableOps().stream().map(SqlServerTableOpInfo::qualifiedTable).toList());

        // (c) The underlying user-defined type dbo.MyTableType is NOT registered anywhere in the
        // model as a distinct type-reference/table-op — this module has no such registry at all
        // (scalar types like INT/VARCHAR get the exact same non-treatment). Documented as
        // "works as designed", not a bug, since there is no existing convention this violates.
        boolean myTableTypeReferenced = r.getTableOps().stream()
                .anyMatch(t -> "MyTableType".equalsIgnoreCase(t.getTableName()));
        assertFalse(myTableTypeReferenced,
                "dbo.MyTableType is not tracked as a table op anywhere (no type-reference registry exists in this module)");

        // (d) real_table: the actual real table must be captured as an INSERT table op, and must
        // not be dropped/confused with @tvp.
        SqlServerTableOpInfo realTableOp = r.getTableOps().stream()
                .filter(t -> "real_table".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(realTableOp, "real_table must be captured as a table op; observed tableOps="
                + r.getTableOps().stream().map(SqlServerTableOpInfo::qualifiedTable).toList());
        assertEquals(SqlServerTableOpInfo.DmlType.INSERT, realTableOp.getDmlType());

        // (e) The standalone "SELECT col FROM @tvp;" must not fabricate a bogus placeholder
        // SELECT op (tableName=null) either — it has a FROM clause, it's just filtered.
        boolean bogusNullSelect = r.getTableOps().stream()
                .anyMatch(t -> t.getDmlType() == SqlServerTableOpInfo.DmlType.SELECT && t.getTableName() == null);
        assertFalse(bogusNullSelect,
                "SELECT ... FROM @tvp has a real FROM clause (on the TVP) and must not add a placeholder null-table SELECT op; tableOps="
                        + r.getTableOps().stream().map(SqlServerTableOpInfo::qualifiedTable).toList());

        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
    }

    // ── Concept 1b: TVP used only as a JOIN source alongside a real table ──────────────────
    @Test
    @DisplayName("Concept 1b: TVP joined against a real table — real table + join must survive")
    void tvpParameter_joinedWithRealTable() {
        String src =
                "CREATE PROCEDURE usp_TestJoin (@tvp dbo.MyTableType READONLY)\n" +
                "AS\n" +
                "BEGIN\n" +
                "    SELECT r.Id, t.Val\n" +
                "    FROM real_table r\n" +
                "    INNER JOIN @tvp t ON r.Id = t.Id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("tvpParameter_joinedWithRealTable", r);

        SqlServerTableOpInfo realTableOp = r.getTableOps().stream()
                .filter(t -> "real_table".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(realTableOp, "real_table must be captured even when joined with a TVP; observed tableOps="
                + r.getTableOps().stream().map(SqlServerTableOpInfo::qualifiedTable).toList());

        boolean tvpAsTableOp = r.getTableOps().stream()
                .anyMatch(t -> "tvp".equalsIgnoreCase(t.getTableName()) || "@tvp".equalsIgnoreCase(t.getTableName()));
        assertFalse(tvpAsTableOp, "@tvp must not be recorded as its own table op when used in a JOIN");

        // Fixed bug: JOIN_PATTERN's joined-table char class excluded `@`, so `INNER JOIN @tvp t
        // ON ...` never matched at all and the join edge was silently dropped. `@` is now
        // accepted in the joined-table group, so the edge is recorded (with the TVP name as the
        // joined-table identifier) without reintroducing @tvp as a base table op.
        assertNotNull(realTableOp, "real_table op must exist to assert its joins");
        assertEquals(1, realTableOp.getJoins().size(),
                "real_table -> @tvp join edge must be recorded, not silently dropped; joins="
                        + realTableOp.getJoins());
        assertEquals("@tvp", realTableOp.getJoins().get(0).getJoinedTable(),
                "joined-table identifier should be the TVP name");
        assertEquals("INNER", realTableOp.getJoins().get(0).getJoinType());
    }

    // ── Concept 2: multi-row VALUES batch INSERT ────────────────────────────────────────────
    @Test
    @DisplayName("Concept 2: multi-row VALUES batch INSERT captures target table exactly once")
    void multiRowBatchInsert_singleTargetTable() {
        String src =
                "CREATE PROCEDURE usp_BatchInsert\n" +
                "AS\n" +
                "BEGIN\n" +
                "    INSERT INTO tab VALUES (1,'a'), (2,'b'), (3,'c');\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("multiRowBatchInsert_singleTargetTable", r);

        long tabInsertCount = r.getTableOps().stream()
                .filter(t -> "tab".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.INSERT)
                .count();
        assertEquals(1, tabInsertCount,
                "multi-row VALUES batch INSERT must yield exactly one INSERT table op for 'tab', got "
                        + tabInsertCount + "; tableOps=" + r.getTableOps().stream()
                        .map(t -> t.getDmlType() + ":" + t.qualifiedTable()).toList());

        long totalTabOps = r.getTableOps().stream()
                .filter(t -> "tab".equalsIgnoreCase(t.getTableName())).count();
        assertEquals(1, totalTabOps,
                "'tab' must appear exactly once across all table ops (no phantom row-group duplicates), got "
                        + totalTabOps);

        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
    }

    // ── Concept 2b: multi-row VALUES batch INSERT with column list ─────────────────────────
    @Test
    @DisplayName("Concept 2b: multi-row VALUES batch INSERT with explicit column list")
    void multiRowBatchInsert_withColumnList() {
        String src =
                "CREATE PROCEDURE usp_BatchInsertCols\n" +
                "AS\n" +
                "BEGIN\n" +
                "    INSERT INTO tab (col1, col2) VALUES (1,'a'), (2,'b'), (3,'c'), (4,'d');\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("multiRowBatchInsert_withColumnList", r);

        long tabOps = r.getTableOps().stream().filter(t -> "tab".equalsIgnoreCase(t.getTableName())).count();
        assertEquals(1, tabOps, "4-row VALUES batch INSERT with column list must still yield exactly one 'tab' op, got "
                + tabOps + "; tableOps=" + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.qualifiedTable()).toList());
    }
}
