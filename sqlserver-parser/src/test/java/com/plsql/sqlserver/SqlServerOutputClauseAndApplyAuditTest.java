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
 * Dedicated SQL Server re-audit: CROSS/OUTER APPLY table-valued-function joins, PIVOT/UNPIVOT,
 * MERGE with a plain-table USING source and an OUTPUT clause, multiple different table hints
 * combined with real JOINs, and sp_executesql parameter-value binding.
 */
@DisplayName("AUDIT: SQL Server APPLY / PIVOT / MERGE-source / multi-hint join / sp_executesql-binding capture")
public class SqlServerOutputClauseAndApplyAuditTest {

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

    // ── 1: CROSS APPLY a table-valued function ────────────────────────────────

    @Test
    @DisplayName("1a: CROSS APPLY dbo.GetOrderLines(o.id) ol — TVF-apply join is dropped entirely")
    void auditCrossApplyTableFunction() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT o.id, ol.qty\n" +
                "  FROM Orders o\n" +
                "  CROSS APPLY dbo.GetOrderLines(o.id) ol;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        List<String> calls = r.getCalls().stream().map(c -> c.getName().toUpperCase()).toList();
        List<?> joins = r.getTableOps().stream().flatMap(t -> t.getJoins().stream()).toList();
        System.out.println("[1a] tableOps=" + names + " calls=" + calls + " joins=" + joins);
        assertTrue(names.contains("ORDERS"), "Orders base table must be captured");
        assertTrue(calls.contains("GETORDERLINES"), "GetOrderLines is captured as an outbound call (correct)");
        // FIXED: JOIN_PATTERN now consumes an optional TVF argument list between the schema.table
        // group and the alias/boundary groups, so CROSS APPLY of a parameterised TVF now produces
        // a join edge, in addition to the (already-correct) outbound call capture above.
        assertFalse(joins.isEmpty(),
                "CROSS APPLY of a parameterised TVF must produce a join entry linking Orders to GetOrderLines");
        SqlServerJoinInfo j = (SqlServerJoinInfo) joins.get(0);
        assertEquals("CROSS APPLY", j.getJoinType());
        assertEquals("GetOrderLines", j.getJoinedTable());
    }

    @Test
    @DisplayName("1b: OUTER APPLY with a derived-table subquery (not a function) — correct case")
    void auditOuterApplySubquery() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT o.id, x.total\n" +
                "  FROM Orders o\n" +
                "  OUTER APPLY (SELECT SUM(amount) AS total FROM OrderLines ol WHERE ol.order_id = o.id) x;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[1b] tableOps=" + names);
        assertTrue(names.contains("ORDERS"), "Orders must be captured");
        // Not a bug: the OUTER APPLY's derived-table subquery starts with '(' so JOIN_PATTERN's
        // mandatory identifier group after APPLY fails to match there (same shape as 1a), but
        // OrderLines inside the subquery is still captured by addTableOpsFromText's independent
        // whole-text FROM-clause scan, so the table itself is not lost here (only the join *edge*
        // to it would be, same gap as 1a).
        assertTrue(names.contains("ORDERLINES"), "OrderLines inside the OUTER APPLY subquery must still be captured");
    }

    // ── 2: PIVOT / UNPIVOT ──────────────────────────────────────────────────

    @Test
    @DisplayName("2a: PIVOT does not desync table capture — no gap found")
    void auditPivot() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT *\n" +
                "  FROM (SELECT category, amount FROM Sales) s\n" +
                "  PIVOT (SUM(amount) FOR category IN ([A],[B],[C])) pvt;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[2a] tableOps=" + names + " full=" + r.getTableOps());
        assertTrue(names.contains("SALES"), "Sales (inside the PIVOT's source subquery) must be captured");
        assertEquals(1, names.stream().filter(n -> n.equals("SALES")).count(),
                "Sales should be captured exactly once, not duplicated/desynced by the PIVOT clause");
    }

    // ── 3: MERGE with a plain-table USING source (RESERVED_NON_TABLE collision) ─

    @Test
    @DisplayName("3a: RESERVED_NON_TABLE silently drops any real table literally named Target/Source")
    void auditMergeTargetSourceNameCollision() {
        // Deliberately named exactly the way "MERGE INTO Target ... USING Source" idiom reads in
        // real schemas — Target/Source are common literal staging/change-log table names, not just
        // MERGE-clause keywords.
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  MERGE INTO dbo.Target AS tgt\n" +
                "  USING dbo.Source AS src\n" +
                "  ON tgt.id = src.id\n" +
                "  WHEN MATCHED THEN UPDATE SET tgt.val = src.val\n" +
                "  WHEN NOT MATCHED THEN INSERT (id, val) VALUES (src.id, src.val);\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[3a] tableOps=" + names + " full=" + r.getTableOps());
        // FIXED: addTableOp only excludes the bare, unqualified words TARGET/SOURCE (MERGE's own
        // conventional bare aliases). A schema-qualified reference (dbo.Target / dbo.Source) can
        // never be MERGE's own keyword usage — which is always unqualified — so it is no longer
        // filtered out by RESERVED_NON_TABLE.
        System.out.println("[3a] MERGE target dbo.Target captured? " + names.contains("TARGET"));
        System.out.println("[3a] MERGE source dbo.Source (via USING) captured? " + names.contains("SOURCE"));
        assertTrue(names.contains("TARGET"),
                "a real, schema-qualified table named Target must be captured, not dropped");
        assertTrue(names.contains("SOURCE"),
                "a real, schema-qualified table named Source (MERGE's USING side) must be captured, not dropped");
    }

    @Test
    @DisplayName("3b: same MERGE with non-colliding table names — USING plain-table source IS captured")
    void auditMergePlainUsingSourceNonColliding() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  MERGE INTO dbo.Inventory AS tgt\n" +
                "  USING dbo.StagingInventory AS src\n" +
                "  ON tgt.id = src.id\n" +
                "  WHEN MATCHED THEN UPDATE SET tgt.val = src.val\n" +
                "  WHEN NOT MATCHED THEN INSERT (id, val) VALUES (src.id, src.val)\n" +
                "  OUTPUT $action, inserted.id, deleted.val INTO @ChangeLog;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[3b] tableOps=" + names + " full=" + r.getTableOps());
        assertTrue(names.contains("INVENTORY"), "MERGE target must be captured when its name doesn't collide with RESERVED_NON_TABLE");
        assertTrue(names.contains("STAGINGINVENTORY"), "MERGE USING plain-table source must be captured (correct, once names don't collide)");
        assertFalse(names.contains("INSERTED"), "OUTPUT's 'inserted' pseudo-table must not be miscounted as a real table");
        assertFalse(names.contains("DELETED"), "OUTPUT's 'deleted' pseudo-table must not be miscounted as a real table");
        assertFalse(names.contains("CHANGELOG"), "OUTPUT ... INTO @ChangeLog (a table variable) must not appear as a phantom table op");
    }

    // ── 4: multiple different table hints combined with real joins ───────────

    @Test
    @DisplayName("4a: FROM a WITH (NOLOCK) JOIN b WITH (INDEX(ix1)) ON ... — join EDGE is dropped")
    void auditMultipleDifferentHints() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT a.id, b.val\n" +
                "  FROM Orders a WITH (NOLOCK)\n" +
                "  JOIN OrderLines b WITH (INDEX(ix1)) ON b.order_id = a.id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        List<?> joins = r.getTableOps().stream().flatMap(t -> t.getJoins().stream()).toList();
        System.out.println("[4a] tableOps=" + names + " joins=" + joins + " full=" + r.getTableOps());
        // Both base tables ARE captured (via the independent FROM/JOIN keyword scan in scanOps/
        // addTableOpsFromText), so nothing is lost at the table level.
        assertTrue(names.contains("ORDERS"), "Orders must survive a NOLOCK hint");
        assertTrue(names.contains("ORDERLINES"), "OrderLines must survive an INDEX(...) hint combined with a real JOIN");
        // FIXED: JOIN_PATTERN now consumes an optional "WITH (hint)" clause between the joined
        // table's alias and its ON condition (or the terminating boundary), so a table hint no
        // longer breaks the join-edge match.
        assertFalse(joins.isEmpty(),
                "a table hint between the joined table and its ON clause must not drop the join edge");
        SqlServerJoinInfo j = (SqlServerJoinInfo) joins.get(0);
        assertEquals("OrderLines", j.getJoinedTable());
    }

    @Test
    @DisplayName("4b: same join WITHOUT any hint — join edge IS captured (control case, confirms 4a is the hint's fault)")
    void auditJoinWithoutHintControlCase() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT a.id, b.val\n" +
                "  FROM Orders a\n" +
                "  JOIN OrderLines b ON b.order_id = a.id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<?> joins = r.getTableOps().stream().flatMap(t -> t.getJoins().stream()).toList();
        System.out.println("[4b] joins=" + joins);
        System.out.println("[4b] join edge captured without any hint present (bare JOIN keyword)? " + !joins.isEmpty());

        String sqlInner = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT a.id, b.val\n" +
                "  FROM Orders a\n" +
                "  INNER JOIN OrderLines b ON b.order_id = a.id;\n" +
                "END";
        SqlServerRoutineInfo r2 = parse(sqlInner);
        List<?> joins2 = r2.getTableOps().stream().flatMap(t -> t.getJoins().stream()).toList();
        System.out.println("[4b-inner] INNER JOIN (explicit qualifier) joins=" + joins2);
    }

    // ── 5: sp_executesql actual PARAMETER BINDING (`@p1=5`) ────────────────────

    @Test
    @DisplayName("5a: EXEC sp_executesql @sql, N'@p1 INT', @p1=5 — bound VALUE is never captured (opaque)")
    void auditSpExecuteSqlParamBindingValueOpaque() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  DECLARE @sql NVARCHAR(200) = N'SELECT * FROM Orders WHERE id = @p1';\n" +
                "  EXEC sp_executesql @sql, N'@p1 INT', @p1=5;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        System.out.println("[5a] dynamicSql=" + r.getDynamicSql());
        // Two entries: one SET_SQL from the DECLARE @sql = N'...' literal assignment, one
        // EXEC_SP_EXECUTESQL from the sp_executesql call itself (Bug 8's existing, correct split).
        assertEquals(2, r.getDynamicSql().size());
        SqlServerDynamicSqlInfo execEntry = r.getDynamicSql().stream()
                .filter(d -> d.getKind() == SqlServerDynamicSqlInfo.Kind.EXEC_SP_EXECUTESQL)
                .findFirst().orElseThrow();
        assertNotNull(execEntry.getSqlSource(), "the @sql literal itself IS resolved (Bug 8, already fixed) — not this gap");
        // FIXED: SqlServerDynamicSqlInfo now carries the parameter-signature string (N'@p1 INT')
        // and the bound name=value pairs (@p1=5) that follow it in the EXEC sp_executesql
        // argument list, so a caller can see not just *what* dynamic query ran but *what value(s)*
        // it ran with.
        assertEquals("N'@p1 INT'", execEntry.getParamSignature());
        assertEquals(java.util.Map.of("@p1", "5"), execEntry.getBoundParameters());
    }
}
