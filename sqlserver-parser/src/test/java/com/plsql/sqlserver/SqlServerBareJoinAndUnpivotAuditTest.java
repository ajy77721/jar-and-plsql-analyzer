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
 * Follow-up to SqlServerOutputClauseAndApplyAuditTest's test 4b, which noticed but did not
 * assert on a control-case observation: a completely bare "JOIN" keyword (no INNER/LEFT/
 * RIGHT/FULL/CROSS/OUTER qualifier) never produces a join edge at all, even with no hint and
 * no ON-clause complication whatsoever. This file makes that gap explicit, and also checks
 * UNPIVOT (the PIVOT counterpart not covered by the existing PIVOT test).
 */
@DisplayName("AUDIT: SQL Server bare JOIN keyword and UNPIVOT capture")
public class SqlServerBareJoinAndUnpivotAuditTest {

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

    @Test
    @DisplayName("1a: bare 'JOIN' (no INNER/LEFT/RIGHT/FULL/CROSS/OUTER qualifier) — join edge is dropped")
    void auditBareJoinKeywordDropsEdge() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT a.id, b.val\n" +
                "  FROM Orders a\n" +
                "  JOIN OrderLines b ON b.order_id = a.id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        List<?> joins = r.getTableOps().stream().flatMap(t -> t.getJoins().stream()).toList();
        System.out.println("[1a] tableOps=" + names + " joins=" + joins);
        // Root cause: JOIN_PATTERN in SqlServerAnalysisVisitor is
        //   (?i)\b(INNER|LEFT\s+OUTER|LEFT|RIGHT\s+OUTER|RIGHT|FULL\s+OUTER|FULL|CROSS|OUTER)\s+(JOIN|APPLY)...
        // The qualifier group is MANDATORY (no `?`), so a plain "JOIN" with none of those words
        // in front of it can never match this pattern at all -- not "sometimes misses the alias",
        // not "sometimes drops the ON clause" -- it produces literally zero regex matches, for
        // what is (by ANSI SQL convention) the single most common way to spell an inner join:
        // bare "JOIN" is a fully valid, extremely common synonym for "INNER JOIN" and arguably
        // used more often than the explicit "INNER JOIN" form in real-world T-SQL.
        assertTrue(names.contains("ORDERS") && names.contains("ORDERLINES"),
                "both base tables are still captured (via the independent FROM/JOIN keyword table scan)");
        // FIXED: JOIN_PATTERN's qualifier group is now optional when followed by JOIN, and
        // attachJoins defaults the join type to INNER when the qualifier is absent, so a bare
        // JOIN keyword now produces a correct INNER join edge.
        assertFalse(joins.isEmpty(),
                "a bare, unqualified JOIN keyword (equivalent to INNER JOIN) must produce a join edge");
        SqlServerJoinInfo j = (SqlServerJoinInfo) joins.get(0);
        assertEquals("INNER", j.getJoinType(), "a bare JOIN keyword must default to INNER join type");
    }

    @Test
    @DisplayName("1b: control case -- explicit 'INNER JOIN' with otherwise-identical SQL DOES produce a join edge")
    void auditInnerJoinQualifierWorksControlCase() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT a.id, b.val\n" +
                "  FROM Orders a\n" +
                "  INNER JOIN OrderLines b ON b.order_id = a.id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<?> joins = r.getTableOps().stream().flatMap(t -> t.getJoins().stream()).toList();
        System.out.println("[1b] joins=" + joins);
        assertFalse(joins.isEmpty(),
                "confirms 1a's gap is specifically the missing bare-JOIN qualifier, not some other unrelated cause: "
              + "the byte-for-byte same query with 'INNER' spelled out explicitly IS captured correctly");
    }

    @Test
    @DisplayName("1c: chain of two bare JOINs -- both edges dropped, only base tables survive")
    void auditBareJoinChainDropsAllEdges() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT a.id, b.val, c.qty\n" +
                "  FROM Orders a\n" +
                "  JOIN OrderLines b ON b.order_id = a.id\n" +
                "  JOIN Products c ON c.id = b.product_id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        List<?> joins = r.getTableOps().stream().flatMap(t -> t.getJoins().stream()).toList();
        System.out.println("[1c] tableOps=" + names + " joins=" + joins);
        assertTrue(names.contains("ORDERS") && names.contains("ORDERLINES") && names.contains("PRODUCTS"),
                "all three base tables must still be captured");
        // FIXED: both bare-JOIN edges in the chain must now be captured.
        assertEquals(2, joins.size(), "both bare-JOIN edges in a two-hop chain must be captured");
    }

    @Test
    @DisplayName("2a: UNPIVOT does not desync table capture -- confirm no gap (parity with PIVOT)")
    void auditUnpivot() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT id, category, amount\n" +
                "  FROM (SELECT id, A, B, C FROM SalesWide) s\n" +
                "  UNPIVOT (amount FOR category IN (A, B, C)) unpvt;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[2a] tableOps=" + names + " full=" + r.getTableOps());
        assertTrue(names.contains("SALESWIDE"), "SalesWide (inside the UNPIVOT's source subquery) must be captured");
        assertEquals(1, names.stream().filter(n -> n.equals("SALESWIDE")).count(),
                "SalesWide should be captured exactly once, not duplicated/desynced by the UNPIVOT clause");
    }

    @Test
    @DisplayName("2b: PIVOT combined with a real JOIN in the source subquery -- join edge still dropped only if bare JOIN used")
    void auditPivotWithJoinInsideSource() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT *\n" +
                "  FROM (SELECT s.category, s.amount FROM Sales s INNER JOIN Regions r ON r.id = s.region_id) x\n" +
                "  PIVOT (SUM(amount) FOR category IN ([A],[B],[C])) pvt;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        List<?> joins = r.getTableOps().stream().flatMap(t -> t.getJoins().stream()).toList();
        System.out.println("[2b] tableOps=" + names + " joins=" + joins);
        assertTrue(names.contains("SALES") && names.contains("REGIONS"),
                "both tables inside the PIVOT source subquery's JOIN must be captured");
        assertFalse(joins.isEmpty(),
                "an explicitly-qualified INNER JOIN inside a PIVOT's source subquery is still captured as a join edge "
              + "(PIVOT itself does not desync join capture; only the bare-JOIN gap from test 1a applies)");
    }
}
