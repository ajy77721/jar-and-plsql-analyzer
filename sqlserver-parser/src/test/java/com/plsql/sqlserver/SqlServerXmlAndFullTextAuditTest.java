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
 * Concepts audited:
 *   1. XML data-type method calls: col.value(...) / col.exist(...) / col.nodes(...) / col.query(...)
 *      -- dot-method-call syntax that is unusual for T-SQL (looks like an OOP method call, or like
 *      a schema-qualified routine call to QUALIFIED_CALL_PATTERN).
 *   2. Full-text search predicates: CONTAINS(...) / FREETEXT(...) / CONTAINSTABLE(...) in a JOIN.
 */
@DisplayName("AUDIT: SQL Server XML method calls and full-text search predicates")
public class SqlServerXmlAndFullTextAuditTest {

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

    private List<String> callNames(SqlServerRoutineInfo r) {
        return r.getCalls().stream()
                .map(c -> (c.getSchema() != null ? c.getSchema() + "." : "") + c.getName())
                .toList();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Concept 1: XML data type method calls
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("1a: xml_col.value(...) / xml_col.exist(...) in SELECT+WHERE -- FROM table is fine, "
            + "but the XML methods are misread as phantom outbound calls")
    void auditXmlValueAndExistMethods() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT id, xml_col.value('(/Root/Name)[1]', 'VARCHAR(100)') AS name\n" +
                "  FROM orders\n" +
                "  WHERE xml_col.exist('/Root/Item') = 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        List<String> calls = callNames(r);
        System.out.println("[1a] tableOps=" + names + " calls=" + calls);

        // Expected/correct: the real base table ORDERS is captured, and there must be NO outbound
        // call recorded for "xml_col.value" / "xml_col.exist" -- these are XML data-type methods on
        // a column, not calls to a schema-qualified routine.
        assertTrue(names.contains("ORDERS"), "the real FROM table must still be captured");

        // FIXED: QUALIFIED_CALL_PATTERN now excludes the fixed set of built-in XML data-type
        // methods (value/exist/query/nodes) regardless of qualifier, so neither "xml_col.value("
        // nor "xml_col.exist(" is recorded as a phantom outbound call.
        assertFalse(calls.contains("xml_col.value") || calls.contains("xml_col.exist"),
                "FIXED: XML methods are no longer captured as phantom outbound calls");
    }

    @Test
    @DisplayName("1b: t.c.value(...) with CROSS APPLY xml_col.nodes(...) AS t(c) -- stacked risky constructs")
    void auditXmlNodesWithCrossApplyColumnAlias() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT t.c.value('.', 'INT')\n" +
                "  FROM orders\n" +
                "  CROSS APPLY xml_col.nodes('/Root/Item') AS t(c);\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        List<?> joins = r.getTableOps().stream().flatMap(t -> t.getJoins().stream()).toList();
        List<String> calls = callNames(r);
        System.out.println("[1b] tableOps=" + names + " joins=" + joins + " calls=" + calls);

        // Correct/expected: ORDERS captured as base table, and a CROSS APPLY join edge exists
        // linking ORDERS to the xml_col.nodes(...) row-set expansion (even if the joined "table"
        // name is imperfect, the join edge itself -- the CROSS APPLY -- should not be silently
        // dropped, matching the previously-fixed TVF-in-APPLY pattern elsewhere in this file).
        assertTrue(names.contains("ORDERS"), "the real FROM table must still be captured");

        // FIXED: the CROSS APPLY's "AS t(c)" alias is a table alias immediately followed by a
        // column-list "(c)" with NO whitespace in between. JOIN_PATTERN's alias group now accepts
        // and discards an optional trailing "(col-list)" right after the alias, so the CROSS APPLY
        // clause matches and a join edge is recorded instead of being silently dropped.
        assertFalse(joins.isEmpty(),
                "FIXED: CROSS APPLY xml_col.nodes(...) AS t(c) now produces a join edge despite the "
              + "immediately-following column-list alias t(c)");

        // Also documents the QUALIFIED_CALL_PATTERN phantom-call side effect on "c.value(" and
        // "xml_col.nodes(" (same class of bug as 1a, now doubly confirmed).
        System.out.println("[1b] phantom calls observed: " + calls);
    }

    @Test
    @DisplayName("1c: .query('/Root') XML method -- also captured as a phantom outbound call")
    void auditXmlQueryMethod() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT id, xml_col.query('/Root') AS fragment\n" +
                "  FROM orders;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        List<String> calls = callNames(r);
        System.out.println("[1c] tableOps=" + names + " calls=" + calls);
        assertTrue(names.contains("ORDERS"), "the real FROM table must still be captured");
        assertFalse(calls.contains("xml_col.query"),
                "FIXED: .query(...) is no longer captured as a phantom outbound call");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Concept 2: Full-text search predicates
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("2a: CONTAINS(body, 'SQL AND performance') -- SQL-keyword-shaped search string does "
            + "NOT break FROM-clause detection")
    void auditContainsPredicateWithSqlLikeSearchString() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM articles WHERE CONTAINS(body, 'SQL AND performance');\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[2a] tableOps=" + names);
        // Correct/expected AND observed: articles is captured cleanly, the 'AND' inside the quoted
        // search term text is never parsed as a SQL keyword because it never reaches a regex whose
        // scope includes FROM/JOIN table detection.
        assertEquals(1, names.stream().filter(n -> n.equals("ARTICLES")).count(),
                "ARTICLES must be captured exactly once; the search string's embedded 'AND' must not desync it");
    }

    @Test
    @DisplayName("2b: FREETEXT(body, 'database tuning tips') -- no false table/keyword capture")
    void auditFreetextPredicate() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM articles WHERE FREETEXT(body, 'database tuning tips');\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[2b] tableOps=" + names);
        assertEquals(List.of("ARTICLES"), names, "only the real base table ARTICLES should be captured");
    }

    @Test
    @DisplayName("2c: CONTAINSTABLE(...) as a JOIN source -- handled the same as other TVF-in-JOIN cases")
    void auditContainstableInJoin() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT a.*, k.RANK\n" +
                "  FROM articles a\n" +
                "  INNER JOIN CONTAINSTABLE(articles, body, 'SQL') AS k ON a.id = k.[KEY];\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        List<?> joins = r.getTableOps().stream().flatMap(t -> t.getJoins().stream()).toList();
        System.out.println("[2c] tableOps=" + names + " joins=" + joins);

        // Correct/expected AND observed: ARTICLES is captured as the base table, and the
        // CONTAINSTABLE(...) TVF-shaped source in the JOIN is captured as a join edge (not dropped),
        // consistent with the earlier APPLY/TVF-in-JOIN fixes elsewhere in this pattern (the argument
        // list `(articles, body, 'SQL')` is consumed by the same "optional TVF call argument list"
        // group that fixed CROSS/OUTER APPLY of a parameterised TVF).
        assertTrue(names.contains("ARTICLES"), "the real base table ARTICLES must be captured");
        assertFalse(joins.isEmpty(),
                "CONTAINSTABLE(...) in an explicit INNER JOIN must produce a join edge, matching prior TVF-in-JOIN fixes");
        SqlServerJoinInfo j = (SqlServerJoinInfo) joins.get(0);
        System.out.println("[2c] join detail: type=" + j.getJoinType() + " joinedTable=" + j.getJoinedTable()
                + " alias=" + j.getJoinedTableAlias() + " condition=" + j.getCondition());
        assertEquals("CONTAINSTABLE", j.getJoinedTable() == null ? null : j.getJoinedTable().toUpperCase(),
                "joined 'table' is CONTAINSTABLE itself (a TVF-shaped system predicate, same phantom-table "
              + "class as any other TVF-in-JOIN, not a new/distinct gap for this construct)");
    }
}
