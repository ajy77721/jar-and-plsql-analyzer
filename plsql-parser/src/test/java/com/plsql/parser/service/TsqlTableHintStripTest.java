package com.plsql.parser.service;

import com.plsql.parser.model.ParsedObject;
import com.plsql.parser.model.TableOperationInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sep05-12 — a T-SQL table hint must not cost a statement its entire column detail.
 *
 * <p>{@code FROM t WITH (NOEXPAND)} is rejected outright by JSqlParser, so every column, predicate
 * and join in the statement was lost for the sake of an instruction to the query planner. This was
 * the last remaining SQL Server gap: 12 statements, measured.
 *
 * <p>The risk in the fix is not the hint but the <b>CTE</b>: {@code WITH x AS (…)} begins with the
 * same keyword, and a pattern that ate it would break statements that parse today. Both directions
 * are pinned below.
 */
class TsqlTableHintStripTest {

    @Test
    @DisplayName("a table hint is removed")
    void hintRemoved() {
        // Whitespace is normalised in the comparison: the hint is replaced by a space, and how
        // many spaces are left behind is not something a caller can depend on.
        assertEquals("SELECT a FROM t WHERE a = 1",
                TableOperationEnricher.stripTableHints("SELECT a FROM t WITH (NOLOCK) WHERE a = 1")
                        .replaceAll("\\s+", " ").trim());
    }

    @Test
    @DisplayName("a nested hint is removed whole, leaving no stray paren")
    void nestedHintRemoved() {
        String out = TableOperationEnricher.stripTableHints(
                "SELECT a FROM t WITH (INDEX(ix_a), NOLOCK) WHERE a = 1");
        assertFalse(out.contains(")"), "a half-removed hint is worse than the hint: " + out);
        assertTrue(out.contains("WHERE a = 1"), out);
    }

    @Test
    @DisplayName("a CTE is NOT touched")
    void cteUntouched() {
        // The dangerous false positive. `WITH sales AS (` must survive verbatim.
        String sql = "WITH sales AS (SELECT id FROM erp.SalesOrder) SELECT id FROM sales";
        assertEquals(sql, TableOperationEnricher.stripTableHints(sql));
    }

    @Test
    @DisplayName("the enricher now gets column detail from a hinted statement")
    void hintedStatementYieldsDetail() {
        TableOperationInfo t = new TableOperationInfo();
        t.setTableName("vw_ProductSales");
        t.setSchema("erp");
        t.setOperation("SELECT");
        t.setLine(10);
        t.setSqlText("SELECT @cnt = @cnt + COUNT(*) FROM erp.vw_ProductSales WITH (NOEXPAND)");
        ParsedObject o = new ParsedObject();
        o.setTableOperations(List.of(t));

        var coverage = new TableOperationEnricher()
                .enrich(List.of(o), TableOperationEnricher.Dialect.SQLSERVER);

        assertEquals(1, coverage.parsed(),
                "the hinted statement still fails to parse, so it yields no detail at all");
    }
}
