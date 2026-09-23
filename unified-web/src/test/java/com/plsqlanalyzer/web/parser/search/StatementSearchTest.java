package com.plsqlanalyzer.web.parser.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The query language and the matcher (§B13).
 *
 * <p>The motivating question is a real one:
 * <i>"is there any UPDATE on this table for this field in this proc"</i>. Everything here exists to
 * make that answerable in one query and, just as importantly, to make a <b>wrong</b> query say so
 * rather than return an empty page that reads as "no such data".
 */
class StatementSearchTest {

    private static final ObjectMapper M = new ObjectMapper();

    /** A small fixture shaped exactly like api/statements/index.json rows. */
    private JsonNode rows() throws Exception {
        return M.readTree("""
            [
              {"nodeId":"ERP$USP_ALLOCATE","nodeName":"USP_ALLOCATE","schema":"ERP",
               "tableName":"ERP_INVENTORY","operation":"UPDATE","line":120,
               "sqlText":"UPDATE erp_inventory SET qty_allocated = qty_allocated + :n WHERE id = :i",
               "columns":["QTY_ALLOCATED","ID"],"columnsKnown":true},
              {"nodeId":"ERP$USP_ALLOCATE","nodeName":"USP_ALLOCATE","schema":"ERP",
               "tableName":"ERP_INVENTORY","operation":"SELECT","line":100,
               "sqlText":"SELECT qty_on_hand FROM erp_inventory WHERE id = :i",
               "columns":["QTY_ON_HAND","ID"],"columnsKnown":true},
              {"nodeId":"ERP$USP_SHIP","nodeName":"USP_SHIP","schema":"ERP",
               "tableName":"ERP_INVENTORY","operation":"UPDATE","line":40,
               "sqlText":"UPDATE erp_inventory SET qty_on_hand = qty_on_hand - :n",
               "columns":["QTY_ON_HAND"],"columnsKnown":true},
              {"nodeId":"ERP$USP_AUDIT","nodeName":"USP_AUDIT","schema":"ERP",
               "tableName":"ERP_AUDIT_LOG","operation":"INSERT","line":9,
               "sqlText":"INSERT INTO erp_audit_log (id, note) VALUES (s.NEXTVAL, :n)",
               "columns":["ID","NOTE"],"columnsKnown":true},
              {"nodeId":"LEG$OLD_PROC","nodeName":"OLD_PROC","schema":"LEG",
               "tableName":"ERP_INVENTORY","operation":"UPDATE","line":7,
               "columns":[],"columnsKnown":false}
            ]""");
    }

    private StatementSearcher.Result run(String q) throws Exception {
        return StatementSearcher.search(rows(), SearchQuery.parse(q), 50);
    }

    /* ---- the question this feature exists for ---- */

    @Test
    @DisplayName("the motivating question: an UPDATE on a table for a column in a proc")
    void theMotivatingQuestion() throws Exception {
        var r = run("op:UPDATE table:ERP_INVENTORY col:QTY_ALLOCATED proc:USP_ALLOCATE");
        assertEquals(1, r.total(), "exactly one statement answers this");
        var hit = r.hits().get(0);
        assertEquals(120, hit.row().get("line").asInt());
        assertEquals("USP_ALLOCATE", hit.row().get("nodeName").asText());
        // Which column answered — with many columns on a statement, "it matched" is not an answer.
        assertTrue(hit.matchedColumns().contains("QTY_ALLOCATED"), hit.matchedColumns().toString());
    }

    @Test
    @DisplayName("different qualifiers AND, so each one narrows")
    void qualifiersAnd() throws Exception {
        assertEquals(3, run("table:ERP_INVENTORY op:UPDATE").total());
        assertEquals(2, run("table:ERP_INVENTORY op:UPDATE schema:ERP").total());
        assertEquals(1, run("table:ERP_INVENTORY op:UPDATE proc:USP_SHIP").total());
    }

    @Test
    @DisplayName("the same qualifier repeated ORs, because that is what naming two of something means")
    void sameQualifierOrs() throws Exception {
        // AND-ing these would always be zero — a language that punishes the obvious reading.
        var r = run("op:UPDATE op:INSERT");
        assertEquals(4, r.total(), "3 UPDATEs + 1 INSERT");
    }

    @Test
    @DisplayName("! excludes")
    void negation() throws Exception {
        assertEquals(1, run("table:ERP_INVENTORY op:UPDATE !schema:LEG proc:USP_ALLOCATE").total());
        assertEquals(2, run("table:ERP_INVENTORY op:UPDATE !schema:LEG").total());
    }

    @Test
    @DisplayName("trailing * is a prefix match, and is the only wildcard")
    void prefixOnly() throws Exception {
        assertEquals(3, run("col:QTY*").total(), "QTY_ALLOCATED and QTY_ON_HAND");
        // A regex is not a wildcard here: it would be a DoS surface over 5,000 sqlText blobs.
        assertEquals(0, run("col:QTY.*").total(), "'.' is literal, not a regex");
    }

    @Test
    @DisplayName("quoted phrases stay one term")
    void quotedPhrase() throws Exception {
        assertEquals(1, run("sql:\"qty_allocated + :n\"").total());
    }

    @Test
    @DisplayName("bare words search what the user can see")
    void bareWords() throws Exception {
        assertEquals(4, run("ERP_INVENTORY").total());
        assertEquals(1, run("ERP_INVENTORY USP_SHIP").total(), "bare words AND");
        // Not the opaque nodeId: a hit with nothing on screen to explain it is worse than a miss.
        assertEquals(0, run("ERP$USP_SHIP").total());
    }

    /* ---- being wrong loudly ---- */

    @Test
    @DisplayName("a mistyped qualifier is an error, not an empty result")
    void unknownQualifierIsReported() {
        var q = SearchQuery.parse("tabel:ERP_INVENTORY");
        assertFalse(q.errors().isEmpty(), "a typo must not be searched as literal text");
        assertTrue(q.errors().get(0).contains("tabel"));
        assertTrue(q.errors().get(0).contains("table"), "the message must list what IS valid");
    }

    @Test
    @DisplayName("text containing a colon is text, not a bad qualifier")
    void colonInTextIsNotAQualifier() throws Exception {
        // The point is to catch "tabel:", not to reject everything with a colon in it.
        var q = SearchQuery.parse("ANALYZERDB.ERP_ORDER:5");
        assertTrue(q.errors().isEmpty(), q.errors().toString());
        assertEquals(1, q.terms().size());
        assertTrue(q.terms().get(0).isBare());
    }

    @Test
    @DisplayName("common aliases are accepted rather than rejected")
    void aliases() throws Exception {
        assertEquals(run("table:ERP_INVENTORY").total(), run("t:ERP_INVENTORY").total());
        assertEquals(run("col:ID").total(), run("column:ID").total());
        assertEquals(run("proc:USP_SHIP").total(), run("procedure:USP_SHIP").total());
    }

    @Test
    @DisplayName("the query is echoed back as it was understood")
    void echoesInterpretation() {
        var q = SearchQuery.parse("op:UPDATE !schema:LEG QTY*");
        assertEquals(java.util.List.of("op UPDATE", "NOT schema LEG", "any QTY*"), q.echo());
    }

    /* ---- counting honestly ---- */

    @Test
    @DisplayName("total counts every match, not the returned page")
    void totalIsNotPageSize() throws Exception {
        var r = StatementSearcher.search(rows(), SearchQuery.parse("table:ERP_INVENTORY"), 2);
        assertEquals(2, r.hits().size(), "the page is capped");
        assertEquals(4, r.total(), "the total must be every match — a modal saying '4' shows 2");
    }

    @Test
    @DisplayName("facets count over every match too")
    void facetsCountAllMatches() throws Exception {
        var r = StatementSearcher.search(rows(), SearchQuery.parse("table:ERP_INVENTORY"), 1);
        assertEquals(3, r.facets().get("operation").get("UPDATE"));
        assertEquals(1, r.facets().get("operation").get("SELECT"));
        assertEquals(4, r.facets().get("tableName").get("ERP.ERP_INVENTORY")
                      + r.facets().get("tableName").getOrDefault("LEG.ERP_INVENTORY", 0));
    }

    @Test
    @DisplayName("a row whose columns were never captured still matches on what IS known")
    void unindexedColumnsDoNotHideTheStatement() throws Exception {
        // LEG$OLD_PROC predates column attribution. Dropping it would shrink the answer to
        // "no UPDATE from LEG", which is false.
        var r = run("op:UPDATE schema:LEG");
        assertEquals(1, r.total());
        assertFalse(r.hits().get(0).row().get("columnsKnown").asBoolean(),
                "and the row must say its columns are unknown, not empty");
    }

    @Test
    @DisplayName("an empty query matches nothing rather than everything")
    void emptyQueryIsNotSelectAll() throws Exception {
        assertTrue(SearchQuery.parse("").isEmpty());
        assertTrue(SearchQuery.parse("   ").isEmpty());
        assertTrue(SearchQuery.parse(null).isEmpty());
    }

    @Test
    @DisplayName("queries are case-insensitive and tolerate padding")
    void caseAndPadding() throws Exception {
        assertEquals(run("op:UPDATE").total(), run("  op:update  ").total());
        assertEquals(3, run("op:update").total());
    }

    /**
     * MID-008 (redefined DoD, §114/§117) — the facet/total walk scans every row regardless of
     * {@code limit}; below {@code maxScanned} it must behave exactly as before (this row's own
     * fixture has 4 rows, so a real, generous cap must not touch it at all).
     */
    @Test
    @DisplayName("below the scan cap, nothing changes")
    void underTheScanCapBehavesExactlyAsBefore() throws Exception {
        var r = StatementSearcher.search(rows(), SearchQuery.parse("op:UPDATE"), 50,
                StatementSearcher.DEFAULT_MAX_SCANNED);
        assertEquals(3, r.total());
        assertFalse(r.scanLimitReached());
    }

    /**
     * Calibrated by planting a {@code maxScanned} of 2 against the 4-row fixture: the query
     * (`op:UPDATE`, 3 real matches at rows 1, 3, and one more) must undercount once the cap cuts
     * the walk short, and the response must say so — a truncated result silently presented as
     * complete would be the same defect MID-008's original wording was written to catch, one layer
     * down.
     */
    @Test
    @DisplayName("above the scan cap, the walk stops and says so")
    void aboveTheScanCapStopsAndReportsIt() throws Exception {
        var full = StatementSearcher.search(rows(), SearchQuery.parse("op:UPDATE"), 50,
                StatementSearcher.DEFAULT_MAX_SCANNED);
        var capped = StatementSearcher.search(rows(), SearchQuery.parse("op:UPDATE"), 50, 2);

        assertFalse(full.scanLimitReached());
        assertTrue(capped.scanLimitReached(), "the cap must be observable, not silently applied");
        assertTrue(capped.total() < full.total(),
                "a 2-row scan cap over a 4-row fixture must undercount the true 3-row total");
    }
}
