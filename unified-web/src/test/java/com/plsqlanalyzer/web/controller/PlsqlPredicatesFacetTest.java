package com.plsqlanalyzer.web.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.web.page.FacetPageResponder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * UI-028 — {@link PlsqlPredicatesFacet}'s port of `plsql/js/predicates.js`'s
 * {@code _buildPredicateData}, and {@link PlsqlScopeFilter}'s predicate-specific scope trim.
 */
class PlsqlPredicatesFacetTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private JsonNode tableWithFilters(String name, String schema, Object[][] filters) {
        // filters: {column, operator, value, procedureId, operation, lineNumber}
        var t = mapper.createObjectNode().put("tableName", name).put("schemaName", schema)
                .put("tableType", "TABLE");
        var details = t.putArray("accessDetails");
        for (Object[] f : filters) {
            var d = details.addObject()
                    .put("procedureId", (String) f[3]).put("procedureName", (String) f[3])
                    .put("operation", (String) f[4]).put("lineNumber", (int) f[5])
                    .put("sourceFile", "x.sql");
            d.putArray("whereFilters").addObject()
                    .put("columnName", (String) f[0]).put("operator", (String) f[1])
                    .put("value", (String) f[2]).put("lineNumber", 0);
        }
        return t;
    }

    private JsonNode tables(JsonNode... ts) {
        var arr = mapper.createArrayNode();
        for (JsonNode t : ts) arr.add(t);
        return arr;
    }

    @Test
    void tablesWithNoWhereFiltersAreDroppedEntirely() {
        var noFilters = mapper.createObjectNode().put("tableName", "EMPTY").put("schemaName", "S");
        noFilters.putArray("accessDetails");
        var out = PlsqlPredicatesFacet.build(mapper, tables(noFilters));
        assertEquals(0, out.size());
    }

    @Test
    void buildsOneRowPerTableWithPredicateCountAndUniqueColumns() {
        var t = tableWithFilters("ORDERS", "S", new Object[][]{
                {"STATUS", "=", "'OPEN'", "PROC_A", "SELECT", 10},
                {"STATUS", "=", "'CLOSED'", "PROC_A", "SELECT", 11},
                {"CUST_ID", "=", "5", "PROC_A", "SELECT", 12},
        });
        var out = PlsqlPredicatesFacet.build(mapper, tables(t));
        assertEquals(1, out.size());
        JsonNode row = out.get(0);
        assertEquals("ORDERS", row.path("tableName").asText());
        assertEquals(3, row.path("predicateCount").asInt(), "one predicate per whereFilter");
        assertEquals(2, row.path("uniqueColumns").asInt(), "STATUS and CUST_ID, STATUS counted once");
    }

    @Test
    void lineNumberFallsBackToTheAccessDetailsLineWhenTheFilterHasNone() {
        // Mirrors the JS: f.lineNumber || d.lineNumber || 0
        var t = mapper.createObjectNode().put("tableName", "T").put("schemaName", "S");
        var d = t.putArray("accessDetails").addObject().put("lineNumber", 42).put("procedureId", "P");
        d.putArray("whereFilters").addObject().put("columnName", "C").put("lineNumber", 0);
        var out = PlsqlPredicatesFacet.build(mapper, tables(t));
        assertEquals(42, out.get(0).path("predicates").get(0).path("lineNumber").asInt());
    }

    @Test
    void pagingAndSearchWorkOverTheBuiltRows() {
        var arr = mapper.createArrayNode();
        for (int i = 0; i < 40; i++) {
            arr.add(tableWithFilters("T" + i, "S", new Object[][]{
                    {"COL" + i, "=", "1", "P" + i, "SELECT", 1}}));
        }
        var built = PlsqlPredicatesFacet.build(mapper, arr);
        var responder = new FacetPageResponder(mapper, 10_000);
        var res = responder.respond(built, null, PlsqlFacetSpecs.PREDICATES,
                new FacetPageResponder.Params(10, null, "NAME_ASC", null, null, null,
                        "application/json"), null);
        assertEquals(10, res.getBody().path("items").size());
        assertTrue(res.getBody().path("page").path("nextCursor").asText("") .length() > 0);
    }

    @Test
    void directScopeTrimsPredicatesAndRecomputesCounts() {
        var t = tableWithFilters("ORDERS", "S", new Object[][]{
                {"STATUS", "=", "'OPEN'", "PKG.PROC_A", "SELECT", 1},
                {"CUST_ID", "=", "5", "PKG.OTHER", "SELECT", 2},
        });
        var built = PlsqlPredicatesFacet.build(mapper, tables(t));
        var out = PlsqlScopeFilter.directPredicates(mapper, built, "PKG.PROC_A");
        assertEquals(1, out.size());
        assertEquals(1, out.get(0).path("predicateCount").asInt());
        assertEquals(1, out.get(0).path("uniqueColumns").asInt());
    }

    @Test
    void subtreeScopeDropsRowsWithNoMatchingPredicateAtAll() {
        var t = tableWithFilters("ORDERS", "S", new Object[][]{
                {"STATUS", "=", "'OPEN'", "UNRELATED_PROC", "SELECT", 1},
        });
        var built = PlsqlPredicatesFacet.build(mapper, tables(t));
        var out = PlsqlScopeFilter.subtreePredicates(mapper, built, Set.of("SOME_OTHER_NODE"));
        assertEquals(0, out.size());
    }
}
