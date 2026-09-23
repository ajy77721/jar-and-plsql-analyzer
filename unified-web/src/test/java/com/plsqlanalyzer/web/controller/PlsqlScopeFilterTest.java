package com.plsqlanalyzer.web.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * UI-028 — {@link PlsqlScopeFilter} ports `plsql/js/tableOps.js` (and the identical logic in
 * `joins.js`/`cursors.js`) `_applyScopeData`'s two scope modes, checked against the client's own
 * fallback rules rather than an "improved" version of them (the acceptance criterion this session
 * has used throughout: a converted screen renders the same rows).
 */
class PlsqlScopeFilterTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private JsonNode row(String name, String... procIds) {
        var r = mapper.createObjectNode().put("tableName", name).put("accessCount", procIds.length);
        var details = r.putArray("accessDetails");
        for (String p : procIds) details.addObject().put("procedureId", p).put("procedureName", p);
        return r;
    }

    private JsonNode rows(JsonNode... rs) {
        var arr = mapper.createArrayNode();
        for (JsonNode r : rs) arr.add(r);
        return arr;
    }

    @Test
    void directKeepsOnlyMatchingRowsAndTrimsTheirAccessDetails() {
        var arr = rows(
                row("ORDERS", "PKG.PLACE_ORDER", "PKG.OTHER_PROC"),
                row("CUSTOMERS", "PKG.OTHER_PROC"));
        var out = PlsqlScopeFilter.direct(mapper, arr, "PKG.PLACE_ORDER");
        assertEquals(1, out.size(), "CUSTOMERS has no matching access and must be dropped");
        assertEquals("ORDERS", out.get(0).path("tableName").asText());
        assertEquals(1, out.get(0).path("accessDetails").size(),
                "the non-matching access detail on the kept row must be trimmed too");
        assertEquals(1, out.get(0).path("accessCount").asInt(), "accessCount must be recomputed");
    }

    @Test
    void directFallsBackToBareNameWhenTheGivenIdHasNoPackage() {
        // The client's own fallback: `pid === currentLast` when currentProcId has no '.'.
        var arr = rows(row("ORDERS", "STANDALONE_PROC"));
        var out = PlsqlScopeFilter.direct(mapper, arr, "STANDALONE_PROC");
        assertEquals(1, out.size());
    }

    @Test
    void directMatchesByPackageAndNameSuffixEvenWithADifferentSchemaPrefix() {
        // The client's own fallback: pid.endsWith('.' + pkg + '.' + last). A fully-qualified
        // access-detail id (SCHEMA.PKG.PROC) must still match a bare PKG.PROC selection.
        var arr = rows(row("ORDERS", "SCHEMA.PKG.PLACE_ORDER"));
        var out = PlsqlScopeFilter.direct(mapper, arr, "PKG.PLACE_ORDER");
        assertEquals(1, out.size());
    }

    @Test
    void directDropsRowsWithNoMatchingAccessAtAll() {
        var arr = rows(row("ORDERS", "PKG.SOMETHING_ELSE"));
        var out = PlsqlScopeFilter.direct(mapper, arr, "PKG.PLACE_ORDER");
        assertEquals(0, out.size());
    }

    @Test
    void directCarriesTheFullBadgeFieldsForTheUiToShowNOfFull() {
        var arr = rows(row("ORDERS", "PKG.PLACE_ORDER", "PKG.OTHER"));
        var out = PlsqlScopeFilter.direct(mapper, arr, "PKG.PLACE_ORDER");
        assertEquals(2, out.get(0).path("_fullAccessCount").asInt());
        assertEquals(2, out.get(0).path("_fullAccessDetails").size());
    }

    @Test
    void subtreeMatchesExactNodeIdsAndFuzzySubstringsEitherDirection() {
        var arr = rows(
                row("ORDERS", "EXACT_MATCH"),
                row("ITEMS", "SOMETHING_CONTAINS_FUZZY_ID_TOO"),
                row("UNRELATED", "NO_MATCH_HERE"));
        var out = PlsqlScopeFilter.subtree(mapper, arr, Set.of("EXACT_MATCH", "FUZZY_ID"));
        var names = new java.util.HashSet<String>();
        out.forEach(r -> names.add(r.path("tableName").asText()));
        assertTrue(names.contains("ORDERS"), "exact node-id match must be kept");
        assertTrue(names.contains("ITEMS"),
                "a procedure id containing a node id (fuzzy match) must be kept, matching the client's own fallback");
        assertFalse(names.contains("UNRELATED"));
    }

    @Test
    void subtreeWithNoNodeIdsReturnsEmptyRatherThanEverything() {
        var arr = rows(row("ORDERS", "ANY_PROC"));
        var out = PlsqlScopeFilter.subtree(mapper, arr, Set.of());
        assertEquals(0, out.size(),
                "an empty node-id set must produce no rows, not fall through to unscoped");
    }
}
