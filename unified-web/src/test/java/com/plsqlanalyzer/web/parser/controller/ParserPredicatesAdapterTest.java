package com.plsqlanalyzer.web.parser.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MID-021 (predicates.js's own paged-conversion project) — {@code ParserPredicatesAdapter} is the
 * server-side twin of {@code PA.api._adaptParserTableRow}'s own §138 fix. Pinning the line-matching
 * behaviour directly, the same way `plsql-tableops-parser-shape-bug.test.mjs` pins the client half.
 */
class ParserPredicatesAdapterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void attachesEachPredicateOnlyToItsOwnLine() throws Exception {
        JsonNode raw = MAPPER.readTree("{\"tables\":[{"
                + "\"name\":\"REF_REGION\",\"schema\":\"ANALYZER_REF\",\"objectType\":\"TABLE\","
                + "\"allOperations\":[\"SELECT\"],"
                + "\"usedBy\":[{\"nodeId\":\"ERP$FN_REGION_NAME\",\"nodeName\":\"FN_REGION_NAME\","
                + "\"operations\":{\"SELECT\":[7,20]},"
                + "\"predicates\":[{\"columnName\":\"REGION_ID\",\"operator\":\"=\",\"value\":\"p_id\",\"lineNumber\":7}]}]"
                + "}]}");

        ArrayNode adapted = ParserPredicatesAdapter.adaptTables(MAPPER, raw);
        assertEquals(1, adapted.size());
        JsonNode row = adapted.get(0);
        assertEquals("REF_REGION", row.get("tableName").asText());
        assertEquals("ANALYZER_REF", row.get("schemaName").asText());

        ArrayNode details = (ArrayNode) row.get("accessDetails");
        assertEquals(2, details.size(), "one accessDetails entry per line, not per predicate");

        JsonNode line7 = null, line20 = null;
        for (JsonNode d : details) {
            if (d.get("lineNumber").asInt() == 7) line7 = d;
            if (d.get("lineNumber").asInt() == 20) line20 = d;
        }
        assertEquals(1, line7.get("whereFilters").size(), "line 7 has its own predicate attached");
        assertEquals("REGION_ID", line7.get("whereFilters").get(0).get("columnName").asText());
        assertTrue(line20.get("whereFilters").isEmpty(), "line 20 has no predicate — must not inherit line 7's");
    }

    /**
     * Call-order regression (the group-by-identity bug class): {@code usedBy[].operations} is stored
     * grouped by operation name, so a naive walk emits all of one operation's lines before the next
     * operation's — clustering by operation instead of following the routine's real execution order.
     *
     * <p>Confirmed against the real corpus before this fix: 1371 of the 2220 {@code usedBy} entries
     * that touch more than one operation emitted their access lines out of true line order. Pattern
     * pinned here is the proven one — a repeated operation interleaved with distinct others:
     * SELECT@5, INSERT@10, SELECT@15, DELETE@20 must come back 5, 10, 15, 20.
     */
    @Test
    void emitsAccessDetailsInTrueLineOrderNotGroupedByOperation() throws Exception {
        JsonNode raw = MAPPER.readTree("{\"tables\":[{"
                + "\"name\":\"ERP_AUDIT_LOG\",\"schema\":\"ANALYZER\",\"objectType\":\"TABLE\","
                + "\"usedBy\":[{\"nodeId\":\"ERP$USP_RUN\",\"nodeName\":\"USP_RUN\","
                + "\"operations\":{\"SELECT\":[5,15],\"INSERT\":[10],\"DELETE\":[20]},"
                + "\"predicates\":[]}]"
                + "}]}");

        ArrayNode details = (ArrayNode) ParserPredicatesAdapter.adaptTables(MAPPER, raw)
                .get(0).get("accessDetails");

        StringBuilder actual = new StringBuilder();
        for (JsonNode d : details) {
            if (actual.length() > 0) actual.append(", ");
            actual.append(d.get("operation").asText()).append('@').append(d.get("lineNumber").asInt());
        }

        assertEquals("SELECT@5, INSERT@10, SELECT@15, DELETE@20", actual.toString(),
                "accesses must follow true source line order across ALL operations; grouping by "
                        + "operation would give SELECT@5, SELECT@15, INSERT@10, DELETE@20");
    }
}
