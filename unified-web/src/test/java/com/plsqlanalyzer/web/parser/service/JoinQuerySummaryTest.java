package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * UI-013 §60/§62/§64 — {@link JoinQuerySummary}, checked against
 * {@code parser/js/joinSummary.js:_buildQueryItems}, the client logic it replaces.
 */
class JoinQuerySummaryTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private JoinQuerySummary.Config cfg() {
        return JoinQuerySummary.Config.defaults();
    }

    private JsonNode raw(String json) throws Exception {
        return mapper.readTree(json);
    }

    @Test
    void joinsWithTheSameQueryIdFromDifferentPairsGroupIntoOneRow() throws Exception {
        // Two different table pairs, same procedure, same queryId — one query, two joins.
        JsonNode raw = raw("""
            [
              {"leftTable": "orders", "rightTable": "customers",
               "accessDetails": [{"procedureId": "P1", "procedureName": "GetOrder",
                                   "queryId": "q1", "lineNumber": 10, "joinType": "INNER",
                                   "onPredicate": "a.id = b.id"}]},
              {"leftTable": "orders", "rightTable": "items",
               "accessDetails": [{"procedureId": "P1", "procedureName": "GetOrder",
                                   "queryId": "q1", "lineNumber": 12, "joinType": "LEFT",
                                   "onPredicate": "a.id = c.order_id"}]}
            ]""");

        ArrayNode items = JoinQuerySummary.build(mapper, raw, cfg());
        assertEquals(1, items.size());
        JsonNode item = items.get(0);
        assertEquals(2, item.path("joinCount").asInt());
        assertEquals(3, item.path("tableCount").asInt()); // ORDERS, CUSTOMERS, ITEMS
        assertEquals(10, item.path("lineNumber").asInt());
        assertEquals(12, item.path("lineEnd").asInt());
        assertEquals(3, item.path("lineSpan").asInt());
    }

    @Test
    void differentQueryIdsInTheSameProcedureStayInSeparateRows() throws Exception {
        // Same procedure, two different SELECTs (queryId q1 vs q2) — must not be mixed, exactly
        // the bug the queryId grouping key exists to avoid.
        JsonNode raw = raw("""
            [
              {"leftTable": "a", "rightTable": "b",
               "accessDetails": [
                 {"procedureId": "P1", "procedureName": "X", "queryId": "q1", "lineNumber": 5, "joinType": "INNER", "onPredicate": "x"},
                 {"procedureId": "P1", "procedureName": "X", "queryId": "q2", "lineNumber": 40, "joinType": "INNER", "onPredicate": "y"}
               ]}
            ]""");

        ArrayNode items = JoinQuerySummary.build(mapper, raw, cfg());
        assertEquals(2, items.size());
    }

    @Test
    void legacyDataWithoutQueryIdFallsBackToA50LineBucket() throws Exception {
        // No queryId at all: lines 5 and 20 fall in bucket 0 (floor(line/50)), lines 60 and 70
        // fall in bucket 1 — matching `procId + '::b' + Math.floor(line / 50)`.
        JsonNode raw = raw("""
            [
              {"leftTable": "a", "rightTable": "b",
               "accessDetails": [
                 {"procedureId": "P1", "procedureName": "X", "lineNumber": 5, "joinType": "INNER", "onPredicate": "x"},
                 {"procedureId": "P1", "procedureName": "X", "lineNumber": 20, "joinType": "INNER", "onPredicate": "y"},
                 {"procedureId": "P1", "procedureName": "X", "lineNumber": 60, "joinType": "INNER", "onPredicate": "z"}
               ]}
            ]""");

        ArrayNode items = JoinQuerySummary.build(mapper, raw, cfg());
        assertEquals(2, items.size());
        int total = 0;
        for (JsonNode item : items) total += item.path("joinCount").asInt();
        assertEquals(3, total);
    }

    @Test
    void complexityIsTheMaxAcrossAllJoinsInTheGroupNotTheSum() throws Exception {
        // One plain INNER join (score 1, base only) and one LEFT join with 3 predicates
        // (base 1 + outerJoinPenalty 0.5 + highPredBonus 1 = 2.5) — the group's complexity must be
        // the max of the two (2.5), not their sum.
        JsonNode raw = raw("""
            [
              {"leftTable": "a", "rightTable": "b",
               "accessDetails": [
                 {"procedureId": "P1", "procedureName": "X", "queryId": "q1", "lineNumber": 1,
                  "joinType": "INNER", "onPredicate": "a.id = b.id"},
                 {"procedureId": "P1", "procedureName": "X", "queryId": "q1", "lineNumber": 2,
                  "joinType": "LEFT", "onPredicate": "a.x = b.x AND a.y = b.y AND a.z = b.z"}
               ]}
            ]""");

        ArrayNode items = JoinQuerySummary.build(mapper, raw, cfg());
        assertEquals(1, items.size());
        assertEquals(2.5, items.get(0).path("complexity").asDouble(), 0.0001);
        assertEquals("MEDIUM", items.get(0).path("complexityLabel").asText());
    }

    @Test
    void aPairWithNoAccessDetailsContributesNothing() throws Exception {
        JsonNode raw = raw("""
            [{"leftTable": "a", "rightTable": "b"}]""");
        ArrayNode items = JoinQuerySummary.build(mapper, raw, cfg());
        assertTrue(items.isEmpty());
    }

    @Test
    void emptyOrNullInputProducesAnEmptyArray() {
        assertTrue(JoinQuerySummary.build(mapper, null, cfg()).isEmpty());
        assertTrue(JoinQuerySummary.build(mapper, mapper.createArrayNode(), cfg()).isEmpty());
    }

    @Test
    void procedureNameIsUsedAsTheGroupingFallbackWhenProcedureIdIsAbsent() throws Exception {
        // `d.procedureId || d.procedureName` — two entries with no procedureId but the same
        // procedureName and queryId must still group together.
        JsonNode raw = raw("""
            [{"leftTable": "a", "rightTable": "b",
              "accessDetails": [
                {"procedureName": "OnlyName", "queryId": "q1", "lineNumber": 1, "joinType": "INNER", "onPredicate": "x"},
                {"procedureName": "OnlyName", "queryId": "q1", "lineNumber": 2, "joinType": "INNER", "onPredicate": "y"}
              ]}]""");
        ArrayNode items = JoinQuerySummary.build(mapper, raw, cfg());
        assertEquals(1, items.size());
        assertEquals(2, items.get(0).path("joinCount").asInt());
    }
}
