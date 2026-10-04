package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FE-012 — {@code /tables/list} returns table rows, not access rows.
 *
 * <h2>What this replaces</h2>
 *
 * <p>The left panel renders a name, a row of operation badges and a count. To get them the browser
 * called {@code getTableOperations()}, which flat-maps {@code usedBy[].operations} into one object
 * per access <em>line</em> — roughly a million at the stated target — and then discards every one.
 *
 * <p>Measured on the real ORACLE artifact in {@code data/}: the full document is 148.4 KB and the
 * projected list is 4.9 KB, a 30× reduction, and the 2,435 access-line objects the client used to
 * build become a single integer per table.
 *
 * <h2>Where the risk actually is</h2>
 *
 * <p>Not in the size — in the count. {@code accessCount} is now accumulated by a hand-written token
 * walk rather than by measuring a materialised array, so the cases below are the ones where a
 * streaming parser silently drifts from the tree it replaced: several operations per node, several
 * nodes per table, empty line arrays, and unexpected fields sitting next to the ones being read.
 */
class TableFacetListTest {

    private static final String ANALYSIS = "ORACLE~P~DEV~DEMO";

    private final InMemoryArtifactStore store = new InMemoryArtifactStore();
    private final StaticAnalysisReader reader = new StaticAnalysisReader(store);

    private void tablesIndex(String json) throws IOException {
        ArtifactKey key = AnalysisPaths.resolveKey(ANALYSIS, "api/tables/index.json");
        try (OutputStream out = store.write(key)) {
            out.write(json.getBytes(StandardCharsets.UTF_8));
        }
    }

    private JsonNode rowsOf(JsonNode doc) {
        return doc.path("tables");
    }

    @Test
    void countsEveryAccessLineAcrossOperationsAndNodes() throws IOException {
        // 3 + 1 lines from the first node, 2 from the second: six access lines for one table.
        tablesIndex("""
                {"totalTables":1,"tables":[
                  {"name":"ORDERS","schema":"APP","objectType":"TABLE",
                   "allOperations":["SELECT","INSERT"],
                   "usedBy":[
                     {"nodeId":"n1","nodeName":"P1","operations":{"SELECT":[1,2,3],"INSERT":[4]}},
                     {"nodeId":"n2","nodeName":"P2","operations":{"SELECT":[9,10]}}]}]}""");

        JsonNode rows = rowsOf(reader.getTablesList(ANALYSIS));
        assertEquals(1, rows.size());
        JsonNode t = rows.get(0);
        assertEquals("ORDERS", t.path("name").asText());
        assertEquals("APP", t.path("schema").asText());
        assertEquals("TABLE", t.path("objectType").asText());
        assertEquals(6, t.path("accessCount").asLong(), "access lines were miscounted");
        assertEquals("[\"SELECT\",\"INSERT\"]", t.path("operations").toString());
    }

    @Test
    void doesNotCarryTheAccessLinesItCounted() throws IOException {
        // The whole point of the endpoint. If usedBy survives into the response, the response still
        // scales with access lines and nothing was gained.
        tablesIndex("""
                {"tables":[
                  {"name":"ORDERS","schema":"APP","allOperations":["SELECT"],
                   "usedBy":[{"nodeId":"n1","nodeName":"P1","operations":{"SELECT":[1,2]}}],
                   "dbColumns":[{"name":"ID"}],"dbIndexes":[{"name":"PK"}]}]}""");

        JsonNode t = rowsOf(reader.getTablesList(ANALYSIS)).get(0);
        assertFalse(t.has("usedBy"), "the access lines are still in the response");
        assertFalse(t.has("dbColumns"), "column metadata is still in the response");
        assertFalse(t.has("dbIndexes"), "index metadata is still in the response");
        assertEquals(2, t.path("accessCount").asLong());
    }

    @Test
    void hidesExactlyTheTablesTheTopbarCountHides() throws IOException {
        // Must match getTablesSummary's predicate: hidden only when validated AND not found.
        // Diverging would make the list and the topbar count disagree, which is the kind of defect
        // that gets reported as "the panel is missing a table" long after the cause is forgettable.
        tablesIndex("""
                {"tables":[
                  {"name":"KEPT_PLAIN","allOperations":[],"usedBy":[]},
                  {"name":"KEPT_FOUND","dbValidated":true,"dbFound":true,"allOperations":[],"usedBy":[]},
                  {"name":"KEPT_UNVALIDATED","dbValidated":false,"dbFound":false,"allOperations":[],"usedBy":[]},
                  {"name":"HIDDEN","dbValidated":true,"dbFound":false,"allOperations":[],"usedBy":[]}]}""");

        JsonNode rows = rowsOf(reader.getTablesList(ANALYSIS));
        assertEquals(3, rows.size(), "the visibility predicate does not match the topbar's");
        StringBuilder names = new StringBuilder();
        rows.forEach(r -> names.append(r.path("name").asText()).append(' '));
        assertFalse(names.toString().contains("HIDDEN"), "a hidden table was listed");
        assertTrue(names.toString().contains("KEPT_UNVALIDATED"),
                "an unvalidated table must still be listed");
    }

    @Test
    void survivesTablesWithNothingToCount() throws IOException {
        // Empty and absent are different in JSON and identical to a reader that stops early.
        tablesIndex("""
                {"tables":[
                  {"name":"NO_USEDBY","allOperations":["SELECT"]},
                  {"name":"EMPTY_USEDBY","allOperations":[],"usedBy":[]},
                  {"name":"EMPTY_LINES","allOperations":["SELECT"],
                   "usedBy":[{"nodeId":"n","operations":{"SELECT":[]}}]}]}""");

        JsonNode rows = rowsOf(reader.getTablesList(ANALYSIS));
        assertEquals(3, rows.size());
        for (JsonNode t : rows) {
            assertEquals(0, t.path("accessCount").asLong(),
                    t.path("name").asText() + " should have no access lines");
        }
    }

    @Test
    void readsPastFieldsItDoesNotCareAbout() throws IOException {
        // A nested object or array between the fields being read is where a token walk loses its
        // place: the parser must skip the whole subtree, not just its first token.
        tablesIndex("""
                {"operationSummary":{"SELECT":99},"totalTablesRaw":7,
                 "tables":[
                  {"dbConstraints":[{"cols":["A","B"],"meta":{"deep":{"deeper":[1,2]}}}],
                   "name":"ORDERS","dbResolvedType":"TABLE",
                   "allOperations":["SELECT"],
                   "usedBy":[{"nodeDepth":3,"extra":{"x":[1,2,3]},
                              "operations":{"SELECT":[1,2,3,4,5]}}],
                   "schema":"APP"}]}""");

        JsonNode doc = reader.getTablesList(ANALYSIS);
        JsonNode t = rowsOf(doc).get(0);
        assertEquals("ORDERS", t.path("name").asText(), "the walk lost its place in the element");
        assertEquals("APP", t.path("schema").asText(), "a field after a nested object was missed");
        assertEquals(5, t.path("accessCount").asLong());
        assertEquals(7, doc.path("totalTablesRaw").asInt(), "root scalars were dropped");
    }

    @Test
    void agreesWithTheTreeWalkItReplaced() throws IOException {
        // Equivalence against the obvious implementation. The streaming version exists for memory,
        // not for different answers, so the two must not disagree on the same input.
        String json = """
                {"tables":[
                  {"name":"A","schema":"S","allOperations":["SELECT"],
                   "usedBy":[{"operations":{"SELECT":[1,2],"UPDATE":[3]}},
                             {"operations":{"DELETE":[4,5,6]}}]},
                  {"name":"B","schema":"S","allOperations":["INSERT"],
                   "usedBy":[{"operations":{"INSERT":[7]}}]}]}""";
        tablesIndex(json);

        JsonNode streamed = rowsOf(reader.getTablesList(ANALYSIS));
        JsonNode tree = new com.fasterxml.jackson.databind.ObjectMapper().readTree(json).path("tables");

        assertEquals(tree.size(), streamed.size());
        for (int i = 0; i < tree.size(); i++) {
            long expected = 0;
            for (JsonNode u : tree.get(i).path("usedBy")) {
                for (JsonNode lines : u.path("operations")) expected += lines.size();
            }
            assertEquals(expected, streamed.get(i).path("accessCount").asLong(),
                    "streamed count disagrees with the tree walk for "
                            + tree.get(i).path("name").asText());
        }
    }

    /**
     * TEMP as the FOURTH table kind, served through {@code /tables/list}: a row the parser flagged
     * TEMP (with its lifetime scope) carries {@code kind=TEMP}; a row with no flag defaults to its
     * objectType (TABLE/VIEW), byte-compatible with artifacts written before the field existed.
     */
    @Test
    void exposesTempKindAndScope() throws IOException {
        tablesIndex("""
                {"tables":[
                  {"name":"GTT_STAGING","schema":"APP","objectType":"TABLE","tableKind":"TEMP",
                   "tempScope":"ON COMMIT DELETE ROWS","allOperations":["DDL"],"usedBy":[]},
                  {"name":"ORDERS","schema":"APP","objectType":"TABLE","allOperations":["SELECT"],"usedBy":[]},
                  {"name":"V_ORDERS","schema":"APP","objectType":"VIEW","allOperations":["SELECT"],"usedBy":[]}]}""");

        JsonNode rows = rowsOf(reader.getTablesList(ANALYSIS));
        JsonNode gtt = rows.get(0);
        assertEquals("TEMP", gtt.path("kind").asText(), "a parse-flagged temp table must serve kind=TEMP");
        assertEquals("ON COMMIT DELETE ROWS", gtt.path("tempScope").asText());

        JsonNode orders = rows.get(1);
        assertEquals("TABLE", orders.path("kind").asText(), "an un-flagged row defaults to TABLE");
        assertFalse(orders.has("tempScope"), "a non-temp row carries no tempScope");

        JsonNode view = rows.get(2);
        assertEquals("VIEW", view.path("kind").asText(), "a view row defaults to its objectType");
    }

    /** The header breakdown: per-kind counts from the same single streaming pass as the total. */
    @Test
    void summaryCountsKindsIncludingTemp() throws IOException {
        tablesIndex("""
                {"totalTables":4,"tables":[
                  {"name":"GTT_A","objectType":"TABLE","tableKind":"TEMP","allOperations":["DDL"],"usedBy":[]},
                  {"name":"T1","objectType":"TABLE","allOperations":["SELECT"],"usedBy":[]},
                  {"name":"V1","objectType":"VIEW","allOperations":["SELECT"],"usedBy":[]},
                  {"name":"MV1","objectType":"MATERIALIZED_VIEW","allOperations":["SELECT"],"usedBy":[]}]}""");

        JsonNode s = reader.getTablesSummary(ANALYSIS);
        assertEquals(4, s.path("validatedTableCount").asInt());
        assertEquals(1, s.path("tempTableCount").asInt(), "one temp table");
        assertEquals(1, s.path("tableCount").asInt(), "one real table");
        assertEquals(2, s.path("viewCount").asInt(), "a view + a materialized view");
        assertEquals(1, s.path("tableKindCounts").path("TEMP").asInt());
    }
}
