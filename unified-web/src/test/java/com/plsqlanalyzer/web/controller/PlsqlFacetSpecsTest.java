package com.plsqlanalyzer.web.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.web.page.FacetPageResponder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MID-106 (first increment) — {@code /procedures/page} on the live-Oracle-crawl PL/SQL app's own
 * {@link AnalyzerController}, a second, previously-unpaged controller distinct from the
 * artifact-store one {@code ParserFacetSpecs} serves.
 *
 * <h2>Why a synthetic fixture, not the real corpus</h2>
 *
 * <p>{@code data/plsql} — where this app's own crawled analyses would live — is empty in this
 * repository (checked, see the registry's §37/§44). Every assertion here is therefore about the
 * paging *mechanism* (does it round-trip every row exactly once, does scope work, does it not
 * return an unbounded array once a caller asks for a page), not about a measured row count or
 * default this repo has no data to derive.
 */
class PlsqlFacetSpecsTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private JsonNode node(String id, String name, String schema, String pkg) {
        return mapper.createObjectNode()
                .put("id", id).put("name", name).put("schemaName", schema)
                .put("packageName", pkg).put("unitType", "PROCEDURE");
    }

    @Test
    void pagingReturnsEveryRowExactlyOnce() {
        var arr = mapper.createArrayNode();
        for (int i = 0; i < 137; i++) {
            arr.add(node("S$P" + i, "P" + i, "S", "PKG"));
        }
        var responder = new FacetPageResponder(mapper, 10_000);

        List<String> seen = new ArrayList<>();
        String cursor = null;
        for (int guard = 0; guard < 20; guard++) {
            var res = responder.respond(arr, null, PlsqlFacetSpecs.PROCEDURES,
                    new FacetPageResponder.Params(25, cursor, "NAME_ASC", null, null, null,
                            "application/json"), null);
            JsonNode body = res.getBody();
            for (JsonNode item : body.path("items")) seen.add(item.path("id").asText());
            cursor = body.path("page").path("nextCursor").asText(null);
            if (cursor == null || cursor.isEmpty()) break;
        }
        assertEquals(137, seen.size(), "paging returned " + seen.size() + " of 137 rows");
        assertEquals(137, seen.stream().distinct().count(), "a row was returned on two pages");
    }

    @Test
    void searchMatchesNameSchemaAndPackage() {
        var arr = mapper.createArrayNode()
                .add(node("A$X", "FIND_ME", "SCHEMA_A", "PKG_A"))
                .add(node("B$Y", "OTHER", "SCHEMA_B", "PKG_B"));
        var responder = new FacetPageResponder(mapper, 10_000);
        var res = responder.respond(arr, null, PlsqlFacetSpecs.PROCEDURES,
                new FacetPageResponder.Params(null, null, null, "find_me", null, null,
                        "application/json"), null);
        assertEquals(1, res.getBody().path("items").size());
        assertEquals("A$X", res.getBody().path("items").get(0).path("id").asText());
    }

    @Test
    void anUnpagedCallerStillGetsTheBareArray() {
        var arr = mapper.createArrayNode().add(node("A$X", "X", "S", "PKG"));
        var responder = new FacetPageResponder(mapper, 10_000);
        var res = responder.respond(arr, null, PlsqlFacetSpecs.PROCEDURES,
                new FacetPageResponder.Params(null, null, null, null, null, null, null), null);
        assertTrue(res.getBody().isArray(),
                "an unpaged request must still receive the bare array, matching every other converted facet");
    }

    // ==================== TABLES (second increment) ====================

    private JsonNode table(String name, String schema, String type, int accessCount,
                           String... procedureNames) {
        var t = mapper.createObjectNode()
                .put("tableName", name).put("schemaName", schema).put("tableType", type)
                .put("accessCount", accessCount);
        var ops = t.putArray("operations");
        ops.add("SELECT");
        var details = t.putArray("accessDetails");
        for (String p : procedureNames) {
            details.addObject().put("procedureId", p).put("procedureName", p).put("operation", "SELECT");
        }
        return t;
    }

    @Test
    void tablesPagingReturnsEveryRowExactlyOnce() {
        var arr = mapper.createArrayNode();
        for (int i = 0; i < 83; i++) arr.add(table("T" + i, "S", "TABLE", i));
        var responder = new FacetPageResponder(mapper, 10_000);

        List<String> seen = new ArrayList<>();
        String cursor = null;
        for (int guard = 0; guard < 20; guard++) {
            var res = responder.respond(arr, null, PlsqlFacetSpecs.TABLES,
                    new FacetPageResponder.Params(20, cursor, "NAME_ASC", null, null, null,
                            "application/json"), null);
            JsonNode body = res.getBody();
            for (JsonNode item : body.path("items")) seen.add(item.path("tableName").asText());
            cursor = body.path("page").path("nextCursor").asText(null);
            if (cursor == null || cursor.isEmpty()) break;
        }
        assertEquals(83, seen.size(), "paging returned " + seen.size() + " of 83 rows");
        assertEquals(83, seen.stream().distinct().count(), "a row was returned on two pages");
    }

    @Test
    void tablesSortByAccessCountDescendingMatchesTheStoredField() {
        // accessCount is a real stored field here (unlike ParserFacetSpecs.TABLES, which computes
        // it) — this pins that the sort reads it rather than recomputing from accessDetails.
        //
        // Table names deliberately contradict alphabetical order (id is schema.tableName): if the
        // accessCount sort silently fell back to the id tiebreaker, "AAA_LOW" would sort first
        // alphabetically and this would pass for the wrong reason — the exact "compared a mutant to
        // itself" trap this registry's own §32 lesson warns about.
        var arr = mapper.createArrayNode()
                .add(table("AAA_LOW", "S", "TABLE", 1))
                .add(table("ZZZ_HIGH", "S", "TABLE", 99));
        var responder = new FacetPageResponder(mapper, 10_000);
        var res = responder.respond(arr, null, PlsqlFacetSpecs.TABLES,
                new FacetPageResponder.Params(null, null, "USAGES_DESC", null, null, null,
                        "application/json"), null);
        assertEquals("ZZZ_HIGH", res.getBody().path("items").get(0).path("tableName").asText());
    }

    @Test
    void tablesSearchMatchesTableNameTypeAndAccessingProcedures() {
        var arr = mapper.createArrayNode()
                .add(table("ORDERS", "S", "TABLE", 5, "PLACE_ORDER"))
                .add(table("CUSTOMERS", "S", "TABLE", 2, "UPDATE_CUSTOMER"));
        var responder = new FacetPageResponder(mapper, 10_000);
        var res = responder.respond(arr, null, PlsqlFacetSpecs.TABLES,
                new FacetPageResponder.Params(null, null, null, "place_order", null, null,
                        "application/json"), null);
        assertEquals(1, res.getBody().path("items").size());
        assertEquals("ORDERS", res.getBody().path("items").get(0).path("tableName").asText());
    }

    @Test
    void tablesScopeFiltersByAccessingProcedure() {
        var arr = mapper.createArrayNode()
                .add(table("ORDERS", "S", "TABLE", 1, "PROC_A"))
                .add(table("CUSTOMERS", "S", "TABLE", 1, "PROC_B"));
        var responder = new FacetPageResponder(mapper, 10_000);
        // FacetSpec::accessDetailProcedureIds upper-cases; a real scope predicate would be built
        // by ParserScopeResolver/an equivalent, but the extractor itself is what this pins.
        assertEquals(List.of("PROC_A"), PlsqlFacetSpecs.TABLES.procedureIds()
                .apply(arr.get(0)));
    }

    @Test
    void tablesExternalFilterColumnMatchesOnlyExternalTables() {
        var external = table("REMOTE_TBL", "S", "TABLE", 1);
        ((com.fasterxml.jackson.databind.node.ObjectNode) external).put("external", true);
        var internal = table("LOCAL_TBL", "S", "TABLE", 1);
        var arr = mapper.createArrayNode().add(external).add(internal);
        var responder = new FacetPageResponder(mapper, 10_000);
        var res = responder.respond(arr, null, PlsqlFacetSpecs.TABLES,
                new FacetPageResponder.Params(null, null, null, null, null, List.of("3:Y"),
                        "application/json"), null);
        assertEquals(1, res.getBody().path("items").size());
        assertEquals("REMOTE_TBL", res.getBody().path("items").get(0).path("tableName").asText());
    }

    @Test
    void tablesEnabledTriggerFilterColumnIgnoresDisabledTriggers() {
        var withEnabled = table("T1", "S", "TABLE", 1);
        ((com.fasterxml.jackson.databind.node.ObjectNode) withEnabled).putArray("triggers")
                .addObject().put("triggerName", "TRG1").put("status", "ENABLED");
        var withDisabledOnly = table("T2", "S", "TABLE", 1);
        ((com.fasterxml.jackson.databind.node.ObjectNode) withDisabledOnly).putArray("triggers")
                .addObject().put("triggerName", "TRG2").put("status", "DISABLED");
        var arr = mapper.createArrayNode().add(withEnabled).add(withDisabledOnly);
        var responder = new FacetPageResponder(mapper, 10_000);
        var res = responder.respond(arr, null, PlsqlFacetSpecs.TABLES,
                new FacetPageResponder.Params(null, null, null, null, null, List.of("4:Y"),
                        "application/json"), null);
        assertEquals(1, res.getBody().path("items").size());
        assertEquals("T1", res.getBody().path("items").get(0).path("tableName").asText());
    }

    // ==================== JOINS (fourth increment) ====================

    private JsonNode join(String left, String right, int accessCount, String onPredicate) {
        var j = mapper.createObjectNode()
                .put("leftTable", left).put("rightTable", right).put("accessCount", accessCount);
        j.putArray("joinTypes").add("INNER");
        j.putArray("accessDetails").addObject()
                .put("procedureId", "P1").put("procedureName", "P1").put("onPredicate", onPredicate);
        return j;
    }

    @Test
    void joinsPagingReturnsEveryRowExactlyOnce() {
        var arr = mapper.createArrayNode();
        for (int i = 0; i < 61; i++) arr.add(join("L" + i, "R" + i, i, "L.ID = R.ID"));
        var responder = new FacetPageResponder(mapper, 10_000);

        List<String> seen = new ArrayList<>();
        String cursor = null;
        for (int guard = 0; guard < 20; guard++) {
            var res = responder.respond(arr, null, PlsqlFacetSpecs.JOINS,
                    new FacetPageResponder.Params(15, cursor, "LEFT_ASC", null, null, null,
                            "application/json"), null);
            JsonNode body = res.getBody();
            for (JsonNode item : body.path("items")) seen.add(item.path("leftTable").asText());
            cursor = body.path("page").path("nextCursor").asText(null);
            if (cursor == null || cursor.isEmpty()) break;
        }
        assertEquals(61, seen.size(), "paging returned " + seen.size() + " of 61 rows");
        assertEquals(61, seen.stream().distinct().count(), "a row was returned on two pages");
    }

    @Test
    void joinsSearchMatchesOnPredicateText() {
        var arr = mapper.createArrayNode()
                .add(join("ORDERS", "CUSTOMERS", 1, "ORDERS.CUST_ID = CUSTOMERS.ID"))
                .add(join("ITEMS", "PRODUCTS", 1, "ITEMS.PROD_ID = PRODUCTS.ID"));
        var responder = new FacetPageResponder(mapper, 10_000);
        var res = responder.respond(arr, null, PlsqlFacetSpecs.JOINS,
                new FacetPageResponder.Params(null, null, null, "cust_id", null, null,
                        "application/json"), null);
        assertEquals(1, res.getBody().path("items").size());
        assertEquals("ORDERS", res.getBody().path("items").get(0).path("leftTable").asText());
    }

    // ==================== SEQUENCES (third increment) ====================

    private JsonNode sequence(String name, String schema, int accessCount) {
        var s = mapper.createObjectNode()
                .put("sequenceName", name).put("schemaName", schema).put("accessCount", accessCount);
        s.putArray("operations").add("NEXTVAL");
        s.putArray("accessDetails").addObject().put("procedureId", "P1").put("procedureName", "P1");
        return s;
    }

    @Test
    void sequencesPagingReturnsEveryRowExactlyOnce() {
        var arr = mapper.createArrayNode();
        for (int i = 0; i < 45; i++) arr.add(sequence("SEQ" + i, "S", i));
        var responder = new FacetPageResponder(mapper, 10_000);

        List<String> seen = new ArrayList<>();
        String cursor = null;
        for (int guard = 0; guard < 20; guard++) {
            var res = responder.respond(arr, null, PlsqlFacetSpecs.SEQUENCES,
                    new FacetPageResponder.Params(10, cursor, "NAME_ASC", null, null, null,
                            "application/json"), null);
            JsonNode body = res.getBody();
            for (JsonNode item : body.path("items")) seen.add(item.path("sequenceName").asText());
            cursor = body.path("page").path("nextCursor").asText(null);
            if (cursor == null || cursor.isEmpty()) break;
        }
        assertEquals(45, seen.size(), "paging returned " + seen.size() + " of 45 rows");
        assertEquals(45, seen.stream().distinct().count(), "a row was returned on two pages");
    }

    // ==================== CURSORS (fifth increment) ====================

    private JsonNode cursor(String name, String type, int accessCount) {
        var c = mapper.createObjectNode()
                .put("cursorName", name).put("cursorType", type).put("accessCount", accessCount);
        c.putArray("operations").add("FETCH");
        c.putArray("accessDetails").addObject().put("procedureId", "P1").put("procedureName", "P1");
        return c;
    }

    @Test
    void cursorsPagingReturnsEveryRowExactlyOnce() {
        var arr = mapper.createArrayNode();
        for (int i = 0; i < 29; i++) arr.add(cursor("C" + i, "EXPLICIT", i));
        var responder = new FacetPageResponder(mapper, 10_000);

        List<String> seen = new ArrayList<>();
        String cur = null;
        for (int guard = 0; guard < 20; guard++) {
            var res = responder.respond(arr, null, PlsqlFacetSpecs.CURSORS,
                    new FacetPageResponder.Params(7, cur, "NAME_ASC", null, null, null,
                            "application/json"), null);
            JsonNode body = res.getBody();
            for (JsonNode item : body.path("items")) seen.add(item.path("cursorName").asText());
            cur = body.path("page").path("nextCursor").asText(null);
            if (cur == null || cur.isEmpty()) break;
        }
        assertEquals(29, seen.size(), "paging returned " + seen.size() + " of 29 rows");
        assertEquals(29, seen.stream().distinct().count(), "a row was returned on two pages");
    }

    @Test
    void cursorsHaveNoSchemaFieldAndDoNotCrashBuildingTheIdOrSearchKey() {
        // Cursors are PL/SQL-local, not database objects — no schemaName on this summary, unlike
        // tables/sequences/joins. Pins that the id/search key were built without assuming one.
        var arr = mapper.createArrayNode().add(cursor("C_CUSTOMERS", "EXPLICIT", 3));
        var responder = new FacetPageResponder(mapper, 10_000);
        var res = responder.respond(arr, null, PlsqlFacetSpecs.CURSORS,
                new FacetPageResponder.Params(null, null, null, "c_customers", null, null,
                        "application/json"), null);
        assertEquals(1, res.getBody().path("items").size());
    }
}
