package com.plsqlanalyzer.web.parser.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.web.page.FacetSort;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The paging contract of the Table Operations facet.
 *
 * <h2>Why the tables spec is the risky one</h2>
 *
 * <p>The other four specs run over rows a reader already merged into the browser's shape. This one
 * runs over {@code api/tables/index.json} as the crawler wrote it, where the fields are
 * {@code name}/{@code schema}/{@code allOperations}/{@code usedBy} rather than
 * {@code tableName}/{@code tableSchema}/{@code operations}/{@code accessDetails}. Every reference to
 * a field name in the spec is therefore a chance to name one that does not exist — and a sort on a
 * missing field does not fail, it silently returns insertion order.
 *
 * <p>{@code accessCount} is the sharpest case: it is not a field at all, it is the size of the
 * access-line structure, so it has to be computed. That is what most of this file checks.
 */
class TablesFacetSpecTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private JsonNode row(String json) {
        try {
            return mapper.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** A table row in the crawler's own shape. */
    private JsonNode table(String schema, String name, String type, String ops, String usedBy) {
        return row("{\"schema\":\"" + schema + "\",\"name\":\"" + name + "\",\"objectType\":\"" + type
                + "\",\"allOperations\":" + ops + ",\"usedBy\":" + usedBy + "}");
    }

    @Test
    void theDefaultSortIsTheOrderTheGridAlreadyShowed() {
        // The grid sets sortCol 4 (Count) descending immediately after init. Any other default here
        // changes page 1 for the user on the day the endpoint is switched on — a silent regression
        // that looks like the data changed.
        assertEquals("USAGES_DESC", ParserFacetSpecs.TABLES.defaultSort());
        assertTrue(ParserFacetSpecs.TABLES.sorts().containsKey("USAGES_DESC"));
    }

    @Test
    void usageSortCountsAccessLinesRatherThanReadingAMissingField() {
        // B has more usedBy entries; A has more access LINES. The sort must score A higher.
        //
        // Asserted on the sort KEY, not on the resulting order. Ordering alone cannot detect this:
        // `comparator(id)` falls back to the row identity, so a sort that scores every row equally
        // still produces a stable, plausible-looking order. Swapping this for a `number("accessCount")`
        // sort — reading a field the artifact does not have — passed the ordering assertion.
        JsonNode a = table("APP", "A", "TABLE", "[\"SELECT\"]",
                "[{\"nodeId\":\"n1\",\"operations\":{\"SELECT\":[1,2,3,4,5]}}]");
        JsonNode b = table("APP", "B", "TABLE", "[\"SELECT\"]",
                "[{\"nodeId\":\"n1\",\"operations\":{\"SELECT\":[1]}},"
                        + "{\"nodeId\":\"n2\",\"operations\":{\"SELECT\":[2]}}]");

        FacetSort desc = ParserFacetSpecs.TABLES.sorts().get("USAGES_DESC");
        assertNotNull(desc, "USAGES_DESC is not declared");
        String ka = desc.key().apply(a);
        String kb = desc.key().apply(b);
        assertTrue(!ka.equals(kb),
                "the usage sort scores 5 access lines the same as 2 — it is reading a field that "
                        + "does not exist on a tables row (got " + ka + " for both)");
        assertTrue(ka.compareTo(kb) > 0,
                "5 access lines must outrank 2; got " + ka + " vs " + kb);

        List<JsonNode> rows = new ArrayList<>(List.of(b, a));
        rows.sort(desc.comparator(ParserFacetSpecs.TABLES.id()));
        assertEquals("A", rows.get(0).path("name").asText(),
                "usage sort did not count access lines");
    }

    @Test
    void everyDeclaredSortActuallyOrdersSomething() {
        // A sort naming a field the artifact does not have is inert: it compiles, the endpoint
        // accepts it, and it scores every row identically. Because the comparator tiebreaks on row
        // identity, the resulting ORDER still looks fine — so the key function is what gets checked.
        //
        // These two rows differ in every dimension any declared sort could read, so a sort that
        // produces one distinct key across them is reading nothing.
        JsonNode first = table("AAA", "AAA", "TABLE", "[\"SELECT\"]",
                "[{\"nodeId\":\"n1\",\"operations\":{\"SELECT\":[1]}}]");
        JsonNode second = table("ZZZ", "ZZZ", "VIEW", "[\"SELECT\"]",
                "[{\"nodeId\":\"n1\",\"operations\":{\"SELECT\":[1,2]}},"
                        + "{\"nodeId\":\"n2\",\"operations\":{\"SELECT\":[3]}}]");

        for (var entry : ParserFacetSpecs.TABLES.sorts().entrySet()) {
            String k1 = entry.getValue().key().apply(first);
            String k2 = entry.getValue().key().apply(second);
            assertTrue(!k1.equals(k2),
                    entry.getKey() + " scores two rows that differ in every field identically — it "
                            + "names a field a tables row does not have, so it sorts nothing");
        }
    }

    @Test
    void rowIdentityIsTheSchemaAndNamePairTheCrawlerMergedOn() {
        // A non-unique cursor tiebreaker loses or repeats a row at a page boundary, and only for
        // data where the duplicate exists — so identity has to match what the crawler merged by.
        var id = ParserFacetSpecs.TABLES.id();
        assertEquals("APP.ORDERS", id.apply(table("APP", "ORDERS", "TABLE", "[]", "[]")));
        // Same name in two schemas must not collide.
        assertTrue(!id.apply(table("APP", "ORDERS", "TABLE", "[]", "[]"))
                .equals(id.apply(table("REPORTING", "ORDERS", "TABLE", "[]", "[]"))),
                "two schemas' tables of the same name share a cursor identity");
    }

    @Test
    void scopeFilteringSeesTheNodeIdsUnderUsedBy() {
        // Scope is applied server-side via procedureIds. The tables index says usedBy[].nodeId, not
        // accessDetails[].procedureId — reusing the shared helper here would return nothing and
        // every scoped request would answer "no tables", which reads as a real empty result.
        JsonNode t = table("APP", "ORDERS", "TABLE", "[\"SELECT\"]",
                "[{\"nodeId\":\"PKG$P1\",\"nodeName\":\"P1\",\"operations\":{\"SELECT\":[1]}},"
                        + "{\"nodeId\":\"PKG$P2\",\"nodeName\":\"P2\",\"operations\":{\"SELECT\":[2]}}]");
        List<String> ids = ParserFacetSpecs.TABLES.procedureIds().apply(t);
        assertEquals(List.of("PKG$P1", "PKG$P2"), ids,
                "scope filtering cannot see which procedures use this table");
    }

    @Test
    void searchMatchesTheProcedureNamesTheGridMatched() {
        // The grid's searchFn matched table name, object type, and the procedure names in the
        // access details. Dropping the last one would quietly narrow search on conversion day.
        JsonNode t = table("APP", "ORDERS", "TABLE", "[\"SELECT\"]",
                "[{\"nodeId\":\"PKG$P1\",\"nodeName\":\"ERP_USP_RUN_CYCLE\","
                        + "\"operations\":{\"SELECT\":[1]}}]");
        String text = ParserFacetSpecs.TABLES.searchText().apply(t);
        assertTrue(text.contains("orders"), "the table name is not searchable");
        assertTrue(text.contains("table"), "the object type is not searchable");
        assertTrue(text.contains("erp_usp_run_cycle"),
                "procedure names are no longer searchable from the tables grid");
    }

    @Test
    void columnFiltersReadTheCrawlersFieldNames() {
        JsonNode t = table("APP", "ORDERS", "VIEW", "[\"SELECT\",\"INSERT\"]", "[]");
        var cols = ParserFacetSpecs.TABLES.filterColumns();
        assertEquals(List.of("APP"), cols.get("0").apply(t), "the Schema filter is empty");
        assertEquals(List.of("ORDERS"), cols.get("1").apply(t), "the Table filter is empty");
        assertEquals(List.of("VIEW"), cols.get("2").apply(t), "the Type filter is empty");
        assertEquals(List.of("SELECT", "INSERT"), cols.get("3").apply(t),
                "the Operation filter is empty");
    }

    @Test
    void theTypeColumnReadsTheRowsEffectiveTypeSoTempIsFilterable() {
        // cf "2" was scalarColumn("objectType") — TEMP could never appear in the Type popover,
        // because a temp table's objectType is still "TABLE" and its TEMP-ness lives in
        // kind/tableKind (what the row's badge and the TEMP pill already read). The column now
        // reads the effective type: kind-first, then the normalized objectType — so the popover
        // lists TEMP exactly when the analysis HAS temp rows, and a ticked TEMP matches the same
        // rows the TEMP pill matches. The popover list and the matcher share this one function.
        var typeCol = ParserFacetSpecs.TABLES.filterColumns().get("2");

        assertEquals(List.of("TEMP"),
                typeCol.apply(row("{\"schema\":\"S\",\"name\":\"#T\",\"objectType\":\"TABLE\","
                        + "\"kind\":\"TEMP\",\"allOperations\":[],\"usedBy\":[]}")),
                "a crawler kind=TEMP row must read TEMP, not its objectType");
        assertEquals(List.of("TEMP"),
                typeCol.apply(row("{\"schema\":\"S\",\"name\":\"#T\",\"objectType\":\"TABLE\","
                        + "\"tableKind\":\"TEMP\",\"allOperations\":[],\"usedBy\":[]}")),
                "a row carrying only tableKind=TEMP (the older field) must read TEMP too");
        assertEquals(List.of("TABLE"),
                typeCol.apply(row("{\"schema\":\"S\",\"name\":\"T\",\"objectType\":\"TABLE\","
                        + "\"allOperations\":[],\"usedBy\":[]}")),
                "a plain table must be unchanged");
        assertEquals(List.of("VIEW"),
                typeCol.apply(row("{\"schema\":\"S\",\"name\":\"V\",\"objectType\":\"VIEW\","
                        + "\"allOperations\":[],\"usedBy\":[]}")),
                "a view must be unchanged");
        assertEquals(List.of("MATERIALIZED_VIEW"),
                typeCol.apply(row("{\"schema\":\"S\",\"name\":\"MV\",\"objectType\":\"MATERIALIZED_VIEW\","
                        + "\"allOperations\":[],\"usedBy\":[]}")),
                "a materialized view must stay normalized");
        // The TEMP pill (types=TEMP) matches through the spec's typeValues fn, the popover through
        // the cf column — both normalize the same fields, so the two can never disagree.
        JsonNode tempRow = row("{\"schema\":\"S\",\"name\":\"#T\",\"objectType\":\"TABLE\","
                + "\"kind\":\"TEMP\",\"allOperations\":[],\"usedBy\":[]}");
        assertEquals(
                ParserFacetSpecs.TABLES.typeValues().apply(tempRow).contains("TEMP"),
                typeCol.apply(tempRow).contains("TEMP"),
                "the Type column and the TEMP pill must agree on what is TEMP");
    }
}
