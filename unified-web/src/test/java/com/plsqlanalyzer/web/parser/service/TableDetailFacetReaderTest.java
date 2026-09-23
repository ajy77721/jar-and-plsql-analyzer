package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Q59 / Q59b scan semantics, which are the browser's and must stay the browser's.
 *
 * <p>Every case here is one the fan-out in {@code tableDetail.js} handled, and getting any of them
 * wrong produces a modal that looks right: a plausible list of joins, missing the incoming ones,
 * or counting a repeated query five times.
 */
class TableDetailFacetReaderTest {

    private static final String ANALYSIS = "ORACLE~P~DEV~DEMO";

    private final InMemoryArtifactStore store = new InMemoryArtifactStore();
    private final StaticAnalysisReader reader = new StaticAnalysisReader(store);

    private void node(String fileName, String json) throws IOException {
        ArtifactKey key = AnalysisPaths.resolveKey(ANALYSIS, "api/nodes/" + fileName);
        try (OutputStream out = store.write(key)) {
            out.write(json.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static List<String> field(JsonNode rows, String name) {
        List<String> out = new ArrayList<>();
        rows.forEach(r -> out.add(r.path(name).asText("")));
        return out;
    }

    @Test
    void collectsOutgoingAndIncomingJoinsAndReportsTheOtherSide() throws IOException {
        node("A.json", """
                {"nodeId":"S$A","name":"A","sourceFile":"S.A.sql","tables":[
                  {"name":"ORDERS","joins":[
                    {"joinType":"JOIN","joinedTable":"CUSTOMER","joinedTableAlias":"c",
                     "condition":"c.id=o.cust","line":10}]},
                  {"name":"INVOICE","alias":"i","joins":[
                    {"joinType":"LEFT","joinedTable":"ORDERS","condition":"i.o=o.id","line":20}]}
                ]}""");

        JsonNode rows = reader.getTableJoins(ANALYSIS, "orders");

        assertEquals(2, rows.size());
        assertEquals(List.of("OUT", "IN"), field(rows, "direction"));
        // The outgoing row names the table joined to; the incoming row names the table that owns
        // the FROM clause, because that is the "Joined Table" the modal shows.
        assertEquals(List.of("CUSTOMER", "INVOICE"), field(rows, "joinedTable"));
        assertEquals(List.of("c", "i"), field(rows, "joinedTableAlias"),
                "an incoming row must carry the other table's alias, not the join's");
        assertEquals(List.of("S$A", "S$A"), field(rows, "procedureId"));
        assertEquals(List.of("0", "1"), field(rows, "seq"), "seq is the scan position");
    }

    @Test
    void deDuplicatesPerRelationshipAndProcedureNotPerLine() throws IOException {
        // The same relationship written three times in one procedure, on three lines, plus once
        // in a second procedure. The modal counted this as two rows, and the Accesses tab counts
        // the same way — so three rows here would be a visible inconsistency, not a nicety.
        node("A.json", """
                {"nodeId":"S$A","name":"A","tables":[{"name":"ORDERS","joins":[
                  {"joinType":"JOIN","joinedTable":"CUSTOMER","line":1},
                  {"joinType":"JOIN","joinedTable":"CUSTOMER","line":2},
                  {"joinType":"JOIN","joinedTable":"CUSTOMER","line":3}]}]}""");
        node("B.json", """
                {"nodeId":"S$B","name":"B","tables":[{"name":"ORDERS","joins":[
                  {"joinType":"JOIN","joinedTable":"CUSTOMER","line":9}]}]}""");

        JsonNode rows = reader.getTableJoins(ANALYSIS, "ORDERS");

        assertEquals(2, rows.size());
        assertEquals(List.of("S$A", "S$B"), field(rows, "procedureId"));
        assertEquals(List.of("1", "9"), field(rows, "line"),
                "the first occurrence wins, as it did in the browser");
    }

    @Test
    void aDifferentJoinTypeIsADifferentRelationship() throws IOException {
        node("A.json", """
                {"nodeId":"S$A","name":"A","tables":[{"name":"ORDERS","joins":[
                  {"joinType":"JOIN","joinedTable":"CUSTOMER","line":1},
                  {"joinType":"LEFT","joinedTable":"CUSTOMER","line":2}]}]}""");

        assertEquals(2, reader.getTableJoins(ANALYSIS, "ORDERS").size());
    }

    @Test
    void tableNameMatchingIsCaseInsensitiveAndUnrelatedTablesAreExcluded() throws IOException {
        node("A.json", """
                {"nodeId":"S$A","name":"A","tables":[
                  {"name":"orders","joins":[{"joinType":"JOIN","joinedTable":"CUSTOMER","line":1}]},
                  {"name":"SHIPMENT","joins":[{"joinType":"JOIN","joinedTable":"CARRIER","line":2}]}
                ]}""");

        JsonNode rows = reader.getTableJoins(ANALYSIS, "ORDERS");
        assertEquals(1, rows.size());
        assertEquals("CUSTOMER", rows.get(0).path("joinedTable").asText());
    }

    @Test
    void missingJoinFieldsAreNormalisedSoTheTableNeverRendersUndefined() throws IOException {
        node("A.json", """
                {"nodeId":"S$A","tables":[{"name":"ORDERS","joins":[{"joinedTable":"CUSTOMER"}]}]}""");

        JsonNode row = reader.getTableJoins(ANALYSIS, "ORDERS").get(0);
        assertEquals("JOIN", row.path("joinType").asText());
        assertEquals("", row.path("condition").asText());
        assertEquals(0, row.path("line").asInt());
        assertEquals("", row.path("procedureName").asText());
    }

    @Test
    void aTableWithNoJoinsIsAnEmptyListNotAnError() throws IOException {
        node("A.json", """
                {"nodeId":"S$A","tables":[{"name":"ORDERS","joins":[]}]}""");
        assertEquals(0, reader.getTableJoins(ANALYSIS, "ORDERS").size());
        assertEquals(0, reader.getTableJoins(ANALYSIS, "NO_SUCH_TABLE").size());
    }

    @Test
    void columnRefsCoverTypeRefsParametersAndJoinConditions() throws IOException {
        node("A.json", """
                {"nodeId":"S$A","name":"A","sourceFile":"S.A.sql",
                 "variables":{"typeRefs":[
                    {"name":"V_SKU","dataType":"ORDERS.SKU%TYPE","refTable":"ORDERS",
                     "refField":"SKU","line":7},
                    {"name":"V_OTHER","dataType":"SHIPMENT.ID%TYPE","refTable":"SHIPMENT",
                     "refField":"ID","line":8}],
                  "rowtypeRefs":[{"name":"V_ROW","dataType":"ORDERS%ROWTYPE","refTable":"ORDERS",
                     "line":9}]},
                 "parameters":[{"name":"P_ID","dataType":"ORDERS.ID%TYPE","line":2}],
                 "tables":[{"name":"ORDERS","joins":[
                    {"joinType":"JOIN","joinedTable":"CUSTOMER",
                     "condition":"ORDERS.CUST_ID = c.id AND ORDERS.STATUS = 'X'","line":11}]}]}""");

        JsonNode rows = reader.getTableColumnRefs(ANALYSIS, "ORDERS");

        assertEquals(List.of("SKU", "", "ID", "CUST_ID", "STATUS"), field(rows, "refField"));
        assertEquals(List.of("TYPE_REF", "TYPE_REF", "PARAMETER", "JOIN_CONDITION",
                "JOIN_CONDITION"), field(rows, "refType"));
        assertEquals(List.of("V_SKU", "V_ROW", "P_ID", "-", "-"), field(rows, "varName"),
                "a table this node merely mentions (SHIPMENT) must not contribute a row");
    }

    @Test
    void aJoinConditionQualifiedByAliasContributesNoColumns() throws IOException {
        // This is why the Columns fan-out returned nothing on the measured analyses: real
        // conditions are written against aliases. Reproduced rather than "fixed", because
        // resolving aliases would invent references the browser never showed.
        node("A.json", """
                {"nodeId":"S$A","tables":[{"name":"ORDERS","joins":[
                  {"joinType":"JOIN","joinedTable":"CUSTOMER","condition":"c.id=o.cust","line":1}]}]}""");

        assertEquals(0, reader.getTableColumnRefs(ANALYSIS, "ORDERS").size());
    }

    @Test
    void columnRefsDeDuplicateOnProcedureVariableColumnAndLine() throws IOException {
        node("A.json", """
                {"nodeId":"S$A","variables":{"typeRef":[
                    {"name":"V","refTable":"ORDERS","refField":"SKU","line":3}],
                  "typeRefs":[
                    {"name":"V","refTable":"ORDERS","refField":"SKU","line":3}]}}""");

        assertEquals(1, reader.getTableColumnRefs(ANALYSIS, "ORDERS").size());
    }

    @Test
    void reAnalysisEvictsTheTableScopedCaches() throws IOException {
        node("A.json", """
                {"nodeId":"S$A","tables":[{"name":"ORDERS","joins":[
                  {"joinType":"JOIN","joinedTable":"CUSTOMER","line":1}]}]}""");
        assertEquals(1, reader.getTableJoins(ANALYSIS, "ORDERS").size());

        // A re-crawl replaces the node document. Without eviction the modal would keep serving
        // rows for a procedure that no longer exists, with nothing to indicate why.
        node("A.json", """
                {"nodeId":"S$A","tables":[{"name":"ORDERS","joins":[]}]}""");
        reader.evictCache(ANALYSIS);

        assertEquals(0, reader.getTableJoins(ANALYSIS, "ORDERS").size());
    }
}
