package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.map.Splitter;
import com.codeanalyzer.ingest.mongo.map.plsql.NodeDetailMapper;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ING-118 — {@code statementSummary} is a map, and a wide one, so it took a whole run's
 * entity-ingest down with it.
 *
 * <h2>What happened</h2>
 *
 * <p>{@code statementSummary} counts statements per TYPE, so its keys are grammar tokens rather
 * than user data. That made it look exempt from the data-keyed-map gate — but the gate measures
 * width, not provenance, and the type vocabulary is open: every grammar addition (CTE, MERGE,
 * window functions, temporal tables) widens it. DB2's {@code ERP_USP_RUN_CYCLE} reached <b>41</b>
 * keys on one node, past {@link Splitter#MAX_SCHEMA_KEYS}, and the throw from {@code BulkWriter}
 * aborted the entity-ingest of the entire run: <b>13</b> node documents written instead of 63, the
 * survivors being the leaf functions with a handful of statement types each.
 *
 * <p>The remedy is the one the gate's own message names — rows, not an exemption. So this test has
 * to prove two things at once, and the second is the one that matters: that a node far past the
 * threshold now survives, AND that the gate itself still refuses a genuinely data-keyed map. A
 * "fix" that raised {@code MAX_SCHEMA_KEYS} would pass the first assertion alone.
 */
class StatementSummaryRowsTest {

    /** 41 distinct types, the width measured on the DB2 node that failed. */
    private static String wideNodeJson() {
        StringBuilder sb = new StringBuilder("""
                {
                  "nodeId": "ERP_USP_RUN_CYCLE",
                  "schema": "DB2INST1",
                  "objectName": "ERP_USP_RUN_CYCLE",
                  "objectType": "PROCEDURE",
                  "statementSummary": {
                """);
        for (int i = 0; i < 41; i++) {
            sb.append("    \"TYPE_").append(i).append("\": ").append(i + 1);
            if (i < 40) sb.append(',');
            sb.append('\n');
        }
        return sb.append("  }\n}\n").toString();
    }

    @Test
    void aNodeWithFortyOneStatementTypesIsNotRefusedByTheGate() throws Exception {
        IngestContext ctx = new IngestContext("DB2~DB2DEMO~LOCAL~ERP_USP_RUN_CYCLE~v1", "DB2");
        List<Document> nodes = new ArrayList<>();
        List<Document> rows = new ArrayList<>();
        new NodeDetailMapper().map(ctx,
                new ByteArrayInputStream(wideNodeJson().getBytes(StandardCharsets.UTF_8)),
                (coll, doc) -> {
                    if (Collections.PROC_NODE.equals(coll)) nodes.add(doc);
                    if (Collections.NODE_STATEMENT_SUMMARY.equals(coll)) rows.add(doc);
                });

        assertEquals(1, nodes.size(), "expected one am_proc_node document");
        Document node = nodes.get(0);
        assertNull(node.get("statementSummary"),
                "the map is still embedded on the node; BulkWriter will refuse the document");
        assertNull(Splitter.dataKeyedMap(node),
                "the node document would still be refused by the gate: "
                        + Splitter.dataKeyedMap(node));

        assertEquals(41, rows.size(), "every statement type must become a row");
        Map<String, Long> counts = new HashMap<>();
        for (Document r : rows) {
            assertEquals(ctx.runId(), r.getString("runId"));
            assertNotNull(r.getString("nodeRef"), "a row must point back at its node");
            assertNotNull(r.getString("key"), "a row needs a business key to be idempotent");
            counts.put(r.getString("statementType"), r.get("count", Number.class).longValue());
        }
        assertEquals(41, counts.size(), "row keys collided, so types were lost");
        assertEquals(1L, counts.get("TYPE_0"), "the count did not survive the reshaping");
        assertEquals(41L, counts.get("TYPE_40"));
    }

    @Test
    void theOrdinaryCaseStillProducesRowsWithTheRightCounts() throws Exception {
        String json = """
                {
                  "nodeId": "ERP_USP_SMALL",
                  "schema": "DB2INST1",
                  "objectName": "ERP_USP_SMALL",
                  "statementSummary": {"IF": 2, "CASE": 1, "MERGE": 1}
                }
                """;
        IngestContext ctx = new IngestContext("DB2~DB2DEMO~LOCAL~X~v1", "DB2");
        List<Document> rows = new ArrayList<>();
        new NodeDetailMapper().map(ctx,
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)),
                (coll, doc) -> {
                    if (Collections.NODE_STATEMENT_SUMMARY.equals(coll)) rows.add(doc);
                });
        assertEquals(3, rows.size());
        // Declaration order is the parser's first-appearance order and is carried, not implied.
        assertEquals("IF", rows.get(0).getString("statementType"));
        assertEquals(0, rows.get(0).getInteger("ordinal"));
        assertEquals(2L, rows.get(0).get("count", Number.class).longValue());
        assertEquals("MERGE", rows.get(2).getString("statementType"));
        assertEquals(2, rows.get(2).getInteger("ordinal"));
    }

    @Test
    void aNodeWithoutAStatementSummaryEmitsNoRows() throws Exception {
        String json = """
                {"nodeId": "NO_SUMMARY", "schema": "DB2INST1", "objectName": "NO_SUMMARY"}
                """;
        IngestContext ctx = new IngestContext("DB2~DB2DEMO~LOCAL~X~v1", "DB2");
        List<Document> rows = new ArrayList<>();
        new NodeDetailMapper().map(ctx,
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)),
                (coll, doc) -> {
                    if (Collections.NODE_STATEMENT_SUMMARY.equals(coll)) rows.add(doc);
                });
        assertTrue(rows.isEmpty(), "rows invented for a node that has no summary: " + rows);
    }

    @Test
    void theGateStillRefusesAGenuinelyDataKeyedMap() {
        // The half of this that proves the fix was not "turn the rule off". A map keyed by table
        // name is the failure the rule exists for, and it must still be caught — by width, and by
        // the dot that makes a key unaddressable.
        Document wide = new Document();
        for (int i = 0; i <= Splitter.MAX_SCHEMA_KEYS; i++) wide.append("ERP_TABLE_" + i, "READ");
        String found = Splitter.dataKeyedMap(new Document("_id", "x").append("tableAccess", wide));
        assertNotNull(found, "the width rule was disabled rather than the field reshaped");
        assertTrue(found.contains("keys"), "the refusal does not name the problem: " + found);

        assertNotNull(Splitter.dataKeyedMap(new Document("_id", "x")
                        .append("tableAccess", new Document("APP.ORDERS", "READ"))),
                "a dotted, unaddressable key was accepted");
    }
}
