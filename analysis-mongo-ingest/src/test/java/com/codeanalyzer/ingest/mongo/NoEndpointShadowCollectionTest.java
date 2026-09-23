package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.map.Splitter;
import com.codeanalyzer.ingest.mongo.map.jar.DagAndEndpointMapper;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The endpoint half of the rule {@link NoShadowCollectionTest} locks down for nodes: a field with a
 * hand-written emitter must never also reach {@code Splitter.flatten}.
 *
 * <h2>What was actually happening</h2>
 *
 * <p>{@code aggregatedCollections} has an emitter — {@code emitCollections}, which joins it with
 * {@code aggregatedCollectionDomains} and {@code aggregatedOperations} into
 * {@code am_endpoint_collection}, one row per (endpoint, collection), indexed both ways. But the
 * field was still on the endpoint when {@code flatten} walked it, so every name was written a
 * second time into {@code am_endpoint__aggregated_collections}.
 *
 * <p>Measured against the stored {@code unified-web} analysis (321 endpoints), ingested into a
 * throwaway database: {@code am_endpoint_collection} 155 rows and
 * {@code am_endpoint__aggregated_collections} 155 rows — a complete duplicate. The shadow rows
 * carry only {@code value} plus the splitter's bookkeeping: no {@code domain}, no
 * {@code operation}, no {@code endpointRef}. Nothing can answer a question from them, the name
 * appears nowhere in {@code analysis-mongo-ingest} or {@code unified-web}, and they are not in
 * {@code Catalog}, so they were never indexed either.
 *
 * <h2>Why the assertion is a negative over the set of collections</h2>
 *
 * <p>Asserting "am_endpoint_collection has 2 rows" cannot catch this — those rows are present in
 * the buggy version too. The only difference between correct and broken is a collection that ought
 * not to exist, so the test has to look at the <em>set</em> of names written.
 */
class NoEndpointShadowCollectionTest {

    /**
     * One endpoint carrying the three aggregate structures, plus the pool-ref arrays that are
     * already stripped and two arrays that legitimately have no emitter. Shapes follow the real
     * stored {@code analysis.json}.
     */
    private static final String ANALYSIS_JSON = """
            {
              "endpoints": [
                {
                  "controllerClass": "com.acme.OrderController",
                  "methodName": "list",
                  "httpMethod": "GET",
                  "fullPath": "/orders",
                  "aggregatedCollections": ["orders", "information_schema.ROUTINES"],
                  "aggregatedCollectionDomains": {"orders": "SALES"},
                  "aggregatedOperations": {"orders": "find"},
                  "externalCallsRefs": [0],
                  "beansRefs": [],
                  "dynamicFlowsRefs": [],
                  "parameters": [{"name": "page", "type": "int"}],
                  "modules": ["sales"]
                }
              ]
            }
            """;

    private static Map<String, Integer> collectionsWritten() throws Exception {
        IngestContext ctx = new IngestContext("JAR~unified-web~TESTRUN~v1", "JAR");
        Map<String, Integer> counts = new TreeMap<>();
        new DagAndEndpointMapper().map(ctx,
                new ByteArrayInputStream(ANALYSIS_JSON.getBytes(StandardCharsets.UTF_8)),
                (coll, doc) -> counts.merge(coll, 1, Integer::sum));
        return counts;
    }

    /** Every endpoint field with a hand-written emitter, and the shadow name it would produce. */
    private static final String[] HAND_MAPPED_FIELDS = {
            "aggregatedCollections",
            "externalCallsRefs", "beansRefs", "dynamicFlowsRefs",
    };

    @Test
    void noHandMappedEndpointFieldAlsoProducesASplitterShadowCollection() throws Exception {
        Map<String, Integer> written = collectionsWritten();

        List<String> shadows = new ArrayList<>();
        for (String field : HAND_MAPPED_FIELDS) {
            String shadow = Splitter.childCollection(Collections.ENDPOINT, field);
            if (written.containsKey(shadow)) {
                shadows.add(shadow + " (" + written.get(shadow) + " rows, duplicating the "
                        + "hand-built collection for '" + field + "')");
            }
        }
        assertTrue(shadows.isEmpty(),
                "these fields reached Splitter.flatten as well as their own emitter: " + shadows
                        + "\nall collections written: " + written);
    }

    /**
     * The strip must not cost the rows it was protecting, and must not cost the side facts either.
     * This is what separates "stripped before flatten" from "stripped, and the emitter now sees
     * nothing" — the failure mode that would make the test above pass for the wrong reason.
     */
    @Test
    void theHandBuiltEndpointCollectionStillReceivesItsRows() throws Exception {
        Map<String, Integer> w = collectionsWritten();

        assertEquals(1, w.get(Collections.ENDPOINT), "the endpoint itself");
        assertEquals(2, w.get(Collections.ENDPOINT_COLLECTION),
                "one row per (endpoint, collection); written: " + w);
        assertEquals(1, w.get(Collections.ENDPOINT_POOL_LINK), "externalCallsRefs[0]");
    }

    /** The joined side facts still land on the row — the reason the emitter exists at all. */
    @Test
    void theJoinedDomainAndOperationSurviveTheStrip() throws Exception {
        IngestContext ctx = new IngestContext("JAR~unified-web~TESTRUN~v1", "JAR");
        List<org.bson.Document> rows = new ArrayList<>();
        new DagAndEndpointMapper().map(ctx,
                new ByteArrayInputStream(ANALYSIS_JSON.getBytes(StandardCharsets.UTF_8)),
                (coll, doc) -> {
                    if (Collections.ENDPOINT_COLLECTION.equals(coll)) rows.add(doc);
                });

        org.bson.Document orders = rows.stream()
                .filter(r -> "orders".equals(r.getString("collectionName")))
                .findFirst().orElseThrow(() -> new AssertionError("no row for 'orders': " + rows));
        assertEquals("SALES", orders.getString("domain"));
        assertEquals("find", orders.getString("operation"));
        assertNotNull(orders.getString("endpointRef"), "the row must resolve to its endpoint");

        // A dotted name is the case that motivated am_endpoint_collection: unaddressable as a
        // field name, ordinary as a value. It must still arrive, with no side facts.
        assertTrue(rows.stream().anyMatch(
                        r -> "information_schema.ROUTINES".equals(r.getString("collectionName"))),
                "a collection with no domain/operation is still a real access: " + rows);
    }

    /**
     * Fields with no hand-written emitter must keep reaching the splitter — it is their only home.
     * Asserted so a future "strip everything" does not quietly delete two collections, the mirror
     * of the {@code calls}/{@code parameters} guard in {@link NoShadowCollectionTest}.
     */
    @Test
    void endpointFieldsWithNoEmitterAreStillSplitOut() throws Exception {
        Map<String, Integer> w = collectionsWritten();

        assertEquals(1, w.get("am_endpoint__parameters"),
                "no emitter writes parameters; the splitter must still do it. Written: " + w);
        assertEquals(1, w.get("am_endpoint__modules"));
    }

    /**
     * The two side maps stay off the endpoint document. They are keyed by collection name, which
     * is data — {@code information_schema.ROUTINES} as a field name cannot be queried at all — so
     * once {@code emitCollections} has consumed them they must not remain embedded.
     */
    @Test
    void theDataKeyedSideMapsDoNotRemainOnTheEndpointDocument() throws Exception {
        IngestContext ctx = new IngestContext("JAR~unified-web~TESTRUN~v1", "JAR");
        List<org.bson.Document> endpoints = new ArrayList<>();
        new DagAndEndpointMapper().map(ctx,
                new ByteArrayInputStream(ANALYSIS_JSON.getBytes(StandardCharsets.UTF_8)),
                (coll, doc) -> { if (Collections.ENDPOINT.equals(coll)) endpoints.add(doc); });

        org.bson.Document ep = endpoints.get(0);
        assertNull(ep.get("aggregatedCollectionDomains"), "keyed by data; moved to the rows");
        assertNull(ep.get("aggregatedOperations"), "keyed by data; moved to the rows");
        assertNull(Splitter.dataKeyedMap(ep), "endpoint document holds a map keyed by data: " + ep);
    }
}
