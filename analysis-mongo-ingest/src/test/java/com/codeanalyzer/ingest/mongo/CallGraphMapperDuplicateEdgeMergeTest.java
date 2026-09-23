package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.map.plsql.CallGraphMapper;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression for the ingest failure that broke {@code ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE}:
 * {@code am_call_edge_unique}'s {@code uk_uedge_natural} index is unique on the *resolved*
 * (fromRef, toRef) pair, but {@code api/call_graph.json}'s own edges array is only guaranteed
 * unique on the *raw* (fromNodeId, toNodeId) pair (see {@code ChunkedFlowWriter.writeCallGraph}).
 *
 * <p>Two edges detected via separate passes (e.g. the whole-body call-expression scanner and the
 * TABLE(fn(...)) collection-expression scanner) can end up with distinct raw node IDs for the same
 * logical target — the overload/arity-suffix case observed live for
 * {@code ERP_PKG_PLANNER.EXPLODE_PRODUCTS} — that both resolve, via {@code ctx.nodeId(...)}, to the
 * very same underlying entity. Emitting one Mongo doc per call_graph.json row then throws
 * {@code E11000} on the second row's insert. {@link CallGraphMapper} must instead merge rows that
 * resolve to the same (fromRef, toRef) pair into exactly one doc, summing {@code count} and
 * unioning/sorting {@code lines}.
 */
class CallGraphMapperDuplicateEdgeMergeTest {

    /**
     * Two distinct raw node IDs ({@code TARGET} and {@code TARGET__1}) for the same logical
     * callee, simulating two independent call-detection passes over the same TABLE(fn(...)) call
     * that assigned different overload-arity suffixes.
     */
    private static final String CALL_GRAPH = """
            {
              "totalEdges": 2,
              "uniqueEdges": 2,
              "backEdges": 0,
              "edges": [
                {
                  "fromNodeId": "CALLER",
                  "fromSchema": "ANALYZER",
                  "from": "CALLER",
                  "toNodeId": "TARGET",
                  "toSchema": "ANALYZER",
                  "to": "PKG.TARGET",
                  "lines": [10],
                  "count": 1,
                  "minDepth": 0,
                  "backEdge": false
                },
                {
                  "fromNodeId": "CALLER",
                  "fromSchema": "ANALYZER",
                  "from": "CALLER",
                  "toNodeId": "TARGET__1",
                  "toSchema": "ANALYZER",
                  "to": "PKG.TARGET",
                  "lines": [11],
                  "count": 1,
                  "minDepth": 1,
                  "backEdge": false
                }
              ]
            }
            """;

    @Test
    void mergesRowsThatResolveToTheSameEntityKeyIntoOneDoc() throws Exception {
        IngestContext ctx = new IngestContext("ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE~v1", "ORACLE");
        // Both raw node IDs resolve to the SAME entity (mirrors overload/arity-suffix variants of
        // one real target both being the same underlying am_proc_node).
        ctx.putNodeId("CALLER", "ORACLE~ANALYZER$CALLER");
        ctx.putNodeId("TARGET", "ORACLE~ANALYZER$TARGET");
        ctx.putNodeId("TARGET__1", "ORACLE~ANALYZER$TARGET");

        List<Document> emitted = new ArrayList<>();
        List<Document> lineRows = new ArrayList<>();
        String linesCollection = Collections.CALL_EDGE_UNIQUE + "__lines";
        new CallGraphMapper().map(ctx,
                new ByteArrayInputStream(CALL_GRAPH.getBytes(StandardCharsets.UTF_8)),
                (coll, doc) -> {
                    if (Collections.CALL_EDGE_UNIQUE.equals(coll)) emitted.add(doc);
                    if (linesCollection.equals(coll)) lineRows.add(doc);
                });

        assertEquals(1, emitted.size(),
                "two call_graph.json rows resolving to the same (fromRef,toRef) must merge into one doc");

        Document merged = emitted.get(0);
        assertEquals(2, merged.getInteger("count"), "counts from both rows must be summed");

        List<Integer> lines = new ArrayList<>();
        for (Document row : lineRows) lines.add((Integer) row.get("value"));
        assertTrue(lines.containsAll(List.of(10, 11)), "lines from both rows must be unioned: " + lines);
        assertEquals(2, lines.size(), "no duplicate lines expected, got: " + lines);
    }
}
