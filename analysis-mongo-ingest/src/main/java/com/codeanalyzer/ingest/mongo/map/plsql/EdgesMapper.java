package com.codeanalyzer.ingest.mongo.map.plsql;

import com.codeanalyzer.ingest.mongo.id.IdCodec;
import com.codeanalyzer.ingest.mongo.id.Identity;
import com.codeanalyzer.ingest.mongo.map.ArtifactMapper;
import com.codeanalyzer.ingest.mongo.map.DocSink;
import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.map.Splitter;
import com.codeanalyzer.ingest.mongo.model.RunMember;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@code edges.json} → {@code am_call_edge}: one row per call <em>occurrence</em>.
 *
 * <h2>Occurrence, not relationship</h2>
 *
 * <p>These two are different and the distinction is the whole design here. ORACLE has 2,258 edge
 * rows describing only 218 distinct caller/callee pairs, and one node appears as a callee 1,819
 * times. Collapsing them would lose every call site; keeping only the collapsed form would lose
 * the counts. Both are stored, in separate collections, because the screens ask both questions:
 * "where is this called from" wants occurrences, "what depends on what" wants the pairs.
 *
 * <p>The aggregated form is not derived here — {@code api/call_graph.json} already computes it and
 * carries six fields the raw file does not have ({@code count}, {@code lines}, {@code minDepth},
 * {@code backEdge}, {@code from}, {@code to}). Recomputing would risk disagreeing with the
 * analyser's own answer; {@link CallGraphMapper} stores it instead.
 */
public final class EdgesMapper implements ArtifactMapper {

    public static final String ARTIFACT = "edges.json";

    private final ObjectMapper json = new ObjectMapper();

    @Override
    public String artifact() {
        return ARTIFACT;
    }

    @Override
    public void map(IngestContext ctx, InputStream in, DocSink sink) throws IOException {
        // Streamed: PostgreSQL already has 3,295 edges and edge count grows with call density
        // rather than with node count, so this is the collection most likely to surprise.
        try (var parser = json.getFactory().createParser(in)) {
            if (parser.nextToken() != JsonToken.START_ARRAY) return;
            while (parser.nextToken() == JsonToken.START_OBJECT) {
                emit(ctx, sink, json.readTree(parser));
            }
        }
    }

    private void emit(IngestContext ctx, DocSink sink, JsonNode e) {
        String from = e.path("fromNodeId").asText(null);
        String to = e.path("toNodeId").asText(null);
        if (from == null || to == null) return;
        int line = e.path("line").asInt(0);

        String fromRef = refFor(ctx, from);
        String toRef = refFor(ctx, to);
        String entityKey = IdCodec.callEdge(fromRef, toRef, line).key();

        Document d = Splitter.flatten(ctx, sink, Collections.CALL_EDGE, entityKey, e);
        d.put("runId", ctx.runId());
        d.put("fromRef", fromRef);
        d.put("toRef", toRef);
        d.put("line", line);

        Identity ident = Identity.ofSource(entityKey, entityKey, e);
        ident.stampOnto(d);
        sink.emit(Collections.CALL_EDGE, d);
        sink.emit(RunMember.COLLECTION,
                RunMember.of(ctx.runId(), ident, Collections.CALL_EDGE, null));
    }

    /**
     * The node's entity key, computed when the node itself was not seen.
     *
     * <p>An edge may name a node that has no file in {@code api/nodes/} — an external or
     * unresolved callee. Dropping the edge would lose a real dependency; pointing it at a computed
     * key keeps the graph complete and lets the verifier report the reference as dangling, which
     * is a fact worth having rather than an edge worth hiding.
     */
    static String refFor(IngestContext ctx, String nodeId) {
        String known = ctx.nodeId(nodeId);
        return known != null ? known : ctx.entityKey(nodeId);
    }
}
