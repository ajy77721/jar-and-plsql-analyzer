package com.codeanalyzer.ingest.mongo.map.plsql;

import com.codeanalyzer.ingest.mongo.id.IdCodec;
import com.codeanalyzer.ingest.mongo.id.Identity;
import com.codeanalyzer.ingest.mongo.map.ArtifactMapper;
import com.codeanalyzer.ingest.mongo.map.DocSink;
import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.map.Splitter;
import com.codeanalyzer.ingest.mongo.model.RunMember;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * {@code api/call_graph.json} → {@code am_call_edge_unique}: the aggregated caller/callee pairs.
 *
 * <h2>Stored, not derived</h2>
 *
 * <p>This was very nearly marked "derivable from edges.json" and dropped. It is not derivable: the
 * aggregated rows carry {@code count}, {@code lines}, {@code minDepth} and {@code backEdge}, none
 * of which appear in the raw edge file. {@code backEdge} in particular is a cycle judgement the
 * analyser made while walking the graph, and recomputing it here would mean reimplementing that
 * walk — with every chance of disagreeing with the answer the rest of the product already shows.
 *
 * <p>The lesson generalises: "the same data in another shape" is a claim to verify by diffing
 * fields, not to accept because two files describe the same subject.
 */
public final class CallGraphMapper implements ArtifactMapper {

    public static final String ARTIFACT = "api/call_graph.json";

    private final ObjectMapper json = new ObjectMapper();

    @Override
    public String artifact() {
        return ARTIFACT;
    }

    @Override
    public void map(IngestContext ctx, InputStream in, DocSink sink) throws IOException {
        JsonNode root = json.readTree(in);
        JsonNode edges = root.path("edges");
        if (!edges.isArray()) return;

        // BUGFIX (§265 dup-edge / am_call_edge_unique E11000): call_graph.json's own edges array
        // is unique per *raw* (fromNodeId, toNodeId) pair (ChunkedFlowWriter.writeCallGraph already
        // dedupes on that), but `uk_uedge_natural` is unique on entityKey = (fromRef, toRef) — the
        // *resolved* entity references. Overload/arity-suffixed node IDs (e.g. two TABLE(fn(...))
        // detections of the same target that ended up with distinct raw node IDs) can resolve,
        // via EdgesMapper.refFor's node-index lookup, to the SAME underlying entity — so two
        // distinct rows here collapse to the same entityKey and a plain insert-per-row throws
        // E11000 on the second one. Group and merge by the *resolved* (fromRef, toRef) pair before
        // emitting, so at most one am_call_edge_unique doc is ever produced per resolved pair,
        // with counts summed and lines unioned/sorted/deduped across the merged rows.
        Map<String, MergedEdge> merged = new LinkedHashMap<>();
        for (JsonNode e : edges) {
            String from = e.path("fromNodeId").asText(null);
            String to = e.path("toNodeId").asText(null);
            if (from == null || to == null) continue;

            String fromRef = EdgesMapper.refFor(ctx, from);
            String toRef = EdgesMapper.refFor(ctx, to);
            String entityKey = IdCodec.callEdgeUnique(fromRef, toRef).key();

            MergedEdge existing = merged.get(entityKey);
            if (existing == null) {
                merged.put(entityKey, new MergedEdge(fromRef, toRef, e));
            } else {
                existing.absorb(e);
            }
        }

        for (Map.Entry<String, MergedEdge> entry : merged.entrySet()) {
            String entityKey = entry.getKey();
            MergedEdge m = entry.getValue();
            JsonNode e = m.toJsonNode(json);

            Document d = Splitter.flatten(ctx, sink, Collections.CALL_EDGE_UNIQUE, entityKey, e);
            d.put("runId", ctx.runId());
            d.put("fromRef", m.fromRef);
            d.put("toRef", m.toRef);

            Identity ident = Identity.ofSource(entityKey, entityKey, e);
            // BUGFIX (§266 continued): deliberately NOT `ident.stampOnto(d)` here. stampOnto sets
            // `_id` to a *content* hash, and BulkWriter.upsert() matches on `_id` when present —
            // so a later run whose call counts/lines differ (a near-certainty for any edge that
            // gets called a different number of times) computes a DIFFERENT `_id`, misses the
            // existing document entirely, and tries to INSERT a second one under the same
            // `entityKey` — which is exactly what `uk_uedge_natural` (unique per (fromRef,toRef))
            // exists to reject. Confirmed live: a stale v4 doc for
            // ERP_USP_RUN_CYCLE→EXPLODE_PRODUCTS (count:1) was never replaced by a later re-run's
            // (count:1, but a *different* one — a different hash either way) doc, and the insert
            // threw E11000. `am_call_edge_unique` is a *current-relationship* collection, not a
            // content-addressed history — re-ingesting the same (fromRef,toRef) pair must replace
            // its doc, not create a content-addressed sibling. Setting `key = entityKey` (with no
            // `_id`) makes BulkWriter.upsert() match on `key` instead, giving true
            // upsert-replace-by-entity semantics.
            d.put("entityKey", entityKey);
            d.put("contentHash", ident.id());
            d.put("key", entityKey);
            sink.emit(Collections.CALL_EDGE_UNIQUE, d);
            sink.emit(RunMember.COLLECTION,
                    RunMember.of(ctx.runId(), ident, Collections.CALL_EDGE_UNIQUE, null));
        }
    }

    /** Accumulates one or more same-(fromRef,toRef) call_graph.json edge rows into one merged row. */
    static final class MergedEdge {
        final String fromRef;
        final String toRef;
        JsonNode base;
        int count;
        boolean backEdge;
        int minDepth = Integer.MAX_VALUE;
        final TreeSet<Integer> lines = new TreeSet<>();

        MergedEdge(String fromRef, String toRef, JsonNode first) {
            this.fromRef = fromRef;
            this.toRef = toRef;
            this.base = first;
            absorb(first);
        }

        void absorb(JsonNode e) {
            count += e.path("count").asInt(0);
            if (e.path("backEdge").asBoolean(false)) backEdge = true;
            if (e.has("minDepth")) minDepth = Math.min(minDepth, e.path("minDepth").asInt(Integer.MAX_VALUE));
            JsonNode lineArr = e.path("lines");
            if (lineArr.isArray()) {
                for (JsonNode ln : lineArr) lines.add(ln.asInt());
            }
        }

        JsonNode toJsonNode(ObjectMapper json) {
            com.fasterxml.jackson.databind.node.ObjectNode node = base.deepCopy();
            node.put("count", count);
            node.put("backEdge", backEdge);
            node.put("minDepth", minDepth == Integer.MAX_VALUE ? 0 : minDepth);
            com.fasterxml.jackson.databind.node.ArrayNode lineNode = json.createArrayNode();
            for (Integer ln : lines) lineNode.add(ln);
            node.set("lines", lineNode);
            return node;
        }
    }
}
