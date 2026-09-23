package com.codeanalyzer.ingest.mongo.map.jar;

import com.codeanalyzer.ingest.mongo.id.IdCodec;
import com.codeanalyzer.ingest.mongo.id.Identity;
import com.codeanalyzer.ingest.mongo.map.ArtifactMapper;
import com.codeanalyzer.ingest.mongo.map.DocSink;
import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.map.Splitter;
import com.codeanalyzer.ingest.mongo.model.RunMember;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * {@code analysis.json} → the call-tree DAG, the endpoints, and the three shared pools.
 *
 * <h2>The DAG stays a DAG</h2>
 *
 * <p>The producer already content-addresses the call tree: {@code callTreeNodes} is keyed by a
 * 20-hex hash and {@code childIds} holds those same hashes, so identical subtrees are stored once.
 * That is how 3,784 physical nodes stand in for <b>20.3 billion</b> logical ones — one endpoint
 * alone expands to 300,167,238. Any design that expanded the tree while storing it would not be
 * slow, it would be impossible.
 *
 * <p>So the keys are adopted rather than replaced, and {@code childIds} becomes an edge collection
 * with an ordinal rather than an embedded array. Both directions are indexed: the screens ask "what
 * does this call" and "how is this reached", and a tree that can only be walked downwards answers
 * half of them.
 *
 * <h2>Endpoints reference the pools; they do not copy them</h2>
 *
 * <p>{@code beansRefs}, {@code externalCallsRefs} and {@code dynamicFlowsRefs} are indices into
 * three shared pools. One endpoint was measured using 106 of 220 beans and 2,027 of 2,080 external
 * calls — copying those into each of 321 endpoints would multiply a small amount of data by two
 * orders of magnitude for nothing.
 */
public final class DagAndEndpointMapper implements ArtifactMapper {

    public static final String ARTIFACT = "analysis.json";

    private final ObjectMapper json = new ObjectMapper();

    @Override
    public String artifact() {
        return ARTIFACT;
    }

    @Override
    public void map(IngestContext ctx, InputStream in, DocSink sink) throws IOException {
        try (JsonParser p = json.getFactory().createParser(in)) {
            if (p.nextToken() != JsonToken.START_OBJECT) return;
            while (p.nextToken() == JsonToken.FIELD_NAME) {
                String field = p.currentName();
                p.nextToken();
                switch (field) {
                    case "callTreeNodes" -> readDag(ctx, sink, p);
                    case "endpointPool" -> readPools(ctx, sink, p);
                    case "endpoints" -> readEndpoints(ctx, sink, p);
                    case "advice" -> readAdvice(ctx, sink, p);
                    default -> p.skipChildren();     // classes and pools belong to the other mapper
                }
            }
        }
    }

    /**
     * Reads an array field and removes it from its parent in one step.
     *
     * <p>The same helper {@code NodeDetailMapper} uses, and for the same reason: read-then-remove
     * has to be atomic at the call site, because doing it in two statements is exactly how a field
     * ends up read by its emitter and still left for {@code Splitter.flatten} to duplicate. The
     * removed subtree stays valid — Jackson's {@code remove} unlinks the child rather than
     * clearing it — so the returned node is still the one the emitter walks.
     *
     * @return the array, or a missing node when the field is absent or not an array
     */
    private static JsonNode take(JsonNode parent, String field) {
        JsonNode v = parent.path(field);
        if (parent.isObject() && v.isArray()) ((ObjectNode) parent).remove(field);
        return v;
    }

    private void readDag(IngestContext ctx, DocSink sink, JsonParser p) throws IOException {
        if (p.currentToken() != JsonToken.START_OBJECT) { p.skipChildren(); return; }
        while (p.nextToken() == JsonToken.FIELD_NAME) {
            String hash = p.currentName();
            p.nextToken();
            emitDagNode(ctx, sink, hash, json.readTree(p));
        }
    }

    private void emitDagNode(IngestContext ctx, DocSink sink, String hash, JsonNode n) {
        String entityKey = IdCodec.dagNode(ctx.entityKey(), hash).key();

        // childIds becomes edges, so it is removed before the node is flattened.
        ObjectNode copy = n.deepCopy();
        JsonNode children = copy.remove("childIds");

        // Three maps keyed by collection NAME, the same shape ING-113 removed from the endpoint.
        // Left in place they make field names out of data: the key set is unbounded, and 4 of the
        // names measured contain a dot, which no query path can address.
        JsonNode domains = copy.remove("collectionDomains");
        JsonNode sources = copy.remove("collectionSources");
        JsonNode verification = copy.remove("collectionVerification");

        Document d = Splitter.flatten(ctx, sink, Collections.DAG_NODE, entityKey, copy);
        d.put("runId", ctx.runId());
        d.put("hash", hash);
        d.put("childCount", children != null && children.isArray() ? children.size() : 0);

        Identity ident = Identity.ofSource(entityKey, entityKey, n);
        ident.stampOnto(d);
        sink.emit(Collections.DAG_NODE, d);
        sink.emit(RunMember.COLLECTION,
                RunMember.of(ctx.runId(), ident, Collections.DAG_NODE, null));
        emitNodeCollections(ctx, sink, entityKey, domains, sources, verification);

        if (children != null && children.isArray()) {
            int ordinal = 0;
            for (JsonNode c : children) {
                String childHash = c.asText(null);
                if (childHash == null) continue;
                String edgeKey = IdCodec.dagEdge(entityKey, ordinal, childHash).key();
                Document e = new Document("runId", ctx.runId())
                        .append("parentRef", entityKey)
                        .append("childRef", IdCodec.dagNode(ctx.entityKey(), childHash).key())
                        .append("childHash", childHash)
                        .append("ordinal", ordinal);
                Identity.of(edgeKey, edgeKey, e).stampOnto(e);
                sink.emit(Collections.DAG_EDGE, e);
                ordinal++;
            }
        }
    }

    /**
     * {@code aggregatedCollections} + {@code aggregatedCollectionDomains} +
     * {@code aggregatedOperations} → one row per (endpoint, collection).
     *
     * <p>Three structures describing one relation: a list of collection names, and two maps from
     * that same name to a domain and an operation. Kept apart they have to be read together and
     * kept in step by every consumer; joined here, the row either exists or it does not.
     *
     * <p>Driven by the array rather than by the maps' key sets, because the array is the
     * authoritative list — a collection with no recorded domain is still accessed, and dropping it
     * because one of the two side maps lacks an entry would lose a real access.
     *
     * <p>{@code names} is passed in rather than read from {@code e}, because it has already been
     * {@link #take}n off the endpoint: a field with a hand-written emitter must never also reach
     * {@code Splitter.flatten}. The two side maps are still read from {@code e} — they are objects,
     * so flatten embeds them rather than emitting rows, and they are dropped from the endpoint
     * document afterwards.
     */
    private void emitCollections(IngestContext ctx, DocSink sink, String endpointKey,
                                 JsonNode names, JsonNode e) {
        if (!names.isArray()) return;
        JsonNode domains = e.path("aggregatedCollectionDomains");
        JsonNode operations = e.path("aggregatedOperations");

        int ordinal = 0;
        for (JsonNode n : names) {
            String name = n.asText(null);
            if (name == null || name.isEmpty()) continue;
            String key = IdCodec.nested(Collections.ENDPOINT_COLLECTION, endpointKey, name).key();
            Document row = new Document("_id", key)
                    .append("key", key)
                    .append("runId", ctx.runId())
                    .append("endpointRef", endpointKey)
                    .append("collectionName", name)
                    .append("ordinal", ordinal++);
            // path() rather than get(): a name absent from either map is normal, and the row is
            // still a real access. Only the side fact is missing.
            String domain = domains.path(name).asText(null);
            if (domain != null && !domain.isEmpty()) row.append("domain", domain);
            String op = operations.path(name).asText(null);
            if (op != null && !op.isEmpty()) row.append("operation", op);
            sink.emit(Collections.ENDPOINT_COLLECTION, row);
        }
    }

    /**
     * The three per-collection maps on a node, joined into one row per collection.
     *
     * <p>Driven by the union of their key sets rather than by one of them: a collection may appear
     * in {@code collectionSources} and not in {@code collectionDomains}, and picking either as the
     * authority would silently drop the rows only the other knows about.
     */
    private void emitNodeCollections(IngestContext ctx, DocSink sink, String nodeKey,
                                     JsonNode domains, JsonNode sources, JsonNode verification) {
        java.util.LinkedHashSet<String> names = new java.util.LinkedHashSet<>();
        for (JsonNode map : new JsonNode[]{domains, sources, verification}) {
            if (map != null && map.isObject()) map.fieldNames().forEachRemaining(names::add);
        }
        if (names.isEmpty()) return;

        int ordinal = 0;
        for (String name : names) {
            String key = IdCodec.nested(Collections.DAG_NODE_COLLECTION, nodeKey, name).key();
            Document row = new Document("_id", key)
                    .append("key", key)
                    .append("runId", ctx.runId())
                    .append("nodeRef", nodeKey)
                    .append("collectionName", name)
                    .append("ordinal", ordinal++);
            putText(row, "domain", domains, name);
            putText(row, "source", sources, name);
            putText(row, "verification", verification, name);
            sink.emit(Collections.DAG_NODE_COLLECTION, row);
        }
    }

    private static void putText(Document row, String field, JsonNode map, String name) {
        if (map == null || !map.isObject()) return;
        JsonNode v = map.get(name);
        if (v == null || v.isNull()) return;
        row.append(field, v.isValueNode() ? v.asText() : v.toString());
    }

    private void readPools(IngestContext ctx, DocSink sink, JsonParser p) throws IOException {
        if (p.currentToken() != JsonToken.START_OBJECT) { p.skipChildren(); return; }
        while (p.nextToken() == JsonToken.FIELD_NAME) {
            String pool = p.currentName();
            p.nextToken();
            JsonNode arr = json.readTree(p);
            if (!arr.isArray()) continue;
            int index = 0;
            for (JsonNode entry : arr) {
                String entityKey = IdCodec.endpointPool(ctx.entityKey(), pool, index).key();
                Document d = Splitter.flatten(ctx, sink, Collections.ENDPOINT_POOL, entityKey, entry);
                d.put("runId", ctx.runId());
                d.put("pool", pool);
                d.put("poolIndex", index);
                Identity.ofSource(entityKey, entityKey, entry).stampOnto(d);
                sink.emit(Collections.ENDPOINT_POOL, d);
                index++;
            }
        }
    }

    /**
     * The endpoint field holding positions, mapped to the pool name those positions index into.
     *
     * <p>The names differ by more than a suffix — {@code beansRefs} indexes {@code beans}, but
     * {@code externalCallsRefs} indexes {@code externalCalls} — so this is a map and not a string
     * operation. Getting it wrong would build every link against a pool that does not exist, and
     * the result would be uniformly dangling rather than subtly wrong, which is the good case.
     */
    private static final Map<String, String> POOL_REF_FIELDS = Map.of(
            "externalCallsRefs", "externalCalls",
            "beansRefs", "beans",
            "dynamicFlowsRefs", "dynamicFlows");

    /**
     * One junction row per (endpoint, pool entry), with both sides resolved to keys.
     *
     * <p>The pool key is built from the jar, the pool name and the index — the same three parts
     * {@link IdCodec#endpointPool} uses when writing the pool itself, so the two cannot disagree
     * about what row 7 of {@code beans} is called. {@code ordinal} is kept because the order an
     * endpoint lists its references in is part of the source data.
     */
    private void emitPoolLinks(IngestContext ctx, DocSink sink, String endpointKey, JsonNode e) {
        for (var entry : POOL_REF_FIELDS.entrySet()) {
            JsonNode arr = e.path(entry.getKey());
            if (!arr.isArray()) continue;
            int ordinal = 0;
            for (JsonNode idx : arr) {
                if (!idx.canConvertToInt()) {
                    // Not silently skipped: a non-integer here means the pooled format changed
                    // and every link built from it would be wrong.
                    throw new IllegalStateException("non-integer pool reference in "
                            + entry.getKey() + " of " + endpointKey + ": " + idx);
                }
                int index = idx.asInt();
                String poolKey = IdCodec.endpointPool(ctx.entityKey(), entry.getValue(), index)
                        .key();
                Document d = new Document();
                d.put("key", endpointKey + "~" + entry.getValue() + "~" + ordinal);
                d.put("runId", ctx.runId());
                d.put("endpointRef", endpointKey);
                d.put("poolRef", poolKey);
                d.put("pool", entry.getValue());
                d.put("poolIndex", index);
                d.put("ordinal", ordinal);
                sink.emit(Collections.ENDPOINT_POOL_LINK, d);
                ordinal++;
            }
        }
    }

    /**
     * A JAR-level fact outside the endpoint call graph — ING-117.
     *
     * <p>Was silently dropped on every ingest: this field switch fell through to
     * {@code default -> p.skipChildren()} for {@code advice}, and the other JAR mapper
     * ({@code AnalysisJsonMapper}) never named it either. Neither omission raised an error,
     * because skipping a field a parser was never told to look for is indistinguishable from the
     * field genuinely being absent — the same failure mode as an unchecked reference, one step
     * earlier in the pipeline.
     *
     * <p>Two rows measured on this corpus, one per run — small and bounded by nature (there is one
     * advice pass per analysis, not one per node), but split anyway rather than embedded in
     * {@code am_run}: the no-arrays invariant is structural in this module for a reason recorded
     * in {@code TablesIndexMapper} — "bounded today" is a property of the data, and an embedded
     * array is one schema change from being unbounded.
     */
    private void readAdvice(IngestContext ctx, DocSink sink, JsonParser p) throws IOException {
        if (p.currentToken() != JsonToken.START_ARRAY) { p.skipChildren(); return; }
        int ordinal = 0;
        while (p.nextToken() != JsonToken.END_ARRAY) {
            JsonNode e = json.readTree(p);
            String entityKey = IdCodec.analysisAdvice(ctx.entityKey(), ordinal).key();
            Document d = Splitter.flatten(ctx, sink, Collections.ANALYSIS_ADVICE, entityKey, e);
            d.put("runId", ctx.runId());
            d.put("ordinal", ordinal);
            Identity ident = Identity.ofSource(entityKey, entityKey, e);
            ident.stampOnto(d);
            sink.emit(Collections.ANALYSIS_ADVICE, d);
            sink.emit(RunMember.COLLECTION,
                    RunMember.of(ctx.runId(), ident, Collections.ANALYSIS_ADVICE, null));
            ordinal++;
        }
    }

    private void readEndpoints(IngestContext ctx, DocSink sink, JsonParser p) throws IOException {
        if (p.currentToken() != JsonToken.START_ARRAY) { p.skipChildren(); return; }
        while (p.nextToken() != JsonToken.END_ARRAY) {
            JsonNode e = json.readTree(p);
            String controller = e.path("controllerClass").asText("");
            String method = e.path("methodName").asText("");
            if (controller.isEmpty() && method.isEmpty()) continue;
            String entityKey = IdCodec.endpoint(ctx.entityKey(), controller, method,
                    e.path("httpMethod").asText(""), e.path("fullPath").asText("")).key();

            // The three ref arrays are handled here, so they become a junction with resolved keys
            // rather than rows holding a bare integer.
            //
            // Taken off the source node BEFORE flatten, not off the result after it: Splitter
            // emits child rows as a side effect *during* flatten, so removing the fields from the
            // returned document would leave the positional rows already written. Storing the same
            // fact twice in two shapes is the state where a query gets written against the wrong
            // one — the reason `aggregatedOperations` is removed too, one step earlier.
            emitPoolLinks(ctx, sink, entityKey, e);
            if (e.isObject()) {
                ((ObjectNode) e).remove(POOL_REF_FIELDS.keySet());
            }

            // Captured and stripped BEFORE flatten, for the same reason the pool refs above are.
            // `aggregatedCollections` has a hand-written emitter (`emitCollections`, building
            // am_endpoint_collection), but it was still reaching the generic splitter, which
            // emitted every element a second time into `am_endpoint__aggregated_collections` —
            // 155 rows against the same 155 real ones, measured on the stored unified-web
            // analysis. Those rows carry no `domain`, no `operation` and no `endpointRef`, so
            // nothing can read them, and nothing does.
            //
            // Removing the field afterwards cannot work: flatten emits the child rows as it
            // walks, so by the time it returns they are already written.
            JsonNode aggregatedCollections = take(e, "aggregatedCollections");

            Document d = Splitter.flatten(ctx, sink, Collections.ENDPOINT, entityKey, e);
            d.put("runId", ctx.runId());
            emitCollections(ctx, sink, entityKey, aggregatedCollections, e);
            // Removed after emitting: left in place they are maps keyed by collection NAME, which
            // makes field names out of data. Two consequences, both silent. The key set grows
            // without bound, which is the failure the no-arrays rule exists to prevent — it simply
            // does not fire on an object. And real keys contain dots
            // ("information_schema.ROUTINES"), so the dotted path a query would use is ambiguous
            // with a nested document and cannot address the field at all.
            d.remove("aggregatedCollectionDomains");
            d.remove("aggregatedOperations");
            String root = e.path("callTreeRootId").asText(null);
            if (root != null) {
                d.put("rootRef", IdCodec.dagNode(ctx.entityKey(), root).key());
            }
            Identity ident = Identity.ofSource(entityKey, entityKey, e);
            ident.stampOnto(d);
            sink.emit(Collections.ENDPOINT, d);
            sink.emit(RunMember.COLLECTION,
                    RunMember.of(ctx.runId(), ident, Collections.ENDPOINT, null));
        }
    }

}
