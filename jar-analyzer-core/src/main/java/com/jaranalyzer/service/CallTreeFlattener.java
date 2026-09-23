package com.jaranalyzer.service;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jaranalyzer.model.CallNode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Turns the in-memory call tree — which is a DAG, not a tree — into a flat, content-addressed
 * node dictionary so the persisted document is proportional to the number of DISTINCT methods
 * rather than to the number of PATHS through the graph.
 *
 * <p>Why this exists: {@code CallTreeBuilder} memoizes subtrees, so a method reached by N call
 * paths is ONE {@code CallNode} instance referenced N times. Memory stays linear. JSON, however,
 * has no way to say "this node again" — Jackson re-serialises that shared subtree once per
 * reference, so the file grows super-linearly and compounds at every level. A 1,597-class WAR
 * produced a 69 GB {@code analysis.json} that never finished writing. Flattened, the same graph
 * is bounded by its distinct-node count.
 *
 * <h2>Node identity</h2>
 * Ids are <em>content-derived</em>, never ordinals: the id is a SHA-256 prefix over the node's own
 * serialised fields plus its already-computed child ids. This matters for three reasons:
 * <ul>
 *   <li>The crawl is non-deterministic (thread pools, disk-backed subtree cache), so ordinals
 *       would renumber between runs and diffs would be meaningless.</li>
 *   <li>{@code CallTreeBuilder} produces call-site-specific variants of the same method (see
 *       {@code CallNode.copyForCallSiteTagging}) — same {@code nodeId}, different
 *       {@code collectionsAccessed}. Keying on {@code nodeId} alone would collapse those variants
 *       and silently change what the API serves. Keying on content keeps them distinct.</li>
 *   <li>Conversely, the disk-backed {@code SubtreeCache} hands back deep copies that are distinct
 *       objects with identical content; content keys collapse those, and they also collapse
 *       identical subtrees ACROSS endpoints, which is where most of the saving comes from.</li>
 * </ul>
 *
 * <h2>Cycles are impossible by construction</h2>
 * A node's id is a hash of its children's ids, so the id graph is a Merkle DAG — an id can never
 * (transitively) reference itself. Back-edges are already cut upstream by the
 * {@code callStack.contains(nodeId)} check in {@code CallTreeBuilder}, which returns a childless
 * node flagged {@code recursive=true}; that node hashes differently from the real one precisely
 * because it has no children. Readers can therefore expand {@code childIds} without a visited set
 * and are guaranteed to terminate.
 *
 * <h2>Record shape</h2>
 * Each dictionary entry is the node's own fields exactly as {@code ObjectMapper} would serialise
 * them, with the {@code children} array replaced <em>in place</em> by {@code childIds} (an array
 * of dictionary keys). Keeping the slot position means a reader can stream-copy the record and
 * swap {@code childIds} back to {@code children} to reproduce today's bytes field-for-field.
 *
 * <h2>Memory</h2>
 * Records are spilled to a caller-supplied scratch stream as they are discovered; only the id
 * set and an identity map of already-visited node instances are held in memory.
 * {@link #streamNodes} then copies that scratch stream into the output generator. The
 * per-endpoint {@link #sidecar} variant keeps its (bounded) dictionary in memory instead.
 */
public final class CallTreeFlattener implements Closeable {

    /** Marker written into analysis.json so readers can tell a flattened doc from a legacy one. */
    public static final String FORMAT = "dag-v1";
    public static final String FORMAT_FIELD = "callTreeFormat";
    public static final String NODES_FIELD = "callTreeNodes";
    public static final String ROOT_ID_FIELD = "callTreeRootId";
    public static final String FRAGMENT_IDS_FIELD = "fragmentChildIds";
    public static final String CHILD_IDS_FIELD = "childIds";

    private static final List<CallNode> NO_CHILDREN = Collections.emptyList();

    private final ObjectMapper mapper;
    private final Set<String> emitted = new HashSet<>();
    private final IdentityHashMap<CallNode, String> byInstance = new IdentityHashMap<>();
    private final BufferedWriter out;
    private boolean closed;

    /** Records held in memory instead of spilled; non-null only in in-memory mode. */
    private final Map<String, String> inMemory;

    /** Re-opens the spill for reading in {@link #streamNodes}. */
    @FunctionalInterface
    public interface SpillReader {
        java.io.InputStream open() throws IOException;
    }

    private final SpillReader spillReader;

    /**
     * Spilling mode, for the whole-document write where the dictionary spans every endpoint.
     *
     * <p>The scratch stream is supplied by the caller rather than opened here: every byte this
     * project writes goes through {@code ArtifactStore}, so the spill lives wherever the
     * configured backend puts it and this class stays free of filesystem calls.
     */
    public CallTreeFlattener(ObjectMapper mapper, java.io.OutputStream spillOut,
                             SpillReader spillReader) {
        this.mapper = mapper;
        this.out = new BufferedWriter(new java.io.OutputStreamWriter(spillOut, StandardCharsets.UTF_8));
        this.spillReader = spillReader;
        this.inMemory = null;
    }

    private CallTreeFlattener(ObjectMapper mapper, boolean memory) {
        this.mapper = mapper;
        this.out = null;
        this.spillReader = null;
        this.inMemory = new java.util.LinkedHashMap<>();
    }

    /**
     * Builds the self-contained per-endpoint sidecar written as {@code call-tree.json}: the same
     * dictionary format as the main document, scoped to one endpoint's tree.
     *
     * <p>The sidecar used to be the fully expanded tree, which made it the second place the
     * path-vs-node explosion showed up — {@code EndpointOutputWriter} writes one per endpoint, so
     * for a 317-endpoint WAR it multiplied the same blow-up 317 times.
     */
    public static ObjectNode sidecar(ObjectMapper mapper, CallNode root) throws IOException {
        CallTreeFlattener f = new CallTreeFlattener(mapper, true);
        String rootId = f.flatten(root);
        ObjectNode doc = mapper.createObjectNode();
        doc.put(FORMAT_FIELD, FORMAT);
        if (rootId == null) doc.putNull(ROOT_ID_FIELD); else doc.put(ROOT_ID_FIELD, rootId);
        ObjectNode nodes = doc.putObject(NODES_FIELD);
        for (Map.Entry<String, String> e : f.inMemory.entrySet()) {
            nodes.set(e.getKey(), mapper.readTree(e.getValue()));
        }
        return doc;
    }

    /** Number of distinct nodes in the dictionary so far. */
    public int nodeCount() {
        return emitted.size();
    }

    /**
     * Adds {@code node} and everything reachable from it to the dictionary.
     *
     * @return the dictionary key for {@code node}, or null if {@code node} is null.
     */
    public String flatten(CallNode node) throws IOException {
        if (node == null) return null;
        String known = byInstance.get(node);
        if (known != null) return known;

        List<CallNode> kids = node.getChildren();
        ArrayNode childIds = mapper.createArrayNode();
        if (kids != null) {
            for (CallNode kid : kids) {
                String kidId = flatten(kid);
                if (kidId != null) childIds.add(kidId);
            }
        }

        // Serialise the node's own fields with an EMPTY children list rather than via a mixin
        // that drops the property: this keeps "children" in its natural declaration slot, so the
        // childIds array lands exactly where the reader must put the expanded children back.
        // The swap is immediately undone — shared nodes must not be observed mutated.
        ObjectNode own;
        try {
            node.setChildren(NO_CHILDREN);
            own = mapper.valueToTree(node);
        } finally {
            node.setChildren(kids);
        }

        ObjectNode record = mapper.createObjectNode();
        boolean placed = false;
        var fields = own.fields();
        while (fields.hasNext()) {
            var e = fields.next();
            if ("children".equals(e.getKey())) {
                record.set(CHILD_IDS_FIELD, childIds);
                placed = true;
            } else {
                record.set(e.getKey(), e.getValue());
            }
        }
        if (!placed) record.set(CHILD_IDS_FIELD, childIds);

        String json = mapper.writeValueAsString(record);
        String id = hash(json);
        if (emitted.add(id)) {
            if (inMemory != null) {
                inMemory.put(id, json);
            } else {
                out.write(id);
                out.write('\t');
                out.write(json);
                out.write('\n');
            }
        }
        byInstance.put(node, id);
        return id;
    }

    /** Flattens a fragment's overflow-children list, returning the list of dictionary keys. */
    public List<String> flattenAll(List<CallNode> nodes) throws IOException {
        List<String> ids = new ArrayList<>();
        if (nodes == null) return ids;
        for (CallNode n : nodes) {
            String id = flatten(n);
            if (id != null) ids.add(id);
        }
        return ids;
    }

    /**
     * Writes the dictionary as a JSON object field. Streams straight from the spill file — the
     * dictionary is never materialised as a Java map.
     */
    public void streamNodes(JsonGenerator gen) throws IOException {
        gen.writeFieldName(NODES_FIELD);
        streamNodesValue(gen);
    }

    /**
     * MID-103 — the value half of {@link #streamNodes}, split out so a caller already positioned
     * at the value (Jackson's own {@code BeanPropertyWriter} already wrote the field name before
     * invoking a property's serializer) can stream the same dictionary without a duplicated field
     * name. See {@code com.jaranalyzer.model.StreamableNodeSerializer}, this method's other caller.
     */
    public void streamNodesValue(JsonGenerator gen) throws IOException {
        out.close();
        gen.writeStartObject();
        int written = 0;
        try (BufferedReader r = new BufferedReader(
                new java.io.InputStreamReader(spillReader.open(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) {
                int tab = line.indexOf('\t');
                if (tab <= 0) continue;
                gen.writeFieldName(line.substring(0, tab));
                gen.writeRawValue(line.substring(tab + 1));
                if (++written % 2000 == 0) gen.flush();
            }
        }
        gen.writeEndObject();
        gen.flush();
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        if (out != null) out.close();   // idempotent; streamNodes closes it first on the happy path
    }

    private static String hash(String json) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] d = md.digest(json.getBytes(StandardCharsets.UTF_8));
            // 20 hex chars = 80 bits. At the ~10^5 distinct nodes this format is designed for the
            // collision probability is ~10^-14; a collision would merge two identical-looking
            // subtrees, not corrupt one.
            StringBuilder sb = new StringBuilder(20);
            for (int i = 0; i < 10; i++) sb.append(String.format("%02x", d[i]));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    /** Convenience for callers holding an endpoint's fragment map. */
    public ObjectNode flattenFragments(Map<String, List<CallNode>> fragments) throws IOException {
        if (fragments == null || fragments.isEmpty()) return null;
        ObjectNode result = mapper.createObjectNode();
        for (Map.Entry<String, List<CallNode>> e : fragments.entrySet()) {
            ArrayNode arr = result.putArray(e.getKey());
            for (String id : flattenAll(e.getValue())) arr.add(id);
        }
        return result;
    }
}
