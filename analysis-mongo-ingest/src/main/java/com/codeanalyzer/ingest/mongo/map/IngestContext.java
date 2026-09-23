package com.codeanalyzer.ingest.mongo.map;

import java.util.HashMap;
import java.util.Map;

/**
 * What a mapper needs to resolve references without reading anything back.
 *
 * <p>The lookups are the mechanism behind "relations are written first-hand": a mapper that needs
 * to point at a {@code db_object} computes or looks up that object's {@code _id} locally and
 * writes it into the child. Nothing is resolved after the fact, and no {@code $lookup} is required
 * to make the link exist.
 */
public final class IngestContext {

    private final String runId;
    private final String dialect;
    /** {@code SCHEMA.NAME} (upper-cased) → the {@code _id} of that {@code am_db_object}. */
    private final Map<String, String> objectIds = new HashMap<>();
    /** {@code nodeId} → the {@code _id} of that {@code am_proc_node}. */
    private final Map<String, String> nodeIds = new HashMap<>();

    public IngestContext(String runId, String dialect) {
        this.runId = runId;
        this.dialect = dialect;
    }

    public String runId()  { return runId; }
    public String dialect() { return dialect; }

    public void putObjectId(String schema, String name, String id) {
        objectIds.put(objectKey(schema, name), id);
    }

    /** Null when the object was never seen — callers must handle that rather than assume. */
    public String objectId(String schema, String name) {
        return objectIds.get(objectKey(schema, name));
    }

    public int knownObjects() { return objectIds.size(); }

    public void putNodeId(String nodeId, String id) { nodeIds.put(nodeId, id); }

    /** Null when that node was never seen; callers must handle it rather than assume. */
    public String nodeId(String nodeId) { return nodeIds.get(nodeId); }

    public int knownNodes() { return nodeIds.size(); }

    /** Pool index → the invocation entity key, so a junction row resolves without a read-back. */
    private final Map<Integer, String> invocationKeys = new HashMap<>();

    public void putInvocationKey(int poolIndex, String key) { invocationKeys.put(poolIndex, key); }

    /**
     * The pooled target's key.
     *
     * <p>Falls back to computing it, because the pool is read before the classes only by
     * convention — key order in JSON is not guaranteed, and a junction row pointing at a computed
     * key is recoverable where a null reference is not.
     */
    public String invocationKey(int poolIndex) {
        String k = invocationKeys.get(poolIndex);
        return k != null ? k
                : com.codeanalyzer.ingest.mongo.id.IdCodec.invocation(runId, poolIndex).key();
    }

    /**
     * A run-independent identity for a thing.
     *
     * <p>Prefixed with the dialect because the same table name legitimately exists in more than one
     * database — 40 names were measured appearing in several dialects — and they are different
     * objects. Everything after the dialect is the natural key of the thing itself, deliberately
     * excluding the run, which is what lets one entity be shared by many runs.
     */
    /** The run-independent scope prefix on its own — the dialect, or JAR. */
    public String entityKey() { return dialect; }

    public String entityKey(String... parts) {
        String[] all = new String[parts.length + 1];
        all[0] = dialect;
        System.arraycopy(parts, 0, all, 1, parts.length);
        return com.codeanalyzer.ingest.mongo.id.Escaping.join(all);
    }

    /**
     * The lookup key.
     *
     * <p>Upper-cased because the corpus mixes cases for the same object — PostgreSQL emits
     * {@code erp_product} where Oracle emits {@code ERP_PRODUCT} — and a case-sensitive lookup
     * would silently fail to match, producing an orphan rather than an error.
     */
    public static String objectKey(String schema, String name) {
        return (schema == null ? "" : schema.toUpperCase(java.util.Locale.ROOT))
                + "." + (name == null ? "" : name.toUpperCase(java.util.Locale.ROOT));
    }
}
