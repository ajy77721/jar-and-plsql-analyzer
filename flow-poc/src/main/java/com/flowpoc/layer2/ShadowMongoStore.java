package com.flowpoc.layer2;

import org.bson.Document;

import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

/**
 * In-memory write buffer that enforces read-after-write consistency within a
 * single flow execution without ever touching the real database.
 *
 * Architecture:
 *   ShadowFilterMatcher       – evaluates full MongoDB filter operators ($gt/$in/$and/$or/…)
 *   ShadowUpdateApplier       – applies all update operators ($set/$inc/$push/$pull/…)
 *   ShadowAggregationExecutor – runs full pipeline stages ($match/$group/$sort/$lookup/…)
 *
 * Lifecycle (managed by CollectingQueryInterceptor):
 *   clear()    – wipes all in-memory documents; called between FlowResult executions
 */
public class ShadowMongoStore {

    // Thread-safe per-collection document stores
    private final Map<String, List<Document>> store = new LinkedHashMap<>();

    private final ShadowFilterMatcher matcher = new ShadowFilterMatcher();
    private final ShadowUpdateApplier updater = new ShadowUpdateApplier();

    // ── Apply writes ──────────────────────────────────────────────────────────

    public synchronized void applyWrite(MongoOp op, String collection, Object input, Object[] args) {
        switch (op) {
            case INSERT         -> storeOne(collection, firstDocument(args));
            case INSERT_MANY    -> storeAll(collection, allDocuments(args));
            case SAVE           -> upsertById(collection, firstDocument(args));
            case SAVE_ALL       -> allDocuments(args).forEach(d -> upsertById(collection, d));
            case UPDATE         -> applyUpdate(collection, input, args, false, false);
            case UPDATE_MANY    -> applyUpdate(collection, input, args, true,  false);
            case UPSERT         -> applyUpdate(collection, input, args, false, true);
            case REPLACE        -> applyReplace(collection, input, args);
            case DELETE         -> applyDelete(collection, input, false);
            case DELETE_MANY,
                 FIND_ALL_AND_REMOVE -> applyDelete(collection, input, true);
            case BULK_WRITE     -> applyBulkItems(collection, input);
            default             -> {}
        }
    }

    // ── Query (find / count / exists) ─────────────────────────────────────────

    public synchronized List<Map<String, Object>> query(String collection, Object filter, int limit) {
        List<Document> docs = store.get(collection);
        if (docs == null || docs.isEmpty()) return Collections.emptyList();
        return docs.stream()
                .filter(d -> matcher.matches(d, filter))
                .limit(limit)
                .map(d -> new LinkedHashMap<>(d))
                .collect(Collectors.toList());
    }

    public synchronized long count(String collection, Object filter) {
        List<Document> docs = store.get(collection);
        if (docs == null) return 0L;
        return docs.stream().filter(d -> matcher.matches(d, filter)).count();
    }

    public synchronized boolean exists(String collection, Object filter) {
        List<Document> docs = store.get(collection);
        if (docs == null) return false;
        return docs.stream().anyMatch(d -> matcher.matches(d, filter));
    }

    // ── Aggregation ───────────────────────────────────────────────────────────

    public synchronized List<Map<String, Object>> aggregate(String collection,
                                                             String pipelineJson, int limit) {
        List<Document> docs = store.get(collection);
        if (docs == null || docs.isEmpty()) return Collections.emptyList();

        List<Document> pipeline = ShadowAggregationExecutor.parsePipeline(pipelineJson);
        if (pipeline.isEmpty()) return Collections.emptyList();

        ShadowAggregationExecutor exec = new ShadowAggregationExecutor(store);
        return exec.execute(new ArrayList<>(docs), pipeline).stream()
                .limit(limit)
                .map(d -> new LinkedHashMap<>(d))
                .collect(Collectors.toList());
    }

    public synchronized boolean hasCollection(String collection) {
        List<Document> docs = store.get(collection);
        return docs != null && !docs.isEmpty();
    }

    // ── WriteImpact ───────────────────────────────────────────────────────────

    /**
     * Describes the real-DB impact of a blocked write operation.
     *
     * matchedCount – how many documents in shadow (pre-seeded from real DB) matched the filter
     * snapshotBefore – copies of those documents BEFORE the write was applied
     */
    public record WriteImpact(long matchedCount, List<Document> snapshotBefore) {
        public static WriteImpact none() { return new WriteImpact(0, Collections.emptyList()); }
    }

    /**
     * Seeds real-DB documents into the shadow without overwriting any document
     * already present (identified by _id).  Called by the interceptor before
     * applying a write so subsequent reads see the post-write state.
     */
    public synchronized void seedCollection(String collection, List<Document> docs) {
        List<Document> coll = store.computeIfAbsent(collection, k -> new ArrayList<>());
        for (Document doc : docs) {
            Object id = doc.get("_id");
            boolean exists = coll.stream().anyMatch(d -> Objects.equals(d.get("_id"), id));
            if (!exists) coll.add(new Document(doc));
        }
    }

    /** Returns how many shadow documents match filter (0 when collection absent). */
    public synchronized long countMatching(String collection, Object filter) {
        List<Document> docs = store.get(collection);
        if (docs == null) return 0L;
        return docs.stream().filter(d -> matcher.matches(d, filter)).count();
    }

    /**
     * Applies the write to shadow and returns a WriteImpact describing which
     * documents were touched.  Use this instead of applyWrite() when the caller
     * needs the impact count (e.g. the interceptor pre-read path).
     */
    public synchronized WriteImpact applyWriteWithImpact(MongoOp op, String collection,
                                                          Object input, Object[] args) {
        return switch (op) {
            case UPDATE      -> applyUpdateImpact(collection, input, args, false, false);
            case UPDATE_MANY -> applyUpdateImpact(collection, input, args, true,  false);
            case UPSERT      -> applyUpdateImpact(collection, input, args, false, true);
            case DELETE      -> applyDeleteImpact(collection, input, false);
            case DELETE_MANY,
                 FIND_ALL_AND_REMOVE -> applyDeleteImpact(collection, input, true);
            default -> { applyWrite(op, collection, input, args); yield WriteImpact.none(); }
        };
    }

    public synchronized void clear() {
        store.clear();
    }

    // ── Write helpers ─────────────────────────────────────────────────────────

    private void storeOne(String collection, Document doc) {
        if (doc == null) return;
        ensureId(doc);
        store.computeIfAbsent(collection, k -> new ArrayList<>()).add(doc);
    }

    private void storeAll(String collection, List<Document> docs) {
        docs.forEach(d -> storeOne(collection, d));
    }

    private void upsertById(String collection, Document doc) {
        if (doc == null) return;
        ensureId(doc);
        List<Document> coll = store.computeIfAbsent(collection, k -> new ArrayList<>());
        Object id = doc.get("_id");
        for (int i = 0; i < coll.size(); i++) {
            if (Objects.equals(coll.get(i).get("_id"), id)) {
                coll.set(i, doc);
                return;
            }
        }
        coll.add(doc);
    }

    private void applyUpdate(String collection, Object filterInput, Object[] args,
                              boolean multi, boolean upsert) {
        Document filter = toDocument(filterInput);
        Document updateSpec = args != null && args.length > 1 ? toDocument(args[1]) : null;

        List<Document> docs = store.getOrDefault(collection, Collections.emptyList());
        boolean matched = false;

        for (Document doc : docs) {
            if (matcher.matches(doc, filter)) {
                updater.apply(doc, updateSpec);
                matched = true;
                if (!multi) break;
            }
        }

        if (!matched && upsert) {
            Document newDoc = filter != null ? new Document(filter) : new Document();
            updater.apply(newDoc, updateSpec);
            storeOne(collection, newDoc);
        }
    }

    private void applyReplace(String collection, Object filterInput, Object[] args) {
        Document filter = toDocument(filterInput);
        Document replacement = args != null && args.length > 1 ? toDocument(args[1]) : null;
        if (replacement == null) return;

        List<Document> docs = store.computeIfAbsent(collection, k -> new ArrayList<>());
        for (int i = 0; i < docs.size(); i++) {
            if (matcher.matches(docs.get(i), filter)) {
                Object id = docs.get(i).get("_id");
                Document rep = new Document(replacement);
                if (id != null && !rep.containsKey("_id")) rep.put("_id", id);
                docs.set(i, rep);
                return;
            }
        }
        // No match → insert
        storeOne(collection, new Document(replacement));
    }

    private void applyDelete(String collection, Object filterInput, boolean multi) {
        List<Document> docs = store.get(collection);
        if (docs == null) return;
        Document filter = toDocument(filterInput);
        Iterator<Document> it = docs.iterator();
        while (it.hasNext()) {
            if (matcher.matches(it.next(), filter)) {
                it.remove();
                if (!multi) return;
            }
        }
    }

    private WriteImpact applyUpdateImpact(String collection, Object filterInput, Object[] args,
                                          boolean multi, boolean upsert) {
        Document filter     = toDocument(filterInput);
        Document updateSpec = args != null && args.length > 1 ? toDocument(args[1]) : null;
        List<Document> docs = store.getOrDefault(collection, Collections.emptyList());
        List<Document> snapshots = new ArrayList<>();

        for (Document doc : docs) {
            if (matcher.matches(doc, filter)) {
                snapshots.add(new Document(doc));   // snapshot before mutation
                updater.apply(doc, updateSpec);
                if (!multi) break;
            }
        }

        if (snapshots.isEmpty() && upsert) {
            Document newDoc = filter != null ? new Document(filter) : new Document();
            updater.apply(newDoc, updateSpec);
            storeOne(collection, newDoc);
        }

        return new WriteImpact(snapshots.size(), snapshots);
    }

    private WriteImpact applyDeleteImpact(String collection, Object filterInput, boolean multi) {
        List<Document> docs = store.get(collection);
        if (docs == null) return WriteImpact.none();
        Document filter = toDocument(filterInput);
        List<Document> removed = new ArrayList<>();
        Iterator<Document> it = docs.iterator();
        while (it.hasNext()) {
            Document doc = it.next();
            if (matcher.matches(doc, filter)) {
                removed.add(new Document(doc));
                it.remove();
                if (!multi) break;
            }
        }
        return new WriteImpact(removed.size(), removed);
    }

    @SuppressWarnings("unchecked")
    private void applyBulkItems(String collection, Object input) {
        // input is List<String> of op descriptions from BulkOperations proxy
        // Best-effort: we can't fully reconstruct ops from string descriptions
        // but we note the bulk write occurred in the shadow
    }

    // ── Document extraction ───────────────────────────────────────────────────

    private Document firstDocument(Object[] args) {
        if (args == null) return null;
        for (Object arg : args) {
            Document d = toDocument(arg);
            if (d != null && !d.isEmpty()) return d;
        }
        return null;
    }

    private List<Document> allDocuments(Object[] args) {
        List<Document> result = new ArrayList<>();
        if (args == null) return result;
        for (Object arg : args) {
            if (arg instanceof Iterable<?> iter) {
                for (Object item : iter) {
                    Document d = toDocument(item);
                    if (d != null) result.add(d);
                }
            } else {
                Document d = toDocument(arg);
                if (d != null) result.add(d);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    Document toDocument(Object obj) {
        if (obj == null)            return null;
        if (obj instanceof Document d) return d;
        if (obj instanceof Map<?,?> m) {
            Document doc = new Document();
            m.forEach((k, v) -> doc.put(String.valueOf(k), v));
            return doc;
        }
        return pojoToDocument(obj);
    }

    private Document pojoToDocument(Object obj) {
        Document doc = new Document();
        if (isPrimitive(obj)) return doc;
        try {
            for (Method m : obj.getClass().getMethods()) {
                String n = m.getName();
                if (!n.startsWith("get") || n.length() <= 3
                        || m.getParameterCount() != 0 || n.equals("getClass")) continue;
                try {
                    Object val = m.invoke(obj);
                    if (val != null) doc.put(lowerFirst(n.substring(3)), val);
                } catch (Exception ignored) {}
            }
        } catch (Exception ignored) {}
        return doc;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void ensureId(Document doc) {
        if (!doc.containsKey("_id")) doc.put("_id", new org.bson.types.ObjectId().toString());
    }

    private boolean isPrimitive(Object obj) {
        return obj instanceof Number || obj instanceof Boolean
                || obj instanceof String || obj instanceof Character;
    }

    private String lowerFirst(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toLowerCase(s.charAt(0)) + s.substring(1);
    }
}
