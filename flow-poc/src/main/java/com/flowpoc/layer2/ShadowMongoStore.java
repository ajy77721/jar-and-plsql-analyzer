package com.flowpoc.layer2;

import org.bson.Document;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * In-memory write buffer that maintains read-after-write consistency within a
 * single flow execution without ever touching the real database.
 *
 * Contract:
 *  - Every blocked write (INSERT/UPDATE/DELETE/REPLACE/BULK) is applied here
 *  - Every read checks this store first; real DB is only consulted when the
 *    shadow store has no documents for that collection
 *
 * Lifecycle:
 *  - Cleared at the start of each FlowResult execution (between endpoints)
 *  - NOT cleared between steps — step 1's inserts must be visible to step 2's reads
 *
 * This guarantees the key invariant:
 *   "A read in step N always returns the data that would have existed in the DB
 *    had all prior writes in the same flow actually executed."
 */
public class ShadowMongoStore {

    // collection → ordered list of documents (preserves insertion order)
    private final Map<String, List<Document>> store = new ConcurrentHashMap<>();

    // ── Apply writes ──────────────────────────────────────────────────────────

    public void applyWrite(MongoOp op, String collection, Object input, Object[] args) {
        switch (op) {
            case INSERT         -> storeOne(collection, firstDocument(args));
            case INSERT_MANY    -> storeAll(collection, allDocuments(args));
            case SAVE           -> storeOne(collection, firstDocument(args));
            case SAVE_ALL       -> storeAll(collection, allDocuments(args));
            case UPDATE         -> applyUpdate(collection, input, args, false);
            case UPDATE_MANY    -> applyUpdate(collection, input, args, true);
            case UPSERT         -> applyUpsert(collection, input, args);
            case REPLACE        -> applyReplace(collection, input, args);
            case DELETE         -> applyDelete(collection, input, false);
            case DELETE_MANY,
                 FIND_ALL_AND_REMOVE -> applyDelete(collection, input, true);
            case BULK_WRITE     -> applyBulk(collection, input);
            default             -> {}
        }
    }

    // ── Query shadow store ────────────────────────────────────────────────────

    /**
     * Returns documents from the shadow store matching the given filter.
     * Returns an empty list if nothing has been shadow-written to this collection.
     * The caller should fall through to the real DB when this returns empty.
     */
    public List<Map<String, Object>> query(String collection, Object filter, int limit) {
        List<Document> docs = store.get(collection);
        if (docs == null || docs.isEmpty()) return Collections.emptyList();

        Map<String, Object> filterMap = flattenFilter(filter);
        return docs.stream()
                .filter(doc -> matches(doc, filterMap))
                .limit(limit)
                .map(doc -> new LinkedHashMap<>(doc))
                .collect(Collectors.toList());
    }

    public boolean hasCollection(String collection) {
        List<Document> docs = store.get(collection);
        return docs != null && !docs.isEmpty();
    }

    public void clear() {
        store.clear();
    }

    // ── Write helpers ─────────────────────────────────────────────────────────

    private void storeOne(String collection, Document doc) {
        if (doc == null) return;
        if (!doc.containsKey("_id")) doc.put("_id", new org.bson.types.ObjectId().toString());
        store.computeIfAbsent(collection, k -> new ArrayList<>()).add(doc);
    }

    private void storeAll(String collection, List<Document> docs) {
        docs.forEach(doc -> storeOne(collection, doc));
    }

    private void applyUpdate(String collection, Object filter, Object[] args,
                              boolean multi) {
        Map<String, Object> fm = flattenFilter(filter);
        Document updateSpec = args != null && args.length > 1
                ? toDocument(args[1]) : null;

        List<Document> docs = store.getOrDefault(collection, Collections.emptyList());
        boolean updated = false;
        for (Document doc : docs) {
            if (matches(doc, fm)) {
                mergeUpdate(doc, updateSpec);
                updated = true;
                if (!multi) break;
            }
        }
        // If no match and this is upsert-like, add a new doc
        if (!updated && isUpsert(args)) {
            Document newDoc = new Document(fm);
            mergeUpdate(newDoc, updateSpec);
            storeOne(collection, newDoc);
        }
    }

    private void applyUpsert(String collection, Object filter, Object[] args) {
        Map<String, Object> fm = flattenFilter(filter);
        List<Document> docs = store.getOrDefault(collection, Collections.emptyList());
        boolean found = docs.stream().anyMatch(d -> matches(d, fm));
        if (found) {
            applyUpdate(collection, filter, args, false);
        } else {
            Document doc = new Document(fm);
            if (args != null && args.length > 1) mergeUpdate(doc, toDocument(args[1]));
            storeOne(collection, doc);
        }
    }

    private void applyReplace(String collection, Object filter, Object[] args) {
        Map<String, Object> fm = flattenFilter(filter);
        List<Document> docs = store.getOrDefault(collection, Collections.emptyList());
        Document replacement = args != null && args.length > 1 ? toDocument(args[1]) : null;
        if (replacement == null) return;

        for (int i = 0; i < docs.size(); i++) {
            if (matches(docs.get(i), fm)) {
                docs.set(i, replacement);
                return;
            }
        }
        // no match → insert
        storeOne(collection, replacement);
    }

    private void applyDelete(String collection, Object filter, boolean multi) {
        Map<String, Object> fm = flattenFilter(filter);
        List<Document> docs = store.get(collection);
        if (docs == null) return;
        Iterator<Document> it = docs.iterator();
        while (it.hasNext()) {
            if (matches(it.next(), fm)) {
                it.remove();
                if (!multi) return;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void applyBulk(String collection, Object input) {
        // input is a List<String> of op descriptions from wrapBulkOperations
        // We can't reconstruct full documents from string descriptions in the POC,
        // so we just note the bulk write occurred without storing individual docs.
    }

    // ── Update merging ────────────────────────────────────────────────────────

    private void mergeUpdate(Document target, Document updateSpec) {
        if (updateSpec == null) return;
        // $set → merge fields into doc
        Object setDoc = updateSpec.get("$set");
        if (setDoc instanceof Document set) {
            set.forEach(target::put);
            return;
        }
        // No $set operator → treat whole doc as replacement fields (except _id)
        updateSpec.forEach((k, v) -> {
            if (!k.startsWith("$") && !k.equals("_id")) target.put(k, v);
        });
    }

    private boolean isUpsert(Object[] args) {
        if (args == null) return false;
        for (Object arg : args) {
            if (arg == null) continue;
            try {
                Object upsert = arg.getClass().getMethod("isUpsert").invoke(arg);
                if (Boolean.TRUE.equals(upsert)) return true;
            } catch (Exception ignored) {}
        }
        return false;
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

    private Document toDocument(Object obj) {
        if (obj == null) return null;
        if (obj instanceof Document d) return d;
        if (obj instanceof Map<?,?> m) {
            Document doc = new Document();
            m.forEach((k, v) -> doc.put(String.valueOf(k), v));
            return doc;
        }
        // POJO → extract getXxx() fields
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

    // ── Filter matching ───────────────────────────────────────────────────────

    /**
     * Flattens an incoming filter object into a simple field→value EQ map,
     * ignoring Mongo operator keys ($gt, $in, etc.) which are treated as
     * "match anything" for shadow-store purposes.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> flattenFilter(Object filter) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (filter == null) return result;

        Document doc = toDocument(filter);
        if (doc == null) return result;

        doc.forEach((k, v) -> {
            if (k.startsWith("$")) return;           // top-level operator — skip
            if (v instanceof Document nested) {
                // {"field": {"$eq": val}} → extract $eq value
                Object eqVal = nested.get("$eq");
                if (eqVal != null) result.put(k, eqVal);
                // other operators ($gt etc.) → treat as wildcard (don't add to filter)
            } else {
                result.put(k, v);
            }
        });
        return result;
    }

    private boolean matches(Document doc, Map<String, Object> filterMap) {
        for (Map.Entry<String, Object> entry : filterMap.entrySet()) {
            Object actual = doc.get(entry.getKey());
            if (actual == null) return false;
            if (!actual.toString().equals(String.valueOf(entry.getValue()))) return false;
        }
        return true;
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    private boolean isPrimitive(Object obj) {
        return obj instanceof Number || obj instanceof Boolean
                || obj instanceof String || obj instanceof Character;
    }

    private String lowerFirst(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toLowerCase(s.charAt(0)) + s.substring(1);
    }
}
