package com.codeanalyzer.ingest.mongo.verify;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Rebuilds a document's arrays from the collections they were split into.
 *
 * <h2>Why this exists, and why it is not merely a test helper</h2>
 *
 * <p>Splitting arrays out is what keeps documents bounded, but it only preserves the data if the
 * original shape can be recovered. Without reassembly, "nothing was lost" is a claim about code
 * nobody has run backwards, and the round-trip proof would be comparing a stripped document
 * against a full one and calling the difference acceptable — proving the opposite of what it says.
 *
 * <p>It is also the read path the UI needs. A screen asking for a table's columns must get an
 * array; that they are stored as rows is a storage decision and should not leak into consumers.
 *
 * <h2>How a row finds its way home</h2>
 *
 * <p>Every split row carries {@code parentRef} (the key of the row or document that owns it),
 * {@code path} (its dotted location <em>relative to that owner</em>) and {@code ordinal}.
 *
 * <p>Relative is the load-bearing word. An absolute path from the document root cannot say which
 * of several array elements a nested row belongs to — {@code tables.joins} is true of every
 * element of {@code tables}. Reattachment is therefore recursive: a row that is itself an object
 * becomes the owner for anything nested inside it, found by its own key.
 */
public final class Reassembler {

    /** Bookkeeping added by the storage layer; never part of the source document. */
    private static final List<String> STORAGE_FIELDS =
            List.of("_id", "key", "parentRef", "runId", "field", "path", "ordinal",
                    "entityKey", "contentHash");

    private final MongoDatabase db;
    private final List<String> collections;

    public Reassembler(MongoDatabase db) {
        this.db = db;
        this.collections = db.listCollectionNames().into(new ArrayList<>());
    }

    /**
     * Returns {@code parent} with its split-out arrays restored, recursively.
     *
     * @param parentCollection the collection {@code parent} came from
     * @param parentKey        the key its children recorded as {@code parentRef}
     * @param fields           ignored; kept for call-site compatibility, all fields are restored
     */
    public Document reassemble(String parentCollection, String parentKey, Document parent,
                               List<String> fields) {
        Document out = strip(parent);
        graftChildrenOf(parentCollection, parentKey, out);
        return out;
    }

    /**
     * Finds every row owned by {@code ownerKey} and places it back.
     *
     * <p>Only the child collections of {@code parentCollection} are searched, so this does not
     * scan the database; each is indexed on {@code parentRef}, making the lookup one seek.
     */
    private void graftChildrenOf(String parentCollection, String ownerKey, Document target) {
        String prefix = parentCollection + "__";
        for (String coll : collections) {
            if (!coll.startsWith(prefix)) continue;
            List<Document> rows = db.getCollection(coll)
                    .find(Filters.eq("parentRef", ownerKey))
                    .sort(Sorts.ascending("ordinal"))
                    .into(new ArrayList<>());
            if (rows.isEmpty()) continue;

            // Group by relative path, shallowest first: a deeper path is placed inside a container
            // that a shallower one may still create.
            Map<String, List<Document>> byPath = new LinkedHashMap<>();
            rows.stream()
                .sorted(Comparator.comparingInt(r -> depth(r.getString("path"))))
                .forEach(r -> byPath.computeIfAbsent(r.getString("path"), k -> new ArrayList<>()).add(r));

            for (Map.Entry<String, List<Document>> e : byPath.entrySet()) {
                List<Object> values = new ArrayList<>();
                for (Document row : e.getValue()) {
                    Document copy = strip(row);
                    if (copy.size() == 1 && copy.containsKey("value")) {
                        values.add(copy.get("value"));          // a scalar element
                    } else {
                        // An object element owns anything nested inside it, keyed by its own key.
                        graftChildrenOf(parentCollection, row.getString("key"), copy);
                        values.add(copy);
                    }
                }
                placeAt(target, e.getKey(), values);
            }
        }
    }

    private static int depth(String path) {
        return path == null || path.isEmpty() ? 0 : path.split("\\.").length;
    }

    private static Document strip(Document d) {
        Document copy = new Document(d);
        STORAGE_FIELDS.forEach(copy::remove);
        return copy;
    }

    private void placeAt(Document target, String dottedPath, List<Object> values) {
        String[] parts = dottedPath.split("\\.");
        Document cursor = target;
        for (int i = 0; i < parts.length - 1; i++) {
            Object next = cursor.get(parts[i]);
            Document step;
            if (next instanceof Document existing) {
                step = existing;
            } else {
                step = new Document();
                cursor.put(parts[i], step);
            }
            cursor = step;
        }
        cursor.put(parts[parts.length - 1], values);
    }
}
