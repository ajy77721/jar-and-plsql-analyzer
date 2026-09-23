/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  org.bson.Document
 *  org.bson.types.ObjectId
 */
package com.flowpoc.layer2;

import com.flowpoc.layer2.MongoOp;
import com.flowpoc.layer2.ShadowAggregationExecutor;
import com.flowpoc.layer2.ShadowFilterMatcher;
import com.flowpoc.layer2.ShadowUpdateApplier;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.bson.Document;
import org.bson.types.ObjectId;

public class ShadowMongoStore {
    private final Map<String, List<Document>> store = new LinkedHashMap<String, List<Document>>();
    private final ShadowFilterMatcher matcher = new ShadowFilterMatcher();
    private final ShadowUpdateApplier updater = new ShadowUpdateApplier();

    public synchronized void applyWrite(MongoOp op, String collection, Object input, Object[] args) {
        switch (op) {
            case INSERT: {
                this.storeOne(collection, this.firstDocument(args));
                break;
            }
            case INSERT_MANY: {
                this.storeAll(collection, this.allDocuments(args));
                break;
            }
            case SAVE: {
                this.upsertById(collection, this.firstDocument(args));
                break;
            }
            case SAVE_ALL: {
                this.allDocuments(args).forEach(d -> this.upsertById(collection, (Document)d));
                break;
            }
            case UPDATE: {
                this.applyUpdate(collection, input, args, false, false);
                break;
            }
            case UPDATE_MANY: {
                this.applyUpdate(collection, input, args, true, false);
                break;
            }
            case UPSERT: {
                this.applyUpdate(collection, input, args, false, true);
                break;
            }
            case REPLACE: {
                this.applyReplace(collection, input, args);
                break;
            }
            case DELETE: {
                this.applyDelete(collection, input, false);
                break;
            }
            case DELETE_MANY: 
            case FIND_ALL_AND_REMOVE: {
                this.applyDelete(collection, input, true);
                break;
            }
            case BULK_WRITE: {
                this.applyBulkItems(collection, input);
                break;
            }
        }
    }

    public synchronized List<Map<String, Object>> query(String collection, Object filter, int limit) {
        List<Document> docs = this.store.get(collection);
        if (docs == null || docs.isEmpty()) {
            return Collections.emptyList();
        }
        return docs.stream().filter(d -> this.matcher.matches((Document)d, filter)).limit(limit).map(d -> new LinkedHashMap<String, Object>(d)).collect(Collectors.toList());
    }

    public synchronized long count(String collection, Object filter) {
        List<Document> docs = this.store.get(collection);
        if (docs == null) {
            return 0L;
        }
        return docs.stream().filter(d -> this.matcher.matches((Document)d, filter)).count();
    }

    public synchronized boolean exists(String collection, Object filter) {
        List<Document> docs = this.store.get(collection);
        if (docs == null) {
            return false;
        }
        return docs.stream().anyMatch(d -> this.matcher.matches((Document)d, filter));
    }

    public synchronized List<Map<String, Object>> aggregate(String collection, String pipelineJson, int limit) {
        List<Document> docs = this.store.get(collection);
        if (docs == null || docs.isEmpty()) {
            return Collections.emptyList();
        }
        List<Document> pipeline = ShadowAggregationExecutor.parsePipeline(pipelineJson);
        if (pipeline.isEmpty()) {
            return Collections.emptyList();
        }
        ShadowAggregationExecutor exec = new ShadowAggregationExecutor(this.store);
        return exec.execute(new ArrayList<Document>(docs), pipeline).stream().limit(limit).map(d -> new LinkedHashMap<String, Object>(d)).collect(Collectors.toList());
    }

    public synchronized boolean hasCollection(String collection) {
        List<Document> docs = this.store.get(collection);
        return docs != null && !docs.isEmpty();
    }

    public synchronized void seedCollection(String collection, List<Document> docs) {
        List<Document> coll = this.store.computeIfAbsent(collection, k -> new ArrayList<Document>());
        for (Document doc : docs) {
            Object id = doc.get((Object)"_id");
            boolean exists = coll.stream().anyMatch(d -> Objects.equals(d.get((Object)"_id"), id));
            if (exists) continue;
            coll.add(new Document((Map)doc));
        }
    }

    public synchronized long countMatching(String collection, Object filter) {
        List<Document> docs = this.store.get(collection);
        if (docs == null) {
            return 0L;
        }
        return docs.stream().filter(d -> this.matcher.matches((Document)d, filter)).count();
    }

    public synchronized WriteImpact applyWriteWithImpact(MongoOp op, String collection, Object input, Object[] args) {
        return switch (op) {
            case UPDATE -> this.applyUpdateImpact(collection, input, args, false, false);
            case UPDATE_MANY -> this.applyUpdateImpact(collection, input, args, true, false);
            case UPSERT -> this.applyUpdateImpact(collection, input, args, false, true);
            case DELETE -> this.applyDeleteImpact(collection, input, false);
            case DELETE_MANY, FIND_ALL_AND_REMOVE -> this.applyDeleteImpact(collection, input, true);
            default -> {
                this.applyWrite(op, collection, input, args);
                yield WriteImpact.none();
            }
        };
    }

    public synchronized void clear() {
        this.store.clear();
    }

    private void storeOne(String collection, Document doc) {
        if (doc == null) {
            return;
        }
        this.ensureId(doc);
        this.store.computeIfAbsent(collection, k -> new ArrayList()).add(doc);
    }

    private void storeAll(String collection, List<Document> docs) {
        docs.forEach(d -> this.storeOne(collection, (Document)d));
    }

    private void upsertById(String collection, Document doc) {
        if (doc == null) {
            return;
        }
        this.ensureId(doc);
        List<Document> coll = this.store.computeIfAbsent(collection, k -> new ArrayList<Document>());
        Object id = doc.get((Object)"_id");
        for (int i = 0; i < coll.size(); ++i) {
            if (!Objects.equals(((Document)coll.get(i)).get((Object)"_id"), id)) continue;
            coll.set(i, doc);
            return;
        }
        coll.add(doc);
    }

    private void applyUpdate(String collection, Object filterInput, Object[] args, boolean multi, boolean upsert) {
        Document filter = this.toDocument(filterInput);
        Document updateSpec = args != null && args.length > 1 ? this.toDocument(args[1]) : null;
        List<Document> docs = this.store.getOrDefault(collection, Collections.emptyList());
        boolean matched = false;
        for (Document doc : docs) {
            if (!this.matcher.matches(doc, filter)) continue;
            this.updater.apply(doc, updateSpec);
            matched = true;
            if (multi) continue;
            break;
        }
        if (!matched && upsert) {
            Document newDoc = filter != null ? new Document((Map)filter) : new Document();
            this.updater.apply(newDoc, updateSpec);
            this.storeOne(collection, newDoc);
        }
    }

    private void applyReplace(String collection, Object filterInput, Object[] args) {
        Document replacement;
        Document filter = this.toDocument(filterInput);
        Document document = replacement = args != null && args.length > 1 ? this.toDocument(args[1]) : null;
        if (replacement == null) {
            return;
        }
        List<Document> docs = this.store.computeIfAbsent(collection, k -> new ArrayList<Document>());
        for (int i = 0; i < docs.size(); ++i) {
            if (!this.matcher.matches((Document)docs.get(i), filter)) continue;
            Object id = ((Document)docs.get(i)).get((Object)"_id");
            Document rep = new Document((Map)replacement);
            if (id != null && !rep.containsKey((Object)"_id")) {
                rep.put("_id", id);
            }
            docs.set(i, rep);
            return;
        }
        this.storeOne(collection, new Document((Map)replacement));
    }

    private void applyDelete(String collection, Object filterInput, boolean multi) {
        List<Document> docs = this.store.get(collection);
        if (docs == null) {
            return;
        }
        Document filter = this.toDocument(filterInput);
        Iterator<Document> it = docs.iterator();
        while (it.hasNext()) {
            if (!this.matcher.matches(it.next(), filter)) continue;
            it.remove();
            if (multi) continue;
            return;
        }
    }

    private WriteImpact applyUpdateImpact(String collection, Object filterInput, Object[] args, boolean multi, boolean upsert) {
        Document filter = this.toDocument(filterInput);
        Document updateSpec = args != null && args.length > 1 ? this.toDocument(args[1]) : null;
        List<Document> docs = this.store.getOrDefault(collection, Collections.emptyList());
        ArrayList<Document> snapshots = new ArrayList<Document>();
        for (Document doc : docs) {
            if (!this.matcher.matches(doc, filter)) continue;
            snapshots.add(new Document((Map)doc));
            this.updater.apply(doc, updateSpec);
            if (multi) continue;
            break;
        }
        if (snapshots.isEmpty() && upsert) {
            Document newDoc = filter != null ? new Document((Map)filter) : new Document();
            this.updater.apply(newDoc, updateSpec);
            this.storeOne(collection, newDoc);
        }
        return new WriteImpact(snapshots.size(), snapshots);
    }

    private WriteImpact applyDeleteImpact(String collection, Object filterInput, boolean multi) {
        List<Document> docs = this.store.get(collection);
        if (docs == null) {
            return WriteImpact.none();
        }
        Document filter = this.toDocument(filterInput);
        ArrayList<Document> removed = new ArrayList<Document>();
        Iterator<Document> it = docs.iterator();
        while (it.hasNext()) {
            Document doc = it.next();
            if (!this.matcher.matches(doc, filter)) continue;
            removed.add(new Document((Map)doc));
            it.remove();
            if (multi) continue;
            break;
        }
        return new WriteImpact(removed.size(), removed);
    }

    private void applyBulkItems(String collection, Object input) {
    }

    private Document firstDocument(Object[] args) {
        if (args == null) {
            return null;
        }
        for (Object arg : args) {
            Document d = this.toDocument(arg);
            if (d == null || d.isEmpty()) continue;
            return d;
        }
        return null;
    }

    private List<Document> allDocuments(Object[] args) {
        ArrayList<Document> result = new ArrayList<Document>();
        if (args == null) {
            return result;
        }
        for (Object arg : args) {
            if (arg instanceof Iterable) {
                Iterable iter = (Iterable)arg;
                for (Object item : iter) {
                    Document d = this.toDocument(item);
                    if (d == null) continue;
                    result.add(d);
                }
                continue;
            }
            Document d = this.toDocument(arg);
            if (d == null) continue;
            result.add(d);
        }
        return result;
    }

    Document toDocument(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Document) {
            Document d = (Document)obj;
            return d;
        }
        if (obj instanceof Map) {
            Map m = (Map)obj;
            Document doc = new Document();
            m.forEach((k, v) -> doc.put(String.valueOf(k), v));
            return doc;
        }
        return this.pojoToDocument(obj);
    }

    private Document pojoToDocument(Object obj) {
        Document doc = new Document();
        if (this.isPrimitive(obj)) {
            return doc;
        }
        try {
            for (Method m : obj.getClass().getMethods()) {
                String n = m.getName();
                if (!n.startsWith("get") || n.length() <= 3 || m.getParameterCount() != 0 || n.equals("getClass")) continue;
                try {
                    Object val = m.invoke(obj, new Object[0]);
                    if (val == null) continue;
                    doc.put(this.lowerFirst(n.substring(3)), val);
                }
                catch (Exception exception) {
                    // empty catch block
                }
            }
        }
        catch (Exception exception) {
            // empty catch block
        }
        return doc;
    }

    private void ensureId(Document doc) {
        if (!doc.containsKey((Object)"_id")) {
            doc.put("_id", (Object)new ObjectId().toString());
        }
    }

    private boolean isPrimitive(Object obj) {
        return obj instanceof Number || obj instanceof Boolean || obj instanceof String || obj instanceof Character;
    }

    private String lowerFirst(String s) {
        if (s == null || s.isEmpty()) {
            return s;
        }
        return Character.toLowerCase(s.charAt(0)) + s.substring(1);
    }

    public record WriteImpact(long matchedCount, List<Document> snapshotBefore) {
        public static WriteImpact none() {
            return new WriteImpact(0L, Collections.emptyList());
        }
    }
}

