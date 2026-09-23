package com.codeanalyzer.ingest.mongo.write;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Layer 1 of reference integrity: a run that produced an unresolvable structural reference is
 * failed before it is ever published.
 *
 * <h2>Why this cannot be checked per document</h2>
 *
 * <p>Every reference here is a computed string, so a child may legitimately be written before its
 * parent — that is what removes the read-back from the write path and is not a defect to fix. A
 * synchronous check at {@code upsert} time would therefore reject correct writes purely on ordering.
 *
 * <h2>Why it cannot be checked against "what this run wrote", either</h2>
 *
 * <p>Deduplication means a parent may not be written by this run at all: if an identical entity
 * already exists from an earlier run, the upsert matches it and no new document appears. Comparing
 * references against a set of keys written during this run would report those as dangling — the
 * check would fail hardest exactly when dedup was working best.
 *
 * <p>So the question is asked of the database, once, after the writes and before the run is
 * published. The run is still {@code IN_PROGRESS} at that point, so a failure leaves data that no
 * reader can see rather than a published run with broken links.
 *
 * <h2>Structural references only</h2>
 *
 * <p>Not every dangling reference is a defect. A call edge may name an external or unresolved
 * callee that has no node of its own, and dropping that edge would lose a real dependency — those
 * are reported by {@code IntegrityChecker} and deliberately not fatal. The references below are the
 * ones where a missing target means the row is unreachable: a child whose parent does not exist can
 * never be found by any query that starts from the parent.
 */
public final class RefAssertion {

    /** Reference field → the collection it must resolve into, by owning collection. */
    private static final Map<String, Map<String, String>> STRUCTURAL = new LinkedHashMap<>();
    static {
        STRUCTURAL.put(Collections.STATEMENT, Map.of("nodeRef", Collections.PROC_NODE));
        STRUCTURAL.put(Collections.CURSOR_DECL, Map.of("nodeRef", Collections.PROC_NODE));
        STRUCTURAL.put(Collections.DYNAMIC_SQL, Map.of("nodeRef", Collections.PROC_NODE));
        STRUCTURAL.put(Collections.EXCEPTION_HANDLER, Map.of("nodeRef", Collections.PROC_NODE));
        STRUCTURAL.put(Collections.TXN_STATEMENT, Map.of("nodeRef", Collections.PROC_NODE));
        STRUCTURAL.put(Collections.VARIABLE_TYPE_REF, Map.of("nodeRef", Collections.PROC_NODE));
        STRUCTURAL.put(Collections.NODE_TABLE_ACCESS, Map.of("nodeRef", Collections.PROC_NODE));
        STRUCTURAL.put(Collections.JAVA_METHOD, Map.of("classRef", Collections.JAVA_CLASS));
        STRUCTURAL.put(Collections.JAVA_FIELD, Map.of("classRef", Collections.JAVA_CLASS));
        STRUCTURAL.put(Collections.METHOD_INVOCATION, Map.of("methodRef", Collections.JAVA_METHOD));
        STRUCTURAL.put(Collections.DAG_EDGE, Map.of("parentRef", Collections.DAG_NODE));
    }

    /** One unresolvable reference, named well enough to find the mapper that produced it. */
    public record Broken(String collection, String field, String target, String value, long count) {
        @Override public String toString() {
            return collection + "." + field + " → " + target + " : " + count
                    + " row(s) point at a missing document, e.g. " + value;
        }
    }

    /** Raised before the run is published. */
    public static final class BrokenReferencesException extends RuntimeException {
        private final transient List<Broken> broken;

        BrokenReferencesException(List<Broken> broken) {
            super("run aborted: " + broken.size() + " structural reference(s) resolve to nothing.\n  "
                    + String.join("\n  ", broken.stream().map(Object::toString).toList()));
            this.broken = broken;
        }

        public List<Broken> broken() {
            return broken;
        }
    }

    private final MongoDatabase db;

    public RefAssertion(MongoDatabase db) {
        this.db = db;
    }

    /** @throws BrokenReferencesException if any structural reference in this run resolves to nothing */
    public void assertAllResolve(String runId) {
        List<Broken> broken = check(runId);
        if (!broken.isEmpty()) throw new BrokenReferencesException(broken);
    }

    /** The unresolvable structural references for one run. Empty is the only clean result. */
    public List<Broken> check(String runId) {
        List<Broken> out = new ArrayList<>();
        for (var owner : STRUCTURAL.entrySet()) {
            String collection = owner.getKey();
            for (var ref : owner.getValue().entrySet()) {
                String field = ref.getKey();
                String target = ref.getValue();

                // distinct() rather than a scan of every row: 111,467 method invocations resolve to
                // 33,409 distinct methods, so this asks a much smaller question than it looks.
                List<String> values = db.getCollection(collection)
                        .distinct(field, Filters.eq("runId", runId), String.class)
                        .into(new ArrayList<>());
                if (values.isEmpty()) continue;

                // Asked of the database in batches, not one round trip per distinct value: doing
                // this one `find().first()` at a time (as before) is a genuine N+1 — 33,409 distinct
                // methods meant 33,409 sequential network round trips. Unnoticeable on localhost
                // (sub-millisecond RTT) but, at even a modest 20-50ms Atlas/WAN round-trip latency,
                // this single field alone could be 10+ minutes by itself, which is the dominant real
                // explanation for "fast on localhost, minutes on Atlas" found this session — not a
                // localhost-vs-Atlas write-concern or connection-pool difference. Batching the lookup
                // with $in turns N round trips into ceil(N / BATCH) and is semantically identical:
                // still asked of the database, not of a set of keys written by this run, so dedup
                // (a parent already published by an earlier run) still resolves correctly.
                java.util.Set<String> present = new java.util.HashSet<>();
                int batchSize = 5_000;
                for (int i = 0; i < values.size(); i += batchSize) {
                    List<String> batch = values.subList(i, Math.min(i + batchSize, values.size()));
                    for (Document hit : db.getCollection(target)
                            .find(Filters.in("entityKey", batch))
                            .projection(new Document("entityKey", 1).append("_id", 0))) {
                        present.add(hit.getString("entityKey"));
                    }
                }

                long missing = 0;
                String example = null;
                for (String v : values) {
                    if (v == null) continue;
                    if (!present.contains(v)) {
                        missing++;
                        if (example == null) example = v;
                    }
                }
                if (missing > 0) out.add(new Broken(collection, field, target, example, missing));
            }
        }
        return out;
    }
}
