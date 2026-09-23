package com.codeanalyzer.ingest.mongo.verify;

import com.codeanalyzer.ingest.mongo.map.Splitter;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The checks MongoDB will not perform for us.
 *
 * <h2>Why this class exists at all</h2>
 *
 * <p>A relational database refuses a foreign key that points at nothing, and refuses a second row
 * with a duplicate primary key. MongoDB does neither: a reference is an ordinary string, and a
 * write that quietly replaces a document looks exactly like one that inserted it. Every identity
 * defect found while building this module — merged methods, vanished DAG edges, collapsed
 * endpoints — was silent, and each was caught by counting rather than by an error.
 *
 * <p>So the guarantees are re-created here as an explicit pass, run after ingest. It is not a
 * substitute for the database enforcing them, and it is not pretending to be: it is a report,
 * and its value is that it turns silence into a number someone can look at.
 */
public final class IntegrityChecker {

    /** One kind of problem, with enough detail to act on it. */
    public record Finding(String kind, String collection, String detail, long count) {
        @Override public String toString() {
            return String.format("%-18s %-26s %6d  %s", kind, collection, count, detail);
        }
    }

    /**
     * Reference fields, and the collection each must resolve into.
     *
     * <p>Held as data rather than as code so a new reference cannot be added without either
     * appearing here or being conspicuously absent from the report.
     */
    private static final Map<String, Map<String, String>> REFS = new LinkedHashMap<>();
    static {
        REFS.put(Collections.STATEMENT, Map.of(
                "nodeRef", Collections.PROC_NODE,
                "objectRef", Collections.DB_OBJECT));
        REFS.put(Collections.TABLE_COLUMN_USAGE, Map.of("objectRef", Collections.DB_OBJECT));
        REFS.put(Collections.CALL_EDGE, Map.of(
                "fromRef", Collections.PROC_NODE,
                "toRef", Collections.PROC_NODE));
        REFS.put(Collections.JAVA_METHOD, Map.of("classRef", Collections.JAVA_CLASS));
        REFS.put(Collections.JAVA_FIELD, Map.of("classRef", Collections.JAVA_CLASS));
        REFS.put(Collections.METHOD_INVOCATION, Map.of(
                "methodRef", Collections.JAVA_METHOD,
                "invocationRef", Collections.INVOCATION));
        REFS.put(Collections.DAG_EDGE, Map.of(
                "parentRef", Collections.DAG_NODE,
                "childRef", Collections.DAG_NODE));
        REFS.put(Collections.ENDPOINT, Map.of("rootRef", Collections.DAG_NODE));
    }

    private final MongoDatabase db;

    public IntegrityChecker(MongoDatabase db) {
        this.db = db;
    }

    /** Every problem found. An empty list is the only clean result. */
    public List<Finding> check(String runId) {
        List<Finding> out = new ArrayList<>();
        out.addAll(danglingRefs(runId));
        out.addAll(arraysInDocuments());
        out.addAll(unreachableFromRun(runId));
        return out;
    }

    /**
     * References that point at nothing.
     *
     * <p>Some dangling references are legitimate and expected — a call edge may name an external
     * callee that has no node of its own — so this reports rather than fails. The number is the
     * point: it should be small and stable, and a jump in it means something changed upstream.
     */
    private List<Finding> danglingRefs(String runId) {
        List<Finding> out = new ArrayList<>();
        for (var entry : REFS.entrySet()) {
            String coll = entry.getKey();
            for (var ref : entry.getValue().entrySet()) {
                List<String> values = db.getCollection(coll)
                        .distinct(ref.getKey(), Filters.eq("runId", runId), String.class)
                        .into(new ArrayList<>());
                if (values.isEmpty()) continue;
                Set<String> present = new HashSet<>(db.getCollection(ref.getValue())
                        .distinct("entityKey", String.class).into(new ArrayList<>()));
                long missing = values.stream().filter(v -> v != null && !present.contains(v)).count();
                if (missing > 0) {
                    out.add(new Finding("dangling-ref", coll,
                            ref.getKey() + " → " + ref.getValue(), missing));
                }
            }
        }
        return out;
    }

    /**
     * The no-arrays invariant, checked against what is stored rather than what was intended.
     *
     * <p>Sampled rather than exhaustive: the point is to notice that an array has reappeared, and
     * the first few hundred documents of a collection will show that. An exhaustive scan of tens of
     * millions of documents would make this too slow to run, and a check nobody runs finds nothing.
     */
    private List<Finding> arraysInDocuments() {
        List<Finding> out = new ArrayList<>();
        for (String name : db.listCollectionNames()) {
            if (!name.startsWith("am_")) continue;
            long offenders = 0;
            for (Document d : db.getCollection(name).find().limit(200)) {
                if (Splitter.containsArray(d)) offenders++;
            }
            if (offenders > 0) {
                out.add(new Finding("array-in-doc", name,
                        "documents holding an array; they will grow without bound", offenders));
            }
        }
        return out;
    }

    /** Entities with no membership row are invisible to every reader — stored but unreachable. */
    private List<Finding> unreachableFromRun(String runId) {
        List<Finding> out = new ArrayList<>();
        long members = db.getCollection("am_run_member").countDocuments(Filters.eq("runRef", runId));
        if (members == 0) {
            out.add(new Finding("no-membership", "am_run_member",
                    "run " + runId + " has no members; nothing would be visible", 1));
        }
        Document run = db.getCollection(Collections.RUN).find(Filters.eq("_id", runId)).first();
        if (run == null) {
            out.add(new Finding("missing-run", Collections.RUN, "no run marker for " + runId, 1));
        } else if (!"COMPLETE".equals(run.getString("status"))) {
            out.add(new Finding("run-not-complete", Collections.RUN,
                    "status is " + run.getString("status") + "; readers will not see this run", 1));
        }
        return out;
    }
}
