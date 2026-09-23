package com.codeanalyzer.ingest.mongo.verify;

import com.codeanalyzer.ingest.mongo.model.RunMember;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Every reference in the database, checked — including the ones nobody remembered to declare.
 *
 * <h2>Why this discovers rather than declares</h2>
 *
 * <p>{@link IntegrityChecker} checks a hand-written map of eight collections. The database has ref
 * fields in dozens: every {@code Splitter}-generated collection carries {@code parentRef}, and each
 * new relation adds its own. A hand-written list does not stay complete — it acquires entries one
 * at a time, and the gap between it and the data is invisible, because a reference nobody checks
 * looks exactly like a reference that is fine.
 *
 * <p>So the field set is read off the documents. Any field whose name ends in {@code Ref} is a
 * reference and must resolve, and a reference whose target cannot be determined is <b>reported</b>
 * rather than skipped. Skipping the unknown is precisely how the declared list stayed incomplete.
 *
 * <h2>Both sides of a junction</h2>
 *
 * <p>A junction row is only correct if both of its references resolve — {@code am_method_invocation}
 * needs its method and its invocation, {@code am_dag_edge} its parent and its child. Discovery gets
 * this for free: both are fields ending in {@code Ref}, so neither can be checked and the other
 * forgotten.
 */
public final class OrphanRefChecker {

    /**
     * Reference fields whose target is not derivable from the field or collection name.
     *
     * <p>{@code parentRef} needs no entry: a split collection is named {@code parent__field}, so its
     * parent is the part before the separator. Everything else is declared here, and anything
     * declared nowhere is reported as unresolvable rather than passed over.
     */
    private static final Map<String, String> TARGETS = new LinkedHashMap<>();
    static {
        TARGETS.put("nodeRef", Collections.PROC_NODE);
        TARGETS.put("objectRef", Collections.DB_OBJECT);
        TARGETS.put("fromRef", Collections.PROC_NODE);
        TARGETS.put("toRef", Collections.PROC_NODE);
        TARGETS.put("classRef", Collections.JAVA_CLASS);
        TARGETS.put("methodRef", Collections.JAVA_METHOD);
        TARGETS.put("invocationRef", Collections.INVOCATION);
        TARGETS.put("childRef", Collections.DAG_NODE);
        TARGETS.put("rootRef", Collections.DAG_NODE);
        TARGETS.put("endpointRef", Collections.ENDPOINT);
        TARGETS.put("sqlTextRef", Collections.SQL_TEXT);
        TARGETS.put("poolRef", Collections.ENDPOINT_POOL);
        TARGETS.put("sequenceRef", Collections.SEQUENCE_OBJ);
        TARGETS.put("cursorRef", Collections.CURSOR_DECL);
    }

    /**
     * Collections where {@code parentRef} does not mean "my split parent".
     *
     * <p>The field name is overloaded, which discovery is what revealed. In a split collection it
     * addresses the entity the array was split out of; in {@code am_dag_edge} it is one side of a
     * graph edge and addresses a DAG node. Deriving the target from the collection name alone gets
     * the second case wrong and reports every edge in the graph as dangling.
     */
    private static final Map<String, String> PARENT_REF_OVERRIDES =
            Map.of(Collections.DAG_EDGE, Collections.DAG_NODE,
                    // am_bundled_jar_resource does not follow the am_x__field split-collection
                    // convention its parentRef target would otherwise be derived from — it is a
                    // genuinely new top-level collection, keyed by position rather than by name,
                    // not an auto-split child of one.
                    Collections.BUNDLED_JAR_RESOURCE, Collections.BUNDLED_JAR,
                    Collections.JAR_DEPENDENCY, Collections.BUNDLED_JAR);

    /**
     * Reference fields whose meaning depends on the collection holding them.
     *
     * <p>{@code nodeRef} is the clearest case: in the PL/SQL model it addresses a procedure, and in
     * {@code am_endpoint_reachable} it addresses a call-tree DAG node. One declared target is
     * necessarily wrong for one of them — here it reported all 3,784 JAR nodes as dangling.
     */
    private static final Map<String, Map<String, String>> PER_COLLECTION = Map.of(
            Collections.ENDPOINT_REACHABLE, Map.of("nodeRef", Collections.DAG_NODE),
            // Added after this checker flagged it on the collection's first ingest: a DAG node's
            // collections address the DAG, not a PL/SQL procedure. The same overload, caught the
            // day the collection appeared rather than in the middle layer months later.
            Collections.DAG_NODE_COLLECTION, Map.of("nodeRef", Collections.DAG_NODE));

    /**
     * {@code am_run_member.entityRef} is polymorphic: the collection it addresses is named by the
     * row's own {@code entityCollection} field. Resolved per row rather than per field, since no
     * single target is correct for the collection as a whole.
     */
    private static final String POLYMORPHIC = "\u0000polymorphic";

    /**
     * References that are legitimately absent, with the reason — VER-002.
     *
     * <p>A null reference is not the same defect as a dangling one. A call edge may name an external
     * callee that has no node; an invocation may resolve to no known method. Flagging those would
     * bury the real findings in noise, and the honest way to exclude them is a list with a
     * justification rather than a blanket "ignore nulls".
     */
    private static final Map<String, String> NULLABLE = Map.of(
            "methodRef", "an invocation may target a method outside the analysed jar",
            "objectRef", "a statement may name a table absent from the catalogue",
            "toRef", "a call edge may name an external or unresolved callee",
            "nodeRef", "a statement may belong to a node with no document of its own");

    /** One problem, named well enough to act on. */
    public record Orphan(String collection, String field, String target, long missing,
                         String example, boolean unresolvableTarget) {
        @Override public String toString() {
            if (unresolvableTarget) {
                return String.format("%-34s %-14s NO KNOWN TARGET — declare it in OrphanRefChecker",
                        collection, field);
            }
            return String.format("%-34s %-14s → %-24s %6d missing, e.g. %s",
                    collection, field, target, missing, example);
        }
    }

    private final MongoDatabase db;

    public OrphanRefChecker(MongoDatabase db) {
        this.db = db;
    }

    /** Every reference field present in the data, by collection. */
    public Map<String, Set<String>> discoverRefFields() {
        Map<String, Set<String>> out = new LinkedHashMap<>();
        for (String name : db.listCollectionNames()) {
            if (!name.startsWith("am_")) continue;
            Set<String> fields = new LinkedHashSet<>();
            // Sampled rather than exhaustive, but widely: a field present on only a handful of rows
            // of a large collection is exactly the kind that a two-document sample misses.
            for (Document d : db.getCollection(name).find().limit(500)) {
                for (String key : d.keySet()) {
                    if (key.endsWith("Ref")) fields.add(key);
                }
            }
            if (!fields.isEmpty()) out.put(name, fields);
        }
        return out;
    }

    /**
     * The collection a reference must resolve into, or null when nothing declares it.
     *
     * <p>{@code parentRef} is derived: {@code am_x__y} is a split of {@code am_x}. That is a rule
     * rather than a list precisely so a new split collection is covered the moment it exists.
     */
    String targetOf(String collection, String field) {
        if (RunMember.COLLECTION.equals(collection)) {
            if ("runRef".equals(field)) return Collections.RUN;
            if ("entityRef".equals(field)) return POLYMORPHIC;
        }
        Map<String, String> perCollection = PER_COLLECTION.get(collection);
        if (perCollection != null && perCollection.containsKey(field)) {
            return perCollection.get(field);
        }
        if ("parentRef".equals(field)) {
            String override = PARENT_REF_OVERRIDES.get(collection);
            if (override != null) return override;
            int sep = collection.indexOf("__");
            return sep > 0 ? collection.substring(0, sep) : null;
        }
        return TARGETS.get(field);
    }

    /** Every unresolvable reference in the database. An empty list is the only clean result. */
    public List<Orphan> check() {
        List<Orphan> out = new ArrayList<>();
        for (var entry : discoverRefFields().entrySet()) {
            String collection = entry.getKey();
            for (String field : entry.getValue()) {
                String target = targetOf(collection, field);
                if (target == null) {
                    // Reported, not skipped. A reference with no declared target is an unchecked
                    // reference, and an unchecked reference is indistinguishable from a valid one.
                    out.add(new Orphan(collection, field, null, 0, null, true));
                    continue;
                }
                Orphan o = POLYMORPHIC.equals(target)
                        ? checkPolymorphic(collection, field)
                        : checkOne(collection, field, target);
                if (o != null) out.add(o);
            }
        }
        return out;
    }

    /**
     * A reference whose target collection is named by a sibling field, checked per row.
     *
     * <p>{@code am_run_member} is the only such relation and it is central: it is how an entity is
     * attached to a run. A single declared target would be wrong for every collection but one, so
     * skipping it — which is what the declared list did — left the most load-bearing reference in
     * the model unchecked.
     */
    private Orphan checkPolymorphic(String collection, String field) {
        long missing = 0;
        String example = null;
        for (Document d : db.getCollection(collection).find()
                .projection(new Document(field, 1).append("entityCollection", 1))) {
            String ref = d.getString(field);
            String target = d.getString("entityCollection");
            if (ref == null || target == null) continue;
            if (keysOf(target).contains(ref)) continue;
            missing++;
            if (example == null) example = ref + " (in " + target + ")";
        }
        if (missing == 0) return null;
        return new Orphan(collection, field, "per-row entityCollection", missing, example, false);
    }

    /** Cached key set of a collection: both naming conventions, read once per target. */
    private final Map<String, Set<String>> keyCache = new LinkedHashMap<>();

    private Set<String> keysOf(String target) {
        return keyCache.computeIfAbsent(target, t -> {
            Set<String> keys = new HashSet<>(db.getCollection(t)
                    .distinct("entityKey", String.class).into(new ArrayList<>()));
            keys.addAll(db.getCollection(t).distinct("key", String.class).into(new ArrayList<>()));
            return keys;
        });
    }

    private Orphan checkOne(String collection, String field, String target) {
        List<String> values = db.getCollection(collection)
                .distinct(field, String.class).into(new ArrayList<>());
        if (values.isEmpty()) return null;

        // A parent may be addressed by either name: content-addressed entities carry entityKey,
        // run-scoped rows carry key. Checking one alone reports the other kind as dangling.
        Set<String> present = new HashSet<>(keysOf(target));

        // Splits nest. Splitter names a collection after the OUTERMOST schema field, so an array
        // inside an already-split element lands in the same collection and its parentRef addresses
        // a sibling row rather than the base entity. Both are legitimate parents.
        if (collection.contains("__")) present.addAll(keysOf(collection));

        long missing = 0;
        String example = null;
        for (String v : values) {
            if (v == null) continue;
            if (present.contains(v)) continue;
            missing++;
            if (example == null) example = v;
        }
        if (missing == 0) return null;
        return new Orphan(collection, field, target, missing, example, false);
    }

    /** True when a null value in this field is expected rather than a defect — see VER-002. */
    public static boolean nullAllowed(String field) {
        return NULLABLE.containsKey(field);
    }

    /** Why a null is allowed there, for a report that explains itself. */
    public static String nullReason(String field) {
        return NULLABLE.get(field);
    }
}
