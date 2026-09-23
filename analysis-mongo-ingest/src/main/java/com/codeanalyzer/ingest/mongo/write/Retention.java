package com.codeanalyzer.ingest.mongo.write;

import com.codeanalyzer.ingest.mongo.id.Escaping;
import com.codeanalyzer.ingest.mongo.model.RunMember;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Dropping a run, without dropping the entities other runs still need.
 *
 * <h2>Why this cannot be a delete by {@code runId}</h2>
 *
 * <p>Entity documents are shared. Four archived runs of one dialect produce 29,172 memberships over
 * 7,293 entities — deleting every document carrying the dropped run's {@code runId} would remove
 * entities that three other runs still point at, and because the entity document is rewritten by
 * whichever run last saw it, the {@code runId} on it names an arbitrary run rather than an owner.
 * A delete keyed on it would be both too broad and unpredictable.
 *
 * <p>So membership is what gets deleted, and an entity is collected only once <b>no</b> membership
 * row refers to it. That is the whole reason membership was separated from the entity in the first
 * place; this is the operation that spends it.
 *
 * <h2>Three kinds of document, three rules</h2>
 *
 * <ol>
 *   <li><b>Entities</b> — have membership rows. Collected when their last membership goes.</li>
 *   <li><b>Split-array children</b> — have no membership of their own; they exist only as part of a
 *       parent. Collected when their {@code parentRef} no longer resolves. Leaving them would be
 *       invisible: nothing references them, no integrity check names them, and they would
 *       accumulate for the life of the database.</li>
 *   <li><b>Content-addressed pools</b> — {@code am_sql_text} is shared by construction and has no
 *       membership. Collected when no statement references it.</li>
 * </ol>
 */
public final class Retention {

    /** What a drop removed, so an operator can see the cost was proportional. */
    public record Removed(long memberships, long entities, long splitChildren, long pooled) {
        public long total() {
            return memberships + entities + splitChildren + pooled;
        }

        @Override public String toString() {
            return String.format("memberships=%,d entities=%,d splitChildren=%,d pooled=%,d",
                    memberships, entities, splitChildren, pooled);
        }
    }

    private final MongoDatabase db;

    public Retention(MongoDatabase db) {
        this.db = db;
    }

    /**
     * Removes one run and anything left unreferenced by its removal.
     *
     * <p>Order matters and is the reverse of ingest: membership first, so that "is this entity
     * still needed" has the right answer by the time it is asked. Doing it the other way round
     * would evaluate every entity as still-referenced and collect nothing.
     */
    public Removed dropRun(String runId) {
        // 1. Which entities did this run point at, and where do they live? Read before deleting;
        //    afterwards there is nothing left to tell us.
        Map<String, Set<String>> candidatesByCollection = new LinkedHashMap<>();
        for (Document m : db.getCollection(RunMember.COLLECTION)
                .find(Filters.eq("runRef", runId))) {
            String coll = m.getString("entityCollection");
            String ref = m.getString("entityRef");
            if (coll == null || ref == null) continue;
            candidatesByCollection.computeIfAbsent(coll, k -> new HashSet<>()).add(ref);
        }

        long memberships = db.getCollection(RunMember.COLLECTION)
                .deleteMany(Filters.eq("runRef", runId)).getDeletedCount();
        db.getCollection(Collections.RUN).deleteOne(Filters.eq("_id", runId));

        // 2. Collect entities no surviving run refers to.
        //
        // Batched into $in queries rather than one find().limit(1) per candidate ref: a run can
        // point at tens of thousands of entities, and a per-ref round trip is exactly the
        // per-document-round-trip cost that made RefAssertion take minutes against a networked
        // (e.g. Atlas) server instead of the sub-second it takes locally.
        long entities = 0;
        Set<String> collectedKeys = new HashSet<>();
        final int batchSize = 5_000;
        for (var e : candidatesByCollection.entrySet()) {
            List<String> refs = new ArrayList<>(e.getValue());
            for (int i = 0; i < refs.size(); i += batchSize) {
                List<String> batch = refs.subList(i, Math.min(i + batchSize, refs.size()));
                Set<String> stillUsed = new HashSet<>();
                for (Document m : db.getCollection(RunMember.COLLECTION)
                        .find(Filters.in("entityRef", batch))
                        .projection(new Document("entityRef", 1))) {
                    stillUsed.add(m.getString("entityRef"));
                }
                List<String> unused = new ArrayList<>();
                for (String ref : batch) {
                    if (!stillUsed.contains(ref)) unused.add(ref);
                }
                if (!unused.isEmpty()) {
                    entities += db.getCollection(e.getKey())
                            .deleteMany(Filters.in("entityKey", unused)).getDeletedCount();
                    collectedKeys.addAll(unused);
                }
            }
        }

        // 3. Everything left unreachable, swept to a fixpoint.
        long splitChildren = sweepUnreachable(collectedKeys);

        // 4. Pooled text nobody quotes any more.
        long pooled = collectUnreferencedSqlText();

        return new Removed(memberships, entities, splitChildren, pooled);
    }

    /**
     * Deletes dependent rows until nothing more becomes unreachable.
     *
     * <h2>Why one pass is not enough</h2>
     *
     * <p>Dependency here is a chain, not a pair. A procedure owns {@code am_subtree_table} rows via
     * {@code nodeRef}; each of those owns {@code am_subtree_table__dynamic} rows via
     * {@code parentRef}. Collecting the procedure makes the first level unreachable, and only
     * deleting <em>that</em> level makes the second unreachable. A single pass left 682 rows in
     * {@code am_subtree_table__dynamic} behind — invisible, since nothing references them and no
     * integrity check names them.
     *
     * <p>Two reference kinds are followed, because the model has two: {@code parentRef} for a split
     * array's children, and {@code nodeRef} for a procedure's children. Both mean "this row exists
     * only as part of that one", which is precisely what makes the row collectable rather than
     * merely dangling.
     *
     * <p>Iterating to a fixpoint rather than to a fixed depth: the depth is a property of the model
     * and would have to be revised every time a level is added, silently leaking until someone
     * counted.
     */
    private long sweepUnreachable(Set<String> deletedKeys) {
        long removed = 0;
        Set<String> frontier = new HashSet<>(deletedKeys);
        while (!frontier.isEmpty()) {
            Set<String> nextFrontier = new HashSet<>();
            for (String name : db.listCollectionNames()) {
                if (Collections.RUN.equals(name) || RunMember.COLLECTION.equals(name)) continue;
                for (String refField : new String[]{"parentRef", "nodeRef"}) {
                    Bson match = Filters.in(refField, frontier);
                    // Read the doomed rows' own keys first: they become the next frontier, and
                    // after the delete there is no way to learn what they were.
                    for (Document d : db.getCollection(name).find(match)
                            .projection(new Document("key", 1).append("entityKey", 1))) {
                        String k = d.getString("entityKey");
                        if (k == null) k = d.getString("key");
                        if (k != null) nextFrontier.add(k);
                    }
                    removed += db.getCollection(name).deleteMany(match).getDeletedCount();
                }
            }
            nextFrontier.removeAll(frontier);
            frontier = nextFrontier;
        }
        return removed;
    }

    /**
     * Deletes {@code am_sql_text} rows no statement references.
     *
     * <p>Separate from the entity sweep because this pool has no membership: it is deduplicated by
     * content across every run (measured 73% duplication — 3.7 MB of captured SQL, 1.0 MB
     * distinct), so the only thing that can retire a row is the absence of a referrer.
     */
    private long collectUnreferencedSqlText() {
        Set<String> referenced = new HashSet<>(db.getCollection(Collections.STATEMENT)
                .distinct("sqlTextRef", String.class).into(new ArrayList<>()));
        long removed = 0;
        List<String> doomed = new ArrayList<>();
        for (Document d : db.getCollection(Collections.SQL_TEXT)
                .find().projection(new Document("entityKey", 1))) {
            String key = d.getString("entityKey");
            if (key != null && !referenced.contains(key)) doomed.add(key);
            if (doomed.size() >= 1000) {
                removed += db.getCollection(Collections.SQL_TEXT)
                        .deleteMany(Filters.in("entityKey", doomed)).getDeletedCount();
                doomed.clear();
            }
        }
        if (!doomed.isEmpty()) {
            removed += db.getCollection(Collections.SQL_TEXT)
                    .deleteMany(Filters.in("entityKey", doomed)).getDeletedCount();
        }
        return removed;
    }

    /**
     * Everything whose key begins with {@code prefix} followed by a separator — DB-105.
     *
     * <h2>Why this is exact rather than approximate</h2>
     *
     * <p>Keys are {@code ~}-joined, and {@link Escaping} guarantees that a {@code ~} inside a
     * component is escaped. So an unescaped {@code ~} appears only between components, and
     * "{@code prefix} then a separator" cannot match halfway through a component's text. Without
     * that guarantee a prefix delete would be a guess: dropping {@code ORACLE~DEMO} would also take
     * {@code ORACLE~DEMO_ARCHIVE}, silently.
     *
     * <p>The separator is required, not optional — matching {@code prefix} alone is exactly the
     * bug above. And the prefix is regex-quoted, because a key component may legitimately contain
     * {@code $} (Oracle package separators) and {@code .}, both of which are regex metacharacters.
     */
    public long deleteByKeyPrefix(String collection, String prefix) {
        Bson filter = Filters.regex("key",
                Pattern.compile("^" + Pattern.quote(prefix) + Pattern.quote(String.valueOf(Escaping.SEP))));
        return db.getCollection(collection).deleteMany(filter).getDeletedCount();
    }
}
