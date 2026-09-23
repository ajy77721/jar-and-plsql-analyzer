package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.map.ArtifactMapper;
import com.codeanalyzer.ingest.mongo.map.IngestContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * VER-003 — a real {@code CountReconciler}, built once "the baseline" had a real, reproducible
 * meaning instead of an undefined one.
 *
 * <h2>What "the baseline" turned out to mean</h2>
 *
 * <p>The row as originally worded asked for a reconciler against "source JSON + baselines" from
 * "§3-§5" of a design document — sections that turned out to be schema design prose with no count
 * table in them (recorded in the task registry). Rather than invent a table (which would immediately
 * go stale the moment the source format or a mapper changed, the exact failure mode
 * {@code AnalysisJsonMapperCountTest}'s own header comment already warns against — "the number came
 * from one analysis and the ingest from another"), the baseline here is <b>computed from the real
 * source file at the moment of the check</b>, the same discipline that test already uses for one
 * collection, generalised to every collection a mapper touches.
 *
 * <h2>What this reconciles</h2>
 *
 * <p>{@link #tally} runs a real {@link ArtifactMapper} over a real source file and counts how many
 * documents it emits per collection — the "expected" side, with no database involved.
 * {@link #reconcile} then compares that tally against an "actual" count per collection (however the
 * caller obtained it — a live query, a snapshot, a second mapper run) and reports every collection
 * where the two disagree. A collection present on only one side is a discrepancy too — a mapper that
 * stopped emitting a collection, or a writer that started dropping one entirely, is exactly the kind
 * of gap a reconciler exists to catch, not a case to silently skip.
 */
public final class CountReconciler {

    private CountReconciler() {
    }

    /** One collection's expected-vs-actual mismatch. Never constructed for a collection that matches. */
    public record Discrepancy(String collection, long expected, long actual) {

        public long difference() {
            return actual - expected;
        }
    }

    /**
     * Runs {@code mapper} over {@code in} and counts the documents it emits, per collection.
     * Streaming: nothing beyond the running per-collection counters is retained, matching the
     * mappers' own "one element at a time" discipline (see {@code AnalysisJsonMapper}'s class
     * javadoc) rather than materialising every emitted document just to count them.
     */
    public static Map<String, Long> tally(ArtifactMapper mapper, IngestContext ctx, InputStream in)
            throws IOException {
        Map<String, Long> counts = new LinkedHashMap<>();
        mapper.map(ctx, in, (collection, doc) -> counts.merge(collection, 1L, Long::sum));
        return counts;
    }

    /**
     * Compares two collection→count maps and returns one {@link Discrepancy} per collection whose
     * counts differ, sorted by collection name for a deterministic, readable report. A collection
     * missing from one map is treated as a count of zero there — not skipped — so "the mapper
     * stopped emitting this collection entirely" and "the writer never wrote it" both surface.
     */
    public static List<Discrepancy> reconcile(Map<String, Long> expected, Map<String, Long> actual) {
        TreeSet<String> collections = new TreeSet<>();
        collections.addAll(expected.keySet());
        collections.addAll(actual.keySet());

        List<Discrepancy> discrepancies = new ArrayList<>();
        for (String c : collections) {
            long exp = expected.getOrDefault(c, 0L);
            long act = actual.getOrDefault(c, 0L);
            if (exp != act) discrepancies.add(new Discrepancy(c, exp, act));
        }
        return discrepancies;
    }
}
