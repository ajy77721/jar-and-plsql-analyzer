package com.analyzer.arch;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * MID-023 — every MongoDB query carries a time budget.
 *
 * <h2>What an untimed query does</h2>
 *
 * <p>A query with no usable index does not fail — it scans. At the measured corpus that is a few
 * thousand documents and nobody notices; at the target volume it is a million and the request never
 * returns, holding a connection and a thread. What reaches anyone is "the page is slow", with
 * nothing naming the query responsible.
 *
 * <h2>What the driver upgrade changed, and what it did not</h2>
 *
 * <p>Driver 5.2.1 has CSOT, and {@link com.analyzer.storage.mongo.MongoConnection} now sets one
 * client-wide operation timeout ({@code QueryBudget.BATCH_MS}). So the original justification for
 * this gate is <b>gone</b>: an unbudgeted query can no longer hang forever, whether or not anyone
 * remembered it. That is the chokepoint this class used to say did not exist.
 *
 * <p>The gate stays because the remaining 50 sites are still wrong, for a smaller reason. The
 * client floor is 300 seconds — right for ingest and retention, and far too long for anything with
 * a person waiting, who has given up after 15. So the ratchet no longer counts "queries that can
 * hang"; it counts "queries running on the batch floor", and the ones on a request path should not
 * be. Same ratchet mechanism as {@code UnboundedResponseGateTest} and {@code DirectFileIoGateTest},
 * with a weaker claim behind it — recorded here because a gate whose stated reason has silently
 * expired is how a list outlives the problem it was written for.
 *
 * <h2>Why {@code maxTime} is now banned outright</h2>
 *
 * <p>Under CSOT the server <b>ignores</b> {@code maxTimeMS}. Measured on 5.2.1 against a
 * 200,000-document unindexed scan: {@code maxTime=1ms} alone is rejected, and {@code maxTime=1ms}
 * with a client timeout set completes in 59 ms. So the old mechanism now reads as a budget and
 * enforces nothing — strictly worse than no budget at all, which at least looks absent. It is
 * asserted at zero rather than ratcheted down.
 */
class QueryTimeoutGateTest {

    /** Modules scanned, relative to this one. */
    private static final List<Path> ROOTS = List.of(
            Path.of("src/main/java"),
            Path.of("../analyzer-storage/src/main/java"),
            Path.of("../analysis-mongo-ingest/src/main/java"));

    /**
     * Query sites left on the batch floor, per file, measured 2026-09-07. These may only decrease.
     *
     * <p>Absent files must have none. The request-serving read path — `MongoAnalysisRepository`,
     * `MongoPlsqlEntityIndex` — is already at zero and is deliberately not listed, so a regression
     * there fails rather than being tolerated by a leftover allowance.
     */
    private static final Map<String, Integer> BUDGET = new TreeMap<>(Map.ofEntries(
            // ChatController.java, ClaudePurposeSummaryService.java, FacetPageResponder.java,
            // ConfigController.java: not MongoDB queries at all — java.util.regex.Matcher.find() and
            // Stream.distinct(), which the QUERY regex cannot distinguish from a Mongo collection's
            // .find()/.distinct() by method name alone (see the regex's own doc comment on this
            // exact class of false positive). ConfigController's is the Matcher over PEM_CERT_BLOCK
            // in readPemCertificates — the per-user connection cert parsing, added 2026-09-24.
            // Confirmed by reading each call site directly.
            Map.entry("ChatController.java", 1),
            Map.entry("ClaudeChunkReader.java", 1),
            Map.entry("ClaudePurposeSummaryService.java", 1),
            Map.entry("ClaudeVerificationService.java", 1),
            Map.entry("ConfigController.java", 1),
            Map.entry("ConversionController.java", 4),
            Map.entry("ConversionService.java", 1),
            Map.entry("EndpointReachability.java", 2),
            Map.entry("FacetCounter.java", 2),
            Map.entry("FacetPageResponder.java", 1),
            Map.entry("FlowAnalysisController.java", 4),
            Map.entry("IntegrityChecker.java", 4),
            Map.entry("LogTailReader.java", 1),
            Map.entry("MongoArtifactStore.java", 14),
            Map.entry("MongoIngestService.java", 1),
            Map.entry("OrphanRefChecker.java", 5),
            Map.entry("Reassembler.java", 1),
            Map.entry("RefAssertion.java", 2),
            Map.entry("Retention.java", 5),
            Map.entry("SchemaMeta.java", 1),
            Map.entry("TableDetailFacetReader.java", 1)));

    /**
     * A query on a collection instance, not any method that happens to share the name.
     *
     * <p>The first version matched {@code \.(find|aggregate|distinct)\(} outright and flagged
     * {@code CollectionReportAggregator.aggregate(...)} — a static call on an ordinary class, with
     * no database anywhere near it. A gate that reports work as outstanding when it is not gets
     * silenced, so the receiver is checked.
     *
     * <p>The rule: the receiver must not start with an upper-case letter. A MongoDB query is always
     * made on an instance — a lower-case variable, or the result of a call like
     * {@code db.getCollection(...)} — while a capitalised receiver is a class name and therefore a
     * static call on something else. Imperfect, and stated as a rule so the next false positive is
     * a change to this comment rather than an entry on a list.
     */
    private static final Pattern QUERY = Pattern.compile(
            "([A-Za-z0-9_]*)\\.(find|aggregate|distinct)\\(");

    /** True when the call is a static one on a class rather than a query on a collection. */
    private static boolean staticCallOnAClass(String receiver) {
        return !receiver.isEmpty() && Character.isUpperCase(receiver.charAt(0));
    }

    /** Source with block and line comments removed, so prose cannot trip a source scan. */
    private static String withoutComments(String src) {
        return src.replaceAll("(?s)/\\*.*?\\*/", "").replaceAll("(?m)^\\s*//.*$", "");
    }

    private static Map<String, Integer> untimedByFile() throws IOException {
        Map<String, Integer> out = new TreeMap<>();
        for (Path root : ROOTS) {
            if (!Files.isDirectory(root)) continue;
            try (Stream<Path> files = Files.walk(root)) {
                for (Path p : files.filter(f -> f.toString().endsWith(".java")).toList()) {
                    String src = Files.readString(p);
                    Matcher m = QUERY.matcher(src);
                    int queries = 0;
                    while (m.find()) {
                        if (staticCallOnAClass(m.group(1))) continue;
                        queries++;
                    }
                    if (queries == 0) continue;
                    // Counted rather than matched per-site: the budget is applied by wrapping the
                    // collection, which a per-site regex cannot reliably associate back to the
                    // query it bounds across a chained call.
                    //
                    // Counting QueryBudget.request( rather than maxTime( changed no number here:
                    // 50 before and 50 after, same 17 files, with the two request-path files at
                    // zero under both. The mechanism moved; the outstanding work did not.
                    int timed = src.split("QueryBudget\\.request\\(", -1).length - 1;
                    int untimed = queries - timed;
                    if (untimed > 0) out.put(p.getFileName().toString(), untimed);
                }
            }
        }
        return out;
    }

    @Test
    void noFileExceedsItsUntimedQueryBudget() throws IOException {
        List<String> regressions = new ArrayList<>();
        for (var e : untimedByFile().entrySet()) {
            int allowed = BUDGET.getOrDefault(e.getKey(), 0);
            if (e.getValue() > allowed) {
                regressions.add(String.format(
                        "%s: %d queries on the batch floor, budget %d — wrap the collection with "
                                + "QueryBudget.request(...), or raise the budget and say why",
                        e.getKey(), e.getValue(), allowed));
            }
        }
        if (!regressions.isEmpty()) fail(String.join("\n  ", regressions));
    }

    @Test
    void theBudgetIsLoweredAsQueriesAreConverted() throws IOException {
        Map<String, Integer> actual = untimedByFile();
        List<String> stale = new ArrayList<>();
        for (var e : BUDGET.entrySet()) {
            int now = actual.getOrDefault(e.getKey(), 0);
            if (now < e.getValue()) {
                stale.add(String.format("%s: budget %d but only %d remain — lower it to %d",
                        e.getKey(), e.getValue(), now, now));
            }
        }
        if (!stale.isEmpty()) fail(String.join("\n  ", stale));
    }

    @Test
    void theRequestServingReadPathIsFullyTimed() throws IOException {
        // The part that matters most, asserted absolutely rather than by budget: these are the
        // queries with a person waiting on them.
        Map<String, Integer> actual = untimedByFile();
        for (String f : List.of("MongoAnalysisRepository.java", "MongoPlsqlEntityIndex.java")) {
            assertEquals(0, actual.getOrDefault(f, 0),
                    f + " has queries on the 300s batch floor on the request-serving read path");
        }
    }

    @Test
    void theScanStillFindsQueries() throws IOException {
        // Every regex gate's failure mode is matching nothing and passing forever.
        int total = untimedByFile().values().stream().mapToInt(Integer::intValue).sum();
        assertEquals(54, total,
                "the scan found " + total + " unbudgeted queries; it measured 54 (2026-09-24, after "
                        + "budgeting a 4th java.util.regex.Matcher false positive it had never "
                        + "accounted for — ConfigController's PEM_CERT_BLOCK scan in "
                        + "readPemCertificates, added with the per-user connection cert work — on top "
                        + "of the 3 pre-existing ones (ChatController, ClaudePurposeSummaryService, "
                        + "FacetPageResponder; was 53). If the number fell, lower the budgets; if the "
                        + "pattern broke, fix the pattern.");
    }

    @Test
    void noMainCodeUsesMaxTimeAnyMore() throws IOException {
        // Absolute, not ratcheted: under CSOT this call is ignored by the server, so a site using
        // it looks budgeted in review and is not. Zero is the only defensible number.
        List<String> offenders = new ArrayList<>();
        for (Path root : ROOTS) {
            if (!Files.isDirectory(root)) continue;
            try (Stream<Path> files = Files.walk(root)) {
                for (Path p : files.filter(f -> f.toString().endsWith(".java")).toList()) {
                    // Comments stripped first. Without that this flagged QueryBudget.java, whose
                    // javadoc quotes the banned call to explain why it is banned — the same way an
                    // earlier version of graph-ceiling.test.mjs failed on the comment describing
                    // its own fix. A gate that fires on its own documentation gets deleted.
                    if (withoutComments(Files.readString(p)).contains(".maxTime(")) {
                        offenders.add(p.getFileName().toString());
                    }
                }
            }
        }
        assertEquals(List.of(), offenders,
                "these files call .maxTime(), which driver 5.2 ignores when a client timeout is "
                        + "set — use QueryBudget.request(collection) instead");
    }
}
