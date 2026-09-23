package com.plsqlanalyzer.web.parser.search;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Matches parsed queries against the flattened statement rows.
 *
 * <h2>A linear scan, on purpose</h2>
 *
 * The largest analysis on this machine holds 5,015 statement rows. A scan over that is well under a
 * millisecond, so a persisted inverted index would add an artifact to keep in step with the data
 * for no measurable gain — and an index that can fall out of step is a source of wrong answers,
 * which is worse than a slow one. If an analysis ever appears where this is too slow, the fix is a
 * memoised in-memory token map, not a stored index.
 *
 * <h2>How terms combine</h2>
 *
 * Different qualifiers AND; the same qualifier ORs. So
 * {@code op:UPDATE op:DELETE table:ORDERS} reads as "(UPDATE or DELETE) on ORDERS", which is what
 * someone naming two operations means. AND-ing repeats would always return nothing.
 *
 * <p>Negated terms are excluded outright, whichever field they name.
 */
public final class StatementSearcher {

    /**
     * MID-008 (redefined DoD, §114/§117) — the facet/total walk below scans every row in the
     * statement index unconditionally, regardless of {@code limit} (this class's own header already
     * says why: facets and the true total must cover every match, not the returned page). That is
     * fine at the one real corpus measured here (5,015 statements, well under a millisecond) but is
     * genuinely unbounded per-request work as the index grows — at the project's stated 8,000-
     * procedure target the same document is ~1.1 GB parsed (this class's javadoc), and a scan over
     * its statement count is proportionally larger, unconditionally, on every search request.
     *
     * <p>This is not the "$group" MID-008 was originally written against (there never was a MongoDB
     * aggregation here — the row's own wording predates this streaming, file-based implementation),
     * but it is the same shape of defect the redefined DoD asks to catch: unbounded work performed
     * once per request, sized by the data rather than by anything the caller controls. Set well
     * above the one measured real corpus (5,015) so no existing analysis is affected, and far below
     * the point where a scan would meaningfully delay a request even at the stated target — not a
     * precise capacity number, a safety rail matching {@code ParserAnalysisReader
     * .MAX_CALL_TREE_NODES}'s own reasoning for the same kind of per-request scan bound elsewhere in
     * this codebase.
     */
    public static final int DEFAULT_MAX_SCANNED = 200_000;

    /** A row plus why it matched, so the UI can show the operation and column that answered. */
    public record Hit(JsonNode row, List<String> matchedColumns) {}

    /**
     * Result of a search: the hits, the true total before any limit, and whether the scan itself
     * was cut short by {@link #DEFAULT_MAX_SCANNED} before reaching the end of the index — in which
     * case {@code total} and {@code facets} are honest counts of what was scanned, not of the whole
     * analysis, and the caller must say so rather than presenting a partial count as complete.
     */
    public record Result(List<Hit> hits, int total, Map<String, Map<String, Integer>> facets,
                          boolean scanLimitReached) {}

    private StatementSearcher() {}

    /**
     * Runs a query over the statement rows.
     *
     * @param statements the {@code statements} array from {@code api/statements/index.json}
     * @param q          the parsed query
     * @param limit      how many hits to return; the reported total is always the full count
     */
    public static Result search(JsonNode statements, SearchQuery q, int limit) {
        // The materialised form, kept so existing callers and their tests are unaffected. An
        // absent or non-array node is "nothing to search", not an error — a JsonNode is Iterable,
        // so an array delegates directly and anything else becomes an empty source.
        return search(statements != null && statements.isArray()
                ? statements : List.<JsonNode>of(), q, limit);
    }

    /**
     * Runs a query over a <b>stream</b> of statement rows.
     *
     * <p>Identical logic to the array form — this is the same loop, given its rows one at a time —
     * so a streaming caller cannot drift from a materialised one. That matters more than it
     * sounds: the two would be a pair of search implementations whose disagreements only show up
     * as a user finding different results on different storage modes.
     *
     * <p>Nothing here retains a row beyond the hit list, which is capped at {@code limit}. Measured
     * on the largest analysis (7.0 MB, 5,015 statements): materialising the document retains
     * <b>24.3 MB</b>, streaming it retains ~0.
     */
    public static Result search(Iterable<JsonNode> rows, SearchQuery q, int limit) {
        return search(rows, q, limit, DEFAULT_MAX_SCANNED);
    }

    /**
     * @param maxScanned MID-008 (§117) — the per-request scan bound; see {@link
     *                   #DEFAULT_MAX_SCANNED}'s own javadoc. Exposed as a parameter (rather than
     *                   only the constant) so a test can plant a small bound and prove the cutoff
     *                   fires without needing to construct 200,000 rows.
     */
    public static Result search(Iterable<JsonNode> rows, SearchQuery q, int limit, int maxScanned) {
        List<Hit> hits = new ArrayList<>();
        int total = 0;

        // Facets are counted over EVERY match, not over the returned page. A modal that says
        // "42 UPDATEs across 9 procs" while showing 50 of 300 rows must be counting the 300.
        Map<String, Map<String, Integer>> facets = new LinkedHashMap<>();
        facets.put("operation", new TreeMap<>());
        facets.put("tableName", new TreeMap<>());
        facets.put("nodeName", new TreeMap<>());

        if (rows == null) return new Result(hits, 0, facets, false);

        // Group terms by field so same-field terms OR while different fields AND. A LinkedHashMap
        // keeps the user's order, which is the order the echo shows.
        //
        // Bare words each get their OWN group, so they AND with one another: `ERP_INVENTORY
        // USP_SHIP` means both, the way every search box behaves. Sharing one group would OR them,
        // and adding a second word would then *widen* the result instead of narrowing it — the
        // opposite of what typing more means.
        Map<String, List<SearchQuery.Term>> positive = new LinkedHashMap<>();
        List<SearchQuery.Term> negative = new ArrayList<>();
        int bareSeq = 0;
        for (SearchQuery.Term t : q.terms()) {
            if (t.negated()) { negative.add(t); continue; }
            String key = t.isBare() ? "bare#" + (bareSeq++) : t.field();
            positive.computeIfAbsent(key, k -> new ArrayList<>()).add(t);
        }

        int scanned = 0;
        boolean scanLimitReached = false;
        for (JsonNode row : rows) {
            if (scanned++ >= maxScanned) { scanLimitReached = true; break; }
            List<String> matchedCols = new ArrayList<>();
            if (!matches(row, positive, negative, matchedCols)) continue;
            total++;
            if (hits.size() < limit) hits.add(new Hit(row, matchedCols));
            bump(facets.get("operation"), row.path("operation").asText(""));
            bump(facets.get("tableName"), qualified(row));
            bump(facets.get("nodeName"), row.path("nodeName").asText(""));
        }
        return new Result(hits, total, facets, scanLimitReached);
    }

    private static void bump(Map<String, Integer> m, String k) {
        if (k != null && !k.isEmpty()) m.merge(k, 1, Integer::sum);
    }

    private static String qualified(JsonNode row) {
        String s = row.path("schema").asText("");
        String t = row.path("tableName").asText("");
        return s.isEmpty() ? t : s + "." + t;
    }

    private static boolean matches(JsonNode row,
                                   Map<String, List<SearchQuery.Term>> positive,
                                   List<SearchQuery.Term> negative,
                                   List<String> matchedCols) {
        for (SearchQuery.Term n : negative) {
            if (termMatches(row, n, null)) return false;
        }
        for (var group : positive.entrySet()) {
            boolean any = false;
            for (SearchQuery.Term t : group.getValue()) {
                if (termMatches(row, t, matchedCols)) { any = true; }
            }
            if (!any) return false;      // groups AND with each other
        }
        return true;
    }

    private static boolean termMatches(JsonNode row, SearchQuery.Term t, List<String> matchedCols) {
        if (t.isBare()) {
            // A bare word searches everything a user can see on the result row. procedureId-style
            // opaque fields are deliberately excluded: matching one produces a hit with nothing on
            // screen to explain it.
            return field(row, "tableName", t) || field(row, "nodeName", t)
                || field(row, "operation", t) || field(row, "schema", t)
                || field(row, "sqlText", t)   || columns(row, t, matchedCols);
        }
        if ("columns".equals(t.field())) return columns(row, t, matchedCols);
        if ("line".equals(t.field())) return String.valueOf(row.path("line").asInt()).equals(t.value());
        return field(row, t.field(), t);
    }

    private static boolean field(JsonNode row, String name, SearchQuery.Term t) {
        JsonNode v = row.get(name);
        if (v == null || v.isNull()) return false;
        String s = v.asText("").toUpperCase(Locale.ROOT);
        return t.prefix() ? s.startsWith(t.value()) : s.contains(t.value());
    }

    private static boolean columns(JsonNode row, SearchQuery.Term t, List<String> matchedCols) {
        JsonNode cols = row.get("columns");
        if (cols == null || !cols.isArray()) return false;
        boolean hit = false;
        for (JsonNode c : cols) {
            String s = c.asText("").toUpperCase(Locale.ROOT);
            boolean m = t.prefix() ? s.startsWith(t.value()) : s.contains(t.value());
            if (m) {
                hit = true;
                // Recorded so the UI can highlight which column answered — with a hundred columns
                // on a statement, "this row matched" is not by itself an answer.
                if (matchedCols != null && !matchedCols.contains(s)) matchedCols.add(s);
            }
        }
        return hit;
    }
}
