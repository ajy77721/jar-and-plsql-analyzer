package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Read/write store for an analysis's precomputed <b>view summaries</b> — small sidecar documents
 * written into {@code <analysis>/views/} so a read endpoint can serve a few hundred bytes (or a few
 * KB) instead of re-scanning every {@code api/nodes/*.json} (or re-parsing the whole index) on
 * every request.
 *
 * <p><b>Never a breaking change.</b> Every caller reads the sidecar first and falls back to the
 * exact computation it used before when the file is absent. An old analysis that predates this
 * feature — or one whose sidecar was deleted — behaves byte-for-byte as it did. The fallback is the
 * pre-change code, unchanged; the sidecar is only ever an optimisation.
 *
 * <p><b>Lazy backfill.</b> {@link #readOrCompute} computes the legacy result on a miss and writes
 * it as the sidecar, so an analysis analysed before this feature existed upgrades itself on first
 * access — the same "seed on first use" shape {@code PromptConfigService.ensureSeeded} uses. The
 * write is best-effort: an unreachable store must never turn a read into a 500, and a failed write
 * simply means the next request recomputes.
 *
 * <p><b>Staleness.</b> A sidecar is only trusted while it is at least as new as the analysis's
 * {@code api/index.json} (the last artifact written in every run). A re-analysis rewrites the index
 * after the sidecars, so the mtime comparison invalidates them with one cheap stat and no document
 * read. A correction that does not touch the index is handled by an explicit
 * {@link #invalidate(String)} from the correction path.
 *
 * <p><b>Format.</b> Each sidecar is the endpoint's own document — a bare array for the node-scan
 * facets — so serving it is byte-identical to serving the legacy computation. {@code header.json}
 * additionally carries {@code formatVersion} (it is a flat object of scalars, so the marker can
 * live beside them); a later format change bumps the constant and older sidecars are ignored rather
 * than misread.
 */
@Component
public class AnalysisViewStore {

    private static final Logger log = LoggerFactory.getLogger(AnalysisViewStore.class);

    /**
     * Bump to invalidate every existing sidecar whose shape this code no longer understands.
     *
     * <p>Version 2: the cursor facet is now one row per cursor INSTANCE
     * ({@code name::node#line}, not {@code name::node}), {@code /summary/stats} distinguishes
     * {@code totalTables} (distinct) from {@code totalTableReferences}, and
     * {@code counts.callsOut} splits overloads. Sidecars written at version 1 carry the old
     * shapes, so {@link #read} refuses them and the warm-up sweep rebuilds the set — an analysis
     * analysed before this change upgrades itself on next access rather than serving stale rows.
     */
    public static final int FORMAT_VERSION = 2;

    /** The sidecar folder inside an analysis directory. */
    public static final String VIEWS_DIR = "views";

    /**
     * The subfolder of {@link #VIEWS_DIR} holding one {@code <nodeId>.json} call-tree sidecar per
     * precomputed entry point. It lives <em>under</em> {@code views/} deliberately: the correction
     * paths already call {@link #invalidate(String)}, which deletes the whole prefix, so the trees
     * are invalidated by the same hook that drops the facet summaries — no second staleness rule to
     * keep in sync.
     */
    public static final String CALL_TREE_DIR = "call-tree";

    /** Listing scalars + {@code formatVersion} — see {@link #writeHeader}. */
    public static final String HEADER = "header.json";
    /** The {@code /sequences} document. */
    public static final String SEQUENCES = "sequences.json";
    /** The {@code /cursors} document. */
    public static final String CURSORS = "cursors.json";
    /** The {@code /dynamic-sql} document. */
    public static final String DYNAMIC_SQL = "dynamic-sql.json";
    /** The {@code /joins} document (serves {@code /joins/summary} and its queries view). */
    public static final String JOINS_SUMMARY = "joins-summary.json";
    /** {@code {table -> rows}} for the table-scoped joins facet. */
    public static final String TABLE_JOINS = "table-joins.json";
    /** {@code {table -> rows}} for the table-scoped column-refs facet. */
    public static final String TABLE_COLUMNS = "table-columns.json";
    /**
     * Per-table split of {@link #TABLE_JOINS}: one small {@code <TABLE>.json} per table, written
     * alongside the whole-map sidecar. A per-table request reads one small file; a lazy backfill
     * writes one small file instead of rewriting the whole map. The map is kept for backward-compat
     * reads — see {@link AnalysisViewWriter} and {@code ArtifactScanRepository#tableView}.
     */
    public static final String TABLE_JOINS_DIR = "table-joins";
    /** Per-table split of {@link #TABLE_COLUMNS}; see {@link #TABLE_JOINS_DIR}. */
    public static final String TABLE_COLUMNS_DIR = "table-columns";
    /** The default-weights {@code /complexity/scores} body. */
    public static final String COMPLEXITY = "complexity.json";
    /** The default-scope {@code /summary/stats} body. */
    public static final String SUMMARY_STATS = "summary-stats.json";
    /** {@code {nodeId -> callers[]}} for the precomputed call-tree entry points. */
    public static final String CALLERS = "callers.json";
    /** The raw facet arrays {@code /global-search} normalizes and filters; see the controller. */
    public static final String SEARCH = "search.json";

    /**
     * The {@code linear} grid neighbours precomputed at save time beside the default: the only two
     * other algorithms {@code complexity.js} offers, both at the endpoint's default weights,
     * thresholds and exponent. A custom request that matches neither computes once and is cached as
     * a hashed variant, capped at {@link #COMPLEXITY_VARIANT_CAP}.
     */
    public static final String COMPLEXITY_BALANCED = "complexity-balanced.json";
    /** See {@link #COMPLEXITY_BALANCED}. */
    public static final String COMPLEXITY_LOGARITHMIC = "complexity-logarithmic.json";
    /** How many hashed custom-weight variants to keep before the oldest (by mtime) is dropped. */
    public static final int COMPLEXITY_VARIANT_CAP = 8;
    /** File-name prefix every non-default complexity variant shares. */
    public static final String COMPLEXITY_VARIANT_PREFIX = "complexity-";

    /**
     * The ten facets {@link AnalysisViewWriter#writeAll} produces, in the order it produces them.
     * {@link #HEADER} is deliberately not here: it is written at save time from the index root the
     * analysis already holds, and it is the one sidecar carrying {@code formatVersion}, so it is the
     * completeness <em>marker</em> rather than just another member of the set.
     *
     * <p>The per-table split files and the non-default complexity variants are not listed: they are
     * members of the {@link #TABLE_JOINS}/{@link #TABLE_COLUMNS}/{@link #COMPLEXITY} facets rather
     * than separate completeness markers, and the table set is unbounded.
     */
    private static final String[] FACET_VIEWS = {
            SEQUENCES, CURSORS, DYNAMIC_SQL, JOINS_SUMMARY,
            TABLE_JOINS, TABLE_COLUMNS, COMPLEXITY, SUMMARY_STATS, CALLERS, SEARCH
    };

    private final ArtifactStore store;
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * One lock per analysis, so a burst of first requests for an un-seeded analysis computes and
     * writes its sidecar once rather than racing to overwrite it.
     */
    private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();

    /**
     * {@code name -> {headerMtime, formatVersionSeen}} — memoises the one header probe a facet read
     * needs to decide format staleness, so the hot path pays a stat rather than a header GET. The
     * mtime is the cache key: a rewritten header invalidates the memo with it.
     */
    private final ConcurrentHashMap<String, long[]> headerFormatMemo = new ConcurrentHashMap<>();

    /**
     * @param store required — the same per-user routed {@code parserArtifactStore} every other
     *              parser artifact goes through, so the sidecars inherit credentials and backend
     *              automatically
     */
    public AnalysisViewStore(@Qualifier("parserArtifactStore") ArtifactStore store) {
        this.store = store;
    }

    /** A legacy computation that may fail; lets {@link #readOrCompute} stay exception-clean. */
    @FunctionalInterface
    public interface IoSupplier<T> {
        T get() throws IOException;
    }

    private ArtifactKey key(String name, String view) {
        return AnalysisPaths.resolveKey(name, VIEWS_DIR + "/" + view);
    }

    private ArtifactKey indexKey(String name) {
        return AnalysisPaths.resolveKey(name, "api/index.json");
    }

    private Object lockFor(String name) {
        return locks.computeIfAbsent(name, k -> new Object());
    }

    /**
     * The stored document, or {@code null} when it is absent, unreadable or stale.
     *
     * <p>Stale means older than {@code api/index.json}: a re-analysis writes the index last, so a
     * sidecar that predates it describes a superseded run.
     */
    public JsonNode read(String name, String view) {
        try {
            ArtifactKey k = key(name, view);
            // A header from a different format means every facet sidecar beside it is from that
            // older format too: refuse it so readOrCompute recomputes and rewrites current-shape
            // output (this is what makes a format bump upgrade an analysis on access). The header
            // itself is exempt — readHeader() applies its own version check.
            if (!HEADER.equals(view) && isHeaderFormatStale(name)) return null;
            // No exists() first: lastModified already returns -1 for an absent key, so an absent
            // (or masked) sidecar fails the freshness check below without a separate HEAD.
            if (store.lastModified(k) < store.lastModified(indexKey(name))) return null;
            try (InputStream in = store.read(k)) {
                JsonNode doc = mapper.readTree(in);
                return (doc == null || doc.isMissingNode()) ? null : doc;
            }
        } catch (Exception e) {
            log.debug("view read {} / {} failed: {}", name, view, e.getMessage());
            return null;
        }
    }

    /**
     * True when an analysis already has a complete, fresh sidecar set — a current-format
     * {@link #HEADER} plus every {@link #FACET_VIEWS facet}. This is the warm-up sweep's skip flag:
     * it asks the same question every read asks (present <em>and</em> at least as new as
     * {@code api/index.json}), so a re-analysis that rewrote the index invalidates the set and the
     * sweep rebuilds it exactly as a first read would.
     *
     * <p>Checks the header first and returns on the first miss, so an analysis with nothing
     * precomputed costs one store probe rather than nine.
     */
    public boolean hasCompleteViewSet(String name) {
        if (name == null || name.isBlank()) return false;
        if (readHeader(name) == null) return false;
        for (String view : FACET_VIEWS) {
            if (read(name, view) == null) return false;
        }
        return true;
    }

    /**
     * The sidecar if present and fresh, otherwise {@code legacy}'s result — which is then written
     * as the sidecar so the next request is served from it (lazy backfill).
     *
     * <p>The legacy result is returned untouched, so a miss is the pre-change response exactly. A
     * write failure is logged and swallowed: the answer is already correct.
     */
    public JsonNode readOrCompute(String name, String view, IoSupplier<JsonNode> legacy)
            throws IOException {
        JsonNode cached = read(name, view);
        if (cached != null) return cached;
        JsonNode computed = legacy.get();
        if (computed != null) {
            write(name, view, computed);
        }
        return computed;
    }

    /** Writes {@code doc} as the sidecar for {@code view}. Best-effort — never throws. */
    public boolean write(String name, String view, JsonNode doc) {
        return write(name, view, doc, true);
    }

    /**
     * As {@link #write(String, String, JsonNode)}, with control over the failure log.
     *
     * <p>The background warm-up passes {@code logFailure=false}: it tallies failures itself and
     * emits one line per sweep, so a deployment whose store is unreachable is not warned once per
     * facet per analysis. The failure is still recorded at DEBUG, and the return value still says
     * whether the write landed, so the quieter caller loses no information.
     *
     * @return true when the sidecar was written, false when it was not
     */
    public boolean write(String name, String view, JsonNode doc, boolean logFailure) {
        if (doc == null) return false;
        // A rewritten header changes what the format probe should answer; drop the memo rather than
        // trust mtime granularity to notice (two writes can share a millisecond).
        if (HEADER.equals(view)) headerFormatMemo.remove(name);
        synchronized (lockFor(name)) {
            try (OutputStream out = store.write(key(name, view))) {
                mapper.writeValue(out, doc);
                return true;
            } catch (Exception e) {
                if (logFailure) {
                    log.warn("view write {} / {} failed: {}", name, view, e.getMessage());
                } else {
                    log.debug("view write {} / {} failed: {}", name, view, e.getMessage());
                }
                return false;
            }
        }
    }

    /**
     * Deletes every sidecar for an analysis. Called by the correction paths, whose change need not
     * move {@code api/index.json}'s mtime (a Claude apply writes {@code claude/tables_merged.json}),
     * so the mtime freshness check alone would not catch it.
     */
    public void invalidate(String name) {
        headerFormatMemo.remove(name);
        synchronized (lockFor(name)) {
            try {
                store.deletePrefix(AnalysisPaths.resolveKey(name, VIEWS_DIR));
            } catch (Exception e) {
                log.debug("view invalidate {} failed: {}", name, e.getMessage());
            }
        }
    }

    /* ─────────────────────────────── header ─────────────────────────────── */

    /**
     * The listing scalars — {@code readIndexHeader}'s own output plus {@code formatVersion} — or
     * {@code null} when absent/stale/from an older format. The caller interprets the fields.
     */
    public JsonNode readHeader(String name) {
        JsonNode doc = read(name, HEADER);
        if (doc == null || !doc.isObject()) return null;
        if (doc.path("formatVersion").asInt(-1) != FORMAT_VERSION) return null;
        return doc;
    }

    /**
     * True when {@code views/header.json} exists but was written by a different format. A format
     * bump therefore invalidates every facet sidecar at once: {@link #read} refuses to serve them,
     * so a read recomputes and rewrites the current shape, and {@code SidecarWarmUpService} drops
     * the stale set before rebuilding it. An <em>absent</em> header is not stale — that is a test
     * fixture or an analysis that never warmed, and the pre-existing mtime rule alone applies.
     */
    public boolean isHeaderFormatStale(String name) {
        if (name == null || name.isBlank()) return false;
        ArtifactKey hk = key(name, HEADER);
        long hm = store.lastModified(hk);
        if (hm < 0) return false;
        long[] memo = headerFormatMemo.get(name);
        if (memo == null || memo[0] != hm) {
            int version = -1;
            try (InputStream in = store.read(hk)) {
                JsonNode doc = mapper.readTree(in);
                version = (doc != null && doc.isObject()) ? doc.path("formatVersion").asInt(-1) : -1;
            } catch (Exception e) {
                log.debug("header format probe {} failed: {}", name, e.getMessage());
            }
            memo = new long[]{hm, version};
            headerFormatMemo.put(name, memo);
        }
        return memo[1] != FORMAT_VERSION;
    }

    /**
     * Stores the listing scalars. The caller passes exactly what {@code readIndexHeader} would have
     * returned, so a sidecar read yields the same {@code AnalysisInfo} the legacy read would.
     * {@code formatVersion} is added here — the one field the legacy reader never produced.
     */
    public boolean writeHeader(String name, ObjectNode scalars) {
        return writeHeader(name, scalars, true);
    }

    /** As {@link #writeHeader(String, ObjectNode)}; see {@link #write(String, String, JsonNode, boolean)}. */
    public boolean writeHeader(String name, ObjectNode scalars, boolean logFailure) {
        if (scalars == null) return false;
        ObjectNode copy = scalars.deepCopy();
        copy.put("formatVersion", FORMAT_VERSION);
        return write(name, HEADER, copy, logFailure);
    }

    /* ──────────────────────────── call trees ──────────────────────────── */

    /**
     * The sidecar file name for one node's call tree, e.g. {@code ERP$P1.json}.
     *
     * <p>Node ids are {@code SCHEMA$NAME} (or {@code SCHEMA$PKG.MEMBER}); a key segment may not carry
     * a path separator ({@link ArtifactKey} rejects it), and a caller-supplied id is untrusted REST
     * input, so anything outside the id alphabet is folded to {@code _}. Real ids never contain such
     * a character, so the fold is a guard, not a transform.
     */
    public static String callTreeFileName(String nodeId) {
        return nodeId.replaceAll("[^A-Za-z0-9._$-]", "_") + ".json";
    }

    /**
     * The precomputed call tree for {@code nodeId} — the exact document
     * {@code /analyses/{name}/call-tree/{nodeId}} serves — or {@code null} when there is none for
     * this node (it was not an entry point, or the sidecar is absent/stale). A miss means "synthesise
     * the legacy way", never "this tree is empty".
     */
    public JsonNode readCallTree(String name, String nodeId) {
        if (name == null || name.isBlank() || nodeId == null || nodeId.isBlank()) return null;
        return read(name, CALL_TREE_DIR + "/" + callTreeFileName(nodeId));
    }

    /** Writes one node's call tree as a sidecar. Best-effort — never throws. */
    public void writeCallTree(String name, String nodeId, JsonNode tree) {
        if (name == null || name.isBlank() || nodeId == null || nodeId.isBlank() || tree == null) return;
        write(name, CALL_TREE_DIR + "/" + callTreeFileName(nodeId), tree);
    }

    /* ──────────────────────────── callers ──────────────────────────── */

    /**
     * The {@code /call-tree/{nodeId}/callers} array for one node out of {@link #CALLERS}, or
     * {@code null} when the map is absent/stale or has no entry for {@code nodeId} — both of which
     * mean "synthesise the legacy way", never "this node has no callers".
     */
    public JsonNode readCallers(String name, String nodeId) {
        if (name == null || name.isBlank() || nodeId == null || nodeId.isBlank()) return null;
        JsonNode doc = read(name, CALLERS);
        if (doc == null || !doc.isObject()) return null;
        JsonNode entry = doc.get(nodeId);
        return (entry == null || entry.isNull()) ? null : entry;
    }

    /**
     * Merges one node's callers array into {@link #CALLERS} (lazy backfill for a node the save-time
     * entry-point selection did not cover). Only ever additive, so a concurrent reader sees a
     * superset, never a false empty.
     */
    public void putCallersEntry(String name, String nodeId, JsonNode callers) {
        if (name == null || name.isBlank() || nodeId == null || nodeId.isBlank() || callers == null) return;
        synchronized (lockFor(name)) {
            JsonNode existing = read(name, CALLERS);
            ObjectNode map = (existing != null && existing.isObject())
                    ? (ObjectNode) existing.deepCopy()
                    : mapper.createObjectNode();
            map.set(nodeId, callers);
            write(name, CALLERS, map);
        }
    }

    /** Replaces the whole {@code {nodeId -> callers[]}} sidecar, used by the writer at save time. */
    public boolean writeCallers(String name, Map<String, ? extends JsonNode> byNode, boolean logFailure) {
        ObjectNode map = mapper.createObjectNode();
        byNode.forEach((nodeId, callers) -> {
            if (nodeId != null && !nodeId.isBlank() && callers != null) map.set(nodeId, callers);
        });
        return write(name, CALLERS, map, logFailure);
    }

    /* ───────────────────────────── search ───────────────────────────── */

    /**
     * The raw per-facet arrays {@code /global-search} filters, or {@code null} when the sidecar is
     * absent/stale. The document is <em>not</em> the endpoint response: it is the unfiltered source
     * rows the controller's {@code buildSearchRows} normalizes and filters, so serving from it is
     * byte-identical to reading the 7 artifacts it stands in for.
     */
    public JsonNode readSearch(String name) {
        JsonNode doc = read(name, SEARCH);
        return (doc != null && doc.isObject()) ? doc : null;
    }

    /** Stores the raw search-facet arrays. Best-effort — never throws. */
    public boolean writeSearch(String name, JsonNode facets, boolean logFailure) {
        return write(name, SEARCH, facets, logFailure);
    }

    /* ──────────────────────── table-scoped facets ──────────────────────── */

    /**
     * One table's rows out of a {@code {table -> rows}} sidecar, or {@code null} when the sidecar
     * is absent/stale or has no entry for {@code table} — both of which mean "compute the legacy way
     * and merge the result in", never "this table is empty".
     */
    public JsonNode readTableEntry(String name, String view, String table) {
        if (table == null || table.isBlank()) return null;
        JsonNode doc = read(name, view);
        if (doc == null || !doc.isObject()) return null;
        return doc.get(normalizeTable(table));
    }

    /**
     * Lazy backfill of one computed table's rows. Writes <b>only</b> that table's split file
     * ({@code views/<dir>/<TABLE>.json}) — never the whole-map sidecar — so the cost of a first
     * access to an uncovered table is one small write, not a rewrite of every table's rows.
     */
    public void putTableEntry(String name, String view, String table, JsonNode rows) {
        writeTableFile(name, tableDirForView(view), table, rows, true);
    }

    /** The per-table split directory backing a whole-map view name, or the view name itself. */
    private static String tableDirForView(String view) {
        if (TABLE_JOINS.equals(view)) return TABLE_JOINS_DIR;
        if (TABLE_COLUMNS.equals(view)) return TABLE_COLUMNS_DIR;
        return view;
    }

    /** Replaces a whole {@code {table -> rows}} sidecar, used by the writer at save time. */
    public boolean writeTableMap(String name, String view, Map<String, ? extends JsonNode> byTable) {
        return writeTableMap(name, view, byTable, true);
    }

    /** As {@link #writeTableMap(String, String, Map)}; see {@link #write(String, String, JsonNode, boolean)}. */
    public boolean writeTableMap(String name, String view, Map<String, ? extends JsonNode> byTable,
                                 boolean logFailure) {
        ObjectNode map = mapper.createObjectNode();
        byTable.forEach((table, rows) -> {
            if (table != null && !table.isBlank() && rows != null) {
                map.set(normalizeTable(table), rows);
            }
        });
        return write(name, view, map, logFailure);
    }

    /** Table keys are case-insensitive, matching the reader's own {@code upper(table)} matching. */
    private static String normalizeTable(String table) {
        return table.trim().toUpperCase(Locale.ROOT);
    }

    /* ────────────────────── per-table split files ────────────────────── */

    /**
     * The file name for one table's split sidecar, e.g. {@code ORDERS.json}. Upper-cased to match
     * the map's own {@link #normalizeTable} key, and folded to the id alphabet because the table
     * name is untrusted REST input that {@link ArtifactKey} would otherwise reject.
     */
    public static String tableFileName(String table) {
        return normalizeTable(table).replaceAll("[^A-Za-z0-9._$-]", "_") + ".json";
    }

    /**
     * One table's rows out of its own {@code views/<dir>/<TABLE>.json} split, or {@code null} when
     * it is absent/stale. {@code dir} is {@link #TABLE_JOINS_DIR} or {@link #TABLE_COLUMNS_DIR}.
     */
    public JsonNode readTableFile(String name, String dir, String table) {
        if (table == null || table.isBlank()) return null;
        return read(name, dir + "/" + tableFileName(table));
    }

    /**
     * Writes one table's split sidecar. The lazy backfill uses this — and only this — so a first
     * access to an uncovered table writes one small file instead of rewriting the whole map.
     */
    public boolean writeTableFile(String name, String dir, String table, JsonNode rows, boolean logFailure) {
        if (table == null || table.isBlank() || rows == null) return false;
        return write(name, dir + "/" + tableFileName(table), rows, logFailure);
    }

    /* ────────────────────── complexity variants ────────────────────── */

    /**
     * The name of the precomputed sidecar for one {@code /complexity/scores} parameter set.
     *
     * <p>The default parameters keep {@link #COMPLEXITY} (the shape every existing golden test and
     * reader already knows). A request matching one of the two precomputed grid neighbours maps to
     * its named file. Anything else maps to a {@code complexity-<hash>.json} variant keyed by a hash
     * of the canonical parameters — computed once on demand and cached, capped at
     * {@link #COMPLEXITY_VARIANT_CAP} (LRU by mtime, pruned by {@link #writeComplexityVariant}).
     */
    public static String complexityViewName(String algorithm, double exponent,
                                            double wLoc, double wTables, double wCallsOut,
                                            double wCursors, double wDynamicSql, double wDepth,
                                            double thMedium, double thHigh) {
        String algo = algorithm == null ? "linear" : algorithm;
        boolean defaultParams =
                Double.compare(wLoc, 0.3) == 0 && Double.compare(wTables, 15) == 0
                        && Double.compare(wCallsOut, 10) == 0 && Double.compare(wCursors, 8) == 0
                        && Double.compare(wDynamicSql, 20) == 0 && Double.compare(wDepth, 5) == 0
                        && Double.compare(thMedium, 50) == 0 && Double.compare(thHigh, 150) == 0
                        && Double.compare(exponent, 1.3) == 0;
        if (defaultParams && "linear".equalsIgnoreCase(algo)) return COMPLEXITY;
        if (defaultParams && "balanced".equalsIgnoreCase(algo)) return COMPLEXITY_BALANCED;
        if (defaultParams && "logarithmic".equalsIgnoreCase(algo)) return COMPLEXITY_LOGARITHMIC;
        String canonical = algo.toLowerCase(Locale.ROOT) + "|" + exponent + "|" + wLoc + "|" + wTables
                + "|" + wCallsOut + "|" + wCursors + "|" + wDynamicSql + "|" + wDepth + "|"
                + thMedium + "|" + thHigh;
        return COMPLEXITY_VARIANT_PREFIX + hash16(canonical) + ".json";
    }

    /**
     * Writes a complexity variant sidecar and prunes the hashed variants to
     * {@link #COMPLEXITY_VARIANT_CAP} by mtime. The named grid files and {@link #COMPLEXITY} are
     * never pruned — the cap is on the unbounded custom-parameter space only.
     */
    public boolean writeComplexityVariant(String name, String view, JsonNode doc) {
        boolean ok = write(name, view, doc);
        if (ok) pruneComplexityVariants(name);
        return ok;
    }

    /** Deletes the oldest hashed complexity variants once more than the cap are present. */
    private void pruneComplexityVariants(String name) {
        try {
            ArtifactKey viewsDir = AnalysisPaths.resolveKey(name, VIEWS_DIR);
            java.util.List<Map.Entry<ArtifactKey, com.analyzer.storage.ArtifactEntry>> variants =
                    new java.util.ArrayList<>();
            for (Map.Entry<ArtifactKey, com.analyzer.storage.ArtifactEntry> e
                    : store.statPrefix(viewsDir).entrySet()) {
                String leaf = e.getKey().leaf();
                if (leaf.startsWith(COMPLEXITY_VARIANT_PREFIX) && leaf.endsWith(".json")) {
                    variants.add(e);
                }
            }
            if (variants.size() <= COMPLEXITY_VARIANT_CAP) return;
            variants.sort(java.util.Comparator.comparingLong(e -> e.getValue().lastModified()));
            int excess = variants.size() - COMPLEXITY_VARIANT_CAP;
            for (int i = 0; i < excess; i++) {
                store.delete(variants.get(i).getKey());
            }
        } catch (Exception e) {
            log.debug("complexity variant prune {} failed: {}", name, e.getMessage());
        }
    }

    /** A short, stable hex digest of {@code s} — the custom-variant cache key. */
    private static String hash16(String s) {
        try {
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(16);
            for (int i = 0; i < 8 && i < digest.length; i++) {
                hex.append(String.format("%02x", digest[i]));
            }
            return hex.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            // SHA-256 is mandated by the platform; this cannot happen. Fall back to a stable id.
            return Integer.toHexString(s.hashCode());
        }
    }
}
