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

    /** Bump to invalidate every existing sidecar whose shape this code no longer understands. */
    public static final int FORMAT_VERSION = 1;

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
    /** The default-weights {@code /complexity/scores} body. */
    public static final String COMPLEXITY = "complexity.json";
    /** The default-scope {@code /summary/stats} body. */
    public static final String SUMMARY_STATS = "summary-stats.json";

    private final ArtifactStore store;
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * One lock per analysis, so a burst of first requests for an un-seeded analysis computes and
     * writes its sidecar once rather than racing to overwrite it.
     */
    private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();

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
            if (!store.exists(k)) return null;
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
    public void write(String name, String view, JsonNode doc) {
        if (doc == null) return;
        synchronized (lockFor(name)) {
            try (OutputStream out = store.write(key(name, view))) {
                mapper.writeValue(out, doc);
            } catch (Exception e) {
                log.warn("view write {} / {} failed: {}", name, view, e.getMessage());
            }
        }
    }

    /**
     * Deletes every sidecar for an analysis. Called by the correction paths, whose change need not
     * move {@code api/index.json}'s mtime (a Claude apply writes {@code claude/tables_merged.json}),
     * so the mtime freshness check alone would not catch it.
     */
    public void invalidate(String name) {
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
     * Stores the listing scalars. The caller passes exactly what {@code readIndexHeader} would have
     * returned, so a sidecar read yields the same {@code AnalysisInfo} the legacy read would.
     * {@code formatVersion} is added here — the one field the legacy reader never produced.
     */
    public void writeHeader(String name, ObjectNode scalars) {
        if (scalars == null) return;
        ObjectNode copy = scalars.deepCopy();
        copy.put("formatVersion", FORMAT_VERSION);
        write(name, HEADER, copy);
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

    /** Merges one computed table's rows into the {@code {table -> rows}} sidecar. */
    public void putTableEntry(String name, String view, String table, JsonNode rows) {
        if (table == null || table.isBlank() || rows == null) return;
        synchronized (lockFor(name)) {
            JsonNode existing = read(name, view);
            ObjectNode map = (existing != null && existing.isObject())
                    ? (ObjectNode) existing.deepCopy()
                    : mapper.createObjectNode();
            map.set(normalizeTable(table), rows);
            write(name, view, map);
        }
    }

    /** Replaces a whole {@code {table -> rows}} sidecar, used by the writer at save time. */
    public void writeTableMap(String name, String view, Map<String, ? extends JsonNode> byTable) {
        ObjectNode map = mapper.createObjectNode();
        byTable.forEach((table, rows) -> {
            if (table != null && !table.isBlank() && rows != null) {
                map.set(normalizeTable(table), rows);
            }
        });
        write(name, view, map);
    }

    /** Table keys are case-insensitive, matching the reader's own {@code upper(table)} matching. */
    private static String normalizeTable(String table) {
        return table.trim().toUpperCase(Locale.ROOT);
    }
}
