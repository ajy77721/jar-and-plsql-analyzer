package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The per-analysis layout manifest (plan 1f): one small JSON that inventories everything the writer
 * emitted, so a reader can route any API with a single small read instead of listing fans and
 * probing keys.
 *
 * <h2>Built in the existing write pass — never re-read</h2>
 *
 * <p>Every registration happens as the artifact is written: {@link #recordArtifact} is fed the byte
 * length the writer already counted on its way out ({@link CountingOutputStream}) and, where it is
 * known for free, the row/record count. Nothing here opens or stats a large file. The one exception
 * is {@code lastModified}, which object storage cannot supply from the write and which is therefore
 * read once per <em>singleton</em> at serialisation time — a handful of HEADs, not one per node.
 *
 * <h2>Keep it small</h2>
 *
 * <p>A per-key entry for every {@code api/nodes/*.json} would make the manifest as large as the
 * thing it indexes. So artifact families with more than {@link #MAX_GROUP} members are collapsed
 * into one aggregate entry ({@code prefix, count, totalBytes}); only genuinely small groups — the
 * singletons at the top of {@code api/}, {@code resolver/}, and the like — are listed per key. The
 * result stays in the low KBs regardless of analysis size.
 *
 * <h2>Additive and degradable</h2>
 *
 * <p>Written after {@code api/index.json} (the staleness anchor) and, in the normal crawl flow,
 * last of all. A missing or corrupt manifest changes nothing: readers that have not adopted it keep
 * working exactly as before.
 */
public final class LayoutManifest {

    public static final int MANIFEST_VERSION = 1;

    /** Singleton key relative to the analysis directory. */
    public static final String FILE = "api/manifest.json";

    /** Above this many keys under one parent, the family is summarised instead of listed. */
    static final int MAX_GROUP = 16;

    /**
     * Sidecars a reader may expect under {@code views/}, whether or not this analysis produced them
     * (a view is skipped when its facet is empty). Listed, not probed — the whole point of the
     * manifest is to replace existence checks with one read.
     */
    static final List<String> EXPECTED_SIDECARS = List.of(
            "views/header.json",
            "views/sequences.json",
            "views/cursors.json",
            "views/dynamic-sql.json",
            "views/joins-summary.json",
            "views/table-joins.json",
            "views/table-columns.json",
            "views/complexity.json",
            "views/summary-stats.json",
            "views/call-tree/<nodeId>.json",
            "views/views-index.json",
            "views/views-used-by.json",
            "views/view-detail/<NAME>.json",
            "views/triggers-index.json",
            "views/triggers-used-by.json",
            "views/trigger-detail/<NAME>.json",
            "views/globals-index.json",
            "views/globals-used-by.json",
            "views/global-detail/<PKG>.json");

    private final String analysisIdentity;
    private final long generatedAt;

    /** Insertion-ordered so the manifest is deterministic: write order, not hash order. */
    private final Map<String, ArtifactKey> keys = new LinkedHashMap<>();
    private final Map<String, Long> lengths = new LinkedHashMap<>();
    private final Map<String, Long> rowCounts = new LinkedHashMap<>();
    private final Map<String, Long> totals = new LinkedHashMap<>();

    /**
     * Facet presence: {@code facet-view-name -> row count} (an empty facet is {@code 0}). Recorded
     * by the write pass for the families it emits and, for the estate facet sidecars, by
     * {@code AnalysisViewStore} as each sidecar lands. A reader can answer "this analysis has no
     * such data" from one small read instead of scanning nodes — see {@code AnalysisViewStore}.
     */
    private final Map<String, Long> facets = new LinkedHashMap<>();

    /**
     * The analysis's listing scalars — the root scalars of {@code api/index.json} (entry point,
     * schema, node/table/edge/LOC counts, timings, {@code dbCallCount}) plus its {@code errors}
     * array. Supplied by the writer from the index root it already holds, so the history listing can
     * answer from this one small document instead of reading (and, for a big analysis, streaming
     * past the {@code nodes[]} of) {@code api/index.json} once per analysis.
     */
    private final Map<String, Object> listing = new LinkedHashMap<>();

    public LayoutManifest(String analysisIdentity) {
        this(analysisIdentity, System.currentTimeMillis());
    }

    /** Test hook: pins {@code generatedAt} so a manifest can be compared byte for byte. */
    LayoutManifest(String analysisIdentity, long generatedAt) {
        this.analysisIdentity = analysisIdentity == null ? "" : analysisIdentity;
        this.generatedAt = generatedAt;
    }

    /** Records one written artifact and its byte length. Re-recording a key updates it in place. */
    public void recordArtifact(ArtifactKey key, long byteLength) {
        recordArtifact(key, byteLength, null);
    }

    /** As {@link #recordArtifact(ArtifactKey, long)}, with a row/record count where the writer knows it. */
    public void recordArtifact(ArtifactKey key, long byteLength, Long rowCount) {
        if (key == null) return;
        String k = key.toString();
        keys.put(k, key);
        lengths.put(k, byteLength);
        if (rowCount != null) rowCounts.put(k, rowCount);
    }

    /** A top-level count (nodes, tables, statements, edges). */
    public void setCount(String name, long value) {
        totals.put(name, value);
    }

    /**
     * The recorded top-level count, or {@code -1} when the write pass never set it. Used by the
     * writer's own post-write verification ({@code ChunkedFlowWriter#verifyManifestCounts}): the
     * value the manifest will serve is compared against the count the write actually emitted, so a
     * mismatch fails the save rather than being served.
     */
    public long count(String name) {
        Long v = totals.get(name);
        return v == null ? -1L : v;
    }

    /**
     * Records a facet's row count — {@code 0} for a facet the analysis genuinely has none of. Part of
     * the manifest's {@code facets} section; see the field's own note.
     */
    public void setFacet(String name, long rows) {
        if (name != null) facets.put(name, rows);
    }

    /**
     * Records the analysis's listing scalars — see {@link #listing}. Replaces any previous set, so
     * the manifest always reflects the most recent index root. Passing {@code null} clears it.
     */
    public void setListingScalars(Map<String, Object> scalars) {
        listing.clear();
        if (scalars != null) listing.putAll(scalars);
    }

    /** Serialises the manifest to {analysis}/{@code api/manifest.json}. */
    public void writeTo(ArtifactStore store, ArtifactKey manifestKey) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        if (ArtifactFormatting.pretty()) mapper.enable(SerializationFeature.INDENT_OUTPUT);
        try (OutputStream out = store.write(manifestKey)) {
            mapper.writeValue(out, toDocument(store));
        }
    }

    /** The manifest as a document — exposed for tests, and the single place its shape is defined. */
    public Map<String, Object> toDocument(ArtifactStore store) throws IOException {
        // Group recorded keys by parent, preserving write order within each group.
        Map<String, List<String>> groups = new LinkedHashMap<>();
        for (String k : keys.keySet()) {
            ArtifactKey key = keys.get(k);
            ArtifactKey parent = key.parent();
            groups.computeIfAbsent(parent == null ? "" : parent.toString(), p -> new ArrayList<>()).add(k);
        }

        List<Map<String, Object>> artifacts = new ArrayList<>();
        List<Map<String, Object>> collections = new ArrayList<>();
        for (Map.Entry<String, List<String>> g : groups.entrySet()) {
            List<String> members = g.getValue();
            if (members.size() > MAX_GROUP) {
                long totalBytes = 0;
                for (String k : members) totalBytes += lengths.get(k);
                Map<String, Object> c = new LinkedHashMap<>();
                c.put("prefix", g.getKey() + "/");
                c.put("count", members.size());
                c.put("totalBytes", totalBytes);
                collections.add(c);
            } else {
                for (String k : members) {
                    Map<String, Object> a = new LinkedHashMap<>();
                    a.put("key", k);
                    a.put("length", lengths.get(k));
                    Long rows = rowCounts.get(k);
                    if (rows != null) a.put("rows", rows);
                    long lastModified = store.lastModified(keys.get(k));
                    if (lastModified >= 0) a.put("lastModified", lastModified);
                    artifacts.add(a);
                }
            }
        }

        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("manifestVersion", MANIFEST_VERSION);
        doc.put("analysisIdentity", analysisIdentity);
        doc.put("generatedAt", generatedAt);
        doc.put("counts", new LinkedHashMap<>(totals));
        doc.put("facets", new LinkedHashMap<>(facets));
        doc.put("listing", new LinkedHashMap<>(listing));
        doc.put("artifacts", artifacts);
        doc.put("collections", collections);
        doc.put("sidecars", EXPECTED_SIDECARS);
        return doc;
    }
}
