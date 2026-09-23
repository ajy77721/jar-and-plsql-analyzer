package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

/**
 * Reads analysis data from the static flow-output directory.
 * This is the original parser output — immutable, written by ChunkedFlowWriter.
 */
@Component("parserStaticAnalysisReader")
public class StaticAnalysisReader implements AnalysisDataReader {

    private static final int MAX_CACHE_ENTRIES = 30;

    final ObjectMapper mapper = new ObjectMapper();
    final Map<String, JsonNode> synthesizedCache = Collections.synchronizedMap(
            new LinkedHashMap<>(32, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, JsonNode> eldest) {
                    return size() > MAX_CACHE_ENTRIES;
                }
            });

    private final ArtifactStore store;

    private final IndexFacetReader indexFacet;
    private final TableFacetReader tableFacet;
    private final NodeScanFacetReader nodeScanFacet;
    private final SequenceFacetReader sequenceFacet;
    private final CallTreeFacetReader callTreeFacet;
    private final CoreFileReader coreFile;
    private final TableDetailFacetReader tableDetailFacet;
    private final TableColumnDetailReader tableColumnDetail;

    /**
     * No data-dir parameter: every location this reader touches is an {@link ArtifactKey} built by
     * {@link AnalysisPaths}, which already carries the {@code plsql-parse/} subtree segment. The
     * removed {@code Path.of(dataDir).resolve("plsql-parse")} root was the last thing here that
     * assumed the parse tree is a local directory.
     */
    public StaticAnalysisReader(@Qualifier("parserArtifactStore") ArtifactStore store) {
        this.store = store;
        this.indexFacet = new IndexFacetReader(this);
        this.tableFacet = new TableFacetReader(this);
        this.nodeScanFacet = new NodeScanFacetReader(this);
        this.sequenceFacet = new SequenceFacetReader(this);
        this.callTreeFacet = new CallTreeFacetReader(this);
        this.coreFile = new CoreFileReader(this);
        this.tableDetailFacet = new TableDetailFacetReader(this);
        this.tableColumnDetail = new TableColumnDetailReader(this);
    }

    /** Q59 — joins touching one table, in either direction. See {@link TableDetailFacetReader}. */
    public JsonNode getTableJoins(String name, String table) throws IOException {
        return tableDetailFacet.getTableJoins(name, table);
    }

    /** B3 — one table's folded column-level statement detail and DDL. See {@link TableColumnDetailReader}. */
    public JsonNode getTableColumnDetail(String name, String table) throws IOException {
        return tableColumnDetail.getTableColumnDetail(name, table);
    }

    /** Q59b — column references to one table. See {@link TableDetailFacetReader}. */
    public JsonNode getTableColumnRefs(String name, String table) throws IOException {
        return tableDetailFacet.getTableColumnRefs(name, table);
    }

    @Override
    public JsonNode getIndex(String name) throws IOException {
        return indexFacet.getIndex(name);
    }

    @Override
    public JsonNode getIndexWithoutNodes(String name) throws IOException {
        return indexFacet.getIndexWithoutNodes(name);
    }

    @Override
    public JsonNode getNodeDetail(String name, String fileName) throws IOException {
        return indexFacet.getNodeDetail(name, fileName);
    }

    @Override
    public JsonNode getProcedures(String name) throws IOException {
        return indexFacet.getProcedures(name);
    }

    @Override
    public JsonNode getTables(String name) throws IOException {
        return tableFacet.getTables(name);
    }

    /** Root scalars of the tables index only — see {@link TableFacetReader#getTablesSummary}. */
    public JsonNode getTablesSummary(String name) throws IOException {
        return tableFacet.getTablesSummary(name);
    }


    /** One row per table with only the fields a list needs; see {@link TableFacetReader}. */
    public JsonNode getTablesList(String name) throws IOException {
        return tableFacet.getTablesList(name);
    }

    @Override
    public JsonNode getCallGraph(String name) throws IOException {
        return coreFile.getCallGraph(name);
    }

    @Override
    public String getSource(String name, String fileName) throws IOException {
        return coreFile.getSource(name, fileName);
    }

    @Override
    public JsonNode getResolver(String name, String type) throws IOException {
        String fileName = switch (type) {
            case "q1" -> "q1_object_resolution.json";
            case "q3" -> "q3_reverse_dependencies.json";
            case "q4" -> "q4_direct_dependencies.json";
            case "ambiguous" -> "q1_ambiguous_objects.json";
            default -> throw new IllegalArgumentException("Unknown resolver type: " + type);
        };
        try (InputStream in = openRead(resolveKey(name, "resolver/" + fileName))) {
            return mapper.readTree(in);
        }
    }

    List<ArtifactKey> collectNodeFiles(String name) throws IOException {
        return coreFile.collectNodeFiles(name);
    }

    @Override
    public JsonNode getJoins(String name) throws IOException {
        return nodeScanFacet.getJoins(name);
    }

    @Override
    public JsonNode getCursors(String name) throws IOException {
        return nodeScanFacet.getCursors(name);
    }

    @Override
    public JsonNode getSequences(String name) throws IOException {
        return sequenceFacet.getSequences(name);
    }

    public JsonNode getDynamicSql(String name) throws IOException {
        return sequenceFacet.getDynamicSql(name);
    }

    String cacheKey(String name, String type) {
        return name + "::" + type;
    }

    public void evictCache(String analysisName) {
        synthesizedCache.remove(cacheKey(analysisName, "joins"));
        synthesizedCache.remove(cacheKey(analysisName, "cursors"));
        synthesizedCache.remove(cacheKey(analysisName, "sequences"));
        synthesizedCache.remove(cacheKey(analysisName, "tables"));
        synthesizedCache.remove(cacheKey(analysisName, "dynamicSql"));
        // The table-scoped scans are keyed per table, so there is no fixed list to remove. Left
        // behind they would serve rows from the superseded run after a re-analysis — the modal
        // would show joins for procedures that no longer exist, with nothing to indicate why.
        String prefix = cacheKey(analysisName, "table");
        synchronized (synthesizedCache) {
            synthesizedCache.keySet().removeIf(k -> k.startsWith(prefix));
        }
    }

    /** Clear the entire synthesized cache (all analyses). Returns the number of entries dropped. */
    public int evictAll() {
        int n = synthesizedCache.size();
        synthesizedCache.clear();
        return n;
    }

    @Override
    public JsonNode getCallTree(String name, String nodeId) throws IOException {
        return callTreeFacet.getCallTree(name, nodeId);
    }

    @Override
    public JsonNode getCallTree(String name, String nodeId, Integer maxDepth) throws IOException {
        return callTreeFacet.getCallTree(name, nodeId, getCallGraph(name), maxDepth);
    }

    /** Build the call tree using a supplied call graph (lets the Claude reader inject
     *  a corrected/overlaid graph so the tree reflects accepted call changes). */
    public JsonNode getCallTree(String name, String nodeId, JsonNode suppliedCallGraph) throws IOException {
        return callTreeFacet.getCallTree(name, nodeId, suppliedCallGraph);
    }

    /** UI-019 (§142) — same as above, with the per-level {@code maxDepth} threaded through too. */
    public JsonNode getCallTree(String name, String nodeId, JsonNode suppliedCallGraph, Integer maxDepth) throws IOException {
        return callTreeFacet.getCallTree(name, nodeId, suppliedCallGraph, maxDepth);
    }

    @Override
    public JsonNode getCallers(String name, String nodeId) throws IOException {
        return callTreeFacet.getCallers(name, nodeId);
    }

    public JsonNode getCallers(String name, String nodeId, JsonNode suppliedCallGraph) throws IOException {
        return callTreeFacet.getCallers(name, nodeId, suppliedCallGraph);
    }

    // --- helpers (package-visible for reuse by Claude reader) ---

    Map<String, String> loadIdMap(String name) {
        try {
            ArtifactKey key = resolveKey(name, "api/node_id_map.json");
            if (!store.exists(key)) return Collections.emptyMap();
            JsonNode root;
            try (InputStream in = openRead(key)) {
                root = mapper.readTree(in);
            }
            JsonNode stf = root.get("shortToFull");
            if (stf == null || !stf.isObject()) return Collections.emptyMap();
            Map<String, String> map = new HashMap<>();
            stf.fields().forEachRemaining(e -> map.put(e.getKey(), e.getValue().asText()));
            return map;
        } catch (IOException e) {
            return Collections.emptyMap();
        }
    }

    /**
     * A file inside an analysis, as a storage key. Identity name maps to a nested
     * {@code <DBTYPE>/<PROJECT>/<ENV>/<OBJECT>} location (see {@link AnalysisPaths}).
     */
    ArtifactKey resolveKey(String name, String relative) {
        return AnalysisPaths.resolveKey(name, relative);
    }

    /** The store every read in this class goes through. Package-visible for the facet readers. */
    ArtifactStore store() { return store; }

    InputStream openRead(ArtifactKey key) throws IOException { return store.read(key); }

    OutputStream openWrite(ArtifactKey key) throws IOException { return store.write(key); }

    OutputStream openWrite(ArtifactKey key, long contentLength) throws IOException {
        return store.write(key, contentLength);
    }

    static String textOrDefault(JsonNode node, String field, String def) {
        JsonNode v = node.get(field);
        return (v != null && !v.isNull()) ? v.asText() : def;
    }

    static int intOrDefault(JsonNode node, String field, int def) {
        JsonNode v = node.get(field);
        return (v != null && v.isNumber()) ? v.asInt() : def;
    }

    static String textOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v != null && !v.isNull()) ? v.asText() : null;
    }
}
