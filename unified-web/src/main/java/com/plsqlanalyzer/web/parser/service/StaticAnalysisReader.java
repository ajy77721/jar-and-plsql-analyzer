package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.analyzer.auth.ConnectionGuard;
import org.springframework.beans.factory.annotation.Autowired;
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

    /** Routing identity for cache scoping; {@code null} in a store-only test construction. */
    private final ConnectionGuard connectionGuard;

    private final IndexFacetReader indexFacet;
    private final TableFacetReader tableFacet;
    private final NodeScanFacetReader nodeScanFacet;
    private final SequenceFacetReader sequenceFacet;
    private final CallTreeFacetReader callTreeFacet;
    private final CoreFileReader coreFile;
    private final TableDetailFacetReader tableDetailFacet;
    private final TableColumnDetailReader tableColumnDetail;
    /** Estate facets — views/MVs, triggers, package globals. See {@link EstateFacetReader}. */
    private final EstateFacetReader estateFacet;

    /**
     * No data-dir parameter: every location this reader touches is an {@link ArtifactKey} built by
     * {@link AnalysisPaths}, which already carries the {@code plsql-parse/} subtree segment. The
     * removed {@code Path.of(dataDir).resolve("plsql-parse")} root was the last thing here that
     * assumed the parse tree is a local directory.
     */
    /** Test / non-Spring construction: no per-user identity, so the cache is shared as before. */
    public StaticAnalysisReader(@Qualifier("parserArtifactStore") ArtifactStore store) {
        this(store, null);
    }

    @Autowired
    public StaticAnalysisReader(@Qualifier("parserArtifactStore") ArtifactStore store,
                                ConnectionGuard connectionGuard) {
        this.store = store;
        this.connectionGuard = connectionGuard;
        this.indexFacet = new IndexFacetReader(this);
        this.tableFacet = new TableFacetReader(this);
        this.nodeScanFacet = new NodeScanFacetReader(this);
        this.sequenceFacet = new SequenceFacetReader(this);
        this.callTreeFacet = new CallTreeFacetReader(this);
        this.coreFile = new CoreFileReader(this);
        this.tableDetailFacet = new TableDetailFacetReader(this);
        this.tableColumnDetail = new TableColumnDetailReader(this);
        this.estateFacet = new EstateFacetReader(this);
    }

    /* ── estate facets: views/MVs, triggers, package globals ── */

    /** Every view / materialized view — see {@link EstateFacetReader#getViews(String)}. */
    public JsonNode getViews(String name) throws IOException {
        return estateFacet.getViews(name);
    }

    /** One view's full detail (row + chain + used-by), or null when the analysis has no such view. */
    public JsonNode getViewDetail(String name, String view) throws IOException {
        return estateFacet.getViewDetail(name, view);
    }

    /** The full view-on-view→base-table chain for one view. */
    public JsonNode getViewChain(String name, String view) throws IOException {
        return estateFacet.viewChain(name, view);
    }

    /** Which nodes read one view. */
    public JsonNode getViewUsedBy(String name, String view) throws IOException {
        return estateFacet.getViewUsedBy(name, view);
    }

    /** Every trigger — see {@link EstateFacetReader#getTriggers(String)}. */
    public JsonNode getTriggers(String name) throws IOException {
        return estateFacet.getTriggers(name);
    }

    public JsonNode getTriggerDetail(String name, String trigger) throws IOException {
        return estateFacet.getTriggerDetail(name, trigger);
    }

    /** The trigger→touched-table→other-triggers chain for one trigger. */
    public JsonNode getTriggerChain(String name, String trigger) throws IOException {
        return estateFacet.triggerChain(name, trigger);
    }

    public JsonNode getTriggerUsedBy(String name, String trigger) throws IOException {
        return estateFacet.getTriggerUsedBy(name, trigger);
    }

    /** Package global variables with their estate-wide usage — see {@link EstateFacetReader#getGlobals(String)}. */
    public JsonNode getGlobals(String name) throws IOException {
        return estateFacet.getGlobals(name);
    }

    public JsonNode getGlobalDetail(String name, String pkg) throws IOException {
        return estateFacet.getGlobalDetail(name, pkg);
    }

    public JsonNode getGlobalUsedBy(String name, String variable) throws IOException {
        return estateFacet.getGlobalUsedBy(name, variable);
    }

    /** Scheduled jobs with schedule/enabled/target — see {@link EstateFacetReader#getJobs(String)}. */
    public JsonNode getJobs(String name) throws IOException {
        return estateFacet.getJobs(name);
    }

    /** {@code {VIEW -> usedBy[]}} whole-analysis map — the {@code views/views-used-by.json} sidecar. */
    public JsonNode getViewsUsedByMap(String name) throws IOException {
        return estateFacet.getViewsUsedByMap(name);
    }

    public JsonNode getTriggersUsedByMap(String name) throws IOException {
        return estateFacet.getTriggersUsedByMap(name);
    }

    public JsonNode getGlobalsUsedByMap(String name) throws IOException {
        return estateFacet.getGlobalsUsedByMap(name);
    }

    /** Q59 — joins touching one table, in either direction. See {@link TableDetailFacetReader}. */
    public JsonNode getTableJoins(String name, String table) throws IOException {
        return tableDetailFacet.getTableJoins(name, table);
    }

    /**
     * Every table's joins in one node scan — the writer's precomputed {@code table-joins} view
     * summary. Each map value equals {@link #getTableJoins(String, String)} for that table; see
     * {@link TableDetailFacetReader#getAllTableJoins}.
     */
    public java.util.Map<String, JsonNode> getAllTableJoins(String name) throws IOException {
        return tableDetailFacet.getAllTableJoins(name);
    }

    /**
     * Every table's column references in one node scan — the writer's precomputed {@code
     * table-columns} view summary. See {@link TableDetailFacetReader#getAllTableColumnRefs}.
     */
    public java.util.Map<String, JsonNode> getAllTableColumnRefs(String name) throws IOException {
        return tableDetailFacet.getAllTableColumnRefs(name);
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
        JsonNode nodes = indexFacet.getProcedures(name);
        if (nodes == null || !nodes.isArray()) return nodes;
        // "scheduled" badge: a procedure that is the target of a discovered scheduled job is
        // stamped `scheduled:true` so the procedures facet can show it. Read from the crawler's
        // api/jobs/index.json; an analysis with no jobs returns the nodes array untouched
        // (byte-identical), so existing output is unchanged.
        Set<String> targets = scheduledTargets(name);
        if (targets.isEmpty()) return nodes;
        ArrayNode out = mapper.createArrayNode();
        for (JsonNode n : nodes) {
            String nodeId = n.path("nodeId").asText("");
            String nodeName = n.path("name").asText("");
            boolean scheduled = targets.contains(nodeId.toUpperCase(Locale.ROOT))
                    || targets.contains(nodeName.toUpperCase(Locale.ROOT));
            if (!scheduled) {
                out.add(n);
            } else {
                ObjectNode copy = n.deepCopy();
                copy.put("scheduled", true);
                out.add(copy);
            }
        }
        return out;
    }

    /** Node ids / bare names of every routine targeted by a discovered scheduled job. */
    private Set<String> scheduledTargets(String name) {
        Set<String> out = new HashSet<>();
        try {
            JsonNode doc = getJobsIndex(name);
            if (doc == null) return out;
            for (JsonNode j : doc.path("jobs")) {
                String targetNodeId = j.path("targetNodeId").asText("");
                if (!targetNodeId.isEmpty()) out.add(targetNodeId.toUpperCase(Locale.ROOT));
                String target = j.path("target").asText("");
                if (!target.isEmpty()) out.add(target.toUpperCase(Locale.ROOT));
            }
        } catch (Exception e) {
            // No jobs artifact (or unreadable) — no scheduled badges. Not an error.
        }
        return out;
    }

    @Override
    public JsonNode getTables(String name) throws IOException {
        return tableFacet.getTables(name);
    }

    /**
     * The crawler's {@code api/jobs/index.json} document ({@code {totalJobs, jobs:[…]}}), or null
     * when the analysis has none. Scheduled jobs are a crawler output (not a node scan), so this is
     * a raw artifact read, exactly like {@code getTables}.
     */
    public JsonNode getJobsIndex(String name) throws IOException {
        ArtifactKey key = resolveKey(name, "api/jobs/index.json");
        if (!store.exists(key)) return null;
        try (InputStream in = openRead(key)) {
            return mapper.readTree(in);
        }
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

    /**
     * One listing of the analysis's {@code sources/} directory, keyed by leaf name — built once per
     * view computation so a node-scan resolves every node's {@code sourceFile} with zero per-source
     * storage ops. See {@link CoreFileReader#sourceKeyIndex(String)}.
     */
    Map<String, ArtifactKey> sourceKeyIndex(String name) throws IOException {
        return coreFile.sourceKeyIndex(name);
    }

    /** Source text resolved against a prebuilt {@link #sourceKeyIndex} listing; null when absent. */
    String getSourceFromIndex(Map<String, ArtifactKey> index, String fileName) throws IOException {
        return coreFile.getSourceFromIndex(index, fileName);
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

    /**
     * The cache key for one synthesized view of one analysis, scoped to the calling user's routing
     * identity as well as the analysis name. Without the identity, the same analysis name read from
     * two users' buckets collided in the one in-process map — one user could be served another's
     * parsed view. Requests that resolve to no identity share the {@link ConnectionGuard#SHARED}
     * partition, preserving the previous single-tenant behaviour.
     */
    String cacheKey(String name, String type) {
        String identity = connectionGuard == null
                ? ConnectionGuard.SHARED : connectionGuard.routingIdentityKey();
        return identity + "::" + name + "::" + type;
    }

    public void evictCache(String analysisName) {
        synthesizedCache.remove(cacheKey(analysisName, "joins"));
        synthesizedCache.remove(cacheKey(analysisName, "cursors"));
        synthesizedCache.remove(cacheKey(analysisName, "sequences"));
        synthesizedCache.remove(cacheKey(analysisName, "tables"));
        synthesizedCache.remove(cacheKey(analysisName, "dynamicSql"));
        // Estate facets: the synthesized index and the memoized tables-index read they derive from.
        synthesizedCache.remove(cacheKey(analysisName, "views"));
        synthesizedCache.remove(cacheKey(analysisName, "triggers"));
        synthesizedCache.remove(cacheKey(analysisName, "globals"));
        synthesizedCache.remove(cacheKey(analysisName, "estate-tables-raw"));
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

    /**
     * The entry-point node ids {@link AnalysisViewWriter} precomputes call trees for at save time —
     * the roots plus their depth-1 callees, capped. See
     * {@link CallTreeFacetReader#entryPointNodeIds(String)}.
     */
    public List<String> callTreeEntryPoints(String name) throws IOException {
        return callTreeFacet.entryPointNodeIds(name);
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

    /**
     * The callers arrays for several node ids in one pass, sharing the index/call-graph/id-map read
     * — the save-time writer's precompute of the entry points' callers. Each value equals
     * {@link #getCallers(String, String)} for that id.
     */
    public Map<String, JsonNode> getCallersForNodes(String name, List<String> nodeIds) throws IOException {
        return callTreeFacet.getCallersForNodes(name, nodeIds);
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
