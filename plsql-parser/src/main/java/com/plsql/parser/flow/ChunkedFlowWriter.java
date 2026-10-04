package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.plsql.parser.model.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Writes the crawler's chunked output through {@link ArtifactStore}.
 *
 * <p>Every artifact is addressed by an {@link ArtifactKey} rooted at the data root, so the same
 * code writes to local disk or to an object store with no branch here. The keys produced are
 * exactly the ones the unified-web readers resolve via {@code AnalysisPaths.resolveKey}.
 */
public class ChunkedFlowWriter {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(ChunkedFlowWriter.class);

    private final ArtifactStore store;
    private final ArtifactKey buildKey;
    private final ArtifactKey chunksKey;
    private final ArtifactKey sourcesKey;
    private final ArtifactKey edgesDirKey;
    private final ArtifactKey progressKey;
    private final ArtifactKey edgesFileKey;
    private final ArtifactKey tablesKey;
    private final ObjectMapper mapper;
    private final AtomicInteger chunkCounter = new AtomicInteger(0);

    /**
     * Layout manifest (plan 1f) — built incrementally as this writer emits artifacts, written last.
     * Fed lengths and counts the write pass already knows; it never re-reads a large file.
     */
    private final LayoutManifest manifest;

    private final Map<String, String> progressMap = new LinkedHashMap<>();
    private List<Map<String, Object>> discoveredTriggers = new ArrayList<>();
    /** Scheduled-job facet rows (name/schedule/enabled/target/nodeId), written to api/jobs/index.json. */
    private List<Map<String, Object>> discoveredJobs = new ArrayList<>();

    /**
     * Package-level variable declarations captured at PARSE time, keyed by upper-case package name
     * — the estate globals facet's declaration list. Written to the dedicated
     * {@code api/globals/declarations.json} artifact during the write pass, INDEPENDENT of whether a
     * PACKAGE_BODY/PACKAGE_SPEC node was crawled (a member entry point crawls no package node, so the
     * per-node {@code packageGlobalDeclarations} path never fired). See
     * {@link #recordPackageGlobals(String, List)}.
     */
    private final Map<String, List<VariableInfo>> packageGlobalDeclarations = new LinkedHashMap<>();

    /**
     * The source artifact name actually written for a node, keyed by node identity — so the
     * index/node-detail {@code sourceFile} records the extension the artifact really has
     * ({@code .prc} for a standalone PROCEDURE, {@code .fnc} for a FUNCTION) instead of always
     * assuming {@code .sql}. Without this, every source read had to probe the extension variants
     * (exact → .sql → .prc → …) because the recorded name and the stored name disagreed.
     *
     * <p>Populated by {@link #recordSourceFile} from the crawler's own source write, which is the
     * only code that knows the extension it chose. A node with no entry falls back to the legacy
     * {@code .sql} name — the same value this writer produced before.
     */
    private final Map<String, String> writtenSourceFileNames = new HashMap<>();

    /** MID-026 (§177) — the fallback for a node id missing from {@code nodeMetrics}, matching the
     *  original mutation-based code's own implicit default (an unset {@code FlowNode} field reads
     *  as {@code 0}/an empty map). */
    private static final DependencyCrawler.NodeMetrics EMPTY_NODE_METRICS =
            new DependencyCrawler.NodeMetrics(Map.of(), 0);

    public ChunkedFlowWriter(ArtifactStore store, ArtifactKey buildKey) throws IOException {
        this.store = store;
        this.buildKey = buildKey;
        this.chunksKey = buildKey.resolve("chunks");
        this.sourcesKey = buildKey.resolve("sources");
        this.edgesDirKey = buildKey.resolve("edges");
        this.progressKey = buildKey.resolve("progress.json");
        this.edgesFileKey = buildKey.resolve("edges.json");
        this.tablesKey = buildKey.resolve("tables.json");

        // No createDirectories: the store materialises parents on write, and an object store has
        // no directories to create.

        this.mapper = new ObjectMapper();
        // Indentation is 27.6% of the bytes on this corpus (310.3 MB pretty, 224.7 MB
        // compact across 2,908 files) and nothing reads it. -Dplsql.artifacts.pretty=true
        // restores it for a debugging run.
        if (ArtifactFormatting.pretty()) mapper.enable(SerializationFeature.INDENT_OUTPUT);

        // Initialised before the first write so every artifact this writer emits is registered.
        this.manifest = new LayoutManifest(buildKey.toString());

        writeString(tablesKey, "{}");
        writeProgress();
    }

    /**
     * Serialises {@code value} straight into the store — never buffered as a byte[] here. The byte
     * count is captured on the way out (no second pass, no stat) and recorded in the layout
     * manifest.
     */
    private long writeJson(ArtifactKey key, Object value) throws IOException {
        long bytes;
        try (CountingOutputStream out = new CountingOutputStream(store.write(key))) {
            mapper.writeValue(out, value);
            bytes = out.count();
        }
        manifest.recordArtifact(key, bytes);
        return bytes;
    }

    private <T> T readJson(ArtifactKey key, Class<T> type) throws IOException {
        try (InputStream in = store.read(key)) {
            return mapper.readValue(in, type);
        }
    }

    private void writeString(ArtifactKey key, String content) throws IOException {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        try (OutputStream out = store.write(key)) {
            out.write(bytes);
        }
        manifest.recordArtifact(key, bytes.length);
    }

    private String readString(ArtifactKey key) throws IOException {
        try (InputStream in = store.read(key)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** Direct children of {@code prefix} whose leaf starts with {@code leafPrefix}, name-sorted. */
    private List<ArtifactKey> listSorted(ArtifactKey prefix, String leafPrefix) throws IOException {
        try (var s = store.list(prefix, false)) {
            return s.filter(java.util.function.Predicate.not(ArtifactEntry::directory))
                    .map(ArtifactEntry::key)
                    .filter(k -> k.leaf().startsWith(leafPrefix))
                    .sorted(Comparator.comparing(ArtifactKey::leaf))
                    .toList();
        }
    }

    /** The {@code sources/} prefix, handed to SourceDownloader as its local source cache. */
    public ArtifactKey getSourcesKey() {
        return sourcesKey;
    }

    /**
     * How many node chunks this writer has written — i.e. how many nodes the crawl produced.
     * Zero means the crawl found nothing to analyze; {@code DependencyCrawler} refuses to
     * persist that as an analysis (see its empty-crawl guard).
     */
    public int getChunkCount() {
        return chunkCounter.get();
    }

    public void setDiscoveredTriggers(List<Map<String, Object>> triggers) {
        this.discoveredTriggers = triggers != null ? triggers : new ArrayList<>();
    }

    /**
     * Record a package's package-level variable declarations, captured at parse time from the
     * PACKAGE_SPEC/PACKAGE_BODY {@code ParsedObject}. Persisted as the dedicated
     * {@code api/globals/declarations.json} during the write pass — the estate globals facet reads
     * it directly, so the declarations survive even when the crawl produced no PACKAGE node (the
     * member entry point case). Duplicate variable names within a package are ignored; a re-record
     * from another member of the same package merges rather than replaces.
     */
    public void recordPackageGlobals(String packageName, List<VariableInfo> vars) {
        if (packageName == null || packageName.isBlank() || vars == null || vars.isEmpty()) return;
        List<VariableInfo> existing = packageGlobalDeclarations
                .computeIfAbsent(packageName.toUpperCase(Locale.ROOT), k -> new ArrayList<>());
        for (VariableInfo v : vars) {
            if (v == null || v.getName() == null || v.getName().isBlank()) continue;
            boolean dup = false;
            for (VariableInfo e : existing) {
                if (e.getName().equalsIgnoreCase(v.getName())) { dup = true; break; }
            }
            if (!dup) existing.add(v);
        }
    }

    /** Writes {@code api/globals/declarations.json} — package → declared variables. */
    private void writeGlobalsDeclarations(ArtifactKey apiDir) throws IOException {
        if (packageGlobalDeclarations.isEmpty()) return;
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("declarations", packageGlobalDeclarations);
        writeJson(apiDir.resolve("globals", "declarations.json"), doc);
    }

    public void setDiscoveredJobs(List<Map<String, Object>> jobs) {
        this.discoveredJobs = jobs != null ? jobs : new ArrayList<>();
    }

    public List<Map<String, Object>> getDiscoveredJobs() {
        return discoveredJobs;
    }

    public void markPending(String visitKey) throws IOException {
        progressMap.put(visitKey, "PENDING");
        writeProgress();
    }

    public void markProcessing(String visitKey) throws IOException {
        progressMap.put(visitKey, "PROCESSING");
        writeProgress();
    }

    public void markDone(String visitKey) throws IOException {
        progressMap.put(visitKey, "DONE");
        writeProgress();
    }

    public void markFailed(String visitKey, String error) throws IOException {
        progressMap.put(visitKey, "FAILED: " + error);
        writeProgress();
    }

    public void writeChunk(FlowNode node) throws IOException {
        int idx = chunkCounter.incrementAndGet();
        String fileName = String.format("chunk_%04d.json", idx);
        writeJson(chunksKey.resolve(fileName), node);
    }

    public void appendEdges(List<FlowEdge> newEdges, int chunkIdx) throws IOException {
        if (newEdges == null || newEdges.isEmpty()) return;
        String fileName = String.format("edges_%04d.json", chunkIdx);
        writeJson(edgesDirKey.resolve(fileName), newEdges);
    }

    /**
     * Append edges under an automatically allocated, collision-free index. Post-crawl additive
     * emitters (scheduled jobs) run after the BFS and the trigger pass, neither of which reports a
     * final chunk index back — reusing {@link #appendEdges}'s caller-supplied index would overwrite
     * an earlier {@code edges_XXXX.json}. This shares the chunk counter so the index is unique.
     */
    public void appendEdgesAuto(List<FlowEdge> newEdges) throws IOException {
        if (newEdges == null || newEdges.isEmpty()) return;
        appendEdges(newEdges, chunkCounter.incrementAndGet());
    }

    public void updateTables(Map<String, SchemaTableInfo> tableInfoMap) throws IOException {
        writeJson(tablesKey, tableInfoMap);
    }

    public void writeSourceFile(String fileName, String content) throws IOException {
        writeString(sourcesKey.resolve(fileName), content);
    }

    public boolean sourceExists(String fileName) {
        return store.exists(sourcesKey.resolve(fileName));
    }

    public String readSourceFile(String fileName) throws IOException {
        ArtifactKey k = sourcesKey.resolve(fileName);
        return store.exists(k) ? readString(k) : null;
    }

    /**
     * MID-026 — the per-chunk re-read primitive the last 2 remaining pieces of the bounded
     * conversion series need. Chunk files are named {@code chunk_%04d.json} by write order
     * ({@link #writeChunk}), not by node id, so a node's chunk cannot be located by name alone.
     *
     * <p>Built once per {@code mergeChunks} run, not per lookup: reads every chunk exactly once
     * (the same single pass {@code mergeChunks} itself already does) and keeps only the resulting
     * {@code nodeId -> chunk key} map — the full {@code FlowNode} bodies read during index
     * construction are discarded immediately after their id is extracted, never retained. This is
     * the same "read once, project, discard" shape as {@link #toLightNode}, applied to file
     * location instead of field selection.
     */
    Map<String, ArtifactKey> buildChunkIndex() throws IOException {
        Map<String, ArtifactKey> index = new LinkedHashMap<>();
        for (ArtifactKey cf : listSorted(chunksKey, "chunk_")) {
            FlowNode n = readJson(cf, FlowNode.class);
            if (n.getNodeId() != null) index.put(n.getNodeId(), cf);
        }
        return index;
    }

    /**
     * Re-reads one node's full body from its chunk file, given an index already built by
     * {@link #buildChunkIndex()}. Returns {@code null} if the id isn't in the index (a dangling
     * reference, not an error this method should throw on — the same "record it, don't crash"
     * reasoning {@code writeStatementIndex}'s own {@code nodeRef} handling already established).
     */
    FlowNode readNodeChunk(String nodeId, Map<String, ArtifactKey> chunkIndex) throws IOException {
        ArtifactKey cf = chunkIndex.get(nodeId);
        return cf == null ? null : readJson(cf, FlowNode.class);
    }

    public FlowResult mergeChunks(String entryPoint, String entrySchema, long crawlTimeMs,
                                   int maxDepthReached, List<String> errors) throws IOException {
        FlowResult result = new FlowResult();
        result.setEntryPoint(entryPoint);
        result.setEntrySchema(entrySchema);
        result.setCrawlTimeMs(crawlTimeMs);
        result.setMaxDepthReached(maxDepthReached);
        result.setErrors(errors);

        List<FlowNode> allNodes = new ArrayList<>();
        for (ArtifactKey cf : listSorted(chunksKey, "chunk_")) {
            // One corrupt/incompatible chunk must not discard every other node already read —
            // mirrors NodeParseCache.get()'s self-healing pattern: log a warning naming the chunk
            // and skip it, rather than letting the exception abort the whole merge (the incident
            // this fixes: ExceptionHandlerInfo.getParentExceptionName() used to leak into chunk
            // JSON and every subsequent readJson() here threw, wiping out the full result).
            try {
                allNodes.add(readJson(cf, FlowNode.class));
            } catch (IOException e) {
                String msg = "Skipping corrupt/unreadable chunk " + cf + ": " + e.getMessage();
                log.warn("[ChunkedFlowWriter] " + msg, e);
                if (errors != null) errors.add(msg);
            }
        }
        result.setFlowNodes(allNodes);

        List<FlowEdge> allEdges = new ArrayList<>();
        for (ArtifactKey ef : listSorted(edgesDirKey, "edges_")) {
            allEdges.addAll(Arrays.asList(readJson(ef, FlowEdge[].class)));
        }
        reconcileEdgeSchemas(allEdges, allNodes);
        result.setCallGraph(allEdges);
        writeJson(edgesFileKey, allEdges);

        String tablesContent = store.exists(tablesKey) ? readString(tablesKey) : "{}";
        if (!tablesContent.isBlank() && !tablesContent.equals("{}")) {
            @SuppressWarnings("unchecked")
            Map<String, SchemaTableInfo> tableMap = mapper.readValue(tablesContent,
                    mapper.getTypeFactory().constructMapType(
                            LinkedHashMap.class, String.class, SchemaTableInfo.class));
            result.setAllTables(new ArrayList<>(tableMap.values()));
        }

        result.setTotalObjectsCrawled(allNodes.size());

        Set<String> uniqueNodeIds = new HashSet<>();
        int procs = 0, funcs = 0, pkgProcs = 0, pkgFuncs = 0, totalLoc = 0;
        for (FlowNode n : allNodes) {
            if (n.getNodeId() != null) uniqueNodeIds.add(n.getNodeId());
            totalLoc += n.getLinesOfCode();
            String type = n.getObjectType() != null ? n.getObjectType().toUpperCase() : "";
            boolean hasPkg = n.getPackageName() != null && !n.getPackageName().isEmpty();
            if (type.contains("PROCEDURE")) {
                if (hasPkg) pkgProcs++; else procs++;
            } else if (type.contains("FUNCTION")) {
                if (hasPkg) pkgFuncs++; else funcs++;
            }
        }
        result.setUniqueObjectsCrawled(uniqueNodeIds.size());
        result.setTotalProcedures(procs);
        result.setTotalFunctions(funcs);
        result.setTotalPackageProcedures(pkgProcs);
        result.setTotalPackageFunctions(pkgFuncs);
        result.setTotalLinesOfCode(totalLoc);

        if (writeFlowJsonEnabled()) {
            ArtifactKey flowJson = buildKey.resolve("flow.json");
            writeJson(flowJson, result);
            System.err.println("[ChunkedFlowWriter] Merged " + allNodes.size()
                    + " chunks into " + flowJson);
        } else {
            System.err.println("[ChunkedFlowWriter] Merged " + allNodes.size()
                    + " chunks (flow.json not written; nothing reads it — "
                    + "-Dplsql.write-flow-json=true to restore)");
        }

        return result;
    }

    /**
     * Single source of truth for a node's schema: the crawled {@link FlowNode} itself.
     *
     * <p><b>The bug this fixes.</b> A {@code FlowEdge}'s {@code toSchema}/{@code toNodeId} are
     * computed in {@code DependencyCrawler.enqueueExternalCalls} at CALL-SITE time, from nothing
     * but the call's own text — and when the call carries no schema the code falls back to the
     * CALLER's schema. The target node's schema, by contrast, is resolved much later (in the BFS
     * body, via {@code resolveObjectSchema} → {@code SchemaResolver}/owner cache) once the
     * target's source has actually been fetched and its real owner is known. For a
     * schema-qualified builtin such as PostgreSQL's {@code pg_catalog.unnest(...)} these two
     * disagree: the node correctly becomes {@code PG_CATALOG$UNNEST} (so
     * {@code api/nodes/PG_CATALOG$UNNEST.json} and {@code api/index.json} are right), while every
     * edge pointing at it kept the stale caller-schema guess {@code ERP$UNNEST} — which is what
     * {@code api/call_graph.json} (and the UI's Hierarchy tab) showed. Nothing ever reconciled the
     * two, so the same fact had two representations and only one of them was ever fixed.
     *
     * <p><b>The fix.</b> Once merging has BOTH the fully resolved node list and the full edge
     * list in hand, re-point any edge endpoint whose {@code nodeId} matches no real node at the
     * unique node with the same package/object (and the same {@code __<arity>} overload
     * discriminator), adopting that node's schema and id. Deliberately conservative:
     *
     * <ul>
     *   <li>edges whose endpoint already resolves to a real node are never touched — so every
     *       dialect's existing, correct output stays byte-identical;</li>
     *   <li>an ambiguous key (the same package/object genuinely crawled under two schemas) is
     *       left alone rather than guessed at;</li>
     *   <li>it is dialect-agnostic — it reconciles against whatever the crawler resolved, so it
     *       cannot drift from the per-node artifacts the way the duplicated derivation did.</li>
     * </ul>
     *
     * <p>Both {@code call_graph.json} and {@code index.json}'s edge counts read
     * {@code result.getCallGraph()}, so fixing the list here fixes every downstream consumer at
     * once.
     */
    static void reconcileEdgeSchemas(List<FlowEdge> edges, List<FlowNode> nodes) {
        if (edges == null || edges.isEmpty() || nodes == null || nodes.isEmpty()) return;

        Set<String> realNodeIds = new HashSet<>();
        // schema-less key ("PKG$OBJ" / "OBJ", plus any "__<arity>") -> the node(s) that match it
        Map<String, List<FlowNode>> bySchemalessKey = new LinkedHashMap<>();
        for (FlowNode n : nodes) {
            if (n == null || n.getNodeId() == null) continue;
            realNodeIds.add(n.getNodeId());
            bySchemalessKey.computeIfAbsent(schemalessKey(n), k -> new ArrayList<>()).add(n);
        }

        for (FlowEdge e : edges) {
            FlowNode to = uniqueMatch(bySchemalessKey, realNodeIds, e.getToNodeId(),
                    e.getToPackage(), e.getToObject());
            if (to != null) {
                e.setToNodeId(to.getNodeId());
                e.setToSchema(to.getSchema());
            }
            FlowNode from = uniqueMatch(bySchemalessKey, realNodeIds, e.getFromNodeId(),
                    e.getFromPackage(), e.getFromObject());
            if (from != null) {
                e.setFromNodeId(from.getNodeId());
                e.setFromSchema(from.getSchema());
            }
        }
    }

    /** The node's id with its leading schema segment removed (keeps any overload discriminator). */
    private static String schemalessKey(FlowNode n) {
        String disc = n.getOverloadDiscriminator();
        return normalizeIdPart(n.getPackageName(), n.getObjectName())
                + (disc != null ? "__" + disc : "");
    }

    /**
     * The node an unresolvable edge endpoint should point at, or {@code null} to leave it alone
     * (endpoint already valid, no match, or an ambiguous cross-schema match).
     */
    private static FlowNode uniqueMatch(Map<String, List<FlowNode>> bySchemalessKey,
                                        Set<String> realNodeIds, String nodeId,
                                        String pkg, String object) {
        if (nodeId == null || nodeId.isEmpty() || realNodeIds.contains(nodeId)) return null;
        // Preserve the overload discriminator the edge already carries. It is either the legacy
        // all-digits arity (__2) or a signature token (__NUM_VARCHAR2, __FUNCTION_NUM) — the
        // latter never contains "__" because FlowNode.sanitizeType collapses runs of "_".
        String arity = "";
        int us = nodeId.lastIndexOf("__");
        if (us > 0 && us + 2 < nodeId.length()) {
            String tail = nodeId.substring(us + 2);
            if (!tail.contains("__") && tail.matches("[A-Z0-9_]+")) {
                arity = nodeId.substring(us);
            }
        }
        List<FlowNode> candidates = bySchemalessKey.get(normalizeIdPart(pkg, object) + arity);
        if (candidates == null || candidates.size() != 1) return null;
        return candidates.get(0);
    }

    /** Same normalization {@code FlowNode.buildShortId}/{@code DependencyCrawler.buildNodeId} use. */
    private static String normalizeIdPart(String pkg, String object) {
        StringBuilder sb = new StringBuilder();
        if (pkg != null && !pkg.isEmpty()) sb.append(pkg);
        if (object != null && !object.isEmpty()) {
            if (sb.length() > 0) sb.append("$");
            sb.append(object);
        }
        return sb.toString().toUpperCase().replaceAll("[^A-Z0-9_$]", "_");
    }

    /**
     * Whether to write {@code flow.json}.
     *
     * <p>It is a merged view that re-embeds every chunk, every edge file and {@code tables.json} —
     * measured at 3.3 MB for a 52-node analysis and extrapolating to roughly 300 MB at 5,000 nodes.
     * <b>Nothing reads it.</b> The UI reads {@code api/**}, and the call graph reads
     * {@code edges.json}; a repo-wide search finds no reader for this file.
     *
     * <p>Off by default because writing an artifact no consumer reads is pure cost — and over a
     * ~0.5 MB/s link to object storage it is minutes per analysis. Set
     * {@code -Dplsql.write-flow-json=true} to restore it without a code change, which is the
     * revert path.
     */
    private static boolean writeFlowJsonEnabled() {
        return Boolean.parseBoolean(System.getProperty("plsql.write-flow-json", "false"));
    }

    /**
     * MID-026 (§177) — {@code nodeMetrics} is {@code DependencyCrawler.computeNodeMetrics}'s own
     * explicit output (statementSummary/callCount per node id), replacing the in-place
     * {@code FlowNode} mutation this method used to rely on implicitly via {@code result
     * .getFlowNodes()} already carrying those fields by the time this ran.
     */
    public void writeFinalResult(FlowResult result,
                                  Map<String, DependencyCrawler.NodeMetrics> nodeMetrics) throws IOException {
        if (writeFlowJsonEnabled()) {
            ArtifactKey flowJson = buildKey.resolve("flow.json");
            writeJson(flowJson, result);
            System.err.println("[ChunkedFlowWriter] Final flow.json written with metrics: "
                    + flowJson);
        }

        // api/** is what the UI actually reads, so this must happen either way.
        writeApiFiles(result, nodeMetrics);
    }

    // ═══════════════════════════════════════════════════════════════
    // API-ready output: index.json, nodes/{id}.json, tables/, call_graph.json
    // Designed for on-demand loading by a UI
    // ═══════════════════════════════════════════════════════════════


    /**
     * A short, evidence-based label for what an access did.
     *
     * <p>Answers "plain select or join select or something else" without inventing anything: every
     * branch is backed by a field already on the operation. Returns {@code null} when there is no
     * evidence, so a row says nothing rather than guessing — the distinction this whole task series
     * is about.
     */
    private static String accessKind(TableOperationInfo t) {
        String op = t.getOperation() == null ? "" : t.getOperation().toUpperCase(java.util.Locale.ROOT);
        boolean joined = t.getJoins() != null && !t.getJoins().isEmpty();
        boolean sub = t.getSubqueryContext() != null && !t.getSubqueryContext().isBlank();

        if (t.getDdl() != null && !t.getDdl().isEmpty()) return "DDL";
        if ("SELECT".equals(op)) {
            if (joined) return "JOIN_READ";
            if (sub) return "SUBQUERY_READ";
            // An aggregate-only read has no plain column and no predicate; saying so is more useful
            // than a blank cell, and it is the dominant shape in the PostgreSQL workload measured.
            boolean anyCols = t.getColumnUsages() != null && !t.getColumnUsages().isEmpty();
            boolean anyPreds = t.getWhereFilters() != null && !t.getWhereFilters().isEmpty();
            String sql = t.getSqlText() == null ? "" : t.getSqlText().toUpperCase(java.util.Locale.ROOT);
            if (!anyCols && !anyPreds
                    && (sql.contains("COUNT(") || sql.contains("SUM(") || sql.contains("MAX(")
                        || sql.contains("MIN(") || sql.contains("AVG("))) {
                return "AGGREGATE_READ";
            }
            return "READ";
        }
        if ("INSERT".equals(op) || "UPDATE".equals(op) || "DELETE".equals(op) || "MERGE".equals(op)) {
            return sub ? "SUBQUERY_WRITE" : "WRITE";
        }
        return op.isEmpty() ? null : op;
    }

    /**
     * One row per statement, flattened for cross-analysis search.
     *
     * <h2>Why this exists</h2>
     *
     * The unit of the question users actually ask — <i>"is there an UPDATE on this table for this
     * column in this procedure"</i> — is a statement together with the columns it touches. Nothing
     * served carried that. {@code api/tables/index.json} carries {@code usedBy[].operations} as
     * {@code {OP: [lines]}}, which loses the column attribution entirely, and the per-node detail
     * files group operations by table for display. Answering the question meant opening procedures
     * one at a time and reading them.
     *
     * <h2>Why here</h2>
     *
     * Written at crawl time because the data is already in memory: this is a flatten of
     * {@code FlowNode.tableOperations}, not a re-parse. Deriving it later would mean reading every
     * node file back and re-deciding what a statement was.
     *
     * <h2>Size</h2>
     *
     * Measured before being built, over the real analyses on this machine: 5,015 rows / 2.3 MB for
     * the largest (MySQL), 2,662 rows / 0.5 MB for PostgreSQL. Well inside the 16 MB artifact rule,
     * so this is written whole rather than chunked. {@code sqlText} is capped per row because a
     * single pathological statement should not be able to dominate the document.
     *
     * <h2>Honest degradation</h2>
     *
     * Analyses parsed before column attribution existed have operations with no
     * {@code columnUsages}. Those rows are still written — the statement, table, operation and line
     * are real and searchable — with an empty {@code columns}. Omitting them would silently shrink
     * the answer to "no UPDATE on that table", which is worse than an answer with a missing column
     * list. {@code columnsKnown} records which case a row is in, so the UI can distinguish
     * "no columns" from "columns not captured".
     */
    /**
     * MID-026 — the second consumer converted to {@link LightNode} (after {@code writeTableIndex}).
     * Read directly before converting, not assumed from an earlier pass's enumeration: this method
     * touches only {@code tableOperations} plus the four identity fields {@code safeNodeId}/
     * {@code buildSourceFileName} already need — every field it reads was already on {@code
     * LightNode}, so no extension was needed this time (unlike {@code writeTableIndex}'s own
     * {@code cursors} finding).
     */
    private int writeStatementIndex(ArtifactKey apiDir, List<LightNode> nodes) throws IOException {
        final int MAX_SQL_TEXT = 2000;

        List<Map<String, Object>> rows = new ArrayList<>();
        int withColumns = 0;
        for (LightNode node : nodes) {
            if (node.getTableOperations() == null) continue;
            String nodeId = safeNodeId(node);
            String nodeName = node.getObjectName();
            String sourceFile = buildSourceFileName(node);

            for (TableOperationInfo t : node.getTableOperations()) {
                if (t.getTableName() == null) continue;

                List<String> cols = new ArrayList<>();
                List<Map<String, Object>> colDetail = new ArrayList<>();
                boolean columnsKnown = t.getColumnUsages() != null;
                if (columnsKnown) {
                    for (com.plsqlanalyzer.parser.model.ColumnUsage u : t.getColumnUsages()) {
                        // Sep05-10: a select item that is not a plain column — COUNT(*), NVL(a,b),
                        // a literal — has no column name, and dropping it here is what made
                        // `SELECT COUNT(*) INTO v_n FROM t` render a wholly empty row. It is the
                        // dominant shape in PostgreSQL: 1,303 of 1,553 empty rows measured.
                        //
                        // It is kept as *detail* and deliberately NOT added to `cols`. `cols` means
                        // "columns of this table that are referenced"; COUNT(*) is not one, and
                        // folding it in would make a column filter match a non-column and would
                        // undo Sep05-08 — the whole point of which is that the screen must
                        // distinguish "no column is referenced" from "we failed to capture it".
                        boolean named = u.getColumnName() != null && !u.getColumnName().isBlank();
                        boolean hasExpr = u.getExpression() != null && !u.getExpression().isBlank();
                        boolean hasInto = u.getIntoVariable() != null && !u.getIntoVariable().isBlank();
                        if (!named && !hasExpr && !hasInto) continue;
                        Map<String, Object> c = new LinkedHashMap<>();
                        if (named) {
                            cols.add(u.getColumnName().toUpperCase(java.util.Locale.ROOT));
                            c.put("name", u.getColumnName());
                        }
                        if (u.getUsage() != null) c.put("usage", u.getUsage());
                        if (u.getValueSource() != null) c.put("valueSource", u.getValueSource());
                        if (u.getExpression() != null) c.put("expression", u.getExpression());
                        // Sep05-04: the SELECT ... INTO target, captured all along and never emitted.
                        if (u.getIntoVariable() != null) c.put("into", u.getIntoVariable());
                        // Sep05-05: the alias. Without it `id` on a three-table join is
                        // unattributable on screen.
                        if (u.getQualifier() != null) c.put("qualifier", u.getQualifier());
                        if (u.getLineNumber() > 0) c.put("line", u.getLineNumber());
                        colDetail.add(c);
                    }
                }
                if (!cols.isEmpty()) withColumns++;

                Map<String, Object> r = new LinkedHashMap<>();
                r.put("nodeId", nodeId);
                r.put("nodeName", nodeName != null ? nodeName : "");
                if (sourceFile != null) r.put("sourceFile", sourceFile);
                r.put("schema", t.getSchema() != null ? t.getSchema() : "");
                r.put("tableName", t.getTableName());
                if (t.getObjectType() != null) r.put("objectType", t.getObjectType());
                r.put("operation", t.getOperation() != null ? t.getOperation() : "");
                r.put("line", t.getLine());
                String sql = t.getSqlText();
                if (sql != null && !sql.isBlank()) {
                    r.put("sqlText", sql.length() > MAX_SQL_TEXT ? sql.substring(0, MAX_SQL_TEXT) : sql);
                    r.put("sqlTruncated", sql.length() > MAX_SQL_TEXT);
                }
                // Sep05-02: the joins. Never emitted at all — measured 0 rows carrying join
                // detail across all five dialects, despite the visitors populating op.joins.
                if (t.getJoins() != null && !t.getJoins().isEmpty()) {
                    List<Map<String, Object>> js = new ArrayList<>();
                    for (com.plsql.parser.model.JoinInfo j : t.getJoins()) {
                        Map<String, Object> m = new LinkedHashMap<>();
                        if (j.getJoinType() != null) m.put("type", j.getJoinType());
                        if (j.getLeftTable() != null) m.put("left", j.getLeftTable());
                        if (j.getJoinedTable() != null) m.put("right", j.getJoinedTable());
                        if (j.getCondition() != null) m.put("on", j.getCondition());
                        // The aliases are what disambiguate two joins on the same pair of tables.
                        if (j.getLeftTableAlias() != null) m.put("leftAlias", j.getLeftTableAlias());
                        if (j.getJoinedTableAlias() != null) m.put("rightAlias", j.getJoinedTableAlias());
                        if (j.getLine() > 0) m.put("line", j.getLine());
                        if (!m.isEmpty()) js.add(m);
                    }
                    if (!js.isEmpty()) r.put("joins", js);
                }

                // Sep05-03: what KIND of access this was.
                //
                // "Is this a plain select, or a select feeding a join?" could not be answered: a
                // read inside a three-table join looked identical to a standalone read. Derived
                // from evidence already on the operation rather than guessed — and only when there
                // IS evidence, so an unparsed statement claims nothing.
                String kind = accessKind(t);
                if (kind != null) r.put("accessKind", kind);

                // Sep05-09: DDL column definitions.
                //
                // Captured on the operation all along and never emitted, so a
                // `CREATE TABLE #stage6(ProductId INT, Qty DECIMAL(18,4))` reached the screen with
                // an empty column cell while naming its columns in the statement. This is a large
                // share of the SQL Server workload — of 675 statements that had SQL and produced
                // nothing, most were temp-table CREATE/DROP/TRUNCATE, which have no predicate and
                // no read/write usage but do define columns.
                if (t.getDdl() != null && !t.getDdl().isEmpty()) {
                    List<Map<String, Object>> ddls = new ArrayList<>();
                    for (com.plsqlanalyzer.parser.model.DdlInfo dd : t.getDdl()) {
                        Map<String, Object> m = new LinkedHashMap<>();
                        if (dd.getAction() != null) m.put("action", dd.getAction());
                        if (dd.getObjectType() != null) m.put("objectType", dd.getObjectType());
                        if (dd.getObjectName() != null) m.put("objectName", dd.getObjectName());
                        if (dd.getColumns() != null && !dd.getColumns().isEmpty())
                            m.put("columns", dd.getColumns());
                        if (dd.getLineNumber() > 0) m.put("line", dd.getLineNumber());
                        if (!m.isEmpty()) ddls.add(m);
                    }
                    if (!ddls.isEmpty()) r.put("ddl", ddls);
                }
                r.put("columns", cols);
                if (!colDetail.isEmpty()) r.put("columnDetail", colDetail);
                r.put("columnsKnown", columnsKnown);
                // Three-way, because an empty column list has three different meanings and the UI
                // was showing one dash for all of them:
                //   null  — never attempted (no sqlText, or an analysis predating enrichment)
                //   false — ANSI parsing of this statement failed, so columns are UNKNOWN
                //   true  — parsed fine; an empty list really does mean no columns
                if (t.getColumnsParsed() != null) r.put("columnsParsed", t.getColumnsParsed());

                // Sep05-01/04/05: the predicate, and the column attributes that were being dropped.
                //
                // A predicate column never becomes a ColumnUsage, so without this the statements
                // index carried no record that a table was filtered or joined on at all — which is
                // the majority of what is worth knowing about a lookup table.
                if (t.getWhereFilters() != null && !t.getWhereFilters().isEmpty()) {
                    List<Map<String, Object>> preds = new ArrayList<>();
                    for (com.plsqlanalyzer.parser.model.WhereFilter w : t.getWhereFilters()) {
                        Map<String, Object> p = new LinkedHashMap<>();
                        if (w.getColumnName() != null) p.put("column", w.getColumnName());
                        if (w.getOperator() != null) p.put("operator", w.getOperator());
                        if (w.getValue() != null) p.put("value", w.getValue());
                        if (w.getLineNumber() > 0) p.put("line", w.getLineNumber());
                        if (!p.isEmpty()) preds.add(p);
                    }
                    if (!preds.isEmpty()) {
                        r.put("predicates", preds);
                        // "table" = the statement named one table, so the predicate is that
                        // table's. "statement" = a join, where WhereFilter carries no qualifier and
                        // the filter cannot be attributed to one side. The screen must not imply
                        // the former when it is the latter.
                        r.put("predicateScope", t.getPredicateScope() == null
                                ? "statement" : t.getPredicateScope());
                    }
                }
                rows.add(r);
            }
        }

        // Plan 1e — the row offsets are captured as this same document is written, so paging can
        // seek to row N instead of parsing the whole file. No second pass, no re-read.
        Map<String, Object> leading = new LinkedHashMap<>();
        leading.put("totalStatements", rows.size());
        leading.put("statementsWithColumns", withColumns);
        ArtifactKey docKey = apiDir.resolve("statements", "index.json");
        ArtifactKey offsetsKey = apiDir.resolve("statements", "index.offsets.json");
        RowArrayOffsetWriter.Result written =
                RowArrayOffsetWriter.write(store, docKey, offsetsKey, leading, "statements", rows);
        manifest.recordArtifact(docKey, written.docLength(), (long) written.rowCount());
        manifest.recordArtifact(offsetsKey, written.offsetsLength(), (long) written.rowCount());
        System.err.println("[ChunkedFlowWriter] statements/index.json: " + rows.size()
                + " rows, " + withColumns + " with column detail"
                + " (+index.offsets.json, " + written.offsetsLength() + " bytes)");
        return rows.size();
    }

    @SuppressWarnings("unchecked")
    private void writeApiFiles(FlowResult result,
                                Map<String, DependencyCrawler.NodeMetrics> nodeMetrics) throws IOException {
        ArtifactKey apiDir = buildKey.resolve("api");
        ArtifactKey nodesDir = apiDir.resolve("nodes");

        List<FlowEdge> edges = result.getCallGraph();

        // MID-026 (final piece) — `nodes` is no longer `result.getFlowNodes()` (the eager full list
        // `mergeChunks` used to retain). Every cross-node lookup in this method (edge resolution,
        // subtree DFS, cycle detection, subtree table aggregation) only ever reads a node's own
        // {@code tableOperations}/{@code linesOfCode}/{@code packageName}/{@code objectName}/{@code
        // schema} — confirmed by reading each call site directly — so those run off the lightweight
        // projection, read once here via `buildLightNodes()` and then discarded from disk (each
        // chunk's full body is read again, one at a time, only where genuinely needed: the two
        // per-node loops below and the transaction-aggregation loop, via `readNodeChunk()`, §174).
        Map<String, ArtifactKey> chunkIndex = buildChunkIndex();
        List<LightNode> rawLightNodes = buildLightNodes();

        // Deduplicate short IDs: if two nodes share SCHEMA$PKG$OBJ, append $2, $3... — same
        // algorithm as before, now computed over the lightweight list (a list, not a map, so a
        // genuine raw-id collision is still counted correctly here even though `buildChunkIndex`'s
        // own map could only resolve one of them back to a chunk — see `buildLightNodes`'s own doc
        // comment). Produces new `LightNode` copies with the resolved id instead of mutating
        // `FlowNode` in place, since records are immutable and nothing here holds a `FlowNode`
        // anyway until a per-node loop below re-reads one from its chunk.
        Map<String, Integer> shortIdCount = new LinkedHashMap<>();
        for (LightNode n : rawLightNodes) {
            shortIdCount.merge(safeNodeId(n), 1, Integer::sum);
        }
        Map<String, Integer> shortIdSeq = new HashMap<>();
        record ResolvedNode(String originalId, LightNode light) {}
        List<ResolvedNode> resolvedNodes = new ArrayList<>(rawLightNodes.size());
        for (LightNode n : rawLightNodes) {
            String shortId = safeNodeId(n);
            String finalId = shortId;
            if (shortIdCount.get(shortId) > 1) {
                int seq = shortIdSeq.merge(shortId, 1, Integer::sum);
                if (seq > 1) finalId = shortId + "$" + seq;
            }
            LightNode resolved = finalId.equals(n.getNodeId()) ? n : withNodeId(n, finalId);
            resolvedNodes.add(new ResolvedNode(shortId, resolved));
        }
        List<LightNode> lightNodes = new ArrayList<>(resolvedNodes.size());
        for (ResolvedNode rn : resolvedNodes) lightNodes.add(rn.light());

        Map<String, LightNode> nodeIndex = new LinkedHashMap<>();
        for (LightNode n : lightNodes) {
            nodeIndex.put(safeNodeId(n), n);
        }

        // Build parent map from edges: nodeId -> set of callerNodeIds
        Map<String, String> firstCallerMap = new LinkedHashMap<>();
        // Build adjacency map: parentNodeId -> set of childNodeIds (for subtree computation)
        Map<String, Set<String>> childrenMap = new LinkedHashMap<>();
        for (FlowEdge e : edges) {
            String fromPartialId = e.getFromNodeId();
            String toPartialId = e.getToNodeId();
            if (toPartialId == null) continue;

            // Resolve both IDs using prefix matching (edges have partial IDs without params)
            String fromId = fromPartialId != null ? findMatchingNodeId(fromPartialId, nodeIndex) : null;
            String toId = findMatchingNodeId(toPartialId, nodeIndex);
            if (toId == null) continue;

            if (!firstCallerMap.containsKey(toId) && fromId != null) {
                firstCallerMap.put(toId, fromId);
            }

            // Populate children adjacency
            if (fromId != null) {
                childrenMap.computeIfAbsent(fromId, k -> new LinkedHashSet<>()).add(toId);
            }
        }

        // Precompute subtree descendants for each node (DFS)
        Map<String, Set<String>> subtreeCache = new LinkedHashMap<>();
        for (String nodeId : nodeIndex.keySet()) {
            if (!subtreeCache.containsKey(nodeId)) {
                computeSubtreeDescendants(nodeId, childrenMap, subtreeCache);
            }
        }

        // ── 1. index.json: lightweight tree for sidebar/tree view ──
        // MID-026 (final piece) — streamed one node at a time via `readNodeChunk`, each fresh
        // instance discarded after this loop's own use of it, rather than iterating a resident
        // `List<FlowNode>`. `node.setNodeId(rn.light().getNodeId())` applies the short-id dedup
        // resolution to this loop's own throwaway instance — safe because it is never shared with
        // any other loop or held past this iteration, unlike the old shared-mutation approach.
        List<Map<String, Object>> indexNodes = new ArrayList<>();
        for (ResolvedNode rn : resolvedNodes) {
            FlowNode node = readNodeChunk(rn.originalId(), chunkIndex);
            if (node == null) continue; // dangling chunk reference, recorded nowhere else either
            node.setNodeId(rn.light().getNodeId());
            Map<String, Object> idx = new LinkedHashMap<>();
            String nodeId = safeNodeId(node);
            String pkg = node.getPackageName() != null ? node.getPackageName() : "";
            String obj = node.getObjectName() != null ? node.getObjectName() : "";
            String fullName = pkg.isEmpty() ? obj : pkg + "." + obj;

            idx.put("nodeId", nodeId);
            idx.put("order", node.getOrder());
            idx.put("depth", node.getDepth());
            idx.put("schema", node.getSchema());
            idx.put("packageName", pkg.isEmpty() ? null : pkg);
            idx.put("objectName", obj);
            idx.put("name", fullName);
            idx.put("objectType", node.getObjectType());
            // Task #29: nested/local subprogram badge for the procedures facet. Additive — only
            // emitted for nested nodes, so existing (non-nested) index.json is byte-identical.
            if (node.isNested()) idx.put("nested", true);
            if (node.getScopeDepth() > 0) idx.put("scopeDepth", node.getScopeDepth());
            // Trigger metadata in the index summary so the UI can associate a trigger with its
            // OWN table (otherwise every trigger node shows under every table/view).
            if (node.getTriggerTable() != null)  idx.put("triggerTable", node.getTriggerTable());
            if (node.getTriggerEvent() != null)  idx.put("triggerEvent", node.getTriggerEvent());
            if (node.getTriggerTiming() != null) idx.put("triggerTiming", node.getTriggerTiming());
            if (node.getTriggerWhenClause() != null) idx.put("triggerWhenClause", node.getTriggerWhenClause());
            if (node.getTriggerLevel() != null) idx.put("triggerLevel", node.getTriggerLevel());
            if (node.getTriggerBody() != null) idx.put("triggerBody", node.getTriggerBody());
            if (node.getTriggerType() != null) idx.put("triggerType", node.getTriggerType());
            if (node.getTriggerStatus() != null) idx.put("triggerStatus", node.getTriggerStatus());
            idx.put("lineStart", node.getLineStart());
            idx.put("lineEnd", node.getLineEnd());
            idx.put("linesOfCode", node.getLinesOfCode());
            // Tasks #26/#27: the procedures facet reads these off the index row (returnsResultSet /
            // resultSets for the "returns data" filter; returnType/returnsColumns for the TVF shape).
            if (node.isReturnsResultSet()) {
                idx.put("returnsResultSet", true);
                idx.put("resultSets", node.getResultSets());
            }
            if (node.getReturnType() != null) idx.put("returnType", node.getReturnType());
            if (node.getReturnsColumns() != null && !node.getReturnsColumns().isEmpty()) {
                idx.put("returnsColumns", node.getReturnsColumns());
            }
            idx.put("readable", node.isReadable());
            if (node.getMessage() != null) idx.put("message", node.getMessage());
            idx.put("parentNodeId", firstCallerMap.get(nodeId));
            idx.put("callCount", nodeMetrics.getOrDefault(nodeId,
                    EMPTY_NODE_METRICS).callCount());

            // Counts only — details in nodes/{id}.json
            int tableCount = node.getTableOperations() != null ? countDistinct(node.getTableOperations()) : 0;
            // Counted with groupCalls()'s own key (below), so counts.callsOut equals the node
            // detail's calls[] length — overloads split by arity instead of merging, and the
            // schema/package qualification matches. The old name-only key under-counted every
            // overloaded target (ERP_PKG_PLANNER.COMPUTE_TOTAL (1 args)/(2 args) collapsed to one),
            // making complexity.callsOut 1 short of node.calls for the affected procedures.
            int[] callCounts = countCallGroups(node.getCalls(), node.getPackageName(), node.getSchema());
            int callCountOut = callCounts[0];
            int internalCallCount = callCounts[1];
            int externalCallCount = callCounts[2];
            int varCount = node.getLocalVariables() != null ? node.getLocalVariables().size() : 0;
            int cursorCount = node.getCursors() != null ? node.getCursors().size() : 0;
            int paramCount = node.getParameters() != null ? node.getParameters().size() : 0;
            int exHandlerCount = node.getExceptionHandlers() != null ? node.getExceptionHandlers().size() : 0;
            int sequenceCount = 0;
            if (node.getCalls() != null) {
                Set<String> seqNames = new HashSet<>();
                for (CallInfo c : node.getCalls()) {
                    String cName = c.getName();
                    if (cName != null && ("NEXTVAL".equals(cName) || "CURRVAL".equals(cName))
                            && c.getPackageName() != null) {
                        seqNames.add(c.getPackageName());
                    }
                }
                sequenceCount = seqNames.size();
            }

            Map<String, Object> counts = new LinkedHashMap<>();
            counts.put("tables", tableCount);
            counts.put("callsOut", callCountOut);
            counts.put("internalCalls", internalCallCount);
            counts.put("externalCalls", externalCallCount);
            counts.put("variables", varCount);
            counts.put("cursors", cursorCount);
            counts.put("parameters", paramCount);
            counts.put("exceptionHandlers", exHandlerCount);
            counts.put("sequences", sequenceCount);
            counts.put("statements", nodeMetrics.getOrDefault(nodeId,
                    EMPTY_NODE_METRICS).statementSummary());

            // Subtree counts
            Set<String> descendants = subtreeCache.getOrDefault(nodeId, Collections.emptySet());
            Set<String> subtreeTableNames = new HashSet<>();
            // Count tables from this node
            if (node.getTableOperations() != null) {
                for (TableOperationInfo t : node.getTableOperations()) {
                    String key = tableKey(t);
                    if (key != null) subtreeTableNames.add(key);
                }
            }
            // Count tables from all descendants
            for (String descId : descendants) {
                LightNode descNode = nodeIndex.get(descId);
                if (descNode != null && descNode.getTableOperations() != null) {
                    for (TableOperationInfo t : descNode.getTableOperations()) {
                        String key = tableKey(t);
                        if (key != null) subtreeTableNames.add(key);
                    }
                }
            }
            counts.put("subtreeTablesCount", subtreeTableNames.size());
            counts.put("subtreeNodesCount", descendants.size());

            // Flow LOC: this node's LOC + all descendant LOC (full execution path)
            int flowLoc = node.getLinesOfCode();
            for (String descId : descendants) {
                LightNode descNode = nodeIndex.get(descId);
                if (descNode != null) flowLoc += descNode.getLinesOfCode();
            }
            counts.put("flowLinesOfCode", flowLoc);

            idx.put("counts", counts);

            // File references for on-demand loading
            String sourceFile = buildSourceFileName(node);
            if (sourceFile != null) idx.put("sourceFile", sourceFile);
            idx.put("detailFile", "nodes/" + safeFileName(nodeId) + ".json");

            indexNodes.add(idx);
        }

        // Compute total flow LOC from entry point's subtree
        int totalFlowLoc = 0;
        if (!lightNodes.isEmpty()) {
            String entryNodeId = lightNodes.get(0).getNodeId();
            Set<String> entryDesc = subtreeCache.getOrDefault(entryNodeId, Collections.emptySet());
            totalFlowLoc = lightNodes.get(0).getLinesOfCode();
            for (String descId : entryDesc) {
                LightNode descNode = nodeIndex.get(descId);
                if (descNode != null) totalFlowLoc += descNode.getLinesOfCode();
            }
        }

        // Detect cycles in call graph
        List<List<String>> cycles = detectCycles(childrenMap, nodeIndex);

        // Build set of nodes involved in cycles for per-node marking
        Set<String> nodesInCycles = new LinkedHashSet<>();
        for (List<String> cycle : cycles) {
            nodesInCycles.addAll(cycle);
        }

        // Mark each index node with cycle involvement
        for (Map<String, Object> idx : indexNodes) {
            String nid = (String) idx.get("nodeId");
            @SuppressWarnings("unchecked")
            Map<String, Object> counts = (Map<String, Object>) idx.get("counts");
            counts.put("inCycle", nodesInCycles.contains(nid));
        }

        Map<String, Object> indexRoot = new LinkedHashMap<>();
        indexRoot.put("entryPoint", result.getEntryPoint());
        indexRoot.put("entrySchema", result.getEntrySchema());
        indexRoot.put("totalNodes", lightNodes.size());
        indexRoot.put("maxDepth", result.getMaxDepthReached());
        indexRoot.put("totalLinesOfCode", result.getTotalLinesOfCode());
        indexRoot.put("totalFlowLinesOfCode", totalFlowLoc);
        indexRoot.put("totalTables", result.getAllTables() != null ? result.getAllTables().size() : 0);
        indexRoot.put("totalEdges", edges.size());
        // Deduplicated from->to pairs (what the UI should show as "Call Edges")
        Set<String> uniqueEdgeKeys = new java.util.LinkedHashSet<>();
        for (FlowEdge e : edges) {
            // Match call_graph.json keying: schema-qualified for standalone objects so
            // same-named cross-schema routines count as distinct edges.
            String from = e.getFromPackage() != null
                    ? e.getFromPackage() + "." + e.getFromObject()
                    : (e.getFromSchema() != null && !e.getFromSchema().isEmpty() ? e.getFromSchema() + "$" + e.getFromObject() : e.getFromObject());
            String to = e.getToPackage() != null
                    ? e.getToPackage() + "." + e.getToObject()
                    : (e.getToSchema() != null && !e.getToSchema().isEmpty() ? e.getToSchema() + "$" + e.getToObject() : e.getToObject());
            // Include nodeIds so overloaded targets (…__<arity>) count distinctly; deterministic
            // for non-overloads so the count is unchanged there.
            uniqueEdgeKeys.add(from + " -> " + to + "|" + e.getFromNodeId() + "->" + e.getToNodeId());
        }
        indexRoot.put("uniqueEdges", uniqueEdgeKeys.size());
        indexRoot.put("totalCycles", cycles.size());
        Map<String, Object> coverage = columnCoverage(result.getAllTables());
        indexRoot.put("columnCoverage", coverage);
        log.info("column coverage [{}] {}/{} tables with column detail ({}%), {} column rows, {} DDL rows",
                result.getEntryPoint(), coverage.get("tablesWithColumnDetail"), coverage.get("tables"),
                coverage.get("pctTablesWithColumnDetail"), coverage.get("columnRows"),
                coverage.get("ddlRows"));

        // Flow-wide transaction scope aggregate (Summary tile + Export) + full statement
        // list (Details "Transactions" sub-tab).
        int txCommits = 0, txRollbacks = 0, txSavepoints = 0, txAutonomousProcs = 0;
        List<Map<String, Object>> txnStatements = new ArrayList<>();
        for (ResolvedNode rn : resolvedNodes) {
            FlowNode fn = readNodeChunk(rn.originalId(), chunkIndex);
            if (fn == null) continue;
            fn.setNodeId(rn.light().getNodeId());
            if (fn.isAutonomousTransaction()) txAutonomousProcs++;
            if (fn.getStatements() == null) continue;
            for (StatementInfo s : fn.getStatements()) {
                String t = s.getType();
                if (t == null) continue;
                String T = t.toUpperCase();
                if (!T.equals("COMMIT") && !T.equals("ROLLBACK") && !T.equals("SAVEPOINT")) continue;
                if (T.equals("COMMIT")) txCommits++;
                else if (T.equals("ROLLBACK")) txRollbacks++;
                else txSavepoints++;
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("nodeId", fn.getNodeId());
                m.put("proc", fn.getObjectName());
                m.put("schema", fn.getSchema());
                m.put("type", T);
                m.put("line", s.getLine());
                if (s.getSavepointName() != null && !s.getSavepointName().isBlank()) m.put("name", s.getSavepointName());
                m.put("autonomous", fn.isAutonomousTransaction());
                txnStatements.add(m);
            }
        }
        Map<String, Object> txnSummary = new LinkedHashMap<>();
        txnSummary.put("commits", txCommits);
        txnSummary.put("rollbacks", txRollbacks);
        txnSummary.put("savepoints", txSavepoints);
        txnSummary.put("autonomousProcs", txAutonomousProcs);
        indexRoot.put("transactionSummary", txnSummary);
        indexRoot.put("transactionStatements", txnStatements);
        if (!cycles.isEmpty()) {
            List<Map<String, Object>> cycleSummaries = new ArrayList<>();
            for (List<String> cycle : cycles) {
                Map<String, Object> cs = new LinkedHashMap<>();
                List<String> readableNames = new ArrayList<>();
                for (String cid : cycle) {
                    LightNode fn = nodeIndex.get(cid);
                    if (fn != null) {
                        String p = fn.getPackageName() != null ? fn.getPackageName() : "";
                        String o = fn.getObjectName() != null ? fn.getObjectName() : "";
                        readableNames.add(p.isEmpty() ? o : p + "." + o);
                    } else {
                        readableNames.add(cid);
                    }
                }
                cs.put("nodeIds", cycle);
                cs.put("path", readableNames);
                cs.put("length", cycle.size());
                cycleSummaries.add(cs);
            }
            indexRoot.put("cycles", cycleSummaries);
        }
        indexRoot.put("errors", result.getErrors());
        indexRoot.put("crawlTimeMs", result.getCrawlTimeMs());
        indexRoot.put("parseMs", result.getParseMs());
        indexRoot.put("discoverMs", result.getDiscoverMs());
        indexRoot.put("saveMs", result.getSaveMs());
        indexRoot.put("dbCallCount", result.getDbCallCount());

        // Overload visibility (W1): per-member count for every genuinely-overloaded routine —
        // the SAME package+object name split into >1 distinct node by its signature discriminator.
        // Derived from the resolved node list (the nodes ARE the overloads); empty when none, so
        // non-overloaded analyses gain only an empty map.
        Map<String, Integer> overloadCounts = new LinkedHashMap<>();
        for (Map<String, Object> idx : indexNodes) {
            Object objRaw = idx.get("objectName");
            if (objRaw == null) continue;
            String obj = objRaw.toString();
            if (obj.isEmpty()) continue;
            Object pkgRaw = idx.get("packageName");
            String pkg = pkgRaw != null ? pkgRaw.toString() : "";
            overloadCounts.merge(pkg.isEmpty() ? obj : pkg + "." + obj, 1, Integer::sum);
        }
        Map<String, Integer> overloads = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> e : overloadCounts.entrySet()) {
            if (e.getValue() > 1) overloads.put(e.getKey(), e.getValue());
        }
        indexRoot.put("overloads", overloads);

        indexRoot.put("nodes", indexNodes);

        ArtifactKey indexKey = apiDir.resolve("index.json");
        long indexBytes = writeJson(indexKey, indexRoot);
        manifest.recordArtifact(indexKey, indexBytes, (long) indexNodes.size());
        // The listing scalars, taken from the index root already in memory — no re-read. The history
        // listing can then answer from the manifest instead of reading index.json (and, for a large
        // analysis, streaming past its nodes[] array) once per analysis.
        manifest.setListingScalars(listingScalars(indexRoot));

        // ── 2. nodes/{nodeId}.json: full detail per node (on-demand) ──
        int nodeDetailCount = 0;
        for (ResolvedNode rn : resolvedNodes) {
            FlowNode node = readNodeChunk(rn.originalId(), chunkIndex);
            if (node == null) continue;
            node.setNodeId(rn.light().getNodeId());
            String nodeId = safeNodeId(node);
            Map<String, Object> detail = new LinkedHashMap<>();

            detail.put("nodeId", nodeId);
            String pkg = node.getPackageName() != null ? node.getPackageName() : "";
            String obj = node.getObjectName() != null ? node.getObjectName() : "";
            detail.put("name", pkg.isEmpty() ? obj : pkg + "." + obj);
            // packageName/objectName are emitted so the estate globals facet can key a package's
            // declaration list by its own package name (the display `name` is "PKG.SUB").
            if (!pkg.isEmpty()) detail.put("packageName", pkg);
            detail.put("objectName", obj);
            detail.put("schema", node.getSchema());
            detail.put("objectType", node.getObjectType());
            // Task #29: nested/local subprogram marker on the node detail too (additive; omitted
            // for ordinary nodes so existing node JSON is byte-identical).
            if (node.isNested()) detail.put("nested", true);
            if (node.getScopeDepth() > 0) detail.put("scopeDepth", node.getScopeDepth());
            detail.put("depth", node.getDepth());
            detail.put("lineStart", node.getLineStart());
            detail.put("lineEnd", node.getLineEnd());
            detail.put("linesOfCode", node.getLinesOfCode());

            // Tasks #26/#27 — result-set marker + TVF/SETOF return shape. Scalars ride Mongo's
            // Splitter into the parent document automatically; returnsColumns[] becomes its own
            // child collection there, the same way cursors[] does.
            if (node.isReturnsResultSet()) {
                detail.put("returnsResultSet", true);
                detail.put("resultSets", node.getResultSets());
            }
            if (node.getReturnType() != null) detail.put("returnType", node.getReturnType());
            if (node.getReturnsColumns() != null && !node.getReturnsColumns().isEmpty()) {
                detail.put("returnsColumns", node.getReturnsColumns());
            }

            // Trigger metadata (populated for TRIGGER nodes)
            if (node.getTriggerTable() != null)  detail.put("triggerTable", node.getTriggerTable());
            if (node.getTriggerEvent() != null)  detail.put("triggerEvent", node.getTriggerEvent());
            if (node.getTriggerTiming() != null) detail.put("triggerTiming", node.getTriggerTiming());
            if (node.getTriggerWhenClause() != null) detail.put("triggerWhenClause", node.getTriggerWhenClause());
            if (node.getTriggerLevel() != null) detail.put("triggerLevel", node.getTriggerLevel());
            if (node.getTriggerBody() != null) detail.put("triggerBody", node.getTriggerBody());
            // Catalog-only metadata (ALL_TRIGGERS TRIGGER_TYPE / STATUS) on a catalog-discovered
            // trigger node — additive, null for every non-catalog trigger node.
            if (node.getTriggerType() != null) detail.put("triggerType", node.getTriggerType());
            if (node.getTriggerStatus() != null) detail.put("triggerStatus", node.getTriggerStatus());

            // Package-global DECLARATIONS (PACKAGE_BODY/PACKAGE_SPEC nodes only) — the estate
            // globals facet's declared variables (name/dataType/defaultValue/constant). Each
            // VariableInfo already carries exactly those fields.
            if (node.getPackageVariables() != null && !node.getPackageVariables().isEmpty()) {
                detail.put("packageGlobalDeclarations", node.getPackageVariables());
            }

            // Parameters
            if (node.getParameters() != null && !node.getParameters().isEmpty()) {
                List<Map<String, Object>> params = new ArrayList<>();
                for (ParameterInfo p : node.getParameters()) {
                    Map<String, Object> pm = new LinkedHashMap<>();
                    pm.put("name", p.getName());
                    pm.put("dataType", p.getDataType());
                    pm.put("direction", p.getDirection());
                    if (p.getDefaultValue() != null) pm.put("default", p.getDefaultValue());
                    pm.put("line", p.getLine());
                    params.add(pm);
                }
                detail.put("parameters", params);
            }

            // Variables — categorized
            if (node.getLocalVariables() != null && !node.getLocalVariables().isEmpty()) {
                detail.put("variables", categorizeVariables(node.getLocalVariables()));
            }

            // Tables — grouped by name with all operations and lines
            if (node.getTableOperations() != null && !node.getTableOperations().isEmpty()) {
                detail.put("tables", groupTableOps(node.getTableOperations()));
                detail.put("directTables", groupTableOps(node.getTableOperations()));
            }

            // Subtree tables — aggregate from this node + all descendants
            {
                Set<String> descendants = subtreeCache.getOrDefault(nodeId, Collections.emptySet());
                detail.put("subtreeTables", buildSubtreeTables(nodeId, descendants, nodeIndex));
            }

            // Cursors — enrich with OPEN/FETCH/CLOSE operations from statements
            if (node.getCursors() != null && !node.getCursors().isEmpty()) {
                // Build cursor name -> operations map from statements
                Map<String, List<Map<String, Object>>> cursorOpsFromStmts = new LinkedHashMap<>();
                if (node.getStatements() != null) {
                    for (StatementInfo s : node.getStatements()) {
                        String sType = s.getType();
                        if (sType == null) continue;
                        String cursorName = null;
                        String opType = null;
                        if ("OPEN_CURSOR".equals(sType) || "OPEN_FOR".equals(sType)) {
                            opType = "OPEN";
                            cursorName = extractCursorNameFromSql(s.getSqlText());
                        } else if ("FETCH".equals(sType) || "BULK_COLLECT".equals(sType)) {
                            opType = sType;
                            cursorName = extractCursorNameFromSql(s.getSqlText());
                        } else if ("CLOSE_CURSOR".equals(sType)) {
                            opType = "CLOSE";
                            cursorName = extractCursorNameFromSql(s.getSqlText());
                        }
                        if (cursorName != null && opType != null) {
                            Map<String, Object> opInfo = new LinkedHashMap<>();
                            opInfo.put("operation", opType);
                            opInfo.put("line", s.getLine());
                            cursorOpsFromStmts.computeIfAbsent(cursorName.toUpperCase(), k -> new ArrayList<>()).add(opInfo);
                        }
                    }
                }

                List<Map<String, Object>> cursors = new ArrayList<>();
                for (CursorInfo c : node.getCursors()) {
                    Map<String, Object> ci = new LinkedHashMap<>();
                    ci.put("name", c.getName());
                    ci.put("line", c.getLine());
                    ci.put("lineEnd", c.getLineEnd());
                    ci.put("forLoop", c.isForLoop());
                    ci.put("refCursor", c.isRefCursor());
                    // Task #26: the "returns data" marker the cursors facet surfaces. It is a
                    // property of the owning node (a cursor lives in a routine that may return
                    // result sets), emitted on each cursor row so the facet can show/filter it.
                    ci.put("returnsData", node.isReturnsResultSet());
                    ci.put("resultSets", node.getResultSets());
                    if (c.getQuery() != null) ci.put("query", c.getQuery());
                    if (c.getTables() != null) ci.put("tables", c.getTables());
                    // Attach post-DECLARE lifecycle operations (OPEN/FETCH/CLOSE/DEALLOCATE).
                    // Prefer the dialect parser's own CursorInfo.stmtOperations when it already
                    // carried them (e.g. SQL Server's visitor matches OPEN/FETCH/CLOSE/DEALLOCATE
                    // to the declaring cursor by name directly, including DEALLOCATE, which the
                    // statement-derived fallback below has no vocabulary for at all). Fall back to
                    // deriving from node.getStatements() only when the parser did not supply them,
                    // which is the existing Oracle PL/SQL path (OPEN_CURSOR/FETCH/CLOSE_CURSOR).
                    String cName = c.getName() != null ? c.getName().toUpperCase() : "";
                    List<Map<String, Object>> stmtOps;
                    if (c.getStmtOperations() != null && !c.getStmtOperations().isEmpty()) {
                        stmtOps = new ArrayList<>();
                        for (CursorInfo.StmtOp op : c.getStmtOperations()) {
                            Map<String, Object> opInfo = new LinkedHashMap<>();
                            opInfo.put("operation", op.getOperation());
                            opInfo.put("line", op.getLine());
                            stmtOps.add(opInfo);
                        }
                    } else {
                        stmtOps = cursorOpsFromStmts.get(cName);
                    }
                    if (stmtOps != null && !stmtOps.isEmpty()) {
                        // A cursor's lifecycle list is a SEQUENCE (OPEN → FETCH… → CLOSE), so it
                        // is emitted in true source order regardless of how it was collected:
                        // the statement-derived fallback groups by cursor name over a statement
                        // list that, for a package-as-entry-point node, used to arrive with the
                        // package initialization block's statements in front (see
                        // DependencyCrawler's aggregation), and a dialect's own stmtOperations is
                        // whatever order that visitor recorded. Stable sort — ties keep it.
                        stmtOps = new ArrayList<>(stmtOps);
                        stmtOps.sort(Comparator.comparingInt(o -> ((Number) o.get("line")).intValue()));
                        ci.put("stmtOperations", stmtOps);
                    }
                    cursors.add(ci);
                }
                detail.put("cursors", cursors);
            }

            // Sequences — extracted from calls with NEXTVAL/CURRVAL
            if (node.getCalls() != null && !node.getCalls().isEmpty()) {
                List<Map<String, Object>> seqList = extractSequences(node.getCalls());
                if (!seqList.isEmpty()) {
                    detail.put("sequences", seqList);
                }
            }

            // Calls — grouped by target (skip builtins)
            if (node.getCalls() != null && !node.getCalls().isEmpty()) {
                detail.put("calls", groupCalls(node.getCalls(), node.getPackageName(), node.getSchema()));
            }

            // Dynamic SQL
            if (node.getDynamicSql() != null && !node.getDynamicSql().isEmpty()) {
                List<Map<String, Object>> dynSql = new ArrayList<>();
                for (DynamicSqlInfo ds : node.getDynamicSql()) {
                    Map<String, Object> dm = new LinkedHashMap<>();
                    dm.put("type", ds.getType());
                    dm.put("line", ds.getLine());
                    dm.put("sqlExpression", ds.getSqlExpression());
                    if (ds.getUsingVariables() != null && !ds.getUsingVariables().isEmpty())
                        dm.put("usingVariables", ds.getUsingVariables());
                    dynSql.add(dm);
                }
                detail.put("dynamicSql", dynSql);
            }

            // Exception handlers
            if (node.getExceptionHandlers() != null && !node.getExceptionHandlers().isEmpty()) {
                List<Map<String, Object>> exHandlers = new ArrayList<>();
                for (ExceptionHandlerInfo eh : node.getExceptionHandlers()) {
                    Map<String, Object> em = new LinkedHashMap<>();
                    if (eh.getExceptionName() != null) em.put("exception", eh.getExceptionName());
                    em.put("line", eh.getLine());
                    em.put("lineEnd", eh.getLineEnd());
                    em.put("statementsCount", eh.getStatementsCount());
                    exHandlers.add(em);
                }
                detail.put("exceptionHandlers", exHandlers);
            }

            // IDENTITY columns (SQL Server)
            if (node.getIdentityColumns() != null && !node.getIdentityColumns().isEmpty()) {
                List<Map<String, Object>> idCols = new ArrayList<>();
                for (IdentityColumnInfo ic : node.getIdentityColumns()) {
                    Map<String, Object> im = new LinkedHashMap<>();
                    im.put("schema", ic.getSchema());
                    im.put("tableName", ic.getTableName());
                    im.put("columnName", ic.getColumnName());
                    im.put("seed", ic.getSeed());
                    im.put("increment", ic.getIncrement());
                    im.put("line", ic.getLine());
                    idCols.add(im);
                }
                detail.put("identityColumns", idCols);
            }

            // Declared FOREIGN KEY relationships. Written as explicit maps rather than serialized
            // POJOs so the key names here are the contract the Mongo mapper and the UI read, and
            // do not silently change if the model class is refactored.
            if (node.getForeignKeys() != null && !node.getForeignKeys().isEmpty()) {
                List<Map<String, Object>> fks = new ArrayList<>();
                for (ForeignKeyInfo fk : node.getForeignKeys()) {
                    Map<String, Object> fm = new LinkedHashMap<>();
                    fm.put("constraintName", fk.getConstraintName());
                    fm.put("schema", fk.getSchema());
                    fm.put("tableName", fk.getTable());
                    fm.put("columns", fk.getColumns());
                    fm.put("referencedSchema", fk.getReferencedSchema());
                    fm.put("referencedTable", fk.getReferencedTable());
                    fm.put("referencedColumns", fk.getReferencedColumns());
                    fm.put("line", fk.getLine());
                    fks.add(fm);
                }
                detail.put("foreignKeys", fks);
            }

            // Statement summary
            detail.put("statementSummary", nodeMetrics.getOrDefault(nodeId,
                    EMPTY_NODE_METRICS).statementSummary());

            // Transaction scope — COMMIT / ROLLBACK / SAVEPOINT + autonomous transaction
            java.util.List<Map<String, Object>> txnStmts = new ArrayList<>();
            int commits = 0, rollbacks = 0, savepoints = 0;
            if (node.getStatements() != null) {
                for (StatementInfo s : node.getStatements()) {
                    String t = s.getType();
                    if (t == null) continue;
                    String T = t.toUpperCase();
                    if (T.equals("COMMIT") || T.equals("ROLLBACK") || T.equals("SAVEPOINT")) {
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("type", T);
                        m.put("line", s.getLine());
                        if (s.getSavepointName() != null && !s.getSavepointName().isBlank()) {
                            m.put("name", s.getSavepointName());
                        }
                        txnStmts.add(m);
                        if (T.equals("COMMIT")) commits++;
                        else if (T.equals("ROLLBACK")) rollbacks++;
                        else savepoints++;
                    }
                }
            }
            // transactionScope.statements is read as the order the COMMIT/ROLLBACK/SAVEPOINTs
            // execute in, so it is emitted in true source order rather than in the node's raw
            // statement-list order (see the cursor stmtOperations note above and DependencyCrawler's
            // package aggregation). Stable sort.
            txnStmts.sort(Comparator.comparingInt(m -> ((Number) m.get("line")).intValue()));
            if (!txnStmts.isEmpty() || node.isAutonomousTransaction()) {
                Map<String, Object> ts = new LinkedHashMap<>();
                ts.put("commits", commits);
                ts.put("rollbacks", rollbacks);
                ts.put("savepoints", savepoints);
                ts.put("autonomous", node.isAutonomousTransaction());
                ts.put("statements", txnStmts);
                detail.put("transactionScope", ts);
            }

            // External package var refs
            if (node.getExternalPackageVarRefs() != null && !node.getExternalPackageVarRefs().isEmpty()) {
                detail.put("externalPackageVarRefs", node.getExternalPackageVarRefs());
            }
            if (node.getExternalPackageVarRefDetails() != null && !node.getExternalPackageVarRefDetails().isEmpty()) {
                detail.put("externalPackageVarRefDetails", node.getExternalPackageVarRefDetails());
            }

            // Source file ref
            String sourceFile = buildSourceFileName(node);
            if (sourceFile != null) detail.put("sourceFile", sourceFile);

            // Package members (package-as-entry-point aggregation only — see FlowNode#getMembers).
            // Lets PackageTreeBuilder build the procedure drill-down tree straight from this one
            // node's own detail, instead of a separate crawl/output folder per procedure.
            if (node.getMembers() != null && !node.getMembers().isEmpty()) {
                List<Map<String, Object>> members = new ArrayList<>();
                for (SubprogramSummaryInfo m : node.getMembers()) {
                    Map<String, Object> mm = new LinkedHashMap<>();
                    mm.put("name", m.getName());
                    mm.put("type", m.getType());
                    mm.put("lineStart", m.getLineStart());
                    mm.put("lineEnd", m.getLineEnd());
                    // Task #29: nested member marker (omitted for ordinary members).
                    if (m.isNested()) mm.put("nested", true);
                    if (m.getScopeDepth() > 0) mm.put("scopeDepth", m.getScopeDepth());
                    members.add(mm);
                }
                detail.put("members", members);
            }

            String safeId = safeFileName(nodeId);
            writeJson(nodesDir.resolve(safeId + ".json"), detail);
            nodeDetailCount++;
        }

        // MID-026: `lightNodes`/`nodeIndex` are now built once, near the top of this method, and
        // reused by every cross-node lookup above as well as the 3 consumers below — no second
        // projection pass, no second id-keyed map. `mergeChunks` itself still builds the full
        // `nodes` list unchanged (this remains additive, not a replacement of that).

        // ── 3. tables/index.json: all tables, who uses them, which operations ──
        int tableCount = writeTableIndex(apiDir, lightNodes);

        // ── 3a. globals/declarations.json: parse-time package-global declarations ──
        // A dedicated artifact (not a per-node section) so the declarations survive a crawl that
        // produced no PACKAGE_BODY/PACKAGE_SPEC node — see recordPackageGlobals.
        writeGlobalsDeclarations(apiDir);

        // ── 3b. jobs/index.json: discovered scheduled jobs (job, schedule, enabled, target) ──
        int jobCount = writeJobsIndex(apiDir);

        // ── 4. call_graph.json: deduplicated edges for graph rendering ──
        writeCallGraph(apiDir, edges, cycles);

        // ── 5. node_id_map.json: shortId -> fullId lookup for resolving edge references ──
        // MID-026's 4th conversion: writeNodeIdMap only ever reads buildFullId()'s finished string
        // (see LightNode.fullId's own doc comment), so it takes the same shared `nodeIndex` this
        // method already built for cross-node lookups above, rather than a second, separately-built
        // map or the full FlowNode map.
        writeNodeIdMap(apiDir, nodeIndex);

        // ── 6. statements/index.json (+ index.offsets.json): one row per statement ──
        int statementCount = writeStatementIndex(apiDir, lightNodes);

        // ── 7. manifest.json: written LAST, after index.json (the staleness anchor) ──
        // The routing artifact every API can use to answer in one small read. Counts are the ones
        // this pass already computed; nothing is re-read to produce it.
        //
        // `nodes` counts the node detail files actually emitted, not `lightNodes.size()`: a dangling
        // chunk reference (a chunk with no id) is skipped by every consumer below, so counting the
        // raw projection would advertise one more node than the analysis serves. The two are equal
        // on every well-formed crawl; verifyManifestCounts fails the save if they ever diverge.
        manifest.setCount("nodes", nodeDetailCount);
        manifest.setCount("tables", tableCount);
        manifest.setCount("statements", statementCount);
        manifest.setCount("edges", edges.size());
        manifest.setCount("jobs", jobCount);
        // Facet presence the write pass can prove: a reader answers "no such facet" from the
        // manifest instead of probing the artifact. The estate facet sidecars are recorded by
        // AnalysisViewStore as they land (the writer does not emit them).
        manifest.setFacet("nodes", nodeDetailCount);
        manifest.setFacet("tables", tableCount);
        manifest.setFacet("statements", statementCount);
        manifest.setFacet("edges", edges.size());
        manifest.setFacet("jobs", jobCount);
        writeManifest();

        // Post-write consistency: the counts the manifest will serve must equal what this pass
        // actually emitted. A mismatch is a wiring bug that would serve a wrong number, so it fails
        // the save loudly instead (the whole point of the accuracy pass).
        verifyManifestCounts(nodeDetailCount, indexNodes.size(), tableCount, statementCount,
                jobCount, edges.size());

        System.err.println("[ChunkedFlowWriter] API files written: " + apiDir);
    }

    /**
     * The manifest's recorded counts must equal the artifacts this write pass emitted. Throws
     * {@link IllegalStateException} on any mismatch — the save fails rather than publishing a
     * manifest that would answer a count question wrongly.
     *
     * <p>Package-private and taking the actuals explicitly so the failure path is unit-testable
     * without provoking a real write-pass bug.
     *
     * @param nodeDetails  node detail files actually written
     * @param indexNodes   rows actually written into {@code api/index.json}'s {@code nodes[]}
     * @param tables       rows actually written into {@code api/tables/index.json}
     * @param statements   rows actually written into {@code api/statements/index.json}
     * @param jobs         rows actually written into {@code api/jobs/index.json}
     * @param edges        edges actually written into {@code api/call_graph.json}
     */
    void verifyManifestCounts(int nodeDetails, int indexNodes, int tables, int statements,
                              int jobs, int edges) {
        // The two node counts are the same artifact viewed twice (both loops skip the same dangling
        // chunks); a divergence means the index and the node detail set disagree.
        checkCount("nodes", manifest.count("nodes"), nodeDetails);
        if (indexNodes != nodeDetails) {
            throw new IllegalStateException("[ChunkedFlowWriter] manifest accuracy: "
                    + "api/index.json nodes=" + indexNodes + " but node detail files=" + nodeDetails);
        }
        checkCount("tables", manifest.count("tables"), tables);
        checkCount("statements", manifest.count("statements"), statements);
        checkCount("edges", manifest.count("edges"), edges);
        checkCount("jobs", manifest.count("jobs"), jobs);
    }

    private static void checkCount(String name, long recorded, long actual) {
        if (recorded != actual) {
            throw new IllegalStateException("[ChunkedFlowWriter] manifest accuracy: counts." + name
                    + "=" + recorded + " but the write emitted " + actual
                    + " — refusing to save a manifest that would serve a wrong count");
        }
    }

    /**
     * The listing scalars of an index root: every root scalar, plus the {@code errors} array, with
     * every nested object/array (above all {@code nodes[]}) skipped — the same projection the
     * history listing needs, so a manifest-served row is byte-identical to an index-served one.
     */
    private static Map<String, Object> listingScalars(Map<String, Object> indexRoot) {
        Map<String, Object> scalars = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : indexRoot.entrySet()) {
            Object v = e.getValue();
            if (v == null || v instanceof String || v instanceof Number || v instanceof Boolean) {
                scalars.put(e.getKey(), v);
            } else if ("errors".equals(e.getKey()) && v instanceof List) {
                scalars.put(e.getKey(), v);
            }
            // any other nested value is skipped, as the listing's index-header read skips it
        }
        return scalars;
    }

    /**
     * Writes {@code api/manifest.json} (plan 1f). Called at the end of the API write and again after
     * the resolver data, so the manifest always reflects the most recent write and stays the last
     * artifact committed.
     */
    public void writeManifest() throws IOException {
        manifest.writeTo(store, buildKey.resolve("api", "manifest.json"));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> categorizeVariables(List<VariableInfo> vars) {
        List<Map<String, Object>> typeRefs = new ArrayList<>();
        List<Map<String, Object>> rowtypeRefs = new ArrayList<>();
        List<Map<String, Object>> collections = new ArrayList<>();
        List<Map<String, Object>> plain = new ArrayList<>();

        for (VariableInfo v : vars) {
            String dt = v.getDataType() != null ? v.getDataType().toUpperCase() : "";
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("name", v.getName());
            info.put("dataType", v.getDataType());
            if (v.getDefaultValue() != null) info.put("default", v.getDefaultValue());
            info.put("line", v.getLine());
            info.put("constant", v.isConstant());

            if (dt.contains("%ROWTYPE")) {
                info.put("refTable", dt.replace("%ROWTYPE", "").trim());
                rowtypeRefs.add(info);
            } else if (dt.contains("%TYPE")) {
                String ref = dt.replace("%TYPE", "").trim();
                info.put("refColumn", ref);
                if (ref.contains(".")) {
                    String[] parts = ref.split("\\.", 2);
                    info.put("refTable", parts[0]);
                    info.put("refField", parts[1]);
                }
                typeRefs.add(info);
            } else if (dt.contains("TABLE OF") || dt.contains("VARRAY")
                    || dt.contains("ASSOCIATIVE") || dt.contains("INDEX BY")
                    || dt.contains("NESTED TABLE")) {
                collections.add(info);
            } else {
                plain.add(info);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("total", vars.size());
        result.put("typeRefCount", typeRefs.size());
        result.put("rowtypeRefCount", rowtypeRefs.size());
        result.put("collectionCount", collections.size());
        result.put("plainCount", plain.size());
        if (!typeRefs.isEmpty()) result.put("typeRefs", typeRefs);
        if (!rowtypeRefs.isEmpty()) result.put("rowtypeRefs", rowtypeRefs);
        if (!collections.isEmpty()) result.put("collections", collections);
        if (!plain.isEmpty()) result.put("plain", plain);
        return result;
    }

    /**
     * Flatten an {@code operation -> [lines]} map into the true source-order sequence of accesses.
     *
     * <p>The grouped map ({@code {"SELECT":[12,88],"INSERT":[40]}}) is the shape the UI's per-table
     * "operations" rendering wants and is kept exactly as-is, but it cannot express the order the
     * accesses actually execute in: a table read at 12, written at 40 and read again at 88 reads
     * back as SELECT(12), SELECT(88), INSERT(40) when walked operation-by-operation. Every emitter
     * of that map therefore also emits this sibling {@code accessSequence} list, so any consumer
     * that needs "the sequence of operations on this table" has it without having to reconstruct it
     * by merge-sorting the grouped map after the fact (which is what unified-web's
     * {@code ParserPredicatesAdapter} had to do).
     *
     * <p>Sorted by line only, and {@link List#sort} is stable, so accesses sharing a line keep the
     * operation order the grouped map recorded — identical to the downstream reconstruction.
     */
    private static List<Map<String, Object>> accessSequence(Map<String, List<Integer>> opsByName) {
        List<Map<String, Object>> seq = new ArrayList<>();
        if (opsByName == null) return seq;
        for (var e : opsByName.entrySet()) {
            if (e.getValue() == null) continue;
            for (Integer line : e.getValue()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("operation", e.getKey());
                m.put("line", line == null ? 0 : line);
                seq.add(m);
            }
        }
        seq.sort(Comparator.comparingInt(m -> (Integer) m.get("line")));
        return seq;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> groupTableOps(List<TableOperationInfo> ops) {
        Map<String, Map<String, Object>> grouped = new LinkedHashMap<>();
        // Accumulate line numbers in a Set to deduplicate entries that arise when the same
        // table is recorded by both cursor promotion and the normal DML visitor path.
        Map<String, Map<String, Set<Integer>>> lineAccum = new LinkedHashMap<>();

        for (TableOperationInfo top : ops) {
            String key = tableKey(top);
            if (key == null) continue; // no real table (e.g. `SELECT @x = ...`) — see tableKey() doc
            // Package-aggregation nodes tag each item with the subprogram it came from (see
            // TableOperationInfo#getSubprogramName). Fold that into the grouping key so the SAME
            // table touched by two different subprograms is not silently merged into one row —
            // that would make it impossible for PackageTreeBuilder to filter this table back out
            // to a single member. Always null for non-package crawls, so the key (and therefore
            // the grouped output) is byte-identical to before there.
            String subprogram = top.getSubprogramName();
            String groupKey = subprogram != null ? key + "\u0000" + subprogram : key;
            Map<String, Object> info = grouped.computeIfAbsent(groupKey, k -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("schema", top.getSchema());
                m.put("name", top.getTableName());
                m.put("objectType", top.getObjectType() != null ? top.getObjectType() : "TABLE");
                if (subprogram != null) m.put("subprogramName", subprogram);
                m.put("operations", new LinkedHashMap<String, List<Integer>>());
                m.put("joins", new ArrayList<Map<String, Object>>());
                return m;
            });

            lineAccum.computeIfAbsent(groupKey, k -> new LinkedHashMap<>())
                    .computeIfAbsent(top.getOperation(), k -> new LinkedHashSet<>())
                    .add(top.getLine());

            if (top.getJoins() != null) {
                List<Map<String, Object>> joinsList = (List<Map<String, Object>>) info.get("joins");
                for (JoinInfo j : top.getJoins()) {
                    Map<String, Object> jm = new LinkedHashMap<>();
                    jm.put("joinType", j.getJoinType());
                    jm.put("joinedTable", j.getJoinedTable());
                    jm.put("joinedTableAlias", j.getJoinedTableAlias());
                    // Left side resolved from the ON-condition aliases (may be null).
                    jm.put("leftTable", j.getLeftTable());
                    jm.put("leftTableAlias", j.getLeftTableAlias());
                    jm.put("condition", j.getCondition());
                    jm.put("line", j.getLine());
                    // Originating query/statement id so joins from different SELECTs
                    // sharing a base table can be grouped separately.
                    jm.put("queryId", j.getQueryId());
                    joinsList.add(jm);
                }
            }
        }

        // Write deduplicated, sorted line numbers into each entry's operations map
        for (var entry : lineAccum.entrySet()) {
            Map<String, List<Integer>> opsMap =
                    (Map<String, List<Integer>>) grouped.get(entry.getKey()).get("operations");
            for (var opEntry : entry.getValue().entrySet()) {
                List<Integer> sorted = new ArrayList<>(opEntry.getValue());
                Collections.sort(sorted);
                opsMap.put(opEntry.getKey(), sorted);
            }
            grouped.get(entry.getKey()).put("accessSequence", accessSequence(opsMap));
        }

        return new ArrayList<>(grouped.values());
    }

    /**
     * Distinct call groups for one node, using <b>the same grouping key as {@link #groupCalls}</b>
     * so the index's {@code counts.callsOut} equals the node detail's {@code calls[]} length (and
     * {@code internalCalls}/{@code externalCalls} sum to it). Returns
     * {@code [callsOut, internalCalls, externalCalls]}.
     *
     * <p>Why this is not a name-only de-dup: a target invoked with more than one argument count is
     * an overload, and {@code groupCalls} splits each arity into its own row
     * ({@code COMPUTE_TOTAL (1 args)} / {@code (2 args)}). A name-only key merged them, so
     * {@code counts.callsOut} was lower than the node's own {@code calls[]} for every procedure that
     * calls an overload — and {@code /complexity/scores} read that short value. Keyed identically
     * here, the two agree by construction, and {@code /summary/stats.totalCalls} (the sum of the
     * internal+external halves) stays reconciled with {@code sum(complexity.callsOut)}.
     */
    private static int[] countCallGroups(List<CallInfo> calls, String nodePackageName, String nodeSchema) {
        if (calls == null) return new int[]{0, 0, 0};
        // Pre-scan mirrors groupCalls: distinct argument counts per (package.)name, before the
        // BUILTIN filter, so overload detection agrees exactly.
        Map<String, Set<Integer>> arityByName = new LinkedHashMap<>();
        for (CallInfo c : calls) {
            if (c.getName() == null) continue;
            String bn = (c.getPackageName() != null ? c.getPackageName() + "." : "") + c.getName();
            arityByName.computeIfAbsent(bn.toUpperCase(), k -> new LinkedHashSet<>()).add(c.getArguments());
        }
        Map<String, Boolean> groups = new LinkedHashMap<>(); // groupKey -> isInternal
        for (CallInfo c : calls) {
            String callType = c.getType() != null ? c.getType() : "UNKNOWN";
            if ("BUILTIN".equalsIgnoreCase(callType)) continue;
            String cPkg = c.getPackageName() != null ? c.getPackageName() : "";
            String cName = c.getName() != null ? c.getName() : "";
            boolean overloaded = c.getName() != null && arityByName.getOrDefault(
                    ((cPkg.isEmpty() ? "" : cPkg + ".") + cName).toUpperCase(),
                    java.util.Collections.emptySet()).size() > 1;
            // INTERNAL calls without a package inherit the node's package (see groupCalls).
            if (cPkg.isEmpty() && "INTERNAL".equalsIgnoreCase(callType)
                    && nodePackageName != null && !nodePackageName.isEmpty()) {
                cPkg = nodePackageName;
            }
            String schema = c.getSchema();
            if ((schema == null || schema.isEmpty()) && nodeSchema != null) schema = nodeSchema;
            String groupKey = !cPkg.isEmpty() ? cPkg + "." + cName
                    : (schema != null && !schema.isEmpty() ? schema + "$" + cName : cName);
            if (overloaded) groupKey = groupKey + "#" + c.getArguments();
            String subprogram = c.getSubprogramName();
            if (subprogram != null) groupKey = groupKey + "\u0000" + subprogram;
            groups.putIfAbsent(groupKey, "INTERNAL".equalsIgnoreCase(callType));
        }
        int internal = 0;
        for (boolean isInternal : groups.values()) if (isInternal) internal++;
        return new int[]{groups.size(), internal, groups.size() - internal};
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> groupCalls(List<CallInfo> calls,
                                                  String nodePackageName, String nodeSchema) {
        // Pre-scan: a target name invoked with MORE THAN ONE distinct argument count in this
        // caller is an overloaded call — split those by arity so each overload shows separately.
        Map<String, Set<Integer>> arityByName = new LinkedHashMap<>();
        for (CallInfo c : calls) {
            if (c.getName() == null) continue;
            String bn = (c.getPackageName() != null ? c.getPackageName() + "." : "") + c.getName();
            arityByName.computeIfAbsent(bn.toUpperCase(), k -> new LinkedHashSet<>()).add(c.getArguments());
        }
        Map<String, Map<String, Object>> grouped = new LinkedHashMap<>();
        for (CallInfo c : calls) {
            String callType = c.getType() != null ? c.getType() : "UNKNOWN";
            if ("BUILTIN".equalsIgnoreCase(callType)) continue;

            String cPkg = c.getPackageName() != null ? c.getPackageName() : "";
            String cName = c.getName() != null ? c.getName() : "";
            boolean overloadedCall = c.getName() != null
                    && arityByName.getOrDefault(
                        ((cPkg.isEmpty() ? "" : cPkg + ".") + cName).toUpperCase(),
                        java.util.Collections.emptySet()).size() > 1;

            // INTERNAL calls without packageName inherit the node's package
            if (cPkg.isEmpty() && "INTERNAL".equalsIgnoreCase(callType)
                    && nodePackageName != null && !nodePackageName.isEmpty()) {
                cPkg = nodePackageName;
            }

            String schema = c.getSchema();
            if ((schema == null || schema.isEmpty()) && nodeSchema != null) {
                schema = nodeSchema;
            }

            String target = cPkg.isEmpty() ? cName : cPkg + "." + cName;
            if (overloadedCall) target = target + " (" + c.getArguments() + " args)";
            String finalPkg = cPkg;
            String finalSchema = schema;
            final int finalArity = c.getArguments();
            final boolean finalOverloaded = overloadedCall;
            final String finalTarget = target;

            // Group standalone calls by SCHEMA-qualified key so same-named routines in
            // different schemas (sales.fn_X vs hr.fn_X) stay distinct and don't merge.
            // Overloaded calls additionally split by arity so each overload shows separately.
            String groupKey = !cPkg.isEmpty() ? cPkg + "." + cName
                    : (schema != null && !schema.isEmpty() ? schema + "$" + cName : cName);
            if (overloadedCall) groupKey = groupKey + "#" + c.getArguments();
            // Package-aggregation nodes tag each call with the subprogram it came from (see
            // CallInfo#getSubprogramName) — fold that into the key so the same target called by
            // two different subprograms is not merged into one row, exactly as groupTableOps does.
            // Always null for non-package crawls, so the key is unchanged there.
            String finalSubprogram = c.getSubprogramName();
            if (finalSubprogram != null) groupKey = groupKey + "\u0000" + finalSubprogram;

            Map<String, Object> info = grouped.computeIfAbsent(groupKey, k -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("target", finalTarget);
                m.put("packageName", finalPkg.isEmpty() ? null : finalPkg);
                m.put("objectName", cName);
                m.put("type", callType);
                m.put("schema", finalSchema);
                if (finalOverloaded) m.put("argCount", finalArity);
                if (finalSubprogram != null) m.put("subprogramName", finalSubprogram);
                m.put("lines", new ArrayList<Integer>());
                m.put("count", 0);
                return m;
            });
            ((List<Integer>) info.get("lines")).add(c.getLine());
            info.put("count", (int) info.get("count") + 1);
            // A post-crawl resolved call (the DB-discovered trigger body's callee) carries the
            // crawled target's node id so the facet can cross-link to it. Ordinary calls leave it
            // null, so this field is absent and existing call rows stay byte-identical.
            if (c.getNodeId() != null && !c.getNodeId().isEmpty() && !info.containsKey("nodeId")) {
                info.put("nodeId", c.getNodeId());
            }
        }
        return new ArrayList<>(grouped.values());
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> extractSequences(List<CallInfo> calls) {
        Map<String, Map<String, Object>> grouped = new LinkedHashMap<>();
        for (CallInfo c : calls) {
            String name = c.getName();
            if (name == null) continue;
            if (!"NEXTVAL".equals(name) && !"CURRVAL".equals(name)) continue;
            String seqName = c.getPackageName();
            if (seqName == null || seqName.isEmpty()) continue;

            Map<String, Object> info = grouped.computeIfAbsent(seqName, k -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("name", seqName);
                m.put("schema", c.getSchema());
                m.put("operations", new LinkedHashMap<String, List<Integer>>());
                return m;
            });
            Map<String, List<Integer>> opsMap = (Map<String, List<Integer>>) info.get("operations");
            opsMap.computeIfAbsent(name, k -> new ArrayList<>()).add(c.getLine());
        }
        return new ArrayList<>(grouped.values());
    }

    /**
     * MID-026 — the lightweight per-node projection this row's own design (§111/§139/§166-168) has
     * been converging on: not every consumer of the merged node list needs a full {@link FlowNode}
     * body simultaneously resident for every node. {@code writeTableIndex} is the first consumer
     * converted to prove the pattern (mode-4's own one-facet-at-a-time discipline, not a single
     * all-at-once rewrite) — it never calls {@code computeSubtreeDescendants} or reads any other
     * node's data, so it has zero cross-node graph-shape dependency, making it the safest starting
     * point of the 7 consumers §168 enumerated.
     *
     * <p>Field list confirmed by reading {@code writeTableIndex}'s own body directly, not assumed
     * from an earlier pass's summary — it turned out to need {@code cursors} too (§168's own
     * enumeration missed this), a small real correction found while actually implementing rather
     * than only investigating.
     *
     * <p>Additive, not a replacement: {@link #toLightNodes} projects from the existing full
     * {@code List<FlowNode>} — {@code mergeChunks} itself is untouched, still building the full list
     * exactly as before. The real memory win (not re-reading full bodies from chunks at all) is a
     * separate, later increment; this section proves the DTO shape and one consumer's correctness
     * first.
     */
    private record LightNode(String nodeId, String schema, String packageName, String objectName,
                             int depth, String objectType, List<TableOperationInfo> tableOperations,
                             List<CursorInfo> cursors, String triggerTable, String triggerEvent,
                             String triggerTiming, String triggerWhenClause, String triggerLevel,
                             String triggerBody, String fullId, int linesOfCode) {
        String getNodeId() { return nodeId; }
        String getSchema() { return schema; }
        String getPackageName() { return packageName; }
        String getObjectName() { return objectName; }
        int getDepth() { return depth; }
        String getObjectType() { return objectType; }
        List<TableOperationInfo> getTableOperations() { return tableOperations; }
        List<CursorInfo> getCursors() { return cursors; }
        String getTriggerTable() { return triggerTable; }
        String getTriggerEvent() { return triggerEvent; }
        String getTriggerTiming() { return triggerTiming; }
        String getTriggerWhenClause() { return triggerWhenClause; }
        String getTriggerLevel() { return triggerLevel; }
        String getTriggerBody() { return triggerBody; }
        /** {@link FlowNode#buildFullId()}'s own result, precomputed once — {@code
         *  writeNodeIdMap} (MID-026's 4th converted consumer) only ever needs the finished string,
         *  never the raw {@code parameters} list {@code buildFullId()} itself reads, so the light
         *  projection carries the answer instead of the ingredients. */
        String getFullId() { return fullId; }
        /** Added when {@code nodeIndex} (the cross-node lookup map used for subtree/cycle/edge
         *  resolution throughout {@code writeApiFiles}) was converted from {@code Map<String,
         *  FlowNode>} to this type — every descendant lookup for subtree LOC aggregation reads
         *  exactly this one extra scalar, confirmed directly against each call site. */
        int getLinesOfCode() { return linesOfCode; }
    }

    private static LightNode toLightNode(FlowNode n) {
        return new LightNode(n.getNodeId(), n.getSchema(), n.getPackageName(), n.getObjectName(),
                n.getDepth(), n.getObjectType(), n.getTableOperations(), n.getCursors(),
                n.getTriggerTable(), n.getTriggerEvent(), n.getTriggerTiming(),
                n.getTriggerWhenClause(), n.getTriggerLevel(), n.getTriggerBody(), n.buildFullId(),
                n.getLinesOfCode());
    }

    private static List<LightNode> toLightNodes(List<FlowNode> nodes) {
        List<LightNode> out = new ArrayList<>(nodes.size());
        for (FlowNode n : nodes) out.add(toLightNode(n));
        return out;
    }

    /**
     * MID-026 (final piece) — {@code writeApiFiles}'s own source of nodes, in place of
     * {@code result.getFlowNodes()}. Reads every chunk exactly once, projects to {@link LightNode},
     * discards the full body immediately — the same "read once, project, discard" shape as
     * {@link #buildChunkIndex()}, over the full node shape instead of just its id.
     *
     * <p>A list, not a map, deliberately: {@link #buildChunkIndex()}'s own map silently collapses to
     * its last write if two chunks ever share a raw {@code nodeId} (an unexercised edge case on this
     * repo's real corpus — confirmed zero {@code $2}/{@code $3}-suffixed ids exist there — but a
     * real, latent limitation of that primitive, not one this method repeats).
     */
    private List<LightNode> buildLightNodes() throws IOException {
        List<LightNode> out = new ArrayList<>();
        for (ArtifactKey cf : listSorted(chunksKey, "chunk_")) {
            out.add(toLightNode(readJson(cf, FlowNode.class)));
        }
        return out;
    }

    /** A {@link LightNode} copy with a different {@code nodeId} — records are immutable, so the
     *  short-id dedup pass (which used to mutate {@code FlowNode} in place) produces a new instance
     *  instead. */
    private static LightNode withNodeId(LightNode n, String nodeId) {
        return new LightNode(nodeId, n.schema(), n.packageName(), n.objectName(), n.depth(),
                n.objectType(), n.tableOperations(), n.cursors(), n.triggerTable(), n.triggerEvent(),
                n.triggerTiming(), n.triggerWhenClause(), n.triggerLevel(), n.triggerBody(),
                n.fullId(), n.linesOfCode());
    }

    /** {@link #safeNodeId(FlowNode)}'s own logic, over a {@link LightNode} instead. */
    private String safeNodeId(LightNode node) {
        if (node.getNodeId() != null && !node.getNodeId().isEmpty()) return node.getNodeId();
        StringBuilder sb = new StringBuilder();
        if (node.getSchema() != null) sb.append(node.getSchema()).append("$");
        if (node.getPackageName() != null) sb.append(node.getPackageName()).append("$");
        if (node.getObjectName() != null) sb.append(node.getObjectName());
        return sb.toString().toUpperCase();
    }

    /** Fills {@code key} on a table-index map only when the current value is absent or blank — so a
     *  richer source-parsed value is never overwritten by the catalog's. Null/blank values are
     *  ignored. */
    private static void putIfAbsent(Map<String, Object> m, String key, Object value) {
        if (m == null || value == null) return;
        if (value instanceof String s && s.isBlank()) return;
        Object cur = m.get(key);
        if (cur == null || (cur instanceof String cs && cs.isBlank())) m.put(key, value);
    }

    @SuppressWarnings("unchecked")
    private int writeTableIndex(ArtifactKey apiDir, List<LightNode> nodes) throws IOException {
        ArtifactKey tablesDir = apiDir.resolve("tables");

        Map<String, String> tableObjectTypes = new LinkedHashMap<>();
        if (store.exists(tablesKey)) {
            try (InputStream in = store.read(tablesKey)) {
                Map<String, SchemaTableInfo> tableMap = mapper.readValue(in,
                        mapper.getTypeFactory().constructMapType(LinkedHashMap.class, String.class, SchemaTableInfo.class));
                for (var e : tableMap.entrySet()) {
                    if (e.getValue().getObjectType() != null) tableObjectTypes.put(e.getKey(), e.getValue().getObjectType());
                }
            } catch (Exception ignored) {}
        }

        // Global table map: tableName -> { schema, name, type, usedBy: [{nodeId, nodeName, operations}] }
        Map<String, Map<String, Object>> globalTables = new LinkedHashMap<>();

        for (LightNode node : nodes) {
            if (node.getTableOperations() == null) continue;
            String nodeId = safeNodeId(node);
            String pkg = node.getPackageName() != null ? node.getPackageName() : "";
            String obj = node.getObjectName() != null ? node.getObjectName() : "";
            String nodeName = pkg.isEmpty() ? obj : pkg + "." + obj;

            // Group this node's table ops by table name, deduplicating line numbers
            Map<String, Map<String, Set<Integer>>> nodeTableOps = new LinkedHashMap<>();
            // MID-021 (§138): TableOperationInfo.whereFilters was always produced by the analyser
            // (see its own doc comment) but dropped right here — only the line number survived into
            // nodeTableOps above. Carried through per table key, deduplicated on the filter's own
            // identity string (a join or multi-statement table access can otherwise repeat one via
            // several TableOperationInfo entries for the same table), so the artifact gains
            // predicate data without changing operations' own existing shape. A LinkedHashMap keyed
            // by that identity is the dedup, not a LinkedHashSet with a custom equals — Java sets
            // don't take a per-instance equality function, only a real key does.
            Map<String, Map<String, com.plsqlanalyzer.parser.model.WhereFilter>> nodePredicates =
                    new LinkedHashMap<>();
            for (TableOperationInfo top : node.getTableOperations()) {
                String key = tableKey(top);
                if (key == null) continue; // no real table (e.g. `SELECT @x = ...`) — see tableKey() doc
                nodeTableOps.computeIfAbsent(key, k -> new LinkedHashMap<>())
                        .computeIfAbsent(top.getOperation(), k -> new LinkedHashSet<>())
                        .add(top.getLine());
                if (top.getWhereFilters() != null && !top.getWhereFilters().isEmpty()) {
                    Map<String, com.plsqlanalyzer.parser.model.WhereFilter> byIdentity =
                            nodePredicates.computeIfAbsent(key, k -> new LinkedHashMap<>());
                    for (com.plsqlanalyzer.parser.model.WhereFilter w : top.getWhereFilters()) {
                        String identity = w.getColumnName() + "|" + w.getOperator() + "|"
                                + w.getValue() + "|" + w.getLineNumber();
                        byIdentity.putIfAbsent(identity, w);
                    }
                }
            }

            for (var entry : nodeTableOps.entrySet()) {
                String tableKey = entry.getKey();
                Map<String, Set<Integer>> opsForNode = entry.getValue();

                Map<String, Object> tableInfo = globalTables.computeIfAbsent(tableKey, k -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    String[] p = tableKey.split("\\.", 2);
                    if (p.length == 2) {
                        m.put("schema", p[0]);
                        m.put("name", p[1]);
                    } else {
                        m.put("name", tableKey);
                    }
                    String ot = tableObjectTypes.get(tableKey);
                    m.put("objectType", ot != null ? ot : "TABLE");
                    m.put("allOperations", new LinkedHashSet<String>());
                    m.put("usedBy", new ArrayList<Map<String, Object>>());
                    return m;
                });

                Set<String> allOps = (Set<String>) tableInfo.get("allOperations");
                allOps.addAll(opsForNode.keySet());

                // Convert Set<Integer> → sorted List<Integer> for stable JSON output
                Map<String, List<Integer>> opsForNodeList = new LinkedHashMap<>();
                for (var opEntry : opsForNode.entrySet()) {
                    List<Integer> sorted = new ArrayList<>(opEntry.getValue());
                    Collections.sort(sorted);
                    opsForNodeList.put(opEntry.getKey(), sorted);
                }

                Map<String, Object> usage = new LinkedHashMap<>();
                usage.put("nodeId", nodeId);
                usage.put("nodeName", nodeName);
                usage.put("nodeDepth", node.getDepth());
                usage.put("operations", opsForNodeList);
                usage.put("accessSequence", accessSequence(opsForNodeList));
                // MID-021 (§138): only added when non-empty, matching this writer's own economy —
                // most table accesses have no WHERE clause (a plain SELECT * or an INSERT), and an
                // empty array on every usage row would be pure overhead at scale.
                Map<String, com.plsqlanalyzer.parser.model.WhereFilter> preds = nodePredicates.get(tableKey);
                if (preds != null && !preds.isEmpty()) {
                    usage.put("predicates", new ArrayList<>(preds.values()));
                }
                ((List<Map<String, Object>>) tableInfo.get("usedBy")).add(usage);
            }

            // Also pick up tables from cursors
            if (node.getCursors() != null) {
                for (CursorInfo cursor : node.getCursors()) {
                    if (cursor.getTables() == null) continue;
                    for (String cursorTable : cursor.getTables()) {
                        String ctKey = cursorTable.toUpperCase();
                        // Check if already in globalTables with schema prefix
                        boolean found = false;
                        for (String gKey : globalTables.keySet()) {
                            if (gKey.endsWith("." + ctKey)) { found = true; break; }
                        }
                        if (!found && !globalTables.containsKey(ctKey)) {
                            Map<String, Object> m = new LinkedHashMap<>();
                            m.put("name", ctKey);
                            m.put("allOperations", new LinkedHashSet<>(Set.of("CURSOR_REF")));
                            List<Map<String, Object>> usedBy = new ArrayList<>();
                            Map<String, Object> usage = new LinkedHashMap<>();
                            usage.put("nodeId", nodeId);
                            usage.put("nodeName", nodeName);
                            Map<String, List<Integer>> cursorRefOps = new LinkedHashMap<>();
                            cursorRefOps.put("CURSOR_REF", List.of(cursor.getLine()));
                            usage.put("operations", cursorRefOps);
                            usage.put("accessSequence", accessSequence(cursorRefOps));
                            usedBy.add(usage);
                            m.put("usedBy", usedBy);
                            globalTables.put(ctKey, m);
                        }
                    }
                }
            }
        }

        for (LightNode node : nodes) {
            if (!"TRIGGER".equalsIgnoreCase(node.getObjectType())) continue;
            String trigTable = node.getTriggerTable();
            if (trigTable == null || trigTable.isEmpty()) continue;
            String trigKey = trigTable.toUpperCase();
            for (var entry : globalTables.entrySet()) {
                String name = (String) entry.getValue().get("name");
                if (name != null && name.equalsIgnoreCase(trigTable) || entry.getKey().equalsIgnoreCase(trigKey)
                        || entry.getKey().endsWith("." + trigKey)) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> triggers = (List<Map<String, Object>>) entry.getValue()
                            .computeIfAbsent("triggers", k -> new ArrayList<Map<String, Object>>());
                    Map<String, Object> trigInfo = new LinkedHashMap<>();
                    trigInfo.put("nodeId", safeNodeId(node));
                    trigInfo.put("name", node.getObjectName());
                    trigInfo.put("schema", node.getSchema());
                    trigInfo.put("event", node.getTriggerEvent());
                    trigInfo.put("timing", node.getTriggerTiming());
                    // Source-parsed trigger facets — the same keys the estate trigger facet reads
                    // (whenClause/level/definition), so a source-parsed trigger reports its WHEN
                    // condition, ROW/STATEMENT level and body without a re-parse at read time.
                    if (node.getTriggerWhenClause() != null) trigInfo.put("whenClause", node.getTriggerWhenClause());
                    if (node.getTriggerLevel() != null) trigInfo.put("level", node.getTriggerLevel());
                    if (node.getTriggerBody() != null) trigInfo.put("definition", node.getTriggerBody());
                    trigInfo.put("source", "SOURCE");
                    triggers.add(trigInfo);
                    break;
                }
            }
        }

        // Merge DB-discovered triggers (from ALL_TRIGGERS for DML tables)
        if (discoveredTriggers != null && !discoveredTriggers.isEmpty()) {
            for (Map<String, Object> dbTrig : discoveredTriggers) {
                String trigTableName = ((String) dbTrig.get("tableName"));
                if (trigTableName == null) continue;
                String trigKey = trigTableName.toUpperCase();

                for (var entry : globalTables.entrySet()) {
                    String name = (String) entry.getValue().get("name");
                    if (name != null && (name.equalsIgnoreCase(trigTableName)
                            || entry.getKey().equalsIgnoreCase(trigKey)
                            || entry.getKey().endsWith("." + trigKey))) {

                        List<Map<String, Object>> triggers = (List<Map<String, Object>>) entry.getValue()
                                .computeIfAbsent("triggers", k -> new ArrayList<Map<String, Object>>());

                        // A trigger already present from source parsing (or from a
                        // catalog-discovered TRIGGER node written above) is the SAME trigger: do not
                        // add a duplicate row. Instead let the catalog fill the metadata only it
                        // knows (status/triggerType/event/timing) — the source-parsed body/definition
                        // and nodeId win because putIfAbsent never overwrites a present value.
                        String trigName = (String) dbTrig.get("name");
                        Map<String, Object> existing = null;
                        if (trigName != null) {
                            for (Map<String, Object> t : triggers) {
                                if (trigName.equalsIgnoreCase((String) t.get("name"))) { existing = t; break; }
                            }
                        }
                        if (existing != null) {
                            putIfAbsent(existing, "triggerType", dbTrig.get("triggerType"));
                            putIfAbsent(existing, "status", dbTrig.get("status"));
                            putIfAbsent(existing, "event", dbTrig.get("event"));
                            putIfAbsent(existing, "timing", dbTrig.get("timing"));
                            putIfAbsent(existing, "tableOps", dbTrig.get("tableOps"));
                            putIfAbsent(existing, "sequences", dbTrig.get("sequences"));
                            putIfAbsent(existing, "definition", dbTrig.get("definition"));
                            continue;
                        }

                        Map<String, Object> trigInfo = new LinkedHashMap<>();
                        trigInfo.put("name", trigName);
                        trigInfo.put("schema", dbTrig.get("schema"));
                        trigInfo.put("event", dbTrig.get("event"));
                        trigInfo.put("timing", dbTrig.get("timing"));
                        trigInfo.put("triggerType", dbTrig.get("triggerType"));
                        trigInfo.put("status", dbTrig.get("status"));
                        if (dbTrig.get("definition") != null) trigInfo.put("definition", dbTrig.get("definition"));
                        if (dbTrig.get("tableOps") != null) trigInfo.put("tableOps", dbTrig.get("tableOps"));
                        if (dbTrig.get("sequences") != null) trigInfo.put("sequences", dbTrig.get("sequences"));
                        trigInfo.put("source", "DATABASE");
                        triggers.add(trigInfo);
                        break;
                    }
                }
            }
        }

        // Convert sets to lists and write
        List<Map<String, Object>> tableList = new ArrayList<>();
        for (Map<String, Object> t : globalTables.values()) {
            Map<String, Object> clean = new LinkedHashMap<>(t);
            clean.put("allOperations", new ArrayList<>((Set<String>) t.get("allOperations")));
            tableList.add(clean);
        }
        tableList.sort(Comparator.comparing(m -> {
            String s = (String) m.get("schema");
            String n = (String) m.get("name");
            return (s != null ? s : "") + "." + (n != null ? n : "");
        }));

        Map<String, Object> tableIndex = new LinkedHashMap<>();
        tableIndex.put("totalTables", tableList.size());

        // Summary: group by operation type
        Map<String, Integer> opSummary = new LinkedHashMap<>();
        for (Map<String, Object> t : tableList) {
            List<String> ops = (List<String>) t.get("allOperations");
            for (String op : ops) opSummary.merge(op, 1, Integer::sum);
        }
        tableIndex.put("operationSummary", opSummary);
        tableIndex.put("tables", tableList);

        ArtifactKey key = tablesDir.resolve("index.json");
        long bytes = writeJson(key, tableIndex);
        manifest.recordArtifact(key, bytes, (long) tableList.size());
        return tableList.size();
    }

    /**
     * {@code api/jobs/index.json} — the scheduled-job facet: one row per discovered job with its
     * schedule, enabled flag, the routine it targets and the job node's own id. Written only from
     * the post-crawl {@link JobDiscoverer}; an estate with no scheduler catalog gets an empty
     * document ({@code totalJobs:0}) so the reader degrades to an empty page rather than a 404.
     */
    private int writeJobsIndex(ArtifactKey apiDir) throws IOException {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map<String, Object> j : discoveredJobs) {
            if (j == null) continue;
            rows.add(new LinkedHashMap<>(j));
        }
        rows.sort(Comparator.comparing(m -> (String.valueOf(m.getOrDefault("schema", ""))
                + "." + m.getOrDefault("name", ""))));
        Map<String, Object> index = new LinkedHashMap<>();
        index.put("totalJobs", rows.size());
        index.put("jobs", rows);
        ArtifactKey key = apiDir.resolve("jobs", "index.json");
        long bytes = writeJson(key, index);
        manifest.recordArtifact(key, bytes, (long) rows.size());
        return rows.size();
    }

    /**
     * MID-026 — {@code nodeIndex} was accepted as a parameter but never read anywhere in this
     * method: every field it writes (`from`/`to`/schema/nodeId/lines/count/backEdge) comes from the
     * {@code FlowEdge} objects themselves, never from a node lookup. Removed rather than converted
     * to {@link LightNode} — this consumer needs no node data at all, full or lightweight, which is
     * a cheaper outcome than the bounded-conversion pattern {@code writeTableIndex}/{@code
     * writeStatementIndex} used, not a partial version of it.
     */
    private void writeCallGraph(ArtifactKey apiDir, List<FlowEdge> edges,
                                List<List<String>> cycles) throws IOException {
        // Build set of back-edges from detected cycles (last->first in each cycle)
        Set<String> backEdgeKeys = new HashSet<>();
        for (List<String> cycle : cycles) {
            if (cycle.size() >= 2) {
                String backFrom = cycle.get(cycle.size() - 1);
                String backTo = cycle.get(0);
                backEdgeKeys.add(backFrom + "->" + backTo);
            }
        }

        // Deduplicated edges: from->to with all occurrences
        Map<String, Map<String, Object>> deduped = new LinkedHashMap<>();
        for (FlowEdge e : edges) {
            String from = (e.getFromPackage() != null ? e.getFromPackage() + "." : "") + e.getFromObject();
            String to = (e.getToPackage() != null ? e.getToPackage() + "." : "") + e.getToObject();
            // Dedup key: for STANDALONE objects (no package) include schema so same-named
            // routines in different schemas stay distinct edges (Oracle package edges keep
            // their historical package-qualified key unchanged).
            String fromKey = e.getFromPackage() != null ? from
                    : (e.getFromSchema() != null && !e.getFromSchema().isEmpty() ? e.getFromSchema() + "$" + e.getFromObject() : from);
            String toKey = e.getToPackage() != null ? to
                    : (e.getToSchema() != null && !e.getToSchema().isEmpty() ? e.getToSchema() + "$" + e.getToObject() : to);
            String key = fromKey + " -> " + toKey;
            // Overloaded targets share the same object name but have distinct nodeIds
            // (…__<arity>); include nodeIds so overload edges stay separate. For non-overloads
            // the nodeId is fully determined by the object key, so this never over-splits.
            key = key + "|" + e.getFromNodeId() + "->" + e.getToNodeId();

            @SuppressWarnings("unchecked")
            Map<String, Object> info = deduped.computeIfAbsent(key, k -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("fromNodeId", e.getFromNodeId());
                m.put("fromSchema", e.getFromSchema());
                m.put("from", from);
                m.put("toNodeId", e.getToNodeId());
                m.put("toSchema", e.getToSchema());
                m.put("to", to);
                m.put("lines", new ArrayList<Integer>());
                m.put("count", 0);
                m.put("minDepth", e.getDepth());
                m.put("backEdge", false);
                if (e.getEdgeType() != null && !e.getEdgeType().isEmpty()) {
                    m.put("edgeType", e.getEdgeType());   // FIRES_TRIGGER / TRIGGER_FN (call edges omit this)
                }
                return m;
            });

            @SuppressWarnings("unchecked")
            List<Integer> lines = (List<Integer>) info.get("lines");
            lines.add(e.getLine());
            info.put("count", (int) info.get("count") + 1);
            if (e.getDepth() < (int) info.get("minDepth")) {
                info.put("minDepth", e.getDepth());
            }

            // Check if this edge is a back-edge in any cycle
            String fromNodeId = e.getFromNodeId();
            String toNodeId = e.getToNodeId();
            if (fromNodeId != null && toNodeId != null) {
                // Try exact match and prefix match
                for (String beKey : backEdgeKeys) {
                    String[] parts = beKey.split("->");
                    if (fromNodeId.startsWith(parts[0]) && toNodeId.startsWith(parts[1])) {
                        info.put("backEdge", true);
                        break;
                    }
                }
            }
        }

        int backEdgeCount = 0;
        for (Map<String, Object> info : deduped.values()) {
            if (Boolean.TRUE.equals(info.get("backEdge"))) backEdgeCount++;
        }

        Map<String, Object> graph = new LinkedHashMap<>();
        graph.put("totalEdges", edges.size());
        graph.put("uniqueEdges", deduped.size());
        graph.put("backEdges", backEdgeCount);
        graph.put("edges", new ArrayList<>(deduped.values()));

        writeJson(apiDir.resolve("call_graph.json"), graph);
    }

    /**
     * Write node_id_map.json: compact ID <-> full ID (with param details).
     * Compact ID is the main key used everywhere (index, edges, URLs).
     * Full ID has parameter suffixes for complete disambiguation.
     */
    private void writeNodeIdMap(ArtifactKey apiDir, Map<String, LightNode> nodeIndex) throws IOException {
        Map<String, String> idToFull = new LinkedHashMap<>();
        Map<String, String> fullToId = new LinkedHashMap<>();

        for (Map.Entry<String, LightNode> entry : nodeIndex.entrySet()) {
            String compactId = entry.getKey();
            LightNode node = entry.getValue();
            String fullId = node.fullId();

            idToFull.put(compactId, fullId);
            fullToId.put(fullId, compactId);
        }

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("shortToFull", idToFull);   // key name must match ParserAnalysisReader.loadIdMap
        map.put("fullToId", fullToId);

        writeJson(apiDir.resolve("node_id_map.json"), map);
    }

    // ═══════════════════════════════════════════════════════════════

    private String findMatchingNodeId(String edgeId, Map<String, LightNode> nodeIndex) {
        if (nodeIndex.containsKey(edgeId)) return edgeId;
        for (String nid : nodeIndex.keySet()) {
            if (nid.startsWith(edgeId + "$") || nid.startsWith(edgeId + "_")) return nid;
        }
        return null;
    }

    private void computeSubtreeDescendants(String nodeId,
                                            Map<String, Set<String>> childrenMap,
                                            Map<String, Set<String>> subtreeCache) {
        computeSubtreeDescendants(nodeId, childrenMap, subtreeCache, new HashSet<>());
    }

    private void computeSubtreeDescendants(String nodeId,
                                            Map<String, Set<String>> childrenMap,
                                            Map<String, Set<String>> subtreeCache,
                                            Set<String> visiting) {
        if (subtreeCache.containsKey(nodeId)) return;
        if (!visiting.add(nodeId)) return; // cycle detected

        Set<String> descendants = new LinkedHashSet<>();
        Set<String> directChildren = childrenMap.getOrDefault(nodeId, Collections.emptySet());
        for (String child : directChildren) {
            if (visiting.contains(child)) continue; // skip back-edges
            descendants.add(child);
            if (!subtreeCache.containsKey(child)) {
                computeSubtreeDescendants(child, childrenMap, subtreeCache, visiting);
            }
            descendants.addAll(subtreeCache.getOrDefault(child, Collections.emptySet()));
        }
        subtreeCache.put(nodeId, descendants);
        visiting.remove(nodeId);
    }

    /**
     * Column-detail coverage for this analysis, measured from the folded tables themselves.
     *
     * <h3>Why it is computed here and not taken from the enricher's log</h3>
     * {@code TableOperationEnricher} already logs how many statements it parsed, and that number
     * once read "100% parsed" while the persisted artifact contained zero column rows — the fold
     * step in between was dropping the fields. A log line about an intermediate stage is not
     * evidence about the artifact. These counts come from exactly the objects that are about to be
     * written, so a dialect losing its column detail shows up as a number in the analysis output
     * rather than staying silent until someone reads a file by hand.
     *
     * <p>All counts are true totals over the full list, never the size of a capped slice.
     */
    private Map<String, Object> columnCoverage(List<SchemaTableInfo> tables) {
        int total = 0, withColumns = 0, columnRows = 0, withDdl = 0, ddlRows = 0;
        if (tables != null) {
            for (SchemaTableInfo t : tables) {
                if (t == null) continue;
                total++;
                int cols = t.getColumns() == null ? 0 : t.getColumns().size();
                if (cols > 0) { withColumns++; columnRows += cols; }
                int ddls = t.getDdl() == null ? 0 : t.getDdl().size();
                if (ddls > 0) { withDdl++; ddlRows += ddls; }
            }
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("tables", total);
        m.put("tablesWithColumnDetail", withColumns);
        m.put("columnRows", columnRows);
        m.put("tablesWithDdl", withDdl);
        m.put("ddlRows", ddlRows);
        m.put("pctTablesWithColumnDetail",
                total == 0 ? 0.0 : Math.round((withColumns * 1000.0) / total) / 10.0);
        return m;
    }

    private List<List<String>> detectCycles(Map<String, Set<String>> childrenMap,
                                             Map<String, LightNode> nodeIndex) {
        List<List<String>> cycles = new ArrayList<>();
        Set<String> visited = new HashSet<>();
        Set<String> onStack = new LinkedHashSet<>();
        Map<String, String> parentMap = new HashMap<>();

        for (String nodeId : nodeIndex.keySet()) {
            if (!visited.contains(nodeId)) {
                detectCyclesDfs(nodeId, childrenMap, visited, onStack, parentMap, cycles);
            }
        }
        return cycles;
    }

    private void detectCyclesDfs(String nodeId,
                                  Map<String, Set<String>> childrenMap,
                                  Set<String> visited,
                                  Set<String> onStack,
                                  Map<String, String> parentMap,
                                  List<List<String>> cycles) {
        visited.add(nodeId);
        onStack.add(nodeId);

        Set<String> children = childrenMap.getOrDefault(nodeId, Collections.emptySet());
        for (String child : children) {
            if (onStack.contains(child)) {
                // Back-edge found: extract cycle path from child -> ... -> nodeId -> child
                List<String> cycle = new ArrayList<>();
                cycle.add(child);
                String cur = nodeId;
                while (cur != null && !cur.equals(child)) {
                    cycle.add(1, cur);
                    cur = parentMap.get(cur);
                }
                cycles.add(cycle);
            } else if (!visited.contains(child)) {
                parentMap.put(child, nodeId);
                detectCyclesDfs(child, childrenMap, visited, onStack, parentMap, cycles);
            }
        }
        onStack.remove(nodeId);
    }

    /**
     * Build the subtreeTables list for a node by aggregating table operations
     * from this node and all its descendants.
     *
     * <p>MID-026 — the {@code node} parameter this method used to also take was never read: every
     * node in {@code subtreeNodeIds} (including {@code nodeId} itself) is re-fetched from {@code
     * nodeIndex} inside the loop below, the same "not actually used" shape {@code writeCallGraph}'s
     * own dead {@code nodeIndex} parameter had. Removed rather than converted.
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> buildSubtreeTables(String nodeId,
                                                          Set<String> descendants,
                                                          Map<String, LightNode> nodeIndex) {
        // Collect all nodes in subtree (this node + descendants)
        List<String> subtreeNodeIds = new ArrayList<>();
        subtreeNodeIds.add(nodeId);
        subtreeNodeIds.addAll(descendants);

        // tableKey -> aggregated info
        Map<String, Map<String, Object>> tableMap = new LinkedHashMap<>();

        for (String nid : subtreeNodeIds) {
            LightNode n = nodeIndex.get(nid);
            if (n == null || n.getTableOperations() == null || n.getTableOperations().isEmpty()) continue;

            String pkg = n.getPackageName() != null ? n.getPackageName() : "";
            String obj = n.getObjectName() != null ? n.getObjectName() : "";
            String nodeName = pkg.isEmpty() ? obj : pkg + "." + obj;
            boolean isDirect = nid.equals(nodeId);

            // Group this node's ops by table
            Map<String, List<TableOperationInfo>> opsByTable = new LinkedHashMap<>();
            for (TableOperationInfo top : n.getTableOperations()) {
                String key = tableKey(top);
                if (key == null) continue; // no real table (e.g. `SELECT @x = ...`) — see tableKey() doc
                opsByTable.computeIfAbsent(key, k -> new ArrayList<>()).add(top);
            }

            for (var entry : opsByTable.entrySet()) {
                String tableKey = entry.getKey();
                List<TableOperationInfo> ops = entry.getValue();

                String firstObjType = ops.isEmpty() ? "TABLE"
                        : (ops.get(0).getObjectType() != null ? ops.get(0).getObjectType() : "TABLE");
                Map<String, Object> tableInfo = tableMap.computeIfAbsent(tableKey, k -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    String[] parts = tableKey.split("\\.", 2);
                    if (parts.length == 2) {
                        m.put("schema", parts[0]);
                        m.put("name", parts[1]);
                    } else {
                        m.put("name", tableKey);
                    }
                    m.put("objectType", firstObjType);
                    m.put("totalUsageCount", 0);
                    m.put("directUsageCount", 0);
                    m.put("operations", new LinkedHashMap<String, List<Integer>>());
                    m.put("usedByNodes", new ArrayList<Map<String, Object>>());
                    return m;
                });

                // Aggregate operations across all nodes in the subtree, deduplicating line numbers
                Map<String, List<Integer>> opsMap = (Map<String, List<Integer>>) tableInfo.get("operations");
                Map<String, Set<Integer>> nodeOpsSet = new LinkedHashMap<>();
                for (TableOperationInfo top : ops) {
                    nodeOpsSet.computeIfAbsent(top.getOperation(), o -> new LinkedHashSet<>()).add(top.getLine());
                }
                // Write deduplicated lines into subtree opsMap and per-node map
                Map<String, List<Integer>> nodeOpsMap = new LinkedHashMap<>();
                for (var opEntry : nodeOpsSet.entrySet()) {
                    List<Integer> sorted = new ArrayList<>(opEntry.getValue());
                    Collections.sort(sorted);
                    nodeOpsMap.put(opEntry.getKey(), sorted);
                    Set<Integer> existing = new LinkedHashSet<>(
                            opsMap.computeIfAbsent(opEntry.getKey(), o -> new ArrayList<>()));
                    existing.addAll(opEntry.getValue());
                    List<Integer> mergedSorted = new ArrayList<>(existing);
                    Collections.sort(mergedSorted);
                    opsMap.put(opEntry.getKey(), mergedSorted);
                }
                int opsCount = nodeOpsMap.values().stream().mapToInt(List::size).sum();
                tableInfo.put("totalUsageCount", (int) tableInfo.get("totalUsageCount") + opsCount);
                if (isDirect) {
                    tableInfo.put("directUsageCount", (int) tableInfo.get("directUsageCount") + opsCount);
                }

                // Add to usedByNodes
                Map<String, Object> nodeUsage = new LinkedHashMap<>();
                nodeUsage.put("nodeId", nid);
                nodeUsage.put("nodeName", nodeName);
                nodeUsage.put("depth", n.getDepth());
                nodeUsage.put("operations", nodeOpsMap);
                nodeUsage.put("accessSequence", accessSequence(nodeOpsMap));
                ((List<Map<String, Object>>) tableInfo.get("usedByNodes")).add(nodeUsage);
            }
        }

        // Sibling true-line-order view of each table's aggregated `operations` map. Computed after
        // the aggregation loop because opsMap is merged into incrementally, node by node.
        for (Map<String, Object> tableInfo : tableMap.values()) {
            tableInfo.put("accessSequence",
                    accessSequence((Map<String, List<Integer>>) tableInfo.get("operations")));
        }

        return new ArrayList<>(tableMap.values());
    }

    public void writeResolverData(SchemaResolver resolver) throws IOException {
        ArtifactKey resolverDir = buildKey.resolve("resolver");

        writeJson(resolverDir.resolve("q1_object_resolution.json"), resolver.exportObjectResolution());
        writeJson(resolverDir.resolve("q4_direct_dependencies.json"), resolver.exportDirectDependencies());
        writeJson(resolverDir.resolve("q3_reverse_dependencies.json"), resolver.exportReverseDependencies());
        writeJson(resolverDir.resolve("q1_ambiguous_objects.json"), resolver.exportAmbiguousObjects());

        // Resolver artifacts are written after writeFinalResult, so refresh the manifest to include
        // them — it stays the last artifact committed.
        writeManifest();

        System.err.println("[ChunkedFlowWriter] Resolver data written to: " + resolverDir);
    }

    public void flushProgress() throws IOException {
        writeProgress();
    }

    private void writeProgress() throws IOException {
        writeJson(progressKey, progressMap);
    }

    private String safeNodeId(FlowNode node) {
        if (node.getNodeId() != null && !node.getNodeId().isEmpty()) return node.getNodeId();
        StringBuilder sb = new StringBuilder();
        if (node.getSchema() != null) sb.append(node.getSchema()).append("$");
        if (node.getPackageName() != null) sb.append(node.getPackageName()).append("$");
        if (node.getObjectName() != null) sb.append(node.getObjectName());
        return sb.toString().toUpperCase();
    }

    private String buildSourceFileName(FlowNode node) {
        return buildSourceFileName(node.getSchema(), node.getPackageName(), node.getObjectName());
    }

    /** {@link #buildSourceFileName(FlowNode)}'s own logic, over a {@link LightNode} instead. */
    private String buildSourceFileName(LightNode node) {
        return buildSourceFileName(node.getSchema(), node.getPackageName(), node.getObjectName());
    }

    private String buildSourceFileName(String schemaIn, String pkg, String objIn) {
        String recorded = writtenSourceFileNames.get(sourceIdentity(schemaIn, pkg, objIn));
        if (recorded != null) return recorded;
        String schema = schemaIn != null ? schemaIn : "UNKNOWN";
        String obj = objIn != null ? objIn : "UNKNOWN";
        if (pkg != null && !pkg.isEmpty()) {
            return schema + "." + pkg + "." + obj + ".sql";
        }
        return schema + "." + obj + ".sql";
    }

    /**
     * Records the exact artifact file name written for a node's source, so the node/index
     * {@code sourceFile} matches it. The key is the node identity the writer itself builds the name
     * from, so the lookup in {@link #buildSourceFileName} is exact.
     */
    public void recordSourceFile(String schema, String pkg, String obj, String fileName) {
        if (fileName != null) {
            writtenSourceFileNames.put(sourceIdentity(schema, pkg, obj), fileName);
        }
    }

    private static String sourceIdentity(String schema, String pkg, String obj) {
        return (schema != null ? schema : "UNKNOWN") + "|"
                + (pkg != null ? pkg : "") + "|"
                + (obj != null ? obj : "UNKNOWN");
    }

    private String safeFileName(String nodeId) {
        String safe = nodeId.replaceAll("[^A-Za-z0-9_$]", "_");
        if (safe.length() <= 150) return safe;
        // Truncate and append short hash to keep unique
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hash = md.digest(safe.getBytes());
            String hex = String.format("%02x%02x%02x%02x", hash[0], hash[1], hash[2], hash[3]);
            return safe.substring(0, 140) + "_" + hex;
        } catch (Exception e) {
            return safe.substring(0, 150);
        }
    }

    private String extractCursorNameFromSql(String sqlText) {
        if (sqlText == null || sqlText.isEmpty()) return null;
        // Statement sqlText typically starts with the cursor name, e.g. "MY_CURSOR" or "OPEN MY_CURSOR" or "FETCH MY_CURSOR"
        String trimmed = sqlText.trim().toUpperCase();
        // Strip common prefixes
        for (String prefix : new String[]{"OPEN ", "FETCH ", "CLOSE ", "BULK COLLECT "}) {
            if (trimmed.startsWith(prefix)) {
                trimmed = trimmed.substring(prefix.length()).trim();
                break;
            }
        }
        // Take first word as cursor name
        String[] tokens = trimmed.split("[\\s(;,]+", 2);
        if (tokens.length > 0 && !tokens[0].isEmpty()) {
            return tokens[0];
        }
        return null;
    }

    private int countDistinct(List<TableOperationInfo> ops) {
        Set<String> seen = new HashSet<>();
        for (TableOperationInfo t : ops) {
            String key = tableKey(t);
            if (key != null) seen.add(key);
        }
        return seen.size();
    }

    /**
     * Real bug, found via a live audit: {@code TableOperationInfo.getTableName()} can genuinely be
     * null — some dialect visitors (e.g. SQL Server's {@code visitSelectStatement}) deliberately
     * record a table-less op for statements like {@code SELECT @x = GETDATE()} (no FROM clause),
     * and ANTLR error-recovery on malformed SQL can also leave a null name. Every call site below
     * used to build its grouping key via plain string concatenation
     * ({@code schema + "." + tableName}), which — via Java's implicit {@code String.valueOf(null)}
     * — silently turned a null name into the literal text "null". Because every occurrence across
     * an entire procedure produces the SAME key ("null" or ".null"), every one of these unrelated,
     * table-less ops then merged into one giant fake "null" table entry, which is exactly the bug
     * reported live (a "null" table with an implausible ~1925 usage count). Returning null here
     * instead tells every call site "this op isn't a real table access — don't group it," which is
     * the correct behavior: a table-less op is still counted elsewhere (statement/call metrics),
     * it just should never masquerade as a table.
     */
    private static String tableKey(TableOperationInfo top) {
        String name = top.getTableName();
        if (name == null || name.isBlank()) return null;
        return (top.getSchema() != null ? top.getSchema() + "." : "") + name;
    }
}
