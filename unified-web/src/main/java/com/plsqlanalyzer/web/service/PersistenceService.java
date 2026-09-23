package com.plsqlanalyzer.web.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.CallEdge;
import com.plsqlanalyzer.analyzer.model.CallGraphNode;
import com.plsqlanalyzer.analyzer.model.CursorOperationSummary;
import com.plsqlanalyzer.analyzer.model.JoinOperationSummary;
import com.plsqlanalyzer.analyzer.model.SequenceOperationSummary;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import com.plsqlanalyzer.parser.model.PlsqlUnit;
import com.plsqlanalyzer.parser.service.OracleDictionaryService.TableMetadata;
import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.plsqlanalyzer.web.parser.service.AnalysisPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Saves/loads analysis results in named folders.
 * Structure: {@code plsql/{name}/analysis.json + metadata.json}
 * Each analysis gets a unique name like "OPUS_CORE_PKG_CUSTOMER_20260416_143022"
 *
 * <p>Every read and write goes through {@link ArtifactStore}; this class holds no
 * {@code Path} and never touches the filesystem, so the same code serves local and S3 storage.
 * Locations are built with {@link AnalysisPaths#claudeDirKey}/{@link AnalysisPaths#claudeFileKey},
 * which root the Claude sidecar subtree at {@code plsql/}.
 */
public class PersistenceService {

    private static final Logger log = LoggerFactory.getLogger(PersistenceService.class);
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    private final ObjectMapper mapper;
    private final ArtifactStore store;

    public PersistenceService(ArtifactStore store) {
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());
        this.mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        this.store = Objects.requireNonNull(store, "ArtifactStore is required");
    }

    // ── Store plumbing ──────────────────────────────────────────────────────

    /** Reads via the store. Callers must close. */
    private InputStream openRead(ArtifactKey key) throws IOException {
        return store.read(key);
    }

    /** Writes via the store; close() is the commit point. */
    private OutputStream openWrite(ArtifactKey key) throws IOException {
        return store.write(key);
    }

    /** Length-aware write, for the callers that already hold the byte count. */
    private OutputStream openWrite(ArtifactKey key, long contentLength) throws IOException {
        return store.write(key, contentLength);
    }

    /** Root of every analysis folder this service owns: {@code plsql/}. */
    private static ArtifactKey claudeRoot() {
        return ArtifactKey.of(AnalysisPaths.CLAUDE_ROOT);
    }

    /**
     * The analysis folder key. Analysis names reach this class from REST paths, so the traversal
     * guard that {@code resolved.startsWith(base)} used to provide is kept here explicitly —
     * {@link ArtifactKey} would throw on a separator, but a caller-facing "not found" is the
     * behaviour every lookup below already expects.
     */
    private static boolean badName(String name) {
        return name == null || name.isBlank()
                || name.contains("..") || name.contains("/") || name.contains("\\");
    }

    private static ArtifactKey dirKey(String name) {
        return AnalysisPaths.claudeDirKey(name);
    }

    private static ArtifactKey fileKey(String name, String relative) {
        return AnalysisPaths.claudeFileKey(name, relative);
    }

    /** Store connection info used for an analysis result (Oracle JDBC, schemas). */
    public void storeConnections(String analysisName, Map<String, Object> info) {
        if (badName(analysisName)) return;
        try (OutputStream out = openWrite(fileKey(analysisName, "connections.json"))) {
            mapper.writerWithDefaultPrettyPrinter().writeValue(out, info);
        } catch (IOException e) {
            log.warn("Failed to store connections.json for {}: {}", analysisName, e.getMessage());
        }
    }

    /** Load connection info for an analysis result. */
    @SuppressWarnings("unchecked")
    public Map<String, Object> loadConnections(String analysisName) {
        if (badName(analysisName)) return null;
        ArtifactKey key = fileKey(analysisName, "connections.json");
        if (!store.exists(key)) return null;
        try (InputStream in = openRead(key)) {
            return mapper.readValue(in, Map.class);
        } catch (Exception e) {
            log.warn("Failed to load connections.json for {}: {}", analysisName, e.getMessage());
            return null;
        }
    }

    /**
     * Generate a unique analysis name from entry point info.
     */
    public String generateName(String schema, String objectName, String procedureName) {
        String base = (schema != null ? schema : "UNKNOWN") + "_"
                + (objectName != null ? objectName : "OBJECT");
        if (procedureName != null && !procedureName.isEmpty()) {
            base += "_" + procedureName;
        }
        base += "_" + TS_FMT.format(LocalDateTime.now());
        return base.toUpperCase().replaceAll("[^A-Z0-9_]", "_");
    }

    /**
     * Save analysis result in its own named folder.
     * Returns the analysis name.
     */
    public String save(AnalysisResult result) {
        String name = result.getName();
        if (name == null || name.isEmpty()) {
            name = generateName(result.getEntrySchema(), result.getEntryObjectName(), result.getEntryProcedure());
            result.setName(name);
        }

        if (result.getAnalysisMode() == null) {
            result.setAnalysisMode("STATIC");
        }

        try {
            // Save full analysis data + split files for lazy loading. Streamed to the store —
            // this document runs to tens of MB and is never buffered.
            AnalysisSnapshot snapshot = toSnapshot(result);
            try (OutputStream out = openWrite(fileKey(name, "analysis.json"))) {
                mapper.writeValue(out, snapshot);
            }
            writeSplitFiles(name, snapshot);

            // Save lightweight metadata for fast listing
            Map<String, Object> meta = new LinkedHashMap<>();
            meta.put("name", name);
            meta.put("entrySchema", result.getEntrySchema());
            meta.put("entryObjectName", result.getEntryObjectName());
            meta.put("entryObjectType", result.getEntryObjectType());
            meta.put("entryProcedure", result.getEntryProcedure());
            meta.put("timestamp", result.getTimestamp().toString());
            meta.put("procedureCount", result.getProcedureCount());
            meta.put("fileCount", result.getFileCount());
            meta.put("tableCount", result.getTableOperations() != null ? result.getTableOperations().size() : 0);
            meta.put("sequenceCount", result.getSequenceOperations() != null ? result.getSequenceOperations().size() : 0);
            meta.put("joinCount", result.getJoinOperations() != null ? result.getJoinOperations().size() : 0);
            meta.put("cursorCount", result.getCursorOperations() != null ? result.getCursorOperations().size() : 0);
            meta.put("errorCount", result.getErrorCount());
            meta.put("nodeCount", result.getCallGraph() != null ? result.getCallGraph().getNodeCount() : 0);
            meta.put("edgeCount", result.getCallGraph() != null ? result.getCallGraph().getEdgeCount() : 0);
            meta.put("analysisMode", result.getAnalysisMode());
            meta.put("claudeIteration", result.getClaudeIteration());
            meta.put("claudeEnrichedAt", result.getClaudeEnrichedAt());

            // Compute flow-scoped counts from entry point (same tree-walk as AnalyzerController.buildSummary)
            try {
                CallGraph cg = result.getCallGraph();
                if (cg != null) {
                    String rootId = findRootId(result, cg);
                    if (rootId != null) {
                        Set<String> reachableIds = new HashSet<>();
                        collectTreeNodeIds(cg.getCallTree(rootId), reachableIds);

                        meta.put("flowNodeCount", reachableIds.size());

                        // Flow-scoped table count
                        int flowTableCount = 0;
                        if (result.getTableOperations() != null) {
                            for (var entry2 : result.getTableOperations().entrySet()) {
                                boolean relevant = false;
                                for (var d : entry2.getValue().getAccessDetails()) {
                                    if (d.getProcedureId() != null && matchesProcIdSet(d.getProcedureId(), reachableIds)) {
                                        relevant = true;
                                        break;
                                    }
                                }
                                if (relevant) flowTableCount++;
                            }
                        }
                        meta.put("flowTableCount", flowTableCount);

                        // Flow-scoped sequence count
                        int flowSeqCount = 0;
                        if (result.getSequenceOperations() != null) {
                            for (var entry2 : result.getSequenceOperations().entrySet()) {
                                boolean relevant = false;
                                for (var d : entry2.getValue().getAccessDetails()) {
                                    if (d.getProcedureId() != null && matchesProcIdSet(d.getProcedureId(), reachableIds)) {
                                        relevant = true;
                                        break;
                                    }
                                }
                                if (relevant) flowSeqCount++;
                            }
                        }
                        meta.put("flowSequenceCount", flowSeqCount);

                        // Flow-scoped join count
                        int flowJoinCount = 0;
                        if (result.getJoinOperations() != null) {
                            for (var entry2 : result.getJoinOperations().entrySet()) {
                                boolean relevant = false;
                                for (var d : entry2.getValue().getAccessDetails()) {
                                    if (d.getProcedureId() != null && matchesProcIdSet(d.getProcedureId(), reachableIds)) {
                                        relevant = true;
                                        break;
                                    }
                                }
                                if (relevant) flowJoinCount++;
                            }
                        }
                        meta.put("flowJoinCount", flowJoinCount);

                        // Flow-scoped cursor count
                        int flowCursorCount = 0;
                        if (result.getCursorOperations() != null) {
                            for (var entry2 : result.getCursorOperations().entrySet()) {
                                boolean relevant = false;
                                for (var d : entry2.getValue().getAccessDetails()) {
                                    if (d.getProcedureId() != null && matchesProcIdSet(d.getProcedureId(), reachableIds)) {
                                        relevant = true;
                                        break;
                                    }
                                }
                                if (relevant) flowCursorCount++;
                            }
                        }
                        meta.put("flowCursorCount", flowCursorCount);

                        // Flow-scoped error count
                        int flowErrorCount = 0;
                        if (result.getUnits() != null) {
                            for (var unit : result.getUnits()) {
                                if (unit.getParseErrors() != null && !unit.getParseErrors().isEmpty()) {
                                    // Check if any procedure in this unit is reachable
                                    boolean unitReachable = false;
                                    for (var proc : unit.getProcedures()) {
                                        String procId = (unit.getSchemaName() != null ? unit.getSchemaName() + "." : "")
                                                + (unit.getName() != null ? unit.getName() + "." : "")
                                                + proc.getName();
                                        if (matchesProcIdSet(procId, reachableIds)) {
                                            unitReachable = true;
                                            break;
                                        }
                                    }
                                    if (unitReachable) {
                                        flowErrorCount += unit.getParseErrors().size();
                                    }
                                }
                            }
                        }
                        meta.put("flowErrorCount", flowErrorCount);
                    }
                }
            } catch (Exception e) {
                log.debug("Error computing flow-scoped metadata: {}", e.getMessage());
            }

            try (OutputStream out = openWrite(fileKey(name, "metadata.json"))) {
                mapper.writeValue(out, meta);
            }

            log.info("Saved analysis '{}' to {}", name, dirKey(name));
        } catch (IOException e) {
            log.error("Failed to save analysis '{}': {}", name, e.getMessage());
        }

        return name;
    }

    /**
     * Save Claude-enriched version. Rotates: current claude → claude_prev, then saves new.
     * Keeps: analysis.json (static, immutable), analysis_claude.json (latest), analysis_claude_prev.json (previous).
     */
    public void saveClaude(AnalysisResult result) {
        String name = result.getName();
        if (badName(name)) return;

        try {
            // Rotate: current claude → prev
            ArtifactKey claudeFile = fileKey(name, "analysis_claude.json");
            ArtifactKey claudePrev = fileKey(name, "analysis_claude_prev.json");
            if (store.exists(claudeFile)) {
                store.move(claudeFile, claudePrev);
                log.info("Rotated claude -> prev for '{}'", name);
            }

            // Save new claude version + update split files
            AnalysisSnapshot snapshot = toSnapshot(result);
            try (OutputStream out = openWrite(claudeFile)) {
                mapper.writeValue(out, snapshot);
            }
            writeSplitFiles(name, snapshot);

            // Update metadata with claude info
            ArtifactKey metaFile = fileKey(name, "metadata.json");
            if (store.exists(metaFile)) {
                Map<String, Object> meta;
                try (InputStream in = openRead(metaFile)) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> read = mapper.readValue(in, LinkedHashMap.class);
                    meta = read;
                }
                meta.put("analysisMode", result.getAnalysisMode());
                meta.put("claudeIteration", result.getClaudeIteration());
                meta.put("claudeEnrichedAt", result.getClaudeEnrichedAt());
                meta.put("tableCount", result.getTableOperations() != null ? result.getTableOperations().size() : 0);
                meta.put("joinCount", result.getJoinOperations() != null ? result.getJoinOperations().size() : 0);
                meta.put("cursorCount", result.getCursorOperations() != null ? result.getCursorOperations().size() : 0);
                try (OutputStream out = openWrite(metaFile)) {
                    mapper.writeValue(out, meta);
                }
            }

            log.info("Saved Claude-enriched analysis '{}' (iteration {})", name, result.getClaudeIteration());
        } catch (IOException e) {
            log.error("Failed to save Claude analysis '{}': {}", name, e.getMessage());
        }
    }

    /**
     * Load best available version using lazy loading (sections loaded on demand).
     */
    public AnalysisResult loadByName(String name) {
        if (badName(name)) return null;
        // ONE listing rather than four exists() probes: over S3 each probe is a ~25 ms HEAD.
        Map<ArtifactKey, ArtifactEntry> stat = statFolder(name);
        boolean hasAny = stat.containsKey(fileKey(name, AnalysisSection.GRAPH.filename))
                || stat.containsKey(fileKey(name, "analysis_claude.json"))
                || stat.containsKey(fileKey(name, "analysis.json"))
                || stat.containsKey(fileKey(name, "api/index.json"));
        if (!hasAny) return null;
        log.info("Creating lazy result for '{}'", name);
        return createLazyResult(name, stat);
    }

    /**
     * Load specifically the static (original) version — full load for Claude verification.
     */
    public AnalysisResult loadStatic(String name) {
        if (badName(name)) return null;
        ArtifactKey analysisFile = fileKey(name, "analysis.json");
        if (!store.exists(analysisFile)) return null;
        return loadFromKey(analysisFile, name);
    }

    /**
     * Load the previous Claude-enriched version (for revert).
     */
    public AnalysisResult loadClaudePrev(String name) {
        if (badName(name)) return null;
        ArtifactKey claudePrev = fileKey(name, "analysis_claude_prev.json");
        if (!store.exists(claudePrev)) return null;
        return loadFromKey(claudePrev, name);
    }

    /**
     * Revert: swap claude_prev → claude (user-initiated).
     */
    public boolean revertClaude(String name) {
        if (badName(name)) return false;
        ArtifactKey claudeFile = fileKey(name, "analysis_claude.json");
        ArtifactKey claudePrev = fileKey(name, "analysis_claude_prev.json");
        if (!store.exists(claudePrev)) return false;
        try {
            store.move(claudePrev, claudeFile);
            log.info("Reverted Claude enrichment for '{}'", name);
            return true;
        } catch (IOException e) {
            log.error("Failed to revert Claude for '{}': {}", name, e.getMessage());
            return false;
        }
    }

    /**
     * Get version info for an analysis (static, claude iterations, file existence).
     */
    public Map<String, Object> getVersionInfo(String name) {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("name", name);
        if (badName(name)) {
            info.put("hasStatic", false);
            info.put("hasClaude", false);
            info.put("hasClaudePrev", false);
            return info;
        }
        // Existence AND size for three files in a single round trip.
        Map<ArtifactKey, ArtifactEntry> stat = statFolder(name);
        ArtifactEntry staticFile = stat.get(fileKey(name, "analysis.json"));
        ArtifactEntry claudeFile = stat.get(fileKey(name, "analysis_claude.json"));
        info.put("hasStatic", staticFile != null);
        info.put("hasClaude", claudeFile != null);
        info.put("hasClaudePrev", stat.containsKey(fileKey(name, "analysis_claude_prev.json")));
        if (claudeFile != null) info.put("claudeSizeKb", claudeFile.size() / 1024);
        if (staticFile != null) info.put("staticSizeKb", staticFile.size() / 1024);
        return info;
    }

    /** Metadata for every artifact in one analysis folder, in a single store round trip. */
    private Map<ArtifactKey, ArtifactEntry> statFolder(String name) {
        try {
            return store.statPrefix(dirKey(name));
        } catch (IOException e) {
            log.debug("Cannot stat analysis folder '{}': {}", name, e.getMessage());
            return Map.of();
        }
    }

    /**
     * Load the most recently saved analysis (by folder modification time).
     */
    public AnalysisResult loadLatest() {
        List<Map<String, Object>> all = listHistory();
        if (all.isEmpty()) return null;
        String name = (String) all.get(0).get("name");
        return loadByName(name);
    }

    /**
     * List all saved analyses with their metadata, sorted newest first.
     */
    public List<Map<String, Object>> listHistory() {
        List<Map<String, Object>> history = new ArrayList<>();
        Map<ArtifactKey, ArtifactEntry> all;
        try {
            // ONE prefix listing for the whole subtree. The previous directory walk did an
            // exists()/size() per candidate file per folder — fine locally, minutes over S3.
            all = store.statPrefix(claudeRoot());
        } catch (IOException e) {
            log.error("Error listing history: {}", e.getMessage());
            return history;
        }

        // Group by the folder segment directly under plsql/.
        Map<String, Map<ArtifactKey, ArtifactEntry>> byName = new LinkedHashMap<>();
        for (Map.Entry<ArtifactKey, ArtifactEntry> e : all.entrySet()) {
            List<String> segs = e.getKey().segments();
            if (segs.size() < 3) continue;                  // plsql/<name>/<file> at minimum
            byName.computeIfAbsent(segs.get(1), k -> new LinkedHashMap<>()).put(e.getKey(), e.getValue());
        }

        List<Map.Entry<String, Map<ArtifactKey, ArtifactEntry>>> candidates = new ArrayList<>();
        for (var e : byName.entrySet()) {
            String name = e.getKey();
            if (e.getValue().containsKey(fileKey(name, "metadata.json"))
                    || e.getValue().containsKey(fileKey(name, "api/index.json"))) {
                candidates.add(e);
            }
        }
        candidates.sort((a, b) -> Long.compare(metaTime(b.getKey(), b.getValue()),
                                               metaTime(a.getKey(), a.getValue())));

        for (var e : candidates) {
            try {
                Map<String, Object> meta = buildMeta(e.getKey(), e.getValue());
                if (meta != null) history.add(meta);
            } catch (IOException ex) {
                log.debug("Skipping folder {}: {}", e.getKey(), ex.getMessage());
            }
        }
        return history;
    }

    /** Sort key: mtime of metadata.json, else api/index.json, else newest artifact in the folder. */
    private long metaTime(String name, Map<ArtifactKey, ArtifactEntry> folder) {
        ArtifactEntry meta = folder.get(fileKey(name, "metadata.json"));
        if (meta != null) return meta.lastModified();
        ArtifactEntry idx = folder.get(fileKey(name, "api/index.json"));
        if (idx != null) return idx.lastModified();
        return folder.values().stream().mapToLong(ArtifactEntry::lastModified).max().orElse(0L);
    }

    private Map<String, Object> buildMeta(String name, Map<ArtifactKey, ArtifactEntry> folder)
            throws IOException {
        ArtifactKey metaFile = fileKey(name, "metadata.json");
        ArtifactKey apiIndex = fileKey(name, "api/index.json");

        if (folder.containsKey(metaFile)) {
            // Legacy format — metadata.json exists
            Map<String, Object> meta;
            try (InputStream in = openRead(metaFile)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> read = mapper.readValue(in, LinkedHashMap.class);
                meta = read;
            }
            ArtifactEntry analysisFile = folder.get(fileKey(name, "analysis.json"));
            if (analysisFile != null) meta.put("sizeKb", analysisFile.size() / 1024);
            ArtifactEntry claude = folder.get(fileKey(name, "analysis_claude.json"));
            meta.put("hasClaude",      claude != null);
            meta.put("hasClaudePrev",  folder.containsKey(fileKey(name, "analysis_claude_prev.json")));
            if (claude != null) meta.put("claudeSizeKb", claude.size() / 1024);
            meta.put("hasConnections", folder.containsKey(fileKey(name, "connections.json")));
            return meta;
        }

        ArtifactEntry idxEntry = folder.get(apiIndex);
        if (idxEntry != null) {
            // New split format — synthesize metadata from api/index.json
            Map<String, Object> idx;
            try (InputStream in = openRead(apiIndex)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> read = mapper.readValue(in, LinkedHashMap.class);
                idx = read;
            }
            Map<String, Object> meta = new LinkedHashMap<>();
            String entryPoint = (String) idx.getOrDefault("entryPoint", name);
            String schema     = (String) idx.getOrDefault("entrySchema", "");
            meta.put("name",        name);
            meta.put("entryPoint",  entryPoint);
            meta.put("schema",      schema);
            meta.put("totalNodes",  idx.getOrDefault("totalNodes", 0));
            meta.put("maxDepth",    idx.getOrDefault("maxDepth", 0));
            meta.put("totalTables", idx.getOrDefault("totalTables", 0));
            meta.put("totalEdges",  idx.getOrDefault("totalEdges", 0));
            meta.put("totalLinesOfCode", idx.getOrDefault("totalLinesOfCode", 0));
            meta.put("crawlTimeMs", idx.getOrDefault("crawlTimeMs", 0));
            meta.put("timestamp",   idxEntry.lastModified());
            meta.put("format",      "split");
            meta.put("hasClaude",   false);
            meta.put("hasClaudePrev", false);
            meta.put("hasConnections", folder.containsKey(fileKey(name, "connections.json")));
            // Count node files as a size proxy — already in the listing, no extra round trip.
            ArtifactKey nodesDir = fileKey(name, "api/nodes");
            long nodeFileCount = folder.keySet().stream()
                    .filter(k -> nodesDir.equals(k.parent())).count();
            if (nodeFileCount > 0) meta.put("nodeFileCount", nodeFileCount);
            return meta;
        }

        return null;
    }

    /**
     * Delete a saved analysis by name (recursive — handles subdirectories like claude/).
     */
    public boolean delete(String name) {
        if (badName(name)) return false;
        try {
            int removed = store.deletePrefix(dirKey(name));
            if (removed == 0) return false;
            log.info("Deleted analysis '{}' ({} artifacts)", name, removed);
            return true;
        } catch (IOException e) {
            log.error("Failed to delete analysis '{}': {}", name, e.getMessage());
            return false;
        }
    }

    // ---- Legacy compat: version-based methods (delegate to name-based) ----

    public List<Map<String, Object>> listVersions() {
        return listHistory();
    }

    // ---- Serialization helpers ----

    private AnalysisSnapshot toSnapshot(AnalysisResult result) {
        AnalysisSnapshot snap = new AnalysisSnapshot();
        snap.name = result.getName();
        snap.version = result.getVersion();
        snap.timestamp = result.getTimestamp();
        snap.fileCount = result.getFileCount();
        snap.procedureCount = result.getProcedureCount();
        snap.errorCount = result.getErrorCount();
        snap.entrySchema = result.getEntrySchema();
        snap.entryObjectName = result.getEntryObjectName();
        snap.entryObjectType = result.getEntryObjectType();
        snap.entryProcedure = result.getEntryProcedure();
        snap.analysisMode = result.getAnalysisMode();
        snap.claudeIteration = result.getClaudeIteration();
        snap.claudeEnrichedAt = result.getClaudeEnrichedAt();

        // Source map
        if (result.getSourceMap() != null && !result.getSourceMap().isEmpty()) {
            snap.sourceMap = result.getSourceMap();
        }

        // Serialize call graph
        CallGraph cg = result.getCallGraph();
        if (cg != null) {
            snap.nodes = new ArrayList<>();
            for (CallGraphNode n : cg.getAllNodes()) {
                AnalysisSnapshot.NodeData nd = new AnalysisSnapshot.NodeData();
                nd.id = n.getId();
                nd.baseId = n.getBaseId();
                nd.schemaName = n.getSchemaName();
                nd.packageName = n.getPackageName();
                nd.procedureName = n.getProcedureName();
                nd.unitType = n.getUnitType() != null ? n.getUnitType().name() : null;
                nd.sourceFile = n.getSourceFile();
                nd.startLine = n.getStartLine();
                nd.endLine = n.getEndLine();
                nd.callType = n.getCallType();
                nd.placeholder = n.isPlaceholder();
                nd.paramSignature = n.getParamSignature();
                nd.paramCount = n.getParamCount();
                snap.nodes.add(nd);
            }
            snap.edges = new ArrayList<>();
            for (CallEdge e : cg.getAllEdges()) {
                AnalysisSnapshot.EdgeData ed = new AnalysisSnapshot.EdgeData();
                ed.from = e.getFromNodeId();
                ed.to = e.getToNodeId();
                ed.lineNumber = e.getCallLineNumber();
                ed.dynamicSql = e.isDynamicSql();
                ed.callType = e.getCallType();
                snap.edges.add(ed);
            }
        }

        // Serialize table operations
        if (result.getTableOperations() != null) {
            snap.tableOperations = result.getTableOperations();
        }

        // Serialize sequence operations
        if (result.getSequenceOperations() != null) {
            snap.sequenceOperations = result.getSequenceOperations();
        }
        // Serialize join operations
        if (result.getJoinOperations() != null) {
            snap.joinOperations = result.getJoinOperations();
        }
        // Serialize cursor operations
        if (result.getCursorOperations() != null) {
            snap.cursorOperations = result.getCursorOperations();
        }

        // Serialize units (lightweight)
        if (result.getUnits() != null) {
            snap.units = result.getUnits();
        }

        // Serialize table metadata (columns, constraints, indexes, view definitions)
        if (result.getTableMetadata() != null && !result.getTableMetadata().isEmpty()) {
            snap.tableMetadata = result.getTableMetadata();
        }

        return snap;
    }

    private AnalysisResult fromSnapshot(AnalysisSnapshot snap) {
        AnalysisResult result = new AnalysisResult();
        result.setName(snap.name);
        result.setVersion(snap.version);
        result.setTimestamp(snap.timestamp);
        result.setFileCount(snap.fileCount);
        result.setProcedureCount(snap.procedureCount);
        result.setErrorCount(snap.errorCount);
        result.setEntrySchema(snap.entrySchema);
        result.setEntryObjectName(snap.entryObjectName);
        result.setEntryObjectType(snap.entryObjectType);
        result.setEntryProcedure(snap.entryProcedure);
        result.setAnalysisMode(snap.analysisMode);
        result.setClaudeIteration(snap.claudeIteration);
        result.setClaudeEnrichedAt(snap.claudeEnrichedAt);

        if (snap.sourceMap != null) {
            result.setSourceMap(snap.sourceMap);
        }

        rebuildCallGraph(result, snap.nodes, snap.edges);
        result.setTableOperations(snap.tableOperations != null ? snap.tableOperations : new LinkedHashMap<>());
        result.setSequenceOperations(snap.sequenceOperations != null ? snap.sequenceOperations : new LinkedHashMap<>());
        result.setJoinOperations(snap.joinOperations != null ? snap.joinOperations : new LinkedHashMap<>());
        result.setCursorOperations(snap.cursorOperations != null ? snap.cursorOperations : new LinkedHashMap<>());
        result.setUnits(snap.units != null ? snap.units : new ArrayList<>());

        // Restore table metadata (columns, constraints, indexes, view definitions)
        if (snap.tableMetadata != null) {
            result.setTableMetadata(snap.tableMetadata);
        }

        return result;
    }

    private AnalysisResult loadFromKey(ArtifactKey file, String name) {
        try {
            AnalysisSnapshot snap;
            try (InputStream in = openRead(file)) {
                snap = mapper.readValue(in, AnalysisSnapshot.class);
            }
            log.info("Loaded analysis '{}' from {}", name, file);
            AnalysisResult result = fromSnapshot(snap);
            if (result.getName() == null) result.setName(name);
            return result;
        } catch (IOException e) {
            log.error("Failed to load {}: {}", file, e.getMessage());
            return null;
        }
    }

    private String findRootId(AnalysisResult result, CallGraph graph) {
        String schema = result.getEntrySchema() != null ? result.getEntrySchema().toUpperCase() : "";
        String obj = result.getEntryObjectName() != null ? result.getEntryObjectName().toUpperCase() : "";
        String proc = result.getEntryProcedure() != null ? result.getEntryProcedure().toUpperCase() : "";

        if (!proc.isEmpty()) {
            String exact = schema + "." + obj + "." + proc;
            if (graph.getNode(exact) != null) return exact;

            // Exact proc name in this schema (any package)
            for (CallGraphNode node : graph.getAllNodes()) {
                if (node.getProcedureName() != null
                        && node.getProcedureName().equalsIgnoreCase(proc)
                        && schema.equalsIgnoreCase(node.getSchemaName())) {
                    return node.getId();
                }
            }

            // Fuzzy match: strip PL/SQL prefixes and compare core name
            String procCore = stripProcPrefix(proc);
            for (CallGraphNode node : graph.getAllNodes()) {
                if (node.getProcedureName() == null || !schema.equalsIgnoreCase(node.getSchemaName())) continue;
                String np = node.getProcedureName().toUpperCase();
                String npCore = stripProcPrefix(np);
                if (npCore.equals(procCore)
                        || np.endsWith(proc) || proc.endsWith(np)
                        || np.contains(proc) || proc.contains(np)) {
                    return node.getId();
                }
            }
        }

        String pkgId = schema + "." + obj;
        if (graph.getNode(pkgId) != null) return pkgId;

        for (CallGraphNode node : graph.getAllNodes()) {
            String nid = node.getId().toUpperCase();
            if (nid.endsWith("." + obj) || nid.equals(obj)) return node.getId();
        }
        return null;
    }

    private static final String[] PROC_PREFIXES = {"P_", "PC_", "FN_", "F_", "PROC_", "FUN_", "FUNC_"};

    private String stripProcPrefix(String name) {
        String upper = name.toUpperCase();
        for (String prefix : PROC_PREFIXES) {
            if (upper.startsWith(prefix)) return upper.substring(prefix.length());
        }
        return upper;
    }

    private void collectReachableIds(CallGraph graph, String nodeId, Set<String> ids, Set<String> visited, int maxDepth) {
        if (maxDepth <= 0 || visited.contains(nodeId)) return;
        visited.add(nodeId);
        ids.add(nodeId.toUpperCase());
        for (CallEdge edge : graph.getOutgoingEdges(nodeId)) {
            collectReachableIds(graph, edge.getToNodeId(), ids, visited, maxDepth - 1);
        }
    }

    private boolean matchesAny(String procedureId, Set<String> reachableIds) {
        if (procedureId == null) return false;
        String upper = procedureId.toUpperCase();
        if (reachableIds.contains(upper)) return true;
        // Try suffix match
        int dot1 = upper.indexOf('.');
        if (dot1 >= 0) {
            String suffix = upper.substring(dot1 + 1);
            if (reachableIds.contains(suffix)) return true;
            int dot2 = suffix.indexOf('.');
            if (dot2 >= 0 && reachableIds.contains(suffix.substring(dot2 + 1))) return true;
        }
        String lastSeg = upper.contains(".") ? upper.substring(upper.lastIndexOf('.') + 1) : upper;
        for (String rid : reachableIds) {
            String ridLast = rid.contains(".") ? rid.substring(rid.lastIndexOf('.') + 1) : rid;
            if (ridLast.equals(lastSeg)) return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private void collectTreeNodeIds(Map<String, Object> tree, Set<String> ids) {
        Object id = tree.get("id");
        if (id != null) ids.add(id.toString().toUpperCase());
        Object children = tree.get("children");
        if (children instanceof List<?> list) {
            for (Object child : list) {
                if (child instanceof Map<?, ?> childMap) {
                    collectTreeNodeIds((Map<String, Object>) childMap, ids);
                }
            }
        }
    }

    private boolean matchesProcIdSet(String procedureId, Set<String> idSet) {
        if (procedureId == null) return false;
        String upper = procedureId.toUpperCase();
        if (idSet.contains(upper)) return true;
        int dot = upper.indexOf('.');
        if (dot >= 0) {
            String suffix = upper.substring(dot + 1);
            if (idSet.contains(suffix)) return true;
            int dot2 = suffix.indexOf('.');
            if (dot2 >= 0 && idSet.contains(suffix.substring(dot2 + 1))) return true;
        }
        String lastSeg = upper.contains(".") ? upper.substring(upper.lastIndexOf('.') + 1) : upper;
        for (String rid : idSet) {
            String ridLast = rid.contains(".") ? rid.substring(rid.lastIndexOf('.') + 1) : rid;
            if (ridLast.equals(lastSeg)) return true;
        }
        return false;
    }

    // ---- Split-file lazy loading ----

    public enum AnalysisSection {
        GRAPH("analysis-graph.json"),
        SOURCE("analysis-source.json"),
        OPS("analysis-ops.json"),
        UNITS("analysis-units.json"),
        TABLE_META("analysis-tablemeta.json");

        public final String filename;
        AnalysisSection(String f) { this.filename = f; }
    }

    public static class GraphSnapshot {
        public List<AnalysisSnapshot.NodeData> nodes;
        public List<AnalysisSnapshot.EdgeData> edges;
    }

    public static class SourceSnapshot {
        public Map<String, List<String>> sourceMap;
    }

    public static class OpsSnapshot {
        public Map<String, TableOperationSummary> tableOperations;
        public Map<String, SequenceOperationSummary> sequenceOperations;
        public Map<String, JoinOperationSummary> joinOperations;
        public Map<String, CursorOperationSummary> cursorOperations;
    }

    public static class UnitsSnapshot {
        public List<PlsqlUnit> units;
    }

    public static class TableMetaSnapshot {
        public Map<String, TableMetadata> tableMetadata;
    }

    private void writeSplitFiles(String name, AnalysisSnapshot snapshot) {
        try {
            GraphSnapshot gs = new GraphSnapshot();
            gs.nodes = snapshot.nodes;
            gs.edges = snapshot.edges;
            writeSection(name, AnalysisSection.GRAPH, gs);

            SourceSnapshot ss = new SourceSnapshot();
            ss.sourceMap = snapshot.sourceMap;
            writeSection(name, AnalysisSection.SOURCE, ss);

            OpsSnapshot os = new OpsSnapshot();
            os.tableOperations = snapshot.tableOperations;
            os.sequenceOperations = snapshot.sequenceOperations;
            os.joinOperations = snapshot.joinOperations;
            os.cursorOperations = snapshot.cursorOperations;
            writeSection(name, AnalysisSection.OPS, os);

            UnitsSnapshot us = new UnitsSnapshot();
            us.units = snapshot.units;
            writeSection(name, AnalysisSection.UNITS, us);

            TableMetaSnapshot ts = new TableMetaSnapshot();
            ts.tableMetadata = snapshot.tableMetadata;
            writeSection(name, AnalysisSection.TABLE_META, ts);

            log.info("Wrote split files for '{}'", name);
        } catch (Exception e) {
            log.error("Failed to write split files for '{}': {}", name, e.getMessage());
        }
    }

    /**
     * The old code wrote a {@code .tmp} sibling and renamed it, to keep a concurrent reader from
     * seeing a half-written section. {@link ArtifactStore#write} is already atomic-on-close, so
     * the temp file — and the extra local write it implied — is gone.
     */
    private void writeSection(String name, AnalysisSection section, Object value) throws IOException {
        try (OutputStream out = openWrite(fileKey(name, section.filename))) {
            mapper.writeValue(out, value);
        }
    }

    private final ConcurrentHashMap<String, Object> migrationLocks = new ConcurrentHashMap<>();

    void loadSection(AnalysisResult target, String name, AnalysisSection section) {
        if (badName(name)) return;
        ArtifactKey splitFile = fileKey(name, section.filename);
        if (store.exists(splitFile)) {
            loadFromSplitFile(target, name, splitFile, section);
        } else {
            migrateAndLoadSection(target, name, section);
        }
    }

    /**
     * MID-027 — one entry from {@code analysis-source.json} without materialising the whole
     * {@code sourceMap}. Every real read-time consumer of {@code AnalysisResult.getSourceMap()}
     * (`MiscQueryService.getSource`, `ClaudeChunkMerger`, `ClaudePromptBuilder`) does a single
     * {@code map.get(key)} — but going through {@code getSourceMap()} on a {@link
     * LazyAnalysisResult} that hasn't loaded its source section yet pulls the entire map into heap
     * (every source file's every line, for every object in the analysis) just to answer one lookup,
     * and it then stays resident for the rest of that result's lifetime.
     *
     * <p>Streams the split file's {@code {"sourceMap": {...}}} object token-by-token with Jackson's
     * streaming API instead of {@code mapper.readValue}: the matching key's value is the only thing
     * ever deserialised into a {@code List<String>}; every other key's value is skipped via {@link
     * com.fasterxml.jackson.core.JsonParser#skipChildren()} without being parsed into objects at
     * all. No live PL/SQL corpus exists in this repo to size a real {@code sourceMap} against (§37),
     * but this fix's correctness — the map is never materialised, regardless of how large it is — is
     * independent of scale, so it is exactly what §37 called the "small if ever justified" fix.
     *
     * @return the source lines for {@code key}, or {@code null} if the file doesn't exist, the key
     *         isn't present, or the file predates the {@code sourceMap} split (in which case the
     *         caller's own fallback to {@code getSourceMap()} — the pre-existing, unchanged behavior
     *         — still answers correctly; this method only ever narrows the read, never replaces it
     *         as the sole path).
     */
    public List<String> readSourceLines(String name, String key) {
        if (badName(name) || key == null) return null;
        ArtifactKey file = fileKey(name, AnalysisSection.SOURCE.filename);
        if (!store.exists(file)) return null;
        try (InputStream in = openRead(file);
             com.fasterxml.jackson.core.JsonParser p = mapper.getFactory().createParser(in)) {
            if (p.nextToken() != com.fasterxml.jackson.core.JsonToken.START_OBJECT) return null;
            while (p.nextToken() != com.fasterxml.jackson.core.JsonToken.END_OBJECT) {
                String field = p.currentName();
                p.nextToken();
                if (!"sourceMap".equals(field)) { p.skipChildren(); continue; }
                if (p.currentToken() != com.fasterxml.jackson.core.JsonToken.START_OBJECT) return null;
                while (p.nextToken() != com.fasterxml.jackson.core.JsonToken.END_OBJECT) {
                    String k = p.currentName();
                    p.nextToken();
                    if (key.equals(k)) {
                        return mapper.readValue(p, new com.fasterxml.jackson.core.type.TypeReference<List<String>>() {});
                    }
                    p.skipChildren();
                }
                return null; // "sourceMap" object closed without a match
            }
            return null; // no "sourceMap" field at all
        } catch (IOException e) {
            log.error("Failed to stream-read source line '{}' for '{}': {}", key, name, e.getMessage());
            return null;
        }
    }

    /**
     * MID-027 — the call-site-facing half of {@link #readSourceLines(String, String)}: takes
     * whatever {@link AnalysisResult} a caller already has (a freshly-crawled, fully in-memory one,
     * or a {@link LazyAnalysisResult} loaded from a persisted analysis) and answers one key without
     * ever forcing the whole {@code sourceMap} resident when it doesn't have to.
     *
     * <p>Only narrows the read when there is something to narrow: a plain {@code AnalysisResult}
     * (the live-crawl case — {@code sourceMap} is already fully built in memory, nothing to save)
     * and a {@code LazyAnalysisResult} whose source section is already loaded (some earlier caller
     * already paid the cost) both fall straight through to the existing {@code getSourceMap().get}
     * path, unchanged. Only the "not yet loaded" case takes the streamed single-key path.
     */
    public List<String> getSourceLines(AnalysisResult result, String key) {
        if (result == null || key == null) return null;
        if (result instanceof LazyAnalysisResult lazy && !lazy.isSourceLoaded()) {
            List<String> lines = readSourceLines(result.getName(), key);
            if (lines != null) return lines;
            // Not found via the streamed path — could be a genuinely missing key, or a
            // pre-split-file analysis this method's file layout assumption doesn't cover. Fall
            // through to the full load rather than reporting "not found" on a format mismatch.
        }
        Map<String, List<String>> map = result.getSourceMap();
        return map != null ? map.get(key) : null;
    }

    private void loadFromSplitFile(AnalysisResult target, String name, ArtifactKey file,
                                   AnalysisSection section) {
        try (InputStream in = openRead(file)) {
            switch (section) {
                case GRAPH -> {
                    GraphSnapshot gs = mapper.readValue(in, GraphSnapshot.class);
                    rebuildCallGraph(target, gs.nodes, gs.edges);
                }
                case SOURCE -> {
                    SourceSnapshot ss = mapper.readValue(in, SourceSnapshot.class);
                    target.setSourceMap(ss.sourceMap != null ? ss.sourceMap : new HashMap<>());
                }
                case OPS -> {
                    OpsSnapshot os = mapper.readValue(in, OpsSnapshot.class);
                    target.setTableOperations(os.tableOperations != null ? os.tableOperations : new LinkedHashMap<>());
                    target.setSequenceOperations(os.sequenceOperations != null ? os.sequenceOperations : new LinkedHashMap<>());
                    target.setJoinOperations(os.joinOperations != null ? os.joinOperations : new LinkedHashMap<>());
                    target.setCursorOperations(os.cursorOperations != null ? os.cursorOperations : new LinkedHashMap<>());
                }
                case UNITS -> {
                    UnitsSnapshot us = mapper.readValue(in, UnitsSnapshot.class);
                    target.setUnits(us.units != null ? us.units : new ArrayList<>());
                }
                case TABLE_META -> {
                    TableMetaSnapshot ts = mapper.readValue(in, TableMetaSnapshot.class);
                    target.setTableMetadata(ts.tableMetadata != null ? ts.tableMetadata : new HashMap<>());
                }
            }
            log.debug("Loaded section {} for '{}'", section, name);
        } catch (IOException e) {
            log.error("Failed to load section {} from {}: {}", section, file, e.getMessage());
        }
    }

    private void migrateAndLoadSection(AnalysisResult target, String name, AnalysisSection section) {
        Object folderLock = migrationLocks.computeIfAbsent(name, k -> new Object());
        synchronized (folderLock) {
            ArtifactKey splitFile = fileKey(name, section.filename);
            // One listing covers the split file plus all three migration sources.
            Map<ArtifactKey, ArtifactEntry> stat = statFolder(name);
            if (stat.containsKey(splitFile)) {
                loadFromSplitFile(target, name, splitFile, section);
                return;
            }
            ArtifactKey claudeFile = fileKey(name, "analysis_claude.json");
            ArtifactKey fullFile   = fileKey(name, "analysis.json");
            ArtifactKey apiIndex   = fileKey(name, "api/index.json");

            boolean hasClaude = stat.containsKey(claudeFile);
            if (hasClaude || stat.containsKey(fullFile)) {
                ArtifactKey fileToLoad = hasClaude ? claudeFile : fullFile;
                try {
                    log.info("Migrating '{}' to split files (first access)", name);
                    AnalysisSnapshot snap;
                    try (InputStream in = openRead(fileToLoad)) {
                        snap = mapper.readValue(in, AnalysisSnapshot.class);
                    }
                    writeSplitFiles(name, snap);
                    loadFromSplitFile(target, name, splitFile, section);
                } catch (IOException e) {
                    log.error("Failed to migrate {}: {}", fileToLoad, e.getMessage());
                }
            } else if (stat.containsKey(apiIndex) && section == AnalysisSection.GRAPH) {
                // api-export format: nodes are inline in api/index.json
                loadGraphFromApiIndex(target, name, apiIndex, stat);
            } else {
                log.debug("No loadable data for section {} in '{}'", section, name);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void loadGraphFromApiIndex(AnalysisResult target, String name, ArtifactKey apiIndex,
                                       Map<ArtifactKey, ArtifactEntry> stat) {
        try {
            Map<String, Object> idx;
            try (InputStream in = openRead(apiIndex)) {
                idx = mapper.readValue(in, LinkedHashMap.class);
            }
            CallGraph cg = new CallGraph();
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) idx.get("nodes");
            if (nodes != null) {
                for (Map<String, Object> n : nodes) {
                    CallGraphNode node = new CallGraphNode();
                    String nodeId = (String) n.getOrDefault("nodeId", "");
                    node.setId(nodeId);
                    node.setBaseId(nodeId);
                    node.setSchemaName((String) n.getOrDefault("schema", ""));
                    node.setPackageName((String) n.get("packageName"));
                    node.setProcedureName((String) n.getOrDefault("objectName", ""));
                    node.setStartLine(toInt(n.get("lineStart")));
                    node.setEndLine(toInt(n.get("lineEnd")));
                    // NOTE: this used to stat and fully parse api/nodes/<nodeId>.json here and
                    // then discard the result — the "merge any additional fields" was never
                    // written. Two I/O ops per node for nothing; over S3 that is a HEAD plus a
                    // GET for every node in the graph. Dropped, with no change in behaviour.
                    cg.addNode(node);
                }
            }
            // Load edges from edges.json if present
            ArtifactKey edgesFile = fileKey(name, "edges.json");
            if (stat.containsKey(edgesFile)) {
                List<Map<String, Object>> edges;
                try (InputStream in = openRead(edgesFile)) {
                    edges = mapper.readValue(in,
                        mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
                }
                for (Map<String, Object> e : edges) {
                    String from = (String) e.getOrDefault("caller", e.get("from"));
                    String to   = (String) e.getOrDefault("callee", e.get("to"));
                    if (from != null && to != null) {
                        CallEdge edge = new CallEdge(from, to, toInt(e.get("lineNumber")), false);
                        cg.addEdge(edge);
                    }
                }
            }
            target.setCallGraph(cg);
            log.info("Loaded call graph from api/index.json for '{}'", name);
        } catch (IOException e) {
            log.error("Failed to load api/index.json from '{}': {}", name, e.getMessage());
        }
    }

    private void rebuildCallGraph(AnalysisResult target, List<AnalysisSnapshot.NodeData> nodes, List<AnalysisSnapshot.EdgeData> edges) {
        CallGraph cg = new CallGraph();
        if (nodes != null) {
            for (AnalysisSnapshot.NodeData nd : nodes) {
                CallGraphNode n = new CallGraphNode();
                n.setId(nd.id);
                n.setBaseId(nd.baseId != null ? nd.baseId : nd.id);
                n.setSchemaName(nd.schemaName);
                n.setPackageName(nd.packageName);
                n.setProcedureName(nd.procedureName);
                if (nd.unitType != null) {
                    try { n.setUnitType(com.plsqlanalyzer.parser.model.PlsqlUnitType.valueOf(nd.unitType)); }
                    catch (Exception ignored) {}
                }
                n.setSourceFile(nd.sourceFile);
                n.setStartLine(nd.startLine);
                n.setEndLine(nd.endLine);
                n.setCallType(nd.callType);
                n.setPlaceholder(nd.placeholder);
                n.setParamSignature(nd.paramSignature);
                n.setParamCount(nd.paramCount);
                cg.addNode(n);
            }
        }
        if (edges != null) {
            for (AnalysisSnapshot.EdgeData ed : edges) {
                CallEdge e = new CallEdge(ed.from, ed.to, ed.lineNumber, ed.dynamicSql);
                e.setCallType(ed.callType);
                cg.addEdge(e);
            }
        }
        target.setCallGraph(cg);
    }

    LazyAnalysisResult createLazyResult(String name, Map<ArtifactKey, ArtifactEntry> stat) {
        LazyAnalysisResult result = new LazyAnalysisResult(name, this);
        ArtifactKey metaFile = fileKey(name, "metadata.json");
        ArtifactKey apiIndex = fileKey(name, "api/index.json");

        if (stat.containsKey(metaFile)) {
            try {
                Map<String, Object> meta;
                try (InputStream in = openRead(metaFile)) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> read = mapper.readValue(in, LinkedHashMap.class);
                    meta = read;
                }
                result.setMetadataCache(meta);
                result.setEntrySchema((String) meta.get("entrySchema"));
                result.setEntryObjectName((String) meta.get("entryObjectName"));
                result.setEntryObjectType((String) meta.get("entryObjectType"));
                result.setEntryProcedure((String) meta.get("entryProcedure"));
                Object ts = meta.get("timestamp");
                if (ts != null) result.setTimestamp(LocalDateTime.parse(ts.toString()));
                result.setProcedureCount(toInt(meta.get("procedureCount")));
                result.setFileCount(toInt(meta.get("fileCount")));
                result.setErrorCount(toInt(meta.get("errorCount")));
                result.setAnalysisMode((String) meta.get("analysisMode"));
                result.setClaudeIteration(toInt(meta.get("claudeIteration")));
                result.setClaudeEnrichedAt((String) meta.get("claudeEnrichedAt"));
                result.setVersion(toInt(meta.get("version")));
            } catch (IOException e) {
                log.error("Failed to read metadata for '{}': {}", name, e.getMessage());
            }
        } else if (stat.containsKey(apiIndex)) {
            // New api-export format — populate from api/index.json
            try {
                Map<String, Object> idx;
                try (InputStream in = openRead(apiIndex)) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> read = mapper.readValue(in, LinkedHashMap.class);
                    idx = read;
                }
                String entryPoint = (String) idx.getOrDefault("entryPoint", name);
                String schema     = (String) idx.getOrDefault("entrySchema", "");
                result.setEntrySchema(schema);
                result.setEntryObjectName(entryPoint);
                result.setEntryProcedure(entryPoint);
                result.setProcedureCount(toInt(idx.get("totalNodes")));
                result.setAnalysisMode("STATIC");
                // Synthesize a metadata cache so the summary endpoint returns useful data
                Map<String, Object> synth = new LinkedHashMap<>();
                synth.put("name",          name);
                synth.put("entrySchema",    schema);
                synth.put("entryObjectName", entryPoint);
                synth.put("entryProcedure",  entryPoint);
                synth.put("procedureCount", idx.getOrDefault("totalNodes", 0));
                synth.put("totalTables",    idx.getOrDefault("totalTables", 0));
                synth.put("totalEdges",     idx.getOrDefault("totalEdges", 0));
                synth.put("maxDepth",       idx.getOrDefault("maxDepth", 0));
                synth.put("totalLinesOfCode", idx.getOrDefault("totalLinesOfCode", 0));
                synth.put("crawlTimeMs",    idx.getOrDefault("crawlTimeMs", 0));
                synth.put("format",         "api-export");
                result.setMetadataCache(synth);
            } catch (IOException e) {
                log.error("Failed to read api/index.json for '{}': {}", name, e.getMessage());
            }
        }
        return result;
    }

    private static int toInt(Object o) {
        if (o == null) return 0;
        if (o instanceof Number n) return n.intValue();
        try { return Integer.parseInt(o.toString()); } catch (Exception e) { return 0; }
    }

    // ---- JSON serialization structure ----

    public static class AnalysisSnapshot {
        public String name;
        public int version;
        public LocalDateTime timestamp;
        public int fileCount;
        public int procedureCount;
        public int errorCount;
        public String entrySchema;
        public String entryObjectName;
        public String entryObjectType;
        public String entryProcedure;
        public String analysisMode;
        public int claudeIteration;
        public String claudeEnrichedAt;
        public Map<String, List<String>> sourceMap;
        public List<NodeData> nodes;
        public List<EdgeData> edges;
        public Map<String, TableOperationSummary> tableOperations;
        public Map<String, SequenceOperationSummary> sequenceOperations;
        public Map<String, JoinOperationSummary> joinOperations;
        public Map<String, CursorOperationSummary> cursorOperations;
        public Map<String, TableMetadata> tableMetadata;
        public List<PlsqlUnit> units;

        public static class NodeData {
            public String id;
            public String baseId;
            public String schemaName;
            public String packageName;
            public String procedureName;
            public String unitType;
            public String sourceFile;
            public int startLine;
            public int endLine;
            public String callType;
            public boolean placeholder;
            public String paramSignature;
            public int paramCount;
        }

        public static class EdgeData {
            public String from;
            public String to;
            public int lineNumber;
            public boolean dynamicSql;
            public String callType;
        }
    }
}
