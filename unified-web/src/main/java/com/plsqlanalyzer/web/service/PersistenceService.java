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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Saves/loads analysis results in named folders.
 * Structure: data/{name}/analysis.json + metadata.json
 * Each analysis gets a unique name like "OPUS_CORE_PKG_CUSTOMER_20260416_143022"
 */
public class PersistenceService {

    private static final Logger log = LoggerFactory.getLogger(PersistenceService.class);
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    private final ObjectMapper mapper;
    private final Path dataDir;

    public PersistenceService(String dataDirectory) {
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());
        this.mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        this.dataDir = Paths.get(dataDirectory);
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            log.error("Cannot create data dir: {}", e.getMessage());
        }
    }

    /** Store connection info used for an analysis result (Oracle JDBC, schemas). */
    public void storeConnections(String analysisName, Map<String, Object> info) {
        try {
            Path folder = dataDir.resolve(analysisName);
            Files.createDirectories(folder);
            mapper.writerWithDefaultPrettyPrinter()
                  .writeValue(folder.resolve("connections.json").toFile(), info);
        } catch (IOException e) {
            log.warn("Failed to store connections.json for {}: {}", analysisName, e.getMessage());
        }
    }

    /** Load connection info for an analysis result. */
    @SuppressWarnings("unchecked")
    public Map<String, Object> loadConnections(String analysisName) {
        Path file = dataDir.resolve(analysisName).resolve("connections.json");
        if (!Files.exists(file)) return null;
        try {
            return mapper.readValue(file.toFile(), Map.class);
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

        Path folder = dataDir.resolve(name);
        try {
            Files.createDirectories(folder);

            // Save full analysis data + split files for lazy loading
            AnalysisSnapshot snapshot = toSnapshot(result);
            mapper.writeValue(folder.resolve("analysis.json").toFile(), snapshot);
            writeSplitFiles(folder, snapshot);

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

            mapper.writeValue(folder.resolve("metadata.json").toFile(), meta);

            log.info("Saved analysis '{}' to {}", name, folder);
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
        if (name == null) return;
        Path folder = dataDir.resolve(name);

        try {
            Files.createDirectories(folder);

            // Rotate: current claude → prev
            Path claudeFile = folder.resolve("analysis_claude.json");
            Path claudePrev = folder.resolve("analysis_claude_prev.json");
            if (Files.exists(claudeFile)) {
                Files.move(claudeFile, claudePrev, StandardCopyOption.REPLACE_EXISTING);
                log.info("Rotated claude -> prev for '{}'", name);
            }

            // Save new claude version + update split files
            AnalysisSnapshot snapshot = toSnapshot(result);
            mapper.writeValue(claudeFile.toFile(), snapshot);
            writeSplitFiles(folder, snapshot);

            // Update metadata with claude info
            Path metaFile = folder.resolve("metadata.json");
            if (Files.exists(metaFile)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> meta = mapper.readValue(metaFile.toFile(), LinkedHashMap.class);
                meta.put("analysisMode", result.getAnalysisMode());
                meta.put("claudeIteration", result.getClaudeIteration());
                meta.put("claudeEnrichedAt", result.getClaudeEnrichedAt());
                meta.put("tableCount", result.getTableOperations() != null ? result.getTableOperations().size() : 0);
                meta.put("joinCount", result.getJoinOperations() != null ? result.getJoinOperations().size() : 0);
                meta.put("cursorCount", result.getCursorOperations() != null ? result.getCursorOperations().size() : 0);
                mapper.writeValue(metaFile.toFile(), meta);
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
        Path folder = dataDir.resolve(name);
        if (!Files.exists(folder)) return null;
        boolean hasAny = Files.exists(folder.resolve(AnalysisSection.GRAPH.filename))
                || Files.exists(folder.resolve("analysis_claude.json"))
                || Files.exists(folder.resolve("analysis.json"));
        if (!hasAny) return null;
        log.info("Creating lazy result for '{}'", name);
        return createLazyResult(folder, name);
    }

    /**
     * Load specifically the static (original) version — full load for Claude verification.
     */
    public AnalysisResult loadStatic(String name) {
        Path folder = dataDir.resolve(name);
        Path analysisFile = folder.resolve("analysis.json");
        if (!Files.exists(analysisFile)) return null;
        return loadFromFile(analysisFile, name);
    }

    /**
     * Load the previous Claude-enriched version (for revert).
     */
    public AnalysisResult loadClaudePrev(String name) {
        Path folder = dataDir.resolve(name);
        Path claudePrev = folder.resolve("analysis_claude_prev.json");
        if (!Files.exists(claudePrev)) return null;
        return loadFromFile(claudePrev, name);
    }

    /**
     * Revert: swap claude_prev → claude (user-initiated).
     */
    public boolean revertClaude(String name) {
        Path folder = dataDir.resolve(name);
        Path claudeFile = folder.resolve("analysis_claude.json");
        Path claudePrev = folder.resolve("analysis_claude_prev.json");
        if (!Files.exists(claudePrev)) return false;
        try {
            Files.move(claudePrev, claudeFile, StandardCopyOption.REPLACE_EXISTING);
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
        Path folder = dataDir.resolve(name);
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("name", name);
        info.put("hasStatic", Files.exists(folder.resolve("analysis.json")));
        info.put("hasClaude", Files.exists(folder.resolve("analysis_claude.json")));
        info.put("hasClaudePrev", Files.exists(folder.resolve("analysis_claude_prev.json")));
        try {
            Path claudeFile = folder.resolve("analysis_claude.json");
            if (Files.exists(claudeFile)) {
                info.put("claudeSizeKb", Files.size(claudeFile) / 1024);
            }
            Path staticFile = folder.resolve("analysis.json");
            if (Files.exists(staticFile)) {
                info.put("staticSizeKb", Files.size(staticFile) / 1024);
            }
        } catch (IOException e) { /* ignore */ }
        return info;
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
        try (Stream<Path> dirs = Files.list(dataDir)) {
            dirs.filter(Files::isDirectory)
                .filter(d -> Files.exists(d.resolve("metadata.json")))
                .sorted((a, b) -> {
                    try {
                        return Files.getLastModifiedTime(b.resolve("metadata.json"))
                                .compareTo(Files.getLastModifiedTime(a.resolve("metadata.json")));
                    } catch (IOException e) { return 0; }
                })
                .forEach(d -> {
                    try {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> meta = mapper.readValue(
                                d.resolve("metadata.json").toFile(), LinkedHashMap.class);
                        // Add file size info
                        Path analysisFile = d.resolve("analysis.json");
                        if (Files.exists(analysisFile)) {
                            meta.put("sizeKb", Files.size(analysisFile) / 1024);
                        }
                        // Add Claude version info
                        meta.put("hasClaude", Files.exists(d.resolve("analysis_claude.json")));
                        meta.put("hasClaudePrev", Files.exists(d.resolve("analysis_claude_prev.json")));
                        if (Files.exists(d.resolve("analysis_claude.json"))) {
                            meta.put("claudeSizeKb", Files.size(d.resolve("analysis_claude.json")) / 1024);
                        }
                        meta.put("hasConnections", Files.exists(d.resolve("connections.json")));
                        history.add(meta);
                    } catch (IOException e) {
                        log.debug("Skipping folder {}: {}", d.getFileName(), e.getMessage());
                    }
                });
        } catch (IOException e) {
            log.error("Error listing history: {}", e.getMessage());
        }
        return history;
    }

    /**
     * Delete a saved analysis by name (recursive — handles subdirectories like claude/).
     */
    public boolean delete(String name) {
        Path folder = dataDir.resolve(name);
        if (!Files.exists(folder) || !Files.isDirectory(folder)) return false;
        try {
            // Walk the tree depth-first and delete everything
            try (Stream<Path> walk = Files.walk(folder)) {
                walk.sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                    });
            }
            log.info("Deleted analysis '{}'", name);
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

    private AnalysisResult loadFromFile(Path file, String name) {
        try {
            AnalysisSnapshot snap = mapper.readValue(file.toFile(), AnalysisSnapshot.class);
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

    private void writeSplitFiles(Path folder, AnalysisSnapshot snapshot) {
        try {
            GraphSnapshot gs = new GraphSnapshot();
            gs.nodes = snapshot.nodes;
            gs.edges = snapshot.edges;
            writeAtomic(folder.resolve(AnalysisSection.GRAPH.filename), gs);

            SourceSnapshot ss = new SourceSnapshot();
            ss.sourceMap = snapshot.sourceMap;
            writeAtomic(folder.resolve(AnalysisSection.SOURCE.filename), ss);

            OpsSnapshot os = new OpsSnapshot();
            os.tableOperations = snapshot.tableOperations;
            os.sequenceOperations = snapshot.sequenceOperations;
            os.joinOperations = snapshot.joinOperations;
            os.cursorOperations = snapshot.cursorOperations;
            writeAtomic(folder.resolve(AnalysisSection.OPS.filename), os);

            UnitsSnapshot us = new UnitsSnapshot();
            us.units = snapshot.units;
            writeAtomic(folder.resolve(AnalysisSection.UNITS.filename), us);

            TableMetaSnapshot ts = new TableMetaSnapshot();
            ts.tableMetadata = snapshot.tableMetadata;
            writeAtomic(folder.resolve(AnalysisSection.TABLE_META.filename), ts);

            log.info("Wrote split files for '{}'", folder.getFileName());
        } catch (Exception e) {
            log.error("Failed to write split files for '{}': {}", folder.getFileName(), e.getMessage());
        }
    }

    private void writeAtomic(Path target, Object value) throws IOException {
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        mapper.writeValue(tmp.toFile(), value);
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
    }

    private final ConcurrentHashMap<String, Object> migrationLocks = new ConcurrentHashMap<>();

    void loadSection(AnalysisResult target, Path folder, AnalysisSection section) {
        Path splitFile = folder.resolve(section.filename);
        if (Files.exists(splitFile)) {
            loadFromSplitFile(target, splitFile, section);
        } else {
            migrateAndLoadSection(target, folder, section);
        }
    }

    private void loadFromSplitFile(AnalysisResult target, Path file, AnalysisSection section) {
        try {
            switch (section) {
                case GRAPH -> {
                    GraphSnapshot gs = mapper.readValue(file.toFile(), GraphSnapshot.class);
                    rebuildCallGraph(target, gs.nodes, gs.edges);
                }
                case SOURCE -> {
                    SourceSnapshot ss = mapper.readValue(file.toFile(), SourceSnapshot.class);
                    target.setSourceMap(ss.sourceMap != null ? ss.sourceMap : new HashMap<>());
                }
                case OPS -> {
                    OpsSnapshot os = mapper.readValue(file.toFile(), OpsSnapshot.class);
                    target.setTableOperations(os.tableOperations != null ? os.tableOperations : new LinkedHashMap<>());
                    target.setSequenceOperations(os.sequenceOperations != null ? os.sequenceOperations : new LinkedHashMap<>());
                    target.setJoinOperations(os.joinOperations != null ? os.joinOperations : new LinkedHashMap<>());
                    target.setCursorOperations(os.cursorOperations != null ? os.cursorOperations : new LinkedHashMap<>());
                }
                case UNITS -> {
                    UnitsSnapshot us = mapper.readValue(file.toFile(), UnitsSnapshot.class);
                    target.setUnits(us.units != null ? us.units : new ArrayList<>());
                }
                case TABLE_META -> {
                    TableMetaSnapshot ts = mapper.readValue(file.toFile(), TableMetaSnapshot.class);
                    target.setTableMetadata(ts.tableMetadata != null ? ts.tableMetadata : new HashMap<>());
                }
            }
            log.debug("Loaded section {} for '{}'", section, file.getParent().getFileName());
        } catch (IOException e) {
            log.error("Failed to load section {} from {}: {}", section, file, e.getMessage());
        }
    }

    private void migrateAndLoadSection(AnalysisResult target, Path folder, AnalysisSection section) {
        Object folderLock = migrationLocks.computeIfAbsent(folder.toString(), k -> new Object());
        synchronized (folderLock) {
            Path splitFile = folder.resolve(section.filename);
            if (Files.exists(splitFile)) {
                loadFromSplitFile(target, splitFile, section);
                return;
            }
            Path claudeFile = folder.resolve("analysis_claude.json");
            Path fullFile = folder.resolve("analysis.json");
            Path fileToLoad = Files.exists(claudeFile) ? claudeFile : fullFile;
            if (!Files.exists(fileToLoad)) {
                log.warn("No analysis file found in '{}'", folder.getFileName());
                return;
            }
            try {
                log.info("Migrating '{}' to split files (first access)", folder.getFileName());
                AnalysisSnapshot snap = mapper.readValue(fileToLoad.toFile(), AnalysisSnapshot.class);
                writeSplitFiles(folder, snap);
                loadFromSplitFile(target, folder.resolve(section.filename), section);
            } catch (IOException e) {
                log.error("Failed to migrate {}: {}", fileToLoad, e.getMessage());
            }
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

    LazyAnalysisResult createLazyResult(Path folder, String name) {
        LazyAnalysisResult result = new LazyAnalysisResult(name, folder, this);
        Path metaFile = folder.resolve("metadata.json");
        if (Files.exists(metaFile)) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> meta = mapper.readValue(metaFile.toFile(), LinkedHashMap.class);
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
