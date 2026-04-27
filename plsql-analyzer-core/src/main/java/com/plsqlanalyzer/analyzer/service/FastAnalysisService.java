package com.plsqlanalyzer.analyzer.service;

import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.*;
import com.plsqlanalyzer.parser.model.*;
import com.plsqlanalyzer.parser.service.OracleDictionaryService;
import com.plsqlanalyzer.parser.service.OracleDictionaryService.*;
import com.plsqlanalyzer.parser.service.PlSqlAnalyzerParserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Fast dependency analysis that works in bulk.
 * Phase 1: BFS dependency discovery via ALL_DEPENDENCIES (batch per level)
 * Phase 2: Bulk source fetch for all discovered objects (1-2 queries)
 * Phase 3: Parallel ANTLR parse (CPU only)
 * Phase 4: Discover new call targets from parsed code, iterate if new objects found
 * Phase 5: Build call graph + collect tables/sequences/joins/cursors
 * Phase 6: Bulk resolve table metadata + triggers (2-3 queries)
 */
public class FastAnalysisService {

    private static final Logger log = LoggerFactory.getLogger(FastAnalysisService.class);
    private static final int MAX_DEPTH = 20;
    private static final int MAX_ITERATIONS = 5;

    private final OracleDictionaryService dictService;
    private final PlSqlAnalyzerParserService parserService;
    private Supplier<Connection> connectionSupplier;

    public FastAnalysisService(OracleDictionaryService dictService, PlSqlAnalyzerParserService parserService) {
        this.dictService = dictService;
        this.parserService = parserService;
    }

    public void setConnectionSupplier(Supplier<Connection> supplier) {
        this.connectionSupplier = supplier;
    }

    public AnalysisResult analyze(Connection conn, String schema, String objectName,
                                   String objectType, String procedureName,
                                   List<String> allSchemas,
                                   Consumer<String> progress) {
        long totalStart = System.currentTimeMillis();
        String entrySchema = schema.toUpperCase();
        String entryObject = objectName.toUpperCase();

        // ============ PHASE 1: BFS dependency discovery ============
        progress.accept("[1/5] Discovering dependencies (BFS bulk)...");
        long phaseStart = System.currentTimeMillis();

        Set<String> objectsToFetch = new LinkedHashSet<>();
        objectsToFetch.add(entrySchema.toUpperCase() + "." + entryObject.toUpperCase());

        try {
            Map<String, List<DependencyRecord>> depGraph =
                    dictService.getRecursiveDependencies(conn, entrySchema, entryObject, MAX_DEPTH);

            for (var entry : depGraph.entrySet()) {
                objectsToFetch.add(entry.getKey().toUpperCase());
                for (DependencyRecord dep : entry.getValue()) {
                    String type = dep.referencedType();
                    if (type != null) {
                        String upper = type.toUpperCase();
                        if ("PACKAGE".equals(upper) || "PACKAGE BODY".equals(upper)
                                || "PROCEDURE".equals(upper) || "FUNCTION".equals(upper)
                                || "TRIGGER".equals(upper)) {
                            objectsToFetch.add(dep.referencedOwner().toUpperCase() + "." + dep.referencedName().toUpperCase());
                        }
                    }
                }
            }
        } catch (SQLException e) {
            log.error("Dependency discovery failed: {}", e.getMessage());
        }

        progress.accept("[1/5] Found " + objectsToFetch.size() + " objects in "
                + (System.currentTimeMillis() - phaseStart) + "ms");

        // ============ PHASE 2: Bulk source fetch ============
        progress.accept("[2/5] Bulk fetching source for " + objectsToFetch.size() + " objects...");
        phaseStart = System.currentTimeMillis();

        Map<String, List<String>> sourceMap = bulkFetchAllSource(conn, objectsToFetch, allSchemas);
        log.info("[TIMING] Phase 2 — bulk source fetch: {}ms, {} sources found",
                System.currentTimeMillis() - phaseStart, sourceMap.size());

        progress.accept("[2/5] Fetched " + sourceMap.size() + " sources in "
                + (System.currentTimeMillis() - phaseStart) + "ms");

        // ============ PHASE 3: Parallel parse ============
        progress.accept("[3/5] Parsing " + sourceMap.size() + " sources in parallel...");
        phaseStart = System.currentTimeMillis();

        List<PlsqlUnit> allUnits = parallelParse(sourceMap);
        log.info("[TIMING] Phase 3 — parse: {}ms, {} units, {} procs",
                System.currentTimeMillis() - phaseStart, allUnits.size(),
                allUnits.stream().mapToInt(u -> u.getProcedures().size()).sum());

        // ============ PHASE 3b: Iterative discovery — parse finds new call targets ============
        Set<String> knownObjects = new HashSet<>(sourceMap.keySet());
        for (int iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
            Set<String> newTargets = discoverNewTargets(allUnits, knownObjects, allSchemas);
            if (newTargets.isEmpty()) break;

            progress.accept("[3/5] Iteration " + (iteration + 1) + ": found " + newTargets.size() + " new call targets, fetching...");
            Map<String, List<String>> newSources = bulkFetchAllSource(conn, newTargets, allSchemas);

            if (newSources.isEmpty()) break;

            List<PlsqlUnit> newUnits = parallelParse(newSources);
            allUnits.addAll(newUnits);
            sourceMap.putAll(newSources);
            knownObjects.addAll(newSources.keySet());

            log.info("[TIMING] Phase 3b iteration {} — {} new sources, {} new units",
                    iteration + 1, newSources.size(), newUnits.size());
        }

        int totalProcs = allUnits.stream().mapToInt(u -> u.getProcedures().size()).sum();
        progress.accept("[3/5] Parse complete: " + allUnits.size() + " units, " + totalProcs + " procedures in "
                + (System.currentTimeMillis() - phaseStart) + "ms");

        // ============ PHASE 4: Build call graph ============
        progress.accept("[4/5] Building call graph...");
        phaseStart = System.currentTimeMillis();

        Map<String, String> pkgSchemaLookup = new HashMap<>();
        for (String key : sourceMap.keySet()) {
            String[] kp = key.split("\\.", 2);
            if (kp.length == 2) pkgSchemaLookup.putIfAbsent(kp[1].toUpperCase(), kp[0].toUpperCase());
        }
        for (PlsqlUnit u : allUnits) {
            if (u.getSchemaName() != null && u.getName() != null) {
                pkgSchemaLookup.putIfAbsent(u.getName().toUpperCase(), u.getSchemaName().toUpperCase());
            }
        }

        CallGraphBuilder graphBuilder = new CallGraphBuilder();
        CallGraph callGraph = graphBuilder.buildGraph(allUnits, pkgSchemaLookup);

        for (CallEdge edge : callGraph.getAllEdges()) {
            edge.setCallType(determineCallType(edge, entrySchema, entryObject, callGraph));
        }
        for (CallGraphNode node : callGraph.getAllNodes()) {
            node.setCallType(determineNodeCallType(node, entrySchema, entryObject));
        }

        log.info("[TIMING] Phase 4 — call graph: {}ms, {} nodes, {} edges",
                System.currentTimeMillis() - phaseStart, callGraph.getNodeCount(), callGraph.getEdgeCount());

        // ============ PHASE 5: Collect tables + bulk metadata + triggers ============
        progress.accept("[5/5] Collecting tables, triggers, metadata (bulk)...");
        phaseStart = System.currentTimeMillis();

        TableOperationCollector tableCollector = new TableOperationCollector();
        SequenceOperationCollector sequenceCollector = new SequenceOperationCollector();
        JoinOperationCollector joinCollector = new JoinOperationCollector();
        CursorOperationCollector cursorCollector = new CursorOperationCollector();

        Map<String, TableOperationSummary> tableOps = tableCollector.collect(allUnits);
        Map<String, SequenceOperationSummary> sequenceOps = sequenceCollector.collect(allUnits);
        Map<String, JoinOperationSummary> joinOps = joinCollector.collect(allUnits);
        Map<String, CursorOperationSummary> cursorOps = cursorCollector.collect(allUnits);

        List<String> allTableNames = tableOps.values().stream()
                .map(s -> s.getTableName().toUpperCase()).toList();

        // Bulk resolve owners + types + triggers in parallel
        bulkResolveTableInfo(conn, tableOps, allTableNames, allSchemas, entrySchema, progress);

        // Bulk fetch trigger info
        bulkResolveTriggers(conn, tableOps, allTableNames, allSchemas, progress);

        // Bulk fetch metadata (columns, constraints, indexes)
        Map<String, TableMetadata> tableMetadata = bulkFetchMetadata(conn, tableOps, progress);

        log.info("[TIMING] Phase 5 — tables+triggers+metadata: {}ms, {} tables, {} triggers",
                System.currentTimeMillis() - phaseStart, tableOps.size(),
                tableOps.values().stream().mapToInt(t -> t.getTriggers().size()).sum());

        // ============ Assemble result ============
        AnalysisResult result = new AnalysisResult();
        result.setCallGraph(callGraph);
        result.setTableOperations(tableOps);
        result.setSequenceOperations(sequenceOps);
        result.setJoinOperations(joinOps);
        result.setCursorOperations(cursorOps);
        result.setTableMetadata(tableMetadata);
        result.setUnits(allUnits);
        result.setSourceMap(sourceMap);
        result.setTimestamp(LocalDateTime.now());
        result.setVersion(1);
        result.setFileCount(sourceMap.size());
        result.setProcedureCount(totalProcs);
        result.setEntrySchema(entrySchema);
        result.setEntryObjectName(entryObject);
        result.setEntryObjectType(objectType);
        result.setEntryProcedure(procedureName);
        result.setAnalysisMode("STATIC");
        result.setErrorCount((int) allUnits.stream()
                .filter(u -> !u.getParseErrors().isEmpty()).count());

        long elapsed = System.currentTimeMillis() - totalStart;
        progress.accept("Analysis complete in " + elapsed + "ms: " + totalProcs + " procs, "
                + callGraph.getNodeCount() + " nodes, " + callGraph.getEdgeCount() + " edges, "
                + tableOps.size() + " tables");
        log.info("[TIMING] TOTAL: {}ms | {} procs, {} nodes, {} edges, {} tables",
                elapsed, totalProcs, callGraph.getNodeCount(), callGraph.getEdgeCount(), tableOps.size());

        return result;
    }

    // ---- Bulk source fetch: single query per type ----

    private Map<String, List<String>> bulkFetchAllSource(Connection conn, Set<String> objectKeys, List<String> allSchemas) {
        Map<String, List<String>> result = new ConcurrentHashMap<>();
        if (objectKeys.isEmpty()) return result;

        // Build (owner, name) pairs
        List<String[]> pairs = new ArrayList<>();
        for (String key : objectKeys) {
            String[] parts = key.split("\\.", 2);
            if (parts.length == 2) pairs.add(new String[]{parts[0], parts[1]});
        }

        // Fetch PACKAGE BODY + PACKAGE + PROCEDURE + FUNCTION + TRIGGER in one bulk query per type
        String[] types = {"PACKAGE BODY", "PACKAGE", "PROCEDURE", "FUNCTION", "TRIGGER"};
        Set<String> found = new HashSet<>();

        for (String type : types) {
            List<String[]> toFetch = new ArrayList<>();
            for (String[] pair : pairs) {
                String key = pair[0] + "." + pair[1];
                if (!found.contains(key)) toFetch.add(pair);
            }
            if (toFetch.isEmpty()) break;

            Map<String, List<String>> batch = bulkFetchSourceByType(conn, toFetch, type);
            for (var entry : batch.entrySet()) {
                if (!entry.getValue().isEmpty()) {
                    result.put(entry.getKey(), entry.getValue());
                    found.add(entry.getKey());
                }
            }
        }

        return result;
    }

    private Map<String, List<String>> bulkFetchSourceByType(Connection conn, List<String[]> pairs, String objectType) {
        Map<String, List<String>> result = new LinkedHashMap<>();
        int chunkSize = 200;

        for (int i = 0; i < pairs.size(); i += chunkSize) {
            List<String[]> chunk = pairs.subList(i, Math.min(i + chunkSize, pairs.size()));
            StringBuilder sql = new StringBuilder(
                    "SELECT owner, name, text FROM all_source WHERE type = ? AND (");
            for (int j = 0; j < chunk.size(); j++) {
                if (j > 0) sql.append(" OR ");
                sql.append("(owner = ? AND name = ?)");
            }
            sql.append(") ORDER BY owner, name, line");

            try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                int idx = 1;
                ps.setString(idx++, objectType);
                for (String[] pair : chunk) {
                    ps.setString(idx++, pair[0]);
                    ps.setString(idx++, pair[1]);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getString("OWNER") + "." + rs.getString("NAME");
                        String text = rs.getString("TEXT");
                        if (text != null && text.endsWith("\n")) text = text.substring(0, text.length() - 1);
                        result.computeIfAbsent(key, k -> new ArrayList<>()).add(text != null ? text : "");
                    }
                }
            } catch (SQLException e) {
                log.warn("Bulk source fetch failed for type {}: {}", objectType, e.getMessage());
            }
        }
        return result;
    }

    // ---- Parallel parse ----

    private List<PlsqlUnit> parallelParse(Map<String, List<String>> sourceMap) {
        List<PlsqlUnit> units = Collections.synchronizedList(new ArrayList<>());
        int threads = Math.min(sourceMap.size(), Runtime.getRuntime().availableProcessors());
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, threads));
        List<Future<?>> futures = new ArrayList<>();

        for (var entry : sourceMap.entrySet()) {
            futures.add(pool.submit(() -> {
                String[] parts = entry.getKey().split("\\.", 2);
                String owner = parts[0];
                String name = parts.length > 1 ? parts[1] : parts[0];
                try {
                    PlsqlUnit unit = parserService.parseLines(entry.getValue(), owner, name);
                    if (unit != null) units.add(unit);
                } catch (Exception e) {
                    log.debug("Parse failed for {}: {}", entry.getKey(), e.getMessage());
                }
            }));
        }

        for (Future<?> f : futures) {
            try { f.get(30, TimeUnit.SECONDS); } catch (Exception e) { /* skip */ }
        }
        pool.shutdown();
        return units;
    }

    // ---- Iterative discovery: find new call targets not yet fetched ----

    private Set<String> discoverNewTargets(List<PlsqlUnit> units, Set<String> known, List<String> schemas) {
        Set<String> newTargets = new LinkedHashSet<>();
        Set<String> knownUpper = new HashSet<>();
        for (String k : known) knownUpper.add(k.toUpperCase());

        for (PlsqlUnit unit : units) {
            String unitSchema = unit.getSchemaName() != null ? unit.getSchemaName().toUpperCase() : null;
            for (PlsqlProcedure proc : unit.getProcedures()) {
                for (ProcedureCall call : proc.getCalls()) {
                    String pkg = call.getPackageName();
                    String callSchema = call.getSchemaName();

                    if (pkg != null && !pkg.isEmpty()) {
                        // Qualified call: PKG.PROC or SCHEMA.PKG.PROC
                        String targetSchema = callSchema != null ? callSchema.toUpperCase() : unitSchema;
                        if (targetSchema != null) {
                            String key = targetSchema + "." + pkg.toUpperCase();
                            if (!knownUpper.contains(key)) newTargets.add(key);
                        }
                        // Also try all schemas if no schema specified
                        if (callSchema == null) {
                            for (String s : schemas) {
                                String key = s.toUpperCase() + "." + pkg.toUpperCase();
                                if (!knownUpper.contains(key)) newTargets.add(key);
                            }
                        }
                    }
                }
            }
        }
        return newTargets;
    }

    // ---- Bulk table resolution ----

    private void bulkResolveTableInfo(Connection conn, Map<String, TableOperationSummary> tableOps,
                                       List<String> allTableNames, List<String> allSchemas,
                                       String entrySchema, Consumer<String> progress) {
        try {
            List<String> toResolve = new ArrayList<>();
            for (TableOperationSummary s : tableOps.values()) {
                if (s.getSchemaName() == null) toResolve.add(s.getTableName());
            }

            // Parallel owner + type resolution
            if (connectionSupplier != null) {
                ExecutorService pool = Executors.newFixedThreadPool(2);
                Future<Map<String, String>> ownerFuture = pool.submit(() -> {
                    try (Connection c = connectionSupplier.get()) {
                        return dictService.bulkResolveTableOwners(c, toResolve, allSchemas);
                    }
                });
                Future<Map<String, String>> typeFuture = pool.submit(() -> {
                    try (Connection c = connectionSupplier.get()) {
                        return dictService.bulkResolveTableTypes(c, allTableNames, allSchemas);
                    }
                });

                Map<String, String> ownerMap = ownerFuture.get(60, TimeUnit.SECONDS);
                Map<String, String> typeMap = typeFuture.get(60, TimeUnit.SECONDS);
                pool.shutdown();

                applyTableInfo(tableOps, ownerMap, typeMap, entrySchema);
            } else {
                Map<String, String> ownerMap = dictService.bulkResolveTableOwners(conn, toResolve, allSchemas);
                Map<String, String> typeMap = dictService.bulkResolveTableTypes(conn, allTableNames, allSchemas);
                applyTableInfo(tableOps, ownerMap, typeMap, entrySchema);
            }
        } catch (Exception e) {
            log.warn("Bulk table info resolve failed: {}", e.getMessage());
        }
    }

    private void applyTableInfo(Map<String, TableOperationSummary> tableOps,
                                 Map<String, String> ownerMap, Map<String, String> typeMap,
                                 String entrySchema) {
        for (TableOperationSummary s : tableOps.values()) {
            if (s.getSchemaName() == null) {
                s.setSchemaName(ownerMap.get(s.getTableName().toUpperCase()));
            }
            s.setExternal(!entrySchema.equalsIgnoreCase(s.getSchemaName()));
            s.setAccessCount(s.getAccessDetails().size());
            String type = typeMap.get(s.getTableName().toUpperCase());
            s.setTableType(type != null ? type : "TABLE");
        }
    }

    // ---- Bulk trigger resolution ----

    private void bulkResolveTriggers(Connection conn, Map<String, TableOperationSummary> tableOps,
                                      List<String> allTableNames, List<String> allSchemas,
                                      Consumer<String> progress) {
        try {
            Map<String, List<TriggerRecord>> triggerMap = dictService.bulkGetTriggers(conn, allTableNames, allSchemas);

            // Collect all unique triggers
            Set<String> uniqueTriggers = new LinkedHashSet<>();
            List<Map.Entry<TableOperationSummary, List<TriggerRecord>>> triggerWork = new ArrayList<>();

            for (TableOperationSummary summary : tableOps.values()) {
                List<TriggerRecord> triggers = triggerMap.getOrDefault(summary.getTableName().toUpperCase(), List.of());
                if (!triggers.isEmpty()) {
                    triggerWork.add(Map.entry(summary, triggers));
                    for (TriggerRecord tr : triggers) {
                        if ("ENABLED".equalsIgnoreCase(tr.status())) {
                            uniqueTriggers.add(tr.owner().toUpperCase() + "." + tr.triggerName().toUpperCase());
                        }
                    }
                }
            }

            if (uniqueTriggers.isEmpty()) return;

            // Bulk fetch all trigger source
            List<String[]> ownerTriggerPairs = new ArrayList<>();
            for (String key : uniqueTriggers) {
                String[] parts = key.split("\\.", 2);
                ownerTriggerPairs.add(new String[]{parts[0], parts[1]});
            }
            Map<String, List<String>> triggerSourceMap = dictService.bulkFetchTriggerSource(conn, ownerTriggerPairs);

            progress.accept("[5/5] Parsing " + triggerSourceMap.size() + " trigger sources...");

            // Parse triggers in parallel (CPU only, no DB)
            TriggerResolver resolver = new TriggerResolver(dictService, parserService);
            ExecutorService pool = Executors.newFixedThreadPool(Math.min(8, Math.max(1, triggerWork.size())));
            List<Future<?>> futures = new ArrayList<>();
            for (var entry : triggerWork) {
                futures.add(pool.submit(() -> {
                    resolver.resolveTriggersFromRecords(null, entry.getKey(), entry.getValue(), triggerSourceMap);
                }));
            }
            for (Future<?> f : futures) {
                try { f.get(120, TimeUnit.SECONDS); } catch (Exception e) { /* skip */ }
            }
            pool.shutdown();
        } catch (Exception e) {
            log.warn("Bulk trigger resolve failed: {}", e.getMessage());
        }
    }

    // ---- Bulk metadata fetch ----

    private Map<String, TableMetadata> bulkFetchMetadata(Connection conn,
                                                          Map<String, TableOperationSummary> tableOps,
                                                          Consumer<String> progress) {
        Map<String, TableMetadata> metadata = new ConcurrentHashMap<>();
        List<String[]> ownerTablePairs = new ArrayList<>();
        for (TableOperationSummary s : tableOps.values()) {
            if (s.getSchemaName() != null && s.getTableName() != null) {
                ownerTablePairs.add(new String[]{s.getSchemaName().toUpperCase(), s.getTableName().toUpperCase()});
            }
        }

        if (ownerTablePairs.isEmpty()) return metadata;
        progress.accept("[5/5] Bulk fetching metadata for " + ownerTablePairs.size() + " tables...");

        try {
            Map<String, List<ColumnInfo>> allColumns;
            Map<String, List<ConstraintInfo>> allConstraints;
            Map<String, List<IndexInfo>> allIndexes;

            if (connectionSupplier != null) {
                ExecutorService pool = Executors.newFixedThreadPool(3);
                Future<Map<String, List<ColumnInfo>>> colFuture = pool.submit(() -> {
                    try (Connection c = connectionSupplier.get()) {
                        return dictService.bulkFetchColumns(c, ownerTablePairs);
                    }
                });
                Future<Map<String, List<ConstraintInfo>>> conFuture = pool.submit(() -> {
                    try (Connection c = connectionSupplier.get()) {
                        return dictService.bulkFetchConstraints(c, ownerTablePairs);
                    }
                });
                Future<Map<String, List<IndexInfo>>> idxFuture = pool.submit(() -> {
                    try (Connection c = connectionSupplier.get()) {
                        return dictService.bulkFetchIndexes(c, ownerTablePairs);
                    }
                });

                allColumns = colFuture.get(120, TimeUnit.SECONDS);
                allConstraints = conFuture.get(120, TimeUnit.SECONDS);
                allIndexes = idxFuture.get(120, TimeUnit.SECONDS);
                pool.shutdown();
            } else {
                allColumns = dictService.bulkFetchColumns(conn, ownerTablePairs);
                allConstraints = dictService.bulkFetchConstraints(conn, ownerTablePairs);
                allIndexes = dictService.bulkFetchIndexes(conn, ownerTablePairs);
            }

            // Assemble TableMetadata
            for (String[] pair : ownerTablePairs) {
                String key = pair[0] + "." + pair[1];
                TableOperationSummary summary = tableOps.get(pair[1]);
                boolean isView = summary != null && "VIEW".equals(summary.getTableType());

                metadata.put(pair[1], new TableMetadata(
                        pair[0], pair[1], isView, null,
                        allColumns.getOrDefault(key, List.of()),
                        allConstraints.getOrDefault(key, List.of()),
                        allIndexes.getOrDefault(key, List.of())
                ));
            }
        } catch (Exception e) {
            log.warn("Bulk metadata fetch failed: {}", e.getMessage());
        }

        return metadata;
    }

    // ---- Call type determination ----

    private String determineCallType(CallEdge edge, String entrySchema, String entryPkg, CallGraph graph) {
        if (edge.isDynamicSql()) return "DYNAMIC";
        CallGraphNode toNode = graph.getNode(edge.getToNodeId());
        if (toNode == null) return "EXTERNAL";
        if (toNode.getUnitType() == PlsqlUnitType.TRIGGER) return "TRIGGER";
        String nodeSchema = toNode.getSchemaName();
        String nodePkg = toNode.getPackageName();
        if (entrySchema.equalsIgnoreCase(nodeSchema) && entryPkg.equalsIgnoreCase(nodePkg)) return "INTERNAL";
        return "EXTERNAL";
    }

    private String determineNodeCallType(CallGraphNode node, String entrySchema, String entryPkg) {
        if (node.getUnitType() == PlsqlUnitType.TRIGGER) return "TRIGGER";
        if (entrySchema.equalsIgnoreCase(node.getSchemaName())
                && entryPkg.equalsIgnoreCase(node.getPackageName())) return "INTERNAL";
        return "EXTERNAL";
    }
}
