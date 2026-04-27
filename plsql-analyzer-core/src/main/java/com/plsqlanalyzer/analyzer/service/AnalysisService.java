package com.plsqlanalyzer.analyzer.service;

import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.CallEdge;
import com.plsqlanalyzer.analyzer.model.CallGraphNode;
import com.plsqlanalyzer.analyzer.model.CursorOperationSummary;
import com.plsqlanalyzer.analyzer.model.JoinOperationSummary;
import com.plsqlanalyzer.analyzer.model.SequenceOperationSummary;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import com.plsqlanalyzer.parser.model.*;
import com.plsqlanalyzer.parser.service.OracleDictionaryService;
import com.plsqlanalyzer.parser.service.OracleDictionaryService.*;
import com.plsqlanalyzer.parser.service.PlSqlAnalyzerParserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Orchestrates PL/SQL analysis from database sources.
 * Flow: fetch dependencies -> fetch source -> parse -> build call graph -> collect tables -> resolve triggers
 */
public class AnalysisService {

    private static final Logger log = LoggerFactory.getLogger(AnalysisService.class);
    private static final int MAX_DEPENDENCY_DEPTH = 20;

    // Configurable thread pool sizes (can be set before analyzeFromDb)
    private int sourceFetchThreads = 8;
    private int triggerThreads = 4;
    private int metadataThreads = 4;

    private final PlSqlAnalyzerParserService parserService;
    private final OracleDictionaryService dictionaryService;
    private final CallGraphBuilder callGraphBuilder;
    private final TableOperationCollector tableCollector;
    private final SequenceOperationCollector sequenceCollector;
    private final JoinOperationCollector joinCollector;
    private final CursorOperationCollector cursorCollector;
    private final TriggerResolver triggerResolver;
    private volatile AnalysisResult latestResult;
    private volatile Supplier<Connection> connectionSupplier;

    public AnalysisService() {
        this.parserService = new PlSqlAnalyzerParserService();
        this.dictionaryService = new OracleDictionaryService();
        this.callGraphBuilder = new CallGraphBuilder();
        this.tableCollector = new TableOperationCollector();
        this.sequenceCollector = new SequenceOperationCollector();
        this.joinCollector = new JoinOperationCollector();
        this.cursorCollector = new CursorOperationCollector();
        this.triggerResolver = new TriggerResolver(dictionaryService, parserService);
    }

    /**
     * Analyze a PL/SQL object from the database with full recursive dependency resolution.
     *
     * @param conn           JDBC connection
     * @param username       schema/owner (e.g., OPUS_CORE)
     * @param objectName     object name (e.g., PKG_CUSTOMER)
     * @param objectType     PACKAGE, PROCEDURE, FUNCTION, TRIGGER
     * @param procedureName  specific procedure within package (null = whole package)
     * @param allSchemas     all configured schemas for cross-schema resolution
     * @param progressCallback optional callback for progress updates
     * @return analysis result with call graph, table operations, source map
     */
    public AnalysisResult analyzeFromDb(Connection conn, String username, String objectName,
                                         String objectType, String procedureName,
                                         List<String> allSchemas,
                                         Consumer<String> progressCallback) {
        log.info("Starting analysis: {}.{} (type={}, proc={})", username, objectName, objectType, procedureName);
        long start = System.currentTimeMillis();
        long stepStart;

        // ============ STEP 1: Dependencies ============
        progress(progressCallback, "[1/6] Fetching dependencies for " + username + "." + objectName + "...");
        stepStart = System.currentTimeMillis();

        Map<String, List<DependencyRecord>> depGraph;
        try {
            depGraph = dictionaryService.getRecursiveDependencies(conn, username, objectName, MAX_DEPENDENCY_DEPTH);
            progress(progressCallback, "[1/6] Found " + depGraph.size() + " dependent objects in " + (System.currentTimeMillis() - stepStart) + "ms");
            log.info("[TIMING] Step 1 — dependencies: {}ms, found {} objects",
                    System.currentTimeMillis() - stepStart, depGraph.size());
        } catch (Exception e) {
            log.error("Failed to fetch dependencies: {}", e.getMessage(), e);
            depGraph = new LinkedHashMap<>();
        }

        // ============ STEP 2: Source fetch ============
        progress(progressCallback, "[2/6] Preparing source fetch...");
        stepStart = System.currentTimeMillis();

        Map<String, List<String>> sourceMap = new LinkedHashMap<>();
        Set<String> objectsToFetch = new LinkedHashSet<>();
        objectsToFetch.add(username.toUpperCase() + "." + objectName.toUpperCase());

        for (Map.Entry<String, List<DependencyRecord>> entry : depGraph.entrySet()) {
            objectsToFetch.add(entry.getKey().toUpperCase());
            for (DependencyRecord dep : entry.getValue()) {
                if (isPLSQLType(dep.referencedType())) {
                    objectsToFetch.add(dep.referencedOwner().toUpperCase() + "." + dep.referencedName().toUpperCase());
                }
            }
        }

        // Build a type hint map from dependency info to avoid trying all 5 types
        Map<String, String> typeHints = new HashMap<>();
        for (Map.Entry<String, List<DependencyRecord>> entry : depGraph.entrySet()) {
            for (DependencyRecord dep : entry.getValue()) {
                String depKey = dep.referencedOwner().toUpperCase() + "." + dep.referencedName().toUpperCase();
                typeHints.putIfAbsent(depKey, dep.referencedType());
            }
        }

        progress(progressCallback, "[2/6] Fetching source for " + objectsToFetch.size() + " objects (" + (connectionSupplier != null ? "parallel" : "sequential") + ")...");

        var fetchAttemptCounter = new java.util.concurrent.atomic.AtomicInteger(0);
        ConcurrentHashMap<String, List<String>> concurrentSourceMap = new ConcurrentHashMap<>();

        // Parallel source fetch using connection pool
        int srcThreads = Math.min(objectsToFetch.size(), connectionSupplier != null ? sourceFetchThreads : 1);
        if (srcThreads > 1 && connectionSupplier != null) {
            ExecutorService srcPool = Executors.newFixedThreadPool(srcThreads);
            List<Future<?>> srcFutures = new ArrayList<>();

            for (String objKey : objectsToFetch) {
                srcFutures.add(srcPool.submit(() -> {
                    String[] parts = objKey.split("\\.", 2);
                    String owner = parts[0];
                    String name = parts[1];
                    try (Connection srcConn = connectionSupplier.get()) {
                        String hint = typeHints.get(objKey);
                        List<String> typesToTry;
                        if ("PACKAGE BODY".equals(hint) || "PACKAGE".equals(hint)) {
                            typesToTry = List.of("PACKAGE BODY", "PACKAGE");
                        } else if ("PROCEDURE".equals(hint)) {
                            typesToTry = List.of("PROCEDURE");
                        } else if ("FUNCTION".equals(hint)) {
                            typesToTry = List.of("FUNCTION");
                        } else if ("TRIGGER".equals(hint)) {
                            typesToTry = List.of("TRIGGER");
                        } else {
                            typesToTry = List.of("PACKAGE BODY", "PACKAGE", "PROCEDURE", "FUNCTION", "TRIGGER");
                        }
                        for (String type : typesToTry) {
                            fetchAttemptCounter.incrementAndGet();
                            List<String> source = dictionaryService.fetchSource(srcConn, owner, name, type);
                            if (!source.isEmpty()) {
                                concurrentSourceMap.put(objKey, source);
                                break;
                            }
                        }
                    } catch (Exception e) {
                        log.warn("Failed to fetch source for {}: {}", objKey, e.getMessage());
                    }
                }));
            }
            for (Future<?> f : srcFutures) {
                try { f.get(60, TimeUnit.SECONDS); } catch (Exception e) { log.warn("Source fetch task failed: {}", e.getMessage()); }
            }
            srcPool.shutdown();
        } else {
            // Single connection fallback
            for (String objKey : objectsToFetch) {
                String[] parts = objKey.split("\\.", 2);
                String owner = parts[0];
                String name = parts[1];
                try {
                    String hint = typeHints.get(objKey);
                    List<String> typesToTry;
                    if ("PACKAGE BODY".equals(hint) || "PACKAGE".equals(hint)) {
                        typesToTry = List.of("PACKAGE BODY", "PACKAGE");
                    } else if ("PROCEDURE".equals(hint)) {
                        typesToTry = List.of("PROCEDURE");
                    } else if ("FUNCTION".equals(hint)) {
                        typesToTry = List.of("FUNCTION");
                    } else if ("TRIGGER".equals(hint)) {
                        typesToTry = List.of("TRIGGER");
                    } else {
                        typesToTry = List.of("PACKAGE BODY", "PACKAGE", "PROCEDURE", "FUNCTION", "TRIGGER");
                    }
                    for (String type : typesToTry) {
                        fetchAttemptCounter.incrementAndGet();
                        List<String> source = dictionaryService.fetchSource(conn, owner, name, type);
                        if (!source.isEmpty()) {
                            concurrentSourceMap.put(objKey, source);
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.warn("Failed to fetch source for {}: {}", objKey, e.getMessage());
                }
            }
        }
        sourceMap.putAll(concurrentSourceMap);

        log.info("[TIMING] Step 2 — source fetch: {}ms, {} objects to fetch, {} SQL queries, {} sources found ({} threads)",
                System.currentTimeMillis() - stepStart, objectsToFetch.size(), fetchAttemptCounter.get(),
                sourceMap.size(), srcThreads);

        // ============ STEP 3: Parse (parallel) ============
        progress(progressCallback, "[3/6] Parsing sources (" + sourceMap.size() + " objects) in parallel...");
        stepStart = System.currentTimeMillis();

        List<PlsqlUnit> allUnits = Collections.synchronizedList(new ArrayList<>());
        var errorCounter = new java.util.concurrent.atomic.AtomicInteger(0);

        // Parse in parallel using a thread pool — parsing is CPU-bound, safe to parallelize
        int threads = Math.min(sourceMap.size(), Runtime.getRuntime().availableProcessors());
        ExecutorService parsePool = Executors.newFixedThreadPool(Math.max(1, threads));
        List<Future<?>> parseFutures = new ArrayList<>();

        for (Map.Entry<String, List<String>> entry : sourceMap.entrySet()) {
            parseFutures.add(parsePool.submit(() -> {
                String[] parts = entry.getKey().split("\\.", 2);
                try {
                    PlsqlUnit unit = parserService.parseLines(entry.getValue(), parts[0], parts[1]);
                    allUnits.add(unit);
                    errorCounter.addAndGet(unit.getParseErrors().size());
                } catch (Exception e) {
                    log.error("Failed to parse {}: {}", entry.getKey(), e.getMessage());
                    errorCounter.incrementAndGet();
                }
            }));
        }

        // Wait for all parse tasks
        for (Future<?> f : parseFutures) {
            try { f.get(60, TimeUnit.SECONDS); }
            catch (Exception e) { log.warn("Parse task failed: {}", e.getMessage()); }
        }
        parsePool.shutdown();
        int errorCount = errorCounter.get();

        log.info("[TIMING] Step 3 — parsing: {}ms, {} units, {} errors (parallel, {} threads)",
                System.currentTimeMillis() - stepStart, allUnits.size(), errorCount, threads);

        // ============ STEP 4: Call graph ============
        progress(progressCallback, "[4/6] Building call graph...");
        stepStart = System.currentTimeMillis();

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

        CallGraph callGraph = callGraphBuilder.buildGraph(allUnits, pkgSchemaLookup);

        String entrySchema = username.toUpperCase();
        String entryPkg = objectName.toUpperCase();
        for (CallEdge edge : callGraph.getAllEdges()) {
            edge.setCallType(determineCallType(edge, entrySchema, entryPkg, callGraph));
        }
        for (CallGraphNode node : callGraph.getAllNodes()) {
            node.setCallType(determineNodeCallType(node, entrySchema, entryPkg));
        }

        log.info("[TIMING] Step 4 — call graph: {}ms, {} nodes, {} edges",
                System.currentTimeMillis() - stepStart, callGraph.getNodeCount(), callGraph.getEdgeCount());

        // ============ STEP 5: Table ops + triggers + metadata (BULK queries) ============
        progress(progressCallback, "[5/6] Collecting table operations, triggers & metadata...");
        stepStart = System.currentTimeMillis();

        Map<String, TableOperationSummary> tableOps = tableCollector.collect(allUnits);
        Map<String, SequenceOperationSummary> sequenceOps = sequenceCollector.collect(allUnits);
        Map<String, JoinOperationSummary> joinOps = joinCollector.collect(allUnits);
        Map<String, CursorOperationSummary> cursorOps = cursorCollector.collect(allUnits);
        long tableCollectTime = System.currentTimeMillis() - stepStart;

        // --- 5a + 5b: Bulk resolve owners AND types in PARALLEL ---
        progress(progressCallback, "[5/6] Step 5a+5b: Resolving owners + types for " + tableOps.size() + " tables (parallel)...");
        long resolveStart = System.currentTimeMillis();
        List<String> tableNamesToResolve = new ArrayList<>();
        for (TableOperationSummary summary : tableOps.values()) {
            if (summary.getSchemaName() == null) {
                tableNamesToResolve.add(summary.getTableName());
            }
        }
        List<String> allTableNames = tableOps.values().stream()
                .map(s -> s.getTableName().toUpperCase()).toList();

        // Run owner resolve and type resolve simultaneously
        if (connectionSupplier != null) {
            ExecutorService resolvePool = Executors.newFixedThreadPool(2);
            Future<Map<String, String>> ownerFuture = resolvePool.submit(() -> {
                try (Connection c = connectionSupplier.get()) {
                    return dictionaryService.bulkResolveTableOwners(c, tableNamesToResolve, allSchemas);
                }
            });
            Future<Map<String, String>> typeFuture = resolvePool.submit(() -> {
                try (Connection c = connectionSupplier.get()) {
                    return dictionaryService.bulkResolveTableTypes(c, allTableNames, allSchemas);
                }
            });

            Map<String, String> ownerMap = Collections.emptyMap();
            Map<String, String> typeMap = Collections.emptyMap();
            try { ownerMap = ownerFuture.get(60, TimeUnit.SECONDS); }
            catch (Exception e) {
                log.warn("Parallel owner resolve failed, retrying on main: {}", e.getMessage());
                try { ownerMap = dictionaryService.bulkResolveTableOwners(conn, tableNamesToResolve, allSchemas); }
                catch (Exception e2) { log.error("Bulk owner resolve failed (retry): {}", e2.getMessage()); }
            }
            try { typeMap = typeFuture.get(60, TimeUnit.SECONDS); }
            catch (Exception e) {
                log.warn("Parallel type resolve failed, retrying on main: {}", e.getMessage());
                try { typeMap = dictionaryService.bulkResolveTableTypes(conn, allTableNames, allSchemas); }
                catch (Exception e2) { log.error("Bulk type resolve failed (retry): {}", e2.getMessage()); }
            }
            resolvePool.shutdown();

            for (TableOperationSummary summary : tableOps.values()) {
                if (summary.getSchemaName() == null) {
                    summary.setSchemaName(ownerMap.get(summary.getTableName().toUpperCase()));
                }
                summary.setExternal(!entrySchema.equalsIgnoreCase(summary.getSchemaName()));
                summary.setAccessCount(summary.getAccessDetails().size());
                String type = typeMap.get(summary.getTableName().toUpperCase());
                summary.setTableType(type != null ? type : "TABLE");
            }
        } else {
            // Sequential fallback on main connection
            try {
                Map<String, String> ownerMap = dictionaryService.bulkResolveTableOwners(conn, tableNamesToResolve, allSchemas);
                for (TableOperationSummary summary : tableOps.values()) {
                    if (summary.getSchemaName() == null) {
                        summary.setSchemaName(ownerMap.get(summary.getTableName().toUpperCase()));
                    }
                    summary.setExternal(!entrySchema.equalsIgnoreCase(summary.getSchemaName()));
                    summary.setAccessCount(summary.getAccessDetails().size());
                }
            } catch (Exception e) { log.error("Bulk owner resolve failed: {}", e.getMessage()); }
            try {
                Map<String, String> typeMap = dictionaryService.bulkResolveTableTypes(conn, allTableNames, allSchemas);
                for (TableOperationSummary summary : tableOps.values()) {
                    String type = typeMap.get(summary.getTableName().toUpperCase());
                    summary.setTableType(type != null ? type : "TABLE");
                }
            } catch (Exception e) { log.error("Bulk type resolve failed: {}", e.getMessage()); }
        }
        long resolveEnd = System.currentTimeMillis();
        log.info("[TIMING] Step 5a+5b — parallel owner+type resolve: {}ms for {} tables",
                resolveEnd - resolveStart, tableOps.size());

        // --- 5c: Bulk fetch triggers + source (2 queries total, no per-trigger DB calls) ---
        progress(progressCallback, "[5/6] Step 5c: Fetching triggers + source (bulk)...");
        long triggerStart = System.currentTimeMillis();
        try {
            Map<String, List<TriggerRecord>> triggerMap = dictionaryService.bulkGetTriggers(conn, allTableNames, allSchemas);

            List<Map.Entry<TableOperationSummary, List<TriggerRecord>>> triggerWork = new ArrayList<>();
            Set<String> uniqueTriggers = new LinkedHashSet<>();
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

            // Bulk fetch ALL trigger source in one query
            List<String[]> ownerTriggerPairs = new ArrayList<>();
            for (String key : uniqueTriggers) {
                String[] parts = key.split("\\.", 2);
                ownerTriggerPairs.add(new String[]{parts[0], parts[1]});
            }
            Map<String, List<String>> triggerSourceMap = Collections.emptyMap();
            if (!ownerTriggerPairs.isEmpty()) {
                progress(progressCallback, "[5/6] Step 5c: Bulk fetching source for " + ownerTriggerPairs.size() + " triggers...");
                triggerSourceMap = dictionaryService.bulkFetchTriggerSource(conn, ownerTriggerPairs);
                log.info("Bulk fetched source for {}/{} triggers in {}ms",
                        triggerSourceMap.size(), ownerTriggerPairs.size(),
                        System.currentTimeMillis() - triggerStart);
            }

            // Parse trigger sources in parallel (CPU-only, no DB calls)
            Map<String, List<String>> trigSrcMap = triggerSourceMap;
            ExecutorService trigPool = Executors.newFixedThreadPool(Math.min(8, Math.max(1, triggerWork.size())));
            List<Future<?>> trigFutures = new ArrayList<>();
            for (var entry : triggerWork) {
                trigFutures.add(trigPool.submit(() -> {
                    triggerResolver.resolveTriggersFromRecords(null, entry.getKey(), entry.getValue(), trigSrcMap);
                }));
            }
            for (Future<?> f : trigFutures) {
                try { f.get(120, TimeUnit.SECONDS); } catch (Exception e) { log.warn("Trigger parse timed out: {}", e.getMessage()); }
            }
            trigPool.shutdown();
            log.info("[TIMING] Step 5c — triggers: {}ms, {} groups, {} unique triggers, {} sources fetched",
                    System.currentTimeMillis() - triggerStart, triggerWork.size(), uniqueTriggers.size(), triggerSourceMap.size());
        } catch (Exception e) {
            log.error("Bulk trigger resolve failed: {}", e.getMessage());
        }

        // --- 5d: Bulk fetch metadata (3 queries instead of 2110) ---
        progress(progressCallback, "[5/6] Step 5d: Fetching columns, constraints, indexes (4 parallel connections)...");
        long metadataStart = System.currentTimeMillis();
        Map<String, TableMetadata> tableMetadataMap = new ConcurrentHashMap<>();
        List<String[]> ownerTablePairs = new ArrayList<>();
        for (TableOperationSummary summary : tableOps.values()) {
            if (summary.getSchemaName() != null && summary.getTableName() != null) {
                ownerTablePairs.add(new String[]{summary.getSchemaName().toUpperCase(), summary.getTableName().toUpperCase()});
            }
        }

        {
            progress(progressCallback, "[5/6] Fetching metadata for " + ownerTablePairs.size() + " tables...");
            // Fetch columns, constraints, indexes in parallel — each isolated so one failure doesn't kill others
            Map<String, List<ColumnInfo>> allColumns = Collections.emptyMap();
            Map<String, List<ConstraintInfo>> allConstraints = Collections.emptyMap();
            Map<String, List<IndexInfo>> allIndexes = Collections.emptyMap();
            Map<String, String> allViewDefs = new HashMap<>();

            // Identify views for definition fetch
            List<String[]> viewPairs = new ArrayList<>();
            for (TableOperationSummary summary : tableOps.values()) {
                if ("VIEW".equals(summary.getTableType()) && summary.getSchemaName() != null) {
                    viewPairs.add(new String[]{summary.getSchemaName().toUpperCase(), summary.getTableName().toUpperCase()});
                }
            }

            if (connectionSupplier != null) {
                // All 4 metadata queries run simultaneously
                ExecutorService metaPool = Executors.newFixedThreadPool(metadataThreads);
                Future<Map<String, List<ColumnInfo>>> colFuture = metaPool.submit(() -> {
                    try (Connection c = connectionSupplier.get()) {
                        return dictionaryService.bulkFetchColumns(c, ownerTablePairs);
                    }
                });
                Future<Map<String, List<ConstraintInfo>>> conFuture = metaPool.submit(() -> {
                    try (Connection c = connectionSupplier.get()) {
                        return dictionaryService.bulkFetchConstraints(c, ownerTablePairs);
                    }
                });
                Future<Map<String, List<IndexInfo>>> idxFuture = metaPool.submit(() -> {
                    try (Connection c = connectionSupplier.get()) {
                        return dictionaryService.bulkFetchIndexes(c, ownerTablePairs);
                    }
                });
                Future<Map<String, String>> viewFuture = metaPool.submit(() -> {
                    try (Connection c = connectionSupplier.get()) {
                        return dictionaryService.fetchViewDefinitionsIndividual(c, viewPairs);
                    }
                });

                // Wait for all 4 simultaneously
                try { allColumns = colFuture.get(90, TimeUnit.SECONDS); }
                catch (Exception e) { log.warn("Parallel column fetch failed, will retry on main: {}", e.getMessage()); }
                try { allConstraints = conFuture.get(90, TimeUnit.SECONDS); }
                catch (Exception e) { log.warn("Parallel constraint fetch failed, will retry on main: {}", e.getMessage()); }
                try { allIndexes = idxFuture.get(90, TimeUnit.SECONDS); }
                catch (Exception e) { log.warn("Parallel index fetch failed, will retry on main: {}", e.getMessage()); }
                try { allViewDefs = viewFuture.get(120, TimeUnit.SECONDS); }
                catch (Exception e) { log.warn("Parallel view def fetch failed, will retry on main: {}", e.getMessage()); }
                metaPool.shutdown();

                // Retry any failures on the main connection (sequential, guaranteed to work)
                if (allColumns.isEmpty() && !ownerTablePairs.isEmpty()) {
                    log.info("Retrying column fetch on main connection...");
                    try { allColumns = dictionaryService.bulkFetchColumns(conn, ownerTablePairs); }
                    catch (Exception e) { log.error("Bulk column fetch failed (retry): {}", e.getMessage()); }
                }
                if (allConstraints.isEmpty() && !ownerTablePairs.isEmpty()) {
                    log.info("Retrying constraint fetch on main connection...");
                    try { allConstraints = dictionaryService.bulkFetchConstraints(conn, ownerTablePairs); }
                    catch (Exception e) { log.error("Bulk constraint fetch failed (retry): {}", e.getMessage()); }
                }
                if (allIndexes.isEmpty() && !ownerTablePairs.isEmpty()) {
                    log.info("Retrying index fetch on main connection...");
                    try { allIndexes = dictionaryService.bulkFetchIndexes(conn, ownerTablePairs); }
                    catch (Exception e) { log.error("Bulk index fetch failed (retry): {}", e.getMessage()); }
                }
                if (allViewDefs.isEmpty() && !viewPairs.isEmpty()) {
                    log.info("Retrying view def fetch on main connection...");
                    try { allViewDefs = dictionaryService.fetchViewDefinitionsIndividual(conn, viewPairs); }
                    catch (Exception e) { log.error("View definition fetch failed (retry): {}", e.getMessage()); }
                }
            } else {
                // No connection supplier — sequential on main connection
                try { allColumns = dictionaryService.bulkFetchColumns(conn, ownerTablePairs); }
                catch (Exception e) { log.error("Bulk column fetch failed: {}", e.getMessage()); }
                try { allConstraints = dictionaryService.bulkFetchConstraints(conn, ownerTablePairs); }
                catch (Exception e) { log.error("Bulk constraint fetch failed: {}", e.getMessage()); }
                try { allIndexes = dictionaryService.bulkFetchIndexes(conn, ownerTablePairs); }
                catch (Exception e) { log.error("Bulk index fetch failed: {}", e.getMessage()); }
                try { allViewDefs = dictionaryService.fetchViewDefinitionsIndividual(conn, viewPairs); }
                catch (Exception e) { log.error("View definition fetch failed: {}", e.getMessage()); }
            }

            // Assemble TableMetadata objects
            for (TableOperationSummary summary : tableOps.values()) {
                if (summary.getSchemaName() == null || summary.getTableName() == null) continue;
                String key = summary.getSchemaName().toUpperCase() + "." + summary.getTableName().toUpperCase();
                boolean isView = "VIEW".equals(summary.getTableType()) || "MATERIALIZED VIEW".equals(summary.getTableType());
                String viewDef = allViewDefs.getOrDefault(key, null);
                TableMetadata meta = new TableMetadata(
                        summary.getSchemaName().toUpperCase(),
                        summary.getTableName().toUpperCase(),
                        isView,
                        viewDef,
                        allColumns.getOrDefault(key, List.of()),
                        allConstraints.getOrDefault(key, List.of()),
                        allIndexes.getOrDefault(key, List.of()));
                tableMetadataMap.put(summary.getTableName().toUpperCase(), meta);
            }
            log.info("[TIMING] Step 5d — bulk metadata (cols+constraints+idx+views): {}ms for {} tables ({} views)",
                    System.currentTimeMillis() - metadataStart, tableMetadataMap.size(), viewPairs.size());
        }

        log.info("[TIMING] Step 5 TOTAL: {}ms (collect={}ms, owners+types={}ms, triggers={}ms, metadata={}ms)",
                System.currentTimeMillis() - stepStart, tableCollectTime,
                resolveEnd - resolveStart,
                System.currentTimeMillis() - triggerStart,
                System.currentTimeMillis() - metadataStart);

        // ============ STEP 6: Finalize ============
        progress(progressCallback, "[6/6] Finalizing — " + tableOps.size() + " tables, " + tableMetadataMap.size() + " with metadata, saving...");
        stepStart = System.currentTimeMillis();

        int procedureCount = allUnits.stream().mapToInt(u -> u.getProcedures().size()).sum();

        AnalysisResult result = new AnalysisResult();
        result.setCallGraph(callGraph);
        result.setTableOperations(tableOps);
        result.setSequenceOperations(sequenceOps);
        result.setJoinOperations(joinOps);
        result.setCursorOperations(cursorOps);
        result.setTableMetadata(tableMetadataMap);
        result.setUnits(allUnits);
        result.setSourceMap(sourceMap);
        result.setFileCount(sourceMap.size());
        result.setProcedureCount(procedureCount);
        result.setErrorCount(errorCount);
        result.setEntrySchema(username.toUpperCase());
        result.setEntryObjectName(objectName.toUpperCase());
        result.setEntryObjectType(objectType != null ? objectType.toUpperCase() : null);
        result.setEntryProcedure(procedureName);

        this.latestResult = result;

        long elapsed = System.currentTimeMillis() - start;
        log.info("[TIMING] Step 6 — finalize: {}ms", System.currentTimeMillis() - stepStart);
        log.info("[TIMING] TOTAL: {}ms | deps=step1, source=step2, parse=step3, graph=step4, triggers=step5, save=step6",
                elapsed);
        log.info("Analysis complete: {} sources, {} procedures, {} tables, {} sequences, {} joins, {} errors in {}ms",
                sourceMap.size(), procedureCount, tableOps.size(), sequenceOps.size(), joinOps.size(), errorCount, elapsed);

        return result;
    }

    /**
     * Get reverse references (used-by) for an object.
     */
    public List<DependencyRecord> getReferences(Connection conn, String owner, String objectName) {
        try {
            return dictionaryService.getReferences(conn, owner, objectName);
        } catch (Exception e) {
            log.error("Failed to fetch references for {}.{}: {}", owner, objectName, e.getMessage());
            return Collections.emptyList();
        }
    }

    public AnalysisResult getLatestResult() {
        return latestResult;
    }

    public void setLatestResult(AnalysisResult result) {
        this.latestResult = result;
    }

    public OracleDictionaryService getDictionaryService() {
        return dictionaryService;
    }

    public PlSqlAnalyzerParserService getParserService() {
        return parserService;
    }

    /**
     * Set a connection supplier for parallel DB operations.
     * Each call to supplier.get() should return a new JDBC connection.
     */
    public void setConnectionSupplier(Supplier<Connection> supplier) {
        this.connectionSupplier = supplier;
    }

    public void setSourceFetchThreads(int threads) { this.sourceFetchThreads = Math.max(1, threads); }
    public void setTriggerThreads(int threads) { this.triggerThreads = Math.max(1, threads); }
    public void setMetadataThreads(int threads) { this.metadataThreads = Math.max(1, threads); }

    private String determineCallType(CallEdge edge, String entrySchema, String entryPkg, CallGraph graph) {
        if (edge.isDynamicSql()) return "DYNAMIC";
        CallGraphNode target = graph.getNode(edge.getToNodeId());
        if (target != null && target.getUnitType() == PlsqlUnitType.TRIGGER) return "TRIGGER";
        String toId = edge.getToNodeId().toUpperCase();
        String prefix = entrySchema + "." + entryPkg;
        if (toId.equals(prefix) || toId.startsWith(prefix + ".")) return "INTERNAL";
        return "EXTERNAL";
    }

    private String determineNodeCallType(CallGraphNode node, String entrySchema, String entryPkg) {
        if (node.getUnitType() == PlsqlUnitType.TRIGGER) return "TRIGGER";
        String id = node.getId().toUpperCase();
        String prefix = entrySchema + "." + entryPkg;
        if (id.equals(prefix) || id.startsWith(prefix + ".")) return "INTERNAL";
        return "EXTERNAL";
    }

    private boolean isPLSQLType(String objectType) {
        if (objectType == null) return false;
        return switch (objectType.toUpperCase()) {
            case "PACKAGE", "PACKAGE BODY", "PROCEDURE", "FUNCTION", "TRIGGER" -> true;
            default -> false;
        };
    }

    private void progress(Consumer<String> callback, String message) {
        if (callback != null) callback.accept(message);
        log.info(message);
    }
}
