package com.plsql.parser.flow;

import com.plsql.parser.PlSqlParserEngine;
import com.plsql.parser.model.*;
import com.plsql.parser.multidb.Db2ProcedureParser;
import com.plsql.parser.multidb.MySqlProcedureParser;
import com.plsql.parser.multidb.PostgresProcedureParser;
import com.plsql.parser.multidb.SqlServerProcedureParser;
import com.plsql.parser.multidb.SourceFetchStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.RequestScopedArtifactStore;
import com.analyzer.storage.sso.CapturedSsoContext;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Main orchestrator for recursive dependency analysis with flow-ordered output.
 *
 * Given an entry point (e.g. "PG_AC_EINVOICE.PC_IRBM_SUBMISSION" or "FN_SOME_FUNC"),
 * it downloads the source from the database, parses it, finds external calls,
 * and recursively crawls each dependency using BFS for flow ordering.
 */
public class DependencyCrawler {

    private static final Logger log = LoggerFactory.getLogger(DependencyCrawler.class);

    private final PlSqlParserEngine engine;
    private final SourceDownloader downloader;
    private final SchemaResolver schemaResolver;

    private int maxDepth = -1;
    private long timeoutPerEntryMs = 120_000;
    // Fan-out bounds for the per-level parallel work (see setCatalogParallelism/setSourceParallelism).
    private int catalogParallelism = 8;
    private int sourceParallelism = 8;
    private int catalogBatchSize = 500;
    private ArtifactStore store;
    private ArtifactKey outputKey;
    private String outputFolderName;
    private NodeParseCache nodeCache;
    private ParseResultCache parseResultCache;
    /**
     * Analysis-level idempotency registry (the third gate of the central fetch decision). Null
     * unless a persistent output namespace is configured. See {@link AnalysisRegistry} — it is
     * PER-ANALYSIS (under {@code outputKey}), per-user via the routed store, never global.
     */
    private AnalysisRegistry registry;
    /**
     * Force-refresh: when true the registry gate is bypassed and every reachable object is
     * re-fetched/re-analysed, even when its LAST_DDL is unchanged and the prior run was complete.
     * Wired from {@code --force-refresh} on the CLI and the analyze endpoint's form field.
     */
    private boolean forceRefresh = false;

    /**
     * THE FRESH-DEFAULT SWITCH. When {@code false} (the default) the crawl is 100% fresh: no
     * cross-run cache reads (persistent source cache, trigger cache, catalog snapshot) and no
     * registry skips — every reachable object is fetched and parsed anew. When {@code true} (the
     * user ticked "Fast re-run (skip unchanged)") the existing cache/registry paths are used to
     * skip unchanged objects. Writes always run in both modes, so a fast run has data to read.
     *
     * <p>The two flags are orthogonal: {@code fastReRun} decides whether the caches are CONSULTED
     * at all; {@code forceRefresh} forces re-analysis even in fast mode.
     */
    private boolean fastReRun = false;

    /**
     * When {@code false} (the default), a crawl that produces ZERO nodes is a failure: the writer
     * refuses to persist an empty analysis. This is the guard against the live incident where a
     * dead database (ORA-12541 "no listener") yielded 0 sources and the pipeline happily wrote a
     * 0-node analysis OVER a populated one of the same identity. Set {@code true} only for callers
     * that genuinely want an empty result written (see {@code app.parser.allow-empty-analysis}).
     */
    private boolean allowEmptyAnalysis = false;

    /**
     * The object type the caller asked for (e.g. {@code PROCEDURE}, {@code PACKAGE}), or null when
     * it was not supplied. Purely advisory: it never selects the fetch path (the downloader already
     * tries every relevant type), it is what lets the depth-0 lookup explain itself — and what tells
     * the package fallback that the caller did <em>not</em> already ask for a package.
     */
    private String entryObjectType;

    // Cache of parse results: "OBJECT_NAME" -> ParseResult
    private final Map<String, ParseResult> parseCache = new ConcurrentHashMap<>();

    // Package SPEC variable declarations, keyed by upper-case package name — the estate globals
    // facet's declaration list. The crawler parses the package BODY (its members come from it), but
    // a package's globals are declared in the SPEC (the body only assigns them), so the spec is
    // parsed once per package here and its packageVariables routed to the writer's dedicated
    // artifact. Cached so many members of one package cost one spec parse.
    private final Map<String, List<VariableInfo>> packageSpecVarsCache = new ConcurrentHashMap<>();

    // Crawled routine identity -> node id, populated as each node is generated. The DB-discovered
    // trigger path runs AFTER the crawl and its body's callees arrive unresolved (the trigger is a
    // catalog artefact, not a crawled node), so this lets it stamp a crawled target's node id onto
    // the call for the UI cross-link. Keys: OBJECT_NAME (upper) for standalone routines and
    // PACKAGE.SUBPROGRAM (upper) for package members; both are recorded per node.
    private final Map<String, String> crawledNodeIds = new ConcurrentHashMap<>();

    // Source-parsed TRIGGER node id keyed SCHEMA.NAME (upper). The post-crawl catalog-trigger pass
    // must not emit a SECOND node for a trigger that the BFS already crawled from source: this map
    // is the dedup guard, so such a trigger stays ONE node (source-parsed wins the body, the catalog
    // record supplies status/triggerType through api/tables/index.json).
    private final Map<String, String> crawledTriggerNodeIds = new ConcurrentHashMap<>();

    // Visited set to avoid infinite loops: "PACKAGE.SUBPROGRAM" or "STANDALONE_NAME"
    private final Set<String> visited = Collections.synchronizedSet(new LinkedHashSet<>());

    // Per-node DML records {fromNodeId, schemaUpper, tableUpper, opUpper} for every reachable
    // node — used after the crawl to build additive FIRES_TRIGGER edges from a routine to the
    // triggers on the tables it writes. (Additive: does not affect normal CALL edges.)
    private final List<String[]> dmlRefs = Collections.synchronizedList(new ArrayList<>());

    // Track which source objects have already had their parse errors reported so that
    // multiple subprograms from the same package don't repeat the same errors N times.
    private final Set<String> parseErrorsReported = Collections.synchronizedSet(new HashSet<>());

    // Names confirmed absent from ALL_OBJECTS (cursor vars, collection types, etc.)
    // Populated by a single batched ALL_OBJECTS query the first time a name has no source.
    // Any name in this set is skipped instantly — zero DB round-trips per node.
    private final Set<String> knownMissingObjects = Collections.synchronizedSet(new HashSet<>());
    // Names queued for the next batch check (accumulated before the batch fires)
    private final Set<String> pendingMissingCheck = Collections.synchronizedSet(new HashSet<>());
    // Name -> canonical Oracle object_type returned by the bulk check (e.g. "PACKAGE BODY", "PROCEDURE").
    // Used by nodeCache peek to probe exactly the right type in 1 call instead of 2 blind probes.
    private final Map<String, String> knownObjectTypes = new ConcurrentHashMap<>();

    // Fix C: unknown external call names deferred for one batched ALL_OBJECTS lookup.
    // tryResolveFromDb previously fired 1 DB call per name during BFS (blocking ~400ms each).
    // Now names accumulate here; flushPendingResolve() fires ONE query per BFS pass.
    private final Set<String> pendingResolveCheck = Collections.synchronizedSet(new HashSet<>());
    // Names already confirmed present (via batch or tryResolveFromDb) — never re-queried.
    private final Set<String> resolvedPresentObjects = Collections.synchronizedSet(new HashSet<>());

    // ── A1 closure-up-front state ─────────────────────────────────────────────────────────────
    // The full reachable-object closure for the entry point, fetched in ONE wave before the BFS.
    // When non-null the BFS skips per-level DB discovery for any level whose objects all live in
    // this set (pure in-memory scoping); a level with an object outside it falls back to the
    // existing level-by-level discovery. The set is populated only when the closure query returned
    // a non-empty result (Oracle); for dialects without it the set stays null and every level uses
    // the fallback path — counted and logged at the end of the crawl.
    private volatile Set<String> closureNames = null;
    // Counters proving the fallback is still exercised and measurable.
    private int closureLevelsServed = 0;
    private int fallbackLevelsServed = 0;

    // A4: parse happens while the next fetch is in flight. Parsing runs on the wave's pool threads,
    // so all parse entry points take this lock — the generated ANTLR parsers share a static DFA
    // cache that is cleared periodically, and concurrent parse + clear would race. Holding the lock
    // still overlaps fetch (network) with parse (CPU) across pool threads.
    private final Object parseLock = new Object();

    // --- Top-level 3-phase timing (parse / discover / save) for the whole crawl() run ---
    // "parse"    = ANTLR/dialect grammar parse of already-fetched source text (CPU-bound, local).
    // "discover" = DB-backed source fetch + schema/table/trigger resolution (network-bound).
    // "save"     = ChunkedFlowWriter/ArtifactStore writes (chunk writes, final result, merge).
    // These wrap only the representative/dominant call sites in the main BFS loop, not every
    // single call, per the intentionally lightweight scope of this instrumentation.
    private final AtomicLong parseNanos = new AtomicLong();
    private final AtomicLong discoverNanos = new AtomicLong();
    private final AtomicLong saveNanos = new AtomicLong();

    // Overload-resolution fallbacks (W1). Mirrors the visitor's AtomicLong fallback-counter
    // pattern: exact-signature matching needs call argument types the grammar rarely supplies, so
    // most overloaded calls resolve by arity and, when the arity is ambiguous, by first match.
    // Public so tests/telemetry can read them without a getter.
    public static final AtomicLong OVERLOAD_ARITY_FALLBACKS = new AtomicLong();
    public static final AtomicLong OVERLOAD_FIRST_MATCH_FALLBACKS = new AtomicLong();

    public DependencyCrawler(PlSqlParserEngine engine, SourceDownloader downloader,
                             SchemaResolver schemaResolver) {
        this.engine = engine;
        this.downloader = downloader;
        this.schemaResolver = schemaResolver;
    }

    public void setMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
    }

    /**
     * The object type the caller asked for, used only for the depth-0 lookup's package fallback and
     * its error message. Null is allowed and means "unspecified". See {@link #entryObjectType}.
     */
    public void setEntryObjectType(String entryObjectType) {
        this.entryObjectType = entryObjectType;
    }

    public void setTimeoutPerEntryMs(long timeoutPerEntryMs) {
        this.timeoutPerEntryMs = timeoutPerEntryMs;
    }

    /**
     * Where the chunked output goes: the PARENT prefix, under which a folder named after the
     * entry point (or {@link #setOutputFolderName}) is created. Both must be set together.
     */
    public void setOutputDir(ArtifactStore store, ArtifactKey outputKey) {
        this.store = store;
        this.outputKey = outputKey;
    }

    /**
     * Explicit output folder name override. When set, the chunked writer uses this name
     * instead of deriving it from the entry point — lets callers scope the folder
     * (e.g. by project/env/connection) so analyses of the same object don't collide.
     */
    public void setOutputFolderName(String name) {
        this.outputFolderName = name;
    }

    public void setNodeCache(NodeParseCache nodeCache) {
        this.nodeCache = nodeCache;
    }

    /**
     * Force-refresh: bypass the idempotency-registry skip so every reachable object is re-analysed
     * even when unchanged. Default false.
     */
    public void setForceRefresh(boolean forceRefresh) {
        this.forceRefresh = forceRefresh;
    }

    /**
     * The fresh-default switch. {@code false} (default) = a complete fresh crawl (no cross-run
     * cache reads, no registry skips); {@code true} = the user chose "Fast re-run", reusing the
     * persistent source/parse caches and the analysis registry to skip unchanged objects. See
     * {@link #fastReRun}.
     */
    public void setFastReRun(boolean fastReRun) {
        this.fastReRun = fastReRun;
    }

    /** True when the fast-rerun (cache-reading) path is active. */
    public boolean isFastReRun() {
        return fastReRun;
    }

    public void setParseResultCache(ParseResultCache parseResultCache) {
        this.parseResultCache = parseResultCache;
    }

    /**
     * Opt out of the empty-crawl guard. Default {@code false}: a crawl that finds no sources throws
     * rather than writing an empty analysis. See {@link #allowEmptyAnalysis}.
     */
    public void setAllowEmptyAnalysis(boolean allowEmptyAnalysis) {
        this.allowEmptyAnalysis = allowEmptyAnalysis;
    }

    /**
     * Upper bound on concurrent catalog lookups (batched ALL_OBJECTS chunks + per-name
     * {@code tryResolveFromDb} calls) fanned out per BFS level. Default 8; the operator lowers it to
     * match a database's connection limit. Never below 1.
     */
    public void setCatalogParallelism(int catalogParallelism) {
        this.catalogParallelism = Math.max(1, catalogParallelism);
    }

    /**
     * Upper bound on concurrent source fetches fanned out across one BFS level (the src-prefetch
     * pool widened to the whole level). Default 8; never below 1.
     */
    public void setSourceParallelism(int sourceParallelism) {
        this.sourceParallelism = Math.max(1, sourceParallelism);
    }

    /** Chunk size for the batched ALL_OBJECTS IN(...) lookups (Oracle bind-variable limit headroom). */
    public void setCatalogBatchSize(int catalogBatchSize) {
        this.catalogBatchSize = Math.max(1, catalogBatchSize);
    }

    /**
     * Crawl from the given entry point and produce a FlowResult.
     *
     * @param entryPoint e.g. "PG_AC_EINVOICE.PC_IRBM_SUBMISSION" or "FN_SOME_FUNC"
     * @return FlowResult with complete call chain, schema-resolved references
     */
    public FlowResult crawl(String entryPoint) {
        long startTime = System.currentTimeMillis();
        String upperEntry = entryPoint.toUpperCase().trim();
        String entrySchema = null;

        // THE FRESH-DEFAULT GATE: only a fast re-run may read cross-run caches. Writes still run in
        // both modes (the source/parse caches and the registry are populated for a possible later
        // fast run), but a default (fresh) crawl never consults them.
        downloader.setCacheReadEnabled(fastReRun);

        // Reset state
        visited.clear();
        dmlRefs.clear();
        crawledTriggerNodeIds.clear();
        pendingResolveCheck.clear();
        resolvedPresentObjects.clear();
        knownObjectTypes.clear();
        closureNames = null;
        closureLevelsServed = 0;
        fallbackLevelsServed = 0;
        parseNanos.set(0);
        discoverNanos.set(0);
        saveNanos.set(0);
        List<String> errors = new ArrayList<>();
        Map<String, SchemaTableInfo> tableInfoMap = new LinkedHashMap<>();
        List<FlowEdge> edgeBatch = new ArrayList<>();
        int maxDepthReached = 0;
        int orderCounter = 0;

        // Setup chunked writer if output dir is set
        ChunkedFlowWriter chunkedWriter = null;
        if (outputKey != null && store != null) {
            try {
                String safeName = (outputFolderName != null && !outputFolderName.isBlank())
                        ? outputFolderName
                        : upperEntry.replace(".", "_").replace("/", "_").replace("\\", "_");
                ArtifactKey buildKey = outputKey.resolve(safeName);
                chunkedWriter = new ChunkedFlowWriter(store, buildKey);
                downloader.setLocalCacheDir(store, chunkedWriter.getSourcesKey());
                // Persistent, DDL-validated source cache. Deliberately under the PARENT outputKey
                // (not the per-analysis folder, which AnalysisService archives on every re-run), so
                // an unchanged object's source survives and skips the re-fetch. Namespace:
                // <outputKey>/.source-cache/. Keyed by connection identity + schema + name + type.
                if (downloader.getConnManager() != null) {
                    downloader.setSourceCache(store, outputKey.resolve(".source-cache"),
                            downloader.getConnManager().getConnectionIdentity());
                }
                // Analysis-level idempotency registry (central decision step 3). Deliberately under
                // the PARENT outputKey — like the source cache — so it survives the per-analysis
                // folder being archived on re-run. PER-ANALYSIS scope, per-user via the routed
                // store; there is no global registry location. Corrupt/missing => re-analyse all.
                registry = new AnalysisRegistry(store, outputKey);
                log.info("[Crawler] Chunked output: " + buildKey);
            } catch (IOException e) {
                log.info("[Crawler] Failed to init chunked writer: " + e.getMessage());
            }
        }

        // Parse entry point
        String packageName = null;
        String subprogramName;
        if (upperEntry.contains(".")) {
            String[] parts = upperEntry.split("\\.", 2);
            packageName = parts[0];
            subprogramName = parts[1];
        } else {
            subprogramName = upperEntry;
        }

        // ── A1: the closure up front ──────────────────────────────────────────────────────────
        // Compute the FULL reachable-object closure for the entry point (the resolver's transitive
        // dependency set, derived from its Q1 CONNECT BY / Q4 direct-dependency queries — already
        // run at resolver construction). Fetch every source in ONE parallel wave, then let the BFS
        // scope purely in memory over those local sources. A dialect without the closure query (or
        // one that returns nothing) leaves the wave empty and the BFS keeps its level-by-level
        // discovery — counted and logged below.
        String entryObjForDeps = packageName != null ? packageName : subprogramName;
        log.info("[Crawler] Pre-computing transitive dependencies for: " + entryObjForDeps);
        // Never crawl Oracle built-in/system packages (DBMS_*, UTL_*, SYS.*, STANDARD, ...).
        // Their source is wrapped, which forces the slow DBMS_METADATA fallback and adds
        // needless DB round-trips.  Copy into a fresh mutable set (the resolver may return
        // an immutable view).
        Set<String> entryDeps = new java.util.LinkedHashSet<>();
        for (String dep : schemaResolver.getReachableClosure(entryObjForDeps)) {
            if (!isBuiltInPackage(dep)) entryDeps.add(dep);
        }
        log.info("[Crawler] Found " + entryDeps.size() + " transitive dependencies");

        // A3: capture the catalog snapshot once (fingerprint-guarded) so the wave and every
        // subsequent per-level lookup are served from memory rather than the catalog. Fast-rerun
        // only: a fresh crawl must not read the snapshot (and snapshotMeta() enforces that too).
        boolean snapshotReady = false;
        if (fastReRun) {
            try {
                snapshotReady = downloader.ensureCatalogSnapshot();
            } catch (Exception e) {
                log.info("[Crawler] catalog snapshot skipped: " + e.getMessage());
            }
        }

        if (!entryDeps.isEmpty()) {
            // A2 + A4: bulk-pull the whole wave where the dialect allows, parsing each source as
            // soon as it is fetched (fetch/parse overlap); per-object parallel fetch otherwise.
            closureNames = runClosureWave(entryDeps);
            log.info("[Crawler] Closure wave: {} objects fetched up front (snapshot={})",
                    closureNames.size(), snapshotReady);
        } else {
            // Fallback: no closure (dialect without it / empty result) — existing per-object
            // prefetch, which the BFS's level-by-level discovery then complements.
            log.info("[Crawler] No closure available — using level-by-level discovery (fallback)");
            prefetchSources(entryDeps);
        }

        // Populate object types (TABLE / VIEW / MATERIALIZED_VIEW) from the dialect catalog so
        // referenced views/MVs are tagged (not defaulted to TABLE) in both per-node and aggregate
        // table listings. No-op for Oracle (resolved via ALL_OBJECTS) and when unsupported.
        try { schemaResolver.batchResolveObjectTypes(null); } catch (Exception e) {
            log.info("[Crawler] object-type resolve skipped: " + e.getMessage());
        }

        // Fix B: after prefetch completes, bulk-resolve ALL table names found across every
        // downloaded package in one single ALL_OBJECTS query.  Without this, resolveTableSchemas()
        // called per-BFS-node fires an incremental batchResolveTableOwners each time (~40 queries).
        bulkPreResolveAllTables();

        LinkedList<BfsItem> queue = new LinkedList<>();
        queue.add(new BfsItem(packageName, subprogramName, 0, null, null, 0));
        int currentBfsDepth = -1;

        while (!queue.isEmpty()) {
            BfsItem item = queue.poll();

            // When BFS depth advances, prepare the WHOLE level in parallel before processing it:
            //   1. flush accumulated unknown call names (batched ALL_OBJECTS, parallel chunks),
            //   2. prime catalog metadata (owner/type/LAST_DDL) for the level — one batched query,
            //   3. fetch every level-N source concurrently (the src-prefetch pool widened to a level).
            // BFS keeps all same-depth items contiguous at the head, so draining them here is exact.
            // The drained items are put back at the head in the same order, and processing below
            // stays serial — so nodes, edges and their order are byte-for-byte the serial result.
            if (item.depth > currentBfsDepth) {
                currentBfsDepth = item.depth;
                List<BfsItem> levelRest = new ArrayList<>();
                while (!queue.isEmpty() && queue.peek().depth == item.depth) {
                    levelRest.add(queue.poll());
                }
                List<BfsItem> level = new ArrayList<>(levelRest.size() + 1);
                level.add(item);
                level.addAll(levelRest);

                // A1: when the closure wave already fetched every object on this level, the BFS is
                // pure in-memory scoping — no catalog query and no source fan-out. A level that
                // carries an object discovered during parsing (not in the closure) keeps the
                // existing discovery path for that level, so correctness is never traded for speed.
                if (closureNames != null && levelCoveredByClosure(level)) {
                    closureLevelsServed++;
                } else {
                    fallbackLevelsServed++;
                    // ONE catalog query per level: the union of the pending resolve/missing names
                    // and the level's own objects, returning owner / object_type / LAST_DDL. This
                    // replaces the previous up-to-three ALL_OBJECTS queries (flushPendingResolve +
                    // flushMissingBatch + fetchLevelCatalogMeta) and primes the type map so the
                    // node-cache peek no longer fires blind per-object source probes.
                    Map<String, String[]> levelMeta = prepareLevelCatalog(level);
                    if (!levelMeta.isEmpty()) downloader.primeCatalogMeta(levelMeta);
                    // Parallel source fan-out: every level-N source fetched simultaneously.
                    prefetchLevelSources(level);
                }

                for (int i = levelRest.size() - 1; i >= 0; i--) queue.addFirst(levelRest.get(i));
            }
            // Task #28: DB2 module members have no item.packageName but do have an owning module;
            // effectivePackageName supplies it as the PACKAGE slot so the visit key is MODULE.SUB.
            String itemPackage = effectivePackageName(item, null);
            String visitKey = buildVisitKey(item.schema, itemPackage, item.subprogramName);
            // Overloaded member: distinguish by SIGNATURE (types when known, else arity) so each
            // overload becomes its own node. Package members resolve against the parsed package;
            // standalone Postgres/DB2 routines against the routine catalog (disc resolved at
            // enqueue and carried on the item). Null → not overloaded → id stays byte-identical.
            String itemOverloadDisc = item.packageName != null
                    ? resolveOverload(item.packageName, item.subprogramName, item.argCount, item.argTypes)
                    : item.overloadDisc;
            boolean isOverloaded = itemOverloadDisc != null;
            if (isOverloaded) visitKey = visitKey + "#" + itemOverloadDisc;

            if (visited.contains(visitKey)) continue;
            visited.add(visitKey);

            if (maxDepth >= 0 && item.depth > maxDepth) {
                errors.add("Max depth " + maxDepth + " exceeded for: " + visitKey);
                continue;
            }

            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed > timeoutPerEntryMs * 10) {
                errors.add("Overall timeout exceeded after " + elapsed + "ms");
                break;
            }

            if (item.depth > maxDepthReached) maxDepthReached = item.depth;

            // ── Central fetch decision, step 3: analysis-level idempotency registry ────────────
            // Consulted ONCE per object per crawl, after in-run dedup and before any DB work. An
            // object whose LAST_DDL is UNCHANGED and whose previous run was COMPLETE is restored
            // from the node cache with ZERO database fetches and ZERO re-parse. Force-refresh,
            // a missing/corrupt node-cache entry, or a partial previous run all fall through to a
            // normal analysis (fail-safe).
            if (fastReRun && registry != null && nodeCache != null && !forceRefresh) {
                String regObj = item.packageName != null ? item.packageName : item.subprogramName;
                String currentDdl = downloader.getCatalogLastDdl(regObj);
                AnalysisRegistry.Entry regEntry = registry.entryFor(visitKey);
                if (registry.isCompleteAndUnchanged(regEntry, currentDdl)) {
                    FlowNode restored = nodeCache.getByLeaf(regEntry.nodeCacheLeaf);
                    if (restored != null) {
                        orderCounter++;
                        restored.setOrder(orderCounter);
                        restored.setDepth(item.depth);
                        recordCrawledTriggerNodeId(crawledTriggerNodeIds, restored);
                        if (item.depth == 0 && restored.getSchema() != null) {
                            entrySchema = restored.getSchema();
                        }
                        String restoredOwner = restored.getSchema();
                        edgeBatch.clear();
                        enqueueExternalCalls(restored.getCalls(), item.depth,
                                itemPackage, item.subprogramName,
                                restoredOwner, queue, edgeBatch,
                                isOverloaded ? itemOverloadDisc : null);
                        aggregateTableInfo(tableInfoMap, restored.getTableOperations(), visitKey);
                        if (chunkedWriter != null) {
                            long tSave0 = System.nanoTime();
                            try {
                                chunkedWriter.writeChunk(restored);
                                chunkedWriter.appendEdges(new ArrayList<>(edgeBatch), orderCounter);
                                chunkedWriter.markDone(visitKey);
                                writeNodeSource(chunkedWriter, restored, restoredOwner);
                            } catch (IOException e) {
                                errors.add("Chunk write error for " + visitKey + ": " + e.getMessage());
                            } finally {
                                saveNanos.addAndGet(System.nanoTime() - tSave0);
                            }
                        }
                        log.info("[Crawler] Done (registry skip — no fetch, no re-parse): {}", visitKey);
                        continue;
                    }
                    // Node cache entry gone/corrupt → re-analyse this object (fail-safe).
                    log.info("[Crawler] Registry entry for {} has no restorable node — re-analysing", visitKey);
                }
            }

            // Mark progress
            if (chunkedWriter != null) {
                try { chunkedWriter.markProcessing(visitKey); } catch (IOException ignored) {}
            }

            long nodeStart = System.currentTimeMillis();
            log.info("[Crawler] Processing: {} (depth={})", visitKey, item.depth);

            // Check if this is a TYPE/SYNONYM/SEQUENCE — create stub node, don't try to parse
            String resolvedType = schemaResolver.resolveObjectType(
                    item.packageName != null ? item.packageName : item.subprogramName);
            if (resolvedType != null && isNonParsableType(resolvedType)) {
                String ownerSch = schemaResolver.resolveSchema(
                        item.packageName != null ? item.packageName : item.subprogramName);
                orderCounter++;
                FlowNode stubNode = new FlowNode();
                stubNode.setOrder(orderCounter);
                stubNode.setDepth(item.depth);
                stubNode.setSchema(ownerSch);
                stubNode.setPackageName(item.packageName);
                stubNode.setObjectName(item.subprogramName);
                stubNode.setObjectType(resolvedType);
                stubNode.setReadable(false);
                stubNode.setMessage("Oracle " + resolvedType + " object - no parseable source");
                stubNode.generateNodeId();
                log.info("[Crawler] Stub node for " + resolvedType + ": " + visitKey);
                if (chunkedWriter != null) {
                    try {
                        chunkedWriter.writeChunk(stubNode);
                        chunkedWriter.markDone(visitKey);
                    } catch (IOException ignored) {}
                }
                continue;
            }

            try {
                // ── Node-level parse cache check ─────────────────────────────────────────
                // Peek at source text (may trigger DB download if not yet cached).
                // If the same source was fully parsed in a previous run the FlowNode JSON
                // is restored from disk, its children enqueued, and ANTLR is skipped entirely.
                // Fast-rerun only — a fresh crawl always re-parses.
                if (fastReRun && nodeCache != null) {
                    // Schema-qualify standalone lookups so same-named objects in different
                    // schemas fetch the correct source (querySourceSqlServer parses "schema.name").
                    // SQL Server only — Oracle's source query matches on the bare name.
                    boolean sqlSrvPeek = downloader.getConnManager() != null && downloader.getConnManager().isSqlServer();
                    String srcObjForCache = item.packageName != null ? item.packageName
                            : (sqlSrvPeek && item.schema != null && !item.schema.isEmpty()
                                ? item.schema + "." + item.subprogramName : item.subprogramName);

                    // Skip the DB peek entirely if this pkg.sub is already known non-subprogram,
                    // or if the standalone name has already been confirmed absent from ALL_OBJECTS
                    // by a prior bulk batch check.  Without these guards, downloadSource fires
                    // 2 DB calls (PROCEDURE + FUNCTION) each potentially waiting 30s for a
                    // connection slot, causing 60s stalls per unknown name (e.g. C_ERROR_TEXT).
                    boolean skipNodeCachePeek = false;
                    if (item.packageName != null && item.subprogramName != null) {
                        String compoundKey = (item.packageName + "." + item.subprogramName).toUpperCase();
                        if (knownMissingObjects.contains(compoundKey)) skipNodeCachePeek = true;
                    }
                    if (!skipNodeCachePeek) {
                        String standaloneKey = srcObjForCache.toUpperCase();
                        if (knownMissingObjects.contains(standaloneKey)) skipNodeCachePeek = true;
                    }

                    // Prime the source downloader so we have the source text for the cache key.
                    // downloadSource is idempotent — returns instantly from in-memory cache on hit.
                    // If the bulk ALL_OBJECTS check already told us the exact object_type, use that
                    // directly (1 DB call).  Otherwise fall back to the 2-probe blind sequence.
                    String peekedSource = null;
                    if (!skipNodeCachePeek) {
                        String knownType = knownObjectTypes.get(srcObjForCache.toUpperCase());
                        if (knownType != null) {
                            peekedSource = downloader.downloadSource(srcObjForCache, knownType);
                        } else {
                            peekedSource = downloader.downloadSource(srcObjForCache,
                                    item.packageName != null ? "PACKAGE BODY" : "PROCEDURE");
                            if (peekedSource == null)
                                peekedSource = downloader.downloadSource(srcObjForCache,
                                        item.packageName != null ? "PACKAGE" : "FUNCTION");
                        }
                    }

                    if (peekedSource != null) {
                        FlowNode cachedNode = nodeCache.get(visitKey, peekedSource);
                        if (cachedNode != null) {
                            // Restore order/depth from current BFS position
                            orderCounter++;
                            cachedNode.setOrder(orderCounter);
                            cachedNode.setDepth(item.depth);
                            recordCrawledTriggerNodeId(crawledTriggerNodeIds, cachedNode);
                            if (item.depth > maxDepthReached) maxDepthReached = item.depth;
                            if (item.depth == 0 && cachedNode.getSchema() != null) {
                                entrySchema = cachedNode.getSchema();
                            }

                            // Enqueue children from the cached call list
                            String cachedOwnerSchema = cachedNode.getSchema();
                            enqueueExternalCalls(cachedNode.getCalls(), item.depth,
                                    itemPackage, item.subprogramName,
                                    cachedOwnerSchema, queue, edgeBatch,
                                    isOverloaded ? itemOverloadDisc : null);
                            aggregateTableInfo(tableInfoMap, cachedNode.getTableOperations(), visitKey);

                            if (chunkedWriter != null) {
                                long tSave0 = System.nanoTime();
                                chunkedWriter.writeChunk(cachedNode);
                                chunkedWriter.appendEdges(new ArrayList<>(edgeBatch), orderCounter);
                                chunkedWriter.markDone(visitKey);
                                writeNodeSource(chunkedWriter, cachedNode, cachedOwnerSchema);
                                saveNanos.addAndGet(System.nanoTime() - tSave0);
                            }
                            log.info("[Crawler] Done (cache): {} in {}ms", visitKey, System.currentTimeMillis() - nodeStart);
                            continue;
                        }
                    }
                }

                long parseStart = System.currentTimeMillis();
                ParseResult parseResult = downloadAndParse(item.packageName, item.subprogramName, item.schema,
                        item.overloadId);
                log.info("[Crawler] downloadAndParse({}) took {}ms", visitKey, System.currentTimeMillis() - parseStart);

                // Entry-object resolution (Fix C). A bare name that downloaded nothing is retried as
                // a PACKAGE before it is declared missing: the UI routinely submits a package name
                // with objectType PROCEDURE (or an owner that does not match), and the object is
                // right there under the same name — just not as a standalone routine. The retry
                // fetches only PACKAGE BODY / PACKAGE, so it cannot misread a genuine standalone
                // PROCEDURE, and it is skipped entirely when the caller already asked for a package.
                if (parseResult == null && item.depth == 0 && item.packageName == null) {
                    ParseResult asPackage = tryPackageFallback(item.subprogramName, item.schema);
                    if (asPackage != null) {
                        log.info("[Crawler] Entry '{}' is not a standalone {} — resolved as a PACKAGE "
                                        + "instead (entry-object fallback)",
                                item.subprogramName,
                                entryObjectType != null ? entryObjectType : "object");
                        parseResult = asPackage;
                    }
                }

                if (parseResult == null) {
                    // Last chance: check if SourceDownloader found it was a TYPE via DB
                    String srcObj = item.packageName != null ? item.packageName : item.subprogramName;
                    String cachedType = downloader.getCachedType(srcObj);
                    if (cachedType != null && isNonParsableType(cachedType.replace(" ", "_"))) {
                        String ownerSch = downloader.getOwner(srcObj);
                        orderCounter++;
                        FlowNode stubNode = new FlowNode();
                        stubNode.setOrder(orderCounter);
                        stubNode.setDepth(item.depth);
                        stubNode.setSchema(ownerSch);
                        stubNode.setPackageName(item.packageName);
                        stubNode.setObjectName(item.subprogramName);
                        stubNode.setObjectType(cachedType);
                        stubNode.setReadable(false);
                        stubNode.setMessage("Oracle " + cachedType + " object - no parseable source");
                        stubNode.generateNodeId();
                        log.info("[Crawler] Stub node for " + cachedType + ": " + visitKey);
                        if (chunkedWriter != null) {
                            try {
                                chunkedWriter.writeChunk(stubNode);
                                chunkedWriter.markDone(visitKey);
                            } catch (IOException ignored) {}
                        }
                        continue;
                    }
                    log.info("[Crawler] No source found for: " + visitKey);
                    if (chunkedWriter != null) {
                        try { chunkedWriter.markFailed(visitKey, "no source"); } catch (IOException ignored) {}
                    }
                    // If the ENTRY object itself can't be found, surface a clear error instead of
                    // a silent empty result (0 procs / 0 tables) — the usual cause is a wrong
                    // procedure name for the selected database/connection.
                    if (item.depth == 0) {
                        String entryDisplay = item.packageName != null
                                ? item.packageName + "." + item.subprogramName
                                : item.subprogramName;
                        // Say exactly what was attempted, so a wrong objectType or owner is
                        // self-correctable from the message alone: which types were probed, and
                        // which owners were searched. A genuinely missing object still fails — the
                        // message just stops being a dead end.
                        errors.add("Entry object '" + entryDisplay + "' not found on the selected "
                                + "connection — tried " + String.join(" and ", entryTypesTried(item.packageName != null))
                                + " in schemas: " + describeSchemasSearched(item.schema)
                                + " — verify the name, its type and its owner.");
                    }
                    continue;
                }

                SubprogramInfo targetSub = null;
                ParsedObject targetObj = null;
                String ownerSchema = null;

                for (ParsedObject obj : parseResult.getObjects()) {
                    String objName = obj.getName() != null ? obj.getName().toUpperCase() : "";
                    if (item.packageName != null) {
                        // Match: exact name, schema-prefixed, or synonym (downloaded source may
                        // have a different package name than the call reference due to synonyms)
                        boolean nameMatch = objName.equalsIgnoreCase(item.packageName)
                                || objName.endsWith("." + item.packageName.toUpperCase());
                        // Also accept if this is the only/first parsed object from the download
                        // (synonym resolved to the real package name)
                        if (!nameMatch && parseResult.getObjects().size() == 1
                                && obj.getType() != null
                                && obj.getType().toUpperCase().contains("PACKAGE")) {
                            nameMatch = true;
                        }
                        if (nameMatch) {
                            targetObj = obj;
                            ownerSchema = resolveObjectSchema(
                                    objName.isEmpty() ? item.packageName : objName,
                                    obj.getSchema());
                            SubprogramInfo firstMatch = null;
                            // Exact-signature preference (W1): when the call supplied argument
                            // types, pick the overload whose declared signature matches — this is
                            // what separates same-arity-different-type overloads.
                            String wantSig = FlowNode.allTypesKnown(item.argTypes)
                                    ? FlowNode.signatureOfTypes(item.argTypes) : null;
                            for (SubprogramInfo sub : obj.getSubprograms()) {
                                if (sub.getName() != null
                                        && sub.getName().equalsIgnoreCase(item.subprogramName)) {
                                    if (firstMatch == null) firstMatch = sub;
                                    if (isOverloaded && wantSig != null
                                            && wantSig.equals(FlowNode.typeSignature(sub.getParameters()))) {
                                        targetSub = sub;
                                        break;
                                    }
                                    // Overload resolution by arity: pick the overload whose
                                    // parameter count matches the call's argument count.
                                    if (isOverloaded && item.argCount >= 0
                                            && sub.getParameters() != null
                                            && sub.getParameters().size() == item.argCount) {
                                        targetSub = sub;
                                        break;
                                    }
                                    if (!isOverloaded) { targetSub = sub; break; }
                                }
                            }
                            if (targetSub == null) targetSub = firstMatch;
                            if (targetSub != null) break;
                        }
                    } else {
                        if (objName.equalsIgnoreCase(item.subprogramName)) {
                            targetObj = obj;
                            ownerSchema = resolveObjectSchema(item.subprogramName, obj.getSchema());
                            break;
                        }
                    }
                }

                // Fallback: if subprogram not found in parse, extract from raw source
                if (targetSub == null && item.packageName != null && targetObj != null) {
                    String rawSource = downloader.getCachedSource(item.packageName);
                    if (rawSource != null) {
                        String extracted = extractSubprogramSource(rawSource,
                                item.subprogramName);
                        if (extracted != null) {
                            long tReparse0 = System.nanoTime();
                            ParseResult reParsed = engine.parseContent(
                                    "CREATE OR REPLACE PACKAGE BODY " + item.packageName
                                    + " AS\n" + extracted + "\nEND " + item.packageName + ";\n/",
                                    item.packageName + "_" + item.subprogramName + ".sql");
                            parseNanos.addAndGet(System.nanoTime() - tReparse0);
                            if (reParsed != null && !reParsed.getObjects().isEmpty()) {
                                ParsedObject rpObj = reParsed.getObjects().get(0);
                                for (SubprogramInfo sub : rpObj.getSubprograms()) {
                                    if (sub.getName() != null
                                            && sub.getName().equalsIgnoreCase(item.subprogramName)) {
                                        targetSub = sub;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                if (item.depth == 0 && ownerSchema != null) {
                    entrySchema = ownerSchema;
                }

                orderCounter++;
                FlowNode node = new FlowNode();
                node.setOrder(orderCounter);
                node.setDepth(item.depth);
                node.setSchema(ownerSchema);
                // Task #28: prefer the parsed module/package name (Db2 module member) over the
                // catalog-derived one, so the node id is MODULE.SUB.
                String nodePackage = itemPackage;
                if (nodePackage == null && targetObj != null
                        && targetObj.getPackageName() != null && !targetObj.getPackageName().isEmpty()) {
                    nodePackage = targetObj.getPackageName();
                }
                node.setPackageName(nodePackage);
                node.setObjectName(item.subprogramName);

                // Package-global DECLARATIONS, captured at parse time and routed to the writer's
                // dedicated artifact (independent of any PACKAGE node — a member entry point crawls
                // none). A body can declare private globals while the PUBLIC ones live only in the
                // spec, so both are merged (spec parsed once per package, cached) rather than
                // trusting whichever the body happened to carry. The DB2 module path already carries
                // them on the parsed object. See ChunkedFlowWriter#recordPackageGlobals.
                if (chunkedWriter != null && nodePackage != null && !nodePackage.isEmpty()) {
                    List<VariableInfo> pkgDecls = new ArrayList<>();
                    if (targetObj != null && targetObj.getPackageVariables() != null) {
                        pkgDecls.addAll(targetObj.getPackageVariables());
                    }
                    for (VariableInfo specVar : packageSpecVariables(nodePackage)) {
                        boolean dup = false;
                        for (VariableInfo e : pkgDecls) {
                            if (e.getName() != null && e.getName().equalsIgnoreCase(specVar.getName())) {
                                dup = true;
                                break;
                            }
                        }
                        if (!dup) pkgDecls.add(specVar);
                    }
                    chunkedWriter.recordPackageGlobals(nodePackage, pkgDecls);
                }

                // Check if source is wrapped/encrypted
                String srcObjName = item.packageName != null ? item.packageName : item.subprogramName;
                if (downloader.isWrapped(srcObjName)
                        || PlSqlParserEngine.isWrappedSource(downloader.getCachedSource(srcObjName))) {
                    node.setReadable(false);
                    node.setMessage("Source is ENCRYPTED/WRAPPED - cannot be parsed");
                }

                edgeBatch.clear();

                if (targetSub != null) {
                    node.setObjectType(targetSub.getType());
                    node.setLineStart(targetSub.getLineStart());
                    node.setLineEnd(targetSub.getLineEnd());
                    node.setParameters(targetSub.getParameters());
                    node.setLocalVariables(targetSub.getLocalVariables());
                    node.setStatements(targetSub.getStatements());
                    node.setCursors(targetSub.getCursors());
                    node.setDynamicSql(targetSub.getDynamicSql());
                    node.setExceptionHandlers(targetSub.getExceptionHandlers());
                    node.setExternalPackageVarRefs(targetSub.getExternalPackageVarRefs());
                    node.setExternalPackageVarRefDetails(targetSub.getExternalPackageVarRefDetails());
                    node.setAutonomousTransaction(targetSub.isPragmaAutonomousTransaction());
                    // Tasks #26/#27: result-set marker + return shape must survive ParsedObject ->
                    // FlowNode, or the node detail / index / facets never see them.
                    node.setReturnsResultSet(targetSub.isReturnsResultSet());
                    node.setResultSets(targetSub.getResultSets());
                    if (targetSub.getReturnType() != null) node.setReturnType(targetSub.getReturnType());
                    // Task #29: nested/local subprogram marker + depth (additive; never affects the id).
                    node.setNested(targetSub.isNested());
                    node.setScopeDepth(targetSub.getScopeDepth());

                    List<CallInfo> resolvedCalls = filterLocalVarCalls(
                            resolveCallSchemas(targetSub.getCalls(), ownerSchema),
                            targetSub.getLocalVariables(), targetSub.getParameters(),
                            packageLevelVars(targetObj));
                    node.setCalls(resolvedCalls);
                    List<TableOperationInfo> resolvedTableOps = resolveTableSchemas(targetSub.getTableOperations(), ownerSchema);
                    node.setTableOperations(resolvedTableOps);
                    aggregateTableInfo(tableInfoMap, resolvedTableOps, visitKey);
                    enqueueExternalCalls(resolvedCalls, item.depth, itemPackage,
                            item.subprogramName, ownerSchema, queue, edgeBatch,
                            isOverloaded ? itemOverloadDisc : null);

                } else if (targetObj != null) {
                    node.setObjectType(targetObj.getType());
                    node.setLineStart(targetObj.getLineStart());
                    node.setLineEnd(targetObj.getLineEnd());
                    node.setParameters(targetObj.getParameters());
                    if (targetObj.getTriggerEvent() != null) node.setTriggerEvent(targetObj.getTriggerEvent());
                    if (targetObj.getTriggerTable() != null) node.setTriggerTable(targetObj.getTriggerTable());
                    if (targetObj.getTriggerTiming() != null) node.setTriggerTiming(targetObj.getTriggerTiming());
                    if (targetObj.getTriggerWhenClause() != null) node.setTriggerWhenClause(targetObj.getTriggerWhenClause());
                    if (targetObj.getTriggerLevel() != null) node.setTriggerLevel(targetObj.getTriggerLevel());
                    if (targetObj.getTriggerBody() != null) node.setTriggerBody(targetObj.getTriggerBody());
                    // Package-level variable declarations — the estate globals facet's declaration
                    // list. Only package bodies/specs have these; a standalone routine's body-level
                    // variables live in globalVariables and are already surfaced as locals.
                    if (targetObj.getPackageVariables() != null && !targetObj.getPackageVariables().isEmpty()) {
                        node.setPackageVariables(targetObj.getPackageVariables());
                    }

                    // When a specific subprogram within a package was requested but could not be
                    // located by name, do NOT merge all other subprograms' data.  Merging the whole
                    // package body would attribute every other proc's table accesses to this node,
                    // corrupting usedBy in tables/index.json with wrong proc names and line numbers.
                    // Instead, only carry package-level (init-section) ops and mark the node.
                    boolean specificSubNotFound = item.packageName != null && item.subprogramName != null;
                    if (specificSubNotFound) {
                        log.warn("[Crawler] Subprogram '{}' not resolved in package '{}' — using package-level ops only to prevent cross-contamination",
                                item.subprogramName, item.packageName);
                        node.setMessage("Subprogram '" + item.subprogramName + "' not found in parsed package body — analysis may be incomplete");
                        // Mark compound key so any future enqueue of this same pkg.sub is
                        // skipped instantly without any DB call or connection stall.
                        String compoundKey = (item.packageName + "." + item.subprogramName).toUpperCase();
                        knownMissingObjects.add(compoundKey);
                    }

                    List<CallInfo> allCalls = new ArrayList<>(
                            resolveCallSchemas(targetObj.getCalls(), ownerSchema));
                    List<TableOperationInfo> allTableOps = new ArrayList<>(
                            resolveTableSchemas(targetObj.getTableOperations(), ownerSchema));
                    List<StatementInfo> allStatements = new ArrayList<>();
                    List<CursorInfo> allCursors = new ArrayList<>();
                    List<DynamicSqlInfo> allDynSql = new ArrayList<>();
                    List<ExceptionHandlerInfo> allExHandlers = new ArrayList<>();
                    List<String> allExtPkgVarRefs = new ArrayList<>();
                    List<PackageVarRefInfo> allExtPkgVarRefDetails = new ArrayList<>();
                    List<VariableInfo> allLocalVars = new ArrayList<>();

                    if (targetObj.getStatements() != null) allStatements.addAll(targetObj.getStatements());
                    if (targetObj.getCursors() != null) allCursors.addAll(targetObj.getCursors());
                    if (targetObj.getDynamicSql() != null) allDynSql.addAll(targetObj.getDynamicSql());
                    if (targetObj.getExceptionHandlers() != null) allExHandlers.addAll(targetObj.getExceptionHandlers());
                    if (targetObj.getExternalPackageVarRefs() != null) allExtPkgVarRefs.addAll(targetObj.getExternalPackageVarRefs());
                    if (targetObj.getExternalPackageVarRefDetails() != null) allExtPkgVarRefDetails.addAll(targetObj.getExternalPackageVarRefDetails());
                    // Standalone routines (no nested subprograms — SQL Server / MySQL procs & functions,
                    // Oracle standalone procs) keep their body variables in globalVariables; surface them
                    // as the node's local variables. Packages are unaffected (they have subprograms).
                    if ((targetObj.getSubprograms() == null || targetObj.getSubprograms().isEmpty())
                            && targetObj.getGlobalVariables() != null) {
                        allLocalVars.addAll(targetObj.getGlobalVariables());
                    }

                    if (!specificSubNotFound) {
                        // Package-as-entry-point: aggregate all subprograms into this node
                        List<SubprogramSummaryInfo> memberSummaries = new ArrayList<>();
                        List<VariableInfo> pkgLevelVars = packageLevelVars(targetObj);
                        for (SubprogramInfo sub : targetObj.getSubprograms()) {
                            // Tag each item with the subprogram it came from so the UI can still
                            // filter/drill into one member's own calls/tableOps/statements out of
                            // this single aggregated node (see CallInfo/TableOperationInfo/
                            // StatementInfo#getSubprogramName doc). Never set outside this branch.
                            List<CallInfo> subCalls = filterLocalVarCalls(
                                    resolveCallSchemas(sub.getCalls(), ownerSchema),
                                    sub.getLocalVariables(), sub.getParameters(),
                                    pkgLevelVars);
                            for (CallInfo c : subCalls) c.setSubprogramName(sub.getName());
                            allCalls.addAll(subCalls);
                            List<TableOperationInfo> subTableOps = resolveTableSchemas(sub.getTableOperations(), ownerSchema);
                            for (TableOperationInfo t : subTableOps) t.setSubprogramName(sub.getName());
                            allTableOps.addAll(subTableOps);
                            if (sub.getStatements() != null) {
                                for (StatementInfo s : sub.getStatements()) s.setSubprogramName(sub.getName());
                                allStatements.addAll(sub.getStatements());
                            }
                            if (sub.getCursors() != null) allCursors.addAll(sub.getCursors());
                            if (sub.getDynamicSql() != null) allDynSql.addAll(sub.getDynamicSql());
                            if (sub.getExceptionHandlers() != null) allExHandlers.addAll(sub.getExceptionHandlers());
                            if (sub.getExternalPackageVarRefs() != null) {
                                for (String ref : sub.getExternalPackageVarRefs()) {
                                    if (!allExtPkgVarRefs.contains(ref)) allExtPkgVarRefs.add(ref);
                                }
                            }
                            if (sub.getExternalPackageVarRefDetails() != null) {
                                allExtPkgVarRefDetails.addAll(sub.getExternalPackageVarRefDetails());
                            }
                            if (sub.getLocalVariables() != null) allLocalVars.addAll(sub.getLocalVariables());
                            if (sub.isPragmaAutonomousTransaction()) node.setAutonomousTransaction(true);
                            // Task #26: a package-as-entry-point node returns data if any member
                            // does; the counts add up so the marker is visible on the package row.
                            if (sub.isReturnsResultSet()) node.setReturnsResultSet(true);
                            node.setResultSets(node.getResultSets() + sub.getResultSets());
                            // Task #29: carry the nested marker/depth so the procedures facet can
                            // badge a member declared inside another member's body.
                            memberSummaries.add(new SubprogramSummaryInfo(
                                    sub.getName(), sub.getType(), sub.getLineStart(), sub.getLineEnd(),
                                    sub.isNested(), sub.getScopeDepth()));
                        }
                        node.setMembers(memberSummaries);
                    }

                    if (targetObj.isPragmaAutonomousTransaction()) node.setAutonomousTransaction(true);

                    // Tasks #26/#27: result-set marker + TVF/SETOF return shape from the parsed
                    // object. Package members aggregate into this same node below.
                    node.setReturnsResultSet(targetObj.isReturnsResultSet());
                    node.setResultSets(targetObj.getResultSets());
                    if (targetObj.getReturnType() != null) node.setReturnType(targetObj.getReturnType());
                    if (targetObj.getReturnsColumns() != null && !targetObj.getReturnsColumns().isEmpty()) {
                        node.setReturnsColumns(new ArrayList<>(targetObj.getReturnsColumns()));
                    }

                    // Package-as-entry-point aggregation above concatenates the object's OWN
                    // members first and then subprogram by subprogram, which is NOT source order:
                    // a package body's initialization block sits at the END of the file but its
                    // statements are ParsedObject-level (see PlSqlAnalysisVisitor.addStatement's
                    // else-branch), so they land at the FRONT of these lists. Verified on a real
                    // PKG_ORDER parse: statements came out UPDATE@18, COMMIT@19, SELECT@6,
                    // COMMIT@7, ... — and every consumer that walks a node's statement/cursor list
                    // in list order and calls the result a sequence (ChunkedFlowWriter's
                    // transactionScope.statements, and its statement-derived cursor
                    // OPEN/FETCH/CLOSE lifecycle list) then reported them out of execution order.
                    // A final stable sort by line puts the aggregate back in true source order;
                    // stable, so same-line entries keep the order the parser recorded them in.
                    allStatements.sort(Comparator.comparingInt(StatementInfo::getLine));
                    allTableOps.sort(Comparator.comparingInt(TableOperationInfo::getLine));
                    allCursors.sort(Comparator.comparingInt(CursorInfo::getLine));

                    node.setCalls(allCalls);
                    node.setTableOperations(allTableOps);
                    node.setStatements(allStatements);
                    node.setCursors(allCursors);
                    node.setDynamicSql(allDynSql);
                    node.setExceptionHandlers(allExHandlers);
                    // IDENTITY columns (SQL Server CREATE TABLE ... IDENTITY(seed,increment)) are
                    // ParsedObject-level, not per-subprogram — no dialect that has packages/
                    // subprograms (Oracle) also has this construct, so only the targetObj branch
                    // needs it.
                    if (targetObj.getIdentityColumns() != null) node.setIdentityColumns(targetObj.getIdentityColumns());
                    // FOREIGN KEY constraints are ParsedObject-level for the same reason: they
                    // come from ALTER TABLE / CREATE TABLE DDL, which is never nested inside a
                    // subprogram, so there is nothing to aggregate across subprograms.
                    if (targetObj.getForeignKeys() != null) node.setForeignKeys(targetObj.getForeignKeys());
                    node.setExternalPackageVarRefs(allExtPkgVarRefs);
                    node.setExternalPackageVarRefDetails(allExtPkgVarRefDetails);
                    node.setLocalVariables(allLocalVars);

                    aggregateTableInfo(tableInfoMap, allTableOps, visitKey);
                    enqueueExternalCalls(allCalls, item.depth, itemPackage,
                            item.subprogramName, ownerSchema, queue, edgeBatch,
                            isOverloaded ? itemOverloadDisc : null);

                } else {
                    // targetObj is null: package/object itself was not matched in the parse result.
                    // Only aggregate package-level ops to avoid cross-proc table contamination.
                    errors.add("Could not find specific subprogram '" + item.subprogramName
                            + "' in parsed result for: " + visitKey);
                    if (!parseResult.getObjects().isEmpty()) {
                        ParsedObject firstObj = parseResult.getObjects().get(0);
                        node.setObjectType(firstObj.getType());
                        ownerSchema = resolveObjectSchema(
                                item.packageName != null ? item.packageName : item.subprogramName,
                                firstObj.getSchema());
                        node.setSchema(ownerSchema);
                        node.setMessage("Subprogram '" + item.subprogramName + "' not found in parsed package body — analysis may be incomplete");

                        // Only include package-level (init-section) ops — do NOT merge all
                        // subprograms here, as that would attribute unrelated procs' table accesses
                        // to this node and corrupt usedBy in tables/index.json.
                        List<CallInfo> objCalls = resolveCallSchemas(firstObj.getCalls(), ownerSchema);
                        List<TableOperationInfo> objTableOps = resolveTableSchemas(firstObj.getTableOperations(), ownerSchema);
                        List<StatementInfo> objStatements = firstObj.getStatements() != null
                                ? new ArrayList<>(firstObj.getStatements()) : new ArrayList<>();

                        node.setCalls(objCalls);
                        node.setTableOperations(objTableOps);
                        node.setStatements(objStatements);
                        aggregateTableInfo(tableInfoMap, objTableOps, visitKey);
                        enqueueExternalCalls(objCalls, item.depth, itemPackage,
                                item.subprogramName, ownerSchema, queue, edgeBatch,
                                isOverloaded ? itemOverloadDisc : null);
                    }
                }

                if (!node.isReadable()) {
                    // Encrypted/wrapped — suppress parse errors
                } else if (parseResult.getErrors() != null && !parseResult.getErrors().isEmpty()) {
                    // Report parse errors once per source object (package/proc), not once per subprogram.
                    // Multiple procs from the same package share the same ParseResult; without this guard
                    // the same 82 errors would appear N times (once per calling proc in the analysis).
                    String srcObjForErr = item.packageName != null ? item.packageName : item.subprogramName;
                    if (parseErrorsReported.add(srcObjForErr.toUpperCase())) {
                        // Check if source appears truncated (ALL_SOURCE returned incomplete pkb).
                        String srcForTrunc = downloader.getCachedSource(srcObjForErr);
                        if (isSourceTruncated(srcForTrunc)) {
                            errors.add("[" + visitKey + "] Source appears TRUNCATED (ALL_SOURCE returned incomplete package body) — parse errors suppressed");
                        } else {
                            for (String err : parseResult.getErrors()) {
                                errors.add("[" + visitKey + "] " + err);
                            }
                        }
                    }
                }

                // Compute per-node metrics inline
                if (node.getLineEnd() > 0 && node.getLineStart() > 0) {
                    node.setLinesOfCode(node.getLineEnd() - node.getLineStart() + 1);
                }
                Map<String, Integer> stmtSummary = new LinkedHashMap<>();
                if (node.getStatements() != null) {
                    for (StatementInfo s : node.getStatements()) {
                        String stype = s.getType() != null ? s.getType().toUpperCase() : "OTHER";
                        stmtSummary.merge(stype, 1, Integer::sum);
                    }
                }
                node.setStatementSummary(stmtSummary);

                // Overloaded member: stamp the SIGNATURE discriminator (types when known, else the
                // legacy arity count) so its nodeId is distinct — and identical to the one the
                // call edge carries (both come from resolveOverload with the same inputs).
                if (isOverloaded) {
                    node.setOverloadSignature(itemOverloadDisc);
                }
                node.generateNodeId();
                // Register the crawled node's id under both its bare name and (for a package
                // member) PACKAGE.SUBPROGRAM, so a post-crawl caller can resolve a routine name to
                // the node it became — see resolveTriggerCallNodeIds.
                recordCrawledNodeId(crawledNodeIds, node);
                recordCrawledTriggerNodeId(crawledTriggerNodeIds, node);

                // Record this node's write-DML (INSERT/UPDATE/DELETE/MERGE) so trigger edges can
                // be attached later from the exact routine that performs the firing statement.
                if (node.getTableOperations() != null) {
                    for (TableOperationInfo t : node.getTableOperations()) {
                        String op = t.getOperation() == null ? "" : t.getOperation().toUpperCase();
                        if (op.startsWith("INSERT") || op.startsWith("UPDATE")
                                || op.startsWith("DELETE") || op.startsWith("MERGE")) {
                            dmlRefs.add(new String[]{
                                    node.getNodeId(),
                                    t.getSchema() == null ? "" : t.getSchema().toUpperCase(),
                                    t.getTableName() == null ? "" : t.getTableName().toUpperCase(),
                                    op });
                        }
                    }
                }

                // Persist node to parse cache so future analyses skip ANTLR for this node
                if (nodeCache != null) {
                    String srcObjForStore = item.packageName != null ? item.packageName : item.subprogramName;
                    String storedSource = downloader.getCachedSource(srcObjForStore);
                    if (storedSource != null) {
                        ArtifactKey leaf = nodeCache.put(visitKey, storedSource, node);
                        // Record this COMPLETE object in the idempotency registry (written once at
                        // the end of the crawl) so the next crawl of the same scope can skip it
                        // without any DB fetch. The node-cache leaf lets that skip restore the node.
                        if (registry != null && leaf != null) {
                            registry.record(visitKey, node.getSchema(), item.subprogramName,
                                    node.getObjectType(),
                                    downloader.getCatalogLastDdl(srcObjForStore), 1, leaf.leaf());
                        }
                    }
                }

                // Write chunk to disk and flush from memory
                if (chunkedWriter != null) {
                    long tSave0 = System.nanoTime();
                    try {
                        chunkedWriter.writeChunk(node);
                        chunkedWriter.appendEdges(new ArrayList<>(edgeBatch), orderCounter);
                        chunkedWriter.markDone(visitKey);

                        // Write source file for this node
                        writeNodeSource(chunkedWriter, node, ownerSchema);
                    } catch (IOException e) {
                        errors.add("Chunk write error for " + visitKey + ": " + e.getMessage());
                    } finally {
                        saveNanos.addAndGet(System.nanoTime() - tSave0);
                    }
                }

            } catch (Exception e) {
                errors.add("Error processing " + visitKey + ": " + e.getMessage());
                log.warn("[Crawler] Error processing {}: {}", visitKey, e.getMessage());
                if (chunkedWriter != null) {
                    try { chunkedWriter.markFailed(visitKey, e.getMessage()); } catch (IOException ignored) {}
                }
            } finally {
                log.info("[Crawler] Done: {} in {}ms", visitKey, System.currentTimeMillis() - nodeStart);
            }
        }

        // Final flush for any names accumulated in the last BFS level — ONE catalog query for the
        // remaining pending resolve/missing names (no per-name probes).
        prepareLevelCatalog(Collections.emptyList());

        // Fail loud on an empty crawl — BEFORE any artifact for this analysis is written.
        //
        // This is the earliest point at which the crawled-source count is known: the BFS has
        // drained and every node that could be found has been chunked. If it is zero the database
        // had nothing to give (down — e.g. ORA-12541 "no listener" — or the schema holds no
        // objects, or the entry point does not exist on this connection), and the writer is about
        // to persist an EMPTY analysis (0 nodes, empty sidecars) under whatever identity it was
        // handed. Live incident: exactly that ran over an existing 51-node analysis with the same
        // identity and destroyed it. Refuse to write the empty result unless explicitly allowed
        // (app.parser.allow-empty-analysis / Options.allowEmptyAnalysis) — the in-memory/stdout
        // path (no chunkedWriter) is unaffected and still returns an empty FlowResult.
        if (chunkedWriter != null && !allowEmptyAnalysis && chunkedWriter.getChunkCount() == 0) {
            throw new IllegalStateException(
                    "Crawled 0 sources — the database had nothing to analyze (is it up and does the "
                    + "schema contain objects?). Refusing to write an empty analysis.");
        }

        // Persist the idempotency registry ONCE for the whole crawl (the read happened when the
        // registry was constructed). Placed AFTER the empty-crawl guard so a dead-DB crawl never
        // records "complete" for objects it never analysed.
        if (registry != null) {
            registry.save();
        }

        // Write tables once after all nodes processed (was previously per-node — unnecessary rewrites)
        if (chunkedWriter != null) {
            long tSave0 = System.nanoTime();
            try {
                chunkedWriter.updateTables(tableInfoMap);
                chunkedWriter.flushProgress();
            } catch (IOException e) {
                log.warn("[Crawler] Final tables write error: {}", e.getMessage());
            } finally {
                saveNanos.addAndGet(System.nanoTime() - tSave0);
            }
        }

        // Discover triggers from DB for tables with DML operations
        if (chunkedWriter != null) {
            long tDiscover0 = System.nanoTime();
            try {
                TriggerDiscoverer triggerDiscoverer = new TriggerDiscoverer(
                        downloader.getConnManager(), schemaResolver, downloader);
                List<Map<String, Object>> dbTriggers = triggerDiscoverer.discover(tableInfoMap);
                discoverNanos.addAndGet(System.nanoTime() - tDiscover0);
                if (!dbTriggers.isEmpty()) {
                    chunkedWriter.setDiscoveredTriggers(dbTriggers);
                    log.info("[Crawler] Discovered " + dbTriggers.size()
                            + " triggers from ALL_TRIGGERS for DML tables");
                    // Promote discovered triggers into the call tree: a routine whose DML fires a
                    // trigger gets an additive FIRES_TRIGGER edge to a trigger node (and, for
                    // PostgreSQL, the trigger's handler function node via a TRIGGER_FN edge).
                    emitTriggerNodesAndEdges(dbTriggers, chunkedWriter, errors, orderCounter);
                }
            } catch (Exception e) {
                errors.add("Trigger discovery warning: " + e.getMessage());
                log.info("[Crawler] Trigger discovery failed (non-fatal): " + e.getMessage());
            }
        }

        // Discover scheduled jobs from each dialect's scheduler catalog (Oracle DBMS_SCHEDULER /
        // DBMS_JOB, SQL Server Agent, pg_cron, MySQL EVENT). Scheduled jobs are the roots of many
        // estates and were previously invisible; each becomes a first-class node with a SCHEDULES
        // edge to its target routine. Purely additive and non-fatal — a dialect without a scheduler
        // catalog simply yields nothing.
        if (chunkedWriter != null) {
            long tJobs0 = System.nanoTime();
            try {
                JobDiscoverer jobDiscoverer = new JobDiscoverer(
                        downloader.getConnManager(), schemaResolver, downloader);
                List<Map<String, Object>> jobs = jobDiscoverer.discover();
                discoverNanos.addAndGet(System.nanoTime() - tJobs0);
                if (!jobs.isEmpty()) {
                    List<Map<String, Object>> jobRows = JobSchedulerEmitter.emit(
                            jobs, chunkedWriter, visited, errors, orderCounter);
                    chunkedWriter.setDiscoveredJobs(jobRows);
                }
            } catch (Exception e) {
                errors.add("Job discovery warning: " + e.getMessage());
                log.info("[Crawler] Job discovery failed (non-fatal): " + e.getMessage());
            }
        }

        // Final merge: read all chunks back and assemble FlowResult
        if (chunkedWriter != null) {
            long tSave0 = System.nanoTime();
            try {
                FlowResult result = chunkedWriter.mergeChunks(
                        upperEntry, entrySchema,
                        System.currentTimeMillis() - startTime,
                        maxDepthReached, errors);
                result.setDbCallCount(downloader.getConnManager() != null
                        ? downloader.getConnManager().getDbCallCount() : 0);
                result.setParseMs(parseNanos.get() / 1_000_000);
                result.setDiscoverMs(discoverNanos.get() / 1_000_000);
                result.setSaveMs(saveNanos.get() / 1_000_000);
                // Compute callCount/statementSummary by streaming each node's chunk on demand
                // (MID-026 final piece) — no longer needs result.getFlowNodes() resident.
                Map<String, NodeMetrics> nodeMetrics =
                        computeNodeMetrics(chunkedWriter, result.getCallGraph());
                // Write final flow.json WITH metrics applied
                chunkedWriter.writeFinalResult(result, nodeMetrics);
                // Write schema resolver query results as separate JSONs
                chunkedWriter.writeResolverData(schemaResolver);
                saveNanos.addAndGet(System.nanoTime() - tSave0);
                logTimingSummary(upperEntry, startTime);
                return result;
            } catch (IOException e) {
                saveNanos.addAndGet(System.nanoTime() - tSave0);
                String msg = "Chunk merge failed, falling back to in-memory (empty) result: " + e.getMessage();
                log.warn("[Crawler] " + msg, e);
                errors.add(msg);
            }
        }

        // Fallback: in-memory result (no output dir)
        FlowResult inMemResult = buildInMemoryResult(upperEntry, entrySchema, startTime,
                maxDepthReached, errors, tableInfoMap);
        logTimingSummary(upperEntry, startTime);
        return inMemResult;
    }

    /**
     * Logs a single grep-friendly summary line with the 3 top-level phase totals (parse /
     * discover / save) plus overall wall-clock total for this crawl() run.
     */
    private void logTimingSummary(String entryName, long startTimeMs) {
        long totalMs = System.currentTimeMillis() - startTimeMs;
        long parseMs = parseNanos.get() / 1_000_000;
        long discoverMs = discoverNanos.get() / 1_000_000;
        long saveMs = saveNanos.get() / 1_000_000;
        log.info("[timing] {}: parse={}ms discover={}ms save={}ms total={}ms",
                entryName, parseMs, discoverMs, saveMs, totalMs);
        log.info("[crawl] {}: closure={} levels={} fallbackLevels={}",
                entryName, closureNames != null ? closureNames.size() : 0,
                closureLevelsServed, fallbackLevelsServed);
    }

    private static String trigStr(Object o) { return o == null ? null : o.toString(); }

    /**
     * Turn every catalog-discovered DB trigger into a first-class node, plus its additive edges.
     *
     * <p>EVERY discovered trigger becomes a node ({@code SCHEMA$TRG$NAME}) carrying its parsed body
     * calls (resolved to the crawled callee's node id), timing/event/level/WHEN and the catalog's
     * triggerType/status. Persisting the node is what lets the estate trigger facet serve
     * {@code bodyCalls}, per-node trigger detail, and lets the trigger's calls join the call graph
     * (a trigger -> callee CALL edge per resolved callee). Before this only triggers whose
     * triggering_event matched a DML actually recorded in the crawl became nodes, so a
     * catalog-discovered trigger whose firing statement lives outside the walked graph (or whose
     * event simply was not exercised) was silently invisible.
     *
     * <p>Edges remain event-gated where that is the correct semantic: a FIRES_TRIGGER edge
     * routine -> trigger is added only for a routine whose write-DML on the trigger's table matches
     * the trigger's own event (a trigger that never fires has no firing routine). For PostgreSQL a
     * TRIGGER_FN edge trigger -> handler-function is added and the handler becomes a node.
     *
     * <p>Dedup: a trigger that is BOTH source-parsed (a crawled TRIGGER node) and catalog-discovered
     * yields ONE node — the source-parsed node wins (it already carries the body and its own CALL
     * edges), the catalog record supplies status/triggerType via api/tables/index.json.
     */
    private void emitTriggerNodesAndEdges(List<Map<String, Object>> triggers,
                                          ChunkedFlowWriter writer, List<String> errors, int startOrder) {
        if (triggers == null || triggers.isEmpty() || writer == null) return;
        boolean isPg = downloader.getConnManager() != null && downloader.getConnManager().isPostgres();
        java.util.Set<String> emittedTrig = new java.util.HashSet<>();
        java.util.Set<String> emittedFn   = new java.util.HashSet<>();
        int order = startOrder;

        for (Map<String, Object> t : triggers) {
            String name = trigStr(t.get("name"));
            if (name == null || name.isBlank()) continue;
            String schema  = trigStr(t.get("schema"));
            String table   = trigStr(t.get("tableName"));
            String event   = trigStr(t.get("event"));
            String timing  = trigStr(t.get("timing"));
            String def     = trigStr(t.get("definition"));
            String tableU  = table == null ? "" : table.toUpperCase();
            String eventU  = event == null ? "" : event.toUpperCase();
            java.util.Set<String> events = new java.util.LinkedHashSet<>();
            for (String e : eventU.split("[,/ ]+")) if (!e.isBlank()) events.add(e.trim());

            // Does any reachable DML against this table match the trigger's own event? This gates
            // only the FIRES_TRIGGER edge (a routine fires this trigger) — NOT the node. A trigger
            // is a real database object and must be served even when its firing statement was not
            // walked (its body still calls routines, and its detail is still requested per node).
            boolean anyMatchingDml = false;
            for (String[] d : dmlRefs) {
                if (!tableU.equals(d[2])) continue;
                String op = d[3];
                if (op.startsWith("MERGE") || events.isEmpty() || events.stream().anyMatch(op::startsWith)) {
                    anyMatchingDml = true;
                    break;
                }
            }

            // ── trigger node ──────────────────────────────────────────────────────────
            FlowNode tn = new FlowNode();
            tn.setSchema(schema);
            tn.setPackageName("TRG");           // namespace triggers so ids never clash with routines
            tn.setObjectName(name);
            tn.setObjectType("TRIGGER");
            tn.setTriggerTable(tableU);
            tn.setTriggerEvent(eventU);
            tn.setTriggerTiming(timing);
            // Catalog metadata the source parser never sees (ALL_TRIGGERS TRIGGER_TYPE / STATUS).
            tn.setTriggerType(trigStr(t.get("triggerType")));
            tn.setTriggerStatus(trigStr(t.get("status")));
            tn.setTriggerLevel(triggerLevelFromCatalog(trigStr(t.get("triggerType")), timing));
            String when = trigStr(t.get("whenClause"));
            if (when != null && !when.isBlank()) tn.setTriggerWhenClause(when);
            // The trigger body's outbound calls, extracted by TriggerDiscoverer from the parsed
            // body. Persisting them on the node is what lets the estate trigger facet expose
            // bodyCalls (the cross-estate edge from a trigger into the routines it calls). The
            // trigger itself is a catalog artefact, not a crawled one, so its body's callees arrive
            // unresolved (type EXTERNAL); resolve each against the crawled estate and stamp the
            // crawled target's node id on the call, so the UI can cross-link to it.
            List<CallInfo> triggerCalls = toCallInfos(t.get("calls"));
            resolveTriggerCallNodeIds(triggerCalls, crawledNodeIds);
            tn.setCalls(triggerCalls);
            tn.setReadable(true);
            // Anchor every trigger at depth 1: a trigger is an artefact of the table it fires on,
            // not a node on the caller's own path, so it is pinned one level under the root rather
            // than at its true call-graph distance. This is why /complexity/scores reports a
            // trigger at depth 1 while the call-tree walker (graph distance from the root) can
            // report it much deeper — two different, intentional definitions of "depth".
            tn.setDepth(1);
            tn.generateNodeId();
            String trigId = tn.getNodeId();

            // Dedup against a source-parsed trigger node: reuse its id and emit no second chunk.
            String sourceNodeId = crawledTriggerNodeIds.get(triggerDedupKey(schema, name));
            boolean sourceParsed = sourceNodeId != null && !sourceNodeId.isBlank();
            if (sourceParsed) trigId = sourceNodeId;
            if (!emittedTrig.add(trigId)) continue;

            if (!sourceParsed) {
                tn.setOrder(++order);
                try {
                    if (def != null && !def.isBlank()) {
                        tn.setLineStart(1);
                        tn.setLineEnd(def.split("\\r?\\n").length);
                        tn.setLinesOfCode(def.split("\\r?\\n").length);
                        // Source filename must match the UI convention "schema.package.object.sql"
                        // (dot-separated), NOT the "$"-separated nodeId, or "view source" 404s.
                        writer.writeSourceFile((schema != null ? schema : "") + ".TRG." + name + ".sql", def);
                    }
                    writer.writeChunk(tn);
                } catch (IOException ex) {
                    errors.add("Trigger node write failed for " + name + ": " + ex.getMessage());
                }
            }

            List<FlowEdge> tedges = new ArrayList<>();

            // ── FIRES_TRIGGER edges from every routine whose matching DML hits this table ──
            if (anyMatchingDml) {
                java.util.Set<String> seenFrom = new java.util.HashSet<>();
                for (String[] d : dmlRefs) {
                    if (!tableU.equals(d[2])) continue;
                    String op = d[3];
                    boolean match = op.startsWith("MERGE") || events.isEmpty()
                            || events.stream().anyMatch(op::startsWith);
                    if (!match) continue;
                    if (!seenFrom.add(d[0])) continue;
                    FlowEdge e = new FlowEdge();
                    e.setFromNodeId(d[0]);
                    e.setToNodeId(trigId);
                    e.setToSchema(schema);
                    e.setToObject(name);
                    e.setEdgeType("FIRES_TRIGGER");
                    e.setDepth(2);
                    tedges.add(e);
                }
            }

            // ── trigger -> each resolved callee: the body's calls join the call graph ──
            // Only for a catalog-emitted node — a source-parsed trigger node already had its own
            // CALL edges written by the BFS, so re-adding them here would double them.
            if (!sourceParsed) {
                java.util.Set<String> seenTo = new java.util.HashSet<>();
                for (CallInfo c : triggerCalls) {
                    if (c == null) continue;
                    String toId = c.getNodeId();
                    if (toId == null || toId.isBlank() || toId.equalsIgnoreCase(trigId)) continue;
                    if (!seenTo.add(toId)) continue;
                    FlowEdge ce = new FlowEdge();
                    ce.setFromNodeId(trigId);
                    ce.setToNodeId(toId);
                    ce.setToSchema(c.getSchema());
                    ce.setToObject(c.getName());
                    ce.setDepth(2);
                    tedges.add(ce);
                }
            }

            // ── PostgreSQL: trigger -> handler function (make the function a reachable node) ──
            if (isPg && !sourceParsed) {
                String fnName   = trigStr(t.get("functionName"));
                String fnSchema = trigStr(t.get("functionSchema"));
                if (fnName != null && !fnName.isBlank()) {
                    FlowNode fnNode = new FlowNode();
                    fnNode.setSchema(fnSchema != null ? fnSchema : schema);
                    fnNode.setObjectName(fnName);
                    fnNode.setObjectType("FUNCTION");
                    fnNode.setReadable(true);
                    fnNode.setDepth(2);
                    fnNode.generateNodeId();
                    String fnId = fnNode.getNodeId();
                    if (emittedFn.add(fnId)) {
                        try {
                            String fnSrc = downloader.downloadAny(fnName);
                            if (fnSrc != null && !fnSrc.isBlank()) {
                                fnNode.setLineStart(1);
                                fnNode.setLineEnd(fnSrc.split("\\r?\\n").length);
                                fnNode.setLinesOfCode(fnSrc.split("\\r?\\n").length);
                                writer.writeSourceFile((fnSchema != null ? fnSchema : schema) + "." + fnName + ".sql", fnSrc);
                            }
                            fnNode.setOrder(++order);
                            writer.writeChunk(fnNode);
                        } catch (Exception ex) {
                            errors.add("Trigger fn node write failed for " + fnName + ": " + ex.getMessage());
                        }
                    }
                    FlowEdge fe = new FlowEdge();
                    fe.setFromNodeId(trigId);
                    fe.setToNodeId(fnId);
                    fe.setToSchema(fnSchema != null ? fnSchema : schema);
                    fe.setToObject(fnName);
                    fe.setEdgeType("TRIGGER_FN");
                    fe.setDepth(2);
                    tedges.add(fe);
                }
            }

            try {
                if (!tedges.isEmpty()) writer.appendEdges(tedges, ++order);
            } catch (IOException ex) {
                errors.add("Trigger edges write failed for " + name + ": " + ex.getMessage());
            }
        }
        log.info("[Crawler] Emitted trigger nodes+edges for " + emittedTrig.size() + " triggers");
    }

    /** The ROW/STATEMENT level implied by a catalog TRIGGER_TYPE (Oracle "AFTER EACH ROW" /
     *  "AFTER STATEMENT", etc.), or null when the catalog does not say. */
    static String triggerLevelFromCatalog(String triggerType, String timing) {
        String tt = triggerType != null ? triggerType.toUpperCase(Locale.ROOT)
                : (timing != null ? timing.toUpperCase(Locale.ROOT) : null);
        if (tt == null) return null;
        if (tt.contains("STATEMENT")) return "STATEMENT";
        if (tt.contains("EACH ROW") || tt.endsWith("ROW")) return "ROW";
        return null;
    }

    /**
     * Convert the trigger map's {@code calls} list (the {@code {name, packageName, type, schema,
     * lines[]}} maps TriggerDiscoverer produces) into {@link CallInfo}s, so the trigger node detail
     * is written through the same {@code groupCalls} path every other node uses. Never null.
     */
    @SuppressWarnings("unchecked")
    private static List<CallInfo> toCallInfos(Object callsObj) {
        List<CallInfo> out = new ArrayList<>();
        if (!(callsObj instanceof List<?> list)) return out;
        for (Object o : list) {
            if (!(o instanceof Map<?, ?> cm)) continue;
            Object nameObj = cm.get("name");
            if (nameObj == null) nameObj = cm.get("objectName");
            if (nameObj == null) continue;
            CallInfo ci = new CallInfo();
            ci.setName(String.valueOf(nameObj));
            Object pkg = cm.get("packageName");
            if (pkg != null) ci.setPackageName(String.valueOf(pkg));
            Object type = cm.get("type");
            ci.setType(type != null ? String.valueOf(type) : "EXTERNAL");
            Object schema = cm.get("schema");
            if (schema != null) ci.setSchema(String.valueOf(schema));
            Object lines = cm.get("lines");
            if (lines instanceof List<?> ln && !ln.isEmpty()) {
                Object first = ln.get(0);
                if (first instanceof Number num) ci.setLine(num.intValue());
            }
            out.add(ci);
        }
        return out;
    }

    /**
     * Registers one crawled node's id under its bare object name and, for a package member, under
     * {@code PACKAGE.SUBPROGRAM} — the two shapes a call target can take. First writer wins, so a
     * later overload does not displace the canonical node. Null/blank ids and names are ignored.
     */
    static void recordCrawledNodeId(Map<String, String> into, FlowNode node) {
        if (into == null || node == null) return;
        String id = node.getNodeId();
        String obj = node.getObjectName();
        if (id == null || id.isEmpty() || obj == null || obj.isBlank()) return;
        String pkg = node.getPackageName();
        if (pkg != null && !pkg.isBlank()) {
            into.putIfAbsent((pkg + "." + obj).toUpperCase(Locale.ROOT), id);
        }
        into.putIfAbsent(obj.toUpperCase(Locale.ROOT), id);
    }

    /**
     * Records a SOURCE-PARSED trigger node's id keyed {@code SCHEMA.NAME} (upper) so the later
     * catalog-trigger pass can recognise a trigger it must not emit a second node for. Non-trigger
     * nodes and nodes without an id/name are ignored.
     */
    static void recordCrawledTriggerNodeId(Map<String, String> into, FlowNode node) {
        if (into == null || node == null) return;
        if (!"TRIGGER".equalsIgnoreCase(node.getObjectType())) return;
        String key = triggerDedupKey(node.getSchema(), node.getObjectName());
        String id = node.getNodeId();
        if (key == null || id == null || id.isEmpty()) return;
        into.putIfAbsent(key, id);
    }

    /** The dedup key shared by the crawler's trigger-node registry and the catalog-trigger pass:
     *  {@code SCHEMA.NAME} upper, or null when the name is blank. */
    static String triggerDedupKey(String schema, String name) {
        if (name == null || name.isBlank()) return null;
        return (schema == null ? "" : schema.toUpperCase(Locale.ROOT)) + "." + name.toUpperCase(Locale.ROOT);
    }

    /**
     * Resolves the DB-discovered trigger body's outbound calls against the crawled estate. The
     * trigger is a catalog artefact, not a crawled node, so its callees arrive unresolved; each is
     * looked up by {@code PACKAGE.NAME} then bare {@code NAME} and, when found, stamped with the
     * crawled target's node id (for the UI cross-link). A call whose target was not crawled is left
     * exactly as it arrived — EXTERNAL, no node id. Type is deliberately NOT changed: the trigger
     * node's package namespace is {@code TRG}, and {@code groupCalls} would fold a bare INTERNAL
     * call into that namespace, corrupting the target name.
     */
    static void resolveTriggerCallNodeIds(List<CallInfo> calls, Map<String, String> crawledNodeIds) {
        if (calls == null || calls.isEmpty() || crawledNodeIds == null || crawledNodeIds.isEmpty()) return;
        for (CallInfo c : calls) {
            if (c == null) continue;
            String name = c.getName();
            if (name == null || name.isBlank()) continue;
            String id = null;
            String pkg = c.getPackageName();
            if (pkg != null && !pkg.isBlank()) {
                id = crawledNodeIds.get((pkg + "." + name).toUpperCase(Locale.ROOT));
            }
            if (id == null) id = crawledNodeIds.get(name.toUpperCase(Locale.ROOT));
            if (id != null) c.setNodeId(id);
        }
    }

    private void writeNodeSource(ChunkedFlowWriter writer, FlowNode node, String ownerSchema) {
        try {
            String schema = ownerSchema != null ? ownerSchema : "UNKNOWN";
            String objName = node.getObjectName() != null ? node.getObjectName() : "UNKNOWN";
            String pkgName = node.getPackageName();

            if (pkgName != null && !pkgName.isEmpty()) {
                String source = downloader.getCachedSource(pkgName);
                if (source != null) {
                    // Write extracted subprogram
                    String fileName = schema + "." + pkgName + "." + objName + ".sql";
                    if (!writer.sourceExists(fileName) && node.getLineStart() > 0 && node.getLineEnd() > 0) {
                        String[] lines = source.split("\\r?\\n");
                        int start = Math.max(0, node.getLineStart() - 1);
                        int end = Math.min(lines.length, node.getLineEnd());
                        StringBuilder extracted = new StringBuilder();
                        extracted.append("-- Extracted from ").append(schema).append(".").append(pkgName).append("\n");
                        extracted.append("-- Lines ").append(node.getLineStart()).append(" to ").append(node.getLineEnd()).append("\n\n");
                        for (int i = start; i < end; i++) {
                            extracted.append(lines[i]).append("\n");
                        }
                        writer.writeSourceFile(fileName, extracted.toString());
                    }
                    // Write full package body once
                    String pkbFile = schema + "." + pkgName + ".pkb";
                    if (!writer.sourceExists(pkbFile)) {
                        writer.writeSourceFile(pkbFile, source);
                    }
                }
            } else {
                // Try bare name first (Oracle), then the schema-qualified key — SQL Server / DB2
                // standalone objects are fetched+cached under "SCHEMA.NAME", so a bare lookup
                // would miss and the sub-proc source would never be written (source-not-found).
                String source = downloader.getCachedSource(objName);
                String typeKey = objName;
                if (source == null && schema != null && !"UNKNOWN".equals(schema)) {
                    String q = schema + "." + objName;
                    String qs = downloader.getCachedSource(q);
                    if (qs != null) { source = qs; typeKey = q; }
                }
                if (source != null) {
                    String ext = "sql";
                    String type = downloader.getCachedType(typeKey);
                    if ("FUNCTION".equals(type)) ext = "fnc";
                    else if ("PROCEDURE".equals(type)) ext = "prc";
                    String fileName = schema + "." + objName + "." + ext;
                    if (!writer.sourceExists(fileName)) {
                        writer.writeSourceFile(fileName, source);
                    }
                    // Record the REAL name (.prc/.fnc), so the node/index sourceFile matches the
                    // artifact exactly and a future read needs no extension probing.
                    writer.recordSourceFile(node.getSchema(), pkgName, objName, fileName);
                }
            }
        } catch (Exception e) {
            log.info("[Crawler] Source write error: " + e.getMessage());
        }
    }

    private FlowResult buildInMemoryResult(String entryPoint, String entrySchema,
                                            long startTime, int maxDepthReached,
                                            List<String> errors,
                                            Map<String, SchemaTableInfo> tableInfoMap) {
        FlowResult result = new FlowResult();
        result.setEntryPoint(entryPoint);
        result.setEntrySchema(entrySchema);
        result.setAllTables(new ArrayList<>(tableInfoMap.values()));
        result.setMaxDepthReached(maxDepthReached);
        result.setErrors(errors);
        result.setCrawlTimeMs(System.currentTimeMillis() - startTime);
        result.setDbCallCount(downloader.getConnManager() != null
                ? downloader.getConnManager().getDbCallCount() : 0);
        result.setParseMs(parseNanos.get() / 1_000_000);
        result.setDiscoverMs(discoverNanos.get() / 1_000_000);
        result.setSaveMs(saveNanos.get() / 1_000_000);
        return result;
    }

    /**
     * Compute per-node metrics: linesOfCode, callCount, calledBy, statementSummary.
     */
    /**
     * MID-026 (§111→§177): used to mutate {@code linesOfCode}/{@code statementSummary}/
     * {@code callCount}/{@code calledBy} onto the {@code FlowNode} instances in {@code nodes} in
     * place, which {@code ChunkedFlowWriter.writeApiFiles}'s remaining two per-node loops then read
     * off those same object instances later — shared mutable object identity as the data-passing
     * mechanism between this class and that one, which breaks the moment nodes are streamed
     * one-at-a-time and discarded rather than kept resident as a full list. {@code linesOfCode}
     * itself needed no change — it is already set earlier, during node construction (see
     * {@code DependencyCrawler}'s own other {@code setLinesOfCode} call sites), so recomputing it
     * here was a harmless no-op; kept as-is. {@code calledBy} is dead output — grepped every reader
     * in {@code ChunkedFlowWriter} and found none, the same "never actually written into any real
     * artifact" shape as MID-102's own {@code calledBy} finding at the entity-reader level — dropped
     * entirely rather than threaded through for nothing. Only {@code statementSummary}/
     * {@code callCount} are real, read outputs, and now return via an explicit map keyed by node id
     * instead of an in-place mutation, so a future streaming rewrite of {@code writeApiFiles} does
     * not need to keep every {@code FlowNode} resident just to read these two fields back later.
     */
    /**
     * MID-026 (final piece) — was {@code computeNodeMetrics(List<FlowNode> nodes, ...)}, which
     * required the caller to hold every node's full body resident. Converted to stream each node's
     * body from its chunk file on demand via {@link ChunkedFlowWriter#buildChunkIndex()}/
     * {@link ChunkedFlowWriter#readNodeChunk}, the same primitive the writer's own bounded-conversion
     * series (§166-177) already proved correct — this is the first caller outside
     * {@code ChunkedFlowWriter} itself to use it. The old {@code linesOfCode} recomputation loop was
     * dropped entirely rather than converted: §177 already established it was a harmless no-op
     * (the value is set earlier, during node construction) that nothing downstream depends on this
     * method for.
     */
    private Map<String, NodeMetrics> computeNodeMetrics(ChunkedFlowWriter writer, List<FlowEdge> edges)
            throws IOException {
        Map<String, ArtifactKey> chunkIndex = writer.buildChunkIndex();
        Set<String> nodeIds = chunkIndex.keySet();

        Map<String, Map<String, Integer>> statementSummaries = new LinkedHashMap<>();
        for (String nodeId : nodeIds) {
            FlowNode n = writer.readNodeChunk(nodeId, chunkIndex);
            if (n == null) continue;
            Map<String, Integer> stmtSummary = new LinkedHashMap<>();
            if (n.getStatements() != null) {
                for (StatementInfo s : n.getStatements()) {
                    String type = s.getType() != null ? s.getType().toUpperCase() : "OTHER";
                    stmtSummary.merge(type, 1, Integer::sum);
                }
            }
            statementSummaries.put(nodeId, stmtSummary);
        }

        // Compute callCount from edges
        Map<String, Integer> callCounts = new LinkedHashMap<>();

        for (FlowEdge e : edges) {
            String toId = e.getToNodeId();
            if (toId == null) continue;

            // Match toNodeId against actual nodes (edges have partial nodeId without params)
            String matchedToId = findMatchingNodeId(toId, nodeIds);
            if (matchedToId != null) {
                callCounts.merge(matchedToId, 1, Integer::sum);
            }
        }

        Map<String, NodeMetrics> result = new LinkedHashMap<>();
        for (String nodeId : nodeIds) {
            result.put(nodeId, new NodeMetrics(
                    statementSummaries.getOrDefault(nodeId, Map.of()),
                    callCounts.getOrDefault(nodeId, 0)));
        }
        return result;
    }

    /**
     * {@code computeNodeMetrics}'s own explicit output shape — see that method's own doc comment
     * for why this replaced in-place {@code FlowNode} mutation.
     */
    public record NodeMetrics(Map<String, Integer> statementSummary, int callCount) {
    }

    /**
     * MID-026 — retyped from {@code Map<String, FlowNode>} to {@code Set<String>}: this method
     * only ever reads the key set (confirmed directly), never a node's body, so
     * {@code computeNodeMetrics}'s own conversion away from holding full bodies didn't need to
     * change this signature's real contract, only its declared type.
     */
    private String findMatchingNodeId(String edgeId, Set<String> nodeIds) {
        if (nodeIds.contains(edgeId)) return edgeId;
        for (String nid : nodeIds) {
            if (nid.startsWith(edgeId + "$") || nid.startsWith(edgeId + "_")) return nid;
        }
        return null;
    }

    /**
     * Returns true if the fetched source looks truncated (cut off mid-body) for the ACTIVE
     * dialect. The decision is delegated to that dialect's own SourceFetchStrategy — there is
     * no cross-dialect logic here, because whether truncation is even possible depends entirely
     * on how each dialect retrieves source (Oracle ALL_SOURCE chunks can truncate; MySQL/DB2/
     * SQL Server fetch complete text; Postgres is judged by dollar-quote balance).
     */
    private boolean isSourceTruncated(String source) {
        if (source == null || source.isBlank()) return false;
        String dbType = (downloader.getConnManager() != null)
                ? downloader.getConnManager().getDbType() : null;
        return com.plsql.parser.multidb.SourceFetchStrategyFactory.get(dbType).isTruncated(source);
    }

    /**
     * Download source from DB and parse it. Results are cached:
     *   1. In-memory parseCache (fastest — within same run)
     *   2. Disk parseResultCache keyed on source MD5 (skips ANTLR on re-runs if source unchanged)
     */
    private ParseResult downloadAndParse(String packageName, String subprogramName) {
        return downloadAndParse(packageName, subprogramName, null);
    }

    private ParseResult downloadAndParse(String packageName, String subprogramName, String schema) {
        return downloadAndParse(packageName, subprogramName, schema, false, null);
    }

    /**
     * W1: {@code overloadId} is a Postgres {@code pg_proc.oid} / DB2 {@code SPECIFICNAME} addressing
     * ONE exact standalone overload, so its source is fetched individually instead of a name-based
     * LIMIT 1. Null for every other case (all existing callers) — behaviour is then unchanged.
     */
    private ParseResult downloadAndParse(String packageName, String subprogramName, String schema,
                                         String overloadId) {
        return downloadAndParse(packageName, subprogramName, schema, false, overloadId);
    }

    /**
     * @param packageOnly when {@code packageName} is null, fetch ONLY PACKAGE BODY / PACKAGE for the
     *                    name instead of the all-types probe {@code downloadAny} does. Used by the
     *                    depth-0 entry fallback, which must not reinterpret a bare name that is
     *                    genuinely something else.
     * @param overloadId  per-overload catalog id (Postgres oid / DB2 SPECIFICNAME), else null.
     */
    private ParseResult downloadAndParse(String packageName, String subprogramName, String schema,
                                         boolean packageOnly, String overloadId) {
        String objectName = packageName != null ? packageName : subprogramName;
        // Standalone objects: qualify the cache key by schema so same-named objects in
        // different schemas don't reuse each other's parse. The FETCH name is schema-qualified
        // ONLY for SQL Server, whose source query parses a "schema.name" hint; Oracle's source
        // query matches on the bare object name, so a "SCHEMA.NAME" fetch would find nothing.
        boolean standaloneWithSchema = packageName == null && schema != null && !schema.isEmpty();
        boolean isSqlServer = downloader.getConnManager() != null && downloader.getConnManager().isSqlServer();
        String fetchName = (standaloneWithSchema && isSqlServer) ? schema + "." + subprogramName : subprogramName;
        String cacheKey = (standaloneWithSchema ? (schema + "$" + subprogramName) : objectName).toUpperCase();
        // Overloads share a name: keep each overload's parse separate so the second overload does
        // not reuse the first's ParseResult. Content-addressed parseResultCache is unaffected.
        if (overloadId != null) cacheKey = cacheKey + "#OVL:" + overloadId;

        // Fast-skip: confirmed absent from Oracle via prior ALL_OBJECTS batch check. Deliberately
        // bypassed for the entry package fallback: that is an explicit second attempt, and skipping
        // it would make the "tried ..." error message claim a probe that never happened.
        if (!packageOnly && knownMissingObjects.contains(cacheKey)) {
            log.debug("[Crawler] Skipping confirmed-missing object: {}", objectName);
            return null;
        }

        // Fast-skip: compound pkg.sub key confirmed as non-subprogram (e.g. package constant)
        if (packageName != null && subprogramName != null) {
            String compoundKey = (packageName + "." + subprogramName).toUpperCase();
            if (knownMissingObjects.contains(compoundKey)) {
                log.debug("[Crawler] Skipping confirmed non-subprogram: {}", compoundKey);
                return null;
            }
        }

        // In-memory cache — instant hit within same run
        ParseResult cached = parseCache.get(cacheKey);
        if (cached != null) return cached;

        String source = null;
        long tDiscover0 = System.nanoTime();
        if (packageName != null) {
            source = downloader.downloadPackageBody(packageName);
            if (source == null) source = downloader.downloadPackageSpec(packageName);
        } else if (packageOnly) {
            source = downloader.downloadPackageBody(fetchName);
            if (source == null) source = downloader.downloadPackageSpec(fetchName);
        } else if (overloadId != null && downloader.getConnManager() != null
                && downloader.getConnManager().isPostgres()) {
            // Exact overload by oid — never a name-based LIMIT 1.
            source = downloader.downloadPostgresSourceByOid(overloadId);
        } else if (overloadId != null && downloader.getConnManager() != null
                && downloader.getConnManager().isDB2()) {
            // Exact overload by SPECIFICNAME.
            source = downloader.downloadDb2SourceBySpecificName(overloadId, schema);
        } else {
            source = downloader.downloadAny(fetchName);
        }
        discoverNanos.addAndGet(System.nanoTime() - tDiscover0);

        if (source == null) {
            // Queue this name for the next ALL_OBJECTS batch check, then run the batch.
            // This fires at most once per unique unknown name — confirms cursor vars /
            // collection types / local identifiers so they are never re-queried.
            pendingMissingCheck.add(cacheKey);
            flushMissingBatch();
            log.info("[Crawler] No source found for: " + objectName);
            return null;
        }

        // Disk parse cache — skip ANTLR if same source was parsed in any prior run.
        // Fast-rerun only: a fresh crawl re-parses every source (the write below still runs).
        if (fastReRun && parseResultCache != null) {
            ParseResult diskCached = parseResultCache.get(source);
            if (diskCached != null) {
                parseCache.put(cacheKey, diskCached);
                log.info("[Crawler] ParseCache HIT (skipped ANTLR) for {}", objectName);
                return diskCached;
            }
        }

        // Parse — use the dialect-specific parser for MySQL/SQL Server, ANTLR PL/SQL for Oracle.
        // The parseLock serializes this with the closure wave's pipelined parses (A4): the ANTLR
        // parsers share a static DFA cache, so parse + periodic clearDFA must not run concurrently.
        long t0 = System.currentTimeMillis();
        long tParse0 = System.nanoTime();
        ParseResult result;
        synchronized (parseLock) {
            result = parseSourceForDialect(source, objectName);
        }
        String dialect = downloader.getConnManager() == null ? "ANTLR"
                : downloader.getConnManager().isMySQL() ? "MySQL"
                : downloader.getConnManager().isSqlServer() ? "SQL Server"
                : downloader.getConnManager().isDB2() ? "DB2"
                : downloader.getConnManager().isPostgres() ? "Postgres"
                : "ANTLR";
        log.info("[Crawler] {} parsed {} in {}ms", dialect, objectName, System.currentTimeMillis() - t0);
        parseNanos.addAndGet(System.nanoTime() - tParse0);
        parseCache.put(cacheKey, result);

        // Persist to disk so next run skips ANTLR for same source
        if (parseResultCache != null && result != null) {
            parseResultCache.put(source, result);
        }

        return result;
    }

    /**
     * The depth-0 entry fallback: fetch {@code name} as a PACKAGE BODY (then PACKAGE) and return its
     * parse result when the source really is a package.
     *
     * <p>Returns null when the caller already asked for a package (nothing to fall back from), when
     * nothing was fetched, or when what was fetched does not carry a PACKAGE-typed object — the last
     * check is what keeps a genuinely-missing standalone routine from being silently re-read as
     * something it is not.
     */
    private ParseResult tryPackageFallback(String name, String schema) {
        if (isPackageType(entryObjectType)) return null;
        ParseResult asPackage = downloadAndParse(null, name, schema, true, null);
        if (asPackage == null || asPackage.getObjects() == null || asPackage.getObjects().isEmpty()) {
            return null;
        }
        for (ParsedObject obj : asPackage.getObjects()) {
            if (isPackageType(obj.getType())) return asPackage;
        }
        return null;
    }

    /** True when {@code type} names a package (spec or body), case-insensitively. */
    private static boolean isPackageType(String type) {
        return type != null && type.toUpperCase().contains("PACKAGE");
    }

    /**
     * The object types the depth-0 lookup probed, in the order it probed them — what the error
     * message reports so a wrong {@code objectType} is visible.
     *
     * <p>For a package member the package body and spec are what was fetched. For a bare name the
     * all-types fetch covers PACKAGE BODY, PROCEDURE, FUNCTION and PACKAGE; when the caller named a
     * type that is reported first, because it is the one the user believes they asked for.
     */
    private List<String> entryTypesTried(boolean packageMember) {
        LinkedHashSet<String> types = new LinkedHashSet<>();
        if (packageMember) {
            types.add("PACKAGE BODY");
            types.add("PACKAGE");
            return new ArrayList<>(types);
        }
        String requested = normalizeType(entryObjectType);
        if (requested != null) types.add(requested);
        else { types.add("PROCEDURE"); types.add("FUNCTION"); }
        types.add("PACKAGE BODY");
        types.add("PACKAGE");
        return new ArrayList<>(types);
    }

    private static String normalizeType(String type) {
        if (type == null || type.isBlank()) return null;
        String t = type.trim().toUpperCase().replace('_', ' ');
        return t.equals("PACKAGE BODY") || t.equals("PACKAGE") || t.equals("PROCEDURE")
                || t.equals("FUNCTION") || t.equals("VIEW") || t.equals("TRIGGER") ? t : null;
    }

    /**
     * The owners the lookup searched, as a comma-separated list for the error message: the
     * connection's configured schemas plus the crawl's explicitly-supplied schema, when it was not
     * already among them. Never throws — a resolver that cannot answer produces a "(none
     * configured)" note rather than masking the original lookup failure.
     */
    private String describeSchemasSearched(String explicitSchema) {
        LinkedHashSet<String> schemas = new LinkedHashSet<>();
        try {
            if (schemaResolver != null && schemaResolver.getConfiguredSchemas() != null) {
                schemas.addAll(schemaResolver.getConfiguredSchemas());
            }
        } catch (Exception ignored) {
            // fall through with whatever we have
        }
        if (explicitSchema != null && !explicitSchema.isBlank()) schemas.add(explicitSchema.toUpperCase());
        if (schemas.isEmpty()) return "(none configured — check the connection's schema list)";
        return String.join(", ", schemas);
    }

    /**
     * Prefetch sources for all transitive dependencies concurrently, then warm
     * parseCache from disk so bulkPreResolveAllTables() has real data to work with.
     */
    private void prefetchSources(Set<String> depNames) {
        if (depNames == null || depNames.isEmpty()) return;
        // This crawl runs on a worker whose installed job context (SsoJobContext, wrapped around
        // ParserAnalysisExecutor) makes the submitter's stores and credentials resolvable; the
        // "src-prefetch" pool threads below have neither, so SourceDownloader.checkLocalCache's
        // store.list/store.read would route/sign as the shared default target. Capture HERE, on the
        // crawl thread and before the pool exists, then re-install per task — mirroring
        // jar-analyzer-core/SourceEnrichmentService. Capture both halves: the job snapshot
        // (credentials + stores) and the store routing scope. Null-safe: nothing captured means
        // install is a no-op, preserving legacy/no-SSO behaviour exactly.
        final CapturedSsoContext prefetchContext = CapturedSsoContext.capture(null);
        final RequestScopedArtifactStore.Scope prefetchScope = store instanceof RequestScopedArtifactStore routed
                ? routed.captureScope() : RequestScopedArtifactStore.Scope.NONE;
        int threads = Math.min(6, depNames.size());
        ExecutorService pool = Executors.newFixedThreadPool(threads,
                r -> { Thread t = new Thread(r, "src-prefetch"); t.setDaemon(true); return t; });
        for (String name : depNames) {
            String upperName = name.toUpperCase();
            if (parseCache.containsKey(upperName) || knownMissingObjects.contains(upperName)) continue;
            if (isBuiltInPackage(name)) continue; // never prefetch Oracle system packages
            final String n = name;
            pool.submit(() -> {
                // try-with-resources close() can throw, but the catch below covers it: the per-task
                // installs are best-effort and must never fail the prefetch.
                try (AutoCloseable scoped = prefetchScope.install();
                     AutoCloseable sso = prefetchContext == null ? null : prefetchContext.install()) {
                    String src = downloader.downloadSource(n, "PACKAGE BODY");
                    if (src == null) src = downloader.downloadSource(n, "PROCEDURE");
                    if (src == null) downloader.downloadSource(n, "FUNCTION");
                } catch (Exception ignored) {}
            });
        }
        pool.shutdown();
        try {
            // Wait for all downloads so bulkPreResolveAllTables sees the full sourceCache.
            pool.awaitTermination(120, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("[Crawler] Prefetch interrupted after timeout");
        }
        log.info("[Crawler] Prefetch done for {} objects", depNames.size());

        // Warm parseCache from disk for every source that was just downloaded.
        // Without this, bulkPreResolveAllTables() has nothing to scan on the first run
        // (parseCache is empty until BFS processes each node).
        if (parseResultCache != null) {
            int warmed = 0;
            for (String name : depNames) {
                String upperName = name.toUpperCase();
                if (parseCache.containsKey(upperName)) continue;
                String src = downloader.getCachedSource(name);
                if (src == null) continue;
                ParseResult pr = parseResultCache.get(src);
                if (pr != null) {
                    parseCache.put(upperName, pr);
                    warmed++;
                }
            }
            if (warmed > 0) {
                log.info("[Crawler] ParseCache warmed from disk: {} entries", warmed);
            }
        }
    }

    // ── A1 closure-up-front wave ────────────────────────────────────────────────────────────────

    /**
     * True when every object on {@code level} is already available locally from the closure wave —
     * so the BFS can scope the level purely in memory (no catalog query, no source fan-out). A
     * built-in package, a confirmed-missing name and an already-parsed object all count as covered
     * (none of them needs a fetch); anything else must be in the closure set.
     */
    private boolean levelCoveredByClosure(Collection<BfsItem> level) {
        Set<String> closure = closureNames;
        if (closure == null) return false;
        for (BfsItem item : level) {
            String name = item.packageName != null ? item.packageName : item.subprogramName;
            if (name == null || name.isBlank()) continue;
            String upper = name.toUpperCase();
            if (isBuiltInPackage(upper)) continue;
            if (knownMissingObjects.contains(upper)) continue;
            if (parseCache.containsKey(upper)) continue;
            if (closure.contains(upper)) continue;
            return false;
        }
        return true;
    }

    /**
     * Fetch the whole reachable closure in ONE wave. Oracle pulls the sources in bulk (A2) — a
     * handful of multi-row ALL_SOURCE queries instead of N round trips — and the per-object
     * fallback covers any name the bulk pull could not resolve (and every non-Oracle dialect).
     * Each source is parsed as soon as it is fetched (A4), overlapping the CPU parse with the next
     * network fetch, and the parsed result is stored so the BFS pays no ANTLR cost.
     *
     * @return the upper-cased closure names the wave attempted (never null).
     */
    private Set<String> runClosureWave(Set<String> depNames) {
        Set<String> closure = new java.util.LinkedHashSet<>();
        for (String n : depNames) {
            if (n != null && !n.isBlank()) closure.add(n.toUpperCase());
        }
        if (closure.isEmpty()) return closure;

        // A2: one bulk pull for everything the dialect can batch.
        Map<String, String> bulk = Collections.emptyMap();
        try {
            Map<String, String> fetched = downloader.fetchSourcesBulk(closure);
            if (fetched != null) bulk = fetched;
        } catch (Exception e) {
            log.info("[Crawler] bulk source pull skipped: " + e.getMessage());
        }
        for (Map.Entry<String, String> e : bulk.entrySet()) {
            // A4: parse immediately; do not wait for the rest of the wave.
            pipelineParse(e.getKey(), e.getValue(), true);
        }

        // Per-object fan-out for the remainder (and all non-Oracle dialects). The task fetches and
        // then parses in the same worker, so worker B keeps fetching while worker A parses.
        Set<String> remaining = new java.util.LinkedHashSet<>(closure);
        remaining.removeAll(bulk.keySet());
        remaining.removeIf(n -> parseCache.containsKey(n) || knownMissingObjects.contains(n));
        if (!remaining.isEmpty()) {
            List<Runnable> tasks = new ArrayList<>(remaining.size());
            for (String n : remaining) {
                if (isBuiltInPackage(n)) continue;
                tasks.add(() -> {
                    try {
                        String src = downloader.downloadAny(n);
                        if (src == null) src = downloader.getCachedSource(n);
                        if (src != null) pipelineParse(n, src, isPackageCached(n));
                    } catch (Exception ignored) {
                        // a failed wave fetch just means the BFS fetches it (again) on demand
                    }
                });
            }
            fanOut(tasks, sourceParallelism);
        }

        // Warm parseCache from disk for every name the wave fetched, so bulkPreResolveAllTables()
        // (and the node-cache peek) see the full set — same guarantee as the old prefetchSources.
        // Also prime knownObjectTypes so the node-cache peek probes the exact type (the skipped
        // per-level prepareLevelCatalog would normally do this).
        for (String n : closure) {
            String[] snap = downloader.snapshotMeta(n);
            if (snap != null && snap[1] != null) {
                knownObjectTypes.put(n, snap[1]);
            } else {
                String t = downloader.getCachedType(n);
                if (t != null) knownObjectTypes.put(n, t);
            }
        }
        if (parseResultCache != null) {
            int warmed = 0;
            for (String n : closure) {
                if (parseCache.containsKey(n)) continue;
                String src = downloader.getCachedSource(n);
                if (src == null) continue;
                ParseResult pr = parseResultCache.get(src);
                if (pr != null) {
                    parseCache.put(n, pr);
                    warmed++;
                }
            }
            if (warmed > 0) log.info("[Crawler] ParseCache warmed from disk: {} entries", warmed);
        }
        log.info("[Crawler] Closure wave: {} bulk + {} per-object, {} sources cached",
                bulk.size(), remaining.size(), countCached(closure));
        return closure;
    }

    private int countCached(Collection<String> names) {
        int n = 0;
        for (String name : names) if (downloader.getCachedSource(name) != null) n++;
        return n;
    }

    private boolean isPackageCached(String name) {
        String type = downloader.getCachedType(name);
        return type != null && type.toUpperCase().contains("PACKAGE");
    }

    /**
     * A4: parse {@code source} now and store the result, so the BFS's {@code downloadAndParse}
     * finds it in the content-addressed parse cache and skips ANTLR. Package results are also put
     * in the name-keyed parseCache (the BFS's key for a package is the package name). Best-effort:
     * a parse failure just leaves the BFS to parse it normally.
     */
    private void pipelineParse(String name, String source, boolean packageResult) {
        if (source == null || source.isBlank()) return;
        if (fastReRun && parseResultCache != null && parseResultCache.get(source) != null) {
            return; // already parsed in a prior run — nothing to do (fast re-run only)
        }
        ParseResult result;
        try {
            synchronized (parseLock) {
                result = parseSourceForDialect(source, name);
            }
        } catch (Exception e) {
            return;
        }
        if (result == null) return;
        if (parseResultCache != null) parseResultCache.put(source, result);
        if (packageResult) parseCache.putIfAbsent(name.toUpperCase(), result);
    }

    /** Parse {@code source} with the dialect-appropriate parser (shared by BFS and the wave). */
    private ParseResult parseSourceForDialect(String source, String objectName) {
        if (downloader.getConnManager() != null && downloader.getConnManager().isMySQL()) {
            return new MySqlProcedureParser().parse(source, objectName, downloader.getConnManager().getMySqlSchema());
        } else if (downloader.getConnManager() != null && downloader.getConnManager().isSqlServer()) {
            return new SqlServerProcedureParser().parse(source, objectName, downloader.getConnManager().getSqlServerSchema());
        } else if (downloader.getConnManager() != null && downloader.getConnManager().isDB2()) {
            return new Db2ProcedureParser().parse(source, objectName, downloader.getConnManager().getDb2Schema());
        } else if (downloader.getConnManager() != null && downloader.getConnManager().isPostgres()) {
            return new PostgresProcedureParser().parse(source, objectName, downloader.getConnManager().getPostgresSchema());
        }
        return engine.parseContent(source, objectName + ".sql");
    }

    // ── Parallel per-level fan-out ──────────────────────────────────────────────────────────────

    /**
     * Run {@code tasks} on a bounded, short-lived pool, carrying the submitting thread's identity to
     * every worker. This is the {@code src-prefetch} pattern generalized: capture the SSO snapshot
     * (credentials + stores) and the routed-store scope HERE, on the crawl thread, then install both
     * per task with try-with-resources so each worker signs as the same principal and reads/writes
     * the submitter's per-user store. Null-safe: with nothing captured, install is a no-op and legacy
     * / CLI behaviour is preserved exactly. A task that throws never fails the batch.
     */
    private void fanOut(Collection<Runnable> tasks, int maxThreads) {
        if (tasks == null || tasks.isEmpty()) return;
        final CapturedSsoContext ctx = CapturedSsoContext.capture(null);
        final RequestScopedArtifactStore.Scope scope = store instanceof RequestScopedArtifactStore routed
                ? routed.captureScope() : RequestScopedArtifactStore.Scope.NONE;
        int threads = Math.max(1, Math.min(maxThreads, tasks.size()));
        ExecutorService pool = Executors.newFixedThreadPool(threads,
                r -> { Thread t = new Thread(r, "crawl-fanout"); t.setDaemon(true); return t; });
        for (Runnable task : tasks) {
            pool.submit(() -> {
                try (AutoCloseable scoped = scope.install();
                     AutoCloseable sso = ctx == null ? null : ctx.install()) {
                    task.run();
                } catch (Exception ignored) {
                    // best-effort fan-out: one failed task must not abort the level
                }
            });
        }
        pool.shutdown();
        try {
            pool.awaitTermination(180, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("[Crawler] fan-out interrupted after timeout");
        }
    }

    /** Result of a batched ALL_OBJECTS lookup: resolved names and names whose chunk failed. */
    private record TypeLookup(Map<String, String> found, Set<String> unresolved) {}

    /**
     * One batched ALL_OBJECTS lookup for {@code names}, split into chunks run IN PARALLEL on the
     * bounded catalog pool (a configurable cap). Each chunk borrows its own pooled connection.
     * Returns the best object_type per name (PACKAGE BODY preferred) plus the names belonging to
     * chunks that failed — those are left for a later level / individual retry rather than being
     * wrongly marked missing. A null connection manager yields all names unresolved.
     */
    private TypeLookup queryObjectTypes(Collection<String> names) {
        Map<String, String> found = new ConcurrentHashMap<>();
        Set<String> unresolved = ConcurrentHashMap.newKeySet();
        if (names == null || names.isEmpty()) return new TypeLookup(found, unresolved);
        if (downloader.getConnManager() == null) {
            unresolved.addAll(names);
            return new TypeLookup(found, unresolved);
        }
        List<String> nameList = new ArrayList<>(names);
        List<List<String>> chunks = new ArrayList<>();
        int size = Math.max(1, catalogBatchSize);
        for (int i = 0; i < nameList.size(); i += size) {
            chunks.add(new ArrayList<>(nameList.subList(i, Math.min(i + size, nameList.size()))));
        }
        List<Runnable> tasks = new ArrayList<>(chunks.size());
        for (List<String> chunk : chunks) {
            tasks.add(() -> {
                java.sql.Connection conn = null;
                try {
                    conn = downloader.getConnManager().getAnyConnection();
                    String ph = String.join(",", Collections.nCopies(chunk.size(), "?"));
                    // Include owner filter so we only count objects our schemas can actually download.
                    // Return object_type so nodeCache peek can target exactly the right type (1 call
                    // vs 2). PACKAGE BODY ranks first so it wins when both PACKAGE and PACKAGE BODY exist.
                    String sql = "SELECT object_name, object_type FROM all_objects "
                            + "WHERE object_name IN (" + ph + ") "
                            + "AND object_type IN ('PACKAGE','PACKAGE BODY','PROCEDURE','FUNCTION') "
                            + "AND owner IN (" + buildSchemaInClause() + ") "
                            + "ORDER BY CASE object_type WHEN 'PACKAGE BODY' THEN 1 WHEN 'PROCEDURE' THEN 2 WHEN 'FUNCTION' THEN 3 ELSE 4 END";
                    downloader.getConnManager().incrementDbCalls();
                    try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
                        for (int j = 0; j < chunk.size(); j++) ps.setString(j + 1, chunk.get(j));
                        try (java.sql.ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                String name = rs.getString(1).toUpperCase();
                                String type = rs.getString(2).toUpperCase();
                                found.putIfAbsent(name, type); // first row wins (ORDER BY rank)
                            }
                        }
                    }
                    downloader.getConnManager().returnToPool(conn);
                    conn = null;
                } catch (Exception e) {
                    unresolved.addAll(chunk);
                    log.warn("[Crawler] catalog chunk ({} names) failed: {}", chunk.size(), e.getMessage());
                } finally {
                    // Never leak the connection/permit on error (would starve the pool over time).
                    if (conn != null) downloader.getConnManager().closeAndDiscard(conn);
                }
            });
        }
        fanOut(tasks, catalogParallelism);
        return new TypeLookup(found, unresolved);
    }

    /**
     * The ONE catalog query per BFS level (central decision, dedup fix): the union of
     * <ul>
     *   <li>the level's own objects (owner / type / LAST_DDL, for the source-cache validation and
     *       for priming the type map before the node-cache peek's negative probes),</li>
     *   <li>names queued for the pending resolve batch (external calls awaiting an existence
     *       check), and</li>
     *   <li>standalone names queued by {@code enqueueExternalCalls} for the missing batch.</li>
     * </ul>
     * It replaces the previous up-to-three ALL_OBJECTS queries per level. Found names populate
     * {@code knownObjectTypes} and, for pending-resolve names, {@code resolvedPresentObjects} plus
     * the resolver's owner/type maps via {@link SchemaResolver#registerBatchResolution} — so the
     * per-name {@code tryResolveFromDb} round-trips are gone. Absent pending names are marked
     * missing; names in a failed chunk are re-queued for a later level rather than probed
     * individually. Returns the level metadata for the source cache; empty on non-Oracle.
     */
    private Map<String, String[]> prepareLevelCatalog(Collection<BfsItem> level) {
        Map<String, String[]> meta = new ConcurrentHashMap<>();
        Set<String> levelNames = new LinkedHashSet<>();
        if (level != null) {
            for (BfsItem it : level) {
                String n = it.packageName != null ? it.packageName : it.subprogramName;
                if (n != null && !n.isBlank()) levelNames.add(n.toUpperCase());
            }
        }
        Set<String> pendingResolve = drain(pendingResolveCheck);
        Set<String> pendingMissing = drain(pendingMissingCheck);
        Set<String> names = new LinkedHashSet<>(levelNames);
        names.addAll(pendingResolve);
        names.addAll(pendingMissing);
        names.removeIf(n -> resolvedPresentObjects.contains(n)
                || knownMissingObjects.contains(n)
                || parseCache.containsKey(n));
        if (names.isEmpty()) return meta;

        // A3: when the fingerprint-guarded catalog snapshot is loaded, answer the whole level from
        // memory — no ALL_OBJECTS query. The snapshot is refreshed whenever the catalog fingerprint
        // changed, so a newly added/modified object is present here (and an absent name is genuinely
        // absent). Same found/missing bookkeeping as the DB path below.
        if (downloader.isCatalogSnapshotLoaded()) {
            for (String n : names) {
                String[] m = downloader.snapshotMeta(n);
                if (m != null && m[1] != null) meta.put(n, m);
            }
            for (Map.Entry<String, String[]> e : meta.entrySet()) {
                String name = e.getKey();
                String[] v = e.getValue();
                if (v[1] != null) knownObjectTypes.put(name, v[1]);
                if (pendingResolve.contains(name)) {
                    resolvedPresentObjects.add(name);
                    schemaResolver.registerBatchResolution(name, v[0], v[1]);
                }
            }
            for (String n : names) {
                if (!meta.containsKey(n) && (pendingResolve.contains(n) || pendingMissing.contains(n))) {
                    knownMissingObjects.add(n);
                }
            }
            log.info("[Crawler] Level catalog (snapshot): checked={} found={} pendingResolve={} pendingMissing={}",
                    names.size(), meta.size(), pendingResolve.size(), pendingMissing.size());
            return meta;
        }

        // Non-Oracle dialects have no ALL_OBJECTS; keep the previous "assume present" behaviour
        // (the downloader's own catalog fetch will decide) and issue no query.
        if (downloader.getConnManager() == null || !downloader.getConnManager().isOracle()) {
            resolvedPresentObjects.addAll(pendingResolve);
            resolvedPresentObjects.addAll(pendingMissing);
            return meta;
        }

        List<String> nameList = new ArrayList<>(names);
        List<List<String>> chunks = new ArrayList<>();
        int size = Math.max(1, catalogBatchSize);
        for (int i = 0; i < nameList.size(); i += size) {
            chunks.add(new ArrayList<>(nameList.subList(i, Math.min(i + size, nameList.size()))));
        }
        Set<String> unresolved = ConcurrentHashMap.newKeySet();
        List<Runnable> tasks = new ArrayList<>(chunks.size());
        for (List<String> chunk : chunks) {
            tasks.add(() -> {
                java.sql.Connection conn = null;
                try {
                    conn = downloader.getConnManager().getAnyConnection();
                    String ph = String.join(",", Collections.nCopies(chunk.size(), "?"));
                    String sql = "SELECT owner, object_name, object_type, "
                            + "TO_CHAR(last_ddl_time,'YYYY-MM-DD HH24:MI:SS') ddl FROM all_objects "
                            + "WHERE object_name IN (" + ph + ") "
                            + "AND object_type IN ('PACKAGE','PACKAGE BODY','PROCEDURE','FUNCTION') "
                            + "AND owner IN (" + buildSchemaInClause() + ") "
                            + "ORDER BY CASE object_type WHEN 'PACKAGE BODY' THEN 1 WHEN 'PROCEDURE' THEN 2 WHEN 'FUNCTION' THEN 3 ELSE 4 END";
                    downloader.getConnManager().incrementDbCalls();
                    try (java.sql.PreparedStatement ps =
                                 downloader.getConnManager().prepareStatement(conn, sql)) {
                        for (int j = 0; j < chunk.size(); j++) ps.setString(j + 1, chunk.get(j));
                        try (java.sql.ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                String name = rs.getString("object_name").toUpperCase();
                                String owner = rs.getString("owner");
                                String type = rs.getString("object_type").toUpperCase();
                                String ddl = rs.getString("ddl");
                                meta.putIfAbsent(name, new String[]{owner, type, ddl});
                            }
                        }
                    }
                    downloader.getConnManager().noteQuerySuccess();
                    downloader.getConnManager().returnToPool(conn);
                    conn = null;
                } catch (Exception e) {
                    unresolved.addAll(chunk);
                    downloader.getConnManager().noteQueryFailure();
                    log.debug("[Crawler] level catalog-meta chunk failed: {}", e.getMessage());
                } finally {
                    if (conn != null) downloader.getConnManager().closeAndDiscard(conn);
                }
            });
        }
        fanOut(tasks, catalogParallelism);

        // Prime the type map for EVERY found name (kills the 2-3 blind ALL_SOURCE probes per
        // untyped function in the node-cache peek) and register owner/type from the batch's own
        // rows so no per-name resolver query is needed.
        for (Map.Entry<String, String[]> e : meta.entrySet()) {
            String name = e.getKey();
            String[] v = e.getValue();
            if (v[1] != null) knownObjectTypes.put(name, v[1]);
            if (pendingResolve.contains(name)) {
                resolvedPresentObjects.add(name);
                schemaResolver.registerBatchResolution(name, v[0], v[1]);
            }
        }
        for (String n : names) {
            if (unresolved.contains(n)) {
                // Chunk failed: re-queue for a later level rather than probing individually.
                if (pendingResolve.contains(n)) pendingResolveCheck.add(n);
                if (pendingMissing.contains(n)) pendingMissingCheck.add(n);
            } else if (!meta.containsKey(n)
                    && (pendingResolve.contains(n) || pendingMissing.contains(n))) {
                knownMissingObjects.add(n);
            }
        }
        log.info("[Crawler] Level catalog: checked={} found={} pendingResolve={} pendingMissing={}",
                names.size(), meta.size(), pendingResolve.size(), pendingMissing.size());
        return meta;
    }

    /** Drain a synchronized pending set into a plain snapshot. */
    private static Set<String> drain(Set<String> pending) {
        synchronized (pending) {
            if (pending.isEmpty()) return Collections.emptySet();
            Set<String> copy = new HashSet<>(pending);
            pending.clear();
            return copy;
        }
    }

    /**
     * Fetch every source in one BFS level simultaneously on the bounded source pool. The serial
     * processing loop that follows then finds each source in the downloader's in-memory cache and
     * pays no further network cost — the parse order (and therefore the analysis output) is
     * unchanged. Skips built-in packages, names already known missing, and names already parsed.
     */
    private void prefetchLevelSources(Collection<BfsItem> level) {
        if (level == null || level.isEmpty()) return;
        if (downloader.getConnManager() == null) return;
        boolean sqlServer = downloader.getConnManager().isSqlServer();
        List<Runnable> tasks = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (BfsItem item : level) {
            String name = item.packageName != null ? item.packageName : item.subprogramName;
            if (name == null || name.isBlank()) continue;
            String upper = name.toUpperCase();
            if (isBuiltInPackage(upper) || knownMissingObjects.contains(upper)) continue;
            if (parseCache.containsKey(upper)) continue;
            // Registry fast path: an unchanged, complete object needs no source fetch at all. This
            // keeps the "no fetch" promise even though the level prefetch runs before the serial
            // loop's own registry check. Skipped items do NOT claim the dedupe slot, so a sibling
            // that does need fetching still gets its task.
            if (fastReRun && registry != null && !forceRefresh) {
                String baseKey = buildVisitKey(item.schema, item.packageName, item.subprogramName);
                if (registry.isSkippablePrefix(baseKey, downloader.getCatalogLastDdl(upper))) continue;
            }
            if (!seen.add(upper)) continue;
            final String pkg = item.packageName;
            final String sub = item.subprogramName;
            final String schema = item.schema;
            tasks.add(() -> {
                try {
                    if (pkg != null) {
                        String s = downloader.downloadPackageBody(pkg);
                        if (s == null) downloader.downloadPackageSpec(pkg);
                    } else {
                        // SQL Server resolves the owner from a "schema.name" hint; Oracle matches the
                        // bare name (same rule as downloadAndParse).
                        String fetchName = (sqlServer && schema != null && !schema.isEmpty())
                                ? schema + "." + sub : sub;
                        downloader.downloadAny(fetchName);
                    }
                } catch (Exception ignored) {
                    // a failed prefetch just means the serial pass fetches it (again)
                }
            });
        }
        fanOut(tasks, sourceParallelism);
    }

    /**
     * Batch-check all pending unknown names against ALL_OBJECTS in one query.
     * Names not found as PACKAGE/PROCEDURE/FUNCTION/PACKAGE BODY are added to
     * knownMissingObjects so subsequent BFS nodes skip them without any DB call.
     */
    private void flushMissingBatch() {
        if (pendingMissingCheck.isEmpty()) return;
        Set<String> batch;
        synchronized (pendingMissingCheck) {
            if (pendingMissingCheck.isEmpty()) return;
            batch = new HashSet<>(pendingMissingCheck);
            pendingMissingCheck.clear();
        }
        // Remove any that turned out to have source (already in parseCache)
        batch.removeIf(parseCache::containsKey);
        if (batch.isEmpty()) return;

        // MySQL / SQL Server have no ALL_OBJECTS — treat all pending names as potentially
        // present so the source downloader gets a chance to fetch them from the catalog views
        if (downloader.getConnManager() != null
                && (downloader.getConnManager().isMySQL() || downloader.getConnManager().isSqlServer() || downloader.getConnManager().isDB2() || downloader.getConnManager().isPostgres())) {
            resolvedPresentObjects.addAll(batch);
            return;
        }

        TypeLookup lookup = queryObjectTypes(batch);
        Map<String, String> foundInOracle = lookup.found();
        // Populate type cache for present objects; mark absent ones as missing. A name whose chunk
        // failed is left unresolved (retried on a later level) rather than wrongly marked missing.
        for (String name : batch) {
            if (lookup.unresolved().contains(name)) continue;
            String type = foundInOracle.get(name);
            if (type != null) {
                knownObjectTypes.put(name, type);
            } else {
                knownMissingObjects.add(name);
            }
        }
        log.info("[Crawler] MissingBatch: checked={} existInOracle={} markedMissing={}",
                batch.size(), foundInOracle.size(), batch.size() - foundInOracle.size());
    }

    /**
     * Fix C (now folded into {@link #prepareLevelCatalog}): unknown external-call names are
     * resolved by the ONE per-level ALL_OBJECTS query, which also registers their owner/type via
     * {@link SchemaResolver#registerBatchResolution} — no per-name {@code tryResolveFromDb} calls.
     */

    /**
     * Fix B: After prefetchSources completes, every ParseResult in parseCache is available.
     * Collect ALL table names referenced across every cached parse result and fire ONE
     * batchResolveTableOwners call.  Without this, resolveTableSchemas() called per BFS node
     * fires an incremental query each time a new table name is seen (~40 separate DB calls).
     */
    private void bulkPreResolveAllTables() {
        // Oracle-only optimization (uses ALL_OBJECTS / ALL_TAB_COLUMNS). MySQL and SQL Server
        // resolve tables from parse results directly, so skip the Oracle catalog pre-resolve.
        if (downloader.getConnManager() != null
                && (downloader.getConnManager().isMySQL() || downloader.getConnManager().isSqlServer() || downloader.getConnManager().isDB2() || downloader.getConnManager().isPostgres())) return;
        Set<String> allTableNames = new LinkedHashSet<>();
        for (ParseResult pr : parseCache.values()) {
            if (pr == null) continue;
            for (com.plsql.parser.model.ParsedObject obj : pr.getObjects()) {
                collectTableNamesFromOps(obj.getTableOperations(), allTableNames);
                for (com.plsql.parser.model.SubprogramInfo sub : obj.getSubprograms()) {
                    collectTableNamesFromOps(sub.getTableOperations(), allTableNames);
                }
            }
        }
        if (allTableNames.isEmpty()) return;
        long t0 = System.currentTimeMillis();
        schemaResolver.batchResolveTableOwners(allTableNames);
        log.info("[Crawler] BulkPreResolveAllTables: {} table names resolved in {}ms",
                allTableNames.size(), System.currentTimeMillis() - t0);
    }

    private void collectTableNamesFromOps(
            List<com.plsql.parser.model.TableOperationInfo> ops, Set<String> out) {
        if (ops == null) return;
        for (com.plsql.parser.model.TableOperationInfo op : ops) {
            if (op.getTableName() != null && !op.getTableName().isEmpty()) {
                String n = op.getTableName().toUpperCase();
                if (!n.contains(".")) out.add(n);
                else out.add(n.split("\\.", 2)[1]); // strip schema prefix
            }
        }
    }

    /** Build a comma-separated quoted IN-clause for configured schemas. */
    private String buildSchemaInClause() {
        StringBuilder sb = new StringBuilder();
        List<String> schemas = schemaResolver.getConfiguredSchemas();
        for (int i = 0; i < schemas.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("'").append(schemas.get(i).replace("'", "''")).append("'");
        }
        return sb.length() > 0 ? sb.toString() : "'UNKNOWN'";
    }

    /**
     * Resolve the schema for calls in the list.
     */
    /**
     * Drop "calls" whose unqualified name matches a declared local variable or parameter of
     * the routine. Such references are variable uses — e.g. collection indexing {@code v_ids(i)}
     * or {@code v_ids.COUNT} in a FORALL — not routine calls, so they must not create phantom
     * stub nodes/edges in the flow.
     */
    private List<CallInfo> filterLocalVarCalls(List<CallInfo> calls,
                                               List<VariableInfo> locals, List<ParameterInfo> params) {
        return filterLocalVarCalls(calls, locals, params, null);
    }

    /**
     * The owning package/object's own top-level variables that a bare, unqualified
     * {@code identifier(args)}/{@code identifier.method} reference inside any of its subprograms
     * could actually be resolving to (as opposed to a real call). Mirrors
     * {@code PlSqlAnalysisVisitor.isDeclaredVariableInScope}, which excludes exactly these two
     * lists at parse time for the subprogram it is currently visiting: package-level variables
     * declared in a PACKAGE_BODY/PACKAGE_SPEC ({@link ParsedObject#getPackageVariables()}) and,
     * for standalone routines with no nested subprograms, body-level variables
     * ({@link ParsedObject#getGlobalVariables()}). A whole-package crawl aggregates calls from
     * every subprogram into one node without re-running that parse-time check per subprogram
     * pair, so this list has to be supplied explicitly to {@link #filterLocalVarCalls} here.
     */
    private List<VariableInfo> packageLevelVars(ParsedObject obj) {
        if (obj == null) return null;
        List<VariableInfo> combined = new ArrayList<>();
        if (obj.getPackageVariables() != null) combined.addAll(obj.getPackageVariables());
        if (obj.getGlobalVariables() != null) combined.addAll(obj.getGlobalVariables());
        return combined;
    }

    /**
     * The package-level variable declarations of a package, read from its SPEC. The crawler parses
     * the package BODY (which holds the subprograms but not the globals — the body only assigns
     * them), so the spec is fetched and parsed here, once per package (cached), purely to capture
     * {@code name/dataType/defaultValue/constant}. Returns an empty list when the spec cannot be
     * fetched or parsed; never null.
     */
    private List<VariableInfo> packageSpecVariables(String packageName) {
        if (packageName == null || packageName.isBlank()) return Collections.emptyList();
        String key = packageName.toUpperCase();
        List<VariableInfo> cached = packageSpecVarsCache.get(key);
        if (cached != null) return cached;
        List<VariableInfo> vars = new ArrayList<>();
        try {
            String spec = downloader.downloadPackageSpec(packageName);
            if (spec != null && !spec.isBlank()) {
                ParseResult r = engine.parseContent(spec, packageName + ".pks");
                if (r != null && r.getObjects() != null) {
                    for (ParsedObject o : r.getObjects()) {
                        if (o.getPackageVariables() != null) vars.addAll(o.getPackageVariables());
                    }
                }
            }
        } catch (Exception | StackOverflowError e) {
            log.debug("[Crawler] package spec parse for {} failed: {}", packageName, e.getMessage());
        }
        packageSpecVarsCache.put(key, vars);
        return vars;
    }

    /**
     * Same as {@link #filterLocalVarCalls(List, List, List)} but also excludes names matching
     * the owning package's global/package-level variables (e.g. a package-level collection
     * declared in the package spec/body and indexed from many different subprograms — such a
     * reference is never in any single subprogram's own locals/params, so without this the
     * package-aggregation crawl path leaked it through as a bogus external call).
     */
    private List<CallInfo> filterLocalVarCalls(List<CallInfo> calls,
                                               List<VariableInfo> locals, List<ParameterInfo> params,
                                               List<VariableInfo> globals) {
        if (calls == null || calls.isEmpty()) return calls;
        Set<String> names = new HashSet<>();
        if (locals != null) for (VariableInfo v : locals) if (v.getName() != null) names.add(v.getName().toUpperCase());
        if (params != null) for (ParameterInfo p : params) if (p.getName() != null) names.add(p.getName().toUpperCase());
        if (globals != null) for (VariableInfo v : globals) if (v.getName() != null) names.add(v.getName().toUpperCase());
        if (names.isEmpty()) return calls;
        List<CallInfo> out = new ArrayList<>();
        for (CallInfo c : calls) {
            if (c.getPackageName() == null && c.getName() != null
                    && names.contains(c.getName().toUpperCase())) {
                continue;   // variable reference, not a routine call
            }
            out.add(c);
        }
        return out;
    }

    private List<CallInfo> resolveCallSchemas(List<CallInfo> calls, String ownerSchema) {
        if (calls == null) return new ArrayList<>();
        List<CallInfo> resolved = new ArrayList<>();
        for (CallInfo call : calls) {
            // Create a copy to avoid modifying original
            CallInfo rc = new CallInfo();
            rc.setType(call.getType());
            rc.setPackageName(call.getPackageName());
            rc.setName(call.getName());
            rc.setLine(call.getLine());
            rc.setArguments(call.getArguments());

            // Resolve schema
            if (call.getSchema() != null && !call.getSchema().isEmpty()) {
                rc.setSchema(call.getSchema());
            } else {
                String target = call.getPackageName() != null
                        ? call.getPackageName() : call.getName();
                if (target != null) {
                    String resolvedSchema = schemaResolver.resolveSchema(target, null, null);
                    rc.setSchema(resolvedSchema != null ? resolvedSchema : ownerSchema);
                }
            }
            resolved.add(rc);
        }
        return resolved;
    }

    /**
     * Resolve schemas for table operations.
     */
    private List<TableOperationInfo> resolveTableSchemas(List<TableOperationInfo> ops,
                                                          String ownerSchema) {
        if (ops == null) return new ArrayList<>();

        List<String> unresolvedNames = new ArrayList<>();
        for (TableOperationInfo op : ops) {
            if ((op.getSchema() == null || op.getSchema().isEmpty()) && op.getTableName() != null
                    && !op.getTableName().contains(".")) {
                unresolvedNames.add(op.getTableName());
            }
        }
        if (!unresolvedNames.isEmpty()) {
            schemaResolver.batchResolveTableOwners(unresolvedNames);
        }

        List<TableOperationInfo> resolved = new ArrayList<>();
        for (TableOperationInfo op : ops) {
            TableOperationInfo rt = new TableOperationInfo();
            rt.setTableName(op.getTableName());
            rt.setOperation(op.getOperation());
            rt.setLine(op.getLine());
            rt.setAlias(op.getAlias());
            rt.setJoins(op.getJoins());
            // Copy EVERY field, not a chosen few. This method rebuilds the operation to resolve
            // its schema, and anything not copied here is silently lost — which is exactly what
            // happened to the column detail: the enricher filled it, this dropped it, and
            // tables.json came out with zero column rows while the logs said 100% parsed.
            rt.setSqlText(op.getSqlText());
            rt.setColumnUsages(op.getColumnUsages());
            rt.setDdl(op.getDdl());
            rt.setObjectType(op.getObjectType());
            rt.setTableKind(op.getTableKind());
            rt.setTempScope(op.getTempScope());
            rt.setSubqueryContext(op.getSubqueryContext());
            rt.setDbLink(op.getDbLink());
            rt.setMergeClause(op.getMergeClause());
            rt.setColumnsParsed(op.getColumnsParsed());
            rt.setWhereFilters(op.getWhereFilters());
            rt.setPredicateScope(op.getPredicateScope());
            rt.setReturningColumns(op.getReturningColumns());
            rt.setSubprogramName(op.getSubprogramName());

            // Resolve schema
            String resolvedName = op.getTableName();
            if (op.getSchema() != null && !op.getSchema().isEmpty()) {
                rt.setSchema(op.getSchema());
            } else if (resolvedName != null) {
                if (resolvedName.contains(".")) {
                    String[] parts = resolvedName.split("\\.", 2);
                    String candidate = schemaResolver.resolveSchema(parts[1], "TABLE", parts[0]);
                    if (candidate != null) {
                        rt.setSchema(candidate);
                        rt.setTableName(parts[1]);
                        resolvedName = parts[1];
                    } else {
                        rt.setSchema(parts[0].toUpperCase());
                        rt.setTableName(parts[1]);
                        resolvedName = parts[1];
                    }
                } else {
                    String resolvedSchema = schemaResolver.resolveSchema(resolvedName, "TABLE", null);
                    rt.setSchema(resolvedSchema != null ? resolvedSchema : ownerSchema);
                }
            }

            // Resolve objectType (TABLE, VIEW, MATERIALIZED VIEW)
            if (resolvedName != null) {
                String objType = schemaResolver.resolveObjectType(resolvedName.toUpperCase());
                rt.setObjectType(objType != null ? objType : "TABLE");
            }
            resolved.add(rt);
        }
        return resolved;
    }

    /**
     * Aggregate table operation info into the global map.
     */
    private void aggregateTableInfo(Map<String, SchemaTableInfo> tableInfoMap,
                                     List<TableOperationInfo> tableOps,
                                     String referencedBy) {
        if (tableOps == null) return;
        for (TableOperationInfo op : tableOps) {
            String schema = op.getSchema() != null ? op.getSchema() : "UNKNOWN";
            String tableName = op.getTableName() != null ? op.getTableName().toUpperCase() : "UNKNOWN";
            String key = schema + "." + tableName;

            SchemaTableInfo info = tableInfoMap.computeIfAbsent(key, k -> {
                SchemaTableInfo sti = new SchemaTableInfo();
                sti.setSchema(schema);
                sti.setTableName(tableName);
                // Try to resolve the object type
                String objType = schemaResolver.resolveObjectType(tableName);
                sti.setObjectType(objType != null ? objType : "TABLE");
                return sti;
            });

            // TEMP as the fourth table kind: a parse-time temp classification on any op for this
            // table (from the dialect visitor) is carried onto the aggregate entry. It never
            // overrides a VIEW/MATERIALIZED_VIEW catalog type, but a temp table has no catalog row
            // anyway, so there is nothing to conflict with.
            if (op.getTableKind() != null) {
                info.setTableKind(op.getTableKind());
                if (op.getTempScope() != null) info.setTempScope(op.getTempScope());
            }

            if (op.getOperation() != null) {
                info.getOperations().add(op.getOperation().toUpperCase());
            }
            if (op.getLine() > 0) {
                info.getLines().add(op.getLine());
            }
            if (referencedBy != null) {
                info.getReferencedBy().add(referencedBy);
            }
            foldColumnUsage(info, op, referencedBy);
            if (op.getDdl() != null && !op.getDdl().isEmpty()) {
                info.getDdl().addAll(op.getDdl());
            }
        }
    }

    /** Line numbers kept per column. A column touched everywhere does not need every line. */
    private static final int MAX_COLUMN_LINES = 25;

    /**
     * Folds a statement's column detail into the table's per-column summary.
     *
     * <p>One row per column rather than per statement: a table with hundreds of statements against
     * it would otherwise carry hundreds of near-identical rows onto its page — the same duplication
     * that had to be pooled out of the JAR analysis document, and unreadable as well as large.
     */
    private void foldColumnUsage(SchemaTableInfo info, TableOperationInfo op, String referencedBy) {
        if (op.getColumnUsages() == null || op.getColumnUsages().isEmpty()) return;
        for (com.plsqlanalyzer.parser.model.ColumnUsage u : op.getColumnUsages()) {
            String col = u.getColumnName();
            // A computed select item (`SUM(x)`) has no column name; it belongs to the statement,
            // not to a column of this table.
            if (col == null || col.isBlank() || "*".equals(col)) continue;
            String key = col.toUpperCase();
            TableColumnUsage tc = null;
            for (TableColumnUsage existing : info.getColumns()) {
                if (key.equalsIgnoreCase(existing.getColumnName())) { tc = existing; break; }
            }
            if (tc == null) {
                tc = new TableColumnUsage(key);
                info.getColumns().add(tc);
            }
            if (u.getUsage() != null) tc.getUsages().add(u.getUsage());
            if (u.getValueSource() != null) tc.getValueSources().add(u.getValueSource());
            if (u.getIntoVariable() != null) tc.getIntoVariables().add(u.getIntoVariable());
            if (referencedBy != null) tc.getReferencedBy().add(referencedBy);
            // addLine keeps the true distinct count even past the cap, so the page can say
            // "showing 25 of 112" instead of presenting a sample as if it were the whole set.
            tc.addLine(u.getLineNumber(), MAX_COLUMN_LINES);
            if (tc.getSampleExpression() == null && u.getExpression() != null
                    && !u.getExpression().equalsIgnoreCase(col)) {
                tc.setSampleExpression(u.getExpression());
            }
        }
    }

    /**
     * Enqueue external calls for BFS processing.
     * Uses ALL_DEPENDENCIES to validate that the call is a real dependency,
     * filtering out false positives like record field accesses and type references.
     */
    private void enqueueExternalCalls(List<CallInfo> calls, int currentDepth,
                                       String callerPackage, String callerObject,
                                       String callerSchema,
                                       Queue<BfsItem> queue, List<FlowEdge> callGraph) {
        enqueueExternalCalls(calls, currentDepth, callerPackage, callerObject, callerSchema,
                queue, callGraph, null);
    }

    private void enqueueExternalCalls(List<CallInfo> calls, int currentDepth,
                                       String callerPackage, String callerObject,
                                       String callerSchema,
                                       Queue<BfsItem> queue, List<FlowEdge> callGraph,
                                       String callerOverloadDisc) {
        if (calls == null) return;

        String sourceObject = callerPackage != null ? callerPackage : callerObject;

        for (CallInfo call : calls) {
            String callType = call.getType();
            boolean isInternal = "INTERNAL".equalsIgnoreCase(callType);
            boolean isExternal = "EXTERNAL".equalsIgnoreCase(callType);

            if (!isExternal && !isInternal) {
                continue;
            }

            String targetPackage = call.getPackageName();
            String targetObject = call.getName();

            if (targetObject == null || targetObject.isEmpty()) {
                continue;
            }

            // For internal calls, the target is within the same package
            if (isInternal) {
                if (callerPackage == null) continue;
                targetPackage = callerPackage;
            }

            // Reclassify: EXTERNAL with no package may be internal to caller's package
            if (isExternal && targetPackage == null && callerPackage != null) {
                String rawSource = downloader.getCachedSource(callerPackage);
                if (rawSource != null && hasSubprogram(rawSource, targetObject)) {
                    targetPackage = callerPackage;
                    isExternal = false;
                    isInternal = true;
                }
            }

            // Cross-schema standalone call: a "OWNER.OBJECT" reference where the first identifier
            // is a real schema/owner rather than a package. Oracle qualifies calls to standalone
            // procedures/functions in another schema as OWNER.OBJECT (e.g.
            // ANALYZER_REF.USP_REF_LOG). Without this, OWNER is treated as a package name, found
            // absent from ALL_OBJECTS, and the whole call is dropped. When OWNER matches a
            // configured schema, reinterpret it as the target schema of a standalone object so the
            // object is crawled and appears as a reachable node in that schema.
            // Cross-schema standalone call detection. The "package" segment may actually be a
            // SCHEMA/owner qualifying a standalone procedure/function in another schema. This
            // applies both when the segment already matches a configured schema and when it is an
            // unknown name that turns out to be a foreign owner the login user can read (probed via
            // ALL_OBJECTS). Only probe unknown segments — a known package/object short-circuits so
            // normal same-schema package calls are never misclassified or charged an extra query.
            String crossSchemaOwner = null;
            if (isExternal && targetPackage != null && targetObject != null
                    && !targetPackage.equalsIgnoreCase(callerPackage)
                    && !isBuiltInPackage(targetPackage)) {
                String pkgUpper = targetPackage.toUpperCase();
                boolean isConfiguredSchema = schemaResolver.getConfiguredSchemas() != null
                        && schemaResolver.getConfiguredSchemas().contains(pkgUpper);
                boolean pkgIsKnownRoutineContainer = schemaResolver.isKnownObject(pkgUpper)
                        || downloader.getCachedSource(pkgUpper) != null
                        || knownObjectTypes.containsKey(pkgUpper);
                boolean crossSchema = isConfiguredSchema;
                if (!crossSchema && !pkgIsKnownRoutineContainer
                        && schemaResolver.isForeignOwnerObject(pkgUpper, targetObject)) {
                    crossSchema = true;
                }
                if (crossSchema) {
                    crossSchemaOwner = pkgUpper;
                    schemaResolver.registerDiscoveredSchema(pkgUpper);
                    targetPackage = null;
                }
            }

            if (isBuiltInPackage(targetPackage) || isBuiltInFunction(targetObject)
                    || isSchedulerPackage(targetPackage)) {
                continue;
            }

            // Skip compound pkg.sub keys already confirmed as non-subprogram.
            if (targetPackage != null && targetObject != null) {
                String compoundKey = (targetPackage + "." + targetObject).toUpperCase();
                if (knownMissingObjects.contains(compoundKey)) continue;
            }

            // If we already have the package source in memory (prefetched or previously parsed),
            // verify the named subprogram actually exists before paying any DB cost.
            // Package-level constants/variables (e.g. CBM.CONTRACT_NO) are parsed by ANTLR
            // as zero-arg call statements — hasSubprogram catches them instantly from cache.
            if (targetPackage != null && targetObject != null && call.getArguments() == 0) {
                String cachedSrc = downloader.getCachedSource(targetPackage);
                if (cachedSrc != null && !hasSubprogram(cachedSrc, targetObject)) {
                    String compoundKey = (targetPackage + "." + targetObject).toUpperCase();
                    knownMissingObjects.add(compoundKey);
                    log.debug("[Crawler] Skipping non-subprogram reference: {}.{}", targetPackage, targetObject);
                    continue;
                }
            }

            // For external calls, validate against schema resolver.
            // Only filter if resolver positively has dep data for the source.
            if (isExternal) {
                String targetToCheck = targetPackage != null ? targetPackage : targetObject;
                if (schemaResolver.hasDependencyData(sourceObject)
                        && !schemaResolver.isDependency(sourceObject, targetToCheck)
                        && !schemaResolver.isKnownObject(targetToCheck)) {
                    String upperTarget = targetToCheck.toUpperCase();
                    // Fix C: batch all unknown names; resolvedPresentObjects is populated by
                    // flushPendingResolve() which fires ONE ALL_OBJECTS IN(...) between BFS levels.
                    if (knownMissingObjects.contains(upperTarget)) continue;
                    if (resolvedPresentObjects.contains(upperTarget)) {
                        // Already confirmed present in a prior batch — allow through
                    } else {
                        // Queue for next batch resolve. Enqueue the BFS item optimistically;
                        // if the name turns out absent, downloadAndParse fast-skips via
                        // knownMissingObjects after the batch fires before the node is processed.
                        pendingResolveCheck.add(upperTarget);
                        // Fall through — still enqueue so no calls are lost
                    }
                }
            }

            // Schema-aware key for standalone targets so same-named routines in different
            // schemas resolve to distinct nodes (no package → schema disambiguates).
            // For a cross-schema standalone call the owner IS the target schema (the call's own
            // schema field carries the caller's schema as a fallback, so it must not win here).
            String targetSchema = crossSchemaOwner != null ? crossSchemaOwner
                    : ((targetPackage == null && call.getSchema() != null && !call.getSchema().isEmpty())
                        ? call.getSchema() : null);
            // Overloaded target: the edge must carry the SAME discriminator as the callee node so
            // it resolves to the correct overload (else edge → missing node). Signature-based when
            // the call's argument types are known, else the arity fallback — both computed exactly
            // as the node side computes them.
            String targetDisc;
            String targetOverloadId = null;
            if (targetPackage != null) {
                targetDisc = resolveOverload(targetPackage, targetObject,
                        call.getArguments(), call.getArgTypes());
            } else {
                StandaloneOverload so = resolveStandaloneOverload(targetObject, targetSchema,
                        call.getArguments(), call.getArgTypes());
                targetDisc = so != null ? so.disc : null;
                if (so != null) targetOverloadId = so.oid != null ? so.oid : so.specificName;
            }
            boolean targetOverloaded = targetDisc != null;

            // Task #28: a DB2 module member called by bare name must produce an edge whose
            // toNodeId matches the member's MODULE.SUB node id, so resolve the owning module as the
            // package slot here. The BFS item below keeps packageName=null (the module is
            // re-derived at poll time), so only the edge key/node id use the effective package.
            String edgeTargetPackage = targetPackage;
            if (edgeTargetPackage == null && targetObject != null) {
                String mod = downloader.db2ModuleOf(targetObject, targetSchema);
                if (mod != null && !mod.isEmpty()) edgeTargetPackage = mod;
            }

            String visitKey = buildVisitKey(targetSchema, edgeTargetPackage, targetObject);
            if (targetOverloaded) visitKey = visitKey + "#" + targetDisc;
            if (visited.contains(visitKey)) {
                addCallEdge(callGraph, callerSchema, callerPackage, callerObject,
                        targetSchema != null ? targetSchema
                                : (call.getSchema() != null ? call.getSchema() : callerSchema),
                        edgeTargetPackage, targetObject,
                        call.getLine(), currentDepth, targetDisc, callerOverloadDisc);
                continue;
            }

            addCallEdge(callGraph, callerSchema, callerPackage, callerObject,
                    targetSchema != null ? targetSchema
                            : (call.getSchema() != null ? call.getSchema() : callerSchema),
                    edgeTargetPackage, targetObject,
                    call.getLine(), currentDepth, targetDisc, callerOverloadDisc);

            // Pre-queue names for bulk ALL_OBJECTS existence check before download:
            //
            // Case A — standalone (no package): e.g. C_ERROR_TEXT
            //   Add the name itself. If absent from Oracle → skip at nodeCache peek.
            //
            // Case B — compound pkg.sub (e.g. CRP.CONTRACT_NO) where CRP source is NOT
            //   yet in memory: add the PACKAGE NAME "CRP". The bulk check confirms whether
            //   CRP exists in Oracle at all. If absent → knownMissingObjects("CRP") → both
            //   nodeCache peek and downloadAndParse skip instantly, zero DB calls.
            //   If CRP exists → download proceeds; hasSubprogram then eliminates CONTRACT_NO.
            String upperPkg = targetPackage != null ? targetPackage.toUpperCase() : null;
            String upperObj = targetObject != null ? targetObject.toUpperCase() : null;
            if (upperPkg == null && upperObj != null) {
                // Case A: standalone
                if (!knownMissingObjects.contains(upperObj) && !resolvedPresentObjects.contains(upperObj)) {
                    pendingMissingCheck.add(upperObj);
                }
            } else if (upperPkg != null) {
                // Case B: compound — pre-check the package name if source not yet cached
                if (!knownMissingObjects.contains(upperPkg)
                        && !knownObjectTypes.containsKey(upperPkg)
                        && downloader.getCachedSource(upperPkg) == null) {
                    pendingMissingCheck.add(upperPkg);
                }
            }

            // Carry the ACTUAL call-site arg count AND argument type hints on the work item. Package
            // members re-derive the discriminator at poll time from those inputs (the callee package
            // is parsed by then); STANDALONE Postgres/DB2 overloads carry the resolved discriminator
            // and per-overload id, since they are resolved against the routine catalog (not a parse).
            queue.add(new BfsItem(targetPackage, targetObject, targetSchema, currentDepth + 1,
                    callerPackage, callerObject, call.getLine(), call.getArguments(), call.getArgTypes(),
                    targetDisc, targetOverloadId));
        }
    }

    /**
     * Add a call edge to the call graph.
     */
    private void addCallEdge(List<FlowEdge> callGraph,
                              String fromSchema, String fromPackage, String fromObject,
                              String toSchema, String toPackage, String toObject,
                              int line, int depth) {
        addCallEdge(callGraph, fromSchema, fromPackage, fromObject,
                toSchema, toPackage, toObject, line, depth, null, null);
    }

    private void addCallEdge(List<FlowEdge> callGraph,
                              String fromSchema, String fromPackage, String fromObject,
                              String toSchema, String toPackage, String toObject,
                              int line, int depth, String toOverloadDisc) {
        addCallEdge(callGraph, fromSchema, fromPackage, fromObject,
                toSchema, toPackage, toObject, line, depth, toOverloadDisc, null);
    }

    /**
     * MID-026 call-graph phantom-root fix (confirmed on the real
     * ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE corpus): an overloaded node (e.g.
     * {@code COMPUTE_TOTAL}, crawled as distinct {@code __1}/{@code __2} arity variants) that is
     * itself the SOURCE of an outgoing call previously had its {@code fromNodeId} built with no
     * discriminator suffix at all — {@code buildNodeId(fromSchema, fromPkg, fromObject)} alone —
     * which matches no real node (only the {@code __N} variants exist). That phantom id had no key
     * in {@code node_id_map.json} either, so it was unresolvable downstream, and any "never appears
     * as a to" root-detection heuristic (client-side call-graph rendering) misclassified it as a
     * second graph root. Fixed by mirroring the {@code toOverloadDisc} handling on the FROM side
     * via this {@code fromOverloadDisc} parameter, fed by the current BFS item's own already-
     * computed overload discriminator (the node being processed IS the "from" side for every edge
     * emitted while processing it). W1: the discriminator is now a SIGNATURE (types when known,
     * else the arity count) — the string form keeps both cases on one code path.
     */
    private void addCallEdge(List<FlowEdge> callGraph,
                              String fromSchema, String fromPackage, String fromObject,
                              String toSchema, String toPackage, String toObject,
                              int line, int depth, String toOverloadDisc, String fromOverloadDisc) {
        FlowEdge edge = new FlowEdge();
        edge.setFromSchema(fromSchema);
        edge.setFromPackage(fromPackage);
        edge.setFromObject(fromObject);
        edge.setToSchema(toSchema);
        edge.setToPackage(toPackage);
        edge.setToObject(toObject);
        edge.setLine(line);
        edge.setDepth(depth);

        // Build nodeIds — for FROM, also check visited set to resolve package
        String fromPkg = fromPackage;
        if (fromPkg == null && fromObject != null) {
            String visitKey = fromObject.toUpperCase();
            for (String v : visited) {
                if (v.endsWith("." + visitKey)) {
                    fromPkg = v.substring(0, v.length() - visitKey.length() - 1);
                    break;
                }
            }
        }
        String fromNodeId = buildNodeId(fromSchema, fromPkg, fromObject);
        if (fromOverloadDisc != null && !fromOverloadDisc.isEmpty()) {
            fromNodeId = fromNodeId + "__" + fromOverloadDisc;
        }
        edge.setFromNodeId(fromNodeId);
        // Overloaded target: append the SIGNATURE discriminator so this edge points at the
        // matching overload node (buildShortId appends the same "__<signature>").
        String toNodeId = buildNodeId(toSchema, toPackage, toObject);
        if (toOverloadDisc != null && !toOverloadDisc.isEmpty()) {
            toNodeId = toNodeId + "__" + toOverloadDisc;
        }
        edge.setToNodeId(toNodeId);
        callGraph.add(edge);
    }

    private String buildNodeId(String schema, String pkg, String object) {
        StringBuilder sb = new StringBuilder();
        if (schema != null && !schema.isEmpty()) sb.append(schema);
        if (pkg != null && !pkg.isEmpty()) sb.append("$").append(pkg);
        if (object != null && !object.isEmpty()) sb.append("$").append(object);
        return sb.toString().toUpperCase().replaceAll("[^A-Z0-9_$]", "_");
    }

    /**
     * Resolve the schema for a given object, using the SchemaResolver
     * and falling back to the source downloader's owner cache.
     */
    private String resolveObjectSchema(String objectName, String parsedSchema) {
        // If the parser already found a schema, use it
        if (parsedSchema != null && !parsedSchema.isEmpty()) {
            return parsedSchema.toUpperCase();
        }

        // Try schema resolver
        String resolved = schemaResolver.resolveSchema(objectName);
        if (resolved != null) {
            return resolved;
        }

        // Try the source downloader's owner cache
        String owner = downloader.getOwner(objectName);
        if (owner != null) {
            return owner.toUpperCase();
        }

        return null;
    }

    /**
     * Build a visit key for the visited set.
     */
    private String buildVisitKey(String packageName, String subprogramName) {
        return buildVisitKey(null, packageName, subprogramName);
    }

    /**
     * Count subprograms in the ALREADY-PARSED package that share this name. A result of
     * &gt;1 means the member is genuinely overloaded (same name, different signatures), which
     * gates all overload-splitting logic so non-overloaded members stay byte-identical.
     * A routine with default parameters is a SINGLE subprogram → count 1 → never split.
     */
    /**
     * Parse source with the active dialect's parser (no DB access, no disk cache side effects).
     * Used to resolve overload counts on demand when a package is referenced before the BFS has
     * parsed it (see overloadCount) — avoids a bare, arity-less duplicate node for an overloaded
     * member that is first reached at a shallow depth.
     */
    private ParseResult parseDialect(String source, String objectName) {
        DbConnectionManager cm = downloader.getConnManager();
        if (cm != null && cm.isMySQL())      return new MySqlProcedureParser().parse(source, objectName, cm.getMySqlSchema());
        if (cm != null && cm.isSqlServer())  return new SqlServerProcedureParser().parse(source, objectName, cm.getSqlServerSchema());
        if (cm != null && cm.isDB2())        return new Db2ProcedureParser().parse(source, objectName, cm.getDb2Schema());
        if (cm != null && cm.isPostgres())   return new PostgresProcedureParser().parse(source, objectName, cm.getPostgresSchema());
        return engine.parseContent(source, objectName + ".sql");
    }

    private int overloadCount(String packageName, String subName) {
        return overloadsOf(packageName, subName).size();
    }

    /**
     * All subprograms of the given name in the ALREADY-PARSED package (parsing on demand from the
     * prefetched source when the member is reached before its package's own BFS node, so overload
     * detection is order-independent). Shared by {@link #overloadCount} and the W1 signature
     * resolver so both see the identical overload set.
     */
    private List<SubprogramInfo> overloadsOf(String packageName, String subName) {
        if (packageName == null || subName == null) return Collections.emptyList();
        ParseResult pr = parseCache.get(packageName.toUpperCase());
        if (pr == null) {
            // Not parsed yet (member reached before its package's BFS node). Parse the already
            // prefetched source on demand so overload detection is order-independent. The result
            // is cached, so the eventual package node reuses it (no extra parse cost).
            String src = downloader.getCachedSource(packageName);
            if (src != null && !src.isBlank()) {
                try {
                    pr = parseDialect(src, packageName);
                    if (pr != null) parseCache.put(packageName.toUpperCase(), pr);
                } catch (Exception e) {
                    return Collections.emptyList();
                }
            }
        }
        if (pr == null || pr.getObjects() == null) return Collections.emptyList();
        List<SubprogramInfo> out = new ArrayList<>();
        for (ParsedObject o : pr.getObjects()) {
            if (o.getSubprograms() == null) continue;
            for (SubprogramInfo s : o.getSubprograms()) {
                if (s.getName() != null && s.getName().equalsIgnoreCase(subName)) out.add(s);
            }
        }
        return out;
    }

    /**
     * W1 overload discriminator for a package member (null when not genuinely overloaded). Uses the
     * call's argument types for an exact-signature match when the grammar supplied them, else
     * arity, else first match — the last two counted into {@link #OVERLOAD_ARITY_FALLBACKS}/
     * {@link #OVERLOAD_FIRST_MATCH_FALLBACKS}.
     */
    private String resolveOverload(String packageName, String subName, int argCount, List<String> argTypes) {
        return FlowNode.resolveOverloadDiscriminator(overloadsOf(packageName, subName), argCount, argTypes,
                OVERLOAD_ARITY_FALLBACKS, OVERLOAD_FIRST_MATCH_FALLBACKS);
    }

    /**
     * Resolved per-overload identity for a STANDALONE Postgres/DB2 routine (W1). {@code disc} is the
     * node-id discriminator (Postgres identity-arguments signature / DB2 SPECIFICNAME); {@code oid}
     * / {@code specificName} address that exact overload's source for a per-overload fetch.
     */
    private static class StandaloneOverload {
        final String disc;
        final String oid;
        final String specificName;
        StandaloneOverload(String disc, String oid, String specificName) {
            this.disc = disc;
            this.oid = oid;
            this.specificName = specificName;
        }
    }

    /**
     * Resolve a STANDALONE (no package) Postgres/DB2 routine's overload identity from the cached
     * routine catalog. Returns null when the name has 0/1 catalog entries (not overloaded) or the
     * dialect has no overloading. Postgres keys by identity-arguments signature (exact type match
     * when the call supplied types, else arity, else first match); DB2 keys by SPECIFICNAME
     * (first match, counted — the catalog carries no call-matchable arity).
     */
    private StandaloneOverload resolveStandaloneOverload(String name, String schema,
            int argCount, List<String> argTypes) {
        DbConnectionManager cm = downloader.getConnManager();
        if (cm == null || name == null) return null;
        if (cm.isPostgres()) {
            List<SourceFetchStrategy.ObjectRef> mine = new ArrayList<>();
            for (SourceFetchStrategy.ObjectRef r : downloader.listPostgresRoutines()) {
                if (r.name != null && r.name.equalsIgnoreCase(name)
                        && (schema == null || schema.isEmpty()
                            || (r.schema != null && r.schema.equalsIgnoreCase(schema)))) {
                    mine.add(r);
                }
            }
            if (mine.size() <= 1) return null;
            String want = FlowNode.allTypesKnown(argTypes)
                    ? FlowNode.signatureOfTypes(argTypes) : null;
            SourceFetchStrategy.ObjectRef pick = null;
            if (want != null) {
                for (SourceFetchStrategy.ObjectRef r : mine) {
                    if (want.equals(FlowNode.sanitizeType(r.identityArgs))) { pick = r; break; }
                }
            }
            if (pick == null) {
                if (argCount < 0) return null;
                OVERLOAD_ARITY_FALLBACKS.incrementAndGet();
                List<SourceFetchStrategy.ObjectRef> byArity = new ArrayList<>();
                for (SourceFetchStrategy.ObjectRef r : mine) {
                    if (identityArgCount(r.identityArgs) == argCount) byArity.add(r);
                }
                if (byArity.size() == 1) {
                    pick = byArity.get(0);
                } else {
                    OVERLOAD_FIRST_MATCH_FALLBACKS.incrementAndGet();
                    pick = byArity.isEmpty() ? mine.get(0) : byArity.get(0);
                }
            }
            String disc = FlowNode.sanitizeType(pick.identityArgs);
            if (disc == null) disc = "NONE";
            return new StandaloneOverload(disc, pick.oid, null);
        }
        if (cm.isDB2()) {
            List<SourceFetchStrategy.ObjectRef> mine = new ArrayList<>();
            for (SourceFetchStrategy.ObjectRef r : downloader.listDb2Routines()) {
                if (r.name != null && r.name.equalsIgnoreCase(name)
                        && (schema == null || schema.isEmpty()
                            || (r.schema != null && r.schema.equalsIgnoreCase(schema)))) {
                    mine.add(r);
                }
            }
            if (mine.size() <= 1) return null;
            OVERLOAD_FIRST_MATCH_FALLBACKS.incrementAndGet();
            SourceFetchStrategy.ObjectRef pick = mine.get(0);
            String disc = FlowNode.sanitizeType(pick.specificName);
            if (disc == null) disc = "NONE";
            return new StandaloneOverload(disc, null, pick.specificName);
        }
        return null;
    }

    /** Number of top-level (outside parens) comma-separated arguments in a Postgres identity-args list. */
    private static int identityArgCount(String identityArgs) {
        if (identityArgs == null || identityArgs.isBlank()) return 0;
        int depth = 0, count = 1;
        for (int i = 0; i < identityArgs.length(); i++) {
            char c = identityArgs.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) count++;
        }
        return count;
    }

    /**
     * Build the BFS de-dup key. Package members keep the historical PACKAGE.SUB key
     * (unchanged — Oracle's dominant case). For STANDALONE objects (no package: all
     * SQL Server, Oracle standalone routines) the schema is included so same-named
     * objects in different schemas (e.g. sales.fn_X vs hr.fn_X) are NOT treated as the
     * same node and never overwrite each other.
     */
    private String buildVisitKey(String schema, String packageName, String subprogramName) {
        if (packageName != null && !packageName.isEmpty()) {
            return packageName.toUpperCase() + "." + subprogramName.toUpperCase();
        }
        // Standalone: use "$" (the node-id separator), NOT "." — a "." prefix would be
        // mistaken for a package by addCallEdge's fromPkg heuristic (which scans for ".OBJ").
        String prefix = (schema != null && !schema.isEmpty()) ? schema.toUpperCase() + "$" : "";
        return prefix + subprogramName.toUpperCase();
    }

    /**
     * Task #28: the effective PACKAGE slot for a BFS item, feeding {@link #buildVisitKey} and
     * {@link FlowNode#setPackageName}. Oracle package members carry {@code item.packageName}; a DB2
     * module member has {@code item.packageName == null} but its owning module is known from the
     * catalog ({@code SYSCAT.ROUTINES.ROUTINEMODULENAME}, via the parsed object when available, else
     * the cached routine catalog), so the member keys as MODULE.SUB. Returns null — leaving the
     * historical standalone key/id — for every other object.
     */
    private String effectivePackageName(BfsItem item, ParsedObject targetObj) {
        if (item.packageName != null && !item.packageName.isEmpty()) return item.packageName;
        if (targetObj != null && targetObj.getPackageName() != null
                && !targetObj.getPackageName().isEmpty()) {
            return targetObj.getPackageName();
        }
        if (item.subprogramName != null) {
            String module = downloader.db2ModuleOf(item.subprogramName, item.schema);
            if (module != null && !module.isEmpty()) return module;
        }
        return null;
    }

    /**
     * Check if an object type is non-parseable (TYPE, SYNONYM, SEQUENCE, etc.)
     */
    private boolean isNonParsableType(String objectType) {
        if (objectType == null) return false;
        String upper = objectType.toUpperCase().replace(" ", "_");
        return upper.equals("TYPE") || upper.equals("TYPE_BODY")
                || upper.equals("SYNONYM") || upper.equals("SEQUENCE")
                || upper.equals("TABLE") || upper.equals("VIEW")
                || upper.equals("MATERIALIZED_VIEW") || upper.equals("INDEX")
                || upper.equals("LIBRARY") || upper.equals("JAVA_CLASS");
    }

    private boolean hasSubprogram(String source, String name) {
        if (source == null || name == null) return false;
        String pat = "(?i)(?:PROCEDURE|FUNCTION)\\s+" + Pattern.quote(name) + "\\b";
        return Pattern.compile(pat).matcher(source).find();
    }

    /**
     * Extract a single subprogram source from a package body using regex.
     * Matches PROCEDURE/FUNCTION name ... END name; patterns.
     */
    private String extractSubprogramSource(String source, String subName) {
        if (source == null || subName == null) return null;
        // Try with END subName; first
        String pat = "(?is)((?:PROCEDURE|FUNCTION)\\s+" + Pattern.quote(subName)
                + "\\b.*?END\\s+" + Pattern.quote(subName) + "\\s*;)";
        Matcher m = Pattern.compile(pat).matcher(source);
        if (m.find()) {
            return m.group(1);
        }

        // Fallback: find start of subprogram, then find the matching END;
        // by counting BEGIN/END nesting
        String startPat = "(?i)(PROCEDURE|FUNCTION)\\s+" + Pattern.quote(subName) + "\\b";
        Matcher startM = Pattern.compile(startPat).matcher(source);
        if (startM.find()) {
            int startIdx = startM.start();
            // Find the first BEGIN after the declaration
            String afterDecl = source.substring(startIdx);
            int nestLevel = 0;
            boolean foundFirstBegin = false;
            int pos = 0;
            // Tokenize to find balanced BEGIN/END
            Pattern tokenPat = Pattern.compile("(?i)\\b(BEGIN|END)\\b");
            Matcher tokenM = tokenPat.matcher(afterDecl);
            while (tokenM.find()) {
                String token = tokenM.group(1).toUpperCase();
                if ("BEGIN".equals(token)) {
                    nestLevel++;
                    foundFirstBegin = true;
                } else if ("END".equals(token) && foundFirstBegin) {
                    nestLevel--;
                    if (nestLevel == 0) {
                        // Find the semicolon after END
                        int endPos = tokenM.end();
                        int semiIdx = afterDecl.indexOf(';', endPos);
                        if (semiIdx >= 0) {
                            return afterDecl.substring(0, semiIdx + 1);
                        }
                        return afterDecl.substring(0, endPos);
                    }
                }
            }
        }
        return null;
    }

    /**
     * Check if a package name is a known Oracle built-in.
     */
    private boolean isBuiltInPackage(String packageName) {
        if (packageName == null) return false;
        // DBMS_SCHEDULER / DBMS_JOB are the scheduler entry points, not built-in routines to be
        // suppressed — they are excluded from the builtin classification so a CREATE_JOB/SUBMIT
        // call is no longer hidden. (Their jobs are discovered as first-class JOB nodes; the
        // scheduler packages themselves have no PL/SQL source to crawl, so the BFS skip for them
        // lives in enqueueExternalCalls, not here.)
        if (isSchedulerPackage(packageName)) return false;
        String upper = packageName.toUpperCase();
        return upper.startsWith("DBMS_")
                || upper.startsWith("UTL_")
                || upper.startsWith("SYS.")
                || upper.equals("STANDARD")
                || upper.equals("DBMS_OUTPUT")
                || upper.equals("DBMS_LOB")
                || upper.equals("DBMS_SQL")
                || upper.equals("DBMS_UTILITY")
                || upper.equals("DBMS_SESSION")
                || upper.equals("DBMS_LOCK")
                || upper.equals("DBMS_METADATA")
                || upper.equals("DBMS_XMLDOM")
                || upper.equals("DBMS_XMLPARSER")
                || upper.equals("DBMS_XMLGEN")
                || upper.equals("DBMS_CRYPTO")
                || upper.equals("DBMS_RANDOM")
                || upper.equals("DBMS_APPLICATION_INFO")
                || upper.equals("APEX_JSON")
                || upper.equals("APEX_WEB_SERVICE")
                || upper.equals("HTF")
                || upper.equals("HTP")
                || upper.equals("OWA_UTIL");
    }

    /** True for the Oracle scheduler pseudo-packages (DBMS_SCHEDULER / legacy DBMS_JOB). */
    private static boolean isSchedulerPackage(String packageName) {
        if (packageName == null) return false;
        String u = packageName.toUpperCase();
        return u.equals("DBMS_SCHEDULER") || u.equals("DBMS_JOB");
    }

    /**
     * Check if a function name is a known Oracle built-in function.
     */
    private boolean isBuiltInFunction(String funcName) {
        if (funcName == null) return false;
        String upper = funcName.toUpperCase();
        return upper.equals("NVL")
                || upper.equals("NVL2")
                || upper.equals("DECODE")
                || upper.equals("TO_CHAR")
                || upper.equals("TO_DATE")
                || upper.equals("TO_NUMBER")
                || upper.equals("TO_TIMESTAMP")
                || upper.equals("SUBSTR")
                || upper.equals("INSTR")
                || upper.equals("LENGTH")
                || upper.equals("REPLACE")
                || upper.equals("TRIM")
                || upper.equals("LTRIM")
                || upper.equals("RTRIM")
                || upper.equals("UPPER")
                || upper.equals("LOWER")
                || upper.equals("INITCAP")
                || upper.equals("LPAD")
                || upper.equals("RPAD")
                || upper.equals("ROUND")
                || upper.equals("TRUNC")
                || upper.equals("MOD")
                || upper.equals("ABS")
                || upper.equals("CEIL")
                || upper.equals("FLOOR")
                || upper.equals("POWER")
                || upper.equals("SQRT")
                || upper.equals("SYSDATE")
                || upper.equals("SYSTIMESTAMP")
                || upper.equals("CURRENT_DATE")
                || upper.equals("CURRENT_TIMESTAMP")
                || upper.equals("ADD_MONTHS")
                || upper.equals("MONTHS_BETWEEN")
                || upper.equals("LAST_DAY")
                || upper.equals("NEXT_DAY")
                || upper.equals("EXTRACT")
                || upper.equals("COALESCE")
                || upper.equals("NULLIF")
                || upper.equals("GREATEST")
                || upper.equals("LEAST")
                || upper.equals("CASE")
                || upper.equals("CAST")
                || upper.equals("COUNT")
                || upper.equals("SUM")
                || upper.equals("AVG")
                || upper.equals("MIN")
                || upper.equals("MAX")
                || upper.equals("LISTAGG")
                || upper.equals("ROW_NUMBER")
                || upper.equals("RANK")
                || upper.equals("DENSE_RANK")
                || upper.equals("ROWNUM")
                || upper.equals("ROWID")
                || upper.equals("RAISE_APPLICATION_ERROR")
                || upper.equals("SQL%ROWCOUNT")
                || upper.equals("SQL%FOUND")
                || upper.equals("SQL%NOTFOUND")
                || upper.equals("SQLCODE")
                || upper.equals("SQLERRM")
                || upper.equals("USER")
                || upper.equals("SYS_CONTEXT")
                || upper.equals("REGEXP_LIKE")
                || upper.equals("REGEXP_REPLACE")
                || upper.equals("REGEXP_SUBSTR")
                || upper.equals("REGEXP_INSTR")
                || upper.equals("XMLTYPE")
                || upper.equals("XMLELEMENT")
                || upper.equals("XMLFOREST")
                || upper.equals("XMLAGG")
                || upper.equals("XMLQUERY")
                || upper.equals("EXISTSNODE")
                || upper.equals("CONCAT")
                || upper.equals("TO_CLOB")
                || upper.equals("TO_BLOB")
                || upper.equals("TO_BINARY_FLOAT")
                || upper.equals("TO_BINARY_DOUBLE")
                || upper.equals("CHARTOROWID")
                || upper.equals("ROWIDTOCHAR")
                || upper.equals("HEXTORAW")
                || upper.equals("RAWTOHEX")
                || upper.equals("NUMTODSINTERVAL")
                || upper.equals("NUMTOYMINTERVAL")
                || upper.equals("INTERVAL")
                || upper.equals("EMPTY_BLOB")
                || upper.equals("EMPTY_CLOB")
                || upper.equals("SIGN")
                || upper.equals("LOG")
                || upper.equals("EXP")
                || upper.equals("DUMP")
                || upper.equals("VSIZE")
                || upper.equals("UID")
                || upper.equals("USERENV")
                || upper.equals("ORA_HASH")
                || upper.equals("STANDARD_HASH")
                || upper.equals("SYS_GUID");
    }

    /**
     * BFS queue item.
     */
    private static class BfsItem {
        final String packageName;
        final String subprogramName;
        final String schema;          // target schema (standalone objects) for schema-aware visitKey
        final int depth;
        final String callerPackage;
        final String callerObject;
        final int callLine;
        final int argCount;           // call-site argument count (for overload resolution); -1 = unknown
        final List<String> argTypes;  // call-site argument type hints (W1); null/empty = unknown
        final String overloadDisc;    // resolved standalone overload discriminator (Postgres/DB2), else null
        final String overloadId;      // Postgres oid / DB2 SPECIFICNAME for that overload, else null

        BfsItem(String packageName, String subprogramName, int depth,
                String callerPackage, String callerObject, int callLine) {
            this(packageName, subprogramName, null, depth, callerPackage, callerObject, callLine, -1, null, null, null);
        }

        BfsItem(String packageName, String subprogramName, String schema, int depth,
                String callerPackage, String callerObject, int callLine) {
            this(packageName, subprogramName, schema, depth, callerPackage, callerObject, callLine, -1, null, null, null);
        }

        BfsItem(String packageName, String subprogramName, String schema, int depth,
                String callerPackage, String callerObject, int callLine, int argCount) {
            this(packageName, subprogramName, schema, depth, callerPackage, callerObject, callLine, argCount, null, null, null);
        }

        BfsItem(String packageName, String subprogramName, String schema, int depth,
                String callerPackage, String callerObject, int callLine, int argCount, List<String> argTypes) {
            this(packageName, subprogramName, schema, depth, callerPackage, callerObject, callLine, argCount, argTypes, null, null);
        }

        BfsItem(String packageName, String subprogramName, String schema, int depth,
                String callerPackage, String callerObject, int callLine, int argCount, List<String> argTypes,
                String overloadDisc, String overloadId) {
            this.packageName = packageName;
            this.subprogramName = subprogramName;
            this.schema = schema;
            this.depth = depth;
            this.callerPackage = callerPackage;
            this.callerObject = callerObject;
            this.callLine = callLine;
            this.argCount = argCount;
            this.argTypes = argTypes;
            this.overloadDisc = overloadDisc;
            this.overloadId = overloadId;
        }
    }
}
