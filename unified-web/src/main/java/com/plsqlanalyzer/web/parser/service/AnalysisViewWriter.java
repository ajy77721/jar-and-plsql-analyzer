package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.RequestScopedArtifactStore;
import com.analyzer.storage.repo.AnalysisQuery;
import com.analyzer.storage.sso.CapturedSsoContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.plsqlanalyzer.web.parser.controller.ParserSearchFacets;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Writes an analysis's precomputed view summaries after {@code AnalysisService.runAnalysis} has
 * persisted every artifact.
 *
 * <p><b>It computes through the very code the endpoints read through — nothing is reimplemented.</b>
 * The four whole-analysis facets are fetched via {@link AnalysisRepository#document}, the same call
 * the controllers make, which on a miss runs the reader and (through {@link AnalysisViewStore})
 * writes the sidecar as a side effect; the two table-scoped facets are grouped from {@link
 * StaticAnalysisReader}'s one-pass scans; complexity and summary stats come from their own services
 * with the same defaults the endpoints use. The sidecar is therefore, by construction, the endpoint
 * payload.
 *
 * <p><b>Best-effort.</b> {@link #writeAll} is called from the analysis queue worker after the
 * primary artifact write has already succeeded; a failure here must never fail the job, and the
 * lazy backfill in {@link AnalysisViewStore} covers whatever is missed.
 */
@Component
public class AnalysisViewWriter {

    private static final Logger log = LoggerFactory.getLogger(AnalysisViewWriter.class);

    /** The endpoint defaults for {@code /complexity/scores} — mirrors {@link ComplexityScoringService}. */
    private static final String DEFAULT_ALGORITHM = "linear";
    private static final double DEFAULT_EXPONENT = 1.3;

    /**
     * Bounded pool for the independent facet computations of one precompute pass. It is deliberately
     * <b>not</b> {@code ShallowTree}'s listing pool: a facet's own node reads fan out on that pool,
     * and running a facet that waits on an inner fan-out on the same bounded pool would let the
     * outer tasks starve it (the classic nested-submission deadlock). Separate pools cannot.
     */
    private static final ExecutorService FACET_POOL = Executors.newFixedThreadPool(6, r -> {
        Thread t = new Thread(r, "view-precompute");
        t.setDaemon(true);
        return t;
    });

    private final AnalysisRepository repository;
    private final StaticAnalysisReader staticReader;
    private final ComplexityScoringService complexityScoringService;
    private final SummaryStatsService summaryStatsService;
    private final AnalysisViewStore views;
    private final ObjectMapper mapper;

    public AnalysisViewWriter(AnalysisRepository repository,
                              StaticAnalysisReader staticReader,
                              ComplexityScoringService complexityScoringService,
                              SummaryStatsService summaryStatsService,
                              AnalysisViewStore views,
                              ObjectMapper mapper) {
        this.repository = repository;
        this.staticReader = staticReader;
        this.complexityScoringService = complexityScoringService;
        this.summaryStatsService = summaryStatsService;
        this.views = views;
        this.mapper = mapper;
    }

    /**
     * How many facets {@link #writeAll} attempts — 4 node-scan + 2 table maps + complexity + stats
     * + callers map + search facets + 3 estate-facet indexes (views/triggers/globals). The
     * per-table split files, the per-object estate detail files and the non-default complexity
     * variants are members of their facets, not separate counts.
     */
    public static final int VIEW_COUNT = 14;

    /**
     * Computes and writes every parser-side view summary for {@code name}. Never throws — each view
     * is attempted independently, so one bad facet cannot stop the others.
     */
    public void writeAll(String name) {
        writeAll(name, true);
    }

    /**
     * As {@link #writeAll(String)}, reporting how many facets landed and optionally staying quiet.
     *
     * <p>The facets are independent, so they are computed <b>concurrently</b> on a bounded pool:
     * one read of {@code api/nodes/*.json} feeds them all (the single-pass node scan opened here),
     * and the four node-scan facets, the two table maps and the estate facets' index/detail/used-by
     * builds no longer queue behind one another. Each pool task re-installs the calling thread's SSO
     * context, so a sidecar computed off-thread still signs as the job's principal.
     *
     * <p>The background warm-up passes {@code verbose=false} and tallies the failures into its one
     * line per sweep, so a deployment whose store is unreachable does not warn once per facet per
     * analysis. The computation is identical either way; only the log level differs.
     *
     * @return the number of the {@link #VIEW_COUNT} facets whose sidecar was written
     */
    public int writeAll(String name, boolean verbose) {
        return writeAll(name, verbose, null);
    }

    /**
     * As {@link #writeAll(String, boolean)}, additionally reporting each facet as it lands.
     *
     * <p>{@code onFacet} is called with the running count of facets attempted so far (1..{@value
     * #VIEW_COUNT}) after each facet task returns. The progress engine passes a callback here so the
     * queue's "Warm up facets" stage shows a live N/{@value #VIEW_COUNT} instead of jumping from 0
     * to 14. The callback runs on the calling thread (the futures are drained there), so it needs no
     * synchronisation. Null means "no reporting" and is the default for every other caller.
     */
    public int writeAll(String name, boolean verbose, java.util.function.IntConsumer onFacet) {
        if (name == null || name.isBlank()) return 0;
        long startNanos = System.nanoTime();

        // Single-pass node scan: one read of every node document feeds every facet below instead of
        // each facet (and each estate detail/used-by build) re-reading all of them. Opened on this
        // thread, where the caller's routing and credential context is installed, so the read signs
        // as the right user. A store that cannot be listed leaves the pass closed and each facet
        // fails independently below, exactly as it did before.
        AutoCloseable pass = null;
        try {
            pass = staticReader.beginNodeScanPass(name);
        } catch (Throwable t) {
            log.debug("view precompute {}: node-scan pass could not be opened: {}", name, t.toString());
        }
        try {
            int written = computeFacets(name, verbose, onFacet);
            log.debug("view precompute {} wrote {}/{} facets in {} ms", name, written, VIEW_COUNT,
                    (System.nanoTime() - startNanos) / 1_000_000L);
            return written;
        } finally {
            if (pass != null) {
                try {
                    pass.close();
                } catch (Exception ignored) {
                    // The pass handle only removes a map entry; nothing here can matter.
                }
            }
        }
    }

    /**
     * Runs the independent facet computations concurrently and returns how many landed.
     *
     * <p>The calling thread's SSO context and routing scope are captured <b>before</b> fanning out
     * and re-installed on every pool task: a pool worker has neither a request-bound session nor the
     * job's {@code SsoJobContext}, so a facet computed there would otherwise read and write as the
     * shared default principal. This is the same hand-off {@code ShallowTree.parallelMap} performs
     * for its listing fan-out.
     */
    private int computeFacets(String name, boolean verbose,
                              java.util.function.IntConsumer onFacet) {
        CapturedSsoContext sso = CapturedSsoContext.capture(null);
        RequestScopedArtifactStore.Scope scope = storeScope(staticReader.store());

        List<Callable<Integer>> tasks = List.of(
                // The four node-scan facets: document() backfills each sidecar on the miss this is.
                () -> writeFacet(name, AnalysisQuery.Dataset.PLSQL_SEQUENCES, verbose),
                () -> writeFacet(name, AnalysisQuery.Dataset.PLSQL_CURSORS, verbose),
                () -> writeFacet(name, AnalysisQuery.Dataset.PLSQL_DYNAMIC_SQL, verbose),
                () -> writeFacet(name, AnalysisQuery.Dataset.PLSQL_JOINS, verbose),
                // The three estate-facet indexes plus their per-object detail/used-by sidecars.
                () -> writeEstateFacet(name, AnalysisQuery.Dataset.PLSQL_VIEWS,
                        AnalysisViewStore.VIEWS_INDEX, verbose),
                () -> writeEstateFacet(name, AnalysisQuery.Dataset.PLSQL_TRIGGERS,
                        AnalysisViewStore.TRIGGERS_INDEX, verbose),
                () -> writeEstateFacet(name, AnalysisQuery.Dataset.PLSQL_GLOBALS,
                        AnalysisViewStore.GLOBALS_INDEX, verbose),
                // Scheduled jobs — the crawler's api/jobs/index.json, cached as views/jobs-index.json.
                () -> writeFacet(name, AnalysisQuery.Dataset.PLSQL_JOBS, verbose),
                // The two table-scoped facets, grouped in one scan each; the split files beside them.
                () -> writeTableFacet(name, AnalysisViewStore.TABLE_JOINS, verbose),
                () -> writeTableFacet(name, AnalysisViewStore.TABLE_COLUMNS, verbose),
                () -> writeComplexityFacet(name, verbose),
                () -> writeStatsFacet(name, verbose),
                () -> writeSearchFacet(name, verbose),
                () -> writeCallTreesFacet(name, verbose)
        );

        List<Future<Integer>> futures = new ArrayList<>(tasks.size());
        for (Callable<Integer> task : tasks) {
            futures.add(FACET_POOL.submit(() -> {
                try (AutoCloseable ssoHandle = sso == null ? null : sso.install();
                     AutoCloseable scopeHandle = scope.install()) {
                    return task.call();
                } catch (Throwable t) {
                    // One facet's failure is contained; the rest still land and the set is rebuilt
                    // on the next sweep (hasCompleteViewSet stays false).
                    log.debug("view precompute {} facet failed: {}", name, t.toString());
                    return 0;
                }
            }));
        }
        int written = 0;
        int attempted = 0;
        for (Future<Integer> f : futures) {
            try {
                written += f.get();
            } catch (Exception ignored) {
                // Each task contains its own failures; a future only fails on interruption.
            } finally {
                attempted++;
                if (onFacet != null) onFacet.accept(attempted);
            }
        }
        return written;
    }

    /** The calling thread's routing scope for {@code store}, or a no-op scope when it has none. */
    private static RequestScopedArtifactStore.Scope storeScope(ArtifactStore store) {
        return store instanceof RequestScopedArtifactStore routed
                ? routed.captureScope() : RequestScopedArtifactStore.Scope.NONE;
    }

    /** One node-scan facet: {@code document()} backfills its sidecar on the miss this call is. */
    private int writeFacet(String name, AnalysisQuery.Dataset dataset, boolean verbose) {
        return facet(name, dataset, verbose) ? 1 : 0;
    }

    /**
     * One estate-facet index plus, beside it, its per-object detail files and whole-analysis used-by
     * map. When the manifest records the facet as empty there is nothing for those builds to hold,
     * so they are skipped — the manifest-driven skip; a stale or absent manifest records nothing and
     * the full precompute runs unchanged.
     */
    private int writeEstateFacet(String name, AnalysisQuery.Dataset dataset, String indexView,
                                 boolean verbose) {
        int n = writeFacet(name, dataset, verbose);
        if (!recordedEmpty(name, indexView)) {
            switch (indexView) {
                case AnalysisViewStore.VIEWS_INDEX -> writeViewDetails(name, verbose);
                case AnalysisViewStore.TRIGGERS_INDEX -> writeTriggerDetails(name, verbose);
                case AnalysisViewStore.GLOBALS_INDEX -> writeGlobalDetails(name, verbose);
                default -> { }
            }
        }
        return n;
    }

    /** True when the fresh manifest records {@code view} as having zero rows. */
    private boolean recordedEmpty(String name, String view) {
        Long rows = views.facetRows(name, view);
        return rows != null && rows == 0L;
    }

    /**
     * One table-scoped facet: the grouped one-scan map, then one small split file per table so the
     * per-table endpoint reads one file and a lazy backfill writes one file (not the whole map).
     */
    private int writeTableFacet(String name, String view, boolean verbose) {
        boolean joins = AnalysisViewStore.TABLE_JOINS.equals(view);
        try {
            Map<String, JsonNode> byTable = joins
                    ? staticReader.getAllTableJoins(name)
                    : staticReader.getAllTableColumnRefs(name);
            int n = views.writeTableMap(name, view, byTable, verbose) ? 1 : 0;
            writeTableFiles(name, joins ? AnalysisViewStore.TABLE_JOINS_DIR
                    : AnalysisViewStore.TABLE_COLUMNS_DIR, byTable, verbose);
            return n;
        } catch (Exception e) {
            logFacet(name, view, e, verbose);
            return 0;
        }
    }

    /** Complexity at the endpoint's default weights, plus the two precomputed grid neighbours. */
    private int writeComplexityFacet(String name, boolean verbose) {
        int n = 0;
        try {
            ResponseEntity<Object> response = complexityScoringService.getScores(
                    name, new ComplexityScoringService.Weights(),
                    new ComplexityScoringService.Thresholds(), DEFAULT_ALGORITHM, DEFAULT_EXPONENT);
            if (writeBody(name, AnalysisViewStore.COMPLEXITY, response, verbose)) n++;
            writeComplexityVariants(name, verbose);
        } catch (Exception e) {
            logFacet(name, AnalysisViewStore.COMPLEXITY, e, verbose);
        }
        return n;
    }

    /** Summary stats at the default (unscoped) request. */
    private int writeStatsFacet(String name, boolean verbose) {
        try {
            return writeBody(name, AnalysisViewStore.SUMMARY_STATS,
                    summaryStatsService.getStats(name, null, null, null), verbose) ? 1 : 0;
        } catch (Exception e) {
            logFacet(name, AnalysisViewStore.SUMMARY_STATS, e, verbose);
            return 0;
        }
    }

    /** The raw search-facet arrays {@code /global-search} filters, stored once. */
    private int writeSearchFacet(String name, boolean verbose) {
        try {
            ObjectNode facets = ParserSearchFacets.build(repository, mapper, name);
            return views.writeSearch(name, facets, verbose) ? 1 : 0;
        } catch (Exception e) {
            logFacet(name, AnalysisViewStore.SEARCH, e, verbose);
            return 0;
        }
    }

    /** The call trees and callers for the analysis's entry points. */
    private int writeCallTreesFacet(String name, boolean verbose) {
        return writeCallTrees(name, verbose) ? 1 : 0;
    }

    /**
     * Writes the per-view detail + chain files and the whole-analysis used-by map beside the views
     * index. Best-effort per view, like {@link #writeTableFiles}.
     */
    private void writeViewDetails(String name, boolean verbose) {
        try {
            JsonNode index = staticReader.getViews(name);
            if (index != null && index.isArray()) {
                for (JsonNode row : index) {
                    String v = row.path("name").asText("");
                    if (v.isEmpty()) continue;
                    try {
                        views.writeFacetFile(name, AnalysisViewStore.VIEW_DETAIL_DIR, v,
                                staticReader.getViewDetail(name, v), verbose);
                        views.writeFacetFile(name, AnalysisViewStore.VIEW_DETAIL_DIR, v + "__chain",
                                staticReader.getViewChain(name, v), verbose);
                    } catch (Exception ex) {
                        log.debug("view detail precompute {} / {} failed: {}", name, v, ex.getMessage());
                    }
                }
            }
            views.write(name, AnalysisViewStore.VIEWS_USED_BY,
                    staticReader.getViewsUsedByMap(name), verbose);
        } catch (Exception e) {
            logFacet(name, AnalysisViewStore.VIEWS_INDEX, e, verbose);
        }
    }

    /** Per-trigger detail + chain files and the fired-by map. Best-effort. */
    private void writeTriggerDetails(String name, boolean verbose) {
        try {
            JsonNode index = staticReader.getTriggers(name);
            if (index != null && index.isArray()) {
                for (JsonNode row : index) {
                    String t = row.path("name").asText("");
                    if (t.isEmpty()) continue;
                    try {
                        views.writeFacetFile(name, AnalysisViewStore.TRIGGER_DETAIL_DIR, t,
                                staticReader.getTriggerDetail(name, t), verbose);
                        views.writeFacetFile(name, AnalysisViewStore.TRIGGER_DETAIL_DIR, t + "__chain",
                                staticReader.getTriggerChain(name, t), verbose);
                    } catch (Exception ex) {
                        log.debug("trigger detail precompute {} / {} failed: {}", name, t, ex.getMessage());
                    }
                }
            }
            views.write(name, AnalysisViewStore.TRIGGERS_USED_BY,
                    staticReader.getTriggersUsedByMap(name), verbose);
        } catch (Exception e) {
            logFacet(name, AnalysisViewStore.TRIGGERS_INDEX, e, verbose);
        }
    }

    /** Per-package detail files and the global used-by map. Best-effort. */
    private void writeGlobalDetails(String name, boolean verbose) {
        try {
            JsonNode index = staticReader.getGlobals(name);
            if (index != null && index.isArray()) {
                for (JsonNode row : index) {
                    String pkg = row.path("package").asText("");
                    if (pkg.isEmpty()) continue;
                    try {
                        views.writeFacetFile(name, AnalysisViewStore.GLOBAL_DETAIL_DIR, pkg,
                                staticReader.getGlobalDetail(name, pkg), verbose);
                    } catch (Exception ex) {
                        log.debug("global detail precompute {} / {} failed: {}", name, pkg, ex.getMessage());
                    }
                }
            }
            views.write(name, AnalysisViewStore.GLOBALS_USED_BY,
                    staticReader.getGlobalsUsedByMap(name), verbose);
        } catch (Exception e) {
            logFacet(name, AnalysisViewStore.GLOBALS_INDEX, e, verbose);
        }
    }

    /**
     * Writes one small split file per table beside the whole-map sidecar. Best-effort per table: one
     * unwritable table does not stop the rest, and the map still serves every table either way.
     */
    private void writeTableFiles(String name, String dir, Map<String, JsonNode> byTable, boolean verbose) {
        for (Map.Entry<String, JsonNode> e : byTable.entrySet()) {
            try {
                views.writeTableFile(name, dir, e.getKey(), e.getValue(), verbose);
            } catch (Exception ex) {
                log.debug("table split precompute {} / {} / {} failed: {}", name, dir, e.getKey(),
                        ex.getMessage());
            }
        }
    }

    /**
     * Precomputes the {@code /complexity/scores} bodies for the two precomputed grid neighbours —
     * the other algorithms {@code complexity.js} offers, at the endpoint's default weights,
     * thresholds and exponent. Custom parameters are not precomputed: the endpoint computes them
     * once and caches a hashed variant. Never throws.
     */
    private void writeComplexityVariants(String name, boolean verbose) {
        for (String algorithm : new String[]{"balanced", "logarithmic"}) {
            try {
                ResponseEntity<Object> response = complexityScoringService.getScores(
                        name, new ComplexityScoringService.Weights(),
                        new ComplexityScoringService.Thresholds(), algorithm, DEFAULT_EXPONENT);
                String view = AnalysisViewStore.complexityViewName(algorithm, DEFAULT_EXPONENT,
                        0.3, 15, 10, 8, 20, 5, 50, 150);
                writeBody(name, view, response, verbose);
            } catch (Exception e) {
                logFacet(name, AnalysisViewStore.COMPLEXITY_VARIANT_PREFIX + algorithm, e, verbose);
            }
        }
    }

    /**
     * Precomputes the call-tree sidecar for each of {@link StaticAnalysisReader#callTreeEntryPoints}
     * — the roots plus their depth-1 callees, capped — and, in the same pass, the
     * {@code views/callers.json} map for those same entry points. Each tree and each callers array
     * is the exact document the endpoint returns for that node, built through the same reader
     * synthesis the read path uses, so serving a sidecar is byte-identical.
     *
     * <p>The callers map is built with one index/call-graph/id-map read shared across every entry
     * point (see {@link StaticAnalysisReader#getCallersForNodes}), not one graph parse per node.
     *
     * <p>Never throws: one node's failure (or the entry-point selection itself) is logged and the
     * remaining nodes still run.
     *
     * @return true when the callers sidecar was written
     */
    private boolean writeCallTrees(String name, boolean verbose) {
        List<String> entryPoints;
        try {
            entryPoints = staticReader.callTreeEntryPoints(name);
        } catch (Exception e) {
            log.warn("call-tree precompute {} failed to select entry points: {}", name, e.getMessage());
            return false;
        }
        int written = 0;
        for (String nodeId : entryPoints) {
            try {
                JsonNode tree = staticReader.getCallTree(name, nodeId);
                if (tree != null) {
                    views.writeCallTree(name, nodeId, tree);
                    written++;
                }
            } catch (Exception e) {
                log.debug("call-tree precompute {} / {} failed: {}", name, nodeId, e.getMessage());
            }
        }
        if (written > 0) {
            log.debug("call-tree precompute {} wrote {} sidecar(s)", name, written);
        }

        try {
            return views.writeCallers(name, staticReader.getCallersForNodes(name, entryPoints), verbose);
        } catch (Exception e) {
            logFacet(name, AnalysisViewStore.CALLERS, e, verbose);
            return false;
        }
    }

    private boolean facet(String name, AnalysisQuery.Dataset dataset, boolean verbose) {
        try {
            // document() writes the sidecar on the miss this call is; a null document means the
            // analysis has no such facet, so no sidecar was written and this facet did not land.
            return repository.document(AnalysisQuery.of(dataset, name)) != null;
        } catch (Exception e) {
            logFacet(name, dataset.toString(), e, verbose);
            return false;
        }
    }

    /** Writes a service response body as a sidecar when the response actually carries one. */
    private boolean writeBody(String name, String view, ResponseEntity<Object> response, boolean verbose) {
        if (response == null || !response.getStatusCode().is2xxSuccessful() || response.getBody() == null) {
            return false;
        }
        JsonNode body = mapper.valueToTree(response.getBody());
        return views.write(name, view, body, verbose);
    }

    private static void logFacet(String name, String view, Exception e, boolean verbose) {
        if (verbose) {
            log.warn("view write {} / {} failed: {}", name, view, e.getMessage());
        } else {
            log.debug("view write {} / {} failed: {}", name, view, e.getMessage());
        }
    }
}
