package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.repo.AnalysisQuery;
import com.fasterxml.jackson.databind.JsonNode;
import com.plsqlanalyzer.web.parser.controller.ParserSearchFacets;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
     * + callers map + search facets. The per-table split files and the non-default complexity
     * variants are members of their facets, not separate counts.
     */
    public static final int VIEW_COUNT = 10;

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
     * <p>The background warm-up passes {@code verbose=false} and tallies the failures into its one
     * line per sweep, so a deployment whose store is unreachable does not warn once per facet per
     * analysis. The computation is identical either way; only the log level differs.
     *
     * @return the number of the {@link #VIEW_COUNT} facets whose sidecar was written (0..8)
     */
    public int writeAll(String name, boolean verbose) {
        if (name == null || name.isBlank()) return 0;
        int written = 0;

        // The four node-scan facets: document() backfills each sidecar on the miss this call is.
        if (facet(name, AnalysisQuery.Dataset.PLSQL_SEQUENCES, verbose)) written++;
        if (facet(name, AnalysisQuery.Dataset.PLSQL_CURSORS, verbose)) written++;
        if (facet(name, AnalysisQuery.Dataset.PLSQL_DYNAMIC_SQL, verbose)) written++;
        if (facet(name, AnalysisQuery.Dataset.PLSQL_JOINS, verbose)) written++;

        // The two table-scoped facets, grouped in one scan each — the endpoint is per-table, so a
        // map is the only shape that serves every table without re-scanning per table. Beside each
        // map, one small split file per table, so the per-table endpoint reads one file and a lazy
        // backfill writes one file (not the whole map).
        try {
            Map<String, JsonNode> joins = staticReader.getAllTableJoins(name);
            if (views.writeTableMap(name, AnalysisViewStore.TABLE_JOINS, joins, verbose)) written++;
            writeTableFiles(name, AnalysisViewStore.TABLE_JOINS_DIR, joins, verbose);
        } catch (Exception e) {
            logFacet(name, AnalysisViewStore.TABLE_JOINS, e, verbose);
        }
        try {
            Map<String, JsonNode> columns = staticReader.getAllTableColumnRefs(name);
            if (views.writeTableMap(name, AnalysisViewStore.TABLE_COLUMNS, columns, verbose)) written++;
            writeTableFiles(name, AnalysisViewStore.TABLE_COLUMNS_DIR, columns, verbose);
        } catch (Exception e) {
            logFacet(name, AnalysisViewStore.TABLE_COLUMNS, e, verbose);
        }

        // Complexity at the endpoint's default weights; the endpoint recomputes for custom weights.
        try {
            ResponseEntity<Object> response = complexityScoringService.getScores(
                    name, new ComplexityScoringService.Weights(),
                    new ComplexityScoringService.Thresholds(), DEFAULT_ALGORITHM, DEFAULT_EXPONENT);
            if (writeBody(name, AnalysisViewStore.COMPLEXITY, response, verbose)) written++;
            writeComplexityVariants(name, verbose);
        } catch (Exception e) {
            logFacet(name, AnalysisViewStore.COMPLEXITY, e, verbose);
        }

        // Summary stats at the default (unscoped) request.
        try {
            if (writeBody(name, AnalysisViewStore.SUMMARY_STATS,
                    summaryStatsService.getStats(name, null, null, null), verbose)) written++;
        } catch (Exception e) {
            logFacet(name, AnalysisViewStore.SUMMARY_STATS, e, verbose);
        }

        // The raw search-facet arrays /global-search filters — the same reads the controller makes,
        // stored once so a search stops re-reading the tables index and every node file per request.
        try {
            ObjectNode facets = ParserSearchFacets.build(repository, mapper, name);
            if (views.writeSearch(name, facets, verbose)) written++;
        } catch (Exception e) {
            logFacet(name, AnalysisViewStore.SEARCH, e, verbose);
        }

        // The call trees and callers for the analysis's entry points, built once here instead of
        // synthesised from the three whole files on every request. Best-effort and capped; any entry
        // point not written is backfilled by the read path on first access.
        if (writeCallTrees(name, verbose)) written++;

        return written;
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
