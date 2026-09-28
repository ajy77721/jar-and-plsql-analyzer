package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.repo.AnalysisQuery;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

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

    /** How many facets {@link #writeAll} attempts — 4 node-scan + 2 table maps + complexity + stats. */
    public static final int VIEW_COUNT = 8;

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
        // map is the only shape that serves every table without re-scanning per table.
        try {
            if (views.writeTableMap(name, AnalysisViewStore.TABLE_JOINS,
                    staticReader.getAllTableJoins(name), verbose)) written++;
        } catch (Exception e) {
            logFacet(name, AnalysisViewStore.TABLE_JOINS, e, verbose);
        }
        try {
            if (views.writeTableMap(name, AnalysisViewStore.TABLE_COLUMNS,
                    staticReader.getAllTableColumnRefs(name), verbose)) written++;
        } catch (Exception e) {
            logFacet(name, AnalysisViewStore.TABLE_COLUMNS, e, verbose);
        }

        // Complexity at the endpoint's default weights; the endpoint recomputes for custom weights.
        try {
            ResponseEntity<Object> response = complexityScoringService.getScores(
                    name, new ComplexityScoringService.Weights(),
                    new ComplexityScoringService.Thresholds(), DEFAULT_ALGORITHM, DEFAULT_EXPONENT);
            if (writeBody(name, AnalysisViewStore.COMPLEXITY, response, verbose)) written++;
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

        // The call trees for the analysis's entry points, built once here instead of synthesised
        // from the three whole files on every request. Best-effort and capped; any entry point not
        // written is backfilled by the read path on first access.
        writeCallTrees(name);

        return written;
    }

    /**
     * Precomputes the call-tree sidecar for each of {@link StaticAnalysisReader#callTreeEntryPoints}
     * — the roots plus their depth-1 callees, capped. Each tree is the exact document the endpoint
     * returns for that node, built through {@link StaticAnalysisReader#getCallTree(String, String)}
     * (the same synthesis the read path uses), so serving the sidecar is byte-identical.
     *
     * <p>Never throws: one node's failure (or the entry-point selection itself) is logged and the
     * remaining nodes still run.
     */
    private void writeCallTrees(String name) {
        List<String> entryPoints;
        try {
            entryPoints = staticReader.callTreeEntryPoints(name);
        } catch (Exception e) {
            log.warn("call-tree precompute {} failed to select entry points: {}", name, e.getMessage());
            return;
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
