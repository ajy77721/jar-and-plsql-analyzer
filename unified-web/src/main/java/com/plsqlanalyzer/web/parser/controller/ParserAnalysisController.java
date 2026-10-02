package com.plsqlanalyzer.web.parser.controller;

import com.analyzer.queue.AnalysisQueueService;
import com.analyzer.queue.QueueJob;
import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.page.Cursor;
import com.analyzer.storage.repo.AnalysisQuery;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.web.page.FacetPageResponder;
import com.plsqlanalyzer.web.page.FacetSpec;
import com.plsqlanalyzer.web.parser.config.JoinComplexityConfig;
import com.plsqlanalyzer.web.parser.model.AnalysisInfo;
import com.plsqlanalyzer.web.parser.service.AnalysisPaths;
import com.plsqlanalyzer.web.parser.service.AnalysisService;
import com.plsqlanalyzer.web.parser.service.ClaudePurposeSummaryModels;
import com.plsqlanalyzer.web.parser.service.ClaudePurposeSummaryService;
import com.plsqlanalyzer.web.parser.service.ComplexityScoringService;
import com.plsqlanalyzer.web.parser.service.DetailHeaderCountsService;
import com.plsqlanalyzer.web.parser.service.EntityCursorsReader;
import com.plsqlanalyzer.web.parser.service.EntityDynamicSqlReader;
import com.plsqlanalyzer.web.parser.service.EntityPredicatesReader;
import com.plsqlanalyzer.web.parser.service.EntitySequencesReader;
import com.plsqlanalyzer.web.parser.service.EntityJoinsReader;
import com.plsqlanalyzer.web.parser.service.EntityProceduresReader;
import com.plsqlanalyzer.web.parser.service.EntityTablesReader;
import com.plsqlanalyzer.web.parser.service.JoinQuerySummary;
import com.plsqlanalyzer.web.parser.service.ParserProcedureReportsService;
import com.plsqlanalyzer.web.parser.service.ParserScopeResolver;
import com.plsqlanalyzer.web.parser.service.StatementsSummaryService;
import com.plsqlanalyzer.web.parser.service.SummaryStatsService;
import com.plsqlanalyzer.web.parser.service.TableTriggersService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

@RestController
@RequestMapping("/api/parser")
public class ParserAnalysisController {

    private final AnalysisService service;
    private final AnalysisQueueService queueService;
    private final FacetPageResponder facets;
    private final ParserScopeResolver scopes;
    private final ObjectMapper mapper;
    /** Precomputed view summaries for the two index-backed endpoints (complexity, summary stats). */
    private final com.plsqlanalyzer.web.parser.service.AnalysisViewStore views;
    /** The four node-scan facets are queries now, not service calls — PLAN-09 Increment C. */
    private final AnalysisRepository repository;
    private final JoinComplexityConfig joinComplexityConfig;
    private final ParserProcedureReportsService procedureReportsService;
    private final StatementsSummaryService statementsSummaryService;
    private final DetailHeaderCountsService detailHeaderCountsService;
    private final TableTriggersService tableTriggersService;
    private final ComplexityScoringService complexityScoringService;
    private final SummaryStatsService summaryStatsService;
    private final EntityTablesReader entityTablesReader;
    private final EntityJoinsReader entityJoinsReader;
    private final EntityCursorsReader entityCursorsReader;
    private final EntitySequencesReader entitySequencesReader;
    private final EntityProceduresReader entityProceduresReader;
    private final EntityPredicatesReader entityPredicatesReader;
    private final EntityDynamicSqlReader entityDynamicSqlReader;
    private final com.analyzer.repo.EntityIngestConfig.EntityIngestConnection entityIngestConnection;
    private final boolean entityReadDefault;
    private final ClaudePurposeSummaryService purposeSummaryService;

    public ParserAnalysisController(@Qualifier("parserAnalysisService") AnalysisService service,
                                     AnalysisQueueService queueService,
                                     FacetPageResponder facets,
                                     ParserScopeResolver scopes,
                                     ObjectMapper mapper,
                                     com.plsqlanalyzer.web.parser.service.AnalysisViewStore views,
                                     AnalysisRepository repository,
                                     JoinComplexityConfig joinComplexityConfig,
                                     ParserProcedureReportsService procedureReportsService,
                                     StatementsSummaryService statementsSummaryService,
                                     DetailHeaderCountsService detailHeaderCountsService,
                                     TableTriggersService tableTriggersService,
                                     ComplexityScoringService complexityScoringService,
                                     SummaryStatsService summaryStatsService,
                                     EntityTablesReader entityTablesReader,
                                     EntityJoinsReader entityJoinsReader,
                                     EntityCursorsReader entityCursorsReader,
                                     EntitySequencesReader entitySequencesReader,
                                     EntityProceduresReader entityProceduresReader,
                                     EntityPredicatesReader entityPredicatesReader,
                                     EntityDynamicSqlReader entityDynamicSqlReader,
                                     com.analyzer.repo.EntityIngestConfig.EntityIngestConnection entityIngestConnection,
                                     @org.springframework.beans.factory.annotation.Value(
                                             "${app.storage.entity-read.enabled:false}") boolean entityReadDefault,
                                     ClaudePurposeSummaryService purposeSummaryService) {
        this.service = service;
        this.queueService = queueService;
        this.joinComplexityConfig = joinComplexityConfig;
        this.facets = facets;
        this.scopes = scopes;
        this.mapper = mapper;
        this.views = views;
        this.repository = repository;
        this.procedureReportsService = procedureReportsService;
        this.statementsSummaryService = statementsSummaryService;
        this.detailHeaderCountsService = detailHeaderCountsService;
        this.tableTriggersService = tableTriggersService;
        this.complexityScoringService = complexityScoringService;
        this.summaryStatsService = summaryStatsService;
        this.entityTablesReader = entityTablesReader;
        this.entityJoinsReader = entityJoinsReader;
        this.entityCursorsReader = entityCursorsReader;
        this.entitySequencesReader = entitySequencesReader;
        this.entityProceduresReader = entityProceduresReader;
        this.entityPredicatesReader = entityPredicatesReader;
        this.entityDynamicSqlReader = entityDynamicSqlReader;
        this.entityIngestConnection = entityIngestConnection;
        this.entityReadDefault = entityReadDefault;
        this.purposeSummaryService = purposeSummaryService;
    }

    /**
     * Mode 4's mode-wide toggle (§159) — resolves the effective read source for all 7 entity-backed
     * facets in one place, rather than each endpoint re-deciding it inline.
     *
     * <p>An explicit {@code source} request param always wins (this is how the 7 comparison tests,
     * and any future debugging, force one path or the other regardless of the default). Absent
     * that, {@code app.storage.entity-read.enabled} decides — off by default, matching every other
     * mode-4 toggle's own "no behaviour change unless explicitly turned on" convention (§149).
     * Never returns {@code true} when {@code entityIngestConnection} itself isn't configured, since
     * there would be nothing to read from.
     */
    private boolean useEntitySource(String source) {
        if (!entityIngestConnection.enabled()) return false;
        if ("artifact".equals(source)) return false;
        if ("entity".equals(source)) return true;
        return entityReadDefault;
    }

    /**
     * Analysis history — Q31 / I11, serving both the History cards and the Performance Summary.
     *
     * <p>Without a paging parameter this is byte-for-byte the list it has always been, {@code
     * errors[]} included. With one it is a page of projected rows: {@code errors[]} becomes
     * {@code errorCount}, and {@code dbType} arrives derived so that neither screen has to scan a
     * JDBC URL for a substring (they had a copy of that scanner each, and the copies disagreed).
     *
     * <p>The datasource and project pills are the {@code cf} column filters, so the cascade the two
     * dropdowns implement runs against the whole history rather than the current page.
     *
     * <p><b>MID-021 (§104): the unpaged branch here is permanently exempt, not a pending
     * conversion.</b> Four real client call sites (`index.html`, `parser/js/sync.js`,
     * `parser/js/api.js`, `plsql/js/api.js`) fetch this endpoint bare, relying on
     * {@code !params.wantsEnvelope()}'s legacy array. Converting them would cost real work for no
     * measured benefit: the list this returns is bounded by how many analyses have ever been run,
     * not by procedure/endpoint count — 2 on this repo's own data — comfortably under the same
     * 1,000-row instant-client-filter threshold MID-002 (§39) already established elsewhere in this
     * registry. This is the same "measured, no benefit" exemption `joins.js`/`sequences.js` already
     * have, just stated here rather than left as an open TODO.
     */
    @GetMapping("/analyses")
    public ResponseEntity<?> listAnalyses(
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestHeader(value = "Accept", required = false) String accept) {
        var params = new FacetPageResponder.Params(limit, after, sort, q, null, cf, accept);
        try {
            if (!params.wantsEnvelope()) {
                return ResponseEntity.ok(service.listAnalyses());
            }
            List<JsonNode> rows = List.copyOf(historyRows());
            return facets.respondStreaming(rows.iterator(), HistoryFacet.SPEC, params, null);
        } catch (IllegalArgumentException | Cursor.InvalidCursorException e) {
            return FacetPageResponder.badRequest(mapper, e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * The counts, dropdown values and running totals the two screens recompute per keystroke.
     *
     * <p>Separate from the page, and filtered by the same {@code q}/{@code cf}, because they answer
     * a different question and must not shrink to the current page: "8 analyses, 1.2 M LOC" is a
     * property of the filter, not of the twenty-five rows on screen. {@code maxDurationMs} is here
     * for the Performance Summary's time bar, which is a percentage of the longest run in the
     * filtered set — the one value a paged table cannot derive for itself.
     */
    @GetMapping("/analyses/facets")
    public ResponseEntity<JsonNode> analysesFacets(
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "cf", required = false) List<String> cf) {
        try {
            // The very predicate the page uses, so the summary and the rows beneath it can never
            // disagree about which analyses a filter matches.
            var params = new FacetPageResponder.Params(null, null, null, q, null, cf, null);
            Predicate<JsonNode> filter = facets.filterFor(HistoryFacet.SPEC, params, null);
            var all = historyRows();
            var matched = new java.util.ArrayList<com.fasterxml.jackson.databind.node.ObjectNode>();
            for (var row : all) {
                if (filter.test(row)) matched.add(row);
            }
            return ResponseEntity.ok(HistoryFacet.summary(mapper, all, matched));
        } catch (IllegalArgumentException e) {
            return FacetPageResponder.badRequest(mapper, e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * The error messages of one analysis — the body the list projection dropped.
     *
     * <p>The list carries {@code errorCount}, which is all either screen renders in a row. The
     * messages themselves are only ever shown in the Performance Summary's detail panel, for the
     * one row a user clicked, so they are fetched then rather than sent for all rows always.
     */
    @GetMapping("/analyses/{name}/errors")
    public ResponseEntity<Map<String, Object>> analysisErrors(@PathVariable("name") String name) {
        try {
            // Task #33: listAnalyses() returns the canonical stored name, so compare canonically.
            String canonical = AnalysisPaths.canonical(name);
            for (AnalysisInfo a : service.listAnalyses()) {
                if (canonical.equals(a.getName())) {
                    List<String> errors = a.getErrors() == null ? List.of() : a.getErrors();
                    return ResponseEntity.ok(Map.of("name", name, "errors", errors));
                }
            }
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    /** History as projected rows. One index.json read per analysis, which is inherent to the list. */
    private List<com.fasterxml.jackson.databind.node.ObjectNode> historyRows() throws Exception {
        List<AnalysisInfo> all = service.listAnalyses();
        var rows = new java.util.ArrayList<com.fasterxml.jackson.databind.node.ObjectNode>(all.size());
        for (AnalysisInfo a : all) rows.add(HistoryFacet.row(mapper, a));
        return rows;
    }


    @PostMapping("/analyze")
    public ResponseEntity<Map<String, Object>> runAnalysis(@RequestBody Map<String, String> body) {
        String entryPoint = body.get("entryPoint");
        if (entryPoint == null || entryPoint.isBlank()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "entryPoint is required"));
        }
        String owner = body.get("owner");
        String objectType = body.get("objectType");
        String project = body.get("project");
        String environment = body.get("environment");
        String dbType = body.get("dbType");
        String connName = body.get("connName");
        // Force-refresh: bypass the analysis idempotency registry for this request. Accepts the
        // usual truthy spellings so a plain form checkbox works.
        String forceRefreshRaw = body.get("forceRefresh");
        boolean forceRefresh = forceRefreshRaw != null
                && ("true".equalsIgnoreCase(forceRefreshRaw.trim())
                    || "1".equals(forceRefreshRaw.trim())
                    || "on".equalsIgnoreCase(forceRefreshRaw.trim())
                    || "yes".equalsIgnoreCase(forceRefreshRaw.trim()));

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("entryPoint", entryPoint.trim());
        if (owner != null && !owner.isBlank()) metadata.put("owner", owner.trim());
        if (objectType != null && !objectType.isBlank()) metadata.put("objectType", objectType.trim());
        if (project != null && !project.isBlank()) metadata.put("project", project.trim());
        if (environment != null && !environment.isBlank()) metadata.put("environment", environment.trim());
        if (dbType != null && !dbType.isBlank()) metadata.put("dbType", dbType.trim());
        if (connName != null && !connName.isBlank()) metadata.put("connName", connName.trim());
        if (forceRefresh) metadata.put("forceRefresh", "true");

        String dbLabel = (dbType != null && !dbType.isBlank() && !"ORACLE".equalsIgnoreCase(dbType))
                ? "[" + dbType.trim() + "] " : "";
        String displayName = "Parse: " + dbLabel + entryPoint.trim();
        QueueJob job = queueService.submit(QueueJob.Type.PARSER_ANALYSIS, displayName, metadata);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "queued");
        result.put("jobId", job.id);
        result.put("entryPoint", entryPoint.trim());
        return ResponseEntity.ok(result);
    }

    @PostMapping("/cache/clear")
    public ResponseEntity<Map<String, Object>> clearCache(
            @RequestParam(value = "name", required = false) String name) {
        int cleared = service.clearCache(name);
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("status", "ok");
        r.put("scope", (name != null && !name.isBlank()) ? name : "all");
        r.put("cleared", cleared);
        return ResponseEntity.ok(r);
    }

    @GetMapping("/cache/by-connection")
    public ResponseEntity<List<Map<String, Object>>> cacheByConnection() {
        try {
            return ResponseEntity.ok(service.cacheByConnection());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping("/cache/clear-connection")
    public ResponseEntity<Map<String, Object>> clearConnectionCache(
            @RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "environment", required = false) String environment,
            @RequestParam(value = "connName", required = false) String connName) {
        try {
            int evicted = service.clearConnectionCache(project, environment, connName);
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("status", "ok");
            r.put("project", project);
            r.put("environment", environment);
            r.put("connName", connName);
            r.put("evicted", evicted);
            return ResponseEntity.ok(r);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @DeleteMapping("/analyses/{name}")
    public ResponseEntity<Void> deleteAnalysis(@PathVariable("name") String name) {
        try {
            service.deleteAnalysis(name);
            return ResponseEntity.noContent().build();
        } catch (java.nio.file.NoSuchFileException e) {
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/analyses/{name}/index")
    public ResponseEntity<JsonNode> getIndex(@PathVariable("name") String name) {
        try {
            // MID-002: nodes[] is 53.1% of this response on the largest analysis measured; the
            // client now fetches it from /procedures instead (paged, searchable) rather than every
            // page load carrying the whole node list regardless of whether it is ever used.
            return ResponseEntity.ok(service.getIndexWithoutNodes(name));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/node/{file}")
    public ResponseEntity<JsonNode> getNodeDetail(
            @PathVariable("name") String name,
            @PathVariable("file") String file) {
        try {
            JsonNode detail = service.getNodeDetail(name, file);
            // §267 — additive purposeSummary merge. Keyed by the node's own "id"/"nodeId" field
            // (whichever the detail JSON carries) rather than the {file} path segment, since that
            // segment is a filename, not necessarily the nodeId. Best-effort: a node with no cached
            // summary yet (not generated, or generation still running) is returned unchanged.
            if (detail != null && detail.isObject()) {
                String nodeId = detail.has("id") ? detail.get("id").asText(null)
                        : detail.has("nodeId") ? detail.get("nodeId").asText(null) : null;
                if (nodeId != null) {
                    ClaudePurposeSummaryModels.PurposeCacheEntry entry =
                            purposeSummaryService.getSummary(name, nodeId);
                    if (entry != null && entry.purposeSummary != null) {
                        com.fasterxml.jackson.databind.node.ObjectNode obj = (com.fasterxml.jackson.databind.node.ObjectNode) detail;
                        obj.put("purposeSummary", entry.purposeSummary);
                    }
                }
            }
            return ResponseEntity.ok(detail);
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * §267 — trigger purpose-summary generation for every procedure in an analysis, as a background
     * {@link AnalysisQueueService} job (matching UI-043/§254's JAR-export-bundle job shape): this can
     * mean one Claude CLI call per ~8 procedures, which for a real analysis can take minutes, so it
     * must not block the HTTP request. Progress/result are read via the standard
     * {@code GET /api/queue/{id}} poll — see this session's own memory note that {@code /api/queue}
     * is the one real progress endpoint.
     */
    @PostMapping("/analyses/{name}/purpose-summary/generate")
    public ResponseEntity<Map<String, Object>> generatePurposeSummaries(@PathVariable("name") String name) {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("analysisName", name);
        QueueJob job = queueService.submit(QueueJob.Type.PLSQL_PURPOSE_SUMMARY,
                "Purpose summaries: " + name, metadata);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("jobId", job.id);
        body.put("status", "QUEUED");
        return ResponseEntity.accepted().body(body);
    }

    /**
     * §267 — every cached purpose summary for an analysis, keyed by nodeId. Read-only, never
     * triggers generation; a node absent from the map simply has none yet.
     */
    @GetMapping("/analyses/{name}/purpose-summary")
    public ResponseEntity<Map<String, ClaudePurposeSummaryModels.PurposeCacheEntry>> getPurposeSummaries(
            @PathVariable("name") String name) {
        return ResponseEntity.ok(purposeSummaryService.getSummaries(name));
    }

    @GetMapping("/analyses/{name}/tables")
    public ResponseEntity<JsonNode> getTables(@PathVariable("name") String name) {
        try {
            return ResponseEntity.ok(service.getTables(name));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * Table counts only, for the topbar.
     *
     * <p>Exists because the topbar used to trigger a fetch of the whole tables index — 152 KB
     * measured, ~307 KB at the largest observed — to render a single integer, on every analysis
     * load. The full index is still available at {@code /tables} for the Table Detail screen.
     */
    /**
     * The Table Operations grid, one page at a time.
     *
     * <p>Separate path from {@code /tables} rather than a replacement: that document is still read
     * whole by the Table Detail modal and by export, both of which need every access line. This is
     * the grid's door, and it is the last parser facet to get one.
     *
     * <p>{@code scope} and {@code procId} are not optional extras. The grid implements
     * Direct/Subtree/SubFlow by filtering each row's access details in the browser; with one page in
     * memory that filter would apply to the fifty rows on screen and look convincing while being
     * wrong. The scope travels with the request for the same reason it does on the other four.
     */
    @GetMapping("/analyses/{name}/tables/page")
    public ResponseEntity<JsonNode> getTablesPage(
            @PathVariable("name") String name,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "types", required = false) String types,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId,
            @RequestParam(value = "directProcId", required = false) String directProcId,
            @RequestParam(value = "nodeIds", required = false) List<String> nodeIds,
            @RequestParam(value = "source", required = false) String source,
            @RequestHeader(value = "Accept", required = false) String accept) {
        // Mode 4 read-side pilot: source=entity is an explicit, reversible opt-in — everything
        // else about this endpoint (paging, sort, scope) is unchanged either way. Falls through to
        // the normal artifact-scan path (documentOverride null) whenever entity-ingest isn't
        // enabled/reachable or the reader finds no matching am_run, rather than erroring — this is
        // never the only path to table data.
        JsonNode entityDocument = null;
        if (useEntitySource(source)) {
            entityDocument = entityTablesReader.readTables(entityIngestConnection.database(), name);
        }
        return facet(name, ParserFacetSpecs.TABLES, "tables", limit, after, sort, q, types, cf,
                scope, procId, directProcId, nodeIds, accept, AnalysisQuery.Dataset.PLSQL_TABLES,
                entityDocument);
    }

    @GetMapping("/analyses/{name}/tables/page/summary")
    public ResponseEntity<JsonNode> getTablesPageSummary(
            @PathVariable("name") String name,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId) {
        return facetSummary(name, ParserFacetSpecs.TABLES, "tables", scope, procId,
                AnalysisQuery.Dataset.PLSQL_TABLES);
    }

    /**
     * Table rows for a list: name, schema, type, operations, access count. Nothing else.
     *
     * <p>The left panel renders a name, operation badges and a count, and used to get them from
     * {@code /tables} — a document whose size is driven by {@code usedBy[].operations}, one entry per
     * access line. The client then built one object per line and threw them all away. This returns
     * the five fields it actually reads.
     */
    @GetMapping("/analyses/{name}/tables/list")
    public ResponseEntity<JsonNode> getTablesList(@PathVariable("name") String name) {
        try {
            return ResponseEntity.ok(service.getTablesList(name));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/tables/summary")
    public ResponseEntity<JsonNode> getTablesSummary(@PathVariable("name") String name) {
        try {
            return ResponseEntity.ok(service.getTablesSummary(name));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/call-graph")
    public ResponseEntity<JsonNode> getCallGraph(@PathVariable("name") String name) {
        try {
            return ResponseEntity.ok(service.getCallGraph(name));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/source/{file}")
    public ResponseEntity<String> getSource(
            @PathVariable("name") String name,
            @PathVariable("file") String file) {
        try {
            return ResponseEntity.ok()
                    .contentType(MediaType.TEXT_PLAIN)
                    .body(service.getSource(name, file));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * The procedure list, paged.
     *
     * <p>Unpaged when no paging parameter is sent, so existing callers are unaffected — the
     * responder's legacy path returns the bare array it always did. A caller that sends
     * {@code limit} gets an envelope.
     */
    /**
     * The most-connected slice of the call graph, computed here rather than shipped whole.
     *
     * <h2>The mismatch this closes</h2>
     *
     * <p>FE-018 capped the graph <em>renderer</em> at 300 nodes, because an all-pairs repulsion step
     * over 200 frames is ~6.4 billion operations at 8,000 nodes. The <em>transfer</em> was never
     * capped: the client still fetched every edge and then threw almost all of them away. Measured
     * 847 edges today, but the load proof recorded <b>35,776 at 832 procedures</b> — linear in call
     * density, so roughly 344,000 edges and ~100 MB at the stated target, to draw 300 nodes.
     *
     * <p>Selection has to happen server-side because the criterion is degree, and degree is a
     * property of the whole graph — the client cannot pick the top 300 without first holding all of
     * it, which is the thing being avoided.
     *
     * <h2>Why not delete `/call-graph`</h2>
     *
     * <p>MID-004 asks for the old route to 410. It has two live consumers: this visualisation and
     * `export.js`, which needs every edge because an export of part of a graph is wrong. So the
     * route stays and gains a bounded sibling; bounding the export is FE-019's problem, not this
     * one.
     */
    @GetMapping("/analyses/{name}/call-graph/top")
    public ResponseEntity<JsonNode> getCallGraphTop(
            @PathVariable("name") String name,
            @RequestParam(value = "limit", required = false, defaultValue = "300") int limit) {
        if (limit < 1 || limit > 5000) {
            return FacetPageResponder.badRequest(mapper,
                    "limit must be between 1 and 5000, got " + limit);
        }
        JsonNode graph;
        try {
            graph = service.getCallGraph(name);
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(
                com.plsqlanalyzer.web.parser.service.CallGraphTopN.select(mapper, graph, limit));
    }

    /**
     * UI-029: gained {@code scope}/{@code procId}, matching the other four converted facets — see
     * {@link ParserScopeResolver} and {@link ParserFacetSpecs#PROCEDURES}'s now-real
     * {@code procedureIds} (a procedure row belongs to itself). Without this, a caller wanting
     * "just the exception-handler-bearing procedures under this node" (`exHandlers.js`, UI-029) had
     * no way to ask the server; the filter existed but could not be scoped, which is a smaller
     * version of the same bug this row's own siblings already fixed for tables/joins/cursors/
     * sequences.
     */
    @GetMapping("/analyses/{name}/procedures")
    public ResponseEntity<JsonNode> getProcedures(
            @PathVariable("name") String name,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "types", required = false) String types,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId,
            @RequestParam(value = "source", required = false) String source,
            @RequestHeader(value = "Accept", required = false) String accept) {
        // Mode 4 read-side, facet 5 (see EntityProceduresReader's own doc comment) — same explicit,
        // reversible opt-in as /tables/page's, /joins's, /cursors's and /sequences's own
        // source=entity.
        JsonNode entityDocument = null;
        if (useEntitySource(source)) {
            entityDocument = entityProceduresReader.readProcedures(entityIngestConnection.database(), name);
        }
        return facet(name, ParserFacetSpecs.PROCEDURES, null, limit, after, sort, q, types, cf,
                scope, procId, null, null, accept, AnalysisQuery.Dataset.PLSQL_PROCEDURES, entityDocument);
    }

    /**
     * UI-024 (§112) — `parser/js/export.js`'s bulk endpoint. Deliberately unpaged, same shape as
     * {@code /tables}/{@code /joins}/{@code /cursors} above: export means every procedure by
     * definition, the same "legitimate whole-corpus consumer" already established for those three,
     * not a grid this project's paging rules apply to. Returns exactly {@link
     * com.plsqlanalyzer.web.parser.service.AnalysisService#getProcedures} does — the same source
     * `/procedures` pages — so this is a new door onto existing data, not a new read path.
     *
     * <p>Added because `export.js`'s Call Flow/Complexity/Summary sheets need the *full* node shape
     * (`counts`, `linesOfCode`, `packageName`, …), which {@code ParserFacetSpecs.PROCEDURES}
     * deliberately projects down to 8 of 21 fields for the grid (MID-002's own doc comment) — the
     * grid's projection cannot serve this caller too without either widening it (defeating the
     * projection's own purpose) or adding a second endpoint, which is what this is.
     */
    @GetMapping("/analyses/{name}/procedures/export")
    public ResponseEntity<JsonNode> getProceduresForExport(@PathVariable("name") String name) {
        try {
            return ResponseEntity.ok(service.getProcedures(name));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * UI-030-BUG's fix — the parser-backend equivalent of UI-030c's {@code /reports} endpoint,
     * built against this backend's own `subtreeTables` shape rather than porting the client logic
     * that was silently broken against it. See {@link ParserProcedureReportsService}.
     */
    @GetMapping("/analyses/{name}/reports")
    public ResponseEntity<Object> getProcedureReports(@PathVariable("name") String name) {
        return procedureReportsService.getProcedureReports(name);
    }

    /**
     * UI-024 — server-side aggregation for `statements.js`'s statement-type summary, replacing its
     * two `PA.nodeIndex.all()` scans (unscoped + per-scope-change). See
     * {@link StatementsSummaryService}.
     */
    @GetMapping("/analyses/{name}/statements/summary")
    public ResponseEntity<Object> getStatementsSummary(
            @PathVariable("name") String name,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId) {
        // Mode 4 read-side investigated for "statements" and deliberately NOT wired here — see
        // EntityStatementsSummaryReader's own doc comment for the real, disqualifying gap found by
        // testing against real data: am_statement counts per-table-access rows, not per-statement
        // occurrences across every statement type (LOOP/IF/ASSIGNMENT/etc.), so it cannot reproduce
        // counts.statements at all, even for the operations it does share (SELECT: 403 real vs 2082
        // derived, same corpus). No source=entity branch added — building one would silently serve
        // wrong aggregate counts under an opt-in that every other facet's own opt-in has proven
        // trustworthy.
        return statementsSummaryService.getSummary(name, scope, procId);
    }

    /**
     * UI-024 — server-side aggregation for `detailHeader.js`'s seven scope-derived stat counts
     * (calls/triggers/cursors, full or scoped). See {@link DetailHeaderCountsService}.
     */
    @GetMapping("/analyses/{name}/detail-header/counts")
    public ResponseEntity<Object> getDetailHeaderCounts(
            @PathVariable("name") String name,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId) {
        return detailHeaderCountsService.getCounts(name, scope, procId);
    }

    /**
     * UI-024 — server-side filter for `tableDetail.js`'s "parsed source triggers" merge step,
     * replacing its `PA.nodeIndex.all()` scan for `TRIGGER`-type nodes matching one table. See
     * {@link TableTriggersService}.
     */
    @GetMapping("/analyses/{name}/tables/{tableName}/triggers")
    public ResponseEntity<Object> getTriggersForTable(
            @PathVariable("name") String name,
            @PathVariable("tableName") String tableName) {
        return tableTriggersService.getTriggersForTable(name, tableName);
    }

    /**
     * UI-024 — server-side computation for `complexity.js`'s per-procedure risk score. The caller
     * sends its own already-resolved weights/thresholds/algorithm (layered from server defaults
     * plus `localStorage` overrides client-side) — this endpoint has no config of its own to apply.
     * See {@link ComplexityScoringService}.
     */
    @GetMapping("/analyses/{name}/complexity/scores")
    public ResponseEntity<Object> getComplexityScores(
            @PathVariable("name") String name,
            @RequestParam(value = "wLoc", required = false, defaultValue = "0.3") double wLoc,
            @RequestParam(value = "wTables", required = false, defaultValue = "15") double wTables,
            @RequestParam(value = "wCallsOut", required = false, defaultValue = "10") double wCallsOut,
            @RequestParam(value = "wCursors", required = false, defaultValue = "8") double wCursors,
            @RequestParam(value = "wDynamicSql", required = false, defaultValue = "20") double wDynamicSql,
            @RequestParam(value = "wDepth", required = false, defaultValue = "5") double wDepth,
            @RequestParam(value = "thMedium", required = false, defaultValue = "50") double thMedium,
            @RequestParam(value = "thHigh", required = false, defaultValue = "150") double thHigh,
            @RequestParam(value = "algorithm", required = false, defaultValue = "linear") String algorithm,
            @RequestParam(value = "exponent", required = false, defaultValue = "1.3") double exponent) {
        ComplexityScoringService.Weights w = new ComplexityScoringService.Weights();
        w.loc = wLoc; w.tables = wTables; w.callsOut = wCallsOut; w.cursors = wCursors;
        w.dynamicSql = wDynamicSql; w.depth = wDepth;
        ComplexityScoringService.Thresholds th = new ComplexityScoringService.Thresholds();
        th.medium = thMedium; th.high = thHigh;
        // The default parameters map to views/complexity.json (backward-compatible); a grid
        // neighbour maps to its named variant; anything else maps to a hashed variant computed once
        // and cached (capped, LRU by mtime). All three share the same serve-or-compute shape.
        String view = com.plsqlanalyzer.web.parser.service.AnalysisViewStore.complexityViewName(
                algorithm, exponent, wLoc, wTables, wCallsOut, wCursors, wDynamicSql, wDepth,
                thMedium, thHigh);
        if (com.plsqlanalyzer.web.parser.service.AnalysisViewStore.COMPLEXITY.equals(view)) {
            return serveView(name, view,
                    () -> complexityScoringService.getScores(name, w, th, algorithm, exponent));
        }
        return serveComplexityVariant(name, view, w, th, algorithm, exponent);
    }

    /**
     * Serves a named-grid or hashed {@code /complexity/scores} variant from its sidecar when fresh;
     * otherwise computes it and caches the body as that variant (pruning the hashed variants to the
     * cap). The computed response is returned untouched, so a miss is today's response exactly.
     */
    private ResponseEntity<Object> serveComplexityVariant(
            String name, String view, ComplexityScoringService.Weights w,
            ComplexityScoringService.Thresholds th, String algorithm, double exponent) {
        if (views == null) return complexityScoringService.getScores(name, w, th, algorithm, exponent);
        JsonNode cached = views.read(name, view);
        if (cached != null) return ResponseEntity.ok(cached);
        ResponseEntity<Object> response = complexityScoringService.getScores(name, w, th, algorithm, exponent);
        if (response != null && response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
            views.writeComplexityVariant(name, view, mapper.valueToTree(response.getBody()));
        }
        return response;
    }

    /**
     * Serves a small precomputed view body when it exists, otherwise the legacy computation — whose
     * result is written back as the sidecar (lazy backfill) and returned unchanged. With no view
     * store wired the legacy response is returned untouched.
     */
    private ResponseEntity<Object> serveView(String name, String view,
                                             java.util.function.Supplier<ResponseEntity<Object>> legacy) {
        if (views == null) return legacy.get();
        JsonNode cached = views.read(name, view);
        if (cached != null) return ResponseEntity.ok(cached);
        ResponseEntity<Object> response = legacy.get();
        if (response != null && response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
            views.write(name, view, mapper.valueToTree(response.getBody()));
        }
        return response;
    }

    /**
     * UI-024 — server-side aggregation for `summary.js`'s scoped dashboard, replacing its
     * `PA.getScopedNodes()` scan. See {@link SummaryStatsService}.
     */
    @GetMapping("/analyses/{name}/summary/stats")
    public ResponseEntity<Object> getSummaryStats(
            @PathVariable("name") String name,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId,
            @RequestParam(value = "entrySchema", required = false) String entrySchema) {
        // The unscoped default is precomputed (views/summary-stats.json); any scope/procId/
        // entrySchema recomputes, exactly as before.
        if ((scope == null || scope.isBlank()) && (procId == null || procId.isBlank())
                && (entrySchema == null || entrySchema.isBlank())) {
            return serveView(name, com.plsqlanalyzer.web.parser.service.AnalysisViewStore.SUMMARY_STATS,
                    () -> summaryStatsService.getStats(name, scope, procId, entrySchema));
        }
        return summaryStatsService.getStats(name, scope, procId, entrySchema);
    }

    @GetMapping("/analyses/{name}/procedures/summary")
    public ResponseEntity<JsonNode> getProceduresSummary(
            @PathVariable("name") String name,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId) {
        return facetSummary(name, ParserFacetSpecs.PROCEDURES, null, scope, procId,
                AnalysisQuery.Dataset.PLSQL_PROCEDURES);
    }

    /*
     * ---------------------------------------------------------------------------------------
     * Converted facets (PLAN-08 §4.3 step 12).
     *
     * Each of the four accepts `limit` / `after` / `sort` / `q` / `types` / `scope`, and each
     * returns TODAY'S EXACT RESPONSE when none of them is sent. That is what lets the parser
     * screens convert one table at a time: an old caller sees no difference, and reverting one
     * table's JS needs no server change.
     *
     * `q`, `sort`, `types` and `scope` are all present for the same reason. Once only one page is
     * in memory, a search box, a column header, a filter pill or the Direct/Subtree selector left
     * in the browser would apply to that page alone and look like it worked. Paging without them
     * would be a regression, not a conversion.
     * ---------------------------------------------------------------------------------------
     */

    @GetMapping("/analyses/{name}/joins")
    public ResponseEntity<JsonNode> getJoins(
            @PathVariable("name") String name,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "types", required = false) String types,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId,
            @RequestParam(value = "directProcId", required = false) String directProcId,
            @RequestParam(value = "nodeIds", required = false) List<String> nodeIds,
            @RequestParam(value = "source", required = false) String source,
            @RequestHeader(value = "Accept", required = false) String accept) {
        // Mode 4 read-side, facet 2 (see EntityJoinsReader's own doc comment) — same explicit,
        // reversible opt-in as /tables/page's own source=entity.
        JsonNode entityDocument = null;
        if (useEntitySource(source)) {
            entityDocument = entityJoinsReader.readJoins(entityIngestConnection.database(), name);
        }
        return facet(name, ParserFacetSpecs.JOINS, null, limit, after, sort, q, types, cf, scope,
                procId, directProcId, nodeIds, accept, AnalysisQuery.Dataset.PLSQL_JOINS,
                entityDocument);
    }

    @GetMapping("/analyses/{name}/joins/summary")
    public ResponseEntity<JsonNode> getJoinsSummary(
            @PathVariable("name") String name,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId) {
        return facetSummary(name, ParserFacetSpecs.JOINS, null, scope, procId,
                AnalysisQuery.Dataset.PLSQL_JOINS);
    }

    /**
     * GET /analyses/{name}/joins/summary/queries
     *
     * UI-013 §60/§62/§64 — {@code parser/js/joinSummary.js}'s query-grouping view
     * ({@code _buildQueryItems}), moved server-side. Unlike the {@code PA.nodeIndex}-based
     * aggregations §62 found blocked, this screen's input (the raw {@code /joins} array) is not
     * guaranteed already resident client-side — it is reachable as its own lazy tab without the
     * plain Joins tab ever loading first — so folding it here, once, is a real payload reduction
     * rather than just a CPU-cost move. See {@link JoinQuerySummary}'s own class javadoc.
     */
    @GetMapping("/analyses/{name}/joins/summary/queries")
    public ResponseEntity<JsonNode> getJoinsQuerySummary(@PathVariable("name") String name) {
        try {
            // The same joins document /joins serves, read through the repository so it comes from
            // the precomputed view summary (falls back to the node scan when absent) rather than
            // re-scanning the node documents for this second view of the same data.
            JsonNode raw = repository.document(AnalysisQuery.of(AnalysisQuery.Dataset.PLSQL_JOINS, name));
            JoinQuerySummary.Config cfg = new JoinQuerySummary.Config(
                    joinComplexityConfig.getBaseScore(),
                    joinComplexityConfig.getOuterJoinPenalty(),
                    joinComplexityConfig.getCrossFullJoinPenalty(),
                    joinComplexityConfig.getMultiPredBonus(),
                    joinComplexityConfig.getHighPredBonus(),
                    joinComplexityConfig.getNoPredPenalty(),
                    joinComplexityConfig.getMultiPredThreshold(),
                    joinComplexityConfig.getHighPredThreshold(),
                    joinComplexityConfig.getThresholdMedium(),
                    joinComplexityConfig.getThresholdHigh());
            return ResponseEntity.ok(JoinQuerySummary.build(mapper, raw, cfg));
        } catch (java.io.IOException e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/cursors")
    public ResponseEntity<JsonNode> getCursors(
            @PathVariable("name") String name,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "types", required = false) String types,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId,
            @RequestParam(value = "directProcId", required = false) String directProcId,
            @RequestParam(value = "nodeIds", required = false) List<String> nodeIds,
            @RequestParam(value = "source", required = false) String source,
            @RequestHeader(value = "Accept", required = false) String accept) {
        // Mode 4 read-side, facet 3 (see EntityCursorsReader's own doc comment) — same explicit,
        // reversible opt-in as /tables/page's and /joins's own source=entity.
        JsonNode entityDocument = null;
        if (useEntitySource(source)) {
            entityDocument = entityCursorsReader.readCursors(entityIngestConnection.database(), name);
        }
        return facet(name, ParserFacetSpecs.CURSORS, null, limit, after, sort, q, types, cf, scope,
                procId, directProcId, nodeIds, accept, AnalysisQuery.Dataset.PLSQL_CURSORS,
                entityDocument);
    }

    @GetMapping("/analyses/{name}/cursors/summary")
    public ResponseEntity<JsonNode> getCursorsSummary(
            @PathVariable("name") String name,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId) {
        return facetSummary(name, ParserFacetSpecs.CURSORS, null, scope, procId,
                AnalysisQuery.Dataset.PLSQL_CURSORS);
    }

    /**
     * MID-021 (predicates.js's own paged-conversion project) — the last of the four screens §106's
     * own audit named (`tableOps.js`/`joins.js`/`cursors.js` done in §107-110, `predicates.js` left
     * open pending §138's data-wiring fix). Not routed through the shared {@code facet()} helper:
     * unlike {@code JOINS}/{@code CURSORS}/{@code TABLES}, this row's document is not the raw
     * dataset itself but {@link com.plsqlanalyzer.web.controller.PlsqlPredicatesFacet#build}'s
     * aggregation of it — the same reason {@code AnalyzerController.getPredicatesPage} (the
     * live-crawl sibling) isn't routed through its own shared {@code pagedFacet} helper either.
     * {@code ParserPredicatesAdapter} bridges the shape gap {@code PlsqlPredicatesFacet.build}
     * itself doesn't know about: the parser's raw {@code usedBy[]} tables document adapted into the
     * {@code accessDetails[].whereFilters[]} shape the live-crawl backend already produces natively.
     */
    @GetMapping("/analyses/{name}/predicates")
    public ResponseEntity<JsonNode> getPredicates(
            @PathVariable("name") String name,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "types", required = false) String types,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestParam(value = "directProcId", required = false) String directProcId,
            @RequestParam(value = "nodeIds", required = false) List<String> nodeIds,
            @RequestParam(value = "source", required = false) String source,
            @RequestHeader(value = "Accept", required = false) String accept) {
        // Mode 4 read-side, facet 6 (see EntityPredicatesReader's own doc comment) — same explicit,
        // reversible opt-in as the other five facets' own source=entity.
        JsonNode entityDocument = null;
        if (useEntitySource(source)) {
            entityDocument = entityPredicatesReader.readPredicates(entityIngestConnection.database(), name);
        }
        if (entityDocument != null) {
            try {
                JsonNode document = entityDocument;
                if (directProcId != null && !directProcId.isBlank()) {
                    document = com.plsqlanalyzer.web.controller.PlsqlScopeFilter.directPredicates(
                            mapper, document, directProcId);
                } else if (nodeIds != null && !nodeIds.isEmpty()) {
                    document = com.plsqlanalyzer.web.controller.PlsqlScopeFilter.subtreePredicates(
                            mapper, document, new java.util.HashSet<>(nodeIds));
                }
                return facets.respond(document, null, ParserFacetSpecs.PREDICATES,
                        new FacetPageResponder.Params(limit, after, sort, q, types, cf, accept), null);
            } catch (IllegalArgumentException | Cursor.InvalidCursorException e) {
                return FacetPageResponder.badRequest(mapper, e.getMessage());
            }
        }
        JsonNode rawTables;
        try {
            rawTables = repository.document(AnalysisQuery.of(AnalysisQuery.Dataset.PLSQL_TABLES, name));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
        try {
            JsonNode adapted = ParserPredicatesAdapter.adaptTables(mapper, rawTables);
            JsonNode document = com.plsqlanalyzer.web.controller.PlsqlPredicatesFacet.build(mapper, adapted);
            if (directProcId != null && !directProcId.isBlank()) {
                document = com.plsqlanalyzer.web.controller.PlsqlScopeFilter.directPredicates(
                        mapper, document, directProcId);
            } else if (nodeIds != null && !nodeIds.isEmpty()) {
                document = com.plsqlanalyzer.web.controller.PlsqlScopeFilter.subtreePredicates(
                        mapper, document, new java.util.HashSet<>(nodeIds));
            }
            return facets.respond(document, null, ParserFacetSpecs.PREDICATES,
                    new FacetPageResponder.Params(limit, after, sort, q, types, cf, accept), null);
        } catch (IllegalArgumentException | Cursor.InvalidCursorException e) {
            return FacetPageResponder.badRequest(mapper, e.getMessage());
        }
    }

    @GetMapping("/analyses/{name}/sequences")
    public ResponseEntity<JsonNode> getSequences(
            @PathVariable("name") String name,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "types", required = false) String types,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId,
            @RequestParam(value = "source", required = false) String source,
            @RequestHeader(value = "Accept", required = false) String accept) {
        // Mode 4 read-side, facet 4 (see EntitySequencesReader's own doc comment) — same explicit,
        // reversible opt-in as /tables/page's, /joins's and /cursors's own source=entity.
        JsonNode entityDocument = null;
        if (useEntitySource(source)) {
            entityDocument = entitySequencesReader.readSequences(entityIngestConnection.database(), name);
        }
        return facet(name, ParserFacetSpecs.SEQUENCES, null, limit, after, sort, q, types, cf, scope,
                procId, null, null, accept, AnalysisQuery.Dataset.PLSQL_SEQUENCES, entityDocument);
    }

    @GetMapping("/analyses/{name}/sequences/summary")
    public ResponseEntity<JsonNode> getSequencesSummary(
            @PathVariable("name") String name,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId) {
        return facetSummary(name, ParserFacetSpecs.SEQUENCES, null, scope, procId,
                AnalysisQuery.Dataset.PLSQL_SEQUENCES);
    }

    @GetMapping("/analyses/{name}/dynamic-sql")
    public ResponseEntity<JsonNode> getDynamicSql(
            @PathVariable("name") String name,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "types", required = false) String types,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId,
            @RequestParam(value = "source", required = false) String source,
            @RequestHeader(value = "Accept", required = false) String accept) {
        // Mode 4 read-side, the final facet (see EntityDynamicSqlReader's own doc comment) — same
        // explicit, reversible opt-in as the other six facets' own source=entity.
        JsonNode entityDocument = null;
        if (useEntitySource(source)) {
            entityDocument = entityDynamicSqlReader.readDynamicSql(entityIngestConnection.database(), name);
        }
        return facet(name, ParserFacetSpecs.DYNAMIC_SQL, null, limit, after, sort, q, types, cf, scope,
                procId, null, null, accept, AnalysisQuery.Dataset.PLSQL_DYNAMIC_SQL, entityDocument);
    }

    @GetMapping("/analyses/{name}/dynamic-sql/summary")
    public ResponseEntity<JsonNode> getDynamicSqlSummary(
            @PathVariable("name") String name,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId) {
        return facetSummary(name, ParserFacetSpecs.DYNAMIC_SQL, null, scope, procId,
                AnalysisQuery.Dataset.PLSQL_DYNAMIC_SQL);
    }

    /* ═══════════════════ estate facets: views / triggers / globals ═══════════════════
     *
     * The three index endpoints are paged facets through the same {@link #facet} helper the seven
     * node-scan facets use — so an analysis with no views/triggers/globals returns an empty page,
     * exactly like an analysis with no sequences. The detail / chain / used-by sub-resources are
     * plain documents served from their per-object sidecar; a name the analysis does not have is a
     * 404, which is the "no facet" answer for a named sub-resource.
     */

    /** {@code /views} — every view/MV with its columns, definition and base tables. */
    @GetMapping("/analyses/{name}/views")
    public ResponseEntity<JsonNode> getViews(
            @PathVariable("name") String name,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "types", required = false) String types,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId,
            @RequestHeader(value = "Accept", required = false) String accept) {
        return facet(name, ParserFacetSpecs.VIEWS, null, limit, after, sort, q, types, cf, scope,
                procId, accept, AnalysisQuery.Dataset.PLSQL_VIEWS);
    }

    @GetMapping("/analyses/{name}/views/{view}")
    public ResponseEntity<JsonNode> getViewDetail(
            @PathVariable("name") String name, @PathVariable("view") String view) {
        try {
            return okOrNotFound(service.getViewDetail(name, view));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    /** The full view→view→…→base-table chain. */
    @GetMapping("/analyses/{name}/views/{view}/chain")
    public ResponseEntity<JsonNode> getViewChain(
            @PathVariable("name") String name, @PathVariable("view") String view) {
        try {
            return okOrNotFound(service.getViewChain(name, view));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/views/{view}/used-by")
    public ResponseEntity<JsonNode> getViewUsedBy(
            @PathVariable("name") String name, @PathVariable("view") String view) {
        try {
            return okOrNotFound(service.getViewUsedBy(name, view));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    /** {@code /triggers} — every trigger with its binding, timing, events, level and body. */
    @GetMapping("/analyses/{name}/triggers")
    public ResponseEntity<JsonNode> getTriggers(
            @PathVariable("name") String name,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "types", required = false) String types,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId,
            @RequestHeader(value = "Accept", required = false) String accept) {
        return facet(name, ParserFacetSpecs.TRIGGERS, null, limit, after, sort, q, types, cf, scope,
                procId, accept, AnalysisQuery.Dataset.PLSQL_TRIGGERS);
    }

    @GetMapping("/analyses/{name}/triggers/{trigger}")
    public ResponseEntity<JsonNode> getTriggerDetail(
            @PathVariable("name") String name, @PathVariable("trigger") String trigger) {
        try {
            return okOrNotFound(service.getTriggerDetail(name, trigger));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    /** The trigger→touched-table→other-triggers chain. */
    @GetMapping("/analyses/{name}/triggers/{trigger}/chain")
    public ResponseEntity<JsonNode> getTriggerChain(
            @PathVariable("name") String name, @PathVariable("trigger") String trigger) {
        try {
            return okOrNotFound(service.getTriggerChain(name, trigger));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/triggers/{trigger}/used-by")
    public ResponseEntity<JsonNode> getTriggerUsedBy(
            @PathVariable("name") String name, @PathVariable("trigger") String trigger) {
        try {
            return okOrNotFound(service.getTriggerUsedBy(name, trigger));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    /** {@code /globals} — package global variables, grouped by package. */
    @GetMapping("/analyses/{name}/globals")
    public ResponseEntity<JsonNode> getGlobals(
            @PathVariable("name") String name,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "types", required = false) String types,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId,
            @RequestHeader(value = "Accept", required = false) String accept) {
        return facet(name, ParserFacetSpecs.GLOBALS, null, limit, after, sort, q, types, cf, scope,
                procId, accept, AnalysisQuery.Dataset.PLSQL_GLOBALS);
    }

    @GetMapping("/analyses/{name}/globals/{pkg}")
    public ResponseEntity<JsonNode> getGlobalDetail(
            @PathVariable("name") String name, @PathVariable("pkg") String pkg) {
        try {
            return okOrNotFound(service.getGlobalDetail(name, pkg));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    /** Where one global variable is read/written across the estate. */
    @GetMapping("/analyses/{name}/globals/{variable}/used-by")
    public ResponseEntity<JsonNode> getGlobalUsedBy(
            @PathVariable("name") String name, @PathVariable("variable") String variable) {
        try {
            return okOrNotFound(service.getGlobalUsedBy(name, variable));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    /** A named sub-resource's document, or 404 when the analysis has no such object. */
    private static ResponseEntity<JsonNode> okOrNotFound(JsonNode doc) {
        return doc == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(doc);
    }

    /**
     * UI-045/§256 — the real, cross-facet Global Search results grid, replacing {@code
     * PA.globalSearch}'s old same-tab-only DOM filter (`plsql/js/app.js`).
     *
     * <p>Reuses each of the 7 real facets' OWN {@code searchFn}/id logic from {@link
     * ParserFacetSpecs} — {@code TABLES.searchText()}, {@code PROCEDURES.searchText()}, etc. — via
     * {@link #buildSearchRows}, rather than a new matching rule per facet. The merged, normalized
     * rows are then paged through the EXACT SAME {@link FacetPageResponder}/{@link
     * com.plsqlanalyzer.web.page.JsonFacetPager} cursor convention every other facet endpoint in
     * this controller already uses — no new pagination scheme.
     *
     * <p>{@code q} is required and non-blank: a bare "show everything" cross-facet dump is not this
     * endpoint's job, and skipping the per-facet {@code searchFn} filter would mean scanning and
     * normalizing every row this analysis has, every one of the 7 facets, on every keystroke of an
     * empty box.
     *
     * <p>Path is {@code global-search}, not {@code search}: {@code PlsqlSearchController} already
     * owns {@code /analyses/{name}/search} (and {@code /search/summary}) for a pre-existing, unrelated
     * feature — §B13's cross-analysis STATEMENT search (a query language like {@code op:UPDATE
     * table:X col:Y}, matching individual SQL statements, not entity names). Reusing {@code /search}
     * here would either silently shadow that endpoint or throw Spring's "Ambiguous mapping" at boot
     * (confirmed: it does, verified during this feature's own first restart) — {@code global-search}
     * avoids the collision without touching that controller at all.
     */
    @GetMapping("/analyses/{name}/global-search")
    public ResponseEntity<JsonNode> getSearch(
            @PathVariable("name") String name,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestHeader(value = "Accept", required = false) String accept) {
        if (q == null || q.isBlank()) {
            return FacetPageResponder.badRequest(mapper, "q is required");
        }
        try {
            com.fasterxml.jackson.databind.node.ArrayNode rows = buildSearchRows(name, q);
            // q is deliberately NOT forwarded here — buildSearchRows already applied it once, per
            // facet, using that facet's own searchFn; SEARCH.searchText() is null by construction
            // (see ParserFacetSpecs.SEARCH's own doc comment) so a second pass would either no-op
            // or throw, neither of which is what re-sending q would look like it does.
            return facets.respond(rows, null, ParserFacetSpecs.SEARCH,
                    new FacetPageResponder.Params(limit, after, sort, null, null, cf, accept), null);
        } catch (IllegalArgumentException | Cursor.InvalidCursorException e) {
            return FacetPageResponder.badRequest(mapper, e.getMessage());
        }
    }

    /**
     * UI-046/§256 — the real, in-analysis {@code Type}/{@code Schema} value sets for the search
     * grid's column-filter popovers, computed over the FULL (unpaged) match set for {@code q} —
     * mirrors every other facet's existing {@code /summary} → {@code columnValues} convention
     * (see {@link FacetPageResponder#summary}), rather than a hardcoded list of the 7 facet names.
     */
    @GetMapping("/analyses/{name}/global-search/summary")
    public ResponseEntity<JsonNode> getSearchSummary(
            @PathVariable("name") String name,
            @RequestParam(value = "q", required = false) String q) {
        if (q == null || q.isBlank()) {
            return FacetPageResponder.badRequest(mapper, "q is required");
        }
        com.fasterxml.jackson.databind.node.ArrayNode rows = buildSearchRows(name, q);
        return facets.summary(rows, null, ParserFacetSpecs.SEARCH, null);
    }

    /**
     * Normalizes every one of the 7 real facets' matches for {@code q} into the common
     * {@code {type, name, schema, matchedField, _idx, raw}} shape §256's plan (§255) called for,
     * re-using each facet's own already-declared id/searchText functions from {@link
     * ParserFacetSpecs} rather than a new matching rule. A facet whose dataset is missing/unreadable
     * for this analysis is skipped, not fatal — a search should still return the other 6 facets'
     * hits rather than 404 the whole grid because one artifact file happens to be absent.
     */
    private com.fasterxml.jackson.databind.node.ArrayNode buildSearchRows(String name, String q) {
        String needle = q.toLowerCase(java.util.Locale.ROOT);
        com.fasterxml.jackson.databind.node.ArrayNode out = mapper.createArrayNode();
        int[] idx = {0};
        // The unfiltered source rows: the save-time sidecar when fresh, else the 7-artifact scan
        // (whose result is written back as the sidecar). Everything below — the per-facet filter
        // and the normalization — is unchanged, so the response is byte-identical either way.
        JsonNode facets = searchFacets(name);

        addMatches(out, idx, needle, "TABLE", ParserFacetSpecs.TABLES,
                facetArray(facets, ParserSearchFacets.TABLES),
                row -> row.path("name").asText(""), row -> row.path("schema").asText(""));
        addMatches(out, idx, needle, "PROCEDURE", ParserFacetSpecs.PROCEDURES,
                facetArray(facets, ParserSearchFacets.PROCEDURES),
                row -> row.path("name").asText(""), row -> row.path("schema").asText(""));
        addMatches(out, idx, needle, "SEQUENCE", ParserFacetSpecs.SEQUENCES,
                facetArray(facets, ParserSearchFacets.SEQUENCES),
                row -> row.path("sequenceName").asText(""), row -> row.path("sequenceSchema").asText(""));
        addMatches(out, idx, needle, "CURSOR", ParserFacetSpecs.CURSORS,
                facetArray(facets, ParserSearchFacets.CURSORS),
                row -> row.path("cursorName").asText(""), row -> "");
        addMatches(out, idx, needle, "JOIN", ParserFacetSpecs.JOINS,
                facetArray(facets, ParserSearchFacets.JOINS),
                row -> row.path("leftTable").asText("") + " ⋈ " + row.path("rightTable").asText(""),
                row -> "");
        addMatches(out, idx, needle, "DYNAMIC_SQL", ParserFacetSpecs.DYNAMIC_SQL,
                facetArray(facets, ParserSearchFacets.DYNAMIC_SQL),
                row -> row.path("procedureName").asText(""), row -> "");
        addMatches(out, idx, needle, "PREDICATE", ParserFacetSpecs.PREDICATES,
                facetArray(facets, ParserSearchFacets.PREDICATES),
                row -> row.path("tableName").asText(""), row -> row.path("schemaName").asText(""));
        return out;
    }

    /**
     * The raw search-facet map from {@code views/search.json} when fresh, otherwise the 7-artifact
     * scan — whose result is written back as the sidecar (lazy backfill). Absent sidecars, or no
     * view store, fall straight through to today's scan.
     */
    private JsonNode searchFacets(String name) {
        if (views != null) {
            JsonNode cached = views.readSearch(name);
            if (cached != null) return cached;
        }
        com.fasterxml.jackson.databind.node.ObjectNode built =
                ParserSearchFacets.build(repository, mapper, name);
        if (views != null) views.writeSearch(name, built, true);
        return built;
    }

    /** One facet's array out of the search-facet map, or {@code null} to skip the facet. */
    private static com.fasterxml.jackson.databind.node.ArrayNode facetArray(JsonNode facets, String key) {
        JsonNode node = facets == null ? null : facets.get(key);
        return (node != null && node.isArray())
                ? (com.fasterxml.jackson.databind.node.ArrayNode) node : null;
    }

    /**
     * Filters {@code rows} by {@code spec}'s own {@code searchText} (never a new matching rule) and
     * appends one normalized {@code {type, name, schema, matchedField, _idx, raw}} object per hit.
     */
    private void addMatches(com.fasterxml.jackson.databind.node.ArrayNode out, int[] idx,
                            String needle, String type, FacetSpec spec,
                            com.fasterxml.jackson.databind.node.ArrayNode rows,
                            java.util.function.Function<JsonNode, String> nameFn,
                            java.util.function.Function<JsonNode, String> schemaFn) {
        if (rows == null) return;
        for (JsonNode row : rows) {
            if (row == null || !row.isObject()) continue;
            String text = spec.searchText().apply(row);
            if (text == null || !text.contains(needle)) continue;
            com.fasterxml.jackson.databind.node.ObjectNode norm = mapper.createObjectNode();
            norm.put("type", type);
            String rowName = nameFn.apply(row);
            String rowSchema = schemaFn.apply(row);
            norm.put("name", rowName);
            norm.put("schema", rowSchema);
            norm.put("matchedField", rowName.toLowerCase(java.util.Locale.ROOT).contains(needle)
                    ? "name" : "content");
            norm.put("_idx", idx[0]++);
            norm.set("raw", row);
            out.add(norm);
        }
    }

    /**
     * @param dataset which facet to query. Replaces the {@code FacetReader} lambda these four
     *                endpoints used to pass: the reader choice is no longer a per-call-site
     *                decision but a property of the dataset, stated once in
     *                {@code ArtifactScanRepository#document}.
     */
    private ResponseEntity<JsonNode> facet(String name, FacetSpec spec, String arrayField,
                                           Integer limit, String after, String sort, String q,
                                           String types, List<String> cf, String scope,
                                           String procId, String accept,
                                           AnalysisQuery.Dataset dataset) {
        return facet(name, spec, arrayField, limit, after, sort, q, types, cf, scope, procId,
                null, null, accept, dataset);
    }

    /**
     * MID-021 (§107) — {@code directProcId}/{@code nodeIds} are `plsql/js/joins.js`/`cursors.js`'s
     * own "all"/"scoped" pair (the {@link com.plsqlanalyzer.web.controller.PlsqlScopeFilter}
     * semantics already ported for the live-crawl backend's own paged siblings), deliberately kept
     * separate from this overload's own {@code scope}/{@code procId} — the parser app's own
     * Direct/Subtree/SubFlow/Full selector, a different screen, a different scope model. Two shapes
     * are handled: {@code JOINS}/{@code CURSORS} (arrayField {@code null} — {@code document} is
     * itself the row array, already {@code accessDetails[]}-shaped) via {@link
     * com.plsqlanalyzer.web.controller.PlsqlScopeFilter#direct}/{@code subtree}, and {@code TABLES}
     * (arrayField {@code "tables"} — {@code document} is the {@code {tables:[...], ...}} wrapper,
     * rows shaped {@code usedBy[]}) via §110's {@code directTables}/{@code subtreeTables} instead.
     */
    private ResponseEntity<JsonNode> facet(String name, FacetSpec spec, String arrayField,
                                           Integer limit, String after, String sort, String q,
                                           String types, List<String> cf, String scope,
                                           String procId, String directProcId, List<String> nodeIds,
                                           String accept, AnalysisQuery.Dataset dataset) {
        return facet(name, spec, arrayField, limit, after, sort, q, types, cf, scope, procId,
                directProcId, nodeIds, accept, dataset, null);
    }

    /**
     * Mode 4 read-side pilot (one facet only, {@code tables} — see {@code EntityTablesReader}'s own
     * doc comment): {@code documentOverride}, when supplied, replaces the artifact-scan read
     * entirely — everything downstream (paging, sort, scope trimming) runs unchanged on top of it,
     * since it is built to the exact same {@code {tables:[...]}} shape. {@code null} (every
     * existing call site) is byte-for-byte today's behaviour.
     */
    private ResponseEntity<JsonNode> facet(String name, FacetSpec spec, String arrayField,
                                           Integer limit, String after, String sort, String q,
                                           String types, List<String> cf, String scope,
                                           String procId, String directProcId, List<String> nodeIds,
                                           String accept, AnalysisQuery.Dataset dataset,
                                           JsonNode documentOverride) {
        // Task #33: canonicalise the identity before it reaches the repository. The artifact-scan
        // repository already resolves through AnalysisPaths (which canonicalises), but the Mongo
        // repository matches the "analysis" field exactly, so the query must carry the canonical
        // spelling for OracleDemo / ORACLEDEMO to hit the same documents.
        name = AnalysisPaths.canonical(name);
        JsonNode document;
        if (documentOverride != null) {
            document = documentOverride;
        } else {
            try {
                document = repository.document(AnalysisQuery.of(dataset, name));
            } catch (Exception e) {
                return ResponseEntity.notFound().build();
            }
        }
        try {
            if (directProcId != null && !directProcId.isBlank()) {
                document = arrayField != null
                        ? com.plsqlanalyzer.web.controller.PlsqlScopeFilter.directTables(mapper, document, arrayField, directProcId)
                        : com.plsqlanalyzer.web.controller.PlsqlScopeFilter.direct(mapper, document, directProcId);
                return facets.respond(document, arrayField, spec,
                        new FacetPageResponder.Params(limit, after, sort, q, types, cf, accept), null);
            }
            if (nodeIds != null && !nodeIds.isEmpty()) {
                document = arrayField != null
                        ? com.plsqlanalyzer.web.controller.PlsqlScopeFilter.subtreeTables(mapper, document, arrayField, new java.util.HashSet<>(nodeIds))
                        : com.plsqlanalyzer.web.controller.PlsqlScopeFilter.subtree(mapper, document, new java.util.HashSet<>(nodeIds));
                return facets.respond(document, arrayField, spec,
                        new FacetPageResponder.Params(limit, after, sort, q, types, cf, accept), null);
            }
            Predicate<JsonNode> scopeFilter =
                    scopes.predicate(name, scope, procId, spec.procedureIds());
            return facets.respond(document, arrayField, spec,
                    new FacetPageResponder.Params(limit, after, sort, q, types, cf, accept),
                    scopeFilter);
        } catch (IllegalArgumentException | Cursor.InvalidCursorException e) {
            // An unknown sort or scope name, an out-of-range limit, or a stale cursor. Answered
            // with a reason, never with a page that happens to be empty.
            return FacetPageResponder.badRequest(mapper, e.getMessage());
        } catch (IllegalStateException e) {
            return FacetPageResponder.badRequest(mapper, e.getMessage());
        }
    }

    private ResponseEntity<JsonNode> facetSummary(String name, FacetSpec spec, String arrayField,
                                                  String scope, String procId,
                                                  AnalysisQuery.Dataset dataset) {
        name = AnalysisPaths.canonical(name);   // task #33 — see facet(...)
        JsonNode document;
        try {
            document = repository.document(AnalysisQuery.of(dataset, name));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
        try {
            return facets.summary(document, arrayField, spec,
                    scopes.predicate(name, scope, procId, spec.procedureIds()));
        } catch (IllegalArgumentException | IllegalStateException e) {
            return FacetPageResponder.badRequest(mapper, e.getMessage());
        }
    }

    /**
     * UI-019 (§142) — {@code maxDepth}, when present, fetches only that many levels rooted at
     * {@code nodeId} instead of the whole (node-count-budgeted) tree; a cut node carries {@code
     * moreChildren}/{@code childCount} so the client can ask again, rooted at that node, for the
     * next slice. Omitting the parameter preserves every existing caller's behaviour exactly —
     * this is additive, not a breaking change to the endpoint's own default contract.
     */
    @GetMapping("/analyses/{name}/call-tree/{nodeId}")
    public ResponseEntity<JsonNode> getCallTree(
            @PathVariable("name") String name,
            @PathVariable("nodeId") String nodeId,
            @RequestParam(value = "maxDepth", required = false) Integer maxDepth) {
        try {
            return ResponseEntity.ok(maxDepth != null
                    ? service.getCallTree(name, nodeId, maxDepth)
                    : service.getCallTree(name, nodeId));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/call-tree/{nodeId}/callers")
    public ResponseEntity<JsonNode> getCallers(
            @PathVariable("name") String name,
            @PathVariable("nodeId") String nodeId) {
        try {
            return ResponseEntity.ok(service.getCallers(name, nodeId));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/resolver/{type}")
    public ResponseEntity<JsonNode> getResolver(
            @PathVariable("name") String name,
            @PathVariable("type") String type) {
        try {
            return ResponseEntity.ok(service.getResolver(name, type));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * {@code /resolver/{type}}, paged — MID-005.
     *
     * <p>No client calls the unpaged route above (checked across the whole static tree), so this
     * follows the lightweight {@code /analyses} pattern rather than the repository-backed one
     * {@code /tables/page} uses: read the small resolver document directly and hand a
     * {@code List<JsonNode>} straight to {@code FacetPageResponder}. See {@link ResolverFacet} for
     * why the heavier path is not warranted here.
     */
    @GetMapping("/analyses/{name}/resolver/{type}/page")
    public ResponseEntity<JsonNode> getResolverPage(
            @PathVariable("name") String name,
            @PathVariable("type") String type,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestHeader(value = "Accept", required = false) String accept) {
        JsonNode document;
        try {
            document = service.getResolver(name, type);
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
        List<JsonNode> rows = "q4".equals(type)
                ? ResolverFacet.rowsFromMap(mapper, document)
                : ResolverFacet.rowsFromArray(document);
        try {
            return facets.respondStreaming(rows.iterator(), ResolverFacet.SPEC,
                    new FacetPageResponder.Params(limit, after, sort, q, null, null, accept),
                    null);
        } catch (IllegalArgumentException | Cursor.InvalidCursorException e) {
            return FacetPageResponder.badRequest(mapper, e.getMessage());
        }
    }

    /**
     * UI-042 — the shared "download everything as one zip" endpoint behind `export.js`'s new
     * bulk-export mode (UI-040). Pure orchestration, per §249's own instruction: every facet is
     * fetched through the exact same {@link AnalysisService} methods the already-existing facet
     * endpoints call ({@link #getTables}/{@code /joins}/{@code /call-graph}/{@code /procedures/
     * export}/{@code /index} equivalents), and every source file through the exact same {@link
     * AnalysisService#getSource} the existing {@code /source/{file}} endpoint calls — no facet or
     * source data is re-derived here.
     *
     * <p>Streamed directly to the response via {@link ZipOutputStream}, entry by entry, rather
     * than buffered in memory first — the doc comment on {@code ArtifactStore} itself ("a single
     * analysis artifact can be far larger than heap") is exactly the risk a whole-zip-in-memory
     * approach would reintroduce, and a real analysis's source set (52 files for
     * {@code ERP_USP_RUN_CYCLE}, potentially far more for others) makes that a real, not
     * theoretical, concern.
     *
     * <p>Request body: {@code {"facets": ["tables","joins","callGraph","procedures","index"],
     * "sources": true}}. Both fields optional/independent — {@code sources: true} with an empty
     * {@code facets} list is exactly UI-040's "source code only" mode; a non-empty {@code facets}
     * list with {@code sources} omitted/false is exactly its "everything except source" mode; both
     * together is its "Everything" mode. An unknown facet name is silently skipped (not a 400) so a
     * future client sending a facet this endpoint hasn't been taught yet degrades to "that one
     * entry missing," not a failed download of everything else requested alongside it.
     *
     * <p>A per-facet/per-file try/catch means one missing or unreadable artifact (e.g. a facet the
     * analysis never produced) drops just that one zip entry rather than failing the whole
     * download — the same "skip on unreadable" tolerance {@link AnalysisService#listAnalyses}
     * already uses for a different per-item failure.
     */
    @PostMapping("/analyses/{name}/export/bundle")
    public void exportBundle(
            @PathVariable("name") String name,
            @RequestBody(required = false) ExportBundleRequest body,
            jakarta.servlet.http.HttpServletResponse response) throws IOException {
        ExportBundleRequest req = body != null ? body : new ExportBundleRequest(null, null);
        java.util.Set<String> requestedFacets = req.facets() == null
                ? java.util.Set.of()
                : new java.util.LinkedHashSet<>(req.facets());
        boolean includeSources = Boolean.TRUE.equals(req.sources());

        String safeName = name.replace(AnalysisPaths.SEP, '_');
        response.setContentType("application/zip");
        response.setHeader("Content-Disposition",
                "attachment; filename=\"" + safeName + "-export.zip\"");

        try (java.util.zip.ZipOutputStream zos =
                     new java.util.zip.ZipOutputStream(response.getOutputStream())) {
            if (requestedFacets.contains("tables")) {
                writeJsonEntry(zos, "facets/tables.json", () -> service.getTables(name));
            }
            if (requestedFacets.contains("joins")) {
                writeJsonEntry(zos, "facets/joins.json", () -> service.getJoins(name));
            }
            if (requestedFacets.contains("callGraph")) {
                writeJsonEntry(zos, "facets/callGraph.json", () -> service.getCallGraph(name));
            }
            if (requestedFacets.contains("procedures")) {
                writeJsonEntry(zos, "facets/procedures.json", () -> service.getProcedures(name));
            }
            if (requestedFacets.contains("index")) {
                writeJsonEntry(zos, "facets/index.json", () -> service.getIndexWithoutNodes(name));
            }
            if (includeSources) {
                List<String> sourceFiles;
                try {
                    sourceFiles = service.listSourceFiles(name);
                } catch (IOException e) {
                    sourceFiles = List.of();
                }
                for (String file : sourceFiles) {
                    try {
                        String content = service.getSource(name, file);
                        zos.putNextEntry(new java.util.zip.ZipEntry("sources/" + file));
                        zos.write(content.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                        zos.closeEntry();
                    } catch (IOException ignored) {
                        // One unreadable source file must not fail the whole bundle — see class doc.
                    }
                }
            }
            zos.finish();
        }
    }

    /** One JSON facet as one zip entry; skipped (not failed) if the facet doesn't exist for this
     *  analysis, same tolerance every {@code catch (Exception e) -> notFound()} facet endpoint
     *  above already applies to a single-facet request. */
    private void writeJsonEntry(java.util.zip.ZipOutputStream zos, String entryName,
                                 JsonSupplier supplier) {
        try {
            JsonNode node = supplier.get();
            byte[] bytes = mapper.writeValueAsBytes(node);
            zos.putNextEntry(new java.util.zip.ZipEntry(entryName));
            zos.write(bytes);
            zos.closeEntry();
        } catch (Exception ignored) {
            // Facet absent/unreadable for this analysis — omit the entry, keep the rest of the zip.
        }
    }

    @FunctionalInterface
    private interface JsonSupplier {
        JsonNode get() throws IOException;
    }

    /** UI-042 request body: both fields optional/independent — see {@link #exportBundle}. */
    public record ExportBundleRequest(List<String> facets, Boolean sources) {}
}
