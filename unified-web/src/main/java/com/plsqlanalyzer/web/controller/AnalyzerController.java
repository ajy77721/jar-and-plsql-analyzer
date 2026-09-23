package com.plsqlanalyzer.web.controller;

import com.plsqlanalyzer.analyzer.model.CursorOperationSummary;
import com.plsqlanalyzer.analyzer.model.JoinOperationSummary;
import com.plsqlanalyzer.analyzer.model.SequenceOperationSummary;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import com.plsqlanalyzer.web.controller.detail.AnalysisTriggerService;
import com.plsqlanalyzer.web.controller.detail.CallGraphQueryService;
import com.plsqlanalyzer.web.controller.detail.FastAnalysisTriggerService;
import com.plsqlanalyzer.web.controller.detail.HistoryQueryService;
import com.plsqlanalyzer.web.controller.detail.JobQueryService;
import com.plsqlanalyzer.web.controller.detail.MiscQueryService;
import com.plsqlanalyzer.web.controller.detail.ProcedureDetailService;
import com.plsqlanalyzer.web.controller.detail.ProcedureReportsService;
import com.plsqlanalyzer.web.controller.detail.TableMetadataService;
import com.plsqlanalyzer.web.controller.detail.TableQueryService;
import com.plsqlanalyzer.web.page.FacetPageResponder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Thin REST controller for PL/SQL analysis. All endpoint logic lives in the
 * collaborating @Service classes under {@code web.controller.detail}; each method
 * here is a pure delegation to preserve routes, signatures and response shapes.
 */
@RestController("plsqlAnalyzerController")
@RequestMapping("/api/plsql/analysis")
public class AnalyzerController {

    private final AnalysisTriggerService analysisTriggerService;
    private final FastAnalysisTriggerService fastAnalysisTriggerService;
    private final HistoryQueryService historyQueryService;
    private final CallGraphQueryService callGraphQueryService;
    private final ProcedureDetailService procedureDetailService;
    private final ProcedureReportsService procedureReportsService;
    private final TableQueryService tableQueryService;
    private final TableMetadataService tableMetadataService;
    private final MiscQueryService miscQueryService;
    private final JobQueryService jobQueryService;
    private final ObjectMapper mapper;
    private final FacetPageResponder facets;

    public AnalyzerController(AnalysisTriggerService analysisTriggerService,
                             FastAnalysisTriggerService fastAnalysisTriggerService,
                             HistoryQueryService historyQueryService,
                             CallGraphQueryService callGraphQueryService,
                             ProcedureDetailService procedureDetailService,
                             ProcedureReportsService procedureReportsService,
                             TableQueryService tableQueryService,
                             TableMetadataService tableMetadataService,
                             MiscQueryService miscQueryService,
                             JobQueryService jobQueryService,
                             ObjectMapper mapper,
                             FacetPageResponder facets) {
        this.analysisTriggerService = analysisTriggerService;
        this.fastAnalysisTriggerService = fastAnalysisTriggerService;
        this.historyQueryService = historyQueryService;
        this.callGraphQueryService = callGraphQueryService;
        this.procedureDetailService = procedureDetailService;
        this.procedureReportsService = procedureReportsService;
        this.tableQueryService = tableQueryService;
        this.tableMetadataService = tableMetadataService;
        this.miscQueryService = miscQueryService;
        this.jobQueryService = jobQueryService;
        this.mapper = mapper;
        this.facets = facets;
    }

    @PostMapping("/analyze")
    public ResponseEntity<Map<String, Object>> triggerAnalysis(@RequestBody Map<String, String> body) {
        return analysisTriggerService.triggerAnalysis(body);
    }

    @PostMapping("/analyze-fast")
    public ResponseEntity<Map<String, Object>> triggerFastAnalysis(@RequestBody Map<String, String> body) {
        return fastAnalysisTriggerService.triggerFastAnalysis(body);
    }

    @GetMapping
    public ResponseEntity<Map<String, Object>> getLatestResult() {
        return historyQueryService.getLatestResult();
    }

    @GetMapping("/history")
    public ResponseEntity<List<Map<String, Object>>> listHistory() {
        return historyQueryService.listHistory();
    }

    @GetMapping("/history/{name}")
    public ResponseEntity<Map<String, Object>> loadAnalysis(@PathVariable String name) {
        return historyQueryService.loadAnalysis(name);
    }

    @DeleteMapping("/history/{name}")
    public ResponseEntity<Map<String, Object>> deleteAnalysis(@PathVariable String name) {
        return historyQueryService.deleteAnalysis(name);
    }

    @GetMapping("/history/{name}/connections")
    public ResponseEntity<Map<String, Object>> getAnalysisConnections(@PathVariable String name) {
        return historyQueryService.getAnalysisConnections(name);
    }

    @GetMapping("/versions")
    public ResponseEntity<List<Map<String, Object>>> listVersions() {
        return historyQueryService.listVersions();
    }

    @GetMapping("/call-graph")
    public ResponseEntity<Object> getFullCallGraph() {
        return callGraphQueryService.getFullCallGraph();
    }

    @GetMapping("/detail/{procName:.+}")
    public ResponseEntity<Map<String, Object>> getProcedureDetail(@PathVariable String procName) {
        return procedureDetailService.getProcedureDetail(procName);
    }

    /**
     * UI-030c — all procedure reports plus the four cross-procedure reports, assembled server-side
     * in one pass over {@code graph.getAllNodes()} instead of the client looping {@code /detail}
     * once per procedure (UI-030's original finding). See {@link ProcedureReportsService}.
     */
    @GetMapping("/reports")
    public ResponseEntity<Object> getProcedureReports() {
        return procedureReportsService.getProcedureReports();
    }

    @GetMapping("/call-tree/{procName:.+}")
    public ResponseEntity<Map<String, Object>> getCallTree(@PathVariable String procName) {
        return callGraphQueryService.getCallTree(procName);
    }

    @GetMapping("/call-tree/{procName:.+}/callers")
    public ResponseEntity<Map<String, Object>> getCallerTree(@PathVariable String procName) {
        return callGraphQueryService.getCallerTree(procName);
    }

    @GetMapping("/tables")
    public ResponseEntity<Collection<TableOperationSummary>> getTableOperations() {
        return tableQueryService.getTableOperations();
    }

    /**
     * MID-106, second increment; UI-028's {@code directProcId}/{@code nodeIds} added once
     * {@link PlsqlScopeFilter} existed. See {@link PlsqlFacetSpecs#TABLES} and
     * {@code /procedures/page}'s javadoc.
     */
    /**
     * UI-002 (dedicated project, §133+) — no longer embeds each row's own {@code accessDetails}/
     * {@code _fullAccessDetails}. Measured directly against {@code TableOperationSummary}: unlike
     * the parser backend's own `/tables/page` (whose wire row is already the compact
     * {@code usedBy[]} form, one entry per accessing procedure), this backend's row is the real
     * POJO, serializing one {@code TableAccessDetail} per individual access line — up to 1,385 on a
     * real analysis, on *every* row of *every* page, regardless of whether that row's detail panel
     * is ever opened. {@code accessCount}/{@code _fullAccessCount} (already scalar, already
     * authoritative for the count column and the "scoped" label) are untouched — only the full
     * per-access array is stripped, via {@link #stripAccessDetails}, applied *after* paging/scope
     * trimming so {@code PlsqlScopeFilter}'s own recomputation (which needs the array present to
     * filter) is unaffected. The expand panel now fetches the one table's own detail on demand — see
     * {@code plsql/js/tableOps.js}'s own updated {@code _renderDetail}.
     */
    @GetMapping("/tables/page")
    public ResponseEntity<com.fasterxml.jackson.databind.JsonNode> getTableOperationsPage(
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "types", required = false) String types,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestParam(value = "directProcId", required = false) String directProcId,
            @RequestParam(value = "nodeIds", required = false) List<String> nodeIds,
            @RequestHeader(value = "Accept", required = false) String accept) {
        return stripAccessDetails(pagedFacet(tableQueryService.getTableOperations().getBody(),
                PlsqlFacetSpecs.TABLES, limit, after, sort, q, types, cf, directProcId, nodeIds, accept));
    }

    /**
     * Removes {@code accessDetails}/{@code _fullAccessDetails} from each row of an already-built
     * page response, leaving every other field (including the scalar counts) untouched. Applied
     * only to {@code /tables/page} — {@code /tables} (the legitimate whole-export read) and
     * {@code /tables/{tableName}} (already bounded to one table) both keep the full field, since
     * neither has the "every row of every page" multiplication this fixes.
     */
    private ResponseEntity<com.fasterxml.jackson.databind.JsonNode> stripAccessDetails(
            ResponseEntity<com.fasterxml.jackson.databind.JsonNode> resp) {
        com.fasterxml.jackson.databind.JsonNode body = resp.getBody();
        if (body == null) return resp;
        com.fasterxml.jackson.databind.JsonNode items = body.has("items") ? body.get("items") : body;
        if (items != null && items.isArray()) {
            for (com.fasterxml.jackson.databind.JsonNode item : items) {
                if (item.isObject()) {
                    com.fasterxml.jackson.databind.node.ObjectNode obj =
                            (com.fasterxml.jackson.databind.node.ObjectNode) item;
                    // The grid's own "AI" badge (plsql/js/tableOps.js's _renderRow) reads a count of
                    // Claude-verified accesses — the one piece of per-row signal the grid genuinely
                    // needs a scalar for, not the full array. Computed here, before the array this
                    // scalar is derived from is discarded, so the badge does not silently go dark.
                    com.fasterxml.jackson.databind.JsonNode details = obj.get("accessDetails");
                    long claudeVerified = 0;
                    if (details != null && details.isArray()) {
                        for (com.fasterxml.jackson.databind.JsonNode d : details) {
                            if ("claude-verified".equals(d.path("sourceFile").asText(null))) claudeVerified++;
                        }
                    }
                    obj.put("claudeVerifiedCount", claudeVerified);
                    obj.remove("accessDetails");
                    obj.remove("_fullAccessDetails");
                }
            }
        }
        return resp;
    }

    @GetMapping("/tables/{tableName}")
    public ResponseEntity<TableOperationSummary> getTableDetail(@PathVariable String tableName) {
        return tableQueryService.getTableDetail(tableName);
    }

    /**
     * UI-028 — `predicates.js`'s aggregation (§53), ported and paged. Not `pagedFacet`: this row's
     * "document" is not the tables collection itself but {@link PlsqlPredicatesFacet#build}'s
     * transform of it, and its scope trim ({@link PlsqlScopeFilter#directPredicates}/{@code
     * subtreePredicates}) operates on a different field ({@code predicates}, not {@code
     * accessDetails}) than every other converted screen here.
     */
    @GetMapping("/predicates/page")
    public ResponseEntity<com.fasterxml.jackson.databind.JsonNode> getPredicatesPage(
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "types", required = false) String types,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestParam(value = "directProcId", required = false) String directProcId,
            @RequestParam(value = "nodeIds", required = false) List<String> nodeIds,
            @RequestHeader(value = "Accept", required = false) String accept) {
        Collection<TableOperationSummary> tables = tableQueryService.getTableOperations().getBody();
        if (tables == null) return ResponseEntity.notFound().build();
        com.fasterxml.jackson.databind.JsonNode document =
                PlsqlPredicatesFacet.build(mapper, mapper.valueToTree(tables));
        if (directProcId != null && !directProcId.isBlank()) {
            document = PlsqlScopeFilter.directPredicates(mapper, document, directProcId);
        } else if (nodeIds != null && !nodeIds.isEmpty()) {
            document = PlsqlScopeFilter.subtreePredicates(mapper, document, new java.util.HashSet<>(nodeIds));
        }
        try {
            return facets.respond(document, null, PlsqlFacetSpecs.PREDICATES,
                    new FacetPageResponder.Params(limit, after, sort, q, types, cf, accept), null);
        } catch (IllegalArgumentException | com.analyzer.storage.page.Cursor.InvalidCursorException e) {
            return FacetPageResponder.badRequest(mapper, e.getMessage());
        }
    }

    @GetMapping("/tables/{tableName}/metadata")
    public ResponseEntity<Map<String, Object>> getTableMetadata(@PathVariable String tableName) {
        return tableMetadataService.getTableMetadata(tableName);
    }

    @GetMapping("/tables/{tableName}/triggers")
    public ResponseEntity<List<Map<String, Object>>> getTableTriggers(@PathVariable String tableName) {
        return tableMetadataService.getTableTriggers(tableName);
    }

    @GetMapping("/sequences")
    public ResponseEntity<Collection<SequenceOperationSummary>> getSequenceOperations() {
        return tableQueryService.getSequenceOperations();
    }

    /** MID-106, third increment. See {@link PlsqlFacetSpecs#SEQUENCES}. */
    @GetMapping("/sequences/page")
    public ResponseEntity<com.fasterxml.jackson.databind.JsonNode> getSequenceOperationsPage(
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "types", required = false) String types,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestHeader(value = "Accept", required = false) String accept) {
        return pagedFacet(tableQueryService.getSequenceOperations().getBody(), PlsqlFacetSpecs.SEQUENCES,
                limit, after, sort, q, types, cf, null, null, accept);
    }

    @GetMapping("/joins")
    public ResponseEntity<Collection<JoinOperationSummary>> getJoinOperations() {
        return tableQueryService.getJoinOperations();
    }

    /** MID-106, fourth increment; UI-028 scope params — see {@code /tables/page}'s javadoc. */
    @GetMapping("/joins/page")
    public ResponseEntity<com.fasterxml.jackson.databind.JsonNode> getJoinOperationsPage(
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "types", required = false) String types,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestParam(value = "directProcId", required = false) String directProcId,
            @RequestParam(value = "nodeIds", required = false) List<String> nodeIds,
            @RequestHeader(value = "Accept", required = false) String accept) {
        return pagedFacet(tableQueryService.getJoinOperations().getBody(), PlsqlFacetSpecs.JOINS,
                limit, after, sort, q, types, cf, directProcId, nodeIds, accept);
    }

    @GetMapping("/cursors")
    public ResponseEntity<Collection<CursorOperationSummary>> getCursorOperations() {
        return tableQueryService.getCursorOperations();
    }

    /** MID-106, fifth increment; UI-028 scope params — see {@code /tables/page}'s javadoc. All five endpoints now have a paged sibling. See {@link PlsqlFacetSpecs#CURSORS}. */
    @GetMapping("/cursors/page")
    public ResponseEntity<com.fasterxml.jackson.databind.JsonNode> getCursorOperationsPage(
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "types", required = false) String types,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestParam(value = "directProcId", required = false) String directProcId,
            @RequestParam(value = "nodeIds", required = false) List<String> nodeIds,
            @RequestHeader(value = "Accept", required = false) String accept) {
        return pagedFacet(tableQueryService.getCursorOperations().getBody(), PlsqlFacetSpecs.CURSORS,
                limit, after, sort, q, types, cf, directProcId, nodeIds, accept);
    }

    /**
     * Shared by {@code /tables/page}, {@code /joins/page}, {@code /sequences/page} and {@code
     * /cursors/page} (and, in spirit, {@code /procedures/page}, which has no {@code types}
     * parameter and so keeps its own body rather than forcing a parameter neither
     * {@link PlsqlFacetSpecs#PROCEDURES} nor its caller uses): converts whichever in-memory
     * collection to a {@code JsonNode} array and pages it through the shared facet machinery. See
     * {@link PlsqlFacetSpecs}'s own javadoc for why the conversion happens here rather than a
     * second, POJO-native pager existing.
     *
     * <p>UI-028: when {@code directProcId} or {@code nodeIds} is present, {@link PlsqlScopeFilter}
     * trims the array to the scoped rows (each with its own {@code accessDetails} trimmed in turn)
     * <b>before</b> paging — the same "Direct" / "Full Flow" scope `tableOps.js`/`joins.js`/
     * `cursors.js` used to compute over a fully-materialised in-memory array. {@code sequences}
     * has no scope UI (checked, not assumed — it is the one screen without a scope toggle), so
     * its call site passes {@code null} for both and never reaches this branch.
     */
    private ResponseEntity<com.fasterxml.jackson.databind.JsonNode> pagedFacet(
            Collection<?> rows, com.plsqlanalyzer.web.page.FacetSpec spec,
            Integer limit, String after, String sort, String q, String types, List<String> cf,
            String directProcId, List<String> nodeIds, String accept) {
        if (rows == null) return ResponseEntity.notFound().build();
        com.fasterxml.jackson.databind.JsonNode document = mapper.valueToTree(rows);
        if (directProcId != null && !directProcId.isBlank()) {
            document = PlsqlScopeFilter.direct(mapper, document, directProcId);
        } else if (nodeIds != null && !nodeIds.isEmpty()) {
            document = PlsqlScopeFilter.subtree(mapper, document, new java.util.HashSet<>(nodeIds));
        }
        try {
            return facets.respond(document, null, spec,
                    new FacetPageResponder.Params(limit, after, sort, q, types, cf, accept), null);
        } catch (IllegalArgumentException | com.analyzer.storage.page.Cursor.InvalidCursorException e) {
            return FacetPageResponder.badRequest(mapper, e.getMessage());
        }
    }

    @GetMapping("/errors")
    public ResponseEntity<List<Map<String, Object>>> getParseErrors() {
        return miscQueryService.getParseErrors();
    }

    @GetMapping("/procedures")
    public ResponseEntity<List<Map<String, Object>>> listProcedures() {
        return callGraphQueryService.listProcedures();
    }

    /**
     * MID-106 (first increment) — {@code /procedures} paged, searched and sorted server-side.
     * Added alongside {@code /procedures}, not in place of it: the plain route is left for
     * whatever already calls it (checked: {@code plsql/js/app.js}'s procedure count and the
     * detail-lookup path, neither of which pages), the same "convert one screen at a time"
     * approach the other controller's facets used. See {@link PlsqlFacetSpecs}.
     */
    @GetMapping("/procedures/page")
    public ResponseEntity<com.fasterxml.jackson.databind.JsonNode> listProceduresPage(
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestHeader(value = "Accept", required = false) String accept) {
        List<Map<String, Object>> procs = callGraphQueryService.listProcedures().getBody();
        if (procs == null) return ResponseEntity.notFound().build();
        com.fasterxml.jackson.databind.JsonNode document = mapper.valueToTree(procs);
        try {
            return facets.respond(document, null, PlsqlFacetSpecs.PROCEDURES,
                    new com.plsqlanalyzer.web.page.FacetPageResponder.Params(
                            limit, after, sort, q, null, cf, accept),
                    null);
        } catch (IllegalArgumentException | com.analyzer.storage.page.Cursor.InvalidCursorException e) {
            return com.plsqlanalyzer.web.page.FacetPageResponder.badRequest(mapper, e.getMessage());
        }
    }

    @GetMapping("/source/{owner}/{objectName:.+}")
    public ResponseEntity<Map<String, Object>> getSource(
            @PathVariable String owner, @PathVariable String objectName) {
        return miscQueryService.getSource(owner, objectName);
    }

    @GetMapping("/references/{objectName:.+}")
    public ResponseEntity<List<Map<String, String>>> getReferences(
            @PathVariable String objectName,
            @RequestParam(required = false) String owner) {
        return miscQueryService.getReferences(objectName, owner);
    }

    @GetMapping("/search")
    public ResponseEntity<List<Map<String, Object>>> search(
            @RequestParam String q,
            @RequestParam(defaultValue = "all") String type) {
        return miscQueryService.search(q, type);
    }

    @GetMapping("/jobs")
    public ResponseEntity<List<Map<String, Object>>> listJobs() {
        return jobQueryService.listJobs();
    }

    @GetMapping("/jobs/{jobId}")
    public ResponseEntity<Map<String, Object>> getJob(@PathVariable String jobId) {
        return jobQueryService.getJob(jobId);
    }

    @PostMapping("/jobs/{jobId}/cancel")
    public ResponseEntity<Map<String, Object>> cancelJob(@PathVariable String jobId) {
        return jobQueryService.cancelJob(jobId);
    }
}
