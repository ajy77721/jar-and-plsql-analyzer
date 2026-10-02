package com.plsqlanalyzer.web.parser.controller;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.page.Cursor;
import com.analyzer.storage.repo.AnalysisQuery;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.web.page.FacetPageResponder;
import com.plsqlanalyzer.web.page.FacetSpec;
import com.plsqlanalyzer.web.parser.service.AnalysisPaths;
import com.plsqlanalyzer.web.parser.service.ParserScopeResolver;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.function.Predicate;

/**
 * The Table Detail modal's two table-scoped lists — PLAN-08 §4.3 step 10, Q59 and Q59b.
 *
 * <p>These replace two {@code Promise.all} fan-outs that fetched every node-detail document in the
 * current call tree (measured: 64 documents, 3.6 MB, per fan-out, per modal open) to produce a few
 * dozen rows. The scan now runs where the documents already are and the client asks for a page.
 *
 * <p>A separate controller from {@link ParserAnalysisController} on purpose: this conversion is
 * meant to be revertible on its own, and deleting a file is a cleaner revert than unpicking
 * methods from a controller other screens share.
 *
 * <p>{@code scope}/{@code procId} are not optional decoration. The fan-out they replace was
 * restricted to the nodes of the open procedure's call tree; without the same restriction on the
 * request the modal would list joins from the whole analysis while the header says otherwise. The
 * client sends {@code subflow}, which is what "this node and everything it calls" means in
 * {@link ParserScopeResolver}, and {@code full} at a package root, where the browser's id set was
 * every node.
 */
@RestController
@RequestMapping("/api/parser/analyses")
public class TableDetailController {

    private final AnalysisRepository repository;
    private final FacetPageResponder facets;
    private final ParserScopeResolver scopes;
    private final ObjectMapper mapper;

    public TableDetailController(AnalysisRepository repository,
                                 FacetPageResponder facets,
                                 ParserScopeResolver scopes,
                                 ObjectMapper mapper) {
        this.repository = repository;
        this.facets = facets;
        this.scopes = scopes;
        this.mapper = mapper;
    }

    @GetMapping("/{name}/table-joins")
    public ResponseEntity<JsonNode> tableJoins(
            @PathVariable("name") String name,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId,
            @RequestHeader(value = "Accept", required = false) String accept) {
        return respond(name, table, TableDetailFacetSpecs.TABLE_JOINS, limit, after, sort, q, cf,
                scope, procId, accept, true);
    }

    @GetMapping("/{name}/table-joins/summary")
    public ResponseEntity<JsonNode> tableJoinsSummary(
            @PathVariable("name") String name,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId) {
        return respond(name, table, TableDetailFacetSpecs.TABLE_JOINS, null, null, null, null, null,
                scope, procId, null, false);
    }

    @GetMapping("/{name}/table-columns")
    public ResponseEntity<JsonNode> tableColumns(
            @PathVariable("name") String name,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "types", required = false) String types,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId,
            @RequestHeader(value = "Accept", required = false) String accept) {
        return respondColumns(name, table, limit, after, sort, q, types, cf, scope, procId, accept,
                true);
    }

    @GetMapping("/{name}/table-columns/summary")
    public ResponseEntity<JsonNode> tableColumnsSummary(
            @PathVariable("name") String name,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId) {
        return respondColumns(name, table, null, null, null, null, null, null, scope, procId, null,
                false);
    }

    /**
     * B3 — one table's column-level statement detail: which columns each statement reads or
     * writes, and for a write where the value came from. Also carries the table's in-procedure
     * {@code ddl}, and the true {@code totalColumns}/{@code totalDdl}.
     *
     * <p><b>A new route rather than an extension of {@code table-columns}.</b> That endpoint
     * answers a different question — {@code %TYPE}/{@code %ROWTYPE} declarations scanned out of
     * the node documents — with a different row shape, from a different artifact. Merging them
     * would mean a row union half of whose fields are null depending on the kind, and it could not
     * be done without changing an existing response. Those four routes are untouched and still
     * byte-identical.
     *
     * <p>Paged by the same responder as every other list here: no paging parameter and no envelope
     * media type gives the whole document, capped and flagged by {@code X-Analyzer-Truncated} if
     * it is ever large enough to matter. {@code totalColumns} is counted over the full array
     * before any cap, so it is a true total in every response.
     *
     * <p>Degrades cleanly where a dialect has less: a table with no column detail answers with an
     * empty {@code columns} and {@code totalColumns: 0}, not a 404 and not an error. That is the
     * honest answer for SQL Server statements that ANSI parsing rejects.
     */
    @GetMapping("/{name}/table-column-detail")
    public ResponseEntity<JsonNode> tableColumnDetail(
            @PathVariable("name") String name,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "types", required = false) String types,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId,
            @RequestHeader(value = "Accept", required = false) String accept) {
        return serve(AnalysisQuery.Dataset.PLSQL_TABLE_COLUMN_DETAIL, name, table,
                TableDetailFacetSpecs.TABLE_COLUMN_DETAIL, limit, after, sort, q, types, cf,
                scope, procId, accept, true);
    }

    /** Counts, pill values and filter values for {@link #tableColumnDetail}. */
    @GetMapping("/{name}/table-column-detail/summary")
    public ResponseEntity<JsonNode> tableColumnDetailSummary(
            @PathVariable("name") String name,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "procId", required = false) String procId) {
        return serve(AnalysisQuery.Dataset.PLSQL_TABLE_COLUMN_DETAIL, name, table,
                TableDetailFacetSpecs.TABLE_COLUMN_DETAIL, null, null, null, null, null, null,
                scope, procId, null, false);
    }

    /* ------------------------------------------------------------------ */

    private ResponseEntity<JsonNode> respond(String name, String table, FacetSpec spec,
                                             Integer limit, String after, String sort, String q,
                                             List<String> cf, String scope, String procId,
                                             String accept, boolean paged) {
        return serve(AnalysisQuery.Dataset.PLSQL_TABLE_JOINS, name, table, spec, limit,
                after, sort, q, null, cf, scope, procId, accept, paged);
    }

    private ResponseEntity<JsonNode> respondColumns(String name, String table, Integer limit,
                                                    String after, String sort, String q,
                                                    String types, List<String> cf, String scope,
                                                    String procId, String accept, boolean paged) {
        return serve(AnalysisQuery.Dataset.PLSQL_TABLE_COLUMNS, name, table,
                TableDetailFacetSpecs.TABLE_COLUMNS, limit, after, sort, q, types, cf, scope,
                procId, accept, paged);
    }

    private ResponseEntity<JsonNode> serve(AnalysisQuery.Dataset dataset, String name,
                                           String table, FacetSpec spec, Integer limit,
                                           String after, String sort, String q, String types,
                                           List<String> cf, String scope, String procId,
                                           String accept, boolean paged) {
        if (table == null || table.isBlank()) {
            // The whole point of these endpoints is that they are table-scoped. Without a table
            // the honest answer is a stated 400, not the entire analysis. Checked here rather than
            // relying on AnalysisQuery's own table requirement, so the caller gets this message
            // instead of a constructor's.
            return FacetPageResponder.badRequest(mapper, "Parameter 'table' is required");
        }
        name = AnalysisPaths.canonical(name);   // task #33 — OracleDemo / ORACLEDEMO resolve alike
        JsonNode document;
        try {
            document = repository.document(AnalysisQuery.ofTable(dataset, name, table));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
        if (dataset == AnalysisQuery.Dataset.PLSQL_TABLE_COLUMN_DETAIL) {
            // This facet is a document with an array under "columns", not a bare array.
            return serveColumnDetail(document, spec, limit, after, sort, q, types, cf, name,
                    scope, procId, accept, paged);
        }
        try {
            Predicate<JsonNode> scopeFilter =
                    scopes.predicate(name, scope, procId, spec.procedureIds());
            if (!paged) {
                return facets.summary(document, null, spec, scopeFilter);
            }
            return facets.respond(document, null, spec,
                    new FacetPageResponder.Params(limit, after, sort, q, types, cf, accept),
                    scopeFilter);
        } catch (IllegalArgumentException | Cursor.InvalidCursorException e) {
            // An unknown sort or scope name, an out-of-range limit, or a stale cursor — answered
            // with the reason, never with a page that happens to be empty.
            return FacetPageResponder.badRequest(mapper, e.getMessage());
        } catch (IllegalStateException e) {
            return FacetPageResponder.badRequest(mapper, e.getMessage());
        }
    }

    private ResponseEntity<JsonNode> serveColumnDetail(
            JsonNode document, FacetSpec spec, Integer limit, String after, String sort, String q,
            String types, List<String> cf, String name, String scope, String procId, String accept,
            boolean paged) {
        try {
            Predicate<JsonNode> scopeFilter =
                    scopes.predicate(name, scope, procId, spec.procedureIds());
            if (!paged) {
                return facets.summary(document, "columns", spec, scopeFilter);
            }
            return facets.respond(document, "columns", spec,
                    new FacetPageResponder.Params(limit, after, sort, q, types, cf, accept),
                    scopeFilter);
        } catch (IllegalArgumentException | Cursor.InvalidCursorException e) {
            return FacetPageResponder.badRequest(mapper, e.getMessage());
        } catch (IllegalStateException e) {
            return FacetPageResponder.badRequest(mapper, e.getMessage());
        }
    }
}
