package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;

/**
 * UI-024 — server-side aggregation for `parser/js/summary.js`'s scoped dashboard
 * (`_computeStats`), replacing its `PA.getScopedNodes()` → `PA.nodeIndex.all()` scan.
 *
 * <p>Unlike `complexity.js` (§98), this genuinely collapses to a small aggregate — the summary
 * dashboard renders totals, breakdowns and four "most X" highlights, never a row per procedure —
 * so this conversion, like `statements.js`/`detailHeader.js`, actually reduces what the browser
 * needs to hold, not just where the computation runs.
 *
 * <p><b>{@code totalTables} means the DISTINCT table count, everywhere.</b> It is the same number
 * {@code /analyses} and {@code /tables} report (the analysis index root's {@code totalTables},
 * written by the crawler from its distinct table set), so the same name means the same thing on
 * every endpoint. The sum of per-node table <em>references</em> — which this endpoint used to
 * mislabel {@code totalTables} (e.g. 1256 vs the 114 distinct) — is now its own field,
 * {@code totalTableReferences}.
 *
 * <p>Root-level fields the client already reads directly off `PA.analysisData` (`totalTables`,
 * `totalEdges`, `maxDepth`, `errors.length`, `entrySchema`/`entryPoint`) are NOT duplicated here —
 * they aren't a scan (a handful of scalars on an already-loaded small object), and the client's own
 * `data.xxx || computed` fallback pattern is preserved by only ever returning the *computed*
 * halves from this endpoint.
 */
@Service
public class SummaryStatsService {

    private final AnalysisService service;
    private final ParserScopeResolver scopeResolver;
    private final ObjectMapper mapper;

    public SummaryStatsService(AnalysisService service, ParserScopeResolver scopeResolver, ObjectMapper mapper) {
        this.service = service;
        this.scopeResolver = scopeResolver;
        this.mapper = mapper;
    }

    public ResponseEntity<Object> getStats(String name, String scope, String procId, String entrySchemaRaw) {
        JsonNode index;
        try {
            index = service.getIndex(name);
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
        if (index == null || !index.has("nodes") || !index.get("nodes").isArray()) {
            return ResponseEntity.notFound().build();
        }

        Predicate<JsonNode> predicate;
        try {
            predicate = scopeResolver.predicate(name, scope, procId,
                    n -> List.of(n.path("nodeId").asText("")));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(500).body(Map.of("error", e.getMessage()));
        }

        String entrySchema = (entrySchemaRaw == null ? "" : entrySchemaRaw).split("\\.")[0].toUpperCase(Locale.ROOT);

        long totalLoc = 0, totalTableReferences = 0, totalCalls = 0, totalCursors = 0;
        long totalParams = 0, totalVars = 0, totalExHandlers = 0, totalStmts = 0;
        long encryptedCount = 0, totalNodes = 0;
        Map<String, Long> schemas = new LinkedHashMap<>();
        Map<String, Long> types = new LinkedHashMap<>();
        Map<String, Long> stmtTypes = new LinkedHashMap<>();
        String[] maxLocName = {""}; long[] maxLocVal = {0};
        String[] maxDepthName = {""}; long[] maxDepthVal = {0};
        String[] maxTablesName = {""}; long[] maxTablesVal = {0};
        String[] maxCallsName = {""}; long[] maxCallsVal = {0};

        for (JsonNode n : index.get("nodes")) {
            if (predicate != null && !predicate.test(n)) continue;
            totalNodes++;

            JsonNode c = n.path("counts");
            long loc = n.path("linesOfCode").asLong(0);
            long tables = c.path("tables").asLong(0);
            long calls = c.path("internalCalls").asLong(0) + c.path("externalCalls").asLong(0);
            long cursors = c.path("cursors").asLong(0);
            long params = c.path("parameters").asLong(0);
            long vars = c.path("variables").asLong(0);
            long exh = c.path("exceptionHandlers").asLong(0);
            long depth = n.path("depth").asLong(0);

            totalLoc += loc;
            totalTableReferences += tables;
            totalCalls += calls;
            totalCursors += cursors;
            totalParams += params;
            totalVars += vars;
            totalExHandlers += exh;

            if (n.path("readable").isBoolean() && !n.path("readable").asBoolean(true)) encryptedCount++;

            String name1 = !n.path("name").asText("").isEmpty() ? n.path("name").asText() : n.path("nodeId").asText("");
            if (loc > maxLocVal[0]) { maxLocVal[0] = loc; maxLocName[0] = name1; }
            if (depth > maxDepthVal[0]) { maxDepthVal[0] = depth; maxDepthName[0] = name1; }
            if (tables > maxTablesVal[0]) { maxTablesVal[0] = tables; maxTablesName[0] = name1; }
            if (calls > maxCallsVal[0]) { maxCallsVal[0] = calls; maxCallsName[0] = name1; }

            String nodeSchema = n.path("schema").asText("").isEmpty() ? "UNKNOWN" : n.path("schema").asText().toUpperCase(Locale.ROOT);
            String schemaKey = (!entrySchema.isEmpty() && !nodeSchema.equals(entrySchema) && !nodeSchema.equals("UNKNOWN"))
                    ? "External (" + nodeSchema + ")" : nodeSchema;
            schemas.merge(schemaKey, 1L, Long::sum);

            String rawType = n.path("objectType").asText("").isEmpty() ? "PROCEDURE" : n.path("objectType").asText().toUpperCase(Locale.ROOT);
            String objType = rawType.contains("FUNC") ? "FUNCTION"
                    : rawType.contains("TRIGGER") ? "TRIGGER"
                    : rawType.contains("MATERIALIZED") ? "MATERIALIZED_VIEW"
                    : rawType.contains("VIEW") ? "VIEW"
                    : rawType.contains("SEQUENCE") ? "SEQUENCE"
                    : rawType.contains("INDEX") ? "INDEX"
                    : rawType.contains("ALTER") ? "ALTER_TABLE"
                    : rawType.contains("TYPE_BODY") ? "TYPE_BODY"
                    : rawType.contains("DOMAIN") ? "DOMAIN"
                    : rawType.contains("TYPE") ? "TYPE"
                    : "PROCEDURE";
            types.merge(objType, 1L, Long::sum);

            JsonNode stmts = c.path("statements");
            if (stmts.isObject()) {
                var it = stmts.fields();
                while (it.hasNext()) {
                    var e = it.next();
                    long scount = e.getValue().asLong(0);
                    stmtTypes.merge(e.getKey(), scount, Long::sum);
                    totalStmts += scount;
                }
            }
        }

        ObjectNode result = mapper.createObjectNode();
        result.put("totalNodes", totalNodes);
        result.put("totalLoc", totalLoc);
        // totalTables is the DISTINCT table count — the same number /analyses and /tables report —
        // and totalTableReferences is the per-node SUM this endpoint used to mislabel totalTables.
        result.put("totalTables", distinctTables(name, index));
        result.put("totalTableReferences", totalTableReferences);
        result.put("totalCalls", totalCalls);
        result.put("totalCursors", totalCursors);
        result.put("totalParams", totalParams);
        result.put("totalVars", totalVars);
        result.put("totalExHandlers", totalExHandlers);
        result.put("totalStmts", totalStmts);
        result.put("encryptedCount", encryptedCount);
        ObjectNode schemasOut = result.putObject("schemas");
        schemas.forEach(schemasOut::put);
        ObjectNode typesOut = result.putObject("types");
        types.forEach(typesOut::put);
        ObjectNode stmtTypesOut = result.putObject("stmtTypes");
        stmtTypes.forEach(stmtTypesOut::put);
        ObjectNode maxLoc = result.putObject("maxLoc");
        maxLoc.put("name", maxLocName[0]); maxLoc.put("value", maxLocVal[0]);
        ObjectNode maxDepthNode = result.putObject("maxDepthNode");
        maxDepthNode.put("name", maxDepthName[0]); maxDepthNode.put("value", maxDepthVal[0]);
        ObjectNode maxTables = result.putObject("maxTables");
        maxTables.put("name", maxTablesName[0]); maxTables.put("value", maxTablesVal[0]);
        ObjectNode maxCalls = result.putObject("maxCalls");
        maxCalls.put("name", maxCallsName[0]); maxCalls.put("value", maxCallsVal[0]);
        return ResponseEntity.ok(result);
    }

    /**
     * The analysis's DISTINCT table count, fetched the way {@code /analyses}/{@code /tables} get
     * theirs so the same name means the same number everywhere: the tables index sidecar's own root
     * scalar ({@code views}-independent, the same document the topbar reads), falling back to the
     * analysis index root's {@code totalTables} — the crawler writes the same distinct count into
     * both. This is analysis-wide, not scope-filtered: the scoped dashboard's own client overlays
     * the same root scalar ({@code summary.js}'s {@code data.totalTables || computed}), so returning
     * anything else here would disagree with what the user sees.
     */
    private long distinctTables(String name, JsonNode index) {
        try {
            JsonNode summary = service.getTablesSummary(name);
            if (summary != null && summary.hasNonNull("totalTables")) {
                return summary.get("totalTables").asLong(0);
            }
        } catch (Exception e) {
            // Older/merged analyses may have no api/tables/index.json; the index root carries the
            // same value, so fall through rather than fail the whole stats read.
        }
        return index.path("totalTables").asLong(0);
    }
}
