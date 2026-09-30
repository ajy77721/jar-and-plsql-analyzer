package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

/**
 * UI-024 — server-side computation for `complexity.js`'s per-procedure risk score
 * (`_metricContribution`/`scoreRaw`/`_computeMetrics`), replacing its `PA.nodeIndex.all()` scan.
 *
 * <p><b>Why this one is a different shape from every other UI-024 conversion in this file's own
 * neighborhood</b>: `statements.js`/`detailHeader.js`'s conversions collapse a full-corpus scan
 * into a small aggregate. This one can't — the client's own interactive table needs one row per
 * procedure to render, sort and filter, so the response here is the same size class as
 * `PA.nodeIndex.all()` itself. Moving here does remove one more `.all()` call site and the
 * per-render CPU cost of scoring every node in the browser, but it does not, by itself, reduce
 * how much data the browser holds — recorded honestly rather than oversold.
 *
 * <p><b>Weights/thresholds/algorithm are the caller's, not the server's</b>: the client's own
 * effective config is layered from server defaults plus `localStorage` overrides
 * (`_resolveConfig`), which this endpoint has no access to and isn't meant to — the caller sends
 * the already-resolved values as request parameters every time, exactly the values it would have
 * used to compute locally.
 *
 * <p>Formulas transcribed field-for-field from `_metricContribution`/`scoreRaw`:
 * {@code linear: w*v}, {@code logarithmic: w*log2(1+v)}, {@code exponential: w*((1+v)^p - 1)},
 * {@code balanced: w*sqrt(v)}; {@code score = round(sum of six metric contributions)};
 * {@code risk = score>=high ? HIGH : score>=medium ? MEDIUM : LOW}.
 *
 * <h2>{@code depth} here is the nesting anchor level, not graph distance</h2>
 *
 * <p>The {@code depth} this endpoint reports and scores is the index node's own {@code depth} — the
 * level the crawler anchored the object at, not its distance from the entry point in the call
 * graph. The two deliberately differ for triggers: {@code DependencyCrawler} anchors every emitted
 * trigger at depth 1 ({@code tn.setDepth(1)}) so a trigger appears as a root-level artefact of the
 * table it fires on, while the call-tree walker's own {@code depth} (see {@code CallGraphNav}) is
 * BFS distance from whatever node the tree is rooted at — so the same trigger can show depth 1 here
 * and depth 7 in the call tree. That divergence is intentional; do not "fix" one to match the other
 * without deciding which meaning the caller wants.
 */
@Service
public class ComplexityScoringService {

    private final AnalysisService service;
    private final ObjectMapper mapper;

    public ComplexityScoringService(AnalysisService service, ObjectMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    public static double metricContribution(double value, double weight, String algorithm, double exponent) {
        switch (algorithm == null ? "linear" : algorithm) {
            case "logarithmic": return weight * (Math.log(1 + value) / Math.log(2));
            case "exponential": return weight * (Math.pow(1 + value, exponent <= 0 ? 1.3 : exponent) - 1);
            case "balanced":    return weight * Math.sqrt(value);
            case "linear":
            default:            return weight * value;
        }
    }

    public ResponseEntity<Object> getScores(String name, Weights w, Thresholds th, String algorithm, double exponent) {
        JsonNode index;
        try {
            index = service.getIndex(name);
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
        if (index == null || !index.has("nodes") || !index.get("nodes").isArray()) {
            return ResponseEntity.notFound().build();
        }

        ArrayNode result = mapper.createArrayNode();
        for (JsonNode n : index.get("nodes")) {
            JsonNode counts = n.path("counts");
            JsonNode stmts = counts.path("statements");
            long totalStmts = 0;
            var it = stmts.fields();
            while (it.hasNext()) totalStmts += it.next().getValue().asLong(0);
            long dynSql = stmts.path("EXECUTE_IMMEDIATE").asLong(0) + stmts.path("DBMS_SQL").asLong(0);
            long tables = counts.path("tables").asLong(0);
            long callsOut = counts.path("callsOut").asLong(0);
            long cursors = counts.path("cursors").asLong(0);
            long exHandlers = counts.path("exceptionHandlers").asLong(0);
            long loc = n.path("linesOfCode").asLong(0);
            // Nesting anchor level from the index (crawler-assigned), NOT call-graph distance —
            // see the class javadoc's "depth here is the nesting anchor level" note.
            long depth = n.path("depth").asLong(0);

            double score = Math.round(
                    metricContribution(loc, w.loc, algorithm, exponent)
                    + metricContribution(tables, w.tables, algorithm, exponent)
                    + metricContribution(callsOut, w.callsOut, algorithm, exponent)
                    + metricContribution(cursors, w.cursors, algorithm, exponent)
                    + metricContribution(dynSql, w.dynamicSql, algorithm, exponent)
                    + metricContribution(depth, w.depth, algorithm, exponent));
            String risk = score >= th.high ? "HIGH" : (score >= th.medium ? "MEDIUM" : "LOW");

            ObjectNode row = mapper.createObjectNode();
            row.put("nodeId", n.path("nodeId").asText(""));
            row.put("name", !n.path("name").asText("").isEmpty() ? n.path("name").asText() : n.path("objectName").asText(""));
            row.put("schema", n.path("schema").asText(""));
            row.put("objectType", n.path("objectType").asText(""));
            row.put("loc", loc);
            row.put("depth", depth);
            row.put("tables", tables);
            row.put("callsOut", callsOut);
            row.put("cursors", cursors);
            row.put("exHandlers", exHandlers);
            row.put("dynSql", dynSql);
            row.put("totalStmts", totalStmts);
            row.set("stmts", stmts);
            row.put("params", counts.path("parameters").asLong(0));
            row.put("variables", counts.path("variables").asLong(0));
            row.put("sequences", counts.path("sequences").asLong(0));
            row.put("readable", !n.path("readable").isBoolean() || n.path("readable").asBoolean(true));
            row.put("score", score);
            row.put("risk", risk);
            row.put("sourceFile", n.path("sourceFile").asText(""));
            row.put("detailFile", n.path("detailFile").asText(""));
            ObjectNode wOut = row.putObject("_w");
            wOut.put("loc", w.loc);
            wOut.put("tables", w.tables);
            wOut.put("callsOut", w.callsOut);
            wOut.put("cursors", w.cursors);
            wOut.put("dynSql", w.dynamicSql);
            wOut.put("depth", w.depth);
            result.add(row);
        }
        return ResponseEntity.ok(result);
    }

    /** Mirrors `_computeMetrics`'s own hardcoded defaults exactly. */
    public static final class Weights {
        public double loc = 0.3, tables = 15, callsOut = 10, cursors = 8, dynamicSql = 20, depth = 5;
    }

    public static final class Thresholds {
        public double medium = 50, high = 150;
    }
}
