package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * UI-030-BUG — the parser-backend fix. `plsql/js/summary-analyzer.js:_analyzeProcedure` was
 * written against the live-crawl backend's `/detail/{procName}` shape (`tableName`, `callTree`);
 * the parser backend's real `/node/{file}` response (confirmed directly against
 * {@code data/plsql-parse/.../api/nodes/*.json}) has neither — its tables use `name`, and it has no
 * `callTree` field at all, only a `subtreeTables` array that is **already subtree-scoped**, richer
 * than a literal call-tree walk would produce. Every procedure analyzed through the parser app was
 * silently rendering with zero real data (caught by `_analyzeProcedure`'s own try/catch,
 * substituting `_minimalReport`) — this class is the dedicated fix the row called for, built
 * against this backend's own real shape rather than porting the broken logic.
 *
 * <h2>Real shape, read off disk, not assumed</h2>
 *
 * <pre>{@code
 * {
 *   "nodeId": "ANALYZER$ERP_PKG_PLANNER$WALK_UP",
 *   "name": "ERP_PKG_PLANNER.WALK_UP",
 *   "schema": "ANALYZER",
 *   "objectType": "PROCEDURE",
 *   "lineStart": 89, "lineEnd": 98, "linesOfCode": 10,
 *   "subtreeTables": [ { "schema", "name", "totalUsageCount", "directUsageCount",
 *                         "operations": { "INSERT": [11], "SELECT": [14] },
 *                         "usedByNodes": [ { "nodeId", "nodeName", "depth", "operations" } ] } ],
 *   "calls": [ { "target", "packageName", "objectName", "type", "schema", "lines", "count" } ],
 *   "statementSummary": { ... }
 * }
 * }</pre>
 *
 * <h2>Adaptation, not a literal port</h2>
 *
 * <p>No call tree is embedded in a node's own detail response here (unlike the live-crawl side),
 * so {@code totalCalls}/{@code callDepth}/{@code crossSchemaCalls} cannot be computed the same way.
 * Adapted instead from data this shape genuinely carries: {@code callDepth} is the max
 * {@code usedByNodes[].depth} across every subtree table (this backend already computed exactly how
 * deep each table access sits under this node); {@code totalCalls}/{@code crossSchemaCalls} come
 * from the flat {@code calls} list (this node's own direct calls, not the whole subtree — narrower
 * than the live-crawl side's whole-subtree count, and documented as such rather than silently
 * assumed equivalent).
 *
 * <p>The output shape matches {@code ProcedureReportBuilder}'s exactly, so
 * {@link com.plsqlanalyzer.web.controller.detail.ProcedureCrossReportBuilder}'s four methods work
 * on parser-backend reports completely unmodified.
 */
public final class ParserProcedureReportBuilder {

    private ParserProcedureReportBuilder() {
    }

    private static String text(JsonNode n) {
        return n == null || n.isNull() ? "" : n.asText("");
    }

    private static final Set<String> WRITE_OPS = Set.of("INSERT", "UPDATE", "DELETE", "MERGE");

    public static ObjectNode build(ObjectMapper mapper, JsonNode node, String entrySchema) {
        String entrySchemaUpper = (entrySchema == null ? "" : entrySchema).toUpperCase(Locale.ROOT);
        String fullName = text(node.get("name"));
        String packageName = "";
        String procName = fullName;
        int dot = fullName.indexOf('.');
        if (dot > 0) {
            packageName = fullName.substring(0, dot);
            procName = fullName.substring(dot + 1);
        }
        String schemaName = text(node.get("schema"));

        // flowTables/directTables — subtreeTables is already subtree-scoped, no walk needed at all.
        ObjectNode flowTables = mapper.createObjectNode();
        ObjectNode directTables = mapper.createObjectNode();
        Set<String> allOps = new LinkedHashSet<>();
        List<String> writeTables = new ArrayList<>();
        List<String> readTables = new ArrayList<>();
        for (JsonNode t : node.path("subtreeTables")) {
            String tName = text(t.get("name")).toUpperCase(Locale.ROOT);
            if (tName.isEmpty()) continue;
            ObjectNode to = mapper.createObjectNode();
            to.put("name", tName);
            to.put("schemaName", !text(t.get("schemaName")).isEmpty() ? text(t.get("schemaName")) : text(t.get("schema")));
            to.put("external", false); // not present in this shape — see class javadoc
            ArrayNode ops = to.putArray("operations");
            boolean hasWrite = false, hasSelect = false;
            JsonNode operations = t.get("operations");
            if (operations != null && operations.isObject()) {
                var it = operations.fieldNames();
                while (it.hasNext()) {
                    String op = it.next().toUpperCase(Locale.ROOT);
                    ops.add(op);
                    allOps.add(op);
                    if (WRITE_OPS.contains(op)) hasWrite = true;
                    if (op.equals("SELECT")) hasSelect = true;
                }
            }
            flowTables.set(tName, to);
            if (hasWrite) writeTables.add(tName);
            if (hasSelect) readTables.add(tName);
            if (t.path("directUsageCount").asInt(0) > 0) {
                directTables.set(tName, to);
            }
        }

        // calls: this node's own direct calls (not a whole-subtree count — see class javadoc).
        ArrayNode callsOut = mapper.createArrayNode();
        Set<String> schemas = new LinkedHashSet<>();
        if (!schemaName.isEmpty()) schemas.add(schemaName.toUpperCase(Locale.ROOT));
        long internalCalls = 0, externalCalls = 0, crossSchemaCalls = 0, totalCalls = 0;
        for (JsonNode c : node.path("calls")) {
            totalCalls++;
            ObjectNode co = mapper.createObjectNode();
            co.put("name", text(c.get("target")).isEmpty() ? text(c.get("objectName")) : text(c.get("target")));
            co.put("packageName", text(c.get("packageName")));
            String callType = text(c.get("type"));
            co.put("callType", callType);
            String callSchema = text(c.get("schema"));
            co.put("schemaName", callSchema);
            callsOut.add(co);
            if (!callSchema.isEmpty()) schemas.add(callSchema.toUpperCase(Locale.ROOT));
            boolean external = "EXTERNAL".equals(callType);
            if (external) externalCalls++; else internalCalls++;
            if (external || (!callSchema.isEmpty() && !entrySchemaUpper.isEmpty()
                    && !callSchema.toUpperCase(Locale.ROOT).equals(entrySchemaUpper))) {
                crossSchemaCalls++;
            }
        }

        // callDepth: max usedByNodes[].depth across every subtree table — the deepest point in the
        // subtree this node's own tables are actually touched at, the closest equivalent this shape
        // carries to a call-tree depth walk.
        int callDepth = 0;
        for (JsonNode t : node.path("subtreeTables")) {
            for (JsonNode u : t.path("usedByNodes")) {
                int d = u.path("depth").asInt(0);
                if (d > callDepth) callDepth = d;
            }
        }

        int startLine = node.path("lineStart").asInt(0);
        int endLine = node.path("lineEnd").asInt(0);
        long loc = node.has("linesOfCode") ? node.path("linesOfCode").asLong(0)
                : (endLine > 0 && startLine > 0 ? (endLine - startLine + 1) : 0);

        ObjectNode report = mapper.createObjectNode();
        report.put("id", text(node.get("nodeId")).isEmpty() ? fullName : text(node.get("nodeId")));
        report.put("name", procName);
        report.put("schemaName", schemaName);
        report.put("packageName", packageName);
        report.put("unitType", node.has("objectType") ? text(node.get("objectType")) : "PROCEDURE");
        report.put("callType", "INTERNAL");
        report.put("startLine", startLine);
        report.put("endLine", endLine);
        report.put("loc", loc);
        report.set("directTables", directTables);
        report.set("flowTables", flowTables);
        report.put("tableCount", flowTables.size());
        report.put("writeTableCount", writeTables.size());
        report.put("readTableCount", readTables.size());
        ArrayNode writeOut = report.putArray("writeTables");
        for (String s : writeTables) writeOut.add(s);
        ArrayNode readOut = report.putArray("readTables");
        for (String s : readTables) readOut.add(s);
        ArrayNode allOpsOut = report.putArray("allOps");
        for (String s : allOps) allOpsOut.add(s);
        ArrayNode schemasOut = report.putArray("schemas");
        for (String s : schemas) schemasOut.add(s);
        report.put("crossSchemaCalls", crossSchemaCalls);
        report.put("totalCalls", totalCalls);
        report.put("callDepth", callDepth);
        report.put("internalCalls", internalCalls);
        report.put("externalCalls", externalCalls);
        report.set("parameters", node.has("parameters") ? node.get("parameters") : mapper.createArrayNode());
        report.set("variables", mapper.createArrayNode());
        report.set("statementCounts", node.has("statementSummary") ? node.get("statementSummary") : mapper.createObjectNode());
        report.set("calledBy", mapper.createArrayNode()); // not present in this shape
        report.set("calls", callsOut);
        report.put("sizeCategory",
                com.plsqlanalyzer.web.controller.detail.ProcedureReportBuilder.sizeCategory(loc));
        double[] scoreHolder = new double[1];
        String complexity = com.plsqlanalyzer.web.controller.detail.ProcedureReportBuilder.calcComplexity(
                flowTables.size(), writeTables.size(), crossSchemaCalls, loc, callDepth, totalCalls, scoreHolder);
        report.put("complexity", complexity);
        report.put("_complexityScore", scoreHolder[0]);
        return report;
    }
}
