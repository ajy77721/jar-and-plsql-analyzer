package com.plsqlanalyzer.web.controller.detail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * UI-030a — server-side port of {@code plsql/js/summary-analyzer.js:_analyzeProcedure}, for the
 * live-crawl backend's detail shape ({@link ProcedureDetailService#getProcedureDetail}) — chosen
 * over the parser backend because it is the one {@code _analyzeProcedure} actually works against
 * today (§82: the parser backend's own node-detail shape breaks it, tracked separately as
 * UI-030-BUG).
 *
 * <h2>The call-tree table walk is dead weight, confirmed structurally, not assumed</h2>
 *
 * <p>{@code _analyzeProcedure} builds {@code flowTables} two ways and merges them: a walk over
 * {@code detail.callTree} reading {@code node.nodeTables || node.tables}, and a direct pass over
 * {@code detail.tables} into {@code directTables}. Checked {@link
 * com.plsqlanalyzer.analyzer.graph.CallGraph#getCallTree}'s own tree-node construction: it never
 * sets a {@code nodeTables} or {@code tables} field on any node — only {@code id}/{@code name}/
 * {@code schemaName}/{@code callType}/line info/{@code children}. So the call-tree table walk
 * contributes nothing on this backend either; {@code flowTables} in practice is always exactly
 * {@code detail.tables} (which {@link ProcedureDetailService} already computes subtree-scoped, via
 * its own {@code treeNodeIds} walk — the reason this class does not re-walk the tree for tables).
 * This class ports the union to a single pass over {@code detail.tables}, not the JS's two
 * redundant ones, and sets {@code directTables} to the identical map {@code flowTables} uses,
 * matching {@code summary-tables.js}/{@code summary-helpers.js}'s own downstream reads of
 * {@code r.directTables} exactly as they behave today (structurally identical to
 * {@code r.flowTables}, since nothing ever diverges the two in the live JS).
 *
 * <h2>What the call-tree walk IS still needed for</h2>
 *
 * <p>{@code crossSchemaCalls}/{@code totalCalls}/{@code callDepth}/{@code schemas} read
 * {@code node.schemaName}/{@code node.callType}, both of which the tree genuinely carries — this
 * walk is real and is ported faithfully, over {@code detail.callTree}'s actual shape.
 */
public final class ProcedureReportBuilder {

    private ProcedureReportBuilder() {
    }

    private static String text(JsonNode n) {
        return n == null || n.isNull() ? "" : n.asText("");
    }

    private static final Set<String> WRITE_OPS = Set.of("INSERT", "UPDATE", "DELETE", "MERGE");

    private static final class TableEntry {
        String name;
        String schemaName;
        boolean external;
        final Set<String> operations = new LinkedHashSet<>();
    }

    /**
     * @param detail       the exact shape {@link ProcedureDetailService#getProcedureDetail} returns
     * @param entrySchema  {@code analysisData.entrySchema} — upper-cased before comparison, matching
     *                     the JS's own {@code (analysisData && analysisData.entrySchema || '').toUpperCase()}
     */
    public static ObjectNode build(ObjectMapper mapper, JsonNode detail, String entrySchema) {
        String entrySchemaUpper = (entrySchema == null ? "" : entrySchema).toUpperCase(Locale.ROOT);

        // flowTables == directTables: both built from detail.tables, the ONLY real source (see
        // class javadoc for why the call-tree table walk contributes nothing).
        Map<String, TableEntry> tables = new LinkedHashMap<>();
        JsonNode tablesSrc = detail.get("tables");
        if (tablesSrc != null && tablesSrc.isArray()) {
            for (JsonNode t : tablesSrc) {
                String name = text(t.get("tableName")).toUpperCase(Locale.ROOT);
                if (name.isEmpty()) continue;
                TableEntry entry = tables.computeIfAbsent(name, k -> {
                    TableEntry e = new TableEntry();
                    e.name = name;
                    e.schemaName = text(t.get("schemaName"));
                    e.external = t.path("external").asBoolean(false);
                    return e;
                });
                JsonNode ops = t.get("operations");
                if (ops != null && ops.isArray()) {
                    for (JsonNode op : ops) entry.operations.add(op.asText("").toUpperCase(Locale.ROOT));
                }
            }
        }

        // Walk the call tree for schema-crossing stats only — the part that's real (§ above).
        Set<String> schemas = new LinkedHashSet<>();
        schemas.add(text(detail.get("schemaName")).toUpperCase(Locale.ROOT));
        long[] totalCalls = {0};
        int[] callDepth = {0};
        long[] crossSchemaCalls = {0};
        JsonNode callTree = detail.get("callTree");
        if (callTree != null && callTree.isObject()) {
            walk(callTree, 0, entrySchemaUpper, schemas, totalCalls, callDepth, crossSchemaCalls);
        }

        // internalCalls/externalCalls: the flat calls list, not the tree.
        long internalCalls = 0, externalCalls = 0;
        JsonNode calls = detail.get("calls");
        if (calls != null && calls.isArray()) {
            for (JsonNode c : calls) {
                if ("EXTERNAL".equals(text(c.get("callType")))) externalCalls++;
                else internalCalls++;
            }
        }

        int startLine = detail.path("startLine").asInt(0);
        int endLine = detail.path("endLine").asInt(0);
        long loc = (endLine > 0 && startLine > 0) ? (endLine - startLine + 1) : 0;

        List<String> writeTables = new ArrayList<>();
        List<String> readTables = new ArrayList<>();
        Set<String> allOps = new LinkedHashSet<>();
        for (TableEntry t : tables.values()) {
            boolean hasWrite = false;
            for (String op : t.operations) {
                allOps.add(op);
                if (WRITE_OPS.contains(op)) hasWrite = true;
            }
            if (hasWrite) writeTables.add(t.name);
            if (t.operations.contains("SELECT")) readTables.add(t.name);
        }

        ObjectNode tablesOut = mapper.createObjectNode();
        for (TableEntry t : tables.values()) {
            ObjectNode to = mapper.createObjectNode();
            to.put("name", t.name);
            to.put("schemaName", t.schemaName);
            ArrayNode opsOut = to.putArray("operations");
            for (String op : t.operations) opsOut.add(op);
            to.put("external", t.external);
            tablesOut.set(t.name, to);
        }

        ObjectNode report = mapper.createObjectNode();
        report.put("id", text(detail.get("id")).isEmpty() ? text(detail.get("name")) : text(detail.get("id")));
        report.put("name", text(detail.get("name")));
        report.put("schemaName", text(detail.get("schemaName")));
        report.put("packageName", text(detail.get("packageName")));
        report.put("unitType", detail.has("unitType") && !detail.get("unitType").isNull()
                ? text(detail.get("unitType")) : "PROCEDURE");
        report.put("callType", detail.has("callType") && !detail.get("callType").isNull()
                ? text(detail.get("callType")) : "INTERNAL");
        report.put("startLine", startLine);
        report.put("endLine", endLine);
        report.put("loc", loc);
        report.set("directTables", tablesOut);
        report.set("flowTables", tablesOut);
        report.put("tableCount", tables.size());
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
        report.put("crossSchemaCalls", crossSchemaCalls[0]);
        report.put("totalCalls", totalCalls[0]);
        report.put("callDepth", callDepth[0]);
        report.put("internalCalls", internalCalls);
        report.put("externalCalls", externalCalls);
        report.set("parameters", detail.has("parameters") ? detail.get("parameters") : mapper.createArrayNode());
        report.set("variables", detail.has("variables") ? detail.get("variables") : mapper.createArrayNode());
        report.set("statementCounts", detail.has("statementCounts") ? detail.get("statementCounts") : mapper.createObjectNode());
        report.set("calledBy", detail.has("calledBy") ? detail.get("calledBy") : mapper.createArrayNode());
        report.set("calls", detail.has("calls") ? detail.get("calls") : mapper.createArrayNode());
        report.put("sizeCategory", sizeCategory(loc));
        double[] scoreHolder = new double[1];
        String complexity = calcComplexity(tables.size(), writeTables.size(), crossSchemaCalls[0], loc,
                callDepth[0], totalCalls[0], scoreHolder);
        report.put("complexity", complexity);
        report.put("_complexityScore", scoreHolder[0]);
        return report;
    }

    private static void walk(JsonNode node, int depth, String entrySchemaUpper, Set<String> schemas,
                             long[] totalCalls, int[] callDepth, long[] crossSchemaCalls) {
        totalCalls[0]++;
        if (depth > callDepth[0]) callDepth[0] = depth;
        String nodeSchema = text(node.get("schemaName")).toUpperCase(Locale.ROOT);
        if (!nodeSchema.isEmpty()) schemas.add(nodeSchema);
        String callType = text(node.get("callType"));
        if ("EXTERNAL".equals(callType)
                || (!nodeSchema.isEmpty() && !nodeSchema.equals(entrySchemaUpper) && !entrySchemaUpper.isEmpty())) {
            crossSchemaCalls[0]++;
        }
        JsonNode children = node.get("children");
        if (children != null && children.isArray()) {
            for (JsonNode child : children) walk(child, depth + 1, entrySchemaUpper, schemas, totalCalls, callDepth, crossSchemaCalls);
        }
    }

    /** {@code _sizeCategory}, transcribed. */
    public static String sizeCategory(long loc) {
        if (loc <= 50) return "S";
        if (loc <= 200) return "M";
        if (loc <= 500) return "L";
        return "XL";
    }

    /** {@code _calcComplexity}, transcribed. {@code scoreOut[0]} receives {@code _complexityScore}. */
    public static String calcComplexity(long tableCount, long writeTableCount, long crossSchemaCalls, long loc,
                                 int callDepth, long totalCalls, double[] scoreOut) {
        double score = 0;
        score += tableCount * 1;
        score += writeTableCount * 2;
        score += crossSchemaCalls * 3;
        score += loc > 200 ? 2 : 0;
        score += callDepth > 5 ? 2 : 0;
        score += totalCalls > 20 ? 1 : 0;
        scoreOut[0] = Math.round(score * 10) / 10.0;
        if (score <= 4) return "Low";
        if (score <= 10) return "Medium";
        return "High";
    }
}
