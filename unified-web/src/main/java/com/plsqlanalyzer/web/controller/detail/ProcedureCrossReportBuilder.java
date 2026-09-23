package com.plsqlanalyzer.web.controller.detail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * UI-030b — Java port of {@code plsql/js/summary-analyzer.js}'s four cross-procedure report
 * builders (`_buildTransactionReport`/`_buildSchemaSliceReport`/`_buildExternalDepsReport`/
 * `_buildBatchReport`), structurally close to `EndpointCrossReportBuilder` (§69) on the JAR side.
 *
 * <p>Each method takes the array of {@link ProcedureReportBuilder#build} outputs (as
 * {@code ArrayNode}, the same shape UI-030c's endpoint will assemble) and produces one of the four
 * aggregate reports, transcribed field-for-field from the JS.
 */
public final class ProcedureCrossReportBuilder {

    private ProcedureCrossReportBuilder() {
    }

    private static String text(JsonNode n) {
        return n == null || n.isNull() ? "" : n.asText("");
    }

    private static List<String> strings(JsonNode arr) {
        List<String> out = new ArrayList<>();
        if (arr != null && arr.isArray()) for (JsonNode n : arr) out.add(n.asText(""));
        return out;
    }

    private static final Set<String> WRITE_OPS = Set.of("INSERT", "UPDATE", "DELETE", "MERGE");

    /** {@code _buildTransactionReport}: detect multi-table DML in the same call tree. */
    public static ArrayNode buildTransactionReport(ObjectMapper mapper, JsonNode procReports) {
        ArrayNode results = mapper.createArrayNode();
        for (JsonNode r : procReports) {
            Map<String, List<String>> schemaWrites = new LinkedHashMap<>();
            JsonNode flowTables = r.get("flowTables");
            if (flowTables != null) {
                var it = flowTables.fields();
                while (it.hasNext()) {
                    var entry = it.next();
                    String name = entry.getKey();
                    JsonNode t = entry.getValue();
                    boolean hasWrite = false;
                    for (JsonNode op : t.path("operations")) {
                        if (WRITE_OPS.contains(op.asText("").toUpperCase(Locale.ROOT))) { hasWrite = true; break; }
                    }
                    if (hasWrite) {
                        String schema = !text(t.get("schemaName")).isEmpty() ? text(t.get("schemaName"))
                                : (!text(r.get("schemaName")).isEmpty() ? text(r.get("schemaName")) : "DEFAULT");
                        schemaWrites.computeIfAbsent(schema, k -> new ArrayList<>()).add(name);
                    }
                }
            }
            List<String> writeSchemas = new ArrayList<>(schemaWrites.keySet());
            List<String> allWriteTables = new ArrayList<>();
            for (List<String> v : schemaWrites.values()) allWriteTables.addAll(v);

            String txnReq;
            if (allWriteTables.size() > 1) {
                txnReq = writeSchemas.size() > 1
                        ? "REQUIRED - cross-schema multi-table DML (" + String.join(", ", writeSchemas) + ")"
                        : "RECOMMENDED - multi-table DML in single schema";
            } else if (allWriteTables.size() == 1) {
                txnReq = "SINGLE TABLE";
            } else {
                txnReq = "READ ONLY";
            }

            ObjectNode row = mapper.createObjectNode();
            row.put("procName", text(r.get("name")));
            row.put("procId", text(r.get("id")));
            row.put("schemaName", text(r.get("schemaName")));
            row.put("packageName", text(r.get("packageName")));
            row.put("unitType", text(r.get("unitType")));
            row.put("transactionRequirement", txnReq);
            ArrayNode ws = row.putArray("writeSchemas");
            for (String s : writeSchemas) ws.add(s);
            ArrayNode wt = row.putArray("writeTables");
            for (String s : allWriteTables) wt.add(s);
            row.put("totalTables", r.path("tableCount").asInt(0));
            row.put("loc", r.path("loc").asLong(0));
            row.put("complexity", text(r.get("complexity")));
            results.add(row);
        }
        return results;
    }

    /** {@code _buildSchemaSliceReport}: cross-schema analysis, equivalent to JAR verticalisation. */
    public static ArrayNode buildSchemaSliceReport(ObjectMapper mapper, JsonNode procReports) {
        Map<String, Set<String>> procedures = new LinkedHashMap<>();
        Map<String, Set<String>> tables = new LinkedHashMap<>();
        Map<String, Set<String>> operations = new LinkedHashMap<>();
        Map<String, Integer> callCount = new LinkedHashMap<>();

        for (JsonNode r : procReports) {
            for (String schema : strings(r.get("schemas"))) {
                procedures.computeIfAbsent(schema, k -> new LinkedHashSet<>()).add(text(r.get("name")));
                callCount.merge(schema, 1, Integer::sum);
                JsonNode flowTables = r.get("flowTables");
                if (flowTables != null) {
                    var it = flowTables.fields();
                    while (it.hasNext()) {
                        var entry = it.next();
                        JsonNode t = entry.getValue();
                        String tSchema = text(t.get("schemaName")).toUpperCase(Locale.ROOT);
                        if (tSchema.equals(schema) || tSchema.isEmpty()) {
                            tables.computeIfAbsent(schema, k -> new LinkedHashSet<>()).add(entry.getKey());
                            for (JsonNode op : t.path("operations")) {
                                operations.computeIfAbsent(schema, k -> new LinkedHashSet<>()).add(op.asText(""));
                            }
                        }
                    }
                }
            }
        }

        List<String> schemasSorted = new ArrayList<>(procedures.keySet());
        schemasSorted.sort(Comparator.comparingInt((String s) -> callCount.getOrDefault(s, 0)).reversed());

        ArrayNode results = mapper.createArrayNode();
        for (String schema : schemasSorted) {
            ObjectNode row = mapper.createObjectNode();
            row.put("schema", schema);
            ArrayNode procs = row.putArray("procedures");
            for (String p : procedures.getOrDefault(schema, Set.of())) procs.add(p);
            ArrayNode tbls = row.putArray("tables");
            for (String t : tables.getOrDefault(schema, Set.of())) tbls.add(t);
            ArrayNode ops = row.putArray("operations");
            for (String o : operations.getOrDefault(schema, Set.of())) ops.add(o);
            row.put("callCount", callCount.getOrDefault(schema, 0));
            results.add(row);
        }
        return results;
    }

    /** {@code _buildExternalDepsReport}: external schema dependencies. */
    public static ArrayNode buildExternalDepsReport(ObjectMapper mapper, JsonNode procReports) {
        Map<String, Set<String>> procedures = new LinkedHashMap<>();
        Map<String, Set<String>> callers = new LinkedHashMap<>();
        Map<String, Set<String>> packages = new LinkedHashMap<>();
        Map<String, Integer> count = new LinkedHashMap<>();

        for (JsonNode r : procReports) {
            for (JsonNode call : r.path("calls")) {
                if (!"EXTERNAL".equals(text(call.get("callType")))) continue;
                String schema = text(call.get("schemaName"));
                schema = (schema.isEmpty() ? "EXTERNAL" : schema).toUpperCase(Locale.ROOT);
                String callName = !text(call.get("name")).isEmpty() ? text(call.get("name")) : text(call.get("id"));
                if (callName.isEmpty()) callName = "?";
                procedures.computeIfAbsent(schema, k -> new LinkedHashSet<>()).add(callName);
                callers.computeIfAbsent(schema, k -> new LinkedHashSet<>()).add(text(r.get("name")));
                if (!text(call.get("packageName")).isEmpty()) {
                    packages.computeIfAbsent(schema, k -> new LinkedHashSet<>()).add(text(call.get("packageName")));
                }
                count.merge(schema, 1, Integer::sum);
            }
        }

        List<String> schemasSorted = new ArrayList<>(count.keySet());
        schemasSorted.sort(Comparator.comparingInt((String s) -> count.getOrDefault(s, 0)).reversed());

        ArrayNode results = mapper.createArrayNode();
        for (String schema : schemasSorted) {
            ObjectNode row = mapper.createObjectNode();
            row.put("schema", schema);
            ArrayNode procs = row.putArray("procedures");
            for (String p : procedures.getOrDefault(schema, Set.of())) procs.add(p);
            ArrayNode cs = row.putArray("callers");
            for (String c : callers.getOrDefault(schema, Set.of())) cs.add(c);
            ArrayNode pkgs = row.putArray("packages");
            for (String p : packages.getOrDefault(schema, Set.of())) pkgs.add(p);
            row.put("count", count.getOrDefault(schema, 0));
            results.add(row);
        }
        return results;
    }

    private static final String[] BATCH_KEYWORDS = {
            "DBMS_SCHEDULER", "DBMS_JOB", "SCHEDULE", "CRON", "BATCH", "NIGHTLY", "DAILY", "WEEKLY"
    };

    /** {@code _buildBatchReport}: detect DBMS_SCHEDULER/DBMS_JOB patterns. */
    public static ArrayNode buildBatchReport(ObjectMapper mapper, JsonNode procReports) {
        ArrayNode results = mapper.createArrayNode();
        for (JsonNode r : procReports) {
            String text = ((text(r.get("name")) + " " + text(r.get("packageName")))).toUpperCase(Locale.ROOT);
            boolean isBatch = false;
            String matchedKeyword = "";
            for (String kw : BATCH_KEYWORDS) {
                if (text.contains(kw)) { isBatch = true; matchedKeyword = kw; break; }
            }
            if (!isBatch) {
                outer:
                for (JsonNode call : r.path("calls")) {
                    String callText = (text(call.get("name")) + " " + text(call.get("packageName"))).toUpperCase(Locale.ROOT);
                    for (String kw : BATCH_KEYWORDS) {
                        if (callText.contains(kw)) { isBatch = true; matchedKeyword = kw; break outer; }
                    }
                }
            }
            if (!isBatch) continue;

            ObjectNode row = mapper.createObjectNode();
            row.put("procName", text(r.get("name")));
            row.put("procId", text(r.get("id")));
            row.put("schemaName", text(r.get("schemaName")));
            row.put("packageName", text(r.get("packageName")));
            row.put("matchedKeyword", matchedKeyword);
            row.put("tableCount", r.path("tableCount").asInt(0));
            row.set("writeTables", r.has("writeTables") ? r.get("writeTables") : mapper.createArrayNode());
            row.put("loc", r.path("loc").asLong(0));
            row.put("complexity", text(r.get("complexity")));
            row.set("schemas", r.has("schemas") ? r.get("schemas") : mapper.createArrayNode());
            results.add(row);
        }
        return results;
    }
}
