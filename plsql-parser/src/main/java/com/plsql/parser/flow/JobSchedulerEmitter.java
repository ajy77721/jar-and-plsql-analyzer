package com.plsql.parser.flow;

import com.plsql.parser.model.CallInfo;
import com.plsql.parser.model.FlowEdge;
import com.plsql.parser.model.FlowNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Turns discovered scheduled jobs into first-class call-tree nodes and edges, after the routine
 * crawl — the counterpart of {@link DependencyCrawler}'s trigger emission.
 *
 * <p>Each job becomes a {@link FlowNode} with {@code objectType="JOB"} (or {@code "EVENT"} for
 * MySQL), carrying its schedule, enabled flag and target. When the target routine is present in
 * the crawl, an additive {@code SCHEDULES} edge is emitted job → target — so a scheduled job is
 * no longer an invisible root: it is a node whose target is reachable and whose outbound calls are
 * the ones the job's ACTION actually makes.
 *
 * <p>Purely additive: normal CALL edges and nodes are untouched, and an estate with no scheduler
 * catalog simply produces nothing here.
 */
public final class JobSchedulerEmitter {

    private static final Logger log = LoggerFactory.getLogger(JobSchedulerEmitter.class);

    private JobSchedulerEmitter() {}

    /**
     * Emit a node per job (plus a SCHEDULES edge when the target is in {@code knownObjects}) and
     * return the facet rows for {@code api/jobs/index.json}.
     *
     * @param knownObjects uppercase visit keys of the crawled routines ("PKG.SUB" or "NAME")
     */
    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> emit(List<Map<String, Object>> jobs,
                                                 ChunkedFlowWriter writer,
                                                 Set<String> knownObjects,
                                                 List<String> errors,
                                                 int startOrder) {
        List<Map<String, Object>> rows = new ArrayList<>();
        if (jobs == null || jobs.isEmpty() || writer == null) return rows;
        int order = startOrder;
        int edgeCount = 0;

        for (Map<String, Object> job : jobs) {
            String name = str(job.get("name"));
            if (name == null || name.isBlank()) continue;
            String schema = str(job.get("schema"));
            String type = str(job.get("type"));
            if (type == null || type.isBlank()) type = "JOB";
            String schedule = str(job.get("schedule"));
            Boolean enabled = (Boolean) job.get("enabled");
            String target = str(job.get("target"));
            String targetSchema = str(job.get("targetSchema"));
            String action = str(job.get("action"));
            List<CallInfo> calls = (List<CallInfo>) job.get("calls");

            FlowNode node = new FlowNode();
            node.setSchema(schema);
            // Namespace the job away from routines of the same name, exactly as triggers use "TRG".
            node.setPackageName("EVENT".equals(type) ? "EVENT" : "JOB");
            node.setObjectName(name);
            node.setObjectType(type);
            node.setSchedule(schedule);
            node.setJobEnabled(enabled);
            node.setJobTarget(target);
            node.setCalls(calls == null ? new ArrayList<>() : calls);
            node.setCallCount(node.getCalls().size());
            node.setReadable(true);
            node.setDepth(1);
            node.generateNodeId();
            String jobId = node.getNodeId();
            node.setOrder(++order);
            try {
                if (action != null && !action.isBlank()) {
                    int lines = action.split("\\r?\\n").length;
                    node.setLineStart(1);
                    node.setLineEnd(lines);
                    node.setLinesOfCode(lines);
                    writer.writeSourceFile((schema != null ? schema : "") + "." + type + "."
                            + name + ".sql", action);
                }
                writer.writeChunk(node);
            } catch (IOException e) {
                errors.add("Job node write failed for " + name + ": " + e.getMessage());
            }

            String targetNodeId = null;
            if (target != null && !target.isBlank() && isKnown(target, knownObjects)) {
                String[] parts = splitTarget(target);
                targetNodeId = buildNodeId(targetSchema != null ? targetSchema : schema,
                        parts[0], parts[1]);
                FlowEdge edge = new FlowEdge();
                edge.setFromNodeId(jobId);
                edge.setFromSchema(schema);
                edge.setFromPackage(node.getPackageName());
                edge.setFromObject(name);
                edge.setToNodeId(targetNodeId);
                edge.setToSchema(targetSchema != null ? targetSchema : schema);
                edge.setToPackage(parts[0]);
                edge.setToObject(parts[1]);
                edge.setEdgeType("SCHEDULES");
                edge.setDepth(2);
                try {
                    writer.appendEdgesAuto(List.of(edge));
                    edgeCount++;
                } catch (IOException e) {
                    errors.add("SCHEDULES edge write failed for " + name + ": " + e.getMessage());
                }
            }

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("name", name);
            row.put("schema", schema);
            row.put("type", type);
            row.put("schedule", schedule);
            row.put("enabled", enabled);
            row.put("target", target);
            row.put("targetSchema", targetSchema);
            row.put("targetNodeId", targetNodeId);
            row.put("nodeId", jobId);
            row.put("callCount", node.getCalls().size());
            rows.add(row);
        }
        log.info("[Crawler] Emitted {} scheduled job node(s), {} SCHEDULES edge(s)", rows.size(), edgeCount);
        return rows;
    }

    private static boolean isKnown(String target, Set<String> knownObjects) {
        if (knownObjects == null || knownObjects.isEmpty()) return false;
        String upper = target.toUpperCase(Locale.ROOT);
        String last = upper.contains(".") ? upper.substring(upper.lastIndexOf('.') + 1) : upper;
        for (String k : knownObjects) {
            String ku = k.toUpperCase(Locale.ROOT);
            if (ku.equals(upper) || ku.endsWith("." + upper) || ku.equals(last)
                    || ku.endsWith("." + last)) {
                return true;
            }
        }
        return false;
    }

    /** {@code [package, object]}; package is null for a bare name. */
    private static String[] splitTarget(String target) {
        if (target == null) return new String[]{null, null};
        String t = target.replace("\"", "").trim();
        int dot = t.lastIndexOf('.');
        if (dot > 0 && dot < t.length() - 1) {
            return new String[]{t.substring(0, dot), t.substring(dot + 1)};
        }
        return new String[]{null, t};
    }

    private static String buildNodeId(String schema, String pkg, String object) {
        StringBuilder sb = new StringBuilder();
        if (schema != null && !schema.isEmpty()) sb.append(schema);
        if (pkg != null && !pkg.isEmpty()) sb.append("$").append(pkg);
        if (object != null && !object.isEmpty()) sb.append("$").append(object);
        return sb.toString().toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9_$]", "_");
    }

    private static String str(Object o) { return o == null ? null : o.toString(); }
}
