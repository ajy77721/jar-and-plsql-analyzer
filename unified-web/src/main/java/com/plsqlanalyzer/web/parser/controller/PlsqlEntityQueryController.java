package com.plsqlanalyzer.web.parser.controller;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.entity.FlowEdgeRow;
import com.analyzer.storage.entity.FlowNodeRow;
import com.analyzer.storage.entity.TableUsageRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The PLAN-04 entity queries, over HTTP. <b>PLAN-09 Increment E.</b>
 *
 * <h2>Why this is a new controller and not a change to an existing one</h2>
 *
 * Increment E's hard constraint is that <b>no existing response changes byte for byte</b>. The
 * surest way to keep that is for the entity queries to have no existing response to change: these
 * are new paths, no screen calls them yet, and every endpoint that exists today still answers from
 * exactly the code it answered from before. PLAN-08 §3 owns converting the screens, and each
 * conversion is the moment to point one at these — with a screen to validate it against, which is
 * the thing this increment does not have.
 *
 * <p>So this is the end-to-end proof and the seam for the next increment, in that order. It answers
 * the question the whole database exists for — <b>"which nodes touch table X"</b> — which
 * {@code document(key)} could not answer at all.
 *
 * <h2>{@code indexed} is in the response on purpose</h2>
 *
 * Both repository implementations answer these paths: {@code artifact-scan} by reading every
 * {@code api/nodes/*.json} of the analysis, {@code mongo} by seeking an index. They return the same
 * rows — that is asserted, not hoped — but not in the same time, and a caller has no other way to
 * know which it got. Hiding it would let a deployment sit on the scan path believing it had cut
 * over, which is PLAN-09 §8 R1.
 */
@RestController
@RequestMapping("/api/parser/analyses")
public class PlsqlEntityQueryController {

    private static final Logger log = LoggerFactory.getLogger(PlsqlEntityQueryController.class);

    private final AnalysisRepository repository;

    public PlsqlEntityQueryController(AnalysisRepository repository) {
        this.repository = repository;
    }

    /**
     * PLAN-08 Q43b / L4. Every flow node that uses a table, with the operation it uses it for.
     *
     * @param usage {@code DIRECT}, {@code SUBTREE}, or absent for both. They are different
     *              questions — "whose own statements touch this table" versus "what reaches it at
     *              all" — so the default is not one of them silently
     */
    @GetMapping("/{name}/entities/tables/{table}/nodes")
    public ResponseEntity<?> nodesUsingTable(@PathVariable String name,
                                             @PathVariable String table,
                                             @RequestParam(required = false) String usage) {
        long t0 = System.nanoTime();
        try {
            List<TableUsageRow> rows = repository.nodesUsingTable(name, table, usage);
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("analysis", name);
            body.put("table", table);
            body.put("usage", usage);
            body.put("indexed", repository.indexed());
            body.put("tookMs", (System.nanoTime() - t0) / 1_000_000);
            body.put("count", rows.size());
            body.put("nodes", rows.stream().map(PlsqlEntityQueryController::usageJson).toList());
            return ResponseEntity.ok(body);
        } catch (UnsupportedOperationException e) {
            // The repository has no entity model. Said plainly with a 501 rather than answered by a
            // silent scan: an "indexed" repository quietly scanning is the failure PLAN-09 §8 R1
            // records, and a 200 with the right rows would hide it perfectly.
            return ResponseEntity.status(501).body(Map.of("error", e.getMessage()));
        } catch (IOException e) {
            log.warn("[entity] nodesUsingTable({}, {}) failed: {}", name, table, e.getMessage());
            return ResponseEntity.status(500).body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    /** PLAN-08 Q58 / L21. One flow node's detail row. */
    @GetMapping("/{name}/entities/nodes/{nodeId}")
    public ResponseEntity<?> flowNode(@PathVariable String name, @PathVariable String nodeId) {
        try {
            Optional<FlowNodeRow> node = repository.flowNode(name, nodeId);
            // Absent is a 404 and not an empty object: "this analysis has no such node" and "this
            // node has no detail" render differently, the same distinction AnalysisRepository#exists
            // exists to preserve.
            return node.<ResponseEntity<?>>map(n -> ResponseEntity.ok(nodeJson(n)))
                    .orElseGet(() -> ResponseEntity.notFound().build());
        } catch (UnsupportedOperationException e) {
            return ResponseEntity.status(501).body(Map.of("error", e.getMessage()));
        } catch (IOException e) {
            return ResponseEntity.status(500).body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    /** PLAN-08 Q60 / L23. The calls out of one node — the browser-side node/edge join, server-side. */
    @GetMapping("/{name}/entities/nodes/{nodeId}/edges")
    public ResponseEntity<?> edgesFrom(@PathVariable String name, @PathVariable String nodeId) {
        try {
            List<FlowEdgeRow> edges = repository.edgesFrom(name, nodeId);
            return ResponseEntity.ok(Map.of("analysis", name, "nodeId", nodeId,
                    "indexed", repository.indexed(), "count", edges.size(),
                    "edges", edges.stream().map(PlsqlEntityQueryController::edgeJson).toList()));
        } catch (UnsupportedOperationException e) {
            return ResponseEntity.status(501).body(Map.of("error", e.getMessage()));
        } catch (IOException e) {
            return ResponseEntity.status(500).body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    /* ─────────────────── projections ───────────────────
     * Written by hand rather than serialised from the record, so that a field added to the entity
     * model is not silently added to a response. The wire shape is a decision; the record is a
     * storage row. */

    private static Map<String, Object> usageJson(TableUsageRow u) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodeId", u.nodeId());
        m.put("nodeName", u.nodeName());
        m.put("tableKey", u.tableKey());
        m.put("schema", u.tableSchema());
        m.put("table", u.tableName());
        m.put("objectType", u.objectType());
        m.put("operation", u.operation());
        m.put("usage", u.usage());
        m.put("lines", u.lines());
        return m;
    }

    private static Map<String, Object> nodeJson(FlowNodeRow n) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodeId", n.nodeId());
        m.put("name", n.name());
        m.put("schema", n.schema());
        m.put("objectType", n.objectType());
        m.put("depth", n.depth());
        m.put("lineStart", n.lineStart());
        m.put("lineEnd", n.lineEnd());
        m.put("linesOfCode", n.linesOfCode());
        m.put("sourceFile", n.sourceFile());
        m.put("parameters", n.parameters().stream().map(p -> {
            Map<String, Object> pm = new LinkedHashMap<>();
            pm.put("ordinal", p.ordinal());
            pm.put("name", p.name());
            pm.put("dataType", p.dataType());
            pm.put("direction", p.direction());
            pm.put("line", p.line());
            return pm;
        }).toList());
        m.put("statements", n.statements().stream().map(s -> {
            Map<String, Object> sm = new LinkedHashMap<>();
            sm.put("statementType", s.statementType());
            sm.put("count", s.count());
            return sm;
        }).toList());
        return m;
    }

    private static Map<String, Object> edgeJson(FlowEdgeRow e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("toNodeId", e.toNodeId());
        m.put("toSchema", e.toSchema());
        m.put("toObject", e.toObject());
        m.put("packageName", e.packageName());
        m.put("callType", e.callType());
        m.put("argCount", e.argCount());
        m.put("count", e.count());
        m.put("lines", e.lines());
        return m;
    }
}
