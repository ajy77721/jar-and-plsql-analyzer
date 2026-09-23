package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;

/**
 * UI-024 — server-side aggregation for {@code parser/js/statements.js}'s statement-type summary
 * (`_aggregate`/`_aggregateScoped`), which used to scan {@code PA.nodeIndex.all()} in the browser
 * twice (once unscoped, once per scope change). Every field this needs
 * (`counts.statements`/`nodeId`/`name`/`schema`/`sourceFile`) is already present on each index node
 * — the same source the client scan read — so this is a pure aggregation move, not a new data need.
 *
 * <p>Scope handled by {@link ParserScopeResolver} — the exact same component already used by the
 * paged facet tables (`tableOps.js`/`exHandlers.js`), reproducing `parser/js/scope.js`'s semantics
 * server-side rather than inventing new ones.
 */
@Service
public class StatementsSummaryService {

    private final AnalysisService service;
    private final ParserScopeResolver scopeResolver;
    private final ObjectMapper mapper;

    public StatementsSummaryService(AnalysisService service, ParserScopeResolver scopeResolver, ObjectMapper mapper) {
        this.service = service;
        this.scopeResolver = scopeResolver;
        this.mapper = mapper;
    }

    public ResponseEntity<Object> getSummary(String name, String scope, String procId) {
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

        Map<String, ObjectNode> typeMap = new LinkedHashMap<>();
        Map<String, List<ObjectNode>> procsByType = new LinkedHashMap<>();

        for (JsonNode n : index.get("nodes")) {
            if (predicate != null && !predicate.test(n)) continue;
            JsonNode stmts = n.path("counts").path("statements");
            if (!stmts.isObject()) continue;

            String nodeId = n.path("nodeId").asText("");
            String nodeName = !n.path("name").asText("").isEmpty() ? n.path("name").asText() : nodeId;
            String schema = n.path("schema").asText("");
            String sourceFile = n.path("sourceFile").asText("");

            var fields = stmts.fields();
            while (fields.hasNext()) {
                var e = fields.next();
                String stype = e.getKey().toUpperCase(Locale.ROOT);
                long count = e.getValue().asLong(0);
                if (count <= 0) continue;

                ObjectNode entry = typeMap.get(stype);
                if (entry == null) {
                    entry = mapper.createObjectNode();
                    entry.put("type", stype);
                    entry.put("totalCount", 0L);
                    entry.put("procCount", 0);
                    typeMap.put(stype, entry);
                    procsByType.put(stype, new ArrayList<>());
                }
                entry.put("totalCount", entry.path("totalCount").asLong(0) + count);
                entry.put("procCount", entry.path("procCount").asInt(0) + 1);

                ObjectNode proc = mapper.createObjectNode();
                proc.put("nodeId", nodeId);
                proc.put("name", nodeName);
                proc.put("schema", schema);
                proc.put("sourceFile", sourceFile);
                proc.put("count", count);
                procsByType.get(stype).add(proc);
            }
        }

        ArrayNode result = mapper.createArrayNode();
        for (Map.Entry<String, ObjectNode> e : typeMap.entrySet()) {
            List<ObjectNode> procs = procsByType.get(e.getKey());
            procs.sort((a, b) -> Long.compare(b.path("count").asLong(0), a.path("count").asLong(0)));
            ArrayNode procsOut = e.getValue().putArray("procs");
            for (ObjectNode p : procs) procsOut.add(p);
            result.add(e.getValue());
        }
        return ResponseEntity.ok(result);
    }
}
