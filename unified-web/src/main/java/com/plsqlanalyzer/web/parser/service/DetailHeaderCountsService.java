package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * UI-024 — server-side aggregation for `detailHeader.js`'s scope-derived stat counts
 * (`_dhUniqueCallCount`/`_dhUniqueCallCountByType`/`_dhUniqueTriggerCount`/`_dhUniqueCursorCount`
 * for the subtree/subflow branches, `_dhFullCallCountByType`/`_dhFullTriggerCount`/
 * `_dhFullCursorCount` for the full/package-root branches) — seven client functions, each its own
 * `PA.nodeIndex.all()` scan, collapsed into one aggregation pass here.
 *
 * <p>Scope handled by {@link ParserScopeResolver}, exactly like {@link StatementsSummaryService}:
 * `subtree` there already means "everything under, not including self" and `subflow` means "self
 * plus everything under" — precisely the `includeSelf=false`/`includeSelf=true` distinction the
 * client functions took as a parameter, so no new scope logic is invented here.
 */
@Service
public class DetailHeaderCountsService {

    private final AnalysisService service;
    private final ParserScopeResolver scopeResolver;
    private final ObjectMapper mapper;

    public DetailHeaderCountsService(AnalysisService service, ParserScopeResolver scopeResolver, ObjectMapper mapper) {
        this.service = service;
        this.scopeResolver = scopeResolver;
        this.mapper = mapper;
    }

    public ResponseEntity<Object> getCounts(String name, String scope, String procId) {
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

        long intCalls = 0, extCalls = 0, triggers = 0, cursors = 0;
        for (JsonNode n : index.get("nodes")) {
            if (predicate != null && !predicate.test(n)) continue;
            JsonNode counts = n.path("counts");
            intCalls += counts.path("internalCalls").asLong(0);
            extCalls += counts.path("externalCalls").asLong(0);
            cursors += counts.path("cursors").asLong(0);
            if ("TRIGGER".equals(n.path("objectType").asText(""))) triggers++;
        }

        ObjectNode result = mapper.createObjectNode();
        result.put("intCalls", intCalls);
        result.put("extCalls", extCalls);
        result.put("totalCalls", intCalls + extCalls);
        result.put("triggers", triggers);
        result.put("cursors", cursors);
        return ResponseEntity.ok(result);
    }
}
