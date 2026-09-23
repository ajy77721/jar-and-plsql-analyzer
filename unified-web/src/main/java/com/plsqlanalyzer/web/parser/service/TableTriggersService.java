package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.Locale;

/**
 * UI-024 — server-side filter for `tableDetail.js`'s step 4 ("merge triggers from call tree nodes
 * / parsed source triggers"), which used to scan the whole `PA.nodeIndex.all()` for `TRIGGER`-type
 * nodes whose `triggerTable` matches the current table.
 *
 * <p>Deliberately narrow: this only replaces the expensive full-corpus scan-and-filter-by-table
 * step. The client's own subsequent filtering — intersecting with `PA.context.callTreeNodeIds`
 * (the current scope's node-id set) and de-duplicating against triggers the tables-API step (its
 * own step 3) already found — is left exactly as it was. That logic isn't a scan (it's a `Set.has`
 * per candidate, over a list already narrowed to one table's triggers) and its semantics were not
 * independently re-verified against real data before this change, per the caution already recorded
 * in the registry for this file — narrowing what reaches the client is safe either way, since the
 * client still makes the same accept/reject decision it always did on each candidate.
 */
@Service
public class TableTriggersService {

    private final AnalysisService service;
    private final ObjectMapper mapper;

    public TableTriggersService(AnalysisService service, ObjectMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    public ResponseEntity<Object> getTriggersForTable(String name, String tableName) {
        if (tableName == null || tableName.isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        JsonNode index;
        try {
            index = service.getIndex(name);
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
        if (index == null || !index.has("nodes") || !index.get("nodes").isArray()) {
            return ResponseEntity.notFound().build();
        }

        String upperTable = tableName.toUpperCase(Locale.ROOT);
        ArrayNode result = mapper.createArrayNode();
        for (JsonNode n : index.get("nodes")) {
            if (!"TRIGGER".equals(n.path("objectType").asText(""))) continue;
            String trigTable = n.path("triggerTable").asText("").toUpperCase(Locale.ROOT);
            if (trigTable.isEmpty() || !trigTable.equals(upperTable)) continue;

            ObjectNode t = mapper.createObjectNode();
            t.put("nodeId", n.path("nodeId").asText(""));
            t.put("name", !n.path("name").asText("").isEmpty() ? n.path("name").asText() : n.path("objectName").asText(""));
            t.put("objectName", n.path("objectName").asText(""));
            t.put("schema", n.path("schema").asText(""));
            t.put("triggerTiming", n.path("triggerTiming").asText(""));
            t.put("triggerEvent", n.path("triggerEvent").asText(""));
            result.add(t);
        }
        return ResponseEntity.ok(result);
    }
}
