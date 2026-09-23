package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsql.parser.model.FlowNode;
import com.plsql.parser.model.TableOperationInfo;
import com.plsqlanalyzer.parser.model.WhereFilter;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MID-021 (§138) — {@code TableOperationInfo.whereFilters} (always produced by the analyser, per its
 * own doc comment) was dropped in {@code writeTableIndex}'s grouping step, so {@code api/tables
 * /index.json}'s {@code usedBy[]} entries never carried predicate data — the real reason
 * {@code predicates.js}'s parser-backend fallback has always produced zero rows, confirmed against
 * real on-disk data in §109/§110. This pins the fix: {@code usedBy[].predicates} now carries every
 * distinct {@code WhereFilter} for that table, deduplicated across repeated {@code
 * TableOperationInfo} entries for the same table.
 */
class TableIndexPredicatesTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private TableOperationInfo op(String table, String operation, int line, WhereFilter... filters) {
        TableOperationInfo t = new TableOperationInfo();
        t.setTableName(table);
        t.setSchema("ERP");
        t.setOperation(operation);
        t.setLine(line);
        if (filters.length > 0) t.setWhereFilters(List.of(filters));
        return t;
    }

    private JsonNode writeAndReadTables(FlowNode node, String buildId) throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ArtifactKey build = ArtifactKey.of(buildId);
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, build);
        ArtifactKey apiDir = ArtifactKey.of(buildId, "api");

        // MID-026: writeTableIndex now takes the lightweight LightNode projection, not FlowNode
        // directly (see ChunkedFlowWriter's own doc comment on that record) — convert via the
        // writer's own toLightNodes(), the same projection the real call site uses, rather than
        // duplicating its field-mapping logic here.
        Method toLightNodes = ChunkedFlowWriter.class.getDeclaredMethod("toLightNodes", List.class);
        toLightNodes.setAccessible(true);
        Object lightNodes = toLightNodes.invoke(null, List.of(node));

        Method m = ChunkedFlowWriter.class.getDeclaredMethod(
                "writeTableIndex", ArtifactKey.class, List.class);
        m.setAccessible(true);
        m.invoke(writer, apiDir, lightNodes);

        try (var in = store.read(apiDir.resolve("tables", "index.json"))) {
            return JSON.readTree(in);
        }
    }

    @Test
    void usedByCarriesThePredicateWhenOneExists() throws Exception {
        WhereFilter f = new WhereFilter("REGION_ID", "=", "p_id", 7);
        FlowNode node = new FlowNode();
        node.setObjectName("FN_REGION_NAME");
        node.setDepth(0);
        node.setTableOperations(List.of(op("REF_REGION", "SELECT", 7, f)));

        JsonNode doc = writeAndReadTables(node, "b-pred-1");
        JsonNode usage = doc.get("tables").get(0).get("usedBy").get(0);
        JsonNode preds = usage.get("predicates");
        assertNotNull(preds, "predicates must be present when a WhereFilter exists");
        assertEquals(1, preds.size());
        assertEquals("REGION_ID", preds.get(0).get("columnName").asText());
        assertEquals("=", preds.get(0).get("operator").asText());
        assertEquals("p_id", preds.get(0).get("value").asText());
        assertEquals(7, preds.get(0).get("lineNumber").asInt());
    }

    @Test
    void noPredicatesFieldWhenTableHasNoWhereFilters() throws Exception {
        FlowNode node = new FlowNode();
        node.setObjectName("P_INSERT_ONLY");
        node.setDepth(0);
        node.setTableOperations(List.of(op("REF_REGION", "INSERT", 10)));

        JsonNode doc = writeAndReadTables(node, "b-pred-2");
        JsonNode usage = doc.get("tables").get(0).get("usedBy").get(0);
        assertNull(usage.get("predicates"),
                "a plain INSERT with no WHERE clause must not carry an empty predicates array");
    }

    @Test
    void duplicateFiltersAcrossRepeatedTableOperationsAreDeduplicated() throws Exception {
        WhereFilter f1 = new WhereFilter("PRODUCT_ID", "=", "p_id", 12);
        WhereFilter f2 = new WhereFilter("PRODUCT_ID", "=", "p_id", 12); // same identity, repeated
        WhereFilter f3 = new WhereFilter("WAREHOUSE_ID", "=", "7", 12);
        FlowNode node = new FlowNode();
        node.setObjectName("P_MULTI_STMT");
        node.setDepth(0);
        node.setTableOperations(List.of(
                op("ERP_STOCK_LEVEL", "SELECT", 12, f1),
                op("ERP_STOCK_LEVEL", "SELECT", 12, f2, f3)));

        JsonNode doc = writeAndReadTables(node, "b-pred-3");
        JsonNode usage = doc.get("tables").get(0).get("usedBy").get(0);
        JsonNode preds = usage.get("predicates");
        assertEquals(2, preds.size(), "the repeated identical filter must be deduplicated: " + preds);
    }
}
