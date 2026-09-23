package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsql.parser.model.FlowNode;
import com.plsql.parser.model.ParsedObject;
import com.plsql.parser.model.TableOperationInfo;
import com.plsql.parser.service.TableOperationEnricher;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sep05-10 — {@code SELECT COUNT(*) INTO v_n FROM t} must not render as an empty row.
 *
 * <h2>Why this had to be a test and not a fix-and-look</h2>
 *
 * <p>This was the last and largest block of blank cells on the table screen, and it was measured
 * rather than guessed: <b>1,303 of PostgreSQL's 1,553 empty rows</b> are aggregate-only reads. The
 * earlier findings in this series were all "the data exists and stops one layer short", and each was
 * found only from the far end, by a user asking why a cell was blank. So this pins both halves of the
 * behaviour at the layer where the loss happened.
 *
 * <h2>The distinction being protected</h2>
 *
 * <p>An aggregate select item is <b>detail but not a column</b>, and the test asserts both:
 *
 * <ul>
 *   <li>it appears in {@code columnDetail}, so the screen can say what the statement read;</li>
 *   <li>it does <b>not</b> appear in {@code columns}. {@code columns} means "columns of this table
 *       that are referenced". {@code COUNT(*)} is not one. Folding it in would make a column filter
 *       match a non-column, and would defeat Sep05-08 — whose entire purpose is that the screen
 *       distinguish "no column is referenced" from "we failed to capture it". Getting the blank cell
 *       to fill by widening what counts as a column would be the same class of mistake as the
 *       original loss, just in the other direction.</li>
 * </ul>
 */
class AggregateSelectItemEmittedTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private TableOperationInfo op(String table, String operation, String sql, int line) {
        TableOperationInfo t = new TableOperationInfo();
        t.setTableName(table);
        t.setSchema("APP");
        t.setOperation(operation);
        t.setSqlText(sql);
        t.setLine(line);
        return t;
    }

    @Test
    @DisplayName("the analyser records an aggregate select item with its expression and INTO target")
    void analyserRecordsTheAggregate() {
        ParsedObject o = new ParsedObject();
        o.setTableOperations(List.of(
                op("ERP_PRODUCT", "SELECT", "SELECT COUNT(*) INTO v_n FROM app.erp_product", 42)));

        new TableOperationEnricher().enrich(List.of(o));

        var usages = o.getTableOperations().get(0).getColumnUsages();
        assertEquals(1, usages.size(), "one select item: " + usages);
        var u = usages.get(0);
        assertNull(u.getColumnName(), "COUNT(*) is not a column, so it must carry no column name");
        assertEquals("COUNT(*)", u.getExpression());
        assertEquals("v_n", u.getIntoVariable(), "the INTO target is the useful half of this row");
    }

    @Test
    @DisplayName("the statements index emits it as detail, and not as a column")
    void producerEmitsAsDetailNotColumn() throws Exception {
        ParsedObject o = new ParsedObject();
        o.setTableOperations(List.of(
                op("ERP_PRODUCT", "SELECT", "SELECT COUNT(*) INTO v_n FROM app.erp_product", 42)));
        new TableOperationEnricher().enrich(List.of(o));

        FlowNode node = new FlowNode();
        node.setObjectName("P_COUNT_PRODUCTS");
        node.setTableOperations(o.getTableOperations());

        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ArtifactKey build = ArtifactKey.of("test-build");
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, build);
        ArtifactKey apiDir = ArtifactKey.of("test-build", "api");

        // MID-026: writeStatementIndex now takes the lightweight LightNode projection, not
        // FlowNode directly — build it via the writer's own toLightNodes(), the same projection
        // the real call site uses, rather than duplicating its field mapping here.
        Method toLightNodes = ChunkedFlowWriter.class.getDeclaredMethod("toLightNodes", List.class);
        toLightNodes.setAccessible(true);
        Object lightNodes = toLightNodes.invoke(null, List.of(node));

        Method m = ChunkedFlowWriter.class.getDeclaredMethod(
                "writeStatementIndex", ArtifactKey.class, List.class);
        m.setAccessible(true);
        m.invoke(writer, apiDir, lightNodes);

        JsonNode doc;
        try (var in = store.read(apiDir.resolve("statements", "index.json"))) {
            doc = JSON.readTree(in);
        }
        JsonNode rows = doc.get("statements");
        assertEquals(1, rows.size(), "one statement expected: " + rows);
        JsonNode row = rows.get(0);

        // MID-026 calibration: LightNode carries objectName through to nodeName — a field the
        // byte-identical real-corpus comparison (§169) already confirmed unchanged, pinned here too
        // so a future regression in the projection is caught at the unit level, not only end to end.
        assertEquals("P_COUNT_PRODUCTS", row.get("nodeName").asText(),
                "nodeName must survive the FlowNode -> LightNode projection unchanged");

        JsonNode detail = row.get("columnDetail");
        assertNotNull(detail, "the aggregate select item was dropped — the Sep05-10 defect");
        assertEquals(1, detail.size(), "columnDetail: " + detail);
        assertFalse(detail.get(0).has("name"),
                "a nameless select item must not be given a fabricated column name");
        assertEquals("COUNT(*)", detail.get(0).get("expression").asText());
        assertEquals("v_n", detail.get(0).get("into").asText());

        JsonNode cols = row.get("columns");
        assertTrue(cols == null || cols.isEmpty(),
                "COUNT(*) must NOT be listed as a column of the table; got " + cols);
    }

    @Test
    @DisplayName("a usage with nothing at all is still dropped")
    void emptyUsageIsStillDropped() throws Exception {
        // The fix widened a `continue`, and a widened skip is easy to widen too far. A usage with
        // no name, no expression and no INTO carries nothing a reader could use, so it must stay
        // out — otherwise the screen fills with rows that say nothing, which reads as noise and is
        // how a reader learns to stop trusting the column.
        TableOperationInfo t = op("ERP_PRODUCT", "SELECT", null, 7);
        t.setColumnUsages(new java.util.ArrayList<>(List.of(
                new com.plsqlanalyzer.parser.model.ColumnUsage(
                        "SELECT", null, null, null, null, null, null, 7))));

        FlowNode node = new FlowNode();
        node.setObjectName("P_X");
        node.setTableOperations(List.of(t));

        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ArtifactKey build = ArtifactKey.of("b2");
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, build);
        ArtifactKey apiDir = ArtifactKey.of("b2", "api");
        // MID-026: writeStatementIndex now takes the lightweight LightNode projection, not
        // FlowNode directly — build it via the writer's own toLightNodes(), the same projection
        // the real call site uses, rather than duplicating its field mapping here.
        Method toLightNodes = ChunkedFlowWriter.class.getDeclaredMethod("toLightNodes", List.class);
        toLightNodes.setAccessible(true);
        Object lightNodes = toLightNodes.invoke(null, List.of(node));

        Method m = ChunkedFlowWriter.class.getDeclaredMethod(
                "writeStatementIndex", ArtifactKey.class, List.class);
        m.setAccessible(true);
        m.invoke(writer, apiDir, lightNodes);

        JsonNode doc;
        try (var in = store.read(apiDir.resolve("statements", "index.json"))) {
            doc = JSON.readTree(in);
        }
        JsonNode detail = doc.get("statements").get(0).get("columnDetail");
        assertTrue(detail == null || detail.isEmpty(),
                "an entirely empty usage must not be emitted; got " + detail);
    }
}
