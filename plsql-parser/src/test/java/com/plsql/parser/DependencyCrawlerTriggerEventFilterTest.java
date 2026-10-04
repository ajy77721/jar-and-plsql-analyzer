package com.plsql.parser;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.LocalArtifactStore;
import com.plsql.parser.flow.ChunkedFlowWriter;
import com.plsql.parser.flow.DependencyCrawler;
import com.plsql.parser.flow.SchemaResolver;
import com.plsql.parser.flow.SourceDownloader;
import com.plsql.parser.model.FlowEdge;
import com.plsql.parser.model.FlowNode;
import com.plsql.parser.model.FlowResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Catalog-discovered triggers are first-class nodes.
 *
 * <p>Every trigger returned by {@code TriggerDiscoverer.discover(...)} is emitted as a
 * {@code SCHEMA$TRG$NAME} node carrying its parsed body calls (resolved against the crawled
 * estate) and its timing/event/level/catalog metadata, so the estate trigger facet can serve
 * {@code bodyCalls} per node and the trigger's calls join the call graph. The trigger's
 * {@code FIRES_TRIGGER} edge remains event-gated: a trigger whose triggering_event never matches a
 * DML recorded in the walked graph is still a node, but has no firing routine. A trigger that is
 * BOTH source-parsed and catalog-discovered yields exactly ONE node.
 *
 * Exercises the private DependencyCrawler#emitTriggerNodesAndEdges(...) method directly via
 * reflection (same convention used by TriggerDiscovererErrorTest for private methods), backed by a
 * real ChunkedFlowWriter over a temp-dir LocalArtifactStore (same convention used by
 * ChunkedFlowWriterErrorTest).
 */
@DisplayName("DependencyCrawler catalog-discovered trigger nodes")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class DependencyCrawlerTriggerEventFilterTest {

    @Mock
    private SourceDownloader downloader;

    @Mock
    private SchemaResolver schemaResolver;

    private DependencyCrawler crawler;

    @TempDir
    Path tempDir;

    private ChunkedFlowWriter writer;

    @BeforeEach
    void setup() throws Exception {
        PlSqlParserEngine engine = new PlSqlParserEngine();
        crawler = new DependencyCrawler(engine, downloader, schemaResolver);
        writer = new ChunkedFlowWriter(new LocalArtifactStore(tempDir), ArtifactKey.of("test-build"));
    }

    @SuppressWarnings("unchecked")
    private static <T> Map<String, T> fieldMap(Object target, String name) throws Exception {
        Field f = DependencyCrawler.class.getDeclaredField(name);
        f.setAccessible(true);
        return (Map<String, T>) f.get(target);
    }

    private void setDmlRefs(List<String[]> refs) throws Exception {
        Field f = DependencyCrawler.class.getDeclaredField("dmlRefs");
        f.setAccessible(true);
        List<String[]> list = (List<String[]>) f.get(crawler);
        list.clear();
        list.addAll(refs);
    }

    private void setCrawledNodeIds(Map<String, String> ids) throws Exception {
        Map<String, String> map = fieldMap(crawler, "crawledNodeIds");
        map.clear();
        map.putAll(ids);
    }

    private void setCrawledTriggerNodeIds(Map<String, String> ids) throws Exception {
        Map<String, String> map = fieldMap(crawler, "crawledTriggerNodeIds");
        map.clear();
        map.putAll(ids);
    }

    private void invokeEmitTriggerNodesAndEdges(List<Map<String, Object>> triggers) throws Exception {
        Method m = DependencyCrawler.class.getDeclaredMethod(
                "emitTriggerNodesAndEdges", List.class, ChunkedFlowWriter.class, List.class, int.class);
        m.setAccessible(true);
        m.invoke(crawler, triggers, writer, new ArrayList<String>(), 0);
    }

    private Map<String, Object> deleteTrigger() {
        Map<String, Object> t = new LinkedHashMap<>();
        t.put("name", "ERP_TRG_SO_STMT_AD");
        t.put("schema", "ANALYZER");
        t.put("tableName", "ERP_SALES_ORDER");
        t.put("event", "DELETE");
        t.put("timing", "AFTER");
        t.put("definition", "BEGIN NULL; END;");
        return t;
    }

    private static FlowNode triggerNode(FlowResult result, String name) {
        return result.getFlowNodes().stream()
                .filter(n -> name.equalsIgnoreCase(n.getObjectName()))
                .findFirst().orElse(null);
    }

    @Test
    @DisplayName("a trigger whose event matches no recorded DML is still a node (no FIRES_TRIGGER edge)")
    void testTriggerWithNoMatchingDmlIsStillANode() throws Exception {
        // Only INSERT/UPDATE against ERP_SALES_ORDER are ever performed in this call graph — no
        // DELETE. The trigger's own event is DELETE, so it never fires: it must still be a node
        // (its detail/bodyCalls are served), but must have NO firing routine.
        List<String[]> refs = new ArrayList<>();
        refs.add(new String[]{"NODE_A", "ANALYZER", "ERP_SALES_ORDER", "INSERT"});
        refs.add(new String[]{"NODE_B", "ANALYZER", "ERP_SALES_ORDER", "UPDATE"});
        setDmlRefs(refs);

        invokeEmitTriggerNodesAndEdges(List.of(deleteTrigger()));

        FlowResult result = writer.mergeChunks("ENTRY", "ANALYZER", 100, 1, new ArrayList<>());
        FlowNode node = triggerNode(result, "ERP_TRG_SO_STMT_AD");
        assertNotNull(node, "a discovered trigger must always be emitted as a node");
        assertEquals("TRIGGER", node.getObjectType());
        assertEquals("ANALYZER$TRG$ERP_TRG_SO_STMT_AD", node.getNodeId());
        boolean fires = result.getCallGraph().stream()
                .anyMatch(e -> "FIRES_TRIGGER".equals(e.getEdgeType())
                        && node.getNodeId().equals(e.getToNodeId()));
        assertFalse(fires,
                "an event-mismatched trigger must have no FIRES_TRIGGER edge");
    }

    @Test
    @DisplayName("a trigger whose event matches a recorded DML is emitted with its FIRES_TRIGGER edge")
    void testTriggerWithMatchingDmlIsEmitted() throws Exception {
        List<String[]> refs = new ArrayList<>();
        refs.add(new String[]{"NODE_A", "ANALYZER", "ERP_SALES_ORDER", "DELETE"});
        setDmlRefs(refs);

        invokeEmitTriggerNodesAndEdges(List.of(deleteTrigger()));

        FlowResult result = writer.mergeChunks("ENTRY", "ANALYZER", 100, 1, new ArrayList<>());
        FlowNode node = triggerNode(result, "ERP_TRG_SO_STMT_AD");
        assertNotNull(node, "DELETE trigger with a matching DELETE DML should be emitted as a node");
        boolean fires = result.getCallGraph().stream()
                .anyMatch(e -> "FIRES_TRIGGER".equals(e.getEdgeType())
                        && node.getNodeId().equals(e.getToNodeId()));
        assertTrue(fires, "a matching DML must add a FIRES_TRIGGER edge to the trigger node");
    }

    @Test
    @DisplayName("the node carries bodyCalls with resolved nodeIds and a trigger->callee edge")
    void testDiscoveredTriggerCarriesBodyCallsAndCalleeEdge() throws Exception {
        Map<String, String> ids = new HashMap<>();
        ids.put("ERP_USP_WRITE_AUDIT_TRAIL", "ERP$ERP_USP_WRITE_AUDIT_TRAIL");
        setCrawledNodeIds(ids);
        setDmlRefs(new ArrayList<>()); // no matching DML — the CALL edge must not depend on firing

        Map<String, Object> t = deleteTrigger();
        t.put("triggerType", "AFTER STATEMENT");
        t.put("status", "ENABLED");
        Map<String, Object> call = new LinkedHashMap<>();
        call.put("name", "erp_usp_write_audit_trail");
        call.put("type", "EXTERNAL");
        t.put("calls", new ArrayList<>(List.of(call)));

        invokeEmitTriggerNodesAndEdges(List.of(t));

        FlowResult result = writer.mergeChunks("ENTRY", "ANALYZER", 100, 1, new ArrayList<>());
        FlowNode node = triggerNode(result, "ERP_TRG_SO_STMT_AD");
        assertNotNull(node);
        assertEquals("STATEMENT", node.getTriggerLevel());
        assertEquals("AFTER STATEMENT", node.getTriggerType());
        assertEquals("ENABLED", node.getTriggerStatus());
        assertNotNull(node.getCalls());
        assertEquals(1, node.getCalls().size());
        assertEquals("ERP$ERP_USP_WRITE_AUDIT_TRAIL", node.getCalls().get(0).getNodeId(),
                "the body call must be resolved to the crawled callee's node id");
        boolean calleeEdge = result.getCallGraph().stream()
                .anyMatch(e -> node.getNodeId().equals(e.getFromNodeId())
                        && "ERP$ERP_USP_WRITE_AUDIT_TRAIL".equals(e.getToNodeId())
                        && (e.getEdgeType() == null || "CALL".equals(e.getEdgeType())));
        assertTrue(calleeEdge, "the trigger body's call must join the call graph");
    }

    @Test
    @DisplayName("a trigger that is both source-parsed and catalog-discovered is ONE node")
    void testDedupSourceParsedWins() throws Exception {
        // A source-parsed trigger node already exists in the crawl.
        FlowNode source = new FlowNode();
        source.setSchema("ANALYZER");
        source.setObjectName("ERP_TRG_SO_STMT_AD");
        source.setObjectType("TRIGGER");
        source.setTriggerTable("ERP_SALES_ORDER");
        source.setReadable(true);
        source.setDepth(1);
        source.generateNodeId();
        String sourceId = source.getNodeId();
        assertEquals("ANALYZER$ERP_TRG_SO_STMT_AD", sourceId);
        writer.writeChunk(source);
        setCrawledTriggerNodeIds(new HashMap<>(Map.of("ANALYZER.ERP_TRG_SO_STMT_AD", sourceId)));

        List<String[]> refs = new ArrayList<>();
        refs.add(new String[]{"NODE_A", "ANALYZER", "ERP_SALES_ORDER", "DELETE"});
        setDmlRefs(refs);

        invokeEmitTriggerNodesAndEdges(List.of(deleteTrigger()));

        FlowResult result = writer.mergeChunks("ENTRY", "ANALYZER", 100, 1, new ArrayList<>());
        long count = result.getFlowNodes().stream()
                .filter(n -> "ERP_TRG_SO_STMT_AD".equalsIgnoreCase(n.getObjectName())).count();
        assertEquals(1, count, "the source-parsed and catalog-discovered trigger must be ONE node");
        assertEquals(sourceId, triggerNode(result, "ERP_TRG_SO_STMT_AD").getNodeId(),
                "the source-parsed node wins (the catalog must not emit a duplicate)");
        boolean fires = result.getCallGraph().stream()
                .anyMatch(e -> "FIRES_TRIGGER".equals(e.getEdgeType())
                        && sourceId.equals(e.getToNodeId()));
        assertTrue(fires, "the firing edge must point at the single (source-parsed) node");
    }
}
