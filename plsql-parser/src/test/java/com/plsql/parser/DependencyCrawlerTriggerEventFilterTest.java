package com.plsql.parser;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.LocalArtifactStore;
import com.plsql.parser.flow.ChunkedFlowWriter;
import com.plsql.parser.flow.DependencyCrawler;
import com.plsql.parser.flow.SchemaResolver;
import com.plsql.parser.flow.SourceDownloader;
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
 * Regression test for the trigger-node reachability bug: a trigger node was written
 * to the flow graph (with a finite depth, e.g. depth=1) unconditionally for every
 * trigger discovered on a referenced table, even when the trigger's own
 * triggering_event (INSERT/UPDATE/DELETE) never matched any DML operation actually
 * performed against that table in the walked call graph (dmlRefs). Only the
 * FIRES_TRIGGER *edges* were previously filtered by event match — the node itself
 * was always emitted, producing a spurious orphan/reachable node.
 *
 * Exercises the private DependencyCrawler#emitTriggerNodesAndEdges(...) method
 * directly via reflection (same convention used by TriggerDiscovererErrorTest for
 * private methods), backed by a real ChunkedFlowWriter over a temp-dir
 * LocalArtifactStore (same convention used by ChunkedFlowWriterErrorTest).
 */
@DisplayName("DependencyCrawler trigger node event-matching filter")
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
    private void setDmlRefs(List<String[]> refs) throws Exception {
        Field f = DependencyCrawler.class.getDeclaredField("dmlRefs");
        f.setAccessible(true);
        List<String[]> list = (List<String[]>) f.get(crawler);
        list.clear();
        list.addAll(refs);
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

    @Test
    @DisplayName("Trigger whose event never matches any DML on its table is NOT emitted as a node")
    void testTriggerWithNoMatchingDmlIsNotEmitted() throws Exception {
        // Only INSERT/UPDATE against ERP_SALES_ORDER are ever performed in this
        // call graph — no DELETE. The trigger's own event is DELETE, so it should
        // never fire and must not appear as a reachable node.
        List<String[]> refs = new ArrayList<>();
        refs.add(new String[]{"NODE_A", "ANALYZER", "ERP_SALES_ORDER", "INSERT"});
        refs.add(new String[]{"NODE_B", "ANALYZER", "ERP_SALES_ORDER", "UPDATE"});
        setDmlRefs(refs);

        invokeEmitTriggerNodesAndEdges(List.of(deleteTrigger()));

        FlowResult result = writer.mergeChunks("ENTRY", "ANALYZER", 100, 1, new ArrayList<>());
        boolean found = result.getFlowNodes().stream()
                .anyMatch(n -> "ERP_TRG_SO_STMT_AD".equalsIgnoreCase(n.getObjectName()));
        assertFalse(found,
                "DELETE trigger with no matching DELETE DML against its table must not be emitted as a node");
    }

    @Test
    @DisplayName("Trigger whose event matches an actual DML on its table IS emitted as a node")
    void testTriggerWithMatchingDmlIsEmitted() throws Exception {
        List<String[]> refs = new ArrayList<>();
        refs.add(new String[]{"NODE_A", "ANALYZER", "ERP_SALES_ORDER", "DELETE"});
        setDmlRefs(refs);

        invokeEmitTriggerNodesAndEdges(List.of(deleteTrigger()));

        FlowResult result = writer.mergeChunks("ENTRY", "ANALYZER", 100, 1, new ArrayList<>());
        boolean found = result.getFlowNodes().stream()
                .anyMatch(n -> "ERP_TRG_SO_STMT_AD".equalsIgnoreCase(n.getObjectName()));
        assertTrue(found,
                "DELETE trigger with a matching DELETE DML against its table should be emitted as a node");
    }
}
