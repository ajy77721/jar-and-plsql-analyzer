package com.plsql.parser;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.LocalArtifactStore;
import com.plsql.parser.flow.DbConnectionManager;
import com.plsql.parser.flow.DependencyCrawler;
import com.plsql.parser.flow.SchemaResolver;
import com.plsql.parser.flow.SourceDownloader;
import com.plsql.parser.model.FlowEdge;
import com.plsql.parser.model.FlowResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * The per-level parallel catalog + source fan-out must not change the analysis output: a crawl with
 * parallelism 1 (serial) and a crawl with parallelism 8 (parallel) over the same fixture must yield
 * the exact same nodes, edges and depth. The parallel path only warms the downloader's caches, so
 * the serial processing loop — and therefore node/edge order — is untouched.
 */
@DisplayName("DependencyCrawler parallel BFS determinism")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class DependencyCrawlerParallelBfsTest {

    @Mock
    private SourceDownloader downloader;

    @Mock
    private SchemaResolver schemaResolver;

    @Mock
    private DbConnectionManager connManager;

    @TempDir
    Path outputDir;

    private final Map<String, String> sources = new LinkedHashMap<>();

    @BeforeEach
    void setup() {
        sources.put("PKG_ROOT", pkg("PKG_ROOT", "PROC_ROOT", "PKG_A.P1;\n    PKG_B.P1;\n    PKG_C.P1;"));
        sources.put("PKG_A", pkg("PKG_A", "P1", "PKG_D.P1;\n    PKG_E.P1;"));
        sources.put("PKG_B", pkg("PKG_B", "P1", "PKG_D.P1;\n    PKG_F.P1;"));
        sources.put("PKG_C", pkg("PKG_C", "P1", "PKG_E.P1;\n    PKG_F.P1;"));
        sources.put("PKG_D", pkg("PKG_D", "P1", "NULL;"));
        sources.put("PKG_E", pkg("PKG_E", "P1", "NULL;"));
        sources.put("PKG_F", pkg("PKG_F", "P1", "NULL;"));

        when(schemaResolver.getTransitiveDependencies(anyString())).thenReturn(Collections.emptySet());
        when(schemaResolver.resolveObjectType(anyString())).thenReturn(null);
        when(schemaResolver.resolveSchema(anyString())).thenReturn("S");
        when(schemaResolver.hasDependencyData(anyString())).thenReturn(false);
        when(schemaResolver.isKnownObject(anyString())).thenReturn(true);
        when(schemaResolver.getConfiguredSchemas()).thenReturn(List.of("S"));
        when(schemaResolver.isForeignOwnerObject(anyString(), anyString())).thenReturn(false);

        for (Map.Entry<String, String> e : sources.entrySet()) {
            when(downloader.downloadPackageBody(e.getKey())).thenReturn(e.getValue());
        }
        when(downloader.downloadPackageSpec(anyString())).thenReturn(null);
        when(downloader.downloadAny(anyString())).thenReturn(null);
        when(downloader.getCachedSource(anyString())).thenReturn(null);
        when(downloader.getOwner(anyString())).thenReturn(null);
        when(downloader.isWrapped(anyString())).thenReturn(false);
        when(downloader.getCachedType(anyString())).thenReturn(null);
        when(downloader.getConnManager()).thenReturn(connManager);
        when(connManager.isSqlServer()).thenReturn(false);
    }

    private static String pkg(String name, String proc, String body) {
        return "CREATE OR REPLACE PACKAGE BODY " + name + " AS\n"
                + "  PROCEDURE " + proc + " IS\n"
                + "  BEGIN\n    " + body + "\n  END " + proc + ";\n"
                + "END " + name + ";\n";
    }

    private FlowResult run(int parallelism, String label) {
        DependencyCrawler crawler = new DependencyCrawler(new PlSqlParserEngine(), downloader, schemaResolver);
        crawler.setMaxDepth(5);
        crawler.setTimeoutPerEntryMs(10_000);
        crawler.setSourceParallelism(parallelism);
        crawler.setCatalogParallelism(parallelism);
        // A real store makes the crawl take the chunked-writer path, so the returned FlowResult
        // carries the merged call graph (the in-memory fallback has none).
        crawler.setOutputDir(new LocalArtifactStore(outputDir), ArtifactKey.of("out", label));
        return crawler.crawl("PKG_ROOT.PROC_ROOT");
    }

    private static List<String> edges(FlowResult r) {
        List<String> out = new ArrayList<>();
        for (FlowEdge e : r.getCallGraph()) {
            out.add(e.getFromNodeId() + "->" + e.getToNodeId() + "@" + e.getLine());
        }
        Collections.sort(out);
        return out;
    }

    @Test
    @DisplayName("parallel (8) crawl equals serial (1) crawl on the same fixture")
    void parallelEqualsSerial() {
        FlowResult serial = run(1, "serial");
        FlowResult parallel = run(8, "parallel");

        assertEquals(serial.getEntryPoint(), parallel.getEntryPoint());
        assertEquals(serial.getTotalObjectsCrawled(), parallel.getTotalObjectsCrawled());
        assertEquals(serial.getMaxDepthReached(), parallel.getMaxDepthReached());
        assertEquals(edges(serial), edges(parallel), "call-graph edges must be identical");
        assertFalse(parallel.getCallGraph().isEmpty(),
                "fixture should produce edges; nodes=" + parallel.getTotalObjectsCrawled()
                        + " edges=" + parallel.getCallGraph().size()
                        + " errors=" + parallel.getErrors());
    }

    @Test
    @DisplayName("A1 closure-wave path equals level-by-level fallback path")
    void closureWaveEqualsFallback() {
        // Reference: no closure (setup's anyString -> empty) -> per-level DB discovery.
        FlowResult fallback = run(8, "fallback");

        // Closure: the resolver now returns the full reachable set. The wave fetches + pre-parses
        // every source up front (A2/A4), so the BFS scopes levels 1+ purely in memory (A1).
        Set<String> closure = new LinkedHashSet<>(
                List.of("PKG_A", "PKG_B", "PKG_C", "PKG_D", "PKG_E", "PKG_F"));
        when(schemaResolver.getReachableClosure("PKG_ROOT")).thenReturn(closure);
        for (Map.Entry<String, String> e : sources.entrySet()) {
            when(downloader.downloadAny(e.getKey())).thenReturn(e.getValue());
        }
        when(downloader.getCachedType(anyString())).thenReturn("PACKAGE BODY");
        when(downloader.getCachedSource(anyString())).thenAnswer(
                inv -> sources.get(((String) inv.getArgument(0)).toUpperCase()));
        when(downloader.fetchSourcesBulk(anyCollection())).thenReturn(Collections.emptyMap());

        FlowResult closureResult = run(8, "closure");

        assertEquals(fallback.getEntryPoint(), closureResult.getEntryPoint());
        assertEquals(fallback.getTotalObjectsCrawled(), closureResult.getTotalObjectsCrawled(),
                "closure-wave crawl must reach the same objects as the fallback");
        assertEquals(fallback.getMaxDepthReached(), closureResult.getMaxDepthReached());
        assertEquals(edges(fallback), edges(closureResult),
                "closure-wave call-graph edges must be byte-identical to the fallback");
        assertFalse(closureResult.getCallGraph().isEmpty(), "fixture should produce edges");
    }
}
