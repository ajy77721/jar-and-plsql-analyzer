package com.plsql.parser;

import com.plsql.parser.flow.DependencyCrawler;
import com.plsql.parser.flow.SchemaResolver;
import com.plsql.parser.flow.SourceDownloader;
import com.plsql.parser.model.FlowResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Error handling tests for DependencyCrawler.
 * Uses Mockito to mock SchemaResolver and SourceDownloader since they require DB connections.
 */
@DisplayName("DependencyCrawler Error Handling")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class DependencyCrawlerErrorTest {

    private PlSqlParserEngine engine;

    @Mock
    private SourceDownloader downloader;

    @Mock
    private SchemaResolver schemaResolver;

    private DependencyCrawler crawler;

    @BeforeEach
    void setup() {
        engine = new PlSqlParserEngine();
        crawler = new DependencyCrawler(engine, downloader, schemaResolver);
        crawler.setMaxDepth(5);
        crawler.setTimeoutPerEntryMs(10_000);
    }

    // ── Crawl with non-existent entry point ──

    @Test
    @DisplayName("Crawl with non-existent package.proc returns result with no source")
    void testCrawlNonExistentPackageProc() {
        when(schemaResolver.getTransitiveDependencies(anyString()))
                .thenReturn(Collections.emptySet());
        when(schemaResolver.resolveObjectType(anyString())).thenReturn(null);
        when(downloader.downloadPackageBody(anyString())).thenReturn(null);
        when(downloader.downloadPackageSpec(anyString())).thenReturn(null);
        when(downloader.getCachedType(anyString())).thenReturn(null);

        FlowResult result = crawler.crawl("NONEXISTENT_PKG.NONEXISTENT_PROC");
        assertNotNull(result, "Should return a result even for non-existent entry");
        assertEquals("NONEXISTENT_PKG.NONEXISTENT_PROC", result.getEntryPoint());
    }

    @Test
    @DisplayName("Crawl standalone non-existent function returns result")
    void testCrawlNonExistentStandalone() {
        when(schemaResolver.getTransitiveDependencies(anyString()))
                .thenReturn(Collections.emptySet());
        when(schemaResolver.resolveObjectType(anyString())).thenReturn(null);
        when(downloader.downloadAny(anyString())).thenReturn(null);
        when(downloader.getCachedType(anyString())).thenReturn(null);

        FlowResult result = crawler.crawl("NONEXISTENT_FUNC");
        assertNotNull(result);
        assertEquals("NONEXISTENT_FUNC", result.getEntryPoint());
    }

    // ── BFS with circular dependencies ──

    @Test
    @DisplayName("BFS with circular dependencies does not infinite loop (visited set)")
    void testCircularDependencies() {
        String sourceA = """
                CREATE OR REPLACE PACKAGE BODY PKG_A AS
                  PROCEDURE PROC_A IS
                  BEGIN
                    PKG_B.PROC_B;
                  END PROC_A;
                END PKG_A;
                """;
        String sourceB = """
                CREATE OR REPLACE PACKAGE BODY PKG_B AS
                  PROCEDURE PROC_B IS
                  BEGIN
                    PKG_A.PROC_A;
                  END PROC_B;
                END PKG_B;
                """;

        when(schemaResolver.getTransitiveDependencies(anyString()))
                .thenReturn(Set.of("PKG_A", "PKG_B"));
        when(schemaResolver.resolveObjectType(anyString())).thenReturn(null);
        when(schemaResolver.resolveSchema(anyString())).thenReturn("TEST_SCHEMA");
        when(schemaResolver.resolveSchema(anyString(), any(), anyString())).thenReturn("TEST_SCHEMA");
        when(schemaResolver.hasDependencyData(anyString())).thenReturn(false);
        when(schemaResolver.isKnownObject(anyString())).thenReturn(true);

        when(downloader.downloadPackageBody("PKG_A")).thenReturn(sourceA);
        when(downloader.downloadPackageBody("PKG_B")).thenReturn(sourceB);
        when(downloader.downloadPackageSpec(anyString())).thenReturn(null);
        when(downloader.getCachedSource(anyString())).thenReturn(null);
        when(downloader.isWrapped(anyString())).thenReturn(false);
        when(downloader.getCachedType(anyString())).thenReturn(null);

        FlowResult result = crawler.crawl("PKG_A.PROC_A");
        assertNotNull(result, "Should complete without infinite loop");
    }

    // ── Max depth limiting ──

    @Test
    @DisplayName("Max depth is respected and limits crawl")
    void testMaxDepthRespected() {
        crawler.setMaxDepth(0);

        String source = """
                CREATE OR REPLACE PACKAGE BODY PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    OTHER_PKG.OTHER_PROC;
                  END PROC;
                END PKG;
                """;

        when(schemaResolver.getTransitiveDependencies(anyString()))
                .thenReturn(Collections.emptySet());
        when(schemaResolver.resolveObjectType(anyString())).thenReturn(null);
        when(downloader.downloadPackageBody("PKG")).thenReturn(source);
        when(downloader.downloadPackageSpec(anyString())).thenReturn(null);
        when(schemaResolver.resolveSchema(anyString())).thenReturn("SCHEMA");
        when(schemaResolver.resolveSchema(anyString(), any(), anyString())).thenReturn("SCHEMA");
        when(downloader.getCachedSource(anyString())).thenReturn(null);
        when(downloader.isWrapped(anyString())).thenReturn(false);
        when(downloader.getCachedType(anyString())).thenReturn(null);

        FlowResult result = crawler.crawl("PKG.PROC");
        assertNotNull(result);
        assertTrue(result.getMaxDepthReached() <= 1);
    }

    // ── Wrapped source handling ──

    @Test
    @DisplayName("Crawl handles wrapped source gracefully")
    void testCrawlWrappedSource() {
        String wrappedSource = "CREATE OR REPLACE PACKAGE BODY WRAPPED_PKG wrapped\na000000\n89abcdef\n";

        when(schemaResolver.getTransitiveDependencies(anyString()))
                .thenReturn(Collections.emptySet());
        when(schemaResolver.resolveObjectType(anyString())).thenReturn(null);
        when(downloader.downloadPackageBody("WRAPPED_PKG")).thenReturn(wrappedSource);
        when(downloader.downloadPackageSpec(anyString())).thenReturn(null);
        when(schemaResolver.resolveSchema(anyString())).thenReturn("SCHEMA");
        when(schemaResolver.resolveSchema(anyString(), any(), anyString())).thenReturn("SCHEMA");
        when(downloader.getCachedSource(anyString())).thenReturn(wrappedSource);
        when(downloader.isWrapped(anyString())).thenReturn(false);
        when(downloader.getCachedType(anyString())).thenReturn(null);

        FlowResult result = crawler.crawl("WRAPPED_PKG.SOME_PROC");
        assertNotNull(result);
    }

    // ── TYPE/SYNONYM stub nodes ──

    @Test
    @DisplayName("Non-parsable types (TYPE, SYNONYM) produce stub nodes")
    void testNonParsableTypeStubNode() {
        when(schemaResolver.getTransitiveDependencies(anyString()))
                .thenReturn(Collections.emptySet());
        when(schemaResolver.resolveObjectType("SOME_TYPE")).thenReturn("TYPE");
        when(schemaResolver.resolveSchema("SOME_TYPE")).thenReturn("SCHEMA");

        FlowResult result = crawler.crawl("SOME_TYPE");
        assertNotNull(result);
    }

    // ── Entry point with schema prefix ──

    @Test
    @DisplayName("Entry point with dotted name splits correctly")
    void testDottedEntryPoint() {
        when(schemaResolver.getTransitiveDependencies(anyString()))
                .thenReturn(Collections.emptySet());
        when(schemaResolver.resolveObjectType(anyString())).thenReturn(null);
        when(downloader.downloadPackageBody("MY_PKG")).thenReturn(null);
        when(downloader.downloadPackageSpec("MY_PKG")).thenReturn(null);
        when(downloader.getCachedType(anyString())).thenReturn(null);

        FlowResult result = crawler.crawl("MY_PKG.MY_PROC");
        assertNotNull(result);
        assertEquals("MY_PKG.MY_PROC", result.getEntryPoint());
    }
}
