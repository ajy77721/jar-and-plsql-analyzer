package com.plsql.parser;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.plsql.parser.flow.DependencyCrawler;
import com.plsql.parser.flow.SchemaResolver;
import com.plsql.parser.flow.SourceDownloader;
import com.plsql.parser.model.CallInfo;
import com.plsql.parser.model.FlowNode;
import com.plsql.parser.model.FlowResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Reproduces the CUSTOMER_MAR2026$COV_STAX_BREAKDOWN_TBL3 / COV_STAX_TRAN_TBL3 bug: a
 * package-level collection variable indexed as {@code g_tbl(i)} is syntactically identical to a
 * call to the ANTLR grammar. {@code PlSqlAnalysisVisitor.classifyCall} would misclassify a bare,
 * unqualified {@code identifier(args)} as an EXTERNAL call with no package unless the parser's own
 * scope tracking recognizes the name — and even when it does, {@code DependencyCrawler}'s
 * {@code filterLocalVarCalls} used to run in the single-subprogram crawl path but was NEVER called
 * in the package-aggregation path (crawling a bare package name, the reliable default for a
 * whole-package crawl), so any surviving false positive leaked into the aggregated node's calls
 * completely unfiltered.
 *
 * <p>Whole-package crawl entry point (no dot in the entry name) drives every subprogram through
 * DependencyCrawler's package-aggregation branch (targetSub == null, targetObj != null,
 * !specificSubNotFound), which is exactly the branch that used to skip filtering entirely.
 */
@DisplayName("DependencyCrawler: package-level global variable is not a false-positive call")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class DependencyCrawlerPackageGlobalVarFilterTest {

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
        // Without an output dir, crawl() takes the in-memory fallback (buildInMemoryResult),
        // which never populates FlowResult#getFlowNodes() (MID-026 removed the resident node
        // list — nodes are only ever streamed to a ChunkedFlowWriter). Route through a real
        // in-memory store + chunked writer so the aggregated FlowNode this test inspects is
        // actually produced.
        crawler.setOutputDir(new InMemoryArtifactStore(), ArtifactKey.of("dependency-crawler-pkg-global-var-test"));
    }

    @Test
    @DisplayName("Whole-package crawl aggregates two subprograms without leaking a bogus call for their shared global collection variable")
    void packageGlobalCollectionVarNotLeakedAsCallInAggregatedNode() {
        String source = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  TYPE t_num_arr IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
                  g_tbl t_num_arr;

                  PROCEDURE proc_one IS
                    v_val NUMBER;
                  BEGIN
                    v_val := g_tbl(1) + 1;
                  END proc_one;

                  PROCEDURE proc_two IS
                    v_val NUMBER;
                  BEGIN
                    v_val := g_tbl(2) + 1;
                  END proc_two;
                END MY_PKG;
                /
                """;

        when(schemaResolver.getTransitiveDependencies(anyString()))
                .thenReturn(Collections.emptySet());
        when(schemaResolver.resolveObjectType(anyString())).thenReturn(null);
        when(schemaResolver.resolveSchema(anyString())).thenReturn("TEST_SCHEMA");
        when(schemaResolver.resolveSchema(anyString(), any(), anyString())).thenReturn("TEST_SCHEMA");
        // Bare package name (no dot) is parsed as a standalone entry point first
        // (packageName == null, subprogramName == "MY_PKG"), so downloadAndParse fetches it via
        // downloadAny — not downloadPackageBody, which is only used once a specific pkg.sub is
        // being resolved. The matched ParsedObject's own type (PACKAGE_BODY) is what drives the
        // package-aggregation branch afterwards.
        when(downloader.downloadAny("MY_PKG")).thenReturn(source);
        when(downloader.downloadPackageBody(anyString())).thenReturn(null);
        when(downloader.downloadPackageSpec(anyString())).thenReturn(null);
        when(downloader.getCachedSource(anyString())).thenReturn(null);
        when(downloader.isWrapped(anyString())).thenReturn(false);
        when(downloader.getCachedType(anyString())).thenReturn(null);

        // Whole-package entry point (no dot) — drives DependencyCrawler's package-aggregation
        // branch (item.packageName == null, item.subprogramName == "MY_PKG"; targetObj matches
        // the package by name, targetSub stays null, specificSubNotFound is false).
        FlowResult result = crawler.crawl("MY_PKG");
        assertNotNull(result);

        FlowNode pkgNode = result.getFlowNodes().stream()
                .filter(n -> "MY_PKG".equalsIgnoreCase(n.getObjectName()))
                .findFirst().orElse(null);
        assertNotNull(pkgNode, "Should produce one aggregated FlowNode for the whole package");
        assertNotNull(pkgNode.getMembers(), "Aggregated node should record its member subprograms");
        assertEquals(2, pkgNode.getMembers().size(), "Both PROC_ONE and PROC_TWO should be aggregated");

        boolean leakedGlobalVarAsCall = pkgNode.getCalls() != null && pkgNode.getCalls().stream()
                .anyMatch(c -> "G_TBL".equalsIgnoreCase(c.getName()));
        assertFalse(leakedGlobalVarAsCall,
                "Indexing the package's own global collection variable (g_tbl(i)) from multiple "
                        + "subprograms must not produce a bogus call entry in the aggregated node — "
                        + "found calls: " + describeCalls(pkgNode));
    }

    private static String describeCalls(FlowNode node) {
        if (node.getCalls() == null) return "[]";
        StringBuilder sb = new StringBuilder("[");
        for (CallInfo c : node.getCalls()) {
            sb.append(c.getPackageName()).append('.').append(c.getName()).append(' ');
        }
        return sb.append(']').toString();
    }
}
