package com.plsql.parser;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.plsql.parser.flow.AnalysisRegistry;
import com.plsql.parser.flow.DependencyCrawler;
import com.plsql.parser.flow.NodeParseCache;
import com.plsql.parser.flow.ParseResultCache;
import com.plsql.parser.flow.SchemaResolver;
import com.plsql.parser.flow.SourceDownloader;
import com.plsql.parser.model.FlowResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * The central fetch decision's third gate, exercised through the real {@link DependencyCrawler}
 * loop: a second crawl of an unchanged scope skips the object with NO database fetch; a LAST_DDL
 * change, a partial prior run, and force-refresh all re-analyse.
 *
 * <p>{@link SourceDownloader}/{@link SchemaResolver} are mocked (they need a live DB) and output
 * goes to an {@link InMemoryArtifactStore}, so "no fetch" is checkable as "the downloader's
 * fetch methods were never called" and the registry/node cache are real, on the in-memory store.
 */
@DisplayName("DependencyCrawler idempotency registry")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DependencyCrawlerIdempotencyTest {

    private static final ArtifactKey OUT = ArtifactKey.of("plsql-parse", "ORACLE", "PROJ", "ENV");
    private static final String PKG_SRC =
            "CREATE OR REPLACE PACKAGE BODY PKG AS\n"
            + "  PROCEDURE P IS BEGIN NULL; END P;\n"
            + "END PKG;\n/\n";

    @Mock private SourceDownloader downloader;
    @Mock private SchemaResolver schemaResolver;

    private PlSqlParserEngine engine;
    private InMemoryArtifactStore store;

    @BeforeEach
    void setup() {
        engine = new PlSqlParserEngine();
        store = new InMemoryArtifactStore();
        AnalysisRegistry.resetCommittedForTest(OUT);

        when(schemaResolver.getTransitiveDependencies(anyString())).thenReturn(Collections.emptySet());
        when(schemaResolver.resolveObjectType(anyString())).thenReturn(null);
        when(schemaResolver.getConfiguredSchemas()).thenReturn(List.of("HR"));
        when(schemaResolver.resolveSchema(anyString())).thenReturn(null);

        when(downloader.getConnManager()).thenReturn(null);
        when(downloader.downloadAny(anyString())).thenReturn(PKG_SRC);
        when(downloader.downloadPackageBody(anyString())).thenReturn(PKG_SRC);
        when(downloader.getCachedSource(anyString())).thenReturn(PKG_SRC);
        when(downloader.isWrapped(anyString())).thenReturn(false);
        when(downloader.getOwner(anyString())).thenReturn("HR");
    }

    @AfterEach
    void cleanup() {
        AnalysisRegistry.resetCommittedForTest(OUT);
    }

    /** A fresh crawler per crawl (a real re-run is a new instance with an empty in-memory cache). */
    private DependencyCrawler newCrawler() {
        DependencyCrawler c = new DependencyCrawler(engine, downloader, schemaResolver);
        c.setMaxDepth(5);
        c.setTimeoutPerEntryMs(60_000);
        c.setOutputDir(store, OUT);
        c.setOutputFolderName("PKG");
        try {
            c.setNodeCache(new NodeParseCache(store, OUT));
            c.setParseResultCache(new ParseResultCache(store, OUT));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return c;
    }

    private void assertNoDownloaderFetch() {
        verify(downloader, never()).downloadAny(anyString());
        verify(downloader, never()).downloadPackageBody(anyString());
        verify(downloader, never()).downloadPackageSpec(anyString());
        verify(downloader, never()).downloadSource(anyString(), anyString());
    }

    @Test
    @DisplayName("second crawl of an unchanged object skips it — no DB fetch at all")
    void secondCrawlSkipsUnchangedObjectWithNoFetch() {
        FlowResult first = newCrawler().crawl("PKG");
        assertEquals(1, first.getTotalObjectsCrawled(), "first crawl analyses PKG");

        // Force a store reload (not just the in-JVM snapshot) to prove persistence.
        AnalysisRegistry.resetCommittedForTest(OUT);
        clearInvocations(downloader);

        FlowResult second = newCrawler().crawl("PKG");

        assertEquals(1, second.getTotalObjectsCrawled(), "the node is restored, not dropped");
        assertNoDownloaderFetch();
    }

    @Test
    @DisplayName("LAST_DDL change forces a re-analysis (a fetch happens)")
    void lastDdlChangeReanalyses() {
        when(downloader.getCatalogLastDdl(anyString())).thenReturn("2026-01-01 00:00:00");
        newCrawler().crawl("PKG");

        AnalysisRegistry.resetCommittedForTest(OUT);
        clearInvocations(downloader);
        // The catalog now reports a newer DDL for the same object.
        when(downloader.getCatalogLastDdl(anyString())).thenReturn("2026-02-02 00:00:00");

        newCrawler().crawl("PKG");

        verify(downloader, atLeastOnce()).downloadAny(anyString());
    }

    @Test
    @DisplayName("partial prior run (node-cache entry gone) re-analyses")
    void partialRunReanalyses() {
        newCrawler().crawl("PKG");

        // Simulate a partial/incomplete prior run: the registry says complete, but the node-cache
        // artifact it points at is missing, so the object must be re-analysed rather than skipped.
        store.deletePrefix(OUT.resolve(".node-cache"));
        AnalysisRegistry.resetCommittedForTest(OUT);
        clearInvocations(downloader);

        newCrawler().crawl("PKG");

        verify(downloader, atLeastOnce()).downloadAny(anyString());
    }

    @Test
    @DisplayName("force-refresh overrides the skip")
    void forceRefreshOverrides() {
        newCrawler().crawl("PKG");

        AnalysisRegistry.resetCommittedForTest(OUT);
        clearInvocations(downloader);

        DependencyCrawler forced = newCrawler();
        forced.setForceRefresh(true);
        forced.crawl("PKG");

        verify(downloader, atLeastOnce()).downloadAny(anyString());
    }

    @Test
    @DisplayName("registry scope isolation: a second scope does NOT skip on the first scope's entries")
    void secondScopeDoesNotSkip() {
        newCrawler().crawl("PKG");

        // A different analysis scope: fresh store + fresh outputKey => independent registry.
        ArtifactKey otherOut = ArtifactKey.of("plsql-parse", "ORACLE", "PROJ", "ENV2");
        InMemoryArtifactStore otherStore = new InMemoryArtifactStore();
        AnalysisRegistry.resetCommittedForTest(otherOut);
        clearInvocations(downloader);

        DependencyCrawler other = new DependencyCrawler(engine, downloader, schemaResolver);
        other.setMaxDepth(5);
        other.setTimeoutPerEntryMs(60_000);
        other.setOutputDir(otherStore, otherOut);
        other.setOutputFolderName("PKG");
        try {
            other.setNodeCache(new NodeParseCache(otherStore, otherOut));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        other.crawl("PKG");

        verify(downloader, atLeastOnce()).downloadAny(anyString());
        AnalysisRegistry.resetCommittedForTest(otherOut);
    }
}
