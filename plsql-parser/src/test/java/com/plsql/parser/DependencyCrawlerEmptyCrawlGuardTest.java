package com.plsql.parser;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
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

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * The empty-crawl guard.
 *
 * <p>Live incident this pins down: a PL/SQL parse was submitted while the Oracle DB was down
 * ({@code ORA-12541: No listener}). The crawler got 0 sources, the pipeline treated it as SUCCESS
 * and wrote an EMPTY analysis (0 nodes, 2-byte sidecars) OVER an existing 51-node analysis of the
 * same identity. A dead-DB crawl must never be a successful empty write.
 *
 * <p>Uses the {@link DependencyCrawlerErrorTest} mock pattern: {@link SourceDownloader} and
 * {@link SchemaResolver} are mocked (they need a live DB), and the output goes to an
 * {@link InMemoryArtifactStore} so the guard's "did anything get written" claim is checkable
 * without touching disk.
 */
@DisplayName("DependencyCrawler empty-crawl guard")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DependencyCrawlerEmptyCrawlGuardTest {

    private static final String EXPECTED_MESSAGE =
            "Crawled 0 sources — the database had nothing to analyze (is it up and does the "
            + "schema contain objects?). Refusing to write an empty analysis.";

    @Mock private SourceDownloader downloader;
    @Mock private SchemaResolver schemaResolver;

    private PlSqlParserEngine engine;
    private DependencyCrawler crawler;
    private InMemoryArtifactStore store;
    private ArtifactKey buildKey;

    @BeforeEach
    void setup() {
        engine = new PlSqlParserEngine();
        crawler = new DependencyCrawler(engine, downloader, schemaResolver);
        crawler.setMaxDepth(5);
        crawler.setTimeoutPerEntryMs(10_000);

        store = new InMemoryArtifactStore();
        crawler.setOutputDir(store, ArtifactKey.of("plsql-parse", "ORACLE", "ORACLEDEMO", "LOCAL"));
        crawler.setOutputFolderName("ERP_USP_RUN_CYCLE");
        buildKey = ArtifactKey.of("plsql-parse", "ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE");

        // Dead DB / empty schema: nothing downloadable, no dependencies, no object types.
        when(schemaResolver.getTransitiveDependencies(anyString())).thenReturn(Collections.emptySet());
        when(schemaResolver.resolveObjectType(anyString())).thenReturn(null);
        when(downloader.downloadPackageBody(anyString())).thenReturn(null);
        when(downloader.downloadPackageSpec(anyString())).thenReturn(null);
        when(downloader.downloadAny(anyString())).thenReturn(null);
        when(downloader.getCachedType(anyString())).thenReturn(null);
        when(downloader.getCachedSource(anyString())).thenReturn(null);
    }

    @Test
    @DisplayName("0 sources -> IllegalStateException with the exact message, and no index written")
    void throwsAndWritesNoIndexWhenCrawlYieldsZeroSources() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> crawler.crawl("ERP_USP_RUN_CYCLE"));

        assertEquals(EXPECTED_MESSAGE, ex.getMessage());
        // The analysis identity's index was never written — this is the artifact whose 0-node
        // overwrite destroyed the live data.
        assertFalse(store.exists(buildKey.resolve("api", "index.json")),
                "an empty crawl must not write api/index.json");
    }

    @Test
    @DisplayName("0 sources with an existing populated index -> throws, existing index untouched")
    void leavesAnExistingPopulatedIndexUntouched() throws Exception {
        ArtifactKey indexKey = buildKey.resolve("api", "index.json");
        byte[] existing = "{\"totalNodes\":51,\"nodes\":[1,2,3]}".getBytes(StandardCharsets.UTF_8);
        try (OutputStream out = store.write(indexKey)) {
            out.write(existing);
        }

        assertThrows(IllegalStateException.class, () -> crawler.crawl("ERP_USP_RUN_CYCLE"));

        assertTrue(store.exists(indexKey), "the pre-existing index must survive");
        try (var in = store.read(indexKey)) {
            assertArrayEquals(existing, in.readAllBytes(),
                    "the pre-existing 51-node index must be byte-for-byte untouched");
        }
    }

    @Test
    @DisplayName("allowEmptyAnalysis=true opts out: the empty analysis is written as before")
    void allowEmptyAnalysisOptOutStillWrites() {
        crawler.setAllowEmptyAnalysis(true);

        FlowResult result = crawler.crawl("ERP_USP_RUN_CYCLE");

        assertNotNull(result);
        assertTrue(store.exists(buildKey.resolve("api", "index.json")),
                "with the opt-out the empty analysis is written");
    }

    @Test
    @DisplayName("no output dir (CLI stdout) still returns an empty result without throwing")
    void noOutputDirStillReturnsEmptyResultForCliStdout() {
        DependencyCrawler stdoutCrawler = new DependencyCrawler(engine, downloader, schemaResolver);
        stdoutCrawler.setMaxDepth(5);
        stdoutCrawler.setTimeoutPerEntryMs(10_000);

        FlowResult result = stdoutCrawler.crawl("ERP_USP_RUN_CYCLE");

        assertNotNull(result, "the in-memory/stdout path writes nothing, so it is not guarded");
    }
}
