package com.plsql.parser;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.plsql.parser.flow.DependencyCrawler;
import com.plsql.parser.flow.SchemaResolver;
import com.plsql.parser.flow.SourceDownloader;
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
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Fix C — the depth-0 entry-object lookup.
 *
 * <p>The UI routinely submits a PACKAGE name with {@code objectType=PROCEDURE} (or an owner that
 * does not match). The crawler treated a bare name as a standalone routine, failed to download it,
 * and reported "Entry object 'X' not found on the selected connection" even though the object was
 * right there under the same name as a package.
 *
 * <p>Two behaviours are pinned here:
 * <ol>
 *   <li>a bare entry that is not a standalone routine is retried as a PACKAGE BODY / PACKAGE before
 *       it is declared missing — and the resulting node is the aggregated package node, exactly as a
 *       {@code objectType=PACKAGE} request would have produced;</li>
 *   <li>when it is genuinely missing the error names every type tried and every schema searched, so
 *       a wrong type or owner is self-correctable from the message.</li>
 * </ol>
 */
@DisplayName("DependencyCrawler: entry-object resolution falls back to PACKAGE and explains itself")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class DependencyCrawlerEntryObjectResolutionTest {

    private static final String PACKAGE_NAME = "MY_PACKAGE";

    private static final String PACKAGE_BODY = """
            CREATE OR REPLACE PACKAGE BODY MY_PACKAGE AS
              PROCEDURE do_work IS
                v_n NUMBER;
              BEGIN
                v_n := 1;
              END do_work;
            END MY_PACKAGE;
            /
            """;

    @Mock
    private SourceDownloader downloader;

    @Mock
    private SchemaResolver schemaResolver;

    private DependencyCrawler crawler;

    @BeforeEach
    void setup() {
        PlSqlParserEngine engine = new PlSqlParserEngine();
        crawler = new DependencyCrawler(engine, downloader, schemaResolver);
        crawler.setMaxDepth(5);
        crawler.setTimeoutPerEntryMs(10_000);
        // Real chunked writer so the produced FlowNodes are observable, exactly as the other
        // DependencyCrawler tests do.
        crawler.setOutputDir(new InMemoryArtifactStore(),
                ArtifactKey.of("dependency-crawler-entry-resolution-test"));

        when(schemaResolver.getTransitiveDependencies(anyString())).thenReturn(Collections.emptySet());
        when(schemaResolver.getConfiguredSchemas()).thenReturn(List.of("ANALYZER"));
        when(schemaResolver.resolveObjectType(anyString())).thenReturn(null);
        when(schemaResolver.resolveSchema(anyString())).thenReturn("TEST_SCHEMA");
        when(schemaResolver.resolveSchema(anyString(), anyString(), anyString())).thenReturn("TEST_SCHEMA");
        when(downloader.getCachedSource(anyString())).thenReturn(null);
        when(downloader.getCachedType(anyString())).thenReturn(null);
        when(downloader.getOwner(anyString())).thenReturn(null);
        when(downloader.isWrapped(anyString())).thenReturn(false);
    }

    @Test
    @DisplayName("a PACKAGE submitted as PROCEDURE is resolved as a package, not reported missing")
    void packageNameSubmittedAsProcedureFallsBackToPackage() {
        // The requested type is PROCEDURE, so the standalone fetch is tried first — and finds
        // nothing, because the object is a package body.
        when(downloader.downloadAny(PACKAGE_NAME)).thenReturn(null);
        // The fallback fetches it as a package.
        when(downloader.downloadPackageBody(PACKAGE_NAME)).thenReturn(PACKAGE_BODY);
        when(downloader.downloadPackageSpec(PACKAGE_NAME)).thenReturn(null);

        crawler.setEntryObjectType("PROCEDURE");
        FlowResult result = crawler.crawl(PACKAGE_NAME);

        assertNotNull(result);
        FlowNode pkgNode = result.getFlowNodes().stream()
                .filter(n -> PACKAGE_NAME.equalsIgnoreCase(n.getObjectName()))
                .findFirst().orElse(null);
        assertNotNull(pkgNode,
                "the package must be crawled as the entry object, not dropped as 'not found'");

        assertTrue(result.getErrors().stream().noneMatch(e -> e.contains("not found")),
                "no 'not found' error may be produced once the package fallback resolved the entry; got: "
                        + result.getErrors());
    }

    @Test
    @DisplayName("a genuinely missing entry still fails, and the message names the types tried and schemas searched")
    void genuinelyMissingEntryReportsWhatWasTried() {
        when(downloader.downloadAny(PACKAGE_NAME)).thenReturn(null);
        when(downloader.downloadPackageBody(PACKAGE_NAME)).thenReturn(null);
        when(downloader.downloadPackageSpec(PACKAGE_NAME)).thenReturn(null);
        // The empty-crawl guard would otherwise refuse to write the (empty) result; this test is
        // about the error text, so let the empty crawl through.
        crawler.setAllowEmptyAnalysis(true);
        crawler.setEntryObjectType("PROCEDURE");

        FlowResult result = crawler.crawl(PACKAGE_NAME);

        assertNotNull(result);
        String notFound = result.getErrors().stream()
                .filter(e -> e.contains("not found"))
                .findFirst().orElse(null);
        assertNotNull(notFound, "a genuinely missing entry must still be reported; got: "
                + result.getErrors());
        assertTrue(notFound.contains("tried PROCEDURE"), notFound);
        assertTrue(notFound.contains("PACKAGE"), notFound);
        assertTrue(notFound.contains("schemas: ANALYZER"),
                "the searched schemas must be named so a wrong owner is self-correctable; got: " + notFound);
    }

    @Test
    @DisplayName("when the caller already asked for a PACKAGE, the fallback does not double-fetch")
    void packageRequestDoesNotTriggerFallback() {
        // A bare package request is resolved by the ordinary all-types fetch (downloadAny), which is
        // what the package-as-entry-point path already used. The fallback must not run on top of it.
        when(downloader.downloadAny(PACKAGE_NAME)).thenReturn(PACKAGE_BODY);
        when(downloader.downloadPackageBody(PACKAGE_NAME)).thenReturn(null);
        when(downloader.downloadPackageSpec(PACKAGE_NAME)).thenReturn(null);

        crawler.setEntryObjectType("PACKAGE");
        FlowResult result = crawler.crawl(PACKAGE_NAME);

        assertNotNull(result);
        assertTrue(result.getFlowNodes().stream()
                        .anyMatch(n -> PACKAGE_NAME.equalsIgnoreCase(n.getObjectName())),
                "a package entry must still produce its aggregated node");
        assertTrue(result.getErrors().stream().noneMatch(e -> e.contains("not found")),
                "a package that downloaded fine must not report 'not found'; got: " + result.getErrors());
    }
}
