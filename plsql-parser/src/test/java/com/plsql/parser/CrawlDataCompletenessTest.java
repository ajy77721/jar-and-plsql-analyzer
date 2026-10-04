package com.plsql.parser;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.LocalArtifactStore;
import com.plsql.parser.flow.DbConnectionManager;
import com.plsql.parser.flow.LayoutManifest;
import com.plsql.parser.flow.SourceDownloader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * DATA-COMPLETENESS INVARIANTS after ANY crawl path.
 *
 * <p>The regression this guards: the A2 bulk source pull returned fetched texts to the parser but
 * did not materialise the per-object {@code sources/<SCHEMA>.<NAME>.<ext>} artifacts, so every
 * bulk-pulled object (e.g. the {@code .prc} procedures) had no source file and the UI's
 * {@code /source/<name>.prc} panel 404'd. The per-object/remainder path DID write them — hence the
 * observed "all .fnc, zero .prc" sources/ folder.
 *
 * <p>These tests assert the invariant directly against the real {@link SourceDownloader} over a
 * real {@link ArtifactStore}: every object a crawl path resolves gets its sources/ artifact, for
 * BOTH the bulk path and the per-object path, with the type-correct extension. A future path change
 * that drops the persistence fails here instantly.
 */
@DisplayName("Crawl data completeness — every resolved object has its sources/ artifact")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class CrawlDataCompletenessTest {

    @TempDir
    Path tempDir;

    @Mock
    private DbConnectionManager connManager;

    @Mock
    private Connection conn;

    @Mock
    private PreparedStatement ps;

    @Mock
    private ResultSet rs;

    private static final String IDENTITY = "ORACLE|jdbc:oracle:thin:@//db:1521/SVC";

    private static final String PROC_TEXT =
            "PROCEDURE PROC_A AS\nBEGIN\n  NULL;\nEND PROC_A;\n";
    private static final String FUNC_TEXT =
            "FUNCTION FUNC_B RETURN NUMBER AS\nBEGIN\n  RETURN 1;\nEND FUNC_B;\n";
    private static final String PKG_TEXT =
            "PACKAGE BODY PKG_C AS\n  PROCEDURE P IS BEGIN NULL; END;\nEND PKG_C;\n";

    private ArtifactStore store;

    @BeforeEach
    void setup() throws Exception {
        store = new LocalArtifactStore(tempDir);
        when(connManager.getAvailableSchemas()).thenReturn(List.of("APP"));
        when(connManager.isOracle()).thenReturn(true);
        when(connManager.isCircuitOpen(anyString())).thenReturn(false);
        when(connManager.getAnyConnection()).thenReturn(conn);
        when(connManager.prepareStatement(any(Connection.class), anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
    }

    private SourceDownloader newDownloader() {
        SourceDownloader d = new SourceDownloader(connManager);
        // The same wiring DependencyCrawler installs: sources/ is the local-cache prefix, and the
        // persistent DDL-validated cache lives alongside it.
        d.setLocalCacheDir(store, ArtifactKey.of("sources"));
        d.setSourceCache(store, ArtifactKey.of(".source-cache"), IDENTITY);
        return d;
    }

    private static void mockBulkRows(ResultSet rs) throws Exception {
        when(rs.next()).thenReturn(true, true, true, false);
        when(rs.getString("OWNER")).thenReturn("APP", "APP", "APP");
        when(rs.getString("NAME")).thenReturn("PROC_A", "FUNC_B", "PKG_C");
        when(rs.getString("TYPE")).thenReturn("PROCEDURE", "FUNCTION", "PACKAGE BODY");
        when(rs.getString("TEXT")).thenReturn(PROC_TEXT, FUNC_TEXT, PKG_TEXT);
    }

    private String read(String... segments) throws Exception {
        ArtifactKey key = ArtifactKey.of(segments);
        assertTrue(store.exists(key), "expected sources/ artifact to exist: " + key);
        try (InputStream in = store.read(key)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    @DisplayName("BULK path: every fetched object is persisted to sources/<SCHEMA>.<NAME>.<ext>")
    void bulkPathPersistsEveryObject() throws Exception {
        mockBulkRows(rs);
        SourceDownloader d = newDownloader();

        Map<String, String> bulk = d.fetchSourcesBulk(List.of("PROC_A", "FUNC_B", "PKG_C"));

        assertEquals(3, bulk.size(), "bulk pull should resolve all three objects");
        // .prc — the file family the regression was missing.
        String prc = read("sources", "APP.PROC_A.prc");
        assertTrue(prc.contains("PROCEDURE PROC_A"), "prc artifact must carry the fetched text");
        // .fnc
        String fnc = read("sources", "APP.FUNC_B.fnc");
        assertTrue(fnc.contains("FUNCTION FUNC_B"), "fnc artifact must carry the fetched text");
        // package body -> .pkb
        String pkb = read("sources", "APP.PKG_C.pkb");
        assertTrue(pkb.contains("PACKAGE BODY PKG_C"), "pkb artifact must carry the fetched text");
        // no stray wrong-extension files (the observed "all .fnc" failure mode)
        assertFalse(store.exists(ArtifactKey.of("sources", "APP.PROC_A.fnc")));
        assertFalse(store.exists(ArtifactKey.of("sources", "APP.PROC_A.sql")));
    }

    @Test
    @DisplayName("PER-OBJECT path: a fetched object is persisted to sources/<SCHEMA>.<NAME>.<ext>")
    void perObjectPathPersistsObject() throws Exception {
        // downloadAnyOracle: one ALL_SOURCE row for PROC_A.
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("OWNER")).thenReturn("APP");
        when(rs.getString("TYPE")).thenReturn("PROCEDURE");
        when(rs.getString("TEXT")).thenReturn(PROC_TEXT);

        SourceDownloader d = newDownloader();
        String src = d.downloadAny("PROC_A");

        assertNotNull(src, "per-object fetch should resolve the procedure");
        String prc = read("sources", "APP.PROC_A.prc");
        assertEquals(src, prc, "the persisted artifact must equal the returned source");
    }

    @Test
    @DisplayName("BULK path: a truncated source is NOT persisted as a stub (per-object fallback owns it)")
    void truncatedBulkSourceNotPersisted() throws Exception {
        // No trailing END/; -> looksLikeTruncated -> the bulk path must skip, not cache a stub.
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("OWNER")).thenReturn("APP");
        when(rs.getString("NAME")).thenReturn("PROC_A");
        when(rs.getString("TYPE")).thenReturn("PROCEDURE");
        when(rs.getString("TEXT")).thenReturn("PROCEDURE PROC_A AS\nBEGIN\n  NULL;\n");

        SourceDownloader d = newDownloader();
        Map<String, String> bulk = d.fetchSourcesBulk(List.of("PROC_A"));

        assertTrue(bulk.isEmpty(), "a truncated bulk source must not be returned as complete");
        assertFalse(store.exists(ArtifactKey.of("sources", "APP.PROC_A.prc")),
                "a truncated source must not be persisted as a sources/ stub");
    }

    @Test
    @DisplayName("MANIFEST invariant: a recorded artifact length equals the real stored file size")
    void manifestArtifactLengthMatchesRealFileSize() throws Exception {
        String content = "CREATE OR REPLACE PROCEDURE PROC_A AS\nBEGIN NULL; END;\n";
        ArtifactKey key = ArtifactKey.of("sources", "APP.PROC_A.prc");
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        try (var out = store.write(key)) {
            out.write(bytes);
        }

        LayoutManifest manifest = new LayoutManifest("test-analysis");
        manifest.recordArtifact(key, bytes.length);
        Map<String, Object> doc = manifest.toDocument(store);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> artifacts = (List<Map<String, Object>>) doc.get("artifacts");
        Map<String, Object> entry = artifacts.stream()
                .filter(a -> key.toString().equals(a.get("key")))
                .findFirst().orElseThrow(() -> new AssertionError("source artifact not in manifest"));
        assertEquals(bytes.length, ((Number) entry.get("length")).longValue(),
                "manifest length must equal the real stored size");
        assertEquals(store.size(key), ((Number) entry.get("length")).longValue());
    }
}
