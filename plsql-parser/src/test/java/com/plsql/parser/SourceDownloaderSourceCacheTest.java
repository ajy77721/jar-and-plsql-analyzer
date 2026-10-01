package com.plsql.parser;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.LocalArtifactStore;
import com.plsql.parser.flow.DbConnectionManager;
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

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * The persistent, DDL-validated source cache: a cache hit must skip the JDBC source fetch
 * entirely; a miss (no entry, or a changed LAST_DDL) must re-fetch.
 */
@DisplayName("SourceDownloader persistent source cache")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class SourceDownloaderSourceCacheTest {

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
    private static final String SOURCE = "CREATE OR REPLACE BEGIN\n  NULL;\nEND;\n";

    @BeforeEach
    void setup() throws Exception {
        when(connManager.getAvailableSchemas()).thenReturn(List.of("APP"));
        when(connManager.isOracle()).thenReturn(true);
        when(connManager.isCircuitOpen(anyString())).thenReturn(false);
        when(connManager.getAnyConnection()).thenReturn(conn);
        // The crawler's catalog/source queries go through the guarded statement creator
        // (read-only guard + queryTimeout), so the downloader calls connManager.prepareStatement.
        when(connManager.prepareStatement(any(Connection.class), anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("OWNER")).thenReturn("APP");
        when(rs.getString("TEXT")).thenReturn("BEGIN\n  NULL;\nEND;\n");
    }

    private SourceDownloader newDownloader(String lastDdl) {
        SourceDownloader d = new SourceDownloader(connManager);
        d.setSourceCache(new LocalArtifactStore(tempDir), ArtifactKey.of(".source-cache"), IDENTITY);
        d.primeCatalogMeta("PKG_A", "APP", "PACKAGE BODY", lastDdl);
        return d;
    }

    @Test
    @DisplayName("First fetch populates the cache, second (unchanged LAST_DDL) skips the DB fetch")
    void cacheHitSkipsFetch() throws Exception {
        SourceDownloader first = newDownloader("2024-01-01 00:00:00");
        String s1 = first.downloadPackageBody("PKG_A");
        assertEquals(SOURCE, s1, "first fetch should return the DB source");
        verify(connManager, times(1)).getAnyConnection();

        // A brand-new downloader over the SAME cache: the object is unchanged, so the source must
        // come from disk and the connection manager must never be touched.
        DbConnectionManager cm2 = mock(DbConnectionManager.class);
        SourceDownloader second = new SourceDownloader(cm2);
        second.setSourceCache(new LocalArtifactStore(tempDir), ArtifactKey.of(".source-cache"), IDENTITY);
        second.primeCatalogMeta("PKG_A", "APP", "PACKAGE BODY", "2024-01-01 00:00:00");

        String s2 = second.downloadPackageBody("PKG_A");
        assertEquals(SOURCE, s2, "cache hit should return the cached source");
        verify(cm2, never()).getAnyConnection();
    }

    @Test
    @DisplayName("Changed LAST_DDL invalidates the entry and re-fetches")
    void staleCacheRefetches() throws Exception {
        SourceDownloader first = newDownloader("2024-01-01 00:00:00");
        assertEquals(SOURCE, first.downloadPackageBody("PKG_A"));

        DbConnectionManager cm2 = mock(DbConnectionManager.class);
        when(cm2.getAvailableSchemas()).thenReturn(List.of("APP"));
        when(cm2.isOracle()).thenReturn(true);
        when(cm2.isCircuitOpen(anyString())).thenReturn(false);
        Connection conn2 = mock(Connection.class);
        PreparedStatement ps2 = mock(PreparedStatement.class);
        ResultSet rs2 = mock(ResultSet.class);
        when(cm2.getAnyConnection()).thenReturn(conn2);
        when(cm2.prepareStatement(any(Connection.class), anyString())).thenReturn(ps2);
        when(ps2.executeQuery()).thenReturn(rs2);
        when(rs2.next()).thenReturn(true, false);
        when(rs2.getString("OWNER")).thenReturn("APP");
        when(rs2.getString("TEXT")).thenReturn("BEGIN\n  NULL;\nEND;\n");

        SourceDownloader second = new SourceDownloader(cm2);
        second.setSourceCache(new LocalArtifactStore(tempDir), ArtifactKey.of(".source-cache"), IDENTITY);
        second.primeCatalogMeta("PKG_A", "APP", "PACKAGE BODY", "2025-06-30 12:00:00");

        String s2 = second.downloadPackageBody("PKG_A");
        assertEquals(SOURCE, s2);
        verify(cm2, times(1)).getAnyConnection(); // stale -> went to the DB
    }

    @Test
    @DisplayName("No entry is a miss and re-fetches")
    void cacheMissRefetches() throws Exception {
        SourceDownloader d = newDownloader("2024-01-01 00:00:00");
        String s = d.downloadPackageBody("PKG_A");
        assertEquals(SOURCE, s);
        verify(connManager, times(1)).getAnyConnection();
    }
}
