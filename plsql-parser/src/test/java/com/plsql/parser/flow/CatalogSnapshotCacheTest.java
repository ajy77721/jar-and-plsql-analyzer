package com.plsql.parser.flow;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A3 — the catalog snapshot cache. The correctness guard is the cheap freshness FINGERPRINT, not
 * the TTL: when the database gains an object the fingerprint changes and the snapshot refreshes, so
 * the new object appears on the very next crawl. The TTL is only a secondary fallback used when the
 * fingerprint probe itself cannot run.
 */
@DisplayName("A3 catalog snapshot cache — fingerprint-guarded freshness")
public class CatalogSnapshotCacheTest {

    /** Fake catalog: mutable fingerprint + rows, counting probes and full fetches. */
    private static final class FakeSource implements SourceDownloader.CatalogSnapshotSource {
        String fingerprint = "fp-1";
        List<String[]> rows = new ArrayList<>();
        int fingerprintCalls = 0;
        int fetchCalls = 0;
        boolean throwOnFingerprint = false;

        @Override
        public String fingerprint(List<String> schemas) throws SQLException {
            fingerprintCalls++;
            if (throwOnFingerprint) throw new SQLException("probe down");
            return fingerprint;
        }

        @Override
        public List<String[]> fetchRows(List<String> schemas) {
            fetchCalls++;
            return new ArrayList<>(rows);
        }
    }

    private static String[] row(String owner, String name, String type, String ddl) {
        return new String[]{owner, name, type, ddl};
    }

    @Test
    @DisplayName("DB adds an object -> fingerprint changes -> snapshot refreshes")
    void fingerprintChangeRefreshesSnapshot() {
        SourceDownloader d = new SourceDownloader(null);
        FakeSource fake = new FakeSource();
        d.setCatalogSnapshotSource(fake);
        fake.rows.add(row("APP", "PROC_A", "PROCEDURE", "2024-01-01 00:00:00"));

        assertTrue(d.ensureCatalogSnapshot());
        assertTrue(d.isCatalogSnapshotLoaded());
        assertNotNull(d.snapshotMeta("PROC_A"));
        assertNull(d.snapshotMeta("PROC_B"), "not yet in the DB");
        assertEquals(1, fake.fetchCalls);

        // The DB gains an object: the fingerprint changes.
        fake.fingerprint = "fp-2";
        fake.rows.add(row("APP", "PROC_B", "PROCEDURE", "2024-02-01 00:00:00"));
        assertTrue(d.ensureCatalogSnapshot());
        assertNotNull(d.snapshotMeta("PROC_B"), "new object must appear after the refresh");
        assertEquals(2, fake.fetchCalls, "a changed fingerprint forces a full refresh");

        // Unchanged fingerprint: reuse, no refresh.
        assertTrue(d.ensureCatalogSnapshot());
        assertEquals(2, fake.fetchCalls, "an unchanged fingerprint must not re-fetch the snapshot");
        assertEquals("fp-2", d.getCatalogSnapshotFingerprint());
    }

    @Test
    @DisplayName("explicit invalidation drops the snapshot")
    void invalidationDropsSnapshot() {
        SourceDownloader d = new SourceDownloader(null);
        FakeSource fake = new FakeSource();
        d.setCatalogSnapshotSource(fake);
        fake.rows.add(row("APP", "PROC_A", "PROCEDURE", null));
        assertTrue(d.ensureCatalogSnapshot());
        assertTrue(d.isCatalogSnapshotLoaded());

        d.invalidateCatalogSnapshot();
        assertFalse(d.isCatalogSnapshotLoaded());
        assertNull(d.snapshotMeta("PROC_A"));
    }

    @Test
    @DisplayName("fingerprint probe failure falls back to the TTL (secondary only)")
    void probeFailureFallsBackToTtl() {
        SourceDownloader d = new SourceDownloader(null);
        FakeSource fake = new FakeSource();
        d.setCatalogSnapshotSource(fake);
        fake.rows.add(row("APP", "PROC_A", "PROCEDURE", null));
        assertTrue(d.ensureCatalogSnapshot());
        int fetches = fake.fetchCalls;

        fake.throwOnFingerprint = true;
        assertTrue(d.ensureCatalogSnapshot(), "a fresh snapshot is reused within TTL when the probe fails");
        assertEquals(fetches, fake.fetchCalls, "no refresh happens on a probe failure");
    }
}
