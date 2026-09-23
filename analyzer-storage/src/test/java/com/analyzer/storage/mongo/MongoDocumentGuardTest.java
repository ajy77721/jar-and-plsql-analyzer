package com.analyzer.storage.mongo;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.CredentialGuardStore;
import com.analyzer.storage.LocalArtifactStore;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The 12 MB soft limit, and the rule that nothing is ever silently truncated.
 *
 * <p>No MongoDB needed: the guard is the decision about <em>whether</em> a payload may go into a
 * document, which is pure arithmetic plus one {@code ArtifactStore} write. Backing it with
 * {@code LocalArtifactStore} rather than a mock is deliberate — the diversion has to actually
 * produce a readable artifact, and a mock would assert that a method was called rather than that
 * the data survived.
 */
class MongoDocumentGuardTest {

    @TempDir Path tmp;

    private static String jsonOf(int payloadBytes) {
        // A realistic shape: one enormous field on an otherwise ordinary row. This is PLAN-06's
        // stated common case — an embedded decompiled `sourceCode` or a long SQL body.
        return "{\"name\":\"Big\",\"sourceCode\":\"" + "x".repeat(payloadBytes) + "\"}";
    }

    // ── the small case ───────────────────────────────────────────────────────

    @Test
    @DisplayName("a payload under the limit is stored inline, with no artifact written")
    void smallPayloadStaysInline() throws IOException {
        ArtifactStore blobs = new LocalArtifactStore(tmp);
        MongoDocumentGuard guard = new MongoDocumentGuard(blobs, 1024);

        String json = jsonOf(100);
        Document stored = guard.store(json);

        assertEquals(json, stored.getString(MongoDocumentGuard.F_JSON));
        assertNull(stored.getString(MongoDocumentGuard.F_BLOB_KEY));
        assertFalse(MongoDocumentGuard.diverted(stored));
        assertEquals(json.getBytes(StandardCharsets.UTF_8).length,
                stored.get(MongoDocumentGuard.F_LEN, Number.class).longValue());
        assertEquals(json, guard.load(stored));
        assertEquals(0, artifactCount(blobs), "no artifact should have been written");
    }

    @Test
    @DisplayName("the boundary: at the limit is inline, one byte over is diverted")
    void limitBoundaryIsExact() throws IOException {
        ArtifactStore blobs = new LocalArtifactStore(tmp);
        int limit = 512;
        MongoDocumentGuard guard = new MongoDocumentGuard(blobs, limit);

        String atLimit = "a".repeat(limit);
        String overLimit = "a".repeat(limit + 1);

        assertFalse(MongoDocumentGuard.diverted(guard.store(atLimit)), "at the limit must be inline");
        assertTrue(MongoDocumentGuard.diverted(guard.store(overLimit)), "one byte over must divert");
    }

    // ── the diversion ────────────────────────────────────────────────────────

    @Test
    @DisplayName("an oversized element is diverted to blob storage — never truncated, never rejected")
    void oversizedPayloadIsDivertedIntact() throws IOException {
        ArtifactStore blobs = new LocalArtifactStore(tmp);
        MongoDocumentGuard guard = new MongoDocumentGuard(blobs, 1024);

        String json = jsonOf(50_000);
        Document stored = guard.store(json);

        // The two failure modes the guard exists to avoid, asserted directly.
        assertTrue(MongoDocumentGuard.diverted(stored));
        assertNull(stored.getString(MongoDocumentGuard.F_JSON),
                "a diverted payload must not ALSO be inline");
        assertEquals(json, guard.load(stored),
                "the payload came back changed — it was truncated or re-encoded");
        assertEquals(json.getBytes(StandardCharsets.UTF_8).length,
                stored.get(MongoDocumentGuard.F_LEN, Number.class).longValue(),
                "the recorded length must be the FULL length, not the stored-in-document length");

        // PLAN-06: "A document references an artifact by storing its ArtifactKey string plus size
        // and a sha256." All three are present, and the key resolves.
        String key = stored.getString(MongoDocumentGuard.F_BLOB_KEY);
        String sha = stored.getString(MongoDocumentGuard.F_SHA256);
        assertNotNull(key);
        assertNotNull(sha);
        assertEquals(64, sha.length(), "sha256 should be 64 hex characters");
        assertTrue(key.endsWith(sha), "the key must be derived from the content hash");
        assertTrue(blobs.exists(MongoDocumentGuard.keyOf(key)), "the diverted artifact is missing");
        assertEquals(json.getBytes(StandardCharsets.UTF_8).length,
                blobs.size(MongoDocumentGuard.keyOf(key)));
    }

    @Test
    @DisplayName("identical oversized payloads deduplicate — content addressing, not a coincidence")
    void identicalPayloadsShareOneArtifact() throws IOException {
        // PLAN-06 expects this to fire on generated code, where identical method bodies repeat
        // across nodes. Two rows, one artifact.
        ArtifactStore blobs = new LocalArtifactStore(tmp);
        MongoDocumentGuard guard = new MongoDocumentGuard(blobs, 1024);

        String json = jsonOf(20_000);
        Document a = guard.store(json);
        Document b = guard.store(json);

        assertEquals(a.getString(MongoDocumentGuard.F_BLOB_KEY),
                     b.getString(MongoDocumentGuard.F_BLOB_KEY));
        assertEquals(1, artifactCount(blobs));
        assertEquals(json, guard.load(b));
    }

    @Test
    @DisplayName("different oversized payloads get different artifacts")
    void differentPayloadsDoNotCollide() throws IOException {
        ArtifactStore blobs = new LocalArtifactStore(tmp);
        MongoDocumentGuard guard = new MongoDocumentGuard(blobs, 1024);

        Document a = guard.store(jsonOf(20_000));
        Document b = guard.store(jsonOf(20_001));

        assertFalse(a.getString(MongoDocumentGuard.F_BLOB_KEY)
                .equals(b.getString(MongoDocumentGuard.F_BLOB_KEY)));
        assertEquals(2, artifactCount(blobs));
    }

    // ── the real limit, with a real oversized input ─────────────────────────

    @Test
    @DisplayName("a 14 MB element — larger than any measured artifact element — diverts at the REAL 12 MB limit")
    void syntheticInputLargerThanAnythingRealIsHandledAtTheProductionLimit() throws IOException {
        // Every other test in this class lowers the limit so it can run in milliseconds. This one
        // does not, because a guard that is only ever exercised at a fake threshold has not been
        // shown to work at the real one — an int overflow, an off-by-a-factor-of-1024, or a
        // measured-in-characters bug would all survive the fast tests and fail here.
        //
        // 14 MB is deliberately above the 12 MB soft limit AND above the 16 MB BSON ceiling's
        // practical margin, and far above the largest element ever measured: PLAN-06 records
        // api/nodes/<id>.json at 370 KB max and node_<n>.json at 6.2 KB max. The 912 MB
        // analysis.json is large in aggregate, not per element — this is what an indivisible
        // element that big would do.
        ArtifactStore blobs = new LocalArtifactStore(tmp);
        MongoDocumentGuard guard = new MongoDocumentGuard(blobs);
        assertEquals(12 * 1024 * 1024, guard.softLimitBytes(), "the soft limit is PLAN-06's 12 MB");

        String json = jsonOf(14 * 1024 * 1024);
        int expectedBytes = json.getBytes(StandardCharsets.UTF_8).length;
        assertTrue(expectedBytes > guard.softLimitBytes());

        Document stored = guard.store(json);

        assertTrue(MongoDocumentGuard.diverted(stored),
                "a 14 MB element must be diverted, not stored in a BSON document");
        assertEquals(expectedBytes, stored.get(MongoDocumentGuard.F_LEN, Number.class).longValue());
        // The whole point: the data is still there, in full.
        assertEquals(json, guard.load(stored));
    }

    @Test
    @DisplayName("bytes, not characters — a multi-byte payload under the char limit but over the byte limit diverts")
    void limitIsMeasuredInEncodedBytes() throws IOException {
        // The subtle version of the bug. 'ä' is one char and two UTF-8 bytes, so a payload of 400
        // of them is 400 characters and 800 bytes. Measuring characters would let it through a
        // 512-byte limit and hand BSON something a third larger than the guard believed.
        ArtifactStore blobs = new LocalArtifactStore(tmp);
        MongoDocumentGuard guard = new MongoDocumentGuard(blobs, 512);

        String json = "ä".repeat(400);
        assertTrue(json.length() < 512, "precondition: under the limit measured in characters");
        assertTrue(json.getBytes(StandardCharsets.UTF_8).length > 512,
                "precondition: over the limit measured in bytes");

        assertTrue(MongoDocumentGuard.diverted(guard.store(json)),
                "the limit was measured in characters, not bytes");
    }

    // ── C4 ───────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("the diversion goes THROUGH the store, so it inherits the credential guard")
    void diversionCannotBypassTheCredentialGuard() throws IOException {
        // The guard writes to whatever ArtifactStore it is handed, and in production that is the
        // CredentialGuardStore-wrapped one from StorageFactory.create. So a diverted payload lands
        // under repo-overflow/ like anything else and gets C4 for free. Asserted by handing it a
        // wrapped store and checking the diversion still works — and separately that the wrapper is
        // in fact still refusing credential keys through the same instance.
        ArtifactStore guarded = new CredentialGuardStore(new LocalArtifactStore(tmp));
        MongoDocumentGuard guard = new MongoDocumentGuard(guarded, 1024);

        String json = jsonOf(5_000);
        assertEquals(json, guard.load(guard.store(json)));

        assertThrows(SecurityException.class,
                () -> guarded.write(ArtifactKey.of("config", "plsql-env-config.yaml")));
    }

    @Test
    @DisplayName("a guard with no store is refused at construction, not at the first oversized element")
    void aStoreIsRequired() {
        assertThrows(IllegalArgumentException.class, () -> new MongoDocumentGuard(null));
        assertThrows(IllegalArgumentException.class,
                () -> new MongoDocumentGuard(new LocalArtifactStore(tmp), 0));
    }

    private static long artifactCount(ArtifactStore blobs) throws IOException {
        try (Stream<com.analyzer.storage.ArtifactEntry> s =
                     blobs.list(ArtifactKey.of(MongoDocumentGuard.OVERFLOW_ROOT), true)) {
            return s.filter(e -> !e.directory()).count();
        }
    }
}
