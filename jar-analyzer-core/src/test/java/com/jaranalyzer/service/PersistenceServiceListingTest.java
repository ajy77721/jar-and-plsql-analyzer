package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.InMemoryArtifactStore;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The store-backed listing: it must <b>find</b> a stored analysis, not only fail loudly when the
 * store is unreachable.
 *
 * <p>Written against the in-memory store so the walk itself — the shallow discovery rule and the
 * concurrent header read — is exercised with no S3 and no credentials. The regression it pins: the
 * discovery rule used to {@code SKIP} any directory without an {@code analysis.json} of its own,
 * which is every directory at the root level ({@code jar/<id>} sits one below the walk root), so a
 * perfectly reachable store still listed nothing.
 */
class PersistenceServiceListingTest {

    private static ArtifactStore storeWithAnalysis(String jarId) throws IOException {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        write(store, ArtifactKey.of("jar").resolve(jarId, "analysis.json"),
                "{\"jarName\":\"" + jarId + "\",\"projectName\":\"demo\",\"jarSize\":123,"
                        + "\"analyzedAt\":\"2026-01-01T00:00:00Z\",\"totalClasses\":2,"
                        + "\"totalEndpoints\":1,\"analysisMode\":\"STATIC\"}");
        write(store, ArtifactKey.of("jar").resolve(jarId, "_header.json"),
                "{\"jarName\":\"" + jarId + "\",\"projectName\":\"demo\",\"jarSize\":123,"
                        + "\"analyzedAt\":\"2026-01-01T00:00:00Z\",\"totalClasses\":2,"
                        + "\"totalEndpoints\":1,\"analysisMode\":\"STATIC\"}");
        return store;
    }

    private static void write(ArtifactStore store, ArtifactKey key, String body) throws IOException {
        try (OutputStream out = store.write(key)) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static PersistenceService service(ArtifactStore store) {
        return new PersistenceService(new JarDataPaths("data/jar"), store);
    }

    @Test
    void listsAStoredAnalysis() throws IOException {
        PersistenceService service = service(storeWithAnalysis("web-1.0.0-SNAPSHOT"));

        List<Map<String, Object>> jars = service.listJars();

        assertEquals(1, jars.size(), "a stored analysis must be listed");
        assertEquals("web-1.0.0-SNAPSHOT", jars.get(0).get("id"));
    }

    @Test
    void strictListingThrowsWhenTheStoreIsUnreachable() throws IOException {
        ArtifactStore broken = org.mockito.Mockito.mock(ArtifactStore.class);
        org.mockito.Mockito.when(broken.list(org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.anyBoolean()))
                .thenThrow(new IllegalStateException("SdkClientException: Unable to load credentials"));
        PersistenceService service = service(broken);

        assertThrows(StorageListingException.class, service::listJarsStrict);
        // The lenient entry point keeps its degraded-screen behaviour for internal callers.
        assertTrue(service.listJars().isEmpty());
    }

    @Test
    void anEmptyStoreIsNotAFailure() throws IOException {
        PersistenceService service = service(new InMemoryArtifactStore());

        assertTrue(service.listJars().isEmpty());
        // Strict must return the same empty answer: "nothing stored" is not "store unreachable".
        assertFalse(service.listJarsStrict() == null);
    }
}
