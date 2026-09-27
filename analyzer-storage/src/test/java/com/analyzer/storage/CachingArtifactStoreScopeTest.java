package com.analyzer.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * The disk cache is the outermost store a consumer injects, and it wraps the per-user router. A
 * fan-out captures the scope from that injected bean, so the cache must pass the request through to
 * the router it wraps — capturing from the cache itself would carry nothing and re-open the
 * routing hole this exists to close.
 */
class CachingArtifactStoreScopeTest {

    @TempDir Path tmp;

    @Test
    void forwardsCaptureToARoutedDelegate() {
        RequestScopedArtifactStore routed = org.mockito.Mockito.mock(RequestScopedArtifactStore.class);
        RequestScopedArtifactStore.Scope scope = () -> () -> { };
        org.mockito.Mockito.when(routed.captureScope()).thenReturn(scope);

        CachingArtifactStore cache = new CachingArtifactStore("t", routed, tmp, 0);

        assertSame(scope, cache.captureScope());
    }

    @Test
    void aPlainDelegateCarriesNothing() {
        CachingArtifactStore cache = new CachingArtifactStore("t", new InMemoryArtifactStore(), tmp, 0);

        assertSame(RequestScopedArtifactStore.Scope.NONE, cache.captureScope());
    }
}
