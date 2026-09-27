package com.analyzer.storage;

import java.nio.file.Path;

/**
 * The {@link ArtifactStoreConformanceTest} contract, run against a {@link CachingArtifactStore}
 * whose backend is an {@link InMemoryArtifactStore}.
 *
 * <p>A cache must be invisible: every behaviour the wrapped store promises has to survive being
 * cached. Running the same suite unmodified is the cheapest proof of that — a divergence between
 * cached and uncached reads would fail here rather than surface as a wrong answer in production.
 * {@code root} from the base class becomes both the cache directory and the (unused) delegate root.
 */
class CachingArtifactStoreConformanceTest extends ArtifactStoreConformanceTest {

    @Override
    protected ArtifactStore newStore(Path root) {
        return new CachingArtifactStore("conformance", new InMemoryArtifactStore(),
                root.resolve("cache").resolve("store"), 64L * 1024 * 1024);
    }
}
