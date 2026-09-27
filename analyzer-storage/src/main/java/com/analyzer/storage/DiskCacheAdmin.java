package com.analyzer.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * The operator surface over the on-disk artifact cache: how big it is, and how to empty it.
 *
 * <p>The cache lives at {@code {app.data-dir}/cache/store/} — one shared root, with a subtree per
 * routed feature ({@code parser/}, {@code jar/}). Both {@link CachingArtifactStore} instances write
 * under the same root, so a single clear empties both. This class owns the root rather than either
 * store because "clear the cache" is a deployment-wide lever, not a per-feature one (see
 * {@code POST /api/admin/cache/clear} and {@code amp-ops.sh clear-cache}).
 *
 * <p><b>Nothing here is cached in heap.</b> Size and clear walk the filesystem directly; the only
 * memory used is proportional to the number of <em>files</em> on disk at the moment of the call,
 * never to the bytes they hold.
 */
public final class DiskCacheAdmin {

    private static final Logger log = LoggerFactory.getLogger(DiskCacheAdmin.class);

    private final Path cacheRoot;

    public DiskCacheAdmin(Path cacheRoot) {
        this.cacheRoot = cacheRoot.toAbsolutePath().normalize();
    }

    /** The {@code {app.data-dir}/cache/store/} root both feature caches share. */
    public Path cacheRoot() {
        return cacheRoot;
    }

    /** Bytes currently held under {@link #cacheRoot()}, or 0 when it does not exist. */
    public long sizeBytes() {
        return sizeOf(cacheRoot);
    }

    /**
     * Empties the cache directory (creating it if absent), returning the number of bytes freed.
     * Safe to call while the app is serving: an in-flight read that loses its file simply re-fetches
     * from the wrapped store, and a concurrent write re-populates the entry.
     */
    public long clear() {
        long before = sizeOf(cacheRoot);
        deleteContents(cacheRoot);
        try {
            Files.createDirectories(cacheRoot);
        } catch (IOException e) {
            log.warn("[cache] could not recreate cache root {}: {}", cacheRoot, e.getMessage());
        }
        log.info("[cache] cleared {} — freed {} bytes", cacheRoot, before);
        return before;
    }

    /** Sums the sizes of every regular file at or below {@code dir}. */
    static long sizeOf(Path dir) {
        if (!Files.isDirectory(dir)) return 0;
        try (Stream<Path> walk = Files.walk(dir)) {
            return walk.filter(Files::isRegularFile).mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (IOException e) {
                    return 0L;
                }
            }).sum();
        } catch (IOException e) {
            return 0L;
        }
    }

    /** Deletes everything inside {@code dir} (not the directory itself). */
    static void deleteContents(Path dir) {
        if (!Files.isDirectory(dir)) return;
        try (Stream<Path> children = Files.list(dir)) {
            children.forEach(DiskCacheAdmin::deleteRecursively);
        } catch (IOException e) {
            log.warn("[cache] could not list {}: {}", dir, e.getMessage());
        }
    }

    /** Deletes {@code path}, and if it is a directory everything below it. */
    static void deleteRecursively(Path path) {
        if (!Files.exists(path)) return;
        try (Stream<Path> walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    log.debug("[cache] could not delete {}: {}", p, e.getMessage());
                }
            });
        } catch (IOException e) {
            log.debug("[cache] could not walk {}: {}", path, e.getMessage());
        }
    }
}
