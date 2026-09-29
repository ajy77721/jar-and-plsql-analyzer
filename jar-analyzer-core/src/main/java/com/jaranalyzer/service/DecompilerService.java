package com.jaranalyzer.service;

import org.benf.cfr.reader.api.CfrDriver;
import org.benf.cfr.reader.api.OutputSinkFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Decompiles .class bytecode to actual Java source using CFR.
 * Works from stored JAR files — extracts class bytes, writes temp, runs CFR, returns source.
 */
@Service
public class DecompilerService {

    private static final Logger log = LoggerFactory.getLogger(DecompilerService.class);
    private static final long DECOMPILE_TIMEOUT_SECONDS = 30;
    private static final Map<String, String> CFR_OPTIONS = Map.of(
            "showversion", "false", "comments", "false",
            "decodestringswitch", "true", "sugarenums", "true",
            "removeboilerplate", "true", "removeinnerclasssynthetics", "true");
    private static final ExecutorService decompileExecutor =
            Executors.newFixedThreadPool(
                    Math.min(4, Runtime.getRuntime().availableProcessors()), r -> {
                Thread t = new Thread(r, "cfr-decompile");
                t.setDaemon(true);
                return t;
            });

    private static final ThreadLocal<Path> threadTempDir = new ThreadLocal<>();

    private final JarDataPaths jarDataPaths;
    private final com.analyzer.storage.ArtifactStore store;

    /**
     * The temp root this app's own artifacts are materialized into, and the filename prefixes it
     * uses there. Used ONLY by the defense-in-depth branch of {@link #decompile(Path, String)}: a
     * regular file under here whose name begins with one of {@link #appTempPrefixes} was written by
     * this app from its OWN upload/materialize path, never a user-supplied arbitrary path, so it is
     * safe to decompile directly from. The guard is NOT opened to anything else.
     */
    private final Path appTempRoot;
    private final List<String> appTempPrefixes;

    /**
     * Prefixes of temp files this app creates in {@code java.io.tmpdir}: {@code artifact-} is
     * {@link com.analyzer.storage.ArtifactStore#materialize} (S3/Mongo download or in-memory
     * scratch), the rest are multipart/upload temps created with {@code Files.createTempFile}.
     */
    private static final List<String> DEFAULT_APP_TEMP_PREFIXES = List.of(
            "artifact-", "jar-analyze-", "jar-targeted-", "jar-reanalyze-",
            "war-analyze-", "war-reanalyze-", "zip-raw-", "cfr-decompile-");

    /**
     * Refused-outside-path WARNs are emitted once, then only every {@link #WARN_EVERY_N}th attempt,
     * so a single misrouted analysis cannot produce ~one identical line per class (the live-staging
     * regression this replaces).
     */
    private static final long WARN_EVERY_N = 100;
    private final AtomicLong outsideBaseWarnCount = new AtomicLong();

    /**
     * TTL for the per-class decompile cache, in hours. Re-derivable data (unlike PL/SQL source or
     * other permanent artifacts), so it is allowed to self-heal on its own after a jar re-upload
     * or a bad decompile, without anyone having to remember to invalidate it by hand.
     *
     * <p><b>Applies identically in every storage mode</b> (local / s3 / mongodb) — deliberately.
     * Rather than a MongoDB-specific TTL index (which only that one backend could honour, and
     * which local/S3 would then silently lack), expiry is checked here in application code using
     * {@link com.analyzer.storage.ArtifactStore#lastModified}, a method every {@code ArtifactStore}
     * implementation already provides. This keeps the shared {@code ArtifactStore} abstraction
     * completely untouched — no new write-time metadata, no new collection, no special-casing of
     * this one artifact "kind" anywhere below the service layer — while still giving TTL behaviour
     * that is honestly identical across all three backends, not "mongo-only, best-effort elsewhere".
     * The tradeoff: expiry is evaluated lazily, on the next read of that exact class, not by a
     * background sweep — an expired entry that is never requested again simply sits there
     * (harmless: it is bounded by nothing worse than "one stale file per ever-decompiled class").
     */
    private final long cacheTtlMillis;

    public DecompilerService(JarDataPaths jarDataPaths, com.analyzer.storage.ArtifactStore store) {
        this(jarDataPaths, store, java.time.Duration.ofHours(48));
    }

    @org.springframework.beans.factory.annotation.Autowired
    public DecompilerService(JarDataPaths jarDataPaths,
                              @org.springframework.beans.factory.annotation.Qualifier("jarArtifactStore") com.analyzer.storage.ArtifactStore store,
                              @Value("${app.jar.decompile-cache.ttl-hours:48}") long cacheTtlHours,
                              // Test-only escape hatch for live verification runs, where waiting out
                              // a real hour-granularity TTL is impractical: overrides ttl-hours with
                              // an exact millisecond duration when set. Not documented in
                              // application.properties on purpose — it exists solely so this
                              // feature's expiry behaviour can be proven against the real running
                              // app without permanently changing the production default.
                              @Value("${app.jar.decompile-cache.ttl-millis-override:-1}") long ttlMillisOverride,
                              // App-owned temp root/prefixes for the defense-in-depth guard below.
                              // Defaults to java.io.tmpdir, which is exactly where
                              // ArtifactStore.materialize (artifact-*) and the multipart upload
                              // temps (jar-analyze-*, ...) are created.
                              @Value("${app.upload.temp-dir:}") String appTempDir,
                              @Value("${app.upload.temp-prefixes:}") String appTempPrefixesCsv) {
        this(jarDataPaths, store, ttlMillisOverride >= 0
                ? java.time.Duration.ofMillis(ttlMillisOverride)
                : java.time.Duration.ofHours(cacheTtlHours),
                appTempDir, appTempPrefixesCsv);
    }

    /**
     * Test/advanced-configuration entry point: TTL as an exact {@link java.time.Duration} rather
     * than whole hours, so a unit test can exercise real expiry (e.g. {@code Duration.ofMillis(50)})
     * without waiting out the production default. Not wired by Spring — the hour-based constructor
     * above is what {@code app.jar.decompile-cache.ttl-hours} actually binds to.
     */
    public DecompilerService(JarDataPaths jarDataPaths, com.analyzer.storage.ArtifactStore store,
                              java.time.Duration cacheTtl) {
        this(jarDataPaths, store, cacheTtl, null, null);
    }

    /**
     * Full constructor. {@code appTempDir}/{@code appTempPrefixesCsv} are the configurable
     * app-owned temp root and comma-separated filename prefixes for the defense-in-depth guard;
     * blank/null means java.io.tmpdir and {@link #DEFAULT_APP_TEMP_PREFIXES}.
     */
    public DecompilerService(JarDataPaths jarDataPaths, com.analyzer.storage.ArtifactStore store,
                              java.time.Duration cacheTtl,
                              String appTempDir, String appTempPrefixesCsv) {
        this.jarDataPaths = jarDataPaths;
        this.store = store;
        this.cacheTtlMillis = cacheTtl == null ? 0 : cacheTtl.toMillis();
        this.appTempRoot = Path.of(appTempDir == null || appTempDir.isBlank()
                ? System.getProperty("java.io.tmpdir", "/tmp")
                : appTempDir).toAbsolutePath().normalize();
        if (appTempPrefixesCsv == null || appTempPrefixesCsv.isBlank()) {
            this.appTempPrefixes = DEFAULT_APP_TEMP_PREFIXES;
        } else {
            List<String> parsed = new ArrayList<>();
            for (String p : appTempPrefixesCsv.split(",")) {
                String trimmed = p.trim();
                if (!trimmed.isEmpty()) parsed.add(trimmed);
            }
            this.appTempPrefixes = parsed.isEmpty() ? DEFAULT_APP_TEMP_PREFIXES : parsed;
        }
    }

    private Path getThreadTempDir() throws IOException {
        Path dir = threadTempDir.get();
        if (dir == null || !Files.isDirectory(dir)) {
            dir = Files.createTempDirectory("cfr-decompile-");
            threadTempDir.set(dir);
        }
        return dir;
    }

    /**
     * Decompile a single class from a JAR file.
     * @param jarPath path to the stored JAR file
     * @param className fully qualified class name (e.g. "com.example.MyService")
     * @return decompiled Java source code, or null if class not found
     */
    public String decompile(Path jarPath, String className) {
        if (jarPath == null) return null;
        com.analyzer.storage.ArtifactKey key = jarDataPaths == null ? null : jarDataPaths.keyForPath(jarPath);
        if (key != null) return decompile(key, className);

        // Defense in depth. The PRIMARY path is key-based (decompile(ArtifactKey, ...)); a caller
        // that still hands over a raw Path normally passes the store-materialized file, which in
        // local mode lies under the data root and keys cleanly above. In S3/Mongo mode
        // store.materialize() instead downloads to a temp file outside the data root
        // (artifact-<n>-stored.jar) — which the containment check below would refuse, failing every
        // class. Such a temp is app-owned: THIS app created it from its own upload/materialize, its
        // name is one of our fixed prefixes, and it is a regular file under our temp root — never a
        // user-supplied arbitrary path. Decompile it directly (uncached, since a temp has no stable
        // store key). Anything else is still refused.
        if (isAppOwnedTemp(jarPath)) {
            log.debug("Decompiling {} from app-owned temp {} (no store key — uncached)", className, jarPath);
            return decompileLocalFile(null, jarPath, className);
        }

        // Rate-limit the refusal: one identical WARN per class is the live-staging regression this
        // replaces. First attempt warns; then only every WARN_EVERY_Nth, so the count still shows
        // up in the log without flooding it.
        long attempts = outsideBaseWarnCount.incrementAndGet();
        if (attempts == 1 || attempts % WARN_EVERY_N == 0) {
            log.warn("Cannot decompile from {} — it is outside {} ({} attempt(s) refused so far;"
                            + " further identical failures logged at debug)",
                    jarPath, jarDataPaths == null ? "the data root" : jarDataPaths.getBaseDir(), attempts);
        } else {
            log.debug("Cannot decompile from {} — it is outside {} (attempt #{})",
                    jarPath, jarDataPaths == null ? "the data root" : jarDataPaths.getBaseDir(), attempts);
        }
        return null;
    }

    /**
     * Resets the refused-path WARN rate limit. Called at the start of an analysis so the limit is
     * per-analysis rather than per-JVM-lifetime.
     */
    public void resetGuardWarnCounter() {
        outsideBaseWarnCount.set(0);
    }

    /** True when {@code p} is a regular app-owned temp file (fixed prefix under our temp root). */
    private boolean isAppOwnedTemp(Path p) {
        try {
            Path abs = p.toAbsolutePath().normalize();
            if (!abs.startsWith(appTempRoot)) return false;
            Path name = abs.getFileName();
            if (name == null) return false;
            String n = name.toString();
            for (String prefix : appTempPrefixes) {
                if (n.startsWith(prefix)) return true;
            }
            return false;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * Decompile a class straight from a local jar file.
     *
     * @param indexKey the nested-JAR index identity for {@code jarPath}; the artifact key when one
     *                 is known (so the index is shared with {@link #decompile(ArtifactKey, String)}),
     *                 or {@code null} for an app-owned temp with no store key
     */
    private String decompileLocalFile(String indexKey, Path jarPath, String className) {
        try {
            byte[] classBytes = extractClassBytes(
                    indexKey != null ? indexKey : "local-temp:" + jarPath.toAbsolutePath().normalize(),
                    jarPath, className);
            if (classBytes == null) return null;
            Path tempDir = getThreadTempDir();
            Future<String> future = decompileExecutor.submit(() -> decompileBytes(classBytes, className, tempDir));
            try {
                return future.get(DECOMPILE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
                log.warn("Decompile timed out after {}s for {}", DECOMPILE_TIMEOUT_SECONDS, className);
                return null;
            } catch (ExecutionException e) {
                log.warn("Decompile failed for {}: {}", className, e.getCause().getMessage());
                return null;
            }
        } catch (Throwable e) {
            log.warn("Failed to decompile {}: {}", className, e.getMessage());
            return null;
        }
    }

    /**
     * Decompile a class from a stored JAR artifact.
     *
     * <p>CFR needs a seekable local JAR, so this is one of the few legitimate uses of
     * {@link com.analyzer.storage.ArtifactStore#materialize}: locally it is a zero-copy handle on
     * the existing file, remotely a temp download that the handle deletes on close.
     */
    public String decompile(com.analyzer.storage.ArtifactKey jarKey, String className) {
        DecompileResult result = decompileWithMeta(jarKey, className);
        return result == null ? null : result.source();
    }

    /**
     * Decompile a class from an <b>already-resolved local jar file</b>, using {@code jarKey} ONLY as
     * the decompile-cache identity.
     *
     * <p>{@link #decompile(com.analyzer.storage.ArtifactKey, String)} re-runs
     * {@code store.exists()} + {@code store.materialize()} for <em>every class</em> — remotely a
     * fresh download and {@link JarFile} open per class, which is the bulk of a large analysis's
     * decompile cost. A caller that holds the artifact's local file for the whole job (the queue
     * executors materialize it once; {@link #pinJar} does the same for a caller that does not)
     * decompiles every class from that one open file, while the key still gives each class its
     * stable {@code decompiled/<class>.java} cache entry — so the cache read/write still routes
     * through the store (and therefore still needs the calling thread's SSO context).
     *
     * <p>Null-safe: a {@code null} jar file or a key that carries no cache identity falls back to
     * the uncached app-owned-temp behaviour rather than failing. Never throws.
     */
    public String decompile(Path localJar, com.analyzer.storage.ArtifactKey jarKey, String className) {
        if (localJar == null) return null;
        if (jarKey == null || store == null || jarDataPaths == null) {
            return decompileLocalFile(jarKey == null ? null : jarKey.toString(), localJar, className);
        }

        com.analyzer.storage.ArtifactKey cacheKey = decompiledCacheKey(jarKey, className);
        if (cacheKey != null) {
            try {
                if (store.exists(cacheKey)) {
                    long age = System.currentTimeMillis() - store.lastModified(cacheKey);
                    if (cacheTtlMillis > 0 && age > cacheTtlMillis) {
                        log.debug("Decompile cache entry for {} expired ({} ms old, ttl {} ms) — evicting and re-decompiling",
                                className, age, cacheTtlMillis);
                        store.delete(cacheKey);
                    } else {
                        try (InputStream in = store.read(cacheKey)) {
                            log.debug("Decompile cache hit for {} ({})", className, cacheKey);
                            return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                        }
                    }
                }
            } catch (IOException e) {
                log.debug("Cache read failed for {}, decompiling from the local jar: {}", cacheKey, e.getMessage());
            }
        }

        String source = decompileLocalFile(jarKey.toString(), localJar, className);
        if (source != null && cacheKey != null) {
            byte[] bytes = source.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            try (OutputStream out = store.write(cacheKey, bytes.length)) {
                out.write(bytes);
            } catch (IOException e) {
                log.debug("Failed to cache decompiled source for {}: {}", cacheKey, e.getMessage());
            }
        }
        return source;
    }

    /**
     * A local jar file held open for the duration of one enrich job so every class decompiles from
     * a single open file (one download in remote mode, not one per class). {@link #close()} releases
     * the temp copy when the artifact came from a remote store.
     */
    public static final class PinnedJar implements AutoCloseable {
        private final com.analyzer.storage.LocalHandle handle;
        private final Path path;

        private PinnedJar(com.analyzer.storage.LocalHandle handle, Path path) {
            this.handle = handle;
            this.path = path;
        }

        /** The open local jar file. Valid until {@link #close()}. */
        public Path path() {
            return path;
        }

        @Override
        public void close() {
            try { handle.close(); } catch (Exception ignored) { /* best effort */ }
        }
    }

    /**
     * Materializes {@code jarKey} to a local file ONCE, for reuse across every class of one enrich
     * job, or returns {@code null} when there is no such artifact (or no store) — the caller then
     * keeps its existing per-class behaviour. Must be called on a context-bearing thread (the queue
     * worker), since it reads through the routing store.
     */
    public PinnedJar pinJar(com.analyzer.storage.ArtifactKey jarKey) {
        if (jarKey == null || store == null) return null;
        try {
            if (!store.exists(jarKey)) return null;
            com.analyzer.storage.LocalHandle handle = store.materialize(jarKey);
            return new PinnedJar(handle, handle.path());
        } catch (Throwable e) {
            log.debug("Could not materialize {} for local reuse ({}); falling back to per-class reads",
                    jarKey, e.getMessage());
            return null;
        }
    }

    /** Whether a decompiled-class response came from the per-class cache or a live CFR run. */
    public enum DecompileSource { CACHE, JAR }

    /** {@code source} is the decompiled Java text; {@code origin} says how it was obtained. */
    public record DecompileResult(String source, DecompileSource origin) {}

    /**
     * As {@link #decompile(com.analyzer.storage.ArtifactKey, String)}, but also reports whether the
     * result came from the per-class cache or a fresh decompile — so a caller (the HTTP layer) can
     * tell the user which one happened, per this feature's whole point.
     */
    public DecompileResult decompileWithMeta(com.analyzer.storage.ArtifactKey jarKey, String className) {
        if (jarKey == null || !store.exists(jarKey)) return null;

        // Cache hit: this exact class was decompiled before for this jar. One artifact per class
        // (not a whole-jar blob), so a cache hit never requires materializing/opening the jar at
        // all — the entire point of caching, since materialize() + JarFile scanning is the
        // expensive part CFR itself is comparatively cheap next to.
        com.analyzer.storage.ArtifactKey cacheKey = decompiledCacheKey(jarKey, className);
        if (cacheKey != null && store.exists(cacheKey)) {
            long age = System.currentTimeMillis() - store.lastModified(cacheKey);
            if (cacheTtlMillis > 0 && age > cacheTtlMillis) {
                log.debug("Decompile cache entry for {} expired ({} ms old, ttl {} ms) — evicting and re-decompiling",
                        className, age, cacheTtlMillis);
                store.delete(cacheKey);
            } else {
                try (InputStream in = store.read(cacheKey)) {
                    String cached = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                    log.debug("Decompile cache hit for {} ({})", className, cacheKey);
                    return new DecompileResult(cached, DecompileSource.CACHE);
                } catch (IOException e) {
                    log.debug("Cache read failed for {}, falling back to decompile: {}", cacheKey, e.getMessage());
                }
            }
        }

        String source;
        try (com.analyzer.storage.LocalHandle handle = store.materialize(jarKey)) {
            Path jarPath = handle.path();
            byte[] classBytes = extractClassBytes(jarKey, jarPath, className);
            if (classBytes == null) {
                log.debug("Class {} not found in JAR {}", className, jarKey);
                return null;
            }

            Path tempDir = getThreadTempDir();
            Future<String> future = decompileExecutor.submit(() -> decompileBytes(classBytes, className, tempDir));
            try {
                source = future.get(DECOMPILE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
                log.warn("Decompile timed out after {}s for {}", DECOMPILE_TIMEOUT_SECONDS, className);
                return null;
            } catch (ExecutionException e) {
                log.warn("Decompile failed for {}: {}", className, e.getCause().getMessage());
                return null;
            }
        } catch (Throwable e) {
            log.warn("Failed to decompile {}: {}", className, e.getMessage());
            return null;
        }

        // Write only the ONE class actually requested — never a batch of every class in the jar.
        if (source != null && cacheKey != null) {
            try (OutputStream out = store.write(cacheKey, source.getBytes(java.nio.charset.StandardCharsets.UTF_8).length)) {
                out.write(source.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            } catch (IOException e) {
                log.debug("Failed to cache decompiled source for {}: {}", cacheKey, e.getMessage());
            }
        }
        return source == null ? null : new DecompileResult(source, DecompileSource.JAR);
    }

    /**
     * Derives the per-class cache key ({@link JarDataPaths#decompiledClassKey}) from the jar's own
     * artifact key, without needing the caller to separately pass the jar id: {@code jarKey} is
     * always {@code <jarRootKey>/stored.jar} (see {@link JarDataPaths#storedJarKey}), so its parent
     * IS the jar's root key and its leaf segment is the (normalized) jar id. Returns null for a jar
     * key that does not follow that shape, or when this instance has no {@link JarDataPaths}
     * (defensive; keeps caching best-effort rather than breaking decompilation for an odd caller).
     */
    private com.analyzer.storage.ArtifactKey decompiledCacheKey(com.analyzer.storage.ArtifactKey jarKey, String className) {
        if (jarDataPaths == null) return null;
        com.analyzer.storage.ArtifactKey jarRoot = jarKey.parent();
        if (jarRoot == null) return null;
        try {
            return jarDataPaths.decompiledClassKey(jarRoot.leaf(), className);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Extract a specific method's source code from a decompiled class.
     * Decompiles the class with CFR, then finds the method by name and extracts its body.
     * Uses brace-matching to handle nested blocks correctly.
     *
     * @param fullSource the full decompiled class source
     * @param methodName the method to extract
     * @return the method's source code (signature + body), or null if not found
     */
    /**
     * Extract a method from decompiled source by name.
     * For overloaded methods, pass parameterTypes to match the right overload.
     */
    public String extractMethod(String fullSource, String methodName) {
        return extractMethod(fullSource, methodName, null);
    }

    /**
     * Extract a specific overloaded method by name + parameter types.
     * Handles: overloading (same name, different params), covariant return,
     * bridge methods, method hiding — all by matching the parameter signature.
     *
     * @param parameterTypes if null or empty, returns the first match (backward compatible).
     *                       If provided, matches the overload whose params contain these type names.
     */
    public String extractMethod(String fullSource, String methodName, List<String> parameterTypes) {
        if (fullSource == null || methodName == null) return null;

        String needle = methodName + "(";
        List<Integer> candidateOffsets = new ArrayList<>();

        // Phase 1: Find all declaration positions using indexOf (avoids split)
        int searchFrom = 0;
        while (searchFrom < fullSource.length()) {
            int pos = fullSource.indexOf(needle, searchFrom);
            if (pos < 0) break;
            searchFrom = pos + needle.length();

            // Find line start for this position
            int lineStart = fullSource.lastIndexOf('\n', pos);
            lineStart = lineStart < 0 ? 0 : lineStart + 1;
            String beforeOnLine = fullSource.substring(lineStart, pos).trim();

            if (beforeOnLine.startsWith("//") || beforeOnLine.startsWith("*") || beforeOnLine.startsWith("/*")) continue;
            // Check if this is a method call (char before method name is a dot) vs a declaration
            if (pos > 0 && fullSource.charAt(pos - 1) == '.') continue;
            String beforeMethod = beforeOnLine;
            if (beforeMethod.isEmpty()
                    || beforeMethod.endsWith("=") || beforeMethod.endsWith("(")) continue;
            candidateOffsets.add(lineStart);
        }

        if (candidateOffsets.isEmpty()) return null;

        // Phase 2: Pick best candidate based on parameter matching
        int bestOffset;
        if (candidateOffsets.size() == 1 || parameterTypes == null || parameterTypes.isEmpty()) {
            bestOffset = candidateOffsets.get(0);
        } else {
            int bestIdx = 0;
            int bestScore = -1;
            for (int c = 0; c < candidateOffsets.size(); c++) {
                int offset = candidateOffsets.get(c);
                int parenStart = fullSource.indexOf(needle, offset) + needle.length();
                int bracePos = fullSource.indexOf('{', parenStart);
                if (bracePos < 0) bracePos = Math.min(offset + 500, fullSource.length());
                int closeParen = fullSource.indexOf(')', parenStart);
                if (closeParen < 0 || closeParen > bracePos) closeParen = bracePos;
                String paramSection = fullSource.substring(parenStart, closeParen);

                int score = 0;
                for (String pt : parameterTypes) {
                    String shortType = pt.contains(".") ? pt.substring(pt.lastIndexOf('.') + 1) : pt;
                    if (paramSection.contains(shortType)) score++;
                }
                long sigParamCount = paramSection.isBlank() ? 0 :
                        paramSection.chars().filter(ch -> ch == ',').count() + 1;
                if (sigParamCount == parameterTypes.size()) score += 2;

                if (score > bestScore) {
                    bestScore = score;
                    bestIdx = c;
                }
            }
            bestOffset = candidateOffsets.get(bestIdx);
        }

        // Phase 3: Brace-match from bestOffset to find end of method
        int braceDepth = 0;
        boolean foundOpen = false;
        int end = bestOffset;
        for (int i = bestOffset; i < fullSource.length(); i++) {
            char ch = fullSource.charAt(i);
            if (ch == '{') { braceDepth++; foundOpen = true; }
            else if (ch == '}') {
                braceDepth--;
                if (foundOpen && braceDepth == 0) { end = i + 1; break; }
            }
        }
        if (!foundOpen) return null;

        return fullSource.substring(bestOffset, end);
    }

    /**
     * Decompile a class and extract a specific method in one call.
     * Caches nothing — caller should cache the full class source if extracting multiple methods.
     */
    public String decompileMethod(Path jarPath, String className, String methodName) {
        String fullSource = decompile(jarPath, className);
        if (fullSource == null) return null;
        return extractMethod(fullSource, methodName);
    }

    /**
     * Batch decompile: decompile a class once, extract multiple methods.
     * Returns a map of methodName → source code.
     * Much more efficient than calling decompileMethod() per method.
     */
    public Map<String, String> decompileMethods(Path jarPath, String className, List<String> methodNames) {
        Map<String, String> results = new LinkedHashMap<>();
        String fullSource = decompile(jarPath, className);
        if (fullSource == null) return results;

        for (String methodName : methodNames) {
            String methodSource = extractMethod(fullSource, methodName);
            if (methodSource != null) {
                results.put(methodName, methodSource);
            }
        }
        return results;
    }

    /**
     * Per-JAR index of {@code package prefix -> BOOT-INF/lib/<nested>.jar}, so a class that lives
     * in a bundled library can be found without rescanning all 200+ nested JARs every time.
     *
     * <p><b>Keyed by JAR.</b> It used to be one service-wide map guarded by a single
     * {@code nestedJarIndexBuilt} boolean, but the map is populated from exactly ONE artifact — so
     * the first JAR analysed after startup owned the index forever, and every later JAR resolved
     * its nested-library packages against that first JAR's layout. Two Spring Boot apps that both
     * bundle, say, {@code com.fasterxml.jackson.databind} then decompiled the WRONG artifact's copy
     * (silently: the lookup succeeds, it just reads a different jar's bytes), and any package the
     * first JAR did not contain was cached as "absent" for all of them. Each JAR now gets its own
     * index.
     */
    private final Map<String, Map<String, String>> nestedJarIndexes = new LinkedHashMap<>();

    /** Bound on {@link #nestedJarIndexes}; an index is cheap to rebuild, unbounded memory is not. */
    private static final int MAX_INDEXED_JARS = 8;

    /**
     * @param jar the already-open JarFile for {@code jarKey} — reusing it avoids opening the same
     *            artifact a second time just to index it.
     */
    private Map<String, String> nestedJarIndex(com.analyzer.storage.ArtifactKey jarKey, JarFile jar) {
        return nestedJarIndex(jarKey.toString(), jar);
    }

    private Map<String, String> nestedJarIndex(String id, JarFile jar) {
        synchronized (nestedJarIndexes) {
            Map<String, String> cached = nestedJarIndexes.get(id);
            if (cached != null) {
                nestedJarIndexes.remove(id);          // LRU: re-insert at the tail
                nestedJarIndexes.put(id, cached);
                return cached;
            }
        }

        Map<String, String> index = buildNestedJarIndex(jar);

        synchronized (nestedJarIndexes) {
            nestedJarIndexes.put(id, index);
            while (nestedJarIndexes.size() > MAX_INDEXED_JARS) {
                Iterator<String> it = nestedJarIndexes.keySet().iterator();
                it.next();
                it.remove();
            }
        }
        log.info("Nested JAR index for {}: {} packages mapped", id, index.size());
        return index;
    }

    private Map<String, String> buildNestedJarIndex(JarFile jar) {
        Map<String, String> packageToNestedJar = new HashMap<>();
        Enumeration<JarEntry> entries = jar.entries();
        while (entries.hasMoreElements()) {
            JarEntry libEntry = entries.nextElement();
            String name = libEntry.getName();
            // WEB-INF/lib/ is the WAR counterpart of BOOT-INF/lib/ - same reasoning as the
            // WEB-INF/classes/ addition above.
            boolean isNestedLib = (name.startsWith("BOOT-INF/lib/") || name.startsWith("WEB-INF/lib/"))
                    && name.endsWith(".jar");
            if (!isNestedLib) continue;
            try (InputStream libIs = jar.getInputStream(libEntry);
                 ZipInputStream zis = new ZipInputStream(libIs)) {
                ZipEntry ze;
                Set<String> seen = new HashSet<>();
                while ((ze = zis.getNextEntry()) != null) {
                    String zeName = ze.getName();
                    if (!zeName.endsWith(".class")) continue;
                    int lastSlash = zeName.lastIndexOf('/');
                    if (lastSlash <= 0) continue;
                    String pkg = zeName.substring(0, lastSlash);
                    if (seen.add(pkg)) {
                        packageToNestedJar.putIfAbsent(pkg, name);
                    }
                }
            } catch (Exception e) {
                // skip corrupt nested JARs
            }
        }
        return packageToNestedJar;
    }

    /**
     * Extract raw .class bytes from a JAR (handles both standard and Spring Boot fat JARs).
     *
     * @param jarKey identity of the artifact {@code jarPath} was materialized from — the nested-JAR
     *               index is cached under it, so two JARs never share one index
     */
    byte[] extractClassBytes(com.analyzer.storage.ArtifactKey jarKey, Path jarPath, String className)
            throws IOException {
        return extractClassBytes(jarKey.toString(), jarPath, className);
    }

    /**
     * As {@link #extractClassBytes(com.analyzer.storage.ArtifactKey, Path, String)} but keyed by an
     * arbitrary string id — used for app-owned temp files that have no store key, so the nested-JAR
     * index is still scoped per file rather than shared service-wide.
     */
    byte[] extractClassBytes(String indexKey, Path jarPath, String className)
            throws IOException {
        String classFilePath = className.replace('.', '/') + ".class";
        String bootInfPath = "BOOT-INF/classes/" + classFilePath;
        // WAR layout (Servlet spec): application classes live under WEB-INF/classes/, the WAR
        // counterpart of a Spring Boot fat JAR's BOOT-INF/classes/ - same reason this needs its
        // own prefix check rather than falling through to the nested-JAR index below.
        String webInfPath = "WEB-INF/classes/" + classFilePath;

        int lastSlash = classFilePath.lastIndexOf('/');
        String packagePath = lastSlash > 0 ? classFilePath.substring(0, lastSlash) : "";

        try (JarFile jar = new JarFile(jarPath.toFile())) {
            JarEntry entry = jar.getJarEntry(classFilePath);
            if (entry != null) {
                try (InputStream is = jar.getInputStream(entry)) {
                    return is.readAllBytes();
                }
            }

            entry = jar.getJarEntry(bootInfPath);
            if (entry != null) {
                try (InputStream is = jar.getInputStream(entry)) {
                    return is.readAllBytes();
                }
            }

            entry = jar.getJarEntry(webInfPath);
            if (entry != null) {
                try (InputStream is = jar.getInputStream(entry)) {
                    return is.readAllBytes();
                }
            }

            // Build this JAR's package→nested JAR index on first miss
            String cachedJar = nestedJarIndex(indexKey, jar).get(packagePath);
            if (cachedJar != null) {
                entry = jar.getJarEntry(cachedJar);
                if (entry != null) {
                    try (InputStream libIs = jar.getInputStream(entry);
                         ZipInputStream zis = new ZipInputStream(libIs)) {
                        ZipEntry ze;
                        while ((ze = zis.getNextEntry()) != null) {
                            if (ze.getName().equals(classFilePath)) {
                                return zis.readAllBytes();
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * Decompile class bytes using CFR. Temp dir is managed by caller.
     */
    private String decompileBytes(byte[] classBytes, String className, Path tempDir) throws IOException {
        String classFilePath = className.replace('.', '/') + ".class";
        Path classFile = tempDir.resolve(classFilePath);
        Files.createDirectories(classFile.getParent());
        Files.write(classFile, classBytes);
        try {
            return doDecompile(classFile, className);
        } finally {
            try { Files.deleteIfExists(classFile); } catch (Exception ignored) {}
        }
    }

    private String doDecompile(Path classFile, String className) {

        List<String> decompiled = new ArrayList<>();

        OutputSinkFactory sinkFactory = new OutputSinkFactory() {
            @Override
            public List<SinkClass> getSupportedSinks(SinkType sinkType, Collection<SinkClass> available) {
                return List.of(SinkClass.STRING);
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> Sink<T> getSink(SinkType sinkType, SinkClass sinkClass) {
                return (T value) -> {
                    if (sinkType == SinkType.JAVA) {
                        decompiled.add(value.toString());
                    }
                };
            }
        };

        CfrDriver driver = new CfrDriver.Builder()
                .withOptions(CFR_OPTIONS)
                .withOutputSink(sinkFactory)
                .build();

        try {
            driver.analyse(List.of(classFile.toString()));
        } catch (Throwable t) {
            log.debug("CFR failed for {}: {}", className, t.getMessage());
        }

        if (!decompiled.isEmpty()) {
            return String.join("\n", decompiled);
        }
        return null;
    }

    private void cleanTempDir(Path tempDir) {
        if (tempDir == null) return;
        try {
            try (var stream = Files.walk(tempDir)) {
                stream.sorted(Comparator.reverseOrder())
                        .forEach(p -> {
                            try { Files.deleteIfExists(p); }
                            catch (Exception ex) { /* best effort */ }
                        });
            }
        } catch (Exception e) {
            log.debug("Temp cleanup failed: {}", tempDir);
        }
    }
}
