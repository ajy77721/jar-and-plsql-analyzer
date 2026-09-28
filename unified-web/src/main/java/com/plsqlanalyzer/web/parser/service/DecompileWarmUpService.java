package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.service.DecompilerService;
import com.jaranalyzer.service.InvocationPool;
import com.jaranalyzer.service.JarDataPaths;
import com.jaranalyzer.service.PersistenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Best-effort pre-decompilation of the JAR analyses' <em>top</em> classes, so the first click on a
 * class in the JAR screen does not pay CFR's cold cost. A cold {@code decompile} measured 8–21s
 * because every class compiles through CFR on first read; {@link DecompilerService} then serves the
 * class from its per-class 48h cache. This class fills that cache ahead of time.
 *
 * <h2>Scope and caps</h2>
 *
 * <p>For each JAR analysis it lists the already-cached {@code decompiled/} classes (one listing per
 * jar, so a cached class costs no per-class round trip), then decompiles the analysis's classes in
 * the order {@code analysis.json} presents them — the order {@code JarExportBundleExecutor} already
 * treats as "first N" — skipping the ones already cached. Two independent caps bound the work:
 * {@code app.sidecar.warmup.decompile-max-classes-per-jar} (default 50) and a per-jar wall-clock
 * budget ({@code app.sidecar.warmup.decompile-budget-seconds-per-jar}, default 60s). A large jar is
 * therefore warmed partially rather than blocking the sweep, and the next sweep continues.
 *
 * <h2>Never throws</h2>
 *
 * <p>It runs inside the sidecar sweep under the first logged-in user's captured credentials, so any
 * store failure — an unreachable listing, a missing jar, one class CFR cannot handle — is contained
 * to a counter and retried next sweep, exactly like the sidecar half. A deployment running in local
 * mode simply warms from the local disk.
 */
@Component
public class DecompileWarmUpService {

    private static final Logger log = LoggerFactory.getLogger(DecompileWarmUpService.class);

    private final PersistenceService jarPersistenceService;
    private final ArtifactStore store;
    private final JarDataPaths jarDataPaths;
    private final DecompilerService decompilerService;
    private final ObjectMapper objectMapper;

    /** Whether the decompile warm-up runs at all; the sidecar sweep is not gated by this. */
    private final boolean enabled;

    private final int maxClassesPerJar;
    private final long budgetMillisPerJar;

    public DecompileWarmUpService(@Qualifier("jarPersistenceService") PersistenceService jarPersistenceService,
                                  @Qualifier("jarArtifactStore") ArtifactStore store,
                                  JarDataPaths jarDataPaths,
                                  DecompilerService decompilerService,
                                  ObjectMapper objectMapper,
                                  @Value("${app.sidecar.warmup.decompile:true}") boolean enabled,
                                  @Value("${app.sidecar.warmup.decompile-max-classes-per-jar:50}")
                                  int maxClassesPerJar,
                                  @Value("${app.sidecar.warmup.decompile-budget-seconds-per-jar:60}")
                                  long budgetSecondsPerJar) {
        this.jarPersistenceService = jarPersistenceService;
        this.store = store;
        this.jarDataPaths = jarDataPaths;
        this.decompilerService = decompilerService;
        this.objectMapper = objectMapper;
        this.enabled = enabled;
        this.maxClassesPerJar = Math.max(0, maxClassesPerJar);
        this.budgetMillisPerJar = Math.max(0, budgetSecondsPerJar) * 1000L;
    }

    /**
     * Warms the top classes of every JAR analysis. Never throws; a store that cannot be read is
     * reported as zero warmed and retried next sweep.
     *
     * @return one count per outcome, for the sweep's single summary line
     */
    public WarmerResult warm() {
        if (!enabled) return new WarmerResult(0, 0, 0, 0);
        List<String> ids = listJarIds();
        int decompiled = 0, cached = 0, failed = 0;
        for (String id : ids) {
            try {
                WarmerResult r = warmJar(id);
                decompiled += r.decompiled();
                cached += r.alreadyCached();
                failed += r.failed();
            } catch (Throwable t) {
                // One jar must not fail the warm-up; retried on the next sweep.
                log.debug("Decompile warm-up failed for {}: {}", id, t.toString());
            }
        }
        return new WarmerResult(ids.size(), decompiled, cached, failed);
    }

    /** The ids of every JAR analysis, or an empty list when the store cannot be listed. */
    private List<String> listJarIds() {
        try {
            return jarPersistenceService.listJars().stream()
                    .map(m -> m == null ? null : (String) m.get("id"))
                    .filter(Objects::nonNull)
                    .toList();
        } catch (Exception e) {
            // One line, retried next sweep — never per jar.
            log.info("Decompile warm-up: could not list JARs ({}); will retry next sweep",
                    e.toString());
            return List.of();
        }
    }

    /** Warms one jar's top classes, honouring both caps. Never throws to its caller. */
    private WarmerResult warmJar(String id) {
        ArtifactKey jarKey = jarDataPaths.storedJarKey(id);
        if (!store.exists(jarKey)) return new WarmerResult(1, 0, 0, 0);

        Set<String> alreadyCached = cachedClassNames(id);
        List<String> names = listClassNames(id);
        if (maxClassesPerJar > 0 && names.size() > maxClassesPerJar) {
            names = names.subList(0, maxClassesPerJar);
        }
        long deadline = System.nanoTime() + budgetMillisPerJar * 1_000_000L;

        int decompiled = 0, cached = 0, failed = 0;
        for (String className : names) {
            if (budgetMillisPerJar > 0 && System.nanoTime() > deadline) {
                log.debug("Decompile warm-up budget ({} ms) reached for {}; continuing next sweep",
                        budgetMillisPerJar, id);
                break;
            }
            if (alreadyCached.contains(className)) {
                cached++;
                continue;
            }
            try {
                DecompilerService.DecompileResult result =
                        decompilerService.decompileWithMeta(jarKey, className);
                if (result != null) decompiled++;
                else failed++;
            } catch (RuntimeException e) {
                failed++;
                log.debug("Decompile warm-up failed for {} in {}: {}", className, id, e.getMessage());
            }
        }
        return new WarmerResult(1, decompiled, cached, failed);
    }

    /**
     * The class names already cached under {@code jar/<id>/decompiled/}, one listing for the whole
     * jar. Leaves empty on a listing failure (the per-class decompile path still checks the cache
     * itself, so a miss here only costs a redundant no-op, never a wrong result).
     */
    private Set<String> cachedClassNames(String id) {
        Set<String> out = new HashSet<>();
        ArtifactKey dir = jarDataPaths.jarRootKey(id).resolve("decompiled");
        try (Stream<ArtifactEntry> entries = store.list(dir, false)) {
            entries.filter(e -> !e.directory()).forEach(e -> {
                String leaf = e.key().leaf();
                if (leaf.endsWith(".java")) out.add(leaf.substring(0, leaf.length() - ".java".length()));
            });
        } catch (Exception e) {
            log.debug("Could not list decompiled classes for {}: {}", id, e.getMessage());
        }
        return out;
    }

    /**
     * Every class's {@code fullyQualifiedName}, streamed out of {@code analysis.json} /
     * {@code analysis_corrected.json} without inflating class bodies — the same streaming entry
     * point {@code DecompileController#extractClassData} and {@code JarExportBundleExecutor} use,
     * collecting every name instead of stopping at the first match.
     */
    private List<String> listClassNames(String id) {
        ArtifactKey key = jarDataPaths.correctedKey(id);
        if (!store.exists(key)) key = jarDataPaths.analysisKey(id);
        if (!store.exists(key)) return List.of();

        List<String> names = new ArrayList<>();
        try (InputStream in = store.read(key);
             JsonParser parser = objectMapper.getFactory().createParser(in)) {
            InvocationPool.Inflater.seekClasses(parser, objectMapper);
            if (parser.currentToken() == JsonToken.START_ARRAY) {
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    JsonNode node = objectMapper.readTree(parser);
                    if (node.has("fullyQualifiedName")) {
                        String fqn = node.get("fullyQualifiedName").asText();
                        if (fqn != null && !fqn.isBlank()) names.add(fqn);
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Could not read class names for {}: {}", id, e.getMessage());
        }
        return names;
    }

    /**
     * Counts from one warm pass.
     *
     * @param jars          JAR analyses considered
     * @param decompiled    classes freshly decompiled (now cached)
     * @param alreadyCached classes skipped because {@code decompiled/} already had them
     * @param failed        classes CFR could not decompile this pass (retried next sweep)
     */
    public record WarmerResult(int jars, int decompiled, int alreadyCached, int failed) {
        /** A no-op result, for a warm-up that is disabled. */
        public static final WarmerResult EMPTY = new WarmerResult(0, 0, 0, 0);

        /** Total classes the pass looked at (fresh + cached + failed). */
        public int touched() {
            return decompiled + alreadyCached + failed;
        }
    }
}
