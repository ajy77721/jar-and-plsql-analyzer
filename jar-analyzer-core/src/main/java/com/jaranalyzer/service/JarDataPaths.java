package com.jaranalyzer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

@Component
public class JarDataPaths {

    private static final Logger log = LoggerFactory.getLogger(JarDataPaths.class);
    private final Path baseDir;

    /**
     * The store every artifact under this base directory is read and written through.
     *
     * <p>It lives here, next to the keys, because a key without a store is only half an address:
     * every collaborator that is handed a {@code JarDataPaths} (providers, writers, caches) needs
     * both, and threading a second constructor parameter through each of them would have changed
     * signatures that unified-web calls. Null only in unit tests that exercise key/path algebra
     * and never touch storage — {@link #store()} fails loudly rather than silently falling back
     * to the filesystem.
     */
    private final ArtifactStore store;

    @org.springframework.beans.factory.annotation.Autowired
    public JarDataPaths(@Value("${app.data-dir:data}") String dataBase,
                        @Qualifier("jarArtifactStore") ArtifactStore store) {
        this.baseDir = Path.of(dataBase);
        this.store = store;
        assertKeyRootMatchesBaseDir();
    }

    /** Storage-less variant, for tests that only assert on keys and paths. */
    public JarDataPaths(String dataBase) {
        this(dataBase, null);
    }

    /** @throws IllegalStateException when this instance was built without a store. */
    public ArtifactStore store() {
        if (store == null) {
            throw new IllegalStateException(getClass().getSimpleName()
                    + " was constructed without an ArtifactStore; storage access is unavailable.");
        }
        return store;
    }

    /**
     * The Path half and the key half must address the same directory.
     *
     * <p>{@code baseDir} comes from a property; {@code keyRoot()} is fixed in code. Nothing forces
     * them to agree, and when they disagree the divergence is invisible in local mode — the Path
     * side stays correct, so reads and writes keep working right up until a store-backed write
     * puts the data somewhere else. That is exactly how WAR analyses ended up in the JAR
     * namespace. Fail at startup instead: a boot error is cheap, silently split data is not.
     */
    private void assertKeyRootMatchesBaseDir() {
        String[] root = keyRoot();
        Path tail = baseDir.normalize();
        int n = tail.getNameCount();
        if (n >= root.length) {
            boolean matches = true;
            for (int i = 0; i < root.length; i++) {
                if (!tail.getName(n - root.length + i).toString().equals(root[i])) {
                    matches = false;
                    break;
                }
            }
            if (matches) return;
        }
        throw new IllegalStateException(
                getClass().getSimpleName() + ": app.data-dir is '" + baseDir + "' but keys are "
                + "written under '" + String.join("/", root) + "/'. These must name the same "
                + "directory or the Path accessors and the ArtifactStore will address different "
                + "locations. See DirPropertyResolver, which is what normally guarantees this.");
    }

    public Path getBaseDir() {
        return baseDir;
    }

    // ── Storage keys ────────────────────────────────────────────────────────
    //
    // Parallel to the Path accessors above, and the direction of travel: a key is
    // storage-neutral, so the same call resolves to a local file or an S3 object depending only
    // on which ArtifactStore is wired.
    //
    // Both forms exist deliberately during the migration. Converting all ~100 Path callers in one
    // change would be a single un-reviewable commit; instead each writer moves to keys when it is
    // migrated, and the Path accessors are deleted once the last caller is gone.
    //
    // NOTE ON ROOT: keys are relative to the DATA ROOT, while `baseDir` here is already
    // {root}/jar (DirPropertyResolver rewrites app.data-dir to the jar subtree). So keys carry the
    // explicit "jar" segment that Paths get implicitly from baseDir — without it every key would
    // land at {root}/jar/jar/... See StorageConfig, which roots the store at app.data-dir.base.

    private static final String JAR = "jar";

    /**
     * The key prefix this instance's artifacts live under, mirroring what {@code baseDir}
     * contributes on the Path side.
     *
     * <p>Overridable because {@link WarDataPaths} puts its baseDir at {@code {root}/jar/war}. If
     * the key root stayed fixed at {@code jar}, WAR artifacts would be keyed into the JAR
     * namespace — orphaning the existing {@code jar/war/} tree and letting a WAR collide with a
     * JAR of the same id.
     */
    protected String[] keyRoot() {
        return new String[]{JAR};
    }

    /** {@code <keyRoot>/<normalizedJarName>} */
    public ArtifactKey jarRootKey(String jarName) {
        String[] root = keyRoot();
        String[] segs = java.util.Arrays.copyOf(root, root.length + 1);
        segs[root.length] = JarNameUtil.normalizeKey(jarName);
        return ArtifactKey.of(segs);
    }

    public ArtifactKey analysisKey(String jarName)       { return jarRootKey(jarName).resolve("analysis.json"); }
    public ArtifactKey correctedKey(String jarName)      { return jarRootKey(jarName).resolve("analysis_corrected.json"); }
    public ArtifactKey correctedPrevKey(String jarName)  { return jarRootKey(jarName).resolve("analysis_corrected_prev.json"); }
    public ArtifactKey summaryCacheKey(String jarName)   { return jarRootKey(jarName).resolve("_summary.json"); }
    public ArtifactKey summaryCacheStaticKey(String jarName) { return jarRootKey(jarName).resolve("_summary_static.json"); }
    public ArtifactKey headerKey(String jarName)         { return jarRootKey(jarName).resolve("_header.json"); }

    /**
     * {@code jar/<id>/views} — precomputed read-side view summaries (the summary header and the
     * collections aggregate). One folder, deleted whole on re-analysis/correction so no stale view
     * can outlive the document it summarises; see {@code PersistenceService#invalidateSummaryCache}.
     */
    public ArtifactKey viewsDirKey(String jarName)       { return jarRootKey(jarName).resolve("views"); }
    /** {@code jar/<id>/views/summary-header.json} — the {@code /summary/header} payload. */
    public ArtifactKey summaryHeaderViewKey(String jarName) { return viewsDirKey(jarName).resolve("summary-header.json"); }
    /** {@code jar/<id>/views/collections-count.json} — the {@code /collections} aggregate. */
    public ArtifactKey collectionsViewKey(String jarName) { return viewsDirKey(jarName).resolve("collections-count.json"); }
    public ArtifactKey storedJarKey(String jarName)      { return jarRootKey(jarName).resolve("stored.jar"); }
    public ArtifactKey domainConfigKey(String jarName)   { return jarRootKey(jarName).resolve("domain-config.json"); }
    public ArtifactKey connectionsKey(String jarName)    { return jarRootKey(jarName).resolve("connections.json"); }
    public ArtifactKey bundledJarsKey(String jarName)    { return jarRootKey(jarName).resolve("bundled-jars.json"); }
    public ArtifactKey jarDepMapKey(String jarName)      { return jarRootKey(jarName).resolve("jar-dep-map.json"); }
    public ArtifactKey claudeDirKey(String jarName)      { return jarRootKey(jarName).resolve("claude"); }
    public ArtifactKey correctionsDirKey(String jarName) { return jarRootKey(jarName).resolve("corrections"); }
    public ArtifactKey endpointsDirKey(String jarName)   { return jarRootKey(jarName).resolve("endpoints"); }
    public ArtifactKey improvementDirKey(String jarName) { return jarRootKey(jarName).resolve("improvement"); }
    public ArtifactKey resourcesDirKey(String jarName)   { return jarRootKey(jarName).resolve("resources"); }
    public ArtifactKey chatHistoryKey(String jarName)    { return jarRootKey(jarName).resolve("chat").resolve("history.jsonl"); }
    public ArtifactKey externalLinksKey(String jarName)  { return jarRootKey(jarName).resolve("external-links.json"); }
    public ArtifactKey externalLinksPendingKey(String jarName) { return jarRootKey(jarName).resolve("external-links-pending.json"); }
    public ArtifactKey mongoCatalogKey(String jarName)   { return jarRootKey(jarName).resolve("mongo-catalog.json"); }

    /**
     * {@code jar/<id>/decompiled/<className>.java} — one artifact per decompiled class, not one
     * blob for the whole jar. Written once on first decompile, read back on every later request
     * for the same class so the jar is never re-decompiled just to view a class again. Mirrors how
     * PL/SQL source is stored: one {@link ArtifactKey} per file, mode-aware through this same
     * {@link ArtifactStore}.
     *
     * @param className fully qualified class name, already validated by the caller against
     *                  {@code [a-zA-Z0-9._$]+} — safe as a single key segment.
     */
    public ArtifactKey decompiledClassKey(String jarName, String className) {
        return jarRootKey(jarName).resolve("decompiled").resolve(className + ".java");
    }

    /** Same blank-version rule as {@link #classTreeCache(String, String)}. */
    public ArtifactKey classTreeCacheKey(String jarName, String version) {
        return (version == null || version.isBlank())
                ? jarRootKey(jarName).resolve("_class-tree.json")
                : jarRootKey(jarName).resolve("_class-tree_" + version.trim() + ".json");
    }

    /**
     * Maps an absolute local path under this base directory to its storage key.
     *
     * <p>The migration lever: callers that already compute a {@code Path} (and there are hundreds)
     * can be routed through the store without changing their signatures — the path they were
     * about to write to becomes the key they write to instead. Same location in local mode, an
     * object key in s3 mode.
     *
     * @return the key, or null when the path lies outside this base directory (in which case the
     *         caller should fall back to the filesystem rather than guess)
     */
    public ArtifactKey keyForPath(Path absolute) {
        try {
            Path base = baseDir.toAbsolutePath().normalize();
            Path abs = absolute.toAbsolutePath().normalize();
            if (!abs.startsWith(base)) return null;
            Path rel = base.relativize(abs);
            ArtifactKey key = ArtifactKey.of(keyRoot());
            // Relativizing a path against ITSELF yields Path.of(""), which reports getNameCount()==1
            // with a single EMPTY name — so iterating it hands ArtifactKey a blank segment, which it
            // rejects, and the catch below turns that into a null the caller then dereferences.
            // Asking for the key of the base directory is legitimate (it is the subtree root), so
            // test the string, not the count.
            if (rel.toString().isEmpty()) return key;
            for (Path seg : rel) key = key.resolve(seg.toString());
            return key;
        } catch (RuntimeException e) {
            return null;   // unmappable -> caller uses the filesystem
        }
    }

    /** {@code jar/<id>/endpoints/<folder>/<file>} */
    public ArtifactKey endpointFileKey(String jarName, String endpointFolder, String fileName) {
        return endpointsDirKey(jarName).resolve(endpointFolder).resolve(fileName);
    }

    public Path jarRoot(String jarName) {
        return baseDir.resolve(JarNameUtil.normalizeKey(jarName));
    }

    public Path analysisFile(String jarName) {
        return jarRoot(jarName).resolve("analysis.json");
    }

    public Path correctedFile(String jarName) {
        return jarRoot(jarName).resolve("analysis_corrected.json");
    }

    public Path correctedPrevFile(String jarName) {
        return jarRoot(jarName).resolve("analysis_corrected_prev.json");
    }

    public Path summaryCache(String jarName) {
        return jarRoot(jarName).resolve("_summary.json");
    }

    public Path summaryCacheStatic(String jarName) {
        return jarRoot(jarName).resolve("_summary_static.json");
    }

    public Path storedJarFile(String jarName) {
        return jarRoot(jarName).resolve("stored.jar");
    }

    public Path mongoCatalogFile(String jarName) {
        return jarRoot(jarName).resolve("mongo-catalog.json");
    }

    /** Per-JAR domain config override file, stored alongside analysis data. */
    public Path domainConfigFile(String jarName) {
        return jarRoot(jarName).resolve("domain-config.json");
    }

    /** Connection info discovered during analysis (MongoDB URI, Oracle JDBC URL). */
    public Path connectionsFile(String jarName) {
        return jarRoot(jarName).resolve("connections.json");
    }

    public Path claudeDir(String jarName) {
        return jarRoot(jarName).resolve("claude");
    }

    public Path correctionsDir(String jarName) {
        return jarRoot(jarName).resolve("corrections");
    }

    public Path endpointsDir(String jarName) {
        return jarRoot(jarName).resolve("endpoints");
    }

    public Path improvementDir(String jarName) {
        return jarRoot(jarName).resolve("improvement");
    }

    public Path chatHistoryFile(String jarName) {
        return jarRoot(jarName).resolve("chat").resolve("history.jsonl");
    }

    public Path resourcesDir(String jarName) {
        return jarRoot(jarName).resolve("resources");
    }

    /** Pre-computed class-tree cache — written once after analysis, served directly to avoid re-streaming the multi-GB analysis.json on every code-structure tab open. */
    public Path classTreeCache(String jarName) {
        return jarRoot(jarName).resolve("_class-tree.json");
    }

    /** Version-aware class-tree cache. null version → default _class-tree.json. */
    public Path classTreeCache(String jarName, String version) {
        // isBlank, not just null: a whitespace-only version produced "_class-tree_  .json" — a
        // real file with spaces in the name, which then never matched the default cache and was
        // never invalidated. Blank means "no version", same as null.
        if (version == null || version.isBlank()) return classTreeCache(jarName);
        return jarRoot(jarName).resolve("_class-tree_" + version.trim() + ".json");
    }

    /** Resolved + ambiguous cross-JAR HTTP_CALL/FEIGN link results (see ExternalApiLinkService). */
    public Path externalLinksFile(String jarName) {
        return jarRoot(jarName).resolve("external-links.json");
    }

    /** Unresolved cross-JAR HTTP_CALL/FEIGN call sites awaiting a matching endpoint elsewhere. */
    public Path externalLinksPendingFile(String jarName) {
        return jarRoot(jarName).resolve("external-links-pending.json");
    }

    /**
     * No-op: the store creates any containing structure on write, and an object store has no
     * directories to create at all. Kept so the many callers that announce their intent up front
     * still compile and read the same.
     */
    public void ensureJarRoot(String jarName) throws IOException {
        // intentionally empty — see javadoc
    }

    /**
     * Ids of every analysis under this base, from ONE prefix listing.
     *
     * <p>The old implementation stat-ed two candidate files per directory. That is two S3 HEADs
     * per analysis; a single listing answers the same question in one round trip.
     */
    public List<String> listJarIds() {
        List<String> ids = new ArrayList<>();
        ArtifactKey root = ArtifactKey.of(keyRoot());
        try {
            var all = store().statPrefix(root);
            java.util.Set<String> seen = new java.util.LinkedHashSet<>();
            int depth = keyRoot().length;
            for (var k : all.keySet()) {
                var segs = k.segments();
                if (segs.size() < depth + 2) continue;              // <root>/<id>/<file>
                String leaf = k.leaf();
                if (!leaf.equals("analysis.json") && !leaf.equals("analysis_corrected.json")) continue;
                if (segs.size() != depth + 2) continue;             // only directly under <id>/
                seen.add(segs.get(depth));
            }
            ids.addAll(seen);
        } catch (Exception e) {
            log.debug("Failed to list jar ids: {}", e.getMessage());
        }
        return ids;
    }

    /**
     * @deprecated the returned {@link Path}s are only real files in local mode. Use
     *             {@link #listJarIds()} and the key accessors.
     */
    @Deprecated
    public List<Path> listJarRoots() {
        List<Path> roots = new ArrayList<>();
        for (String id : listJarIds()) roots.add(baseDir.resolve(id));
        return roots;
    }

    public void deleteJarRoot(String jarName) throws IOException {
        int removed = store().deletePrefix(jarRootKey(jarName));
        if (removed > 0) log.info("Deleted jar root: {} ({} artifacts)", jarRootKey(jarName), removed);
    }
}
