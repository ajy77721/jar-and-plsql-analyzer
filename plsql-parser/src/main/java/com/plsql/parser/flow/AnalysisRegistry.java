package com.plsql.parser.flow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Analysis-level idempotency registry: the third gate of the central fetch decision
 * (in-run dedup → cross-run source cache + LAST_DDL → <b>this</b>).
 *
 * <p>After a successful crawl, one entry is written per analyzed object recording
 * {@code (identity, LAST_DDL) analysed at T, result COMPLETE}. On the NEXT crawl of the same
 * scope an object whose LAST_DDL is UNCHANGED and whose previous result was COMPLETE is skipped
 * entirely — no database fetch, no re-parse — and its node is restored from the node cache.
 *
 * <h2>Scope — per-analysis, never global</h2>
 * The registry is a single JSON artifact at {@code <outputKey>/.analysis-registry.json}, where
 * {@code outputKey} is the analysis parent prefix (datasource/project/env). It is addressed
 * through the SAME routed, per-user {@link ArtifactStore} as the cross-run source cache, so one
 * user's registry is invisible to another. There is deliberately <b>no global/static registry
 * location</b>: a global one would cross user and scope boundaries and let one analysis skip work
 * another analysis never performed. It mirrors the source-cache design exactly (see
 * {@link SourceDownloader#setSourceCache}) and lives under the analysis namespace so it dies with
 * that analysis scope, not with a shared root.
 *
 * <h2>Skip semantics</h2>
 * An object is skippable only when ALL hold:
 * <ul>
 *   <li>an entry exists for its identity, and</li>
 *   <li>the entry is COMPLETE ({@code nodesProduced > 0}) — a partial run (0 nodes / no facets)
 *       is never trusted, so it is re-analysed, and</li>
 *   <li>the LAST_DDL is unchanged. As in the source cache, a missing DDL on either side counts as
 *       "cannot prove it changed" and still reuses; only two KNOWN, DIFFERENT DDLs force a
 *       re-analysis, and</li>
 *   <li>the recorded node-cache leaf is present (the crawler verifies it can actually restore the
 *       node; a missing/corrupt entry falls through to a normal re-analysis).</li>
 * </ul>
 * Force-refresh is applied by the caller, not here.
 *
 * <h2>Concurrency</h2>
 * Concurrent crawls of the same scope are serialized on a per-registry-key lock (a lock is kept
 * per artifact-key string). Writes merge this run's entries over the last committed in-JVM
 * snapshot for the scope, so two crawls in the same JVM never lose each other's entries.
 * A corrupt or missing registry loads as EMPTY — fail-safe: every object is re-analysed.
 */
public class AnalysisRegistry {

    private static final Logger log = LoggerFactory.getLogger(AnalysisRegistry.class);

    private static final String FILE_NAME = ".analysis-registry.json";
    private static final int FORMAT_VERSION = 1;

    /** One analyzed object. Public fields for Jackson. */
    public static final class Entry {
        public String identity;
        public String schema;
        public String name;
        public String type;
        public String lastDdl;
        public long analyzedAt;
        /** Number of nodes this object produced. 0 (or absent) = PARTIAL = never skippable. */
        public int nodesProduced;
        /** Leaf of the node-cache artifact this object was written to, for a fetch-free restore. */
        public String nodeCacheLeaf;

        public Entry() {}
    }

    /** On-disk envelope. Public for Jackson. */
    public static final class RegistryFile {
        public int version = FORMAT_VERSION;
        public Map<String, Entry> entries = new LinkedHashMap<>();
        public RegistryFile() {}
    }

    // Per-registry-key lock: concurrent crawls of the same scope serialize their read-modify-write.
    private static final ConcurrentHashMap<String, Object> LOCKS = new ConcurrentHashMap<>();
    // Last committed entries per scope, so a second concurrent crawl's commit merges rather than
    // clobbers the first's (no extra store read needed at commit).
    private static final ConcurrentHashMap<String, Map<String, Entry>> COMMITTED = new ConcurrentHashMap<>();

    private final ArtifactStore store;
    private final ArtifactKey file;
    private final Object lock;
    private final String scopeKey;

    private final Map<String, Entry> entries = new LinkedHashMap<>();
    private final Map<String, Entry> pending = new LinkedHashMap<>();

    private final ObjectMapper mapper = new ObjectMapper();

    public AnalysisRegistry(ArtifactStore store, ArtifactKey outputKey) {
        this.store = store;
        this.file = outputKey.resolve(FILE_NAME);
        this.scopeKey = file.toString();
        this.lock = LOCKS.computeIfAbsent(scopeKey, k -> new Object());
        if (ArtifactFormatting.pretty()) mapper.enable(SerializationFeature.INDENT_OUTPUT);
        load();
    }

    /** Artifact key of the registry file — exposed for tests and diagnostics. */
    public ArtifactKey fileKey() {
        return file;
    }

    /** Load once at crawl start; corrupt/missing/IO-error => empty (fail-safe re-analyse). */
    private void load() {
        synchronized (lock) {
            Map<String, Entry> committed = COMMITTED.get(scopeKey);
            if (committed != null) {
                entries.putAll(committed);
                return;
            }
            if (store == null || !store.exists(file)) return;
            try (InputStream in = store.read(file)) {
                RegistryFile rf = mapper.readValue(in, RegistryFile.class);
                if (rf != null && rf.entries != null) entries.putAll(rf.entries);
                log.info("[AnalysisRegistry] Loaded {} entries from {}", entries.size(), file);
            } catch (IOException e) {
                entries.clear();
                log.warn("[AnalysisRegistry] Corrupt/unreadable registry {} — re-analysing all: {}",
                        file, e.getMessage());
            }
            COMMITTED.put(scopeKey, new LinkedHashMap<>(entries));
        }
    }

    /** The entry for an object identity, or null. */
    public Entry entryFor(String identity) {
        if (identity == null) return null;
        return entries.get(identity.toUpperCase());
    }

    /** True when the entry for {@code identity} is complete and unchanged (see above). */
    public boolean isSkippable(String identity, String currentDdl) {
        return isCompleteAndUnchanged(entryFor(identity), currentDdl);
    }

    /**
     * True when the object addressed by {@code baseIdentity} — or any overload of it
     * ({@code baseIdentity#<signature>}) — is complete and unchanged. Used by the level prefetch
     * to avoid even opening a connection for an object the serial loop will skip anyway. Over-
     * skipping a prefetch is safe: the serial path simply fetches what it still needs.
     */
    public boolean isSkippablePrefix(String baseIdentity, String currentDdl) {
        if (baseIdentity == null) return false;
        String base = baseIdentity.toUpperCase();
        if (isSkippable(base, currentDdl)) return true;
        String prefix = base + "#";
        for (Map.Entry<String, Entry> e : entries.entrySet()) {
            if (e.getKey().startsWith(prefix) && isCompleteAndUnchanged(e.getValue(), currentDdl)) {
                return true;
            }
        }
        return false;
    }

    /**
     * True when {@code entry} describes a COMPLETE previous analysis whose LAST_DDL matches
     * {@code currentDdl} and which has a restorable node-cache leaf. Missing DDLs on either side
     * count as "cannot prove it changed" (mirrors the source cache). Never throws.
     */
    public boolean isCompleteAndUnchanged(Entry entry, String currentDdl) {
        if (entry == null) return false;
        if (entry.nodesProduced <= 0) return false;          // PARTIAL — not done
        if (entry.nodeCacheLeaf == null || entry.nodeCacheLeaf.isBlank()) return false;
        if (entry.lastDdl != null && currentDdl != null && !entry.lastDdl.equals(currentDdl)) {
            return false;                                    // LAST_DDL changed — re-analyse
        }
        return true;
    }

    /** Record a COMPLETE analyzed object for this run. */
    public void record(String identity, String schema, String name, String type,
                       String lastDdl, int nodesProduced, String nodeCacheLeaf) {
        if (identity == null) return;
        Entry e = new Entry();
        e.identity = identity.toUpperCase();
        e.schema = schema;
        e.name = name;
        e.type = type;
        e.lastDdl = lastDdl;
        e.analyzedAt = System.currentTimeMillis();
        e.nodesProduced = nodesProduced;
        e.nodeCacheLeaf = nodeCacheLeaf;
        synchronized (pending) {
            pending.put(e.identity, e);
            // Also reflect it in the live view so a decision made on THIS instance (before save)
            // sees what was recorded — and so save()'s merge base is complete.
            entries.put(e.identity, e);
        }
    }

    /** Number of entries recorded for this run (diagnostics/tests). */
    public int pendingCount() {
        synchronized (pending) {
            return pending.size();
        }
    }

    /**
     * Persist the registry — ONE store write for the whole crawl (the read happened in the
     * constructor). Merges this run's entries over the last committed snapshot for the scope
     * under the per-key lock, so concurrent crawls in the same JVM do not clobber each other.
     * Best-effort: an IO failure never fails the crawl.
     */
    public void save() {
        synchronized (pending) {
            if (pending.isEmpty()) return;
        }
        synchronized (lock) {
            Map<String, Entry> base = COMMITTED.getOrDefault(scopeKey, entries);
            base.putAll(pending);
            RegistryFile rf = new RegistryFile();
            rf.version = FORMAT_VERSION;
            rf.entries = base;
            try (OutputStream out = store.write(file)) {
                mapper.writeValue(out, rf);
                log.info("[AnalysisRegistry] Saved {} entries to {}", base.size(), file);
            } catch (IOException e) {
                log.warn("[AnalysisRegistry] Save failed for {}: {}", file, e.getMessage());
            }
            COMMITTED.put(scopeKey, base);
        }
    }

    /** Test hook: drop the in-JVM snapshot for a scope so a fresh store is re-read. */
    public static void resetCommittedForTest(ArtifactKey outputKey) {
        String key = outputKey.resolve(FILE_NAME).toString();
        COMMITTED.remove(key);
        LOCKS.remove(key);
    }
}
