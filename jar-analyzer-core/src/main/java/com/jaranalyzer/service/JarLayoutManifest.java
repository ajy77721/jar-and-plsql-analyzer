package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The per-JAR layout manifest (plan 2c): one small JSON that inventories what the save wrote, so a
 * reader can route an API with a single small read instead of listing fans and probing keys.
 *
 * <p>Mirrors the PL/SQL {@code LayoutManifest} (plan 1f) in design: built from values the writer
 * already knows (byte lengths it counted on the way out, record counts it counted anyway), never by
 * re-reading {@code analysis.json}; families with many members are collapsed into one aggregate
 * entry so the manifest stays in the low KBs regardless of analysis size; and a missing or corrupt
 * manifest changes nothing — readers that have not adopted it keep working exactly as before.
 *
 * <h2>Counts</h2>
 *
 * <p>{@code counts.classes} and {@code counts.endpoints} come straight from the write. {@code
 * counts.packages} is the number of distinct package names seen while the classes array was written
 * — counted in the same pass, at no extra read.
 */
public final class JarLayoutManifest {

    public static final int MANIFEST_VERSION = 1;

    /** Above this many keys under one parent, the family is summarised instead of listed. */
    static final int MAX_GROUP = 16;

    private final String jarName;
    private final long generatedAt;

    /** Insertion-ordered so the manifest is deterministic: write order, not hash order. */
    private final Map<String, ArtifactKey> keys = new LinkedHashMap<>();
    private final Map<String, Long> lengths = new LinkedHashMap<>();
    private final Map<String, Long> rowCounts = new LinkedHashMap<>();
    /**
     * Last-modified millis supplied by a caller that already has them (a single
     * {@link ArtifactStore#statPrefix} listing carries every key's size AND mtime), so
     * {@link #toDocument} does not have to issue one HEAD per recorded artifact.
     */
    private final Map<String, Long> lastModified = new LinkedHashMap<>();
    private final Map<String, Long> counts = new LinkedHashMap<>();
    private final Map<String, Object> sections = new LinkedHashMap<>();
    /** Prefix aggregates recorded via {@link #recordPrefix}, merged into the document's collections. */
    private final List<Map<String, Object>> prefixCollections = new ArrayList<>();

    public JarLayoutManifest(String jarName) {
        this(jarName, System.currentTimeMillis());
    }

    /** Test hook: pins {@code generatedAt} so a manifest can be compared byte for byte. */
    JarLayoutManifest(String jarName, long generatedAt) {
        this.jarName = jarName == null ? "" : jarName;
        this.generatedAt = generatedAt;
    }

    /** Records one written artifact and its byte length. Re-recording a key updates it in place. */
    public JarLayoutManifest recordArtifact(ArtifactKey key, long byteLength) {
        return recordArtifact(key, byteLength, null);
    }

    /** As above, with a record count where the writer knows it. */
    public JarLayoutManifest recordArtifact(ArtifactKey key, long byteLength, Long rows) {
        return recordArtifact(key, byteLength, rows, null);
    }

    /**
     * As above, with a last-modified millis the caller already holds from a listing. When supplied
     * (and non-negative) {@link #toDocument} uses it instead of a per-key {@code lastModified} HEAD,
     * which is what lets a whole manifest be built from one {@code statPrefix}.
     */
    public JarLayoutManifest recordArtifact(ArtifactKey key, long byteLength, Long rows, Long mtime) {
        if (key == null) return this;
        String k = key.toString();
        keys.put(k, key);
        lengths.put(k, byteLength);
        if (rows != null) rowCounts.put(k, rows);
        if (mtime != null && mtime >= 0) lastModified.put(k, mtime);
        return this;
    }

    /** A top-level count (classes, endpoints, packages). */
    public JarLayoutManifest setCount(String name, long value) {
        counts.put(name, value);
        return this;
    }

    /** A structured sub-section (offsets refs, class-tree chunks). */
    public JarLayoutManifest setSection(String name, Map<String, Object> value) {
        if (name != null && value != null) sections.put(name, value);
        return this;
    }

    /** Serialises the manifest to {@code key}. */
    public void writeTo(ArtifactStore store, ArtifactKey key) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        if (pretty()) mapper.enable(SerializationFeature.INDENT_OUTPUT);
        try (OutputStream out = store.write(key)) {
            mapper.writeValue(out, toDocument(store));
        }
    }

    private static boolean pretty() {
        return Boolean.getBoolean("analyzer.artifact.pretty");
    }

    /** The manifest as a document — exposed for tests, and the single place its shape is defined. */
    public Map<String, Object> toDocument(ArtifactStore store) throws IOException {
        Map<String, List<String>> groups = new LinkedHashMap<>();
        for (String k : keys.keySet()) {
            ArtifactKey key = keys.get(k);
            ArtifactKey parent = key.parent();
            groups.computeIfAbsent(parent == null ? "" : parent.toString(), p -> new ArrayList<>()).add(k);
        }

        List<Map<String, Object>> artifacts = new ArrayList<>();
        List<Map<String, Object>> collections = new ArrayList<>();
        for (Map.Entry<String, List<String>> g : groups.entrySet()) {
            List<String> members = g.getValue();
            if (members.size() > MAX_GROUP) {
                long totalBytes = 0;
                for (String k : members) totalBytes += lengths.get(k);
                Map<String, Object> c = new LinkedHashMap<>();
                c.put("prefix", g.getKey() + "/");
                c.put("count", members.size());
                c.put("totalBytes", totalBytes);
                collections.add(c);
            } else {
                for (String k : members) {
                    Map<String, Object> a = new LinkedHashMap<>();
                    a.put("key", k);
                    a.put("length", lengths.get(k));
                    Long rows = rowCounts.get(k);
                    if (rows != null) a.put("rows", rows);
                    // Prefer the mtime the caller already read from the listing; only fall back to a
                    // HEAD when it did not supply one. Same value either way — a listing's mtime IS
                    // the object's last-modified — so the document bytes are unchanged.
                    Long known = lastModified.get(k);
                    long lastModified = known != null ? known : store.lastModified(keys.get(k));
                    if (lastModified >= 0) a.put("lastModified", lastModified);
                    artifacts.add(a);
                }
            }
        }
        collections.addAll(prefixCollections);

        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("manifestVersion", MANIFEST_VERSION);
        doc.put("jarName", jarName);
        doc.put("generatedAt", generatedAt);
        doc.put("counts", new LinkedHashMap<>(counts));
        doc.put("artifacts", artifacts);
        doc.put("collections", collections);
        doc.putAll(sections);
        return doc;
    }

    /**
     * Inventories a directory family as one aggregate entry, so a per-endpoint folder tree does not
     * make the manifest as large as the thing it indexes. One listing, no per-key HEADs.
     */
    public JarLayoutManifest recordPrefix(ArtifactStore store, ArtifactKey prefix) throws IOException {
        if (prefix == null) return this;
        return recordPrefix(store.statPrefix(prefix), prefix);
    }

    /**
     * As {@link #recordPrefix(ArtifactStore, ArtifactKey)}, but from a listing the caller already
     * holds — so a save that needs both the artifact probes and the prefix aggregate pays for ONE
     * {@code statPrefix} rather than one per family.
     */
    public JarLayoutManifest recordPrefix(Map<ArtifactKey, ArtifactEntry> tree, ArtifactKey prefix) {
        if (prefix == null || tree == null) return this;
        long count = 0;
        long totalBytes = 0;
        for (Map.Entry<ArtifactKey, ArtifactEntry> e : tree.entrySet()) {
            if (!isUnder(e.getKey(), prefix)) continue;
            ArtifactEntry entry = e.getValue();
            if (entry != null && entry.directory()) continue;
            count++;
            totalBytes += entry == null ? 0 : entry.size();
        }
        if (count == 0) return this;
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("prefix", prefix + "/");
        c.put("count", count);
        c.put("totalBytes", totalBytes);
        prefixCollections.add(c);
        return this;
    }

    /** True when {@code key} is {@code prefix} itself or sits under it. */
    private static boolean isUnder(ArtifactKey key, ArtifactKey prefix) {
        if (key == null) return false;
        List<String> k = key.segments();
        List<String> p = prefix.segments();
        return k.size() >= p.size() && k.subList(0, p.size()).equals(p);
    }
}
