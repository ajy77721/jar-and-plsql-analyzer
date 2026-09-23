package com.analyzer.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

/**
 * An {@link ArtifactStore} with no filesystem behind it at all.
 *
 * <p>This exists to prove a claim that is otherwise very hard to prove: that the application never
 * writes analysis data to local disk. Testing that against {@link LocalArtifactStore} is circular —
 * it writes to disk by design, so a stray {@code Files.write} somewhere in the application is
 * indistinguishable from correct behaviour. Against this store, any code that still reaches for the
 * filesystem either fails outright or leaves a file behind where there should be none, and both are
 * detectable.
 *
 * <p>It is also the honest stand-in for S3 in an offline build. It shares the properties that
 * actually break naive code — no directories, no atomic rename, no append, no seekable write — so
 * code that works here is code that will work against an object store, without needing a live
 * bucket to find out.
 *
 * <p>Not intended for production: everything is held in memory. It is deliberately in {@code main}
 * rather than {@code test} so that other modules' tests can wire it.
 */
public final class InMemoryArtifactStore implements ArtifactStore {

    /** Sorted so prefix scans are a subMap rather than a full walk, as an object store's are. */
    private final ConcurrentSkipListMap<String, byte[]> objects = new ConcurrentSkipListMap<>();
    private final Map<String, Long> modified = new HashMap<>();

    /** Every temp file handed out by {@link #materialize}, so a test can assert they were cleaned up. */
    private final List<java.nio.file.Path> materialized = Collections.synchronizedList(new ArrayList<>());

    public int objectCount() {
        return objects.size();
    }

    public Set<String> keys() {
        return new TreeSet<>(objects.keySet());
    }

    /** Temp files created by {@link #materialize} that still exist — should be empty after a run. */
    public List<java.nio.file.Path> leakedTempFiles() {
        synchronized (materialized) {
            return materialized.stream().filter(Files::exists).toList();
        }
    }

    @Override
    public OutputStream write(ArtifactKey key) {
        String k = key.toString();
        // Buffered then published on close, mirroring the atomic-on-close contract: a reader never
        // sees a half-written object, and a failed write leaves no partial one.
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();
                objects.put(k, toByteArray());
                modified.put(k, System.currentTimeMillis());
            }
        };
    }

    @Override
    public OutputStream write(ArtifactKey key, long contentLength) {
        return write(key);
    }

    @Override
    public InputStream read(ArtifactKey key) throws IOException {
        byte[] b = objects.get(key.toString());
        if (b == null) throw new java.io.FileNotFoundException("No such artifact: " + key);
        return new ByteArrayInputStream(b);
    }

    @Override
    public InputStream read(ArtifactKey key, long offset, long length) throws IOException {
        byte[] b = objects.get(key.toString());
        if (b == null) throw new java.io.FileNotFoundException("No such artifact: " + key);
        int from = (int) Math.min(offset, b.length);
        int len = (int) Math.min(length, b.length - (long) from);
        return new ByteArrayInputStream(b, from, Math.max(len, 0));
    }

    @Override
    public long size(ArtifactKey key) {
        byte[] b = objects.get(key.toString());
        return b == null ? -1 : b.length;
    }

    /**
     * Backdates an object, for tests that exercise time-based retention.
     *
     * <p>Writes stamp {@code currentTimeMillis}, so without this a test could only ever produce
     * "just written" objects and a retention window could not be exercised at all. Present on the
     * in-memory double only; the real stores take their timestamp from the medium.
     *
     * @return true if the key existed
     */
    public boolean setLastModifiedForTest(ArtifactKey key, long millis) {
        if (!objects.containsKey(key.toString())) return false;
        modified.put(key.toString(), millis);
        return true;
    }

    @Override
    public long lastModified(ArtifactKey key) {
        return modified.getOrDefault(key.toString(), -1L);
    }

    @Override
    public boolean exists(ArtifactKey key) {
        return objects.containsKey(key.toString());
    }

    @Override
    public boolean delete(ArtifactKey key) {
        modified.remove(key.toString());
        return objects.remove(key.toString()) != null;
    }

    @Override
    public int deletePrefix(ArtifactKey prefix) {
        String p = prefix.toString() + "/";
        List<String> doomed = new ArrayList<>(objects.subMap(p, p + Character.MAX_VALUE).keySet());
        if (objects.containsKey(prefix.toString())) doomed.add(prefix.toString());
        doomed.forEach(k -> { objects.remove(k); modified.remove(k); });
        return doomed.size();
    }

    @Override
    public Stream<ArtifactEntry> list(ArtifactKey prefix, boolean recursive) {
        String p = prefix.toString() + "/";
        Map<String, ArtifactEntry> out = new LinkedHashMap<>();
        for (Map.Entry<String, byte[]> e : objects.subMap(p, p + Character.MAX_VALUE).entrySet()) {
            String rel = e.getKey().substring(p.length());
            int slash = rel.indexOf('/');
            if (!recursive && slash >= 0) {
                // Shallow listing synthesises the common prefix, exactly as S3's delimiter does.
                // There is no directory object to report a size or mtime for, hence the zeroes.
                String dir = p + rel.substring(0, slash);
                out.putIfAbsent(dir, new ArtifactEntry(keyOf(dir), 0, 0, true));
            } else {
                out.put(e.getKey(), new ArtifactEntry(keyOf(e.getKey()), e.getValue().length,
                        modified.getOrDefault(e.getKey(), 0L), false));
            }
        }
        return out.values().stream();
    }

    @Override
    public void copy(ArtifactKey from, ArtifactKey to) throws IOException {
        byte[] b = objects.get(from.toString());
        if (b == null) throw new java.io.FileNotFoundException("No such artifact: " + from);
        objects.put(to.toString(), b.clone());
        modified.put(to.toString(), System.currentTimeMillis());
    }

    @Override
    public void move(ArtifactKey from, ArtifactKey to) throws IOException {
        copy(from, to);
        delete(from);
    }

    @Override
    public LocalHandle materialize(ArtifactKey key) throws IOException {
        byte[] b = objects.get(key.toString());
        if (b == null) throw new java.io.FileNotFoundException("No such artifact: " + key);
        // A temp file OUTSIDE the data root — the documented escape hatch for APIs needing a
        // seekable file. Tracked so a test can prove the handle was closed.
        java.nio.file.Path tmp = Files.createTempFile("artifact-", "-" + key.leaf());
        Files.write(tmp, b);
        materialized.add(tmp);
        return new LocalHandle() {
            @Override public java.nio.file.Path path() { return tmp; }
            @Override public boolean temporary() { return true; }
            @Override public void close() {
                try { Files.deleteIfExists(tmp); } catch (IOException ignore) { /* best effort */ }
            }
        };
    }

    private static ArtifactKey keyOf(String s) {
        return ArtifactKey.of(s.split("/"));
    }
}
