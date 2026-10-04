package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.LocalHandle;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

/**
 * TEST-SCOPE profiling decorator around a real {@link ArtifactStore}. Counts and times every
 * operation, counts bytes for reads, and can (a) run read-only (every mutation throws, which the
 * production callers already treat as best-effort failure) and (b) hide a key prefix so a sidecar
 * looks absent without deleting anything from the real backing store.
 *
 * <p>Nothing here is production code; it exists only to produce the PROFILE tables.
 */
public final class ProfilingArtifactStore implements ArtifactStore {

    private final ArtifactStore delegate;
    private final boolean readOnly;
    /** Key.toString() prefix hidden from every read/metadata/listing call; null = hide nothing. */
    private final String maskPrefix;

    // op counts
    private final LongAdder readOps = new LongAdder();
    private final LongAdder rangeReadOps = new LongAdder();
    private final LongAdder existsOps = new LongAdder();
    private final LongAdder sizeOps = new LongAdder();
    private final LongAdder lastModifiedOps = new LongAdder();
    private final LongAdder listOps = new LongAdder();
    private final LongAdder statPrefixOps = new LongAdder();
    private final LongAdder writeOps = new LongAdder();
    private final LongAdder otherOps = new LongAdder();

    private final LongAdder bytesRead = new LongAdder();
    private final LongAdder bytesWritten = new LongAdder();
    private final LongAdder maskedOps = new LongAdder();
    private final LongAdder deniedWrites = new LongAdder();

    // latencies (nanos) per op class
    private final List<Long> getNanos = Collections.synchronizedList(new ArrayList<>());
    private final List<Long> headNanos = Collections.synchronizedList(new ArrayList<>());
    private final List<Long> listNanos = Collections.synchronizedList(new ArrayList<>());

    // per-key touch counts: "<op> <key>"
    private final Map<String, LongAdder> touches = new ConcurrentHashMap<>();

    public ProfilingArtifactStore(ArtifactStore delegate) {
        this(delegate, false, null);
    }

    public ProfilingArtifactStore(ArtifactStore delegate, boolean readOnly, String maskPrefix) {
        this.delegate = delegate;
        this.readOnly = readOnly;
        this.maskPrefix = maskPrefix;
    }

    private boolean masked(ArtifactKey key) {
        return maskPrefix != null && key.toString().startsWith(maskPrefix);
    }

    private void touch(String op, ArtifactKey key) {
        touches.computeIfAbsent(op + " " + key, k -> new LongAdder()).increment();
    }

    /* ───────────────────────────── reads ───────────────────────────── */

    @Override
    public InputStream read(ArtifactKey key) throws IOException {
        if (masked(key)) {
            maskedOps.increment();
            touch("GET(masked)", key);
            throw new NoSuchFileException(key.toString());
        }
        readOps.increment();
        touch("GET", key);
        long start = System.nanoTime();
        InputStream in = delegate.read(key);
        return new CountingInputStream(in, bytesRead, () -> getNanos.add(System.nanoTime() - start));
    }

    @Override
    public InputStream read(ArtifactKey key, long offset, long length) throws IOException {
        if (masked(key)) {
            maskedOps.increment();
            touch("GET(rng)(masked)", key);
            throw new NoSuchFileException(key.toString());
        }
        rangeReadOps.increment();
        touch("GET(rng)", key);
        long start = System.nanoTime();
        InputStream in = delegate.read(key, offset, length);
        return new CountingInputStream(in, bytesRead, () -> getNanos.add(System.nanoTime() - start));
    }

    /* ─────────────────────────── metadata ─────────────────────────── */

    @Override
    public boolean exists(ArtifactKey key) {
        if (masked(key)) {
            maskedOps.increment();
            touch("HEAD(exists)(masked)", key);
            return false;
        }
        existsOps.increment();
        touch("HEAD(exists)", key);
        long start = System.nanoTime();
        try {
            return delegate.exists(key);
        } finally {
            headNanos.add(System.nanoTime() - start);
        }
    }

    @Override
    public long size(ArtifactKey key) {
        if (masked(key)) {
            maskedOps.increment();
            return -1;
        }
        sizeOps.increment();
        touch("HEAD(size)", key);
        long start = System.nanoTime();
        try {
            return delegate.size(key);
        } finally {
            headNanos.add(System.nanoTime() - start);
        }
    }

    @Override
    public long lastModified(ArtifactKey key) {
        if (masked(key)) {
            maskedOps.increment();
            touch("HEAD(lastMod)(masked)", key);
            return -1;
        }
        lastModifiedOps.increment();
        touch("HEAD(lastMod)", key);
        long start = System.nanoTime();
        try {
            return delegate.lastModified(key);
        } finally {
            headNanos.add(System.nanoTime() - start);
        }
    }

    /* ─────────────────────────── listing ─────────────────────────── */

    @Override
    public Stream<ArtifactEntry> list(ArtifactKey prefix, boolean recursive) throws IOException {
        if (masked(prefix)) {
            maskedOps.increment();
            return Stream.empty();
        }
        listOps.increment();
        touch("LIST", prefix);
        long start = System.nanoTime();
        Stream<ArtifactEntry> s = delegate.list(prefix, recursive);
        return s.onClose(() -> listNanos.add(System.nanoTime() - start));
    }

    @Override
    public Map<ArtifactKey, ArtifactEntry> statPrefix(ArtifactKey prefix) throws IOException {
        if (masked(prefix)) {
            maskedOps.increment();
            return new LinkedHashMap<>();
        }
        statPrefixOps.increment();
        touch("STATPREFIX", prefix);
        long start = System.nanoTime();
        try {
            return delegate.statPrefix(prefix);
        } finally {
            listNanos.add(System.nanoTime() - start);
        }
    }

    /* ─────────────────────────── mutations ─────────────────────────── */

    @Override
    public OutputStream write(ArtifactKey key) throws IOException {
        return denyWrite(key, -1);
    }

    @Override
    public OutputStream write(ArtifactKey key, long contentLength) throws IOException {
        return denyWrite(key, contentLength);
    }

    private OutputStream denyWrite(ArtifactKey key, long len) throws IOException {
        if (readOnly) {
            deniedWrites.increment();
            touch("PUT(denied)", key);
            throw new IOException("read-only profiling store: write denied for " + key);
        }
        writeOps.increment();
        touch("PUT", key);
        OutputStream out = len < 0 ? delegate.write(key) : delegate.write(key, len);
        return new CountingOutputStream(out, bytesWritten);
    }

    @Override
    public boolean delete(ArtifactKey key) {
        if (readOnly) {
            deniedWrites.increment();
            return false;
        }
        otherOps.increment();
        return delegate.delete(key);
    }

    @Override
    public int deletePrefix(ArtifactKey prefix) throws IOException {
        if (readOnly) {
            deniedWrites.increment();
            return 0;
        }
        otherOps.increment();
        return delegate.deletePrefix(prefix);
    }

    @Override
    public void copy(ArtifactKey from, ArtifactKey to) throws IOException {
        if (readOnly) {
            deniedWrites.increment();
            throw new IOException("read-only profiling store: copy denied");
        }
        otherOps.increment();
        delegate.copy(from, to);
    }

    @Override
    public void move(ArtifactKey from, ArtifactKey to) throws IOException {
        if (readOnly) {
            deniedWrites.increment();
            throw new IOException("read-only profiling store: move denied");
        }
        otherOps.increment();
        delegate.move(from, to);
    }

    @Override
    public LocalHandle materialize(ArtifactKey key) throws IOException {
        readOps.increment();
        touch("GET(materialize)", key);
        return delegate.materialize(key);
    }

    /* ─────────────────────────── reporting ─────────────────────────── */

    public long totalOps() {
        return readOps.sum() + rangeReadOps.sum() + existsOps.sum() + sizeOps.sum()
                + lastModifiedOps.sum() + listOps.sum() + statPrefixOps.sum() + writeOps.sum()
                + otherOps.sum() + maskedOps.sum() + deniedWrites.sum();
    }

    public long totalHeadOps() {
        return existsOps.sum() + sizeOps.sum() + lastModifiedOps.sum();
    }

    /** Total whole+range GETs. */
    public long getOps() { return readOps.sum() + rangeReadOps.sum(); }

    /**
     * How many data ops (GET/HEAD) touched {@code key} — the assertion hook for "this artifact is
     * read exactly once by a single-pass precompute". Zero when it was never touched.
     */
    public long dataOpsForKey(ArtifactKey key) {
        long n = 0;
        for (Map.Entry<String, LongAdder> e : touches.entrySet()) {
            String entry = e.getKey();
            int sp = entry.indexOf(' ');
            String op = entry.substring(0, sp);
            if (!op.startsWith("GET") && !op.startsWith("HEAD")) continue;
            if (entry.substring(sp + 1).equals(key.toString())) n += e.getValue().sum();
        }
        return n;
    }

    /** Total HEADs (exists + size + lastModified). */
    public long headOps() { return totalHeadOps(); }

    /** Total LIST calls. */
    public long listOps() { return listOps.sum(); }

    public long bytesRead() {
        return bytesRead.sum();
    }

    /** Human-readable PROFILE block. */
    public String report(String label) {
        StringBuilder b = new StringBuilder();
        b.append("\n===== PROFILE: ").append(label).append(" =====\n");
        b.append(String.format(Locale.ROOT, "ops total .......... %,d  (of which masked/hidden: %,d; denied writes: %,d)%n",
                totalOps(), maskedOps.sum(), deniedWrites.sum()));
        b.append(String.format(Locale.ROOT, "  GET  (whole) ..... %,d%n", readOps.sum()));
        b.append(String.format(Locale.ROOT, "  GET  (range) ..... %,d%n", rangeReadOps.sum()));
        b.append(String.format(Locale.ROOT, "  HEAD (exists) .... %,d%n", existsOps.sum()));
        b.append(String.format(Locale.ROOT, "  HEAD (size) ...... %,d%n", sizeOps.sum()));
        b.append(String.format(Locale.ROOT, "  HEAD (lastMod) ... %,d%n", lastModifiedOps.sum()));
        b.append(String.format(Locale.ROOT, "  LIST ............. %,d%n", listOps.sum()));
        b.append(String.format(Locale.ROOT, "  STATPREFIX ....... %,d%n", statPrefixOps.sum()));
        b.append(String.format(Locale.ROOT, "  PUT .............. %,d%n", writeOps.sum()));
        b.append(String.format(Locale.ROOT, "  other ............ %,d%n", otherOps.sum()));
        b.append(String.format(Locale.ROOT, "bytes read ......... %,d  (%.2f MB)%n", bytesRead.sum(),
                bytesRead.sum() / 1048576.0));
        b.append(latency("GET ", getNanos));
        b.append(latency("HEAD", headNanos));
        b.append(latency("LIST", listNanos));
        b.append(byArea());
        b.append(topKeys(10));
        return b.toString();
    }

    /** Op counts grouped by which part of the analysis tree is touched. */
    private String byArea() {
        Map<String, long[]> areas = new LinkedHashMap<>(); // name -> [GET, HEAD, LIST, bytesEst]
        Map<String, LongAdder> bytesByArea = new LinkedHashMap<>();
        for (Map.Entry<String, LongAdder> e : touches.entrySet()) {
            String entry = e.getKey();
            int sp = entry.indexOf(' ');
            String op = entry.substring(0, sp);
            String key = entry.substring(sp + 1);
            String area = areaOf(key);
            long[] c = areas.computeIfAbsent(area, k -> new long[3]);
            if (op.startsWith("GET")) c[0]++;
            else if (op.startsWith("HEAD")) c[1]++;
            else if (op.startsWith("LIST")) c[2]++;
        }
        StringBuilder b = new StringBuilder("ops by area (GET / HEAD / LIST):\n");
        for (Map.Entry<String, long[]> e : areas.entrySet()) {
            long[] c = e.getValue();
            b.append(String.format(Locale.ROOT, "  %-10s GET %4d | HEAD %4d | LIST %3d%n",
                    e.getKey(), c[0], c[1], c[2]));
        }
        b.append(String.format(Locale.ROOT, "distinct keys touched: %d%n", distinctKeys()));
        return b.toString();
    }

    private static String areaOf(String key) {
        if (key.contains("/api/nodes/")) return "nodes";
        if (key.contains("/sources/")) return "sources";
        if (key.contains("/views/")) return "views";
        if (key.endsWith("api/index.json")) return "index";
        if (key.contains("/api/")) return "api-other";
        return "other";
    }

    private long distinctKeys() {
        java.util.Set<String> keys = new java.util.HashSet<>();
        for (String entry : touches.keySet()) {
            int sp = entry.indexOf(' ');
            keys.add(entry.substring(sp + 1));
        }
        return keys.size();
    }

    private String latency(String name, List<Long> nanos) {
        List<Long> copy;
        synchronized (nanos) {
            copy = new ArrayList<>(nanos);
        }
        if (copy.isEmpty()) {
            return String.format(Locale.ROOT, "%s latency ...... (none)%n", name);
        }
        Collections.sort(copy);
        double min = copy.get(0) / 1e6;
        double med = copy.get(copy.size() / 2) / 1e6;
        double max = copy.get(copy.size() - 1) / 1e6;
        double sum = copy.stream().mapToLong(Long::longValue).sum() / 1e6;
        double avg = sum / copy.size();
        return String.format(Locale.ROOT,
                "%s latency ...... min %.1f ms | median %.1f ms | avg %.1f ms | max %.1f ms | total %.1f ms (n=%d)%n",
                name, min, med, avg, max, sum, copy.size());
    }

    private String topKeys(int n) {
        List<Map.Entry<String, LongAdder>> entries = new ArrayList<>(touches.entrySet());
        entries.sort((a, b) -> Long.compare(b.getValue().sum(), a.getValue().sum()));
        StringBuilder b = new StringBuilder("top keys touched (op + key -> count):\n");
        for (int i = 0; i < Math.min(n, entries.size()); i++) {
            b.append(String.format(Locale.ROOT, "  %,4d x  %s%n", entries.get(i).getValue().sum(),
                    shortKey(entries.get(i).getKey())));
        }
        return b.toString();
    }

    private static String shortKey(String s) {
        // collapse the repeated analysis prefix so the key is readable
        return s.replace("plsql-parse/POSTGRES/POSTGRESDEMO/LOCAL/ERP_USP_RUN_CYCLE/", "…/");
    }

    /** Every key touched more than once by a data op, with its counts — the redundancy finding. */
    public String duplicates() {
        Map<String, Long> perKey = new LinkedHashMap<>();
        for (Map.Entry<String, LongAdder> e : touches.entrySet()) {
            String entry = e.getKey();
            int sp = entry.indexOf(' ');
            String op = entry.substring(0, sp);
            if (!op.startsWith("GET") && !op.startsWith("HEAD")) continue;
            perKey.merge(entry.substring(sp + 1), e.getValue().sum(), Long::sum);
        }
        List<Map.Entry<String, Long>> dup = new ArrayList<>();
        for (Map.Entry<String, Long> e : perKey.entrySet()) if (e.getValue() > 1) dup.add(e);
        dup.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));
        StringBuilder b = new StringBuilder(String.format(Locale.ROOT,
                "redundant keys (touched >1x by a data op): %d of %d distinct%n", dup.size(), perKey.size()));
        for (int i = 0; i < Math.min(15, dup.size()); i++) {
            b.append(String.format(Locale.ROOT, "  %,4d x  %s%n", dup.get(i).getValue(), shortKey(dup.get(i).getKey())));
        }
        long redundantOps = dup.stream().mapToLong(e -> e.getValue() - 1).sum();
        b.append(String.format(Locale.ROOT, "  => %,d redundant ops%n", redundantOps));
        return b.toString();
    }

    /* ─────────────────────────── stream wrappers ─────────────────────────── */

    private static final class CountingInputStream extends InputStream {
        private final InputStream in;
        private final LongAdder counter;
        private final Runnable onClose;
        private boolean closed;

        CountingInputStream(InputStream in, LongAdder counter, Runnable onClose) {
            this.in = in;
            this.counter = counter;
            this.onClose = onClose;
        }

        @Override public int read() throws IOException {
            int r = in.read();
            if (r >= 0) counter.increment();
            return r;
        }

        @Override public int read(byte[] b, int off, int len) throws IOException {
            int r = in.read(b, off, len);
            if (r > 0) counter.add(r);
            return r;
        }

        @Override public int available() throws IOException { return in.available(); }

        @Override public void close() throws IOException {
            if (closed) return;
            closed = true;
            try {
                in.close();
            } finally {
                onClose.run();
            }
        }
    }

    private static final class CountingOutputStream extends OutputStream {
        private final OutputStream out;
        private final LongAdder counter;
        CountingOutputStream(OutputStream out, LongAdder counter) { this.out = out; this.counter = counter; }
        @Override public void write(int b) throws IOException { out.write(b); counter.increment(); }
        @Override public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len); counter.add(len);
        }
        @Override public void flush() throws IOException { out.flush(); }
        @Override public void close() throws IOException { out.close(); }
    }
}
