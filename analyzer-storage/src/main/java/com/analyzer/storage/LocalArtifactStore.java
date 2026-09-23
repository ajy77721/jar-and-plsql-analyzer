package com.analyzer.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Filesystem-backed {@link ArtifactStore} — the {@code internal} storage mode.
 *
 * <p>This is the default and must remain behaviourally identical to the direct {@code Files.*}
 * calls it replaces: same paths, same bytes, same listing order. It exists so the rest of the
 * codebase can be written against the interface; it is not an optimisation.
 *
 * <p>One deliberate improvement over the raw calls: {@link #write} is atomic-on-close (write to a
 * sibling temp file, then move into place). Previously a crash mid-write left a truncated JSON
 * file that later failed to parse with a confusing error. Readers now see either the old file or
 * the new one.
 */
public class LocalArtifactStore implements ArtifactStore {

    private static final Logger log = LoggerFactory.getLogger(LocalArtifactStore.class);

    private final Path root;

    public LocalArtifactStore(Path root) {
        this.root = root.toAbsolutePath().normalize();
    }

    public Path root() {
        return root;
    }

    /** Resolves a key beneath the root. Segment validation in ArtifactKey guarantees containment. */
    private Path resolve(ArtifactKey key) {
        return key.toPath(root);
    }

    @Override
    public OutputStream write(ArtifactKey key) throws IOException {
        return write(key, -1);
    }

    @Override
    public OutputStream write(ArtifactKey key, long contentLength) throws IOException {
        Path target = resolve(key);
        Files.createDirectories(target.getParent());
        // Temp sibling, not a system temp dir: guarantees the final move is same-filesystem and
        // therefore atomic. A cross-device move would silently degrade to copy-then-delete.
        Path temp = Files.createTempFile(target.getParent(), "." + target.getFileName() + ".", ".tmp");
        OutputStream raw = new BufferedOutputStream(Files.newOutputStream(temp), 256 * 1024);
        return new FilterOutputStream(raw) {
            private boolean closed;

            // FilterOutputStream.write(byte[],int,int) loops over single-byte writes, which is
            // catastrophically slow for multi-GB artifacts. Delegate the bulk form directly.
            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                out.write(b, off, len);
            }

            @Override
            public void close() throws IOException {
                if (closed) return;
                closed = true;
                try {
                    super.close();
                    Files.move(temp, target,
                            StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException e) {
                    Files.deleteIfExists(temp);
                    throw e;
                }
            }
        };
    }

    @Override
    public InputStream read(ArtifactKey key) throws IOException {
        return Files.newInputStream(resolve(key));
    }

    @Override
    public InputStream read(ArtifactKey key, long offset, long length) throws IOException {
        if (offset < 0) throw new IllegalArgumentException("offset must be >= 0, was " + offset);
        InputStream in = Files.newInputStream(resolve(key));
        try {
            // Throws EOFException if the object is shorter than offset — surfaced as-is so a
            // bad range is a clear error rather than a silently empty read.
            in.skipNBytes(offset);
            return length < 0 ? in : new BoundedInputStream(in, length);
        } catch (IOException e) {
            in.close();
            throw e;
        }
    }

    @Override
    public boolean exists(ArtifactKey key) {
        return Files.exists(resolve(key));
    }

    @Override
    public long size(ArtifactKey key) {
        try {
            return Files.size(resolve(key));
        } catch (IOException e) {
            return -1;
        }
    }

    @Override
    public long lastModified(ArtifactKey key) {
        try {
            return Files.getLastModifiedTime(resolve(key)).toMillis();
        } catch (IOException e) {
            return -1;
        }
    }

    @Override
    public boolean delete(ArtifactKey key) {
        try {
            return Files.deleteIfExists(resolve(key));
        } catch (IOException e) {
            log.warn("Failed to delete {}: {}", key, e.getMessage());
            return false;
        }
    }

    @Override
    public int deletePrefix(ArtifactKey prefix) throws IOException {
        Path dir = resolve(prefix);
        if (!Files.exists(dir)) return 0;
        int[] count = {0};
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    if (Files.deleteIfExists(p)) count[0]++;
                } catch (IOException e) {
                    log.warn("Failed to delete {}: {}", p, e.getMessage());
                }
            });
        }
        return count[0];
    }

    @Override
    public Stream<ArtifactEntry> list(ArtifactKey prefix, boolean recursive) throws IOException {
        Path dir = resolve(prefix);
        if (!Files.isDirectory(dir)) return Stream.empty();
        Stream<Path> walk = recursive ? Files.walk(dir) : Files.list(dir);
        return walk.filter(p -> !p.equals(dir)).map(p -> toEntry(prefix, dir, p));
    }

    private ArtifactEntry toEntry(ArtifactKey prefix, Path dir, Path p) {
        Path rel = dir.relativize(p);
        ArtifactKey key = prefix;
        for (Path seg : rel) {
            key = key.resolve(seg.toString());
        }
        boolean isDir = Files.isDirectory(p);
        long size = 0;
        long mtime = -1;
        try {
            if (!isDir) size = Files.size(p);
            mtime = Files.getLastModifiedTime(p).toMillis();
        } catch (IOException ignored) {
            // Entry vanished between listing and stat — report what we have rather than failing
            // the whole listing.
        }
        return new ArtifactEntry(key, size, mtime, isDir);
    }

    @Override
    public void copy(ArtifactKey from, ArtifactKey to) throws IOException {
        Path target = resolve(to);
        Files.createDirectories(target.getParent());
        Files.copy(resolve(from), target, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void move(ArtifactKey from, ArtifactKey to) throws IOException {
        Path target = resolve(to);
        Files.createDirectories(target.getParent());
        Files.move(resolve(from), target, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public LocalHandle materialize(ArtifactKey key) throws IOException {
        Path p = resolve(key);
        if (!Files.exists(p)) throw new IOException("No such artifact: " + key);
        // Already local — hand back the real file and make close() a no-op. Nothing is copied,
        // and crucially nothing is deleted on close.
        return new LocalHandle() {
            @Override public Path path() { return p; }
            @Override public boolean temporary() { return false; }
            @Override public void close() { }
        };
    }

    /** Caps a stream at {@code remaining} bytes, for ranged reads. */
    private static final class BoundedInputStream extends InputStream {
        private final InputStream in;
        private long remaining;

        BoundedInputStream(InputStream in, long limit) {
            this.in = in;
            this.remaining = limit;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) return -1;
            int b = in.read();
            if (b >= 0) remaining--;
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remaining <= 0) return -1;
            int n = in.read(b, off, (int) Math.min(len, remaining));
            if (n > 0) remaining -= n;
            return n;
        }

        @Override
        public int available() throws IOException {
            return (int) Math.min(in.available(), remaining);
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
