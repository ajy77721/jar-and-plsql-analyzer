package com.analyzer.storage;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A typed, '/'-joined, traversal-safe location for a stored artifact.
 *
 * <p>This is the single place artifact locations are constructed. Every segment is validated on
 * the way in, so a key can never contain {@code ""}, {@code "."}, {@code ".."}, a path separator,
 * or a NUL — which means {@link #toPath(Path)} cannot escape its base directory and
 * {@link #s3Key(String)} cannot escape its prefix. Callers therefore do not need their own
 * traversal guards.
 *
 * <p>Deliberately storage-agnostic: the same key resolves to a local {@link Path} in
 * {@code internal} mode and to an S3 object key in {@code external} mode.
 */
public final class ArtifactKey {

    private final List<String> segments;

    private ArtifactKey(List<String> segments) {
        this.segments = Collections.unmodifiableList(segments);
    }

    /**
     * @throws IllegalArgumentException if any segment is null, blank, "." or "..", or contains
     *         '/', '\\' or NUL. This is a programming error, not user input handling — callers
     *         that accept user input must sanitize first (see JarNameUtil).
     */
    public static ArtifactKey of(String... segments) {
        if (segments == null || segments.length == 0) {
            throw new IllegalArgumentException("ArtifactKey requires at least one segment");
        }
        List<String> validated = new ArrayList<>(segments.length);
        for (String s : segments) {
            validated.add(validate(s));
        }
        return new ArtifactKey(validated);
    }

    private static String validate(String s) {
        if (s == null || s.isBlank()) {
            throw new IllegalArgumentException("ArtifactKey segment must not be null or blank");
        }
        if (s.equals(".") || s.equals("..")) {
            throw new IllegalArgumentException("ArtifactKey segment must not be a relative path: " + s);
        }
        if (s.indexOf('/') >= 0 || s.indexOf('\\') >= 0 || s.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(
                    "ArtifactKey segment must not contain a separator or NUL: " + s);
        }
        return s;
    }

    /** A new key with additional segments appended. This key is unchanged. */
    public ArtifactKey resolve(String... more) {
        List<String> combined = new ArrayList<>(segments);
        for (String s : more) {
            combined.add(validate(s));
        }
        return new ArtifactKey(combined);
    }

    /** @return the containing key, or null when this key has a single segment. */
    public ArtifactKey parent() {
        if (segments.size() <= 1) return null;
        return new ArtifactKey(new ArrayList<>(segments.subList(0, segments.size() - 1)));
    }

    /** The final segment — the "file name". */
    public String leaf() {
        return segments.get(segments.size() - 1);
    }

    public List<String> segments() {
        return segments;
    }

    /** S3 object key: {@code rootPrefix/seg0/seg1/...}. A blank prefix yields the bare key. */
    public String s3Key(String rootPrefix) {
        String joined = String.join("/", segments);
        if (rootPrefix == null || rootPrefix.isBlank()) return joined;
        String p = rootPrefix.endsWith("/") ? rootPrefix.substring(0, rootPrefix.length() - 1) : rootPrefix;
        return p + "/" + joined;
    }

    /**
     * Local filesystem location under {@code base}. Because every segment is validated, the
     * result is always inside {@code base} — no normalization or containment check is needed
     * at the call site.
     */
    public Path toPath(Path base) {
        Path p = base;
        for (String s : segments) {
            p = p.resolve(s);
        }
        return p;
    }

    @Override
    public String toString() {
        return String.join("/", segments);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ArtifactKey other && segments.equals(other.segments);
    }

    @Override
    public int hashCode() {
        return segments.hashCode();
    }
}
