package com.codeanalyzer.ingest.mongo.write;

import com.codeanalyzer.ingest.mongo.MongoIngestService.ArtifactSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;

/**
 * A fingerprint of the artifacts an ingest would read, used to skip re-ingesting unchanged source.
 *
 * <h2>Why a digest and not a timestamp</h2>
 *
 * <p>Timestamps say when a file was written, not whether its contents differ. Re-running an
 * analysis rewrites every artifact — the storage layer has no notion of "unchanged" — so mtime
 * changes on every run and would never short-circuit. Content is the only thing that answers the
 * question actually being asked.
 *
 * <h2>What is hashed, and the ordering trap</h2>
 *
 * <p>Each artifact contributes its path and its bytes. The path matters: two artifacts whose
 * contents were swapped are a different analysis, and hashing bytes alone would call them equal.
 *
 * <p>Paths are sorted before hashing. {@code ArtifactSource.list} makes no ordering guarantee — a
 * local directory listing and an S3 key listing already differ — so an unsorted digest would vary
 * by storage backend for byte-identical input, and the short-circuit would simply never fire.
 *
 * <p>A missing artifact contributes nothing rather than a marker. That is deliberate: an artifact
 * that is absent in both runs should not make them differ, and one that appears in the second run
 * changes the digest through its own path and bytes anyway.
 */
public final class SourceDigest {

    private SourceDigest() {
    }

    /** Field name on the run document. */
    public static final String FIELD = "sourceDigest";

    /**
     * SHA-256 over every readable artifact, streamed.
     *
     * <p>Streamed in 64 KB blocks rather than read whole: the JAR {@code analysis.json} is 49 MB
     * and the statements index is 20.7 MB, and this runs before the ingest that those sizes are
     * the reason for. Loading them to decide whether to skip loading them would be self-defeating.
     */
    public static String of(ArtifactSource source, List<String> artifactPaths) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every JVM", e);
        }
        List<String> paths = new ArrayList<>(artifactPaths);
        Collections.sort(paths);

        byte[] buf = new byte[64 * 1024];
        for (String path : paths) {
            try (InputStream in = source.open(path)) {
                if (in == null) continue;
                md.update(path.getBytes(StandardCharsets.UTF_8));
                md.update((byte) 0);
                int n;
                while ((n = in.read(buf)) > 0) md.update(buf, 0, n);
            } catch (IOException e) {
                // An artifact that cannot be read is not "unchanged". Returning a digest that
                // ignored it could short-circuit a run whose input is in fact different, so the
                // safe answer is a value that never matches.
                return "unreadable:" + path;
            }
        }
        return HexFormat.of().formatHex(md.digest());
    }
}
