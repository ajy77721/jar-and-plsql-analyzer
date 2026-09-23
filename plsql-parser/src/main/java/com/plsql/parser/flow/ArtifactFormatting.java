package com.plsql.parser.flow;

/**
 * Whether stored artifacts are written indented.
 *
 * <h2>Why they are not, by default</h2>
 *
 * <p>Indentation is whitespace, and on this corpus it is <b>27.6%</b> of the bytes — measured across
 * 2,908 artifact files, 310.3 MB pretty against 224.7 MB compact, 85.6 MB of pure padding. That
 * fraction does not improve with scale: it is charged again on every write, every read, every byte
 * transferred to a reader, and every byte of an object store.
 *
 * <p>Nothing consumes the indentation. These files are read by the ingest, by the web layer and by
 * {@code jq} — none of which care — and the round-trip prover compares parsed documents, not text.
 *
 * <h2>The escape hatch, and why it is a property rather than a code change</h2>
 *
 * <p>A human occasionally does read one, mid-debugging, and re-running an analysis with a patched
 * build to get readable output is a poor trade. {@code -Dplsql.artifacts.pretty=true} restores it
 * for that run without touching the default that everything else pays for.
 */
public final class ArtifactFormatting {

    /** System property that restores indentation for a single run. */
    public static final String PRETTY_PROPERTY = "plsql.artifacts.pretty";

    private ArtifactFormatting() {
    }

    /** True when this run should indent stored artifacts. */
    public static boolean pretty() {
        return Boolean.getBoolean(PRETTY_PROPERTY);
    }
}
