package com.analyzer.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Refuses to let credential files through the store, whatever backs it.
 *
 * <p>{@code plsql-config.yaml} and {@code plsql-env-config.yaml} hold plaintext database
 * passwords, and the store is rooted at the whole data tree — {@code config/} included — so in a
 * remote mode they would otherwise be uploaded along with everything else. One analysis run also
 * generates {@code plsql-env-config.yaml} inside its own output folder, which is why a
 * config-directory exclusion alone is not enough: the name can appear anywhere.
 *
 * <p>The guard lives here, at the boundary, rather than in the callers. There are 90-odd classes
 * doing file I/O and the set only grows; a check in one of them protects one of them. Matching is
 * on the exact leaf name — never an extension — because plenty of harmless {@code .yaml} files
 * must keep working.
 *
 * <p>Reads and listings are filtered too, not just writes. A guard that blocks the upload but
 * still serves the object over {@code read()} has moved the disclosure rather than closed it.
 */
public final class CredentialGuardStore implements ArtifactStore {

    private static final Logger log = LoggerFactory.getLogger(CredentialGuardStore.class);

    /** Exact leaf names, never extensions. */
    static final Set<String> CREDENTIAL_FILES =
            Set.of("plsql-config.yaml", "plsql-env-config.yaml");

    private final ArtifactStore delegate;

    public CredentialGuardStore(ArtifactStore delegate) {
        this.delegate = delegate;
    }

    /**
     * The wrapped backend.
     *
     * <p>Read-only, and deliberately <b>not</b> a way to get an unguarded store: every one of the
     * fourteen interface methods still goes through this class, and the only caller of this
     * accessor is housekeeping that needs to know <em>which backend</em> is active (GridFS orphans
     * exist only in {@code mongodb} mode) rather than a path around the guard. Exposed here rather
     * than by re-deriving the backend from properties, because a second construction would be a
     * second connection pool and could disagree about the root prefix.
     */
    public ArtifactStore delegate() {
        return delegate;
    }

    static boolean isCredentialFile(ArtifactKey key) {
        return CREDENTIAL_FILES.contains(key.leaf());
    }

    private static void reject(ArtifactKey key, String op) {
        log.warn("Blocked {} of credential file through the artifact store: {}", op, key);
        throw new SecurityException(
                "Refusing to " + op + " " + key.leaf() + " through the artifact store: it holds "
                + "plaintext database credentials and must stay on local disk.");
    }

    private static ArtifactKey checked(ArtifactKey key, String op) {
        if (isCredentialFile(key)) reject(key, op);
        return key;
    }

    @Override
    public OutputStream write(ArtifactKey key) throws IOException {
        return delegate.write(checked(key, "write"));
    }

    @Override
    public OutputStream write(ArtifactKey key, long contentLength) throws IOException {
        return delegate.write(checked(key, "write"), contentLength);
    }

    @Override
    public InputStream read(ArtifactKey key) throws IOException {
        return delegate.read(checked(key, "read"));
    }

    @Override
    public InputStream read(ArtifactKey key, long offset, long length) throws IOException {
        return delegate.read(checked(key, "read"), offset, length);
    }

    @Override
    public long size(ArtifactKey key) {
        return isCredentialFile(key) ? -1 : delegate.size(key);
    }

    @Override
    public long lastModified(ArtifactKey key) {
        return isCredentialFile(key) ? -1 : delegate.lastModified(key);
    }

    @Override
    public boolean exists(ArtifactKey key) {
        // Reported absent rather than blocked: callers branch on this, and an exception here would
        // turn a hidden file into a crash on paths that merely probe for it.
        return !isCredentialFile(key) && delegate.exists(key);
    }

    @Override
    public boolean delete(ArtifactKey key) {
        // Deleting is not a disclosure, and cleanup paths legitimately need it.
        return delegate.delete(key);
    }

    @Override
    public int deletePrefix(ArtifactKey prefix) throws IOException {
        return delegate.deletePrefix(prefix);
    }

    @Override
    public Stream<ArtifactEntry> list(ArtifactKey prefix, boolean recursive) throws IOException {
        return delegate.list(prefix, recursive).filter(e -> !isCredentialFile(e.key()));
    }

    @Override
    public void copy(ArtifactKey from, ArtifactKey to) throws IOException {
        delegate.copy(checked(from, "copy"), checked(to, "copy"));
    }

    @Override
    public void move(ArtifactKey from, ArtifactKey to) throws IOException {
        delegate.move(checked(from, "move"), checked(to, "move"));
    }

    @Override
    public LocalHandle materialize(ArtifactKey key) throws IOException {
        return delegate.materialize(checked(key, "materialize"));
    }
}
