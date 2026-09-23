package com.analyzer.storage;

import java.nio.file.Path;

/**
 * A real local file for an artifact, from {@link ArtifactStore#materialize}.
 *
 * <p>Always use with try-with-resources. {@link #close()} deletes the temp copy when the artifact
 * came from a remote store, and does nothing when it was already a local file — so the caller
 * never needs to know which happened.
 */
public interface LocalHandle extends AutoCloseable {

    /** The local path. Valid until {@link #close()}. */
    Path path();

    /** True when this handle owns a temp copy that close() will delete. */
    boolean temporary();

    @Override
    void close();   // narrowed: must not throw
}
