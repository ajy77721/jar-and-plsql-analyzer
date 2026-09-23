package com.analyzer.storage;

/**
 * One entry from {@link ArtifactStore#list}.
 *
 * @param key          location of the artifact
 * @param size         bytes; 0 for a directory
 * @param lastModified millis since epoch
 * @param directory    true for a local directory or an S3 common prefix. Object stores have no
 *                     real directories, so this is presentational only — never branch storage
 *                     behaviour on it.
 */
public record ArtifactEntry(ArtifactKey key, long size, long lastModified, boolean directory) {}
