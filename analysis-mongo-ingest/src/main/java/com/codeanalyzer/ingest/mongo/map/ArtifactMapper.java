package com.codeanalyzer.ingest.mongo.map;

import java.io.IOException;
import java.io.InputStream;

/**
 * Maps one artifact kind to documents.
 *
 * <p>One class per artifact, each with its own {@link #artifact()} constant — the convention
 * {@code MongoPlsqlEntityIndex} already states for this codebase, rather than a growing switch
 * inside a single mapper.
 *
 * <p>{@code map} does <b>not</b> close the stream, matching the existing {@code PlsqlEntityMapper}
 * contract: the caller owns the stream because only the caller knows whether it is reading one
 * artifact or many from the same source.
 */
public interface ArtifactMapper {

    /** The artifact path this mapper consumes, e.g. {@code api/tables/index.json}. */
    String artifact();

    void map(IngestContext ctx, InputStream in, DocSink sink) throws IOException;
}
