package com.codeanalyzer.ingest.mongo.schema;

import org.bson.Document;

/**
 * One index to ensure.
 *
 * @param name           explicit, so drift can be detected and the index replaced by name
 * @param keys           the key document, e.g. {@code {runId: 1, schemaName: 1}}
 * @param unique         whether the index enforces uniqueness
 * @param partialFilter  restricts which documents the index covers; null for none
 */
public record IndexSpec(String name, Document keys, boolean unique, Document partialFilter) {

    public static IndexSpec of(String name, Document keys) {
        return new IndexSpec(name, keys, false, null);
    }

    public static IndexSpec unique(String name, Document keys) {
        return new IndexSpec(name, keys, true, null);
    }

    /**
     * A unique index that applies only to documents matching {@code filter}.
     *
     * <p>This is how "only one current run per entry point" is enforced without forbidding the
     * historical runs beside it — the relational equivalent of a filtered unique index, and the
     * only way to express it in MongoDB.
     */
    public static IndexSpec uniquePartial(String name, Document keys, Document filter) {
        return new IndexSpec(name, keys, true, filter);
    }
}
