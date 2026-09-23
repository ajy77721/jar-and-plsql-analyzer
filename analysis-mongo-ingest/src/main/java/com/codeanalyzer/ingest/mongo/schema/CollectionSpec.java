package com.codeanalyzer.ingest.mongo.schema;

import org.bson.Document;

import java.util.List;

/**
 * Everything the database needs to know about one collection: its validator and its indexes.
 *
 * @param name       the {@code am_*} collection name
 * @param validator  a {@code $jsonSchema} document, or null to impose none
 * @param indexes    indexes to ensure, excluding the implicit {@code _id} one
 */
public record CollectionSpec(String name, Document validator, List<IndexSpec> indexes) {}
