package com.codeanalyzer.ingest.mongo.schema;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Creates collections with their validators and ensures their indexes. Idempotent.
 *
 * <h2>Drift tolerance, and why it is not optional</h2>
 *
 * <p>An index whose definition changes cannot simply be re-created: MongoDB rejects it with
 * {@code IndexOptionsConflict} (85) or {@code IndexKeySpecsConflict} (86) because an index of that
 * name already exists with different options. Without handling that, the first schema change makes
 * <em>every subsequent run</em> fail, and the failure looks unrelated to the change that caused it.
 *
 * <p>The recovery — drop by name, re-create — follows the precedent already set in
 * {@code MongoPlsqlEntityIndex.java:231-243} rather than inventing a second convention.
 */
public final class SchemaInstaller {

    private static final Logger log = LoggerFactory.getLogger(SchemaInstaller.class);

    private static final int INDEX_OPTIONS_CONFLICT = 85;
    private static final int INDEX_KEY_SPECS_CONFLICT = 86;
    private static final int NAMESPACE_EXISTS = 48;

    private final MongoDatabase db;

    public SchemaInstaller(MongoDatabase db) {
        this.db = db;
    }

    /** Installs every collection in the catalog. Safe to call on every startup. */
    public void install() {
        // Before anything is created or modified: an unsupported build that discovers the mismatch
        // after installing its validators has already done the damage the check exists to prevent.
        SchemaMeta.checkAndStamp(db);
        for (CollectionSpec spec : Catalog.all()) install(spec);
    }

    void install(CollectionSpec spec) {
        Set<String> existing = new HashSet<>();
        db.listCollectionNames().into(existing);

        if (!existing.contains(spec.name())) {
            Document options = new Document("validationLevel", "strict")
                    .append("validationAction", "error");
            if (spec.validator() != null) options.append("validator", spec.validator());
            try {
                db.runCommand(new Document("create", spec.name()).append("validator", spec.validator())
                        .append("validationLevel", "strict").append("validationAction", "error"));
                log.info("created collection {}", spec.name());
            } catch (MongoCommandException e) {
                // A concurrent installer won the race. That is success, not failure.
                if (e.getErrorCode() != NAMESPACE_EXISTS) throw e;
            }
        } else if (spec.validator() != null) {
            // Bring an existing collection up to the current validator.
            db.runCommand(new Document("collMod", spec.name())
                    .append("validator", spec.validator())
                    .append("validationLevel", "strict")
                    .append("validationAction", "error"));
        }
        ensureIndexes(spec);
    }

    private void ensureIndexes(CollectionSpec spec) {
        var coll = db.getCollection(spec.name());
        for (IndexSpec ix : spec.indexes()) {
            IndexOptions opts = new IndexOptions().name(ix.name()).unique(ix.unique());
            if (ix.partialFilter() != null) opts.partialFilterExpression(ix.partialFilter());
            try {
                coll.createIndex(ix.keys(), opts);
            } catch (MongoCommandException e) {
                if (e.getErrorCode() == INDEX_OPTIONS_CONFLICT
                        || e.getErrorCode() == INDEX_KEY_SPECS_CONFLICT) {
                    log.info("index {} on {} drifted; dropping and recreating", ix.name(), spec.name());
                    coll.dropIndex(ix.name());
                    coll.createIndex(ix.keys(), opts);
                } else {
                    throw e;
                }
            }
        }
    }

    /** For tests and for {@code drop(runId)} tooling. */
    public void dropAll() {
        for (CollectionSpec spec : Catalog.all()) {
            db.getCollection(spec.name()).drop();
        }
    }

    /** The collection names this module owns, for diagnostics. */
    public static List<String> ownedCollections() {
        return Catalog.all().stream().map(CollectionSpec::name).toList();
    }
}
