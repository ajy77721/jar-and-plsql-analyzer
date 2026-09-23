package com.analyzer.repo;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Mode 4 (entity-ingest): the {@code am_*} collections, populated in-process after each PL/SQL
 * analyze completes, on top of whichever base {@link com.analyzer.storage.StorageMode} wrote the
 * raw artifacts.
 *
 * <pre>
 * app.storage.entity-ingest.enabled=false        # off by default — no behaviour change otherwise
 * app.storage.entity-ingest.mongo.uri=            # falls back to app.storage.mongodb.uri when
 * app.storage.entity-ingest.mongo.database=       # app.storage.mode=mongodb; else required
 * </pre>
 *
 * <p><b>A new, orthogonal toggle — not a fourth {@code app.storage.mode} value.</b> {@code am_*}
 * entities are Mongo-only regardless of where the base artifacts live (local disk, S3, or a
 * different Mongo document shape entirely), so "which mode wrote the artifacts" and "should
 * entities also be derived from them" are two independent questions, the same reasoning
 * {@link com.analyzer.storage.repo.RepositoryMode} already established for query-serving.
 *
 * <p><b>Fails fast at boot, not silently on the first analysis.</b> Enabling this with base mode
 * {@code local}/{@code s3} and no separate URI configured is a real misconfiguration — there is
 * nowhere for entities to go — and PLAN-09 §8 R1's own standing rule is that a storage
 * misconfiguration surfaces at startup with the property to fix, not as a background failure log
 * nobody is watching after the first real analysis silently produces zero entity rows.
 */
@Configuration
public class EntityIngestConfig {

    private static final Logger log = LoggerFactory.getLogger(EntityIngestConfig.class);

    @Bean
    public EntityIngestSettings entityIngestSettings(
            @Value("${app.storage.entity-ingest.enabled:false}") boolean enabled,
            @Value("${app.storage.mode:local}") String storageModeProp,
            @Value("${app.storage.mongodb.uri:}") String baseMongoUri,
            @Value("${app.storage.mongodb.database:}") String baseMongoDatabase,
            @Value("${app.storage.entity-ingest.mongo.uri:}") String entityUri,
            @Value("${app.storage.entity-ingest.mongo.database:}") String entityDatabase) {
        if (!enabled) {
            return new EntityIngestSettings(false, null, null);
        }
        boolean baseIsMongo = com.analyzer.storage.StorageMode.parse(storageModeProp)
                == com.analyzer.storage.StorageMode.MONGODB;
        String uri = !entityUri.isBlank() ? entityUri : (baseIsMongo ? baseMongoUri : "");
        String database = !entityDatabase.isBlank() ? entityDatabase
                : (baseIsMongo ? baseMongoDatabase : "");
        if (uri.isBlank() || database.isBlank()) {
            throw new IllegalStateException(
                    "app.storage.entity-ingest.enabled=true but no Mongo connection is configured for "
                    + "it. app.storage.mode=" + storageModeProp + " does not supply one on its own "
                    + "unless it is 'mongodb' — set app.storage.entity-ingest.mongo.uri and "
                    + ".database explicitly, or switch app.storage.mode to mongodb to reuse its own "
                    + "connection.");
        }
        log.info("[entity-ingest] enabled, writing am_* collections to {} (database {})",
                redact(uri), database);
        return new EntityIngestSettings(true, uri, database);
    }

    @Bean(destroyMethod = "close")
    public EntityIngestConnection entityIngestConnection(EntityIngestSettings settings) {
        if (!settings.enabled()) {
            return EntityIngestConnection.unconfigured();
        }
        // Pinged implicitly by the first real write; MongoClients.create itself does not connect
        // eagerly. A dedicated ping-at-boot (matching MongoConnection.open's own PLAN-09 §5/§8 R1
        // reasoning) is deliberately not added here: entity-ingest is best-effort by design (see
        // EntityIngestTrigger's own doc comment) — an unreachable server should not fail the whole
        // application context for a capability that already degrades to "log and continue" on
        // every subsequent call. Config *shape* errors (missing URI) still fail fast, above.
        var client = MongoClients.create(settings.uri());
        MongoDatabase db = client.getDatabase(settings.database());
        installSchemaBestEffort(db);
        return new EntityIngestConnection(client, db);
    }

    /**
     * Ensures every {@code am_*} collection, validator, and index exists before the first real
     * write — {@link com.codeanalyzer.ingest.mongo.schema.SchemaInstaller#install()} is idempotent
     * and documents itself as "safe to call on every startup," but nothing actually called it at
     * boot: it only ever ran inside {@code MongoIngestService}/{@code BulkWriter}, lazily, one
     * collection at a time, on whichever collection a given analysis happened to touch first. A
     * fresh database (or one missing just-added collections after an upgrade) would silently create
     * schema piecemeal during the first few real ingests instead of confirming it up front.
     *
     * <p>Best-effort, matching this class's own "entity-ingest degrades to log-and-continue"
     * philosophy above (see {@code entityIngestConnection}'s doc comment) — an unreachable Mongo
     * here must not fail the whole application context; the first real write will simply hit the
     * same lazy per-collection path as before and surface the same error there.
     */
    private void installSchemaBestEffort(MongoDatabase db) {
        try {
            var installer = new com.codeanalyzer.ingest.mongo.schema.SchemaInstaller(db);
            java.util.List<String> before = new java.util.ArrayList<>();
            db.listCollectionNames().into(before);
            java.util.Set<String> beforeSet = new java.util.HashSet<>(before);
            installer.install();
            java.util.List<String> owned = com.codeanalyzer.ingest.mongo.schema.SchemaInstaller.ownedCollections();
            long alreadyPresent = owned.stream().filter(beforeSet::contains).count();
            long created = owned.size() - alreadyPresent;
            log.info("[entity-ingest] schema check complete: {} collections owned, {} already present, "
                            + "{} created/verified just now (indexes ensured on all)",
                    owned.size(), alreadyPresent, created);
        } catch (Exception e) {
            log.warn("[entity-ingest] schema pre-install failed ({}); collections/indexes will be "
                    + "created lazily on first write instead, per-collection", e.getMessage());
        }
    }

    private static String redact(String uri) {
        int at = uri.indexOf('@');
        return at < 0 ? uri : uri.substring(0, uri.indexOf("://") + 3) + "***" + uri.substring(at);
    }

    /** Parsed, validated configuration. A record so "not configured" and "no database" cannot both
     *  be represented as null in two different ways across call sites. */
    public record EntityIngestSettings(boolean enabled, String uri, String database) {
    }

    /** Owns the connection lifecycle; {@link #database()} is null when {@link #enabled()} is false,
     *  the same "always a bean, sometimes unconfigured" shape {@code MongoConnection} already uses. */
    public static final class EntityIngestConnection implements AutoCloseable {
        private static final EntityIngestConnection UNCONFIGURED =
                new EntityIngestConnection(null, null);

        private final com.mongodb.client.MongoClient client;
        private final MongoDatabase database;

        EntityIngestConnection(com.mongodb.client.MongoClient client, MongoDatabase database) {
            this.client = client;
            this.database = database;
        }

        static EntityIngestConnection unconfigured() {
            return UNCONFIGURED;
        }

        public boolean enabled() {
            return database != null;
        }

        public MongoDatabase database() {
            return database;
        }

        @Override
        public void close() {
            if (client != null) client.close();
        }
    }
}
