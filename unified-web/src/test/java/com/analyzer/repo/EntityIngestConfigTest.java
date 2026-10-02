package com.analyzer.repo;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Mode 4 (entity-ingest) — {@link EntityIngestConfig}'s resolution and fail-fast rule: the
 * fallback follows the PARSER feature's mode (not the shared one), a configured shared Mongo
 * connection is reused instead of opening a second pool, and enabling the feature with no resolvable
 * connection anywhere must fail at boot (PLAN-09 §8 R1's own standing rule), never silently produce
 * a disabled/no-op bean.
 */
class EntityIngestConfigTest {

    @Test
    void disabledByDefaultIsANoOpNotAnError() {
        var settings = EntityIngestConfig.resolve(false, "local", false, "", "", "", "");
        assertFalse(settings.enabled());
        assertNull(settings.uri());
        assertNull(settings.database());
        assertFalse(settings.useShared());
    }

    @Test
    void anExplicitEntityIngestConnectionAlwaysWins() {
        var settings = EntityIngestConfig.resolve(true, "s3", false, "",
                "", "mongodb://entities:27017", "entities-db");
        assertTrue(settings.enabled());
        assertEquals("mongodb://entities:27017", settings.uri());
        assertEquals("entities-db", settings.database());
        assertFalse(settings.useShared());
    }

    @Test
    void aConfiguredSharedConnectionIsReusedRatherThanOpeningASecondPool() {
        var settings = EntityIngestConfig.resolve(true, "mongodb", true,
                "mongodb://localhost:27017", "analyzer", "", "");
        assertTrue(settings.enabled());
        assertTrue(settings.useShared(), "the shared MongoConnection bean must be reused");
        assertNull(settings.uri());
        assertNull(settings.database());
    }

    @Test
    void repositoryMongoWithS3ParserAlsoReusesTheSharedConnection() {
        // mode=s3, repository=mongo: the parser is not Mongo-backed, but the shared bean IS
        // configured for the repository — entity-ingest must accept it rather than demand a URI.
        var settings = EntityIngestConfig.resolve(true, "s3", true,
                "mongodb://localhost:27017", "analyzer", "", "");
        assertTrue(settings.enabled());
        assertTrue(settings.useShared());
    }

    @Test
    void parserMongoModeFallsBackToTheBaseConnectionWithoutASharedBean() {
        // The per-feature fallback: app.storage.mode could be s3 while the parser is mongodb.
        var settings = EntityIngestConfig.resolve(true, "mongodb", false,
                "mongodb://localhost:27017", "analyzer", "", "");
        assertTrue(settings.enabled());
        assertEquals("mongodb://localhost:27017", settings.uri());
        assertEquals("analyzer", settings.database());
        assertFalse(settings.useShared());
    }

    @Test
    void parserHybridModeAlsoFallsBackToTheBaseConnection() {
        var settings = EntityIngestConfig.resolve(true, "hybrid", false,
                "mongodb://localhost:27017", "analyzer", "", "");
        assertTrue(settings.enabled());
        assertEquals("analyzer", settings.database());
    }

    /**
     * Calibrated by removing the shared/parser fallbacks (making the check unconditionally pass):
     * confirmed this test failed (no exception thrown), restored.
     */
    @Test
    void localParserModeWithNoConnectionAnywhereFailsFastAtBoot() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> EntityIngestConfig.resolve(true, "local", false, "", "", "", ""));
        assertTrue(e.getMessage().contains("app.storage.entity-ingest.enabled=true"),
                "the error must name the property to fix, not just say something is wrong");
        assertTrue(e.getMessage().contains("app.storage.parser.mode"),
                "the error must name the parser mode it actually consulted");
    }

    @Test
    void s3ParserModeWithNoConnectionAnywhereAlsoFailsFast() {
        assertThrows(IllegalStateException.class,
                () -> EntityIngestConfig.resolve(true, "s3", false, "", "", "", ""));
    }
}
