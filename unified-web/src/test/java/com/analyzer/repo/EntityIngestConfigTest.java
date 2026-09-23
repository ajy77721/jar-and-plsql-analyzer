package com.analyzer.repo;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Mode 4 (entity-ingest) — {@link EntityIngestConfig}'s fail-fast rule: enabling entity-ingest
 * without a resolvable Mongo connection must fail at boot (PLAN-09 §8 R1's own standing rule),
 * never silently produce a disabled/no-op bean.
 */
class EntityIngestConfigTest {

    private final EntityIngestConfig cfg = new EntityIngestConfig();

    @Test
    void disabledByDefaultIsANoOpNotAnError() {
        var settings = cfg.entityIngestSettings(false, "local", "", "", "", "");
        assertFalse(settings.enabled());
        assertNull(settings.uri());
        assertNull(settings.database());
    }

    @Test
    void mongodbBaseModeReusesItsOwnConnectionWhenNoSeparateOneIsGiven() {
        var settings = cfg.entityIngestSettings(true, "mongodb",
                "mongodb://localhost:27017", "analyzer", "", "");
        assertTrue(settings.enabled());
        assertEquals("mongodb://localhost:27017", settings.uri());
        assertEquals("analyzer", settings.database());
    }

    @Test
    void anExplicitEntityIngestConnectionAlwaysWinsEvenInMongodbMode() {
        var settings = cfg.entityIngestSettings(true, "mongodb",
                "mongodb://base:27017", "base-db",
                "mongodb://entities:27017", "entities-db");
        assertEquals("mongodb://entities:27017", settings.uri());
        assertEquals("entities-db", settings.database());
    }

    /**
     * Calibrated by removing the {@code isBlank()} guard (making the check unconditionally pass):
     * confirmed this test failed (no exception thrown), restored.
     */
    @Test
    void localBaseModeWithNoSeparateConnectionFailsFastAtBoot() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> cfg.entityIngestSettings(true, "local", "", "", "", ""));
        assertTrue(e.getMessage().contains("app.storage.entity-ingest.enabled=true"),
                "the error must name the property to fix, not just say something is wrong");
    }

    @Test
    void s3BaseModeWithNoSeparateConnectionAlsoFailsFast() {
        assertThrows(IllegalStateException.class,
                () -> cfg.entityIngestSettings(true, "s3", "", "", "", ""));
    }
}
