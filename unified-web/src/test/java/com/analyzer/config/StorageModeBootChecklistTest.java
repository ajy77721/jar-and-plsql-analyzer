package com.analyzer.config;

import com.analyzer.config.StorageModeBootChecklist.Facts;
import com.analyzer.config.StorageModeBootChecklist.Report;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The startup requirements block: one line per requirement, REQUIRED misses fail the boot,
 * RECOMMENDED misses warn, and the fallbacks that make {@code app.storage.mode=X} a single switch
 * (the parser-mode fallback, the shared Mongo connection, the entity-ingest toggle) are visible.
 */
class StorageModeBootChecklistTest {

    private static Facts facts(String sharedMode, String parserMode, String jarMode, String bucket,
                               String region, String ssoStartUrl, String mongoUri,
                               String mongoDatabase, boolean entityEnabled, String entityUri,
                               String entityDb, boolean sharedConfigured, boolean authRequired) {
        return new Facts(sharedMode, parserMode, jarMode, "data", bucket, "data", region, "",
                region, ssoStartUrl, mongoUri, mongoDatabase, "data", 0, 0, "", false,
                true, 2048, 64, entityEnabled, entityUri, entityDb, sharedConfigured,
                authRequired, "test");
    }

    @Test
    @DisplayName("local mode has no required properties and reports auth off")
    void localMode() {
        Report r = StorageModeBootChecklist.evaluate(
                facts("local", "local", "local", "", "", "", "", "", false, "", "", false, false));
        assertFalse(r.criticalMissing());
        assertFalse(r.hasRecommendedMiss());
        String block = r.render();
        assertTrue(block.contains("mode check"), block);
        assertTrue(block.contains("auth required=false"), block);
        assertTrue(block.contains("OK — all requirements present"), block);
    }

    @Test
    @DisplayName("s3 mode without bucket/region/start-url marks them MISSING and fails on the required one")
    void s3Missing() {
        Report r = StorageModeBootChecklist.evaluate(
                facts("s3", "s3", "s3", "", "", "", "", "", false, "", "", false, true));
        assertTrue(r.criticalMissing(), "bucket is REQUIRED");
        assertTrue(r.hasRecommendedMiss(), "region and start-url are RECOMMENDED");
        String block = r.render();
        assertTrue(block.contains("app.storage.s3.bucket=(unset) MISSING"), block);
        assertTrue(block.contains("app.storage.sso.start-url=(unset) MISSING"), block);
        assertTrue(block.contains("MISSING REQUIRED PROPERTY"), block);
    }

    @Test
    @DisplayName("a fully-configured s3 mode shows every requirement present")
    void s3Complete() {
        Report r = StorageModeBootChecklist.evaluate(
                facts("s3", "s3", "s3", "my-bucket", "ap-south-1", "https://d-x.awsapps.com/start", "", "",
                        false, "", "", false, true));
        assertFalse(r.criticalMissing());
        assertFalse(r.hasRecommendedMiss());
        String block = r.render();
        assertTrue(block.contains("app.storage.s3.bucket=my-bucket ✓"), block);
        assertTrue(block.contains("app.storage.sso.start-url=https://d-x.awsapps.com/start ✓"), block);
        assertTrue(block.contains("app.storage.s3.write-behind=false"), block);
    }

    @Test
    @DisplayName("mongodb mode requires uri and database")
    void mongoMissing() {
        Report r = StorageModeBootChecklist.evaluate(
                facts("mongodb", "mongodb", "mongodb", "", "", "", "", "", false, "", "", false, false));
        assertTrue(r.criticalMissing());
        String block = r.render();
        assertTrue(block.contains("app.storage.mongodb.uri=(unset) MISSING"), block);
        assertTrue(block.contains("app.storage.mongodb.database=(unset) MISSING"), block);
    }

    @Test
    @DisplayName("hybrid mode requires BOTH the bucket and the Mongo connection")
    void hybridRequiresBoth() {
        Report r = StorageModeBootChecklist.evaluate(
                facts("hybrid", "hybrid", "hybrid", "", "", "", "", "", false, "", "", false, true));
        assertTrue(r.criticalMissing());
        String block = r.render();
        assertTrue(block.contains("app.storage.s3.bucket=(unset) MISSING"), block);
        assertTrue(block.contains("app.storage.mongodb.uri=(unset) MISSING"), block);
        assertTrue(block.contains("app.storage.hybrid.big-threshold"), block);
    }

    @Test
    @DisplayName("entity-ingest is resolvable through a configured shared Mongo connection")
    void entityIngestResolvedBySharedConnection() {
        Report resolved = StorageModeBootChecklist.evaluate(
                facts("s3", "s3", "s3", "b", "r", "u", "mongodb://localhost:27017", "analyzer",
                        true, "", "", true, true));
        assertFalse(resolved.criticalMissing(), resolved.render());
        assertTrue(resolved.render().contains("app.storage.entity-ingest.mongo.uri/database (resolved)"),
                resolved.render());

        Report unresolved = StorageModeBootChecklist.evaluate(
                facts("s3", "s3", "s3", "b", "r", "u", "", "", true, "", "", false, true));
        assertTrue(unresolved.criticalMissing(), "entity-ingest with nowhere to write must fail");
    }
}
