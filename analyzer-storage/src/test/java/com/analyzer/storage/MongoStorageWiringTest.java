package com.analyzer.storage;

import com.analyzer.storage.mongo.MongoConnection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The wiring for {@code app.storage.mode=mongodb}: mode parsing, settings validation and URI
 * redaction. No server needed — none of this touches one.
 *
 * <p>PLAN-09 §5's requirement is that a misconfiguration is refused at <b>boot</b>, naming the
 * property, rather than surfacing deep inside an analysis run. These are the assertions for that,
 * and the reason each one exists is that the failure it prevents is otherwise silent: a mode string
 * nobody validated defaults to {@code local}, and a deployment believes it has cut over while every
 * artifact still lands on disk.
 */
class MongoStorageWiringTest {

    // ── mode parsing ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("mongodb and mongo both parse; the existing spellings are untouched")
    void modeParsing() {
        assertEquals(StorageMode.MONGODB, StorageMode.parse("mongodb"));
        assertEquals(StorageMode.MONGODB, StorageMode.parse("MongoDB"));
        assertEquals(StorageMode.MONGODB, StorageMode.parse("  MONGO  "));
        // The regression that matters most: adding a mode must not disturb the two that ship today,
        // legacy aliases included.
        assertEquals(StorageMode.LOCAL, StorageMode.parse(null));
        assertEquals(StorageMode.LOCAL, StorageMode.parse(""));
        assertEquals(StorageMode.LOCAL, StorageMode.parse("local"));
        assertEquals(StorageMode.LOCAL, StorageMode.parse("internal"));
        assertEquals(StorageMode.S3, StorageMode.parse("s3"));
        assertEquals(StorageMode.S3, StorageMode.parse("external"));
    }

    @Test
    @DisplayName("an unknown mode names the valid list, and now includes mongodb in it")
    void unknownModeIsRejectedWithTheValidList() {
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> StorageMode.parse("mongoose"));
        assertTrue(e.getMessage().contains("mongodb"),
                "the error should list mongodb as an option: " + e.getMessage());
    }

    // ── settings validation ──────────────────────────────────────────────────

    @Test
    @DisplayName("mongodb settings without a URI or database are refused, naming the property")
    void mongodbSettingsRequireUriAndDatabase() {
        IllegalArgumentException noUri = assertThrows(IllegalArgumentException.class,
                () -> StorageFactory.Settings.mongodb(null, "analyzer", "data", 0).validate());
        assertTrue(noUri.getMessage().contains("app.storage.mongodb.uri"), noUri.getMessage());

        IllegalArgumentException noDb = assertThrows(IllegalArgumentException.class,
                () -> StorageFactory.Settings.mongodb("mongodb://h:27017", " ", "data", 0).validate());
        assertTrue(noDb.getMessage().contains("app.storage.mongodb.database"), noDb.getMessage());
    }

    @Test
    @DisplayName("an inline threshold above the 8 MiB ceiling is refused at boot")
    void inlineThresholdCeilingIsEnforcedInSettings() {
        int tooBig = com.analyzer.storage.mongo.MongoArtifactStore.MAX_INLINE_THRESHOLD + 1;
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> StorageFactory.Settings.mongodb("mongodb://h:27017", "db", "data", tooBig)
                        .validate());
        assertTrue(e.getMessage().contains("inline-threshold"), e.getMessage());

        // 0 means "default" and must remain valid, or every deployment that omits the property
        // would fail to boot.
        StorageFactory.Settings.mongodb("mongodb://h:27017", "db", "data", 0).validate();
        StorageFactory.Settings.mongodb("mongodb://h:27017", "db", "data",
                com.analyzer.storage.mongo.MongoArtifactStore.MAX_INLINE_THRESHOLD).validate();
    }

    @Test
    @DisplayName("the prefix normalises the same way S3's does, so the two modes produce one key")
    void prefixNormalisationMatchesS3() {
        // The property that makes a mode flip need no key rewriting: the manifest _id IS the S3
        // key string, so if these ever diverged a rollback would silently address nothing.
        assertEquals("data", StorageFactory.Settings.mongodb("u", "d", "/data/", 0).normalisedPrefix());
        assertEquals("data", StorageFactory.Settings.s3("b", "/data/", null, null).normalisedPrefix());
        assertEquals("", StorageFactory.Settings.mongodb("u", "d", null, 0).normalisedPrefix());
    }

    @Test
    @DisplayName("adding mongodb did not change local or s3 validation")
    void existingModesStillValidateAsBefore() {
        assertThrows(IllegalArgumentException.class,
                () -> StorageFactory.Settings.local(null).validate());
        assertThrows(IllegalArgumentException.class,
                () -> StorageFactory.Settings.s3("", "data", null, null).validate());
        StorageFactory.Settings.local(java.nio.file.Path.of("data")).validate();
        StorageFactory.Settings.s3("bucket", "data", null, null).validate();
    }

    // ── credential redaction ─────────────────────────────────────────────────

    @Test
    @DisplayName("an inline password is redacted before the URI reaches a log")
    void uriCredentialsAreRedacted() {
        assertEquals("mongodb://***@host:27017/db",
                MongoConnection.redact("mongodb://alice:s3cr3t@host:27017/db"));
        assertEquals("mongodb+srv://***@cluster.example.net/analyzer",
                MongoConnection.redact("mongodb+srv://u:p@cluster.example.net/analyzer"));
        // A replica-set URI with several hosts and credentials.
        assertEquals("mongodb://***@h1:27017,h2:27017/db?replicaSet=rs0",
                MongoConnection.redact("mongodb://u:p@h1:27017,h2:27017/db?replicaSet=rs0"));
    }

    @Test
    @DisplayName("a credential-free URI is logged as-is, including '@' later in the string")
    void redactionDoesNotMangleCleanUris() {
        assertEquals("mongodb://localhost:27017", MongoConnection.redact("mongodb://localhost:27017"));
        assertEquals("mongodb://localhost:27017/analyzer",
                MongoConnection.redact("mongodb://localhost:27017/analyzer"));
        // '@' is legal in a database name and in a query parameter value, and it appears AFTER the
        // host section. A lastIndexOf('@') over the whole string would eat the host here.
        assertEquals("mongodb://localhost:27017/db?appName=a@b",
                MongoConnection.redact("mongodb://localhost:27017/db?appName=a@b"));
        assertEquals("", MongoConnection.redact(null));
    }

    // ── the unconfigured state ───────────────────────────────────────────────

    @Test
    @DisplayName("unconfigured is a usable state, not an error, and closing it is a no-op")
    void unconfiguredConnectionIsSafe() {
        // Every deployment today is local or s3 with the scan repository, so this is the normal
        // path. It must not throw on construction, on close, or on being injected.
        MongoConnection none = MongoConnection.unconfigured();
        assertFalse(none.configured());
        assertEquals("(unconfigured)", none.redactedUri());
        none.close();
        // Only asking for the database fails, and it says which properties to set.
        IllegalStateException e = assertThrows(IllegalStateException.class, none::database);
        assertTrue(e.getMessage().contains("app.storage.mongodb.uri"), e.getMessage());
    }

    @Test
    @DisplayName("opening without a URI or database is rejected before any connection attempt")
    void openRequiresUriAndDatabase() {
        assertThrows(IllegalArgumentException.class, () -> MongoConnection.open(null, "db"));
        assertThrows(IllegalArgumentException.class, () -> MongoConnection.open("", "db"));
        assertThrows(IllegalArgumentException.class,
                () -> MongoConnection.open("mongodb://localhost:27017", " "));
    }
}
