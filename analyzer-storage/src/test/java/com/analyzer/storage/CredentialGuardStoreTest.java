package com.analyzer.storage;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The credential files must not be reachable through the store in any mode.
 *
 * <p>These names hold plaintext production database passwords. The store spans the whole data
 * tree, so without this guard a remote backend would upload them; that is the failure this test
 * exists to prevent from ever regressing.
 */
class CredentialGuardStoreTest {

    @TempDir
    Path tmp;

    private ArtifactStore store() {
        return StorageFactory.create(StorageFactory.Settings.local(tmp));
    }

    @Test
    @DisplayName("the factory always wraps the backend in the guard")
    void factoryAlwaysGuards() {
        assertInstanceOf(CredentialGuardStore.class, store(),
                "a backend reachable unwrapped would bypass the guard entirely");
    }

    @Test
    @DisplayName("writing a credential file through the store is refused")
    void writeIsRefused() {
        for (String name : CredentialGuardStore.CREDENTIAL_FILES) {
            var key = ArtifactKey.of("config", name);
            assertThrows(SecurityException.class, () -> store().write(key), name);
            assertThrows(SecurityException.class, () -> store().write(key, 10), name);
        }
    }

    @Test
    @DisplayName("the guard matches the exact leaf name, anywhere in the tree")
    void guardIsNotScopedToTheConfigDirectory() {
        // AnalysisService generates this file inside a per-analysis output folder, so a guard that
        // only covered config/ would miss the copy that actually travels with the analysis.
        assertThrows(SecurityException.class,
                () -> store().write(ArtifactKey.of("plsql-parse", "RUN1", "plsql-env-config.yaml")));
    }

    @Test
    @DisplayName("reads and listings are filtered too, not just writes")
    void readAndListAreFiltered() throws Exception {
        // Plant the file directly on disk, behind the store's back, as a real deployment would
        // have it: written locally by buildEnvConfigFile, never through the store.
        Path onDisk = tmp.resolve("config").resolve("plsql-config.yaml");
        Files.createDirectories(onDisk.getParent());
        Files.writeString(onDisk, "password: hunter2");

        var key = ArtifactKey.of("config", "plsql-config.yaml");
        assertThrows(SecurityException.class, () -> store().read(key),
            "blocking the write but serving the read would move the disclosure, not close it");
        assertFalse(store().exists(key), "probing must report absent rather than throw");
        try (var s = store().list(ArtifactKey.of("config"), true)) {
            assertTrue(s.noneMatch(e -> e.key().leaf().equals("plsql-config.yaml")),
                    "a listing that names the file hands a caller the key to ask for");
        }
    }

    @Test
    @DisplayName("similarly-named and same-extension files still work")
    void unrelatedYamlIsUnaffected() throws Exception {
        for (String name : new String[]{"plsql-export-config.json", "domain-config.yaml",
                                        "settings.yaml", "plsql-config.yaml.bak"}) {
            var key = ArtifactKey.of("config", name);
            try (OutputStream out = store().write(key)) {
                out.write("ok".getBytes());
            }
            assertTrue(store().exists(key), name + " must not be caught by the guard");
        }
    }

    @Test
    @DisplayName("deleting a credential file is still allowed")
    void deleteIsAllowed() throws Exception {
        // Cleanup paths legitimately remove these, and removing one is not a disclosure.
        Path onDisk = tmp.resolve("config").resolve("plsql-env-config.yaml");
        Files.createDirectories(onDisk.getParent());
        Files.writeString(onDisk, "password: hunter2");
        assertTrue(store().delete(ArtifactKey.of("config", "plsql-env-config.yaml")));
        assertFalse(Files.exists(onDisk));
    }
}
