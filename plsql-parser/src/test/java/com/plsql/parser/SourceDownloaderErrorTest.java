package com.plsql.parser;

import com.plsql.parser.flow.SourceDownloader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Error handling tests for SourceDownloader.
 * Tests local cache behavior, wrapped detection, and null/missing scenarios
 * without requiring a live database connection.
 */
@DisplayName("SourceDownloader Error Handling")
public class SourceDownloaderErrorTest {

    private SourceDownloader downloader;

    @BeforeEach
    void setup() {
        // connManager = null => all DB calls return null gracefully
        downloader = new SourceDownloader(null);
    }

    // ── downloadSource with null connManager ──

    @Test
    @DisplayName("downloadSource with null connManager returns null")
    void testDownloadSourceNullConnManager() {
        String result = downloader.downloadSource("SOME_PKG", "PACKAGE BODY");
        assertNull(result, "Should return null when no DB connection is available");
    }

    @Test
    @DisplayName("downloadPackageBody returns null without DB")
    void testDownloadPackageBodyNoDb() {
        assertNull(downloader.downloadPackageBody("MY_PKG"));
    }

    @Test
    @DisplayName("downloadAny returns null for all types without DB")
    void testDownloadAnyNoDb() {
        assertNull(downloader.downloadAny("NONEXISTENT"));
    }

    // ── Local cache with non-existent directory ──

    @Test
    @DisplayName("checkLocalCache with non-existent dir returns null")
    void testLocalCacheNonExistentDir() {
        downloader.setLocalCacheDir(Path.of("C:/nonexistent_dir_12345"));
        String result = downloader.downloadSource("MY_PKG", "PACKAGE BODY");
        assertNull(result);
    }

    // ── Local cache with file matching SCHEMA.NAME.ext pattern ──

    @Test
    @DisplayName("checkLocalCache finds file with SCHEMA.NAME.pkb pattern and extracts schema")
    void testLocalCacheSchemaNamePattern(@TempDir Path tempDir) throws IOException {
        // Create a file matching pattern: MYSCHEMA.MY_PKG.pkb
        String content = "CREATE OR REPLACE PACKAGE BODY MY_PKG AS\n"
                + "  PROCEDURE P IS BEGIN NULL; END P;\n"
                + "END MY_PKG;\n/\n";
        Files.writeString(tempDir.resolve("MYSCHEMA.MY_PKG.pkb"), content);

        downloader.setLocalCacheDir(tempDir);
        String result = downloader.downloadSource("MY_PKG", "PACKAGE BODY");
        assertNotNull(result, "Should find the file in local cache");
        assertTrue(result.contains("MY_PKG"));

        // Verify schema was extracted from filename
        String owner = downloader.getOwner("MY_PKG");
        assertEquals("MYSCHEMA", owner, "Owner should be extracted from SCHEMA.NAME.ext filename");
    }

    @Test
    @DisplayName("checkLocalCache finds NAME.pkb without schema prefix")
    void testLocalCacheNameOnly(@TempDir Path tempDir) throws IOException {
        String content = "CREATE OR REPLACE PACKAGE BODY TEST_PKG AS\n"
                + "  PROCEDURE P IS BEGIN NULL; END P;\n"
                + "END TEST_PKG;\n/\n";
        Files.writeString(tempDir.resolve("TEST_PKG.pkb"), content);

        downloader.setLocalCacheDir(tempDir);
        String result = downloader.downloadSource("TEST_PKG", "PACKAGE BODY");
        assertNotNull(result, "Should find NAME.ext file");
    }

    @Test
    @DisplayName("checkLocalCache maps PROCEDURE type to .prc extension")
    void testLocalCacheProcedureExtension(@TempDir Path tempDir) throws IOException {
        Files.writeString(tempDir.resolve("MY_PROC.prc"),
                "CREATE OR REPLACE PROCEDURE MY_PROC IS BEGIN NULL; END;\n/\n");

        downloader.setLocalCacheDir(tempDir);
        String result = downloader.downloadSource("MY_PROC", "PROCEDURE");
        assertNotNull(result, "Should find .prc file for PROCEDURE type");
    }

    @Test
    @DisplayName("checkLocalCache maps FUNCTION type to .fnc extension")
    void testLocalCacheFunctionExtension(@TempDir Path tempDir) throws IOException {
        Files.writeString(tempDir.resolve("MY_FN.fnc"),
                "CREATE OR REPLACE FUNCTION MY_FN RETURN NUMBER IS BEGIN RETURN 1; END;\n/\n");

        downloader.setLocalCacheDir(tempDir);
        String result = downloader.downloadSource("MY_FN", "FUNCTION");
        assertNotNull(result, "Should find .fnc file for FUNCTION type");
    }

    @Test
    @DisplayName("checkLocalCache skips empty files")
    void testLocalCacheSkipsEmptyFiles(@TempDir Path tempDir) throws IOException {
        Files.writeString(tempDir.resolve("EMPTY_PKG.pkb"), "   \n  \n  ");

        downloader.setLocalCacheDir(tempDir);
        String result = downloader.downloadSource("EMPTY_PKG", "PACKAGE BODY");
        assertNull(result, "Should skip blank files");
    }

    // ── getCachedSource for non-existent object ──

    @Test
    @DisplayName("getCachedSource for non-existent object returns null")
    void testGetCachedSourceNonExistent() {
        assertNull(downloader.getCachedSource("NEVER_DOWNLOADED"));
    }

    @Test
    @DisplayName("getCachedType for non-existent object returns null")
    void testGetCachedTypeNonExistent() {
        assertNull(downloader.getCachedType("NEVER_DOWNLOADED"));
    }

    // ── isCached / isWrapped ──

    @Test
    @DisplayName("isCached returns false for non-downloaded object")
    void testIsCachedFalse() {
        assertFalse(downloader.isCached("SOME_OBJ", "PACKAGE BODY"));
    }

    @Test
    @DisplayName("isWrapped returns false for non-tracked object")
    void testIsWrappedFalse() {
        assertFalse(downloader.isWrapped("SOME_OBJ"));
    }

    // ── getOwner ──

    @Test
    @DisplayName("getOwner returns null for unknown object")
    void testGetOwnerNull() {
        assertNull(downloader.getOwner("UNKNOWN_OBJ"));
    }

    // ── clearCache ──

    @Test
    @DisplayName("clearCache removes all cached entries")
    void testClearCache(@TempDir Path tempDir) throws IOException {
        Files.writeString(tempDir.resolve("PKG.pkb"),
                "CREATE OR REPLACE PACKAGE BODY PKG AS PROCEDURE P IS BEGIN NULL; END P; END PKG;\n/\n");
        downloader.setLocalCacheDir(tempDir);

        // Load into cache
        String first = downloader.downloadSource("PKG", "PACKAGE BODY");
        assertNotNull(first);
        assertTrue(downloader.isCached("PKG", "PACKAGE BODY"));

        // Clear and verify
        downloader.clearCache();
        assertFalse(downloader.isCached("PKG", "PACKAGE BODY"));
        assertNull(downloader.getCachedSource("PKG"));
    }

    // ── Case insensitivity ──

    @Test
    @DisplayName("downloadSource is case-insensitive for object names")
    void testCaseInsensitivity(@TempDir Path tempDir) throws IOException {
        Files.writeString(tempDir.resolve("MY_PKG.pkb"),
                "CREATE OR REPLACE PACKAGE BODY MY_PKG AS PROCEDURE P IS BEGIN NULL; END P; END MY_PKG;\n/\n");
        downloader.setLocalCacheDir(tempDir);

        String result = downloader.downloadSource("my_pkg", "package body");
        assertNotNull(result, "Should match regardless of case");
    }
}
