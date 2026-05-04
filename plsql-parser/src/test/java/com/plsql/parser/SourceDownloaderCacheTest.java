package com.plsql.parser;

import com.plsql.parser.flow.SourceDownloader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class SourceDownloaderCacheTest {

    @TempDir
    Path tempDir;

    private SourceDownloader downloader;

    @BeforeEach
    void setup() {
        downloader = new SourceDownloader(null);
        downloader.setLocalCacheDir(tempDir);
    }

    @Test
    @DisplayName("Local cache: exact match for package body")
    void testExactMatchPackageBody() throws Exception {
        Files.writeString(tempDir.resolve("CUSTOMER.PG_BPGE_WORKFLOW.pkb"),
                "CREATE OR REPLACE PACKAGE BODY PG_BPGE_WORKFLOW AS\nEND;\n");

        String source = downloader.downloadPackageBody("PG_BPGE_WORKFLOW");
        assertNotNull(source);
        assertTrue(source.contains("PG_BPGE_WORKFLOW"));
    }

    @Test
    @DisplayName("Local cache: no substring match for similar package name")
    void testNoSubstringMatchForSimilarName() throws Exception {
        Files.writeString(tempDir.resolve("CUSTOMER.PG_BPGE_WORKFLOW_CLM.pkb"),
                "CREATE OR REPLACE PACKAGE BODY PG_BPGE_WORKFLOW_CLM AS\nEND;\n");

        String source = downloader.downloadPackageBody("PG_BPGE_WORKFLOW");
        assertNull(source, "Should NOT match PG_BPGE_WORKFLOW_CLM when looking for PG_BPGE_WORKFLOW");
    }

    @Test
    @DisplayName("Local cache: correct file returned when both similar names exist")
    void testCorrectFileWhenBothExist() throws Exception {
        Files.writeString(tempDir.resolve("CUSTOMER.PG_BPGE_WORKFLOW.pkb"),
                "CREATE OR REPLACE PACKAGE BODY PG_BPGE_WORKFLOW AS\n  v_correct := TRUE;\nEND;\n");
        Files.writeString(tempDir.resolve("CUSTOMER.PG_BPGE_WORKFLOW_CLM.pkb"),
                "CREATE OR REPLACE PACKAGE BODY PG_BPGE_WORKFLOW_CLM AS\n  v_wrong := TRUE;\nEND;\n");

        String source = downloader.downloadPackageBody("PG_BPGE_WORKFLOW");
        assertNotNull(source);
        assertTrue(source.contains("v_correct"), "Should return PG_BPGE_WORKFLOW, not PG_BPGE_WORKFLOW_CLM");
        assertFalse(source.contains("v_wrong"));
    }

    @Test
    @DisplayName("Local cache: longer name matches its own file, not the shorter one")
    void testLongerNameMatchesCorrectFile() throws Exception {
        Files.writeString(tempDir.resolve("CUSTOMER.PG_BPGE_WORKFLOW.pkb"),
                "CREATE OR REPLACE PACKAGE BODY PG_BPGE_WORKFLOW AS\n  v_base := TRUE;\nEND;\n");
        Files.writeString(tempDir.resolve("CUSTOMER.PG_BPGE_WORKFLOW_CLM.pkb"),
                "CREATE OR REPLACE PACKAGE BODY PG_BPGE_WORKFLOW_CLM AS\n  v_clm := TRUE;\nEND;\n");

        String source = downloader.downloadPackageBody("PG_BPGE_WORKFLOW_CLM");
        assertNotNull(source);
        assertTrue(source.contains("v_clm"), "Should return PG_BPGE_WORKFLOW_CLM source");
        assertFalse(source.contains("v_base"));
    }

    @Test
    @DisplayName("Local cache: file without schema prefix matches")
    void testFileWithoutSchemaPrefix() throws Exception {
        Files.writeString(tempDir.resolve("PG_GC_COMMON.pkb"),
                "CREATE OR REPLACE PACKAGE BODY PG_GC_COMMON AS\nEND;\n");

        String source = downloader.downloadPackageBody("PG_GC_COMMON");
        assertNotNull(source);
        assertTrue(source.contains("PG_GC_COMMON"));
    }

    @Test
    @DisplayName("Local cache: procedure file matches by exact name")
    void testProcedureExactMatch() throws Exception {
        Files.writeString(tempDir.resolve("CUSTOMER.PROC_GEN_ACC_LOG.prc"),
                "CREATE OR REPLACE PROCEDURE PROC_GEN_ACC_LOG AS\nBEGIN NULL; END;\n");

        String source = downloader.downloadProcedure("PROC_GEN_ACC_LOG");
        assertNotNull(source);
    }

    @Test
    @DisplayName("Local cache: function file matches by exact name")
    void testFunctionExactMatch() throws Exception {
        Files.writeString(tempDir.resolve("CUSTOMER.FN_GET_EINV_IRBM_MAPPING.fnc"),
                "CREATE OR REPLACE FUNCTION FN_GET_EINV_IRBM_MAPPING RETURN VARCHAR2 AS\nBEGIN RETURN NULL; END;\n");

        String source = downloader.downloadFunction("FN_GET_EINV_IRBM_MAPPING");
        assertNotNull(source);
    }

    @Test
    @DisplayName("Local cache: no match returns null gracefully")
    void testNoMatchReturnsNull() throws Exception {
        Files.writeString(tempDir.resolve("CUSTOMER.SOME_OTHER_PKG.pkb"),
                "CREATE OR REPLACE PACKAGE BODY SOME_OTHER_PKG AS\nEND;\n");

        String source = downloader.downloadPackageBody("PG_BPGE_WORKFLOW");
        assertNull(source);
    }
}
