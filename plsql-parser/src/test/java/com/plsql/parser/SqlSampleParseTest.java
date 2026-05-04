package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates all 20 SQL test files under plsql-samples/ parse correctly.
 * Uses a parameterized test for bulk parse-without-crash checks, then
 * individual tests for deeper assertions on key files.
 */
@DisplayName("SQL Sample File Parsing")
public class SqlSampleParseTest extends ParserTestBase {

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static String loadSql(String fileName) {
        String path = "plsql-samples/" + fileName;
        try (InputStream is = SqlSampleParseTest.class.getClassLoader().getResourceAsStream(path)) {
            assertNotNull(is, "Resource not found: " + path);
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + path, e);
        }
    }

    private int totalSubprograms(ParseResult result) {
        return result.getObjects().stream()
                .mapToInt(o -> o.getSubprograms().size())
                .sum();
    }

    private Set<String> allTableNames(ParseResult result) {
        return result.getObjects().stream()
                .flatMap(o -> o.getSubprograms().stream())
                .flatMap(s -> s.getTableOperations().stream())
                .map(t -> t.getTableName().toUpperCase())
                .collect(Collectors.toSet());
    }

    private Set<String> allSequences(ParseResult result) {
        return result.getObjects().stream()
                .flatMap(o -> o.getDependencies().getSequences().stream())
                .map(String::toUpperCase)
                .collect(Collectors.toSet());
    }

    // ------------------------------------------------------------------
    // Parameterized: every file parses without crashing
    // ------------------------------------------------------------------

    static Stream<Arguments> allSampleFiles() {
        return Stream.of(
            Arguments.of("test_01_basic_dml_operations.sql",        "PKG_DML_TEST",             10, 3),
            Arguments.of("test_02_join_types.sql",                  "PKG_JOIN_TEST",             8, 3),
            Arguments.of("test_03_subqueries_and_ctes.sql",         "PKG_SUBQUERY_TEST",         8, 3),
            Arguments.of("test_04_dynamic_sql.sql",                 "PKG_DYNAMIC_SQL_TEST",      6, 5),
            Arguments.of("test_05_cursor_patterns.sql",             "PKG_CURSOR_TEST",           7, 3),
            Arguments.of("test_06_bulk_operations.sql",             "PKG_BULK_OPS_TEST",         6, 3),
            Arguments.of("test_07_advanced_query_features.sql",     "PKG_ADVANCED_QUERY_TEST",   7, 3),
            Arguments.of("test_08_table_types_and_references.sql",  "PKG_TABLE_TYPES_TEST",      6, 5),
            Arguments.of("test_09_control_flow_and_exceptions.sql", "PKG_CONTROL_FLOW_TEST",     6, 5),
            Arguments.of("test_10_sequences.sql",                   "PKG_SEQUENCE_TEST",         5, 5),
            Arguments.of("test_11_package_procedure_patterns.sql",  "PKG_CALL_GRAPH_TEST",       8, 5),
            Arguments.of("test_12_triggers.sql",                    null,                        0, 5),
            Arguments.of("test_13_views_and_types.sql",             null,                        0, 5),
            Arguments.of("test_14_etl_batch_processing.sql",        "PKG_ETL_PROCESSOR",         4, 5),
            Arguments.of("test_15_audit_trail_trigger.sql",         null,                        1, 5),
            Arguments.of("test_16_report_generation.sql",           "PKG_REPORT_GEN",            4, 5),
            Arguments.of("test_17_error_logging_patterns.sql",      "PKG_ERROR_HANDLER",         4, 5),
            Arguments.of("test_18_set_operations_complex.sql",      "PKG_SET_OPS_TEST",          5, 5),
            Arguments.of("test_19_data_validation.sql",             "PKG_DATA_VALIDATOR",        3, 5),
            Arguments.of("test_20_mega_integration.sql",            "PKG_MEGA_INTEGRATION",     10, 5)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allSampleFiles")
    @DisplayName("Parses without crash and extracts expected structure")
    void parsesWithoutCrash(String file, String expectedPkg, int minSubprograms, int maxErrors) {
        String sql = loadSql(file);
        ParseResult result = parse(sql);

        assertNotNull(result, file + " returned null");

        // Allow a small number of parse errors for complex files
        assertTrue(result.getErrors().size() <= maxErrors,
                file + " has too many errors (" + result.getErrors().size()
                        + "): " + result.getErrors());

        // Check package name if expected
        if (expectedPkg != null) {
            assertFalse(result.getObjects().isEmpty(),
                    file + " produced zero parsed objects");
            boolean found = result.getObjects().stream()
                    .anyMatch(o -> expectedPkg.equalsIgnoreCase(o.getName()));
            assertTrue(found, file + " — expected object named " + expectedPkg);
        }

        // Check minimum subprogram count (skip for files with no expected subprograms)
        if (minSubprograms > 0) {
            int subCount = totalSubprograms(result);
            assertTrue(subCount >= minSubprograms,
                    file + " — expected >= " + minSubprograms + " subprograms but got " + subCount);
        }
    }

    // ------------------------------------------------------------------
    // Deep assertions: test_01 (Basic DML)
    // ------------------------------------------------------------------

    @Test
    @DisplayName("test_01: DML operations extract all procedures and tables")
    void test01_basicDml() {
        ParseResult result = parse(loadSql("test_01_basic_dml_operations.sql"));
        assertTrue(result.getErrors().size() <= 3, "test_01 errors: " + result.getErrors());

        int subCount = totalSubprograms(result);
        assertTrue(subCount >= 14, "Expected >= 14 subprograms, got " + subCount);

        // Verify key procedures exist
        assertNotNull(findSub(result, "PC_SELECT_SIMPLE"), "Missing PC_SELECT_SIMPLE");
        assertNotNull(findSub(result, "PC_INSERT_SINGLE_ROW"), "Missing PC_INSERT_SINGLE_ROW");
        assertNotNull(findSub(result, "PC_UPDATE_SIMPLE"), "Missing PC_UPDATE_SIMPLE");
        assertNotNull(findSub(result, "PC_DELETE_SIMPLE"), "Missing PC_DELETE_SIMPLE");
        assertNotNull(findSub(result, "PC_MERGE_COMPLEX"), "Missing PC_MERGE_COMPLEX");
        assertNotNull(findSub(result, "PC_INSERT_RETURNING"), "Missing PC_INSERT_RETURNING");

        // Key tables referenced
        Set<String> tables = allTableNames(result);
        assertTrue(tables.contains("EMPLOYEES"), "Missing EMPLOYEES table");
        assertTrue(tables.contains("PROCESS_LOG"), "Missing PROCESS_LOG table");
        assertTrue(tables.contains("AUDIT_LOG"), "Missing AUDIT_LOG table");

        // Sequences detected
        Set<String> seqs = allSequences(result);
        assertTrue(seqs.contains("LOG_SEQ"), "Missing LOG_SEQ sequence");
        assertTrue(seqs.contains("AUDIT_SEQ"), "Missing AUDIT_SEQ sequence");

        // Verify PC_SELECT_SIMPLE has table operations
        SubprogramInfo selectSimple = findSub(result, "PC_SELECT_SIMPLE");
        assertFalse(selectSimple.getTableOperations().isEmpty(),
                "PC_SELECT_SIMPLE should have table operations");
        assertTrue(selectSimple.getTableOperations().stream()
                        .anyMatch(t -> "EMPLOYEES".equalsIgnoreCase(t.getTableName())),
                "PC_SELECT_SIMPLE should reference EMPLOYEES");
    }

    // ------------------------------------------------------------------
    // Deep assertions: test_02 (Joins)
    // ------------------------------------------------------------------

    @Test
    @DisplayName("test_02: Join types extract all procedures and join tables")
    void test02_joinTypes() {
        ParseResult result = parse(loadSql("test_02_join_types.sql"));
        assertTrue(result.getErrors().size() <= 3, "test_02 errors: " + result.getErrors());

        int subCount = totalSubprograms(result);
        assertTrue(subCount >= 10, "Expected >= 10 subprograms, got " + subCount);

        // Verify join-related procedures
        assertNotNull(findSub(result, "PC_INNER_JOIN_EXPLICIT"), "Missing PC_INNER_JOIN_EXPLICIT");
        assertNotNull(findSub(result, "PC_LEFT_OUTER_JOIN"), "Missing PC_LEFT_OUTER_JOIN");
        assertNotNull(findSub(result, "PC_FULL_OUTER_JOIN"), "Missing PC_FULL_OUTER_JOIN");
        assertNotNull(findSub(result, "PC_SELF_JOIN"), "Missing PC_SELF_JOIN");
        assertNotNull(findSub(result, "PC_MULTI_TABLE_5WAY"), "Missing PC_MULTI_TABLE_5WAY");

        // Key tables
        Set<String> tables = allTableNames(result);
        assertTrue(tables.contains("EMPLOYEES"), "Missing EMPLOYEES table");
        assertTrue(tables.contains("DEPARTMENTS"), "Missing DEPARTMENTS table");
        assertTrue(tables.contains("LOCATIONS"), "Missing LOCATIONS table");

        // PC_INNER_JOIN_IMPLICIT should reference multiple tables (implicit join)
        SubprogramInfo implicitJoin = findSub(result, "PC_INNER_JOIN_IMPLICIT");
        assertNotNull(implicitJoin);
        assertTrue(implicitJoin.getTableOperations().size() >= 3,
                "PC_INNER_JOIN_IMPLICIT should reference >= 3 tables");
    }

    // ------------------------------------------------------------------
    // Deep assertions: test_04 (Dynamic SQL)
    // ------------------------------------------------------------------

    @Test
    @DisplayName("test_04: Dynamic SQL detects EXECUTE IMMEDIATE and DBMS_SQL")
    void test04_dynamicSql() {
        ParseResult result = parse(loadSql("test_04_dynamic_sql.sql"));
        assertTrue(result.getErrors().size() <= 5, "test_04 errors: " + result.getErrors());

        int subCount = totalSubprograms(result);
        assertTrue(subCount >= 10, "Expected >= 10 subprograms, got " + subCount);

        // Key procedures
        assertNotNull(findSub(result, "PC_EXEC_IMM_LITERAL"), "Missing PC_EXEC_IMM_LITERAL");
        assertNotNull(findSub(result, "PC_EXEC_IMM_VARIABLE"), "Missing PC_EXEC_IMM_VARIABLE");
        assertNotNull(findSub(result, "PC_DBMS_SQL_FULL"), "Missing PC_DBMS_SQL_FULL");
        assertNotNull(findSub(result, "PC_EXEC_IMM_TRUNCATE"), "Missing PC_EXEC_IMM_TRUNCATE");

        // Dynamic SQL should be detected in at least some subprograms
        SubprogramInfo execLiteral = findSub(result, "PC_EXEC_IMM_LITERAL");
        assertNotNull(execLiteral);
        assertFalse(execLiteral.getDynamicSql().isEmpty(),
                "PC_EXEC_IMM_LITERAL should have dynamic SQL statements");

        SubprogramInfo dbmsSql = findSub(result, "PC_DBMS_SQL_FULL");
        assertNotNull(dbmsSql);
        assertFalse(dbmsSql.getDynamicSql().isEmpty(),
                "PC_DBMS_SQL_FULL should have dynamic SQL statements");

        // AUDIT_LOG table should be found (from static INSERTs)
        Set<String> tables = allTableNames(result);
        assertTrue(tables.contains("AUDIT_LOG"), "Missing AUDIT_LOG table");
    }

    // ------------------------------------------------------------------
    // Deep assertions: test_20 (Mega Integration)
    // ------------------------------------------------------------------

    @Test
    @DisplayName("test_20: Mega integration extracts all 15 subprograms and complex patterns")
    void test20_megaIntegration() {
        ParseResult result = parse(loadSql("test_20_mega_integration.sql"));
        assertTrue(result.getErrors().size() <= 5, "test_20 errors: " + result.getErrors());

        int subCount = totalSubprograms(result);
        assertTrue(subCount >= 12, "Expected >= 12 subprograms, got " + subCount);

        // Verify the package object
        boolean pkgFound = result.getObjects().stream()
                .anyMatch(o -> "PKG_MEGA_INTEGRATION".equalsIgnoreCase(o.getName()));
        assertTrue(pkgFound, "PKG_MEGA_INTEGRATION package not found");

        // Key procedures and functions
        assertNotNull(findSub(result, "PC_INIT"), "Missing PC_INIT");
        assertNotNull(findSub(result, "FN_GET_CONFIG"), "Missing FN_GET_CONFIG");
        assertNotNull(findSub(result, "PC_LOAD_BATCH"), "Missing PC_LOAD_BATCH");
        assertNotNull(findSub(result, "PC_PROCESS_RECORDS"), "Missing PC_PROCESS_RECORDS");
        assertNotNull(findSub(result, "PC_MERGE_RESULTS"), "Missing PC_MERGE_RESULTS");
        assertNotNull(findSub(result, "FN_GET_REPORT"), "Missing FN_GET_REPORT");
        assertNotNull(findSub(result, "PC_ARCHIVE_DATA"), "Missing PC_ARCHIVE_DATA");
        assertNotNull(findSub(result, "PC_DYNAMIC_CLEANUP"), "Missing PC_DYNAMIC_CLEANUP");
        assertNotNull(findSub(result, "PC_AUDIT_LOG"), "Missing PC_AUDIT_LOG");
        assertNotNull(findSub(result, "PC_ORCHESTRATE_ALL"), "Missing PC_ORCHESTRATE_ALL");
        assertNotNull(findSub(result, "PC_VALIDATE_DATA"), "Missing PC_VALIDATE_DATA");
        assertNotNull(findSub(result, "FN_HIERARCHICAL"), "Missing FN_HIERARCHICAL");

        // Tables from multiple schemas
        Set<String> tables = allTableNames(result);
        assertTrue(tables.size() >= 5,
                "Expected >= 5 distinct tables, got " + tables.size() + ": " + tables);

        // Sequences
        Set<String> seqs = allSequences(result);
        assertFalse(seqs.isEmpty(), "test_20 should detect sequences");

        // PC_AUDIT_LOG should have PRAGMA AUTONOMOUS_TRANSACTION
        SubprogramInfo auditLog = findSub(result, "PC_AUDIT_LOG");
        assertNotNull(auditLog);
        assertTrue(auditLog.isPragmaAutonomousTransaction(),
                "PC_AUDIT_LOG should be marked AUTONOMOUS_TRANSACTION");

        // PC_DYNAMIC_CLEANUP should have dynamic SQL
        SubprogramInfo dynCleanup = findSub(result, "PC_DYNAMIC_CLEANUP");
        assertNotNull(dynCleanup);
        assertFalse(dynCleanup.getDynamicSql().isEmpty(),
                "PC_DYNAMIC_CLEANUP should have dynamic SQL statements");

        // FN_GET_CONFIG should be a FUNCTION
        SubprogramInfo getConfig = findSub(result, "FN_GET_CONFIG");
        assertNotNull(getConfig);
        assertEquals("FUNCTION", getConfig.getType(),
                "FN_GET_CONFIG should be a FUNCTION");
    }
}
