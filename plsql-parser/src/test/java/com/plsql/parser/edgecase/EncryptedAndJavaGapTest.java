package com.plsql.parser.edgecase;

import com.plsql.parser.ParserTestBase;
import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge-case tests for Gap #3 (Java stored procedures not parsed),
 * Gap #4 (Encrypted/wrapped package handling), and
 * Gap #5 (XML/JSON function grammar coverage).
 *
 * Validates graceful handling of encrypted source, garbage input,
 * unclosed blocks, and modern Oracle XML/JSON functions.
 */
@DisplayName("Gap #3/#4/#5: Encrypted, Error Recovery & XML/JSON Functions")
public class EncryptedAndJavaGapTest extends ParserTestBase {

    // ── Helper ──────────────────────────────────────────────────────────────

    private String wrap(String body) {
        return """
                CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                  %s
                END TEST_PKG;
                /
                """.formatted(body);
    }

    // ── 1. Wrapped package detected ─────────────────────────────────────────

    @Test
    @DisplayName("Wrapped/encrypted package is detected without parse errors")
    void testWrappedPackageDetected() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY wrapped_pkg wrapped
                a000000
                89abcdef
                abcdef0123456789abcdef0123456789abcdef0123456789
                0123456789abcdef0123456789abcdef0123456789abcdef
                """;
        ParseResult result = parse(sql);
        assertNotNull(result, "ParseResult should not be null for wrapped source");
        assertFalse(result.getObjects().isEmpty(),
                "Should produce at least one stub object for wrapped source");
        ParsedObject obj = result.getObjects().get(0);
        assertTrue(obj.getType().toUpperCase().contains("ENCRYPTED")
                        || obj.getType().toUpperCase().contains("WRAPPED"),
                "Object type should indicate ENCRYPTED or WRAPPED, got: " + obj.getType());
        assertTrue(result.getErrors() == null || result.getErrors().isEmpty(),
                "No parse errors should be raised for wrapped source");
    }

    // ── 2. Wrapped package — no subprograms, no crash ───────────────────────

    @Test
    @DisplayName("Wrapped package produces no subprograms and does not crash")
    void testWrappedPackageNoSubprograms() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY another_wrapped wrapped
                a000000
                1234abcd
                deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef
                """;
        ParseResult result = parse(sql);
        assertNotNull(result);
        assertFalse(result.getObjects().isEmpty());
        ParsedObject obj = result.getObjects().get(0);
        assertTrue(obj.getSubprograms().isEmpty(),
                "Wrapped source should produce zero subprograms");
    }

    // ── 3. Empty package body — zero subprograms, no error ──────────────────

    @Test
    @DisplayName("Empty package body produces zero subprograms and no parse error")
    void testEmptyPackageBody() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY empty_pkg AS
                END empty_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Empty package body");
        assertFalse(result.getObjects().isEmpty(),
                "Should produce one object for the empty package body");
        ParsedObject obj = result.getObjects().get(0);
        assertTrue(obj.getSubprograms().isEmpty(),
                "Empty package body should have zero subprograms");
    }

    // ── 4. Garbage input — no exception, errors collected ───────────────────

    @Test
    @DisplayName("Completely garbage input does not throw, errors are collected")
    void testGarbageInputNoException() {
        String garbage = "@@@ THIS IS NOT SQL !!! $$$$ %%%% ^^^^ &&&& **** ((((";
        ParseResult result = assertDoesNotThrow(() -> parse(garbage),
                "Parsing garbage should not throw an exception");
        assertNotNull(result, "ParseResult should not be null even for garbage");
        assertNotNull(result.getErrors(), "Errors list should not be null");
    }

    // ── 5. Unclosed block — graceful error recovery ─────────────────────────

    @Test
    @DisplayName("Unclosed BEGIN block recovers gracefully, table may still be extracted")
    void testUnclosedBlock() {
        String sql = """
                CREATE OR REPLACE PROCEDURE broken_proc IS
                BEGIN
                  SELECT COUNT(*) FROM recoverable_table WHERE 1=1;
                """;
        ParseResult result = assertDoesNotThrow(() -> parse(sql),
                "Unclosed block should not throw");
        assertNotNull(result);
        // Errors are expected for the unclosed block
        assertNotNull(result.getErrors());

        // Best-effort: the parser may still extract the table reference
        boolean tableFound = result.getObjects().stream()
                .flatMap(o -> o.getTableOperations().stream())
                .anyMatch(t -> "RECOVERABLE_TABLE".equalsIgnoreCase(t.getTableName()));
        boolean tableFoundInSubs = result.getObjects().stream()
                .flatMap(o -> o.getSubprograms().stream())
                .flatMap(s -> s.getTableOperations().stream())
                .anyMatch(t -> "RECOVERABLE_TABLE".equalsIgnoreCase(t.getTableName()));
        // At minimum the parser should not crash; table extraction is best-effort
        assertTrue(tableFound || tableFoundInSubs || !result.getErrors().isEmpty(),
                "Should either extract RECOVERABLE_TABLE or report errors (graceful degradation)");
    }

    // ── 6. XMLELEMENT — parses without error, table extracted ───────────────

    @Test
    @DisplayName("XMLELEMENT with XMLFOREST parses without error and extracts table")
    void testXmlElementParsesWithoutError() {
        String sql = wrap("""
                PROCEDURE P IS
                  v_xml XMLTYPE;
                BEGIN
                  SELECT XMLELEMENT("emp",
                           XMLFOREST(e.first_name AS "name", e.salary AS "salary"))
                    INTO v_xml
                    FROM employees e
                   WHERE e.employee_id = 100;
                END P;
                """);
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "XMLELEMENT with XMLFOREST");
        SubprogramInfo proc = findSub(result, "P");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                        "EMPLOYEES".equalsIgnoreCase(t.getTableName())),
                "EMPLOYEES should be extracted from XMLELEMENT query");
    }

    // ── 7. XMLAGG — parses without error, table extracted ───────────────────

    @Test
    @DisplayName("XMLAGG with XMLELEMENT parses without error and extracts table")
    void testXmlAggParsesWithoutError() {
        String sql = wrap("""
                PROCEDURE P IS
                  v_xml XMLTYPE;
                BEGIN
                  SELECT XMLAGG(XMLELEMENT("name", e.first_name) ORDER BY e.first_name)
                    INTO v_xml
                    FROM employees e
                   WHERE e.department_id = 10;
                END P;
                """);
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "XMLAGG");
        SubprogramInfo proc = findSub(result, "P");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                        "EMPLOYEES".equalsIgnoreCase(t.getTableName())),
                "EMPLOYEES should be extracted from XMLAGG query");
    }

    // ── 8. JSON_VALUE — parses without error, table extracted ───────────────

    @Test
    @DisplayName("JSON_VALUE function parses without error and extracts table")
    void testJsonValueParsesWithoutError() {
        String sql = wrap("""
                PROCEDURE P IS
                  v_name VARCHAR2(200);
                BEGIN
                  SELECT JSON_VALUE(data, '$.name')
                    INTO v_name
                    FROM json_data
                   WHERE ROWNUM = 1;
                END P;
                """);
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "JSON_VALUE");
        SubprogramInfo proc = findSub(result, "P");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                        "JSON_DATA".equalsIgnoreCase(t.getTableName())),
                "JSON_DATA should be extracted from JSON_VALUE query");
    }

    // ── 9. JSON_OBJECT — parses without error, table extracted ──────────────

    @Test
    @DisplayName("JSON_OBJECT function parses without error and extracts table")
    void testJsonObjectParsesWithoutError() {
        String sql = wrap("""
                PROCEDURE P IS
                  v_json VARCHAR2(4000);
                BEGIN
                  SELECT JSON_OBJECT('id' VALUE id, 'name' VALUE first_name)
                    INTO v_json
                    FROM employees
                   WHERE ROWNUM = 1;
                END P;
                """);
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "JSON_OBJECT");
        SubprogramInfo proc = findSub(result, "P");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                        "EMPLOYEES".equalsIgnoreCase(t.getTableName())),
                "EMPLOYEES should be extracted from JSON_OBJECT query");
    }
}
