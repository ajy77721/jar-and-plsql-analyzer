package com.plsql.parser;

import com.plsql.parser.model.*;
import com.plsql.parser.visitor.PlSqlAnalysisVisitor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regex → grammar migration, Oracle site: the table targets inside an EXECUTE IMMEDIATE /
 * DBMS_SQL.PARSE string literal are now read off a recursive ANTLR parse of the literal
 * ({@link PlSqlAnalysisVisitor}'s nested parse) rather than the legacy DYN_SQL_TABLE_PATTERN
 * regex, which survives only as a counted fallback.
 *
 * <p>The fixture below is deliberately one the regex gets wrong: the old pattern had no
 * comma-separated FROM alternative, so only the first table of `FROM emp a, dept b` was ever
 * recorded. The grammar path resolves both, proving the parse tree — not the regex — is now
 * the source of the rows.
 */
@DisplayName("Dynamic SQL: recursive grammar parse (regex → fallback only)")
public class DynamicSqlRecursiveParseTest extends ParserTestBase {

    @Test
    @DisplayName("comma-separated FROM list yields every table (regex saw only the first)")
    void commaSeparatedFromList() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    EXECUTE IMMEDIATE 'SELECT a.id FROM emp a, dept b WHERE a.id = b.id';
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "comma FROM list");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                        "EMP".equalsIgnoreCase(t.getTableName())
                                && "DYNAMIC".equalsIgnoreCase(t.getOperation())),
                "EMP should be extracted as DYNAMIC");
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                        "DEPT".equalsIgnoreCase(t.getTableName())
                                && "DYNAMIC".equalsIgnoreCase(t.getOperation())),
                "DEPT should be extracted as DYNAMIC by the recursive grammar parse "
                        + "(the old regex only matched the table directly after FROM)");
    }

    @Test
    @DisplayName("subquery + join tables all captured through the grammar")
    void subqueryAndJoinTables() {
        String sql = """
                CREATE OR REPLACE PROCEDURE READ_IT IS
                BEGIN
                  EXECUTE IMMEDIATE 'SELECT * FROM orders o JOIN customers c ON o.cid = c.id '
                      || 'WHERE o.id IN (SELECT id FROM archive_orders)';
                END READ_IT;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "subquery + join");
        ParsedObject po = result.getObjects().get(0);
        assertTrue(po.getTableOperations().stream().anyMatch(t ->
                        "ORDERS".equalsIgnoreCase(t.getTableName())),
                "ORDERS should be extracted");
        assertTrue(po.getTableOperations().stream().anyMatch(t ->
                        "CUSTOMERS".equalsIgnoreCase(t.getTableName())),
                "CUSTOMERS should be extracted from the dynamic JOIN");
        assertTrue(po.getTableOperations().stream().anyMatch(t ->
                        "ARCHIVE_ORDERS".equalsIgnoreCase(t.getTableName())),
                "ARCHIVE_ORDERS should be extracted from the dynamic subquery");
    }

    @Test
    @DisplayName("a concatenation fragment the grammar cannot parse falls back to regex, counted")
    void regexFallbackIsCounted() {
        long before = PlSqlAnalysisVisitor.DYN_SQL_REGEX_FALLBACKS.get();
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC (p_table VARCHAR2) IS
                  BEGIN
                    EXECUTE IMMEDIATE 'DELETE FROM ' || p_table || ' WHERE 1=1';
                  END PROC;
                END MY_PKG;
                /
                """;
        parse(sql);
        assertTrue(PlSqlAnalysisVisitor.DYN_SQL_REGEX_FALLBACKS.get() > before,
                "a literal fragment the grammar cannot parse must be counted as a regex fallback");
    }
}
