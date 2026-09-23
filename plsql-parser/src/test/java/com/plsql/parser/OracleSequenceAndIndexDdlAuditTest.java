package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Read-only audit (2026-09-18): CREATE SEQUENCE and CREATE INDEX support probe against the real
 * Oracle ANTLR grammar/visitor ({@link PlSqlParserEngine} -&gt; {@code PlSqlAnalysisVisitor}).
 *
 * <p>Grammar-level findings (see plsql-parser/src/main/antlr4/com/plsql/parser/PlSqlParser.g4):
 * <ul>
 *   <li>{@code create_sequence} rule exists (around line 1179) and IS wired into the top-level
 *       {@code unit_statement} alternation (around line 113), same as {@code create_trigger}/
 *       {@code create_view}.</li>
 *   <li>{@code create_index} rule exists (around line 1581) and IS ALSO wired into
 *       {@code unit_statement} (around line 95).</li>
 * </ul>
 *
 * <p>Visitor-level findings (see
 * plsql-parser/src/main/java/com/plsql/parser/visitor/PlSqlAnalysisVisitor.java):
 * <ul>
 *   <li>There is NO {@code visitCreate_trigger}-style override for {@code visitCreate_sequence}
 *       or {@code visitCreate_index}. Unlike {@code CREATE TRIGGER}/{@code CREATE VIEW}, which
 *       build a {@link ParsedObject} (type="TRIGGER"/"VIEW", name, dependencies, etc.) and add it
 *       to {@code results}, a standalone {@code CREATE SEQUENCE} or {@code CREATE INDEX} falls
 *       through to the default ANTLR {@code visitChildren} traversal with no dedicated capture —
 *       i.e. it parses (grammar supports it, no crash) but produces no distinct
 *       SEQUENCE/INDEX {@code ParsedObject} in {@code ParseResult.getObjects()}.</li>
 *   <li>The only place {@code CreateIndex} data is captured at all in this codebase is a
 *       *separate* JSqlParser-based path in
 *       {@code com.plsqlanalyzer.parser.service.SqlAnalyzer#extractDdl} used for dynamic-SQL
 *       string extraction — not the static ANTLR visitor exercised here.</li>
 * </ul>
 *
 * This is a diagnostic + permanent evidence file. It does not "fix" anything, and it does not
 * assert blind success — it records what is actually observed for each scenario.
 */
public class OracleSequenceAndIndexDdlAuditTest extends ParserTestBase {

    private ParsedObject findObject(ParseResult result, String type, String name) {
        for (ParsedObject po : result.getObjects()) {
            if (type.equalsIgnoreCase(po.getType())
                    && (name == null || name.equalsIgnoreCase(po.getName()))) {
                return po;
            }
        }
        return null;
    }

    private void dumpAllObjects(String label, ParseResult result) {
        System.out.println("=== " + label + " ===");
        System.out.println("  errors=" + result.getErrors());
        if (result.getObjects() == null || result.getObjects().isEmpty()) {
            System.out.println("  (NO ParsedObject captured at all — statement was not surfaced as a distinct object)");
            return;
        }
        for (ParsedObject po : result.getObjects()) {
            System.out.println("  captured object: type=" + po.getType() + " name=" + po.getName());
        }
    }

    // 1. CREATE SEQUENCE — standalone, with common clauses.
    @Test
    @DisplayName("1: CREATE SEQUENCE seq_orders START WITH 1 INCREMENT BY 1 CACHE 20 NOCYCLE")
    void createSequenceStandalone() {
        String sql = """
                CREATE SEQUENCE seq_orders START WITH 1 INCREMENT BY 1 CACHE 20 NOCYCLE;
                """;
        ParseResult result = parse(sql);

        // Does it parse without error at top level? (grammar has create_sequence wired into
        // unit_statement, so this is expected to succeed with zero errors.)
        assertNoParsErrors(result, "CREATE SEQUENCE");
        dumpAllObjects("1: CREATE SEQUENCE", result);

        ParsedObject po = findObject(result, "SEQUENCE", "SEQ_ORDERS");
        System.out.println("  SEQUENCE ParsedObject found? " + (po != null));
        Assertions.assertNotNull(po, "visitCreate_sequence should capture a SEQUENCE ParsedObject");
        Assertions.assertEquals("SEQ_ORDERS", po.getName());
        Assertions.assertEquals(1L, po.getSequenceStartWith());
        Assertions.assertEquals(1L, po.getSequenceIncrementBy());
        Assertions.assertEquals(20L, po.getSequenceCache());
        Assertions.assertEquals(Boolean.FALSE, po.getSequenceCycle());
    }

    // 2. CREATE INDEX — single column, non-unique.
    @Test
    @DisplayName("2: CREATE INDEX idx_orders_status ON orders(status)")
    void createIndexSingleColumn() {
        String sql = """
                CREATE INDEX idx_orders_status ON orders(status);
                """;
        ParseResult result = parse(sql);

        assertNoParsErrors(result, "CREATE INDEX (single column)");
        dumpAllObjects("2: CREATE INDEX single column", result);

        ParsedObject po = findObject(result, "INDEX", "IDX_ORDERS_STATUS");
        System.out.println("  INDEX ParsedObject found? " + (po != null));
        Assertions.assertNotNull(po, "visitCreate_index should capture an INDEX ParsedObject");
        Assertions.assertEquals("IDX_ORDERS_STATUS", po.getName());
        Assertions.assertFalse(po.isIndexUnique());
        Assertions.assertFalse(po.isIndexBitmap());
        Assertions.assertEquals(java.util.List.of("STATUS"), po.getIndexColumns());
        Assertions.assertTrue(po.getDependencies().getTables().contains("ORDERS"),
                "INDEX target table ORDERS should be captured as a dependency");
    }

    // 3. CREATE UNIQUE INDEX — verifies the UNIQUE variant also parses / is (not) captured.
    @Test
    @DisplayName("3: CREATE UNIQUE INDEX idx_orders_email ON orders(email)")
    void createUniqueIndex() {
        String sql = """
                CREATE UNIQUE INDEX idx_orders_email ON orders(email);
                """;
        ParseResult result = parse(sql);

        assertNoParsErrors(result, "CREATE UNIQUE INDEX");
        dumpAllObjects("3: CREATE UNIQUE INDEX", result);

        ParsedObject po = findObject(result, "INDEX", "IDX_ORDERS_EMAIL");
        System.out.println("  INDEX ParsedObject found? " + (po != null));
        Assertions.assertNotNull(po, "visitCreate_index should capture an INDEX ParsedObject");
        Assertions.assertTrue(po.isIndexUnique());
        Assertions.assertFalse(po.isIndexBitmap());
        Assertions.assertEquals(java.util.List.of("EMAIL"), po.getIndexColumns());
    }

    // 4. CREATE INDEX — composite (multi-column) index, to confirm the grammar's
    // table_index_clause column list also parses cleanly and check for capture.
    @Test
    @DisplayName("4: CREATE INDEX idx_orders_composite ON orders(customer_id, status, created_at)")
    void createCompositeIndex() {
        String sql = """
                CREATE INDEX idx_orders_composite ON orders(customer_id, status, created_at);
                """;
        ParseResult result = parse(sql);

        assertNoParsErrors(result, "CREATE INDEX (composite)");
        dumpAllObjects("4: CREATE INDEX composite", result);

        ParsedObject po = findObject(result, "INDEX", "IDX_ORDERS_COMPOSITE");
        System.out.println("  INDEX ParsedObject found? " + (po != null));
        Assertions.assertNotNull(po, "visitCreate_index should capture an INDEX ParsedObject");
        Assertions.assertEquals(
                java.util.List.of("CUSTOMER_ID", "STATUS", "CREATED_AT"), po.getIndexColumns());
    }
}
