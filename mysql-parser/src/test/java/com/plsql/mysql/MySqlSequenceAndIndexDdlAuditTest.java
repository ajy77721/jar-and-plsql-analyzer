package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlRoutineInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Read-only audit of CREATE SEQUENCE and CREATE INDEX support in mysql-parser.
 *
 * <p><b>CREATE SEQUENCE — N/A for MySQL.</b> MySQL has no native CREATE SEQUENCE
 * statement; auto-incrementing values are produced via the AUTO_INCREMENT column
 * attribute (and read back with LAST_INSERT_ID()), not a standalone sequence object.
 * Grep evidence from the grammar confirms there is no sequence-DDL rule:
 * <pre>
 *   $ grep -ni sequence mysql-parser/src/main/antlr4/com/plsql/mysql/grammar/MySqlParser.g4
 *   244:// SECTION 5 — Block body (sequence of statements)
 * </pre>
 * The only hit is an unrelated comment ("sequence of statements" = a block's list of
 * statements), not a grammar rule. This is corroborated by
 * {@code MySqlRecursiveCteAndSequenceAuditTest}, whose "Sequence" in the class name
 * refers to AUTO_INCREMENT / LAST_INSERT_ID() chains, not a CREATE SEQUENCE DDL
 * statement — because no such MySQL statement exists. No CREATE SEQUENCE test is
 * written here, per the task instructions, since inventing syntax for a
 * nonexistent MySQL statement would not be real evidence.
 *
 * <p><b>CREATE INDEX — supported.</b> The grammar gap this audit originally recorded
 * (no {@code createIndex} rule; CREATE INDEX failed at the top level with "no viable
 * alternative") has been closed. The entry point is now:
 * <pre>
 *   compilationUnit
 *       : (createRoutine | createTrigger | createView | createIndex) EOF
 *       ;
 * </pre>
 * backed by a {@code createIndex} rule covering
 * {@code CREATE [UNIQUE|FULLTEXT|SPATIAL] INDEX name [USING {BTREE|HASH}] ON table (col [ASC|DESC], ...)},
 * a matching {@code visitCreateIndex} in {@code MySqlAnalysisVisitor} (the same
 * identity/target-table pattern as {@code visitCreateTrigger}/{@code visitCreateView}),
 * and {@code RoutineType.INDEX} plus indexTable/indexKind/indexType/indexColumns fields
 * on {@code MySqlRoutineInfo}.
 *
 * <p>The tests below feed real CREATE INDEX snippets through the actual
 * {@code MySqlGrammarEngine} (real lexer/parser/visitor, no mocks) and assert the
 * captured facts.
 */
public class MySqlSequenceAndIndexDdlAuditTest {

    @Test
    @DisplayName("SEQUENCE: CREATE SEQUENCE is N/A for MySQL (documentation-as-code, no invented syntax)")
    void createSequenceIsNotApplicableToMySql() {
        // No test SQL is executed here on purpose: MySQL has no CREATE SEQUENCE
        // statement, so there is no real syntax to feed the parser. The N/A
        // determination is backed entirely by the grep evidence in the class
        // Javadoc above (MySqlParser.g4 grep for "sequence" yields only an
        // unrelated comment) and by the absence of any createSequence rule or
        // visitCreateSequence method in the codebase.
        assertTrue(true, "CREATE SEQUENCE is N/A for MySQL; see class Javadoc for grep evidence");
    }

    @Test
    @DisplayName("INDEX 1: CREATE INDEX idx_orders_status ON orders(status)")
    void createIndexSimple() {
        MySqlRoutineInfo r = parseIndex("INDEX-1 (simple CREATE INDEX)",
                "CREATE INDEX idx_orders_status ON orders(status);");
        assertEquals(MySqlRoutineInfo.RoutineType.INDEX, r.getType());
        assertEquals("idx_orders_status", r.getName());
        assertEquals("orders", r.getIndexTable());
        assertEquals(List.of("status"), r.getIndexColumns());
        assertNull(r.getIndexKind(), "plain index has no UNIQUE/FULLTEXT/SPATIAL flavour");
        assertFalse(r.isUniqueIndex());
    }

    @Test
    @DisplayName("INDEX 2: CREATE UNIQUE INDEX idx_orders_email ON orders(email)")
    void createUniqueIndex() {
        MySqlRoutineInfo r = parseIndex("INDEX-2 (CREATE UNIQUE INDEX)",
                "CREATE UNIQUE INDEX idx_orders_email ON orders(email);");
        assertEquals(MySqlRoutineInfo.RoutineType.INDEX, r.getType());
        assertEquals("idx_orders_email", r.getName());
        assertEquals("orders", r.getIndexTable());
        assertEquals(List.of("email"), r.getIndexColumns());
        assertEquals("UNIQUE", r.getIndexKind());
        assertTrue(r.isUniqueIndex());
    }

    @Test
    @DisplayName("INDEX 3: CREATE INDEX idx_orders_composite ON orders(customer_id, status, created_at)")
    void createIndexComposite() {
        MySqlRoutineInfo r = parseIndex("INDEX-3 (composite column CREATE INDEX)",
                "CREATE INDEX idx_orders_composite ON orders(customer_id, status, created_at);");
        assertEquals(MySqlRoutineInfo.RoutineType.INDEX, r.getType());
        assertEquals("idx_orders_composite", r.getName());
        assertEquals("orders", r.getIndexTable());
        assertEquals(List.of("customer_id", "status", "created_at"), r.getIndexColumns());
    }

    @Test
    @DisplayName("INDEX 4: FULLTEXT / SPATIAL flavours, USING BTREE|HASH, ASC/DESC columns, schema-qualified table")
    void createIndexFlavoursAndOptions() {
        MySqlRoutineInfo ft = parseIndex("INDEX-4a (FULLTEXT)",
                "CREATE FULLTEXT INDEX idx_art_body ON articles(body);");
        assertEquals("FULLTEXT", ft.getIndexKind());

        MySqlRoutineInfo sp = parseIndex("INDEX-4b (SPATIAL)",
                "CREATE SPATIAL INDEX idx_geo ON places(location);");
        assertEquals("SPATIAL", sp.getIndexKind());

        MySqlRoutineInfo bt = parseIndex("INDEX-4c (USING BTREE + ASC/DESC + schema-qualified table)",
                "CREATE INDEX idx_o USING BTREE ON sales.orders (created_at DESC, status ASC);");
        assertEquals("BTREE", bt.getIndexType());
        assertEquals("orders", bt.getIndexTable());
        assertEquals(List.of("created_at", "status"), bt.getIndexColumns());

        MySqlRoutineInfo hs = parseIndex("INDEX-4d (USING HASH)",
                "CREATE INDEX idx_h USING HASH ON lookups(k);");
        assertEquals("HASH", hs.getIndexType());
        assertEquals("lookups", hs.getIndexTable());
    }

    @Test
    @DisplayName("INDEX 5: no regression — `hash`, `btree`, `spatial`, `fulltext` still usable as identifiers")
    void newKeywordsStillUsableAsIdentifiers() {
        String sql = """
                CREATE PROCEDURE p()
                BEGIN
                  DECLARE hash VARCHAR(64);
                  DECLARE btree INT;
                  SELECT fulltext, spatial INTO hash, btree FROM t;
                END;
                """;
        MySqlRoutineInfo r = new MySqlGrammarEngine().parse(sql, "INDEX-5", null);
        assertTrue(r.getErrors().isEmpty(),
                "non-reserved new keywords must remain usable as identifiers, errors=" + r.getErrors());
        assertEquals(MySqlRoutineInfo.RoutineType.PROCEDURE, r.getType());
    }

    /** Runs the snippet through the real MySqlGrammarEngine and asserts a clean parse. */
    private MySqlRoutineInfo parseIndex(String label, String sql) {
        MySqlRoutineInfo r = new MySqlGrammarEngine().parse(sql, label, null);
        System.out.println("[" + label + "] sql=" + sql
                + " type=" + r.getType()
                + " name=" + r.getName()
                + " table=" + r.getIndexTable()
                + " kind=" + r.getIndexKind()
                + " using=" + r.getIndexType()
                + " cols=" + r.getIndexColumns()
                + " errors=" + r.getErrors());
        assertTrue(r.getErrors().isEmpty(),
                "[" + label + "] expected a clean parse, got errors: " + r.getErrors());
        return r;
    }
}
