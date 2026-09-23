package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.Db2RoutineInfo;
import com.plsql.db2.model.Db2TableOpInfo;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * READ-ONLY audit (2026-09-18): does db2-parser support CREATE SEQUENCE / CREATE INDEX DDL?
 *
 * Grammar evidence (Db2Parser.g4):
 *   compilationUnit : (routineDefinition | createTrigger | createView)? EOF ;
 * There is no createSequence or createIndex rule anywhere in the file, and no SEQUENCE / INDEX /
 * UNIQUE token exists in Db2Lexer.g4 either (grep for "sequence"/"index" in the parser grammar
 * only matches the unrelated NEXT VALUE FOR / "... FOR seq" *reference* syntax used inside
 * expressions/stmtToken, e.g. `SET v = NEXT VALUE FOR order_seq` — that is a sequence *reference*
 * inside a routine body, not a CREATE SEQUENCE *definition*, and there is nothing equivalent for
 * CREATE INDEX at all).
 *
 * Unlike CREATE TRIGGER / CREATE VIEW (fixed this session), CREATE SEQUENCE and CREATE INDEX are
 * NOT wired into compilationUnit's top-level alternation. Since SEQUENCE, INDEX and UNIQUE are not
 * lexer keywords, "CREATE SEQUENCE ..." / "CREATE INDEX ..." / "CREATE UNIQUE INDEX ..." fail to
 * match compilationUnit's single optional alternative (which demands PROCEDURE/FUNCTION/TRIGGER/
 * VIEW right after CREATE (OR REPLACE)?), producing ANTLR syntax errors surfaced via
 * Db2ParseErrorListener and an empty/near-empty Db2RoutineInfo — no sequence name, no target
 * table/index name captured anywhere, and no permissive catch-all absorbs it either (the catch-all
 * rules live inside routine bodies, not at the top level).
 *
 * FIXED (2026-09-18): compilationUnit now also accepts `createSequence` (and `createIndex`), with
 * SEQUENCE/INDEX/UNIQUE/START/INCREMENT/MINVALUE/MAXVALUE/CYCLE/CACHE added to Db2Lexer.g4 and
 * visitCreateSequence / visitCreateIndex capturing the metadata. The assertions below are now hard
 * assertions on the supported behavior.
 */
class Db2SequenceAndIndexDdlAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private static void dump(String label, Db2RoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("type=" + r.getType());
        System.out.println("name=" + r.getName());
        System.out.println("errors=" + r.getErrors());
        System.out.println("tableOps.size()=" + r.getTableOps().size());
        for (Db2TableOpInfo t : r.getTableOps()) {
            System.out.println("  " + t.getDmlType() + " " + t.qualifiedTable() + " alias=" + t.getAlias());
        }
        System.out.println("statements.size()=" + r.getStatements().size());
        System.out.println("calls.size()=" + r.getCalls().size());
    }

    // ── CREATE SEQUENCE ──────────────────────────────────────────────────────

    private static final String CREATE_SEQUENCE_SRC =
        "CREATE SEQUENCE seq_orders START WITH 1 INCREMENT BY 1 CACHE 20 NO CYCLE;";

    @Test
    @DisplayName("CREATE SEQUENCE seq_orders START WITH 1 INCREMENT BY 1 CACHE 20 NO CYCLE")
    void createSequenceIsParsedAndCaptured() {
        Db2RoutineInfo r = engine.parse(CREATE_SEQUENCE_SRC, "seq_orders", null);
        dump("CreateSequence", r);

        assertTrue(r.getErrors().isEmpty(), "expected clean parse, got " + r.getErrors());
        assertEquals(Db2RoutineInfo.RoutineType.SEQUENCE, r.getType());
        assertEquals("seq_orders", r.getName());
        assertEquals("1", r.getSequenceStartWith());
        assertEquals("1", r.getSequenceIncrementBy());
        assertEquals("20", r.getSequenceCache());
        assertTrue(r.isSequenceNoCycle(), "NO CYCLE should be flagged");
        assertFalse(r.isSequenceCycle());
    }

    @Test
    @DisplayName("CREATE SEQUENCE with AS type, negative increment, MINVALUE/MAXVALUE, CYCLE, NO CACHE")
    void createSequenceWithFullOptionSet() {
        Db2RoutineInfo r = engine.parse(
                "CREATE SEQUENCE app.seq_rev AS BIGINT START WITH 100 INCREMENT BY -1 "
                        + "MINVALUE 1 MAXVALUE 9999 CYCLE NO CACHE ORDER;",
                "seq_rev", null);
        dump("CreateSequenceFull", r);

        assertTrue(r.getErrors().isEmpty(), "expected clean parse, got " + r.getErrors());
        assertEquals(Db2RoutineInfo.RoutineType.SEQUENCE, r.getType());
        assertEquals("app", r.getSchema());
        assertEquals("seq_rev", r.getName());
        assertEquals("BIGINT", r.getSequenceDataType());
        assertEquals("100", r.getSequenceStartWith());
        assertEquals("-1", r.getSequenceIncrementBy());
        assertEquals("1", r.getSequenceMinValue());
        assertEquals("9999", r.getSequenceMaxValue());
        assertTrue(r.isSequenceCycle());
        assertTrue(r.isSequenceNoCache());
        assertTrue(r.isSequenceOrder());
    }

    @Test
    @DisplayName("CREATE SEQUENCE with NO MINVALUE / NO MAXVALUE and no other options")
    void createSequenceMinimalAndNegatedOptions() {
        Db2RoutineInfo r = engine.parse("CREATE SEQUENCE seq_bare;", "seq_bare", null);
        assertTrue(r.getErrors().isEmpty(), "expected clean parse, got " + r.getErrors());
        assertEquals(Db2RoutineInfo.RoutineType.SEQUENCE, r.getType());
        assertEquals("seq_bare", r.getName());
        assertNull(r.getSequenceStartWith());

        Db2RoutineInfo r2 = engine.parse(
                "CREATE SEQUENCE seq_nn NO MINVALUE NO MAXVALUE NO CYCLE;", "seq_nn", null);
        assertTrue(r2.getErrors().isEmpty(), "expected clean parse, got " + r2.getErrors());
        assertTrue(r2.isSequenceNoMinValue());
        assertTrue(r2.isSequenceNoMaxValue());
        assertTrue(r2.isSequenceNoCycle());
    }

    // ── CREATE INDEX ─────────────────────────────────────────────────────────

    private static final String CREATE_INDEX_SRC =
        "CREATE INDEX idx_orders_status ON orders(status);";

    @Test
    @DisplayName("CREATE INDEX idx_orders_status ON orders(status)")
    void createIndexIsParsedAndCaptured() {
        Db2RoutineInfo r = engine.parse(CREATE_INDEX_SRC, "idx_orders_status", null);
        dump("CreateIndex", r);

        assertTrue(r.getErrors().isEmpty(), "expected clean parse, got " + r.getErrors());
        assertEquals(Db2RoutineInfo.RoutineType.INDEX, r.getType());
        assertEquals("idx_orders_status", r.getName());
        assertEquals("ORDERS", r.getIndexTable().toUpperCase());
        assertFalse(r.isIndexUnique());
        assertEquals(List.of("status"), r.getIndexColumns());
        assertTrue(r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName())),
                "indexed table should surface as a table op");
    }

    // ── CREATE UNIQUE INDEX ──────────────────────────────────────────────────

    private static final String CREATE_UNIQUE_INDEX_SRC =
        "CREATE UNIQUE INDEX idx_orders_email ON orders(email);";

    @Test
    @DisplayName("CREATE UNIQUE INDEX idx_orders_email ON orders(email)")
    void createUniqueIndexIsParsedAndCaptured() {
        Db2RoutineInfo r = engine.parse(CREATE_UNIQUE_INDEX_SRC, "idx_orders_email", null);
        dump("CreateUniqueIndex", r);

        assertTrue(r.getErrors().isEmpty(), "expected clean parse, got " + r.getErrors());
        assertEquals(Db2RoutineInfo.RoutineType.INDEX, r.getType());
        assertEquals("idx_orders_email", r.getName());
        assertEquals("ORDERS", r.getIndexTable().toUpperCase());
        assertTrue(r.isIndexUnique());
        assertEquals(List.of("email"), r.getIndexColumns());
    }

    // ── CREATE INDEX with composite key ──────────────────────────────────────

    private static final String CREATE_INDEX_COMPOSITE_SRC =
        "CREATE INDEX idx_orders_composite ON orders(customer_id, status, created_at);";

    @Test
    @DisplayName("CREATE INDEX idx_orders_composite ON orders(customer_id, status, created_at)")
    void createCompositeIndexIsParsedAndCaptured() {
        Db2RoutineInfo r = engine.parse(CREATE_INDEX_COMPOSITE_SRC, "idx_orders_composite", null);
        dump("CreateIndexComposite", r);

        assertTrue(r.getErrors().isEmpty(), "expected clean parse, got " + r.getErrors());
        assertEquals(Db2RoutineInfo.RoutineType.INDEX, r.getType());
        assertEquals("ORDERS", r.getIndexTable().toUpperCase());
        assertEquals(List.of("customer_id", "status", "created_at"), r.getIndexColumns());
    }

    @Test
    @DisplayName("CREATE INDEX with schema-qualified table and ASC/DESC key directions")
    void createIndexWithSchemaAndSortDirections() {
        Db2RoutineInfo r = engine.parse(
                "CREATE UNIQUE INDEX app.idx_orders_sorted ON app.orders(customer_id ASC, created_at DESC);",
                "idx_orders_sorted", null);
        dump("CreateIndexSorted", r);

        assertTrue(r.getErrors().isEmpty(), "expected clean parse, got " + r.getErrors());
        assertEquals(Db2RoutineInfo.RoutineType.INDEX, r.getType());
        assertEquals("app", r.getSchema());
        assertEquals("idx_orders_sorted", r.getName());
        assertEquals("APP.ORDERS", r.getIndexTable().toUpperCase());
        assertTrue(r.isIndexUnique());
        assertEquals(List.of("customer_id ASC", "created_at DESC"), r.getIndexColumns());
    }
}
