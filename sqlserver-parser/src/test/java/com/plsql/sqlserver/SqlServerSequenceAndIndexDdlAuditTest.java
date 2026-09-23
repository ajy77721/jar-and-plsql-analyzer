package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Top-level CREATE SEQUENCE / CREATE INDEX DDL support for the SQL Server dialect.
 *
 * Originally an audit file: the dialect had zero grammar/lexer support for either construct —
 * SqlServerLexer.g4 defined no SEQUENCE/INDEX token, compilationUnit was
 * {@code (routineDefinition | triggerDefinition | viewDefinition)? EOF}, and both statements
 * failed with "no viable alternative at input 'CREATE...'". The only sequence-related logic was
 * extractSequences(), which regex-scans a routine BODY for {@code NEXT VALUE FOR} *usages* and
 * structurally cannot see a standalone CREATE SEQUENCE definition.
 *
 * Both are now real constructs:
 *   - SqlServerLexer.g4 defines SEQUENCE and INDEX tokens (the surrounding option words —
 *     START/INCREMENT/MINVALUE/MAXVALUE/CYCLE/CACHE/NO, UNIQUE/CLUSTERED/NONCLUSTERED/INCLUDE —
 *     deliberately stay as plain ID so they remain usable as ordinary identifiers).
 *   - SqlServerParser.g4 adds createSequence and createIndex as compilationUnit alternatives,
 *     with permissive tails (createSequenceTail / createIndexTail) for the order-independent
 *     option lists, mirroring the viewHeader / triggerHeader approach.
 *   - SqlServerRoutineInfo.RoutineType gains SEQUENCE and INDEX, plus dedicated metadata fields.
 *   - SqlServerAnalysisVisitor gains visitCreateSequence / visitCreateIndex.
 *
 * The assertions below are therefore hard pass/fail checks, not evidence-gathering prints.
 */
@DisplayName("SQL Server CREATE SEQUENCE / CREATE INDEX top-level DDL support")
public class SqlServerSequenceAndIndexDdlAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_sequence_or_index", null);
    }

    @Test
    @DisplayName("CREATE SEQUENCE seq_orders AS INT START WITH 1 INCREMENT BY 1 parses as a top-level SEQUENCE")
    void createSequenceStandalone() {
        String sql = "CREATE SEQUENCE seq_orders AS INT START WITH 1 INCREMENT BY 1;";

        SqlServerRoutineInfo r = parse(sql);

        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        assertEquals(SqlServerRoutineInfo.RoutineType.SEQUENCE, r.getType());
        assertEquals("seq_orders", r.getName());
        assertEquals("INT", r.getSequenceDataType());
        assertEquals(1L, r.getSequenceStartWith());
        assertEquals(1L, r.getSequenceIncrementBy());
        assertEquals(1, r.getSequences().size());
        assertEquals("seq_orders", r.getSequences().get(0).getName());
    }

    @Test
    @DisplayName("CREATE SEQUENCE with schema, MINVALUE/MAXVALUE/CYCLE/CACHE options")
    void createSequenceFullOptions() {
        String sql = "CREATE SEQUENCE dbo.seq_invoice AS BIGINT START WITH 100 INCREMENT BY 5 "
                + "MINVALUE 1 MAXVALUE 999999 CYCLE CACHE 50;";

        SqlServerRoutineInfo r = parse(sql);

        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        assertEquals(SqlServerRoutineInfo.RoutineType.SEQUENCE, r.getType());
        assertEquals("dbo", r.getSchema());
        assertEquals("seq_invoice", r.getName());
        assertEquals("BIGINT", r.getSequenceDataType());
        assertEquals(100L, r.getSequenceStartWith());
        assertEquals(5L, r.getSequenceIncrementBy());
        assertEquals(1L, r.getSequenceMinValue());
        assertEquals(999999L, r.getSequenceMaxValue());
        assertEquals(Boolean.TRUE, r.getSequenceCycle());
        assertEquals(Boolean.TRUE, r.getSequenceCache());
        assertEquals(50L, r.getSequenceCacheSize());
    }

    @Test
    @DisplayName("CREATE SEQUENCE with NO MINVALUE / NO MAXVALUE / NO CYCLE / NO CACHE")
    void createSequenceNegatedOptions() {
        String sql = "CREATE SEQUENCE [seq_audit] NO MINVALUE NO MAXVALUE NO CYCLE NO CACHE;";

        SqlServerRoutineInfo r = parse(sql);

        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        assertEquals(SqlServerRoutineInfo.RoutineType.SEQUENCE, r.getType());
        assertEquals("seq_audit", r.getName());
        assertNull(r.getSequenceMinValue());
        assertNull(r.getSequenceMaxValue());
        assertEquals(Boolean.FALSE, r.getSequenceCycle());
        assertEquals(Boolean.FALSE, r.getSequenceCache());
    }

    @Test
    @DisplayName("CREATE SEQUENCE with no options at all")
    void createSequenceBare() {
        SqlServerRoutineInfo r = parse("CREATE SEQUENCE seq_min;");

        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        assertEquals(SqlServerRoutineInfo.RoutineType.SEQUENCE, r.getType());
        assertEquals("seq_min", r.getName());
        assertNull(r.getSequenceStartWith());
    }

    @Test
    @DisplayName("CREATE INDEX idx_orders_status ON orders(status) parses as a top-level INDEX")
    void createIndexStandalone() {
        SqlServerRoutineInfo r = parse("CREATE INDEX idx_orders_status ON orders(status);");

        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        assertEquals(SqlServerRoutineInfo.RoutineType.INDEX, r.getType());
        assertEquals("idx_orders_status", r.getName());
        assertEquals("orders", r.getIndexTable());
        assertEquals(java.util.List.of("status"), r.getIndexColumns());
        assertFalse(r.isIndexUnique());
        assertNull(r.getIndexClustering());
    }

    @Test
    @DisplayName("CREATE UNIQUE INDEX idx_orders_email ON orders(email) sets the unique flag")
    void createUniqueIndexStandalone() {
        SqlServerRoutineInfo r = parse("CREATE UNIQUE INDEX idx_orders_email ON orders(email);");

        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        assertEquals(SqlServerRoutineInfo.RoutineType.INDEX, r.getType());
        assertEquals("idx_orders_email", r.getName());
        assertEquals("orders", r.getIndexTable());
        assertEquals(java.util.List.of("email"), r.getIndexColumns());
        assertTrue(r.isIndexUnique());
    }

    @Test
    @DisplayName("CREATE INDEX on composite key columns captures every column in order")
    void createCompositeIndexStandalone() {
        SqlServerRoutineInfo r = parse(
                "CREATE INDEX idx_orders_composite ON orders(customer_id, status, created_at);");

        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        assertEquals(SqlServerRoutineInfo.RoutineType.INDEX, r.getType());
        assertEquals("idx_orders_composite", r.getName());
        assertEquals("orders", r.getIndexTable());
        assertEquals(java.util.List.of("customer_id", "status", "created_at"), r.getIndexColumns());
    }

    @Test
    @DisplayName("CREATE UNIQUE NONCLUSTERED INDEX with schema-qualified table, ASC/DESC, INCLUDE and a filter")
    void createIndexWithIncludeAndFilter() {
        SqlServerRoutineInfo r = parse(
                "CREATE UNIQUE NONCLUSTERED INDEX [ix_orders_cust] ON dbo.orders(customer_id ASC, created_at DESC) "
                        + "INCLUDE (total_amount, [status]) WHERE status <> 'CANCELLED';");

        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        assertEquals(SqlServerRoutineInfo.RoutineType.INDEX, r.getType());
        assertEquals("ix_orders_cust", r.getName());
        assertTrue(r.isIndexUnique());
        assertEquals("NONCLUSTERED", r.getIndexClustering());
        assertEquals("dbo.orders", r.getIndexTable());
        assertEquals(java.util.List.of("customer_id", "created_at"), r.getIndexColumns());
        assertEquals(java.util.List.of("total_amount", "status"), r.getIndexIncludeColumns());
        assertEquals("status <> 'CANCELLED'", r.getIndexFilter());
    }
}
