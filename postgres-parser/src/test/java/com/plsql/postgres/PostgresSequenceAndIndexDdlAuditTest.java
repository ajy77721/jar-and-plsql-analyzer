package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.PostgresRoutineInfo;
import org.junit.jupiter.api.Test;

/**
 * READ-ONLY AUDIT — Postgres CREATE SEQUENCE and CREATE INDEX support.
 *
 * Grammar/visitor were read fresh before writing this:
 *   - postgres-parser/src/main/antlr4/com/plsql/postgres/grammar/PostgresParser.g4
 *   - postgres-parser/src/main/java/com/plsql/postgres/visitor/PostgresAnalysisVisitor.java
 *
 * Findings:
 *
 *  1. compilationUnit is:
 *         compilationUnit : (routineDefinition | doStatement | createTrigger | createView)? EOF ;
 *     There is NO createSequence or createIndex alternative here (unlike CREATE TRIGGER /
 *     CREATE VIEW which were added this session). grep -ni "sequence" against
 *     PostgresParser.g4 returns ZERO matches anywhere in the file. grep -ni "index" returns
 *     exactly one hit, a comment at line 601 ("... CREATE INDEX ..., etc.") documenting that
 *     CREATE INDEX is only handled by the generic permissive catch-all rule:
 *         createStatement : CREATE (~SEMI)* SEMI ;
 *     `createStatement` is only reachable via the inner `statement` rule (used for routine
 *     BODY statements, i.e. inside a BEGIN...END block of a routineDefinition) — it is NOT one
 *     of the four alternatives of the top-level `compilationUnit` rule. So a standalone
 *     top-level "CREATE SEQUENCE ...;" or "CREATE INDEX ...;" (not inside a function/procedure
 *     body) cannot match compilationUnit at all.
 *
 *  2. The visitor (PostgresAnalysisVisitor) has exactly one override relevant to createStatement:
 *     visitCreateStatement(), which applies a CREATE_TABLE_NAME_PATTERN regex over the raw text
 *     and only recognizes CREATE [TEMP|TEMPORARY|UNLOGGED] TABLE. There is no
 *     visitCreateSequence / visitCreateIndex anywhere, and no regex/branch for SEQUENCE or
 *     INDEX. So even in the one context where createStatement is reachable (inside a routine
 *     body), CREATE SEQUENCE / CREATE INDEX text matches the permissive catch-all but produces
 *     zero captured objects (no PostgresTableOpInfo, no sequence-specific model) — confirmed
 *     empirically below.
 *
 *  3. As standalone top-level DDL (the realistic case for a sequence/index migration script),
 *     since none of compilationUnit's four alternatives can match a bare CREATE SEQUENCE /
 *     CREATE INDEX, ANTLR's error-recovery mechanism kicks in (same mechanism observed for
 *     CREATE TRIGGER/VIEW before those were wired in): PostgresAnalysisEngine.parse() does NOT
 *     throw, but returns an emptyish PostgresRoutineInfo with getErrors() populated.
 */
class PostgresSequenceAndIndexDdlAuditTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private PostgresRoutineInfo parse(String src) {
        return engine.parse(src, "test_obj", null);
    }

    // ─────────────────────────────────────────────────────────────────────
    // CREATE SEQUENCE — top-level statement
    // ─────────────────────────────────────────────────────────────────────

    @Test
    void createSequence_topLevel_isCapturedAsSequenceObject() {
        String sql =
                "CREATE SEQUENCE seq_orders START WITH 1 INCREMENT BY 1 CACHE 20 NO CYCLE;";
        PostgresRoutineInfo r = parse(sql);

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "CREATE SEQUENCE now has its own compilationUnit alternative: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals(
                PostgresRoutineInfo.RoutineType.SEQUENCE, r.getType());
        org.junit.jupiter.api.Assertions.assertEquals("seq_orders", r.getName());
        org.junit.jupiter.api.Assertions.assertNull(r.getSchema());
        org.junit.jupiter.api.Assertions.assertEquals("1", r.getSequenceStart());
        org.junit.jupiter.api.Assertions.assertEquals("1", r.getSequenceIncrement());
        org.junit.jupiter.api.Assertions.assertEquals("20", r.getSequenceCache());
        org.junit.jupiter.api.Assertions.assertEquals(Boolean.FALSE, r.getSequenceCycle());
        // Options that were not written stay null, so "unspecified" is distinguishable.
        org.junit.jupiter.api.Assertions.assertNull(r.getSequenceMinValue());
        org.junit.jupiter.api.Assertions.assertNull(r.getSequenceMaxValue());
        org.junit.jupiter.api.Assertions.assertNull(r.getSequenceDataType());
    }

    @Test
    void createSequence_schemaQualifiedWithAllOptions() {
        String sql =
                "CREATE SEQUENCE app.seq_big AS bigint START 100 INCREMENT 5 "
                        + "MINVALUE 10 MAXVALUE 9999 CYCLE CACHE 3;";
        PostgresRoutineInfo r = parse(sql);

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals(
                PostgresRoutineInfo.RoutineType.SEQUENCE, r.getType());
        org.junit.jupiter.api.Assertions.assertEquals("app", r.getSchema());
        org.junit.jupiter.api.Assertions.assertEquals("seq_big", r.getName());
        org.junit.jupiter.api.Assertions.assertEquals("bigint", r.getSequenceDataType());
        // START/INCREMENT parse with the optional WITH/BY keyword omitted, too.
        org.junit.jupiter.api.Assertions.assertEquals("100", r.getSequenceStart());
        org.junit.jupiter.api.Assertions.assertEquals("5", r.getSequenceIncrement());
        org.junit.jupiter.api.Assertions.assertEquals("10", r.getSequenceMinValue());
        org.junit.jupiter.api.Assertions.assertEquals("9999", r.getSequenceMaxValue());
        org.junit.jupiter.api.Assertions.assertEquals(Boolean.TRUE, r.getSequenceCycle());
        org.junit.jupiter.api.Assertions.assertEquals("3", r.getSequenceCache());
    }

    @Test
    void createSequence_minimalAndNegativeIncrementAndNoBounds() {
        PostgresRoutineInfo bare = parse("CREATE SEQUENCE seq_plain;");
        org.junit.jupiter.api.Assertions.assertTrue(bare.getErrors().isEmpty(),
                "" + bare.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals("seq_plain", bare.getName());
        org.junit.jupiter.api.Assertions.assertEquals(
                PostgresRoutineInfo.RoutineType.SEQUENCE, bare.getType());
        org.junit.jupiter.api.Assertions.assertNull(bare.getSequenceCycle());

        PostgresRoutineInfo desc = parse(
                "CREATE SEQUENCE IF NOT EXISTS seq_down INCREMENT BY -1 "
                        + "NO MINVALUE NO MAXVALUE NO CYCLE;");
        org.junit.jupiter.api.Assertions.assertTrue(desc.getErrors().isEmpty(),
                "" + desc.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals("seq_down", desc.getName());
        org.junit.jupiter.api.Assertions.assertEquals("-1", desc.getSequenceIncrement());
        org.junit.jupiter.api.Assertions.assertEquals("NO MINVALUE", desc.getSequenceMinValue());
        org.junit.jupiter.api.Assertions.assertEquals("NO MAXVALUE", desc.getSequenceMaxValue());
        org.junit.jupiter.api.Assertions.assertEquals(Boolean.FALSE, desc.getSequenceCycle());
    }

    @Test
    void createSequence_ownedByTailDoesNotBreakTheParse() {
        // OWNED BY is a lower-priority option consumed by the permissive sequenceTail; the
        // options written BEFORE it must still be captured and the statement must not error.
        PostgresRoutineInfo r = parse(
                "CREATE SEQUENCE seq_owned START WITH 7 OWNED BY orders.id;");
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals("seq_owned", r.getName());
        org.junit.jupiter.api.Assertions.assertEquals("7", r.getSequenceStart());
    }

    @Test
    void sequenceKeywordsRemainUsableAsOrdinaryIdentifiers() {
        // Promoting CACHE/CYCLE/START/NO/INCREMENT/MINVALUE/MAXVALUE/SEQUENCE from ID to real
        // keyword tokens must not regress routines that use one of them as a column/variable.
        String sql =
                "CREATE FUNCTION f() RETURNS int AS $$\n" +
                "BEGIN\n" +
                "  SELECT cache, cycle, start, no, increment FROM sequence WHERE minvalue > 0;\n" +
                "  RETURN 1;\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql;";
        PostgresRoutineInfo r = parse(sql);
        org.junit.jupiter.api.Assertions.assertEquals("f", r.getName());
        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                        .anyMatch(op -> "sequence".equalsIgnoreCase(op.getTableName())),
                "a table literally named 'sequence' is still resolved: " + r.getTableOps());
    }

    // ─────────────────────────────────────────────────────────────────────
    // CREATE INDEX — top-level statement
    // ─────────────────────────────────────────────────────────────────────

    @Test
    void createIndex_topLevel_isCapturedAsIndexObject() {
        String sql = "CREATE INDEX idx_orders_status ON orders(status);";
        PostgresRoutineInfo r = parse(sql);

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "CREATE INDEX now has its own compilationUnit alternative: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals(
                PostgresRoutineInfo.RoutineType.INDEX, r.getType());
        org.junit.jupiter.api.Assertions.assertEquals("idx_orders_status", r.getName());
        org.junit.jupiter.api.Assertions.assertEquals("idx_orders_status", r.getIndexName());
        org.junit.jupiter.api.Assertions.assertEquals("orders", r.getIndexTable());
        org.junit.jupiter.api.Assertions.assertEquals(
                java.util.List.of("status"), r.getIndexColumns());
        org.junit.jupiter.api.Assertions.assertFalse(r.isIndexUnique());
        org.junit.jupiter.api.Assertions.assertFalse(r.isIndexConcurrently());
        org.junit.jupiter.api.Assertions.assertNull(r.getIndexWhere());
        org.junit.jupiter.api.Assertions.assertNull(r.getIndexMethod());
        // The indexed table is also recorded as a table op, so the index shows up as a
        // dependency on "orders" (same treatment CREATE TRIGGER gives its target table).
        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                        .anyMatch(op -> "orders".equalsIgnoreCase(op.getTableName())),
                "the index's target table 'orders' is captured: " + r.getTableOps());
    }

    @Test
    void createUniqueIndex_topLevel_setsUniqueFlag() {
        PostgresRoutineInfo r = parse("CREATE UNIQUE INDEX idx_orders_email ON orders(email);");

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals(
                PostgresRoutineInfo.RoutineType.INDEX, r.getType());
        org.junit.jupiter.api.Assertions.assertEquals("idx_orders_email", r.getName());
        org.junit.jupiter.api.Assertions.assertTrue(r.isIndexUnique());
        org.junit.jupiter.api.Assertions.assertEquals("orders", r.getIndexTable());
        org.junit.jupiter.api.Assertions.assertEquals(
                java.util.List.of("email"), r.getIndexColumns());
    }

    @Test
    void createIndex_compositeColumns_capturesEveryColumnInOrder() {
        PostgresRoutineInfo r = parse(
                "CREATE INDEX idx_orders_composite ON orders(customer_id, status, created_at);");

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals("idx_orders_composite", r.getName());
        org.junit.jupiter.api.Assertions.assertEquals("orders", r.getIndexTable());
        org.junit.jupiter.api.Assertions.assertEquals(
                java.util.List.of("customer_id", "status", "created_at"), r.getIndexColumns());
    }

    @Test
    void createIndex_partialIndexCapturesWhereClause() {
        PostgresRoutineInfo r = parse(
                "CREATE INDEX idx_open_orders ON sales.orders (created_at) "
                        + "WHERE status = 'OPEN';");

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals("idx_open_orders", r.getName());
        org.junit.jupiter.api.Assertions.assertEquals("orders", r.getIndexTable());
        org.junit.jupiter.api.Assertions.assertEquals("sales", r.getIndexTableSchema());
        org.junit.jupiter.api.Assertions.assertEquals(
                java.util.List.of("created_at"), r.getIndexColumns());
        org.junit.jupiter.api.Assertions.assertNotNull(r.getIndexWhere());
        org.junit.jupiter.api.Assertions.assertTrue(
                r.getIndexWhere().replaceAll("\\s+", " ").equalsIgnoreCase("status = 'OPEN'"),
                "partial-index predicate text: " + r.getIndexWhere());
    }

    @Test
    void createIndex_usingMethodConcurrentlyAndExpressionElement() {
        PostgresRoutineInfo r = parse(
                "CREATE INDEX CONCURRENTLY idx_cust_lower ON customers USING btree "
                        + "(lower(email), created_at DESC);");

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals("idx_cust_lower", r.getName());
        org.junit.jupiter.api.Assertions.assertTrue(r.isIndexConcurrently());
        org.junit.jupiter.api.Assertions.assertEquals("btree", r.getIndexMethod());
        org.junit.jupiter.api.Assertions.assertEquals("customers", r.getIndexTable());
        // A DESC sort suffix must not be swallowed into the column expression.
        org.junit.jupiter.api.Assertions.assertEquals(
                java.util.List.of("lower(email)", "created_at"), r.getIndexColumns());
    }

    @Test
    void createIndex_anonymousIndexNameIsOptional() {
        // PostgreSQL allows omitting the index name entirely.
        PostgresRoutineInfo r = parse("CREATE INDEX ON orders (status);");

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals(
                PostgresRoutineInfo.RoutineType.INDEX, r.getType());
        org.junit.jupiter.api.Assertions.assertNull(r.getIndexName(),
                "no name was written, so indexName stays null");
        org.junit.jupiter.api.Assertions.assertEquals("orders", r.getIndexTable());
        org.junit.jupiter.api.Assertions.assertEquals(
                java.util.List.of("status"), r.getIndexColumns());
    }

    @Test
    void indexKeywordsRemainUsableAsOrdinaryIdentifiers() {
        String sql =
                "CREATE FUNCTION g() RETURNS int AS $$\n" +
                "BEGIN\n" +
                "  SELECT index, unique, concurrently FROM idx_meta;\n" +
                "  RETURN 1;\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql;";
        PostgresRoutineInfo r = parse(sql);
        org.junit.jupiter.api.Assertions.assertEquals("g", r.getName());
        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                        .anyMatch(op -> "idx_meta".equalsIgnoreCase(op.getTableName())),
                "INDEX/UNIQUE/CONCURRENTLY still work as column names: " + r.getTableOps());
    }

    // ─────────────────────────────────────────────────────────────────────
    // Same DDL nested inside a routine BODY (where createStatement IS reachable) — to confirm
    // the permissive catch-all swallows it silently rather than producing any sequence/index
    // capture, distinguishing this from the top-level parse-error case above.
    // ─────────────────────────────────────────────────────────────────────

    @Test
    void createSequence_insideRoutineBody_swallowedByPermissiveCatchAll() {
        String sql =
                "CREATE PROCEDURE setup_seq() AS $$\n" +
                "BEGIN\n" +
                "  CREATE SEQUENCE seq_orders START WITH 1 INCREMENT BY 1 CACHE 20 NO CYCLE;\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql;";
        PostgresRoutineInfo r = parse(sql);

        System.out.println("[createSequenceInBody] name=" + r.getName() + " type=" + r.getType()
                + " tableOps=" + r.getTableOps() + " errors=" + r.getErrors());

        // The outer CREATE PROCEDURE shell parses fine (ordinary routineDefinition).
        org.junit.jupiter.api.Assertions.assertEquals("setup_seq", r.getName());
        // The inner CREATE SEQUENCE matches createStatement's permissive catch-all, but
        // visitCreateStatement only recognizes CREATE TABLE text, so it produces zero capture.
        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                        .noneMatch(op -> "seq_orders".equalsIgnoreCase(op.getTableName())),
                "CREATE SEQUENCE inside a routine body is swallowed with no capture at all");
    }

    @Test
    void createIndex_insideRoutineBody_swallowedByPermissiveCatchAll() {
        String sql =
                "CREATE PROCEDURE setup_idx() AS $$\n" +
                "BEGIN\n" +
                "  CREATE INDEX idx_orders_status ON orders(status);\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql;";
        PostgresRoutineInfo r = parse(sql);

        System.out.println("[createIndexInBody] name=" + r.getName() + " type=" + r.getType()
                + " tableOps=" + r.getTableOps() + " errors=" + r.getErrors());

        org.junit.jupiter.api.Assertions.assertEquals("setup_idx", r.getName());
        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                        .noneMatch(op -> "orders".equalsIgnoreCase(op.getTableName())),
                "CREATE INDEX inside a routine body is swallowed with no capture of its "
                        + "target table 'orders' at all");
    }
}
