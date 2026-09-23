package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.PostgresRoutineInfo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PostgreSQL CREATE TYPE (enum / composite / shell) and CREATE DOMAIN support.
 *
 * ORIGINAL AUDIT FINDING (kept for context): there was no TYPE, ENUM or DOMAIN lexer token and no
 * createType/createDomain parser rule, and compilationUnit had no alternative for either — so a
 * standalone CREATE TYPE / CREATE DOMAIN was a hard parse failure with nothing captured, and
 * RoutineType had no member able to hold the result.
 *
 * FIXED: `createType` / `createDomain` (PostgresParser.g4) are now compilationUnit alternatives,
 * RoutineType gained TYPE and DOMAIN, and visitCreateEnumType / visitCreateCompositeType /
 * visitCreateShellType / visitCreateDomain capture the name plus the declared shape.
 */
class PostgresTypeDdlAuditTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private PostgresRoutineInfo parse(String src) {
        return engine.parse(src, "test_obj", null);
    }

    private static PostgresRoutineInfo dump(String label, PostgresRoutineInfo r) {
        System.out.println("[" + label + "] name=" + r.getName() + " type=" + r.getType()
                + " kind=" + r.getTypeKind() + " labels=" + r.getTypeEnumLabels()
                + " attrs=" + r.getTypeAttributes() + " base=" + r.getDomainBaseType()
                + " check=" + r.getDomainCheck() + " errors=" + r.getErrors());
        return r;
    }

    @Test
    void createEnumType_topLevel_captured() {
        PostgresRoutineInfo r = dump("enumType",
                parse("CREATE TYPE status_enum AS ENUM ('ACTIVE','INACTIVE','PENDING');"));

        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertEquals(PostgresRoutineInfo.RoutineType.TYPE, r.getType());
        assertEquals("ENUM", r.getTypeKind());
        assertEquals("status_enum", r.getName());
        assertEquals(java.util.List.of("ACTIVE", "INACTIVE", "PENDING"), r.getTypeEnumLabels());
    }

    @Test
    void createEnumType_schemaQualified_captured() {
        PostgresRoutineInfo r = dump("enumTypeQualified",
                parse("CREATE TYPE sales.order_state AS ENUM ('NEW','SHIPPED');"));

        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertEquals("sales", r.getSchema());
        assertEquals("order_state", r.getName());
        assertEquals(java.util.List.of("NEW", "SHIPPED"), r.getTypeEnumLabels());
    }

    @Test
    void createCompositeType_topLevel_captured() {
        PostgresRoutineInfo r = dump("compositeType",
                parse("CREATE TYPE point_type AS (x INTEGER, y INTEGER);"));

        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertEquals(PostgresRoutineInfo.RoutineType.TYPE, r.getType());
        assertEquals("COMPOSITE", r.getTypeKind());
        assertEquals("point_type", r.getName());
        assertEquals(java.util.List.of("x INTEGER", "y INTEGER"), r.getTypeAttributes());
    }

    @Test
    void createShellType_topLevel_captured() {
        PostgresRoutineInfo r = dump("shellType", parse("CREATE TYPE opaque_thing;"));

        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertEquals(PostgresRoutineInfo.RoutineType.TYPE, r.getType());
        assertEquals("SHELL", r.getTypeKind());
        assertEquals("opaque_thing", r.getName());
    }

    @Test
    void createDomain_topLevel_captured() {
        PostgresRoutineInfo r = dump("domain",
                parse("CREATE DOMAIN positive_int AS INTEGER CHECK (VALUE > 0);"));

        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertEquals(PostgresRoutineInfo.RoutineType.DOMAIN, r.getType());
        assertEquals("DOMAIN", r.getTypeKind());
        assertEquals("positive_int", r.getName());
        assertEquals("INTEGER", r.getDomainBaseType());
        assertTrue(r.getDomainCheck() != null && r.getDomainCheck().toUpperCase().contains("VALUE"),
                "CHECK predicate captured: " + r.getDomainCheck());
    }

    @Test
    void createDomain_withoutAs_andNotNullDefault_captured() {
        PostgresRoutineInfo r = dump("domainNoAs", parse(
                "CREATE DOMAIN email_addr VARCHAR(255) NOT NULL DEFAULT 'x@y.z' "
                        + "CONSTRAINT chk_at CHECK (VALUE LIKE '%@%');"));

        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertEquals(PostgresRoutineInfo.RoutineType.DOMAIN, r.getType());
        assertEquals("email_addr", r.getName());
        assertEquals("VARCHAR(255)", r.getDomainBaseType());
        assertTrue(r.getDomainCheck() != null && r.getDomainCheck().contains("@"),
                "named CHECK predicate captured: " + r.getDomainCheck());
    }

    @Test
    void enumTypeKeywords_stillUsableAsIdentifiers() {
        // TYPE / ENUM / DOMAIN became lexer tokens for the rules above; they are also listed in
        // the permissive `id` rule, so ordinary source that uses them as names must be unaffected.
        PostgresRoutineInfo r = dump("keywordsAsIds", parse(
                "CREATE PROCEDURE p() AS $$\n"
                        + "DECLARE type INT;\n"
                        + "BEGIN\n"
                        + "  SELECT domain FROM enum;\n"
                        + "END;\n"
                        + "$$ LANGUAGE plpgsql;"));

        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertEquals("p", r.getName());
        assertTrue(r.getTableOps().stream().anyMatch(op -> "enum".equalsIgnoreCase(op.getTableName())),
                "a table literally named 'enum' still parses: " + r.getTableOps());
    }
}
