package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.Db2RoutineInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DB2 CREATE TYPE (structured type) and CREATE DISTINCT TYPE support (added 2026-09-19;
 * this file started as a read-only audit of the gap and is now its regression suite).
 *
 * Before: no TYPE token existed in Db2Lexer.g4 and compilationUnit's alternatives all
 * required a different literal after CREATE (PROCEDURE/FUNCTION/TRIGGER/VIEW/SEQUENCE/
 * INDEX), so both forms were a clean "no viable alternative" failure and RoutineType had
 * no member to hold a capture.
 *
 * After: a `createType` rule is wired into compilationUnit and
 * Db2AnalysisVisitor.visitCreateType records the type's identity plus its kind —
 * STRUCTURED (attribute list + MODE DB2SQL) or DISTINCT (base type [WITH COMPARISONS]).
 */
@DisplayName("DB2 CREATE TYPE (structured type) / CREATE DISTINCT TYPE DDL")
public class Db2TypeDdlAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private Db2RoutineInfo parse(String src) {
        return engine.parse(src, "test_type", null);
    }

    @Test
    @DisplayName("CREATE TYPE t_address AS (street VARCHAR(100), city VARCHAR(50)) MODE DB2SQL — structured type")
    void createStructuredType() {
        Db2RoutineInfo r = parse(
                "CREATE TYPE t_address AS (street VARCHAR(100), city VARCHAR(50)) MODE DB2SQL;");

        System.out.println("[structuredType] name=" + r.getName() + " type=" + r.getType()
                + " kind=" + r.getTypeKind() + " attrs=" + r.getTypeAttributes()
                + " mode=" + r.getTypeMode() + " errors=" + r.getErrors());

        assertTrue(r.getErrors().isEmpty(), "expected a clean parse, got " + r.getErrors());
        assertEquals(Db2RoutineInfo.RoutineType.TYPE, r.getType());
        assertEquals("t_address", r.getName());
        assertEquals("STRUCTURED", r.getTypeKind());
        assertEquals(2, r.getTypeAttributes().size());
        assertEquals("street VARCHAR(100)", r.getTypeAttributes().get(0));
        assertEquals("city VARCHAR(50)", r.getTypeAttributes().get(1));
        assertEquals("DB2SQL", r.getTypeMode().toUpperCase());
        assertNull(r.getTypeBaseType(), "a structured type has no base type");
        // A type is a schema object, not a data flow — no table op is invented for it.
        assertTrue(r.getTableOps().isEmpty());
    }

    @Test
    @DisplayName("CREATE DISTINCT TYPE us_dollar AS DECIMAL(10,2) WITH COMPARISONS — distinct type")
    void createDistinctType() {
        Db2RoutineInfo r = parse("CREATE DISTINCT TYPE us_dollar AS DECIMAL(10,2) WITH COMPARISONS;");

        System.out.println("[distinctType] name=" + r.getName() + " type=" + r.getType()
                + " kind=" + r.getTypeKind() + " base=" + r.getTypeBaseType()
                + " withComparisons=" + r.isTypeWithComparisons() + " errors=" + r.getErrors());

        assertTrue(r.getErrors().isEmpty(), "expected a clean parse, got " + r.getErrors());
        assertEquals(Db2RoutineInfo.RoutineType.TYPE, r.getType());
        assertEquals("us_dollar", r.getName());
        assertEquals("DISTINCT", r.getTypeKind());
        assertEquals("DECIMAL(10,2)", r.getTypeBaseType().toUpperCase());
        assertTrue(r.isTypeWithComparisons());
        assertTrue(r.getTypeAttributes().isEmpty());
    }

    @Test
    @DisplayName("CREATE DISTINCT TYPE without WITH COMPARISONS, schema-qualified")
    void createDistinctTypeQualifiedNoComparisons() {
        Db2RoutineInfo r = parse("CREATE DISTINCT TYPE app.euro AS DECIMAL(12,2);");

        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertEquals("app", r.getSchema());
        assertEquals("euro", r.getName());
        assertEquals("DISTINCT", r.getTypeKind());
        assertFalse(r.isTypeWithComparisons());
    }

    @Test
    @DisplayName("TYPE remains usable as an ordinary identifier after becoming a lexer token")
    void typeKeywordStillUsableAsIdentifier() {
        Db2RoutineInfo r = engine.parse(
                "CREATE PROCEDURE p() BEGIN DECLARE type INT; SET type = 1; END;", "p", null);
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertEquals(Db2RoutineInfo.RoutineType.PROCEDURE, r.getType());
        assertTrue(r.getVariables().stream().anyMatch(v -> "type".equalsIgnoreCase(v.getName())),
                "variable named 'type' must still be declarable");
    }
}
