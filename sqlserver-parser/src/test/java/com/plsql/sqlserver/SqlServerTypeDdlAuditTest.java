package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CREATE TYPE (alias type and table type) DDL support — SQL Server dialect.
 *
 * HISTORY (2026-09-19 audit -> fix): TYPE was not a lexer token, so "CREATE TYPE ..." was
 * greedily absorbed by {@code createIndex} (whose {@code indexModifier : ID} matched the literal
 * word TYPE). Error recovery then produced a partial {@code CreateIndexContext} with a null
 * {@code tableRef()}, and {@code SqlServerAnalysisVisitor.visitCreateIndex} NPE'd on
 * {@code ctx.tableRef().getText()} — a real crash, surfaced only as a misleading generic
 * "CreateIndexContext" error string by the outer catch in {@code SqlServerAnalysisEngine.parse}.
 *
 * FIXED by: a TYPE lexer keyword (re-admitted as an ordinary identifier via {@code exprAtom}),
 * a {@code createType} parser rule listed before {@code createIndex} in {@code compilationUnit}
 * covering both the FROM-alias and AS TABLE forms, a {@code visitCreateType} capture, a
 * {@code RoutineType.TYPE} model value, and a defensive null guard in {@code visitCreateIndex}.
 *
 * These are now hard pass/fail assertions.
 */
@DisplayName("SQL Server CREATE TYPE (alias / table type) DDL audit")
public class SqlServerTypeDdlAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_type", null);
    }

    @Test
    @DisplayName("CREATE TYPE dbo.PhoneNumber FROM VARCHAR(20) NOT NULL — alias type parses and is captured")
    void createAliasType_topLevel_parses() {
        String sql = "CREATE TYPE dbo.PhoneNumber FROM VARCHAR(20) NOT NULL;";
        SqlServerRoutineInfo r = parse(sql);

        System.out.println("[aliasType] name=" + r.getName() + " type=" + r.getType()
                + " errors=" + r.getErrors());

        assertTrue(r.getErrors().isEmpty(), "must parse cleanly, no crash: " + r.getErrors());
        assertEquals(SqlServerRoutineInfo.RoutineType.TYPE, r.getType());
        assertEquals("dbo", r.getSchema());
        assertEquals("PhoneNumber", r.getName());
        assertEquals("ALIAS", r.getTypeForm());
        assertEquals("VARCHAR(20)", r.getTypeBaseType());
        assertEquals(Boolean.TRUE, r.getTypeNotNull());
    }

    @Test
    @DisplayName("CREATE TYPE dbo.MyTableType AS TABLE (id INT, name VARCHAR(50)) — table type parses and is captured")
    void createTableType_topLevel_parses() {
        String sql = "CREATE TYPE dbo.MyTableType AS TABLE (id INT, name VARCHAR(50));";
        SqlServerRoutineInfo r = parse(sql);

        System.out.println("[tableType] name=" + r.getName() + " type=" + r.getType()
                + " errors=" + r.getErrors());

        assertTrue(r.getErrors().isEmpty(), "must parse cleanly, no crash: " + r.getErrors());
        assertEquals(SqlServerRoutineInfo.RoutineType.TYPE, r.getType());
        assertEquals("dbo", r.getSchema());
        assertEquals("MyTableType", r.getName());
        assertEquals("TABLE", r.getTypeForm());
        assertEquals(java.util.Arrays.asList("id", "name"), r.getTypeColumns());
    }

    @Test
    @DisplayName("dbo.MyTableType as a TVP parameter data type still parses fine (contrast case)")
    void tableTypeAsParameterDataType_stillParses() {
        // Contrast case confirming the gap is specifically the CREATE TYPE *definition*
        // statement, not the general use of a user type name as a data type reference.
        String sql = "CREATE PROCEDURE dbo.UseTvp (@p dbo.MyTableType READONLY) AS "
                + "BEGIN SELECT 1; END;";
        SqlServerRoutineInfo r = parse(sql);

        System.out.println("[tvpParam] name=" + r.getName() + " type=" + r.getType()
                + " errors=" + r.getErrors());

        assertTrue(r.getErrors().isEmpty(),
                "using dbo.MyTableType as a parameter data type is unaffected: " + r.getErrors());
        assertEquals("UseTvp", r.getName());
    }
}
