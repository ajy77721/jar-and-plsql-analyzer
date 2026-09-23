package com.plsql.parser.multidb;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SqlServerProcedureParserTest {

    private SqlServerProcedureParser parser;

    @BeforeEach
    void setUp() {
        parser = new SqlServerProcedureParser();
    }

    // ── Signature extraction ─────────────────────────────────────────────────

    @Test
    void simpleProc_nameAndTypeExtracted() {
        String src = """
                CREATE PROCEDURE usp_get_orders
                AS
                BEGIN
                    SELECT * FROM Orders;
                END
                """;
        ParseResult r = parser.parse(src, "usp_get_orders", "dbo");
        ParsedObject obj = single(r);
        assertEquals("usp_get_orders", obj.getName());
        assertEquals("PROCEDURE", obj.getType());
    }

    @Test
    void createOrAlter_parsed() {
        String src = """
                CREATE OR ALTER PROCEDURE dbo.usp_sync
                AS
                BEGIN
                    SELECT 1;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "usp_sync", "dbo"));
        assertEquals("usp_sync", obj.getName());
        assertEquals("PROCEDURE", obj.getType());
    }

    @Test
    void procAbbreviation_typeMappedToProcedure() {
        String src = """
                CREATE PROC usp_short
                AS
                BEGIN
                    SELECT 1;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "usp_short", null));
        assertEquals("PROCEDURE", obj.getType());
    }

    @Test
    void function_typeIsFunction() {
        String src = """
                CREATE FUNCTION dbo.fn_calc(@amount DECIMAL(10,2))
                RETURNS DECIMAL(10,2)
                AS
                BEGIN
                    RETURN @amount * 1.1;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "fn_calc", "dbo"));
        assertEquals("fn_calc", obj.getName());
        assertEquals("FUNCTION", obj.getType());
    }

    @Test
    void schemaQualifiedProc_schemaExtracted() {
        String src = """
                CREATE PROCEDURE [sales].[usp_report]
                AS
                BEGIN
                    SELECT 1;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "usp_report", null));
        assertEquals("usp_report", obj.getName());
        assertEquals("sales", obj.getSchema());
    }

    @Test
    void fallbackName_usedWhenSignatureUnparseable() {
        String src = "-- no CREATE\nSELECT 1;";
        ParsedObject obj = single(parser.parse(src, "my_proc", null));
        assertEquals("my_proc", obj.getName());
        assertEquals("PROCEDURE", obj.getType());
    }

    // ── Parameter extraction ─────────────────────────────────────────────────

    @Test
    void atParam_extracted() {
        String src = """
                CREATE PROCEDURE usp_find_user
                    @user_id INT
                AS
                BEGIN
                    SELECT * FROM Users WHERE id = @user_id;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "usp_find_user", null));
        List<ParameterInfo> params = obj.getParameters();
        assertNotNull(params);
        assertFalse(params.isEmpty());
        ParameterInfo p = params.get(0);
        assertEquals("@user_id", p.getName());
        assertEquals("INT", p.getDataType());
        assertEquals("IN", p.getDirection());
    }

    @Test
    void outputParam_directionIsOUT() {
        String src = """
                CREATE PROCEDURE usp_count
                    @total INT OUTPUT
                AS
                BEGIN
                    SELECT @total = COUNT(*) FROM Orders;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "usp_count", null));
        List<ParameterInfo> params = obj.getParameters();
        assertNotNull(params);
        assertFalse(params.isEmpty());
        assertTrue(params.stream().anyMatch(p -> "OUT".equals(p.getDirection())));
    }

    @Test
    void outShorthand_directionIsOUT() {
        String src = """
                CREATE PROCEDURE usp_p
                    @result NVARCHAR(200) OUT
                AS
                BEGIN
                    SET @result = 'done';
                END
                """;
        ParsedObject obj = single(parser.parse(src, "usp_p", null));
        List<ParameterInfo> params = obj.getParameters();
        assertNotNull(params);
        assertTrue(params.stream().anyMatch(p -> "OUT".equals(p.getDirection())));
    }

    @Test
    void multipleParams_allExtracted() {
        String src = """
                CREATE PROCEDURE usp_transfer
                    @from_acc   INT,
                    @to_acc     INT,
                    @amount     DECIMAL(10,2),
                    @status     NVARCHAR(50) OUTPUT
                AS
                BEGIN
                    SELECT 1;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "usp_transfer", null));
        List<ParameterInfo> params = obj.getParameters();
        assertNotNull(params);
        assertEquals(4, params.size());
    }

    @Test
    void defaultParamValue_doesNotBreakParsing() {
        String src = """
                CREATE PROCEDURE usp_with_defaults
                    @page INT = 1,
                    @size INT = 20
                AS
                BEGIN
                    SELECT 1;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "usp_with_defaults", null));
        List<ParameterInfo> params = obj.getParameters();
        assertNotNull(params);
        assertEquals(2, params.size());
    }

    // ── DML extraction ───────────────────────────────────────────────────────

    @Test
    void selectStatement_extracted() {
        String src = """
                CREATE PROCEDURE p
                AS
                BEGIN
                    SELECT id, name FROM Customers WHERE active = 1;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "p", null));
        List<TableOperationInfo> ops = obj.getTableOperations();
        assertNotNull(ops);
        assertTrue(ops.stream().anyMatch(o -> "SELECT".equals(o.getOperation())));
    }

    @Test
    void insertStatement_extracted() {
        String src = """
                CREATE PROCEDURE p
                AS
                BEGIN
                    INSERT INTO AuditLog (action, ts) VALUES ('created', GETDATE());
                END
                """;
        ParsedObject obj = single(parser.parse(src, "p", null));
        List<TableOperationInfo> ops = obj.getTableOperations();
        assertNotNull(ops);
        assertTrue(ops.stream().anyMatch(o -> "INSERT".equals(o.getOperation())));
    }

    @Test
    void updateStatement_extracted() {
        String src = """
                CREATE PROCEDURE p
                AS
                BEGIN
                    UPDATE Orders SET status = 'shipped' WHERE id = 42;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "p", null));
        List<TableOperationInfo> ops = obj.getTableOperations();
        assertNotNull(ops);
        assertTrue(ops.stream().anyMatch(o -> "UPDATE".equals(o.getOperation())));
    }

    @Test
    void deleteStatement_extracted() {
        String src = """
                CREATE PROCEDURE p
                AS
                BEGIN
                    DELETE FROM TempData WHERE created_at < GETDATE();
                END
                """;
        ParsedObject obj = single(parser.parse(src, "p", null));
        List<TableOperationInfo> ops = obj.getTableOperations();
        assertNotNull(ops);
        assertTrue(ops.stream().anyMatch(o -> "DELETE".equals(o.getOperation())));
    }

    @Test
    void atVariableAssignmentInSelect_normalised() {
        // "SELECT @v = col FROM t" should not break extraction
        String src = """
                CREATE PROCEDURE p
                AS
                BEGIN
                    SELECT @count = COUNT(*) FROM Orders;
                END
                """;
        assertDoesNotThrow(() -> parser.parse(src, "p", null));
    }

    // ── EXEC / EXECUTE call extraction ───────────────────────────────────────

    @Test
    void execStatement_extracted() {
        String src = """
                CREATE PROCEDURE p
                AS
                BEGIN
                    EXEC usp_refresh_cache;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "p", null));
        List<CallInfo> calls = obj.getCalls();
        assertNotNull(calls);
        assertFalse(calls.isEmpty(), "Expected at least one EXEC call");
        assertEquals("usp_refresh_cache", calls.get(0).getName());
        // Cross-object calls are typed EXTERNAL so the dependency crawler follows them
        // (a "PROCEDURE" type would not be followed → no edges). SQL Server has no packages,
        // so every outbound routine call is EXTERNAL.
        assertEquals("EXTERNAL", calls.get(0).getType());
    }

    @Test
    void executeKeyword_alsoExtracted() {
        String src = """
                CREATE PROCEDURE p
                AS
                BEGIN
                    EXECUTE dbo.usp_audit;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "p", null));
        List<CallInfo> calls = obj.getCalls();
        assertNotNull(calls);
        assertFalse(calls.isEmpty());
        assertEquals("usp_audit", calls.get(0).getName());
        assertEquals("dbo", calls.get(0).getSchema());
    }

    @Test
    void multipleExecCalls_allExtracted() {
        String src = """
                CREATE PROCEDURE p
                AS
                BEGIN
                    EXEC usp_step1;
                    EXEC usp_step2;
                    EXEC usp_step3;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "p", null));
        List<CallInfo> calls = obj.getCalls();
        assertNotNull(calls);
        assertTrue(calls.size() >= 3);
    }

    // ── DECLARE @variable extraction ─────────────────────────────────────────

    @Test
    void declareVariable_extracted() {
        String src = """
                CREATE PROCEDURE p
                AS
                BEGIN
                    DECLARE @v_count INT;
                    DECLARE @v_name NVARCHAR(100);
                    SELECT @v_count;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "p", null));
        List<VariableInfo> vars = obj.getGlobalVariables();
        assertNotNull(vars);
        assertEquals(2, vars.size());
        assertTrue(vars.stream().anyMatch(v -> "@v_count".equals(v.getName())));
        assertTrue(vars.stream().anyMatch(v -> "@v_name".equals(v.getName())));
    }

    @Test
    void declareVariable_dataTypePreserved() {
        String src = """
                CREATE PROCEDURE p
                AS
                BEGIN
                    DECLARE @dt DATETIME;
                    SELECT @dt;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "p", null));
        List<VariableInfo> vars = obj.getGlobalVariables();
        assertNotNull(vars);
        assertFalse(vars.isEmpty());
        assertEquals("DATETIME", vars.get(0).getDataType());
    }

    @Test
    void declareVariable_lineNumberSet() {
        String src = "CREATE PROCEDURE p\nAS\nBEGIN\n    DECLARE @x INT;\nEND";
        ParsedObject obj = single(parser.parse(src, "p", null));
        List<VariableInfo> vars = obj.getGlobalVariables();
        assertNotNull(vars);
        assertFalse(vars.isEmpty());
        assertTrue(vars.get(0).getLine() >= 1);
    }

    // ── Line numbers / metadata ───────────────────────────────────────────────

    @Test
    void lineNumbers_set() {
        String src = """
                CREATE PROCEDURE p
                AS
                BEGIN
                    SELECT 1;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "p", null));
        assertEquals(1, obj.getLineStart());
        assertTrue(obj.getLineEnd() >= 5);
    }

    @Test
    void parseResult_fileNameSet() {
        ParseResult r = parser.parse("CREATE PROCEDURE p AS BEGIN SELECT 1; END", "p", null);
        assertEquals("p.sql", r.getFileName());
    }

    @Test
    void parseResult_parseTimeRecorded() {
        ParseResult r = parser.parse("CREATE PROCEDURE p AS BEGIN SELECT 1; END", "p", null);
        assertTrue(r.getParseTimeMs() >= 0);
    }

    @Test
    void emptyBody_noException() {
        String src = """
                CREATE PROCEDURE empty_proc
                AS
                BEGIN
                END
                """;
        assertDoesNotThrow(() -> parser.parse(src, "empty_proc", null));
    }

    @Test
    void noBeginBlock_bodyAfterAs() {
        // Some T-SQL procs omit BEGIN/END and are just a single statement after AS
        String src = "CREATE PROCEDURE p AS SELECT 1;";
        assertDoesNotThrow(() -> parser.parse(src, "p", null));
    }

    // ── Audit #8: IDENTITY columns ───────────────────────────────────────────

    @Test
    void identityColumn_seedAndIncrementCaptured() {
        String src = """
                CREATE PROCEDURE usp_make_temp
                AS
                BEGIN
                    CREATE TABLE #t (Id INT IDENTITY(1,1), Name VARCHAR(50));
                END
                """;
        ParseResult r = parser.parse(src, "usp_make_temp", "dbo");
        ParsedObject obj = single(r);
        assertEquals(1, obj.getIdentityColumns().size());
        IdentityColumnInfo ic = obj.getIdentityColumns().get(0);
        assertEquals("#t", ic.getTableName());
        assertEquals("Id", ic.getColumnName());
        assertEquals(1L, ic.getSeed());
        assertEquals(1L, ic.getIncrement());
        // Not mistaken for a real NEXT-VALUE-FOR sequence usage.
        assertTrue(obj.getCalls().stream().noneMatch(c -> "NEXTVAL".equals(c.getName())));
    }

    // ── Audit #7: predicates (WHERE) parity ──────────────────────────────────

    @Test
    void updateStatement_wherePredicatesCaptured() {
        String src = """
                CREATE PROCEDURE usp_update_order
                AS
                BEGIN
                    UPDATE erp.SalesOrder SET Status = 1 WHERE OrderId = 5 AND Total > 0;
                END
                """;
        ParseResult r = parser.parse(src, "usp_update_order", "dbo");
        ParsedObject obj = single(r);
        TableOperationInfo op = obj.getTableOperations().stream()
                .filter(t -> "UPDATE".equals(t.getOperation()))
                .findFirst().orElseThrow();
        assertFalse(op.getWhereFilters().isEmpty(), "Expected WHERE predicates on the UPDATE op");
    }

    // ── Helper ───────────────────────────────────────────────────────────────

    private ParsedObject single(ParseResult r) {
        assertNotNull(r);
        assertFalse(r.getObjects().isEmpty(), "Expected at least one ParsedObject");
        return r.getObjects().get(0);
    }
}
