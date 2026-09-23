package com.plsql.parser.multidb;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MySqlProcedureParserTest {

    private MySqlProcedureParser parser;

    @BeforeEach
    void setUp() {
        parser = new MySqlProcedureParser();
    }

    // ── Signature extraction ─────────────────────────────────────────────────

    @Test
    void simpleProc_nameAndTypeExtracted() {
        String src = """
                CREATE PROCEDURE get_orders()
                BEGIN
                  SELECT * FROM orders;
                END
                """;
        ParseResult r = parser.parse(src, "get_orders", "mydb");
        ParsedObject obj = single(r);
        assertEquals("get_orders", obj.getName());
        assertEquals("PROCEDURE", obj.getType());
    }

    @Test
    void function_typeIsFunction() {
        String src = """
                CREATE FUNCTION calc_total(amount DECIMAL(10,2))
                RETURNS DECIMAL(10,2)
                BEGIN
                  RETURN amount * 1.1;
                END
                """;
        ParseResult r = parser.parse(src, "calc_total", null);
        ParsedObject obj = single(r);
        assertEquals("calc_total", obj.getName());
        assertEquals("FUNCTION", obj.getType());
    }

    @Test
    void definerIsStripped_nameStillParsed() {
        String src = """
                CREATE DEFINER=`root`@`localhost` PROCEDURE `mydb`.`usp_sync`()
                BEGIN
                  SELECT 1;
                END
                """;
        ParseResult r = parser.parse(src, "usp_sync", "mydb");
        ParsedObject obj = single(r);
        assertEquals("usp_sync", obj.getName());
        assertEquals("PROCEDURE", obj.getType());
    }

    @Test
    void fallbackName_usedWhenSignatureUnparseable() {
        String src = "-- no CREATE statement here\nSELECT 1;";
        ParseResult r = parser.parse(src, "my_proc", null);
        ParsedObject obj = single(r);
        assertEquals("my_proc", obj.getName());
        assertEquals("PROCEDURE", obj.getType());
    }

    // ── Parameter extraction ─────────────────────────────────────────────────

    @Test
    void inParam_directionIsIN() {
        String src = """
                CREATE PROCEDURE find_user(IN user_id INT)
                BEGIN
                  SELECT * FROM users WHERE id = user_id;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "find_user", null));
        List<ParameterInfo> params = obj.getParameters();
        assertNotNull(params);
        assertEquals(1, params.size());
        assertEquals("user_id", params.get(0).getName());
        assertEquals("INT", params.get(0).getDataType());
        assertEquals("IN", params.get(0).getDirection());
    }

    @Test
    void outParam_directionIsOUT() {
        String src = """
                CREATE PROCEDURE get_count(OUT total INT)
                BEGIN
                  SELECT COUNT(*) INTO total FROM orders;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "get_count", null));
        List<ParameterInfo> params = obj.getParameters();
        assertNotNull(params);
        assertEquals(1, params.size());
        assertEquals("OUT", params.get(0).getDirection());
    }

    @Test
    void inoutParam_directionIsINOUT() {
        String src = """
                CREATE PROCEDURE adjust(INOUT balance DECIMAL(10,2))
                BEGIN
                  SET balance = balance + 100;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "adjust", null));
        List<ParameterInfo> params = obj.getParameters();
        assertNotNull(params);
        assertEquals(1, params.size());
        assertEquals("INOUT", params.get(0).getDirection());
    }

    @Test
    void multipleParams_allExtracted() {
        String src = """
                CREATE PROCEDURE transfer(IN from_acc INT, IN to_acc INT, INOUT amount DECIMAL(10,2), OUT status VARCHAR(20))
                BEGIN
                  SELECT 1;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "transfer", null));
        List<ParameterInfo> params = obj.getParameters();
        assertNotNull(params);
        assertEquals(4, params.size());
        assertEquals("from_acc", params.get(0).getName());
        assertEquals("to_acc",   params.get(1).getName());
        assertEquals("amount",   params.get(2).getName());
        assertEquals("status",   params.get(3).getName());
    }

    @Test
    void noParams_emptyParamList() {
        String src = """
                CREATE PROCEDURE rebuild_cache()
                BEGIN
                  DELETE FROM cache;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "rebuild_cache", null));
        List<ParameterInfo> params = obj.getParameters();
        assertNotNull(params);
        assertTrue(params.isEmpty());
    }

    // ── DML extraction ───────────────────────────────────────────────────────

    @Test
    void selectStatement_extracted() {
        String src = """
                CREATE PROCEDURE p()
                BEGIN
                  SELECT id, name FROM customers WHERE active = 1;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "p", null));
        List<TableOperationInfo> ops = obj.getTableOperations();
        assertNotNull(ops);
        assertFalse(ops.isEmpty());
        assertTrue(ops.stream().anyMatch(o -> "SELECT".equals(o.getOperation())));
    }

    @Test
    void insertStatement_extracted() {
        String src = """
                CREATE PROCEDURE p()
                BEGIN
                  INSERT INTO audit_log (action, ts) VALUES ('created', NOW());
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
                CREATE PROCEDURE p()
                BEGIN
                  UPDATE orders SET status = 'shipped' WHERE id = 42;
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
                CREATE PROCEDURE p()
                BEGIN
                  DELETE FROM temp_data WHERE created_at < NOW();
                END
                """;
        ParsedObject obj = single(parser.parse(src, "p", null));
        List<TableOperationInfo> ops = obj.getTableOperations();
        assertNotNull(ops);
        assertTrue(ops.stream().anyMatch(o -> "DELETE".equals(o.getOperation())));
    }

    @Test
    void multipleDmlStatements_allExtracted() {
        String src = """
                CREATE PROCEDURE sync_data()
                BEGIN
                  SELECT id FROM source_table WHERE active = 1;
                  INSERT INTO target_table SELECT id FROM source_table;
                  UPDATE target_table SET processed = 1 WHERE id > 0;
                  DELETE FROM temp_table WHERE age > 30;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "sync_data", null));
        List<TableOperationInfo> ops = obj.getTableOperations();
        assertNotNull(ops);
        assertTrue(ops.size() >= 2, "Expected at least 2 DML ops, got: " + ops.size());
    }

    // ── CALL extraction ──────────────────────────────────────────────────────

    @Test
    void callStatement_extracted() {
        String src = """
                CREATE PROCEDURE p()
                BEGIN
                  CALL refresh_cache();
                  CALL audit.log_event('action');
                END
                """;
        ParsedObject obj = single(parser.parse(src, "p", null));
        List<CallInfo> calls = obj.getCalls();
        assertNotNull(calls);
        assertFalse(calls.isEmpty(), "Expected at least one CALL");
        assertTrue(calls.stream().anyMatch(c -> "refresh_cache".equals(c.getName())));
    }

    @Test
    void schemaQualifiedCall_schemaExtracted() {
        String src = """
                CREATE PROCEDURE p()
                BEGIN
                  CALL `myschema`.`do_work`();
                END
                """;
        ParsedObject obj = single(parser.parse(src, "p", null));
        List<CallInfo> calls = obj.getCalls();
        assertNotNull(calls);
        assertFalse(calls.isEmpty());
        CallInfo c = calls.get(0);
        assertEquals("do_work", c.getName());
        assertEquals("myschema", c.getSchema());
    }

    // ── DECLARE variable extraction ──────────────────────────────────────────

    @Test
    void declareVariable_extracted() {
        String src = """
                CREATE PROCEDURE p()
                BEGIN
                  DECLARE v_count INT DEFAULT 0;
                  DECLARE v_name VARCHAR(100);
                  SELECT v_count;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "p", null));
        List<VariableInfo> vars = obj.getGlobalVariables();
        assertNotNull(vars);
        assertEquals(2, vars.size());
        assertTrue(vars.stream().anyMatch(v -> "v_count".equals(v.getName())));
        assertTrue(vars.stream().anyMatch(v -> "v_name".equals(v.getName())));
    }

    @Test
    void declareWithDefault_defaultValueCaptured() {
        String src = """
                CREATE PROCEDURE p()
                BEGIN
                  DECLARE v_status VARCHAR(20) DEFAULT 'pending';
                  SELECT v_status;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "p", null));
        List<VariableInfo> vars = obj.getGlobalVariables();
        assertNotNull(vars);
        assertFalse(vars.isEmpty());
        VariableInfo v = vars.get(0);
        assertEquals("v_status", v.getName());
        assertNotNull(v.getDefaultValue());
        assertTrue(v.getDefaultValue().contains("pending"));
    }

    // ── Line numbers ─────────────────────────────────────────────────────────

    @Test
    void lineNumbers_set() {
        String src = """
                CREATE PROCEDURE p()
                BEGIN
                  SELECT 1;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "p", null));
        assertEquals(1, obj.getLineStart());
        assertTrue(obj.getLineEnd() >= 4);
    }

    // ── ParseResult metadata ─────────────────────────────────────────────────

    @Test
    void parseResult_fileNameSet() {
        ParseResult r = parser.parse("CREATE PROCEDURE p() BEGIN SELECT 1; END", "p", null);
        assertEquals("p.sql", r.getFileName());
    }

    @Test
    void parseResult_parseTimeRecorded() {
        ParseResult r = parser.parse("CREATE PROCEDURE p() BEGIN SELECT 1; END", "p", null);
        assertTrue(r.getParseTimeMs() >= 0);
    }

    @Test
    void emptyBody_noException() {
        String src = """
                CREATE PROCEDURE empty_proc()
                BEGIN
                END
                """;
        assertDoesNotThrow(() -> parser.parse(src, "empty_proc", null));
    }

    // ── Schema from signature ────────────────────────────────────────────────

    @Test
    void schemaInSignature_appliedToObject() {
        String src = """
                CREATE PROCEDURE `reporting`.`daily_summary`()
                BEGIN
                  SELECT 1;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "daily_summary", null));
        assertEquals("reporting", obj.getSchema());
    }

    // ── Helper ───────────────────────────────────────────────────────────────

    private ParsedObject single(ParseResult r) {
        assertNotNull(r);
        assertFalse(r.getObjects().isEmpty(), "Expected at least one ParsedObject");
        return r.getObjects().get(0);
    }
}
