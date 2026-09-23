package com.plsql.parser.multidb;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class Db2ProcedureParserTest {

    private Db2ProcedureParser parser;

    @BeforeEach
    void setUp() {
        parser = new Db2ProcedureParser();
    }

    // ── Signature extraction ──────────────────────────────────────────────────

    @Test
    void procedure_nameAndTypeExtracted() {
        String src = "CREATE PROCEDURE get_orders()\nLANGUAGE SQL\nBEGIN\n  SELECT * FROM orders;\nEND";
        ParsedObject obj = single(parser.parse(src, "get_orders", "mydb"));
        assertEquals("get_orders", obj.getName());
        assertEquals("PROCEDURE", obj.getType());
    }

    @Test
    void function_typeExtracted() {
        String src = "CREATE FUNCTION calc_tax(IN amount DECIMAL(10,2))\n" +
                     "RETURNS DECIMAL(10,2)\nLANGUAGE SQL\nBEGIN ATOMIC\n  RETURN amount * 0.1;\nEND";
        ParsedObject obj = single(parser.parse(src, "calc_tax", null));
        assertEquals("calc_tax", obj.getName());
        assertEquals("FUNCTION", obj.getType());
    }

    @Test
    void orReplace_signatureParsed() {
        String src = "CREATE OR REPLACE PROCEDURE update_status(IN p_id INT)\nLANGUAGE SQL\nBEGIN\n  UPDATE t SET s = 1 WHERE id = p_id;\nEND";
        ParsedObject obj = single(parser.parse(src, "update_status", null));
        assertEquals("update_status", obj.getName());
        assertEquals("PROCEDURE", obj.getType());
    }

    @Test
    void schemaQualifiedSignature_schemaExtracted() {
        String src = "CREATE PROCEDURE hrschema.emp_hire(IN p_name VARCHAR(100))\nLANGUAGE SQL\nBEGIN END";
        ParsedObject obj = single(parser.parse(src, "emp_hire", null));
        assertEquals("emp_hire", obj.getName());
        assertEquals("hrschema", obj.getSchema());
    }

    @Test
    void fallbackName_usedWhenSignatureUnparseable() {
        String src = "-- no CREATE here\nSELECT 1 FROM SYSIBM.SYSDUMMY1;";
        ParsedObject obj = single(parser.parse(src, "my_fallback", null));
        assertEquals("my_fallback", obj.getName());
        assertEquals("PROCEDURE", obj.getType());
    }

    // ── Parameter extraction ──────────────────────────────────────────────────

    @Test
    void inParameter_extracted() {
        String src = "CREATE PROCEDURE find_emp(IN p_emp_id INT)\nLANGUAGE SQL\nBEGIN\n  SELECT * FROM employees WHERE id = p_emp_id;\nEND";
        ParsedObject obj = single(parser.parse(src, "find_emp", null));
        List<ParameterInfo> params = obj.getParameters();
        assertNotNull(params);
        assertEquals(1, params.size());
        assertEquals("p_emp_id", params.get(0).getName());
        assertEquals("IN", params.get(0).getDirection());
    }

    @Test
    void outParameter_extracted() {
        String src = "CREATE PROCEDURE get_count(OUT p_count INT)\nLANGUAGE SQL\nBEGIN\n  SELECT COUNT(*) INTO p_count FROM employees;\nEND";
        ParsedObject obj = single(parser.parse(src, "get_count", null));
        List<ParameterInfo> params = obj.getParameters();
        assertNotNull(params);
        assertEquals(1, params.size());
        assertEquals("OUT", params.get(0).getDirection());
        assertEquals("p_count", params.get(0).getName());
    }

    @Test
    void inoutParameter_extracted() {
        String src = "CREATE PROCEDURE adjust_salary(INOUT p_amount DECIMAL(10,2))\nLANGUAGE SQL\nBEGIN\n  SET p_amount = p_amount * 1.1;\nEND";
        ParsedObject obj = single(parser.parse(src, "adjust_salary", null));
        List<ParameterInfo> params = obj.getParameters();
        assertNotNull(params);
        assertEquals(1, params.size());
        assertEquals("INOUT", params.get(0).getDirection());
    }

    @Test
    void multipleParameters_allExtracted() {
        String src = "CREATE PROCEDURE transfer(\n" +
                     "  IN p_from INT,\n  IN p_to INT,\n  INOUT p_amount DECIMAL(10,2),\n  OUT p_status VARCHAR(20))\n" +
                     "LANGUAGE SQL\nBEGIN END";
        ParsedObject obj = single(parser.parse(src, "transfer", null));
        assertEquals(4, obj.getParameters().size());
        assertEquals("p_from",   obj.getParameters().get(0).getName());
        assertEquals("p_to",     obj.getParameters().get(1).getName());
        assertEquals("p_amount", obj.getParameters().get(2).getName());
        assertEquals("p_status", obj.getParameters().get(3).getName());
    }

    @Test
    void noParameters_emptyParamList() {
        String src = "CREATE PROCEDURE rebuild_cache()\nLANGUAGE SQL\nBEGIN\n  DELETE FROM cache;\nEND";
        ParsedObject obj = single(parser.parse(src, "rebuild_cache", null));
        assertNotNull(obj.getParameters());
        assertTrue(obj.getParameters().isEmpty());
    }

    // ── CALL extraction ───────────────────────────────────────────────────────

    @Test
    void callStatement_extracted() {
        String src = "CREATE PROCEDURE daily_sync()\nLANGUAGE SQL\nBEGIN\n  CALL process_orders();\nEND";
        ParsedObject obj = single(parser.parse(src, "daily_sync", null));
        assertFalse(obj.getCalls().isEmpty());
        assertTrue(obj.getCalls().stream().anyMatch(c -> "process_orders".equals(c.getName())));
    }

    @Test
    void multipleCalls_allExtracted() {
        String src = "CREATE PROCEDURE full_sync()\nLANGUAGE SQL\nBEGIN\n" +
                     "  CALL process_orders();\n  CALL update_inventory();\n  CALL log_audit('SYNC');\n" +
                     "END";
        ParsedObject obj = single(parser.parse(src, "full_sync", null));
        assertEquals(3, obj.getCalls().size());
    }

    @Test
    void schemaQualifiedCall_schemaExtracted() {
        String src = "CREATE PROCEDURE test()\nLANGUAGE SQL\nBEGIN\n  CALL hrschema.emp_promote(1);\nEND";
        ParsedObject obj = single(parser.parse(src, "test", null));
        List<CallInfo> calls = obj.getCalls();
        assertFalse(calls.isEmpty());
        CallInfo c = calls.get(0);
        assertEquals("emp_promote", c.getName());
        assertEquals("hrschema", c.getSchema());
    }

    @Test
    void callInsideIF_extracted() {
        String src = "CREATE PROCEDURE test(IN p_mode VARCHAR(10))\nLANGUAGE SQL\nBEGIN\n" +
                     "  IF p_mode = 'FULL' THEN\n    CALL run_full_sync();\n  END IF;\nEND";
        ParsedObject obj = single(parser.parse(src, "test", null));
        assertTrue(obj.getCalls().stream().anyMatch(c -> "run_full_sync".equals(c.getName())));
    }

    // ── DML table operations ──────────────────────────────────────────────────

    @Test
    void insertTableOp_extracted() {
        String src = "CREATE PROCEDURE emp_hire(IN p_name VARCHAR(100))\nLANGUAGE SQL\nBEGIN\n" +
                     "  INSERT INTO employees (emp_name) VALUES (p_name);\nEND";
        ParsedObject obj = single(parser.parse(src, "emp_hire", null));
        assertTrue(obj.getTableOperations().stream()
                .anyMatch(o -> "INSERT".equals(o.getOperation()) && "EMPLOYEES".equals(o.getTableName())));
    }

    @Test
    void updateTableOp_extracted() {
        String src = "CREATE PROCEDURE emp_terminate(IN p_id INT)\nLANGUAGE SQL\nBEGIN\n" +
                     "  UPDATE employees SET status = 'TERM' WHERE emp_id = p_id;\nEND";
        ParsedObject obj = single(parser.parse(src, "emp_terminate", null));
        assertTrue(obj.getTableOperations().stream()
                .anyMatch(o -> "UPDATE".equals(o.getOperation()) && "EMPLOYEES".equals(o.getTableName())));
    }

    @Test
    void deleteTableOp_extracted() {
        String src = "CREATE PROCEDURE cleanup()\nLANGUAGE SQL\nBEGIN\n" +
                     "  DELETE FROM audit_log WHERE log_date < CURRENT DATE;\nEND";
        ParsedObject obj = single(parser.parse(src, "cleanup", null));
        assertTrue(obj.getTableOperations().stream()
                .anyMatch(o -> "DELETE".equals(o.getOperation()) && "AUDIT_LOG".equals(o.getTableName())));
    }

    @Test
    void selectTableOp_extracted() {
        String src = "CREATE PROCEDURE list_orders()\nLANGUAGE SQL\nBEGIN\n" +
                     "  SELECT order_id, total FROM orders WHERE status = 'OPEN';\nEND";
        ParsedObject obj = single(parser.parse(src, "list_orders", null));
        assertTrue(obj.getTableOperations().stream().anyMatch(o -> "SELECT".equals(o.getOperation())));
    }

    @Test
    void selectWithJoin_multipleTables() {
        String src = "CREATE PROCEDURE join_test()\nLANGUAGE SQL\nBEGIN\n" +
                     "  SELECT o.order_id, e.emp_name FROM orders o " +
                     "JOIN employees e ON e.emp_id = o.emp_id;\nEND";
        ParsedObject obj = single(parser.parse(src, "join_test", null));
        assertFalse(obj.getTableOperations().isEmpty(), "expected at least one table op for JOIN");
    }

    // ── DECLARE variable / cursor ─────────────────────────────────────────────

    @Test
    void declareVariable_extracted() {
        String src = "CREATE PROCEDURE test()\nLANGUAGE SQL\nBEGIN\n" +
                     "  DECLARE v_count INT DEFAULT 0;\n  DECLARE v_name VARCHAR(100);\n" +
                     "  SET v_count = 1;\nEND";
        ParsedObject obj = single(parser.parse(src, "test", null));
        List<VariableInfo> vars = obj.getGlobalVariables();
        assertNotNull(vars);
        assertTrue(vars.stream().anyMatch(v -> "v_count".equals(v.getName())));
        assertTrue(vars.stream().anyMatch(v -> "v_name".equals(v.getName())));
    }

    @Test
    void declareCursor_typeIsCursor() {
        String src = "CREATE PROCEDURE cursor_test()\nLANGUAGE SQL\nBEGIN\n" +
                     "  DECLARE cur_emps CURSOR FOR SELECT emp_id FROM employees;\n" +
                     "  OPEN cur_emps;\n  CLOSE cur_emps;\nEND";
        ParsedObject obj = single(parser.parse(src, "cursor_test", null));
        List<VariableInfo> vars = obj.getGlobalVariables();
        assertTrue(vars.stream().anyMatch(v -> "cur_emps".equals(v.getName()) && "CURSOR".equals(v.getDataType())));
    }

    @Test
    void declareCursorWithHold_extracted() {
        String src = "CREATE PROCEDURE test()\nLANGUAGE SQL\nBEGIN\n" +
                     "  DECLARE cur CURSOR WITH HOLD FOR SELECT id FROM t;\nEND";
        ParsedObject obj = single(parser.parse(src, "test", null));
        assertTrue(obj.getGlobalVariables().stream()
                .anyMatch(v -> "cur".equals(v.getName()) && "CURSOR".equals(v.getDataType())));
    }

    // ── Dynamic SQL (PREPARE / EXECUTE) ───────────────────────────────────────

    @Test
    void dynamicSql_prepareAndExecute_parsedWithoutError() {
        String src = "CREATE PROCEDURE dyn_test(IN p_dept INT)\nLANGUAGE SQL\nBEGIN\n" +
                     "  DECLARE v_sql VARCHAR(500);\n" +
                     "  SET v_sql = 'UPDATE employees SET salary = salary * 1.1 WHERE dept_id = ' || CHAR(p_dept);\n" +
                     "  PREPARE stmt1 FROM v_sql;\n  EXECUTE stmt1;\nEND";
        ParseResult r = parser.parse(src, "dyn_test", null);
        assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
        ParsedObject obj = single(r);
        assertNotNull(obj.getName());
    }

    @Test
    void dynamicSql_literalString_parsedWithoutError() {
        String src = "CREATE PROCEDURE dyn_literal()\nLANGUAGE SQL\nBEGIN\n" +
                     "  PREPARE s FROM 'DELETE FROM temp_data WHERE expired = 1';\n  EXECUTE s;\nEND";
        ParseResult r = parser.parse(src, "dyn_literal", null);
        assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
    }

    // ── Nested BEGIN ATOMIC blocks ────────────────────────────────────────────

    @Test
    void nestedBeginAtomic_parsedWithoutError() {
        String src = "CREATE PROCEDURE nested_test()\nLANGUAGE SQL\nBEGIN\n" +
                     "  DECLARE v_id INT;\n" +
                     "  BEGIN ATOMIC\n" +
                     "    INSERT INTO orders (status) VALUES ('NEW');\n" +
                     "    CALL log_event('order created');\n" +
                     "  END;\nEND";
        ParsedObject obj = single(parser.parse(src, "nested_test", null));
        assertTrue(obj.getCalls().stream().anyMatch(c -> "log_event".equals(c.getName())));
    }

    // ── ParseResult metadata ──────────────────────────────────────────────────

    @Test
    void parseResult_fileNameMatchesProcedureName() {
        ParseResult r = parser.parse(
                "CREATE PROCEDURE my_proc()\nLANGUAGE SQL\nBEGIN\n  SELECT 1 FROM SYSIBM.SYSDUMMY1;\nEND",
                "my_proc", null);
        assertEquals("my_proc.sql", r.getFileName());
    }

    @Test
    void parseResult_parseTimeRecorded() {
        ParseResult r = parser.parse(
                "CREATE PROCEDURE p()\nLANGUAGE SQL\nBEGIN END", "p", null);
        assertTrue(r.getParseTimeMs() >= 0);
    }

    @Test
    void lineNumbers_set() {
        String src = "CREATE PROCEDURE p()\nLANGUAGE SQL\nBEGIN\n  SELECT 1 FROM SYSIBM.SYSDUMMY1;\nEND";
        ParsedObject obj = single(parser.parse(src, "p", null));
        assertEquals(1, obj.getLineStart());
        assertTrue(obj.getLineEnd() >= 5);
    }

    // ── Schema from schema parameter ──────────────────────────────────────────

    @Test
    void schemaParameter_propagatedToObject() {
        ParsedObject obj = single(parser.parse(
                "CREATE PROCEDURE daily_sync()\nLANGUAGE SQL\nBEGIN END",
                "daily_sync", "HRSCHEMA"));
        assertEquals("HRSCHEMA", obj.getSchema());
    }

    // ── Factory routing ───────────────────────────────────────────────────────

    @Test
    void factory_supportsDb2() {
        assertTrue(StoredProcedureParserFactory.supportsDbType(
                com.plsqlanalyzer.config.DbType.DB2));
    }

    @Test
    void factory_returnsDb2Parser() {
        assertNotNull(StoredProcedureParserFactory.get(com.plsqlanalyzer.config.DbType.DB2));
    }

    // ── Helper ───────────────────────────────────────────────────────────────

    private ParsedObject single(ParseResult r) {
        assertNotNull(r);
        assertFalse(r.getObjects().isEmpty(), "Expected at least one ParsedObject");
        return r.getObjects().get(0);
    }
}
