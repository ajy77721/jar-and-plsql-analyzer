package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression coverage (2026-09-17) for dynamic/string-based cursor capture in db2-parser:
 * PREPARE + DECLARE CURSOR FOR stmt (Bug 1 — the grammar's second declareCursorStmt alternative,
 * a cursor over a prepared-statement name, was silently dropped), EXECUTE IMMEDIATE variable
 * resolution (Bug 8, DB2 portion — unresolvedVarName flagging), parameterized-cursor support
 * (N/A check), and DB2's FOR ... AS SELECT ... DO cursor-loop form.
 *
 * Renamed in spirit from "audit" (prints evidence) to hard assertions of correct behavior, but
 * kept the file name since it still documents/covers the same five scenarios end to end.
 */
class Db2DynamicCursorAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    // 1. Classic DB2 PREPARE + DECLARE CURSOR FOR stmt, fully literal SQL string.
    @Test
    @DisplayName("1: PREPARE + DECLARE CURSOR FOR stmt — fully literal SQL resolves to a table + selectText")
    void prepareDeclareCursorLiteral() {
        String sql =
            "CREATE PROCEDURE p_cur_literal()\n"
          + "LANGUAGE SQL\n"
          + "BEGIN\n"
          + "  DECLARE v_sql VARCHAR(200);\n"
          + "  DECLARE v_status VARCHAR(10) DEFAULT 'OPEN';\n"
          + "  SET v_sql = 'SELECT * FROM orders WHERE status = ?';\n"
          + "  PREPARE stmt1 FROM v_sql;\n"
          + "  DECLARE cur1 CURSOR FOR stmt1;\n"
          + "  OPEN cur1 USING v_status;\n"
          + "END";
        Db2RoutineInfo r = engine.parse(sql, "p_cur_literal", null);
        assertTrue(r.getErrors().isEmpty(), "expected no parse errors: " + r.getErrors());

        assertEquals(1, r.getCursors().size());
        Db2CursorInfo cur = r.getCursors().get(0);
        assertEquals("cur1", cur.getName());
        assertEquals("SELECT * FROM orders WHERE status = ?", cur.getSelectText(),
                "cursor over a prepared statement should resolve to the PREPARE's literal SQL text");

        List<Db2DynamicSqlInfo> dyn = r.getDynamicSql();
        assertEquals(1, dyn.size());
        Db2DynamicSqlInfo prep = dyn.get(0);
        assertEquals(Db2DynamicSqlInfo.Kind.PREPARE, prep.getKind());
        assertEquals("stmt1", prep.getStmtName());
        assertNull(prep.getUnresolvedVarName(), "fully-literal PREPARE source must not be flagged unresolved");

        assertTrue(
            r.getTableOps().stream().anyMatch(t ->
                t.getDmlType() == Db2TableOpInfo.DmlType.DYNAMIC && "orders".equalsIgnoreCase(t.getTableName())),
            "expected a DYNAMIC table op for 'orders' resolved via the prepared cursor");
    }

    // 2. Same idiom, but table name is variable-interrupted (concatenated).
    @Test
    @DisplayName("2: PREPARE + DECLARE CURSOR FOR stmt — variable-interrupted table name is flagged unresolved")
    void prepareDeclareCursorVariable() {
        String sql =
            "CREATE PROCEDURE p_cur_dynamic(IN p_table VARCHAR(30))\n"
          + "LANGUAGE SQL\n"
          + "BEGIN\n"
          + "  DECLARE v_sql VARCHAR(500);\n"
          + "  DECLARE v_status VARCHAR(10) DEFAULT 'OPEN';\n"
          + "  SET v_sql = 'SELECT * FROM ' || p_table || ' WHERE status = ?';\n"
          + "  PREPARE stmt2 FROM v_sql;\n"
          + "  DECLARE cur2 CURSOR FOR stmt2;\n"
          + "  OPEN cur2 USING v_status;\n"
          + "END";
        Db2RoutineInfo r = engine.parse(sql, "p_cur_dynamic", null);
        assertTrue(r.getErrors().isEmpty(), "expected no parse errors: " + r.getErrors());

        assertEquals(1, r.getCursors().size());
        Db2CursorInfo cur = r.getCursors().get(0);
        assertEquals("cur2", cur.getName());
        assertNull(cur.getSelectText(),
                "a variable-interrupted table name cannot be resolved; must not be guessed");

        List<Db2DynamicSqlInfo> dyn = r.getDynamicSql();
        assertEquals(1, dyn.size());
        Db2DynamicSqlInfo prep = dyn.get(0);
        assertEquals(Db2DynamicSqlInfo.Kind.PREPARE, prep.getKind());
        assertEquals("stmt2", prep.getStmtName());
        assertEquals("v_sql", prep.getUnresolvedVarName(),
                "variable-interrupted PREPARE source must be flagged with the unresolved variable name");

        assertTrue(
            r.getTableOps().stream().noneMatch(t -> t.getDmlType() == Db2TableOpInfo.DmlType.DYNAMIC),
            "no DYNAMIC table op should be fabricated when the table name is not literally resolvable");
    }

    // 3a. Control comparison: EXECUTE IMMEDIATE with a fully-literal string (known-fixed path).
    @Test
    @DisplayName("3a: EXECUTE IMMEDIATE literal string resolves to a DYNAMIC table op")
    void executeImmediateLiteral() {
        String sql =
            "CREATE PROCEDURE p_exec_imm_literal()\n"
          + "LANGUAGE SQL\n"
          + "BEGIN\n"
          + "  EXECUTE IMMEDIATE 'DELETE FROM stale_orders WHERE created < CURRENT DATE - 30 DAYS';\n"
          + "END";
        Db2RoutineInfo r = engine.parse(sql, "p_exec_imm_literal", null);
        assertTrue(r.getErrors().isEmpty(), "expected no parse errors: " + r.getErrors());

        List<Db2DynamicSqlInfo> dyn = r.getDynamicSql();
        assertEquals(1, dyn.size());
        assertEquals(Db2DynamicSqlInfo.Kind.EXECUTE_IMMEDIATE, dyn.get(0).getKind());
        assertNull(dyn.get(0).getUnresolvedVarName());

        assertTrue(
            r.getTableOps().stream().anyMatch(t ->
                t.getDmlType() == Db2TableOpInfo.DmlType.DYNAMIC && "stale_orders".equalsIgnoreCase(t.getTableName())),
            "expected a DYNAMIC table op for 'stale_orders'");
    }

    // 3b. EXECUTE IMMEDIATE with a variable-interrupted string (Bug 8, DB2 portion).
    @Test
    @DisplayName("3b: EXECUTE IMMEDIATE variable-interrupted string is flagged unresolved")
    void executeImmediateVariable() {
        String sql =
            "CREATE PROCEDURE p_exec_imm_var(IN p_table VARCHAR(30))\n"
          + "LANGUAGE SQL\n"
          + "BEGIN\n"
          + "  DECLARE v_sql VARCHAR(200);\n"
          + "  SET v_sql = 'DELETE FROM ' || p_table || ' WHERE created < CURRENT DATE - 30 DAYS';\n"
          + "  EXECUTE IMMEDIATE v_sql;\n"
          + "END";
        Db2RoutineInfo r = engine.parse(sql, "p_exec_imm_var", null);
        assertTrue(r.getErrors().isEmpty(), "expected no parse errors: " + r.getErrors());

        List<Db2DynamicSqlInfo> dyn = r.getDynamicSql();
        assertEquals(1, dyn.size());
        Db2DynamicSqlInfo d = dyn.get(0);
        assertEquals(Db2DynamicSqlInfo.Kind.EXECUTE_IMMEDIATE, d.getKind());
        assertEquals("v_sql", d.getSqlSource());
        assertEquals("v_sql", d.getUnresolvedVarName(),
                "variable-interrupted EXECUTE IMMEDIATE source must be flagged unresolved");

        assertTrue(
            r.getTableOps().stream().noneMatch(t -> t.getDmlType() == Db2TableOpInfo.DmlType.DYNAMIC),
            "no DYNAMIC table op should be fabricated when the table name is not literally resolvable");
    }

    // 4. Parameterized cursor (Oracle-style CURSOR cur(p_id INT) IS SELECT...) — DB2 SQL PL has
    // no such syntax; verify the grammar rejects it (N/A check).
    @Test
    @DisplayName("4: parameterized cursor (Oracle-style) — DB2 grammar has no such rule and errors")
    void parameterizedCursorNotSupported() {
        String sql =
            "CREATE PROCEDURE p_cur_param()\n"
          + "LANGUAGE SQL\n"
          + "BEGIN\n"
          + "  DECLARE cur1 CURSOR(p_id INTEGER) FOR SELECT * FROM orders WHERE order_id = p_id;\n"
          + "END";
        Db2RoutineInfo r = engine.parse(sql, "p_cur_param", null);
        assertFalse(r.getErrors().isEmpty(),
                "DB2 SQL PL has no parameterized-cursor syntax; a parse error is the correct, expected outcome");
        assertTrue(r.getCursors().isEmpty(), "no cursor should be captured from an unparseable declaration");
    }

    // 5. DB2 FOR ... AS SELECT ... DO ... END FOR cursor loop.
    @Test
    @DisplayName("5: FOR row AS SELECT ... DO ... END FOR (implicit cursor loop) captures the SELECT table op")
    void forStatementCursorLoop() {
        String sql =
            "CREATE PROCEDURE p_for_loop()\n"
          + "LANGUAGE SQL\n"
          + "BEGIN\n"
          + "  DECLARE v_total INTEGER DEFAULT 0;\n"
          + "  FOR rec AS\n"
          + "    SELECT o.order_id, o.amount FROM orders o WHERE o.status = 'OPEN'\n"
          + "  DO\n"
          + "    SET v_total = v_total + rec.amount;\n"
          + "  END FOR;\n"
          + "END";
        Db2RoutineInfo r = engine.parse(sql, "p_for_loop", null);
        assertTrue(r.getErrors().isEmpty(), "expected no parse errors: " + r.getErrors());

        assertTrue(
            r.getTableOps().stream().anyMatch(t ->
                t.getDmlType() == Db2TableOpInfo.DmlType.SELECT
                        && "orders".equalsIgnoreCase(t.getTableName())
                        && "o".equalsIgnoreCase(t.getAlias())),
            "expected a SELECT table op for 'orders' aliased 'o' inside the FOR...AS cursor loop");
        assertTrue(
            r.getStatements().stream().anyMatch(s -> "FOR_LOOP".equals(s.getType())),
            "expected a FOR_LOOP statement record");
    }
}
