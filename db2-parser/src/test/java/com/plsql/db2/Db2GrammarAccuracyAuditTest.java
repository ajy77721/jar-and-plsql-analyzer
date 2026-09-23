package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Grammar-accuracy audit — read-only evidence only, no fixes here.
 *
 * Covers: delimited identifiers, DB2-specific type modifiers, numeric literal
 * edge cases, labeled compound-statement / loop scoping, and whitespace-tolerant
 * dotted names.
 */
class Db2GrammarAccuracyAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private Db2RoutineInfo parse(String body) {
        return engine.parse(
                "CREATE PROCEDURE p()\nLANGUAGE SQL\nBEGIN\n" + body + "\nEND", "p", null);
    }

    private Db2RoutineInfo parseFull(String sql) {
        return engine.parse(sql, "p", null);
    }

    // ── 1. Delimited identifiers ─────────────────────────────────────────────

    @Test
    @DisplayName("1a. quoted lowercase table name preserves exact case (not folded)")
    void quotedIdentifierPreservesCase() {
        Db2RoutineInfo r = parse("SELECT * FROM \"lowercase_name\";");
        assertFalse(r.getTableOps().isEmpty(), "no table op captured at all");
        String tbl = r.getTableOps().get(0).getTableName();
        System.out.println("[1a] captured table name = [" + tbl + "]");
        assertEquals("lowercase_name", tbl,
                "a double-quoted DB2 identifier must preserve exact case verbatim");
    }

    @Test
    @DisplayName("1b. unquoted table name is folded to upper-case per DB2 default collation")
    void unquotedIdentifierIsFolded() {
        Db2RoutineInfo r = parse("SELECT * FROM lowercase_name;");
        assertFalse(r.getTableOps().isEmpty(), "no table op captured at all");
        String tbl = r.getTableOps().get(0).getTableName();
        System.out.println("[1b] captured table name = [" + tbl + "]");
        // Real DB2 semantics: an unquoted identifier is implicitly upper-cased by the engine.
        assertEquals("LOWERCASE_NAME", tbl,
                "an unquoted DB2 identifier should be folded to upper-case");
    }

    @Test
    @DisplayName("1c. quoted identifier with embedded spaces is captured whole")
    void quotedIdentifierWithSpaces() {
        Db2RoutineInfo r = parse("SELECT * FROM \"Table Name\";");
        assertFalse(r.getTableOps().isEmpty(), "no table op captured at all");
        String tbl = r.getTableOps().get(0).getTableName();
        System.out.println("[1c] captured table name = [" + tbl + "]");
        assertEquals("Table Name", tbl);
    }

    // ── 2. DB2-specific type modifiers ───────────────────────────────────────

    @Test
    @DisplayName("2a. CHAR(10) FOR BIT DATA parameter does not corrupt the param list")
    void forBitDataParam() {
        Db2RoutineInfo r = engine.parse(
                "CREATE PROCEDURE p(IN p_data CHAR(10) FOR BIT DATA, IN p_id INTEGER)\n"
              + "LANGUAGE SQL\nBEGIN\n  SET p_id = p_id;\nEND", "p", null);
        System.out.println("[2a] params = " + r.getParams().size());
        for (Db2ParamInfo p : r.getParams()) {
            System.out.println("      -> " + p.getName() + " : " + p.getDataType());
        }
        assertEquals(2, r.getParams().size(),
                "FOR BIT DATA suffix desynced parsing of the remaining parameter list");
    }

    @Test
    @DisplayName("2b. GRAPHIC / VARGRAPHIC and DECIMAL(10,2) params parse cleanly")
    void graphicAndDecimalParams() {
        Db2RoutineInfo r = engine.parse(
                "CREATE PROCEDURE p(IN p_g GRAPHIC(10), IN p_amt DECIMAL(10,2), IN p_id INTEGER)\n"
              + "LANGUAGE SQL\nBEGIN\n  SET p_id = p_id;\nEND", "p", null);
        System.out.println("[2b] params = " + r.getParams().size());
        for (Db2ParamInfo p : r.getParams()) {
            System.out.println("      -> " + p.getName() + " : " + p.getDataType());
        }
        assertEquals(3, r.getParams().size());
    }

    @Test
    @DisplayName("2c. hex literal X'1A2B' inside SET does not desync statement parsing")
    void hexLiteral() {
        Db2RoutineInfo r = parse("SET v_x = X'1A2B';\nSELECT * FROM ERP_ORDERS;");
        System.out.println("[2c] errors = " + r.getErrors());
        System.out.println("[2c] tableOps = " + r.getTableOps().size());
        assertFalse(r.getTableOps().isEmpty(),
                "hex literal desynced parsing; the following SELECT was lost");
    }

    // ── 3. Numeric literal edge cases ────────────────────────────────────────

    @Test
    @DisplayName("3a. scientific notation literal (1.5E10) does not desync statement parsing")
    void scientificNotationLiteral() {
        Db2RoutineInfo r = parse("SET v_x = 1.5E10;\nSELECT * FROM ERP_ORDERS;");
        System.out.println("[3a] errors = " + r.getErrors());
        System.out.println("[3a] tableOps = " + r.getTableOps().size());
        assertFalse(r.getTableOps().isEmpty(),
                "scientific-notation literal desynced parsing; the following SELECT was lost");
    }

    @Test
    @DisplayName("3b. DECFLOAT literal suffix does not desync statement parsing")
    void decfloatLiteral() {
        Db2RoutineInfo r = parse("SET v_x = 3.14159265358979D;\nSELECT * FROM ERP_ORDERS;");
        System.out.println("[3b] errors = " + r.getErrors());
        System.out.println("[3b] tableOps = " + r.getTableOps().size());
        assertFalse(r.getTableOps().isEmpty(),
                "DECFLOAT literal desynced parsing; the following SELECT was lost");
    }

    // ── 4. Label / compound-statement scoping ────────────────────────────────

    @Test
    @DisplayName("4a. labeled compound statement (my_label: BEGIN...END my_label) parses and its body is analyzed")
    void labeledCompoundStatement() {
        Db2RoutineInfo r = parse(
                "my_label: BEGIN\n"
              + "  SELECT * FROM ERP_ORDERS;\n"
              + "END my_label;");
        System.out.println("[4a] errors = " + r.getErrors());
        System.out.println("[4a] tableOps = " + r.getTableOps().size());
        assertFalse(r.getTableOps().isEmpty(),
                "labeled BEGIN...END block body was lost / not analyzed");
    }

    @Test
    @DisplayName("4b. labeled loop with LEAVE targeting the label exits correctly; inner statements still captured")
    void labeledLoopWithLeave() {
        Db2RoutineInfo r = parse(
                "my_loop: LOOP\n"
              + "  SELECT * FROM ERP_ORDERS;\n"
              + "  LEAVE my_loop;\n"
              + "END LOOP my_loop;");
        System.out.println("[4b] errors = " + r.getErrors());
        System.out.println("[4b] tableOps = " + r.getTableOps().size());
        assertFalse(r.getTableOps().isEmpty(),
                "labeled LOOP body (containing LEAVE) was lost / not analyzed");
    }

    @Test
    @DisplayName("4c. ITERATE referencing an outer label inside a nested loop is not misclassified as a table/call")
    void iterateWithLabel() {
        Db2RoutineInfo r = parse(
                "outer_loop: LOOP\n"
              + "  inner_loop: LOOP\n"
              + "    ITERATE outer_loop;\n"
              + "  END LOOP inner_loop;\n"
              + "END LOOP outer_loop;");
        System.out.println("[4c] errors = " + r.getErrors());
        System.out.println("[4c] calls = " + r.getCalls().size());
        System.out.println("[4c] tableOps = " + r.getTableOps().size());
        // ITERATE outer_loop must not be captured as a CALL to routine "outer_loop"
        boolean misclassifiedAsCall = r.getCalls().stream()
                .anyMatch(c -> "outer_loop".equalsIgnoreCase(c.getName()));
        assertFalse(misclassifiedAsCall,
                "ITERATE <label> was misclassified as a routine CALL");
    }

    // ── 5. Whitespace-tolerant dotted names ──────────────────────────────────

    @Test
    @DisplayName("5a. schema . table with spaces around the dot resolves the same as schema.table")
    void spacedDottedName() {
        Db2RoutineInfo r = parse("SELECT * FROM my_schema . my_table;");
        assertFalse(r.getTableOps().isEmpty(), "no table op captured at all");
        Db2TableOpInfo op = r.getTableOps().get(0);
        System.out.println("[5a] schema=[" + op.getSchemaName() + "] table=[" + op.getTableName() + "]");
        assertEquals("MY_SCHEMA", op.getSchemaName());
        assertEquals("MY_TABLE", op.getTableName());
    }

    @Test
    @DisplayName("5b. schema.\"Quoted Table\" mixed quoting extracts schema plain and table exact-case")
    void mixedQuotingDottedName() {
        Db2RoutineInfo r = parse("SELECT * FROM my_schema.\"Quoted Table\";");
        assertFalse(r.getTableOps().isEmpty(), "no table op captured at all");
        Db2TableOpInfo op = r.getTableOps().get(0);
        System.out.println("[5b] schema=[" + op.getSchemaName() + "] table=[" + op.getTableName() + "]");
        assertEquals("MY_SCHEMA", op.getSchemaName());
        assertEquals("Quoted Table", op.getTableName());
    }
}
