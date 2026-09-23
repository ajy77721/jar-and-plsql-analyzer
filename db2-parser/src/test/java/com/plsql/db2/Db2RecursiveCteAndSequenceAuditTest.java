package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT ONLY (read-only investigation, 2026-09-17) — printed evidence for recursive CTEs and
 * sequence/identity usage against {@code Db2AnalysisEngine}, mirroring the existing pattern in
 * {@code Db2DmlCaptureGapAuditTest}. Each routine below is a single, self-contained
 * CREATE PROCEDURE body (the grammar's {@code compilationUnit} accepts exactly one routine).
 *
 * <p>Scope notes (grammar evidence gathered before writing this file — see
 * {@code Db2Parser.g4} / {@code Db2Lexer.g4}):
 * <ul>
 *   <li><b>CONNECT BY / START WITH (hierarchical queries)</b> — grep of both grammar files for
 *   CONNECT/START WITH/PRIOR found zero lexer tokens and zero parser rules. The grammar has no
 *   support for Oracle-style hierarchical query syntax at all (consistent with DB2 for i/LUW,
 *   which historically lacks CONNECT BY). This concept does NOT apply to this parser and is
 *   SKIPPED — no test below covers it.</li>
 *   <li><b>RECURSIVE keyword variant of WITH</b> — {@code withClause : WITH cteDef (COMMA cteDef)*}
 *   has no optional RECURSIVE token; DB2's plain {@code WITH name AS (...)} form (no RECURSIVE
 *   keyword) is the only form the grammar accepts. A literal {@code WITH RECURSIVE ...} would
 *   fail to parse (RECURSIVE is not a declared keyword/token), so that spelling is out of scope;
 *   only the keyword-less DB2 form is tested here.</li>
 *   <li><b>GENERATED ALWAYS AS IDENTITY (DDL)</b> — grep of the grammar for CREATE found only
 *   {@code CREATE (OR REPLACE)? (PROCEDURE | FUNCTION)}; there is no CREATE TABLE rule anywhere,
 *   so column-level IDENTITY DDL is entirely out of scope for this parser (it only parses
 *   procedure/function bodies, not DDL). SKIPPED for that reason.</li>
 *   <li><b>NEXTVAL/PREVVAL/NEXT VALUE FOR sequence references</b> — DOES apply; grammar rule at
 *   {@code Db2Parser.g4} lines ~470-471 and visitor {@code recordSequenceRef}/{@code result.getSequences()}
 *   in {@code Db2AnalysisVisitor.java}. Tested below.</li>
 * </ul>
 */
class Db2RecursiveCteAndSequenceAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private static void dump(String label, Db2RoutineInfo r) {
        System.out.println("=== " + label + " errors === " + r.getErrors());
        System.out.println("--- tableOps ---");
        for (Db2TableOpInfo t : r.getTableOps()) {
            System.out.println("  " + t.getDmlType() + " " + t.qualifiedTable()
                    + " alias=" + t.getAlias() + " line=" + t.getLine());
        }
        System.out.println("--- statements ---");
        for (Db2StatementInfo s : r.getStatements()) {
            System.out.println("  " + s.getType() + " line=" + s.getLine());
        }
        System.out.println("--- sequences ---");
        for (Db2SequenceInfo s : r.getSequences()) {
            System.out.println("  op=" + s.getOperation() + " seq=" + s.getSequenceName() + " line=" + s.getLine());
        }
    }

    // ── Concept 2a/2b/2c: simple recursive CTE — anchor branch base table, recursive branch
    // base table, and the CTE's own name must NOT be wrongly recorded as an external table in
    // the recursive branch. ──

    private static final String SIMPLE_RECURSIVE_CTE_SRC =
        "CREATE PROCEDURE p_recursive_simple(IN p_root INTEGER)\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_cnt INTEGER;\n"
      + "  WITH emp_hier (emp_id, mgr_id, lvl) AS (\n"
      + "    SELECT e.emp_id, e.mgr_id, 1 FROM employees e WHERE e.mgr_id IS NULL\n"
      + "    UNION ALL\n"
      + "    SELECT e2.emp_id, e2.mgr_id, h.lvl + 1 FROM employees e2\n"
      + "    INNER JOIN emp_hier h ON e2.mgr_id = h.emp_id\n"
      + "  )\n"
      + "  SELECT COUNT(*) INTO v_cnt FROM emp_hier;\n"
      + "END";

    @Test
    @DisplayName("Concept 2: simple recursive CTE — anchor base table, recursive base table, own-name not mis-recorded")
    void simpleRecursiveCte() {
        Db2RoutineInfo r = engine.parse(SIMPLE_RECURSIVE_CTE_SRC, "p_recursive_simple", null);
        dump("SimpleRecursiveCte", r);

        long employeesOccurrences = r.getTableOps().stream()
                .filter(t -> "EMPLOYEES".equalsIgnoreCase(t.getTableName())).count();
        boolean anchorAliasSeen = r.getTableOps().stream()
                .anyMatch(t -> "EMPLOYEES".equalsIgnoreCase(t.getTableName()) && "e".equalsIgnoreCase(t.getAlias()));
        boolean recursiveAliasSeen = r.getTableOps().stream()
                .anyMatch(t -> "EMPLOYEES".equalsIgnoreCase(t.getTableName()) && "e2".equalsIgnoreCase(t.getAlias()));
        boolean ownNameWronglyCaptured = r.getTableOps().stream()
                .anyMatch(t -> "emp_hier".equalsIgnoreCase(t.getTableName()));

        System.out.println(">>> employeesOccurrences(expect 2, one per branch)=" + employeesOccurrences
                + " anchorAliasSeen(e)=" + anchorAliasSeen
                + " recursiveAliasSeen(e2)=" + recursiveAliasSeen
                + " ownNameWronglyCapturedAsTable(shouldBeFalse)=" + ownNameWronglyCaptured);

        // Document actual behavior as assertions (audit evidence, not a fix mandate).
        assertTrue(anchorAliasSeen, "anchor branch base table 'employees' (alias e) must be captured");
        assertTrue(recursiveAliasSeen, "recursive branch base table 'employees' (alias e2) must be captured");
        assertFalse(ownNameWronglyCaptured, "CTE's own name 'emp_hier' must NOT appear as an external table reference");
    }

    // ── Concept 2d: multi-level chained recursive CTEs — a second WITH-defined name (built on
    // top of a recursive CTE) referencing the first CTE. Checks whether the second CTE's
    // reference to the first CTE's name is wrongly treated as a base table. ──

    private static final String CHAINED_RECURSIVE_CTE_SRC =
        "CREATE PROCEDURE p_recursive_chained(IN p_root INTEGER)\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_cnt INTEGER;\n"
      + "  WITH emp_hier (emp_id, mgr_id, lvl) AS (\n"
      + "    SELECT e.emp_id, e.mgr_id, 1 FROM employees e WHERE e.mgr_id IS NULL\n"
      + "    UNION ALL\n"
      + "    SELECT e2.emp_id, e2.mgr_id, h.lvl + 1 FROM employees e2\n"
      + "    INNER JOIN emp_hier h ON e2.mgr_id = h.emp_id\n"
      + "  ),\n"
      + "  hier_with_dept AS (\n"
      + "    SELECT eh.emp_id, eh.lvl, d.dept_name\n"
      + "    FROM emp_hier eh\n"
      + "    INNER JOIN departments d ON eh.emp_id = d.owner_emp_id\n"
      + "  )\n"
      + "  SELECT COUNT(*) INTO v_cnt FROM hier_with_dept;\n"
      + "END";

    @Test
    @DisplayName("Concept 2d: multi-level chained recursive CTE — non-recursive CTE referencing a recursive one")
    void chainedRecursiveCte() {
        Db2RoutineInfo r = engine.parse(CHAINED_RECURSIVE_CTE_SRC, "p_recursive_chained", null);
        dump("ChainedRecursiveCte", r);

        boolean employeesCaptured = r.getTableOps().stream()
                .anyMatch(t -> "EMPLOYEES".equalsIgnoreCase(t.getTableName()));
        boolean departmentsCaptured = r.getTableOps().stream()
                .anyMatch(t -> "DEPARTMENTS".equalsIgnoreCase(t.getTableName()));
        boolean firstCteNameWronglyCapturedInSecondCte = r.getTableOps().stream()
                .anyMatch(t -> "emp_hier".equalsIgnoreCase(t.getTableName()));
        boolean secondCteNameWronglyCaptured = r.getTableOps().stream()
                .anyMatch(t -> "hier_with_dept".equalsIgnoreCase(t.getTableName()));
        long cteStatementsRecorded = r.getStatements().stream().filter(s -> "CTE".equals(s.getType())).count();

        System.out.println(">>> employeesCaptured=" + employeesCaptured
                + " departmentsCaptured=" + departmentsCaptured
                + " firstCteNameWronglyCapturedInSecondCte(shouldBeFalse)=" + firstCteNameWronglyCapturedInSecondCte
                + " secondCteNameWronglyCaptured(shouldBeFalse)=" + secondCteNameWronglyCaptured
                + " cteStatementsRecorded(2 expected)=" + cteStatementsRecorded);

        assertTrue(employeesCaptured, "base table 'employees' must be captured inside the first CTE");
        assertTrue(departmentsCaptured, "base table 'departments' must be captured inside the second CTE");
        assertFalse(firstCteNameWronglyCapturedInSecondCte,
                "first CTE's name 'emp_hier' referenced from the second CTE must NOT be recorded as a base table");
        assertFalse(secondCteNameWronglyCaptured, "second CTE's own name must NOT be recorded as a base table");
    }

    // ── Concept 3a: GENERATED ALWAYS AS IDENTITY — DDL is entirely out of grammar scope
    // (no CREATE TABLE rule exists). Not applicable; no test — see class javadoc for grep
    // evidence. ──

    // ── Concept 3b: NEXTVAL FOR seq_name / NEXT VALUE FOR seq_name inside INSERT ... VALUES,
    // real target table must still be captured alongside the sequence usage. ──

    private static final String SEQUENCE_IN_INSERT_SRC =
        "CREATE PROCEDURE p_seq_insert()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  INSERT INTO orders (order_id, cust_id, status)\n"
      + "    VALUES (NEXT VALUE FOR order_seq, 100, 'NEW');\n"
      + "  INSERT INTO shipments (shipment_id, order_id)\n"
      + "    VALUES (NEXTVAL FOR shipment_seq, 100);\n"
      + "END";

    @Test
    @DisplayName("Concept 3b: NEXT VALUE FOR / NEXTVAL FOR sequence usage inside INSERT — real target table not dropped")
    void sequenceUsageInInsert() {
        Db2RoutineInfo r = engine.parse(SEQUENCE_IN_INSERT_SRC, "p_seq_insert", null);
        dump("SequenceInInsert", r);

        boolean ordersInsertCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == Db2TableOpInfo.DmlType.INSERT);
        boolean shipmentsInsertCaptured = r.getTableOps().stream()
                .anyMatch(t -> "shipments".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == Db2TableOpInfo.DmlType.INSERT);
        boolean orderSeqRecordedDistinctly = r.getSequences().stream()
                .anyMatch(s -> "order_seq".equalsIgnoreCase(s.getSequenceName()));
        boolean shipmentSeqRecordedDistinctly = r.getSequences().stream()
                .anyMatch(s -> "shipment_seq".equalsIgnoreCase(s.getSequenceName()));
        boolean seqNameLeakedAsPhantomTable = r.getTableOps().stream()
                .anyMatch(t -> "order_seq".equalsIgnoreCase(t.getTableName())
                        || "shipment_seq".equalsIgnoreCase(t.getTableName()));

        System.out.println(">>> ordersInsertCaptured=" + ordersInsertCaptured
                + " shipmentsInsertCaptured=" + shipmentsInsertCaptured
                + " orderSeqRecordedDistinctly=" + orderSeqRecordedDistinctly
                + " shipmentSeqRecordedDistinctly=" + shipmentSeqRecordedDistinctly
                + " seqNameLeakedAsPhantomTable(shouldBeFalse)=" + seqNameLeakedAsPhantomTable);

        assertTrue(ordersInsertCaptured, "real target table 'orders' must be captured despite sequence usage in VALUES");
        assertTrue(shipmentsInsertCaptured, "real target table 'shipments' must be captured despite sequence usage in VALUES");
        assertTrue(orderSeqRecordedDistinctly, "order_seq must be recorded as a sequence reference (NEXT VALUE FOR)");
        assertTrue(shipmentSeqRecordedDistinctly, "shipment_seq must be recorded as a sequence reference (NEXTVAL FOR)");
        assertFalse(seqNameLeakedAsPhantomTable, "sequence names must not be recorded as phantom table ops");
    }

    // ── Concept 3c: PREVVAL FOR seq_name used in a plain SELECT (read of current sequence
    // value), alongside a real FROM table — both must be distinctly recognized. ──

    private static final String SEQUENCE_PREVVAL_SELECT_SRC =
        "CREATE PROCEDURE p_seq_prevval(IN p_id INTEGER)\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_last INTEGER;\n"
      + "  SELECT PREVVAL FOR order_seq INTO v_last FROM orders WHERE order_id = p_id;\n"
      + "END";

    @Test
    @DisplayName("Concept 3c: PREVVAL FOR seq_name in SELECT — sequence recognized distinctly, real FROM table not dropped")
    void sequencePrevvalInSelect() {
        Db2RoutineInfo r = engine.parse(SEQUENCE_PREVVAL_SELECT_SRC, "p_seq_prevval", null);
        dump("SequencePrevvalInSelect", r);

        boolean ordersSelectCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        boolean prevvalRecordedDistinctly = r.getSequences().stream()
                .anyMatch(s -> "order_seq".equalsIgnoreCase(s.getSequenceName())
                        && "PREVVAL".equalsIgnoreCase(s.getOperation()));
        boolean seqNameLeakedAsPhantomTable = r.getTableOps().stream()
                .anyMatch(t -> "order_seq".equalsIgnoreCase(t.getTableName()));

        System.out.println(">>> ordersSelectCaptured=" + ordersSelectCaptured
                + " prevvalRecordedDistinctly=" + prevvalRecordedDistinctly
                + " seqNameLeakedAsPhantomTable(shouldBeFalse)=" + seqNameLeakedAsPhantomTable);

        assertTrue(ordersSelectCaptured, "real FROM table 'orders' must be captured despite PREVVAL FOR usage");
        assertTrue(prevvalRecordedDistinctly, "order_seq must be recorded with operation PREVVAL");
        assertFalse(seqNameLeakedAsPhantomTable, "sequence names must not be recorded as phantom table ops");
    }
}
