package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Third-round deep probe (2026-09-19) — regression coverage for two defects found while hunting
 * for MORE constructs with the round-2 "silent total routine loss" signature (empty tableOps AND
 * empty statements with {@code errors=[]}).
 *
 * <ol>
 *   <li><b>Temporal-table period specifications</b> ({@code FOR SYSTEM_TIME AS OF …},
 *   {@code FOR BUSINESS_TIME FROM … TO …}, {@code FOR SYSTEM_TIME BETWEEN … AND …}) — CONFIRMED
 *   silent total loss before the fix: every table of the query vanished, with no error recorded.
 *   Fixed by the {@code temporalSpec} / {@code temporalValue} rules in {@code Db2Parser.g4}
 *   (gated on a semantic predicate so a plain column named SYSTEM_TIME, the "FOR BIT DATA" type
 *   suffix and the FOR-loop keyword are all untouched).</li>
 *
 *   <li><b>Empty OLAP window clause</b> ({@code COUNT(*) OVER ()}) — NOT a total loss, but the
 *   window clause was recorded as a user-routine call edge literally named "OVER". Fixed by an
 *   explicit {@code OVER LPAREN atomFuncArgs RPAREN} alternative in {@code exprAtom}.</li>
 * </ol>
 *
 * <p>Also locks in the constructs probed in the same round that were found ALREADY CORRECT, so a
 * future grammar change cannot silently regress them: chained MERGE combined with a recursive
 * CTE, window functions over JOINs, a CTE referencing an earlier CTE in the same WITH clause,
 * {@code TABLE(udf(...))} in FROM, and three-part (federated/nickname) table names.
 */
@DisplayName("AUDIT R3: temporal tables, OLAP window clauses, and companion constructs")
class Db2TemporalAndWindowAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private Db2RoutineInfo parse(String sql) {
        return engine.parse(sql, "test_proc", null);
    }

    private static List<String> tables(Db2RoutineInfo r) {
        return r.getTableOps().stream()
                .map(Db2TableOpInfo::getTableName)
                .filter(java.util.Objects::nonNull)
                .map(String::toUpperCase)
                .toList();
    }

    private static List<String> calls(Db2RoutineInfo r) {
        return r.getCalls().stream()
                .map(Db2CallInfo::getName)
                .filter(java.util.Objects::nonNull)
                .toList();
    }

    /** The round-2 failure signature: nothing captured at all, and no error to show for it. */
    private static void assertNotSilentTotalLoss(Db2RoutineInfo r, String what) {
        assertFalse(r.getTableOps().isEmpty() && r.getStatements().isEmpty(),
                what + ": silent TOTAL ROUTINE LOSS — tableOps and statements are both empty"
                        + " (errors=" + r.getErrors() + ")");
    }

    // ── 1. Temporal tables — the newly fixed total-loss bug ──────────────────────────────────

    @Test
    @DisplayName("FOR SYSTEM_TIME AS OF <literal> — table must not be lost")
    void temporalSystemTimeAsOf() {
        Db2RoutineInfo r = parse(
                "CREATE PROCEDURE pc()\nLANGUAGE SQL\nBEGIN\n"
              + "  SELECT * FROM policy FOR SYSTEM_TIME AS OF '2020-01-01' WHERE id = 1;\n"
              + "END");

        assertNotSilentTotalLoss(r, "FOR SYSTEM_TIME AS OF");
        assertTrue(tables(r).contains("POLICY"), "POLICY must be captured, got " + tables(r));
    }

    @Test
    @DisplayName("FOR SYSTEM_TIME AS OF CURRENT TIMESTAMP + JOIN — both tables kept")
    void temporalAsOfCurrentTimestampWithJoin() {
        Db2RoutineInfo r = parse(
                "CREATE PROCEDURE pd()\nLANGUAGE SQL\nBEGIN\n"
              + "  SELECT p.id, c.name FROM policy FOR SYSTEM_TIME AS OF CURRENT TIMESTAMP p\n"
              + "  JOIN customers c ON c.id = p.cid;\n"
              + "END");

        assertNotSilentTotalLoss(r, "temporal + JOIN");
        assertTrue(tables(r).contains("POLICY"), "POLICY must be captured, got " + tables(r));
        assertTrue(tables(r).contains("CUSTOMERS"),
                "the JOINed CUSTOMERS must not be lost with it, got " + tables(r));
    }

    @Test
    @DisplayName("FOR BUSINESS_TIME FROM <v> TO <v>")
    void temporalBusinessTimeFromTo() {
        Db2RoutineInfo r = parse(
                "CREATE PROCEDURE pe()\nLANGUAGE SQL\nBEGIN\n"
              + "  SELECT * FROM policy FOR BUSINESS_TIME FROM '2020-01-01' TO '2021-01-01';\n"
              + "END");

        assertNotSilentTotalLoss(r, "FOR BUSINESS_TIME FROM..TO");
        assertTrue(tables(r).contains("POLICY"), "POLICY must be captured, got " + tables(r));
    }

    @Test
    @DisplayName("FOR SYSTEM_TIME BETWEEN <v> AND <v> with an explicit AS alias + JOIN")
    void temporalBetweenWithAlias() {
        Db2RoutineInfo r = parse(
                "CREATE PROCEDURE pf()\nLANGUAGE SQL\nBEGIN\n"
              + "  SELECT p.id FROM policy FOR SYSTEM_TIME BETWEEN '2020-01-01' AND '2021-01-01' AS p\n"
              + "  JOIN customers c ON c.id = p.cid WHERE p.id > 0;\n"
              + "END");

        assertNotSilentTotalLoss(r, "FOR SYSTEM_TIME BETWEEN");
        assertTrue(tables(r).contains("POLICY"), "POLICY must be captured, got " + tables(r));
        assertTrue(tables(r).contains("CUSTOMERS"), "CUSTOMERS must be captured, got " + tables(r));
    }

    @Test
    @DisplayName("temporal spec on the JOINed side, bound is a host variable")
    void temporalOnJoinedSide() {
        Db2RoutineInfo r = parse(
                "CREATE PROCEDURE pg(IN v TIMESTAMP)\nLANGUAGE SQL\nBEGIN\n"
              + "  SELECT * FROM orders o JOIN policy FOR SYSTEM_TIME AS OF v p ON p.id = o.pid;\n"
              + "END");

        assertNotSilentTotalLoss(r, "temporal on JOINed side");
        assertTrue(tables(r).contains("ORDERS"), "ORDERS must be captured, got " + tables(r));
        assertTrue(tables(r).contains("POLICY"), "POLICY must be captured, got " + tables(r));
    }

    @Test
    @DisplayName("GUARD: the temporalSpec predicate must not steal FOR BIT DATA, a FOR loop, "
               + "or a plain column named SYSTEM_TIME")
    void temporalPredicateDoesNotOverreach() {
        Db2RoutineInfo r = parse(
                "CREATE PROCEDURE ph()\nLANGUAGE SQL\nBEGIN\n"
              + "  DECLARE v CHAR(10) FOR BIT DATA;\n"
              + "  SELECT system_time FROM audit_tbl;\n"
              + "  FOR rw AS SELECT id FROM loop_tbl DO SET v = rw.id; END FOR;\n"
              + "END");

        assertTrue(tables(r).contains("AUDIT_TBL"),
                "a column named SYSTEM_TIME must not disturb the query, got " + tables(r));
        assertTrue(tables(r).contains("LOOP_TBL"),
                "the FOR-loop cursor query must still be captured, got " + tables(r));
    }

    // ── 2. OLAP window functions ─────────────────────────────────────────────────────────────

    @Test
    @DisplayName("OVER (PARTITION BY … ORDER BY …) combined with a JOIN")
    void windowPartitionByWithJoin() {
        Db2RoutineInfo r = parse(
                "CREATE PROCEDURE p4()\nLANGUAGE SQL\nBEGIN\n"
              + "  SELECT o.id, ROW_NUMBER() OVER (PARTITION BY o.cust_id ORDER BY o.dt DESC) AS rn, c.name\n"
              + "  FROM orders o JOIN customers c ON c.id = o.cust_id;\n"
              + "END");

        assertNotSilentTotalLoss(r, "window + JOIN");
        assertTrue(tables(r).contains("ORDERS"), "ORDERS must be captured, got " + tables(r));
        assertTrue(tables(r).contains("CUSTOMERS"), "CUSTOMERS must be captured, got " + tables(r));
    }

    @Test
    @DisplayName("windowed aggregate with an explicit ROWS frame over two JOINs")
    void windowWithFrameOverMultipleJoins() {
        Db2RoutineInfo r = parse(
                "CREATE PROCEDURE p5()\nLANGUAGE SQL\nBEGIN\n"
              + "  SELECT SUM(l.amt) OVER (PARTITION BY l.oid ORDER BY l.seq\n"
              + "         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running\n"
              + "  FROM lines l JOIN orders o ON o.id = l.oid LEFT JOIN customers c ON c.id = o.cust_id;\n"
              + "END");

        assertNotSilentTotalLoss(r, "windowed aggregate with frame");
        assertTrue(tables(r).containsAll(List.of("LINES", "ORDERS", "CUSTOMERS")),
                "all three joined tables must survive the window frame, got " + tables(r));
    }

    @Test
    @DisplayName("empty window spec COUNT(*) OVER () must not produce a call edge named OVER")
    void emptyWindowSpecIsNotACall() {
        Db2RoutineInfo r = parse(
                "CREATE PROCEDURE p6()\nLANGUAGE SQL\nBEGIN\n"
              + "  SELECT RANK() OVER (ORDER BY s.amt) AS r, COUNT(*) OVER () AS total\n"
              + "  FROM sales s JOIN region g ON g.id = s.rid;\n"
              + "END");

        assertTrue(tables(r).contains("SALES"), "SALES must be captured, got " + tables(r));
        assertTrue(tables(r).contains("REGION"), "REGION must be captured, got " + tables(r));
        assertTrue(calls(r).stream().noneMatch(n -> n.equalsIgnoreCase("OVER")),
                "the OLAP window clause must not be recorded as a call edge, got calls=" + calls(r));
    }

    // ── 3. Constructs probed and found ALREADY CORRECT — locked in against regression ────────

    @Test
    @DisplayName("already-correct: recursive CTE and two chained MERGEs in the SAME routine")
    void recursiveCteWithChainedMerges() {
        Db2RoutineInfo r = parse(
                "CREATE PROCEDURE p3()\nLANGUAGE SQL\nBEGIN\n"
              + "  WITH RECURSIVE rc (id) AS (SELECT id FROM base_a\n"
              + "       UNION ALL SELECT b.id FROM base_b b JOIN rc ON b.pid = rc.id)\n"
              + "  SELECT * FROM rc;\n"
              + "  MERGE INTO m_one o USING src_one s ON o.id = s.id\n"
              + "    WHEN MATCHED THEN UPDATE SET o.v = s.v;\n"
              + "  MERGE INTO m_two o USING src_two s ON o.id = s.id\n"
              + "    WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);\n"
              + "END");

        assertNotSilentTotalLoss(r, "recursive CTE + chained MERGE");
        assertTrue(tables(r).containsAll(List.of("BASE_A", "BASE_B", "M_ONE", "SRC_ONE", "M_TWO", "SRC_TWO")),
                "every side of both MERGEs and both CTE branches must be captured, got " + tables(r));
        assertFalse(tables(r).contains("RC"), "the CTE's own name must not be a base table, got " + tables(r));
    }

    @Test
    @DisplayName("already-correct: MERGE whose USING source is itself a recursive CTE")
    void mergeUsingRecursiveCte() {
        Db2RoutineInfo r = parse(
                "CREATE PROCEDURE p2()\nLANGUAGE SQL\nBEGIN\n"
              + "  MERGE INTO tgt_tbl t\n"
              + "  USING (WITH RECURSIVE anc (id, pid) AS (\n"
              + "           SELECT id, parent_id FROM emp_root\n"
              + "           UNION ALL\n"
              + "           SELECT e.id, e.parent_id FROM emp e JOIN anc a ON e.parent_id = a.id)\n"
              + "         SELECT id, pid FROM anc) s\n"
              + "  ON t.id = s.id\n"
              + "  WHEN MATCHED THEN UPDATE SET t.pid = s.pid;\n"
              + "END");

        assertNotSilentTotalLoss(r, "MERGE USING recursive CTE");
        assertTrue(tables(r).containsAll(List.of("TGT_TBL", "EMP_ROOT", "EMP")),
                "the MERGE target and both CTE branches must be captured, got " + tables(r));
    }

    @Test
    @DisplayName("already-correct: a CTE that references an earlier CTE in the same WITH clause")
    void cteReferencingEarlierCte() {
        Db2RoutineInfo r = parse(
                "CREATE PROCEDURE p7()\nLANGUAGE SQL\nBEGIN\n"
              + "  WITH c1 AS (SELECT id, amt FROM raw_tbl),\n"
              + "       c2 AS (SELECT id, SUM(amt) AS s FROM c1 GROUP BY id),\n"
              + "       c3 AS (SELECT c2.id FROM c2 JOIN other_tbl o ON o.id = c2.id)\n"
              + "  SELECT * FROM c3;\n"
              + "END");

        assertNotSilentTotalLoss(r, "chained CTEs");
        assertTrue(tables(r).contains("RAW_TBL"), "RAW_TBL must be captured, got " + tables(r));
        assertTrue(tables(r).contains("OTHER_TBL"), "OTHER_TBL must be captured, got " + tables(r));
        assertTrue(tables(r).stream().noneMatch(t -> t.equals("C1") || t.equals("C2") || t.equals("C3")),
                "no CTE name may be reported as a base table, got " + tables(r));
    }

    @Test
    @DisplayName("already-correct: TABLE(udf(...)) in FROM is a call, not a base table")
    void tableFunctionInFrom() {
        Db2RoutineInfo r = parse(
                "CREATE PROCEDURE p9(IN x INT)\nLANGUAGE SQL\nBEGIN\n"
              + "  SELECT t.a, o.b FROM orders o, TABLE(sch.some_udf(o.id, x)) AS t WHERE t.a = o.b;\n"
              + "END");

        assertTrue(tables(r).contains("ORDERS"), "ORDERS must be captured, got " + tables(r));
        assertFalse(tables(r).contains("TABLE"), "TABLE(...) must not be a base table, got " + tables(r));
        assertTrue(calls(r).stream().anyMatch(n -> n.equalsIgnoreCase("some_udf")),
                "the table function must be recorded as a call, got calls=" + calls(r));
    }

    @Test
    @DisplayName("already-correct: three-part (federated/nickname) table name")
    void threePartFederatedName() {
        Db2RoutineInfo r = parse(
                "CREATE PROCEDURE pa()\nLANGUAGE SQL\nBEGIN\n"
              + "  SELECT a.c1 FROM remotesrv.remschema.remtable a JOIN local_tbl l ON l.id = a.c1;\n"
              + "END");

        assertNotSilentTotalLoss(r, "three-part name");
        assertTrue(tables(r).contains("REMTABLE"),
                "the last segment is the table name, got " + tables(r));
        assertTrue(tables(r).contains("LOCAL_TBL"), "LOCAL_TBL must be captured, got " + tables(r));
    }
}
