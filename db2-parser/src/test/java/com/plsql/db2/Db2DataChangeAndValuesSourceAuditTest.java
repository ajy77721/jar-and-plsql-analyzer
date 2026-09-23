package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Third-round deep probe (2026-09-19), second sweep — two further FROM-clause source forms that
 * were mis-parsed, both found by systematically walking DB2's table-reference grammar rather
 * than by a reported failure.
 *
 * <ol>
 *   <li><b>Data-change-table-reference</b> — {@code SELECT … FROM FINAL TABLE (INSERT …)},
 *   {@code OLD TABLE (UPDATE …)}, {@code NEW TABLE (…)}. FINAL/OLD/NEW are ordinary identifiers
 *   in this grammar, so the introducer word alone matched {@code atomTable} and was reported as
 *   a BOGUS base table named "FINAL"/"OLD", while the parenthesised DML was swallowed by the
 *   permissive {@code selectRest} catch-all — meaning the routine's actual WRITE was completely
 *   invisible to lineage. Fixed by the {@code atomDataChangeTable} alternative plus
 *   {@code dataChangeStatement} / {@code dataChangeTail} in {@code Db2Parser.g4}.</li>
 *
 *   <li><b>VALUES row-constructor as a derived table</b> — {@code FROM (VALUES (1),(2)) AS v(x)}.
 *   {@code atomFunc} matched only the first {@code VALUES (1)} and then could not match the
 *   {@code ,(2)} that followed, so the statement failed. Measured consequence: in
 *   {@code FROM (VALUES …) AS v(x) JOIN t1 ON …} the real table T1 was LOST outright. Fixed by
 *   the {@code atomValues} alternative.</li>
 * </ol>
 *
 * <p>A VALUES list and a table/array function are derived sources, not base tables, so a query
 * whose ONLY source is one of them correctly reports zero table ops — the assertions below
 * therefore pin the survival of the REAL tables around them, which is what lineage depends on.
 */
@DisplayName("AUDIT R3: data-change-table-references and VALUES derived tables in FROM")
class Db2DataChangeAndValuesSourceAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private Db2RoutineInfo parse(String body) {
        return engine.parse("CREATE PROCEDURE zz()\nLANGUAGE SQL\nBEGIN\n" + body + "\nEND",
                "test_proc", null);
    }

    private static List<String> tables(Db2RoutineInfo r) {
        return r.getTableOps().stream()
                .map(Db2TableOpInfo::getTableName)
                .filter(java.util.Objects::nonNull)
                .map(String::toUpperCase)
                .toList();
    }

    private static String describe(Db2RoutineInfo r) {
        StringBuilder sb = new StringBuilder();
        for (Db2TableOpInfo t : r.getTableOps()) {
            sb.append('[').append(t.getDmlType()).append(' ')
              .append(t.getTableName() == null ? "?" : t.getTableName().toUpperCase()).append(']');
        }
        return sb.toString();
    }

    private static boolean hasOp(Db2RoutineInfo r, Db2TableOpInfo.DmlType type, String table) {
        return r.getTableOps().stream().anyMatch(t ->
                t.getDmlType() == type
                        && t.getTableName() != null
                        && t.getTableName().equalsIgnoreCase(table));
    }

    // ── 1. Data-change-table-reference ───────────────────────────────────────────────────────

    @Test
    @DisplayName("FINAL TABLE (INSERT …) — the INSERT must be visible, and no bogus FINAL table")
    void finalTableInsert() {
        Db2RoutineInfo r = parse("SELECT id FROM FINAL TABLE (INSERT INTO t1 (a) VALUES (1));");

        assertTrue(hasOp(r, Db2TableOpInfo.DmlType.INSERT, "t1"),
                "the nested INSERT into T1 must be captured, got " + describe(r));
        assertFalse(tables(r).contains("FINAL"),
                "FINAL is a data-change-reference introducer, not a table, got " + describe(r));
    }

    @Test
    @DisplayName("OLD TABLE (UPDATE … SET …) — the UPDATE must be visible, and no bogus OLD table")
    void oldTableUpdate() {
        Db2RoutineInfo r = parse("SELECT id FROM OLD TABLE (UPDATE t1 SET a = 2 WHERE b = 1);");

        assertTrue(hasOp(r, Db2TableOpInfo.DmlType.UPDATE, "t1"),
                "the nested UPDATE of T1 must be captured, got " + describe(r));
        assertFalse(tables(r).contains("OLD"),
                "OLD is a data-change-reference introducer, not a table, got " + describe(r));
    }

    @Test
    @DisplayName("FINAL TABLE (DELETE …)")
    void finalTableDelete() {
        Db2RoutineInfo r = parse("SELECT id FROM FINAL TABLE (DELETE FROM t1 WHERE a = 1);");

        assertTrue(hasOp(r, Db2TableOpInfo.DmlType.DELETE, "t1"),
                "the nested DELETE of T1 must be captured, got " + describe(r));
        assertFalse(tables(r).contains("FINAL"), "no bogus FINAL table, got " + describe(r));
    }

    @Test
    @DisplayName("NEW TABLE (INSERT … SELECT …) — both the write target and the read source")
    void newTableInsertFromSelect() {
        Db2RoutineInfo r = parse(
                "SELECT id FROM NEW TABLE (INSERT INTO t1 (a) SELECT a FROM t2) AS n;");

        assertTrue(hasOp(r, Db2TableOpInfo.DmlType.INSERT, "t1"),
                "the write target T1 must be captured, got " + describe(r));
        assertTrue(hasOp(r, Db2TableOpInfo.DmlType.SELECT, "t2"),
                "the nested SELECT source T2 must not be swallowed as opaque tokens, got " + describe(r));
        assertFalse(tables(r).contains("NEW"), "no bogus NEW table, got " + describe(r));
    }

    @Test
    @DisplayName("GUARD: the FINAL/OLD/NEW predicate must not steal an ordinary table named OLD")
    void dataChangePredicateDoesNotOverreach() {
        Db2RoutineInfo r = parse("SELECT a FROM old JOIN new_tbl n ON n.id = old.id;");

        assertTrue(tables(r).contains("OLD"),
                "a real table genuinely named OLD must still be captured, got " + describe(r));
        assertTrue(tables(r).contains("NEW_TBL"), "NEW_TBL must be captured, got " + describe(r));
    }

    // ── 2. VALUES row constructor as a derived table ─────────────────────────────────────────

    @Test
    @DisplayName("(VALUES (1),(2)) AS v(x) JOINed to a real table — the real table must survive")
    void valuesDerivedTableJoinedFirst() {
        Db2RoutineInfo r = parse(
                "SELECT * FROM (VALUES (1),(2)) AS v(x) JOIN t1 ON t1.id = v.x;");

        assertTrue(tables(r).contains("T1"),
                "T1 was lost outright before the atomValues fix, got " + describe(r));
        assertFalse(tables(r).contains("VALUES"),
                "the row constructor must not be reported as a base table, got " + describe(r));
    }

    @Test
    @DisplayName("real table JOINed to (VALUES …) on the right-hand side")
    void valuesDerivedTableJoinedSecond() {
        Db2RoutineInfo r = parse(
                "SELECT * FROM t1 JOIN (VALUES (1),(2)) AS v(x) ON t1.id = v.x;");

        assertTrue(tables(r).contains("T1"), "T1 must be captured, got " + describe(r));
        assertFalse(tables(r).contains("VALUES"), "no bogus VALUES table, got " + describe(r));
    }

    @Test
    @DisplayName("(VALUES …) alongside a real table in a comma-separated FROM list")
    void valuesAlongsideRealTable() {
        Db2RoutineInfo r = parse("SELECT * FROM t1, (VALUES (1),(2)) AS v;");

        assertTrue(tables(r).contains("T1"), "T1 must be captured, got " + describe(r));
    }

    @Test
    @DisplayName("a statement following a VALUES-only query must still be analysed")
    void statementAfterValuesOnlyQuery() {
        Db2RoutineInfo r = parse(
                "SELECT * FROM (VALUES (1)) AS v;\n  INSERT INTO t9 (a) VALUES (1);");

        assertTrue(hasOp(r, Db2TableOpInfo.DmlType.INSERT, "t9"),
                "the following INSERT must not be lost, got " + describe(r));
    }

    @Test
    @DisplayName("already-correct: an array/table function source does not lose its JOIN partner")
    void functionSourceKeepsJoinPartner() {
        Db2RoutineInfo r = parse("SELECT * FROM UNNEST(my_arr) AS u JOIN t1 ON t1.id = u.v;");

        assertTrue(tables(r).contains("T1"), "T1 must be captured, got " + describe(r));
        assertFalse(tables(r).contains("UNNEST"),
                "UNNEST(...) is a derived source, not a base table, got " + describe(r));
    }
}
