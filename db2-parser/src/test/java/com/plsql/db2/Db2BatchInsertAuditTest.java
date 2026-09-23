package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT ONLY (read-only investigation, 2026-09-17) — dedicated evidence for the multi-row
 * batch INSERT concept (`INSERT INTO tab VALUES (1,'a'), (2,'b'), (3,'c');`) against
 * {@code Db2AnalysisEngine}, plus a grammar-evidence check for any native ARRAY / Oracle-style
 * collection construct in the db2-parser grammar (see class-level notes below for the grep
 * evidence). This does not modify any source under db2-parser/src/main.
 *
 * ── Grammar evidence: ARRAY / collection support ──────────────────────────────────────────
 * `grep -in "array" db2-parser/src/main/antlr4/com/plsql/db2/grammar/Db2Lexer.g4
 *  db2-parser/src/main/antlr4/com/plsql/db2/grammar/Db2Parser.g4`               → NO MATCHES
 * `grep -in "collection|varray|nested table|table type|associative"  ...`        → NO MATCHES
 * There is no ARRAY token, no ARRAY[...] constructor rule, no VARRAY/nested-table/associative-
 * array rule anywhere in either grammar file. DB2 for i/LUW does have some native ARRAY support
 * in SQL PL, but this parser module implements none of it — variable declarations
 * (`declareVariableStmt`) accept only a bare `dataType` (id-based type name with optional
 * precision), so `v_arr INTEGER ARRAY[10]`-style declarations and `ARRAY[1,2,3]` constructor
 * literals are NOT recognized as a distinct construct. Such lines would either fail to parse
 * (surface as a parse error captured in {@code Db2RoutineInfo.getErrors()}) or, if inside a
 * loosely-eaten region (e.g. an expr/stmtTail context), be swallowed as opaque tokens with no
 * semantic capture. Oracle-style collection constructs (VARRAY/nested table/associative array
 * TYPE ... IS TABLE OF) are equally absent — expected, since this is a DB2 SQL PL grammar, not
 * PL/SQL. CONCLUSION: array/collection concept is NOT APPLICABLE to db2-parser as it stands;
 * no dedicated array test is added per the task's own conditional instruction.
 *
 * ── Batch insert concept ──────────────────────────────────────────────────────────────────
 * insertStatement : INSERT INTO? tableRef stmtTail SEMI? ;  (Db2Parser.g4)
 * stmtTail is a permissive, purely token-eating rule (stmtToken*) with balanced-paren handling
 * and no per-tuple production — so the VALUES(...),(...),(...) list is never split into
 * multiple statement-shaped subtrees. visitInsertStatement (Db2AnalysisVisitor.java) builds
 * exactly ONE Db2TableOpInfo from ctx.tableRef() before recursing into stmtTail via
 * visitChildren(ctx) — the row count of VALUES cannot cause double/triple counting by
 * construction.
 */
class Db2BatchInsertAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private static void dump(String label, Db2RoutineInfo r) {
        System.out.println("=== " + label + " errors === " + r.getErrors());
        System.out.println("--- tableOps ---");
        for (Db2TableOpInfo t : r.getTableOps()) {
            System.out.println("  " + t.getDmlType() + " " + t.qualifiedTable()
                    + " alias=" + t.getAlias() + " line=" + t.getLine()
                    + " sql=[" + t.getSqlText() + "]");
        }
        System.out.println("--- calls ---");
        for (Db2CallInfo c : r.getCalls()) {
            System.out.println("  " + c.getSchema() + "." + c.getName() + " line=" + c.getLine());
        }
    }

    private static long insertOpsFor(Db2RoutineInfo r, String table) {
        return r.getTableOps().stream()
                .filter(t -> t.getDmlType() == Db2TableOpInfo.DmlType.INSERT
                        && table.equalsIgnoreCase(t.getTableName()))
                .count();
    }

    // ── 1 row (baseline, no batch) ──────────────────────────────────────────────────────────

    private static final String ONE_ROW_SRC =
        "CREATE PROCEDURE p_insert_1row()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  INSERT INTO tab VALUES (1, 'a');\n"
      + "END";

    @Test
    @DisplayName("1-row INSERT VALUES — exactly one INSERT tableOp for tab")
    void oneRowInsert() {
        Db2RoutineInfo r = engine.parse(ONE_ROW_SRC, "p_insert_1row", null);
        dump("1row", r);
        long count = insertOpsFor(r, "tab");
        System.out.println(">>> insertOpsForTab(expect 1)=" + count);
        assertEquals(1, count, "single-row VALUES must yield exactly one INSERT tableOp");
    }

    // ── 3 rows — the exact concept snippet from the task ───────────────────────────────────

    private static final String THREE_ROW_SRC =
        "CREATE PROCEDURE p_insert_3row()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  INSERT INTO tab VALUES (1, 'a'), (2, 'b'), (3, 'c');\n"
      + "END";

    @Test
    @DisplayName("3-row multi-row VALUES INSERT — exactly one INSERT tableOp for tab, not three")
    void threeRowInsert() {
        Db2RoutineInfo r = engine.parse(THREE_ROW_SRC, "p_insert_3row", null);
        dump("3row", r);
        long count = insertOpsFor(r, "tab");
        System.out.println(">>> insertOpsForTab(expect 1, not 3)=" + count);
        assertEquals(1, count, "3-row multi-row VALUES must yield exactly one INSERT tableOp, not one per row");

        // Fixed bug: stmtTail's permissive `functionCall` alternative used to match the bare
        // `VALUES` keyword-as-identifier followed by `(` as if it were a routine call, recording
        // a spurious CALL entry "null.VALUES" once per INSERT...VALUES statement. VALUES is
        // unconditionally excluded now (see Db2AnalysisVisitor.visitFunctionCall).
        boolean spuriousValuesCall = r.getCalls().stream()
                .anyMatch(c -> "VALUES".equalsIgnoreCase(c.getName()));
        assertFalse(spuriousValuesCall, "bare VALUES(...) must never be recorded as a call; calls="
                + r.getCalls().stream().map(c -> c.getSchema() + "." + c.getName()).toList());
    }

    // ── 5 rows — larger batch, same table, to rule out any row-count-dependent scaling bug ──

    private static final String FIVE_ROW_SRC =
        "CREATE PROCEDURE p_insert_5row()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  INSERT INTO tab (id, name) VALUES\n"
      + "    (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');\n"
      + "END";

    @Test
    @DisplayName("5-row multi-row VALUES INSERT with explicit column list — still exactly one INSERT tableOp")
    void fiveRowInsert() {
        Db2RoutineInfo r = engine.parse(FIVE_ROW_SRC, "p_insert_5row", null);
        dump("5row", r);
        long count = insertOpsFor(r, "tab");
        System.out.println(">>> insertOpsForTab(expect 1, not 5)=" + count);
        assertEquals(1, count, "5-row multi-row VALUES with column list must yield exactly one INSERT tableOp");
    }

    // ── two separate batch-insert statements against two different tables in one routine ───

    private static final String TWO_STATEMENTS_SRC =
        "CREATE PROCEDURE p_insert_two_tables()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  INSERT INTO tab_a VALUES (1, 'a'), (2, 'b');\n"
      + "  INSERT INTO tab_b VALUES (10, 'x'), (20, 'y'), (30, 'z');\n"
      + "END";

    @Test
    @DisplayName("two separate multi-row VALUES INSERTs to different tables — one tableOp per statement, not merged/duplicated")
    void twoTablesBatchInsert() {
        Db2RoutineInfo r = engine.parse(TWO_STATEMENTS_SRC, "p_insert_two_tables", null);
        dump("twoTables", r);
        long countA = insertOpsFor(r, "tab_a");
        long countB = insertOpsFor(r, "tab_b");
        System.out.println(">>> insertOpsForTabA(expect 1)=" + countA + " insertOpsForTabB(expect 1)=" + countB);
        assertEquals(1, countA, "tab_a batch insert must yield exactly one INSERT tableOp");
        assertEquals(1, countB, "tab_b batch insert (3 rows) must yield exactly one INSERT tableOp");
        assertEquals(2, r.getTableOps().stream()
                .filter(t -> t.getDmlType() == Db2TableOpInfo.DmlType.INSERT).count(),
                "total INSERT tableOps across the routine must be exactly 2 (one per statement)");

        // Fixed bug: sqlText for one statement used to bleed into the next statement's text
        // (and the last statement's into the trailing END), because StatementTextCapture always
        // scanned forward from ctx.getStop()+1 looking for a terminator, even when ctx already
        // ended in its own semicolon. Each statement's captured sqlText must now be bounded to
        // itself.
        Db2TableOpInfo opA = r.getTableOps().stream()
                .filter(t -> "tab_a".equalsIgnoreCase(t.getTableName())).findFirst().orElseThrow();
        Db2TableOpInfo opB = r.getTableOps().stream()
                .filter(t -> "tab_b".equalsIgnoreCase(t.getTableName())).findFirst().orElseThrow();
        assertEquals("INSERT INTO tab_a VALUES (1, 'a'), (2, 'b');", opA.getSqlText(),
                "tab_a's sqlText must not bleed into tab_b's statement");
        assertEquals("INSERT INTO tab_b VALUES (10, 'x'), (20, 'y'), (30, 'z');", opB.getSqlText(),
                "tab_b's sqlText must not bleed into the trailing END");
    }
}
