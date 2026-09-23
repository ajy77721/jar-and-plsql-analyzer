package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT ONLY (read-only investigation, 2026-09-17) — printed evidence for two concepts against
 * {@code Db2AnalysisEngine}, mirroring the existing pattern in {@code Db2DmlCaptureGapAuditTest}.
 * Each routine below is a single, self-contained CREATE PROCEDURE body.
 *
 * Concept 1: NULL-handling functions (COALESCE / VALUE-as-NVL) interacting with dynamic SQL
 *            (EXECUTE IMMEDIATE) and with CASE inside a real (non-dynamic) query.
 * Concept 2: GROUP BY extensions (ROLLUP / CUBE / GROUPING SETS) — grammar support check plus
 *            whether they desync the FROM-clause table capture.
 *
 * Grammar-support facts established by grep of
 * src/main/antlr4/com/plsql/db2/grammar/Db2Lexer.g4 and Db2Parser.g4 BEFORE writing this test:
 *   - No COALESCE or NVL lexer token exists at all. COALESCE/NVL are therefore never keywords —
 *     they lex as plain ID tokens and are only ever recognized generically wherever a bare
 *     function-call shape (id LPAREN ... RPAREN) or a permissive token run accepts them.
 *   - VALUE and VALUES ARE lexer tokens (Db2Lexer.g4 lines 107-108), and VALUE is explicitly
 *     listed as an allowed `id` alternative (Db2Parser.g4 line 64), so VALUE(col,default) as an
 *     NVL-equivalent parses only via that carve-out, not via any COALESCE/NVL-specific rule.
 *   - selectRest / selectRestToken (Db2Parser.g4 lines 345-358) and stmtTail / stmtToken
 *     (lines 474-497) are flat, permissive token scanners with explicit balanced-paren and
 *     balanced-CASE...END alternatives — they do NOT implement a real recursive expression
 *     grammar for WHERE/CASE. The FROM clause is parsed by the dedicated fromClause rule BEFORE
 *     selectRest ever runs, so nothing inside a WHERE/CASE expression can retroactively affect
 *     whether the FROM table was already captured.
 *   - GROUP token exists in the lexer (line 52) purely as an `id`-exclusion keyword; there is NO
 *     BY, ROLLUP, CUBE, or GROUPING SETS token anywhere in Db2Lexer.g4, and no groupByClause rule
 *     anywhere in Db2Parser.g4 (confirmed via grep — zero hits for rollup|cube|grouping,
 *     case-insensitive, across both grammar files). GROUP BY (in any form) is therefore never a
 *     structured grammar construct here — it is always just more permissive token soup consumed
 *     by selectRestToken's catch-all + its LPAREN alt for the parenthesized column lists.
 */
class Db2NullHandlingAndGroupByAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private static void dump(String label, Db2RoutineInfo r) {
        System.out.println("=== " + label + " errors === " + r.getErrors());
        System.out.println("--- tableOps ---");
        for (Db2TableOpInfo t : r.getTableOps()) {
            System.out.println("  " + t.getDmlType() + " " + t.qualifiedTable()
                    + " alias=" + t.getAlias() + " line=" + t.getLine());
        }
        System.out.println("--- dynamicSql ---");
        for (Db2DynamicSqlInfo d : r.getDynamicSql()) {
            System.out.println("  kind=" + d.getKind() + " stmtName=" + d.getStmtName()
                    + " sqlSource=" + d.getSqlSource() + " unresolvedVar=" + d.getUnresolvedVarName()
                    + " line=" + d.getLine());
        }
    }

    // ── Concept 1a-i: COALESCE embedded in a FULLY-LITERAL EXECUTE IMMEDIATE string ──

    private static final String DYN_LITERAL_COALESCE_SRC =
        "CREATE PROCEDURE p_dyn_literal_coalesce()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  EXECUTE IMMEDIATE 'SELECT COALESCE(status, ''X'') FROM orders WHERE id = 1';\n"
      + "END";

    @Test
    @DisplayName("Concept 1a-i: fully-literal EXECUTE IMMEDIATE with COALESCE(...) — orders still extracted by FROM-regex")
    void dynamicLiteralCoalesce() {
        Db2RoutineInfo r = engine.parse(DYN_LITERAL_COALESCE_SRC, "p_dyn_literal_coalesce", null);
        dump("Concept1a-i", r);
        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == Db2TableOpInfo.DmlType.DYNAMIC);
        System.out.println(">>> ordersCapturedAsDynamic=" + ordersCaptured);
    }

    // ── Concept 1a-ii: COALESCE embedded in a SET v_sql = '<literal with FROM before the
    //    concatenation point>' CONCAT p_id ; EXECUTE IMMEDIATE v_sql — literal prefix contains
    //    the real FROM table, so it should resolve despite COALESCE and the doubled '' escape. ──

    private static final String DYN_CONCAT_COALESCE_SRC =
        "CREATE PROCEDURE p_dyn_concat_coalesce(IN p_id INTEGER)\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_sql VARCHAR(500);\n"
      + "  SET v_sql = 'SELECT COALESCE(status, ''X'') FROM orders WHERE id = ' CONCAT p_id;\n"
      + "  EXECUTE IMMEDIATE v_sql;\n"
      + "END";

    @Test
    @DisplayName("Concept 1a-ii: SET v_sql = '<literal FROM orders...>' CONCAT p_id; EXECUTE IMMEDIATE v_sql — literal prefix before CONCAT still resolves orders despite COALESCE/escaped quotes")
    void dynamicConcatWithCoalesceInLiteralPrefix() {
        Db2RoutineInfo r = engine.parse(DYN_CONCAT_COALESCE_SRC, "p_dyn_concat_coalesce", null);
        dump("Concept1a-ii", r);
        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == Db2TableOpInfo.DmlType.DYNAMIC);
        boolean unresolvedFlagged = !r.getDynamicSql().isEmpty()
                && r.getDynamicSql().get(0).getUnresolvedVarName() != null;
        System.out.println(">>> ordersCapturedAsDynamic=" + ordersCaptured + " unresolvedVarFlagged=" + unresolvedFlagged);
    }

    // ── Concept 1a-iii: same shape as the DML-capture-gap audit's DYNAMIC_CONCAT_SRC (Pattern 6),
    //    but with COALESCE in the literal head AND the table name itself built from concatenation
    //    (variable-interrupted before FROM's argument is known) — documents the existing
    //    "no table captured" gap is unaffected (neither fixed nor worsened) by COALESCE's presence. ──

    private static final String DYN_CONCAT_TABLE_NAME_COALESCE_SRC =
        "CREATE PROCEDURE p_dyn_concat_tablename(IN v_table_prefix VARCHAR(30))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_sql VARCHAR(500);\n"
      + "  SET v_sql = 'SELECT COALESCE(a,b) FROM ' || v_table_prefix || '_orders';\n"
      + "  EXECUTE IMMEDIATE v_sql;\n"
      + "END";

    @Test
    @DisplayName("Concept 1a-iii: COALESCE + table name built via concat (variable-interrupted) — no tableOp, matches pre-existing documented gap")
    void dynamicConcatCoalesceTableNameFromVariable() {
        Db2RoutineInfo r = engine.parse(DYN_CONCAT_TABLE_NAME_COALESCE_SRC, "p_dyn_concat_tablename", null);
        dump("Concept1a-iii", r);
        boolean anyTableOpCaptured = !r.getTableOps().isEmpty();
        System.out.println(">>> anyTableOpCaptured(expectFalse-documentedGap)=" + anyTableOpCaptured);
    }

    // ── Concept 1b: COALESCE nested inside CASE WHEN in a real (non-dynamic) query's WHERE ──

    private static final String CASE_COALESCE_WHERE_SRC =
        "CREATE PROCEDURE p_case_coalesce_where()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_cnt INTEGER;\n"
      + "  SELECT COUNT(*) INTO v_cnt FROM orders\n"
      + "  WHERE CASE WHEN COALESCE(a,b) = 1 THEN 1 ELSE 0 END = 1;\n"
      + "END";

    @Test
    @DisplayName("Concept 1b: SELECT * FROM orders WHERE CASE WHEN COALESCE(a,b)=1 THEN 1 ELSE 0 END = 1 — orders must still be captured")
    void caseWithCoalesceInWhereDoesNotBreakCapture() {
        Db2RoutineInfo r = engine.parse(CASE_COALESCE_WHERE_SRC, "p_case_coalesce_where", null);
        dump("Concept1b", r);
        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == Db2TableOpInfo.DmlType.SELECT);
        System.out.println(">>> ordersCapturedDespiteCaseCoalesceInWhere=" + ordersCaptured);
        assertTrue(ordersCaptured, "FROM orders must still be captured despite CASE/COALESCE in WHERE (permissive selectRest scanner design)");
    }

    // ── Concept 1b variant: VALUE(...) used as NVL-equivalent nested in CASE, in WHERE ──

    private static final String CASE_VALUE_AS_NVL_WHERE_SRC =
        "CREATE PROCEDURE p_case_value_nvl_where()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_cnt INTEGER;\n"
      + "  SELECT COUNT(*) INTO v_cnt FROM orders o\n"
      + "  WHERE CASE WHEN VALUE(o.status, 'X') = 'A' THEN 1 ELSE 0 END = 1;\n"
      + "END";

    @Test
    @DisplayName("Concept 1b variant: VALUE(col,default) as NVL-equivalent nested in CASE in WHERE — orders must still be captured")
    void caseWithValueAsNvlInWhereDoesNotBreakCapture() {
        Db2RoutineInfo r = engine.parse(CASE_VALUE_AS_NVL_WHERE_SRC, "p_case_value_nvl_where", null);
        dump("Concept1b-variant", r);
        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName())
                        && "o".equalsIgnoreCase(t.getAlias())
                        && t.getDmlType() == Db2TableOpInfo.DmlType.SELECT);
        System.out.println(">>> ordersCapturedDespiteCaseValueInWhere=" + ordersCaptured);
        assertTrue(ordersCaptured, "FROM orders o must still be captured despite CASE/VALUE(...) in WHERE");
    }

    // ── Concept 2a: GROUP BY ROLLUP(a,b) — grammar has no ROLLUP token/rule; consumed as
    //    permissive token soup. Table capture check. ──

    private static final String GROUP_BY_ROLLUP_SRC =
        "CREATE PROCEDURE p_group_by_rollup()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_cnt INTEGER;\n"
      + "  SELECT region, SUM(qty) INTO v_cnt, v_cnt FROM orders\n"
      + "  GROUP BY ROLLUP(region, cust_id);\n"
      + "END";

    @Test
    @DisplayName("Concept 2a: GROUP BY ROLLUP(region, cust_id) — orders must still be captured (no ROLLUP token/rule in grammar; grep-confirmed)")
    void groupByRollupDoesNotBreakCapture() {
        Db2RoutineInfo r = engine.parse(GROUP_BY_ROLLUP_SRC, "p_group_by_rollup", null);
        dump("Concept2a", r);
        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println(">>> ordersCapturedWithRollup=" + ordersCaptured + " errors=" + r.getErrors());
        assertTrue(ordersCaptured, "FROM orders must still be captured despite GROUP BY ROLLUP(...)");
    }

    // ── Concept 2b: GROUP BY CUBE(a,b) ──

    private static final String GROUP_BY_CUBE_SRC =
        "CREATE PROCEDURE p_group_by_cube()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_cnt INTEGER;\n"
      + "  SELECT region, SUM(qty) INTO v_cnt, v_cnt FROM orders\n"
      + "  GROUP BY CUBE(region, cust_id);\n"
      + "END";

    @Test
    @DisplayName("Concept 2b: GROUP BY CUBE(region, cust_id) — orders must still be captured")
    void groupByCubeDoesNotBreakCapture() {
        Db2RoutineInfo r = engine.parse(GROUP_BY_CUBE_SRC, "p_group_by_cube", null);
        dump("Concept2b", r);
        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println(">>> ordersCapturedWithCube=" + ordersCaptured + " errors=" + r.getErrors());
        assertTrue(ordersCaptured, "FROM orders must still be captured despite GROUP BY CUBE(...)");
    }

    // ── Concept 2c: GROUP BY GROUPING SETS ((a),(b),(a,b)) — nested parens ──

    private static final String GROUP_BY_GROUPING_SETS_SRC =
        "CREATE PROCEDURE p_group_by_grouping_sets()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_cnt INTEGER;\n"
      + "  SELECT region, cust_id, SUM(qty) INTO v_cnt, v_cnt, v_cnt FROM orders\n"
      + "  GROUP BY GROUPING SETS ((region), (cust_id), (region, cust_id));\n"
      + "END";

    @Test
    @DisplayName("Concept 2c: GROUP BY GROUPING SETS ((a),(b),(a,b)) — orders must still be captured despite nested paren groups")
    void groupByGroupingSetsDoesNotBreakCapture() {
        Db2RoutineInfo r = engine.parse(GROUP_BY_GROUPING_SETS_SRC, "p_group_by_grouping_sets", null);
        dump("Concept2c", r);
        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println(">>> ordersCapturedWithGroupingSets=" + ordersCaptured + " errors=" + r.getErrors());
        assertTrue(ordersCaptured, "FROM orders must still be captured despite GROUP BY GROUPING SETS(...)");
    }

    // ── Concept 2d: GROUP BY ROLLUP combined with a multi-table JOIN — combo stress case ──

    private static final String GROUP_BY_ROLLUP_JOIN_SRC =
        "CREATE PROCEDURE p_group_by_rollup_join()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_cnt INTEGER;\n"
      + "  SELECT o.region, SUM(s.qty) INTO v_cnt, v_cnt\n"
      + "  FROM orders o INNER JOIN shipments s ON o.order_id = s.order_id\n"
      + "  GROUP BY ROLLUP(o.region, s.product_id);\n"
      + "END";

    @Test
    @DisplayName("Concept 2d: multi-table JOIN + GROUP BY ROLLUP(...) — both orders and shipments must be captured")
    void groupByRollupWithJoinDoesNotBreakCapture() {
        Db2RoutineInfo r = engine.parse(GROUP_BY_ROLLUP_JOIN_SRC, "p_group_by_rollup_join", null);
        dump("Concept2d", r);
        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        boolean shipmentsJoinCaptured = r.getTableOps().stream()
                .anyMatch(t -> t.getJoins().stream()
                        .anyMatch(j -> "shipments".equalsIgnoreCase(j.getJoinedTable())));
        System.out.println(">>> ordersCaptured=" + ordersCaptured + " shipmentsJoinCaptured=" + shipmentsJoinCaptured
                + " errors=" + r.getErrors());
        assertTrue(ordersCaptured, "FROM orders must still be captured despite trailing GROUP BY ROLLUP(...)");
        assertTrue(shipmentsJoinCaptured, "JOIN shipments must still be captured despite trailing GROUP BY ROLLUP(...)");
    }
}
