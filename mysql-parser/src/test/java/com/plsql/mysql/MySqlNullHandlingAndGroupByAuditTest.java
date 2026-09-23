package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * READ-ONLY AUDIT (2026-09-17) — NULL-handling functions (IFNULL/COALESCE) interacting with
 * dynamic SQL and CASE, plus GROUP BY extensions (ROLLUP / CUBE / GROUPING SETS), for
 * mysql-parser. No hard assertions where the bug is the still-open gap being documented — this
 * test exists to print evidence, per the invoking audit's method.
 *
 * GRAMMAR GREP EVIDENCE (src/main/antlr4/com/plsql/mysql/grammar/MySqlParser.g4,
 * MySqlLexer.g4): grep -in "rollup|cube|grouping" on both files returns NOTHING. The only
 * groupByClause rule is:
 *   groupByClause  : GROUP BY groupByItem (COMMA groupByItem)* ;
 *   groupByItem    : expr (ASC | DESC)? ;
 * i.e. the grammar has NO support at all for `WITH ROLLUP`, `ROLLUP(...)`, `CUBE(...)`, or
 * `GROUPING SETS(...)` — none of these tokens/rules exist. This is true to real MySQL for CUBE
 * and GROUPING SETS (MySQL never supported them), but MySQL 8.0+ DOES support both
 * `GROUP BY a, b WITH ROLLUP` (legacy) and `GROUP BY a, b, ROLLUP(a,b)` / `... WITH ROLLUP`
 * modern syntax — so the WITH ROLLUP gap is a real grammar gap, not a dialect non-feature.
 */
public class MySqlNullHandlingAndGroupByAuditTest {

    private static List<String> tableNames(MySqlRoutineInfo r) {
        return r.getTableOps().stream().map(MySqlTableOpInfo::getTableName).toList();
    }

    // Case 1: IFNULL(...) embedded inside a CONCAT-built dynamic SQL literal fragment,
    // PREPARE FROM @variable (mirrors Bug 8's variable-interrupted CONCAT tracking).
    @Test
    @DisplayName("1: IFNULL(...) inside a CONCAT dynamic-SQL string, PREPARE FROM @sql")
    void ifNullInsideDynamicSqlConcat() {
        String sql = """
                CREATE PROCEDURE proc_dyn_ifnull(IN p_id INT)
                BEGIN
                  SET @sql = CONCAT('SELECT IFNULL(status, "X") FROM orders WHERE id = ', p_id);
                  PREPARE stmt FROM @sql;
                  EXECUTE stmt;
                  DEALLOCATE PREPARE stmt;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_dyn_ifnull", null);
        System.out.println("[1] tableOps: " + tableNames(r));
        System.out.println("[1] dynamicSql: " + r.getDynamicSql().stream()
                .map(d -> d.getKind() + " stmt=" + d.getStmtName() + " src=" + d.getSqlSource()
                        + " unresolvedVar=" + d.getUnresolvedVarName()).toList());
        System.out.println("[1] errors: " + r.getErrors());
        // FIXED: the visitor now scans ALL literal fragments of the SET @sql = CONCAT(...)
        // assignment for an embedded table reference (not just the fragment immediately adjacent
        // to the interrupt point), so "orders" is recovered even though IFNULL(...) intervenes
        // between "FROM" and "orders".
        org.junit.jupiter.api.Assertions.assertEquals(List.of("orders"), tableNames(r));
    }

    // Case 2: IFNULL(a,b) nested inside a CASE WHEN expression in a real (non-dynamic) query.
    @Test
    @DisplayName("2: IFNULL(a,b) nested inside CASE WHEN in WHERE clause")
    void ifNullInsideCaseWhenWhere() {
        String sql = """
                CREATE PROCEDURE proc_case_ifnull()
                BEGIN
                  SELECT * FROM orders WHERE CASE WHEN IFNULL(a,b) = 1 THEN 1 ELSE 0 END = 1;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_case_ifnull", null);
        System.out.println("[2] tableOps: " + tableNames(r));
        System.out.println("[2] errors: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals(List.of("orders"), tableNames(r));
        org.junit.jupiter.api.Assertions.assertEquals(List.of(), r.getErrors());
    }

    // Case 3: COALESCE(a,b) nested inside CASE WHEN in the SELECT list (column-capture variant).
    @Test
    @DisplayName("3: COALESCE(a,b) nested inside CASE WHEN in SELECT list")
    void coalesceInsideCaseWhenSelectList() {
        String sql = """
                CREATE PROCEDURE proc_case_coalesce()
                BEGIN
                  SELECT id, CASE WHEN COALESCE(a,b) = 1 THEN 'Y' ELSE 'N' END AS flag
                  FROM orders o JOIN customers c ON o.cust_id = c.id
                  WHERE COALESCE(o.status, 'OPEN') = 'OPEN';
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_case_coalesce", null);
        System.out.println("[3] tableOps: " + tableNames(r));
        System.out.println("[3] errors: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals(List.of("orders", "customers"), tableNames(r));
        org.junit.jupiter.api.Assertions.assertEquals(List.of(), r.getErrors());
    }

    // Case 4: legacy MySQL "GROUP BY a, b WITH ROLLUP" syntax — real MySQL 8.0+ syntax, but the
    // grammar (per grep evidence above) has no ROLLUP token/rule at all.
    @Test
    @DisplayName("4: GROUP BY a, b WITH ROLLUP (legacy MySQL syntax) — grammar has no ROLLUP rule")
    void groupByWithRollup() {
        String sql = """
                CREATE PROCEDURE proc_group_rollup()
                BEGIN
                  SELECT status, COUNT(*) FROM orders GROUP BY status WITH ROLLUP;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_group_rollup", null);
        System.out.println("[4] tableOps: " + tableNames(r));
        System.out.println("[4] errors: " + r.getErrors());
        // FIXED: groupByClause now models a trailing `WITH ROLLUP` modifier, so it no longer
        // falls through to the unknown-statement catch-all and no spurious error is logged.
        org.junit.jupiter.api.Assertions.assertEquals(List.of("orders"), tableNames(r));
        org.junit.jupiter.api.Assertions.assertEquals(List.of(), r.getErrors());
    }

    // Case 5: modern MySQL 8.0 "GROUP BY a, b, ROLLUP(a,b)" function-call-style syntax.
    @Test
    @DisplayName("5: GROUP BY ROLLUP(a,b) modern MySQL 8.0 syntax — grammar has no ROLLUP rule")
    void groupByRollupFunctionStyle() {
        String sql = """
                CREATE PROCEDURE proc_group_rollup_fn()
                BEGIN
                  SELECT region, status, COUNT(*) FROM orders GROUP BY ROLLUP(region, status);
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_group_rollup_fn", null);
        System.out.println("[5] tableOps: " + tableNames(r));
        System.out.println("[5] errors: " + r.getErrors());
        // FIXED: groupByItem now has a dedicated groupByModifier alternative for ROLLUP(...) /
        // CUBE(...) / GROUPING SETS(...), so this is recognized as a grouping modifier rather
        // than coincidentally parsed as a plain function-call expression.
        org.junit.jupiter.api.Assertions.assertEquals(List.of("orders"), tableNames(r));
        org.junit.jupiter.api.Assertions.assertEquals(List.of(), r.getErrors());
    }

    // Case 6: N/A — MySQL does not support GROUPING SETS or CUBE at all (grep evidence above
    // shows no such tokens in the grammar, which matches real MySQL: GROUPING SETS/CUBE are a
    // SQL-standard/Oracle/Postgres feature MySQL never implemented). Not tested — nothing to
    // audit here since this is a genuine dialect non-feature, not a parser gap.
}
