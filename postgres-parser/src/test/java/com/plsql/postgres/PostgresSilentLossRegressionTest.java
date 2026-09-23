package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for the six "silent loss" defects found in the 2026-09-19 consolidated audit
 * pass. Every one of them produced missing output with {@code errors=[]} — nothing in the
 * reported errors told a reader that anything had been dropped.
 *
 * <ol>
 *   <li>{@code PERFORM <query>} (the SELECT-shaped form, as opposed to {@code PERFORM fn(a)})
 *       matched no rule and fell into otherStatement's catch-all: a routine whose only query was
 *       such a PERFORM reported ZERO tableOps, ZERO statements and ZERO calls.</li>
 *   <li>Every {@code ELSE} / {@code ELSIF} branch of an IF was swallowed by otherStatement,
 *       which was allowed to start on the ELSE/ELSIF keyword.</li>
 *   <li>{@code ELSIF} — the canonical PL/pgSQL spelling — had no lexer token at all.</li>
 *   <li>{@code TRUNCATE} had no rule; the truncated table was invisible.</li>
 *   <li>{@code INSERT … VALUES (…) ON CONFLICT (col) DO UPDATE SET …} lost its UPSERT
 *       classification and fabricated a phantom table op named {@code SET}.</li>
 *   <li>{@code CREATE TABLE … AS SELECT} recorded the new table but not the source table(s).</li>
 * </ol>
 */
class PostgresSilentLossRegressionTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private PostgresRoutineInfo run(String body) {
        return engine.parse("CREATE FUNCTION p() RETURNS void AS $$\nDECLARE v int;\nBEGIN\n"
                + body + "\nEND;\n$$ LANGUAGE plpgsql;\n", "p", null);
    }

    private static List<String> tables(PostgresRoutineInfo r) {
        return r.getTableOps().stream().map(PostgresTableOpInfo::getTableName).toList();
    }

    private static List<String> ops(PostgresRoutineInfo r) {
        return r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName()).toList();
    }

    private static List<String> stmtTypes(PostgresRoutineInfo r) {
        return r.getStatements().stream().map(PostgresStatementInfo::getType).toList();
    }

    // ── 1. PERFORM <query> ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("PERFORM with a FROM clause is a query, not an opaque statement")
    void performQueryCapturesTables() {
        PostgresRoutineInfo r = run("PERFORM o.id FROM orders o JOIN customers c ON c.id = o.cid;");
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertTrue(tables(r).contains("orders"), ops(r).toString());
        assertTrue(tables(r).contains("customers"), ops(r).toString());
    }

    @Test
    @DisplayName("PERFORM with nested subqueries and a correlated subquery in WHERE")
    void performWithNestedAndCorrelatedSubqueries() {
        PostgresRoutineInfo r = run("""
                PERFORM a.id FROM (
                  SELECT b.id FROM (SELECT c.id FROM deep_c c) b JOIN mid_m m ON m.id = b.id
                ) a WHERE a.id IN (SELECT x.id FROM outer_x x WHERE x.k = a.k);""");
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertTrue(tables(r).containsAll(List.of("deep_c", "mid_m", "outer_x")), ops(r).toString());
    }

    @Test
    @DisplayName("PERFORM fn(args) still records a call, not a query")
    void performCallFormStillWorks() {
        PostgresRoutineInfo r = run("PERFORM erp.recalc_totals(v);");
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertEquals(1, r.getCalls().size(), "calls: " + r.getCalls().size());
        assertEquals("recalc_totals", r.getCalls().get(0).getName());
    }

    // ── 2/3. ELSE and ELSIF branches ──────────────────────────────────────────────────────

    @Test
    @DisplayName("ELSE-branch DML is captured (was silently swallowed by otherStatement)")
    void elseBranchIsNotSwallowed() {
        PostgresRoutineInfo r = run("""
                IF v > 0 THEN
                  SELECT COUNT(*) INTO v FROM t_if;
                ELSE
                  SELECT COUNT(*) INTO v FROM t_else;
                END IF;""");
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertTrue(tables(r).contains("t_if"), ops(r).toString());
        assertTrue(tables(r).contains("t_else"), ops(r).toString());
    }

    @Test
    @DisplayName("ELSIF (canonical PL/pgSQL spelling) parses and its branch is captured")
    void elsifBranchIsCaptured() {
        PostgresRoutineInfo r = run("""
                IF v > 0 THEN
                  SELECT COUNT(*) INTO v FROM t_if;
                ELSIF v < 0 THEN
                  SELECT COUNT(*) INTO v FROM t_elsif;
                ELSE
                  SELECT COUNT(*) INTO v FROM t_else;
                END IF;""");
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertTrue(tables(r).containsAll(List.of("t_if", "t_elsif", "t_else")), ops(r).toString());
    }

    @Test
    @DisplayName("ELSEIF alias keeps working alongside ELSIF")
    void elseifAliasStillWorks() {
        PostgresRoutineInfo r = run("""
                IF v > 0 THEN
                  SELECT COUNT(*) INTO v FROM t_if;
                ELSEIF v < 0 THEN
                  SELECT COUNT(*) INTO v FROM t_elseif;
                END IF;""");
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertTrue(tables(r).containsAll(List.of("t_if", "t_elseif")), ops(r).toString());
    }

    @Test
    @DisplayName("\"truncate\" is still usable as an ordinary identifier")
    void truncateStillUsableAsIdentifier() {
        PostgresRoutineInfo r = run("SELECT truncate INTO v FROM t_cfg;");
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertTrue(tables(r).contains("t_cfg"), ops(r).toString());
    }

    // ── 4. TRUNCATE ───────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("TRUNCATE records a TRUNCATE table op and statement")
    void truncateIsCaptured() {
        PostgresRoutineInfo r = run("TRUNCATE TABLE staging.t_trunc;");
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertEquals(List.of("TRUNCATE:t_trunc"), ops(r));
        assertEquals("staging", r.getTableOps().get(0).getSchemaName());
        assertTrue(stmtTypes(r).contains("TRUNCATE"), stmtTypes(r).toString());
    }

    @Test
    @DisplayName("TRUNCATE with several targets, ONLY and RESTART IDENTITY CASCADE")
    void truncateMultiTarget() {
        PostgresRoutineInfo r = run("TRUNCATE ONLY t_a, t_b RESTART IDENTITY CASCADE;");
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertEquals(List.of("TRUNCATE:t_a", "TRUNCATE:t_b"), ops(r));
    }

    // ── 5. ON CONFLICT with a parenthesized conflict target ───────────────────────────────

    @Test
    @DisplayName("INSERT … VALUES … ON CONFLICT (col) DO UPDATE: UPSERT recorded, no phantom SET")
    void onConflictTargetDoUpdate() {
        PostgresRoutineInfo r = run(
                "INSERT INTO t_dup(a) VALUES (4) ON CONFLICT (a) DO UPDATE SET a = t_dup.a + 1;");
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertEquals(List.of("INSERT:t_dup"), ops(r), "phantom table op leaked");
        assertTrue(stmtTypes(r).contains("UPSERT"), stmtTypes(r).toString());
    }

    @Test
    @DisplayName("INSERT … VALUES … ON CONFLICT (col) DO NOTHING is classified as an UPSERT")
    void onConflictTargetDoNothing() {
        PostgresRoutineInfo r = run("INSERT INTO t_dup(a) VALUES (4) ON CONFLICT (a) DO NOTHING;");
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertEquals(List.of("INSERT:t_dup"), ops(r));
        assertTrue(stmtTypes(r).contains("UPSERT"), stmtTypes(r).toString());
    }

    // ── 6. CREATE TABLE … AS SELECT ───────────────────────────────────────────────────────

    @Test
    @DisplayName("CTAS records both the new table and the source table")
    void ctasCapturesSourceTable() {
        PostgresRoutineInfo r = run("CREATE TEMP TABLE tmp_x AS SELECT * FROM t_src JOIN t_dim d ON d.id = t_src.id;");
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertTrue(tables(r).contains("tmp_x"), ops(r).toString());
        assertTrue(tables(r).contains("t_src"), ops(r).toString());
        assertTrue(tables(r).contains("t_dim"), ops(r).toString());
    }

    @Test
    @DisplayName("Plain CREATE TABLE with column definitions is unaffected")
    void plainCreateTableUnaffected() {
        PostgresRoutineInfo r = run("CREATE TABLE t_new (id int, amt numeric DEFAULT 0);");
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertEquals(List.of("CREATE:t_new"), ops(r));
    }
}
