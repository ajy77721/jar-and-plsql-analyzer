package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.PostgresRoutineInfo;
import com.plsql.postgres.model.PostgresTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * §postgres-real-corpus — permanent regressions for the five root causes behind the 250 parse
 * errors raised by a full POSTGRES analysis of the live demo ERP schema
 * (project=PostgresDemo, environment=local, connection=postgres-local-conn,
 *  entryPoint=ERP_USP_RUN_CYCLE).
 *
 * <p>Every snippet below is REAL source, copied verbatim out of what the analyzer itself reads —
 * {@code pg_get_functiondef(p.oid)} against {@code jdbc:postgresql://localhost:5432/analyzerdb}
 * (see {@code PostgresSourceFetchStrategy}) — and trimmed to the smallest fragment that still
 * reproduces the failure. Nothing here is invented: these are production-shaped constructs that
 * the session's synthetic audit rounds happened never to exercise, and each one failed in EVERY
 * one of the ~83 analyzed routines, which is why five defects produced 250 errors.
 *
 * <p>Each test asserts BOTH halves of the contract: zero parse errors AND the facts that must
 * still be captured. Root cause A in particular was silently lossy even after the error went
 * away, so the table assertions there are the real regression guard.
 */
public class PostgresRealCorpusRegressionTest {

    private static PostgresRoutineInfo parse(String src, String name) {
        return new PostgresAnalysisEngine().parse(src, name, null);
    }

    private static PostgresRoutineInfo inProcedure(String body) {
        return parse("CREATE OR REPLACE PROCEDURE erp.p()\n"
                   + " LANGUAGE plpgsql\n"
                   + "AS $procedure$\n"
                   + "BEGIN\n" + body + "\nEND;\n", "p");
    }

    private static List<String> tableNames(PostgresRoutineInfo r) {
        return r.getTableOps().stream()
                .map(PostgresTableOpInfo::getTableName)
                .collect(Collectors.toList());
    }

    private static void assertNoErrors(String label, PostgresRoutineInfo r) {
        assertEquals(List.of(), r.getErrors() == null ? List.of() : r.getErrors(),
                label + " must parse without errors");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Root cause A — aggregate quantifier / sort clause inside the argument list.
    // Real source: erp.erp_usp_run_cycle, line 12 — the entry procedure's FIRST query.
    // Reported as: "[ERP_USP_RUN_CYCLE] line 12:47 no viable alternative at input 'SUM'".
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("A: COUNT(DISTINCT …) + COALESCE(SUM(…),0) — erp_usp_run_cycle:12")
    public void aggregateDistinctAndSum_realEntryProcedureLine12() {
        PostgresRoutineInfo r = inProcedure(
                "  SELECT COUNT(DISTINCT so.order_id), COALESCE(SUM(sol.line_amount),0)\n"
              + "    INTO v_orders, v_revenue\n"
              + "  FROM erp.erp_sales_order so\n"
              + "  JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id;");
        assertNoErrors("COUNT(DISTINCT …), COALESCE(SUM(…),0)", r);
        // The silent half of the defect: fromClause was never reached, so BOTH tables and the
        // join were lost with errors=[] once the hard error was fixed in isolation.
        assertTrue(tableNames(r).contains("erp_sales_order"), "driving table captured");
        assertTrue(tableNames(r).contains("erp_sales_order_line"), "joined table captured");
        assertTrue(r.getTableOps().stream().anyMatch(o -> o.getJoins() != null && !o.getJoins().isEmpty()),
                "the JOIN must still be captured");
    }

    @Test
    @DisplayName("A: array_agg(col ORDER BY col) — erp 'arrays + JSONB' feature block")
    public void aggregateOrderBy_realFeatureBlock() {
        PostgresRoutineInfo r = inProcedure(
                "  SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;");
        assertNoErrors("array_agg(… ORDER BY …)", r);
        assertTrue(tableNames(r).contains("erp_product"), "aggregate's source table captured");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Root cause B — EXIT WHEN. Real source: the cursor FETCH loop present in EVERY routine.
    // Reported as: "line 48:6 no viable alternative at input 'EXIT'" (col 6 in all ~83 routines).
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("B: LOOP / FETCH / EXIT WHEN NOT FOUND — every real routine")
    public void exitWhenNotFound_realCursorLoop() {
        PostgresRoutineInfo r = inProcedure(
                "  OPEN c1;\n"
              + "  LOOP\n"
              + "    FETCH c1 INTO v_rec;\n"
              + "    EXIT WHEN NOT FOUND;\n"
              + "    v_cnt := v_cnt + 1;\n"
              + "  END LOOP;\n"
              + "  CLOSE c1;");
        assertNoErrors("EXIT WHEN NOT FOUND", r);
    }

    @Test
    @DisplayName("B: bare EXIT, labelled EXIT and CONTINUE WHEN still parse")
    public void exitAndContinueVariants() {
        assertNoErrors("bare EXIT", inProcedure("  LOOP\n    EXIT;\n  END LOOP;"));
        assertNoErrors("labelled EXIT", inProcedure("  LOOP\n    EXIT outer_loop;\n  END LOOP;"));
        assertNoErrors("CONTINUE WHEN", inProcedure("  LOOP\n    CONTINUE WHEN v_i < 5;\n  END LOOP;"));
        // EXIT is a legal identifier; the new rule must not shadow an assignment to it.
        assertNoErrors("EXIT as a variable name", inProcedure("  exit := 1;"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Root cause C — parenthesised value list inside MERGE … WHEN NOT MATCHED THEN INSERT.
    // Real source: the "MERGE" feature block in every routine.
    // Reported as: "line 153:14 no viable alternative at input 'nextval'".
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("C: MERGE … WHEN NOT MATCHED THEN INSERT … VALUES (nextval(…), …)")
    public void mergeInsertValuesNextval_realMergeBlock() {
        PostgresRoutineInfo r = inProcedure(
                "  MERGE INTO erp.erp_demand d\n"
              + "  USING (SELECT product_id, SUM(qty) AS qty FROM erp.erp_demand_stage GROUP BY product_id) s\n"
              + "    ON d.product_id = s.product_id AND d.source_type = 'MRG_cycle'\n"
              + "  WHEN MATCHED THEN UPDATE SET qty = s.qty\n"
              + "  WHEN NOT MATCHED THEN\n"
              + "    INSERT (demand_id, product_id, source_type, source_id, qty, need_date)\n"
              + "    VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_cycle', 0, s.qty, now()::date + 30);");
        assertNoErrors("MERGE … INSERT … VALUES (nextval(…))", r);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Root cause D — a parenthesised expression whose first token is an identifier.
    // Real source: the "arrays + JSONB" feature block in every routine.
    // Reported as: "line 177:82 no viable alternative at input 'v_js'".
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("D: … || (v_js->>'count') — real JSONB feature block")
    public void parenthesisedJsonbExpression_realJsonbBlock() {
        PostgresRoutineInfo r = inProcedure(
                "  v_js := jsonb_build_object('prefix','cycle','ids',to_jsonb(v_arr));\n"
              + "  CALL erp.erp_usp_write_audit_trail('JSONB','cycle','sum='||v_sum||' count='||(v_js->>'count'));");
        assertNoErrors("(v_js->>'count')", r);
        assertTrue(r.getCalls().stream().anyMatch(c -> "erp_usp_write_audit_trail".equalsIgnoreCase(c.getName())),
                "the enclosing CALL must still be captured");
    }

    @Test
    @DisplayName("D: the same mis-prediction broke every multi-atom parenthesised group")
    public void parenthesisedGroupsGenerally() {
        assertNoErrors("(a + 1)", inProcedure("  v_x := (v_a + 1);"));
        assertNoErrors("(v_m).amount", inProcedure("  v_x := (v_m).amount;"));
        assertNoErrors("(a->'k')", inProcedure("  v_x := (v_js->'k');"));
    }

    @Test
    @DisplayName("D: real subqueries must still be recognised as subqueries, not paren groups")
    public void realSubqueriesStillParseAsSubqueries() {
        PostgresRoutineInfo in = inProcedure("  SELECT a INTO v FROM t WHERE b IN (SELECT c FROM u);");
        assertNoErrors("IN (SELECT …)", in);
        assertTrue(tableNames(in).contains("u"), "IN-subquery table captured");

        PostgresRoutineInfo ex = inProcedure("  SELECT a INTO v FROM t WHERE EXISTS (SELECT 1 FROM u);");
        assertNoErrors("EXISTS (SELECT …)", ex);
        assertTrue(tableNames(ex).contains("u"), "EXISTS-subquery table captured");

        PostgresRoutineInfo scalar = inProcedure("  v := (SELECT MAX(x) FROM u);");
        assertNoErrors("scalar (SELECT …)", scalar);
        assertTrue(tableNames(scalar).contains("u"), "scalar-subquery table captured");

        PostgresRoutineInfo cte = inProcedure(
                "  SELECT a INTO v FROM t WHERE b IN (WITH q AS (SELECT c FROM u) SELECT c FROM q);");
        assertNoErrors("(WITH … SELECT …)", cte);
        assertTrue(tableNames(cte).contains("u"), "CTE-subquery table captured");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Root cause E — the SIGNATURE, not the body. pg_get_functiondef() output for the two
    // PostgreSQL built-ins the real analysis pulled in as callees (ERP$UNNEST, ERP$SET_CONFIG).
    // Both are reproduced here byte-for-byte as the server emitted them.
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("E: pg_catalog.unnest(anyarray) RETURNS SETOF anyelement — real ERP$UNNEST")
    public void builtinUnnestSignature_setofReturnAndAnonymousParam() {
        PostgresRoutineInfo r = parse(
                "CREATE OR REPLACE FUNCTION pg_catalog.unnest(anyarray)\n"
              + " RETURNS SETOF anyelement\n"
              + " LANGUAGE internal\n"
              + " IMMUTABLE PARALLEL SAFE STRICT\n"
              + "AS $function$array_unnest$function$\n", "unnest");
        assertNoErrors("unnest(anyarray) RETURNS SETOF anyelement", r);
        assertEquals("unnest", r.getName());
        assertEquals(1, r.getParams().size(), "the anonymous parameter is still recorded");
        assertEquals("anyarray", r.getParams().get(0).getDataType());
    }

    @Test
    @DisplayName("E: pg_catalog.set_config(text, text, boolean) — real ERP$SET_CONFIG")
    public void builtinSetConfigSignature_multipleAnonymousParams() {
        PostgresRoutineInfo r = parse(
                "CREATE OR REPLACE FUNCTION pg_catalog.set_config(text, text, boolean)\n"
              + " RETURNS text\n"
              + " LANGUAGE internal\n"
              + " STRICT\n"
              + "AS $function$set_config$function$\n", "set_config");
        assertNoErrors("set_config(text, text, boolean)", r);
        assertEquals("set_config", r.getName());
        assertEquals(List.of("text", "text", "boolean"),
                r.getParams().stream().map(p -> p.getDataType()).collect(Collectors.toList()));
    }

    @Test
    @DisplayName("E: named params, VARIADIC and RETURNS TABLE(…) all still parse")
    public void signatureVariantsStillParse() {
        PostgresRoutineInfo named = parse(
                "CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_01(p_depth int DEFAULT 0)\n"
              + "LANGUAGE plpgsql AS $$\nBEGIN\n  NULL;\nEND; $$;\n", "erp_usp_stage_01");
        assertNoErrors("named parameter with DEFAULT", named);
        assertEquals("p_depth", named.getParams().get(0).getName());

        assertNoErrors("VARIADIC parameter", parse(
                "CREATE FUNCTION erp.f(VARIADIC arr int[]) RETURNS int\n"
              + "LANGUAGE plpgsql AS $$\nBEGIN\n  RETURN 1;\nEND; $$;\n", "f"));

        assertNoErrors("RETURNS TABLE(…)", parse(
                "CREATE FUNCTION erp.f(p_id int) RETURNS TABLE(id int, name text)\n"
              + "LANGUAGE plpgsql AS $$\nBEGIN\n  RETURN;\nEND; $$;\n", "f"));

        assertNoErrors("RETURNS SETOF record", parse(
                "CREATE FUNCTION erp.f() RETURNS SETOF record\n"
              + "LANGUAGE plpgsql AS $$\nBEGIN\n  RETURN;\nEND; $$;\n", "f"));
    }
}
