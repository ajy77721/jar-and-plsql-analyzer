package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.PostgresCallInfo;
import com.plsql.postgres.model.PostgresRoutineInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression suite for the "SQL keyword recorded as a call" class of false positive in
 * {@code PostgresAnalysisVisitor.visitFunctionCall}.
 *
 * <p>PostgresParser.g4's {@code id} rule deliberately admits non-structural keywords so they can
 * still be used as ordinary identifiers. A side effect is that any such keyword followed by a
 * parenthesised operand structurally matches
 * {@code functionCall : id (DOT id)? LPAREN functionArgs? RPAREN}. Only {@code VALUES} used to be
 * filtered out, so real source like
 *
 * <pre>DELETE FROM erp.erp_demand WHERE source_type IN ('SALES_ORDER','FORECAST');</pre>
 *
 * was recorded as a call to a routine named {@code IN}. In the live Postgres demo analysis that
 * produced a phantom {@code ERP$IN} node plus a phantom call edge from
 * {@code erp_usp_init_mrp_run}, which the dependency-graph view rendered as 117 nodes for an
 * analysis whose node index contained 116.
 *
 * <p>Every keyword asserted here is a PostgreSQL reserved word that cannot name an unqualified
 * user routine, so filtering it can never discard a genuine edge. The guard is restricted to
 * unqualified names, which {@link #schemaQualifiedCallIsStillRecorded()} pins down.
 */
class PostgresSqlKeywordNotACallTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private PostgresRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private static List<String> callNames(PostgresRoutineInfo r) {
        return r.getCalls().stream()
                .map(c -> c.getName() == null ? "" : c.getName().toUpperCase(Locale.ROOT))
                .toList();
    }

    private static String dump(PostgresRoutineInfo r) {
        StringBuilder sb = new StringBuilder();
        for (PostgresCallInfo c : r.getCalls()) {
            sb.append("\n  call: schema=").append(c.getSchema())
              .append(" name=").append(c.getName())
              .append(" line=").append(c.getLine());
        }
        return sb.toString();
    }

    /** The exact shape found in the live corpus (erp.erp_usp_init_mrp_run, line 12). */
    @Test
    void inPredicate_isNotRecordedAsACallToARoutineNamedIn() {
        PostgresRoutineInfo r = parse(
                "CREATE OR REPLACE PROCEDURE erp.erp_usp_init_mrp_run(INOUT p_mrp_run_id integer)\n"
              + " LANGUAGE plpgsql\n"
              + "AS $procedure$\n"
              + "BEGIN\n"
              + "  DELETE FROM erp.erp_demand WHERE source_type IN ('SALES_ORDER','FORECAST','DEPENDENT');\n"
              + "END\n"
              + "$procedure$");

        assertFalse(callNames(r).contains("IN"),
                "`WHERE x IN (...)` must not be recorded as a call to a routine named IN." + dump(r));
    }

    /** The other reserved words the permissive `id` rule lets through in call position. */
    @Test
    void sqlOperatorKeywords_areNeverRecordedAsCalls() {
        PostgresRoutineInfo r = parse(
                "CREATE OR REPLACE PROCEDURE erp.p()\n"
              + " LANGUAGE plpgsql\n"
              + "AS $procedure$\n"
              + "DECLARE v_n int; v_t text;\n"
              + "BEGIN\n"
              + "  DELETE FROM erp.t WHERE c IN ('a','b');\n"
              + "  v_n := CAST('7' AS int);\n"
              + "  SELECT count(*) INTO v_n FROM erp.t WHERE c = 'x' OR (d = 'y' AND e = 'z');\n"
              + "  IF EXISTS (SELECT 1 FROM erp.t WHERE c = 'q') THEN v_n := 1; END IF;\n"
              + "  INSERT INTO erp.t(c) VALUES ('v');\n"
              + "END\n"
              + "$procedure$");

        List<String> names = callNames(r);
        for (String kw : List.of("IN", "CAST", "OR", "EXISTS", "VALUES")) {
            assertFalse(names.contains(kw),
                    "SQL keyword `" + kw + "` must not be recorded as a call." + dump(r));
        }
    }

    /** Guard against over-filtering: real calls in the same routine must survive. */
    @Test
    void realCallsAlongsideKeywordNoiseAreStillRecorded() {
        PostgresRoutineInfo r = parse(
                "CREATE OR REPLACE PROCEDURE erp.p()\n"
              + " LANGUAGE plpgsql\n"
              + "AS $procedure$\n"
              + "BEGIN\n"
              + "  DELETE FROM erp.t WHERE c IN ('a','b');\n"
              + "  CALL erp.erp_usp_write_audit_trail('INIT_START','ERP_MRP_RUN','x');\n"
              + "END\n"
              + "$procedure$");

        List<String> names = callNames(r);
        assertFalse(names.contains("IN"), "IN must still be filtered." + dump(r));
        assertTrue(names.contains("ERP_USP_WRITE_AUDIT_TRAIL"),
                "The genuine CALL edge must still be recorded." + dump(r));
    }

    /** The guard is unqualified-only: a schema-qualified name is always a real call. */
    @Test
    void schemaQualifiedCallIsStillRecorded() {
        PostgresRoutineInfo r = parse(
                "CREATE OR REPLACE PROCEDURE erp.p()\n"
              + " LANGUAGE plpgsql\n"
              + "AS $procedure$\n"
              + "DECLARE v_n int;\n"
              + "BEGIN\n"
              + "  v_n := erp.erp_fn_net_requirement(1);\n"
              + "END\n"
              + "$procedure$");

        assertTrue(callNames(r).contains("ERP_FN_NET_REQUIREMENT"),
                "Schema-qualified calls must be unaffected by the keyword guard." + dump(r));
    }
}
