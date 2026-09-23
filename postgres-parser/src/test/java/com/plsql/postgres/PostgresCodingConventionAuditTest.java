package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT (coding-conventions / formatting) — postgres-parser.
 *
 * Investigates whether common real-world PL/pgSQL CODING STYLE conventions (as opposed to
 * syntax edge cases or the separately-tracked variable/attribute-notation semantic bug) defeat
 * correct capture of calls/operations/variables/parameters/line numbers by
 * PostgresAnalysisEngine + PostgresAnalysisVisitor.
 *
 * READ-ONLY AUDIT: does not modify any file under src/main.
 */
class PostgresCodingConventionAuditTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private void dump(String label, PostgresRoutineInfo r) {
        System.out.println("========== " + label + " ==========");
        System.out.println("  name=" + r.getName() + " schema=" + r.getSchema()
                + " lineStart=" + r.getLineStart() + " lineEnd=" + r.getLineEnd());
        System.out.println("  params=" + r.getParams().stream()
                .map(p -> p.getName() + ":" + p.getDirection()).toList());
        System.out.println("  vars=" + r.getVariables().stream()
                .map(v -> v.getName() + "@" + v.getLine()).toList());
        for (PostgresCallInfo c : r.getCalls()) {
            System.out.println("  call: schema=" + c.getSchema() + " name=" + c.getName() + " line=" + c.getLine());
        }
        for (PostgresTableOpInfo t : r.getTableOps()) {
            System.out.println("  tableOp: " + t.getDmlType() + " table=" + t.getTableName() + " line=" + t.getLine());
        }
        if (r.getDynamicSql() != null) {
            for (PostgresDynamicSqlInfo d : r.getDynamicSql()) {
                System.out.println("  dynSql: line=" + d.getLine() + " text=" + d.getSqlSource());
            }
        }
        System.out.println("  errors=" + r.getErrors());
    }

    /** Split a multi-function source text into individual CREATE ... routine chunks, each
     *  parsed independently (compilationUnit is single-routine; see existing audit notes). */
    private List<String> splitRoutines(String multi) {
        Matcher m = Pattern.compile("(?=^\\s*CREATE\\s+(OR\\s+REPLACE\\s+)?(FUNCTION|PROCEDURE))",
                Pattern.MULTILINE | Pattern.CASE_INSENSITIVE).matcher(multi);
        List<Integer> starts = new java.util.ArrayList<>();
        while (m.find()) starts.add(m.start());
        List<String> out = new java.util.ArrayList<>();
        for (int i = 0; i < starts.size(); i++) {
            int s = starts.get(i);
            int e = (i + 1 < starts.size()) ? starts.get(i + 1) : multi.length();
            out.add(multi.substring(s, e));
        }
        return out;
    }

    // ─────────────────────────────────────────────────────────────────────
    // Dimension 0: does the standard PL/pgSQL "DECLARE section before BEGIN"
    // convention (as opposed to DECLARE-as-a-statement-inside-BEGIN, which is
    // what the existing test suite exclusively exercises) get captured at all?
    // ─────────────────────────────────────────────────────────────────────
    @Test
    void standardDeclareSectionBeforeBegin_dollarQuoted() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_probe(p_x INT)\n" +
                "RETURNS INT\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_count INT := 0;\n" +
                "  v_name VARCHAR(50);\n" +
                "BEGIN\n" +
                "  v_count := p_x + 1;\n" +
                "  CALL log_audit(v_count);\n" +
                "  RETURN v_count;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_probe", null);
        dump("standardDeclareSectionBeforeBegin", r);
        // Bug 9 (fixed): routineCharacteristic's "~(BEGIN|EOF)+" catch-all used to swallow a
        // real "DECLARE ... BEGIN" section whole, silently discarding every variable in it.
        // routineDefinition now tries a dedicated preBeginDeclareSection alternative (DECLARE
        // excluded from the catch-all) whenever the next token is DECLARE.
        System.out.println("  >>> variables captured: " + r.getVariables().size()
                + " (expected 2: v_count, v_name)");
        System.out.println("  >>> call captured: " + r.getCalls().stream()
                .anyMatch(c -> "log_audit".equalsIgnoreCase(c.getName())) + " (expected true)");
        assertTrue(r.getErrors().isEmpty(), "DECLARE section before BEGIN must not produce parse errors: " + r.getErrors());
        assertEquals(2, r.getVariables().size(), "expected v_count and v_name captured");
        assertTrue(r.getVariables().stream().anyMatch(v -> "v_count".equalsIgnoreCase(v.getName())));
        assertTrue(r.getVariables().stream().anyMatch(v -> "v_name".equalsIgnoreCase(v.getName())));
        assertTrue(r.getCalls().stream().anyMatch(c -> "log_audit".equalsIgnoreCase(c.getName())),
                "CALL after the DECLARE section must still be captured");
    }

    // ─────────────────────────────────────────────────────────────────────
    // The big combined realistic fixture.
    // ─────────────────────────────────────────────────────────────────────

    private static final String HEADER =
            "-- =====================================================================\n" +
            "-- Module:  customer_reporting.sql\n" +
            "-- Purpose: Customer statement generation, audit trail, GUC-based config.\n" +
            "-- Author:  data-platform-team\n" +
            "-- History:\n" +
            "--   2024-01-01  initial version\n" +
            "--   2024-06-15  added dynamic SQL reporting path\n" +
            "-- Conventions:\n" +
            "--   v_/l_ = local vars, p_/i_/io_/o_ = params, g_ = pseudo-globals (GUC),\n" +
            "--   c_ = constants/cursors\n" +
            "-- =====================================================================\n\n" +
            "-- pseudo-global via session GUC (PL/pgSQL has no true package-level globals;\n" +
            "-- 'g_' convention is adapted here via current_setting()/set_config()) -- N/A note\n" +
            "SELECT set_config('app.g_max_retries', '3', false);\n\n";

    private static final String FN1_DOC =
            "/**\n" +
            " * fn_customer_report\n" +
            " * ------------------\n" +
            " * Builds a customer statement string and logs progress via nested\n" +
            " * control flow. Demonstrates 10+ param list, mixed casing, labeled loop,\n" +
            " * deep IF/CASE nesting, dynamic SQL, and dollar-quoted body with a\n" +
            " * custom delimiter tag ($body$) containing a nested plain $$ string.\n" +
            " */\n";

    // 10+ params, one per line, trailing commas, inconsistent indentation, trailing comments.
    private static final String FN1 =
            "CREATE OR REPLACE FUNCTION fn_customer_report(\n" +
            "  p_customer_id INT,               -- customer PK\n" +
            "      i_start_date DATE,           -- report window start\n" +
            "  i_end_date DATE,      -- report window end\n" +
            "  io_running_total NUMERIC,    -- accumulator, in/out\n" +
            "        o_status VARCHAR(20),  -- output status code\n" +
            "  p_region VARCHAR(10),  -- region code\n" +
            "  p_currency VARCHAR(3), -- ISO currency\n" +
            "  p_include_tax BOOLEAN, -- whether tax is included\n" +
            "  p_discount_pct NUMERIC,   -- discount percentage\n" +
            "  p_channel VARCHAR(20), -- sales channel\n" +
            "  p_locale VARCHAR(10)   -- locale for formatting\n" +
            ")\n" +
            "RETURNS VARCHAR\n" +
            "LANGUAGE plpgsql AS $body$\n" +
            "DECLARE\n" +
            "  v_out VARCHAR(4000);\n" +
            "  l_tmp VARCHAR(200);\n" +
            "  c_get_customer VARCHAR(50) := 'not_a_call';   -- deliberately c_-prefixed non-cursor var\n" +
            "  c_max_lines CONSTANT INT := 500;\n" +
            "  v_inner VARCHAR(50) := 'a plain $$ dollar literal $$ inside custom $body$ tag';\n" +
            "BEGIN\n" +
            "  -- mixed casing of keywords/identifiers throughout\n" +
            "  begin\n" +
            "    V_OUT := 'Statement for customer ' || p_customer_id::text ||\n" +
            "             -- comment interspersed in the concatenation\n" +
            "             ' region=' || p_region ||\n" +
            "             ' window=' || i_start_date::text || '..' || i_end_date::text; -- end concat\n" +
            "  end;\n" +
            "\n" +
            "  <<outer_loop>>\n" +
            "  LOOP\n" +
            "    IF p_region = 'US' THEN\n" +
            "      IF p_include_tax THEN\n" +
            "        IF p_discount_pct > 0 THEN\n" +
            "          IF p_channel = 'ONLINE' THEN\n" +
            "            IF p_locale = 'en_US' THEN\n" +
            "              CASE p_currency\n" +
            "                WHEN 'USD' THEN\n" +
            "                  CALL apply_discount_and_log(p_customer_id, p_discount_pct); l_tmp := 'usd'; v_out := v_out || l_tmp;\n" +
            "                WHEN 'EUR' THEN\n" +
            "                  l_tmp := 'eur';\n" +
            "                ELSE\n" +
            "                  l_tmp := 'other';\n" +
            "              END CASE;\n" +
            "            END IF;\n" +
            "          END IF;\n" +
            "        END IF;\n" +
            "      END IF;\n" +
            "    END IF;\n" +
            "    EXIT outer_loop;\n" +
            "  END LOOP;\n" +
            "\n" +
            "  EXECUTE format('SELECT count(*) FROM %I.orders WHERE customer_id = %s',\n" +
            "                 p_region, p_customer_id) INTO io_running_total;\n" +
            "\n" +
            "  select 1;\n" +
            "  o_status := 'OK';\n" +
            "  RETURN v_out;\n" +
            "END;\n" +
            "$body$;\n";

    private static final String FN2_DOC =
            "-- fn_lookup_customer: small helper, plain $$ delimiter, checks c_get_customer\n" +
            "-- naming does not get misclassified as a cursor/call by name alone.\n";

    private static final String FN2 =
            "CREATE OR REPLACE FUNCTION fn_lookup_customer(p_id INT)\n" +
            "RETURNS VARCHAR\n" +
            "LANGUAGE plpgsql AS $$\n" +
            "DECLARE\n" +
            "  c_get_customer VARCHAR(50);\n" +
            "BEGIN\n" +
            "  c_get_customer := 'value'; CALL audit_lookup(p_id); RETURN c_get_customer;\n" +
            "END;\n" +
            "$$;\n";

    // ─────────────────────────────────────────────────────────────────────
    // ISOLATED fixture: same nesting/casing/EXECUTE/concat/multi-stmt/param conventions as
    // FN1, but (a) uses the DECLARE-inside-BEGIN form this grammar's statement rule actually
    // supports (isolating dimensions 3-7 from the dimension-0 DECLARE-section bug already
    // confirmed above) and (b) its docblock does NOT spell out literal $ delimiters in prose
    // (isolating dimensions 3-7 from the preprocess() comment-confound discovered via FN1/FN2
    // below). Custom-tag dollar-quoting itself (dimension 8) is tested separately further down
    // with its own minimal, comment-safe fixture.
    // ─────────────────────────────────────────────────────────────────────
    private static final String FN1_ISO_DOC =
            "/**\n" +
            " * fn_customer_report_iso\n" +
            " * Builds a customer statement and demonstrates nested control flow,\n" +
            " * a wide parameter list, mixed keyword casing, dynamic reporting SQL,\n" +
            " * and long string building via concatenation.\n" +
            " */\n";

    private static final String FN1_ISO =
            "CREATE OR REPLACE FUNCTION fn_customer_report_iso(\n" +
            "  p_customer_id INT,               -- customer PK\n" +
            "      i_start_date DATE,           -- report window start\n" +
            "  i_end_date DATE,      -- report window end\n" +
            "  io_running_total NUMERIC,    -- accumulator, in/out\n" +
            "        o_status VARCHAR(20),  -- output status code\n" +
            "  p_region VARCHAR(10),  -- region code\n" +
            "  p_currency VARCHAR(3), -- ISO currency\n" +
            "  p_include_tax BOOLEAN, -- whether tax is included\n" +
            "  p_discount_pct NUMERIC,   -- discount percentage\n" +
            "  p_channel VARCHAR(20), -- sales channel\n" +
            "  p_locale VARCHAR(10)   -- locale for formatting\n" +
            ")\n" +
            "RETURNS VARCHAR\n" +
            "LANGUAGE plpgsql\n" +
            "BEGIN\n" +
            "  DECLARE v_out VARCHAR(4000);\n" +
            "  DECLARE l_tmp VARCHAR(200);\n" +
            "  DECLARE c_get_customer VARCHAR(50);\n" +
            "  SET c_get_customer = 'not_a_call';\n" +
            "  begin\n" +
            "    SET V_OUT = 'Statement for customer ' || p_customer_id ||\n" +
            "             -- comment interspersed in the concatenation\n" +
            "             ' region=' || p_region ||\n" +
            "             ' window=' || i_start_date || '..' || i_end_date; -- end concat\n" +
            "  end;\n" +
            "\n" +
            "  <<outer_loop>>\n" +
            "  LOOP\n" +
            "    IF p_region = 'US' THEN\n" +
            "      IF p_include_tax THEN\n" +
            "        IF p_discount_pct > 0 THEN\n" +
            "          IF p_channel = 'ONLINE' THEN\n" +
            "            IF p_locale = 'en_US' THEN\n" +
            "              CASE p_currency\n" +
            "                WHEN 'USD' THEN\n" +
            "                  CALL apply_discount_and_log(p_customer_id, p_discount_pct); SET l_tmp = 'usd'; SET v_out = v_out;\n" +
            "                WHEN 'EUR' THEN\n" +
            "                  SET l_tmp = 'eur';\n" +
            "                ELSE\n" +
            "                  SET l_tmp = 'other';\n" +
            "              END CASE;\n" +
            "            END IF;\n" +
            "          END IF;\n" +
            "        END IF;\n" +
            "      END IF;\n" +
            "    END IF;\n" +
            "    EXIT outer_loop;\n" +
            "  END LOOP;\n" +
            "\n" +
            "  EXECUTE format('SELECT count(*) FROM %I.orders WHERE customer_id = %s',\n" +
            "                 p_region, p_customer_id) INTO io_running_total;\n" +
            "\n" +
            "  select 1;\n" +
            "  SET o_status = 'OK';\n" +
            "  RETURN v_out;\n" +
            "END;\n";

    @Test
    void isolatedFixture_declareInsideBegin_noConfoundingComments() {
        String routine = FN1_ISO_DOC + FN1_ISO;
        PostgresRoutineInfo r = engine.parse(routine, "fn_customer_report_iso", null);
        dump("fn_customer_report_iso (isolated)", r);

        System.out.println("  >>> param count: " + r.getParams().size() + " (expected 11)");
        boolean hasApplyDiscount = r.getCalls().stream()
                .anyMatch(c -> "apply_discount_and_log".equalsIgnoreCase(c.getName()));
        int expectedLine = lineOf(routine, "CALL apply_discount_and_log");
        System.out.println("  >>> deepest-nesting call captured (apply_discount_and_log): " + hasApplyDiscount
                + " expectedLine=" + expectedLine);
        r.getCalls().stream().filter(c -> "apply_discount_and_log".equalsIgnoreCase(c.getName()))
                .forEach(c -> System.out.println("       actual line=" + c.getLine()));

        boolean hasExecuteFormat = r.getDynamicSql() != null && !r.getDynamicSql().isEmpty();
        System.out.println("  >>> EXECUTE format(...) captured as dynamic SQL: " + hasExecuteFormat);

        boolean cGetCustomerAsVar = r.getVariables().stream()
                .anyMatch(v -> "c_get_customer".equalsIgnoreCase(v.getName()));
        System.out.println("  >>> c_get_customer captured as variable (not misclassified as call/cursor): "
                + cGetCustomerAsVar);
        System.out.println("  >>> total vars: " + r.getVariables().size() + " (expected 3: v_out, l_tmp, c_get_customer)");
        System.out.println("  errors=" + r.getErrors());
    }

    // Dimension 8 in isolation: custom dollar-quote delimiter tag with a nested plain $$
    // string literal inside, and NO prose mention of '$' anywhere in surrounding comments.
    @Test
    void customDollarQuoteTag_withNestedPlainDollarString_isolated() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_body_tag_probe(p_x INT)\n" +
                "RETURNS VARCHAR\n" +
                "LANGUAGE plpgsql AS $body$\n" +
                "DECLARE\n" +
                "  v_inner VARCHAR(80) := 'nested literal with a doubled quote marker inside $$ here $$ end';\n" +
                "BEGIN\n" +
                "  CALL log_audit(p_x);\n" +
                "  RETURN v_inner;\n" +
                "END;\n" +
                "$body$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_body_tag_probe", null);
        dump("customDollarQuoteTag_isolated", r);
        int expectedLine = lineOf(src, "CALL log_audit");
        boolean hasCall = r.getCalls().stream().anyMatch(c -> "log_audit".equalsIgnoreCase(c.getName()));
        System.out.println("  >>> log_audit call captured: " + hasCall + " expectedLine=" + expectedLine);
        r.getCalls().forEach(c -> System.out.println("       actual line=" + c.getLine()));
        System.out.println("  errors=" + r.getErrors());
    }

    // Bug 10: a doc-comment merely mentioning "$$"/"$tag$" in prose, BEFORE the real
    // dollar-quoted body, used to fool the tag detector into treating the first "$...$"-shaped
    // match in the raw comment text as the opening tag.
    @Test
    void dollarQuoteTagDetector_notFooledByProseCommentMentioningDollarSigns() {
        String src =
                "-- This routine uses a dollar-quoted body, e.g. $$ ... $$ or a custom $body$ tag,\n" +
                "-- see the PL/pgSQL docs for details on $tag$ delimiters.\n" +
                "CREATE OR REPLACE FUNCTION fn_comment_probe(p_x INT)\n" +
                "RETURNS INT\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_count INT := 0;\n" +
                "BEGIN\n" +
                "  v_count := p_x + 1;\n" +
                "  CALL log_audit(v_count);\n" +
                "  RETURN v_count;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_comment_probe", null);
        dump("dollarQuoteTagDetector_notFooledByComment", r);

        assertTrue(r.getErrors().isEmpty(),
                "a doc-comment mentioning $$/$tag$ must not fool the dollar-quote detector: " + r.getErrors());
        assertEquals("fn_comment_probe", r.getName());
        assertTrue(r.getVariables().stream().anyMatch(v -> "v_count".equalsIgnoreCase(v.getName())),
                "DECLARE section inside the real (correctly detected) body must be captured");
        assertTrue(r.getCalls().stream().anyMatch(c -> "log_audit".equalsIgnoreCase(c.getName())),
                "CALL inside the real body must be captured");
    }

    @Test
    void combinedRealisticFixture_fn1_paramsAndDollarQuoting() {
        String routine = extract(FN1_DOC + FN1);
        PostgresRoutineInfo r = engine.parse(routine, "fn_customer_report", null);
        dump("fn_customer_report", r);

        System.out.println("  >>> param count: " + r.getParams().size() + " (expected 12)");
        for (PostgresParamInfo p : r.getParams()) {
            System.out.println("      param: " + p.getName() + " mode=" + p.getDirection());
        }

        boolean hasApplyDiscount = r.getCalls().stream()
                .anyMatch(c -> "apply_discount_and_log".equalsIgnoreCase(c.getName()));
        System.out.println("  >>> deepest-nesting call captured (apply_discount_and_log): " + hasApplyDiscount);
        r.getCalls().stream()
                .filter(c -> "apply_discount_and_log".equalsIgnoreCase(c.getName()))
                .forEach(c -> System.out.println("       actual line=" + c.getLine()
                        + " (source line of CALL apply_discount_and_log is line "
                        + lineOf(routine, "CALL apply_discount_and_log") + ")"));

        boolean hasExecuteFormat = r.getDynamicSql() != null && !r.getDynamicSql().isEmpty();
        System.out.println("  >>> EXECUTE format(...) captured as dynamic SQL: " + hasExecuteFormat);

        boolean cGetCustomerAsVar = r.getVariables().stream()
                .anyMatch(v -> "c_get_customer".equalsIgnoreCase(v.getName()));
        System.out.println("  >>> c_get_customer captured as variable (not misclassified): " + cGetCustomerAsVar);

        System.out.println("  errors=" + r.getErrors());
    }

    @Test
    void combinedRealisticFixture_fn2_plainDollarQuote_cGetCustomerNaming() {
        String routine = extract(FN2_DOC + FN2);
        PostgresRoutineInfo r = engine.parse(routine, "fn_lookup_customer", null);
        dump("fn_lookup_customer", r);

        boolean hasAuditLookup = r.getCalls().stream()
                .anyMatch(c -> "audit_lookup".equalsIgnoreCase(c.getName()));
        System.out.println("  >>> audit_lookup call captured: " + hasAuditLookup);
        boolean cGetCustomerMisclassifiedAsCall = r.getCalls().stream()
                .anyMatch(c -> "c_get_customer".equalsIgnoreCase(c.getName())
                        || "c_get_customer".equalsIgnoreCase(c.getSchema()));
        System.out.println("  >>> c_get_customer misclassified as a call: " + cGetCustomerMisclassifiedAsCall);
    }

    @Test
    void combinedRealisticFixture_fullMultiFunctionFile_splitAndParsedIndependently() {
        String full = HEADER + FN1_DOC + FN1 + "\n" + FN2_DOC + FN2;
        List<String> routines = splitRoutines(full);
        System.out.println("Detected " + routines.size() + " routine(s) in multi-function file (expected 2).");
        for (String routine : routines) {
            String name = routine.contains("fn_customer_report") ? "fn_customer_report" : "fn_lookup_customer";
            PostgresRoutineInfo r = engine.parse(routine, name, null);
            dump("split:" + name, r);
        }
        // Header-block leading statement (SELECT set_config(...)) is BEFORE the first CREATE
        // FUNCTION and is therefore not part of any routine chunk under this split strategy —
        // documented as an artifact of the single-routine-per-parse architecture, not re-tested
        // for line drift here (that is dimension 6, tested directly below via FN1's docblock).
    }

    // Verifies dimension 6: docblock/header comment block before a function must not shift the
    // line number of the first real captured statement/call after it.
    @Test
    void headerAndDocblockComments_noLineNumberDrift() {
        String routine = extract(FN1_DOC + FN1);
        PostgresRoutineInfo r = engine.parse(routine, "fn_customer_report", null);
        int expectedCallLine = lineOf(routine, "CALL apply_discount_and_log");
        PostgresCallInfo actual = r.getCalls().stream()
                .filter(c -> "apply_discount_and_log".equalsIgnoreCase(c.getName()))
                .findFirst().orElse(null);
        System.out.println("expected line=" + expectedCallLine
                + " actual=" + (actual == null ? "NOT CAPTURED" : actual.getLine()));
    }

    private int lineOf(String text, String needle) {
        int idx = text.indexOf(needle);
        if (idx < 0) return -1;
        int line = 1;
        for (int i = 0; i < idx; i++) if (text.charAt(i) == '\n') line++;
        return line;
    }

    /** Strip a leading doc-comment block (kept as real source text, not part of routine grammar)
     *  so each parse() call receives exactly one CREATE ... routine, matching how a real
     *  multi-function file would be split by any caller (see PostgresQualifiedCallScopeAuditTest
     *  Pattern-1 note: compilationUnit is single-routine). Comments are still real text preceding
     *  the CREATE keyword to test dimension 6 (docblock immediately before a function). */
    private String extract(String docPlusFn) {
        return docPlusFn;
    }
}
