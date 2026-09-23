package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * READ-ONLY AUDIT (2026-09-18) — CORE GRAMMAR ACCURACY.
 *
 * Scope: lexical/syntactic correctness of PostgresLexer.g4 / PostgresParser.g4, exercised through
 * the real ANTLR-generated parser + PostgresAnalysisVisitor via PostgresAnalysisEngine (no mocks).
 * This file only records ACTUAL observed behavior with evidence; nothing under src/main is
 * modified. See the class-level dump() helper for what is printed for every probe.
 *
 * Concepts probed:
 *   1. Dollar-quoted strings with custom/nested tags
 *   2. `::` type-cast operator (bare, chained, applied to a parenthesized subquery)
 *   3. Array/composite syntax: ARRAY[...] vs '{...}' string-literal array, (row).field access
 *   4. Case-sensitive double-quoted identifiers vs Postgres lower-casing of unquoted identifiers
 *   5. Postgres-specific operators @> <@ -> ->> ~ ~* mixed into a WHERE clause
 */
class PostgresGrammarAccuracyAuditTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private void dump(String label, PostgresRoutineInfo r) {
        System.out.println("========== " + label + " ==========");
        for (PostgresTableOpInfo t : r.getTableOps()) {
            System.out.println("  tableOp: " + t.getDmlType() + " table=" + t.qualifiedTable()
                    + " alias=" + t.getAlias() + " line=" + t.getLine());
        }
        for (PostgresCallInfo c : r.getCalls()) {
            System.out.println("  call: " + c.qualifiedName() + " line=" + c.getLine());
        }
        for (PostgresVariableInfo v : r.getVariables()) {
            System.out.println("  var: " + v.getName() + " type=" + v.getDataType()
                    + " default=" + v.getDefaultValue());
        }
        System.out.println("  errors=" + r.getErrors());
    }

    // ── 1a. Custom-tag dollar-quoted body: $body$ ... $body$ (no nesting). ─────────────────────
    @Test
    @DisplayName("1a: custom-tag dollar-quoted function body ($body$...$body$) parses cleanly")
    void customTagDollarQuoteBody() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_custom_tag_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $body$\n" +
                "BEGIN\n" +
                "  INSERT INTO tab1 (id) VALUES (1);\n" +
                "END;\n" +
                "$body$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_custom_tag_probe", null);
        dump("1a custom-tag $body$", r);
        boolean sawTab1 = r.getTableOps().stream().anyMatch(t -> "tab1".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> tab1 captured: " + sawTab1 + " errors=" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(sawTab1, "custom-tag $body$...$body$ wrapper must not block table capture");
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty());
    }

    // ── 1b. Nested dollar-quoting with DIFFERENT tags: outer $body$, inner $sql$ wraps a dynamic
    // SQL string literal passed to EXECUTE. This is the real-world idiom for embedding SQL text
    // inside a function body without backslash/'' escaping. ─────────────────────────────────────
    @Test
    @DisplayName("1b: nested dollar-quote with different inner tag ($body$ outer / $sql$ inner)")
    void nestedDifferentTagDollarQuote() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_nested_dq_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $body$\n" +
                "BEGIN\n" +
                "  EXECUTE $sql$SELECT * FROM tab2 WHERE id = 1$sql$;\n" +
                "  INSERT INTO tab3 (id) VALUES (2);\n" +
                "END;\n" +
                "$body$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_nested_dq_probe", null);
        dump("1b nested $body$/$sql$", r);
        boolean sawTab3 = r.getTableOps().stream().anyMatch(t -> "tab3".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> tab3 (statement AFTER the nested $sql$...$sql$ EXECUTE literal) captured: " + sawTab3
                + " errors=" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(sawTab3,
                "a statement following a nested different-tag dollar-quoted literal must still be reachable/parsed");
    }

    // ── 2a. Bare `::` cast on a column in a WHERE clause; must not break FROM/table capture. ───
    @Test
    @DisplayName("2a: `col::INTEGER` cast in WHERE clause")
    void bareCastInWhere() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_cast_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  UPDATE tab4 SET flag = 1 WHERE code::INTEGER = 5;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_cast_probe", null);
        dump("2a bare cast", r);
        boolean sawTab4 = r.getTableOps().stream().anyMatch(t -> "tab4".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> tab4 captured: " + sawTab4 + " errors=" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(sawTab4, "`::` cast in WHERE must not drop the UPDATE target table");
    }

    // ── 2b. Chained cast `col::TEXT::INTEGER` plus a cast applied to a parenthesized subquery
    // result `(SELECT x FROM t)::INTEGER` in the same statement's SELECT list. ──────────────────
    @Test
    @DisplayName("2b: chained cast + cast on parenthesized subquery result")
    void chainedCastAndSubqueryCast() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_chained_cast_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_x INTEGER;\n" +
                "BEGIN\n" +
                "  SELECT a.code::TEXT::INTEGER, (SELECT cnt FROM tab6)::INTEGER\n" +
                "    INTO v_x, v_x\n" +
                "    FROM tab5 a;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_chained_cast_probe", null);
        dump("2b chained cast + subquery cast", r);
        boolean sawTab5 = r.getTableOps().stream().anyMatch(t -> "tab5".equalsIgnoreCase(t.getTableName()));
        boolean sawTab6 = r.getTableOps().stream().anyMatch(t -> "tab6".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> tab5 (outer FROM) captured: " + sawTab5
                + "  tab6 (subquery-in-select-list FROM) captured: " + sawTab6
                + "  errors=" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(sawTab5, "chained `::` cast must not drop the outer FROM table");
    }

    // ── 3a. ARRAY[...] literal vs '{...}' string-literal array form, both as DEFAULT/assignment
    // values — must not break the surrounding statement. ────────────────────────────────────────
    @Test
    @DisplayName("3a: '{1,2,3}' string-literal array form alongside ARRAY[...] form")
    void stringLiteralArrayForm() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_strlit_array_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_a INTEGER[] := '{1,2,3}';\n" +
                "  v_b INTEGER[] := ARRAY[1,2,3];\n" +
                "BEGIN\n" +
                "  INSERT INTO tab7 (arr) VALUES (v_a);\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_strlit_array_probe", null);
        dump("3a string-literal array", r);
        PostgresVariableInfo vA = r.getVariables().stream().filter(v -> "v_a".equalsIgnoreCase(v.getName())).findFirst().orElse(null);
        boolean sawTab7 = r.getTableOps().stream().anyMatch(t -> "tab7".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> v_a captured: " + (vA != null) + " default=" + (vA != null ? vA.getDefaultValue() : null)
                + "  tab7 captured: " + sawTab7 + " errors=" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(sawTab7, "'{1,2,3}' string-literal array init must not break the rest of the routine");
    }

    // ── 3b. Composite-type field access `(my_row).field_name`. ─────────────────────────────────
    @Test
    @DisplayName("3b: composite-type field access (my_row).field_name in a SELECT list / WHERE")
    void compositeFieldAccess() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_composite_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_row RECORD;\n" +
                "  v_val INTEGER;\n" +
                "BEGIN\n" +
                "  SELECT (v_row).field_name INTO v_val FROM tab8;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_composite_probe", null);
        dump("3b composite field access", r);
        boolean sawTab8 = r.getTableOps().stream().anyMatch(t -> "tab8".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> tab8 captured: " + sawTab8 + " errors=" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(sawTab8, "(row).field composite access must not drop the FROM table");
    }

    // ── 4a. Double-quoted mixed-case identifier must preserve exact case (case-sensitive). ─────
    @Test
    @DisplayName("4a: double-quoted \"MixedCaseTable\" preserves exact case")
    void doubleQuotedMixedCasePreserved() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_mixedcase_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  INSERT INTO \"MixedCaseTable\" (id) VALUES (1);\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_mixedcase_probe", null);
        dump("4a double-quoted mixed case", r);
        PostgresTableOpInfo op = r.getTableOps().stream().filter(t -> t.getTableName() != null).findFirst().orElse(null);
        System.out.println("  >>> captured table name (raw): " + (op != null ? op.getTableName() : null)
                + " errors=" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertNotNull(op);
        org.junit.jupiter.api.Assertions.assertEquals("MixedCaseTable", op.getTableName(),
                "double-quoted identifier case must be preserved exactly (Postgres is case-sensitive when quoted)");
    }

    // ── 4b. Unquoted mixed-case identifier: real Postgres folds UNQUOTED identifiers to
    // LOWERCASE (opposite of Oracle/DB2 upper-case folding). We only assert the raw captured
    // text here (folding policy, if any, is a downstream/business-logic concern, not the
    // grammar) — the point of this probe is to document whether the analyzer applies any
    // case-folding at all, and if so which direction. ───────────────────────────────────────────
    @Test
    @DisplayName("4b: unquoted MixedCaseTable — document raw captured case (no assumed folding)")
    void unquotedMixedCaseRawCapture() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_unquoted_mixedcase_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  INSERT INTO MixedCaseTable (id) VALUES (1);\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_unquoted_mixedcase_probe", null);
        dump("4b unquoted mixed case", r);
        PostgresTableOpInfo op = r.getTableOps().stream().filter(t -> t.getTableName() != null).findFirst().orElse(null);
        System.out.println("  >>> captured table name (raw, unquoted MixedCaseTable): " + (op != null ? op.getTableName() : null)
                + "  (real Postgres would fold this to lowercase 'mixedcasetable'; if the tool preserves "
                + "or upper-cases it instead, that's a dialect-fidelity note, not a crash)"
                + " errors=" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertNotNull(op);
    }

    // ── 5a. Containment operators @> / <@ in WHERE mixed with normal comparisons. ──────────────
    @Test
    @DisplayName("5a: containment operators @> / <@ in WHERE clause")
    void containmentOperatorsInWhere() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_containment_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  UPDATE tab9 SET flag = 1 WHERE tags @> ARRAY['x'] AND id > 5 AND ARRAY['y'] <@ tags;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_containment_probe", null);
        dump("5a containment @>", r);
        boolean sawTab9 = r.getTableOps().stream().anyMatch(t -> "tab9".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> tab9 captured: " + sawTab9 + " errors=" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(sawTab9, "`@>` containment operator must not desync UPDATE table capture");
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "`@>`/`<@` must be recognized as their own tokens with zero parse errors");
    }

    // ── 5b. JSON operators -> / ->> in WHERE / SELECT. ──────────────────────────────────────────
    @Test
    @DisplayName("5b: JSON operators -> / ->> mixed with comparisons")
    void jsonOperatorsInWhere() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_json_op_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  UPDATE tab10 SET flag = 1 WHERE payload->'k' ->> 'inner' = 'v' AND id > 5;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_json_op_probe", null);
        dump("5b json -> / ->>", r);
        boolean sawTab10 = r.getTableOps().stream().anyMatch(t -> "tab10".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> tab10 captured: " + sawTab10 + " errors=" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(sawTab10, "`->`/`->>` JSON operators must not desync UPDATE table capture");
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "`->`/`->>` must be recognized as their own JSON-operator tokens with zero parse errors");
    }

    // ── 5c. Regex match operators ~ / ~* in WHERE. ──────────────────────────────────────────────
    @Test
    @DisplayName("5c: regex operators ~ / ~* mixed with comparisons")
    void regexOperatorsInWhere() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_regex_op_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  UPDATE tab11 SET flag = 1 WHERE name ~* '^abc.*' AND code ~ '[0-9]+' AND id > 5;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_regex_op_probe", null);
        dump("5c regex ~ / ~*", r);
        boolean sawTab11 = r.getTableOps().stream().anyMatch(t -> "tab11".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> tab11 captured: " + sawTab11 + " errors=" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(sawTab11, "`~`/`~*` regex operators must not desync UPDATE table capture");
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "`~`/`~*` must be recognized as distinct tokens with zero parse errors");
    }
}
