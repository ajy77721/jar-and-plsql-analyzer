package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * READ-ONLY AUDIT (2026-09-17) — closes the two gaps not already covered by
 * {@link PostgresUniqueConceptsAuditTest}#16 (which already probes `v_ids := ARRAY[1,2,3,4,5];`,
 * single-index `v_ids[1]`, slice `v_ids[1:3]`, and `array_append(...)`):
 *
 *   A. Native array literal DECLARE-with-inline-initializer (`v_arr INTEGER[] := ARRAY[1,2,3];`
 *      in one DECLARE item, not a separate DECLARE+assignment) and multi-dimensional read access
 *      `v_arr[1][2]` — does declaration+init and multidim indexing parse/classify correctly,
 *      matching grammar rule `preBeginDeclareItem: id dataType ((DEFAULT|ASSIGN|EQ) expr)? SEMI`
 *      (PostgresParser.g4:83) and `exprAtom: id (DOT id)* (LBRACK expr (COMMA expr)* RBRACK)*`
 *      (PostgresParser.g4:618, which repeats the bracket group with `*`, so two consecutive
 *      `[...]` groups are structurally supported for reads).
 *   B. Multi-row batch INSERT `INSERT INTO tab VALUES (1,'a'), (2,'b'), (3,'c');` — Postgres
 *      allows several VALUES rows in one INSERT. `insertStatement` (PostgresParser.g4:316-318)
 *      has exactly one `tableRef`, so by grammar construction there should be exactly ONE
 *      table-op captured for `tab`, never one-per-row and never zero.
 *
 * Tested against the real ANTLR-generated parser via PostgresAnalysisEngine — no mocks. Prints
 * real captured output; does not modify anything under src/main.
 */
class PostgresArrayAndBatchAuditTest {

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

    // ── A. DECLARE v_arr INTEGER[] := ARRAY[1,2,3]; (single declare item, inline literal init)
    // plus multidimensional read access v_arr[1][2]. ────────────────────────────────────────
    @Test
    @DisplayName("A: native array literal DECLARE-with-init + multidim [1][2] read access")
    void arrayLiteralDeclareWithInitAndMultidimAccess() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_array_multidim_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_arr INTEGER[] := ARRAY[1,2,3];\n" +
                "  v_matrix INTEGER[][];\n" +
                "  v_val INTEGER;\n" +
                "BEGIN\n" +
                "  v_val := v_matrix[1][2];\n" +
                "  RAISE NOTICE 'val=%, arr0=%', v_val, v_arr[1];\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_array_multidim_probe", null);
        dump("A: array literal DECLARE-with-init + multidim access", r);

        PostgresVariableInfo vArr = r.getVariables().stream()
                .filter(v -> "v_arr".equalsIgnoreCase(v.getName())).findFirst().orElse(null);
        System.out.println("  >>> v_arr captured: " + (vArr != null)
                + " dataType=" + (vArr != null ? vArr.getDataType() : null)
                + " default=" + (vArr != null ? vArr.getDefaultValue() : null));
        org.junit.jupiter.api.Assertions.assertNotNull(vArr,
                "v_arr INTEGER[] := ARRAY[1,2,3]; (inline-initialized array declare item) must be captured as a variable");
        org.junit.jupiter.api.Assertions.assertTrue(
                vArr.getDataType() != null && vArr.getDataType().toUpperCase().contains("INTEGER"),
                "v_arr's declared array element type should be recoverable from dataType, got: "
                        + (vArr != null ? vArr.getDataType() : null));

        boolean bogusMatrixCall = r.getCalls().stream()
                .anyMatch(c -> "v_matrix".equalsIgnoreCase(c.getName()) || "v_arr".equalsIgnoreCase(c.getName()));
        System.out.println("  >>> v_matrix[1][2] / v_arr[1] misclassified as CallInfo: " + bogusMatrixCall
                + " (expect false)");
        System.out.println("  >>> errors on multidim array declare+access: " + r.getErrors()
                + " (expect empty for a clean parse)");

        org.junit.jupiter.api.Assertions.assertFalse(bogusMatrixCall,
                "multidim array index access v_matrix[1][2] must not be misclassified as a function/procedure call");
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "DECLARE-with-inline-array-literal-init + multidim [1][2] read access should parse cleanly, got: "
                        + r.getErrors());
    }

    // ── B. Multi-row batch INSERT: INSERT INTO tab VALUES (1,'a'), (2,'b'), (3,'c');
    // Exactly one INSERT table-op for `tab` expected — not 0, not 3. ───────────────────────
    @Test
    @DisplayName("B: multi-row batch INSERT VALUES (...),(...),(...) captures exactly one table-op")
    void multiRowBatchInsertSingleTableOp() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_batch_insert_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  INSERT INTO tab VALUES (1,'a'), (2,'b'), (3,'c');\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_batch_insert_probe", null);
        dump("B: multi-row batch INSERT", r);

        long tabInsertCount = r.getTableOps().stream()
                .filter(t -> "INSERT".equalsIgnoreCase(t.getDmlType().name())
                        && t.qualifiedTable() != null && t.qualifiedTable().toUpperCase().contains("TAB"))
                .count();
        System.out.println("  >>> INSERT table-ops recorded for `tab` (3-row VALUES list): " + tabInsertCount
                + " (expect exactly 1)");
        System.out.println("  >>> all table-ops: " + r.getTableOps().size()
                + " total, errors=" + r.getErrors());

        org.junit.jupiter.api.Assertions.assertEquals(1, tabInsertCount,
                "a single multi-row-VALUES INSERT statement into `tab` must be captured as exactly one "
                        + "table-op (found " + tabInsertCount + "); double/triple counting per VALUES row, "
                        + "or dropping the table entirely, is a bug");
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "multi-row batch INSERT VALUES should parse cleanly, got: " + r.getErrors());
    }

    // ── B2. Same as B but with an explicit column list, closer to real-world usage, and with a
    // qualified/schema-prefixed table name to also confirm qualifiedTable() isn't duplicated. ──
    @Test
    @DisplayName("B2: multi-row batch INSERT with column list + qualified table name")
    void multiRowBatchInsertWithColumnsAndSchema() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_batch_insert_probe2()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  INSERT INTO erp.tab (id, name) VALUES (1,'a'), (2,'b'), (3,'c'), (4,'d');\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_batch_insert_probe2", null);
        dump("B2: multi-row batch INSERT with columns + schema", r);

        long tabInsertCount = r.getTableOps().stream()
                .filter(t -> "INSERT".equalsIgnoreCase(t.getDmlType().name())
                        && t.qualifiedTable() != null && t.qualifiedTable().toUpperCase().contains("TAB"))
                .count();
        System.out.println("  >>> INSERT table-ops recorded for `erp.tab` (4-row VALUES list, explicit columns): "
                + tabInsertCount + " (expect exactly 1)");

        org.junit.jupiter.api.Assertions.assertEquals(1, tabInsertCount,
                "a single 4-row-VALUES INSERT with an explicit column list into a schema-qualified table "
                        + "must still be captured as exactly one table-op (found " + tabInsertCount + ")");
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "multi-row batch INSERT with column list + schema-qualified table should parse cleanly, got: "
                        + r.getErrors());
    }
}
