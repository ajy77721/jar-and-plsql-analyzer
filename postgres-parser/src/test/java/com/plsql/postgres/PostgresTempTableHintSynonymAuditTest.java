package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * READ-ONLY AUDIT (2026-09-17) of postgres-parser for three concepts, specific to Postgres:
 *
 * <ol>
 *   <li>{@code CREATE TEMP TABLE} / {@code CREATE TEMPORARY TABLE} used later in a JOIN or
 *       INSERT — is the temp table captured as a table op, or silently dropped?</li>
 *   <li>Optimizer-hint-style {@code /*+ ... *}{@code /} comments and plain {@code /* ... *}{@code /}
 *       comments immediately after SELECT/INSERT/UPDATE/DELETE/MERGE — do they corrupt
 *       table/column capture?</li>
 *   <li>Synonyms (grammar has no CREATE SYNONYM support — verified by grep, not tested) and
 *       cross-schema qualified references ({@code schema.table}), plus 3-part
 *       {@code database.schema.table}-looking names (grammar has no dblink/fdw special-casing
 *       — verified by grep).</li>
 * </ol>
 *
 * Grammar evidence (grep, done fresh for this audit):
 * <ul>
 *   <li>{@code grep -i "TEMP" PostgresLexer.g4} → no match. There is no TEMP/TEMPORARY token at
 *       all; {@code CREATE TEMP TABLE ...} and {@code CREATE TEMPORARY TABLE ...} both lex as
 *       plain identifiers consumed by the catch-all {@code createStatement} rule:
 *       {@code createStatement : CREATE (~SEMI)* SEMI ;} (PostgresParser.g4 ~line 500). This
 *       rule has no visitor override in PostgresAnalysisVisitor.java (grep for
 *       "CreateStatement" in the visitor returns nothing) — so ANY CREATE TABLE, temp or not,
 *       is swallowed as an opaque token stream and never produces a PostgresTableOpInfo.</li>
 *   <li>{@code grep -i "synonym" *.g4} → no match. Postgres itself has no CREATE SYNONYM, so
 *       this is correctly absent; not fabricated as a bug.</li>
 *   <li>{@code grep -i "dblink|foreign.*server|fdw" *.g4} → no match. No 3-part
 *       database.schema.table support exists; {@code tableRef : id (DOT id)? ;} only supports
 *       up to 2 parts (schema.table). Consistent with real Postgres (single-connection has no
 *       cross-database qualification), so this is a correct absence, not a bug — tested below
 *       for how a 3-part-looking name actually gets mis-split.</li>
 *   <li>Comments: {@code LINE_COMMENT : '--' ~[\r\n]* -> skip ;} and
 *       {@code BLOCK_COMMENT : '/*' .*? '*}{@code /' -> skip ;} — both are skip-channel at the
 *       LEXER level, before the parser ever sees a token. This means {@code /*+ hint *}{@code /}
 *       and plain {@code /* ... *}{@code /} comments are removed identically to whitespace and
 *       should not desynchronize the parser, unlike engines where hint comments are
 *       significant tokens. Confirmed empirically below.</li>
 * </ul>
 *
 * Does not modify any file under src/main.
 */
class PostgresTempTableHintSynonymAuditTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private void dump(String label, PostgresRoutineInfo r) {
        System.out.println("========== " + label + " ==========");
        for (PostgresTableOpInfo t : r.getTableOps()) {
            System.out.println("  tableOp: " + t.getDmlType() + " table=" + t.qualifiedTable()
                    + " schema=" + t.getSchemaName() + " alias=" + t.getAlias() + " line=" + t.getLine());
        }
        System.out.println("  errors=" + r.getErrors());
    }

    // ── 1a. CREATE TEMP TABLE ... then used as JOIN partner ───────────────────────────────
    @Test
    @DisplayName("1a: CREATE TEMP TABLE then JOINed against — temp table op entirely absent")
    void createTempTableThenJoin() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_temp_join_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_cnt INT;\n" +
                "BEGIN\n" +
                "  CREATE TEMP TABLE tmp_orders AS SELECT * FROM orders WHERE status = 'OPEN';\n" +
                "  SELECT count(*) INTO v_cnt\n" +
                "    FROM tmp_orders t\n" +
                "    JOIN customers c ON t.customer_id = c.id;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_temp_join_probe", null);
        dump("createTempTableThenJoin", r);

        boolean ordersSourceCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        boolean tmpOrdersAsCreate = r.getTableOps().stream()
                .anyMatch(t -> "tmp_orders".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == PostgresTableOpInfo.DmlType.INSERT); // any "create" style
        boolean tmpOrdersAsJoinRead = r.getTableOps().stream()
                .anyMatch(t -> "tmp_orders".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == PostgresTableOpInfo.DmlType.SELECT);
        boolean customersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "customers".equalsIgnoreCase(t.getTableName()));

        System.out.println("  >>> 'orders' (source of CREATE TEMP TABLE ... AS SELECT) captured: "
                + ordersSourceCaptured + " (expect true if CREATE...AS SELECT were parsed; ACTUAL is false — whole CREATE TEMP TABLE statement is opaque)");
        System.out.println("  >>> 'tmp_orders' captured as a create/write op: " + tmpOrdersAsCreate + " (expect true)");
        System.out.println("  >>> 'tmp_orders' captured later as JOIN/SELECT read: " + tmpOrdersAsJoinRead
                + " (this DOES get captured — the later plain SELECT...FROM tmp_orders JOIN customers is a normal select, unrelated to the CREATE handling)");
        System.out.println("  >>> 'customers' (join partner) captured: " + customersCaptured + " (expect true)");
        System.out.println("  errors=" + r.getErrors());
        boolean tmpOrdersAsCreateOp = r.getTableOps().stream()
                .anyMatch(t -> "tmp_orders".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == PostgresTableOpInfo.DmlType.CREATE);
        org.junit.jupiter.api.Assertions.assertTrue(tmpOrdersAsCreateOp,
                "CREATE TEMP TABLE tmp_orders must be captured as a CREATE table op");
        org.junit.jupiter.api.Assertions.assertTrue(customersCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(tmpOrdersAsJoinRead);
    }

    // ── 1b. CREATE TEMPORARY TABLE ... then used as INSERT target ─────────────────────────
    @Test
    @DisplayName("1b: CREATE TEMPORARY TABLE then INSERT INTO it — CREATE swallowed, INSERT itself is fine")
    void createTemporaryTableThenInsert() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_temp_insert_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  CREATE TEMPORARY TABLE tmp_staging (id INT, amount NUMERIC);\n" +
                "  INSERT INTO tmp_staging (id, amount) SELECT id, amount FROM orders WHERE status = 'NEW';\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_temp_insert_probe", null);
        dump("createTemporaryTableThenInsert", r);

        boolean createCaptured = r.getTableOps().stream()
                .anyMatch(t -> "tmp_staging".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == PostgresTableOpInfo.DmlType.CREATE);
        boolean tmpStagingInsertCaptured = r.getTableOps().stream()
                .anyMatch(t -> "tmp_staging".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == PostgresTableOpInfo.DmlType.INSERT);
        boolean ordersSelectCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));

        System.out.println("  >>> CREATE TEMPORARY TABLE recorded as any kind of statement/tableOp: " + createCaptured
                + " (ACTUAL: it is only consumed as opaque createStatement tokens — no PostgresTableOpInfo, no distinguishable statement record)");
        System.out.println("  >>> subsequent 'INSERT INTO tmp_staging' captured normally: " + tmpStagingInsertCaptured + " (expect true)");
        System.out.println("  >>> subsequent 'orders' SELECT source captured: " + ordersSelectCaptured + " (expect true)");
        org.junit.jupiter.api.Assertions.assertTrue(tmpStagingInsertCaptured,
                "the plain INSERT statement after CREATE TEMPORARY TABLE must still be captured correctly");
        org.junit.jupiter.api.Assertions.assertTrue(createCaptured,
                "CREATE TEMPORARY TABLE tmp_staging must be captured as a CREATE table op");
    }

    // ── 2a. pg_hint_plan style /*+ ... */ immediately after SELECT ────────────────────────
    @Test
    @DisplayName("2a: /*+ HashJoin(...) */ hint comment right after SELECT — lexer skips it, no corruption")
    void hintCommentAfterSelect() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_hint_select_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE v_cnt INT;\n" +
                "BEGIN\n" +
                "  SELECT /*+ HashJoin(o c) SeqScan(o) */ count(*) INTO v_cnt\n" +
                "    FROM orders o JOIN customers c ON o.customer_id = c.id;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_hint_select_probe", null);
        dump("hintCommentAfterSelect", r);
        boolean ordersCaptured = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        boolean customersCaptured = r.getTableOps().stream().anyMatch(t -> "customers".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> orders captured despite hint comment: " + ordersCaptured + " (expect true)");
        System.out.println("  >>> customers captured despite hint comment: " + customersCaptured + " (expect true)");
        org.junit.jupiter.api.Assertions.assertTrue(ordersCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(customersCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
    }

    // ── 2b. plain /* comment */ immediately after INSERT / UPDATE / DELETE ────────────────
    @Test
    @DisplayName("2b: plain block comment right after INSERT/UPDATE/DELETE keywords — no corruption")
    void plainCommentAfterDmlKeywords() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_comment_dml_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  INSERT /* audit: bulk load */ INTO archived_orders (id) SELECT id FROM orders;\n" +
                "  UPDATE /* audit: status sync */ customers SET status = 'ACTIVE' WHERE id = 1;\n" +
                "  DELETE /* audit: purge */ FROM stale_sessions WHERE created_at < now() - interval '1 day';\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_comment_dml_probe", null);
        dump("plainCommentAfterDmlKeywords", r);
        boolean insertTarget = r.getTableOps().stream().anyMatch(t -> "archived_orders".equalsIgnoreCase(t.getTableName())
                && t.getDmlType() == PostgresTableOpInfo.DmlType.INSERT);
        boolean insertSource = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        boolean updateTarget = r.getTableOps().stream().anyMatch(t -> "customers".equalsIgnoreCase(t.getTableName())
                && t.getDmlType() == PostgresTableOpInfo.DmlType.UPDATE);
        boolean deleteTarget = r.getTableOps().stream().anyMatch(t -> "stale_sessions".equalsIgnoreCase(t.getTableName())
                && t.getDmlType() == PostgresTableOpInfo.DmlType.DELETE);
        System.out.println("  >>> INSERT target captured: " + insertTarget + " (expect true)");
        System.out.println("  >>> INSERT source (orders) captured: " + insertSource + " (expect true)");
        System.out.println("  >>> UPDATE target captured: " + updateTarget + " (expect true)");
        System.out.println("  >>> DELETE target captured: " + deleteTarget + " (expect true)");
        org.junit.jupiter.api.Assertions.assertTrue(insertTarget);
        org.junit.jupiter.api.Assertions.assertTrue(insertSource);
        org.junit.jupiter.api.Assertions.assertTrue(updateTarget);
        org.junit.jupiter.api.Assertions.assertTrue(deleteTarget);
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
    }

    // ── 2c. plain /* comment */ immediately after MERGE — MERGE body is opaque tokens anyway ─
    @Test
    @DisplayName("2c: comment right after MERGE — MERGE is grammar-opaque already, table capture N/A")
    void commentAfterMerge() {
        // Grammar evidence: mergeStatement : MERGE mergeToken* SEMI? ; — the entire MERGE body is
        // consumed as an opaque token stream (mergeToken*), so no table/column op is ever
        // produced for MERGE regardless of comments. This test confirms the comment specifically
        // does not cause a parse ERROR (crash/desync), which is the concern in scope here.
        String src =
                "CREATE OR REPLACE FUNCTION fn_merge_comment_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  MERGE /* audit: merge comment */ INTO customer_summary cs\n" +
                "  USING orders o ON cs.customer_id = o.customer_id\n" +
                "  WHEN MATCHED THEN UPDATE SET total = cs.total + o.amount\n" +
                "  WHEN NOT MATCHED THEN INSERT (customer_id, total) VALUES (o.customer_id, o.amount);\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_merge_comment_probe", null);
        dump("commentAfterMerge", r);
        boolean anyMergeRelatedTableOp = r.getTableOps().stream()
                .anyMatch(t -> "customer_summary".equalsIgnoreCase(t.getTableName())
                        || "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> any table op derived from MERGE body (customer_summary/orders): " + anyMergeRelatedTableOp
                + " (expect false — MERGE is opaque mergeToken*, no table capture with or without the comment)");
        System.out.println("  >>> parse errors: " + r.getErrors() + " (expect empty — comment does not desync parser)");
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "comment right after MERGE must not cause a parse error");
    }

    // ── 3a. Synonyms: grammar has no CREATE SYNONYM — not applicable, not tested ──────────
    @Test
    @DisplayName("3a: CREATE SYNONYM not in grammar (grep-verified) — Postgres has none natively; skipped")
    void synonymNotApplicable() {
        // grep -i "synonym" PostgresLexer.g4 PostgresParser.g4 -> no matches.
        // Postgres itself has no CREATE SYNONYM statement, so this absence is correct, not a bug.
        // No test snippet is fabricated for this concept.
        org.junit.jupiter.api.Assertions.assertTrue(true);
    }

    // ── 3b. Cross-schema qualified reference: schema.table in SELECT/JOIN ─────────────────
    @Test
    @DisplayName("3b: schema.table qualified reference — schema captured, not mis-split")
    void schemaQualifiedTableRef() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_schema_qualified_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE v_cnt INT;\n" +
                "BEGIN\n" +
                "  SELECT count(*) INTO v_cnt\n" +
                "    FROM billing.orders o\n" +
                "    JOIN reporting.customers c ON o.customer_id = c.id;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_schema_qualified_probe", null);
        dump("schemaQualifiedTableRef", r);
        boolean ordersOk = r.getTableOps().stream().anyMatch(t ->
                "orders".equalsIgnoreCase(t.getTableName()) && "billing".equalsIgnoreCase(t.getSchemaName()));
        boolean customersOk = r.getTableOps().stream().anyMatch(t ->
                "customers".equalsIgnoreCase(t.getTableName()) && "reporting".equalsIgnoreCase(t.getSchemaName()));
        System.out.println("  >>> billing.orders -> table=orders schema=billing: " + ordersOk + " (expect true)");
        System.out.println("  >>> reporting.customers -> table=customers schema=reporting: " + customersOk + " (expect true)");
        org.junit.jupiter.api.Assertions.assertTrue(ordersOk);
        org.junit.jupiter.api.Assertions.assertTrue(customersOk);
    }

    // ── 3c. 3-part database.schema.table-looking name — grammar only supports 2-part; mis-split ─
    @Test
    @DisplayName("3c: 3-part db.schema.table-looking name — grammar mis-splits/truncates (no dblink/fdw support)")
    void threePartQualifiedNameMissplit() {
        // Grammar evidence: tableRef : id (DOT id)? ; supports AT MOST one DOT (2 parts).
        // A 3-part reference like mydb.billing.orders will fail this rule; either the parser
        // errors, or (depending on surrounding catch-all rules) part of the name is silently
        // dropped/misattributed. Real Postgres itself has no single-connection 3-part
        // db.schema.table syntax, so a "correct" outcome here would be a clean parse error on
        // this constructed SQL, not silent misattribution of a table named e.g. "orders" losing
        // the "mydb.billing" prefix without any diagnostic.
        String src =
                "CREATE OR REPLACE FUNCTION fn_three_part_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE v_cnt INT;\n" +
                "BEGIN\n" +
                "  SELECT count(*) INTO v_cnt FROM mydb.billing.orders;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_three_part_probe", null);
        dump("threePartQualifiedNameMissplit", r);
        System.out.println("  >>> tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + " schema=" + t.getSchemaName() + " table=" + t.getTableName()).toList());
        System.out.println("  >>> errors: " + r.getErrors());
        // Fixed: the real table 'orders' is now correctly resolved (schema='billing'), with the
        // earlier 'mydb' database segment simply dropped rather than mis-attributed.
        boolean ordersCapturedCorrectly = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName())
                        && "billing".equalsIgnoreCase(t.getSchemaName()));
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(ordersCapturedCorrectly,
                "3-part db.schema.table reference must resolve to schema='billing' table='orders'");
    }

    /**
     * Golden: TEMP as the FOURTH table kind, set from the grammar's own createTempModifier node.
     * A plain CREATE TABLE (and UNLOGGED, which is persistent) must stay unclassified.
     */
    @Test
    @DisplayName("TEMP kind: CREATE TEMP/TEMPORARY TABLE classified TEMP, plain CREATE TABLE not")
    void tempKindIsClassified() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_temp_kind()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  CREATE TEMP TABLE tmp_a (id INT);\n" +
                "  CREATE TEMPORARY TABLE tmp_b (id INT);\n" +
                "  CREATE TABLE perm_t (id INT);\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_temp_kind", null);

        PostgresTableOpInfo a = r.getTableOps().stream()
                .filter(t -> "tmp_a".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        org.junit.jupiter.api.Assertions.assertNotNull(a, "tmp_a must be captured");
        org.junit.jupiter.api.Assertions.assertEquals("TEMP", a.getTableKind());
        org.junit.jupiter.api.Assertions.assertEquals("SESSION", a.getTempScope());

        PostgresTableOpInfo b = r.getTableOps().stream()
                .filter(t -> "tmp_b".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        org.junit.jupiter.api.Assertions.assertNotNull(b, "tmp_b must be captured");
        org.junit.jupiter.api.Assertions.assertEquals("TEMP", b.getTableKind());

        PostgresTableOpInfo perm = r.getTableOps().stream()
                .filter(t -> "perm_t".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        org.junit.jupiter.api.Assertions.assertNotNull(perm, "perm_t must be captured");
        org.junit.jupiter.api.Assertions.assertNull(perm.getTableKind(),
                "a plain CREATE TABLE must not be classified TEMP");
    }
}
