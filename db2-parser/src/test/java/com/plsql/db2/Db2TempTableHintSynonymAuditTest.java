package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * READ-ONLY AUDIT (2026-09-17) — DB2 temp tables, "hint"/comment-after-keyword corruption,
 * and cross-schema/cross-catalog qualified references + ALIAS (DB2's synonym equivalent).
 *
 * <p>No production code touched. Grammar/visitor evidence gathered via grep of
 * {@code Db2Parser.g4} / {@code Db2Lexer.g4} / {@code Db2AnalysisVisitor.java} is
 * summarized in each section's comment; the tests below only pin down runtime behavior.
 */
class Db2TempTableHintSynonymAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private static void dump(String label, Db2RoutineInfo r) {
        System.out.println("=== " + label + " errors === " + r.getErrors());
        System.out.println("--- tableOps ---");
        for (Db2TableOpInfo t : r.getTableOps()) {
            System.out.println("  " + t.getDmlType() + " schema=" + t.getSchemaName()
                    + " table=" + t.getTableName() + " qualified=" + t.qualifiedTable()
                    + " alias=" + t.getAlias() + " line=" + t.getLine());
            for (Db2JoinInfo j : t.getJoins()) {
                System.out.println("      JOIN " + j.getJoinType() + " " + j.getJoinedTable()
                        + " alias=" + j.getJoinedTableAlias());
            }
        }
        System.out.println("--- statements ---");
        for (Db2StatementInfo s : r.getStatements()) {
            System.out.println("  " + s.getType() + " line=" + s.getLine());
        }
    }

    // ═════════════════════════════════════════════════════════════════════
    // 1. DECLARE GLOBAL TEMPORARY TABLE, used later in a JOIN and an INSERT.
    //
    // Grammar evidence (Db2Parser.g4):
    //   declareGlobalTempTable
    //       : DECLARE id id id id (DOT id)? LPAREN (~SEMI)* SEMI
    //       ;
    // — matched permissively, no dedicated Db2AnalysisVisitor.visitDeclareGlobalTempTable
    // override exists (grep confirmed zero hits for "TempTable"/"GlobalTempTable" in the
    // visitor and zero model class for it under src/main/java/com/plsql/db2/model/). So the
    // temp-table *declaration* itself is never recorded as any entity in Db2RoutineInfo.
    // ═════════════════════════════════════════════════════════════════════

    private static final String TEMP_TABLE_JOIN_SRC =
        "CREATE PROCEDURE p_temp_join(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE GLOBAL TEMPORARY TABLE SESSION.tmp_orders (\n"
      + "    order_id INTEGER, region CHAR(2)\n"
      + "  ) ON COMMIT PRESERVE ROWS NOT LOGGED;\n"
      + "  DECLARE cur1 CURSOR FOR\n"
      + "    SELECT t.order_id, o.customer_name\n"
      + "    FROM SESSION.tmp_orders t\n"
      + "    JOIN orders o ON o.order_id = t.order_id\n"
      + "    WHERE t.region = p_region;\n"
      + "END";

    @Test
    @DisplayName("1a: DECLARE GLOBAL TEMPORARY TABLE + later JOIN against it — declaration recorded? join captured as plain table?")
    void tempTableDeclarationThenJoin() {
        Db2RoutineInfo r = engine.parse(TEMP_TABLE_JOIN_SRC, "p_temp_join", null);
        dump("1a", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2CursorInfo cur1 = r.getCursors().stream()
                .filter(c -> "cur1".equalsIgnoreCase(c.getName())).findFirst().orElse(null);
        assertNotNull(cur1, "cursor with JOIN against the temp table must still be captured");
        // The joined "table" is SESSION.tmp_orders — captured as an ordinary base table,
        // indistinguishable from a real persistent table (no isTemporary flag exists anywhere).
        // The FROM-clause tableOp itself:
        boolean tmpOrdersAsFromTable = r.getTableOps().stream()
                .anyMatch(t -> "tmp_orders".equalsIgnoreCase(t.getTableName()));
        System.out.println(">>> tmpOrdersCapturedAsFromTable(indistinguishable from a real table)=" + tmpOrdersAsFromTable);
        assertTrue(tmpOrdersAsFromTable, "the temp table must be captured as a FROM-clause table op");
        boolean declarationCaptured = r.getStatements().stream()
                .anyMatch(s -> "DECLARE_GLOBAL_TEMPORARY_TABLE".equalsIgnoreCase(s.getType()));
        assertTrue(declarationCaptured, "DECLARE GLOBAL TEMPORARY TABLE must be recorded as a statement, not silently dropped");
    }

    private static final String TEMP_TABLE_INSERT_SRC =
        "CREATE PROCEDURE p_temp_insert(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE GLOBAL TEMPORARY TABLE tmp_staging (\n"
      + "    id INTEGER\n"
      + "  ) ON COMMIT DELETE ROWS;\n"
      + "  INSERT INTO SESSION.tmp_staging (id) SELECT order_id FROM orders WHERE region = p_region;\n"
      + "END";

    @Test
    @DisplayName("1b: DECLARE GLOBAL TEMPORARY TABLE + later INSERT INTO SESSION.<name> — INSERT table op captured, schema split correctly?")
    void tempTableDeclarationThenInsert() {
        Db2RoutineInfo r = engine.parse(TEMP_TABLE_INSERT_SRC, "p_temp_insert", null);
        dump("1b", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2TableOpInfo insertOp = r.getTableOps().stream()
                .filter(t -> t.getDmlType() == Db2TableOpInfo.DmlType.INSERT)
                .findFirst().orElse(null);
        assertNotNull(insertOp, "INSERT INTO the temp table must be captured as a table op");
        System.out.println(">>> insertOp schema=" + insertOp.getSchemaName()
                + " table=" + insertOp.getTableName());
        assertEquals("SESSION", insertOp.getSchemaName(), "SESSION should be split out as schema");
        assertEquals("TMP_STAGING", insertOp.getTableName());
        System.out.println(">>> NOTE: 'SESSION' is DB2's real qualifier for a declared global "
                + "temporary table, and the engine reports it as an ordinary schema-qualified "
                + "persistent table — it is not flagged temporary anywhere.");
    }

    // ═════════════════════════════════════════════════════════════════════
    // 2. Optimizer hints / comment-right-after-keyword corruption.
    //
    // Grammar evidence: grep of Db2Lexer.g4 / Db2Parser.g4 for "hint"/"OPTIMIZE"/"+*" found
    // NOTHING. DB2 LUW's actual optimizer-directive mechanisms (OPTIMIZE FOR n ROWS clause,
    // access-plan hints via db2advis/explain, or the deprecated NOFORCE) are not modeled as
    // any special grammar construct here, and DB2 does not support Oracle/MySQL-style
    // "/*+ ... */" embedded optimizer-hint comments at all — so that construct is
    // GENUINELY INAPPLICABLE to DB2 and is not fabricated as a test below.
    //
    // Comments in general are lexed as:
    //   LINE_COMMENT  : '--' ~[\r\n]*   -> skip ;
    //   BLOCK_COMMENT : '/*' .*? '*/'  -> skip ;
    // i.e. skipped unconditionally at the lexer level, same treatment regardless of content
    // or position (including a "/*+ ... */"-shaped one, which is just an ordinary comment to
    // this lexer). What IS tested here instead, per the task's fallback instruction, is a
    // plain block comment placed immediately after SELECT / INSERT / UPDATE / DELETE / MERGE.
    //
    // MERGE evidence: MERGE has NO lexer keyword and NO dedicated parser rule (grep of the
    // grammar for "MERGE" returns zero hits). Db2AnalysisVisitor.visitOtherStatement (lines
    // ~915-926) detects it only by checking ctx.getStart().getText().equalsIgnoreCase("MERGE")
    // and then regex-scans the raw statement text for "MERGE INTO ..." / "USING ...".
    // ═════════════════════════════════════════════════════════════════════

    private static final String CMT_AFTER_SELECT_SRC =
        "CREATE PROCEDURE p_cmt_after_select(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE cur1 CURSOR FOR\n"
      + "    SELECT /*+ ALL_ROWS */ o.order_id, o.customer_name\n"
      + "    FROM orders o\n"
      + "    WHERE o.region = p_region;\n"
      + "END";

    @Test
    @DisplayName("2a: '/*+ hint-shaped */' comment right after SELECT — treated as plain comment (DB2 has no hint syntax), select list intact?")
    void commentImmediatelyAfterSelect() {
        Db2RoutineInfo r = engine.parse(CMT_AFTER_SELECT_SRC, "p_cmt_after_select", null);
        dump("2a", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2CursorInfo cur1 = r.getCursors().stream()
                .filter(c -> "cur1".equalsIgnoreCase(c.getName())).findFirst().orElse(null);
        assertNotNull(cur1, "cursor must survive a comment right after SELECT");
        boolean customerNameSurvived = cur1.getSelectText() != null
                && cur1.getSelectText().toUpperCase().contains("CUSTOMER_NAME");
        System.out.println(">>> customerNameSurvived=" + customerNameSurvived);
        assertTrue(customerNameSurvived);
    }

    private static final String CMT_AFTER_INSERT_SRC =
        "CREATE PROCEDURE p_cmt_after_insert(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  INSERT /*+ APPEND */ INTO orders (order_id, region) VALUES (1, p_region);\n"
      + "END";

    @Test
    @DisplayName("2b: comment right after INSERT (before INTO) — table still captured?")
    void commentImmediatelyAfterInsert() {
        Db2RoutineInfo r = engine.parse(CMT_AFTER_INSERT_SRC, "p_cmt_after_insert", null);
        dump("2b", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2TableOpInfo op = r.getTableOps().stream()
                .filter(t -> t.getDmlType() == Db2TableOpInfo.DmlType.INSERT)
                .findFirst().orElse(null);
        System.out.println(">>> insertOp=" + op);
        assertNotNull(op, "INSERT INTO orders must be captured even with a comment right after INSERT");
        assertEquals("ORDERS", op.getTableName());
    }

    private static final String CMT_AFTER_UPDATE_SRC =
        "CREATE PROCEDURE p_cmt_after_update(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  UPDATE /*+ INDEX(orders idx1) */ orders SET status = 'X' WHERE region = p_region;\n"
      + "END";

    @Test
    @DisplayName("2c: comment right after UPDATE — table still captured?")
    void commentImmediatelyAfterUpdate() {
        Db2RoutineInfo r = engine.parse(CMT_AFTER_UPDATE_SRC, "p_cmt_after_update", null);
        dump("2c", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2TableOpInfo op = r.getTableOps().stream()
                .filter(t -> t.getDmlType() == Db2TableOpInfo.DmlType.UPDATE)
                .findFirst().orElse(null);
        System.out.println(">>> updateOp=" + op);
        assertNotNull(op, "UPDATE orders must be captured even with a comment right after UPDATE");
        assertEquals("ORDERS", op.getTableName());
    }

    private static final String CMT_AFTER_MERGE_SRC =
        "CREATE PROCEDURE p_cmt_after_merge(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  MERGE /*+ some-hint-shaped-text */ INTO orders AS tgt\n"
      + "    USING staging_orders AS src\n"
      + "    ON tgt.order_id = src.order_id\n"
      + "  WHEN MATCHED THEN UPDATE SET tgt.status = src.status\n"
      + "  WHEN NOT MATCHED THEN INSERT (order_id, status) VALUES (src.order_id, src.status);\n"
      + "END";

    @Test
    @DisplayName("2d: DB2 MERGE with a comment right after MERGE (before INTO) — MERGE detection is via regex over raw text; does the comment break it?")
    void commentImmediatelyAfterMerge() {
        Db2RoutineInfo r = engine.parse(CMT_AFTER_MERGE_SRC, "p_cmt_after_merge", null);
        dump("2d", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        boolean mergeStatementSeen = r.getStatements().stream()
                .anyMatch(s -> "MERGE".equalsIgnoreCase(s.getType()));
        System.out.println(">>> mergeStatementSeen=" + mergeStatementSeen);
        Db2TableOpInfo target = r.getTableOps().stream()
                .filter(t -> t.getDmlType() == Db2TableOpInfo.DmlType.MERGE)
                .findFirst().orElse(null);
        Db2TableOpInfo source = r.getTableOps().stream()
                .filter(t -> t.getDmlType() == Db2TableOpInfo.DmlType.SELECT
                        && "staging_orders".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        System.out.println(">>> mergeTargetOp=" + target);
        System.out.println(">>> mergeSourceOp=" + source);
        assertNotNull(target, "MERGE target must be captured even with a comment between MERGE and INTO");
        assertEquals("ORDERS", target.getTableName());
    }

    // Baseline: MERGE with NO comment, to isolate whether any failure above is caused by the
    // comment specifically or is a pre-existing gap unrelated to comments.
    private static final String MERGE_NO_COMMENT_SRC =
        "CREATE PROCEDURE p_merge_plain(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  MERGE INTO orders AS tgt\n"
      + "    USING staging_orders AS src\n"
      + "    ON tgt.order_id = src.order_id\n"
      + "  WHEN MATCHED THEN UPDATE SET tgt.status = src.status\n"
      + "  WHEN NOT MATCHED THEN INSERT (order_id, status) VALUES (src.order_id, src.status);\n"
      + "END";

    @Test
    @DisplayName("2e (baseline): plain DB2 MERGE, no comment — target/source captured?")
    void plainMergeBaseline() {
        Db2RoutineInfo r = engine.parse(MERGE_NO_COMMENT_SRC, "p_merge_plain", null);
        dump("2e", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2TableOpInfo target = r.getTableOps().stream()
                .filter(t -> t.getDmlType() == Db2TableOpInfo.DmlType.MERGE)
                .findFirst().orElse(null);
        Db2TableOpInfo source = r.getTableOps().stream()
                .filter(t -> t.getDmlType() == Db2TableOpInfo.DmlType.SELECT
                        && "staging_orders".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        System.out.println(">>> mergeTargetOp=" + target);
        System.out.println(">>> mergeSourceOp=" + source);
    }

    // ═════════════════════════════════════════════════════════════════════
    // 3. Cross-schema/cross-catalog qualified references and ALIAS (DB2's synonym).
    //
    // Grammar evidence:
    //   tableRef : id (DOT id)? ;                    // AT MOST 2 parts, no loop/recursion
    //   extractTableSchema()/extractTableName() in Db2AnalysisVisitor read ids.get(0) as
    //   schema and ids.get(size-1) as table — also hard-capped at 2 parts.
    // So a 3-part "catalog.schema.table" reference cannot be represented by this grammar's
    // tableRef rule at all (it only ever consumes one DOT).
    //
    // grep of Db2Lexer.g4 / Db2Parser.g4 for "ALIAS"/"SYNONYM" returns ZERO hits: DB2's
    // CREATE ALIAS (its synonym equivalent) has NO grammar rule whatsoever — it is not
    // recognized as routineDefinition (CREATE is followed only by PROCEDURE|FUNCTION) so a
    // top-level "CREATE ALIAS ..." statement would not even parse as a routine body at all.
    // This is GENUINELY INAPPLICABLE / unsupported, not fabricated below.
    // ═════════════════════════════════════════════════════════════════════

    private static final String THREE_PART_QUALIFIED_SRC =
        "CREATE PROCEDURE p_three_part(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE cur1 CURSOR FOR\n"
      + "    SELECT o.order_id\n"
      + "    FROM mycatalog.myschema.orders o\n"
      + "    WHERE o.region = p_region;\n"
      + "END";

    @Test
    @DisplayName("3a: 3-part catalog.schema.table reference in FROM — parses cleanly? table/schema fields correct or mis-split?")
    void threePartQualifiedTableReference() {
        Db2RoutineInfo r = engine.parse(THREE_PART_QUALIFIED_SRC, "p_three_part", null);
        dump("3a", r);
        System.out.println(">>> parseErrors=" + r.getErrors());
        Db2CursorInfo cur1 = r.getCursors().stream()
                .filter(c -> "cur1".equalsIgnoreCase(c.getName())).findFirst().orElse(null);
        System.out.println(">>> cursorCaptured=" + (cur1 != null));
        if (cur1 != null) {
            System.out.println(">>> selectText=[" + cur1.getSelectText() + "]");
        }
        boolean ordersCapturedCorrectly = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName())
                        && "myschema".equalsIgnoreCase(t.getSchemaName()));
        boolean garbageFragmentCaptured = r.getTableOps().stream()
                .anyMatch(t -> "myschema".equalsIgnoreCase(t.getTableName())
                        || "mycatalog".equalsIgnoreCase(t.getTableName())
                        || "mycatalog".equalsIgnoreCase(t.getSchemaName()));
        System.out.println(">>> ordersCapturedCorrectly(schema=myschema,table=orders)=" + ordersCapturedCorrectly);
        System.out.println(">>> garbageFragmentCapturedInstead(e.g. table=mycatalog or table=myschema)=" + garbageFragmentCaptured);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        assertTrue(ordersCapturedCorrectly, "real table 'orders' with schema='myschema' must be captured from a 3-part catalog.schema.table reference");
        assertFalse(garbageFragmentCaptured, "no garbage catalog/schema fragment should be captured as a table");
    }

    private static final String THREE_PART_INSERT_SRC =
        "CREATE PROCEDURE p_three_part_insert(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  INSERT INTO mycatalog.myschema.orders (order_id, region) VALUES (1, p_region);\n"
      + "END";

    @Test
    @DisplayName("3b: 3-part catalog.schema.table reference in INSERT INTO — parses cleanly? statement/table op preserved or dropped?")
    void threePartQualifiedInsert() {
        Db2RoutineInfo r = engine.parse(THREE_PART_INSERT_SRC, "p_three_part_insert", null);
        dump("3b", r);
        System.out.println(">>> parseErrors=" + r.getErrors());
        Db2TableOpInfo op = r.getTableOps().stream()
                .filter(t -> t.getDmlType() == Db2TableOpInfo.DmlType.INSERT)
                .findFirst().orElse(null);
        System.out.println(">>> insertOp=" + op);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        assertNotNull(op, "INSERT INTO a 3-part catalog.schema.table reference must be captured");
        assertEquals("MYSCHEMA", op.getSchemaName());
        assertEquals("ORDERS", op.getTableName());
    }

    private static final String TWO_PART_QUALIFIED_BASELINE_SRC =
        "CREATE PROCEDURE p_two_part(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE cur1 CURSOR FOR\n"
      + "    SELECT o.order_id\n"
      + "    FROM myschema.orders o\n"
      + "    WHERE o.region = p_region;\n"
      + "END";

    @Test
    @DisplayName("3c (baseline): 2-part schema.table reference — correctly split, for comparison against the 3-part case")
    void twoPartQualifiedBaseline() {
        Db2RoutineInfo r = engine.parse(TWO_PART_QUALIFIED_BASELINE_SRC, "p_two_part", null);
        dump("3c", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2TableOpInfo op = r.getTableOps().stream()
                .filter(t -> "orders".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        assertNotNull(op);
        assertEquals("MYSCHEMA", op.getSchemaName());
    }
}
