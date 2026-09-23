package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * READ-ONLY AUDIT (2026-09-17) — hierarchical query / recursive CTE / sequence-equivalent
 * audit for mysql-parser, run against the real MySqlGrammarEngine / MySqlAnalysisVisitor.
 * No source fixes are made here; this exists to print real captured evidence.
 *
 * <p>Item 1 (CONNECT BY / START WITH / LEVEL / SYS_CONNECT_BY_PATH) is confirmed NOT
 * APPLICABLE to MySQL: {@code grep -rniE "connect_by|sys_connect_by_path|start with|level"
 * mysql-parser/src/main} returns zero grammar/keyword matches (only unrelated doc-comment
 * hits for "level" as an English word). MySQL has no Oracle-style hierarchical query syntax
 * at all (it uses recursive CTEs instead, covered below), so no fabricated test for it.
 */
public class MySqlRecursiveCteAndSequenceAuditTest {

    private static List<String> tableNames(MySqlRoutineInfo r) {
        return r.getTableOps().stream().map(MySqlTableOpInfo::getTableName).toList();
    }

    // ── Item 2: WITH RECURSIVE common table expressions ────────────────────────────────────

    @Test
    @DisplayName("2a/2b: WITH RECURSIVE anchor + recursive branch base tables captured")
    void recursiveCteAnchorAndRecursiveBranchBaseTables() {
        String sql = """
                CREATE PROCEDURE proc_org_chart(IN p_root_id INT)
                BEGIN
                  WITH RECURSIVE org_chain AS (
                    SELECT id, manager_id, name, 1 AS depth
                    FROM employees
                    WHERE id = p_root_id
                    UNION ALL
                    SELECT e.id, e.manager_id, e.name, oc.depth + 1
                    FROM employees e
                    JOIN org_chain oc ON e.manager_id = oc.id
                  )
                  SELECT * FROM org_chain ORDER BY depth;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_org_chart", null);
        System.out.println("[2ab] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName() + "@L" + t.getLine()).toList());
        System.out.println("[2ab] errors: " + r.getErrors());

        boolean anchorEmployeesCaptured = r.getTableOps().stream()
                .anyMatch(t -> "employees".equalsIgnoreCase(t.getTableName()));
        System.out.println("[2ab] anchorEmployeesCaptured=" + anchorEmployeesCaptured);
        // (c) does the CTE's own name (org_chain) leak in as a fake external base table
        // from the recursive branch's "JOIN org_chain oc ON ..."?
        boolean cteNameLeakedAsTable = r.getTableOps().stream()
                .anyMatch(t -> "org_chain".equalsIgnoreCase(t.getTableName()));
        System.out.println("[2c] cteNameLeakedAsTable=" + cteNameLeakedAsTable
                + " (expected false — org_chain is the CTE's own name, not a base table)");
    }

    @Test
    @DisplayName("2d: multi-level chained recursive CTEs (one recursive CTE feeding another)")
    void chainedMultiLevelRecursiveCtes() {
        String sql = """
                CREATE PROCEDURE proc_multi_level_chain()
                BEGIN
                  WITH RECURSIVE levels AS (
                    SELECT id, parent_id, 1 AS depth
                    FROM categories
                    WHERE parent_id IS NULL
                    UNION ALL
                    SELECT c.id, c.parent_id, l.depth + 1
                    FROM categories c
                    JOIN levels l ON c.parent_id = l.id
                  ),
                  paths AS (
                    SELECT id, depth, 1 AS step
                    FROM levels
                    WHERE depth = 1
                    UNION ALL
                    SELECT lv.id, lv.depth, p.step + 1
                    FROM levels lv
                    JOIN paths p ON lv.depth = p.depth + 1
                  )
                  SELECT * FROM paths;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_multi_level_chain", null);
        System.out.println("[2d] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName() + "@L" + t.getLine()).toList());
        System.out.println("[2d] errors: " + r.getErrors());

        boolean categoriesCaptured = r.getTableOps().stream()
                .anyMatch(t -> "categories".equalsIgnoreCase(t.getTableName()));
        boolean levelsLeakedAsTable = r.getTableOps().stream()
                .anyMatch(t -> "levels".equalsIgnoreCase(t.getTableName()));
        boolean pathsLeakedAsTable = r.getTableOps().stream()
                .anyMatch(t -> "paths".equalsIgnoreCase(t.getTableName()));
        System.out.println("[2d] categoriesCaptured=" + categoriesCaptured
                + " levelsLeakedAsTable=" + levelsLeakedAsTable
                + " (levels is a CTE name used as a base table in the 'paths' CTE, and the"
                + " visitor's cteNames set is never scoped/cleared, so this is also exercising"
                + " whether an unrelated real table sharing a name with an EARLIER script's CTE"
                + " could be wrongly skipped — not exercised directly here, but cteNames leaking"
                + " across sibling CTE defs within the same WITH clause is the risk under test)"
                + " pathsLeakedAsTable=" + pathsLeakedAsTable);
    }

    @Test
    @DisplayName("2c-focused: recursive branch referencing CTE name with a table alias")
    void recursiveBranchCteNameWithAlias() {
        String sql = """
                CREATE PROCEDURE proc_bom_explosion(IN p_root_part INT)
                BEGIN
                  WITH RECURSIVE bom AS (
                    SELECT part_id, component_id, 1 AS lvl
                    FROM bill_of_materials
                    WHERE part_id = p_root_part
                    UNION ALL
                    SELECT b.part_id, b.component_id, bom.lvl + 1
                    FROM bill_of_materials b, bom
                    WHERE b.part_id = bom.component_id
                  )
                  SELECT * FROM bom;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_bom_explosion", null);
        System.out.println("[2c-2] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName() + "@L" + t.getLine()).toList());
        System.out.println("[2c-2] errors: " + r.getErrors());

        boolean bomLeakedAsTable = r.getTableOps().stream()
                .anyMatch(t -> "bom".equalsIgnoreCase(t.getTableName()));
        boolean bomTableCaptured = r.getTableOps().stream()
                .anyMatch(t -> "bill_of_materials".equalsIgnoreCase(t.getTableName()));
        System.out.println("[2c-2] bomLeakedAsTable=" + bomLeakedAsTable
                + " (expected false) bomTableCaptured=" + bomTableCaptured);
    }

    // ── Item 3: AUTO_INCREMENT / LAST_INSERT_ID() sequence-equivalents ──────────────────────

    @Test
    @DisplayName("3a: INSERT into an AUTO_INCREMENT-style table, then SELECT LAST_INSERT_ID()")
    void insertThenLastInsertId() {
        String sql = """
                CREATE PROCEDURE proc_create_order(IN p_customer_id INT, OUT p_new_order_id INT)
                BEGIN
                  INSERT INTO orders (customer_id, status) VALUES (p_customer_id, 'NEW');
                  SELECT LAST_INSERT_ID() INTO p_new_order_id;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_create_order", null);
        System.out.println("[3a] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName() + "@L" + t.getLine()).toList());
        System.out.println("[3a] errors: " + r.getErrors());

        boolean ordersInsertCaptured = r.getTableOps().stream().anyMatch(t ->
                t.getDmlType() == MySqlTableOpInfo.DmlType.INSERT
                        && "orders".equalsIgnoreCase(t.getTableName()));
        // LAST_INSERT_ID must NOT show up as a mis-parsed table name anywhere in tableOps.
        boolean lastInsertIdMisparsedAsTable = r.getTableOps().stream()
                .anyMatch(t -> t.getTableName() != null
                        && t.getTableName().toUpperCase().contains("LAST_INSERT_ID"));
        System.out.println("[3a] ordersInsertCaptured=" + ordersInsertCaptured
                + " lastInsertIdMisparsedAsTable=" + lastInsertIdMisparsedAsTable
                + " (expected true / false respectively)");
    }

    @Test
    @DisplayName("3b: LAST_INSERT_ID() used directly in an INSERT ... SELECT chain into a child table")
    void lastInsertIdUsedInChainedInsert() {
        String sql = """
                CREATE PROCEDURE proc_create_order_with_item(IN p_customer_id INT, IN p_sku VARCHAR(64))
                BEGIN
                  INSERT INTO orders (customer_id, status) VALUES (p_customer_id, 'NEW');
                  INSERT INTO order_items (order_id, sku) VALUES (LAST_INSERT_ID(), p_sku);
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_create_order_with_item", null);
        System.out.println("[3b] tableOps: " + tableNames(r));
        System.out.println("[3b] errors: " + r.getErrors());

        boolean ordersCaptured = tableNames(r).stream().anyMatch("orders"::equalsIgnoreCase);
        boolean orderItemsCaptured = tableNames(r).stream().anyMatch("order_items"::equalsIgnoreCase);
        System.out.println("[3b] ordersCaptured=" + ordersCaptured
                + " orderItemsCaptured=" + orderItemsCaptured
                + " (both real tables must survive; LAST_INSERT_ID() is only a VALUES expression,"
                + " not a table reference, so it must never appear in tableOps at all)");
    }

    @Test
    @DisplayName("3c: table literally named auto_increment-adjacent, to rule out keyword collision")
    void tableNameNotConfusedWithAutoIncrementKeyword() {
        String sql = """
                CREATE PROCEDURE proc_touch_sequence_table()
                BEGIN
                  UPDATE id_sequences SET current_value = LAST_INSERT_ID() WHERE seq_name = 'orders';
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_touch_sequence_table", null);
        System.out.println("[3c] tableOps: " + tableNames(r));
        System.out.println("[3c] errors: " + r.getErrors());

        boolean idSequencesCaptured = tableNames(r).stream().anyMatch("id_sequences"::equalsIgnoreCase);
        System.out.println("[3c] idSequencesCaptured=" + idSequencesCaptured);
    }

    @Test
    @DisplayName("2d: WITH RECURSIVE with an explicit CTE column list, joined in both branches")
    void recursiveCteWithExplicitColumnListAndJoins() {
        // `WITH RECURSIVE tree (id, parent_id, lvl) AS (...)` — naming the CTE's columns up front
        // is the common way to write a recursive CTE, because it is what makes the anchor and the
        // recursive branch line up for a reader. cteDefinition did not allow the list, so this
        // failed to parse and the ENTIRE statement was discarded as unsupported: no org table, no
        // depts table, and no join edges at all — a silently empty result, not a visible error.
        String sql = """
                CREATE PROCEDURE proc_org_tree()
                BEGIN
                  WITH RECURSIVE tree (id, parent_id, lvl) AS (
                    SELECT id, parent_id, 0 FROM org WHERE parent_id IS NULL
                    UNION ALL
                    SELECT o.id, o.parent_id, t.lvl + 1
                      FROM org o JOIN tree t ON o.parent_id = t.id
                  )
                  SELECT t.id, d.name FROM tree t LEFT JOIN depts d ON d.id = t.id;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_org_tree", null);
        System.out.println("[2d] tableOps: " + tableNames(r));
        System.out.println("[2d] errors: " + r.getErrors());

        assertTrue(r.getErrors().isEmpty(), "CTE column list must parse: " + r.getErrors());
        assertTrue(tableNames(r).stream().anyMatch("org"::equalsIgnoreCase),
                "the recursive CTE's real base table must be captured; got " + tableNames(r));
        assertTrue(tableNames(r).stream().anyMatch("depts"::equalsIgnoreCase),
                "the outer query's joined table must be captured; got " + tableNames(r));
        assertTrue(r.getTableOps().stream().flatMap(t -> t.getJoins().stream()).count() >= 1,
                "the JOIN inside the recursive branch must survive");
    }

    @Test
    @DisplayName("2e: JSON path operators -> and ->>, including FROM-like text inside the path")
    void jsonPathOperatorsAreRecognized() {
        // `doc->'$.a'` / `doc->>'$.status'` lexed as MINUS GT (GT), no expression alternative
        // matched, and the whole SELECT — with its FROM table — was dropped as unsupported.
        // The JSON path arguments here deliberately contain SQL-looking text, which must stay
        // inside the literal and never be read as a second query.
        String sql = """
                CREATE PROCEDURE proc_json_docs()
                BEGIN
                  SELECT JSON_EXTRACT(doc, '$.from_table') AS f,
                         doc->'$.a.b'                      AS b,
                         doc->>'$."select * from evil"'    AS e
                    FROM jdocs
                   WHERE doc->>'$.status' = 'OK';
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_json_docs", null);
        System.out.println("[2e] tableOps: " + tableNames(r));
        System.out.println("[2e] errors: " + r.getErrors());

        assertTrue(r.getErrors().isEmpty(), "JSON path operators must parse: " + r.getErrors());
        assertTrue(tableNames(r).stream().anyMatch("jdocs"::equalsIgnoreCase),
                "the real FROM table must be captured; got " + tableNames(r));
        assertFalse(tableNames(r).stream().anyMatch("evil"::equalsIgnoreCase),
                "FROM-like text inside a JSON path literal must not become a table: " + tableNames(r));
        assertFalse(tableNames(r).stream().anyMatch("from_table"::equalsIgnoreCase),
                "JSON path text must not become a table: " + tableNames(r));
    }
}
