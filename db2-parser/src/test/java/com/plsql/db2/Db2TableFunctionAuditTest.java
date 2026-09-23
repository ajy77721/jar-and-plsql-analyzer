package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT (read-only, evidence-gathering) test file. Not a bug-fix.
 *
 * DB2's real equivalent of Oracle's TABLE(fn()) construct is a table function used directly in
 * FROM, e.g. {@code SELECT * FROM TABLE(my_func(...)) AS t}. Per source inspection,
 * Db2Parser.g4's `tableAtom` rule has ONLY three alternatives — atomTable, atomSubquery,
 * atomParen — with no atomFunc-style alternative (unlike Postgres); the `TABLE` keyword is not
 * wired structurally into tableAtom at all (it only appears for
 * DECLARE GLOBAL TEMPORARY TABLE). This suggests a genuine grammar gap for DB2 table functions.
 * This test records the ACTUAL observed parse/extraction behavior for confirmation.
 *
 * DB2 has no equivalent of Oracle's old-style {@code (+)} outer-join syntax (confirmed: grep
 * found zero matches for "(+)"/"outer_join_sign" in db2-parser) — intentionally not tested here.
 */
@DisplayName("AUDIT: DB2 TABLE(fn()) table-function usage in FROM/JOIN")
public class Db2TableFunctionAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private Db2RoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    @Test
    @DisplayName("AUDIT: JOIN TABLE(my_func(...)) AS t against a real table")
    void auditTableFunctionJoinedToRealTable() {
        String sql = "CREATE PROCEDURE list_items(IN p_id INT)\n" +
                "LANGUAGE SQL\n" +
                "BEGIN\n" +
                "  SELECT o.order_id, t.col1\n" +
                "  FROM orders o\n" +
                "  JOIN TABLE(get_line_items(o.order_id)) AS t ON t.some_col = o.some_col\n" +
                "  WHERE o.order_id = p_id;\n" +
                "END";

        Db2RoutineInfo r = parse(sql);

        List<String> tableNames = r.getTableOps().stream()
                .map(Db2TableOpInfo::getTableName)
                .filter(java.util.Objects::nonNull)
                .map(String::toUpperCase)
                .toList();
        List<String> callNames = r.getCalls().stream()
                .map(Db2CallInfo::getName)
                .filter(java.util.Objects::nonNull)
                .toList();

        System.out.println("[AUDIT] tableOps=" + tableNames + " calls=" + callNames
                + " errors=" + r.getErrors());

        boolean foundOrders = tableNames.contains("ORDERS");
        boolean foundCall = callNames.stream().anyMatch(n -> n.equalsIgnoreCase("get_line_items"));

        assertTrue(foundOrders, "AUDIT: ORDERS real table must not be dropped when joined against TABLE(fn())");
        assertTrue(foundCall, "AUDIT: get_line_items(...) should be recorded as a call from TABLE(...) in JOIN");
    }

    @Test
    @DisplayName("AUDIT: bare SELECT * FROM TABLE(some_pipelined_func(p1, p2)) AS t")
    void auditBareSelectFromTableFunction() {
        String sql = "CREATE PROCEDURE list_items2(IN p1 INT, IN p2 INT)\n" +
                "LANGUAGE SQL\n" +
                "BEGIN\n" +
                "  SELECT * FROM TABLE(some_pipelined_func(p1, p2)) AS t;\n" +
                "END";

        Db2RoutineInfo r = parse(sql);

        List<String> tableNames = r.getTableOps().stream()
                .map(Db2TableOpInfo::getTableName)
                .filter(java.util.Objects::nonNull)
                .toList();
        List<String> callNames = r.getCalls().stream()
                .map(Db2CallInfo::getName)
                .filter(java.util.Objects::nonNull)
                .toList();

        System.out.println("[AUDIT] bare select tableOps=" + tableNames + " calls=" + callNames
                + " errors=" + r.getErrors());

        boolean foundCall = callNames.stream().anyMatch(n -> n.equalsIgnoreCase("some_pipelined_func"));
        assertTrue(foundCall, "AUDIT: some_pipelined_func should be recorded as a call for bare SELECT * FROM TABLE(fn(...))");
    }

    @Test
    @DisplayName("AUDIT: multiple real tables joined together with one TABLE(fn()) mixed in")
    void auditMixedRealTablesAndTableFunction() {
        String sql = "CREATE PROCEDURE list_items3(IN p_id INT)\n" +
                "LANGUAGE SQL\n" +
                "BEGIN\n" +
                "  SELECT c.name, o.order_id, t.col1\n" +
                "  FROM customers c\n" +
                "  JOIN orders o ON o.customer_id = c.id\n" +
                "  JOIN TABLE(get_line_items(o.order_id)) AS t ON t.order_id = o.order_id\n" +
                "  WHERE c.id = p_id;\n" +
                "END";

        Db2RoutineInfo r = parse(sql);

        List<String> tableNames = r.getTableOps().stream()
                .map(Db2TableOpInfo::getTableName)
                .filter(java.util.Objects::nonNull)
                .map(String::toUpperCase)
                .toList();
        List<String> callNames = r.getCalls().stream()
                .map(Db2CallInfo::getName)
                .filter(java.util.Objects::nonNull)
                .toList();

        System.out.println("[AUDIT] mixed tableOps=" + tableNames + " calls=" + callNames
                + " errors=" + r.getErrors());

        assertTrue(tableNames.contains("CUSTOMERS"), "AUDIT: CUSTOMERS must be present");
        assertTrue(tableNames.contains("ORDERS"), "AUDIT: ORDERS must be present");
    }
}
