package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import com.plsql.sqlserver.model.SqlServerTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT ONLY — investigates whether SqlServerAnalysisEngine/SqlServerAnalysisVisitor correctly
 * capture tables/columns for eight realistic T-SQL DML shapes: multi-table joins (plain +
 * cursor), subqueries, CTEs, MERGE, OUTPUT-clause bulk INSERT, dynamic SQL with concatenated
 * table names, self-joins, and bare-name SELECT (view vs table ambiguity).
 *
 * Does NOT touch main/ code. Prints dump() evidence for each pattern and documents actual
 * behavior (including gaps) via assertions/println.
 */
class SqlServerDmlCaptureGapAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private void dump(String label, SqlServerRoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.qualifiedTable() + "@" + t.getLine()
                        + " joins=" + t.getJoins().stream().map(j -> j.getJoinType() + " " + j.getJoinedTable()).toList())
                .toList());
        System.out.println("dynamicSql: " + r.getDynamicSql().stream()
                .map(d -> d.getKind() + "@" + d.getLine() + " src=" + d.getSqlSource()).toList());
        System.out.println("cursors: " + r.getCursors().stream()
                .map(c -> c.getName() + "@" + c.getLine()).toList());
        System.out.println("errors: " + r.getErrors());
    }

    // ── Pattern 1a: multi-table JOIN (4 tables, mixed INNER/LEFT) in a plain SELECT ─────────
    @Test
    @DisplayName("Pattern 1a: multi-table JOIN in plain SELECT")
    void pattern1a_multiTableJoin_plainSelect() {
        String src =
                "CREATE PROCEDURE usp_join_plain\n" +
                "AS\nBEGIN\n" +
                "    SELECT o.OrderId, c.Name, p.ProductName, s.ShipperName\n" +
                "    FROM Orders o\n" +
                "    INNER JOIN Customers c ON o.CustomerId = c.CustomerId\n" +
                "    LEFT JOIN OrderDetails p ON o.OrderId = p.OrderId\n" +
                "    LEFT JOIN Shippers s ON o.ShipperId = s.ShipperId;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("pattern1a_multiTableJoin_plainSelect", r);

        SqlServerTableOpInfo orders = r.getTableOps().stream()
                .filter(t -> "Orders".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        assertNotNull(orders, "expected Orders base table op captured");
        System.out.println("pattern1a joins on Orders = " + orders.getJoins().size()
                + " -> " + orders.getJoins().stream().map(j -> j.getJoinType() + " " + j.getJoinedTable()).toList());
        // Bug 2 fix (§DML-audit): all 3 chained JOINs must attach to the Orders base op — the
        // terminator lookahead in JOIN_PATTERN previously required trailing whitespace even for
        // end-of-text, dropping the last join in any chain.
        assertEquals(3, orders.getJoins().size(), "expected all 3 joins attached to Orders base op, got: "
                + orders.getJoins().stream().map(j -> j.getJoinType() + " " + j.getJoinedTable()).toList());
        assertTrue(orders.getJoins().stream().anyMatch(j -> "Shippers".equalsIgnoreCase(j.getJoinedTable())),
                "3rd chained join (Shippers) must not be dropped");
        assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
    }

    // ── Pattern 1b: same multi-table JOIN inside a cursor declaration inside a procedure ────
    @Test
    @DisplayName("Pattern 1b: multi-table JOIN inside cursor declaration")
    void pattern1b_multiTableJoin_insideCursor() {
        String src =
                "CREATE PROCEDURE usp_join_cursor\n" +
                "AS\nBEGIN\n" +
                "    DECLARE @OrderId INT;\n" +
                "    DECLARE cur_orders CURSOR FOR\n" +
                "        SELECT o.OrderId\n" +
                "        FROM Orders o\n" +
                "        INNER JOIN Customers c ON o.CustomerId = c.CustomerId\n" +
                "        LEFT JOIN OrderDetails p ON o.OrderId = p.OrderId;\n" +
                "    OPEN cur_orders;\n" +
                "    FETCH NEXT FROM cur_orders INTO @OrderId;\n" +
                "    CLOSE cur_orders;\n" +
                "    DEALLOCATE cur_orders;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("pattern1b_multiTableJoin_insideCursor", r);

        assertTrue(r.getCursors().stream().anyMatch(c -> "cur_orders".equalsIgnoreCase(c.getName())),
                "expected cursor cur_orders captured");
        boolean ordersCaptured = r.getTableOps().stream().anyMatch(t -> "Orders".equalsIgnoreCase(t.getTableName()));
        boolean customersCaptured = r.getTableOps().stream().anyMatch(t -> "Customers".equalsIgnoreCase(t.getTableName()))
                || r.getTableOps().stream().flatMap(t -> t.getJoins().stream()).anyMatch(j -> "Customers".equalsIgnoreCase(j.getJoinedTable()));
        System.out.println("pattern1b ordersCaptured=" + ordersCaptured + " customersCaptured=" + customersCaptured);
        assertTrue(ordersCaptured, "expected Orders captured from inside cursor SELECT");
        assertTrue(customersCaptured, "expected Customers (joined table) captured from inside cursor SELECT");
    }

    // ── Pattern 2: subqueries (WHERE, SELECT-list scalar, derived table in FROM) ────────────
    @Test
    @DisplayName("Pattern 2: WHERE/scalar/derived-table subqueries")
    void pattern2_subqueries() {
        String src =
                "CREATE PROCEDURE usp_subqueries\n" +
                "AS\nBEGIN\n" +
                "    SELECT o.OrderId,\n" +
                "           (SELECT COUNT(*) FROM OrderLines ol WHERE ol.OrderId = o.OrderId) AS LineCount\n" +
                "    FROM Orders o\n" +
                "    WHERE o.CustomerId IN (SELECT c.CustomerId FROM Customers c WHERE c.Active = 1)\n" +
                "    AND EXISTS (SELECT 1 FROM DerivedSourceTable d WHERE d.OrderId = o.OrderId);\n" +
                "\n" +
                "    SELECT dt.TotalAmount\n" +
                "    FROM (SELECT SUM(Amount) AS TotalAmount FROM Payments p) dt;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("pattern2_subqueries", r);

        boolean orders = r.getTableOps().stream().anyMatch(t -> "Orders".equalsIgnoreCase(t.getTableName()));
        boolean orderLines = tableAnywhere(r, "OrderLines");
        boolean customers = tableAnywhere(r, "Customers");
        boolean derived = tableAnywhere(r, "DerivedSourceTable");
        boolean payments = tableAnywhere(r, "Payments");
        System.out.println("pattern2: Orders=" + orders + " OrderLines(scalar-subq)=" + orderLines
                + " Customers(where-subq)=" + customers + " DerivedSourceTable(exists-subq)=" + derived
                + " Payments(derived-table-in-from)=" + payments);
        assertTrue(orders, "expected outer Orders table captured");
        assertTrue(orderLines, "expected OrderLines from SELECT-list scalar subquery captured");
        assertTrue(customers, "expected Customers from WHERE IN subquery captured");
        assertTrue(derived, "expected DerivedSourceTable from EXISTS subquery captured");
        assertTrue(payments, "expected Payments from derived-table-in-FROM subquery captured");
    }

    private boolean tableAnywhere(SqlServerRoutineInfo r, String name) {
        return r.getTableOps().stream().anyMatch(t -> name.equalsIgnoreCase(t.getTableName()))
                || r.getTableOps().stream().flatMap(t -> t.getJoins().stream())
                        .anyMatch(j -> name.equalsIgnoreCase(j.getJoinedTable()));
    }

    // ── Pattern 3: CTE (two CTEs, one referencing another, main query selects from CTE) ─────
    @Test
    @DisplayName("Pattern 3: WITH-CTE, CTE name not a table, base tables inside CTE are")
    void pattern3_cte() {
        String src =
                "CREATE PROCEDURE usp_cte\n" +
                "AS\nBEGIN\n" +
                "    WITH ActiveCustomers AS (\n" +
                "        SELECT CustomerId, Name FROM Customers WHERE Active = 1\n" +
                "    ),\n" +
                "    ActiveCustomerOrders AS (\n" +
                "        SELECT o.OrderId, ac.Name\n" +
                "        FROM Orders o\n" +
                "        INNER JOIN ActiveCustomers ac ON o.CustomerId = ac.CustomerId\n" +
                "    )\n" +
                "    SELECT * FROM ActiveCustomerOrders;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("pattern3_cte", r);

        boolean customersCaptured = tableAnywhere(r, "Customers");
        boolean ordersCaptured = tableAnywhere(r, "Orders");
        boolean cte1AsTable = r.getTableOps().stream().anyMatch(t -> "ActiveCustomers".equalsIgnoreCase(t.getTableName()));
        boolean cte2AsTable = r.getTableOps().stream().anyMatch(t -> "ActiveCustomerOrders".equalsIgnoreCase(t.getTableName()));
        System.out.println("pattern3: Customers=" + customersCaptured + " Orders=" + ordersCaptured
                + " CTE1-as-table(should be false)=" + cte1AsTable + " CTE2-as-table(should be false)=" + cte2AsTable);
        assertTrue(customersCaptured, "expected real base table Customers (inside first CTE body) captured");
        assertTrue(ordersCaptured, "expected real base table Orders (inside second CTE body) captured");
        assertFalse(cte1AsTable, "CTE name ActiveCustomers must NOT be captured as a real table");
        assertFalse(cte2AsTable, "CTE name ActiveCustomerOrders must NOT be captured as a real table");
    }

    // ── Pattern 4: MERGE target/source, WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT ────────
    @Test
    @DisplayName("Pattern 4: MERGE target write, source read")
    void pattern4_merge() {
        String src =
                "CREATE PROCEDURE usp_merge\n" +
                "AS\nBEGIN\n" +
                "    MERGE INTO Inventory AS tgt\n" +
                "    USING StagingInventory AS src\n" +
                "    ON tgt.ProductId = src.ProductId\n" +
                "    WHEN MATCHED THEN\n" +
                "        UPDATE SET tgt.Qty = src.Qty\n" +
                "    WHEN NOT MATCHED THEN\n" +
                "        INSERT (ProductId, Qty) VALUES (src.ProductId, src.Qty);\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("pattern4_merge", r);

        SqlServerTableOpInfo mergeOp = r.getTableOps().stream()
                .filter(t -> "Inventory".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.MERGE)
                .findFirst().orElse(null);
        System.out.println("pattern4: mergeTarget=" + (mergeOp != null ? mergeOp.qualifiedTable() + ":" + mergeOp.getDmlType() : "NONE"));
        assertNotNull(mergeOp, "expected Inventory captured with MERGE dml type (target/write)");
        boolean phantomSet = r.getTableOps().stream().anyMatch(t -> "SET".equalsIgnoreCase(t.getTableName()));
        System.out.println("pattern4: phantom SET table captured=" + phantomSet + " (regression guard for Sep05-11 fix)");
        assertFalse(phantomSet, "MERGE ... WHEN MATCHED THEN UPDATE SET must not invent a table named SET");
        boolean sourceCaptured = tableAnywhere(r, "StagingInventory");
        // Bug 3 fix (§DML-audit): MERGE ... USING <source> must be captured as a read op.
        System.out.println("pattern4: sourceCaptured(StagingInventory)=" + sourceCaptured);
        assertTrue(sourceCaptured, "MERGE USING source (StagingInventory) must be captured");
        SqlServerTableOpInfo sourceOp = r.getTableOps().stream()
                .filter(t -> "StagingInventory".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(sourceOp, "expected StagingInventory table op");
        assertEquals(SqlServerTableOpInfo.DmlType.SELECT, sourceOp.getDmlType(),
                "MERGE USING source must be recorded as a read (SELECT) op");
    }

    // ── Pattern 5: bulk INSERT...OUTPUT...SELECT — target(write) + source(read), one statement ─
    @Test
    @DisplayName("Pattern 5: INSERT ... OUTPUT ... SELECT FROM source")
    void pattern5_insertOutputSelect() {
        String src =
                "CREATE PROCEDURE usp_insert_output\n" +
                "AS\nBEGIN\n" +
                "    INSERT INTO OrderArchive (OrderId, Amount)\n" +
                "    OUTPUT inserted.OrderId, inserted.Amount INTO @InsertedLog\n" +
                "    SELECT OrderId, Amount FROM OrdersSource WHERE Archived = 0;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("pattern5_insertOutputSelect", r);

        boolean targetCaptured = r.getTableOps().stream().anyMatch(t -> "OrderArchive".equalsIgnoreCase(t.getTableName())
                && t.getDmlType() == SqlServerTableOpInfo.DmlType.INSERT);
        boolean sourceCaptured = tableAnywhere(r, "OrdersSource");
        boolean insertedMisparsedAsTable = r.getTableOps().stream().anyMatch(t -> "inserted".equalsIgnoreCase(t.getTableName()));
        System.out.println("pattern5: target(OrderArchive,write)=" + targetCaptured
                + " source(OrdersSource,read)=" + sourceCaptured
                + " OUTPUT-pseudo-table-miscounted=" + insertedMisparsedAsTable
                + " totalTableOps=" + r.getTableOps().size());
        assertTrue(targetCaptured, "expected OrderArchive captured as INSERT target/write");
        assertTrue(sourceCaptured, "expected OrdersSource captured as SELECT source/read");
        assertFalse(insertedMisparsedAsTable, "OUTPUT clause's 'inserted' pseudo-table must not be miscounted as a real table");
    }

    // ── Pattern 6: dynamic SQL via sp_executesql / EXEC() with concatenated table name ──────
    @Test
    @DisplayName("Pattern 6: dynamic SQL — table name built via string concatenation")
    void pattern6_dynamicSql_concatenatedTableName() {
        String src =
                "CREATE PROCEDURE usp_dynamic_concat\n" +
                "    @tablePrefix NVARCHAR(50)\n" +
                "AS\nBEGIN\n" +
                "    DECLARE @sql NVARCHAR(MAX);\n" +
                "    SET @sql = 'SELECT * FROM ' + @tablePrefix + '_orders';\n" +
                "    EXEC sp_executesql @sql;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("pattern6_dynamicSql_concatenatedTableName", r);

        System.out.println("pattern6: dynamicSql entries=" + r.getDynamicSql().size());
        r.getDynamicSql().forEach(d -> System.out.println("  kind=" + d.getKind() + " sqlSource=" + d.getSqlSource()));
        // Does ANY table op record a table derived from the concatenated dynamic SQL literal
        // fragment (cf. plsql-parser's DynamicSqlConcatTest, which extracts EMPLOYEES /
        // SOURCE_DATA from EXECUTE IMMEDIATE-built strings)?
        boolean anyOrdersLikeTableCaptured = r.getTableOps().stream()
                .anyMatch(t -> t.getTableName() != null && t.getTableName().toLowerCase().contains("orders"));
        System.out.println("pattern6: any '*orders*' table op captured from dynamic SQL fragment=" + anyOrdersLikeTableCaptured);
        System.out.println("pattern6: tableOps total=" + r.getTableOps().size());
        // Documents current behavior — SqlServerDynamicSqlInfo has kind/sqlSource/line only,
        // no extracted table list, unlike plsql-parser's dynamic-SQL table recovery.
        assertTrue(r.getDynamicSql().stream().anyMatch(d -> d.getKind() == com.plsql.sqlserver.model.SqlServerDynamicSqlInfo.Kind.SET_SQL),
                "expected SET_SQL dynamicSql entry captured for the @sql assignment");
        assertTrue(r.getDynamicSql().stream().anyMatch(d -> d.getKind() == com.plsql.sqlserver.model.SqlServerDynamicSqlInfo.Kind.EXEC_SP_EXECUTESQL),
                "expected EXEC_SP_EXECUTESQL dynamicSql entry captured for the sp_executesql call");
    }

    // ── Pattern 6b: Bug 9 (§DML-audit, lower priority, literal-only scope) — EXEC('<literal>')
    // with no variable/concatenation must still yield a table op. ──────────────────────────
    @Test
    @DisplayName("Pattern 6b: EXEC('literal SQL') with no variable extracts the table name")
    void pattern6b_dynamicSql_literalExecString() {
        String src =
                "CREATE PROCEDURE usp_dynamic_literal\n" +
                "AS\nBEGIN\n" +
                "    EXEC('DELETE FROM temp_staging WHERE 1 = 1');\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("pattern6b_dynamicSql_literalExecString", r);
        boolean tempStagingCaptured = r.getTableOps().stream()
                .anyMatch(t -> "temp_staging".equalsIgnoreCase(t.getTableName()));
        assertTrue(tempStagingCaptured, "fully-literal EXEC('...') dynamic SQL must yield a table op");
    }

    // ── Pattern 7: self-join — single "orders" table captured despite two aliases ───────────
    @Test
    @DisplayName("Pattern 7: self-join collapses to a single table entry")
    void pattern7_selfJoin() {
        String src =
                "CREATE PROCEDURE usp_self_join\n" +
                "AS\nBEGIN\n" +
                "    SELECT o1.OrderId, o2.OrderId AS PriorOrderId\n" +
                "    FROM Orders o1\n" +
                "    JOIN Orders o2 ON o1.CustomerId = o2.CustomerId AND o2.OrderId < o1.OrderId;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("pattern7_selfJoin", r);

        long ordersOpsCount = r.getTableOps().stream().filter(t -> "Orders".equalsIgnoreCase(t.getTableName())).count();
        System.out.println("pattern7: distinct Orders table-op entries=" + ordersOpsCount);
        assertEquals(1, ordersOpsCount, "expected a single Orders table-op entry for a self-join, got "
                + ordersOpsCount + ": " + r.getTableOps().stream().map(SqlServerTableOpInfo::qualifiedTable).toList());
        SqlServerTableOpInfo orders = r.getTableOps().stream().filter(t -> "Orders".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        assertNotNull(orders);
        System.out.println("pattern7: joins on Orders=" + orders.getJoins().stream().map(j -> j.getJoinType() + " " + j.getJoinedTable()).toList());
    }

    // ── Pattern 8: plain SELECT from a bare name — could be a view, treated as table ────────
    @Test
    @DisplayName("Pattern 8: bare-name SELECT treated identically whether table or view")
    void pattern8_bareNameSelect_viewVsTable() {
        String src =
                "CREATE PROCEDURE usp_bare_select\n" +
                "AS\nBEGIN\n" +
                "    SELECT * FROM vw_ActiveCustomers;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("pattern8_bareNameSelect_viewVsTable", r);

        boolean captured = r.getTableOps().stream().anyMatch(t -> "vw_ActiveCustomers".equalsIgnoreCase(t.getTableName())
                && t.getDmlType() == SqlServerTableOpInfo.DmlType.SELECT);
        System.out.println("pattern8: vw_ActiveCustomers captured as plain SELECT table op=" + captured
                + " (structural limitation: parser has no catalog, cannot distinguish view from table)");
        assertTrue(captured, "expected bare name vw_ActiveCustomers captured as a SELECT table op, "
                + "indistinguishable from a real table without a catalog lookup");
    }
}
