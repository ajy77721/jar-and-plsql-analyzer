package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerParamInfo;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import com.plsql.sqlserver.model.SqlServerTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * AUDIT ONLY — read-only investigation of four SQL-Server-unique T-SQL concepts against the
 * real ANTLR parser: table-valued parameters, TRY/CATCH with THROW/RAISERROR, recursive CTE
 * with OPTION(MAXRECURSION), and OUTPUT clause with a table-variable target. Does NOT modify
 * any main/ code. Prints dump() evidence for each pattern.
 */
class SqlServerUniqueConceptsAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private void dump(String label, SqlServerRoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("params: " + r.getParams().stream()
                .map(p -> p.getName() + " type=" + p.getDataType() + " dir=" + p.getDirection() + "@" + p.getLine())
                .toList());
        System.out.println("tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.qualifiedTable() + "@" + t.getLine()
                        + " joins=" + t.getJoins().stream().map(j -> j.getJoinType() + " " + j.getJoinedTable()).toList())
                .toList());
        System.out.println("errors: " + r.getErrors());
    }

    // ── Item 6: table-valued parameter ───────────────────────────────────────────────────────
    @Test
    @DisplayName("Item 6: table-valued parameter (TVP) declared READONLY, used in body")
    void item6_tableValuedParameter() {
        String src =
                "CREATE PROCEDURE usp_bulk_insert_orders\n" +
                "    @tvp dbo.OrderTableType READONLY\n" +
                "AS\nBEGIN\n" +
                "    INSERT INTO Orders (OrderId, CustomerId, Amount)\n" +
                "    SELECT OrderId, CustomerId, Amount FROM @tvp;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("item6_tableValuedParameter", r);

        SqlServerParamInfo tvp = r.getParams().stream()
                .filter(p -> "@tvp".equalsIgnoreCase(p.getName()) || "tvp".equalsIgnoreCase(p.getName()))
                .findFirst().orElse(null);
        System.out.println("item6: tvp param found=" + (tvp != null)
                + " dataType=" + (tvp != null ? tvp.getDataType() : "N/A"));

        boolean tvpMisclassifiedAsTable = r.getTableOps().stream()
                .anyMatch(t -> t.getTableName() != null && t.getTableName().toLowerCase().contains("tvp"));
        boolean ordersTargetCaptured = r.getTableOps().stream()
                .anyMatch(t -> "Orders".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.INSERT);
        System.out.println("item6: Orders INSERT target captured=" + ordersTargetCaptured
                + " @tvp misclassified as separate table op=" + tvpMisclassifiedAsTable
                + " totalTableOps=" + r.getTableOps().size());

        // Regression guard: "SELECT ... FROM @tvp" previously left a bogus SELECT table op with
        // tableName=null in the results — the null-guard in visitSelectStatement's "guarantee at
        // least one SELECT op" fallback fired even when a FROM clause was present but its only
        // source (@tvp) was deliberately filtered out as a parameter, not a real table.
        boolean nullNamedTableOp = r.getTableOps().stream().anyMatch(t -> t.getTableName() == null);
        org.junit.jupiter.api.Assertions.assertFalse(nullNamedTableOp,
                "no table op with a null table name should be produced for SELECT ... FROM @tvp");
        org.junit.jupiter.api.Assertions.assertEquals(1, r.getTableOps().size(),
                "only the Orders INSERT should be captured; the @tvp SELECT source must not add a bogus op");
        org.junit.jupiter.api.Assertions.assertTrue(ordersTargetCaptured);
        org.junit.jupiter.api.Assertions.assertFalse(tvpMisclassifiedAsTable);
    }

    // ── Item 7: TRY/CATCH with THROW and RAISERROR ───────────────────────────────────────────
    @Test
    @DisplayName("Item 7: TRY/CATCH with THROW + RAISERROR, normal capture inside blocks")
    void item7_tryCatchThrowRaiserror() {
        String src =
                "CREATE PROCEDURE usp_process_payment\n" +
                "    @orderId INT\n" +
                "AS\nBEGIN\n" +
                "    BEGIN TRY\n" +
                "        INSERT INTO PaymentLedger (OrderId, Status)\n" +
                "        VALUES (@orderId, 'PENDING');\n" +
                "        UPDATE Orders SET Status = 'PAID' WHERE OrderId = @orderId;\n" +
                "        IF @@ROWCOUNT = 0\n" +
                "            THROW 51000, 'Order not found', 1;\n" +
                "    END TRY\n" +
                "    BEGIN CATCH\n" +
                "        INSERT INTO ErrorLog (Message, Proc)\n" +
                "        SELECT ERROR_MESSAGE(), 'usp_process_payment' FROM ErrorSourceInfo;\n" +
                "        RAISERROR('Payment processing failed: %s', 16, 1, ERROR_MESSAGE());\n" +
                "    END CATCH\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("item7_tryCatchThrowRaiserror", r);

        boolean insertInTry = r.getTableOps().stream()
                .anyMatch(t -> "PaymentLedger".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.INSERT);
        boolean updateInTry = r.getTableOps().stream()
                .anyMatch(t -> "Orders".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.UPDATE);
        boolean insertInCatch = r.getTableOps().stream()
                .anyMatch(t -> "ErrorLog".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.INSERT);
        boolean selectSourceInCatch = r.getTableOps().stream()
                .anyMatch(t -> "ErrorSourceInfo".equalsIgnoreCase(t.getTableName()));
        System.out.println("item7: insertInTry(PaymentLedger)=" + insertInTry
                + " updateInTry(Orders)=" + updateInTry
                + " insertInCatch(ErrorLog)=" + insertInCatch
                + " selectSourceInCatch(ErrorSourceInfo)=" + selectSourceInCatch
                + " errors=" + r.getErrors());
    }

    // ── Item 8: recursive CTE + OPTION(MAXRECURSION) ─────────────────────────────────────────
    @Test
    @DisplayName("Item 8: recursive CTE with anchor+recursive members, OPTION(MAXRECURSION)")
    void item8_recursiveCteWithMaxRecursion() {
        String src =
                "CREATE PROCEDURE usp_org_chart\n" +
                "    @rootEmployeeId INT\n" +
                "AS\nBEGIN\n" +
                "    WITH EmpHierarchy AS (\n" +
                "        SELECT EmployeeId, ManagerId, 0 AS Level\n" +
                "        FROM Employees\n" +
                "        WHERE EmployeeId = @rootEmployeeId\n" +
                "        UNION ALL\n" +
                "        SELECT e.EmployeeId, e.ManagerId, h.Level + 1\n" +
                "        FROM Employees e\n" +
                "        JOIN EmpHierarchy h ON e.ManagerId = h.EmployeeId\n" +
                "    )\n" +
                "    SELECT * FROM EmpHierarchy\n" +
                "    OPTION (MAXRECURSION 100);\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("item8_recursiveCteWithMaxRecursion", r);

        boolean employeesCaptured = r.getTableOps().stream()
                .anyMatch(t -> "Employees".equalsIgnoreCase(t.getTableName()))
                || r.getTableOps().stream().flatMap(t -> t.getJoins().stream())
                        .anyMatch(j -> "Employees".equalsIgnoreCase(j.getJoinedTable()));
        boolean cteNameAsTable = r.getTableOps().stream()
                .anyMatch(t -> "EmpHierarchy".equalsIgnoreCase(t.getTableName()))
                || r.getTableOps().stream().flatMap(t -> t.getJoins().stream())
                        .anyMatch(j -> "EmpHierarchy".equalsIgnoreCase(j.getJoinedTable()));
        System.out.println("item8: Employees captured (anchor+recursive)=" + employeesCaptured
                + " EmpHierarchy misclassified as real table=" + cteNameAsTable
                + " errors=" + r.getErrors());
    }

    // ── Item 9: OUTPUT clause with table-variable target ─────────────────────────────────────
    @Test
    @DisplayName("Item 9: INSERT ... OUTPUT inserted.id INTO @table_var VALUES (...)")
    void item9_outputClauseWithTableVariableTarget() {
        String src =
                "CREATE PROCEDURE usp_create_order\n" +
                "    @customerId INT, @amount DECIMAL(10,2)\n" +
                "AS\nBEGIN\n" +
                "    DECLARE @InsertedIds TABLE (OrderId INT);\n" +
                "    INSERT INTO Orders (CustomerId, Amount)\n" +
                "    OUTPUT inserted.OrderId INTO @InsertedIds\n" +
                "    VALUES (@customerId, @amount);\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("item9_outputClauseWithTableVariableTarget", r);

        boolean targetCaptured = r.getTableOps().stream()
                .anyMatch(t -> "Orders".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.INSERT);
        boolean insertedMisparsed = r.getTableOps().stream()
                .anyMatch(t -> "inserted".equalsIgnoreCase(t.getTableName()));
        boolean tableVarMisparsedAsRealTable = r.getTableOps().stream()
                .anyMatch(t -> t.getTableName() != null && t.getTableName().toLowerCase().contains("insertedids"));
        System.out.println("item9: Orders INSERT target captured=" + targetCaptured
                + " 'inserted' pseudo-table misparsed=" + insertedMisparsed
                + " @InsertedIds table-var misclassified as real table=" + tableVarMisparsedAsRealTable
                + " totalTableOps=" + r.getTableOps().size()
                + " errors=" + r.getErrors());
    }
}
