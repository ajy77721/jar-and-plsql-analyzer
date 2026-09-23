package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ING-024 — {@code TableOperationEnricher.enrich()}/{@code coverage()} only ever walked {@code
 * ParsedObject.tableOperations}, never {@code SubprogramInfo.tableOperations}, where a {@code
 * PACKAGE BODY}-contained procedure's own operations land (the ANTLR visitor's {@code
 * currentSubprogram()} is null at the top level, so a standalone procedure's operations land on
 * the object instead). Column/predicate/join/DDL enrichment was therefore a silent no-op for every
 * procedure declared inside a package body, on every dialect — found while fixing ING-019.
 *
 * <p>Fixed by walking both lists ({@code enrichOps}, reused for each). The real obstacle, found by
 * measuring rather than guessing: a multi-table statement (a JOIN chain) produces one {@code
 * TableOperationInfo} per table it touches, all sharing the identical {@code sqlText} — but each
 * with a <em>different</em> {@code line} (confirmed with a debug dump of a real 3-join cursor
 * before writing the fix, not assumed). The visitor's own native population already puts the
 * statement's full, correct join list on exactly one of those sibling ops; enriching every sibling
 * independently re-derived and re-attached the same joins, so a real 3-join query first measured
 * as 12 (unscoped), then 6 (scoped to each op's own table, but each join still recorded once per
 * side), before landing on the fix actually shipped: join-carry is skipped entirely for any op
 * whose statement ({@code sqlText} alone — {@code line} varies per sibling and cannot be part of
 * the key) already has native joins on a sibling, and otherwise scoped to the op's own table (the
 * same criterion {@code ownedBy} already applies to column usages).
 */
public class PackageBodyEnrichmentTest extends ParserTestBase {

    private TableOperationInfo firstOp(java.util.List<TableOperationInfo> ops, String tableName) {
        for (TableOperationInfo t : ops) {
            if (tableName.equalsIgnoreCase(t.getTableName())) return t;
        }
        return null;
    }

    @Test
    @DisplayName("A package-body procedure's DML gets column enrichment, same as a standalone one")
    void testPackageBodyProcedureGetsColumnEnrichment() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY ORDER_PKG AS
                  PROCEDURE UPDATE_STATUS IS
                  BEGIN
                    UPDATE erp_orders SET status = 'SHIPPED' WHERE order_id = 42;
                  END UPDATE_STATUS;
                END ORDER_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "package-body UPDATE");
        SubprogramInfo proc = findSub(result, "UPDATE_STATUS");
        assertNotNull(proc);
        TableOperationInfo op = firstOp(proc.getTableOperations(), "erp_orders");
        assertNotNull(op, "erp_orders operation must be on the subprogram, not lost");
        assertEquals(Boolean.TRUE, op.getColumnsParsed(), "enrichment must have actually run against this op");
        assertFalse(op.getColumnUsages().isEmpty(), "SET status must be captured as a column usage");
    }

    @Test
    @DisplayName("A package-body procedure's dynamic DDL is classified DDL, same as ING-019's standalone case")
    void testPackageBodyProcedureGetsDdlEnrichment() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY ADMIN_PKG AS
                  PROCEDURE SETUP IS
                  BEGIN
                    EXECUTE IMMEDIATE 'CREATE TABLE staging_orders (id NUMBER, amt NUMBER)';
                  END SETUP;
                END ADMIN_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "package-body EXECUTE IMMEDIATE CREATE TABLE");
        SubprogramInfo proc = findSub(result, "SETUP");
        assertNotNull(proc);
        TableOperationInfo op = firstOp(proc.getTableOperations(), "STAGING_ORDERS");
        assertNotNull(op, "STAGING_ORDERS should be extracted from the dynamic CREATE TABLE");
        assertEquals("DDL", op.getOperation());
        assertFalse(op.getDdl().isEmpty(), "enrichment must have populated DdlInfo");
        assertEquals("CREATE_TABLE", op.getDdl().get(0).getAction());
    }

    /**
     * The regression this fix's own first two attempts hit, pinned directly rather than relying
     * only on {@code JoinLeftTableResolutionTest} (which exercises the same fixture but reads
     * joins a different way) — proves the exact count and, per table, exactly one copy of each
     * join that touches it, not zero and not a duplicate.
     */
    @Test
    @DisplayName("A multi-table JOIN chain enriches to exactly 3 joins, not 12 or 6, across its per-table ops")
    void testPackageBodyMultiTableJoinDoesNotMultiply() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    CURSOR c IS
                      SELECT p.id, p.list_price, pp.price
                      FROM ERP_PRODUCT p
                      JOIN ERP_PRODUCT_PRICE pp ON pp.product_id = p.id
                      JOIN ERP_PRICE_LIST pl ON pl.id = pp.price_list_id
                      JOIN ERP_PRODUCT_CATEGORY pc ON pc.id = p.category_id
                      ORDER BY p.id;
                  BEGIN
                    NULL;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "3-join chain, package-body");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);

        int totalJoins = 0;
        for (TableOperationInfo op : proc.getTableOperations()) {
            totalJoins += op.getJoins().size();
        }
        assertEquals(3, totalJoins, "exactly 3 joins total across all per-table ops, not 12 (unscoped) or 6 (double-counted per side)");
    }
}
