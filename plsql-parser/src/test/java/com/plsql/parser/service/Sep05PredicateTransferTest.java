package com.plsql.parser.service;

import com.plsql.parser.model.ParsedObject;
import com.plsql.parser.model.TableOperationInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sep05-01 — the predicate must reach the operation.
 *
 * <p>The analyser always produced {@code whereFilters}; the enricher transferred only
 * {@code columnUsages} and {@code ddl}, and {@code TableOperationInfo} had no field to receive
 * them. Every predicate was computed and dropped.
 *
 * <p>That is where the empty column cells came from. A predicate column never becomes a
 * {@code ColumnUsage}:
 *
 * <pre>
 *   SELECT qty_on_hand FROM erp_stock_level WHERE product_id = :p AND warehouse_id = 7
 *     -> 1 columnUsage (qty_on_hand), 2 whereFilters (product_id, warehouse_id)
 * </pre>
 *
 * <p>So for a table used mainly as a join or filter target, all of its column information sat in
 * the discarded collection — {@code ERP_PRODUCT} had 475 accesses and a blank column cell on every
 * row where it was filtered rather than selected. It is also why {@code DELETE} measured 0% column
 * coverage: a DELETE has no select list and no SET.
 */
class Sep05PredicateTransferTest {

    private final TableOperationEnricher enricher = new TableOperationEnricher();

    private ParsedObject objectWith(TableOperationInfo... ops) {
        ParsedObject o = new ParsedObject();
        o.setTableOperations(List.of(ops));
        return o;
    }

    private TableOperationInfo op(String table, String operation, String sql) {
        TableOperationInfo t = new TableOperationInfo();
        t.setTableName(table);
        t.setSchema("APP");
        t.setOperation(operation);
        t.setSqlText(sql);
        t.setLine(10);
        return t;
    }

    @Test
    @DisplayName("a SELECT's predicate reaches the operation, with its column, operator and value")
    void selectPredicateTransferred() {
        var o = objectWith(op("ERP_STOCK_LEVEL", "SELECT",
                "SELECT qty_on_hand FROM erp_stock_level WHERE product_id = :p_id AND warehouse_id = 7"));
        enricher.enrich(List.of(o));

        var t = o.getTableOperations().get(0);
        assertEquals(2, t.getWhereFilters().size(),
                "both predicates must survive: " + t.getWhereFilters());
        // Compared case-insensitively: the analyser upper-cases predicate column names while
        // leaving select-list names as written, so a case-sensitive assertion here would be
        // testing that incidental difference rather than the transfer.
        var cols = t.getWhereFilters().stream()
                .map(w -> w.getColumnName().toUpperCase()).toList();
        assertTrue(cols.contains("PRODUCT_ID"), cols.toString());
        assertTrue(cols.contains("WAREHOUSE_ID"), cols.toString());
        assertEquals("table", t.getPredicateScope(),
                "one table named, so the predicate is provably this table's");
    }

    @Test
    @DisplayName("a DELETE now carries information — previously it had none at all")
    void deletePredicateTransferred() {
        // The measured 0% column coverage for DELETE was not a parser weakness: a DELETE has no
        // select list and no SET, so its only column information is the predicate.
        var o = objectWith(op("ERP_GTT_STAGE", "DELETE",
                "DELETE FROM erp_gtt_stage WHERE run_id = :r"));
        enricher.enrich(List.of(o));

        var t = o.getTableOperations().get(0);
        assertTrue(t.getColumnUsages().isEmpty(), "a DELETE genuinely has no column usages");
        assertEquals(1, t.getWhereFilters().size(),
                "but it does have a predicate, and that is the whole story for this row");
        assertEquals("RUN_ID", t.getWhereFilters().get(0).getColumnName().toUpperCase());
    }

    @Test
    @DisplayName("on a join the predicate is marked statement-scoped, not claimed for one table")
    void joinPredicateIsHonestlyScoped() {
        // WhereFilter carries no qualifier, so a filter cannot be attributed to one side of a join.
        // Saying so is the point: over-claiming here would repeat the defect being fixed.
        var o = objectWith(op("ERP_SALES_ORDER", "SELECT",
                "SELECT o.order_id, c.customer_name FROM erp_sales_order o "
                        + "JOIN erp_customer c ON c.customer_id = o.customer_id WHERE o.status = 'OPEN'"));
        enricher.enrich(List.of(o));

        var t = o.getTableOperations().get(0);
        assertFalse(t.getWhereFilters().isEmpty(), "the predicate must still be shown");
        assertEquals("statement", t.getPredicateScope(),
                "two tables referenced, so the predicate cannot be attributed to one of them");
    }

    @Test
    @DisplayName("an unparseable statement leaves no predicate and does not throw")
    void unparseableIsQuiet() {
        var o = objectWith(op("ORDERS", "UPDATE", "UPDATE orders SET x = MULTISET(...) MODEL RETURN"));
        enricher.enrich(List.of(o));
        var t = o.getTableOperations().get(0);
        assertTrue(t.getWhereFilters().isEmpty());
        assertNull(t.getPredicateScope(), "no predicate means no scope claim either");
    }
}
