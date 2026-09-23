
package com.plsql.parser.service;
import com.plsqlanalyzer.parser.service.SqlAnalyzer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

/** Sep05-00: does the analyser really produce predicates and joins? Everything else depends on it. */
class Sep05AnalyserCapabilityTest {
    @Test @DisplayName("predicates, joins and INTO are produced for a realistic statement")
    void capabilityExists() {
        var a = new SqlAnalyzer();

        var upd = a.analyze("UPDATE erp_stock_level SET qty_reserved = qty_reserved + :p_qty "
                + "WHERE product_id = :p_id AND warehouse_id = 7", 10);
        System.out.println("  UPDATE  filters=" + upd.getWhereFilters().size()
                + "  cols=" + upd.getColumnUsages().size());
        upd.getWhereFilters().forEach(w -> System.out.println("     predicate: " + w));
        upd.getColumnUsages().forEach(c -> System.out.println("     col: " + c.getColumnName()
                + " usage=" + c.getUsage() + " src=" + c.getValueSource() + " expr=" + c.getExpression()));
        assertFalse(upd.getWhereFilters().isEmpty(), "no predicate produced");

        var sel = a.analyze("SELECT o.order_id, c.customer_name INTO v_id, v_name "
                + "FROM erp_sales_order o JOIN erp_customer c ON c.customer_id = o.customer_id "
                + "WHERE o.status = 'OPEN'", 20);
        System.out.println("  SELECT  joins=" + sel.getJoinInfos().size()
                + "  filters=" + sel.getWhereFilters().size());
        sel.getJoinInfos().forEach(j -> System.out.println("     join: " + j));
        sel.getColumnUsages().forEach(c -> System.out.println("     col: " + c.getColumnName()
                + " qual=" + c.getQualifier() + " into=" + c.getIntoVariable()));
        assertFalse(sel.getJoinInfos().isEmpty(), "no join produced");
    }
}
