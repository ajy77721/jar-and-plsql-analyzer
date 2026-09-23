package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves the reported ERP_USP_RUN_CYCLE bug is fixed: a 3-JOIN query must yield
 * exactly 3 joins with the correct left/right tables (resolved from ON-condition
 * aliases, NOT defaulted to the FROM base table), join type, and condition.
 */
@DisplayName("JOIN left-table resolution from ON-condition aliases")
public class JoinLeftTableResolutionTest extends ParserTestBase {

    private List<JoinInfo> allJoins(SubprogramInfo proc) {
        List<JoinInfo> joins = new ArrayList<>();
        for (TableOperationInfo t : proc.getTableOperations()) {
            joins.addAll(t.getJoins());
        }
        return joins;
    }

    private JoinInfo find(List<JoinInfo> joins, String joinedTable) {
        return joins.stream()
                .filter(j -> j.getJoinedTable() != null
                        && j.getJoinedTable().toUpperCase().endsWith(joinedTable))
                .findFirst().orElse(null);
    }

    @Test
    @DisplayName("Three explicit joins resolve correct left/right/type/condition")
    void testThreeJoinChain() {
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
        assertNoParsErrors(result, "3-join chain");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);

        List<JoinInfo> joins = allJoins(proc);
        assertEquals(3, joins.size(), "Should capture exactly 3 joins, got: " + joins.size());

        // Join 1: ERP_PRODUCT_PRICE (pp) <-> ERP_PRODUCT (p)
        JoinInfo j1 = find(joins, "ERP_PRODUCT_PRICE");
        assertNotNull(j1, "Missing ERP_PRODUCT_PRICE join (problem #2)");
        assertEquals("ERP_PRODUCT", j1.getLeftTable());
        assertTrueContains(j1.getCondition(), "product_id");
        assertNotNull(j1.getJoinType());

        // Join 2: ERP_PRICE_LIST (pl) <-> ERP_PRODUCT_PRICE (pp) -- NOT the base table
        JoinInfo j2 = find(joins, "ERP_PRICE_LIST");
        assertNotNull(j2);
        assertEquals("ERP_PRODUCT_PRICE", j2.getLeftTable(),
                "Left of ERP_PRICE_LIST join must be ERP_PRODUCT_PRICE, not the FROM base (problem #1)");

        // Join 3: ERP_PRODUCT_CATEGORY (pc) <-> ERP_PRODUCT (p)
        JoinInfo j3 = find(joins, "ERP_PRODUCT_CATEGORY");
        assertNotNull(j3);
        assertEquals("ERP_PRODUCT", j3.getLeftTable());

        // All joins share the same queryId (same statement)
        assertTrue(j1.getQueryId() > 0);
        assertEquals(j1.getQueryId(), j2.getQueryId());
        assertEquals(j1.getQueryId(), j3.getQueryId());
    }

    private void assertTrueContains(String haystack, String needle) {
        assertNotNull(haystack, "condition should not be null");
        assertTrue(haystack.toLowerCase().contains(needle.toLowerCase()),
                "condition '" + haystack + "' should contain '" + needle + "'");
    }
}
