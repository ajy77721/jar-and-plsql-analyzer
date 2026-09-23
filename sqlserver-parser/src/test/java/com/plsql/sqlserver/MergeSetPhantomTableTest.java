package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sep05-11 — {@code MERGE … WHEN MATCHED THEN UPDATE SET} must not invent a table called SET.
 *
 * <h2>Why a phantom is worse than a gap</h2>
 *
 * <p>MERGE reaches the generic {@code otherStatement} rule, so its tables can only be recovered by
 * scanning text. The scan reads "UPDATE" followed by a word and takes that word as the table — and
 * in the MERGE update clause the next word is {@code SET}. The result is a table in the table list
 * that does not exist, indistinguishable on screen from a real one. A missing table prompts someone
 * to look; an invented one does not.
 *
 * <p>This is the <b>second</b> dialect to show this exact defect — the MySQL MERGE path produced
 * the same {@code SET} phantom earlier — which is why the guard lives at {@code addTableOp}, the one
 * funnel every recovery pattern passes through, rather than in the regex that happened to expose it.
 */
class MergeSetPhantomTableTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private List<String> tableNames(String src) {
        return engine.parse(src, "test_proc", null).getTableOps().stream()
                .map(SqlServerTableOpInfo::getTableName)
                .filter(java.util.Objects::nonNull)
                .map(s -> s.toUpperCase(Locale.ROOT))
                .collect(Collectors.toList());
    }

    private static final String MERGE_PROC =
            "CREATE PROCEDURE usp_Alloc\nAS\nBEGIN\n"
          + "  MERGE erp.Inventory AS tgt\n"
          + "  USING (SELECT ol.ProductId, SUM(ol.Qty) AS Q FROM erp.SalesOrderLine ol\n"
          + "         INNER JOIN erp.SalesOrder o ON o.OrderId = ol.OrderId\n"
          + "         GROUP BY ol.ProductId) AS src\n"
          + "      ON tgt.ProductId = src.ProductId\n"
          + "  WHEN MATCHED THEN UPDATE SET tgt.QtyAllocated = tgt.QtyAllocated + src.Q\n"
          + "  WHEN NOT MATCHED THEN INSERT(ProductId, QtyOnHand) VALUES(src.ProductId, 0);\n"
          + "END";

    @Test
    @DisplayName("the MERGE update clause does not produce a table named SET")
    void noSetPhantom() {
        List<String> tables = tableNames(MERGE_PROC);
        assertFalse(tables.contains("SET"),
                "`UPDATE SET` was read as `UPDATE <table>`; tables=" + tables);
    }

    @Test
    @DisplayName("the real tables of the MERGE are still found")
    void realTablesSurvive() {
        // The guard drops names, so this is the half that matters most: a filter that also removed
        // real tables would trade a visible wrong answer for an invisible missing one.
        List<String> tables = tableNames(MERGE_PROC);
        assertTrue(tables.contains("INVENTORY"), "the MERGE target is missing; tables=" + tables);
        assertTrue(tables.contains("SALESORDERLINE"), "the USING source is missing; tables=" + tables);
        assertTrue(tables.contains("SALESORDER"), "the joined table is missing; tables=" + tables);
    }

    @Test
    @DisplayName("an ordinary UPDATE is unaffected")
    void ordinaryUpdateStillWorks() {
        List<String> tables = tableNames(
                "CREATE PROCEDURE usp_Touch\nAS\nBEGIN\n"
              + "  UPDATE erp.Inventory SET QtyOnHand = 0 WHERE ProductId = 1;\n"
              + "END");
        assertTrue(tables.contains("INVENTORY"), "tables=" + tables);
        assertFalse(tables.contains("SET"), "tables=" + tables);
    }
}
