package com.plsqlanalyzer.parser.service;

import com.plsqlanalyzer.parser.model.ColumnUsage;
import com.plsqlanalyzer.parser.model.SqlAnalysisResult;
import com.plsqlanalyzer.parser.model.SqlOperationType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Column-level statement detail: what is read, what is written, and where written values come from.
 *
 * <p>Before this, a statement recorded only its tables, its WHERE filters and its joins — so
 * "UPDATE touches ORDERS" was the whole answer, with no way to say which columns changed or what
 * they were set from. DDL was not detected at all, meaning a procedure that ALTERed or TRUNCATEd a
 * table left no trace on that table.
 */
class SqlAnalyzerColumnDetailTest {

    private final SqlAnalyzer analyzer = new SqlAnalyzer();

    private List<ColumnUsage> usages(SqlAnalysisResult r, String usage) {
        return r.getColumnUsages().stream()
                .filter(u -> usage.equals(u.getUsage())).collect(Collectors.toList());
    }

    @Test
    @DisplayName("SELECT records the selected columns and their aliases")
    void selectColumns() {
        var r = analyzer.analyze(
                "SELECT o.id, o.total AS amt FROM orders o WHERE o.status = 'NEW'", 10);
        var sel = usages(r, "SELECT");
        assertEquals(2, sel.size());
        assertEquals("id", sel.get(0).getColumnName());
        assertEquals("o", sel.get(0).getQualifier());
        assertEquals("amt", sel.get(1).getAlias());
        // predicates were already captured; they must not regress
        assertEquals(1, r.getWhereFilters().size());
    }

    @Test
    @DisplayName("SELECT INTO records the PL/SQL variable each column is read into")
    void selectIntoVariables() {
        // The Oracle procedural form. JSqlParser parses it and exposes the targets, which is why
        // this can come from the AST rather than a regex over the text.
        var r = analyzer.analyze("SELECT total, status INTO v_total, v_status FROM orders WHERE id = :p_id", 20);
        var sel = usages(r, "SELECT");
        assertEquals(2, sel.size());
        assertEquals("v_total", sel.get(0).getIntoVariable(), "INTO targets are positional");
        assertEquals("v_status", sel.get(1).getIntoVariable());
    }

    @Test
    @DisplayName("UPDATE records each SET column and where its value came from")
    void updateSetProvenance() {
        var r = analyzer.analyze(
                "UPDATE orders SET status = 'X', total = total + :p_amt, ref = seq_r.NEXTVAL "
                        + "WHERE id = :p_id", 30);
        var sets = usages(r, "SET");
        assertEquals(3, sets.size());
        assertEquals("status", sets.get(0).getColumnName());
        assertEquals("LITERAL", sets.get(0).getValueSource());
        assertEquals("total + :p_amt", sets.get(1).getExpression());
        assertEquals("EXPRESSION", sets.get(1).getValueSource(),
                "an expression over a column and a bind is the interesting provenance case");
        assertEquals("SEQUENCE", sets.get(2).getValueSource());
    }

    @Test
    @DisplayName("INSERT pairs each column with the value in its position")
    void insertColumnValues() {
        var r = analyzer.analyze(
                "INSERT INTO orders (id, status, created) VALUES (seq_o.NEXTVAL, :p_status, SYSDATE)", 40);
        var ins = usages(r, "INSERT");
        assertEquals(3, ins.size());
        assertEquals("id", ins.get(0).getColumnName());
        assertEquals("SEQUENCE", ins.get(0).getValueSource());
        assertEquals("BIND", ins.get(1).getValueSource());
        assertEquals("status", ins.get(1).getColumnName());
    }

    @Test
    @DisplayName("INSERT ... SELECT marks the column source as a subquery")
    void insertFromSelect() {
        var r = analyzer.analyze(
                "INSERT INTO archive (id, total) SELECT id, total FROM orders WHERE status = 'OLD'", 50);
        var ins = usages(r, "INSERT");
        assertEquals(2, ins.size());
        assertEquals("SUBQUERY", ins.get(0).getValueSource());
        // and the SELECT side is captured too, so the source table is not lost
        assertFalse(usages(r, "SELECT").isEmpty());
        assertTrue(r.getTableReferences().stream()
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName())));
    }

    @Test
    @DisplayName("DELETE keeps its predicates")
    void deletePredicates() {
        var r = analyzer.analyze("DELETE FROM orders WHERE status = 'OLD' AND created < :p_cut", 60);
        assertEquals(SqlOperationType.DELETE, r.getOperationType());
        assertEquals(2, r.getWhereFilters().size());
    }

    @Test
    @DisplayName("a multi-table join keeps every ON predicate")
    void joins() {
        var r = analyzer.analyze(
                "SELECT 1 FROM a JOIN b ON b.id = a.bid LEFT JOIN c ON c.id = b.cid", 70);
        assertEquals(2, r.getJoinInfos().size());
        assertEquals("b.id = a.bid", r.getJoinInfos().get(0).getOnPredicate());
    }

    @Test
    @DisplayName("DDL is detected, with the object it acts on")
    void ddlDetected() {
        for (String[] c : new String[][]{
                {"TRUNCATE TABLE staging_orders", "TRUNCATE", "STAGING_ORDERS"},
                {"ALTER TABLE orders ADD (note VARCHAR2(50))", "ALTER_TABLE", "ORDERS"},
                {"DROP TABLE tmp_orders", "DROP", "TMP_ORDERS"}}) {
            var r = analyzer.analyze(c[0], 80);
            assertEquals(1, r.getDdl().size(), c[0]);
            assertEquals(c[1], r.getDdl().get(0).getAction(), c[0]);
            assertEquals(c[2], r.getDdl().get(0).getObjectName(), c[0]);
            assertEquals(SqlOperationType.DDL, r.getOperationType(), c[0]);
            // the table must also appear as a reference, so it shows on that table's page
            assertTrue(r.getTableReferences().stream()
                    .anyMatch(t -> c[2].equals(t.getTableName())), c[0]);
        }
    }

    @Test
    @DisplayName("unparseable SQL degrades quietly and keeps the raw text")
    void unparseableIsNotFatal() {
        // PL/SQL is not plain SQL; plenty of statements will not parse, and losing the analysis
        // for the rest of a procedure because one line is exotic would be far worse.
        var r = analyzer.analyze("EXECUTE IMMEDIATE 'begin ' || v_proc || '; end;'", 90);
        assertNotNull(r);
        assertEquals("EXECUTE IMMEDIATE 'begin ' || v_proc || '; end;'", r.getRawSql());
        assertTrue(r.getColumnUsages().isEmpty());
        assertTrue(r.getDdl().isEmpty());
    }
}
