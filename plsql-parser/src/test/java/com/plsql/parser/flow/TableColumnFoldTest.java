package com.plsql.parser.flow;

import com.plsql.parser.model.ParsedObject;
import com.plsql.parser.model.SchemaTableInfo;
import com.plsql.parser.model.TableColumnUsage;
import com.plsql.parser.model.TableOperationInfo;
import com.plsql.parser.service.TableOperationEnricher;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What reaches a table's page: one row per column, folded across every statement.
 *
 * <p>The per-statement records are the right grain for a statement and the wrong grain for a table.
 * A table with hundreds of statements against it would otherwise carry hundreds of near-identical
 * rows — the same duplication that had to be pooled out of the JAR analysis document, and
 * unreadable besides. So the fold is the contract, and this pins it.
 */
class TableColumnFoldTest {

    private final TableOperationEnricher enricher = new TableOperationEnricher();

    private TableOperationInfo op(String table, String operation, String sql, int line) {
        TableOperationInfo t = new TableOperationInfo();
        t.setTableName(table);
        t.setSchema("APP");
        t.setOperation(operation);
        t.setSqlText(sql);
        t.setLine(line);
        return t;
    }

    private ParsedObject objectWith(TableOperationInfo... ops) {
        ParsedObject o = new ParsedObject();
        o.setTableOperations(List.of(ops));
        return o;
    }

    @Test
    @DisplayName("the enricher fills column detail from the statement SQL")
    void enricherFillsDetail() {
        var o = objectWith(op("ORDERS", "UPDATE",
                "UPDATE orders SET status = 'X', total = total + :p_amt WHERE id = :p_id", 10));
        var coverage = enricher.enrich(List.of(o));

        assertEquals(1, coverage.attempted());
        assertEquals(1, coverage.parsed());
        var cols = o.getTableOperations().get(0).getColumnUsages();
        assertEquals(2, cols.size(), "two SET columns: " + cols);
        assertEquals("status", cols.get(0).getColumnName());
        assertEquals("LITERAL", cols.get(0).getValueSource());
        assertEquals("total", cols.get(1).getColumnName());
        assertEquals("EXPRESSION", cols.get(1).getValueSource(),
                "total + :p_amt is an expression over a column and a bind");
        assertEquals("total + :p_amt", cols.get(1).getExpression());
    }

    @Test
    @DisplayName("a statement that will not parse yields no column detail, and breaks nothing")
    void unparseableDegradesQuietly() {
        // Dialect SQL contains plenty that ANSI parsing rejects. A wrong column list would show on
        // a table's page as fact, which is worse than an absent one.
        var o = objectWith(op("ORDERS", "UPDATE", "UPDATE orders SET x = MULTISET(...) MODEL RETURN", 5));
        var coverage = enricher.enrich(List.of(o));

        assertEquals(1, coverage.attempted());
        assertEquals(0, coverage.withColumns());
        var t = o.getTableOperations().get(0);
        assertTrue(t.getColumnUsages().isEmpty());
        assertEquals("ORDERS", t.getTableName(), "the operation itself must be untouched");
        assertEquals("UPDATE", t.getOperation());
    }

    @Test
    @DisplayName("coverage() measures without mutating")
    void coverageDoesNotMutate() {
        var o = objectWith(op("ORDERS", "UPDATE", "UPDATE orders SET status = 'X'", 1));
        var c = enricher.coverage(List.of(o));
        assertEquals(1, c.attempted());
        assertEquals(1, c.withColumns());
        assertTrue(o.getTableOperations().get(0).getColumnUsages().isEmpty(),
                "coverage() reports; it must not fill anything in");
    }

    @Test
    @DisplayName("operations with no SQL text are skipped, not counted as failures")
    void noSqlTextIsNotAFailure() {
        // Oracle's own path does not set sqlText on table operations, and analyses stored before
        // this change have none. Counting those as parse failures would make coverage meaningless.
        var o = objectWith(op("ORDERS", "SELECT", null, 1));
        var c = enricher.enrich(List.of(o));
        assertEquals(0, c.attempted());
    }

    /* ---- the fold onto the table page ---- */

    @Test
    @DisplayName("many statements on one table fold to one row per column")
    void foldsPerColumn() {
        // Two procedures, four statements, all touching ORDERS.STATUS — one row expected.
        var enriched = List.of(
                objectWith(op("ORDERS", "UPDATE", "UPDATE orders SET status = 'A' WHERE id = 1", 10),
                           op("ORDERS", "UPDATE", "UPDATE orders SET status = 'B' WHERE id = 2", 20)),
                objectWith(op("ORDERS", "INSERT", "INSERT INTO orders (id, status) VALUES (s.NEXTVAL, 'C')", 30)));
        for (var o : enriched) enricher.enrich(List.of(o));

        SchemaTableInfo info = new SchemaTableInfo();
        info.setSchema("APP");
        info.setTableName("ORDERS");
        // Mirrors DependencyCrawler.foldColumnUsage; asserted here because that method is private
        // and this is the property that matters, not its call site.
        fold(info, enriched.get(0).getTableOperations(), "PROC_A");
        fold(info, enriched.get(1).getTableOperations(), "PROC_B");

        var status = info.getColumns().stream()
                .filter(c -> "STATUS".equals(c.getColumnName())).findFirst().orElse(null);
        assertNotNull(status, "columns=" + info.getColumns());
        assertEquals(1, info.getColumns().stream()
                .filter(c -> "STATUS".equals(c.getColumnName())).count(), "one row per column");
        assertTrue(status.getUsages().containsAll(List.of("SET", "INSERT")),
                "both ways of writing it, on one row: " + status.getUsages());
        assertTrue(status.getReferencedBy().containsAll(List.of("PROC_A", "PROC_B")));

        var id = info.getColumns().stream()
                .filter(c -> "ID".equals(c.getColumnName())).findFirst().orElse(null);
        assertNotNull(id);
        assertTrue(id.getValueSources().contains("SEQUENCE"),
                "the sequence provenance must survive the fold: " + id.getValueSources());
    }

    /* ---- the line cap must report itself ---- */

    @Test
    @DisplayName("past the cap, the true line count survives so the page can say 'showing 25 of N'")
    void cappedLinesStillReportTheirTrueCount() {
        // The cap keeps a heavily-used column from carrying thousands of line numbers. On its own it
        // lies: twenty-five entries look exactly like "used in twenty-five places", so a reader
        // auditing where a column is written stops at the last one and believes they have seen
        // everything. That is a confident, incomplete answer — the shape of defect this project
        // keeps finding.
        TableColumnUsage tc = new TableColumnUsage("STATUS");
        for (int line = 1; line <= 112; line++) tc.addLine(line, 25);

        assertEquals(25, tc.getLines().size(), "the cap must still bound what is stored");
        assertEquals(112, tc.getLineCount(), "the true count must survive the cap");
        assertTrue(tc.isLinesTruncated(), "the page needs to know this is a sample, not the set");
        assertEquals(List.of(1, 2, 3), tc.getLines().subList(0, 3), "the kept lines are the first ones");
    }

    @Test
    @DisplayName("the count is of distinct lines, and is not truncated when it need not be")
    void countIsDistinctAndHonestBelowTheCap() {
        TableColumnUsage tc = new TableColumnUsage("ID");
        tc.addLine(10, 25);
        tc.addLine(10, 25);   // the same line reached twice — one place, not two
        tc.addLine(20, 25);
        tc.addLine(0, 25);    // no line information; must not inflate the count
        tc.addLine(-3, 25);

        assertEquals(2, tc.getLineCount());
        assertEquals(List.of(10, 20), tc.getLines());
        assertFalse(tc.isLinesTruncated(),
                "below the cap nothing was dropped, so the page must not imply otherwise");
    }

    @Test
    @DisplayName("the counting set stays out of the document, and lineCount goes in")
    void bookkeepingDoesNotReachTheDocument() throws Exception {
        // seenLines exists only to keep the count exact past the cap. Serialising it would put every
        // line back into the document — defeating the exact cap it is there to report on, and
        // undoing the pooling work that shrank these documents in the first place.
        TableColumnUsage tc = new TableColumnUsage("STATUS");
        for (int line = 1; line <= 112; line++) tc.addLine(line, 25);

        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        String json = mapper.writeValueAsString(tc);
        assertFalse(json.contains("seenLines"), "internal bookkeeping leaked into the document: " + json);

        // Read the document back rather than matching substrings: "lineCount":112 contains "112,",
        // so a substring check on the lines array would report a leak that is not there.
        var back = mapper.readTree(json);
        assertEquals(112, back.get("lineCount").asInt(), "the true count must reach the UI");
        assertEquals(25, back.get("lines").size(), "the cap must still bound the stored lines");
    }

    /** The same folding rule as DependencyCrawler, kept in step by the assertions above. */
    private void fold(SchemaTableInfo info, List<TableOperationInfo> ops, String referencedBy) {
        for (TableOperationInfo op : ops) {
            for (var u : op.getColumnUsages()) {
                String col = u.getColumnName();
                if (col == null || col.isBlank() || "*".equals(col)) continue;
                String key = col.toUpperCase();
                TableColumnUsage tc = info.getColumns().stream()
                        .filter(c -> key.equalsIgnoreCase(c.getColumnName())).findFirst().orElse(null);
                if (tc == null) { tc = new TableColumnUsage(key); info.getColumns().add(tc); }
                if (u.getUsage() != null) tc.getUsages().add(u.getUsage());
                if (u.getValueSource() != null) tc.getValueSources().add(u.getValueSource());
                if (referencedBy != null) tc.getReferencedBy().add(referencedBy);
            }
        }
    }
}
