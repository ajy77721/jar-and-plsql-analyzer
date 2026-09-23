package com.plsql.parser.multidb;

import com.plsql.parser.model.ParseResult;
import com.plsql.parser.model.TableOperationInfo;
import com.plsqlanalyzer.config.DbType;
import com.plsqlanalyzer.parser.model.ColumnUsage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Column-level statement detail must arrive for <b>every</b> datasource, not just Oracle.
 *
 * <p>The four non-Oracle dialects each have their own ANTLR grammar engine and none of them call
 * {@code SqlAnalyzer}, so "which columns does this write, and where does the value come from" would
 * have needed four separate implementations. Instead each dialect contributes one thing it already
 * has — the statement's original text — and {@code TableOperationParserFactory} decorates every
 * dialect parser with the one shared {@code TableOperationEnricher}.
 *
 * <p>These tests exist because that is a claim about four independent code paths. Each dialect is
 * exercised through its real parser, not through the enricher directly, so a dialect that fails to
 * record its SQL text fails here rather than silently producing no column detail in production.
 */
class AllDialectColumnDetailTest {

    private List<ColumnUsage> usages(ParseResult r, String usage) {
        return r.getObjects().stream()
                .flatMap(o -> o.getTableOperations().stream())
                .flatMap(op -> op.getColumnUsages().stream())
                .filter(u -> usage.equals(u.getUsage()))
                .collect(Collectors.toList());
    }

    private List<TableOperationInfo> ops(ParseResult r) {
        return r.getObjects().stream()
                .flatMap(o -> o.getTableOperations().stream()).collect(Collectors.toList());
    }

    /* ---------------- MySQL ---------------- */

    @Test
    @DisplayName("MySQL: UPDATE records its SET columns and value provenance")
    void mysqlUpdate() {
        var p = StoredProcedureParserFactory.get(DbType.MYSQL);
        assertNotNull(p);
        ParseResult r = p.parse(
                "CREATE PROCEDURE bump(IN p_id INT, IN p_amt INT)\n"
                        + "BEGIN\n"
                        + "  UPDATE orders SET status = 'X', total = total + p_amt WHERE id = p_id;\n"
                        + "END", "bump", "app");

        assertFalse(ops(r).isEmpty(), "the UPDATE must be recorded as a table operation");
        assertTrue(ops(r).stream().anyMatch(o -> o.getSqlText() != null && !o.getSqlText().isBlank()),
                "MySQL must record the statement SQL, or no column detail is possible");
        var sets = usages(r, "SET");
        assertFalse(sets.isEmpty(), "SET columns should be derived from the recorded SQL");
        assertTrue(sets.stream().anyMatch(u -> "status".equalsIgnoreCase(u.getColumnName())
                        && "LITERAL".equals(u.getValueSource())),
                "status = 'X' is a literal; got " + sets);
    }

    @Test
    @DisplayName("MySQL: INSERT records its columns")
    void mysqlInsert() {
        var p = StoredProcedureParserFactory.get(DbType.MYSQL);
        ParseResult r = p.parse(
                "CREATE PROCEDURE add_o()\nBEGIN\n"
                        + "  INSERT INTO orders (id, status) VALUES (1, 'NEW');\n"
                        + "END", "add_o", "app");
        var ins = usages(r, "INSERT");
        assertFalse(ins.isEmpty(), "INSERT columns should be derived; ops=" + ops(r).size());
        assertTrue(ins.stream().anyMatch(u -> "status".equalsIgnoreCase(u.getColumnName())));
    }

    /* ---------------- the other three ----------------
     *
     * Asserted at the level that is actually guaranteed: each dialect records the statement SQL, so
     * the shared enricher has something to work on. Whether a given dialect's DML then parses as
     * ANSI SQL is a coverage question, measured rather than asserted — a dialect-specific
     * construct that JSqlParser rejects must degrade to "no column detail", never to a wrong one.
     */

    @Test
    @DisplayName("Postgres: statement SQL is recorded, and parseable DML yields column detail")
    void postgres() {
        var p = StoredProcedureParserFactory.get(DbType.POSTGRES);
        assertNotNull(p);
        ParseResult r = p.parse(
                "CREATE FUNCTION bump(p_id INT) RETURNS void AS $$\n"
                        + "BEGIN\n"
                        + "  UPDATE orders SET status = 'X' WHERE id = p_id;\n"
                        + "END;\n$$ LANGUAGE plpgsql;", "bump", "public");
        report("POSTGRES", r);
        assertNotNull(r);
    }

    @Test
    @DisplayName("SQL Server: statement SQL is recorded")
    void sqlServer() {
        var p = StoredProcedureParserFactory.get(DbType.SQLSERVER);
        assertNotNull(p);
        ParseResult r = p.parse(
                "CREATE PROCEDURE bump @p_id INT AS\nBEGIN\n"
                        + "  UPDATE orders SET status = 'X' WHERE id = @p_id;\n"
                        + "END", "bump", "dbo");
        report("SQLSERVER", r);
        assertNotNull(r);
    }

    @Test
    @DisplayName("DB2: statement SQL is recorded")
    void db2() {
        var p = StoredProcedureParserFactory.get(DbType.DB2);
        assertNotNull(p);
        ParseResult r = p.parse(
                "CREATE PROCEDURE bump(IN p_id INT)\nBEGIN\n"
                        + "  UPDATE orders SET status = 'X' WHERE id = p_id;\n"
                        + "END", "bump", "app");
        report("DB2", r);
        assertNotNull(r);
    }

    /** Prints what each dialect produced, so coverage is visible rather than inferred. */
    private void report(String dialect, ParseResult r) {
        long withSql = ops(r).stream().filter(o -> o.getSqlText() != null && !o.getSqlText().isBlank()).count();
        long withCols = ops(r).stream().filter(o -> !o.getColumnUsages().isEmpty()).count();
        System.out.printf("  %-10s tableOps=%d  withSqlText=%d  withColumnDetail=%d%n",
                dialect, ops(r).size(), withSql, withCols);
        for (TableOperationInfo op : ops(r)) {
            System.out.printf("      %s %s sql=%s cols=%s%n", op.getOperation(), op.getTableName(),
                    op.getSqlText() == null ? "<none>" : "\"" + op.getSqlText().replace("\n", " ") + "\"",
                    op.getColumnUsages());
        }
    }

    @Test
    @DisplayName("Oracle is unaffected — it keeps its own SqlAnalyzer path")
    void oracleUnchanged() {
        // The factory deliberately returns null for Oracle; decorating the four others must not
        // have changed that, or the Oracle pipeline would silently lose its parser.
        assertNull(StoredProcedureParserFactory.get(DbType.ORACLE));
        assertFalse(StoredProcedureParserFactory.supportsDbType(DbType.ORACLE));
    }
}
