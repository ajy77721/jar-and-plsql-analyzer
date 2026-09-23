package com.plsqlanalyzer.parser.service;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What JSqlParser can actually tell us, established before building on it.
 *
 * <p>The statement metadata being added (selected columns, INTO variables, SET assignments and
 * their value sources) is only worth modelling for the shapes the parser really exposes. Oracle
 * PL/SQL in particular is not plain SQL — {@code SELECT ... INTO v_x} is a procedural construct —
 * so this pins what works rather than assuming.
 */
class SqlParserCapabilityTest {

    @Test
    @DisplayName("SELECT: the select list is available, with aliases")
    void selectList() throws Exception {
        Statement st = CCJSqlParserUtil.parse(
                "SELECT o.id, o.total AS amt, c.name FROM orders o JOIN customers c ON c.id = o.cust_id");
        PlainSelect p = ((Select) st).getPlainSelect();
        assertEquals(3, p.getSelectItems().size());
        assertEquals("o.id", p.getSelectItems().get(0).getExpression().toString());
        assertEquals("amt", p.getSelectItems().get(1).getAlias().getName());
    }

    @Test
    @DisplayName("UPDATE: SET columns and their value expressions are available")
    void updateSetClause() throws Exception {
        Statement st = CCJSqlParserUtil.parse(
                "UPDATE orders SET status = 'X', total = total + :p_amt WHERE id = :p_id");
        Update u = (Update) st;
        assertEquals(2, u.getUpdateSets().size());
        assertEquals("status", u.getUpdateSets().get(0).getColumns().get(0).getColumnName());
        assertEquals("'X'", u.getUpdateSets().get(0).getValues().get(0).toString());
        // The second is an expression over the column itself — the "where does the value come
        // from" case that matters most.
        assertEquals("total + :p_amt", u.getUpdateSets().get(1).getValues().get(0).toString());
    }

    @Test
    @DisplayName("INSERT: column list and values are available")
    void insertColumnsAndValues() throws Exception {
        Statement st = CCJSqlParserUtil.parse(
                "INSERT INTO orders (id, status, created) VALUES (seq_o.NEXTVAL, 'NEW', SYSDATE)");
        Insert i = (Insert) st;
        assertNotNull(i.getColumns());
        assertEquals(3, i.getColumns().size());
        assertEquals("id", i.getColumns().get(0).getColumnName());
        assertTrue(i.getValues().toString().toUpperCase().contains("NEXTVAL"));
    }

    @Test
    @DisplayName("what happens to Oracle SELECT ... INTO a PL/SQL variable")
    void selectIntoVariable() {
        // The answer decides whether INTO targets can come from the AST or must be recovered
        // from the raw text. Recorded either way so the design is based on fact.
        String sql = "SELECT total INTO v_total FROM orders WHERE id = :p_id";
        try {
            Statement st = CCJSqlParserUtil.parse(sql);
            PlainSelect p = ((Select) st).getPlainSelect();
            System.out.println("SELECT INTO parsed; class=" + st.getClass().getSimpleName());
            System.out.println("  selectItems=" + p.getSelectItems());
            try {
                var m = PlainSelect.class.getMethod("getIntoTables");
                System.out.println("  getIntoTables=" + m.invoke(p));
            } catch (NoSuchMethodException e) {
                System.out.println("  getIntoTables: not present in this JSqlParser version");
            }
        } catch (Exception e) {
            System.out.println("SELECT INTO NOT parseable: " + e.getClass().getSimpleName()
                    + " -> INTO targets must come from the raw text");
        }
    }

    @Test
    @DisplayName("what happens to DDL")
    void ddl() {
        for (String sql : new String[]{
                "CREATE TABLE t (id NUMBER, name VARCHAR2(30))",
                "ALTER TABLE t ADD (col2 NUMBER)",
                "DROP TABLE t",
                "TRUNCATE TABLE t",
                "CREATE INDEX ix_t ON t (id)"}) {
            try {
                Statement st = CCJSqlParserUtil.parse(sql);
                System.out.println("DDL ok: " + st.getClass().getSimpleName() + "  <- " + sql);
            } catch (Exception e) {
                System.out.println("DDL FAILED: " + sql + " -> " + e.getClass().getSimpleName());
            }
        }
    }

    @Test
    @DisplayName("a multi-table join exposes every ON predicate")
    void multiJoin() throws Exception {
        Statement st = CCJSqlParserUtil.parse(
                "SELECT 1 FROM a JOIN b ON b.id = a.bid LEFT JOIN c ON c.id = b.cid "
                        + "INNER JOIN d ON d.k = c.k AND d.active = 1");
        PlainSelect p = ((Select) st).getPlainSelect();
        assertEquals(3, p.getJoins().size());
        assertEquals("d.k = c.k AND d.active = 1",
                p.getJoins().get(2).getOnExpressions().iterator().next().toString());
    }
}
