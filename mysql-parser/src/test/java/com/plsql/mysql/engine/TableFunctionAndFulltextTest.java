package com.plsql.mysql.engine;

import com.plsql.mysql.model.MySqlCallInfo;
import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * FROM-clause and select-list constructs whose absence cost far more than the construct itself.
 *
 * <p>Four of the five findings here share one failure shape: an unmodelled piece of syntax made
 * the enclosing statement unparseable, the unknown-statement catch-all swallowed it, and every
 * ORDINARY table in the same query disappeared with it. A SELECT joining four real tables that
 * happened to also use JSON_TABLE, LATERAL, PARTITION or MATCH…AGAINST reported none of them.
 *
 * <p>The fifth — {@link #firstFunctionInASelectListIsNotEatenAsAModifier()} — is the dangerous
 * one, and it is the reason this file exists in the shape it does. It produced NO error at all:
 * the query parsed, the FROM table was reported, and only the first select expression was
 * quietly wrong. That is the failure class you cannot find by grepping for errors.
 */
class TableFunctionAndFulltextTest {

    private static final MySqlGrammarEngine ENGINE = new MySqlGrammarEngine();

    private MySqlRoutineInfo parse(String body) {
        return ENGINE.parse(body, "P_TEST", "analyzerdb");
    }

    private List<String> tables(MySqlRoutineInfo r) {
        return r.getTableOps().stream().map(MySqlTableOpInfo::getTableName).toList();
    }

    private List<String> ops(MySqlRoutineInfo r) {
        return r.getTableOps().stream()
                .map(o -> o.getDmlType() + ":" + o.getTableName()).toList();
    }

    private List<String> calls(MySqlRoutineInfo r) {
        return r.getCalls().stream().map(MySqlCallInfo::qualifiedName).toList();
    }

    private void assertClean(MySqlRoutineInfo r) {
        assertTrue(r.getErrors().isEmpty(), "must parse cleanly: " + r.getErrors());
    }

    // ── 1. JSON_TABLE — MySQL's only table function ──────────────────────────────

    @Test
    @DisplayName("JSON_TABLE in FROM no longer takes the query's real tables down with it")
    void jsonTableDoesNotDiscardTheQuery() {
        MySqlRoutineInfo r = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  SELECT jt.id, jt.nm
                  FROM orders o,
                       JSON_TABLE(o.payload, '$.items[*]'
                         COLUMNS (id INT PATH '$.id',
                                  nm VARCHAR(40) PATH '$.name')) AS jt;
                END""");
        assertClean(r);
        // `orders` is the point: before the fix the statement was skipped whole and `orders`
        // — an ordinary base table with nothing JSON about it — was never reported.
        assertTrue(tables(r).stream().anyMatch("orders"::equalsIgnoreCase), ops(r).toString());
        // JSON_TABLE is a table FUNCTION, not a table: it must not become a phantom table op.
        assertFalse(tables(r).stream().anyMatch(t -> t.toUpperCase().contains("JSON_TABLE")),
                "JSON_TABLE must not be recorded as a table: " + ops(r));
    }

    @Test
    @DisplayName("JSON_TABLE's full column spec: FOR ORDINALITY, ON EMPTY/ERROR, NESTED PATH")
    void jsonTableFullColumnSpec() {
        MySqlRoutineInfo r = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  SELECT *
                  FROM JSON_TABLE('[1,2]', '$[*]'
                         COLUMNS (rn FOR ORDINALITY,
                                  v  INT PATH '$' DEFAULT '0' ON EMPTY ERROR ON ERROR,
                                  ex INT EXISTS PATH '$.flag',
                                  NESTED PATH '$.sub[*]'
                                    COLUMNS (s VARCHAR(10) PATH '$.s'))) jt
                  JOIN t ON t.id = jt.v;
                END""");
        assertClean(r);
        assertTrue(tables(r).stream().anyMatch("t"::equalsIgnoreCase),
                "the JOINed real table must survive: " + ops(r));
    }

    @Test
    @DisplayName("`path`, `columns`, `nested`, `error`, `empty`, `ordinality` stay usable as identifiers")
    void jsonTableKeywordsRemainIdentifiers() {
        // Every keyword the JSON_TABLE spec needed is non-reserved in MySQL, so all of them
        // must still lex as ordinary column names — otherwise supporting one construct
        // regresses bodies that parsed before it existed.
        MySqlRoutineInfo r = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  UPDATE t SET path = 1, columns = 2, nested = 3,
                               error = 4, empty = 5, ordinality = 6
                  WHERE id = 7;
                END""");
        assertClean(r);
        assertEquals(List.of("UPDATE:t"), ops(r));
    }

    // ── 2. LATERAL derived tables (MySQL 8.0.14+) ────────────────────────────────

    @Test
    @DisplayName("LATERAL derived tables, comma-form and JOIN-form")
    void lateralDerivedTables() {
        MySqlRoutineInfo comma = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  SELECT a.id, d.total
                  FROM accounts a,
                       LATERAL (SELECT SUM(amt) AS total FROM payments p
                                WHERE p.acct = a.id) AS d;
                END""");
        assertClean(comma);
        // Both the outer base table and the lateral subquery's own table must appear.
        assertTrue(tables(comma).stream().anyMatch("accounts"::equalsIgnoreCase), ops(comma).toString());
        assertTrue(tables(comma).stream().anyMatch("payments"::equalsIgnoreCase), ops(comma).toString());

        MySqlRoutineInfo joined = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  SELECT * FROM accounts a
                    LEFT JOIN LATERAL (SELECT 1 AS x FROM payments p
                                       WHERE p.acct = a.id) d ON TRUE;
                END""");
        assertClean(joined);
        assertEquals(ops(comma), ops(joined),
                "the JOIN spelling must report exactly what the comma spelling does");
    }

    // ── 3. CTEs combined with window functions (already working — pinned) ────────

    @Test
    @DisplayName("a recursive CTE and a window function in the same query (regression pin)")
    void cteWithWindowFunction() {
        MySqlRoutineInfo r = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  WITH RECURSIVE tree (id, pid, lvl) AS (
                    SELECT id, pid, 0 FROM nodes WHERE pid IS NULL
                    UNION ALL
                    SELECT n.id, n.pid, t.lvl + 1 FROM nodes n JOIN tree t ON n.pid = t.id
                  )
                  SELECT id, SUM(lvl) OVER w AS s
                  FROM tree WINDOW w AS (PARTITION BY lvl);
                END""");
        assertClean(r);
        assertTrue(tables(r).stream().anyMatch("nodes"::equalsIgnoreCase), ops(r).toString());
        // `tree` is the CTE, not a base table — the phantom-table guard must still hold with a
        // window function in the mix.
        assertFalse(tables(r).stream().anyMatch("tree"::equalsIgnoreCase),
                "CTE name leaked into the table inventory: " + ops(r));
    }

    // ── 4. The silent one: a function name eaten as a select modifier ────────────

    @Test
    @DisplayName("the FIRST function in a select list is not eaten as a SELECT modifier")
    void firstFunctionInASelectListIsNotEatenAsAModifier() {
        // selectModifier used to end in a catch-all `ID`, and a function NAME could match it:
        // `SELECT fa(1), fb(2) FROM t` parsed as modifier `fa`, item `(1)`, item `fb(2)`.
        // No syntax error, table still reported — and `fa` missing from the call graph. Only
        // the second and later functions of any select list were ever recorded.
        MySqlRoutineInfo r = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  SELECT calc_total(o.id), other_fn(o.id)
                  FROM orders o
                  WHERE fn_is_active(o.id) = 1;
                END""");
        assertClean(r);
        assertTrue(calls(r).stream().anyMatch("calc_total"::equalsIgnoreCase),
                "the first select-list function was lost from the call graph: " + calls(r));
        assertTrue(calls(r).stream().anyMatch("other_fn"::equalsIgnoreCase), calls(r).toString());
        assertTrue(calls(r).stream().anyMatch("fn_is_active"::equalsIgnoreCase), calls(r).toString());
    }

    @Test
    @DisplayName("the real SELECT modifiers still parse, and the non-reserved ones stay identifiers")
    void selectModifiersStillParse() {
        MySqlRoutineInfo r = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  SELECT SQL_CALC_FOUND_ROWS DISTINCT a FROM t;
                  SELECT SQL_NO_CACHE SQL_BIG_RESULT a FROM t2;
                  SELECT STRAIGHT_JOIN a FROM t3;
                  UPDATE t4 SET sql_no_cache = 1, sql_buffer_result = 2 WHERE id = 3;
                END""");
        assertClean(r);
        assertEquals(List.of("SELECT:t", "SELECT:t2", "SELECT:t3", "UPDATE:t4"), ops(r));
    }

    // ── 5. Explicit partition selection ──────────────────────────────────────────

    @Test
    @DisplayName("PARTITION (p0, p1) on SELECT / INSERT / UPDATE / DELETE")
    void partitionSelection() {
        MySqlRoutineInfo r = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  SELECT * FROM sales PARTITION (p0, p1) s WHERE s.amt > 0;
                  INSERT INTO sales PARTITION (p0) (id) VALUES (1);
                  UPDATE sales PARTITION (p0) SET amt = 1 WHERE id = 3;
                  DELETE FROM sales PARTITION (p1) WHERE id = 2;
                END""");
        assertClean(r);
        // Partition selection narrows which partitions of ONE table are touched; the table
        // identity is unchanged, so all four ops must land on `sales` and the partition names
        // must not appear as tables of their own.
        assertEquals(List.of("SELECT:sales", "INSERT:sales", "UPDATE:sales", "DELETE:sales"), ops(r));
        assertFalse(tables(r).stream().anyMatch(t -> t.equalsIgnoreCase("p0") || t.equalsIgnoreCase("p1")),
                "a partition name became a phantom table: " + ops(r));
    }

    // ── 6. Fulltext search ───────────────────────────────────────────────────────

    @Test
    @DisplayName("MATCH … AGAINST parses, and SQL-looking text inside the search string is inert")
    void fulltextSearch() {
        MySqlRoutineInfo r = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  SELECT id, MATCH(body) AGAINST ('x') AS score
                  FROM docs
                  WHERE MATCH(title, body)
                        AGAINST ('+select * from secret_table' IN BOOLEAN MODE);
                  UPDATE docs SET hit = 1
                  WHERE MATCH(body) AGAINST ('q' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION);
                END""");
        assertClean(r);
        assertEquals(List.of("SELECT:docs", "UPDATE:docs"), ops(r));
        // Two phantoms to rule out. The search string is a literal and is never scanned, so
        // `secret_table` must not become a table…
        assertFalse(tables(r).stream().anyMatch(t -> t.toUpperCase().contains("SECRET")),
                "text inside the AGAINST literal produced a phantom table: " + ops(r));
        // …and MATCH is fulltext syntax, not a user routine, so it must not become a call edge
        // (it did before, when `MATCH(...)` parsed as an ordinary function call).
        assertFalse(calls(r).stream().anyMatch("MATCH"::equalsIgnoreCase),
                "MATCH was reported as a user-routine call: " + calls(r));
    }

    // ── 7. Spatial functions (already working — pinned) ──────────────────────────

    @Test
    @DisplayName("spatial functions survive unusual formatting, and WKT text stays inert")
    void spatialFunctions() {
        MySqlRoutineInfo r = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  SELECT ST_Distance_Sphere ( geo , ST_GeomFromText('POINT(1 2)', 4326) )
                  FROM places
                  WHERE ST_Contains(
                            region ,
                            ST_Buffer( geo
                                     , 100 ) );
                END""");
        assertClean(r);
        assertEquals(List.of("SELECT:places"), ops(r));
        // ST_Distance_Sphere is the leading select-list function, so this also pins the
        // select-modifier fix above against the spatial spelling that first exposed it.
        assertTrue(calls(r).stream().anyMatch("ST_Distance_Sphere"::equalsIgnoreCase), calls(r).toString());
        assertTrue(calls(r).stream().anyMatch("ST_Contains"::equalsIgnoreCase), calls(r).toString());
    }
}
