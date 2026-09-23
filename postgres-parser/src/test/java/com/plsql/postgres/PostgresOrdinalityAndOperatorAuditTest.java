package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round-3 deep-probe regression cover.
 *
 * <p>Two defect classes were found and fixed:
 * <ol>
 *   <li><b>{@code WITH ORDINALITY}</b> on a set-returning function in FROM. {@code WITH} is both a
 *       real token and a legal {@code id}, so the optional bare table alias matched the
 *       {@code WITH} keyword itself and left {@code ORDINALITY} (and the {@code AS t(v, n)} column
 *       list) unconsumed. The FROM clause therefore ended early and {@code selectRest}'s
 *       permissive catch-all swallowed everything after it — <em>including a following
 *       {@code JOIN other_table ON …}</em>, whose table vanished from the analysis with
 *       {@code errors=[]}. That is SILENT capture loss, the most dangerous class.</li>
 *   <li><b>Un-lexable operator characters.</b> {@code #}, {@code ?}, {@code @@} and {@code !} had no
 *       lexer rule at all, so ANTLR's default error recovery DISCARDED the character and the
 *       operator collapsed into an unrelated one — {@code payload #> '{a}'} became a bare
 *       greater-than, and {@code a !~ 'x'} became the <em>inverted</em> regex match {@code a ~ 'x'}.
 *       Same class as the already-fixed {@code @>} / {@code ->} bugs.</li>
 * </ol>
 *
 * <p>The remaining probes here are "confirmed already working" locks: LATERAL joins, window
 * functions over {@code JOIN … USING}, FROM-like text inside array / range / JSON literals, and
 * {@code EXECUTE format(...) USING ...}. They assert the absence of phantom tables and of silent
 * loss, so a future change cannot regress them unnoticed.
 */
class PostgresOrdinalityAndOperatorAuditTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private PostgresRoutineInfo parse(String body) {
        return engine.parse("CREATE PROCEDURE p() AS $$ BEGIN " + body + " END; $$ LANGUAGE plpgsql;",
                "p", null);
    }

    private List<String> tables(PostgresRoutineInfo r) {
        return r.getTableOps().stream().map(t -> t.getTableName().toUpperCase()).sorted().toList();
    }

    private List<String> dynamicTables(PostgresRoutineInfo r) {
        return r.getTableOps().stream()
                .filter(t -> t.getDmlType() == PostgresTableOpInfo.DmlType.DYNAMIC)
                .map(PostgresTableOpInfo::getTableName)
                .toList();
    }

    // ── 1. WITH ORDINALITY ───────────────────────────────────────────────────

    @Test
    @DisplayName("WITH ORDINALITY no longer swallows a following JOIN (was silent table loss)")
    void ordinalityThenJoinKeepsJoinedTable() {
        PostgresRoutineInfo r = parse(
                "SELECT * FROM unnest(ARRAY[1]) WITH ORDINALITY AS u(v, n) JOIN orders o ON o.id = u.v;");
        assertEquals(List.of("ORDERS"), tables(r), "joined table lost after WITH ORDINALITY");
        assertTrue(r.getErrors().isEmpty(), "errors=" + r.getErrors());
    }

    @Test
    @DisplayName("WITH ORDINALITY without an alias also keeps the following JOIN")
    void ordinalityNoAliasThenJoin() {
        PostgresRoutineInfo r = parse(
                "SELECT * FROM generate_series(1, 3) WITH ORDINALITY JOIN orders o ON true;");
        assertEquals(List.of("ORDERS"), tables(r));
        assertTrue(r.getErrors().isEmpty(), "errors=" + r.getErrors());
    }

    @Test
    @DisplayName("a plain WITH ORDINALITY FROM item is a call, not a phantom table")
    void ordinalityIsNotATable() {
        PostgresRoutineInfo r = parse("SELECT * FROM unnest(ARRAY[1,2]) WITH ORDINALITY AS t(v, n);");
        assertTrue(r.getTableOps().isEmpty(), "phantom table: " + tables(r));
        assertTrue(r.getCalls().stream().anyMatch(c -> "unnest".equalsIgnoreCase(c.getName())));
        assertTrue(r.getErrors().isEmpty(), "errors=" + r.getErrors());
    }

    @Test
    @DisplayName("a CTE's WITH is unaffected by the withOrdinality predicate")
    void cteWithStillParses() {
        PostgresRoutineInfo r = parse(
                "WITH c AS (SELECT * FROM src) SELECT * FROM c JOIN dst d ON d.id = c.id;");
        assertEquals(List.of("DST", "SRC"), tables(r));
        assertTrue(r.getErrors().isEmpty(), "errors=" + r.getErrors());
    }

    // ── 2. Operators that used to lose a character ───────────────────────────

    @Test
    @DisplayName("#> / #>> JSON path operators lex cleanly (were silently collapsed to '>')")
    void jsonPathOperators() {
        PostgresRoutineInfo r = parse("SELECT payload #> '{a,b}' FROM events WHERE payload #>> '{c}' = 'x';");
        assertEquals(List.of("EVENTS"), tables(r));
        assertTrue(r.getErrors().isEmpty(), "errors=" + r.getErrors());
    }

    @Test
    @DisplayName("jsonb key-existence ? / ?| / ?& lex cleanly")
    void jsonbExistenceOperators() {
        for (String op : new String[]{"?", "?|", "?&"}) {
            PostgresRoutineInfo r = parse("SELECT * FROM t1 WHERE j " + op + " ARRAY['a'];");
            assertEquals(List.of("T1"), tables(r), "op=" + op);
            assertTrue(r.getErrors().isEmpty(), "op=" + op + " errors=" + r.getErrors());
        }
    }

    @Test
    @DisplayName("@@ full-text match, # XOR, #- jsonb delete and @ absolute-value lex cleanly")
    void miscOperators() {
        assertTrue(parse("SELECT * FROM docs WHERE tsv @@ to_tsquery('x');").getErrors().isEmpty());
        assertTrue(parse("SELECT a # b FROM t1;").getErrors().isEmpty());
        assertTrue(parse("SELECT j #- '{a}' FROM t1;").getErrors().isEmpty());
        assertTrue(parse("SELECT * FROM t1 WHERE @ x > 1;").getErrors().isEmpty());
    }

    @Test
    @DisplayName("!~ / !~* regex NOT-match lex cleanly (the '!' was dropped, inverting the test)")
    void regexNotMatchOperators() {
        PostgresRoutineInfo r = parse("SELECT * FROM t1 WHERE a !~ 'x' AND b !~* 'y';");
        assertEquals(List.of("T1"), tables(r));
        assertTrue(r.getErrors().isEmpty(), "errors=" + r.getErrors());
    }

    // ── 3. Confirmed-working locks ───────────────────────────────────────────

    @Test
    @DisplayName("LATERAL: both the outer and the correlated inner table are captured, no phantom")
    void lateralJoins() {
        PostgresRoutineInfo r = parse(
                "SELECT a.id, s.n FROM accounts a, LATERAL (SELECT count(*) n FROM ledger b WHERE b.acct = a.id) s;");
        assertEquals(List.of("ACCOUNTS", "LEDGER"), tables(r));
        assertTrue(r.getErrors().isEmpty(), "errors=" + r.getErrors());

        PostgresRoutineInfo r2 = parse(
                "SELECT * FROM accounts a CROSS JOIN LATERAL (SELECT * FROM ledger l WHERE l.acct = a.id) s;");
        assertEquals(List.of("ACCOUNTS", "LEDGER"), tables(r2));
        assertTrue(r2.getErrors().isEmpty(), "errors=" + r2.getErrors());
    }

    @Test
    @DisplayName("window function over JOIN ... USING keeps both tables and the USING condition")
    void windowOverJoinUsing() {
        PostgresRoutineInfo r = parse(
                "SELECT o.id, ROW_NUMBER() OVER (PARTITION BY c.region ORDER BY o.amt DESC) rn "
                        + "FROM orders o JOIN customers c USING (cust_id);");
        assertEquals(List.of("CUSTOMERS", "ORDERS"), tables(r));
        PostgresTableOpInfo base = r.getTableOps().stream()
                .filter(t -> "orders".equalsIgnoreCase(t.getTableName())).findFirst().orElseThrow();
        assertEquals(1, base.getJoins().size());
        assertEquals("USING (cust_id)", base.getJoins().get(0).getCondition());
        assertTrue(r.getErrors().isEmpty(), "errors=" + r.getErrors());
    }

    @Test
    @DisplayName("FROM-like text inside array / range / JSON literals creates no phantom table")
    void noPhantomTableFromLiterals() {
        assertEquals(List.of("REAL_T"),
                tables(parse("v := ARRAY['SELECT * FROM fake']; INSERT INTO real_t VALUES (1);")));
        assertEquals(List.of("REALTAB"),
                tables(parse("SELECT ARRAY['a FROM fake', 'b'] FROM realtab;")));
        assertEquals(List.of("BOOKINGS"),
                tables(parse("SELECT * FROM bookings WHERE span @> '[2020-01-01,2020-02-01)'::daterange;")));
        assertEquals(List.of("EVENTS"),
                tables(parse("SELECT payload->'k' FROM events WHERE payload->>'t' = 'SELECT * FROM fake';")));
    }

    @Test
    @DisplayName("EXECUTE format(...) USING resolves every %I and keeps the USING args out of the template")
    void executeFormatWithUsing() {
        PostgresRoutineInfo r = parse(
                "EXECUTE format('SELECT * FROM %I WHERE id = $1', 'emp') INTO v USING p_id;");
        assertEquals(List.of("emp"), dynamicTables(r));
        assertTrue(r.getErrors().isEmpty(), "errors=" + r.getErrors());

        PostgresRoutineInfo r2 = parse(
                "EXECUTE format('INSERT INTO %I SELECT * FROM %I WHERE k = $1', 'tgt', 'src') USING p_k;");
        assertEquals(List.of("tgt", "src"), dynamicTables(r2));
        assertTrue(r2.getErrors().isEmpty(), "errors=" + r2.getErrors());
    }

    @Test
    @DisplayName("CREATE TABLE ... INHERITS (parent) inside a body loses nothing")
    void createTableInheritsInBody() {
        PostgresRoutineInfo r = parse(
                "CREATE TABLE child_t (extra int) INHERITS (parent_t); INSERT INTO child_t VALUES (1);");
        // This assertion used to read List.of("CHILD_T", "CHILD_T") — the two child ops (the
        // CREATE and the INSERT) — because PARENT_T was swallowed by the CREATE TABLE
        // statement's opaque tail, so the child->parent inheritance edge was lost despite this
        // test's name. `inheritsClause` now records the parent as a SELECT-kind read dependency,
        // the same representation ALTER TABLE ... REFERENCES uses for an FK target.
        // (tables() sorts, hence PARENT_T last.)
        assertEquals(List.of("CHILD_T", "CHILD_T", "PARENT_T"), tables(r));
        assertEquals(List.of("child_t->parent_t"), r.getTableInheritsRefs());
        assertTrue(r.getErrors().isEmpty(), "errors=" + r.getErrors());
    }
}
