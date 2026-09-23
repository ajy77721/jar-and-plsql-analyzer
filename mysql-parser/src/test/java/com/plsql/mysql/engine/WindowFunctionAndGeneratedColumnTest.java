package com.plsql.mysql.engine;

import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Window functions and generated columns — two constructs whose failure mode was, once again,
 * not "the construct is not reported" but "the statement containing it was silently discarded".
 *
 * <p>The window-function finding is the sharpest one in this file, because it looked fine from a
 * distance. {@code ROW_NUMBER() OVER (PARTITION BY g)} parsed; {@code SUM(x) OVER (PARTITION BY
 * g)} did not. A window function with no arguments worked and the same function <i>with</i> an
 * argument did not — which is to say every SUM/AVG/LAG/LEAD OVER in the estate was being dropped
 * while the spot-check everyone would reach for (ROW_NUMBER) passed.
 *
 * <p>The cause was not in the window grammar, which accepted all of these when
 * {@code selectStatement} was parsed directly. It was {@code unknownStatement}: because
 * {@code selectStatement} takes an OPTIONAL trailing semicolon, "end the SELECT early at
 * {@code SUM(x)} and start a new statement at {@code (PARTITION BY g) ...}" was a viable parse of
 * the body, and the catch-all made it viable by being willing to begin a statement with
 * {@code (}. No real MySQL statement starts with {@code (} or with {@code OVER}, so excluding
 * those two from the catch-all's first set removes the rival parse and the intended one stands.
 */
class WindowFunctionAndGeneratedColumnTest {

    private static final MySqlGrammarEngine ENGINE = new MySqlGrammarEngine();

    private MySqlRoutineInfo parse(String body) {
        return ENGINE.parse(body, "P_TEST", "analyzerdb");
    }

    private List<String> tables(MySqlRoutineInfo r) {
        return r.getTableOps().stream().map(MySqlTableOpInfo::getTableName).toList();
    }

    private void assertQuerySurvives(String select) {
        MySqlRoutineInfo r = parse("CREATE PROCEDURE p() BEGIN " + select + " END");
        assertTrue(r.getErrors().isEmpty(), select + "\n  -> " + r.getErrors());
        assertTrue(tables(r).stream().anyMatch("t"::equalsIgnoreCase),
                select + "\n  -> lost the FROM table; got " + tables(r));
    }

    @Test
    @DisplayName("a window function WITH arguments is not dropped (the ROW_NUMBER-passes trap)")
    void windowFunctionWithArgumentsSurvives() {
        // These four differ only in whether the function takes an argument. Before the fix the
        // first two passed and the last two lost the entire statement.
        assertQuerySurvives("SELECT ROW_NUMBER() OVER (PARTITION BY g) AS r FROM t;");
        assertQuerySurvives("SELECT COUNT(*)     OVER (PARTITION BY g) AS r FROM t;");
        assertQuerySurvives("SELECT SUM(x)       OVER (PARTITION BY g) AS r FROM t;");
        assertQuerySurvives("SELECT LAG(x)       OVER (ORDER BY id)    AS r FROM t;");
    }

    @Test
    @DisplayName("named windows: `OVER w` and the `WINDOW w AS (...)` clause that defines it")
    void namedWindowClauseSurvives() {
        // `OVER w` was accepted by overClause, but WINDOW and OVER were both listed as usable
        // identifiers, so `SUM(x) OVER w` parsed as the column SUM(x) ALIASED "OVER", and
        // `FROM t WINDOW w AS (...)` parsed as the table t ALIASED "WINDOW". Both are reserved
        // words in MySQL 8.0, so neither spelling was ever legal input to begin with.
        MySqlRoutineInfo r = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  SELECT SUM(x) OVER w AS s,
                         AVG(x) OVER (ORDER BY id
                                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS a
                  FROM t WINDOW w AS (PARTITION BY g ORDER BY id);
                END""");
        assertTrue(r.getErrors().isEmpty(), "named window query must parse: " + r.getErrors());
        assertTrue(tables(r).stream().anyMatch("t"::equalsIgnoreCase),
                "lost the FROM table; got " + tables(r));
    }

    @Test
    @DisplayName("a generated column does not cost us the whole CREATE TABLE")
    void generatedColumnDoesNotDiscardTheTable() {
        // Only one column of four is generated, but the column definition did not match, so the
        // whole CREATE TABLE fell into the catch-all and the table went unrecorded entirely.
        // Both spellings MySQL accepts are covered: the full GENERATED ALWAYS AS (...) STORED and
        // the short AS (...) VIRTUAL, here with the expression split across lines.
        MySqlRoutineInfo withGenerated = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  CREATE TABLE gt (
                    a INT,
                    b INT,
                    c INT GENERATED ALWAYS AS
                         (
                            a + b
                         ) STORED,
                    d VARCHAR(10) AS (CONCAT(a,'x')) VIRTUAL
                  );
                  INSERT INTO gt (a,b) VALUES (1,2);
                END""");
        MySqlRoutineInfo without = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  CREATE TABLE gt (a INT, b INT);
                  INSERT INTO gt (a,b) VALUES (1,2);
                END""");

        assertTrue(withGenerated.getErrors().isEmpty(),
                "generated columns must parse: " + withGenerated.getErrors());
        // THE assertion: the generated column must cost us nothing the plain table did not.
        assertEquals(opKinds(without), opKinds(withGenerated),
                "a generated column changed what the CREATE TABLE reported");
        assertTrue(opKinds(withGenerated).contains("CREATE:gt"), opKinds(withGenerated).toString());
    }

    private List<String> opKinds(MySqlRoutineInfo r) {
        return r.getTableOps().stream()
                .map(o -> o.getDmlType() + ":" + o.getTableName())
                .toList();
    }

    @Test
    @DisplayName("identifiers named `stored`, `virtual`, `always`, `generated` still work")
    void newlyTokenizedKeywordsRemainUsableAsIdentifiers() {
        // Every keyword added for the generated-column syntax is non-reserved in MySQL, so all of
        // them must stay legal as ordinary column names — otherwise supporting one construct
        // regresses bodies that parsed before.
        MySqlRoutineInfo r = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  UPDATE t SET stored = 1, virtual = 2, always = 3, generated = 4 WHERE id = 5;
                END""");
        assertTrue(r.getErrors().isEmpty(), r.getErrors().toString());
        assertTrue(tables(r).stream().anyMatch("t"::equalsIgnoreCase), tables(r).toString());
    }
}
