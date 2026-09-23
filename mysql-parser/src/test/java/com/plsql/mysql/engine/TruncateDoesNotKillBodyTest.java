package com.plsql.mysql.engine;

import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * One unsupported statement must not discard the statements around it.
 *
 * <p>{@code TRUNCATE TABLE} was missing from {@code routineStatement}. The cost was not that
 * TRUNCATE went unreported — it was that <b>no alternative matched, the enclosing body failed, and
 * the whole procedure yielded nothing</b>. Measured against a real MySQL procedure before the fix:
 *
 * <pre>
 *   ERP_USP_DDL_PROBE     (TRUNCATE + INSERT + UPDATE)   0 tables, 0 operations
 *   ERP_USP_NODDL_PROBE   (INSERT + UPDATE only)         1 table,  [INSERT, UPDATE]
 * </pre>
 *
 * <p>Identical procedures apart from one line. Nothing failed loudly: the node came back with
 * {@code statementSummary: {}} and three lines of code, so the analysis was silently empty rather
 * than visibly broken — the failure shape this project keeps finding.
 *
 * <p>The first test is the one that matters. The second only checks that TRUNCATE is now recorded;
 * the first checks that the statements <i>near</i> it survive, which is the property that was
 * actually lost and the one that would break again if another statement type went missing.
 */
class TruncateDoesNotKillBodyTest {

    private static final MySqlGrammarEngine ENGINE = new MySqlGrammarEngine();

    private MySqlRoutineInfo parse(String body) {
        return ENGINE.parse(body, "P_TEST", "analyzerdb");
    }

    private Set<MySqlTableOpInfo.DmlType> opsOf(MySqlRoutineInfo r) {
        return r.getTableOps().stream()
                .map(MySqlTableOpInfo::getDmlType)
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(MySqlTableOpInfo.DmlType.class)));
    }

    @Test
    @DisplayName("a TRUNCATE does not discard the DML around it")
    void truncateDoesNotSwallowTheBody() {
        MySqlRoutineInfo withTruncate = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  TRUNCATE TABLE staging;
                  INSERT INTO staging (id, note) VALUES (1, 'seeded');
                  UPDATE staging SET note = 'updated' WHERE id = 1;
                END""");
        MySqlRoutineInfo without = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  INSERT INTO staging (id, note) VALUES (1, 'seeded');
                  UPDATE staging SET note = 'updated' WHERE id = 1;
                END""");

        // THE assertion: adding a TRUNCATE must not cost us the INSERT and the UPDATE.
        assertTrue(opsOf(withTruncate).containsAll(opsOf(without)),
                "adding a TRUNCATE lost the surrounding DML: with=" + opsOf(withTruncate)
                        + " without=" + opsOf(without));
        assertTrue(opsOf(withTruncate).contains(MySqlTableOpInfo.DmlType.INSERT));
        assertTrue(opsOf(withTruncate).contains(MySqlTableOpInfo.DmlType.UPDATE));
    }

    @Test
    @DisplayName("TRUNCATE itself is recorded, against the right table")
    void truncateIsRecorded() {
        MySqlRoutineInfo r = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  TRUNCATE TABLE staging;
                END""");
        MySqlTableOpInfo op = r.getTableOps().stream()
                .filter(o -> o.getDmlType() == MySqlTableOpInfo.DmlType.TRUNCATE)
                .findFirst().orElse(null);
        assertNotNull(op, "TRUNCATE must be recorded; got " + opsOf(r));
        assertEquals("staging", op.getTableName().toLowerCase());
    }

    @Test
    @DisplayName("the optional TABLE keyword is accepted either way")
    void tableKeywordOptional() {
        // MySQL accepts both spellings; rejecting one would reintroduce the whole-body failure for
        // any procedure that used it.
        assertTrue(opsOf(parse("CREATE PROCEDURE p() BEGIN TRUNCATE TABLE t; END"))
                .contains(MySqlTableOpInfo.DmlType.TRUNCATE));
        assertTrue(opsOf(parse("CREATE PROCEDURE p() BEGIN TRUNCATE t; END"))
                .contains(MySqlTableOpInfo.DmlType.TRUNCATE));
    }

    @Test
    @DisplayName("the statement carries its SQL, so column detail can be derived")
    void truncateCarriesSqlText() {
        // The shared TableOperationEnricher works from sqlText; without it this operation would
        // reach the table page with no DDL detail even though the grammar now accepts it.
        MySqlRoutineInfo r = parse("""
                CREATE PROCEDURE p()
                BEGIN
                  TRUNCATE TABLE staging;
                END""");
        MySqlTableOpInfo op = r.getTableOps().stream()
                .filter(o -> o.getDmlType() == MySqlTableOpInfo.DmlType.TRUNCATE)
                .findFirst().orElseThrow();
        assertNotNull(op.getSqlText(), "TRUNCATE must record its SQL text");
        assertTrue(op.getSqlText().toUpperCase().contains("TRUNCATE"), op.getSqlText());
    }
}
