package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests covering the gap fixes applied to PlSqlAnalysisVisitor:
 *   Fix 1 — DB link stored in TableOperationInfo.dbLink
 *   Fix 2 — MERGE branch (MATCHED / NOT_MATCHED) stored in TableOperationInfo.mergeClause
 *   Fix 3 — RAISE_APPLICATION_ERROR emits StatementInfo with errorCode + errorMessage
 *   Fix 4 — SAVEPOINT name stored in StatementInfo.savepointName
 *   Fix 5 — Implicit cursor attributes (SQL%ROWCOUNT etc.) emitted as CURSOR_ATTRIBUTE statements
 */
@DisplayName("PL/SQL Parser Gap Fixes")
public class PlSqlGapFixesTest extends ParserTestBase {

    // ----------------------------------------------------------------
    // Fix 1: DB Link in TableOperationInfo
    // ----------------------------------------------------------------
    @Nested
    @DisplayName("Fix 1 — DB Link stored in TableOperationInfo")
    class DbLinkFix {

        @Test
        @DisplayName("Simple table@link captured on TableOperationInfo")
        void tableWithDbLink() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_id NUMBER;
                      BEGIN
                        SELECT id INTO v_id FROM remote_schema.employees@MY_LINK WHERE id = 1;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "table with DB link");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);

            TableOperationInfo toi = proc.getTableOperations().stream()
                    .filter(t -> "EMPLOYEES".equalsIgnoreCase(t.getTableName()))
                    .findFirst().orElse(null);
            assertNotNull(toi, "EMPLOYEES table operation should exist");
            assertNotNull(toi.getDbLink(), "dbLink should not be null");
            assertTrue(toi.getDbLink().toUpperCase().contains("MY_LINK"),
                    "dbLink should contain MY_LINK, got: " + toi.getDbLink());
        }

        @Test
        @DisplayName("DB link also stored in DependencySummary.dbLinks")
        void dbLinkInDependencies() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_id NUMBER;
                      BEGIN
                        SELECT id INTO v_id FROM orders@REMOTE_DB WHERE id = 1;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "db link in dependencies");

            ParsedObject pkg = result.getObjects().stream()
                    .filter(o -> "MY_PKG".equalsIgnoreCase(o.getName()))
                    .findFirst().orElse(null);
            assertNotNull(pkg);
            assertTrue(pkg.getDependencies().getDbLinks().stream()
                    .anyMatch(l -> l.toUpperCase().contains("REMOTE_DB")),
                    "dbLinks dependency should contain REMOTE_DB");
        }

        @Test
        @DisplayName("Table without DB link has null dbLink field")
        void tableWithoutDbLink() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_id NUMBER;
                      BEGIN
                        SELECT id INTO v_id FROM employees WHERE id = 1;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "table without db link");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);
            proc.getTableOperations().forEach(toi ->
                    assertNull(toi.getDbLink(), "dbLink should be null when no @link present"));
        }
    }

    // ----------------------------------------------------------------
    // Fix 2: MERGE branch attribution
    // ----------------------------------------------------------------
    @Nested
    @DisplayName("Fix 2 — MERGE branch stored in TableOperationInfo.mergeClause")
    class MergeBranchFix {

        @Test
        @DisplayName("WHEN MATCHED UPDATE sets mergeClause = MATCHED")
        void mergeMatchedClause() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                      BEGIN
                        MERGE INTO target_table t
                        USING source_table s ON (t.id = s.id)
                        WHEN MATCHED THEN
                          UPDATE SET t.val = s.val
                        WHEN NOT MATCHED THEN
                          INSERT (id, val) VALUES (s.id, s.val);
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "merge matched clause");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);

            boolean hasMatched = proc.getTableOperations().stream()
                    .anyMatch(t -> "MATCHED".equals(t.getMergeClause()));
            assertTrue(hasMatched, "At least one TableOperationInfo should have mergeClause=MATCHED");
        }

        @Test
        @DisplayName("WHEN NOT MATCHED INSERT sets mergeClause = NOT_MATCHED")
        void mergeNotMatchedClause() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                      BEGIN
                        MERGE INTO target_table t
                        USING source_table s ON (t.id = s.id)
                        WHEN MATCHED THEN
                          UPDATE SET t.val = s.val
                        WHEN NOT MATCHED THEN
                          INSERT (id, val) VALUES (s.id, s.val);
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "merge not matched clause");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);

            boolean hasNotMatched = proc.getTableOperations().stream()
                    .anyMatch(t -> "NOT_MATCHED".equals(t.getMergeClause()));
            assertTrue(hasNotMatched, "At least one TableOperationInfo should have mergeClause=NOT_MATCHED");
        }

        @Test
        @DisplayName("Target table in MERGE has no mergeClause (it is the main MERGE target)")
        void mergeTargetTableHasNoClause() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                      BEGIN
                        MERGE INTO only_target t
                        USING source_data s ON (t.id = s.id)
                        WHEN MATCHED THEN
                          UPDATE SET t.col = s.col;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "merge target no clause");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);

            // The main MERGE target table is encountered before entering any branch — no clause
            boolean hasNullClauseEntry = proc.getTableOperations().stream()
                    .anyMatch(t -> t.getMergeClause() == null || t.getMergeClause().isEmpty());
            assertTrue(hasNullClauseEntry,
                    "At least one table operation (target/source) should have no mergeClause");
        }
    }

    // ----------------------------------------------------------------
    // Fix 3: RAISE_APPLICATION_ERROR detection
    // ----------------------------------------------------------------
    @Nested
    @DisplayName("Fix 3 — RAISE_APPLICATION_ERROR captured with errorCode and errorMessage")
    class RaiseApplicationErrorFix {

        @Test
        @DisplayName("RAISE_APPLICATION_ERROR emits RAISE_APPLICATION_ERROR statement")
        void raiseAppErrorBasic() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                      BEGIN
                        RAISE_APPLICATION_ERROR(-20001, 'Something went wrong');
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "raise_application_error basic");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);

            StatementInfo rae = proc.getStatements().stream()
                    .filter(s -> "RAISE_APPLICATION_ERROR".equals(s.getType()))
                    .findFirst().orElse(null);
            assertNotNull(rae, "Should have a RAISE_APPLICATION_ERROR statement");
        }

        @Test
        @DisplayName("RAISE_APPLICATION_ERROR captures numeric error code")
        void raiseAppErrorCode() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                      BEGIN
                        RAISE_APPLICATION_ERROR(-20042, 'Custom error message');
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "raise_application_error code");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);

            StatementInfo rae = proc.getStatements().stream()
                    .filter(s -> "RAISE_APPLICATION_ERROR".equals(s.getType()))
                    .findFirst().orElse(null);
            assertNotNull(rae, "Should have a RAISE_APPLICATION_ERROR statement");
            assertNotNull(rae.getErrorCode(), "errorCode should not be null");
            assertEquals(-20042, rae.getErrorCode());
        }

        @Test
        @DisplayName("RAISE_APPLICATION_ERROR captures message text")
        void raiseAppErrorMessage() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                      BEGIN
                        RAISE_APPLICATION_ERROR(-20001, 'Record not found');
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "raise_application_error message");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);

            StatementInfo rae = proc.getStatements().stream()
                    .filter(s -> "RAISE_APPLICATION_ERROR".equals(s.getType()))
                    .findFirst().orElse(null);
            assertNotNull(rae);
            assertNotNull(rae.getErrorMessage(), "errorMessage should not be null");
            assertTrue(rae.getErrorMessage().contains("Record not found"),
                    "errorMessage should contain 'Record not found', got: " + rae.getErrorMessage());
        }

        @Test
        @DisplayName("Multiple RAISE_APPLICATION_ERROR calls are all captured")
        void multipleRaiseAppErrors() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                      BEGIN
                        IF 1 = 0 THEN
                          RAISE_APPLICATION_ERROR(-20001, 'Error one');
                        ELSE
                          RAISE_APPLICATION_ERROR(-20002, 'Error two');
                        END IF;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "multiple raise_application_error");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);

            long count = proc.getStatements().stream()
                    .filter(s -> "RAISE_APPLICATION_ERROR".equals(s.getType()))
                    .count();
            assertEquals(2, count, "Should detect 2 RAISE_APPLICATION_ERROR calls");
        }

        @Test
        @DisplayName("Plain RAISE statement is still captured as RAISE type")
        void plainRaiseStillWorks() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                      BEGIN
                        RAISE NO_DATA_FOUND;
                      EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                          RAISE;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "plain raise still works");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);

            boolean hasRaise = proc.getStatements().stream()
                    .anyMatch(s -> "RAISE".equals(s.getType()));
            assertTrue(hasRaise, "RAISE statement should still be captured");
        }
    }

    // ----------------------------------------------------------------
    // Fix 4: SAVEPOINT name extraction
    // ----------------------------------------------------------------
    @Nested
    @DisplayName("Fix 4 — SAVEPOINT name extracted into StatementInfo.savepointName")
    class SavepointNameFix {

        @Test
        @DisplayName("SAVEPOINT statement captures the savepoint name")
        void savepointNameCaptured() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                      BEGIN
                        SAVEPOINT before_update;
                        UPDATE employees SET salary = salary * 1.1;
                      EXCEPTION
                        WHEN OTHERS THEN
                          ROLLBACK TO before_update;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "savepoint name captured");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);

            StatementInfo sp = proc.getStatements().stream()
                    .filter(s -> "SAVEPOINT".equals(s.getType()))
                    .findFirst().orElse(null);
            assertNotNull(sp, "Should have a SAVEPOINT statement");
            assertNotNull(sp.getSavepointName(), "savepointName should not be null");
            assertEquals("BEFORE_UPDATE", sp.getSavepointName().toUpperCase());
        }

        @Test
        @DisplayName("Multiple savepoints each capture their own name")
        void multipleSavepointNames() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                      BEGIN
                        SAVEPOINT start_of_batch;
                        INSERT INTO batch_log VALUES (1);
                        SAVEPOINT mid_batch;
                        UPDATE batch_data SET status = 'P';
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "multiple savepoint names");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);

            List<StatementInfo> savepoints = proc.getStatements().stream()
                    .filter(s -> "SAVEPOINT".equals(s.getType()))
                    .toList();
            assertEquals(2, savepoints.size(), "Should capture 2 savepoints");

            boolean hasStart = savepoints.stream()
                    .anyMatch(s -> "START_OF_BATCH".equalsIgnoreCase(s.getSavepointName()));
            boolean hasMid = savepoints.stream()
                    .anyMatch(s -> "MID_BATCH".equalsIgnoreCase(s.getSavepointName()));
            assertTrue(hasStart, "Should capture START_OF_BATCH savepoint name");
            assertTrue(hasMid, "Should capture MID_BATCH savepoint name");
        }
    }

    // ----------------------------------------------------------------
    // Fix 5: Implicit cursor attribute tracking
    // ----------------------------------------------------------------
    @Nested
    @DisplayName("Fix 5 — Implicit cursor attributes (SQL%ROWCOUNT etc.) detected")
    class ImplicitCursorAttributeFix {

        @Test
        @DisplayName("SQL%ROWCOUNT reference emits CURSOR_ATTRIBUTE statement")
        void sqlRowcount() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_rows NUMBER;
                      BEGIN
                        UPDATE employees SET status = 'A' WHERE dept_id = 10;
                        v_rows := SQL%ROWCOUNT;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "SQL%ROWCOUNT");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);

            StatementInfo attr = proc.getStatements().stream()
                    .filter(s -> "CURSOR_ATTRIBUTE".equals(s.getType()))
                    .filter(s -> s.getSqlText() != null && s.getSqlText().contains("ROWCOUNT"))
                    .findFirst().orElse(null);
            assertNotNull(attr, "Should detect SQL%ROWCOUNT as CURSOR_ATTRIBUTE statement");
            assertTrue(attr.getSqlText().contains("SQL"), "sqlText should contain SQL cursor name");
        }

        @Test
        @DisplayName("SQL%FOUND reference emits CURSOR_ATTRIBUTE statement")
        void sqlFound() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                      BEGIN
                        SELECT 1 INTO :x FROM dual;
                        IF SQL%FOUND THEN
                          DBMS_OUTPUT.PUT_LINE('found');
                        END IF;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "SQL%FOUND");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);

            boolean hasFound = proc.getStatements().stream()
                    .anyMatch(s -> "CURSOR_ATTRIBUTE".equals(s.getType())
                            && s.getSqlText() != null && s.getSqlText().contains("FOUND"));
            assertTrue(hasFound, "Should detect SQL%FOUND as CURSOR_ATTRIBUTE statement");
        }

        @Test
        @DisplayName("SQL%NOTFOUND reference emits CURSOR_ATTRIBUTE statement")
        void sqlNotFound() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                      BEGIN
                        UPDATE orders SET status = 'C' WHERE id = 99;
                        IF SQL%NOTFOUND THEN
                          DBMS_OUTPUT.PUT_LINE('nothing updated');
                        END IF;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "SQL%NOTFOUND");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);

            boolean hasNotFound = proc.getStatements().stream()
                    .anyMatch(s -> "CURSOR_ATTRIBUTE".equals(s.getType())
                            && s.getSqlText() != null && s.getSqlText().contains("NOTFOUND"));
            assertTrue(hasNotFound, "Should detect SQL%NOTFOUND as CURSOR_ATTRIBUTE statement");
        }

        @Test
        @DisplayName("Named explicit cursor attribute detected: MY_CUR%ISOPEN")
        void namedCursorIsOpen() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        CURSOR my_cur IS SELECT id FROM employees;
                      BEGIN
                        IF my_cur%ISOPEN THEN
                          CLOSE my_cur;
                        END IF;
                        OPEN my_cur;
                        CLOSE my_cur;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "named cursor %ISOPEN");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);

            boolean hasIsOpen = proc.getStatements().stream()
                    .anyMatch(s -> "CURSOR_ATTRIBUTE".equals(s.getType())
                            && s.getSqlText() != null && s.getSqlText().contains("ISOPEN"));
            assertTrue(hasIsOpen, "Should detect MY_CUR%ISOPEN as CURSOR_ATTRIBUTE statement");
        }

        @Test
        @DisplayName("Named explicit cursor %ROWCOUNT detected")
        void namedCursorRowcount() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        CURSOR c IS SELECT id FROM employees;
                        v_count NUMBER;
                      BEGIN
                        OPEN c;
                        FETCH c BULK COLLECT INTO v_count;
                        v_count := c%ROWCOUNT;
                        CLOSE c;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "named cursor %ROWCOUNT");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);

            boolean hasRowcount = proc.getStatements().stream()
                    .anyMatch(s -> "CURSOR_ATTRIBUTE".equals(s.getType())
                            && s.getSqlText() != null && s.getSqlText().contains("ROWCOUNT"));
            assertTrue(hasRowcount, "Should detect C%ROWCOUNT as CURSOR_ATTRIBUTE statement");
        }
    }

    // ----------------------------------------------------------------
    // Regression: existing features still work
    // ----------------------------------------------------------------
    @Nested
    @DisplayName("Regression — existing features not broken by gap fixes")
    class RegressionTests {

        @Test
        @DisplayName("Normal SELECT still captures table operations")
        void normalSelectNotBroken() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v NUMBER;
                      BEGIN
                        SELECT count(*) INTO v FROM employees e JOIN departments d ON e.dept_id = d.id;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "normal select regression");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);
            assertFalse(proc.getTableOperations().isEmpty(), "Should have table operations");
        }

        @Test
        @DisplayName("MERGE still captures operation as MERGE type")
        void mergeOperationTypeNotBroken() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                      BEGIN
                        MERGE INTO target t
                        USING source s ON (t.id = s.id)
                        WHEN MATCHED THEN UPDATE SET t.v = s.v;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "merge operation type regression");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);

            boolean hasMergeOp = proc.getTableOperations().stream()
                    .anyMatch(t -> "MERGE".equals(t.getOperation()));
            assertTrue(hasMergeOp, "Should still have MERGE operation type");
        }

        @Test
        @DisplayName("Sequence NEXTVAL reference still works")
        void sequenceNextvalNotBroken() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v NUMBER;
                      BEGIN
                        v := my_seq.NEXTVAL;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "sequence nextval regression");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);

            boolean hasSeq = proc.getCalls().stream()
                    .anyMatch(c -> "SEQUENCE".equals(c.getType()));
            assertTrue(hasSeq, "Should still detect sequence NEXTVAL call");
        }

        @Test
        @DisplayName("Exception handler body calls still captured")
        void exceptionHandlerCallsNotBroken() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                      BEGIN
                        UPDATE employees SET status = 'A';
                      EXCEPTION
                        WHEN OTHERS THEN
                          DBMS_OUTPUT.PUT_LINE(SQLERRM);
                          ROLLBACK;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "exception handler calls regression");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);
            assertFalse(proc.getExceptionHandlers().isEmpty(), "Should have exception handlers");
        }

        @Test
        @DisplayName("DBMS_SQL.PARSE still captured as dynamic SQL")
        void dbmsSqlParseNotBroken() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_cursor INTEGER;
                      BEGIN
                        v_cursor := DBMS_SQL.OPEN_CURSOR;
                        DBMS_SQL.PARSE(v_cursor, 'SELECT * FROM dual', DBMS_SQL.NATIVE);
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "dbms_sql.parse regression");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);
            assertFalse(proc.getDynamicSql().isEmpty(), "Should have dynamic SQL entries");
        }
    }
}
