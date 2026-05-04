package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Gap #2: FORALL & BULK COLLECT Detail Extraction")
public class ForallBulkCollectDetailTest extends ParserTestBase {

    @Test
    @DisplayName("FORALL with range bounds captures index, bounds, dmlType=INSERT")
    void testForallRangeBounds() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    TYPE t_ids IS TABLE OF NUMBER;
                    l_ids t_ids := t_ids(1, 2, 3);
                  BEGIN
                    FORALL i IN 1..l_ids.COUNT
                      INSERT INTO batch_log (id) VALUES (l_ids(i));
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "FORALL range");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        StatementInfo forall = proc.getStatements().stream()
                .filter(s -> "FORALL".equals(s.getType()))
                .findFirst().orElse(null);
        assertNotNull(forall, "Should have FORALL statement");
        assertEquals("I", forall.getIndexVariable());
        assertNotNull(forall.getBoundsExpression());
        assertEquals("INSERT", forall.getDmlType());
    }

    @Test
    @DisplayName("FORALL with DELETE captures dmlType=DELETE")
    void testForallDelete() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    TYPE t_ids IS TABLE OF NUMBER;
                    l_ids t_ids := t_ids(1, 2, 3);
                  BEGIN
                    FORALL i IN 1..l_ids.COUNT
                      DELETE FROM staging WHERE id = l_ids(i);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        StatementInfo forall = proc.getStatements().stream()
                .filter(s -> "FORALL".equals(s.getType()))
                .findFirst().orElse(null);
        assertNotNull(forall);
        assertEquals("DELETE", forall.getDmlType());
    }

    @Test
    @DisplayName("FORALL with UPDATE captures dmlType=UPDATE")
    void testForallUpdate() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    TYPE t_ids IS TABLE OF NUMBER;
                    l_ids t_ids := t_ids(1, 2, 3);
                  BEGIN
                    FORALL i IN 1..l_ids.COUNT
                      UPDATE batch_control SET status = 'DONE' WHERE id = l_ids(i);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        StatementInfo forall = proc.getStatements().stream()
                .filter(s -> "FORALL".equals(s.getType()))
                .findFirst().orElse(null);
        assertNotNull(forall);
        assertEquals("UPDATE", forall.getDmlType());
    }

    @Test
    @DisplayName("FORALL with SAVE EXCEPTIONS still parses")
    void testForallSaveExceptions() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    TYPE t_ids IS TABLE OF NUMBER;
                    l_ids t_ids := t_ids(1, 2, 3);
                  BEGIN
                    FORALL i IN 1..l_ids.COUNT SAVE EXCEPTIONS
                      INSERT INTO batch_log (id) VALUES (l_ids(i));
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "FORALL SAVE EXCEPTIONS");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        StatementInfo forall = proc.getStatements().stream()
                .filter(s -> "FORALL".equals(s.getType()))
                .findFirst().orElse(null);
        assertNotNull(forall);
        assertEquals("INSERT", forall.getDmlType());
    }

    @Test
    @DisplayName("BULK COLLECT with numeric LIMIT captures collection and limit")
    void testBulkCollectNumericLimit() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    CURSOR c1 IS SELECT id FROM employees;
                    TYPE t_ids IS TABLE OF NUMBER;
                    l_ids t_ids;
                  BEGIN
                    OPEN c1;
                    FETCH c1 BULK COLLECT INTO l_ids LIMIT 1000;
                    CLOSE c1;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "BULK COLLECT LIMIT");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        StatementInfo bulk = proc.getStatements().stream()
                .filter(s -> "BULK_COLLECT".equals(s.getType()))
                .findFirst().orElse(null);
        assertNotNull(bulk, "Should have BULK_COLLECT statement");
        assertNotNull(bulk.getCollectionName(), "Should capture collection name");
        assertEquals("1000", bulk.getLimitValue());
    }

    @Test
    @DisplayName("BULK COLLECT without LIMIT has null limitValue")
    void testBulkCollectNoLimit() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    CURSOR c1 IS SELECT id FROM employees;
                    TYPE t_ids IS TABLE OF NUMBER;
                    l_ids t_ids;
                  BEGIN
                    OPEN c1;
                    FETCH c1 BULK COLLECT INTO l_ids;
                    CLOSE c1;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        StatementInfo bulk = proc.getStatements().stream()
                .filter(s -> "BULK_COLLECT".equals(s.getType()))
                .findFirst().orElse(null);
        assertNotNull(bulk);
        assertNull(bulk.getLimitValue());
    }
}
