package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Gap #8: Loop Nesting Depth Tracking")
public class LoopNestingDepthTest extends ParserTestBase {

    @Test
    @DisplayName("Single LOOP has nesting depth 1")
    void testSingleLoop() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    LOOP
                      EXIT;
                    END LOOP;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Single LOOP");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        StatementInfo loop = proc.getStatements().stream()
                .filter(s -> "LOOP".equals(s.getType()))
                .findFirst().orElse(null);
        assertNotNull(loop);
        assertEquals(1, loop.getNestingDepth());
    }

    @Test
    @DisplayName("Nested FOR inside WHILE has depth 2")
    void testNestedForInsideWhile() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_done BOOLEAN := FALSE;
                  BEGIN
                    WHILE NOT v_done LOOP
                      FOR i IN 1..10 LOOP
                        NULL;
                      END LOOP;
                      v_done := TRUE;
                    END LOOP;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        List<StatementInfo> loops = proc.getStatements().stream()
                .filter(s -> s.getType() != null && s.getType().contains("LOOP"))
                .toList();
        assertTrue(loops.size() >= 2);
        assertTrue(loops.stream().anyMatch(s -> s.getNestingDepth() == 1));
        assertTrue(loops.stream().anyMatch(s -> s.getNestingDepth() == 2));
    }

    @Test
    @DisplayName("Triple-nested loops have depths 1, 2, 3")
    void testTripleNestedLoops() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    FOR i IN 1..5 LOOP
                      FOR j IN 1..5 LOOP
                        LOOP
                          EXIT;
                        END LOOP;
                      END LOOP;
                    END LOOP;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        List<StatementInfo> loops = proc.getStatements().stream()
                .filter(s -> s.getType() != null && s.getType().contains("LOOP"))
                .toList();
        assertTrue(loops.stream().anyMatch(s -> s.getNestingDepth() == 1));
        assertTrue(loops.stream().anyMatch(s -> s.getNestingDepth() == 2));
        assertTrue(loops.stream().anyMatch(s -> s.getNestingDepth() == 3));
    }

    @Test
    @DisplayName("Two sibling loops both have depth 1")
    void testSiblingLoops() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    FOR i IN 1..5 LOOP
                      NULL;
                    END LOOP;
                    FOR j IN 1..5 LOOP
                      NULL;
                    END LOOP;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        List<StatementInfo> loops = proc.getStatements().stream()
                .filter(s -> "FOR_LOOP".equals(s.getType()))
                .toList();
        assertEquals(2, loops.size());
        assertTrue(loops.stream().allMatch(s -> s.getNestingDepth() == 1));
    }

    @Test
    @DisplayName("FORALL inside a loop has depth 2")
    void testForallInsideLoop() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    TYPE t_ids IS TABLE OF NUMBER;
                    l_ids t_ids := t_ids(1, 2, 3);
                  BEGIN
                    FOR i IN 1..3 LOOP
                      FORALL j IN 1..l_ids.COUNT
                        INSERT INTO log_table (id) VALUES (l_ids(j));
                    END LOOP;
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
        assertEquals(2, forall.getNestingDepth());
    }
}
