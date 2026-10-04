package com.plsql.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsql.parser.model.ParseResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The parallel parse must be deterministic: parsing the same source concurrently (many threads,
 * many repetitions — the engine hands each thread its own ANTLR DFA/context caches) must yield a
 * byte-identical {@link ParseResult} to parsing it serially. This is the correctness contract that
 * lets the crawl drop its global parse lock.
 *
 * <p>Equality is a full structural comparison: the result is serialized to JSON with the only
 * non-deterministic field ({@code parseTimeMs}) zeroed, so any difference in objects, calls,
 * statements, table operations or errors fails the test.
 */
@DisplayName("PlSqlParserEngine parallel parse determinism")
public class PlSqlParserEngineParallelParseTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Map<String, String> SOURCES = new LinkedHashMap<>();
    static {
        SOURCES.put("PKG_ROOT", "CREATE OR REPLACE PACKAGE BODY PKG_ROOT AS\n"
                + "  PROCEDURE P1 IS\n  BEGIN\n    PKG_A.P1;\n    PKG_B.P1;\n    INSERT INTO T1 VALUES (1);\n  END P1;\n"
                + "  FUNCTION F1 RETURN NUMBER IS\n  BEGIN\n    RETURN PKG_A.F1;\n  END F1;\n"
                + "END PKG_ROOT;\n");
        SOURCES.put("PKG_A", "CREATE OR REPLACE PACKAGE BODY PKG_A AS\n"
                + "  PROCEDURE P1 IS\n  BEGIN\n    UPDATE T2 SET C = 1 WHERE ID = 2;\n  END P1;\n"
                + "  FUNCTION F1 RETURN NUMBER IS\n  BEGIN\n    RETURN 1;\n  END F1;\n"
                + "END PKG_A;\n");
        SOURCES.put("PKG_B", "CREATE OR REPLACE PACKAGE BODY PKG_B AS\n"
                + "  PROCEDURE P1 IS\n  BEGIN\n    DELETE FROM T3 WHERE ID = 3;\n  END P1;\n"
                + "END PKG_B;\n");
        SOURCES.put("PROC_STANDALONE", "CREATE OR REPLACE PROCEDURE PROC_STANDALONE AS\n"
                + "BEGIN\n  PKG_A.P1;\n  PKG_B.P1;\nEND PROC_STANDALONE;\n");
        SOURCES.put("TRG_T1", "CREATE OR REPLACE TRIGGER TRG_T1 BEFORE INSERT ON T1 FOR EACH ROW\n"
                + "BEGIN\n  PKG_A.P1;\nEND;\n");
        // Deliberately includes a construct the fast SLL pass rejects, exercising the LL fallback.
        SOURCES.put("PROC_COMPLEX", "CREATE OR REPLACE PROCEDURE PROC_COMPLEX AS\n"
                + "  CURSOR C1 IS SELECT * FROM T4 WHERE ID IN (SELECT ID FROM T5);\n"
                + "  V NUMBER;\n"
                + "BEGIN\n  FOR R IN C1 LOOP\n    V := R.ID;\n  END LOOP;\n"
                + "  EXECUTE IMMEDIATE 'DELETE FROM T6 WHERE ID = ' || V;\nEND PROC_COMPLEX;\n");
    }

    /** Canonical, timing-free JSON for a parse result — structural equality across runs. */
    private static String canonical(ParseResult r) {
        try {
            r.setParseTimeMs(0); // the only wall-clock-dependent field
            return MAPPER.writeValueAsString(r);
        } catch (Exception e) {
            throw new AssertionError("failed to serialize ParseResult", e);
        }
    }

    @Test
    @DisplayName("concurrent parses equal the serial parse for every fixture source")
    void concurrentEqualsSerial() throws Exception {
        // Serial reference, computed first on the test thread.
        Map<String, String> serial = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : SOURCES.entrySet()) {
            ParseResult r = new PlSqlParserEngine().parseContent(e.getValue(), e.getKey() + ".sql");
            assertTrue(r.getErrors() == null || r.getErrors().isEmpty(),
                    "fixture " + e.getKey() + " should parse cleanly, got " + r.getErrors());
            serial.put(e.getKey(), canonical(r));
        }

        // Parallel: the same sources, many repetitions, on a fixed pool. New pool threads get
        // fresh per-thread ANTLR caches, so this also covers cold-cache concurrent parses.
        int threads = 8;
        int repetitions = 12;
        ExecutorService pool = Executors.newFixedThreadPool(threads,
                r -> { Thread t = new Thread(r, "parse-determinism"); t.setDaemon(true); return t; });
        try {
            List<Callable<String[]>> tasks = new ArrayList<>();
            for (int rep = 0; rep < repetitions; rep++) {
                for (Map.Entry<String, String> e : SOURCES.entrySet()) {
                    final String name = e.getKey();
                    final String src = e.getValue();
                    tasks.add(() -> new String[]{
                            name,
                            canonical(new PlSqlParserEngine().parseContent(src, name + ".sql"))
                    });
                }
            }
            List<Future<String[]>> futures = pool.invokeAll(tasks, 120, TimeUnit.SECONDS);
            int checked = 0;
            for (Future<String[]> f : futures) {
                String[] got = f.get(); // surfaces any worker exception (incl. ANTLR corruption)
                assertEquals(serial.get(got[0]), got[1],
                        "parallel parse of " + got[0] + " differs from the serial result");
                checked++;
            }
            assertEquals(SOURCES.size() * repetitions, checked);
        } finally {
            pool.shutdownNow();
        }

        // The test thread's own result must still be identical after all that concurrent traffic.
        for (Map.Entry<String, String> e : SOURCES.entrySet()) {
            ParseResult again = new PlSqlParserEngine().parseContent(e.getValue(), e.getKey() + ".sql");
            assertEquals(serial.get(e.getKey()), canonical(again),
                    "post-parallel serial reparse of " + e.getKey() + " changed");
        }
    }

    /**
     * Worst-case contention: every thread parses the SAME source from the same instant (a barrier),
     * so they all traverse the same grammar decisions at once. With a shared DFA/context cache this
     * is exactly where corruption would show; with per-thread caches every result must still equal
     * the serial one.
     */
    @Test
    @DisplayName("simultaneous same-source parses stay identical (no shared-cache race)")
    void simultaneousSameSourceBurst() throws Exception {
        String name = "PROC_COMPLEX";
        String src = SOURCES.get(name);
        String expected = canonical(new PlSqlParserEngine().parseContent(src, name + ".sql"));

        int threads = 8;
        int rounds = 5;
        for (int round = 0; round < rounds; round++) {
            ExecutorService pool = Executors.newFixedThreadPool(threads,
                    r -> { Thread t = new Thread(r, "parse-burst"); t.setDaemon(true); return t; });
            CountDownLatch start = new CountDownLatch(1);
            List<Future<String>> futures = new ArrayList<>();
            try {
                for (int i = 0; i < threads; i++) {
                    futures.add(pool.submit(() -> {
                        start.await(); // all threads hit the parser at the same instant
                        return canonical(new PlSqlParserEngine().parseContent(src, name + ".sql"));
                    }));
                }
                start.countDown();
                for (Future<String> f : futures) {
                    assertEquals(expected, f.get(120, TimeUnit.SECONDS),
                            "simultaneous parse produced a different result");
                }
            } finally {
                pool.shutdownNow();
            }
        }
    }
}
