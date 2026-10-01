package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.PostgresRoutineInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Batch 3 (regex → lexer): the PL/pgSQL dollar-quote wrapper is now located from the
 * {@code PostgresLexer.DOLLAR_QUOTED_STRING} token rather than a regex over hand-blanked text.
 * These tests pin the two halves of that contract: the lexer path handles a normal (including
 * custom-tag and nested-different-tag) wrapper without touching the legacy fallback, and an
 * unlexable (unbalanced) wrapper still falls back and is COUNTED.
 */
@DisplayName("PostgreSQL dollar-quote lexer migration (batch 3)")
public class PostgresDollarQuoteLexerTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    @Test
    @DisplayName("custom-tag $body$ wrapper is located by the lexer, not the regex fallback")
    void customTagWrapperUsesLexer() {
        long before = PostgresAnalysisEngine.DOLLAR_QUOTE_LEXER_FALLBACKS.get();
        String src = "CREATE OR REPLACE FUNCTION f() RETURNS VOID LANGUAGE plpgsql AS $body$\n" +
                "BEGIN\n  INSERT INTO tab1 (id) VALUES (1);\nEND;\n$body$;\n";
        PostgresRoutineInfo r = engine.parse(src, "f", null);
        assertTrue(r.getTableOps().stream().anyMatch(t -> "tab1".equalsIgnoreCase(t.getTableName())),
                "body tables must still be captured through the lexer-located wrapper");
        assertEquals(before, PostgresAnalysisEngine.DOLLAR_QUOTE_LEXER_FALLBACKS.get(),
                "a well-formed wrapper must not use the regex fallback");
    }

    @Test
    @DisplayName("nested different-tag literal inside the body still parses (statement after it reachable)")
    void nestedDifferentTagBodyStillParses() {
        String src = "CREATE OR REPLACE FUNCTION f() RETURNS VOID LANGUAGE plpgsql AS $body$\n" +
                "BEGIN\n" +
                "  EXECUTE $sql$SELECT * FROM tab2 WHERE id = 1$sql$;\n" +
                "  INSERT INTO tab3 (id) VALUES (2);\n" +
                "END;\n" +
                "$body$;\n";
        PostgresRoutineInfo r = engine.parse(src, "f", null);
        assertTrue(r.getTableOps().stream().anyMatch(t -> "tab3".equalsIgnoreCase(t.getTableName())),
                "the statement after a nested different-tag dollar literal must still be parsed");
    }

    @Test
    @DisplayName("unbalanced wrapper cannot be lexed → counted regex fallback still recovers")
    void unbalancedWrapperFallsBackAndIsCounted() {
        long before = PostgresAnalysisEngine.DOLLAR_QUOTE_LEXER_FALLBACKS.get();
        String src = "CREATE PROCEDURE p() LANGUAGE plpgsql AS $body$\nBEGIN\n  SELECT 1;\nEND;\n";
        engine.parse(src, "p", null);
        assertTrue(PostgresAnalysisEngine.DOLLAR_QUOTE_LEXER_FALLBACKS.get() > before,
                "an unbalanced dollar wrapper must increment the counted lexer fallback");
    }
}
