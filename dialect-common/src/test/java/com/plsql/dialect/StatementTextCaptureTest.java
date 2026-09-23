package com.plsql.dialect;

import org.antlr.v4.runtime.CharStreams;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The two rules that decide where a captured statement ends.
 *
 * <p>Both were originally fixed twice, once in each dialect's copy of this code. Testing them here,
 * at the single source, is the payoff for merging them.
 */
class StatementTextCaptureTest {

    /** parenDepth is the piece that is independently testable without an ANTLR parse tree. */
    @Test
    @DisplayName("paren depth ignores parentheses inside string literals")
    void parenDepthIgnoresLiterals() {
        var cs = CharStreams.fromString("SELECT f((a), ')') FROM t");
        assertEquals(0, StatementTextCapture.parenDepth(cs, 0, cs.size() - 1),
                "a ')' inside a literal is data, not structure");
    }

    @Test
    @DisplayName("paren depth reports an unclosed group")
    void parenDepthCountsOpenGroups() {
        var cs = CharStreams.fromString("SELECT (a, (b");
        assertEquals(2, StatementTextCapture.parenDepth(cs, 0, cs.size() - 1));
    }

    @Test
    @DisplayName("THEN and END are not block keywords, so CASE survives")
    void caseKeywordsAreNotTerminators() {
        // The single most important property here. If END or THEN were added to either set, every
        // `CASE WHEN … THEN … END` in a select list would be cut mid-expression, turning statements
        // that parse today into ones that do not.
        assertFalse(StatementTextCapture.DEFAULT_BLOCK_KEYWORDS.contains("END"));
        assertFalse(StatementTextCapture.DEFAULT_BLOCK_KEYWORDS.contains("THEN"));
        assertFalse(StatementTextCapture.DB2_BLOCK_KEYWORDS.contains("END"));
        assertFalse(StatementTextCapture.DB2_BLOCK_KEYWORDS.contains("THEN"));
    }

    @Test
    @DisplayName("DB2 adds DO, its loop opener, and the default set does not")
    void db2AddsDo() {
        assertTrue(StatementTextCapture.DB2_BLOCK_KEYWORDS.contains("DO"));
        assertFalse(StatementTextCapture.DEFAULT_BLOCK_KEYWORDS.contains("DO"),
                "DO is a DB2 spelling; adding it everywhere would cut valid SQL in other dialects");
        assertTrue(StatementTextCapture.DB2_BLOCK_KEYWORDS.containsAll(
                StatementTextCapture.DEFAULT_BLOCK_KEYWORDS),
                "the DB2 set must be a superset, or DB2 would lose a cut the others make");
    }

    @Test
    @DisplayName("a null context yields null rather than throwing")
    void nullContextIsSafe() {
        // Statement text is a bonus; failing the whole parse to obtain it would be the wrong trade.
        assertNull(StatementTextCapture.capture(null, StatementTextCapture.DEFAULT_BLOCK_KEYWORDS));
    }
}
