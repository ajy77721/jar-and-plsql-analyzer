package com.plsql.parser;

import com.plsql.parser.flow.TriggerDiscoverer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regex → grammar migration, trigger site: {@link TriggerDiscoverer} feeds the trigger body
 * through the real Oracle ANTLR path ({@code PlSqlParserEngine}) and derives its DML operations
 * and sequence references from the visitor output; the DML_PATTERN / SEQ_PATTERN text scans
 * survive only as a counted fallback for bodies that will not parse (Oracle wrapped source …).
 *
 * <p>The comma-separated FROM list is the proof the grammar path is primary: the old DML_PATTERN
 * had no comma alternative, so only the table directly after FROM was ever recorded.
 */
@DisplayName("Trigger body: parsed through the Oracle grammar (regex → fallback only)")
class TriggerBodyGrammarParseTest {

    private final TriggerDiscoverer discoverer = new TriggerDiscoverer(null, null);

    @Test
    @DisplayName("comma-separated FROM in a trigger body yields every table (regex saw only the first)")
    void commaFromListResolvedByGrammar() {
        String body = "BEGIN INSERT INTO audit_log (id) "
                + "SELECT x.id FROM emp x, dept y WHERE x.id = y.id; END;";
        List<Map<String, Object>> ops = discoverer.extractDmlFromBody(body, Collections.emptyMap());
        List<String> tables = ops.stream().map(o -> String.valueOf(o.get("tableName"))).toList();
        assertTrue(tables.stream().anyMatch(t -> t.equalsIgnoreCase("AUDIT_LOG")), "got " + tables);
        assertTrue(tables.stream().anyMatch(t -> t.equalsIgnoreCase("EMP")), "got " + tables);
        assertTrue(tables.stream().anyMatch(t -> t.equalsIgnoreCase("DEPT")),
                "DEPT should come from the comma FROM list via the grammar, got " + tables);
    }

    @Test
    @DisplayName("sequence refs still resolved through the grammar")
    void sequenceResolvedByGrammar() {
        String body = "BEGIN :NEW.ID := MY_SEQ.NEXTVAL; END;";
        List<Map<String, Object>> seqs = discoverer.extractSequencesFromBody(body);
        assertEquals(1, seqs.size());
        assertEquals("MY_SEQ", seqs.get(0).get("sequenceName"));
    }

    @Test
    @DisplayName("a body the grammar cannot parse falls back to the regex scan, counted")
    void regexFallbackIsCounted() {
        long before = TriggerDiscoverer.TRIGGER_BODY_REGEX_FALLBACKS.get();
        // Not valid PL/SQL — the grammar path must decline and the text scan take over.
        String body = "BEGIN ))) not plsql at all (((";
        discoverer.extractDmlFromBody(body, Collections.emptyMap());
        assertTrue(TriggerDiscoverer.TRIGGER_BODY_REGEX_FALLBACKS.get() > before,
                "an unparseable trigger body must be counted as a regex fallback");
    }
}
