package com.plsql.parser.multidb;

import com.plsql.parser.model.ParseResult;
import com.plsql.parser.model.ParsedObject;
import com.plsql.parser.model.TableOperationInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * One unsupported statement must not discard the statements around it — in every dialect.
 *
 * <h2>The failure this generalises</h2>
 *
 * <p>{@code TRUNCATE TABLE} was missing from the MySQL grammar's {@code routineStatement}. The cost
 * was not that TRUNCATE went unreported. It was that no alternative matched, the enclosing body
 * failed, and <b>the entire procedure yielded nothing</b>: zero tables, zero operations, an empty
 * {@code statementSummary}, and no error anywhere. A procedure that writes to eight tables reported
 * that it writes to none, and the analysis looked complete.
 *
 * <p>{@code TruncateDoesNotKillBodyTest} pins that one statement in that one grammar. But the defect
 * was never really about TRUNCATE — it was about a grammar that fails the whole body instead of
 * skipping what it does not know, and there are four more grammars here built the same way. Fixing
 * the instance without gating the class is how it comes back under a different keyword.
 *
 * <h2>What is and is not asserted</h2>
 *
 * <p>This gate asserts only that <b>the neighbours survive</b>. It deliberately does <i>not</i>
 * require the probe statement to be recognised: demanding full coverage of every construct in five
 * dialects is a feature request, and one that would be met by disabling the test. Silently losing an
 * unsupported statement is an acceptable, bounded degradation. Silently losing the eight statements
 * next to it is not.
 *
 * <p>Each probe is therefore a differential measurement — parse the body with the extra statement
 * and without it, and compare. Any operation present in the smaller body must still be present in
 * the larger one. That framing means the test says nothing about how good a grammar is, only that
 * adding a line never subtracts information.
 */
class WholeBodyLossGateTest {

    /** A dialect, its parser, and the syntax needed to write the same probe body in it. */
    private record Dialect(String name, String create, String begin, String end, String schema) {}

    private static final List<Dialect> DIALECTS = List.of(
            new Dialect("MYSQL",
                    "CREATE PROCEDURE p_probe()", "BEGIN", "END", "analyzerdb"),
            new Dialect("DB2",
                    "CREATE PROCEDURE p_probe()", "BEGIN", "END", "APP"),
            new Dialect("POSTGRES",
                    "CREATE OR REPLACE FUNCTION p_probe() RETURNS void AS $$", "BEGIN", "END; $$ LANGUAGE plpgsql", "public"),
            new Dialect("SQLSERVER",
                    "CREATE PROCEDURE p_probe AS", "BEGIN", "END", "dbo"));

    /**
     * Statements to interleave. Each is written in syntax the dialect at least tokenises.
     *
     * <p>Chosen because each is a plausible thing to meet in a real procedure and each is the kind of
     * construct a hand-written grammar tends to omit — the TRUNCATE-shaped hole.
     */
    private static final Map<String, String> PROBES = new LinkedHashMap<>();
    static {
        PROBES.put("TRUNCATE",        "TRUNCATE TABLE staging;");
        PROBES.put("COMMIT",          "COMMIT;");
        PROBES.put("MERGE",           "MERGE INTO staging t USING staging s ON (t.id = s.id) "
                                    + "WHEN MATCHED THEN UPDATE SET t.note = s.note;");
        PROBES.put("CASE",            "CASE WHEN 1 = 1 THEN SET @x = 1; ELSE SET @x = 2; END CASE;");
        PROBES.put("WHILE",           "WHILE 1 = 0 DO SET @x = 1; END WHILE;");
        PROBES.put("LOCK",            "LOCK TABLE staging IN EXCLUSIVE MODE;");
        PROBES.put("CREATE_INDEX",    "CREATE INDEX ix_probe ON staging (id);");
        PROBES.put("ALTER",           "ALTER TABLE staging ADD COLUMN extra INT;");
        PROBES.put("GRANT",           "GRANT SELECT ON staging TO some_role;");
        PROBES.put("SAVEPOINT",       "SAVEPOINT sp_probe;");
    }

    /** The two statements whose survival is the whole point. */
    private static final String SURROUNDING_DML = """
              INSERT INTO staging (id, note) VALUES (1, 'seeded');
              UPDATE staging SET note = 'updated' WHERE id = 1;
            """;

    @Test
    @DisplayName("in every dialect, an unsupported statement never costs the DML around it")
    void noProbeStatementDiscardsItsNeighbours() {
        List<String> losses = new ArrayList<>();
        List<String> unsupportedDialects = new ArrayList<>();

        for (Dialect d : DIALECTS) {
            StoredProcedureParser parser = StoredProcedureParserFactory.get(d.name());
            if (parser == null) { unsupportedDialects.add(d.name()); continue; }

            Set<String> baseline = opsOf(parser, d, null);
            if (baseline.isEmpty()) {
                // The control itself found nothing, so no differential is meaningful for this
                // dialect. Reported rather than silently passed: a baseline of zero is exactly the
                // symptom this gate exists to catch, and skipping it would hide it.
                losses.add(d.name() + " / <control>: the plain INSERT+UPDATE body yielded no "
                        + "operations at all, so this dialect cannot be measured");
                continue;
            }

            for (var e : PROBES.entrySet()) {
                Set<String> withProbe = opsOf(parser, d, e.getValue());
                Set<String> lost = new LinkedHashSet<>(baseline);
                lost.removeAll(withProbe);
                if (!lost.isEmpty()) {
                    losses.add(d.name() + " / " + e.getKey() + ": adding one statement lost "
                            + lost + "  (control=" + baseline + " withProbe=" + withProbe + ")");
                }
            }
        }

        if (!losses.isEmpty()) {
            fail("A single unrecognised statement discarded the DML around it. The procedure will\n"
                    + "report fewer tables than it touches, with nothing logged and nothing thrown —\n"
                    + "the grammar must skip what it cannot parse, not fail the enclosing body.\n"
                    + (unsupportedDialects.isEmpty() ? ""
                        : "(no parser registered for: " + unsupportedDialects + ")\n")
                    + "\n  " + String.join("\n  ", losses));
        }
    }

    /** Parses the standard body, optionally with one extra statement in the middle. */
    private Set<String> opsOf(StoredProcedureParser parser, Dialect d, String probe) {
        String body = d.create() + "\n" + d.begin() + "\n"
                + (probe == null ? "" : "  " + probe + "\n")
                + SURROUNDING_DML
                + d.end();
        Set<String> ops = new LinkedHashSet<>();
        ParseResult r;
        try {
            r = parser.parse(body, "P_PROBE", d.schema());
        } catch (RuntimeException ex) {
            // A thrown exception is a different (and louder) failure than silent loss; record it as
            // total loss so it shows up in the same report rather than aborting the sweep.
            return ops;
        }
        if (r == null || r.getObjects() == null) return ops;
        for (ParsedObject o : r.getObjects()) {
            if (o.getTableOperations() == null) continue;
            for (TableOperationInfo t : o.getTableOperations()) {
                if (t.getOperation() == null || t.getTableName() == null) continue;
                ops.add(t.getOperation().toUpperCase(Locale.ROOT) + ":"
                        + t.getTableName().toUpperCase(Locale.ROOT));
            }
        }
        return ops;
    }
}
