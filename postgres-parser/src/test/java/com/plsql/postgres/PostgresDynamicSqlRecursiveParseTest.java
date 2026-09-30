package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.PostgresRoutineInfo;
import com.plsql.postgres.model.PostgresTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regex → grammar migration, Postgres site: a fully-literal {@code EXECUTE '<sql>'} now has its
 * table targets read off a recursive parse of the literal with the same Postgres grammar, rather
 * than the legacy DYN_SQL_TABLE_PATTERN regex, which survives only as a counted fallback.
 *
 * <p>The comma-separated FROM list is the proof: the old regex had no comma alternative, so only
 * the table directly after FROM was ever recorded.
 */
@DisplayName("Postgres dynamic SQL: recursive grammar parse (regex → fallback only)")
class PostgresDynamicSqlRecursiveParseTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private PostgresRoutineInfo parse(String body) {
        return engine.parse(
                "CREATE PROCEDURE p() AS $$ BEGIN " + body + " END; $$ LANGUAGE plpgsql;",
                "p", null);
    }

    private List<String> dynamicTables(PostgresRoutineInfo r) {
        return r.getTableOps().stream()
                .filter(t -> t.getDmlType() == PostgresTableOpInfo.DmlType.DYNAMIC)
                .map(PostgresTableOpInfo::getTableName)
                .toList();
    }

    @Test
    @DisplayName("EXECUTE literal with comma-separated FROM yields every table")
    void executeLiteralCommaFromList() {
        PostgresRoutineInfo r = parse("EXECUTE 'SELECT * FROM alpha, beta WHERE alpha.id = beta.id';");
        List<String> tables = dynamicTables(r);
        assertTrue(tables.stream().anyMatch(t -> "alpha".equalsIgnoreCase(t)), "got " + tables);
        assertTrue(tables.stream().anyMatch(t -> "beta".equalsIgnoreCase(t)),
                "beta should come from the comma FROM list, got " + tables);
    }

    @Test
    @DisplayName("EXECUTE literal INSERT ... SELECT yields both target and source")
    void executeLiteralInsertSelect() {
        PostgresRoutineInfo r = parse("EXECUTE 'INSERT INTO dst SELECT * FROM src WHERE id > 0';");
        List<String> tables = dynamicTables(r);
        assertTrue(tables.stream().anyMatch(t -> "dst".equalsIgnoreCase(t)), "got " + tables);
        assertTrue(tables.stream().anyMatch(t -> "src".equalsIgnoreCase(t)), "got " + tables);
    }
}
