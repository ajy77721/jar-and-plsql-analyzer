package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression cover for the 2026-09-19 dynamic-SQL / join round:
 *   1. E'…' escape strings (new ESQ_STRING lexer token) usable wherever a plain literal is,
 *      including as a format() template and inside the dollar-quote pre-scan.
 *   2. Schema-qualified pg_catalog.format(...) treated as format(...).
 *   3. EVERY %I placeholder of a format() template resolved, not only the first.
 *   4. JOIN ... USING (cols) — synthesized condition + left side resolved from the base table.
 */
class PostgresFormatAndJoinUsingTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private PostgresRoutineInfo parse(String body) {
        return engine.parse("CREATE PROCEDURE p() AS $$ BEGIN " + body + " END; $$ LANGUAGE plpgsql;",
                "p", null);
    }

    private List<String> dynamicTables(PostgresRoutineInfo r) {
        return r.getTableOps().stream()
                .filter(t -> t.getDmlType() == PostgresTableOpInfo.DmlType.DYNAMIC)
                .map(PostgresTableOpInfo::getTableName)
                .toList();
    }

    @Test
    @DisplayName("E'…' format template yields the table op")
    void escapeStringFormatTemplate() {
        PostgresRoutineInfo r = parse("EXECUTE format(E'SELECT * FROM %I', 'emp');");
        assertEquals(List.of("emp"), dynamicTables(r));
        assertTrue(r.getErrors().isEmpty(), "errors=" + r.getErrors());
    }

    @Test
    @DisplayName("E'…' with a backslash-escaped quote does not break the parse")
    void escapeStringWithEscapedQuote() {
        PostgresRoutineInfo r = parse("v := E'x\\'y'; INSERT INTO t VALUES (1);");
        assertTrue(r.getErrors().isEmpty(), "errors=" + r.getErrors());
        assertTrue(r.getTableOps().stream().anyMatch(t -> "t".equalsIgnoreCase(t.getTableName())));
    }

    @Test
    @DisplayName("schema-qualified pg_catalog.format(...) is still format(...)")
    void qualifiedFormatCall() {
        PostgresRoutineInfo r = parse("EXECUTE pg_catalog.format('SELECT * FROM %I', 'emp');");
        assertEquals(List.of("emp"), dynamicTables(r));
    }

    @Test
    @DisplayName("every %I placeholder resolves, not just the first")
    void allPlaceholdersResolved() {
        PostgresRoutineInfo r = parse(
                "EXECUTE format('INSERT INTO %I SELECT * FROM %I', 'dst', 'src');");
        assertEquals(List.of("dst", "src"), dynamicTables(r));
    }

    @Test
    @DisplayName("a variable-fed %I still only sets unresolvedVarName")
    void unresolvedPlaceholderUnchanged() {
        PostgresRoutineInfo r = parse("EXECUTE format('SELECT * FROM %I', v_tab);");
        assertTrue(dynamicTables(r).isEmpty());
        assertEquals("v_tab", r.getDynamicSql().get(0).getUnresolvedVarName());
    }

    @Test
    @DisplayName("JOIN ... USING (cols) records a condition and a left table")
    void joinUsing() {
        PostgresRoutineInfo r = parse("SELECT 1 FROM a x JOIN b y USING (id, k);");
        PostgresJoinInfo j = r.getTableOps().stream()
                .flatMap(t -> t.getJoins().stream()).findFirst().orElseThrow();
        assertEquals("b", j.getJoinedTable());
        assertEquals("USING (id, k)", j.getCondition());
        assertEquals("A", j.getLeftTable());
        assertEquals("X", j.getLeftTableAlias());
        assertTrue(r.getErrors().isEmpty(), "errors=" + r.getErrors());
    }
}
