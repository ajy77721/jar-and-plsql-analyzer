package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regex → grammar migration, MySQL site: a fully-literal {@code PREPARE ... FROM '<sql>'} now has
 * its table targets read off a recursive parse of the literal with the same MySQL grammar
 * ({@link com.plsql.mysql.visitor.MySqlAnalysisVisitor}), rather than the legacy
 * DYN_SQL_TABLE_PATTERN regex, which survives only as a counted fallback.
 *
 * <p>The comma-separated FROM list is the proof: the old regex had no comma alternative, so only
 * the table directly after FROM was ever recorded.
 */
@DisplayName("MySQL dynamic SQL: recursive grammar parse (regex → fallback only)")
class MySqlDynamicSqlRecursiveParseTest {

    private final MySqlGrammarEngine engine = new MySqlGrammarEngine();

    @Test
    @DisplayName("PREPARE literal with comma-separated FROM yields every table")
    void prepareLiteralCommaFromList() {
        String sql = """
                CREATE PROCEDURE p_dyn()
                BEGIN
                  PREPARE s FROM 'SELECT * FROM alpha, beta WHERE alpha.id = beta.id';
                  EXECUTE s;
                  DEALLOCATE PREPARE s;
                END
                """;
        MySqlRoutineInfo r = engine.parse(sql, "p_dyn", null);
        List<String> dynamic = r.getTableOps().stream()
                .filter(t -> t.getDmlType() == MySqlTableOpInfo.DmlType.DYNAMIC)
                .map(MySqlTableOpInfo::getTableName)
                .toList();
        assertTrue(dynamic.stream().anyMatch(t -> "alpha".equalsIgnoreCase(t)),
                "alpha should be a DYNAMIC table op, got " + dynamic);
        assertTrue(dynamic.stream().anyMatch(t -> "beta".equalsIgnoreCase(t)),
                "beta should be a DYNAMIC table op from the comma FROM list, got " + dynamic);
    }

    @Test
    @DisplayName("PREPARE literal INSERT ... SELECT yields both target and source")
    void prepareLiteralInsertSelect() {
        String sql = """
                CREATE PROCEDURE p_dyn2()
                BEGIN
                  PREPARE s FROM 'INSERT INTO dst SELECT * FROM src WHERE id > 0';
                  EXECUTE s;
                  DEALLOCATE PREPARE s;
                END
                """;
        MySqlRoutineInfo r = engine.parse(sql, "p_dyn2", null);
        List<String> dynamic = r.getTableOps().stream()
                .filter(t -> t.getDmlType() == MySqlTableOpInfo.DmlType.DYNAMIC)
                .map(MySqlTableOpInfo::getTableName)
                .toList();
        assertTrue(dynamic.stream().anyMatch(t -> "dst".equalsIgnoreCase(t)), "got " + dynamic);
        assertTrue(dynamic.stream().anyMatch(t -> "src".equalsIgnoreCase(t)), "got " + dynamic);
    }
}
