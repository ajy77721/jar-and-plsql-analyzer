package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.Db2RoutineInfo;
import com.plsql.db2.model.Db2TableOpInfo;
import com.plsql.db2.visitor.Db2AnalysisVisitor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regex → grammar migration, DB2 site: a fully-literal {@code EXECUTE IMMEDIATE '<sql>'} has its
 * table targets read off a recursive parse of the literal with the same DB2 grammar, rather than
 * the legacy DYN_SQL_TABLE_PATTERN regex, which survives only as a counted fallback.
 *
 * <p>Proves both the recursive-parse rows and the fallback counter of the migration.
 */
@DisplayName("DB2 dynamic SQL: recursive grammar parse (regex → fallback only)")
class Db2DynamicSqlRecursiveParseTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    @Test
    @DisplayName("EXECUTE IMMEDIATE literal with comma-separated FROM yields every table")
    void executeImmediateCommaFromList() {
        String sql =
            "CREATE PROCEDURE p_dyn()\n"
          + "LANGUAGE SQL\n"
          + "BEGIN\n"
          + "  EXECUTE IMMEDIATE 'SELECT * FROM alpha, beta WHERE alpha.id = beta.id';\n"
          + "END";
        Db2RoutineInfo r = engine.parse(sql, "p_dyn", null);
        List<String> dynamic = r.getTableOps().stream()
                .filter(t -> t.getDmlType() == Db2TableOpInfo.DmlType.DYNAMIC)
                .map(Db2TableOpInfo::getTableName)
                .toList();
        assertTrue(dynamic.stream().anyMatch(t -> "alpha".equalsIgnoreCase(t)), "got " + dynamic);
        assertTrue(dynamic.stream().anyMatch(t -> "beta".equalsIgnoreCase(t)),
                "beta should come from the comma FROM list, got " + dynamic);
    }

    @Test
    @DisplayName("a literal the grammar cannot attribute a table to is counted as a regex fallback")
    void regexFallbackIsCounted() {
        long before = Db2AnalysisVisitor.dynamicSqlRegexFallbacks();
        String sql =
            "CREATE PROCEDURE p_dyn2()\n"
          + "LANGUAGE SQL\n"
          + "BEGIN\n"
          + "  EXECUTE IMMEDIATE 'SELECT * FROM ' || v_prefix || '_orders';\n"
          + "END";
        engine.parse(sql, "p_dyn2", null);
        assertTrue(Db2AnalysisVisitor.dynamicSqlRegexFallbacks() > before,
                "an unparseable dynamic-SQL fragment must be counted as a regex fallback");
    }
}
