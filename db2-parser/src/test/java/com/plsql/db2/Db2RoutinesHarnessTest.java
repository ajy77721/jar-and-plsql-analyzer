package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.Db2RoutineInfo;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Diagnostic harness (not an assertion test): parses representative DB2 SQL PL routines from
 * src/test/resources/db2-routines.txt and prints per-routine ANTLR parse errors + extracted
 * facet counts, so grammar/visitor gaps are visible in one run.
 *   mvn -pl db2-parser test -Dtest=Db2RoutinesHarnessTest
 */
public class Db2RoutinesHarnessTest {

    private static final Pattern HEADER = Pattern.compile("===OBJECT:([^:]+):([A-Z_]+)===");

    @Test
    public void parseAllRoutines() throws Exception {
        String all;
        try (InputStream in = getClass().getResourceAsStream("/db2-routines.txt")) {
            if (in == null) { System.out.println("db2-routines.txt not found"); return; }
            all = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        Db2AnalysisEngine engine = new Db2AnalysisEngine();
        int total = 0, withErrors = 0;

        for (String block : all.split("===END===")) {
            Matcher m = HEADER.matcher(block);
            if (!m.find()) continue;
            String qualified = m.group(1);
            String source = block.substring(m.end()).trim();
            if (source.isEmpty()) continue;
            String name = qualified.contains(".") ? qualified.split("\\.")[1] : qualified;

            total++;
            Db2RoutineInfo r = engine.parse(source, name, null);
            int errs = r.getErrors() != null ? r.getErrors().size() : 0;
            int calls = r.getCalls() != null ? r.getCalls().size() : 0;
            int curs = r.getCursors() != null ? r.getCursors().size() : 0;
            int tabs = r.getTableOps() != null ? r.getTableOps().size() : 0;
            int dyn = r.getDynamicSql() != null ? r.getDynamicSql().size() : 0;
            int vars = r.getVariables() != null ? r.getVariables().size() : 0;
            int joins = 0;
            if (r.getTableOps() != null) {
                for (var op : r.getTableOps()) {
                    joins += op.getJoins() != null ? op.getJoins().size() : 0;
                }
            }

            System.out.printf("%s %-22s calls=%d cursors=%d tableOps=%d joins=%d dyn=%d vars=%d errs=%d%n",
                    errs > 0 ? "X" : "OK", qualified, calls, curs, tabs, joins, dyn, vars, errs);
            if (errs > 0) {
                withErrors++;
                for (String e : r.getErrors()) {
                    System.out.println("      " + (e.length() > 200 ? e.substring(0, 200) + "…" : e));
                }
            }
        }
        System.out.println("----------------------------------------------");
        System.out.printf("TOTAL=%d  clean=%d  withErrors=%d%n", total, total - withErrors, withErrors);
    }
}
