package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Not an assertion test — a diagnostic harness. Parses every object dumped from
 * analyzer-db (sales/hr/fin flows) and prints a per-object report of parse errors
 * plus extracted calls / table-ops / dynamic-SQL counts, so grammar/extraction
 * gaps are visible in one run. Run with: mvn -pl sqlserver-parser test -Dtest=FlowsParseHarnessTest
 */
public class FlowsParseHarnessTest {

    private static final Pattern HEADER =
            Pattern.compile("===OBJECT:([^:]+):([A-Z_]+)===");

    @Test
    public void parseAllFlowObjects() throws Exception {
        String all;
        try (InputStream in = getClass().getResourceAsStream("/flows-objects.txt")) {
            if (in == null) { System.out.println("flows-objects.txt not found"); return; }
            all = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }

        String[] blocks = all.split("===END===");
        SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

        // Pre-pass: collect every object name so call targets can be resolved.
        Set<String> knownNames = new HashSet<>();
        for (String block : blocks) {
            Matcher m = HEADER.matcher(block);
            if (!m.find()) continue;
            String q = m.group(1);
            String nm = q.contains(".") ? q.split("\\.")[1] : q;
            knownNames.add(nm.toUpperCase());
        }

        int total = 0, withErrors = 0, withGaps = 0;
        List<String> failing = new ArrayList<>();
        List<String> coverage = new ArrayList<>();

        for (String block : blocks) {
            Matcher m = HEADER.matcher(block);
            if (!m.find()) continue;
            String qualified = m.group(1);           // schema.name
            String typeDesc = m.group(2);
            String source = block.substring(m.end()).trim();
            if (source.isEmpty()) continue;

            String schema = qualified.contains(".") ? qualified.split("\\.")[0] : "dbo";
            String name   = qualified.contains(".") ? qualified.split("\\.")[1] : qualified;

            total++;
            SqlServerRoutineInfo info = engine.parse(source, name, schema);
            int calls = info.getCalls() != null ? info.getCalls().size() : 0;
            int tables = info.getTableOps() != null ? info.getTableOps().size() : 0;
            int dyn = info.getDynamicSql() != null ? info.getDynamicSql().size() : 0;
            int errs = info.getErrors() != null ? info.getErrors().size() : 0;

            String flag = errs > 0 ? "✘" : "✔";
            System.out.printf("%s %-28s [%-32s] calls=%d tables=%d dyn=%d errs=%d%n",
                    flag, qualified, typeDesc, calls, tables, dyn, errs);
            if (errs > 0) {
                withErrors++;
                for (String e : info.getErrors()) {
                    String trimmed = e.length() > 160 ? e.substring(0, 160) + "…" : e;
                    System.out.println("      " + trimmed);
                    failing.add(qualified + ": " + trimmed);
                }
            }

            // Coverage cross-check: source keywords vs extracted entities.
            List<CoverageCheck.Gap> gaps = CoverageCheck.gaps(source, info);
            List<String> unresolved = CoverageCheck.unresolvedCalls(info, knownNames);
            if (!gaps.isEmpty() || !unresolved.isEmpty()) {
                withGaps++;
                coverage.add(qualified);
                for (CoverageCheck.Gap g : gaps) coverage.add("    UNDER  " + g);
                for (String u : unresolved) coverage.add("    UNRESOLVED-CALL " + u);
            }
        }

        System.out.println("────────────────────────────────────────────");
        System.out.printf("TOTAL=%d  clean=%d  withErrors=%d  withCoverageGaps=%d%n",
                total, total - withErrors, withErrors, withGaps);
        if (!failing.isEmpty()) {
            System.out.println("FAILING OBJECTS:");
            failing.forEach(f -> System.out.println("  - " + f));
        }
        if (!coverage.isEmpty()) {
            System.out.println("COVERAGE GAPS (source has more than extractor found — candidates to root-cause):");
            coverage.forEach(System.out::println);
        }
    }
}
