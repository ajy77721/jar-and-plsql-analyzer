package com.plsql.parser.service;

import com.plsqlanalyzer.parser.model.SqlAnalysisResult;
import com.plsqlanalyzer.parser.service.SqlAnalyzer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * How much real DML the shared analyzer actually parses.
 *
 * <p>The cross-dialect design rests on one claim: JSqlParser handles enough ANSI DML that a single
 * enricher can serve all five datasources. That is a measurement, not an assumption, and this is
 * the measurement — run over the PL/SQL sources on this machine rather than over invented examples,
 * which would only prove that the examples were chosen to pass.
 *
 * <p>It reports rather than asserts a threshold. A number that has to be met turns into a number
 * that gets adjusted; the useful output is the figure and the sample of what failed, so a dialect
 * with poor coverage is a visible signal to add dialect-specific handling.
 */
class RealSourceCoverageTest {

    private static final Path SOURCES = Path.of("../data/plsql-parse");

    /** Statement starts. Deliberately crude: the point is to find candidate DML, not to parse it. */
    private static final Pattern DML = Pattern.compile(
            "(?is)\\b(SELECT|INSERT\\s+INTO|UPDATE|DELETE\\s+FROM|MERGE\\s+INTO)\\b");

    @Test
    @DisplayName("measure ANSI-parse coverage over the real PL/SQL sources on this machine")
    void measureCoverage() throws IOException {
        assumeTrue(Files.isDirectory(SOURCES),
                "no data/plsql-parse on this machine — coverage can only be measured against real sources");

        List<Path> files;
        try (var walk = Files.walk(SOURCES)) {
            files = walk.filter(Files::isRegularFile)
                    .filter(p -> p.getParent() != null && p.getParent().getFileName().toString().equals("sources"))
                    .toList();
        }
        assumeTrue(!files.isEmpty(), "no source files under data/plsql-parse/*/sources");

        SqlAnalyzer analyzer = new SqlAnalyzer();
        Map<String, int[]> byKind = new TreeMap<>();   // kind -> {attempted, parsed}
        List<String> failures = new ArrayList<>();
        int attempted = 0, parsed = 0, withColumns = 0;

        for (Path f : files) {
            String src;
            try {
                src = Files.readString(f);
            } catch (Exception skip) {
                continue;   // a binary or oddly-encoded file is not a coverage result
            }
            for (String stmt : candidateStatements(src)) {
                String kind = kindOf(stmt);
                attempted++;
                byKind.computeIfAbsent(kind, k -> new int[2])[0]++;
                SqlAnalysisResult r = analyzer.analyze(stmt, 0);
                if (r.getOperationType() != null) {
                    parsed++;
                    byKind.get(kind)[1]++;
                    if (!r.getColumnUsages().isEmpty()) withColumns++;
                } else if (failures.size() < 12) {
                    failures.add(oneLine(stmt));
                }
            }
        }

        System.out.println();
        System.out.println("  ANSI-parse coverage over " + files.size() + " real source files");
        System.out.printf("    candidate statements : %d%n", attempted);
        System.out.printf("    parsed               : %d  (%.1f%%)%n",
                parsed, attempted == 0 ? 0 : parsed * 100.0 / attempted);
        System.out.printf("    with column detail   : %d  (%.1f%%)%n",
                withColumns, attempted == 0 ? 0 : withColumns * 100.0 / attempted);
        System.out.println("    by statement kind:");
        byKind.forEach((k, v) -> System.out.printf("      %-12s %5d attempted  %5d parsed  (%.1f%%)%n",
                k, v[0], v[1], v[0] == 0 ? 0 : v[1] * 100.0 / v[0]));
        if (!failures.isEmpty()) {
            System.out.println("    sample of what did NOT parse — this is where dialect-specific");
            System.out.println("    handling would be needed if the percentage is unacceptable:");
            for (String s : failures) System.out.println("      " + s);
        }
        System.out.println();
    }

    /**
     * Slices candidate statements out of PL/SQL source.
     *
     * <p>From a DML keyword to the next semicolon, tracking quotes so a semicolon inside a literal
     * does not end the statement. This is the same rule the dialect visitors use to capture text,
     * so the coverage figure reflects what they will actually hand the enricher.
     */
    private List<String> candidateStatements(String src) {
        List<String> out = new ArrayList<>();
        Matcher m = DML.matcher(src);
        while (m.find()) {
            int start = m.start();
            int i = start;
            char quote = 0;
            while (i < src.length()) {
                char c = src.charAt(i);
                if (quote != 0) {
                    if (c == quote) quote = 0;
                } else if (c == '\'' || c == '"') {
                    quote = c;
                } else if (c == ';') {
                    break;
                }
                i++;
            }
            String stmt = src.substring(start, Math.min(i, src.length())).trim();
            // Skip the trivially tiny and the absurdly large; neither is a fair coverage sample.
            if (stmt.length() > 12 && stmt.length() < 8000) out.add(stmt);
            m.region(Math.min(i + 1, src.length()), src.length());
        }
        return out;
    }

    private String kindOf(String stmt) {
        String u = stmt.trim().toUpperCase(Locale.ROOT);
        if (u.startsWith("SELECT")) return "SELECT";
        if (u.startsWith("INSERT")) return "INSERT";
        if (u.startsWith("UPDATE")) return "UPDATE";
        if (u.startsWith("DELETE")) return "DELETE";
        if (u.startsWith("MERGE")) return "MERGE";
        return "OTHER";
    }

    private String oneLine(String s) {
        String t = s.replaceAll("\\s+", " ").trim();
        return t.length() > 150 ? t.substring(0, 150) + " …" : t;
    }
}
