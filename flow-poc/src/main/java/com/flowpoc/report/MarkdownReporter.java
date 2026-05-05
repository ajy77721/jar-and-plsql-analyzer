package com.flowpoc.report;

import com.flowpoc.model.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Generates a developer-readable Markdown report from the full flow analysis.
 *
 * Sections produced:
 *   1. Executive Summary   — endpoint count, severity breakdown, static/transactional split
 *   2. Static vs Transactional Summary table
 *   3. Per-endpoint detail:
 *        a. Call hierarchy (ASCII tree)
 *        b. Findings table (sorted HIGH → LOW)
 *        c. Code suggestions block
 *        d. Claude AI recommendations (if present)
 *   4. Collection index hints appendix
 *
 * Usage:
 *   new MarkdownReporter().write(results, outputStream);
 *   // or write to a file:
 *   new MarkdownReporter().writeToFile(results, Paths.get("report.md"));
 */
public class MarkdownReporter implements FlowReporter {

    @Override
    public void write(List<FlowResult> results, OutputStream out) throws IOException {
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8), true);
        writeReport(results, pw);
    }

    public void writeToFile(List<FlowResult> results, java.nio.file.Path path) throws IOException {
        try (PrintWriter pw = new PrintWriter(new FileWriter(path.toFile(), StandardCharsets.UTF_8))) {
            writeReport(results, pw);
        }
    }

    // ── Top-level ─────────────────────────────────────────────────────────────

    private void writeReport(List<FlowResult> results, PrintWriter pw) {
        pw.println("# Flow Analysis Report");
        pw.println("Generated: " + LocalDate.now());
        pw.println();

        writeExecutiveSummary(results, pw);
        writeStaticTransactionalTable(results, pw);

        for (FlowResult r : results) {
            writeEndpoint(r, pw);
        }

        writeIndexAppendix(results, pw);
    }

    // ── Executive Summary ─────────────────────────────────────────────────────

    private void writeExecutiveSummary(List<FlowResult> results, PrintWriter pw) {
        pw.println("## Executive Summary");
        pw.println();

        long high   = countBySeverity(results, OptimizationFinding.Severity.HIGH);
        long medium = countBySeverity(results, OptimizationFinding.Severity.MEDIUM);
        long low    = countBySeverity(results, OptimizationFinding.Severity.LOW);

        long staticQ = countQueriesByClass(results, ExtractedQuery.DataClass.STATIC);
        long transQ  = countQueriesByClass(results, ExtractedQuery.DataClass.TRANSACTIONAL);

        long nPlusOne = countByCategory(results, OptimizationFinding.Category.N_PLUS_ONE);
        long aggRew   = countByCategory(results, OptimizationFinding.Category.AGGREGATION_REWRITE);
        long ctrlDb   = countByCategory(results, OptimizationFinding.Category.CONTROLLER_DB_ACCESS);
        long claudeAI = countByCategory(results, OptimizationFinding.Category.OTHER);

        pw.println("| Metric | Value |");
        pw.println("|--------|-------|");
        pw.println("| Endpoints analyzed | " + results.size() + " |");
        pw.println("| 🔴 HIGH severity issues | " + high + " |");
        pw.println("| 🟡 MEDIUM severity issues | " + medium + " |");
        pw.println("| 🟢 LOW severity issues | " + low + " |");
        pw.println("| Static (cacheable) queries | " + staticQ + " |");
        pw.println("| Transactional queries | " + transQ + " |");
        pw.println("| N+1 patterns | " + nPlusOne + " |");
        pw.println("| Aggregation rewrite opportunities | " + aggRew + " |");
        pw.println("| Controller direct-DB violations | " + ctrlDb + " |");
        pw.println("| Claude AI suggestions | " + claudeAI + " |");
        pw.println();

        if (high > 0) {
            pw.println("> **⚠️ Action required:** " + high + " HIGH severity issue"
                    + (high > 1 ? "s" : "") + " detected. Address these before production.");
            pw.println();
        }
        if (staticQ > 0) {
            pw.println("> **💡 Quick win:** " + staticQ + " quer" + (staticQ > 1 ? "ies" : "y")
                    + " can be cached at startup — eliminating repeated DB round-trips.");
            pw.println();
        }
    }

    // ── Static vs Transactional table ────────────────────────────────────────

    private void writeStaticTransactionalTable(List<FlowResult> results, PrintWriter pw) {
        pw.println("## Static vs Transactional Query Split");
        pw.println();
        pw.println("| Endpoint | Static | Transactional | Writes | Claude AI |");
        pw.println("|----------|--------|--------------|--------|-----------|");

        for (FlowResult r : results) {
            long s  = r.allSteps().stream().flatMap(st -> st.getQueries().stream())
                        .filter(q -> q.getDataClass() == ExtractedQuery.DataClass.STATIC).count();
            long t  = r.allSteps().stream().flatMap(st -> st.getQueries().stream())
                        .filter(q -> q.getDataClass() == ExtractedQuery.DataClass.TRANSACTIONAL).count();
            long w  = r.allSteps().stream().flatMap(st -> st.getQueries().stream())
                        .filter(q -> q.getType() == ExtractedQuery.QueryType.INSERT
                                  || q.getType() == ExtractedQuery.QueryType.UPDATE
                                  || q.getType() == ExtractedQuery.QueryType.DELETE).count();
            long ai = r.getOptimizations().stream()
                        .filter(f -> f.getCategory() == OptimizationFinding.Category.OTHER).count();

            pw.println("| `" + r.getEndpointMethod() + " " + r.getEndpointPath() + "` | "
                    + s + " | " + t + " | " + w + " | " + ai + " |");
        }
        pw.println();
    }

    // ── Per-endpoint ──────────────────────────────────────────────────────────

    private void writeEndpoint(FlowResult r, PrintWriter pw) {
        pw.println("---");
        pw.println("## `" + r.getEndpointMethod() + " " + r.getEndpointPath() + "`");
        pw.println();

        writeCallHierarchy(r, pw);
        writeFindingsTable(r, pw);
        writeCodeSuggestions(r, pw);
        writeClaudeSection(r, pw);
    }

    private void writeCallHierarchy(FlowResult r, PrintWriter pw) {
        pw.println("### Call Hierarchy");
        pw.println("```");
        printStep(r.getRootStep(), "", true, pw);
        pw.println("```");
        pw.println();
    }

    private void printStep(FlowStep step, String prefix, boolean isLast, PrintWriter pw) {
        String connector = isLast ? "└─" : "├─";
        String kind  = "[" + step.getKind().name().charAt(0) + "] ";
        String label = step.label() + "." + step.getMethodName() + "()";

        StringBuilder line = new StringBuilder(prefix).append(connector).append(kind).append(label);

        // Inline DB operations
        for (ExtractedQuery eq : step.getQueries()) {
            String dc = eq.getDataClass() == ExtractedQuery.DataClass.STATIC ? " ← STATIC"
                      : eq.getDataClass() == ExtractedQuery.DataClass.TRANSACTIONAL ? ""
                      : "";
            line.append("\n").append(prefix).append(isLast ? "   " : "│  ")
                .append("    ").append(eq.getType().name())
                .append(" ").append(eq.getTableName() != null ? eq.getTableName() : "?")
                .append(dc);
        }

        pw.println(line);

        List<FlowStep> children = step.getChildren();
        for (int i = 0; i < children.size(); i++) {
            String childPrefix = prefix + (isLast ? "   " : "│  ");
            printStep(children.get(i), childPrefix, i == children.size() - 1, pw);
        }
    }

    private void writeFindingsTable(FlowResult r, PrintWriter pw) {
        List<OptimizationFinding> findings = r.getOptimizations().stream()
                .filter(f -> f.getCategory() != OptimizationFinding.Category.OTHER) // Claude in own section
                .sorted(Comparator.comparing(f -> severityOrder(f.getSeverity())))
                .collect(Collectors.toList());

        if (findings.isEmpty()) {
            pw.println("### Optimization Findings");
            pw.println("No issues detected.");
            pw.println();
            return;
        }

        pw.println("### Optimization Findings");
        pw.println();
        pw.println("| Severity | Category | Collection | Description |");
        pw.println("|----------|---------|-----------|-------------|");

        for (OptimizationFinding f : findings) {
            String sev  = severityBadge(f.getSeverity());
            String cat  = f.getCategory().name().replace("_", " ");
            String col  = f.getTable() != null ? "`" + f.getTable() + "`" : "-";
            String desc = f.getDescription().replace("|", "\\|");
            pw.println("| " + sev + " | " + cat + " | " + col + " | " + desc + " |");
        }
        pw.println();
    }

    private void writeCodeSuggestions(FlowResult r, PrintWriter pw) {
        List<OptimizationFinding> withCode = r.getOptimizations().stream()
                .filter(f -> f.getCategory() != OptimizationFinding.Category.OTHER)
                .filter(f -> f.getSuggestedCode() != null && !f.getSuggestedCode().isBlank())
                .sorted(Comparator.comparing(f -> severityOrder(f.getSeverity())))
                .collect(Collectors.toList());

        if (withCode.isEmpty()) return;

        pw.println("### Suggested Changes");
        pw.println();
        for (OptimizationFinding f : withCode) {
            pw.println("**" + severityBadge(f.getSeverity()) + " " + f.getCategory().name().replace("_"," ")
                    + "** — `" + f.getTable() + "`");
            pw.println("```");
            pw.println(f.getSuggestedCode());
            pw.println("```");
            pw.println();
        }
    }

    private void writeClaudeSection(FlowResult r, PrintWriter pw) {
        List<OptimizationFinding> aiFindings = r.getOptimizations().stream()
                .filter(f -> f.getCategory() == OptimizationFinding.Category.OTHER)
                .collect(Collectors.toList());

        if (aiFindings.isEmpty()) return;

        pw.println("### 🤖 Claude AI Recommendations");
        pw.println();
        for (OptimizationFinding f : aiFindings) {
            String badge = severityBadge(f.getSeverity());
            pw.println("> **" + badge + "** " + f.getDescription().replace("[Claude AI] ", ""));
            if (f.getSuggestedCode() != null && !f.getSuggestedCode().isBlank()) {
                pw.println("> ```");
                pw.println("> " + f.getSuggestedCode());
                pw.println("> ```");
            }
            pw.println();
        }
    }

    // ── Index appendix ────────────────────────────────────────────────────────

    private void writeIndexAppendix(List<FlowResult> results, PrintWriter pw) {
        // Collect all MISSING_INDEX findings grouped by collection
        Map<String, Set<String>> indexNeeded = new LinkedHashMap<>();
        for (FlowResult r : results) {
            for (OptimizationFinding f : r.getOptimizations()) {
                if (f.getCategory() == OptimizationFinding.Category.MISSING_INDEX
                        && f.getTable() != null && f.getColumn() != null) {
                    indexNeeded.computeIfAbsent(f.getTable(), k -> new LinkedHashSet<>())
                               .add(f.getColumn());
                }
            }
        }

        if (indexNeeded.isEmpty()) return;

        pw.println("---");
        pw.println("## Appendix: Recommended Indexes");
        pw.println();
        pw.println("Run these in MongoDB shell to create missing indexes:");
        pw.println("```js");
        for (Map.Entry<String, Set<String>> entry : indexNeeded.entrySet()) {
            for (String col : entry.getValue()) {
                pw.println("db." + entry.getKey() + ".createIndex({ " + col + ": 1 })");
            }
        }
        pw.println("```");
        pw.println();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private long countBySeverity(List<FlowResult> results, OptimizationFinding.Severity s) {
        return results.stream().flatMap(r -> r.getOptimizations().stream())
                .filter(f -> f.getSeverity() == s).count();
    }

    private long countByCategory(List<FlowResult> results, OptimizationFinding.Category c) {
        return results.stream().flatMap(r -> r.getOptimizations().stream())
                .filter(f -> f.getCategory() == c).count();
    }

    private long countQueriesByClass(List<FlowResult> results, ExtractedQuery.DataClass dc) {
        return results.stream().flatMap(r -> r.allSteps().stream())
                .flatMap(s -> s.getQueries().stream())
                .filter(q -> q.getDataClass() == dc).count();
    }

    private int severityOrder(OptimizationFinding.Severity s) {
        return switch (s) { case HIGH -> 0; case MEDIUM -> 1; case LOW -> 2; };
    }

    private String severityBadge(OptimizationFinding.Severity s) {
        return switch (s) { case HIGH -> "🔴 HIGH"; case MEDIUM -> "🟡 MEDIUM"; case LOW -> "🟢 LOW"; };
    }
}
