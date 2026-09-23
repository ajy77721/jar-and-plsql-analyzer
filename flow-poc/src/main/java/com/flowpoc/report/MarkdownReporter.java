/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.report;

import com.flowpoc.model.ExtractedQuery;
import com.flowpoc.model.FlowResult;
import com.flowpoc.model.FlowStep;
import com.flowpoc.model.OptimizationFinding;
import com.flowpoc.model.Predicate;
import com.flowpoc.model.TestDataSet;
import com.flowpoc.report.FlowReporter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MarkdownReporter
implements FlowReporter {
    private final String mode;

    public MarkdownReporter() {
        this.mode = "optimize";
    }

    public MarkdownReporter(String mode) {
        this.mode = mode != null ? mode : "optimize";
    }

    @Override
    public void write(List<FlowResult> results, OutputStream out) throws IOException {
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8), true);
        if ("testdata".equals(this.mode)) {
            this.writeTestDataReport(results, pw);
        } else {
            this.writeOptimizeReport(results, pw);
        }
    }

    private void writeTestDataReport(List<FlowResult> results, PrintWriter pw) {
        pw.println("# Test Data Preparation Report");
        pw.println("Generated: " + String.valueOf(LocalDate.now()));
        pw.println();
        int totalDatasets = results.stream().mapToInt(r -> r.getTestDataSets().size()).sum();
        long happyCases = this.countTestCaseVariant(results, TestDataSet.TestCase.Variant.HAPPY_PATH);
        long failureCases = this.countAllTestCases(results) - happyCases;
        pw.println("## Summary");
        pw.println();
        pw.println("| Metric | Value |");
        pw.println("|--------|-------|");
        pw.println("| Endpoints | " + results.size() + " |");
        pw.println("| DB steps with datasets | " + totalDatasets + " |");
        pw.println("| \u2705 Happy-path test cases | " + happyCases + " |");
        pw.println("| \u26a0\ufe0f Failure / edge-case test cases | " + failureCases + " |");
        pw.println();
        for (FlowResult r2 : results) {
            this.writeTestDataEndpoint(r2, pw);
        }
    }

    private void writeTestDataEndpoint(FlowResult r, PrintWriter pw) {
        pw.println("---");
        pw.println("## `" + r.getEndpointMethod() + " " + r.getEndpointPath() + "`");
        pw.println();
        pw.println("### Flow Overview");
        pw.println("```");
        this.printStep(r.getRootStep(), "", true, pw);
        pw.println("```");
        pw.println();
        List<TestDataSet> datasets = r.getTestDataSets();
        if (datasets.isEmpty()) {
            pw.println("> No DB operations detected in static analysis for this endpoint.");
            pw.println("> Common causes: JPA `save()`/`find()` with no explicit `@Query`, or dynamic queries built at runtime.");
            pw.println();
            return;
        }
        pw.println("### Prepared Test Datasets");
        pw.println();
        int idx = 1;
        for (TestDataSet ds2 : datasets) {
            this.writeDataset(ds2, idx++, pw);
        }
        List<TestDataSet.TestCase> allCases = datasets.stream().flatMap(ds -> ds.getTestCases().stream()).collect(Collectors.toList());
        if (!allCases.isEmpty()) {
            this.writeTestCases(allCases, pw);
        }
    }

    private void writeDataset(TestDataSet ds, int idx, PrintWriter pw) {
        List<Map<String, Object>> rows;
        List<Predicate> preds;
        pw.println("#### Dataset " + idx + " \u2014 `" + ds.getQueryStep() + "`");
        if (ds.getTable() != null && !ds.getTable().isBlank()) {
            pw.println("Collection / Table: **`" + ds.getTable() + "`**");
        }
        pw.println();
        if (ds.getSql() != null && !ds.getSql().isBlank()) {
            pw.println("**Query:**");
            pw.println("```");
            pw.println(ds.getSql());
            pw.println("```");
            pw.println();
        }
        if ((preds = ds.getAppliedPredicates()) != null && !preds.isEmpty()) {
            pw.println("**Filter Predicates:**");
            pw.println();
            pw.println("| Field | Operator | Value |");
            pw.println("|-------|---------|-------|");
            for (Predicate p : preds) {
                pw.println("| `" + p.getColumn() + "` | " + p.getOp().name() + " | `" + (p.getResolvedValue() != null ? p.getResolvedValue() : p.getRawValue()) + "` |");
            }
            pw.println();
        }
        if ((rows = ds.getSampleRows()) != null && !rows.isEmpty()) {
            pw.println("**Sample Records (real DB \u2014 " + rows.size() + " row" + (rows.size() != 1 ? "s" : "") + "):**");
            pw.println();
            this.writeRowsAsTable(rows, pw);
            pw.println("**Setup Script** (insert these records before running tests):");
            pw.println("```js");
            this.writeInsertScript(ds.getTable(), rows, pw);
            pw.println("```");
            pw.println();
        } else {
            pw.println("> No sample records fetched (MongoDB not connected or no matching data).");
            pw.println("> Connect MongoDB to get real document shapes and field values.");
            pw.println();
        }
        Map<String, String> bindings = ds.getDownstreamBindings();
        if (bindings != null && !bindings.isEmpty()) {
            pw.println("**Passes to next step:**");
            pw.println();
            pw.println("| This step output field | \u2192 Next step input |");
            pw.println("|------------------------|------------------|");
            for (Map.Entry<String, String> e : bindings.entrySet()) {
                pw.println("| `" + e.getKey() + "` | `" + e.getValue() + "` |");
            }
            pw.println();
        }
    }

    private void writeRowsAsTable(List<Map<String, Object>> rows, PrintWriter pw) {
        if (rows.isEmpty()) {
            return;
        }
        LinkedHashSet<String> keys = new LinkedHashSet<String>();
        for (Map<String, Object> row : rows) {
            keys.addAll(row.keySet());
        }
        List<String> cols = new ArrayList<String>(keys);
        pw.print("| ");
        for (String string : cols) {
            pw.print(string + " | ");
        }
        pw.println();
        pw.print("| ");
        for (String string : cols) {
            pw.print("--- | ");
        }
        pw.println();
        for (Map map : rows) {
            pw.print("| ");
            for (String col : cols) {
                Object val = map.get(col);
                pw.print((val != null ? String.valueOf(val).replace("|", "\\|") : "\u2014") + " | ");
            }
            pw.println();
        }
        pw.println();
    }

    private void writeInsertScript(String table, List<Map<String, Object>> rows, PrintWriter pw) {
        String coll;
        String string = coll = table != null ? table : "collection";
        if (rows.size() == 1) {
            pw.println("db." + coll + ".insertOne(" + this.rowToJson(rows.get(0)) + ");");
        } else {
            pw.println("db." + coll + ".insertMany([");
            for (int i = 0; i < rows.size(); ++i) {
                pw.print("  " + this.rowToJson(rows.get(i)));
                pw.println(i < rows.size() - 1 ? "," : "");
            }
            pw.println("]);");
        }
    }

    private String rowToJson(Map<String, Object> row) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> e : row.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append("\"").append(e.getKey()).append("\": ");
            Object v = e.getValue();
            if (v == null) {
                sb.append("null");
                continue;
            }
            if (v instanceof Number || v instanceof Boolean) {
                sb.append(v);
                continue;
            }
            sb.append("\"").append(String.valueOf(v).replace("\"", "\\\"")).append("\"");
        }
        sb.append("}");
        return sb.toString();
    }

    private void writeTestCases(List<TestDataSet.TestCase> cases, PrintWriter pw) {
        pw.println("### Test Cases");
        pw.println();
        List<TestDataSet.TestCase> happyPath = cases.stream().filter(c -> c.getVariant() == TestDataSet.TestCase.Variant.HAPPY_PATH).collect(Collectors.toList());
        List<TestDataSet.TestCase> failureCases = cases.stream().filter(c -> c.getVariant() != TestDataSet.TestCase.Variant.HAPPY_PATH).collect(Collectors.toList());
        if (!happyPath.isEmpty()) {
            pw.println("#### \u2705 Happy Path Cases");
            pw.println();
            for (TestDataSet.TestCase tc : happyPath) {
                this.writeSingleTestCase(tc, "\u2705", pw);
            }
        }
        if (!failureCases.isEmpty()) {
            pw.println("#### \u26a0\ufe0f Failure / Edge Cases");
            pw.println();
            for (TestDataSet.TestCase tc : failureCases) {
                String icon = switch (tc.getVariant()) {
                    case EMPTY_RESULT -> "\u2b1c";
                    case NULL_VALUE -> "\ud83d\udeab";
                    case MAX_VALUE, MIN_VALUE, BOUNDARY -> "\u26a1";
                    default -> "\u26a0\ufe0f";
                };
                this.writeSingleTestCase(tc, icon, pw);
            }
        }
    }

    private void writeSingleTestCase(TestDataSet.TestCase tc, String icon, PrintWriter pw) {
        pw.println("##### " + icon + " " + tc.getVariant().name() + " \u2014 " + tc.getDescription());
        pw.println();
        Map<String, Object> input = tc.getInputData();
        Map<String, Object> output = tc.getExpectedOutput();
        if (input != null && !input.isEmpty()) {
            pw.println("**Input data:**");
            pw.println("```json");
            pw.println(this.rowToJson(input));
            pw.println("```");
        }
        if (output != null && !output.isEmpty()) {
            pw.println("**Expected output:**");
            pw.println("```json");
            pw.println(this.rowToJson(output));
            pw.println("```");
        }
        pw.println();
    }

    private void writeOptimizeReport(List<FlowResult> results, PrintWriter pw) {
        pw.println("# Optimization Analysis Report");
        pw.println("Generated: " + String.valueOf(LocalDate.now()));
        pw.println();
        this.writeExecutiveSummary(results, pw);
        this.writeWhatWhereTable(results, pw);
        for (FlowResult r : results) {
            this.writeOptimizeEndpoint(r, pw);
        }
        this.writeIndexAppendix(results, pw);
        this.writeClaudePromptSection(results, pw);
    }

    private void writeExecutiveSummary(List<FlowResult> results, PrintWriter pw) {
        pw.println("## Executive Summary");
        pw.println();
        long high = this.countBySeverity(results, OptimizationFinding.Severity.HIGH);
        long medium = this.countBySeverity(results, OptimizationFinding.Severity.MEDIUM);
        long low = this.countBySeverity(results, OptimizationFinding.Severity.LOW);
        long staticQ = this.countQueriesByClass(results, ExtractedQuery.DataClass.STATIC);
        long transQ = this.countQueriesByClass(results, ExtractedQuery.DataClass.TRANSACTIONAL);
        pw.println("| Metric | Value |");
        pw.println("|--------|-------|");
        pw.println("| Endpoints analyzed | " + results.size() + " |");
        pw.println("| \ud83d\udd34 HIGH severity issues | " + high + " |");
        pw.println("| \ud83d\udfe1 MEDIUM severity issues | " + medium + " |");
        pw.println("| \ud83d\udfe2 LOW severity issues | " + low + " |");
        pw.println("| Static (cacheable) queries | " + staticQ + " |");
        pw.println("| Transactional queries | " + transQ + " |");
        pw.println();
        if (high > 0L) {
            pw.println("> **\u26a0\ufe0f Action required:** " + high + " HIGH severity issue" + (high > 1L ? "s" : "") + " detected. Address these before production.");
            pw.println();
        }
        if (staticQ > 0L) {
            pw.println("> **\ud83d\udca1 Quick win:** " + staticQ + " quer" + (staticQ > 1L ? "ies" : "y") + " can be cached at startup \u2014 eliminating repeated DB round-trips.");
            pw.println();
        }
        if (high + medium + low == 0L) {
            pw.println("> No optimization issues detected. The static call tree has no explicit DB operations");
            pw.println("> (common for JPA `save()`/derived-method endpoints). Run against an endpoint with");
            pw.println("> `@Query` annotations or explicit SQL to get findings.");
            pw.println();
        }
    }

    private void writeWhatWhereTable(List<FlowResult> results, PrintWriter pw) {
        List<OptimizationFinding> all = results.stream().flatMap(r -> r.getOptimizations().stream().filter(f -> f.getCategory() != OptimizationFinding.Category.OTHER)).sorted(Comparator.comparing(f -> this.severityOrder(f.getSeverity()))).collect(Collectors.toList());
        if (all.isEmpty()) {
            return;
        }
        pw.println("## What & Where to Optimize");
        pw.println();
        pw.println("| # | Severity | What | Where | Collection |");
        pw.println("|---|---------|------|-------|------------|");
        int n = 1;
        for (OptimizationFinding f2 : all) {
            String sev = this.severityBadge(f2.getSeverity());
            String what = f2.getCategory().name().replace("_", " ");
            String where = f2.getLocation() != null ? "`" + this.simplify(f2.getLocation()) + "`" : "\u2014";
            String coll = f2.getTable() != null ? "`" + f2.getTable() + "`" : "\u2014";
            pw.println("| " + n++ + " | " + sev + " | " + what + " | " + where + " | " + coll + " |");
        }
        pw.println();
    }

    private void writeOptimizeEndpoint(FlowResult r, PrintWriter pw) {
        pw.println("---");
        pw.println("## `" + r.getEndpointMethod() + " " + r.getEndpointPath() + "`");
        pw.println();
        this.writeCallHierarchy(r, pw);
        this.writeDetailedFindings(r, pw);
        this.writeClaudeSection(r, pw);
    }

    private void writeCallHierarchy(FlowResult r, PrintWriter pw) {
        pw.println("### Call Hierarchy");
        pw.println("```");
        this.printStep(r.getRootStep(), "", true, pw);
        pw.println("```");
        pw.println();
    }

    private void writeDetailedFindings(FlowResult r, PrintWriter pw) {
        List<OptimizationFinding> findings = r.getOptimizations().stream().filter(f -> f.getCategory() != OptimizationFinding.Category.OTHER).sorted(Comparator.comparing(f -> this.severityOrder(f.getSeverity()))).collect(Collectors.toList());
        if (findings.isEmpty()) {
            pw.println("### Optimization Findings");
            pw.println("No issues detected for this endpoint.");
            pw.println();
            return;
        }
        pw.println("### Detailed Fix Guide");
        pw.println();
        int n = 1;
        for (OptimizationFinding f2 : findings) {
            pw.println("#### " + n++ + ". " + this.severityBadge(f2.getSeverity()) + " \u2014 " + f2.getCategory().name().replace("_", " "));
            pw.println();
            pw.println("**What:** " + f2.getDescription().replace("|", "\\|"));
            pw.println();
            if (f2.getLocation() != null && !f2.getLocation().isBlank()) {
                pw.println("**Where:** `" + f2.getLocation() + "`");
                pw.println();
            }
            if (f2.getTable() != null) {
                String col = f2.getColumn() != null ? "." + f2.getColumn() : "";
                pw.println("**Collection / Table:** `" + f2.getTable() + col + "`");
                pw.println();
            }
            if (f2.getEvidence() != null && !f2.getEvidence().isBlank()) {
                pw.println("**Evidence:**");
                pw.println("```");
                pw.println(f2.getEvidence());
                pw.println("```");
                pw.println();
            }
            if (f2.getSuggestedCode() != null && !f2.getSuggestedCode().isBlank()) {
                pw.println("**How to fix:**");
                pw.println("```");
                pw.println(f2.getSuggestedCode());
                pw.println("```");
                pw.println();
            }
            if (f2.getClaudeInsight() == null || f2.getClaudeInsight().isBlank()) continue;
            pw.println("**\ud83e\udd16 Claude AI insight:**");
            pw.println();
            for (String line : f2.getClaudeInsight().split("\n")) {
                pw.println("> " + line.trim());
            }
            pw.println();
        }
    }

    private void writeClaudeSection(FlowResult r, PrintWriter pw) {
    }

    private void writeIndexAppendix(List<FlowResult> results, PrintWriter pw) {
        LinkedHashMap<String, Set<String>> indexNeeded = new LinkedHashMap<String, Set<String>>();
        for (FlowResult flowResult : results) {
            for (OptimizationFinding f : flowResult.getOptimizations()) {
                if (f.getCategory() != OptimizationFinding.Category.MISSING_INDEX || f.getTable() == null || f.getColumn() == null) continue;
                indexNeeded.computeIfAbsent(f.getTable(), k -> new LinkedHashSet<String>()).add(f.getColumn());
            }
        }
        if (indexNeeded.isEmpty()) {
            return;
        }
        pw.println("---");
        pw.println("## Appendix: Recommended Indexes");
        pw.println();
        pw.println("Run in MongoDB shell:");
        pw.println("```js");
        for (Map.Entry<String, Set<String>> entry : indexNeeded.entrySet()) {
            for (String col : entry.getValue()) {
                pw.println("db." + entry.getKey() + ".createIndex({ " + col + ": 1 })");
            }
        }
        pw.println("```");
        pw.println();
    }

    private void writeClaudePromptSection(List<FlowResult> results, PrintWriter pw) {
        long medium;
        long high = this.countBySeverity(results, OptimizationFinding.Severity.HIGH);
        if (high + (medium = this.countBySeverity(results, OptimizationFinding.Severity.MEDIUM)) == 0L) {
            return;
        }
        pw.println("---");
        pw.println("## \ud83e\udd16 How to Use This Report with Claude");
        pw.println();
        pw.println("Copy the prompt below and paste it into a Claude conversation along with this");
        pw.println("report to receive specific, copy-paste-ready Java and MongoDB code fixes.");
        pw.println();
        pw.println("---");
        pw.println();
        pw.println("```");
        pw.println("You are a senior Spring Boot + MongoDB performance engineer.");
        pw.println("I have attached a static analysis report for my application.");
        pw.println("For each HIGH and MEDIUM severity finding, provide:");
        pw.println();
        pw.println("1. The exact Java code change (show the before/after method, or a unified diff).");
        pw.println("   - Use Spring Data / MongoTemplate idioms where possible.");
        pw.println("   - Keep the fix minimal \u2014 don't restructure unrelated code.");
        pw.println();
        pw.println("2. The MongoDB index to create (mongo shell command) if the fix requires one.");
        pw.println();
        pw.println("3. A one-line explanation of why this fix improves performance.");
        pw.println();
        pw.println("Group your answer by endpoint (## section header matching the report).");
        pw.println("Skip endpoints with no issues.");
        pw.println();
        pw.println("--- BEGIN REPORT ---");
        pw.println();
        List<OptimizationFinding> allFindings = results.stream().flatMap(r -> r.getOptimizations().stream().filter(f -> f.getCategory() != OptimizationFinding.Category.OTHER && f.getSeverity() != OptimizationFinding.Severity.LOW)).sorted(Comparator.comparing(f -> this.severityOrder(f.getSeverity()))).collect(Collectors.toList());
        for (OptimizationFinding f2 : allFindings) {
            pw.println("[" + f2.getSeverity().name() + "] " + f2.getCategory().name().replace("_", " ") + " | " + this.simplify(f2.getLocation()) + " | collection=" + (f2.getTable() != null ? f2.getTable() : "?"));
            pw.println("  Description: " + f2.getDescription());
            if (f2.getSuggestedCode() != null && !f2.getSuggestedCode().isBlank()) {
                pw.println("  Suggested fix: " + f2.getSuggestedCode().replace("\n", " | "));
            }
            if (f2.getEvidence() != null && !f2.getEvidence().isBlank()) {
                pw.println("  Evidence: " + f2.getEvidence());
            }
            pw.println();
        }
        pw.println("--- END REPORT ---");
        pw.println("```");
        pw.println();
    }

    private void printStep(FlowStep step, String prefix, boolean isLast, PrintWriter pw) {
        String connector = isLast ? "\u2514\u2500" : "\u251c\u2500";
        String kind = "[" + step.getKind().name().charAt(0) + "] ";
        String label = step.label() + "." + step.getMethodName() + "()";
        StringBuilder line = new StringBuilder(prefix).append(connector).append(kind).append(label);
        for (ExtractedQuery eq : step.getQueries()) {
            String dc = eq.getDataClass() == ExtractedQuery.DataClass.STATIC ? " \u2190 STATIC" : (eq.getDataClass() == ExtractedQuery.DataClass.TRANSACTIONAL ? " \u2190 TRANSACTIONAL" : "");
            line.append("\n").append(prefix).append(isLast ? "   " : "\u2502  ").append("    \u21b3 ").append(eq.getType().name()).append(" ").append(eq.getTableName() != null ? eq.getTableName() : "?").append(dc);
        }
        pw.println(line);
        List<FlowStep> children = step.getChildren();
        for (int i = 0; i < children.size(); ++i) {
            String childPrefix = prefix + (isLast ? "   " : "\u2502  ");
            this.printStep(children.get(i), childPrefix, i == children.size() - 1, pw);
        }
    }

    private long countBySeverity(List<FlowResult> results, OptimizationFinding.Severity s) {
        return results.stream().flatMap(r -> r.getOptimizations().stream()).filter(f -> f.getSeverity() == s).count();
    }

    private long countQueriesByClass(List<FlowResult> results, ExtractedQuery.DataClass dc) {
        return results.stream().flatMap(r -> r.allSteps().stream()).flatMap(s -> s.getQueries().stream()).filter(q -> q.getDataClass() == dc).count();
    }

    private long countTestCaseVariant(List<FlowResult> results, TestDataSet.TestCase.Variant v) {
        return results.stream().flatMap(r -> r.getTestDataSets().stream()).flatMap(ds -> ds.getTestCases().stream()).filter(tc -> tc.getVariant() == v).count();
    }

    private long countAllTestCases(List<FlowResult> results) {
        return results.stream().flatMap(r -> r.getTestDataSets().stream()).mapToLong(ds -> ds.getTestCases().size()).sum();
    }

    private int severityOrder(OptimizationFinding.Severity s) {
        return switch (s) {
            default -> throw new IncompatibleClassChangeError();
            case HIGH -> 0;
            case MEDIUM -> 1;
            case LOW -> 2;
        };
    }

    private String severityBadge(OptimizationFinding.Severity s) {
        return switch (s) {
            default -> throw new IncompatibleClassChangeError();
            case HIGH -> "\ud83d\udd34 HIGH";
            case MEDIUM -> "\ud83d\udfe1 MEDIUM";
            case LOW -> "\ud83d\udfe2 LOW";
        };
    }

    private String simplify(String location) {
        if (location == null) {
            return "";
        }
        String[] parts = location.split("\\.");
        if (parts.length >= 2) {
            return parts[parts.length - 2] + "." + parts[parts.length - 1];
        }
        return location;
    }
}

