/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  com.fasterxml.jackson.databind.Module
 *  com.fasterxml.jackson.databind.ObjectMapper
 *  com.fasterxml.jackson.databind.SerializationFeature
 *  com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
 */
package com.flowpoc.report;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.flowpoc.model.ClaudeCallRecord;
import com.flowpoc.model.ExtractedQuery;
import com.flowpoc.model.FlowResult;
import com.flowpoc.model.FlowStep;
import com.flowpoc.model.OptimizationFinding;
import com.flowpoc.model.Predicate;
import com.flowpoc.model.TestDataSet;
import com.flowpoc.report.FlowReporter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JsonReporter
implements FlowReporter {
    private final ObjectMapper mapper = new ObjectMapper().registerModule((Module)new JavaTimeModule()).enable(SerializationFeature.INDENT_OUTPUT);

    @Override
    public void write(List<FlowResult> results, OutputStream out) throws IOException {
        ArrayList<Map<String, Object>> payload = new ArrayList<Map<String, Object>>();
        for (FlowResult r : results) {
            payload.add(this.serialize(r));
        }
        this.mapper.writeValue(out, payload);
    }

    private Map<String, Object> serialize(FlowResult r) {
        LinkedHashMap<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("endpoint", r.getEndpointMethod() + " " + r.getEndpointPath());
        m.put("optimizations", this.serializeOptimizations(r.getOptimizations()));
        m.put("testDataSets", this.serializeTestDataSets(r.getTestDataSets()));
        m.put("flowSteps", this.serializeStep(r.getRootStep()));
        if (!r.getClaudeCallLogs().isEmpty()) {
            m.put("claudeLogs", this.serializeClaudeLogs(r.getClaudeCallLogs()));
        }
        return m;
    }

    private List<Map<String, Object>> serializeClaudeLogs(List<ClaudeCallRecord> logs) {
        ArrayList<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
        for (ClaudeCallRecord log : logs) {
            LinkedHashMap<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("node", log.getNodeLocation());
            m.put("chunk", log.getChunkIndex() + " / " + log.getTotalChunks());
            m.put("prompt", log.getPrompt());
            m.put("rawResponse", log.getRawResponse());
            out.add(m);
        }
        return out;
    }

    private List<Map<String, Object>> serializeOptimizations(List<OptimizationFinding> findings) {
        ArrayList<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
        for (OptimizationFinding f : findings) {
            LinkedHashMap<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("category", f.getCategory().name());
            m.put("severity", f.getSeverity() != null ? f.getSeverity().name() : "MEDIUM");
            m.put("table", f.getTable());
            m.put("column", f.getColumn());
            m.put("description", f.getDescription());
            m.put("location", f.getLocation());
            m.put("evidence", f.getEvidence());
            m.put("suggestedCode", f.getSuggestedCode());
            if (f.getClaudeInsight() != null && !f.getClaudeInsight().isBlank()) {
                m.put("claudeInsight", f.getClaudeInsight());
            }
            out.add(m);
        }
        return out;
    }

    private List<Map<String, Object>> serializeTestDataSets(List<TestDataSet> sets) {
        ArrayList<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
        for (TestDataSet tds : sets) {
            LinkedHashMap<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("step", tds.getQueryStep());
            m.put("sql", tds.getSql());
            m.put("table", tds.getTable());
            m.put("sampleRows", tds.getSampleRows());
            m.put("testCases", this.serializeTestCases(tds.getTestCases()));
            m.put("downstreamBindings", tds.getDownstreamBindings());
            out.add(m);
        }
        return out;
    }

    private List<Map<String, Object>> serializeTestCases(List<TestDataSet.TestCase> cases) {
        ArrayList<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
        for (TestDataSet.TestCase tc : cases) {
            LinkedHashMap<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("variant", tc.getVariant().name());
            m.put("description", tc.getDescription());
            m.put("inputData", tc.getInputData());
            m.put("expectedOutput", tc.getExpectedOutput());
            out.add(m);
        }
        return out;
    }

    private Map<String, Object> serializeStep(FlowStep step) {
        LinkedHashMap<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("class", step.getClassName());
        m.put("method", step.getMethodName());
        m.put("kind", step.getKind().name());
        m.put("depth", step.getDepth());
        m.put("queries", this.serializeQueries(step.getQueries()));
        ArrayList<Map<String, Object>> children = new ArrayList<Map<String, Object>>();
        for (FlowStep child : step.getChildren()) {
            children.add(this.serializeStep(child));
        }
        m.put("children", children);
        return m;
    }

    private List<Map<String, Object>> serializeQueries(List<ExtractedQuery> queries) {
        ArrayList<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
        for (ExtractedQuery q : queries) {
            LinkedHashMap<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("type", q.getType().name());
            m.put("table", q.getTableName());
            m.put("sql", q.getRawSql());
            m.put("predicates", this.serializePredicates(q.getPredicates()));
            out.add(m);
        }
        return out;
    }

    private List<Map<String, Object>> serializePredicates(List<Predicate> predicates) {
        ArrayList<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
        for (Predicate p : predicates) {
            LinkedHashMap<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("column", p.getColumn());
            m.put("op", p.getOp().name());
            m.put("rawValue", p.getRawValue());
            m.put("resolvedValue", p.getResolvedValue());
            out.add(m);
        }
        return out;
    }
}

