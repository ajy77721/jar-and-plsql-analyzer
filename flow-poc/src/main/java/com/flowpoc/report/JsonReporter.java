package com.flowpoc.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.flowpoc.model.*;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

/**
 * Serialises FlowResult list to structured JSON for storage or UI consumption.
 */
public class JsonReporter implements FlowReporter {

    private final ObjectMapper mapper;

    public JsonReporter() {
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .enable(SerializationFeature.INDENT_OUTPUT);
    }

    @Override
    public void write(List<FlowResult> results, OutputStream out) throws IOException {
        List<Map<String, Object>> payload = new ArrayList<>();
        for (FlowResult r : results) {
            payload.add(serialize(r));
        }
        mapper.writeValue(out, payload);
    }

    private Map<String, Object> serialize(FlowResult r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("endpoint",      r.getEndpointMethod() + " " + r.getEndpointPath());
        m.put("optimizations", serializeOptimizations(r.getOptimizations()));
        m.put("testDataSets",  serializeTestDataSets(r.getTestDataSets()));
        m.put("flowSteps",     serializeStep(r.getRootStep()));
        return m;
    }

    private List<Map<String, Object>> serializeOptimizations(List<OptimizationFinding> findings) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (OptimizationFinding f : findings) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("category",    f.getCategory().name());
            m.put("table",       f.getTable());
            m.put("column",      f.getColumn());
            m.put("description", f.getDescription());
            m.put("location",    f.getLocation());
            m.put("evidence",    f.getEvidence());
            out.add(m);
        }
        return out;
    }

    private List<Map<String, Object>> serializeTestDataSets(List<TestDataSet> sets) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (TestDataSet tds : sets) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("step",       tds.getQueryStep());
            m.put("sql",        tds.getSql());
            m.put("table",      tds.getTable());
            m.put("sampleRows", tds.getSampleRows());
            m.put("testCases",  serializeTestCases(tds.getTestCases()));
            m.put("downstreamBindings", tds.getDownstreamBindings());
            out.add(m);
        }
        return out;
    }

    private List<Map<String, Object>> serializeTestCases(List<TestDataSet.TestCase> cases) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (TestDataSet.TestCase tc : cases) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("variant",        tc.getVariant().name());
            m.put("description",    tc.getDescription());
            m.put("inputData",      tc.getInputData());
            m.put("expectedOutput", tc.getExpectedOutput());
            out.add(m);
        }
        return out;
    }

    private Map<String, Object> serializeStep(FlowStep step) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("class",   step.getClassName());
        m.put("method",  step.getMethodName());
        m.put("kind",    step.getKind().name());
        m.put("depth",   step.getDepth());
        m.put("queries", serializeQueries(step.getQueries()));

        List<Map<String, Object>> children = new ArrayList<>();
        for (FlowStep child : step.getChildren()) children.add(serializeStep(child));
        m.put("children", children);
        return m;
    }

    private List<Map<String, Object>> serializeQueries(List<ExtractedQuery> queries) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (ExtractedQuery q : queries) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("type",       q.getType().name());
            m.put("table",      q.getTableName());
            m.put("sql",        q.getRawSql());
            m.put("predicates", serializePredicates(q.getPredicates()));
            out.add(m);
        }
        return out;
    }

    private List<Map<String, Object>> serializePredicates(List<Predicate> predicates) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Predicate p : predicates) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("column",        p.getColumn());
            m.put("op",            p.getOp().name());
            m.put("rawValue",      p.getRawValue());
            m.put("resolvedValue", p.getResolvedValue());
            out.add(m);
        }
        return out;
    }
}
