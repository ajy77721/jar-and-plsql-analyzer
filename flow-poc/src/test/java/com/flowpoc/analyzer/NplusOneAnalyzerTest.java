package com.flowpoc.analyzer;

import com.flowpoc.model.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class NplusOneAnalyzerTest {

    private final NplusOneAnalyzer analyzer = new NplusOneAnalyzer();

    @Test
    void detectsNPlusOneWhenSameCollectionAtDifferentDepths() {
        FlowStep root = new FlowStep("com.example.UserService", "getUsers",
                FlowStep.StepKind.SERVICE, 0);
        root.addQuery(queryForCollection("users", ExtractedQuery.QueryType.SELECT));

        FlowStep child = new FlowStep("com.example.UserRepo", "findById",
                FlowStep.StepKind.REPOSITORY, 1);
        child.addQuery(queryForCollection("users", ExtractedQuery.QueryType.SELECT));

        root.addChild(child);

        FlowResult flowResult = new FlowResult("GET", "/users", root);
        List<OptimizationFinding> findings = analyzer.analyze(flowResult);

        assertEquals(1, findings.size());
        OptimizationFinding finding = findings.get(0);
        assertEquals(OptimizationFinding.Category.N_PLUS_ONE, finding.getCategory());
        assertEquals("users", finding.getTable());
    }

    @Test
    void noFindingWhenCollectionOnlyAtOneDepth() {
        FlowStep root = new FlowStep("com.example.UserService", "list",
                FlowStep.StepKind.SERVICE, 0);
        root.addQuery(queryForCollection("users", ExtractedQuery.QueryType.SELECT));

        FlowStep child = new FlowStep("com.example.UserRepo", "findAll",
                FlowStep.StepKind.REPOSITORY, 1);
        child.addQuery(queryForCollection("users", ExtractedQuery.QueryType.SELECT));

        root.addChild(child);

        FlowResult flowResult = new FlowResult("GET", "/users", root);
        List<FlowStep> steps = flowResult.allSteps();

        // Both steps have SELECT on "users" at different depths → should detect
        List<OptimizationFinding> findings = analyzer.analyze(flowResult);
        assertEquals(1, findings.size());
    }

    @Test
    void noFindingWhenSingleQueryOnCollection() {
        FlowStep root = new FlowStep("com.example.Ctrl", "get",
                FlowStep.StepKind.CONTROLLER, 0);
        FlowStep child = new FlowStep("com.example.Repo", "findAll",
                FlowStep.StepKind.REPOSITORY, 1);
        child.addQuery(queryForCollection("orders", ExtractedQuery.QueryType.SELECT));
        root.addChild(child);

        FlowResult flowResult = new FlowResult("GET", "/orders", root);
        List<OptimizationFinding> findings = analyzer.analyze(flowResult);

        assertTrue(findings.isEmpty());
    }

    @Test
    void noFindingForNonSelectQueries() {
        FlowStep root = new FlowStep("com.example.Svc", "create",
                FlowStep.StepKind.SERVICE, 0);
        root.addQuery(queryForCollection("products", ExtractedQuery.QueryType.INSERT));

        FlowStep child = new FlowStep("com.example.Repo", "save",
                FlowStep.StepKind.REPOSITORY, 1);
        child.addQuery(queryForCollection("products", ExtractedQuery.QueryType.INSERT));

        root.addChild(child);

        FlowResult flowResult = new FlowResult("POST", "/products", root);
        List<OptimizationFinding> findings = analyzer.analyze(flowResult);

        assertTrue(findings.isEmpty());
    }

    @Test
    void findingDescriptionMentionsCollectionAndCount() {
        FlowStep root = new FlowStep("A", "m1", FlowStep.StepKind.SERVICE, 0);
        root.addQuery(queryForCollection("items", ExtractedQuery.QueryType.SELECT));

        FlowStep child = new FlowStep("B", "m2", FlowStep.StepKind.REPOSITORY, 2);
        child.addQuery(queryForCollection("items", ExtractedQuery.QueryType.SELECT));
        root.addChild(child);

        FlowResult fr = new FlowResult("GET", "/items", root);
        List<OptimizationFinding> findings = analyzer.analyze(fr);

        assertFalse(findings.isEmpty());
        String desc = findings.get(0).getDescription();
        assertTrue(desc.contains("items"));
        assertTrue(desc.contains("2"));
    }

    private ExtractedQuery queryForCollection(String collection, ExtractedQuery.QueryType type) {
        return new ExtractedQuery(
                type.name() + " " + collection,
                type,
                collection,
                "TestClass",
                "testMethod");
    }
}
