package com.flowpoc.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowpoc.engine.visitor.CallTreeVisitor;
import com.flowpoc.model.FlowResult;
import com.flowpoc.model.FlowStep;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Template-method engine: walks the call tree for every endpoint in analysis.json.
 * Extraction logic is delegated to pluggable {@link CallTreeVisitor} instances (OCP).
 *
 * Usage:
 *   FlowWalker walker = new FlowWalker(List.of(new MongoOperationVisitor(), ...));
 *   List<FlowResult> results = walker.walk(new File("analysis.json"));
 */
public class FlowWalker {

    private final ObjectMapper         mapper;
    private final List<CallTreeVisitor> visitors;

    public FlowWalker(List<CallTreeVisitor> visitors) {
        this.mapper   = new ObjectMapper();
        this.visitors = List.copyOf(visitors);
    }

    public List<FlowResult> walk(File analysisJson) throws IOException {
        JsonNode root      = mapper.readTree(analysisJson);
        JsonNode endpoints = root.path("endpoints");
        List<FlowResult> results = new ArrayList<>();

        for (JsonNode ep : endpoints) {
            String method = ep.path("httpMethod").asText("?");
            String path   = ep.path("fullPath").asText("/");
            JsonNode ct   = ep.path("callTree");
            if (ct.isMissingNode() || ct.isNull()) continue;

            FlowStep root_ = buildStep(ct, 0);
            results.add(new FlowResult(method, path, root_));
        }
        return results;
    }

    // --- Template method ---

    private FlowStep buildStep(JsonNode node, int depth) {
        String cls    = node.path("className").asText("");
        String method = node.path("methodName").asText("");
        FlowStep.StepKind kind = resolveKind(node.path("stereotype").asText(""));

        FlowStep step = new FlowStep(cls, method, kind, depth);

        // Apply every registered visitor to this node (OCP – add new visitors, no code change here)
        for (CallTreeVisitor v : visitors) {
            v.visit(node, step);
        }

        JsonNode children = node.path("children");
        if (children.isArray()) {
            for (JsonNode child : children) {
                step.addChild(buildStep(child, depth + 1));
            }
        }
        return step;
    }

    private FlowStep.StepKind resolveKind(String stereotype) {
        if (stereotype == null) return FlowStep.StepKind.OTHER;
        return switch (stereotype.toUpperCase()) {
            case "REST_CONTROLLER", "CONTROLLER" -> FlowStep.StepKind.CONTROLLER;
            case "SERVICE"                       -> FlowStep.StepKind.SERVICE;
            case "REPOSITORY", "DAO"             -> FlowStep.StepKind.REPOSITORY;
            default                              -> FlowStep.StepKind.OTHER;
        };
    }
}
