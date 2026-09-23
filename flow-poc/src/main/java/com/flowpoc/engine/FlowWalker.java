/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  com.fasterxml.jackson.core.JsonFactory
 *  com.fasterxml.jackson.core.JsonParser
 *  com.fasterxml.jackson.core.JsonToken
 *  com.fasterxml.jackson.databind.JsonNode
 *  com.fasterxml.jackson.databind.ObjectMapper
 */
package com.flowpoc.engine;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowpoc.engine.visitor.CallTreeVisitor;
import com.flowpoc.model.FlowResult;
import com.flowpoc.model.FlowStep;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class FlowWalker {
    private final ObjectMapper mapper = new ObjectMapper();
    private final JsonFactory factory = this.mapper.getFactory();
    private final List<CallTreeVisitor> visitors;

    public FlowWalker(List<CallTreeVisitor> visitors) {
        this.visitors = List.copyOf(visitors);
    }

    public List<FlowResult> walk(File analysisJson) throws IOException {
        return this.walk(analysisJson, Collections.emptySet());
    }

    public List<FlowResult> walk(File analysisJson, Set<String> selectedEndpoints) throws IOException {
        try (JsonParser jp = this.factory.createParser(analysisJson);){
            List<FlowResult> list = this.walk(jp, selectedEndpoints);
            return list;
        }
    }

    public List<FlowResult> walk(InputStream analysisJson, Set<String> selectedEndpoints) throws IOException {
        try (JsonParser jp = this.factory.createParser(analysisJson);){
            List<FlowResult> list = this.walk(jp, selectedEndpoints);
            return list;
        }
    }

    private List<FlowResult> walk(JsonParser jp, Set<String> selectedEndpoints) throws IOException {
        boolean filtered = selectedEndpoints != null && !selectedEndpoints.isEmpty();
        ArrayList<FlowResult> results = new ArrayList<FlowResult>();
        while (jp.nextToken() != null) {
            if (jp.currentToken() != JsonToken.FIELD_NAME || !"endpoints".equals(jp.currentName())) continue;
            jp.nextToken();
            break;
        }
        while (jp.nextToken() == JsonToken.START_OBJECT) {
            JsonNode ct;
            JsonNode epNode = (JsonNode)this.mapper.readTree(jp);
            String method = epNode.path("httpMethod").asText("?");
            String path = epNode.path("fullPath").asText("/");
            String key = method + " " + path;
            if (filtered && !selectedEndpoints.contains(key) || (ct = epNode.path("callTree")).isMissingNode() || ct.isNull()) continue;
            FlowStep root_ = this.buildStep(ct, 0);
            results.add(new FlowResult(method, path, root_));
        }
        return results;
    }

    private FlowStep buildStep(JsonNode node, int depth) {
        String cls = node.path("className").asText("");
        String method = node.path("methodName").asText("");
        FlowStep.StepKind kind = this.resolveKind(node.path("stereotype").asText(""));
        FlowStep step = new FlowStep(cls, method, kind, depth);
        for (CallTreeVisitor v : this.visitors) {
            v.visit(node, step);
        }
        JsonNode children = node.path("children");
        if (children.isArray()) {
            for (JsonNode child : children) {
                step.addChild(this.buildStep(child, depth + 1));
            }
        }
        return step;
    }

    private FlowStep.StepKind resolveKind(String stereotype) {
        if (stereotype == null) {
            return FlowStep.StepKind.OTHER;
        }
        return switch (stereotype.toUpperCase()) {
            case "REST_CONTROLLER", "CONTROLLER" -> FlowStep.StepKind.CONTROLLER;
            case "SERVICE" -> FlowStep.StepKind.SERVICE;
            case "REPOSITORY", "DAO" -> FlowStep.StepKind.REPOSITORY;
            default -> FlowStep.StepKind.OTHER;
        };
    }
}

