/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  com.fasterxml.jackson.databind.JsonNode
 *  com.fasterxml.jackson.databind.ObjectMapper
 */
package com.flowpoc.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowpoc.engine.MongoFilterParser;
import com.flowpoc.model.Predicate;
import java.util.ArrayList;
import java.util.List;

public class AggregationPipelineParser {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final MongoFilterParser filterParser = new MongoFilterParser();

    public ParsedPipeline parse(String pipelineJson) {
        if (pipelineJson == null || pipelineJson.isBlank()) {
            return this.empty();
        }
        try {
            JsonNode root = MAPPER.readTree(pipelineJson.trim());
            if (root.isArray()) {
                return this.parsePipelineArray(root);
            }
            return new ParsedPipeline(this.filterParser.parse(pipelineJson), List.of(), null, null, null, List.of(), List.of());
        }
        catch (Exception e) {
            return this.empty();
        }
    }

    private ParsedPipeline parsePipelineArray(JsonNode array) {
        ArrayList<Predicate> matchPredicates = new ArrayList<Predicate>();
        ArrayList<String> groupFields = new ArrayList<String>();
        String lookupCollection = null;
        String lookupLocal = null;
        String lookupForeign = null;
        ArrayList<String> sortFields = new ArrayList<String>();
        ArrayList<String> projectFields = new ArrayList<String>();
        for (JsonNode stage : array) {
            JsonNode project;
            JsonNode sort;
            JsonNode lookup;
            JsonNode group;
            JsonNode match = stage.path("$match");
            if (!match.isMissingNode()) {
                matchPredicates.addAll(this.filterParser.parse(match.toString()));
            }
            if (!(group = stage.path("$group")).isMissingNode()) {
                this.extractGroupFields(group.path("_id"), groupFields);
            }
            if (!(lookup = stage.path("$lookup")).isMissingNode() && lookupCollection == null) {
                lookupCollection = lookup.path("from").asText(null);
                lookupLocal = lookup.path("localField").asText(null);
                lookupForeign = lookup.path("foreignField").asText(null);
            }
            if (!(sort = stage.path("$sort")).isMissingNode()) {
                sort.fieldNames().forEachRemaining(sortFields::add);
            }
            if ((project = stage.path("$project")).isMissingNode()) continue;
            project.fieldNames().forEachRemaining(f -> {
                if (!f.equals("_id")) {
                    projectFields.add((String)f);
                }
            });
        }
        return new ParsedPipeline(matchPredicates, groupFields, lookupCollection, lookupLocal, lookupForeign, sortFields, projectFields);
    }

    private void extractGroupFields(JsonNode id, List<String> out) {
        if (id.isTextual()) {
            String s = id.asText();
            if (s.startsWith("$")) {
                out.add(s.substring(1));
            }
        } else if (id.isObject()) {
            id.fieldNames().forEachRemaining(k -> {
                JsonNode v = id.path(k);
                if (v.isTextual() && v.asText().startsWith("$")) {
                    out.add(v.asText().substring(1));
                }
            });
        }
    }

    private ParsedPipeline empty() {
        return new ParsedPipeline(List.of(), List.of(), null, null, null, List.of(), List.of());
    }

    public record ParsedPipeline(List<Predicate> matchPredicates, List<String> groupFields, String lookupCollection, String lookupLocalField, String lookupForeignField, List<String> sortFields, List<String> projectFields) {
    }
}

