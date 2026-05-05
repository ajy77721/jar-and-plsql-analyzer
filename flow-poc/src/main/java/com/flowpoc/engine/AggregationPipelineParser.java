package com.flowpoc.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowpoc.model.Predicate;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses a MongoDB aggregation pipeline JSON array into a structured ParsedPipeline.
 *
 * Handles stages: $match, $group, $lookup, $sort, $project, $unwind.
 * Predicates are extracted only from $match stages (using MongoFilterParser).
 * Grouping keys, lookup joins, and sort/project fields are captured for
 * index-hint and test-data-chain purposes.
 */
public class AggregationPipelineParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final MongoFilterParser filterParser = new MongoFilterParser();

    public record ParsedPipeline(
            List<Predicate> matchPredicates,
            List<String>    groupFields,
            String          lookupCollection,
            String          lookupLocalField,
            String          lookupForeignField,
            List<String>    sortFields,
            List<String>    projectFields
    ) {}

    public ParsedPipeline parse(String pipelineJson) {
        if (pipelineJson == null || pipelineJson.isBlank()) return empty();
        try {
            JsonNode root = MAPPER.readTree(pipelineJson.trim());
            if (root.isArray()) return parsePipelineArray(root);
            // Single-object form used by some @Aggregation annotations — treat as $match
            return new ParsedPipeline(filterParser.parse(pipelineJson),
                    List.of(), null, null, null, List.of(), List.of());
        } catch (Exception e) {
            return empty();
        }
    }

    private ParsedPipeline parsePipelineArray(JsonNode array) {
        List<Predicate> matchPredicates  = new ArrayList<>();
        List<String>    groupFields      = new ArrayList<>();
        String          lookupCollection = null;
        String          lookupLocal      = null;
        String          lookupForeign    = null;
        List<String>    sortFields       = new ArrayList<>();
        List<String>    projectFields    = new ArrayList<>();

        for (JsonNode stage : array) {

            JsonNode match = stage.path("$match");
            if (!match.isMissingNode()) {
                matchPredicates.addAll(filterParser.parse(match.toString()));
            }

            JsonNode group = stage.path("$group");
            if (!group.isMissingNode()) {
                extractGroupFields(group.path("_id"), groupFields);
            }

            JsonNode lookup = stage.path("$lookup");
            if (!lookup.isMissingNode() && lookupCollection == null) {
                lookupCollection = lookup.path("from").asText(null);
                lookupLocal      = lookup.path("localField").asText(null);
                lookupForeign    = lookup.path("foreignField").asText(null);
            }

            JsonNode sort = stage.path("$sort");
            if (!sort.isMissingNode()) {
                sort.fieldNames().forEachRemaining(sortFields::add);
            }

            JsonNode project = stage.path("$project");
            if (!project.isMissingNode()) {
                project.fieldNames().forEachRemaining(f -> {
                    if (!f.equals("_id")) projectFields.add(f);
                });
            }
        }

        return new ParsedPipeline(matchPredicates, groupFields, lookupCollection,
                lookupLocal, lookupForeign, sortFields, projectFields);
    }

    private void extractGroupFields(JsonNode id, List<String> out) {
        if (id.isTextual()) {
            String s = id.asText();
            if (s.startsWith("$")) out.add(s.substring(1));
        } else if (id.isObject()) {
            id.fieldNames().forEachRemaining(k -> {
                JsonNode v = id.path(k);
                if (v.isTextual() && v.asText().startsWith("$"))
                    out.add(v.asText().substring(1));
            });
        }
    }

    private ParsedPipeline empty() {
        return new ParsedPipeline(List.of(), List.of(), null, null, null, List.of(), List.of());
    }
}
