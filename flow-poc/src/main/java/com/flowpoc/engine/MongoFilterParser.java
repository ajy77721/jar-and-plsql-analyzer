package com.flowpoc.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowpoc.model.Predicate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MongoFilterParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public List<Predicate> parse(String filterJson) {
        List<Predicate> result = new ArrayList<>();
        if (filterJson == null || filterJson.isBlank()) return result;
        try {
            JsonNode root = MAPPER.readTree(filterJson);
            parseNode(root, result);
        } catch (Exception ignored) {
        }
        return result;
    }

    private void parseNode(JsonNode node, List<Predicate> out) {
        if (!node.isObject()) return;
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String key = entry.getKey();
            JsonNode value = entry.getValue();

            if ("$and".equals(key) || "$or".equals(key)) {
                if (value.isArray()) {
                    for (JsonNode element : value) {
                        parseNode(element, out);
                    }
                }
                continue;
            }

            if (value.isNull()) {
                out.add(new Predicate(key, Predicate.Op.IS_NULL, null));
                continue;
            }

            if (value.isTextual()) {
                String text = value.asText();
                if (isBindPlaceholder(text)) {
                    Predicate p = new Predicate(key, Predicate.Op.EQ, text);
                    out.add(p);
                } else {
                    out.add(new Predicate(key, Predicate.Op.EQ, text));
                }
                continue;
            }

            if (value.isNumber() || value.isBoolean()) {
                out.add(new Predicate(key, Predicate.Op.EQ, value.asText()));
                continue;
            }

            if (value.isObject()) {
                parseOperatorNode(key, value, out);
                continue;
            }
        }
    }

    private void parseOperatorNode(String field, JsonNode opNode, List<Predicate> out) {
        Iterator<Map.Entry<String, JsonNode>> fields = opNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String op = entry.getKey();
            JsonNode val = entry.getValue();
            String rawVal = val.isNull() ? null : val.asText();

            switch (op) {
                case "$gt"    -> out.add(new Predicate(field, Predicate.Op.GT,  rawVal));
                case "$gte"   -> out.add(new Predicate(field, Predicate.Op.GTE, rawVal));
                case "$lt"    -> out.add(new Predicate(field, Predicate.Op.LT,  rawVal));
                case "$lte"   -> out.add(new Predicate(field, Predicate.Op.LTE, rawVal));
                case "$ne"    -> out.add(new Predicate(field, Predicate.Op.NEQ, rawVal));
                case "$regex" -> out.add(new Predicate(field, Predicate.Op.LIKE, rawVal));
                case "$in"    -> {
                    String inVal = val.isArray() ? val.toString() : rawVal;
                    out.add(new Predicate(field, Predicate.Op.IN, inVal));
                }
                default -> {}
            }
        }
    }

    private boolean isBindPlaceholder(String text) {
        if (text == null) return false;
        return text.matches("\\?\\d*") || text.startsWith(":");
    }
}
