/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  com.fasterxml.jackson.core.JsonParser$Feature
 *  com.fasterxml.jackson.databind.JsonNode
 *  com.fasterxml.jackson.databind.ObjectMapper
 */
package com.flowpoc.engine;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowpoc.model.Predicate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MongoFilterParser {
    private static final ObjectMapper MAPPER = new ObjectMapper().enable(new JsonParser.Feature[]{JsonParser.Feature.ALLOW_SINGLE_QUOTES}).enable(new JsonParser.Feature[]{JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES});

    public List<Predicate> parse(String filterJson) {
        ArrayList<Predicate> result = new ArrayList<Predicate>();
        if (filterJson == null || filterJson.isBlank()) {
            return result;
        }
        try {
            String normalized = filterJson.replaceAll("(:\\s*)(\\?\\d+)", "$1\"$2\"");
            JsonNode root = MAPPER.readTree(normalized);
            this.parseNode(root, result);
        }
        catch (Exception exception) {
            // empty catch block
        }
        return result;
    }

    private void parseNode(JsonNode node, List<Predicate> out) {
        if (!node.isObject()) {
            return;
        }
        Iterator fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry entry = (Map.Entry)fields.next();
            String key = (String)entry.getKey();
            JsonNode value = (JsonNode)entry.getValue();
            if ("$and".equals(key) || "$or".equals(key)) {
                if (!value.isArray()) continue;
                for (JsonNode element : value) {
                    this.parseNode(element, out);
                }
                continue;
            }
            if (value.isNull()) {
                out.add(new Predicate(key, Predicate.Op.IS_NULL, null));
                continue;
            }
            if (value.isTextual()) {
                String text = value.asText();
                if (this.isBindPlaceholder(text)) {
                    Predicate p = new Predicate(key, Predicate.Op.EQ, text);
                    out.add(p);
                    continue;
                }
                out.add(new Predicate(key, Predicate.Op.EQ, text));
                continue;
            }
            if (value.isNumber() || value.isBoolean()) {
                out.add(new Predicate(key, Predicate.Op.EQ, value.asText()));
                continue;
            }
            if (!value.isObject()) continue;
            this.parseOperatorNode(key, value, out);
        }
    }

    private void parseOperatorNode(String field, JsonNode opNode, List<Predicate> out) {
        Iterator fields = opNode.fields();
        while (fields.hasNext()) {
            Map.Entry entry = (Map.Entry)fields.next();
            String op = (String)entry.getKey();
            JsonNode val = (JsonNode)entry.getValue();
            String rawVal = val.isNull() ? null : val.asText();
            switch (op) {
                case "$gt": {
                    out.add(new Predicate(field, Predicate.Op.GT, rawVal));
                    break;
                }
                case "$gte": {
                    out.add(new Predicate(field, Predicate.Op.GTE, rawVal));
                    break;
                }
                case "$lt": {
                    out.add(new Predicate(field, Predicate.Op.LT, rawVal));
                    break;
                }
                case "$lte": {
                    out.add(new Predicate(field, Predicate.Op.LTE, rawVal));
                    break;
                }
                case "$ne": {
                    out.add(new Predicate(field, Predicate.Op.NEQ, rawVal));
                    break;
                }
                case "$regex": {
                    out.add(new Predicate(field, Predicate.Op.LIKE, rawVal));
                    break;
                }
                case "$in": {
                    String inVal = val.isArray() ? val.toString() : rawVal;
                    out.add(new Predicate(field, Predicate.Op.IN, inVal));
                    break;
                }
            }
        }
    }

    private boolean isBindPlaceholder(String text) {
        if (text == null) {
            return false;
        }
        return text.matches("\\?\\d*") || text.startsWith(":");
    }
}

