package com.flowpoc.engine.visitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowpoc.model.ExtractedQuery;
import com.flowpoc.model.FlowStep;
import com.flowpoc.model.MongoOperation;

/**
 * Visitor that extracts MongoDB operations from a call-tree node.
 *
 * Sources mined (in priority order):
 *  1. collectionsAccessed  – direct analysis output with collection + op
 *  2. stringLiterals       – Spring Data / MongoTemplate method-name patterns
 *  3. annotations          – @Document, @Query on method
 */
public class MongoOperationVisitor implements CallTreeVisitor {

    @Override
    public void visit(JsonNode node, FlowStep step) {
        String className  = node.path("className").asText("");
        String methodName = node.path("methodName").asText("");

        visitCollectionsAccessed(node, step, className, methodName);
        visitStringLiterals(node, step, className, methodName);
        visitAnnotations(node, step, className, methodName);
    }

    private void visitCollectionsAccessed(JsonNode node, FlowStep step,
                                           String cls, String method) {
        JsonNode colls = node.path("collectionsAccessed");
        if (!colls.isArray()) return;

        for (JsonNode coll : colls) {
            String collection = coll.isTextual()
                    ? coll.asText()
                    : coll.path("collection").asText("");
            String op = coll.isTextual()
                    ? inferOpFromMethod(method)
                    : coll.path("operation").asText("FIND");

            if (collection.isBlank()) continue;

            MongoOperation mop = new MongoOperation(collection, MongoOperation.Op.fromString(op),
                    null, cls, method);
            step.addQuery(mop.toExtractedQuery());
        }
    }

    private void visitStringLiterals(JsonNode node, FlowStep step,
                                      String cls, String method) {
        JsonNode lits = node.path("stringLiterals");
        if (!lits.isArray()) return;

        for (JsonNode lit : lits) {
            String s = lit.asText("").trim();
            // Looks like a MongoDB collection name (no spaces, no SQL keywords)
            if (s.isBlank() || s.contains(" ") || s.length() > 80) continue;
            if (looksLikeCollectionName(s)) {
                MongoOperation mop = new MongoOperation(
                        s, MongoOperation.Op.fromString(inferOpFromMethod(method)),
                        null, cls, method);
                step.addQuery(mop.toExtractedQuery());
            }
        }
    }

    private void visitAnnotations(JsonNode node, FlowStep step,
                                   String cls, String method) {
        JsonNode anns = node.path("annotations");
        if (!anns.isArray()) return;

        for (JsonNode ann : anns) {
            String name = ann.path("name").asText("");
            if (!"Query".equals(name) && !"Aggregation".equals(name)) continue;

            JsonNode attrs = ann.path("attributes");
            String queryStr = attrs.path("value").asText(attrs.path("pipeline").asText(""));
            if (queryStr.isBlank()) continue;

            // @Query on Spring Data repository — collection is the entity name
            String collection = inferCollectionFromClassName(cls);
            MongoOperation mop = new MongoOperation(
                    collection, MongoOperation.Op.FIND, queryStr, cls, method);
            step.addQuery(mop.toExtractedQuery());
        }
    }

    private String inferOpFromMethod(String method) {
        if (method == null) return "FIND";
        String m = method.toLowerCase();
        if (m.startsWith("find") || m.startsWith("get") || m.startsWith("fetch")
                || m.startsWith("load") || m.startsWith("search") || m.startsWith("list"))
            return "FIND";
        if (m.startsWith("save") || m.startsWith("insert") || m.startsWith("create"))
            return "INSERT";
        if (m.startsWith("update") || m.startsWith("set") || m.startsWith("modify")
                || m.startsWith("patch"))
            return "UPDATE";
        if (m.startsWith("delete") || m.startsWith("remove"))
            return "DELETE";
        if (m.startsWith("count"))  return "COUNT";
        if (m.startsWith("exists")) return "EXISTS";
        if (m.contains("aggregate") || m.contains("pipeline")) return "AGGREGATE";
        return "FIND";
    }

    private boolean looksLikeCollectionName(String s) {
        return s.matches("[a-zA-Z][a-zA-Z0-9_]*") && s.length() >= 2;
    }

    private String inferCollectionFromClassName(String cls) {
        if (cls == null || cls.isBlank()) return "unknown";
        String simple = cls.contains(".") ? cls.substring(cls.lastIndexOf('.') + 1) : cls;
        // Remove Repository/Repo/Dao suffix → likely entity name → lower-case = collection
        String entity = simple.replaceAll("(?i)(Repository|Repo|Dao|Service|Impl)$", "");
        return entity.isEmpty() ? simple.toLowerCase() : entity.toLowerCase();
    }
}
