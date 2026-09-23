/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  com.fasterxml.jackson.databind.JsonNode
 */
package com.flowpoc.engine.visitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowpoc.engine.AggregationPipelineParser;
import com.flowpoc.engine.MongoFilterParser;
import com.flowpoc.engine.visitor.CallTreeVisitor;
import com.flowpoc.model.FlowStep;
import com.flowpoc.model.MongoOperation;
import com.flowpoc.model.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MongoOperationVisitor
implements CallTreeVisitor {
    private final MongoFilterParser filterParser;
    private final AggregationPipelineParser aggregationParser;
    private static final Set<String> COLLECTION_NAME_BLOCKLIST = Set.of("from", "where", "select", "insert", "update", "delete", "join", "on", "into", "set", "values", "group", "order", "by", "having", "limit", "offset", "distinct", "inner", "outer", "left", "right", "full", "union", "all", "and", "or", "not", "in", "exists", "between", "like", "is", "null", "true", "false", "case", "when", "then", "else", "end", "with", "as", "table", "index", "view", "trigger", "asc", "desc", "id", "key", "value", "name", "type", "status", "date", "time", "data", "info", "result", "error", "msg", "message");

    public MongoOperationVisitor() {
        this.filterParser = new MongoFilterParser();
        this.aggregationParser = new AggregationPipelineParser();
    }

    public MongoOperationVisitor(MongoFilterParser filterParser) {
        this.filterParser = filterParser;
        this.aggregationParser = new AggregationPipelineParser();
    }

    @Override
    public void visit(JsonNode node, FlowStep step) {
        String className = node.path("className").asText("");
        String methodName = node.path("methodName").asText("");
        this.visitCollectionsAccessed(node, step, className, methodName);
        this.visitStringLiterals(node, step, className, methodName);
        this.visitAnnotations(node, step, className, methodName);
    }

    private void visitCollectionsAccessed(JsonNode node, FlowStep step, String cls, String method) {
        JsonNode colls = node.path("collectionsAccessed");
        if (!colls.isArray()) {
            return;
        }
        String nodeOpType = node.path("operationType").asText("");
        JsonNode collSources = node.path("collectionSources");
        String stereotype = node.path("stereotype").asText("");
        boolean filterRepoCalls = !stereotype.equalsIgnoreCase("REPOSITORY") && !stereotype.equalsIgnoreCase("DAO") && !collSources.isMissingNode() && collSources.isObject();
        for (JsonNode coll : colls) {
            String op;
            String collection;
            String string = collection = coll.isTextual() ? coll.asText() : coll.path("collection").asText("");
            if (coll.isTextual()) {
                String src;
                if (filterRepoCalls && ("REPOSITORY_CALL".equalsIgnoreCase(src = collSources.path(collection).asText("")) || "DOCUMENT_ANNOTATION".equalsIgnoreCase(src))) continue;
                boolean opKnown = !nodeOpType.isBlank() && !"UNKNOWN".equalsIgnoreCase(nodeOpType);
                op = opKnown ? nodeOpType : this.inferOpFromMethod(method);
            } else {
                String collOp = coll.path("operation").asText("FIND");
                String string2 = op = "UNKNOWN".equalsIgnoreCase(collOp) ? this.inferOpFromMethod(method) : collOp;
            }
            if (collection.isBlank() || COLLECTION_NAME_BLOCKLIST.contains(collection.toLowerCase())) continue;
            MongoOperation mop = new MongoOperation(collection, MongoOperation.Op.fromString(op), null, cls, method);
            String pipelineStr = coll.path("pipeline").asText("");
            if (!pipelineStr.isBlank() && mop.getOp() == MongoOperation.Op.AGGREGATE) {
                mop.setPipeline(pipelineStr);
                this.aggregationParser.parse(pipelineStr).matchPredicates().forEach(mop::addPredicate);
            }
            if (mop.getPredicates().isEmpty() && mop.getOp() == MongoOperation.Op.FIND) {
                this.inferSpringDataPredicates(method).forEach(mop::addPredicate);
            }
            step.addQuery(mop.toExtractedQuery());
        }
    }

    private void visitStringLiterals(JsonNode node, FlowStep step, String cls, String method) {
        JsonNode lits = node.path("stringLiterals");
        if (!lits.isArray()) {
            return;
        }
        for (JsonNode lit : lits) {
            String s = lit.asText("").trim();
            if (s.isBlank() || s.contains(" ") || s.length() > 80 || !this.looksLikeCollectionName(s)) continue;
            MongoOperation mop = new MongoOperation(s, MongoOperation.Op.fromString(this.inferOpFromMethod(method)), null, cls, method);
            step.addQuery(mop.toExtractedQuery());
        }
    }

    private void visitAnnotations(JsonNode node, FlowStep step, String cls, String method) {
        JsonNode anns = node.path("annotationDetails");
        if (!anns.isArray() || anns.isEmpty()) {
            return;
        }
        JsonNode colls = node.path("collectionsAccessed");
        if (colls.isArray() && colls.size() > 0) {
            return;
        }
        for (JsonNode ann : anns) {
            MongoOperation mop;
            JsonNode attrs;
            String queryStr;
            String name = ann.path("name").asText("");
            if (!"Query".equals(name) && !"Aggregation".equals(name) || (queryStr = (attrs = ann.path("attributes")).path("value").asText(attrs.path("pipeline").asText(""))).isBlank()) continue;
            String collection = this.inferCollectionFromClassName(cls);
            if ("Aggregation".equals(name)) {
                mop = new MongoOperation(collection, MongoOperation.Op.AGGREGATE, queryStr, cls, method);
                mop.setPipeline(queryStr);
                AggregationPipelineParser.ParsedPipeline parsed = this.aggregationParser.parse(queryStr);
                parsed.matchPredicates().forEach(mop::addPredicate);
                step.addQuery(mop.toExtractedQuery());
                continue;
            }
            mop = new MongoOperation(collection, MongoOperation.Op.FIND, queryStr, cls, method);
            for (Predicate p : this.filterParser.parse(queryStr)) {
                mop.addPredicate(p);
            }
            step.addQuery(mop.toExtractedQuery());
        }
    }

    private String inferOpFromMethod(String method) {
        if (method == null) {
            return "FIND";
        }
        String m = method.toLowerCase();
        if (m.startsWith("find") || m.startsWith("get") || m.startsWith("fetch") || m.startsWith("load") || m.startsWith("search") || m.startsWith("list")) {
            return "FIND";
        }
        if (m.startsWith("save") || m.startsWith("insert") || m.startsWith("create")) {
            return "INSERT";
        }
        if (m.startsWith("update") || m.startsWith("set") || m.startsWith("modify") || m.startsWith("patch")) {
            return "UPDATE";
        }
        if (m.startsWith("delete") || m.startsWith("remove")) {
            return "DELETE";
        }
        if (m.startsWith("count")) {
            return "COUNT";
        }
        if (m.startsWith("exists")) {
            return "EXISTS";
        }
        if (m.contains("aggregate") || m.contains("pipeline")) {
            return "AGGREGATE";
        }
        return "FIND";
    }

    private List<Predicate> inferSpringDataPredicates(String method) {
        ArrayList<Predicate> preds = new ArrayList<Predicate>();
        if (method == null) {
            return preds;
        }
        Pattern verbPat = Pattern.compile("^(?:find|get|load|fetch|search|list|exists|count)(?:All)?By(.+)$", 2);
        Matcher verbMatch = verbPat.matcher(method);
        if (!verbMatch.matches()) {
            return preds;
        }
        String criteria = verbMatch.group(1);
        if ((criteria = criteria.replaceAll("(?i)(OrderBy|Top\\d+|First\\d*|Distinct).*$", "")).isBlank()) {
            return preds;
        }
        String[] parts = criteria.split("(?=[A-Z])And(?=[A-Z])|(?=[A-Z])Or(?=[A-Z])");
        int paramIdx = 0;
        for (String part : parts) {
            String fieldPart = part.replaceAll("(?i)(NotIn|In|IsNull|IsNotNull|NotLike|Like|Containing|StartingWith|EndingWith|Before|After|Between|Not|True|False)$", "");
            if (fieldPart.isBlank()) continue;
            Object field = Character.toLowerCase(fieldPart.charAt(0)) + fieldPart.substring(1);
            if ("id".equals(field)) {
                field = "_id";
            }
            preds.add(new Predicate((String)field, Predicate.Op.EQ, "?" + paramIdx));
            ++paramIdx;
        }
        return preds;
    }

    private boolean looksLikeCollectionName(String s) {
        if (s.length() < 3) {
            return false;
        }
        boolean allUpper = s.chars().filter(Character::isLetter).allMatch(Character::isUpperCase);
        if (allUpper) {
            return false;
        }
        if (!s.matches("[a-z][a-zA-Z0-9_]*")) {
            return false;
        }
        return !COLLECTION_NAME_BLOCKLIST.contains(s.toLowerCase());
    }

    private String inferCollectionFromClassName(String cls) {
        if (cls == null || cls.isBlank()) {
            return "unknown";
        }
        String simple = cls.contains(".") ? cls.substring(cls.lastIndexOf(46) + 1) : cls;
        String entity = simple.replaceAll("(?i)(Repository|Repo|Dao|Service|Impl)$", "");
        return entity.isEmpty() ? simple.toLowerCase() : entity.toLowerCase();
    }
}

