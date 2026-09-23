/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  com.fasterxml.jackson.databind.JsonNode
 *  com.fasterxml.jackson.databind.ObjectMapper
 *  org.bson.Document
 */
package com.flowpoc.layer2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowpoc.layer2.ShadowFilterMatcher;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.bson.Document;

public class ShadowAggregationExecutor {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final ShadowFilterMatcher matcher = new ShadowFilterMatcher();
    private final Map<String, List<Document>> allCollections;

    public ShadowAggregationExecutor(Map<String, List<Document>> allCollections) {
        this.allCollections = allCollections;
    }

    public List<Document> execute(List<Document> input, List<Document> pipeline) {
        List<Document> current = new ArrayList<Document>(input);
        for (Document stage : pipeline) {
            current = this.processStage(current, stage);
        }
        return current;
    }

    public static List<Document> parsePipeline(String pipelineJson) {
        if (pipelineJson == null || pipelineJson.isBlank()) {
            return Collections.emptyList();
        }
        try {
            ArrayList<Document> stages = new ArrayList<Document>();
            JsonNode root = MAPPER.readTree(pipelineJson.trim());
            if (root.isArray()) {
                for (JsonNode node : root) {
                    stages.add(Document.parse((String)node.toString()));
                }
            }
            return stages;
        }
        catch (Exception e) {
            return Collections.emptyList();
        }
    }

    private List<Document> processStage(List<Document> docs, Document stage) {
        if (stage.containsKey((Object)"$match")) {
            return this.stageMatch(docs, stage.get((Object)"$match"));
        }
        if (stage.containsKey((Object)"$project")) {
            return this.stageProject(docs, this.asDoc(stage.get((Object)"$project")));
        }
        if (stage.containsKey((Object)"$group")) {
            return this.stageGroup(docs, this.asDoc(stage.get((Object)"$group")));
        }
        if (stage.containsKey((Object)"$sort")) {
            return this.stageSort(docs, this.asDoc(stage.get((Object)"$sort")));
        }
        if (stage.containsKey((Object)"$limit")) {
            return this.stageLimit(docs, this.toInt(stage.get((Object)"$limit")));
        }
        if (stage.containsKey((Object)"$skip")) {
            return this.stageSkip(docs, this.toInt(stage.get((Object)"$skip")));
        }
        if (stage.containsKey((Object)"$unwind")) {
            return this.stageUnwind(docs, stage.get((Object)"$unwind"));
        }
        if (stage.containsKey((Object)"$addFields")) {
            return this.stageAddFields(docs, this.asDoc(stage.get((Object)"$addFields")));
        }
        if (stage.containsKey((Object)"$set")) {
            return this.stageAddFields(docs, this.asDoc(stage.get((Object)"$set")));
        }
        if (stage.containsKey((Object)"$count")) {
            return this.stageCount(docs, String.valueOf(stage.get((Object)"$count")));
        }
        if (stage.containsKey((Object)"$lookup")) {
            return this.stageLookup(docs, this.asDoc(stage.get((Object)"$lookup")));
        }
        return docs;
    }

    private List<Document> stageMatch(List<Document> docs, Object filterSpec) {
        return docs.stream().filter(d -> this.matcher.matches((Document)d, filterSpec)).collect(Collectors.toList());
    }

    private List<Document> stageProject(List<Document> docs, Document spec) {
        if (spec == null) {
            return docs;
        }
        boolean inclusive = spec.entrySet().stream().filter(e -> !((String)e.getKey()).equals("_id")).anyMatch(e -> this.toInt(e.getValue()) == 1 || e.getValue() instanceof String);
        return docs.stream().map(doc -> {
            Document result = new Document();
            if (inclusive) {
                for (Map.Entry e : spec.entrySet()) {
                    if (((String)e.getKey()).equals("_id") && this.toInt(e.getValue()) == 0) continue;
                    if (((String)e.getKey()).equals("_id") || this.toInt(e.getValue()) == 1) {
                        Object val = this.matcher.getField((Document)doc, (String)e.getKey());
                        if (val == null) continue;
                        result.put((String)e.getKey(), val);
                        continue;
                    }
                    Object patt4771$temp = e.getValue();
                    if (patt4771$temp instanceof String) {
                        String expr = (String)patt4771$temp;
                        result.put((String)e.getKey(), this.evalExpr((Document)doc, expr));
                        continue;
                    }
                    Object patt4910$temp = e.getValue();
                    if (!(patt4910$temp instanceof Document)) continue;
                    Document computed = (Document)patt4910$temp;
                    result.put((String)e.getKey(), this.evalDocExpr((Document)doc, computed));
                }
                if (!spec.containsKey((Object)"_id") && doc.containsKey((Object)"_id")) {
                    result.put("_id", doc.get((Object)"_id"));
                }
            } else {
                result.putAll((Map)doc);
                spec.forEach((k, v) -> {
                    if (this.toInt(v) == 0) {
                        result.remove(k);
                    }
                });
            }
            return result;
        }).collect(Collectors.toList());
    }

    private List<Document> stageGroup(List<Document> docs, Document spec) {
        if (spec == null) {
            return docs;
        }
        Object idExpr = spec.get((Object)"_id");
        LinkedHashMap<String, List> groups = new LinkedHashMap<String, List>();
        LinkedHashMap<String, String> groupKeys = new LinkedHashMap<String, String>();
        for (Document doc : docs) {
            String key = this.resolveGroupKey(doc, idExpr);
            groups.computeIfAbsent(key, k -> new ArrayList()).add(doc);
            groupKeys.put(key, key);
        }
        ArrayList<Document> result = new ArrayList<Document>();
        for (Map.Entry entry : groups.entrySet()) {
            Document group = new Document();
            String key = (String)entry.getKey();
            List groupDocs = (List)entry.getValue();
            group.put("_id", this.resolveGroupIdValue((Document)groupDocs.get(0), idExpr));
            for (Map.Entry acc : spec.entrySet()) {
                Document accSpec;
                if (((String)acc.getKey()).equals("_id") || (accSpec = this.asDoc(acc.getValue())) == null) continue;
                group.put((String)acc.getKey(), this.computeAccumulator(groupDocs, accSpec));
            }
            result.add(group);
        }
        return result;
    }

    private String resolveGroupKey(Document doc, Object idExpr) {
        String s;
        if (idExpr == null) {
            return "__all__";
        }
        if (idExpr instanceof String && (s = (String)idExpr).startsWith("$")) {
            return String.valueOf(this.matcher.getField(doc, s.substring(1)));
        }
        if (idExpr instanceof Document) {
            Document d = (Document)idExpr;
            StringBuilder sb = new StringBuilder();
            d.forEach((k, v) -> {
                String expr;
                sb.append((String)k).append(":").append(v instanceof String && (expr = (String)v).startsWith("$") ? this.matcher.getField(doc, expr.substring(1)) : v).append("|");
            });
            return sb.toString();
        }
        return String.valueOf(idExpr);
    }

    private Object resolveGroupIdValue(Document sample, Object idExpr) {
        String s;
        if (idExpr == null) {
            return null;
        }
        if (idExpr instanceof String && (s = (String)idExpr).startsWith("$")) {
            return this.matcher.getField(sample, s.substring(1));
        }
        if (idExpr instanceof Document) {
            Document d = (Document)idExpr;
            Document res = new Document();
            d.forEach((k, v) -> {
                String expr;
                res.put(k, v instanceof String && (expr = (String)v).startsWith("$") ? this.matcher.getField(sample, expr.substring(1)) : v);
            });
            return res;
        }
        return idExpr;
    }

    private Object computeAccumulator(List<Document> docs, Document spec) {
        Map.Entry acc = (Map.Entry)spec.entrySet().iterator().next();
        String op = (String)acc.getKey();
        Object expr = acc.getValue();
        return switch (op) {
            case "$sum" -> {
                Integer i;
                if (expr instanceof Integer && (i = (Integer)expr) == 1) {
                    yield docs.size();
                }
                double sum = 0.0;
                for (Document d : docs) {
                    Object v = this.resolveExpr(d, expr);
                    if (!(v instanceof Number)) continue;
                    Number n = (Number)v;
                    sum += n.doubleValue();
                }
                yield sum == (double)((long)sum) ? (double)((long)sum) : sum;
            }
            case "$count" -> docs.size();
            case "$avg" -> {
                double sum2 = 0.0;
                int cnt = 0;
                for (Document d : docs) {
                    Object v = this.resolveExpr(d, expr);
                    if (!(v instanceof Number)) continue;
                    Number n = (Number)v;
                    sum2 += n.doubleValue();
                    ++cnt;
                }
                yield cnt == 0 ? 0.0 : sum2 / (double)cnt;
            }
            case "$min" -> docs.stream().map(d -> this.resolveExpr((Document)d, expr)).filter(Objects::nonNull).min((a, b) -> {
                int n;
                if (a instanceof Number) {
                    Number na = (Number)a;
                    if (b instanceof Number) {
                        Number nb = (Number)b;
                        n = Double.compare(na.doubleValue(), nb.doubleValue());
                        return n;
                    }
                }
                n = a.toString().compareTo(b.toString());
                return n;
            }).orElse(null);
            case "$max" -> docs.stream().map(d -> this.resolveExpr((Document)d, expr)).filter(Objects::nonNull).max((a, b) -> {
                int n;
                if (a instanceof Number) {
                    Number na = (Number)a;
                    if (b instanceof Number) {
                        Number nb = (Number)b;
                        n = Double.compare(na.doubleValue(), nb.doubleValue());
                        return n;
                    }
                }
                n = a.toString().compareTo(b.toString());
                return n;
            }).orElse(null);
            case "$first" -> {
                if (docs.isEmpty()) {
                    yield null;
                }
                yield this.resolveExpr(docs.get(0), expr);
            }
            case "$last" -> {
                if (docs.isEmpty()) {
                    yield null;
                }
                yield this.resolveExpr(docs.get(docs.size() - 1), expr);
            }
            case "$push" -> docs.stream().map(d -> this.resolveExpr((Document)d, expr)).collect(Collectors.toList());
            case "$addToSet" -> {
                ArrayList<Object> set = new ArrayList<Object>();
                for (Document d : docs) {
                    Object v = this.resolveExpr(d, expr);
                    if (v == null || !set.stream().noneMatch(x -> Objects.equals(x, v))) continue;
                    set.add(v);
                }
                yield set;
            }
            default -> null;
        };
    }

    private List<Document> stageSort(List<Document> docs, Document spec) {
        if (spec == null) {
            return docs;
        }
        ArrayList<Document> sorted = new ArrayList<Document>(docs);
        List<Map.Entry<String, Object>> sortFields = new ArrayList<Map.Entry<String, Object>>(spec.entrySet());
        sorted.sort((a, b) -> {
            for (Map.Entry<String, Object> sf : sortFields) {
                Object va = this.matcher.getField((Document)a, sf.getKey());
                Object vb = this.matcher.getField((Document)b, sf.getKey());
                int dir = this.toInt(sf.getValue());
                int cmp = this.compareValues(va, vb);
                if (cmp == 0) continue;
                return dir >= 0 ? cmp : -cmp;
            }
            return 0;
        });
        return sorted;
    }

    private int compareValues(Object a, Object b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }
        if (a instanceof Number) {
            Number na = (Number)a;
            if (b instanceof Number) {
                Number nb = (Number)b;
                return Double.compare(na.doubleValue(), nb.doubleValue());
            }
        }
        return a.toString().compareTo(b.toString());
    }

    private List<Document> stageLimit(List<Document> docs, int n) {
        return n <= 0 ? docs : docs.stream().limit(n).collect(Collectors.toList());
    }

    private List<Document> stageSkip(List<Document> docs, int n) {
        return n <= 0 ? docs : docs.stream().skip(n).collect(Collectors.toList());
    }

    private List<Document> stageUnwind(List<Document> docs, Object spec) {
        String fieldPath;
        boolean preserveNullAndEmpty = false;
        if (spec instanceof String) {
            String s = (String)spec;
            fieldPath = s.startsWith("$") ? s.substring(1) : s;
        } else if (spec instanceof Document) {
            Document d = (Document)spec;
            String p = String.valueOf(d.get((Object)"path"));
            fieldPath = p.startsWith("$") ? p.substring(1) : p;
            preserveNullAndEmpty = Boolean.TRUE.equals(d.get((Object)"preserveNullAndEmptyArrays"));
        } else {
            return docs;
        }
        ArrayList<Document> result = new ArrayList<Document>();
        for (Document doc : docs) {
            Object val = this.matcher.getField(doc, fieldPath);
            if (val instanceof List) {
                List list = (List)val;
                if (list.isEmpty() && preserveNullAndEmpty) {
                    result.add(doc);
                    continue;
                }
                for (Object item : list) {
                    Document copy = new Document((Map)doc);
                    copy.put(fieldPath, item);
                    result.add(copy);
                }
                continue;
            }
            if (val == null) {
                if (!preserveNullAndEmpty) continue;
                result.add(doc);
                continue;
            }
            result.add(doc);
        }
        return result;
    }

    private List<Document> stageAddFields(List<Document> docs, Document spec) {
        if (spec == null) {
            return docs;
        }
        return docs.stream().map(doc -> {
            Document copy = new Document((Map)doc);
            spec.forEach((k, v) -> copy.put(k, this.resolveExpr(copy, v)));
            return copy;
        }).collect(Collectors.toList());
    }

    private List<Document> stageCount(List<Document> docs, String fieldName) {
        return List.of(new Document(fieldName, (Object)docs.size()));
    }

    private List<Document> stageLookup(List<Document> docs, Document spec) {
        if (spec == null) {
            return docs;
        }
        String from = String.valueOf(spec.get((Object)"from"));
        String localField = String.valueOf(spec.get((Object)"localField"));
        String foreignField = String.valueOf(spec.get((Object)"foreignField"));
        String as = String.valueOf(spec.get((Object)"as"));
        List<Document> foreign = this.allCollections.getOrDefault(from, Collections.emptyList());
        return docs.stream().map(doc -> {
            Object localVal = this.matcher.getField((Document)doc, localField);
            List<Document> matched = foreign.stream().filter(f -> Objects.equals(localVal, this.matcher.getField(f, foreignField)) || localVal != null && localVal.toString().equals(String.valueOf(this.matcher.getField(f, foreignField)))).collect(Collectors.toList());
            Document copy = new Document((Map)doc);
            copy.put(as, matched);
            return copy;
        }).collect(Collectors.toList());
    }

    private Object resolveExpr(Document doc, Object expr) {
        String s;
        if (expr instanceof String && (s = (String)expr).startsWith("$")) {
            return this.matcher.getField(doc, s.substring(1));
        }
        if (expr instanceof Document) {
            Document d = (Document)expr;
            return this.evalDocExpr(doc, d);
        }
        return expr;
    }

    private Object evalDocExpr(Document doc, Document expr) {
        Number na;
        Object b;
        Object a;
        List parts;
        Object sb;
        Object object;
        if (expr.containsKey((Object)"$toLower")) {
            Object v = this.resolveExpr(doc, expr.get((Object)"$toLower"));
            return v != null ? v.toString().toLowerCase() : null;
        }
        if (expr.containsKey((Object)"$toUpper")) {
            Object v = this.resolveExpr(doc, expr.get((Object)"$toUpper"));
            return v != null ? v.toString().toUpperCase() : null;
        }
        if (expr.containsKey((Object)"$toString")) {
            Object v = this.resolveExpr(doc, expr.get((Object)"$toString"));
            return v != null ? v.toString() : null;
        }
        if (expr.containsKey((Object)"$concat") && (object = expr.get((Object)"$concat")) instanceof List) {
            List parts2 = (List)object;
            sb = new StringBuilder();
            for (Object p : parts2) {
                Object v = this.resolveExpr(doc, p);
                ((StringBuilder)sb).append(v != null ? v : "");
            }
            return ((StringBuilder)sb).toString();
        }
        if (expr.containsKey((Object)"$add") && (sb = expr.get((Object)"$add")) instanceof List && (parts = (List)sb).size() == 2) {
            a = this.resolveExpr(doc, parts.get(0));
            b = this.resolveExpr(doc, parts.get(1));
            if (a instanceof Number) {
                na = (Number)a;
                if (b instanceof Number) {
                    Number nb = (Number)b;
                    return na.doubleValue() + nb.doubleValue();
                }
            }
        }
        if (expr.containsKey((Object)"$subtract") && (a = expr.get((Object)"$subtract")) instanceof List && (parts = (List)a).size() == 2) {
            a = this.resolveExpr(doc, parts.get(0));
            b = this.resolveExpr(doc, parts.get(1));
            if (a instanceof Number) {
                na = (Number)a;
                if (b instanceof Number) {
                    Number nb = (Number)b;
                    return na.doubleValue() - nb.doubleValue();
                }
            }
        }
        return expr.toString();
    }

    private Object evalExpr(Document doc, Object expr) {
        return this.resolveExpr(doc, expr);
    }

    private Document asDoc(Object obj) {
        if (obj instanceof Document) {
            Document d = (Document)obj;
            return d;
        }
        if (obj instanceof Map) {
            Map m = (Map)obj;
            return new Document(m);
        }
        return null;
    }

    private int toInt(Object v) {
        if (v instanceof Number) {
            Number n = (Number)v;
            return n.intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(v));
        }
        catch (Exception e) {
            return 0;
        }
    }
}

