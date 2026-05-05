package com.flowpoc.layer2;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Executes MongoDB aggregation pipeline stages against an in-memory list of Documents.
 *
 * Supported stages:
 *   $match      – filter using ShadowFilterMatcher (full operator support)
 *   $project    – include / exclude / compute fields
 *   $group      – group by expression with $sum $count $avg $min $max $first $last $push $addToSet
 *   $sort       – sort by one or more fields (1 asc, -1 desc)
 *   $limit      – cap result count
 *   $skip       – skip N documents
 *   $unwind     – flatten array fields
 *   $addFields / $set  – add / overwrite computed fields
 *   $count      – return count document
 *   $lookup     – left-join from another shadow collection (or skip if no shadow data)
 */
public class ShadowAggregationExecutor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ShadowFilterMatcher matcher = new ShadowFilterMatcher();
    private final Map<String, List<Document>> allCollections;

    public ShadowAggregationExecutor(Map<String, List<Document>> allCollections) {
        this.allCollections = allCollections;
    }

    public List<Document> execute(List<Document> input, List<Document> pipeline) {
        List<Document> current = new ArrayList<>(input);
        for (Document stage : pipeline) {
            current = processStage(current, stage);
        }
        return current;
    }

    public static List<Document> parsePipeline(String pipelineJson) {
        if (pipelineJson == null || pipelineJson.isBlank()) return Collections.emptyList();
        try {
            List<Document> stages = new ArrayList<>();
            var root = MAPPER.readTree(pipelineJson.trim());
            if (root.isArray()) {
                for (var node : root) stages.add(Document.parse(node.toString()));
            }
            return stages;
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    // ── Stage dispatch ────────────────────────────────────────────────────────

    private List<Document> processStage(List<Document> docs, Document stage) {
        if (stage.containsKey("$match"))             return stageMatch(docs, stage.get("$match"));
        if (stage.containsKey("$project"))           return stageProject(docs, asDoc(stage.get("$project")));
        if (stage.containsKey("$group"))             return stageGroup(docs, asDoc(stage.get("$group")));
        if (stage.containsKey("$sort"))              return stageSort(docs, asDoc(stage.get("$sort")));
        if (stage.containsKey("$limit"))             return stageLimit(docs, toInt(stage.get("$limit")));
        if (stage.containsKey("$skip"))              return stageSkip(docs, toInt(stage.get("$skip")));
        if (stage.containsKey("$unwind"))            return stageUnwind(docs, stage.get("$unwind"));
        if (stage.containsKey("$addFields"))         return stageAddFields(docs, asDoc(stage.get("$addFields")));
        if (stage.containsKey("$set"))               return stageAddFields(docs, asDoc(stage.get("$set")));
        if (stage.containsKey("$count"))             return stageCount(docs, String.valueOf(stage.get("$count")));
        if (stage.containsKey("$lookup"))            return stageLookup(docs, asDoc(stage.get("$lookup")));
        return docs; // unknown stage — pass through
    }

    // ── $match ────────────────────────────────────────────────────────────────

    private List<Document> stageMatch(List<Document> docs, Object filterSpec) {
        return docs.stream()
                .filter(d -> matcher.matches(d, filterSpec))
                .collect(Collectors.toList());
    }

    // ── $project ─────────────────────────────────────────────────────────────

    private List<Document> stageProject(List<Document> docs, Document spec) {
        if (spec == null) return docs;
        boolean inclusive = spec.entrySet().stream()
                .filter(e -> !e.getKey().equals("_id"))
                .anyMatch(e -> toInt(e.getValue()) == 1 || e.getValue() instanceof String);

        return docs.stream().map(doc -> {
            Document result = new Document();
            if (inclusive) {
                // Include only listed fields
                for (Map.Entry<String, Object> e : spec.entrySet()) {
                    if (e.getKey().equals("_id") && toInt(e.getValue()) == 0) continue;
                    if (e.getKey().equals("_id") || toInt(e.getValue()) == 1) {
                        Object val = matcher.getField(doc, e.getKey());
                        if (val != null) result.put(e.getKey(), val);
                    } else if (e.getValue() instanceof String expr) {
                        result.put(e.getKey(), evalExpr(doc, expr));
                    } else if (e.getValue() instanceof Document computed) {
                        result.put(e.getKey(), evalDocExpr(doc, computed));
                    }
                }
                // Always include _id unless explicitly excluded
                if (!spec.containsKey("_id") && doc.containsKey("_id"))
                    result.put("_id", doc.get("_id"));
            } else {
                // Exclude listed fields
                result.putAll(doc);
                spec.forEach((k, v) -> { if (toInt(v) == 0) result.remove(k); });
            }
            return result;
        }).collect(Collectors.toList());
    }

    // ── $group ────────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private List<Document> stageGroup(List<Document> docs, Document spec) {
        if (spec == null) return docs;
        Object idExpr = spec.get("_id");

        // Build groups
        Map<String, List<Document>> groups = new LinkedHashMap<>();
        Map<String, String>         groupKeys = new LinkedHashMap<>();

        for (Document doc : docs) {
            String key = resolveGroupKey(doc, idExpr);
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(doc);
            groupKeys.put(key, key);
        }

        List<Document> result = new ArrayList<>();
        for (Map.Entry<String, List<Document>> entry : groups.entrySet()) {
            Document group = new Document();
            String   key   = entry.getKey();
            List<Document> groupDocs = entry.getValue();

            group.put("_id", resolveGroupIdValue(groupDocs.get(0), idExpr));

            for (Map.Entry<String, Object> acc : spec.entrySet()) {
                if (acc.getKey().equals("_id")) continue;
                Document accSpec = asDoc(acc.getValue());
                if (accSpec == null) continue;
                group.put(acc.getKey(), computeAccumulator(groupDocs, accSpec));
            }
            result.add(group);
        }
        return result;
    }

    private String resolveGroupKey(Document doc, Object idExpr) {
        if (idExpr == null) return "__all__";
        if (idExpr instanceof String s && s.startsWith("$"))
            return String.valueOf(matcher.getField(doc, s.substring(1)));
        if (idExpr instanceof Document d) {
            StringBuilder sb = new StringBuilder();
            d.forEach((k, v) -> sb.append(k).append(":").append(
                    v instanceof String expr && expr.startsWith("$")
                            ? matcher.getField(doc, expr.substring(1))
                            : v).append("|"));
            return sb.toString();
        }
        return String.valueOf(idExpr);
    }

    private Object resolveGroupIdValue(Document sample, Object idExpr) {
        if (idExpr == null) return null;
        if (idExpr instanceof String s && s.startsWith("$"))
            return matcher.getField(sample, s.substring(1));
        if (idExpr instanceof Document d) {
            Document res = new Document();
            d.forEach((k, v) -> res.put(k, v instanceof String expr && expr.startsWith("$")
                    ? matcher.getField(sample, expr.substring(1)) : v));
            return res;
        }
        return idExpr;
    }

    @SuppressWarnings("unchecked")
    private Object computeAccumulator(List<Document> docs, Document spec) {
        Map.Entry<String, Object> acc = spec.entrySet().iterator().next();
        String op = acc.getKey();
        Object expr = acc.getValue();

        return switch (op) {
            case "$sum" -> {
                if (expr instanceof Integer i && i == 1) yield docs.size();
                double sum = 0;
                for (Document d : docs) { Object v = resolveExpr(d, expr); if (v instanceof Number n) sum += n.doubleValue(); }
                yield sum == (long) sum ? (long) sum : sum;
            }
            case "$count" -> docs.size();
            case "$avg" -> {
                double sum2 = 0; int cnt = 0;
                for (Document d : docs) { Object v = resolveExpr(d, expr); if (v instanceof Number n) { sum2 += n.doubleValue(); cnt++; } }
                yield cnt == 0 ? 0.0 : sum2 / cnt;
            }
            case "$min" -> docs.stream().map(d -> resolveExpr(d, expr))
                    .filter(Objects::nonNull)
                    .min((a, b) -> a instanceof Number na && b instanceof Number nb
                            ? Double.compare(na.doubleValue(), nb.doubleValue()) : a.toString().compareTo(b.toString()))
                    .orElse(null);
            case "$max" -> docs.stream().map(d -> resolveExpr(d, expr))
                    .filter(Objects::nonNull)
                    .max((a, b) -> a instanceof Number na && b instanceof Number nb
                            ? Double.compare(na.doubleValue(), nb.doubleValue()) : a.toString().compareTo(b.toString()))
                    .orElse(null);
            case "$first" -> docs.isEmpty() ? null : resolveExpr(docs.get(0), expr);
            case "$last"  -> docs.isEmpty() ? null : resolveExpr(docs.get(docs.size() - 1), expr);
            case "$push"  -> docs.stream().map(d -> resolveExpr(d, expr)).collect(Collectors.toList());
            case "$addToSet" -> {
                List<Object> set = new ArrayList<>();
                for (Document d : docs) {
                    Object v = resolveExpr(d, expr);
                    if (v != null && set.stream().noneMatch(x -> Objects.equals(x, v))) set.add(v);
                }
                yield set;
            }
            default -> null;
        };
    }

    // ── $sort ─────────────────────────────────────────────────────────────────

    private List<Document> stageSort(List<Document> docs, Document spec) {
        if (spec == null) return docs;
        List<Document> sorted = new ArrayList<>(docs);
        List<Map.Entry<String, Object>> sortFields = new ArrayList<>(spec.entrySet());

        sorted.sort((a, b) -> {
            for (Map.Entry<String, Object> sf : sortFields) {
                Object va = matcher.getField(a, sf.getKey());
                Object vb = matcher.getField(b, sf.getKey());
                int dir  = toInt(sf.getValue());
                int cmp  = compareValues(va, vb);
                if (cmp != 0) return dir >= 0 ? cmp : -cmp;
            }
            return 0;
        });
        return sorted;
    }

    private int compareValues(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return  1;
        if (a instanceof Number na && b instanceof Number nb)
            return Double.compare(na.doubleValue(), nb.doubleValue());
        return a.toString().compareTo(b.toString());
    }

    // ── $limit / $skip ────────────────────────────────────────────────────────

    private List<Document> stageLimit(List<Document> docs, int n) {
        return n <= 0 ? docs : docs.stream().limit(n).collect(Collectors.toList());
    }

    private List<Document> stageSkip(List<Document> docs, int n) {
        return n <= 0 ? docs : docs.stream().skip(n).collect(Collectors.toList());
    }

    // ── $unwind ───────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private List<Document> stageUnwind(List<Document> docs, Object spec) {
        String fieldPath;
        boolean preserveNullAndEmpty = false;

        if (spec instanceof String s) {
            fieldPath = s.startsWith("$") ? s.substring(1) : s;
        } else if (spec instanceof Document d) {
            String p = String.valueOf(d.get("path"));
            fieldPath = p.startsWith("$") ? p.substring(1) : p;
            preserveNullAndEmpty = Boolean.TRUE.equals(d.get("preserveNullAndEmptyArrays"));
        } else {
            return docs;
        }

        List<Document> result = new ArrayList<>();
        for (Document doc : docs) {
            Object val = matcher.getField(doc, fieldPath);
            if (val instanceof List<?> list) {
                if (list.isEmpty() && preserveNullAndEmpty) { result.add(doc); continue; }
                for (Object item : list) {
                    Document copy = new Document(doc);
                    copy.put(fieldPath, item);
                    result.add(copy);
                }
            } else if (val == null) {
                if (preserveNullAndEmpty) result.add(doc);
            } else {
                result.add(doc); // scalar — pass through
            }
        }
        return result;
    }

    // ── $addFields / $set ─────────────────────────────────────────────────────

    private List<Document> stageAddFields(List<Document> docs, Document spec) {
        if (spec == null) return docs;
        return docs.stream().map(doc -> {
            Document copy = new Document(doc);
            spec.forEach((k, v) -> copy.put(k, resolveExpr(copy, v)));
            return copy;
        }).collect(Collectors.toList());
    }

    // ── $count ────────────────────────────────────────────────────────────────

    private List<Document> stageCount(List<Document> docs, String fieldName) {
        return List.of(new Document(fieldName, docs.size()));
    }

    // ── $lookup ───────────────────────────────────────────────────────────────

    private List<Document> stageLookup(List<Document> docs, Document spec) {
        if (spec == null) return docs;
        String from        = String.valueOf(spec.get("from"));
        String localField  = String.valueOf(spec.get("localField"));
        String foreignField= String.valueOf(spec.get("foreignField"));
        String as          = String.valueOf(spec.get("as"));

        List<Document> foreign = allCollections.getOrDefault(from, Collections.emptyList());

        return docs.stream().map(doc -> {
            Object localVal = matcher.getField(doc, localField);
            List<Document> matched = foreign.stream()
                    .filter(f -> Objects.equals(localVal, matcher.getField(f, foreignField))
                              || (localVal != null && localVal.toString()
                                    .equals(String.valueOf(matcher.getField(f, foreignField)))))
                    .collect(Collectors.toList());
            Document copy = new Document(doc);
            copy.put(as, matched);
            return copy;
        }).collect(Collectors.toList());
    }

    // ── Expression evaluation ─────────────────────────────────────────────────

    private Object resolveExpr(Document doc, Object expr) {
        if (expr instanceof String s && s.startsWith("$")) return matcher.getField(doc, s.substring(1));
        if (expr instanceof Document d) return evalDocExpr(doc, d);
        return expr;
    }

    private Object evalDocExpr(Document doc, Document expr) {
        // Simple $toLower / $toUpper / $toString / $concat
        if (expr.containsKey("$toLower")) {
            Object v = resolveExpr(doc, expr.get("$toLower"));
            return v != null ? v.toString().toLowerCase() : null;
        }
        if (expr.containsKey("$toUpper")) {
            Object v = resolveExpr(doc, expr.get("$toUpper"));
            return v != null ? v.toString().toUpperCase() : null;
        }
        if (expr.containsKey("$toString")) {
            Object v = resolveExpr(doc, expr.get("$toString"));
            return v != null ? v.toString() : null;
        }
        if (expr.containsKey("$concat")) {
            if (expr.get("$concat") instanceof List<?> parts) {
                StringBuilder sb = new StringBuilder();
                for (Object p : parts) {
                    Object v = resolveExpr(doc, p);
                    sb.append(v != null ? v : "");
                }
                return sb.toString();
            }
        }
        if (expr.containsKey("$add")) {
            if (expr.get("$add") instanceof List<?> parts && parts.size() == 2) {
                Object a = resolveExpr(doc, parts.get(0));
                Object b = resolveExpr(doc, parts.get(1));
                if (a instanceof Number na && b instanceof Number nb)
                    return na.doubleValue() + nb.doubleValue();
            }
        }
        if (expr.containsKey("$subtract")) {
            if (expr.get("$subtract") instanceof List<?> parts && parts.size() == 2) {
                Object a = resolveExpr(doc, parts.get(0));
                Object b = resolveExpr(doc, parts.get(1));
                if (a instanceof Number na && b instanceof Number nb)
                    return na.doubleValue() - nb.doubleValue();
            }
        }
        // Fallback: evaluate as field expression
        return expr.toString();
    }

    private Object evalExpr(Document doc, Object expr) {
        return resolveExpr(doc, expr);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private Document asDoc(Object obj) {
        if (obj instanceof Document d) return d;
        if (obj instanceof Map<?,?> m)  return new Document((Map<String,Object>) m);
        return null;
    }

    private int toInt(Object v) {
        if (v instanceof Number n) return n.intValue();
        try { return Integer.parseInt(String.valueOf(v)); } catch (Exception e) { return 0; }
    }
}
