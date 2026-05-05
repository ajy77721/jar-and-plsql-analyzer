package com.flowpoc.layer2;

import org.bson.Document;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Evaluates MongoDB filter documents against in-memory Document objects.
 *
 * Supported operators:
 *   Comparison : $eq $ne $gt $gte $lt $lte $in $nin
 *   Logical    : $and $or $nor $not
 *   Element    : $exists $type
 *   Evaluation : $regex
 *   Array      : $elemMatch $all $size
 *   Dot-path   : nested field access via "a.b.c"
 */
public class ShadowFilterMatcher {

    public boolean matches(Document doc, Object filter) {
        if (filter == null) return true;
        Document f = coerceDoc(filter);
        return f == null || evalDoc(doc, f);
    }

    // ── Top-level document evaluation ─────────────────────────────────────────

    private boolean evalDoc(Document doc, Document filter) {
        for (Map.Entry<String, Object> e : filter.entrySet()) {
            String key = e.getKey();
            Object val = e.getValue();
            switch (key) {
                case "$and" -> { if (!evalAnd(doc, val)) return false; }
                case "$or"  -> { if (!evalOr(doc, val))  return false; }
                case "$nor" -> { if ( evalOr(doc, val))  return false; }
                case "$not" -> { Document sub = coerceDoc(val);
                                 if (sub != null && evalDoc(doc, sub)) return false; }
                default     -> { if (!evalField(getField(doc, key), val)) return false; }
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private boolean evalAnd(Document doc, Object val) {
        if (!(val instanceof List<?> list)) return true;
        for (Object item : list) {
            Document sub = coerceDoc(item);
            if (sub != null && !evalDoc(doc, sub)) return false;
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private boolean evalOr(Document doc, Object val) {
        if (!(val instanceof List<?> list)) return false;
        for (Object item : list) {
            Document sub = coerceDoc(item);
            if (sub != null && evalDoc(doc, sub)) return true;
        }
        return false;
    }

    // ── Field predicate ────────────────────────────────────────────────────────

    private boolean evalField(Object docVal, Object filterVal) {
        if (filterVal instanceof Document ops) {
            for (Map.Entry<String, Object> op : ops.entrySet()) {
                if (!evalOp(docVal, op.getKey(), op.getValue())) return false;
            }
            return true;
        }
        return eq(docVal, filterVal);
    }

    private boolean evalOp(Object docVal, String op, Object opVal) {
        return switch (op) {
            case "$eq"       -> eq(docVal, opVal);
            case "$ne"       -> !eq(docVal, opVal);
            case "$gt"       -> cmp(docVal, opVal) > 0;
            case "$gte"      -> cmp(docVal, opVal) >= 0;
            case "$lt"       -> cmp(docVal, opVal) < 0;
            case "$lte"      -> cmp(docVal, opVal) <= 0;
            case "$in"       -> inList(docVal, opVal);
            case "$nin"      -> !inList(docVal, opVal);
            case "$exists"   -> Boolean.TRUE.equals(opVal) ? docVal != null : docVal == null;
            case "$regex"    -> matchRegex(docVal, opVal, null);
            case "$options"  -> true;   // handled alongside $regex — skip standalone
            case "$type"     -> matchType(docVal, opVal);
            case "$elemMatch"-> matchElemMatch(docVal, opVal);
            case "$all"      -> matchAll(docVal, opVal);
            case "$size"     -> matchSize(docVal, opVal);
            case "$not"      -> { Document sub = coerceDoc(opVal);
                                   yield sub == null || !evalField(docVal, sub); }
            default          -> true;   // unknown — pass through
        };
    }

    // ── Operators ─────────────────────────────────────────────────────────────

    private boolean eq(Object a, Object b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        if (a.equals(b))            return true;
        // Cross-type numeric comparison
        if (a instanceof Number na && b instanceof Number nb)
            return na.doubleValue() == nb.doubleValue();
        return a.toString().equals(b.toString());
    }

    @SuppressWarnings("unchecked")
    private int cmp(Object a, Object b) {
        if (a == null) return -1;
        if (b == null) return 1;
        if (a instanceof Number na && b instanceof Number nb)
            return Double.compare(na.doubleValue(), nb.doubleValue());
        if (a instanceof Comparable ca)
            return ca.compareTo(b);
        return a.toString().compareTo(b.toString());
    }

    @SuppressWarnings("unchecked")
    private boolean inList(Object docVal, Object opVal) {
        if (!(opVal instanceof List<?> list)) return false;
        for (Object item : list) { if (eq(docVal, item)) return true; }
        return false;
    }

    private boolean matchRegex(Object docVal, Object pattern, Object options) {
        if (docVal == null) return false;
        String s = docVal.toString();
        String p = pattern instanceof Pattern pat ? pat.pattern() : String.valueOf(pattern);
        try { return Pattern.compile(p).matcher(s).find(); }
        catch (Exception e) { return false; }
    }

    private boolean matchType(Object docVal, Object typeVal) {
        if (docVal == null) return "null".equals(String.valueOf(typeVal));
        String t = String.valueOf(typeVal).toLowerCase();
        return switch (t) {
            case "string", "2"  -> docVal instanceof String;
            case "int", "16"    -> docVal instanceof Integer;
            case "long", "18"   -> docVal instanceof Long;
            case "double", "1"  -> docVal instanceof Double;
            case "bool", "boolean", "8" -> docVal instanceof Boolean;
            case "array", "4"   -> docVal instanceof List;
            case "object", "3"  -> docVal instanceof Document || docVal instanceof Map;
            default             -> true;
        };
    }

    @SuppressWarnings("unchecked")
    private boolean matchElemMatch(Object docVal, Object opVal) {
        if (!(docVal instanceof List<?> list)) return false;
        Document sub = coerceDoc(opVal);
        if (sub == null) return false;
        for (Object elem : list) {
            Document elemDoc = coerceDoc(elem);
            if (elemDoc != null && evalDoc(elemDoc, sub)) return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private boolean matchAll(Object docVal, Object opVal) {
        if (!(docVal instanceof List<?> docList) || !(opVal instanceof List<?> reqList)) return false;
        for (Object req : reqList) {
            boolean found = false;
            for (Object item : docList) { if (eq(item, req)) { found = true; break; } }
            if (!found) return false;
        }
        return true;
    }

    private boolean matchSize(Object docVal, Object opVal) {
        if (!(docVal instanceof List<?> list)) return false;
        int expected = ((Number) opVal).intValue();
        return list.size() == expected;
    }

    // ── Nested field access via dot-path ──────────────────────────────────────

    Object getField(Document doc, String path) {
        if (!path.contains(".")) return doc.get(path);
        String[] parts = path.split("\\.", 2);
        Object nested = doc.get(parts[0]);
        if (nested instanceof Document nd) return getField(nd, parts[1]);
        if (nested instanceof Map<?,?> m) {
            return getField(new Document((Map<String,Object>) m), parts[1]);
        }
        return null;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    Document coerceDoc(Object obj) {
        if (obj == null)            return null;
        if (obj instanceof Document d) return d;
        if (obj instanceof Map<?,?> m) return new Document((Map<String,Object>) m);
        return null;
    }
}
