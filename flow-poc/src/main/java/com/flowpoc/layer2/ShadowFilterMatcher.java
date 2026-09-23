/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  org.bson.Document
 */
package com.flowpoc.layer2;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.bson.Document;

public class ShadowFilterMatcher {
    public boolean matches(Document doc, Object filter) {
        if (filter == null) {
            return true;
        }
        Document f = this.coerceDoc(filter);
        return f == null || this.evalDoc(doc, f);
    }

    private boolean evalDoc(Document doc, Document filter) {
        block12: for (Map.Entry e : filter.entrySet()) {
            String key = (String)e.getKey();
            Object val = e.getValue();
            switch (key) {
                case "$and": {
                    if (this.evalAnd(doc, val)) continue block12;
                    return false;
                }
                case "$or": {
                    if (this.evalOr(doc, val)) continue block12;
                    return false;
                }
                case "$nor": {
                    if (!this.evalOr(doc, val)) continue block12;
                    return false;
                }
                case "$not": {
                    Document sub = this.coerceDoc(val);
                    if (sub == null || !this.evalDoc(doc, sub)) continue block12;
                    return false;
                }
                default: {
                    if (this.evalField(this.getField(doc, key), val)) continue block12;
                    return false;
                }
            }
        }
        return true;
    }

    private boolean evalAnd(Document doc, Object val) {
        if (!(val instanceof List)) {
            return true;
        }
        List list = (List)val;
        for (Object item : list) {
            Document sub = this.coerceDoc(item);
            if (sub == null || this.evalDoc(doc, sub)) continue;
            return false;
        }
        return true;
    }

    private boolean evalOr(Document doc, Object val) {
        if (!(val instanceof List)) {
            return false;
        }
        List list = (List)val;
        for (Object item : list) {
            Document sub = this.coerceDoc(item);
            if (sub == null || !this.evalDoc(doc, sub)) continue;
            return true;
        }
        return false;
    }

    private boolean evalField(Object docVal, Object filterVal) {
        if (filterVal instanceof Document) {
            Document ops = (Document)filterVal;
            for (Map.Entry op : ops.entrySet()) {
                if (this.evalOp(docVal, (String)op.getKey(), op.getValue())) continue;
                return false;
            }
            return true;
        }
        return this.eq(docVal, filterVal);
    }

    private boolean evalOp(Object docVal, String op, Object opVal) {
        return switch (op) {
            case "$eq" -> this.eq(docVal, opVal);
            case "$ne" -> {
                if (!this.eq(docVal, opVal)) {
                    yield true;
                }
                yield false;
            }
            case "$gt" -> {
                if (this.cmp(docVal, opVal) > 0) {
                    yield true;
                }
                yield false;
            }
            case "$gte" -> {
                if (this.cmp(docVal, opVal) >= 0) {
                    yield true;
                }
                yield false;
            }
            case "$lt" -> {
                if (this.cmp(docVal, opVal) < 0) {
                    yield true;
                }
                yield false;
            }
            case "$lte" -> {
                if (this.cmp(docVal, opVal) <= 0) {
                    yield true;
                }
                yield false;
            }
            case "$in" -> this.inList(docVal, opVal);
            case "$nin" -> {
                if (!this.inList(docVal, opVal)) {
                    yield true;
                }
                yield false;
            }
            case "$exists" -> {
                if (Boolean.TRUE.equals(opVal)) {
                    if (docVal != null) {
                        yield true;
                    }
                    yield false;
                }
                if (docVal == null) {
                    yield true;
                }
                yield false;
            }
            case "$regex" -> this.matchRegex(docVal, opVal, null);
            case "$options" -> true;
            case "$type" -> this.matchType(docVal, opVal);
            case "$elemMatch" -> this.matchElemMatch(docVal, opVal);
            case "$all" -> this.matchAll(docVal, opVal);
            case "$size" -> this.matchSize(docVal, opVal);
            case "$not" -> {
                Document sub = this.coerceDoc(opVal);
                if (sub == null || !this.evalField(docVal, sub)) {
                    yield true;
                }
                yield false;
            }
            default -> true;
        };
    }

    private boolean eq(Object a, Object b) {
        if (a == null && b == null) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if (a.equals(b)) {
            return true;
        }
        if (a instanceof Number) {
            Number na = (Number)a;
            if (b instanceof Number) {
                Number nb = (Number)b;
                return na.doubleValue() == nb.doubleValue();
            }
        }
        return a.toString().equals(b.toString());
    }

    private int cmp(Object a, Object b) {
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
        if (a instanceof Comparable) {
            Comparable ca = (Comparable)a;
            return ca.compareTo(b);
        }
        return a.toString().compareTo(b.toString());
    }

    private boolean inList(Object docVal, Object opVal) {
        if (!(opVal instanceof List)) {
            return false;
        }
        List list = (List)opVal;
        for (Object item : list) {
            if (!this.eq(docVal, item)) continue;
            return true;
        }
        return false;
    }

    private boolean matchRegex(Object docVal, Object pattern, Object options) {
        String string;
        if (docVal == null) {
            return false;
        }
        String s = docVal.toString();
        if (pattern instanceof Pattern) {
            Pattern pat = (Pattern)pattern;
            string = pat.pattern();
        } else {
            string = String.valueOf(pattern);
        }
        String p = string;
        try {
            return Pattern.compile(p).matcher(s).find();
        }
        catch (Exception e) {
            return false;
        }
    }

    private boolean matchType(Object docVal, Object typeVal) {
        String t;
        if (docVal == null) {
            return "null".equals(String.valueOf(typeVal));
        }
        return switch (t = String.valueOf(typeVal).toLowerCase()) {
            case "string", "2" -> docVal instanceof String;
            case "int", "16" -> docVal instanceof Integer;
            case "long", "18" -> docVal instanceof Long;
            case "double", "1" -> docVal instanceof Double;
            case "bool", "boolean", "8" -> docVal instanceof Boolean;
            case "array", "4" -> docVal instanceof List;
            case "object", "3" -> {
                if (docVal instanceof Document || docVal instanceof Map) {
                    yield true;
                }
                yield false;
            }
            default -> true;
        };
    }

    private boolean matchElemMatch(Object docVal, Object opVal) {
        if (!(docVal instanceof List)) {
            return false;
        }
        List list = (List)docVal;
        Document sub = this.coerceDoc(opVal);
        if (sub == null) {
            return false;
        }
        for (Object elem : list) {
            Document elemDoc = this.coerceDoc(elem);
            if (elemDoc == null || !this.evalDoc(elemDoc, sub)) continue;
            return true;
        }
        return false;
    }

    private boolean matchAll(Object docVal, Object opVal) {
        List docList;
        block5: {
            block4: {
                if (!(docVal instanceof List)) break block4;
                docList = (List)docVal;
                if (opVal instanceof List) break block5;
            }
            return false;
        }
        List reqList = (List)opVal;
        for (Object req : reqList) {
            boolean found = false;
            for (Object item : docList) {
                if (!this.eq(item, req)) continue;
                found = true;
                break;
            }
            if (found) continue;
            return false;
        }
        return true;
    }

    private boolean matchSize(Object docVal, Object opVal) {
        if (!(docVal instanceof List)) {
            return false;
        }
        List list = (List)docVal;
        int expected = ((Number)opVal).intValue();
        return list.size() == expected;
    }

    Object getField(Document doc, String path) {
        if (!path.contains(".")) {
            return doc.get((Object)path);
        }
        String[] parts = path.split("\\.", 2);
        Object nested = doc.get((Object)parts[0]);
        if (nested instanceof Document) {
            Document nd = (Document)nested;
            return this.getField(nd, parts[1]);
        }
        if (nested instanceof Map) {
            Map m = (Map)nested;
            return this.getField(new Document(m), parts[1]);
        }
        return null;
    }

    Document coerceDoc(Object obj) {
        if (obj == null) {
            return null;
        }
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
}

